"""Fetch verified contract source code from block explorers and store it in BigQuery."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
CONTRACTS_TABLE = os.getenv("BQ_CONTRACTS_TABLE", "contracts")
CODE_TABLE = os.getenv("BQ_CONTRACT_CODE_TABLE", "contract_code")
BATCH_SIZE = int(os.getenv("CONTRACT_BATCH_SIZE", "100"))
RATE_LIMIT = float(os.getenv("EXPLORER_RATE_LIMIT", "5"))  # requests/sec
SLEEP_SECONDS = 1 / RATE_LIMIT if RATE_LIMIT > 0 else 0.25

EXPLORER_CONFIG: Dict[int, Dict[str, str]] = {
    1: {
        "base_url": "https://api.etherscan.io/api",
        "api_key": os.getenv("ETHERSCAN_API_KEY", ""),
    },
    56: {
        "base_url": "https://api.bscscan.com/api",
        "api_key": os.getenv("BSCSCAN_API_KEY", ""),
    },
    137: {
        "base_url": "https://api.polygonscan.com/api",
        "api_key": os.getenv("POLYGONSCAN_API_KEY", ""),
    },
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_new_contracts(client: bigquery.Client, limit: int) -> pd.DataFrame:
    """Return contracts in `contracts` table that are missing in `contract_code`."""
    query = f"""
        SELECT c.contract_address, c.chain_id
        FROM `{client.project}.{DATASET}.{CONTRACTS_TABLE}` c
        LEFT JOIN `{client.project}.{DATASET}.{CODE_TABLE}` d
        ON LOWER(c.contract_address) = LOWER(d.contract_address)
           AND c.chain_id = d.chain_id
        WHERE d.contract_address IS NULL
        LIMIT {limit}
    """
    logging.info("Fetching up to %d new contracts", limit)
    return client.query(query).to_dataframe()


def fetch_contract_info(address: str, chain_id: int) -> Optional[Dict[str, Any]]:
    """Fetch contract source code and bytecode from the appropriate explorer."""
    cfg = EXPLORER_CONFIG.get(chain_id)
    if not cfg or not cfg.get("api_key"):
        logging.warning("No explorer configuration for chain_id %s", chain_id)
        return None

    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": cfg["api_key"],
    }
    try:
        resp = requests.get(cfg["base_url"], params=params, timeout=10)
    except Exception as exc:  # pragma: no cover - network issues
        logging.exception("Explorer request failed for %s: %s", address, exc)
        return None
    time.sleep(SLEEP_SECONDS)

    if resp.status_code != 200:
        logging.error("Explorer API returned %s for %s", resp.status_code, address)
        return None

    data = resp.json()
    result = data.get("result")
    if not isinstance(result, list) or not result:
        logging.error("Malformed response for %s", address)
        return None

    entry = result[0]
    is_verified = bool(entry.get("SourceCode"))
    source_code = entry.get("SourceCode", "")
    abi = entry.get("ABI", "")
    contract_name = entry.get("ContractName", "")

    # Fetch runtime bytecode
    params_code = {
        "module": "proxy",
        "action": "eth_getCode",
        "address": address,
        "apikey": cfg["api_key"],
    }
    try:
        code_resp = requests.get(cfg["base_url"], params=params_code, timeout=10)
        bytecode = code_resp.json().get("result", "") if code_resp.status_code == 200 else ""
    except Exception:  # pragma: no cover - network issues
        logging.exception("Failed to fetch bytecode for %s", address)
        bytecode = ""
    time.sleep(SLEEP_SECONDS)

    return {
        "contract_address": address.lower(),
        "chain_id": int(chain_id),
        "is_verified": bool(is_verified),
        "source_code": source_code,
        "abi": abi,
        "bytecode": bytecode,
        "contract_name": contract_name,
        "timestamp": datetime.now(tz=timezone.utc),
    }


def store_contract_code(client: bigquery.Client, rows: List[Dict[str, Any]]) -> None:
    """Append fetched contract code rows into BigQuery."""
    if not rows:
        logging.info("No contract code to store")
        return

    table_ref = f"{client.project}.{DATASET}.{CODE_TABLE}"
    df = pd.DataFrame(rows)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Uploading %d contract codes to %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


async def run_contract_fetcher_worker() -> None:
    """Main worker entry to fetch verified contract code."""
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    client = bigquery.Client(project=PROJECT_ID)

    try:
        contracts = get_new_contracts(client, BATCH_SIZE)
    except Exception:  # pragma: no cover - network/credentials issues
        logging.exception("Failed to fetch new contracts")
        return

    if contracts.empty:
        logging.info("No new contracts to process")
        return

    results: List[Dict[str, Any]] = []
    for _, row in contracts.iterrows():
        address = row["contract_address"]
        chain_id = int(row["chain_id"])
        info = fetch_contract_info(address, chain_id)
        if info:
            results.append(info)

    try:
        store_contract_code(client, results)
    except Exception:  # pragma: no cover - network/credentials issues
        logging.exception("Failed to store contract code")


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_contract_fetcher_worker())
