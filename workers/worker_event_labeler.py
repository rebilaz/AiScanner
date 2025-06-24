"""Decode Ethereum logs into labeled events and store them in BigQuery."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from google.cloud import bigquery
from web3 import Web3
from web3._utils.events import get_event_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
LOGS_TABLE = os.getenv("BQ_LOGS_RAW_TABLE", "logs_raw")
TOKENS_TABLE = os.getenv("BQ_TOKENS_TABLE", "token_addresses")
DEST_TABLE = os.getenv("BQ_LABELED_EVENTS_TABLE", "labeled_events")
BATCH_SIZE = int(os.getenv("EVENTS_BATCH_SIZE", "1000"))

# Static ABI mapping {address: ABI list}
# Extend or replace this mapping with dynamic fetching if required.
ABI_MAPPING: dict[str, list[dict[str, Any]]] = {
    # "0x1234...": json.loads("[...]"),
}

# Global Web3 instance for decoding
w3 = Web3()


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def fetch_raw_logs(client: bigquery.Client, limit: int) -> pd.DataFrame:
    """Retrieve a batch of raw logs not yet labeled."""
    query = f"""
        SELECT lr.*
        FROM `{client.project}.{DATASET}.{LOGS_TABLE}` AS lr
        LEFT JOIN `{client.project}.{DATASET}.{DEST_TABLE}` AS le
        ON lr.block_number = le.block_number
           AND lr.transaction_hash = le.transaction_hash
           AND lr.log_index = le.log_index
        WHERE le.transaction_hash IS NULL
        ORDER BY lr.block_number
        LIMIT {limit}
    """
    return client.query(query).to_dataframe()


def get_event_abi(abi: List[Dict[str, Any]], topic0: str) -> Optional[Dict[str, Any]]:
    """Find the ABI entry for the given topic hash."""
    for entry in abi:
        if entry.get("type") != "event":
            continue
        signature = f"{entry['name']}({','.join(i['type'] for i in entry['inputs'])})"
        if Web3.keccak(text=signature).hex().lower() == topic0.lower():
            return entry
    return None


def decode_log(row: pd.Series, abi: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Decode a single log entry using the provided ABI."""
    event_abi = get_event_abi(abi, row["topics"][0])
    if not event_abi:
        logging.warning("ABI missing for topic %s address %s", row["topics"][0], row["address"])
        return None
    raw_log = {
        "address": row["address"],
        "topics": row["topics"],
        "data": row["data"],
    }
    try:
        decoded = get_event_data(w3.codec, event_abi, raw_log)  # type: ignore[arg-type]
    except Exception as exc:  # pragma: no cover - complex web3 errors
        logging.exception("Failed to decode log %s: %s", row.get("transaction_hash"), exc)
        return None
    return {
        "event_name": event_abi["name"],
        **decoded["args"],
        "block_number": row["block_number"],
        "transaction_hash": row["transaction_hash"],
        "log_index": row["log_index"],
        "contract_address": row["address"].lower(),
    }


def load_labeled_events(client: bigquery.Client, df: pd.DataFrame) -> None:
    """Upload decoded events into the destination table."""
    if df.empty:
        logging.info("No decoded events to load")
        return

    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Loading %d events into %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


async def run_label_events_worker() -> None:
    """Main worker entry to decode raw Ethereum logs."""
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    client = bigquery.Client(project=PROJECT_ID)
    tokens_df = client.query(
        f"SELECT address, name, symbol FROM `{client.project}.{DATASET}.{TOKENS_TABLE}`"
    ).to_dataframe()

    while True:
        raw_logs = fetch_raw_logs(client, BATCH_SIZE)
        if raw_logs.empty:
            logging.info("No more raw logs to process")
            break

        decoded_records: list[dict[str, Any]] = []
        for _, row in raw_logs.iterrows():
            abi = ABI_MAPPING.get(row["address"].lower())
            if not abi:
                logging.warning("No ABI for contract %s", row["address"])
                continue
            record = decode_log(row, abi)
            if record:
                decoded_records.append(record)

        if not decoded_records:
            logging.info("No events decoded in this batch")
            continue

        df_events = pd.DataFrame(decoded_records)
        df_events = df_events.merge(
            tokens_df,
            how="left",
            left_on="contract_address",
            right_on="address",
        )
        load_labeled_events(client, df_events)


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_label_events_worker())
