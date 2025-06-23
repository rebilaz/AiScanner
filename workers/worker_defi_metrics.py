"""Aggregate on-chain and off-chain data to compute DeFi protocol metrics."""

from __future__ import annotations

import logging
import os
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
LABELED_EVENTS_TABLE = os.getenv("BQ_LABELED_EVENTS_TABLE", "labeled_events")
PRICES_TABLE = os.getenv("BQ_PRICES_TABLE", "token_prices")
DEST_TABLE = os.getenv("BQ_PROTOCOL_METRICS_TABLE", "protocol_metrics")

# Example protocols configuration. Extend as needed.
PROTOCOLS: List[Dict[str, Any]] = [
    {
        "name": "example",
        "defillama_id": "example",
        "contracts": ["0x0000000000000000000000000000000000000000"],
    }
]

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_token_balances(client: bigquery.Client, protocol: Dict[str, Any]) -> Dict[str, float]:
    """Return token balances for the given protocol."""
    contracts = ", ".join(f"'{c.lower()}'" for c in protocol.get("contracts", []))
    if not contracts:
        return {}

    query = f"""
        SELECT token_address, event_name, SUM(CAST(COALESCE(value, amount, 0) AS FLOAT64)) AS quantity
        FROM `{client.project}.{DATASET}.{LABELED_EVENTS_TABLE}`
        WHERE LOWER(contract_address) IN ({contracts})
          AND event_name IN ('Deposit', 'Withdrawal')
        GROUP BY token_address, event_name
    """
    df = client.query(query).to_dataframe()
    balances: Dict[str, float] = {}
    if df.empty:
        return balances

    for token, group in df.groupby("token_address"):
        deposit = group[group["event_name"] == "Deposit"]["quantity"].sum()
        withdraw = group[group["event_name"] == "Withdrawal"]["quantity"].sum()
        balances[token] = float(deposit - withdraw)
    return balances


def get_prices(client: bigquery.Client, tokens: List[str]) -> Dict[str, float]:
    """Retrieve the latest USD price for each token."""
    if not tokens:
        return {}

    token_list = ", ".join(f"'{t.lower()}'" for t in tokens)
    query = f"""
        WITH latest AS (
            SELECT token_address, price_usd,
                   ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY price_timestamp DESC) AS rn
            FROM `{client.project}.{DATASET}.{PRICES_TABLE}`
            WHERE LOWER(token_address) IN ({token_list})
        )
        SELECT token_address, price_usd FROM latest WHERE rn = 1
    """
    df = client.query(query).to_dataframe()
    return {row["token_address"].lower(): float(row["price_usd"]) for _, row in df.iterrows()}


def calculate_tvl(balances: Dict[str, float], prices: Dict[str, float]) -> float:
    """Calculate total value locked from balances and prices."""
    tvl = 0.0
    for token, amount in balances.items():
        price = prices.get(token.lower())
        if price is None:
            logging.warning("Missing price for token %s", token)
            continue
        tvl += amount * price
    return tvl


def calculate_revenues(client: bigquery.Client, protocol: Dict[str, Any]) -> float:
    """Calculate protocol revenues from Swap events (if available)."""
    contracts = ", ".join(f"'{c.lower()}'" for c in protocol.get("contracts", []))
    if not contracts:
        return 0.0

    query = f"""
        SELECT SUM(CAST(fee AS FLOAT64)) AS total_fees
        FROM `{client.project}.{DATASET}.{LABELED_EVENTS_TABLE}`
        WHERE LOWER(contract_address) IN ({contracts})
          AND event_name = 'Swap'
    """
    try:
        df = client.query(query).to_dataframe()
        if df.empty:
            return 0.0
        return float(df.iloc[0]["total_fees"] or 0)
    except Exception:
        logging.exception("Failed to calculate revenues for %s", protocol["name"])
        return 0.0


def validate_with_defillama(protocol: Dict[str, Any], local_tvl: float) -> Optional[float]:
    """Fetch TVL from DeFiLlama for comparison."""
    url = f"https://api.llama.fi/tvl/{protocol['defillama_id']}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict):
                return float(data.get("tvl") or data.get("tvlUsd", 0))
            return float(data)
        logging.error("DeFiLlama API returned %s for %s", resp.status_code, protocol["name"])
    except Exception as exc:  # pragma: no cover - network issues
        logging.exception("Failed to fetch DeFiLlama TVL for %s: %s", protocol["name"], exc)
    return None


def store_protocol_metrics(client: bigquery.Client, results: List[Dict[str, Any]]) -> None:
    """Upload protocol metrics to BigQuery."""
    if not results:
        logging.info("No metrics to store")
        return

    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    df = pd.DataFrame(results)
    logging.info("Uploading %d protocol metrics to %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


async def run_defi_metrics_worker() -> None:
    """Main worker to compute and store DeFi protocol metrics."""
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    client = bigquery.Client(project=PROJECT_ID)

    metrics: List[Dict[str, Any]] = []
    for protocol in PROTOCOLS:
        logging.info("Processing protocol %s", protocol["name"])
        balances = get_token_balances(client, protocol)
        prices = get_prices(client, list(balances.keys()))
        local_tvl = calculate_tvl(balances, prices)
        revenue = calculate_revenues(client, protocol)
        llama_tvl = validate_with_defillama(protocol, local_tvl)
        diff_pct = None
        if llama_tvl and llama_tvl > 0:
            diff_pct = ((local_tvl - llama_tvl) / llama_tvl) * 100
        metrics.append(
            {
                "protocol": protocol["name"],
                "local_tvl": local_tvl,
                "defillama_tvl": llama_tvl,
                "difference_pct": diff_pct,
                "revenue": revenue,
            }
        )

    store_protocol_metrics(client, metrics)


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_defi_metrics_worker())
