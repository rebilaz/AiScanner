"""Compute daily cross-chain bridge flows and store results in BigQuery."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import yaml
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
EVENTS_TABLE = os.getenv("BQ_LABELED_EVENTS_TABLE", "labeled_events")
PRICES_TABLE = os.getenv("BQ_PRICES_TABLE", "token_prices")
DEST_TABLE = os.getenv("BQ_DAILY_FLOWS_TABLE", "daily_cross_chain_flows")
CONFIG_PATH = os.getenv("BRIDGES_CONFIG_PATH", "bridges/bridges_config.yaml")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "1"))


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def load_config() -> List[Dict[str, Any]]:
    """Load bridges configuration from YAML."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("bridges", [])


def fetch_bridge_events(client: bigquery.Client, bridges: List[Dict[str, Any]]) -> pd.DataFrame:
    """Fetch deposit and withdrawal events for configured bridges."""
    if not bridges:
        return pd.DataFrame()

    clauses = []
    for bridge in bridges:
        for chain in bridge.get("chains", []):
            addr = chain.get("contract_address", "").lower()
            dep = chain.get("deposit_event_name")
            wdr = chain.get("withdrawal_event_name")
            if addr and dep and wdr:
                clauses.append(
                    f"(LOWER(contract_address)='{addr}' AND event_name IN ('{dep}','{wdr}'))"
                )
    if not clauses:
        return pd.DataFrame()

    where_clause = " OR ".join(clauses)
    query = f"""
        SELECT contract_address, event_name, token_address,
               CAST(COALESCE(value, amount, 0) AS FLOAT64) AS amount,
               block_timestamp, chain_id
        FROM `{client.project}.{DATASET}.{EVENTS_TABLE}`
        WHERE ({where_clause})
          AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_DAYS} DAY)
    """
    logging.info("Executing bridge events query")
    return client.query(query).to_dataframe()


def apply_pricing(client: bigquery.Client, df: pd.DataFrame) -> pd.DataFrame:
    """Attach USD value to each transfer using historical prices."""
    if df.empty:
        return df
    tokens = df["token_address"].dropna().unique()
    token_list = ", ".join(f"'{t.lower()}'" for t in tokens)
    query = f"""
        WITH latest AS (
            SELECT token_address,
                   DATE(price_timestamp) AS day,
                   price_usd,
                   ROW_NUMBER() OVER (PARTITION BY token_address, DATE(price_timestamp) ORDER BY price_timestamp DESC) AS rn
            FROM `{client.project}.{DATASET}.{PRICES_TABLE}`
            WHERE LOWER(token_address) IN ({token_list})
        )
        SELECT token_address, day, price_usd FROM latest WHERE rn = 1
    """
    price_df = client.query(query).to_dataframe()
    if price_df.empty:
        df["usd_value"] = 0.0
        return df
    df["day"] = df["block_timestamp"].dt.date
    merged = df.merge(price_df, how="left", left_on=["token_address", "day"], right_on=["token_address", "day"])
    merged["usd_value"] = merged["amount"] * merged["price_usd"].fillna(0)
    return merged


def classify_flows(df: pd.DataFrame, bridges: List[Dict[str, Any]]) -> pd.DataFrame:
    """Label events as inflow or outflow based on configuration."""
    mapping: Dict[tuple[str, str], str] = {}
    for bridge in bridges:
        for chain in bridge.get("chains", []):
            addr = chain.get("contract_address", "").lower()
            dep = chain.get("deposit_event_name")
            wdr = chain.get("withdrawal_event_name")
            if addr and dep and wdr:
                mapping[(addr, dep)] = "outflow"
                mapping[(addr, wdr)] = "inflow"

    df["flow_type"] = [mapping.get((a.lower(), e), "") for a, e in zip(df["contract_address"], df["event_name"])]
    return df[df["flow_type"] != ""]


def aggregate_flows(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate inflows and outflows per chain per day."""
    if df.empty:
        return pd.DataFrame()
    df["day"] = df["block_timestamp"].dt.date
    inflow = (
        df[df["flow_type"] == "inflow"].groupby(["chain_id", "day"])["usd_value"].sum().reset_index(name="total_inflow_usd")
    )
    outflow = (
        df[df["flow_type"] == "outflow"].groupby(["chain_id", "day"])["usd_value"].sum().reset_index(name="total_outflow_usd")
    )
    result = pd.merge(inflow, outflow, on=["chain_id", "day"], how="outer").fillna(0)
    result["net_flow_usd"] = result["total_inflow_usd"] - result["total_outflow_usd"]
    return result


def store_flows(client: bigquery.Client, df: pd.DataFrame) -> None:
    """Upload aggregated flows into BigQuery."""
    if df.empty:
        logging.info("No flow data to store")
        return
    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Uploading %d flow records to %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


async def run_bridge_flows_worker() -> None:
    """Main entry point to compute bridge liquidity flows."""
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    bridges = load_config()
    client = bigquery.Client(project=PROJECT_ID)

    events = fetch_bridge_events(client, bridges)
    if events.empty:
        logging.info("No bridge events found")
        return

    events = apply_pricing(client, events)
    events = classify_flows(events, bridges)
    aggregated = aggregate_flows(events)
    store_flows(client, aggregated)


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_bridge_flows_worker())
