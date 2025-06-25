"""Analyze trending NFT collections using on-chain, market, and social data."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import yaml
from google.cloud import bigquery
from gcp_utils import create_bq_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
TOKEN_TRANSFERS_TABLE = os.getenv("BQ_TOKEN_TRANSFERS_TABLE", "token_transfers")
SENTIMENT_TABLE = os.getenv("BQ_SENTIMENT_TABLE", "social_sentiment_timeseries")
DEST_TABLE = os.getenv("BQ_NFT_TRENDS_TABLE", "nft_collection_trends")
SECURITY_TASKS_TABLE = os.getenv("BQ_SECURITY_TASKS_TABLE", "security_analysis_tasks")
CONFIG_PATH = os.getenv(
    "NFT_CONFIG_PATH",
    os.path.join(os.path.dirname(__file__), "worker_7_4_config.yaml"),
)
OPENSEA_API_KEY = os.getenv("OPENSEA_API_KEY")

# ---------------------------------------------------------------------------
# ARCHITECTURE : Boucles de rétroaction et système analytique intégré
#
# Toute détection d’une collection NFT "tendance" (par exemple, nouveau pic de volume ou de hype) doit automatiquement
# déclencher une analyse de sécurité prioritaire sur ses contrats par le Worker 7.1.
# Ce mécanisme peut se faire par :
# - Envoi d’un message Pub/Sub (topic "security_analysis") contenant l’adresse du contrat détecté
# - Ajout d’une ligne dans une table BigQuery "security_analysis_tasks"
# Cela crée un écosystème analytique auto-apprenant où la sortie d’un module devient
# l’entrée prioritaire d’un autre, renforçant la sécurité et la réactivité du système NFT.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def load_config() -> List[Dict[str, str]]:
    """Load collections configuration from YAML."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("collections", [])


def fetch_onchain_metrics(client: bigquery.Client, address: str) -> Dict[str, Any]:
    """Return on-chain transfer metrics for the last 24h."""
    query = f"""
        SELECT
            COUNT(*) AS transfer_count,
            COUNT(DISTINCT from_address) + COUNT(DISTINCT to_address) AS unique_wallets
        FROM `{client.project}.{DATASET}.{TOKEN_TRANSFERS_TABLE}`
        WHERE LOWER(token_address) = '{address.lower()}'
          AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        return {"transfer_count": 0, "unique_wallets": 0}
    row = df.iloc[0]
    return {"transfer_count": int(row.transfer_count), "unique_wallets": int(row.unique_wallets)}


def fetch_market_metrics(slug: str) -> Dict[str, Any]:
    """Fetch market data from OpenSea API."""
    url = f"https://api.opensea.io/api/v2/collection/{slug}/stats"
    headers = {"Accept": "application/json"}
    if OPENSEA_API_KEY:
        headers["X-API-KEY"] = OPENSEA_API_KEY
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json().get("stats", {})
            return {
                "floor_price": data.get("floor_price"),
                "volume_usd": data.get("total_volume"),
            }
        logging.error("OpenSea API returned %s for %s", resp.status_code, slug)
    except Exception as exc:  # pragma: no cover - network issues
        logging.exception("Failed to fetch OpenSea data for %s: %s", slug, exc)
    return {"floor_price": None, "volume_usd": None}


def fetch_social_metrics(client: bigquery.Client, address: str) -> Dict[str, Any]:
    """Retrieve latest hype score for the collection."""
    query = f"""
        SELECT hype_score
        FROM `{client.project}.{DATASET}.{SENTIMENT_TABLE}`
        WHERE asset = '{address.lower()}'
        ORDER BY timestamp DESC
        LIMIT 1
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        return {"hype_score": None}
    return {"hype_score": float(df.iloc[0].hype_score)}


def get_last_metrics(client: bigquery.Client, address: str) -> Optional[Dict[str, Any]]:
    """Fetch previous metrics for velocity calculation."""
    table = f"{client.project}.{DATASET}.{DEST_TABLE}"
    query = f"""
        SELECT * FROM `{table}`
        WHERE collection_address = '{address.lower()}'
        ORDER BY timestamp DESC
        LIMIT 1
    """
    df = client.query(query).to_dataframe()
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def compute_velocity(current: Dict[str, Any], previous: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute rate of change between current and previous metrics."""
    metrics = ["floor_price", "volume_usd", "transfer_count", "unique_wallets", "hype_score"]
    for m in metrics:
        curr_val = current.get(m)
        prev_val = previous.get(m) if previous else None
        if curr_val is not None and prev_val not in (None, 0):
            current[f"{m}_growth"] = (curr_val - prev_val) / max(prev_val, 1)
        else:
            current[f"{m}_growth"] = None
    return current


def store_metrics(client: bigquery.Client, rows: List[Dict[str, Any]]) -> None:
    """Upload metrics to BigQuery and trigger security analysis tasks."""
    if not rows:
        logging.info("No metrics to store")
        return

    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    df = pd.DataFrame(rows)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Uploading %d records to %s", len(df), table_ref)
    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()

    # Trigger security analysis for trending collections
    task_table = f"{client.project}.{DATASET}.{SECURITY_TASKS_TABLE}"
    tasks = [{"collection_address": r["collection_address"], "timestamp": r["timestamp"]} for r in rows]
    client.load_table_from_json(tasks, task_table, job_config=job_config).result()


async def run_trend_analyzer() -> None:
    """Main entry point for trend analysis."""
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration")
        return

    collections = load_config()
    if not collections:
        logging.error("No collections configured")
        return

    client = create_bq_client(PROJECT_ID)
    results: List[Dict[str, Any]] = []

    for coll in collections:
        addr = coll.get("address", "").lower()
        slug = coll.get("opensea_slug", "")
        if not addr:
            continue
        logging.info("Processing collection %s", addr)
        onchain = fetch_onchain_metrics(client, addr)
        market = fetch_market_metrics(slug) if slug else {"floor_price": None, "volume_usd": None}
        social = fetch_social_metrics(client, addr)
        metrics = {"collection_address": addr, "timestamp": datetime.utcnow()}
        metrics.update(onchain)
        metrics.update(market)
        metrics.update(social)
        previous = get_last_metrics(client, addr)
        metrics = compute_velocity(metrics, previous)
        results.append(metrics)

    store_metrics(client, results)


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_trend_analyzer())
