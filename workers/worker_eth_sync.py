"""Incremental Ethereum data sync from BigQuery public dataset.

This worker fetches new Ethereum blocks from the Google Cloud public
crypto_ethereum dataset and appends them to a private BigQuery table.
The script is idempotent and keeps track of the last synced block
number for each target table.
"""

from __future__ import annotations

import logging
import os
from typing import Tuple

import pandas as pd
from google.cloud import bigquery

from gcp_utils import BigQueryClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DEST_DATASET = os.getenv("BQ_DATASET") or ""
MAX_BLOCK_RANGE = int(os.getenv("ETH_SYNC_MAX_BLOCKS", "0"))  # 0 = no limit

SOURCE_TABLES = {
    "transactions": "bigquery-public-data.crypto_ethereum.transactions",
    "logs": "bigquery-public-data.crypto_ethereum.logs",
    "token_transfers": "bigquery-public-data.crypto_ethereum.token_transfers",
    "traces": "bigquery-public-data.crypto_ethereum.traces",
}

DEST_TABLES = {
    "transactions": os.getenv("BQ_TRANSACTIONS_TABLE", "eth_transactions"),
    "logs": os.getenv("BQ_LOGS_TABLE", "eth_logs"),
    "token_transfers": os.getenv("BQ_TOKEN_TRANSFERS_TABLE", "eth_token_transfers"),
    "traces": os.getenv("BQ_TRACES_TABLE", "eth_traces"),
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_last_synced_block(client: bigquery.Client, dataset: str, table: str) -> int:
    """Return the maximum block_number already ingested."""
    query = f"SELECT MAX(block_number) AS max_block FROM `{client.project}.{dataset}.{table}`"
    try:
        result = client.query(query).result()
        row = next(iter(result), None)
        return int(row.max_block) if row and row.max_block is not None else 0
    except Exception:  # pragma: no cover - network/credentials issues
        logging.exception("Failed to fetch last synced block for %s", table)
        return 0


def extract_new_data(
    client: bigquery.Client,
    source_table: str,
    last_block: int,
    block_range: int | None = None,
) -> Tuple[pd.DataFrame, int]:
    """Query new rows from the public dataset."""
    where = f"block_number > {last_block}"
    if block_range and block_range > 0:
        where += f" AND block_number <= {last_block + block_range}"
    query = f"SELECT * FROM `{source_table}` WHERE {where} ORDER BY block_number"
    job = client.query(query)
    df = job.result().to_dataframe()
    bytes_proc = job.total_bytes_processed or 0
    return df, bytes_proc


def ingest_data(client: bigquery.Client, df: pd.DataFrame, dataset: str, table: str) -> None:
    """Append dataframe to destination table."""
    if df.empty:
        logging.info("No new data for %s", table)
        return

    table_ref = f"{client.project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Ingesting %d rows into %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


def sync_table(client: bigquery.Client, name: str, total_bytes: int) -> int:
    """Sync a single source table and return bytes processed."""
    source = SOURCE_TABLES[name]
    dest = DEST_TABLES[name]
    last_block = get_last_synced_block(client, DEST_DATASET, dest)
    logging.info("%s last synced block: %s", name, last_block)

    df, bytes_proc = extract_new_data(client, source, last_block, MAX_BLOCK_RANGE)
    logging.info("%s query processed %.2f MB and returned %d rows", name, bytes_proc / 1_000_000, len(df))

    ingest_data(client, df, DEST_DATASET, dest)
    total_bytes += bytes_proc
    if not df.empty:
        logging.info("%s sync completed up to block %s", name, int(df['block_number'].max()))
    return total_bytes


async def run_eth_sync_worker() -> None:
    """Main entry point for incremental sync."""
    if not PROJECT_ID or not DEST_DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    bq_client = BigQueryClient(PROJECT_ID)
    bq_client.ensure_dataset_exists(DEST_DATASET)
    client = bq_client.client

    total_bytes = 0
    for name in SOURCE_TABLES:
        try:
            total_bytes = sync_table(client, name, total_bytes)
        except Exception:  # pragma: no cover - network/credentials issues
            logging.exception("Failed to sync table %s", name)

    logging.info("Total bytes processed: %.2f MB", total_bytes / 1_000_000)


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_eth_sync_worker())
