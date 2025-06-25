"""Identify whales and smart money addresses from blockchain data."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from google.cloud import bigquery
from gcp_utils import create_bq_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
TOKEN_TRANSFERS_TABLE = os.getenv("BQ_TOKEN_TRANSFERS_TABLE", "eth_token_transfers")
TRANSACTIONS_TABLE = os.getenv("BQ_TRANSACTIONS_TABLE", "eth_transactions")
PRICES_TABLE = os.getenv("BQ_PRICES_TABLE", "token_prices")
LABELS_TABLE = os.getenv("BQ_LABELED_ADDRESSES_TABLE", "labeled_addresses")

WHALE_USD_THRESHOLD = float(os.getenv("WHALE_USD_THRESHOLD", "1000000"))
SMART_MONEY_SCORE_THRESHOLD = int(os.getenv("SMART_MONEY_SCORE_THRESHOLD", "3"))

# ---------------------------------------------------------------------------
# Whale detection
# ---------------------------------------------------------------------------

def identify_whales(client: bigquery.Client) -> pd.DataFrame:
    """Return addresses whose portfolio value exceeds the threshold."""
    query = f"""
        WITH transfers AS (
            SELECT
                LOWER(from_address) AS from_addr,
                LOWER(to_address) AS to_addr,
                LOWER(token_address) AS token_address,
                CAST(value AS FLOAT64) AS value
            FROM `{client.project}.{DATASET}.{TOKEN_TRANSFERS_TABLE}`
        ),
        balances AS (
            SELECT to_addr AS address, token_address, SUM(value) AS qty
            FROM transfers
            GROUP BY address, token_address
            UNION ALL
            SELECT from_addr AS address, token_address, SUM(-value) AS qty
            FROM transfers
            GROUP BY address, token_address
        ),
        agg AS (
            SELECT address, token_address, SUM(qty) AS balance
            FROM balances
            GROUP BY address, token_address
        ),
        prices AS (
            SELECT token_address, price_usd
            FROM (
                SELECT token_address, price_usd,
                       ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY price_timestamp DESC) AS rn
                FROM `{client.project}.{DATASET}.{PRICES_TABLE}`
            )
            WHERE rn = 1
        ),
        portfolio AS (
            SELECT a.address, SUM(a.balance * p.price_usd) AS portfolio_usd_value
            FROM agg a
            JOIN prices p ON a.token_address = p.token_address
            GROUP BY a.address
        )
        SELECT address, portfolio_usd_value
        FROM portfolio
        WHERE portfolio_usd_value > {WHALE_USD_THRESHOLD}
    """
    logging.info("Running whale detection query")
    return client.query(query).to_dataframe()


# ---------------------------------------------------------------------------
# Smart money detection
# ---------------------------------------------------------------------------

def identify_smart_money(client: bigquery.Client) -> pd.DataFrame:
    """Return addresses that accumulated tokens before a price pump."""
    query = f"""
        WITH current_price AS (
            SELECT token_address, price_usd
            FROM (
                SELECT token_address, price_usd,
                       ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY price_timestamp DESC) AS rn
                FROM `{client.project}.{DATASET}.{PRICES_TABLE}`
            ) WHERE rn = 1
        ),
        past_price AS (
            SELECT token_address, price_usd
            FROM (
                SELECT token_address, price_usd,
                       ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY price_timestamp DESC) AS rn
                FROM `{client.project}.{DATASET}.{PRICES_TABLE}`
                WHERE price_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
            ) WHERE rn = 1
        ),
        pumps AS (
            SELECT c.token_address
            FROM current_price c
            JOIN past_price p ON c.token_address = p.token_address
            WHERE c.price_usd >= p.price_usd * 2
        ),
        pre_pump AS (
            SELECT
                LOWER(to_address) AS address,
                token_address,
                SUM(CAST(value AS FLOAT64)) AS acquired
            FROM `{client.project}.{DATASET}.{TOKEN_TRANSFERS_TABLE}`
            WHERE token_address IN (SELECT token_address FROM pumps)
              AND block_timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 21 DAY)
                                     AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
            GROUP BY address, token_address
            HAVING acquired > 0
        ),
        scores AS (
            SELECT address, COUNT(DISTINCT token_address) AS smart_money_score
            FROM pre_pump
            GROUP BY address
        )
        SELECT address, smart_money_score
        FROM scores
        WHERE smart_money_score >= {SMART_MONEY_SCORE_THRESHOLD}
    """
    logging.info("Running smart money detection query")
    return client.query(query).to_dataframe()


# ---------------------------------------------------------------------------
# Storage helper
# ---------------------------------------------------------------------------

def update_labels_table(client: bigquery.Client, whales: pd.DataFrame, smart: pd.DataFrame) -> None:
    """Merge whale and smart money labels into the destination table."""
    if whales.empty and smart.empty:
        logging.info("No labels to update")
        return

    whales = whales.assign(is_whale=True)
    smart = smart.assign(is_smart=True)
    df = pd.merge(whales, smart, on="address", how="outer")
    df["labels"] = df.apply(
        lambda row: [l for l in ["Whale" if row.get("is_whale") else None, "Smart Money" if row.get("is_smart") else None] if l],
        axis=1,
    )
    df = df.drop(columns=[c for c in ["is_whale", "is_smart"] if c in df.columns])
    df["portfolio_usd_value"] = df["portfolio_usd_value"].fillna(0)
    df["smart_money_score"] = df["smart_money_score"].fillna(0).astype(int)
    df["last_updated_timestamp"] = datetime.now(tz=timezone.utc)

    table_ref = f"{client.project}.{DATASET}.{LABELS_TABLE}"
    temp_table = f"{table_ref}_tmp_{int(datetime.now().timestamp())}"
    schema = [
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("labels", "STRING", mode="REPEATED"),
        bigquery.SchemaField("portfolio_usd_value", "FLOAT"),
        bigquery.SchemaField("smart_money_score", "INTEGER"),
        bigquery.SchemaField("last_updated_timestamp", "TIMESTAMP"),
    ]
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
    logging.info("Uploading %d labeled addresses to temporary table %s", len(df), temp_table)
    client.load_table_from_dataframe(df, temp_table, job_config=job_config).result()

    merge_query = f"""
        MERGE `{table_ref}` T
        USING `{temp_table}` S
        ON T.address = S.address
        WHEN MATCHED THEN
          UPDATE SET
            labels = S.labels,
            portfolio_usd_value = S.portfolio_usd_value,
            smart_money_score = S.smart_money_score,
            last_updated_timestamp = S.last_updated_timestamp
        WHEN NOT MATCHED THEN
          INSERT (address, labels, portfolio_usd_value, smart_money_score, last_updated_timestamp)
          VALUES(S.address, S.labels, S.portfolio_usd_value, S.smart_money_score, S.last_updated_timestamp)
    """
    logging.info("Merging results into %s", table_ref)
    client.query(merge_query).result()
    client.delete_table(temp_table, not_found_ok=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run_address_labeler_worker() -> None:
    """Entry point for labeling whales and smart money addresses."""
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    client = create_bq_client(PROJECT_ID)
    try:
        whales = identify_whales(client)
        smart = identify_smart_money(client)
        update_labels_table(client, whales, smart)
    except Exception:  # pragma: no cover - network/credentials issues
        logging.exception("Address labeler worker failed")


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_address_labeler_worker())
