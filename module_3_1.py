"""Telegram alert module reading BigQuery tables and sending a summary."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from google.cloud import bigquery
from telegram import Bot

from gcp_utils import create_bq_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
MARKET_TABLE = os.getenv("BQ_MARKET_DECISION_TABLE", "market_decision_outputs")
ANOMALY_TABLE = os.getenv("BQ_ANOMALY_TABLE", "anomaly_alerts_onchain")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def _get_row_count(client: bigquery.Client, table: str) -> int:
    """Return the number of rows for a table, or 0 on failure."""
    try:
        table_ref = f"{client.project}.{DATASET}.{table}"
        tbl = client.get_table(table_ref)
        logging.info("Row count for %s: %d", table_ref, tbl.num_rows)
        return int(tbl.num_rows)
    except Exception as exc:  # pragma: no cover - BigQuery access issues
        logging.error("Failed to fetch row count for %s: %s", table, exc)
        return 0


def compose_summary(signals: int, anomalies: int, incidents: Optional[int] = None) -> str:
    """Create the Telegram message summarizing pipeline results."""
    parts = [f"\U0001F4CA Scan terminé ! {signals} signaux", f"{anomalies} anomalies"]
    if incidents is not None:
        parts.append(f"{incidents} incident(s) on-chain")
    return ", ".join(parts) + "."


async def _send_async(token: str, chat_id: str, text: str) -> None:
    bot = Bot(token)
    await bot.send_message(chat_id=chat_id, text=text)


def send_telegram_message(token: str, chat_id: str, text: str) -> None:
    """Send a Telegram message, compatible with sync or async contexts."""
    async def runner() -> None:
        await _send_async(token, chat_id, text)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(runner())
    else:  # pragma: no cover - running within event loop
        loop.create_task(runner())


def alert_from_bigquery() -> None:
    """Fetch table counts from BigQuery and send a summary Telegram alert."""
    if not all([PROJECT_ID, DATASET, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        logging.error("Missing configuration for BigQuery or Telegram")
        return

    client = create_bq_client(PROJECT_ID)
    signals = _get_row_count(client, MARKET_TABLE)
    anomalies = _get_row_count(client, ANOMALY_TABLE)

    incidents: Optional[int] = None
    try:
        query = (
            f"SELECT COUNT(1) AS c FROM `{client.project}.{DATASET}.{ANOMALY_TABLE}` "
            "WHERE LOWER(CAST(is_incident AS STRING))='true'"
        )
        result = client.query(query).result()
        incidents = int(list(result)[0].c)
        logging.info("Incident count for %s: %d", ANOMALY_TABLE, incidents)
    except Exception:
        logging.info("No incident information available")

    message = compose_summary(signals, anomalies, incidents)
    send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, message)
    logging.info("Telegram alert sent")


if __name__ == "__main__":
    alert_from_bigquery()
