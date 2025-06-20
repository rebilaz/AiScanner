"""Worker aggregating price/volume data from CEXs."""
from __future__ import annotations

import asyncio
import logging
from typing import Iterable

import pandas as pd

import config
from clients import BinanceClient
from gcp_utils import upload_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def fetch_exchange_data(client: BinanceClient, pairs: Iterable[str], interval: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for pair in pairs:
        try:
            df = await client.get_ohlcv(pair, interval)
            frames.append(df)
            logger.info("%s: fetched %d rows for %s", client.__class__.__name__, len(df), pair)
        except Exception as exc:
            logger.error("Failed to fetch %s for %s: %s", interval, pair, exc)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


async def run() -> None:
    """Run the CEX aggregation worker."""
    pairs = getattr(config, "CEX_PAIRS", "BTC/USDT").split(",")
    interval = getattr(config, "CEX_INTERVAL", "1m")
    table_id = f"{config.BIGQUERY_PROJECT_ID}.{config.BIGQUERY_DATASET_ID}.{config.BIGQUERY_MASTER_TABLE}"

    binance = BinanceClient()
    df = await fetch_exchange_data(binance, pairs, interval)
    await binance.close()

    if not df.empty:
        upload_dataframe(df, table_id)
    else:
        logger.warning("No data collected from exchanges")


if __name__ == "__main__":
    asyncio.run(run())
