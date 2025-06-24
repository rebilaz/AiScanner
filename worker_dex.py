import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict

import pandas as pd
from dotenv import load_dotenv

from gcp_utils import BigQueryClient
from dex_clients.thegraph import TheGraphClient

REQUIRED_COLUMNS = [
    "ingestion_timestamp",
    "event_timestamp",
    "transaction_hash",
    "dex_source",
    "pair",
    "price_usd",
    "volume_usd",
    "token0_symbol",
    "token1_symbol",
    "amount0",
    "amount1",
]


def _validate_df(df: pd.DataFrame) -> bool:
    """Basic validation of the swaps dataframe before upload."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        logging.error("Missing columns in swap data: %s", ", ".join(missing))
        return False
    if df[REQUIRED_COLUMNS].isnull().any().any():
        logging.error("Null values detected in swap data")
        return False
    return True


async def run_dex_worker() -> None:
    """Collect swaps from Uniswap V3 and upload them to BigQuery."""
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("DEX_BIGQUERY_TABLE", "dex_market_data")
    endpoint = os.getenv(
        "THEGRAPH_UNISWAP_ENDPOINT",
        "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
    )

    if not project_id or not dataset:
        logging.error("Missing GCP configuration for DEX worker")
        return

    client = TheGraphClient(endpoint)
    bq_client = BigQueryClient(project_id)
    bq_client.ensure_dataset_exists(dataset)

    now = datetime.now(tz=timezone.utc)
    end = int(now.timestamp())
    start = int((now - timedelta(hours=1)).timestamp())

    swaps = await client.fetch_swaps(start, end)
    if not swaps:
        logging.info("No swaps fetched for the period")
        return

    records: list[Dict[str, Any]] = []
    for s in swaps:
        try:
            amount0 = float(s.get("amount0", 0))
            amount1 = float(s.get("amount1", 0))
            amount_usd = float(s.get("amountUSD")) if s.get("amountUSD") else None
            if amount_usd is None:
                # fallback price using sqrtPriceX96 if amountUSD missing
                # price = sqrtPriceX96^2 / 2^192
                price = (int(s.get("sqrtPriceX96")) ** 2) / (2 ** 192)
                amount_usd = abs(amount0) * price if amount0 != 0 else abs(amount1) * price
            price_usd = amount_usd / abs(amount0) if amount0 != 0 else amount_usd / abs(amount1)
            records.append(
                {
                    "ingestion_timestamp": datetime.now(tz=timezone.utc),
                    "event_timestamp": datetime.fromtimestamp(int(s["timestamp"]), tz=timezone.utc),
                    "transaction_hash": s["transaction"]["id"],
                    "dex_source": "uniswap_v3",
                    "pair": f"{s['pool']['token0']['symbol']}/{s['pool']['token1']['symbol']}",
                    "price_usd": price_usd,
                    "volume_usd": amount_usd,
                    "token0_symbol": s["pool"]["token0"]["symbol"],
                    "token1_symbol": s["pool"]["token1"]["symbol"],
                    "amount0": amount0,
                    "amount1": amount1,
                }
            )
        except Exception as exc:
            logging.error("Failed to parse swap: %s", exc)

    df = pd.DataFrame(records)
    if df.empty:
        logging.info("No valid swap data to upload")
        return

    df = df.astype(
        {
            "ingestion_timestamp": "datetime64[ns, UTC]",
            "event_timestamp": "datetime64[ns, UTC]",
            "transaction_hash": str,
            "dex_source": str,
            "pair": str,
            "price_usd": float,
            "volume_usd": float,
            "token0_symbol": str,
            "token1_symbol": str,
            "amount0": float,
            "amount1": float,
        }
    )

    if not _validate_df(df):
        logging.error("Validation failed; aborting upload")
        return

    bq_client.upload_dataframe(df, dataset, table)
    logging.info("DEX worker completed with %d swaps", len(df))


if __name__ == "__main__":
    asyncio.run(run_dex_worker())
