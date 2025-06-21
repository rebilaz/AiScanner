import asyncio
import logging
import os
from datetime import datetime, timezone

import aiohttp
import ssl
import pandas as pd
from dotenv import load_dotenv

from gcp_utils import BigQueryClient
from clients.binance import BinanceClient
from clients.kraken import KrakenClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def _normalize_ohlcv(df: pd.DataFrame, pair: str, source: str, interval: str) -> pd.DataFrame:
    """Normalize OHLCV data to the unified BigQuery schema."""
    df = df.copy()
    df["ingestion_timestamp"] = pd.Timestamp(datetime.now(tz=timezone.utc))
    df["trading_pair"] = pair
    df["exchange_source"] = source
    df["granularity"] = interval
    return df[
        [
            "ingestion_timestamp",
            "event_timestamp",
            "trading_pair",
            "exchange_source",
            "granularity",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ]


async def gather_ohlcv(clients: list[tuple[str, object]], pairs: list[str], interval: str) -> pd.DataFrame:
    tasks = []
    meta = []
    for pair in pairs:
        for name, client in clients:
            tasks.append(client.get_ohlcv(pair, interval))
            meta.append((pair, name))
    results = await asyncio.gather(*tasks)
    frames = []
    for (pair, name), df in zip(meta, results):
        if df is None:
            logging.warning(
                f"Aucune donn\u00e9e re\u00e7ue de {name} pour la paire {pair}, probablement en raison d'une erreur de connexion permanente."
            )
            continue
        frames.append(_normalize_ohlcv(df, pair, name, interval))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


async def main() -> None:
    """Entry point for the asynchronous CEX worker."""
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("BQ_TABLE")
    https_proxy = os.getenv("HTTPS_PROXY")
    cert_file = os.getenv("SSL_CERT_FILE")
    pairs = [p.strip() for p in os.getenv("TRADING_PAIRS", "").split(",") if p]
    interval = os.getenv("INTERVAL", "1m")
    binance_rate = int(os.getenv("BINANCE_RATE_LIMIT", "5"))
    kraken_rate = int(os.getenv("KRAKEN_RATE_LIMIT", "5"))
    playwright_ws = os.getenv("PLAYWRIGHT_WS")
    enabled_clients_str = os.getenv("ENABLED_CLIENTS", "Binance,Kraken")
    enabled_clients = {client.strip().lower() for client in enabled_clients_str.split(',')}

    if not project_id or not dataset or not table or not pairs:
        logging.error("Missing required environment configuration")
        return

    bq_client = BigQueryClient(project_id)
    bq_client.ensure_dataset_exists(dataset)

    ssl_context = None
    if cert_file:
        ssl_context = ssl.create_default_context(cafile=cert_file)

    connector = aiohttp.TCPConnector(ssl=ssl_context)

    async with aiohttp.ClientSession(connector=connector) as session:
        clients = []
        if "binance" in enabled_clients:
            clients.append(
                (
                    "Binance",
                    BinanceClient(
                        session,
                        rate_limit=binance_rate,
                        proxy=https_proxy,
                        browser_ws=playwright_ws,
                    ),
                )
            )
        if "kraken" in enabled_clients:
            clients.append(
                (
                    "Kraken",
                    KrakenClient(
                        session,
                        rate_limit=kraken_rate,
                        proxy=https_proxy,
                        browser_ws=playwright_ws,
                    ),
                )
            )
        logging.info("Starting data collection for %s", ", ".join(pairs))
        df = await gather_ohlcv(clients, pairs, interval)
        if not df.empty:
            bq_client.upload_dataframe(df, dataset, table)
        logging.info("Data collection completed")


if __name__ == "__main__":
    asyncio.run(main())
