import asyncio
import logging
import os
from datetime import datetime, timezone

import aiohttp
import pandas as pd
from dotenv import load_dotenv

from clients.coingecko import CoinGeckoClient
from gcp_utils import BigQueryClient


def _extract_data(raw: dict, now_utc: str) -> dict:
    roi = raw.get("roi") or {}
    md = raw.get("market_data") or {}
    return {
        "id_projet": raw.get("id"),
        "symbole": raw.get("symbol", "").upper(),
        "nom": raw.get("name"),
        "image": raw.get("image", {}).get("large"),
        "prix_usd": md.get("current_price", {}).get("usd"),
        "market_cap": md.get("market_cap", {}).get("usd"),
        "market_cap_rank": raw.get("market_cap_rank"),
        "fully_diluted_valuation": md.get("fully_diluted_valuation", {}).get("usd"),
        "volume_24h": md.get("total_volume", {}).get("usd"),
        "high_24h": md.get("high_24h", {}).get("usd"),
        "low_24h": md.get("low_24h", {}).get("usd"),
        "price_change_24h": md.get("price_change_24h"),
        "variation_24h_pct": md.get("price_change_percentage_24h"),
        "market_cap_change_24h": md.get("market_cap_change_24h"),
        "market_cap_change_percentage_24h": md.get("market_cap_change_percentage_24h"),
        "circulating_supply": md.get("circulating_supply"),
        "total_supply": md.get("total_supply"),
        "max_supply": md.get("max_supply"),
        "ath_usd": md.get("ath", {}).get("usd"),
        "ath_change_pct": md.get("ath_change_percentage", {}).get("usd"),
        "ath_date": md.get("ath_date", {}).get("usd"),
        "atl": md.get("atl", {}).get("usd"),
        "atl_change_percentage": md.get("atl_change_percentage", {}).get("usd"),
        "atl_date": md.get("atl_date", {}).get("usd"),
        "roi": {
            "currency": roi.get("currency"),
            "percentage": roi.get("percentage"),
            "times": roi.get("times"),
        },
        "last_updated": raw.get("last_updated"),
        "chaine_contrat": next(iter(raw.get("platforms", {}) or {}), None),
        "adresse_contrat": next(iter((raw.get("platforms", {}) or {}).values()), None),
        "lien_site_web": (raw.get("links", {}).get("homepage") or [None])[0],
        "lien_github": (raw.get("links", {}).get("repos_url", {}).get("github") or [None])[0],
        "derniere_maj": now_utc,
    }


async def run_coingecko_worker() -> None:
    """Fetch token data from CoinGecko and upload to BigQuery."""
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("COINGECKO_BIGQUERY_TABLE")
    category = os.getenv("COINGECKO_CATEGORY", "artificial-intelligence")
    min_cap = int(os.getenv("COINGECKO_MIN_MARKET_CAP", "0"))
    min_vol = int(os.getenv("COINGECKO_MIN_VOLUME_USD", "0"))
    batch_size = int(os.getenv("COINGECKO_BATCH_SIZE", "20"))

    if not project_id or not dataset or not table:
        logging.error("Missing GCP configuration for CoinGecko worker")
        return

    bq_client = BigQueryClient(project_id)
    bq_client.ensure_dataset_exists(dataset)

    async with aiohttp.ClientSession() as session:
        client = CoinGeckoClient(session)
        market = await client.list_tokens(category)
        if not market:
            logging.error("No tokens retrieved from CoinGecko")
            return

        df_market = pd.DataFrame(market)
        df_market["market_cap"] = pd.to_numeric(df_market["market_cap"], errors="coerce").fillna(0)
        df_market["total_volume"] = pd.to_numeric(df_market["total_volume"], errors="coerce").fillna(0)
        df_market = df_market[
            (df_market["market_cap"] >= min_cap) & (df_market["total_volume"] >= min_vol)
        ]

        records: list[dict] = []
        for token_id in df_market["id"].tolist():
            detail = await client.token_details(token_id)
            if not detail:
                continue
            now_utc = datetime.now(timezone.utc).isoformat()
            records.append(_extract_data(detail, now_utc))
            if len(records) >= batch_size:
                bq_client.upload_dataframe(pd.DataFrame(records), dataset, table)
                records = []
                await asyncio.sleep(1)
            await asyncio.sleep(1)
        if records:
            bq_client.upload_dataframe(pd.DataFrame(records), dataset, table)
