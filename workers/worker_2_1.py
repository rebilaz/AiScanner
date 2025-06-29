import os
import asyncio
import logging
import socket
from datetime import datetime, timezone
from typing import Dict, List, Any

import pandas as pd
import aiohttp
from dotenv import load_dotenv

from clients.coingecko import CoinGeckoClient
from gcp_utils import BigQueryClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_existing_ids(bq_client: BigQueryClient, project_id: str, dataset: str, table: str) -> set:
    logging.info("Récupération des IDs existants depuis BigQuery...")
    query = f"SELECT DISTINCT id_projet FROM `{project_id}.{dataset}.{table}`"
    try:
        results = bq_client.client.query(query)
        existing_ids = {row["id_projet"] for row in results}
        logging.info(f"{len(existing_ids)} IDs existants trouvés.")
        return existing_ids
    except Exception as e:
        logging.warning(f"Impossible de récupérer les IDs existants (la table est peut-être vide) : {e}")
        return set()

def _extract_data(raw: dict, now_utc: str) -> dict:
    if not raw or not isinstance(raw, dict):
        return {}
    roi_raw = raw.get("roi") or {}
    roi = {
        "times": roi_raw.get("times") if roi_raw else None,
        "currency": roi_raw.get("currency") if roi_raw else None,
        "percentage": roi_raw.get("percentage") if roi_raw else None,
    }
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
        "roi": roi,
        "last_updated": raw.get("last_updated"),
        "chaine_contrat": next(iter(raw.get("platforms", {}) or {}), None),
        "adresse_contrat": next(iter((raw.get("platforms", {}) or {}).values()), None),
        "lien_site_web": (raw.get("links", {}).get("homepage") or [None])[0],
        "lien_github": (raw.get("links", {}).get("repos_url", {}).get("github") or [None])[0],
        "derniere_maj": now_utc,
    }

def prepare_dataframe_for_bq(df: pd.DataFrame) -> pd.DataFrame:
    float_cols = [
        "prix_usd", "volume_24h", "high_24h", "low_24h", "price_change_24h",
        "variation_24h_pct", "market_cap_change_24h", "market_cap_change_percentage_24h",
        "circulating_supply", "total_supply", "max_supply", "ath_usd",
        "ath_change_pct", "atl", "atl_change_percentage",
    ]
    int_cols = ["market_cap", "fully_diluted_valuation", "market_cap_rank"]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    if 'roi' in df.columns:
        df['roi'] = df['roi'].apply(lambda x: str(x) if x is not None else None)
    return df

async def run_coingecko_worker() -> None:
    logging.info("\n--- WORKER COINGECKO : DÉMARRAGE ---\n")
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("COINGECKO_BIGQUERY_TABLE")
    category = os.getenv("COINGECKO_CATEGORY")
    if not project_id:
        raise ValueError("La variable d'environnement GCP_PROJECT_ID est manquante.")
    if not dataset:
        raise ValueError("La variable d'environnement BQ_DATASET est manquante.")
    if not table:
        raise ValueError("La variable d'environnement COINGECKO_BIGQUERY_TABLE est manquante.")
    if not category:
        raise ValueError("La variable d'environnement COINGECKO_CATEGORY est manquante.")
    min_cap = int(os.getenv("COINGECKO_MIN_MARKET_CAP", "0"))
    min_vol = int(os.getenv("COINGECKO_MIN_VOLUME_USD", "0"))
    batch_size = int(os.getenv("COINGECKO_BATCH_SIZE", "50"))
    rate_limit = 5
    logging.info(f"Configuration chargée : project={project_id}, dataset={dataset}, table={table}, category='{category}'")
    bq_client = BigQueryClient(project_id)
    logging.info("Client BigQuery initialisé.")

    try:
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            client = CoinGeckoClient(session, rate_limit=rate_limit)
            logging.info(f"\nAppel à l'API CoinGecko pour la catégorie : '{category}'...")
            market = await client.list_tokens(category)
            if not market:
                logging.info("Aucun token retourné par l'API.")
                return
            logging.info(f"--> API a retourné {len(market)} tokens.")
            df_market = pd.DataFrame(market)
            df_market["market_cap"] = pd.to_numeric(df_market["market_cap"], errors="coerce").fillna(0)
            df_market["total_volume"] = pd.to_numeric(df_market["total_volume"], errors="coerce").fillna(0)
            logging.info(f"\nFiltrage avec les critères : market_cap >= {min_cap} et total_volume >= {min_vol}...")
            df_filtered = df_market[(df_market["market_cap"] >= min_cap) & (df_market["total_volume"] >= min_vol)]
            if df_filtered.empty:
                logging.info("Aucun token ne correspond aux critères de filtre.")
                return
            existing_ids = fetch_existing_ids(bq_client, project_id, dataset, table)
            token_ids_to_fetch = [t for t in df_filtered["id"].tolist() if t not in existing_ids]
            if not token_ids_to_fetch:
                logging.info("Aucun nouveau token à traiter.")
                return
            logging.info(f"{len(token_ids_to_fetch)} nouveaux tokens à traiter.")

            now_utc = datetime.now(timezone.utc).isoformat()
            all_records: List[Dict[str, Any]] = []

            for i, token_id in enumerate(token_ids_to_fetch):
                if i > 0:
                    await asyncio.sleep(12)  # 12s d'attente entre chaque appel
                logging.info(f"Traitement du token {i+1}/{len(token_ids_to_fetch)}: {token_id}")
                try:
                    res = await client.token_details(token_id)
                    if isinstance(res, dict):
                        all_records.append(_extract_data(res, now_utc))
                    else:
                        logging.warning(f"Aucun détail valide reçu pour {token_id} après les tentatives.")
                except Exception as e:
                    logging.error(f"Échec final de la récupération des détails pour {token_id}: {e}")

            if not all_records:
                logging.warning("Aucun enregistrement n'a pu être créé.")
                return
            logging.info(f"\nPréparation de l'envoi de {len(all_records)} enregistrements vers BigQuery...")
            for i in range(0, len(all_records), batch_size):
                batch = all_records[i:i + batch_size]
                logging.info(f"Envoi du lot {i//batch_size + 1} ({len(batch)} enregistrements)...")
                df_batch = pd.DataFrame(batch)
                df_batch = prepare_dataframe_for_bq(df_batch)
                try:
                    bq_client.upload_dataframe(df_batch, dataset, table, write_disposition="WRITE_APPEND")
                except Exception as upload_error:
                    logging.error(f"Erreur lors de l'envoi du lot à BigQuery : {upload_error}")
            logging.info("\n--- WORKER COINGECKO : TERMINÉ AVEC SUCCÈS ---")
    except Exception as e:
        logging.critical(f"Une erreur fatale a arrêté le worker : {e}", exc_info=True)

if __name__ == "__main__":
    try:
        asyncio.run(run_coingecko_worker())
    except KeyboardInterrupt:
        logging.info("Arrêt manuel du worker.")
