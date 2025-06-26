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


def fetch_existing_ids(bq_client, project_id, dataset, table) -> set:
    """Retourne un set de tous les id_projet déjà présents dans BigQuery."""
    query = f"SELECT DISTINCT id_projet FROM `{project_id}.{dataset}.{table}`"
    results = bq_client.client.query(query)
    return {row["id_projet"] for row in results}


def _extract_data(raw: dict, now_utc: str) -> dict:
    roi_raw = raw.get("roi") or {}
    roi = {
        "times": roi_raw.get("times"),
        "currency": roi_raw.get("currency"),
        "percentage": roi_raw.get("percentage"),
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


<<<<<<< ifkvoe-codex/analyser-blocage-api-coingecko-et-solutions
def force_float(df: pd.DataFrame, float_cols=None, int_cols=None) -> pd.DataFrame:
    """Cast specified columns to float or integer types."""
    if float_cols is None:
        float_cols = [
            "prix_usd",
            "fully_diluted_valuation",
            "volume_24h",
            "high_24h",
            "low_24h",
            "price_change_24h",
            "variation_24h_pct",
            "market_cap_change_24h",
            "market_cap_change_percentage_24h",
            "circulating_supply",
            "total_supply",
            "max_supply",
            "ath_usd",
            "ath_change_pct",
            "atl",
            "atl_change_percentage",
        ]
    if int_cols is None:
        int_cols = ["market_cap", "fully_diluted_valuation"]

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    for col in int_cols:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .round()
                .astype("Int64")
            )
    return df
=======

def force_float(df: pd.DataFrame, float_cols=None, int_cols=None) -> pd.DataFrame:
    """Cast specified columns to float or integer types."""
    if float_cols is None:
        float_cols = [
            "prix_usd",
            "fully_diluted_valuation",
            "volume_24h",
            "high_24h",
            "low_24h",
            "price_change_24h",
            "variation_24h_pct",
            "market_cap_change_24h",
            "market_cap_change_percentage_24h",
            "circulating_supply",
            "total_supply",
            "max_supply",
            "ath_usd",
            "ath_change_pct",
            "atl",
            "atl_change_percentage",
        ]
    if int_cols is None:
        int_cols = ["market_cap", "fully_diluted_valuation"]

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df
>>>>>>> main



async def run_coingecko_worker() -> None:
    logging.info("\n--- WORKER COINGECKO : DÉMARRAGE ---\n")
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("La variable d'environnement GCP_PROJECT_ID est manquante.")

    dataset = os.getenv("BQ_DATASET")
    if not dataset:
        raise ValueError("La variable d'environnement BQ_DATASET est manquante.")
    
    table = os.getenv("COINGECKO_BIGQUERY_TABLE")
    if not table:
        raise ValueError("La variable d'environnement COINGECKO_BIGQUERY_TABLE est manquante.")

    category = os.getenv("COINGECKO_CATEGORY")
    if not category:
        raise ValueError("La variable d'environnement COINGECKO_CATEGORY est manquante.")

<<<<<<< ifkvoe-codex/analyser-blocage-api-coingecko-et-solutions
    min_cap = int(os.getenv("COINGECKO_MIN_MARKET_CAP", "0"))
    min_vol = int(os.getenv("COINGECKO_MIN_VOLUME_USD", "0"))
    batch_size = int(os.getenv("COINGECKO_BATCH_SIZE", "20"))
    rate_limit = int(os.getenv("COINGECKO_RATE_LIMIT", "50"))
=======

    min_cap = int(os.getenv("COINGECKO_MIN_MARKET_CAP", "0"))
    min_vol = int(os.getenv("COINGECKO_MIN_VOLUME_USD", "0"))
    batch_size = int(os.getenv("COINGECKO_BATCH_SIZE", "20"))
    rate_limit = int(os.getenv("COINGECKO_RATE_LIMIT", "50"))

>>>>>>> main
    
    logging.info(f"Configuration chargée : project={project_id}, dataset={dataset}, table={table}, category='{category}'")

    bq_client = BigQueryClient(project_id)
    logging.info("Client BigQuery initialisé.")

    # Cherche les IDs déjà présents
    existing_ids = fetch_existing_ids(bq_client, project_id, dataset, table)
    logging.info(f"{len(existing_ids)} tokens déjà présents dans la table BigQuery.")

    try:
<<<<<<< ifkvoe-codex/analyser-blocage-api-coingecko-et-solutions
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            client = CoinGeckoClient(session, rate_limit=rate_limit)
=======
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        async with aiohttp.ClientSession(connector=connector) as session:
            client = CoinGeckoClient(session, rate_limit=rate_limit)

>>>>>>> main
            logging.info(f"\nAppel à l'API CoinGecko pour la catégorie : '{category}'...")
            market = await client.list_tokens(category)
            
            if not market:
                logging.info(f"\n--> INFORMATION : L'API CoinGecko n'a retourné aucun token pour la catégorie '{category}'. Le worker se termine normalement.\n")
                return

            logging.info(f"--> API a retourné {len(market)} tokens.")
            
            df_market = pd.DataFrame(market)
            df_market["market_cap"] = pd.to_numeric(df_market["market_cap"], errors="coerce").fillna(0)
            df_market["total_volume"] = pd.to_numeric(df_market["total_volume"], errors="coerce").fillna(0)
            
            logging.info(f"\nFiltrage avec les critères : market_cap >= {min_cap} et total_volume >= {min_vol}...")
            df_filtered = df_market[(df_market["market_cap"] >= min_cap) & (df_market["total_volume"] >= min_vol)]

            if df_filtered.empty:
                logging.info(f"\n--> INFORMATION : Aucun token ne correspond aux critères de filtre. Le worker se termine normalement.\n")
                return
                
            # On retire ceux déjà présents en base
            token_ids_to_fetch = [t for t in df_filtered["id"].tolist() if t not in existing_ids]
            logging.info(f"{len(token_ids_to_fetch)} tokens à traiter (hors doublons déjà en base).")

            records: List[Dict[str, Any]] = []

            for i, token_id in enumerate(token_ids_to_fetch):
                logging.info(f"  [{i+1}/{len(token_ids_to_fetch)}] Récupération des détails pour : {token_id}")
                try:
                    detail = await client.token_details(token_id)
                    if not detail:
                        logging.warning(f"  --> AVERTISSEMENT : Pas de détails pour {token_id}. On passe au suivant.")
                        continue
                    
                    now_utc = datetime.now(timezone.utc).isoformat()
                    records.append(_extract_data(detail, now_utc))
                    
                    if len(records) >= batch_size:
                        df = pd.DataFrame(records)
                        df = force_float(df)
                        logging.info(f"  --> LOT PRÊT ({len(df)} lignes). Envoi vers BigQuery...")
                        bq_client.upload_dataframe(df, dataset, table)
                        logging.info("  --> LOT ENVOYÉ.")
                        records = []
                        await asyncio.sleep(7)
                    
                    await asyncio.sleep(7)  # Respecter le rate limit de l'API
                except Exception as e:
                    logging.error(f"  !!! ERREUR LORS DU TRAITEMENT de {token_id}: {e}. On passe au suivant.")
                    continue
                
            if records:
                df = pd.DataFrame(records)
                df = force_float(df)
                logging.info(f"\n--> DERNIER LOT : Envoi des {len(df)} lignes restantes vers BigQuery...")
                bq_client.upload_dataframe(df, dataset, table)
                logging.info("--> DERNIER LOT ENVOYÉ.")
            
            logging.info("\n--- WORKER COINGECKO : TERMINÉ AVEC SUCCÈS ---\n")

    except aiohttp.ClientConnectorError as e:
        logging.error("!!! ERREUR DE CONNEXION RÉSEAU : Impossible de se connecter à l'API. Vérifiez votre connexion internet ou le statut de WSL.")
        logging.error(f"Détail de l'erreur : {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(run_coingecko_worker())
    except Exception as e:
        logging.error(f"Une erreur fatale a arrêté le worker : {e}", exc_info=False)
        exit(1)
