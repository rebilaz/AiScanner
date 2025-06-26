# Fichier : workers/worker_2_1.py

import os
import asyncio
import logging
import socket  # Nécessaire pour le correctif réseau IPv4
from datetime import datetime, timezone
from typing import Dict, List, Any

import pandas as pd
import aiohttp
from dotenv import load_dotenv

# Assurez-vous que ces fichiers sont importables
from clients.coingecko import CoinGeckoClient
from gcp_utils import BigQueryClient

# Configuration du logging pour des messages clairs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _extract_data(raw: dict, now_utc: str) -> dict:
    """Extrait et formate les données brutes de l'API pour BigQuery."""
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
        "roi": str(roi),
        "last_updated": raw.get("last_updated"),
        "chaine_contrat": next(iter(raw.get("platforms", {}) or {}), None),
        "adresse_contrat": next(iter((raw.get("platforms", {}) or {}).values()), None),
        "lien_site_web": (raw.get("links", {}).get("homepage") or [None])[0],
        "lien_github": (raw.get("links", {}).get("repos_url", {}).get("github") or [None])[0],
        "derniere_maj": now_utc,
    }

async def run_coingecko_worker() -> None:
    """Fetch token data from CoinGecko and upload to BigQuery."""
    logging.info("\n--- WORKER COINGECKO : DÉMARRAGE ---\n")
    load_dotenv()

    # --- Étape 1: Chargement et validation de la configuration ---
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

    min_cap = int(os.getenv("COINGECKO_MIN_MARKET_CAP", "0"))
    min_vol = int(os.getenv("COINGECKO_MIN_VOLUME_USD", "0"))
    batch_size = int(os.getenv("COINGECKO_BATCH_SIZE", "20"))
    
    logging.info(f"Configuration chargée : project={project_id}, dataset={dataset}, table={table}, category='{category}'")

    # --- Étape 2: Initialisation des clients ---
    bq_client = BigQueryClient(project_id)
    logging.info("Client BigQuery initialisé.")
    
    # --- Étape 3: Récupération des données ---
    try:
        # --- CORRECTIF RÉSEAU WSL2 ---
        # Crée le connecteur qui force l'utilisation de l'IPv4
        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        
        # On passe ce connecteur à la ClientSession
        async with aiohttp.ClientSession(connector=connector) as session:
            client = CoinGeckoClient(session)
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
                
            logging.info(f"--> {len(df_filtered)} tokens restants après filtrage. Lancement de la boucle de récupération des détails...\n")

            records: List[Dict[str, Any]] = []
            token_ids_to_fetch = df_filtered["id"].tolist()

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
                        logging.info(f"  --> LOT PRÊT ({len(records)} lignes). Envoi vers BigQuery...")
                        bq_client.upload_dataframe(pd.DataFrame(records), dataset, table)
                        logging.info("  --> LOT ENVOYÉ.")
                        records = []
                        await asyncio.sleep(2)
                    
                    await asyncio.sleep(2)
                except Exception as e:
                    logging.error(f"  !!! ERREUR LORS DU TRAITEMENT de {token_id}: {e}. On passe au suivant.")
                    continue
                
            if records:
                logging.info(f"\n--> DERNIER LOT : Envoi des {len(records)} lignes restantes vers BigQuery...")
                bq_client.upload_dataframe(pd.DataFrame(records), dataset, table)
                logging.info("--> DERNIER LOT ENVOYÉ.")
            
            logging.info("\n--- WORKER COINGECKO : TERMINÉ AVEC SUCCÈS ---\n")

    except aiohttp.ClientConnectorError as e:
        logging.error("!!! ERREUR DE CONNEXION RÉSEAU : Impossible de se connecter à l'API. Vérifiez votre connexion internet ou le statut de WSL.")
        logging.error(f"Détail de l'erreur : {e}")
        raise # On propage l'erreur pour que Airflow la voie comme un échec

if __name__ == "__main__":
    try:
        asyncio.run(run_coingecko_worker())
    except Exception as e:
        logging.error(f"Une erreur fatale a arrêté le worker : {e}", exc_info=False)
        exit(1)