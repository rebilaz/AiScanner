"""
Worker d'analyse On-chain.
Ce script récupère les transactions de contrats, calcule des métriques de santé
et les sauvegarde dans BigQuery.
Il crée la table de destination si elle n'existe pas.
"""

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
from google.cloud import bigquery
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound

# --- Classe utilitaire BigQuery ---
class BigQueryClient:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def upload_dataframe(self, df: pd.DataFrame, dataset_id: str, table_id: str, schema: List[bigquery.SchemaField]):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=schema)
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logging.info("DataFrame chargé avec succès dans %s.%s", dataset_id, table_id)

# --- Fonctions de récupération de données ---

def get_tvl(address: str, chain: str) -> Optional[float]:
    """Récupère la TVL depuis DeFiLlama."""
    try:
        url = f"https://api.llama.fi/tvl/{chain}:{address}"
        logging.info("Appel à Llama.fi pour TVL : %s", url)
        response = requests.get(url, timeout=10)
        if response.ok:
            return float(response.json())
        logging.warning("Llama.fi a répondu avec le statut %d", response.status_code)
        return None
    except Exception as e:
        logging.error("Erreur pendant la récupération de la TVL pour %s: %s", address, e)
        return None

def calculate_advanced_metrics(
    transactions: List[Dict], project_id: str, market_cap: Optional[float], whale_usd: int
) -> Optional[Dict[str, Any]]:
    """Calcule les métriques on-chain avancées à partir d'une liste de transactions."""
    if market_cap is None: market_cap = 0.0

    logging.info("Récupération du prix actuel pour le calcul des métriques...")
    price_response = requests.get(
        f"https://api.coingecko.com/api/v3/simple/price?ids={project_id}&vs_currencies=usd"
    )
    if not price_response.ok:
        logging.warning("Impossible de récupérer le prix pour %s via CoinGecko.", project_id)
        return None
        
    current_price = price_response.json().get(project_id, {}).get("usd")
    if not current_price:
        logging.warning("Prix non trouvé pour %s dans la réponse de CoinGecko.", project_id)
        return None
    logging.info("Prix actuel de %s : %f USD", project_id, current_price)

    tx_in_7d, active_wallets_7d, tx_24h_volume_usd = [], set(), 0.0
    whale_tx_7d, quality_score_sum = 0, 0
    
    seven_days_ago = (datetime.now(timezone.utc) - timedelta(days=7)).timestamp()
    one_day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).timestamp()

    for tx in transactions:
        ts = int(tx.get("timeStamp", 0))
        if ts < seven_days_ago:
            continue
            
        from_addr, to_addr = tx.get("from"), tx.get("to")
        if not from_addr or not to_addr: continue

        tx_in_7d.append(tx)
        active_wallets_7d.add(from_addr)
        active_wallets_7d.add(to_addr)
        
        try:
            decimals = int(tx.get("tokenDecimal", 18))
            value = int(tx.get("value", 0))
            value_usd = (value / (10**decimals)) * current_price
        except (ValueError, TypeError):
            continue

        if ts > one_day_ago:
            tx_24h_volume_usd += value_usd
        if value_usd > whale_usd:
            whale_tx_7d += 1
            
        function_name = tx.get("functionName", "").lower()
        if "approve" in function_name or "swap" in function_name:
            quality_score_sum += 2
        elif "stake" in function_name or "liquidity" in function_name:
            quality_score_sum += 5
        else:
            quality_score_sum += 1

    return {
        "tx_count_7d": len(tx_in_7d),
        "active_wallets_7d": len(active_wallets_7d),
        "whale_tx_count_7d": whale_tx_7d,
        "tx_quality_score_7d": quality_score_sum / len(tx_in_7d) if tx_in_7d else 0,
        "normalized_velocity_24h": (tx_24h_volume_usd / market_cap) * 100 if market_cap > 0 else 0,
    }

# --- Fonction Principale du Worker ---

def run_onchain_worker() -> bool:
    logging.info("--- WORKER ON-CHAIN : DÉMARRAGE ---")
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    static_table = os.getenv("BQ_STATIC_TABLE", "Market_data")
    table_name = os.getenv("ONCHAIN_BIGQUERY_TABLE", "onchain_metrics")
    batch_size = int(os.getenv("ONCHAIN_BATCH_SIZE", "5"))
    whale_usd = int(os.getenv("WHALE_TRANSACTION_USD", "100000"))

    if not all([project_id, dataset, table_name]):
        logging.error("Configuration GCP manquante (projet, dataset, ou nom de table).")
        return False
    
    # Assertions pour l'analyseur de type (Pylance)
    assert project_id is not None
    assert dataset is not None
    assert table_name is not None

    bq_client = BigQueryClient(project_id)
    client = bq_client.client
    onchain_table_id = f"{project_id}.{dataset}.{table_name}"
    static_table_id = f"{project_id}.{dataset}.{static_table}"

    schema = [
        bigquery.SchemaField("id_projet", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date_analyse", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("tvl", "FLOAT"),
        bigquery.SchemaField("tx_count_7d", "INTEGER"),
        bigquery.SchemaField("active_wallets_7d", "INTEGER"),
        bigquery.SchemaField("whale_tx_count_7d", "INTEGER"),
        bigquery.SchemaField("tx_quality_score_7d", "FLOAT"),
        bigquery.SchemaField("normalized_velocity_24h", "FLOAT"),
    ]

    try:
        client.get_table(onchain_table_id)
        logging.info("La table destination %s existe déjà.", onchain_table_id)
    except NotFound:
        logging.warning("La table destination %s n'existe pas. Création en cours...", onchain_table_id)
        table_ref = bigquery.Table(onchain_table_id, schema=schema)
        client.create_table(table_ref)
        logging.info("Table %s créée avec succès.", onchain_table_id)

    query = f"""
        SELECT id_projet, nom, chaine_contrat, adresse_contrat, market_cap
        FROM `{static_table_id}` s
        WHERE s.adresse_contrat IS NOT NULL AND s.adresse_contrat != 'N/A - Pièce Native'
        AND s.id_projet NOT IN (SELECT DISTINCT id_projet FROM `{onchain_table_id}` WHERE date_analyse > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
    """
    logging.info("Exécution de la requête pour trouver les projets à analyser...")
    try:
        tasks_df = client.query(query).to_dataframe()
    except Exception as e:
        logging.error("Échec de la requête BigQuery : %s", e)
        return False
    
    if tasks_df.empty:
        logging.info("Aucun nouveau projet à analyser. Le worker a terminé sa tâche.")
        return True

    batch = tasks_df.head(batch_size)
    logging.info("%d projets à analyser au total. Traitement d'un lot de %d.", len(tasks_df), len(batch))
    results: List[Dict[str, Any]] = []

    endpoints = {
        "ethereum": {"url": "https://api.etherscan.io/api", "key": os.getenv("ETHERSCAN_API_KEY")},
        "binance-smart-chain": {"url": "https://api.bscscan.com/api", "key": os.getenv("BSCSCAN_API_KEY")},
        "polygon-pos": {"url": "https://api.polygonscan.com/api", "key": os.getenv("POLYGONSCAN_API_KEY")},
    }

    for _, row in batch.iterrows():
        project_id_val, name, chain, address, market_cap_val = row["id_projet"], row["nom"], row["chaine_contrat"], row["adresse_contrat"], row["market_cap"]
        logging.info("--- Analyse du projet : %s (%s) ---", name, project_id_val)
        
        if not address or not chain: continue
        
        tvl = get_tvl(address, chain)

        if chain not in endpoints or not endpoints[chain].get("key"):
            logging.warning("Chaîne '%s' non supportée ou clé API manquante. Passage au projet suivant.", chain)
            continue

        api_info = endpoints[chain]
        params = { "module": "account", "action": "tokentx", "contractaddress": address, "page": 1, "offset": 10000, "sort": "desc", "apikey": api_info["key"] }
        
        logging.info("Appel à l'API de %sscan.com pour les transactions...", chain)
        response = requests.get(api_info["url"], params=params, timeout=15)
        
        if not response.ok or response.json().get("status") == "0":
            logging.warning("Pas de données de transaction trouvées pour %s.", name)
            continue
        
        transactions = response.json().get("result", [])
        if not isinstance(transactions, list): continue

        logging.info("%d transactions trouvées pour %s.", len(transactions), name)
        
        metrics = calculate_advanced_metrics(transactions, project_id_val, market_cap_val, whale_usd)
        if not metrics:
            logging.warning("Le calcul des métriques n'a rien retourné pour %s.", name)
            continue

        final_result = { "id_projet": project_id_val, "date_analyse": datetime.now(timezone.utc), "tvl": tvl, **metrics }
        results.append(final_result)
        logging.info("Résultat pour %s ajouté à la liste.", name)
        time.sleep(1)

    if not results:
        logging.info("Aucun résultat d'analyse n'a été produit dans ce lot.")
        return True

    logging.info("Préparation de l'envoi de %d résultats vers BigQuery.", len(results))
    df = pd.DataFrame(results)
    
    try:
        # La variable 'dataset' et 'table_name' sont bien définies et non-None ici.
        bq_client.upload_dataframe(df, dataset, table_name, schema)
        logging.info("--> SUCCÈS ! %d analyses ajoutées à BigQuery.", len(results))
    except Exception as e:
        logging.error("Échec de l'envoi des données vers BigQuery : %s", e)
        return False
    
    return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    run_onchain_worker()