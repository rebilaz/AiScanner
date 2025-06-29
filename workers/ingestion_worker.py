# Fichier : workers/ingestion_worker.py (Version Finale Stable)
"""
Worker d'ingestion des logs bruts de la blockchain.
Ce script scanne les blocs et récupère uniquement les logs des adresses
présentes dans la table Market_data.
"""
import os
import time
import logging
from typing import List, Dict, Any

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv
from web3 import Web3
from web3.types import LogReceipt

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
MARKET_DATA_TABLE = os.getenv("BQ_STATIC_TABLE", "Market_data")
LOGS_TABLE = os.getenv("BQ_LOGS_RAW_TABLE", "logs_raw")
ETHEREUM_NODE_RPC = os.getenv("ETHEREUM_NODE_RPC") 
BLOCK_BATCH_SIZE = 500
LOG_BATCH_SIZE = 10000

# --- Fonctions Utilitaires ---

def format_log(log: LogReceipt) -> Dict[str, Any]:
    """Convertit un log Web3 en un dictionnaire compatible BigQuery."""
    return {
        "log_index": log["logIndex"],
        "transaction_hash": log["transactionHash"].hex(),
        "block_number": log["blockNumber"],
        "address": log["address"],
        "data": log["data"].hex(),
        "topics": [topic.hex() for topic in log["topics"]],
    }

# --- Fonction Principale ---

def run_targeted_ingestion_worker() -> bool:
    logging.info("--- WORKER D'INGESTION CIBLÉ (Final) : DÉMARRAGE ---")

    if not all([PROJECT_ID, DATASET, ETHEREUM_NODE_RPC]):
        logging.error("Configuration manquante (GCP_PROJECT_ID, BQ_DATASET, ou ETHEREUM_NODE_RPC).")
        return False

    assert PROJECT_ID is not None and DATASET is not None

    w3 = Web3(Web3.HTTPProvider(ETHEREUM_NODE_RPC))
    if not w3.is_connected():
        logging.error("Impossible de se connecter au nœud Ethereum à l'URL : %s", ETHEREUM_NODE_RPC)
        return False
    
    latest_block_on_chain = w3.eth.block_number
    logging.info("Connecté au nœud Ethereum. Dernier block : %d", latest_block_on_chain)

    client = bigquery.Client(project=PROJECT_ID)
    logs_table_id = f"{PROJECT_ID}.{DATASET}.{LOGS_TABLE}"
    market_data_table_id = f"{PROJECT_ID}.{DATASET}.{MARKET_DATA_TABLE}"

    try:
        client.get_table(logs_table_id)
        logging.info("La table de destination %s existe déjà.", logs_table_id)
    except NotFound:
        logging.warning("Table `logs_raw` non trouvée. Création en cours avec le schéma correct...")
        schema = [
            bigquery.SchemaField("log_index", "INTEGER"),
            bigquery.SchemaField("transaction_hash", "STRING"),
            bigquery.SchemaField("block_number", "INTEGER"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("data", "STRING"),
            bigquery.SchemaField("topics", "STRING", mode="REPEATED"),
        ]
        table = bigquery.Table(logs_table_id, schema=schema)
        client.create_table(table)
        logging.info("Table %s créée avec succès.", logs_table_id)

    try:
        query_addresses = f"SELECT DISTINCT adresse_contrat FROM `{market_data_table_id}` WHERE chaine_contrat = 'ethereum' AND adresse_contrat IS NOT NULL AND adresse_contrat != 'Indisponible'"
        logging.info("Récupération de la liste des adresses Ethereum à surveiller...")
        addresses_to_watch_df = client.query(query_addresses).to_dataframe()
        addresses_to_watch = [Web3.to_checksum_address(addr) for addr in addresses_to_watch_df["adresse_contrat"].tolist()]
        if not addresses_to_watch:
            logging.warning("Aucune adresse Ethereum trouvée dans Market_data. Le worker n'a rien à faire.")
            return True
        logging.info("%d adresses Ethereum uniques seront surveillées.", len(addresses_to_watch))
    except Exception as e:
        logging.error("Impossible de récupérer la liste des adresses depuis Market_data : %s", e)
        return False

    start_block = 0
    try:
        query_last_block = f"SELECT MAX(block_number) as last_block FROM `{logs_table_id}`"
        result_df = client.query(query_last_block).to_dataframe()
        if not result_df.empty and pd.notna(result_df["last_block"][0]):
            start_block = int(result_df["last_block"][0]) + 1
            logging.info("Reprise du scan à partir du dernier bloc trouvé : %d", start_block)
        else:
            logging.warning("Table `logs_raw` vide. Démarrage du scan sur les 10,000 derniers blocs.")
            start_block = latest_block_on_chain - 10000
    except Exception:
        logging.warning("Impossible de lire le dernier bloc, démarrage sur les 10,000 derniers blocs.")
        start_block = latest_block_on_chain - 10000

    end_block = latest_block_on_chain
    all_logs_to_load = []

    for from_block in range(start_block, end_block, BLOCK_BATCH_SIZE):
        to_block = min(from_block + BLOCK_BATCH_SIZE - 1, end_block)
        if from_block > to_block: continue
        
        logging.info("Scanning des blocs de %d à %d (pour nos %d adresses)...", from_block, to_block, len(addresses_to_watch))
        try:
            log_filter = {"fromBlock": from_block, "toBlock": to_block, "address": addresses_to_watch}
            logs = w3.eth.get_logs(log_filter)  # type: ignore
            
            if logs:
                formatted_logs = [format_log(log) for log in logs]
                all_logs_to_load.extend(formatted_logs)
                logging.info("   -> %d logs trouvés et ajoutés au lot.", len(logs))
        except Exception as e:
            logging.error("Erreur lors de la récupération des logs pour les blocs %d-%d: %s", from_block, to_block, e)
        
        if len(all_logs_to_load) >= LOG_BATCH_SIZE:
            logging.info("Envoi d'un lot de %d logs vers BigQuery...", len(all_logs_to_load))
            try:
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                job = client.load_table_from_json(all_logs_to_load, logs_table_id, job_config=job_config)
                job.result()
                all_logs_to_load.clear()
            except Exception as e:
                logging.error("Échec de l'envoi vers BigQuery: %s", e)
        
        time.sleep(1)

    if all_logs_to_load:
        logging.info("Envoi du dernier lot de %d logs vers BigQuery...", len(all_logs_to_load))
        try:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = client.load_table_from_json(all_logs_to_load, logs_table_id, job_config=job_config)
            job.result()
        except Exception as e:
            logging.error("Échec de l'envoi du dernier lot vers BigQuery: %s", e)
            return False

    logging.info("--- WORKER D'INGESTION CIBLÉ : TERMINÉ ---")
    return True

if __name__ == "__main__":
    run_targeted_ingestion_worker()