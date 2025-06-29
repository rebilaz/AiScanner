# Fichier : workers/worker_3_1.py (Version corrigée et full logs)
"""
Worker de décodage d'événements (logs) avec détection de proxy robuste via
lecture directe du stockage de la blockchain (EIP-1967).
"""
from __future__ import annotations
import os
import time
import logging
import json
from typing import Any, Dict, List, Optional
import traceback

import pandas as pd
import requests
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv
from web3 import Web3
from web3._utils.events import get_event_data

# --- Classe utilitaire BigQuery (intégrée pour la simplicité) ---
class BigQueryClient:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)

    def upload_dataframe(self, df: pd.DataFrame, dataset_id: str, table_id: str):
        """Charge un DataFrame dans BigQuery avec détection auto du schéma."""
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,  # Laisse BigQuery gérer les nouvelles colonnes d'événements
        )
        # Colonnes de base
        job_config.schema = [
            bigquery.SchemaField("transaction_hash", "STRING"),
            bigquery.SchemaField("log_index", "INTEGER"),
            bigquery.SchemaField("contract_address", "STRING"),
            bigquery.SchemaField("event_name", "STRING"),
        ]

        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logging.info("DataFrame chargé avec succès dans %s.%s", dataset_id, table_id)


# --- Configuration du Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# --- Configuration Globale ---
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
LOGS_TABLE = os.getenv("BQ_LOGS_RAW_TABLE", "logs_raw")
TOKENS_TABLE = os.getenv("BQ_TOKENS_TABLE", "Market_data")
DEST_TABLE = os.getenv("BQ_LABELED_EVENTS_TABLE", "labeled_events")
BATCH_SIZE = int(os.getenv("EVENTS_BATCH_SIZE", "5"))

NODE_RPCS = {
    "ethereum": os.getenv("ETHEREUM_NODE_RPC"),
    "binance-smart-chain": os.getenv("BSC_NODE_RPC"),
    "polygon-pos": os.getenv("POLYGON_NODE_RPC"),
}

w3_global_decoder = Web3()
abi_cache: dict[str, list[dict[str, Any]]] = {}

# --- MAPPING MANUEL D'ABI POUR CAS COMPLEXES ---
ABI_MAPPING: dict[str, list[dict[str, Any]]] = {
    "0xde4ee8057785a7e8e800db58f9784845a5c2cbd6": json.loads("""
       [{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]
    """)
}

# --- Fonctions Utilitaires ---

def fetch_abi_from_explorer(address: str, chain: str, endpoints: Dict) -> Optional[List[Dict[str, Any]]]:
    """Appelle l'API de l'explorateur pour obtenir un ABI."""
    api_info = endpoints.get(chain)
    if not api_info or not api_info.get("key"):
        logging.warning("Chaîne '%s' non supportée ou clé API manquante.", chain)
        return None
    
    params = {"module": "contract", "action": "getabi", "address": address, "apikey": api_info["key"]}
    try:
        logging.info("Appel à l'explorateur pour l'ABI de %s sur %s...", address, chain)
        response = requests.get(api_info["url"], params=params, timeout=15)
        if response.ok and response.json().get("status") == "1" and response.json().get("result"):
            return json.loads(response.json()["result"])
        logging.warning("Impossible de récupérer l'ABI pour %s via l'explorateur: %s", address, response.text)
    except Exception as e:
        logging.error("Erreur API ...scan pour %s: %s", address, e)
    return None

def get_final_abi(address: str, chain: str, endpoints: Dict, w3_provider: Web3) -> Optional[List[Dict[str, Any]]]:
    """Tente de trouver l'ABI d'implémentation pour les proxies EIP-1967."""
    if address.lower() in ABI_MAPPING:
        logging.info("ABI trouvé dans le mapping manuel pour %s.", address)
        return ABI_MAPPING[address.lower()]
        
    IMPLEMENTATION_SLOT = int("0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc", 16)
    try:
        checksum_address = Web3.to_checksum_address(address)
        storage_content = w3_provider.eth.get_storage_at(checksum_address, IMPLEMENTATION_SLOT)
        if storage_content is not None and hasattr(storage_content, "hex"):
            implementation_address = "0x" + storage_content.hex()[-40:]
            if int(implementation_address, 16) != 0:
                logging.info("Proxy EIP-1967 détecté ! Implémentation : %s", implementation_address)
                abi = fetch_abi_from_explorer(implementation_address, chain, endpoints)
                if abi:
                    return abi
    except Exception as e:
        logging.debug("Ce n'est probablement pas un proxy EIP-1967 (%s): %s", address, e)
    return fetch_abi_from_explorer(address, chain, endpoints)

def get_event_abi(abi: List[Dict[str, Any]], topic0: str) -> Optional[Dict[str, Any]]:
    """Trouve l'entrée ABI pour un topic hash donné."""
    for entry in abi:
        if entry.get("type") != "event" or entry.get("anonymous", False):
            continue
        inputs = entry.get("inputs", [])
        signature_text = f"{entry['name']}({','.join([i['type'] for i in inputs])})"
        signature_hash = Web3.keccak(text=signature_text).hex()
        if signature_hash.lower() == topic0.lower():
            return entry
    return None

def decode_log(raw_log: Any, abi: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Decode a log using the provided ABI with extensive debug information."""

    log = raw_log.to_dict() if hasattr(raw_log, "to_dict") else raw_log

    if not log.get("topics"):
        print("[decode_log] Pas de topics pour le log", log.get("transaction_hash"))
        return None

    print(f"[decode_log] Contract: {log.get('address')} | block: {log.get('block_number')} | tx: {log.get('transaction_hash')}")
    print(f"[decode_log] Topics ({len(log['topics'])}): {log['topics']}")
    print(f"[decode_log] topic0: {log['topics'][0]}")

    signatures = []
    for entry in abi:
        if entry.get("type") != "event" or entry.get("anonymous", False):
            continue
        inputs = entry.get("inputs", [])
        sig_text = f"{entry['name']}({','.join([i['type'] for i in inputs])})"
        sig_hash = Web3.keccak(text=sig_text).hex()
        signatures.append(f"{entry['name']}: {sig_text} -> {sig_hash}")
    print("[decode_log] Signatures ABI:\n" + "\n".join(signatures))

    topic0 = log["topics"][0]
    event_abi = get_event_abi(abi, topic0)
    if not event_abi:
        print(f"[decode_log] Aucun event ne correspond au topic {topic0}")
        print(f"[decode_log] Taille topics: {len(log['topics'])}")
        print("[decode_log] ABI complet:\n" + json.dumps(abi, indent=2))
        return None

    print("[decode_log] ABI utilisé:", json.dumps(event_abi, indent=2))
    print(f"[decode_log] Event attendu: {event_abi.get('name')} signature topic0")

    try:
        formatted_log = {
            "address": Web3.to_checksum_address(log["address"]),
            "topics": [bytes.fromhex(t[2:]) if isinstance(t, str) else t for t in log["topics"]],
            "data": bytes.fromhex(log["data"][2:]) if isinstance(log["data"], str) else log["data"],
        }
        decoded = get_event_data(w3_global_decoder.codec, event_abi, formatted_log)  # type: ignore
        decoded_args = {k: str(v) for k, v in decoded["args"].items()}
        record = {
            "event_name": event_abi["name"],
            **decoded_args,
            "block_number": log.get("block_number"),
            "transaction_hash": log.get("transaction_hash"),
            "log_index": log.get("log_index"),
            "contract_address": log.get("address", "").lower(),
        }
        print("[decode_log] Décodage réussi", record)
        return record
    except Exception:
        print("[decode_log] Échec du décodage pour le topic", topic0)
        print("[decode_log] ABI utilisé:", json.dumps(event_abi, indent=2))
        print("[decode_log] Event trouvé:", topic0)
        traceback.print_exc()
        return None

# --- Fonction Principale du Worker ---

def run_label_events_worker() -> bool:
    """Point d'entrée principal pour décoder les logs bruts."""
    logging.info("--- WORKER TRADUCTEUR (Final) : DÉMARRAGE ---")

    if not all([PROJECT_ID, DATASET]):
        logging.error("Configuration GCP manquante (PROJECT_ID, BQ_DATASET).")
        return False
    
    assert PROJECT_ID is not None and DATASET is not None

    web3_providers = {
        chain: Web3(Web3.HTTPProvider(rpc_url))
        for chain, rpc_url in NODE_RPCS.items()
        if rpc_url and Web3(Web3.HTTPProvider(rpc_url)).is_connected()
    }
    if not web3_providers:
        logging.error("Aucun noeud RPC valide n'a pu être connecté. Vérifiez les variables d'environnement RPC.")
        return False
    logging.info("Connecté à %d chaînes.", len(web3_providers))
    
    bq_client = BigQueryClient(PROJECT_ID)
    client = bq_client.client
    dest_table_id = f"{PROJECT_ID}.{DATASET}.{DEST_TABLE}"

    try:
        client.get_table(dest_table_id)
        logging.info("La table destination %s existe déjà.", dest_table_id)
    except NotFound:
        logging.warning("Table destination %s non trouvée. Création...", dest_table_id)
        schema = [
            bigquery.SchemaField("transaction_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("log_index", "INTEGER", mode="REQUIRED"),
        ]
        client.create_table(bigquery.Table(dest_table_id, schema=schema))
        logging.info("Table %s créée.", dest_table_id)

    endpoints = {
        "ethereum": {"url": "https://api.etherscan.io/api", "key": os.getenv("ETHERSCAN_API_KEY")},
        "binance-smart-chain": {"url": "https://api.bscscan.com/api", "key": os.getenv("BSCSCAN_API_KEY")},
        "polygon-pos": {"url": "https://api.polygonscan.com/api", "key": os.getenv("POLYGONSCAN_API_KEY")}
    }

    query_contracts = (
        f"SELECT DISTINCT adresse_contrat, chaine_contrat, nom, symbole FROM `{PROJECT_ID}.{DATASET}.{TOKENS_TABLE}` "
        f"WHERE adresse_contrat IS NOT NULL AND adresse_contrat != 'Indisponible'"
    )
    try:
        contracts_to_process_df = client.query(query_contracts).to_dataframe().head(BATCH_SIZE)
        logging.info("%d contrats à traiter.", len(contracts_to_process_df))
    except Exception as e:
        logging.error("Impossible de charger les contrats depuis la table `%s`: %s", TOKENS_TABLE, e)
        return False

    if contracts_to_process_df.empty:
        logging.info("Aucun contrat à traiter.")
        return True

    all_decoded_records = []
    success_count = 0
    failure_count = 0
    
    for _, contract_row in contracts_to_process_df.iterrows():
        address, chain, nom, symbole = (
            contract_row["adresse_contrat"].lower(),
            contract_row["chaine_contrat"],
            contract_row["nom"],
            contract_row["symbole"],
        )
        logging.info("--- Traitement du contrat : %s (%s) sur %s ---", nom, symbole, chain)
        
        w3_provider = web3_providers.get(chain)
        if not w3_provider:
            logging.warning("Pas de connexion RPC pour la chaîne '%s'. Contrat ignoré.", chain)
            continue
            
        abi = abi_cache.get(address)
        if not abi:
            logging.info("Récupération de l'ABI pour %s sur %s", address, chain)
            abi = get_final_abi(address, chain, endpoints, w3_provider)
            if not abi:
                logging.warning("Impossible de continuer sans ABI pour %s. Contrat ignoré.", address)
                continue
            abi_cache[address] = abi
        
        time.sleep(1)

        query_raw_logs = f"""
            SELECT lr.* FROM `{PROJECT_ID}.{DATASET}.{LOGS_TABLE}` AS lr 
            LEFT JOIN `{dest_table_id}` AS le ON lr.transaction_hash = le.transaction_hash AND lr.log_index = le.log_index 
            WHERE le.transaction_hash IS NULL AND LOWER(lr.address) = '{address}' 
            ORDER BY lr.block_number 
            LIMIT 2000
        """
        try:
            raw_logs_df = client.query(query_raw_logs).to_dataframe()
            if raw_logs_df.empty:
                logging.info("Aucun nouveau log à traiter pour %s.", address)
                continue
            logging.info("%d logs bruts trouvés pour %s.", len(raw_logs_df), address)
        except Exception as e:
            if "Not found" in str(e) and LOGS_TABLE in str(e):
                logging.error("La table source `%s` n'existe pas.", LOGS_TABLE)
                return False
            logging.warning("Erreur récupération logs pour %s: %s", address, e)
            continue
        
        for _, log_row in raw_logs_df.iterrows():
            logging.debug(f"Décodage du log: {log_row.to_dict()}")
            record = decode_log(log_row, abi)
            if record:
                success_count += 1
                record.update({'token_name': nom, 'token_symbol': symbole})
                all_decoded_records.append(record)
            else:
                failure_count += 1

    if not all_decoded_records:
        print("Aucun nouvel événement n'a été décodé sur l'ensemble du lot.")
        print(f"Logs décodés: {success_count} / échecs: {failure_count}")
        return True

    df_events = pd.DataFrame(all_decoded_records)
    logging.info(
        "Envoi de %d événements décodés vers BigQuery.",
        len(df_events),
    )
    print(f"Logs décodés: {success_count} / échecs: {failure_count}")
    try:
        bq_client.upload_dataframe(df_events, DATASET, DEST_TABLE)
    except Exception as e:
        logging.error("Échec de l'envoi vers BigQuery: %s", e)
        return False

    logging.info("--- WORKER TRADUCTEUR : TERMINÉ AVEC SUCCÈS ---")
    return True

if __name__ == "__main__":
    run_label_events_worker()
