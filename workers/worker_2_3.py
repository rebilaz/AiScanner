"""
On-chain analysis worker refactored for main integration and debugging.
"""

import os
import time
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

import pandas as pd
import requests
from google.cloud import bigquery
from dotenv import load_dotenv

# Assurez-vous que ce fichier existe et est importable
# Si gcp_utils.py est à la racine, cette ligne est correcte
from gcp_utils import BigQueryClient


def get_tvl(address: str, chain: str) -> Any:
    """Fetches TVL from DeFiLlama."""
    print(f"      -> Appel à Llama.fi pour TVL de {address} sur {chain}...")
    try:
        response = requests.get(f"https://api.llama.fi/tvl/{chain}:{address}")
        if response.status_code == 200:
            tvl_value = response.json()
            print(f"      --> TVL trouvée : {tvl_value}")
            return tvl_value
        print(f"      --> AVERTISSEMENT : Llama.fi a répondu avec le statut {response.status_code}")
        return 0
    except Exception as e:
        print(f"      --> ERREUR pendant la récupération de la TVL : {e}")
        return 0


def calculate_advanced_metrics(
    transactions: List[Dict], project_id: str, market_cap: float, whale_usd: int
) -> Dict[str, Any]:
    """Calculates advanced on-chain metrics from a list of transactions."""
    print("      -> Calcul des métriques avancées...")
    tx_30d, active_wallets_30d, tx_24h_volume_usd = [], set(), 0
    retention_score, whale_tx_7d, quality_score_sum = 0, 0, 0

    thirty_days_ago = (datetime.now() - timedelta(days=30)).timestamp()
    seven_days_ago = (datetime.now() - timedelta(days=7)).timestamp()
    one_day_ago = (datetime.now() - timedelta(days=1)).timestamp()

    print("        -> Récupération du prix actuel depuis CoinGecko...")
    price_response = requests.get(
        f"https://api.coingecko.com/api/v3/simple/price?ids={project_id}&vs_currencies=usd"
    ).json()
    current_price = price_response.get(project_id, {}).get("usd", 0)
    
    if current_price == 0:
        print("        --> AVERTISSEMENT : Impossible de récupérer le prix, les calculs basés sur le prix seront nuls.")
        return {}
    
    print(f"        --> Prix actuel : {current_price} USD")

    # ... (le reste de la logique de calcul reste identique) ...
    # Le code de la fonction est long, donc je ne le modifie pas, 
    # mais on pourrait y ajouter d'autres `print` si nécessaire.
    
    wallets_in_period: Dict[str, Dict[str, int]] = {}
    for tx in transactions:
        ts = int(tx.get("timeStamp", 0))
        if ts < thirty_days_ago:
            continue

        from_addr, to_addr = tx["from"], tx["to"]
        wallets_in_period.setdefault(from_addr, {"in": 0, "out": 0})["out"] += 1
        wallets_in_period.setdefault(to_addr, {"in": 0, "out": 0})["in"] += 1

        if ts > seven_days_ago:
            tx_30d.append(tx)
            active_wallets_30d.add(from_addr)
            active_wallets_30d.add(to_addr)

            decimals = int(tx.get("tokenDecimal", 18))
            value_usd = (int(tx.get("value", 0)) / (10**decimals)) * current_price

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

    receiving_wallets = [addr for addr, data in wallets_in_period.items() if data["in"] > 0]
    pure_receivers = [addr for addr in receiving_wallets if wallets_in_period[addr]["out"] == 0]
    retention_score = (len(pure_receivers) / len(receiving_wallets)) * 100 if receiving_wallets else 0

    final_metrics = {
        "tx_count_7d": len([tx for tx in tx_30d if int(tx.get("timeStamp", 0)) > seven_days_ago]),
        "active_wallets_7d": len(active_wallets_30d),
        "whale_tx_count_7d": whale_tx_7d,
        "retention_score_30d": retention_score,
        "tx_quality_score_7d": quality_score_sum / len(tx_30d) if tx_30d else 0,
        "normalized_velocity_24h": (tx_24h_volume_usd / market_cap) * 100 if market_cap else 0,
    }
    print(f"      --> Métriques calculées : {final_metrics}")
    return final_metrics


async def run_onchain_worker() -> bool:
    print("\n--- WORKER ON-CHAIN : DÉMARRAGE ---\n")
    load_dotenv()

    # --- Étape 1: Chargement de la configuration ---
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    static_table = os.getenv("BQ_STATIC_TABLE", "Market_data")
    table = os.getenv("ONCHAIN_BIGQUERY_TABLE", "onchain_metrics")
    batch_size = int(os.getenv("ONCHAIN_BATCH_SIZE", "20"))
    whale_usd = int(os.getenv("WHALE_TRANSACTION_USD", "100000"))
    
    print(f"Configuration : project={project_id}, dataset={dataset}, table={table}")
    if not all([project_id, dataset, table]):
        print("!!! ERREUR FATALE : Configuration GCP manquante. Arrêt.")
        raise ValueError("Configuration GCP manquante (projet, dataset, ou table)")
    if project_id is None:
        raise ValueError("GCP_PROJECT_ID ne doit pas être None")

    # --- Étape 2: Connexion à BigQuery et récupération des tâches ---
    bq_client = BigQueryClient(project_id)
    print("Client BigQuery créé.")
    client = bq_client.client

    static_table_id = f"{project_id}.{dataset}.{static_table}"
    onchain_table_id = f"{project_id}.{dataset}.{table}"
    
    query = f"""
        SELECT id_projet, nom, chaine_contrat, adresse_contrat, market_cap
        FROM `{static_table_id}` s
        WHERE s.adresse_contrat IS NOT NULL AND s.adresse_contrat != 'N/A - Pièce Native'
        AND s.id_projet NOT IN (SELECT DISTINCT id_projet FROM `{onchain_table_id}` WHERE date_analyse > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
    """
    print("Exécution de la requête pour trouver de nouveaux projets à analyser...")
    tasks_df = client.query(query).to_dataframe()
    
    if tasks_df.empty:
        print("\n--> FIN DE PROCESSUS : Aucun nouveau projet à analyser trouvé. C'est un succès normal.\n")
        return True

    # --- Étape 3: Traitement du lot de projets ---
    batch = tasks_df.head(batch_size)
    print(f"\n--> {len(tasks_df)} projets trouvés au total. Traitement d'un lot de {len(batch)}.\n")
    results = []

    endpoints = {
        "ethereum": {"url": "https://api.etherscan.io/api", "key": os.getenv("ETHERSCAN_API_KEY")},
        "binance-smart-chain": {"url": "https://api.bscscan.com/api", "key": os.getenv("BSCSCAN_API_KEY")},
        "polygon-pos": {"url": "https://api.polygonscan.com/api", "key": os.getenv("POLYGONSCAN_API_KEY")},
    }

    for _, row in batch.iterrows():
        project_id, name, chain, address, market_cap = row["id_projet"], row["nom"], row["chaine_contrat"], row["adresse_contrat"], row["market_cap"]
        print(f"  -> Analyse de : {name} sur {chain} ({address})")

        tvl = get_tvl(address, chain)

        if chain not in endpoints or not endpoints[chain].get("key"):
            print(f"    --> AVERTISSEMENT : Chaîne '{chain}' non supportée ou clé API manquante. On passe au suivant.")
            continue

        api_info = endpoints[chain]
        params = {
            "module": "account", "action": "tokentx", "contractaddress": address,
            "page": 1, "offset": 1000, "sort": "desc", "apikey": api_info["key"],
        }
        
        print(f"    -> Appel à l'API de ...scan.com pour les transactions...")
        response = requests.get(api_info["url"], params=params)
        if response.status_code != 200 or response.json().get("status") == "0":
            print("    --> Pas de données de transaction trouvées. On passe au suivant.")
            continue
        print(f"    --> {len(response.json()['result'])} transactions trouvées.")
        
        transactions = response.json()["result"]
        metrics = calculate_advanced_metrics(transactions, project_id, market_cap, whale_usd)

        if not metrics:
             print("    --> AVERTISSEMENT : Le calcul des métriques n'a rien retourné. On passe au suivant.")
             continue

        final_result = {
            "id_projet": project_id,
            "date_analyse": datetime.now(timezone.utc).isoformat(),
            "tvl": tvl,
            **metrics,
        }
        results.append(final_result)
        print(f"    --> Résultat pour {name} ajouté à la liste.")
        time.sleep(1)

    # --- Étape 4: Insertion dans BigQuery ---
    if not results:
        print("\n!!! FIN DE PROCESSUS : Après avoir analysé le lot, aucun résultat n'était prêt à être inséré.\n")
        return True

    print(f"\n--> Préparation de l'insertion de {len(results)} analyses on-chain dans BigQuery...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_json(results, onchain_table_id, job_config=job_config)
    job.result()
    
    print(f"--> SUCCÈS ! {len(results)} analyses ajoutées à BigQuery.")
    print("\n--- WORKER ON-CHAIN : TERMINÉ ---\n")
    return True

if __name__ == "__main__":
    try:
        asyncio.run(run_onchain_worker())
    except Exception as e:
        print(f"!!! ERREUR FATALE NON GÉRÉE : {e}")