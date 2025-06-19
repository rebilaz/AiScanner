# Fichier : worker_onchain.py
import requests
import time
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
import pandas as pd
from config import *

API_ENDPOINTS = {
    "ethereum": {"url": "https://api.etherscan.io/api", "key": ETHERSCAN_API_KEY},
    "binance-smart-chain": {"url": "https://api.bscscan.com/api", "key": BSCSCAN_API_KEY},
    "polygon-pos": {"url": "https://api.polygonscan.com/api", "key": POLYGONSCAN_API_KEY}
}
CEX_ADDRESSES = {
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be", # Binance 8
    "0x73BCEb1Cd57C711feCac2226092f672E13b33Be2", # Coinbase 1
    "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8", # Binance 7
}

def analyze_onchain_activity():
    print("--- Démarrage du Worker On-Chain [Sociologue]...")
    client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT_ID)
    static_table_id = f"{GOOGLE_CLOUD_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_STATIC_TABLE}"
    onchain_table_id = f"{GOOGLE_CLOUD_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_ONCHAIN_TABLE}"

    try:
        query = f"""
            SELECT id_projet, nom, chaine_contrat, adresse_contrat, market_cap
            FROM `{static_table_id}` s
            LEFT JOIN `{GOOGLE_CLOUD_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_MARKET_TABLE}` m ON s.id_projet = m.id_projet
            WHERE s.adresse_contrat IS NOT NULL AND s.adresse_contrat != 'N/A - Pièce Native'
            AND s.id_projet NOT IN (SELECT DISTINCT id_projet FROM `{onchain_table_id}` WHERE date_analyse > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
        """
        tasks_df = client.query(query).to_dataframe()
        if tasks_df.empty:
            print("-> Aucun nouveau projet à analyser.")
            return True

        batch = tasks_df.head(BATCH_SIZE)
        print(f"-> {len(tasks_df)} projets à analyser. Traitement d'un lot de {len(batch)}.")
        results = []

        for _, row in batch.iterrows():
            project_id, name, chain, address, market_cap = row['id_projet'], row['nom'], row['chaine_contrat'], row['adresse_contrat'], row['market_cap']
            print(f"  -> Analyse de : {name} sur {chain}")

            # 1. TVL (DeFiLlama)
            tvl = get_tvl(address, chain)
            
            # 2. Données Etherscan
            if chain not in API_ENDPOINTS:
                print(f"    -> Chaîne {chain} non supportée.")
                continue

            api_info = API_ENDPOINTS[chain]
            params = {"module": "account", "action": "tokentx", "contractaddress": address, "page": 1, "offset": 1000, "sort": "desc", "apikey": api_info['key']}
            response = requests.get(api_info['url'], params=params)
            
            if response.status_code != 200 or response.json().get("status") == "0":
                print(f"    -> Pas de données de transaction trouvées.")
                continue

            transactions = response.json()["result"]
            
            # 3. Calcul des métriques avancées
            metrics = calculate_advanced_metrics(transactions, project_id, market_cap)
            
            final_result = {
                "id_projet": project_id, "date_analyse": datetime.now(timezone.utc).isoformat(),
                "tvl": tvl, **metrics
            }
            results.append(final_result)
            time.sleep(1)

        if results:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = client.load_table_from_json(results, onchain_table_id, job_config=job_config)
            job.result()
            print(f"-> Succès ! {len(results)} analyses on-chain ajoutées à BigQuery.")
        
        return True

    except Exception as e:
        print(f"Une erreur est survenue dans le worker on-chain : {e}")
        return False

def get_tvl(address, chain):
    try:
        response = requests.get(f"https://api.llama.fi/tvl/{chain}:{address}")
        return response.json() if response.status_code == 200 else 0
    except Exception:
        return 0

def calculate_advanced_metrics(transactions, project_id, market_cap):
    # Initialisation
    tx_30d, active_wallets_30d, tx_24h_volume_usd = [], set(), 0
    retention_score, whale_tx_7d, quality_score_sum = 0, 0, 0
    
    # Timestamps
    thirty_days_ago = (datetime.now() - timedelta(days=30)).timestamp()
    seven_days_ago = (datetime.now() - timedelta(days=7)).timestamp()
    one_day_ago = (datetime.now() - timedelta(days=1)).timestamp()

    # On a besoin du prix actuel pour valoriser les transactions
    price_response = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={project_id}&vs_currencies=usd").json()
    current_price = price_response.get(project_id, {}).get("usd", 0)
    if current_price == 0: return {} # On ne peut rien calculer sans le prix

    # Analyse des transactions
    wallets_in_period = {} # {address: {'in': count, 'out': count}}
    for tx in transactions:
        ts = int(tx.get("timeStamp", 0))
        if ts < thirty_days_ago: continue
        
        from_addr, to_addr = tx["from"], tx["to"]
        
        # Pour le score de rétention
        wallets_in_period.setdefault(from_addr, {'in': 0, 'out': 0})['out'] += 1
        wallets_in_period.setdefault(to_addr, {'in': 0, 'out': 0})['in'] += 1
        
        if ts > seven_days_ago:
            tx_30d.append(tx) # On utilise tx_30d comme base, mais on pourrait séparer
            active_wallets_30d.add(from_addr)
            active_wallets_30d.add(to_addr)
            
            decimals = int(tx.get('tokenDecimal', 18))
            value_usd = (int(tx.get('value', 0)) / (10**decimals)) * current_price
            
            if ts > one_day_ago: tx_24h_volume_usd += value_usd
            if value_usd > WHALE_TRANSACTION_USD: whale_tx_7d += 1
            
            # Score de qualité
            function_name = tx.get("functionName", "").lower()
            if "approve" in function_name or "swap" in function_name: quality_score_sum += 2
            elif "stake" in function_name or "liquidity" in function_name: quality_score_sum += 5
            else: quality_score_sum += 1

    # Calcul final des scores
    # Score de Rétention : % de wallets qui ont reçu sans envoyer
    receiving_wallets = [addr for addr, data in wallets_in_period.items() if data['in'] > 0]
    pure_receivers = [addr for addr in receiving_wallets if wallets_in_period[addr]['out'] == 0]
    retention_score = (len(pure_receivers) / len(receiving_wallets)) * 100 if receiving_wallets else 0

    return {
        "tx_count_7d": len([tx for tx in tx_30d if int(tx.get("timeStamp", 0)) > seven_days_ago]),
        "active_wallets_7d": len(active_wallets_30d),
        "whale_tx_count_7d": whale_tx_7d,
        "retention_score_30d": retention_score,
        "tx_quality_score_7d": quality_score_sum / len(tx_30d) if tx_30d else 0,
        "normalized_velocity_24h": (tx_24h_volume_usd / market_cap) * 100 if market_cap else 0
    }