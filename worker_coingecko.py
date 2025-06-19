import os
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone
import time
import config

# --- Configuration ---
# Set Google credentials for BigQuery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.GOOGLE_CREDENTIALS_PATH

# BigQuery config
PROJECT_ID = config.BIGQUERY_PROJECT_ID
DATASET_ID = config.BIGQUERY_DATASET_ID
TABLE_NAME = config.BIGQUERY_MASTER_TABLE
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

# CoinGecko config
API_BASE = config.COINGECKO_API_BASE_URL
CATEGORY = config.TARGET_CATEGORY
MIN_MARKET_CAP = config.MIN_MARKET_CAP
MIN_VOLUME_USD = config.MIN_VOLUME_USD
BATCH_SIZE = getattr(config, "BATCH_SIZE", 20)
# --- Fin de la Configuration ---


def make_api_request(url, params, max_retries=3, retry_delay_seconds=60):
    """
    Effectue une requête API de manière robuste avec une logique de retry.
    Gère spécifiquement l'erreur 429 (rate limiting).
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=15) # Ajout d'un timeout

            # Cas spécifique : rate limiting
            if response.status_code == 429:
                print(f"Rate limit (429) atteint. Attente de {retry_delay_seconds}s avant la prochaine tentative...")
                time.sleep(retry_delay_seconds)
                continue  # On passe à la tentative suivante

            # Lève une exception pour les autres codes d'erreur HTTP (404, 500, etc.)
            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Erreur de requête pour {url} (tentative {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Petite pause avant de retenter pour une erreur réseau
            else:
                print(f"Échec final de la requête pour {url} après {max_retries} tentatives.")
                return None
    return None


def get_projects_by_category():
    """
    Récupère la liste des projets pour une catégorie donnée.
    Utilise la fonction robuste make_api_request.
    """
    url = f"{API_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "category": CATEGORY,
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false"
    }
    data = make_api_request(url, params)
    if data:
        return pd.DataFrame(data)
    # Retourne un DataFrame vide en cas d'échec pour éviter de planter le script
    return pd.DataFrame()


def get_token_details(token_id):
    """
    Récupère les détails d'un token.
    Utilise la fonction robuste make_api_request.
    """
    url = f"{API_BASE}/coins/{token_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "true",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false"
    }
    return make_api_request(url, params)


def extract_data(raw, now_utc):
    """
    Extrait et structure les données depuis la réponse JSON brute de l'API.
    """
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


def upload_to_bigquery(df, table_id):
    """
    Charge un DataFrame dans une table BigQuery.
    """
    client = bigquery.Client()
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Upload de {len(df)} lignes terminé sur BigQuery.")


def main():
    """
    Fonction principale du script.
    """
    print(f"Récupération de la liste des tokens de la catégorie : {CATEGORY}")
    market_df = get_projects_by_category()
    
    if market_df.empty:
        print("Aucun token n'a pu être récupéré. Arrêt du script.")
        return

    print(f"{len(market_df)} tokens trouvés.")

    # Filtrage par market cap et volume
    market_df["market_cap"] = pd.to_numeric(market_df["market_cap"], errors="coerce").fillna(0)
    market_df["total_volume"] = pd.to_numeric(market_df["total_volume"], errors="coerce").fillna(0)
    market_df = market_df[
        (market_df["market_cap"] >= MIN_MARKET_CAP) &
        (market_df["total_volume"] >= MIN_VOLUME_USD)
    ].reset_index(drop=True)
    print(f"{len(market_df)} tokens après filtrage market cap / volume.")

    client = bigquery.Client()
    deja = {}
    try:
        table = client.get_table(TABLE_ID)
        # On ne récupère que l'ID et la date de mise à jour pour optimiser
        rows = client.list_rows(table, selected_fields=[table.schema[0], table.schema[-1]])
        deja = {r[0]: str(r[-1]) for r in rows}
        print(f"{len(deja)} tokens déjà présents dans BigQuery.")
    except Exception as e:
        print(f"Impossible de lire la table BigQuery '{TABLE_ID}' (sera ignoré): {e}")

    to_upload = []
    count = 0
    for _, row in market_df.iterrows():
        token_id = row["id"]
        
        # Si déjà dans la table, on skippe (évite l'appel API)
        if token_id in deja:
            print(f"Déjà présent, on skip : {token_id}")
            continue

        print(f"Extraction des données pour : {token_id}")
        now_utc = datetime.now(timezone.utc).isoformat()
        
        detail = get_token_details(token_id)
        if not detail:
            # L'erreur est déjà loggée dans get_token_details, on passe au suivant
            continue
        
        data = extract_data(detail, now_utc)
        to_upload.append(data)
        count += 1
        
        # Upload par batch pour limiter la RAM
        if count % BATCH_SIZE == 0:
            print(f"Upload d'un batch de {len(to_upload)} tokens sur BigQuery...")
            df_batch = pd.DataFrame(to_upload)
            upload_to_bigquery(df_batch, TABLE_ID)
            to_upload = []

        # Pause pour respecter l'API
        time.sleep(3)  # Pause de 1.5 seconde entre chaque token

    # Upload du dernier batch s'il en reste
    if to_upload:
        print(f"Upload du dernier batch de {len(to_upload)} tokens sur BigQuery...")
        df_final = pd.DataFrame(to_upload)
        upload_to_bigquery(df_final, TABLE_ID)
        
    print("Script terminé.")


if __name__ == "__main__":
    main()