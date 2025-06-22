import requests
import time
import pandas as pd
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Setup des credentials
load_dotenv()
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""))

# Paramètres
category = os.getenv("COINGECKO_CATEGORY", "artificial-intelligence")
table_id = os.getenv("COINGECKO_TABLE_ID", "project.dataset.table")
LIMIT = 20

def fetch_with_retry(url, retries=3, sleep_sec=2):
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code in (429, 502):
                print(f"Rate limit ou erreur serveur, retry dans {sleep_sec}s...")
                time.sleep(sleep_sec)
            else:
                print(f"Erreur HTTP {r.status_code} pour {url}")
                return None
        except Exception as e:
            print(f"Exception lors de l'appel API : {e}")
            time.sleep(sleep_sec)
    return None

def main():
    print(f"Récupération de la liste des tokens de la catégorie : {category}")
    url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category={category}&order=market_cap_desc&per_page=250&page=1"
    tokens = fetch_with_retry(url)
    if not tokens:
        print("Impossible de récupérer la liste des tokens.")
        return

    tokens = [t for t in tokens if t.get("market_cap", 0) and t.get("total_volume", 0)]
    print(f"{len(tokens)} tokens après filtrage market cap / volume.")

    data_list = []
    for i, t in enumerate(tokens[:LIMIT]):
        token_id = t["id"]
        print(f"\n{i+1}/{LIMIT} Extraction des données pour : {token_id}")
        detail_url = f"https://api.coingecko.com/api/v3/coins/{token_id}"
        detail = fetch_with_retry(detail_url)
        if not detail:
            print(f"Erreur API CoinGecko pour {token_id}")
            continue

        # Extraction simple et sécurisée (adapte si tu veux plus de champs)
        d = {
            "id_projet": detail.get("id"),
            "symbole": detail.get("symbol"),
            "nom": detail.get("name"),
            "image": detail.get("image", {}).get("large"),
            "prix_usd": detail.get("market_data", {}).get("current_price", {}).get("usd"),
            "market_cap": detail.get("market_data", {}).get("market_cap", {}).get("usd"),
            "market_cap_rank": detail.get("market_cap_rank"),
            "volume_24h": detail.get("market_data", {}).get("total_volume", {}).get("usd"),
            "ath_usd": detail.get("market_data", {}).get("ath", {}).get("usd"),
            "ath_date": detail.get("market_data", {}).get("ath_date", {}).get("usd"),
            "variation_24h_pct": detail.get("market_data", {}).get("price_change_percentage_24h"),
            "chaine_contrat": None,
            "adresse_contrat": None,
            "lien_site_web": detail.get("links", {}).get("homepage", [None])[0],
            "lien_github": detail.get("links", {}).get("repos_url", {}).get("github", [None])[0],
            "derniere_maj": detail.get("last_updated"),
        }
        # Essayons de détecter chain/contrat automatiquement (ETH/L2...)
        contracts = detail.get("platforms", {})
        if contracts:
            for chain, contract in contracts.items():
                if contract:
                    d["chaine_contrat"] = chain
                    d["adresse_contrat"] = contract
                    break
        print("  => Données extraites :", d)
        data_list.append(d)
        time.sleep(2)  # pas plus de 30 calls/minute en free

    if not data_list:
        print("Aucune donnée à uploader.")
        return

    df = pd.DataFrame(data_list)
    print("\nAperçu DataFrame à uploader :")
    print(df.head())

    # Upload sur BigQuery
    creds = service_account.Credentials.from_service_account_file(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    )
    client = bigquery.Client(credentials=creds, project=creds.project_id)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print("Upload terminé sur BigQuery.")

if __name__ == "__main__":
    main()
