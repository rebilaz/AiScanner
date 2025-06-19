# config.py

# --- CLÉS D'API (À REMPLIR) ---
# Obtenez votre clé sur https://www.coingecko.com/en/api/pricing
COINGECKO_API_KEY = "CG-bBGEZGSKWDG95LXQcqbFmGg4" 

# Clés pour des extensions futures (non utilisées par ce script pour le moment)
GITHUB_PAT = "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"
ETHERSCAN_API_KEY = "ITJ8KQVT96FPGYCJSIUT4YQBKDZ99HWV2B"
BSCSCAN_API_KEY = "ITJ8KQVT96FPGYCJSIUT4YQBKDZ99HWV2B"
POLYGONSCAN_API_KEY = "ITJ8KQVT96FPGYCJSIUT4YQBKDZ99HWV2B"
GOOGLE_CREDENTIALS_PATH = r"C:\Users\zrebi\Desktop\crypto-analysis-engine\starlit-verve-458814-u9-fb0541c07a00.json"

# --- CONFIGURATION API COINGECKO ---
COINGECKO_API_BASE_URL = "https://api.coingecko.com/api/v3"

# --- INFOS PROJET GOOGLE CLOUD ---
BIGQUERY_PROJECT_ID = "starlit-verve-458814-u9"
BIGQUERY_DATASET_ID = "projectscanner"
BIGQUERY_MASTER_TABLE = "Market_data"

# --- FILTRES DE SÉLECTION DES PROJETS ---
TARGET_CATEGORY = "artificial-intelligence"
MIN_MARKET_CAP = 10000000
MIN_VOLUME_USD = 50000

# --- PARAMÈTRES D'EXÉCUTION (non utilisés par ce script) ---
BATCH_SIZE = 20
TEST_MODE = False