import os
from dotenv import load_dotenv

# Charge le .env local (utile pour dev/local)
load_dotenv()

# --- CLÉS D'API ---
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
PAT_GITHUB = os.getenv("PAT_GITHUB")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
BSCSCAN_API_KEY = os.getenv("BSCSCAN_API_KEY")
POLYGONSCAN_API_KEY = os.getenv("POLYGONSCAN_API_KEY")

# --- CREDENTIALS GCP ---
DEFAULT_CREDENTIALS = "starlit-verve-458814-u9-fb0541c07a00.json"
GOOGLE_CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    os.path.join(os.path.dirname(__file__), DEFAULT_CREDENTIALS),
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS_PATH

# --- CONFIGURATION API COINGECKO ---
COINGECKO_API_BASE_URL = "https://api.coingecko.com/api/v3"

# --- INFOS PROJET GOOGLE CLOUD ---
BIGQUERY_PROJECT_ID = "starlit-verve-458814-u9"
BIGQUERY_DATASET_ID = "projectscanner"
BIGQUERY_MASTER_TABLE = "Market_data"

# --- Variables d'alias pour compatibilité ---
# Certains workers utilisent d'anciens noms de constantes ;
# on les redéfinit ici pour éviter des erreurs d'import.
GOOGLE_CLOUD_PROJECT_ID = BIGQUERY_PROJECT_ID
BIGQUERY_DATASET = BIGQUERY_DATASET_ID
BIGQUERY_STATIC_TABLE = BIGQUERY_MASTER_TABLE
BIGQUERY_ONCHAIN_TABLE = BIGQUERY_MASTER_TABLE
BIGQUERY_MARKET_TABLE = BIGQUERY_MASTER_TABLE

# --- FILTRES DE SÉLECTION DES PROJETS ---
TARGET_CATEGORY = "artificial-intelligence"
MIN_MARKET_CAP = 10000000
MIN_VOLUME_USD = 50000

# --- PARAMÈTRES D'EXÉCUTION (non utilisés par ce script) ---
BATCH_SIZE = 20
TEST_MODE = False
