# Fichier : fetch_github_test_data.py
# Ce script ne sert qu'une seule fois : récupérer des exemples de données GitHub.
import requests
import json
from config import PAT_GITHUB

# Un exemple de dépôt de code de dApp/protocole (celui de Golem)
# pour avoir des données de test réalistes.
REPO_OWNER = "golemfactory"
REPO_NAME = "golem"

# Noms des fichiers de sortie
REPO_OUTPUT_FILE = "test_data_repo.json"
LANGUAGES_OUTPUT_FILE = "test_data_languages.json"

print(f"Récupération des données de test pour le dépôt : {REPO_OWNER}/{REPO_NAME}...")

headers = {'Authorization': f'Bearer {PAT_GITHUB}'}
base_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"

try:
    # Appel 1: Infos générales du dépôt
    repo_response = requests.get(base_url, headers=headers)
    repo_response.raise_for_status()
    with open(REPO_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(repo_response.json(), f, ensure_ascii=False, indent=4)
    print(f"-> Infos du dépôt sauvegardées dans '{REPO_OUTPUT_FILE}'.")

    # Appel 2: Langages du dépôt
    lang_response = requests.get(f"{base_url}/languages", headers=headers)
    lang_response.raise_for_status()
    with open(LANGUAGES_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(lang_response.json(), f, ensure_ascii=False, indent=4)
    print(f"-> Langages du dépôt sauvegardés dans '{LANGUAGES_OUTPUT_FILE}'.")
    
    print("\nSuccès ! Données de test GitHub prêtes.")

except Exception as e:
    print(f"Une erreur est survenue : {e}")