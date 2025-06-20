# Fichier : fetch_github_test_data.py (Corrigé)
import requests
import json # <-- AJOUT DE L'IMPORT
from config import PAT_GITHUB

REPO_OWNER = "golemfactory"
REPO_NAME = "golem"
REPO_OUTPUT_FILE = "test_data_repo.json"
LANGUAGES_OUTPUT_FILE = "test_data_languages.json"

print(f"Récupération des données de test pour le dépôt : {REPO_OWNER}/{REPO_NAME}...")

headers = {'Authorization': f'Bearer {PAT_GITHUB}'}
base_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"

try:
    if not PAT_GITHUB or PAT_GITHUB == "VOTRE_PAT_GITHUB_ICI":
        raise ValueError("La clé API GitHub (PAT_GITHUB) n'est pas configurée dans config.py")

    # Appel 1: Infos générales
    repo_response = requests.get(base_url, headers=headers)
    repo_response.raise_for_status()
    with open(REPO_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(repo_response.json(), f, ensure_ascii=False, indent=4)
    print(f"-> Infos du dépôt sauvegardées dans '{REPO_OUTPUT_FILE}'.")

    # Appel 2: Langages
    lang_response = requests.get(f"{base_url}/languages", headers=headers)
    lang_response.raise_for_status()
    with open(LANGUAGES_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(lang_response.json(), f, ensure_ascii=False, indent=4)
    print(f"-> Langages du dépôt sauvegardés dans '{LANGUAGES_OUTPUT_FILE}'.")
    
    print("\nSuccès ! Données de test GitHub prêtes.")

except Exception as e:
    print(f"Une erreur est survenue : {e}")