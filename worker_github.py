# Fichier : worker_github.py
import requests
import time
import re
import json
from datetime import datetime, timezone
from google.cloud import bigquery
import pandas as pd
from typing import Dict, Any
from config import (
    GITHUB_PAT, GOOGLE_CLOUD_PROJECT_ID, BIGQUERY_DATASET,
    BIGQUERY_STATIC_TABLE, BATCH_SIZE, TEST_MODE
)

def analyze_github_quality() -> bool:
    print("--- Démarrage du Worker GitHub : Analyse de la qualité...")
    client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT_ID)
    table_id = f"{GOOGLE_CLOUD_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_STATIC_TABLE}"

    try:
        query = f"SELECT * FROM `{table_id}` WHERE lien_github IS NOT NULL AND lien_github != 'N/A' AND score_confiance_github IS NULL"
        tasks_to_do_df = client.query(query).to_dataframe()

        if tasks_to_do_df.empty:
            print("-> Aucun nouveau projet avec un lien GitHub à analyser.")
            return True

        all_projects_df = client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
        batch = tasks_to_do_df.head(BATCH_SIZE)
        print(f"-> {len(tasks_to_do_df)} projets à analyser au total. Traitement d'un lot de {len(batch)}.")

        for _, row in batch.iterrows():
            project_id = row['id_projet']
            url = row['lien_github']
            print(f"  -> Analyse de : {url}")
            
            try:
                best_repo_url = url
                match = re.match(r'^https:\/\/github\.com\/([^\/]+)\/?$', url)
                if match:
                    best_repo = find_best_repo_for(match.group(1))
                    best_repo_url = best_repo['html_url'] if best_repo else url
                
                analysis = analyze_specific_repo(best_repo_url)
                if analysis:
                    original_index = all_projects_df[all_projects_df['id_projet'] == project_id].index
                    if not original_index.empty:
                        all_projects_df.loc[original_index, 'depot_github_choisi'] = best_repo_url
                        all_projects_df.loc[original_index, 'score_confiance_github'] = int(analysis['score'])
                        all_projects_df.loc[original_index, 'type_depot'] = analysis['type']
            except Exception as e:
                print(f"    Erreur lors de l'analyse de {project_id}: {e}")
                original_index = all_projects_df[all_projects_df['id_projet'] == project_id].index
                if not original_index.empty:
                    all_projects_df.loc[original_index, 'score_confiance_github'] = -999
                    all_projects_df.loc[original_index, 'type_depot'] = "Erreur d'analyse"
            
            if not TEST_MODE: time.sleep(0.5)

        print(f"-> Réécriture de la table {table_id} avec les nouveaux scores...")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(all_projects_df, table_id, job_config=job_config).result()

        print(f"-> Succès ! La table a été mise à jour.")
        return True

    except Exception as e:
        print(f"Une erreur est survenue dans le worker GitHub : {e}")
        return False

def find_best_repo_for(owner: str) -> Dict[str, Any] | None:
    headers = {'Authorization': f'Bearer {GITHUB_PAT}'}
    response = requests.get(f"https://api.github.com/users/{owner}/repos?type=owner&sort=pushed&per_page=100", headers=headers)
    if response.status_code != 200:
        response = requests.get(f"https://api.github.com/orgs/{owner}/repos?type=public&sort=pushed&per_page=100", headers=headers)
        if response.status_code != 200: return None
    repos = response.json()
    if not repos: return None

    best_repo, highest_score = None, -float('inf')
    for repo in repos:
        if repo.get('archived') or repo.get('fork'): continue
        relevance_score = 0
        name = repo.get('name', '').lower()
        if any(kw in name for kw in ["protocol", "core", "dapp", "contracts"]): relevance_score += 100
        if any(kw in name for kw in ["website", "docs", ".github.io"]): relevance_score -= 200
        relevance_score += repo.get('stargazers_count', 0)
        relevance_score += repo.get('forks_count', 0) * 2
        last_push = datetime.fromisoformat(repo['pushed_at'].replace('Z', '+00:00'))
        if (datetime.now(timezone.utc) - last_push).days < 90: relevance_score += 50
        if relevance_score > highest_score:
            highest_score, best_repo = relevance_score, repo
    return best_repo

def analyze_specific_repo(repo_url: str) -> Dict[str, Any] | None:
    if TEST_MODE:
        with open('test_data_repo.json', 'r') as f_repo: repo_data = json.load(f_repo)
    else:
        match = re.match(r"github\.com\/([^\/]+)\/([^\/]+)", repo_url)
        if not match: raise ValueError("URL de dépôt invalide.")
        owner, repo_name = match.group(1), match.group(2).replace('.git', '')
        headers = {'Authorization': f'Bearer {GITHUB_PAT}'}
        repo_response = requests.get(f"https://api.github.com/repos/{owner}/{repo_name}", headers=headers)
        if repo_response.status_code != 200: return None
        repo_data = repo_response.json()

    score = 0
    months_since_commit = (datetime.now(timezone.utc) - datetime.fromisoformat(repo_data['pushed_at'].replace('Z', '+00:00'))).days / 30.44
    if months_since_commit > 12: score -= 50
    elif months_since_commit > 6: score -= 20
    elif months_since_commit < 2: score += 30
    score += min(20, (repo_data.get('stargazers_count', 0) // 500))
    
    repo_type = "dApp / Protocole Probable" if score > 0 else "Site Web / Incertain"
    return {'score': score, 'type': repo_type}