"""GitHub analysis worker refactored for integration with main.py."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# Assurez-vous d'avoir un fichier gcp_utils.py ou intégrez cette classe
# Exemple de classe BigQueryClient nécessaire
class BigQueryClient:
    def __init__(self, project_id: str):
        from google.cloud import bigquery
        self.client = bigquery.Client(project=project_id)

    def ensure_dataset_exists(self, dataset_id: str):
        from google.cloud import bigquery
        dataset_ref = self.client.dataset(dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            logging.info("Le dataset %s existe déjà.", dataset_id)
        except NotFound:
            logging.warning("Le dataset %s n'existe pas. Création...", dataset_id)
            self.client.create_dataset(bigquery.Dataset(dataset_ref))
            logging.info("Dataset %s créé.", dataset_id)

    def upload_dataframe(self, df: pd.DataFrame, dataset_id: str, table_id: str):
        from google.cloud import bigquery
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logging.info("DataFrame chargé avec succès dans %s.%s", dataset_id, table_id)


def create_ipv4_aiohttp_session() -> aiohttp.ClientSession:
    """
    Crée et retourne une session aiohttp pré-configurée pour forcer l'utilisation
    de l'IPv4.

    Ceci est le correctif pour le bug "Network is unreachable" dans WSL2.
    """
    logging.debug("Création d'un connecteur aiohttp avec la famille AF_INET (IPv4).")
    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    return aiohttp.ClientSession(connector=connector)


@dataclass
class RepoAnalysis:
    """Data structure stored in BigQuery."""

    id_projet: str
    depot_github: str
    date_analyse: str
    score_activite: float
    score_communaute: float
    score_bonnes_pratiques: float
    score_pertinence_code: float
    score_total: float
    type_depot: str

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__


class GitHubAnalyzer:
    """Asynchronous GitHub API analyzer with rate limit handling."""

    API_BASE = "https://api.github.com"

    def __init__(self, token: str, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            }
        )

    async def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        for attempt in range(3):
            logging.debug("Tentative #%d pour GET %s", attempt + 1, url)
            try:
                async with self.session.get(url, params=params, timeout=15) as response:
                    if response.status == 202:
                        logging.info("API a répondu 202 (en cours de calcul), attente de 2s.")
                        await asyncio.sleep(2)
                        continue
                    if (
                        response.status == 403
                        and int(response.headers.get("X-RateLimit-Remaining", 1)) == 0
                    ):
                        reset_ts = int(response.headers.get("X-RateLimit-Reset", time.time()))
                        wait = max(reset_ts - int(time.time()), 1)
                        logging.warning("Rate limit atteint. Pause de %s secondes.", wait)
                        await asyncio.sleep(wait)
                        continue
                    if response.status == 404:
                        logging.warning("Ressource non trouvée (404) à l'URL : %s", url)
                        return None
                    if response.ok:
                        return await response.json()
                    
                    logging.warning(
                        "Appel API à %s a échoué avec le code %d.", url, response.status
                    )
                    await asyncio.sleep(2)
            except asyncio.TimeoutError:
                logging.error("Timeout lors de l'appel à %s", url)
                await asyncio.sleep(2)
        return None

    async def find_best_repo(self, owner: str) -> Optional[Dict[str, Any]]:
        url_users = f"{self.API_BASE}/users/{owner}/repos"
        repos = await self._get_json(
            url_users, params={"type": "owner", "sort": "pushed", "per_page": 100}
        )
        if repos is None:
            logging.info("Aucun dépôt trouvé pour l'utilisateur %s, essai en tant qu'organisation.", owner)
            url_orgs = f"{self.API_BASE}/orgs/{owner}/repos"
            repos = await self._get_json(
                url_orgs, params={"type": "public", "sort": "pushed", "per_page": 100}
            )
        if not repos:
            logging.warning("Aucun dépôt public trouvé pour %s.", owner)
            return None

        best_score = -1
        best_repo = None
        for repo in repos:
            if repo.get("archived") or repo.get("fork"):
                continue
            
            score = repo.get("stargazers_count", 0) + repo.get("forks_count", 0) * 2
            name = repo.get("name", "").lower()
            
            if any(k in name for k in ["protocol", "core", "contracts", "dapp"]):
                score += 100
            if any(k in name for k in ["website", "docs", ".github.io"]):
                score -= 100
            
            if score > best_score:
                best_score = score
                best_repo = repo
        
        if best_repo:
            logging.info("Meilleur dépôt trouvé pour %s : %s (score : %d)", owner, best_repo['name'], best_score)
        return best_repo

    async def compute_scores(self, owner: str, repo: str) -> Optional[RepoAnalysis]:
        logging.info("Début du calcul des scores pour %s/%s", owner, repo)
        
        tasks = {
            "repo_data": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}"),
            "commits": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/stats/commit_activity"),
            "latest_release": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/releases/latest"),
            "contributors": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/contributors", params={"per_page": 1, "anon": "true"}),
            "readme": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/readme"),
            "contributing": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/contents/CONTRIBUTING.md"),
            "workflows": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/actions/workflows"),
            "req_txt": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/contents/requirements.txt"),
            "pkg_json": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/contents/package.json"),
            "tests": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/contents/tests"),
            "contracts": self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/contents/contracts"),
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        data = dict(zip(tasks.keys(), results))

        repo_data = data.get("repo_data")
        if not isinstance(repo_data, dict):
            logging.error("Impossible de récupérer les données de base pour %s/%s. Erreur : %s", owner, repo, repo_data)
            return None

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        activity_score = 0.0
        if isinstance(data["commits"], list) and data["commits"]:
            total_commits = sum(week.get("total", 0) for week in data["commits"][-4:])
            activity_score += min(total_commits / 10.0, 20)
        if isinstance(data["latest_release"], dict) and data["latest_release"].get("published_at"):
            try:
                published = datetime.strptime(data["latest_release"]["published_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - published).days < 90:
                    activity_score += 5
            except Exception: pass
        activity_score += max(0, 5 - repo_data.get("open_issues_count", 0) / 50)
        logging.info("  -> Score d'activité : %.2f", activity_score)

        community_score = 0.0
        community_score += min(repo_data.get("stargazers_count", 0) / 100.0, 20)
        community_score += min(repo_data.get("forks_count", 0) / 50.0, 10)
        if isinstance(data["contributors"], list) and data["contributors"]:
            community_score += 5
        logging.info("  -> Score de communauté : %.2f", community_score)

        good_score = 0.0
        if repo_data.get("license"): good_score += 5
        if isinstance(data["readme"], dict) and data["readme"].get("size", 0) > 1000: good_score += 5
        if isinstance(data["contributing"], dict): good_score += 5
        if isinstance(data["workflows"], dict) and data["workflows"].get("total_count", 0) > 0: good_score += 5
        logging.info("  -> Score de bonnes pratiques : %.2f", good_score)
        
        code_score = 0.0
        if isinstance(data["req_txt"], dict) or isinstance(data["pkg_json"], dict): code_score += 5
        if isinstance(data["tests"], list): code_score += 5
        if isinstance(data["contracts"], list): code_score += 5
        logging.info("  -> Score de pertinence du code : %.2f", code_score)

        total = activity_score + community_score + good_score + code_score
        repo_type = "dApp / Protocole Probable" if code_score >= 10 else "Site Web / Incertain"
        logging.info("  => SCORE TOTAL pour %s/%s : %.2f", owner, repo, total)

        return RepoAnalysis(
            id_projet="",
            depot_github=f"https://github.com/{owner}/{repo}",
            date_analyse=now,
            score_activite=round(activity_score, 2),
            score_communaute=round(community_score, 2),
            score_bonnes_pratiques=round(good_score, 2),
            score_pertinence_code=round(code_score, 2),
            score_total=round(total, 2),
            type_depot=repo_type,
        )

    async def analyze(self, project_id: str, url: str) -> Optional[RepoAnalysis]:
        parts = url.strip("/").split("/")
        repo_path = "/".join(parts[3:])
        
        if len(repo_path.split('/')) == 1:
            owner = repo_path.split('/')[0]
            logging.info("URL de profil détectée (%s), recherche du meilleur dépôt...", owner)
            best = await self.find_best_repo(owner)
            if not best:
                logging.warning("Aucun dépôt pertinent trouvé pour l'URL %s", url)
                return None
            owner, repo = best["owner"]["login"], best["name"]
        else:
            owner, repo = repo_path.split('/')[:2]

        analysis = await self.compute_scores(owner, repo)
        if analysis:
            analysis.id_projet = project_id
        return analysis


async def run_github_worker() -> bool:
    logging.info("--- DÉMARRAGE DU WORKER GITHUB ---")
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    source_table = os.getenv("GITHUB_SOURCE_TABLE", "Market_data")
    dest_table = os.getenv("GITHUB_DEST_TABLE", "github_score")
    batch_size = int(os.getenv("GITHUB_BATCH_SIZE", "20"))
    token = os.getenv("PAT_GITHUB")
    
    logging.info("Configuration chargée :")
    logging.info("  - GCP Project ID: %s", project_id)
    logging.info("  - BigQuery Dataset: %s", dataset)
    logging.info("  - Table Source: %s", source_table)
    logging.info("  - Table Destination: %s", dest_table)
    logging.info("  - Taille du lot (Batch Size): %d", batch_size)

    if not all([project_id, dataset, token]):
        logging.error("Configuration incomplète. Vérifiez les variables d'environnement GCP_PROJECT_ID, BQ_DATASET, PAT_GITHUB.")
        return False
    
    # --- Assertions pour l'analyseur de type (Pylance) ---
    assert project_id is not None
    assert dataset is not None
    assert token is not None

    bq_client = BigQueryClient(project_id)
    bq_client.ensure_dataset_exists(dataset)
    client = bq_client.client
    dest_table_id = f"{project_id}.{dataset}.{dest_table}"

    try:
        client.get_table(dest_table_id)
    except NotFound:
        logging.warning("La table destination %s n'existe pas. Création...", dest_table_id)
        schema = [
            bigquery.SchemaField("id_projet", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("depot_github", "STRING"),
            bigquery.SchemaField("date_analyse", "TIMESTAMP"),
            bigquery.SchemaField("score_activite", "FLOAT"),
            bigquery.SchemaField("score_communaute", "FLOAT"),
            bigquery.SchemaField("score_bonnes_pratiques", "FLOAT"),
            bigquery.SchemaField("score_pertinence_code", "FLOAT"),
            bigquery.SchemaField("score_total", "FLOAT"),
            bigquery.SchemaField("type_depot", "STRING"),
        ]
        table = bigquery.Table(dest_table_id, schema=schema)
        client.create_table(table)
        logging.info("Table %s créée avec succès.", dest_table_id)

    source_table_id = f"{project_id}.{dataset}.{source_table}"
    query = (
        f"SELECT t1.id_projet, t1.lien_github FROM `{source_table_id}` AS t1 "
        f"LEFT JOIN `{dest_table_id}` AS t2 ON t1.id_projet = t2.id_projet "
        "WHERE t2.id_projet IS NULL AND t1.lien_github IS NOT NULL AND t1.lien_github != 'Indisponible'"
    )
    logging.info("Exécution de la requête pour trouver les nouveaux projets...")
    tasks_df = client.query(query).to_dataframe(create_bqstorage_client=False)
    
    if tasks_df.empty:
        logging.info("Aucun nouveau projet à analyser. Le worker a terminé sa tâche.")
        return True

    batch = tasks_df.head(batch_size)
    logging.info("%d projets à analyser au total. Traitement d'un lot de %d.", len(tasks_df), len(batch))

    async with create_ipv4_aiohttp_session() as session:
        analyzer = GitHubAnalyzer(token, session)
        results: List[Dict[str, Any]] = []

        for _, row in batch.iterrows():
            project_id_val = row["id_projet"]
            url = row["lien_github"]
            logging.info("--- Analyse du projet : %s (%s) ---", project_id_val, url)
            try:
                analysis = await analyzer.analyze(project_id_val, url)
                if analysis:
                    results.append(analysis.to_dict())
            except Exception:
                logging.exception("Erreur inattendue lors de l'analyse de %s.", project_id_val)
            await asyncio.sleep(1)

    if results:
        logging.info("Préparation de l'envoi de %d résultats vers BigQuery.", len(results))
        df = pd.DataFrame(results)
        if "date_analyse" in df.columns:
            df["date_analyse"] = pd.to_datetime(df["date_analyse"], errors="coerce", utc=True)
        
        try:
            bq_client.upload_dataframe(df, dataset, dest_table)
            logging.info("%d résultats ajoutés avec succès à %s.", len(df), dest_table_id)
        except Exception:
            logging.exception("Échec de l'envoi des données vers BigQuery.")
            return False
    else:
        logging.warning("Aucun résultat d'analyse n'a été produit dans ce lot.")

    logging.info("--- WORKER GITHUB : TERMINÉ AVEC SUCCÈS ---")
    return True

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    asyncio.run(run_github_worker())