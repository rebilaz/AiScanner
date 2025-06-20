# Fichier : worker_github.py
"""Worker d'analyse GitHub.

Ce module analyse les dépôts GitHub de projets répertoriés dans
``Market_data`` et écrit le résultat dans ``github_score``.
L'implémentation s'inspire de ``worker_coingecko.py`` pour rester
cohérente avec les autres workers du projet.
"""

from __future__ import annotations

import os
import sys
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import config

# ---------------------------------------------------------------------------
# Chargement de la configuration
# ---------------------------------------------------------------------------

load_dotenv()

PAT_GITHUB = os.getenv("PAT_GITHUB", config.PAT_GITHUB)
GOOGLE_CLOUD_PROJECT_ID = os.getenv(
    "GOOGLE_CLOUD_PROJECT_ID", config.GOOGLE_CLOUD_PROJECT_ID
)
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", config.BIGQUERY_DATASET)
SOURCE_TABLE = os.getenv("BIGQUERY_SOURCE_TABLE", config.BIGQUERY_STATIC_TABLE)
DEST_TABLE = os.getenv("BIGQUERY_DEST_TABLE", "github_score")
BATCH_SIZE = getattr(config, "BATCH_SIZE", 20)
TEST_MODE = getattr(config, "TEST_MODE", False)

# Authentification BigQuery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.GOOGLE_CREDENTIALS_PATH

# Identifiants complets des tables
SOURCE_TABLE_ID = f"{GOOGLE_CLOUD_PROJECT_ID}.{BIGQUERY_DATASET}.{SOURCE_TABLE}"
DEST_TABLE_ID = f"{GOOGLE_CLOUD_PROJECT_ID}.{BIGQUERY_DATASET}.{DEST_TABLE}"

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


@dataclass
class RepoAnalysis:
    """Données finales insérées dans BigQuery."""

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
    """Analyseur GitHub avec gestion de la rate limit."""

    API_BASE = "https://api.github.com"

    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            }
        )

    # ------------------------------ Helpers ---------------------------------
    def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Effectue une requête GET avec gestion du rate limit."""
        for _ in range(3):
            response = self.session.get(url, params=params, timeout=15)
            if response.status_code == 202:
                # GitHub calcule encore la statistique (cas des endpoints /stats)
                time.sleep(2)
                continue
            if (
                response.status_code == 403
                and int(response.headers.get("X-RateLimit-Remaining", 1)) == 0
            ):
                reset_ts = int(response.headers.get("X-RateLimit-Reset", time.time()))
                wait = max(reset_ts - int(time.time()), 1)
                logging.warning("Rate limit atteint, pause de %s s", wait)
                time.sleep(wait)
                continue
            if response.status_code == 404:
                return None
            if response.ok:
                return response.json()
            logging.warning(
                "Appel %s renvoyé %s", url, response.status_code
            )
            time.sleep(2)
        return None

    def find_best_repo(self, owner: str) -> Optional[Dict[str, Any]]:
        """Sélectionne le dépôt le plus pertinent pour un owner."""
        repos = self._get_json(
            f"{self.API_BASE}/users/{owner}/repos",
            params={"type": "owner", "sort": "pushed", "per_page": 100},
        )
        if repos is None:
            repos = self._get_json(
                f"{self.API_BASE}/orgs/{owner}/repos",
                params={"type": "public", "sort": "pushed", "per_page": 100},
            )
        if not repos:
            return None

        best_score = -1
        best_repo = None
        for repo in repos:
            if repo.get("archived") or repo.get("fork"):
                continue
            score = repo.get("stargazers_count", 0)
            score += repo.get("forks_count", 0) * 2
            name = repo.get("name", "").lower()
            if any(k in name for k in ["protocol", "core", "contracts", "dapp"]):
                score += 100
            if any(k in name for k in ["website", "docs", ".github.io"]):
                score -= 100
            if score > best_score:
                best_score = score
                best_repo = repo
        return best_repo

    # ----------------------------- Scoring ----------------------------------
    def compute_scores(self, owner: str, repo: str) -> Optional[RepoAnalysis]:
        repo_data = self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}")
        if not repo_data:
            return None

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # Activity score -----------------------------------------------------
        activity_score = 0.0
        commits = self._get_json(
            f"{self.API_BASE}/repos/{owner}/{repo}/stats/commit_activity"
        )
        if commits:
            last_month = commits[-4:]
            total_commits = sum(week.get("total", 0) for week in last_month)
            activity_score += min(total_commits / 10.0, 20)
        latest_release = self._get_json(
            f"{self.API_BASE}/repos/{owner}/{repo}/releases/latest"
        )
        if latest_release and latest_release.get("published_at"):
            try:
                published = datetime.strptime(
                    latest_release["published_at"], "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)
                days = (datetime.now(timezone.utc) - published).days
                if days < 90:
                    activity_score += 5
            except Exception:
                pass
        open_issues = repo_data.get("open_issues_count", 0)
        activity_score += max(0, 5 - open_issues / 50)

        # Community score ----------------------------------------------------
        community_score = 0.0
        community_score += min(repo_data.get("stargazers_count", 0) / 100.0, 20)
        community_score += min(repo_data.get("forks_count", 0) / 50.0, 10)
        contributors = self._get_json(
            f"{self.API_BASE}/repos/{owner}/{repo}/contributors",
            params={"per_page": 1, "anon": "true"},
        )
        if isinstance(contributors, list) and contributors:
            community_score += 5

        # Good practices score ----------------------------------------------
        good_score = 0.0
        if repo_data.get("license"):
            good_score += 5
        readme = self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/readme")
        if readme and readme.get("size", 0) > 1000:
            good_score += 5
        if self._get_json(
            f"{self.API_BASE}/repos/{owner}/{repo}/contents/CONTRIBUTING.md"
        ):
            good_score += 5
        workflows = self._get_json(
            f"{self.API_BASE}/repos/{owner}/{repo}/actions/workflows"
        )
        if workflows and workflows.get("total_count", 0) > 0:
            good_score += 5

        # Code relevance score ----------------------------------------------
        code_score = 0.0
        pkg_files = ["requirements.txt", "package.json"]
        for file in pkg_files:
            if self._get_json(
                f"{self.API_BASE}/repos/{owner}/{repo}/contents/{file}"
            ):
                code_score += 5
                break
        if self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/contents/tests"):
            code_score += 5
        if self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}/contents/contracts"):
            code_score += 5

        total = activity_score + community_score + good_score + code_score
        repo_type = (
            "dApp / Protocole Probable" if code_score >= 5 else "Site Web / Incertain"
        )

        return RepoAnalysis(
            id_projet=repo_data.get("name"),
            depot_github=f"https://github.com/{owner}/{repo}",
            date_analyse=now,
            score_activite=activity_score,
            score_communaute=community_score,
            score_bonnes_pratiques=good_score,
            score_pertinence_code=code_score,
            score_total=total,
            type_depot=repo_type,
        )

    # ---------------------------------------------------------------------
    def analyze(self, project_id: str, url: str) -> Optional[RepoAnalysis]:
        """Analyse un projet à partir de son URL GitHub."""
        # URL possible: https://github.com/owner/repo ou https://github.com/owner
        parts = url.rstrip("/").split("/")[-2:]
        if len(parts) == 1:
            best = self.find_best_repo(parts[0])
            if not best:
                logging.warning("Aucun dépôt trouvé pour %s", url)
                return None
            owner = best["owner"]["login"]
            repo = best["name"]
        else:
            owner, repo = parts
        analysis = self.compute_scores(owner, repo)
        if analysis:
            analysis.id_projet = project_id
        return analysis


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_github_worker() -> bool:
    """Point d'entrée du worker GitHub."""
    logging.info("Démarrage du worker GitHub")
    client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT_ID)

    # Création de la table destination si nécessaire
    try:
        client.get_table(DEST_TABLE_ID)
    except NotFound:
        schema = [
            bigquery.SchemaField("id_projet", "STRING"),
            bigquery.SchemaField("depot_github", "STRING"),
            bigquery.SchemaField("date_analyse", "TIMESTAMP"),
            bigquery.SchemaField("score_activite", "FLOAT"),
            bigquery.SchemaField("score_communaute", "FLOAT"),
            bigquery.SchemaField("score_bonnes_pratiques", "FLOAT"),
            bigquery.SchemaField("score_pertinence_code", "FLOAT"),
            bigquery.SchemaField("score_total", "FLOAT"),
            bigquery.SchemaField("type_depot", "STRING"),
        ]
        table = bigquery.Table(DEST_TABLE_ID, schema=schema)
        client.create_table(table)
        logging.info("Table %s créée", DEST_TABLE_ID)

    query = (
        f"SELECT t1.id_projet, t1.lien_github FROM `{SOURCE_TABLE_ID}` t1 "
        f"LEFT JOIN `{DEST_TABLE_ID}` t2 ON t1.id_projet = t2.id_projet "
        "WHERE t2.id_projet IS NULL AND t1.lien_github IS NOT NULL"
    )
    tasks = client.query(query).to_dataframe()
    if tasks.empty:
        logging.info("Aucun nouveau projet à analyser.")
        return True

    analyzer = GitHubAnalyzer(PAT_GITHUB)
    results: List[Dict[str, Any]] = []

    batch = tasks.head(BATCH_SIZE)
    logging.info("%d projets à analyser (lot de %d)", len(tasks), len(batch))

    for _, row in batch.iterrows():
        project_id = row["id_projet"]
        url = row["lien_github"]
        logging.info("Analyse de %s", url)
        try:
            analysis = analyzer.analyze(project_id, url)
            if analysis:
                results.append(analysis.to_dict())
        except Exception as exc:
            logging.exception("Erreur lors de l'analyse de %s", project_id)
        if not TEST_MODE:
            time.sleep(1)

    if results:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        df = pd.DataFrame(results)
        # Conversion silencieuse de la date au format datetime64[ns, UTC]
        if "date_analyse" in df.columns:
            df["date_analyse"] = pd.to_datetime(
                df["date_analyse"], errors="coerce", utc=True
            )
        client.load_table_from_dataframe(df, DEST_TABLE_ID, job_config=job_config).result()
        logging.info("%d résultats ajoutés à %s", len(df), DEST_TABLE_ID)
    return True


if __name__ == "__main__":
    run_github_worker()
