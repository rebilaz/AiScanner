"""GitHub analysis worker refactored for integration with main.py."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from dotenv import load_dotenv

from gcp_utils import BigQueryClient


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
    """Simple GitHub API analyzer with rate limit handling."""

    API_BASE = "https://api.github.com"

    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            }
        )

    def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        for _ in range(3):
            response = self.session.get(url, params=params, timeout=15)
            if response.status_code == 202:
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
            logging.warning("Appel %s renvoyé %s", url, response.status_code)
            time.sleep(2)
        return None

    def find_best_repo(self, owner: str) -> Optional[Dict[str, Any]]:
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

    def compute_scores(self, owner: str, repo: str) -> Optional[RepoAnalysis]:
        repo_data = self._get_json(f"{self.API_BASE}/repos/{owner}/{repo}")
        if not repo_data:
            return None

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

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

        community_score = 0.0
        community_score += min(repo_data.get("stargazers_count", 0) / 100.0, 20)
        community_score += min(repo_data.get("forks_count", 0) / 50.0, 10)
        contributors = self._get_json(
            f"{self.API_BASE}/repos/{owner}/{repo}/contributors",
            params={"per_page": 1, "anon": "true"},
        )
        if isinstance(contributors, list) and contributors:
            community_score += 5

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

    def analyze(self, project_id: str, url: str) -> Optional[RepoAnalysis]:
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


async def run_github_worker() -> bool:
    """Entry point for the GitHub worker."""
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    source_table = os.getenv("GITHUB_SOURCE_TABLE", "Market_data")
    dest_table = os.getenv("GITHUB_DEST_TABLE", "github_score")
    batch_size = int(os.getenv("GITHUB_BATCH_SIZE", "20"))
    token = os.getenv("PAT_GITHUB")
    test_mode = os.getenv("GITHUB_TEST_MODE", "false").lower() == "true"

    if not project_id or not dataset or not token:
        logging.error("Missing configuration for GitHub worker")
        return False

    bq_client = BigQueryClient(project_id)
    bq_client.ensure_dataset_exists(dataset)
    client = bq_client.client

    dest_table_id = f"{project_id}.{dataset}.{dest_table}"
    source_table_id = f"{project_id}.{dataset}.{source_table}"

    try:
        client.get_table(dest_table_id)
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
        table = bigquery.Table(dest_table_id, schema=schema)
        client.create_table(table)
        logging.info("Table %s créée", dest_table_id)

    query = (
        f"SELECT t1.id_projet, t1.lien_github FROM `{source_table_id}` t1 "
        f"LEFT JOIN `{dest_table_id}` t2 ON t1.id_projet = t2.id_projet "
        "WHERE t2.id_projet IS NULL AND t1.lien_github IS NOT NULL"
    )
    tasks = client.query(query).to_dataframe(create_bqstorage_client=False)
    if tasks.empty:
        logging.info("Aucun nouveau projet à analyser.")
        return True

    analyzer = GitHubAnalyzer(token)
    results: List[Dict[str, Any]] = []

    batch = tasks.head(batch_size)
    logging.info("%d projets à analyser (lot de %d)", len(tasks), len(batch))

    for _, row in batch.iterrows():
        project_id = row["id_projet"]
        url = row["lien_github"]
        logging.info("Analyse de %s", url)
        try:
            analysis = analyzer.analyze(project_id, url)
            if analysis:
                results.append(analysis.to_dict())
        except Exception:
            logging.exception("Erreur lors de l'analyse de %s", project_id)
        if not test_mode:
            time.sleep(1)

    if results:
        df = pd.DataFrame(results)
        if "date_analyse" in df.columns:
            df["date_analyse"] = pd.to_datetime(df["date_analyse"], errors="coerce", utc=True).dt.tz_convert(None)
        bq_client.upload_dataframe(df, dataset, dest_table)
        logging.info("%d résultats ajoutés à %s", len(df), dest_table_id)
    return True
