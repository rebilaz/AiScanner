"""Run Slither static analysis on smart contract source code and store scores in BigQuery."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from google.cloud import bigquery
from gcp_utils import create_bq_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

import socket
import aiohttp

def create_ipv4_aiohttp_session() -> aiohttp.ClientSession:
    """
    Crée et retourne une session aiohttp pré-configurée pour forcer l'utilisation
    de l'IPv4.

    Ceci est le correctif pour le bug "Network is unreachable" dans WSL2.
    """
    # Crée le connecteur qui force l'utilisation de l'IPv4
    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    
    # Crée une session en utilisant ce connecteur et la retourne
    print("INFO: Création d'une session aiohttp avec le correctif IPv4.")
    return aiohttp.ClientSession(connector=connector)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
CODE_TABLE = os.getenv("BQ_CONTRACT_CODE_TABLE", "contract_code")
DEST_TABLE = os.getenv("BQ_STATIC_ANALYSIS_TABLE", "contract_static_analysis")
BATCH_SIZE = int(os.getenv("STATIC_ANALYSIS_BATCH_SIZE", "20"))

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_contracts_to_scan(client: bigquery.Client, limit: int) -> pd.DataFrame:
    """Return contracts with source code not yet analyzed."""
    query = f"""
        SELECT cc.contract_address, cc.source_code
        FROM `{client.project}.{DATASET}.{CODE_TABLE}` AS cc
        LEFT JOIN `{client.project}.{DATASET}.{DEST_TABLE}` AS sa
        ON LOWER(cc.contract_address) = LOWER(sa.contract_address)
        WHERE sa.contract_address IS NULL AND cc.source_code != ''
        LIMIT {limit}
    """
    return client.query(query).to_dataframe()


def run_slither(source_code: str) -> tuple[int, List[Dict[str, Any]]]:
    """Execute Slither on the provided Solidity code and return a score."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sol_path = os.path.join(tmpdir, "contract.sol")
        json_path = os.path.join(tmpdir, "results.json")
        with open(sol_path, "w", encoding="utf-8") as f:
            f.write(source_code)
        try:
            subprocess.run(
                ["slither", sol_path, "--json", json_path],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as exc:
            logging.error("Slither failed: %s", exc)
            return 0, []
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            logging.exception("Failed to load Slither output")
            return 0, []

    detectors = data.get("results", {}).get("detectors", [])
    high = sum(1 for d in detectors if d.get("impact") == "High")
    medium = sum(1 for d in detectors if d.get("impact") == "Medium")
    low = sum(1 for d in detectors if d.get("impact") == "Low")
    score = max(0, 100 - (10 * high + 3 * medium + low))
    return score, detectors


def store_results(client: bigquery.Client, rows: List[Dict[str, Any]]) -> None:
    """Append analysis results to BigQuery."""
    if not rows:
        logging.info("No analysis results to store")
        return
    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    df = pd.DataFrame(rows)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Uploading %d static analysis rows to %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


# ---------------------------------------------------------------------------
# Main worker
# ---------------------------------------------------------------------------

def run_static_analysis_worker() -> None:
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    client = create_bq_client(PROJECT_ID)
    contracts = get_contracts_to_scan(client, BATCH_SIZE)
    if contracts.empty:
        logging.info("No contracts to analyze")
        return

    results: List[Dict[str, Any]] = []
    for _, row in contracts.iterrows():
        score, vulns = run_slither(row["source_code"])
        results.append(
            {
                "contract_address": row["contract_address"].lower(),
                "score_static": score,
                "vulnerabilities_list": json.dumps(vulns),
                "timestamp": datetime.now(tz=timezone.utc),
            }
        )

    store_results(client, results)


if __name__ == "__main__":
    run_static_analysis_worker()
