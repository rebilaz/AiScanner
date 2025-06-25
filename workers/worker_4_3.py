"""Infer honeypot risk from contract opcodes using pretrained ML models."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import joblib
import pandas as pd
from google.cloud import bigquery
from gcp_utils import create_bq_client
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.base import BaseEstimator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
CODE_TABLE = os.getenv("BQ_CONTRACT_CODE_TABLE", "contract_code")
DEST_TABLE = os.getenv("BQ_ML_ANALYSIS_TABLE", "contract_ml_analysis")
VECTORIZER_PATH = os.getenv("OPCODE_VECTORIZER", "vectorizer.joblib")
MODEL_PATH = os.getenv("OPCODE_MODEL", "model.joblib")
BATCH_SIZE = int(os.getenv("ML_ANALYSIS_BATCH_SIZE", "20"))

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_contracts_to_analyze(client: bigquery.Client, limit: int) -> pd.DataFrame:
    """Return contracts with opcodes not yet processed."""
    query = f"""
        SELECT cc.contract_address, cc.opcodes
        FROM `{client.project}.{DATASET}.{CODE_TABLE}` AS cc
        LEFT JOIN `{client.project}.{DATASET}.{DEST_TABLE}` AS ml
        ON LOWER(cc.contract_address) = LOWER(ml.contract_address)
        WHERE ml.contract_address IS NULL AND cc.opcodes IS NOT NULL
        LIMIT {limit}
    """
    return client.query(query).to_dataframe()


def load_models() -> tuple[TfidfVectorizer, BaseEstimator]:
    """Load vectorizer and ML model from disk."""
    vectorizer: TfidfVectorizer = joblib.load(VECTORIZER_PATH)
    model: BaseEstimator = joblib.load(MODEL_PATH)
    return vectorizer, model


def store_results(client: bigquery.Client, rows: List[Dict[str, Any]]) -> None:
    """Append ML analysis results to BigQuery."""
    if not rows:
        logging.info("No ML analysis results to store")
        return
    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    df = pd.DataFrame(rows)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Uploading %d ML analysis rows to %s", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


# ---------------------------------------------------------------------------
# Main worker
# ---------------------------------------------------------------------------

def run_ml_analysis_worker() -> None:
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration (GCP_PROJECT_ID, BQ_DATASET)")
        return

    client = create_bq_client(PROJECT_ID)
    contracts = get_contracts_to_analyze(client, BATCH_SIZE)
    if contracts.empty:
        logging.info("No contracts to analyze")
        return

    vectorizer, model = load_models()
    rows: List[Dict[str, Any]] = []
    for _, row in contracts.iterrows():
        opcodes = row["opcodes"]
        X = vectorizer.transform([opcodes])
        prob = 0.0
        pred_label: str | None = None
        try:
            if hasattr(model, "predict_proba"):
                proba = model.predict_proba(X)
                prob = float(proba[:, 1][0])
            pred = model.predict(X)
            pred_label = str(pred[0])
        except Exception:
            logging.exception("ML inference failed for %s", row["contract_address"])
        rows.append(
            {
                "contract_address": row["contract_address"].lower(),
                "honeypot_probability": prob,
                "predicted_vuln_type": pred_label,
                "ml_details": json.dumps({"model": os.path.basename(MODEL_PATH) }),
                "timestamp": datetime.now(tz=timezone.utc),
            }
        )

    store_results(client, rows)


if __name__ == "__main__":
    run_ml_analysis_worker()
