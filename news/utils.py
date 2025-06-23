"""Utility functions for the news aggregator."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

import yaml
from google.cloud import bigquery


def load_config(path: str) -> Dict[str, Any]:
    """Load YAML configuration from the given path."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def article_exists(client: bigquery.Client, dataset: str, table: str, url: str) -> bool:
    """Return True if the article URL already exists in the destination table."""
    query = (
        f"SELECT 1 FROM `{client.project}.{dataset}.{table}` "
        "WHERE url = @url LIMIT 1"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("url", "STRING", url)]
    )
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        return not df.empty
    except Exception:  # pragma: no cover - network/credentials issues
        logging.exception("Failed to check existence for %s", url)
        return False
