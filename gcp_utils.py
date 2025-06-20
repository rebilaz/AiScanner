"""Helper functions for Google BigQuery interactions."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from google.cloud import bigquery


logger = logging.getLogger(__name__)


def get_bq_client() -> bigquery.Client:
    return bigquery.Client()


def upload_dataframe(df: pd.DataFrame, table_id: str) -> None:
    """Upload a DataFrame to BigQuery."""
    client = get_bq_client()
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    logger.info("Uploaded %d rows to %s", len(df), table_id)
