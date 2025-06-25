"""Simple worker that fetches, processes, and uploads data to BigQuery."""

import os
from pathlib import Path
from typing import List, Dict

from google.cloud import bigquery


def fetch_data_from_api() -> List[Dict[str, str]]:
    """Fetch records from a data source.

    Returns a list of dictionaries representing rows to upload. In a real-world
    scenario this would call an external API.
    """

    print("Fetching data from API...")
    return [{"id": 1, "value": "hello"}]


def process_data(api_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Process raw API data into rows ready for BigQuery."""

    print("Processing data...")
    processed = [{"id": row["id"], "value": row["value"].upper()} for row in api_data]
    return processed


def main() -> None:  # pragma: no cover - simple integration script
    """Run the worker and upload results to BigQuery."""

    print("--- WORKER STARTED ---")

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set"
        )

    credentials_file = Path(credentials_path)
    print(f"Using credentials file: {credentials_file}")
    if not credentials_file.is_file():
        raise FileNotFoundError(
            f"Credential file does not exist: {credentials_file}"
        )

    client = bigquery.Client.from_service_account_json(str(credentials_file))
    print("BigQuery client created successfully.")

    api_data = fetch_data_from_api()
    print(f"Fetched {len(api_data)} records.")
    if not api_data:
        print("Error: API returned no data.")
        raise ValueError("No data available to insert into BigQuery.")

    rows_to_insert = process_data(api_data)
    print(f"{len(rows_to_insert)} rows ready for insertion.")
    if not rows_to_insert:
        print("Error: processed data is empty.")
        raise ValueError("No data available to insert into BigQuery.")

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET")
    table = os.getenv("BQ_TABLE")
    if not all([project_id, dataset, table]):
        raise RuntimeError(
            "Missing BigQuery configuration (GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE)"
        )

    table_id = f"{project_id}.{dataset}.{table}"

    print(f"Inserting data into BigQuery table {table_id}...")
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        raise RuntimeError(f"Failed to insert rows into BigQuery: {errors}")

    print("--- WORKER FINISHED ---")


if __name__ == "__main__":
    main()
