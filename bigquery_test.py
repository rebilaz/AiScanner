import os
from pathlib import Path

from google.cloud import bigquery


def main() -> None:
    """Run a simple query against BigQuery using service account credentials."""
    print("Starting BigQuery test script...")

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    print("Reading GOOGLE_APPLICATION_CREDENTIALS environment variable...")

    if not credentials_path:
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set"
        )

    credentials_file = Path(credentials_path)
    print(f"GOOGLE_APPLICATION_CREDENTIALS found: {credentials_file}")

    if not credentials_file.is_file():
        raise FileNotFoundError(
            f"Credential file does not exist: {credentials_file}"
        )
    print("Credential file exists. Creating BigQuery client...")

    client = bigquery.Client.from_service_account_json(str(credentials_file))
    print("BigQuery client created successfully.")

    query = "SELECT 1 + 1 AS result"
    print(f"Executing test query: {query}")
    query_job = client.query(query)
    result = query_job.result()
    for row in result:
        print(f"Query result: {row['result']}")

    print("BigQuery connection test completed.")


if __name__ == "__main__":
    main()
