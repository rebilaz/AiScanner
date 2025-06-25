import os
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
    print(f"GOOGLE_APPLICATION_CREDENTIALS found: {credentials_path}")

    if not os.path.isfile(credentials_path):
        raise FileNotFoundError(
            f"Credential file does not exist: {credentials_path}"
        )
    print("Credential file exists. Creating BigQuery client...")

    client = bigquery.Client.from_service_account_json(credentials_path)
    print("BigQuery client created successfully.")

    query = "SELECT 1 + 1 AS result"
    print(f"Executing test query: {query}")
    query_job = client.query(query)
    result = query_job.result()
    for row in result:
        print(f"Query result: {row['result']}")

    print("BigQuery connection test completed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Log the error and re-raise so that the calling environment can detect failure.
        print(f"An error occurred: {exc}")
        raise
