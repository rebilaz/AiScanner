import logging
from typing import List

import pandas as pd
from google.cloud import bigquery
from google.cloud import exceptions


class BigQueryClient:
    """Utility wrapper around :class:`google.cloud.bigquery.Client`."""

    def __init__(self, project_id: str) -> None:
        """Initialize the BigQuery client.

        Parameters
        ----------
        project_id : str
            Google Cloud project identifier.
        """
        self.client = bigquery.Client(project=project_id)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _generate_schema(self, df: pd.DataFrame) -> List[bigquery.SchemaField]:
        """Generate BigQuery schema from DataFrame dtypes."""
        schema: List[bigquery.SchemaField] = []
        for column, dtype in df.dtypes.items():
            field_type = "STRING"
            if pd.api.types.is_integer_dtype(dtype):
                field_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                field_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                field_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                field_type = "TIMESTAMP"
            schema.append(bigquery.SchemaField(str(column), field_type))
        return schema

    def ensure_dataset_exists(self, dataset_id: str, location: str = "EU") -> None:
        """Ensure that a BigQuery dataset exists.

        Parameters
        ----------
        dataset_id : str
            The dataset identifier.
        location : str, optional
            Dataset location, by default "EU".
        """
        dataset_ref = bigquery.Dataset(f"{self.client.project}.{dataset_id}")
        try:
            self.client.get_dataset(dataset_ref.reference)
            self.logger.info("Dataset %s already exists", dataset_id)
        except exceptions.NotFound:
            dataset_ref.location = location
            self.client.create_dataset(dataset_ref)
            self.logger.info("Created dataset %s in %s", dataset_id, location)

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
    ) -> None:
        """Upload a DataFrame to a BigQuery table.

        Parameters
        ----------
        df : pandas.DataFrame
            Data to upload.
        dataset_id : str
            Target dataset identifier.
        table_id : str
            Target table identifier.
        write_disposition : str, optional
            BigQuery write disposition, by default "WRITE_APPEND".
        """
        table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            schema=self._generate_schema(df),
        )
        try:
            self.logger.info("Uploading %d rows to %s", len(df), table_ref)
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()
            self.logger.info("Upload to %s completed", table_ref)
        except exceptions.GoogleCloudError as exc:
            self.logger.error("Failed to upload dataframe to %s: %s", table_ref, exc)
            raise
