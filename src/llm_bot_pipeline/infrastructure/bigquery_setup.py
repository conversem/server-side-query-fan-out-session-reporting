"""
BigQuery dataset and table setup.
"""

import logging
from pathlib import Path
from typing import Optional

from google.api_core import exceptions
from google.cloud import bigquery

from ..config.constants import DATASET_RAW, DATASET_REPORT, DATASET_STAGING

logger = logging.getLogger(__name__)


class BigQuerySetup:
    """Manages BigQuery dataset and table creation."""

    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[Path] = None,
        location: str = "US",
    ):
        """
        Initialize BigQuery setup.

        Args:
            project_id: GCP project ID
            credentials_path: Path to service account JSON key file
            location: BigQuery dataset location (default: US)
        """
        self.project_id = project_id
        self.location = location

        if credentials_path:
            self.client = bigquery.Client.from_service_account_json(
                str(credentials_path), project=project_id
            )
        else:
            # Use default credentials (ADC)
            self.client = bigquery.Client(project=project_id)

    def create_dataset(self, dataset_id: str, description: str) -> bigquery.Dataset:
        """
        Create a BigQuery dataset if it doesn't exist.

        Args:
            dataset_id: Dataset ID (without project prefix)
            description: Dataset description

        Returns:
            Created or existing dataset
        """
        dataset_ref = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
        dataset_ref.location = self.location
        dataset_ref.description = description

        try:
            dataset = self.client.create_dataset(dataset_ref, exists_ok=True)
            logger.info("✓ Created/verified BigQuery dataset: %s", dataset_id)
            return dataset
        except exceptions.Forbidden as e:
            logger.warning("✗ Permission denied creating dataset %s: %s", dataset_id, e)
            raise
        except exceptions.GoogleAPICallError as e:
            logger.warning("✗ Failed to create dataset %s: %s", dataset_id, e)
            raise
        except Exception as e:
            logger.error("Unexpected error creating dataset %s: %s", dataset_id, e)
            raise

    def create_all_datasets(self) -> dict[str, bigquery.Dataset]:
        """
        Create all required POC datasets.

        Returns:
            Dictionary of dataset_id -> Dataset objects
        """
        datasets_config = [
            (DATASET_RAW, "POC: Raw LLM bot traffic from Cloudflare Logpush"),
            (DATASET_STAGING, "POC: Intermediate processing data for LLM bot traffic"),
            (DATASET_REPORT, "POC: Cleaned LLM bot traffic ready for reporting"),
        ]

        created = {}
        for dataset_id, description in datasets_config:
            created[dataset_id] = self.create_dataset(dataset_id, description)

        return created

    def create_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: list[bigquery.SchemaField],
        partition_field: Optional[str] = None,
        clustering_fields: Optional[list[str]] = None,
    ) -> bigquery.Table:
        """
        Create a BigQuery table with optional partitioning and clustering.

        Args:
            dataset_id: Dataset ID
            table_id: Table ID
            schema: List of SchemaField objects
            partition_field: Field to partition by (must be DATE or TIMESTAMP)
            clustering_fields: Fields to cluster by (max 4)

        Returns:
            Created or existing table
        """
        table_ref = bigquery.Table(
            f"{self.project_id}.{dataset_id}.{table_id}",
            schema=schema,
        )

        if partition_field:
            table_ref.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )

        if clustering_fields:
            table_ref.clustering_fields = clustering_fields

        try:
            table = self.client.create_table(table_ref, exists_ok=True)
            logger.info(
                "✓ Created/verified BigQuery table: %s.%s", dataset_id, table_id
            )
            return table
        except exceptions.GoogleAPICallError as e:
            logger.warning(
                "✗ Failed to create table %s.%s: %s", dataset_id, table_id, e
            )
            raise
        except Exception as e:
            logger.error(
                "Unexpected error creating table %s.%s: %s", dataset_id, table_id, e
            )
            raise

    def verify_access(self) -> bool:
        """
        Verify BigQuery access by running a simple query.

        Returns:
            True if access is verified
        """
        try:
            query = "SELECT 1 as test"
            job = self.client.query(query)
            job.result()
            logger.info("✓ Verified BigQuery access")
            return True
        except exceptions.GoogleAPICallError as e:
            logger.warning("✗ BigQuery access verification failed: %s", e)
            return False
        except Exception as e:
            logger.error("Unexpected error verifying BigQuery access: %s", e)
            raise

    def list_datasets(self) -> list[str]:
        """List all datasets in the project."""
        return [ds.dataset_id for ds in self.client.list_datasets()]

    def create_all_tables(self) -> dict[str, bigquery.Table]:
        """
        Create all required tables for the pipeline.

        Returns:
            Dictionary of table_id -> Table objects
        """
        from ..storage.bigquery_schemas import TABLE_CONFIG, _get_table_schemas

        table_schemas = _get_table_schemas()
        dataset_map = {
            "raw": DATASET_RAW,
            "report": DATASET_REPORT,
        }

        tables = {}
        for table_name, schema_fields in table_schemas.items():
            config = TABLE_CONFIG.get(table_name, {})
            dataset_key = config.get("dataset_key", "report")
            dataset_id = dataset_map.get(dataset_key, DATASET_REPORT)

            tables[table_name] = self.create_table(
                dataset_id=dataset_id,
                table_id=table_name,
                schema=schema_fields,
                partition_field=config.get("partition_field"),
                clustering_fields=config.get("clustering_fields"),
            )

        return tables

    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """Check if a table exists."""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        try:
            self.client.get_table(table_ref)
            return True
        except exceptions.NotFound:
            return False

    def get_table_info(self, dataset_id: str, table_id: str) -> dict:
        """Get information about a table."""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        try:
            table = self.client.get_table(table_ref)
            return {
                "full_id": table_ref,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created,
                "modified": table.modified,
                "partitioning": (
                    str(table.time_partitioning) if table.time_partitioning else None
                ),
                "clustering": table.clustering_fields,
            }
        except exceptions.NotFound:
            return {"error": "Table not found"}

    def create_view(
        self,
        dataset_id: str,
        view_id: str,
        view_query: str,
        description: Optional[str] = None,
    ) -> bigquery.Table:
        """
        Create or replace a BigQuery view.

        Args:
            dataset_id: Dataset ID
            view_id: View ID
            view_query: SQL query defining the view
            description: Optional view description

        Returns:
            Created or updated view
        """
        view_ref = bigquery.Table(f"{self.project_id}.{dataset_id}.{view_id}")
        view_ref.view_query = view_query
        if description:
            view_ref.description = description

        try:
            # Delete existing view if present (views can't be updated in place)
            try:
                self.client.delete_table(view_ref, not_found_ok=True)
            except exceptions.GoogleAPICallError:
                logger.debug(
                    "Could not delete existing view %s", view_id, exc_info=True
                )

            view = self.client.create_table(view_ref)
            logger.info("✓ Created BigQuery view: %s.%s", dataset_id, view_id)
            return view
        except exceptions.GoogleAPICallError as e:
            logger.warning("✗ Failed to create view %s.%s: %s", dataset_id, view_id, e)
            raise
        except Exception as e:
            logger.error(
                "Unexpected error creating view %s.%s: %s", dataset_id, view_id, e
            )
            raise

    def view_exists(self, dataset_id: str, view_id: str) -> bool:
        """Check if a view exists."""
        view_ref = f"{self.project_id}.{dataset_id}.{view_id}"
        try:
            table = self.client.get_table(view_ref)
            return table.table_type == "VIEW"
        except exceptions.NotFound:
            return False

    def list_views(self, dataset_id: str) -> list[str]:
        """List all views in a dataset."""
        dataset_ref = f"{self.project_id}.{dataset_id}"
        tables = self.client.list_tables(dataset_ref)
        return [t.table_id for t in tables if t.table_type == "VIEW"]
