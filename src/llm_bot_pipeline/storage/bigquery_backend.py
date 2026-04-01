"""
BigQuery storage backend implementation.

Ported from cloudflare-logpush (battle-tested production code).
Adapted to the unified StorageBackend interface with capability flags,
dual-dataset architecture, and generic configuration.
"""

import json
import logging
import os
import re
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from ..config.constants import BATCH_THRESHOLD
from ..monitoring.retry_handler import RetryConfig, RetryManager
from .base import (
    BackendCapabilities,
    QueryError,
    SchemaError,
    StorageBackend,
    StorageConnectionError,
    StorageError,
    validate_date_column,
    validate_table_name,
)
from .bigquery_schemas import TABLE_CONFIG, _get_table_schemas
from .bigquery_views import create_views as _create_bq_views

logger = logging.getLogger(__name__)


def _convert_sqlite_params_to_bigquery(sql: str) -> str:
    """Convert SQLite-style :param placeholders to BigQuery @param."""
    return re.sub(r":(\w+)", r"@\1", sql)


def _get_bq_type(value: Any, param_name: str = "") -> str:
    """Get BigQuery type string for a Python value.

    Uses param_name to infer types for None values (e.g. similarity fields).
    """
    if value is None:
        float_fields = {
            "mean_cosine_similarity",
            "min_cosine_similarity",
            "max_cosine_similarity",
            "pre_refinement_mibcs",
            "mean_mibcs_improvement",
            "window_ms",
        }
        if param_name in float_fields:
            return "FLOAT64"
        return "STRING"
    if isinstance(value, bool):
        return "BOOL"
    elif isinstance(value, int):
        return "INT64"
    elif isinstance(value, float):
        return "FLOAT64"
    elif isinstance(value, datetime):
        return "TIMESTAMP"
    elif isinstance(value, date):
        return "DATE"
    return "STRING"


# =============================================================================
# BigQuery Backend
# =============================================================================


class BigQueryBackend(StorageBackend):
    """BigQuery storage backend ported from cloudflare-logpush.

    Supports dual-dataset architecture (raw + report), explicit credentials
    or ADC, and automatic schema migration on initialize().
    """

    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[str] = None,
        dataset_raw: str = "bot_logs_raw",
        dataset_report: str = "bot_logs",
        location: str = "EU",
    ):
        self.project_id = project_id
        self.dataset_raw = dataset_raw
        self.dataset_report = dataset_report
        self.location = location
        self._credentials_path = Path(credentials_path) if credentials_path else None
        self._client = None
        self._table_schemas: Optional[dict] = None

    @property
    def backend_type(self) -> str:
        return "bigquery"

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_sql=True,
            supports_streaming=True,
            supports_partitioning=True,
            supports_transactions=False,
            supports_upsert=True,
            parameter_style="at",
        )

    # -----------------------------------------------------------------
    # Client management
    # -----------------------------------------------------------------

    def _get_client(self):
        """Get or create BigQuery client (ADC or explicit credentials)."""
        if self._client is None:
            try:
                from google.cloud import bigquery

                if self._credentials_path:
                    self._client = bigquery.Client.from_service_account_json(
                        str(self._credentials_path), project=self.project_id
                    )
                else:
                    self._client = bigquery.Client(project=self.project_id)
                logger.debug(f"Connected to BigQuery project: {self.project_id}")
            except Exception as e:
                raise StorageConnectionError(
                    f"Failed to connect to BigQuery: {e}"
                ) from e
        return self._client

    def get_full_table_id(self, table_name: str) -> str:
        """Get fully qualified BigQuery table ID."""
        return self._get_full_table_id(table_name)

    def _get_full_table_id(self, table_name: str) -> str:
        """Resolve simple table name to fully-qualified BigQuery table ID.

        Handles TABLE_CONFIG lookup, dotted names, and fallback to report dataset.
        """
        if table_name in TABLE_CONFIG:
            cfg = TABLE_CONFIG[table_name]
            ds = (
                self.dataset_raw if cfg["dataset_key"] == "raw" else self.dataset_report
            )
            return f"{self.project_id}.{ds}.{table_name}"

        if "." in table_name:
            parts = table_name.split(".")
            if len(parts) == 2:
                return f"{self.project_id}.{table_name}"
            return table_name

        return f"{self.project_id}.{self.dataset_report}.{table_name}"

    def _get_table_schemas(self) -> dict[str, list]:
        if self._table_schemas is None:
            self._table_schemas = _get_table_schemas()
        return self._table_schemas

    # -----------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------

    def initialize(self) -> None:
        """Create datasets, tables, and run schema migrations (idempotent)."""
        from google.api_core import exceptions as gexc
        from google.cloud import bigquery

        logger.info(f"Initializing BigQuery for project: {self.project_id}")
        client = self._get_client()

        for dataset_id in [self.dataset_raw, self.dataset_report]:
            ds_ref = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
            ds_ref.location = self.location
            try:
                client.create_dataset(ds_ref, exists_ok=True)
                logger.debug(f"Created/verified dataset: {dataset_id}")
            except gexc.Forbidden as e:
                raise SchemaError(f"Permission denied creating dataset: {e}") from e

        schemas = self._get_table_schemas()
        for table_name, schema_fields in schemas.items():
            cfg = TABLE_CONFIG.get(table_name, {})
            full_id = self._get_full_table_id(table_name)
            table_ref = bigquery.Table(full_id, schema=schema_fields)

            pf = cfg.get("partition_field")
            if pf:
                table_ref.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=pf,
                )

            cf = cfg.get("clustering_fields")
            if cf:
                table_ref.clustering_fields = cf

            try:
                client.create_table(table_ref, exists_ok=True)
                logger.debug(f"Created/verified table: {table_name}")
                self._migrate_table_schema(client, full_id, schema_fields)
            except Exception as e:
                raise SchemaError(f"Failed to create table {table_name}: {e}") from e

        dataset_ref = f"{self.project_id}.{self.dataset_report}"
        created = _create_bq_views(client, dataset_ref)
        logger.info("BigQuery initialization complete (%d views created)", len(created))

    def _migrate_table_schema(
        self,
        client,
        table_id: str,
        expected_schema: list,
    ) -> None:
        """Add new NULLABLE columns to an existing table.

        BigQuery allows adding NULLABLE columns without data loss.
        Non-NULLABLE columns are skipped with a warning.
        """
        from google.api_core.exceptions import NotFound

        try:
            table = client.get_table(table_id)
            existing_names = {f.name for f in table.schema}
            expected_names = {f.name for f in expected_schema}

            new_cols = expected_names - existing_names
            if not new_cols:
                return

            new_schema = list(table.schema)
            for field in expected_schema:
                if field.name in new_cols:
                    if field.mode != "NULLABLE":
                        logger.warning(
                            f"Cannot add non-NULLABLE column {field.name} "
                            f"to existing table {table_id}"
                        )
                        continue
                    new_schema.append(field)
                    logger.info(f"Migrating: Adding column {field.name} to {table_id}")

            if len(new_schema) > len(table.schema):
                table.schema = new_schema
                client.update_table(table, ["schema"])
                logger.info(f"Schema migration complete for {table_id}")

        except NotFound:
            pass
        except Exception as e:
            logger.warning(f"Schema migration failed for {table_id}: {e}")

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            logger.debug("BigQuery client closed")

    # -----------------------------------------------------------------
    # SQL interface
    # -----------------------------------------------------------------

    def query(self, sql: str, params: Optional[dict] = None) -> list[dict]:
        """Execute query (supports both :param and @param syntax)."""
        from google.cloud import bigquery

        client = self._get_client()
        sql = _convert_sqlite_params_to_bigquery(sql)

        try:
            if params:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            name, _get_bq_type(value, name), value
                        )
                        for name, value in params.items()
                    ]
                )
                job = client.query(sql, job_config=job_config)
            else:
                job = client.query(sql)

            result = job.result()
            return [dict(row) for row in result]

        except Exception as e:
            raise QueryError(f"Query execution failed: {e}") from e

    def execute(self, sql: str, params: Optional[dict] = None) -> int:
        """Execute DML/DDL (supports both :param and @param syntax)."""
        from google.cloud import bigquery

        client = self._get_client()
        sql = _convert_sqlite_params_to_bigquery(sql)

        try:
            if params:
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            name, _get_bq_type(value, name), value
                        )
                        for name, value in params.items()
                    ]
                )
                job = client.query(sql, job_config=job_config)
            else:
                job = client.query(sql)

            job.result()
            return job.num_dml_affected_rows or 0

        except Exception as e:
            raise QueryError(f"Statement execution failed: {e}") from e

    def table_exists(self, table_name: str) -> bool:
        from google.api_core.exceptions import NotFound

        try:
            self._get_client().get_table(self._get_full_table_id(table_name))
            return True
        except NotFound:
            return False

    def get_table_row_count(self, table_name: str) -> int:
        from google.api_core.exceptions import NotFound

        try:
            table = self._get_client().get_table(self._get_full_table_id(table_name))
            return table.num_rows or 0
        except NotFound as e:
            raise SchemaError(f"Table '{table_name}' does not exist") from e

    # -----------------------------------------------------------------
    # Record interface
    # -----------------------------------------------------------------

    def insert_raw_records(self, records: list[dict]) -> int:
        """Insert raw log records with adaptive strategy.

        Uses streaming insert for small batches (<1000) and temp-file load
        job for large batches (more robust, no timeout issues).
        """
        if not records:
            return 0

        client = self._get_client()
        table_id = self._get_full_table_id("raw_bot_requests")

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for record in records:
            if "_ingestion_time" not in record:
                record = {**record, "_ingestion_time": now}
            rows.append(record)

        try:
            if len(rows) < BATCH_THRESHOLD:
                errors = client.insert_rows_json(table_id, rows)
                if errors:
                    msgs = [str(e) for e in errors[:5]]
                    detail = "; ".join(msgs)
                    raise QueryError(f"BigQuery streaming insert failed: {detail}")
            else:
                logger.info(f"Using load job for {len(rows):,} records")
                self._load_from_temp_file(client, table_id, rows)

            return len(rows)

        except Exception as e:
            if isinstance(e, (QueryError, SchemaError)):
                raise
            raise QueryError(f"Failed to insert records: {e}") from e

    def _load_from_temp_file(self, client, table_id: str, records: list[dict]) -> None:
        """Load records via temp NDJSON file (robust for large batches)."""
        from google.cloud import bigquery

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
            temp_path = f.name

        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False,
            )
            with open(temp_path, "rb") as source_file:
                load_job = client.load_table_from_file(
                    source_file,
                    table_id,
                    job_config=job_config,
                )
            load_job.result()

            if load_job.errors:
                msgs = [str(e) for e in load_job.errors[:5]]
                detail = "; ".join(msgs)
                raise QueryError(f"BigQuery load job failed: {detail}")

            logger.info(f"Load job completed: {load_job.output_rows:,} rows loaded")
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def insert_records(self, table_name: str, records: list[dict]) -> int:
        """Bulk insert via load job for any table."""
        if not records:
            return 0
        from google.cloud import bigquery

        client = self._get_client()
        table_id = self._get_full_table_id(table_name)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
        )
        try:
            load_job = client.load_table_from_json(
                records, table_id, job_config=job_config
            )
            load_job.result()
            return load_job.output_rows or len(records)
        except Exception as e:
            raise StorageError(f"BigQuery load failed for {table_name}: {e}") from e

    def insert_clean_records(self, records: list[dict]) -> int:
        """Insert processed records into bot_requests_daily.

        Adds _processed_at if not present (avoids mutating input).
        Wraps streaming insert with RetryManager for transient error resilience.
        """
        if not records:
            return 0

        client = self._get_client()
        table_id = self._get_full_table_id("bot_requests_daily")

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for record in records:
            if "_processed_at" not in record:
                record = {**record, "_processed_at": now}
            rows.append(record)

        retry_mgr = RetryManager(
            config=RetryConfig(
                max_retries=3,
                base_delay_seconds=2.0,
                exponential_base=2.0,
            )
        )

        def _do_streaming_insert():
            return client.insert_rows_json(table_id, rows)

        result = retry_mgr.execute_with_retry(_do_streaming_insert)
        if not result.success:
            raise result.last_error

        bq_errors = result.result
        if bq_errors:
            msgs = [str(e) for e in bq_errors[:5]]
            detail = "; ".join(msgs)
            raise QueryError(f"BigQuery streaming insert failed: {detail}")

        return len(records)

    def insert_sitemap_urls(self, entries: list[dict]) -> int:
        """Insert sitemap URL entries, replacing all existing rows for each domain.

        Performs a domain-scoped DELETE before the bulk insert to prevent
        duplicate accumulation across ingestion runs (truncate-then-reload
        per domain). This ensures sitemap_urls has at most one row per
        (domain, url_path) after each run.
        """
        if not entries:
            return 0

        domains = list({e.get("domain") for e in entries if e.get("domain")})
        if domains:
            table_id = self._get_full_table_id("sitemap_urls")
            domain_list = ", ".join(f"'{d}'" for d in domains)
            delete_sql = f"DELETE FROM `{table_id}` WHERE domain IN ({domain_list})"
            self._client.query(delete_sql).result()

        now = datetime.now(timezone.utc).isoformat()
        rows = [
            {
                "url": e["url"],
                "url_path": e["url_path"],
                "domain": e.get("domain"),
                "lastmod": e.get("lastmod"),
                "lastmod_month": e.get("lastmod_month"),
                "sitemap_source": e["sitemap_source"],
                "_fetched_at": now,
            }
            for e in entries
        ]
        return self.insert_records("sitemap_urls", rows)

    # -----------------------------------------------------------------
    # Date range operations (with SQL injection protection)
    # -----------------------------------------------------------------

    def delete_date_range(
        self,
        table_name: str,
        date_column: str,
        start_date: date,
        end_date: date,
    ) -> int:
        validate_table_name(table_name)
        validate_date_column(date_column)
        table_id = self._get_full_table_id(table_name)
        sql = (
            f"DELETE FROM `{table_id}` "
            f"WHERE DATE({date_column}) >= @start_date "
            f"AND DATE({date_column}) <= @end_date"
        )
        return self.execute(sql, {"start_date": start_date, "end_date": end_date})

    def get_date_range_count(
        self,
        table_name: str,
        date_column: str,
        start_date: date,
        end_date: date,
    ) -> int:
        validate_table_name(table_name)
        validate_date_column(date_column)
        table_id = self._get_full_table_id(table_name)
        sql = (
            f"SELECT COUNT(*) as count FROM `{table_id}` "
            f"WHERE DATE({date_column}) >= @start_date "
            f"AND DATE({date_column}) <= @end_date"
        )
        result = self.query(sql, {"start_date": start_date, "end_date": end_date})
        return result[0]["count"] if result else 0

    def get_completed_dates_in_range(
        self,
        table_name: str,
        date_column: str,
        start_date: date,
        end_date: date,
    ) -> set[date]:
        """Return distinct dates that have data in the table for the given range."""
        validate_table_name(table_name)
        validate_date_column(date_column)
        table_id = self._get_full_table_id(table_name)
        sql = (
            f"SELECT DISTINCT DATE({date_column}) as d FROM `{table_id}` "
            f"WHERE DATE({date_column}) >= @start_date "
            f"AND DATE({date_column}) <= @end_date"
        )
        result = self.query(sql, {"start_date": start_date, "end_date": end_date})
        out: set[date] = set()
        for row in result or []:
            val = row.get("d")
            if val is not None:
                if isinstance(val, date):
                    out.add(val)
                else:
                    out.add(date.fromisoformat(str(val)[:10]))
        return out

    # -----------------------------------------------------------------
    # BigQuery-specific helpers
    # -----------------------------------------------------------------

    def run_query_to_table(
        self,
        sql: str,
        destination_table: str,
        write_disposition: str = "WRITE_APPEND",
    ) -> int:
        """Run a query and write results to a destination table (ETL helper)."""
        from google.cloud import bigquery

        client = self._get_client()
        dest_id = self._get_full_table_id(destination_table)
        job_config = bigquery.QueryJobConfig(
            destination=dest_id,
            write_disposition=write_disposition,
        )
        try:
            job = client.query(sql, job_config=job_config)
            result = job.result()
            return result.total_rows or 0
        except Exception as e:
            raise QueryError(f"Query to table failed: {e}") from e

    def get_table_info(self, table_name: str) -> dict:
        """Get detailed table metadata."""
        from google.api_core.exceptions import NotFound

        table_id = self._get_full_table_id(table_name)
        try:
            table = self._get_client().get_table(table_id)
            return {
                "full_id": table_id,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "partitioning": (
                    table.time_partitioning.field if table.time_partitioning else None
                ),
                "clustering": table.clustering_fields,
            }
        except NotFound:
            return {"error": f"Table not found: {table_name}"}

    def migrate_schema(self, table_name: str, new_fields: list) -> None:
        """Public API for schema migration on a single table."""
        self._migrate_table_schema(
            self._get_client(),
            self._get_full_table_id(table_name),
            new_fields,
        )

    def health_check(self) -> dict:
        try:
            self.query("SELECT 1 as test")
            client = self._get_client()
            datasets = list(client.list_datasets(max_results=10))
            return {
                "healthy": True,
                "backend_type": self.backend_type,
                "message": "BigQuery connection is operational",
                "details": {
                    "project_id": self.project_id,
                    "location": self.location,
                    "dataset_count": len(datasets),
                    "datasets": [ds.dataset_id for ds in datasets],
                },
            }
        except Exception as e:
            return {
                "healthy": False,
                "backend_type": self.backend_type,
                "message": f"BigQuery health check failed: {str(e)}",
                "details": {"error": str(e)},
            }
