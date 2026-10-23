"""
Aggregation queries for reporting tables (BigQuery implementation).

Generates pre-aggregated summaries from clean bot traffic data:
- daily_summary: Aggregations by date and bot characteristics
- url_performance: Aggregations by URL for content analysis

Note: Uses BigQuery-specific SQL. For backend-agnostic implementation,
see LocalReportingAggregator in local_aggregations.py.
"""

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from google.cloud import bigquery

from ..config.constants import (
    DATASET_REPORT,
    TABLE_CLEAN_BOT_REQUESTS,
    TABLE_DAILY_SUMMARY,
    TABLE_URL_PERFORMANCE,
)
from ..storage import get_backend
from .freshness_tracker import DataFreshnessTracker
from .models import AggregationResult

logger = logging.getLogger(__name__)


class ReportingAggregator:
    """
    Creates pre-aggregated reporting tables from clean bot traffic data.

    Generates:
    - daily_summary: Bot traffic aggregated by date and bot characteristics
    - url_performance: Bot traffic aggregated by URL path
    """

    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[Path] = None,
    ):
        """
        Initialize the aggregator.

        Args:
            project_id: GCP project ID
            credentials_path: Path to service account JSON key (uses ADC if None)
        """
        self.project_id = project_id

        if credentials_path:
            self.client = bigquery.Client.from_service_account_json(
                str(credentials_path),
                project=project_id,
            )
        else:
            self.client = bigquery.Client(project=project_id)

        # Table references
        self.source_table_id = (
            f"{project_id}.{DATASET_REPORT}.{TABLE_CLEAN_BOT_REQUESTS}"
        )
        self.daily_summary_table_id = (
            f"{project_id}.{DATASET_REPORT}.{TABLE_DAILY_SUMMARY}"
        )
        self.url_performance_table_id = (
            f"{project_id}.{DATASET_REPORT}.{TABLE_URL_PERFORMANCE}"
        )

        # Freshness tracker (lazy initialized)
        self._freshness_tracker: Optional[DataFreshnessTracker] = None
        self._credentials_path = credentials_path

        logger.info(f"ReportingAggregator initialized for project: {project_id}")

    @property
    def freshness_tracker(self) -> DataFreshnessTracker:
        """Get or create freshness tracker (lazy initialization)."""
        if self._freshness_tracker is None:
            backend = get_backend(
                "bigquery",
                project_id=self.project_id,
                credentials_path=self._credentials_path,
            )
            self._freshness_tracker = DataFreshnessTracker(backend=backend)
            self._freshness_tracker.initialize()
        return self._freshness_tracker

    def log_freshness(self, result: AggregationResult) -> bool:
        """
        Log freshness data for a completed aggregation.

        Args:
            result: AggregationResult from an aggregation run

        Returns:
            True if freshness was logged successfully
        """
        if not result.success or not result.end_date:
            return False

        return self.freshness_tracker.update_freshness(
            table_name=result.table_name,
            last_processed_date=result.end_date,
            rows_processed=result.rows_inserted,
        )

    def close(self) -> None:
        """Close any open resources."""
        if self._freshness_tracker:
            self._freshness_tracker.close()
            self._freshness_tracker = None

    def __enter__(self) -> "ReportingAggregator":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def aggregate_daily_summary(
        self,
        start_date: date,
        end_date: date,
        write_disposition: str = "WRITE_APPEND",
        delete_existing: bool = True,
    ) -> AggregationResult:
        """
        Create daily summary aggregations.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            write_disposition: BigQuery write disposition
            delete_existing: Clear the date range before appending so re-runs
                are idempotent (default True).

        Returns:
            AggregationResult with success status and row count

        Raises:
            ValueError: If start_date > end_date
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        started_at = datetime.now(timezone.utc)
        logger.info(f"Aggregating daily_summary: {start_date} to {end_date}")

        if delete_existing:
            deleted = self.delete_date_range("daily_summary", start_date, end_date)
            logger.info(f"daily_summary: cleared {deleted:,} existing rows for range")

        query = self._build_daily_summary_query(start_date, end_date)

        try:
            job_config = bigquery.QueryJobConfig(
                destination=self.daily_summary_table_id,
                write_disposition=write_disposition,
            )

            query_job = self.client.query(query, job_config=job_config)
            result = query_job.result()

            rows_inserted = result.total_rows or 0
            duration = (datetime.now(timezone.utc) - started_at).total_seconds()

            logger.info(
                f"daily_summary: inserted {rows_inserted:,} rows in {duration:.1f}s"
            )

            return AggregationResult(
                success=True,
                table_name="daily_summary",
                rows_inserted=rows_inserted,
                start_date=start_date,
                end_date=end_date,
                duration_seconds=duration,
            )

        except Exception as e:
            logger.exception(f"Failed to aggregate daily_summary: {e}")
            return AggregationResult(
                success=False,
                table_name="daily_summary",
                start_date=start_date,
                end_date=end_date,
                error=str(e),
                duration_seconds=(
                    datetime.now(timezone.utc) - started_at
                ).total_seconds(),
            )

    def aggregate_url_performance(
        self,
        start_date: date,
        end_date: date,
        write_disposition: str = "WRITE_APPEND",
        delete_existing: bool = True,
    ) -> AggregationResult:
        """
        Create URL performance aggregations.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            write_disposition: BigQuery write disposition
            delete_existing: Clear the date range before appending so re-runs
                are idempotent (default True).

        Returns:
            AggregationResult with success status and row count

        Raises:
            ValueError: If start_date > end_date
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        started_at = datetime.now(timezone.utc)
        logger.info(f"Aggregating url_performance: {start_date} to {end_date}")

        if delete_existing:
            deleted = self.delete_date_range("url_performance", start_date, end_date)
            logger.info(f"url_performance: cleared {deleted:,} existing rows for range")

        query = self._build_url_performance_query(start_date, end_date)

        try:
            job_config = bigquery.QueryJobConfig(
                destination=self.url_performance_table_id,
                write_disposition=write_disposition,
            )

            query_job = self.client.query(query, job_config=job_config)
            result = query_job.result()

            rows_inserted = result.total_rows or 0
            duration = (datetime.now(timezone.utc) - started_at).total_seconds()

            logger.info(
                f"url_performance: inserted {rows_inserted:,} rows in {duration:.1f}s"
            )

            return AggregationResult(
                success=True,
                table_name="url_performance",
                rows_inserted=rows_inserted,
                start_date=start_date,
                end_date=end_date,
                duration_seconds=duration,
            )

        except Exception as e:
            logger.exception(f"Failed to aggregate url_performance: {e}")
            return AggregationResult(
                success=False,
                table_name="url_performance",
                start_date=start_date,
                end_date=end_date,
                error=str(e),
                duration_seconds=(
                    datetime.now(timezone.utc) - started_at
                ).total_seconds(),
            )

    def aggregate_all(
        self,
        start_date: date,
        end_date: date,
        write_disposition: str = "WRITE_APPEND",
        track_freshness: bool = True,
    ) -> list[AggregationResult]:
        """
        Run all aggregations for a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            write_disposition: BigQuery write disposition
            track_freshness: If True, update data_freshness table after each aggregation

        Returns:
            List of AggregationResult for each table

        Raises:
            ValueError: If start_date > end_date
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        logger.info(f"Running all aggregations: {start_date} to {end_date}")

        results = [
            self.aggregate_daily_summary(start_date, end_date, write_disposition),
            self.aggregate_url_performance(start_date, end_date, write_disposition),
        ]

        # Log freshness for successful aggregations
        if track_freshness:
            for result in results:
                if result.success:
                    self.log_freshness(result)

        successful = sum(1 for r in results if r.success)
        logger.info(f"Aggregations complete: {successful}/{len(results)} successful")

        return results

    def delete_date_range(
        self,
        table_name: str,
        start_date: date,
        end_date: date,
    ) -> int:
        """
        Delete existing data for a date range from a reporting table.

        Args:
            table_name: 'daily_summary' or 'url_performance'
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            Number of rows deleted

        Raises:
            ValueError: If start_date > end_date or invalid table_name
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        table_map = {
            "daily_summary": self.daily_summary_table_id,
            "url_performance": self.url_performance_table_id,
        }

        if table_name not in table_map:
            raise ValueError(
                f"Invalid table_name: {table_name}. Use 'daily_summary' or 'url_performance'"
            )

        table_id = table_map[table_name]

        query = f"""
        DELETE FROM `{table_id}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
        """

        query_job = self.client.query(query)
        query_job.result()

        deleted = query_job.num_dml_affected_rows or 0
        logger.info(f"Deleted {deleted:,} rows from {table_name}")

        return deleted

    def _build_daily_summary_query(self, start_date: date, end_date: date) -> str:
        """Build aggregation query for daily_summary table."""
        return f"""
        SELECT
            request_date,
            domain,
            bot_provider,
            bot_name,
            bot_category,
            -- Metrics
            COUNT(*) AS total_requests,
            COUNT(DISTINCT request_uri) AS unique_urls,
            COUNT(DISTINCT request_host) AS unique_hosts,
            COUNTIF(response_status_category = '2xx_success') AS successful_requests,
            COUNTIF(response_status_category IN ('4xx_client_error', '5xx_server_error')) AS error_requests,
            COUNTIF(response_status_category = '3xx_redirect') AS redirect_requests,
            -- Metadata
            CURRENT_TIMESTAMP() AS _aggregated_at
        FROM `{self.source_table_id}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
        GROUP BY
            request_date,
            domain,
            bot_provider,
            bot_name,
            bot_category
        """

    def _build_url_performance_query(self, start_date: date, end_date: date) -> str:
        """Build aggregation query for url_performance table."""
        return f"""
        SELECT
            request_date,
            domain,
            request_host,
            COALESCE(url_path, '/') AS url_path,
            -- Bot metrics
            COUNT(*) AS total_bot_requests,
            COUNT(DISTINCT bot_provider) AS unique_bot_providers,
            COUNT(DISTINCT bot_name) AS unique_bot_names,
            -- Category breakdown
            COUNTIF(bot_category = 'training') AS training_hits,
            COUNTIF(bot_category = 'user_request') AS user_request_hits,
            -- Response breakdown
            COUNTIF(response_status_category = '2xx_success') AS successful_requests,
            COUNTIF(response_status_category IN ('4xx_client_error', '5xx_server_error')) AS error_requests,
            -- Time tracking
            MIN(request_timestamp) AS first_seen,
            MAX(request_timestamp) AS last_seen,
            -- Metadata
            CURRENT_TIMESTAMP() AS _aggregated_at
        FROM `{self.source_table_id}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
        GROUP BY
            request_date,
            domain,
            request_host,
            COALESCE(url_path, '/')
        """

    def get_freshness_stats(self) -> dict:
        """
        Get data freshness statistics for reporting tables.

        Returns:
            Dictionary with freshness info per table
        """
        stats = {}

        for name, table_id in [
            ("daily_summary", self.daily_summary_table_id),
            ("url_performance", self.url_performance_table_id),
        ]:
            try:
                query = f"""
                SELECT
                    MAX(request_date) AS latest_date,
                    MAX(_aggregated_at) AS last_aggregated,
                    COUNT(*) AS total_rows
                FROM `{table_id}`
                """
                result = self.client.query(query).result()
                row = next(iter(result), None)

                stats[name] = {
                    "latest_date": row.latest_date if row else None,
                    "last_aggregated": row.last_aggregated if row else None,
                    "total_rows": row.total_rows if row else 0,
                }
            except Exception as e:
                stats[name] = {"error": str(e)}

        return stats
