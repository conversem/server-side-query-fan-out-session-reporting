"""
Local aggregation queries for reporting tables using storage abstraction.

Provides backend-agnostic aggregations for both SQLite (POC) and BigQuery.
Uses SQLBuilder for backend-specific SQL generation.
Generates same table structures for consistency across backends.
"""

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from ..pipeline.sql_compat import SQLBuilder
from ..storage import StorageBackend, get_backend
from .freshness_tracker import DataFreshnessTracker
from .models import AggregationResult

logger = logging.getLogger(__name__)


class LocalReportingAggregator:
    """
    Creates pre-aggregated reporting tables using storage abstraction.

    Works with both SQLite (POC) and BigQuery backends through StorageBackend.

    Generates:
    - daily_summary: Bot traffic aggregated by date and bot characteristics
    - url_performance: Bot traffic aggregated by URL path
    """

    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        backend_type: str = "sqlite",
        db_path: Optional[Path] = None,
    ):
        """
        Initialize the local aggregator.

        Args:
            backend: Pre-initialized StorageBackend (optional)
            backend_type: Backend type if creating new ('sqlite' or 'bigquery')
            db_path: Path to SQLite database (for sqlite backend)
        """
        if backend:
            self._backend = backend
            self._owns_backend = False
        else:
            kwargs = {}
            if backend_type == "sqlite" and db_path:
                kwargs["db_path"] = db_path
            self._backend = get_backend(backend_type, **kwargs)
            self._owns_backend = True

        self._backend_type = self._backend.backend_type
        self._sql = SQLBuilder(self._backend_type)
        self._initialized = False
        self._freshness_tracker: Optional[DataFreshnessTracker] = None

        logger.info(
            f"LocalReportingAggregator initialized with {self._backend_type} backend"
        )

    def initialize(self) -> None:
        """Initialize the backend (create tables if needed)."""
        if not self._initialized:
            self._backend.initialize()
            self._initialized = True

    def close(self) -> None:
        """Close the backend connection."""
        if self._freshness_tracker:
            self._freshness_tracker.close()
            self._freshness_tracker = None
        if self._owns_backend:
            self._backend.close()

    @property
    def freshness_tracker(self) -> DataFreshnessTracker:
        """Get or create freshness tracker (lazy initialization)."""
        if self._freshness_tracker is None:
            self._freshness_tracker = DataFreshnessTracker(backend=self._backend)
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

    def __enter__(self) -> "LocalReportingAggregator":
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def aggregate_daily_summary(
        self,
        start_date: date,
        end_date: date,
    ) -> AggregationResult:
        """
        Create daily summary aggregations.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            AggregationResult with success status and row count
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        self.initialize()
        started_at = datetime.now(timezone.utc)
        logger.info(f"Aggregating daily_summary: {start_date} to {end_date}")

        try:
            # Build aggregation query
            query, params = self._build_daily_summary_query(start_date, end_date)

            # Execute and get results
            results = self._backend.query(query, params)

            if not results:
                duration = (datetime.now(timezone.utc) - started_at).total_seconds()
                return AggregationResult(
                    success=True,
                    table_name="daily_summary",
                    rows_inserted=0,
                    start_date=start_date,
                    end_date=end_date,
                    duration_seconds=duration,
                )

            # Insert aggregated rows using batch insert
            rows_inserted = self._batch_insert_daily_summary(results)

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
            duration = (datetime.now(timezone.utc) - started_at).total_seconds()
            return AggregationResult(
                success=False,
                table_name="daily_summary",
                start_date=start_date,
                end_date=end_date,
                error=str(e),
                duration_seconds=duration,
            )

    def aggregate_url_performance(
        self,
        start_date: date,
        end_date: date,
    ) -> AggregationResult:
        """
        Create URL performance aggregations.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            AggregationResult with success status and row count
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        self.initialize()
        started_at = datetime.now(timezone.utc)
        logger.info(f"Aggregating url_performance: {start_date} to {end_date}")

        try:
            # Build aggregation query
            query, params = self._build_url_performance_query(start_date, end_date)

            # Execute and get results
            results = self._backend.query(query, params)

            if not results:
                duration = (datetime.now(timezone.utc) - started_at).total_seconds()
                return AggregationResult(
                    success=True,
                    table_name="url_performance",
                    rows_inserted=0,
                    start_date=start_date,
                    end_date=end_date,
                    duration_seconds=duration,
                )

            # Insert aggregated rows using batch insert
            rows_inserted = self._batch_insert_url_performance(results)

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
            duration = (datetime.now(timezone.utc) - started_at).total_seconds()
            return AggregationResult(
                success=False,
                table_name="url_performance",
                start_date=start_date,
                end_date=end_date,
                error=str(e),
                duration_seconds=duration,
            )

    def aggregate_all(
        self,
        start_date: date,
        end_date: date,
        track_freshness: bool = True,
    ) -> list[AggregationResult]:
        """
        Run all aggregations for a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            track_freshness: If True, update data_freshness table after each aggregation

        Returns:
            List of AggregationResult for each table
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        logger.info(f"Running all aggregations: {start_date} to {end_date}")

        results = [
            self.aggregate_daily_summary(start_date, end_date),
            self.aggregate_url_performance(start_date, end_date),
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
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        if table_name not in ("daily_summary", "url_performance"):
            raise ValueError(
                f"Invalid table_name: {table_name}. "
                "Use 'daily_summary' or 'url_performance'"
            )

        self.initialize()

        if not self._backend.table_exists(table_name):
            return 0

        sql = f"""
            DELETE FROM {table_name}
            WHERE request_date >= :start_date
              AND request_date <= :end_date
        """
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        try:
            return self._backend.execute(sql, params)
        except Exception as e:
            logger.warning(f"Failed to delete from {table_name}: {e}")
            return 0

    def _build_daily_summary_query(
        self, start_date: date, end_date: date
    ) -> tuple[str, dict]:
        """Build aggregation query for daily_summary table."""
        # Use SQLBuilder for backend-specific SQL
        countif_success = self._sql.countif("response_status_category = '2xx_success'")
        countif_error = self._sql.countif(
            "response_status_category IN ('4xx_client_error', '5xx_server_error')"
        )
        countif_redirect = self._sql.countif(
            "response_status_category = '3xx_redirect'"
        )
        current_ts = self._sql.current_timestamp()

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        sql = f"""
            SELECT
                request_date,
                domain,
                bot_provider,
                bot_name,
                bot_category,
                COUNT(*) AS total_requests,
                COUNT(DISTINCT request_uri) AS unique_urls,
                COUNT(DISTINCT request_host) AS unique_hosts,
                AVG(bot_score) AS avg_bot_score,
                {countif_success} AS successful_requests,
                {countif_error} AS error_requests,
                {countif_redirect} AS redirect_requests,
                {current_ts} AS _aggregated_at
            FROM bot_requests_daily
            WHERE request_date >= :start_date
              AND request_date <= :end_date
            GROUP BY
                request_date,
                domain,
                bot_provider,
                bot_name,
                bot_category
        """
        return sql, params

    def _build_url_performance_query(
        self, start_date: date, end_date: date
    ) -> tuple[str, dict]:
        """Build aggregation query for url_performance table."""
        countif_training = self._sql.countif("bot_category = 'training'")
        countif_user_request = self._sql.countif("bot_category = 'user_request'")
        countif_success = self._sql.countif("response_status_category = '2xx_success'")
        countif_error = self._sql.countif(
            "response_status_category IN ('4xx_client_error', '5xx_server_error')"
        )
        current_ts = self._sql.current_timestamp()

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        sql = f"""
            SELECT
                request_date,
                domain,
                request_host,
                COALESCE(url_path, '/') AS url_path,
                COUNT(*) AS total_bot_requests,
                COUNT(DISTINCT bot_provider) AS unique_bot_providers,
                COUNT(DISTINCT bot_name) AS unique_bot_names,
                {countif_training} AS training_hits,
                {countif_user_request} AS user_request_hits,
                {countif_success} AS successful_requests,
                {countif_error} AS error_requests,
                MIN(request_timestamp) AS first_seen,
                MAX(request_timestamp) AS last_seen,
                {current_ts} AS _aggregated_at
            FROM bot_requests_daily
            WHERE request_date >= :start_date
              AND request_date <= :end_date
            GROUP BY
                request_date,
                domain,
                request_host,
                COALESCE(url_path, '/')
        """
        return sql, params

    def _batch_insert_daily_summary(self, results: list[dict]) -> int:
        """Batch insert daily_summary rows for better performance."""
        columns = [
            "request_date",
            "domain",
            "bot_provider",
            "bot_name",
            "bot_category",
            "total_requests",
            "unique_urls",
            "unique_hosts",
            "avg_bot_score",
            "successful_requests",
            "error_requests",
            "redirect_requests",
            "_aggregated_at",
        ]
        return self._batch_insert("daily_summary", columns, results)

    def _batch_insert_url_performance(self, results: list[dict]) -> int:
        """Batch insert url_performance rows for better performance."""
        columns = [
            "request_date",
            "domain",
            "request_host",
            "url_path",
            "total_bot_requests",
            "unique_bot_providers",
            "unique_bot_names",
            "training_hits",
            "user_request_hits",
            "successful_requests",
            "error_requests",
            "first_seen",
            "last_seen",
            "_aggregated_at",
        ]
        return self._batch_insert("url_performance", columns, results)

    def _batch_insert(self, table: str, columns: list[str], results: list[dict]) -> int:
        """Batch insert rows using the StorageBackend abstraction."""
        if not results:
            return 0

        cleaned = []
        for row in results:
            record = {}
            for col in columns:
                val = row.get(col)
                if isinstance(val, bool):
                    val = 1 if val else 0
                record[col] = val
            cleaned.append(record)

        return self._backend.insert_records(table, cleaned)

    def get_freshness_stats(self) -> dict:
        """
        Get data freshness statistics for reporting tables.

        Returns:
            Dictionary with freshness info per table
        """
        self.initialize()
        stats = {}

        for table_name in ["daily_summary", "url_performance"]:
            if not self._backend.table_exists(table_name):
                stats[table_name] = {"error": "Table does not exist"}
                continue

            try:
                query = f"""
                    SELECT
                        MAX(request_date) AS latest_date,
                        MAX(_aggregated_at) AS last_aggregated,
                        COUNT(*) AS total_rows
                    FROM {table_name}
                """
                result = self._backend.query(query)
                if result:
                    row = result[0]
                    stats[table_name] = {
                        "latest_date": row.get("latest_date"),
                        "last_aggregated": row.get("last_aggregated"),
                        "total_rows": row.get("total_rows", 0),
                    }
                else:
                    stats[table_name] = {
                        "latest_date": None,
                        "last_aggregated": None,
                        "total_rows": 0,
                    }
            except Exception as e:
                stats[table_name] = {"error": str(e)}

        return stats
