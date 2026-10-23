"""
Pipeline health monitoring queries for BigQuery dashboard.

Covers:
- Pipeline health status
- Stale tables
- Data quality summary
- Pipeline alerts
"""

from datetime import date, timedelta
from typing import Optional

from ...config.constants import DATASET_REPORT
from ..models import QueryResult


class HealthMixin:
    """Pipeline health monitoring query methods."""

    def get_pipeline_health(
        self,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get current pipeline health status for all tracked tables.

        Monitors ETL execution status using the data_freshness table.
        Returns freshness status and time since last update.

        Returns:
            QueryResult with health status for each table
        """
        data_freshness_table = f"{self.project_id}.{DATASET_REPORT}.data_freshness"

        query = f"""
        SELECT
            table_name,
            last_processed_date,
            last_updated_at,
            rows_processed,
            DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) AS hours_since_update,
            CASE
                WHEN DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) <= 24 THEN 'FRESH'
                WHEN DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) <= 48 THEN 'STALE'
                ELSE 'CRITICAL'
            END AS freshness_status
        FROM `{data_freshness_table}`
        WHERE 1=1
        {self._domain_filter(domain)}
        ORDER BY last_updated_at DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="pipeline_health",
            rows=rows,
            row_count=len(rows),
            description="Pipeline health status for all tracked tables",
        )

    def get_stale_tables(
        self,
        max_hours: int = 24,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get tables that haven't been updated within the specified time window.

        Useful for alerting on pipeline failures or delays.

        Args:
            max_hours: Maximum hours since last update (default: 24)

        Returns:
            QueryResult with stale tables
        """
        data_freshness_table = f"{self.project_id}.{DATASET_REPORT}.data_freshness"

        query = f"""
        SELECT
            table_name,
            last_processed_date,
            last_updated_at,
            rows_processed,
            DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) AS hours_since_update
        FROM `{data_freshness_table}`
        WHERE DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) > {max_hours}
        {self._domain_filter(domain)}
        ORDER BY hours_since_update DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="stale_tables",
            rows=rows,
            row_count=len(rows),
            description=f"Tables not updated in the last {max_hours} hours",
        )

    def get_data_quality_summary(
        self,
        check_date: Optional[date] = None,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get data quality summary for a specific date.

        Checks record counts, variance from average, and error rates.

        Args:
            check_date: Date to check (default: yesterday)

        Returns:
            QueryResult with data quality metrics
        """
        if check_date is None:
            check_date = date.today() - timedelta(days=1)

        query = f"""
        WITH daily_stats AS (
            SELECT
                request_date,
                SUM(total_requests) AS total_requests,
                SUM(successful_requests) AS successful_requests,
                SUM(error_requests) AS error_requests,
                COUNT(DISTINCT bot_provider) AS unique_providers
            FROM `{self.daily_summary_table}`
            WHERE request_date >= DATE_SUB('{check_date}', INTERVAL 14 DAY)
              AND request_date <= '{check_date}'
            {self._domain_filter(domain)}
            GROUP BY request_date
        ),
        historical AS (
            SELECT
                AVG(total_requests) AS avg_requests,
                STDDEV(total_requests) AS stddev_requests
            FROM daily_stats
            WHERE request_date < '{check_date}'
        ),
        current_day AS (
            SELECT *
            FROM daily_stats
            WHERE request_date = '{check_date}'
        )
        SELECT
            cd.request_date,
            cd.total_requests,
            cd.successful_requests,
            cd.error_requests,
            ROUND(100.0 * cd.successful_requests / NULLIF(cd.total_requests, 0), 2) AS success_rate_pct,
            ROUND(100.0 * cd.error_requests / NULLIF(cd.total_requests, 0), 2) AS error_rate_pct,
            ROUND(h.avg_requests, 0) AS historical_avg,
            ROUND(
                ABS(cd.total_requests - h.avg_requests) / NULLIF(h.avg_requests, 0) * 100,
                2
            ) AS variance_pct,
            CASE
                WHEN cd.total_requests IS NULL THEN 'NO_DATA'
                WHEN ABS(cd.total_requests - h.avg_requests) > 2 * h.stddev_requests THEN 'ANOMALY'
                WHEN ABS(cd.total_requests - h.avg_requests) > h.stddev_requests THEN 'WARNING'
                WHEN 100.0 * cd.error_requests / NULLIF(cd.total_requests, 0) > 10 THEN 'HIGH_ERRORS'
                ELSE 'OK'
            END AS quality_status
        FROM current_day cd
        CROSS JOIN historical h
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="data_quality_summary",
            rows=rows,
            row_count=len(rows),
            description=f"Data quality summary for {check_date}",
        )

    def get_pipeline_alerts(
        self,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get all current pipeline alerts that need attention.

        Combines stale tables, anomalies, and high error rates.

        Returns:
            QueryResult with active alerts
        """
        data_freshness_table = f"{self.project_id}.{DATASET_REPORT}.data_freshness"
        yesterday = (date.today() - timedelta(days=1)).isoformat()

        query = f"""
        WITH stale_alerts AS (
            SELECT
                table_name AS source,
                'STALE_DATA' AS alert_type,
                CONCAT('Table not updated for ',
                    CAST(DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) AS STRING),
                    ' hours') AS message,
                CASE
                    WHEN DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) > 48 THEN 'CRITICAL'
                    ELSE 'WARNING'
                END AS severity
            FROM `{data_freshness_table}`
            WHERE DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) > 24
            {self._domain_filter(domain)}
        ),
        error_alerts AS (
            SELECT
                'daily_summary' AS source,
                'HIGH_ERROR_RATE' AS alert_type,
                CONCAT('Error rate: ',
                    CAST(ROUND(100.0 * SUM(error_requests) / NULLIF(SUM(total_requests), 0), 1) AS STRING),
                    '%') AS message,
                CASE
                    WHEN 100.0 * SUM(error_requests) / NULLIF(SUM(total_requests), 0) > 10 THEN 'CRITICAL'
                    ELSE 'WARNING'
                END AS severity
            FROM `{self.daily_summary_table}`
            WHERE request_date = '{yesterday}'
            {self._domain_filter(domain)}
            HAVING 100.0 * SUM(error_requests) / NULLIF(SUM(total_requests), 0) > 5
        )
        SELECT * FROM stale_alerts
        UNION ALL
        SELECT * FROM error_alerts
        ORDER BY
            CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
            alert_type
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="pipeline_alerts",
            rows=rows,
            row_count=len(rows),
            description="Active pipeline alerts requiring attention",
        )
