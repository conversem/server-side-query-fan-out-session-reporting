"""
Time-series analysis queries for local dashboard.

Covers:
- Hourly traffic patterns
- Day-of-week patterns
- Week-over-week comparisons
"""

from datetime import date, timedelta
from typing import Optional

from ..models import QueryResult


class TimeSeriesMixin:
    """Time-series analysis query methods."""

    def get_hourly_pattern(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """Get hourly traffic patterns for bot activity."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        domain_filter = self._domain_filter(domain)

        query = f"""
            SELECT
                request_hour,
                COUNT(*) AS total_requests,
                ROUND(
                    100.0 * COUNT(*) /
                    (SELECT COUNT(*) FROM bot_requests_daily
                     WHERE request_date >= '{start_date.isoformat()}'
                       AND request_date <= '{end_date.isoformat()}'
                       {domain_filter}),
                    2
                ) AS percentage_share,
                COUNT(DISTINCT request_date) AS days_with_activity,
                ROUND(CAST(COUNT(*) AS REAL) / COUNT(DISTINCT request_date), 2) AS avg_requests_per_day
            FROM bot_requests_daily
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
              {domain_filter}
            GROUP BY request_hour
            ORDER BY request_hour ASC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="hourly_pattern",
            rows=rows,
            row_count=len(rows),
            description="Hourly traffic patterns for LLM bot activity",
        )

    def get_day_of_week_pattern(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """Get day-of-week traffic patterns."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=29)

        domain_filter = self._domain_filter(domain)

        query = f"""
            SELECT
                day_of_week,
                COUNT(*) AS total_requests,
                ROUND(
                    100.0 * COUNT(*) /
                    (SELECT COUNT(*) FROM bot_requests_daily
                     WHERE request_date >= '{start_date.isoformat()}'
                       AND request_date <= '{end_date.isoformat()}'
                       {domain_filter}),
                    2
                ) AS percentage_share,
                COUNT(DISTINCT request_date) AS weeks_observed
            FROM bot_requests_daily
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
              {domain_filter}
            GROUP BY day_of_week
            ORDER BY
                CASE day_of_week
                    WHEN 'Monday' THEN 1
                    WHEN 'Tuesday' THEN 2
                    WHEN 'Wednesday' THEN 3
                    WHEN 'Thursday' THEN 4
                    WHEN 'Friday' THEN 5
                    WHEN 'Saturday' THEN 6
                    WHEN 'Sunday' THEN 7
                END
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="day_of_week_pattern",
            rows=rows,
            row_count=len(rows),
            description="Day-of-week traffic patterns for LLM bots",
        )

    def get_week_over_week_comparison(
        self,
        weeks: int = 4,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """Get week-over-week comparison of bot activity."""
        if weeks < 1:
            raise ValueError(f"weeks must be >= 1, got {weeks}")

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(weeks=weeks)

        domain_filter = self._domain_filter(domain)

        query = f"""
            WITH weekly_data AS (
                SELECT
                    date(request_date, 'weekday 0', '-6 days') AS week_start,
                    SUM(total_requests) AS total_requests,
                    SUM(successful_requests) AS successful_requests,
                    COUNT(DISTINCT bot_provider) AS unique_providers
                FROM daily_summary
                WHERE request_date >= '{start_date.isoformat()}'
                  AND request_date <= '{end_date.isoformat()}'
                  {domain_filter}
                GROUP BY week_start
            ),
            with_prev AS (
                SELECT
                    week_start,
                    total_requests,
                    successful_requests,
                    unique_providers,
                    LAG(total_requests) OVER (ORDER BY week_start) AS prev_week_requests
                FROM weekly_data
            )
            SELECT
                week_start,
                total_requests,
                successful_requests,
                unique_providers,
                prev_week_requests,
                ROUND(
                    100.0 * (total_requests - prev_week_requests) /
                    NULLIF(prev_week_requests, 0),
                    2
                ) AS wow_change_pct
            FROM with_prev
            ORDER BY week_start ASC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="week_over_week_comparison",
            rows=rows,
            row_count=len(rows),
            description=f"Week-over-week comparison for last {weeks} weeks",
        )
