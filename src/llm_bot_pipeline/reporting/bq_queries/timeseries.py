"""
Time-series analysis and summary queries for BigQuery dashboard.

Covers:
- Hourly traffic patterns
- Day-of-week patterns
- Week-over-week comparisons
- Executive summary
"""

from datetime import date, timedelta
from typing import Any, Optional

from ..models import QueryResult


class TimeSeriesMixin:
    """Time-series analysis and summary query methods."""

    def get_hourly_pattern(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get hourly traffic patterns for bot activity.

        Shows when LLM bots are most active (useful for capacity planning).

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            QueryResult with hourly breakdown
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
        SELECT
            request_hour,
            COUNT(*) AS total_requests,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            ) AS percentage_share,
            COUNT(DISTINCT request_date) AS days_with_activity,
            ROUND(COUNT(*) / COUNT(DISTINCT request_date), 2) AS avg_requests_per_day
        FROM `{self.clean_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
        {self._domain_filter(domain)}
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
        """
        Get day-of-week traffic patterns.

        Shows weekly patterns (useful for understanding bot behavior).

        Args:
            start_date: Start date (default: 30 days ago)
            end_date: End date (default: yesterday)

        Returns:
            QueryResult with day-of-week breakdown
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=29)

        query = f"""
        SELECT
            day_of_week,
            COUNT(*) AS total_requests,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            ) AS percentage_share,
            COUNT(DISTINCT request_date) AS weeks_observed
        FROM `{self.clean_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
        {self._domain_filter(domain)}
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
        """
        Get week-over-week comparison of bot activity.

        Shows growth/decline trends for strategic planning.

        Args:
            weeks: Number of weeks to compare (default: 4)

        Returns:
            QueryResult with weekly comparison metrics

        Raises:
            ValueError: If weeks < 1
        """
        if weeks < 1:
            raise ValueError(f"weeks must be >= 1, got {weeks}")

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(weeks=weeks)

        query = f"""
        WITH weekly_data AS (
            SELECT
                DATE_TRUNC(request_date, WEEK(MONDAY)) AS week_start,
                SUM(total_requests) AS total_requests,
                SUM(successful_requests) AS successful_requests,
                COUNT(DISTINCT bot_provider) AS unique_providers
            FROM `{self.daily_summary_table}`
            WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
            {self._domain_filter(domain)}
            GROUP BY week_start
        )
        SELECT
            week_start,
            total_requests,
            successful_requests,
            unique_providers,
            LAG(total_requests) OVER (ORDER BY week_start) AS prev_week_requests,
            ROUND(
                100.0 * (total_requests - LAG(total_requests) OVER (ORDER BY week_start)) /
                NULLIF(LAG(total_requests) OVER (ORDER BY week_start), 0),
                2
            ) AS wow_change_pct
        FROM weekly_data
        ORDER BY week_start ASC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="week_over_week_comparison",
            rows=rows,
            row_count=len(rows),
            description=f"Week-over-week comparison for last {weeks} weeks",
        )

    def get_executive_summary(
        self,
        days: int = 7,
        domain: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Get executive summary of all key metrics.

        Single call to get a high-level overview for dashboards.

        Args:
            days: Number of days to analyze (default: 7)

        Returns:
            Dictionary with summary metrics
        """
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        query = f"""
        SELECT
            SUM(total_requests) AS total_requests,
            SUM(successful_requests) AS successful_requests,
            SUM(error_requests) AS error_requests,
            ROUND(100.0 * SUM(successful_requests) / NULLIF(SUM(total_requests), 0), 2) AS success_rate_pct,
            COUNT(DISTINCT bot_provider) AS unique_providers,
            COUNT(DISTINCT bot_name) AS unique_bots,
            SUM(unique_urls) AS unique_urls_accessed,
            SUM(CASE WHEN bot_category = 'user_request' THEN total_requests ELSE 0 END) AS user_request_hits,
            SUM(CASE WHEN bot_category = 'training' THEN total_requests ELSE 0 END) AS training_hits,
            ROUND(
                100.0 * SUM(CASE WHEN bot_category = 'user_request' THEN total_requests ELSE 0 END) /
                NULLIF(SUM(total_requests), 0),
                2
            ) AS user_request_pct
        FROM `{self.daily_summary_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
        {self._domain_filter(domain)}
        """

        rows = self._execute_query(query)
        summary = rows[0] if rows else {}

        return {
            "period": f"{start_date} to {end_date}",
            "days": days,
            "metrics": summary,
        }
