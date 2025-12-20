"""
Local dashboard queries using storage abstraction.

Provides SQLite-compatible dashboard queries for local development mode.
Returns same result structures as SQLite version for consistency.

SQL Compatibility Notes:
- STRING_AGG() → GROUP_CONCAT()
- DATE_TRUNC(date, WEEK) → strftime with week calculation
- COUNTIF() → SUM(CASE WHEN...)
- Window functions work in SQLite 3.25+
"""

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

from ..pipeline.sql_compat import SQLBuilder
from ..storage import StorageBackend, get_backend

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Result of a dashboard query."""

    query_name: str
    rows: list[dict[str, Any]]
    row_count: int
    description: str = ""


class LocalDashboardQueries:
    """
    Local dashboard queries using storage abstraction.

    Works with both SQLite  and SQLite backends.
    All queries operate on local reporting tables.
    """

    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        backend_type: str = "sqlite",
        db_path: Optional[Path] = None,
    ):
        """
        Initialize local dashboard queries.

        Args:
            backend: Pre-initialized StorageBackend (optional)
            backend_type: Backend type if creating new ('sqlite')
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

        logger.info(
            f"LocalDashboardQueries initialized with {self._backend_type} backend"
        )

    def initialize(self) -> None:
        """Initialize the backend."""
        if not self._initialized:
            self._backend.initialize()
            self._initialized = True

    def close(self) -> None:
        """Close the backend connection."""
        if self._owns_backend:
            self._backend.close()

    def __enter__(self) -> "LocalDashboardQueries":
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def _execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        self.initialize()
        return self._backend.query(query)

    # =========================================================================
    # KPI METRICS
    # =========================================================================

    def get_llm_retrieval_rate(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> QueryResult:
        """Get LLM retrieval rate (average requests per URL per day)."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            WITH categorized AS (
                SELECT
                    request_host,
                    url_path,
                    request_date,
                    training_hits,
                    user_request_hits,
                    total_bot_requests
                FROM url_performance
                WHERE request_date >= '{start_date.isoformat()}'
                  AND request_date <= '{end_date.isoformat()}'
            )
            SELECT
                'training' AS bot_category,
                COUNT(DISTINCT request_host || url_path) AS unique_urls,
                SUM(training_hits) AS total_requests,
                COUNT(DISTINCT request_date) AS days_active,
                ROUND(
                    CAST(SUM(training_hits) AS REAL) /
                    NULLIF(COUNT(DISTINCT request_host || url_path), 0) /
                    NULLIF(COUNT(DISTINCT request_date), 0),
                    2
                ) AS avg_requests_per_url_per_day
            FROM categorized
            WHERE training_hits > 0
            UNION ALL
            SELECT
                'user_request' AS bot_category,
                COUNT(DISTINCT request_host || url_path) AS unique_urls,
                SUM(user_request_hits) AS total_requests,
                COUNT(DISTINCT request_date) AS days_active,
                ROUND(
                    CAST(SUM(user_request_hits) AS REAL) /
                    NULLIF(COUNT(DISTINCT request_host || url_path), 0) /
                    NULLIF(COUNT(DISTINCT request_date), 0),
                    2
                ) AS avg_requests_per_url_per_day
            FROM categorized
            WHERE user_request_hits > 0
            ORDER BY total_requests DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="llm_retrieval_rate",
            rows=rows,
            row_count=len(rows),
            description="Average LLM bot requests per URL per day by category",
        )

    def get_user_request_bot_share(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> QueryResult:
        """Get percentage split between user_request and training bots."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            SELECT
                bot_category,
                SUM(total_requests) AS total_requests,
                ROUND(
                    100.0 * SUM(total_requests) /
                    (SELECT SUM(total_requests) FROM daily_summary
                     WHERE request_date >= '{start_date.isoformat()}'
                       AND request_date <= '{end_date.isoformat()}'),
                    2
                ) AS percentage_share
            FROM daily_summary
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
            GROUP BY bot_category
            ORDER BY total_requests DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="user_request_bot_share",
            rows=rows,
            row_count=len(rows),
            description="Percentage split between user_request and training bot categories",
        )

    def get_bot_provider_diversity(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> QueryResult:
        """Get bot provider diversity metrics."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            SELECT
                bot_provider,
                COUNT(DISTINCT bot_name) AS unique_bots,
                SUM(total_requests) AS total_requests,
                COUNT(DISTINCT request_date) AS days_active,
                ROUND(
                    100.0 * SUM(total_requests) /
                    (SELECT SUM(total_requests) FROM daily_summary
                     WHERE request_date >= '{start_date.isoformat()}'
                       AND request_date <= '{end_date.isoformat()}'),
                    2
                ) AS percentage_share
            FROM daily_summary
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
            GROUP BY bot_provider
            ORDER BY total_requests DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="bot_provider_diversity",
            rows=rows,
            row_count=len(rows),
            description="Bot provider breakdown with request counts and share",
        )

    def get_response_success_rate(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> QueryResult:
        """Get response success rate (% of 2xx responses)."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            SELECT
                bot_provider,
                SUM(total_requests) AS total_requests,
                SUM(successful_requests) AS successful_requests,
                SUM(error_requests) AS error_requests,
                SUM(redirect_requests) AS redirect_requests,
                ROUND(
                    100.0 * SUM(successful_requests) / NULLIF(SUM(total_requests), 0),
                    2
                ) AS success_rate_pct,
                ROUND(
                    100.0 * SUM(error_requests) / NULLIF(SUM(total_requests), 0),
                    2
                ) AS error_rate_pct
            FROM daily_summary
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
            GROUP BY bot_provider
            ORDER BY total_requests DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="response_success_rate",
            rows=rows,
            row_count=len(rows),
            description="Response success rate by bot provider",
        )

    def get_top_content_by_llm_interest(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 20,
    ) -> QueryResult:
        """Get top URLs ranked by LLM bot interest."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            SELECT
                request_host,
                url_path,
                SUM(total_bot_requests) AS total_requests,
                SUM(training_hits) AS training_hits,
                SUM(user_request_hits) AS user_request_hits,
                MAX(unique_bot_providers) AS unique_providers,
                MIN(first_seen) AS first_seen,
                MAX(last_seen) AS last_seen
            FROM url_performance
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
            GROUP BY request_host, url_path
            ORDER BY total_requests DESC
            LIMIT {limit}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="top_content_by_llm_interest",
            rows=rows,
            row_count=len(rows),
            description=f"Top {limit} URLs by LLM bot interest",
        )

    # =========================================================================
    # DASHBOARD VIEWS
    # =========================================================================

    def get_daily_activity_trend(
        self,
        days: int = 30,
    ) -> QueryResult:
        """Get daily bot activity trend for the last N days."""
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        query = f"""
            SELECT
                request_date,
                SUM(total_requests) AS total_requests,
                SUM(successful_requests) AS successful_requests,
                SUM(error_requests) AS error_requests,
                SUM(CASE WHEN bot_category = 'user_request' THEN total_requests ELSE 0 END) AS user_request_hits,
                SUM(CASE WHEN bot_category = 'training' THEN total_requests ELSE 0 END) AS training_hits,
                COUNT(DISTINCT bot_provider) AS unique_providers
            FROM daily_summary
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
            GROUP BY request_date
            ORDER BY request_date ASC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="daily_activity_trend",
            rows=rows,
            row_count=len(rows),
            description=f"Daily bot activity trend for last {days} days",
        )

    def get_provider_breakdown_with_active_days(
        self,
        days: int = 30,
    ) -> QueryResult:
        """Get bot provider breakdown with active day counts."""
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        # SQLite uses GROUP_CONCAT instead of STRING_AGG
        query = f"""
            SELECT
                bot_provider,
                COUNT(DISTINCT request_date) AS days_active,
                {days} AS total_days_in_period,
                ROUND(100.0 * COUNT(DISTINCT request_date) / {days}, 2) AS activity_rate_pct,
                SUM(total_requests) AS total_requests,
                ROUND(CAST(SUM(total_requests) AS REAL) / COUNT(DISTINCT request_date), 2) AS avg_daily_requests,
                SUM(unique_urls) AS unique_urls_accessed,
                GROUP_CONCAT(DISTINCT bot_name) AS bot_names
            FROM daily_summary
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
            GROUP BY bot_provider
            ORDER BY total_requests DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="provider_breakdown_with_active_days",
            rows=rows,
            row_count=len(rows),
            description=f"Bot provider breakdown with activity metrics ({days} days)",
        )

    def get_top_urls_by_user_request_interest(
        self,
        days: int = 7,
        limit: int = 20,
    ) -> QueryResult:
        """
        Get top URLs by user-request bot interest.

        Focuses on real-time AI assistant queries, excluding training crawlers.

        Args:
            days: Number of days to analyze (default: 7)
            limit: Number of URLs to return (default: 20)

        Returns:
            QueryResult with top URLs for user requests
        """
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        query = f"""
            SELECT
                request_host,
                url_path,
                SUM(user_request_hits) AS user_request_hits,
                SUM(training_hits) AS training_hits,
                SUM(total_bot_requests) AS total_requests,
                ROUND(
                    100.0 * SUM(user_request_hits) / NULLIF(SUM(total_bot_requests), 0),
                    2
                ) AS user_request_pct,
                MAX(unique_bot_providers) AS unique_providers
            FROM url_performance
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
              AND user_request_hits > 0
            GROUP BY request_host, url_path
            ORDER BY user_request_hits DESC
            LIMIT {limit}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="top_urls_by_user_request_interest",
            rows=rows,
            row_count=len(rows),
            description=f"Top {limit} URLs by user-request bot interest ({days} days)",
        )

    # =========================================================================
    # TIME-SERIES ANALYSIS
    # =========================================================================

    def get_hourly_pattern(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> QueryResult:
        """Get hourly traffic patterns for bot activity."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            SELECT
                request_hour,
                COUNT(*) AS total_requests,
                ROUND(
                    100.0 * COUNT(*) /
                    (SELECT COUNT(*) FROM bot_requests_daily
                     WHERE request_date >= '{start_date.isoformat()}'
                       AND request_date <= '{end_date.isoformat()}'),
                    2
                ) AS percentage_share,
                COUNT(DISTINCT request_date) AS days_with_activity,
                ROUND(CAST(COUNT(*) AS REAL) / COUNT(DISTINCT request_date), 2) AS avg_requests_per_day
            FROM bot_requests_daily
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
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
    ) -> QueryResult:
        """Get day-of-week traffic patterns."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=29)

        query = f"""
            SELECT
                day_of_week,
                COUNT(*) AS total_requests,
                ROUND(
                    100.0 * COUNT(*) /
                    (SELECT COUNT(*) FROM bot_requests_daily
                     WHERE request_date >= '{start_date.isoformat()}'
                       AND request_date <= '{end_date.isoformat()}'),
                    2
                ) AS percentage_share,
                COUNT(DISTINCT request_date) AS weeks_observed
            FROM bot_requests_daily
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
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
    ) -> QueryResult:
        """Get week-over-week comparison of bot activity."""
        if weeks < 1:
            raise ValueError(f"weeks must be >= 1, got {weeks}")

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(weeks=weeks)

        # SQLite: Use strftime for week grouping
        # strftime('%W', date) gives ISO week number
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

    # =========================================================================
    # SUMMARY REPORT
    # =========================================================================

    def get_executive_summary(
        self,
        days: int = 7,
    ) -> dict[str, Any]:
        """Get executive summary of all key metrics."""
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
            FROM daily_summary
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
        """

        rows = self._execute_query(query)
        summary = rows[0] if rows else {}

        return {
            "period": f"{start_date} to {end_date}",
            "days": days,
            "metrics": summary,
        }

    # =========================================================================
    # QUERY FAN-OUT SESSION KPIs
    # =========================================================================

    def get_sessions_per_day(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        bot_provider: Optional[str] = None,
    ) -> QueryResult:
        """
        Get count of query fan-out sessions per day.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)

        Returns:
            QueryResult with daily session counts
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        provider_filter = ""
        if bot_provider:
            provider_filter = f"AND bot_provider = '{bot_provider}'"

        query = f"""
            SELECT
                session_date,
                COUNT(DISTINCT session_id) AS total_sessions,
                SUM(request_count) AS total_requests,
                COUNT(DISTINCT bot_provider) AS unique_providers
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
              {provider_filter}
            GROUP BY session_date
            ORDER BY session_date DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="sessions_per_day",
            rows=rows,
            row_count=len(rows),
            description="Query fan-out sessions per day",
        )

    def get_avg_urls_per_session(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        bot_provider: Optional[str] = None,
    ) -> QueryResult:
        """
        Get average unique URLs per session (fan-out intensity).

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)

        Returns:
            QueryResult with average URLs per session
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        provider_filter = ""
        if bot_provider:
            provider_filter = f"AND bot_provider = '{bot_provider}'"

        query = f"""
            SELECT
                ROUND(AVG(unique_urls), 2) AS avg_urls_per_session,
                ROUND(AVG(request_count), 2) AS avg_requests_per_session,
                MIN(unique_urls) AS min_urls,
                MAX(unique_urls) AS max_urls,
                COUNT(*) AS total_sessions
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
              {provider_filter}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="avg_urls_per_session",
            rows=rows,
            row_count=len(rows),
            description="Average unique URLs per session (fan-out intensity)",
        )

    def get_multi_url_session_rate(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        bot_provider: Optional[str] = None,
    ) -> QueryResult:
        """
        Get percentage of sessions with 2+ URLs (topical authority indicator).

        Higher rate indicates LLMs find multiple relevant pages per query.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)

        Returns:
            QueryResult with multi-URL session rate
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        provider_filter = ""
        if bot_provider:
            provider_filter = f"AND bot_provider = '{bot_provider}'"

        query = f"""
            SELECT
                COUNT(*) AS total_sessions,
                SUM(CASE WHEN unique_urls >= 2 THEN 1 ELSE 0 END) AS multi_url_sessions,
                SUM(CASE WHEN unique_urls = 1 THEN 1 ELSE 0 END) AS single_url_sessions,
                ROUND(
                    100.0 * SUM(CASE WHEN unique_urls >= 2 THEN 1 ELSE 0 END) /
                    NULLIF(COUNT(*), 0),
                    2
                ) AS multi_url_rate_pct
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
              {provider_filter}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="multi_url_session_rate",
            rows=rows,
            row_count=len(rows),
            description="Multi-URL session rate (topical authority indicator)",
        )

    def get_fanout_ratio(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        bot_provider: Optional[str] = None,
    ) -> QueryResult:
        """
        Get fan-out ratio (total requests / total sessions).

        Measures request amplification per user query.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)

        Returns:
            QueryResult with fan-out ratio
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        provider_filter = ""
        if bot_provider:
            provider_filter = f"AND bot_provider = '{bot_provider}'"

        query = f"""
            SELECT
                SUM(request_count) AS total_requests,
                COUNT(session_id) AS total_sessions,
                ROUND(
                    CAST(SUM(request_count) AS REAL) / NULLIF(COUNT(session_id), 0),
                    2
                ) AS fanout_ratio
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
              {provider_filter}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="fanout_ratio",
            rows=rows,
            row_count=len(rows),
            description="Fan-out ratio (request amplification per session)",
        )

    def get_high_confidence_rate(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        bot_provider: Optional[str] = None,
    ) -> QueryResult:
        """
        Get percentage of high-confidence sessions.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)

        Returns:
            QueryResult with confidence level distribution
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        provider_filter = ""
        if bot_provider:
            provider_filter = f"AND bot_provider = '{bot_provider}'"

        query = f"""
            SELECT
                COUNT(*) AS total_sessions,
                SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) AS high_confidence,
                SUM(CASE WHEN confidence_level = 'medium' THEN 1 ELSE 0 END) AS medium_confidence,
                SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) AS low_confidence,
                ROUND(
                    100.0 * SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) /
                    NULLIF(COUNT(*), 0),
                    2
                ) AS high_confidence_rate_pct
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
              {provider_filter}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="high_confidence_rate",
            rows=rows,
            row_count=len(rows),
            description="Confidence level distribution for sessions",
        )

    def get_daily_session_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> QueryResult:
        """
        Get daily session summary with key metrics (PRD query).

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            QueryResult with daily session summary
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            SELECT
                session_date,
                COUNT(*) AS total_sessions,
                ROUND(AVG(request_count), 2) AS avg_requests_per_session,
                ROUND(AVG(unique_urls), 2) AS avg_urls_per_session,
                ROUND(AVG(mean_cosine_similarity), 3) AS avg_coherence,
                ROUND(
                    100.0 * SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) /
                    NULLIF(COUNT(*), 0),
                    2
                ) AS high_confidence_pct
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
            GROUP BY session_date
            ORDER BY session_date DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="daily_session_summary",
            rows=rows,
            row_count=len(rows),
            description="Daily query fan-out session summary",
        )

    def get_provider_session_comparison(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> QueryResult:
        """
        Get session metrics compared by bot provider (PRD query).

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            QueryResult with provider comparison
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            SELECT
                bot_provider,
                COUNT(*) AS sessions,
                ROUND(AVG(request_count), 2) AS avg_bundle_size,
                ROUND(AVG(mean_cosine_similarity), 3) AS avg_coherence,
                ROUND(AVG(unique_urls), 2) AS avg_urls_per_session,
                ROUND(
                    100.0 * SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) /
                    NULLIF(COUNT(*), 0),
                    2
                ) AS high_confidence_pct
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
            GROUP BY bot_provider
            ORDER BY sessions DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="provider_session_comparison",
            rows=rows,
            row_count=len(rows),
            description="Session metrics by bot provider",
        )

    def get_top_session_topics(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 20,
        min_session_count: int = 1,
    ) -> QueryResult:
        """
        Get most common session topics (fanout_session_name).

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            limit: Number of topics to return (default: 20)
            min_session_count: Minimum sessions for topic (default: 1)

        Returns:
            QueryResult with top session topics
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            SELECT
                fanout_session_name,
                COUNT(*) AS session_count,
                ROUND(AVG(request_count), 2) AS avg_requests,
                ROUND(AVG(mean_cosine_similarity), 3) AS avg_coherence,
                GROUP_CONCAT(DISTINCT bot_provider) AS providers
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
              AND fanout_session_name IS NOT NULL
            GROUP BY fanout_session_name
            HAVING COUNT(*) >= {min_session_count}
            ORDER BY session_count DESC
            LIMIT {limit}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="top_session_topics",
            rows=rows,
            row_count=len(rows),
            description=f"Top {limit} session topics by frequency",
        )

    def get_session_kpi_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> dict[str, Any]:
        """
        Get all query fan-out session KPIs in one call.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            Dictionary with all session KPIs
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
            SELECT
                COUNT(*) AS total_sessions,
                SUM(request_count) AS total_requests,
                ROUND(AVG(unique_urls), 2) AS avg_urls_per_session,
                ROUND(AVG(request_count), 2) AS avg_requests_per_session,
                ROUND(
                    CAST(SUM(request_count) AS REAL) / NULLIF(COUNT(*), 0),
                    2
                ) AS fanout_ratio,
                ROUND(
                    100.0 * SUM(CASE WHEN unique_urls >= 2 THEN 1 ELSE 0 END) /
                    NULLIF(COUNT(*), 0),
                    2
                ) AS multi_url_session_rate_pct,
                ROUND(
                    100.0 * SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) /
                    NULLIF(COUNT(*), 0),
                    2
                ) AS high_confidence_rate_pct,
                ROUND(AVG(mean_cosine_similarity), 3) AS avg_coherence,
                COUNT(DISTINCT bot_provider) AS unique_providers,
                MIN(session_date) AS earliest_date,
                MAX(session_date) AS latest_date
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
        """

        rows = self._execute_query(query)
        summary = rows[0] if rows else {}

        return {
            "period": f"{start_date} to {end_date}",
            "kpis": summary,
        }
