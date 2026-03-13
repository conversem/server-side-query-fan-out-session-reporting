"""
Query fan-out session KPI queries for local dashboard.

Covers:
- Sessions per day
- Avg URLs per session (fan-out intensity)
- Multi-URL session rate
- Fan-out ratio
- High-confidence rate
- Daily session summary
- Provider session comparison
- Top session topics
- Session KPI summary
"""

from datetime import date, timedelta
from typing import Any, Optional

from ..models import QueryResult


class SessionMixin:
    """Query fan-out session KPI methods."""

    def get_sessions_per_day(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        bot_provider: Optional[str] = None,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get count of query fan-out sessions per day.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with daily session counts
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        params: dict = {}
        provider_filter = ""
        if bot_provider:
            provider_filter = "AND bot_provider = :bot_provider"
            params["bot_provider"] = bot_provider

        domain_filter = self._domain_filter(domain)

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
              {domain_filter}
            GROUP BY session_date
            ORDER BY session_date DESC
        """

        rows = self._execute_query(query, params)
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get average unique URLs per session (fan-out intensity).

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with average URLs per session
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        params: dict = {}
        provider_filter = ""
        if bot_provider:
            provider_filter = "AND bot_provider = :bot_provider"
            params["bot_provider"] = bot_provider

        domain_filter = self._domain_filter(domain)

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
              {domain_filter}
        """

        rows = self._execute_query(query, params)
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get percentage of sessions with 2+ URLs (topical authority indicator).

        Higher rate indicates LLMs find multiple relevant pages per query.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with multi-URL session rate
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        params: dict = {}
        provider_filter = ""
        if bot_provider:
            provider_filter = "AND bot_provider = :bot_provider"
            params["bot_provider"] = bot_provider

        domain_filter = self._domain_filter(domain)

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
              {domain_filter}
        """

        rows = self._execute_query(query, params)
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get fan-out ratio (total requests / total sessions).

        Measures request amplification per user query.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with fan-out ratio
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        params: dict = {}
        provider_filter = ""
        if bot_provider:
            provider_filter = "AND bot_provider = :bot_provider"
            params["bot_provider"] = bot_provider

        domain_filter = self._domain_filter(domain)

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
              {domain_filter}
        """

        rows = self._execute_query(query, params)
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get percentage of high-confidence sessions.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            bot_provider: Filter by bot provider (optional)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with confidence level distribution
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        params: dict = {}
        provider_filter = ""
        if bot_provider:
            provider_filter = "AND bot_provider = :bot_provider"
            params["bot_provider"] = bot_provider

        domain_filter = self._domain_filter(domain)

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
              {domain_filter}
        """

        rows = self._execute_query(query, params)
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get daily session summary with key metrics.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with daily session summary
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        domain_filter = self._domain_filter(domain)

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
              {domain_filter}
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get session metrics compared by bot provider.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with provider comparison
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        domain_filter = self._domain_filter(domain)

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
              {domain_filter}
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get most common session topics (fanout_session_name).

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            limit: Number of topics to return (default: 20)
            min_session_count: Minimum sessions for topic (default: 1)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with top session topics
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        domain_filter = self._domain_filter(domain)

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
              {domain_filter}
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
        domain: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Get all query fan-out session KPIs in one call.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            domain: Filter by domain (optional)

        Returns:
            Dictionary with all session KPIs
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        domain_filter = self._domain_filter(domain)

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
              {domain_filter}
        """

        rows = self._execute_query(query)
        summary = rows[0] if rows else {}

        return {
            "period": f"{start_date} to {end_date}",
            "kpis": summary,
        }
