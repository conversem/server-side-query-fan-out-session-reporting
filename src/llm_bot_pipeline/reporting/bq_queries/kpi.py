"""
KPI metrics and dashboard view queries for BigQuery dashboard.

Covers:
- LLM retrieval rate
- User-request bot share
- Bot provider diversity
- Response success rate
- Top content by LLM interest
- Daily activity trend
- Top URLs by user-request interest
- Provider breakdown with active days
"""

from datetime import date, timedelta
from typing import Optional

from ..models import QueryResult


class KpiMixin:
    """KPI metrics and dashboard view query methods."""

    # =========================================================================
    # KPI METRICS
    # =========================================================================

    def get_llm_retrieval_rate(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get LLM retrieval rate (average requests per URL per day).

        This metric shows how frequently LLM bots are accessing content.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            QueryResult with retrieval rate metrics
        """
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
            FROM `{self.url_performance_table}`
            WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
              {self._domain_filter(domain)}
        )
        SELECT
            'training' AS bot_category,
            COUNT(DISTINCT CONCAT(request_host, url_path)) AS unique_urls,
            SUM(training_hits) AS total_requests,
            COUNT(DISTINCT request_date) AS days_active,
            ROUND(
                SUM(training_hits) /
                NULLIF(COUNT(DISTINCT CONCAT(request_host, url_path)), 0) /
                NULLIF(COUNT(DISTINCT request_date), 0),
                2
            ) AS avg_requests_per_url_per_day
        FROM categorized
        WHERE training_hits > 0
        UNION ALL
        SELECT
            'user_request' AS bot_category,
            COUNT(DISTINCT CONCAT(request_host, url_path)) AS unique_urls,
            SUM(user_request_hits) AS total_requests,
            COUNT(DISTINCT request_date) AS days_active,
            ROUND(
                SUM(user_request_hits) /
                NULLIF(COUNT(DISTINCT CONCAT(request_host, url_path)), 0) /
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get percentage split between user_request and training bots.

        Shows the balance between real-time AI assistant queries
        vs training/crawling activity.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            QueryResult with category share percentages
        """
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
                SUM(SUM(total_requests)) OVER (),
                2
            ) AS percentage_share
        FROM `{self.daily_summary_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
          {self._domain_filter(domain)}
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get bot provider diversity metrics.

        Shows which AI providers are most active on your content.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            QueryResult with provider breakdown
        """
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
                SUM(SUM(total_requests)) OVER (),
                2
            ) AS percentage_share
        FROM `{self.daily_summary_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
          {self._domain_filter(domain)}
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get response success rate (% of 2xx responses).

        Shows how well your infrastructure serves LLM bot requests.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            QueryResult with success rate by provider
        """
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
        FROM `{self.daily_summary_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
          {self._domain_filter(domain)}
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get top URLs ranked by LLM bot interest.

        Shows which content is most frequently accessed by LLM bots.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            limit: Number of URLs to return (default: 20)

        Returns:
            QueryResult with top URLs and their metrics
        """
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
        FROM `{self.url_performance_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
          {self._domain_filter(domain)}
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
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get daily bot activity trend for the last N days.

        Shows traffic patterns over time for trend analysis.

        Args:
            days: Number of days to include (default: 30)

        Returns:
            QueryResult with daily activity breakdown

        Raises:
            ValueError: If days < 1
        """
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
        FROM `{self.daily_summary_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
          {self._domain_filter(domain)}
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

    def get_top_urls_by_user_request_interest(
        self,
        days: int = 7,
        limit: int = 20,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get top URLs by user-request bot interest.

        Focuses on real-time AI assistant queries, excluding training crawlers.

        Args:
            days: Number of days to analyze (default: 7)
            limit: Number of URLs to return (default: 20)

        Returns:
            QueryResult with top URLs for user requests

        Raises:
            ValueError: If days < 1 or limit < 1
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
        FROM `{self.url_performance_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
          {self._domain_filter(domain)}
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

    def get_provider_breakdown_with_active_days(
        self,
        days: int = 30,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get bot provider breakdown with active day counts.

        Shows provider activity patterns over time.

        Args:
            days: Number of days to analyze (default: 30)

        Returns:
            QueryResult with provider activity details

        Raises:
            ValueError: If days < 1
        """
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        query = f"""
        SELECT
            bot_provider,
            COUNT(DISTINCT request_date) AS days_active,
            {days} AS total_days_in_period,
            ROUND(100.0 * COUNT(DISTINCT request_date) / {days}, 2) AS activity_rate_pct,
            SUM(total_requests) AS total_requests,
            ROUND(SUM(total_requests) / COUNT(DISTINCT request_date), 2) AS avg_daily_requests,
            SUM(unique_urls) AS unique_urls_accessed,
            STRING_AGG(DISTINCT bot_name, ', ' ORDER BY bot_name) AS bot_names
        FROM `{self.daily_summary_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
          {self._domain_filter(domain)}
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
