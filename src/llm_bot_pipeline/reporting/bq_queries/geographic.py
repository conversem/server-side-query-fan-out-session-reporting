"""
Geographic analysis queries for BigQuery dashboard.

IMPORTANT: Geographic data represents crawler server locations,
NOT end-user geography.
"""

from datetime import date, timedelta
from typing import Optional

from ..models import QueryResult


class GeographicMixin:
    """Geographic analysis query methods."""

    def get_geographic_analysis(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 20,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get geographic distribution of bot requests.

        ⚠️ IMPORTANT CAVEAT: This data represents CRAWLER SERVER locations,
        NOT end-user geography. LLM providers typically run crawlers from
        data centers (often US-based), regardless of where end users are located.

        Use cases:
        - Understanding infrastructure distribution of LLM providers
        - Detecting unusual patterns (e.g., unexpected countries)
        - Compliance and access control decisions

        NOT useful for:
        - Understanding your actual audience geography
        - End-user location analysis

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            limit: Number of countries to return (default: 20)

        Returns:
            QueryResult with geographic distribution
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
        SELECT
            COALESCE(crawler_country, 'Unknown') AS crawler_country,
            COUNT(*) AS total_requests,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            ) AS percentage_share,
            COUNT(DISTINCT bot_provider) AS unique_providers,
            COUNT(DISTINCT bot_name) AS unique_bots,
            STRING_AGG(DISTINCT bot_provider, ', ' ORDER BY bot_provider) AS providers
        FROM `{self.clean_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
        {self._domain_filter(domain)}
        GROUP BY crawler_country
        ORDER BY total_requests DESC
        LIMIT {limit}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="geographic_analysis",
            rows=rows,
            row_count=len(rows),
            description=(
                f"Crawler server locations (NOT end-user geography) - Top {limit} countries. "
                "⚠️ See docstring for important caveats."
            ),
        )

    def get_geographic_by_provider(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get geographic distribution broken down by bot provider.

        ⚠️ CAVEAT: Represents crawler server locations, not end users.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)

        Returns:
            QueryResult with provider-country breakdown
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        query = f"""
        SELECT
            bot_provider,
            COALESCE(crawler_country, 'Unknown') AS crawler_country,
            COUNT(*) AS total_requests,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY bot_provider),
                2
            ) AS pct_of_provider_traffic
        FROM `{self.clean_table}`
        WHERE request_date >= '{start_date}' AND request_date <= '{end_date}'
        {self._domain_filter(domain)}
        GROUP BY bot_provider, crawler_country
        ORDER BY bot_provider, total_requests DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="geographic_by_provider",
            rows=rows,
            row_count=len(rows),
            description="Crawler locations by provider (NOT end-user geography)",
        )
