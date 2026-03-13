"""
Refinement monitoring queries for local dashboard.

Covers:
- Refinement summary metrics
- Refinement by provider
- Daily refinement trend
- Split session details
- Refinement log
- Refinement KPI summary
"""

from datetime import date, timedelta
from typing import Any, Optional

from ..models import QueryResult


class RefinementMixin:
    """Refinement monitoring query methods."""

    def get_refinement_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get refinement summary metrics for sessions.

        Metrics include:
        - refinement_rate: % of sessions that were refined
        - split_success_rate: % of refined sessions successfully split
        - avg_coherence_improvement: mean MIBCS improvement from refinement

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with refinement summary
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        domain_filter = self._domain_filter(domain)

        query = f"""
            SELECT
                COUNT(*) AS total_sessions,
                SUM(CASE WHEN was_refined = 1 THEN 1 ELSE 0 END) AS refined_sessions,
                SUM(CASE WHEN parent_session_id IS NOT NULL THEN 1 ELSE 0 END) AS sub_sessions,
                ROUND(
                    100.0 * SUM(CASE WHEN was_refined = 1 THEN 1 ELSE 0 END) /
                    NULLIF(COUNT(*), 0),
                    2
                ) AS refinement_rate_pct,
                COUNT(DISTINCT parent_session_id) AS parent_sessions_split,
                ROUND(AVG(mean_cosine_similarity), 3) AS avg_mibcs_overall,
                ROUND(
                    AVG(CASE WHEN was_refined = 1 THEN mean_cosine_similarity END),
                    3
                ) AS avg_mibcs_refined,
                ROUND(
                    AVG(CASE WHEN was_refined = 0 THEN mean_cosine_similarity END),
                    3
                ) AS avg_mibcs_unrefined
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
              {domain_filter}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="refinement_summary",
            rows=rows,
            row_count=len(rows),
            description="Session refinement summary metrics",
        )

    def get_refinement_by_provider(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get refinement metrics broken down by bot provider.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with refinement by provider
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        domain_filter = self._domain_filter(domain)

        query = f"""
            SELECT
                bot_provider,
                COUNT(*) AS total_sessions,
                SUM(CASE WHEN was_refined = 1 THEN 1 ELSE 0 END) AS refined_sessions,
                ROUND(
                    100.0 * SUM(CASE WHEN was_refined = 1 THEN 1 ELSE 0 END) /
                    NULLIF(COUNT(*), 0),
                    2
                ) AS refinement_rate_pct,
                ROUND(AVG(mean_cosine_similarity), 3) AS avg_mibcs,
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
            ORDER BY total_sessions DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="refinement_by_provider",
            rows=rows,
            row_count=len(rows),
            description="Refinement metrics by bot provider",
        )

    def get_refinement_trend(
        self,
        days: int = 14,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get daily refinement trend.

        Args:
            days: Number of days to analyze (default: 14)
            domain: Filter by domain (optional)

        Returns:
            QueryResult with daily refinement trend
        """
        if days < 1:
            raise ValueError(f"days must be >= 1, got {days}")

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)

        domain_filter = self._domain_filter(domain)

        query = f"""
            SELECT
                session_date,
                COUNT(*) AS total_sessions,
                SUM(CASE WHEN was_refined = 1 THEN 1 ELSE 0 END) AS refined_sessions,
                ROUND(
                    100.0 * SUM(CASE WHEN was_refined = 1 THEN 1 ELSE 0 END) /
                    NULLIF(COUNT(*), 0),
                    2
                ) AS refinement_rate_pct,
                ROUND(AVG(mean_cosine_similarity), 3) AS avg_mibcs
            FROM query_fanout_sessions
            WHERE session_date >= '{start_date.isoformat()}'
              AND session_date <= '{end_date.isoformat()}'
              {domain_filter}
            GROUP BY session_date
            ORDER BY session_date DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="refinement_trend",
            rows=rows,
            row_count=len(rows),
            description=f"Daily refinement trend for last {days} days",
        )

    def get_split_session_details(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 20,
        domain: Optional[str] = None,
    ) -> QueryResult:
        """
        Get details of split (refined) sessions with their sub-sessions.

        The domain filter is applied to both the CTE (parent selection) and the
        outer JOIN (sub-session retrieval) to enforce strict per-domain isolation.
        Query fan-out sessions are always single-domain; the dual filter ensures
        no cross-domain sub-sessions appear even if data integrity is compromised.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            limit: Maximum number of parent sessions to return
            domain: Filter by domain (optional)

        Returns:
            QueryResult with split session details
        """
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")

        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        domain_filter = self._domain_filter(domain)

        query = f"""
            WITH parent_sessions AS (
                SELECT DISTINCT parent_session_id
                FROM query_fanout_sessions
                WHERE session_date >= '{start_date.isoformat()}'
                  AND session_date <= '{end_date.isoformat()}'
                  AND parent_session_id IS NOT NULL
                  {domain_filter}
                LIMIT {limit}
            )
            SELECT
                p.parent_session_id,
                COUNT(*) AS sub_session_count,
                SUM(s.request_count) AS total_requests,
                ROUND(AVG(s.mean_cosine_similarity), 3) AS avg_mibcs,
                GROUP_CONCAT(s.session_id) AS sub_session_ids,
                MIN(s.session_date) AS session_date
            FROM parent_sessions p
            JOIN query_fanout_sessions s ON s.parent_session_id = p.parent_session_id
            WHERE 1=1
              {domain_filter}
            GROUP BY p.parent_session_id
            ORDER BY sub_session_count DESC
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="split_session_details",
            rows=rows,
            row_count=len(rows),
            description=f"Top {limit} split session details",
        )

    def get_refinement_log(
        self,
        limit: int = 20,
    ) -> QueryResult:
        """
        Get recent entries from the session refinement log.

        Args:
            limit: Maximum number of log entries to return

        Returns:
            QueryResult with refinement log entries
        """
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")

        query = f"""
            SELECT
                run_timestamp,
                window_ms,
                total_bundles,
                collision_candidates,
                bundles_split,
                sub_bundles_created,
                ROUND(mean_mibcs_improvement, 3) AS mean_mibcs_improvement,
                ROUND(refinement_duration_ms, 1) AS refinement_duration_ms,
                collision_ip_threshold,
                similarity_threshold
            FROM session_refinement_log
            ORDER BY run_timestamp DESC
            LIMIT {limit}
        """

        rows = self._execute_query(query)
        return QueryResult(
            query_name="refinement_log",
            rows=rows,
            row_count=len(rows),
            description=f"Last {limit} refinement log entries",
        )

    def get_refinement_kpi_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domain: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Get all refinement KPIs in one call.

        Args:
            start_date: Start date (default: 7 days ago)
            end_date: End date (default: yesterday)
            domain: Filter by domain (optional)

        Returns:
            Dictionary with all refinement KPIs
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=6)

        summary = self.get_refinement_summary(start_date, end_date, domain=domain)
        summary_row = summary.rows[0] if summary.rows else {}

        log = self.get_refinement_log(limit=1)
        last_run = log.rows[0] if log.rows else {}

        return {
            "period": f"{start_date} to {end_date}",
            "metrics": {
                "total_sessions": summary_row.get("total_sessions", 0),
                "refined_sessions": summary_row.get("refined_sessions", 0),
                "refinement_rate_pct": summary_row.get("refinement_rate_pct", 0),
                "parent_sessions_split": summary_row.get("parent_sessions_split", 0),
                "avg_mibcs_overall": summary_row.get("avg_mibcs_overall"),
                "avg_mibcs_refined": summary_row.get("avg_mibcs_refined"),
                "avg_mibcs_unrefined": summary_row.get("avg_mibcs_unrefined"),
            },
            "last_refinement_run": {
                "timestamp": last_run.get("run_timestamp"),
                "bundles_split": last_run.get("bundles_split"),
                "mean_mibcs_improvement": last_run.get("mean_mibcs_improvement"),
            },
        }
