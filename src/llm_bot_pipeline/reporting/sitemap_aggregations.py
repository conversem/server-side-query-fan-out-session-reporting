"""
Sitemap freshness and URL volume decay aggregations.

Cross-references sitemap URLs with bot request data to produce:
- sitemap_freshness: Per-URL freshness metrics (request counts, days since lastmod)
- url_volume_decay: Time-series decay analysis (weekly/monthly request trends)

Works with both SQLite and BigQuery via the StorageBackend abstraction.
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from ..config.constants import (
    TABLE_CLEAN_BOT_REQUESTS,
    TABLE_SITEMAP_FRESHNESS,
    TABLE_SITEMAP_URLS,
    TABLE_URL_VOLUME_DECAY,
)
from ..storage import StorageBackend

logger = logging.getLogger(__name__)


@dataclass
class SitemapAggregationResult:
    """Result of a sitemap aggregation operation."""

    success: bool
    table_name: str
    rows_inserted: int = 0
    error: Optional[str] = None
    duration_seconds: float = 0.0


class SitemapAggregator:
    """
    Aggregates sitemap freshness and URL volume decay data.

    Uses SQL queries that work on both SQLite and BigQuery backends.
    Populates the sitemap_freshness and url_volume_decay tables by
    cross-referencing sitemap_urls with bot_requests_daily.
    """

    def __init__(self, backend: StorageBackend):
        self._backend = backend

    def _table(self, table_name: str) -> str:
        """Fully qualified table reference (handles BigQuery dataset prefix)."""
        full_id = self._backend.get_full_table_id(table_name)
        if getattr(self._backend, "backend_type", "sqlite") == "bigquery":
            return f"`{full_id}`"
        return full_id

    def aggregate_freshness(
        self,
        reference_date: Optional[date] = None,
    ) -> SitemapAggregationResult:
        """Compute sitemap freshness by joining sitemap URLs with request data.

        For each URL in sitemap_urls, computes:
        - first_seen_date / last_seen_date from bot_requests_daily
        - request_count and unique_bots from bot_requests_daily
        - days_since_lastmod relative to reference_date

        Args:
            reference_date: Date to compute days_since_lastmod from.
                Defaults to today.

        Returns:
            SitemapAggregationResult with row counts.
        """
        if reference_date is None:
            reference_date = date.today()

        start = datetime.now(timezone.utc)
        ref_str = reference_date.isoformat()

        try:
            freshness = self._table(TABLE_SITEMAP_FRESHNESS)
            sitemap = self._table(TABLE_SITEMAP_URLS)
            clean = self._table(TABLE_CLEAN_BOT_REQUESTS)

            self._backend.execute(f"DELETE FROM {freshness} WHERE 1=1")

            sql = f"""
                INSERT INTO {freshness}
                    (url_path, domain, lastmod, lastmod_month, sitemap_source,
                     first_seen_date, last_seen_date,
                     request_count, unique_urls, unique_bots,
                     days_since_lastmod, _aggregated_at)
                SELECT
                    sm.url_path,
                    sm.domain,
                    sm.lastmod,
                    sm.lastmod_month,
                    sm.sitemap_source,
                    MIN(br.request_date) AS first_seen_date,
                    MAX(br.request_date) AS last_seen_date,
                    COALESCE(COUNT(br.url_path), 0) AS request_count,
                    COUNT(DISTINCT br.request_uri) AS unique_urls,
                    COUNT(DISTINCT br.bot_provider) AS unique_bots,
                    CASE
                        WHEN sm.lastmod IS NOT NULL
                        THEN {self._days_between('sm.lastmod', ref_str)}
                        ELSE NULL
                    END AS days_since_lastmod,
                    {self._current_timestamp()} AS _aggregated_at
                FROM {sitemap} sm
                LEFT JOIN {clean} br
                    ON sm.url_path = br.url_path AND sm.domain = br.domain
                GROUP BY
                    sm.url_path, sm.domain, sm.lastmod, sm.lastmod_month,
                    sm.sitemap_source
            """
            rows = self._backend.execute(sql)

            elapsed = (datetime.now(timezone.utc) - start).total_seconds()

            count_rows = self._backend.query(f"SELECT COUNT(*) AS cnt FROM {freshness}")
            row_count = count_rows[0]["cnt"] if count_rows else 0

            logger.info(
                "Sitemap freshness aggregation: %d rows in %.1fs",
                row_count,
                elapsed,
            )
            return SitemapAggregationResult(
                success=True,
                table_name=TABLE_SITEMAP_FRESHNESS,
                rows_inserted=row_count,
                duration_seconds=elapsed,
            )

        except Exception as e:
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            logger.error("Sitemap freshness aggregation failed: %s", e)
            return SitemapAggregationResult(
                success=False,
                table_name=TABLE_SITEMAP_FRESHNESS,
                error=str(e),
                duration_seconds=elapsed,
            )

    def aggregate_volume_decay(
        self,
        period: str = "monthly",
        lookback_days: int = 365,
    ) -> SitemapAggregationResult:
        """Compute URL volume decay over time periods.

        For each sitemap URL, computes request_count per period (weekly or
        monthly) and the decay_rate relative to the previous period.

        Args:
            period: 'weekly' or 'monthly'.
            lookback_days: How far back to look for request data.

        Returns:
            SitemapAggregationResult with row counts.
        """
        start = datetime.now(timezone.utc)
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
        period_label = "week" if period == "weekly" else "month"

        try:
            decay = self._table(TABLE_URL_VOLUME_DECAY)
            sitemap = self._table(TABLE_SITEMAP_URLS)
            clean = self._table(TABLE_CLEAN_BOT_REQUESTS)

            self._backend.execute(
                f"DELETE FROM {decay} WHERE period = :period",
                {"period": period_label},
            )

            period_expr = self._period_start_expr(period)

            sql = f"""
                INSERT INTO {decay}
                    (url_path, domain, period, period_start,
                     request_count, unique_urls, unique_bots,
                     prev_request_count, decay_rate,
                     _aggregated_at)
                SELECT
                    sub.url_path,
                    sub.domain,
                    :period AS period,
                    sub.period_start,
                    sub.request_count,
                    sub.unique_urls,
                    sub.unique_bots,
                    NULL AS prev_request_count,
                    NULL AS decay_rate,
                    {self._current_timestamp()} AS _aggregated_at
                FROM (
                    SELECT
                        sm.url_path,
                        sm.domain,
                        {period_expr} AS period_start,
                        COUNT(*) AS request_count,
                        COUNT(DISTINCT br.request_uri) AS unique_urls,
                        COUNT(DISTINCT br.bot_provider) AS unique_bots
                    FROM {sitemap} sm
                    INNER JOIN {clean} br
                        ON sm.url_path = br.url_path AND sm.domain = br.domain
                    WHERE br.request_date >= :cutoff
                    GROUP BY sm.url_path, sm.domain, {period_expr}
                ) sub
            """
            self._backend.execute(sql, {"period": period_label, "cutoff": cutoff})

            self._compute_decay_rates(period_label)

            elapsed = (datetime.now(timezone.utc) - start).total_seconds()

            count_rows = self._backend.query(
                f"SELECT COUNT(*) AS cnt FROM {decay}" f" WHERE period = :period",
                {"period": period_label},
            )
            row_count = count_rows[0]["cnt"] if count_rows else 0

            logger.info(
                "URL volume decay (%s): %d rows in %.1fs",
                period_label,
                row_count,
                elapsed,
            )
            return SitemapAggregationResult(
                success=True,
                table_name=TABLE_URL_VOLUME_DECAY,
                rows_inserted=row_count,
                duration_seconds=elapsed,
            )

        except Exception as e:
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            logger.error("URL volume decay aggregation failed: %s", e)
            return SitemapAggregationResult(
                success=False,
                table_name=TABLE_URL_VOLUME_DECAY,
                error=str(e),
                duration_seconds=elapsed,
            )

    def run_all(
        self,
        reference_date: Optional[date] = None,
        lookback_days: int = 365,
    ) -> list[SitemapAggregationResult]:
        """Run all sitemap aggregations: freshness + weekly + monthly decay."""
        results = []
        results.append(self.aggregate_freshness(reference_date))
        results.append(
            self.aggregate_volume_decay(period="weekly", lookback_days=lookback_days)
        )
        results.append(
            self.aggregate_volume_decay(period="monthly", lookback_days=lookback_days)
        )
        return results

    # ------------------------------------------------------------------
    # Looker Studio / dashboard query helpers
    # ------------------------------------------------------------------

    def get_freshness_heatmap(self, domain: Optional[str] = None) -> list[dict]:
        """Freshness heatmap: URL freshness vs crawl frequency.

        Returns rows with url_path, domain, lastmod_month, request_count,
        unique_bots, days_since_lastmod for visualization.
        """
        freshness = self._table(TABLE_SITEMAP_FRESHNESS)
        where = "WHERE request_count > 0"
        params: dict = {}
        if domain:
            where += " AND domain = :domain"
            params["domain"] = domain
        sql = f"""
            SELECT url_path, domain, lastmod_month, request_count,
                   unique_bots, days_since_lastmod
            FROM {freshness}
            {where}
            ORDER BY days_since_lastmod DESC NULLS LAST, request_count DESC
        """
        return self._backend.query(sql, params or None)

    def get_decay_curves(
        self, period: str = "month", domain: Optional[str] = None
    ) -> list[dict]:
        """Decay curves: request volume over time per URL cohort.

        Groups by lastmod_month to show how request volume changes
        as content ages.

        Args:
            period: 'week' or 'month'.
            domain: Optional domain filter.
        """
        decay = self._table(TABLE_URL_VOLUME_DECAY)
        freshness = self._table(TABLE_SITEMAP_FRESHNESS)
        domain_filter = "AND sf.domain = :domain" if domain else ""
        params: dict = {"period": period}
        if domain:
            params["domain"] = domain
        sql = f"""
            SELECT sf.lastmod_month, sf.domain, vd.period_start,
                   SUM(vd.request_count) AS total_requests,
                   COUNT(DISTINCT vd.url_path) AS url_count,
                   AVG(vd.decay_rate) AS avg_decay_rate
            FROM {decay} vd
            INNER JOIN {freshness} sf
                ON vd.url_path = sf.url_path AND vd.domain = sf.domain
            WHERE vd.period = :period
                AND sf.lastmod_month IS NOT NULL
                {domain_filter}
            GROUP BY sf.lastmod_month, sf.domain, vd.period_start
            ORDER BY sf.lastmod_month DESC, vd.period_start
        """
        return self._backend.query(sql, params)

    def get_coverage_gaps(self, domain: Optional[str] = None) -> list[dict]:
        """Coverage gaps: sitemap URLs with zero bot requests."""
        freshness = self._table(TABLE_SITEMAP_FRESHNESS)
        where = "WHERE request_count = 0"
        params: dict = {}
        if domain:
            where += " AND domain = :domain"
            params["domain"] = domain
        sql = f"""
            SELECT url_path, domain, lastmod, lastmod_month,
                   sitemap_source, days_since_lastmod
            FROM {freshness}
            {where}
            ORDER BY days_since_lastmod ASC NULLS LAST
        """
        return self._backend.query(sql, params or None)

    def get_freshness_summary(self, domain: Optional[str] = None) -> list[dict]:
        """Summary statistics for Looker Studio overview.

        Returns per-lastmod_month and per-domain aggregates: total URLs,
        requested URLs, coverage percentage, average days since lastmod.
        """
        freshness = self._table(TABLE_SITEMAP_FRESHNESS)
        where = "WHERE lastmod_month IS NOT NULL"
        params: dict = {}
        if domain:
            where += " AND domain = :domain"
            params["domain"] = domain
        sql = f"""
            SELECT lastmod_month, domain,
                   COUNT(*) AS total_urls,
                   SUM(CASE WHEN request_count > 0 THEN 1 ELSE 0 END) AS requested_urls,
                   ROUND(
                       100.0 * SUM(CASE WHEN request_count > 0 THEN 1 ELSE 0 END)
                       / COUNT(*), 1
                   ) AS coverage_pct,
                   AVG(request_count) AS avg_requests,
                   AVG(days_since_lastmod) AS avg_days_since_lastmod
            FROM {freshness}
            {where}
            GROUP BY lastmod_month, domain
            ORDER BY lastmod_month DESC
        """
        return self._backend.query(sql, params or None)

    # ------------------------------------------------------------------
    # SQL dialect helpers (backend-specific expressions)
    # ------------------------------------------------------------------

    def _days_between(self, date_col: str, ref_date_str: str) -> str:
        """SQL expression for days between a date column and a reference date."""
        backend_type = getattr(self._backend, "backend_type", "sqlite")
        if backend_type == "bigquery":
            return (
                f"DATE_DIFF('{ref_date_str}', PARSE_DATE('%Y-%m-%d', {date_col}), DAY)"
            )
        return f"CAST(julianday('{ref_date_str}') - julianday({date_col}) AS INTEGER)"

    def _current_timestamp(self) -> str:
        backend_type = getattr(self._backend, "backend_type", "sqlite")
        if backend_type == "bigquery":
            return "CURRENT_TIMESTAMP()"
        return "datetime('now')"

    def _period_start_expr(self, period: str) -> str:
        """SQL expression to truncate request_date to period start."""
        backend_type = getattr(self._backend, "backend_type", "sqlite")
        if period == "weekly":
            if backend_type == "bigquery":
                return "DATE_TRUNC(br.request_date, WEEK)"
            return "date(br.request_date, 'weekday 0', '-6 days')"
        # monthly
        if backend_type == "bigquery":
            return "DATE_TRUNC(br.request_date, MONTH)"
        return "date(br.request_date, 'start of month')"

    def _compute_decay_rates(self, period_label: str) -> None:
        """Update prev_request_count and decay_rate via self-join."""
        backend_type = getattr(self._backend, "backend_type", "sqlite")
        decay = self._table(TABLE_URL_VOLUME_DECAY)

        if backend_type == "bigquery":
            sql = f"""
                UPDATE {decay} cur
                SET
                    prev_request_count = prev.request_count,
                    decay_rate = CASE
                        WHEN prev.request_count > 0
                        THEN ROUND(
                            (CAST(cur.request_count AS FLOAT64) - prev.request_count)
                            / prev.request_count, 4
                        )
                        ELSE NULL
                    END
                FROM {decay} prev
                WHERE cur.url_path = prev.url_path
                    AND cur.domain = prev.domain
                    AND cur.period = prev.period
                    AND cur.period = '{period_label}'
                    AND prev.period_start = (
                        SELECT MAX(p2.period_start)
                        FROM {decay} p2
                        WHERE p2.url_path = cur.url_path
                            AND p2.domain = cur.domain
                            AND p2.period = cur.period
                            AND p2.period_start < cur.period_start
                    )
            """
        else:
            bare = TABLE_URL_VOLUME_DECAY
            sql = f"""
                UPDATE {bare}
                SET
                    prev_request_count = (
                        SELECT prev.request_count
                        FROM {bare} prev
                        WHERE prev.url_path = {bare}.url_path
                            AND prev.domain = {bare}.domain
                            AND prev.period = {bare}.period
                            AND prev.period_start < {bare}.period_start
                        ORDER BY prev.period_start DESC
                        LIMIT 1
                    ),
                    decay_rate = CASE
                        WHEN (
                            SELECT prev.request_count
                            FROM {bare} prev
                            WHERE prev.url_path = {bare}.url_path
                                AND prev.domain = {bare}.domain
                                AND prev.period = {bare}.period
                                AND prev.period_start < {bare}.period_start
                            ORDER BY prev.period_start DESC
                            LIMIT 1
                        ) > 0
                        THEN ROUND(
                            (CAST({bare}.request_count AS REAL) - (
                                SELECT prev.request_count
                                FROM {bare} prev
                                WHERE prev.url_path = {bare}.url_path
                                    AND prev.domain = {bare}.domain
                                    AND prev.period = {bare}.period
                                    AND prev.period_start < {bare}.period_start
                                ORDER BY prev.period_start DESC
                                LIMIT 1
                            )) / (
                                SELECT prev.request_count
                                FROM {bare} prev
                                WHERE prev.url_path = {bare}.url_path
                                    AND prev.domain = {bare}.domain
                                    AND prev.period = {bare}.period
                                    AND prev.period_start < {bare}.period_start
                                ORDER BY prev.period_start DESC
                                LIMIT 1
                            ), 4
                        )
                        ELSE NULL
                    END
                WHERE period = :period
            """
        self._backend.execute(
            sql, {"period": period_label} if backend_type != "bigquery" else None
        )
