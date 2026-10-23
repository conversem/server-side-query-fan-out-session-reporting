"""Unit tests for BigQuery view SQL string correctness."""

import pytest

from llm_bot_pipeline.storage.bigquery_views import VIEW_DEFINITIONS

DS = "project.dataset"


def _sql(view_name: str) -> str:
    for name, template in VIEW_DEFINITIONS:
        if name == view_name:
            return template.format(ds=DS)
    raise ValueError(f"View not found: {view_name}")


class TestBigQueryViewsDomain:
    def test_v_session_url_distribution_has_domain(self):
        assert "domain" in _sql("v_session_url_distribution")

    def test_v_session_singleton_binary_has_domain(self):
        assert "domain" in _sql("v_session_singleton_binary")

    def test_v_bot_volume_has_domain(self):
        assert "domain" in _sql("v_bot_volume")

    def test_v_top_session_topics_has_domain(self):
        assert "domain" in _sql("v_top_session_topics")

    def test_v_daily_kpis_has_domain(self):
        assert "domain" in _sql("v_daily_kpis")

    def test_v_category_comparison_has_domain_in_all_legs(self):
        sql = _sql("v_category_comparison")
        assert sql.count("domain") >= 3

    def test_v_url_freshness_join_domain_scoped(self):
        sql = _sql("v_url_freshness")
        assert "sud.domain = sm.domain" in sql

    def test_v_decay_unique_urls_join_domain_scoped(self):
        sql = _sql("v_decay_unique_urls")
        assert "sud.domain = sm.domain" in sql

    def test_v_decay_request_volume_join_domain_scoped(self):
        sql = _sql("v_decay_request_volume")
        assert "sud.domain = sm.domain" in sql


class TestBigQueryViewsFullUrl:
    def test_v_url_cooccurrence_has_full_url(self):
        sql = _sql("v_url_cooccurrence")
        assert "full_url" in sql
        assert "CONCAT('https://', domain, url)" in sql


class TestBigQueryViewRegistry:
    def test_registry_has_10_views(self):
        assert len(VIEW_DEFINITIONS) >= 10

    def test_all_view_names_unique(self):
        names = [name for name, _ in VIEW_DEFINITIONS]
        assert len(names) == len(set(names))


class TestNewBigQueryViews:
    def test_registry_has_16_views(self):
        assert len(VIEW_DEFINITIONS) == 16

    def test_v_decay_unique_urls_by_domain_exists(self):
        sql = _sql("v_decay_unique_urls_by_domain")
        assert "domain" in sql

    def test_v_url_freshness_detail_has_full_url(self):
        sql = _sql("v_url_freshness_detail")
        assert "full_url" in sql
        assert "CONCAT('https://', " in sql

    def test_v_sessions_by_content_age_joins_correctly(self):
        sql = _sql("v_sessions_by_content_age")
        assert "query_fanout_sessions" in sql
        assert "sitemap_urls" in sql

    def test_v_url_performance_with_freshness_is_left_join(self):
        sql = _sql("v_url_performance_with_freshness")
        assert "LEFT JOIN" in sql


class TestSitemapDedup:
    """All four views that JOIN sitemap_urls must use a dedup_sitemap CTE."""

    def test_v_url_freshness_has_dedup_cte(self):
        sql = _sql("v_url_freshness")
        assert "dedup_sitemap" in sql, "v_url_freshness must use dedup_sitemap CTE"

    def test_v_url_freshness_sitemap_urls_appears_only_in_cte(self):
        sql = _sql("v_url_freshness")
        # The raw sitemap_urls table must appear exactly once: inside the dedup_sitemap CTE.
        # Count full table-reference token (with dot prefix) to avoid matching column aliases
        # like "total_sitemap_urls".
        assert (
            sql.count(".sitemap_urls`") == 1
        ), "sitemap_urls table must appear exactly once (inside dedup_sitemap CTE only)"

    def test_v_url_freshness_detail_has_dedup_cte(self):
        sql = _sql("v_url_freshness_detail")
        assert (
            "dedup_sitemap" in sql
        ), "v_url_freshness_detail must use dedup_sitemap CTE"

    def test_v_sessions_by_content_age_has_dedup_cte(self):
        sql = _sql("v_sessions_by_content_age")
        assert (
            "dedup_sitemap" in sql
        ), "v_sessions_by_content_age must use dedup_sitemap CTE"

    def test_v_url_performance_with_freshness_has_dedup_cte(self):
        sql = _sql("v_url_performance_with_freshness")
        assert (
            "dedup_sitemap" in sql
        ), "v_url_performance_with_freshness must use dedup_sitemap CTE"

    def test_v_url_freshness_sitemap_totals_uses_dedup(self):
        sql = _sql("v_url_freshness")
        # sitemap_totals must select FROM dedup_sitemap, not FROM sitemap_urls
        assert "FROM dedup_sitemap" in sql

    def test_v_url_freshness_detail_uses_distinct_in_cte(self):
        sql = _sql("v_url_freshness_detail")
        assert "SELECT DISTINCT" in sql

    def test_v_url_freshness_summary_has_dedup_cte(self):
        sql = _sql("v_url_freshness_summary")
        assert (
            "dedup_sitemap" in sql
        ), "v_url_freshness_summary must use dedup_sitemap CTE"

    def test_v_url_freshness_summary_no_session_date(self):
        sql = _sql("v_url_freshness_summary")
        # session_date is allowed in filter predicates (last-30d window)
        # but must never be an output column.
        assert (
            "AS session_date" not in sql
        ), "v_url_freshness_summary must not expose session_date as an output column"

    def test_v_url_freshness_summary_has_domain(self):
        sql = _sql("v_url_freshness_summary")
        assert "domain" in sql, "v_url_freshness_summary must include domain column"

    def test_v_url_freshness_summary_uses_count_distinct(self):
        sql = _sql("v_url_freshness_summary")
        assert (
            "COUNT(DISTINCT" in sql
        ), "v_url_freshness_summary must use COUNT(DISTINCT) for dedup-safe URL counts"

    def test_v_url_freshness_summary_has_url_requests(self):
        sql = _sql("v_url_freshness_summary")
        assert (
            "url_requests" in sql
        ), "v_url_freshness_summary must include url_requests column"

    def test_v_url_freshness_summary_has_pct_of_total_requests(self):
        sql = _sql("v_url_freshness_summary")
        assert (
            "pct_of_total_requests" in sql
        ), "v_url_freshness_summary must include pct_of_total_requests column"

    def test_v_url_freshness_summary_has_30day_filter(self):
        sql = _sql("v_url_freshness_summary")
        assert "INTERVAL 30 DAY" in sql, "must have 30-day interval"
        assert "DATE_SUB" in sql, "must use DATE_SUB for rolling window"
        assert "CURRENT_DATE" in sql, "must use CURRENT_DATE for dynamic window"

    def test_v_url_freshness_summary_uses_safe_divide(self):
        sql = _sql("v_url_freshness_summary")
        assert (
            "SAFE_DIVIDE" in sql
        ), "v_url_freshness_summary must use SAFE_DIVIDE for pct_of_total_requests"
