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
    def test_registry_has_15_views(self):
        assert len(VIEW_DEFINITIONS) == 15

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
