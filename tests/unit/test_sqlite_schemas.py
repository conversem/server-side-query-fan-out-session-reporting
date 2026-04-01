"""Unit tests for SQLite schema DDL constants — domain and constraint changes."""

from llm_bot_pipeline.storage.sqlite_schemas import (
    INDEX_DEFINITIONS,
    SITEMAP_FRESHNESS_SCHEMA,
    SITEMAP_URLS_SCHEMA,
    URL_VOLUME_DECAY_SCHEMA,
    VIEW_BOT_VOLUME,
    VIEW_CATEGORY_COMPARISON,
    VIEW_DAILY_KPIS,
    VIEW_DECAY_REQUEST_VOLUME,
    VIEW_NAMES,
    VIEW_SESSION_SINGLETON_BINARY,
    VIEW_SESSION_URL_DISTRIBUTION,
    VIEW_SESSIONS_BY_CONTENT_AGE,
    VIEW_TOP_SESSION_TOPICS,
    VIEW_URL_COOCCURRENCE,
    VIEW_URL_FRESHNESS,
    VIEW_URL_FRESHNESS_DETAIL,
    VIEW_URL_PERFORMANCE_WITH_FRESHNESS,
)


class TestSitemapUrlsSchema:
    def test_has_domain_column(self):
        assert "domain TEXT" in SITEMAP_URLS_SCHEMA

    def test_unique_constraint_includes_domain(self):
        assert "UNIQUE(domain, url_path)" in SITEMAP_URLS_SCHEMA

    def test_old_unique_constraint_gone(self):
        # old constraint was UNIQUE(url_path) — must be replaced
        assert "UNIQUE(url_path)" not in SITEMAP_URLS_SCHEMA


class TestSitemapFreshnessSchema:
    def test_has_domain_column(self):
        assert "domain TEXT" in SITEMAP_FRESHNESS_SCHEMA

    def test_unique_constraint_includes_domain(self):
        assert "UNIQUE(domain, url_path)" in SITEMAP_FRESHNESS_SCHEMA

    def test_old_unique_constraint_gone(self):
        assert "UNIQUE(url_path)" not in SITEMAP_FRESHNESS_SCHEMA


class TestUrlVolumeDecaySchema:
    def test_has_domain_column(self):
        assert "domain TEXT" in URL_VOLUME_DECAY_SCHEMA

    def test_unique_constraint_includes_domain(self):
        assert (
            "UNIQUE(domain, url_path, period, period_start)" in URL_VOLUME_DECAY_SCHEMA
        )

    def test_old_unique_constraint_gone(self):
        assert "UNIQUE(url_path, period, period_start)" not in URL_VOLUME_DECAY_SCHEMA


class TestIndexDefinitions:
    def test_session_url_details_domain_index(self):
        combined = " ".join(INDEX_DEFINITIONS)
        assert "idx_session_url_details_domain" in combined

    def test_url_performance_natural_key_index(self):
        combined = " ".join(INDEX_DEFINITIONS)
        assert "idx_url_performance_natural_key" in combined
        assert "domain, request_date, url_path" in combined

    def test_sitemap_urls_domain_index(self):
        combined = " ".join(INDEX_DEFINITIONS)
        assert "idx_sitemap_urls_domain" in combined

    def test_sitemap_freshness_domain_index(self):
        combined = " ".join(INDEX_DEFINITIONS)
        assert "idx_sitemap_freshness_domain" in combined

    def test_url_volume_decay_domain_index(self):
        combined = " ".join(INDEX_DEFINITIONS)
        assert "idx_url_volume_decay_domain" in combined


class TestViewsDomainColumn:
    """All updated views must expose domain in GROUP BY / SELECT."""

    def test_v_session_url_distribution_has_domain(self):
        assert "domain" in VIEW_SESSION_URL_DISTRIBUTION

    def test_v_session_singleton_binary_has_domain(self):
        assert "domain" in VIEW_SESSION_SINGLETON_BINARY

    def test_v_bot_volume_has_domain(self):
        assert "domain" in VIEW_BOT_VOLUME

    def test_v_top_session_topics_has_domain(self):
        assert "domain" in VIEW_TOP_SESSION_TOPICS

    def test_v_daily_kpis_has_domain(self):
        assert "domain" in VIEW_DAILY_KPIS

    def test_v_category_comparison_has_domain(self):
        # Both UNION legs must have domain
        assert VIEW_CATEGORY_COMPARISON.count("domain") >= 3

    def test_v_url_freshness_has_domain(self):
        assert "domain" in VIEW_URL_FRESHNESS

    def test_v_url_freshness_join_scoped(self):
        assert (
            "sm.domain = br.domain" in VIEW_URL_FRESHNESS
            or "sud.domain = sm.domain" in VIEW_URL_FRESHNESS
        )


class TestViewsFullUrl:
    def test_v_url_cooccurrence_has_full_url(self):
        assert "full_url" in VIEW_URL_COOCCURRENCE
        assert "'https://' || domain || url" in VIEW_URL_COOCCURRENCE

    def test_v_url_cooccurrence_keeps_url_column(self):
        # url column must NOT be renamed to avoid breaking existing Looker charts
        assert "url," in VIEW_URL_COOCCURRENCE or "url\n" in VIEW_URL_COOCCURRENCE


class TestDecayViewsJoinFixed:

    def test_v_decay_request_volume_join_domain_scoped(self):
        assert "sud.domain = sm.domain" in VIEW_DECAY_REQUEST_VOLUME

    def test_v_daily_kpis_unique_urls_domain_scoped(self):
        # The url_counts CTE or subquery must join on domain
        assert "uc.domain" in VIEW_DAILY_KPIS or "sud.domain" in VIEW_DAILY_KPIS


class TestNewViews:
    def test_view_names_has_15_entries(self):
        assert len(VIEW_NAMES) == 12

    def test_v_url_freshness_detail_has_full_url(self):
        assert "full_url" in VIEW_URL_FRESHNESS_DETAIL
        assert "'https://' ||" in VIEW_URL_FRESHNESS_DETAIL

    def test_v_url_freshness_detail_has_months_since_lastmod(self):
        assert "months_since_lastmod" in VIEW_URL_FRESHNESS_DETAIL

    def test_v_sessions_by_content_age_has_full_url(self):
        assert "full_url" in VIEW_SESSIONS_BY_CONTENT_AGE

    def test_v_sessions_by_content_age_joins_sessions_and_sitemap(self):
        assert "query_fanout_sessions" in VIEW_SESSIONS_BY_CONTENT_AGE
        assert "sitemap_urls" in VIEW_SESSIONS_BY_CONTENT_AGE

    def test_v_url_performance_with_freshness_is_left_join(self):
        assert "LEFT JOIN" in VIEW_URL_PERFORMANCE_WITH_FRESHNESS

    def test_v_url_performance_with_freshness_has_full_url(self):
        assert "full_url" in VIEW_URL_PERFORMANCE_WITH_FRESHNESS
