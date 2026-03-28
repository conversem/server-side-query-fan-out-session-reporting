"""Integration tests for SitemapAggregator with real SQLite backend.

Tests aggregate_freshness(), aggregate_volume_decay(), and run_all()
using a temporary SQLite database populated with sitemap and request data.
"""

from datetime import date, timedelta

import pytest

from llm_bot_pipeline.reporting.sitemap_aggregations import SitemapAggregator
from llm_bot_pipeline.storage import get_backend


@pytest.fixture
def backend(tmp_path):
    """Initialized SQLite backend with tables created."""
    db = tmp_path / "sitemap_test.db"
    backend = get_backend("sqlite", db_path=db)
    backend.initialize()
    yield backend
    backend.close()


@pytest.fixture
def seeded_backend(backend):
    """Backend with sitemap URLs and matching bot request data."""
    sitemap_entries = [
        {
            "url": "https://example.com/page-a",
            "url_path": "/page-a",
            "domain": "example.com",
            "lastmod": "2024-11-15",
            "lastmod_month": "2024-11",
            "sitemap_source": "https://example.com/sitemap.xml",
        },
        {
            "url": "https://example.com/page-b",
            "url_path": "/page-b",
            "domain": "example.com",
            "lastmod": "2025-01-10",
            "lastmod_month": "2025-01",
            "sitemap_source": "https://example.com/sitemap.xml",
        },
        {
            "url": "https://example.com/page-c",
            "url_path": "/page-c",
            "domain": "example.com",
            "lastmod": None,
            "lastmod_month": None,
            "sitemap_source": "https://example.com/sitemap.xml",
        },
    ]
    backend.insert_sitemap_urls(sitemap_entries)

    today = date.today()
    clean_records = []
    for day_offset in range(7):
        d = today - timedelta(days=day_offset + 1)
        for url_path in ["/page-a", "/page-b"]:
            clean_records.append(
                {
                    "request_timestamp": f"{d}T10:00:00",
                    "request_date": d.isoformat(),
                    "request_hour": 10,
                    "day_of_week": "Monday",
                    "request_uri": f"https://example.com{url_path}?q=1",
                    "request_host": "example.com",
                    "domain": "example.com",
                    "url_path": url_path,
                    "url_path_depth": 1,
                    "user_agent_raw": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                    "bot_name": "GPTBot",
                    "bot_provider": "OpenAI",
                    "bot_category": "training",
                    "crawler_country": "US",
                    "response_status": 200,
                    "response_status_category": "2xx_success",
                    "_processed_at": f"{d}T12:00:00",
                }
            )
        # Add a second provider for page-a to test unique_bots
        clean_records.append(
            {
                "request_timestamp": f"{d}T11:00:00",
                "request_date": d.isoformat(),
                "request_hour": 11,
                "day_of_week": "Monday",
                "request_uri": "https://example.com/page-a",
                "request_host": "example.com",
                "domain": "example.com",
                "url_path": "/page-a",
                "url_path_depth": 1,
                "user_agent_raw": "Mozilla/5.0 (compatible; ClaudeBot/1.0)",
                "bot_name": "ClaudeBot",
                "bot_provider": "Anthropic",
                "bot_category": "training",
                "crawler_country": "GB",
                "response_status": 200,
                "response_status_category": "2xx_success",
                "_processed_at": f"{d}T12:00:00",
            }
        )

    backend.insert_clean_records(clean_records)
    return backend


@pytest.fixture
def multi_domain_backend(tmp_path):
    """Backend seeded with two domains sharing the same URL path /page-a."""
    db = tmp_path / "multi_domain.db"
    backend = get_backend("sqlite", db_path=db)
    backend.initialize()

    sitemap_entries = [
        {
            "url": "https://domain-a.com/page-a",
            "url_path": "/page-a",
            "domain": "domain-a.com",
            "lastmod": "2024-11-15",
            "lastmod_month": "2024-11",
            "sitemap_source": "https://domain-a.com/sitemap.xml",
        },
        {
            "url": "https://domain-b.com/page-a",
            "url_path": "/page-a",
            "domain": "domain-b.com",
            "lastmod": "2024-06-01",
            "lastmod_month": "2024-06",
            "sitemap_source": "https://domain-b.com/sitemap.xml",
        },
    ]
    backend.insert_sitemap_urls(sitemap_entries)

    from datetime import date, timedelta

    today = date.today()
    clean_records = []
    for day_offset in range(3):
        d = today - timedelta(days=day_offset + 1)
        for dom in ["domain-a.com", "domain-b.com"]:
            clean_records.append(
                {
                    "request_timestamp": f"{d}T10:00:00",
                    "request_date": d.isoformat(),
                    "request_hour": 10,
                    "day_of_week": "Monday",
                    "request_uri": f"https://{dom}/page-a",
                    "request_host": dom,
                    "domain": dom,
                    "url_path": "/page-a",
                    "url_path_depth": 1,
                    "user_agent_raw": "GPTBot/1.0",
                    "bot_name": "GPTBot",
                    "bot_provider": "OpenAI",
                    "bot_category": "training",
                    "crawler_country": "US",
                    "response_status": 200,
                    "response_status_category": "2xx_success",
                    "_processed_at": f"{d}T12:00:00",
                }
            )
    backend.insert_clean_records(clean_records)
    yield backend
    backend.close()


class TestMultiDomainFreshness:
    def test_freshness_produces_two_rows_for_shared_path(self, multi_domain_backend):
        """Two domains with same url_path must produce two separate freshness rows."""
        agg = SitemapAggregator(multi_domain_backend)
        agg.aggregate_freshness(reference_date=date.today())

        rows = multi_domain_backend.query(
            "SELECT domain, url_path, lastmod_month FROM sitemap_freshness WHERE url_path = '/page-a'"
        )
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}: {rows}"
        domains = {r["domain"] for r in rows}
        assert domains == {"domain-a.com", "domain-b.com"}

    def test_freshness_lastmod_preserved_per_domain(self, multi_domain_backend):
        """Each domain retains its own lastmod — domain-b.com has older lastmod."""
        agg = SitemapAggregator(multi_domain_backend)
        agg.aggregate_freshness(reference_date=date.today())

        rows = {
            r["domain"]: r
            for r in multi_domain_backend.query(
                "SELECT domain, lastmod_month FROM sitemap_freshness WHERE url_path = '/page-a'"
            )
        }
        assert rows["domain-a.com"]["lastmod_month"] == "2024-11"
        assert rows["domain-b.com"]["lastmod_month"] == "2024-06"


class TestMultiDomainDecay:
    def test_decay_produces_two_rows_for_shared_path(self, multi_domain_backend):
        agg = SitemapAggregator(multi_domain_backend)
        agg.aggregate_volume_decay(period="monthly")

        rows = multi_domain_backend.query(
            "SELECT domain FROM url_volume_decay WHERE url_path = '/page-a'"
        )
        assert len(rows) == 2
        domains = {r["domain"] for r in rows}
        assert domains == {"domain-a.com", "domain-b.com"}

    def test_decay_rates_do_not_cross_contaminate(self, multi_domain_backend):
        """prev_request_count for domain-a must not use domain-b's request_count."""
        multi_domain_backend.execute("""
            INSERT OR IGNORE INTO url_volume_decay
                (url_path, domain, period, period_start, request_count, unique_urls, unique_bots)
            VALUES
                ('/page-a', 'domain-a.com', 'month', '2024-01-01', 10, 1, 1),
                ('/page-a', 'domain-b.com', 'month', '2024-01-01', 3, 1, 1),
                ('/page-a', 'domain-a.com', 'month', '2024-02-01', 6, 1, 1),
                ('/page-a', 'domain-b.com', 'month', '2024-02-01', 9, 1, 1)
        """)

        agg = SitemapAggregator(multi_domain_backend)
        agg._compute_decay_rates("month")

        rows = {
            r["domain"]: r
            for r in multi_domain_backend.query(
                "SELECT domain, period_start, request_count, prev_request_count "
                "FROM url_volume_decay WHERE url_path = '/page-a' AND period_start = '2024-02-01'"
            )
        }
        # domain-a: went from 10 → 6; prev must be 10, not domain-b's 3
        assert rows["domain-a.com"]["prev_request_count"] == 10
        # domain-b: went from 3 → 9; prev must be 3, not domain-a's 10
        assert rows["domain-b.com"]["prev_request_count"] == 3


class TestAggregateFreshness:
    def test_produces_rows(self, seeded_backend):
        agg = SitemapAggregator(seeded_backend)
        result = agg.aggregate_freshness(reference_date=date.today())

        assert result.success is True
        assert result.table_name == "sitemap_freshness"
        assert result.rows_inserted >= 2

    def test_freshness_row_content(self, seeded_backend):
        agg = SitemapAggregator(seeded_backend)
        agg.aggregate_freshness(reference_date=date.today())

        rows = seeded_backend.query(
            "SELECT * FROM sitemap_freshness WHERE url_path = '/page-a'"
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["request_count"] >= 7
        assert row["unique_bots"] == 2
        assert row["unique_urls"] >= 1
        assert row["days_since_lastmod"] is not None

    def test_page_c_has_no_requests(self, seeded_backend):
        agg = SitemapAggregator(seeded_backend)
        agg.aggregate_freshness(reference_date=date.today())

        rows = seeded_backend.query(
            "SELECT * FROM sitemap_freshness WHERE url_path = '/page-c'"
        )
        assert len(rows) == 1
        assert rows[0]["request_count"] == 0
        assert rows[0]["days_since_lastmod"] is None

    def test_is_idempotent(self, seeded_backend):
        agg = SitemapAggregator(seeded_backend)
        r1 = agg.aggregate_freshness(reference_date=date.today())
        r2 = agg.aggregate_freshness(reference_date=date.today())
        assert r1.rows_inserted == r2.rows_inserted


class TestAggregateVolumeDecay:
    def test_monthly_produces_rows(self, seeded_backend):
        agg = SitemapAggregator(seeded_backend)
        result = agg.aggregate_volume_decay(period="monthly")

        assert result.success is True
        assert result.table_name == "url_volume_decay"
        assert result.rows_inserted >= 1

    def test_weekly_produces_rows(self, seeded_backend):
        agg = SitemapAggregator(seeded_backend)
        result = agg.aggregate_volume_decay(period="weekly")

        assert result.success is True
        assert result.rows_inserted >= 1

    def test_decay_row_content(self, seeded_backend):
        agg = SitemapAggregator(seeded_backend)
        agg.aggregate_volume_decay(period="monthly")

        rows = seeded_backend.query(
            "SELECT * FROM url_volume_decay WHERE url_path = '/page-a' LIMIT 1"
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["period"] == "month"
        assert row["request_count"] > 0
        assert row["unique_urls"] >= 1

    def test_is_idempotent(self, seeded_backend):
        agg = SitemapAggregator(seeded_backend)
        r1 = agg.aggregate_volume_decay(period="monthly")
        r2 = agg.aggregate_volume_decay(period="monthly")
        assert r1.rows_inserted == r2.rows_inserted


class TestRunAll:
    def test_run_all_returns_three_results(self, seeded_backend):
        agg = SitemapAggregator(seeded_backend)
        results = agg.run_all()

        assert len(results) == 3
        assert all(r.success for r in results)
        tables = {r.table_name for r in results}
        assert "sitemap_freshness" in tables
        assert "url_volume_decay" in tables


class TestEmptyData:
    def test_freshness_on_empty_sitemap(self, backend):
        agg = SitemapAggregator(backend)
        result = agg.aggregate_freshness()
        assert result.success is True
        assert result.rows_inserted == 0

    def test_volume_decay_on_empty_sitemap(self, backend):
        agg = SitemapAggregator(backend)
        result = agg.aggregate_volume_decay()
        assert result.success is True
        assert result.rows_inserted == 0
