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
            "lastmod": "2024-11-15",
            "lastmod_month": "2024-11",
            "sitemap_source": "https://example.com/sitemap.xml",
        },
        {
            "url": "https://example.com/page-b",
            "url_path": "/page-b",
            "lastmod": "2025-01-10",
            "lastmod_month": "2025-01",
            "sitemap_source": "https://example.com/sitemap.xml",
        },
        {
            "url": "https://example.com/page-c",
            "url_path": "/page-c",
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
                    "url_path": url_path,
                    "url_path_depth": 1,
                    "user_agent_raw": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                    "bot_name": "GPTBot",
                    "bot_provider": "OpenAI",
                    "bot_category": "training",
                    "bot_score": 10,
                    "is_verified_bot": 1,
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
                "url_path": "/page-a",
                "url_path_depth": 1,
                "user_agent_raw": "Mozilla/5.0 (compatible; ClaudeBot/1.0)",
                "bot_name": "ClaudeBot",
                "bot_provider": "Anthropic",
                "bot_category": "training",
                "bot_score": 15,
                "is_verified_bot": 1,
                "crawler_country": "GB",
                "response_status": 200,
                "response_status_category": "2xx_success",
                "_processed_at": f"{d}T12:00:00",
            }
        )

    backend.insert_clean_records(clean_records)
    return backend


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
