"""Integration tests for migration scripts."""

import sqlite3
from pathlib import Path

import pytest

from llm_bot_pipeline.storage import get_backend


@pytest.fixture
def v211_db(tmp_path):
    """Simulate a v2.1.1 database: sitemap tables without domain column."""
    db_path = tmp_path / "v211.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE sitemap_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            url_path TEXT NOT NULL,
            lastmod TEXT,
            lastmod_month TEXT,
            sitemap_source TEXT NOT NULL,
            _fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(url_path)
        );
        INSERT INTO sitemap_urls (url, url_path, sitemap_source)
        VALUES
            ('/a', '/a', 'https://example.com/sitemap.xml'),
            ('/b', '/b', 'https://www.other.com/sitemap.xml');

        CREATE TABLE sitemap_freshness (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_path TEXT NOT NULL,
            lastmod TEXT,
            lastmod_month TEXT,
            sitemap_source TEXT NOT NULL,
            request_count INTEGER NOT NULL DEFAULT 0,
            _aggregated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(url_path)
        );
        INSERT INTO sitemap_freshness (url_path, sitemap_source)
        VALUES ('/a', 'https://example.com/sitemap.xml');

        CREATE TABLE url_volume_decay (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_path TEXT NOT NULL,
            period TEXT NOT NULL,
            period_start TEXT NOT NULL,
            request_count INTEGER NOT NULL DEFAULT 0,
            _aggregated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(url_path, period, period_start)
        );
        CREATE TABLE url_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT,
            request_date TEXT NOT NULL,
            url_path TEXT NOT NULL,
            request_host TEXT NOT NULL,
            total_bot_requests INTEGER NOT NULL DEFAULT 0,
            unique_bot_providers INTEGER NOT NULL DEFAULT 0,
            unique_bot_names INTEGER NOT NULL DEFAULT 0,
            training_hits INTEGER NOT NULL DEFAULT 0,
            user_request_hits INTEGER NOT NULL DEFAULT 0,
            successful_requests INTEGER NOT NULL DEFAULT 0,
            error_requests INTEGER NOT NULL DEFAULT 0,
            first_seen TEXT NOT NULL DEFAULT '',
            last_seen TEXT NOT NULL DEFAULT '',
            _aggregated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.close()
    return db_path


class TestMigrateAddDomainToSitemapTables:
    def test_domain_column_added_to_sitemap_urls(self, v211_db):
        from scripts.migrations.migrate_add_domain_to_sitemap_tables import (
            migrate_sqlite,
        )

        migrate_sqlite(v211_db)

        conn = sqlite3.connect(v211_db)
        cols = [
            r[1] for r in conn.execute("PRAGMA table_info(sitemap_urls)").fetchall()
        ]
        conn.close()
        assert "domain" in cols

    def test_domain_backfilled_from_sitemap_source(self, v211_db):
        from scripts.migrations.migrate_add_domain_to_sitemap_tables import (
            migrate_sqlite,
        )

        migrate_sqlite(v211_db)

        conn = sqlite3.connect(v211_db)
        rows = conn.execute(
            "SELECT url_path, domain FROM sitemap_urls ORDER BY url_path"
        ).fetchall()
        conn.close()
        assert rows[0] == ("/a", "example.com")
        assert rows[1] == ("/b", "other.com")  # www. stripped

    def test_domain_added_to_sitemap_freshness_and_backfilled(self, v211_db):
        from scripts.migrations.migrate_add_domain_to_sitemap_tables import (
            migrate_sqlite,
        )

        migrate_sqlite(v211_db)

        conn = sqlite3.connect(v211_db)
        cols = [
            r[1]
            for r in conn.execute("PRAGMA table_info(sitemap_freshness)").fetchall()
        ]
        rows = conn.execute("SELECT domain FROM sitemap_freshness").fetchall()
        conn.close()
        assert "domain" in cols
        assert rows[0][0] == "example.com"

    def test_domain_added_to_url_volume_decay_no_backfill(self, v211_db):
        from scripts.migrations.migrate_add_domain_to_sitemap_tables import (
            migrate_sqlite,
        )

        migrate_sqlite(v211_db)

        conn = sqlite3.connect(v211_db)
        cols = [
            r[1] for r in conn.execute("PRAGMA table_info(url_volume_decay)").fetchall()
        ]
        conn.close()
        assert "domain" in cols

    def test_idempotent(self, v211_db):
        from scripts.migrations.migrate_add_domain_to_sitemap_tables import (
            migrate_sqlite,
        )

        migrate_sqlite(v211_db)
        migrate_sqlite(v211_db)  # Should not raise


class TestMigrateFixUrlPerformanceUniqueKey:
    def test_unique_index_created(self, v211_db):
        from scripts.migrations.migrate_fix_url_performance_unique_key import (
            migrate_sqlite,
        )

        migrate_sqlite(v211_db)

        conn = sqlite3.connect(v211_db)
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_url_performance_natural_key'"
        ).fetchall()
        conn.close()
        assert len(indexes) == 1

    def test_idempotent(self, v211_db):
        from scripts.migrations.migrate_fix_url_performance_unique_key import (
            migrate_sqlite,
        )

        migrate_sqlite(v211_db)
        migrate_sqlite(v211_db)  # Should not raise
