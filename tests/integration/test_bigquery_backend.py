"""Integration tests for BigQueryBackend with mocked google.cloud.bigquery.Client.

Verifies that all BigQueryBackend methods call the client correctly
without requiring real GCP credentials or network access.
"""

from datetime import date
from unittest.mock import MagicMock

import pytest

from llm_bot_pipeline.storage.bigquery_backend import BigQueryBackend


@pytest.fixture
def mock_client():
    """Create a mock BigQuery client."""
    client = MagicMock()
    client.project = "test-project"
    return client


@pytest.fixture
def bq_backend(mock_client):
    """Create a BigQueryBackend with a pre-injected mock client."""
    backend = BigQueryBackend(
        project_id="test-project",
        dataset_raw="raw_logs",
        dataset_report="reports",
        location="EU",
    )
    backend._client = mock_client
    return backend


class TestBackendProperties:
    def test_backend_type(self, bq_backend):
        assert bq_backend.backend_type == "bigquery"

    def test_capabilities(self, bq_backend):
        caps = bq_backend.capabilities
        assert caps.supports_sql is True
        assert caps.supports_streaming is True
        assert caps.supports_partitioning is True
        assert caps.supports_transactions is False
        assert caps.parameter_style == "at"


class TestTableIdResolution:
    def test_raw_table_resolution(self, bq_backend):
        tid = bq_backend.get_full_table_id("raw_bot_requests")
        assert tid == "test-project.raw_logs.raw_bot_requests"

    def test_report_table_resolution(self, bq_backend):
        tid = bq_backend.get_full_table_id("bot_requests_daily")
        assert tid == "test-project.reports.bot_requests_daily"

    def test_dotted_name_two_parts(self, bq_backend):
        tid = bq_backend.get_full_table_id("custom_ds.my_table")
        assert tid == "test-project.custom_ds.my_table"

    def test_dotted_name_three_parts(self, bq_backend):
        tid = bq_backend.get_full_table_id("other-project.ds.tbl")
        assert tid == "other-project.ds.tbl"

    def test_unknown_table_falls_back_to_report_dataset(self, bq_backend):
        tid = bq_backend.get_full_table_id("some_unknown_table")
        assert tid == "test-project.reports.some_unknown_table"


class TestQuery:
    def test_query_without_params(self, bq_backend, mock_client):
        mock_client.query.return_value.result.return_value = [{"col1": 42}]

        results = bq_backend.query("SELECT 1 AS col1")
        mock_client.query.assert_called_once()
        assert len(results) == 1

    def test_query_converts_sqlite_params(self, bq_backend, mock_client):
        mock_client.query.return_value.result.return_value = []

        bq_backend.query(
            "SELECT * FROM t WHERE x = :val",
            {"val": "test"},
        )
        call_args = mock_client.query.call_args
        sql_arg = call_args[0][0]
        assert "@val" in sql_arg
        assert ":val" not in sql_arg


class TestExecute:
    def test_execute_returns_affected_rows(self, bq_backend, mock_client):
        mock_client.query.return_value.result.return_value = None
        mock_client.query.return_value.num_dml_affected_rows = 5

        count = bq_backend.execute("DELETE FROM `t` WHERE id = @id", {"id": 1})
        assert count == 5

    def test_execute_returns_zero_for_ddl(self, bq_backend, mock_client):
        mock_client.query.return_value.result.return_value = None
        mock_client.query.return_value.num_dml_affected_rows = None

        count = bq_backend.execute("CREATE TABLE t (id INT)")
        assert count == 0


class TestTableExists:
    def test_table_exists_true(self, bq_backend, mock_client):
        mock_client.get_table.return_value = MagicMock()
        assert bq_backend.table_exists("bot_requests_daily") is True

    def test_table_exists_false_on_not_found(self, bq_backend, mock_client):
        from google.api_core.exceptions import NotFound

        mock_client.get_table.side_effect = NotFound("not found")
        assert bq_backend.table_exists("bot_requests_daily") is False


class TestGetTableRowCount:
    def test_returns_num_rows(self, bq_backend, mock_client):
        mock_table = MagicMock()
        mock_table.num_rows = 1000
        mock_client.get_table.return_value = mock_table

        assert bq_backend.get_table_row_count("bot_requests_daily") == 1000


class TestInsertRawRecords:
    def test_insert_raw_records_streaming(self, bq_backend, mock_client):
        mock_client.insert_rows_json.return_value = []

        records = [
            {"EdgeStartTimestamp": "2025-01-01T00:00:00Z", "ClientIP": "1.2.3.4"},
            {"EdgeStartTimestamp": "2025-01-01T00:01:00Z", "ClientIP": "1.2.3.5"},
            {"EdgeStartTimestamp": "2025-01-01T00:02:00Z", "ClientIP": "1.2.3.6"},
        ]
        count = bq_backend.insert_raw_records(records)
        mock_client.insert_rows_json.assert_called_once()
        assert count == 3

    def test_insert_raw_records_error(self, bq_backend, mock_client):
        mock_client.insert_rows_json.return_value = [{"errors": ["bad data"]}]

        from llm_bot_pipeline.storage.base import QueryError

        with pytest.raises(QueryError, match="streaming insert failed"):
            bq_backend.insert_raw_records([{"EdgeStartTimestamp": "bad"}])


class TestDeleteDateRange:
    def test_validates_table_name(self, bq_backend):
        with pytest.raises(ValueError, match="Invalid table name"):
            bq_backend.delete_date_range(
                "evil_table", "request_date", date(2025, 1, 1), date(2025, 1, 31)
            )

    def test_validates_date_column(self, bq_backend):
        with pytest.raises(ValueError, match="Invalid date column"):
            bq_backend.delete_date_range(
                "bot_requests_daily", "bad_col", date(2025, 1, 1), date(2025, 1, 31)
            )

    def test_executes_delete(self, bq_backend, mock_client):
        mock_client.query.return_value.result.return_value = None
        mock_client.query.return_value.num_dml_affected_rows = 10

        count = bq_backend.delete_date_range(
            "bot_requests_daily", "request_date", date(2025, 1, 1), date(2025, 1, 31)
        )
        assert count == 10
        call_sql = mock_client.query.call_args[0][0]
        assert "DELETE FROM" in call_sql


class TestGetDateRangeCount:
    def test_validates_table_name(self, bq_backend):
        with pytest.raises(ValueError, match="Invalid table name"):
            bq_backend.get_date_range_count(
                "evil_table", "request_date", date(2025, 1, 1), date(2025, 1, 31)
            )

    def test_returns_count(self, bq_backend, mock_client):
        mock_client.query.return_value.result.return_value = [{"count": 42}]

        count = bq_backend.get_date_range_count(
            "bot_requests_daily", "request_date", date(2025, 1, 1), date(2025, 1, 31)
        )
        assert count == 42


class TestClose:
    def test_close_clears_client(self, bq_backend, mock_client):
        bq_backend.close()
        mock_client.close.assert_called_once()
        assert bq_backend._client is None

    def test_close_idempotent(self, bq_backend):
        bq_backend._client = None
        bq_backend.close()


class TestHealthCheck:
    def test_healthy(self, bq_backend, mock_client):
        mock_client.query.return_value.result.return_value = [{"test": 1}]
        result = bq_backend.health_check()
        assert result["healthy"] is True
        assert result["backend_type"] == "bigquery"


class TestInsertSitemapUrls:
    def test_delete_issued_before_insert_for_domain(self, bq_backend, mock_client):
        """insert_sitemap_urls must DELETE existing rows for the domain before inserting."""
        entries = [
            {
                "url": "https://example.com/page1",
                "url_path": "/page1",
                "domain": "example.com",
                "lastmod": "2026-03-01",
                "lastmod_month": "2026-03",
                "sitemap_source": "https://example.com/sitemap.xml",
            },
            {
                "url": "https://example.com/page2",
                "url_path": "/page2",
                "domain": "example.com",
                "lastmod": "2026-02-15",
                "lastmod_month": "2026-02",
                "sitemap_source": "https://example.com/sitemap.xml",
            },
        ]

        # Track call order
        call_log = []
        mock_client.query.side_effect = lambda sql, **kw: (
            call_log.append(("DELETE", sql)) or MagicMock()
        )
        mock_client.load_table_from_json.return_value = MagicMock()
        mock_client.load_table_from_json.return_value.result.return_value = None

        bq_backend.insert_sitemap_urls(entries)

        # DELETE must be called
        delete_calls = [c for c in call_log if c[0] == "DELETE"]
        assert len(delete_calls) >= 1, "Expected at least one DELETE call"

        # DELETE must be domain-scoped
        delete_sql = delete_calls[0][1]
        assert "example.com" in delete_sql
        assert "sitemap_urls" in delete_sql

        # DELETE must happen before load_table_from_json (the insert)
        # mock_client.query must be called before load_table_from_json
        query_order = [str(c) for c in mock_client.method_calls]
        query_idx = next(i for i, c in enumerate(query_order) if "query" in c)
        load_idx = next(
            i for i, c in enumerate(query_order) if "load_table_from_json" in c
        )
        assert (
            query_idx < load_idx
        ), "DELETE (query) must be called before load_table_from_json (insert)"

    def test_empty_entries_skips_delete_and_insert(self, bq_backend, mock_client):
        """insert_sitemap_urls with empty list must not call DELETE or INSERT."""
        result = bq_backend.insert_sitemap_urls([])
        assert result == 0
        mock_client.query.assert_not_called()
        mock_client.load_table_from_json.assert_not_called()

    def test_multi_domain_delete_scoped_per_domain(self, bq_backend, mock_client):
        """DELETE must only target domains present in the entries batch."""
        entries = [
            {
                "url": "https://a.com/p1",
                "url_path": "/p1",
                "domain": "a.com",
                "lastmod": None,
                "lastmod_month": None,
                "sitemap_source": "https://a.com/sitemap.xml",
            },
            {
                "url": "https://b.com/p2",
                "url_path": "/p2",
                "domain": "b.com",
                "lastmod": None,
                "lastmod_month": None,
                "sitemap_source": "https://b.com/sitemap.xml",
            },
        ]

        delete_sqls = []
        mock_client.query.side_effect = lambda sql, **kw: (
            delete_sqls.append(sql) or MagicMock()
        )
        mock_client.load_table_from_json.return_value = MagicMock()
        mock_client.load_table_from_json.return_value.result.return_value = None

        bq_backend.insert_sitemap_urls(entries)

        combined = " ".join(delete_sqls)
        assert "a.com" in combined
        assert "b.com" in combined
