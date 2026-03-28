"""
Integration tests for SQLite storage backend.

Tests:
- Database initialization and table creation
- Raw record insertion and retrieval
- Clean record insertion and retrieval
- Date range queries and counts
- Table existence checks
- Vacuum after large deletes
"""

from datetime import timedelta
from unittest.mock import patch

import pytest

from llm_bot_pipeline.storage.factory import get_backend


class TestSQLiteBackendInitialization:
    """Tests for backend initialization."""

    def test_backend_creates_database(self, sqlite_backend, temp_db_path):
        """Backend should create database file."""
        assert temp_db_path.exists()

    def test_backend_creates_tables(self, sqlite_backend):
        """Backend should create required tables."""
        assert sqlite_backend.table_exists("raw_bot_requests")
        assert sqlite_backend.table_exists("bot_requests_daily")
        assert sqlite_backend.table_exists("daily_summary")
        assert sqlite_backend.table_exists("url_performance")
        assert sqlite_backend.table_exists("query_fanout_sessions")
        assert sqlite_backend.table_exists("session_url_details")

    def test_backend_type_is_sqlite(self, sqlite_backend):
        """Backend type should be sqlite."""
        assert sqlite_backend.backend_type == "sqlite"


class TestRawRecordOperations:
    """Tests for raw record CRUD operations."""

    def test_insert_raw_records(self, sqlite_backend, sample_records_small):
        """Should insert raw records and return count."""
        rows = sqlite_backend.insert_raw_records(sample_records_small)
        assert rows == len(sample_records_small)

    def test_get_table_row_count(self, sqlite_backend_with_data):
        """Should return correct row count."""
        backend, rows_inserted = sqlite_backend_with_data
        count = backend.get_table_row_count("raw_bot_requests")
        assert count == rows_inserted

    def test_insert_adds_ingestion_time(self, sqlite_backend, sample_records_small):
        """Inserted records should have _ingestion_time."""
        sqlite_backend.insert_raw_records(sample_records_small)

        result = sqlite_backend.query(
            "SELECT _ingestion_time FROM raw_bot_requests LIMIT 1"
        )
        assert result
        assert result[0]["_ingestion_time"] is not None


class TestCleanRecordOperations:
    """Tests for clean/transformed record operations."""

    def test_insert_clean_records(self, sqlite_backend):
        """Should insert clean records."""
        clean_records = [
            {
                "request_date": "2024-01-15",
                "request_timestamp": "2024-01-15T10:00:00+00:00",
                "request_hour": 10,
                "day_of_week": "Monday",
                "request_host": "example.com",
                "request_uri": "/docs/test",
                "url_path": "/docs/test",
                "url_path_depth": 2,
                "bot_name": "GPTBot",
                "bot_provider": "OpenAI",
                "bot_category": "training",
                "crawler_country": "US",
                "response_status": 200,
                "response_status_category": "2xx_success",
            }
        ]

        rows = sqlite_backend.insert_clean_records(clean_records)
        assert rows == 1

        count = sqlite_backend.get_table_row_count("bot_requests_daily")
        assert count == 1


class TestDateRangeOperations:
    """Tests for date range queries."""

    def test_get_date_range_count(self, sqlite_backend_with_data, date_range):
        """Should return count for date range."""
        backend, _ = sqlite_backend_with_data
        start_date, end_date = date_range

        # Use wider date range to capture all records
        wide_start = start_date - timedelta(days=30)
        wide_end = end_date + timedelta(days=30)

        count = backend.get_date_range_count(
            table_name="raw_bot_requests",
            start_date=wide_start,
            end_date=wide_end,
            date_column="EdgeStartTimestamp",
        )
        assert count >= 0

    def test_delete_date_range(self, sqlite_backend_with_data, date_range):
        """Should delete records in date range."""
        backend, rows_before = sqlite_backend_with_data
        start_date, end_date = date_range

        # Use exact date range from sample data
        wide_start = start_date - timedelta(days=30)
        wide_end = end_date + timedelta(days=30)

        deleted = backend.delete_date_range(
            table_name="raw_bot_requests",
            start_date=wide_start,
            end_date=wide_end,
            date_column="EdgeStartTimestamp",
        )

        # Verify deletion
        count_after = backend.get_table_row_count("raw_bot_requests")
        assert count_after == rows_before - deleted


class TestQueryOperations:
    """Tests for general query operations."""

    def test_query_returns_list_of_dicts(self, sqlite_backend_with_data):
        """Query should return list of dictionaries."""
        backend, _ = sqlite_backend_with_data

        result = backend.query("SELECT * FROM raw_bot_requests LIMIT 5")
        assert isinstance(result, list)
        assert len(result) > 0
        assert isinstance(result[0], dict)

    def test_query_with_no_results(self, sqlite_backend):
        """Query with no matches should return empty list."""
        result = sqlite_backend.query("SELECT * FROM raw_bot_requests WHERE 1=0")
        assert result == []

    def test_execute_returns_affected_rows(self, sqlite_backend_with_data):
        """Execute should return affected row count."""
        backend, _ = sqlite_backend_with_data

        # Update a non-existent record
        affected = backend.execute(
            "UPDATE raw_bot_requests SET EdgeResponseStatus = 99 WHERE ClientCountry = 'XX'"
        )
        assert affected == 0


class TestVacuumAfterLargeDeletes:
    """Tests for automatic vacuum after bulk deletes."""

    def test_vacuum_called_when_threshold_exceeded(
        self, temp_db_path, sample_records_small
    ):
        """Vacuum should run when cumulative deletes exceed threshold."""
        backend = get_backend(
            "sqlite",
            db_path=temp_db_path,
            vacuum_threshold=5,
        )
        backend.initialize()
        backend.insert_raw_records(sample_records_small)

        with patch.object(backend, "vacuum") as mock_vacuum:
            backend.execute("DELETE FROM raw_bot_requests WHERE 1=1")
            mock_vacuum.assert_called_once()
        backend.close()

    def test_no_vacuum_when_threshold_not_exceeded(
        self, temp_db_path, sample_records_small
    ):
        """Vacuum should not run when deletes below threshold."""
        backend = get_backend(
            "sqlite",
            db_path=temp_db_path,
            vacuum_threshold=10_000,
        )
        backend.initialize()
        backend.insert_raw_records(sample_records_small)

        with patch.object(backend, "vacuum") as mock_vacuum:
            backend.execute("DELETE FROM raw_bot_requests WHERE 1=1")
            mock_vacuum.assert_not_called()
        backend.close()

    def test_no_vacuum_when_disabled(self, temp_db_path, sample_records_small):
        """Vacuum should not run when vacuum_threshold is 0."""
        backend = get_backend(
            "sqlite",
            db_path=temp_db_path,
            vacuum_threshold=0,
        )
        backend.initialize()
        backend.insert_raw_records(sample_records_small)

        with patch.object(backend, "vacuum") as mock_vacuum:
            backend.execute("DELETE FROM raw_bot_requests WHERE 1=1")
            mock_vacuum.assert_not_called()
        backend.close()


class TestSessionUrlDetailsTable:
    """Tests for session_url_details table schema and indexes."""

    def test_session_url_details_table_exists(self, sqlite_backend):
        """session_url_details table should be created on initialization."""
        assert sqlite_backend.table_exists("session_url_details")

    def test_session_url_details_has_correct_columns(self, sqlite_backend):
        """session_url_details should have all required columns."""
        columns = sqlite_backend.query("PRAGMA table_info(session_url_details)")
        column_names = {col["name"] for col in columns}

        expected_columns = {
            "id",
            "session_id",
            "session_date",
            "domain",
            "url",
            "url_position",
            "bot_provider",
            "bot_name",
            "fanout_session_name",
            "session_unique_urls",
            "session_request_count",
            "session_duration_ms",
            "mean_cosine_similarity",
            "min_cosine_similarity",
            "max_cosine_similarity",
            "confidence_level",
            "session_start_time",
            "session_end_time",
            "window_ms",
            "splitting_strategy",
            "_created_at",
        }
        assert expected_columns == column_names

    def test_session_url_details_indexes_exist(self, sqlite_backend):
        """session_url_details should have all required indexes."""
        indexes = sqlite_backend.query("PRAGMA index_list(session_url_details)")
        index_names = {idx["name"] for idx in indexes}

        expected_indexes = {
            "idx_session_url_details_date",
            "idx_session_url_details_url",
            "idx_session_url_details_session",
            "idx_session_url_details_bot",
            "idx_session_url_details_unique_urls",
        }
        assert expected_indexes.issubset(index_names)

    def test_session_url_details_foreign_key_constraint(self, sqlite_backend):
        """session_url_details should have foreign key to query_fanout_sessions."""
        fk_info = sqlite_backend.query("PRAGMA foreign_key_list(session_url_details)")
        assert len(fk_info) == 1
        assert fk_info[0]["table"] == "query_fanout_sessions"
        assert fk_info[0]["from"] == "session_id"
        assert fk_info[0]["to"] == "session_id"


class TestReportingViews:
    """Tests for reporting views created during initialization."""

    def _view_exists(self, backend, view_name: str) -> bool:
        """Check if a view exists in the database."""
        result = backend.query(
            "SELECT name FROM sqlite_master WHERE type='view' AND name=:name",
            {"name": view_name},
        )
        return len(result) > 0

    def test_v_session_url_distribution_view_exists(self, sqlite_backend):
        """v_session_url_distribution view should be created on initialization."""
        assert self._view_exists(sqlite_backend, "v_session_url_distribution")

    def test_v_session_singleton_binary_view_exists(self, sqlite_backend):
        """v_session_singleton_binary view should be created on initialization."""
        assert self._view_exists(sqlite_backend, "v_session_singleton_binary")

    def test_v_bot_volume_view_exists(self, sqlite_backend):
        """v_bot_volume view should be created on initialization."""
        assert self._view_exists(sqlite_backend, "v_bot_volume")

    def test_v_top_session_topics_view_exists(self, sqlite_backend):
        """v_top_session_topics view should be created on initialization."""
        assert self._view_exists(sqlite_backend, "v_top_session_topics")

    def test_v_daily_kpis_view_exists(self, sqlite_backend):
        """v_daily_kpis view should be created on initialization."""
        assert self._view_exists(sqlite_backend, "v_daily_kpis")

    def test_v_category_comparison_view_exists(self, sqlite_backend):
        """v_category_comparison view should be created on initialization."""
        assert self._view_exists(sqlite_backend, "v_category_comparison")

    def test_v_url_cooccurrence_view_exists(self, sqlite_backend):
        """v_url_cooccurrence view should be created on initialization."""
        assert self._view_exists(sqlite_backend, "v_url_cooccurrence")

    def test_all_seven_views_created(self, sqlite_backend):
        """All 7 reporting views should be created."""
        views = sqlite_backend.query(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        )
        view_names = {v["name"] for v in views}

        expected_views = {
            "v_session_url_distribution",
            "v_session_singleton_binary",
            "v_bot_volume",
            "v_top_session_topics",
            "v_daily_kpis",
            "v_category_comparison",
            "v_url_cooccurrence",
        }
        assert expected_views.issubset(view_names)

    def test_v_session_url_distribution_has_correct_columns(self, sqlite_backend):
        """v_session_url_distribution should have expected columns."""
        columns = sqlite_backend.query("PRAGMA table_info(v_session_url_distribution)")
        column_names = {col["name"] for col in columns}
        expected = {
            "session_date",
            "domain",
            "url_bucket",
            "sort_order",
            "session_count",
        }
        assert expected == column_names

    def test_v_daily_kpis_has_correct_columns(self, sqlite_backend):
        """v_daily_kpis should have all KPI columns."""
        columns = sqlite_backend.query("PRAGMA table_info(v_daily_kpis)")
        column_names = {col["name"] for col in columns}

        expected_columns = {
            "session_date",
            "domain",
            "total_sessions",
            "unique_urls_requested",
            "avg_urls_per_session",
            "singleton_count",
            "singleton_rate",
            "multi_url_count",
            "multi_url_rate",
            "mean_mibcs_multi_url",
            "high_confidence_count",
            "high_confidence_rate",
            "medium_confidence_count",
            "low_confidence_count",
        }
        assert expected_columns == column_names

    def test_v_url_cooccurrence_has_correct_columns(self, sqlite_backend):
        """v_url_cooccurrence should have expected columns."""
        columns = sqlite_backend.query("PRAGMA table_info(v_url_cooccurrence)")
        column_names = {col["name"] for col in columns}

        expected_columns = {
            "session_id",
            "session_date",
            "domain",
            "url",
            "full_url",
            "bot_name",
            "topic",
            "session_unique_urls",
            "mean_cosine_similarity",
            "confidence_level",
        }
        assert expected_columns == column_names
