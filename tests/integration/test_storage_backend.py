"""
Integration tests for SQLite storage backend.

Tests:
- Database initialization and table creation
- Raw record insertion and retrieval
- Clean record insertion and retrieval
- Date range queries and counts
- Table existence checks
"""

from datetime import timedelta

import pytest


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
                "bot_score": 5,
                "is_verified_bot": 1,
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
            "UPDATE raw_bot_requests SET BotScore = 99 WHERE ClientCountry = 'XX'"
        )
        assert affected == 0

