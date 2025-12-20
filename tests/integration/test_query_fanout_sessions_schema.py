"""
Integration tests for query_fanout_sessions table schema.

Tests:
- Schema creation with all required fields
- Index creation
- CHECK constraint validation for confidence_level
- Data insertion with valid and invalid values
"""

import json
from datetime import datetime

import pytest

from llm_bot_pipeline.schemas.bundles import QUERY_FANOUT_SESSIONS_INDEXES

# QUERY_FANOUT_SESSIONS_COLUMNS used for reference only
# from llm_bot_pipeline.schemas.bundles import (


class TestQueryFanoutSessionsSchema:
    """Tests for query_fanout_sessions table schema."""

    def test_table_exists_after_initialization(self, sqlite_backend):
        """Table should exist after backend initialization."""
        assert sqlite_backend.table_exists("query_fanout_sessions")

    def test_schema_has_all_required_fields(self, sqlite_backend):
        """Schema should have all required fields from PRD."""
        schema_info = sqlite_backend.get_schema_info()
        columns = {col["name"]: col for col in schema_info["query_fanout_sessions"]}

        # Required fields from PRD
        required_fields = [
            "id",
            "session_id",
            "session_date",
            "session_start_time",
            "session_end_time",
            "duration_ms",
            "bot_provider",
            "request_count",
            "unique_urls",
            "mean_cosine_similarity",
            "min_cosine_similarity",
            "confidence_level",
            "fanout_session_name",
            "url_list",
            "_created_at",
        ]

        for field in required_fields:
            assert field in columns, f"Missing required field: {field}"

    def test_session_id_is_unique(self, sqlite_backend):
        """session_id should have UNIQUE constraint."""
        schema_info = sqlite_backend.get_schema_info()
        columns = {col["name"]: col for col in schema_info["query_fanout_sessions"]}

        # Check if session_id has unique constraint
        # In SQLite, we check by attempting to insert duplicate
        session_id = "test-session-123"

        # First insert should succeed
        sqlite_backend.execute(
            """
            INSERT INTO query_fanout_sessions (
                session_id, session_date, session_start_time, session_end_time,
                duration_ms, bot_provider, request_count, unique_urls,
                confidence_level, url_list, window_ms
            ) VALUES (:session_id, :session_date, :session_start_time, :session_end_time,
                :duration_ms, :bot_provider, :request_count, :unique_urls,
                :confidence_level, :url_list, :window_ms)
            """,
            {
                "session_id": session_id,
                "session_date": "2024-01-15",
                "session_start_time": "2024-01-15T10:00:00+00:00",
                "session_end_time": "2024-01-15T10:00:01+00:00",
                "duration_ms": 1000,
                "bot_provider": "OpenAI",
                "request_count": 2,
                "unique_urls": 2,
                "confidence_level": "high",
                "url_list": json.dumps(
                    ["https://example.com/page1", "https://example.com/page2"]
                ),
                "window_ms": 100.0,
            },
        )

        # Second insert with same session_id should fail
        with pytest.raises(Exception):  # sqlite3.IntegrityError
            sqlite_backend.execute(
                """
                INSERT INTO query_fanout_sessions (
                    session_id, session_date, session_start_time, session_end_time,
                    duration_ms, bot_provider, request_count, unique_urls,
                    confidence_level, url_list, window_ms
                ) VALUES (:session_id, :session_date, :session_start_time, :session_end_time,
                    :duration_ms, :bot_provider, :request_count, :unique_urls,
                    :confidence_level, :url_list, :window_ms)
                """,
                {
                    "session_id": session_id,
                    "session_date": "2024-01-15",
                    "session_start_time": "2024-01-15T10:00:00+00:00",
                    "session_end_time": "2024-01-15T10:00:01+00:00",
                    "duration_ms": 1000,
                    "bot_provider": "OpenAI",
                    "request_count": 2,
                    "unique_urls": 2,
                    "confidence_level": "high",
                    "url_list": json.dumps(["https://example.com/page1"]),
                    "window_ms": 100.0,
                },
            )

    def test_duration_ms_is_integer(self, sqlite_backend):
        """duration_ms should be INTEGER type."""
        schema_info = sqlite_backend.get_schema_info()
        columns = {col["name"]: col for col in schema_info["query_fanout_sessions"]}

        duration_col = columns["duration_ms"]
        # SQLite stores INTEGER as "INTEGER" type
        assert duration_col["type"].upper() in ("INTEGER", "INT")

    def test_url_list_is_not_null(self, sqlite_backend):
        """url_list should be NOT NULL."""
        schema_info = sqlite_backend.get_schema_info()
        columns = {col["name"]: col for col in schema_info["query_fanout_sessions"]}

        url_list_col = columns["url_list"]
        assert url_list_col["notnull"] == 1, "url_list should be NOT NULL"

    def test_fanout_session_name_field_exists(self, sqlite_backend):
        """fanout_session_name field should exist."""
        schema_info = sqlite_backend.get_schema_info()
        columns = {col["name"]: col for col in schema_info["query_fanout_sessions"]}

        assert "fanout_session_name" in columns

    def test_indexes_are_created(self, sqlite_backend):
        """All required indexes should be created."""
        indexes = sqlite_backend.query(
            """
            SELECT name FROM sqlite_master
            WHERE type='index' AND tbl_name='query_fanout_sessions'
            """
        )
        index_names = {idx["name"] for idx in indexes}

        # Check for required indexes from PRD
        required_indexes = {
            "idx_sessions_date",
            "idx_sessions_provider",
            "idx_sessions_confidence",
            "idx_sessions_request_count",
        }

        for idx_name in required_indexes:
            assert idx_name in index_names, f"Missing index: {idx_name}"

    def test_check_constraint_valid_confidence_high(self, sqlite_backend):
        """CHECK constraint should allow 'high' confidence level."""
        sqlite_backend.execute(
            """
            INSERT INTO query_fanout_sessions (
                session_id, session_date, session_start_time, session_end_time,
                duration_ms, bot_provider, request_count, unique_urls,
                confidence_level, url_list, window_ms
            ) VALUES (:session_id, :session_date, :session_start_time, :session_end_time,
                :duration_ms, :bot_provider, :request_count, :unique_urls,
                :confidence_level, :url_list, :window_ms)
            """,
            {
                "session_id": "test-high-1",
                "session_date": "2024-01-15",
                "session_start_time": "2024-01-15T10:00:00+00:00",
                "session_end_time": "2024-01-15T10:00:01+00:00",
                "duration_ms": 1000,
                "bot_provider": "OpenAI",
                "request_count": 2,
                "unique_urls": 2,
                "confidence_level": "high",
                "url_list": json.dumps(["https://example.com/page1"]),
                "window_ms": 100.0,
            },
        )

        # Should succeed without error
        result = sqlite_backend.query(
            "SELECT confidence_level FROM query_fanout_sessions WHERE session_id = :session_id",
            {"session_id": "test-high-1"},
        )
        assert result[0]["confidence_level"] == "high"

    def test_check_constraint_valid_confidence_medium(self, sqlite_backend):
        """CHECK constraint should allow 'medium' confidence level."""
        sqlite_backend.execute(
            """
            INSERT INTO query_fanout_sessions (
                session_id, session_date, session_start_time, session_end_time,
                duration_ms, bot_provider, request_count, unique_urls,
                confidence_level, url_list, window_ms
            ) VALUES (:session_id, :session_date, :session_start_time, :session_end_time,
                :duration_ms, :bot_provider, :request_count, :unique_urls,
                :confidence_level, :url_list, :window_ms)
            """,
            {
                "session_id": "test-medium-1",
                "session_date": "2024-01-15",
                "session_start_time": "2024-01-15T10:00:00+00:00",
                "session_end_time": "2024-01-15T10:00:01+00:00",
                "duration_ms": 1000,
                "bot_provider": "OpenAI",
                "request_count": 2,
                "unique_urls": 2,
                "confidence_level": "medium",
                "url_list": json.dumps(["https://example.com/page1"]),
                "window_ms": 100.0,
            },
        )

        result = sqlite_backend.query(
            "SELECT confidence_level FROM query_fanout_sessions WHERE session_id = :session_id",
            {"session_id": "test-medium-1"},
        )
        assert result[0]["confidence_level"] == "medium"

    def test_check_constraint_valid_confidence_low(self, sqlite_backend):
        """CHECK constraint should allow 'low' confidence level."""
        sqlite_backend.execute(
            """
            INSERT INTO query_fanout_sessions (
                session_id, session_date, session_start_time, session_end_time,
                duration_ms, bot_provider, request_count, unique_urls,
                confidence_level, url_list, window_ms
            ) VALUES (:session_id, :session_date, :session_start_time, :session_end_time,
                :duration_ms, :bot_provider, :request_count, :unique_urls,
                :confidence_level, :url_list, :window_ms)
            """,
            {
                "session_id": "test-low-1",
                "session_date": "2024-01-15",
                "session_start_time": "2024-01-15T10:00:00+00:00",
                "session_end_time": "2024-01-15T10:00:01+00:00",
                "duration_ms": 1000,
                "bot_provider": "OpenAI",
                "request_count": 2,
                "unique_urls": 2,
                "confidence_level": "low",
                "url_list": json.dumps(["https://example.com/page1"]),
                "window_ms": 100.0,
            },
        )

        result = sqlite_backend.query(
            "SELECT confidence_level FROM query_fanout_sessions WHERE session_id = :session_id",
            {"session_id": "test-low-1"},
        )
        assert result[0]["confidence_level"] == "low"

    def test_check_constraint_rejects_invalid_confidence(self, sqlite_backend):
        """CHECK constraint should reject invalid confidence levels."""
        with pytest.raises(Exception):  # sqlite3.IntegrityError
            sqlite_backend.execute(
                """
                INSERT INTO query_fanout_sessions (
                    session_id, session_date, session_start_time, session_end_time,
                    duration_ms, bot_provider, request_count, unique_urls,
                    confidence_level, url_list, window_ms
                ) VALUES (:session_id, :session_date, :session_start_time, :session_end_time,
                    :duration_ms, :bot_provider, :request_count, :unique_urls,
                    :confidence_level, :url_list, :window_ms)
                """,
                {
                    "session_id": "test-invalid-1",
                    "session_date": "2024-01-15",
                    "session_start_time": "2024-01-15T10:00:00+00:00",
                    "session_end_time": "2024-01-15T10:00:01+00:00",
                    "duration_ms": 1000,
                    "bot_provider": "OpenAI",
                    "request_count": 2,
                    "unique_urls": 2,
                    "confidence_level": "invalid",
                    "url_list": json.dumps(["https://example.com/page1"]),
                    "window_ms": 100.0,
                },
            )

    def test_insert_with_fanout_session_name(self, sqlite_backend):
        """Should be able to insert with fanout_session_name."""
        sqlite_backend.execute(
            """
            INSERT INTO query_fanout_sessions (
                session_id, session_date, session_start_time, session_end_time,
                duration_ms, bot_provider, request_count, unique_urls,
                confidence_level, fanout_session_name, url_list, window_ms
            ) VALUES (:session_id, :session_date, :session_start_time, :session_end_time,
                :duration_ms, :bot_provider, :request_count, :unique_urls,
                :confidence_level, :fanout_session_name, :url_list, :window_ms)
            """,
            {
                "session_id": "test-with-name-1",
                "session_date": "2024-01-15",
                "session_start_time": "2024-01-15T10:00:00+00:00",
                "session_end_time": "2024-01-15T10:00:01+00:00",
                "duration_ms": 1000,
                "bot_provider": "OpenAI",
                "request_count": 2,
                "unique_urls": 2,
                "confidence_level": "high",
                "fanout_session_name": "home buying guide",
                "url_list": json.dumps(["https://example.com/page1"]),
                "window_ms": 100.0,
            },
        )

        result = sqlite_backend.query(
            "SELECT fanout_session_name FROM query_fanout_sessions WHERE session_id = :session_id",
            {"session_id": "test-with-name-1"},
        )
        assert result[0]["fanout_session_name"] == "home buying guide"

    def test_insert_with_all_fields(self, sqlite_backend):
        """Should be able to insert record with all fields."""
        sqlite_backend.execute(
            """
            INSERT INTO query_fanout_sessions (
                session_id, session_date, session_start_time, session_end_time,
                duration_ms, bot_provider, bot_name, request_count, unique_urls,
                mean_cosine_similarity, min_cosine_similarity, max_cosine_similarity,
                confidence_level, fanout_session_name, url_list, window_ms
            ) VALUES (:session_id, :session_date, :session_start_time, :session_end_time,
                :duration_ms, :bot_provider, :bot_name, :request_count, :unique_urls,
                :mean_cosine_similarity, :min_cosine_similarity, :max_cosine_similarity,
                :confidence_level, :fanout_session_name, :url_list, :window_ms)
            """,
            {
                "session_id": "test-complete-1",
                "session_date": "2024-01-15",
                "session_start_time": "2024-01-15T10:00:00+00:00",
                "session_end_time": "2024-01-15T10:00:01+00:00",
                "duration_ms": 1000,
                "bot_provider": "OpenAI",
                "bot_name": "ChatGPT-User",
                "request_count": 3,
                "unique_urls": 3,
                "mean_cosine_similarity": 0.85,
                "min_cosine_similarity": 0.72,
                "max_cosine_similarity": 0.95,
                "confidence_level": "high",
                "fanout_session_name": "mortgage calculator",
                "url_list": json.dumps(
                    [
                        "https://example.com/page1",
                        "https://example.com/page2",
                        "https://example.com/page3",
                    ]
                ),
                "window_ms": 100.0,
            },
        )

        result = sqlite_backend.query(
            "SELECT * FROM query_fanout_sessions WHERE session_id = :session_id",
            {"session_id": "test-complete-1"},
        )
        assert len(result) == 1
        record = result[0]
        assert record["session_id"] == "test-complete-1"
        assert record["bot_provider"] == "OpenAI"
        assert record["confidence_level"] == "high"
        assert record["fanout_session_name"] == "mortgage calculator"
        assert record["duration_ms"] == 1000
