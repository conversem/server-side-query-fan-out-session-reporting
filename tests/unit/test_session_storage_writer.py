"""Tests for SessionStorageWriter class."""

from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.reporting.session_aggregations import SessionRecord
from llm_bot_pipeline.reporting.session_storage_writer import (
    BIGQUERY_BATCH_SIZE,
    TABLE_NAME,
    SessionStorageWriter,
    record_to_dict,
)


def _make_record(
    session_id: str = "s1",
    bot_provider: str = "openai",
    request_count: int = 3,
) -> SessionRecord:
    """Build a SessionRecord with sensible defaults."""
    return SessionRecord(
        session_id=session_id,
        session_date="2025-01-15",
        session_start_time="2025-01-15T10:00:00+00:00",
        session_end_time="2025-01-15T10:00:05+00:00",
        duration_ms=5000.0,
        bot_provider=bot_provider,
        bot_name="gpt-4",
        request_count=request_count,
        unique_urls=request_count,
        mean_cosine_similarity=0.85,
        min_cosine_similarity=0.70,
        max_cosine_similarity=0.95,
        confidence_level="high",
        fanout_session_name="example-page",
        url_list='["https://example.com/a","https://example.com/b"]',
        window_ms=5000.0,
        splitting_strategy=None,
        _created_at="2025-01-15T12:00:00+00:00",
    )


def _make_sqlite_backend() -> MagicMock:
    backend = MagicMock()
    backend.backend_type = "sqlite"
    backend.get_full_table_id.return_value = TABLE_NAME
    backend.execute.return_value = 1
    return backend


def _make_bigquery_backend() -> MagicMock:
    backend = MagicMock()
    backend.backend_type = "bigquery"
    backend.get_full_table_id.return_value = "project.dataset.query_fanout_sessions"
    mock_client = MagicMock()
    mock_client.insert_rows_json.return_value = []
    backend._get_client.return_value = mock_client
    return backend


class TestRecordToDict:
    """Test the record_to_dict helper function."""

    def test_all_fields_present(self):
        record = _make_record()
        d = record_to_dict(record)

        assert d["session_id"] == "s1"
        assert d["bot_provider"] == "openai"
        assert d["duration_ms"] == 5000
        assert isinstance(d["duration_ms"], int)
        assert d["mean_cosine_similarity"] == 0.85
        assert d["was_refined"] is False
        assert d["parent_session_id"] is None

    def test_refined_record_fields(self):
        record = _make_record()
        record.was_refined = True
        record.parent_session_id = "parent_1"
        record.refinement_reason = "semantic_split"
        record.pre_refinement_mibcs = 0.42

        d = record_to_dict(record)
        assert d["was_refined"] is True
        assert d["parent_session_id"] == "parent_1"
        assert d["refinement_reason"] == "semantic_split"
        assert d["pre_refinement_mibcs"] == 0.42


class TestSessionStorageWriterInserts:
    """Mock backend and feed sessions, assert insert called with correct data."""

    def test_sqlite_insert_calls_execute(self):
        backend = _make_sqlite_backend()
        writer = SessionStorageWriter(backend)
        records = [_make_record(session_id="s1"), _make_record(session_id="s2")]

        inserted = writer.insert_sessions(records)

        assert inserted == 2
        assert backend.execute.call_count == 2

        first_call_params = backend.execute.call_args_list[0][0][1]
        assert first_call_params["session_id"] == "s1"
        assert first_call_params["was_refined"] == 0

    @pytest.mark.bigquery
    def test_bigquery_insert_uses_streaming(self):
        backend = _make_bigquery_backend()
        writer = SessionStorageWriter(backend)
        records = [_make_record(session_id="s1"), _make_record(session_id="s2")]

        inserted = writer.insert_sessions(records)

        assert inserted == 2
        client = backend._get_client()
        client.insert_rows_json.assert_called_once()
        rows_arg = client.insert_rows_json.call_args[0][1]
        assert len(rows_arg) == 2
        assert rows_arg[0]["session_id"] == "s1"

    def test_empty_records_returns_zero(self):
        backend = _make_sqlite_backend()
        writer = SessionStorageWriter(backend)

        assert writer.insert_sessions([]) == 0
        backend.execute.assert_not_called()

    def test_backend_property(self):
        backend = _make_sqlite_backend()
        writer = SessionStorageWriter(backend)
        assert writer.backend is backend


@pytest.mark.bigquery
class TestBatchOperations:
    """Verify batching behavior for BigQuery backend."""

    def test_large_batch_splits_into_chunks(self):
        backend = _make_bigquery_backend()
        client = backend._get_client()
        writer = SessionStorageWriter(backend)

        records = [
            _make_record(session_id=f"s{i}") for i in range(BIGQUERY_BATCH_SIZE + 5)
        ]

        inserted = writer.insert_sessions(records)

        assert inserted == BIGQUERY_BATCH_SIZE + 5
        assert client.insert_rows_json.call_count == 2

        first_batch = client.insert_rows_json.call_args_list[0][0][1]
        second_batch = client.insert_rows_json.call_args_list[1][0][1]
        assert len(first_batch) == BIGQUERY_BATCH_SIZE
        assert len(second_batch) == 5


class TestErrorHandling:
    """Verify errors propagate correctly / are handled gracefully."""

    def test_sqlite_partial_failure_continues(self):
        backend = _make_sqlite_backend()
        backend.execute.side_effect = [1, Exception("disk full"), 1]
        writer = SessionStorageWriter(backend)

        records = [
            _make_record(session_id="s1"),
            _make_record(session_id="s2"),
            _make_record(session_id="s3"),
        ]

        inserted = writer.insert_sessions(records)

        assert inserted == 2
        assert backend.execute.call_count == 3

    @pytest.mark.bigquery
    def test_bigquery_batch_failure_falls_back_to_row_by_row(self):
        backend = _make_bigquery_backend()
        client = backend._get_client()
        client.insert_rows_json.side_effect = [
            Exception("transient error"),
            [],
            [],
        ]
        writer = SessionStorageWriter(backend)
        records = [_make_record(session_id="s1"), _make_record(session_id="s2")]

        inserted = writer.insert_sessions(records)

        assert inserted == 2
        assert client.insert_rows_json.call_count == 3

    @pytest.mark.bigquery
    def test_bigquery_partial_errors_in_batch(self):
        backend = _make_bigquery_backend()
        client = backend._get_client()
        client.insert_rows_json.return_value = [{"index": 0, "errors": ["bad"]}]
        writer = SessionStorageWriter(backend)
        records = [_make_record(session_id="s1"), _make_record(session_id="s2")]

        inserted = writer.insert_sessions(records)

        assert inserted == 1

    def test_delete_sessions_error_returns_zero(self):
        backend = _make_sqlite_backend()
        backend.execute.side_effect = Exception("table missing")
        writer = SessionStorageWriter(backend)

        assert writer.delete_sessions() == 0


class TestDeleteSessions:
    """Test delete_sessions with various filters."""

    def test_delete_all(self):
        backend = _make_sqlite_backend()
        backend.execute.return_value = 42
        writer = SessionStorageWriter(backend)

        deleted = writer.delete_sessions()

        assert deleted == 42
        sql = backend.execute.call_args[0][0]
        assert "DELETE FROM" in sql
        assert "WHERE" not in sql

    def test_delete_by_date(self):
        backend = _make_sqlite_backend()
        backend.execute.return_value = 5
        writer = SessionStorageWriter(backend)

        deleted = writer.delete_sessions(session_date="2025-01-15")

        assert deleted == 5
        sql = backend.execute.call_args[0][0]
        assert "session_date = :session_date" in sql

    def test_delete_by_provider(self):
        backend = _make_sqlite_backend()
        backend.execute.return_value = 3
        writer = SessionStorageWriter(backend)

        deleted = writer.delete_sessions(bot_provider="openai")

        assert deleted == 3
        sql = backend.execute.call_args[0][0]
        assert "bot_provider = :bot_provider" in sql

    def test_delete_by_date_and_provider(self):
        backend = _make_sqlite_backend()
        backend.execute.return_value = 2
        writer = SessionStorageWriter(backend)

        deleted = writer.delete_sessions(
            session_date="2025-01-15", bot_provider="openai"
        )

        assert deleted == 2
        sql = backend.execute.call_args[0][0]
        assert "session_date = :session_date" in sql
        assert "bot_provider = :bot_provider" in sql
