"""Unit tests for LocalPipeline batch insert fallback with sub-batch processing."""

import logging
from unittest.mock import MagicMock, call, patch

import pytest

from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline
from llm_bot_pipeline.storage import StorageBackend, StorageError


def _make_pipeline(output_backend=None):
    """Create a LocalPipeline with mocked backends."""
    raw_backend = MagicMock(spec=StorageBackend)
    raw_backend.backend_type = "sqlite"
    if output_backend is None:
        output_backend = MagicMock(spec=StorageBackend)
        output_backend.backend_type = "bigquery"
        output_backend.insert_clean_records = MagicMock(return_value=0)
    pipeline = LocalPipeline(backend=raw_backend, output_backend=output_backend)
    return pipeline, output_backend


def _make_records(n):
    """Generate n minimal clean record dicts."""
    return [{"request_date": "2025-01-01", "bot_name": f"bot_{i}"} for i in range(n)]


class TestFullBatchSucceeds:
    def test_no_fallback_needed(self):
        """When insert_clean_records succeeds, no retry or sub-batch logic runs."""
        pipeline, mock_out = _make_pipeline()
        records = _make_records(100)
        mock_out.insert_clean_records.return_value = 100

        stats = pipeline._batch_insert_with_fallback(records)

        assert stats["rows_inserted"] == 100
        assert stats["rows_failed"] == 0
        assert stats["sub_batches_attempted"] == 0
        assert stats["per_row_fallbacks"] == 0
        mock_out.insert_clean_records.assert_called_once_with(records)

    def test_empty_records(self):
        """Empty input returns zero stats with no backend calls."""
        pipeline, mock_out = _make_pipeline()

        stats = pipeline._batch_insert_with_fallback([])

        assert stats["total_records"] == 0
        assert stats["rows_inserted"] == 0
        mock_out.insert_clean_records.assert_not_called()


class TestBatchRetrySucceeds:
    def test_first_attempt_fails_retry_succeeds(self):
        """First batch fails, single retry succeeds."""
        pipeline, mock_out = _make_pipeline()
        records = _make_records(50)
        mock_out.insert_clean_records.side_effect = [
            StorageError("transient"),
            50,
        ]

        stats = pipeline._batch_insert_with_fallback(records)

        assert stats["rows_inserted"] == 50
        assert stats["rows_failed"] == 0
        assert stats["sub_batches_attempted"] == 0
        assert mock_out.insert_clean_records.call_count == 2


class TestSubBatchProcessing:
    def test_splits_on_double_failure(self):
        """Full batch fails twice -> binary split into sub-batches."""
        pipeline, mock_out = _make_pipeline()
        records = _make_records(100)

        call_count = [0]

        def side_effect(batch):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise StorageError("full batch fail")
            return len(batch)

        mock_out.insert_clean_records.side_effect = side_effect

        stats = pipeline._batch_insert_with_fallback(records)

        assert stats["rows_inserted"] == 100
        assert stats["sub_batches_attempted"] == 2
        assert stats["per_row_fallbacks"] == 0
        # 2 full-batch attempts + 2 sub-batch attempts = 4
        assert mock_out.insert_clean_records.call_count == 4

    def test_recursive_split_to_per_row(self):
        """All batch inserts fail -> eventually falls back to per-row."""
        pipeline, mock_out = _make_pipeline()
        records = _make_records(8)

        mock_out.insert_clean_records.side_effect = StorageError("always fail")
        mock_out.execute = MagicMock(return_value=0)

        with patch.object(
            LocalPipeline, "_build_insert_sql", return_value="INSERT INTO t VALUES(1)"
        ):
            stats = pipeline._batch_insert_with_fallback(records)

        assert stats["rows_inserted"] == 8
        assert stats["per_row_fallbacks"] >= 1
        assert mock_out.execute.call_count == 8

    def test_one_sub_batch_succeeds_other_fails(self):
        """Left sub-batch succeeds, right sub-batch fails and splits further."""
        pipeline, mock_out = _make_pipeline()
        records = _make_records(20)

        call_count = [0]

        def side_effect(batch):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise StorageError("full batch fail")
            if call_count[0] == 3:
                return len(batch)
            if call_count[0] == 4:
                raise StorageError("right sub-batch fail")
            return len(batch)

        mock_out.insert_clean_records.side_effect = side_effect

        stats = pipeline._batch_insert_with_fallback(records)

        assert stats["rows_inserted"] == 20
        assert stats["sub_batches_attempted"] >= 2


class TestProgressLogging:
    def test_sub_batch_logging(self, caplog):
        """Verify log output contains sub-batch progress info."""
        pipeline, mock_out = _make_pipeline()
        records = _make_records(30)

        call_count = [0]

        def side_effect(batch):
            call_count[0] += 1
            if call_count[0] <= 2:
                raise StorageError("fail")
            return len(batch)

        mock_out.insert_clean_records.side_effect = side_effect

        with caplog.at_level(logging.INFO):
            stats = pipeline._batch_insert_with_fallback(records)

        log_text = caplog.text
        assert "Sub-batch" in log_text
        assert "records" in log_text
        assert stats["sub_batches_attempted"] >= 2

    def test_per_row_fallback_logged(self, caplog):
        """Per-row fallback is logged."""
        pipeline, mock_out = _make_pipeline()
        records = _make_records(5)

        mock_out.insert_clean_records.side_effect = StorageError("always fail")
        mock_out.execute = MagicMock(return_value=0)

        with patch.object(
            LocalPipeline, "_build_insert_sql", return_value="INSERT INTO t VALUES(1)"
        ):
            with caplog.at_level(logging.INFO):
                pipeline._batch_insert_with_fallback(records)

        assert "Per-row fallback" in caplog.text


class TestNoInsertCleanRecords:
    def test_backend_without_insert_clean_records(self):
        """Backend without insert_clean_records goes straight to per-row."""
        raw_backend = MagicMock(spec=StorageBackend)
        raw_backend.backend_type = "sqlite"

        out_backend = MagicMock(spec=["execute", "backend_type", "table_exists"])
        out_backend.backend_type = "sqlite"
        out_backend.execute = MagicMock(return_value=0)

        pipeline = LocalPipeline(backend=raw_backend, output_backend=out_backend)
        records = _make_records(3)

        with patch.object(
            LocalPipeline, "_build_insert_sql", return_value="INSERT INTO t VALUES(1)"
        ):
            stats = pipeline._batch_insert_with_fallback(records)

        assert stats["rows_inserted"] == 3
        assert stats["per_row_fallbacks"] == 1
        assert out_backend.execute.call_count == 3
