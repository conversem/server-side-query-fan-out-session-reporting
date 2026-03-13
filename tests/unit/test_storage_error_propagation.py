"""Tests that StorageError propagates from pipeline stats methods instead of being silently swallowed."""

import logging
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline
from llm_bot_pipeline.storage import StorageError


@pytest.fixture
def mock_backend():
    backend = MagicMock()
    backend.backend_type = "sqlite"
    return backend


@pytest.fixture
def pipeline(mock_backend):
    p = LocalPipeline(backend=mock_backend)
    p._initialized = True
    return p


class TestGetRawCountPropagatesError:
    def test_storage_error_propagates(self, pipeline, mock_backend):
        mock_backend.query.side_effect = StorageError("connection lost")

        with pytest.raises(
            StorageError, match="Failed to get raw count.*connection lost"
        ):
            pipeline._get_raw_count(date(2025, 1, 1), date(2025, 1, 31))

    def test_preserves_exception_chain(self, pipeline, mock_backend):
        original = StorageError("disk full")
        mock_backend.query.side_effect = original

        with pytest.raises(StorageError) as exc_info:
            pipeline._get_raw_count(date(2025, 1, 1), date(2025, 1, 31))

        assert exc_info.value.__cause__ is original

    def test_includes_date_range_in_message(self, pipeline, mock_backend):
        mock_backend.query.side_effect = StorageError("timeout")

        with pytest.raises(StorageError, match="2025-01-01.*2025-01-31"):
            pipeline._get_raw_count(date(2025, 1, 1), date(2025, 1, 31))


class TestGetTransformStatsPropagatesError:
    def test_storage_error_propagates(self, pipeline, mock_backend):
        mock_backend.query.side_effect = StorageError("query failed")

        with pytest.raises(
            StorageError, match="Failed to get transform stats.*query failed"
        ):
            pipeline._get_transform_stats(date(2025, 1, 1), date(2025, 1, 31))

    def test_preserves_exception_chain(self, pipeline, mock_backend):
        original = StorageError("lock timeout")
        mock_backend.query.side_effect = original

        with pytest.raises(StorageError) as exc_info:
            pipeline._get_transform_stats(date(2025, 1, 1), date(2025, 1, 31))

        assert exc_info.value.__cause__ is original

    def test_includes_date_range_in_message(self, pipeline, mock_backend):
        mock_backend.query.side_effect = StorageError("error")

        with pytest.raises(StorageError, match="2025-01-01.*2025-01-31"):
            pipeline._get_transform_stats(date(2025, 1, 1), date(2025, 1, 31))


class TestCallerHandlesStorageErrorGracefully:
    def test_run_captures_storage_error_in_result(self, pipeline, mock_backend):
        mock_backend.query.side_effect = StorageError("backend unavailable")

        result = pipeline.run(date(2025, 1, 1), date(2025, 1, 31))

        assert result.success is False
        assert any("backend unavailable" in e for e in result.errors)

    def test_run_logs_storage_error(self, pipeline, mock_backend, caplog):
        mock_backend.query.side_effect = StorageError("backend unavailable")

        with caplog.at_level(logging.ERROR):
            pipeline.run(date(2025, 1, 1), date(2025, 1, 31))

        assert any("Storage error" in r.message for r in caplog.records)

    def test_run_still_sets_completed_at(self, pipeline, mock_backend):
        mock_backend.query.side_effect = StorageError("backend down")

        result = pipeline.run(date(2025, 1, 1), date(2025, 1, 31))

        assert result.completed_at is not None
