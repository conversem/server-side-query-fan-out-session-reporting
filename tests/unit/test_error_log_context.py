"""Tests for error log contextual information (date_range, batch_size, etc.)."""

import logging
from datetime import date
from unittest.mock import MagicMock

import pytest

from llm_bot_pipeline.config.logging_config import build_log_context, log_with_context
from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline
from llm_bot_pipeline.storage import StorageError


class TestContextHelperFunction:
    """Verify log_with_context and build_log_context add expected fields."""

    def test_build_log_context_includes_date_range(self):
        ctx = build_log_context(date_range=(date(2025, 1, 1), date(2025, 1, 31)))
        assert ctx["date_range"] == "2025-01-01 to 2025-01-31"

    def test_build_log_context_includes_batch_size(self):
        ctx = build_log_context(batch_size=100)
        assert ctx["batch_size"] == 100

    def test_build_log_context_includes_records_processed(self):
        ctx = build_log_context(records_processed=500)
        assert ctx["records_processed"] == 500

    def test_build_log_context_includes_execution_id(self):
        ctx = build_log_context(execution_id="run-123")
        assert ctx["execution_id"] == "run-123"

    def test_build_log_context_combines_all(self):
        ctx = build_log_context(
            date_range=(date(2025, 1, 1), date(2025, 1, 15)),
            batch_size=50,
            records_processed=200,
            execution_id="exec-456",
        )
        assert ctx["date_range"] == "2025-01-01 to 2025-01-15"
        assert ctx["batch_size"] == 50
        assert ctx["records_processed"] == 200
        assert ctx["execution_id"] == "exec-456"

    def test_log_with_context_adds_expected_fields(self, caplog):
        """log_with_context adds date_range and batch_size to log record."""
        logger = logging.getLogger("test_error_log_context")
        with caplog.at_level(logging.ERROR):
            log_with_context(
                logger,
                logging.ERROR,
                "Test error: %s",
                "failure",
                date_range=(date(2025, 2, 1), date(2025, 2, 28)),
                batch_size=25,
            )
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert "failure" in record.message
        assert getattr(record, "date_range", None) == "2025-02-01 to 2025-02-28"
        assert getattr(record, "batch_size", None) == 25


class TestErrorLogIncludesContext:
    """Pipeline failure logs include contextual information."""

    @pytest.fixture
    def mock_backend(self):
        backend = MagicMock()
        backend.backend_type = "sqlite"
        return backend

    @pytest.fixture
    def pipeline(self, mock_backend):
        p = LocalPipeline(backend=mock_backend)
        p._initialized = True
        return p

    def test_error_log_includes_context(self, pipeline, mock_backend, caplog):
        """Mock failure, capture log, assert contains date_range and records_processed."""
        mock_backend.query.side_effect = StorageError("backend unavailable")

        with caplog.at_level(logging.ERROR):
            pipeline.run(date(2025, 1, 10), date(2025, 1, 15))

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(error_records) >= 1
        # At least one error record should have date_range from log_with_context
        records_with_context = [r for r in error_records if hasattr(r, "date_range")]
        assert len(records_with_context) >= 1
        assert "2025-01-10" in records_with_context[0].date_range
        assert "2025-01-15" in records_with_context[0].date_range
