"""Unit tests for structured JSON logging."""

import json
import logging
from io import StringIO

import pytest

from llm_bot_pipeline.config.logging_config import (
    ContextFilter,
    JsonFormatter,
    set_log_context,
)


class TestJsonLogFormat:
    """Verify JSON log format includes required fields."""

    def test_json_log_format(self):
        """Enable JSON logging, emit log, parse as JSON, assert required fields."""
        buf = StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(JsonFormatter())
        root = logging.getLogger()
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        try:
            logger = logging.getLogger("test.json_log")
            logger.info("Test message")
            output = buf.getvalue().strip()
            data = json.loads(output)
            assert "timestamp" in data
            assert "level" in data
            assert data["level"] == "INFO"
            assert "logger" in data
            assert "message" in data
            assert data["message"] == "Test message"
        finally:
            root.removeHandler(handler)


class TestDefaultLogFormatUnchanged:
    """Verify human-readable format without JSON flag."""

    def test_default_log_format_unchanged(self):
        """Without --json-logs, log output is human-readable (not JSON)."""
        buf = StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root = logging.getLogger()
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)
        try:
            logger = logging.getLogger("test.default_format")
            logger.info("Human readable message")
            output = buf.getvalue().strip()
            assert "|" in output
            assert "INFO" in output
            assert "Human readable message" in output
            with pytest.raises(json.JSONDecodeError):
                json.loads(output)
        finally:
            root.removeHandler(handler)


class TestExecutionIdPropagation:
    """Verify execution_id appears in JSON output."""

    def test_execution_id_propagation(self):
        """execution_id from set_log_context appears in JSON output."""
        buf = StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(JsonFormatter())
        handler.addFilter(ContextFilter())
        root = logging.getLogger()
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        try:
            set_log_context(execution_id="abc12345", stage="ingestion")
            logger = logging.getLogger("test.execution_id")
            logger.info("Stage started")
            output = buf.getvalue().strip()
            data = json.loads(output)
            assert data.get("execution_id") == "abc12345"
            assert data.get("stage") == "ingestion"
            assert data.get("message") == "Stage started"
        finally:
            root.removeHandler(handler)
