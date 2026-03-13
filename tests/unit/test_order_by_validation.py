"""Tests for order_by validation in StorageBackend.read_records."""

import pytest

from llm_bot_pipeline.storage.base import validate_order_by


class TestValidateOrderByDirect:
    def test_order_by_valid_column(self):
        """'request_date DESC' should be valid."""
        result = validate_order_by("request_date DESC")
        assert result == "request_date DESC"

    def test_order_by_column_only(self):
        """'request_date' without direction should be valid (default ASC)."""
        result = validate_order_by("request_date")
        assert result == "request_date"

    def test_order_by_none(self):
        """None is always valid (no validation needed)."""
        assert validate_order_by(None) is None

    def test_order_by_injection(self):
        """SQL injection attempts should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid order_by"):
            validate_order_by("1; DROP TABLE x")

    def test_order_by_injection_semicolon(self):
        """Semicolon in order_by should fail pattern match."""
        with pytest.raises(ValueError, match="Invalid order_by"):
            validate_order_by("request_date; DELETE FROM users")

    def test_order_by_invalid_column(self):
        """Non-whitelisted column should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid order_by column"):
            validate_order_by("malicious_column DESC")

    def test_order_by_valid_asc(self):
        """Explicit ASC should be valid."""
        result = validate_order_by("request_date ASC")
        assert result == "request_date ASC"


class TestReadRecordsOrderBy:
    """Integration tests: read_records validates order_by before query."""

    def test_order_by_valid_column(self, sqlite_backend):
        """'request_date DESC' should succeed."""
        result = sqlite_backend.read_records(
            "bot_requests_daily", order_by="request_date DESC", limit=5
        )
        assert isinstance(result, list)

    def test_order_by_injection(self, sqlite_backend):
        """'1; DROP TABLE x' should raise ValueError before query."""
        with pytest.raises(ValueError, match="Invalid order_by"):
            sqlite_backend.read_records("raw_bot_requests", order_by="1; DROP TABLE x")

    def test_order_by_none(self, sqlite_backend):
        """None should succeed (no ordering)."""
        result = sqlite_backend.read_records("raw_bot_requests", order_by=None)
        assert isinstance(result, list)

    def test_order_by_column_only(self, sqlite_backend):
        """'request_date' without direction should succeed."""
        result = sqlite_backend.read_records(
            "bot_requests_daily", order_by="request_date", limit=5
        )
        assert isinstance(result, list)
