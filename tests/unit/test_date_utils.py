"""Tests for date_utils."""

from datetime import date, timezone

import pytest

from llm_bot_pipeline.utils.date_utils import parse_date, utc_now


class TestUtcNow:
    def test_utc_now_returns_timezone_aware(self):
        now = utc_now()
        assert now.tzinfo is timezone.utc


class TestParseDate:
    def test_parse_valid_date(self):
        assert parse_date("2026-01-15") == date(2026, 1, 15)

    def test_parse_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date("01-15-2026")

    def test_parse_empty_string(self):
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date("")

    def test_parse_leap_day(self):
        assert parse_date("2024-02-29") == date(2024, 2, 29)

    def test_parse_boundary_year_start(self):
        assert parse_date("2026-01-01") == date(2026, 1, 1)

    def test_parse_boundary_year_end(self):
        assert parse_date("2026-12-31") == date(2026, 12, 31)

    def test_parse_none_input(self):
        with pytest.raises((TypeError, ValueError)):
            parse_date(None)
