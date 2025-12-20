"""
Unit tests for http_utils module.

Tests HTTP status code categorization.
"""

import pytest

from llm_bot_pipeline.utils.http_utils import (
    get_status_category,
    is_error_status,
    is_success_status,
)


class TestGetStatusCategory:
    """Tests for get_status_category function."""

    def test_200_success(self):
        """200 OK should be 2xx_success."""
        assert get_status_category(200) == "2xx_success"

    def test_201_success(self):
        """201 Created should be 2xx_success."""
        assert get_status_category(201) == "2xx_success"

    def test_299_success(self):
        """299 should be 2xx_success."""
        assert get_status_category(299) == "2xx_success"

    def test_301_redirect(self):
        """301 Moved Permanently should be 3xx_redirect."""
        assert get_status_category(301) == "3xx_redirect"

    def test_302_redirect(self):
        """302 Found should be 3xx_redirect."""
        assert get_status_category(302) == "3xx_redirect"

    def test_304_redirect(self):
        """304 Not Modified should be 3xx_redirect."""
        assert get_status_category(304) == "3xx_redirect"

    def test_400_client_error(self):
        """400 Bad Request should be 4xx_client_error."""
        assert get_status_category(400) == "4xx_client_error"

    def test_403_client_error(self):
        """403 Forbidden should be 4xx_client_error."""
        assert get_status_category(403) == "4xx_client_error"

    def test_404_client_error(self):
        """404 Not Found should be 4xx_client_error."""
        assert get_status_category(404) == "4xx_client_error"

    def test_429_client_error(self):
        """429 Too Many Requests should be 4xx_client_error."""
        assert get_status_category(429) == "4xx_client_error"

    def test_500_server_error(self):
        """500 Internal Server Error should be 5xx_server_error."""
        assert get_status_category(500) == "5xx_server_error"

    def test_502_server_error(self):
        """502 Bad Gateway should be 5xx_server_error."""
        assert get_status_category(502) == "5xx_server_error"

    def test_503_server_error(self):
        """503 Service Unavailable should be 5xx_server_error."""
        assert get_status_category(503) == "5xx_server_error"


class TestEdgeCases:
    """Tests for edge cases in status categorization."""

    def test_none_returns_none(self):
        """None status code should return None."""
        assert get_status_category(None) is None

    def test_invalid_low_returns_none(self):
        """Status below 200 should return None."""
        assert get_status_category(100) is None
        assert get_status_category(199) is None

    def test_invalid_high_returns_none(self):
        """Status 600+ should return None."""
        assert get_status_category(600) is None
        assert get_status_category(999) is None


class TestIsSuccessStatus:
    """Tests for is_success_status function."""

    def test_200_is_success(self):
        """200 should be success."""
        assert is_success_status(200) is True

    def test_204_is_success(self):
        """204 No Content should be success."""
        assert is_success_status(204) is True

    def test_301_not_success(self):
        """301 redirect is not success."""
        assert is_success_status(301) is False

    def test_404_not_success(self):
        """404 is not success."""
        assert is_success_status(404) is False

    def test_500_not_success(self):
        """500 is not success."""
        assert is_success_status(500) is False

    def test_none_not_success(self):
        """None is not success."""
        assert is_success_status(None) is False


class TestIsErrorStatus:
    """Tests for is_error_status function."""

    def test_400_is_error(self):
        """400 should be error."""
        assert is_error_status(400) is True

    def test_404_is_error(self):
        """404 should be error."""
        assert is_error_status(404) is True

    def test_500_is_error(self):
        """500 should be error."""
        assert is_error_status(500) is True

    def test_503_is_error(self):
        """503 should be error."""
        assert is_error_status(503) is True

    def test_200_not_error(self):
        """200 is not error."""
        assert is_error_status(200) is False

    def test_301_not_error(self):
        """301 redirect is not error."""
        assert is_error_status(301) is False

    def test_none_not_error(self):
        """None is not error."""
        assert is_error_status(None) is False

