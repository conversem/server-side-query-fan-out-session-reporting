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

    @pytest.mark.parametrize(
        "status_code,category",
        [
            (200, "2xx_success"),
            (201, "2xx_success"),
            (299, "2xx_success"),
            (301, "3xx_redirect"),
            (302, "3xx_redirect"),
            (304, "3xx_redirect"),
            (400, "4xx_client_error"),
            (403, "4xx_client_error"),
            (404, "4xx_client_error"),
            (429, "4xx_client_error"),
            (500, "5xx_server_error"),
            (502, "5xx_server_error"),
            (503, "5xx_server_error"),
        ],
    )
    def test_status_category(self, status_code, category):
        """Status code should map to expected category."""
        assert get_status_category(status_code) == category


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

    @pytest.mark.parametrize("status_code", [200, 204])
    def test_success_status_true(self, status_code):
        """2xx status codes should be success."""
        assert is_success_status(status_code) is True

    @pytest.mark.parametrize("status_code", [301, 404, 500])
    def test_success_status_false(self, status_code):
        """Non-2xx status codes should not be success."""
        assert is_success_status(status_code) is False

    def test_none_not_success(self):
        """None is not success."""
        assert is_success_status(None) is False


class TestIsErrorStatus:
    """Tests for is_error_status function."""

    @pytest.mark.parametrize("status_code", [400, 404, 500, 503])
    def test_error_status_true(self, status_code):
        """4xx and 5xx status codes should be error."""
        assert is_error_status(status_code) is True

    @pytest.mark.parametrize("status_code", [200, 301])
    def test_error_status_false(self, status_code):
        """2xx and 3xx status codes should not be error."""
        assert is_error_status(status_code) is False

    def test_none_not_error(self):
        """None is not error."""
        assert is_error_status(None) is False
