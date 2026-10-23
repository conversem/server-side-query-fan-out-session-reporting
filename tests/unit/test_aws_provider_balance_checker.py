"""Unit tests for AwsProviderBalanceChecker.

Verifies configured AWS domains are compared against raw_bot_requests row
counts for the check date. Zero or missing counts are FAIL; deployments
with no AWS domains in config return PASS without querying.
"""

from datetime import date
from unittest.mock import MagicMock

from llm_bot_pipeline.monitoring.checkers.aws_provider_balance import (
    AwsProviderBalanceChecker,
)
from llm_bot_pipeline.monitoring.checkers.models import QualityStatus


def _backend_with_rows(rows: list[dict]):
    backend = MagicMock()
    backend.query.return_value = rows
    return backend


def test_ok_when_aws_domain_has_rows():
    backend = _backend_with_rows(
        [
            {"source_provider": "aws_cloudfront", "domain": "x.com", "rows": 12345},
        ]
    )
    checker = AwsProviderBalanceChecker(
        backend=backend,
        expected_aws_pairs=[("x.com", "aws_cloudfront")],
    )
    result = checker.check_balance(check_date=date(2026, 5, 21))
    assert result.status == QualityStatus.PASS


def test_alert_when_aws_domain_missing_from_bigquery():
    """No GROUP BY row means zero rows for that domain on the check date."""
    backend = _backend_with_rows([])
    checker = AwsProviderBalanceChecker(
        backend=backend,
        expected_aws_pairs=[("x.com", "aws_cloudfront")],
    )
    result = checker.check_balance(check_date=date(2026, 5, 21))
    assert result.status == QualityStatus.FAIL
    assert "x.com" in result.details["domains_with_zero"]


def test_ok_when_no_aws_domains_configured():
    """A pure-CF deployment must not query or raise an alert."""
    backend = _backend_with_rows(
        [
            {"source_provider": "cloudflare", "domain": "x.com", "rows": 1000},
        ]
    )
    checker = AwsProviderBalanceChecker(backend=backend, expected_aws_pairs=[])
    result = checker.check_balance(check_date=date(2026, 5, 21))
    assert result.status == QualityStatus.PASS
    backend.query.assert_not_called()


def test_partial_aws_domain_failure():
    backend = _backend_with_rows(
        [
            {"source_provider": "aws_cloudfront", "domain": "ok.com", "rows": 500},
        ]
    )
    checker = AwsProviderBalanceChecker(
        backend=backend,
        expected_aws_pairs=[
            ("ok.com", "aws_cloudfront"),
            ("missing.com", "aws_alb"),
        ],
    )
    result = checker.check_balance(check_date=date(2026, 5, 21))
    assert result.status == QualityStatus.FAIL
    assert "missing.com" in result.details["domains_with_zero"]
    assert "ok.com" not in result.details["domains_with_zero"]


class TestRowCountRollingMedian:
    """D9 — spec §6: row count per (domain, source_provider) within ±50% of
    7-day rolling median, WARN outside that band."""

    def test_within_band_returns_ok(self):
        backend = MagicMock()
        backend.query.return_value = [
            {
                "source_provider": "aws_cloudfront",
                "domain": "x.com",
                "rows_today": 1000,
                "rolling_median_7d": 1000,
            },
        ]
        checker = AwsProviderBalanceChecker(backend=backend)
        result = checker.check_row_count_band(check_date=date(2026, 5, 21))
        assert result.status == QualityStatus.PASS

    def test_above_band_returns_warning(self):
        backend = MagicMock()
        backend.query.return_value = [
            {
                "source_provider": "aws_cloudfront",
                "domain": "x.com",
                "rows_today": 1600,
                "rolling_median_7d": 1000,
            },
        ]
        checker = AwsProviderBalanceChecker(backend=backend)
        result = checker.check_row_count_band(check_date=date(2026, 5, 21))
        assert result.status == QualityStatus.WARN
        assert "x.com" in result.message

    def test_below_band_returns_warning(self):
        backend = MagicMock()
        backend.query.return_value = [
            {
                "source_provider": "aws_cloudfront",
                "domain": "x.com",
                "rows_today": 400,
                "rolling_median_7d": 1000,
            },
        ]
        checker = AwsProviderBalanceChecker(backend=backend)
        result = checker.check_row_count_band(check_date=date(2026, 5, 21))
        assert result.status == QualityStatus.WARN

    def test_no_history_returns_ok(self):
        """A new AWS domain with no 7-day history shouldn't flap-WARN on day 1."""
        backend = MagicMock()
        backend.query.return_value = [
            {
                "source_provider": "aws_cloudfront",
                "domain": "x.com",
                "rows_today": 1000,
                "rolling_median_7d": None,
            },
        ]
        checker = AwsProviderBalanceChecker(backend=backend)
        result = checker.check_row_count_band(check_date=date(2026, 5, 21))
        assert result.status == QualityStatus.PASS


def test_dedup_ratio_under_threshold_returns_ok():
    backend = _backend_with_rows(
        [
            {
                "source_provider": "aws_cloudfront",
                "domain": "x.com",
                "raw": 1000,
                "unique": 990,
                "ratio": 0.01,
            },
        ]
    )
    checker = AwsProviderBalanceChecker(backend=backend)
    result = checker.check_dedup_ratio(check_date=date(2026, 5, 21), threshold=0.02)
    assert result.status == QualityStatus.PASS


def test_dedup_ratio_over_threshold_returns_warning():
    backend = _backend_with_rows(
        [
            {
                "source_provider": "aws_cloudfront",
                "domain": "x.com",
                "raw": 1000,
                "unique": 950,
                "ratio": 0.05,
            },
        ]
    )
    checker = AwsProviderBalanceChecker(backend=backend)
    result = checker.check_dedup_ratio(check_date=date(2026, 5, 21), threshold=0.02)
    assert result.status == QualityStatus.WARN
    assert "0.05" in result.message or "5" in result.message
