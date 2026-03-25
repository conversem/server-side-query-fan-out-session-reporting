"""Unit tests for data quality monitoring module."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from llm_bot_pipeline.monitoring.data_quality import (
    DataQualityChecker,
    DataQualityReport,
    FreshnessChecker,
    QualityCheckResult,
    QualityStatus,
)


def _make_backend():
    """Create a mock storage backend."""
    backend = MagicMock()
    backend.get_full_table_id.return_value = "project.dataset.bot_requests_daily"
    return backend


class TestFreshnessChecker:
    """Tests for FreshnessChecker."""

    def test_check_date_coverage_detects_gaps(self):
        """Feed data with date gap, assert flagged (WARN)."""
        backend = _make_backend()
        # Expected 7 days (e.g. check_date - 6 to check_date), actual 4 days
        backend.query.return_value = [
            {
                "min_date": date(2025, 1, 10),
                "max_date": date(2025, 1, 15),
                "distinct_dates": 4,
            }
        ]

        checker = FreshnessChecker(backend=backend)
        result = checker.check_date_coverage(
            table_id="project.dataset.bot_requests_daily",
            expected_start_date=date(2025, 1, 10),
            expected_end_date=date(2025, 1, 16),
        )

        assert result.status == QualityStatus.WARN
        assert "Incomplete date coverage" in result.message
        assert result.details["actual_days"] == 4
        assert result.details["expected_days"] == 7

    def test_check_date_coverage_full_coverage_passes(self):
        """Full date coverage returns PASS."""
        backend = _make_backend()
        backend.query.return_value = [
            {
                "min_date": date(2025, 1, 10),
                "max_date": date(2025, 1, 16),
                "distinct_dates": 7,
            }
        ]

        checker = FreshnessChecker(backend=backend)
        result = checker.check_date_coverage(
            table_id="project.dataset.bot_requests_daily",
            expected_start_date=date(2025, 1, 10),
            expected_end_date=date(2025, 1, 16),
        )

        assert result.status == QualityStatus.PASS
        assert "Date coverage OK" in result.message

    def test_check_date_coverage_no_data_fails(self):
        """No data for expected range returns FAIL."""
        backend = _make_backend()
        backend.query.return_value = [
            {"min_date": None, "max_date": None, "distinct_dates": 0}
        ]

        checker = FreshnessChecker(backend=backend)
        result = checker.check_date_coverage(
            table_id="project.dataset.bot_requests_daily",
            expected_start_date=date(2025, 1, 10),
            expected_end_date=date(2025, 1, 16),
        )

        assert result.status == QualityStatus.FAIL
        assert "No data found" in result.message


class TestDataQualityChecker:
    """Tests for DataQualityChecker."""

    def test_data_quality_checker_detects_gaps(self):
        """Feed data with date gap, assert report flags it."""
        backend = _make_backend()
        check_date = date(2025, 1, 16)
        recent = datetime.now(timezone.utc) - timedelta(hours=1)

        # Order: freshness, date_coverage, daily_counts, required_fields, value_ranges, duplicates
        backend.query.side_effect = [
            [
                {
                    "latest_timestamp": recent,
                    "earliest_timestamp": recent,
                    "row_count": 1000,
                }
            ],
            [
                {
                    "min_date": date(2025, 1, 10),
                    "max_date": check_date,
                    "distinct_dates": 4,
                }
            ],
            [{"record_count": 500}],
            [
                {
                    "total_rows": 500,
                    "null_request_timestamp": 0,
                    "null_request_date": 0,
                    "null_bot_name": 0,
                    "null_bot_provider": 0,
                    "null_bot_category": 0,
                    "null_response_status": 0,
                }
            ],
            [
                {
                    "total_rows": 500,
                    "invalid_request_hour": 0,
                    "invalid_response_status": 0,
                }
            ],
            [
                {
                    "unique_combinations": 500,
                    "total_rows": 500,
                    "duplicate_rows": 0,
                    "max_occurrences": 1,
                }
            ],
        ]

        checker = DataQualityChecker(backend=backend)
        report = checker.run_all_checks(check_date=check_date, skip_variance=True)

        assert isinstance(report, DataQualityReport)
        date_coverage_results = [
            r for r in report.results if r.check_name == "date_coverage"
        ]
        assert len(date_coverage_results) == 1
        assert date_coverage_results[0].status == QualityStatus.WARN
        assert "Incomplete date coverage" in date_coverage_results[0].message
        assert report.overall_status == QualityStatus.WARN


class TestQualityCheckResult:
    """Tests for QualityCheckResult and DataQualityReport."""

    def test_quality_check_result_to_dict(self):
        """QualityCheckResult serializes to dict."""
        result = QualityCheckResult(
            check_name="freshness",
            status=QualityStatus.PASS,
            message="OK",
            details={"hours": 2},
        )
        d = result.to_dict()
        assert d["check_name"] == "freshness"
        assert d["status"] == "pass"
        assert d["details"]["hours"] == 2

    def test_data_quality_report_overall_status(self):
        """Report overall_status reflects worst result."""
        results = [
            QualityCheckResult("a", QualityStatus.PASS, "ok"),
            QualityCheckResult("b", QualityStatus.WARN, "warn"),
        ]
        report = DataQualityReport(
            table_name="t", check_date=date.today(), results=results
        )
        assert report.overall_status == QualityStatus.WARN
        assert report.passed is True

    def test_data_quality_report_fail_overrides_warn(self):
        """FAIL overrides WARN in overall status."""
        results = [
            QualityCheckResult("a", QualityStatus.WARN, "warn"),
            QualityCheckResult("b", QualityStatus.FAIL, "fail"),
        ]
        report = DataQualityReport(
            table_name="t", check_date=date.today(), results=results
        )
        assert report.overall_status == QualityStatus.FAIL
        assert report.passed is False
