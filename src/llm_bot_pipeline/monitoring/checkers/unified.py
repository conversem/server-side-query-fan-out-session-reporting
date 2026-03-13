"""
Unified data quality checker.

Combines all checker classes into a single interface for
comprehensive data quality validation.
"""

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING, Optional

from ...config.constants import DATASET_RAW, DATASET_REPORT
from .duplicate import DuplicateChecker
from .freshness import FreshnessChecker
from .models import DataQualityReport, QualityCheckResult
from .record_count import RecordCountChecker
from .schema import SchemaChecker

if TYPE_CHECKING:
    from ...storage.base import StorageBackend

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """
    Unified data quality checker combining all quality checks.

    Provides a single interface for comprehensive data quality validation.
    Works with any ``StorageBackend`` implementation.
    """

    CLEAN_TABLE_REQUIRED_FIELDS = [
        "request_timestamp",
        "request_date",
        "bot_name",
        "bot_provider",
        "bot_category",
        "response_status",
    ]

    CLEAN_TABLE_KEY_FIELDS = [
        "request_timestamp",
        "request_uri",
        "request_host",
        "bot_name",
    ]

    CLEAN_TABLE_RANGES = {
        "request_hour": (0, 23),
        "response_status": (100, 599),
        "bot_score": (1, 99),
    }

    def __init__(
        self,
        backend: "StorageBackend",
        table_name: str = "bot_requests_daily",
        dataset_raw: str = DATASET_RAW,
        dataset_report: str = DATASET_REPORT,
    ):
        self.backend = backend
        self.table_name = table_name
        self.dataset_raw = dataset_raw
        self.dataset_report = dataset_report

        self.freshness_checker = FreshnessChecker(backend=backend)
        self.count_checker = RecordCountChecker(backend=backend)
        self.schema_checker = SchemaChecker(backend=backend)
        self.duplicate_checker = DuplicateChecker(backend=backend)

    def run_all_checks(
        self,
        table_name: Optional[str] = None,
        check_date: Optional[date] = None,
        skip_variance: bool = False,
    ) -> DataQualityReport:
        """
        Run all quality checks on a table.

        Args:
            table_name: Table name (without project/dataset prefix).
                Falls back to the instance default.
            check_date: Date to check (defaults to yesterday).
            skip_variance: Skip variance check (useful for new tables).

        Returns:
            DataQualityReport with all check results.
        """
        if table_name is None:
            table_name = self.table_name

        if check_date is None:
            check_date = date.today() - timedelta(days=1)

        table_id = self.backend.get_full_table_id(table_name)
        results: list[QualityCheckResult] = []

        logger.info(f"Running data quality checks for {table_id} on {check_date}")

        results.append(self.freshness_checker.check_table_freshness(table_id))

        results.append(
            self.freshness_checker.check_date_coverage(
                table_id=table_id,
                expected_start_date=check_date - timedelta(days=6),
                expected_end_date=check_date,
            )
        )

        results.append(
            self.count_checker.check_daily_counts(
                table_id=table_id,
                check_date=check_date,
            )
        )

        if not skip_variance:
            results.append(
                self.count_checker.check_count_variance(
                    table_id=table_id,
                    check_date=check_date,
                )
            )

        results.append(
            self.schema_checker.check_required_fields(
                table_id=table_id,
                required_fields=self.CLEAN_TABLE_REQUIRED_FIELDS,
                check_date=check_date,
            )
        )

        results.append(
            self.schema_checker.check_value_ranges(
                table_id=table_id,
                range_checks=self.CLEAN_TABLE_RANGES,
                check_date=check_date,
            )
        )

        results.append(
            self.duplicate_checker.check_duplicates(
                table_id=table_id,
                key_fields=self.CLEAN_TABLE_KEY_FIELDS,
                check_date=check_date,
            )
        )

        report = DataQualityReport(
            table_name=table_name,
            check_date=check_date,
            results=results,
        )

        logger.info(
            f"Quality check complete: {report.summary['passed']} passed, "
            f"{report.summary['warnings']} warnings, "
            f"{report.summary['failed']} failed"
        )

        return report
