"""
Freshness checker for data quality monitoring.

Validates data recency — checks that data has been updated
within acceptable time windows.
"""

from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from .models import (
    QualityCheckResult,
    QualityStatus,
    _error_result,
    _validate_identifier,
)

if TYPE_CHECKING:
    from ...storage.base import StorageBackend


class FreshnessChecker:
    """
    Validates data freshness (recency).

    Checks that data has been updated within acceptable time windows.
    Works with any ``StorageBackend`` implementation.
    """

    def __init__(
        self,
        backend: "StorageBackend",
        max_staleness_hours: int = 24,
        warn_staleness_hours: int = 12,
    ):
        self.backend = backend
        self.max_staleness_hours = max_staleness_hours
        self.warn_staleness_hours = warn_staleness_hours

    def check_table_freshness(
        self,
        table_id: str,
        timestamp_column: str = "request_timestamp",
    ) -> QualityCheckResult:
        """Check freshness of a table based on latest timestamp."""
        table_id = _validate_identifier(table_id, "table_id")
        timestamp_column = _validate_identifier(timestamp_column, "timestamp_column")

        query = f"""
        SELECT
            MAX({timestamp_column}) as latest_timestamp,
            MIN({timestamp_column}) as earliest_timestamp,
            COUNT(*) as row_count
        FROM {table_id}
        """

        try:
            rows = self.backend.query(query)
            row = rows[0] if rows else {}

            row_count = row.get("row_count", 0) or 0
            latest = row.get("latest_timestamp")

            if row_count == 0 or latest is None:
                return QualityCheckResult(
                    check_name="freshness",
                    status=QualityStatus.FAIL,
                    message="No data found in table",
                    details={"table_id": table_id, "row_count": 0},
                )

            if isinstance(latest, str):
                latest = datetime.fromisoformat(latest)

            now = datetime.now(timezone.utc)
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)

            hours_since_update = (now - latest).total_seconds() / 3600

            details = {
                "table_id": table_id,
                "latest_timestamp": latest.isoformat(),
                "hours_since_update": round(hours_since_update, 2),
                "row_count": row_count,
                "max_staleness_hours": self.max_staleness_hours,
                "warn_staleness_hours": self.warn_staleness_hours,
            }

            if hours_since_update > self.max_staleness_hours:
                return QualityCheckResult(
                    check_name="freshness",
                    status=QualityStatus.FAIL,
                    message=f"Data is stale: {hours_since_update:.1f}h since last update",
                    details=details,
                )
            elif hours_since_update > self.warn_staleness_hours:
                return QualityCheckResult(
                    check_name="freshness",
                    status=QualityStatus.WARN,
                    message=f"Data approaching staleness: {hours_since_update:.1f}h since last update",
                    details=details,
                )
            else:
                return QualityCheckResult(
                    check_name="freshness",
                    status=QualityStatus.PASS,
                    message=f"Data is fresh: {hours_since_update:.1f}h since last update",
                    details=details,
                )

        except Exception as e:
            return _error_result("freshness", e, {"table_id": table_id})

    def check_date_coverage(
        self,
        table_id: str,
        expected_start_date: date,
        expected_end_date: date,
        date_column: str = "request_date",
    ) -> QualityCheckResult:
        """Check that data covers expected date range."""
        table_id = _validate_identifier(table_id, "table_id")
        date_column = _validate_identifier(date_column, "date_column")

        query = f"""
        SELECT
            MIN({date_column}) as min_date,
            MAX({date_column}) as max_date,
            COUNT(DISTINCT {date_column}) as distinct_dates
        FROM {table_id}
        WHERE {date_column} >= '{expected_start_date.isoformat()}'
          AND {date_column} <= '{expected_end_date.isoformat()}'
        """

        try:
            rows = self.backend.query(query)
            row = rows[0] if rows else {}

            expected_days = (expected_end_date - expected_start_date).days + 1
            actual_days = row.get("distinct_dates", 0) or 0
            min_date = row.get("min_date")
            max_date = row.get("max_date")

            details = {
                "table_id": table_id,
                "expected_start": expected_start_date.isoformat(),
                "expected_end": expected_end_date.isoformat(),
                "actual_min_date": (
                    min_date.isoformat()
                    if hasattr(min_date, "isoformat")
                    else str(min_date) if min_date else None
                ),
                "actual_max_date": (
                    max_date.isoformat()
                    if hasattr(max_date, "isoformat")
                    else str(max_date) if max_date else None
                ),
                "expected_days": expected_days,
                "actual_days": actual_days,
                "coverage_pct": (
                    round(100 * actual_days / expected_days, 2)
                    if expected_days > 0
                    else 0
                ),
            }

            if actual_days == 0:
                return QualityCheckResult(
                    check_name="date_coverage",
                    status=QualityStatus.FAIL,
                    message="No data found for expected date range",
                    details=details,
                )
            elif actual_days < expected_days * 0.8:
                return QualityCheckResult(
                    check_name="date_coverage",
                    status=QualityStatus.WARN,
                    message=f"Incomplete date coverage: {actual_days}/{expected_days} days",
                    details=details,
                )
            else:
                return QualityCheckResult(
                    check_name="date_coverage",
                    status=QualityStatus.PASS,
                    message=f"Date coverage OK: {actual_days}/{expected_days} days",
                    details=details,
                )

        except Exception as e:
            return _error_result("date_coverage", e, {"table_id": table_id})
