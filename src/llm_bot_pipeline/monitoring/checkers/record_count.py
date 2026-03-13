"""
Record count checker for data quality monitoring.

Validates record counts against expected thresholds.
Detects anomalies like sudden drops or spikes in data volume.
"""

from datetime import date, timedelta
from typing import TYPE_CHECKING, Optional

from .models import (
    QualityCheckResult,
    QualityStatus,
    _error_result,
    _validate_identifier,
)

if TYPE_CHECKING:
    from ...storage.base import StorageBackend


class RecordCountChecker:
    """
    Validates record counts against expected thresholds.

    Detects anomalies like sudden drops or spikes in data volume.
    Works with any ``StorageBackend`` implementation.
    """

    def __init__(
        self,
        backend: "StorageBackend",
        min_daily_records: int = 100,
        max_daily_records: Optional[int] = None,
        variance_threshold_pct: float = 50.0,
    ):
        self.backend = backend
        self.min_daily_records = min_daily_records
        self.max_daily_records = max_daily_records
        self.variance_threshold_pct = variance_threshold_pct

    def check_daily_counts(
        self,
        table_id: str,
        check_date: date,
        date_column: str = "request_date",
    ) -> QualityCheckResult:
        """Check record count for a specific date."""
        table_id = _validate_identifier(table_id, "table_id")
        date_column = _validate_identifier(date_column, "date_column")

        query = f"""
        SELECT COUNT(*) as record_count
        FROM {table_id}
        WHERE {date_column} = '{check_date.isoformat()}'
        """

        try:
            rows = self.backend.query(query)
            count = (rows[0].get("record_count", 0) or 0) if rows else 0

            details = {
                "table_id": table_id,
                "check_date": check_date.isoformat(),
                "record_count": count,
                "min_expected": self.min_daily_records,
                "max_expected": self.max_daily_records,
            }

            if count == 0:
                return QualityCheckResult(
                    check_name="daily_record_count",
                    status=QualityStatus.FAIL,
                    message=f"No records found for {check_date}",
                    details=details,
                )
            elif count < self.min_daily_records:
                return QualityCheckResult(
                    check_name="daily_record_count",
                    status=QualityStatus.WARN,
                    message=f"Low record count: {count:,} (expected >= {self.min_daily_records:,})",
                    details=details,
                )
            elif self.max_daily_records and count > self.max_daily_records:
                return QualityCheckResult(
                    check_name="daily_record_count",
                    status=QualityStatus.WARN,
                    message=f"High record count: {count:,} (expected <= {self.max_daily_records:,})",
                    details=details,
                )
            else:
                return QualityCheckResult(
                    check_name="daily_record_count",
                    status=QualityStatus.PASS,
                    message=f"Record count OK: {count:,}",
                    details=details,
                )

        except Exception as e:
            return _error_result("daily_record_count", e, {"table_id": table_id})

    def check_count_variance(
        self,
        table_id: str,
        check_date: date,
        lookback_days: int = 7,
        date_column: str = "request_date",
    ) -> QualityCheckResult:
        """Check if today's count varies significantly from historical average."""
        table_id = _validate_identifier(table_id, "table_id")
        date_column = _validate_identifier(date_column, "date_column")

        start_date = check_date - timedelta(days=lookback_days)

        query = f"""
        WITH daily_counts AS (
            SELECT
                {date_column} as check_date,
                COUNT(*) as record_count
            FROM {table_id}
            WHERE {date_column} >= '{start_date.isoformat()}'
              AND {date_column} <= '{check_date.isoformat()}'
            GROUP BY {date_column}
        ),
        stats AS (
            SELECT
                AVG(record_count) as avg_count,
                COUNT(record_count) as num_days
            FROM daily_counts
            WHERE check_date < '{check_date.isoformat()}'
        ),
        current_count AS (
            SELECT record_count
            FROM daily_counts
            WHERE check_date = '{check_date.isoformat()}'
        )
        SELECT
            current_count.record_count as today_count,
            stats.avg_count,
            stats.num_days,
            CASE
                WHEN stats.avg_count > 0
                THEN ABS(current_count.record_count - stats.avg_count) / stats.avg_count * 100
                ELSE 0
            END as variance_pct
        FROM current_count
        CROSS JOIN stats
        """

        try:
            rows = self.backend.query(query)

            if not rows:
                return QualityCheckResult(
                    check_name="count_variance",
                    status=QualityStatus.SKIP,
                    message="Insufficient data for variance check",
                    details={
                        "table_id": table_id,
                        "check_date": check_date.isoformat(),
                    },
                )

            row = rows[0]
            today_count = row.get("today_count", 0) or 0
            avg_count = row.get("avg_count", 0) or 0
            variance_pct = row.get("variance_pct", 0) or 0

            details = {
                "table_id": table_id,
                "check_date": check_date.isoformat(),
                "today_count": today_count,
                "avg_count": round(avg_count, 2) if avg_count else 0,
                "num_lookback_days": row.get("num_days", 0) or 0,
                "variance_pct": round(variance_pct, 2),
                "threshold_pct": self.variance_threshold_pct,
                "lookback_days": lookback_days,
            }

            if variance_pct > self.variance_threshold_pct:
                return QualityCheckResult(
                    check_name="count_variance",
                    status=QualityStatus.WARN,
                    message=f"High variance: {variance_pct:.1f}% from average",
                    details=details,
                )
            else:
                return QualityCheckResult(
                    check_name="count_variance",
                    status=QualityStatus.PASS,
                    message=f"Variance OK: {variance_pct:.1f}% from average",
                    details=details,
                )

        except Exception as e:
            return _error_result("count_variance", e, {"table_id": table_id})
