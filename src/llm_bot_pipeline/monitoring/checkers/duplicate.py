"""
Duplicate checker for data quality monitoring.

Detects duplicate records in pipeline data based on key fields.
"""

from datetime import date
from typing import TYPE_CHECKING, Optional

from .models import (
    QualityCheckResult,
    QualityStatus,
    _error_result,
    _validate_identifier,
)

if TYPE_CHECKING:
    from ...storage.base import StorageBackend


class DuplicateChecker:
    """
    Detects duplicate records in pipeline data.

    Identifies exact duplicates based on key fields.
    Works with any ``StorageBackend`` implementation.
    """

    def __init__(
        self,
        backend: "StorageBackend",
        max_duplicate_pct: float = 1.0,
    ):
        self.backend = backend
        self.max_duplicate_pct = max_duplicate_pct

    def check_duplicates(
        self,
        table_id: str,
        key_fields: list[str],
        check_date: Optional[date] = None,
        date_column: str = "request_date",
    ) -> QualityCheckResult:
        """Check for duplicate records based on key fields."""
        table_id = _validate_identifier(table_id, "table_id")
        date_column = _validate_identifier(date_column, "date_column")
        key_fields = [_validate_identifier(f, "key_field") for f in key_fields]

        key_list = ", ".join(key_fields)

        where_clause = ""
        if check_date:
            where_clause = f"WHERE {date_column} = '{check_date.isoformat()}'"

        query = f"""
        WITH counts AS (
            SELECT
                {key_list},
                COUNT(*) as occurrence_count
            FROM {table_id}
            {where_clause}
            GROUP BY {key_list}
        )
        SELECT
            COUNT(*) as unique_combinations,
            SUM(occurrence_count) as total_rows,
            SUM(CASE WHEN occurrence_count > 1 THEN occurrence_count - 1 ELSE 0 END) as duplicate_rows,
            MAX(occurrence_count) as max_occurrences
        FROM counts
        """

        try:
            rows = self.backend.query(query)
            row = rows[0] if rows else {}

            total = row.get("total_rows", 1) or 1
            duplicates = row.get("duplicate_rows", 0) or 0
            duplicate_pct = 100 * duplicates / total

            details = {
                "table_id": table_id,
                "check_date": (check_date.isoformat() if check_date else "all"),
                "key_fields": key_fields,
                "total_rows": total,
                "unique_combinations": row.get("unique_combinations", 0) or 0,
                "duplicate_rows": duplicates,
                "duplicate_pct": round(duplicate_pct, 2),
                "max_occurrences": row.get("max_occurrences", 1) or 1,
                "max_duplicate_pct": self.max_duplicate_pct,
            }

            if duplicate_pct > self.max_duplicate_pct:
                return QualityCheckResult(
                    check_name="duplicates",
                    status=QualityStatus.FAIL,
                    message=f"High duplicate rate: {duplicate_pct:.2f}% ({duplicates:,} duplicates)",
                    details=details,
                )
            elif duplicates > 0:
                return QualityCheckResult(
                    check_name="duplicates",
                    status=QualityStatus.WARN,
                    message=f"Some duplicates found: {duplicate_pct:.2f}% ({duplicates:,} duplicates)",
                    details=details,
                )
            else:
                return QualityCheckResult(
                    check_name="duplicates",
                    status=QualityStatus.PASS,
                    message="No duplicates found",
                    details=details,
                )

        except Exception as e:
            return _error_result("duplicates", e, {"table_id": table_id})
