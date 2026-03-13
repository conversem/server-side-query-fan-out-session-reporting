"""
Schema checker for data quality monitoring.

Validates data schema and field completeness.
Checks for null values in required fields and value range issues.
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


class SchemaChecker:
    """
    Validates data schema and field completeness.

    Checks for null values in required fields and data type issues.
    Works with any ``StorageBackend`` implementation.
    """

    def __init__(
        self,
        backend: "StorageBackend",
        max_null_pct: float = 5.0,
    ):
        self.backend = backend
        self.max_null_pct = max_null_pct

    def check_required_fields(
        self,
        table_id: str,
        required_fields: list[str],
        check_date: Optional[date] = None,
        date_column: str = "request_date",
    ) -> QualityCheckResult:
        """Check that required fields have acceptable null rates."""
        table_id = _validate_identifier(table_id, "table_id")
        date_column = _validate_identifier(date_column, "date_column")
        required_fields = [
            _validate_identifier(f, "required_field") for f in required_fields
        ]

        null_checks = ", ".join(
            [
                f"SUM(CASE WHEN {fld} IS NULL THEN 1 ELSE 0 END) as null_{fld}"
                for fld in required_fields
            ]
        )

        where_clause = ""
        if check_date:
            where_clause = f"WHERE {date_column} = '{check_date.isoformat()}'"

        query = f"""
        SELECT
            COUNT(*) as total_rows,
            {null_checks}
        FROM {table_id}
        {where_clause}
        """

        try:
            rows = self.backend.query(query)
            row = rows[0] if rows else {}
            total = row.get("total_rows", 1) or 1

            null_rates = {}
            issues = []

            for fld in required_fields:
                null_count = row.get(f"null_{fld}", 0) or 0
                null_pct = 100 * null_count / total
                null_rates[fld] = {
                    "null_count": null_count,
                    "null_pct": round(null_pct, 2),
                }
                if null_pct > self.max_null_pct:
                    issues.append(f"{fld}: {null_pct:.1f}% null")

            details = {
                "table_id": table_id,
                "check_date": (check_date.isoformat() if check_date else "all"),
                "total_rows": total,
                "null_rates": null_rates,
                "max_null_pct": self.max_null_pct,
            }

            if issues:
                return QualityCheckResult(
                    check_name="required_fields",
                    status=QualityStatus.FAIL,
                    message=f"High null rates: {', '.join(issues)}",
                    details=details,
                )
            else:
                return QualityCheckResult(
                    check_name="required_fields",
                    status=QualityStatus.PASS,
                    message="All required fields have acceptable null rates",
                    details=details,
                )

        except Exception as e:
            return _error_result("required_fields", e, {"table_id": table_id})

    def check_value_ranges(
        self,
        table_id: str,
        range_checks: dict[str, tuple],
        check_date: Optional[date] = None,
        date_column: str = "request_date",
    ) -> QualityCheckResult:
        """Check that numeric fields are within expected ranges."""
        table_id = _validate_identifier(table_id, "table_id")
        date_column = _validate_identifier(date_column, "date_column")
        for field_name in range_checks:
            _validate_identifier(field_name, "range_check_field")

        range_clauses = []
        for fld, (min_val, max_val) in range_checks.items():
            range_clauses.append(
                f"SUM(CASE WHEN {fld} < {min_val} OR {fld} > {max_val} THEN 1 ELSE 0 END) as invalid_{fld}"
            )

        where_clause = ""
        if check_date:
            where_clause = f"WHERE {date_column} = '{check_date.isoformat()}'"

        query = f"""
        SELECT
            COUNT(*) as total_rows,
            {', '.join(range_clauses)}
        FROM {table_id}
        {where_clause}
        """

        try:
            rows = self.backend.query(query)
            row = rows[0] if rows else {}
            total = row.get("total_rows", 1) or 1

            issues = []
            range_results = {}

            for fld in range_checks:
                invalid_count = row.get(f"invalid_{fld}", 0) or 0
                invalid_pct = 100 * invalid_count / total
                range_results[fld] = {
                    "invalid_count": invalid_count,
                    "invalid_pct": round(invalid_pct, 2),
                    "expected_range": range_checks[fld],
                }
                if invalid_count > 0:
                    issues.append(f"{fld}: {invalid_count:,} out of range")

            details = {
                "table_id": table_id,
                "check_date": (check_date.isoformat() if check_date else "all"),
                "total_rows": total,
                "range_results": range_results,
            }

            if issues:
                return QualityCheckResult(
                    check_name="value_ranges",
                    status=QualityStatus.WARN,
                    message=f"Values out of range: {', '.join(issues)}",
                    details=details,
                )
            else:
                return QualityCheckResult(
                    check_name="value_ranges",
                    status=QualityStatus.PASS,
                    message="All values within expected ranges",
                    details=details,
                )

        except Exception as e:
            return _error_result("value_ranges", e, {"table_id": table_id})
