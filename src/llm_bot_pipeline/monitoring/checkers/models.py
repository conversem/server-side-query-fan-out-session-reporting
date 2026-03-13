"""
Data quality models and shared helpers.

Defines QualityStatus, QualityCheckResult, DataQualityReport,
and helper utilities used across all checker classes.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.`-]*$")

_TRANSIENT_KEYWORDS = (
    "timeout",
    "deadline exceeded",
    "503",
    "unavailable",
    "retry",
)


def _validate_identifier(value: str, name: str) -> str:
    """Validate SQL identifier to prevent injection."""
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(f"Invalid SQL identifier for {name}: {value!r}")
    return value


def _is_transient(error: Exception) -> bool:
    error_str = str(error).lower()
    return any(kw in error_str for kw in _TRANSIENT_KEYWORDS)


class QualityStatus(Enum):
    """Data quality check status."""

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class QualityCheckResult:
    """Result of a single quality check."""

    check_name: str
    status: QualityStatus
    message: str
    details: dict = field(default_factory=dict)
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "check_name": self.check_name,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
            "checked_at": self.checked_at.isoformat(),
        }


@dataclass
class DataQualityReport:
    """Complete data quality report with all check results."""

    table_name: str
    check_date: date
    results: list[QualityCheckResult] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def overall_status(self) -> QualityStatus:
        """Get overall status based on all check results."""
        if any(r.status == QualityStatus.FAIL for r in self.results):
            return QualityStatus.FAIL
        if any(r.status == QualityStatus.WARN for r in self.results):
            return QualityStatus.WARN
        if all(r.status == QualityStatus.SKIP for r in self.results):
            return QualityStatus.SKIP
        return QualityStatus.PASS

    @property
    def passed(self) -> bool:
        """Check if all critical checks passed."""
        return self.overall_status != QualityStatus.FAIL

    @property
    def summary(self) -> dict:
        """Get summary of check results."""
        return {
            "total_checks": len(self.results),
            "passed": sum(1 for r in self.results if r.status == QualityStatus.PASS),
            "warnings": sum(1 for r in self.results if r.status == QualityStatus.WARN),
            "failed": sum(1 for r in self.results if r.status == QualityStatus.FAIL),
            "skipped": sum(1 for r in self.results if r.status == QualityStatus.SKIP),
        }

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "table_name": self.table_name,
            "check_date": self.check_date.isoformat(),
            "overall_status": self.overall_status.value,
            "summary": self.summary,
            "results": [r.to_dict() for r in self.results],
            "created_at": self.created_at.isoformat(),
        }


def _error_result(
    check_name: str, error: Exception, extra_details: Optional[dict] = None
) -> QualityCheckResult:
    """Build a standardised error result for a failed check."""
    transient = _is_transient(error)
    status = QualityStatus.SKIP if transient else QualityStatus.FAIL
    verb = "timed out" if transient else "failed"
    logger.exception(f"{check_name} check {verb}: {error}")
    details = dict(extra_details or {})
    details["error"] = str(error)
    return QualityCheckResult(
        check_name=check_name,
        status=status,
        message=f"{check_name} check {verb}: {error}",
        details=details,
    )
