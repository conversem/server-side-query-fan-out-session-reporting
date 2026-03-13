"""
Data quality monitoring — backward-compatibility shim.

All logic lives in the ``checkers`` sub-package.
"""

from .checkers import (  # noqa: F401
    DataQualityChecker,
    DataQualityReport,
    DuplicateChecker,
    FreshnessChecker,
    QualityCheckResult,
    QualityStatus,
    RecordCountChecker,
    SchemaChecker,
)

__all__ = [
    "DataQualityChecker",
    "DataQualityReport",
    "DuplicateChecker",
    "FreshnessChecker",
    "QualityCheckResult",
    "QualityStatus",
    "RecordCountChecker",
    "SchemaChecker",
]
