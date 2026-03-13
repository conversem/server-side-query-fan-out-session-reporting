"""
Shared data models for reporting module.

Contains dataclasses used across both BigQuery and local aggregation implementations.
"""

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional


@dataclass
class AggregationResult:
    """
    Result of an aggregation operation.

    Used by both ReportingAggregator (BigQuery) and LocalReportingAggregator (SQLite).

    Attributes:
        success: Whether the aggregation completed successfully
        table_name: Target table name (e.g., 'daily_summary', 'url_performance')
        rows_inserted: Number of rows inserted
        start_date: Start of date range processed
        end_date: End of date range processed
        error: Error message if success is False
        duration_seconds: Time taken for the aggregation
    """

    success: bool
    table_name: str
    rows_inserted: int = 0
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    error: Optional[str] = None
    duration_seconds: float = 0.0

    def __post_init__(self) -> None:
        """Validate aggregation result fields."""
        if not self.table_name or not self.table_name.strip():
            raise ValueError("table_name must be non-empty")
        if self.rows_inserted < 0:
            raise ValueError("rows_inserted must be >= 0")
        if self.duration_seconds < 0:
            raise ValueError("duration_seconds must be >= 0")


@dataclass
class QueryResult:
    """Result of a dashboard query."""

    query_name: str
    rows: list[dict[str, Any]]
    row_count: int
    description: str = ""

    def __post_init__(self) -> None:
        """Validate query result fields."""
        if self.row_count < 0:
            raise ValueError("row_count must be >= 0")
        if self.row_count != len(self.rows):
            raise ValueError(
                f"row_count ({self.row_count}) must match len(rows) ({len(self.rows)})"
            )
