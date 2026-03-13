"""
Pipeline result dataclass and logging setup.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional


@dataclass
class LocalPipelineResult:
    """Result of a local pipeline run."""

    success: bool
    start_date: date
    end_date: date
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    # Stats
    raw_rows: int = 0
    transformed_rows: int = 0
    duplicates_removed: int = 0
    # Errors
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Get pipeline duration in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict:
        """Convert to dictionary for logging/serialization."""
        return {
            "success": self.success,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "started_at": self.started_at.isoformat(),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "duration_seconds": self.duration_seconds,
            "raw_rows": self.raw_rows,
            "transformed_rows": self.transformed_rows,
            "duplicates_removed": self.duplicates_removed,
            "errors": self.errors,
        }


def setup_logging(
    level: int = logging.INFO,
    json_logs: "bool | None" = None,
) -> None:
    """
    Configure logging for the pipeline.

    Args:
        level: Logging level (default: INFO)
        json_logs: Use JSON formatter when True. When None, reads from
            JSON_LOGS env or Settings. Default preserves human-readable format.
    """
    from ...config.logging_config import setup_logging as _setup

    _setup(level=level, json_logs=json_logs)
