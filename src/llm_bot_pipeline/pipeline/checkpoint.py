"""
Checkpoint manager for local_bq_buffered mode resume capability.

Tracks which dates were successfully loaded to BigQuery so partial failures
can be resumed without re-processing.
"""

import json
import logging
from datetime import date
from pathlib import Path
from typing import Optional

from ..config.constants import TABLE_CLEAN_BOT_REQUESTS

logger = logging.getLogger(__name__)


def _date_to_str(d: date) -> str:
    """Serialize date to ISO string."""
    return d.isoformat()


def _str_to_date(s: str) -> date:
    """Parse ISO date string."""
    return date.fromisoformat(s)


class CheckpointManager:
    """
    Tracks completed date/table/row_count entries in a JSON file.

    Used by local_bq_buffered mode to skip dates already loaded to BigQuery
    on re-run after partial failures.
    """

    def __init__(self, checkpoint_path: Path):
        """
        Initialize checkpoint manager.

        Args:
            checkpoint_path: Path to JSON checkpoint file.
        """
        self._path = Path(checkpoint_path)
        self._entries: list[dict] = []

    def _load(self) -> None:
        """Load checkpoint from disk."""
        self._entries = []
        if not self._path.exists():
            return
        try:
            with open(self._path) as f:
                data = json.load(f)
            self._entries = data.get("completed", [])
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Checkpoint file invalid or unreadable: %s", e)

    def _save(self) -> None:
        """Persist checkpoint to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w") as f:
            json.dump({"completed": self._entries}, f, indent=2)

    def record_completed(
        self,
        target_date: date,
        table: str = TABLE_CLEAN_BOT_REQUESTS,
        row_count: int = 0,
    ) -> None:
        """
        Record a date as successfully loaded to the output table.

        Args:
            target_date: Date that was loaded.
            table: Target table name (default: bot_requests_daily).
            row_count: Number of rows inserted.
        """
        self._load()
        entry = {
            "date": _date_to_str(target_date),
            "table": table,
            "row_count": row_count,
        }
        # Replace existing entry for same date/table
        self._entries = [
            e
            for e in self._entries
            if not (
                e.get("date") == _date_to_str(target_date) and e.get("table") == table
            )
        ]
        self._entries.append(entry)
        self._save()
        logger.debug(
            "Checkpoint recorded: %s -> %s (%d rows)", target_date, table, row_count
        )

    def get_completed_dates(
        self, start_date: date, end_date: date, table: str = TABLE_CLEAN_BOT_REQUESTS
    ) -> set[date]:
        """
        Get dates in range that are marked completed in checkpoint.

        Args:
            start_date: Range start (inclusive).
            end_date: Range end (inclusive).
            table: Filter by table name.

        Returns:
            Set of dates that are completed.
        """
        self._load()
        result: set[date] = set()
        for e in self._entries:
            if e.get("table") != table:
                continue
            try:
                d = _str_to_date(e["date"])
                if start_date <= d <= end_date:
                    result.add(d)
            except (KeyError, ValueError):
                continue
        return result

    def is_date_completed(
        self, target_date: date, table: str = TABLE_CLEAN_BOT_REQUESTS
    ) -> bool:
        """Check if a single date is marked completed."""
        return target_date in self.get_completed_dates(
            target_date, target_date, table=table
        )
