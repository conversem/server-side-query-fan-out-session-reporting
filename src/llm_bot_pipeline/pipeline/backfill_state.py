"""
Backfill state manager for gcp_bq mode resume capability.

Tracks which dates were successfully completed in a backfill run so partial
failures can be resumed without re-processing. Uses the CheckpointManager
pattern from task 39.
"""

import json
import logging
from datetime import date, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def _date_to_str(d: date) -> str:
    """Serialize date to ISO string."""
    return d.isoformat()


def _str_to_date(s: str) -> date:
    """Parse ISO date string."""
    return date.fromisoformat(s)


def _range_key(start_date: date, end_date: date) -> str:
    """Build state key for a backfill range."""
    return f"{_date_to_str(start_date)}_{_date_to_str(end_date)}"


class BackfillStateManager:
    """
    Tracks completed dates for a backfill run in a JSON state file.

    Used by gcp_bq mode to resume interrupted backfills from the next
    incomplete date. State is keyed by start_date/end_date so different
    backfill ranges are tracked independently.
    """

    def __init__(self, state_path: Path):
        """
        Initialize backfill state manager.

        Args:
            state_path: Path to JSON state file.
        """
        self._path = Path(state_path)
        self._data: dict = {}

    def _load(self) -> None:
        """Load state from disk."""
        self._data = {}
        if not self._path.exists():
            return
        try:
            with open(self._path) as f:
                self._data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Backfill state file invalid or unreadable: %s", e)

    def _save(self) -> None:
        """Persist state to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w") as f:
            json.dump(self._data, f, indent=2)

    def get_completed_dates(self, start_date: date, end_date: date) -> set[date]:
        """
        Get dates in range that are marked completed for this backfill.

        Args:
            start_date: Range start (inclusive).
            end_date: Range end (inclusive).

        Returns:
            Set of dates that are completed.
        """
        self._load()
        key = _range_key(start_date, end_date)
        entries = self._data.get("backfills", {}).get(key, {}).get("completed", [])
        result: set[date] = set()
        for s in entries:
            try:
                d = _str_to_date(s)
                if start_date <= d <= end_date:
                    result.add(d)
            except (ValueError, TypeError):
                continue
        return result

    def get_dates_to_process(self, start_date: date, end_date: date) -> list[date]:
        """
        Get sorted list of dates in range that still need processing.

        Args:
            start_date: Range start (inclusive).
            end_date: Range end (inclusive).

        Returns:
            List of dates not yet completed, in chronological order.
        """
        completed = self.get_completed_dates(start_date, end_date)
        result = []
        current = start_date
        while current <= end_date:
            if current not in completed:
                result.append(current)
            current += timedelta(days=1)
        return result

    def record_completed(
        self,
        target_date: date,
        start_date: date,
        end_date: date,
    ) -> None:
        """
        Record a date as successfully completed for this backfill range.

        Args:
            target_date: Date that was completed.
            start_date: Backfill range start (for state key).
            end_date: Backfill range end (for state key).
        """
        self._load()
        key = _range_key(start_date, end_date)
        if "backfills" not in self._data:
            self._data["backfills"] = {}
        if key not in self._data["backfills"]:
            self._data["backfills"][key] = {
                "start_date": _date_to_str(start_date),
                "end_date": _date_to_str(end_date),
                "completed": [],
            }
        completed = self._data["backfills"][key]["completed"]
        date_str = _date_to_str(target_date)
        if date_str not in completed:
            completed.append(date_str)
            completed.sort()
        self._save()
        logger.debug("Backfill state: recorded %s for range %s", target_date, key)

    def clear_range(self, start_date: date, end_date: date) -> None:
        """
        Clear completed state for a backfill range (force-restart).

        Args:
            start_date: Backfill range start.
            end_date: Backfill range end.
        """
        self._load()
        key = _range_key(start_date, end_date)
        if "backfills" in self._data and key in self._data["backfills"]:
            del self._data["backfills"][key]
            self._save()
            logger.info("Backfill state: cleared range %s", key)
