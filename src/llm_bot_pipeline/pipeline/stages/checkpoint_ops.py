"""
Checkpoint operation methods for the local pipeline.

Handles per-date checkpointing for local_bq_buffered resume support.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING, Optional

from ...config.constants import TABLE_CLEAN_BOT_REQUESTS
from ...storage import StorageError

if TYPE_CHECKING:
    from ...storage import StorageBackend
    from ..checkpoint import CheckpointManager

logger = logging.getLogger(__name__)


class CheckpointOpsMixin:
    """Checkpoint and date iteration methods."""

    _checkpoint_manager: Optional[CheckpointManager]
    _output_backend: StorageBackend

    def _execute_with_checkpoint(
        self,
        start_date: date,
        end_date: date,
        mode: str,
        raw_count: int,
        result,
    ) -> None:
        """Execute pipeline with per-date checkpointing for local_bq_buffered resume."""
        skip_dates = self._get_completed_dates(start_date, end_date)
        dates_to_process = [
            d for d in self._iter_dates(start_date, end_date) if d not in skip_dates
        ]
        if skip_dates:
            logger.info(
                "  Checkpointing: skipping %d already-completed date(s)",
                len(skip_dates),
            )
        if not dates_to_process:
            logger.info("  All dates already completed; nothing to process")
            result.transformed_rows = 0
            result.duplicates_removed = 0
            result.success = True
            return

        total_transformed = 0
        total_duplicates = 0
        for i, target_date in enumerate(dates_to_process):
            logger.info(
                "[4/4] Transforming and loading... date %s (%d/%d)",
                target_date,
                i + 1,
                len(dates_to_process),
            )
            if mode == "full":
                deleted = self._delete_clean_data(target_date, target_date)
                if deleted:
                    logger.debug(
                        "  Deleted %d existing rows for %s", deleted, target_date
                    )
            raw_for_date = self._get_raw_count(target_date, target_date)
            transform_result = self._transform_and_load(
                target_date, target_date, raw_count=raw_for_date
            )
            rows = transform_result["rows_transformed"]
            dups = transform_result.get("duplicates_removed", 0)
            total_transformed += rows
            total_duplicates += dups
            if rows > 0 and self._checkpoint_manager:
                self._checkpoint_manager.record_completed(
                    target_date, TABLE_CLEAN_BOT_REQUESTS, rows
                )

        result.transformed_rows = total_transformed
        result.duplicates_removed = total_duplicates
        result.success = True
        logger.info(f"  Transformed {result.transformed_rows:,} rows")
        logger.info(f"  Removed {result.duplicates_removed:,} duplicates")

    def _get_completed_dates(self, start_date: date, end_date: date) -> set[date]:
        """Get dates already completed (checkpoint + BQ query)."""
        completed: set[date] = set()
        if self._checkpoint_manager:
            completed |= self._checkpoint_manager.get_completed_dates(
                start_date, end_date, TABLE_CLEAN_BOT_REQUESTS
            )
        if hasattr(self._output_backend, "get_completed_dates_in_range"):
            try:
                bq_dates = self._output_backend.get_completed_dates_in_range(
                    TABLE_CLEAN_BOT_REQUESTS,
                    "request_date",
                    start_date,
                    end_date,
                )
                completed |= bq_dates
            except StorageError as e:
                logger.warning("Could not query BQ for completed dates: %s", e)
        return completed

    @staticmethod
    def _iter_dates(start_date: date, end_date: date):
        """Iterate over dates from start to end inclusive."""
        d = start_date
        while d <= end_date:
            yield d
            d += timedelta(days=1)
