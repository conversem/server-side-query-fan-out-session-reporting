"""
Insert strategy methods for the local pipeline.

Implements batch insert with progressive fallback:
full batch → retry → binary-split sub-batches → per-row.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ...storage import StorageError

if TYPE_CHECKING:
    from ...storage import StorageBackend

logger = logging.getLogger(__name__)


class InsertMixin:
    """Batch insert methods with progressive fallback strategy."""

    _output_backend: StorageBackend

    # Minimum sub-batch size before falling back to per-row inserts
    _MIN_SUB_BATCH_SIZE = 10

    def _batch_insert_with_fallback(self, records: list[dict]) -> dict:
        """Insert records with progressive fallback strategy.

        Order: full batch -> retry once -> binary-split sub-batches -> per-row.
        """
        stats = {
            "total_records": len(records),
            "rows_inserted": 0,
            "rows_failed": 0,
            "sub_batches_attempted": 0,
            "per_row_fallbacks": 0,
        }

        if not records:
            return stats

        if not hasattr(self._output_backend, "insert_clean_records"):
            self._insert_per_row(records, stats)
            return stats

        # Attempt 1: full batch
        try:
            stats["rows_inserted"] = self._output_backend.insert_clean_records(records)
            return stats
        except StorageError as e:
            logger.warning("Full batch insert failed (%d records): %s", len(records), e)

        # Attempt 2: retry full batch once
        try:
            stats["rows_inserted"] = self._output_backend.insert_clean_records(records)
            logger.info("Batch retry succeeded (%d records)", len(records))
            return stats
        except StorageError as e:
            logger.warning(
                "Batch retry also failed (%d records), splitting: %s",
                len(records),
                e,
            )

        # Attempt 3: binary-split sub-batches
        self._insert_sub_batches(records, stats)
        return stats

    def _insert_sub_batches(self, records: list[dict], stats: dict) -> None:
        """Recursively split failed batches in half until success or per-row."""
        if len(records) <= self._MIN_SUB_BATCH_SIZE:
            self._insert_per_row(records, stats)
            return

        mid = len(records) // 2
        left, right = records[:mid], records[mid:]

        for sub_batch in (left, right):
            stats["sub_batches_attempted"] += 1
            logger.info(
                "Sub-batch #%d: inserting %d records",
                stats["sub_batches_attempted"],
                len(sub_batch),
            )
            try:
                inserted = self._output_backend.insert_clean_records(sub_batch)
                stats["rows_inserted"] += inserted
                logger.info("Sub-batch succeeded: %d records inserted", inserted)
            except StorageError as e:
                logger.warning("Sub-batch failed (%d records): %s", len(sub_batch), e)
                self._insert_sub_batches(sub_batch, stats)

    def _insert_per_row(self, records: list[dict], stats: dict) -> None:
        """Insert records one-by-one (last resort fallback)."""
        stats["per_row_fallbacks"] += 1
        logger.info("Per-row fallback for %d records", len(records))
        for record in records:
            insert_sql = self._build_insert_sql(record)
            try:
                self._output_backend.execute(insert_sql)
                stats["rows_inserted"] += 1
            except StorageError as e:
                stats["rows_failed"] += 1
                logger.warning("Failed to insert row: %s", e)
