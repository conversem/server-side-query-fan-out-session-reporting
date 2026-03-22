"""
Data operation methods for the local pipeline.

Covers raw count, transform stats, delete clean data,
and the transform-and-load step.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING

from ...config.constants import TABLE_CLEAN_BOT_REQUESTS, TABLE_RAW_BOT_REQUESTS
from ...storage import StorageError

if TYPE_CHECKING:
    from ...storage import StorageBackend
    from ..sql_compat import SQLBuilder

logger = logging.getLogger(__name__)


class DataOpsMixin:
    """Data operation methods: extract, delete, transform & load."""

    _backend: StorageBackend
    _output_backend: StorageBackend
    _sql: SQLBuilder

    def _get_raw_count(self, start_date: date, end_date: date) -> int:
        """Get count of raw records for date range.

        Raises:
            StorageError: If the count query fails.
        """
        date_filter = self._sql.date_filter("EdgeStartTimestamp", start_date, end_date)
        sql = f"""
            SELECT COUNT(*) as cnt FROM {TABLE_RAW_BOT_REQUESTS}
            WHERE {date_filter}
        """
        try:
            result = self._backend.query(sql)
            return result[0]["cnt"] if result else 0
        except StorageError as e:
            raise StorageError(
                f"Failed to get raw count for {start_date} to {end_date}: {e}"
            ) from e

    def _get_transform_stats(self, start_date: date, end_date: date) -> dict:
        """Get transformation statistics.

        Raises:
            StorageError: If the stats query fails.
        """
        date_filter = self._sql.date_filter("EdgeStartTimestamp", start_date, end_date)
        verified_count = self._sql.countif("VerifiedBot = 1")

        sql = f"""
            SELECT
                COUNT(*) as total_rows,
                {verified_count} as verified_bots,
                COUNT(DISTINCT ClientRequestUserAgent) as unique_user_agents
            FROM {TABLE_RAW_BOT_REQUESTS}
            WHERE {date_filter}
        """
        try:
            result = self._backend.query(sql)
            if result:
                return dict(result[0])
            return {}
        except StorageError as e:
            raise StorageError(
                f"Failed to get transform stats for {start_date} to {end_date}: {e}"
            ) from e

    def _delete_clean_data(self, start_date: date, end_date: date) -> int:
        """Delete existing clean data for date range."""
        if not self._output_backend.table_exists(TABLE_CLEAN_BOT_REQUESTS):
            return 0

        sql = f"""
            DELETE FROM {TABLE_CLEAN_BOT_REQUESTS}
            WHERE request_date >= :start_date
              AND request_date <= :end_date
        """
        try:
            return self._output_backend.execute(
                sql,
                {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )
        except StorageError as e:
            logger.warning(f"Failed to delete clean data: {e}")
            return 0

    def _transform_and_load(
        self, start_date: date, end_date: date, raw_count: int = 0
    ) -> dict:
        """Transform raw data and load into clean table."""
        transform_sql = self._build_transform_sql(start_date, end_date)

        result = self._backend.query(transform_sql)

        if not result:
            return {"rows_transformed": 0, "duplicates_removed": 0}

        records = []
        url_filtered = 0
        for row in result:
            clean = self._row_to_clean_record(row)
            if clean is not None:
                records.append(clean)
            else:
                url_filtered += 1

        if url_filtered:
            logger.info(
                "URL filtering dropped %d non-user-facing records", url_filtered
            )

        if not records:
            return {"rows_transformed": 0, "duplicates_removed": 0}

        insert_stats = self._batch_insert_with_fallback(records)
        rows_inserted = insert_stats["rows_inserted"]

        if insert_stats["rows_failed"] > 0:
            logger.warning(
                "Insert stats: %d inserted, %d failed, %d sub-batches, %d per-row fallbacks",
                insert_stats["rows_inserted"],
                insert_stats["rows_failed"],
                insert_stats["sub_batches_attempted"],
                insert_stats["per_row_fallbacks"],
            )

        duplicates = raw_count - rows_inserted

        return {
            "rows_transformed": rows_inserted,
            "duplicates_removed": max(0, duplicates),
        }
