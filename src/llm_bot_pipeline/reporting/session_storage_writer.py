"""
Storage writer for query fan-out session records.

Handles persisting SessionRecord objects to storage backends (SQLite, BigQuery).
Extracted from SessionAggregator to separate storage concerns from aggregation logic.

Supports:
- SQLite: individual parameterized INSERTs (fast for local)
- BigQuery: streaming batch insert with automatic fallback to row-by-row on error
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from ..config.constants import BIGQUERY_BATCH_SIZE
from ..storage import StorageBackend

if TYPE_CHECKING:
    from .session_aggregations import SessionRecord

logger = logging.getLogger(__name__)

TABLE_NAME = "query_fanout_sessions"


def record_to_dict(record: SessionRecord) -> dict:
    """Convert a SessionRecord to a flat dictionary for insertion."""
    return {
        "session_id": record.session_id,
        "session_date": record.session_date,
        "domain": record.domain,
        "session_start_time": record.session_start_time,
        "session_end_time": record.session_end_time,
        "duration_ms": int(record.duration_ms),
        "bot_provider": record.bot_provider,
        "bot_name": record.bot_name,
        "request_count": record.request_count,
        "unique_urls": record.unique_urls,
        "mean_cosine_similarity": record.mean_cosine_similarity,
        "min_cosine_similarity": record.min_cosine_similarity,
        "max_cosine_similarity": record.max_cosine_similarity,
        "confidence_level": record.confidence_level,
        "fanout_session_name": record.fanout_session_name,
        "url_list": record.url_list,
        "window_ms": record.window_ms,
        "splitting_strategy": record.splitting_strategy,
        "_created_at": record._created_at,
        "parent_session_id": record.parent_session_id,
        "was_refined": record.was_refined,
        "refinement_reason": record.refinement_reason,
        "pre_refinement_mibcs": record.pre_refinement_mibcs,
    }


class SessionStorageWriter:
    """
    Persists SessionRecord objects to a StorageBackend.

    Automatically selects the optimal insert strategy for the backend:
    - BigQuery: streaming batch insert (up to 10k rows per batch)
    - SQLite: parameterized individual INSERTs

    Usage::

        writer = SessionStorageWriter(backend)
        inserted = writer.insert_sessions(records)
    """

    def __init__(self, backend: StorageBackend) -> None:
        self._backend = backend

    @property
    def backend(self) -> StorageBackend:
        return self._backend

    def _get_table_ref(self) -> str:
        """Get the backend-specific table reference for SQL queries."""
        full_id = self._backend.get_full_table_id(TABLE_NAME)
        if self._backend.backend_type == "bigquery":
            return f"`{full_id}`"
        return full_id

    def insert_sessions(self, records: list[SessionRecord]) -> int:
        """
        Insert session records into the storage backend.

        Dispatches to BigQuery batch or SQLite individual insert depending
        on the active backend type.

        Args:
            records: SessionRecord objects to persist.

        Returns:
            Number of records successfully inserted.
        """
        if not records:
            return 0

        if self._backend.backend_type == "bigquery":
            return self._insert_bigquery_batch(records)

        return self._insert_sqlite(records)

    def _insert_bigquery_batch(self, records: list[SessionRecord]) -> int:
        """Batch-insert sessions into BigQuery via streaming insert.

        Inserts in chunks of BIGQUERY_BATCH_SIZE.  On batch failure, falls
        back to row-by-row insertion so partial data is not lost.
        """
        from google.cloud import bigquery as bq  # noqa: F401

        client = self._backend._get_client()
        table_id = self._backend.get_full_table_id(TABLE_NAME)

        rows = [record_to_dict(r) for r in records]

        inserted = 0
        failed_count = 0

        for i in range(0, len(rows), BIGQUERY_BATCH_SIZE):
            batch = rows[i : i + BIGQUERY_BATCH_SIZE]
            try:
                errors = client.insert_rows_json(table_id, batch)
                if errors:
                    for err in errors[:3]:
                        logger.warning("BigQuery insert error: %s", err)
                    inserted += len(batch) - len(errors)
                    failed_count += len(errors)
                else:
                    inserted += len(batch)

                if (i + BIGQUERY_BATCH_SIZE) % 50000 == 0:
                    logger.info("Inserted %s sessions so far...", f"{inserted:,}")
            except Exception as e:
                logger.error("Batch insert failed at offset %d: %s", i, e)
                for row in batch:
                    try:
                        client.insert_rows_json(table_id, [row])
                        inserted += 1
                    except Exception as inner_e:
                        logger.warning("Failed to insert session: %s", inner_e)
                        failed_count += 1

        if failed_count:
            logger.warning(
                "BigQuery batch insert: %d of %d sessions failed",
                failed_count,
                len(rows),
            )

        return inserted

    def _insert_sqlite(self, records: list[SessionRecord]) -> int:
        """Insert sessions into SQLite one by one with parameterized SQL."""
        table_ref = self._get_table_ref()
        sql = f"""
            INSERT INTO {table_ref} (
                session_id, session_date, domain, session_start_time, session_end_time,
                duration_ms, bot_provider, bot_name, request_count, unique_urls,
                mean_cosine_similarity, min_cosine_similarity, max_cosine_similarity,
                confidence_level, fanout_session_name, url_list, window_ms,
                splitting_strategy, _created_at,
                parent_session_id, was_refined, refinement_reason, pre_refinement_mibcs
            ) VALUES (
                :session_id, :session_date, :domain, :session_start_time, :session_end_time,
                :duration_ms, :bot_provider, :bot_name, :request_count, :unique_urls,
                :mean_cosine_similarity, :min_cosine_similarity, :max_cosine_similarity,
                :confidence_level, :fanout_session_name, :url_list, :window_ms,
                :splitting_strategy, :_created_at,
                :parent_session_id, :was_refined, :refinement_reason, :pre_refinement_mibcs
            )
        """

        inserted = 0
        failed_count = 0
        for record in records:
            try:
                params = record_to_dict(record)
                params["was_refined"] = 1 if record.was_refined else 0
                self._backend.execute(sql, params)
                inserted += 1
            except Exception as e:
                logger.warning("Failed to insert session %s: %s", record.session_id, e)
                failed_count += 1

        if failed_count:
            logger.warning(
                "SQLite session insert: %d of %d sessions failed",
                failed_count,
                len(records),
            )

        return inserted

    def delete_sessions(
        self,
        session_date: Optional[str] = None,
        bot_provider: Optional[str] = None,
    ) -> int:
        """
        Delete existing sessions (for reprocessing).

        Args:
            session_date: Delete sessions for this date only (YYYY-MM-DD).
            bot_provider: Delete sessions for this provider only.

        Returns:
            Number of sessions deleted.
        """
        conditions: list[str] = []
        params: dict = {}

        if session_date:
            conditions.append("session_date = :session_date")
            params["session_date"] = session_date

        if bot_provider:
            conditions.append("bot_provider = :bot_provider")
            params["bot_provider"] = bot_provider

        table_ref = self._get_table_ref()
        if conditions:
            where_clause = " AND ".join(conditions)
            sql = f"DELETE FROM {table_ref} WHERE {where_clause}"
        else:
            sql = f"DELETE FROM {table_ref}"

        try:
            return self._backend.execute(sql, params)
        except Exception as e:
            logger.warning("Failed to delete sessions: %s", e)
            return 0
