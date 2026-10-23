"""Session URL-detail population helpers."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from ..storage import StorageBackend

logger = logging.getLogger(__name__)


class SessionUrlDetailsPopulator:
    """Populate and maintain flattened session URL details."""

    TABLE_NAME = "session_url_details"

    def __init__(self, backend: StorageBackend, sessions_table_name: str) -> None:
        self._backend = backend
        self._sessions_table_name = sessions_table_name

    def _table_ref(self, table_name: str) -> str:
        full_id = self._backend.get_full_table_id(table_name)
        if self._backend.backend_type == "bigquery":
            return f"`{full_id}`"
        return full_id

    def populate(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        delete_existing: bool = True,
    ) -> int:
        sessions_table = self._table_ref(self._sessions_table_name)
        details_table = self._table_ref(self.TABLE_NAME)
        date_filter, params = self._build_date_filter(start_date, end_date)

        if delete_existing and date_filter:
            delete_sql = f"DELETE FROM {details_table} {date_filter}"
            try:
                deleted = self._backend.execute(delete_sql, params)
                logger.info(f"Deleted {deleted:,} existing URL detail rows")
            except Exception as e:
                logger.warning(f"Failed to delete existing URL details: {e}")

        if self._backend.backend_type == "bigquery":
            return self._populate_bigquery(
                sessions_table, details_table, date_filter, params
            )
        return self._populate_sqlite(sessions_table, details_table, date_filter, params)

    def delete(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> int:
        details_table = self._table_ref(self.TABLE_NAME)
        where_clause, params = self._build_date_filter(start_date, end_date)
        sql = f"DELETE FROM {details_table} {where_clause}"

        try:
            return self._backend.execute(sql, params)
        except Exception as e:
            logger.warning(f"Failed to delete URL details: {e}")
            return 0

    def summary(self) -> dict:
        details_table = self._table_ref(self.TABLE_NAME)
        sql = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT session_id) as unique_sessions,
                COUNT(DISTINCT url) as unique_urls,
                COUNT(DISTINCT bot_provider) as unique_providers,
                MIN(session_date) as earliest_date,
                MAX(session_date) as latest_date
            FROM {details_table}
        """

        try:
            results = self._backend.query(sql)
            if results:
                return results[0]
            return {}
        except Exception as e:
            logger.warning(f"Failed to get URL details summary: {e}")
            return {"error": str(e)}

    def _build_date_filter(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> tuple[str, dict]:
        conditions = []
        params: dict = {}
        if start_date:
            conditions.append("session_date >= :start_date")
            params["start_date"] = str(start_date)
        if end_date:
            conditions.append("session_date <= :end_date")
            params["end_date"] = str(end_date)
        return (f"WHERE {' AND '.join(conditions)}" if conditions else ""), params

    def _populate_bigquery(
        self,
        sessions_table: str,
        details_table: str,
        date_filter: str,
        params: dict,
    ) -> int:
        sql = f"""
            INSERT INTO {details_table} (
                session_id, session_date, domain, url, url_position,
                bot_provider, bot_name, fanout_session_name, confidence_level,
                session_request_count, session_unique_urls, session_duration_ms,
                mean_cosine_similarity, min_cosine_similarity, max_cosine_similarity,
                session_start_time, session_end_time,
                window_ms, splitting_strategy, _created_at
            )
            SELECT
                s.session_id,
                s.session_date,
                s.domain,
                url,
                url_position,
                s.bot_provider,
                s.bot_name,
                s.fanout_session_name,
                s.confidence_level,
                s.request_count AS session_request_count,
                s.unique_urls AS session_unique_urls,
                s.duration_ms AS session_duration_ms,
                s.mean_cosine_similarity,
                s.min_cosine_similarity,
                s.max_cosine_similarity,
                s.session_start_time,
                s.session_end_time,
                s.window_ms,
                s.splitting_strategy,
                CURRENT_TIMESTAMP() AS _created_at
            FROM {sessions_table} s,
            UNNEST(JSON_EXTRACT_STRING_ARRAY(s.url_list)) AS url WITH OFFSET AS url_position
            {date_filter}
        """

        rows = self._backend.execute(sql, params)
        logger.info(f"Inserted {rows:,} URL detail rows (BigQuery)")
        return rows

    def _populate_sqlite(
        self,
        sessions_table: str,
        details_table: str,
        date_filter: str,
        params: dict,
    ) -> int:
        sessions = self._backend.query(
            f"SELECT * FROM {sessions_table} {date_filter}", params
        )
        if not sessions:
            logger.info("No sessions found to process")
            return 0

        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        for session in sessions:
            try:
                inserted += self._insert_sqlite_session_urls(
                    details_table, session, now
                )
            except Exception as e:
                logger.warning(
                    f"Failed to process session {session.get('session_id')}: {e}"
                )

        logger.info(f"Inserted {inserted:,} URL detail rows (SQLite)")
        return inserted

    def _insert_sqlite_session_urls(
        self,
        details_table: str,
        session: dict,
        created_at: str,
    ) -> int:
        insert_sql = f"""
            INSERT INTO {details_table} (
                session_id, session_date, domain, url, url_position,
                bot_provider, bot_name, fanout_session_name, confidence_level,
                session_request_count, session_unique_urls, session_duration_ms,
                mean_cosine_similarity, min_cosine_similarity, max_cosine_similarity,
                session_start_time, session_end_time,
                window_ms, splitting_strategy, _created_at
            ) VALUES (
                :session_id, :session_date, :domain, :url, :url_position,
                :bot_provider, :bot_name, :fanout_session_name, :confidence_level,
                :session_request_count, :session_unique_urls, :session_duration_ms,
                :mean_cosine_similarity, :min_cosine_similarity, :max_cosine_similarity,
                :session_start_time, :session_end_time,
                :window_ms, :splitting_strategy, :_created_at
            )
        """
        inserted = 0
        for position, url in enumerate(
            json.loads(session.get("url_list", "[]")), start=1
        ):
            self._backend.execute(
                insert_sql,
                {
                    "session_id": session["session_id"],
                    "session_date": session["session_date"],
                    "domain": session.get("domain"),
                    "url": url,
                    "url_position": position,
                    "bot_provider": session["bot_provider"],
                    "bot_name": session.get("bot_name"),
                    "fanout_session_name": session.get("fanout_session_name"),
                    "confidence_level": session["confidence_level"],
                    "session_request_count": session["request_count"],
                    "session_unique_urls": session["unique_urls"],
                    "session_duration_ms": session["duration_ms"],
                    "mean_cosine_similarity": session.get("mean_cosine_similarity"),
                    "min_cosine_similarity": session.get("min_cosine_similarity"),
                    "max_cosine_similarity": session.get("max_cosine_similarity"),
                    "session_start_time": session["session_start_time"],
                    "session_end_time": session["session_end_time"],
                    "window_ms": session["window_ms"],
                    "splitting_strategy": session.get("splitting_strategy"),
                    "_created_at": created_at,
                },
            )
            inserted += 1
        return inserted
