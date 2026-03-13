"""
SQL building methods for the local pipeline.

Provides the transform SQL query and bot classification CASE statements.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from ...config.constants import BOT_CLASSIFICATION, TABLE_RAW_BOT_REQUESTS
from ..python_transformer import url_path_depth
from ..sql_utils import build_clean_insert_sql

if TYPE_CHECKING:
    from ..sql_compat import SQLBuilder


class SqlBuilderMixin:
    """SQL query building methods."""

    _sql: SQLBuilder

    def _row_to_clean_record(self, row: dict) -> dict:
        """Convert a query result row to a clean record for insertion."""
        return {
            "request_timestamp": row.get("request_timestamp"),
            "request_date": row.get("request_date"),
            "request_hour": row.get("request_hour"),
            "day_of_week": row.get("day_of_week"),
            "request_uri": row.get("request_uri"),
            "request_host": row.get("request_host"),
            "domain": row.get("domain"),
            "url_path": row.get("url_path", ""),
            "url_path_depth": url_path_depth(row.get("url_path", "")),
            "user_agent_raw": row.get("user_agent_raw"),
            "bot_name": row.get("bot_name"),
            "bot_provider": row.get("bot_provider"),
            "bot_category": row.get("bot_category"),
            "bot_score": row.get("bot_score"),
            "is_verified_bot": row.get("is_verified_bot"),
            "crawler_country": row.get("crawler_country"),
            "response_status": row.get("response_status"),
            "response_status_category": row.get("response_status_category"),
            "_processed_at": row.get("_processed_at"),
        }

    def _build_transform_sql(self, start_date: date, end_date: date) -> str:
        """Build the SQLite transformation SQL query."""
        date_filter = self._sql.date_filter("EdgeStartTimestamp", start_date, end_date)

        bot_case = self._build_bot_classification()
        url_path = self._sql.url_path("ClientRequestURI")

        return f"""
            WITH deduplicated AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY EdgeStartTimestamp, ClientIP, ClientRequestURI, ClientRequestHost
                        ORDER BY EdgeStartTimestamp
                    ) as row_num
                FROM {TABLE_RAW_BOT_REQUESTS}
                WHERE {date_filter}
                  AND EdgeStartTimestamp IS NOT NULL
                  AND EdgeResponseStatus IS NOT NULL
            ),
            filtered AS (
                SELECT * FROM deduplicated WHERE row_num = 1
            )
            SELECT
                EdgeStartTimestamp as request_timestamp,
                date(EdgeStartTimestamp) as request_date,
                CAST(strftime('%H', EdgeStartTimestamp) AS INTEGER) as request_hour,
                CASE CAST(strftime('%w', EdgeStartTimestamp) AS INTEGER)
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END as day_of_week,
                ClientRequestURI as request_uri,
                ClientRequestHost as request_host,
                domain,
                {url_path} as url_path,
                ClientRequestUserAgent as user_agent_raw,
                {bot_case['bot_name']} as bot_name,
                {bot_case['bot_provider']} as bot_provider,
                {bot_case['bot_category']} as bot_category,
                BotScore as bot_score,
                COALESCE(VerifiedBot, 0) as is_verified_bot,
                ClientCountry as crawler_country,
                EdgeResponseStatus as response_status,
                {self._sql.status_category('EdgeResponseStatus')} as response_status_category,
                datetime('now') as _processed_at
            FROM filtered
            WHERE {bot_case['bot_name']} != 'Unknown'
        """

    def _build_bot_classification(self) -> dict[str, str]:
        """Build bot classification CASE statements."""
        name_cases = []
        provider_cases = []
        category_cases = []

        for bot_name, info in BOT_CLASSIFICATION.items():
            condition = self._sql.bot_match("ClientRequestUserAgent", bot_name)
            name_cases.append(f"WHEN {condition} THEN '{bot_name}'")
            provider_cases.append(f"WHEN {condition} THEN '{info['provider']}'")
            category_cases.append(f"WHEN {condition} THEN '{info['category']}'")

        bot_name_sql = "CASE\n        " + "\n        ".join(name_cases)
        bot_name_sql += "\n        ELSE 'Unknown'\n    END"

        bot_provider_sql = "CASE\n        " + "\n        ".join(provider_cases)
        bot_provider_sql += "\n        ELSE 'Unknown'\n    END"

        bot_category_sql = "CASE\n        " + "\n        ".join(category_cases)
        bot_category_sql += "\n        ELSE 'unknown'\n    END"

        return {
            "bot_name": bot_name_sql,
            "bot_provider": bot_provider_sql,
            "bot_category": bot_category_sql,
        }

    @staticmethod
    def _build_insert_sql(row: dict) -> str:
        """Build INSERT statement for a transformed row."""
        return build_clean_insert_sql(row)
