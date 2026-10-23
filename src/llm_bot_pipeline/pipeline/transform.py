"""
Data transformation and cleaning for LLM bot traffic (BigQuery-specific).

Implements the Transform stage of the ETL pipeline:
- Deduplication
- URL path extraction
- Temporal field extraction
- Bot classification
- Response status categorization

Architecture Note:
    This module is BigQuery-specific and uses direct SQL string templates
    rather than the sql_compat layer. This is intentional because:
    1. The transformation query is complex and benefits from being a single
       readable SQL block rather than composed fragments
    2. The class uses BigQuery client directly for job submission
    3. For SQLite/local development, use LocalPipeline which leverages sql_compat

    The raw table stores EdgeStartTimestamp as INT64 nanoseconds, which requires
    conversion via TIMESTAMP_MICROS(DIV(column, 1000)) before date operations.
"""

import logging
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from google.api_core import exceptions as gexc
from google.cloud import bigquery

logger = logging.getLogger(__name__)

from ..config.constants import (
    BOT_CLASSIFICATION,
    DATASET_RAW,
    DATASET_REPORT,
    TABLE_CLEAN_BOT_REQUESTS,
    TABLE_RAW_BOT_REQUESTS,
)
from ..config.settings import UrlFilteringSettings, get_settings


@dataclass
class TransformResult:
    """Result of a transformation operation."""

    success: bool
    rows_transformed: int = 0
    rows_deduplicated: int = 0
    error: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class LLMBotTransformer:
    """
    Transforms raw LLM bot traffic data into clean, analysis-ready format.

    Uses SQL-based transformations in BigQuery for performance:
    - Deduplication via ROW_NUMBER() window function
    - URL path extraction via REGEXP_EXTRACT
    - Temporal field extraction from timestamps
    - Bot classification via CASE statements
    - Response status categorization
    """

    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[Path] = None,
        source_dataset: str = DATASET_RAW,
        source_table: str = TABLE_RAW_BOT_REQUESTS,
        target_dataset: str = DATASET_REPORT,
        target_table: str = TABLE_CLEAN_BOT_REQUESTS,
    ):
        """
        Initialize the transformer.

        Args:
            project_id: GCP project ID
            credentials_path: Path to service account JSON key (uses ADC if None)
            source_dataset: Source dataset (raw data)
            source_table: Source table (raw data)
            target_dataset: Target dataset (clean data)
            target_table: Target table (clean data)
        """
        self.project_id = project_id
        self.source_table_id = f"{project_id}.{source_dataset}.{source_table}"
        self.target_table_id = f"{project_id}.{target_dataset}.{target_table}"

        if credentials_path:
            self.client = bigquery.Client.from_service_account_json(
                str(credentials_path), project=project_id
            )
        else:
            self.client = bigquery.Client(project=project_id)

        try:
            self._url_settings = get_settings().url_filtering
        except Exception:
            self._url_settings = UrlFilteringSettings()

    def transform_and_load(
        self,
        start_date: date,
        end_date: date,
        write_disposition: str = "WRITE_APPEND",
    ) -> TransformResult:
        """
        Transform raw data and load into clean table.

        Performs:
        1. Deduplication (removes exact duplicates)
        2. URL path extraction
        3. Temporal field extraction
        4. Bot classification
        5. Response status categorization
        6. Inserts into clean table

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            write_disposition: WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY

        Returns:
            TransformResult with success status and row counts
        """
        if start_date > end_date:
            return TransformResult(
                success=False,
                error=f"start_date ({start_date}) must be <= end_date ({end_date})",
                start_date=start_date,
                end_date=end_date,
            )

        try:
            # Build and execute transformation query
            query = self._build_transform_query(start_date, end_date)

            job_config = bigquery.QueryJobConfig(
                destination=self.target_table_id,
                write_disposition=write_disposition,
            )

            job = self.client.query(query, job_config=job_config)
            result = job.result()

            rows_transformed = result.total_rows

            # Get deduplication stats
            stats = self._get_dedup_stats(start_date, end_date)

            return TransformResult(
                success=True,
                rows_transformed=rows_transformed,
                rows_deduplicated=stats.get("duplicates_removed", 0),
                start_date=start_date,
                end_date=end_date,
            )

        except Exception as e:
            return TransformResult(
                success=False,
                error=str(e),
                start_date=start_date,
                end_date=end_date,
            )

    def _build_date_filter_clause(self, start_date: date, end_date: date) -> str:
        """
        Build WHERE clause for date range and required non-null fields.

        EdgeStartTimestamp is INT64 nanoseconds; converted via
        TIMESTAMP_MICROS(DIV(column, 1000)).
        """
        return (
            f"DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) >= '{start_date.isoformat()}'"
            f"\n              AND DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) <= '{end_date.isoformat()}'"
            "\n              AND EdgeStartTimestamp IS NOT NULL"
            "\n              AND EdgeResponseStatus IS NOT NULL"
        )

    def _build_status_clause(self) -> str:
        """Build CASE expression for response_status_category."""
        return """
                CASE
                    WHEN EdgeResponseStatus BETWEEN 200 AND 299 THEN '2xx_success'
                    WHEN EdgeResponseStatus BETWEEN 300 AND 399 THEN '3xx_redirect'
                    WHEN EdgeResponseStatus BETWEEN 400 AND 499 THEN '4xx_client_error'
                    WHEN EdgeResponseStatus BETWEEN 500 AND 599 THEN '5xx_server_error'
                    ELSE 'other'
                END as response_status_category"""

    def _build_bot_classification_clause(self) -> dict[str, str]:
        """Build SQL CASE statements for bot classification (bot_name, bot_provider, bot_category)."""
        name_cases = []
        provider_cases = []
        category_cases = []

        for bot_name, info in BOT_CLASSIFICATION.items():
            cond = f"REGEXP_CONTAINS(ClientRequestUserAgent, r'(?i)\\b{bot_name}\\b')"
            name_cases.append(f"WHEN {cond} THEN '{bot_name}'")
            provider_cases.append(f"WHEN {cond} THEN '{info['provider']}'")
            category_cases.append(f"WHEN {cond} THEN '{info['category']}'")

        def _case_sql(when_clauses: list[str], else_val: str) -> str:
            return (
                f"CASE\n                    "
                + "\n                    ".join(when_clauses)
                + f"\n                    ELSE '{else_val}'\n                END"
            )

        return {
            "bot_name": _case_sql(name_cases, "Unknown"),
            "bot_provider": _case_sql(provider_cases, "Unknown"),
            "bot_category": _case_sql(category_cases, "unknown"),
        }

    def _build_aggregation_clause(self) -> str:
        """
        Build SELECT expressions for temporal and URL-derived fields.

        Converts EdgeStartTimestamp (INT64 nanoseconds) to timestamp/date fields
        and extracts url_path with depth.
        """
        return """
                -- Temporal fields (convert nanoseconds INT64 to TIMESTAMP using DIV)
                TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)) as request_timestamp,
                DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) as request_date,
                EXTRACT(HOUR FROM TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) as request_hour,
                FORMAT_DATE('%A', DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)))) as day_of_week,

                -- Request fields
                ClientRequestURI as request_uri,
                ClientRequestHost as request_host,
                domain,

                -- URL path extraction (remove query params and fragments)
                REGEXP_EXTRACT(ClientRequestURI, r'^([^?#]*)') as url_path,

                -- URL path depth (count segments, 0 for root)
                CASE
                    WHEN REGEXP_EXTRACT(ClientRequestURI, r'^([^?#]*)') IN ('/', '') THEN 0
                    ELSE ARRAY_LENGTH(
                        SPLIT(
                            REGEXP_REPLACE(
                                REGEXP_EXTRACT(ClientRequestURI, r'^([^?#]*)'),
                                r'^/|/$', ''
                            ),
                            '/'
                        )
                    )
                END as url_path_depth"""

    def _build_url_filter_clause(self) -> str:
        """Generate BigQuery WHERE clause to exclude non-user-facing URLs.

        Uses ``url_path`` (already extracted in the classified CTE) rather than
        the raw ``ClientRequestURI`` which is no longer available at the final
        SELECT level.
        """
        if not self._url_settings.enabled:
            return "TRUE"

        conditions = []

        if self._url_settings.drop_extensions:
            ext_pattern = "|".join(
                re.escape(e) for e in sorted(self._url_settings.drop_extensions)
            )
            conditions.append(
                f"NOT REGEXP_CONTAINS(LOWER(url_path), r'\\.({ext_pattern})$')"
            )

        for prefix in self._url_settings.drop_path_prefixes:
            escaped = prefix.replace("'", "\\'")
            conditions.append(f"NOT STARTS_WITH(LOWER(url_path), '{escaped.lower()}')")

        return " AND ".join(conditions) if conditions else "TRUE"

    def _build_resource_type_clause(self) -> str:
        """Generate CASE expression for resource_type column."""
        if not self._url_settings.enabled or not self._url_settings.image_extensions:
            return "'document' as resource_type"

        ext_pattern = "|".join(
            re.escape(e) for e in sorted(self._url_settings.image_extensions)
        )
        return (
            f"CASE WHEN REGEXP_CONTAINS("
            f"LOWER(REGEXP_EXTRACT(ClientRequestURI, r'^([^?#]*)')), "
            f"r'\\.({ext_pattern})$') THEN 'image' "
            f"ELSE 'document' END as resource_type"
        )

    def _build_transform_query(self, start_date: date, end_date: date) -> str:
        """
        Build the SQL transformation query.

        Composes clause builders for:
        1. Deduplication using ROW_NUMBER()
        2. URL path extraction
        3. Temporal field extraction
        4. Bot classification
        5. Response status categorization
        """
        date_filter = self._build_date_filter_clause(start_date, end_date)
        bot_case = self._build_bot_classification_clause()
        aggregation = self._build_aggregation_clause()
        status_clause = self._build_status_clause()
        resource_type_clause = self._build_resource_type_clause()
        url_filter = self._build_url_filter_clause()

        query = f"""
        WITH deduplicated AS (
            -- Step 1: Deduplicate using ROW_NUMBER
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        EdgeStartTimestamp,
                        ClientIP,
                        ClientRequestURI,
                        ClientRequestHost
                    ORDER BY EdgeStartTimestamp
                ) as row_num
            FROM `{self.source_table_id}`
            WHERE {date_filter}
        ),

        filtered AS (
            -- Step 2: Keep only first occurrence of each duplicate
            SELECT * FROM deduplicated WHERE row_num = 1
        ),

        classified AS (
            -- Step 3: Add bot classification and derived fields
            SELECT
                {aggregation},

                -- URL resource type classification
                {resource_type_clause},

                -- Bot identification
                ClientRequestUserAgent as user_agent_raw,
                {bot_case['bot_name']} as bot_name,
                {bot_case['bot_provider']} as bot_provider,
                {bot_case['bot_category']} as bot_category,

                -- Geographic
                ClientCountry as crawler_country,

                -- Response
                EdgeResponseStatus as response_status,
                {status_clause},

                -- Metadata
                CURRENT_TIMESTAMP() as _processed_at

            FROM filtered
        )

        -- Final output: only include rows with identified bots and user-facing URLs
        SELECT * FROM classified
        WHERE bot_name != 'Unknown'
          AND {url_filter}
        """

        return query

    def _get_dedup_stats(self, start_date: date, end_date: date) -> dict:
        """Get statistics about deduplication."""
        # Note: EdgeStartTimestamp is INT64 nanoseconds, convert with DIV for integer division
        query = f"""
        WITH raw_count AS (
            SELECT COUNT(*) as total
            FROM `{self.source_table_id}`
            WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) >= '{start_date.isoformat()}'
              AND DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) <= '{end_date.isoformat()}'
        ),
        dedup_count AS (
            SELECT COUNT(*) as unique_count
            FROM (
                SELECT
                    ROW_NUMBER() OVER (
                        PARTITION BY EdgeStartTimestamp, ClientIP, ClientRequestURI, ClientRequestHost
                        ORDER BY EdgeStartTimestamp
                    ) as row_num
                FROM `{self.source_table_id}`
                WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) >= '{start_date.isoformat()}'
                  AND DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) <= '{end_date.isoformat()}'
            )
            WHERE row_num = 1
        )
        SELECT
            raw_count.total as raw_rows,
            dedup_count.unique_count as unique_rows,
            raw_count.total - dedup_count.unique_count as duplicates_removed
        FROM raw_count, dedup_count
        """

        try:
            result = self.client.query(query).result()
            row = list(result)[0]
            return {
                "raw_rows": row.raw_rows,
                "unique_rows": row.unique_rows,
                "duplicates_removed": row.duplicates_removed,
            }
        except gexc.NotFound:
            logger.warning("Table not found when getting dedup stats")
            return {"duplicates_removed": 0}
        except (gexc.BadRequest, IndexError) as e:
            logger.warning("Dedup stats query returned no data: %s", e)
            return {"duplicates_removed": 0}

    def preview_transform(
        self, start_date: date, end_date: date, limit: int = 10
    ) -> list[dict]:
        """
        Preview transformation results without writing to target table.

        Args:
            start_date: Start date
            end_date: End date
            limit: Max rows to return

        Returns:
            List of transformed row dictionaries

        Raises:
            ValueError: If start_date > end_date
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        query = self._build_transform_query(start_date, end_date)
        query += f"\nLIMIT {limit}"

        result = self.client.query(query).result()
        return [dict(row) for row in result]

    def get_transformation_stats(self, start_date: date, end_date: date) -> dict:
        """
        Get statistics about the transformation without executing it.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Dictionary with transformation statistics

        Raises:
            ValueError: If start_date > end_date
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        # Note: EdgeStartTimestamp is INT64 nanoseconds, convert with DIV for integer division
        query = f"""
        WITH source_data AS (
            SELECT
                ClientRequestUserAgent,
                EdgeResponseStatus
            FROM `{self.source_table_id}`
            WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) >= '{start_date.isoformat()}'
              AND DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) <= '{end_date.isoformat()}'
        )
        SELECT
            COUNT(*) as total_rows,
            COUNTIF(EdgeResponseStatus BETWEEN 200 AND 299) as success_responses,
            COUNTIF(EdgeResponseStatus BETWEEN 300 AND 399) as redirect_responses,
            COUNTIF(EdgeResponseStatus >= 400) as error_responses,
            COUNT(DISTINCT ClientRequestUserAgent) as unique_user_agents
        FROM source_data
        """

        result = self.client.query(query).result()
        row = list(result)[0]

        return {
            "total_rows": row.total_rows,
            "success_responses": row.success_responses,
            "redirect_responses": row.redirect_responses,
            "error_responses": row.error_responses,
            "unique_user_agents": row.unique_user_agents,
        }

    def delete_date_range(self, start_date: date, end_date: date) -> int:
        """
        Delete rows from clean table for a date range (for reprocessing).

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Number of rows deleted

        Raises:
            ValueError: If start_date > end_date
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        query = f"""
        DELETE FROM `{self.target_table_id}`
        WHERE request_date >= '{start_date.isoformat()}'
          AND request_date <= '{end_date.isoformat()}'
        """

        job = self.client.query(query)
        job.result()

        return job.num_dml_affected_rows or 0
