"""
Data extraction from BigQuery raw table (BigQuery-specific).

Implements the Extract stage of the ETL pipeline for LLM bot traffic.

Architecture Note:
    This module is BigQuery-specific and uses direct SQL string templates.
    For SQLite/local development, use LocalPipeline which leverages sql_compat.

    The raw table stores EdgeStartTimestamp as INT64 nanoseconds, which requires
    conversion via TIMESTAMP_MICROS(DIV(column, 1000)) before date operations.
"""

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Optional

from google.api_core import exceptions as gexc
from google.cloud import bigquery
from google.cloud.bigquery import QueryJob

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd

from ..config.constants import DATASET_RAW, TABLE_RAW_BOT_REQUESTS


@dataclass
class ExtractionResult:
    """Result of a data extraction operation."""

    success: bool
    rows_extracted: int = 0
    query_job: Optional[QueryJob] = None
    error: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class LLMBotExtractor:
    """
    Extracts raw LLM bot traffic data from BigQuery.

    Handles:
    - Date range filtering for incremental processing
    - Response status filtering (configurable)
    - Data validation
    - Error handling
    """

    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[Path] = None,
        dataset_id: str = DATASET_RAW,
        table_id: str = TABLE_RAW_BOT_REQUESTS,
    ):
        """
        Initialize the extractor.

        Args:
            project_id: GCP project ID
            credentials_path: Path to service account JSON key (uses ADC if None)
            dataset_id: BigQuery dataset ID for raw data
            table_id: BigQuery table ID for raw data
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.full_table_id = f"{project_id}.{dataset_id}.{table_id}"

        if credentials_path:
            self.client = bigquery.Client.from_service_account_json(
                str(credentials_path), project=project_id
            )
        else:
            self.client = bigquery.Client(project=project_id)

    def extract_raw_data(
        self,
        start_date: date,
        end_date: date,
        include_redirects: bool = True,
        limit: Optional[int] = None,
    ) -> ExtractionResult:
        """
        Extract raw bot traffic data for a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            include_redirects: If True, include 3xx responses; if False, only 2xx
            limit: Maximum rows to return (optional, for testing)

        Returns:
            ExtractionResult with query job and metadata

        Raises:
            ValueError: If start_date > end_date
        """
        # Validate date range
        if start_date > end_date:
            return ExtractionResult(
                success=False,
                error=f"start_date ({start_date}) must be <= end_date ({end_date})",
                start_date=start_date,
                end_date=end_date,
            )

        try:
            # Build query with filters
            query = self._build_extraction_query(
                start_date=start_date,
                end_date=end_date,
                include_redirects=include_redirects,
                limit=limit,
            )

            # Execute query
            job = self.client.query(query)

            # Wait for results and count rows
            results = job.result()
            row_count = results.total_rows

            return ExtractionResult(
                success=True,
                rows_extracted=row_count,
                query_job=job,
                start_date=start_date,
                end_date=end_date,
            )

        except Exception as e:
            return ExtractionResult(
                success=False,
                error=str(e),
                start_date=start_date,
                end_date=end_date,
            )

    def extract_to_dataframe(
        self,
        start_date: date,
        end_date: date,
        include_redirects: bool = True,
        limit: Optional[int] = None,
    ) -> "pd.DataFrame":
        """
        Extract raw data and return as pandas DataFrame.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            include_redirects: If True, include 3xx responses; if False, only 2xx
            limit: Maximum rows to return (optional)

        Returns:
            pandas DataFrame with extracted data

        Raises:
            ValueError: If start_date > end_date
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        query = self._build_extraction_query(
            start_date=start_date,
            end_date=end_date,
            include_redirects=include_redirects,
            limit=limit,
        )

        return self.client.query(query).to_dataframe()

    def iterate_rows(
        self,
        start_date: date,
        end_date: date,
        include_redirects: bool = True,
    ) -> Iterator[dict]:
        """
        Iterate over extracted rows as dictionaries.

        Memory-efficient for large datasets - BigQuery streams results
        automatically without loading all into memory.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            include_redirects: If True, include 3xx responses

        Yields:
            Dictionary for each row

        Raises:
            ValueError: If start_date > end_date
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        query = self._build_extraction_query(
            start_date=start_date,
            end_date=end_date,
            include_redirects=include_redirects,
        )

        job = self.client.query(query)

        for row in job.result():
            yield dict(row)

    def _build_extraction_query(
        self,
        start_date: date,
        end_date: date,
        include_redirects: bool = True,
        limit: Optional[int] = None,
    ) -> str:
        """
        Build the SQL query for data extraction.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            include_redirects: If True, include 3xx responses
            limit: Maximum rows

        Returns:
            SQL query string
        """
        # Base status filter
        if include_redirects:
            status_filter = "EdgeResponseStatus BETWEEN 200 AND 399"
        else:
            status_filter = "EdgeResponseStatus BETWEEN 200 AND 299"

        # Build WHERE conditions
        # Note: EdgeStartTimestamp is INT64 nanoseconds, use DIV for integer division
        conditions = [
            f"DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) >= '{start_date.isoformat()}'",
            f"DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) <= '{end_date.isoformat()}'",
            status_filter,
            "EdgeStartTimestamp IS NOT NULL",
        ]

        where_clause = " AND ".join(conditions)

        query = f"""
        SELECT
            EdgeStartTimestamp,
            ClientRequestURI,
            ClientRequestHost,
            domain,
            ClientRequestUserAgent,
            ClientIP,
            ClientCountry,
            EdgeResponseStatus
        FROM `{self.full_table_id}`
        WHERE {where_clause}
        ORDER BY EdgeStartTimestamp
        """

        if limit:
            query += f"\nLIMIT {limit}"

        return query

    def get_date_range_stats(self, start_date: date, end_date: date) -> dict:
        """
        Get statistics for a date range without extracting all data.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Dictionary with row_count, min_timestamp, max_timestamp
        """
        # Note: EdgeStartTimestamp is INT64 nanoseconds, use DIV for integer division
        query = f"""
        SELECT
            COUNT(*) as row_count,
            MIN(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) as min_timestamp,
            MAX(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) as max_timestamp,
            COUNT(DISTINCT DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)))) as distinct_days,
            COUNT(DISTINCT ClientRequestHost) as distinct_hosts
        FROM `{self.full_table_id}`
        WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) >= '{start_date.isoformat()}'
          AND DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) <= '{end_date.isoformat()}'
        """

        result = self.client.query(query).result()
        row = list(result)[0]

        return {
            "row_count": row.row_count,
            "min_timestamp": row.min_timestamp,
            "max_timestamp": row.max_timestamp,
            "distinct_days": row.distinct_days,
            "distinct_hosts": row.distinct_hosts,
        }

    def validate_data_quality(self, start_date: date, end_date: date) -> dict:
        """
        Check data quality metrics for a date range.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Dictionary with quality metrics
        """
        # Note: EdgeStartTimestamp is INT64 nanoseconds, use DIV for integer division
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNTIF(EdgeStartTimestamp IS NULL) as null_timestamps,
            COUNTIF(ClientRequestUserAgent IS NULL) as null_user_agents,
            COUNTIF(EdgeResponseStatus IS NULL) as null_status_codes
        FROM `{self.full_table_id}`
        WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) >= '{start_date.isoformat()}'
          AND DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) <= '{end_date.isoformat()}'
        """

        result = self.client.query(query).result()
        row = list(result)[0]

        total = row.total_rows or 1  # Avoid division by zero

        return {
            "total_rows": row.total_rows,
            "null_timestamps": row.null_timestamps,
            "null_user_agents": row.null_user_agents,
            "null_status_codes": row.null_status_codes,
            "pct_valid_timestamps": 100 * (1 - row.null_timestamps / total),
            "pct_valid_user_agents": 100 * (1 - row.null_user_agents / total),
        }

    def table_exists(self) -> bool:
        """Check if the raw table exists."""
        try:
            self.client.get_table(self.full_table_id)
            return True
        except gexc.NotFound:
            return False

    def get_table_row_count(self) -> int:
        """Get total row count in the raw table."""
        try:
            table = self.client.get_table(self.full_table_id)
            return table.num_rows or 0
        except gexc.NotFound:
            logger.warning("Table %s not found", self.full_table_id)
            return 0
