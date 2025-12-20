"""
Local ETL Pipeline for SQLite backend.

Provides a lightweight ETL pipeline for local development mode using SQLite.
Mirrors the SQLite pipeline's functionality but works entirely locally.

Key differences from SQLite pipeline:
- Uses StorageBackend abstraction instead of direct SQLite calls
- Generates SQLite-compatible SQL
- Supports in-memory processing for small datasets
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from ..config.constants import BOT_CLASSIFICATION
from ..storage import StorageBackend, get_backend
from .sql_compat import SQLBuilder

logger = logging.getLogger(__name__)


@dataclass
class LocalPipelineResult:
    """Result of a local pipeline run."""

    success: bool
    start_date: date
    end_date: date
    started_at: datetime = field(default_factory=lambda: datetime.now().astimezone())
    completed_at: Optional[datetime] = None
    # Stats
    raw_rows: int = 0
    transformed_rows: int = 0
    duplicates_removed: int = 0
    # Errors
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Get pipeline duration in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict:
        """Convert to dictionary for logging/serialization."""
        return {
            "success": self.success,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "started_at": self.started_at.isoformat(),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "duration_seconds": self.duration_seconds,
            "raw_rows": self.raw_rows,
            "transformed_rows": self.transformed_rows,
            "duplicates_removed": self.duplicates_removed,
            "errors": self.errors,
        }


class LocalPipeline:
    """
    Local ETL pipeline using storage abstraction.

    Works with both SQLite  and SQLite (production) backends
    through the StorageBackend interface.

    Pipeline stages:
    1. Extract: Read raw data from raw_bot_requests table
    2. Transform: Deduplicate, classify, enrich
    3. Load: Insert into bot_requests_daily table
    """

    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        backend_type: str = "sqlite",
        db_path: Optional[Path] = None,
    ):
        """
        Initialize the local pipeline.

        Args:
            backend: Pre-initialized StorageBackend (optional)
            backend_type: Backend type if creating new ('sqlite')
            db_path: Path to SQLite database (for sqlite backend)
        """
        if backend:
            self._backend = backend
            self._owns_backend = False
        else:
            kwargs = {}
            if backend_type == "sqlite" and db_path:
                kwargs["db_path"] = db_path
            self._backend = get_backend(backend_type, **kwargs)
            self._owns_backend = True

        self._backend_type = self._backend.backend_type
        self._sql = SQLBuilder(self._backend_type)
        self._initialized = False

        logger.info(f"LocalPipeline initialized with {self._backend_type} backend")

    def initialize(self) -> None:
        """Initialize the backend (create tables if needed)."""
        if not self._initialized:
            self._backend.initialize()
            self._initialized = True

    def close(self) -> None:
        """Close the backend connection."""
        if self._owns_backend:
            self._backend.close()

    def __enter__(self) -> "LocalPipeline":
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def run(
        self,
        start_date: date,
        end_date: date,
        mode: str = "full",
        dry_run: bool = False,
    ) -> LocalPipelineResult:
        """
        Run the ETL pipeline for a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            mode: 'incremental' (append) or 'full' (delete then insert)
            dry_run: If True, validate only without writing

        Returns:
            LocalPipelineResult with stats and status
        """
        self.initialize()

        result = LocalPipelineResult(
            success=False,
            start_date=start_date,
            end_date=end_date,
        )

        # Validate inputs
        if start_date > end_date:
            result.errors.append(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )
            result.completed_at = datetime.now().astimezone()
            return result

        if mode not in ("incremental", "full"):
            result.errors.append(f"Invalid mode: {mode}. Use 'incremental' or 'full'")
            result.completed_at = datetime.now().astimezone()
            return result

        logger.info(
            f"Starting local pipeline: {start_date} to {end_date} (mode={mode})"
        )

        try:
            # Step 1: Check source data
            logger.info("[1/4] Checking source data...")
            raw_count = self._get_raw_count(start_date, end_date)
            result.raw_rows = raw_count
            logger.info(f"  Found {raw_count:,} rows in raw_bot_requests")

            if raw_count == 0:
                logger.warning("  No data found for date range")
                result.success = True
                result.completed_at = datetime.now().astimezone()
                return result

            # Step 2: Preview stats
            logger.info("[2/4] Analyzing data...")
            stats = self._get_transform_stats(start_date, end_date)
            logger.info(f"  Verified bots: {stats.get('verified_bots', 0):,}")
            logger.info(f"  Unique user agents: {stats.get('unique_user_agents', 0):,}")

            if dry_run:
                logger.info("[DRY RUN] Skipping transformation")
                result.success = True
                result.completed_at = datetime.now().astimezone()
                return result

            # Step 3: Delete existing if full mode
            if mode == "full":
                logger.info("[3/4] Deleting existing data for date range...")
                deleted = self._delete_clean_data(start_date, end_date)
                logger.info(f"  Deleted {deleted:,} existing rows")
            else:
                logger.info("[3/4] Incremental mode - skipping delete")

            # Step 4: Transform and load
            logger.info("[4/4] Transforming and loading...")
            transform_result = self._transform_and_load(start_date, end_date)

            result.transformed_rows = transform_result["rows_transformed"]
            result.duplicates_removed = transform_result.get("duplicates_removed", 0)
            result.success = True

            logger.info(f"  Transformed {result.transformed_rows:,} rows")
            logger.info(f"  Removed {result.duplicates_removed:,} duplicates")

        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            result.errors.append(str(e))

        result.completed_at = datetime.now().astimezone()

        if result.success:
            logger.info(
                f"Pipeline completed successfully in {result.duration_seconds:.1f}s"
            )
        else:
            logger.error(f"Pipeline failed: {result.errors}")

        return result

    def run_daily(self, target_date: Optional[date] = None) -> LocalPipelineResult:
        """
        Run pipeline for a single day.

        Args:
            target_date: Date to process (defaults to yesterday)

        Returns:
            LocalPipelineResult
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        logger.info(f"Running daily pipeline for {target_date}")
        return self.run(
            start_date=target_date,
            end_date=target_date,
            mode="full",
        )

    def _get_raw_count(self, start_date: date, end_date: date) -> int:
        """Get count of raw records for date range."""
        date_filter = self._sql.date_filter("EdgeStartTimestamp", start_date, end_date)
        sql = f"""
            SELECT COUNT(*) as cnt FROM raw_bot_requests
            WHERE {date_filter}
        """
        try:
            result = self._backend.query(sql)
            return result[0]["cnt"] if result else 0
        except Exception:
            return 0

    def _get_transform_stats(self, start_date: date, end_date: date) -> dict:
        """Get transformation statistics."""
        date_filter = self._sql.date_filter("EdgeStartTimestamp", start_date, end_date)
        verified_count = self._sql.countif("VerifiedBot = 1")

        sql = f"""
            SELECT
                COUNT(*) as total_rows,
                {verified_count} as verified_bots,
                COUNT(DISTINCT ClientRequestUserAgent) as unique_user_agents
            FROM raw_bot_requests
            WHERE {date_filter}
        """
        try:
            result = self._backend.query(sql)
            if result:
                return dict(result[0])
            return {}
        except Exception as e:
            logger.warning(f"Failed to get transform stats: {e}")
            return {}

    def _delete_clean_data(self, start_date: date, end_date: date) -> int:
        """Delete existing clean data for date range."""
        if not self._backend.table_exists("bot_requests_daily"):
            return 0

        sql = f"""
            DELETE FROM bot_requests_daily
            WHERE request_date >= '{start_date.isoformat()}'
              AND request_date <= '{end_date.isoformat()}'
        """
        try:
            return self._backend.execute(sql)
        except Exception as e:
            logger.warning(f"Failed to delete clean data: {e}")
            return 0

    def _transform_and_load(self, start_date: date, end_date: date) -> dict:
        """Transform raw data and load into clean table."""
        # Build the transformation SQL
        transform_sql = self._build_transform_sql(start_date, end_date)

        # Execute transformation
        result = self._backend.query(transform_sql)

        if not result:
            return {"rows_transformed": 0, "duplicates_removed": 0}

        # Convert to list of dicts for batch insert
        records = [self._row_to_clean_record(row) for row in result]

        if not records:
            return {"rows_transformed": 0, "duplicates_removed": 0}

        # Use batch insert for efficiency
        rows_inserted = 0
        try:
            if hasattr(self._backend, "insert_clean_records"):
                rows_inserted = self._backend.insert_clean_records(records)
            else:
                # Fallback to individual inserts if batch not available
                for record in records:
                    insert_sql = self._build_insert_sql(record)
                    try:
                        self._backend.execute(insert_sql)
                        rows_inserted += 1
                    except Exception as e:
                        logger.warning(f"Failed to insert row: {e}")
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            # Fallback to individual inserts
            for record in records:
                insert_sql = self._build_insert_sql(record)
                try:
                    self._backend.execute(insert_sql)
                    rows_inserted += 1
                except Exception as insert_e:
                    logger.warning(f"Failed to insert row: {insert_e}")

        # Calculate duplicates removed
        raw_count = self._get_raw_count(start_date, end_date)
        duplicates = raw_count - rows_inserted

        return {
            "rows_transformed": rows_inserted,
            "duplicates_removed": max(0, duplicates),
        }

    def _row_to_clean_record(self, row: dict) -> dict:
        """Convert a query result row to a clean record for insertion."""
        # Calculate url_path_depth from url_path
        url_path = row.get("url_path", "")
        if url_path in ("/", "", None):
            url_path_depth = 0
        else:
            # Remove leading/trailing slashes and count segments
            path = str(url_path).strip("/")
            url_path_depth = len(path.split("/")) if path else 0

        return {
            "request_timestamp": row.get("request_timestamp"),
            "request_date": row.get("request_date"),
            "request_hour": row.get("request_hour"),
            "day_of_week": row.get("day_of_week"),
            "request_uri": row.get("request_uri"),
            "request_host": row.get("request_host"),
            "url_path": url_path,
            "url_path_depth": url_path_depth,
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
        """Build the transformation SQL query."""
        date_filter = self._sql.date_filter("EdgeStartTimestamp", start_date, end_date)

        # Build bot classification
        bot_case = self._build_bot_classification()

        # URL path extraction
        url_path = self._sql.url_path("ClientRequestURI")

        # Build SQL
        if self._backend_type == "sqlite":
            sql = f"""
                WITH deduplicated AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY EdgeStartTimestamp, ClientIP, ClientRequestURI, ClientRequestHost
                            ORDER BY EdgeStartTimestamp
                        ) as row_num
                    FROM raw_bot_requests
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
        else:
            # SQLite syntax (for future compatibility)
            sql = f"""
                WITH deduplicated AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY EdgeStartTimestamp, ClientIP, ClientRequestURI, ClientRequestHost
                            ORDER BY EdgeStartTimestamp
                        ) as row_num
                    FROM raw_bot_requests
                    WHERE {date_filter}
                      AND EdgeStartTimestamp IS NOT NULL
                      AND EdgeResponseStatus IS NOT NULL
                ),
                filtered AS (
                    SELECT * FROM deduplicated WHERE row_num = 1
                )
                SELECT
                    EdgeStartTimestamp as request_timestamp,
                    DATE(EdgeStartTimestamp) as request_date,
                    EXTRACT(HOUR FROM EdgeStartTimestamp) as request_hour,
                    FORMAT_DATE('%A', DATE(EdgeStartTimestamp)) as day_of_week,
                    ClientRequestURI as request_uri,
                    ClientRequestHost as request_host,
                    REGEXP_EXTRACT(ClientRequestURI, r'^([^?#]*)') as url_path,
                    ClientRequestUserAgent as user_agent_raw,
                    {bot_case['bot_name']} as bot_name,
                    {bot_case['bot_provider']} as bot_provider,
                    {bot_case['bot_category']} as bot_category,
                    BotScore as bot_score,
                    COALESCE(VerifiedBot, FALSE) as is_verified_bot,
                    ClientCountry as crawler_country,
                    EdgeResponseStatus as response_status,
                    {self._sql.status_category('EdgeResponseStatus')} as response_status_category,
                    CURRENT_TIMESTAMP() as _processed_at
                FROM filtered
                WHERE {bot_case['bot_name']} != 'Unknown'
            """

        return sql

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

    def _build_insert_sql(self, row: dict) -> str:
        """Build INSERT statement for a transformed row."""
        columns = [
            "request_timestamp",
            "request_date",
            "request_hour",
            "day_of_week",
            "request_uri",
            "request_host",
            "url_path",
            "url_path_depth",
            "user_agent_raw",
            "bot_name",
            "bot_provider",
            "bot_category",
            "bot_score",
            "is_verified_bot",
            "crawler_country",
            "response_status",
            "response_status_category",
            "_processed_at",
        ]

        values = []
        for col in columns:
            val = row.get(col)
            if val is None:
                values.append("NULL")
            elif isinstance(val, (int, float)):
                values.append(str(val))
            elif isinstance(val, bool):
                values.append("1" if val else "0")
            else:
                # Escape single quotes
                escaped = str(val).replace("'", "''")
                values.append(f"'{escaped}'")

        return f"""
            INSERT INTO bot_requests_daily ({', '.join(columns)})
            VALUES ({', '.join(values)})
        """

    def get_pipeline_status(self) -> dict:
        """Get current pipeline status."""
        self.initialize()

        raw_exists = self._backend.table_exists("raw_bot_requests")
        clean_exists = self._backend.table_exists("bot_requests_daily")

        return {
            "backend_type": self._backend_type,
            "raw_table_exists": raw_exists,
            "raw_row_count": (
                self._backend.get_table_row_count("raw_bot_requests")
                if raw_exists
                else 0
            ),
            "clean_table_exists": clean_exists,
            "clean_row_count": (
                self._backend.get_table_row_count("bot_requests_daily")
                if clean_exists
                else 0
            ),
        }

