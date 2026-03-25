"""
Local ETL Pipeline with dual-backend support.

Supports two local processing modes:
- local_sqlite: SQLite raw -> SQLite SQL transform -> SQLite clean
- local_bq_buffered: SQLite raw -> SQLite SQL transform -> BigQuery clean
"""

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from ..config.constants import TABLE_CLEAN_BOT_REQUESTS, TABLE_RAW_BOT_REQUESTS
from ..config.logging_config import log_with_context
from ..storage import StorageBackend, StorageError, get_backend
from .checkpoint import CheckpointManager
from .exceptions import PipelineError
from .sql_compat import SQLBuilder
from .stages import setup_logging  # noqa: F401 — re-exported for backward compatibility
from .stages import (
    CheckpointOpsMixin,
    DataOpsMixin,
    InsertMixin,
    LocalPipelineResult,
    SqlBuilderMixin,
)

logger = logging.getLogger(__name__)


class LocalPipeline(
    SqlBuilderMixin,
    InsertMixin,
    DataOpsMixin,
    CheckpointOpsMixin,
):
    """
    Local ETL pipeline with dual-backend support.

    Pipeline stages:
    1. Extract: Read raw data from raw_bot_requests (via raw backend)
    2. Transform: Deduplicate, classify, enrich (SQL against raw backend)
    3. Load: Insert into bot_requests_daily (via output backend)

    The raw backend (typically SQLite) holds raw_bot_requests and runs the
    transform SQL. The output backend (SQLite or BigQuery) receives the
    cleaned records. When output_backend is None, both roles use the same
    backend (backward-compatible local_sqlite mode).
    """

    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        output_backend: Optional[StorageBackend] = None,
        backend_type: str = "sqlite",
        db_path: Optional[Path] = None,
        checkpoint_path: Optional[Path] = None,
    ):
        """
        Initialize the local pipeline.

        Args:
            backend: Pre-initialized raw StorageBackend (optional)
            output_backend: Separate backend for clean output (optional).
                            When None, output goes to the same backend as raw.
            backend_type: Backend type if creating new ('sqlite')
            db_path: Path to SQLite database (for sqlite backend)
            checkpoint_path: Path to checkpoint JSON for local_bq_buffered resume.
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

        if output_backend is not None:
            self._output_backend = output_backend
            self._owns_output_backend = False
        else:
            self._output_backend = self._backend
            self._owns_output_backend = False

        self._backend_type = self._backend.backend_type
        self._sql = SQLBuilder(self._backend_type)

        from ..config.settings import UrlFilteringSettings

        try:
            from ..config.settings import get_settings

            settings = get_settings()
            self._url_settings = settings.url_filtering
        except Exception:
            self._url_settings = UrlFilteringSettings()

        self._initialized = False
        self._checkpoint_manager: Optional[CheckpointManager] = (
            CheckpointManager(checkpoint_path) if checkpoint_path else None
        )

        out_type = self._output_backend.backend_type
        if out_type != self._backend_type:
            logger.info(
                "LocalPipeline initialized: raw=%s, output=%s",
                self._backend_type,
                out_type,
            )
        else:
            logger.info("LocalPipeline initialized with %s backend", self._backend_type)

    def initialize(self) -> None:
        """Initialize the backend(s) (create tables if needed)."""
        if not self._initialized:
            self._backend.initialize()
            if self._output_backend is not self._backend:
                self._output_backend.initialize()
            self._initialized = True

    def close(self) -> None:
        """Close backend connections."""
        if self._owns_backend:
            self._backend.close()
        if self._owns_output_backend and self._output_backend is not self._backend:
            self._output_backend.close()

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
        """Run the ETL pipeline for a date range."""
        self.initialize()

        result = LocalPipelineResult(
            success=False,
            start_date=start_date,
            end_date=end_date,
        )

        errors = self._validate_run_inputs(start_date, end_date, mode)
        if errors:
            result.errors.extend(errors)
            result.completed_at = datetime.now(timezone.utc)
            return result

        logger.info(
            f"Starting local pipeline: {start_date} to {end_date} (mode={mode})"
        )

        try:
            self._execute_pipeline(start_date, end_date, mode, dry_run, result)
        except (StorageError, PipelineError) as e:
            log_with_context(
                logger,
                logging.ERROR,
                "Pipeline failed during %s->%s: %s",
                start_date,
                end_date,
                e,
                exc_info=True,
                date_range=(start_date, end_date),
                records_processed=result.raw_rows,
            )
            result.errors.append(str(e))

        result.completed_at = datetime.now(timezone.utc)

        if result.success:
            logger.info(
                f"Pipeline completed successfully in {result.duration_seconds:.1f}s"
            )
        else:
            log_with_context(
                logger,
                logging.ERROR,
                "Pipeline failed: %s",
                result.errors,
                date_range=(start_date, end_date),
                records_processed=result.transformed_rows,
            )

        return result

    @staticmethod
    def _validate_run_inputs(start_date: date, end_date: date, mode: str) -> list[str]:
        """Return validation errors for run() parameters."""
        errors: list[str] = []
        if start_date > end_date:
            errors.append(f"start_date ({start_date}) must be <= end_date ({end_date})")
        if mode not in ("incremental", "full"):
            errors.append(f"Invalid mode: {mode}. Use 'incremental' or 'full'")
        return errors

    def _execute_pipeline(
        self,
        start_date: date,
        end_date: date,
        mode: str,
        dry_run: bool,
        result: LocalPipelineResult,
    ) -> None:
        """Core pipeline logic: check, analyse, delete, transform & load.

        Raises:
            StorageError: If source data check or analysis queries fail.
        """
        logger.info("[1/4] Checking source data...")
        try:
            raw_count = self._get_raw_count(start_date, end_date)
        except StorageError:
            log_with_context(
                logger,
                logging.ERROR,
                "Storage error during source data check",
                exc_info=True,
                date_range=(start_date, end_date),
            )
            raise
        result.raw_rows = raw_count
        logger.info(f"  Found {raw_count:,} rows in {TABLE_RAW_BOT_REQUESTS}")

        if raw_count == 0:
            logger.warning("  No data found for date range")
            result.success = True
            return

        logger.info("[2/4] Analyzing data...")
        try:
            stats = self._get_transform_stats(start_date, end_date)
        except StorageError:
            log_with_context(
                logger,
                logging.ERROR,
                "Storage error during data analysis",
                exc_info=True,
                date_range=(start_date, end_date),
            )
            raise
        logger.info(f"  Unique user agents: {stats.get('unique_user_agents', 0):,}")

        if dry_run:
            logger.info("[DRY RUN] Skipping transformation")
            result.success = True
            return

        if self._checkpoint_manager and self._output_backend.backend_type == "bigquery":
            self._execute_with_checkpoint(start_date, end_date, mode, raw_count, result)
        else:
            if mode == "full":
                logger.info("[3/4] Deleting existing data for date range...")
                deleted = self._delete_clean_data(start_date, end_date)
                logger.info(f"  Deleted {deleted:,} existing rows")
            else:
                logger.info("[3/4] Incremental mode - skipping delete")

            logger.info("[4/4] Transforming and loading...")
            transform_result = self._transform_and_load(
                start_date, end_date, raw_count=raw_count
            )

            result.transformed_rows = transform_result["rows_transformed"]
            result.duplicates_removed = transform_result.get("duplicates_removed", 0)
            result.success = True

            logger.info(f"  Transformed {result.transformed_rows:,} rows")
            logger.info(f"  Removed {result.duplicates_removed:,} duplicates")

    def run_daily(self, target_date: Optional[date] = None) -> LocalPipelineResult:
        """
        Run pipeline for a single day.

        Args:
            target_date: Date to process (defaults to yesterday)

        Returns:
            LocalPipelineResult
        """
        if target_date is None:
            from datetime import timedelta

            target_date = date.today() - timedelta(days=1)

        logger.info(f"Running daily pipeline for {target_date}")
        return self.run(
            start_date=target_date,
            end_date=target_date,
            mode="full",
        )

    def get_pipeline_status(self) -> dict:
        """Get current pipeline status."""
        self.initialize()

        raw_exists = self._backend.table_exists(TABLE_RAW_BOT_REQUESTS)
        clean_exists = self._output_backend.table_exists(TABLE_CLEAN_BOT_REQUESTS)

        return {
            "raw_backend_type": self._backend_type,
            "output_backend_type": self._output_backend.backend_type,
            "raw_table_exists": raw_exists,
            "raw_row_count": (
                self._backend.get_table_row_count(TABLE_RAW_BOT_REQUESTS)
                if raw_exists
                else 0
            ),
            "clean_table_exists": clean_exists,
            "clean_row_count": (
                self._output_backend.get_table_row_count(TABLE_CLEAN_BOT_REQUESTS)
                if clean_exists
                else 0
            ),
        }
