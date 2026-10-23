"""
ETL Pipeline Orchestrator.

Coordinates the complete ETL pipeline for LLM bot traffic:
Extract → Transform → Load

Provides:
- Full pipeline execution
- Incremental processing by date range
- Progress logging
- Error handling and recovery
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from ..config.logging_config import log_with_context
from ..storage import StorageError
from .exceptions import PipelineError
from .extract import LLMBotExtractor
from .transform import LLMBotTransformer

# Configure logger
logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result of a complete pipeline run."""

    success: bool
    start_date: date
    end_date: date
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
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


class ETLPipeline:
    """
    Complete ETL pipeline for LLM bot traffic analysis.

    Orchestrates:
    1. Extraction from raw BigQuery table
    2. Transformation (dedup, enrich, classify)
    3. Loading into clean table

    Supports:
    - Full reprocessing (truncate and reload)
    - Incremental processing (append new data)
    - Date-based partitioning
    """

    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[Path] = None,
    ):
        """
        Initialize the ETL pipeline.

        Args:
            project_id: GCP project ID
            credentials_path: Path to service account JSON key (uses ADC if None)
        """
        self.project_id = project_id
        self.credentials_path = credentials_path

        # Initialize components
        self.extractor = LLMBotExtractor(
            project_id=project_id,
            credentials_path=credentials_path,
        )
        self.transformer = LLMBotTransformer(
            project_id=project_id,
            credentials_path=credentials_path,
        )

        logger.info(f"ETL Pipeline initialized for project: {project_id}")

    def run(
        self,
        start_date: date,
        end_date: date,
        mode: str = "incremental",
        dry_run: bool = False,
    ) -> PipelineResult:
        """
        Run the complete ETL pipeline for a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            mode: 'incremental' (append) or 'full' (truncate target first)
            dry_run: If True, validate and preview without writing

        Returns:
            PipelineResult with success status and stats
        """
        result = PipelineResult(
            success=False,
            start_date=start_date,
            end_date=end_date,
        )

        # Validate inputs
        if start_date > end_date:
            result.errors.append(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )
            result.completed_at = datetime.now(timezone.utc)
            return result

        if mode not in ("incremental", "full"):
            result.errors.append(f"Invalid mode: {mode}. Use 'incremental' or 'full'")
            result.completed_at = datetime.now(timezone.utc)
            return result

        logger.info(f"Starting ETL pipeline: {start_date} to {end_date} (mode={mode})")

        try:
            # Step 1: Check source data
            logger.info("[1/4] Checking source data...")
            if not self.extractor.table_exists():
                result.errors.append("Source table does not exist")
                result.completed_at = datetime.now(timezone.utc)
                return result

            raw_count = self._get_source_row_count(start_date, end_date)
            if raw_count is None:
                result.errors.append("Failed to query source row count")
                result.completed_at = datetime.now(timezone.utc)
                return result

            result.raw_rows = raw_count
            logger.info(f"  Found {raw_count:,} rows in source table")

            if raw_count == 0:
                logger.warning("  No data found for date range")
                result.success = True
                result.completed_at = datetime.now(timezone.utc)
                return result

            # Step 2: Preview transformation (validation)
            logger.info("[2/4] Validating transformation...")
            stats = self.transformer.get_transformation_stats(start_date, end_date)
            logger.info(f"  Success responses: {stats.get('success_responses', 0):,}")

            if dry_run:
                logger.info("[DRY RUN] Skipping actual transformation")
                result.success = True
                result.completed_at = datetime.now(timezone.utc)
                return result

            # Step 3: Delete existing data if full mode
            if mode == "full":
                logger.info("[3/4] Deleting existing data for date range...")
                deleted = self.transformer.delete_date_range(start_date, end_date)
                logger.info(f"  Deleted {deleted:,} existing rows")
            else:
                logger.info("[3/4] Incremental mode - skipping delete")

            # Step 4: Transform and load
            logger.info("[4/4] Transforming and loading data...")
            write_disposition = (
                "WRITE_APPEND"  # Always append (we deleted if full mode)
            )

            transform_result = self.transformer.transform_and_load(
                start_date=start_date,
                end_date=end_date,
                write_disposition=write_disposition,
            )

            if not transform_result.success:
                result.errors.append(f"Transform failed: {transform_result.error}")
                result.completed_at = datetime.now(timezone.utc)
                return result

            result.transformed_rows = transform_result.rows_transformed
            result.duplicates_removed = transform_result.rows_deduplicated
            result.success = True

            logger.info(f"  Transformed {result.transformed_rows:,} rows")
            logger.info(f"  Removed {result.duplicates_removed:,} duplicates")

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

    def run_daily(self, target_date: Optional[date] = None) -> PipelineResult:
        """
        Run pipeline for a single day.

        Args:
            target_date: Date to process (defaults to yesterday)

        Returns:
            PipelineResult
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)

        logger.info(f"Running daily pipeline for {target_date}")
        return self.run(
            start_date=target_date,
            end_date=target_date,
            mode="full",  # Full mode for single day to ensure idempotency
        )

    def run_backfill(
        self,
        start_date: date,
        end_date: date,
        batch_days: int = 7,
    ) -> list[PipelineResult]:
        """
        Run pipeline in batches for a large date range.

        Useful for backfilling historical data.

        Args:
            start_date: Start date
            end_date: End date
            batch_days: Days per batch (must be >= 1)

        Returns:
            List of PipelineResult for each batch

        Raises:
            ValueError: If start_date > end_date or batch_days < 1
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )
        if batch_days < 1:
            raise ValueError(f"batch_days must be >= 1, got {batch_days}")

        results = []
        current_start = start_date

        logger.info(
            f"Starting backfill: {start_date} to {end_date} ({batch_days}-day batches)"
        )

        while current_start <= end_date:
            current_end = min(
                current_start + timedelta(days=batch_days - 1),
                end_date,
            )

            logger.info(f"Processing batch: {current_start} to {current_end}")
            result = self.run(
                start_date=current_start,
                end_date=current_end,
                mode="full",
            )
            results.append(result)

            if not result.success:
                log_with_context(
                    logger,
                    logging.ERROR,
                    "Batch failed, stopping backfill: %s",
                    result.errors,
                    date_range=(current_start, current_end),
                    records_processed=result.transformed_rows,
                )
                break

            current_start = current_end + timedelta(days=1)

        # Summary
        total_rows = sum(r.transformed_rows for r in results)
        successful = sum(1 for r in results if r.success)
        logger.info(
            f"Backfill complete: {successful}/{len(results)} batches, "
            f"{total_rows:,} total rows"
        )

        return results

    def _get_source_row_count(self, start_date: date, end_date: date) -> Optional[int]:
        """Get source row count. Returns None on query failure, 0 for no data."""
        try:
            stats = self.extractor.get_date_range_stats(start_date, end_date)
            return stats.get("row_count", 0) or 0
        except (StorageError, PipelineError) as e:
            log_with_context(
                logger,
                logging.ERROR,
                "Failed to query source row count for %s->%s: %s",
                start_date,
                end_date,
                e,
                date_range=(start_date, end_date),
            )
            return None

    def get_pipeline_status(self) -> dict:
        """
        Get current status of pipeline components.

        Returns:
            Dictionary with status information
        """
        return {
            "project_id": self.project_id,
            "source_table_exists": self.extractor.table_exists(),
            "source_row_count": self.extractor.get_table_row_count(),
            "target_table": self.transformer.target_table_id,
        }


def setup_logging(
    level: int = logging.INFO,
    json_logs: "bool | None" = None,
) -> None:
    """
    Configure logging for the pipeline.

    Args:
        level: Logging level (default: INFO)
        json_logs: Use JSON formatter when True. When None, reads from
            JSON_LOGS env or Settings. Default preserves human-readable format.
    """
    from ..config.logging_config import setup_logging as _setup

    _setup(level=level, json_logs=json_logs)
