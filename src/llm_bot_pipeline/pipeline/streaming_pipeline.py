"""
Streaming pipeline for local_bq_streaming mode.

Wires IngestionRecord generators directly through PythonTransformer
to a BigQuery (or any StorageBackend) output, with no intermediate
SQLite step. Records flow through memory in batches.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from ..config.constants import DEFAULT_MAX_PENDING_BATCHES, DEFAULT_STREAMING_BATCH_SIZE
from ..config.logging_config import log_with_context
from ..config.settings import UrlFilteringSettings, get_settings
from ..ingestion.base import IngestionRecord
from ..monitoring.retry_handler import RetryConfig, RetryManager
from ..storage import StorageBackend, StorageError
from .exceptions import PipelineError
from .python_transformer import PythonTransformer
from .sql_utils import build_clean_insert_sql

logger = logging.getLogger(__name__)

DEFAULT_DEAD_LETTER_PATH = "data/dead_letter.jsonl"


@dataclass
class StreamingPipelineResult:
    """Result of a streaming pipeline run."""

    success: bool
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    records_in: int = 0
    records_transformed: int = 0
    records_filtered: int = 0
    duplicates: int = 0
    url_filtered: int = 0
    batches_flushed: int = 0
    dead_lettered_count: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "started_at": self.started_at.isoformat(),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "duration_seconds": self.duration_seconds,
            "records_in": self.records_in,
            "records_transformed": self.records_transformed,
            "records_filtered": self.records_filtered,
            "duplicates": self.duplicates,
            "url_filtered": self.url_filtered,
            "batches_flushed": self.batches_flushed,
            "dead_lettered_count": self.dead_lettered_count,
            "errors": self.errors,
        }


class StreamingPipeline:
    """Stream IngestionRecords through Python transform to any StorageBackend.

    This pipeline:
    1. Consumes an iterator of IngestionRecord
    2. Transforms each via PythonTransformer (bot classification, enrichment)
    3. Batches clean records and inserts them into output_backend

    No SQLite or disk I/O is involved -- pure memory streaming.
    """

    def __init__(
        self,
        output_backend: StorageBackend,
        batch_size: int = DEFAULT_STREAMING_BATCH_SIZE,
        max_pending_batches: int = DEFAULT_MAX_PENDING_BATCHES,
        retry_config: Optional[RetryConfig] = None,
        dead_letter_path: Optional[str] = None,
    ) -> None:
        try:
            url_settings = get_settings().url_filtering
        except Exception:
            url_settings = UrlFilteringSettings()
        self._transformer = PythonTransformer(url_filtering_settings=url_settings)
        self._output = output_backend
        self._batch_size = batch_size
        self._max_pending_batches = max_pending_batches
        self._retry_manager = RetryManager(config=retry_config or RetryConfig())
        self._dead_letter_path = Path(dead_letter_path or DEFAULT_DEAD_LETTER_PATH)

    def run(
        self,
        records: Iterator[IngestionRecord],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> StreamingPipelineResult:
        """Stream records through transform and into the output backend.

        Args:
            records: Iterator of IngestionRecord objects (from any adapter)
            start_date: Optional, for logging only
            end_date: Optional, for logging only

        Returns:
            StreamingPipelineResult with stats
        """
        result = StreamingPipelineResult(success=False)

        date_info = ""
        if start_date and end_date:
            date_info = f" ({start_date} to {end_date})"
        elif start_date:
            date_info = f" ({start_date})"

        logger.info("StreamingPipeline: starting%s", date_info)

        self._output.initialize()
        batch: list[dict] = []
        pending_batches: list[list[dict]] = []
        current_date: Optional[date] = None

        try:
            for record in records:
                result.records_in += 1
                record_date = record.timestamp.date()
                if current_date is not None and record_date != current_date:
                    self._transformer.reset_seen()
                    logger.debug("Reset deduplication set for new date boundary")
                if current_date is None or record_date != current_date:
                    current_date = record_date

                clean = self._transformer.transform(record)
                if clean is None:
                    continue

                batch.append(clean)
                if len(batch) >= self._batch_size:
                    pending_batches.append(batch)
                    batch = []
                    # Backpressure: block iterator until we're under the limit
                    if len(pending_batches) >= self._max_pending_batches:
                        logger.warning(
                            "Backpressure applied: max_pending_batches=%d reached, "
                            "flushing before consuming more records",
                            self._max_pending_batches,
                        )
                        while len(pending_batches) >= self._max_pending_batches:
                            self._flush_batch(pending_batches.pop(0), result)

            # Flush remaining pending batches and final partial batch
            for b in pending_batches:
                self._flush_batch(b, result)
            if batch:
                self._flush_batch(batch, result)

            stats = self._transformer.stats
            result.records_transformed = stats["transformed"]
            result.records_filtered = stats["filtered"]
            result.duplicates = stats["duplicates"]
            result.url_filtered = stats.get("url_filtered", 0)
            result.success = True

            logger.info(
                "StreamingPipeline: done -- %d in, %d transformed, "
                "%d filtered, %d url-filtered, %d dupes, %d batches, "
                "%d dead-lettered",
                result.records_in,
                result.records_transformed,
                result.records_filtered,
                result.url_filtered,
                result.duplicates,
                result.batches_flushed,
                result.dead_lettered_count,
            )

        except (StorageError, PipelineError) as e:
            log_with_context(
                logger,
                logging.ERROR,
                "StreamingPipeline failed: %s",
                e,
                exc_info=True,
                date_range=(start_date, end_date) if start_date and end_date else None,
                batch_size=self._batch_size,
                records_processed=result.records_in,
            )
            result.errors.append(str(e))

        result.completed_at = datetime.now(timezone.utc)
        return result

    def _flush_batch(self, batch: list[dict], result: StreamingPipelineResult) -> None:
        """Insert a batch of clean records into the output backend.

        Uses RetryManager for transient failures. On retry exhaustion the
        batch is persisted to a dead-letter file (JSON lines) and processing
        continues with the next batch.
        """

        def _do_insert() -> None:
            if hasattr(self._output, "insert_clean_records"):
                self._output.insert_clean_records(batch)
            else:
                for record in batch:
                    self._output.execute(self._build_insert_sql(record))

        retry_result = self._retry_manager.execute_with_retry(_do_insert)

        if retry_result.success:
            result.batches_flushed += 1
            return

        log_with_context(
            logger,
            logging.ERROR,
            "Batch insert failed after %d attempts, dead-lettering %d records",
            retry_result.attempts,
            len(batch),
            batch_size=len(batch),
            records_processed=result.records_in + result.dead_lettered_count,
        )
        result.errors.append(f"batch_insert: {retry_result.last_error}")
        self._write_dead_letter(batch)
        result.dead_lettered_count += len(batch)

    def _write_dead_letter(self, batch: list[dict]) -> None:
        """Append failed batch records to the dead-letter file as JSON lines."""
        try:
            self._dead_letter_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._dead_letter_path, "a") as f:
                for record in batch:
                    f.write(json.dumps(record, default=str) + "\n")
            logger.info(
                "Wrote %d records to dead-letter file %s",
                len(batch),
                self._dead_letter_path,
            )
        except OSError:
            log_with_context(
                logger,
                logging.ERROR,
                "Failed to write dead-letter file",
                exc_info=True,
                batch_size=len(batch),
            )

    @staticmethod
    def _build_insert_sql(row: dict) -> str:
        """Fallback INSERT for backends without insert_clean_records."""
        return build_clean_insert_sql(row)
