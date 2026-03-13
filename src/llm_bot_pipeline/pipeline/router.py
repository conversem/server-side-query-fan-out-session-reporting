"""
Unified pipeline router -- dispatches to the correct pipeline based on processing_mode.

Reads processing_mode from Settings (YAML / env var) and instantiates:
- local_sqlite      -> LocalPipeline(backend=sqlite)
- local_bq_buffered -> LocalPipeline(backend=sqlite, output_backend=bigquery)
- local_bq_streaming -> StreamingPipeline(output_backend=bigquery)
- gcp_bq            -> ETLPipeline (existing BQ orchestrator)
"""

import logging
import uuid
from datetime import date
from pathlib import Path
from typing import Iterator, Optional, Union

from ..config.constants import DEFAULT_STREAMING_BATCH_SIZE, VALID_PROCESSING_MODES
from ..config.logging_config import set_log_context
from ..config.settings import get_settings
from ..storage import StorageBackend, get_backend

logger = logging.getLogger(__name__)


def _make_bq_backend(settings=None) -> StorageBackend:
    """Create a BigQuery StorageBackend from settings."""
    settings = settings or get_settings()
    return get_backend(
        "bigquery",
        project_id=settings.gcp_project_id,
        credentials_path=(
            str(settings.service_account_key_path)
            if settings.service_account_key_path.exists()
            else None
        ),
        dataset_raw=settings.dataset_raw,
        dataset_report=settings.dataset_report,
        location=settings.gcp_location,
    )


def _make_sqlite_backend(settings=None, db_path=None) -> StorageBackend:
    """Create a SQLite StorageBackend from settings."""
    settings = settings or get_settings()
    path = Path(db_path) if db_path else Path(settings.sqlite_db_path)
    return get_backend("sqlite", db_path=path)


def processing_mode_to_backend_type(mode: str) -> str:
    """Map processing_mode to backend type for get_backend (session aggregation, sitemap)."""
    if mode == "local_sqlite":
        return "sqlite"
    return "bigquery"  # local_bq_buffered, local_bq_streaming, gcp_bq


def run_pipeline(
    start_date: date,
    end_date: date,
    processing_mode: Optional[str] = None,
    records: Optional[Iterator] = None,
    dry_run: bool = False,
    **kwargs,
) -> Union[dict, object]:
    """Dispatch to the correct pipeline based on processing_mode.

    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        processing_mode: Override processing_mode from config (optional)
        records: Iterator of IngestionRecord (required for local_bq_streaming)
        dry_run: If True, preview without writing
        **kwargs: Extra args forwarded to the pipeline (e.g. mode for incremental/full)

    Returns:
        Pipeline-specific result object (LocalPipelineResult,
        StreamingPipelineResult, PipelineResult, or dict)
    """
    settings = get_settings()
    mode = processing_mode or settings.processing_mode

    if mode not in VALID_PROCESSING_MODES:
        raise ValueError(
            f"Invalid processing_mode '{mode}'. "
            f"Must be one of {VALID_PROCESSING_MODES}"
        )

    execution_id = str(uuid.uuid4())[:8]
    set_log_context(execution_id=execution_id)

    logger.info("Pipeline router: mode=%s, %s to %s", mode, start_date, end_date)

    db_path = kwargs.pop("db_path", None)

    if mode == "local_sqlite":
        return _run_local_sqlite(
            start_date, end_date, dry_run, settings, db_path=db_path, **kwargs
        )

    elif mode == "local_bq_buffered":
        return _run_local_bq_buffered(
            start_date, end_date, dry_run, settings, db_path=db_path, **kwargs
        )

    elif mode == "local_bq_streaming":
        if records is None:
            raise ValueError(
                "local_bq_streaming requires an IngestionRecord iterator "
                "via the 'records' parameter"
            )
        return _run_local_bq_streaming(
            records, start_date, end_date, settings, **kwargs
        )

    elif mode == "gcp_bq":
        return _run_gcp_bq(start_date, end_date, dry_run, settings, **kwargs)


def _run_local_sqlite(start_date, end_date, dry_run, settings, db_path=None, **kwargs):
    """local_sqlite: SQLite raw -> SQLite SQL transform -> SQLite clean."""
    from .local_pipeline import LocalPipeline

    sqlite = _make_sqlite_backend(settings, db_path=db_path)
    pipeline = LocalPipeline(backend=sqlite)
    try:
        return pipeline.run(start_date, end_date, dry_run=dry_run, **kwargs)
    finally:
        pipeline.close()


def _run_local_bq_buffered(
    start_date, end_date, dry_run, settings, db_path=None, **kwargs
):
    """local_bq_buffered: SQLite raw -> SQLite SQL transform -> BigQuery clean."""
    from .local_pipeline import LocalPipeline

    sqlite = _make_sqlite_backend(settings, db_path=db_path)
    bq = _make_bq_backend(settings)
    checkpoint_path = Path(settings.checkpoint_path)
    pipeline = LocalPipeline(
        backend=sqlite,
        output_backend=bq,
        checkpoint_path=checkpoint_path,
    )
    try:
        return pipeline.run(start_date, end_date, dry_run=dry_run, **kwargs)
    finally:
        pipeline.close()
        bq.close()


def _run_local_bq_streaming(records, start_date, end_date, settings, **kwargs):
    """local_bq_streaming: Memory -> PythonTransformer -> BigQuery clean."""
    from .streaming_pipeline import StreamingPipeline

    bq = _make_bq_backend(settings)
    batch_size = kwargs.pop("batch_size", DEFAULT_STREAMING_BATCH_SIZE)
    pipeline = StreamingPipeline(output_backend=bq, batch_size=batch_size)
    try:
        return pipeline.run(records, start_date=start_date, end_date=end_date)
    finally:
        bq.close()


def _run_gcp_bq(start_date, end_date, dry_run, settings, **kwargs):
    """gcp_bq: BigQuery raw -> BigQuery SQL transform -> BigQuery clean."""
    from .orchestrator import ETLPipeline

    creds_path = str(settings.service_account_key_path)
    if not settings.service_account_key_path.exists():
        creds_path = None

    pipeline = ETLPipeline(
        project_id=settings.gcp_project_id,
        credentials_path=creds_path,
        **kwargs,
    )
    return pipeline.run(start_date=start_date, end_date=end_date)


def get_pipeline_status(
    mode: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> dict:
    """Get pipeline status for the given processing mode.

    Supported modes: local_sqlite, local_bq_buffered, gcp_bq.
    local_bq_streaming has no persistent tables and returns empty status.

    Args:
        mode: Processing mode (default: from settings)
        db_path: Optional SQLite db path override (for local_sqlite/local_bq_buffered)

    Returns:
        Status dict with raw/clean table info.
    """
    settings = get_settings()
    mode = mode or settings.processing_mode

    if mode not in VALID_PROCESSING_MODES:
        raise ValueError(
            f"Invalid processing_mode '{mode}'. "
            f"Must be one of {VALID_PROCESSING_MODES}"
        )

    if mode == "local_bq_streaming":
        return {"mode": mode, "note": "Streaming mode has no persistent tables"}

    if mode in ("local_sqlite", "local_bq_buffered"):
        from .local_pipeline import LocalPipeline

        sqlite = _make_sqlite_backend(settings, db_path=db_path)
        if mode == "local_bq_buffered":
            bq = _make_bq_backend(settings)
            pipeline = LocalPipeline(
                backend=sqlite,
                output_backend=bq,
                checkpoint_path=Path(settings.checkpoint_path),
            )
        else:
            pipeline = LocalPipeline(backend=sqlite)
        try:
            return pipeline.get_pipeline_status()
        finally:
            pipeline.close()

    if mode == "gcp_bq":
        from .orchestrator import ETLPipeline

        creds_path = str(settings.service_account_key_path)
        if not settings.service_account_key_path.exists():
            creds_path = None
        pipeline = ETLPipeline(
            project_id=settings.gcp_project_id,
            credentials_path=creds_path,
        )
        return pipeline.get_pipeline_status()
