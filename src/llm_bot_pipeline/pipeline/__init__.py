"""ETL pipeline module supporting local, streaming, and BigQuery backends."""

import logging
from typing import TYPE_CHECKING, Union

from .exceptions import PipelineError
from .local_pipeline import LocalPipeline, LocalPipelineResult, setup_logging
from .python_transformer import PythonTransformer
from .router import run_pipeline
from .sql_compat import SQLBuilder
from .streaming_pipeline import StreamingPipeline, StreamingPipelineResult

if TYPE_CHECKING:
    from .orchestrator import ETLPipeline

logger = logging.getLogger(__name__)

__all__ = [
    # Exceptions
    "PipelineError",
    # Local Pipeline (SQLite / dual-backend)
    "LocalPipeline",
    "LocalPipelineResult",
    "setup_logging",
    # Streaming Pipeline (pure Python -> BQ)
    "StreamingPipeline",
    "StreamingPipelineResult",
    "PythonTransformer",
    # Unified Router
    "run_pipeline",
    # SQL Compatibility
    "SQLBuilder",
    # Factory
    "get_pipeline",
]

# BigQuery pipeline classes are lazy-imported to avoid requiring
# google-cloud-bigquery unless explicitly used.
_bigquery_available = False
try:
    from .extract import ExtractionResult, LLMBotExtractor
    from .orchestrator import ETLPipeline, PipelineResult
    from .transform import LLMBotTransformer, TransformResult

    _bigquery_available = True
    __all__ += [
        # BigQuery Pipeline
        "LLMBotExtractor",
        "ExtractionResult",
        "LLMBotTransformer",
        "TransformResult",
        "ETLPipeline",
        "PipelineResult",
    ]
except ImportError as e:
    logger.info("BigQuery pipeline not available (missing dependencies): %s", e)


def get_pipeline(backend=None, **kwargs) -> Union[LocalPipeline, "ETLPipeline"]:
    """Create the appropriate pipeline for a given storage backend.

    Args:
        backend: A StorageBackend instance, a backend type string
            ("sqlite" or "bigquery"), or None to read from settings.
        **kwargs: Extra arguments forwarded to the pipeline constructor.
            For SQLite: db_path, backend_type.
            For BigQuery: project_id, credentials_path.

    Returns:
        LocalPipeline for SQLite backends, ETLPipeline for BigQuery.

    Raises:
        ValueError: If backend type has no matching pipeline.
        ImportError: If BigQuery pipeline is requested but deps are missing.
    """
    from ..config.settings import get_settings
    from ..storage import StorageBackend

    if backend is None:
        backend = get_settings().storage_backend

    if isinstance(backend, str):
        backend_type = backend
        backend_instance = None
    elif isinstance(backend, StorageBackend):
        backend_type = backend.backend_type
        backend_instance = backend
    else:
        raise TypeError(f"Expected StorageBackend or str, got {type(backend).__name__}")

    if backend_type == "sqlite":
        if backend_instance is not None:
            return LocalPipeline(backend=backend_instance, **kwargs)
        return LocalPipeline(backend_type="sqlite", **kwargs)

    if backend_type == "bigquery":
        if not _bigquery_available:
            raise ImportError(
                "BigQuery pipeline requires google-cloud-bigquery. "
                "Install with: pip install '.[gcp]'"
            )
        if backend_instance is not None:
            project_id = getattr(backend_instance, "project_id", None)
            creds = getattr(backend_instance, "credentials_path", None)
            return ETLPipeline(project_id=project_id, credentials_path=creds, **kwargs)  # type: ignore[arg-type]
        if "project_id" not in kwargs:
            settings = get_settings()
            kwargs.setdefault("project_id", settings.gcp_project_id)
            creds = settings.service_account_key_path
            if creds.exists():
                kwargs.setdefault("credentials_path", str(creds))
        return ETLPipeline(**kwargs)

    raise ValueError(f"No pipeline for backend type: {backend_type}")
