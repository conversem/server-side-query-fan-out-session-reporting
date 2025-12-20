"""ETL pipeline module for SQLite backend."""

from .local_pipeline import LocalPipeline, LocalPipelineResult, setup_logging
from .sql_compat import SQLBuilder

__all__ = [
    # Local Pipeline (SQLite)
    "LocalPipeline",
    "LocalPipelineResult",
    "setup_logging",
    # SQL Compatibility
    "SQLBuilder",
]
