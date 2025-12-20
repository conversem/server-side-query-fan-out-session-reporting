"""ETL pipeline module for SQLite backend."""

from .local_pipeline import LocalPipeline, LocalPipelineResult
from .sql_compat import SQLBuilder

__all__ = [
    # Local Pipeline (SQLite)
    "LocalPipeline",
    "LocalPipelineResult",
    # SQL Compatibility
    "SQLBuilder",
]
