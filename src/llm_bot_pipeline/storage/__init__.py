"""
Storage abstraction layer for LLM bot traffic pipeline.

Provides a unified interface for data storage operations supporting
multiple backends (SQLite, BigQuery, etc.).

Usage:
    from llm_bot_pipeline.storage import get_backend

    # Get backend from configuration
    backend = get_backend()

    # Or explicitly specify backend
    backend = get_backend('sqlite', db_path='data/logs.db')
    backend = get_backend('bigquery', project_id='my-project')

    # Use as context manager
    with get_backend() as backend:
        backend.initialize()
        results = backend.query("SELECT * FROM table")
"""

from .base import (
    BackendCapabilities,
    DiskSpaceError,
    QueryError,
    SchemaError,
    StorageBackend,
    StorageConnectionError,
    StorageError,
    validate_date_column,
    validate_table_name,
)
from .factory import (
    get_backend,
    is_backend_available,
    list_available_backends,
    register_backend,
)

__all__ = [
    # Base classes and exceptions
    "BackendCapabilities",
    "StorageBackend",
    "StorageError",
    "StorageConnectionError",
    "QueryError",
    "SchemaError",
    "DiskSpaceError",
    # Validation
    "validate_table_name",
    "validate_date_column",
    # Factory functions
    "get_backend",
    "register_backend",
    "list_available_backends",
    "is_backend_available",
]
