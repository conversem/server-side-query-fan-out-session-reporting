"""
Storage abstraction layer for LLM bot traffic pipeline.

Provides a unified interface for data storage operations using SQLite.

Usage:
    from llm_bot_pipeline.storage import get_backend

    # Get backend from configuration
    backend = get_backend()

    # Or explicitly specify backend
    backend = get_backend('sqlite', db_path='data/logs.db')

    # Use as context manager
    with get_backend() as backend:
        backend.initialize()
        results = backend.query("SELECT * FROM table")
"""

from .base import (
    QueryError,
    SchemaError,
    StorageBackend,
    StorageConnectionError,
    StorageError,
)
from .factory import (
    get_backend,
    is_backend_available,
    list_available_backends,
    register_backend,
)

__all__ = [
    # Base classes and exceptions
    "StorageBackend",
    "StorageError",
    "StorageConnectionError",
    "QueryError",
    "SchemaError",
    # Factory functions
    "get_backend",
    "register_backend",
    "list_available_backends",
    "is_backend_available",
]
