"""
Storage backend factory.

Provides factory function to create SQLite storage backend.
"""

import logging
from pathlib import Path
from typing import Optional

from .base import StorageBackend, StorageError

logger = logging.getLogger(__name__)

# Registry of available backends
_BACKEND_REGISTRY: dict[str, type[StorageBackend]] = {}


def register_backend(backend_type: str, backend_class: type[StorageBackend]) -> None:
    """
    Register a storage backend class.

    Args:
        backend_type: Backend identifier (e.g., 'sqlite')
        backend_class: Class implementing StorageBackend interface
    """
    _BACKEND_REGISTRY[backend_type.lower()] = backend_class
    logger.debug(f"Registered storage backend: {backend_type}")


def get_backend(
    backend_type: Optional[str] = None,
    **kwargs,
) -> StorageBackend:
    """
    Get a storage backend instance based on configuration.

    Args:
        backend_type: Backend type ('sqlite').
                      If None, loads from settings.
        **kwargs: Additional arguments passed to backend constructor.
                  For SQLite: db_path

    Returns:
        Initialized StorageBackend instance.

    Raises:
        StorageError: If backend type is not supported or initialization fails.

    Examples:
        # Get backend from settings
        backend = get_backend()

        # Explicitly request SQLite
        backend = get_backend('sqlite', db_path='data/logs.db')
    """
    # Get backend type from settings if not specified
    if backend_type is None:
        from ..config.settings import get_settings

        settings = get_settings()
        backend_type = settings.storage_backend

    backend_type = backend_type.lower()

    # Lazy-load backend implementations
    if backend_type not in _BACKEND_REGISTRY:
        _load_backend(backend_type)

    if backend_type not in _BACKEND_REGISTRY:
        available = list(_BACKEND_REGISTRY.keys()) if _BACKEND_REGISTRY else ["none"]
        raise StorageError(
            f"Unknown storage backend: '{backend_type}'. "
            f"Available backends: {', '.join(available)}"
        )

    backend_class = _BACKEND_REGISTRY[backend_type]

    # Get default configuration from settings if needed
    if not kwargs:
        kwargs = _get_default_kwargs(backend_type)

    try:
        backend = backend_class(**kwargs)
        logger.info(f"Created {backend_type} storage backend")
        return backend
    except Exception as e:
        raise StorageError(f"Failed to create {backend_type} backend: {e}") from e


def _load_backend(backend_type: str) -> None:
    """
    Lazy-load a backend implementation.

    Args:
        backend_type: Backend type to load
    """
    if backend_type == "sqlite":
        try:
            from .sqlite_backend import SQLiteBackend

            register_backend("sqlite", SQLiteBackend)
        except ImportError as e:
            logger.warning(f"SQLite backend not available: {e}")


def _get_default_kwargs(backend_type: str) -> dict:
    """
    Get default constructor arguments from settings.

    Args:
        backend_type: Backend type

    Returns:
        Dictionary of constructor arguments
    """
    from ..config.settings import get_settings

    settings = get_settings()

    if backend_type == "sqlite":
        return {
            "db_path": Path(settings.sqlite_db_path),
        }
    else:
        return {}


def list_available_backends() -> list[str]:
    """
    List all registered backend types.

    Returns:
        List of backend type identifiers.
    """
    # Attempt to load all known backends
    for backend_type in ["sqlite"]:
        if backend_type not in _BACKEND_REGISTRY:
            _load_backend(backend_type)

    return list(_BACKEND_REGISTRY.keys())


def is_backend_available(backend_type: str) -> bool:
    """
    Check if a specific backend is available.

    Args:
        backend_type: Backend type to check

    Returns:
        True if backend can be loaded, False otherwise.
    """
    backend_type = backend_type.lower()

    if backend_type not in _BACKEND_REGISTRY:
        _load_backend(backend_type)

    return backend_type in _BACKEND_REGISTRY
