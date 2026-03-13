"""
Base class for local dashboard queries.

Provides backend initialization, lifecycle management, and query execution.
"""

import logging
from pathlib import Path
from typing import Any, Optional

from ...pipeline.sql_compat import SQLBuilder
from ...storage import StorageBackend, get_backend

logger = logging.getLogger(__name__)


class LocalDashboardQueriesBase:
    """Base class: backend setup, lifecycle, and low-level query execution."""

    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        backend_type: str = "sqlite",
        db_path: Optional[Path] = None,
    ):
        """
        Initialize local dashboard queries.

        Args:
            backend: Pre-initialized StorageBackend (optional)
            backend_type: Backend type if creating new ('sqlite' or 'bigquery')
            db_path: Path to SQLite database (for sqlite backend)
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

        self._backend_type = self._backend.backend_type
        self._sql = SQLBuilder(self._backend_type)
        self._initialized = False

        logger.info(
            f"LocalDashboardQueries initialized with {self._backend_type} backend"
        )

    def initialize(self) -> None:
        """Initialize the backend."""
        if not self._initialized:
            self._backend.initialize()
            self._initialized = True

    def close(self) -> None:
        """Close the backend connection."""
        if self._owns_backend:
            self._backend.close()

    def __enter__(self) -> "LocalDashboardQueriesBase":
        """Context manager entry."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    @staticmethod
    def _domain_filter(domain: Optional[str] = None) -> str:
        """Return a SQL AND clause for domain filtering, or empty string."""
        if domain:
            safe = domain.replace("'", "''")
            return f"AND domain = '{safe}'"
        return ""

    def _execute_query(
        self, query: str, params: Optional[dict] = None
    ) -> list[dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        self.initialize()
        return self._backend.query(query, params)
