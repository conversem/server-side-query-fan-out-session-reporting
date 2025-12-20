"""
Abstract base class for storage backends.

Provides a unified interface for data storage operations,
enabling switching between SQLite  and SQLite (production).
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.

    All storage implementations (SQLite, SQLite) must implement this interface
    to ensure consistent behavior across backends.
    """

    @property
    @abstractmethod
    def backend_type(self) -> str:
        """Return the backend type identifier (e.g., 'sqlite')."""
        pass

    @abstractmethod
    def initialize(self) -> None:
        """
        Initialize the storage backend.

        Creates tables and indexes if they don't exist.
        Should be idempotent - safe to call multiple times.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close connections and release resources.

        Should be called when the backend is no longer needed.
        """
        pass

    @abstractmethod
    def insert_raw_records(self, records: list[dict]) -> int:
        """
        Insert raw log records into the raw data table.

        Args:
            records: List of dictionaries containing raw log data.
                     Expected fields match the raw schema (EdgeStartTimestamp,
                     ClientRequestURI, etc.)

        Returns:
            Number of records successfully inserted.

        Raises:
            StorageError: If insertion fails.
        """
        pass

    @abstractmethod
    def query(
        self,
        sql: str,
        params: Optional[dict] = None,
    ) -> list[dict]:
        """
        Execute a query and return results as a list of dictionaries.

        Args:
            sql: SQL query string (may contain parameter placeholders)
            params: Optional dictionary of query parameters

        Returns:
            List of dictionaries, one per row.

        Raises:
            StorageError: If query execution fails.

        Note:
            Parameter syntax differs between backends:
            - SQLite: :param_name or ?
            - SQLite: @param_name
        """
        pass

    @abstractmethod
    def execute(
        self,
        sql: str,
        params: Optional[dict] = None,
    ) -> int:
        """
        Execute a statement (INSERT, UPDATE, DELETE, DDL).

        Args:
            sql: SQL statement string
            params: Optional dictionary of parameters

        Returns:
            Number of affected rows (0 for DDL statements).

        Raises:
            StorageError: If execution fails.
        """
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the storage backend.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists, False otherwise.
        """
        pass

    @abstractmethod
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get the total row count for a table.

        Args:
            table_name: Name of the table

        Returns:
            Number of rows in the table.

        Raises:
            StorageError: If table doesn't exist or query fails.
        """
        pass

    def get_date_range_count(
        self,
        table_name: str,
        date_column: str,
        start_date: date,
        end_date: date,
    ) -> int:
        """
        Get row count for a specific date range.

        Default implementation uses query(); backends may override
        for better performance.

        Args:
            table_name: Name of the table
            date_column: Name of the date column to filter
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            Number of rows in the date range.
        """
        # Default implementation - backends can override for optimization
        sql = f"""
            SELECT COUNT(*) as count
            FROM {table_name}
            WHERE {date_column} >= :start_date
              AND {date_column} <= :end_date
        """
        result = self.query(
            sql,
            {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )
        return result[0]["count"] if result else 0

    def health_check(self) -> dict:
        """
        Perform a health check on the storage backend.

        Returns:
            Dictionary with health status information:
            {
                "healthy": bool,
                "backend_type": str,
                "message": str,
                "details": dict
            }
        """
        try:
            # Basic connectivity test
            self.query("SELECT 1 as test")
            return {
                "healthy": True,
                "backend_type": self.backend_type,
                "message": "Backend is operational",
                "details": {},
            }
        except Exception as e:
            return {
                "healthy": False,
                "backend_type": self.backend_type,
                "message": f"Health check failed: {str(e)}",
                "details": {"error": str(e)},
            }

    def __enter__(self) -> "StorageBackend":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - ensures resources are released."""
        self.close()


class StorageError(Exception):
    """Base exception for storage backend errors."""

    pass


class StorageConnectionError(StorageError):
    """Raised when connection to storage backend fails."""

    pass


class QueryError(StorageError):
    """Raised when a query fails to execute."""

    pass


class SchemaError(StorageError):
    """Raised when there's a schema-related error."""

    pass

