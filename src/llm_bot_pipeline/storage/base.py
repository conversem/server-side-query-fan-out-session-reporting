"""
Abstract base class for storage backends.

Provides a unified interface for data storage operations,
enabling switching between SQLite, BigQuery, and other backends.

Two access patterns are supported:
    1. SQL interface: query() / execute()
    2. Record interface: insert_records() / read_records()

Backends declare capabilities via the ``capabilities`` property so pipeline
code can choose the correct code path (Python/Pandas vs SQL transforms).
"""

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

from ..config.constants import (
    VALID_DATE_COLUMNS,
    VALID_ORDER_BY_COLUMNS,
    VALID_TABLE_NAMES,
)


def validate_table_name(table_name: str) -> str:
    """Validate a table name against the whitelist.

    Raises:
        ValueError: If table_name is not in VALID_TABLE_NAMES.
    """
    if table_name not in VALID_TABLE_NAMES:
        raise ValueError(
            f"Invalid table name: '{table_name}'. "
            f"Must be one of: {sorted(VALID_TABLE_NAMES)}"
        )
    return table_name


def validate_date_column(date_column: str) -> str:
    """Validate a date column name against the whitelist.

    Raises:
        ValueError: If date_column is not in VALID_DATE_COLUMNS.
    """
    if date_column not in VALID_DATE_COLUMNS:
        raise ValueError(
            f"Invalid date column: '{date_column}'. "
            f"Must be one of: {sorted(VALID_DATE_COLUMNS)}"
        )
    return date_column


_ORDER_BY_PATTERN = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*(ASC|DESC)?$")


def validate_order_by(order_by: Optional[str]) -> Optional[str]:
    """Validate order_by parameter for read_records.

    Accepts 'column [ASC|DESC]' patterns where column is whitelisted.
    None is always valid (no ordering).

    Raises:
        ValueError: If order_by does not match pattern or column is not whitelisted.
    """
    if order_by is None:
        return None
    match = _ORDER_BY_PATTERN.match(order_by.strip())
    if not match:
        raise ValueError(
            f"Invalid order_by: '{order_by}'. "
            "Expected 'column' or 'column ASC' or 'column DESC'."
        )
    column = match.group(1)
    if column not in VALID_ORDER_BY_COLUMNS:
        raise ValueError(
            f"Invalid order_by column: '{column}'. "
            f"Must be one of: {sorted(VALID_ORDER_BY_COLUMNS)}"
        )
    return order_by


@dataclass(frozen=True)
class BackendCapabilities:
    """Declares what a storage backend supports.

    Pipeline code inspects these flags to choose the right code path
    (e.g., SQL transforms for BigQuery vs Pandas transforms for SQLite).
    """

    supports_sql: bool = True
    supports_streaming: bool = False
    supports_partitioning: bool = False
    supports_transactions: bool = True
    supports_upsert: bool = False
    parameter_style: str = "named"  # "named" (:p), "pyformat" (%(p)s), "at" (@p)


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.

    All storage implementations (SQLite, BigQuery, etc.) must implement
    the core abstract methods.  Two access patterns are provided:

    - **SQL interface** — ``query()`` / ``execute()`` for SQL-native workflows.
    - **Record interface** — ``insert_records()`` / ``read_records()`` for
      Pandas-based workflows.  Default implementations build SQL from the
      record data; backends may override with native bulk operations.
    """

    @property
    @abstractmethod
    def backend_type(self) -> str:
        """Return the backend type identifier (e.g., 'sqlite', 'bigquery')."""
        pass

    @property
    @abstractmethod
    def capabilities(self) -> BackendCapabilities:
        """Return capability flags for this backend."""
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
            - BigQuery: @param_name or %(param_name)s
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

    @abstractmethod
    def insert_clean_records(self, records: list[dict]) -> int:
        """Insert cleaned/transformed records.

        Args:
            records: List of cleaned record dictionaries.

        Returns:
            Number of records inserted.
        """
        pass

    @abstractmethod
    def insert_sitemap_urls(self, entries: list[dict]) -> int:
        """Insert sitemap URL entries.

        Args:
            entries: List of dicts with keys: url, url_path, lastmod,
                     lastmod_month, sitemap_source.

        Returns:
            Number of entries inserted.
        """
        pass

    def get_full_table_id(self, table_name: str) -> str:
        """Get the fully qualified table identifier.

        For SQLite, returns the bare table name.
        For BigQuery, returns project.dataset.table format.

        Args:
            table_name: Logical table name.

        Returns:
            Backend-specific fully qualified table identifier.
        """
        return table_name

    # -----------------------------------------------------------------
    # Record interface (insert_records / read_records)
    # -----------------------------------------------------------------

    def insert_records(self, table_name: str, records: list[dict]) -> int:
        """Insert records into a named table.

        Default implementation generates parameterized INSERT statements
        using ``execute()``.  Backends like BigQuery should override with
        native bulk-load operations for better throughput.

        Args:
            table_name: Target table name.
            records: List of dictionaries (one per row).

        Returns:
            Number of records inserted.
        """
        validate_table_name(table_name)
        if not records:
            return 0

        columns = list(records[0].keys())
        col_list = ", ".join(columns)
        placeholders = ", ".join(f":{col}" for col in columns)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

        count = 0
        for record in records:
            self.execute(sql, record)
            count += 1
        return count

    def read_records(
        self,
        table_name: str,
        columns: Optional[list[str]] = None,
        filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> list[dict]:
        """Read records from a named table.

        Default implementation generates a SELECT query via ``query()``.
        Backends can override with native read operations (e.g. BigQuery
        Storage Read API for streaming large result sets).

        Args:
            table_name: Source table name.
            columns: Column names to return (``None`` = all).
            filters: Simple equality filters ``{column: value}``.
            limit: Maximum rows to return.
            order_by: ORDER BY clause (e.g. ``"created_at DESC"``).

        Returns:
            List of dictionaries, one per row.
        """
        validate_table_name(table_name)
        validate_order_by(order_by)
        col_list = ", ".join(columns) if columns else "*"
        sql = f"SELECT {col_list} FROM {table_name}"
        params: dict[str, Any] = {}

        if filters:
            conditions = []
            for key, value in filters.items():
                param_name = f"f_{key}"
                conditions.append(f"{key} = :{param_name}")
                params[param_name] = value
            sql += " WHERE " + " AND ".join(conditions)

        if order_by:
            sql += f" ORDER BY {order_by}"

        if limit is not None:
            sql += f" LIMIT {limit}"

        return self.query(sql, params)

    def delete_date_range(
        self,
        table_name: str,
        date_column: str,
        start_date: date,
        end_date: date,
    ) -> int:
        """Delete records within a date range (for reprocessing).

        Default implementation uses ``execute()`` with parameterized SQL.
        Backends may override to add validation or use native operations.

        Args:
            table_name: Table to delete from.
            date_column: Date column to filter on.
            start_date: Start date (inclusive).
            end_date: End date (inclusive).

        Returns:
            Number of rows deleted.
        """
        validate_table_name(table_name)
        validate_date_column(date_column)
        sql = f"""
            DELETE FROM {table_name}
            WHERE {date_column} >= :start_date
              AND {date_column} <= :end_date
        """
        return self.execute(
            sql,
            {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )

    # -----------------------------------------------------------------
    # Utility methods
    # -----------------------------------------------------------------

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
        validate_table_name(table_name)
        validate_date_column(date_column)
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


class DiskSpaceError(StorageError):
    """Raised when available disk space is below the configured threshold."""

    pass
