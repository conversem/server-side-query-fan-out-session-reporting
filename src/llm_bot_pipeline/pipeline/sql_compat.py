"""
SQL Compatibility Layer for SQLite and SQLite.

Provides SQL generation functions that produce backend-specific syntax
for common operations, enabling the same pipeline logic to work with
both SQLite (production) and SQLite (local development).

SQL Differences Handled:
- Date functions: DATE_TRUNC, DATE_SUB, FORMAT_DATE
- Aggregation: COUNTIF → SUM(CASE WHEN...)
- String functions: STRING_AGG → GROUP_CONCAT
- Regex: REGEXP_CONTAINS → LIKE patterns
- Array operations: ARRAY_LENGTH(SPLIT()) → length-based
- Timestamps: CURRENT_TIMESTAMP() → datetime('now')
"""

from datetime import date
from typing import Literal, Optional

BackendType = Literal["sqlite"]


def current_timestamp(backend: BackendType) -> str:
    """Generate current timestamp expression."""
    if backend == "sqlite":
        return "datetime('now')"
    return "CURRENT_TIMESTAMP()"


def date_from_timestamp(column: str, backend: BackendType) -> str:
    """Extract date from timestamp column."""
    if backend == "sqlite":
        return f"date({column})"
    return f"DATE({column})"


def extract_hour(column: str, backend: BackendType) -> str:
    """Extract hour from timestamp column."""
    if backend == "sqlite":
        return f"CAST(strftime('%H', {column}) AS INTEGER)"
    return f"EXTRACT(HOUR FROM {column})"


def day_of_week(column: str, backend: BackendType) -> str:
    """Get day of week name from timestamp/date."""
    if backend == "sqlite":
        # SQLite strftime %w returns 0-6 (Sunday=0)
        return f"""CASE CAST(strftime('%w', {column}) AS INTEGER)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END"""
    return f"FORMAT_DATE('%A', DATE({column}))"


def date_filter(
    column: str,
    start_date: date,
    end_date: date,
    backend: BackendType,
) -> str:
    """Generate date range filter clause."""
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    if backend == "sqlite":
        return f"date({column}) >= '{start_str}' AND date({column}) <= '{end_str}'"
    return f"DATE({column}) >= '{start_str}' AND DATE({column}) <= '{end_str}'"


def countif(condition: str, backend: BackendType) -> str:
    """Generate COUNTIF equivalent."""
    if backend == "sqlite":
        return f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END)"
    return f"COUNTIF({condition})"


def coalesce_bool(column: str, default: bool, backend: BackendType) -> str:
    """Generate COALESCE for boolean with backend-appropriate default."""
    default_val = "1" if default else "0"
    if backend == "sqlite":
        return f"COALESCE({column}, {default_val})"
    return f"COALESCE({column}, {'TRUE' if default else 'FALSE'})"


def table_reference(table_path: str, backend: BackendType) -> str:
    """Generate table reference with appropriate quoting."""
    if backend == "sqlite":
        # SQLite uses simple table names
        return table_path.split(".")[-1]
    # SQLite uses backtick-quoted full paths
    return f"`{table_path}`"


def url_path_extract(column: str, backend: BackendType) -> str:
    """Extract URL path (remove query params and fragments)."""
    if backend == "sqlite":
        # SQLite: Use SUBSTR and INSTR for path extraction
        # Find the first occurrence of ? or #
        return f"""CASE
            WHEN INSTR({column}, '?') > 0 AND (INSTR({column}, '#') = 0 OR INSTR({column}, '?') < INSTR({column}, '#'))
                THEN SUBSTR({column}, 1, INSTR({column}, '?') - 1)
            WHEN INSTR({column}, '#') > 0
                THEN SUBSTR({column}, 1, INSTR({column}, '#') - 1)
            ELSE {column}
        END"""
    # SQLite: Use REGEXP_EXTRACT
    return f"REGEXP_EXTRACT({column}, r'^([^?#]*)')"


def url_path_depth(column: str, backend: BackendType) -> str:
    """Calculate URL path depth (number of segments)."""
    if backend == "sqlite":
        # SQLite: Count slashes minus leading/trailing
        # This is an approximation - counts '/' occurrences
        path_expr = url_path_extract(column, backend)
        return f"""CASE
            WHEN ({path_expr}) IN ('/', '') THEN 0
            ELSE LENGTH(REPLACE({path_expr}, '/', '')) - LENGTH(REPLACE(REPLACE({path_expr}, '/', ''), '/', ''))
                + CASE WHEN SUBSTR({path_expr}, 1, 1) = '/' THEN 0 ELSE 1 END
                - CASE WHEN SUBSTR({path_expr}, -1, 1) = '/' THEN 1 ELSE 0 END
        END"""
    # SQLite: Use ARRAY_LENGTH(SPLIT())
    return f"""CASE
        WHEN REGEXP_EXTRACT({column}, r'^([^?#]*)') IN ('/', '') THEN 0
        ELSE ARRAY_LENGTH(
            SPLIT(
                REGEXP_REPLACE(
                    REGEXP_EXTRACT({column}, r'^([^?#]*)'),
                    r'^/|/$', ''
                ),
                '/'
            )
        )
    END"""


def bot_pattern_match(column: str, bot_name: str, backend: BackendType) -> str:
    """Generate case-insensitive bot name match condition."""
    if backend == "sqlite":
        # SQLite: Use LIKE with COLLATE NOCASE for case-insensitive matching
        # Note: This is less precise than regex but works for most cases
        return f"{column} LIKE '%{bot_name}%' COLLATE NOCASE"
    # SQLite: Use REGEXP_CONTAINS for word boundary matching
    return f"REGEXP_CONTAINS({column}, r'(?i)\\\\b{bot_name}\\\\b')"


def string_agg(
    column: str,
    separator: str,
    distinct: bool,
    backend: BackendType,
    order_by: Optional[str] = None,
) -> str:
    """Generate string aggregation function."""
    if backend == "sqlite":
        if distinct:
            # SQLite doesn't support DISTINCT in GROUP_CONCAT directly with ORDER BY
            return f"GROUP_CONCAT(DISTINCT {column}, '{separator}')"
        return f"GROUP_CONCAT({column}, '{separator}')"
    # SQLite
    distinct_str = "DISTINCT " if distinct else ""
    order_str = f" ORDER BY {order_by}" if order_by else ""
    return f"STRING_AGG({distinct_str}{column}, '{separator}'{order_str})"


def row_number_dedup(
    partition_cols: list[str],
    order_col: str,
    backend: BackendType,
) -> str:
    """Generate ROW_NUMBER for deduplication."""
    partition = ", ".join(partition_cols)
    # Same syntax for both backends
    return f"ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order_col})"


def response_status_category(column: str, backend: BackendType) -> str:
    """Generate response status category CASE statement."""
    # Same syntax for both backends
    return f"""CASE
        WHEN {column} BETWEEN 200 AND 299 THEN '2xx_success'
        WHEN {column} BETWEEN 300 AND 399 THEN '3xx_redirect'
        WHEN {column} BETWEEN 400 AND 499 THEN '4xx_client_error'
        WHEN {column} BETWEEN 500 AND 599 THEN '5xx_server_error'
        ELSE 'other'
    END"""


class SQLBuilder:
    """
    SQL query builder with backend-aware syntax.

    Provides a fluent interface for building queries that work
    with both SQLite and SQLite.
    """

    def __init__(self, backend: BackendType):
        """Initialize with target backend type."""
        self.backend = backend

    def current_timestamp(self) -> str:
        """Get current timestamp expression."""
        return current_timestamp(self.backend)

    def date_from_timestamp(self, column: str) -> str:
        """Extract date from timestamp."""
        return date_from_timestamp(column, self.backend)

    def extract_hour(self, column: str) -> str:
        """Extract hour from timestamp."""
        return extract_hour(column, self.backend)

    def day_of_week(self, column: str) -> str:
        """Get day of week name."""
        return day_of_week(column, self.backend)

    def date_filter(self, column: str, start_date: date, end_date: date) -> str:
        """Generate date range filter."""
        return date_filter(column, start_date, end_date, self.backend)

    def countif(self, condition: str) -> str:
        """Generate COUNTIF equivalent."""
        return countif(condition, self.backend)

    def coalesce_bool(self, column: str, default: bool = False) -> str:
        """Generate boolean COALESCE."""
        return coalesce_bool(column, default, self.backend)

    def table_ref(self, table_path: str) -> str:
        """Generate table reference."""
        return table_reference(table_path, self.backend)

    def url_path(self, column: str) -> str:
        """Extract URL path."""
        return url_path_extract(column, self.backend)

    def url_depth(self, column: str) -> str:
        """Calculate URL path depth."""
        return url_path_depth(column, self.backend)

    def bot_match(self, column: str, bot_name: str) -> str:
        """Generate bot name match condition."""
        return bot_pattern_match(column, bot_name, self.backend)

    def status_category(self, column: str) -> str:
        """Generate response status category."""
        return response_status_category(column, self.backend)

    def row_number(self, partition_cols: list[str], order_col: str) -> str:
        """Generate ROW_NUMBER for deduplication."""
        return row_number_dedup(partition_cols, order_col, self.backend)
