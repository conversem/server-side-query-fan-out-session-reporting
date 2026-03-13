"""
SQL Compatibility Layer for BigQuery and SQLite.

Provides SQL generation functions that produce backend-specific syntax
for common operations, enabling the same pipeline logic to work with
both BigQuery (production) and SQLite (POC/development).

SQL Differences Handled:
- Date functions: DATE_TRUNC, DATE_SUB, FORMAT_DATE
- Aggregation: COUNTIF → SUM(CASE WHEN...)
- String functions: STRING_AGG → GROUP_CONCAT
- Regex: REGEXP_CONTAINS → LIKE patterns
- Array operations: ARRAY_LENGTH(SPLIT()) → length-based
- Timestamps: CURRENT_TIMESTAMP() → datetime('now')
- Raw timestamps: INT64 nanoseconds → TIMESTAMP_MICROS(DIV())
"""

from datetime import date
from typing import Literal, Optional

BackendType = Literal["sqlite", "bigquery"]


def current_timestamp(backend: BackendType) -> str:
    """Generate current timestamp expression."""
    if backend == "sqlite":
        return "datetime('now')"
    return "CURRENT_TIMESTAMP()"


def timestamp_from_raw(column: str, backend: BackendType) -> str:
    """Convert raw timestamp column to proper TIMESTAMP type.

    Handles the difference between:
    - SQLite: EdgeStartTimestamp stored as TEXT (ISO format)
    - BigQuery: EdgeStartTimestamp stored as INT64 (nanoseconds since epoch)
    """
    if backend == "sqlite":
        return column
    return f"TIMESTAMP_MICROS(DIV({column}, 1000))"


def date_from_timestamp(column: str, backend: BackendType) -> str:
    """Extract date from timestamp column (assumes proper timestamp type)."""
    if backend == "sqlite":
        return f"date({column})"
    return f"DATE({column})"


def date_from_raw_timestamp(column: str, backend: BackendType) -> str:
    """Extract date from raw timestamp column (handles nanoseconds INT64).

    Composes timestamp_from_raw() with date_from_timestamp().
    """
    ts_expr = timestamp_from_raw(column, backend)
    return date_from_timestamp(ts_expr, backend)


def extract_hour(column: str, backend: BackendType) -> str:
    """Extract hour from timestamp column (assumes proper timestamp type)."""
    if backend == "sqlite":
        return f"CAST(strftime('%H', {column}) AS INTEGER)"
    return f"EXTRACT(HOUR FROM {column})"


def extract_hour_from_raw(column: str, backend: BackendType) -> str:
    """Extract hour from raw timestamp column (handles nanoseconds INT64)."""
    ts_expr = timestamp_from_raw(column, backend)
    return extract_hour(ts_expr, backend)


def day_of_week(column: str, backend: BackendType) -> str:
    """Get day of week name from timestamp/date (assumes proper timestamp type)."""
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


def day_of_week_from_raw(column: str, backend: BackendType) -> str:
    """Get day of week name from raw timestamp (handles nanoseconds INT64)."""
    ts_expr = timestamp_from_raw(column, backend)
    return day_of_week(ts_expr, backend)


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


def date_filter_raw(
    column: str,
    start_date: date,
    end_date: date,
    backend: BackendType,
) -> str:
    """Generate date range filter for raw timestamp (handles nanoseconds INT64)."""
    ts_expr = timestamp_from_raw(column, backend)
    return date_filter(ts_expr, start_date, end_date, backend)


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
    # BigQuery uses backtick-quoted full paths
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
    # BigQuery: Use REGEXP_EXTRACT
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
    # BigQuery: Use ARRAY_LENGTH(SPLIT())
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
    # BigQuery: Use REGEXP_CONTAINS for word boundary matching
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
    # BigQuery
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


def json_array_unnest(
    table: str,
    json_column: str,
    backend: BackendType,
    alias: str = "j",
) -> tuple[str, str]:
    """Generate a cross-join that unnests a JSON array column.

    SQLite:  ``table, json_each(table.json_column)`` (value via ``json_each.value``)
    BigQuery: ``table CROSS JOIN UNNEST(JSON_EXTRACT_STRING_ARRAY(...)) AS alias``

    Args:
        table: Table name or alias already in FROM.
        json_column: Column containing a JSON array (TEXT).
        backend: Target SQL dialect.
        alias: Alias for the unnested element (BigQuery only).

    Returns:
        Tuple ``(from_clause, value_expr)`` where *from_clause* is the text
        to place in ``FROM`` and *value_expr* references the scalar element.
    """
    if backend == "sqlite":
        from_clause = f"{table}, json_each({table}.{json_column})"
        value_expr = "json_each.value"
    else:
        from_clause = (
            f"{table} CROSS JOIN "
            f"UNNEST(JSON_EXTRACT_STRING_ARRAY({table}.{json_column})) AS {alias}"
        )
        value_expr = alias
    return from_clause, value_expr


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
    """SQL query builder with backend-aware syntax.

    Provides a fluent interface for building queries that work
    with both BigQuery and SQLite.
    """

    def __init__(self, backend: BackendType):
        self.backend = backend

    def current_timestamp(self) -> str:
        return current_timestamp(self.backend)

    def timestamp_from_raw(self, column: str) -> str:
        """Convert raw timestamp (nanoseconds INT64) to proper timestamp."""
        return timestamp_from_raw(column, self.backend)

    def date_from_timestamp(self, column: str) -> str:
        return date_from_timestamp(column, self.backend)

    def date_from_raw_timestamp(self, column: str) -> str:
        """Extract date from raw timestamp (handles nanoseconds INT64)."""
        return date_from_raw_timestamp(column, self.backend)

    def extract_hour(self, column: str) -> str:
        return extract_hour(column, self.backend)

    def extract_hour_from_raw(self, column: str) -> str:
        """Extract hour from raw timestamp (handles nanoseconds INT64)."""
        return extract_hour_from_raw(column, self.backend)

    def day_of_week(self, column: str) -> str:
        return day_of_week(column, self.backend)

    def day_of_week_from_raw(self, column: str) -> str:
        """Get day of week from raw timestamp (handles nanoseconds INT64)."""
        return day_of_week_from_raw(column, self.backend)

    def date_filter(self, column: str, start_date: date, end_date: date) -> str:
        return date_filter(column, start_date, end_date, self.backend)

    def date_filter_raw(self, column: str, start_date: date, end_date: date) -> str:
        """Generate date range filter for raw timestamp (handles nanoseconds)."""
        return date_filter_raw(column, start_date, end_date, self.backend)

    def countif(self, condition: str) -> str:
        return countif(condition, self.backend)

    def coalesce_bool(self, column: str, default: bool = False) -> str:
        return coalesce_bool(column, default, self.backend)

    def table_ref(self, table_path: str) -> str:
        return table_reference(table_path, self.backend)

    def url_path(self, column: str) -> str:
        return url_path_extract(column, self.backend)

    def url_depth(self, column: str) -> str:
        return url_path_depth(column, self.backend)

    def bot_match(self, column: str, bot_name: str) -> str:
        return bot_pattern_match(column, bot_name, self.backend)

    def status_category(self, column: str) -> str:
        return response_status_category(column, self.backend)

    def row_number(self, partition_cols: list[str], order_col: str) -> str:
        return row_number_dedup(partition_cols, order_col, self.backend)

    def json_array_unnest(
        self, table: str, json_column: str, alias: str = "j"
    ) -> tuple[str, str]:
        """Unnest a JSON array column. Returns (from_clause, value_expr)."""
        return json_array_unnest(table, json_column, self.backend, alias=alias)
