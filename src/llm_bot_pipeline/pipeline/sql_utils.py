"""Shared SQL utilities for pipeline modules."""

import re

from llm_bot_pipeline.config.constants import VALID_TABLE_NAMES

COLUMN_REGEX = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def build_clean_insert_sql(row: dict, table: str = "bot_requests_daily") -> str:
    """Build an INSERT statement for a clean record dict.

    Args:
        row: Dict of column -> value pairs.
        table: Target table name.

    Returns:
        SQL INSERT string with properly escaped values.

    Raises:
        ValueError: If table name is not in VALID_TABLE_NAMES or any
            column name fails regex validation.
    """
    if table not in VALID_TABLE_NAMES:
        raise ValueError(f"Invalid table name: {table!r}")
    columns = list(row.keys())
    for col in columns:
        if not COLUMN_REGEX.match(col):
            raise ValueError(f"Invalid column name: {col!r}")
    values = []
    for col in columns:
        val = row[col]
        if val is None:
            values.append("NULL")
        elif isinstance(val, bool):
            values.append("1" if val else "0")
        elif isinstance(val, (int, float)):
            values.append(str(val))
        else:
            escaped = str(val).replace("'", "''")
            values.append(f"'{escaped}'")
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) " f"VALUES ({', '.join(values)})"
    )
