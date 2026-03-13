"""
Neutral schema definitions with converters to backend-specific formats.

Provides a single source of truth for field definitions that can be
converted to SQLite DDL, BigQuery SchemaField, or PyArrow field types.

Usage:
    from llm_bot_pipeline.schemas.converters import TableSchema, FieldDefinition

    schema = TableSchema("my_table", [
        FieldDefinition("id", FieldType.INTEGER, nullable=False),
        FieldDefinition("name", FieldType.STRING),
        FieldDefinition("created_at", FieldType.TIMESTAMP),
    ])

    sqlite_ddl = schema.to_sqlite_ddl()
    bq_schema  = schema.to_bigquery_schema()   # requires google-cloud-bigquery
    pa_schema  = schema.to_pyarrow_schema()     # requires pyarrow
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class FieldType(str, Enum):
    """Backend-agnostic logical field types."""

    STRING = "STRING"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    JSON = "JSON"


# -------------------------------------------------------------------------
# SQLite type mapping
# -------------------------------------------------------------------------

_SQLITE_TYPE_MAP: dict[FieldType, str] = {
    FieldType.STRING: "TEXT",
    FieldType.INTEGER: "INTEGER",
    FieldType.FLOAT: "REAL",
    FieldType.BOOLEAN: "INTEGER",
    FieldType.TIMESTAMP: "TEXT",
    FieldType.DATE: "TEXT",
    FieldType.JSON: "TEXT",
}


@dataclass(frozen=True)
class FieldDefinition:
    """A single column/field definition in backend-agnostic form."""

    name: str
    field_type: FieldType
    nullable: bool = True
    description: str = ""
    default: Optional[str] = None
    primary_key: bool = False

    # ------- SQLite -------

    def to_sqlite_column(self) -> str:
        """Return ``name TYPE [constraints]`` for a CREATE TABLE statement."""
        parts = [self.name, _SQLITE_TYPE_MAP[self.field_type]]
        if self.primary_key:
            parts.append("PRIMARY KEY")
        if not self.nullable and not self.primary_key:
            parts.append("NOT NULL")
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")
        return " ".join(parts)

    # ------- BigQuery -------

    def to_bigquery_field(self):
        """Return a ``google.cloud.bigquery.SchemaField``.

        Raises ``ImportError`` if ``google-cloud-bigquery`` is not installed.
        """
        from google.cloud.bigquery import SchemaField

        bq_type_map: dict[FieldType, str] = {
            FieldType.STRING: "STRING",
            FieldType.INTEGER: "INT64",
            FieldType.FLOAT: "FLOAT64",
            FieldType.BOOLEAN: "BOOL",
            FieldType.TIMESTAMP: "TIMESTAMP",
            FieldType.DATE: "DATE",
            FieldType.JSON: "JSON",
        }
        mode = "NULLABLE" if self.nullable else "REQUIRED"
        return SchemaField(
            name=self.name,
            field_type=bq_type_map[self.field_type],
            mode=mode,
            description=self.description,
        )

    # ------- PyArrow -------

    def to_pyarrow_field(self):
        """Return a ``pyarrow.field``.

        Raises ``ImportError`` if ``pyarrow`` is not installed.
        """
        import pyarrow as pa

        pa_type_map: dict[FieldType, pa.DataType] = {
            FieldType.STRING: pa.string(),
            FieldType.INTEGER: pa.int64(),
            FieldType.FLOAT: pa.float64(),
            FieldType.BOOLEAN: pa.bool_(),
            FieldType.TIMESTAMP: pa.timestamp("us"),
            FieldType.DATE: pa.date32(),
            FieldType.JSON: pa.string(),
        }
        return pa.field(self.name, pa_type_map[self.field_type], nullable=self.nullable)


@dataclass
class TableSchema:
    """Collection of fields for a single table, convertible to any backend format."""

    table_name: str
    fields: list[FieldDefinition] = field(default_factory=list)
    description: str = ""

    # ------- SQLite -------

    def to_sqlite_ddl(self) -> str:
        """Generate ``CREATE TABLE IF NOT EXISTS …`` for SQLite."""
        columns = ", ".join(f.to_sqlite_column() for f in self.fields)
        return f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns})"

    # ------- BigQuery -------

    def to_bigquery_schema(self) -> list:
        """Return list of ``google.cloud.bigquery.SchemaField`` objects."""
        return [f.to_bigquery_field() for f in self.fields]

    # ------- PyArrow -------

    def to_pyarrow_schema(self):
        """Return a ``pyarrow.Schema``."""
        import pyarrow as pa

        return pa.schema([f.to_pyarrow_field() for f in self.fields])

    # ------- Helpers -------

    def column_names(self) -> list[str]:
        """Return ordered list of column names."""
        return [f.name for f in self.fields]

    def to_column_dict(self) -> dict[str, str]:
        """Return ``{name: SQLITE_TYPE}`` dict for backward compatibility."""
        return {f.name: _SQLITE_TYPE_MAP[f.field_type] for f in self.fields}
