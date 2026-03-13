"""Unit tests for schemas modules."""

from llm_bot_pipeline.ingestion.base import IngestionRecord
from llm_bot_pipeline.pipeline.python_transformer import PythonTransformer
from llm_bot_pipeline.schemas.clean import CLEAN_BOT_REQUESTS_COLUMNS
from llm_bot_pipeline.schemas.converters import FieldDefinition, FieldType, TableSchema
from llm_bot_pipeline.schemas.raw import RAW_BOT_REQUESTS_COLUMNS
from llm_bot_pipeline.schemas.reporting import (
    BOT_PROVIDER_SUMMARY_COLUMNS,
    DAILY_SUMMARY_COLUMNS,
    URL_PERFORMANCE_COLUMNS,
)


class TestRawToCleanConversion:
    """Test raw record conversion to clean record structure."""

    def test_raw_to_clean_conversion(self):
        """Convert sample raw record via IngestionRecord + PythonTransformer, assert clean fields."""
        # Build IngestionRecord from raw-like data (universal schema format)
        raw_like = {
            "timestamp": "2026-01-15T10:30:00+00:00",
            "client_ip": "192.168.1.1",
            "method": "GET",
            "host": "example.com",
            "path": "/api/v1/models",
            "status_code": 200,
            "user_agent": "Mozilla/5.0 GPTBot/1.0 (+https://openai.com/bot)",
            "extra": {
                "BotScore": 95,
                "VerifiedBot": 1,
                "ClientCountry": "US",
            },
        }
        record = IngestionRecord.from_dict(raw_like)
        transformer = PythonTransformer()
        clean = transformer.transform(record)

        assert clean is not None
        # Assert all expected clean schema columns are present
        expected_keys = set(CLEAN_BOT_REQUESTS_COLUMNS.keys())
        actual_keys = set(clean.keys())
        assert expected_keys == actual_keys, (
            f"Clean record keys mismatch: missing={expected_keys - actual_keys}, "
            f"extra={actual_keys - expected_keys}"
        )
        # Assert key field values
        assert clean["request_timestamp"] == "2026-01-15T10:30:00+00:00"
        assert clean["request_date"] == "2026-01-15"
        assert clean["request_hour"] == 10
        assert clean["day_of_week"] == "Thursday"
        assert clean["request_uri"] == "/api/v1/models"
        assert clean["request_host"] == "example.com"
        assert clean["url_path"] == "/api/v1/models"
        assert clean["url_path_depth"] == 3
        assert clean["bot_name"] == "GPTBot"
        assert clean["bot_provider"] == "OpenAI"
        assert clean["bot_category"] == "training"
        assert clean["bot_score"] == 95
        assert clean["is_verified_bot"] == 1
        assert clean["crawler_country"] == "US"
        assert clean["response_status"] == 200
        assert clean["response_status_category"] == "2xx"


class TestCleanSchemaFields:
    """Test clean schema definition."""

    def test_clean_schema_contains_expected_columns(self):
        """Assert CLEAN_BOT_REQUESTS_COLUMNS contains all expected columns."""
        expected = {
            "request_timestamp",
            "request_date",
            "request_hour",
            "day_of_week",
            "request_uri",
            "request_host",
            "domain",
            "url_path",
            "url_path_depth",
            "user_agent_raw",
            "bot_name",
            "bot_provider",
            "bot_category",
            "bot_score",
            "is_verified_bot",
            "crawler_country",
            "response_status",
            "response_status_category",
            "_processed_at",
        }
        actual = set(CLEAN_BOT_REQUESTS_COLUMNS.keys())
        assert expected == actual

    def test_clean_schema_temporal_fields_have_not_null(self):
        """Assert temporal fields have NOT NULL constraint."""
        assert "NOT NULL" in CLEAN_BOT_REQUESTS_COLUMNS["request_timestamp"]
        assert "NOT NULL" in CLEAN_BOT_REQUESTS_COLUMNS["request_date"]


class TestReportingSchemaFields:
    """Test reporting schema definitions."""

    def test_daily_summary_schema_structure(self):
        """Assert DAILY_SUMMARY_COLUMNS matches expected structure."""
        expected_keys = {
            "summary_date",
            "total_requests",
            "unique_bots",
            "unique_ips",
            "unique_urls",
            "avg_bot_score",
            "verified_bot_pct",
            "success_rate",
            "_created_at",
        }
        assert set(DAILY_SUMMARY_COLUMNS.keys()) == expected_keys
        assert "PRIMARY KEY" in DAILY_SUMMARY_COLUMNS["summary_date"]

    def test_url_performance_schema_structure(self):
        """Assert URL_PERFORMANCE_COLUMNS matches expected structure."""
        expected_keys = {
            "url_path",
            "request_count",
            "unique_bots",
            "avg_bot_score",
            "success_rate",
            "first_seen",
            "last_seen",
            "_created_at",
        }
        assert set(URL_PERFORMANCE_COLUMNS.keys()) == expected_keys
        assert "PRIMARY KEY" in URL_PERFORMANCE_COLUMNS["url_path"]

    def test_bot_provider_summary_schema_structure(self):
        """Assert BOT_PROVIDER_SUMMARY_COLUMNS matches expected structure."""
        expected_keys = {
            "summary_date",
            "bot_provider",
            "request_count",
            "unique_urls",
            "avg_requests_per_session",
            "session_count",
            "_created_at",
        }
        assert set(BOT_PROVIDER_SUMMARY_COLUMNS.keys()) == expected_keys


class TestRawSchemaFields:
    """Test raw schema definition."""

    def test_raw_schema_contains_expected_columns(self):
        """Assert RAW_BOT_REQUESTS_COLUMNS contains all expected columns."""
        expected = {
            "EdgeStartTimestamp",
            "ClientRequestURI",
            "ClientRequestHost",
            "domain",
            "ClientRequestUserAgent",
            "BotScore",
            "BotScoreSrc",
            "VerifiedBot",
            "BotTags",
            "ClientIP",
            "ClientCountry",
            "EdgeResponseStatus",
            "RayID",
            "_ingested_at",
            "source_provider",
        }
        assert set(RAW_BOT_REQUESTS_COLUMNS.keys()) == expected


class TestSchemaConverters:
    """Test FieldDefinition and TableSchema converters."""

    def test_field_definition_to_sqlite_column(self):
        """FieldDefinition produces correct SQLite column spec."""
        fd = FieldDefinition("id", FieldType.INTEGER, nullable=False, primary_key=True)
        assert fd.to_sqlite_column() == "id INTEGER PRIMARY KEY"

    def test_field_definition_with_default(self):
        """FieldDefinition with default produces correct SQLite column."""
        fd = FieldDefinition(
            "created_at",
            FieldType.TIMESTAMP,
            default="CURRENT_TIMESTAMP",
        )
        assert "DEFAULT CURRENT_TIMESTAMP" in fd.to_sqlite_column()

    def test_table_schema_to_sqlite_ddl(self):
        """TableSchema produces valid SQLite DDL."""
        schema = TableSchema(
            "test_table",
            [
                FieldDefinition("id", FieldType.INTEGER, primary_key=True),
                FieldDefinition("name", FieldType.STRING),
            ],
        )
        ddl = schema.to_sqlite_ddl()
        assert "CREATE TABLE IF NOT EXISTS test_table" in ddl
        assert "id INTEGER PRIMARY KEY" in ddl
        assert "name TEXT" in ddl

    def test_table_schema_column_names(self):
        """TableSchema.column_names returns ordered list."""
        schema = TableSchema(
            "t",
            [
                FieldDefinition("a", FieldType.STRING),
                FieldDefinition("b", FieldType.INTEGER),
            ],
        )
        assert schema.column_names() == ["a", "b"]

    def test_table_schema_to_column_dict(self):
        """TableSchema.to_column_dict returns SQLite type mapping."""
        schema = TableSchema(
            "t",
            [
                FieldDefinition("name", FieldType.STRING),
                FieldDefinition("count", FieldType.INTEGER),
            ],
        )
        d = schema.to_column_dict()
        assert d == {"name": "TEXT", "count": "INTEGER"}

    def test_field_type_sqlite_mapping(self):
        """All FieldType values map to valid SQLite types."""
        for ft in FieldType:
            fd = FieldDefinition("col", ft)
            col = fd.to_sqlite_column()
            assert col.startswith("col ")
            assert len(col) > 4
