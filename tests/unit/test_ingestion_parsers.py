"""
Unit tests for ingestion parsers.

Tests CSV/TSV, JSON/NDJSON parsers, schema validation, and field validators.
Includes tests for gzip-compressed files.
"""

import gzip
import io
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from llm_bot_pipeline.ingestion.parsers import (
    OPTIONAL_FIELDS,
    REQUIRED_FIELDS,
    UNIVERSAL_SCHEMA,
    CSVParser,
    FieldDefinition,
    FieldType,
    JSONParser,
    get_optional_field_names,
    get_required_field_names,
    parse_csv_file,
    parse_json_file,
    parse_ndjson_file,
    parse_tsv_file,
    validate_field,
    validate_http_method,
    validate_ip_address,
    validate_positive_integer,
    validate_record,
    validate_status_code,
    validate_timestamp,
)

# =============================================================================
# Schema Tests
# =============================================================================


class TestUniversalSchema:
    """Tests for universal schema definition."""

    def test_required_fields_defined(self):
        """All required fields should be defined."""
        assert len(REQUIRED_FIELDS) == 7
        required_names = [f.name for f in REQUIRED_FIELDS]
        assert "timestamp" in required_names
        assert "client_ip" in required_names
        assert "method" in required_names
        assert "host" in required_names
        assert "path" in required_names
        assert "status_code" in required_names
        assert "user_agent" in required_names

    def test_optional_fields_defined(self):
        """All optional fields should be defined."""
        assert len(OPTIONAL_FIELDS) == 9
        optional_names = [f.name for f in OPTIONAL_FIELDS]
        assert "query_string" in optional_names
        assert "response_bytes" in optional_names
        assert "cache_status" in optional_names

    def test_schema_has_all_fields(self):
        """Schema should contain all required and optional fields."""
        assert len(UNIVERSAL_SCHEMA) == 16
        assert "timestamp" in UNIVERSAL_SCHEMA
        assert "cache_status" in UNIVERSAL_SCHEMA

    def test_field_definition_structure(self):
        """FieldDefinition should have correct structure."""
        field = UNIVERSAL_SCHEMA["timestamp"]
        assert isinstance(field, FieldDefinition)
        assert field.name == "timestamp"
        assert field.field_type == FieldType.TIMESTAMP
        assert field.required is True
        assert field.validator is not None

    def test_get_required_field_names(self):
        """get_required_field_names should return correct list."""
        names = get_required_field_names()
        assert len(names) == 7
        assert "timestamp" in names
        assert "cache_status" not in names

    def test_get_optional_field_names(self):
        """get_optional_field_names should return correct list."""
        names = get_optional_field_names()
        assert len(names) == 9
        assert "cache_status" in names
        assert "timestamp" not in names


class TestFieldValidators:
    """Tests for individual field validators."""

    def test_validate_timestamp_datetime(self):
        """Datetime objects should be valid."""
        assert validate_timestamp(datetime.now()) is True
        assert validate_timestamp(datetime.now(timezone.utc)) is True

    def test_validate_timestamp_iso_string(self):
        """ISO 8601 strings should be valid."""
        assert validate_timestamp("2024-01-15T12:30:45Z") is True
        assert validate_timestamp("2024-01-15T12:30:45+00:00") is True
        assert validate_timestamp("2024-01-15T12:30:45") is True

    def test_validate_timestamp_unix(self):
        """Unix timestamps should be valid."""
        assert validate_timestamp(1705323045) is True  # Seconds
        assert validate_timestamp(1705323045000) is True  # Milliseconds

    def test_validate_timestamp_microseconds(self):
        """Microsecond Unix timestamps should be valid."""
        assert validate_timestamp(1705323045000000) is True

    def test_validate_timestamp_nanoseconds(self):
        """Nanosecond Unix timestamps should be valid."""
        assert validate_timestamp(1705323045000000000) is True

    def test_validate_timestamp_string_numeric(self):
        """String representations of Unix timestamps should be valid."""
        assert validate_timestamp("1705323045") is True  # Seconds
        assert validate_timestamp("1705323045000") is True  # Milliseconds
        assert validate_timestamp("1705323045000000") is True  # Microseconds
        assert validate_timestamp("1705323045000000000") is True  # Nanoseconds

    def test_validate_timestamp_invalid(self):
        """Invalid timestamps should fail."""
        assert validate_timestamp(None) is False
        assert validate_timestamp("not-a-date") is False
        assert validate_timestamp("") is False
        assert validate_timestamp("abc123") is False

    def test_validate_ip_address_ipv4(self):
        """IPv4 addresses should be valid."""
        assert validate_ip_address("192.0.2.100") is True
        assert validate_ip_address("10.0.0.1") is True
        assert validate_ip_address("127.0.0.1") is True

    def test_validate_ip_address_ipv6(self):
        """IPv6 addresses should be valid."""
        assert validate_ip_address("::1") is True
        assert validate_ip_address("2001:db8::1") is True
        assert validate_ip_address("fe80::1") is True

    def test_validate_ip_address_invalid(self):
        """Invalid IP addresses should fail."""
        assert validate_ip_address(None) is False
        assert validate_ip_address("") is False
        assert validate_ip_address("not-an-ip") is False
        assert validate_ip_address("999.999.999.999") is False

    def test_validate_http_method_valid(self):
        """Valid HTTP methods should pass."""
        for method in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]:
            assert validate_http_method(method) is True

    def test_validate_http_method_case_insensitive(self):
        """HTTP methods should be case-insensitive."""
        assert validate_http_method("get") is True
        assert validate_http_method("Get") is True

    def test_validate_http_method_invalid(self):
        """Invalid HTTP methods should fail."""
        assert validate_http_method(None) is False
        assert validate_http_method("") is False
        assert validate_http_method("INVALID") is False

    def test_validate_status_code_valid(self):
        """Valid status codes should pass."""
        assert validate_status_code(200) is True
        assert validate_status_code(404) is True
        assert validate_status_code(500) is True
        assert validate_status_code(100) is True
        assert validate_status_code(599) is True

    def test_validate_status_code_string(self):
        """Status code as string should be valid."""
        assert validate_status_code("200") is True
        assert validate_status_code("404") is True

    def test_validate_status_code_invalid(self):
        """Invalid status codes should fail."""
        assert validate_status_code(None) is False
        assert validate_status_code(99) is False
        assert validate_status_code(600) is False
        assert validate_status_code("invalid") is False

    def test_validate_positive_integer(self):
        """Positive integers should be valid."""
        assert validate_positive_integer(0) is True
        assert validate_positive_integer(100) is True
        assert validate_positive_integer("1234") is True
        assert validate_positive_integer(None) is True  # Optional

    def test_validate_positive_integer_invalid(self):
        """Negative integers should fail."""
        assert validate_positive_integer(-1) is False
        assert validate_positive_integer("not-a-number") is False


class TestValidateField:
    """Tests for validate_field function."""

    def test_valid_required_field(self):
        """Valid required field should pass."""
        is_valid, error = validate_field("status_code", 200)
        assert is_valid is True
        assert error == ""

    def test_missing_required_field(self):
        """Missing required field should fail."""
        is_valid, error = validate_field("timestamp", None)
        assert is_valid is False
        assert "Required field" in error

    def test_invalid_field_value(self):
        """Invalid field value should fail."""
        is_valid, error = validate_field("status_code", "not-a-number")
        assert is_valid is False
        assert "Invalid value" in error

    def test_unknown_field_passes(self):
        """Unknown fields should pass (stored in extra)."""
        is_valid, error = validate_field("custom_field", "any_value")
        assert is_valid is True

    def test_none_optional_field_passes(self):
        """None value for optional field should pass."""
        is_valid, error = validate_field("cache_status", None)
        assert is_valid is True


class TestValidateRecord:
    """Tests for validate_record function."""

    def test_valid_complete_record(self):
        """Complete valid record should pass."""
        data = {
            "timestamp": "2024-01-15T12:30:45Z",
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/api/data",
            "status_code": 200,
            "user_agent": "TestBot/1.0",
        }
        is_valid, errors = validate_record(data)
        assert is_valid is True
        assert errors == []

    def test_missing_required_field(self):
        """Missing required field should fail."""
        data = {
            "timestamp": "2024-01-15T12:30:45Z",
            "client_ip": "192.0.2.100",
            # Missing method, host, path, status_code, user_agent
        }
        is_valid, errors = validate_record(data)
        assert is_valid is False
        assert len(errors) > 0


# =============================================================================
# CSV Parser Tests
# =============================================================================


class TestCSVParser:
    """Tests for CSVParser class."""

    @pytest.fixture
    def field_mapping(self):
        """Standard field mapping for tests."""
        return {
            "timestamp": "timestamp",
            "client_ip": "client_ip",
            "method": "method",
            "host": "host",
            "path": "path",
            "status_code": "status_code",
            "user_agent": "user_agent",
        }

    def test_parse_simple_csv(self, field_mapping):
        """Parse a simple CSV with header."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api/data,200,TestBot/1.0
2024-01-15T12:31:00Z,192.0.2.101,POST,example.com,/api/submit,201,TestBot/1.0"""

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 2
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200
        assert records[1].method == "POST"
        assert records[1].status_code == 201

    def test_parse_csv_with_custom_mapping(self):
        """Parse CSV with custom column names."""
        csv_data = """Timestamp,ClientIP,Method,Host,Path,Status,UA
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0"""

        mapping = {
            "Timestamp": "timestamp",
            "ClientIP": "client_ip",
            "Method": "method",
            "Host": "host",
            "Path": "path",
            "Status": "status_code",
            "UA": "user_agent",
        }

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, mapping))

        assert len(records) == 1
        assert records[0].client_ip == "192.0.2.100"

    def test_parse_tsv(self, field_mapping):
        """Parse TSV (tab-separated)."""
        tsv_data = "timestamp\tclient_ip\tmethod\thost\tpath\tstatus_code\tuser_agent\n"
        tsv_data += (
            "2024-01-15T12:30:45Z\t192.0.2.100\tGET\texample.com\t/api\t200\tBot/1.0"
        )

        parser = CSVParser(delimiter="\t")
        file = io.StringIO(tsv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 1
        assert records[0].path == "/api"

    def test_parse_csv_with_optional_fields(self, field_mapping):
        """Parse CSV with optional fields."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent,cache_status,response_bytes
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0,HIT,1234"""

        mapping = {
            **field_mapping,
            "cache_status": "cache_status",
            "response_bytes": "response_bytes",
        }

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, mapping))

        assert len(records) == 1
        assert records[0].cache_status == "HIT"
        assert records[0].response_bytes == 1234

    def test_parse_csv_with_empty_values(self, field_mapping):
        """Parse CSV with empty optional values."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent,cache_status
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0,"""

        mapping = {**field_mapping, "cache_status": "cache_status"}

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, mapping))

        assert len(records) == 1
        assert records[0].cache_status is None

    def test_parse_csv_skips_empty_rows(self, field_mapping):
        """Empty rows should be skipped."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0

2024-01-15T12:31:00Z,192.0.2.101,GET,example.com,/api,200,Bot/1.0"""

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 2

    def test_parse_csv_extra_columns(self, field_mapping):
        """Extra columns should be stored in extra field."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent,ray_id
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0,abc123"""

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 1
        assert records[0].extra.get("ray_id") == "abc123"

    def test_parse_csv_missing_required_mapping_raises(self, field_mapping):
        """Missing required field mapping should raise ParseError."""
        from llm_bot_pipeline.ingestion.exceptions import ParseError

        csv_data = "col1,col2,col3\nval1,val2,val3"

        parser = CSVParser()
        file = io.StringIO(csv_data)

        with pytest.raises(ParseError) as exc_info:
            list(parser.parse(file, {}))

        assert "Missing required field mappings" in str(exc_info.value)


class TestParseCSVFile:
    """Tests for parse_csv_file convenience function."""

    @pytest.fixture
    def field_mapping(self):
        return {
            "timestamp": "timestamp",
            "client_ip": "client_ip",
            "method": "method",
            "host": "host",
            "path": "path",
            "status_code": "status_code",
            "user_agent": "user_agent",
        }

    def test_parse_csv_file(self, field_mapping, tmp_path):
        """parse_csv_file should parse a file from disk."""
        csv_content = """timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0"""

        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        records = list(parse_csv_file(csv_file, field_mapping))
        assert len(records) == 1
        assert records[0].client_ip == "192.0.2.100"

    def test_parse_csv_file_not_found(self, field_mapping):
        """parse_csv_file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            list(parse_csv_file("/nonexistent/file.csv", field_mapping))

    def test_parse_tsv_file(self, field_mapping, tmp_path):
        """parse_tsv_file should parse tab-separated files."""
        tsv_content = (
            "timestamp\tclient_ip\tmethod\thost\tpath\tstatus_code\tuser_agent\n"
        )
        tsv_content += (
            "2024-01-15T12:30:45Z\t192.0.2.100\tGET\texample.com\t/api\t200\tBot/1.0"
        )

        tsv_file = tmp_path / "test.tsv"
        tsv_file.write_text(tsv_content)

        records = list(parse_tsv_file(tsv_file, field_mapping))
        assert len(records) == 1


# =============================================================================
# JSON Parser Tests
# =============================================================================


class TestJSONParser:
    """Tests for JSONParser class."""

    @pytest.fixture
    def field_mapping(self):
        """Standard field mapping for tests."""
        return {
            "timestamp": "timestamp",
            "client_ip": "client_ip",
            "method": "method",
            "host": "host",
            "path": "path",
            "status_code": "status_code",
            "user_agent": "user_agent",
        }

    def test_parse_ndjson(self, field_mapping):
        """Parse NDJSON (one object per line)."""
        ndjson_data = """{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}
{"timestamp": "2024-01-15T12:31:00Z", "client_ip": "192.0.2.101", "method": "POST", "host": "example.com", "path": "/submit", "status_code": 201, "user_agent": "Bot/1.0"}"""

        parser = JSONParser()
        file = io.StringIO(ndjson_data)
        records = list(parser.parse_ndjson(file, field_mapping))

        assert len(records) == 2
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[1].method == "POST"

    def test_parse_ndjson_with_empty_lines(self, field_mapping):
        """Empty lines should be skipped."""
        ndjson_data = """{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}

{"timestamp": "2024-01-15T12:31:00Z", "client_ip": "192.0.2.101", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}"""

        parser = JSONParser()
        file = io.StringIO(ndjson_data)
        records = list(parser.parse_ndjson(file, field_mapping))

        assert len(records) == 2

    def test_parse_json_array(self, field_mapping):
        """Parse JSON array of objects."""
        json_data = """[
            {"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"},
            {"timestamp": "2024-01-15T12:31:00Z", "client_ip": "192.0.2.101", "method": "POST", "host": "example.com", "path": "/submit", "status_code": 201, "user_agent": "Bot/1.0"}
        ]"""

        parser = JSONParser()
        file = io.StringIO(json_data)
        records = list(parser.parse_json(file, field_mapping))

        assert len(records) == 2

    def test_parse_json_single_object(self, field_mapping):
        """Parse single JSON object."""
        json_data = """{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}"""

        parser = JSONParser()
        file = io.StringIO(json_data)
        records = list(parser.parse_json(file, field_mapping))

        assert len(records) == 1

    def test_parse_json_nested_fields(self):
        """Parse JSON with nested fields using dot notation."""
        json_data = """[{"httpRequest": {"remoteIp": "192.0.2.100", "requestMethod": "GET"}, "timestamp": "2024-01-15T12:30:45Z", "host": "example.com", "path": "/api", "status": 200, "userAgent": "Bot/1.0"}]"""

        mapping = {
            "httpRequest.remoteIp": "client_ip",
            "httpRequest.requestMethod": "method",
            "timestamp": "timestamp",
            "host": "host",
            "path": "path",
            "status": "status_code",
            "userAgent": "user_agent",
        }

        parser = JSONParser()
        file = io.StringIO(json_data)
        records = list(parser.parse_json(file, mapping))

        assert len(records) == 1
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"

    def test_parse_json_with_records_path(self, field_mapping):
        """Parse JSON with records nested at a specific path."""
        json_data = """{"data": {"logs": [{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}]}}"""

        parser = JSONParser()
        file = io.StringIO(json_data)
        records = list(parser.parse_json(file, field_mapping, records_path="data.logs"))

        assert len(records) == 1

    def test_parse_ndjson_cloudflare_timestamps(self):
        """Parse Cloudflare-style nanosecond timestamps."""
        ndjson_data = """{"EdgeStartTimestamp": 1705323045000000000, "ClientIP": "192.0.2.100", "ClientRequestMethod": "GET", "ClientRequestHost": "example.com", "ClientRequestURI": "/api", "EdgeResponseStatus": 200, "ClientRequestUserAgent": "Bot/1.0"}"""

        mapping = {
            "EdgeStartTimestamp": "timestamp",
            "ClientIP": "client_ip",
            "ClientRequestMethod": "method",
            "ClientRequestHost": "host",
            "ClientRequestURI": "path",
            "EdgeResponseStatus": "status_code",
            "ClientRequestUserAgent": "user_agent",
        }

        parser = JSONParser()
        file = io.StringIO(ndjson_data)
        records = list(parser.parse_ndjson(file, mapping))

        assert len(records) == 1
        assert records[0].client_ip == "192.0.2.100"
        # Timestamp should be parsed correctly
        assert records[0].timestamp.year == 2024

    def test_parse_ndjson_extra_fields(self, field_mapping):
        """Extra fields should be stored in extra."""
        ndjson_data = """{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0", "ray_id": "abc123", "country": "US"}"""

        parser = JSONParser()
        file = io.StringIO(ndjson_data)
        records = list(parser.parse_ndjson(file, field_mapping))

        assert len(records) == 1
        assert records[0].extra.get("ray_id") == "abc123"
        assert records[0].extra.get("country") == "US"


class TestParseJSONFile:
    """Tests for JSON file parsing convenience functions."""

    @pytest.fixture
    def field_mapping(self):
        return {
            "timestamp": "timestamp",
            "client_ip": "client_ip",
            "method": "method",
            "host": "host",
            "path": "path",
            "status_code": "status_code",
            "user_agent": "user_agent",
        }

    def test_parse_ndjson_file(self, field_mapping, tmp_path):
        """parse_ndjson_file should parse file from disk."""
        content = """{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}"""

        ndjson_file = tmp_path / "test.ndjson"
        ndjson_file.write_text(content)

        records = list(parse_ndjson_file(ndjson_file, field_mapping))
        assert len(records) == 1

    def test_parse_json_file(self, field_mapping, tmp_path):
        """parse_json_file should parse JSON file from disk."""
        content = """[{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}]"""

        json_file = tmp_path / "test.json"
        json_file.write_text(content)

        records = list(parse_json_file(json_file, field_mapping))
        assert len(records) == 1

    def test_parse_json_file_not_found(self, field_mapping):
        """parse_json_file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            list(parse_json_file("/nonexistent/file.json", field_mapping))

    def test_parse_ndjson_file_not_found(self, field_mapping):
        """parse_ndjson_file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            list(parse_ndjson_file("/nonexistent/file.ndjson", field_mapping))


class TestGzipSupport:
    """Tests for gzip-compressed file support."""

    @pytest.fixture
    def field_mapping(self):
        return {
            "timestamp": "timestamp",
            "client_ip": "client_ip",
            "method": "method",
            "host": "host",
            "path": "path",
            "status_code": "status_code",
            "user_agent": "user_agent",
        }

    def test_parse_gzip_csv_file(self, field_mapping, tmp_path):
        """parse_csv_file should handle gzip-compressed files."""
        csv_content = """timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0
2024-01-15T12:31:00Z,192.0.2.101,POST,example.com,/submit,201,Bot/1.0"""

        # Write gzip compressed file
        csv_gz_file = tmp_path / "test.csv.gz"
        with gzip.open(csv_gz_file, "wt", encoding="utf-8") as f:
            f.write(csv_content)

        records = list(parse_csv_file(csv_gz_file, field_mapping))
        assert len(records) == 2
        assert records[0].client_ip == "192.0.2.100"
        assert records[1].method == "POST"

    def test_parse_gzip_tsv_file(self, field_mapping, tmp_path):
        """parse_tsv_file should handle gzip-compressed files."""
        tsv_content = (
            "timestamp\tclient_ip\tmethod\thost\tpath\tstatus_code\tuser_agent\n"
        )
        tsv_content += (
            "2024-01-15T12:30:45Z\t192.0.2.100\tGET\texample.com\t/api\t200\tBot/1.0"
        )

        tsv_gz_file = tmp_path / "test.tsv.gz"
        with gzip.open(tsv_gz_file, "wt", encoding="utf-8") as f:
            f.write(tsv_content)

        records = list(parse_tsv_file(tsv_gz_file, field_mapping))
        assert len(records) == 1
        assert records[0].path == "/api"

    def test_parse_gzip_ndjson_file(self, field_mapping, tmp_path):
        """parse_ndjson_file should handle gzip-compressed files."""
        ndjson_content = """{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}
{"timestamp": "2024-01-15T12:31:00Z", "client_ip": "192.0.2.101", "method": "POST", "host": "example.com", "path": "/submit", "status_code": 201, "user_agent": "Bot/1.0"}"""

        ndjson_gz_file = tmp_path / "test.ndjson.gz"
        with gzip.open(ndjson_gz_file, "wt", encoding="utf-8") as f:
            f.write(ndjson_content)

        records = list(parse_ndjson_file(ndjson_gz_file, field_mapping))
        assert len(records) == 2
        assert records[0].client_ip == "192.0.2.100"
        assert records[1].status_code == 201

    def test_parse_gzip_json_file(self, field_mapping, tmp_path):
        """parse_json_file should handle gzip-compressed files."""
        json_content = """[{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}]"""

        json_gz_file = tmp_path / "test.json.gz"
        with gzip.open(json_gz_file, "wt", encoding="utf-8") as f:
            f.write(json_content)

        records = list(parse_json_file(json_gz_file, field_mapping))
        assert len(records) == 1
        assert records[0].method == "GET"

    def test_parse_gzip_by_magic_bytes(self, field_mapping, tmp_path):
        """Gzip files without .gz extension should be detected by magic bytes."""
        csv_content = """timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0"""

        # Write gzip file but without .gz extension
        gzip_file = tmp_path / "test_compressed.csv"
        with gzip.open(gzip_file, "wt", encoding="utf-8") as f:
            f.write(csv_content)

        records = list(parse_csv_file(gzip_file, field_mapping))
        assert len(records) == 1
        assert records[0].client_ip == "192.0.2.100"


class TestParserEdgeCases:
    """Tests for parser edge cases and error handling."""

    @pytest.fixture
    def field_mapping(self):
        return {
            "timestamp": "timestamp",
            "client_ip": "client_ip",
            "method": "method",
            "host": "host",
            "path": "path",
            "status_code": "status_code",
            "user_agent": "user_agent",
        }

    def test_csv_with_quoted_fields(self, field_mapping):
        """CSV with quoted fields containing delimiters."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,"/api/data?key=value,more",200,"Mozilla/5.0 (Windows NT; compatible)"""

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 1
        assert "/api/data?key=value,more" in records[0].path

    def test_json_with_invalid_line_non_strict(self, field_mapping):
        """Invalid JSON lines should be skipped in non-strict mode."""
        ndjson_data = """{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}
invalid json line
{"timestamp": "2024-01-15T12:31:00Z", "client_ip": "192.0.2.101", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}"""

        parser = JSONParser(strict_validation=False)
        file = io.StringIO(ndjson_data)
        records = list(parser.parse_ndjson(file, field_mapping))

        assert len(records) == 2

    def test_json_with_invalid_line_strict_raises(self, field_mapping):
        """Invalid JSON lines should raise in strict mode."""
        from llm_bot_pipeline.ingestion.exceptions import ParseError

        ndjson_data = """{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}
invalid json line"""

        parser = JSONParser(strict_validation=True)
        file = io.StringIO(ndjson_data)

        with pytest.raises(ParseError):
            list(parser.parse_ndjson(file, field_mapping))

    def test_csv_with_dash_as_null(self, field_mapping):
        """Dash '-' should be treated as null."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent,cache_status
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0,-"""

        mapping = {**field_mapping, "cache_status": "cache_status"}

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, mapping))

        assert len(records) == 1
        assert records[0].cache_status is None

    def test_csv_with_unix_timestamp(self, field_mapping):
        """CSV should parse Unix timestamps in various formats."""
        # Unix timestamp in seconds
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent
1705323045,192.0.2.100,GET,example.com,/api,200,Bot/1.0"""

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 1
        assert records[0].timestamp.year == 2024

    def test_csv_with_nanosecond_timestamp(self, field_mapping):
        """CSV should parse nanosecond timestamps (Cloudflare format)."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent
1705323045000000000,192.0.2.100,GET,example.com,/api,200,Bot/1.0"""

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 1
        assert records[0].timestamp.year == 2024

    def test_csv_strict_validation_raises(self, field_mapping):
        """Strict validation should raise on invalid records."""
        from llm_bot_pipeline.ingestion.exceptions import ParseError

        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,invalid-ip,GET,example.com,/api,200,Bot/1.0"""

        parser = CSVParser(strict_validation=True)
        file = io.StringIO(csv_data)

        with pytest.raises(ParseError):
            list(parser.parse(file, field_mapping))

    def test_json_invalid_records_path_raises(self, field_mapping):
        """Invalid records_path should raise ParseError."""
        from llm_bot_pipeline.ingestion.exceptions import ParseError

        json_data = """{"data": {"logs": []}}"""

        parser = JSONParser()
        file = io.StringIO(json_data)

        with pytest.raises(ParseError) as exc_info:
            list(
                parser.parse_json(file, field_mapping, records_path="nonexistent.path")
            )

        assert "not found" in str(exc_info.value)

    def test_json_optional_fields_not_duplicated_in_extra(self, field_mapping):
        """Optional schema fields should not appear in extra."""
        ndjson_data = """{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0", "cache_status": "HIT", "custom_field": "value"}"""

        parser = JSONParser()
        file = io.StringIO(ndjson_data)
        records = list(parser.parse_ndjson(file, field_mapping))

        assert len(records) == 1
        # cache_status is a schema field, should be in the record, not extra
        assert records[0].cache_status == "HIT"
        assert "cache_status" not in records[0].extra
        # custom_field is not in schema, should be in extra
        assert records[0].extra.get("custom_field") == "value"

    def test_csv_optional_fields_auto_mapped(self, field_mapping):
        """CSV columns matching optional schema fields should be auto-mapped."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent,cache_status,response_bytes
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0,HIT,1234"""

        # Note: field_mapping doesn't include cache_status or response_bytes
        # They should be auto-mapped because they match schema field names

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 1
        assert records[0].cache_status == "HIT"
        assert records[0].response_bytes == 1234

    def test_csv_skipped_records_counted_correctly(self, field_mapping, caplog):
        """Skipped records should be counted in non-strict mode."""
        import logging

        # First row valid, second row has invalid IP (will fail validation)
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent
2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0
2024-01-15T12:31:00Z,invalid-ip,GET,example.com,/api,200,Bot/1.0
2024-01-15T12:32:00Z,192.0.2.102,GET,example.com,/api,200,Bot/1.0"""

        parser = CSVParser(strict_validation=False)
        file = io.StringIO(csv_data)

        with caplog.at_level(logging.INFO):
            records = list(parser.parse(file, field_mapping))

        # Should have 2 valid records
        assert len(records) == 2
        # Log should show 2 parsed, 1 skipped
        assert "2 records parsed" in caplog.text
        assert "1 skipped" in caplog.text

    def test_csv_empty_file_no_header(self, field_mapping, caplog):
        """Completely empty CSV file should be handled gracefully."""
        import logging

        csv_data = ""

        parser = CSVParser()
        file = io.StringIO(csv_data)

        with caplog.at_level(logging.WARNING):
            records = list(parser.parse(file, field_mapping))

        assert len(records) == 0
        assert "Empty CSV" in caplog.text

    def test_csv_with_bom(self, field_mapping):
        """CSV with BOM (byte order mark) should be parsed correctly."""
        # BOM is common in files exported from Excel
        csv_data = "\ufefftimestamp,client_ip,method,host,path,status_code,user_agent\n"
        csv_data += "2024-01-15T12:30:45Z,192.0.2.100,GET,example.com,/api,200,Bot/1.0"

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 1
        assert records[0].client_ip == "192.0.2.100"

    def test_csv_header_only_no_data(self, field_mapping, caplog):
        """CSV with only header and no data rows should return empty."""
        import logging

        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent"""

        parser = CSVParser()
        file = io.StringIO(csv_data)

        with caplog.at_level(logging.INFO):
            records = list(parser.parse(file, field_mapping))

        assert len(records) == 0
        assert "0 records parsed" in caplog.text

    def test_csv_with_millisecond_timestamp(self, field_mapping):
        """CSV should parse millisecond timestamps."""
        csv_data = """timestamp,client_ip,method,host,path,status_code,user_agent
1705323045000,192.0.2.100,GET,example.com,/api,200,Bot/1.0"""

        parser = CSVParser()
        file = io.StringIO(csv_data)
        records = list(parser.parse(file, field_mapping))

        assert len(records) == 1
        assert records[0].timestamp.year == 2024
        assert records[0].timestamp.tzinfo is not None

    def test_json_array_with_non_object_elements(self, field_mapping):
        """JSON array with non-object elements should skip invalid entries."""
        json_data = """[
            {"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.100", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"},
            "not an object",
            123,
            null,
            {"timestamp": "2024-01-15T12:31:00Z", "client_ip": "192.0.2.101", "method": "GET", "host": "example.com", "path": "/api", "status_code": 200, "user_agent": "Bot/1.0"}
        ]"""

        parser = JSONParser(strict_validation=False)
        file = io.StringIO(json_data)
        records = list(parser.parse_json(file, field_mapping))

        # Should have 2 valid records, 3 skipped (string, int, null)
        assert len(records) == 2
