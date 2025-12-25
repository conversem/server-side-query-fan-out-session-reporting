"""
Unit tests for ingestion module base classes and data models.

Tests IngestionRecord, IngestionSource, IngestionAdapter interface,
IngestionRegistry functionality, and custom exceptions.
"""

from datetime import datetime, timezone
from typing import Iterator, Optional

import pytest

from llm_bot_pipeline.ingestion import (
    IngestionAdapter,
    IngestionError,
    IngestionRecord,
    IngestionRegistry,
    IngestionSource,
    ParseError,
    ProviderNotFoundError,
    SourceValidationError,
    ValidationError,
    get_adapter,
    list_providers,
    register_adapter,
)


class TestIngestionRecord:
    """Tests for IngestionRecord dataclass."""

    def test_create_minimal_record(self):
        """Create record with only required fields."""
        timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        record = IngestionRecord(
            timestamp=timestamp,
            client_ip="192.0.2.100",
            method="GET",
            host="example.com",
            path="/api/data",
            status_code=200,
            user_agent="Mozilla/5.0 (compatible; GPTBot/1.0)",
        )

        assert record.timestamp == timestamp
        assert record.client_ip == "192.0.2.100"
        assert record.method == "GET"
        assert record.host == "example.com"
        assert record.path == "/api/data"
        assert record.status_code == 200
        assert record.user_agent == "Mozilla/5.0 (compatible; GPTBot/1.0)"

    def test_create_full_record(self):
        """Create record with all optional fields."""
        timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        record = IngestionRecord(
            timestamp=timestamp,
            client_ip="192.0.2.100",
            method="GET",
            host="example.com",
            path="/api/data",
            status_code=200,
            user_agent="Mozilla/5.0 (compatible; GPTBot/1.0)",
            query_string="key=value",
            response_bytes=1234,
            request_bytes=567,
            response_time_ms=45,
            cache_status="HIT",
            edge_location="LAX1",
            referer="https://google.com",
            protocol="HTTP/2",
            ssl_protocol="TLSv1.3",
            extra={"ray_id": "abc123"},
        )

        assert record.query_string == "key=value"
        assert record.response_bytes == 1234
        assert record.request_bytes == 567
        assert record.response_time_ms == 45
        assert record.cache_status == "HIT"
        assert record.edge_location == "LAX1"
        assert record.referer == "https://google.com"
        assert record.protocol == "HTTP/2"
        assert record.ssl_protocol == "TLSv1.3"
        assert record.extra == {"ray_id": "abc123"}

    def test_optional_fields_default_none(self):
        """Optional fields should default to None."""
        record = IngestionRecord(
            timestamp=datetime.now(timezone.utc),
            client_ip="192.0.2.100",
            method="GET",
            host="example.com",
            path="/",
            status_code=200,
            user_agent="Bot/1.0",
        )

        assert record.query_string is None
        assert record.response_bytes is None
        assert record.request_bytes is None
        assert record.response_time_ms is None
        assert record.cache_status is None
        assert record.edge_location is None
        assert record.referer is None
        assert record.protocol is None
        assert record.ssl_protocol is None
        assert record.extra == {}

    def test_to_dict(self):
        """to_dict should return proper dictionary representation."""
        timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        record = IngestionRecord(
            timestamp=timestamp,
            client_ip="192.0.2.100",
            method="GET",
            host="example.com",
            path="/api/data",
            status_code=200,
            user_agent="Bot/1.0",
            cache_status="HIT",
            extra={"ray_id": "abc123"},
        )

        result = record.to_dict()

        assert result["timestamp"] == "2024-01-15T12:30:45+00:00"
        assert result["client_ip"] == "192.0.2.100"
        assert result["method"] == "GET"
        assert result["host"] == "example.com"
        assert result["path"] == "/api/data"
        assert result["status_code"] == 200
        assert result["user_agent"] == "Bot/1.0"
        assert result["cache_status"] == "HIT"
        assert result["_extra_ray_id"] == "abc123"

    def test_from_dict_minimal(self):
        """from_dict should create record from minimal dict."""
        data = {
            "timestamp": "2024-01-15T12:30:45+00:00",
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/api/data",
            "status_code": 200,
            "user_agent": "Bot/1.0",
        }

        record = IngestionRecord.from_dict(data)

        assert record.client_ip == "192.0.2.100"
        assert record.method == "GET"
        assert record.status_code == 200

    def test_from_dict_with_datetime_object(self):
        """from_dict should accept datetime objects for timestamp."""
        timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        data = {
            "timestamp": timestamp,
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
        }

        record = IngestionRecord.from_dict(data)
        assert record.timestamp == timestamp

    def test_from_dict_with_extras(self):
        """from_dict should extract _extra_ prefixed fields."""
        data = {
            "timestamp": "2024-01-15T12:30:45+00:00",
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
            "_extra_ray_id": "abc123",
            "_extra_custom_field": "value",
        }

        record = IngestionRecord.from_dict(data)

        assert record.extra == {"ray_id": "abc123", "custom_field": "value"}

    def test_from_dict_missing_required_field(self):
        """from_dict should raise ValidationError for missing fields."""
        data = {
            "timestamp": "2024-01-15T12:30:45+00:00",
            "client_ip": "192.0.2.100",
            # Missing: method, host, path, status_code, user_agent
        }

        with pytest.raises(ValidationError) as exc_info:
            IngestionRecord.from_dict(data)

        assert exc_info.value.field == "method"

    def test_from_dict_invalid_timestamp(self):
        """from_dict should raise ValidationError for invalid timestamp."""
        data = {
            "timestamp": "not-a-timestamp",
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
        }

        with pytest.raises(ValidationError) as exc_info:
            IngestionRecord.from_dict(data)

        assert exc_info.value.field == "timestamp"

    def test_roundtrip_to_dict_from_dict(self):
        """to_dict followed by from_dict should preserve data."""
        timestamp = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        original = IngestionRecord(
            timestamp=timestamp,
            client_ip="192.0.2.100",
            method="GET",
            host="example.com",
            path="/api/data",
            status_code=200,
            user_agent="Bot/1.0",
            cache_status="HIT",
            response_bytes=1234,
            extra={"ray_id": "abc123"},
        )

        data = original.to_dict()
        restored = IngestionRecord.from_dict(data)

        assert restored.timestamp == original.timestamp
        assert restored.client_ip == original.client_ip
        assert restored.method == original.method
        assert restored.host == original.host
        assert restored.path == original.path
        assert restored.status_code == original.status_code
        assert restored.user_agent == original.user_agent
        assert restored.cache_status == original.cache_status
        assert restored.response_bytes == original.response_bytes
        assert restored.extra == original.extra

    def test_from_dict_timestamp_always_timezone_aware(self):
        """from_dict should always return timezone-aware timestamps."""
        # Test with Unix timestamp
        data_unix = {
            "timestamp": 1705323045,
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
        }
        record_unix = IngestionRecord.from_dict(data_unix)
        assert record_unix.timestamp.tzinfo is not None

        # Test with ISO string (no timezone)
        data_iso = {
            "timestamp": "2024-01-15T12:30:45",
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
        }
        record_iso = IngestionRecord.from_dict(data_iso)
        assert record_iso.timestamp.tzinfo is not None

        # Test with naive datetime object
        naive_dt = datetime(2024, 1, 15, 12, 30, 45)
        data_naive = {
            "timestamp": naive_dt,
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
        }
        record_naive = IngestionRecord.from_dict(data_naive)
        assert record_naive.timestamp.tzinfo is not None

    def test_from_dict_with_unix_timestamp(self):
        """from_dict should handle Unix timestamps."""
        data = {
            "timestamp": 1705323045,  # Unix seconds
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
        }

        record = IngestionRecord.from_dict(data)
        assert record.timestamp.year == 2024

    def test_from_dict_with_nanosecond_timestamp(self):
        """from_dict should handle nanosecond timestamps (Cloudflare format)."""
        data = {
            "timestamp": 1705323045000000000,  # Nanoseconds
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
        }

        record = IngestionRecord.from_dict(data)
        assert record.timestamp.year == 2024

    def test_from_dict_with_string_unix_timestamp(self):
        """from_dict should handle string Unix timestamps."""
        data = {
            "timestamp": "1705323045",  # Unix seconds as string
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
        }

        record = IngestionRecord.from_dict(data)
        assert record.timestamp.year == 2024

    def test_from_dict_with_extra_dict_key(self):
        """from_dict should handle 'extra' key containing a dict."""
        data = {
            "timestamp": "2024-01-15T12:30:45+00:00",
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
            "extra": {"custom_field": "value", "another": 123},
        }

        record = IngestionRecord.from_dict(data)
        assert record.extra == {"custom_field": "value", "another": 123}

    def test_from_dict_merges_extra_sources(self):
        """from_dict should merge _extra_ prefixed keys with 'extra' dict."""
        data = {
            "timestamp": "2024-01-15T12:30:45+00:00",
            "client_ip": "192.0.2.100",
            "method": "GET",
            "host": "example.com",
            "path": "/",
            "status_code": 200,
            "user_agent": "Bot/1.0",
            "_extra_ray_id": "abc123",
            "extra": {"custom_field": "value"},
        }

        record = IngestionRecord.from_dict(data)
        assert record.extra == {"ray_id": "abc123", "custom_field": "value"}


class TestIngestionSource:
    """Tests for IngestionSource dataclass."""

    def test_create_api_source(self):
        """Create an API source."""
        source = IngestionSource(
            provider="cloudflare",
            source_type="api",
            path_or_uri="zone_abc123",
        )

        assert source.provider == "cloudflare"
        assert source.source_type == "api"
        assert source.path_or_uri == "zone_abc123"
        assert source.is_api_source() is True
        assert source.is_file_source() is False
        assert source.is_cloud_source() is False

    def test_create_file_source(self):
        """Create a local file source."""
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri="/data/logs.csv",
        )

        assert source.is_file_source() is True
        assert source.is_api_source() is False
        assert source.is_cloud_source() is False

    def test_create_w3c_file_source(self):
        """Create a W3C extended log format file source."""
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri="/data/cloudfront-logs.tsv",
        )

        assert source.source_type == "w3c_file"
        assert source.is_file_source() is True
        assert source.is_api_source() is False
        assert source.is_cloud_source() is False

    def test_create_cloud_source(self):
        """Create a cloud storage source."""
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="s3",
            path_or_uri="s3://bucket/logs/",
            credentials={"aws_access_key": "key", "aws_secret_key": "secret"},
        )

        assert source.is_cloud_source() is True
        assert source.is_api_source() is False
        assert source.is_file_source() is False
        assert source.credentials == {
            "aws_access_key": "key",
            "aws_secret_key": "secret",
        }

    def test_all_file_source_types(self):
        """All file source types should be recognized."""
        for source_type in [
            "csv_file",
            "json_file",
            "ndjson_file",
            "tsv_file",
            "w3c_file",
        ]:
            source = IngestionSource(
                provider="test", source_type=source_type, path_or_uri="/data/logs"
            )
            assert source.is_file_source() is True

    def test_all_cloud_source_types(self):
        """All cloud source types should be recognized."""
        for source_type in ["s3", "gcs", "azure_blob"]:
            source = IngestionSource(
                provider="test", source_type=source_type, path_or_uri="bucket/path"
            )
            assert source.is_cloud_source() is True

    def test_invalid_source_type_raises_error(self):
        """Invalid source type should raise SourceValidationError."""
        with pytest.raises(SourceValidationError) as exc_info:
            IngestionSource(
                provider="test",
                source_type="invalid_type",
                path_or_uri="/path",
            )

        assert "invalid_type" in str(exc_info.value)
        assert exc_info.value.source_type == "invalid_type"

    def test_options_default_empty_dict(self):
        """Options should default to empty dict."""
        source = IngestionSource(
            provider="test",
            source_type="api",
            path_or_uri="/path",
        )
        assert source.options == {}
        assert source.credentials == {}


class TestIngestionAdapter:
    """Tests for IngestionAdapter abstract base class."""

    def test_cannot_instantiate_abstract_class(self):
        """Abstract class should not be instantiable."""
        with pytest.raises(TypeError):
            IngestionAdapter()

    def test_concrete_implementation_works(self):
        """Concrete implementation with all methods should work."""

        class MockAdapter(IngestionAdapter):
            @property
            def provider_name(self) -> str:
                return "mock_provider"

            @property
            def supported_source_types(self) -> list[str]:
                return ["csv_file", "json_file"]

            def ingest(
                self,
                source: IngestionSource,
                start_time: Optional[datetime] = None,
                end_time: Optional[datetime] = None,
                filter_bots: bool = True,
                **kwargs,
            ) -> Iterator[IngestionRecord]:
                yield IngestionRecord(
                    timestamp=datetime.now(timezone.utc),
                    client_ip="127.0.0.1",
                    method="GET",
                    host="test.com",
                    path="/",
                    status_code=200,
                    user_agent="TestBot/1.0",
                )

            def validate_source(self, source: IngestionSource) -> tuple[bool, str]:
                return (True, "")

        adapter = MockAdapter()
        assert adapter.provider_name == "mock_provider"
        assert adapter.supported_source_types == ["csv_file", "json_file"]
        assert adapter.supports_source_type("csv_file") is True
        assert adapter.supports_source_type("s3") is False

    def test_supports_source_type_method(self):
        """supports_source_type should check against supported list."""

        class MockAdapter(IngestionAdapter):
            @property
            def provider_name(self) -> str:
                return "mock"

            @property
            def supported_source_types(self) -> list[str]:
                return ["api", "csv_file"]

            def ingest(self, source, **kwargs):
                yield from []

            def validate_source(self, source):
                return (True, "")

        adapter = MockAdapter()
        assert adapter.supports_source_type("api") is True
        assert adapter.supports_source_type("csv_file") is True
        assert adapter.supports_source_type("s3") is False
        assert adapter.supports_source_type("json_file") is False


class TestIngestionRegistry:
    """Tests for IngestionRegistry."""

    def setup_method(self):
        """Clear registry before each test."""
        IngestionRegistry.clear()

    def teardown_method(self):
        """Clear registry after each test."""
        IngestionRegistry.clear()

    def _create_mock_adapter(self, name: str):
        """Create a mock adapter class for testing."""

        class MockAdapter(IngestionAdapter):
            @property
            def provider_name(self) -> str:
                return name

            @property
            def supported_source_types(self) -> list[str]:
                return ["csv_file"]

            def ingest(self, source, **kwargs):
                yield from []

            def validate_source(self, source):
                return (True, "")

        return MockAdapter

    def test_register_and_get_adapter(self):
        """Register and retrieve an adapter."""
        MockAdapter = self._create_mock_adapter("test_provider")
        IngestionRegistry.register_provider("test_provider", MockAdapter)

        adapter = IngestionRegistry.get_adapter("test_provider")
        assert adapter.provider_name == "test_provider"

    def test_register_decorator(self):
        """Register using decorator syntax."""

        @IngestionRegistry.register("decorated_provider")
        class DecoratedAdapter(IngestionAdapter):
            @property
            def provider_name(self) -> str:
                return "decorated_provider"

            @property
            def supported_source_types(self) -> list[str]:
                return ["api"]

            def ingest(self, source, **kwargs):
                yield from []

            def validate_source(self, source):
                return (True, "")

        adapter = IngestionRegistry.get_adapter("decorated_provider")
        assert adapter.provider_name == "decorated_provider"

    def test_get_unknown_provider_raises_error(self):
        """Getting unknown provider should raise ProviderNotFoundError."""
        with pytest.raises(ProviderNotFoundError) as exc_info:
            IngestionRegistry.get_adapter("unknown_provider")

        assert exc_info.value.provider_name == "unknown_provider"

    def test_list_providers(self):
        """List all registered providers."""
        MockA = self._create_mock_adapter("provider_a")
        MockB = self._create_mock_adapter("provider_b")

        IngestionRegistry.register_provider("provider_a", MockA)
        IngestionRegistry.register_provider("provider_b", MockB)

        providers = IngestionRegistry.list_providers()
        assert "provider_a" in providers
        assert "provider_b" in providers
        assert providers == sorted(providers)  # Should be sorted

    def test_is_provider_registered(self):
        """Check if provider is registered."""
        MockAdapter = self._create_mock_adapter("registered")
        IngestionRegistry.register_provider("registered", MockAdapter)

        assert IngestionRegistry.is_provider_registered("registered") is True
        assert IngestionRegistry.is_provider_registered("not_registered") is False

    def test_case_insensitive_lookup(self):
        """Provider lookup should be case-insensitive."""
        MockAdapter = self._create_mock_adapter("CamelCase")
        IngestionRegistry.register_provider("CamelCase", MockAdapter)

        adapter = IngestionRegistry.get_adapter("camelcase")
        assert adapter is not None

    def test_clear_registry(self):
        """Clear should remove all registered adapters."""
        MockAdapter = self._create_mock_adapter("test")
        IngestionRegistry.register_provider("test", MockAdapter)

        IngestionRegistry.clear()

        assert IngestionRegistry.list_providers() == []

    def test_register_non_adapter_raises_error(self):
        """Registering non-IngestionAdapter should raise TypeError."""

        class NotAnAdapter:
            pass

        with pytest.raises(TypeError) as exc_info:
            IngestionRegistry.register_provider("bad", NotAnAdapter)

        assert "IngestionAdapter" in str(exc_info.value)

    def test_get_adapter_class(self):
        """Get adapter class without instantiation."""
        MockAdapter = self._create_mock_adapter("test_provider")
        IngestionRegistry.register_provider("test_provider", MockAdapter)

        adapter_class = IngestionRegistry.get_adapter_class("test_provider")

        assert adapter_class is MockAdapter
        # Verify it's a class, not an instance
        assert isinstance(adapter_class, type)

    def test_get_adapter_class_unknown_raises_error(self):
        """Getting unknown provider class should raise ProviderNotFoundError."""
        with pytest.raises(ProviderNotFoundError):
            IngestionRegistry.get_adapter_class("unknown")


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def setup_method(self):
        """Clear registry before each test."""
        IngestionRegistry.clear()

    def teardown_method(self):
        """Clear registry after each test."""
        IngestionRegistry.clear()

    def _create_mock_adapter(self, name: str):
        """Create a mock adapter class for testing."""

        class MockAdapter(IngestionAdapter):
            @property
            def provider_name(self) -> str:
                return name

            @property
            def supported_source_types(self) -> list[str]:
                return ["csv_file"]

            def ingest(self, source, **kwargs):
                yield from []

            def validate_source(self, source):
                return (True, "")

        return MockAdapter

    def test_get_adapter_function(self):
        """get_adapter convenience function should work."""
        MockAdapter = self._create_mock_adapter("func_test")
        IngestionRegistry.register_provider("func_test", MockAdapter)

        adapter = get_adapter("func_test")
        assert adapter.provider_name == "func_test"

    def test_register_adapter_function(self):
        """register_adapter convenience function should work."""
        MockAdapter = self._create_mock_adapter("registered_via_func")

        register_adapter("registered_via_func", MockAdapter)

        assert IngestionRegistry.is_provider_registered("registered_via_func")

    def test_list_providers_function(self):
        """list_providers convenience function should work."""
        MockA = self._create_mock_adapter("provider_x")
        MockB = self._create_mock_adapter("provider_y")
        IngestionRegistry.register_provider("provider_x", MockA)
        IngestionRegistry.register_provider("provider_y", MockB)

        providers = list_providers()

        assert "provider_x" in providers
        assert "provider_y" in providers


class TestIngestionError:
    """Tests for IngestionError base exception."""

    def test_basic_error(self):
        """Create base IngestionError."""
        error = IngestionError("Something went wrong")
        assert str(error) == "Something went wrong"

    def test_inherits_from_exception(self):
        """IngestionError should inherit from Exception."""
        error = IngestionError("test")
        assert isinstance(error, Exception)

    def test_can_catch_derived_exceptions(self):
        """Should be able to catch all derived exceptions."""
        with pytest.raises(IngestionError):
            raise ValidationError("test")

        with pytest.raises(IngestionError):
            raise ParseError("test")

        with pytest.raises(IngestionError):
            raise ProviderNotFoundError("test")

        with pytest.raises(IngestionError):
            raise SourceValidationError("test")


class TestValidationError:
    """Tests for ValidationError exception."""

    def test_basic_error(self):
        """Create error with just message."""
        error = ValidationError("Invalid data")
        assert str(error) == "Invalid data"

    def test_error_with_field(self):
        """Create error with field context."""
        error = ValidationError("Missing required field", field="timestamp")
        assert "timestamp" in str(error)
        assert error.field == "timestamp"

    def test_error_with_field_and_value(self):
        """Create error with field and value context."""
        error = ValidationError("Invalid type", field="status_code", value="not_an_int")
        assert "status_code" in str(error)
        assert "not_an_int" in str(error)


class TestParseError:
    """Tests for ParseError exception."""

    def test_basic_error(self):
        """Create error with just message."""
        error = ParseError("Failed to parse JSON")
        assert str(error) == "Failed to parse JSON"

    def test_error_with_line_number(self):
        """Create error with line number."""
        error = ParseError("Malformed CSV", line_number=42)
        assert "line 42" in str(error)
        assert error.line_number == 42

    def test_error_with_line_content(self):
        """Create error with line content."""
        error = ParseError(
            "Invalid format", line_number=10, line_content="bad,data,here"
        )
        assert "line 10" in str(error)
        assert "bad,data,here" in str(error)

    def test_long_line_content_truncated(self):
        """Long line content should be truncated."""
        long_line = "x" * 200
        error = ParseError("Error", line_number=1, line_content=long_line)
        # Should be truncated to ~100 chars + "..."
        assert "..." in str(error)
        assert len(str(error)) < 250


class TestProviderNotFoundError:
    """Tests for ProviderNotFoundError exception."""

    def test_basic_error(self):
        """Create error with just provider name."""
        error = ProviderNotFoundError("unknown")
        assert "unknown" in str(error)
        assert "No providers registered" in str(error)

    def test_error_with_available_providers(self):
        """Create error with available providers list."""
        error = ProviderNotFoundError(
            "unknown", available_providers=["cloudflare", "aws_cloudfront"]
        )
        assert "unknown" in str(error)
        assert "cloudflare" in str(error)
        assert "aws_cloudfront" in str(error)
        assert error.available_providers == ["cloudflare", "aws_cloudfront"]


class TestSourceValidationError:
    """Tests for SourceValidationError exception."""

    def test_basic_error(self):
        """Create error with just message."""
        error = SourceValidationError("Source not accessible")
        assert str(error) == "Source not accessible"

    def test_error_with_source_type(self):
        """Create error with source type."""
        error = SourceValidationError("Invalid configuration", source_type="s3")
        assert "s3" in str(error)
        assert error.source_type == "s3"

    def test_error_with_reason(self):
        """Create error with reason."""
        error = SourceValidationError(
            "Validation failed", source_type="api", reason="Missing credentials"
        )
        assert "api" in str(error)
        assert "Missing credentials" in str(error)
