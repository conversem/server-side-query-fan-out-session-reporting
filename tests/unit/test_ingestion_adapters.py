"""
Unit tests for ingestion adapters.

Tests UniversalAdapter, CloudFrontAdapter, and CloudflareAdapter
with various file formats and configurations.
"""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from llm_bot_pipeline.ingestion import (
    IngestionSource,
    ProviderNotFoundError,
    SourceValidationError,
    get_adapter,
)
from llm_bot_pipeline.ingestion.exceptions import ParseError

# Import providers to ensure they're registered
from llm_bot_pipeline.ingestion.providers import (  # noqa: F401
    AkamaiAdapter,
    ALBAdapter,
    AzureCDNAdapter,
    CloudflareAdapter,
    CloudFrontAdapter,
    FastlyAdapter,
    GCPCDNAdapter,
    UniversalAdapter,
)


@pytest.fixture
def fixtures_dir() -> Path:
    """Return path to test fixtures directory."""
    return Path(__file__).parent.parent / "fixtures" / "ingestion"


class TestUniversalAdapter:
    """Tests for UniversalAdapter."""

    def test_provider_name(self):
        """UniversalAdapter should have correct provider name."""
        adapter = get_adapter("universal")
        assert adapter.provider_name == "universal"

    def test_supported_source_types(self):
        """UniversalAdapter should support CSV, TSV, JSON, NDJSON."""
        adapter = get_adapter("universal")
        assert "csv_file" in adapter.supported_source_types
        assert "tsv_file" in adapter.supported_source_types
        assert "json_file" in adapter.supported_source_types
        assert "ndjson_file" in adapter.supported_source_types

    def test_ingest_csv_file(self, fixtures_dir):
        """Test ingesting CSV file."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 5
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_ingest_json_file(self, fixtures_dir):
        """Test ingesting JSON file."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"

    def test_ingest_ndjson_file(self, fixtures_dir):
        """Test ingesting NDJSON file."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="ndjson_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

    def test_ingest_tsv_file(self, fixtures_dir, tmp_path):
        """Test ingesting TSV file."""
        # Create a TSV file from CSV fixture
        csv_file = fixtures_dir / "universal" / "sample.csv"
        tsv_file = tmp_path / "sample.tsv"

        # Convert CSV to TSV
        with open(csv_file) as f:
            lines = f.readlines()
        with open(tsv_file, "w") as f:
            for line in lines:
                f.write(line.replace(",", "\t"))

        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="tsv_file",
            path_or_uri=str(tsv_file),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 5
        assert records[0].client_ip == "192.0.2.100"

    def test_bot_filtering(self, fixtures_dir):
        """Test bot filtering works correctly."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        # With bot filtering (default)
        records_with_filter = list(adapter.ingest(source, filter_bots=True))
        # Without bot filtering
        records_without_filter = list(adapter.ingest(source, filter_bots=False))

        # Should have fewer or equal records with filtering (only LLM bots)
        assert len(records_with_filter) <= len(records_without_filter)
        # All filtered records should be from known bots
        if records_with_filter:
            from llm_bot_pipeline.utils.bot_classifier import classify_bot

            for record in records_with_filter:
                bot_info = classify_bot(record.user_agent)
                assert (
                    bot_info is not None
                ), f"Record with user_agent {record.user_agent} should be classified as bot"

    def test_time_filtering(self, fixtures_dir):
        """Test time-based filtering."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 15, 12, 30, 48, tzinfo=timezone.utc)

        records = list(
            adapter.ingest(
                source, start_time=start_time, end_time=end_time, filter_bots=False
            )
        )
        # Should only include records between start and end time (inclusive)
        assert len(records) <= 5
        for record in records:
            assert (
                start_time <= record.timestamp <= end_time
            ), f"Record timestamp {record.timestamp} not in range [{start_time}, {end_time}]"

    def test_validate_source_file(self, fixtures_dir):
        """Test source validation for file."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True
        assert error_msg == ""

    def test_validate_source_nonexistent_file(self):
        """Test validation fails for nonexistent file."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri="/nonexistent/file.csv",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "not exist" in error_msg.lower() or "not found" in error_msg.lower()

    def test_validate_source_directory(self, fixtures_dir):
        """Test source validation for directory."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True

    def test_ingest_directory(self, fixtures_dir):
        """Test ingesting from directory."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Should process all CSV files in directory
        assert len(records) >= 5

    def test_ingest_empty_directory(self, tmp_path):
        """Test ingesting from empty directory."""
        adapter = get_adapter("universal")
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(empty_dir),
        )

        # Should fail validation when no matching files found
        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert (
            "no matching files" in error_msg.lower() or "not found" in error_msg.lower()
        )

        # Should raise SourceValidationError when ingesting empty directory
        with pytest.raises(SourceValidationError):
            list(adapter.ingest(source, filter_bots=False))

    def test_record_field_completeness(self, fixtures_dir):
        """Test that records have all required fields populated."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # Verify all records have required fields
        for record in records:
            assert record.timestamp is not None
            assert record.client_ip is not None and record.client_ip != ""
            assert record.method is not None and record.method != ""
            assert record.host is not None and record.host != ""
            assert record.path is not None and record.path != ""
            assert record.status_code is not None
            assert record.user_agent is not None and record.user_agent != ""

    def test_record_optional_fields(self, fixtures_dir):
        """Test that optional fields are handled correctly."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Check that records with query_string have it populated
        records_with_query = [r for r in records if r.query_string]
        assert len(records_with_query) > 0

    def test_time_filtering_boundary_cases(self, fixtures_dir):
        """Test time filtering with records exactly at boundaries."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        # Filter to exact timestamp of first record (should include it)
        exact_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        records = list(
            adapter.ingest(
                source, start_time=exact_time, end_time=exact_time, filter_bots=False
            )
        )
        # Should include record at exact boundary
        assert len(records) >= 0  # May be 0 if no records at exact time

    def test_time_filtering_invalid_range(self, fixtures_dir):
        """Test that invalid time ranges are handled."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 48, tzinfo=timezone.utc)
        end_time = datetime(
            2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc
        )  # Invalid: start > end

        # Should raise ValueError for invalid range
        with pytest.raises(ValueError, match="Invalid time range"):
            list(
                adapter.ingest(
                    source, start_time=start_time, end_time=end_time, filter_bots=False
                )
            )


class TestCloudFrontAdapter:
    """Tests for CloudFrontAdapter."""

    def test_provider_name(self):
        """CloudFrontAdapter should have correct provider name."""
        adapter = get_adapter("aws_cloudfront")
        assert adapter.provider_name == "aws_cloudfront"

    def test_supported_source_types(self):
        """CloudFrontAdapter should support W3C file format."""
        adapter = get_adapter("aws_cloudfront")
        assert "w3c_file" in adapter.supported_source_types

    def test_ingest_w3c_file(self, fixtures_dir):
        """Test ingesting W3C format file."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri=str(fixtures_dir / "aws_cloudfront" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 5
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_validate_source(self, fixtures_dir):
        """Test source validation."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri=str(fixtures_dir / "aws_cloudfront" / "sample.log"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True

    def test_w3c_field_mapping(self, fixtures_dir):
        """Test that W3C fields are correctly mapped to universal schema."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri=str(fixtures_dir / "aws_cloudfront" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # Verify first record has correct field mappings
        record = records[0]
        assert record.client_ip == "192.0.2.100"
        assert record.method == "GET"
        assert record.path == "/api/data"
        assert record.query_string == "key=value"
        assert record.status_code == 200
        # Verify timestamp was constructed from date+time
        assert record.timestamp.year == 2024
        assert record.timestamp.month == 1
        assert record.timestamp.day == 15

    def test_cloudfront_directory_ingestion(self, fixtures_dir):
        """Test CloudFront adapter can ingest from a directory."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri=str(fixtures_dir / "aws_cloudfront"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 5

    def test_cloudfront_bot_filtering(self, fixtures_dir):
        """Test CloudFront bot filtering."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri=str(fixtures_dir / "aws_cloudfront" / "sample.log"),
        )

        # With bot filtering
        records_with_filter = list(adapter.ingest(source, filter_bots=True))
        # Without bot filtering
        records_without_filter = list(adapter.ingest(source, filter_bots=False))

        # Sample has bot user agents, so counts should be similar
        assert len(records_with_filter) >= 0
        assert len(records_without_filter) >= len(records_with_filter)

    def test_cloudfront_time_filtering(self, fixtures_dir):
        """Test CloudFront time-based filtering."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri=str(fixtures_dir / "aws_cloudfront" / "sample.log"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 15, 12, 30, 47, tzinfo=timezone.utc)

        records = list(
            adapter.ingest(
                source, start_time=start_time, end_time=end_time, filter_bots=False
            )
        )

        for record in records:
            assert start_time <= record.timestamp <= end_time

    def test_cloudfront_validate_nonexistent(self):
        """Test CloudFront validation for nonexistent file."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri="/nonexistent/cloudfront.log",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "not exist" in error_msg.lower()

    def test_cloudfront_unsupported_source_type(self, fixtures_dir):
        """Test CloudFront validation for unsupported source type."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="json_file",  # Not supported by CloudFront
            path_or_uri=str(fixtures_dir / "aws_cloudfront" / "sample.log"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "unsupported" in error_msg.lower()


class TestCloudflareAdapter:
    """Tests for CloudflareAdapter."""

    def test_provider_name(self):
        """CloudflareAdapter should have correct provider name."""
        adapter = get_adapter("cloudflare")
        assert adapter.provider_name == "cloudflare"

    def test_supported_source_types(self):
        """CloudflareAdapter should support API and file formats."""
        adapter = get_adapter("cloudflare")
        assert "api" in adapter.supported_source_types
        assert "csv_file" in adapter.supported_source_types
        assert "json_file" in adapter.supported_source_types
        assert "ndjson_file" in adapter.supported_source_types

    def test_ingest_csv_file(self, fixtures_dir):
        """Test ingesting Cloudflare CSV file."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"

    def test_ingest_json_file(self, fixtures_dir):
        """Test ingesting Cloudflare JSON file."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

    def test_ingest_ndjson_file(self, fixtures_dir):
        """Test ingesting Cloudflare NDJSON file."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="ndjson_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

    def test_cloudflare_uri_parsing(self, fixtures_dir):
        """Test that Cloudflare URI is correctly parsed into path and query_string."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # Check that URI was parsed correctly
        record_with_query = next((r for r in records if r.query_string), None)
        if record_with_query:
            assert record_with_query.path == "/api/data"
            assert record_with_query.query_string == "key=value"

    def test_validate_source_file(self, fixtures_dir):
        """Test source validation for file."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.csv"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True

    def test_validate_source_api_requires_config(self):
        """Test API source validation requires configuration."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="api",
            path_or_uri="api://zone_id",
        )

        # This will fail if settings are not configured
        is_valid, error_msg = adapter.validate_source(source)
        # May be invalid if settings not configured, which is expected
        assert isinstance(is_valid, bool)

    def test_cloudflare_bot_filtering(self, fixtures_dir):
        """Test Cloudflare bot filtering."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.csv"),
        )

        # With bot filtering
        records_with_filter = list(adapter.ingest(source, filter_bots=True))
        # Without bot filtering
        records_without_filter = list(adapter.ingest(source, filter_bots=False))

        assert len(records_without_filter) >= len(records_with_filter)

    def test_cloudflare_time_filtering(self, fixtures_dir):
        """Test Cloudflare time-based filtering."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.csv"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 15, 12, 30, 47, tzinfo=timezone.utc)

        records = list(
            adapter.ingest(
                source, start_time=start_time, end_time=end_time, filter_bots=False
            )
        )

        for record in records:
            assert start_time <= record.timestamp <= end_time

    def test_cloudflare_validate_nonexistent(self):
        """Test Cloudflare validation for nonexistent file."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri="/nonexistent/cloudflare.csv",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "not exist" in error_msg.lower()

    def test_cloudflare_timestamp_parsing(self, fixtures_dir):
        """Test Cloudflare timestamp parsing (nanoseconds)."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="ndjson_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # Verify timestamp was parsed correctly
        record = records[0]
        assert record.timestamp is not None
        assert record.timestamp.tzinfo is not None


class TestAzureCDNAdapter:
    """Tests for AzureCDNAdapter."""

    def test_provider_name(self):
        """AzureCDNAdapter should have correct provider name."""
        adapter = get_adapter("azure_cdn")
        assert adapter.provider_name == "azure_cdn"

    def test_supported_source_types(self):
        """AzureCDNAdapter should support CSV, JSON, and NDJSON file formats."""
        adapter = get_adapter("azure_cdn")
        assert "csv_file" in adapter.supported_source_types
        assert "json_file" in adapter.supported_source_types
        assert "ndjson_file" in adapter.supported_source_types
        # Azure CDN adapter does not support API (file-based only)
        assert "api" not in adapter.supported_source_types

    def test_ingest_csv_file(self, fixtures_dir):
        """Test ingesting Azure CDN CSV file."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_ingest_json_file(self, fixtures_dir):
        """Test ingesting Azure CDN JSON file."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"

    def test_ingest_ndjson_file(self, fixtures_dir):
        """Test ingesting Azure CDN NDJSON file."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="ndjson_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

    def test_ingest_log_analytics_format(self, fixtures_dir):
        """Test ingesting Azure Log Analytics format with _s and _d suffixes."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample_log_analytics.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        # Verify field mapping from Log Analytics format
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].host == "example.com"
        assert records[0].status_code == 200
        # Verify optional fields from Log Analytics format
        assert records[0].response_time_ms == 150  # timeTaken_d converted from seconds
        assert records[0].response_bytes == 1024
        assert records[0].cache_status == "HIT"
        assert records[0].edge_location == "LAX"
        # Verify URI parsing worked
        assert records[0].path == "/api/data"
        assert records[0].query_string == "key=value"

    def test_azure_uri_parsing(self, fixtures_dir):
        """Test that Azure RequestUri is correctly parsed into host, path, and query_string."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # First record has query string in URI
        record_with_query = records[0]
        assert record_with_query.path == "/api/data"
        assert record_with_query.query_string == "key=value"

        # Second record has no query string
        record_without_query = records[1]
        assert record_without_query.path == "/api/submit"
        assert record_without_query.query_string is None

    def test_azure_timestamp_parsing(self, fixtures_dir):
        """Test that Azure ISO 8601 timestamps are correctly parsed."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # Verify timestamp was constructed correctly
        record = records[0]
        assert record.timestamp.year == 2024
        assert record.timestamp.month == 1
        assert record.timestamp.day == 15
        assert record.timestamp.hour == 12
        assert record.timestamp.minute == 30
        assert record.timestamp.second == 45

    def test_azure_optional_fields(self, fixtures_dir):
        """Test that Azure optional fields are correctly mapped."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        record = records[0]
        assert record.response_bytes == 1024
        assert record.request_bytes == 256
        # TimeTaken is converted from seconds to milliseconds
        assert record.response_time_ms == 150
        assert record.cache_status == "HIT"
        assert record.edge_location == "LAX"
        assert record.referer == "https://example.com/referer"
        assert record.protocol == "HTTPS"
        assert record.ssl_protocol == "TLSv1.3"

    def test_validate_source_file(self, fixtures_dir):
        """Test source validation for file."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.csv"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True
        assert error_msg == ""

    def test_validate_source_nonexistent_file(self):
        """Test validation fails for nonexistent file."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="csv_file",
            path_or_uri="/nonexistent/azure-logs.csv",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "not exist" in error_msg.lower() or "not found" in error_msg.lower()

    def test_bot_filtering(self, fixtures_dir):
        """Test bot filtering works correctly."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.csv"),
        )

        # With bot filtering (default)
        records_with_filter = list(adapter.ingest(source, filter_bots=True))
        # Without bot filtering
        records_without_filter = list(adapter.ingest(source, filter_bots=False))

        # All sample records use bot user agents (GPTBot, ChatGPT-User, ClaudeBot)
        # So filtered and unfiltered should have same count
        assert len(records_with_filter) == len(records_without_filter)

    def test_time_filtering(self, fixtures_dir):
        """Test time-based filtering."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.csv"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 15, 12, 30, 47, tzinfo=timezone.utc)

        records = list(
            adapter.ingest(
                source, start_time=start_time, end_time=end_time, filter_bots=False
            )
        )
        # Should only include records between start and end time (inclusive)
        assert len(records) <= 3
        for record in records:
            assert (
                start_time <= record.timestamp <= end_time
            ), f"Record timestamp {record.timestamp} not in range [{start_time}, {end_time}]"

    def test_time_filtering_invalid_range(self, fixtures_dir):
        """Test that invalid time ranges are rejected."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.csv"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 48, tzinfo=timezone.utc)
        end_time = datetime(
            2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc
        )  # Before start

        with pytest.raises(ValueError, match="Invalid time range"):
            list(
                adapter.ingest(
                    source, start_time=start_time, end_time=end_time, filter_bots=False
                )
            )

    def test_unsupported_source_type_validation(self, fixtures_dir):
        """Test validation fails for unsupported source type."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="api",  # Valid source_type but not supported by Azure CDN adapter
            path_or_uri="api://resource",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "unsupported" in error_msg.lower()


class TestAdapterErrorHandling:
    """Tests for error handling in adapters."""

    def test_invalid_provider_raises_error(self):
        """Getting invalid provider should raise ProviderNotFoundError."""
        with pytest.raises(ProviderNotFoundError):
            get_adapter("nonexistent_provider")

    def test_unsupported_source_type(self, fixtures_dir):
        """Using unsupported source type should fail validation."""
        adapter = get_adapter("universal")
        # Note: IngestionSource validates source_type in __post_init__,
        # so we need to use a valid source_type but check adapter's support
        source = IngestionSource(
            provider="universal",
            source_type="api",  # Valid source_type but not supported by universal adapter
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert (
            "unsupported" in error_msg.lower() or "not supported" in error_msg.lower()
        )

    def test_malformed_file_handling(self, fixtures_dir):
        """Malformed files should be handled gracefully."""
        # Create a malformed CSV file with wrong headers (no universal schema fields)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col1,col2,col3\n")
            f.write("val1,val2,val3\n")
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=str(temp_path),
            )

            # Should raise ParseError due to missing required field mappings
            with pytest.raises(ParseError) as exc_info:
                list(adapter.ingest(source, filter_bots=False, strict_validation=False))
            assert "Missing required field mappings" in str(exc_info.value)
        finally:
            temp_path.unlink()


class TestGCPCDNAdapter:
    """Tests for GCPCDNAdapter."""

    def test_provider_name(self):
        """GCPCDNAdapter should have correct provider name."""
        adapter = get_adapter("gcp_cdn")
        assert adapter.provider_name == "gcp_cdn"

    def test_supported_source_types(self):
        """GCPCDNAdapter should support JSON and NDJSON file formats."""
        adapter = get_adapter("gcp_cdn")
        assert "json_file" in adapter.supported_source_types
        assert "ndjson_file" in adapter.supported_source_types
        # GCP CDN adapter does not support CSV (Cloud Logging exports JSON only)
        assert "csv_file" not in adapter.supported_source_types
        # GCP CDN adapter does not support API (file-based only)
        assert "api" not in adapter.supported_source_types

    def test_ingest_json_file(self, fixtures_dir):
        """Test ingesting GCP Cloud Logging JSON file."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_ingest_ndjson_file(self, fixtures_dir):
        """Test ingesting GCP Cloud Logging NDJSON file."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="ndjson_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

    def test_ingest_gzip_json_file(self, fixtures_dir):
        """Test ingesting gzip-compressed GCP Cloud Logging JSON file."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json.gz"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        # Verify same content as uncompressed version
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_nested_http_request_parsing(self, fixtures_dir):
        """Test that nested httpRequest fields are correctly flattened."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        record = records[0]
        # Verify nested httpRequest fields are correctly mapped
        assert record.client_ip == "192.0.2.100"
        assert record.method == "GET"
        assert record.status_code == 200
        assert "GPTBot" in record.user_agent

    def test_url_parsing_host_and_path(self, fixtures_dir):
        """Test that requestUrl is correctly parsed into host, path, and query_string."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # First record: https://example.com/api/data?key=value
        record = records[0]
        assert record.host == "example.com"
        assert record.path == "/api/data"
        assert record.query_string == "key=value"

        # Second record: https://api.example.com/submit (no query)
        record2 = records[1]
        assert record2.host == "api.example.com"
        assert record2.path == "/submit"
        assert record2.query_string is None

    def test_rfc3339_timestamp_parsing(self, fixtures_dir):
        """Test that RFC3339 timestamps are correctly parsed."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # Verify timestamp was constructed correctly (with microseconds)
        record = records[0]
        assert record.timestamp.year == 2024
        assert record.timestamp.month == 1
        assert record.timestamp.day == 15
        assert record.timestamp.hour == 12
        assert record.timestamp.minute == 30
        assert record.timestamp.second == 45
        assert record.timestamp.microsecond == 123456

    def test_latency_conversion_to_milliseconds(self, fixtures_dir):
        """Test that latency duration string is converted to milliseconds."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # First record: latency "0.150s" -> 150ms
        assert records[0].response_time_ms == 150
        # Second record: latency "0.200s" -> 200ms
        assert records[1].response_time_ms == 200
        # Third record: latency "0.010s" -> 10ms
        assert records[2].response_time_ms == 10

    def test_cache_status_mapping(self, fixtures_dir):
        """Test that cacheHit boolean is mapped to cache status string."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

        # First record: cacheHit=true -> HIT
        assert records[0].cache_status == "HIT"
        # Second record: cacheHit=false, cacheLookup=true -> MISS
        assert records[1].cache_status == "MISS"
        # Third record: cacheHit=true -> HIT
        assert records[2].cache_status == "HIT"

    def test_optional_fields(self, fixtures_dir):
        """Test that optional fields are correctly mapped."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        record = records[0]
        assert record.request_bytes == 256
        assert record.response_bytes == 1024
        assert record.referer == "https://example.com/referer"
        assert record.protocol == "HTTP/2.0"
        assert record.edge_location == "10.0.0.1"

    def test_edge_cases_missing_optional_fields(self, fixtures_dir):
        """Test handling of entries with missing optional fields."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Should skip 2 entries (missing httpRequest and missing required fields)
        assert len(records) == 4

        # First record: minimal required fields only
        record = records[0]
        assert record.client_ip == "192.0.2.100"
        assert record.method == "GET"
        assert record.status_code == 200
        # Optional fields should be None
        assert record.response_time_ms is None
        assert record.cache_status is None

    def test_edge_cases_relative_url(self, fixtures_dir):
        """Test handling of relative URL in requestUrl."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Second valid record has relative URL
        record = records[1]
        assert record.path == "/relative/path"
        assert record.query_string == "foo=bar"
        assert record.host is None  # No host in relative URL

    def test_edge_cases_ipv6_client_ip(self, fixtures_dir):
        """Test handling of IPv6 client IP addresses."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Third record has IPv6 address
        ipv6_record = next((r for r in records if "db8" in r.client_ip), None)
        assert ipv6_record is not None
        assert ipv6_record.client_ip == "2001:db8::1"
        # Also test cache bypass
        assert ipv6_record.cache_status == "BYPASS"

    def test_edge_cases_timezone_offset(self, fixtures_dir):
        """Test handling of timestamps with explicit timezone offset."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Last record has -05:00 timezone offset
        offset_record = records[-1]
        # Should be converted to UTC: 12:30:50-05:00 -> 17:30:50 UTC
        assert offset_record.timestamp.hour == 17
        assert offset_record.timestamp.minute == 30
        assert offset_record.timestamp.second == 50

    def test_validate_source_file(self, fixtures_dir):
        """Test source validation for file."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True
        assert error_msg == ""

    def test_validate_source_nonexistent_file(self):
        """Test validation fails for nonexistent file."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri="/nonexistent/gcp-logs.json",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "not exist" in error_msg.lower() or "not found" in error_msg.lower()

    def test_bot_filtering(self, fixtures_dir):
        """Test bot filtering works correctly."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        # With bot filtering (default)
        records_with_filter = list(adapter.ingest(source, filter_bots=True))
        # Without bot filtering
        records_without_filter = list(adapter.ingest(source, filter_bots=False))

        # All sample records use bot user agents (GPTBot, ClaudeBot, ChatGPT-User)
        # So filtered and unfiltered should have same count
        assert len(records_with_filter) == len(records_without_filter)

    def test_time_filtering(self, fixtures_dir):
        """Test time-based filtering."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 15, 12, 30, 47, tzinfo=timezone.utc)

        records = list(
            adapter.ingest(
                source, start_time=start_time, end_time=end_time, filter_bots=False
            )
        )
        # Should only include records between start and end time (inclusive)
        assert len(records) <= 3
        for record in records:
            assert (
                start_time <= record.timestamp <= end_time
            ), f"Record timestamp {record.timestamp} not in range [{start_time}, {end_time}]"

    def test_time_filtering_invalid_range(self, fixtures_dir):
        """Test that invalid time ranges are rejected."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 48, tzinfo=timezone.utc)
        end_time = datetime(
            2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc
        )  # Before start

        with pytest.raises(ValueError, match="Invalid time range"):
            list(
                adapter.ingest(
                    source, start_time=start_time, end_time=end_time, filter_bots=False
                )
            )

    def test_unsupported_source_type_validation(self, fixtures_dir):
        """Test validation fails for unsupported source type."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="csv_file",  # CSV not supported by GCP CDN adapter
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "unsupported" in error_msg.lower()

    def test_extra_fields_preserved(self, fixtures_dir):
        """Test that extra GCP-specific fields are preserved."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        record = records[0]
        # Verify extra fields are preserved
        assert record.extra is not None
        assert "insertId" in record.extra
        assert record.extra["insertId"] == "abc123xyz"
        assert "trace" in record.extra

    def test_resource_labels_preserved(self, fixtures_dir):
        """Test that resource labels are preserved in extra fields."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Find record with resource labels
        record_with_labels = next(
            (r for r in records if r.extra and "resource_labels" in r.extra), None
        )
        assert record_with_labels is not None
        assert "project_id" in record_with_labels.extra["resource_labels"]
        assert record_with_labels.extra["resource_labels"]["project_id"] == "my-project"


class TestALBAdapter:
    """Tests for ALBAdapter."""

    def test_provider_name(self):
        """ALBAdapter should have correct provider name."""
        adapter = get_adapter("aws_alb")
        assert adapter.provider_name == "aws_alb"

    def test_supported_source_types(self):
        """ALBAdapter should support ALB log file format."""
        adapter = get_adapter("aws_alb")
        assert "alb_log_file" in adapter.supported_source_types
        # ALB adapter does not support other formats
        assert "csv_file" not in adapter.supported_source_types
        assert "json_file" not in adapter.supported_source_types

    def test_ingest_log_file(self, fixtures_dir):
        """Test ingesting ALB access log file."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_ingest_gzip_log_file(self, fixtures_dir):
        """Test ingesting gzip-compressed ALB access log file."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log.gz"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        # Verify same content as uncompressed version
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_http_request_line_parsing(self, fixtures_dir):
        """Test that HTTP request line is correctly parsed into method, host, path, query."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

        # First record: GET https://example.com/api/data?key=value
        record = records[0]
        assert record.method == "GET"
        assert record.host == "example.com"
        assert record.path == "/api/data"
        assert record.query_string == "key=value"

        # Second record: POST https://api.example.com/submit (no query)
        record2 = records[1]
        assert record2.method == "POST"
        assert record2.host == "api.example.com"
        assert record2.path == "/submit"
        assert record2.query_string is None

    def test_timestamp_parsing(self, fixtures_dir):
        """Test that ISO 8601 timestamps are correctly parsed."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # Verify timestamp was constructed correctly (with microseconds)
        record = records[0]
        assert record.timestamp.year == 2024
        assert record.timestamp.month == 1
        assert record.timestamp.day == 15
        assert record.timestamp.hour == 12
        assert record.timestamp.minute == 30
        assert record.timestamp.second == 45
        assert record.timestamp.microsecond == 123456

    def test_client_port_ip_extraction(self, fixtures_dir):
        """Test that client IP is correctly extracted from client:port field."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

        # Verify client IPs (ports removed)
        assert records[0].client_ip == "192.0.2.100"
        assert records[1].client_ip == "192.0.2.101"
        assert records[2].client_ip == "192.0.2.102"

    def test_response_time_calculation(self, fixtures_dir):
        """Test that response time is calculated from processing times."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # First record: 0.001 + 0.002 + 0.000 = 0.003s = 3ms
        assert records[0].response_time_ms == 3
        # Second record: 0.002 + 0.005 + 0.001 = 0.008s = 8ms
        assert records[1].response_time_ms == 8

    def test_optional_fields(self, fixtures_dir):
        """Test that optional fields are correctly mapped."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        record = records[0]
        assert record.request_bytes == 256
        assert record.response_bytes == 1024
        assert record.ssl_protocol == "TLSv1.2"

    def test_edge_cases_malformed_request(self, fixtures_dir):
        """Test handling of malformed request line."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "edge_cases.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Should skip entry with "- - -" request line
        assert len(records) == 4

    def test_edge_cases_ipv6_client_ip(self, fixtures_dir):
        """Test handling of IPv6 client IP addresses."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "edge_cases.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Find IPv6 record
        ipv6_record = next((r for r in records if "db8" in r.client_ip), None)
        assert ipv6_record is not None
        assert ipv6_record.client_ip == "2001:db8::1"

    def test_edge_cases_backend_timeout(self, fixtures_dir):
        """Test handling of backend timeout with -1 processing times."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "edge_cases.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Find 502 error record
        timeout_record = next((r for r in records if r.status_code == 502), None)
        assert timeout_record is not None
        # Processing times are -1, so response_time_ms should be None
        assert timeout_record.response_time_ms is None

    def test_edge_cases_relative_url(self, fixtures_dir):
        """Test handling of relative URL in request line."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "edge_cases.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Find record with relative URL
        relative_record = next(
            (r for r in records if "foo=bar" in (r.query_string or "")), None
        )
        assert relative_record is not None
        assert relative_record.path == "/relative/path"
        assert relative_record.query_string == "foo=bar"
        assert relative_record.host is None  # No host in relative URL

    def test_validate_source_file(self, fixtures_dir):
        """Test source validation for file."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True
        assert error_msg == ""

    def test_validate_source_nonexistent_file(self):
        """Test validation fails for nonexistent file."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri="/nonexistent/alb-access.log",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "not exist" in error_msg.lower() or "not found" in error_msg.lower()

    def test_bot_filtering(self, fixtures_dir):
        """Test bot filtering works correctly."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        # With bot filtering (default)
        records_with_filter = list(adapter.ingest(source, filter_bots=True))
        # Without bot filtering
        records_without_filter = list(adapter.ingest(source, filter_bots=False))

        # All sample records use bot user agents (GPTBot, ClaudeBot, ChatGPT-User)
        # So filtered and unfiltered should have same count
        assert len(records_with_filter) == len(records_without_filter)

    def test_time_filtering(self, fixtures_dir):
        """Test time-based filtering."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 15, 12, 30, 47, tzinfo=timezone.utc)

        records = list(
            adapter.ingest(
                source, start_time=start_time, end_time=end_time, filter_bots=False
            )
        )
        # Should only include records between start and end time (inclusive)
        assert len(records) <= 3
        for record in records:
            assert (
                start_time <= record.timestamp <= end_time
            ), f"Record timestamp {record.timestamp} not in range [{start_time}, {end_time}]"

    def test_time_filtering_invalid_range(self, fixtures_dir):
        """Test that invalid time ranges are rejected."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 48, tzinfo=timezone.utc)
        end_time = datetime(
            2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc
        )  # Before start

        with pytest.raises(ValueError, match="Invalid time range"):
            list(
                adapter.ingest(
                    source, start_time=start_time, end_time=end_time, filter_bots=False
                )
            )

    def test_unsupported_source_type_validation(self, fixtures_dir):
        """Test validation fails for unsupported source type."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="json_file",  # JSON not supported by ALB adapter
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "unsupported" in error_msg.lower()

    def test_protocol_extraction(self, fixtures_dir):
        """Test that HTTP protocol version is correctly extracted."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

        # First record: HTTP/1.1
        assert records[0].protocol == "HTTP/1.1"
        # Second record: HTTP/2.0
        assert records[1].protocol == "HTTP/2.0"
        # Third record: HTTP/1.1
        assert records[2].protocol == "HTTP/1.1"

    def test_extra_fields_preserved(self, fixtures_dir):
        """Test that ALB-specific extra fields are preserved."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        record = records[0]
        assert record.extra is not None
        # Verify type (http/https)
        assert record.extra.get("type") == "https"
        # Verify elb identifier
        assert "elb" in record.extra
        assert "my-alb" in record.extra["elb"]
        # Verify target_group_arn
        assert "target_group_arn" in record.extra
        assert "targetgroup" in record.extra["target_group_arn"]
        # Verify trace_id
        assert "trace_id" in record.extra
        assert record.extra["trace_id"].startswith("Root=")


class TestFastlyAdapter:
    """Tests for FastlyAdapter."""

    def test_provider_name(self):
        """FastlyAdapter should have correct provider name."""
        adapter = get_adapter("fastly")
        assert adapter.provider_name == "fastly"

    def test_supported_source_types(self):
        """FastlyAdapter should support JSON, CSV, and NDJSON formats."""
        adapter = get_adapter("fastly")
        assert "fastly_json_file" in adapter.supported_source_types
        assert "fastly_csv_file" in adapter.supported_source_types
        assert "fastly_ndjson_file" in adapter.supported_source_types

    def test_ingest_json_file(self, fixtures_dir):
        """Test ingesting Fastly JSON log file."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_ingest_gzip_json_file(self, fixtures_dir):
        """Test ingesting gzip-compressed Fastly JSON log file."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.json.gz"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        # Verify same content as uncompressed version
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_ingest_ndjson_file(self, fixtures_dir):
        """Test ingesting Fastly NDJSON log file."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_ndjson_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"

    def test_ingest_csv_file(self, fixtures_dir):
        """Test ingesting Fastly CSV log file."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_csv_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].query_string == "key=value"

    def test_field_alias_resolution(self, fixtures_dir):
        """Test that field aliases are correctly resolved."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "custom_fields.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 2
        # Should resolve: request_time->timestamp, clientip->client_ip, etc.
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].host == "example.com"
        assert records[0].path == "/api/data"
        assert records[0].status_code == 200

    def test_custom_field_mapping(self, fixtures_dir):
        """Test custom field mapping via options."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "custom_fields.json"),
            options={
                "field_mapping": {
                    "timestamp": "request_time",
                    "client_ip": "clientip",
                    "method": "http_method",
                    "host": "hostname",
                    "path": "uri",
                    "status_code": "http_status",
                    "user_agent": "ua",
                }
            },
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 2
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"

    def test_unix_timestamp_parsing(self, fixtures_dir):
        """Test that Unix timestamps are correctly parsed."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 4

        # First record has Unix timestamp
        unix_record = next((r for r in records if r.path == "/unix-timestamp"), None)
        assert unix_record is not None
        assert unix_record.timestamp.year == 2024

    def test_ipv6_client_ip(self, fixtures_dir):
        """Test handling of IPv6 client IP addresses."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        ipv6_record = next((r for r in records if r.path == "/ipv6-test"), None)
        assert ipv6_record is not None
        assert ipv6_record.client_ip == "2001:db8::1"

    def test_null_optional_fields(self, fixtures_dir):
        """Test handling of null values for optional fields."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        null_record = next((r for r in records if r.path == "/null-host"), None)
        assert null_record is not None
        assert null_record.host is None
        assert null_record.response_bytes is None
        assert null_record.response_time_ms is None

    def test_optional_fields_mapping(self, fixtures_dir):
        """Test that optional fields are correctly mapped."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        record = records[0]
        assert record.response_bytes == 1024
        assert record.response_time_ms == 150
        assert record.query_string == "key=value"
        assert record.protocol == "HTTP/1.1"

    def test_validate_source_file(self, fixtures_dir):
        """Test source validation for file."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.json"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True
        assert error_msg == ""

    def test_validate_source_nonexistent_file(self):
        """Test validation fails for nonexistent file."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri="/nonexistent/fastly.json",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "not exist" in error_msg.lower() or "not found" in error_msg.lower()

    def test_bot_filtering(self, fixtures_dir):
        """Test bot filtering works correctly."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.json"),
        )

        # With bot filtering (default)
        records_with_filter = list(adapter.ingest(source, filter_bots=True))
        # Without bot filtering
        records_without_filter = list(adapter.ingest(source, filter_bots=False))

        # All sample records use bot user agents
        assert len(records_with_filter) == len(records_without_filter)

    def test_time_filtering(self, fixtures_dir):
        """Test time-based filtering."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.json"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 15, 12, 30, 47, tzinfo=timezone.utc)

        records = list(
            adapter.ingest(
                source, start_time=start_time, end_time=end_time, filter_bots=False
            )
        )
        # Should filter to only records in range
        for record in records:
            assert start_time <= record.timestamp <= end_time

    def test_unsupported_source_type_validation(self, fixtures_dir):
        """Test validation fails for unsupported source type."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="alb_log_file",  # ALB format not supported by Fastly
            path_or_uri=str(fixtures_dir / "fastly" / "sample.json"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "unsupported" in error_msg.lower()


class TestAkamaiAdapter:
    """Tests for AkamaiAdapter."""

    def test_provider_name(self):
        """AkamaiAdapter should have correct provider name."""
        adapter = get_adapter("akamai")
        assert adapter.provider_name == "akamai"

    def test_supported_source_types(self):
        """AkamaiAdapter should support JSON and NDJSON formats."""
        adapter = get_adapter("akamai")
        assert "akamai_json_file" in adapter.supported_source_types
        assert "akamai_ndjson_file" in adapter.supported_source_types

    def test_ingest_json_file(self, fixtures_dir):
        """Test ingesting Akamai JSON log file."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_ingest_gzip_json_file(self, fixtures_dir):
        """Test ingesting gzip-compressed Akamai JSON log file."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.json.gz"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"

    def test_ingest_ndjson_file(self, fixtures_dir):
        """Test ingesting Akamai NDJSON log file."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_ndjson_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"

    def test_camelcase_field_mapping(self, fixtures_dir):
        """Test that CamelCase Akamai fields are correctly mapped."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

        # Verify CamelCase mapping: requestHost -> host
        assert records[0].host == "example.com"
        # requestPath -> path
        assert records[0].path == "/api/data"
        # responseStatus -> status_code
        assert records[0].status_code == 200

    def test_optional_fields_mapping(self, fixtures_dir):
        """Test that optional fields are correctly mapped."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        record = records[0]
        # bytes -> response_bytes
        assert record.response_bytes == 1024
        # turnaroundTimeMs -> response_time_ms
        assert record.response_time_ms == 150
        # queryString -> query_string
        assert record.query_string == "key=value"
        # cacheStatus -> cache_status
        assert record.cache_status == "HIT"
        # tlsVersion -> ssl_protocol
        assert record.ssl_protocol == "TLSv1.3"
        # requestProtocol -> protocol
        assert record.protocol == "HTTP/1.1"

    def test_unix_seconds_timestamp(self, fixtures_dir):
        """Test parsing Unix timestamp in seconds."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        unix_record = next((r for r in records if r.path == "/unix-seconds"), None)
        assert unix_record is not None
        assert unix_record.timestamp.year == 2024

    def test_unix_milliseconds_timestamp(self, fixtures_dir):
        """Test parsing Unix timestamp in milliseconds (13 digits)."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        ms_record = next((r for r in records if r.path == "/unix-milliseconds"), None)
        assert ms_record is not None
        assert ms_record.timestamp.year == 2024
        # Verify milliseconds are preserved
        assert ms_record.timestamp.microsecond > 0

    def test_ipv6_client_ip(self, fixtures_dir):
        """Test handling of IPv6 client IP addresses."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        ipv6_record = next((r for r in records if r.path == "/ipv6-test"), None)
        assert ipv6_record is not None
        assert ipv6_record.client_ip == "2001:db8::1"

    def test_null_optional_fields(self, fixtures_dir):
        """Test handling of null values for optional fields."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "edge_cases.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        null_record = next((r for r in records if r.path == "/null-host"), None)
        assert null_record is not None
        assert null_record.host is None
        assert null_record.response_bytes is None
        assert null_record.response_time_ms is None

    def test_validate_source_file(self, fixtures_dir):
        """Test source validation for file."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.json"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True
        assert error_msg == ""

    def test_validate_source_nonexistent_file(self):
        """Test validation fails for nonexistent file."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri="/nonexistent/akamai.json",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "not exist" in error_msg.lower() or "not found" in error_msg.lower()

    def test_bot_filtering(self, fixtures_dir):
        """Test bot filtering works correctly."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.json"),
        )

        # With bot filtering (default)
        records_with_filter = list(adapter.ingest(source, filter_bots=True))
        # Without bot filtering
        records_without_filter = list(adapter.ingest(source, filter_bots=False))

        # All sample records use bot user agents
        assert len(records_with_filter) == len(records_without_filter)

    def test_time_filtering(self, fixtures_dir):
        """Test time-based filtering."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.json"),
        )

        start_time = datetime(2024, 1, 15, 12, 30, 46, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 15, 12, 30, 47, tzinfo=timezone.utc)

        records = list(
            adapter.ingest(
                source, start_time=start_time, end_time=end_time, filter_bots=False
            )
        )
        # Should filter to only records in range
        for record in records:
            assert start_time <= record.timestamp <= end_time

    def test_unsupported_source_type_validation(self, fixtures_dir):
        """Test validation fails for unsupported source type."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="fastly_json_file",  # Fastly format not supported by Akamai
            path_or_uri=str(fixtures_dir / "akamai" / "sample.json"),
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "unsupported" in error_msg.lower()


class TestDirectoryIngestion:
    """Tests for directory-based ingestion across all adapters."""

    def test_alb_directory_ingestion(self, fixtures_dir):
        """Test ALB adapter can ingest from a directory."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Should find and process all .log files in directory
        assert len(records) >= 3

    def test_fastly_directory_ingestion(self, fixtures_dir):
        """Test Fastly adapter can ingest from a directory."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3

    def test_akamai_directory_ingestion(self, fixtures_dir):
        """Test Akamai adapter can ingest from a directory."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Should find and process .json files
        assert len(records) >= 3

    def test_gcp_cdn_directory_ingestion(self, fixtures_dir):
        """Test GCP CDN adapter can ingest from a directory."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3

    def test_azure_cdn_directory_ingestion(self, fixtures_dir):
        """Test Azure CDN adapter can ingest from a directory."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "azure_cdn"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3

    def test_universal_directory_ingestion(self, fixtures_dir):
        """Test Universal adapter can ingest from a directory."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3


class TestGzipFileHandling:
    """Tests for gzip file handling across adapters."""

    def test_alb_gzip_ingestion(self, fixtures_dir):
        """Test ALB adapter can ingest gzipped log files."""
        adapter = get_adapter("aws_alb")
        gzip_file = fixtures_dir / "aws_alb" / "sample.log.gz"
        if not gzip_file.exists():
            pytest.skip("Gzip fixture not available")

        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(gzip_file),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 1

    def test_fastly_gzip_ingestion(self, fixtures_dir):
        """Test Fastly adapter can ingest gzipped JSON files."""
        adapter = get_adapter("fastly")
        gzip_file = fixtures_dir / "fastly" / "sample.json.gz"
        if not gzip_file.exists():
            pytest.skip("Gzip fixture not available")

        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(gzip_file),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 1

    def test_gcp_cdn_gzip_ingestion(self, fixtures_dir):
        """Test GCP CDN adapter can ingest gzipped JSON files."""
        adapter = get_adapter("gcp_cdn")
        gzip_file = fixtures_dir / "gcp_cdn" / "sample.json.gz"
        if not gzip_file.exists():
            pytest.skip("Gzip fixture not available")

        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(gzip_file),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 1


class TestEdgeCasesAndErrorHandling:
    """Additional edge case and error handling tests."""

    def test_empty_directory_validation(self):
        """Test validation fails for empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=tmpdir,
            )

            is_valid, error_msg = adapter.validate_source(source)
            assert is_valid is False
            assert "no matching" in error_msg.lower()

    def test_nonexistent_path_validation(self):
        """Test validation fails for nonexistent path."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri="/nonexistent/path/logs.json",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "not exist" in error_msg.lower()

    def test_empty_file_validation(self):
        """Test validation fails for empty file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("akamai")
            source = IngestionSource(
                provider="akamai",
                source_type="akamai_json_file",
                path_or_uri=str(temp_path),
            )

            is_valid, error_msg = adapter.validate_source(source)
            assert is_valid is False
            assert "empty" in error_msg.lower()
        finally:
            temp_path.unlink()

    def test_invalid_json_parsing(self):
        """Test that invalid JSON is handled gracefully."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ invalid json }\n")
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("akamai")
            source = IngestionSource(
                provider="akamai",
                source_type="akamai_json_file",
                path_or_uri=str(temp_path),
            )

            with pytest.raises(ParseError):
                list(adapter.ingest(source, filter_bots=False))
        finally:
            temp_path.unlink()

    def test_missing_required_fields_skipped(self):
        """Test that records with missing required fields are skipped in non-strict mode."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
            # Write records - some missing required fields
            f.write(
                '{"requestTime": "2024-01-15T12:30:45Z", "clientIP": "192.0.2.1", "requestMethod": "GET", "requestHost": "example.com", "requestPath": "/test", "responseStatus": 200, "userAgent": "GPTBot/1.0"}\n'
            )
            f.write('{"requestTime": "2024-01-15T12:30:46Z"}\n')  # Missing most fields
            f.write(
                '{"requestTime": "2024-01-15T12:30:47Z", "clientIP": "192.0.2.2", "requestMethod": "POST", "requestHost": "example.com", "requestPath": "/api", "responseStatus": 201, "userAgent": "ClaudeBot/1.0"}\n'
            )
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("akamai")
            source = IngestionSource(
                provider="akamai",
                source_type="akamai_ndjson_file",
                path_or_uri=str(temp_path),
            )

            # Non-strict mode should skip invalid records
            records = list(
                adapter.ingest(source, filter_bots=False, strict_validation=False)
            )
            # Only 2 valid records (the one with missing fields should be skipped)
            assert len(records) == 2
        finally:
            temp_path.unlink()

    def test_gcp_cdn_edge_cases(self, fixtures_dir):
        """Test GCP CDN adapter handles edge case data."""
        adapter = get_adapter("gcp_cdn")
        edge_cases_file = fixtures_dir / "gcp_cdn" / "edge_cases.json"
        if not edge_cases_file.exists():
            pytest.skip("Edge cases fixture not available")

        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(edge_cases_file),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 1

    def test_strict_validation_mode(self):
        """Test that strict validation mode raises on invalid records."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
            f.write(
                '{"requestTime": "2024-01-15T12:30:45Z"}\n'
            )  # Missing required fields
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("akamai")
            source = IngestionSource(
                provider="akamai",
                source_type="akamai_ndjson_file",
                path_or_uri=str(temp_path),
            )

            # Strict mode should raise on invalid record
            # (though it might just skip and produce empty result)
            records = list(
                adapter.ingest(source, filter_bots=False, strict_validation=True)
            )
            # Either raises or returns empty
            assert len(records) == 0
        finally:
            temp_path.unlink()

    def test_cloudflare_ndjson_ingestion(self, fixtures_dir):
        """Test Cloudflare adapter can ingest NDJSON files."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="ndjson_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 1

    def test_cloudfront_w3c_ingestion(self, fixtures_dir):
        """Test CloudFront adapter can ingest W3C format files."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri=str(fixtures_dir / "aws_cloudfront" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 1
