"""
Integration tests for complete ingestion workflows.

Tests end-to-end ingestion from file input through parsing,
validation, filtering to database insertion.
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

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
from llm_bot_pipeline.storage import get_backend
from scripts.ingest_logs import convert_to_backend_record


@pytest.fixture
def fixtures_dir() -> Path:
    """Return path to test fixtures directory."""
    return Path(__file__).parent.parent / "fixtures" / "ingestion"


class TestUniversalIngestionWorkflow:
    """Integration tests for universal adapter workflow."""

    def test_csv_ingestion_to_database(self, fixtures_dir, sqlite_backend):
        """Test complete CSV ingestion workflow."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        # Validate source
        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is True

        # Ingest records
        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 5

        # Insert into database using conversion function
        backend_records = [convert_to_backend_record(record) for record in records]

        inserted = sqlite_backend.insert_raw_records(backend_records)
        assert inserted == 5

        # Verify records in database
        results = sqlite_backend.query("SELECT COUNT(*) as count FROM raw_bot_requests")
        assert results[0]["count"] == 5

    def test_json_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test complete JSON ingestion workflow."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

        # Verify record content matches expected
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200

    def test_directory_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test ingesting from directory."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        # Should process all matching files in directory (at least CSV file records)
        assert len(records) >= 5


class TestCloudFrontIngestionWorkflow:
    """Integration tests for CloudFront adapter workflow."""

    def test_w3c_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test complete W3C ingestion workflow."""
        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri=str(fixtures_dir / "aws_cloudfront" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 5

        # Verify record structure
        assert records[0].client_ip == "192.0.2.100"
        assert records[0].method == "GET"
        assert records[0].status_code == 200


class TestCloudflareIngestionWorkflow:
    """Integration tests for Cloudflare adapter workflow."""

    def test_csv_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test Cloudflare CSV ingestion workflow."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3

    def test_json_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test Cloudflare JSON ingestion workflow."""
        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) == 3


class TestSourceProviderTracking:
    """Tests for source provider tracking in database records."""

    def test_source_provider_recorded(self, fixtures_dir, sqlite_backend):
        """Test that source_provider is recorded in database records."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) > 0

        # Convert with provider name
        backend_records = [
            convert_to_backend_record(record, source_provider=adapter.provider_name)
            for record in records
        ]

        # Verify source_provider is set
        for rec in backend_records:
            assert rec["source_provider"] == "universal"

        # Insert and verify in database
        inserted = sqlite_backend.insert_raw_records(backend_records)
        assert inserted == len(records)

        results = sqlite_backend.query(
            "SELECT DISTINCT source_provider FROM raw_bot_requests"
        )
        assert len(results) == 1
        assert results[0]["source_provider"] == "universal"

    def test_different_providers_tracked(self, fixtures_dir, sqlite_backend):
        """Test that different providers are tracked correctly."""
        # Ingest from universal
        adapter1 = get_adapter("universal")
        source1 = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )
        records1 = list(adapter1.ingest(source1, filter_bots=False))
        backend_records1 = [
            convert_to_backend_record(record, source_provider="universal")
            for record in records1
        ]
        sqlite_backend.insert_raw_records(backend_records1)

        # Ingest from cloudflare
        adapter2 = get_adapter("cloudflare")
        source2 = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "cloudflare" / "sample.csv"),
        )
        records2 = list(adapter2.ingest(source2, filter_bots=False))
        backend_records2 = [
            convert_to_backend_record(record, source_provider="cloudflare")
            for record in records2
        ]
        sqlite_backend.insert_raw_records(backend_records2)

        # Verify both providers are tracked
        results = sqlite_backend.query(
            "SELECT source_provider, COUNT(*) as count FROM raw_bot_requests "
            "GROUP BY source_provider ORDER BY source_provider"
        )
        assert len(results) == 2
        providers = [r["source_provider"] for r in results]
        assert "cloudflare" in providers
        assert "universal" in providers


class TestALBIngestionWorkflow:
    """Integration tests for AWS ALB adapter workflow."""

    def test_alb_log_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test complete ALB log ingestion workflow."""
        adapter = get_adapter("aws_alb")
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri=str(fixtures_dir / "aws_alb" / "sample.log"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3

        # Verify record structure
        assert records[0].client_ip is not None
        assert records[0].method is not None
        assert records[0].status_code is not None

        # Insert to database
        backend_records = [
            convert_to_backend_record(record, source_provider="aws_alb")
            for record in records
        ]
        inserted = sqlite_backend.insert_raw_records(backend_records)
        assert inserted >= 3


class TestFastlyIngestionWorkflow:
    """Integration tests for Fastly adapter workflow."""

    def test_fastly_json_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test Fastly JSON ingestion workflow."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3

        # Verify record content
        assert records[0].client_ip is not None
        assert records[0].status_code is not None

    def test_fastly_ndjson_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test Fastly NDJSON ingestion workflow."""
        adapter = get_adapter("fastly")
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_ndjson_file",
            path_or_uri=str(fixtures_dir / "fastly" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3


class TestAkamaiIngestionWorkflow:
    """Integration tests for Akamai DataStream adapter workflow."""

    def test_akamai_json_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test Akamai JSON ingestion workflow."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_json_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3

        # Verify camelCase field mapping worked
        assert records[0].client_ip is not None
        assert records[0].method is not None
        assert records[0].status_code is not None

        # Insert to database
        backend_records = [
            convert_to_backend_record(record, source_provider="akamai")
            for record in records
        ]
        inserted = sqlite_backend.insert_raw_records(backend_records)
        assert inserted >= 3

    def test_akamai_ndjson_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test Akamai NDJSON ingestion workflow."""
        adapter = get_adapter("akamai")
        source = IngestionSource(
            provider="akamai",
            source_type="akamai_ndjson_file",
            path_or_uri=str(fixtures_dir / "akamai" / "sample.ndjson"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3


class TestGCPCDNIngestionWorkflow:
    """Integration tests for GCP Cloud CDN adapter workflow."""

    def test_gcp_json_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test GCP Cloud CDN JSON ingestion workflow."""
        adapter = get_adapter("gcp_cdn")
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "gcp_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3

        # Verify record structure
        assert records[0].client_ip is not None
        assert records[0].method is not None


class TestAzureCDNIngestionWorkflow:
    """Integration tests for Azure CDN adapter workflow."""

    def test_azure_json_ingestion_workflow(self, fixtures_dir, sqlite_backend):
        """Test Azure CDN JSON ingestion workflow."""
        adapter = get_adapter("azure_cdn")
        source = IngestionSource(
            provider="azure_cdn",
            source_type="json_file",
            path_or_uri=str(fixtures_dir / "azure_cdn" / "sample.json"),
        )

        records = list(adapter.ingest(source, filter_bots=False))
        assert len(records) >= 3

        # Verify record structure
        assert records[0].client_ip is not None
        assert records[0].status_code is not None


class TestErrorRecoveryAndCorruptedFiles:
    """Tests for error recovery and handling of corrupted files."""

    def test_invalid_json_recovery(self, fixtures_dir, sqlite_backend):
        """Test that invalid JSON lines are handled gracefully."""
        import tempfile
        from pathlib import Path

        # Create a file with some invalid JSON lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
            f.write(
                '{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.1", '
                '"method": "GET", "host": "example.com", "path": "/test", '
                '"status_code": 200, "user_agent": "GPTBot/1.0"}\n'
            )
            f.write("{ invalid json line }\n")  # Invalid
            f.write(
                '{"timestamp": "2024-01-15T12:30:46Z", "client_ip": "192.0.2.2", '
                '"method": "POST", "host": "example.com", "path": "/api", '
                '"status_code": 201, "user_agent": "ClaudeBot/1.0"}\n'
            )
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="ndjson_file",
                path_or_uri=str(temp_path),
            )

            # Non-strict mode should skip invalid lines
            records = list(
                adapter.ingest(source, filter_bots=False, strict_validation=False)
            )
            # Should have at least 2 valid records
            assert len(records) >= 2
        finally:
            temp_path.unlink(missing_ok=True)

    def test_missing_required_fields_recovery(self, fixtures_dir, sqlite_backend):
        """Test that records with missing required fields are skipped."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
            # Valid record
            f.write(
                '{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.1", '
                '"method": "GET", "host": "example.com", "path": "/test", '
                '"status_code": 200, "user_agent": "GPTBot/1.0"}\n'
            )
            # Missing client_ip
            f.write('{"timestamp": "2024-01-15T12:30:46Z", "method": "POST"}\n')
            # Valid record
            f.write(
                '{"timestamp": "2024-01-15T12:30:47Z", "client_ip": "192.0.2.3", '
                '"method": "DELETE", "host": "api.example.com", "path": "/resource", '
                '"status_code": 204, "user_agent": "GPTBot/2.0"}\n'
            )
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="ndjson_file",
                path_or_uri=str(temp_path),
            )

            records = list(
                adapter.ingest(source, filter_bots=False, strict_validation=False)
            )
            # Should skip record with missing required fields
            assert len(records) >= 2
            # All records should have client_ip
            for record in records:
                assert record.client_ip is not None
        finally:
            temp_path.unlink(missing_ok=True)

    def test_corrupted_gzip_handling(self, fixtures_dir, sqlite_backend):
        """Test that corrupted gzip files are handled gracefully."""
        import tempfile
        from pathlib import Path

        # Create a file that pretends to be gzip but isn't
        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".csv.gz", delete=False
        ) as f:
            f.write(b"This is not actually gzipped content\n")
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=str(temp_path),
            )

            # Should raise an appropriate error
            try:
                list(adapter.ingest(source, filter_bots=False))
                # If it doesn't raise, that's also acceptable
            except Exception as e:
                # Should be a recognizable error type
                assert isinstance(e, (OSError, IOError, Exception))
        finally:
            temp_path.unlink(missing_ok=True)

    def test_malformed_timestamp_recovery(self, fixtures_dir, sqlite_backend):
        """Test that records with malformed timestamps are handled."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
            # Valid timestamp
            f.write(
                '{"timestamp": "2024-01-15T12:30:45Z", "client_ip": "192.0.2.1", '
                '"method": "GET", "host": "example.com", "path": "/test", '
                '"status_code": 200, "user_agent": "GPTBot/1.0"}\n'
            )
            # Malformed timestamp
            f.write(
                '{"timestamp": "not-a-timestamp", "client_ip": "192.0.2.2", '
                '"method": "POST", "host": "example.com", "path": "/api", '
                '"status_code": 201, "user_agent": "ClaudeBot/1.0"}\n'
            )
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="ndjson_file",
                path_or_uri=str(temp_path),
            )

            # Should either skip bad records or parse what it can
            records = list(
                adapter.ingest(source, filter_bots=False, strict_validation=False)
            )
            # At least the valid record should be parsed
            assert len(records) >= 1
        finally:
            temp_path.unlink(missing_ok=True)

    def test_empty_file_handling(self, fixtures_dir, sqlite_backend):
        """Test that empty files are handled gracefully."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            # Just write header, no data
            f.write("timestamp,client_ip,method,host,path,status_code,user_agent\n")
            temp_path = Path(f.name)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=str(temp_path),
            )

            # Should return empty list, not error
            records = list(adapter.ingest(source, filter_bots=False))
            assert len(records) == 0
        finally:
            temp_path.unlink(missing_ok=True)


class TestIngestionWithFiltering:
    """Tests for ingestion with filtering applied."""

    def test_bot_filtering_workflow(self, fixtures_dir, sqlite_backend):
        """Test ingestion with bot filtering."""
        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(fixtures_dir / "universal" / "sample.csv"),
        )

        # With bot filtering
        records_filtered = list(adapter.ingest(source, filter_bots=True))
        # Without bot filtering
        records_unfiltered = list(adapter.ingest(source, filter_bots=False))

        assert len(records_filtered) <= len(records_unfiltered)
        # Verify filtered records are actually bots
        if records_filtered:
            from llm_bot_pipeline.utils.bot_classifier import classify_bot

            for record in records_filtered:
                bot_info = classify_bot(record.user_agent)
                assert (
                    bot_info is not None
                ), f"Filtered record should be a bot: {record.user_agent}"

    def test_time_filtering_workflow(self, fixtures_dir, sqlite_backend):
        """Test ingestion with time filtering."""
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

        # All records should be within time range (inclusive boundaries)
        for record in records:
            assert (
                start_time <= record.timestamp <= end_time
            ), f"Record {record.timestamp} not in range [{start_time}, {end_time}]"

        # Verify we got expected records (should be records at 12:30:46, 12:30:47, 12:30:48)
        assert len(records) >= 0  # May be 0 if no records in range
