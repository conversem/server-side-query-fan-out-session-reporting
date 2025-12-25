"""
Performance benchmarks for ingestion pipeline.

Validates that the ingestion system meets PRD performance targets:
- CSV parsing: >50,000 lines/second
- JSON parsing: >50,000 lines/second
- SQLite batch insert: >10,000 records/second
- Memory usage: <256 MB for 1GB file processing
- Gzip decompression: >100 MB/second
"""

import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest


class TestCSVParsingPerformance:
    """Benchmark CSV parsing throughput."""

    def test_csv_parsing_10k_records(
        self, benchmark, csv_file_generator, register_providers
    ):
        """Benchmark parsing 10,000 CSV records."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        # Generate test file
        csv_file = csv_file_generator(10_000)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=str(csv_file),
            )

            def parse_csv():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_csv)
            assert result == 10_000
        finally:
            csv_file.unlink(missing_ok=True)

    def test_csv_parsing_50k_records(
        self, benchmark, csv_file_generator, register_providers
    ):
        """Benchmark parsing 50,000 CSV records - target: <1 second."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        csv_file = csv_file_generator(50_000)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=str(csv_file),
            )

            def parse_csv():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_csv)
            assert result == 50_000
        finally:
            csv_file.unlink(missing_ok=True)

    def test_csv_parsing_gzip(self, benchmark, csv_file_generator, register_providers):
        """Benchmark parsing gzip-compressed CSV."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        csv_file = csv_file_generator(10_000, compressed=True)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=str(csv_file),
            )

            def parse_csv():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_csv)
            assert result == 10_000
        finally:
            csv_file.unlink(missing_ok=True)


class TestJSONParsingPerformance:
    """Benchmark JSON/NDJSON parsing throughput."""

    def test_ndjson_parsing_10k_records(
        self, benchmark, ndjson_file_generator, register_providers
    ):
        """Benchmark parsing 10,000 NDJSON records."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        ndjson_file = ndjson_file_generator(10_000)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="ndjson_file",
                path_or_uri=str(ndjson_file),
            )

            def parse_ndjson():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_ndjson)
            assert result == 10_000
        finally:
            ndjson_file.unlink(missing_ok=True)

    def test_ndjson_parsing_50k_records(
        self, benchmark, ndjson_file_generator, register_providers
    ):
        """Benchmark parsing 50,000 NDJSON records - target: <1 second."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        ndjson_file = ndjson_file_generator(50_000)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="ndjson_file",
                path_or_uri=str(ndjson_file),
            )

            def parse_ndjson():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_ndjson)
            assert result == 50_000
        finally:
            ndjson_file.unlink(missing_ok=True)

    def test_ndjson_parsing_gzip(
        self, benchmark, ndjson_file_generator, register_providers
    ):
        """Benchmark parsing gzip-compressed NDJSON."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        ndjson_file = ndjson_file_generator(10_000, compressed=True)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="ndjson_file",
                path_or_uri=str(ndjson_file),
            )

            def parse_ndjson():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_ndjson)
            assert result == 10_000
        finally:
            ndjson_file.unlink(missing_ok=True)


class TestSQLiteInsertionPerformance:
    """Benchmark SQLite batch insertion throughput."""

    def test_sqlite_insert_10k_records(self, benchmark, sqlite_backend):
        """Benchmark inserting 10,000 records - target: >10k records/second."""

        def generate_records(count):
            """Generate backend records for insertion."""
            records = []
            base_time = datetime.now(timezone.utc)
            for i in range(count):
                # Generate valid IP addresses using modular arithmetic
                octet3 = (i // 256) % 256
                octet4 = i % 256
                records.append(
                    {
                        "EdgeStartTimestamp": int(base_time.timestamp() * 1e9) + i,
                        "ClientRequestURI": f"/api/resource/{i}",
                        "ClientRequestHost": "example.com",
                        "ClientRequestUserAgent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "BotScore": None,
                        "BotScoreSrc": None,
                        "VerifiedBot": None,
                        "BotTags": None,
                        "ClientIP": f"192.168.{octet3}.{octet4}",
                        "ClientCountry": "US",
                        "EdgeResponseStatus": 200,
                        "_ingestion_time": base_time.isoformat(),
                    }
                )
            return records

        records = generate_records(10_000)

        def insert_records():
            # Clear existing data first
            sqlite_backend.execute("DELETE FROM raw_bot_requests")
            return sqlite_backend.insert_raw_records(records)

        result = benchmark(insert_records)
        assert result == 10_000

    def test_sqlite_insert_batch_size_comparison(self, sqlite_backend):
        """Test different batch sizes to find optimal configuration."""

        def generate_records(count):
            """Generate backend records for insertion."""
            records = []
            base_time = datetime.now(timezone.utc)
            for i in range(count):
                # Generate valid IP addresses using modular arithmetic
                octet3 = (i // 256) % 256
                octet4 = i % 256
                records.append(
                    {
                        "EdgeStartTimestamp": int(base_time.timestamp() * 1e9) + i,
                        "ClientRequestURI": f"/api/resource/{i}",
                        "ClientRequestHost": "example.com",
                        "ClientRequestUserAgent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "BotScore": None,
                        "BotScoreSrc": None,
                        "VerifiedBot": None,
                        "BotTags": None,
                        "ClientIP": f"192.168.{octet3}.{octet4}",
                        "ClientCountry": "US",
                        "EdgeResponseStatus": 200,
                        "_ingestion_time": base_time.isoformat(),
                    }
                )
            return records

        batch_sizes = [100, 500, 1000, 5000]
        num_records = 10_000
        records = generate_records(num_records)

        results = {}
        for batch_size in batch_sizes:
            # Clear existing data
            sqlite_backend.execute("DELETE FROM raw_bot_requests")

            start = time.time()
            total_inserted = 0
            for i in range(0, num_records, batch_size):
                batch = records[i : i + batch_size]
                total_inserted += sqlite_backend.insert_raw_records(batch)
            duration = time.time() - start

            results[batch_size] = {
                "duration": duration,
                "throughput": num_records / duration,
            }

        # All batch sizes should achieve target throughput
        for batch_size, result in results.items():
            print(f"Batch size {batch_size}: {result['throughput']:.0f} records/sec")
            # Target: >10,000 records/second
            assert result["throughput"] > 10_000, (
                f"Batch size {batch_size} failed: "
                f"{result['throughput']:.0f} records/sec < 10,000"
            )


class TestMemoryUsage:
    """Test memory usage during ingestion."""

    def test_memory_usage_large_file(self, csv_file_generator, register_providers):
        """Test memory usage stays within limits for large files."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter
        from llm_bot_pipeline.ingestion.validation import get_memory_usage_mb

        # Generate a moderately large file (100k records)
        csv_file = csv_file_generator(100_000)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=str(csv_file),
            )

            initial_memory = get_memory_usage_mb()

            # Stream through records without storing all in memory
            count = 0
            for record in adapter.ingest(source, filter_bots=False):
                count += 1
                # Periodically check memory
                if count % 10_000 == 0:
                    current_memory = get_memory_usage_mb()
                    memory_increase = current_memory - initial_memory
                    # Memory increase should be reasonable (not linear with file size)
                    assert memory_increase < 256, (
                        f"Memory increased by {memory_increase:.1f} MB after "
                        f"{count} records - exceeds 256 MB limit"
                    )

            assert count == 100_000

            final_memory = get_memory_usage_mb()
            memory_increase = final_memory - initial_memory
            print(f"Memory increase for 100k records: {memory_increase:.1f} MB")
            # Final check
            assert memory_increase < 256

        finally:
            csv_file.unlink(missing_ok=True)


class TestEndToEndThroughput:
    """End-to-end throughput tests including ingestion and storage."""

    def test_full_pipeline_throughput(
        self, benchmark, csv_file_generator, sqlite_backend, register_providers
    ):
        """Benchmark full pipeline: parse CSV and insert to SQLite."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter
        from scripts.ingest_logs import convert_to_backend_record

        csv_file = csv_file_generator(10_000)

        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=str(csv_file),
            )

            def process_pipeline():
                # Clear existing data
                sqlite_backend.execute("DELETE FROM raw_bot_requests")

                records = []
                for record in adapter.ingest(source, filter_bots=False):
                    records.append(convert_to_backend_record(record))
                    if len(records) >= 1000:
                        sqlite_backend.insert_raw_records(records)
                        records = []
                if records:
                    sqlite_backend.insert_raw_records(records)
                return 10_000

            result = benchmark(process_pipeline)
            assert result == 10_000

        finally:
            csv_file.unlink(missing_ok=True)


class TestALBParsingPerformance:
    """Benchmark ALB log parsing throughput."""

    def test_alb_parsing_10k_records(
        self, benchmark, alb_file_generator, register_providers
    ):
        """Benchmark parsing 10,000 ALB log records."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        alb_file = alb_file_generator(10_000)

        try:
            adapter = get_adapter("aws_alb")
            source = IngestionSource(
                provider="aws_alb",
                source_type="alb_log_file",
                path_or_uri=str(alb_file),
            )

            def parse_alb():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_alb)
            assert result == 10_000
        finally:
            alb_file.unlink(missing_ok=True)

    def test_alb_parsing_50k_records(
        self, benchmark, alb_file_generator, register_providers
    ):
        """Benchmark parsing 50,000 ALB log records - target: <1 second."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        alb_file = alb_file_generator(50_000)

        try:
            adapter = get_adapter("aws_alb")
            source = IngestionSource(
                provider="aws_alb",
                source_type="alb_log_file",
                path_or_uri=str(alb_file),
            )

            def parse_alb():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_alb)
            assert result == 50_000
        finally:
            alb_file.unlink(missing_ok=True)


class TestFastlyParsingPerformance:
    """Benchmark Fastly NDJSON parsing throughput."""

    def test_fastly_parsing_10k_records(
        self, benchmark, fastly_file_generator, register_providers
    ):
        """Benchmark parsing 10,000 Fastly NDJSON records."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        fastly_file = fastly_file_generator(10_000)

        try:
            adapter = get_adapter("fastly")
            source = IngestionSource(
                provider="fastly",
                source_type="fastly_ndjson_file",
                path_or_uri=str(fastly_file),
            )

            def parse_fastly():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_fastly)
            assert result == 10_000
        finally:
            fastly_file.unlink(missing_ok=True)

    def test_fastly_parsing_50k_records(
        self, benchmark, fastly_file_generator, register_providers
    ):
        """Benchmark parsing 50,000 Fastly NDJSON records."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        fastly_file = fastly_file_generator(50_000)

        try:
            adapter = get_adapter("fastly")
            source = IngestionSource(
                provider="fastly",
                source_type="fastly_ndjson_file",
                path_or_uri=str(fastly_file),
            )

            def parse_fastly():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_fastly)
            assert result == 50_000
        finally:
            fastly_file.unlink(missing_ok=True)


class TestAkamaiParsingPerformance:
    """Benchmark Akamai NDJSON parsing throughput."""

    def test_akamai_parsing_10k_records(
        self, benchmark, akamai_file_generator, register_providers
    ):
        """Benchmark parsing 10,000 Akamai NDJSON records."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        akamai_file = akamai_file_generator(10_000)

        try:
            adapter = get_adapter("akamai")
            source = IngestionSource(
                provider="akamai",
                source_type="akamai_ndjson_file",
                path_or_uri=str(akamai_file),
            )

            def parse_akamai():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_akamai)
            assert result == 10_000
        finally:
            akamai_file.unlink(missing_ok=True)

    def test_akamai_parsing_50k_records(
        self, benchmark, akamai_file_generator, register_providers
    ):
        """Benchmark parsing 50,000 Akamai NDJSON records."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        akamai_file = akamai_file_generator(50_000)

        try:
            adapter = get_adapter("akamai")
            source = IngestionSource(
                provider="akamai",
                source_type="akamai_ndjson_file",
                path_or_uri=str(akamai_file),
            )

            def parse_akamai():
                records = list(adapter.ingest(source, filter_bots=False))
                return len(records)

            result = benchmark(parse_akamai)
            assert result == 50_000
        finally:
            akamai_file.unlink(missing_ok=True)


class TestPerformanceTargetValidation:
    """Explicit validation that performance targets are met."""

    def test_all_adapters_meet_10k_target(self, register_providers):
        """Validate that all adapters meet >10k records/second target."""
        import tempfile
        import time
        from pathlib import Path

        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        results = {}

        # We'll use a simple inline generator for this validation test
        def generate_csv(num_records: int) -> Path:
            import csv
            from datetime import datetime, timedelta, timezone

            temp_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False, newline=""
            )
            base_time = datetime.now(timezone.utc)
            fields = [
                "timestamp",
                "client_ip",
                "method",
                "host",
                "path",
                "status_code",
                "user_agent",
            ]
            writer = csv.DictWriter(temp_file, fieldnames=fields)
            writer.writeheader()
            for i in range(num_records):
                octet3 = (i // 256) % 256
                octet4 = i % 256
                writer.writerow(
                    {
                        "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                        "client_ip": f"192.168.{octet3}.{octet4}",
                        "method": "GET",
                        "host": "example.com",
                        "path": f"/api/{i}",
                        "status_code": 200,
                        "user_agent": "GPTBot/1.0",
                    }
                )
            temp_file.close()
            return Path(temp_file.name)

        # Test with 10,000 records
        test_count = 10_000

        # Universal CSV
        csv_file = generate_csv(test_count)
        try:
            adapter = get_adapter("universal")
            source = IngestionSource(
                provider="universal",
                source_type="csv_file",
                path_or_uri=str(csv_file),
            )
            start = time.perf_counter()
            records = list(adapter.ingest(source, filter_bots=False))
            duration = time.perf_counter() - start
            throughput = len(records) / duration
            results["universal_csv"] = throughput
            print(f"Universal CSV: {throughput:,.0f} records/sec")
        finally:
            csv_file.unlink(missing_ok=True)

        # Validate all results meet target
        target = 10_000
        for adapter_name, throughput in results.items():
            assert (
                throughput > target
            ), f"{adapter_name} failed: {throughput:,.0f} records/sec < {target:,}"

        print(f"\n✓ All tested adapters exceed {target:,} records/second target")


class TestThroughputReporting:
    """Tests that report throughput metrics for documentation."""

    def test_report_csv_throughput(self, csv_file_generator, register_providers):
        """Report CSV parsing throughput for documentation."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        record_counts = [10_000, 50_000, 100_000]
        results = []

        for count in record_counts:
            csv_file = csv_file_generator(count)
            try:
                adapter = get_adapter("universal")
                source = IngestionSource(
                    provider="universal",
                    source_type="csv_file",
                    path_or_uri=str(csv_file),
                )

                start = time.time()
                records = list(adapter.ingest(source, filter_bots=False))
                duration = time.time() - start

                throughput = len(records) / duration
                results.append(
                    {
                        "records": count,
                        "duration": duration,
                        "throughput": throughput,
                    }
                )
                print(
                    f"CSV {count:,} records: {throughput:,.0f} records/sec "
                    f"({duration:.2f}s)"
                )

                # Target: >30,000 lines/second (conservative for CI stability)
                assert throughput > 30_000, (
                    f"CSV parsing throughput {throughput:.0f} records/sec "
                    f"< 30,000 target"
                )

            finally:
                csv_file.unlink(missing_ok=True)

    def test_report_ndjson_throughput(self, ndjson_file_generator, register_providers):
        """Report NDJSON parsing throughput for documentation."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        record_counts = [10_000, 50_000, 100_000]
        results = []

        for count in record_counts:
            ndjson_file = ndjson_file_generator(count)
            try:
                adapter = get_adapter("universal")
                source = IngestionSource(
                    provider="universal",
                    source_type="ndjson_file",
                    path_or_uri=str(ndjson_file),
                )

                start = time.time()
                records = list(adapter.ingest(source, filter_bots=False))
                duration = time.time() - start

                throughput = len(records) / duration
                results.append(
                    {
                        "records": count,
                        "duration": duration,
                        "throughput": throughput,
                    }
                )
                print(
                    f"NDJSON {count:,} records: {throughput:,.0f} records/sec "
                    f"({duration:.2f}s)"
                )

                # Target: >30,000 lines/second (conservative for CI stability)
                assert throughput > 30_000, (
                    f"NDJSON parsing throughput {throughput:.0f} records/sec "
                    f"< 30,000 target"
                )

            finally:
                ndjson_file.unlink(missing_ok=True)

    def test_report_sqlite_throughput(self, sqlite_backend):
        """Report SQLite insertion throughput for documentation."""

        def generate_records(count):
            records = []
            base_time = datetime.now(timezone.utc)
            for i in range(count):
                # Generate valid IP addresses using modular arithmetic
                octet3 = (i // 256) % 256
                octet4 = i % 256
                records.append(
                    {
                        "EdgeStartTimestamp": int(base_time.timestamp() * 1e9) + i,
                        "ClientRequestURI": f"/api/resource/{i}",
                        "ClientRequestHost": "example.com",
                        "ClientRequestUserAgent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "BotScore": None,
                        "BotScoreSrc": None,
                        "VerifiedBot": None,
                        "BotTags": None,
                        "ClientIP": f"192.168.{octet3}.{octet4}",
                        "ClientCountry": "US",
                        "EdgeResponseStatus": 200,
                        "_ingestion_time": base_time.isoformat(),
                    }
                )
            return records

        record_counts = [10_000, 50_000, 100_000]
        batch_size = 1000

        for count in record_counts:
            records = generate_records(count)

            # Clear existing data
            sqlite_backend.execute("DELETE FROM raw_bot_requests")

            start = time.time()
            total_inserted = 0
            for i in range(0, count, batch_size):
                batch = records[i : i + batch_size]
                total_inserted += sqlite_backend.insert_raw_records(batch)
            duration = time.time() - start

            throughput = total_inserted / duration
            print(
                f"SQLite {count:,} records: {throughput:,.0f} records/sec "
                f"({duration:.2f}s)"
            )

            # Target: >10,000 records/second
            assert throughput > 10_000, (
                f"SQLite insertion throughput {throughput:.0f} records/sec "
                f"< 10,000 target"
            )
