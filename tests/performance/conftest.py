"""
Pytest configuration and fixtures for performance tests.

Provides fixtures for generating large test data files.
"""

import csv
import gzip
import json
import random
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


@pytest.fixture
def sample_record():
    """Generate a sample log record dictionary."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "client_ip": f"192.168.1.{random.randint(1, 254)}",
        "method": random.choice(["GET", "POST", "PUT", "DELETE"]),
        "host": "example.com",
        "path": f"/api/v1/resource/{random.randint(1, 10000)}",
        "status_code": random.choice([200, 201, 400, 404, 500]),
        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0; +https://openai.com/gptbot)",
        "query_string": f"page={random.randint(1, 100)}",
        "response_bytes": random.randint(100, 10000),
        "request_bytes": random.randint(50, 500),
    }


@pytest.fixture
def csv_file_generator():
    """Factory fixture for generating CSV files with specified number of records."""

    def _generate(num_records: int, compressed: bool = False) -> Path:
        """Generate a CSV file with the specified number of records."""
        suffix = ".csv.gz" if compressed else ".csv"
        temp_file = tempfile.NamedTemporaryFile(
            mode="wb" if compressed else "w",
            suffix=suffix,
            delete=False,
            newline="" if not compressed else None,
        )

        # Generate records
        fields = [
            "timestamp",
            "client_ip",
            "method",
            "host",
            "path",
            "status_code",
            "user_agent",
            "query_string",
            "response_bytes",
            "request_bytes",
        ]

        base_time = datetime.now(timezone.utc)

        if compressed:
            with gzip.open(temp_file.name, "wt", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                for i in range(num_records):
                    # Generate valid IP addresses using modular arithmetic
                    octet3 = (i // 256) % 256
                    octet4 = i % 256
                    record = {
                        "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                        "client_ip": f"192.168.{octet3}.{octet4}",
                        "method": ["GET", "POST"][i % 2],
                        "host": "example.com",
                        "path": f"/api/resource/{i}",
                        "status_code": 200,
                        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "query_string": f"id={i}",
                        "response_bytes": 1024,
                        "request_bytes": 256,
                    }
                    writer.writerow(record)
        else:
            with open(temp_file.name, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                writer.writeheader()
                for i in range(num_records):
                    # Generate valid IP addresses using modular arithmetic
                    octet3 = (i // 256) % 256
                    octet4 = i % 256
                    record = {
                        "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                        "client_ip": f"192.168.{octet3}.{octet4}",
                        "method": ["GET", "POST"][i % 2],
                        "host": "example.com",
                        "path": f"/api/resource/{i}",
                        "status_code": 200,
                        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "query_string": f"id={i}",
                        "response_bytes": 1024,
                        "request_bytes": 256,
                    }
                    writer.writerow(record)

        return Path(temp_file.name)

    return _generate


@pytest.fixture
def ndjson_file_generator():
    """Factory fixture for generating NDJSON files with specified number of records."""

    def _generate(num_records: int, compressed: bool = False) -> Path:
        """Generate an NDJSON file with the specified number of records."""
        suffix = ".ndjson.gz" if compressed else ".ndjson"
        temp_file = tempfile.NamedTemporaryFile(
            mode="wb" if compressed else "w",
            suffix=suffix,
            delete=False,
        )

        base_time = datetime.now(timezone.utc)

        if compressed:
            with gzip.open(temp_file.name, "wt") as f:
                for i in range(num_records):
                    # Generate valid IP addresses using modular arithmetic
                    octet3 = (i // 256) % 256
                    octet4 = i % 256
                    record = {
                        "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                        "client_ip": f"192.168.{octet3}.{octet4}",
                        "method": ["GET", "POST"][i % 2],
                        "host": "example.com",
                        "path": f"/api/resource/{i}",
                        "status_code": 200,
                        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "query_string": f"id={i}",
                        "response_bytes": 1024,
                        "request_bytes": 256,
                    }
                    f.write(json.dumps(record) + "\n")
        else:
            with open(temp_file.name, "w") as f:
                for i in range(num_records):
                    # Generate valid IP addresses using modular arithmetic
                    octet3 = (i // 256) % 256
                    octet4 = i % 256
                    record = {
                        "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                        "client_ip": f"192.168.{octet3}.{octet4}",
                        "method": ["GET", "POST"][i % 2],
                        "host": "example.com",
                        "path": f"/api/resource/{i}",
                        "status_code": 200,
                        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "query_string": f"id={i}",
                        "response_bytes": 1024,
                        "request_bytes": 256,
                    }
                    f.write(json.dumps(record) + "\n")

        return Path(temp_file.name)

    return _generate


@pytest.fixture
def sqlite_backend():
    """Create a temporary SQLite backend for testing."""
    import tempfile

    from llm_bot_pipeline.storage import get_backend

    # Create temporary database
    temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_db.close()

    backend = get_backend("sqlite", db_path=Path(temp_db.name))
    backend.initialize()

    yield backend

    # Cleanup
    backend.close()
    Path(temp_db.name).unlink(missing_ok=True)


@pytest.fixture
def register_providers():
    """Ensure providers are registered before tests."""
    from llm_bot_pipeline.ingestion.providers import (
        AkamaiAdapter,
        ALBAdapter,
        AzureCDNAdapter,
        CloudflareAdapter,
        CloudFrontAdapter,
        FastlyAdapter,
        GCPCDNAdapter,
        UniversalAdapter,
    )
    from llm_bot_pipeline.ingestion.registry import IngestionRegistry

    if not IngestionRegistry.is_provider_registered("universal"):
        IngestionRegistry.register_provider("universal", UniversalAdapter)
    if not IngestionRegistry.is_provider_registered("aws_cloudfront"):
        IngestionRegistry.register_provider("aws_cloudfront", CloudFrontAdapter)
    if not IngestionRegistry.is_provider_registered("cloudflare"):
        IngestionRegistry.register_provider("cloudflare", CloudflareAdapter)
    if not IngestionRegistry.is_provider_registered("aws_alb"):
        IngestionRegistry.register_provider("aws_alb", ALBAdapter)
    if not IngestionRegistry.is_provider_registered("fastly"):
        IngestionRegistry.register_provider("fastly", FastlyAdapter)
    if not IngestionRegistry.is_provider_registered("akamai"):
        IngestionRegistry.register_provider("akamai", AkamaiAdapter)
    if not IngestionRegistry.is_provider_registered("gcp_cdn"):
        IngestionRegistry.register_provider("gcp_cdn", GCPCDNAdapter)
    if not IngestionRegistry.is_provider_registered("azure_cdn"):
        IngestionRegistry.register_provider("azure_cdn", AzureCDNAdapter)


@pytest.fixture
def alb_file_generator():
    """Factory fixture for generating ALB log files with specified number of records."""

    def _generate(num_records: int, compressed: bool = False) -> Path:
        """Generate an ALB log file with the specified number of records."""
        suffix = ".log.gz" if compressed else ".log"
        temp_file = tempfile.NamedTemporaryFile(
            mode="wb" if compressed else "w",
            suffix=suffix,
            delete=False,
        )

        base_time = datetime.now(timezone.utc)

        lines = []
        for i in range(num_records):
            # Generate valid IP addresses using modular arithmetic
            octet3 = (i // 256) % 256
            octet4 = i % 256
            ts = (base_time + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S.%f")[
                :-3
            ] + "Z"
            line = (
                f"http 2024-01-15T12:30:{i % 60:02d}.{i % 1000:03d}000Z app/my-alb/1234567890 "
                f"192.168.{octet3}.{octet4}:12345 10.0.0.1:80 0.001 0.002 0.003 200 200 "
                f'256 1024 "GET http://example.com/api/resource/{i} HTTP/1.1" '
                f'"Mozilla/5.0 (compatible; GPTBot/1.0)" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 '
                f"arn:aws:elasticloadbalancing:us-east-1:123456789:targetgroup/tg 1.2 "
                f'"Root=1-12345-{i:08d}" "example.com" "arn:aws:acm:cert" 1 {ts} '
                f'"forward" "-" "-" "10.0.0.1:80" "200" "-" "-"'
            )
            lines.append(line)

        if compressed:
            with gzip.open(temp_file.name, "wt") as f:
                f.write("\n".join(lines))
        else:
            with open(temp_file.name, "w") as f:
                f.write("\n".join(lines))

        return Path(temp_file.name)

    return _generate


@pytest.fixture
def fastly_file_generator():
    """Factory fixture for generating Fastly NDJSON files with specified number of records."""

    def _generate(num_records: int, compressed: bool = False) -> Path:
        """Generate a Fastly NDJSON file with the specified number of records."""
        suffix = ".ndjson.gz" if compressed else ".ndjson"
        temp_file = tempfile.NamedTemporaryFile(
            mode="wb" if compressed else "w",
            suffix=suffix,
            delete=False,
        )

        base_time = datetime.now(timezone.utc)

        if compressed:
            with gzip.open(temp_file.name, "wt") as f:
                for i in range(num_records):
                    octet3 = (i // 256) % 256
                    octet4 = i % 256
                    record = {
                        "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                        "client_ip": f"192.168.{octet3}.{octet4}",
                        "method": ["GET", "POST"][i % 2],
                        "host": "example.com",
                        "path": f"/api/resource/{i}",
                        "status_code": 200,
                        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "response_bytes": 1024,
                        "cache_status": "HIT",
                        "pop": "SFO",
                    }
                    f.write(json.dumps(record) + "\n")
        else:
            with open(temp_file.name, "w") as f:
                for i in range(num_records):
                    octet3 = (i // 256) % 256
                    octet4 = i % 256
                    record = {
                        "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                        "client_ip": f"192.168.{octet3}.{octet4}",
                        "method": ["GET", "POST"][i % 2],
                        "host": "example.com",
                        "path": f"/api/resource/{i}",
                        "status_code": 200,
                        "user_agent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "response_bytes": 1024,
                        "cache_status": "HIT",
                        "pop": "SFO",
                    }
                    f.write(json.dumps(record) + "\n")

        return Path(temp_file.name)

    return _generate


@pytest.fixture
def akamai_file_generator():
    """Factory fixture for generating Akamai NDJSON files with specified number of records."""

    def _generate(num_records: int, compressed: bool = False) -> Path:
        """Generate an Akamai NDJSON file with the specified number of records."""
        suffix = ".ndjson.gz" if compressed else ".ndjson"
        temp_file = tempfile.NamedTemporaryFile(
            mode="wb" if compressed else "w",
            suffix=suffix,
            delete=False,
        )

        base_time = datetime.now(timezone.utc)

        if compressed:
            with gzip.open(temp_file.name, "wt") as f:
                for i in range(num_records):
                    octet3 = (i // 256) % 256
                    octet4 = i % 256
                    record = {
                        "requestTime": (base_time + timedelta(seconds=i)).isoformat(),
                        "clientIP": f"192.168.{octet3}.{octet4}",
                        "requestMethod": ["GET", "POST"][i % 2],
                        "requestHost": "example.com",
                        "requestPath": f"/api/resource/{i}",
                        "responseStatus": 200,
                        "userAgent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "bytesSent": 1024,
                        "cacheStatus": "HIT",
                        "edgeLocation": "ASH",
                    }
                    f.write(json.dumps(record) + "\n")
        else:
            with open(temp_file.name, "w") as f:
                for i in range(num_records):
                    octet3 = (i // 256) % 256
                    octet4 = i % 256
                    record = {
                        "requestTime": (base_time + timedelta(seconds=i)).isoformat(),
                        "clientIP": f"192.168.{octet3}.{octet4}",
                        "requestMethod": ["GET", "POST"][i % 2],
                        "requestHost": "example.com",
                        "requestPath": f"/api/resource/{i}",
                        "responseStatus": 200,
                        "userAgent": "Mozilla/5.0 (compatible; GPTBot/1.0)",
                        "bytesSent": 1024,
                        "cacheStatus": "HIT",
                        "edgeLocation": "ASH",
                    }
                    f.write(json.dumps(record) + "\n")

        return Path(temp_file.name)

    return _generate
