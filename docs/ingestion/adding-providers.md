# Adding New Providers

## Overview

This guide explains how to add support for new CDN or cloud providers to the ingestion system. The system uses an adapter pattern where each provider implements the `IngestionAdapter` interface.

## Architecture

The ingestion system follows this architecture:

```
IngestionAdapter (abstract base class)
    ├── UniversalAdapter (CSV/JSON/NDJSON)
    ├── CloudFrontAdapter (W3C format)
    ├── CloudflareAdapter (API + files)
    └── YourNewAdapter (your provider)
```

## Step-by-Step Guide

### Step 1: Create Provider Directory

Create a new directory for your provider:

```bash
mkdir -p src/llm_bot_pipeline/ingestion/providers/your_provider
touch src/llm_bot_pipeline/ingestion/providers/your_provider/__init__.py
touch src/llm_bot_pipeline/ingestion/providers/your_provider/adapter.py
```

### Step 2: Implement the Adapter

Create your adapter class inheriting from `IngestionAdapter`:

```python
"""
Your Provider adapter for log ingestion.

Supports ingestion from Your Provider's log format.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from ...base import IngestionAdapter, IngestionRecord, IngestionSource
from ...exceptions import ParseError, SourceValidationError
from ...registry import IngestionRegistry

logger = logging.getLogger(__name__)


@IngestionRegistry.register("your_provider")
class YourProviderAdapter(IngestionAdapter):
    """
    Your Provider adapter for log ingestion.

    Supports ingestion from Your Provider's log format.

    Supported source types:
        - csv_file: CSV format exports
        - json_file: JSON format exports

    Example:
        source = IngestionSource(
            provider="your_provider",
            source_type="csv_file",
            path_or_uri="/path/to/logs.csv",
        )
        adapter = YourProviderAdapter()
        for record in adapter.ingest(source, filter_bots=True):
            print(record)
    """

    @property
    def provider_name(self) -> str:
        """Return the provider name identifier."""
        return "your_provider"

    @property
    def supported_source_types(self) -> list[str]:
        """Return list of supported source types."""
        return ["csv_file", "json_file"]

    def ingest(
        self,
        source: IngestionSource,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_bots: bool = True,
        **kwargs,
    ) -> Iterator[IngestionRecord]:
        """
        Ingest logs from the source.

        Args:
            source: Ingestion source configuration
            start_time: Optional start time filter (UTC)
            end_time: Optional end time filter (UTC)
            filter_bots: If True, only yield records from known LLM bots
            **kwargs: Additional options

        Yields:
            IngestionRecord objects in universal format

        Raises:
            SourceValidationError: If source is invalid
            ParseError: If log data cannot be parsed
        """
        # Validate source
        is_valid, error_msg = self.validate_source(source)
        if not is_valid:
            raise SourceValidationError(
                f"Source validation failed: {error_msg}",
                source_type=source.source_type,
                reason=error_msg,
            )

        path = Path(source.path_or_uri)

        # Process file or directory
        if path.is_file():
            yield from self._ingest_file(path, source.source_type, start_time, end_time, filter_bots)
        elif path.is_dir():
            yield from self._ingest_directory(path, source.source_type, start_time, end_time, filter_bots)
        else:
            raise SourceValidationError(
                f"Path does not exist: {path}",
                source_type=source.source_type,
                reason="Path not found",
            )

    def validate_source(self, source: IngestionSource) -> tuple[bool, str]:
        """
        Validate that the source is accessible and valid.

        Args:
            source: Ingestion source to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if source.provider != self.provider_name:
            return False, f"Provider mismatch: expected {self.provider_name}, got {source.provider}"

        if source.source_type not in self.supported_source_types:
            return False, f"Unsupported source type: {source.source_type}"

        path = Path(source.path_or_uri)

        if not path.exists():
            return False, f"Path does not exist: {path}"

        if path.is_file():
            # Validate file extension
            ext = path.suffix.lower()
            if ext == ".gz":
                ext = path.stem.split(".")[-1].lower()
            if ext not in [".csv", ".json"]:
                return False, f"Unsupported file extension: {ext}"

        return True, ""

    def _ingest_file(
        self,
        file_path: Path,
        source_type: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from a single file."""
        # Parse file based on source_type
        if source_type == "csv_file":
            records = self._parse_csv(file_path)
        elif source_type == "json_file":
            records = self._parse_json(file_path)
        else:
            raise ParseError(f"Unsupported source type: {source_type}")

        # Apply filters
        for record in records:
            # Time filtering
            if start_time and record.timestamp < start_time:
                continue
            if end_time and record.timestamp > end_time:
                continue

            # Bot filtering
            if filter_bots:
                from ....utils.bot_classifier import classify_bot

                bot_info = classify_bot(record.user_agent)
                if bot_info is None:
                    continue

            yield record

    def _parse_csv(self, file_path: Path) -> Iterator[IngestionRecord]:
        """Parse CSV file and yield IngestionRecord objects."""
        import csv

        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Map provider fields to universal schema
                record = self._map_to_universal_schema(row)
                if record:
                    yield record

    def _parse_json(self, file_path: Path) -> Iterator[IngestionRecord]:
        """Parse JSON file and yield IngestionRecord objects."""
        import json

        with open(file_path, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                records = data
            else:
                records = [data]

            for row in records:
                # Map provider fields to universal schema
                record = self._map_to_universal_schema(row)
                if record:
                    yield record

    def _map_to_universal_schema(self, provider_record: dict) -> Optional[IngestionRecord]:
        """
        Map provider-specific fields to universal schema.

        Args:
            provider_record: Raw record from provider

        Returns:
            IngestionRecord or None if record is invalid
        """
        # Define field mapping
        FIELD_MAPPING = {
            "provider_timestamp": "timestamp",
            "provider_client_ip": "client_ip",
            "provider_method": "method",
            "provider_host": "host",
            "provider_path": "path",
            "provider_status": "status_code",
            "provider_user_agent": "user_agent",
        }

        # Map fields
        mapped = {}
        for provider_field, universal_field in FIELD_MAPPING.items():
            if provider_field in provider_record:
                mapped[universal_field] = provider_record[provider_field]

        # Validate required fields
        required_fields = ["timestamp", "client_ip", "method", "host", "path", "status_code", "user_agent"]
        if not all(field in mapped for field in required_fields):
            logger.warning(f"Skipping record with missing required fields: {provider_record}")
            return None

        # Parse timestamp
        timestamp = IngestionRecord._parse_timestamp_value(mapped["timestamp"])
        if timestamp is None:
            logger.warning(f"Skipping record with invalid timestamp: {provider_record}")
            return None

        # Create IngestionRecord
        try:
            return IngestionRecord(
                timestamp=timestamp,
                client_ip=mapped["client_ip"],
                method=mapped["method"],
                host=mapped["host"],
                path=mapped["path"],
                status_code=int(mapped["status_code"]),
                user_agent=mapped["user_agent"],
                query_string=mapped.get("query_string"),
                response_bytes=mapped.get("response_bytes"),
                request_bytes=mapped.get("request_bytes"),
                response_time_ms=mapped.get("response_time_ms"),
                cache_status=mapped.get("cache_status"),
                edge_location=mapped.get("edge_location"),
                referer=mapped.get("referer"),
                protocol=mapped.get("protocol"),
                ssl_protocol=mapped.get("ssl_protocol"),
            )
        except (ValueError, TypeError) as e:
            logger.warning(f"Skipping record with invalid data: {e}")
            return None

    def _ingest_directory(
        self,
        dir_path: Path,
        source_type: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from a directory."""
        # Find matching files
        extensions = {
            "csv_file": [".csv", ".csv.gz"],
            "json_file": [".json", ".json.gz"],
        }

        for ext in extensions.get(source_type, []):
            for file_path in dir_path.rglob(f"*{ext}"):
                try:
                    yield from self._ingest_file(file_path, source_type, start_time, end_time, filter_bots)
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    continue
```

### Step 3: Register the Adapter

Ensure your adapter is imported in the providers `__init__.py`:

```python
# src/llm_bot_pipeline/ingestion/providers/__init__.py

from .universal import UniversalAdapter
from .aws_cloudfront import CloudFrontAdapter
from .cloudflare import CloudflareAdapter
from .your_provider import YourProviderAdapter  # Add this

__all__ = [
    "UniversalAdapter",
    "CloudFrontAdapter",
    "CloudflareAdapter",
    "YourProviderAdapter",  # Add this
]
```

### Step 4: Add Tests

Create test files:

```python
# tests/unit/test_your_provider_adapter.py

import pytest
from pathlib import Path

from llm_bot_pipeline.ingestion import IngestionSource, get_adapter
from llm_bot_pipeline.ingestion.providers import YourProviderAdapter  # noqa: F401


def test_provider_name():
    """Test provider name."""
    adapter = get_adapter("your_provider")
    assert adapter.provider_name == "your_provider"


def test_supported_source_types():
    """Test supported source types."""
    adapter = get_adapter("your_provider")
    assert "csv_file" in adapter.supported_source_types
    assert "json_file" in adapter.supported_source_types


def test_ingest_csv_file(fixtures_dir):
    """Test ingesting CSV file."""
    adapter = get_adapter("your_provider")
    source = IngestionSource(
        provider="your_provider",
        source_type="csv_file",
        path_or_uri=str(fixtures_dir / "your_provider" / "sample.csv"),
    )

    records = list(adapter.ingest(source, filter_bots=False))
    assert len(records) > 0
    assert records[0].client_ip is not None
```

### Step 5: Create Sample Data

Add sample data fixtures:

```bash
mkdir -p tests/fixtures/ingestion/your_provider
# Create sample.csv and sample.json files
```

### Step 6: Update Documentation

Add provider documentation:

```bash
touch docs/ingestion/providers/your-provider.md
```

## Field Mapping Best Practices

1. **Map all required fields**: Ensure all 7 required universal schema fields are mapped
2. **Handle missing fields**: Use `.get()` with defaults for optional fields
3. **Validate data types**: Convert strings to integers/floats as needed
4. **Parse timestamps**: Use `IngestionRecord._parse_timestamp_value()` for timestamp parsing
5. **Handle URL encoding**: Decode URL-encoded fields (User-Agent, query strings)
6. **Preserve extra data**: Store provider-specific fields in `extra` dict

## Common Patterns

### Using Existing Parsers

If your provider uses standard CSV/JSON formats, you can reuse existing parsers:

```python
from ...parsers import parse_csv_file, parse_json_file

def _ingest_file(self, file_path: Path, source_type: str, ...):
    if source_type == "csv_file":
        for record_dict in parse_csv_file(file_path):
            yield self._map_to_universal_schema(record_dict)
```

### Custom Parser

For custom formats (like W3C), implement your own parser:

```python
def _parse_custom_format(self, file_path: Path) -> Iterator[dict]:
    """Parse custom format and yield dict records."""
    with open(file_path, "r") as f:
        # Parse header
        header = f.readline().strip()
        # Parse records
        for line in f:
            # Parse line into dict
            yield self._parse_line(line, header)
```

### Time Filtering

Always apply time filtering after parsing:

```python
for record in self._parse_file(file_path):
    if start_time and record.timestamp < start_time:
        continue
    if end_time and record.timestamp > end_time:
        continue
    yield record
```

### Bot Filtering

Use the bot classifier utility:

```python
from ....utils.bot_classifier import classify_bot

if filter_bots:
    bot_info = classify_bot(record.user_agent)
    if bot_info is None:
        continue  # Skip non-bot records
```

## Testing Checklist

- [ ] Provider name is correct
- [ ] Supported source types are listed
- [ ] Source validation works
- [ ] File ingestion works
- [ ] Directory ingestion works
- [ ] Time filtering works
- [ ] Bot filtering works
- [ ] Field mapping is correct
- [ ] Error handling is robust
- [ ] Tests pass

## Example: Complete Adapter

See existing adapters for complete examples:

- `src/llm_bot_pipeline/ingestion/providers/universal/adapter.py` - Simple CSV/JSON adapter
- `src/llm_bot_pipeline/ingestion/providers/aws_cloudfront/adapter.py` - W3C format adapter
- `src/llm_bot_pipeline/ingestion/providers/cloudflare/adapter.py` - API + file adapter

## Next Steps

1. Implement your adapter following the template above
2. Add tests for your adapter
3. Create sample data fixtures
4. Update documentation
5. Submit a pull request

For questions or help, see the existing adapter implementations or open an issue.


