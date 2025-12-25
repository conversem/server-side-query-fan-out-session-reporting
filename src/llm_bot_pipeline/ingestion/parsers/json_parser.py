"""
JSON and NDJSON parser with streaming support.

Provides memory-efficient parsing for log files in JSON formats:
- Single JSON object
- JSON array of objects
- NDJSON (newline-delimited JSON / JSON Lines)

Supports gzip-compressed files.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from ..base import IngestionRecord
from ..exceptions import ParseError, ValidationError
from ..file_utils import open_file_auto_decompress
from .schema import get_optional_field_names, get_required_field_names, validate_record

logger = logging.getLogger(__name__)


class JSONParser:
    """
    Streaming JSON/NDJSON parser for log files.

    Supports:
    - NDJSON (one JSON object per line) - recommended for large files
    - Single JSON object containing log data
    - JSON array of log objects
    - Nested field extraction with dot notation

    Usage:
        parser = JSONParser()
        with open('logs.ndjson') as f:
            for record in parser.parse_ndjson(f, field_mapping):
                process(record)
    """

    def __init__(
        self,
        strict_validation: bool = False,
    ):
        """
        Initialize JSON parser.

        Args:
            strict_validation: If True, reject records that fail validation
        """
        self.strict_validation = strict_validation

    def parse_ndjson(
        self,
        file_handle: IO[str],
        field_mapping: dict[str, str],
    ) -> Iterator[IngestionRecord]:
        """
        Parse NDJSON data (one JSON object per line).

        This is the preferred format for large files as it supports
        true streaming without loading the entire file into memory.

        Args:
            file_handle: Open file handle (text mode)
            field_mapping: Mapping from JSON field names to universal schema fields
                          e.g., {"ClientIP": "client_ip", "EdgeStartTimestamp": "timestamp"}
                          Supports dot notation for nested fields:
                          {"httpRequest.remoteIp": "client_ip"}

        Yields:
            IngestionRecord objects

        Raises:
            ParseError: If line cannot be parsed as JSON
        """
        line_number = 0
        records_parsed = 0
        records_skipped = 0

        for line in file_handle:
            line_number += 1
            line = line.strip()

            if not line:
                continue  # Skip empty lines

            try:
                obj = json.loads(line)
                record = self._parse_object(obj, field_mapping, line_number)
                if record:
                    records_parsed += 1
                    yield record
                else:
                    records_skipped += 1

            except json.JSONDecodeError as e:
                records_skipped += 1
                if self.strict_validation:
                    raise ParseError(
                        f"Invalid JSON: {e}",
                        line_number=line_number,
                        line_content=line[:100],
                    )
                logger.debug(f"Skipping invalid JSON at line {line_number}: {e}")

            except (ValidationError, ValueError) as e:
                records_skipped += 1
                if self.strict_validation:
                    raise ParseError(
                        f"Record validation failed: {e}",
                        line_number=line_number,
                    )
                logger.debug(f"Skipping invalid record at line {line_number}: {e}")

        logger.info(
            f"NDJSON parsing complete: {records_parsed} records parsed, "
            f"{records_skipped} skipped"
        )

    def parse_json(
        self,
        file_handle: IO[str],
        field_mapping: dict[str, str],
        records_path: Optional[str] = None,
    ) -> Iterator[IngestionRecord]:
        """
        Parse JSON file (single object or array).

        Note: This loads the entire JSON file into memory. For large files,
        use NDJSON format with parse_ndjson() instead.

        Args:
            file_handle: Open file handle (text mode)
            field_mapping: Mapping from JSON field names to universal schema fields
            records_path: Dot-notation path to the records array within the JSON
                         e.g., "data.logs" for {"data": {"logs": [...]}}

        Yields:
            IngestionRecord objects
        """
        try:
            data = json.load(file_handle)
        except json.JSONDecodeError as e:
            raise ParseError(f"Invalid JSON file: {e}") from e

        # Navigate to records array if path specified
        if records_path:
            data = self._get_nested_value(data, records_path)
            if data is None:
                raise ParseError(f"Records path '{records_path}' not found in JSON")

        # Handle single object vs array
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            records = data
        else:
            raise ParseError(
                f"Expected JSON object or array, got {type(data).__name__}"
            )

        records_parsed = 0
        records_skipped = 0

        for idx, obj in enumerate(records):
            try:
                record = self._parse_object(obj, field_mapping, idx + 1)
                if record:
                    records_parsed += 1
                    yield record
                else:
                    records_skipped += 1
            except (ValidationError, ValueError) as e:
                records_skipped += 1
                if self.strict_validation:
                    raise ParseError(f"Record {idx + 1} validation failed: {e}")
                logger.debug(f"Skipping invalid record {idx + 1}: {e}")

        logger.info(
            f"JSON parsing complete: {records_parsed} records parsed, "
            f"{records_skipped} skipped"
        )

    def _parse_object(
        self,
        obj: dict,
        field_mapping: dict[str, str],
        record_number: int,
    ) -> Optional[IngestionRecord]:
        """
        Parse a single JSON object into an IngestionRecord.

        Args:
            obj: JSON object (dictionary)
            field_mapping: Field name mapping
            record_number: Record number for error reporting

        Returns:
            IngestionRecord or None if parsing fails
        """
        if not isinstance(obj, dict):
            if self.strict_validation:
                raise ValidationError(f"Expected object, got {type(obj).__name__}")
            logger.debug(
                f"Skipping record {record_number}: expected object, got {type(obj).__name__}"
            )
            return None

        data = {}
        extra = {}

        # Apply field mapping
        for source_field, target_field in field_mapping.items():
            value = self._get_nested_value(obj, source_field)
            if value is not None:
                data[target_field] = value

        # Also check for fields that match schema directly (not already mapped)
        all_schema_fields = set(get_required_field_names()) | set(
            get_optional_field_names()
        )
        for field_name in all_schema_fields:
            if field_name not in data and field_name in obj:
                data[field_name] = obj[field_name]

        # Collect unmapped fields (not in mapping and not in schema)
        mapped_sources = set(field_mapping.keys())
        for key, value in obj.items():
            if key not in mapped_sources and key not in all_schema_fields:
                extra[key] = value

        # Validate required fields
        is_valid, errors = validate_record(data, strict=False)
        if not is_valid:
            if self.strict_validation:
                raise ValidationError(f"Record validation failed: {'; '.join(errors)}")
            logger.debug(f"Skipping record {record_number}: {'; '.join(errors)}")
            return None

        # Parse timestamp
        timestamp = self._parse_timestamp(data.get("timestamp"))
        if timestamp is None:
            if self.strict_validation:
                raise ValidationError(
                    f"Invalid timestamp: {data.get('timestamp')}",
                    field="timestamp",
                    value=data.get("timestamp"),
                )
            logger.debug(
                f"Skipping record {record_number}: invalid timestamp {data.get('timestamp')!r}"
            )
            return None

        return IngestionRecord(
            timestamp=timestamp,
            client_ip=str(data["client_ip"]),
            method=str(data["method"]).upper(),
            host=str(data["host"]),
            path=str(data["path"]),
            status_code=int(data["status_code"]),
            user_agent=str(data["user_agent"]),
            query_string=self._to_optional_str(data.get("query_string")),
            response_bytes=self._to_optional_int(data.get("response_bytes")),
            request_bytes=self._to_optional_int(data.get("request_bytes")),
            response_time_ms=self._to_optional_int(data.get("response_time_ms")),
            cache_status=self._to_optional_str(data.get("cache_status")),
            edge_location=self._to_optional_str(data.get("edge_location")),
            referer=self._to_optional_str(data.get("referer")),
            protocol=self._to_optional_str(data.get("protocol")),
            ssl_protocol=self._to_optional_str(data.get("ssl_protocol")),
            extra=extra,
        )

    def _get_nested_value(self, obj: dict, path: str) -> Any:
        """
        Get a value from a nested dictionary using dot notation.

        Args:
            obj: Dictionary to search
            path: Dot-separated path (e.g., "httpRequest.remoteIp")

        Returns:
            Value at path or None if not found
        """
        parts = path.split(".")
        current = obj

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

        return current

    def _parse_timestamp(self, value: Any) -> Optional[datetime]:
        """Parse a timestamp value into a UTC timezone-aware datetime object."""
        if value is None:
            return None

        if isinstance(value, datetime):
            # Ensure timezone-aware (assume UTC if naive)
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        if isinstance(value, str):
            # Try ISO format
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                # Ensure timezone-aware
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass

            # Try removing trailing 's' from latency format (e.g., "0.123s")
            if value.endswith("s"):
                value = value[:-1]

        # Try numeric timestamp (Unix timestamps are always UTC)
        try:
            ts = float(value) if isinstance(value, str) else value
            if isinstance(ts, (int, float)):
                if ts > 1e18:  # Nanoseconds (Cloudflare EdgeStartTimestamp)
                    return datetime.fromtimestamp(ts / 1e9, tz=timezone.utc)
                elif ts > 1e15:  # Microseconds
                    return datetime.fromtimestamp(ts / 1e6, tz=timezone.utc)
                elif ts > 1e12:  # Milliseconds
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                else:  # Seconds
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (ValueError, OSError, OverflowError, TypeError):
            pass

        return None

    def _to_optional_str(self, value: Any) -> Optional[str]:
        """Convert to optional string."""
        if value is None or value == "":
            return None
        return str(value)

    def _to_optional_int(self, value: Any) -> Optional[int]:
        """Convert to optional int."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None


def parse_ndjson_file(
    file_path: Union[str, Path],
    field_mapping: dict[str, str],
    encoding: str = "utf-8",
    strict_validation: bool = False,
) -> Iterator[IngestionRecord]:
    """
    Parse an NDJSON file and yield IngestionRecord objects.

    Recommended for large log files as it supports true streaming.
    Automatically handles gzip-compressed files (.gz extension or gzip magic bytes).

    Args:
        file_path: Path to NDJSON file (supports .ndjson and .ndjson.gz)
        field_mapping: Mapping from JSON field names to universal schema fields
        encoding: File encoding (default: utf-8)
        strict_validation: If True, reject invalid records

    Yields:
        IngestionRecord objects

    Raises:
        FileNotFoundError: If file doesn't exist
        ParseError: If file cannot be parsed
    """
    parser = JSONParser(strict_validation=strict_validation)

    with open_file_auto_decompress(file_path, encoding) as f:
        yield from parser.parse_ndjson(f, field_mapping)


def parse_json_file(
    file_path: Union[str, Path],
    field_mapping: dict[str, str],
    records_path: Optional[str] = None,
    encoding: str = "utf-8",
    strict_validation: bool = False,
) -> Iterator[IngestionRecord]:
    """
    Parse a JSON file and yield IngestionRecord objects.

    Note: Loads entire file into memory. Use parse_ndjson_file() for large files.
    Automatically handles gzip-compressed files (.gz extension or gzip magic bytes).

    Args:
        file_path: Path to JSON file (supports .json and .json.gz)
        field_mapping: Mapping from JSON field names to universal schema fields
        records_path: Dot-notation path to records array (e.g., "data.logs")
        encoding: File encoding (default: utf-8)
        strict_validation: If True, reject invalid records

    Yields:
        IngestionRecord objects
    """
    parser = JSONParser(strict_validation=strict_validation)

    with open_file_auto_decompress(file_path, encoding) as f:
        yield from parser.parse_json(f, field_mapping, records_path)
