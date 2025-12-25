"""
W3C Extended Log Format parser with streaming support.

Provides memory-efficient parsing for W3C extended log format files
used by AWS CloudFront, DigitalOcean Spaces, and other providers.

W3C Extended Log Format:
    #Version: 1.0
    #Fields: date time c-ip cs-method cs-uri-stem sc-status cs(User-Agent)
    2024-01-15	12:30:45	192.0.2.100	GET	/api/data	200	Mozilla/5.0
"""

import logging
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Iterator, Optional, Union

from ..base import IngestionRecord
from ..exceptions import ParseError, ValidationError
from ..file_utils import open_file_auto_decompress
from .schema import get_optional_field_names, get_required_field_names, validate_record

logger = logging.getLogger(__name__)


class W3CParser:
    """
    Streaming W3C Extended Log Format parser.

    Supports:
    - Header directive parsing (#Version, #Fields)
    - Tab-separated value parsing
    - URL decoding for encoded fields
    - Streaming parsing for large files
    - Gzip compression support

    Usage:
        parser = W3CParser()
        with open('cloudfront.log') as f:
            for record in parser.parse(f, field_mapping):
                process(record)
    """

    def __init__(
        self,
        url_decode: bool = True,
        strict_validation: bool = False,
    ):
        """
        Initialize W3C parser.

        Args:
            url_decode: If True, URL-decode fields like User-Agent and query strings
            strict_validation: If True, reject records that fail validation
        """
        self.url_decode = url_decode
        self.strict_validation = strict_validation

    def parse(
        self,
        file_handle: IO[str],
        field_mapping: dict[str, str],
    ) -> Iterator[IngestionRecord]:
        """
        Parse W3C extended log format data from a file handle.

        Args:
            file_handle: Open file handle (text mode)
            field_mapping: Mapping from W3C field names to universal schema fields
                          e.g., {"c-ip": "client_ip", "cs-method": "method"}

        Yields:
            IngestionRecord objects

        Raises:
            ParseError: If file cannot be parsed
        """
        # Parse header directives
        header_info = self._parse_header(file_handle)
        field_names = header_info["fields"]
        version = header_info.get("version", "1.0")

        logger.debug(f"W3C log version: {version}, fields: {len(field_names)}")

        # Build column index to field name mapping
        col_to_w3c_field = {idx: name for idx, name in enumerate(field_names)}

        # Build mapping from W3C field names to universal schema fields
        w3c_to_schema = {}
        for w3c_field, schema_field in field_mapping.items():
            w3c_to_schema[w3c_field] = schema_field

        # Also check for direct matches (field name matches schema directly)
        all_schema_fields = set(get_required_field_names()) | set(
            get_optional_field_names()
        )
        for idx, w3c_field in enumerate(field_names):
            if w3c_field not in w3c_to_schema and w3c_field in all_schema_fields:
                w3c_to_schema[w3c_field] = w3c_field

        # Verify required fields are mapped
        # Note: timestamp can be constructed from date+time fields, so it's special
        mapped_schema_fields = set(w3c_to_schema.values())
        required_fields = set(get_required_field_names())

        # Check if timestamp can be constructed from date+time
        has_timestamp = "timestamp" in mapped_schema_fields
        has_date = "date" in mapped_schema_fields or any(
            w3c_field == "date" for w3c_field in w3c_to_schema.keys()
        )
        has_time = "time" in mapped_schema_fields or any(
            w3c_field == "time" for w3c_field in w3c_to_schema.keys()
        )
        can_construct_timestamp = has_date and has_time

        # Remove timestamp from required if we can construct it
        if can_construct_timestamp and not has_timestamp:
            required_fields.discard("timestamp")

        missing_required = required_fields - mapped_schema_fields

        if missing_required:
            raise ParseError(
                f"Missing required field mappings: {', '.join(sorted(missing_required))}. "
                f"Available W3C fields: {', '.join(field_names)}"
            )

        # Parse data rows
        line_number = len(header_info.get("header_lines", []))
        records_parsed = 0
        records_skipped = 0

        # Helper function to process a single line (reduces code duplication)
        def process_line(line: str, line_num: int) -> Optional[IngestionRecord]:
            """Process a single line and return IngestionRecord or None."""
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                return None

            try:
                record = self._parse_row(
                    line, col_to_w3c_field, w3c_to_schema, field_names, line_num
                )
                return record
            except (ValidationError, ValueError) as e:
                if self.strict_validation:
                    raise ParseError(
                        f"Row validation failed: {e}",
                        line_number=line_num,
                    )
                logger.debug(f"Skipping invalid row {line_num}: {e}")
                return None

        # Process first data line if it was captured during header parsing
        first_line = header_info.get("first_data_line")
        if first_line:
            line_number += 1
            record = process_line(first_line, line_number)
            if record:
                records_parsed += 1
                yield record
            else:
                records_skipped += 1

        # Process remaining lines
        for line in file_handle:
            line_number += 1
            record = process_line(line, line_number)
            if record:
                records_parsed += 1
                yield record
            else:
                records_skipped += 1

        logger.info(
            f"W3C parsing complete: {records_parsed} records parsed, "
            f"{records_skipped} skipped"
        )

    def _parse_header(self, file_handle: IO[str]) -> dict:
        """
        Parse W3C header directives (#Version, #Fields).

        Args:
            file_handle: Open file handle positioned at start

        Returns:
            Dictionary with 'version', 'fields' (list), 'header_lines' (list),
            and 'first_data_line' (str or None)

        Raises:
            ParseError: If header is malformed
        """
        header_lines = []
        version = None
        fields = None
        first_data_line = None

        for line in file_handle:
            stripped_line = line.strip()
            header_lines.append(stripped_line)

            if not stripped_line or not stripped_line.startswith("#"):
                # End of header, save first data line
                if stripped_line:  # Non-empty data line
                    first_data_line = line  # Keep original (with newline if present)
                break

            if stripped_line.startswith("#Version:"):
                version = stripped_line.split(":", 1)[1].strip()
            elif stripped_line.startswith("#Fields:"):
                # Fields are tab-separated or space-separated
                fields_str = stripped_line.split(":", 1)[1].strip()
                # Split by tabs first, fall back to spaces
                if "\t" in fields_str:
                    fields = [f.strip() for f in fields_str.split("\t") if f.strip()]
                else:
                    fields = [f.strip() for f in fields_str.split() if f.strip()]

        if fields is None:
            raise ParseError("Missing #Fields directive in W3C log header")

        return {
            "version": version,
            "fields": fields,
            "header_lines": header_lines,
            "first_data_line": first_data_line,
        }

    def _parse_row(
        self,
        line: str,
        col_to_w3c_field: dict[int, str],
        w3c_to_schema: dict[str, str],
        field_names: list[str],
        line_number: int,
    ) -> Optional[IngestionRecord]:
        """
        Parse a single W3C log row into an IngestionRecord.

        Args:
            line: Tab-separated row data
            col_to_w3c_field: Column index to W3C field name mapping
            w3c_to_schema: W3C field name to universal schema field mapping
            field_names: List of W3C field names (for extra fields)
            line_number: Current line number for error reporting

        Returns:
            IngestionRecord or None if parsing fails
        """
        # Split by tabs
        values = line.split("\t")

        data = {}
        extra = {}

        # Map values to schema fields
        for idx, value in enumerate(values):
            # Strip whitespace from value before processing
            value = value.strip() if isinstance(value, str) else value

            if idx in col_to_w3c_field:
                w3c_field = col_to_w3c_field[idx]
                if w3c_field in w3c_to_schema:
                    schema_field = w3c_to_schema[w3c_field]
                    # Apply URL decoding if enabled and field needs it
                    decoded_value = self._decode_value(value, w3c_field)
                    data[schema_field] = decoded_value
                else:
                    # Store unmapped W3C fields in extra
                    extra[w3c_field] = value
            elif idx < len(field_names):
                # Store unmapped columns in extra
                extra[field_names[idx]] = value

        # Parse timestamp (may need to combine date+time fields) BEFORE validation
        timestamp = self._parse_timestamp(data, w3c_to_schema, values, col_to_w3c_field)
        if timestamp is None:
            if self.strict_validation:
                raise ValidationError(
                    f"Invalid timestamp: unable to construct from available fields",
                    field="timestamp",
                )
            logger.debug(f"Skipping row {line_number}: invalid timestamp")
            return None

        # Add timestamp to data for validation
        data["timestamp"] = timestamp

        # Validate required fields
        is_valid, errors = validate_record(data, strict=False)
        if not is_valid:
            if self.strict_validation:
                raise ValidationError(f"Record validation failed: {'; '.join(errors)}")
            logger.debug(f"Skipping row {line_number}: {'; '.join(errors)}")
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
            response_time_ms=self._parse_response_time_ms(
                data.get("response_time_ms"), w3c_to_schema, values, col_to_w3c_field
            ),
            cache_status=self._to_optional_str(data.get("cache_status")),
            edge_location=self._to_optional_str(data.get("edge_location")),
            referer=self._to_optional_str(data.get("referer")),
            protocol=self._to_optional_str(data.get("protocol")),
            ssl_protocol=self._to_optional_str(data.get("ssl_protocol")),
            extra=extra,
        )

    def _decode_value(self, value: str, w3c_field: str) -> Optional[str]:
        """
        Decode a field value, applying URL decoding if needed.

        Args:
            value: Raw field value
            w3c_field: W3C field name (to determine if decoding is needed)

        Returns:
            Decoded value or None if empty/dash/null
        """
        # Strip whitespace and check for empty/null values (consistent with CSV parser)
        if isinstance(value, str):
            value = value.strip()
        if (
            not value
            or value == "-"
            or (isinstance(value, str) and value.lower() == "null")
        ):
            return None

        # Fields that typically need URL decoding
        url_decode_fields = {
            "cs-uri-query",
            "cs(Referer)",
            "cs(User-Agent)",
            "cs-uri-stem",  # Sometimes contains encoded characters
        }

        if self.url_decode and w3c_field in url_decode_fields:
            try:
                # Use 'replace' instead of 'strict' for better resilience
                return urllib.parse.unquote(value, errors="replace")
            except (UnicodeDecodeError, ValueError) as e:
                # If decoding fails, log warning and return original
                logger.debug(f"Failed to URL-decode {w3c_field}: {value} ({e})")
                return value

        return value

    def _parse_timestamp(
        self,
        data: dict,
        w3c_to_schema: dict[str, str],
        values: list[str],
        col_to_w3c_field: dict[int, str],
    ) -> Optional[datetime]:
        """
        Parse timestamp from W3C log data.

        W3C format typically has separate 'date' and 'time' fields that need
        to be combined. Also checks if timestamp is already in data.

        Args:
            data: Parsed data dictionary
            w3c_to_schema: W3C to schema field mapping
            values: Raw row values
            col_to_w3c_field: Column index to W3C field mapping

        Returns:
            Datetime object or None if cannot be parsed
        """
        # Check if timestamp is already in data (from field mapping)
        if "timestamp" in data:
            timestamp_value = data["timestamp"]
            if isinstance(timestamp_value, datetime):
                return timestamp_value
            if isinstance(timestamp_value, str):
                # Try parsing as ISO format or other formats
                parsed = self._parse_timestamp_value(timestamp_value)
                if parsed:
                    return parsed

        # Try to combine date and time fields
        date_value = None
        time_value = None

        # Look for date and time in raw W3C field values first (before any processing)
        for idx, w3c_field in col_to_w3c_field.items():
            if w3c_field == "date" and idx < len(values):
                raw_date = (
                    values[idx].strip()
                    if idx < len(values) and isinstance(values[idx], str)
                    else None
                )
                date_value = raw_date if raw_date and raw_date != "-" else None
            elif w3c_field == "time" and idx < len(values):
                raw_time = (
                    values[idx].strip()
                    if idx < len(values) and isinstance(values[idx], str)
                    else None
                )
                time_value = raw_time if raw_time and raw_time != "-" else None

        # Fall back to mapped data if not found in raw values
        if not date_value and "date" in data:
            date_value = data["date"]
        if not time_value and "time" in data:
            time_value = data["time"]

        if date_value and time_value:
            # Combine date and time (format: YYYY-MM-DD HH:MM:SS)
            try:
                dt_str = f"{date_value} {time_value}"
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                logger.debug(
                    f"Failed to parse timestamp from date={date_value}, time={time_value}"
                )
                pass

        return None

    def _parse_timestamp_value(self, value: str) -> Optional[datetime]:
        """Parse a timestamp string into a UTC timezone-aware datetime object."""
        if not value or value == "-":
            return None

        # Try ISO format
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

        # Try common log format
        try:
            return datetime.strptime(value, "%d/%b/%Y:%H:%M:%S %z")
        except ValueError:
            pass

        return None

    def _to_optional_str(self, value: Optional[str]) -> Optional[str]:
        """Convert to optional string, treating empty/dash/null as None."""
        if value is None:
            return None
        # Strip and check for empty/null values (consistent with CSV parser)
        str_value = str(value).strip()
        if str_value == "" or str_value == "-" or str_value.lower() == "null":
            return None
        return str_value

    def _to_optional_int(self, value: Optional[str]) -> Optional[int]:
        """Convert to optional int, treating empty/dash/null as None."""
        if value is None:
            return None
        # Strip and check for empty/null values (consistent with CSV parser)
        str_value = str(value).strip()
        if str_value == "" or str_value == "-" or str_value.lower() == "null":
            return None
        try:
            return int(float(str_value))  # Handle "123.0" style values
        except (ValueError, TypeError):
            return None

    def _parse_response_time_ms(
        self,
        value: Optional[str],
        w3c_to_schema: dict[str, str],
        values: list[str],
        col_to_w3c_field: dict[int, str],
    ) -> Optional[int]:
        """
        Parse response time and convert to milliseconds.

        W3C format uses 'time-taken' field in seconds (float).
        We need to convert to milliseconds for universal schema.

        Args:
            value: Response time value from mapped data
            w3c_to_schema: W3C to schema field mapping
            values: Raw row values
            col_to_w3c_field: Column index to W3C field mapping

        Returns:
            Response time in milliseconds or None
        """
        # Check if already in data
        if value is not None:
            try:
                # Convert seconds to milliseconds
                seconds = float(value)
                return int(seconds * 1000)
            except (ValueError, TypeError):
                pass

        # Look for time-taken in raw W3C field values (before any processing)
        for idx, w3c_field in col_to_w3c_field.items():
            if w3c_field == "time-taken" and idx < len(values):
                time_taken = (
                    values[idx].strip() if isinstance(values[idx], str) else values[idx]
                )
                if time_taken and time_taken != "-" and time_taken.lower() != "null":
                    try:
                        seconds = float(time_taken)
                        return int(seconds * 1000)
                    except (ValueError, TypeError):
                        pass

        return None


def parse_w3c_file(
    file_path: Union[str, Path],
    field_mapping: dict[str, str],
    encoding: str = "utf-8",
    url_decode: bool = True,
    strict_validation: bool = False,
) -> Iterator[IngestionRecord]:
    """
    Parse a W3C extended log format file and yield IngestionRecord objects.

    Convenience function for parsing W3C log files. Automatically handles
    gzip-compressed files (.gz extension or gzip magic bytes).

    Args:
        file_path: Path to W3C log file (supports .log, .txt, .gz)
        field_mapping: Mapping from W3C field names to universal schema fields
                      e.g., {"c-ip": "client_ip", "cs-method": "method", "date": "date", "time": "time"}
        encoding: File encoding (default: utf-8)
        url_decode: If True, URL-decode fields like User-Agent and query strings
        strict_validation: If True, reject invalid records

    Yields:
        IngestionRecord objects

    Raises:
        FileNotFoundError: If file doesn't exist
        ParseError: If file cannot be parsed
    """
    parser = W3CParser(url_decode=url_decode, strict_validation=strict_validation)

    with open_file_auto_decompress(file_path, encoding) as f:
        yield from parser.parse(f, field_mapping)
