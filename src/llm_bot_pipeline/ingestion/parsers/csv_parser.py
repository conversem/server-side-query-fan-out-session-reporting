"""
CSV and TSV parser with streaming support.

Provides memory-efficient parsing for large log files in CSV/TSV format.
Supports gzip-compressed files.
"""

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Iterator, Optional, Union

from ..base import IngestionRecord
from ..exceptions import ParseError, ValidationError
from ..file_utils import open_file_auto_decompress
from .schema import get_optional_field_names, get_required_field_names, validate_record

logger = logging.getLogger(__name__)


class CSVParser:
    """
    Streaming CSV/TSV parser for log files.

    Supports:
    - Configurable delimiters (comma, tab, etc.)
    - Header detection and field mapping
    - Streaming parsing for large files
    - Schema validation

    Usage:
        parser = CSVParser(delimiter=',')
        with open('logs.csv') as f:
            for record in parser.parse(f, field_mapping):
                process(record)
    """

    def __init__(
        self,
        delimiter: str = ",",
        quotechar: str = '"',
        strict_validation: bool = False,
    ):
        """
        Initialize CSV parser.

        Args:
            delimiter: Field delimiter (default: comma)
            quotechar: Quote character for fields containing delimiter
            strict_validation: If True, reject records that fail validation
        """
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.strict_validation = strict_validation

    def parse(
        self,
        file_handle: IO[str],
        field_mapping: dict[str, str],
        header: Optional[list[str]] = None,
    ) -> Iterator[IngestionRecord]:
        """
        Parse CSV data from a file handle.

        Args:
            file_handle: Open file handle (text mode)
            field_mapping: Mapping from CSV column names to universal schema fields
                          e.g., {"ClientIP": "client_ip", "EdgeStartTimestamp": "timestamp"}
            header: Optional explicit header (if None, read from first row)

        Yields:
            IngestionRecord objects

        Raises:
            ParseError: If file cannot be parsed
        """
        reader = csv.reader(
            file_handle,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
        )

        # Get header
        if header is None:
            try:
                header = next(reader)
            except StopIteration:
                logger.warning("Empty CSV file")
                return

        # Strip BOM from first column if present (common in Excel exports)
        if header and header[0].startswith("\ufeff"):
            header[0] = header[0].lstrip("\ufeff")

        # Build column index mapping
        all_schema_fields = set(get_required_field_names()) | set(
            get_optional_field_names()
        )
        col_to_field = {}
        for idx, col_name in enumerate(header):
            if col_name in field_mapping:
                col_to_field[idx] = field_mapping[col_name]
            elif col_name in all_schema_fields:
                # Column name matches schema field directly (required or optional)
                col_to_field[idx] = col_name

        # Verify required fields are mapped
        mapped_fields = set(col_to_field.values())
        required_fields = set(get_required_field_names())
        missing_required = required_fields - mapped_fields

        if missing_required:
            raise ParseError(
                f"Missing required field mappings: {', '.join(sorted(missing_required))}. "
                f"Available columns: {', '.join(header)}"
            )

        # Parse rows
        line_number = 1  # Header was line 1
        records_parsed = 0
        records_skipped = 0

        for row in reader:
            line_number += 1

            if not row or all(cell.strip() == "" for cell in row):
                continue  # Skip empty rows

            try:
                record = self._parse_row(row, col_to_field, header, line_number)
                if record:
                    records_parsed += 1
                    yield record
                else:
                    # _parse_row returned None (validation failed in non-strict mode)
                    records_skipped += 1
            except (ValidationError, ValueError) as e:
                records_skipped += 1
                if self.strict_validation:
                    raise ParseError(
                        f"Row validation failed: {e}",
                        line_number=line_number,
                    )
                logger.debug(f"Skipping invalid row {line_number}: {e}")

        logger.info(
            f"CSV parsing complete: {records_parsed} records parsed, "
            f"{records_skipped} skipped"
        )

    def _parse_row(
        self,
        row: list[str],
        col_to_field: dict[int, str],
        header: list[str],
        line_number: int,
    ) -> Optional[IngestionRecord]:
        """
        Parse a single CSV row into an IngestionRecord.

        Args:
            row: List of cell values
            col_to_field: Column index to field name mapping
            header: Header row for extra fields
            line_number: Current line number for error reporting

        Returns:
            IngestionRecord or None if parsing fails
        """
        data = {}
        extra = {}

        for idx, value in enumerate(row):
            if idx in col_to_field:
                field_name = col_to_field[idx]
                data[field_name] = self._parse_value(value, field_name)
            elif idx < len(header):
                # Store unmapped columns in extra
                extra[header[idx]] = value

        # Validate required fields
        is_valid, errors = validate_record(data, strict=False)
        if not is_valid:
            if self.strict_validation:
                raise ValidationError(f"Record validation failed: {'; '.join(errors)}")
            logger.debug(f"Skipping row {line_number}: {'; '.join(errors)}")
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
                f"Skipping row {line_number}: invalid timestamp {data.get('timestamp')!r}"
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

    def _parse_value(self, value: str, field_name: str) -> Optional[str]:
        """Parse a cell value, handling empty strings and special values."""
        value = value.strip()
        if value == "" or value == "-" or value.lower() == "null":
            return None
        return value

    def _parse_timestamp(self, value: Optional[str]) -> Optional[datetime]:
        """Parse a timestamp string into a UTC timezone-aware datetime object."""
        if value is None:
            return None

        # Try ISO format first
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            # Ensure timezone-aware (assume UTC if naive)
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

        # Try Unix timestamp (seconds, milliseconds, microseconds, nanoseconds)
        # Unix timestamps are always UTC
        try:
            ts = float(value)
            if ts > 1e18:  # Nanoseconds (Cloudflare EdgeStartTimestamp)
                return datetime.fromtimestamp(ts / 1e9, tz=timezone.utc)
            elif ts > 1e15:  # Microseconds
                return datetime.fromtimestamp(ts / 1e6, tz=timezone.utc)
            elif ts > 1e12:  # Milliseconds
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            else:  # Seconds
                return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            pass

        return None

    def _to_optional_str(self, value: Any) -> Optional[str]:
        """Convert to optional string, treating empty as None."""
        if value is None or value == "":
            return None
        return str(value)

    def _to_optional_int(self, value: Any) -> Optional[int]:
        """Convert to optional int, treating empty as None."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))  # Handle "123.0" style values
        except (ValueError, TypeError):
            return None


def parse_csv_file(
    file_path: Union[str, Path],
    field_mapping: dict[str, str],
    delimiter: str = ",",
    encoding: str = "utf-8",
    strict_validation: bool = False,
) -> Iterator[IngestionRecord]:
    """
    Parse a CSV file and yield IngestionRecord objects.

    Convenience function for parsing CSV files. Automatically handles
    gzip-compressed files (.gz extension or gzip magic bytes).

    Args:
        file_path: Path to CSV file (supports .csv and .csv.gz)
        field_mapping: Mapping from CSV column names to universal schema fields
        delimiter: Field delimiter (default: comma)
        encoding: File encoding (default: utf-8)
        strict_validation: If True, reject invalid records

    Yields:
        IngestionRecord objects

    Raises:
        FileNotFoundError: If file doesn't exist
        ParseError: If file cannot be parsed
    """
    parser = CSVParser(
        delimiter=delimiter,
        strict_validation=strict_validation,
    )

    with open_file_auto_decompress(file_path, encoding) as f:
        yield from parser.parse(f, field_mapping)


def parse_tsv_file(
    file_path: Union[str, Path],
    field_mapping: dict[str, str],
    encoding: str = "utf-8",
    strict_validation: bool = False,
) -> Iterator[IngestionRecord]:
    """
    Parse a TSV file and yield IngestionRecord objects.

    Convenience function for parsing tab-separated files. Automatically handles
    gzip-compressed files (.gz extension or gzip magic bytes).

    Args:
        file_path: Path to TSV file (supports .tsv and .tsv.gz)
        field_mapping: Mapping from TSV column names to universal schema fields
        encoding: File encoding (default: utf-8)
        strict_validation: If True, reject invalid records

    Yields:
        IngestionRecord objects
    """
    yield from parse_csv_file(
        file_path=file_path,
        field_mapping=field_mapping,
        delimiter="\t",
        encoding=encoding,
        strict_validation=strict_validation,
    )
