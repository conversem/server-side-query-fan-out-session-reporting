"""
Fastly CDN adapter for log ingestion.

Supports ingestion from Fastly real-time log streaming via:
- JSON exports (fastly_json_file)
- CSV exports (fastly_csv_file)
- NDJSON exports (fastly_ndjson_file)

Fastly logs are highly configurable - users define their own format using
Apache-style placeholders. This adapter supports flexible field mapping
via the options parameter.

Default Field Mapping:
    Fastly Field    -> Universal Schema Field
    timestamp       -> timestamp (ISO 8601 or Unix)
    client_ip       -> client_ip
    method          -> method
    host            -> host
    path            -> path
    status_code     -> status_code
    user_agent      -> user_agent
    query_string    -> query_string
    response_bytes  -> response_bytes
    response_time_ms -> response_time_ms
    referer         -> referer
"""

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Union

from llm_bot_pipeline.ingestion.file_utils import open_file_auto_decompress

from ....utils.bot_classifier import classify_bot
from ...base import IngestionAdapter, IngestionRecord, IngestionSource
from ...exceptions import ParseError, SourceValidationError
from ...registry import IngestionRegistry
from ...security import validate_path_safe

logger = logging.getLogger(__name__)


# Default field name mappings (universal field -> expected Fastly field name)
DEFAULT_FIELD_MAPPING: dict[str, str] = {
    "timestamp": "timestamp",
    "client_ip": "client_ip",
    "method": "method",
    "host": "host",
    "path": "path",
    "status_code": "status_code",
    "user_agent": "user_agent",
    "query_string": "query_string",
    "request_bytes": "request_bytes",
    "response_bytes": "response_bytes",
    "response_time_ms": "response_time_ms",
    "referer": "referer",
    "protocol": "protocol",
    "ssl_protocol": "ssl_protocol",
    "cache_status": "cache_status",
    "edge_location": "edge_location",
}

# Common field name variations (universal field -> list of common alternatives)
FIELD_ALIASES: dict[str, list[str]] = {
    "timestamp": ["timestamp", "time", "date", "request_time", "start_time"],
    "client_ip": ["client_ip", "clientip", "client", "ip", "remote_addr"],
    "method": ["method", "http_method", "request_method", "verb"],
    "host": ["host", "hostname", "server_name", "domain"],
    "path": ["path", "uri", "url", "request_uri", "request_path"],
    "status_code": ["status_code", "status", "http_status", "response_code"],
    "user_agent": ["user_agent", "useragent", "user-agent", "ua"],
    "query_string": ["query_string", "query", "qs", "querystring"],
    "request_bytes": ["request_bytes", "request_size", "bytes_received"],
    "response_bytes": ["response_bytes", "bytes", "body_bytes", "size", "bytes_sent"],
    "response_time_ms": [
        "response_time_ms",
        "response_time",
        "duration",
        "latency",
        "time_taken",
    ],
    "referer": ["referer", "referrer", "http_referer"],
    "protocol": ["protocol", "http_protocol", "http_version"],
    "ssl_protocol": ["ssl_protocol", "tls_version", "ssl_version", "tls_protocol"],
    "cache_status": ["cache_status", "cache", "hit", "cache_hit", "fastly_info"],
    "edge_location": [
        "edge_location",
        "pop",
        "datacenter",
        "server_region",
        "location",
    ],
}


@IngestionRegistry.register("fastly")
class FastlyAdapter(IngestionAdapter):
    """
    Fastly CDN adapter for log ingestion.

    Supports ingestion from Fastly real-time log streaming via:
    - JSON exports - source_type: "fastly_json_file"
    - CSV exports - source_type: "fastly_csv_file"
    - NDJSON exports - source_type: "fastly_ndjson_file"

    The adapter supports configurable field mapping since Fastly logs
    use customer-defined field names.

    Example with custom field mapping:
        source = IngestionSource(
            provider="fastly",
            source_type="fastly_json_file",
            path_or_uri="/path/to/logs.json",
            options={
                "field_mapping": {
                    "timestamp": "request_time",
                    "client_ip": "clientip",
                    "status_code": "http_status",
                }
            }
        )
        adapter = FastlyAdapter()
        for record in adapter.ingest(source, filter_bots=True):
            print(record)
    """

    @property
    def provider_name(self) -> str:
        """Return the provider name identifier."""
        return "fastly"

    @property
    def supported_source_types(self) -> list[str]:
        """Return list of supported source types."""
        return ["fastly_json_file", "fastly_csv_file", "fastly_ndjson_file"]

    def ingest(
        self,
        source: IngestionSource,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_bots: bool = True,
        **kwargs,
    ) -> Iterator[IngestionRecord]:
        """
        Ingest logs from Fastly log exports.

        Args:
            source: Ingestion source configuration
            start_time: Optional start time filter (UTC)
            end_time: Optional end time filter (UTC)
            filter_bots: If True, only yield records from known LLM bots
            **kwargs: Additional options:
                - strict_validation: If True, reject invalid records (default: False)

        Yields:
            IngestionRecord objects in universal format

        Raises:
            SourceValidationError: If source is invalid or inaccessible
            ParseError: If log data cannot be parsed
        """
        # Validate source first
        is_valid, error_msg = self.validate_source(source)
        if not is_valid:
            raise SourceValidationError(
                f"Source validation failed: {error_msg}",
                source_type=source.source_type,
                reason=error_msg,
            )

        strict_validation = kwargs.get("strict_validation", False)

        # Ensure timezone-aware datetimes for filtering
        if start_time is not None:
            start_time = self._ensure_utc(start_time)
        if end_time is not None:
            end_time = self._ensure_utc(end_time)

        # Validate time range
        if start_time is not None and end_time is not None:
            if start_time > end_time:
                raise ValueError(
                    f"Invalid time range: start_time ({start_time}) > end_time ({end_time})"
                )

        # Get field mapping from options or use defaults
        field_mapping = self._get_field_mapping(source.options)

        path = Path(source.path_or_uri)

        # Determine if source is a file or directory
        if path.is_file():
            yield from self._ingest_file(
                source,
                path,
                field_mapping,
                start_time,
                end_time,
                filter_bots,
                strict_validation,
            )
        elif path.is_dir():
            yield from self._ingest_directory(
                source,
                path,
                field_mapping,
                start_time,
                end_time,
                filter_bots,
                strict_validation,
            )
        else:
            raise SourceValidationError(
                f"Path does not exist or is not accessible: {path}",
                source_type=source.source_type,
                reason="Path not found",
            )

    def validate_source(
        self, source: IngestionSource, base_dir: Optional[Path] = None
    ) -> tuple[bool, str]:
        """
        Validate that the source is accessible and properly formatted.

        Args:
            source: Ingestion source configuration
            base_dir: Optional base directory to constrain file access within

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check source type is supported
        if not self.supports_source_type(source.source_type):
            return (
                False,
                f"Unsupported source type: {source.source_type}. "
                f"Supported types: {', '.join(self.supported_source_types)}",
            )

        path = Path(source.path_or_uri)

        # Security: Validate path is safe from traversal attacks
        is_safe, security_error = validate_path_safe(
            path, base_dir=base_dir, allow_symlinks=True
        )
        if not is_safe:
            return (False, f"Security validation failed: {security_error}")

        # Check if path exists
        if not path.exists():
            return (False, f"Path does not exist: {path}")

        # Check if path is readable
        if path.is_file():
            try:
                if path.stat().st_size == 0:
                    return (False, f"File is empty: {path}")
            except (OSError, PermissionError) as e:
                return (False, f"Cannot access file {path}: {e}")
        elif path.is_dir():
            # Check if directory has any matching files
            matching_files = list(self._find_matching_files(path, source.source_type))
            if not matching_files:
                return (
                    False,
                    f"No matching log files found in directory: {path}",
                )

        return (True, "")

    def _get_field_mapping(self, options: dict) -> dict[str, str]:
        """
        Get field mapping from options, merged with defaults.

        Args:
            options: Source options dict

        Returns:
            Complete field mapping dict
        """
        # Start with defaults
        mapping = dict(DEFAULT_FIELD_MAPPING)

        # Override with custom mappings from options
        custom_mapping = options.get("field_mapping", {})
        mapping.update(custom_mapping)

        return mapping

    def _ingest_file(
        self,
        source: IngestionSource,
        file_path: Path,
        field_mapping: dict[str, str],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from a single Fastly log file."""
        logger.info(f"Ingesting Fastly logs from file: {file_path}")

        try:
            if source.source_type == "fastly_json_file":
                yield from self._parse_json_file(
                    file_path,
                    field_mapping,
                    start_time,
                    end_time,
                    filter_bots,
                    strict_validation,
                )
            elif source.source_type == "fastly_ndjson_file":
                yield from self._parse_ndjson_file(
                    file_path,
                    field_mapping,
                    start_time,
                    end_time,
                    filter_bots,
                    strict_validation,
                )
            elif source.source_type == "fastly_csv_file":
                yield from self._parse_csv_file(
                    file_path,
                    field_mapping,
                    start_time,
                    end_time,
                    filter_bots,
                    strict_validation,
                )
        except (ParseError, SourceValidationError):
            raise
        except Exception as e:
            raise ParseError(
                f"Failed to parse Fastly log file {file_path}: {e}",
            ) from e

    def _ingest_directory(
        self,
        source: IngestionSource,
        dir_path: Path,
        field_mapping: dict[str, str],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from all matching log files in a directory."""
        logger.info(f"Ingesting Fastly logs from directory: {dir_path}")

        matching_files = list(self._find_matching_files(dir_path, source.source_type))
        logger.info(f"Found {len(matching_files)} matching log files")

        for file_path in matching_files:
            try:
                yield from self._ingest_file(
                    source,
                    file_path,
                    field_mapping,
                    start_time,
                    end_time,
                    filter_bots,
                    strict_validation,
                )
            except Exception as e:
                logger.warning(f"Failed to ingest {file_path}: {e}")
                if strict_validation:
                    raise
                continue

    def _find_matching_files(self, dir_path: Path, source_type: str) -> Iterator[Path]:
        """Find all matching log files in directory based on source type."""
        extension_map = {
            "fastly_json_file": [".json", ".json.gz"],
            "fastly_csv_file": [".csv", ".csv.gz"],
            "fastly_ndjson_file": [".ndjson", ".jsonl", ".ndjson.gz", ".jsonl.gz"],
        }
        extensions = extension_map.get(source_type, [".json"])

        seen = set()
        try:
            for ext in extensions:
                for file_path in dir_path.rglob(f"*{ext}"):
                    try:
                        resolved_path = file_path.resolve()
                        if resolved_path not in seen:
                            seen.add(resolved_path)
                            yield file_path
                    except (OSError, RuntimeError):
                        logger.debug(f"Skipping inaccessible file: {file_path}")
                        continue
        except PermissionError:
            logger.error(f"Permission denied accessing directory: {dir_path}")
            raise
        except Exception as e:
            logger.warning(f"Error searching directory {dir_path}: {e}")
            raise

    def _parse_json_file(
        self,
        file_path: Path,
        field_mapping: dict[str, str],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Parse a JSON log file (array of objects or single object)."""
        with open_file_auto_decompress(file_path) as f:
            data = json.load(f)

        # Handle both array and single object
        if isinstance(data, dict):
            entries = [data]
        elif isinstance(data, list):
            entries = data
        else:
            raise ParseError(f"Unexpected JSON structure in {file_path}")

        for idx, entry in enumerate(entries):
            try:
                record = self._map_entry_to_record(entry, field_mapping)
                if record is None:
                    continue

                # Time filtering
                if start_time is not None and record.timestamp < start_time:
                    continue
                if end_time is not None and record.timestamp > end_time:
                    continue

                # Bot filtering
                if filter_bots:
                    bot_info = classify_bot(record.user_agent)
                    if bot_info is None:
                        continue

                yield record

            except Exception as e:
                if strict_validation:
                    raise ParseError(
                        f"Failed to parse entry {idx} in {file_path}: {e}"
                    ) from e
                logger.debug(f"Skipping invalid entry {idx}: {e}")
                continue

    def _parse_ndjson_file(
        self,
        file_path: Path,
        field_mapping: dict[str, str],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Parse a NDJSON (newline-delimited JSON) log file."""
        with open_file_auto_decompress(file_path) as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    entry = json.loads(line)
                    record = self._map_entry_to_record(entry, field_mapping)
                    if record is None:
                        continue

                    # Time filtering
                    if start_time is not None and record.timestamp < start_time:
                        continue
                    if end_time is not None and record.timestamp > end_time:
                        continue

                    # Bot filtering
                    if filter_bots:
                        bot_info = classify_bot(record.user_agent)
                        if bot_info is None:
                            continue

                    yield record

                except Exception as e:
                    if strict_validation:
                        raise ParseError(
                            f"Failed to parse line {line_num} in {file_path}: {e}"
                        ) from e
                    logger.debug(f"Skipping invalid line {line_num}: {e}")
                    continue

    def _parse_csv_file(
        self,
        file_path: Path,
        field_mapping: dict[str, str],
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Parse a CSV log file with header row."""
        with open_file_auto_decompress(file_path) as f:
            reader = csv.DictReader(f)

            for row_num, row in enumerate(reader, 2):  # Start at 2 (after header)
                try:
                    record = self._map_entry_to_record(dict(row), field_mapping)
                    if record is None:
                        continue

                    # Time filtering
                    if start_time is not None and record.timestamp < start_time:
                        continue
                    if end_time is not None and record.timestamp > end_time:
                        continue

                    # Bot filtering
                    if filter_bots:
                        bot_info = classify_bot(record.user_agent)
                        if bot_info is None:
                            continue

                    yield record

                except Exception as e:
                    if strict_validation:
                        raise ParseError(
                            f"Failed to parse row {row_num} in {file_path}: {e}"
                        ) from e
                    logger.debug(f"Skipping invalid row {row_num}: {e}")
                    continue

    def _map_entry_to_record(
        self, entry: dict[str, Any], field_mapping: dict[str, str]
    ) -> Optional[IngestionRecord]:
        """
        Map a log entry to an IngestionRecord using field mapping.

        Args:
            entry: Raw log entry dict
            field_mapping: Universal field -> Fastly field name mapping

        Returns:
            IngestionRecord or None if required fields are missing
        """
        # Extract required fields using mapping with alias fallback
        timestamp = self._get_field_with_aliases(entry, "timestamp", field_mapping)
        if timestamp is None:
            logger.debug("Missing timestamp field")
            return None

        try:
            timestamp_dt = self._parse_timestamp(timestamp)
        except ValueError as e:
            logger.debug(f"Failed to parse timestamp '{timestamp}': {e}")
            return None

        client_ip = self._get_field_with_aliases(entry, "client_ip", field_mapping)
        if not client_ip:
            logger.debug("Missing client_ip field")
            return None

        method = self._get_field_with_aliases(entry, "method", field_mapping)
        if not method:
            logger.debug("Missing method field")
            return None

        host = self._get_field_with_aliases(entry, "host", field_mapping)
        path = self._get_field_with_aliases(entry, "path", field_mapping) or "/"

        status_code = self._get_field_with_aliases(entry, "status_code", field_mapping)
        if status_code is None:
            logger.debug("Missing status_code field")
            return None
        status_code = self._to_optional_int(status_code)
        if status_code is None:
            logger.debug("Invalid status_code value")
            return None

        user_agent = (
            self._get_field_with_aliases(entry, "user_agent", field_mapping) or ""
        )

        # Optional fields
        query_string = self._get_field_with_aliases(
            entry, "query_string", field_mapping
        )
        request_bytes = self._to_optional_int(
            self._get_field_with_aliases(entry, "request_bytes", field_mapping)
        )
        response_bytes = self._to_optional_int(
            self._get_field_with_aliases(entry, "response_bytes", field_mapping)
        )
        response_time_ms = self._to_optional_int(
            self._get_field_with_aliases(entry, "response_time_ms", field_mapping)
        )
        referer = self._get_field_with_aliases(entry, "referer", field_mapping)
        protocol = self._get_field_with_aliases(entry, "protocol", field_mapping)
        ssl_protocol = self._get_field_with_aliases(
            entry, "ssl_protocol", field_mapping
        )
        cache_status = self._get_field_with_aliases(
            entry, "cache_status", field_mapping
        )
        edge_location = self._get_field_with_aliases(
            entry, "edge_location", field_mapping
        )

        # Collect unmapped extra fields
        mapped_fields = set()
        for universal_field in field_mapping:
            if universal_field in FIELD_ALIASES:
                mapped_fields.add(field_mapping[universal_field])
                mapped_fields.update(FIELD_ALIASES[universal_field])

        extra = {k: v for k, v in entry.items() if k not in mapped_fields and v}

        return IngestionRecord(
            timestamp=timestamp_dt,
            client_ip=str(client_ip),
            method=str(method),
            host=str(host) if host else None,
            path=str(path),
            status_code=status_code,
            user_agent=str(user_agent),
            query_string=str(query_string) if query_string else None,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
            response_time_ms=response_time_ms,
            referer=str(referer) if referer else None,
            protocol=str(protocol) if protocol else None,
            ssl_protocol=str(ssl_protocol) if ssl_protocol else None,
            cache_status=str(cache_status) if cache_status else None,
            edge_location=str(edge_location) if edge_location else None,
            extra=extra if extra else None,
        )

    def _get_field_with_aliases(
        self, entry: dict[str, Any], universal_field: str, field_mapping: dict[str, str]
    ) -> Optional[Any]:
        """
        Get a field value, trying the mapped name first, then common aliases.

        Args:
            entry: Log entry dict
            universal_field: Universal field name (e.g., "client_ip")
            field_mapping: Custom field mapping

        Returns:
            Field value or None
        """
        # Try mapped field name first
        mapped_name = field_mapping.get(universal_field)
        if mapped_name and mapped_name in entry:
            value = entry[mapped_name]
            if value is not None and value != "":
                return value

        # Try common aliases
        aliases = FIELD_ALIASES.get(universal_field, [])
        for alias in aliases:
            if alias in entry:
                value = entry[alias]
                if value is not None and value != "":
                    return value

        return None

    def _parse_timestamp(self, timestamp: Any) -> datetime:
        """
        Parse timestamp from various formats.

        Handles:
        - ISO 8601 format (with and without Z suffix)
        - Unix timestamps (integer or string)
        - Common datetime formats

        Args:
            timestamp: Timestamp value (string, int, or float)

        Returns:
            Timezone-aware datetime in UTC

        Raises:
            ValueError: If timestamp cannot be parsed
        """
        # Handle Unix timestamp
        if isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)

        timestamp_str = str(timestamp)

        # Handle Unix timestamp as string
        if timestamp_str.isdigit():
            return datetime.fromtimestamp(int(timestamp_str), tz=timezone.utc)

        # Handle 'Z' suffix (UTC)
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(timestamp_str)
        except ValueError:
            # Fallback for edge cases
            from dateutil import parser

            dt = parser.isoparse(timestamp_str)

        # Convert to UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt

    @staticmethod
    def _to_optional_int(value: Any) -> Optional[int]:
        """Convert value to integer or return None."""
        if value is None or value == "" or value == "-":
            return None
        try:
            return int(float(value))  # Handle "200.0" -> 200
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _ensure_utc(dt: datetime) -> datetime:
        """Ensure datetime is timezone-aware and in UTC."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
