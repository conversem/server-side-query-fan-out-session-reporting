"""
Azure CDN / Front Door adapter for log ingestion.

Supports ingestion from Azure CDN and Azure Front Door via:
- CSV exports from Azure Monitor
- JSON exports from Log Analytics
- NDJSON streaming exports

Field Mapping (Native Azure Front Door format):
    Azure Field                   -> Universal Schema Field
    Time                         -> timestamp (ISO 8601 datetime)
    ClientIp                     -> client_ip
    HttpMethod                   -> method
    HostName                     -> host
    RequestUri                   -> path + query_string (parsed from full URI)
    HttpStatusCode               -> status_code
    UserAgent                    -> user_agent
    ResponseBytes                -> response_bytes
    RequestBytes                 -> request_bytes
    TimeTaken                    -> response_time_ms (converted from seconds)
    CacheStatus                  -> cache_status
    Pop                          -> edge_location
    Referrer                     -> referer
    RequestProtocol              -> protocol
    SecurityProtocol             -> ssl_protocol

Field Mapping (Log Analytics AzureDiagnostics format with suffixes):
    TimeGenerated                -> timestamp
    clientIp_s                   -> client_ip
    requestMethod_s / httpMethod_s -> method
    hostName_s                   -> host
    requestUri_s                 -> path + query_string (parsed from full URI)
    httpStatusCode_d             -> status_code
    userAgent_s                  -> user_agent
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urlparse

from ....utils.bot_classifier import classify_bot
from ...base import IngestionAdapter, IngestionRecord, IngestionSource
from ...exceptions import ParseError, SourceValidationError
from ...parsers import parse_csv_file, parse_json_file, parse_ndjson_file
from ...registry import IngestionRegistry
from ...security import validate_path_safe

logger = logging.getLogger(__name__)


@IngestionRegistry.register("azure_cdn")
class AzureCDNAdapter(IngestionAdapter):
    """
    Azure CDN / Front Door adapter for log ingestion.

    Supports ingestion from Azure CDN and Azure Front Door via:
    - CSV exports from Azure Monitor - source_type: "csv_file"
    - JSON exports from Log Analytics - source_type: "json_file"
    - NDJSON streaming exports - source_type: "ndjson_file"

    The adapter automatically handles:
    - Two field naming conventions (native Front Door and Log Analytics export)
    - ISO 8601 and Unix timestamp formats
    - URI parsing (extracting path and query string from full URLs)
    - Bot filtering
    - Time-based filtering

    Example:
        source = IngestionSource(
            provider="azure_cdn",
            source_type="csv_file",
            path_or_uri="/path/to/azure-frontdoor-logs.csv",
        )
        adapter = AzureCDNAdapter()
        for record in adapter.ingest(source, filter_bots=True):
            print(record)
    """

    # Native Azure Front Door field mapping (direct export format)
    # RequestUri is mapped to "path" - will be parsed in post-processing to extract
    # the actual path and query_string from the full URL
    # Note: TimeTaken is NOT mapped here - it's in seconds and needs conversion
    # to milliseconds. We handle it separately via extra field.
    AZURE_NATIVE_FIELD_MAPPING = {
        "Time": "timestamp",
        "ClientIp": "client_ip",
        "HttpMethod": "method",
        "HostName": "host",
        "RequestUri": "path",  # Will be parsed for path/query in post-processing
        "HttpStatusCode": "status_code",
        "UserAgent": "user_agent",
        "ResponseBytes": "response_bytes",
        "RequestBytes": "request_bytes",
        # TimeTaken is stored in extra and converted to milliseconds in post-processing
        "CacheStatus": "cache_status",
        "Pop": "edge_location",
        "Referrer": "referer",
        "RequestProtocol": "protocol",
        "SecurityProtocol": "ssl_protocol",
    }

    # Log Analytics export field mapping (with _s and _d suffixes)
    # requestUri_s is mapped to "path" - will be parsed in post-processing
    AZURE_LOG_ANALYTICS_FIELD_MAPPING = {
        "TimeGenerated": "timestamp",
        "time": "timestamp",  # Alternative timestamp field
        "clientIp_s": "client_ip",
        "ClientIp": "client_ip",
        "requestMethod_s": "method",
        "httpMethod_s": "method",
        "HttpMethod": "method",
        "hostName_s": "host",
        "HostName": "host",
        "requestUri_s": "path",  # Will be parsed for path/query in post-processing
        "RequestUri": "path",
        "httpStatusCode_d": "status_code",
        "HttpStatusCode": "status_code",
        "userAgent_s": "user_agent",
        "UserAgent": "user_agent",
        "responseBytes_d": "response_bytes",
        "ResponseBytes": "response_bytes",
        "requestBytes_d": "request_bytes",
        "RequestBytes": "request_bytes",
        # TimeTaken/timeTaken_d is stored in extra and converted to milliseconds
        "cacheStatus_s": "cache_status",
        "CacheStatus": "cache_status",
        "pop_s": "edge_location",
        "Pop": "edge_location",
        "referrer_s": "referer",
        "Referrer": "referer",
        "requestProtocol_s": "protocol",
        "RequestProtocol": "protocol",
        "securityProtocol_s": "ssl_protocol",
        "SecurityProtocol": "ssl_protocol",
    }

    @property
    def provider_name(self) -> str:
        """Return the provider name identifier."""
        return "azure_cdn"

    @property
    def supported_source_types(self) -> list[str]:
        """Return list of supported source types."""
        return ["csv_file", "json_file", "ndjson_file"]

    def ingest(
        self,
        source: IngestionSource,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_bots: bool = True,
        **kwargs,
    ) -> Iterator[IngestionRecord]:
        """
        Ingest logs from Azure CDN / Front Door exports.

        Reads Azure log data from CSV, JSON, or NDJSON files
        and yields normalized IngestionRecord objects.

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

        path = Path(source.path_or_uri)

        # Determine if source is a file or directory
        if path.is_file():
            yield from self._ingest_file(
                source, path, start_time, end_time, filter_bots, strict_validation
            )
        elif path.is_dir():
            yield from self._ingest_directory(
                source, path, start_time, end_time, filter_bots, strict_validation
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

        Performs pre-flight checks to ensure the source can be read
        before attempting full ingestion. Includes path traversal protection.

        Args:
            source: Ingestion source configuration
            base_dir: Optional base directory to constrain file access within

        Returns:
            Tuple of (is_valid, error_message)
            - is_valid: True if source passes validation
            - error_message: Empty string if valid, error details if invalid
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

    def _ingest_file(
        self,
        source: IngestionSource,
        file_path: Path,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from a single Azure log file."""
        logger.info(f"Ingesting Azure CDN/Front Door logs from file: {file_path}")

        # Build combined field mapping for parsing
        field_mapping = self._get_combined_field_mapping()

        try:
            # Parse based on source type
            if source.source_type == "csv_file":
                records = parse_csv_file(
                    file_path, field_mapping, strict_validation=strict_validation
                )
            elif source.source_type == "json_file":
                records = parse_json_file(
                    file_path, field_mapping, strict_validation=strict_validation
                )
            elif source.source_type == "ndjson_file":
                records = parse_ndjson_file(
                    file_path, field_mapping, strict_validation=strict_validation
                )
            else:
                raise SourceValidationError(
                    f"Unsupported file source type: {source.source_type}",
                    source_type=source.source_type,
                )

            # Apply filters and post-process records
            for record in records:
                # Post-process: handle URI parsing if needed
                record = self._post_process_record(record)

                # Time filtering (inclusive range)
                if start_time is not None and record.timestamp < start_time:
                    continue
                if end_time is not None and record.timestamp > end_time:
                    continue

                # Bot filtering
                if filter_bots:
                    bot_info = classify_bot(record.user_agent)
                    if bot_info is None:
                        continue  # Skip non-bot records

                yield record

        except (ParseError, SourceValidationError):
            # Re-raise parsing and validation errors as-is
            raise
        except Exception as e:
            # Wrap other exceptions (IOError, etc.) in ParseError
            raise ParseError(
                f"Failed to parse Azure log file {file_path}: {e}",
            ) from e

    def _ingest_directory(
        self,
        source: IngestionSource,
        dir_path: Path,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from all matching log files in a directory."""
        logger.info(f"Ingesting Azure CDN/Front Door logs from directory: {dir_path}")

        matching_files = list(self._find_matching_files(dir_path, source.source_type))
        logger.info(f"Found {len(matching_files)} matching log files")

        for file_path in matching_files:
            try:
                yield from self._ingest_file(
                    source,
                    file_path,
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
        """
        Find all matching log files in directory.

        Args:
            dir_path: Directory to search
            source_type: Source type to determine file extensions

        Yields:
            Path objects for matching files
        """
        # Determine extensions based on source type
        if source_type == "csv_file":
            extensions = [".csv", ".csv.gz"]
        elif source_type == "json_file":
            extensions = [".json", ".json.gz"]
        elif source_type == "ndjson_file":
            extensions = [".ndjson", ".jsonl", ".ndjson.gz", ".jsonl.gz"]
        else:
            extensions = []

        # Recursively find matching files
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

    def _get_combined_field_mapping(self) -> dict[str, str]:
        """
        Get combined field mapping supporting both Azure formats.

        Returns:
            Dictionary mapping Azure field names to universal schema fields
        """
        # Start with native mapping
        mapping = dict(self.AZURE_NATIVE_FIELD_MAPPING)
        # Add Log Analytics mappings
        mapping.update(self.AZURE_LOG_ANALYTICS_FIELD_MAPPING)
        return mapping

    def _post_process_record(self, record: IngestionRecord) -> IngestionRecord:
        """
        Post-process a record to handle URI parsing and field normalization.

        Azure logs often have the full URI in requestUri/RequestUri.
        This needs to be parsed to extract path and query_string.

        Also converts TimeTaken from seconds (float) to milliseconds (int).
        TimeTaken is stored in the extra dict since the parsers would truncate
        float values to 0 when converting to int.

        Args:
            record: The parsed IngestionRecord

        Returns:
            Post-processed IngestionRecord with correct path/query_string
        """
        path = record.path
        query_string = record.query_string
        host = record.host
        response_time_ms = record.response_time_ms
        extra = dict(record.extra) if record.extra else {}

        # Check if path contains a full URI that needs parsing
        if path and ("://" in path or path.startswith("http")):
            try:
                parsed = urlparse(path)
                # Extract host from URL if not already set
                if not host and parsed.netloc:
                    host = parsed.netloc
                # Extract path
                path = parsed.path or "/"
                # Extract query string if not already set
                if not query_string and parsed.query:
                    query_string = parsed.query
            except Exception:
                # If URL parsing fails, use path as-is
                pass
        elif path and "?" in path and not query_string:
            # Path contains query string but wasn't detected as full URL
            parts = path.split("?", 1)
            path = parts[0] or "/"
            query_string = parts[1] if len(parts) > 1 else None

        # Ensure path starts with /
        if path and not path.startswith("/"):
            path = "/" + path

        # Default path if empty
        if not path:
            path = "/"

        # Convert TimeTaken from seconds to milliseconds
        # TimeTaken/timeTaken_d is stored in extra because the parsers would
        # truncate float values (e.g., 0.150) to 0 when converting to int
        time_taken_seconds = None
        for key in ("TimeTaken", "timeTaken_d"):
            if key in extra:
                try:
                    time_taken_seconds = float(extra[key])
                    del extra[key]  # Remove from extra after processing
                    break
                except (ValueError, TypeError):
                    pass

        if time_taken_seconds is not None:
            # Convert seconds to milliseconds
            response_time_ms = int(time_taken_seconds * 1000)
        elif response_time_ms is not None:
            # Fallback: if response_time_ms was somehow set, ensure it's an int
            response_time_ms = int(response_time_ms)

        # Return new record if any field changed
        if (
            path != record.path
            or query_string != record.query_string
            or host != record.host
            or response_time_ms != record.response_time_ms
            or extra != record.extra
        ):
            return IngestionRecord(
                timestamp=record.timestamp,
                client_ip=record.client_ip,
                method=record.method,
                host=host or record.host,
                path=path,
                status_code=record.status_code,
                user_agent=record.user_agent,
                query_string=query_string,
                response_bytes=record.response_bytes,
                request_bytes=record.request_bytes,
                response_time_ms=response_time_ms,
                cache_status=record.cache_status,
                edge_location=record.edge_location,
                referer=record.referer,
                protocol=record.protocol,
                ssl_protocol=record.ssl_protocol,
                extra=extra,
            )

        return record

    @staticmethod
    def _ensure_utc(dt: datetime) -> datetime:
        """
        Ensure datetime is timezone-aware and in UTC.

        Args:
            dt: Datetime object (may be naive or timezone-aware)

        Returns:
            Timezone-aware datetime in UTC

        Raises:
            ValueError: If datetime cannot be converted to UTC
        """
        if dt.tzinfo is None:
            # Assume naive datetime is UTC
            return dt.replace(tzinfo=timezone.utc)
        # Convert to UTC if in different timezone
        return dt.astimezone(timezone.utc)
