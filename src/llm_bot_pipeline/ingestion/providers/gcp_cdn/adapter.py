"""
Google Cloud CDN / HTTP(S) Load Balancer adapter for log ingestion.

Supports ingestion from GCP Cloud CDN via:
- JSON exports from Cloud Logging
- NDJSON streaming exports

Note: We don't use the standard parsers because GCP logs have nested httpRequest
structure that requires custom flattening logic. See _parse_gcp_json_file and
_parse_gcp_ndjson_file methods below.

Field Mapping (Cloud Logging httpRequest structure):
    GCP Field Path                  -> Universal Schema Field
    timestamp                       -> timestamp (RFC3339 format)
    httpRequest.remoteIp            -> client_ip
    httpRequest.requestMethod       -> method
    httpRequest.requestUrl          -> host + path + query_string (parsed)
    httpRequest.status              -> status_code
    httpRequest.userAgent           -> user_agent
    httpRequest.requestSize         -> request_bytes
    httpRequest.responseSize        -> response_bytes
    httpRequest.latency             -> response_time_ms (converted from "0.150s")
    httpRequest.cacheHit            -> cache_status (mapped to HIT/MISS)
    httpRequest.referer             -> referer
    httpRequest.protocol            -> protocol
    httpRequest.serverIp            -> edge_location (optional)
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Union
from urllib.parse import urlparse

from llm_bot_pipeline.ingestion.file_utils import open_file_auto_decompress

from ....utils.bot_classifier import classify_bot
from ...base import IngestionAdapter, IngestionRecord, IngestionSource
from ...exceptions import ParseError, SourceValidationError
from ...registry import IngestionRegistry
from ...security import validate_path_safe

logger = logging.getLogger(__name__)


@IngestionRegistry.register("gcp_cdn")
class GCPCDNAdapter(IngestionAdapter):
    """
    Google Cloud CDN adapter for log ingestion.

    Supports ingestion from GCP Cloud CDN and HTTP(S) Load Balancer via:
    - JSON exports from Cloud Logging - source_type: "json_file"
    - NDJSON streaming exports - source_type: "ndjson_file"

    The adapter automatically handles:
    - Nested httpRequest structure (flattening nested fields)
    - RFC3339 timestamp parsing with timezone handling
    - URL parsing to extract host, path, and query_string from requestUrl
    - Latency conversion from duration strings ("0.150s") to milliseconds
    - Cache status mapping from boolean cacheHit to HIT/MISS strings
    - Bot filtering
    - Time-based filtering

    Example:
        source = IngestionSource(
            provider="gcp_cdn",
            source_type="json_file",
            path_or_uri="/path/to/cloud-logging-export.json",
        )
        adapter = GCPCDNAdapter()
        for record in adapter.ingest(source, filter_bots=True):
            print(record)
    """

    # GCP Cloud Logging does not support flat field mapping because httpRequest
    # is a nested object. We'll parse records manually after loading JSON.
    # This mapping is used for documentation and to guide the flattening logic.
    NESTED_FIELD_MAPPING = {
        "timestamp": "timestamp",
        "httpRequest.remoteIp": "client_ip",
        "httpRequest.requestMethod": "method",
        "httpRequest.requestUrl": "request_url",  # Post-processed for host/path
        "httpRequest.status": "status_code",
        "httpRequest.userAgent": "user_agent",
        "httpRequest.requestSize": "request_bytes",
        "httpRequest.responseSize": "response_bytes",
        "httpRequest.latency": "latency",  # Post-processed to response_time_ms
        "httpRequest.cacheHit": "cache_hit",  # Post-processed to cache_status
        "httpRequest.referer": "referer",
        "httpRequest.protocol": "protocol",
        "httpRequest.serverIp": "edge_location",
    }

    @property
    def provider_name(self) -> str:
        """Return the provider name identifier."""
        return "gcp_cdn"

    @property
    def supported_source_types(self) -> list[str]:
        """Return list of supported source types."""
        return ["json_file", "ndjson_file"]

    def ingest(
        self,
        source: IngestionSource,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_bots: bool = True,
        **kwargs,
    ) -> Iterator[IngestionRecord]:
        """
        Ingest logs from GCP Cloud CDN / Cloud Logging exports.

        Reads GCP Cloud Logging data from JSON or NDJSON files
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
        """Ingest records from a single GCP Cloud Logging file."""
        logger.info(f"Ingesting GCP Cloud CDN logs from file: {file_path}")

        try:
            # GCP logs have nested httpRequest - we need to flatten before parsing
            # Use a custom approach: load raw JSON, flatten, then convert
            if source.source_type == "json_file":
                records_iter = self._parse_gcp_json_file(file_path, strict_validation)
            elif source.source_type == "ndjson_file":
                records_iter = self._parse_gcp_ndjson_file(file_path, strict_validation)
            else:
                raise SourceValidationError(
                    f"Unsupported file source type: {source.source_type}",
                    source_type=source.source_type,
                )

            # Apply filters and post-process records
            for record in records_iter:
                if record is None:
                    continue

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
                f"Failed to parse GCP log file {file_path}: {e}",
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
        logger.info(f"Ingesting GCP Cloud CDN logs from directory: {dir_path}")

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
        if source_type == "json_file":
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

    def _parse_gcp_json_file(
        self, file_path: Path, strict_validation: bool
    ) -> Iterator[Optional[IngestionRecord]]:
        """
        Parse GCP Cloud Logging JSON file with nested httpRequest.

        Supports both regular and gzip-compressed files.

        Args:
            file_path: Path to JSON file
            strict_validation: If True, raise on invalid records

        Yields:
            IngestionRecord objects
        """
        try:
            with open_file_auto_decompress(file_path) as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ParseError(f"Invalid JSON in {file_path}: {e}") from e

        # Handle both array and single object
        if isinstance(data, dict):
            data = [data]

        for idx, entry in enumerate(data):
            try:
                record = self._convert_gcp_entry(entry)
                if record is not None:
                    yield record
            except Exception as e:
                if strict_validation:
                    raise ParseError(
                        f"Failed to parse entry {idx} in {file_path}: {e}"
                    ) from e
                logger.debug(f"Skipping invalid entry {idx}: {e}")
                continue

    def _parse_gcp_ndjson_file(
        self, file_path: Path, strict_validation: bool
    ) -> Iterator[Optional[IngestionRecord]]:
        """
        Parse GCP Cloud Logging NDJSON file with nested httpRequest.

        Supports both regular and gzip-compressed files.

        Args:
            file_path: Path to NDJSON file
            strict_validation: If True, raise on invalid records

        Yields:
            IngestionRecord objects
        """
        try:
            with open_file_auto_decompress(file_path) as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        record = self._convert_gcp_entry(entry)
                        if record is not None:
                            yield record
                    except json.JSONDecodeError as e:
                        if strict_validation:
                            raise ParseError(
                                f"Invalid JSON at line {line_num} in {file_path}: {e}"
                            ) from e
                        logger.debug(f"Skipping invalid JSON at line {line_num}: {e}")
                        continue
                    except Exception as e:
                        if strict_validation:
                            raise ParseError(
                                f"Failed to parse line {line_num} in {file_path}: {e}"
                            ) from e
                        logger.debug(f"Skipping invalid entry at line {line_num}: {e}")
                        continue
        except (OSError, IOError) as e:
            raise ParseError(f"Failed to read {file_path}: {e}") from e

    def _convert_gcp_entry(self, entry: dict[str, Any]) -> Optional[IngestionRecord]:
        """
        Convert a GCP Cloud Logging entry to IngestionRecord.

        Args:
            entry: Raw JSON entry from Cloud Logging

        Returns:
            IngestionRecord or None if required fields are missing
        """
        # Extract httpRequest object
        http_request = entry.get("httpRequest", {})
        if not http_request:
            logger.debug("Skipping entry without httpRequest")
            return None

        # Parse timestamp (required)
        timestamp_str = entry.get("timestamp")
        if not timestamp_str:
            logger.debug("Skipping entry without timestamp")
            return None

        try:
            timestamp = self._parse_rfc3339_timestamp(timestamp_str)
        except ValueError as e:
            logger.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

        # Extract required fields from httpRequest
        client_ip = http_request.get("remoteIp")
        method = http_request.get("requestMethod")
        request_url = http_request.get("requestUrl")
        status_code = http_request.get("status")
        user_agent = http_request.get("userAgent")

        # Validate required fields
        if not all([client_ip, method, status_code]):
            logger.debug(
                "Skipping entry missing required fields "
                f"(client_ip={client_ip}, method={method}, status_code={status_code})"
            )
            return None

        # Parse URL to extract host, path, query_string
        host = None
        path = "/"
        query_string = None

        if request_url:
            try:
                parsed_url = urlparse(request_url)
                host = parsed_url.netloc or None
                path = parsed_url.path or "/"
                query_string = parsed_url.query or None
            except Exception:
                # If URL parsing fails, try to extract path directly
                path = request_url

        # Ensure path starts with /
        if path and not path.startswith("/"):
            path = "/" + path

        # Convert status_code to int
        try:
            status_code = int(status_code)
        except (ValueError, TypeError):
            logger.debug(f"Invalid status_code: {status_code}")
            return None

        # Extract optional fields
        request_bytes = self._to_optional_int(http_request.get("requestSize"))
        response_bytes = self._to_optional_int(http_request.get("responseSize"))
        referer = http_request.get("referer")
        protocol = http_request.get("protocol")
        edge_location = http_request.get("serverIp")

        # Parse latency to milliseconds
        response_time_ms = self._parse_latency(http_request.get("latency"))

        # Map cacheHit boolean to cache_status string
        cache_status = self._map_cache_status(
            http_request.get("cacheHit"), http_request.get("cacheLookup")
        )

        # Collect extra fields
        extra = {}
        for key in ("insertId", "trace", "spanId", "severity", "logName"):
            if key in entry:
                extra[key] = entry[key]

        # Include resource labels if present
        resource = entry.get("resource", {})
        if resource.get("labels"):
            extra["resource_labels"] = resource["labels"]

        return IngestionRecord(
            timestamp=timestamp,
            client_ip=client_ip,
            method=method,
            host=host,
            path=path,
            status_code=status_code,
            user_agent=user_agent or "",
            query_string=query_string,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
            response_time_ms=response_time_ms,
            cache_status=cache_status,
            edge_location=edge_location,
            referer=referer,
            protocol=protocol,
            extra=extra if extra else None,
        )

    def _parse_rfc3339_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse RFC3339/ISO8601 timestamp string to datetime.

        Args:
            timestamp_str: Timestamp string (e.g., "2024-01-15T12:30:45.123456Z")

        Returns:
            Timezone-aware datetime in UTC

        Raises:
            ValueError: If timestamp cannot be parsed
        """
        # Handle 'Z' suffix (UTC)
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"

        try:
            # Python 3.11+ supports fromisoformat with timezone
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

    def _parse_latency(self, latency_str: Optional[str]) -> Optional[int]:
        """
        Parse GCP latency string to milliseconds.

        Args:
            latency_str: Latency string (e.g., "0.150s")

        Returns:
            Latency in milliseconds or None
        """
        if not latency_str:
            return None

        try:
            if isinstance(latency_str, str) and latency_str.endswith("s"):
                seconds = float(latency_str.rstrip("s"))
                return int(seconds * 1000)
            elif isinstance(latency_str, (int, float)):
                # Assume already in seconds if numeric
                return int(float(latency_str) * 1000)
        except (ValueError, TypeError):
            pass

        return None

    def _map_cache_status(
        self, cache_hit: Optional[bool], cache_lookup: Optional[bool]
    ) -> Optional[str]:
        """
        Map GCP cache boolean fields to cache status string.

        Args:
            cache_hit: Whether the request was a cache hit
            cache_lookup: Whether a cache lookup was performed

        Returns:
            Cache status string ("HIT", "MISS", "BYPASS") or None
        """
        if cache_hit is True:
            return "HIT"
        elif cache_hit is False and cache_lookup is True:
            return "MISS"
        elif cache_lookup is False:
            return "BYPASS"
        return None

    @staticmethod
    def _to_optional_int(value: Any) -> Optional[int]:
        """
        Convert value to integer or return None.

        Args:
            value: Value to convert (string, int, float, or None)

        Returns:
            Integer value or None
        """
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

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
