"""
AWS Application Load Balancer (ALB) adapter for log ingestion.

Supports ingestion from AWS ALB access logs via:
- Space-separated log files (.log)
- Gzip-compressed log files (.log.gz)

The adapter uses shlex for parsing space-separated fields with quoted strings.

Field Mapping (1-indexed as per AWS docs):
    ALB Field Position              -> Universal Schema Field
    Field 2 (time)                  -> timestamp (ISO 8601 format)
    Field 4 (client:port)           -> client_ip (extract IP before colon)
    Field 9 (elb_status_code)       -> status_code
    Field 13 ("request")            -> method, host, path, query_string (parse HTTP line)
    Field 14 ("user_agent")         -> user_agent
    Field 11 (received_bytes)       -> request_bytes (optional)
    Field 12 (sent_bytes)           -> response_bytes (optional)
    Field 16 (ssl_protocol)         -> ssl_protocol (optional)
    Fields 6+7+8 (processing times) -> response_time_ms (sum × 1000)
"""

import logging
import shlex
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


@IngestionRegistry.register("aws_alb")
class ALBAdapter(IngestionAdapter):
    """
    AWS Application Load Balancer adapter for log ingestion.

    Supports ingestion from AWS ALB access logs via:
    - Space-separated log files - source_type: "alb_log_file"
    - Gzip-compressed log files (.log.gz)

    The adapter automatically handles:
    - Space-separated parsing with shlex for quoted fields
    - HTTP request line parsing to extract method, host, path, query_string
    - Client:port field parsing to extract client IP
    - ISO 8601 timestamp parsing
    - Processing time conversion to milliseconds
    - Bot filtering
    - Time-based filtering

    Example:
        source = IngestionSource(
            provider="aws_alb",
            source_type="alb_log_file",
            path_or_uri="/path/to/alb-access.log.gz",
        )
        adapter = ALBAdapter()
        for record in adapter.ingest(source, filter_bots=True):
            print(record)
    """

    # Field positions (0-indexed) - maps to 1-indexed positions in AWS docs
    # AWS Position 2 = Index 1 (time)
    # AWS Position 4 = Index 3 (client:port)
    # AWS Position 9 = Index 8 (elb_status_code)
    # AWS Position 13 = Index 12 (request - quoted)
    # AWS Position 14 = Index 13 (user_agent - quoted)
    FIELD_POSITIONS = {
        "type": 0,
        "time": 1,
        "elb": 2,
        "client_port": 3,
        "target_port": 4,
        "request_processing_time": 5,
        "target_processing_time": 6,
        "response_processing_time": 7,
        "elb_status_code": 8,
        "target_status_code": 9,
        "received_bytes": 10,
        "sent_bytes": 11,
        "request": 12,
        "user_agent": 13,
        "ssl_cipher": 14,
        "ssl_protocol": 15,
        "target_group_arn": 16,
    }

    # Minimum number of fields expected in a valid ALB log line
    MIN_FIELD_COUNT = 17

    @property
    def provider_name(self) -> str:
        """Return the provider name identifier."""
        return "aws_alb"

    @property
    def supported_source_types(self) -> list[str]:
        """Return list of supported source types."""
        return ["alb_log_file"]

    def ingest(
        self,
        source: IngestionSource,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_bots: bool = True,
        **kwargs,
    ) -> Iterator[IngestionRecord]:
        """
        Ingest logs from AWS ALB access log files.

        Reads ALB access logs from space-separated log files
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

    def _ingest_file(
        self,
        source: IngestionSource,
        file_path: Path,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from a single ALB log file."""
        logger.info(f"Ingesting AWS ALB logs from file: {file_path}")

        try:
            with open_file_auto_decompress(file_path) as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = self._parse_alb_line(line)
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

                    except Exception as e:
                        if strict_validation:
                            raise ParseError(
                                f"Failed to parse line {line_num} in {file_path}: {e}"
                            ) from e
                        logger.debug(f"Skipping invalid line {line_num}: {e}")
                        continue

        except (ParseError, SourceValidationError):
            raise
        except Exception as e:
            raise ParseError(
                f"Failed to parse ALB log file {file_path}: {e}",
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
        logger.info(f"Ingesting AWS ALB logs from directory: {dir_path}")

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
        """Find all matching log files in directory."""
        # ALB log file extensions
        extensions = [".log", ".log.gz", ".txt", ".txt.gz"]

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

    def _parse_alb_line(self, line: str) -> Optional[IngestionRecord]:
        """
        Parse a single ALB log line.

        Args:
            line: Raw log line from ALB access log

        Returns:
            IngestionRecord or None if line is invalid/malformed
        """
        try:
            # Use shlex to parse space-separated fields with quoted strings
            fields = shlex.split(line)
        except ValueError as e:
            logger.debug(f"Failed to parse line with shlex: {e}")
            return None

        # Validate minimum field count
        if len(fields) < self.MIN_FIELD_COUNT:
            logger.debug(
                f"Line has {len(fields)} fields, expected at least {self.MIN_FIELD_COUNT}"
            )
            return None

        # Extract timestamp (Field 2, index 1)
        timestamp_str = fields[self.FIELD_POSITIONS["time"]]
        try:
            timestamp = self._parse_timestamp(timestamp_str)
        except ValueError as e:
            logger.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return None

        # Extract client IP from client:port (Field 4, index 3)
        client_port = fields[self.FIELD_POSITIONS["client_port"]]
        client_ip = self._extract_client_ip(client_port)
        if not client_ip:
            logger.debug(f"Failed to extract client IP from '{client_port}'")
            return None

        # Extract status code (Field 9, index 8)
        status_code_str = fields[self.FIELD_POSITIONS["elb_status_code"]]
        status_code = self._to_optional_int(status_code_str)
        if status_code is None:
            logger.debug(f"Invalid status code: {status_code_str}")
            return None

        # Parse HTTP request line (Field 13, index 12)
        request_line = fields[self.FIELD_POSITIONS["request"]]
        method, host, path, query_string, protocol = self._parse_request_line(
            request_line
        )
        if method is None:
            logger.debug(f"Failed to parse request line: {request_line}")
            return None

        # Extract user agent (Field 14, index 13)
        user_agent = fields[self.FIELD_POSITIONS["user_agent"]]
        if user_agent == "-":
            user_agent = ""

        # Extract optional fields
        request_bytes = self._to_optional_int(
            fields[self.FIELD_POSITIONS["received_bytes"]]
        )
        response_bytes = self._to_optional_int(
            fields[self.FIELD_POSITIONS["sent_bytes"]]
        )
        ssl_protocol = fields[self.FIELD_POSITIONS["ssl_protocol"]]
        if ssl_protocol == "-":
            ssl_protocol = None

        # Calculate response time from processing times (sum × 1000 for ms)
        response_time_ms = self._calculate_response_time(fields)

        # Collect extra fields (ALB-specific metadata)
        extra: dict[str, Any] = {}

        # Request type (http/https/h2/grpcs/ws/wss)
        request_type = fields[self.FIELD_POSITIONS["type"]]
        if request_type and request_type != "-":
            extra["type"] = request_type

        # Load balancer identifier
        elb = fields[self.FIELD_POSITIONS["elb"]]
        if elb and elb != "-":
            extra["elb"] = elb

        # Target group ARN
        target_group_arn = fields[self.FIELD_POSITIONS["target_group_arn"]]
        if target_group_arn and target_group_arn != "-":
            extra["target_group_arn"] = target_group_arn

        # Trace ID (if available, position 17 after target_group_arn)
        if len(fields) > 17:
            trace_id = fields[17]
            if trace_id and trace_id != "-":
                extra["trace_id"] = trace_id

        return IngestionRecord(
            timestamp=timestamp,
            client_ip=client_ip,
            method=method,
            host=host,
            path=path,
            status_code=status_code,
            user_agent=user_agent,
            query_string=query_string,
            request_bytes=request_bytes,
            response_bytes=response_bytes,
            response_time_ms=response_time_ms,
            ssl_protocol=ssl_protocol,
            protocol=protocol,
            extra=extra if extra else None,
        )

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """
        Parse ISO 8601 timestamp from ALB log.

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

    def _extract_client_ip(self, client_port: str) -> Optional[str]:
        """
        Extract client IP from client:port field.

        Handles both IPv4 and IPv6 addresses:
        - IPv4: 192.0.2.100:54321 -> 192.0.2.100
        - IPv6: [2001:db8::1]:54321 -> 2001:db8::1

        Args:
            client_port: Client:port string

        Returns:
            Client IP address or None
        """
        if not client_port or client_port == "-":
            return None

        # Handle IPv6 addresses (bracketed format)
        if client_port.startswith("["):
            # [2001:db8::1]:54321
            bracket_end = client_port.find("]")
            if bracket_end != -1:
                return client_port[1:bracket_end]
            return None

        # Handle IPv4 addresses (simple colon split)
        # Use rsplit to handle edge case of IPv6 without brackets
        parts = client_port.rsplit(":", 1)
        if len(parts) >= 1:
            return parts[0]

        return None

    def _parse_request_line(
        self, request_line: str
    ) -> tuple[Optional[str], Optional[str], str, Optional[str], Optional[str]]:
        """
        Parse HTTP request line to extract method, host, path, query_string, protocol.

        Format: "METHOD URL HTTP/VERSION"
        Example: "GET https://example.com/api/data?key=value HTTP/1.1"

        Args:
            request_line: HTTP request line

        Returns:
            Tuple of (method, host, path, query_string, protocol)
            Returns (None, None, "/", None, None) for invalid request lines
        """
        # Handle malformed request
        if not request_line or request_line == "- - -":
            return (None, None, "/", None, None)

        parts = request_line.split(" ")
        if len(parts) < 2:
            return (None, None, "/", None, None)

        method = parts[0]
        if method == "-":
            return (None, None, "/", None, None)

        url = parts[1]
        if url == "-":
            return (method, None, "/", None, None)

        # Extract protocol (HTTP version) from parts[2] if present
        protocol = None
        if len(parts) >= 3 and parts[2] not in ("-", ""):
            protocol = parts[2]

        # Parse URL to extract components
        try:
            parsed = urlparse(url)
            host = parsed.netloc or None
            path = parsed.path or "/"
            query_string = parsed.query or None

            # Ensure path starts with /
            if path and not path.startswith("/"):
                path = "/" + path

            return (method, host, path, query_string, protocol)
        except Exception:
            # If URL parsing fails, treat as path
            return (
                method,
                None,
                url if url.startswith("/") else "/" + url,
                None,
                protocol,
            )

    def _calculate_response_time(self, fields: list[str]) -> Optional[int]:
        """
        Calculate total response time from processing times.

        Sums request_processing_time + target_processing_time + response_processing_time
        and converts from seconds to milliseconds.

        Args:
            fields: Parsed log fields

        Returns:
            Response time in milliseconds or None
        """
        try:
            times = []
            for pos in [
                "request_processing_time",
                "target_processing_time",
                "response_processing_time",
            ]:
                value = fields[self.FIELD_POSITIONS[pos]]
                if value != "-" and value != "-1":
                    times.append(float(value))

            if times:
                total_seconds = sum(times)
                return int(total_seconds * 1000)
        except (ValueError, TypeError, IndexError):
            pass

        return None

    @staticmethod
    def _to_optional_int(value: Any) -> Optional[int]:
        """Convert value to integer or return None."""
        if value is None or value == "-":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _ensure_utc(dt: datetime) -> datetime:
        """Ensure datetime is timezone-aware and in UTC."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
