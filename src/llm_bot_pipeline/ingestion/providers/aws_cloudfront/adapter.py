"""
AWS CloudFront adapter for W3C extended log format ingestion.

Supports ingestion from AWS CloudFront access logs exported to S3.
CloudFront logs use W3C extended log format with tab-separated values.

Field Mapping:
    CloudFront W3C Field          -> Universal Schema Field
    date + time                  -> timestamp
    c-ip                         -> client_ip
    cs-method                    -> method
    cs(Host)                     -> host
    cs-uri-stem                  -> path
    cs-uri-query                 -> query_string
    sc-status                    -> status_code
    cs(User-Agent)               -> user_agent
    sc-bytes                     -> response_bytes
    cs-bytes                     -> request_bytes
    time-taken                   -> response_time_ms
    x-edge-result-type           -> cache_status
    x-edge-location              -> edge_location
    cs(Referer)                  -> referer
    cs-protocol                  -> protocol
    ssl-protocol                 -> ssl_protocol
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from ....utils.bot_classifier import classify_bot
from ...base import IngestionAdapter, IngestionRecord, IngestionSource
from ...exceptions import ParseError, SourceValidationError
from ...parsers import parse_w3c_file
from ...registry import IngestionRegistry
from ...security import validate_path_safe

logger = logging.getLogger(__name__)


@IngestionRegistry.register("aws_cloudfront")
class CloudFrontAdapter(IngestionAdapter):
    """
    AWS CloudFront adapter for W3C extended log format.

    Supports ingestion from AWS CloudFront access logs exported to S3.
    CloudFront logs use W3C extended log format with tab-separated values.

    Supported source types:
        - w3c_file: W3C extended log format file (tab-separated)

    The adapter automatically handles:
    - Gzip compression (.gz files)
    - Directory processing (recursive file discovery)
    - Time-based filtering
    - Bot filtering

    Example:
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri="/path/to/cloudfront-logs/",
        )
        adapter = CloudFrontAdapter()
        for record in adapter.ingest(source, filter_bots=True):
            print(record)
    """

    # CloudFront W3C field to universal schema field mapping
    CLOUDFRONT_FIELD_MAPPING = {
        "date": "date",
        "time": "time",
        "c-ip": "client_ip",
        "cs-method": "method",
        "cs(Host)": "host",
        "cs-uri-stem": "path",
        "cs-uri-query": "query_string",
        "sc-status": "status_code",
        "cs(User-Agent)": "user_agent",
        "sc-bytes": "response_bytes",
        "cs-bytes": "request_bytes",
        "time-taken": "response_time_ms",
        "x-edge-result-type": "cache_status",
        "x-edge-location": "edge_location",
        "cs(Referer)": "referer",
        "cs-protocol": "protocol",
        "ssl-protocol": "ssl_protocol",
    }

    @property
    def provider_name(self) -> str:
        """Return the provider name identifier."""
        return "aws_cloudfront"

    @property
    def supported_source_types(self) -> list[str]:
        """Return list of supported source types."""
        return ["w3c_file"]

    def ingest(
        self,
        source: IngestionSource,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_bots: bool = True,
        **kwargs,
    ) -> Iterator[IngestionRecord]:
        """
        Ingest logs from AWS CloudFront W3C format files.

        Reads CloudFront log data from W3C extended log format files
        and yields normalized IngestionRecord objects. Supports both
        single files and directories.

        Args:
            source: Ingestion source configuration
            start_time: Optional start time filter (UTC)
            end_time: Optional end time filter (UTC)
            filter_bots: If True, only yield records from known LLM bots
            **kwargs: Additional options:
                - strict_validation: If True, reject invalid records (default: False)
                - url_decode: If True, URL-decode fields (default: True)

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

        path = Path(source.path_or_uri)
        strict_validation = kwargs.get("strict_validation", False)
        url_decode = kwargs.get("url_decode", True)

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

        # Determine if source is a file or directory
        if path.is_file():
            yield from self._ingest_file(
                path, start_time, end_time, filter_bots, strict_validation, url_decode
            )
        elif path.is_dir():
            yield from self._ingest_directory(
                path, start_time, end_time, filter_bots, strict_validation, url_decode
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
            matching_files = list(self._find_matching_files(path))
            if not matching_files:
                return (
                    False,
                    f"No matching W3C log files found in directory: {path}",
                )

        return (True, "")

    def _ingest_file(
        self,
        file_path: Path,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
        url_decode: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from a single W3C log file."""
        logger.info(f"Ingesting CloudFront logs from file: {file_path}")

        try:
            # Use W3C parser with CloudFront field mapping
            records = parse_w3c_file(
                file_path,
                self.CLOUDFRONT_FIELD_MAPPING,
                url_decode=url_decode,
                strict_validation=strict_validation,
            )

            # Apply filters
            for record in records:
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
                f"Failed to parse CloudFront log file {file_path}: {e}",
            ) from e

    def _ingest_directory(
        self,
        dir_path: Path,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
        url_decode: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from all matching W3C log files in a directory."""
        logger.info(f"Ingesting CloudFront logs from directory: {dir_path}")

        matching_files = list(self._find_matching_files(dir_path))
        logger.info(f"Found {len(matching_files)} matching W3C log files")

        for file_path in matching_files:
            try:
                yield from self._ingest_file(
                    file_path,
                    start_time,
                    end_time,
                    filter_bots,
                    strict_validation,
                    url_decode,
                )
            except Exception as e:
                logger.warning(f"Failed to ingest {file_path}: {e}")
                if strict_validation:
                    raise
                continue

    def _find_matching_files(self, dir_path: Path) -> Iterator[Path]:
        """
        Find all W3C log files in directory.

        Recursively searches for W3C log files with matching extensions,
        including gzip-compressed variants. Handles errors gracefully.

        Args:
            dir_path: Directory to search

        Yields:
            Path objects for matching files

        Raises:
            PermissionError: If directory is not readable
        """
        # CloudFront log file extensions (including gzip variants)
        extensions = [".log", ".log.gz", ".txt", ".txt.gz"]

        # Recursively find matching files
        seen = set()  # Track seen files to avoid duplicates
        try:
            for ext in extensions:
                for file_path in dir_path.rglob(f"*{ext}"):
                    # Use resolved path to avoid duplicates (handles symlinks)
                    # Handle case where file might be deleted between listing and resolving
                    try:
                        resolved_path = file_path.resolve()
                        if resolved_path not in seen:
                            seen.add(resolved_path)
                            yield file_path
                    except (OSError, RuntimeError):
                        # File deleted or symlink broken, skip it
                        logger.debug(f"Skipping inaccessible file: {file_path}")
                        continue
        except PermissionError:
            logger.error(f"Permission denied accessing directory: {dir_path}")
            raise
        except Exception as e:
            logger.warning(f"Error searching directory {dir_path}: {e}")
            raise

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
