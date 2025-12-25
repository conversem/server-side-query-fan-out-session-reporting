"""
Cloudflare adapter for Logpull API and Logpush file imports.

Supports ingestion from Cloudflare via:
- Logpull API (real-time, 7-day retention)
- Logpush CSV/JSON file exports

Field Mapping:
    Cloudflare Field              -> Universal Schema Field
    EdgeStartTimestamp            -> timestamp (nanoseconds to datetime)
    ClientIP                      -> client_ip
    ClientRequestMethod           -> method
    ClientRequestHost             -> host
    ClientRequestURI (path)       -> path
    ClientRequestURI (query)      -> query_string
    EdgeResponseStatus            -> status_code
    ClientRequestUserAgent        -> user_agent
    EdgeResponseBytes             -> response_bytes
    ClientRequestBytes            -> request_bytes
    OriginResponseTime            -> response_time_ms
    CacheCacheStatus              -> cache_status
    EdgeColoCode                  -> edge_location
    ClientRequestReferer          -> referer
    ClientRequestProtocol         -> protocol
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urlparse

from ....cloudflare.logpull import pull_logs
from ....config.settings import get_settings
from ....utils.bot_classifier import classify_bot
from ...base import IngestionAdapter, IngestionRecord, IngestionSource
from ...exceptions import ParseError, SourceValidationError
from ...parsers import parse_csv_file, parse_json_file, parse_ndjson_file
from ...registry import IngestionRegistry
from ...security import check_rate_limit, validate_path_safe

logger = logging.getLogger(__name__)


@IngestionRegistry.register("cloudflare")
class CloudflareAdapter(IngestionAdapter):
    """
    Cloudflare adapter for Logpull API and Logpush file imports.

    Supports ingestion from Cloudflare via:
    - Logpull API (real-time, 7-day retention) - source_type: "api"
    - Logpush CSV/JSON file exports - source_type: "csv_file", "json_file", "ndjson_file"

    The adapter automatically handles:
    - Timestamp conversion (nanoseconds to datetime)
    - URI parsing (path and query string extraction)
    - Bot filtering
    - Time-based filtering

    Example (API):
        source = IngestionSource(
            provider="cloudflare",
            source_type="api",
            path_or_uri="api://zone_id",  # or use settings
        )
        adapter = CloudflareAdapter()
        for record in adapter.ingest(source, start_time=..., end_time=..., filter_bots=True):
            print(record)

    Example (File):
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri="/path/to/logpush.csv",
        )
        adapter = CloudflareAdapter()
        for record in adapter.ingest(source, filter_bots=True):
            print(record)
    """

    # Cloudflare field to universal schema field mapping
    CLOUDFLARE_FIELD_MAPPING = {
        "EdgeStartTimestamp": "timestamp",
        "ClientIP": "client_ip",
        "ClientRequestMethod": "method",
        "ClientRequestHost": "host",
        "ClientRequestURI": "request_uri",  # Will be split into path/query
        "EdgeResponseStatus": "status_code",
        "ClientRequestUserAgent": "user_agent",
        "EdgeResponseBytes": "response_bytes",
        "ClientRequestBytes": "request_bytes",
        "OriginResponseTime": "response_time_ms",
        "CacheCacheStatus": "cache_status",
        "EdgeColoCode": "edge_location",
        "ClientRequestReferer": "referer",
        "ClientRequestProtocol": "protocol",
    }

    @property
    def provider_name(self) -> str:
        """Return the provider name identifier."""
        return "cloudflare"

    @property
    def supported_source_types(self) -> list[str]:
        """Return list of supported source types."""
        return ["api", "csv_file", "json_file", "ndjson_file"]

    def ingest(
        self,
        source: IngestionSource,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_bots: bool = True,
        **kwargs,
    ) -> Iterator[IngestionRecord]:
        """
        Ingest logs from Cloudflare API or file exports.

        Supports both Logpull API (real-time) and Logpush file exports.

        Args:
            source: Ingestion source configuration
            start_time: Optional start time filter (UTC)
            end_time: Optional end time filter (UTC)
            filter_bots: If True, only yield records from known LLM bots
            **kwargs: Additional options:
                - strict_validation: If True, reject invalid records (default: False)
                - zone_id: Cloudflare zone ID (for API source, uses settings if None)
                - filter_verified_bots: If True, only return verified bot traffic (API only)

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

        # Route to appropriate ingestion method
        if source.source_type == "api":
            yield from self._ingest_api(
                source, start_time, end_time, filter_bots, **kwargs
            )
        elif source.source_type in ["csv_file", "json_file", "ndjson_file"]:
            yield from self._ingest_file(
                source, start_time, end_time, filter_bots, strict_validation
            )
        else:
            raise SourceValidationError(
                f"Unsupported source type: {source.source_type}",
                source_type=source.source_type,
            )

    def validate_source(
        self, source: IngestionSource, base_dir: Optional[Path] = None
    ) -> tuple[bool, str]:
        """
        Validate that the source is accessible and properly configured.

        Performs pre-flight checks to ensure the source can be used
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

        if source.source_type == "api":
            # For API, check that settings are configured
            try:
                settings = get_settings()
                if not settings.cloudflare_api_token:
                    return (False, "Cloudflare API token not configured")
                if not settings.cloudflare_zone_id:
                    return (False, "Cloudflare zone ID not configured")
            except Exception as e:
                return (False, f"Failed to load settings: {e}")

        elif source.source_type in ["csv_file", "json_file", "ndjson_file"]:
            # For file sources, check path exists
            path = Path(source.path_or_uri)

            # Security: Validate path is safe from traversal attacks
            is_safe, security_error = validate_path_safe(
                path, base_dir=base_dir, allow_symlinks=True
            )
            if not is_safe:
                return (False, f"Security validation failed: {security_error}")

            if not path.exists():
                return (False, f"Path does not exist: {path}")

            if path.is_file():
                try:
                    if path.stat().st_size == 0:
                        return (False, f"File is empty: {path}")
                except (OSError, PermissionError) as e:
                    return (False, f"Cannot access file {path}: {e}")

        return (True, "")

    def _ingest_api(
        self,
        source: IngestionSource,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        **kwargs,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from Cloudflare Logpull API."""
        logger.info("Ingesting Cloudflare logs from API")

        # Check rate limit before API call (100 requests per minute default)
        max_requests = kwargs.get("rate_limit_requests", 100)
        window_seconds = kwargs.get("rate_limit_window", 60.0)

        if not check_rate_limit(
            "cloudflare_api", max_requests=max_requests, window_seconds=window_seconds
        ):
            raise SourceValidationError(
                "Cloudflare API rate limit exceeded. Please wait before retrying.",
                source_type=source.source_type,
                reason="rate_limit_exceeded",
            )

        # Extract zone_id from path_or_uri if provided (format: "api://zone_id")
        zone_id = None
        if source.path_or_uri and source.path_or_uri.startswith("api://"):
            zone_id = source.path_or_uri.replace("api://", "")

        # Get filter options
        filter_verified_bots = kwargs.get("filter_verified_bots", True)
        filter_llm_bots = filter_bots  # Use filter_bots parameter for LLM bot filtering

        # Require explicit time range for API sources (no defaults)
        # This prevents accidental large data pulls
        if start_time is None or end_time is None:
            raise ValueError(
                "start_time and end_time are required for Cloudflare API source. "
                "For file sources, time filtering is optional."
            )

        try:
            # Pull logs from API
            for cloudflare_record in pull_logs(
                start_time=start_time,
                end_time=end_time,
                zone_id=zone_id,
                filter_verified_bots=filter_verified_bots,
                filter_llm_bots=filter_llm_bots,
            ):
                # Convert Cloudflare record to IngestionRecord
                record = self._convert_cloudflare_record(cloudflare_record)
                if record:
                    yield record

        except ValueError:
            # Re-raise ValueError (e.g., retention limit exceeded) as-is
            raise
        except Exception as e:
            # Wrap other exceptions in ParseError
            raise ParseError(
                f"Failed to pull logs from Cloudflare API: {e}",
            ) from e

    def _ingest_file(
        self,
        source: IngestionSource,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from Cloudflare Logpush file exports."""
        logger.info(f"Ingesting Cloudflare logs from file: {source.path_or_uri}")

        path = Path(source.path_or_uri)

        # Create field mapping for file parsing
        # Cloudflare Logpush files may use different field names
        field_mapping = self._get_file_field_mapping()

        try:
            # Parse based on source type
            if source.source_type == "csv_file":
                records = parse_csv_file(
                    path, field_mapping, strict_validation=strict_validation
                )
            elif source.source_type == "json_file":
                records = parse_json_file(
                    path, field_mapping, strict_validation=strict_validation
                )
            elif source.source_type == "ndjson_file":
                records = parse_ndjson_file(
                    path, field_mapping, strict_validation=strict_validation
                )
            else:
                raise SourceValidationError(
                    f"Unsupported file source type: {source.source_type}",
                    source_type=source.source_type,
                )

            # Apply filters and post-process records
            for record in records:
                # Post-process: if path contains query string (from URI field), split it
                # This handles cases where Logpush files map URI to path field
                if record.path and "?" in record.path and not record.query_string:
                    parsed = urlparse(record.path)
                    # Create new record with split path/query (dataclass is immutable)
                    record = IngestionRecord(
                        timestamp=record.timestamp,
                        client_ip=record.client_ip,
                        method=record.method,
                        host=record.host,
                        path=parsed.path or "/",
                        status_code=record.status_code,
                        user_agent=record.user_agent,
                        query_string=parsed.query if parsed.query else None,
                        response_bytes=record.response_bytes,
                        request_bytes=record.request_bytes,
                        response_time_ms=record.response_time_ms,
                        cache_status=record.cache_status,
                        edge_location=record.edge_location,
                        referer=record.referer,
                        protocol=record.protocol,
                        ssl_protocol=record.ssl_protocol,
                        extra=record.extra,
                    )

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
                f"Failed to parse Cloudflare log file {path}: {e}",
            ) from e

    def _convert_cloudflare_record(self, record: dict) -> Optional[IngestionRecord]:
        """
        Convert a Cloudflare API record to IngestionRecord.

        Args:
            record: Cloudflare log record dictionary

        Returns:
            IngestionRecord or None if conversion fails
        """
        try:
            # Parse timestamp (nanoseconds to datetime)
            timestamp_ns = record.get("EdgeStartTimestamp")
            if timestamp_ns:
                # Cloudflare uses nanoseconds
                timestamp = datetime.fromtimestamp(timestamp_ns / 1e9, tz=timezone.utc)
            else:
                return None

            # Parse URI into path and query string
            request_uri = record.get("ClientRequestURI", "")
            if request_uri:
                parsed_uri = urlparse(request_uri)
                path = parsed_uri.path or "/"
                query_string = parsed_uri.query if parsed_uri.query else None
            else:
                # Default to root path if URI is missing
                path = "/"
                query_string = None

            # Extract required fields
            client_ip = record.get("ClientIP", "")
            method = record.get("ClientRequestMethod", "GET").upper()
            host = record.get("ClientRequestHost", "")
            status_code = record.get("EdgeResponseStatus", 0)
            user_agent = record.get("ClientRequestUserAgent", "")

            # Validate required fields
            if not client_ip or not host or not user_agent:
                logger.debug(
                    f"Skipping record with missing required fields: "
                    f"client_ip={bool(client_ip)}, host={bool(host)}, user_agent={bool(user_agent)}"
                )
                return None

            # Optional fields
            response_bytes = record.get("EdgeResponseBytes")
            request_bytes = record.get("ClientRequestBytes")
            response_time_ms = record.get("OriginResponseTime")
            cache_status = record.get("CacheCacheStatus")
            edge_location = record.get("EdgeColoCode")
            referer = record.get("ClientRequestReferer")
            protocol = record.get("ClientRequestProtocol")
            ssl_protocol = record.get("ClientRequestSSLProtocol")

            # Store extra Cloudflare-specific fields
            extra = {}
            for key, value in record.items():
                if key not in self.CLOUDFLARE_FIELD_MAPPING:
                    extra[key] = value

            return IngestionRecord(
                timestamp=timestamp,
                client_ip=client_ip,
                method=method,
                host=host,
                path=path,
                status_code=status_code,
                user_agent=user_agent,
                query_string=query_string,
                response_bytes=response_bytes,
                request_bytes=request_bytes,
                response_time_ms=response_time_ms,
                cache_status=cache_status,
                edge_location=edge_location,
                referer=referer,
                protocol=protocol,
                ssl_protocol=ssl_protocol,
                extra=extra,
            )

        except Exception as e:
            logger.debug(f"Failed to convert Cloudflare record: {e}")
            return None

    def _get_file_field_mapping(self) -> dict[str, str]:
        """
        Get field mapping for Cloudflare Logpush file formats.

        Logpush files may use slightly different field names than API.
        This mapping handles common variations.

        Note: For files, we map ClientRequestURI directly to path and query_string
        separately if the file format provides them. If only URI is available,
        the parser will need to handle URI splitting, or we can post-process.

        Returns:
            Dictionary mapping Cloudflare file field names to universal schema fields
        """
        # Start with API mapping (but note URI handling is different for files)
        mapping = {
            k: v
            for k, v in self.CLOUDFLARE_FIELD_MAPPING.items()
            if k != "ClientRequestURI"  # Handle URI separately for files
        }

        # Map URI fields - files may have separate path/query or combined URI
        # If file has separate fields, map them directly
        mapping.update(
            {
                "ClientRequestPath": "path",
                "ClientRequestQuery": "query_string",
                "path": "path",
                "query_string": "query_string",
            }
        )

        # If file only has URI, map to path (query_string will be None or parsed separately)
        # Note: Universal adapter can handle this, but Cloudflare Logpush typically
        # provides separate fields or full URI that needs parsing
        mapping.update(
            {
                "ClientRequestURI": "path",  # Will be parsed in post-processing if needed
                "URI": "path",
                "uri": "path",
            }
        )

        # Add common Logpush variations
        mapping.update(
            {
                # Alternative field name variations
                "EdgeStartTimestamp": "timestamp",
                "Timestamp": "timestamp",
                "timestamp": "timestamp",
                "ClientIP": "client_ip",
                "client_ip": "client_ip",
                "ClientRequestMethod": "method",
                "Method": "method",
                "method": "method",
                "ClientRequestHost": "host",
                "Host": "host",
                "host": "host",
                "EdgeResponseStatus": "status_code",
                "Status": "status_code",
                "status_code": "status_code",
                "ClientRequestUserAgent": "user_agent",
                "UserAgent": "user_agent",
                "user_agent": "user_agent",
            }
        )

        return mapping

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
