"""
Abstract base class and data models for ingestion adapters.

Provides the core interface and data structures for multi-provider
log ingestion following the universal schema.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator, Optional


@dataclass
class IngestionRecord:
    """
    Universal log record format for normalized ingestion.

    Represents a single log entry normalized to a common schema
    that works across all CDN and cloud providers.

    Required Fields:
        timestamp: Request timestamp (UTC)
        client_ip: Client IP address
        method: HTTP method (GET, POST, etc.)
        host: Host header / domain
        path: Request URI path
        status_code: HTTP response status code
        user_agent: User-Agent header

    Optional Fields (provider-dependent):
        query_string: Query parameters
        response_bytes: Response body size
        request_bytes: Request body size
        response_time_ms: Response latency in milliseconds
        cache_status: Cache hit/miss status
        edge_location: Edge POP identifier
        referer: Referer header
        protocol: HTTP protocol version (HTTP/1.1, HTTP/2, etc.)
        ssl_protocol: TLS version
        extra: Provider-specific extended fields
    """

    # Required fields (all providers must supply these)
    timestamp: datetime
    client_ip: str
    method: str
    host: str
    path: str
    status_code: int
    user_agent: str

    # Optional fields (provider-dependent)
    query_string: Optional[str] = None
    response_bytes: Optional[int] = None
    request_bytes: Optional[int] = None
    response_time_ms: Optional[int] = None
    cache_status: Optional[str] = None
    edge_location: Optional[str] = None
    referer: Optional[str] = None
    protocol: Optional[str] = None
    ssl_protocol: Optional[str] = None

    # Provider-specific extensions (always a dict, never None)
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """
        Convert to dictionary representation.

        Returns:
            Dictionary with all fields, including extras flattened
        """
        result = {
            "timestamp": self.timestamp.isoformat(),
            "client_ip": self.client_ip,
            "method": self.method,
            "host": self.host,
            "path": self.path,
            "status_code": self.status_code,
            "user_agent": self.user_agent,
            "query_string": self.query_string,
            "response_bytes": self.response_bytes,
            "request_bytes": self.request_bytes,
            "response_time_ms": self.response_time_ms,
            "cache_status": self.cache_status,
            "edge_location": self.edge_location,
            "referer": self.referer,
            "protocol": self.protocol,
            "ssl_protocol": self.ssl_protocol,
        }
        # Include extra fields with prefix
        if self.extra:
            for key, value in self.extra.items():
                result[f"_extra_{key}"] = value
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "IngestionRecord":
        """
        Create an IngestionRecord from a dictionary.

        Args:
            data: Dictionary with record fields. Timestamp can be a datetime
                  object, an ISO format string, or a Unix timestamp (int/float/string).

        Returns:
            IngestionRecord instance

        Raises:
            ValidationError: If required fields are missing or invalid
        """
        from .exceptions import ValidationError

        # Required fields
        required_fields = [
            "timestamp",
            "client_ip",
            "method",
            "host",
            "path",
            "status_code",
            "user_agent",
        ]
        for field_name in required_fields:
            if field_name not in data:
                raise ValidationError(
                    f"Missing required field: {field_name}",
                    field=field_name,
                )

        # Parse timestamp
        timestamp = cls._parse_timestamp_value(data["timestamp"])
        if timestamp is None:
            raise ValidationError(
                f"Invalid timestamp format: {data['timestamp']}",
                field="timestamp",
                value=data["timestamp"],
            )

        # Extract extra fields from multiple sources:
        # 1. Keys prefixed with _extra_
        # 2. The "extra" key if it's a dict
        extra = {}
        for key, value in data.items():
            if key.startswith("_extra_"):
                extra_key = key[7:]  # Remove "_extra_" prefix
                extra[extra_key] = value

        # Also check for "extra" key containing a dict
        if "extra" in data and isinstance(data["extra"], dict):
            extra.update(data["extra"])

        return cls(
            timestamp=timestamp,
            client_ip=data["client_ip"],
            method=data["method"],
            host=data["host"],
            path=data["path"],
            status_code=data["status_code"],
            user_agent=data["user_agent"],
            query_string=data.get("query_string"),
            response_bytes=data.get("response_bytes"),
            request_bytes=data.get("request_bytes"),
            response_time_ms=data.get("response_time_ms"),
            cache_status=data.get("cache_status"),
            edge_location=data.get("edge_location"),
            referer=data.get("referer"),
            protocol=data.get("protocol"),
            ssl_protocol=data.get("ssl_protocol"),
            extra=extra,
        )

    @staticmethod
    def _parse_timestamp_value(value: Any) -> Optional[datetime]:
        """
        Parse a timestamp value into a datetime object.

        All returned datetimes are UTC timezone-aware for consistency.

        Supports:
        - datetime objects (converted to UTC if naive)
        - ISO 8601 formatted strings
        - Unix timestamps (int, float, or numeric string)
        - Nanosecond/microsecond/millisecond timestamps
        """
        if value is None:
            return None

        if isinstance(value, datetime):
            # Ensure timezone-aware (assume UTC if naive)
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        # Try ISO format for strings
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                # Ensure timezone-aware
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                pass

        # Try numeric timestamp (Unix timestamps are always UTC)
        try:
            if isinstance(value, str):
                ts = float(value)
            elif isinstance(value, (int, float)):
                ts = value
            else:
                return None

            if ts > 1e18:  # Nanoseconds
                return datetime.fromtimestamp(ts / 1e9, tz=timezone.utc)
            elif ts > 1e15:  # Microseconds
                return datetime.fromtimestamp(ts / 1e6, tz=timezone.utc)
            elif ts > 1e12:  # Milliseconds
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            else:  # Seconds
                return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (ValueError, OSError, OverflowError, TypeError):
            return None


@dataclass
class IngestionSource:
    """
    Configuration for an ingestion source.

    Defines where to read log data from and how to access it.

    Attributes:
        provider: Provider name (e.g., 'cloudflare', 'aws_cloudfront')
        source_type: Type of source ('api', 'csv_file', 'json_file',
                     's3', 'gcs', 'azure_blob')
        path_or_uri: Path or URI to the data source
        credentials: Optional credentials for API/cloud access
        options: Additional provider-specific options
    """

    provider: str
    source_type: str
    path_or_uri: str
    credentials: dict = field(default_factory=dict)
    options: dict = field(default_factory=dict)

    # Valid source types
    VALID_SOURCE_TYPES = frozenset(
        [
            "akamai_json_file",  # Akamai DataStream JSON exports
            "akamai_ndjson_file",  # Akamai DataStream NDJSON exports
            "alb_log_file",  # AWS ALB access logs (space-separated)
            "api",
            "csv_file",
            "fastly_csv_file",  # Fastly CSV log exports
            "fastly_json_file",  # Fastly JSON log exports
            "fastly_ndjson_file",  # Fastly NDJSON log exports
            "json_file",
            "ndjson_file",
            "tsv_file",
            "w3c_file",  # W3C extended log format (AWS CloudFront, DigitalOcean)
            "s3",
            "gcs",
            "azure_blob",
        ]
    )

    def __post_init__(self):
        """Validate source configuration after initialization."""
        if self.source_type not in self.VALID_SOURCE_TYPES:
            from .exceptions import SourceValidationError

            raise SourceValidationError(
                f"Invalid source_type: '{self.source_type}'",
                source_type=self.source_type,
                reason=f"Must be one of: {', '.join(sorted(self.VALID_SOURCE_TYPES))}",
            )

    def is_file_source(self) -> bool:
        """Check if this source is a local file."""
        return self.source_type in {
            "csv_file",
            "json_file",
            "ndjson_file",
            "tsv_file",
            "w3c_file",
        }

    def is_cloud_source(self) -> bool:
        """Check if this source is a cloud storage location."""
        return self.source_type in {"s3", "gcs", "azure_blob"}

    def is_api_source(self) -> bool:
        """Check if this source is an API endpoint."""
        return self.source_type == "api"


class IngestionAdapter(ABC):
    """
    Abstract base class for all ingestion adapters.

    Each cloud/CDN provider implements this interface to handle
    reading log data and converting it to the universal schema.

    Subclasses must implement:
        - provider_name: Property returning the provider identifier
        - supported_source_types: Property returning list of supported source types
        - ingest(): Generator yielding IngestionRecord objects
        - validate_source(): Validate source configuration

    Example Implementation:
        @IngestionRegistry.register('cloudflare')
        class CloudflareAdapter(IngestionAdapter):
            @property
            def provider_name(self) -> str:
                return 'cloudflare'

            @property
            def supported_source_types(self) -> list[str]:
                return ['api', 'json_file', 'csv_file']

            def ingest(self, source, **kwargs):
                # Parse and yield records
                for record in self._read_source(source):
                    yield self._to_ingestion_record(record)

            def validate_source(self, source):
                # Check source accessibility
                return (True, '')
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Return the provider name identifier.

        This is used for registry lookup and logging.

        Returns:
            Provider identifier string (e.g., 'cloudflare', 'aws_cloudfront')
        """
        pass

    @property
    @abstractmethod
    def supported_source_types(self) -> list[str]:
        """
        Return list of supported source types.

        Returns:
            List of source type strings (e.g., ['api', 'csv_file', 's3'])
        """
        pass

    @abstractmethod
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

        Reads log data from the configured source, parses it according
        to the provider's format, and yields normalized IngestionRecord
        objects.

        Args:
            source: Ingestion source configuration
            start_time: Optional start time filter (UTC)
            end_time: Optional end time filter (UTC)
            filter_bots: If True, apply LLM bot filtering
            **kwargs: Additional provider-specific options

        Yields:
            IngestionRecord objects in universal format

        Raises:
            SourceValidationError: If source is invalid or inaccessible
            ParseError: If log data cannot be parsed
            IngestionError: For other ingestion failures
        """
        pass

    @abstractmethod
    def validate_source(self, source: IngestionSource) -> tuple[bool, str]:
        """
        Validate that the source is accessible and properly formatted.

        Performs pre-flight checks to ensure the source can be read
        before attempting full ingestion.

        Args:
            source: Ingestion source configuration

        Returns:
            Tuple of (is_valid, error_message)
            - is_valid: True if source passes validation
            - error_message: Empty string if valid, error details if invalid
        """
        pass

    def supports_source_type(self, source_type: str) -> bool:
        """
        Check if this adapter supports a given source type.

        Args:
            source_type: Source type to check

        Returns:
            True if source type is supported
        """
        return source_type in self.supported_source_types
