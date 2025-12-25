"""
Multi-provider ingestion layer for log data normalization.

Provides a unified interface for ingesting log data from various CDN
and cloud providers (Cloudflare, AWS CloudFront, Azure CDN, etc.)
into a normalized universal schema.

Usage:
    from llm_bot_pipeline.ingestion import (
        IngestionAdapter,
        IngestionRecord,
        IngestionSource,
        IngestionRegistry,
        get_adapter,
    )

    # Get an adapter for a specific provider
    adapter = get_adapter('cloudflare')

    # Configure the source
    source = IngestionSource(
        provider='cloudflare',
        source_type='api',
        path_or_uri='zone_id',
    )

    # Ingest records
    for record in adapter.ingest(source):
        print(record.timestamp, record.client_ip)
"""

from .base import IngestionAdapter, IngestionRecord, IngestionSource
from .file_utils import open_file_auto_decompress
from .exceptions import (
    IngestionError,
    ParseError,
    ProviderNotFoundError,
    SourceValidationError,
    ValidationError,
)
from .registry import IngestionRegistry, get_adapter, list_providers, register_adapter
from .security import (
    PathTraversalError,
    RateLimiter,
    SecurityValidationError,
    check_rate_limit,
    get_rate_limiter,
    sanitize_path,
    sanitize_string,
    validate_field_length,
    validate_path_safe,
)
from .validation import (
    DEFAULT_MAX_FILE_SIZE_BYTES,
    WARN_FILE_SIZE_BYTES,
    ErrorCodes,
    FileValidationResult,
    ValidationIssue,
    ValidationReport,
    check_memory_limit,
    format_file_size,
    get_memory_usage_mb,
    validate_directory,
    validate_file_path,
)

__all__ = [
    # Base classes and data models
    "IngestionAdapter",
    "IngestionRecord",
    "IngestionSource",
    # Registry functions
    "IngestionRegistry",
    "get_adapter",
    "register_adapter",
    "list_providers",
    # Exceptions
    "IngestionError",
    "ValidationError",
    "ParseError",
    "ProviderNotFoundError",
    "SourceValidationError",
    "PathTraversalError",
    "SecurityValidationError",
    # Validation utilities
    "ValidationReport",
    "FileValidationResult",
    "ValidationIssue",
    "ErrorCodes",
    "validate_file_path",
    "validate_directory",
    "get_memory_usage_mb",
    "check_memory_limit",
    "format_file_size",
    # Security utilities
    "validate_path_safe",
    "sanitize_path",
    "sanitize_string",
    "validate_field_length",
    "RateLimiter",
    "get_rate_limiter",
    "check_rate_limit",
    # File utilities
    "open_file_auto_decompress",
]
