"""
Universal schema definition and field validators for log ingestion.

Defines the canonical schema that all providers map to, along with
validation functions for each field type.
"""

import ipaddress
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class FieldType(Enum):
    """Supported field types in the universal schema."""

    TIMESTAMP = "timestamp"
    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"


@dataclass
class FieldDefinition:
    """
    Definition of a field in the universal schema.

    Attributes:
        name: Field name in the universal schema
        field_type: Expected data type
        required: Whether the field is required
        description: Human-readable description
        validator: Optional custom validation function
        max_length: Maximum allowed length for string fields (security limit)
    """

    name: str
    field_type: FieldType
    required: bool
    description: str
    validator: Optional[Callable[[Any], bool]] = None
    max_length: Optional[int] = None


# =============================================================================
# Field Validators
# =============================================================================


def validate_timestamp(value: Any) -> bool:
    """
    Validate a timestamp value.

    Accepts:
    - datetime objects
    - ISO 8601 formatted strings
    - Unix timestamps (int, float, or numeric string)

    Args:
        value: Value to validate

    Returns:
        True if valid
    """
    if value is None:
        return False

    if isinstance(value, datetime):
        return True

    # Try ISO format and common date formats for strings
    if isinstance(value, str):
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except ValueError:
            pass
        # Try parsing common formats
        for fmt in [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d/%b/%Y:%H:%M:%S %z",  # Common log format
        ]:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        # Fall through to try numeric parsing

    # Try numeric Unix timestamp (int, float, or numeric string)
    try:
        if isinstance(value, str):
            # Try to convert string to float for numeric timestamp
            ts = float(value)
        elif isinstance(value, (int, float)):
            ts = value
        else:
            return False

        if ts > 1e18:  # Nanoseconds (Cloudflare EdgeStartTimestamp)
            datetime.fromtimestamp(ts / 1e9, tz=timezone.utc)
        elif ts > 1e15:  # Microseconds
            datetime.fromtimestamp(ts / 1e6, tz=timezone.utc)
        elif ts > 1e12:  # Milliseconds
            datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        else:  # Seconds
            datetime.fromtimestamp(ts, tz=timezone.utc)
        return True
    except (ValueError, OSError, OverflowError, TypeError):
        return False


def validate_ip_address(value: Any) -> bool:
    """
    Validate an IP address (IPv4 or IPv6).

    Args:
        value: Value to validate

    Returns:
        True if valid IPv4 or IPv6 address
    """
    if not value or not isinstance(value, str):
        return False

    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def validate_http_method(value: Any) -> bool:
    """
    Validate an HTTP method.

    Args:
        value: Value to validate

    Returns:
        True if valid HTTP method
    """
    if not value or not isinstance(value, str):
        return False

    valid_methods = {
        "GET",
        "POST",
        "PUT",
        "DELETE",
        "PATCH",
        "HEAD",
        "OPTIONS",
        "TRACE",
        "CONNECT",
    }
    return value.upper() in valid_methods


def validate_status_code(value: Any) -> bool:
    """
    Validate an HTTP status code.

    Args:
        value: Value to validate

    Returns:
        True if valid status code (100-599)
    """
    if value is None:
        return False

    try:
        code = int(value)
        return 100 <= code <= 599
    except (ValueError, TypeError):
        return False


def validate_non_empty_string(value: Any) -> bool:
    """
    Validate a non-empty string.

    Args:
        value: Value to validate

    Returns:
        True if non-empty string
    """
    return isinstance(value, str) and len(value.strip()) > 0


def validate_positive_integer(value: Any) -> bool:
    """
    Validate a non-negative integer (zero or positive).

    Used for optional integer fields like response_bytes, request_bytes, etc.
    Returns True for None values since these are optional fields.

    Args:
        value: Value to validate

    Returns:
        True if None, zero, or positive integer
    """
    if value is None:
        return True  # Optional fields can be None

    try:
        return int(value) >= 0
    except (ValueError, TypeError):
        return False


def validate_optional_string(value: Any) -> bool:
    """
    Validate an optional string field.

    Args:
        value: Value to validate

    Returns:
        True if None or string
    """
    return value is None or isinstance(value, str)


# =============================================================================
# Universal Schema Definition
# =============================================================================

# Required fields - all providers must supply these
REQUIRED_FIELDS: list[FieldDefinition] = [
    FieldDefinition(
        name="timestamp",
        field_type=FieldType.TIMESTAMP,
        required=True,
        description="Request timestamp (UTC, ISO 8601)",
        validator=validate_timestamp,
    ),
    FieldDefinition(
        name="client_ip",
        field_type=FieldType.STRING,
        required=True,
        description="Client IP address (IPv4 or IPv6)",
        validator=validate_ip_address,
        max_length=45,  # IPv6 max length
    ),
    FieldDefinition(
        name="method",
        field_type=FieldType.STRING,
        required=True,
        description="HTTP method (GET, POST, etc.)",
        validator=validate_http_method,
        max_length=10,  # Longest HTTP method
    ),
    FieldDefinition(
        name="host",
        field_type=FieldType.STRING,
        required=True,
        description="Host header / domain",
        validator=validate_non_empty_string,
        max_length=253,  # Max DNS hostname
    ),
    FieldDefinition(
        name="path",
        field_type=FieldType.STRING,
        required=True,
        description="Request URI path",
        validator=validate_non_empty_string,
        max_length=8192,  # Reasonable URL path limit
    ),
    FieldDefinition(
        name="status_code",
        field_type=FieldType.INTEGER,
        required=True,
        description="HTTP response status code (100-599)",
        validator=validate_status_code,
    ),
    FieldDefinition(
        name="user_agent",
        field_type=FieldType.STRING,
        required=True,
        description="User-Agent header",
        validator=validate_non_empty_string,
        max_length=4096,  # Reasonable UA limit
    ),
]

# Optional fields - provider-dependent
OPTIONAL_FIELDS: list[FieldDefinition] = [
    FieldDefinition(
        name="query_string",
        field_type=FieldType.STRING,
        required=False,
        description="Query parameters",
        validator=validate_optional_string,
        max_length=8192,  # Generous query string limit
    ),
    FieldDefinition(
        name="response_bytes",
        field_type=FieldType.INTEGER,
        required=False,
        description="Response body size in bytes",
        validator=validate_positive_integer,
    ),
    FieldDefinition(
        name="request_bytes",
        field_type=FieldType.INTEGER,
        required=False,
        description="Request body size in bytes",
        validator=validate_positive_integer,
    ),
    FieldDefinition(
        name="response_time_ms",
        field_type=FieldType.INTEGER,
        required=False,
        description="Response latency in milliseconds",
        validator=validate_positive_integer,
    ),
    FieldDefinition(
        name="cache_status",
        field_type=FieldType.STRING,
        required=False,
        description="Cache hit/miss status (HIT, MISS, etc.)",
        validator=validate_optional_string,
        max_length=50,  # Cache status codes
    ),
    FieldDefinition(
        name="edge_location",
        field_type=FieldType.STRING,
        required=False,
        description="Edge POP identifier",
        validator=validate_optional_string,
        max_length=50,  # Edge POP codes
    ),
    FieldDefinition(
        name="referer",
        field_type=FieldType.STRING,
        required=False,
        description="Referer header",
        validator=validate_optional_string,
        max_length=4096,  # Reasonable referer limit
    ),
    FieldDefinition(
        name="protocol",
        field_type=FieldType.STRING,
        required=False,
        description="HTTP protocol version (HTTP/1.1, HTTP/2, etc.)",
        validator=validate_optional_string,
        max_length=20,  # HTTP/2, etc.
    ),
    FieldDefinition(
        name="ssl_protocol",
        field_type=FieldType.STRING,
        required=False,
        description="TLS version",
        validator=validate_optional_string,
        max_length=20,  # TLSv1.3, etc.
    ),
]

# Combined schema
UNIVERSAL_SCHEMA: dict[str, FieldDefinition] = {
    field.name: field for field in REQUIRED_FIELDS + OPTIONAL_FIELDS
}


# =============================================================================
# Validation Functions
# =============================================================================


def validate_field(field_name: str, value: Any) -> tuple[bool, str]:
    """
    Validate a single field value.

    Args:
        field_name: Name of the field
        value: Value to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    if field_name not in UNIVERSAL_SCHEMA:
        # Unknown fields are allowed (stored in extra)
        return (True, "")

    field_def = UNIVERSAL_SCHEMA[field_name]

    # Check required fields
    if field_def.required and value is None:
        return (False, f"Required field '{field_name}' is missing")

    # Skip validation for None optional fields
    if value is None and not field_def.required:
        return (True, "")

    # Check field length for string fields (security limit)
    if field_def.max_length is not None and isinstance(value, str):
        if len(value) > field_def.max_length:
            return (
                False,
                f"Field '{field_name}' exceeds maximum length: "
                f"{len(value)} > {field_def.max_length}",
            )

    # Run validator if defined
    if field_def.validator:
        if not field_def.validator(value):
            return (
                False,
                f"Invalid value for '{field_name}': {value!r} "
                f"(expected {field_def.field_type.value})",
            )

    return (True, "")


def validate_record(
    data: dict[str, Any],
    strict: bool = False,
) -> tuple[bool, list[str]]:
    """
    Validate a complete record against the universal schema.

    Args:
        data: Dictionary of field values
        strict: If True, validate all fields; if False, only required fields

    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []

    # Check required fields
    for field_def in REQUIRED_FIELDS:
        if field_def.name not in data or data[field_def.name] is None:
            errors.append(f"Missing required field: {field_def.name}")
            continue

        is_valid, error = validate_field(field_def.name, data[field_def.name])
        if not is_valid:
            errors.append(error)

    # Validate optional fields if strict mode
    if strict:
        for field_def in OPTIONAL_FIELDS:
            if field_def.name in data and data[field_def.name] is not None:
                is_valid, error = validate_field(field_def.name, data[field_def.name])
                if not is_valid:
                    errors.append(error)

    return (len(errors) == 0, errors)


def get_required_field_names() -> list[str]:
    """Get list of required field names."""
    return [f.name for f in REQUIRED_FIELDS]


def get_optional_field_names() -> list[str]:
    """Get list of optional field names."""
    return [f.name for f in OPTIONAL_FIELDS]
