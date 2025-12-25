# Security Guide

This document covers the security features implemented in the log ingestion pipeline.

## Overview

The ingestion pipeline handles potentially untrusted input from various log sources. Several security layers protect against common attack vectors:

1. **Path Traversal Protection** - Prevents directory traversal attacks
2. **Input Sanitization** - Cleans and validates field values
3. **Field Length Limits** - Prevents DoS via oversized fields
4. **Encoding Validation** - Ensures proper UTF-8 encoding
5. **Rate Limiting** - Protects against API abuse
6. **File Size Limits** - Prevents resource exhaustion

## Path Traversal Protection

### Threat Model

Attackers may attempt to read or write files outside intended directories by using:
- `../` sequences to escape directories
- Absolute paths pointing elsewhere
- Symlink attacks
- Null byte injection

### Protection

The `validate_path_safe()` function provides comprehensive path validation:

```python
from llm_bot_pipeline.ingestion.security import validate_path_safe
from pathlib import Path

# Basic validation
is_safe, error = validate_path_safe(Path("logs/app.log"))

# Restrict to base directory
is_safe, error = validate_path_safe(
    Path("../etc/passwd"),
    base_dir=Path("/app/logs")
)
# Returns: (False, "Path escapes base directory: /etc/passwd is not within /app/logs")
```

### CLI Usage

Use `--base-dir` to restrict file access:

```bash
# Only allow files within /var/logs/cdn
python scripts/ingest_logs.py \
    --provider universal \
    --input /var/logs/cdn/access.log \
    --base-dir /var/logs/cdn
```

Attempted path traversal will be rejected:

```bash
python scripts/ingest_logs.py \
    --provider universal \
    --input ../../../etc/passwd \
    --base-dir /var/logs/cdn
# Error: Path traversal detected
```

### Checked Patterns

The following patterns are blocked:
- `..` (parent directory)
- `~` (home directory expansion)
- `${}` (variable expansion)
- `$()` (command substitution)
- `` ` `` (backtick command substitution)
- `|`, `;`, `&`, `>`, `<` (shell operators)
- Null bytes (`\x00`)

## Input Sanitization

### Field Value Sanitization

All string fields are sanitized before storage:

```python
from llm_bot_pipeline.ingestion.security import sanitize_string

# Removes control characters, truncates to max length
clean = sanitize_string(user_input, max_length=2048)
```

Features:
- Removes control characters (except tab, newline, CR)
- Truncates to maximum length
- Preserves valid UTF-8 content

### Sanitization in Schema Validation

Field sanitization is integrated into the schema validation layer. When records are validated, string fields are automatically sanitized:

```python
from llm_bot_pipeline.ingestion.parsers.schema import validate_field

# Field is sanitized during validation
is_valid, cleaned_value, errors = validate_field(
    field_name="user_agent",
    value=potentially_dirty_input
)
```

## Field Length Limits

### Default Limits

Maximum lengths are enforced for common fields to prevent resource exhaustion:

| Field | Max Length |
|-------|------------|
| `client_ip` | 45 (IPv6 max) |
| `method` | 10 |
| `host` | 253 (DNS max) |
| `path` | 2,048 |
| `query_string` | 8,192 |
| `user_agent` | 2,048 |
| `referer` | 2,048 |
| `protocol` | 20 |

### Custom Limits

```python
from llm_bot_pipeline.ingestion.security import validate_field_length

is_valid, error = validate_field_length(
    field_name="custom_field",
    value=some_value,
    max_length=1000
)
```

### Schema-Level Enforcement

Add `max_length` to field definitions:

```python
from llm_bot_pipeline.ingestion.parsers.schema import FieldDefinition, FieldType

CUSTOM_SCHEMA = {
    "user_agent": FieldDefinition(
        name="user_agent",
        field_type=FieldType.STRING,
        max_length=2048,  # Enforce max length
    ),
}
```

## Encoding Validation

### UTF-8 Validation

Ensure data is valid UTF-8 before processing:

```python
from llm_bot_pipeline.ingestion.security import validate_encoding

with open("logfile.txt", "rb") as f:
    data = f.read(4096)
    is_valid, error = validate_encoding(data, expected_encoding="utf-8")
```

### Parser Integration

Parsers validate encoding when reading files:
- Invalid encoding generates warnings
- Invalid bytes are handled gracefully
- Encoding errors are logged with position information

## Rate Limiting

### API Protection

Rate limiting prevents abuse of API-based ingestion sources:

```python
from llm_bot_pipeline.ingestion.security import check_rate_limit, RateLimiter

# Quick check
if not check_rate_limit("cloudflare_api", max_requests=100, window_seconds=60):
    raise Exception("Rate limited")

# Or use limiter directly
limiter = RateLimiter(max_requests=100, window_seconds=60.0)
if not limiter.acquire():
    # Wait for rate limit to reset
    limiter.wait_and_acquire(timeout=30.0)
```

### Cloudflare Adapter Integration

The Cloudflare adapter includes built-in rate limiting for API calls:

```python
# In CloudflareAdapter._ingest_api():
if not check_rate_limit("cloudflare_api"):
    raise IngestionError("Rate limit exceeded")
```

### Configuration

Default rate limits:
- 100 requests per 60-second window
- Customizable per-source via limiter configuration

## File Size Limits

### CLI Configuration

Limit maximum file size with `--max-file-size`:

```bash
# Limit to 1GB
python scripts/ingest_logs.py \
    --provider universal \
    --input large-log.csv \
    --max-file-size 1073741824

# Human-readable format supported
python scripts/ingest_logs.py \
    --provider universal \
    --input large-log.csv \
    --max-file-size 10GB
```

### Default Limit

The default maximum file size is 10GB. Files exceeding this limit are rejected with a clear error message.

### Validation Integration

File size is checked during source validation:

```python
from llm_bot_pipeline.ingestion.validation import validate_file_path

result = validate_file_path(
    file_path=Path("huge-file.log"),
    max_size_bytes=10 * 1024 * 1024 * 1024,  # 10GB
)

if not result.is_valid:
    for error in result.errors:
        print(f"Error: {error.message}")
```

## Security Best Practices

### For Operators

1. **Use `--base-dir`** to restrict file access when processing untrusted file paths
2. **Set `--max-file-size`** appropriate to your infrastructure
3. **Monitor rate limiting** to detect abuse patterns
4. **Validate log sources** before enabling automated ingestion

### For Developers

1. **Always validate paths** with `validate_path_safe()` before file operations
2. **Sanitize user input** before storage or display
3. **Enforce field limits** to prevent resource exhaustion
4. **Use rate limiters** for external API calls
5. **Validate encoding** when reading untrusted data

### Deployment Considerations

1. **Principle of Least Privilege** - Run ingestion with minimal filesystem permissions
2. **Network Isolation** - Restrict API access to necessary endpoints
3. **Logging** - Enable debug logging to detect suspicious activity
4. **Monitoring** - Track rate limit hits and validation failures

## Error Codes

Security-related error codes in `ErrorCodes`:

| Code | Description |
|------|-------------|
| `PATH_TRAVERSAL_DETECTED` | Path contains traversal sequence or escapes base directory |
| `PERMISSION_DENIED` | Insufficient permissions to access file |
| `FILE_TOO_LARGE` | File exceeds maximum size limit |
| `FIELD_TOO_LONG` | Field value exceeds maximum length |
| `INVALID_ENCODING` | Data is not valid in expected encoding |
| `RATE_LIMIT_EXCEEDED` | Too many requests in time window |

## Exceptions

The security module defines specific exceptions:

```python
from llm_bot_pipeline.ingestion.security import (
    PathTraversalError,
    SecurityValidationError,
)

try:
    sanitize_path(untrusted_path, base_dir=safe_dir)
except PathTraversalError as e:
    print(f"Security violation: {e}")
    print(f"Attempted path: {e.path}")
    print(f"Base directory: {e.base_dir}")
```

## See Also

- [CLI Usage Guide](cli-usage.md) - Command-line options including security flags
- [Troubleshooting](troubleshooting.md) - Common issues including security errors
- [Adding Providers](adding-providers.md) - Developer guide for creating custom adapters

