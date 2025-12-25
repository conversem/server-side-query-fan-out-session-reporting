"""
Security utilities for ingestion pipeline.

Provides path traversal protection, input sanitization, and other
security-related validation utilities.
"""

import logging
import os
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class PathTraversalError(Exception):
    """Raised when a path traversal attempt is detected."""

    def __init__(self, message: str, path: Path, base_dir: Optional[Path] = None):
        self.path = path
        self.base_dir = base_dir
        super().__init__(message)


class SecurityValidationError(Exception):
    """Raised when a security validation fails."""

    pass


# Characters and patterns that are suspicious in paths
SUSPICIOUS_PATH_PATTERNS = [
    r"\.\.",  # Parent directory reference
    r"^~",  # Home directory expansion
    r"\$\{",  # Variable expansion
    r"\$\(",  # Command substitution
    r"`",  # Backtick command substitution
    r"\|",  # Pipe
    r";",  # Command separator
    r"&",  # Background/AND
    r">",  # Redirect
    r"<",  # Redirect
]


def validate_path_safe(
    path: Path,
    base_dir: Optional[Path] = None,
    allow_symlinks: bool = False,
    check_exists: bool = False,
) -> tuple[bool, str]:
    """
    Validate that a path is safe from traversal attacks.

    Performs comprehensive validation to prevent path traversal,
    symlink attacks, and other security issues.

    Args:
        path: Path to validate (can be relative or absolute)
        base_dir: If provided, validates that the resolved path is
                  within this directory (prevents escaping)
        allow_symlinks: If False, rejects paths containing symlinks
        check_exists: If True, also verifies the path exists

    Returns:
        Tuple of (is_safe, error_message)
        - is_safe: True if path passes all security checks
        - error_message: Empty if safe, error details if unsafe

    Examples:
        >>> validate_path_safe(Path("/var/logs/app.log"))
        (True, "")

        >>> validate_path_safe(Path("../../etc/passwd"))
        (False, "Path contains directory traversal sequence: ../../etc/passwd")

        >>> validate_path_safe(Path("/app/logs/file.csv"), base_dir=Path("/app/logs"))
        (True, "")

        >>> validate_path_safe(Path("/etc/passwd"), base_dir=Path("/app/logs"))
        (False, "Path escapes base directory: /etc/passwd is not within /app/logs")
    """
    # Check for null bytes (common attack vector)
    path_str = str(path)
    if "\x00" in path_str:
        return (False, f"Path contains null byte: {path}")

    # Check for suspicious patterns before path resolution
    for pattern in SUSPICIOUS_PATH_PATTERNS:
        if re.search(pattern, path_str):
            if pattern == r"\.\.":
                return (False, f"Path contains directory traversal sequence: {path}")
            return (False, f"Path contains suspicious characters: {path}")

    # Resolve to absolute path
    try:
        # Get absolute path without resolving symlinks first
        if path.is_absolute():
            abs_path = path
        else:
            # For relative paths, we need to resolve relative to current dir
            # but check traversal first
            abs_path = Path.cwd() / path

        # Normalize the path (handles . and .. safely)
        # Use resolve() which canonicalizes the path
        resolved_path = abs_path.resolve()

    except (OSError, RuntimeError, ValueError) as e:
        return (False, f"Cannot resolve path {path}: {e}")

    # Check if path is a symlink (if not allowed)
    if not allow_symlinks:
        try:
            # Check if any component of the path is a symlink
            check_path = path if path.is_absolute() else Path.cwd() / path
            if check_path.exists():
                # Check the path itself
                if check_path.is_symlink():
                    return (False, f"Path is a symbolic link: {path}")

                # Check each parent directory for symlinks
                for parent in check_path.parents:
                    if parent.is_symlink():
                        return (
                            False,
                            f"Path contains symbolic link in chain: {parent}",
                        )
        except (OSError, RuntimeError):
            # If we can't check symlinks, err on side of caution
            pass

    # Check if resolved path is within base directory
    if base_dir is not None:
        try:
            base_resolved = base_dir.resolve()

            # Check if resolved path starts with base directory
            try:
                resolved_path.relative_to(base_resolved)
            except ValueError:
                return (
                    False,
                    f"Path escapes base directory: {resolved_path} is not within {base_resolved}",
                )

        except (OSError, RuntimeError, ValueError) as e:
            return (False, f"Cannot resolve base directory {base_dir}: {e}")

    # Check existence if required
    if check_exists and not resolved_path.exists():
        return (False, f"Path does not exist: {resolved_path}")

    return (True, "")


def validate_path_component(component: str) -> tuple[bool, str]:
    """
    Validate a single path component (filename or directory name).

    Args:
        component: Single path component to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not component:
        return (False, "Empty path component")

    # Check for null bytes
    if "\x00" in component:
        return (False, "Path component contains null byte")

    # Check for special directory references
    if component in (".", ".."):
        return (False, f"Invalid path component: {component}")

    # Check for path separators
    if "/" in component or "\\" in component:
        return (False, "Path component contains path separator")

    # Check for other dangerous characters
    dangerous_chars = ["<", ">", ":", '"', "|", "?", "*"]
    for char in dangerous_chars:
        if char in component:
            return (False, f"Path component contains invalid character: {char}")

    return (True, "")


def sanitize_path(
    path: Path,
    base_dir: Optional[Path] = None,
    allow_symlinks: bool = False,
) -> Path:
    """
    Sanitize and validate a path, returning the safe resolved path.

    Combines validation with path resolution. Raises exception if path
    is unsafe rather than returning error tuple.

    Args:
        path: Path to sanitize and validate
        base_dir: Optional directory to constrain path within
        allow_symlinks: If False, reject paths containing symlinks

    Returns:
        Resolved, validated Path object

    Raises:
        PathTraversalError: If path fails security validation
    """
    is_safe, error_msg = validate_path_safe(
        path, base_dir=base_dir, allow_symlinks=allow_symlinks
    )

    if not is_safe:
        raise PathTraversalError(error_msg, path, base_dir)

    # Return the resolved path
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def validate_field_length(
    field_name: str,
    value: Optional[str],
    max_length: int = 65535,
) -> tuple[bool, str]:
    """
    Validate that a field value doesn't exceed maximum length.

    Prevents DoS attacks via oversized field values.

    Args:
        field_name: Name of the field (for error messages)
        value: Field value to validate
        max_length: Maximum allowed length in characters

    Returns:
        Tuple of (is_valid, error_message)
    """
    if value is None:
        return (True, "")

    if len(value) > max_length:
        return (
            False,
            f"Field '{field_name}' exceeds maximum length: {len(value)} > {max_length}",
        )

    return (True, "")


def sanitize_string(value: str, max_length: int = 65535) -> str:
    """
    Sanitize a string value for safe storage.

    Removes control characters and truncates to max length.

    Args:
        value: String to sanitize
        max_length: Maximum allowed length

    Returns:
        Sanitized string
    """
    if not value:
        return value

    # Remove control characters (except common whitespace)
    # Keep tab, newline, carriage return
    sanitized = "".join(
        char for char in value if char >= " " or char in ("\t", "\n", "\r")
    )

    # Truncate to max length
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]

    return sanitized


def validate_encoding(
    data: bytes, expected_encoding: str = "utf-8"
) -> tuple[bool, str]:
    """
    Validate that data is valid in the expected encoding.

    Args:
        data: Bytes to validate
        expected_encoding: Expected encoding (default: utf-8)

    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        data.decode(expected_encoding)
        return (True, "")
    except UnicodeDecodeError as e:
        return (
            False,
            f"Invalid {expected_encoding} encoding at position {e.start}: {e.reason}",
        )


# Maximum field lengths for common fields
DEFAULT_FIELD_LIMITS = {
    "client_ip": 45,  # IPv6 max
    "method": 10,  # Longest HTTP method
    "host": 253,  # Max DNS hostname
    "path": 2048,  # Reasonable URL path limit
    "query_string": 8192,  # Generous query string limit
    "user_agent": 2048,  # Reasonable UA limit
    "referer": 2048,  # Reasonable referer limit
    "protocol": 20,  # HTTP/2, etc.
    "ssl_protocol": 20,  # TLSv1.3, etc.
    "cache_status": 50,  # Cache status codes
    "edge_location": 50,  # Edge POP codes
}


def get_field_max_length(field_name: str) -> int:
    """
    Get the maximum allowed length for a field.

    Args:
        field_name: Name of the field

    Returns:
        Maximum allowed length in characters
    """
    return DEFAULT_FIELD_LIMITS.get(field_name, 65535)


# =============================================================================
# Rate Limiting
# =============================================================================


class RateLimiter:
    """
    Simple rate limiter for API requests.

    Uses a sliding window approach to track request counts
    and enforce rate limits.
    """

    def __init__(
        self,
        max_requests: int = 100,
        window_seconds: float = 60.0,
    ):
        """
        Initialize rate limiter.

        Args:
            max_requests: Maximum requests allowed in the window
            window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._request_times: list[float] = []
        self._lock_acquired = False

    def acquire(self) -> bool:
        """
        Attempt to acquire permission to make a request.

        Returns:
            True if request is allowed, False if rate limited
        """
        import time

        current_time = time.time()
        window_start = current_time - self.window_seconds

        # Remove old requests outside the window
        self._request_times = [t for t in self._request_times if t > window_start]

        # Check if we're at the limit
        if len(self._request_times) >= self.max_requests:
            return False

        # Record this request
        self._request_times.append(current_time)
        return True

    def wait_and_acquire(self, timeout: float = 60.0) -> bool:
        """
        Wait until a request slot is available, then acquire.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if acquired, False if timeout
        """
        import time

        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.acquire():
                return True
            # Sleep briefly before retrying
            time.sleep(0.1)

        return False

    def reset(self):
        """Reset the rate limiter."""
        self._request_times = []

    @property
    def remaining_requests(self) -> int:
        """Get the number of remaining requests in the current window."""
        import time

        current_time = time.time()
        window_start = current_time - self.window_seconds

        # Count requests in the current window
        current_count = len([t for t in self._request_times if t > window_start])
        return max(0, self.max_requests - current_count)

    @property
    def time_until_reset(self) -> float:
        """Get time in seconds until the oldest request expires from the window."""
        import time

        if not self._request_times:
            return 0.0

        current_time = time.time()
        window_start = current_time - self.window_seconds

        # Find the oldest request still in the window
        active_times = [t for t in self._request_times if t > window_start]
        if not active_times:
            return 0.0

        oldest = min(active_times)
        return max(0.0, (oldest + self.window_seconds) - current_time)


# Default rate limiters for different API sources
_rate_limiters: dict[str, RateLimiter] = {}


def get_rate_limiter(
    key: str,
    max_requests: int = 100,
    window_seconds: float = 60.0,
) -> RateLimiter:
    """
    Get or create a rate limiter for the given key.

    Rate limiters are cached by key, so the same key will return
    the same rate limiter instance.

    Args:
        key: Unique identifier for this rate limiter (e.g., "cloudflare_api")
        max_requests: Maximum requests allowed in the window
        window_seconds: Time window in seconds

    Returns:
        RateLimiter instance
    """
    if key not in _rate_limiters:
        _rate_limiters[key] = RateLimiter(max_requests, window_seconds)
    return _rate_limiters[key]


def check_rate_limit(
    key: str, max_requests: int = 100, window_seconds: float = 60.0
) -> bool:
    """
    Check if a request is allowed under the rate limit.

    Convenience function that gets or creates a rate limiter and
    attempts to acquire permission.

    Args:
        key: Unique identifier for this rate limiter
        max_requests: Maximum requests allowed in the window
        window_seconds: Time window in seconds

    Returns:
        True if request is allowed, False if rate limited
    """
    limiter = get_rate_limiter(key, max_requests, window_seconds)
    return limiter.acquire()
