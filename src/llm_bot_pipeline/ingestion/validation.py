"""
Comprehensive validation utilities for ingestion pipeline.

Provides validation for file paths, permissions, formats, and schema,
along with detailed error reporting and resource monitoring.
"""

import logging
import os
import resource
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# =============================================================================
# Validation Results and Reports
# =============================================================================


@dataclass
class ValidationIssue:
    """Represents a single validation error or warning."""

    error_code: str
    message: str
    field: Optional[str] = None
    value: Optional[object] = None
    line_number: Optional[int] = None
    suggestion: Optional[str] = None


@dataclass
class FileValidationResult:
    """Result of validating a file."""

    file_path: Path
    is_valid: bool
    errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    file_size_bytes: Optional[int] = None
    is_readable: bool = False
    format_detected: Optional[str] = None


@dataclass
class ValidationReport:
    """Comprehensive validation report for an ingestion run."""

    start_time: datetime
    end_time: Optional[datetime] = None
    files_processed: int = 0
    files_failed: int = 0
    records_processed: int = 0
    records_valid: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    errors: list[ValidationIssue] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    file_results: list[FileValidationResult] = field(default_factory=list)
    peak_memory_mb: Optional[float] = None
    duration_seconds: Optional[float] = None

    def to_dict(self) -> dict:
        """Convert report to dictionary."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "files_processed": self.files_processed,
            "files_failed": self.files_failed,
            "records_processed": self.records_processed,
            "records_valid": self.records_valid,
            "records_skipped": self.records_skipped,
            "records_failed": self.records_failed,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "peak_memory_mb": self.peak_memory_mb,
            "duration_seconds": self.duration_seconds,
        }


# =============================================================================
# File Validation
# =============================================================================


def validate_file_path(
    file_path: Path,
    check_exists: bool = True,
    check_readable: bool = True,
    max_size_bytes: Optional[int] = None,
    allowed_extensions: Optional[list[str]] = None,
) -> FileValidationResult:
    """
    Validate a file path for ingestion.

    Args:
        file_path: Path to validate
        check_exists: If True, check file exists
        check_readable: If True, check file is readable
        max_size_bytes: Maximum allowed file size (None = no limit)
        allowed_extensions: List of allowed extensions (None = all allowed)

    Returns:
        FileValidationResult with validation status and errors
    """
    result = FileValidationResult(file_path=file_path, is_valid=True)

    # Check if path exists
    if check_exists:
        if not file_path.exists():
            result.is_valid = False
            result.errors.append(
                ValidationIssue(
                    error_code="file_not_found",
                    message=f"File does not exist: {file_path}",
                    suggestion="Verify the file path is correct and the file has not been moved or deleted.",
                )
            )
            return result

        if not file_path.is_file():
            result.is_valid = False
            result.errors.append(
                ValidationIssue(
                    error_code="not_a_file",
                    message=f"Path is not a file: {file_path}",
                    suggestion="Ensure the path points to a file, not a directory.",
                )
            )
            return result

    # Check file size
    try:
        file_size = file_path.stat().st_size
        result.file_size_bytes = file_size

        if file_size == 0:
            result.is_valid = False
            result.errors.append(
                ValidationIssue(
                    error_code=ErrorCodes.EMPTY_FILE,
                    message=f"File is empty: {file_path}",
                    suggestion="Ensure the file contains data before ingestion.",
                )
            )

        if max_size_bytes and file_size > max_size_bytes:
            result.is_valid = False
            result.errors.append(
                ValidationIssue(
                    error_code=ErrorCodes.FILE_TOO_LARGE,
                    message=f"File size ({format_file_size(file_size)}) exceeds maximum limit "
                    f"({format_file_size(max_size_bytes)})",
                    suggestion="Use a smaller file, split the file, or increase --max-file-size limit.",
                )
            )
        elif file_size > WARN_FILE_SIZE_BYTES:
            result.warnings.append(
                f"File size ({format_file_size(file_size)}) is large. Processing may be slow."
            )
    except (OSError, PermissionError) as e:
        result.is_valid = False
        result.errors.append(
            ValidationIssue(
                error_code=ErrorCodes.CANNOT_ACCESS_FILE,
                message=f"Cannot access file: {e}",
                suggestion="Check file permissions and ensure the file is accessible.",
            )
        )
        return result

    # Check readability
    if check_readable:
        if not os.access(file_path, os.R_OK):
            result.is_valid = False
            result.errors.append(
                ValidationIssue(
                    error_code="permission_denied",
                    message=f"Permission denied: {file_path}",
                    suggestion="Check file permissions and ensure read access is granted.",
                )
            )
        else:
            result.is_readable = True

    # Check file extension
    if allowed_extensions:
        ext = file_path.suffix.lower()
        if ext == ".gz":
            # Check base extension for gzip files
            if "." in file_path.stem:
                ext = "." + file_path.stem.split(".")[-1].lower()
            else:
                ext = ""

        # Normalize extension (ensure it starts with dot)
        if ext and not ext.startswith("."):
            ext = "." + ext

        if ext and ext not in allowed_extensions:
            result.is_valid = False
            result.errors.append(
                ValidationIssue(
                    error_code="unsupported_format",
                    message=f"Unsupported file format: {ext}",
                    suggestion=f"Supported formats: {', '.join(allowed_extensions)}",
                )
            )

    return result


def validate_directory(
    dir_path: Path,
    check_exists: bool = True,
    check_readable: bool = True,
    min_files: int = 1,
) -> FileValidationResult:
    """
    Validate a directory path for ingestion.

    Args:
        dir_path: Directory path to validate
        check_exists: If True, check directory exists
        check_readable: If True, check directory is readable
        min_files: Minimum number of files required

    Returns:
        FileValidationResult with validation status
    """
    result = FileValidationResult(file_path=dir_path, is_valid=True)

    if check_exists:
        if not dir_path.exists():
            result.is_valid = False
            result.errors.append(
                ValidationIssue(
                    error_code="directory_not_found",
                    message=f"Directory does not exist: {dir_path}",
                    suggestion="Verify the directory path is correct.",
                )
            )
            return result

        if not dir_path.is_dir():
            result.is_valid = False
            result.errors.append(
                ValidationIssue(
                    error_code="not_a_directory",
                    message=f"Path is not a directory: {dir_path}",
                    suggestion="Ensure the path points to a directory, not a file.",
                )
            )
            return result

    if check_readable:
        if not os.access(dir_path, os.R_OK):
            result.is_valid = False
            result.errors.append(
                ValidationIssue(
                    error_code="permission_denied",
                    message=f"Permission denied: {dir_path}",
                    suggestion="Check directory permissions and ensure read access is granted.",
                )
            )
            return result

        # Check if directory has files
        try:
            file_count = sum(1 for _ in dir_path.rglob("*") if _.is_file())
            if file_count < min_files:
                result.warnings.append(
                    f"Directory contains {file_count} file(s), "
                    f"minimum recommended: {min_files}"
                )
        except (OSError, PermissionError) as e:
            result.warnings.append(f"Cannot enumerate directory contents: {e}")
        except Exception as e:
            # Catch any other unexpected errors during enumeration
            logger.debug(f"Unexpected error enumerating directory {dir_path}: {e}")
            result.warnings.append(f"Cannot enumerate directory contents: {e}")

    return result


# =============================================================================
# Resource Monitoring
# =============================================================================


def get_memory_usage_mb() -> float:
    """
    Get current memory usage in megabytes.

    Returns:
        Memory usage in MB
    """
    try:
        # Get memory usage (RSS - Resident Set Size)
        usage = resource.getrusage(resource.RUSAGE_SELF)
        # ru_maxrss is in kilobytes on Linux, bytes on macOS
        if sys.platform == "darwin":
            return usage.ru_maxrss / (1024 * 1024)  # Convert bytes to MB
        else:
            return usage.ru_maxrss / 1024  # Convert KB to MB
    except Exception as e:
        logger.debug(f"Failed to get memory usage: {e}")
        return 0.0


def check_memory_limit(max_memory_mb: float) -> tuple[bool, float]:
    """
    Check if current memory usage exceeds limit.

    Args:
        max_memory_mb: Maximum allowed memory in MB

    Returns:
        Tuple of (is_within_limit, current_usage_mb)
    """
    current_mb = get_memory_usage_mb()
    is_within_limit = current_mb <= max_memory_mb
    return (is_within_limit, current_mb)


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    if size_bytes < 0:
        return "0 B"

    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"


# =============================================================================
# Error Code Constants
# =============================================================================


# Default file size limits
DEFAULT_MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB
WARN_FILE_SIZE_BYTES = 1 * 1024 * 1024 * 1024  # 1 GB - warn threshold


class ErrorCodes:
    """Standard error codes for validation errors."""

    # File errors
    FILE_NOT_FOUND = "file_not_found"
    NOT_A_FILE = "not_a_file"
    EMPTY_FILE = "empty_file"
    PERMISSION_DENIED = "permission_denied"
    CANNOT_ACCESS_FILE = "cannot_access_file"
    UNSUPPORTED_FORMAT = "unsupported_format"
    FILE_TOO_LARGE = "file_too_large"

    # Directory errors
    DIRECTORY_NOT_FOUND = "directory_not_found"
    NOT_A_DIRECTORY = "not_a_directory"

    # Schema errors
    MISSING_REQUIRED_FIELD = "missing_required_field"
    INVALID_FIELD_TYPE = "invalid_field_type"
    INVALID_FIELD_VALUE = "invalid_field_value"
    SCHEMA_VALIDATION_FAILED = "schema_validation_failed"

    # Parse errors
    PARSE_ERROR = "parse_error"
    MALFORMED_ROW = "malformed_row"
    INVALID_TIMESTAMP = "invalid_timestamp"
    INVALID_ENCODING = "invalid_encoding"

    # Resource errors
    MEMORY_LIMIT_EXCEEDED = "memory_limit_exceeded"
    TIMEOUT = "timeout"
    PROCESSING_ERROR = "processing_error"
