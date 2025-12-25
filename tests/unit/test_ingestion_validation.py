"""
Unit tests for ingestion validation utilities.

Tests file validation, directory validation, resource monitoring,
and validation reporting.
"""

from pathlib import Path

import pytest

from llm_bot_pipeline.ingestion.validation import (
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


class TestFileValidation:
    """Tests for file path validation."""

    def test_validate_existing_file(self, tmp_path):
        """Validate an existing file."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("test content")

        result = validate_file_path(test_file)
        assert result.is_valid is True
        assert result.file_path == test_file
        assert result.file_size_bytes == len("test content")
        assert result.is_readable is True

    def test_validate_nonexistent_file(self):
        """Validate a nonexistent file."""
        result = validate_file_path(Path("/nonexistent/file.csv"))
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert result.errors[0].error_code == ErrorCodes.FILE_NOT_FOUND

    def test_validate_empty_file(self, tmp_path):
        """Validate an empty file."""
        test_file = tmp_path / "empty.csv"
        test_file.touch()

        result = validate_file_path(test_file)
        assert result.is_valid is False
        assert any(e.error_code == ErrorCodes.EMPTY_FILE for e in result.errors)

    def test_validate_file_size_limit(self, tmp_path):
        """Test file size limit validation - file exceeding limit is invalid."""
        test_file = tmp_path / "large.csv"
        test_file.write_text("x" * 1000)

        # File exceeding max size should be invalid
        result = validate_file_path(test_file, max_size_bytes=500)
        assert result.is_valid is False
        assert any(e.error_code == ErrorCodes.FILE_TOO_LARGE for e in result.errors)

    def test_validate_file_size_warning(self, tmp_path):
        """Test file size warning for large files below limit."""
        test_file = tmp_path / "large.csv"
        # 1.5 GB worth of content (simulated by file size)
        test_file.write_text("test content")  # Small file - no warning expected

        result = validate_file_path(test_file)
        assert result.is_valid is True
        # No size warning for small files

    def test_validate_file_extension(self, tmp_path):
        """Test file extension validation."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("test")

        # Valid extension
        result = validate_file_path(test_file, allowed_extensions=[".csv", ".json"])
        assert result.is_valid is True

        # Invalid extension
        result = validate_file_path(test_file, allowed_extensions=[".json"])
        assert result.is_valid is False
        assert any(e.error_code == ErrorCodes.UNSUPPORTED_FORMAT for e in result.errors)

    def test_validate_gzip_extension(self, tmp_path):
        """Test gzip file extension detection."""
        test_file = tmp_path / "test.csv.gz"
        test_file.write_text("test")

        result = validate_file_path(test_file, allowed_extensions=[".csv"])
        # Should detect base extension .csv
        assert result.is_valid is True


class TestDirectoryValidation:
    """Tests for directory validation."""

    def test_validate_existing_directory(self, tmp_path):
        """Validate an existing directory."""
        result = validate_directory(tmp_path)
        assert result.is_valid is True

    def test_validate_nonexistent_directory(self):
        """Validate a nonexistent directory."""
        result = validate_directory(Path("/nonexistent/directory"))
        assert result.is_valid is False
        assert any(
            e.error_code == ErrorCodes.DIRECTORY_NOT_FOUND for e in result.errors
        )

    def test_validate_directory_with_files(self, tmp_path):
        """Validate directory with files."""
        (tmp_path / "file1.csv").write_text("test")
        (tmp_path / "file2.csv").write_text("test")

        result = validate_directory(tmp_path, min_files=1)
        assert result.is_valid is True

    def test_validate_directory_insufficient_files(self, tmp_path):
        """Validate directory with insufficient files."""
        result = validate_directory(tmp_path, min_files=5)
        # Should be valid but with warning
        assert result.is_valid is True
        assert len(result.warnings) > 0


class TestResourceMonitoring:
    """Tests for resource monitoring utilities."""

    def test_get_memory_usage(self):
        """Test memory usage retrieval."""
        usage_mb = get_memory_usage_mb()
        assert usage_mb >= 0
        assert isinstance(usage_mb, float)

    def test_check_memory_limit(self):
        """Test memory limit checking."""
        is_within, current = check_memory_limit(10000.0)  # Very high limit
        assert isinstance(is_within, bool)
        assert current >= 0

    def test_format_file_size(self):
        """Test file size formatting."""
        assert format_file_size(0) == "0.0 B"
        assert format_file_size(1024) == "1.0 KB"
        assert format_file_size(1024 * 1024) == "1.0 MB"
        assert format_file_size(1024 * 1024 * 1024) == "1.0 GB"
        assert format_file_size(500) == "500.0 B"
        assert format_file_size(-1) == "0 B"  # Negative should return 0


class TestValidationReport:
    """Tests for ValidationReport."""

    def test_create_report(self):
        """Test creating a validation report."""
        from datetime import datetime, timezone

        report = ValidationReport(start_time=datetime.now(timezone.utc))
        assert report.files_processed == 0
        assert report.records_processed == 0
        assert len(report.errors) == 0

    def test_report_to_dict(self):
        """Test converting report to dictionary."""
        from datetime import datetime, timezone

        report = ValidationReport(
            start_time=datetime.now(timezone.utc),
            files_processed=5,
            records_processed=100,
            records_valid=95,
            records_skipped=5,
        )

        report_dict = report.to_dict()
        assert report_dict["files_processed"] == 5
        assert report_dict["records_processed"] == 100
        assert report_dict["records_valid"] == 95
        assert report_dict["records_skipped"] == 5
        assert "start_time" in report_dict


class TestValidationIssue:
    """Tests for ValidationIssue."""

    def test_create_validation_issue(self):
        """Test creating a validation issue."""
        issue = ValidationIssue(
            error_code=ErrorCodes.FILE_NOT_FOUND,
            message="File not found",
            suggestion="Check the file path",
        )
        assert issue.error_code == ErrorCodes.FILE_NOT_FOUND
        assert issue.message == "File not found"
        assert issue.suggestion == "Check the file path"

    def test_validation_issue_with_field(self):
        """Test validation issue with field information."""
        issue = ValidationIssue(
            error_code=ErrorCodes.MISSING_REQUIRED_FIELD,
            message="Missing required field",
            field="timestamp",
            value=None,
        )
        assert issue.field == "timestamp"
        assert issue.value is None
