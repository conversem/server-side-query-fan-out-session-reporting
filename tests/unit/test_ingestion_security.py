"""
Unit tests for ingestion security utilities.

Tests path traversal protection, input validation, and other security features.
"""

import tempfile
from pathlib import Path

import pytest

from llm_bot_pipeline.ingestion.security import (
    PathTraversalError,
    RateLimiter,
    check_rate_limit,
    get_rate_limiter,
    sanitize_path,
    sanitize_string,
    validate_encoding,
    validate_field_length,
    validate_path_component,
    validate_path_safe,
)


class TestValidatePathSafe:
    """Tests for validate_path_safe function."""

    def test_valid_absolute_path(self, tmp_path):
        """Valid absolute path should pass validation."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        is_safe, error_msg = validate_path_safe(test_file)
        assert is_safe is True
        assert error_msg == ""

    def test_valid_relative_path(self, tmp_path):
        """Valid relative path should pass validation."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        # Change to tmp_path and use relative path
        import os

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            is_safe, error_msg = validate_path_safe(Path("test.txt"))
            assert is_safe is True
            assert error_msg == ""
        finally:
            os.chdir(original_cwd)

    def test_path_traversal_with_dotdot(self):
        """Path with '..' should fail validation."""
        is_safe, error_msg = validate_path_safe(Path("../etc/passwd"))
        assert is_safe is False
        assert "directory traversal" in error_msg.lower()

    def test_path_traversal_with_multiple_dotdot(self):
        """Path with multiple '..' should fail validation."""
        is_safe, error_msg = validate_path_safe(Path("../../../../../../etc/passwd"))
        assert is_safe is False
        assert "directory traversal" in error_msg.lower()

    def test_path_traversal_in_middle(self):
        """Path with '..' in the middle should fail validation."""
        is_safe, error_msg = validate_path_safe(Path("logs/../../../etc/passwd"))
        assert is_safe is False
        assert "directory traversal" in error_msg.lower()

    def test_null_byte_in_path(self):
        """Path with null byte should fail validation."""
        is_safe, error_msg = validate_path_safe(Path("test\x00.txt"))
        assert is_safe is False
        assert "null byte" in error_msg.lower()

    def test_base_directory_constraint_valid(self, tmp_path):
        """Path within base directory should pass validation."""
        test_file = tmp_path / "logs" / "test.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("test")

        is_safe, error_msg = validate_path_safe(
            test_file, base_dir=tmp_path, check_exists=True
        )
        assert is_safe is True
        assert error_msg == ""

    def test_base_directory_constraint_escape_fails(self, tmp_path):
        """Path escaping base directory should fail validation."""
        # Create base_dir
        base_dir = tmp_path / "logs"
        base_dir.mkdir()

        # Try to escape using absolute path
        outside_path = tmp_path / "secrets" / "password.txt"
        outside_path.parent.mkdir(parents=True, exist_ok=True)
        outside_path.write_text("secret")

        is_safe, error_msg = validate_path_safe(outside_path, base_dir=base_dir)
        assert is_safe is False
        assert "escapes base directory" in error_msg.lower()

    def test_symlink_detection(self, tmp_path):
        """Symlinks should be detected when allow_symlinks=False."""
        # Create a real file
        real_file = tmp_path / "real.txt"
        real_file.write_text("real")

        # Create a symlink
        symlink = tmp_path / "link.txt"
        try:
            symlink.symlink_to(real_file)
        except OSError:
            pytest.skip("Cannot create symlinks (requires privileges)")

        # Should fail with allow_symlinks=False
        is_safe, error_msg = validate_path_safe(symlink, allow_symlinks=False)
        assert is_safe is False
        assert "symbolic link" in error_msg.lower()

        # Should pass with allow_symlinks=True
        is_safe, error_msg = validate_path_safe(symlink, allow_symlinks=True)
        assert is_safe is True

    def test_nonexistent_path_with_check_exists(self):
        """Nonexistent path should fail when check_exists=True."""
        is_safe, error_msg = validate_path_safe(
            Path("/nonexistent/path/file.txt"), check_exists=True
        )
        assert is_safe is False
        assert "does not exist" in error_msg.lower()

    def test_nonexistent_path_without_check_exists(self):
        """Nonexistent path should pass when check_exists=False."""
        is_safe, error_msg = validate_path_safe(
            Path("/tmp/nonexistent_but_safe_path.txt"), check_exists=False
        )
        assert is_safe is True


class TestValidatePathComponent:
    """Tests for validate_path_component function."""

    def test_valid_filename(self):
        """Valid filename should pass validation."""
        is_valid, error_msg = validate_path_component("test_file.csv")
        assert is_valid is True
        assert error_msg == ""

    def test_empty_component(self):
        """Empty component should fail validation."""
        is_valid, error_msg = validate_path_component("")
        assert is_valid is False
        assert "empty" in error_msg.lower()

    def test_dot_component(self):
        """Single dot component should fail validation."""
        is_valid, error_msg = validate_path_component(".")
        assert is_valid is False

    def test_dotdot_component(self):
        """Double dot component should fail validation."""
        is_valid, error_msg = validate_path_component("..")
        assert is_valid is False

    def test_path_separator_in_component(self):
        """Path separator in component should fail validation."""
        is_valid, error_msg = validate_path_component("dir/file.txt")
        assert is_valid is False
        assert "path separator" in error_msg.lower()

    def test_null_byte_in_component(self):
        """Null byte in component should fail validation."""
        is_valid, error_msg = validate_path_component("file\x00.txt")
        assert is_valid is False
        assert "null byte" in error_msg.lower()


class TestSanitizePath:
    """Tests for sanitize_path function."""

    def test_sanitize_valid_path(self, tmp_path):
        """Valid path should be sanitized and returned."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        result = sanitize_path(test_file)
        assert result.is_absolute()
        assert result.exists()

    def test_sanitize_traversal_raises_error(self):
        """Path with traversal should raise PathTraversalError."""
        with pytest.raises(PathTraversalError) as exc_info:
            sanitize_path(Path("../etc/passwd"))

        assert "traversal" in str(exc_info.value).lower()

    def test_sanitize_with_base_dir(self, tmp_path):
        """Path within base_dir should pass."""
        test_file = tmp_path / "logs" / "test.txt"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("test")

        result = sanitize_path(test_file, base_dir=tmp_path)
        assert result.is_absolute()

    def test_sanitize_escaping_base_dir_raises(self, tmp_path):
        """Path escaping base_dir should raise PathTraversalError."""
        base_dir = tmp_path / "logs"
        base_dir.mkdir()

        outside_path = tmp_path / "secrets" / "password.txt"
        outside_path.parent.mkdir(parents=True, exist_ok=True)
        outside_path.write_text("secret")

        with pytest.raises(PathTraversalError) as exc_info:
            sanitize_path(outside_path, base_dir=base_dir)

        assert "escapes" in str(exc_info.value).lower()


class TestValidateFieldLength:
    """Tests for validate_field_length function."""

    def test_valid_field_length(self):
        """Field within max length should pass."""
        is_valid, error_msg = validate_field_length("test_field", "short value")
        assert is_valid is True
        assert error_msg == ""

    def test_field_exceeds_max_length(self):
        """Field exceeding max length should fail."""
        is_valid, error_msg = validate_field_length(
            "test_field", "a" * 100, max_length=50
        )
        assert is_valid is False
        assert "exceeds maximum length" in error_msg.lower()

    def test_none_value(self):
        """None value should pass validation."""
        is_valid, error_msg = validate_field_length("test_field", None)
        assert is_valid is True

    def test_exact_max_length(self):
        """Field at exact max length should pass."""
        is_valid, error_msg = validate_field_length(
            "test_field", "a" * 50, max_length=50
        )
        assert is_valid is True


class TestSanitizeString:
    """Tests for sanitize_string function."""

    def test_clean_string(self):
        """Clean string should be unchanged."""
        result = sanitize_string("Hello, World!")
        assert result == "Hello, World!"

    def test_removes_control_characters(self):
        """Control characters should be removed."""
        result = sanitize_string("Hello\x00\x01\x02World")
        assert result == "HelloWorld"

    def test_preserves_common_whitespace(self):
        """Tab, newline, carriage return should be preserved."""
        result = sanitize_string("Hello\t\n\rWorld")
        assert result == "Hello\t\n\rWorld"

    def test_truncates_to_max_length(self):
        """Long strings should be truncated."""
        result = sanitize_string("a" * 100, max_length=50)
        assert len(result) == 50

    def test_empty_string(self):
        """Empty string should be handled."""
        result = sanitize_string("")
        assert result == ""


class TestAdapterSecurityIntegration:
    """Integration tests for security features in adapters."""

    @pytest.fixture
    def fixtures_dir(self) -> Path:
        """Return path to test fixtures directory."""
        return Path(__file__).parent.parent / "fixtures" / "ingestion"

    def test_universal_adapter_path_traversal_blocked(
        self, fixtures_dir, register_providers
    ):
        """UniversalAdapter should block path traversal attempts."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri="../../../etc/passwd",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "security" in error_msg.lower() or "traversal" in error_msg.lower()

    def test_universal_adapter_base_dir_constraint(
        self, fixtures_dir, tmp_path, register_providers
    ):
        """UniversalAdapter should respect base_dir constraint."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        # Create a file outside the base_dir
        outside_file = tmp_path / "outside" / "test.csv"
        outside_file.parent.mkdir(parents=True, exist_ok=True)
        outside_file.write_text(
            "timestamp,client_ip,method,host,path,status_code,user_agent\n"
        )

        # Create base_dir
        base_dir = tmp_path / "allowed"
        base_dir.mkdir()

        adapter = get_adapter("universal")
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri=str(outside_file),
        )

        is_valid, error_msg = adapter.validate_source(source, base_dir=base_dir)
        assert is_valid is False
        assert "escapes" in error_msg.lower() or "security" in error_msg.lower()

    def test_cloudfront_adapter_path_traversal_blocked(
        self, fixtures_dir, register_providers
    ):
        """CloudFrontAdapter should block path traversal attempts."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        adapter = get_adapter("aws_cloudfront")
        source = IngestionSource(
            provider="aws_cloudfront",
            source_type="w3c_file",
            path_or_uri="../../../etc/passwd",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "security" in error_msg.lower() or "traversal" in error_msg.lower()

    def test_cloudflare_adapter_path_traversal_blocked(
        self, fixtures_dir, register_providers
    ):
        """CloudflareAdapter should block path traversal attempts for file sources."""
        from llm_bot_pipeline.ingestion import IngestionSource, get_adapter

        adapter = get_adapter("cloudflare")
        source = IngestionSource(
            provider="cloudflare",
            source_type="csv_file",
            path_or_uri="../../../etc/passwd",
        )

        is_valid, error_msg = adapter.validate_source(source)
        assert is_valid is False
        assert "security" in error_msg.lower() or "traversal" in error_msg.lower()


class TestSuspiciousPathPatterns:
    """Tests for detection of suspicious path patterns."""

    def test_tilde_expansion_blocked(self):
        """Tilde home directory expansion should be blocked."""
        is_safe, error_msg = validate_path_safe(Path("~/.ssh/id_rsa"))
        assert is_safe is False
        assert "suspicious" in error_msg.lower()

    def test_variable_expansion_blocked(self):
        """Variable expansion should be blocked."""
        is_safe, error_msg = validate_path_safe(Path("${HOME}/secrets"))
        assert is_safe is False
        assert "suspicious" in error_msg.lower()

    def test_command_substitution_blocked(self):
        """Command substitution should be blocked."""
        is_safe, error_msg = validate_path_safe(Path("$(whoami)/data"))
        assert is_safe is False
        assert "suspicious" in error_msg.lower()

    def test_backtick_substitution_blocked(self):
        """Backtick command substitution should be blocked."""
        is_safe, error_msg = validate_path_safe(Path("`id`/data"))
        assert is_safe is False
        assert "suspicious" in error_msg.lower()

    def test_pipe_character_blocked(self):
        """Pipe character should be blocked."""
        is_safe, error_msg = validate_path_safe(Path("file.txt|cat"))
        assert is_safe is False
        assert "suspicious" in error_msg.lower()

    def test_semicolon_blocked(self):
        """Semicolon command separator should be blocked."""
        is_safe, error_msg = validate_path_safe(Path("file.txt;rm -rf /"))
        assert is_safe is False
        assert "suspicious" in error_msg.lower()


class TestRateLimiter:
    """Tests for RateLimiter class."""

    def test_acquire_within_limit(self):
        """Should allow requests within limit."""
        limiter = RateLimiter(max_requests=5, window_seconds=60.0)

        for _ in range(5):
            assert limiter.acquire() is True

    def test_acquire_exceeds_limit(self):
        """Should reject requests exceeding limit."""
        limiter = RateLimiter(max_requests=3, window_seconds=60.0)

        # First 3 should succeed
        for _ in range(3):
            assert limiter.acquire() is True

        # 4th should fail
        assert limiter.acquire() is False

    def test_remaining_requests(self):
        """Should correctly track remaining requests."""
        limiter = RateLimiter(max_requests=5, window_seconds=60.0)

        assert limiter.remaining_requests == 5
        limiter.acquire()
        assert limiter.remaining_requests == 4
        limiter.acquire()
        assert limiter.remaining_requests == 3

    def test_reset(self):
        """Should reset rate limiter."""
        limiter = RateLimiter(max_requests=3, window_seconds=60.0)

        # Use up all requests
        for _ in range(3):
            limiter.acquire()

        assert limiter.remaining_requests == 0

        # Reset
        limiter.reset()
        assert limiter.remaining_requests == 3

    def test_get_rate_limiter_caching(self):
        """Should return same rate limiter for same key."""
        limiter1 = get_rate_limiter("test_key", max_requests=10, window_seconds=60.0)
        limiter2 = get_rate_limiter("test_key", max_requests=10, window_seconds=60.0)

        assert limiter1 is limiter2

    def test_check_rate_limit_function(self):
        """Should check rate limit using convenience function."""
        # Use unique key to avoid conflicts with other tests
        import uuid

        key = f"test_{uuid.uuid4()}"

        # First request should succeed
        assert check_rate_limit(key, max_requests=2, window_seconds=60.0) is True
        assert check_rate_limit(key, max_requests=2, window_seconds=60.0) is True
        # Third should fail
        assert check_rate_limit(key, max_requests=2, window_seconds=60.0) is False


class TestEncodingValidation:
    """Tests for encoding validation."""

    def test_valid_utf8(self):
        """Valid UTF-8 should pass."""
        is_valid, error_msg = validate_encoding("Hello, World!".encode("utf-8"))
        assert is_valid is True
        assert error_msg == ""

    def test_invalid_utf8(self):
        """Invalid UTF-8 should fail."""
        # Create invalid UTF-8 bytes
        invalid_bytes = b"\xff\xfe"
        is_valid, error_msg = validate_encoding(invalid_bytes)
        assert is_valid is False
        assert "invalid" in error_msg.lower() or "utf-8" in error_msg.lower()

    def test_valid_latin1(self):
        """Valid Latin-1 should pass when specified."""
        # Latin-1 encoded string
        latin1_bytes = "Héllo".encode("latin-1")
        is_valid, error_msg = validate_encoding(
            latin1_bytes, expected_encoding="latin-1"
        )
        assert is_valid is True


class TestFieldLengthValidation:
    """Tests for schema field length validation."""

    def test_field_within_max_length(self):
        """Field within max length should pass validation."""
        from llm_bot_pipeline.ingestion.parsers.schema import validate_field

        is_valid, error_msg = validate_field("client_ip", "192.168.1.1")
        assert is_valid is True
        assert error_msg == ""

    def test_field_exceeds_max_length(self):
        """Field exceeding max length should fail validation."""
        from llm_bot_pipeline.ingestion.parsers.schema import validate_field

        # client_ip has max_length=45 (IPv6 max)
        long_ip = "a" * 100
        is_valid, error_msg = validate_field("client_ip", long_ip)
        # Note: This will fail on IP format validation first
        assert is_valid is False

    def test_user_agent_max_length(self):
        """User agent should respect max length."""
        from llm_bot_pipeline.ingestion.parsers.schema import UNIVERSAL_SCHEMA

        # Check that max_length is defined for user_agent
        field_def = UNIVERSAL_SCHEMA["user_agent"]
        assert field_def.max_length is not None
        assert field_def.max_length == 4096

    def test_path_max_length(self):
        """Path should respect max length."""
        from llm_bot_pipeline.ingestion.parsers.schema import validate_field

        # path has max_length=8192
        long_path = "/" + "a" * 10000
        is_valid, error_msg = validate_field("path", long_path)
        assert is_valid is False
        assert "exceeds maximum length" in error_msg.lower()
