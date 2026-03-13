"""Tests for path validation (validate_path_safe)."""

import pytest

from llm_bot_pipeline.utils.path_utils import validate_path_safe


def test_output_normal_path(tmp_path):
    """Normal path under base is accepted."""
    base = tmp_path / "reports"
    base.mkdir()
    path = base / "out.xlsx"
    validate_path_safe(path, tmp_path)


def test_output_traversal_rejected(tmp_path):
    """Path traversal (../) outside base raises ValueError."""
    base = tmp_path / "reports"
    base.mkdir()
    path = base / ".." / ".." / ".." / "etc" / "out.xlsx"
    with pytest.raises(ValueError, match="Path outside base directory"):
        validate_path_safe(path, tmp_path)


def test_output_absolute_outside_base(tmp_path):
    """Absolute path outside base raises ValueError."""
    with pytest.raises(ValueError, match="Path outside base directory"):
        validate_path_safe("/tmp/evil.xlsx", tmp_path)
