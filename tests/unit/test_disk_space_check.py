"""Tests for disk space check utility."""

from unittest.mock import patch

import pytest

from llm_bot_pipeline.storage.base import DiskSpaceError
from llm_bot_pipeline.storage.disk_space import check_disk_space


def _make_usage(free_bytes: int):
    """Create a fake shutil.disk_usage result."""
    total = 100 * 1024 * 1024 * 1024  # 100 GB
    used = total - free_bytes
    # shutil.disk_usage returns a named tuple with (total, used, free)
    from collections import namedtuple

    DiskUsage = namedtuple("usage", ["total", "used", "free"])
    return DiskUsage(total=total, used=used, free=free_bytes)


class TestDiskSpaceCheck:
    """Tests for check_disk_space utility."""

    @patch("llm_bot_pipeline.storage.disk_space.shutil.disk_usage")
    def test_disk_space_sufficient(self, mock_disk_usage, tmp_path):
        """10 GB free should not raise with default threshold."""
        free_10gb = 10 * 1024 * 1024 * 1024
        mock_disk_usage.return_value = _make_usage(free_10gb)

        check_disk_space(tmp_path, threshold_mb=500)

        mock_disk_usage.assert_called_once()

    @patch("llm_bot_pipeline.storage.disk_space.shutil.disk_usage")
    def test_disk_space_insufficient(self, mock_disk_usage, tmp_path):
        """100 MB free should raise DiskSpaceError with default threshold."""
        free_100mb = 100 * 1024 * 1024
        mock_disk_usage.return_value = _make_usage(free_100mb)

        with pytest.raises(DiskSpaceError, match="Insufficient disk space"):
            check_disk_space(tmp_path, threshold_mb=500)

    @patch("llm_bot_pipeline.storage.disk_space.shutil.disk_usage")
    def test_disk_space_threshold_configurable(self, mock_disk_usage, tmp_path):
        """Custom threshold of 50 MB should pass with 100 MB free."""
        free_100mb = 100 * 1024 * 1024
        mock_disk_usage.return_value = _make_usage(free_100mb)

        # 100 MB free with 50 MB threshold should pass
        check_disk_space(tmp_path, threshold_mb=50)

        # 100 MB free with 200 MB threshold should fail
        with pytest.raises(DiskSpaceError):
            check_disk_space(tmp_path, threshold_mb=200)

    @patch("llm_bot_pipeline.storage.disk_space.shutil.disk_usage")
    def test_disk_space_nonexistent_path_resolves_to_parent(self, mock_disk_usage):
        """Check resolves to an existing parent when path doesn't exist yet."""
        free_10gb = 10 * 1024 * 1024 * 1024
        mock_disk_usage.return_value = _make_usage(free_10gb)

        check_disk_space("/tmp/nonexistent/db/file.db", threshold_mb=500)

        call_path = mock_disk_usage.call_args[0][0]
        assert call_path.exists()
