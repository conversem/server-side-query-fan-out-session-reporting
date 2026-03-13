"""Disk space validation utility for storage backends."""

import logging
import shutil
from pathlib import Path

from .base import DiskSpaceError

logger = logging.getLogger(__name__)


def check_disk_space(path: Path | str, threshold_mb: int = 500) -> None:
    """Verify that sufficient disk space is available before write operations.

    Args:
        path: File or directory path to check disk space for.
              Uses the parent directory if the path doesn't exist yet.
        threshold_mb: Minimum required free space in megabytes.

    Raises:
        DiskSpaceError: When available space is below threshold_mb.
    """
    check_path = Path(path)
    while not check_path.exists():
        check_path = check_path.parent

    usage = shutil.disk_usage(check_path)
    free_mb = usage.free / (1024 * 1024)

    if free_mb < threshold_mb:
        raise DiskSpaceError(
            f"Insufficient disk space: {free_mb:.0f} MB available, "
            f"{threshold_mb} MB required. Path: {path}"
        )

    logger.debug(
        "Disk space check passed: %.0f MB free (threshold: %d MB)",
        free_mb,
        threshold_mb,
    )
