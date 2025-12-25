"""
Shared file utilities for ingestion module.

Provides common file operations used across multiple adapters and parsers.
"""

import gzip
from pathlib import Path
from typing import IO, Union


def open_file_auto_decompress(
    file_path: Union[str, Path],
    encoding: str = "utf-8",
) -> IO[str]:
    """
    Open a file, automatically detecting gzip compression.

    Gzip detection is performed by:
    1. Checking for .gz file extension
    2. Checking for gzip magic bytes (0x1f 0x8b) even without .gz extension

    Args:
        file_path: Path to the file
        encoding: Text encoding (default: utf-8)

    Returns:
        Open file handle (text mode)

    Raises:
        FileNotFoundError: If file doesn't exist
        PermissionError: If file cannot be read
        gzip.BadGzipFile: If file has .gz extension but is not valid gzip
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Check for gzip by extension
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", encoding=encoding)

    # Also check magic bytes for gzip files without .gz extension
    with open(path, "rb") as f:
        magic = f.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(path, "rt", encoding=encoding)

    return open(path, "r", encoding=encoding)
