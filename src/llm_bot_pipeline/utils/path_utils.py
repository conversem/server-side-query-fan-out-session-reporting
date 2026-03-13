"""Path validation utilities to prevent path traversal attacks."""

from pathlib import Path


def validate_path_safe(path: str | Path, base_dir: str | Path) -> None:
    """
    Validate that the resolved path is within the base directory.

    Prevents path traversal attacks (e.g. ../../../etc/passwd).

    Args:
        path: The path to validate (e.g. --output argument).
        base_dir: The base directory that the path must be under.

    Raises:
        ValueError: If the resolved path is outside the base directory.
    """
    abs_path = Path(path).resolve()
    base = Path(base_dir).resolve()
    try:
        abs_path.relative_to(base)
    except ValueError:
        raise ValueError("Path outside base directory")
