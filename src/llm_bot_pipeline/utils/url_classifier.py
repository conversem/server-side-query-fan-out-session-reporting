"""URL resource type classifier.

Classifies URL paths as user-facing ('document', 'image') or
non-user-facing (None = drop). Reads all rules from UrlFilteringSettings
so they can be customized per deployment via YAML config.
"""

from typing import Optional

from ..config.settings import UrlFilteringSettings


def classify_url(url_path: str, settings: UrlFilteringSettings) -> Optional[str]:
    """Classify a URL path by resource type.

    Args:
        url_path: Clean URL path (no query string or fragment).
        settings: URL filtering configuration.

    Returns:
        "document" for user-facing pages/files,
        "image" for image files,
        None for non-user-facing assets (should be dropped).
    """
    if not settings.enabled:
        return "document"

    if not url_path or url_path == "/":
        return "document"

    ext = _extract_extension(url_path)

    if ext and ext in settings.drop_extensions:
        return None

    if ext and ext in settings.image_extensions:
        return "image"

    lower_path = url_path.lower()
    for prefix in settings.drop_path_prefixes:
        if lower_path.startswith(prefix.lower()):
            return None

    return "document"


def _extract_extension(url_path: str) -> Optional[str]:
    """Extract lowercase file extension from the last path segment.

    Returns None if no extension found (e.g. '/about', '/').
    """
    basename = url_path.rsplit("/", 1)[-1]
    if "." not in basename:
        return None
    ext = basename.rsplit(".", 1)[-1].lower()
    if not ext:
        return None
    return ext
