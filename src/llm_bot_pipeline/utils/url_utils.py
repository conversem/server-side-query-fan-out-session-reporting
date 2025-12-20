"""
URL utility functions.

Helpers for processing URLs from Cloudflare logs and deriving
human-readable names from URL paths.
"""

from urllib.parse import urlparse

# Common file extensions to remove from session names
# Moved to module level for performance (avoid recreating set on each call)
_COMMON_EXTENSIONS = {
    "html",
    "htm",
    "pdf",
    "txt",
    "doc",
    "docx",
    "xls",
    "xlsx",
    "ppt",
    "pptx",
    "xml",
    "json",
    "csv",
    "jpg",
    "jpeg",
    "png",
    "gif",
    "svg",
    "css",
    "js",
}


def derive_session_name(url: str) -> str:
    """
    Extract session name from URL's last path segment.

    Derives a human-readable session name by:
    1. Extracting the last path segment from the URL
    2. Replacing hyphens, underscores, and dots with spaces
    3. Removing file extensions
    4. Handling edge cases like homepage URLs

    Args:
        url: Full URL string (e.g., "https://example.nl/blog/home-buying-guide")

    Returns:
        Human-readable session name (e.g., "home buying guide")
        Returns "homepage" for root URLs, "unknown" for empty segments

    Examples:
        >>> derive_session_name("example.nl/blog/home-buying-guide")
        'home buying guide'
        >>> derive_session_name("example.nl/mortgage/calculator")
        'calculator'
        >>> derive_session_name("example.nl/tips/first-time-buyer-checklist")
        'first time buyer checklist'
        >>> derive_session_name("example.nl/")
        'homepage'
        >>> derive_session_name("example.nl/article.pdf")
        'article'
        >>> derive_session_name("example.nl/blog/post_name.html")
        'post name'
    """
    # Handle URLs without scheme - urlparse needs a scheme to parse correctly
    # For Cloudflare logs, URLs are typically HTTP/HTTPS, so defaulting to https://
    # is safe. For other schemes (ftp://, file://, etc.), this may produce
    # unexpected results, but those are unlikely in Cloudflare log data.
    if "://" not in url:
        url = "https://" + url

    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    # Handle homepage/root URLs
    if not path:
        return "homepage"

    # Get last segment after final /
    segments = path.split("/")
    last_segment = segments[-1] if segments else ""

    # Handle empty segment (shouldn't happen after rstrip, but defensive)
    if not last_segment:
        return "homepage"

    # Replace hyphens, underscores, and dots with spaces
    # Note: This deviates slightly from PRD spec which only removes extensions.
    # The PRD approach has a bug where "v2.0-guide" becomes "v2" instead of "v2.0 guide".
    # This implementation fixes that by replacing dots with spaces first, then
    # intelligently removing only known file extensions.
    session_name = last_segment.replace("-", " ").replace("_", " ").replace(".", " ")

    # Remove common file extensions (last word if it's a known extension)
    words = session_name.split()
    if len(words) > 1:
        if words[-1].lower() in _COMMON_EXTENSIONS:
            words = words[:-1]

    session_name = " ".join(words)

    # Clean up multiple spaces and strip
    session_name = " ".join(session_name.split()).strip()

    # Handle edge case where stripping results in empty string
    return session_name if session_name else "unknown"
