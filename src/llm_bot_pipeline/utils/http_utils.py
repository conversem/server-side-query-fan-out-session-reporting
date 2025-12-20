"""
HTTP utility functions.

Helpers for processing HTTP-related data from Cloudflare logs.
"""

from typing import Optional


def get_status_category(status_code: Optional[int]) -> Optional[str]:
    """
    Categorize an HTTP status code into a human-readable category.

    Categories:
        - '2xx_success': Successful responses (200-299)
        - '3xx_redirect': Redirection messages (300-399)
        - '4xx_client_error': Client errors (400-499)
        - '5xx_server_error': Server errors (500-599)

    Args:
        status_code: HTTP status code (e.g., 200, 404, 500)

    Returns:
        Category string, or None if status_code is None or invalid

    Examples:
        >>> get_status_category(200)
        '2xx_success'
        >>> get_status_category(404)
        '4xx_client_error'
        >>> get_status_category(None)
        None
    """
    if status_code is None:
        return None

    if 200 <= status_code < 300:
        return "2xx_success"
    elif 300 <= status_code < 400:
        return "3xx_redirect"
    elif 400 <= status_code < 500:
        return "4xx_client_error"
    elif 500 <= status_code < 600:
        return "5xx_server_error"
    else:
        return None


def is_success_status(status_code: Optional[int]) -> bool:
    """
    Check if status code indicates success (2xx).

    Args:
        status_code: HTTP status code

    Returns:
        True if status is in 2xx range
    """
    return status_code is not None and 200 <= status_code < 300


def is_error_status(status_code: Optional[int]) -> bool:
    """
    Check if status code indicates an error (4xx or 5xx).

    Args:
        status_code: HTTP status code

    Returns:
        True if status is in 4xx or 5xx range
    """
    return status_code is not None and status_code >= 400

