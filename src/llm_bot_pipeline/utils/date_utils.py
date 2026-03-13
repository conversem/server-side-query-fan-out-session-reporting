"""Shared date parsing utilities."""

from datetime import date, datetime, timezone


def utc_now() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def parse_date(s: str) -> date:
    """Parse a YYYY-MM-DD string into a :class:`datetime.date`.

    Raises:
        ValueError: If *s* is not in ``YYYY-MM-DD`` format.
    """
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid date format: {s}. Use YYYY-MM-DD")
