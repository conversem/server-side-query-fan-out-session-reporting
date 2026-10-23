"""Shared date parsing utilities."""

from datetime import date, datetime, timedelta, timezone


def utc_now() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def utc_yesterday(now: datetime | None = None) -> date:
    """Return yesterday's date in UTC (not the process local calendar)."""
    current = now or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).date() - timedelta(days=1)


def parse_date(s: str) -> date:
    """Parse a YYYY-MM-DD string into a :class:`datetime.date`.

    Raises:
        ValueError: If *s* is not in ``YYYY-MM-DD`` format.
    """
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid date format: {s}. Use YYYY-MM-DD")
