"""
Pure Python transformer for IngestionRecord -> clean record dicts.

Used by StreamingPipeline (local_bq_streaming mode) to transform records
in-memory without SQL or SQLite. Reuses classify_bot() for bot detection.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from ..config.settings import UrlFilteringSettings
from ..ingestion.base import IngestionRecord
from ..utils.bot_classifier import classify_bot
from ..utils.url_classifier import classify_url

logger = logging.getLogger(__name__)

# Day-of-week lookup (datetime.weekday() returns 0=Monday..6=Sunday)
_WEEKDAY_NAMES = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)


def _status_category(code: int) -> str:
    """Map HTTP status code to category string."""
    if 200 <= code < 300:
        return "2xx"
    elif 300 <= code < 400:
        return "3xx"
    elif 400 <= code < 500:
        return "4xx"
    elif 500 <= code < 600:
        return "5xx"
    return "other"


def extract_url_path(path: str) -> str:
    """Strip query string and fragment from a URL path."""
    return path.split("?")[0].split("#")[0]


def url_path_depth(path: str) -> int:
    """Count path segments (e.g. '/a/b/c' -> 3)."""
    if path in ("/", "", None):
        return 0
    stripped = path.strip("/")
    return len(stripped.split("/")) if stripped else 0


class PythonTransformer:
    """Transform IngestionRecord objects to clean record dicts in pure Python.

    Performs the same logic as the SQL-based LocalPipeline transform but
    entirely in memory, making it suitable for streaming to BigQuery
    without an intermediate SQLite step.

    Deduplication is tracked per-instance via a seen-set of
    (timestamp_iso, client_ip, path, host) tuples. Call reset_dedup()
    between date boundaries or batches if needed.
    """

    def __init__(
        self, url_filtering_settings: Optional[UrlFilteringSettings] = None
    ) -> None:
        self._seen: set[tuple] = set()
        self._stats = {
            "transformed": 0,
            "filtered": 0,
            "duplicates": 0,
            "url_filtered": 0,
        }
        self._url_settings = url_filtering_settings or UrlFilteringSettings()

    def reset_dedup(self) -> None:
        """Clear the deduplication set (e.g. between dates)."""
        self._seen.clear()

    def reset_seen(self) -> None:
        """Clear the deduplication set. Alias for reset_dedup()."""
        self.reset_dedup()

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    def transform(self, record: IngestionRecord) -> Optional[dict]:
        """Transform a single IngestionRecord to a clean record dict.

        Returns None if the record should be filtered out (not a known bot
        or is a duplicate).
        """
        classification = classify_bot(record.user_agent)
        if classification is None:
            self._stats["filtered"] += 1
            return None

        dedup_key = (
            record.timestamp.isoformat(),
            record.client_ip,
            record.path,
            record.host,
        )
        if dedup_key in self._seen:
            self._stats["duplicates"] += 1
            return None
        self._seen.add(dedup_key)

        url_path = extract_url_path(record.path)

        resource_type = classify_url(url_path, self._url_settings)
        if resource_type is None:
            self._stats["url_filtered"] += 1
            return None

        clean = {
            "request_timestamp": record.timestamp.isoformat(),
            "request_date": record.timestamp.date().isoformat(),
            "request_hour": record.timestamp.hour,
            "day_of_week": _WEEKDAY_NAMES[record.timestamp.weekday()],
            "request_uri": record.path,
            "request_host": record.host,
            "domain": record.extra.get("domain"),
            "url_path": url_path,
            "url_path_depth": url_path_depth(url_path),
            "resource_type": resource_type,
            "user_agent_raw": record.user_agent,
            "bot_name": classification.bot_name,
            "bot_provider": classification.bot_provider,
            "bot_category": classification.bot_category,
            "crawler_country": record.extra.get("ClientCountry", ""),
            "response_status": record.status_code,
            "response_status_category": _status_category(record.status_code),
            "_processed_at": datetime.now(timezone.utc).isoformat(),
        }

        self._stats["transformed"] += 1
        return clean
