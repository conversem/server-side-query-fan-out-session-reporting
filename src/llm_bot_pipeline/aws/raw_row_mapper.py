"""Map IngestionRecord to Cloudflare-shaped raw_bot_requests rows."""

from __future__ import annotations

from typing import Any, Optional

from ..ingestion.base import IngestionRecord
from ..utils.bot_classifier import BotClassification


def map_to_raw_row(
    record: IngestionRecord,
    *,
    domain: str,
    source_provider: str,
    bot_info: Optional[BotClassification],
) -> dict[str, Any]:
    """Map an IngestionRecord to a raw_bot_requests row dict."""
    if record.timestamp is None:
        raise ValueError("IngestionRecord.timestamp is required")

    uri = record.path or "/"
    if record.query_string:
        uri = f"{uri}?{record.query_string}"

    ts_us = int(record.timestamp.timestamp() * 1_000_000)
    edge_start_ns = ts_us * 1_000

    return {
        "EdgeStartTimestamp": edge_start_ns,
        "ClientRequestURI": uri,
        "ClientRequestHost": record.host,
        "domain": domain,
        "ClientRequestUserAgent": record.user_agent,
        "ClientIP": record.client_ip,
        "ClientCountry": None,
        "EdgeResponseStatus": record.status_code,
        "RayID": None,
        "_bot_name": bot_info.bot_name if bot_info else None,
        "_bot_provider": bot_info.bot_provider if bot_info else None,
        "_bot_category": bot_info.bot_category if bot_info else None,
        "source_provider": source_provider,
    }
