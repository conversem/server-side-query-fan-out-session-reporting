"""
Fastly CDN adapter for log ingestion.

Supports ingestion from Fastly real-time log streaming via:
- JSON exports (fastly_json_file)
- CSV exports (fastly_csv_file)
- NDJSON exports (fastly_ndjson_file)

Fastly logs are highly configurable - field names depend on customer
configuration. The adapter supports configurable field mapping via options.
"""

from .adapter import FastlyAdapter

__all__ = ["FastlyAdapter"]
