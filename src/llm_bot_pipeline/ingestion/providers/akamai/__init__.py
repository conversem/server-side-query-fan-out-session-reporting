"""
Akamai DataStream 2 adapter for log ingestion.

Supports ingestion from Akamai DataStream via:
- JSON exports (akamai_json_file)
- NDJSON exports (akamai_ndjson_file)

Akamai uses CamelCase field names (requestTime, clientIP, etc.)
that are mapped to the universal schema.
"""

from .adapter import AkamaiAdapter

__all__ = ["AkamaiAdapter"]
