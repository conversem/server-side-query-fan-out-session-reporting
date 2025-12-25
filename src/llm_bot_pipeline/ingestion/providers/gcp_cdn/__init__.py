"""
Google Cloud CDN adapter for log ingestion.

Supports ingestion from GCP Cloud CDN and HTTP(S) Load Balancer via:
- Cloud Logging JSON exports
- NDJSON streaming exports

The adapter handles:
- Nested httpRequest structure flattening
- RFC3339 timestamp parsing
- URL extraction for host/path from requestUrl
"""

from .adapter import GCPCDNAdapter

__all__ = ["GCPCDNAdapter"]
