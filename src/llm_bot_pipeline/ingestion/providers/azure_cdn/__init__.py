"""
Azure CDN / Front Door adapter for log ingestion.

Supports ingestion from Azure CDN and Azure Front Door via:
- Azure Monitor CSV exports
- Log Analytics JSON exports
- NDJSON streaming exports
"""

from .adapter import AzureCDNAdapter

__all__ = ["AzureCDNAdapter"]
