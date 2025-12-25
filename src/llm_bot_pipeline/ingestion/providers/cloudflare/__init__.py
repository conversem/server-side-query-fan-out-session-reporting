"""
Cloudflare adapter for Logpull API and Logpush file imports.

Supports ingestion from Cloudflare via:
- Logpull API (real-time, 7-day retention)
- Logpush CSV/JSON file exports
"""

from .adapter import CloudflareAdapter

__all__ = ["CloudflareAdapter"]
