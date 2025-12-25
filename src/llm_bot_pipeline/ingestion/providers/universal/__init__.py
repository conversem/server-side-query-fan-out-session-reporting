"""
Universal adapter for standard CSV/JSON formats.

Supports ingestion from any provider using standard CSV, JSON, or NDJSON
formats that match the universal schema.
"""

from .adapter import UniversalAdapter

__all__ = ["UniversalAdapter"]
