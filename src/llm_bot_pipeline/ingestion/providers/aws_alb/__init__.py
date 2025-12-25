"""
AWS Application Load Balancer (ALB) adapter for log ingestion.

Supports ingestion from AWS ALB access logs via:
- Space-separated log files
- Gzip-compressed log files (.log.gz)

The adapter handles:
- Space-separated parsing with quoted field support
- HTTP request line parsing (METHOD URL HTTP/VERSION)
- Client:port IP extraction
- ISO 8601 timestamp parsing
"""

from .adapter import ALBAdapter

__all__ = ["ALBAdapter"]
