"""
AWS CloudFront adapter for W3C extended log format.

Supports ingestion from AWS CloudFront access logs exported to S3
in W3C extended log format (tab-separated values).
"""

from .adapter import CloudFrontAdapter

__all__ = ["CloudFrontAdapter"]
