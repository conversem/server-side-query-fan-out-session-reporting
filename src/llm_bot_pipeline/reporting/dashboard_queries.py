"""
BigQuery dashboard queries — backward-compatibility shim.

All logic lives in the ``bq_queries`` sub-package.
"""

from google.cloud import bigquery  # noqa: F401 — kept for patch compatibility

from .bq_queries import DashboardQueries

__all__ = ["DashboardQueries"]
