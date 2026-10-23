"""
BigQuery dashboard queries package.

Assembles DashboardQueries from focused sub-modules via mixin classes.
"""

from .base import DashboardQueriesBase
from .geographic import GeographicMixin
from .health import HealthMixin
from .kpi import KpiMixin
from .timeseries import TimeSeriesMixin


class DashboardQueries(
    KpiMixin,
    GeographicMixin,
    TimeSeriesMixin,
    HealthMixin,
    DashboardQueriesBase,
):
    """
    Pre-built dashboard queries for LLM bot traffic analysis.

    All queries operate on aggregated reporting tables for performance.
    Date ranges default to sensible values if not specified.
    """


__all__ = ["DashboardQueries"]
