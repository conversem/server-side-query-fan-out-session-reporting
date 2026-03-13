"""
Local dashboard queries package.

Assembles LocalDashboardQueries from focused sub-modules via mixin classes.
"""

from .base import LocalDashboardQueriesBase
from .kpi import KpiMixin
from .refinement import RefinementMixin
from .session import SessionMixin
from .timeseries import TimeSeriesMixin


class LocalDashboardQueries(
    KpiMixin,
    TimeSeriesMixin,
    SessionMixin,
    RefinementMixin,
    LocalDashboardQueriesBase,
):
    """
    Local dashboard queries using storage abstraction.

    Works with both SQLite (POC) and BigQuery backends.
    All queries operate on local reporting tables.
    """


__all__ = ["LocalDashboardQueries"]
