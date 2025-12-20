"""Reporting and analytics module."""

from .aggregations import AggregationResult, ReportingAggregator
from .dashboard_queries import DashboardQueries, QueryResult
from .local_aggregations import LocalReportingAggregator
from .local_dashboard_queries import LocalDashboardQueries
from .session_aggregations import (
    SessionAggregationResult,
    SessionAggregator,
    SessionRecord,
)

__all__ = [
    # Aggregations (SQLite)
    "ReportingAggregator",
    "AggregationResult",
    # Dashboard Queries (SQLite)
    "DashboardQueries",
    "QueryResult",
    # Local Aggregations
    "LocalReportingAggregator",
    # Local Dashboard Queries
    "LocalDashboardQueries",
    # Query Fan-Out Sessions
    "SessionAggregator",
    "SessionAggregationResult",
    "SessionRecord",
]
