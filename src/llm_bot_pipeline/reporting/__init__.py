"""Reporting and analytics module."""

from .excel_exporter import ExcelExporter
from .freshness_tracker import DataFreshnessTracker, FreshnessRecord
from .local_aggregations import LocalReportingAggregator
from .local_dashboard_queries import LocalDashboardQueries
from .models import AggregationResult, QueryResult
from .reporting_utils import DashboardMetrics, ReportingUtilities, ValidationResult
from .session_aggregations import (
    RefinementMetrics,
    SessionAggregationResult,
    SessionAggregator,
    SessionRecord,
)
from .session_refiner import QualityScore, RefinedSession, RefinerResult, SessionRefiner
from .session_storage_writer import SessionStorageWriter
from .sitemap_aggregations import SitemapAggregationResult, SitemapAggregator
from .temporal_bundler import BundleResult, TemporalBundler

__all__ = [
    # Excel Export
    "ExcelExporter",
    # Shared Models (always available)
    "AggregationResult",
    "QueryResult",
    "FreshnessRecord",
    # Data Freshness Tracking
    "DataFreshnessTracker",
    # Local Aggregations (SQLite/BigQuery via storage abstraction)
    "LocalReportingAggregator",
    # Local Dashboard Queries (SQLite/POC)
    "LocalDashboardQueries",
    # Query Fan-Out Sessions
    "SessionAggregator",
    "SessionAggregationResult",
    "SessionRecord",
    "RefinementMetrics",
    # Session Storage Writer
    "SessionStorageWriter",
    # Session Refiner
    "SessionRefiner",
    "RefinerResult",
    "RefinedSession",
    "QualityScore",
    # Temporal Bundler
    "TemporalBundler",
    "BundleResult",
    # Sitemap Freshness
    "SitemapAggregator",
    "SitemapAggregationResult",
    # Reporting Utilities
    "ReportingUtilities",
    "ValidationResult",
    "DashboardMetrics",
]

# BigQuery-dependent (optional)
try:
    from .aggregations import ReportingAggregator
    from .dashboard_queries import DashboardQueries

    __all__.extend(["ReportingAggregator", "DashboardQueries"])
except ImportError:
    pass
