"""
Shared fixtures for integration tests.

Provides integration-specific fixtures that build on the shared fixtures
defined in tests/conftest.py (sqlite_backend, sample_records, temp_db_path, etc.).
"""

from pathlib import Path

import pytest

from llm_bot_pipeline.pipeline import LocalPipeline
from llm_bot_pipeline.reporting import LocalDashboardQueries, LocalReportingAggregator

# =============================================================================
# DATABASE FIXTURES (integration-specific)
# =============================================================================


@pytest.fixture
def sqlite_backend_with_data(sqlite_backend, sample_records):
    """
    SQLite backend pre-populated with sample data.

    Returns tuple of (backend, records_inserted).
    """
    rows = sqlite_backend.insert_raw_records(sample_records)
    return sqlite_backend, rows


# =============================================================================
# PIPELINE FIXTURES
# =============================================================================


@pytest.fixture
def local_pipeline(temp_db_path: Path):
    """
    Create a LocalPipeline with temporary database.

    Automatically cleans up after test.
    """
    pipeline = LocalPipeline(backend_type="sqlite", db_path=temp_db_path)
    pipeline.initialize()
    yield pipeline
    pipeline.close()


@pytest.fixture
def pipeline_with_data(local_pipeline, sample_records):
    """
    LocalPipeline with raw data already ingested.

    Returns tuple of (pipeline, records_count).
    """
    local_pipeline._backend.insert_raw_records(sample_records)
    return local_pipeline, len(sample_records)


# =============================================================================
# REPORTING FIXTURES
# =============================================================================


@pytest.fixture
def local_aggregator(temp_db_path: Path):
    """
    Create a LocalReportingAggregator with temporary database.
    """
    aggregator = LocalReportingAggregator(backend_type="sqlite", db_path=temp_db_path)
    aggregator.initialize()
    yield aggregator
    aggregator.close()


@pytest.fixture
def local_dashboard(temp_db_path: Path):
    """
    Create a LocalDashboardQueries with temporary database.
    """
    dashboard = LocalDashboardQueries(backend_type="sqlite", db_path=temp_db_path)
    dashboard.initialize()
    yield dashboard
    dashboard.close()
