"""
End-to-end integration tests for the complete pipeline.

Tests the full flow:
1. Sample data generation
2. Raw data ingestion
3. ETL transformation
4. Aggregation creation
5. Dashboard query execution

This validates that all components work together correctly.
"""

from datetime import date, timedelta
from pathlib import Path

import pytest


class TestFullPipelineFlow:
    """
    End-to-end tests validating the complete pipeline.

    These tests simulate the real workflow from data ingestion
    through to dashboard queries.
    """

    @pytest.fixture
    def e2e_db_path(self, tmp_path: Path) -> Path:
        """Create a dedicated database path for E2E tests."""
        return tmp_path / "e2e_test.db"

    def test_complete_pipeline_flow(self, e2e_db_path):
        """
        Test complete flow: ingest → transform → aggregate → query.

        This is the primary E2E test validating all components.
        """
        from llm_bot_pipeline.pipeline import LocalPipeline
        from llm_bot_pipeline.reporting import (
            LocalDashboardQueries,
            LocalReportingAggregator,
        )
        from llm_bot_pipeline.storage import get_backend
        from tests.integration.conftest import generate_sample_records

        # Setup dates
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=2)

        # Step 1: Generate sample data
        sample_records = generate_sample_records(
            num_records=200,
            start_date=start_date,
            end_date=end_date,
        )
        assert len(sample_records) == 200

        # Step 2: Ingest raw data
        backend = get_backend("sqlite", db_path=e2e_db_path)
        backend.initialize()
        rows_ingested = backend.insert_raw_records(sample_records)
        assert rows_ingested == 200
        backend.close()

        # Step 3: Run ETL pipeline
        pipeline = LocalPipeline(backend_type="sqlite", db_path=e2e_db_path)
        pipeline.initialize()

        result = pipeline.run(start_date, end_date, mode="full")
        assert result.success
        assert result.raw_rows == 200
        assert result.transformed_rows > 0
        pipeline.close()

        # Step 4: Run aggregations
        aggregator = LocalReportingAggregator(
            backend_type="sqlite", db_path=e2e_db_path
        )
        aggregator.initialize()

        agg_results = aggregator.aggregate_all(start_date, end_date)
        assert all(r.success for r in agg_results)
        assert agg_results[0].rows_inserted > 0  # daily_summary
        assert agg_results[1].rows_inserted > 0  # url_performance
        aggregator.close()

        # Step 5: Run dashboard queries
        dashboard = LocalDashboardQueries(backend_type="sqlite", db_path=e2e_db_path)
        dashboard.initialize()

        summary = dashboard.get_executive_summary(days=7)
        assert "metrics" in summary
        assert summary["metrics"]["total_requests"] is not None

        provider_diversity = dashboard.get_bot_provider_diversity(start_date, end_date)
        assert provider_diversity.row_count > 0

        dashboard.close()

    def test_data_consistency_through_pipeline(self, e2e_db_path):
        """
        Verify data consistency at each pipeline stage.

        Ensures no data loss or corruption through transformations.
        """
        from llm_bot_pipeline.pipeline import LocalPipeline
        from llm_bot_pipeline.reporting import LocalReportingAggregator
        from llm_bot_pipeline.storage import get_backend
        from tests.integration.conftest import generate_sample_records

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=2)

        # Generate known quantity of records
        sample_records = generate_sample_records(
            num_records=100,
            start_date=start_date,
            end_date=end_date,
        )

        # Ingest
        backend = get_backend("sqlite", db_path=e2e_db_path)
        backend.initialize()
        backend.insert_raw_records(sample_records)

        raw_count = backend.get_table_row_count("raw_bot_requests")
        assert raw_count == 100
        backend.close()

        # Transform
        pipeline = LocalPipeline(backend_type="sqlite", db_path=e2e_db_path)
        pipeline.initialize()
        result = pipeline.run(start_date, end_date, mode="full")

        clean_count = pipeline._backend.get_table_row_count("bot_requests_daily")
        assert clean_count == result.transformed_rows
        assert clean_count <= raw_count  # May have duplicates removed
        pipeline.close()

        # Aggregate
        aggregator = LocalReportingAggregator(
            backend_type="sqlite", db_path=e2e_db_path
        )
        aggregator.initialize()
        aggregator.aggregate_all(start_date, end_date)

        stats = aggregator.get_freshness_stats()
        assert stats["daily_summary"]["total_rows"] > 0
        assert stats["url_performance"]["total_rows"] > 0
        aggregator.close()


class TestMultiDayProcessing:
    """Tests for processing data across multiple days."""

    def test_process_week_of_data(self, tmp_path):
        """Should correctly process a full week of data."""
        from llm_bot_pipeline.pipeline import LocalPipeline
        from llm_bot_pipeline.storage import get_backend
        from tests.integration.conftest import generate_sample_records

        db_path = tmp_path / "week_test.db"

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)  # 7 days

        # Generate week of data
        sample_records = generate_sample_records(
            num_records=350,  # ~50 per day
            start_date=start_date,
            end_date=end_date,
        )

        # Ingest
        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()
        backend.insert_raw_records(sample_records)
        backend.close()

        # Transform
        pipeline = LocalPipeline(backend_type="sqlite", db_path=db_path)
        pipeline.initialize()
        result = pipeline.run(start_date, end_date, mode="full")

        assert result.success
        assert result.transformed_rows > 0

        # Verify multiple days in clean table (350 records over 7 days)
        days_result = pipeline._backend.query(
            "SELECT COUNT(DISTINCT request_date) as days FROM bot_requests_daily"
        )
        # With 350 random records over 7 days, we should have at least 3+ days covered
        assert days_result[0]["days"] >= 3

        pipeline.close()


class TestBotClassificationIntegration:
    """Tests verifying bot classification through the pipeline."""

    def test_all_bots_classified(self, tmp_path):
        """All ingested records should be classified in transformation."""
        from llm_bot_pipeline.pipeline import LocalPipeline
        from llm_bot_pipeline.storage import get_backend
        from tests.integration.conftest import generate_sample_records

        db_path = tmp_path / "classification_test.db"

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=2)

        sample_records = generate_sample_records(
            num_records=100,
            start_date=start_date,
            end_date=end_date,
        )

        # Ingest
        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()
        backend.insert_raw_records(sample_records)
        backend.close()

        # Transform
        pipeline = LocalPipeline(backend_type="sqlite", db_path=db_path)
        pipeline.initialize()
        pipeline.run(start_date, end_date, mode="full")

        # Check all records have bot classification
        result = pipeline._backend.query(
            """
            SELECT COUNT(*) as cnt 
            FROM bot_requests_daily 
            WHERE bot_name IS NULL OR bot_provider IS NULL
            """
        )

        # All records should be classified (our sample data uses known bots)
        assert result[0]["cnt"] == 0

        pipeline.close()


class TestErrorRecovery:
    """Tests for error handling and recovery."""

    def test_pipeline_handles_empty_data(self, tmp_path):
        """Pipeline should handle empty data gracefully."""
        from llm_bot_pipeline.pipeline import LocalPipeline

        db_path = tmp_path / "empty_test.db"

        pipeline = LocalPipeline(backend_type="sqlite", db_path=db_path)
        pipeline.initialize()

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=2)

        result = pipeline.run(start_date, end_date, mode="full")

        # Should succeed but with 0 rows
        assert result.success
        assert result.raw_rows == 0
        assert result.transformed_rows == 0

        pipeline.close()

    def test_aggregation_handles_no_clean_data(self, tmp_path):
        """Aggregations should handle missing clean data."""
        from llm_bot_pipeline.reporting import LocalReportingAggregator

        db_path = tmp_path / "no_clean_test.db"

        aggregator = LocalReportingAggregator(backend_type="sqlite", db_path=db_path)
        aggregator.initialize()

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=2)

        result = aggregator.aggregate_daily_summary(start_date, end_date)

        assert result.success
        assert result.rows_inserted == 0

        aggregator.close()


class TestPerformanceBaseline:
    """Basic performance tests to establish baselines."""

    def test_large_batch_ingestion(self, tmp_path):
        """Should handle large batch ingestion efficiently."""
        import time

        from llm_bot_pipeline.storage import get_backend
        from tests.integration.conftest import generate_sample_records

        db_path = tmp_path / "perf_test.db"

        # Generate 1000 records
        sample_records = generate_sample_records(num_records=1000)

        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()

        start_time = time.time()
        rows = backend.insert_raw_records(sample_records)
        duration = time.time() - start_time

        assert rows == 1000
        # Should complete in under 5 seconds
        assert duration < 5.0

        backend.close()

    def test_pipeline_performance(self, tmp_path):
        """Pipeline should process 1000 records efficiently."""
        import time

        from llm_bot_pipeline.pipeline import LocalPipeline
        from llm_bot_pipeline.storage import get_backend
        from tests.integration.conftest import generate_sample_records

        db_path = tmp_path / "pipeline_perf_test.db"

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)

        sample_records = generate_sample_records(
            num_records=1000,
            start_date=start_date,
            end_date=end_date,
        )

        # Ingest
        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()
        backend.insert_raw_records(sample_records)
        backend.close()

        # Time the pipeline
        pipeline = LocalPipeline(backend_type="sqlite", db_path=db_path)
        pipeline.initialize()

        start_time = time.time()
        result = pipeline.run(start_date, end_date, mode="full")
        duration = time.time() - start_time

        assert result.success
        # Should complete in under 10 seconds
        assert duration < 10.0

        pipeline.close()

