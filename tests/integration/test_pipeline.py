"""
Integration tests for the local ETL pipeline.

Tests:
- Pipeline initialization
- Data transformation (raw → clean)
- Duplicate handling
- Pipeline status reporting
- Full and incremental modes
"""

from datetime import date, timedelta

import pytest


class TestPipelineInitialization:
    """Tests for pipeline initialization."""

    def test_pipeline_initializes(self, local_pipeline):
        """Pipeline should initialize without errors."""
        assert local_pipeline._initialized

    def test_pipeline_creates_backend(self, local_pipeline):
        """Pipeline should create storage backend."""
        assert local_pipeline._backend is not None
        assert local_pipeline._backend.backend_type == "sqlite"


class TestPipelineTransformation:
    """Tests for data transformation."""

    def test_run_transforms_raw_to_clean(self, pipeline_with_data, date_range):
        """Pipeline run should transform raw records to clean table."""
        pipeline, records_count = pipeline_with_data
        start_date, end_date = date_range

        # Extend date range to capture all sample data
        wide_start = start_date - timedelta(days=30)
        wide_end = end_date + timedelta(days=30)

        result = pipeline.run(wide_start, wide_end, mode="full")

        assert result.success
        assert result.raw_rows == records_count
        assert result.transformed_rows > 0

    def test_transformation_adds_derived_fields(self, pipeline_with_data, date_range):
        """Transformation should add bot classification fields."""
        pipeline, _ = pipeline_with_data
        start_date, end_date = date_range

        wide_start = start_date - timedelta(days=30)
        wide_end = end_date + timedelta(days=30)

        pipeline.run(wide_start, wide_end, mode="full")

        # Check clean table has derived fields
        result = pipeline._backend.query(
            "SELECT bot_name, bot_provider, bot_category FROM bot_requests_daily LIMIT 1"
        )

        assert result
        assert result[0]["bot_name"] is not None
        assert result[0]["bot_provider"] is not None
        assert result[0]["bot_category"] is not None

    def test_transformation_calculates_response_category(
        self, pipeline_with_data, date_range
    ):
        """Transformation should calculate response_status_category."""
        pipeline, _ = pipeline_with_data
        start_date, end_date = date_range

        wide_start = start_date - timedelta(days=30)
        wide_end = end_date + timedelta(days=30)

        pipeline.run(wide_start, wide_end, mode="full")

        result = pipeline._backend.query(
            "SELECT response_status_category FROM bot_requests_daily LIMIT 1"
        )

        assert result
        # Should be one of the valid categories
        valid_categories = [
            "2xx_success",
            "3xx_redirect",
            "4xx_client_error",
            "5xx_server_error",
        ]
        assert result[0]["response_status_category"] in valid_categories


class TestPipelineModes:
    """Tests for full vs incremental pipeline modes."""

    def test_full_mode_replaces_existing(self, pipeline_with_data, date_range):
        """Full mode should delete existing data before insert."""
        pipeline, _ = pipeline_with_data
        start_date, end_date = date_range

        wide_start = start_date - timedelta(days=30)
        wide_end = end_date + timedelta(days=30)

        # First run
        result1 = pipeline.run(wide_start, wide_end, mode="full")
        count_after_first = pipeline._backend.get_table_row_count("bot_requests_daily")

        # Second run in full mode should have same count
        result2 = pipeline.run(wide_start, wide_end, mode="full")
        count_after_second = pipeline._backend.get_table_row_count("bot_requests_daily")

        assert count_after_first == count_after_second

    def test_incremental_mode_appends(self, pipeline_with_data, date_range):
        """Incremental mode should append to existing data."""
        pipeline, _ = pipeline_with_data
        start_date, end_date = date_range

        wide_start = start_date - timedelta(days=30)
        wide_end = end_date + timedelta(days=30)

        # First run in full mode
        pipeline.run(wide_start, wide_end, mode="full")
        count_after_first = pipeline._backend.get_table_row_count("bot_requests_daily")

        # Second run in incremental mode
        pipeline.run(wide_start, wide_end, mode="incremental")
        count_after_second = pipeline._backend.get_table_row_count("bot_requests_daily")

        # Incremental should add more rows (though duplicates may be filtered)
        assert count_after_second >= count_after_first


class TestPipelineStatus:
    """Tests for pipeline status reporting."""

    def test_get_pipeline_status(self, pipeline_with_data, date_range):
        """Should return pipeline status dict."""
        pipeline, _ = pipeline_with_data
        start_date, end_date = date_range

        wide_start = start_date - timedelta(days=30)
        wide_end = end_date + timedelta(days=30)

        pipeline.run(wide_start, wide_end, mode="full")

        status = pipeline.get_pipeline_status()

        # Status returns row counts directly
        assert "raw_row_count" in status
        assert "clean_row_count" in status
        assert status["raw_row_count"] > 0
        assert status["clean_row_count"] > 0


class TestPipelineResultDataclass:
    """Tests for LocalPipelineResult dataclass."""

    def test_result_has_expected_fields(self, pipeline_with_data, date_range):
        """Pipeline result should have all expected fields."""
        pipeline, _ = pipeline_with_data
        start_date, end_date = date_range

        wide_start = start_date - timedelta(days=30)
        wide_end = end_date + timedelta(days=30)

        result = pipeline.run(wide_start, wide_end, mode="full")

        assert hasattr(result, "success")
        assert hasattr(result, "start_date")
        assert hasattr(result, "end_date")
        assert hasattr(result, "raw_rows")
        assert hasattr(result, "transformed_rows")
        assert hasattr(result, "duplicates_removed")
        # Result has started_at/completed_at for timing, and errors list
        assert hasattr(result, "started_at")
        assert hasattr(result, "errors")


class TestPipelineErrorHandling:
    """Tests for pipeline error handling."""

    def test_empty_date_range_succeeds_with_zero_rows(self, local_pipeline):
        """Pipeline should handle empty data gracefully with success=True."""
        # Use valid dates but no data exists
        end_date = date.today() - timedelta(days=10)
        start_date = end_date - timedelta(days=5)

        result = local_pipeline.run(start_date, end_date, mode="full")

        # Empty data should succeed but with 0 rows
        assert result.success
        assert result.raw_rows == 0
        assert result.transformed_rows == 0
