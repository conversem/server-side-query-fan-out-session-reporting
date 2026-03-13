"""Unit tests for reporting models validation."""

import pytest

from llm_bot_pipeline.reporting.models import AggregationResult, QueryResult


class TestModelsValidation:
    """Create model with invalid data, assert validation error."""

    def test_aggregation_result_empty_table_name_raises(self):
        """AggregationResult with empty table_name raises ValueError."""
        with pytest.raises(ValueError, match="table_name must be non-empty"):
            AggregationResult(success=True, table_name="")

    def test_aggregation_result_whitespace_table_name_raises(self):
        """AggregationResult with whitespace-only table_name raises ValueError."""
        with pytest.raises(ValueError, match="table_name must be non-empty"):
            AggregationResult(success=True, table_name="   ")

    def test_aggregation_result_negative_rows_inserted_raises(self):
        """AggregationResult with negative rows_inserted raises ValueError."""
        with pytest.raises(ValueError, match="rows_inserted must be >= 0"):
            AggregationResult(
                success=True,
                table_name="daily_summary",
                rows_inserted=-1,
            )

    def test_aggregation_result_negative_duration_raises(self):
        """AggregationResult with negative duration_seconds raises ValueError."""
        with pytest.raises(ValueError, match="duration_seconds must be >= 0"):
            AggregationResult(
                success=True,
                table_name="daily_summary",
                duration_seconds=-1.0,
            )

    def test_query_result_negative_row_count_raises(self):
        """QueryResult with negative row_count raises ValueError."""
        with pytest.raises(ValueError, match="row_count must be >= 0"):
            QueryResult(
                query_name="test",
                rows=[],
                row_count=-1,
            )

    def test_query_result_row_count_mismatch_raises(self):
        """QueryResult with row_count != len(rows) raises ValueError."""
        with pytest.raises(ValueError, match="row_count.*must match len\\(rows\\)"):
            QueryResult(
                query_name="test",
                rows=[{"a": 1}, {"b": 2}],
                row_count=5,
            )
