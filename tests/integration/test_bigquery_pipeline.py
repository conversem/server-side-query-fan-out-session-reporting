"""Integration tests for ETLPipeline (BigQuery) with mocked components.

Tests the orchestrator's run() method end-to-end using mocked
LLMBotExtractor and LLMBotTransformer to avoid real GCP calls.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.pipeline.orchestrator import ETLPipeline
from llm_bot_pipeline.pipeline.transform import TransformResult
from llm_bot_pipeline.storage import StorageError


@pytest.fixture
def mock_extractor():
    ext = MagicMock()
    ext.table_exists.return_value = True
    ext.get_table_row_count.return_value = 500
    ext.get_date_range_stats.return_value = {"row_count": 500}
    return ext


@pytest.fixture
def mock_transformer():
    tfm = MagicMock()
    tfm.get_transformation_stats.return_value = {
        "success_responses": 450,
    }
    tfm.delete_date_range.return_value = 100
    tfm.transform_and_load.return_value = TransformResult(
        success=True,
        rows_transformed=480,
        rows_deduplicated=20,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 7),
    )
    return tfm


@pytest.fixture
def pipeline(mock_extractor, mock_transformer):
    """ETLPipeline with mocked extractor and transformer."""
    with (
        patch(
            "llm_bot_pipeline.pipeline.orchestrator.LLMBotExtractor",
            return_value=mock_extractor,
        ),
        patch(
            "llm_bot_pipeline.pipeline.orchestrator.LLMBotTransformer",
            return_value=mock_transformer,
        ),
    ):
        p = ETLPipeline(project_id="test-project")
    p.extractor = mock_extractor
    p.transformer = mock_transformer
    return p


class TestETLPipelineRun:
    def test_successful_incremental_run(
        self, pipeline, mock_extractor, mock_transformer
    ):
        result = pipeline.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            mode="incremental",
        )
        assert result.success is True
        assert result.raw_rows == 500
        assert result.transformed_rows == 480
        assert result.duplicates_removed == 20
        mock_transformer.transform_and_load.assert_called_once()

    def test_invalid_date_range(self, pipeline):
        result = pipeline.run(
            start_date=date(2025, 1, 10),
            end_date=date(2025, 1, 1),
        )
        assert result.success is False
        assert len(result.errors) > 0

    def test_invalid_mode(self, pipeline):
        result = pipeline.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            mode="invalid",
        )
        assert result.success is False
        assert "Invalid mode" in result.errors[0]

    def test_source_count_failure_aborts(self, pipeline, mock_extractor):
        mock_extractor.get_date_range_stats.side_effect = StorageError(
            "permission denied"
        )
        result = pipeline.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        assert result.success is False

    def test_zero_rows_returns_early_success(self, pipeline, mock_extractor):
        mock_extractor.get_date_range_stats.return_value = {"row_count": 0}
        result = pipeline.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        assert result.success is True
        assert result.raw_rows == 0

    def test_transform_failure(self, pipeline, mock_transformer):
        mock_transformer.transform_and_load.return_value = TransformResult(
            success=False,
            error="Query quota exceeded",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        result = pipeline.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        assert result.success is False

    def test_dry_run_skips_transform(self, pipeline, mock_extractor, mock_transformer):
        result = pipeline.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            dry_run=True,
        )
        assert result.success is True
        mock_transformer.transform_and_load.assert_not_called()

    def test_result_has_timestamps(self, pipeline):
        result = pipeline.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        assert result.started_at is not None
        assert result.completed_at is not None

    def test_full_mode_deletes_then_appends(self, pipeline, mock_transformer):
        result = pipeline.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
            mode="full",
        )
        assert result.success is True
        mock_transformer.delete_date_range.assert_called_once()
        mock_transformer.transform_and_load.assert_called_once()
        call_kwargs = mock_transformer.transform_and_load.call_args[1]
        assert call_kwargs.get("write_disposition") == "WRITE_APPEND"

    def test_source_table_missing_aborts(self, pipeline, mock_extractor):
        mock_extractor.table_exists.return_value = False
        result = pipeline.run(
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 7),
        )
        assert result.success is False
        assert "Source table does not exist" in result.errors[0]
