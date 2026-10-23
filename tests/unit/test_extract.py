"""Unit tests for pipeline/extract.py."""

from datetime import date
from unittest.mock import MagicMock, patch

from llm_bot_pipeline.config.constants import DATASET_RAW, TABLE_RAW_BOT_REQUESTS
from llm_bot_pipeline.pipeline.extract import ExtractionResult, LLMBotExtractor


def _make_extractor(
    project_id: str = "test-project",
    dataset_id: str = DATASET_RAW,
    table_id: str = TABLE_RAW_BOT_REQUESTS,
):
    """Create LLMBotExtractor with mocked BigQuery client."""
    with patch("llm_bot_pipeline.pipeline.extract.bigquery.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        extractor = LLMBotExtractor(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )
        extractor.client = mock_client
        return extractor, mock_client


class TestExtractBuildsDateQuery:
    """Tests for date range query building."""

    def test_extract_builds_date_query(self):
        """Given date range, assert SQL contains correct WHERE clause."""
        extractor, _ = _make_extractor()
        start = date(2025, 3, 1)
        end = date(2025, 3, 4)

        query = extractor._build_extraction_query(
            start_date=start,
            end_date=end,
        )

        assert f">= '{start.isoformat()}'" in query
        assert f"<= '{end.isoformat()}'" in query
        assert "DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)))" in query
        assert "WHERE" in query
        assert "EdgeStartTimestamp IS NOT NULL" in query


class TestExtractHandlesEmptyResult:
    """Tests for empty query result handling."""

    def test_extract_handles_empty_result(self):
        """Mock empty query result, assert rows_extracted is 0."""
        extractor, mock_client = _make_extractor()
        mock_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 0
        mock_job.result.return_value = mock_result
        mock_client.query.return_value = mock_job

        result = extractor.extract_raw_data(
            start_date=date(2025, 3, 1),
            end_date=date(2025, 3, 4),
        )

        assert isinstance(result, ExtractionResult)
        assert result.success is True
        assert result.rows_extracted == 0


class TestExtractDifferentBackends:
    """Tests for query building with different backend configs."""

    def test_extract_different_backends(self):
        """Test query building for different backend configs."""
        configs = [
            ("proj-a", "dataset_a", "table_a"),
            ("proj-b", "bot_logs_raw", "raw_bot_requests"),
            ("my-gcp-project", "custom_dataset", "custom_table"),
        ]

        for project_id, dataset_id, table_id in configs:
            extractor, _ = _make_extractor(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
            )
            expected_table = f"{project_id}.{dataset_id}.{table_id}"

            query = extractor._build_extraction_query(
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 31),
            )

            assert f"FROM `{expected_table}`" in query
