"""Unit tests for pipeline/transform.py."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.config.settings import UrlFilteringSettings
from llm_bot_pipeline.pipeline.transform import LLMBotTransformer, TransformResult


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client for all DB operations."""
    with patch(
        "llm_bot_pipeline.pipeline.transform.bigquery.Client",
        return_value=MagicMock(),
    ) as mock_cls:
        yield mock_cls.return_value


@pytest.fixture
def transformer(mock_bq_client):
    """LLMBotTransformer with mocked BigQuery client."""
    t = LLMBotTransformer(project_id="test-project")
    t.client = mock_bq_client
    return t


class TestTransformBuildsCorrectSql:
    """Given transform config, assert SQL has all required clauses."""

    def test_transform_builds_correct_sql(self, transformer):
        start_date = date(2025, 3, 1)
        end_date = date(2025, 3, 3)

        query = transformer._build_transform_query(start_date, end_date)

        # Deduplication
        assert "WITH deduplicated AS" in query
        assert "ROW_NUMBER() OVER" in query
        assert "PARTITION BY" in query
        assert "EdgeStartTimestamp" in query
        assert "ClientIP" in query
        assert "ClientRequestURI" in query
        assert "ClientRequestHost" in query
        assert "row_num = 1" in query

        # Date filtering
        assert "DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)))" in query
        assert "2025-03-01" in query
        assert "2025-03-03" in query

        # Temporal fields
        assert "request_timestamp" in query
        assert "request_date" in query
        assert "request_hour" in query
        assert "day_of_week" in query

        # URL path extraction
        assert "url_path" in query
        assert "REGEXP_EXTRACT" in query
        assert "url_path_depth" in query

        # Bot classification
        assert "bot_name" in query
        assert "bot_provider" in query
        assert "bot_category" in query

        # Response status categorization
        assert "response_status_category" in query
        assert "2xx_success" in query
        assert "4xx_client_error" in query
        assert "5xx_server_error" in query

        # Filter known bots only
        assert "bot_name != 'Unknown'" in query

        # Source/target table IDs
        assert "test-project" in query


class TestTransformHandlesNoData:
    """Test with no matching records, assert zero rows."""

    def test_transform_handles_no_data(self, transformer, mock_bq_client):
        start_date = date(2025, 3, 1)
        end_date = date(2025, 3, 3)

        # Mock transform query: job returns 0 rows
        mock_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 0
        mock_job.result.return_value = mock_result

        # Mock _get_dedup_stats query
        mock_stats_row = MagicMock()
        mock_stats_row.raw_rows = 0
        mock_stats_row.unique_rows = 0
        mock_stats_row.duplicates_removed = 0
        mock_stats_job = MagicMock()
        mock_stats_job.result.return_value = iter([mock_stats_row])

        mock_bq_client.query.side_effect = [mock_job, mock_stats_job]

        result = transformer.transform_and_load(start_date, end_date)

        assert result.success is True
        assert result.rows_transformed == 0
        assert result.rows_deduplicated == 0
        assert result.start_date == start_date
        assert result.end_date == end_date


class TestTransformDataFlow:
    """Test data transformation with sample input/output."""

    def test_transform_data_flow(self, transformer, mock_bq_client):
        start_date = date(2025, 3, 1)
        end_date = date(2025, 3, 3)

        # Mock transform query: job returns 2 rows
        mock_job = MagicMock()
        mock_result = MagicMock()
        mock_result.total_rows = 2
        mock_job.result.return_value = mock_result

        # Mock preview-style rows (transformed output structure)
        mock_row1 = {
            "request_timestamp": "2025-03-01T10:00:00",
            "request_date": "2025-03-01",
            "request_hour": 10,
            "url_path": "/docs/getting-started",
            "bot_name": "GPTBot",
            "bot_provider": "OpenAI",
            "bot_category": "training",
            "response_status_category": "2xx_success",
        }
        mock_row2 = {
            "request_timestamp": "2025-03-01T11:00:00",
            "request_date": "2025-03-01",
            "request_hour": 11,
            "url_path": "/api/reference",
            "bot_name": "ClaudeBot",
            "bot_provider": "Anthropic",
            "bot_category": "training",
            "response_status_category": "2xx_success",
        }

        # Mock _get_dedup_stats
        mock_stats_row = MagicMock()
        mock_stats_row.raw_rows = 5
        mock_stats_row.unique_rows = 2
        mock_stats_row.duplicates_removed = 3
        mock_stats_job = MagicMock()
        mock_stats_job.result.return_value = iter([mock_stats_row])

        mock_bq_client.query.side_effect = [mock_job, mock_stats_job]

        result = transformer.transform_and_load(start_date, end_date)

        assert result.success is True
        assert result.rows_transformed == 2
        assert result.rows_deduplicated == 3
        assert result.start_date == start_date
        assert result.end_date == end_date

    def test_preview_transform_returns_transformed_rows(
        self, transformer, mock_bq_client
    ):
        """preview_transform returns list of dicts with expected keys."""
        start_date = date(2025, 3, 1)
        end_date = date(2025, 3, 3)

        sample_row = {
            "request_timestamp": "2025-03-01T10:00:00",
            "request_date": "2025-03-01",
            "bot_name": "GPTBot",
            "url_path": "/docs",
        }
        mock_job = MagicMock()
        mock_job.result.return_value = [sample_row]
        mock_bq_client.query.return_value = mock_job

        rows = transformer.preview_transform(start_date, end_date, limit=5)

        assert len(rows) == 1
        assert rows[0]["request_timestamp"] == "2025-03-01T10:00:00"
        assert rows[0]["bot_name"] == "GPTBot"
        assert rows[0]["url_path"] == "/docs"


class TestTransformResultValidation:
    """TransformResult and edge cases."""

    def test_invalid_date_range_returns_failure(self, transformer):
        """start_date > end_date returns TransformResult with success=False."""
        result = transformer.transform_and_load(
            start_date=date(2025, 3, 5),
            end_date=date(2025, 3, 1),
        )

        assert result.success is False
        assert "start_date" in (result.error or "")
        assert "end_date" in (result.error or "")


def _make_transformer(url_settings: UrlFilteringSettings) -> LLMBotTransformer:
    """Create LLMBotTransformer bypassing __init__ (no GCP credentials)."""
    t = LLMBotTransformer.__new__(LLMBotTransformer)
    t.project_id = "test-project"
    t.source_table_id = "test-project.raw.raw_bot_requests"
    t.target_table_id = "test-project.report.bot_requests_daily"
    t._url_settings = url_settings
    return t


class TestUrlFilteringSQL:
    """URL filtering integration into the BigQuery transform query."""

    def test_transform_query_contains_resource_type(self):
        transformer = _make_transformer(UrlFilteringSettings())
        query = transformer._build_transform_query(date(2025, 1, 1), date(2025, 1, 1))
        assert "resource_type" in query

    def test_transform_query_filters_js_css(self):
        transformer = _make_transformer(UrlFilteringSettings())
        query = transformer._build_transform_query(date(2025, 1, 1), date(2025, 1, 1))
        assert "NOT REGEXP_CONTAINS" in query or "NOT STARTS_WITH" in query

    def test_transform_query_disabled_no_filter(self):
        transformer = _make_transformer(UrlFilteringSettings(enabled=False))
        query = transformer._build_transform_query(date(2025, 1, 1), date(2025, 1, 1))
        assert "'document' as resource_type" in query
