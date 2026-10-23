"""Unit tests for BigQuery retry integration in insert_clean_records."""

from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import BadRequest, ServiceUnavailable

from llm_bot_pipeline.storage.bigquery_backend import BigQueryBackend


def _make_backend():
    """Create a BigQueryBackend with a mocked BQ client."""
    backend = BigQueryBackend(project_id="test-project")
    mock_client = MagicMock()
    backend._client = mock_client
    return backend, mock_client


class TestBigQueryRetry:
    @patch("llm_bot_pipeline.monitoring.retry_handler.time.sleep")
    def test_insert_retries_on_transient_error(self, mock_sleep):
        """ServiceUnavailable twice then success on third attempt."""
        backend, mock_client = _make_backend()
        mock_client.insert_rows_json.side_effect = [
            ServiceUnavailable("Service unavailable"),
            ServiceUnavailable("Service unavailable"),
            [],
        ]

        result = backend.insert_clean_records([{"col": "val"}])

        assert result == 1
        assert mock_client.insert_rows_json.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("llm_bot_pipeline.monitoring.retry_handler.time.sleep")
    def test_insert_no_retry_on_schema_error(self, mock_sleep):
        """BadRequest propagates immediately without retry."""
        backend, mock_client = _make_backend()
        mock_client.insert_rows_json.side_effect = BadRequest("Schema mismatch")

        with pytest.raises(BadRequest, match="Schema mismatch"):
            backend.insert_clean_records([{"col": "val"}])

        assert mock_client.insert_rows_json.call_count == 1
        mock_sleep.assert_not_called()

    @patch("llm_bot_pipeline.monitoring.retry_handler.time.sleep")
    def test_retry_exhaustion_raises(self, mock_sleep):
        """After max retries (3), exception propagates."""
        backend, mock_client = _make_backend()
        mock_client.insert_rows_json.side_effect = ServiceUnavailable(
            "Service unavailable"
        )

        with pytest.raises(ServiceUnavailable):
            backend.insert_clean_records([{"col": "val"}])

        # 1 initial + 3 retries = 4 total calls
        assert mock_client.insert_rows_json.call_count == 4
        assert mock_sleep.call_count == 3
