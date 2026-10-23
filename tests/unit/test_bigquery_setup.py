"""Unit tests for BigQuery setup module."""

from unittest.mock import MagicMock, patch

from google.cloud import bigquery

from llm_bot_pipeline.infrastructure.bigquery_setup import BigQuerySetup


class TestBigQuerySetupCreateDataset:
    """Verify BigQuerySetup.create_dataset creates dataset with correct params."""

    def test_create_dataset_creates_dataset(self):
        """Mock BQ client, assert dataset created with exists_ok=True."""
        mock_client = MagicMock()
        mock_dataset = MagicMock(spec=bigquery.Dataset)
        mock_dataset.dataset_id = "bot_logs_raw"
        mock_client.create_dataset.return_value = mock_dataset

        with patch(
            "llm_bot_pipeline.infrastructure.bigquery_setup.bigquery.Client",
            return_value=mock_client,
        ):
            setup = BigQuerySetup(project_id="test-proj", credentials_path=None)

            result = setup.create_dataset(
                dataset_id="bot_logs_raw",
                description="POC: Raw LLM bot traffic",
            )

        assert result == mock_dataset
        mock_client.create_dataset.assert_called_once()
        call_args = mock_client.create_dataset.call_args
        dataset_ref = call_args[0][0]
        assert dataset_ref.dataset_id == "bot_logs_raw"
        assert dataset_ref.project == "test-proj"
        assert dataset_ref.description == "POC: Raw LLM bot traffic"
        assert call_args[1]["exists_ok"] is True

    def test_create_dataset_uses_location(self):
        """Verify dataset is created with configured location."""
        mock_client = MagicMock()
        mock_dataset = MagicMock(spec=bigquery.Dataset)
        mock_client.create_dataset.return_value = mock_dataset

        with patch(
            "llm_bot_pipeline.infrastructure.bigquery_setup.bigquery.Client",
            return_value=mock_client,
        ):
            setup = BigQuerySetup(
                project_id="p",
                credentials_path=None,
                location="EU",
            )

            setup.create_dataset(
                dataset_id="ds",
                description="desc",
            )

        call_args = mock_client.create_dataset.call_args
        dataset_ref = call_args[0][0]
        assert dataset_ref.location == "EU"
