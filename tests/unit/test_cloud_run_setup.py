"""Unit tests for Cloud Run setup module."""

import sys
from unittest.mock import MagicMock, patch

from llm_bot_pipeline.infrastructure.cloud_run_setup import create_cloud_run_job


class TestCreateCloudRunJob:
    """Verify create_cloud_run_job creates Job with expected fields."""

    def test_create_cloud_run_job_returns_job_fields(self):
        """Mock Run client, assert Job object fields (name, uri)."""
        mock_result = MagicMock()
        mock_result.name = (
            "projects/test-proj/locations/us-central1/jobs/llm-bot-daily-etl"
        )
        mock_result.uri = (
            "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/"
            "namespaces/test-proj/jobs/llm-bot-daily-etl"
        )

        mock_operation = MagicMock()
        mock_operation.result.return_value = mock_result

        mock_client = MagicMock()
        mock_client.create_job.return_value = mock_operation

        mock_run_v2 = MagicMock()
        mock_run_v2.JobsClient.return_value = mock_client

        with patch.dict(sys.modules, {"google.cloud.run_v2": mock_run_v2}):
            result = create_cloud_run_job(
                project_id="test-proj",
                region="us-central1",
                service_account_email="sa@test-proj.iam.gserviceaccount.com",
            )

        assert result["name"] == mock_result.name
        assert result["uri"] == mock_result.uri
        mock_client.create_job.assert_called_once()
        call_kwargs = mock_client.create_job.call_args
        assert call_kwargs.kwargs["parent"] == (
            "projects/test-proj/locations/us-central1"
        )
        assert call_kwargs.kwargs["job_id"] == "llm-bot-daily-etl"

    def test_create_cloud_run_job_uses_custom_image(self):
        """Verify custom image is passed to Job template."""
        mock_result = MagicMock()
        mock_result.name = "projects/p/locations/r/jobs/j"
        mock_result.uri = "https://example.com/j"

        mock_operation = MagicMock()
        mock_operation.result.return_value = mock_result

        mock_client = MagicMock()
        mock_client.create_job.return_value = mock_operation

        mock_run_v2 = MagicMock()
        mock_run_v2.JobsClient.return_value = mock_client

        with patch.dict(sys.modules, {"google.cloud.run_v2": mock_run_v2}):
            create_cloud_run_job(
                project_id="p",
                region="r",
                service_account_email="sa@p.iam.gserviceaccount.com",
                image="gcr.io/p/custom-image:v1",
            )

        # Container is built with image=...; verify Container was called with it
        mock_run_v2.Container.assert_called()
        container_calls = mock_run_v2.Container.call_args_list
        assert len(container_calls) >= 1
        assert container_calls[0].kwargs["image"] == "gcr.io/p/custom-image:v1"
