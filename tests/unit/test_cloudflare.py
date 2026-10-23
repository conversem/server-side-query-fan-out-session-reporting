"""Unit tests for Cloudflare modules (logpush, logpull, filters)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.cloudflare import filters, logpull, logpush_job
from llm_bot_pipeline.config.settings import Settings

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def cloudflare_settings():
    """Settings with Cloudflare config for tests."""
    return Settings(
        cloudflare_api_token="test-token",
        cloudflare_zone_id="test-zone-id",
        gcp_project_id="test-project",
        dataset_raw="test_dataset",
        logpush_job_name="test-logpush-job",
        logpush_enabled=True,
    )


# -----------------------------------------------------------------------------
# Logpush Job Tests
# -----------------------------------------------------------------------------


class TestLogpushCreateJob:
    def test_logpush_create_job(self, cloudflare_settings):
        """Mock Cloudflare API, create job, assert correct API call."""
        mock_client = MagicMock()
        mock_job = MagicMock()
        mock_job.id = 12345
        mock_client.logpush.jobs.create.return_value = mock_job

        with (
            patch.object(
                logpush_job,
                "get_cloudflare_client",
                return_value=mock_client,
            ),
            patch.object(
                logpush_job,
                "find_job_by_name",
                return_value=logpush_job.LogpushJobResult(
                    success=True, job_id=None, message="Job not found"
                ),
            ),
        ):
            result = logpush_job.create_logpush_job(cloudflare_settings)

        assert result.success is True
        assert result.job_id == 12345

        mock_client.logpush.jobs.create.assert_called_once()
        call_kwargs = mock_client.logpush.jobs.create.call_args.kwargs

        assert call_kwargs["zone_id"] == "test-zone-id"
        assert call_kwargs["name"] == "test-logpush-job"
        assert call_kwargs["dataset"] == "http_requests"
        assert call_kwargs["enabled"] is True
        assert (
            "bigquery://test-project/test_dataset/" in call_kwargs["destination_conf"]
        )
        assert "fields=" in call_kwargs["logpull_options"]
        assert "timestamps=rfc3339" in call_kwargs["logpull_options"]
        assert call_kwargs["filter"] == filters.get_filter_json()
        assert call_kwargs["output_options"] == {
            "output_type": "ndjson",
            "timestamp_format": "rfc3339",
        }


# -----------------------------------------------------------------------------
# Logpull Tests
# -----------------------------------------------------------------------------


class TestLogpullFetchLogs:
    def test_logpull_fetch_logs(self, cloudflare_settings):
        """Mock streaming API returning log data, assert parsed correctly."""
        lines = [
            '{"EdgeStartTimestamp":"2026-03-01T12:00:00Z","ClientRequestUserAgent":"GPTBot/1.0"}',
            '{"EdgeStartTimestamp":"2026-03-01T12:01:00Z","ClientRequestUserAgent":"GPTBot/1.0"}',
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_lines.return_value = iter(lines)
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_stream = MagicMock(return_value=mock_response)

        with (
            patch(
                "llm_bot_pipeline.cloudflare.logpull.httpx.stream",
                mock_stream,
            ),
            patch(
                "llm_bot_pipeline.cloudflare.logpull.get_cloudflare_client",
                return_value=MagicMock(),
            ),
        ):
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(hours=2)
            end_time = now - timedelta(hours=1)

            records = list(
                logpull.pull_logs(
                    start_time=start_time,
                    end_time=end_time,
                    settings=cloudflare_settings,
                    filter_llm_bots=False,
                )
            )

        assert len(records) == 2
        assert records[0]["EdgeStartTimestamp"] == "2026-03-01T12:00:00Z"
        assert records[0]["ClientRequestUserAgent"] == "GPTBot/1.0"
        assert records[1]["EdgeStartTimestamp"] == "2026-03-01T12:01:00Z"


class TestLogpullApiError:
    def test_logpull_api_error(self, cloudflare_settings):
        """Mock 500 response, assert error raised."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.read.return_value = b"Internal Server Error"
        mock_response.text = "Internal Server Error"
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_stream = MagicMock(return_value=mock_response)

        with (
            patch(
                "llm_bot_pipeline.cloudflare.logpull.httpx.stream",
                mock_stream,
            ),
            patch("llm_bot_pipeline.cloudflare.logpull.time.sleep"),
        ):
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(hours=2)
            end_time = now - timedelta(hours=1)

            with pytest.raises(Exception) as exc_info:
                list(
                    logpull._fetch_logs_chunk(
                        client=MagicMock(),
                        zone_id="test-zone-id",
                        start_time=start_time,
                        end_time=end_time,
                        fields=["EdgeStartTimestamp", "ClientRequestUserAgent"],
                        filter_llm_bots=False,
                        retry_attempts=2,
                        base_delay=0.01,
                    )
                )

        assert "500" in str(exc_info.value)
