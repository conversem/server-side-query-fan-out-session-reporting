"""
Unit tests for run_pipeline router dispatch logic.

Tests mode validation, dispatch to correct pipeline implementations,
and that backends/pipelines are created as expected (all mocked).
"""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from llm_bot_pipeline.pipeline.router import run_pipeline


@pytest.fixture
def mock_settings():
    """Settings mock with processing_mode."""
    s = MagicMock()
    s.processing_mode = "local_sqlite"
    s.gcp_project_id = "test-project"
    s.service_account_key_path = MagicMock()
    s.service_account_key_path.exists.return_value = False
    s.dataset_raw = "raw"
    s.dataset_report = "report"
    s.gcp_location = "EU"
    s.sqlite_db_path = "/tmp/test.db"
    s.checkpoint_path = "data/checkpoint.json"
    return s


@pytest.fixture
def sample_dates():
    """Common date range for tests."""
    return date(2025, 3, 1), date(2025, 3, 3)


class TestInvalidMode:
    """Invalid processing_mode raises ValueError."""

    def test_invalid_mode_raises(self, mock_settings, sample_dates):
        start_date, end_date = sample_dates
        with patch(
            "llm_bot_pipeline.pipeline.router.get_settings",
            return_value=mock_settings,
        ):
            with pytest.raises(ValueError) as exc_info:
                run_pipeline(start_date, end_date, processing_mode="invalid_mode")
            assert "Invalid processing_mode" in str(exc_info.value)
            assert "invalid_mode" in str(exc_info.value)


class TestLocalSqliteDispatch:
    """local_sqlite creates LocalPipeline with sqlite backend."""

    def test_local_sqlite_dispatches(self, mock_settings, sample_dates):
        start_date, end_date = sample_dates
        mock_sqlite = MagicMock()
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = {"rows": 42}

        with patch(
            "llm_bot_pipeline.pipeline.router.get_settings",
            return_value=mock_settings,
        ):
            with patch(
                "llm_bot_pipeline.pipeline.router._make_sqlite_backend",
                return_value=mock_sqlite,
            ):
                with patch(
                    "llm_bot_pipeline.pipeline.local_pipeline.LocalPipeline",
                    return_value=mock_pipeline,
                ) as LocalPipelineCls:
                    result = run_pipeline(
                        start_date,
                        end_date,
                        processing_mode="local_sqlite",
                        dry_run=True,
                    )

        LocalPipelineCls.assert_called_once_with(backend=mock_sqlite)
        mock_pipeline.run.assert_called_once_with(start_date, end_date, dry_run=True)
        mock_pipeline.close.assert_called_once()
        assert result == {"rows": 42}


class TestLocalBqBufferedDispatch:
    """local_bq_buffered creates LocalPipeline with sqlite + output_backend."""

    def test_local_bq_buffered_dispatches(self, mock_settings, sample_dates):
        start_date, end_date = sample_dates
        mock_sqlite = MagicMock()
        mock_bq = MagicMock()
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = {"rows": 100}

        with patch(
            "llm_bot_pipeline.pipeline.router.get_settings",
            return_value=mock_settings,
        ):
            with patch(
                "llm_bot_pipeline.pipeline.router._make_sqlite_backend",
                return_value=mock_sqlite,
            ):
                with patch(
                    "llm_bot_pipeline.pipeline.router._make_bq_backend",
                    return_value=mock_bq,
                ):
                    with patch(
                        "llm_bot_pipeline.pipeline.local_pipeline.LocalPipeline",
                        return_value=mock_pipeline,
                    ) as LocalPipelineCls:
                        result = run_pipeline(
                            start_date,
                            end_date,
                            processing_mode="local_bq_buffered",
                            dry_run=True,
                        )

        LocalPipelineCls.assert_called_once_with(
            backend=mock_sqlite,
            output_backend=mock_bq,
            checkpoint_path=Path("data/checkpoint.json"),
        )
        mock_pipeline.run.assert_called_once_with(start_date, end_date, dry_run=True)
        mock_pipeline.close.assert_called_once()
        mock_bq.close.assert_called_once()
        assert result == {"rows": 100}


class TestLocalBqStreaming:
    """local_bq_streaming requires records and creates StreamingPipeline."""

    def test_local_bq_streaming_requires_records(self, mock_settings, sample_dates):
        start_date, end_date = sample_dates
        mock_settings.processing_mode = "local_bq_streaming"

        with patch(
            "llm_bot_pipeline.pipeline.router.get_settings",
            return_value=mock_settings,
        ):
            with pytest.raises(ValueError) as exc_info:
                run_pipeline(
                    start_date,
                    end_date,
                    processing_mode="local_bq_streaming",
                    records=None,
                )
            assert "records" in str(exc_info.value).lower()
            assert "local_bq_streaming" in str(exc_info.value)

    def test_local_bq_streaming_dispatches(self, mock_settings, sample_dates):
        start_date, end_date = sample_dates
        records = iter([MagicMock()])
        mock_bq = MagicMock()
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = {"streamed": 5}

        with patch(
            "llm_bot_pipeline.pipeline.router.get_settings",
            return_value=mock_settings,
        ):
            with patch(
                "llm_bot_pipeline.pipeline.router._make_bq_backend",
                return_value=mock_bq,
            ):
                with patch(
                    "llm_bot_pipeline.pipeline.streaming_pipeline.StreamingPipeline",
                    return_value=mock_pipeline,
                ) as StreamingPipelineCls:
                    result = run_pipeline(
                        start_date,
                        end_date,
                        processing_mode="local_bq_streaming",
                        records=records,
                    )

        StreamingPipelineCls.assert_called_once()
        call_kw = StreamingPipelineCls.call_args[1]
        assert call_kw["output_backend"] == mock_bq
        mock_pipeline.run.assert_called_once_with(
            records, start_date=start_date, end_date=end_date
        )
        mock_bq.close.assert_called_once()
        assert result == {"streamed": 5}


@pytest.mark.bigquery
class TestGcpBqDispatch:
    """gcp_bq creates ETLPipeline."""

    def test_gcp_bq_dispatches(self, mock_settings, sample_dates):
        start_date, end_date = sample_dates
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = {"job_id": "abc123"}

        with patch(
            "llm_bot_pipeline.pipeline.router.get_settings",
            return_value=mock_settings,
        ):
            with patch(
                "llm_bot_pipeline.pipeline.orchestrator.ETLPipeline",
                return_value=mock_pipeline,
            ) as ETLPipelineCls:
                result = run_pipeline(
                    start_date, end_date, processing_mode="gcp_bq", dry_run=True
                )

        ETLPipelineCls.assert_called_once_with(
            project_id="test-project",
            credentials_path=None,
        )
        mock_pipeline.run.assert_called_once_with(
            start_date=start_date, end_date=end_date
        )
        assert result == {"job_id": "abc123"}


class TestModeFromSettings:
    """Omitting mode uses settings.processing_mode."""

    def test_mode_from_settings(self, mock_settings, sample_dates):
        start_date, end_date = sample_dates
        mock_settings.processing_mode = "local_bq_buffered"
        mock_sqlite = MagicMock()
        mock_bq = MagicMock()
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = {}

        with patch(
            "llm_bot_pipeline.pipeline.router.get_settings",
            return_value=mock_settings,
        ):
            with patch(
                "llm_bot_pipeline.pipeline.router._make_sqlite_backend",
                return_value=mock_sqlite,
            ):
                with patch(
                    "llm_bot_pipeline.pipeline.router._make_bq_backend",
                    return_value=mock_bq,
                ):
                    with patch(
                        "llm_bot_pipeline.pipeline.local_pipeline.LocalPipeline",
                        return_value=mock_pipeline,
                    ) as LocalPipelineCls:
                        run_pipeline(start_date, end_date)

        LocalPipelineCls.assert_called_once_with(
            backend=mock_sqlite,
            output_backend=mock_bq,
            checkpoint_path=Path("data/checkpoint.json"),
        )
