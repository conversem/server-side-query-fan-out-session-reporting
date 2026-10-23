"""Unit tests for run_daily_etl stage helpers and orchestrator."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest


class TestRunIngestionStageIsolation:
    """Test _run_ingestion_stage with mocked dependencies."""

    def test_run_ingestion_stage_calls_run_ingestion_with_args(self):
        """Stage delegates to run_ingestion with correct parameters."""
        from llm_bot_pipeline.pipeline.daily.orchestrator import run_ingestion_stage

        mock_logger = MagicMock()
        mock_result = {"success": True, "records": 42}

        with patch(
            "llm_bot_pipeline.pipeline.daily.orchestrator.run_ingestion",
            return_value=mock_result,
        ) as mock_run_ingestion:
            config = {"gcp": {"project_id": "test-project"}}
            result = run_ingestion_stage(
                config=config,
                start_date=date(2026, 1, 15),
                end_date=date(2026, 1, 15),
                dry_run=True,
                logger=mock_logger,
            )

        mock_run_ingestion.assert_called_once_with(
            config=config,
            start_date=date(2026, 1, 15),
            end_date=date(2026, 1, 15),
            dry_run=True,
            logger=mock_logger,
            only_domains=None,
        )
        assert result == mock_result

    def test_run_ingestion_stage_dry_run_returns_early(self):
        """Dry run returns without calling Cloudflare or BigQuery."""
        from llm_bot_pipeline.pipeline.daily.orchestrator import run_ingestion_stage

        with patch(
            "llm_bot_pipeline.pipeline.daily.orchestrator.run_ingestion",
            return_value={"success": True, "records": 0, "dry_run": True},
        ) as mock_run_ingestion:
            result = run_ingestion_stage(
                config={},
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 1),
                dry_run=True,
                logger=MagicMock(),
            )
        assert result["dry_run"] is True
        mock_run_ingestion.assert_called_once()


class TestOrchestratorCallsAllStages:
    """Test run_daily_pipeline orchestrator invokes all stage helpers."""

    def test_orchestrator_calls_all_stages_on_success(self):
        """Orchestrator invokes ingestion, etl, session stages in sequence."""
        from llm_bot_pipeline.pipeline.daily.orchestrator import run_daily_pipeline

        config = {
            "gcp": {"project_id": "test-project"},
            "monitoring": {"alert_channels": []},
        }

        with patch(
            "llm_bot_pipeline.pipeline.daily.orchestrator.load_pipeline_config",
            return_value=config,
        ):
            with patch(
                "llm_bot_pipeline.pipeline.daily.orchestrator.run_ingestion_stage",
                return_value={"success": True, "records": 10},
            ) as mock_ingestion:
                with patch(
                    "llm_bot_pipeline.pipeline.daily.orchestrator.run_etl_stage",
                    return_value={"success": True, "rows": 100},
                ) as mock_etl:
                    with patch(
                        "llm_bot_pipeline.pipeline.daily.orchestrator.run_session_stage",
                        return_value={"success": True},
                    ) as mock_session:
                        with patch(
                            "llm_bot_pipeline.pipeline.daily.orchestrator.track_execution",
                        ) as mock_track:
                            mock_tracker = MagicMock()
                            mock_track.return_value.__enter__ = MagicMock(
                                return_value=mock_tracker
                            )
                            mock_track.return_value.__exit__ = MagicMock(
                                return_value=False
                            )

                            result = run_daily_pipeline(
                                config_path=MagicMock(),
                                default_sops_key=MagicMock(),
                                start_date=date(2026, 1, 1),
                                end_date=date(2026, 1, 1),
                                dry_run=True,
                                skip_ingestion=False,
                                skip_sessions=True,
                                skip_url_details=True,
                                skip_sitemap=True,
                            )

        assert result["success"] is True
        mock_ingestion.assert_called_once()
        mock_etl.assert_called_once()
        mock_session.assert_called_once()
        assert "ingestion" in result["stages"]
        assert "etl" in result["stages"]
        assert "session" in result["stages"]

    def test_orchestrator_skips_ingestion_when_requested(self):
        """Orchestrator skips ingestion stage when skip_ingestion=True."""
        from llm_bot_pipeline.pipeline.daily.orchestrator import run_daily_pipeline

        config = {
            "gcp": {"project_id": "test-project"},
            "monitoring": {"alert_channels": []},
        }

        with patch(
            "llm_bot_pipeline.pipeline.daily.orchestrator.load_pipeline_config",
            return_value=config,
        ):
            with patch(
                "llm_bot_pipeline.pipeline.daily.orchestrator.run_ingestion_stage",
            ) as mock_ingestion:
                with patch(
                    "llm_bot_pipeline.pipeline.daily.orchestrator.run_etl_stage",
                    return_value={"success": True, "rows": 100},
                ):
                    with patch(
                        "llm_bot_pipeline.pipeline.daily.orchestrator.run_session_stage",
                        return_value={"success": True},
                    ):
                        with patch(
                            "llm_bot_pipeline.pipeline.daily.orchestrator.track_execution",
                        ):
                            result = run_daily_pipeline(
                                config_path=MagicMock(),
                                default_sops_key=MagicMock(),
                                start_date=date(2026, 1, 1),
                                end_date=date(2026, 1, 1),
                                dry_run=True,
                                skip_ingestion=True,
                                skip_sessions=True,
                                skip_url_details=True,
                                skip_sitemap=True,
                            )

        mock_ingestion.assert_not_called()
        assert "ingestion" not in result["stages"]
        assert result["success"] is True

    def test_orchestrator_skips_quality_when_requested(self):
        from llm_bot_pipeline.pipeline.daily.orchestrator import run_daily_pipeline

        config = {
            "gcp": {"project_id": "test-project"},
            "monitoring": {"alert_channels": []},
        }
        with patch(
            "llm_bot_pipeline.pipeline.daily.orchestrator.load_pipeline_config",
            return_value=config,
        ):
            with patch(
                "llm_bot_pipeline.pipeline.daily.orchestrator.run_ingestion_stage",
                return_value={"success": True, "records": 1},
            ):
                with patch(
                    "llm_bot_pipeline.pipeline.daily.orchestrator.run_etl_stage",
                    return_value={"success": True, "rows": 1},
                ):
                    with patch(
                        "llm_bot_pipeline.pipeline.daily.orchestrator.run_session_stage",
                        return_value={"success": True},
                    ):
                        with patch(
                            "llm_bot_pipeline.pipeline.daily.orchestrator.track_execution",
                        ):
                            with patch(
                                "llm_bot_pipeline.pipeline.daily.orchestrator.run_quality_stage",
                            ) as mock_quality:
                                result = run_daily_pipeline(
                                    config_path=MagicMock(),
                                    default_sops_key=MagicMock(),
                                    start_date=date(2026, 1, 1),
                                    end_date=date(2026, 1, 1),
                                    dry_run=False,
                                    skip_quality=True,
                                )
        assert result["success"] is True
        mock_quality.assert_not_called()

    def test_orchestrator_skips_failure_alert_when_disabled(self):
        from llm_bot_pipeline.pipeline.daily.orchestrator import run_daily_pipeline

        config = {
            "gcp": {"project_id": "test-project"},
            "monitoring": {"alert_channels": []},
        }
        with patch(
            "llm_bot_pipeline.pipeline.daily.orchestrator.load_pipeline_config",
            return_value=config,
        ):
            with patch(
                "llm_bot_pipeline.pipeline.daily.orchestrator.run_ingestion_stage",
                return_value={"success": False, "error": "boom"},
            ):
                with patch(
                    "llm_bot_pipeline.pipeline.daily.orchestrator.build_alert_manager",
                    return_value=MagicMock(),
                ) as mock_alerts:
                    with patch(
                        "llm_bot_pipeline.pipeline.daily.orchestrator.create_pipeline_failure_alert",
                    ) as mock_create:
                        with patch(
                            "llm_bot_pipeline.pipeline.daily.orchestrator.track_execution",
                        ):
                            result = run_daily_pipeline(
                                config_path=MagicMock(),
                                default_sops_key=MagicMock(),
                                start_date=date(2026, 1, 1),
                                end_date=date(2026, 1, 1),
                                dry_run=True,
                                emit_failure_alert=False,
                            )
        assert result["success"] is False
        mock_create.assert_not_called()
        mock_alerts.return_value.send_alert.assert_not_called()


class TestRunIngestionOnlyDomains:
    def test_run_ingestion_filters_only_domains(self):
        from llm_bot_pipeline.pipeline.daily import stages

        dc_keep = MagicMock(domain="www.example.com", provider="cloudflare")
        dc_skip = MagicMock(domain="other.example.com", provider="cloudflare")
        settings = MagicMock()
        settings.domains = [dc_keep, dc_skip]
        mock_backend = MagicMock()

        with patch.object(stages, "get_backend", return_value=mock_backend):
            with patch.object(stages, "resolve_creds", return_value=("p", "k")):
                with patch(
                    "llm_bot_pipeline.config.settings.get_settings",
                    return_value=settings,
                ):
                    with patch(
                        "llm_bot_pipeline.pipeline.ingestion_dispatch.dispatch",
                        return_value=MagicMock(),
                    ):
                        with patch.object(
                            stages, "ingest_domain", return_value=1
                        ) as mock_ingest:
                            stages.run_ingestion(
                                config={"gcp": {}},
                                start_date=date(2026, 1, 1),
                                end_date=date(2026, 1, 1),
                                only_domains=["www.example.com"],
                            )

        assert mock_ingest.call_count == 1
        assert mock_ingest.call_args[1]["domain_config"] is dc_keep

    def test_run_ingestion_stage_forwards_only_domains(self):
        from llm_bot_pipeline.pipeline.daily.orchestrator import run_ingestion_stage

        with patch(
            "llm_bot_pipeline.pipeline.daily.orchestrator.run_ingestion",
            return_value={"success": True, "records": 1},
        ) as mock_run_ingestion:
            run_ingestion_stage(
                config={},
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 1),
                dry_run=True,
                logger=MagicMock(),
                only_domains=["www.example.com"],
            )
        assert mock_run_ingestion.call_args[1]["only_domains"] == ["www.example.com"]


class TestRunAggregationsTrackFreshness:
    def test_run_aggregations_skips_log_freshness_when_disabled(self):
        from llm_bot_pipeline.pipeline.daily import stages

        mock_agg = MagicMock()
        daily = MagicMock(success=True, rows_inserted=1, error=None)
        url = MagicMock(success=True, rows_inserted=1, error=None)
        mock_agg.aggregate_daily_summary.return_value = daily
        mock_agg.aggregate_url_performance.return_value = url

        with patch.object(stages, "ReportingAggregator", return_value=mock_agg):
            with patch.object(stages, "resolve_creds", return_value=("p", "k")):
                result = stages.run_aggregations(
                    config={},
                    start_date=date(2026, 1, 1),
                    end_date=date(2026, 1, 1),
                    track_freshness=False,
                )

        assert result["success"] is True
        mock_agg.log_freshness.assert_not_called()
