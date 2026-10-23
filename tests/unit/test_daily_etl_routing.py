"""Unit tests for run_daily_etl.py processing_mode routing."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

# Import before autouse freezegun; a late pandas import via bigquery segfaults.
from google.cloud.bigquery import ScalarQueryParameter  # noqa: F401  # isort: skip


class TestDailyEtlLocalSqliteMode:
    """--processing-mode local_sqlite dispatches via router."""

    def test_daily_etl_local_sqlite_mode(self):
        """Verify LocalPipeline path when --processing-mode local_sqlite."""
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        with patch(
            "sys.argv",
            [
                "run_daily_etl",
                "--date",
                yesterday,
                "--processing-mode",
                "local_sqlite",
                "--no-log-file",
            ],
        ):
            with patch(
                "scripts.run_daily_etl.router_run_pipeline",
                return_value=MagicMock(success=True),
            ) as mock_router:
                with patch(
                    "scripts.run_daily_etl.run_daily_pipeline",
                ) as mock_daily:
                    from scripts.run_daily_etl import main

                    exit_code = main()
        assert exit_code == 0
        mock_router.assert_called_once()
        call_kw = mock_router.call_args[1]
        assert call_kw["processing_mode"] == "local_sqlite"
        mock_daily.assert_not_called()


class TestDailyEtlGcpBqMode:
    """--processing-mode gcp_bq uses BQ ETL path."""

    def test_daily_etl_gcp_bq_mode(self):
        """Verify BQ ETL path when --processing-mode gcp_bq (single date)."""
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        with patch(
            "sys.argv",
            [
                "run_daily_etl",
                "--date",
                yesterday,
                "--processing-mode",
                "gcp_bq",
                "--no-log-file",
            ],
        ):
            with patch(
                "scripts.run_daily_etl.run_daily_pipeline",
                return_value={"success": True},
            ) as mock_daily:
                with patch(
                    "scripts.run_daily_etl.router_run_pipeline",
                ) as mock_router:
                    with patch(
                        "scripts.run_daily_etl.run_backfill_pipeline",
                    ) as mock_backfill:
                        from scripts.run_daily_etl import main

                        exit_code = main()
        assert exit_code == 0
        mock_daily.assert_called_once()
        mock_backfill.assert_not_called()
        mock_router.assert_not_called()

    def test_daily_etl_gcp_bq_backfill_uses_run_backfill_pipeline(self):
        """Verify backfill path when gcp_bq + date range."""
        with patch(
            "sys.argv",
            [
                "run_daily_etl",
                "--start-date",
                "2026-01-20",
                "--end-date",
                "2026-01-22",
                "--processing-mode",
                "gcp_bq",
                "--no-log-file",
            ],
        ):
            with patch(
                "scripts.run_daily_etl.run_backfill_pipeline",
                return_value={"success": True},
            ) as mock_backfill:
                with patch(
                    "scripts.run_daily_etl.run_daily_pipeline",
                ) as mock_daily:
                    with patch(
                        "scripts.run_daily_etl.router_run_pipeline",
                    ) as mock_router:
                        from scripts.run_daily_etl import main

                        exit_code = main()
        assert exit_code == 0
        mock_backfill.assert_called_once()
        call_kw = mock_backfill.call_args[1]
        assert call_kw["start_date"].isoformat() == "2026-01-20"
        assert call_kw["end_date"].isoformat() == "2026-01-22"
        assert "state_path" in call_kw
        mock_daily.assert_not_called()
        mock_router.assert_not_called()


class TestDailyEtlDefaultFromConfig:
    """Omitting --processing-mode uses config fallback."""

    def test_daily_etl_default_from_config(self):
        """Verify config fallback when --processing-mode not specified."""
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        mock_settings = MagicMock()
        mock_settings.processing_mode = "local_bq_buffered"

        with patch(
            "sys.argv",
            ["run_daily_etl", "--date", yesterday, "--no-log-file"],
        ):
            with patch(
                "llm_bot_pipeline.config.settings.get_settings",
                return_value=mock_settings,
            ):
                with patch(
                    "scripts.run_daily_etl.router_run_pipeline",
                    return_value=MagicMock(success=True),
                ) as mock_router:
                    with patch(
                        "scripts.run_daily_etl.run_daily_pipeline",
                    ) as mock_daily:
                        from scripts.run_daily_etl import main

                        exit_code = main()
        assert exit_code == 0
        mock_router.assert_called_once()
        call_kw = mock_router.call_args[1]
        assert call_kw["processing_mode"] == "local_bq_buffered"
        mock_daily.assert_not_called()


class TestDailyEtlAutoBackfill:
    def _settings(self):
        mock_settings = MagicMock()
        mock_settings.processing_mode = "gcp_bq"
        mock_settings.auto_backfill.enabled = True
        mock_settings.auto_backfill.lookback_days = 7
        mock_settings.auto_backfill.min_raw_rows = 100
        mock_settings.auto_backfill.time_budget_seconds = 2400
        mock_settings.auto_backfill.respect_cloudflare_retention = True
        mock_settings.domains = [
            MagicMock(domain="www.example.com", provider="cloudflare")
        ]
        return mock_settings

    def test_unflagged_gcp_bq_uses_auto_backfill(self):
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        with patch(
            "sys.argv",
            ["run_daily_etl", "--processing-mode", "gcp_bq", "--no-log-file"],
        ):
            with patch(
                "llm_bot_pipeline.config.settings.get_settings",
                return_value=self._settings(),
            ):
                with patch(
                    "scripts.run_daily_etl.run_auto_backfill_pipeline",
                    return_value={"success": True},
                ) as mock_auto:
                    with patch(
                        "scripts.run_daily_etl.run_daily_pipeline"
                    ) as mock_daily:
                        with patch(
                            "scripts.run_daily_etl.run_backfill_pipeline"
                        ) as mock_backfill:
                            from scripts.run_daily_etl import main

                            assert main() == 0
        mock_auto.assert_called_once()
        mock_daily.assert_not_called()
        mock_backfill.assert_not_called()
        assert mock_auto.call_args[1]["yesterday"] == yesterday

    def test_explicit_date_skips_auto_backfill(self):
        yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        with patch(
            "sys.argv",
            [
                "run_daily_etl",
                "--date",
                yesterday,
                "--processing-mode",
                "gcp_bq",
                "--no-log-file",
            ],
        ):
            with patch(
                "scripts.run_daily_etl.run_daily_pipeline",
                return_value={"success": True},
            ) as mock_daily:
                with patch(
                    "scripts.run_daily_etl.run_auto_backfill_pipeline"
                ) as mock_auto:
                    from scripts.run_daily_etl import main

                    assert main() == 0
        mock_daily.assert_called_once()
        mock_auto.assert_not_called()

    def test_days_seven_skips_auto_backfill(self):
        with patch(
            "sys.argv",
            [
                "run_daily_etl",
                "--days",
                "7",
                "--processing-mode",
                "gcp_bq",
                "--no-log-file",
            ],
        ):
            with patch(
                "scripts.run_daily_etl.run_backfill_pipeline",
                return_value={"success": True},
            ) as mock_backfill:
                with patch(
                    "scripts.run_daily_etl.run_auto_backfill_pipeline"
                ) as mock_auto:
                    from scripts.run_daily_etl import main

                    assert main() == 0
        mock_backfill.assert_called_once()
        mock_auto.assert_not_called()

    def test_days_one_still_uses_auto_backfill(self):
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        with (
            patch(
                "sys.argv",
                [
                    "run_daily_etl",
                    "--days",
                    "1",
                    "--processing-mode",
                    "gcp_bq",
                    "--no-log-file",
                ],
            ),
            patch(
                "llm_bot_pipeline.config.settings.get_settings",
                return_value=self._settings(),
            ),
            patch(
                "scripts.run_daily_etl.run_auto_backfill_pipeline",
                return_value={"success": True},
            ) as mock_auto,
            patch("scripts.run_daily_etl.run_daily_pipeline") as mock_daily,
        ):
            from scripts.run_daily_etl import main

            assert main() == 0
        mock_auto.assert_called_once()
        mock_daily.assert_not_called()
        assert mock_auto.call_args[1]["yesterday"] == yesterday

    def test_no_auto_backfill_flag_uses_yesterday_only(self):
        with patch(
            "sys.argv",
            [
                "run_daily_etl",
                "--no-auto-backfill",
                "--processing-mode",
                "gcp_bq",
                "--no-log-file",
            ],
        ):
            with patch(
                "scripts.run_daily_etl.run_daily_pipeline",
                return_value={"success": True},
            ) as mock_daily:
                with patch(
                    "scripts.run_daily_etl.run_auto_backfill_pipeline"
                ) as mock_auto:
                    from scripts.run_daily_etl import main

                    assert main() == 0
        mock_daily.assert_called_once()
        mock_auto.assert_not_called()
