"""Tests for processing_mode features in Settings."""

import os
from unittest.mock import patch

import pytest

from llm_bot_pipeline.config.settings import (
    ConfigurationError,
    Settings,
    clear_settings_cache,
    get_settings,
)


class TestDefaultProcessingMode:
    def test_default_processing_mode(self):
        s = Settings()
        assert s.processing_mode == "local_sqlite"


class TestValidation:
    def test_validate_invalid_mode(self):
        s = Settings(processing_mode="bad_mode")
        errors = s.validate()
        assert any("processing_mode" in e for e in errors)

    def test_validate_bq_mode_requires_project_id(self):
        s = Settings(processing_mode="local_bq_buffered", gcp_project_id="")
        errors = s.validate()
        assert any("gcp" in e.lower() and "project" in e.lower() for e in errors)

    def test_validate_local_sqlite_no_project_ok(self):
        s = Settings(processing_mode="local_sqlite", gcp_project_id="")
        errors = s.validate()
        mode_errors = [e for e in errors if "processing_mode" in e]
        assert mode_errors == []


class TestProperties:
    @pytest.mark.parametrize(
        "mode,expected",
        [
            ("local_sqlite", True),
            ("local_bq_buffered", True),
            ("local_bq_streaming", False),
            ("gcp_bq", False),
        ],
    )
    def test_needs_sqlite_property(self, mode, expected):
        s = Settings(processing_mode=mode)
        assert s.needs_sqlite is expected

    @pytest.mark.parametrize(
        "mode,expected",
        [
            ("local_sqlite", False),
            ("local_bq_buffered", True),
            ("local_bq_streaming", True),
            ("gcp_bq", True),
        ],
    )
    def test_needs_bigquery_property(self, mode, expected):
        s = Settings(processing_mode=mode)
        assert s.needs_bigquery is expected


class TestFromDict:
    def test_from_dict_reads_pipeline_key(self):
        config = {
            "pipeline": {"processing_mode": "gcp_bq"},
            "gcp": {"project_id": "my-proj"},
        }
        s = Settings.from_dict(config)
        assert s.processing_mode == "gcp_bq"

    def test_from_dict_reads_metrics_config(self):
        config = {
            "metrics": {
                "export_enabled": True,
                "backend": "cloud_monitoring",
                "pushgateway_url": "http://push:9091",
            },
            "gcp": {"project_id": "my-proj"},
        }
        s = Settings.from_dict(config)
        assert s.metrics_export_enabled is True
        assert s.metrics_backend == "cloud_monitoring"
        assert s.metrics_pushgateway_url == "http://push:9091"


class TestMetricsValidation:
    def test_validate_invalid_metrics_backend(self):
        s = Settings(metrics_backend="invalid")
        errors = s.validate()
        assert any("metrics_backend" in e for e in errors)

    def test_validate_cloud_monitoring_requires_project_id(self):
        s = Settings(
            metrics_export_enabled=True,
            metrics_backend="cloud_monitoring",
            gcp_project_id="",
        )
        errors = s.validate()
        assert any("project_id" in e for e in errors)


class TestFromEnv:
    def test_from_env_reads_processing_mode(self):
        with patch.dict(os.environ, {"PROCESSING_MODE": "local_bq_streaming"}):
            s = Settings.from_env()
            assert s.processing_mode == "local_bq_streaming"


class TestSettingsRepr:
    def test_settings_repr_redacts_token(self):
        s = Settings(cloudflare_api_token="secret-token-123")
        r = repr(s)
        assert "***REDACTED***" in r
        assert "secret-token-123" not in r

    def test_settings_repr_shows_safe_fields(self):
        s = Settings(
            processing_mode="local_sqlite",
            gcp_project_id="my-project",
        )
        r = repr(s)
        assert "processing_mode='local_sqlite'" in r or "processing_mode=" in r
        assert "gcp_project_id='my-project'" in r or "gcp_project_id=" in r


class TestGetSettingsValidation:
    """Tests that get_settings() calls validate() and raises ConfigurationError."""

    def setup_method(self):
        clear_settings_cache()

    def teardown_method(self):
        clear_settings_cache()

    def test_get_settings_valid_config(self, tmp_path):
        env = {"PROCESSING_MODE": "local_sqlite", "STORAGE_BACKEND": "sqlite"}
        with patch.dict(os.environ, env, clear=False):
            clear_settings_cache()
            settings = get_settings(config_path=str(tmp_path / "nonexistent.yaml"))
            assert isinstance(settings, Settings)
            assert settings.processing_mode == "local_sqlite"

    def test_get_settings_invalid_mode(self, tmp_path):
        env = {"PROCESSING_MODE": "invalid", "STORAGE_BACKEND": "sqlite"}
        with patch.dict(os.environ, env, clear=False):
            clear_settings_cache()
            with pytest.raises(ConfigurationError, match="processing_mode"):
                get_settings(config_path=str(tmp_path / "nonexistent.yaml"))

    def test_get_settings_bq_missing_project(self, tmp_path):
        env = {
            "PROCESSING_MODE": "gcp_bq",
            "STORAGE_BACKEND": "bigquery",
            "GCP_PROJECT_ID": "",
        }
        with patch.dict(os.environ, env, clear=False):
            clear_settings_cache()
            with pytest.raises(ConfigurationError, match="project"):
                get_settings(config_path=str(tmp_path / "nonexistent.yaml"))
