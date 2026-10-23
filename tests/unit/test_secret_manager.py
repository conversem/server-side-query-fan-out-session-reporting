"""Tests for secret_manager module."""

import builtins
import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound

from llm_bot_pipeline.config.secret_manager import (
    create_secret,
    is_secret_manager_enabled,
    load_from_secret_manager,
)


class TestLoadConfigSuccess:
    """Verify load_from_secret_manager returns parsed config on success."""

    def test_load_config_success(self):
        """Mock SM client returning valid JSON, assert parsed dict."""
        mock_response = MagicMock()
        mock_response.payload.data = (
            b'{"pipeline": {"processing_mode": "local_sqlite"}, "key": "value"}'
        )

        with patch(
            "google.cloud.secretmanager.SecretManagerServiceClient"
        ) as mock_client_cls:
            mock_client = MagicMock()
            mock_client.access_secret_version.return_value = mock_response
            mock_client_cls.return_value = mock_client

            result = load_from_secret_manager(
                project_id="test-proj", secret_name="test-secret"
            )

        assert result == {
            "pipeline": {"processing_mode": "local_sqlite"},
            "key": "value",
        }
        mock_client.access_secret_version.assert_called_once_with(
            request={"name": "projects/test-proj/secrets/test-secret/versions/latest"}
        )

    def test_load_config_env_defaults(self):
        """Secret name and project_id from env when not passed."""
        mock_response = MagicMock()
        mock_response.payload.data = b'{"key": "value"}'

        with patch.dict(
            "os.environ",
            {"GCP_PROJECT_ID": "env-proj", "CONFIG_SECRET": "env-secret"},
            clear=False,
        ):
            with patch(
                "google.cloud.secretmanager.SecretManagerServiceClient"
            ) as mock_client_cls:
                mock_client = MagicMock()
                mock_client.access_secret_version.return_value = mock_response
                mock_client_cls.return_value = mock_client

                result = load_from_secret_manager()

        assert result == {"key": "value"}
        mock_client.access_secret_version.assert_called_once_with(
            request={"name": "projects/env-proj/secrets/env-secret/versions/latest"}
        )


class TestLoadConfigMissingSecret:
    """Verify error handling when secret is not found."""

    def test_load_config_missing_secret(self):
        """Mock SM client raising NotFound, assert error handling."""
        with patch(
            "google.cloud.secretmanager.SecretManagerServiceClient"
        ) as mock_client_cls:
            mock_client = MagicMock()
            mock_client.access_secret_version.side_effect = NotFound("Secret not found")
            mock_client_cls.return_value = mock_client

            with pytest.raises(RuntimeError, match="Failed to access secret"):
                load_from_secret_manager(
                    project_id="test-proj", secret_name="missing-secret"
                )


class TestLoadConfigInvalidJson:
    """Verify error handling when secret contains invalid JSON."""

    def test_load_config_invalid_json(self):
        """Mock SM returning invalid JSON, assert error handling."""
        mock_response = MagicMock()
        mock_response.payload.data = b"not valid json {{{"

        with patch(
            "google.cloud.secretmanager.SecretManagerServiceClient"
        ) as mock_client_cls:
            mock_client = MagicMock()
            mock_client.access_secret_version.return_value = mock_response
            mock_client_cls.return_value = mock_client

            with pytest.raises(RuntimeError, match="does not contain valid JSON"):
                load_from_secret_manager(
                    project_id="test-proj", secret_name="bad-secret"
                )


class TestLogLevelIsDebug:
    """Assert secret metadata logged at DEBUG level, not INFO."""

    def test_log_level_is_debug(self, caplog):
        """Loaded config message is DEBUG, not INFO."""
        mock_response = MagicMock()
        mock_response.payload.data = (
            b'{"pipeline": {"processing_mode": "local_sqlite"}}'
        )

        with patch(
            "google.cloud.secretmanager.SecretManagerServiceClient"
        ) as mock_client_cls:
            mock_client = MagicMock()
            mock_client.access_secret_version.return_value = mock_response
            mock_client_cls.return_value = mock_client

            with caplog.at_level(logging.INFO):
                load_from_secret_manager(
                    project_id="test-proj", secret_name="test-secret"
                )

            info_messages = [
                r.message for r in caplog.records if r.levelno == logging.INFO
            ]
            assert not any("Loaded config" in m for m in info_messages)

        with patch(
            "google.cloud.secretmanager.SecretManagerServiceClient"
        ) as mock_client_cls:
            mock_client = MagicMock()
            mock_client.access_secret_version.return_value = mock_response
            mock_client_cls.return_value = mock_client

            with caplog.at_level(logging.DEBUG):
                load_from_secret_manager(
                    project_id="test-proj", secret_name="test-secret"
                )

            debug_messages = [
                r.message for r in caplog.records if r.levelno == logging.DEBUG
            ]
            assert any("Loaded config" in m for m in debug_messages)


class TestLoadConfigValueError:
    """Verify ValueError when project_id is not provided."""

    def test_load_config_value_error_no_project_id(self):
        """Raise ValueError when project_id is None and env not set."""
        with patch.dict("os.environ", {"GCP_PROJECT_ID": ""}, clear=False):
            with pytest.raises(ValueError, match="GCP project ID is required"):
                load_from_secret_manager(project_id=None)

    def test_load_config_value_error_empty_project_id(self):
        """Raise ValueError when project_id is empty string."""
        with pytest.raises(ValueError, match="GCP project ID is required"):
            load_from_secret_manager(project_id="")


class TestLoadConfigImportError:
    """Verify ImportError when google-cloud-secret-manager is not installed."""

    def test_load_config_import_error(self):
        """Raise ImportError when secretmanager module cannot be imported."""
        import sys

        saved = sys.modules.pop("google.cloud.secretmanager", None)
        parent = sys.modules.get("google.cloud")
        if parent is not None and hasattr(parent, "secretmanager"):
            delattr(parent, "secretmanager")
        try:
            orig_import = builtins.__import__

            def mock_import(name, *args, **kwargs):
                if name == "google.cloud.secretmanager":
                    raise ImportError("No module named 'google.cloud.secretmanager'")
                return orig_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=mock_import):
                with pytest.raises(ImportError, match="google-cloud-secret-manager"):
                    load_from_secret_manager(project_id="test-proj")
        finally:
            if saved is not None:
                sys.modules["google.cloud.secretmanager"] = saved


class TestIsSecretManagerEnabled:
    """Verify is_secret_manager_enabled checks env correctly."""

    def test_enabled_true(self):
        """USE_SECRET_MANAGER=true enables."""
        with patch.dict("os.environ", {"USE_SECRET_MANAGER": "true"}):
            assert is_secret_manager_enabled() is True

    def test_enabled_1(self):
        """USE_SECRET_MANAGER=1 enables."""
        with patch.dict("os.environ", {"USE_SECRET_MANAGER": "1"}):
            assert is_secret_manager_enabled() is True

    def test_disabled_empty(self):
        """Empty or unset disables."""
        with patch.dict("os.environ", {"USE_SECRET_MANAGER": ""}, clear=False):
            assert is_secret_manager_enabled() is False


class TestCreateSecret:
    """Verify create_secret creates or updates secrets."""

    def test_create_secret_new(self):
        """Create new secret when get_secret raises NotFound."""
        mock_version = MagicMock()
        mock_version.name = "projects/p/secrets/s/versions/1"

        with patch(
            "google.cloud.secretmanager.SecretManagerServiceClient"
        ) as mock_client_cls:
            mock_client = MagicMock()
            mock_client.get_secret.side_effect = NotFound("not found")
            mock_client.add_secret_version.return_value = mock_version
            mock_client_cls.return_value = mock_client

            result = create_secret(
                project_id="p",
                secret_name="s",
                config={"key": "value"},
            )

        assert result == "projects/p/secrets/s/versions/1"
        mock_client.create_secret.assert_called_once()
        mock_client.add_secret_version.assert_called_once()
        call = mock_client.add_secret_version.call_args
        assert call.kwargs["request"]["payload"]["data"] == json.dumps(
            {"key": "value"}, indent=2
        ).encode("utf-8")

    def test_create_secret_exists(self):
        """Add version when secret already exists."""
        mock_version = MagicMock()
        mock_version.name = "projects/p/secrets/s/versions/2"

        with patch(
            "google.cloud.secretmanager.SecretManagerServiceClient"
        ) as mock_client_cls:
            mock_client = MagicMock()
            mock_client.get_secret.return_value = MagicMock()
            mock_client.add_secret_version.return_value = mock_version
            mock_client_cls.return_value = mock_client

            result = create_secret(
                project_id="p",
                secret_name="s",
                config={"a": 1},
                locations=["us-central1"],
            )

        assert result == "projects/p/secrets/s/versions/2"
        mock_client.create_secret.assert_not_called()
        mock_client.add_secret_version.assert_called_once()
