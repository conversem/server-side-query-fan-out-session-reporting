"""Unit tests for config/sops_loader.py decryption and error handling."""

import subprocess
from unittest.mock import patch

import pytest

from llm_bot_pipeline.config.sops_loader import (
    check_sops_installed,
    decrypt_sops_file,
    load_config,
)


class TestDecryptSopsSuccess:
    """Verify successful SOPS decryption and YAML parsing."""

    def test_sops_decrypt_success(self, tmp_path):
        """Mock subprocess.run returning valid YAML, assert parsed config."""
        config_file = tmp_path / "secrets.yaml"
        config_file.touch()

        mock_yaml = "storage:\n  backend: sqlite\n  sqlite_db_path: /tmp/test.db"
        mock_result = type("Result", (), {"stdout": mock_yaml, "returncode": 0})()

        with patch("llm_bot_pipeline.config.sops_loader.subprocess.run") as mock_run:
            mock_run.return_value = mock_result

            config = decrypt_sops_file(config_file)

        assert config == {
            "storage": {
                "backend": "sqlite",
                "sqlite_db_path": "/tmp/test.db",
            }
        }
        mock_run.assert_called_once_with(
            ["sops", "-d", str(config_file)],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )


class TestDecryptSopsFileNotFound:
    """Verify FileNotFoundError when file does not exist."""

    def test_sops_file_not_found(self, tmp_path):
        """Test with non-existent path, assert FileNotFoundError."""
        nonexistent = tmp_path / "nonexistent.yaml"

        with pytest.raises(FileNotFoundError, match="Encrypted config file not found"):
            decrypt_sops_file(nonexistent)


class TestDecryptSopsFailure:
    """Verify error handling when SOPS decryption fails."""

    def test_sops_decrypt_failure(self, tmp_path):
        """Mock subprocess returning error, assert error handling."""
        config_file = tmp_path / "secrets.yaml"
        config_file.touch()

        with patch("llm_bot_pipeline.config.sops_loader.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=["sops", "-d", str(config_file)],
                stderr="failed to decrypt: invalid key",
            )

            with pytest.raises(
                RuntimeError, match="SOPS decryption failed"
            ) as exc_info:
                decrypt_sops_file(config_file)

        assert "invalid key" in str(exc_info.value)


class TestDecryptSopsNotInstalled:
    """Verify RuntimeError when SOPS is not installed."""

    def test_sops_not_installed(self, tmp_path):
        """Mock FileNotFoundError from subprocess (sops not found)."""
        config_file = tmp_path / "secrets.yaml"
        config_file.touch()

        with patch("llm_bot_pipeline.config.sops_loader.subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError("sops: command not found")

            with pytest.raises(RuntimeError, match="SOPS not installed"):
                decrypt_sops_file(config_file)


class TestLoadConfig:
    """Verify load_config behavior."""

    def test_load_config_success(self, tmp_path):
        """Load from SOPS file when decryption succeeds."""
        config_file = tmp_path / "secrets.yaml"
        config_file.touch()

        mock_yaml = "cloudflare:\n  api_token: abc123"
        mock_result = type("Result", (), {"stdout": mock_yaml, "returncode": 0})()

        with patch("llm_bot_pipeline.config.sops_loader.subprocess.run") as mock_run:
            mock_run.return_value = mock_result

            config = load_config(encrypted_path=config_file)

        assert config == {"cloudflare": {"api_token": "abc123"}}

    def test_load_config_file_not_found_fallback(self, tmp_path):
        """Fall back to env when encrypted path does not exist."""
        nonexistent = tmp_path / "nonexistent.yaml"

        config = load_config(encrypted_path=nonexistent, fallback_to_env=True)

        assert "storage" in config
        assert config["storage"]["backend"] == "sqlite"

    def test_load_config_decrypt_failure_fallback(self, tmp_path):
        """Fall back to env when decryption fails and fallback_to_env=True."""
        config_file = tmp_path / "secrets.yaml"
        config_file.touch()

        with patch("llm_bot_pipeline.config.sops_loader.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=["sops", "-d", str(config_file)],
                stderr="decrypt failed",
            )

            config = load_config(encrypted_path=config_file, fallback_to_env=True)

        assert "storage" in config

    def test_load_config_decrypt_failure_no_fallback(self, tmp_path):
        """Raise when decryption fails and fallback_to_env=False."""
        config_file = tmp_path / "secrets.yaml"
        config_file.touch()

        with patch("llm_bot_pipeline.config.sops_loader.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=["sops", "-d", str(config_file)],
                stderr="decrypt failed",
            )

            with pytest.raises(RuntimeError, match="SOPS decryption failed"):
                load_config(encrypted_path=config_file, fallback_to_env=False)


class TestCheckSopsInstalled:
    """Verify check_sops_installed behavior."""

    def test_check_sops_installed_true(self):
        """Return True when sops runs successfully."""
        with patch("llm_bot_pipeline.config.sops_loader.subprocess.run") as mock_run:
            mock_run.return_value = type("Result", (), {"returncode": 0})()

            assert check_sops_installed() is True

    def test_check_sops_installed_false(self):
        """Return False when sops fails or is not found."""
        with patch("llm_bot_pipeline.config.sops_loader.subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError()

            assert check_sops_installed() is False
