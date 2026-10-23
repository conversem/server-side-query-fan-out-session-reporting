"""Configuration loading for the daily ETL runner."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from ...config import load_config


def setup_sops_environment(default_sops_key: Path) -> bool:
    """Ensure ``SOPS_AGE_KEY_FILE`` is set for config decryption."""
    if os.environ.get("SOPS_AGE_KEY_FILE"):
        return True
    if default_sops_key.exists():
        os.environ["SOPS_AGE_KEY_FILE"] = str(default_sops_key)
        return True
    return False


def load_daily_config(config_path: Path, default_sops_key: Path) -> dict:
    """Load daily pipeline config from Secret Manager, SOPS, or environment."""
    from ...config.secret_manager import (
        is_secret_manager_enabled,
        load_from_secret_manager,
    )

    if is_secret_manager_enabled():
        try:
            config = load_from_secret_manager()
            if config:
                return config
        except Exception as e:
            logging.getLogger(__name__).warning(
                "Secret Manager load failed, falling back to SOPS: %s", e
            )

    if not setup_sops_environment(default_sops_key):
        raise RuntimeError(
            f"SOPS not configured. Set SOPS_AGE_KEY_FILE or create {default_sops_key}"
        )

    config = load_config(encrypted_path=config_path, fallback_to_env=True)
    if not config:
        raise RuntimeError("Failed to load configuration")
    return config
