"""
Application settings and configuration management.

Supports loading from:
1. SOPS-encrypted YAML files (config.enc.yaml)
2. Environment variables (fallback)
"""

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional


@dataclass
class Settings:
    """Application settings for SQLite backend."""

    # Storage Backend Settings
    storage_backend: str = "sqlite"
    sqlite_db_path: str = "data/llm-bot-logs.db"

    # Cloudflare Settings
    cloudflare_api_token: str = ""
    cloudflare_zone_id: str = ""

    def validate(self) -> list[str]:
        """Validate required settings are present. Returns list of errors."""
        errors = []

        if self.storage_backend != "sqlite":
            errors.append("Only SQLite backend is supported in this version")

        if not self.cloudflare_api_token:
            errors.append("cloudflare.api_token is required")

        if not self.cloudflare_zone_id:
            errors.append("cloudflare.zone_id is required")

        return errors

    @classmethod
    def from_dict(cls, config: dict[str, Any]) -> "Settings":
        """Create Settings from configuration dictionary (e.g., from SOPS)."""
        storage = config.get("storage", {})
        cf = config.get("cloudflare", {})

        return cls(
            storage_backend=storage.get("backend", "sqlite"),
            sqlite_db_path=storage.get("sqlite_db_path", "data/llm-bot-logs.db"),
            cloudflare_api_token=cf.get("api_token", ""),
            cloudflare_zone_id=cf.get("zone_id", ""),
        )

    @classmethod
    def from_env(cls) -> "Settings":
        """Create Settings from environment variables."""
        return cls(
            storage_backend="sqlite",
            sqlite_db_path=os.environ.get("SQLITE_DB_PATH", "data/llm-bot-logs.db"),
            cloudflare_api_token=os.environ.get("CLOUDFLARE_API_TOKEN", ""),
            cloudflare_zone_id=os.environ.get("CLOUDFLARE_ZONE_ID", ""),
        )


# Default config file path
DEFAULT_CONFIG_PATH = Path("config.enc.yaml")


@lru_cache
def get_settings(config_path: Optional[str] = None) -> Settings:
    """
    Get cached settings instance.

    Loads from SOPS-encrypted config file if available, otherwise from env vars.

    Args:
        config_path: Optional path to SOPS-encrypted config file

    Returns:
        Settings instance
    """
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

    if path.exists():
        try:
            from .sops_loader import decrypt_sops_file

            config = decrypt_sops_file(path)
            return Settings.from_dict(config)
        except Exception as e:
            print(f"⚠ Failed to load SOPS config from {path}: {e}")
            print("  Falling back to environment variables")

    return Settings.from_env()


def clear_settings_cache() -> None:
    """Clear the cached settings (useful for testing)."""
    get_settings.cache_clear()
