"""Configuration module."""

from .constants import (
    BOT_CLASSIFICATION,
    LLM_BOT_NAMES,
    OPTIMAL_WINDOW_MS,
    OUTPUT_FIELDS,
    WINDOW_50MS,
    WINDOW_100MS,
)
from .settings import Settings, clear_settings_cache, get_settings
from .sops_loader import check_sops_installed, decrypt_sops_file, load_config

__all__ = [
    # Session configuration
    "OPTIMAL_WINDOW_MS",
    "WINDOW_50MS",
    "WINDOW_100MS",
    # Bot classification
    "BOT_CLASSIFICATION",
    "LLM_BOT_NAMES",
    "OUTPUT_FIELDS",
    # Settings
    "Settings",
    "get_settings",
    "clear_settings_cache",
    # Config loading
    "load_config",
    "decrypt_sops_file",
    "check_sops_installed",
]
