"""
Application settings and configuration management.

Supports loading from:
1. SOPS-encrypted YAML files (config.enc.yaml)
2. Environment variables (fallback)
"""

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional


# =============================================================================
# Session Refinement Settings
# =============================================================================


@dataclass
class SessionRefinementSettings:
    """
    Configuration for session refinement and collision detection.

    Controls the behavior of semantic splitting for detecting and resolving
    collision bundles (where multiple independent queries were accidentally
    merged due to temporal proximity).

    Research finding: IP-based refinement is OFF by default because IP diversity
    does not discriminate between clean bundles and collisions (r=0.023).
    See docs/research/ip-fingerprint-analysis-report.md for details.
    """

    # Feature flags
    # Semantic refinement ON by default (uses MIBCS for collision detection)
    # IP-based refinement OFF by default (research shows IP does not discriminate collisions)
    enabled: bool = True
    enable_semantic_refinement: bool = True
    enable_ip_based_refinement: bool = False  # Research: r=0.023, not useful

    # Collision detection thresholds
    collision_ip_threshold: int = 2
    collision_homogeneity_threshold: float = 0.5

    # Semantic splitting parameters
    similarity_threshold: float = 0.5
    min_sub_bundle_size: int = 2
    min_mibcs_improvement: float = 0.05

    # Logging and metrics
    log_refinement_details: bool = True
    collect_refinement_metrics: bool = True

    def validate(self) -> list[str]:
        """Validate settings values. Returns list of errors."""
        errors = []

        if not 0.0 <= self.collision_homogeneity_threshold <= 1.0:
            errors.append(
                f"collision_homogeneity_threshold must be 0-1, "
                f"got {self.collision_homogeneity_threshold}"
            )
        if not 0.0 <= self.similarity_threshold <= 1.0:
            errors.append(
                f"similarity_threshold must be 0-1, got {self.similarity_threshold}"
            )
        if self.min_sub_bundle_size < 1:
            errors.append(
                f"min_sub_bundle_size must be >= 1, got {self.min_sub_bundle_size}"
            )
        if self.collision_ip_threshold < 1:
            errors.append(
                f"collision_ip_threshold must be >= 1, got {self.collision_ip_threshold}"
            )
        if not 0.0 <= self.min_mibcs_improvement <= 1.0:
            errors.append(
                f"min_mibcs_improvement must be 0-1, got {self.min_mibcs_improvement}"
            )

        return errors

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "enabled": self.enabled,
            "enable_semantic_refinement": self.enable_semantic_refinement,
            "enable_ip_based_refinement": self.enable_ip_based_refinement,
            "collision_ip_threshold": self.collision_ip_threshold,
            "collision_homogeneity_threshold": self.collision_homogeneity_threshold,
            "similarity_threshold": self.similarity_threshold,
            "min_sub_bundle_size": self.min_sub_bundle_size,
            "min_mibcs_improvement": self.min_mibcs_improvement,
            "log_refinement_details": self.log_refinement_details,
            "collect_refinement_metrics": self.collect_refinement_metrics,
        }

    @classmethod
    def from_dict(cls, config: dict[str, Any]) -> "SessionRefinementSettings":
        """Create from configuration dictionary."""
        return cls(
            enabled=config.get("enabled", True),
            enable_semantic_refinement=config.get("enable_semantic_refinement", True),
            enable_ip_based_refinement=config.get("enable_ip_based_refinement", False),
            collision_ip_threshold=config.get("collision_ip_threshold", 2),
            collision_homogeneity_threshold=config.get(
                "collision_homogeneity_threshold", 0.5
            ),
            similarity_threshold=config.get("similarity_threshold", 0.5),
            min_sub_bundle_size=config.get("min_sub_bundle_size", 2),
            min_mibcs_improvement=config.get("min_mibcs_improvement", 0.05),
            log_refinement_details=config.get("log_refinement_details", True),
            collect_refinement_metrics=config.get("collect_refinement_metrics", True),
        )

    @classmethod
    def from_env(cls) -> "SessionRefinementSettings":
        """Create from environment variables."""

        def safe_int(key: str, default: int) -> int:
            """Safely parse int from env var, using default on error."""
            try:
                return int(os.environ.get(key, str(default)))
            except ValueError:
                return default

        def safe_float(key: str, default: float) -> float:
            """Safely parse float from env var, using default on error."""
            try:
                return float(os.environ.get(key, str(default)))
            except ValueError:
                return default

        def safe_bool(key: str, default: bool) -> bool:
            """Safely parse bool from env var."""
            return os.environ.get(key, str(default).lower()).lower() == "true"

        return cls(
            enabled=safe_bool("SESSION_REFINEMENT_ENABLED", True),
            enable_semantic_refinement=safe_bool("SESSION_REFINEMENT_SEMANTIC", True),
            enable_ip_based_refinement=safe_bool("SESSION_REFINEMENT_IP_BASED", False),
            collision_ip_threshold=safe_int(
                "SESSION_REFINEMENT_COLLISION_IP_THRESHOLD", 2
            ),
            collision_homogeneity_threshold=safe_float(
                "SESSION_REFINEMENT_HOMOGENEITY_THRESHOLD", 0.5
            ),
            similarity_threshold=safe_float(
                "SESSION_REFINEMENT_SIMILARITY_THRESHOLD", 0.5
            ),
            min_sub_bundle_size=safe_int("SESSION_REFINEMENT_MIN_BUNDLE_SIZE", 2),
            min_mibcs_improvement=safe_float(
                "SESSION_REFINEMENT_MIN_MIBCS_IMPROVEMENT", 0.05
            ),
            log_refinement_details=safe_bool("SESSION_REFINEMENT_LOG_DETAILS", True),
            collect_refinement_metrics=safe_bool(
                "SESSION_REFINEMENT_COLLECT_METRICS", True
            ),
        )


# =============================================================================
# Main Settings
# =============================================================================


@dataclass
class Settings:
    """Application settings for SQLite backend."""

    # Storage Backend Settings
    storage_backend: str = "sqlite"
    sqlite_db_path: str = "data/llm-bot-logs.db"

    # Cloudflare Settings
    cloudflare_api_token: str = ""
    cloudflare_zone_id: str = ""

    # Session Refinement (collision detection and splitting)
    session_refinement: SessionRefinementSettings = field(
        default_factory=SessionRefinementSettings
    )

    def validate(self) -> list[str]:
        """Validate required settings are present. Returns list of errors."""
        errors = []

        if self.storage_backend != "sqlite":
            errors.append("Only SQLite backend is supported in this version")

        if not self.cloudflare_api_token:
            errors.append("cloudflare.api_token is required")

        if not self.cloudflare_zone_id:
            errors.append("cloudflare.zone_id is required")

        # Validate nested settings
        errors.extend(self.session_refinement.validate())

        return errors

    @classmethod
    def from_dict(cls, config: dict[str, Any]) -> "Settings":
        """Create Settings from configuration dictionary (e.g., from SOPS)."""
        storage = config.get("storage", {})
        cf = config.get("cloudflare", {})
        sr = config.get("session_refinement", {})

        return cls(
            storage_backend=storage.get("backend", "sqlite"),
            sqlite_db_path=storage.get("sqlite_db_path", "data/llm-bot-logs.db"),
            cloudflare_api_token=cf.get("api_token", ""),
            cloudflare_zone_id=cf.get("zone_id", ""),
            session_refinement=SessionRefinementSettings.from_dict(sr),
        )

    @classmethod
    def from_env(cls) -> "Settings":
        """Create Settings from environment variables."""
        return cls(
            storage_backend="sqlite",
            sqlite_db_path=os.environ.get("SQLITE_DB_PATH", "data/llm-bot-logs.db"),
            cloudflare_api_token=os.environ.get("CLOUDFLARE_API_TOKEN", ""),
            cloudflare_zone_id=os.environ.get("CLOUDFLARE_ZONE_ID", ""),
            session_refinement=SessionRefinementSettings.from_env(),
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
