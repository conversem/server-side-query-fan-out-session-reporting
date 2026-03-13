"""
Application settings and configuration management.

Supports loading from:
1. SOPS-encrypted YAML files (config.enc.yaml)
2. Environment variables (fallback)
"""

import logging
import os
from dataclasses import dataclass, field, fields
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Raised when Settings validation fails.

    Collects all validation errors and reports them together.
    """

    def __init__(self, errors: list[str]):
        self.errors = errors
        msg = "Configuration validation failed:\n" + "\n".join(
            f"  - {e}" for e in errors
        )
        super().__init__(msg)


from .constants import (
    BQ_PROCESSING_MODES,
    DATASET_RAW,
    DATASET_REPORT,
    DATASET_STAGING,
    KEY_FILE_NAME,
    SERVICE_ACCOUNT_ID,
    SQLITE_PROCESSING_MODES,
    VALID_PROCESSING_MODES,
)

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

    # Splitting strategy: 'mibcs_only', 'network_only', 'network_then_mibcs', 'mibcs_then_network'
    splitting_strategy: Optional[str] = "mibcs_only"

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
            splitting_strategy=config.get("splitting_strategy", "mibcs_only"),
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
            splitting_strategy=os.environ.get(
                "SESSION_SPLITTING_STRATEGY", "mibcs_only"
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
# Multi-Domain Configuration
# =============================================================================


@dataclass
class DomainConfig:
    """Configuration for a single domain in multi-domain setups.

    Each domain maps to one Cloudflare zone. The ``domain`` label is injected
    into every ingested record so that shared BigQuery datasets can be filtered
    per domain.
    """

    domain: str
    zone_id: str
    cf_token: str = ""
    sitemaps: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DomainConfig":
        return cls(
            domain=d["domain"],
            zone_id=d["zone_id"],
            cf_token=d.get("cf_token", ""),
            sitemaps=d.get("sitemaps", []),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "zone_id": self.zone_id,
            "cf_token": self.cf_token,
            "sitemaps": self.sitemaps,
        }


# =============================================================================
# Main Settings
# =============================================================================


VALID_BACKENDS = ("sqlite", "bigquery")
VALID_METRICS_BACKENDS = ("prometheus", "cloud_monitoring")


@dataclass
class Settings:
    """Application settings supporting multiple storage backends and processing modes."""

    # Pipeline Processing Mode
    processing_mode: str = "local_sqlite"

    # Storage Backend Settings
    storage_backend: str = "sqlite"
    sqlite_db_path: str = "data/llm-bot-logs.db"

    # GCP Settings
    gcp_project_id: str = ""
    gcp_location: str = "EU"
    service_account_key_path: Path = field(default_factory=lambda: Path(KEY_FILE_NAME))

    # BigQuery Settings (dual-dataset architecture)
    dataset_raw: str = DATASET_RAW
    dataset_staging: str = DATASET_STAGING
    dataset_report: str = DATASET_REPORT

    # Cloudflare Settings (single-domain)
    cloudflare_api_token: str = ""
    cloudflare_zone_id: str = ""

    # Multi-domain settings (takes precedence over single cloudflare_* when non-empty)
    domains: list[DomainConfig] = field(default_factory=list)

    # Service Account
    service_account_id: str = SERVICE_ACCOUNT_ID

    # Logpush Settings
    logpush_job_name: str = "llm-bot-logpush"
    logpush_enabled: bool = True

    # Sitemap Settings
    sitemap_urls: list[str] = field(default_factory=list)
    sitemap_enabled: bool = True
    sitemap_cache_in_backend: bool = True

    # Dead-letter path for failed streaming batches (JSON lines format)
    dead_letter_path: str = "data/dead_letter.jsonl"

    # Checkpoint path for local_bq_buffered resume (JSON file)
    checkpoint_path: str = "data/checkpoint.json"

    # Structured JSON logging for cloud environments (JSON_LOGS env, --json-logs CLI)
    json_logs: bool = False

    # Minimum free disk space (MB) required before SQLite write operations
    disk_space_threshold_mb: int = 500

    # Vacuum SQLite after bulk deletes exceed this row count (0 = disabled)
    vacuum_threshold: int = 10_000

    # Metrics export (Cloud Monitoring or Prometheus push gateway)
    metrics_export_enabled: bool = False
    metrics_backend: str = "prometheus"  # "prometheus" | "cloud_monitoring"
    metrics_pushgateway_url: str = "http://localhost:9091"

    # Session Refinement (collision detection and splitting)
    session_refinement: SessionRefinementSettings = field(
        default_factory=SessionRefinementSettings
    )

    def validate(self) -> list[str]:
        """Validate required settings are present. Returns list of errors."""
        errors = []

        if self.processing_mode not in VALID_PROCESSING_MODES:
            errors.append(
                f"processing_mode must be one of {VALID_PROCESSING_MODES}, "
                f"got '{self.processing_mode}'"
            )

        if self.storage_backend not in VALID_BACKENDS:
            errors.append(
                f"storage_backend must be one of {VALID_BACKENDS}, "
                f"got '{self.storage_backend}'"
            )

        if self.processing_mode in BQ_PROCESSING_MODES and not self.gcp_project_id:
            errors.append(
                f"gcp.project_id is required for processing_mode='{self.processing_mode}'"
            )

        if self.storage_backend == "bigquery" and not self.gcp_project_id:
            errors.append("gcp.project_id is required when using BigQuery backend")

        if self.metrics_backend not in VALID_METRICS_BACKENDS:
            errors.append(
                f"metrics_backend must be one of {VALID_METRICS_BACKENDS}, "
                f"got '{self.metrics_backend}'"
            )

        if (
            self.metrics_export_enabled
            and self.metrics_backend == "cloud_monitoring"
            and not self.gcp_project_id
        ):
            errors.append(
                "gcp.project_id is required when metrics_backend is cloud_monitoring"
            )

        errors.extend(self.session_refinement.validate())

        return errors

    def __repr__(self) -> str:
        """Repr with sensitive fields redacted (tokens, secrets, keys, etc.)."""
        REDACTED = "***REDACTED***"
        SENSITIVE_PATTERNS = ("token", "secret", "key", "password", "credential")
        EXPLICIT_SENSITIVE = frozenset(
            ("cloudflare_api_token", "service_account_key_path")
        )

        def is_sensitive(name: str) -> bool:
            lower = name.lower()
            return (
                any(p in lower for p in SENSITIVE_PATTERNS)
                or name in EXPLICIT_SENSITIVE
            )

        parts = []
        for f in fields(self):
            val = getattr(self, f.name)
            if is_sensitive(f.name):
                parts.append(f"{f.name}='{REDACTED}'")
            else:
                parts.append(f"{f.name}={val!r}")
        return f"Settings({', '.join(parts)})"

    @property
    def needs_sqlite(self) -> bool:
        """Whether the current processing mode requires SQLite."""
        return self.processing_mode in SQLITE_PROCESSING_MODES

    @property
    def needs_bigquery(self) -> bool:
        """Whether the current processing mode outputs to BigQuery."""
        return self.processing_mode in BQ_PROCESSING_MODES

    @property
    def raw_table_id(self) -> str:
        """Full BigQuery table ID for raw data."""
        return f"{self.gcp_project_id}.{self.dataset_raw}.bot_requests"

    @property
    def clean_table_id(self) -> str:
        """Full BigQuery table ID for clean data."""
        return f"{self.gcp_project_id}.{self.dataset_report}.bot_requests_daily"

    @property
    def service_account_email(self) -> str:
        """Full service account email."""
        return (
            f"{self.service_account_id}@{self.gcp_project_id}.iam.gserviceaccount.com"
        )

    def backend_kwargs(self) -> dict:
        """Get constructor kwargs for the configured storage backend."""
        if self.storage_backend == "sqlite":
            return {
                "db_path": Path(self.sqlite_db_path),
                "disk_space_threshold_mb": self.disk_space_threshold_mb,
                "vacuum_threshold": self.vacuum_threshold,
            }
        elif self.storage_backend == "bigquery":
            creds = str(self.service_account_key_path)
            return {
                "project_id": self.gcp_project_id,
                "credentials_path": (
                    creds if self.service_account_key_path.exists() else None
                ),
                "dataset_raw": self.dataset_raw,
                "dataset_report": self.dataset_report,
                "location": self.gcp_location,
            }
        return {}

    @classmethod
    def from_dict(cls, config: dict[str, Any]) -> "Settings":
        """Create Settings from configuration dictionary (e.g., from SOPS)."""
        pipeline = config.get("pipeline", {})
        storage = config.get("storage", {})
        gcp = config.get("gcp", {})
        bq = config.get("bigquery", {})
        cf = config.get("cloudflare", {})
        sr = config.get("session_refinement", {})
        sm = config.get("sitemap", {})
        metrics = config.get("metrics", {})

        domains_raw = config.get("domains", [])
        domains = (
            [DomainConfig.from_dict(d) for d in domains_raw] if domains_raw else []
        )

        sitemap_urls = sm.get("urls", [])
        if not sitemap_urls and domains:
            sitemap_urls = [url for d in domains for url in d.sitemaps]

        return cls(
            processing_mode=pipeline.get("processing_mode", "local_sqlite"),
            storage_backend=storage.get("backend", "sqlite"),
            sqlite_db_path=storage.get("sqlite_db_path", "data/llm-bot-logs.db"),
            gcp_project_id=gcp.get("project_id", ""),
            gcp_location=gcp.get("location", "EU"),
            service_account_key_path=Path(
                gcp.get("service_account_key_path", KEY_FILE_NAME)
            ),
            dataset_raw=bq.get("dataset_raw", DATASET_RAW),
            dataset_staging=bq.get("dataset_staging", DATASET_STAGING),
            dataset_report=bq.get("dataset_report", DATASET_REPORT),
            cloudflare_api_token=cf.get("api_token", ""),
            cloudflare_zone_id=cf.get("zone_id", ""),
            domains=domains,
            logpush_job_name=cf.get("logpush_job_name", "llm-bot-logpush"),
            logpush_enabled=cf.get("logpush_enabled", True),
            sitemap_urls=sitemap_urls,
            sitemap_enabled=sm.get("enabled", True),
            sitemap_cache_in_backend=sm.get("cache_in_backend", True),
            disk_space_threshold_mb=storage.get("disk_space_threshold_mb", 500),
            vacuum_threshold=storage.get("vacuum_threshold", 10_000),
            checkpoint_path=storage.get("checkpoint_path", "data/checkpoint.json"),
            json_logs=pipeline.get("json_logs", False),
            metrics_export_enabled=metrics.get("export_enabled", False),
            metrics_backend=metrics.get("backend", "prometheus"),
            metrics_pushgateway_url=metrics.get(
                "pushgateway_url", "http://localhost:9091"
            ),
            session_refinement=SessionRefinementSettings.from_dict(sr),
        )

    @classmethod
    def from_env(cls) -> "Settings":
        """Create Settings from environment variables."""
        return cls(
            processing_mode=os.environ.get("PROCESSING_MODE", "local_sqlite"),
            storage_backend=os.environ.get("STORAGE_BACKEND", "sqlite"),
            sqlite_db_path=os.environ.get("SQLITE_DB_PATH", "data/llm-bot-logs.db"),
            gcp_project_id=os.environ.get("GCP_PROJECT_ID", ""),
            gcp_location=os.environ.get("GCP_LOCATION", "EU"),
            service_account_key_path=Path(
                os.environ.get("GCP_SERVICE_ACCOUNT_KEY", KEY_FILE_NAME)
            ),
            dataset_raw=os.environ.get("BQ_DATASET_RAW", DATASET_RAW),
            dataset_staging=os.environ.get("BQ_DATASET_STAGING", DATASET_STAGING),
            dataset_report=os.environ.get("BQ_DATASET_REPORT", DATASET_REPORT),
            cloudflare_api_token=os.environ.get("CLOUDFLARE_API_TOKEN", ""),
            cloudflare_zone_id=os.environ.get("CLOUDFLARE_ZONE_ID", ""),
            logpush_job_name=os.environ.get("LOGPUSH_JOB_NAME", "llm-bot-logpush"),
            logpush_enabled=os.environ.get("LOGPUSH_ENABLED", "true").lower() == "true",
            sitemap_urls=[
                u.strip()
                for u in os.environ.get("SITEMAP_URLS", "").split(",")
                if u.strip()
            ],
            sitemap_enabled=os.environ.get("SITEMAP_ENABLED", "true").lower() == "true",
            sitemap_cache_in_backend=os.environ.get(
                "SITEMAP_CACHE_IN_BACKEND", "true"
            ).lower()
            == "true",
            disk_space_threshold_mb=int(
                os.environ.get("DISK_SPACE_THRESHOLD_MB", "500")
            ),
            vacuum_threshold=int(os.environ.get("SQLITE_VACUUM_THRESHOLD", "10000")),
            checkpoint_path=os.environ.get("CHECKPOINT_PATH", "data/checkpoint.json"),
            json_logs=os.environ.get("JSON_LOGS", "").lower() in ("true", "1", "yes"),
            metrics_export_enabled=os.environ.get(
                "METRICS_EXPORT_ENABLED", "false"
            ).lower()
            in ("true", "1", "yes"),
            metrics_backend=os.environ.get("METRICS_BACKEND", "prometheus"),
            metrics_pushgateway_url=os.environ.get(
                "METRICS_PUSHGATEWAY_URL", "http://localhost:9091"
            ),
            session_refinement=SessionRefinementSettings.from_env(),
        )


# Default config file path
DEFAULT_CONFIG_PATH = Path("config.enc.yaml")


@lru_cache
def get_settings(config_path: Optional[str] = None) -> Settings:
    """Get cached settings instance.

    Resolution order:
        1. GCP Secret Manager (when USE_SECRET_MANAGER is set)
        2. SOPS-encrypted YAML file (local development)
        3. Environment variables (CI/CD, containers)

    Args:
        config_path: Optional path to SOPS-encrypted config file.

    Returns:
        Settings instance.
    """
    from .secret_manager import is_secret_manager_enabled, load_from_secret_manager

    if is_secret_manager_enabled():
        try:
            config = load_from_secret_manager()
            settings = Settings.from_dict(config)
        except Exception as e:
            logger.error("Failed to load from Secret Manager: %s", e)
            raise RuntimeError(
                f"USE_SECRET_MANAGER is enabled but Secret Manager failed: {e}"
            ) from e
    else:
        path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

        if path.exists():
            try:
                from .sops_loader import decrypt_sops_file

                config = decrypt_sops_file(path)
                settings = Settings.from_dict(config)
            except Exception as e:
                logger.warning("Failed to load SOPS config from %s: %s", path, e)
                logger.warning("Falling back to environment variables")
                settings = Settings.from_env()
        else:
            settings = Settings.from_env()

    errors = settings.validate()
    if errors:
        raise ConfigurationError(errors)

    return settings


def clear_settings_cache() -> None:
    """Clear the cached settings (useful for testing)."""
    get_settings.cache_clear()
