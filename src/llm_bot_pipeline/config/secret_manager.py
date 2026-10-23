"""
GCP Secret Manager integration for cloud-native config resolution.

Loads pipeline configuration from Secret Manager as an alternative to
SOPS-encrypted files (local development) or environment variables (CI/CD).

Usage:
    Set USE_SECRET_MANAGER=true and GCP_PROJECT_ID to enable.
    The config secret name defaults to "pipeline-config" but can be
    overridden via CONFIG_SECRET env var.
"""

import json
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)


def is_secret_manager_enabled() -> bool:
    """Check if Secret Manager config loading is enabled."""
    return os.environ.get("USE_SECRET_MANAGER", "").lower() in ("true", "1", "yes")


def load_from_secret_manager(
    project_id: Optional[str] = None,
    secret_name: Optional[str] = None,
    version: str = "latest",
) -> dict[str, Any]:
    """Load configuration from GCP Secret Manager.

    The secret should contain a JSON-encoded config dictionary matching
    the structure expected by Settings.from_dict().

    Args:
        project_id: GCP project ID. Defaults to GCP_PROJECT_ID env var.
        secret_name: Secret name. Defaults to CONFIG_SECRET env var or
            "pipeline-config".
        version: Secret version. Defaults to "latest".

    Returns:
        Configuration dictionary.

    Raises:
        ImportError: If google-cloud-secret-manager is not installed.
        ValueError: If project_id is not provided.
        RuntimeError: If secret access fails.
    """
    try:
        from google.api_core import exceptions as api_exceptions
        from google.cloud import secretmanager
    except ImportError:
        raise ImportError(
            "google-cloud-secret-manager is required for Secret Manager integration. "
            "Install with: pip install google-cloud-secret-manager"
        )

    if project_id is None:
        project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError(
            "GCP project ID is required. Set GCP_PROJECT_ID env var or pass project_id."
        )

    if secret_name is None:
        secret_name = os.environ.get("CONFIG_SECRET", "pipeline-config")

    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"

    try:
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": secret_path})
        payload = response.payload.data.decode("utf-8")
    except api_exceptions.GoogleAPIError as e:
        raise RuntimeError(
            f"Failed to access secret '{secret_name}' in project '{project_id}': {e}"
        ) from e
    except Exception as e:
        logger.error("Unexpected error accessing secret '%s': %s", secret_name, e)
        raise

    try:
        config = json.loads(payload)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Secret '{secret_name}' does not contain valid JSON: {e}"
        ) from e

    logger.debug("Loaded config from Secret Manager: %s/%s", project_id, secret_name)
    return config


DEFAULT_EU_LOCATIONS = ["europe-west1", "europe-west4"]


def create_secret(
    project_id: str,
    secret_name: str,
    config: dict[str, Any],
    locations: Optional[list[str]] = None,
) -> str:
    """Create or update a secret in Secret Manager.

    Uses user-managed replication restricted to EU regions by default
    for GDPR compliance.  Pass ``locations`` to override.

    Args:
        project_id: GCP project ID.
        secret_name: Name for the secret.
        config: Configuration dictionary to store.
        locations: Replication regions.  Defaults to ``DEFAULT_EU_LOCATIONS``.

    Returns:
        The full secret version resource name.
    """
    from google.api_core import exceptions as api_exceptions
    from google.cloud import secretmanager

    if locations is None:
        locations = DEFAULT_EU_LOCATIONS

    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}"
    secret_path = f"{parent}/secrets/{secret_name}"

    replication = {
        "user_managed": {
            "replicas": [{"location": loc} for loc in locations],
        }
    }

    try:
        client.get_secret(request={"name": secret_path})
    except api_exceptions.NotFound:
        logger.debug("Secret %s not found, creating it", secret_name)
        client.create_secret(
            request={
                "parent": parent,
                "secret_id": secret_name,
                "secret": {"replication": replication},
            }
        )
        logger.info("Created secret: %s (replicas: %s)", secret_name, locations)
    except Exception as e:
        logger.error("Unexpected error checking secret %s: %s", secret_name, e)
        raise

    payload = json.dumps(config, indent=2).encode("utf-8")
    version = client.add_secret_version(
        request={
            "parent": secret_path,
            "payload": {"data": payload},
        }
    )
    logger.info("Added secret version: %s", version.name)
    return version.name
