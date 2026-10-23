"""
GCP Service Account management.

Note: This module uses gcloud CLI for service account operations.
Ensure gcloud is installed and authenticated.
"""

import base64
import json
import logging
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class ServiceAccountManager:
    """Manages GCP service account creation and configuration."""

    def __init__(self, project_id: str):
        """
        Initialize service account manager.

        Args:
            project_id: GCP project ID
        """
        self.project_id = project_id

    def _run_gcloud(
        self, args: list[str], check: bool = True
    ) -> subprocess.CompletedProcess:
        """Run a gcloud command."""
        cmd = ["gcloud"] + args + ["--project", self.project_id]
        return subprocess.run(cmd, capture_output=True, text=True, check=check)

    def service_account_exists(self, service_account_id: str) -> bool:
        """Check if a service account exists."""
        email = f"{service_account_id}@{self.project_id}.iam.gserviceaccount.com"
        result = self._run_gcloud(
            ["iam", "service-accounts", "describe", email],
            check=False,
        )
        return result.returncode == 0

    def create_service_account(
        self,
        service_account_id: str,
        display_name: str,
        description: Optional[str] = None,
    ) -> str:
        """
        Create a service account.

        Args:
            service_account_id: Service account ID (without @project...)
            display_name: Display name
            description: Optional description

        Returns:
            Service account email
        """
        email = f"{service_account_id}@{self.project_id}.iam.gserviceaccount.com"

        if self.service_account_exists(service_account_id):
            logger.warning("⚠ Service account already exists: %s", email)
            return email

        args = [
            "iam",
            "service-accounts",
            "create",
            service_account_id,
            "--display-name",
            display_name,
        ]

        if description:
            args.extend(["--description", description])

        try:
            self._run_gcloud(args)
            logger.info("✓ Created service account: %s", email)
            return email
        except subprocess.CalledProcessError as e:
            logger.warning("✗ Failed to create service account: %s", e.stderr)
            raise

    def grant_role(self, service_account_email: str, role: str) -> bool:
        """
        Grant an IAM role to a service account.

        Args:
            service_account_email: Service account email
            role: IAM role (e.g., 'roles/bigquery.dataEditor')

        Returns:
            True if successful
        """
        member = f"serviceAccount:{service_account_email}"

        try:
            self._run_gcloud(
                [
                    "projects",
                    "add-iam-policy-binding",
                    self.project_id,
                    "--member",
                    member,
                    "--role",
                    role,
                ]
            )
            logger.info("✓ Granted role %s to %s", role, service_account_email)
            return True
        except subprocess.CalledProcessError as e:
            logger.warning("✗ Failed to grant role %s: %s", role, e.stderr)
            return False

    def grant_bigquery_roles(self, service_account_email: str) -> bool:
        """
        Grant standard BigQuery roles for Logpush ingestion.

        Args:
            service_account_email: Service account email

        Returns:
            True if all roles granted successfully
        """
        roles = [
            "roles/bigquery.dataEditor",
            "roles/bigquery.jobUser",
        ]

        success = True
        for role in roles:
            if not self.grant_role(service_account_email, role):
                success = False

        return success

    def grant_aws_staging_access(
        self,
        sa_email: str,
        staging_bucket: str,
    ) -> None:
        """Grant the pipeline SA AWS-path-specific IAM permissions.

        - storage.objectViewer on the staging bucket (read objects STS deposits).
        - storagetransfer.user on the project (list/trigger transfer jobs).
        """
        self._run_gcloud(
            [
                "storage",
                "buckets",
                "add-iam-policy-binding",
                f"gs://{staging_bucket}",
                "--member",
                f"serviceAccount:{sa_email}",
                "--role",
                "roles/storage.objectViewer",
            ]
        )
        self._run_gcloud(
            [
                "projects",
                "add-iam-policy-binding",
                self.project_id,
                "--member",
                f"serviceAccount:{sa_email}",
                "--role",
                "roles/storagetransfer.user",
            ]
        )
        logger.info(
            "Granted AWS-staging access to %s (bucket=%s)", sa_email, staging_bucket
        )

    def create_key(
        self, service_account_email: str, key_path: Path, overwrite: bool = False
    ) -> bool:
        """
        Create a JSON key file for a service account.

        Args:
            service_account_email: Service account email
            key_path: Path to save the key file
            overwrite: Whether to overwrite existing key

        Returns:
            True if successful
        """
        if key_path.exists() and not overwrite:
            logger.warning("⚠ Key file already exists: %s", key_path)
            return True

        try:
            self._run_gcloud(
                [
                    "iam",
                    "service-accounts",
                    "keys",
                    "create",
                    str(key_path),
                    "--iam-account",
                    service_account_email,
                ]
            )
            logger.info("✓ Created service account key: %s", key_path)
            return True
        except subprocess.CalledProcessError as e:
            logger.warning("✗ Failed to create key: %s", e.stderr)
            return False

    def get_key_base64(self, key_path: Path) -> str:
        """
        Read a key file and return base64 encoded content.

        This is needed for Cloudflare Logpush BigQuery destination.

        Args:
            key_path: Path to the key file

        Returns:
            Base64 encoded key content
        """
        with open(key_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")


def check_gcloud_installed() -> bool:
    """Check if gcloud CLI is installed and accessible."""
    try:
        result = subprocess.run(
            ["gcloud", "--version"],
            capture_output=True,
            text=True,
            check=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def check_gcloud_authenticated() -> Optional[str]:
    """
    Check if gcloud is authenticated.

    Returns:
        Current account email if authenticated, None otherwise
    """
    try:
        result = subprocess.run(
            [
                "gcloud",
                "auth",
                "list",
                "--filter=status:ACTIVE",
                "--format=value(account)",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        account = result.stdout.strip()
        return account if account else None
    except subprocess.CalledProcessError:
        return None
