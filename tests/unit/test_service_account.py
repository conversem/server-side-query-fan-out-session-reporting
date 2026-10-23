"""Unit tests for Service Account management module."""

from unittest.mock import MagicMock, patch

from llm_bot_pipeline.infrastructure.service_account import ServiceAccountManager


class TestServiceAccountCreate:
    """Verify ServiceAccountManager creates service account and grants roles."""

    def test_create_service_account_returns_email(self):
        """Mock gcloud, assert create returns correct service account email."""
        manager = ServiceAccountManager(project_id="test-proj")

        with patch.object(manager, "_run_gcloud") as mock_run:
            # First call (service_account_exists describe) returns non-zero
            # Second call (create) succeeds
            mock_run.side_effect = [
                MagicMock(returncode=1),  # describe fails = doesn't exist
                MagicMock(returncode=0),  # create succeeds
            ]

            email = manager.create_service_account(
                service_account_id="llm-bot-pipeline",
                display_name="LLM Bot Pipeline",
            )

        assert email == "llm-bot-pipeline@test-proj.iam.gserviceaccount.com"
        assert mock_run.call_count == 2

    def test_grant_bigquery_roles_grants_correct_roles(self):
        """Mock IAM/gcloud, assert correct roles (dataEditor, jobUser) granted."""
        manager = ServiceAccountManager(project_id="test-proj")
        sa_email = "sa@test-proj.iam.gserviceaccount.com"

        with patch.object(manager, "grant_role") as mock_grant:
            mock_grant.return_value = True

            result = manager.grant_bigquery_roles(sa_email)

        assert result is True
        assert mock_grant.call_count == 2
        granted_roles = [c[0][1] for c in mock_grant.call_args_list]
        assert "roles/bigquery.dataEditor" in granted_roles
        assert "roles/bigquery.jobUser" in granted_roles

    def test_grant_role_calls_gcloud_with_correct_args(self):
        """Verify grant_role invokes gcloud add-iam-policy-binding correctly."""
        manager = ServiceAccountManager(project_id="my-project")
        sa_email = "sa@my-project.iam.gserviceaccount.com"
        role = "roles/bigquery.dataEditor"

        with patch.object(manager, "_run_gcloud") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)

            manager.grant_role(sa_email, role)

        mock_run.assert_called_once()
        args_list = mock_run.call_args[0][0]
        assert "projects" in args_list
        assert "add-iam-policy-binding" in args_list
        assert "my-project" in args_list
        assert "--member" in args_list
        assert f"serviceAccount:{sa_email}" in args_list
        assert "--role" in args_list
        assert role in args_list


class TestAWSStagingGrants:
    def test_grant_aws_staging_access(self):
        manager = ServiceAccountManager(project_id="my-proj")

        with patch.object(manager, "_run_gcloud") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            manager.grant_aws_staging_access(
                sa_email="pipeline@my-proj.iam.gserviceaccount.com",
                staging_bucket="acme-aws-logs-staging",
            )

        assert mock_run.call_count == 2
        bucket_args = mock_run.call_args_list[0].args[0]
        project_args = mock_run.call_args_list[1].args[0]
        assert bucket_args[:3] == ["storage", "buckets", "add-iam-policy-binding"]
        assert bucket_args[3] == "gs://acme-aws-logs-staging"
        assert "serviceAccount:pipeline@my-proj.iam.gserviceaccount.com" in bucket_args
        assert "roles/storage.objectViewer" in bucket_args
        assert project_args[:2] == ["projects", "add-iam-policy-binding"]
        assert project_args[2] == "my-proj"
        assert "roles/storagetransfer.user" in project_args
