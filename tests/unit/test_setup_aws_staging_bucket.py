"""Unit tests for the staging-bucket setup helpers.

The helpers compute STS service-agent identity, bucket-name conventions, and
IAM-policy bindings. Real GCS / IAM calls are mocked.
"""

from unittest.mock import MagicMock

from scripts.infrastructure.setup_aws_staging_bucket import (
    apply_iam_bindings,
    staging_bucket_name,
    sts_service_agent_email,
)


def test_sts_service_agent_email_format():
    """Per https://cloud.google.com/storage-transfer/docs/service-agents."""
    email = sts_service_agent_email(project_number="123456789012")
    assert email == (
        "project-123456789012@storage-transfer-service.iam.gserviceaccount.com"
    )


def test_staging_bucket_name_convention():
    assert staging_bucket_name("acme-prod") == "acme-prod-aws-logs-staging"


def test_apply_iam_bindings_grants_object_admin_to_sts_agent_and_object_viewer_to_pipeline_sa():
    bucket = MagicMock()
    policy = MagicMock()
    policy.bindings = []
    bucket.get_iam_policy.return_value = policy

    apply_iam_bindings(
        bucket=bucket,
        sts_service_agent_email="project-123@storage-transfer-service.iam.gserviceaccount.com",
        pipeline_sa_email="pipeline@my-proj.iam.gserviceaccount.com",
    )

    bucket.set_iam_policy.assert_called_once()
    set_policy = bucket.set_iam_policy.call_args[0][0]
    roles = {
        b.get("role") if isinstance(b, dict) else b.role for b in set_policy.bindings
    }
    assert "roles/storage.objectAdmin" in roles  # STS agent writes
    assert "roles/storage.objectViewer" in roles  # pipeline SA reads


def test_ensure_bucket_applies_30_day_delete_lifecycle():
    """D5: spec §1 locked-in 30-day GCS staging auto-delete."""
    from scripts.infrastructure.setup_aws_staging_bucket import (
        StagingBucketSpec,
        ensure_bucket,
    )

    bucket = MagicMock()
    bucket.lifecycle_rules = []
    client = MagicMock()
    client.get_bucket.return_value = bucket  # bucket already exists

    spec = StagingBucketSpec(
        project_id="p",
        project_number="123",
        pipeline_sa_email="sa@p.iam.gserviceaccount.com",
        client_name="acme",
    )
    ensure_bucket(client, spec)

    # Lifecycle was patched
    bucket.patch.assert_called_once()
    rules = bucket.lifecycle_rules
    assert len(rules) == 1
    assert rules[0]["action"]["type"] == "Delete"
    assert rules[0]["condition"]["age"] == 30
