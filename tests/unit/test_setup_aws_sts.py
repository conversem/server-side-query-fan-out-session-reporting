"""Unit tests for STS transfer-job setup.

Real STS API calls are mocked. Tests verify the request shape — especially
that role_arn (G14: federated identity, not access keys) is set on
aws_s3_data_source.
"""

from unittest.mock import MagicMock, patch

import pytest

from scripts.infrastructure.setup_aws_sts import (
    TransferJobSpec,
    build_transfer_job_request,
    upsert_transfer_job,
)


@pytest.fixture(autouse=True)
def _freeze_time():
    """Override root conftest freeze; schedule test expects year >= 2026."""
    yield


def _spec() -> TransferJobSpec:
    return TransferJobSpec(
        project_id="my-proj",
        client_name="acme",
        aws_bucket="acme-aws-logs",
        aws_prefix="cloudfront/dist-123/",
        reader_role_arn="arn:aws:iam::111122223333:role/log-reader",
        gcs_staging_bucket="acme-aws-logs-staging",
        gcs_staging_prefix="cloudfront/dist-123/",
        schedule_hour_utc=3,
    )


def test_request_uses_role_arn_not_access_keys():
    """G14: federated identity must be the auth mode."""
    req = build_transfer_job_request(_spec())
    aws_src = req["transfer_job"]["transfer_spec"]["aws_s3_data_source"]
    assert aws_src["role_arn"] == "arn:aws:iam::111122223333:role/log-reader"
    assert "aws_access_key" not in aws_src


def test_request_has_daily_schedule_at_configured_hour():
    req = build_transfer_job_request(_spec())
    sched = req["transfer_job"]["schedule"]
    assert sched["schedule_start_date"]["year"] >= 2026
    assert sched["start_time_of_day"]["hours"] == 3  # D6: 03:00 UTC
    # Daily transfer: start_date == end_date is omitted; STS treats this as
    # "run every day starting at schedule_start_date" until disabled.
    assert "schedule_end_date" not in sched


def test_request_carries_overwrite_when_different_and_no_delete():
    req = build_transfer_job_request(_spec())
    opts = req["transfer_job"]["transfer_spec"]["transfer_options"]
    assert opts["overwrite_when"] == "DIFFERENT"
    assert opts.get("delete_objects_from_source_after_transfer") in (False, None)


def test_upsert_creates_when_no_existing_job():
    client = MagicMock()
    client.list_transfer_jobs.return_value = iter([])
    spec = _spec()
    upsert_transfer_job(client=client, spec=spec)
    client.create_transfer_job.assert_called_once()


def test_upsert_updates_when_job_exists():
    existing = MagicMock()
    existing.description = "aws-logs-acme-cloudfront-dist-123"
    client = MagicMock()
    client.list_transfer_jobs.return_value = iter([existing])
    spec = _spec()
    upsert_transfer_job(client=client, spec=spec)
    client.update_transfer_job.assert_called_once()
    client.create_transfer_job.assert_not_called()
