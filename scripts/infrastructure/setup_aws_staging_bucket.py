"""Idempotent GCS staging-bucket setup for the AWS log-source path.

The bucket receives objects from GCP Storage Transfer Service (sourced from
S3). Two IAM grants are required:

  - storage.objectAdmin to the STS service agent so STS can write.
  - storage.objectViewer to the pipeline service account so the Cloud Run
    job can read.

The script is safe to re-run: bucket creation is exists_ok, IAM policy is
merged (existing bindings preserved).
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Optional

from google.api_core import exceptions as gexc
from google.cloud import storage

logger = logging.getLogger(__name__)


def sts_service_agent_email(project_number: str) -> str:
    """Return the canonical STS service-agent email for a GCP project."""
    return f"project-{project_number}@storage-transfer-service.iam.gserviceaccount.com"


def staging_bucket_name(client_name: str) -> str:
    """Convention: '<client>-aws-logs-staging'. 63-char-safe."""
    base = f"{client_name}-aws-logs-staging"
    if len(base) > 63:
        raise ValueError(
            f"client_name too long for staging-bucket convention: {client_name!r}"
        )
    return base


@dataclass
class StagingBucketSpec:
    project_id: str
    project_number: str
    pipeline_sa_email: str
    client_name: str
    location: str = "EU"
    storage_class: str = "STANDARD"
    lifecycle_age_days: int = 30  # D5: spec §1 locked-in 30-day staging window


def ensure_bucket(client: storage.Client, spec: StagingBucketSpec) -> storage.Bucket:
    """Create the bucket if missing AND ensure the 30-day lifecycle rule.

    Spec §1 locked-in: 30-day GCS staging auto-delete is the replay budget.
    The lifecycle is applied on every run (idempotent) so a hand-edit that
    removed it is healed automatically.
    """
    name = staging_bucket_name(spec.client_name)
    try:
        bucket = client.get_bucket(name)
        logger.info("Staging bucket already exists: %s", name)
    except gexc.NotFound:
        bucket = storage.Bucket(client=client, name=name)
        bucket.storage_class = spec.storage_class
        bucket.location = spec.location
        bucket = client.create_bucket(bucket)
        logger.info("Created staging bucket: %s (location=%s)", name, spec.location)

    desired_rule = {
        "action": {"type": "Delete"},
        "condition": {"age": spec.lifecycle_age_days},
    }
    current_rules = list(bucket.lifecycle_rules or [])
    if desired_rule not in [
        {"action": r.get("action"), "condition": r.get("condition")}
        for r in current_rules
    ]:
        bucket.lifecycle_rules = [desired_rule]
        bucket.patch()
        logger.info(
            "Applied %d-day delete lifecycle to %s", spec.lifecycle_age_days, name
        )

    return bucket


def apply_iam_bindings(
    bucket: storage.Bucket,
    sts_service_agent_email: str,
    pipeline_sa_email: str,
) -> None:
    """Merge required role bindings into the bucket's IAM policy."""
    policy = bucket.get_iam_policy(requested_policy_version=3)

    grants = [
        ("roles/storage.objectAdmin", f"serviceAccount:{sts_service_agent_email}"),
        ("roles/storage.objectViewer", f"serviceAccount:{pipeline_sa_email}"),
    ]

    for role, member in grants:
        binding = next((b for b in policy.bindings if b.get("role") == role), None)
        if binding is None:
            policy.bindings.append({"role": role, "members": {member}})
        else:
            members = set(binding.get("members", set()))
            members.add(member)
            binding["members"] = members

    bucket.set_iam_policy(policy)
    logger.info(
        "IAM bindings applied to %s: %s",
        bucket.name,
        [(r, m) for r, m in grants],
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--project-number", required=True)
    parser.add_argument("--pipeline-sa-email", required=True)
    parser.add_argument("--client-name", required=True)
    parser.add_argument("--location", default="EU")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    spec = StagingBucketSpec(
        project_id=args.project_id,
        project_number=args.project_number,
        pipeline_sa_email=args.pipeline_sa_email,
        client_name=args.client_name,
        location=args.location,
    )

    client = storage.Client(project=args.project_id)
    bucket = ensure_bucket(client, spec)
    apply_iam_bindings(
        bucket=bucket,
        sts_service_agent_email=sts_service_agent_email(spec.project_number),
        pipeline_sa_email=spec.pipeline_sa_email,
    )

    print(f"OK: staging bucket gs://{bucket.name} ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
