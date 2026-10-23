"""Group daily-ingestion domains that can share one log scan."""

from __future__ import annotations

from collections.abc import Sequence

from ...config.settings import DomainConfig


def aws_scan_key(domain_config: DomainConfig) -> tuple[str, str] | None:
    """Return the shared-scan key for an AWS domain, or None.

    Domains with the same source type and GCS prefix read the same objects.
    Scanning that prefix once and splitting by host filter is equivalent to
    scanning it again for each domain.
    """
    aws = domain_config.aws
    if domain_config.provider != "aws_s3" or aws is None:
        return None
    return (aws.source_type, aws.gcs_staging_prefix.rstrip("/"))


def group_domains_for_ingestion(
    domains: Sequence[DomainConfig],
) -> list[list[DomainConfig]]:
    """Group AWS domains that read the same GCS prefix. Others stay alone.

    Groups are emitted in first-seen order. A later domain that shares a key
    joins the earlier group, so a non-AWS domain between them does not cause
    a second scan of the same objects.
    """
    groups: list[list[DomainConfig]] = []
    index_by_key: dict[tuple[str, str], int] = {}
    for domain_config in domains:
        key = aws_scan_key(domain_config)
        if key is None:
            groups.append([domain_config])
            continue
        existing = index_by_key.get(key)
        if existing is None:
            index_by_key[key] = len(groups)
            groups.append([domain_config])
        else:
            groups[existing].append(domain_config)
    return groups
