"""Unit tests for the ingestion dispatch registry.

The registry maps DomainConfig.provider → pull function. CF is the default;
aws_s3 dispatches to pull_aws_logs_for_date_range. Unknown providers fail
loudly.
"""

import pytest

from llm_bot_pipeline.aws.s3_log_source import pull_aws_logs_for_date_range
from llm_bot_pipeline.cloudflare.logpull import pull_logs_for_date_range
from llm_bot_pipeline.config.settings import (
    AWSSourceConfig,
    DomainConfig,
)
from llm_bot_pipeline.pipeline.ingestion_dispatch import dispatch


def test_cloudflare_domain_returns_logpull_function():
    dc = DomainConfig(domain="x.com", zone_id="z", provider="cloudflare")
    assert dispatch(dc) is pull_logs_for_date_range


def test_aws_s3_domain_returns_aws_pull_function():
    dc = DomainConfig(
        domain="x.com",
        zone_id="",
        provider="aws_s3",
        aws=AWSSourceConfig(
            source_type="alb",
            s3_bucket="b",
            s3_prefixes=["p/"],
            gcs_staging_prefix="gs://x/",
            reader_role_arn="arn:aws:iam::111122223333:role/r",
        ),
    )
    assert dispatch(dc) is pull_aws_logs_for_date_range


def test_unknown_provider_raises():
    class _FakeConfig:
        provider = "myspace"
        domain = "x.com"

    with pytest.raises(KeyError, match="myspace"):
        dispatch(_FakeConfig())  # type: ignore[arg-type]
