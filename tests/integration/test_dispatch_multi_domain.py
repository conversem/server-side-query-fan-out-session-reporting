"""Cross-provider dispatch: settings with both CF and AWS domains routes
each domain to its own pull function without cross-talk.

Pure-Python; both pull functions are monkey-patched so no GCS/CF API is hit.
"""

from datetime import date
from unittest.mock import patch

from llm_bot_pipeline.config.settings import (
    AWSSourceConfig,
    DomainConfig,
    Settings,
)
from llm_bot_pipeline.pipeline.ingestion_dispatch import dispatch


def _cf_dc() -> DomainConfig:
    return DomainConfig(
        domain="cf.example.com", zone_id="zone-cf", provider="cloudflare"
    )


def _aws_dc() -> DomainConfig:
    return DomainConfig(
        domain="aws.example.com",
        zone_id="",
        provider="aws_s3",
        aws=AWSSourceConfig(
            source_type="cloudfront",
            s3_bucket="b",
            s3_prefixes=["p/"],
            gcs_staging_prefix="gs://x/",
            reader_role_arn="arn:aws:iam::111122223333:role/r",
        ),
    )


def test_dispatch_routes_per_domain_with_no_cross_talk():
    settings = Settings(domains=[_cf_dc(), _aws_dc()])

    cf_calls: list[dict] = []
    aws_calls: list[dict] = []

    def fake_cf(**kwargs):
        cf_calls.append(kwargs)
        return iter([])

    def fake_aws(**kwargs):
        aws_calls.append(kwargs)
        return iter([])

    with patch(
        "llm_bot_pipeline.pipeline.ingestion_dispatch._REGISTRY",
        {"cloudflare": fake_cf, "aws_s3": fake_aws},
    ):
        for dc in settings.domains:
            pull_fn = dispatch(dc)
            list(
                pull_fn(
                    start_date=date(2026, 5, 21),
                    end_date=date(2026, 5, 21),
                    domain_config=dc,
                    settings=settings,
                )
            )

    assert len(cf_calls) == 1
    assert cf_calls[0]["domain_config"].domain == "cf.example.com"

    assert len(aws_calls) == 1
    assert aws_calls[0]["domain_config"].domain == "aws.example.com"
