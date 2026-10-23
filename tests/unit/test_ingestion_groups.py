"""Domain grouping for a single shared AWS log scan."""

from llm_bot_pipeline.config.settings import AWSSourceConfig, DomainConfig
from llm_bot_pipeline.pipeline.daily.ingestion_groups import group_domains_for_ingestion


def _aws(domain: str, prefix: str) -> DomainConfig:
    return DomainConfig(
        domain=domain,
        zone_id="",
        provider="aws_s3",
        aws=AWSSourceConfig(
            source_type="alb",
            s3_bucket="logs",
            s3_prefixes=["alb/"],
            gcs_staging_prefix=prefix,
            reader_role_arn="arn:aws:iam::111122223333:role/reader",
        ),
    )


def test_shared_prefix_groups_together_around_other_providers():
    shared = "gs://staging/alb/example/"
    domains = [
        _aws("example.com", shared),
        _aws("energielabel.nl", shared),
        DomainConfig(domain="verbeterjehuis.nl", zone_id="z", provider="cloudflare"),
        _aws("duurzaamleven.nl", shared + "/"),
        _aws("energiehulpnetwerk.nl", shared),
        _aws("other.example", "gs://staging/alb/other/"),
    ]

    groups = group_domains_for_ingestion(domains)

    assert [[domain.domain for domain in group] for group in groups] == [
        [
            "example.com",
            "energielabel.nl",
            "duurzaamleven.nl",
            "energiehulpnetwerk.nl",
        ],
        ["verbeterjehuis.nl"],
        ["other.example"],
    ]
