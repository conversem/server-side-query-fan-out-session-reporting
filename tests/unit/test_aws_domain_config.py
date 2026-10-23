"""Unit tests for DomainConfig with the new provider field + AWSSourceConfig."""

import pytest

from llm_bot_pipeline.config.settings import AWSSourceConfig, DomainConfig


class TestAWSSourceConfig:
    def test_minimal_cloudfront(self):
        cfg = AWSSourceConfig(
            source_type="cloudfront",
            s3_bucket="client-logs",
            s3_prefixes=["cloudfront/dist-123/"],
            gcs_staging_prefix="gs://staging/client-a/cloudfront/",
            reader_role_arn="arn:aws:iam::111122223333:role/log-reader",
        )
        assert cfg.source_type == "cloudfront"
        assert cfg.s3_prefixes == ["cloudfront/dist-123/"]

    def test_minimal_alb(self):
        cfg = AWSSourceConfig(
            source_type="alb",
            s3_bucket="client-logs",
            s3_prefixes=["alb/api-prod/AWSLogs/111/elasticloadbalancing/eu-west-1/"],
            gcs_staging_prefix="gs://staging/client-a/alb/",
            reader_role_arn="arn:aws:iam::111122223333:role/log-reader",
        )
        assert cfg.source_type == "alb"

    def test_invalid_source_type(self):
        with pytest.raises(ValueError, match="source_type"):
            AWSSourceConfig(
                source_type="invalid",
                s3_bucket="b",
                s3_prefixes=["p/"],
                gcs_staging_prefix="gs://x/",
                reader_role_arn="arn:aws:iam::111122223333:role/r",
            )

    def test_empty_prefixes(self):
        with pytest.raises(ValueError, match="s3_prefixes"):
            AWSSourceConfig(
                source_type="alb",
                s3_bucket="b",
                s3_prefixes=[],
                gcs_staging_prefix="gs://x/",
                reader_role_arn="arn:aws:iam::111122223333:role/r",
            )

    def test_from_dict_legacy_single_prefix(self):
        cfg = AWSSourceConfig.from_dict(
            {
                "source_type": "alb",
                "s3_bucket": "b",
                "s3_prefix": "alb/api/",
                "gcs_staging_prefix": "gs://x/",
                "reader_role_arn": "arn:aws:iam::111122223333:role/r",
            }
        )
        assert cfg.s3_prefixes == ["alb/api/"]

    def test_from_dict_list_prefixes(self):
        cfg = AWSSourceConfig.from_dict(
            {
                "source_type": "alb",
                "s3_bucket": "b",
                "s3_prefixes": ["alb/api/", "alb/web/"],
                "gcs_staging_prefix": "gs://x/",
                "reader_role_arn": "arn:aws:iam::111122223333:role/r",
            }
        )
        assert cfg.s3_prefixes == ["alb/api/", "alb/web/"]


class TestDomainConfigProvider:
    def test_default_provider_is_cloudflare(self):
        dc = DomainConfig(domain="example.com", zone_id="zone-abc")
        assert dc.provider == "cloudflare"
        assert dc.aws is None

    def test_aws_provider(self):
        aws = AWSSourceConfig(
            source_type="cloudfront",
            s3_bucket="b",
            s3_prefixes=["p/"],
            gcs_staging_prefix="gs://x/",
            reader_role_arn="arn:aws:iam::111122223333:role/r",
        )
        dc = DomainConfig(domain="example.com", zone_id="", provider="aws_s3", aws=aws)
        assert dc.provider == "aws_s3"
        assert dc.aws is aws

    def test_aws_provider_requires_aws_block(self):
        with pytest.raises(ValueError, match="aws"):
            DomainConfig(domain="example.com", zone_id="", provider="aws_s3", aws=None)

    def test_cloudflare_provider_requires_zone_id(self):
        with pytest.raises(ValueError, match="zone_id"):
            DomainConfig(domain="example.com", zone_id="", provider="cloudflare")

    def test_unknown_provider(self):
        with pytest.raises(ValueError, match="provider"):
            DomainConfig(domain="example.com", zone_id="z", provider="myspace")

    def test_from_dict_cloudflare_backward_compat(self):
        dc = DomainConfig.from_dict({"domain": "example.com", "zone_id": "zone-abc"})
        assert dc.provider == "cloudflare"
        assert dc.aws is None
        assert dc.host_suffix == ""

    def test_host_suffix_from_dict_and_wildcard_domain(self):
        stamped = DomainConfig.from_dict(
            {
                "domain": "www.example.com",
                "zone_id": "zone-abc",
                "host_suffix": "example.com",
            }
        )
        assert stamped.host_suffix == "example.com"
        wildcard = DomainConfig.from_dict(
            {"domain": "*.example.com", "zone_id": "zone-abc"}
        )
        assert wildcard.host_suffix == "example.com"

    def test_from_dict_aws(self):
        dc = DomainConfig.from_dict(
            {
                "domain": "example.com",
                "zone_id": "",
                "provider": "aws_s3",
                "aws": {
                    "source_type": "cloudfront",
                    "s3_bucket": "b",
                    "s3_prefixes": ["p/"],
                    "gcs_staging_prefix": "gs://x/",
                    "reader_role_arn": "arn:aws:iam::111122223333:role/r",
                },
            }
        )
        assert dc.provider == "aws_s3"
        assert dc.aws is not None
        assert dc.aws.source_type == "cloudfront"


class TestSettingsDuplicateDomainValidation:
    def test_rejects_duplicate_domain_with_different_providers(self):
        from llm_bot_pipeline.config.settings import (
            AWSSourceConfig,
            DomainConfig,
            Settings,
        )

        cf = DomainConfig(domain="shop.acme.com", zone_id="z", provider="cloudflare")
        aws = DomainConfig(
            domain="shop.acme.com",
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
        errors = Settings(domains=[cf, aws]).validate()
        assert any("duplicate domain" in e for e in errors)

    def test_allows_same_domain_with_same_provider(self):
        from llm_bot_pipeline.config.settings import DomainConfig, Settings

        dc = DomainConfig(domain="shop.acme.com", zone_id="z", provider="cloudflare")
        Settings(domains=[dc, dc]).validate()

    def test_allows_different_domains_with_different_providers(self):
        from llm_bot_pipeline.config.settings import (
            AWSSourceConfig,
            DomainConfig,
            Settings,
        )

        cf = DomainConfig(domain="blog.acme.com", zone_id="z", provider="cloudflare")
        aws = DomainConfig(
            domain="shop.acme.com",
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
        Settings(domains=[cf, aws]).validate()


class TestGCPServiceAccountUniqueId:
    def test_settings_carries_sa_unique_id(self):
        from llm_bot_pipeline.config.settings import Settings

        s = Settings(gcp_service_account_unique_id="1234567890123456789")
        assert s.gcp_service_account_unique_id == "1234567890123456789"

    def test_from_dict_reads_gcp_block(self):
        from llm_bot_pipeline.config.settings import Settings

        s = Settings.from_dict(
            {
                "gcp": {
                    "project_id": "my-proj",
                    "service_account_unique_id": "1234567890123456789",
                },
            }
        )
        assert s.gcp_service_account_unique_id == "1234567890123456789"
        assert s.gcp_project_id == "my-proj"
