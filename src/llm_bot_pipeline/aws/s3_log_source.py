"""AWS S3 log source — daily ETL pull (CF logpull contract, raw_bot_requests rows)."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, timedelta
from typing import IO, Any, Iterator

from ..config.settings import AWSSourceConfig, DomainConfig, Settings
from ..ingestion.base import IngestionRecord
from ..ingestion.exceptions import ParseError
from ..ingestion.providers.aws_alb.adapter import ALBAdapter
from ..ingestion.providers.aws_cloudfront.adapter import CloudFrontAdapter
from ..ingestion.registry import get_adapter
from ..utils.bot_classifier import classify_bot
from .gcs_log_streamer import canonical_gcs_prefix, stream_log_handles
from .raw_row_mapper import map_to_raw_row

logger = logging.getLogger(__name__)


def _aws_config(domain_config: DomainConfig) -> AWSSourceConfig:
    """Return the AWS block or raise when this domain cannot be pulled from GCS."""
    aws = domain_config.aws
    if domain_config.provider != "aws_s3":
        raise ValueError(
            f"AWS log pull requires provider='aws_s3', got "
            f"{domain_config.provider!r} (domain={domain_config.domain!r})"
        )
    if aws is None:
        raise ValueError(
            "AWS log pull requires a non-None aws block "
            f"on DomainConfig (domain={domain_config.domain!r})"
        )
    return aws


def pull_aws_logs_for_date_range(
    start_date: date,
    end_date: date,
    domain_config: DomainConfig,
    settings: Settings,
    filter_llm_bots: bool = True,
) -> Iterator[dict[str, Any]]:
    """Pull AWS logs for one domain and yield raw_bot_requests-shaped dicts."""
    yield from pull_shared_aws_logs(
        start_date=start_date,
        end_date=end_date,
        domain_configs=[domain_config],
        settings=settings,
        filter_llm_bots=filter_llm_bots,
    )


def pull_shared_aws_logs(
    start_date: date,
    end_date: date,
    domain_configs: Sequence[DomainConfig],
    settings: Settings,
    filter_llm_bots: bool = True,
) -> Iterator[dict[str, Any]]:
    """Pull one GCS prefix once and fan matching rows out to each domain.

    Every domain must use the same ``source_type`` and ``gcs_staging_prefix``.
    A request is emitted once per domain whose host filter matches. With no
    host filter, each domain receives the row, which is the same result as
    scanning that prefix separately for each domain.
    """
    if not domain_configs:
        raise ValueError("pull_shared_aws_logs requires at least one domain")

    aws_configs = [_aws_config(domain_config) for domain_config in domain_configs]
    source_types = {aws.source_type for aws in aws_configs}
    prefixes = {canonical_gcs_prefix(aws.gcs_staging_prefix) for aws in aws_configs}
    if len(source_types) != 1 or len(prefixes) != 1:
        raise ValueError(
            "Shared AWS pull requires the same source_type and gcs_staging_prefix"
        )

    aws_cfg = aws_configs[0]
    gcs_prefix = next(iter(prefixes))
    source_provider = f"aws_{aws_cfg.source_type}"
    if aws_cfg.source_type not in ("cloudfront", "alb"):
        raise ValueError(f"Unknown AWS source_type: {aws_cfg.source_type!r}")

    adapter = get_adapter(source_provider)

    def parse(handle: IO[str]) -> Iterator[IngestionRecord]:
        if isinstance(adapter, CloudFrontAdapter):
            return adapter.parse_stream(handle)
        if isinstance(adapter, ALBAdapter):
            return adapter.parse_stream(handle)
        raise TypeError(f"Adapter {adapter!r} does not support stream parsing")

    # Extend the end by one day: a request near midnight can land in the next
    # day's object. The per-record timestamp check still bounds the output.
    scan_range = (start_date, end_date + timedelta(days=1))
    if len(domain_configs) == 1:
        logger.info(
            "[aws] pull domain=%s source_type=%s prefix=%s range=%s..%s",
            domain_configs[0].domain,
            aws_cfg.source_type,
            gcs_prefix,
            start_date,
            end_date,
        )
    else:
        logger.info(
            "[aws] pull shared prefix once domains=%s source_type=%s "
            "prefix=%s range=%s..%s",
            ",".join(domain_config.domain for domain_config in domain_configs),
            aws_cfg.source_type,
            gcs_prefix,
            start_date,
            end_date,
        )
    for blob_name, handle in stream_log_handles(gcs_prefix, date_range=scan_range):
        try:
            for record in parse(handle):
                if record.timestamp is not None:
                    record_date = record.timestamp.date()
                    if not (start_date <= record_date <= end_date):
                        continue

                bot_info = None
                if filter_llm_bots:
                    bot_info = classify_bot(record.user_agent)
                    if bot_info is None:
                        continue

                for domain_config, domain_aws in zip(domain_configs, aws_configs):
                    host_filter = getattr(domain_aws, "host_filter", None)
                    if host_filter is not None and not host_filter.matches(record.host):
                        continue
                    yield map_to_raw_row(
                        record,
                        domain=domain_config.domain,
                        source_provider=source_provider,
                        bot_info=bot_info,
                    )
        except ParseError as e:
            logger.warning("Skipping blob %s: %s", blob_name, e)
            continue
        finally:
            handle.close()
