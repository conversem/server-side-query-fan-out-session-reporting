"""Provider → pull-function dispatch for daily ETL ingestion."""

from __future__ import annotations

from typing import Any, Callable, Iterator

from ..aws.s3_log_source import pull_aws_logs_for_date_range
from ..cloudflare.logpull import pull_logs_for_date_range
from ..config.settings import DomainConfig

PullFn = Callable[..., Iterator[dict[str, Any]]]

_REGISTRY: dict[str, PullFn] = {
    "cloudflare": pull_logs_for_date_range,
    "aws_s3": pull_aws_logs_for_date_range,
}


def dispatch(domain_config: DomainConfig) -> PullFn:
    """Return the pull function for ``domain_config.provider``."""
    provider = domain_config.provider
    if provider not in _REGISTRY:
        known = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"No pull function registered for provider={provider!r}; "
            f"known providers: [{known}]"
        )
    return _REGISTRY[provider]


def register(provider: str, pull_fn: PullFn) -> None:
    """Register a pull function (tests/extensions)."""
    _REGISTRY[provider] = pull_fn
