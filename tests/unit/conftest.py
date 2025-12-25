"""
Pytest configuration and shared fixtures for unit tests.
"""

import pytest


@pytest.fixture
def register_providers():
    """
    Fixture to ensure providers are registered before tests that need them.

    Use this fixture in tests that depend on registered providers.
    Since IngestionRegistry.clear() may have been called, we explicitly
    re-register the provider classes.
    """
    # Import provider classes
    from llm_bot_pipeline.ingestion.providers import (
        AkamaiAdapter,
        ALBAdapter,
        AzureCDNAdapter,
        CloudflareAdapter,
        CloudFrontAdapter,
        FastlyAdapter,
        GCPCDNAdapter,
        UniversalAdapter,
    )
    from llm_bot_pipeline.ingestion.registry import IngestionRegistry

    # Explicitly register them (in case registry was cleared)
    if not IngestionRegistry.is_provider_registered("universal"):
        IngestionRegistry.register_provider("universal", UniversalAdapter)
    if not IngestionRegistry.is_provider_registered("akamai"):
        IngestionRegistry.register_provider("akamai", AkamaiAdapter)
    if not IngestionRegistry.is_provider_registered("aws_alb"):
        IngestionRegistry.register_provider("aws_alb", ALBAdapter)
    if not IngestionRegistry.is_provider_registered("aws_cloudfront"):
        IngestionRegistry.register_provider("aws_cloudfront", CloudFrontAdapter)
    if not IngestionRegistry.is_provider_registered("azure_cdn"):
        IngestionRegistry.register_provider("azure_cdn", AzureCDNAdapter)
    if not IngestionRegistry.is_provider_registered("cloudflare"):
        IngestionRegistry.register_provider("cloudflare", CloudflareAdapter)
    if not IngestionRegistry.is_provider_registered("fastly"):
        IngestionRegistry.register_provider("fastly", FastlyAdapter)
    if not IngestionRegistry.is_provider_registered("gcp_cdn"):
        IngestionRegistry.register_provider("gcp_cdn", GCPCDNAdapter)
