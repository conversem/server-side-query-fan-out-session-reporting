"""
Provider-specific ingestion adapters.

Each subdirectory contains an adapter implementation for a specific
CDN or cloud provider (e.g., cloudflare/, aws_cloudfront/, azure_cdn/).

Adapters are auto-registered when imported via the ingestion module.
"""

# Import Akamai DataStream adapter (auto-registers via decorator)
from .akamai import AkamaiAdapter  # noqa: F401

# Import AWS ALB adapter (auto-registers via decorator)
from .aws_alb import ALBAdapter  # noqa: F401

# Import AWS CloudFront adapter (auto-registers via decorator)
from .aws_cloudfront import CloudFrontAdapter  # noqa: F401

# Import Azure CDN / Front Door adapter (auto-registers via decorator)
from .azure_cdn import AzureCDNAdapter  # noqa: F401

# Import Cloudflare adapter (auto-registers via decorator)
from .cloudflare import CloudflareAdapter  # noqa: F401

# Import Fastly adapter (auto-registers via decorator)
from .fastly import FastlyAdapter  # noqa: F401

# Import GCP Cloud CDN adapter (auto-registers via decorator)
from .gcp_cdn import GCPCDNAdapter  # noqa: F401

# Import universal adapter (auto-registers via decorator)
from .universal import UniversalAdapter  # noqa: F401

__all__: list[str] = [
    "AkamaiAdapter",
    "ALBAdapter",
    "AzureCDNAdapter",
    "CloudflareAdapter",
    "CloudFrontAdapter",
    "FastlyAdapter",
    "GCPCDNAdapter",
    "UniversalAdapter",
]
