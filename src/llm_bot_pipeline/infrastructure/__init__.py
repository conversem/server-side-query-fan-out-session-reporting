"""Infrastructure setup module for GCP resources."""

__all__ = ["BigQuerySetup", "ServiceAccountManager", "setup_gcp_bq_infrastructure"]

try:
    from .service_account import ServiceAccountManager
except ImportError:
    ServiceAccountManager = None  # GCP service account module not available

try:
    from .bigquery_setup import BigQuerySetup
except ImportError:
    BigQuerySetup = None  # google-cloud-bigquery not installed

try:
    from .cloud_run_setup import setup_gcp_bq_infrastructure
except ImportError:
    setup_gcp_bq_infrastructure = None  # google-cloud-run not installed
