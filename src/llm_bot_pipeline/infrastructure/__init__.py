"""Infrastructure setup module for GCP resources."""

from .service_account import ServiceAccountManager

__all__ = ["BigQuerySetup", "ServiceAccountManager", "setup_gcp_bq_infrastructure"]

try:
    from .bigquery_setup import BigQuerySetup
except ImportError:
    BigQuerySetup = None  # google-cloud-bigquery not installed

try:
    from .cloud_run_setup import setup_gcp_bq_infrastructure
except ImportError:
    setup_gcp_bq_infrastructure = None  # google-cloud-run not installed
