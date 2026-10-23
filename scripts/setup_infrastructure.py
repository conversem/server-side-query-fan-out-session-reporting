#!/usr/bin/env python3
"""
GCP Infrastructure Setup Script
-------------------------------
Creates service accounts, BigQuery datasets, and tables for the
LLM bot traffic analysis pipeline.

Usage:
    python scripts/setup_infrastructure.py --project-id PROJECT_ID

Requirements:
    - Google Cloud SDK (gcloud) installed and authenticated
    - Or: Application Default Credentials configured
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.config.constants import (
    DATASET_RAW,
    DATASET_REPORT,
    DATASET_STAGING,
    KEY_FILE_NAME,
    SERVICE_ACCOUNT_DISPLAY_NAME,
    SERVICE_ACCOUNT_ID,
)
from llm_bot_pipeline.infrastructure.bigquery_setup import BigQuerySetup
from llm_bot_pipeline.infrastructure.service_account import (
    ServiceAccountManager,
    check_gcloud_authenticated,
    check_gcloud_installed,
)


def main():
    """Main setup function."""
    parser = argparse.ArgumentParser(
        description="Set up GCP infrastructure for LLM bot traffic pipeline"
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project ID",
    )
    parser.add_argument(
        "--key-file",
        default=KEY_FILE_NAME,
        help=f"Path to save service account key file (default: {KEY_FILE_NAME})",
    )
    parser.add_argument(
        "--skip-service-account",
        action="store_true",
        help="Skip service account creation (use existing or ADC)",
    )
    parser.add_argument(
        "--skip-key-generation",
        action="store_true",
        help="Skip service account key generation",
    )
    parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Also create BigQuery tables (not just datasets)",
    )
    parser.add_argument(
        "--location",
        default="EU",
        help="BigQuery dataset location (default: EU)",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip interactive prompts (WARNING: will regenerate service account keys)",
    )

    args = parser.parse_args()
    project_id = args.project_id
    key_path = Path(args.key_file)

    print(f"\n{'='*60}")
    print("GCP Infrastructure Setup for LLM Bot Traffic Pipeline")
    print(f"{'='*60}")
    print(f"Project ID: {project_id}")
    print(f"Key file: {key_path}")
    print()

    # Step 1: Check prerequisites
    print("[1/5] Checking prerequisites...")

    gcloud_ok = check_gcloud_installed()
    if not gcloud_ok:
        print("  gcloud CLI not found. Service account operations will be skipped.")
        print("  Install: https://cloud.google.com/sdk/docs/install")
        args.skip_service_account = True
    else:
        account = check_gcloud_authenticated()
        if account:
            print(f"  gcloud authenticated as: {account}")
        else:
            print("  gcloud not authenticated. Run: gcloud auth login")
            args.skip_service_account = True

    # Step 2: Create service account
    service_account_email = None
    if not args.skip_service_account:
        print("\n[2/5] Creating service account...")
        sa_manager = ServiceAccountManager(project_id)

        service_account_email = sa_manager.create_service_account(
            service_account_id=SERVICE_ACCOUNT_ID,
            display_name=SERVICE_ACCOUNT_DISPLAY_NAME,
            description="Service account for LLM bot log ingestion",
        )

        # Grant BigQuery roles
        print("\n[3/5] Granting IAM roles...")
        sa_manager.grant_bigquery_roles(service_account_email)

        # Generate key
        if not args.skip_key_generation:
            print("\n[4/5] Generating service account key...")
            if key_path.exists() and not args.yes:
                response = input(
                    f"Key file {key_path} already exists. Overwrite? (y/N): "
                )
                if response.lower() == "y":
                    sa_manager.create_key(
                        service_account_email, key_path, overwrite=True
                    )
                else:
                    print("  Skipping key generation")
            elif key_path.exists() and args.yes:
                print(f"  Overwriting existing key at {key_path}")
                sa_manager.create_key(service_account_email, key_path, overwrite=True)
            else:
                sa_manager.create_key(service_account_email, key_path)
        else:
            print("\n[4/5] Skipping key generation...")
    else:
        print("\n[2/5] Skipping service account creation...")
        print("[3/5] Skipping IAM role grants...")
        print("[4/5] Skipping key generation...")

    # Step 5: Create BigQuery datasets
    print("\n[5/5] Creating BigQuery datasets...")

    credentials_path = key_path if key_path.exists() else None

    try:
        bq_setup = BigQuerySetup(
            project_id=project_id,
            credentials_path=credentials_path,
            location=args.location,
        )

        if not bq_setup.verify_access():
            print("  Cannot access BigQuery. Check credentials and permissions.")
            sys.exit(1)

        bq_setup.create_all_datasets()

        if args.create_tables:
            print("\nCreating BigQuery tables...")
            bq_setup.create_all_tables()

    except Exception as e:
        print(f"  BigQuery setup failed: {e}")
        sys.exit(1)

    # Summary
    print(f"\n{'='*60}")
    print("Setup Complete!")
    print(f"{'='*60}")

    if service_account_email:
        print(f"\nService Account: {service_account_email}")

    if key_path.exists():
        print(f"Key File: {key_path.absolute()}")
        print("\n  IMPORTANT: Add the key file to .gitignore!")

    print("\nCreated BigQuery Datasets:")
    print(f"  - {DATASET_RAW} (raw Logpush data)")
    print(f"  - {DATASET_STAGING} (intermediate processing)")
    print(f"  - {DATASET_REPORT} (clean analysis-ready data)")

    print("\nNext steps:")
    print("  1. Set environment variables:")
    print(f"     export GCP_PROJECT_ID={project_id}")
    if key_path.exists():
        print(f"     export GCP_SERVICE_ACCOUNT_KEY={key_path}")
    print("  2. Configure Cloudflare Logpush job")
    print("  3. Run the pipeline")


if __name__ == "__main__":
    main()
