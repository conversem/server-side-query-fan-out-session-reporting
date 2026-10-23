#!/usr/bin/env python3
"""
CLI script to create BigQuery monitoring views for the LLM bot traffic pipeline.

Creates dashboard views for:
- Pipeline health monitoring
- Traffic summaries
- Provider analytics
- Content performance

Usage:
    python scripts/create_monitoring_views.py --project-id PROJECT_ID
    python scripts/create_monitoring_views.py --project-id PROJECT_ID --list
    python scripts/create_monitoring_views.py --project-id PROJECT_ID --view pipeline_health
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from monitoring_view_definitions import get_view_definitions

from llm_bot_pipeline.config.constants import DATASET_REPORT
from llm_bot_pipeline.infrastructure.bigquery_setup import BigQuerySetup


def cmd_create_all(bq: BigQuerySetup, project_id: str) -> int:
    """Create all monitoring views."""
    print("Creating all monitoring views...")
    print()

    views = get_view_definitions(project_id)
    created = 0
    failed = 0

    for view_name, (sql, description) in views.items():
        try:
            bq.create_view(
                dataset_id=DATASET_REPORT,
                view_id=view_name,
                view_query=sql,
                description=description,
            )
            created += 1
        except Exception as e:
            print(f"  Failed to create {view_name}: {e}")
            failed += 1

    print()
    print(f"Created {created} views")
    if failed:
        print(f"Failed to create {failed} views")
        return 1
    return 0


def cmd_create_view(bq: BigQuerySetup, project_id: str, view_name: str) -> int:
    """Create a specific view."""
    views = get_view_definitions(project_id)

    if view_name not in views:
        print(f"Unknown view: {view_name}")
        print(f"   Available views: {', '.join(views.keys())}")
        return 1

    sql, description = views[view_name]

    try:
        bq.create_view(
            dataset_id=DATASET_REPORT,
            view_id=view_name,
            view_query=sql,
            description=description,
        )
        return 0
    except Exception as e:
        print(f"Failed to create view: {e}")
        return 1


def cmd_list(bq: BigQuerySetup, project_id: str) -> int:
    """List all available views and their status."""
    views = get_view_definitions(project_id)

    print("Available Monitoring Views")
    print("=" * 70)

    for view_name, (_, description) in views.items():
        exists = bq.view_exists(DATASET_REPORT, view_name)
        status = "exists" if exists else "not created"
        print(f"  {view_name:35} {status}")
        print(f"      {description}")
        print()

    existing_views = set(bq.list_views(DATASET_REPORT))
    defined_views = set(views.keys())
    extra_views = existing_views - defined_views

    if extra_views:
        print()
        print("Additional views in BigQuery (not managed by this script):")
        for v in sorted(extra_views):
            print(f"  - {v}")

    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create BigQuery monitoring views for LLM bot traffic pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create all monitoring views
  python scripts/create_monitoring_views.py --project-id my-project

  # List views and their status
  python scripts/create_monitoring_views.py --project-id my-project --list

  # Create a specific view
  python scripts/create_monitoring_views.py --project-id my-project --view v_pipeline_health
        """,
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project ID",
    )
    parser.add_argument(
        "--view",
        help="Create a specific view (use --list to see available views)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available views and their status",
    )
    parser.add_argument(
        "--credentials",
        type=str,
        default=None,
        help="Path to service account JSON key file (uses ADC if not provided)",
    )

    args = parser.parse_args()

    credentials_path = Path(args.credentials) if args.credentials else None

    try:
        bq = BigQuerySetup(
            project_id=args.project_id,
            credentials_path=credentials_path,
        )
    except Exception as e:
        print(f"Failed to initialize BigQuery client: {e}")
        return 1

    if args.list:
        return cmd_list(bq, args.project_id)
    elif args.view:
        return cmd_create_view(bq, args.project_id, args.view)
    else:
        return cmd_create_all(bq, args.project_id)


if __name__ == "__main__":
    sys.exit(main())
