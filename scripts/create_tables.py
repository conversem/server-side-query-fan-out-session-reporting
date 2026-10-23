#!/usr/bin/env python3
"""
CLI script to create BigQuery tables for the LLM bot traffic pipeline.

Usage:
    python scripts/create_tables.py --project-id PROJECT_ID
    python scripts/create_tables.py --project-id PROJECT_ID --table raw
    python scripts/create_tables.py --project-id PROJECT_ID --table clean
    python scripts/create_tables.py --project-id PROJECT_ID --table reporting
    python scripts/create_tables.py --project-id PROJECT_ID --info
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.config.constants import (
    DATASET_RAW,
    DATASET_REPORT,
    TABLE_CLEAN_BOT_REQUESTS,
    TABLE_DAILY_SUMMARY,
    TABLE_RAW_BOT_REQUESTS,
    TABLE_URL_PERFORMANCE,
)
from llm_bot_pipeline.infrastructure.bigquery_setup import BigQuerySetup
from llm_bot_pipeline.storage.bigquery_schemas import TABLE_CONFIG, _get_table_schemas


def _get_schema(table_name: str) -> list:
    """Get BigQuery SchemaField list for a table."""
    schemas = _get_table_schemas()
    if table_name not in schemas:
        raise KeyError(f"No schema defined for table: {table_name}")
    return schemas[table_name]


def _get_config(table_name: str) -> dict:
    """Get partition/clustering config for a table."""
    return TABLE_CONFIG.get(table_name, {})


def print_schema(
    name: str, schema: list, partition: str = None, clustering: list = None
):
    """Print a schema in a readable format."""
    print(f"\n  {name} Schema:")
    print("-" * 60)
    for field in schema:
        mode = f" ({field.mode})" if field.mode != "NULLABLE" else ""
        print(f"  {field.name:30} {field.field_type:12}{mode}")

    if partition:
        print(f"\n  Partitioned by: {partition}")
    if clustering:
        print(f"  Clustered by: {', '.join(clustering)}")
    print()


def cmd_create_all(bq: BigQuerySetup) -> int:
    """Create all tables."""
    print("Creating all BigQuery tables...")
    print()

    try:
        bq.create_all_tables()
        print()
        print("All tables created successfully!")
        return 0
    except Exception as e:
        print(f"Failed to create tables: {e}")
        return 1


def cmd_create_raw(bq: BigQuerySetup) -> int:
    """Create raw table only."""
    print(f"Creating raw table: {DATASET_RAW}.{TABLE_RAW_BOT_REQUESTS}")

    try:
        schema = _get_schema(TABLE_RAW_BOT_REQUESTS)
        config = _get_config(TABLE_RAW_BOT_REQUESTS)
        bq.create_table(
            dataset_id=DATASET_RAW,
            table_id=TABLE_RAW_BOT_REQUESTS,
            schema=schema,
            partition_field=config.get("partition_field"),
            clustering_fields=config.get("clustering_fields"),
        )
        print("Raw table created successfully!")
        return 0
    except Exception as e:
        print(f"Failed to create raw table: {e}")
        return 1


def cmd_create_clean(bq: BigQuerySetup) -> int:
    """Create clean table only."""
    print(f"Creating clean table: {DATASET_REPORT}.{TABLE_CLEAN_BOT_REQUESTS}")

    try:
        schema = _get_schema(TABLE_CLEAN_BOT_REQUESTS)
        config = _get_config(TABLE_CLEAN_BOT_REQUESTS)
        bq.create_table(
            dataset_id=DATASET_REPORT,
            table_id=TABLE_CLEAN_BOT_REQUESTS,
            schema=schema,
            partition_field=config.get("partition_field"),
            clustering_fields=config.get("clustering_fields"),
        )
        print("Clean table created successfully!")
        return 0
    except Exception as e:
        print(f"Failed to create clean table: {e}")
        return 1


def cmd_create_reporting(bq: BigQuerySetup) -> int:
    """Create reporting tables (daily_summary, url_performance)."""
    print("Creating reporting tables...")

    try:
        for table_name in [TABLE_DAILY_SUMMARY, TABLE_URL_PERFORMANCE]:
            print(f"  Creating {DATASET_REPORT}.{table_name}...")
            schema = _get_schema(table_name)
            config = _get_config(table_name)
            bq.create_table(
                dataset_id=DATASET_REPORT,
                table_id=table_name,
                schema=schema,
                partition_field=config.get("partition_field"),
                clustering_fields=config.get("clustering_fields"),
            )

        print("Reporting tables created successfully!")
        return 0
    except Exception as e:
        print(f"Failed to create reporting tables: {e}")
        return 1


def cmd_info(bq: BigQuerySetup) -> int:
    """Show table information and schemas."""
    print("BigQuery Table Information")
    print("=" * 60)

    tables_to_check = [
        (DATASET_RAW, TABLE_RAW_BOT_REQUESTS),
        (DATASET_REPORT, TABLE_CLEAN_BOT_REQUESTS),
        (DATASET_REPORT, TABLE_DAILY_SUMMARY),
        (DATASET_REPORT, TABLE_URL_PERFORMANCE),
    ]

    for dataset, table_name in tables_to_check:
        try:
            schema = _get_schema(table_name)
            config = _get_config(table_name)
            print_schema(
                f"{dataset}.{table_name}",
                schema,
                partition=config.get("partition_field"),
                clustering=config.get("clustering_fields"),
            )
        except KeyError:
            print(f"  No schema for {table_name}")

    # Check table status
    print("Table Status:")
    print("-" * 60)

    for dataset, table in tables_to_check:
        exists = bq.table_exists(dataset, table)
        status = "exists" if exists else "not created"
        print(f"  {dataset}.{table}: {status}")

        if exists:
            info = bq.get_table_info(dataset, table)
            print(f"    Rows: {info.get('num_rows', 0):,}")
            if info.get("partitioning"):
                print(f"    Partitioning: {info['partitioning']}")
            if info.get("clustering"):
                print(f"    Clustering: {info['clustering']}")

    print()
    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create BigQuery tables for LLM bot traffic pipeline"
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="GCP project ID",
    )
    parser.add_argument(
        "--table",
        choices=["raw", "clean", "reporting", "all"],
        default="all",
        help="Which table(s) to create (default: all)",
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="Show table schemas and status without creating",
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

    if args.info:
        return cmd_info(bq)
    elif args.table == "raw":
        return cmd_create_raw(bq)
    elif args.table == "clean":
        return cmd_create_clean(bq)
    elif args.table == "reporting":
        return cmd_create_reporting(bq)
    else:
        return cmd_create_all(bq)


if __name__ == "__main__":
    sys.exit(main())
