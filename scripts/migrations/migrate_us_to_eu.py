#!/usr/bin/env python3
"""
BigQuery Region Migration: US → EU

Migrates bot_logs and bot_logs_raw datasets from US to EU region.
BigQuery doesn't support dataset rename or location change, so this uses
a create-copy-swap flow with temporary datasets.

Flow:
  1. Create temp EU datasets (_migrating)
  2. Create tables with correct schema in temp datasets
  3. Copy data cross-region (US → temp EU)
  4. Verify row counts match
  5. Delete US datasets
  6. Create final EU datasets with original names (via setup_infrastructure.py)
  7. Copy data from temp EU → final EU
  8. Verify row counts match
  9. Keep temp datasets as backup

Usage:
    python scripts/migrations/migrate_us_to_eu.py [--dry-run] [--step STEP]
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from google.cloud import bigquery

from llm_bot_pipeline.storage.bigquery_schemas import TABLE_CONFIG, _get_table_schemas

PROJECT = "example"
DS_RAW = "bot_logs_raw"
DS_REPORT = "bot_logs"
DS_STAGING = "bot_logs_staging"
DS_RAW_MIG = "bot_logs_raw_migrating"
DS_REPORT_MIG = "bot_logs_migrating"

TABLES_RAW = ["raw_bot_requests"]
TABLES_REPORT = [
    "bot_requests_daily",
    "daily_summary",
    "url_performance",
    "data_freshness",
    "query_fanout_sessions",
    "session_url_details",
    "sitemap_urls",
    "sitemap_freshness",
    "url_volume_decay",
]


def get_row_counts(client, datasets_tables):
    counts = {}
    for ds, tbl in datasets_tables:
        try:
            t = client.get_table(f"{PROJECT}.{ds}.{tbl}")
            counts[f"{ds}.{tbl}"] = t.num_rows
        except Exception as e:
            counts[f"{ds}.{tbl}"] = f"ERROR: {e}"
    return counts


def print_counts(counts, label):
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}")
    for k, v in counts.items():
        print(f"  {k}: {v}")


def create_temp_datasets(client, dry_run=False):
    print("\n--- Creating temporary EU datasets ---")
    for ds_id in [DS_RAW_MIG, DS_REPORT_MIG]:
        ds = bigquery.Dataset(f"{PROJECT}.{ds_id}")
        ds.location = "EU"
        if dry_run:
            print(f"  [DRY-RUN] Would create {ds_id} in EU")
        else:
            client.create_dataset(ds, exists_ok=True)
            print(f"  Created {ds_id} in EU")


def create_tables_in_dataset(client, target_ds, tables, dry_run=False):
    print(f"\n--- Creating tables in {target_ds} ---")
    schemas = _get_table_schemas()
    for tbl_name in tables:
        if tbl_name not in schemas:
            print(f"  SKIP {tbl_name} — not in schema definitions")
            continue
        table_ref = f"{PROJECT}.{target_ds}.{tbl_name}"
        table = bigquery.Table(table_ref, schema=schemas[tbl_name])
        cfg = TABLE_CONFIG.get(tbl_name, {})
        if cfg.get("partition_field"):
            table.time_partitioning = bigquery.TimePartitioning(
                field=cfg["partition_field"]
            )
        if cfg.get("clustering_fields"):
            table.clustering_fields = cfg["clustering_fields"]
        if dry_run:
            print(f"  [DRY-RUN] Would create {tbl_name}")
        else:
            client.create_table(table, exists_ok=True)
            print(f"  Created {tbl_name}")


def copy_data_bq(client, src_ds, dst_ds, tables, dry_run=False):
    """Copy tables using BigQuery copy_table API (works cross-region and same-region).
    Uses WRITE_TRUNCATE to overwrite any pre-existing tables."""
    print(f"\n--- Copying data: {src_ds} → {dst_ds} ---")

    for tbl in tables:
        src_ref = f"{PROJECT}.{src_ds}.{tbl}"
        dst_ref = f"{PROJECT}.{dst_ds}.{tbl}"

        if dry_run:
            print(f"  {tbl}: [DRY-RUN] copy_table {src_ref} → {dst_ref}")
            continue

        job_config = bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        t0 = time.time()
        job = client.copy_table(src_ref, dst_ref, job_config=job_config)
        job.result()
        elapsed = time.time() - t0
        print(f"  {tbl}: copied in {elapsed:.1f}s")


def copy_data_same_region(client, src_ds, dst_ds, tables, dry_run=False):
    """Copy data within the same region using INSERT INTO...SELECT.
    Handles schema differences (extra columns in dst get NULL)."""
    print(f"\n--- Copying data (same region): {src_ds} → {dst_ds} ---")

    for tbl in tables:
        src = f"`{PROJECT}.{src_ds}.{tbl}`"
        dst = f"`{PROJECT}.{dst_ds}.{tbl}`"

        if dry_run:
            print(f"  {tbl}: [DRY-RUN] INSERT INTO {dst_ds}.{tbl} SELECT ... FROM {src_ds}.{tbl}")
            continue

        src_table = client.get_table(f"{PROJECT}.{src_ds}.{tbl}")
        src_cols = {f.name for f in src_table.schema}

        dst_table = client.get_table(f"{PROJECT}.{dst_ds}.{tbl}")
        dst_cols = {f.name for f in dst_table.schema}

        extra_in_dst = dst_cols - src_cols

        if extra_in_dst:
            dst_col_list = [f.name for f in dst_table.schema]
            src_select = []
            for col in dst_col_list:
                if col in extra_in_dst:
                    src_select.append(f"NULL AS {col}")
                else:
                    src_select.append(col)
            col_list_str = ", ".join(dst_col_list)
            select_str = ", ".join(src_select)
            sql = f"INSERT INTO {dst} ({col_list_str}) SELECT {select_str} FROM {src}"
            print(f"  {tbl}: explicit columns (new: {extra_in_dst})")
        else:
            sql = f"INSERT INTO {dst} SELECT * FROM {src}"
            print(f"  {tbl}: SELECT *")

        t0 = time.time()
        job = client.query(sql)
        job.result()
        elapsed = time.time() - t0
        print(f"    Done in {elapsed:.1f}s")


def delete_datasets(client, dataset_ids, dry_run=False):
    print("\n--- Deleting US datasets ---")
    for ds_id in dataset_ids:
        if dry_run:
            print(f"  [DRY-RUN] Would delete {ds_id}")
        else:
            client.delete_dataset(
                f"{PROJECT}.{ds_id}", delete_contents=True, not_found_ok=True
            )
            print(f"  Deleted {ds_id}")


def verify_counts(before, after, label):
    print(f"\n--- Verification: {label} ---")
    ok = True
    for key, expected in before.items():
        if isinstance(expected, str):
            continue
        actual = after.get(key, "MISSING")
        if isinstance(actual, str):
            ds_new = key.split(".")[0]
            for after_key, after_val in after.items():
                if after_key.endswith(key.split(".")[1]):
                    actual = after_val
                    break

        status = "OK" if actual == expected else "MISMATCH"
        if status == "MISMATCH":
            ok = False
        print(f"  {key}: expected={expected}, actual={actual} [{status}]")
    return ok


def main():
    parser = argparse.ArgumentParser(description="Migrate BigQuery US → EU")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--step",
        type=int,
        help="Run only this step (1=create temp, 2=copy to temp, 3=delete US, 4=copy to final)",
    )
    args = parser.parse_args()

    client = bigquery.Client()

    all_tables = [(DS_RAW, t) for t in TABLES_RAW] + [
        (DS_REPORT, t) for t in TABLES_REPORT
    ]

    if args.step is None or args.step == 0:
        print("Recording baseline row counts...")
        baseline = get_row_counts(client, all_tables)
        print_counts(baseline, "BASELINE (US datasets)")

        with open("/tmp/bq-migration-baseline.txt", "w") as f:
            for k, v in baseline.items():
                f.write(f"{k}: {v}\n")

    if args.step is None or args.step == 1:
        print("\n" + "=" * 60)
        print("  STEP 1: Create temp EU datasets + tables")
        print("=" * 60)
        create_temp_datasets(client, args.dry_run)
        create_tables_in_dataset(client, DS_RAW_MIG, TABLES_RAW, args.dry_run)
        create_tables_in_dataset(client, DS_REPORT_MIG, TABLES_REPORT, args.dry_run)

    if args.step is None or args.step == 2:
        print("\n" + "=" * 60)
        print("  STEP 2: Copy data US → temp EU (cross-region)")
        print("=" * 60)
        copy_data_bq(client, DS_RAW, DS_RAW_MIG, TABLES_RAW, args.dry_run)
        copy_data_bq(client, DS_REPORT, DS_REPORT_MIG, TABLES_REPORT, args.dry_run)

        if not args.dry_run:
            mig_tables = [(DS_RAW_MIG, t) for t in TABLES_RAW] + [
                (DS_REPORT_MIG, t) for t in TABLES_REPORT
            ]
            mig_counts = get_row_counts(client, mig_tables)
            print_counts(mig_counts, "TEMP EU COUNTS")

            baseline = {}
            with open("/tmp/bq-migration-baseline.txt") as f:
                for line in f:
                    k, v = line.strip().split(": ")
                    baseline[k] = int(v) if v.isdigit() else v
            remap = {}
            for k, v in baseline.items():
                ds, tbl = k.split(".")
                new_ds = DS_RAW_MIG if ds == DS_RAW else DS_REPORT_MIG
                remap[f"{new_ds}.{tbl}"] = v
            ok = verify_counts(remap, mig_counts, "US baseline vs temp EU")
            if not ok:
                print("\n  VERIFICATION FAILED — aborting before US delete")
                sys.exit(1)

    if args.step is None or args.step == 3:
        print("\n" + "=" * 60)
        print("  STEP 3: Delete US datasets")
        print("=" * 60)
        if not args.dry_run:
            resp = input("  Temp EU verified. Delete US datasets? [y/N] ")
            if resp.lower() != "y":
                print("  Aborted.")
                sys.exit(0)
        delete_datasets(
            client, [DS_REPORT, DS_RAW, DS_STAGING], args.dry_run
        )

    if args.step is None or args.step == 4:
        print("\n" + "=" * 60)
        print("  STEP 4: Create final EU datasets + copy from temp")
        print("=" * 60)
        print("  Creating final EU datasets with original names...")
        for ds_id in [DS_RAW, DS_STAGING, DS_REPORT]:
            ds = bigquery.Dataset(f"{PROJECT}.{ds_id}")
            ds.location = "EU"
            if args.dry_run:
                print(f"  [DRY-RUN] Would create {ds_id} in EU")
            else:
                client.create_dataset(ds, exists_ok=True)
                print(f"  Created {ds_id} in EU")

        print("\n  Copying data from temp EU → final EU (copy_table)...")
        copy_data_bq(client, DS_RAW_MIG, DS_RAW, TABLES_RAW, args.dry_run)
        copy_data_bq(client, DS_REPORT_MIG, DS_REPORT, TABLES_REPORT, args.dry_run)

        if not args.dry_run:
            final_counts = get_row_counts(client, all_tables)
            print_counts(final_counts, "FINAL EU COUNTS")

            baseline = {}
            with open("/tmp/bq-migration-baseline.txt") as f:
                for line in f:
                    k, v = line.strip().split(": ")
                    baseline[k] = int(v) if v.isdigit() else v
            ok = verify_counts(baseline, final_counts, "baseline vs final EU")
            if not ok:
                print("\n  VERIFICATION FAILED — data in _migrating is safe")
                sys.exit(1)

            for ds_id in [DS_RAW, DS_REPORT]:
                ds = client.get_dataset(f"{PROJECT}.{ds_id}")
                print(f"\n  {ds_id} location: {ds.location}")

    print("\n" + "=" * 60)
    print("  MIGRATION COMPLETE")
    print("=" * 60)
    print("  Temp datasets (_migrating) kept as backup.")
    print("  Delete after 4 weeks:")
    print(f"    bq rm -r -f {PROJECT}:{DS_RAW_MIG}")
    print(f"    bq rm -r -f {PROJECT}:{DS_REPORT_MIG}")


if __name__ == "__main__":
    main()
