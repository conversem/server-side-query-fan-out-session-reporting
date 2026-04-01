#!/usr/bin/env python3
"""
One-time migration: deduplicate sitemap_urls in BigQuery.

Collapses 13-16× duplicate rows (from repeated sitemap ingestion runs)
down to one row per (domain, url_path), keeping the most recent _fetched_at.

Usage:
    python scripts/migrations/migrate_dedup_sitemap_urls.py \
        --project ga-tot-gbq --dataset bot_logs [--dry-run]

Rollback (if migration fails or table is found empty after abort):
    INSERT INTO `<project>.<dataset>.sitemap_urls`
    SELECT * FROM `<project>.<dataset>.sitemap_urls_backup`;

IMPORTANT: Drop sitemap_urls_backup manually only after verification passes.
If re-running after a prior partial run, first DROP TABLE sitemap_urls_backup.
"""

import argparse
import logging
import sys

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def run(project: str, dataset: str, dry_run: bool) -> int:
    client = bigquery.Client(project=project)
    table = f"`{project}.{dataset}.sitemap_urls`"
    backup = f"`{project}.{dataset}.sitemap_urls_backup`"
    dedup_tmp = f"`{project}.{dataset}.sitemap_urls_dedup_tmp`"

    def count(tbl: str) -> int:
        return list(client.query(f"SELECT COUNT(*) AS n FROM {tbl}").result())[0]["n"]

    log.info("Counting rows in %s ...", table)
    pre_count = count(table)
    log.info("Pre-migration row count: %d", pre_count)

    if dry_run:
        dedup_sql = f"""
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY domain, url_path
                        ORDER BY _fetched_at DESC
                    ) AS _rn
                FROM {table}
            ) WHERE _rn = 1
        """
        dedup_rows = list(
            client.query(f"SELECT COUNT(*) AS n FROM ({dedup_sql}) t").result()
        )[0]["n"]
        log.info(
            "[DRY RUN] Would reduce: %d → %d rows (saves %d)",
            pre_count,
            dedup_rows,
            pre_count - dedup_rows,
        )
        return 0

    # Step 1: Build dedup temp table
    log.info("Building dedup temp table %s ...", dedup_tmp)
    client.query(f"DROP TABLE IF EXISTS {dedup_tmp}").result()
    client.query(f"""
        CREATE TABLE {dedup_tmp} AS
        SELECT * EXCEPT(_rn) FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY domain, url_path
                    ORDER BY _fetched_at DESC
                ) AS _rn
            FROM {table}
        ) WHERE _rn = 1
    """).result()
    dedup_count = count(dedup_tmp)
    log.info(
        "Dedup temp table has %d rows (will reduce from %d)", dedup_count, pre_count
    )

    # Step 2: Create backup — fails loudly if backup already exists from a prior run.
    # If this errors: inspect sitemap_urls first; if the table is intact, DROP the backup and retry.
    log.info("Creating backup %s ...", backup)
    client.query(f"CREATE TABLE {backup} AS SELECT * FROM {table}").result()
    backup_count = count(backup)
    if backup_count != pre_count:
        log.error(
            "ABORT: backup count %d != pre_count %d — possible partial backup",
            backup_count,
            pre_count,
        )
        client.query(f"DROP TABLE IF EXISTS {dedup_tmp}").result()
        return 1

    # Step 3: Replace main table
    log.info("Deleting all rows from %s ...", table)
    client.query(f"DELETE FROM {table} WHERE TRUE").result()

    log.info("Inserting %d deduplicated rows ...", dedup_count)
    client.query(f"INSERT INTO {table} SELECT * FROM {dedup_tmp}").result()

    # Step 4: Assert
    final_count = count(table)
    if final_count != dedup_count:
        log.error(
            "ABORT: final count %d != expected %d — table may be incomplete",
            final_count,
            dedup_count,
        )
        log.error("Rollback: INSERT INTO %s SELECT * FROM %s", table, backup)
        return 1

    # Step 5: Cleanup
    client.query(f"DROP TABLE IF EXISTS {dedup_tmp}").result()
    log.info(
        "Done. %s reduced from %d → %d rows (%d duplicates removed).",
        table,
        pre_count,
        final_count,
        pre_count - final_count,
    )
    log.info("Backup %s retained — drop manually after verification.", backup)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row counts without writing",
    )
    args = parser.parse_args()

    exit_code = run(args.project, args.dataset, args.dry_run)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
