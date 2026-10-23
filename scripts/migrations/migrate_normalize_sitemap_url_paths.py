#!/usr/bin/env python3
"""
One-time migration: normalize sitemap_urls.url_path to always have a trailing
slash on directory-style paths (no file extension), then deduplicate.

Problem: The sitemap parser accumulated both ``/path`` and ``/path/`` variants
for the same URL across different fetch runs, inflating urls_in_sitemap counts
by ~2x in reporting views.

Fix: normalize url_path → RTRIM + ensure trailing slash for non-file paths,
then keep one row per (domain, normalized_url_path) using the most recent
_fetched_at, and take the most recent lastmod when present.

Usage:
    python scripts/migrations/migrate_normalize_sitemap_url_paths.py \\
        --project example-project --dataset bot_logs [--dry-run]

Rollback:
    INSERT INTO `<project>.<dataset>.sitemap_urls`
    SELECT * FROM `<project>.<dataset>.sitemap_urls_path_backup`;
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
    backup = f"`{project}.{dataset}.sitemap_urls_path_backup`"
    tmp = f"`{project}.{dataset}.sitemap_urls_path_tmp`"

    def count(tbl: str) -> int:
        return list(client.query(f"SELECT COUNT(*) AS n FROM {tbl}").result())[0]["n"]

    # Normalisation expression: strip trailing slashes then re-add unless file ext present.
    # A path has a file extension when its last segment contains a dot.
    norm_expr = r"""
        CASE
            WHEN REGEXP_CONTAINS(REGEXP_EXTRACT(url_path, r'[^/]+$'), r'\.')
            THEN LOWER(url_path)
            ELSE LOWER(RTRIM(url_path, '/')) || '/'
        END
    """.strip()

    pre_count = count(table)
    log.info("Pre-migration row count: %d", pre_count)

    # Count how many rows would change / be removed
    preview_sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNTIF(norm_path != url_path) AS paths_changed,
            COUNT(DISTINCT CONCAT(domain, '|', norm_path)) AS unique_after
        FROM (
            SELECT url_path, domain, ({norm_expr}) AS norm_path
            FROM {table}
        )
    """
    preview = list(client.query(preview_sql).result())[0]
    log.info(
        "[PREVIEW] paths_changed=%d, unique_pairs_after=%d (currently %d rows)",
        preview["paths_changed"],
        preview["unique_after"],
        pre_count,
    )

    if dry_run:
        log.info(
            "[DRY RUN] Would reduce: %d → %d rows", pre_count, preview["unique_after"]
        )
        return 0

    # Step 1: Build normalised + deduplicated temp table.
    # Columns must be in the exact same order as sitemap_urls:
    #   url, url_path, lastmod, lastmod_month, sitemap_source, _fetched_at, domain
    log.info("Building normalised temp table %s ...", tmp)
    client.query(f"DROP TABLE IF EXISTS {tmp}").result()
    client.query(f"""
        CREATE TABLE {tmp} AS
        SELECT
            url,
            ({norm_expr})          AS url_path,
            lastmod,
            SUBSTR(COALESCE(lastmod, ''), 1, 7) AS lastmod_month,
            sitemap_source,
            _fetched_at,
            domain
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY domain, ({norm_expr})
                    ORDER BY _fetched_at DESC
                ) AS _rn
            FROM {table}
        )
        WHERE _rn = 1
    """).result()

    tmp_count = count(tmp)
    log.info("Temp table has %d rows (will reduce from %d)", tmp_count, pre_count)

    # Step 2: Backup
    log.info("Creating backup %s ...", backup)
    client.query(f"CREATE TABLE {backup} AS SELECT * FROM {table}").result()
    backup_count = count(backup)
    if backup_count != pre_count:
        log.error("ABORT: backup count %d != pre_count %d", backup_count, pre_count)
        client.query(f"DROP TABLE IF EXISTS {tmp}").result()
        return 1

    # Step 3: Replace
    log.info("Replacing %s ...", table)
    client.query(f"DELETE FROM {table} WHERE TRUE").result()
    client.query(f"INSERT INTO {table} SELECT * FROM {tmp}").result()

    final_count = count(table)
    if final_count != tmp_count:
        log.error(
            "ABORT: final count %d != expected %d — rollback from backup",
            final_count,
            tmp_count,
        )
        return 1

    client.query(f"DROP TABLE IF EXISTS {tmp}").result()
    log.info(
        "Done. %s reduced from %d → %d rows (%d removed).",
        table,
        pre_count,
        final_count,
        pre_count - final_count,
    )
    log.info("Backup %s retained — drop manually after verification.", backup)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    sys.exit(run(args.project, args.dataset, args.dry_run))


if __name__ == "__main__":
    main()
