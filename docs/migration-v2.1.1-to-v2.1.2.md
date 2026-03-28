# Migrating from v2.1.1 to v2.1.2

This release adds `domain` as a first-class column to the sitemap tables, fixes multi-domain
reporting bugs in all sitemap-related SQL views, adds 5 new analytical views, and exposes
`full_url` in every view that references a URL.

**If you are starting fresh** (no existing database), skip this guide — `initialize()` creates
the correct schema automatically.

---

## What Changed

### Schema changes

| Table | Change |
|---|---|
| `sitemap_urls` | New column `domain TEXT`. UNIQUE constraint changed from `(url_path)` to `(domain, url_path)` |
| `sitemap_freshness` | New column `domain TEXT`. UNIQUE constraint changed from `(url_path)` to `(domain, url_path)` |
| `url_volume_decay` | New column `domain TEXT`. UNIQUE constraint changed from `(url_path, period, period_start)` to `(domain, url_path, period, period_start)` |
| `url_performance` | New composite UNIQUE index on `(domain, request_date, url_path)` |

### View changes

All 10 existing dashboard views now include `domain` in `SELECT` and `GROUP BY`.
`v_url_cooccurrence` also gains a `full_url` column.
5 new views added — see the [dashboard guide](dashboard-guide.md).

### Bug fixes

- Sitemap JOINs were domain-blind in multi-domain setups (all views and aggregations).
- `_compute_decay_rates()` self-join was domain-blind, producing wrong decay rates.
- `v_daily_kpis.unique_urls_requested` was counting across all domains regardless of the domain filter.

---

## Migration Steps

### Step 1 — Run the sitemap domain migration

```bash
# Single database
python scripts/migrations/migrate_add_domain_to_sitemap_tables.py --db-path data/llm-bot-logs.db

# Multiple databases (glob)
python scripts/migrations/migrate_add_domain_to_sitemap_tables.py --db-path "data/*.db"

# Preview without applying
python scripts/migrations/migrate_add_domain_to_sitemap_tables.py --dry-run
```

This script:
- Adds `domain TEXT` to `sitemap_urls`, `sitemap_freshness`, and `url_volume_decay`
- Backfills `domain` on `sitemap_urls` from `sitemap_source` (strips `www.`)
- Backfills `domain` on `sitemap_freshness` from its `sitemap_source` column
- Leaves `url_volume_decay.domain = NULL` (repopulated in Step 5)

### Step 2 — Add url_performance unique index

```bash
python scripts/migrations/migrate_fix_url_performance_unique_key.py --db-path data/llm-bot-logs.db
```

### Step 3 — Deploy updated code

Pull/install the new version before running the sitemap pipeline. If you run the old code
after Step 1, new sitemap rows will be inserted with `domain = NULL`.

```bash
git pull && pip install -e .
```

### Step 4 — Re-fetch sitemaps

```bash
python scripts/run_aggregations.py --backend sqlite --sitemap-only
```

This populates `domain` on newly fetched rows using the updated code.

### Step 5 — Re-run SitemapAggregator

```bash
python scripts/run_aggregations.py --backend sqlite
```

This repopulates `url_volume_decay` with correct `domain` values (the column added in Step 1
had `NULL` because that table has no `sitemap_source` to backfill from).

### Step 6 — Recreate views

Views are recreated automatically the next time the pipeline runs `initialize()`. To force
immediate recreation:

```bash
python -c "
from llm_bot_pipeline.storage import get_backend
b = get_backend('sqlite', db_path='data/llm-bot-logs.db')
b.initialize()
b.close()
print('Views recreated.')
"
```

---

## BigQuery

For BigQuery deployments:

1. Run `ALTER TABLE` for each sitemap table:
   ```sql
   ALTER TABLE `project.dataset.sitemap_urls` ADD COLUMN IF NOT EXISTS domain STRING;
   ALTER TABLE `project.dataset.sitemap_freshness` ADD COLUMN IF NOT EXISTS domain STRING;
   ALTER TABLE `project.dataset.url_volume_decay` ADD COLUMN IF NOT EXISTS domain STRING;
   ```
2. Backfill from `sitemap_source`:
   ```sql
   UPDATE `project.dataset.sitemap_urls`
   SET domain = REGEXP_EXTRACT(sitemap_source, r'(?:https?://)?(?:www\.)?([^/]+)')
   WHERE domain IS NULL;
   ```
3. Re-run the sitemap pipeline to populate domain on new rows.
4. Run `create_views()` in Python to recreate all 15 views.

---

## Validation

After migration, verify domain is populated:

```sql
-- SQLite
SELECT domain, COUNT(*) FROM sitemap_urls GROUP BY domain;
SELECT domain, COUNT(*) FROM sitemap_freshness GROUP BY domain;

-- Verify new views exist
SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name;
-- Should list 15 views
```
