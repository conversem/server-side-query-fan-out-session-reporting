# Migrating from v1 to v2

This guide covers upgrading an existing SQLite database from the original v1 schema (first public release) to the v2 schema used in the current release.

**If you are starting fresh** (no existing database), skip this guide — the pipeline creates the correct schema automatically on first run.

---

## What Changed in v2

### New Columns (existing tables)

| Table | Column | Type | Description |
|-------|--------|------|-------------|
| `raw_bot_requests` | `domain` | TEXT | Hostname this request belongs to |
| `raw_bot_requests` | `RayID` | TEXT | Cloudflare Ray ID for request tracing |
| `bot_requests_daily` | `domain` | TEXT | Hostname this aggregate belongs to |
| `daily_summary` | `domain` | TEXT | Hostname this summary belongs to |
| `url_performance` | `domain` | TEXT | Hostname this URL belongs to |
| `query_fanout_sessions` | `domain` | TEXT | Hostname this session belongs to |
| `query_fanout_sessions` | `splitting_strategy` | TEXT | Strategy used to split this session |
| `query_fanout_sessions` | `parent_session_id` | TEXT | Parent session ID for refined sessions |
| `query_fanout_sessions` | `was_refined` | INTEGER | `1` if session was refined, else `0` |
| `query_fanout_sessions` | `refinement_reason` | TEXT | Reason a session was re-analysed |
| `query_fanout_sessions` | `pre_refinement_mibcs` | REAL | MIBCS score before refinement |

### New Tables, Views, and Indexes

v2 also introduces new tables, views, and indexes. These are created automatically when you run the migration script (which calls `initialize()` internally).

---

## Migration Options

### Option A — Automatic (recommended)

The pipeline's `SQLiteBackend.initialize()` **auto-detects and upgrades v1 databases** when you run the pipeline for the first time after updating. No manual action is required for single-database setups.

```bash
# Just run the pipeline as normal; it will migrate automatically
python scripts/run_pipeline.py --mode sqlite
```

The auto-migration:
- Detects v1 schema via sentinel column (`domain` missing from `raw_bot_requests`)
- Adds all missing columns idempotently
- Does **not** modify existing data
- Logs a summary of columns added

### Option B — Manual Script (recommended for production / multi-domain)

For production environments, scheduled migrations, or multi-domain setups where you want explicit control:

```bash
# Single database
python scripts/migrations/migrate_v1_to_v2.py --db-path data/llm-bot-logs.db

# Multi-domain (glob pattern)
python scripts/migrations/migrate_v1_to_v2.py --db-path "data/*.db"

# Dry-run first (no changes made)
python scripts/migrations/migrate_v1_to_v2.py --db-path data/llm-bot-logs.db --dry-run

# With domain backfill (single-domain users — see below)
python scripts/migrations/migrate_v1_to_v2.py --db-path data/llm-bot-logs.db --backfill-domain
```

---

## Script Reference

```
usage: migrate_v1_to_v2.py [-h] [--db-path PATH] [--dry-run]
                            [--backfill-domain] [--verbose]

Options:
  --db-path PATH     Path to database (glob supported, default: data/llm-bot-logs.db)
  --dry-run          Report what would change, make no modifications
  --backfill-domain  Populate domain from ClientRequestHost for existing rows
  --verbose          Log each column check
```

**Exit codes:**
- `0` — all databases migrated successfully (or already at v2)
- `1` — one or more databases failed to migrate

---

## Domain Backfill

The `domain` column is `NULL` for all rows after migration, because v1 did not record which hostname a request belonged to.

If you only ever analysed **one domain**, you can backfill the column from the `ClientRequestHost` field:

```bash
python scripts/migrations/migrate_v1_to_v2.py \
    --db-path data/llm-bot-logs.db \
    --backfill-domain
```

This sets `domain = ClientRequestHost` for all rows where `domain IS NULL`.

If you analysed **multiple domains** from a single database in v1, the `domain` column will remain `NULL` for historical rows. New rows ingested after migration will have `domain` populated correctly.

---

## Multi-Domain Setup

If you have separate databases per domain, migrate them all at once using a glob:

```bash
python scripts/migrations/migrate_v1_to_v2.py \
    --db-path "data/*.db" \
    --verbose
```

The script logs each database separately and reports a final count of succeeded/failed migrations.

---

## Verification

After migration, verify the schema was upgraded correctly:

```bash
sqlite3 data/llm-bot-logs.db ".schema raw_bot_requests" | grep domain
# Expected: "domain" TEXT,
```

Or run a quick count:

```bash
sqlite3 data/llm-bot-logs.db "PRAGMA table_info(raw_bot_requests);"
# Should include rows for domain and RayID
```

---

## Rollback

SQLite does not support removing columns (before v3.35). The migration **only adds** columns — it never modifies or removes existing data. Rollback is not needed in most cases; the new columns are `NULL`-able and do not break existing queries.

If you need to revert to v1, restore from a backup taken before migration.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Database not found` error | Wrong `--db-path` | Check the path, use an absolute path if needed |
| Migration reports `Already at v2 schema` | Database was already migrated | No action needed |
| Columns missing after `--dry-run` | Dry-run does not apply changes | Re-run without `--dry-run` |
| `OperationalError: no such column: domain` after migration | Pipeline ran before migration completed | Re-run migration, then restart pipeline |
| New tables/views not created | `initialize()` import failed (no venv) | Run `source venv/bin/activate` first, then migrate |
