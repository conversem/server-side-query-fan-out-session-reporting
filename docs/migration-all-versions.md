# Upgrading from Older Versions

If you are running an older version and want to reach the latest, run the migration scripts
**in order**. Each migration is idempotent — safe to run even if already applied.

---

## Version Map

| From | To | Script(s) | Guide |
|---|---|---|---|
| v1.0.0 | v2.0.0 | `migrate_v1_to_v2.py` | [migration-v1-to-v2.md](migration-v1-to-v2.md) |
| v1.1.0 | v2.0.0 | `migrate_v1_to_v2.py` + `add_source_provider_column.py` (exists in `scripts/migrations/`) | [migration-v1-to-v2.md](migration-v1-to-v2.md) |
| v2.0.0 | v2.1.x | No schema migration required (v2.0 → v2.1 added `resource_type` with DEFAULT) | — |
| v2.1.1 | v2.1.2 | `migrate_add_domain_to_sitemap_tables.py` + `migrate_fix_url_performance_unique_key.py` | [migration-v2.1.1-to-v2.1.2.md](migration-v2.1.1-to-v2.1.2.md) |

---

## Upgrade Path by Starting Version

### Starting at v1.0.0 or v1.1.0 → latest

```bash
# Step 1: v1 → v2
python scripts/migrations/migrate_v1_to_v2.py --db-path data/llm-bot-logs.db

# Step 2: add source_provider column (v1.1 only — skip if on v1.0)
python scripts/migrations/add_source_provider_column.py --db-path data/llm-bot-logs.db

# Step 3: v2.1.1 → v2.1.2 (sitemap domain + url_performance index)
python scripts/migrations/migrate_add_domain_to_sitemap_tables.py --db-path data/llm-bot-logs.db
python scripts/migrations/migrate_fix_url_performance_unique_key.py --db-path data/llm-bot-logs.db
```

Then follow Steps 3–6 in [migration-v2.1.1-to-v2.1.2.md](migration-v2.1.1-to-v2.1.2.md)
(deploy new code, re-fetch sitemaps, re-run aggregator, recreate views).

### Starting at v2.0.0 or v2.1.1 → latest

```bash
python scripts/migrations/migrate_add_domain_to_sitemap_tables.py --db-path data/llm-bot-logs.db
python scripts/migrations/migrate_fix_url_performance_unique_key.py --db-path data/llm-bot-logs.db
```

Then follow Steps 3–6 in [migration-v2.1.1-to-v2.1.2.md](migration-v2.1.1-to-v2.1.2.md).

---

## Notes

- All scripts support `--dry-run` to preview changes without applying them.
- All scripts are idempotent — running them twice is safe.
- Multi-database glob patterns work on all scripts: `--db-path "data/*.db"`
- BigQuery users: see the BigQuery section in each migration guide.
