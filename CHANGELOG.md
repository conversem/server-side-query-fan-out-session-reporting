# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.3] - 2026-04-01

### Fixed

- **Duplicate sitemap rows inflating freshness/decay views** — repeated sitemap ingestion runs
  accumulated duplicate entries in `sitemap_urls`, causing all freshness and decay views to
  overcount URLs and request volume by 13–16×. Fixed in two places:
  - `v_url_freshness`, `v_url_freshness_detail`, `v_sessions_by_content_age`, and
    `v_url_performance_with_freshness` now open with a `WITH dedup_sitemap AS (SELECT DISTINCT ...)`
    CTE so duplicate rows in the table do not affect view output.
  - BigQuery `insert_sitemap_urls()` now performs a domain-scoped `DELETE` before each bulk insert
    (truncate-then-reload per domain), preventing future accumulation.

### Added

- **`migrate_dedup_sitemap_urls.py`** — one-time BigQuery migration to collapse existing duplicate
  rows in `sitemap_urls` down to one row per `(domain, url_path)`, keeping the most recent
  `_fetched_at`. Includes dry-run mode, row count verification, and a backup table for rollback.
  Run this if you used the sitemap pipeline before v2.1.3.

---

## [2.1.2] - 2026-03-26

### Added

- **`domain` column in sitemap tables** — `sitemap_urls`, `sitemap_freshness`, and `url_volume_decay`
  now include a `domain TEXT` column. `UNIQUE` constraints updated to `(domain, url_path)`, enabling
  correct multi-domain reporting when multiple websites share the same URL paths.
- **5 new analytical views** — `v_decay_unique_urls_by_domain`, `v_decay_request_volume_by_domain`,
  `v_url_freshness_detail`, `v_sessions_by_content_age`, `v_url_performance_with_freshness`.
  Total view count: 15. See `docs_public/dashboard-guide.md` for usage.
- **`full_url` column in URL views** — `v_url_cooccurrence` and all new URL-exposing views now
  include `CONCAT('https://', domain, url_path) AS full_url` for clickable, unambiguous URLs in
  Looker Studio or any BI tool.
- **`domain` dimension in all 10 existing dashboard views** — all views updated to include `domain`
  in SELECT and GROUP BY, enabling per-domain filtering and rollup without code changes.
- **`idx_url_performance_natural_key`** — composite UNIQUE index on `url_performance(domain,
  request_date, url_path)` to prevent duplicate rows from re-aggregation runs.
- **Migration scripts** — `scripts/migrations/migrate_add_domain_to_sitemap_tables.py` and
  `scripts/migrations/migrate_fix_url_performance_unique_key.py`; both idempotent and
  dry-run capable. See `docs_public/migration-v2.1.1-to-v2.1.2.md`.
- **Dashboard guide** — `docs_public/dashboard-guide.md`: complete reference for building
  Looker Studio dashboards, including which views to use, `full_url` as clickable link,
  domain filter setup, decay curve caveats, and suggested page structure.

### Fixed

- **Sitemap JOIN cross-contamination** — `aggregate_freshness()`, `aggregate_volume_decay()`,
  and all decay views previously joined `sitemap_urls` to session data on `url_path` only.
  In multi-domain setups this caused sessions from domain A to match sitemap entries from
  domain B. All JOINs now include `AND sud.domain = sm.domain`.
- **`_compute_decay_rates()` self-join** — the decay rate self-join previously matched rows
  on `url_path + period` only, producing wrong `prev_request_count` when two domains share
  a URL path. Fixed to include `AND cur.domain = prev.domain`.
- **`v_daily_kpis` unique_urls_requested** — the subquery/CTE for unique URLs was domain-blind,
  returning the all-domain count on every per-domain row. Rewritten as a domain-scoped CTE
  with a `LEFT JOIN` on `(session_date, domain)`.

### Changed

- **`sitemap_source` backfill in `sitemap_freshness`** — this table already had `sitemap_source`
  and is now also included in the domain backfill migration, alongside `sitemap_urls`.
  `url_volume_decay` (no `sitemap_source`) is repopulated by re-running `SitemapAggregator`.

## [2.1.2] - 2026-03-25

### Added

- **`domain` column on all sitemap tables** (`sitemap_urls`, `sitemap_freshness`,
  `url_volume_decay`) — enables correct per-domain reporting in multi-domain setups.
  UNIQUE constraints updated to be domain-scoped.
- **5 new SQLite analytical views** — `v_sitemap_domain_coverage`,
  `v_sitemap_freshness_by_domain`, `v_sitemap_url_age`, `v_decay_by_domain`,
  `v_url_volume_trend`; total view count raised to 15.
- **`full_url` column** exposed in `v_url_cooccurrence` and all views that reference
  a URL path.
- **Migration scripts** for upgrading existing databases:
  - `scripts/migrations/migrate_add_domain_to_sitemap_tables.py` — adds `domain`
    column and backfills from `sitemap_source`; supports glob patterns for
    multi-database setups.
  - `scripts/migrations/migrate_fix_url_performance_unique_key.py` — adds composite
    UNIQUE index on `(domain, request_date, url_path)`.
- **Dashboard guide** (`docs/dashboard-guide.md`) — documents all 15 SQLite views
  with example queries.
- **Unified migration index** (`docs/migration-all-versions.md`) — one place to find
  all migration guides across all releases.

### Fixed

- Sitemap JOINs were domain-blind in multi-domain setups — all views and aggregations
  now filter and group by `domain`.
- `_compute_decay_rates()` self-join produced wrong decay rates across domains.
- `v_daily_kpis.unique_urls_requested` was counting across all domains regardless of
  domain filter.

### Changed

- All 10 existing dashboard views now include `domain` in `SELECT` and `GROUP BY`.

## [2.1.1] - 2026-03-13

### Changed

- **Cloudflare Logpull/Logpush** — removed legacy Enterprise Bot Management fields
  (`bot_score`, `is_verified_bot`, `BotScore`, `VerifiedBot`) from all schemas, pipeline
  stages, and storage backends; `build_llm_bot_filter()` now returns `{}` (no pre-filter);
  LLM bot classification is done entirely post-ingestion via user-agent pattern matching.
  Resolves a deployment failure caused by referencing unavailable Enterprise fields.
- **`filters.py`** — `build_llm_bot_filter()` documented as intentional no-op; Cloudflare
  API does not support string-contains on user-agent, so filtering is applied post-ingest.

## [2.1.0] - 2026-03-13

### Added

- **URL resource type filtering** (`utils/url_classifier.py`) — classifies URLs before
  storage: JS, CSS, fonts, and static asset paths are dropped; image files are kept
  with `resource_type = "image"`; everything else is `resource_type = "document"`.
  Filtering runs consistently across all pipeline modes (`local_sqlite`,
  `local_bq_buffered`, `local_bq_streaming`). Fully configurable via `config.yaml`
  or environment variables; can be disabled entirely.
- **`resource_type` column in `bot_requests_daily`** — existing rows default to
  `"document"`; new rows are classified on ingest. No backfill required.
- **URL filtering documentation** — `docs/url-filtering.md` with configuration
  reference, SQL usage examples, and migration notes.

### Changed

- **`config.example.yaml`** — new `url_filtering` section with all configurable
  extensions and path prefixes documented
- **Streaming pipeline** — backpressure mechanism prevents unbounded memory growth
  under high throughput; dedup set resets per date boundary
- **SQLite backend** — `VACUUM` after large deletes to reclaim disk space;
  improved error propagation instead of silent failures
- **Settings** — `Settings.validate()` raises `ConfigurationError` on invalid
  config; secret-redacting `__repr__` to prevent accidental log leaks
- **Pipeline stages** — order-by parameter validation; table/column name whitelist
  to prevent SQL injection; specific `StorageError`/`PipelineError` exceptions
  replace broad `Exception` handlers
- **Logging** — structured JSON logging for cloud environments; error logs enriched
  with contextual information
- **CI** — bandit security scanner and pip-audit added to pre-commit hooks and
  GitHub Actions workflow

## [2.0.0] - 2026-03-12

Third public release. Major expansion of reporting, pipeline modes, and database
schema — with Excel reporting as a centrepiece: full multi-sheet workbooks covering
sessions, URL-level performance, daily KPIs, and showing how sitemap lastmod date freshness correlate strongly with LLM preference; the more recent the page is updated the more likely it is fetched by e.g. ChatGPT-User.

### Added

- **Multi-sheet Excel reporting** (`excel_exporter.py`) — export full workbooks with
  sheets for sessions, URL performance, daily KPIs, sitemap freshness, and raw requests;
  configurable sheet selection per export
- **Sitemap integration** (`sitemap/` module) — fetch and parse XML sitemaps, track
  URL coverage, freshness by `lastmod` month, decay rates, and pct. of sitemap
  requested per period; drives sitemap sheets in Excel and `v_url_freshness` SQLite view
- **11 SQLite analytical views** (`sqlite_schemas.py`) — `v_session_url_distribution`,
  `v_session_singleton_binary`, `v_bot_volume`, `v_top_session_topics`, `v_daily_kpis`,
  `v_category_comparison`, `v_url_cooccurrence`, `v_url_freshness`, `v_decay_unique_urls`,
  `v_decay_request_volume`
- **Pipeline router** (`pipeline/router.py`) — unified `run_pipeline()` dispatcher with
  three modes: `local_sqlite` (default), `local_bq_buffered` (SQLite → BigQuery),
  `local_bq_streaming` (in-memory → BigQuery)
- **Streaming pipeline** (`streaming_pipeline.py`) — memory-efficient streaming path to
  BigQuery without intermediate SQLite storage
- **Backfill pipeline** (`backfill.py` + `checkpoint.py`) — date-range backfill with
  checkpoint/resume support for large historical ingestion jobs
- **Session refinement module** — MIBCS collision detection splits accidentally merged
  Query Fan-Out Sessions; `session_refinement_log` table audits all refinement runs
- **Session URL details table** — flattened URL-level data per session enabling per-URL
  performance breakdowns in Excel and SQL
- **URL volume decay tracking** — `url_volume_decay` table with `v_decay_*` views for
  monitoring URL request trends over time
- **Sitemap freshness tables** — `sitemap_urls` and `sitemap_freshness` tables populated
  from configured sitemap sources
- **Multi-domain support** (`run_multi_domain.py`) — per-domain SQLite databases with
  domain column propagated through all tables
- **SQLite v1→v2 migration script** — `scripts/migrations/migrate_v1_to_v2.py`;
  idempotent, dry-run capable, handles all new columns and tables
- **Auto-migration in SQLiteBackend** — `initialize()` adds missing columns when
  connecting to a v1 database; transparent upgrade path
- **Fingerprint analysis** (`research/fingerprint_analysis.py`) — IP homogeneity and
  collision candidate detection for session quality assessment
- **GitHub Actions CI workflow** — lint (black, isort), pytest, bandit, pip-audit;
  runs on push and pull requests
- **Data quality monitoring** (`monitoring/data_quality.py`) — schema and row-count checks
- **Daily pipeline script** (`scripts/daily_pipeline.sh`) — shell wrapper for cron/scheduler use

### Changed

- **Storage schemas** extracted to `sqlite_schemas.py` — all DDL centralised; backend
  no longer embeds CREATE TABLE/VIEW/INDEX statements inline
- **Session bundling** — temporal window default remains 100ms; now fully configurable
  via settings with research-backed justification
- **Configuration** — SOPS-encrypted YAML config with env var fallback chain;
  `config.example.yaml` updated with sitemap, multi-domain, and BigQuery mode options
- **pyproject.toml** — GCP dependencies in `[gcp]` optional extra; new optional
  groups: `ml`, `monitoring`, `viz`
- **Test suite** expanded — unit tests for pipeline router, streaming pipeline,
  session refiner, sitemap parser, SQL utils, storage factory, and more

### Fixed

- SQLite `initialize()` handles both fresh databases and upgrades from v1 schema
  without data loss

## [1.1.0] - 2025-12-25

Second public release. Added native log ingestion adapters for 8 CDN/cloud providers,
removing the requirement to export logs manually via the Cloudflare API.

### Added

- **8 ingestion providers** — Cloudflare (file export), AWS CloudFront (W3C format),
  AWS ALB (space-separated access logs), Azure CDN / Front Door, Google Cloud CDN,
  Fastly, Akamai DataStream, and Universal (CSV/JSON/NDJSON)
- **Modular provider architecture** — `IngestionAdapter` ABC with auto-discovery
  registry; add new providers by implementing one class
- **Auto-detection** of provider format from file content signatures
- **Gzip decompression** support for all adapters (`.gz` files)
- **Streaming parser** for large files (>1 GB) without loading into memory
- **Input sanitization and security** — path traversal protection, rate limiting,
  schema validation, configurable size limits
- **690+ tests** with 70%+ coverage; full fixture sets per provider
- **Provider documentation** in `docs/ingestion/providers/` — per-provider export
  instructions and format references
- **`add_source_provider_column` migration** — adds `source_provider` column to
  existing v1.0 databases

## [1.0.0] - 2025-12-20

Initial public release accompanying the research article
[The Query Fan-Out Session](https://conversem.com/the-query-fan-out-session/).

### Added

- **Query Fan-Out Session detection** — temporal bundling at optimal 100ms window,
  validated on hold-out data; defines the core methodology
- **Research module** — `temporal_analysis`, `semantic_embeddings` (TF-IDF + Transformer
  URL tokenisation), `window_optimizer` (OptScore composite metric), `experiment_runner`
  with full train/validate/report protocol
- **Local SQLite pipeline** — full ETL from Cloudflare Logpull API to normalised
  session database; incremental and full-refresh modes
- **Session and URL performance reporting** — `session_aggregations`, `local_dashboard_queries`,
  `local_aggregations`; SQL-based analytics without BigQuery
- **Storage factory pattern** — `StorageBackend` ABC with pluggable backends;
  `SqliteBackend` ships with the open-source release
- **Basic Excel report export** — single-sheet per-table export via `export_session_report.py`
- **Bot user-agent classification** — `bot_classifier` with provider mapping
- **Cloudflare Logpull integration** — `cloudflare/logpull.py` fetches logs via API
- **SOPS-encrypted configuration** — Age key support with `sops_loader`; `config.example.yaml`
- **Sample data and research scripts** — `generate_sample_data.py`,
  `analyze_temporal_patterns.py`, `run_window_experiment.py` for reproducing findings
