# Pipeline Architecture

## Overview

Multi-provider, multi-backend ETL pipeline for analyzing LLM bot traffic patterns across
websites. The pipeline ingests CDN/cloud provider logs, identifies LLM bot requests, groups
them into **Query Fan-Out Sessions** (bursts of requests from a single user question), and
produces analytics via Excel reports or database queries.

Two execution modes are supported:

1. **Local execution** — SQLite storage + Excel reports (zero cloud dependencies)
2. **Cloud-native (Enterprise)** — BigQuery storage + Looker Studio dashboards on GCP
   (available as a managed implementation; see [Enterprise](#enterprise-cloud-pipeline))

## Architecture Diagram

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   INGESTION     │ →  │  TRANSFORM   │ →  │    STORAGE      │
│                 │    │              │    │                 │
│ • Cloudflare    │    │ • Classify   │    │ • SQLite (local)│
│ • AWS ALB       │    │ • Enrich     │    │ • BigQuery (GCP)│
│ • AWS CloudFront│    │ • Deduplicate│    │                 │
│ • Azure CDN     │    │ • Validate   │    └────────┬────────┘
│ • GCP CDN       │    └──────────────┘             │
│ • Fastly        │                        ┌────────▼────────┐
│ • Akamai        │                        │    REPORTING     │
│ • Universal CSV │                        │                 │
└─────────────────┘                        │ • Sessions      │
  Logpull (via cloudflare/ module)         │ • Dashboards    │
                                           │ • Sitemap       │
                                           │ • Excel         │
                                           └────────┬────────┘
                                                    │
                                           ┌────────▼────────┐
                                           │   MONITORING     │
                                           │                 │
                                           │ • Data Quality  │
                                           │ • Retry Handler │
                                           └─────────────────┘
```

## Module Structure

```
src/llm_bot_pipeline/
├── ingestion/          Multi-provider log ingestion (8+ adapters)
├── pipeline/           ETL orchestration (local + streaming paths)
├── storage/            StorageBackend ABC + SQLite implementation
├── reporting/          Aggregations, sessions, sitemap, Excel export
├── monitoring/         Data quality checks, retry handling
├── research/           Advanced analytics (temporal, fingerprint, refinement)
├── cloudflare/         Cloudflare-specific (Logpull API)
├── sitemap/            Sitemap XML parsing and freshness analysis
├── config/             Settings, constants, SOPS loader
├── infrastructure/     Backend stubs (extensible for custom backends)
├── schemas/            Data schemas (raw, clean, bundles, reporting)
└── utils/              Bot classifier, URL/HTTP utilities
```

### `ingestion/` — Multi-Provider Log Ingestion

Pluggable adapter system for ingesting logs from any CDN provider. Each adapter normalizes
provider-specific formats into a universal `IngestionRecord` schema.

| Component | Purpose |
|-----------|---------|
| `base.py` | `IngestionAdapter` abstract base class |
| `registry.py` | Provider registry with auto-discovery |
| `security.py` | Path traversal protection, input sanitization |
| `validation.py` | Schema validation for ingested records |
| `file_utils.py` | Gzip auto-detection, file handling |
| `parsers/` | Format-specific parsers (CSV, JSON, W3C) |
| `providers/` | 8+ provider adapters (see [Provider Guides](ingestion/providers/)) |

### `pipeline/` — ETL Orchestration

Dual-mode transform pipeline: Python/Pandas for SQLite, extensible for other backends.

| Component | Purpose |
|-----------|---------|
| `router.py` | `run_pipeline()` factory — dispatches by processing mode |
| `local_pipeline.py` | SQLite-path ETL pipeline (Python/Pandas transforms) |
| `streaming_pipeline.py` | Streaming path (in-memory → output backend) |
| `python_transformer.py` | Pure-Python classifier and enrichment logic |
| `extract.py` | Data extraction from storage |
| `transform.py` | Transformation orchestration |
| `backfill.py` | Historical data backfill utilities |
| `stages/` | Individual pipeline stage modules |

### `storage/` — Storage Backends

| Component | Purpose |
|-----------|---------|
| `base.py` | `StorageBackend` ABC + `BackendCapabilities` flags |
| `factory.py` | `get_backend()` factory (lazy-loads implementations) |
| `sqlite_backend.py` | SQLite implementation with auto-migration |
| `sqlite_schemas.py` | DDL for all tables, indexes, and views |
| `disk_space.py` | Disk space monitoring utilities |

### `reporting/` — Analytics and Export

| Component | Purpose |
|-----------|---------|
| `excel_exporter.py` | Multi-sheet Excel workbook generation |
| `local_aggregations.py` | SQLite-based aggregation queries |
| `local_dashboard_queries.py` | Dashboard-style SQL queries for local use |
| `local_queries/` | Modular query library for local analytics |
| `session_aggregations.py` | Session-level rollups |
| `session_refiner.py` | MIBCS collision detection and session splitting |
| `session_storage_writer.py` | Writes refined sessions back to storage |
| `temporal_bundler.py` | Time-window-based session bundling |
| `sitemap_aggregations.py` | Sitemap coverage and freshness metrics |
| `freshness_tracker.py` | URL freshness tracking logic |
| `models.py` | Reporting data models |
| `reporting_utils.py` | Shared utilities for reporting |

### `config/` — Settings and Configuration

| Component | Purpose |
|-----------|---------|
| `settings.py` | `Settings` + `SessionRefinementSettings` dataclasses |
| `constants.py` | Bot classifications, field names, window defaults |
| `sops_loader.py` | SOPS-encrypted YAML config loader |

### `research/` — Advanced Analytics

| Component | Purpose |
|-----------|---------|
| `temporal_analysis.py` | Window size analysis and OptScore computation |
| `semantic_embeddings.py` | Sentence-transformer embeddings for session topics |
| `session_refinement.py` | Collision detection algorithms |
| `experiment_runner.py` | Reproducible window optimization experiments |
| `fingerprint_analysis.py` | IP/user-agent fingerprint analysis |
| `window_optimizer.py` | Grid search over temporal window parameters |

## Storage Backend Architecture

The storage layer uses an abstract base class (`StorageBackend`) with capability flags
that let pipeline code choose the correct execution path at runtime.

### `BackendCapabilities` Flags

```python
@dataclass(frozen=True)
class BackendCapabilities:
    supports_sql: bool = True           # SQL query/execute interface
    supports_streaming: bool = False    # Streaming reads
    supports_partitioning: bool = False # Date partitioning + clustering
    supports_transactions: bool = True  # Transaction support
    supports_upsert: bool = False       # Native MERGE/upsert
    parameter_style: str = "named"      # :param, %(param)s, or @param
```

### Dual Interface

Every backend exposes two access patterns:

| Interface | Methods | Use Case |
|-----------|---------|----------|
| **SQL interface** | `query()`, `execute()` | SQL-native workflows, DDL |
| **Record interface** | `insert_records()`, `read_records()` | Pandas-based bulk operations |

### Factory Pattern

Backends are created via `get_backend()` which lazy-loads implementations:

```python
from llm_bot_pipeline.storage.factory import get_backend

# From settings (auto-detects backend type from config)
backend = get_backend()

# Explicit SQLite
backend = get_backend("sqlite", db_path="data/logs.db")
```

## Configuration Resolution

Settings are resolved via a 2-tier fallback chain (open source):

```
┌──────────────────────────────┐
│ 1. SOPS-Encrypted YAML       │  Local (config.enc.yaml)
│    Decrypted via Age keys    │
└──────────┬───────────────────┘
           │ fallback
┌──────────▼───────────────────┐
│ 2. Environment Variables      │  CI/CD, containers, quick overrides
│    STORAGE_BACKEND, etc.     │
└──────────────────────────────┘
```

Each tier produces a `Settings` dataclass with all pipeline configuration.

See [SOPS Quick Start](sops/quickstart.md) for local encrypted config setup.

## Execution Modes

### 1. Local Execution (SQLite + Excel)

Zero cloud dependencies. Data is stored in SQLite databases and reports are exported
as multi-sheet Excel workbooks.

```bash
# Ingest logs from any provider
python scripts/ingest_logs.py --provider cloudflare --input ./logs/

# Run ETL pipeline
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07

# Export Excel report
python scripts/export_session_report.py --format xlsx --output report.xlsx
```

**Database layout**: Each domain gets its own SQLite file in `data/`. Multi-domain mode
uses `scripts/run_multi_domain.py` with domain configs from `config.enc.yaml`.
See [Query Fan-Out Sessions](query-fanout-sessions.md).

### 2. Local BigQuery Modes

Push data from your local machine to BigQuery without cloud infrastructure:

- **`local_bq_buffered`** — SQLite for raw data, BigQuery as output backend
- **`local_bq_streaming`** — In-memory transforms, stream directly to BigQuery

```bash
pip install ".[gcp]"
export PROCESSING_MODE=local_bq_streaming
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07
```

## Enterprise Cloud Pipeline

The enterprise tier adds a fully-managed cloud mode running on GCP Cloud Run Jobs
with BigQuery as the backend and Looker Studio for dashboards.

[Contact us](https://conversem.com/contact/) for enterprise implementation details.

## Data Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│                     Multi-Provider Log Sources                        │
├───────────┬───────────┬───────────┬───────────┬───────────┬──────────┤
│ Cloudflare│ AWS ALB   │ CloudFront│ Azure CDN │ Fastly    │ Akamai   │
└─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┴─────┬─────┴────┬────┘
      │           │           │           │           │          │
      └───────────┴───────────┴─────┬─────┴───────────┴──────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │  Ingestion Adapters  │  Normalize to universal schema
                         └──────────┬───────────┘
                                    │
                      ┌─────────────┼─────────────┐
                      │             │             │
              ┌───────▼─────┐ ┌────▼────┐ ┌──────▼──────┐
              │ SQLite Path │ │  Both   │ │ Streaming   │
              │ Pandas xform│ │ Shared: │ │ BQ path     │
              └──────┬──────┘ │ classify│ └──────┬──────┘
                     │        │ enrich  │        │
                     │        └─────────┘        │
                     │                           │
              ┌──────▼──────────────────────────▼──┐
              │      Session Aggregation            │
              │  1. Temporal bundling (100ms window) │
              │  2. Semantic refinement (collisions) │
              │  3. Summary + URL details            │
              │  Each session = exactly one domain   │
              └──────┬──────────────────────┬───────┘
                     │                      │
              ┌──────▼──────┐        ┌──────▼──────┐
              │ Excel Export │        │ BigQuery    │
              │ 7+ sheets   │        │ (local BQ   │
              │ Sitemap data │        │  modes)     │
              └─────────────┘        └─────────────┘
```

## Research Methodology

The core research component determines optimal time windows for bundling LLM bot requests
into Query Fan-Out Sessions using the **OptScore** composite metric:

```
OptScore = α·MIBCS + β·Silhouette + γ·BPS - δ·SingletonRate - ε·GiantRate - ζ·ThematicVariance
```

| Component | Weight | Description |
|-----------|--------|-------------|
| MIBCS | α=0.30 | Mean Intra-Bundle Cosine Similarity |
| Silhouette | β=0.25 | Cluster separation quality |
| BPS | γ=0.25 | Bundle Purity Score |
| SingletonRate | δ=0.10 | Penalty for single-request bundles |
| GiantRate | ε=0.05 | Penalty for oversized bundles |
| ThematicVariance | ζ=0.05 | Penalty for thematic inconsistency |

**Key findings**: The optimal default window is **100ms** (captures 91%+ of burst gaps
with 93.9% high-confidence sessions). A tighter 50ms window yields 94.6% high confidence
for use cases requiring higher coherence.

After temporal bundling, **session refinement** detects and splits collision bundles —
cases where multiple independent queries were accidentally merged.

**Domain isolation**: Every query fan-out session belongs to exactly one domain. Both
temporal bundling and semantic splitting preserve this invariant. See
[Query Fan-Out Sessions](query-fanout-sessions.md) for the full design rationale.

## Design Principles

- **Modularity** — Each module has a single responsibility. Small, focused files.
- **Pluggable** — Ingestion adapters, storage backends, and reporting outputs are all swappable.
- **Research-first** — Core algorithms isolated in `research/`. Configurable parameters.
  Reproducible results.
- **LLM/AI friendly** — Small context per file, clear naming, self-documenting structure.
