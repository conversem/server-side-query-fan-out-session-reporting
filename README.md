# Server-Side Query Fan-Out Session monitoring & Reporting

[![License: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/conversem/server-side-query-fan-out-session-reporting/actions/workflows/ci.yml/badge.svg)](https://github.com/conversem/server-side-query-fan-out-session-reporting/actions)

A research-backed pipeline for **server-side LLM activity tracking**, built around the
**Query Fan-Out Session** methodology — a new way to measure how your content answers real
user questions in AI interfaces.

📄 **Read the research article:** [The Query Fan-Out Session: Server-side Query Fan-Out Tracking](https://conversem.com/the-query-fan-out-session/)

---

## What is a Query Fan-Out Session?

A **Query Fan-Out Session** is a bundle of web requests from an LLM chat assistant that
originated from a single user question. When LLM-powered services (like ChatGPT, Perplexity,
or Claude) process a query, they fan out multiple rapid requests to gather information —
often 4–5 requests within 10–20ms from a single user question. By detecting these bursts,
we can group requests into meaningful sessions that represent actual user interactions.

This turns "500 ChatGPT requests today" into "your content answered approximately 220 AI-assisted
user questions today" — a fundamentally more useful signal for content strategy and GEO measurement.

**Key research findings** (validated on production server logs):

| Observation | Value |
|---|---|
| Most common gap between requests | **9ms** |
| Median gap between requests | **10ms** |
| 84% of all request gaps | **≤ 20ms** |
| 90% of all request gaps | **≤ 53ms** |
| Sessions with high thematic coherence at 100ms window | **91%+** |
| Over-bundling rate at 100ms window | **0.04%** |
| Validation ranking agreement on hold-out data | **100%** |

**Recommendation: 100ms as the standard bundling window.** Both OpenAI and Perplexity show
consistent burst patterns (17–22ms median). The 100ms threshold captures genuine query fan-outs
while virtually eliminating false merges between unrelated queries.

**Multi-page sessions reveal topical authority.** When an LLM pulls 2–4 pages from your site
in a single fan-out, it indicates the AI found multiple relevant pieces of content for one
question — a strong topical authority signal. The grouped URLs also reveal the decision journey
the LLM is composing an answer for.

This framework allows you to reproduce the research and apply it to your own server logs:

1. **Ingest** CDN logs from 8+ providers (Cloudflare, AWS ALB, AWS CloudFront, Azure CDN,
   GCP CDN, Fastly, Akamai, Universal CSV/JSON/NDJSON) or via the Cloudflare Logpull API
2. **Identify** request bursts using temporal bundling with the 100ms window
3. **Refine** sessions with MIBCS collision detection — splits accidentally merged queries
   using graph-based semantic analysis
4. **Report** Query Fan-Out Sessions in multi-sheet Excel workbooks with KPIs, URL-level
   performance, daily trends, and sitemap freshness

---

## v2.0: Production-Ready Pipeline

Version 2.0 upgrades the framework from a research prototype to a modular production pipeline:

- **8+ ingestion providers** — Cloudflare, AWS ALB, AWS CloudFront, Azure CDN, GCP CDN,
  Fastly, Akamai, and Universal CSV/JSON/NDJSON
- **Modular plugin architecture** — `IngestionAdapter` ABC with auto-discovery registry;
  add new providers by implementing one class
- **SQLite local analysis** — zero-config local storage with full schema including
  sessions, sitemap data, and analytics views
- **Multi-domain support** — per-domain SQLite databases; domain column throughout
- **MIBCS collision detection** — semantic refinement splits accidentally merged sessions
- **Multi-sheet Excel reporting** — sessions, URL performance, daily KPIs, sitemap freshness
- **Sitemap freshness tracking** — URL coverage analysis and decay rate monitoring
- **Local BigQuery modes** — push data from your machine to BigQuery without cloud
  infrastructure (`local_bq_buffered`, `local_bq_streaming`)
- **Database migration tooling** — idempotent v1→v2 schema migration with dry-run support
- **Research module** — reproduce the OptScore window experiments on your own data

---

## Enterprise: Managed Cloud Pipeline

For organizations needing production-grade LLM traffic analysis at scale:

| Feature | Open Source | Enterprise |
|---------|:-----------:|:----------:|
| 8+ ingestion providers | ✓ | ✓ |
| SQLite local analysis | ✓ | ✓ |
| Multi-domain (SQLite per domain) | ✓ | ✓ |
| Session analysis (MIBCS) | ✓ | ✓ |
| Excel reporting | ✓ | ✓ |
| Local BigQuery modes | ✓ | ✓ |
| **GCP BigQuery cloud pipeline** | — | ✓ |
| **Cloud Run automated scheduling** | — | ✓ |
| **Looker Studio dashboards** | — | ✓ |
| **Multi-domain cloud orchestration** | — | ✓ |
| **Monitoring & alerting** | — | ✓ |
| **Dedicated support** | — | ✓ |

[Contact us](https://conversem.com/contact/) for enterprise pricing and implementation.

---

## Quick Start

### Prerequisites

- Python 3.10+
- 2 GB+ free disk space for logs and database

### Option A: Local SQLite + Excel (zero cloud dependencies)

```bash
git clone https://github.com/conversem/server-side-query-fan-out-session-reporting.git
cd server-side-query-fan-out-session-reporting

python -m venv venv
source venv/bin/activate        # Linux/Mac
# .\venv\Scripts\Activate.ps1  # Windows PowerShell

pip install .
# For session refinement (ML): pip install ".[ml]"

# Configure
cp config.example.yaml config.yaml
# Edit config.yaml with your settings, then encrypt:
sops -e config.yaml > config.enc.yaml && rm config.yaml

# Ingest logs
python scripts/ingest_logs.py --provider cloudflare --input ./logs/

# Run ETL pipeline
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07

# Export Excel report
python scripts/export_session_report.py --output data/reports/sessions.xlsx
```

### Option B: Docker

```bash
cp config.example.yaml config.yaml
# Edit config.yaml, then encrypt with SOPS

docker compose run --rm app \
  python scripts/ingest_logs.py --provider cloudflare --input ./logs/
docker compose run --rm app \
  python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07
```

### Option C: Local BigQuery modes

Push data from your machine to BigQuery without any cloud infrastructure:

```bash
pip install ".[gcp]"

export PROCESSING_MODE=local_bq_streaming

python scripts/ingest_logs.py --provider cloudflare --input ./logs/
python scripts/run_pipeline.py --start-date 2025-01-01 --end-date 2025-01-07
```

---

## Ingestion Providers

| Provider | Format | Configuration |
|----------|--------|---------------|
| **Cloudflare** | Logpull API / Logpush files | `--provider cloudflare` |
| **AWS ALB** | Access log format | `--provider aws_alb` |
| **AWS CloudFront** | W3C extended log format | `--provider aws_cloudfront` |
| **Azure CDN** | Access log format | `--provider azure_cdn` |
| **GCP Cloud CDN** | HTTP/S load balancer logs | `--provider gcp_cdn` |
| **Fastly** | Access log format | `--provider fastly` |
| **Akamai** | Datastream 2 format | `--provider akamai` |
| **Universal** | CSV / JSON / NDJSON | `--provider universal` |

See [docs/ingestion/](docs/ingestion/README.md) for provider-specific guides.

---

## Pipeline Modes

| Mode | Storage | Compute | Requirements |
|------|---------|---------|-------------|
| `local_sqlite` | SQLite | Local Python | None (default) |
| `local_bq_buffered` | SQLite → BigQuery | Local Python | GCP project |
| `local_bq_streaming` | Memory → BigQuery | Local Python | GCP project |

```bash
export PROCESSING_MODE=local_sqlite   # default
```

---

## Multi-Domain Analysis

Run independent analysis for multiple websites with isolated databases:

```bash
python scripts/run_multi_domain.py \
  --config config.enc.yaml \
  --start-date 2025-01-01 \
  --end-date 2025-01-07
```

Each domain gets its own SQLite database (`data/{domain}.db`). Domain is tracked as a column
in every table.

---

## Upgrading from v1

If you have an existing v1 database, run the migration script:

```bash
python scripts/migrations/migrate_v1_to_v2.py --db-path data/llm-bot-logs.db

# Dry run first
python scripts/migrations/migrate_v1_to_v2.py --db-path data/llm-bot-logs.db --dry-run
```

See [docs/migration-v1-to-v2.md](docs/migration-v1-to-v2.md) for the full upgrade guide.

---

## Documentation

| Topic | Link |
|-------|------|
| Architecture overview | [docs/architecture.md](docs/architecture.md) |
| Processing modes | [docs/processing-modes.md](docs/processing-modes.md) |
| Storage backend guide | [docs/backend-guide.md](docs/backend-guide.md) |
| Ingestion providers | [docs/ingestion/](docs/ingestion/README.md) |
| SOPS configuration | [docs/sops/quickstart.md](docs/sops/quickstart.md) |
| Session methodology | [docs/query-fanout-sessions.md](docs/query-fanout-sessions.md) |
| Excel reporting | [docs/reporting-excel.md](docs/reporting-excel.md) |
| Sitemap analysis | [docs/sitemap-analysis.md](docs/sitemap-analysis.md) |
| Security | [docs/security.md](docs/security.md) |
| v1→v2 migration | [docs/migration-v1-to-v2.md](docs/migration-v1-to-v2.md) |
| Contributing | [CONTRIBUTING.md](CONTRIBUTING.md) |

---

## Requirements

- **Python** 3.10 or later
- **Core** (`pip install .`): cloudflare, pandas, openpyxl, defusedxml, pyyaml, httpx
- **ML** (`pip install ".[ml]"`): scikit-learn, scipy, sentence-transformers
- **GCP** (`pip install ".[gcp]"`): google-cloud-bigquery (for local BQ modes)
- **Viz** (`pip install ".[viz]"`): matplotlib (for research visualizations)

---

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0).

For commercial licensing or enterprise implementation, contact
[conversem.com/contact](https://conversem.com/contact/).

---

## Citation

If you use this pipeline or the Query Fan-Out Session methodology in your research:

> Remy, R. (2025). *Query Fan-Out Session Analysis: Determining Optimal Time Windows for
> LLM Bot Request Bundling*. Conversem Research Report.

```bibtex
@misc{remy2025queryfanout,
  author = {Remy, Ruben},
  title  = {The Query Fan-Out Session: Server-side Query Fan-Out Tracking},
  year   = {2025},
  url    = {https://conversem.com/the-query-fan-out-session/}
}
```
