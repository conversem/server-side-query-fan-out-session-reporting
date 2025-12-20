# Project Architecture

## Overview

This project provides a framework for analyzing LLM bot traffic patterns and bundling requests into semantic query fan-out sessions. It uses Cloudflare's Logpull API to ingest data into a local SQLite database, then applies research-backed algorithms to identify session patterns.

## Folder Structure

```
server-side-query-fan-out-session-reporting/
├── src/                              # Main source code
│   └── llm_bot_pipeline/             # Python package
│       │
│       ├── config/                   # Configuration management
│       │   ├── settings.py           # Environment & project settings
│       │   ├── constants.py          # Bot classifications, field names
│       │   └── sops_loader.py        # SOPS encrypted config loader
│       │
│       ├── cloudflare/               # Cloudflare Logpull integration
│       │   ├── logpull.py            # Logpull API client
│       │   └── filters.py            # LLM bot filter definitions
│       │
│       ├── storage/                  # SQLite storage layer
│       │   ├── base.py               # StorageBackend abstract class
│       │   ├── factory.py            # Backend factory pattern
│       │   └── sqlite_backend.py     # SQLite implementation
│       │
│       ├── pipeline/                 # ETL pipeline
│       │   ├── local_pipeline.py     # SQLite ETL pipeline
│       │   ├── sql_compat.py         # SQL compatibility layer
│       │   ├── extract.py            # Data extraction
│       │   └── transform.py          # Data transformation
│       │
│       ├── reporting/                # Analytics & reporting
│       │   ├── session_aggregations.py  # Session bundling logic
│       │   ├── local_aggregations.py    # Aggregation queries
│       │   └── local_dashboard_queries.py # Dashboard queries
│       │
│       ├── research/                 # Research methodology (core)
│       │   ├── experiment_runner.py  # Window optimization experiments
│       │   ├── temporal_analysis.py  # Time-based pattern analysis
│       │   ├── window_optimizer.py   # Optimal window calculation
│       │   └── semantic_embeddings.py # URL semantic similarity
│       │
│       ├── schemas/                  # Data schemas
│       │   ├── raw.py                # Raw table schema
│       │   ├── clean.py              # Clean/processed schema
│       │   └── bundles.py            # Session bundle schema
│       │
│       └── utils/                    # Shared utilities
│           ├── bot_classifier.py     # User-agent classification
│           ├── url_utils.py          # URL parsing utilities
│           └── http_utils.py         # HTTP status helpers
│
├── scripts/                          # CLI entry points
│   ├── ingest_logs.py                # Cloudflare Logpull ingestion
│   ├── run_pipeline.py               # ETL pipeline execution
│   ├── run_window_experiment.py      # Research: optimal window
│   ├── export_session_report.py      # Export to CSV/Excel
│   ├── analyze_temporal_patterns.py  # Temporal pattern analysis
│   ├── plot_window_comparison.py     # Visualization generation
│   ├── backfill_sessions.py          # Session backfill
│   └── generate_sample_data.py       # Generate test data
│
├── tests/                            # Test suite
│   ├── unit/                         # Unit tests
│   └── integration/                  # Integration tests
│
├── docs/                             # Documentation
│   ├── sops/                         # SOPS encryption guide
│   │   ├── quickstart.md
│   │   ├── key-management.md
│   │   ├── daily-usage.md
│   │   └── troubleshooting.md
│   ├── prds/
│   │   └── query-fanout-bundling-PRD.md
│   └── architecture.md               # This file
│
├── data/                             # Local data storage
│   └── .gitkeep                      # SQLite databases gitignored
│
├── credentials/                      # Credentials (gitignored)
│   └── .gitkeep
│
├── Dockerfile
├── docker-compose.yml
├── .sops.yaml.example                # SOPS template
├── config.example.yaml               # Configuration template
├── requirements.txt
├── pyproject.toml
└── README.md
```

## Data Flow

```
┌─────────────────┐     ┌───────────────────┐     ┌─────────────────────┐
│   Cloudflare    │────►│   SQLite Database │────►│   Session Reports   │
│    Logpull      │     │   raw_bot_requests│     │   CSV / Excel       │
└─────────────────┘     └───────────────────┘     └─────────────────────┘
        │                        │                         │
        │                        │                         │
   cloudflare/              pipeline/                 reporting/
   logpull.py               local_pipeline.py         session_aggregations.py
   filters.py               transform.py              export_session_report.py
                                 │
                                 ▼
                           ┌───────────────────┐
                           │     Research      │
                           │  Window Optimizer │
                           │  OptScore Calc    │
                           └───────────────────┘
                                 │
                                 ▼
                           research/
                           experiment_runner.py
                           window_optimizer.py
                           temporal_analysis.py
```

## Research Methodology

The core research component determines optimal time windows for bundling LLM bot requests into query fan-out sessions.

### Key Concepts

1. **Query Fan-Out Session**: A bundle of related requests from the same bot provider within a time window
2. **Time Window**: The maximum gap (in milliseconds) between requests in the same session
3. **OptScore**: A composite metric balancing multiple optimization objectives

### OptScore Formula

```
OptScore = α·MIBCS + β·Silhouette + γ·BPS - δ·SingletonRate - ε·GiantRate - ζ·ThematicVariance
```

Where:
- **MIBCS** (α=0.30): Mean Intra-Bundle Cosine Similarity
- **Silhouette** (β=0.25): Cluster separation quality
- **BPS** (γ=0.25): Bundle Purity Score
- **SingletonRate** (δ=0.10): Penalty for single-request bundles
- **GiantRate** (ε=0.05): Penalty for oversized bundles
- **ThematicVariance** (ζ=0.05): Penalty for thematic inconsistency

### Experiment Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Window Optimization Experiment                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. Load Data                                                        │
│     └─► Filter by bot category (user_request)                       │
│     └─► Exclude noise providers (Microsoft/Bing)                    │
│                                                                      │
│  2. Generate Candidate Windows                                       │
│     └─► [50ms, 100ms, 500ms, 1000ms, 3000ms, 5000ms]                │
│                                                                      │
│  3. For Each Window:                                                 │
│     └─► Bundle requests by (provider, time_window)                  │
│     └─► Calculate semantic embeddings (TF-IDF or Transformer)       │
│     └─► Compute OptScore metrics                                    │
│                                                                      │
│  4. Select Optimal Window                                            │
│     └─► Highest OptScore                                            │
│     └─► Cross-validation agreement                                  │
│     └─► Confidence level (high/medium/low)                          │
│                                                                      │
│  5. Generate Report                                                  │
│     └─► Recommendation with confidence                              │
│     └─► Per-provider analysis                                       │
│     └─► Visualization outputs                                       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Storage Layer

The project uses SQLite for local data storage:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SQLite Database                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   Tables:                                                            │
│   ├── raw_bot_requests      # Ingested Cloudflare logs              │
│   ├── bot_requests_daily    # Cleaned/enriched daily data           │
│   ├── query_fanout_sessions # Bundled sessions                      │
│   └── daily_summary         # Aggregated metrics                    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Configuration

### Secrets Management

All sensitive configuration is encrypted using SOPS:

```
.sops.yaml.example  →  Copy  →  .sops.yaml (with your key)
                                      │
config.example.yaml  →  Encrypt  →  config.enc.yaml
                                      │
                                sops_loader.py
                                      │
                                      ▼
                               Settings object
```

See [docs/sops/quickstart.md](sops/quickstart.md) for setup instructions.

### Environment Variables (Alternative)

| Variable | Description |
|----------|-------------|
| `SQLITE_DB_PATH` | SQLite database path (default: `data/llm-bot-logs.db`) |
| `CLOUDFLARE_API_TOKEN` | Cloudflare API token |
| `CLOUDFLARE_ZONE_ID` | Cloudflare zone ID |

## Design Principles

### Modularity
- Each module has a single responsibility
- Small, focused files (< 200 lines ideally)
- Clear interfaces between modules

### Research-First
- Core algorithms are isolated in `research/` module
- Configurable parameters for experimentation
- Reproducible results with seed control

### LLM/AI Friendly
- Small context per file for efficient AI assistance
- Clear naming conventions
- Self-documenting code structure

### Human Friendly
- Intuitive navigation
- Logical grouping by domain
- Comprehensive documentation
