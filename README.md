# Server-Side Query Fan-Out Session Reporting

A research framework for analyzing LLM bot traffic patterns and identifying query fan-out sessions from server-side log data.

## What is Query Fan-Out?

When LLM-powered services (like ChatGPT, Perplexity, or Claude) process user queries, they often make multiple rapid requests to gather information. These "fan-out" patterns appear as bursts of requests within milliseconds of each other. This framework:

1. **Ingests** Cloudflare logs via the Logpull API
2. **Identifies** request bundles using temporal and semantic analysis
3. **Optimizes** the time window for accurate session detection
4. **Reports** bundled sessions in CSV/Excel format

## Key Features

- **Research-backed methodology**: Uses OptScore composite metric for window optimization
- **Semantic analysis**: TF-IDF and Transformer-based URL embeddings
- **Provider-specific tuning**: Different bots have different behaviors
- **Reproducible experiments**: Configurable parameters with validation

## Quick Start

### Prerequisites

- Python 3.11+
- Cloudflare account with API access
- SOPS and Age for secrets encryption

### 1. Clone and Setup

```bash
git clone https://github.com/your-org/server-side-query-fan-out-session-reporting.git
cd server-side-query-fan-out-session-reporting

python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: .\venv\Scripts\Activate.ps1  # Windows PowerShell

pip install -r requirements.txt
```

### 2. Configure Secrets (Required)

This project uses SOPS for secure secret management.

```bash
# Install SOPS and Age
brew install sops age  # macOS
# See docs/sops/quickstart.md for Linux/Windows

# Generate your encryption key
mkdir -p ~/.sops/age
age-keygen -o ~/.sops/age/keys.txt
# Note the public key from the output

# Configure SOPS
cp .sops.yaml.example .sops.yaml
# Edit .sops.yaml and replace age1YOUR_PUBLIC_KEY_HERE with your key

# Set environment variable (add to ~/.bashrc or ~/.zshrc)
export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt

# Create and encrypt your config
cp config.example.yaml config.yaml
# Edit config.yaml with your Cloudflare credentials
sops -e config.yaml > config.enc.yaml
rm config.yaml  # Remove unencrypted version
```

See [docs/sops/quickstart.md](docs/sops/quickstart.md) for detailed instructions.

### 3. Ingest Logs

```bash
# Pull last 7 days of logs from Cloudflare
python scripts/ingest_logs.py --days 7

# Or specific date range
python scripts/ingest_logs.py --start-date 2024-01-01 --end-date 2024-01-07
```

### 4. Run ETL Pipeline

```bash
python scripts/run_pipeline.py --start-date 2024-01-01 --end-date 2024-01-07
```

### 5. Run Window Optimization Experiment

```bash
# Run with default settings
python scripts/run_window_experiment.py

# Custom windows
python scripts/run_window_experiment.py --windows 50,100,500,1000,3000
```

### 6. Export Session Reports

```bash
# Export to Excel
python scripts/export_session_report.py --format xlsx --output data/reports/sessions.xlsx

# Export to CSV with filters
python scripts/export_session_report.py \
    --start-date 2024-01-01 \
    --provider OpenAI \
    --output data/reports/openai_sessions.csv
```

## Research Methodology

### OptScore Formula

The framework uses a composite optimization score:

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

### Experiment Output

Running `run_window_experiment.py` produces:

- **Optimal window recommendation** with confidence level
- **Per-provider analysis** showing behavioral differences
- **Validation metrics** from hold-out testing
- **Visualization** of window comparisons

## Project Structure

```
├── src/llm_bot_pipeline/
│   ├── cloudflare/      # Logpull API integration
│   ├── storage/         # SQLite storage layer
│   ├── pipeline/        # ETL processing
│   ├── research/        # Window optimization algorithms
│   └── reporting/       # Session aggregation & export
├── scripts/             # CLI entry points
├── docs/                # Documentation
└── tests/               # Test suite
```

See [docs/architecture.md](docs/architecture.md) for detailed architecture.

## Sample Data

Generate synthetic data for testing:

```bash
python scripts/generate_sample_data.py --output data/sample_requests.csv --count 5000
```

## Configuration

### config.example.yaml

```yaml
storage:
  backend: "sqlite"
  sqlite_db_path: "data/llm-bot-logs.db"

cloudflare:
  api_token: "your-cloudflare-api-token"
  zone_id: "your-zone-id"
```

## Documentation

- [Architecture Overview](docs/architecture.md)
- [SOPS Quick Start](docs/sops/quickstart.md)
- [Research PRD](docs/prds/query-fanout-bundling-PRD.md)

## Contributing

Contributions are welcome! Please read the contributing guidelines before submitting PRs.

## License

[MIT License](LICENSE)

## Citation

If you use this framework in your research, please cite:

```bibtex
@software{query_fanout_sessions,
  title = {Server-Side Query Fan-Out Session Reporting},
  year = {2024},
  url = {https://github.com/your-org/server-side-query-fan-out-session-reporting}
}
```
