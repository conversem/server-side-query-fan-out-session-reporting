# Contributing Guide

Welcome! We appreciate contributions from the community. This guide will help you get
started with development, code standards, and the contribution workflow.

## Development Setup

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/server-side-query-fan-out-session-reporting.git
cd server-side-query-fan-out-session-reporting

# Add upstream remote
git remote add upstream https://github.com/conversem/server-side-query-fan-out-session-reporting.git
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate    # Linux/Mac
# .\venv\Scripts\Activate.ps1  # Windows PowerShell
```

### 3. Install Dependencies

```bash
# Core + all optional dependencies for development
pip install -e ".[dev,ml,monitoring,viz]"
```

Optional dependency groups:

| Group | Packages | Purpose |
|-------|----------|---------|
| `dev` | black, isort, pytest, pytest-cov, pytest-benchmark | Development tooling |
| `ml` | scikit-learn, scipy, sentence-transformers | Semantic analysis |
| `monitoring` | apscheduler | Local scheduling |
| `viz` | matplotlib | Window comparison visualizations |

### 4. Verify Setup

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=llm_bot_pipeline --cov-report=term-missing
```

## Code Standards

### Formatting

All Python code is formatted with **black** and imports sorted with **isort**:

```bash
black src/ scripts/ tests/
isort src/ scripts/ tests/
```

Configuration is in `pyproject.toml`:

```toml
[tool.black]
line-length = 88
target-version = ["py310"]

[tool.isort]
profile = "black"
line_length = 88
known_first_party = ["llm_bot_pipeline"]
```

### File Size

Keep individual files small and focused (under 200 lines where possible). Split by
responsibility. This keeps context small for both humans and AI agents.

### Comments

Only add comments that explain non-obvious intent, trade-offs, or constraints. Avoid
narrating what the code does.

## Contribution Workflow

### Reporting Issues

- Use [GitHub Issues](https://github.com/conversem/server-side-query-fan-out-session-reporting/issues)
- Search for existing issues before creating a new one
- Include: Python version, OS, error message, minimal reproduction steps

### Submitting Pull Requests

1. **Create a branch** from `main`:
   ```bash
   git checkout -b feature/my-feature
   ```

2. **Make your changes** following the code standards above

3. **Write tests** for new functionality

4. **Format your code**:
   ```bash
   black src/ scripts/ tests/
   isort src/ scripts/ tests/
   ```

5. **Run tests** and ensure they pass:
   ```bash
   pytest
   ```

6. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add support for new log format"
   ```

7. **Push and open a PR**:
   ```bash
   git push origin feature/my-feature
   # Then open a PR on GitHub against the main branch
   ```

### Commit Message Format

Use conventional commit style:

```
feat: add new ingestion provider for XYZ CDN
fix: handle empty log files in universal parser
docs: update Akamai provider guide
test: add unit tests for session refinement
refactor: extract URL normalization to utils
```

## Testing

### Test Structure

```
tests/
├── unit/           Unit tests (fast, isolated)
├── integration/    Integration tests (database, API)
└── performance/    Performance benchmarks
```

### Running Tests

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit/

# With coverage
pytest --cov=llm_bot_pipeline --cov-report=term-missing

# Specific test file
pytest tests/unit/test_ingestion.py -v

# Performance benchmarks
pytest tests/performance/ --benchmark-only
```

### Writing Tests

- Place tests in the appropriate directory (`unit/`, `integration/`, `performance/`)
- Name test files `test_*.py` and test functions `test_*`
- Use fixtures for common setup (database backends, sample data)
- Coverage target: 65%+ overall

## Project Conventions

### Module Organization

The codebase follows a modular structure optimized for both humans and AI:

- **One module per concern**: ingestion, pipeline, storage, reporting, etc.
- **Pluggable adapters**: Ingestion providers and storage backends are swappable
- **Shared utilities**: Common logic (bot classification, URL parsing) in `utils/`
- **Configuration as code**: All settings in dataclasses, validated at load time

### Adding a New Ingestion Provider

See [ingestion/adding-providers.md](../ingestion/adding-providers.md) for the full guide.
The short version:

1. Create `src/llm_bot_pipeline/ingestion/providers/my_provider.py`
2. Implement `IngestionAdapter` ABC
3. Register via `@register_adapter("my_provider")` decorator
4. Add tests in `tests/unit/ingestion/`
5. Add a provider guide in `docs/ingestion/providers/`

### Adding a New Storage Backend

1. Create `storage/my_backend.py` implementing `StorageBackend` ABC
2. Define `BackendCapabilities` for the new backend
3. Register in `storage/factory.py`
4. Add settings fields to `config/settings.py` if needed

### Environment

Always activate the virtual environment before running any commands:

```bash
source venv/bin/activate
```

Format before committing:

```bash
black src/ scripts/ tests/
isort src/ scripts/ tests/
```

## Security Policy

- Do **not** commit real credentials, API tokens, or zone IDs
- Use `config.example.yaml` with placeholder values for examples
- Report security vulnerabilities privately via
  [GitHub Security Advisories](https://github.com/conversem/server-side-query-fan-out-session-reporting/security)
- See [security.md](../security.md) for the full security guide
