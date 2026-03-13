"""
Root pytest configuration and shared fixtures for all test suites.

Centralizes common fixtures used across unit, integration, and performance tests
to eliminate duplication and improve reusability.
"""

import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from freezegun import freeze_time

# ---------------------------------------------------------------------------
# BigQuery availability check — skip tests that require BigQuery when it is
# not installed or the backend module is not present. Tests decorated with
# @pytest.mark.bigquery are automatically skipped in environments where the
# BigQuery backend (premium feature) is not available.
# ---------------------------------------------------------------------------
try:
    from llm_bot_pipeline.storage.bigquery_backend import BigQueryBackend  # noqa: F401
    _BIGQUERY_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    _BIGQUERY_AVAILABLE = False


def pytest_collection_modifyitems(items):
    """Auto-skip tests marked with @pytest.mark.bigquery when BQ is not available."""
    if _BIGQUERY_AVAILABLE:
        return
    skip_marker = pytest.mark.skip(
        reason="BigQuery backend not available in this environment"
    )
    for item in items:
        if item.get_closest_marker("bigquery"):
            item.add_marker(skip_marker)

# ---------------------------------------------------------------------------
# Register explicit sqlite3 adapters for date/datetime (Python 3.12+).
# The built-in default adapters are deprecated; these replacements follow
# the recipes from the official sqlite3 documentation.
# ---------------------------------------------------------------------------
sqlite3.register_adapter(date, lambda val: val.isoformat())
sqlite3.register_adapter(datetime, lambda val: val.isoformat())

from llm_bot_pipeline.storage import get_backend

# Fixed date for deterministic tests (2024-01-15)
FIXED_TEST_DATE = date(2024, 1, 15)


@pytest.fixture(autouse=True)
def _freeze_time():
    """Freeze time at 2024-01-15 for deterministic test behavior."""
    with freeze_time("2024-01-15"):
        yield


@pytest.fixture
def fixed_date() -> date:
    """Return fixed date for tests (2024-01-15)."""
    return FIXED_TEST_DATE


# =============================================================================
# PROVIDER REGISTRATION
# =============================================================================


@pytest.fixture
def register_providers():
    """
    Fixture to ensure ingestion providers are registered before tests.

    Use this fixture in tests that depend on registered providers.
    Since IngestionRegistry.clear() may have been called, we explicitly
    re-register the provider classes.
    """
    from llm_bot_pipeline.ingestion.providers import (
        AkamaiAdapter,
        ALBAdapter,
        AzureCDNAdapter,
        CloudflareAdapter,
        CloudFrontAdapter,
        FastlyAdapter,
        GCPCDNAdapter,
        UniversalAdapter,
    )
    from llm_bot_pipeline.ingestion.registry import IngestionRegistry

    providers = {
        "universal": UniversalAdapter,
        "akamai": AkamaiAdapter,
        "aws_alb": ALBAdapter,
        "aws_cloudfront": CloudFrontAdapter,
        "azure_cdn": AzureCDNAdapter,
        "cloudflare": CloudflareAdapter,
        "fastly": FastlyAdapter,
        "gcp_cdn": GCPCDNAdapter,
    }
    for name, adapter_cls in providers.items():
        if not IngestionRegistry.is_provider_registered(name):
            IngestionRegistry.register_provider(name, adapter_cls)


# =============================================================================
# FIXTURES DIRECTORY
# =============================================================================


@pytest.fixture
def fixtures_dir() -> Path:
    """Return path to test fixtures/ingestion directory."""
    return Path(__file__).parent / "fixtures" / "ingestion"


# =============================================================================
# SAMPLE DATA FIXTURES
# =============================================================================


def generate_sample_records(
    num_records: int = 100,
    start_date: date = None,
    end_date: date = None,
    seed: int = 42,
) -> list[dict]:
    """
    Generate sample bot traffic records for testing.

    Args:
        num_records: Number of records to generate
        start_date: Start date for records (default: 3 days ago)
        end_date: End date for records (default: yesterday)
        seed: Random seed for reproducibility (default: 42)

    Returns:
        List of record dictionaries
    """
    import random
    from datetime import datetime, timezone

    random.seed(seed)

    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=2)

    bots = [
        ("GPTBot", "OpenAI", "training"),
        ("ChatGPT-User", "OpenAI", "user_request"),
        ("ClaudeBot", "Anthropic", "training"),
        ("Claude-User", "Anthropic", "user_request"),
        ("PerplexityBot", "Perplexity", "search"),
    ]

    user_agents = {
        "GPTBot": "Mozilla/5.0 (compatible; GPTBot/1.2; +https://openai.com/gptbot)",
        "ChatGPT-User": "Mozilla/5.0 (compatible; ChatGPT-User/1.0; +https://openai.com/bot)",
        "ClaudeBot": "Mozilla/5.0 (compatible; ClaudeBot/1.0; +https://anthropic.com)",
        "Claude-User": "Mozilla/5.0 (compatible; Claude-User/1.0; +https://anthropic.com)",
        "PerplexityBot": "Mozilla/5.0 (compatible; PerplexityBot/1.0; +https://perplexity.ai)",
    }

    urls = [
        "/docs/getting-started",
        "/api/reference/users",
        "/blog/2024/01/introduction",
        "/products/enterprise",
        "/help/billing",
    ]

    countries = ["US", "DE", "GB", "IE", "JP"]
    statuses = [200, 200, 200, 200, 200, 304, 404, 403, 500]

    records = []
    days_range = (end_date - start_date).days + 1

    for i in range(num_records):
        bot_name, provider, category = random.choice(bots)

        day_offset = random.randint(0, days_range - 1)
        record_date = start_date + timedelta(days=day_offset)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        timestamp = datetime(
            record_date.year,
            record_date.month,
            record_date.day,
            hour,
            minute,
            second,
            tzinfo=timezone.utc,
        )

        record = {
            "EdgeStartTimestamp": timestamp.isoformat(),
            "ClientRequestURI": random.choice(urls),
            "ClientRequestHost": "example.com",
            "ClientRequestUserAgent": user_agents[bot_name],
            "BotScore": random.randint(1, 30),
            "BotScoreSrc": "Machine Learning",
            "VerifiedBot": True,
            "BotTags": ["llm"],
            "ClientIP": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "ClientCountry": random.choice(countries),
            "EdgeResponseStatus": random.choice(statuses),
        }
        records.append(record)

    return records


@pytest.fixture
def sample_records() -> list[dict]:
    """Generate 100 sample records for testing."""
    return generate_sample_records(num_records=100)


@pytest.fixture
def sample_records_small() -> list[dict]:
    """Generate 20 sample records for quick tests."""
    return generate_sample_records(num_records=20)


@pytest.fixture
def sample_records_large() -> list[dict]:
    """Generate 500 sample records for performance tests."""
    return generate_sample_records(num_records=500)


# =============================================================================
# DATABASE FIXTURES
# =============================================================================


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test_llm_bot.db"


@pytest.fixture
def sqlite_backend(temp_db_path: Path):
    """
    Create an initialized SQLite backend with temporary database.

    Automatically cleans up after test.
    """
    backend = get_backend("sqlite", db_path=temp_db_path)
    backend.initialize()
    yield backend
    backend.close()


# =============================================================================
# DATE FIXTURES
# =============================================================================


@pytest.fixture
def date_range():
    """Return a standard date range for testing (last 3 days)."""
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=2)
    return start_date, end_date
