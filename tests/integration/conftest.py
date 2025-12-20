"""
Shared fixtures for integration tests.

Provides:
- Temporary SQLite database for isolated testing
- Sample data generator fixtures
- Pipeline and reporting component fixtures
"""

from datetime import date, timedelta
from pathlib import Path

import pytest

# Import pipeline components
from llm_bot_pipeline.pipeline import LocalPipeline
from llm_bot_pipeline.reporting import LocalDashboardQueries, LocalReportingAggregator
from llm_bot_pipeline.storage import get_backend

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

    # Set seed for reproducible tests
    random.seed(seed)

    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=2)

    # Bot profiles for generating diverse data
    bots = [
        ("GPTBot", "OpenAI", "training"),
        ("ChatGPT-User", "OpenAI", "user_request"),
        ("ClaudeBot", "Anthropic", "training"),
        ("Claude-User", "Anthropic", "user_request"),
        ("PerplexityBot", "Perplexity", "user_request"),
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
    statuses = [200, 200, 200, 200, 200, 304, 404, 403, 500]  # Weighted toward success

    records = []
    days_range = (end_date - start_date).days + 1

    for i in range(num_records):
        bot_name, provider, category = random.choice(bots)

        # Generate timestamp within date range
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


@pytest.fixture
def sqlite_backend_with_data(sqlite_backend, sample_records):
    """
    SQLite backend pre-populated with sample data.

    Returns tuple of (backend, records_inserted).
    """
    rows = sqlite_backend.insert_raw_records(sample_records)
    return sqlite_backend, rows


# =============================================================================
# PIPELINE FIXTURES
# =============================================================================


@pytest.fixture
def local_pipeline(temp_db_path: Path):
    """
    Create a LocalPipeline with temporary database.

    Automatically cleans up after test.
    """
    pipeline = LocalPipeline(backend_type="sqlite", db_path=temp_db_path)
    pipeline.initialize()
    yield pipeline
    pipeline.close()


@pytest.fixture
def pipeline_with_data(local_pipeline, sample_records):
    """
    LocalPipeline with raw data already ingested.

    Returns tuple of (pipeline, records_count).
    """
    local_pipeline._backend.insert_raw_records(sample_records)
    return local_pipeline, len(sample_records)


# =============================================================================
# REPORTING FIXTURES
# =============================================================================


@pytest.fixture
def local_aggregator(temp_db_path: Path):
    """
    Create a LocalReportingAggregator with temporary database.
    """
    aggregator = LocalReportingAggregator(backend_type="sqlite", db_path=temp_db_path)
    aggregator.initialize()
    yield aggregator
    aggregator.close()


@pytest.fixture
def local_dashboard(temp_db_path: Path):
    """
    Create a LocalDashboardQueries with temporary database.
    """
    dashboard = LocalDashboardQueries(backend_type="sqlite", db_path=temp_db_path)
    dashboard.initialize()
    yield dashboard
    dashboard.close()


# =============================================================================
# DATE FIXTURES
# =============================================================================


@pytest.fixture
def date_range():
    """Return a standard date range for testing (last 3 days)."""
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=2)
    return start_date, end_date
