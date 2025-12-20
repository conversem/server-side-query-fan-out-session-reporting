#!/usr/bin/env python3
"""
Generate realistic sample LLM bot traffic data for testing.

Creates synthetic bot traffic data that mimics real Cloudflare log patterns,
suitable for testing the pipeline without requiring API access.

Usage:
    # Generate 7 days of sample data (default)
    python scripts/generate_sample_data.py

    # Generate data for specific date range
    python scripts/generate_sample_data.py --start-date 2024-01-01 --end-date 2024-01-07

    # Generate high volume data
    python scripts/generate_sample_data.py --daily-requests 10000 --days 30

    # Insert directly into SQLite database
    python scripts/generate_sample_data.py --output sqlite --db-path data/test.db

    # Output as JSON for inspection
    python scripts/generate_sample_data.py --output json --limit 100
"""

import argparse
import json
import logging
import random
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.pipeline.local_pipeline import setup_logging

logger = logging.getLogger(__name__)


# =============================================================================
# BOT PROFILES - Realistic bot behavior patterns
# =============================================================================


@dataclass
class BotProfile:
    """Profile defining a bot's behavior patterns."""

    name: str
    provider: str
    category: str
    user_agent_template: str
    # Traffic distribution (relative weight)
    traffic_weight: float
    # Bot score range (Cloudflare's 1-99 scale)
    bot_score_min: int
    bot_score_max: int
    # Response success rate (0-1)
    success_rate: float
    # Active hours (0-23, peak activity)
    peak_hours: list[int]


# Realistic bot profiles based on known LLM bot behaviors
BOT_PROFILES = [
    # OpenAI bots
    BotProfile(
        name="GPTBot",
        provider="OpenAI",
        category="training",
        user_agent_template="Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.2; +https://openai.com/gptbot)",
        traffic_weight=0.25,
        bot_score_min=1,
        bot_score_max=15,
        success_rate=0.92,
        peak_hours=[2, 3, 4, 5, 14, 15, 16],  # Off-peak crawling
    ),
    BotProfile(
        name="ChatGPT-User",
        provider="OpenAI",
        category="user_request",
        user_agent_template="Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ChatGPT-User/1.0; +https://openai.com/bot)",
        traffic_weight=0.20,
        bot_score_min=5,
        bot_score_max=30,
        success_rate=0.95,
        peak_hours=[9, 10, 11, 14, 15, 16, 17, 20, 21],  # Business + evening hours
    ),
    BotProfile(
        name="OAI-SearchBot",
        provider="OpenAI",
        category="user_request",
        user_agent_template="Mozilla/5.0 (compatible; OAI-SearchBot/1.0; +https://openai.com/searchbot)",
        traffic_weight=0.08,
        bot_score_min=10,
        bot_score_max=35,
        success_rate=0.94,
        peak_hours=[8, 9, 10, 11, 12, 13, 14, 15, 16, 17],  # Workday search
    ),
    # Anthropic bots
    BotProfile(
        name="ClaudeBot",
        provider="Anthropic",
        category="training",
        user_agent_template="Mozilla/5.0 (compatible; ClaudeBot/1.0; +https://anthropic.com/claudebot)",
        traffic_weight=0.15,
        bot_score_min=1,
        bot_score_max=12,
        success_rate=0.93,
        peak_hours=[1, 2, 3, 4, 5, 6],  # Late night crawling
    ),
    BotProfile(
        name="Claude-User",
        provider="Anthropic",
        category="user_request",
        user_agent_template="Mozilla/5.0 (compatible; Claude-User/1.0; +https://anthropic.com/claude)",
        traffic_weight=0.10,
        bot_score_min=8,
        bot_score_max=28,
        success_rate=0.96,
        peak_hours=[9, 10, 11, 14, 15, 16, 17, 19, 20],
    ),
    BotProfile(
        name="Claude-SearchBot",
        provider="Anthropic",
        category="user_request",
        user_agent_template="Mozilla/5.0 (compatible; Claude-SearchBot/1.0; +https://anthropic.com/searchbot)",
        traffic_weight=0.02,
        bot_score_min=10,
        bot_score_max=32,
        success_rate=0.93,
        peak_hours=[9, 10, 11, 12, 13, 14, 15, 16, 17],
    ),
    # Google
    BotProfile(
        name="Google-Extended",
        provider="Google",
        category="training",
        user_agent_template="Mozilla/5.0 (compatible; Google-Extended; +https://developers.google.com/search/docs/crawling-indexing/google-common-crawlers)",
        traffic_weight=0.10,
        bot_score_min=1,
        bot_score_max=10,
        success_rate=0.97,
        peak_hours=[0, 1, 2, 3, 4, 5, 6, 7],  # Very off-peak
    ),
    # Perplexity
    BotProfile(
        name="PerplexityBot",
        provider="Perplexity",
        category="user_request",
        user_agent_template="Mozilla/5.0 (compatible; PerplexityBot/1.0; +https://perplexity.ai/bot)",
        traffic_weight=0.05,
        bot_score_min=15,
        bot_score_max=40,
        success_rate=0.91,
        peak_hours=[10, 11, 12, 13, 14, 15, 16, 17, 18],
    ),
    # Microsoft
    BotProfile(
        name="bingbot",
        provider="Microsoft",
        category="user_request",
        user_agent_template="Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
        traffic_weight=0.03,
        bot_score_min=5,
        bot_score_max=25,
        success_rate=0.94,
        peak_hours=[9, 10, 11, 12, 13, 14, 15, 16, 17],
    ),
    # Apple
    BotProfile(
        name="Applebot-Extended",
        provider="Apple",
        category="training",
        user_agent_template="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15 Applebot-Extended/0.1",
        traffic_weight=0.02,
        bot_score_min=1,
        bot_score_max=20,
        success_rate=0.95,
        peak_hours=[3, 4, 5, 6, 7, 8],
    ),
]


# =============================================================================
# URL PATTERNS - Realistic content being accessed
# =============================================================================

URL_PATTERNS = [
    # Blog/article content
    ("/blog/{year}/{month}/{slug}", 0.25),
    ("/articles/{category}/{slug}", 0.15),
    ("/news/{year}/{slug}", 0.10),
    # Documentation
    ("/docs/{section}/{page}", 0.15),
    ("/api/reference/{endpoint}", 0.08),
    ("/guides/{topic}", 0.07),
    # Product pages
    ("/products/{category}/{product}", 0.08),
    ("/features/{feature}", 0.05),
    # Support
    ("/help/{topic}", 0.04),
    ("/faq/{category}", 0.02),
    # Root and common pages
    ("/", 0.01),
]

SLUG_WORDS = [
    "getting-started",
    "introduction",
    "quickstart",
    "overview",
    "tutorial",
    "best-practices",
    "advanced",
    "configuration",
    "installation",
    "setup",
    "authentication",
    "api-keys",
    "webhooks",
    "integrations",
    "plugins",
    "performance",
    "optimization",
    "scaling",
    "monitoring",
    "debugging",
    "security",
    "privacy",
    "compliance",
    "gdpr",
    "terms-of-service",
    "pricing",
    "enterprise",
    "team",
    "individual",
    "free-tier",
    "machine-learning",
    "ai-agents",
    "llm-integration",
    "embeddings",
    "vectors",
    "python",
    "javascript",
    "typescript",
    "rust",
    "go",
    "java",
]

CATEGORIES = ["tech", "business", "product", "engineering", "design", "marketing"]
SECTIONS = ["v1", "v2", "latest", "stable", "beta"]
ENDPOINTS = ["users", "items", "orders", "analytics", "reports", "webhooks"]
TOPICS = ["billing", "account", "api", "errors", "limits", "usage"]
FEATURES = ["dashboard", "analytics", "automation", "collaboration", "integrations"]
PRODUCTS = ["starter", "pro", "enterprise", "team", "individual"]


# =============================================================================
# GEOGRAPHIC DISTRIBUTION - Where crawlers operate from
# =============================================================================

# Country distribution (crawler server locations, NOT end-user locations)
COUNTRY_DISTRIBUTION = [
    ("US", 0.55),  # Most crawlers are US-based
    ("DE", 0.12),  # Germany - major data center hub
    ("IE", 0.08),  # Ireland - AWS/Google regions
    ("GB", 0.06),  # UK
    ("NL", 0.05),  # Netherlands
    ("JP", 0.04),  # Japan
    ("SG", 0.04),  # Singapore
    ("AU", 0.03),  # Australia
    ("CA", 0.02),  # Canada
    ("FR", 0.01),  # France
]


# =============================================================================
# RESPONSE STATUS DISTRIBUTION
# =============================================================================


def get_response_status(success_rate: float) -> int:
    """Generate realistic HTTP response status based on success rate."""
    rand = random.random()

    if rand < success_rate:
        # Success - mostly 200, sometimes 304
        return random.choices([200, 304], weights=[0.95, 0.05])[0]
    else:
        # Errors
        error_distribution = [
            (404, 0.50),  # Not found - most common
            (403, 0.20),  # Forbidden
            (429, 0.15),  # Rate limited
            (500, 0.10),  # Server error
            (503, 0.05),  # Service unavailable
        ]
        statuses, weights = zip(*error_distribution)
        return random.choices(statuses, weights=weights)[0]


# =============================================================================
# DATA GENERATOR
# =============================================================================


class SampleDataGenerator:
    """Generates realistic LLM bot traffic data."""

    def __init__(
        self,
        daily_requests: int = 1000,
        hosts: Optional[list[str]] = None,
        seed: Optional[int] = None,
    ):
        """
        Initialize the generator.

        Args:
            daily_requests: Average requests per day
            hosts: List of host domains (default: example domains)
            seed: Random seed for reproducibility
        """
        self.daily_requests = daily_requests
        self.hosts = hosts or ["example.com", "docs.example.com", "blog.example.com"]

        if seed is not None:
            random.seed(seed)

        # Pre-calculate weights for bot selection
        self.bot_weights = [p.traffic_weight for p in BOT_PROFILES]

        # Pre-calculate URL pattern weights
        _, self.url_weights = zip(*URL_PATTERNS)

        # Pre-calculate country weights
        _, self.country_weights = zip(*COUNTRY_DISTRIBUTION)
        self.countries = [c for c, _ in COUNTRY_DISTRIBUTION]

    def generate_url(self) -> str:
        """Generate a realistic URL path."""
        pattern, _ = random.choices(URL_PATTERNS, weights=self.url_weights)[0]

        # Fill in template variables
        url = pattern
        url = url.replace("{year}", str(random.randint(2022, 2024)))
        url = url.replace("{month}", f"{random.randint(1, 12):02d}")
        url = url.replace("{slug}", random.choice(SLUG_WORDS))
        url = url.replace("{category}", random.choice(CATEGORIES))
        url = url.replace("{section}", random.choice(SECTIONS))
        url = url.replace("{page}", random.choice(SLUG_WORDS))
        url = url.replace("{endpoint}", random.choice(ENDPOINTS))
        url = url.replace("{topic}", random.choice(TOPICS))
        url = url.replace("{feature}", random.choice(FEATURES))
        url = url.replace("{product}", random.choice(PRODUCTS))

        return url

    def generate_record(
        self,
        timestamp: datetime,
        bot_profile: Optional[BotProfile] = None,
    ) -> dict:
        """
        Generate a single log record.

        Args:
            timestamp: Request timestamp
            bot_profile: Specific bot profile to use (random if None)

        Returns:
            Dictionary matching Cloudflare log format
        """
        # Select bot profile if not specified
        if bot_profile is None:
            bot_profile = random.choices(BOT_PROFILES, weights=self.bot_weights)[0]

        # Generate record fields
        host = random.choice(self.hosts)
        uri = self.generate_url()
        country = random.choices(self.countries, weights=self.country_weights)[0]

        # Bot score with some variation
        bot_score = random.randint(bot_profile.bot_score_min, bot_profile.bot_score_max)

        # Response status based on bot's success rate
        response_status = get_response_status(bot_profile.success_rate)

        # Generate fake IP (private range for safety)
        ip = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

        return {
            "EdgeStartTimestamp": timestamp.isoformat(),
            "ClientRequestURI": uri,
            "ClientRequestHost": host,
            "ClientRequestUserAgent": bot_profile.user_agent_template,
            "BotScore": bot_score,
            "BotScoreSrc": "Machine Learning",
            "VerifiedBot": True,
            "BotTags": ["llm"],
            "ClientIP": ip,
            "ClientCountry": country,
            "EdgeResponseStatus": response_status,
        }

    def generate_day(self, target_date: date) -> list[dict]:
        """
        Generate all records for a specific day.

        Traffic follows realistic hourly patterns based on bot profiles.

        Args:
            target_date: The date to generate data for

        Returns:
            List of record dictionaries
        """
        records = []

        # Add some daily variation (±20%)
        day_requests = int(self.daily_requests * random.uniform(0.8, 1.2))

        # Weekend reduction
        if target_date.weekday() >= 5:  # Saturday=5, Sunday=6
            day_requests = int(day_requests * 0.6)

        for _ in range(day_requests):
            # Select bot profile
            bot_profile = random.choices(BOT_PROFILES, weights=self.bot_weights)[0]

            # Generate timestamp with hour bias toward peak hours
            hour = self._weighted_hour(bot_profile.peak_hours)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            microsecond = random.randint(0, 999999)

            timestamp = datetime(
                target_date.year,
                target_date.month,
                target_date.day,
                hour,
                minute,
                second,
                microsecond,
                tzinfo=timezone.utc,
            )

            record = self.generate_record(timestamp, bot_profile)
            records.append(record)

        # Sort by timestamp
        records.sort(key=lambda r: r["EdgeStartTimestamp"])

        logger.info(
            f"Generated {len(records)} records for {target_date} "
            f"({target_date.strftime('%A')})"
        )

        return records

    def _weighted_hour(self, peak_hours: list[int]) -> int:
        """Select an hour with bias toward peak hours."""
        if random.random() < 0.6:  # 60% chance of peak hour
            return random.choice(peak_hours)
        else:
            return random.randint(0, 23)

    def generate_date_range(
        self,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Generate records for a date range.

        Args:
            start_date: First day (inclusive)
            end_date: Last day (inclusive)

        Returns:
            List of all record dictionaries
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must be <= end_date ({end_date})"
            )

        all_records = []
        current = start_date

        while current <= end_date:
            day_records = self.generate_day(current)
            all_records.extend(day_records)
            current += timedelta(days=1)

        logger.info(
            f"Generated {len(all_records)} total records "
            f"from {start_date} to {end_date}"
        )

        return all_records


# =============================================================================
# OUTPUT HANDLERS
# =============================================================================


def output_json(records: list[dict], limit: Optional[int] = None):
    """Output records as JSON to stdout."""
    if limit:
        records = records[:limit]
    print(json.dumps(records, indent=2, default=str))


def output_jsonl(records: list[dict], output_path: Path):
    """Output records as JSON Lines file."""
    with open(output_path, "w") as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")
    logger.info(f"Wrote {len(records)} records to {output_path}")


def output_sqlite(records: list[dict], db_path: Optional[Path] = None):
    """Insert records directly into SQLite database."""
    from llm_bot_pipeline.storage import get_backend

    # Pass db_path only if specified, let factory use default otherwise
    kwargs = {}
    if db_path:
        kwargs["db_path"] = db_path

    backend = get_backend("sqlite", **kwargs)
    backend.initialize()

    try:
        rows_inserted = backend.insert_raw_records(records)
        logger.info(f"Inserted {rows_inserted} raw records into SQLite")
    finally:
        backend.close()


def output_stats(records: list[dict]):
    """Print statistics about generated data."""
    print("\n📊 Generated Data Statistics")
    print("=" * 50)
    print(f"  Total records: {len(records):,}")

    if not records:
        print("  No records generated.")
        print()
        return

    # Date range
    dates = sorted(set(r["EdgeStartTimestamp"][:10] for r in records))
    print(f"  Date range: {dates[0]} to {dates[-1]}")
    print(f"  Days covered: {len(dates)}")

    # Bot breakdown
    print("\n  Bot Distribution:")
    bot_counts = {}
    for r in records:
        ua = r["ClientRequestUserAgent"]
        for profile in BOT_PROFILES:
            if profile.name in ua:
                bot_counts[profile.name] = bot_counts.get(profile.name, 0) + 1
                break

    for bot, count in sorted(bot_counts.items(), key=lambda x: -x[1]):
        pct = 100 * count / len(records)
        print(f"    {bot:20} {count:6,} ({pct:5.1f}%)")

    # Category breakdown
    print("\n  Category Breakdown:")
    cat_counts = {"training": 0, "user_request": 0}
    for r in records:
        ua = r["ClientRequestUserAgent"]
        for profile in BOT_PROFILES:
            if profile.name in ua:
                cat_counts[profile.category] += 1
                break

    for cat, count in cat_counts.items():
        pct = 100 * count / len(records)
        print(f"    {cat:20} {count:6,} ({pct:5.1f}%)")

    # Response status breakdown
    print("\n  Response Status Breakdown:")
    status_counts = {}
    for r in records:
        status = r["EdgeResponseStatus"]
        status_counts[status] = status_counts.get(status, 0) + 1

    for status, count in sorted(status_counts.items()):
        pct = 100 * count / len(records)
        print(f"    {status:20} {count:6,} ({pct:5.1f}%)")

    # Country breakdown
    print("\n  Country Breakdown (top 5):")
    country_counts = {}
    for r in records:
        country = r["ClientCountry"]
        country_counts[country] = country_counts.get(country, 0) + 1

    for country, count in sorted(country_counts.items(), key=lambda x: -x[1])[:5]:
        pct = 100 * count / len(records)
        print(f"    {country:20} {count:6,} ({pct:5.1f}%)")

    print()


# =============================================================================
# CLI
# =============================================================================


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate realistic sample LLM bot traffic data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 7 days of sample data (default, prints stats)
  python scripts/generate_sample_data.py

  # Generate and insert into SQLite
  python scripts/generate_sample_data.py --output sqlite

  # Generate high volume for 30 days
  python scripts/generate_sample_data.py --daily-requests 10000 --days 30

  # Output as JSON for inspection
  python scripts/generate_sample_data.py --output json --limit 100

  # Generate specific date range to JSONL file
  python scripts/generate_sample_data.py --start-date 2024-01-01 --end-date 2024-01-07 \\
      --output jsonl --output-path data/sample.jsonl

  # Reproducible generation with seed
  python scripts/generate_sample_data.py --seed 42
        """,
    )

    # Volume options
    parser.add_argument(
        "--daily-requests",
        type=int,
        default=1000,
        help="Average requests per day (default: 1000)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to generate (default: 7)",
    )

    # Date range options
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date (YYYY-MM-DD, default: days ago from yesterday)",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date (YYYY-MM-DD, default: yesterday)",
    )

    # Output options
    parser.add_argument(
        "--output",
        choices=["stats", "json", "jsonl", "sqlite"],
        default="stats",
        help="Output format (default: stats)",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        help="Output file path (for jsonl output)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        help="SQLite database path (default: data/llm-bot-logs.db)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit records for JSON output",
    )

    # Generator options
    parser.add_argument(
        "--hosts",
        nargs="+",
        help="Host domains to use (default: example.com variants)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Random seed for reproducibility",
    )

    # Logging
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    # Determine date range
    if args.end_date:
        end_date = args.end_date
    else:
        end_date = date.today() - timedelta(days=1)

    if args.start_date:
        start_date = args.start_date
    else:
        start_date = end_date - timedelta(days=args.days - 1)

    # Validate
    if start_date > end_date:
        logger.error(f"start_date ({start_date}) must be <= end_date ({end_date})")
        return 1

    if args.daily_requests < 1:
        logger.error(f"--daily-requests must be >= 1, got {args.daily_requests}")
        return 1

    # Print header
    print()
    print("🤖 LLM Bot Traffic Sample Data Generator")
    print("=" * 50)
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Daily requests: ~{args.daily_requests:,}")
    print(f"  Output: {args.output}")
    if args.seed:
        print(f"  Seed: {args.seed}")
    print()

    # Generate data
    generator = SampleDataGenerator(
        daily_requests=args.daily_requests,
        hosts=args.hosts,
        seed=args.seed,
    )

    records = generator.generate_date_range(start_date, end_date)

    # Output
    if args.output == "stats":
        output_stats(records)
    elif args.output == "json":
        output_json(records, limit=args.limit)
    elif args.output == "jsonl":
        output_path = args.output_path or Path("data/sample_data.jsonl")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_jsonl(records, output_path)
        output_stats(records)
    elif args.output == "sqlite":
        output_sqlite(records, db_path=args.db_path)
        output_stats(records)

    return 0


if __name__ == "__main__":
    sys.exit(main())
