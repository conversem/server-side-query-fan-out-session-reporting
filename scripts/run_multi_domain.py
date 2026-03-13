#!/usr/bin/env python3
"""
Multi-domain log ingestion and reporting.

Fetches logs from multiple Cloudflare zones using a shared API token
and stores data in separate SQLite databases per domain.

Useful when a single Cloudflare API token has access to multiple zones
(e.g., multiple domains for the same organization).

Usage:
    # Fetch last 7 days for all domains
    python scripts/run_multi_domain.py --fetch

    # Fetch specific date range
    python scripts/run_multi_domain.py --fetch --start-date 2026-01-26 --end-date 2026-02-01

    # Run reports for all domains
    python scripts/run_multi_domain.py --report

    # Fetch and report (full pipeline)
    python scripts/run_multi_domain.py --fetch --report

    # Process single domain only
    python scripts/run_multi_domain.py --fetch --domain example.com

    # List configured domains
    python scripts/run_multi_domain.py --list-domains
"""

import argparse
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.config.settings import get_settings
from llm_bot_pipeline.pipeline import setup_logging
from llm_bot_pipeline.utils.date_utils import parse_date

logger = logging.getLogger(__name__)


# =============================================================================
# Domain Configuration
# =============================================================================


@dataclass
class DomainConfig:
    """Configuration for a single domain."""

    domain: str
    zone_id: str
    db_name: str  # Database filename (stored in data/)
    sitemaps: list[str] = field(default_factory=list)


def load_domains_from_config() -> list[DomainConfig]:
    """
    Load domain configurations from config.enc.yaml.

    Domains are configured in the 'domains' section of the config file.
    Each domain needs: domain name, zone_id, and db_name.

    Returns:
        List of DomainConfig objects
    """
    settings = get_settings()

    # Access raw config for domains section (not in Settings dataclass)
    from llm_bot_pipeline.config.sops_loader import decrypt_sops_file

    config_path = Path("config.enc.yaml")
    if not config_path.exists():
        logger.warning("No config.enc.yaml found, returning empty domain list")
        return []

    try:
        config = decrypt_sops_file(config_path)
        domains_config = config.get("domains", [])

        domains = []
        for d in domains_config:
            if not all(k in d for k in ("domain", "zone_id", "db_name")):
                logger.warning(f"Skipping incomplete domain config: {d}")
                continue
            domains.append(
                DomainConfig(
                    domain=d["domain"],
                    zone_id=d["zone_id"],
                    db_name=d["db_name"],
                    sitemaps=d.get("sitemaps", []),
                )
            )

        return domains

    except Exception as e:
        logger.error(f"Failed to load domains from config: {e}")
        return []


def get_domains() -> list[DomainConfig]:
    """Get list of configured domains."""
    return load_domains_from_config()


def get_domain_by_name(domain_name: str) -> DomainConfig | None:
    """Get domain config by domain name."""
    for domain in get_domains():
        if domain.domain == domain_name:
            return domain
    return None


# =============================================================================
# Ingestion
# =============================================================================


def fetch_domain_logs(
    domain: DomainConfig,
    start_date: date,
    end_date: date,
    data_dir: Path,
    dry_run: bool = False,
) -> dict:
    """
    Fetch logs for a single domain directly from Cloudflare API.

    Uses the shared API token from SOPS config with domain-specific zone_id.

    Args:
        domain: Domain configuration
        start_date: Start date for log fetch
        end_date: End date for log fetch
        data_dir: Directory to store database files
        dry_run: If True, preview only

    Returns:
        Result dictionary with success status and metrics
    """
    from llm_bot_pipeline.cloudflare.logpull import (
        RateLimiter,
        get_available_date_range,
        pull_logs,
    )
    from llm_bot_pipeline.storage import get_backend

    db_path = data_dir / domain.db_name

    print(f"\n{'='*60}")
    print(f"📥 Fetching: {domain.domain}")
    print(f"{'='*60}")
    print(f"  Zone ID: {domain.zone_id}")
    print(f"  Database: {db_path}")
    print(f"  Date range: {start_date} to {end_date}")

    # Check date range against retention
    earliest, latest = get_available_date_range()
    if start_date < earliest:
        print(
            f"  ⚠️  Adjusting start_date from {start_date} to {earliest} (retention limit)"
        )
        start_date = earliest

    if dry_run:
        print("  ⚠️  DRY RUN - no data will be written")
        return {"success": True, "domain": domain.domain, "dry_run": True}

    start_time_total = time.time()
    backend = None

    try:
        # Initialize storage backend
        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()

        # Get settings for API token (zone_id will be passed explicitly)
        settings = get_settings()

        # Convert dates to datetimes (UTC)
        start_dt = datetime.combine(
            start_date, datetime.min.time(), tzinfo=timezone.utc
        )
        end_dt = datetime.combine(
            end_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
        )

        # Fetch logs with explicit zone_id
        rate_limiter = RateLimiter()
        records_ingested = 0
        batch: list[dict] = []
        batch_size = 1000

        print(f"  Starting log pull...")

        for record in pull_logs(
            start_time=start_dt,
            end_time=end_dt,
            zone_id=domain.zone_id,
            settings=settings,
            filter_llm_bots=True,
            rate_limiter=rate_limiter,
        ):
            batch.append(record)

            if len(batch) >= batch_size:
                inserted = backend.insert_raw_records(batch)
                records_ingested += inserted
                print(f"    Ingested {records_ingested:,} records...", end="\r")
                batch = []

        # Insert remaining records
        if batch:
            inserted = backend.insert_raw_records(batch)
            records_ingested += inserted

        duration = time.time() - start_time_total

        print(f"\n  ✅ Success")
        print(f"  Records ingested: {records_ingested:,}")
        print(f"  Duration: {duration:.1f}s")

        return {
            "success": True,
            "domain": domain.domain,
            "raw_rows": records_ingested,
            "transformed_rows": 0,  # Transform happens separately
            "duration_seconds": duration,
            "errors": [],
        }

    except Exception as e:
        logger.exception(f"Failed to fetch logs for {domain.domain}")
        print(f"  ❌ Error: {e}")
        return {
            "success": False,
            "domain": domain.domain,
            "error": str(e),
        }

    finally:
        if backend is not None:
            backend.close()


def run_etl_for_domain(
    domain: DomainConfig,
    start_date: date,
    end_date: date,
    data_dir: Path,
    dry_run: bool = False,
) -> dict:
    """Run ETL pipeline to transform raw logs into bot_requests_daily."""
    import subprocess

    db_path = data_dir / domain.db_name

    print(f"\n  📊 Running ETL transformation...")

    cmd = [
        sys.executable,
        "scripts/run_pipeline.py",
        "--start-date",
        str(start_date),
        "--end-date",
        str(end_date),
        "--db-path",
        str(db_path),
        "--mode",
        "incremental",
        "--skip-sessions",  # Sessions are run separately
    ]

    if dry_run:
        cmd.append("--dry-run")

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"  ETL completed")
        return {"success": True}
    except subprocess.CalledProcessError as e:
        logger.error(f"ETL stderr: {e.stderr}")
        print(f"  ❌ ETL failed: {e}")
        return {"success": False, "error": str(e)}


def run_session_aggregation_for_domain(
    domain: DomainConfig,
    start_date: date,
    end_date: date,
    data_dir: Path,
    dry_run: bool = False,
) -> dict:
    """Run session aggregation for a domain's database."""
    # Import here to avoid circular dependency
    from run_pipeline import run_session_aggregation

    db_path = data_dir / domain.db_name

    print(f"\n  🔗 Running session aggregation...")

    result = run_session_aggregation(
        db_path=db_path,
        start_date=start_date,
        end_date=end_date,
        dry_run=dry_run,
    )

    print(f"  Sessions created: {result['sessions_created']:,}")
    print(f"  Requests processed: {result['requests_processed']:,}")

    return result


# =============================================================================
# Sitemap Fetching
# =============================================================================


def fetch_sitemaps_for_domain(domain: DomainConfig, data_dir: Path) -> dict:
    """Fetch XML sitemaps and store URL/lastmod data for a domain.

    Skips silently if no sitemaps are configured for the domain.

    Args:
        domain: Domain configuration (with sitemaps list)
        data_dir: Directory containing database files

    Returns:
        Result dictionary with success status and URL count
    """
    if not domain.sitemaps:
        return {"success": True, "domain": domain.domain, "urls": 0, "skipped": True}

    from llm_bot_pipeline.sitemap import fetch_sitemap
    from llm_bot_pipeline.storage import get_backend

    db_path = data_dir / domain.db_name

    print(f"\n  🗺️  Fetching sitemaps for {domain.domain}...")

    try:
        all_entries = []
        for sitemap_url in domain.sitemaps:
            entries = fetch_sitemap(sitemap_url)
            all_entries.extend(entries)
            print(f"    {sitemap_url}: {len(entries)} URLs")

        if not all_entries:
            print(f"    No URLs found in sitemaps")
            return {"success": True, "domain": domain.domain, "urls": 0}

        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()

        try:
            entry_dicts = [
                {
                    "url": e.url,
                    "url_path": e.url_path,
                    "lastmod": e.lastmod,
                    "lastmod_month": e.lastmod_month,
                    "sitemap_source": e.sitemap_source,
                }
                for e in all_entries
            ]
            count = backend.insert_sitemap_urls(entry_dicts)
            print(f"    ✅ Stored {count} sitemap URLs")
            return {"success": True, "domain": domain.domain, "urls": count}
        finally:
            backend.close()

    except Exception as e:
        logger.exception(f"Sitemap fetch failed for {domain.domain}")
        print(f"    ❌ Error: {e}")
        return {"success": False, "domain": domain.domain, "error": str(e)}


# =============================================================================
# Reporting
# =============================================================================


def run_domain_report(domain: DomainConfig, data_dir: Path) -> dict:
    """
    Run report for a single domain.

    Generates console summary and Excel report file.

    Args:
        domain: Domain configuration
        data_dir: Directory containing database files

    Returns:
        Result dictionary with report data
    """
    from export_session_report import export_to_excel

    from llm_bot_pipeline.reporting.session_aggregations import SessionAggregator
    from llm_bot_pipeline.storage import get_backend

    db_path = data_dir / domain.db_name

    if not db_path.exists():
        print(f"\n⚠️  {domain.domain}: No database found at {db_path}")
        return {"success": False, "domain": domain.domain, "error": "No database"}

    print(f"\n{'='*60}")
    print(f"📊 Report: {domain.domain}")
    print(f"{'='*60}")

    backend = None
    excel_path = None

    try:
        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()

        # Basic stats query
        stats = backend.query("""
            SELECT
                COUNT(*) as total_requests,
                COUNT(DISTINCT request_date) as days_with_data,
                MIN(request_date) as first_date,
                MAX(request_date) as last_date,
                COUNT(DISTINCT bot_provider) as unique_providers
            FROM bot_requests_daily
            """)

        if not (stats and stats[0]["total_requests"]):
            print("  No data in database")
            return {"success": True, "domain": domain.domain, "total_requests": 0}

        s = stats[0]
        print(f"  Total requests: {s['total_requests']:,}")
        print(f"  Date range: {s['first_date']} to {s['last_date']}")
        print(f"  Days with data: {s['days_with_data']}")
        print(f"  Unique bot providers: {s['unique_providers']}")

        # Bot breakdown
        bot_stats = backend.query("""
            SELECT bot_provider, bot_name, COUNT(*) as count
            FROM bot_requests_daily
            GROUP BY bot_provider, bot_name
            ORDER BY count DESC
            LIMIT 10
            """)

        if bot_stats:
            print(f"\n  Top bots:")
            for bot in bot_stats:
                print(f"    {bot['bot_provider']}/{bot['bot_name']}: {bot['count']:,}")

        # Session stats if available
        session_stats = backend.query("""
            SELECT
                COUNT(*) as total_sessions,
                AVG(request_count) as avg_requests,
                SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) as high_conf,
                SUM(CASE WHEN confidence_level = 'medium' THEN 1 ELSE 0 END) as med_conf,
                SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) as low_conf
            FROM query_fanout_sessions
            """)

        if session_stats and session_stats[0]["total_sessions"]:
            ss = session_stats[0]
            print(f"\n  Sessions: {ss['total_sessions']:,}")
            print(f"  Avg requests/session: {ss['avg_requests']:.1f}")
            print(
                f"  Confidence: high={ss['high_conf']}, "
                f"medium={ss['med_conf']}, low={ss['low_conf']}"
            )

            # Populate session_url_details for any sessions missing URL details
            missing_dates = backend.query("""
                SELECT DISTINCT session_date FROM query_fanout_sessions
                WHERE session_date NOT IN (
                    SELECT DISTINCT session_date FROM session_url_details
                )
                ORDER BY session_date
                """)
            if missing_dates:
                dates_list = [r["session_date"] for r in missing_dates]
                print(
                    f"\n  📋 Populating URL details for {len(dates_list)} missing dates..."
                )
                aggregator = SessionAggregator(backend)
                total_created = 0
                for d in dates_list:
                    result = aggregator.populate_session_url_details(session_date=d)
                    total_created += result["url_details_created"]
                print(f"     Created {total_created} URL detail rows")

            # Generate Excel report
            reports_dir = data_dir / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)

            domain_slug = domain.domain.split(".")[0]
            today_str = date.today().strftime("%Y%m%d")
            excel_path = reports_dir / f"{domain_slug}_report_{today_str}.xlsx"

            print(f"\n  📊 Generating Excel report...")
            session_count = export_to_excel(backend, excel_path)
            print(f"     Exported {session_count} sessions to {excel_path}")

        return {
            "success": True,
            "domain": domain.domain,
            "total_requests": s["total_requests"],
            "date_range": f"{s['first_date']} to {s['last_date']}",
            "excel_report": str(excel_path) if excel_path else None,
        }

    except Exception as e:
        logger.exception(f"Report failed for {domain.domain}")
        print(f"  ❌ Error: {e}")
        return {"success": False, "domain": domain.domain, "error": str(e)}

    finally:
        if backend is not None:
            backend.close()


# =============================================================================
# Main
# =============================================================================


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Multi-domain log ingestion and reporting",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Actions
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Fetch logs from Cloudflare for all domains",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Run reports for all domains",
    )
    parser.add_argument(
        "--list-domains",
        action="store_true",
        help="List configured domains and exit",
    )

    # Date range
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date (YYYY-MM-DD). Default: 7 days ago",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date (YYYY-MM-DD). Default: yesterday",
    )

    # Filtering
    parser.add_argument(
        "--domain",
        type=str,
        help="Process single domain only (e.g., example.com)",
    )

    # Options
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory for database files (default: data/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without writing data",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    # List domains and exit
    if args.list_domains:
        domains = get_domains()
        print("\n📋 Configured Domains")
        print("=" * 60)
        if not domains:
            print("  No domains configured in config.enc.yaml")
            print("  Add a 'domains' section to your config file.")
        else:
            for domain in domains:
                print(f"  {domain.domain}")
                print(f"    Zone ID: {domain.zone_id}")
                print(f"    Database: {domain.db_name}")
                if domain.sitemaps:
                    print(f"    Sitemaps: {', '.join(domain.sitemaps)}")
        return 0

    # Validate actions
    if not args.fetch and not args.report:
        parser.error("Must specify --fetch and/or --report")

    # Determine date range
    if args.start_date:
        start_date = args.start_date
    else:
        start_date = date.today() - timedelta(days=7)

    if args.end_date:
        end_date = args.end_date
    else:
        end_date = date.today() - timedelta(days=1)

    # Validate date range
    if start_date > end_date:
        parser.error(f"Start date ({start_date}) must be before end date ({end_date})")

    # Load domains from config
    all_domains = get_domains()
    if not all_domains:
        parser.error(
            "No domains configured. Add a 'domains' section to config.enc.yaml.\n"
            "See config.example.yaml for the expected format."
        )

    # Filter domains if specified
    if args.domain:
        domain = get_domain_by_name(args.domain)
        if domain is None:
            parser.error(
                f"Unknown domain: {args.domain}. "
                f"Available: {', '.join(d.domain for d in all_domains)}"
            )
        domains_to_process = [domain]
    else:
        domains_to_process = all_domains

    # Ensure data directory exists
    args.data_dir.mkdir(parents=True, exist_ok=True)

    # Header
    print("\n🌐 Multi-Domain Pipeline")
    print("=" * 60)
    print(f"  Domains: {len(domains_to_process)}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Data directory: {args.data_dir}")
    if args.dry_run:
        print("  ⚠️  DRY RUN MODE")

    # Track results
    fetch_results = []
    report_results = []

    # Fetch logs
    if args.fetch:
        print("\n" + "=" * 60)
        print("📥 FETCHING LOGS")
        print("=" * 60)

        for domain in domains_to_process:
            # Skip domains with placeholder zone IDs
            if domain.zone_id.startswith("ZONE_ID_PLACEHOLDER"):
                print(f"\n⚠️  Skipping {domain.domain} - zone ID not configured")
                fetch_results.append(
                    {
                        "success": False,
                        "domain": domain.domain,
                        "error": "Zone ID not configured",
                    }
                )
                continue

            result = fetch_domain_logs(
                domain=domain,
                start_date=start_date,
                end_date=end_date,
                data_dir=args.data_dir,
                dry_run=args.dry_run,
            )
            fetch_results.append(result)

            # Run ETL and session aggregation if fetch succeeded
            if result.get("success") and not args.dry_run:
                # Step 1: Transform raw logs to daily aggregates
                run_etl_for_domain(
                    domain=domain,
                    start_date=start_date,
                    end_date=end_date,
                    data_dir=args.data_dir,
                    dry_run=args.dry_run,
                )

                # Step 2: Aggregate into sessions
                run_session_aggregation_for_domain(
                    domain=domain,
                    start_date=start_date,
                    end_date=end_date,
                    data_dir=args.data_dir,
                    dry_run=args.dry_run,
                )

                # Step 3: Fetch sitemaps for URL freshness
                fetch_sitemaps_for_domain(domain=domain, data_dir=args.data_dir)

    # Run reports
    if args.report:
        print("\n" + "=" * 60)
        print("📊 RUNNING REPORTS")
        print("=" * 60)

        for domain in domains_to_process:
            # Refresh sitemaps before report (ensures data is current even for report-only runs)
            fetch_sitemaps_for_domain(domain=domain, data_dir=args.data_dir)
            result = run_domain_report(domain=domain, data_dir=args.data_dir)
            report_results.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY")
    print("=" * 60)

    if fetch_results:
        successful_fetches = sum(1 for r in fetch_results if r.get("success"))
        print(f"  Fetch: {successful_fetches}/{len(fetch_results)} domains successful")
        total_rows = sum(r.get("raw_rows", 0) for r in fetch_results)
        print(f"  Total rows fetched: {total_rows:,}")

    if report_results:
        successful_reports = sum(1 for r in report_results if r.get("success"))
        print(
            f"  Reports: {successful_reports}/{len(report_results)} domains successful"
        )

    # Return success if all operations succeeded
    all_success = all(r.get("success") for r in fetch_results + report_results)
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
