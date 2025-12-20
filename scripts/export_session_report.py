#!/usr/bin/env python3
"""
Export query fan-out sessions to CSV or Excel format.

Usage:
    # Export all sessions to CSV
    python scripts/export_session_report.py --output data/reports/sessions.csv

    # Export with filters
    python scripts/export_session_report.py \
        --start-date 2025-12-10 \
        --end-date 2025-12-17 \
        --provider OpenAI \
        --min-confidence medium \
        --output data/reports/openai_sessions.csv

    # Export to Excel with multiple sheets
    python scripts/export_session_report.py \
        --format xlsx \
        --output data/reports/sessions.xlsx
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.pipeline.local_pipeline import setup_logging
from llm_bot_pipeline.storage import get_backend

logger = logging.getLogger(__name__)

# Confidence level hierarchy for filtering
CONFIDENCE_HIERARCHY = {"high": 3, "medium": 2, "low": 1}


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


def get_sessions_query(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider: Optional[str] = None,
    min_confidence: Optional[str] = None,
) -> tuple[str, dict]:
    """
    Build SQL query for fetching sessions with filters.

    Args:
        start_date: Filter sessions from this date
        end_date: Filter sessions until this date
        provider: Filter by bot provider
        min_confidence: Minimum confidence level (includes equal and higher)

    Returns:
        Tuple of (sql_query, params_dict)
    """
    query = """
        SELECT 
            session_id,
            fanout_session_name,
            session_date,
            session_start_time,
            session_end_time,
            duration_ms,
            bot_provider,
            request_count,
            unique_urls,
            mean_cosine_similarity,
            min_cosine_similarity,
            confidence_level,
            url_list
        FROM query_fanout_sessions
        WHERE 1=1
    """
    params = {}

    if start_date:
        query += " AND session_date >= :start_date"
        params["start_date"] = start_date.isoformat()

    if end_date:
        query += " AND session_date <= :end_date"
        params["end_date"] = end_date.isoformat()

    if provider:
        query += " AND bot_provider = :provider"
        params["provider"] = provider

    if min_confidence:
        min_level = CONFIDENCE_HIERARCHY.get(min_confidence.lower(), 0)
        if min_level >= 3:
            query += " AND confidence_level = 'high'"
        elif min_level >= 2:
            query += " AND confidence_level IN ('high', 'medium')"
        # min_level 1 (low) includes all, no filter needed

    query += " ORDER BY session_date DESC, session_start_time DESC"

    return query, params


def get_summary_query(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider: Optional[str] = None,
) -> tuple[str, dict]:
    """Build SQL query for daily summary aggregates."""
    query = """
        SELECT
            session_date,
            COUNT(*) AS total_sessions,
            ROUND(AVG(request_count), 2) AS avg_requests_per_session,
            ROUND(AVG(unique_urls), 2) AS avg_urls_per_session,
            ROUND(AVG(mean_cosine_similarity), 3) AS avg_coherence,
            SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) AS high_confidence,
            SUM(CASE WHEN confidence_level = 'medium' THEN 1 ELSE 0 END) AS medium_confidence,
            SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) AS low_confidence
        FROM query_fanout_sessions
        WHERE 1=1
    """
    params = {}

    if start_date:
        query += " AND session_date >= :start_date"
        params["start_date"] = start_date.isoformat()

    if end_date:
        query += " AND session_date <= :end_date"
        params["end_date"] = end_date.isoformat()

    if provider:
        query += " AND bot_provider = :provider"
        params["provider"] = provider

    query += " GROUP BY session_date ORDER BY session_date DESC"

    return query, params


def get_top_urls_query(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 50,
) -> tuple[str, dict]:
    """Build SQL query for top URLs across all sessions."""
    # Note: This requires parsing url_list JSON - for SQLite we use json_each
    query = """
        SELECT
            json_each.value AS url,
            COUNT(*) AS frequency,
            COUNT(DISTINCT session_id) AS session_count,
            ROUND(AVG(mean_cosine_similarity), 3) AS avg_coherence
        FROM query_fanout_sessions, json_each(url_list)
        WHERE 1=1
    """
    params = {}

    if start_date:
        query += " AND session_date >= :start_date"
        params["start_date"] = start_date.isoformat()

    if end_date:
        query += " AND session_date <= :end_date"
        params["end_date"] = end_date.isoformat()

    query += f"""
        GROUP BY json_each.value
        ORDER BY frequency DESC
        LIMIT {limit}
    """

    return query, params


def get_provider_stats_query(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> tuple[str, dict]:
    """Build SQL query for provider statistics."""
    query = """
        SELECT
            bot_provider,
            COUNT(*) AS total_sessions,
            SUM(request_count) AS total_requests,
            ROUND(AVG(request_count), 2) AS avg_requests_per_session,
            ROUND(AVG(unique_urls), 2) AS avg_urls_per_session,
            ROUND(AVG(mean_cosine_similarity), 3) AS avg_coherence,
            SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) AS high_confidence,
            SUM(CASE WHEN confidence_level = 'medium' THEN 1 ELSE 0 END) AS medium_confidence,
            SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) AS low_confidence,
            ROUND(
                100.0 * SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) /
                NULLIF(COUNT(*), 0),
                2
            ) AS high_confidence_pct
        FROM query_fanout_sessions
        WHERE 1=1
    """
    params = {}

    if start_date:
        query += " AND session_date >= :start_date"
        params["start_date"] = start_date.isoformat()

    if end_date:
        query += " AND session_date <= :end_date"
        params["end_date"] = end_date.isoformat()

    query += " GROUP BY bot_provider ORDER BY total_sessions DESC"

    return query, params


def export_to_csv(
    backend,
    output_path: Path,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider: Optional[str] = None,
    min_confidence: Optional[str] = None,
) -> int:
    """
    Export sessions to CSV file.

    Returns:
        Number of sessions exported
    """
    query, params = get_sessions_query(start_date, end_date, provider, min_confidence)
    rows = backend.query(query, params)

    if not rows:
        logger.warning("No sessions found matching the criteria")
        return 0

    df = pd.DataFrame(rows)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)
    logger.info(f"Exported {len(df)} sessions to {output_path}")

    return len(df)


def export_to_excel(
    backend,
    output_path: Path,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider: Optional[str] = None,
    min_confidence: Optional[str] = None,
) -> int:
    """
    Export sessions to Excel file with multiple sheets.

    Sheets:
        - Sessions: Full session data
        - Summary: Daily aggregates and KPIs
        - Top URLs: Most frequently requested URLs
        - Provider Stats: Breakdown by bot provider

    Returns:
        Number of sessions exported
    """
    # Fetch all data
    sessions_query, sessions_params = get_sessions_query(
        start_date, end_date, provider, min_confidence
    )
    sessions_rows = backend.query(sessions_query, sessions_params)

    if not sessions_rows:
        logger.warning("No sessions found matching the criteria")
        return 0

    # Fetch summary data (without min_confidence filter for full picture)
    summary_query, summary_params = get_summary_query(start_date, end_date, provider)
    summary_rows = backend.query(summary_query, summary_params)

    # Fetch top URLs
    top_urls_query, top_urls_params = get_top_urls_query(start_date, end_date)
    top_urls_rows = backend.query(top_urls_query, top_urls_params)

    # Fetch provider stats
    provider_stats_query, provider_stats_params = get_provider_stats_query(
        start_date, end_date
    )
    provider_stats_rows = backend.query(provider_stats_query, provider_stats_params)

    # Create DataFrames
    df_sessions = pd.DataFrame(sessions_rows)
    df_summary = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame()
    df_top_urls = pd.DataFrame(top_urls_rows) if top_urls_rows else pd.DataFrame()
    df_provider_stats = (
        pd.DataFrame(provider_stats_rows) if provider_stats_rows else pd.DataFrame()
    )

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to Excel with multiple sheets
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_sessions.to_excel(writer, sheet_name="Sessions", index=False)

        if not df_summary.empty:
            df_summary.to_excel(writer, sheet_name="Summary", index=False)

        if not df_top_urls.empty:
            df_top_urls.to_excel(writer, sheet_name="Top URLs", index=False)

        if not df_provider_stats.empty:
            df_provider_stats.to_excel(writer, sheet_name="Provider Stats", index=False)

        # Adjust column widths for better readability
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for column_cells in worksheet.columns:
                col_letter = column_cells[0].column_letter
                header = column_cells[0].value

                # Set specific widths for known columns
                if header == "url_list":
                    worksheet.column_dimensions[col_letter].width = 150
                elif header in ("session_id", "session_start_time", "session_end_time"):
                    worksheet.column_dimensions[col_letter].width = 30
                elif header == "fanout_session_name":
                    worksheet.column_dimensions[col_letter].width = 40
                elif header == "url":
                    worksheet.column_dimensions[col_letter].width = 80
                else:
                    # Auto-fit based on header length with minimum
                    worksheet.column_dimensions[col_letter].width = max(
                        len(str(header)) + 2, 12
                    )

    logger.info(f"Exported {len(df_sessions)} sessions to {output_path}")
    logger.info(
        f"  Sheets: Sessions ({len(df_sessions)} rows), "
        f"Summary ({len(df_summary)} rows), "
        f"Top URLs ({len(df_top_urls)} rows), "
        f"Provider Stats ({len(df_provider_stats)} rows)"
    )

    return len(df_sessions)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export query fan-out sessions to CSV or Excel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export all sessions to CSV
  python scripts/export_session_report.py --output data/reports/sessions.csv

  # Export with filters
  python scripts/export_session_report.py \\
      --start-date 2025-12-10 \\
      --end-date 2025-12-17 \\
      --provider OpenAI \\
      --min-confidence medium \\
      --output data/reports/openai_sessions.csv

  # Export to Excel with multiple sheets
  python scripts/export_session_report.py \\
      --format xlsx \\
      --output data/reports/sessions.xlsx
        """,
    )

    # Required output
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        required=True,
        help="Output file path (e.g., data/reports/sessions.csv)",
    )

    # Format selection
    parser.add_argument(
        "--format",
        "-f",
        choices=["csv", "xlsx"],
        default=None,
        help="Export format (auto-detected from output extension if not specified)",
    )

    # Database options
    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: data/llm-bot-logs.db)",
    )

    # Filtering options
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--provider",
        type=str,
        help="Filter by bot provider (e.g., OpenAI, Perplexity)",
    )
    parser.add_argument(
        "--min-confidence",
        choices=["high", "medium", "low"],
        help="Minimum confidence level to include",
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

    # Determine format from extension if not specified
    output_format = args.format
    if output_format is None:
        suffix = args.output.suffix.lower()
        if suffix == ".csv":
            output_format = "csv"
        elif suffix in (".xlsx", ".xls"):
            output_format = "xlsx"
        else:
            parser.error(
                f"Cannot determine format from extension '{suffix}'. "
                "Use --format to specify csv or xlsx"
            )

    # Initialize storage backend
    try:
        backend = get_backend(backend_type="sqlite", db_path=args.db_path)
        backend.initialize()
    except Exception as e:
        logger.error(f"Failed to initialize storage backend: {e}")
        return 1

    try:
        # Export based on format
        if output_format == "csv":
            count = export_to_csv(
                backend,
                args.output,
                start_date=args.start_date,
                end_date=args.end_date,
                provider=args.provider,
                min_confidence=args.min_confidence,
            )
        else:
            count = export_to_excel(
                backend,
                args.output,
                start_date=args.start_date,
                end_date=args.end_date,
                provider=args.provider,
                min_confidence=args.min_confidence,
            )

        if count == 0:
            print("No sessions found matching the criteria")
            return 0

        print(f"\n✅ Successfully exported {count} sessions to {args.output}")
        return 0

    except Exception as e:
        logger.exception(f"Export failed: {e}")
        return 1

    finally:
        backend.close()


if __name__ == "__main__":
    sys.exit(main())
