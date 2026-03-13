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
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
# Add src to path for imports
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from llm_bot_pipeline.pipeline.local_pipeline import setup_logging
from llm_bot_pipeline.pipeline.sql_compat import json_array_unnest
from llm_bot_pipeline.storage import get_backend
from llm_bot_pipeline.utils.date_utils import parse_date
from llm_bot_pipeline.utils.path_utils import validate_path_safe

logger = logging.getLogger(__name__)

# Confidence level hierarchy for filtering
CONFIDENCE_HIERARCHY = {"high": 3, "medium": 2, "low": 1}

# Dashboard view sheets: maps Excel sheet name to database view name.
# SQLite views are created by sqlite_backend.py during initialize().
# BigQuery views are created by bigquery_views.py during initialize().
DASHBOARD_VIEW_SHEETS = {
    "Daily KPIs": "v_daily_kpis",
    "URL Distribution": "v_session_url_distribution",
    "Singleton Binary": "v_session_singleton_binary",
    "Bot Volume": "v_bot_volume",
    "Top Topics": "v_top_session_topics",
    "Category Comparison": "v_category_comparison",
    "URL Cooccurrence": "v_url_cooccurrence",
}

# View name to date column mapping for filtering.
# Most views use session_date; v_category_comparison uses date.
VIEW_DATE_COLUMNS = {
    "v_category_comparison": "date",
}

# Default date column for views not in VIEW_DATE_COLUMNS
_DEFAULT_DATE_COLUMN = "session_date"


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
            bot_name,
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
            (SELECT COUNT(DISTINCT sud.url)
             FROM session_url_details sud
             WHERE sud.session_date = q.session_date) AS unique_urls_requested,
            ROUND(AVG(request_count), 2) AS avg_requests_per_session,
            ROUND(AVG(unique_urls), 2) AS avg_urls_per_session,
            ROUND(AVG(mean_cosine_similarity), 3) AS avg_coherence,
            SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) AS high_confidence,
            SUM(CASE WHEN confidence_level = 'medium' THEN 1 ELSE 0 END) AS medium_confidence,
            SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) AS low_confidence
        FROM query_fanout_sessions q
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
    backend_type: str = "sqlite",
) -> tuple[str, dict]:
    """Build SQL query for top URLs across all sessions."""
    from_clause, value_expr = json_array_unnest(
        "query_fanout_sessions", "url_list", backend_type
    )
    query = f"""
        SELECT
            {value_expr} AS url,
            COUNT(*) AS frequency,
            COUNT(DISTINCT session_id) AS session_count,
            ROUND(AVG(mean_cosine_similarity), 3) AS avg_coherence
        FROM {from_clause}
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
        GROUP BY {value_expr}
        ORDER BY frequency DESC
        LIMIT {limit}
    """

    return query, params


def get_provider_stats_query(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> tuple[str, dict]:
    """Build SQL query for provider statistics (grouped by provider + bot_name).

    Combines session-based stats for user_request bots with request-based
    stats for search bots from daily_summary.
    """
    # Session-based stats for user_request bots
    session_part = """
        SELECT
            bot_provider,
            bot_name,
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
        session_part += " AND session_date >= :start_date"
        params["start_date"] = start_date.isoformat()

    if end_date:
        session_part += " AND session_date <= :end_date"
        params["end_date"] = end_date.isoformat()

    session_part += " GROUP BY bot_provider, bot_name"

    # Request-based stats for search bots (no sessions)
    search_part = """
        SELECT
            bot_provider,
            bot_name,
            NULL AS total_sessions,
            SUM(total_requests) AS total_requests,
            NULL AS avg_requests_per_session,
            NULL AS avg_urls_per_session,
            NULL AS avg_coherence,
            NULL AS high_confidence,
            NULL AS medium_confidence,
            NULL AS low_confidence,
            NULL AS high_confidence_pct
        FROM daily_summary
        WHERE bot_category = 'search'
    """

    if start_date:
        search_part += " AND request_date >= :start_date"

    if end_date:
        search_part += " AND request_date <= :end_date"

    search_part += " GROUP BY bot_provider, bot_name"

    query = session_part + "\n\n        UNION ALL\n" + search_part
    query += "\n        ORDER BY total_requests DESC"

    return query, params


def get_view_query(
    view_name: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> tuple[str, dict]:
    """
    Build SQL query for a reporting view with date filtering.

    Args:
        view_name: Name of the view to query (must be in DASHBOARD_VIEW_SHEETS)
        start_date: Filter from this date
        end_date: Filter until this date

    Returns:
        Tuple of (sql_query, params_dict)

    Raises:
        ValueError: If view_name is not a known dashboard view
    """
    # Validate against whitelist to prevent SQL injection
    allowed_views = set(DASHBOARD_VIEW_SHEETS.values())
    if view_name not in allowed_views:
        raise ValueError(f"Unknown view: {view_name}. Allowed: {sorted(allowed_views)}")

    date_col = VIEW_DATE_COLUMNS.get(view_name, _DEFAULT_DATE_COLUMN)
    query = f"SELECT * FROM {view_name} WHERE 1=1"
    params = {}

    if start_date:
        query += f" AND {date_col} >= :start_date"
        params["start_date"] = start_date.isoformat()

    if end_date:
        query += f" AND {date_col} <= :end_date"
        params["end_date"] = end_date.isoformat()

    query += f" ORDER BY {date_col} DESC"

    return query, params


def get_url_details_query(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> tuple[str, dict]:
    """
    Build SQL query for session URL details table.

    Args:
        start_date: Filter from this date
        end_date: Filter until this date

    Returns:
        Tuple of (sql_query, params_dict)
    """
    query = """
        SELECT
            sud.session_id,
            sud.session_date,
            sud.url,
            sm.lastmod,
            sm.lastmod_month,
            sud.url_position,
            sud.bot_provider,
            sud.bot_name,
            sud.fanout_session_name,
            sud.session_unique_urls,
            sud.session_request_count,
            sud.session_duration_ms,
            sud.mean_cosine_similarity,
            sud.confidence_level
        FROM session_url_details sud
        LEFT JOIN sitemap_urls sm ON sud.url = sm.url_path
        WHERE 1=1
    """
    params = {}

    if start_date:
        query += " AND sud.session_date >= :start_date"
        params["start_date"] = start_date.isoformat()

    if end_date:
        query += " AND sud.session_date <= :end_date"
        params["end_date"] = end_date.isoformat()

    query += " ORDER BY sud.session_date DESC, sud.session_id, sud.url_position"

    return query, params


def _get_freshness_base_query(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> tuple[str, dict]:
    """Build the base query for freshness analysis (session URLs joined with sitemap lastmod)."""
    query = """
        SELECT
            sud.session_date,
            sud.url,
            sm.lastmod_month
        FROM session_url_details sud
        LEFT JOIN sitemap_urls sm ON sud.url = sm.url_path
        WHERE 1=1
    """
    params = {}

    if start_date:
        query += " AND sud.session_date >= :start_date"
        params["start_date"] = start_date.isoformat()

    if end_date:
        query += " AND sud.session_date <= :end_date"
        params["end_date"] = end_date.isoformat()

    return query, params


def build_freshness_sheet(
    backend,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Optional[pd.DataFrame]:
    """Build URL Freshness pivot: session_date x lastmod_month.

    Scoped to URLs with a known sitemap lastmod only.
    Columns per lastmod_month (most recent first):
      - request count: raw number of request rows
      - unique urls updated: total sitemap pages with that lastmod_month
      - pct requested: unique URLs requested / total sitemap URLs for month
    """
    query, params = _get_freshness_base_query(start_date, end_date)
    rows = backend.query(query, params)
    if not rows:
        return None

    df = pd.DataFrame(rows)
    if df.empty or df["lastmod_month"].isna().all():
        return None

    df = df.dropna(subset=["lastmod_month"])
    if df.empty:
        return None

    # Total sitemap URLs per lastmod_month (independent of requests)
    sitemap_totals_rows = backend.query(
        "SELECT lastmod_month, COUNT(*) AS total FROM sitemap_urls "
        "WHERE lastmod_month IS NOT NULL GROUP BY lastmod_month"
    )
    sitemap_totals = {r["lastmod_month"]: r["total"] for r in sitemap_totals_rows}

    # Raw request count per session_date x lastmod_month
    request_count_pivot = (
        df.groupby(["session_date", "lastmod_month"]).size().unstack(fill_value=0)
    )

    # Unique URLs requested per session_date x lastmod_month
    unique_requested_pivot = (
        df.groupby(["session_date", "lastmod_month"])["url"]
        .nunique()
        .unstack(fill_value=0)
    )

    # Sort months descending (most recent first)
    months = sorted(request_count_pivot.columns, reverse=True)
    request_count_pivot = request_count_pivot.reindex(columns=months, fill_value=0)
    unique_requested_pivot = unique_requested_pivot.reindex(
        columns=months, fill_value=0
    )

    # Build sitemap totals row (constant across dates)
    sitemap_totals_df = pd.DataFrame(
        {m: sitemap_totals.get(m, 0) for m in months},
        index=request_count_pivot.index,
    )

    # Pct requested = unique URLs requested / total sitemap URLs for that month
    pct_df = (
        unique_requested_pivot.div(
            pd.Series({m: sitemap_totals.get(m, 1) for m in months})
        )
        .mul(100)
        .round(1)
    )

    # Combine into MultiIndex columns: (month, metric)
    combined = pd.concat(
        {
            "request count": request_count_pivot,
            "urls in sitemap": sitemap_totals_df,
            "pct requested": pct_df,
        },
        axis=1,
    ).swaplevel(axis=1)

    # Reorder so each month group has metrics together, months descending
    ordered_cols = []
    for m in months:
        ordered_cols.append((m, "request count"))
        ordered_cols.append((m, "urls in sitemap"))
        ordered_cols.append((m, "pct requested"))
    combined = combined[ordered_cols]

    # Add total request count column
    combined[("total", "request count")] = request_count_pivot.sum(axis=1)

    combined = combined.sort_index()
    return combined


def _prepare_freshness_decay_df(
    backend,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Optional[pd.DataFrame]:
    """Shared prep for decay sheets: filter to known lastmod, compute months_ago."""
    query, params = _get_freshness_base_query(start_date, end_date)
    rows = backend.query(query, params)
    if not rows:
        return None

    df = pd.DataFrame(rows)
    if df.empty or df["lastmod_month"].isna().all():
        return None

    df = df.dropna(subset=["lastmod_month"])
    if df.empty:
        return None

    df["session_month"] = pd.to_datetime(df["session_date"]).dt.to_period("M")
    df["lastmod_period"] = pd.to_datetime(df["lastmod_month"]).dt.to_period("M")
    df["months_ago"] = (df["session_month"] - df["lastmod_period"]).apply(
        lambda x: x.n if hasattr(x, "n") else None
    )
    df = df.dropna(subset=["months_ago"])
    df["months_ago"] = df["months_ago"].astype(int)
    return df


def build_decay_unique_urls(
    backend,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    max_months: int = 36,
) -> Optional[pd.DataFrame]:
    """Freshness Decay by unique URLs: cumulative % within last N months.

    Scoped to sitemap-known URLs only. Denominator is unique URLs with a
    known lastmod requested that day.
    """
    df = _prepare_freshness_decay_df(backend, start_date, end_date)
    if df is None:
        return None

    result_rows = []
    for session_date, group in df.groupby("session_date"):
        unique_urls = group.drop_duplicates(subset=["url"])
        total = len(unique_urls)
        if total == 0:
            continue
        row = {"session_date": session_date}
        for n in range(1, max_months + 1):
            within_n = unique_urls[unique_urls["months_ago"] <= n]
            row[f"<={n}mo"] = round(len(within_n) / total * 100, 1)
        result_rows.append(row)

    if not result_rows:
        return None
    return pd.DataFrame(result_rows).set_index("session_date").sort_index()


def build_decay_request_volume(
    backend,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    max_months: int = 36,
) -> Optional[pd.DataFrame]:
    """Freshness Decay by request volume: cumulative % within last N months.

    Scoped to sitemap-known URLs only. Denominator is total request rows
    (not unique URLs) with a known lastmod that day.
    """
    df = _prepare_freshness_decay_df(backend, start_date, end_date)
    if df is None:
        return None

    result_rows = []
    for session_date, group in df.groupby("session_date"):
        total = len(group)
        if total == 0:
            continue
        row = {"session_date": session_date}
        for n in range(1, max_months + 1):
            within_n = group[group["months_ago"] <= n]
            row[f"<={n}mo"] = round(len(within_n) / total * 100, 1)
        result_rows.append(row)

    if not result_rows:
        return None
    return pd.DataFrame(result_rows).set_index("session_date").sort_index()


def build_sitemap_freshness_summary(backend) -> Optional[pd.DataFrame]:
    """Build Sitemap Freshness summary from pre-aggregated sitemap_freshness table.

    Returns DataFrame with per-URL freshness metrics including unique_urls,
    or None if the table is empty.
    """
    try:
        rows = backend.query(
            "SELECT url_path, lastmod, lastmod_month, sitemap_source, "
            "first_seen_date, last_seen_date, request_count, unique_urls, "
            "unique_bots, days_since_lastmod "
            "FROM sitemap_freshness ORDER BY url_path"
        )
    except Exception as e:
        logger.debug(
            "Skipping Sitemap Summary: sitemap_freshness table not available: %s", e
        )
        return None
    if not rows:
        return None
    return pd.DataFrame(rows)


def build_volume_decay_summary(backend) -> Optional[pd.DataFrame]:
    """Build URL Volume Decay summary from pre-aggregated url_volume_decay table.

    Returns DataFrame with per-URL per-period volume metrics including
    unique_urls, or None if the table is empty.
    """
    try:
        rows = backend.query(
            "SELECT url_path, period, period_start, request_count, "
            "unique_urls, unique_bots, prev_request_count, decay_rate "
            "FROM url_volume_decay ORDER BY period, period_start DESC, url_path"
        )
    except Exception as e:
        logger.debug(
            "Skipping URL Volume Decay: url_volume_decay table not available: %s", e
        )
        return None
    if not rows:
        return None
    return pd.DataFrame(rows)


def _build_sessions_sheet(
    backend,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider: Optional[str] = None,
    min_confidence: Optional[str] = None,
) -> Optional[dict[str, pd.DataFrame]]:
    """Build Sessions, Summary, Top URLs, Provider Stats, URL Details sheets."""
    sessions_query, sessions_params = get_sessions_query(
        start_date, end_date, provider, min_confidence
    )
    sessions_rows = backend.query(sessions_query, sessions_params)
    if not sessions_rows:
        return None

    sheets = {"Sessions": pd.DataFrame(sessions_rows)}

    summary_query, summary_params = get_summary_query(start_date, end_date, provider)
    if summary_rows := backend.query(summary_query, summary_params):
        sheets["Summary"] = pd.DataFrame(summary_rows)

    top_urls_query, top_urls_params = get_top_urls_query(
        start_date, end_date, backend_type=backend.backend_type
    )
    if top_urls_rows := backend.query(top_urls_query, top_urls_params):
        sheets["Top URLs"] = pd.DataFrame(top_urls_rows)

    provider_stats_query, provider_stats_params = get_provider_stats_query(
        start_date, end_date
    )
    if provider_stats_rows := backend.query(
        provider_stats_query, provider_stats_params
    ):
        sheets["Provider Stats"] = pd.DataFrame(provider_stats_rows)

    try:
        url_details_query, url_details_params = get_url_details_query(
            start_date, end_date
        )
        if url_details_rows := backend.query(url_details_query, url_details_params):
            sheets["URL Details"] = pd.DataFrame(url_details_rows)
            logger.debug(f"Fetched {len(url_details_rows)} rows for URL Details")
    except Exception as e:
        logger.warning(f"Failed to fetch URL Details: {e}")

    return sheets


def _build_dashboard_sheets(
    backend,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> dict[str, pd.DataFrame]:
    """Build dashboard view sheets (Daily KPIs, URL Distribution, etc.)."""
    sheets = {}
    for sheet_name, view_name in DASHBOARD_VIEW_SHEETS.items():
        try:
            query, params = get_view_query(view_name, start_date, end_date)
            rows = backend.query(query, params)
            if rows:
                sheets[sheet_name] = pd.DataFrame(rows)
                logger.debug(f"Fetched {len(rows)} rows for {sheet_name}")
        except Exception as e:
            logger.warning(f"Failed to fetch {sheet_name} from {view_name}: {e}")
    return sheets


def _build_freshness_sheets(
    backend,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> dict[str, tuple[pd.DataFrame, bool]]:
    """Build freshness sheets. Returns dict of sheet_name -> (df, use_index)."""
    sheets = {}

    try:
        if df := build_freshness_sheet(backend, start_date, end_date):
            sheets["URL Freshness"] = (df, True)
            logger.debug(f"Built URL Freshness: {df.shape}")
    except Exception as e:
        logger.warning(f"Failed to build URL Freshness sheet: {e}")

    try:
        if df := build_decay_unique_urls(backend, start_date, end_date):
            sheets["Decay Unique URLs"] = (df, True)
            logger.debug(f"Built Decay (Unique URLs): {df.shape}")
    except Exception as e:
        logger.warning(f"Failed to build Decay (Unique URLs) sheet: {e}")

    try:
        if df := build_decay_request_volume(backend, start_date, end_date):
            sheets["Decay Request Volume"] = (df, True)
            logger.debug(f"Built Decay (Request Volume): {df.shape}")
    except Exception as e:
        logger.warning(f"Failed to build Decay (Request Volume) sheet: {e}")

    try:
        if df := build_sitemap_freshness_summary(backend):
            sheets["Sitemap Summary"] = (df, False)
            logger.debug(f"Built Sitemap Summary: {df.shape}")
    except Exception as e:
        logger.warning(f"Failed to build Sitemap Summary sheet: {e}")

    try:
        if df := build_volume_decay_summary(backend):
            sheets["URL Volume Decay"] = (df, False)
            logger.debug(f"Built Volume Decay: {df.shape}")
    except Exception as e:
        logger.warning(f"Failed to build Volume Decay sheet: {e}")

    return sheets


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

    Sheets (Existing):
        - Sessions: Full session data
        - Summary: Daily aggregates and KPIs
        - Top URLs: Most frequently requested URLs
        - Provider Stats: Breakdown by bot provider

    Sheets (Dashboard Views):
        - Daily KPIs: Daily key performance indicators
        - URL Distribution: Session distribution by URL count buckets
        - Singleton Binary: Session split between singleton/plural
        - Bot Volume: Session counts by bot
        - Top Topics: Session topics with metrics
        - Category Comparison: User questions vs training data
        - URL Cooccurrence: URL co-occurrence in multi-URL sessions

    Sheets (Detail Table):
        - URL Details: Flattened session URLs with metadata (incl. lastmod)

    Sheets (Freshness Analysis):
        - URL Freshness: Pivot of session_date x lastmod_month
          (request count, urls in sitemap, pct requested)
        - Decay Unique URLs: Cumulative % of unique URLs within last N months
        - Decay Request Volume: Cumulative % of request volume within last N months

    Returns:
        Number of sessions exported
    """
    sessions_sheets = _build_sessions_sheet(
        backend, start_date, end_date, provider, min_confidence
    )
    if sessions_sheets is None:
        logger.warning("No sessions found matching the criteria")
        return 0

    dashboard_sheets = _build_dashboard_sheets(backend, start_date, end_date)
    freshness_sheets = _build_freshness_sheets(backend, start_date, end_date)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in {**sessions_sheets, **dashboard_sheets}.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        for sheet_name, (df, use_index) in freshness_sheets.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=sheet_name, index=use_index)
        _adjust_column_widths(writer)

    session_count = len(sessions_sheets.get("Sessions", pd.DataFrame()))
    logger.info(f"Exported {session_count} sessions to {output_path}")
    all_sheets = {**sessions_sheets, **dashboard_sheets}
    logger.info(
        f"  Sheets: {', '.join(f'{n} ({len(d)} rows)' for n, d in all_sheets.items())}"
    )
    return session_count


def _adjust_column_widths(writer: pd.ExcelWriter) -> None:
    """
    Adjust column widths for all sheets in the Excel writer.

    Uses predefined widths for known columns and auto-fits others
    based on header length.
    """
    # Column width configuration
    column_widths = {
        # URL columns (wide)
        "url_list": 150,
        "url": 80,
        # Session identifiers
        "session_id": 30,
        "session_start_time": 30,
        "session_end_time": 30,
        "fanout_session_name": 40,
        "topic": 50,
        # Bot/provider columns
        "bot_name": 30,
        "bot_provider": 20,
        # Category/type columns
        "category": 30,
        "url_bucket": 20,
        "session_type": 25,
        "confidence_level": 18,
        # Date columns
        "session_date": 15,
        "date": 15,
        # Count columns (narrower)
        "unique_urls_requested": 22,
        "session_count": 15,
        "total_sessions": 15,
        "singleton_count": 15,
        "multi_url_count": 15,
        "high_confidence_count": 20,
        "medium_confidence_count": 22,
        "low_confidence_count": 20,
        # Rate/percentage columns
        "singleton_rate": 15,
        "multi_url_rate": 15,
        "high_confidence_rate": 20,
        # Metric columns
        "avg_urls_per_session": 20,
        "mean_cosine_similarity": 22,
        "avg_mibcs_multi_url": 20,
        "mean_mibcs_multi_url": 20,
        # Sitemap/freshness columns
        "lastmod": 14,
        "lastmod_month": 14,
        "total": 10,
    }

    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        for column_cells in worksheet.columns:
            # Skip merged cells (e.g. from MultiIndex column headers)
            first_cell = column_cells[0]
            if not hasattr(first_cell, "column_letter"):
                continue

            col_letter = first_cell.column_letter
            header = first_cell.value

            if header in column_widths:
                worksheet.column_dimensions[col_letter].width = column_widths[header]
            else:
                # Auto-fit based on header length with minimum
                worksheet.column_dimensions[col_letter].width = max(
                    len(str(header)) + 2, 12
                )


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

    # Backend options
    parser.add_argument(
        "--backend",
        choices=["sqlite", "bigquery"],
        default=None,
        help="Storage backend (default: from settings)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: from settings)",
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
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Use structured JSON logging (cloud environments)",
    )

    args = parser.parse_args()
    validate_path_safe(args.output, PROJECT_ROOT)

    # Setup logging
    setup_logging(
        level=logging.DEBUG if args.verbose else logging.INFO,
        json_logs=args.json_logs,
    )

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
        kwargs = {}
        if args.db_path:
            kwargs["db_path"] = args.db_path
        backend = get_backend(args.backend, **kwargs)
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

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130

    except Exception as e:
        logger.exception(f"Export failed: {e}")
        return 1

    finally:
        backend.close()


if __name__ == "__main__":
    sys.exit(main())
