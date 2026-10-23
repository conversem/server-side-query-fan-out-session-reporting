"""
BigQuery view definitions for dashboard and freshness/decay reporting.

Translates the SQLite dashboard views from sqlite_schemas.py plus
freshness/decay computations into BigQuery CREATE OR REPLACE VIEW statements.

All views are created in the report dataset (bot_logs) alongside the
source tables they reference.  Views are computed on read and require
no materialization or refresh.

Template variable:
    {ds} — fully qualified dataset reference, e.g. ``project_id.bot_logs``
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ============================================================================
# Dashboard views (ported from SQLite VIEW definitions in sqlite_schemas.py)
# ============================================================================

_VIEW_DAILY_KPIS = """
CREATE OR REPLACE VIEW `{ds}.v_daily_kpis` AS
WITH url_counts AS (
    SELECT session_date, domain, COUNT(DISTINCT url) AS unique_urls_requested
    FROM `{ds}.session_url_details`
    GROUP BY session_date, domain
)
SELECT
    qfs.session_date,
    qfs.domain,
    COUNT(*) AS total_sessions,
    MAX(COALESCE(uc.unique_urls_requested, 0)) AS unique_urls_requested,
    AVG(qfs.unique_urls) AS avg_urls_per_session,
    SUM(CASE WHEN qfs.unique_urls = 1 THEN 1 ELSE 0 END) AS singleton_count,
    ROUND(
        100.0 * SUM(CASE WHEN qfs.unique_urls = 1 THEN 1 ELSE 0 END)
        / COUNT(*), 2
    ) AS singleton_rate,
    SUM(CASE WHEN qfs.unique_urls > 1 THEN 1 ELSE 0 END) AS multi_url_count,
    ROUND(
        100.0 * SUM(CASE WHEN qfs.unique_urls > 1 THEN 1 ELSE 0 END)
        / COUNT(*), 2
    ) AS multi_url_rate,
    AVG(
        CASE WHEN qfs.unique_urls > 1 THEN qfs.mean_cosine_similarity END
    ) AS mean_mibcs_multi_url,
    SUM(CASE WHEN qfs.confidence_level = 'high' THEN 1 ELSE 0 END)
        AS high_confidence_count,
    ROUND(
        100.0 * SUM(CASE WHEN qfs.confidence_level = 'high' THEN 1 ELSE 0 END)
        / COUNT(*), 2
    ) AS high_confidence_rate,
    SUM(CASE WHEN qfs.confidence_level = 'medium' THEN 1 ELSE 0 END)
        AS medium_confidence_count,
    SUM(CASE WHEN qfs.confidence_level = 'low' THEN 1 ELSE 0 END)
        AS low_confidence_count
FROM `{ds}.query_fanout_sessions` qfs
LEFT JOIN url_counts uc ON qfs.session_date = uc.session_date AND qfs.domain = uc.domain
GROUP BY qfs.session_date, qfs.domain
"""

_VIEW_SESSION_URL_DISTRIBUTION = """
CREATE OR REPLACE VIEW `{ds}.v_session_url_distribution` AS
SELECT
    session_date,
    domain,
    CASE
        WHEN unique_urls = 1 THEN '1 (Singleton)'
        WHEN unique_urls = 2 THEN '2'
        WHEN unique_urls = 3 THEN '3'
        ELSE '4+'
    END AS url_bucket,
    CASE
        WHEN unique_urls = 1 THEN 1
        WHEN unique_urls = 2 THEN 2
        WHEN unique_urls = 3 THEN 3
        ELSE 4
    END AS sort_order,
    COUNT(*) AS session_count
FROM `{ds}.query_fanout_sessions`
GROUP BY session_date, domain, url_bucket, sort_order
"""

_VIEW_SESSION_SINGLETON_BINARY = """
CREATE OR REPLACE VIEW `{ds}.v_session_singleton_binary` AS
SELECT
    session_date,
    domain,
    CASE
        WHEN unique_urls = 1 THEN 'Singleton (1 URL)'
        ELSE 'Plural (2+ URLs)'
    END AS session_type,
    CASE WHEN unique_urls = 1 THEN 1 ELSE 2 END AS sort_order,
    COUNT(*) AS session_count
FROM `{ds}.query_fanout_sessions`
GROUP BY session_date, domain, session_type, sort_order
"""

_VIEW_BOT_VOLUME = """
CREATE OR REPLACE VIEW `{ds}.v_bot_volume` AS
SELECT
    session_date,
    domain,
    bot_name,
    bot_provider,
    COUNT(*) AS session_count,
    AVG(unique_urls) AS avg_urls_per_session,
    SUM(CASE WHEN unique_urls = 1 THEN 1 ELSE 0 END) AS singleton_count,
    ROUND(
        100.0 * SUM(CASE WHEN unique_urls = 1 THEN 1 ELSE 0 END)
        / COUNT(*), 2
    ) AS singleton_rate
FROM `{ds}.query_fanout_sessions`
GROUP BY session_date, domain, bot_name, bot_provider
"""

_VIEW_TOP_SESSION_TOPICS = """
CREATE OR REPLACE VIEW `{ds}.v_top_session_topics` AS
SELECT
    session_date,
    domain,
    fanout_session_name AS topic,
    COUNT(*) AS session_count,
    AVG(unique_urls) AS avg_urls_per_session,
    SUM(CASE WHEN unique_urls = 1 THEN 1 ELSE 0 END) AS singleton_count,
    SUM(CASE WHEN unique_urls > 1 THEN 1 ELSE 0 END) AS multi_url_count,
    AVG(
        CASE WHEN unique_urls > 1 THEN mean_cosine_similarity END
    ) AS avg_mibcs_multi_url,
    SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END)
        AS high_confidence_count,
    SUM(CASE WHEN confidence_level = 'medium' THEN 1 ELSE 0 END)
        AS medium_confidence_count,
    SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END)
        AS low_confidence_count
FROM `{ds}.query_fanout_sessions`
WHERE fanout_session_name IS NOT NULL
GROUP BY session_date, domain, fanout_session_name
"""

_VIEW_CATEGORY_COMPARISON = """
CREATE OR REPLACE VIEW `{ds}.v_category_comparison` AS
SELECT
    session_date AS date,
    domain,
    'User Questions' AS category,
    COUNT(*) AS count,
    1 AS sort_order
FROM `{ds}.query_fanout_sessions`
GROUP BY session_date, domain

UNION ALL

SELECT
    request_date AS date,
    domain,
    'Training (Unique URLs)' AS category,
    CAST(SUM(unique_urls) AS INT64) AS count,
    2 AS sort_order
FROM `{ds}.daily_summary`
WHERE bot_category = 'training'
GROUP BY request_date, domain

UNION ALL

SELECT
    request_date AS date,
    domain,
    'Search Engine (Requests)' AS category,
    CAST(SUM(total_requests) AS INT64) AS count,
    3 AS sort_order
FROM `{ds}.daily_summary`
WHERE bot_category = 'search'
GROUP BY request_date, domain
"""

_VIEW_URL_COOCCURRENCE = """
CREATE OR REPLACE VIEW `{ds}.v_url_cooccurrence` AS
SELECT
    session_id,
    session_date,
    domain,
    url,
    CONCAT('https://', domain, url) AS full_url,
    bot_name,
    fanout_session_name AS topic,
    session_unique_urls,
    mean_cosine_similarity,
    confidence_level
FROM `{ds}.session_url_details`
WHERE session_unique_urls > 1
"""

# ============================================================================
# Freshness / decay views (ported from Python in export_session_report.py)
# ============================================================================

_VIEW_URL_FRESHNESS = """
CREATE OR REPLACE VIEW `{ds}.v_url_freshness` AS
WITH dedup_sitemap AS (
    SELECT DISTINCT
        domain,
        url_path,
        lastmod_month
    FROM `{ds}.sitemap_urls`
    WHERE lastmod_month IS NOT NULL
),
request_counts AS (
    SELECT
        sud.session_date,
        sud.domain,
        sm.lastmod_month,
        COUNT(*) AS request_count,
        COUNT(DISTINCT sud.url) AS unique_urls_requested
    FROM `{ds}.session_url_details` sud
    JOIN dedup_sitemap sm
        ON sud.url = sm.url_path AND sud.domain = sm.domain
    GROUP BY sud.session_date, sud.domain, sm.lastmod_month
),
sitemap_totals AS (
    SELECT
        domain,
        lastmod_month,
        COUNT(*) AS total_sitemap_urls
    FROM dedup_sitemap
    GROUP BY domain, lastmod_month
)
SELECT
    rc.session_date,
    rc.domain,
    rc.lastmod_month,
    rc.request_count,
    st.total_sitemap_urls AS urls_in_sitemap,
    rc.unique_urls_requested,
    ROUND(
        100.0 * rc.unique_urls_requested / st.total_sitemap_urls, 1
    ) AS pct_requested
FROM request_counts rc
JOIN sitemap_totals st ON rc.lastmod_month = st.lastmod_month AND rc.domain = st.domain
"""

_VIEW_URL_FRESHNESS_SUMMARY = """
CREATE OR REPLACE VIEW `{ds}.v_url_freshness_summary` AS
-- Summary of sitemap coverage per (domain, lastmod_month).
-- All request metrics use a rolling ~30-day window (session_date >= today - 30).
-- urls_in_sitemap is always from the full sitemap — no time filter.
-- pct_of_total_requests denominator = sitemap-matched requests for the domain only.
-- pct_of_total_requests is NULL when a domain has zero sitemap-matched traffic in window.
WITH dedup_sitemap AS (
    -- One row per (domain, url_path, lastmod_month).
    -- sitemap_urls is already deduped on (domain, url_path) so no fan-out risk.
    SELECT DISTINCT
        domain,
        url_path,
        lastmod_month
    FROM `{ds}.sitemap_urls`
    WHERE lastmod_month IS NOT NULL
),
sitemap_totals AS (
    SELECT
        domain,
        lastmod_month,
        COUNT(*) AS urls_in_sitemap
    FROM dedup_sitemap
    GROUP BY domain, lastmod_month
),
requests_30d AS (
    -- LEFT JOIN (sitemap → sessions) gives 0 for months with no traffic.
    -- COUNT(sud.url) not COUNT(*): NULL cols from LEFT JOIN score 0, not 1.
    SELECT
        sm.domain,
        sm.lastmod_month,
        COUNT(DISTINCT sud.url) AS unique_urls_requested,
        COUNT(sud.url)          AS url_requests
    FROM dedup_sitemap sm
    LEFT JOIN `{ds}.session_url_details` sud
        ON  sud.url         = sm.url_path
        AND sud.domain      = sm.domain
        AND sud.session_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY sm.domain, sm.lastmod_month
),
domain_totals_30d AS (
    SELECT domain, SUM(url_requests) AS total_requests
    FROM requests_30d
    GROUP BY domain
)
SELECT
    st.domain,
    st.lastmod_month,
    st.urls_in_sitemap,
    r.unique_urls_requested,
    ROUND(
        100.0 * r.unique_urls_requested / st.urls_in_sitemap, 1
    ) AS pct_requested,
    r.url_requests,
    ROUND(
        SAFE_DIVIDE(100.0 * r.url_requests, dt.total_requests), 1
    ) AS pct_of_total_requests
FROM sitemap_totals st
JOIN requests_30d r USING (domain, lastmod_month)
JOIN domain_totals_30d dt USING (domain)
"""

_VIEW_DECAY_UNIQUE_URLS = """
CREATE OR REPLACE VIEW `{ds}.v_decay_unique_urls` AS
WITH url_ages AS (
    SELECT DISTINCT
        sud.session_date,
        sud.url,
        DATE_DIFF(
            sud.session_date,
            DATE(CONCAT(sm.lastmod_month, '-01')),
            MONTH
        ) AS months_ago
    FROM `{ds}.session_url_details` sud
    JOIN `{ds}.sitemap_urls` sm
        ON sud.url = sm.url_path AND sud.domain = sm.domain
    WHERE sm.lastmod_month IS NOT NULL
),
daily_totals AS (
    SELECT session_date, COUNT(*) AS total_urls
    FROM url_ages
    GROUP BY session_date
)
SELECT
    ua.session_date,
    b AS months_bucket,
    ROUND(
        100.0 * COUNTIF(ua.months_ago <= b) / ANY_VALUE(dt.total_urls),
        1
    ) AS cumulative_pct
FROM url_ages ua
CROSS JOIN UNNEST(GENERATE_ARRAY(1, 36)) AS b
JOIN daily_totals dt ON ua.session_date = dt.session_date
GROUP BY ua.session_date, b
"""

_VIEW_DECAY_REQUEST_VOLUME = """
CREATE OR REPLACE VIEW `{ds}.v_decay_request_volume` AS
WITH request_ages AS (
    SELECT
        sud.session_date,
        DATE_DIFF(
            sud.session_date,
            DATE(CONCAT(sm.lastmod_month, '-01')),
            MONTH
        ) AS months_ago
    FROM `{ds}.session_url_details` sud
    JOIN `{ds}.sitemap_urls` sm
        ON sud.url = sm.url_path AND sud.domain = sm.domain
    WHERE sm.lastmod_month IS NOT NULL
),
daily_totals AS (
    SELECT session_date, COUNT(*) AS total_requests
    FROM request_ages
    GROUP BY session_date
)
SELECT
    ra.session_date,
    b AS months_bucket,
    ROUND(
        100.0 * COUNTIF(ra.months_ago <= b) / ANY_VALUE(dt.total_requests),
        1
    ) AS cumulative_pct
FROM request_ages ra
CROSS JOIN UNNEST(GENERATE_ARRAY(1, 36)) AS b
JOIN daily_totals dt ON ra.session_date = dt.session_date
GROUP BY ra.session_date, b
"""

# ============================================================================
# Domain-scoped decay and freshness detail views
# ============================================================================

_VIEW_DECAY_UNIQUE_URLS_BY_DOMAIN = """
CREATE OR REPLACE VIEW `{ds}.v_decay_unique_urls_by_domain` AS
WITH url_ages AS (
    SELECT DISTINCT
        sud.session_date,
        sud.domain,
        sud.url,
        DATE_DIFF(
            sud.session_date,
            DATE(CONCAT(sm.lastmod_month, '-01')),
            MONTH
        ) AS months_ago
    FROM `{ds}.session_url_details` sud
    JOIN `{ds}.sitemap_urls` sm
        ON sud.url = sm.url_path AND sud.domain = sm.domain
    WHERE sm.lastmod_month IS NOT NULL
),
daily_totals AS (
    SELECT session_date, domain, COUNT(*) AS total_urls
    FROM url_ages
    GROUP BY session_date, domain
)
SELECT
    ua.session_date,
    ua.domain,
    b AS months_bucket,
    ROUND(
        100.0 * COUNTIF(ua.months_ago <= b) / ANY_VALUE(dt.total_urls),
        1
    ) AS cumulative_pct
FROM url_ages ua
CROSS JOIN UNNEST(GENERATE_ARRAY(1, 36)) AS b
JOIN daily_totals dt ON ua.session_date = dt.session_date AND ua.domain = dt.domain
GROUP BY ua.session_date, ua.domain, b
"""

_VIEW_DECAY_REQUEST_VOLUME_BY_DOMAIN = """
CREATE OR REPLACE VIEW `{ds}.v_decay_request_volume_by_domain` AS
WITH request_ages AS (
    SELECT
        sud.session_date,
        sud.domain,
        DATE_DIFF(
            sud.session_date,
            DATE(CONCAT(sm.lastmod_month, '-01')),
            MONTH
        ) AS months_ago
    FROM `{ds}.session_url_details` sud
    JOIN `{ds}.sitemap_urls` sm
        ON sud.url = sm.url_path AND sud.domain = sm.domain
    WHERE sm.lastmod_month IS NOT NULL
),
daily_totals AS (
    SELECT session_date, domain, COUNT(*) AS total_requests
    FROM request_ages
    GROUP BY session_date, domain
)
SELECT
    ra.session_date,
    ra.domain,
    b AS months_bucket,
    ROUND(
        100.0 * COUNTIF(ra.months_ago <= b) / ANY_VALUE(dt.total_requests),
        1
    ) AS cumulative_pct
FROM request_ages ra
CROSS JOIN UNNEST(GENERATE_ARRAY(1, 36)) AS b
JOIN daily_totals dt ON ra.session_date = dt.session_date AND ra.domain = dt.domain
GROUP BY ra.session_date, ra.domain, b
"""

_VIEW_URL_FRESHNESS_DETAIL = """
CREATE OR REPLACE VIEW `{ds}.v_url_freshness_detail` AS
WITH dedup_sitemap AS (
    SELECT DISTINCT
        domain,
        url_path,
        lastmod_month,
        lastmod,
        sitemap_source
    FROM `{ds}.sitemap_urls`
    WHERE lastmod_month IS NOT NULL
)
SELECT
    sud.session_date,
    sud.domain,
    sud.url AS url_path,
    CONCAT('https://', sud.domain, sud.url) AS full_url,
    sm.lastmod_month,
    sm.lastmod,
    sm.sitemap_source,
    DATE_DIFF(
        DATE(sud.session_date),
        DATE(CONCAT(sm.lastmod_month, '-01')),
        MONTH
    ) AS months_since_lastmod,
    COUNT(*) AS request_count
FROM `{ds}.session_url_details` sud
JOIN dedup_sitemap sm
    ON sud.url = sm.url_path AND sud.domain = sm.domain
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""

_VIEW_SESSIONS_BY_CONTENT_AGE = """
CREATE OR REPLACE VIEW `{ds}.v_sessions_by_content_age` AS
WITH dedup_sitemap AS (
    SELECT DISTINCT
        domain,
        url_path,
        lastmod_month,
        lastmod,
        sitemap_source
    FROM `{ds}.sitemap_urls`
    WHERE lastmod_month IS NOT NULL
)
SELECT
    qfs.session_date,
    qfs.domain,
    sud.url AS url_path,
    CONCAT('https://', qfs.domain, sud.url) AS full_url,
    sm.lastmod_month,
    sm.lastmod,
    sm.sitemap_source,
    DATE_DIFF(
        DATE(qfs.session_date),
        DATE(CONCAT(sm.lastmod_month, '-01')),
        MONTH
    ) AS months_since_lastmod,
    COUNT(DISTINCT qfs.session_id) AS session_count,
    SUM(qfs.unique_urls) AS total_urls_cited,
    AVG(CASE WHEN qfs.unique_urls > 1 THEN qfs.mean_cosine_similarity END) AS avg_mibcs
FROM `{ds}.session_url_details` sud
JOIN `{ds}.query_fanout_sessions` qfs
    ON sud.session_id = qfs.session_id AND sud.domain = qfs.domain
JOIN dedup_sitemap sm
    ON sud.url = sm.url_path AND sud.domain = sm.domain
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""

_VIEW_URL_PERFORMANCE_WITH_FRESHNESS = """
CREATE OR REPLACE VIEW `{ds}.v_url_performance_with_freshness` AS
WITH dedup_sitemap AS (
    SELECT DISTINCT
        domain,
        url_path,
        lastmod_month,
        lastmod,
        sitemap_source
    FROM `{ds}.sitemap_urls`
)
SELECT
    up.request_date,
    up.domain,
    up.url_path,
    CONCAT('https://', up.request_host, up.url_path) AS full_url,
    up.training_hits,
    up.user_request_hits,
    up.total_bot_requests,
    up.unique_bot_providers,
    up.unique_bot_names,
    up.first_seen,
    up.last_seen,
    sm.lastmod_month,
    sm.lastmod,
    sm.sitemap_source,
    CASE
        WHEN sm.lastmod_month IS NOT NULL
        THEN DATE_DIFF(
            DATE(up.request_date),
            DATE(CONCAT(sm.lastmod_month, '-01')),
            MONTH
        )
        ELSE NULL
    END AS months_since_lastmod
FROM `{ds}.url_performance` up
LEFT JOIN dedup_sitemap sm
    ON up.url_path = sm.url_path AND up.domain = sm.domain
"""

# ============================================================================
# Registry
# ============================================================================

VIEW_DEFINITIONS: list[tuple[str, str]] = [
    ("v_daily_kpis", _VIEW_DAILY_KPIS),
    ("v_session_url_distribution", _VIEW_SESSION_URL_DISTRIBUTION),
    ("v_session_singleton_binary", _VIEW_SESSION_SINGLETON_BINARY),
    ("v_bot_volume", _VIEW_BOT_VOLUME),
    ("v_top_session_topics", _VIEW_TOP_SESSION_TOPICS),
    ("v_category_comparison", _VIEW_CATEGORY_COMPARISON),
    ("v_url_cooccurrence", _VIEW_URL_COOCCURRENCE),
    ("v_url_freshness", _VIEW_URL_FRESHNESS),
    ("v_url_freshness_summary", _VIEW_URL_FRESHNESS_SUMMARY),
    ("v_decay_unique_urls", _VIEW_DECAY_UNIQUE_URLS),
    ("v_decay_request_volume", _VIEW_DECAY_REQUEST_VOLUME),
    ("v_decay_unique_urls_by_domain", _VIEW_DECAY_UNIQUE_URLS_BY_DOMAIN),
    ("v_decay_request_volume_by_domain", _VIEW_DECAY_REQUEST_VOLUME_BY_DOMAIN),
    ("v_url_freshness_detail", _VIEW_URL_FRESHNESS_DETAIL),
    ("v_sessions_by_content_age", _VIEW_SESSIONS_BY_CONTENT_AGE),
    ("v_url_performance_with_freshness", _VIEW_URL_PERFORMANCE_WITH_FRESHNESS),
]

VIEW_NAMES: list[str] = [name for name, _ in VIEW_DEFINITIONS]


def get_view_sql_statements(
    dataset_ref: str,
) -> list[tuple[str, str]]:
    """Return ``(view_name, CREATE OR REPLACE VIEW …)`` pairs.

    Args:
        dataset_ref: Fully-qualified dataset, e.g. ``project_id.bot_logs``.
    """
    return [(name, sql.format(ds=dataset_ref)) for name, sql in VIEW_DEFINITIONS]


def create_views(
    client,
    dataset_ref: str,
    view_names: Optional[list[str]] = None,
) -> list[str]:
    """Create (or replace) BigQuery views.

    Args:
        client: ``google.cloud.bigquery.Client`` instance.
        dataset_ref: Fully-qualified dataset, e.g. ``project_id.bot_logs``.
        view_names: Subset of views to create.  ``None`` creates all.

    Returns:
        List of view names successfully created.
    """
    created: list[str] = []
    for name, sql in get_view_sql_statements(dataset_ref):
        if view_names and name not in view_names:
            continue
        try:
            client.query(sql).result()
            logger.info("Created/replaced view: %s", name)
            created.append(name)
        except Exception:
            logger.exception("Failed to create view %s", name)
    return created
