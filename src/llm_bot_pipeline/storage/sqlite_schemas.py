"""
SQLite schema definitions for the LLM bot traffic pipeline.

Contains all DDL constants: CREATE TABLE, CREATE VIEW, CREATE INDEX statements.
Separated from sqlite_backend.py to keep CRUD operations distinct from schema definitions.
"""

# =============================================================================
# Table Schemas
# =============================================================================

RAW_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_bot_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    EdgeStartTimestamp TEXT NOT NULL,
    ClientRequestURI TEXT,
    ClientRequestHost TEXT,
    domain TEXT,
    ClientRequestUserAgent TEXT,
    ClientIP TEXT,
    ClientCountry TEXT,
    EdgeResponseStatus INTEGER,
    RayID TEXT,
    _ingestion_time TEXT NOT NULL,
    source_provider TEXT  -- Tracks data provenance (universal, cloudflare, aws_cloudfront)
)
"""

CLEAN_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_requests_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_timestamp TEXT NOT NULL,
    request_date TEXT NOT NULL,
    request_hour INTEGER NOT NULL,
    day_of_week TEXT NOT NULL,
    request_uri TEXT NOT NULL,
    request_host TEXT NOT NULL,
    domain TEXT,
    url_path TEXT,
    url_path_depth INTEGER,
    user_agent_raw TEXT,
    bot_name TEXT NOT NULL,
    bot_provider TEXT NOT NULL,
    bot_category TEXT NOT NULL,
    crawler_country TEXT,
    response_status INTEGER NOT NULL,
    response_status_category TEXT NOT NULL,
    resource_type TEXT NOT NULL DEFAULT 'document',
    _processed_at TEXT NOT NULL
)
"""

DAILY_SUMMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_date TEXT NOT NULL,
    domain TEXT,
    bot_provider TEXT NOT NULL,
    bot_name TEXT NOT NULL,
    bot_category TEXT NOT NULL,
    total_requests INTEGER NOT NULL,
    unique_urls INTEGER NOT NULL,
    unique_hosts INTEGER NOT NULL,
    successful_requests INTEGER NOT NULL,
    error_requests INTEGER NOT NULL,
    redirect_requests INTEGER NOT NULL,
    _aggregated_at TEXT NOT NULL
)
"""

URL_PERFORMANCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS url_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_date TEXT NOT NULL,
    domain TEXT,
    request_host TEXT NOT NULL,
    url_path TEXT NOT NULL,
    total_bot_requests INTEGER NOT NULL,
    unique_bot_providers INTEGER NOT NULL,
    unique_bot_names INTEGER NOT NULL,
    training_hits INTEGER NOT NULL,
    user_request_hits INTEGER NOT NULL,
    successful_requests INTEGER NOT NULL,
    error_requests INTEGER NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    _aggregated_at TEXT NOT NULL
)
"""

DATA_FRESHNESS_SCHEMA = """
CREATE TABLE IF NOT EXISTS data_freshness (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL UNIQUE,
    last_processed_date TEXT NOT NULL,
    last_updated_at TEXT NOT NULL,
    rows_processed INTEGER NOT NULL
)
"""

QUERY_FANOUT_SESSIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS query_fanout_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL UNIQUE,
    session_date TEXT NOT NULL,
    domain TEXT,
    session_start_time TEXT NOT NULL,
    session_end_time TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    bot_provider TEXT NOT NULL,
    bot_name TEXT,
    request_count INTEGER NOT NULL,
    unique_urls INTEGER NOT NULL,
    mean_cosine_similarity REAL,
    min_cosine_similarity REAL,
    max_cosine_similarity REAL,
    confidence_level TEXT NOT NULL,
    fanout_session_name TEXT,
    url_list TEXT NOT NULL,
    window_ms REAL NOT NULL,
    splitting_strategy TEXT,
    _created_at TEXT NOT NULL DEFAULT (datetime('now')),
    parent_session_id TEXT,
    was_refined INTEGER NOT NULL DEFAULT 0,
    refinement_reason TEXT,
    pre_refinement_mibcs REAL,
    CONSTRAINT valid_confidence CHECK (confidence_level IN ('high', 'medium', 'low'))
)
"""

# Natural key unique index: prevents duplicate sessions from repeated aggregation runs.
# session_id uses random UUIDs so cannot prevent content duplicates on its own.
# bot_name is included because different user-agents (e.g. ChatGPT-User vs
# PerplexityBot) represent legitimately different bot requests.
QUERY_FANOUT_SESSIONS_NATURAL_KEY_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_session_natural_key
ON query_fanout_sessions (session_date, domain, session_start_time, bot_provider, bot_name, url_list)
"""

SESSION_REFINEMENT_LOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS session_refinement_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TEXT NOT NULL DEFAULT (datetime('now')),
    window_ms REAL NOT NULL,
    total_bundles INTEGER NOT NULL,
    collision_candidates INTEGER NOT NULL,
    bundles_split INTEGER NOT NULL,
    sub_bundles_created INTEGER NOT NULL,
    mean_mibcs_improvement REAL,
    refinement_duration_ms REAL,
    collision_ip_threshold INTEGER,
    collision_homogeneity_threshold REAL,
    similarity_threshold REAL,
    min_sub_bundle_size INTEGER,
    min_mibcs_improvement REAL
)
"""

SITEMAP_URLS_SCHEMA = """
CREATE TABLE IF NOT EXISTS sitemap_urls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    url_path TEXT NOT NULL,
    domain TEXT,
    lastmod TEXT,
    lastmod_month TEXT,
    sitemap_source TEXT NOT NULL,
    _fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(domain, url_path)
)
"""

SITEMAP_FRESHNESS_SCHEMA = """
CREATE TABLE IF NOT EXISTS sitemap_freshness (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url_path TEXT NOT NULL,
    domain TEXT,
    lastmod TEXT,
    lastmod_month TEXT,
    sitemap_source TEXT NOT NULL,
    first_seen_date TEXT,
    last_seen_date TEXT,
    request_count INTEGER NOT NULL DEFAULT 0,
    unique_urls INTEGER NOT NULL DEFAULT 0,
    unique_bots INTEGER NOT NULL DEFAULT 0,
    days_since_lastmod INTEGER,
    _aggregated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(domain, url_path)
)
"""

URL_VOLUME_DECAY_SCHEMA = """
CREATE TABLE IF NOT EXISTS url_volume_decay (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url_path TEXT NOT NULL,
    domain TEXT,
    period TEXT NOT NULL,
    period_start TEXT NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 0,
    unique_urls INTEGER NOT NULL DEFAULT 0,
    unique_bots INTEGER NOT NULL DEFAULT 0,
    prev_request_count INTEGER,
    decay_rate REAL,
    _aggregated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(domain, url_path, period, period_start)
)
"""

SESSION_URL_DETAILS_SCHEMA = """
CREATE TABLE IF NOT EXISTS session_url_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    session_date TEXT NOT NULL,
    domain TEXT,
    url TEXT NOT NULL,
    url_position INTEGER NOT NULL,
    bot_provider TEXT NOT NULL,
    bot_name TEXT,
    fanout_session_name TEXT,
    session_unique_urls INTEGER NOT NULL,
    session_request_count INTEGER NOT NULL,
    session_duration_ms INTEGER NOT NULL,
    mean_cosine_similarity REAL,
    min_cosine_similarity REAL,
    max_cosine_similarity REAL,
    confidence_level TEXT NOT NULL,
    session_start_time TEXT,
    session_end_time TEXT,
    window_ms REAL,
    splitting_strategy TEXT,
    _created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (session_id) REFERENCES query_fanout_sessions(session_id)
)
"""

# =============================================================================
# Reporting View Definitions
# =============================================================================

VIEW_SESSION_URL_DISTRIBUTION = """
CREATE VIEW IF NOT EXISTS v_session_url_distribution AS
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
FROM query_fanout_sessions
GROUP BY session_date, domain, url_bucket, sort_order
"""

VIEW_SESSION_SINGLETON_BINARY = """
CREATE VIEW IF NOT EXISTS v_session_singleton_binary AS
SELECT
    session_date,
    domain,
    CASE WHEN unique_urls = 1 THEN 'Singleton (1 URL)' ELSE 'Plural (2+ URLs)' END AS session_type,
    CASE WHEN unique_urls = 1 THEN 1 ELSE 2 END AS sort_order,
    COUNT(*) AS session_count
FROM query_fanout_sessions
GROUP BY session_date, domain, session_type, sort_order
"""

VIEW_BOT_VOLUME = """
CREATE VIEW IF NOT EXISTS v_bot_volume AS
SELECT
    session_date,
    domain,
    bot_name,
    bot_provider,
    COUNT(*) AS session_count,
    AVG(unique_urls) AS avg_urls_per_session,
    SUM(CASE WHEN unique_urls = 1 THEN 1 ELSE 0 END) AS singleton_count,
    ROUND(100.0 * SUM(CASE WHEN unique_urls = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS singleton_rate
FROM query_fanout_sessions
GROUP BY session_date, domain, bot_name, bot_provider
"""

VIEW_TOP_SESSION_TOPICS = """
CREATE VIEW IF NOT EXISTS v_top_session_topics AS
SELECT
    session_date,
    domain,
    fanout_session_name AS topic,
    COUNT(*) AS session_count,
    AVG(unique_urls) AS avg_urls_per_session,
    SUM(CASE WHEN unique_urls = 1 THEN 1 ELSE 0 END) AS singleton_count,
    SUM(CASE WHEN unique_urls > 1 THEN 1 ELSE 0 END) AS multi_url_count,
    AVG(CASE WHEN unique_urls > 1 THEN mean_cosine_similarity END) AS avg_mibcs_multi_url,
    SUM(CASE WHEN confidence_level = 'high' THEN 1 ELSE 0 END) AS high_confidence_count,
    SUM(CASE WHEN confidence_level = 'medium' THEN 1 ELSE 0 END) AS medium_confidence_count,
    SUM(CASE WHEN confidence_level = 'low' THEN 1 ELSE 0 END) AS low_confidence_count
FROM query_fanout_sessions
WHERE fanout_session_name IS NOT NULL
GROUP BY session_date, domain, fanout_session_name
"""

VIEW_DAILY_KPIS = """
CREATE VIEW IF NOT EXISTS v_daily_kpis AS
WITH url_counts AS (
    SELECT session_date, domain, COUNT(DISTINCT url) AS unique_urls_requested
    FROM session_url_details
    GROUP BY session_date, domain
)
SELECT
    qfs.session_date,
    qfs.domain,
    COUNT(*) AS total_sessions,
    COALESCE(uc.unique_urls_requested, 0) AS unique_urls_requested,
    AVG(qfs.unique_urls) AS avg_urls_per_session,
    SUM(CASE WHEN qfs.unique_urls = 1 THEN 1 ELSE 0 END) AS singleton_count,
    ROUND(100.0 * SUM(CASE WHEN qfs.unique_urls = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS singleton_rate,
    SUM(CASE WHEN qfs.unique_urls > 1 THEN 1 ELSE 0 END) AS multi_url_count,
    ROUND(100.0 * SUM(CASE WHEN qfs.unique_urls > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS multi_url_rate,
    AVG(CASE WHEN qfs.unique_urls > 1 THEN qfs.mean_cosine_similarity END) AS mean_mibcs_multi_url,
    SUM(CASE WHEN qfs.confidence_level = 'high' THEN 1 ELSE 0 END) AS high_confidence_count,
    ROUND(100.0 * SUM(CASE WHEN qfs.confidence_level = 'high' THEN 1 ELSE 0 END) / COUNT(*), 2) AS high_confidence_rate,
    SUM(CASE WHEN qfs.confidence_level = 'medium' THEN 1 ELSE 0 END) AS medium_confidence_count,
    SUM(CASE WHEN qfs.confidence_level = 'low' THEN 1 ELSE 0 END) AS low_confidence_count
FROM query_fanout_sessions qfs
LEFT JOIN url_counts uc ON qfs.session_date = uc.session_date AND qfs.domain = uc.domain
GROUP BY qfs.session_date, qfs.domain
"""

VIEW_CATEGORY_COMPARISON = """
CREATE VIEW IF NOT EXISTS v_category_comparison AS
SELECT
    session_date AS date,
    domain,
    'User Questions' AS category,
    COUNT(*) AS count,
    1 AS sort_order
FROM query_fanout_sessions
GROUP BY session_date, domain

UNION ALL

SELECT
    request_date AS date,
    domain,
    'Training (Unique URLs)' AS category,
    SUM(unique_urls) AS count,
    2 AS sort_order
FROM daily_summary
WHERE bot_category = 'training'
GROUP BY request_date, domain

UNION ALL

SELECT
    request_date AS date,
    domain,
    'Search (Requests)' AS category,
    SUM(total_requests) AS count,
    3 AS sort_order
FROM daily_summary
WHERE bot_category = 'search'
GROUP BY request_date, domain
"""

VIEW_URL_COOCCURRENCE = """
CREATE VIEW IF NOT EXISTS v_url_cooccurrence AS
SELECT
    session_id,
    session_date,
    domain,
    url,
    'https://' || domain || url AS full_url,
    bot_name,
    fanout_session_name AS topic,
    session_unique_urls,
    mean_cosine_similarity,
    confidence_level
FROM session_url_details
WHERE session_unique_urls > 1
"""

# -- Freshness / decay views (match BigQuery views in bigquery_views.py) ------

VIEW_URL_FRESHNESS = """
CREATE VIEW IF NOT EXISTS v_url_freshness AS
WITH dedup_sitemap AS (
    SELECT DISTINCT
        domain,
        url_path,
        lastmod_month
    FROM sitemap_urls
    WHERE lastmod_month IS NOT NULL
),
request_counts AS (
    SELECT
        sud.session_date,
        sud.domain,
        sm.lastmod_month,
        COUNT(*) AS request_count,
        COUNT(DISTINCT sud.url) AS unique_urls_requested
    FROM session_url_details sud
    JOIN dedup_sitemap sm ON sud.url = sm.url_path AND sud.domain = sm.domain
    GROUP BY sud.session_date, sud.domain, sm.lastmod_month
),
sitemap_totals AS (
    SELECT domain, lastmod_month, COUNT(*) AS total_sitemap_urls
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
    ROUND(100.0 * rc.unique_urls_requested / st.total_sitemap_urls, 1) AS pct_requested
FROM request_counts rc
JOIN sitemap_totals st ON rc.lastmod_month = st.lastmod_month AND rc.domain = st.domain
"""


VIEW_DECAY_REQUEST_VOLUME = """
CREATE VIEW IF NOT EXISTS v_decay_request_volume AS
WITH RECURSIVE months_gen(n) AS (
    SELECT 1 UNION ALL SELECT n + 1 FROM months_gen WHERE n < 36
),
request_ages AS (
    SELECT
        sud.session_date,
        (CAST(strftime('%Y', sud.session_date) AS INTEGER)
         - CAST(strftime('%Y', sm.lastmod_month || '-01') AS INTEGER)) * 12
        + (CAST(strftime('%m', sud.session_date) AS INTEGER)
         - CAST(strftime('%m', sm.lastmod_month || '-01') AS INTEGER))
        AS months_ago
    FROM session_url_details sud
    JOIN sitemap_urls sm ON sud.url = sm.url_path AND sud.domain = sm.domain
    WHERE sm.lastmod_month IS NOT NULL
),
daily_totals AS (
    SELECT session_date, COUNT(*) AS total_requests
    FROM request_ages
    GROUP BY session_date
)
SELECT
    ra.session_date,
    mg.n AS months_bucket,
    ROUND(
        100.0 * SUM(CASE WHEN ra.months_ago <= mg.n THEN 1 ELSE 0 END)
        / dt.total_requests, 1
    ) AS cumulative_pct
FROM request_ages ra
CROSS JOIN months_gen mg
JOIN daily_totals dt ON ra.session_date = dt.session_date
GROUP BY ra.session_date, mg.n
"""



VIEW_URL_FRESHNESS_DETAIL = """
CREATE VIEW IF NOT EXISTS v_url_freshness_detail AS
WITH dedup_sitemap AS (
    SELECT DISTINCT
        domain,
        url_path,
        lastmod_month,
        lastmod,
        sitemap_source
    FROM sitemap_urls
    WHERE lastmod_month IS NOT NULL
)
SELECT
    sud.session_date,
    sud.domain,
    sud.url AS url_path,
    'https://' || sud.domain || sud.url AS full_url,
    sm.lastmod_month,
    sm.lastmod,
    sm.sitemap_source,
    (CAST(strftime('%Y', sud.session_date) AS INTEGER)
     - CAST(strftime('%Y', sm.lastmod_month || '-01') AS INTEGER)) * 12
    + (CAST(strftime('%m', sud.session_date) AS INTEGER)
     - CAST(strftime('%m', sm.lastmod_month || '-01') AS INTEGER))
    AS months_since_lastmod,
    COUNT(*) AS request_count
FROM session_url_details sud
JOIN dedup_sitemap sm ON sud.url = sm.url_path AND sud.domain = sm.domain
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""

VIEW_SESSIONS_BY_CONTENT_AGE = """
CREATE VIEW IF NOT EXISTS v_sessions_by_content_age AS
WITH dedup_sitemap AS (
    SELECT DISTINCT
        domain,
        url_path,
        lastmod_month,
        lastmod,
        sitemap_source
    FROM sitemap_urls
    WHERE lastmod_month IS NOT NULL
)
SELECT
    qfs.session_date,
    qfs.domain,
    sud.url AS url_path,
    'https://' || qfs.domain || sud.url AS full_url,
    sm.lastmod_month,
    sm.lastmod,
    sm.sitemap_source,
    (CAST(strftime('%Y', qfs.session_date) AS INTEGER)
     - CAST(strftime('%Y', sm.lastmod_month || '-01') AS INTEGER)) * 12
    + (CAST(strftime('%m', qfs.session_date) AS INTEGER)
     - CAST(strftime('%m', sm.lastmod_month || '-01') AS INTEGER))
    AS months_since_lastmod,
    COUNT(DISTINCT qfs.session_id) AS session_count,
    SUM(qfs.unique_urls) AS total_urls_cited,
    AVG(CASE WHEN qfs.unique_urls > 1 THEN qfs.mean_cosine_similarity END) AS avg_mibcs
FROM session_url_details sud
JOIN query_fanout_sessions qfs ON sud.session_id = qfs.session_id AND sud.domain = qfs.domain
JOIN dedup_sitemap sm ON sud.url = sm.url_path AND sud.domain = sm.domain
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""

VIEW_URL_PERFORMANCE_WITH_FRESHNESS = """
CREATE VIEW IF NOT EXISTS v_url_performance_with_freshness AS
WITH dedup_sitemap AS (
    SELECT DISTINCT domain, url_path, lastmod_month, lastmod, sitemap_source
    FROM sitemap_urls
)
SELECT
    up.request_date,
    up.domain,
    up.url_path,
    'https://' || up.request_host || up.url_path AS full_url,
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
        THEN (CAST(strftime('%Y', up.request_date) AS INTEGER)
              - CAST(strftime('%Y', sm.lastmod_month || '-01') AS INTEGER)) * 12
           + (CAST(strftime('%m', up.request_date) AS INTEGER)
              - CAST(strftime('%m', sm.lastmod_month || '-01') AS INTEGER))
        ELSE NULL
    END AS months_since_lastmod
FROM url_performance up
LEFT JOIN dedup_sitemap sm ON up.url_path = sm.url_path AND up.domain = sm.domain
"""

# View names for drop-before-recreate during schema changes
VIEW_NAMES = [
    "v_session_url_distribution",
    "v_session_singleton_binary",
    "v_bot_volume",
    "v_top_session_topics",
    "v_daily_kpis",
    "v_category_comparison",
    "v_url_cooccurrence",
    "v_url_freshness",
    "v_decay_request_volume",
    "v_url_freshness_detail",
    "v_sessions_by_content_age",
    "v_url_performance_with_freshness",
]

# List of all view definitions for easy iteration (order matches VIEW_NAMES)
VIEW_DEFINITIONS = [
    VIEW_SESSION_URL_DISTRIBUTION,
    VIEW_SESSION_SINGLETON_BINARY,
    VIEW_BOT_VOLUME,
    VIEW_TOP_SESSION_TOPICS,
    VIEW_DAILY_KPIS,
    VIEW_CATEGORY_COMPARISON,
    VIEW_URL_COOCCURRENCE,
    VIEW_URL_FRESHNESS,
    VIEW_DECAY_REQUEST_VOLUME,
    VIEW_URL_FRESHNESS_DETAIL,
    VIEW_SESSIONS_BY_CONTENT_AGE,
    VIEW_URL_PERFORMANCE_WITH_FRESHNESS,
]

# =============================================================================
# Index Definitions
# =============================================================================

INDEX_DEFINITIONS = [
    # Raw table indexes
    "CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw_bot_requests(EdgeStartTimestamp)",
    "CREATE INDEX IF NOT EXISTS idx_raw_host ON raw_bot_requests(ClientRequestHost)",
    "CREATE INDEX IF NOT EXISTS idx_raw_domain ON raw_bot_requests(domain)",
    # Clean table indexes (matching SQLite clustering)
    "CREATE INDEX IF NOT EXISTS idx_clean_date ON bot_requests_daily(request_date)",
    "CREATE INDEX IF NOT EXISTS idx_clean_provider ON bot_requests_daily(bot_provider)",
    "CREATE INDEX IF NOT EXISTS idx_clean_category ON bot_requests_daily(bot_category)",
    "CREATE INDEX IF NOT EXISTS idx_clean_host ON bot_requests_daily(request_host)",
    "CREATE INDEX IF NOT EXISTS idx_clean_domain ON bot_requests_daily(domain)",
    "CREATE INDEX IF NOT EXISTS idx_clean_date_domain ON bot_requests_daily(request_date, domain)",
    # Summary table indexes
    "CREATE INDEX IF NOT EXISTS idx_summary_date ON daily_summary(request_date)",
    "CREATE INDEX IF NOT EXISTS idx_summary_provider ON daily_summary(bot_provider)",
    "CREATE INDEX IF NOT EXISTS idx_summary_date_domain ON daily_summary(request_date, domain)",
    # URL performance indexes
    "CREATE INDEX IF NOT EXISTS idx_url_date ON url_performance(request_date)",
    "CREATE INDEX IF NOT EXISTS idx_url_host ON url_performance(request_host)",
    "CREATE INDEX IF NOT EXISTS idx_url_date_domain ON url_performance(request_date, domain)",
    # Query fan-out sessions indexes
    "CREATE INDEX IF NOT EXISTS idx_sessions_date ON query_fanout_sessions(session_date)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_provider ON query_fanout_sessions(bot_provider)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_confidence ON query_fanout_sessions(confidence_level)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_request_count ON query_fanout_sessions(request_count)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_domain ON query_fanout_sessions(domain)",
    # Session URL details indexes (flattened URL-level data)
    "CREATE INDEX IF NOT EXISTS idx_session_url_details_date ON session_url_details(session_date)",
    "CREATE INDEX IF NOT EXISTS idx_session_url_details_url ON session_url_details(url)",
    "CREATE INDEX IF NOT EXISTS idx_session_url_details_session ON session_url_details(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_session_url_details_bot ON session_url_details(bot_name)",
    "CREATE INDEX IF NOT EXISTS idx_session_url_details_unique_urls ON session_url_details(session_unique_urls)",
    # Session URL details — domain index (needed for decay view JOINs)
    "CREATE INDEX IF NOT EXISTS idx_session_url_details_domain ON session_url_details(domain)",
    # URL performance — composite unique key prevents duplicate re-aggregation rows
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_url_performance_natural_key ON url_performance(domain, request_date, url_path)",
    # Sitemap tables — domain as first clustering key for multi-domain JOINs
    "CREATE INDEX IF NOT EXISTS idx_sitemap_urls_url_path ON sitemap_urls(url_path)",
    "CREATE INDEX IF NOT EXISTS idx_sitemap_urls_domain ON sitemap_urls(domain)",
    "CREATE INDEX IF NOT EXISTS idx_sitemap_freshness_url_path ON sitemap_freshness(url_path)",
    "CREATE INDEX IF NOT EXISTS idx_sitemap_freshness_domain ON sitemap_freshness(domain)",
    "CREATE INDEX IF NOT EXISTS idx_sitemap_freshness_lastmod_month ON sitemap_freshness(lastmod_month)",
    "CREATE INDEX IF NOT EXISTS idx_url_volume_decay_domain ON url_volume_decay(domain)",
    "CREATE INDEX IF NOT EXISTS idx_url_volume_decay_url_path ON url_volume_decay(url_path)",
    "CREATE INDEX IF NOT EXISTS idx_url_volume_decay_period ON url_volume_decay(period, period_start)",
]
