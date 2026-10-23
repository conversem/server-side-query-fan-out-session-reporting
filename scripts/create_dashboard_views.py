#!/usr/bin/env python3
"""
Create BigQuery views for Looker Studio LLM Activity Dashboard.

This script creates the following views in the report dataset:
1. v_session_url_distribution - Session counts by URL bucket (1, 2, 3, 4+)
2. v_session_singleton_binary - Binary singleton vs plural sessions
3. v_bot_volume - Bot name session counts
4. v_top_session_topics - Topic aggregations from fanout_session_name

Usage:
    source venv/bin/activate
    export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt
    python scripts/create_dashboard_views.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.config import load_config
from llm_bot_pipeline.config.constants import DATASET_REPORT

CONFIG_PATH = Path(__file__).parent.parent / "config.enc.yaml"


def _load_config():
    """Load config at execution time, not at import time."""
    return load_config(encrypted_path=CONFIG_PATH, fallback_to_env=True)


def get_client(config: dict):
    """Get BigQuery client."""
    from google.cloud import bigquery

    project_id = config.get("gcp", {}).get("project_id")
    credentials_path = config.get("gcp", {}).get("service_account_key_path")

    if credentials_path:
        return bigquery.Client.from_service_account_json(
            credentials_path, project=project_id
        )
    return bigquery.Client(project=project_id)


def create_view(client, view_id: str, view_sql: str, description: str):
    """Create or replace a BigQuery view."""
    from google.cloud import bigquery

    view = bigquery.Table(view_id)
    view.view_query = view_sql
    view.description = description

    try:
        client.delete_table(view_id, not_found_ok=True)
        client.create_table(view)
        print(f"  Created view: {view_id}")
        return True
    except Exception as e:
        print(f"  Failed to create {view_id}: {e}")
        return False


def main():
    config = _load_config()
    project_id = config.get("gcp", {}).get("project_id")
    dataset = DATASET_REPORT

    print("=" * 70)
    print("Creating BigQuery Views for LLM Activity Dashboard")
    print("=" * 70)
    print(f"Project: {project_id}")
    print(f"Dataset: {dataset}")
    print()

    client = get_client(config)

    views = [
        {
            "name": "v_session_url_distribution",
            "description": "Session counts by URL bucket (1, 2, 3, 4+) for dashboard charts",
            "sql": f"""
SELECT
  session_date,
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
  COUNT(*) AS session_count,
  SUM(COUNT(*)) OVER (PARTITION BY session_date) AS daily_total,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY session_date), 2) AS percentage
FROM `{project_id}.{dataset}.query_fanout_sessions`
GROUP BY session_date, url_bucket, sort_order
""",
        },
        {
            "name": "v_session_singleton_binary",
            "description": "Binary view of singleton (1 URL) vs plural (2+ URLs) sessions",
            "sql": f"""
SELECT
  session_date,
  IF(unique_urls = 1, 'Singleton (1 URL)', 'Plural (2+ URLs)') AS session_type,
  CASE WHEN unique_urls = 1 THEN 1 ELSE 2 END AS sort_order,
  COUNT(*) AS session_count,
  SUM(COUNT(*)) OVER (PARTITION BY session_date) AS daily_total,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY session_date), 2) AS percentage
FROM `{project_id}.{dataset}.query_fanout_sessions`
GROUP BY session_date, session_type, sort_order
""",
        },
        {
            "name": "v_bot_volume",
            "description": "Session counts by bot_name for bot analytics",
            "sql": f"""
SELECT
  session_date,
  bot_name,
  bot_provider,
  COUNT(*) AS session_count,
  SUM(COUNT(*)) OVER (PARTITION BY session_date) AS daily_total,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY session_date), 2) AS percentage,
  AVG(unique_urls) AS avg_urls_per_session,
  COUNTIF(unique_urls = 1) AS singleton_count,
  ROUND(100.0 * COUNTIF(unique_urls = 1) / COUNT(*), 2) AS singleton_rate
FROM `{project_id}.{dataset}.query_fanout_sessions`
GROUP BY session_date, bot_name, bot_provider
""",
        },
        {
            "name": "v_top_session_topics",
            "description": "Aggregated session topics from fanout_session_name for content analysis",
            "sql": f"""
SELECT
  session_date,
  fanout_session_name AS topic,
  COUNT(*) AS session_count,
  AVG(unique_urls) AS avg_urls_per_session,
  COUNTIF(unique_urls = 1) AS singleton_count,
  COUNTIF(unique_urls > 1) AS multi_url_count,
  AVG(CASE WHEN unique_urls > 1 THEN mean_cosine_similarity END) AS avg_mibcs_multi_url,
  COUNTIF(confidence_level = 'high') AS high_confidence_count,
  COUNTIF(confidence_level = 'medium') AS medium_confidence_count,
  COUNTIF(confidence_level = 'low') AS low_confidence_count
FROM `{project_id}.{dataset}.query_fanout_sessions`
WHERE fanout_session_name IS NOT NULL
GROUP BY session_date, fanout_session_name
""",
        },
        {
            "name": "v_daily_kpis",
            "description": "Pre-aggregated daily KPIs for dashboard scorecards",
            "sql": f"""
SELECT
  session_date,
  COUNT(*) AS total_sessions,
  AVG(unique_urls) AS avg_urls_per_session,
  COUNTIF(unique_urls = 1) AS singleton_count,
  ROUND(100.0 * COUNTIF(unique_urls = 1) / COUNT(*), 2) AS singleton_rate,
  COUNTIF(unique_urls > 1) AS multi_url_count,
  ROUND(100.0 * COUNTIF(unique_urls > 1) / COUNT(*), 2) AS multi_url_rate,
  AVG(CASE WHEN unique_urls > 1 THEN mean_cosine_similarity END) AS mean_mibcs_multi_url,
  COUNTIF(confidence_level = 'high') AS high_confidence_count,
  ROUND(100.0 * COUNTIF(confidence_level = 'high') / COUNT(*), 2) AS high_confidence_rate,
  COUNTIF(confidence_level = 'medium') AS medium_confidence_count,
  COUNTIF(confidence_level = 'low') AS low_confidence_count
FROM `{project_id}.{dataset}.query_fanout_sessions`
GROUP BY session_date
""",
        },
        {
            "name": "v_category_comparison",
            "description": "Fair comparison of user_request sessions vs training unique URLs",
            "sql": f"""
WITH user_sessions AS (
  SELECT
    session_date AS date,
    'User Questions' AS category,
    COUNT(*) AS count,
    1 AS sort_order
  FROM `{project_id}.{dataset}.query_fanout_sessions`
  GROUP BY session_date
),
training_urls AS (
  SELECT
    request_date AS date,
    'Training (Unique URLs)' AS category,
    SUM(unique_urls) AS count,
    2 AS sort_order
  FROM `{project_id}.{dataset}.daily_summary`
  WHERE bot_category = 'training'
  GROUP BY request_date
)
SELECT * FROM user_sessions
UNION ALL
SELECT * FROM training_urls
""",
        },
        {
            "name": "v_url_cooccurrence",
            "description": "URLs that appear together in multi-URL sessions for content clustering",
            "sql": f"""
SELECT
  session_date,
  url,
  bot_name,
  fanout_session_name AS topic,
  session_unique_urls,
  mean_cosine_similarity,
  confidence_level,
  COUNT(*) OVER (PARTITION BY session_date, url) AS url_appearances,
  COUNT(DISTINCT session_id) OVER (PARTITION BY session_date, url) AS unique_sessions
FROM `{project_id}.{dataset}.session_url_details`
WHERE session_unique_urls > 1
""",
        },
    ]

    success_count = 0
    for view in views:
        view_id = f"{project_id}.{dataset}.{view['name']}"
        print(f"\nCreating {view['name']}...")
        if create_view(client, view_id, view["sql"], view["description"]):
            success_count += 1

    print()
    print("=" * 70)
    print(f"Completed: {success_count}/{len(views)} views created successfully")
    print("=" * 70)

    if success_count == len(views):
        print("\nAll views ready for Looker Studio!")
        print("\nViews created:")
        for view in views:
            print(f"  - {dataset}.{view['name']}")

    return success_count == len(views)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
