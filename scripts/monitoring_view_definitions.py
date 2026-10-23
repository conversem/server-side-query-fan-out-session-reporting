"""
BigQuery monitoring view SQL definitions for the LLM bot traffic pipeline.

Contains view definitions for:
- Pipeline health monitoring
- Traffic summaries
- Provider analytics
- Content performance

Used by create_monitoring_views.py to create dashboard views.
"""

from llm_bot_pipeline.config.constants import DATASET_REPORT

# =============================================================================
# View SQL Templates (project_id, dataset placeholders)
# =============================================================================

VIEW_DEFINITIONS = {
    "v_pipeline_health": (
        """
            SELECT
                table_name,
                last_processed_date,
                last_updated_at,
                rows_processed,
                DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) AS hours_since_update,
                CASE
                    WHEN DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) <= 24 THEN 'FRESH'
                    WHEN DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) <= 48 THEN 'STALE'
                    ELSE 'CRITICAL'
                END AS freshness_status
            FROM `{project_id}.{dataset}.data_freshness`
            """,
        "Pipeline health status for all tracked tables",
    ),
    "v_stale_tables": (
        """
            SELECT
                table_name,
                last_processed_date,
                last_updated_at,
                DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) AS hours_since_update
            FROM `{project_id}.{dataset}.data_freshness`
            WHERE DATETIME_DIFF(CURRENT_TIMESTAMP(), last_updated_at, HOUR) > 24
            """,
        "Tables that haven't been updated in the last 24 hours",
    ),
    "v_daily_traffic_summary": (
        """
            SELECT
                request_date,
                SUM(total_requests) AS total_requests,
                SUM(successful_requests) AS successful_requests,
                SUM(error_requests) AS error_requests,
                SUM(redirect_requests) AS redirect_requests,
                ROUND(100.0 * SUM(successful_requests) / NULLIF(SUM(total_requests), 0), 2) AS success_rate_pct,
                ROUND(100.0 * SUM(error_requests) / NULLIF(SUM(total_requests), 0), 2) AS error_rate_pct,
                COUNT(DISTINCT bot_provider) AS unique_providers,
                COUNT(DISTINCT bot_name) AS unique_bots
            FROM `{project_id}.{dataset}.daily_summary`
            GROUP BY request_date
            """,
        "Daily traffic summary with success/error rates",
    ),
    "v_weekly_traffic_trend": (
        """
            WITH weekly_data AS (
                SELECT
                    DATE_TRUNC(request_date, WEEK(MONDAY)) AS week_start,
                    SUM(total_requests) AS total_requests,
                    SUM(successful_requests) AS successful_requests,
                    COUNT(DISTINCT bot_provider) AS unique_providers
                FROM `{project_id}.{dataset}.daily_summary`
                GROUP BY week_start
            )
            SELECT
                week_start,
                total_requests,
                successful_requests,
                unique_providers,
                LAG(total_requests) OVER (ORDER BY week_start) AS prev_week_requests,
                ROUND(
                    100.0 * (total_requests - LAG(total_requests) OVER (ORDER BY week_start))
                    / NULLIF(LAG(total_requests) OVER (ORDER BY week_start), 0),
                    2
                ) AS wow_change_pct
            FROM weekly_data
            """,
        "Week-over-week traffic comparison",
    ),
    "v_bot_category_split": (
        """
            SELECT
                request_date,
                bot_category,
                SUM(total_requests) AS total_requests,
                ROUND(
                    100.0 * SUM(total_requests) /
                    SUM(SUM(total_requests)) OVER (PARTITION BY request_date),
                    2
                ) AS daily_share_pct
            FROM `{project_id}.{dataset}.daily_summary`
            GROUP BY request_date, bot_category
            """,
        "Daily split between training and user_request bot categories",
    ),
    "v_provider_summary": (
        """
            SELECT
                bot_provider,
                SUM(total_requests) AS total_requests,
                COUNT(DISTINCT bot_name) AS unique_bots,
                COUNT(DISTINCT request_date) AS days_active,
                SUM(unique_urls) AS total_urls_accessed,
                ROUND(SUM(successful_requests) * 100.0 / NULLIF(SUM(total_requests), 0), 2) AS success_rate_pct,
                ROUND(
                    100.0 * SUM(total_requests) / SUM(SUM(total_requests)) OVER (),
                    2
                ) AS market_share_pct
            FROM `{project_id}.{dataset}.daily_summary`
            GROUP BY bot_provider
            """,
        "Provider summary with market share and success rates",
    ),
    "v_provider_daily": (
        """
            SELECT
                request_date,
                bot_provider,
                SUM(total_requests) AS total_requests,
                SUM(successful_requests) AS successful_requests,
                SUM(error_requests) AS error_requests,
                COUNT(DISTINCT bot_name) AS unique_bots
            FROM `{project_id}.{dataset}.daily_summary`
            GROUP BY request_date, bot_provider
            """,
        "Daily breakdown by provider",
    ),
    "v_top_content_7d": (
        """
            SELECT
                request_host,
                url_path,
                SUM(total_bot_requests) AS total_requests,
                SUM(user_request_hits) AS user_request_hits,
                SUM(training_hits) AS training_hits,
                MAX(unique_bot_providers) AS unique_providers,
                MIN(first_seen) AS first_seen,
                MAX(last_seen) AS last_seen
            FROM `{project_id}.{dataset}.url_performance`
            WHERE request_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            GROUP BY request_host, url_path
            """,
        "Top content by LLM bot interest (last 7 days)",
    ),
    "v_content_by_user_request": (
        """
            SELECT
                request_host,
                url_path,
                SUM(user_request_hits) AS user_request_hits,
                SUM(training_hits) AS training_hits,
                SUM(total_bot_requests) AS total_requests,
                ROUND(
                    100.0 * SUM(user_request_hits) / NULLIF(SUM(total_bot_requests), 0),
                    2
                ) AS user_request_pct
            FROM `{project_id}.{dataset}.url_performance`
            WHERE request_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
              AND user_request_hits > 0
            GROUP BY request_host, url_path
            """,
        "Content attracting user_request bots (real-time AI queries)",
    ),
    "v_traffic_anomaly_check": (
        """
            WITH daily_stats AS (
                SELECT
                    request_date,
                    SUM(total_requests) AS daily_total
                FROM `{project_id}.{dataset}.daily_summary`
                WHERE request_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                GROUP BY request_date
            ),
            stats AS (
                SELECT
                    AVG(daily_total) AS avg_requests,
                    STDDEV(daily_total) AS stddev_requests
                FROM daily_stats
                WHERE request_date < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            )
            SELECT
                ds.request_date,
                ds.daily_total AS total_requests,
                ROUND(stats.avg_requests, 0) AS historical_avg,
                ROUND(
                    ABS(ds.daily_total - stats.avg_requests) / NULLIF(stats.avg_requests, 0) * 100,
                    2
                ) AS variance_pct,
                CASE
                    WHEN ds.daily_total < stats.avg_requests - 2 * stats.stddev_requests
                        THEN 'ALERT_LOW'
                    WHEN ds.daily_total > stats.avg_requests + 2 * stats.stddev_requests
                        THEN 'ALERT_HIGH'
                    WHEN ABS(ds.daily_total - stats.avg_requests) > stats.stddev_requests
                        THEN 'WARNING'
                    ELSE 'NORMAL'
                END AS status
            FROM daily_stats ds
            CROSS JOIN stats
            """,
        "Traffic anomaly detection with status indicators",
    ),
    "v_error_rate_check": (
        """
            SELECT
                request_date,
                SUM(total_requests) AS total_requests,
                SUM(error_requests) AS error_requests,
                ROUND(100.0 * SUM(error_requests) / NULLIF(SUM(total_requests), 0), 2) AS error_rate_pct,
                CASE
                    WHEN 100.0 * SUM(error_requests) / NULLIF(SUM(total_requests), 0) > 10
                        THEN 'CRITICAL'
                    WHEN 100.0 * SUM(error_requests) / NULLIF(SUM(total_requests), 0) > 5
                        THEN 'WARNING'
                    ELSE 'OK'
                END AS status
            FROM `{project_id}.{dataset}.daily_summary`
            GROUP BY request_date
            """,
        "Error rate monitoring with alert thresholds",
    ),
    "v_executive_summary_7d": (
        """
            SELECT
                SUM(total_requests) AS total_requests,
                SUM(successful_requests) AS successful_requests,
                SUM(error_requests) AS error_requests,
                ROUND(100.0 * SUM(successful_requests) / NULLIF(SUM(total_requests), 0), 2) AS success_rate_pct,
                COUNT(DISTINCT bot_provider) AS unique_providers,
                COUNT(DISTINCT bot_name) AS unique_bots,
                SUM(unique_urls) AS unique_urls_accessed,
                SUM(CASE WHEN bot_category = 'user_request' THEN total_requests ELSE 0 END) AS user_request_hits,
                SUM(CASE WHEN bot_category = 'training' THEN total_requests ELSE 0 END) AS training_hits,
                ROUND(
                    100.0 * SUM(CASE WHEN bot_category = 'user_request' THEN total_requests ELSE 0 END)
                    / NULLIF(SUM(total_requests), 0),
                    2
                ) AS user_request_pct
            FROM `{project_id}.{dataset}.daily_summary`
            WHERE request_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            """,
        "Executive summary metrics for the last 7 days",
    ),
}


def get_view_definitions(project_id: str, dataset: str = DATASET_REPORT) -> dict:
    """
    Get all view definitions with project ID and dataset substituted.

    Args:
        project_id: GCP project ID
        dataset: BigQuery dataset name (default: DATASET_REPORT)

    Returns:
        Dictionary of view_name -> (sql_query, description)
    """
    result = {}
    for view_name, (sql_template, description) in VIEW_DEFINITIONS.items():
        sql = sql_template.format(project_id=project_id, dataset=dataset)
        result[view_name] = (sql, description)
    return result
