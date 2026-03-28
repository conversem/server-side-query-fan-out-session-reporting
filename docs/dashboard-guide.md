# Dashboard Guide — LLM Bot Pipeline

> **v2.1.2** — All views include `domain`. Use `full_url` as a clickable link. Total views: 15.

This guide covers building Looker Studio dashboards on top of the LLM Bot Pipeline reporting
views. It applies to both SQLite (via the BigQuery-SQLite connector or direct connection) and
BigQuery backends.

---

## 1. Connecting Your Data

### BigQuery

1. In Looker Studio, click **Add data → BigQuery**.
2. Select your project and dataset.
3. Add each view as a separate data source (one per page, or shared where columns overlap).
4. Set the date dimension to `session_date` or `request_date` depending on the view.

### SQLite

Use the [SQLite connector for Looker Studio](https://github.com/your-org/looker-sqlite-connector)
or export to BigQuery first via `scripts/export_sqlite_to_bq.py`. For local development, you
can also connect via a Google Sheets intermediary.

---

## 2. The Golden Rule: Always Use `full_url`

Every view that exposes a URL path now includes a `full_url` computed column:

```sql
CONCAT('https://', domain, url_path) AS full_url
```

**Always use `full_url` in dashboard tables and URL cards**, not `url_path` or `url`. Reasons:

- `url_path` is ambiguous when multiple domains share the same paths (e.g. `/over-ons/`).
- `full_url` is clickable in Looker Studio — set the **hyperlink** field to `full_url` in
  the data source configuration.
- `url` in `session_url_details` is the raw URL as seen in logs (may contain query strings,
  port numbers, or protocol variants). Prefer `full_url` for display.

**To set up clickable links in Looker Studio:**

1. In the data source editor, find the `full_url` field.
2. Click the field type icon → set to **URL**.
3. In table charts, add `full_url` as a dimension and configure the column to display as a
   hyperlink.

---

## 3. Suggested Dashboard Structure

### Page 1: Executive Summary

**Purpose:** Top-level KPIs and session trends for stakeholders.

**Data source:** `v_daily_kpis`, `v_session_url_distribution`

**Suggested charts:**
- Scorecards: `SUM(total_sessions)`, `AVG(avg_urls_per_session)`, `AVG(singleton_rate)`,
  `AVG(mean_mibcs_multi_url)`
- Time series: `total_sessions` over `session_date`
- Donut / bar: `v_session_url_distribution` — sessions by URL bucket (1, 2, 3, 4+)
- Stacked bar: singleton vs multi-URL sessions over time (`v_session_singleton_binary`)

**Filters:** Date range, `domain`

---

### Page 2: Domain Comparison

**Purpose:** Side-by-side view of LLM activity across all monitored domains.

**Data source:** `v_daily_kpis`, `v_bot_volume`, `v_url_freshness`

**Suggested charts:**
- Bar chart: `total_sessions` by `domain`
- Time series: `total_sessions` split by `domain` (use `domain` as a breakdown dimension)
- Table: `domain`, `total_sessions`, `singleton_rate`, `avg_urls_per_session`
- Bar: top bots per domain (`v_bot_volume` filtered to each domain)

**Setup tip:** Use a `domain` filter control connected to all charts on this page. In
Looker Studio, add a **Filter control** → Dimension: `domain` → Style: checkbox or dropdown.

---

### Page 3: Content & Topics

**Purpose:** Which content is LLMs fetching, and what topics are driving multi-URL sessions.

**Data source:** `v_url_cooccurrence`, `v_top_session_topics`

**Suggested charts:**
- Table: `full_url`, `domain`, `session_unique_urls`, `fanout_session_name`, `confidence_level`
  — filter to `session_unique_urls > 1` for multi-URL sessions only.
  Set `full_url` column as a hyperlink.
- Bar / word cloud: `v_top_session_topics` — `topic` by `session_count`
- Scatter: `avg_urls_per_session` vs `session_count` per topic

**Caveat — `v_top_session_topics`:** Topics are assigned per session, not per domain. A topic
like "duurzaam wonen" may appear in sessions spanning both `example.nl` and `example.be`.
Add a `domain` filter to scope topics to a single site, but note that `fanout_session_name`
itself is domain-agnostic.

---

### Page 4: Freshness & Decay

**Purpose:** How content age and sitemap freshness correlate with LLM crawl activity.

**Data source:** `v_url_freshness`, `v_url_freshness_detail`, `v_decay_unique_urls_by_domain`,
`v_decay_request_volume_by_domain`, `v_sessions_by_content_age`, `v_url_performance_with_freshness`

**Suggested charts:**
- Table: `v_url_freshness_detail` — `full_url`, `domain`, `lastmod`, `months_since_lastmod`,
  `request_count`. Sort by `request_count DESC` to see freshest/most-crawled pages.
  Add `WHERE lastmod IS NULL` filter to find un-sitemapped but bot-crawled pages.
- Line chart: `v_decay_unique_urls_by_domain` — `cumulative_pct` over `months_since_lastmod`,
  broken down by `domain`. Shows the decay curve of unique URLs per domain.
- Line chart: `v_decay_request_volume_by_domain` — same but for request volume.
- Scatter / bar: `v_sessions_by_content_age` — `months_since_lastmod` buckets vs session count.
  Filter `months_since_lastmod >= 6` to focus on stale content.
- Table: `v_url_performance_with_freshness` — `full_url`, `domain`, `request_count`,
  `lastmod`, `months_since_lastmod`. Filter `lastmod IS NULL` to find pages cited by bots
  but absent from the sitemap.

#### Critical caveat: decay curve denominators

`v_decay_unique_urls_by_domain` and `v_decay_request_volume_by_domain` use a **per-domain
denominator**: `cumulative_pct` is the percentage of *that domain's* total unique URLs (or
request volume) that falls within the given age bucket.

**Do not** mix rows from different domains in a single cumulative-percentage calculation. Use
`domain` as a breakdown dimension (not a filter that excludes domains) so each domain's curve
is drawn independently.

The older global decay views (`v_url_decay_by_age_bucket`, `v_url_request_volume_by_age_bucket`)
use a single denominator across all domains. In single-domain deployments they are equivalent.
In multi-domain deployments, prefer the `_by_domain` variants.

---

### Page 5: Bot Analytics

**Purpose:** Which bots are active, what is their singleton rate, and how does this vary over time.

**Data source:** `v_bot_volume`, `v_daily_kpis`

**Suggested charts:**
- Bar: `bot_name` by `session_count` (total period)
- Time series: `session_count` over `session_date` per `bot_name`
- Table: `bot_name`, `bot_provider`, `SUM(session_count)`, `AVG(singleton_rate)`,
  `AVG(avg_urls_per_session)`
- Scorecard: total distinct bots active (`COUNT(DISTINCT bot_name)`)

---

### Page 6: Session Explorer

**Purpose:** Drill-down into individual sessions and URLs for investigation.

**Data source:** `session_url_details` (direct table, not a view)

**Suggested charts:**
- Table: `session_id`, `session_date`, `domain`, `bot_name`, `fanout_session_name`,
  `session_unique_urls`, `confidence_level`
- On row click / filter: show `full_url` list for that session

**Note:** `session_url_details` is a large table. Always apply a date range filter. In
Looker Studio, add a **Required filter** to prevent unbounded queries.

---

## 4. Which View for Which Goal

| Goal | View | Key columns |
|---|---|---|
| Daily KPI scorecards | `v_daily_kpis` | `total_sessions`, `singleton_rate`, `avg_urls_per_session`, `mean_mibcs_multi_url` |
| URL bucket distribution | `v_session_url_distribution` | `url_bucket`, `session_count` |
| Singleton vs multi-URL | `v_session_singleton_binary` | `session_type`, `session_count` |
| Bot activity over time | `v_bot_volume` | `bot_name`, `session_count`, `singleton_rate` |
| Top session topics | `v_top_session_topics` | `topic`, `session_count`, `avg_urls_per_session` |
| User vs training comparison | `v_category_comparison` | `category`, `count` |
| URLs in multi-URL sessions | `v_url_cooccurrence` | `full_url`, `domain`, `session_unique_urls`, `topic` |
| Per-URL freshness overview | `v_url_freshness` | `domain`, `url_path`, `request_count`, `bot_count` |
| Per-URL freshness with age | `v_url_freshness_detail` | `full_url`, `domain`, `lastmod`, `months_since_lastmod` |
| Sessions on stale content | `v_sessions_by_content_age` | `months_since_lastmod`, `session_count` |
| URL traffic vs sitemap | `v_url_performance_with_freshness` | `full_url`, `request_count`, `lastmod` |
| Domain decay (unique URLs) | `v_decay_unique_urls_by_domain` | `domain`, `months_since_lastmod`, `cumulative_pct` |
| Domain decay (volume) | `v_decay_request_volume_by_domain` | `domain`, `months_since_lastmod`, `cumulative_pct` |
| Global decay (unique URLs) | `v_url_decay_by_age_bucket` | `months_since_lastmod`, `cumulative_pct` |
| Global decay (volume) | `v_url_request_volume_by_age_bucket` | `months_since_lastmod`, `cumulative_pct` |

---

## 5. Key Column Reference

| Column | Found in | Meaning |
|---|---|---|
| `domain` | All views (v2.1.2+) | Hostname without protocol or `www.` prefix, e.g. `example.nl` |
| `full_url` | `v_url_cooccurrence`, `v_url_freshness_detail`, `v_sessions_by_content_age`, `v_url_performance_with_freshness` | `CONCAT('https://', domain, url_path)` — use as hyperlink |
| `url_path` | Sitemap and URL views | Path component only, e.g. `/over-ons/` |
| `url` | `session_url_details` | Raw URL from request logs — may include query strings |
| `fanout_session_name` | Session views | Semantic topic name assigned to the session |
| `cumulative_pct` | Decay views | Percentage of total (domain-scoped in `_by_domain` views) within this age bucket |
| `months_since_lastmod` | Freshness views | Months since the page's `<lastmod>` in the sitemap |
| `request_count` | `url_performance`, freshness/decay views | Raw bot request count for this URL |
| `mean_mibcs_multi_url` | `v_daily_kpis` | Mean inter-bundle cosine similarity, scoped to multi-URL sessions |
| `singleton_rate` | `v_daily_kpis`, `v_bot_volume` | Percentage of sessions with exactly 1 URL |

---

## 6. Domain Filter Setup

To filter all charts on a page by domain:

1. In Looker Studio, click **Add a control → Drop-down list**.
2. Set **Data source** to any view that has `domain`.
3. Set **Control field** to `domain`.
4. In the **Properties** panel, enable **Apply to all data sources on this page** if you want
   one filter to control all charts simultaneously.
5. Optionally set a **Default value** to your primary domain.

If charts use different data sources, you may need to configure **cross-data-source filters**
in Looker Studio's resource settings.

---

## 7. Decay Curve Caveats

### Global vs per-domain

The original decay views (`v_url_decay_by_age_bucket`, `v_url_request_volume_by_age_bucket`)
compute `cumulative_pct` against a single global denominator that spans all domains. In a
single-domain setup, these are equivalent to the per-domain views. In a multi-domain setup:

- Using these views with a `domain` filter gives incorrect percentages — the denominator
  still includes all domains.
- Use `v_decay_unique_urls_by_domain` and `v_decay_request_volume_by_domain` instead when
  comparing decay curves per domain.

### What "cumulative_pct" means

`cumulative_pct` is the cumulative percentage of URLs (or request volume) whose sitemap
`lastmod` falls within the given age bucket or older. A value of `80` at `months_since_lastmod = 3`
means 80% of the domain's URLs (by count or volume) have a `lastmod` within the last 3 months.

Pages with `lastmod IS NULL` are excluded from the decay calculation but are visible in
`v_url_performance_with_freshness`.

### Stale content analysis

To find content that bots are actively crawling despite being old:

```sql
SELECT full_url, domain, request_count, months_since_lastmod
FROM v_url_performance_with_freshness
WHERE months_since_lastmod >= 6
ORDER BY request_count DESC
LIMIT 20;
```

To find pages cited by bots but absent from the sitemap:

```sql
SELECT full_url, domain, request_count
FROM v_url_performance_with_freshness
WHERE lastmod IS NULL
ORDER BY request_count DESC;
```
