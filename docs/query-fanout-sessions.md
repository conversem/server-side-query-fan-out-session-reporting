# Query Fan-Out Sessions

## What Is a Query Fan-Out Session?

When an LLM bot processes a user question it typically fires a burst of HTTP requests to a website within a very short time window — fetching multiple relevant pages in parallel or in rapid succession. This burst is called a **Query Fan-Out Session**: a group of requests from a single bot IP that belong to a single user question.

The pipeline captures these sessions through two sequential stages:

1. **Temporal bundling** — requests within a configurable time window (default 100 ms) from the same IP and bot provider are grouped into a single session.
2. **Semantic refinement** — sessions where multiple independent queries accidentally merged (IP collision) are detected and split into distinct sub-sessions.

---

## Per-Domain Constraint (NON-NEGOTIABLE)

**Every query fan-out session belongs to exactly one domain. This is a fundamental design invariant, not a configuration option.**

### Why

- The pipeline processes each domain's log data independently.
- A session represents a bot querying a specific website — by definition it targets one domain.
- Mixing domains inside a session would corrupt all KPIs: request counts, fan-out ratios, MIBCS coherence scores, and refinement rates would all be invalid.
- Reporting queries are domain-scoped; a cross-domain session has no valid place in any domain's analytics.

### How It Is Enforced

The per-domain constraint is enforced at every layer:

| Layer | Enforcement mechanism |
|---|---|
| **Ingestion** | Each domain's log files are ingested separately; the `domain` field is set at record creation. |
| **Session creation** | `SessionRecord.domain` is derived from the first request in the bundle (`session_aggregations.py`). Sub-bundles produced by refinement inherit `EnrichedBundle.domain` from their parent. |
| **Database** | The `query_fanout_sessions` table has a `domain` column; all insert paths set it. |
| **Reporting queries** | All 29 reporting methods in `kpi.py`, `timeseries.py`, `session.py`, and `refinement.py` accept a `domain` parameter and apply `_domain_filter()` in their SQL `WHERE` clause. |
| **Split-session details** | `get_split_session_details` applies `_domain_filter()` twice — once in the CTE that selects parent session IDs, and again in the outer `WHERE` clause that retrieves sub-sessions — so no cross-domain sub-session can appear even under data integrity failure. |

---

## Session Splitting

Splitting produces sub-sessions from a single parent session. Both splitting mechanisms are per-domain; no splitting operation can create a session that spans multiple domains.

### 1. Temporal Splitting (time-bound sessions)

The temporal bundler groups requests within a rolling time window. Because input data is already domain-scoped at ingestion, every bundle (and therefore every time-bound session) is strictly single-domain. Two requests from different domains will never share a session regardless of timing.

### 2. Semantic Splitting (collision refinement)

After temporal bundling, the refinement stage detects **collision bundles** — sessions where requests from two or more independent user questions were merged because the same bot IP was reused within the window. Refinement uses MIBCS (Mean Intra-Bundle Cosine Similarity) to identify low-coherence bundles and splits them into semantically coherent sub-sessions.

Sub-sessions produced by semantic splitting:

- Carry the same `domain` as their parent session (`EnrichedBundle.domain` is propagated).
- Are recorded in `query_fanout_sessions` with `parent_session_id` pointing to the original bundle and `was_refined = True`.
- Are never split across domains — the refinement algorithm operates on URL-level semantic similarity within a single domain's data.

---

## Data Model

```
query_fanout_sessions
├── session_id          Unique identifier (UUID or derived from bundle_id)
├── session_date        Date of the session
├── domain              Target domain — always a single domain (NOT NULL in practice)
├── bot_provider        Provider (openai, anthropic, google, …)
├── bot_name            Specific bot user-agent name
├── request_count       Total requests in the session
├── unique_urls         Deduplicated URL count
├── mean_cosine_similarity  MIBCS — intra-bundle semantic coherence
├── confidence_level    'high' / 'medium' / 'low'
├── fanout_session_name Derived human-readable topic (from first URL)
├── url_list            JSON array of deduplicated URLs (order preserved)
├── parent_session_id   Set for sub-sessions produced by refinement
├── was_refined         True if this session was produced by splitting
└── refinement_reason   'semantic_split' or NULL
```

Related table: `session_url_details` — one row per URL per session (denormalized from `url_list` for URL-level queries). See [backend-guide.md](backend-guide.md) for schema details.

---

## Reporting Query Domain Filtering

All reporting methods use `_domain_filter()` from `LocalQueryBase`:

```python
@staticmethod
def _domain_filter(domain: Optional[str] = None) -> str:
    if domain:
        safe = domain.replace("'", "''")
        return f"AND domain = '{safe}'"
    return ""
```

This returns `""` when no domain is given (returns all domains) and `AND domain = '...'` when a domain is specified. Single-quote doubling is the standard SQLite SQL-injection defence for string literals.

### Split-session query (dual filter)

`get_split_session_details` is the only method that uses a CTE + JOIN pattern. The domain filter is applied to both the CTE and the outer query to guarantee isolation:

```sql
WITH parent_sessions AS (
    SELECT DISTINCT parent_session_id
    FROM query_fanout_sessions
    WHERE session_date >= '...' AND session_date <= '...'
      AND parent_session_id IS NOT NULL
      AND domain = '<domain>'          -- selects parent IDs for this domain
    LIMIT <limit>
)
SELECT ...
FROM parent_sessions p
JOIN query_fanout_sessions s ON s.parent_session_id = p.parent_session_id
WHERE 1=1
  AND domain = '<domain>'             -- sub-sessions must also belong to this domain
GROUP BY p.parent_session_id
```

---

## Related Documentation

- [Architecture overview](architecture.md) — pipeline stages and module structure
- [Storage backend guide](backend-guide.md) — SQLite schema, tables, and dashboard views
- [Research methodology](architecture.md#research-methodology) — OptScore, time window selection, MIBCS
