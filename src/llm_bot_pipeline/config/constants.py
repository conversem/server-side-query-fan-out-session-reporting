"""
Constants for LLM bot classification and Cloudflare field definitions.
"""

# =============================================================================
# Query Fan-Out Session Configuration
# =============================================================================

# Time window options for session grouping (validated via research)
# See docs/research/ for validation details
#
# Timing analysis of user_request records shows:
#   - Mode: 9ms, Median: 10ms, P75: 15ms, P90: 53ms
#   - 84% of burst gaps are ≤20ms
#
# Window comparison (high confidence %):
#   - 50ms: 94.6% high confidence, captures 97% of burst gaps
#   - 100ms: 93.9% high confidence, captures 91% of burst gaps
#
WINDOW_50MS = 50  # Tighter grouping, higher coherence
WINDOW_100MS = 100  # Conservative, well-tested default

# Default window for production use
OPTIMAL_WINDOW_MS = WINDOW_100MS

# =============================================================================
# LLM Bot Classification
# =============================================================================

# Maps bot name patterns to their provider and category
BOT_CLASSIFICATION = {
    # OpenAI
    "GPTBot": {"provider": "OpenAI", "category": "training"},
    "ChatGPT-User": {"provider": "OpenAI", "category": "user_request"},
    "OAI-SearchBot": {"provider": "OpenAI", "category": "user_request"},
    # Anthropic
    "ClaudeBot": {"provider": "Anthropic", "category": "training"},
    "Claude-User": {"provider": "Anthropic", "category": "user_request"},
    "Claude-SearchBot": {"provider": "Anthropic", "category": "user_request"},
    # Google
    "Google-Extended": {"provider": "Google", "category": "training"},
    # Perplexity
    "PerplexityBot": {"provider": "Perplexity", "category": "user_request"},
    # Apple
    "Applebot-Extended": {"provider": "Apple", "category": "training"},
    # Microsoft - Note: bingbot is a regular search engine crawler, NOT Copilot user requests
    # It should be excluded from query fan-out session analysis
    "bingbot": {"provider": "Microsoft", "category": "search_engine"},
}

# List of LLM bot names for filtering
LLM_BOT_NAMES = list(BOT_CLASSIFICATION.keys())

# Cloudflare Logpull output fields
OUTPUT_FIELDS = [
    "EdgeStartTimestamp",
    "ClientRequestURI",
    "ClientRequestHost",
    "ClientRequestUserAgent",
    "BotScore",
    "BotScoreSrc",
    "VerifiedBot",
    "BotTags",
    "ClientIP",
    "ClientCountry",
    "EdgeResponseStatus",
]

# Basic fields (works with standard Logpull API token)
OUTPUT_FIELDS_BASIC = [
    "EdgeStartTimestamp",
    "ClientRequestURI",
    "ClientRequestHost",
    "ClientRequestUserAgent",
    "ClientIP",
    "ClientCountry",
    "EdgeResponseStatus",
    "RayID",
]

# SQLite table names
TABLE_RAW_BOT_REQUESTS = "raw_bot_requests"
TABLE_CLEAN_BOT_REQUESTS = "bot_requests_daily"
TABLE_DAILY_SUMMARY = "daily_summary"
TABLE_URL_PERFORMANCE = "url_performance"
TABLE_QUERY_FANOUT_SESSIONS = "query_fanout_sessions"
