"""
Cloudflare filter definitions for LLM bot traffic.

Builds filter expressions for the Cloudflare Logpush/Logpull API.
Bot classification is done at the processing stage via user-agent matching.
"""

import json
from typing import Any

from ..config.constants import LLM_BOT_NAMES


def build_llm_bot_filter() -> dict[str, Any]:
    """
    Build Cloudflare filter for LLM bot traffic.

    Returns an empty filter — all bot classification is done at the
    processing stage via user-agent pattern matching against
    BOT_CLASSIFICATION, since the Cloudflare API does not support
    string-contains on user-agent.

    Returns:
        Filter dictionary for Cloudflare API (empty)
    """
    return {}


def get_filter_json() -> str:
    """
    Get the LLM bot filter as JSON string for Cloudflare API.

    Returns:
        JSON-encoded filter string
    """
    return json.dumps(build_llm_bot_filter())


def get_llm_bot_user_agent_patterns() -> list[str]:
    """
    Get user-agent patterns for LLM bot identification.

    These patterns are used for post-processing in SQLite
    since The API doesn't support user-agent substring matching.

    Returns:
        List of bot name patterns to match in user-agent
    """
    return LLM_BOT_NAMES.copy()
