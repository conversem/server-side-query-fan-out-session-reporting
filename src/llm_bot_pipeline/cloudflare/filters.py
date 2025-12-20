"""
Cloudflare Cloudflare filter definitions for LLM bot traffic.

Builds filter expressions to extract verified LLM bot traffic from http_requests dataset.
"""

import json
from typing import Any

from ..config.constants import LLM_BOT_NAMES


def build_verified_bot_filter() -> dict[str, Any]:
    """
    Build Cloudflare filter for verified bot traffic.

    Returns filter that matches: VerifiedBot = true

    Returns:
        Filter dictionary for Cloudflare Cloudflare API
    """
    return {"where": {"key": "VerifiedBot", "operator": "eq", "value": True}}


def build_llm_bot_filter() -> dict[str, Any]:
    """
    Build Cloudflare filter for verified LLM bot traffic.

    Creates filter that matches:
    - VerifiedBot = true (ensures only verified bots)

    Note: Additional user-agent filtering is done at the SQLite processing
    stage since Cloudflare filters don't support string contains on user-agent.

    Returns:
        Filter dictionary for Cloudflare Cloudflare API
    """
    # The API doesn't support 'contains' operator for user-agent filtering.
    # We filter for verified bots here, then further classify in SQLite.
    return build_verified_bot_filter()


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
