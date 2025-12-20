"""
Bot classification from user-agent strings.

Parses user-agent strings to identify and classify LLM bots
into training vs user-request categories.
"""

import re
from dataclasses import dataclass
from typing import Optional

from ..config.constants import BOT_CLASSIFICATION


@dataclass
class BotClassification:
    """Result of bot classification."""

    bot_name: str
    bot_provider: str
    bot_category: str

    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary."""
        return {
            "bot_name": self.bot_name,
            "bot_provider": self.bot_provider,
            "bot_category": self.bot_category,
        }


# Pre-compiled regex patterns for efficient matching
# Pattern matches bot name in user-agent string (case-sensitive as bot names are specific)
_BOT_PATTERNS: dict[str, re.Pattern] = {
    bot_name: re.compile(rf"\b{re.escape(bot_name)}\b", re.IGNORECASE)
    for bot_name in BOT_CLASSIFICATION.keys()
}


def classify_bot(user_agent: Optional[str]) -> Optional[BotClassification]:
    """
    Classify a bot from its user-agent string.

    Parses the user-agent to identify known LLM bots and categorize them
    as either 'training' or 'user_request' bots.

    Args:
        user_agent: The HTTP User-Agent header value

    Returns:
        BotClassification with bot_name, bot_provider, and bot_category,
        or None if no known bot is identified

    Examples:
        >>> result = classify_bot("Mozilla/5.0 GPTBot/1.0")
        >>> result.bot_name
        'GPTBot'
        >>> result.bot_category
        'training'

        >>> classify_bot("Mozilla/5.0 (Windows NT 10.0) Chrome/120")
        None
    """
    if not user_agent:
        return None

    # Check each known bot pattern
    for bot_name, pattern in _BOT_PATTERNS.items():
        if pattern.search(user_agent):
            info = BOT_CLASSIFICATION[bot_name]
            return BotClassification(
                bot_name=bot_name,
                bot_provider=info["provider"],
                bot_category=info["category"],
            )

    return None


def classify_bot_dict(user_agent: Optional[str]) -> dict[str, Optional[str]]:
    """
    Classify a bot and return result as a dictionary.

    Convenience function that returns a dict with None values for unknown bots,
    useful for DataFrame operations.

    Args:
        user_agent: The HTTP User-Agent header value

    Returns:
        Dictionary with keys: bot_name, bot_provider, bot_category
        Values are None if no bot is identified
    """
    result = classify_bot(user_agent)
    if result:
        return result.to_dict()
    return {
        "bot_name": None,
        "bot_provider": None,
        "bot_category": None,
    }


def is_training_bot(user_agent: Optional[str]) -> bool:
    """
    Check if user-agent belongs to a training/crawling bot.

    Training bots typically scrape content for AI model training.

    Args:
        user_agent: The HTTP User-Agent header value

    Returns:
        True if the user-agent is from a known training bot
    """
    result = classify_bot(user_agent)
    return result is not None and result.bot_category == "training"


def is_user_request_bot(user_agent: Optional[str]) -> bool:
    """
    Check if user-agent belongs to a user-request bot.

    User-request bots fetch content in response to user queries.

    Args:
        user_agent: The HTTP User-Agent header value

    Returns:
        True if the user-agent is from a known user-request bot
    """
    result = classify_bot(user_agent)
    return result is not None and result.bot_category == "user_request"


def get_bot_names_by_category(category: str) -> list[str]:
    """
    Get list of bot names for a specific category.

    Args:
        category: Either 'training' or 'user_request'

    Returns:
        List of bot names in that category
    """
    return [
        name
        for name, info in BOT_CLASSIFICATION.items()
        if info["category"] == category
    ]


def get_bot_names_by_provider(provider: str) -> list[str]:
    """
    Get list of bot names for a specific provider.

    Args:
        provider: Provider name (e.g., 'OpenAI', 'Anthropic', 'Google')

    Returns:
        List of bot names from that provider
    """
    return [
        name
        for name, info in BOT_CLASSIFICATION.items()
        if info["provider"] == provider
    ]

