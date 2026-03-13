"""
Unit tests for bot_classifier module.

Tests user-agent parsing and LLM bot classification.
"""

import pytest

from llm_bot_pipeline.utils.bot_classifier import (
    BotClassification,
    classify_bot,
    classify_bot_dict,
    get_bot_names_by_category,
    get_bot_names_by_provider,
    is_training_bot,
    is_user_request_bot,
)


class TestClassifyBot:
    """Tests for classify_bot function."""

    @pytest.mark.parametrize(
        "user_agent,bot_name,bot_provider,bot_category",
        [
            (
                "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.0; +https://openai.com/gptbot)",
                "GPTBot",
                "OpenAI",
                "training",
            ),
            (
                "Mozilla/5.0 (compatible; ChatGPT-User/1.0; +https://openai.com/bot)",
                "ChatGPT-User",
                "OpenAI",
                "user_request",
            ),
            (
                "Mozilla/5.0 (compatible; ClaudeBot/1.0; +https://anthropic.com)",
                "ClaudeBot",
                "Anthropic",
                "training",
            ),
            (
                "Mozilla/5.0 (compatible; Claude-User/1.0)",
                "Claude-User",
                "Anthropic",
                "user_request",
            ),
            (
                "Mozilla/5.0 (compatible; Google-Extended)",
                "Google-Extended",
                "Google",
                "training",
            ),
            (
                "Mozilla/5.0 (compatible; PerplexityBot/1.0)",
                "PerplexityBot",
                "Perplexity",
                "search",
            ),
            (
                "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
                "bingbot",
                "Microsoft",
                "search",
            ),
            (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Applebot-Extended/0.1",
                "Applebot-Extended",
                "Apple",
                "training",
            ),
            (
                "Mozilla/5.0 (compatible; OAI-SearchBot/1.0)",
                "OAI-SearchBot",
                "OpenAI",
                "search",
            ),
            (
                "Mozilla/5.0 (compatible; Claude-SearchBot/1.0)",
                "Claude-SearchBot",
                "Anthropic",
                "search",
            ),
        ],
    )
    def test_bot_classification(self, user_agent, bot_name, bot_provider, bot_category):
        """User-agent should be classified with expected bot name, provider, category."""
        result = classify_bot(user_agent)
        assert result is not None
        assert result.bot_name == bot_name
        assert result.bot_provider == bot_provider
        assert result.bot_category == bot_category


class TestUnknownBots:
    """Tests for handling unknown or invalid user-agents."""

    @pytest.mark.parametrize(
        "user_agent",
        [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
            "",
            "Mozilla/5.0 (compatible; SomeOtherBot/1.0)",
        ],
    )
    def test_returns_none(self, user_agent):
        """Unknown or invalid user-agent should return None."""
        result = classify_bot(user_agent)
        assert result is None

    def test_none_returns_none(self):
        """None input should return None."""
        result = classify_bot(None)
        assert result is None


class TestCaseInsensitiveMatching:
    """Tests for case-insensitive bot matching."""

    @pytest.mark.parametrize(
        "user_agent,expected_bot_name",
        [
            ("Mozilla/5.0 (compatible; gptbot/1.0)", "GPTBot"),
            ("Mozilla/5.0 (compatible; BINGBOT/2.0)", "bingbot"),
        ],
    )
    def test_case_insensitive_matching(self, user_agent, expected_bot_name):
        """Bot names should match case-insensitively."""
        result = classify_bot(user_agent)
        assert result is not None
        assert result.bot_name == expected_bot_name


class TestClassifyBotDict:
    """Tests for classify_bot_dict function."""

    def test_known_bot_returns_dict(self):
        """Known bot should return populated dict."""
        result = classify_bot_dict("GPTBot/1.0")
        assert result == {
            "bot_name": "GPTBot",
            "bot_provider": "OpenAI",
            "bot_category": "training",
        }

    def test_unknown_returns_none_values(self):
        """Unknown user-agent should return dict with None values."""
        result = classify_bot_dict("Chrome/120")
        assert result == {
            "bot_name": None,
            "bot_provider": None,
            "bot_category": None,
        }


class TestBotCategoryHelpers:
    """Tests for is_training_bot and is_user_request_bot."""

    @pytest.mark.parametrize(
        "user_agent,expected",
        [
            ("GPTBot/1.0", True),
            ("ChatGPT-User/1.0", False),
            ("Chrome/120", False),
        ],
    )
    def test_is_training_bot(self, user_agent, expected):
        """is_training_bot should return expected result for user-agent."""
        assert is_training_bot(user_agent) is expected

    @pytest.mark.parametrize(
        "user_agent,expected",
        [
            ("ChatGPT-User/1.0", True),
            ("GPTBot/1.0", False),
        ],
    )
    def test_is_user_request_bot(self, user_agent, expected):
        """is_user_request_bot should return expected result for user-agent."""
        assert is_user_request_bot(user_agent) is expected


class TestBotNameHelpers:
    """Tests for get_bot_names_by_category and get_bot_names_by_provider."""

    def test_get_training_bots(self):
        """Should return all training bot names."""
        training_bots = get_bot_names_by_category("training")
        assert "GPTBot" in training_bots
        assert "ClaudeBot" in training_bots
        assert "Google-Extended" in training_bots
        assert "Applebot-Extended" in training_bots
        # User request bots should not be in training
        assert "ChatGPT-User" not in training_bots
        assert "PerplexityBot" not in training_bots

    def test_get_user_request_bots(self):
        """Should return all user_request bot names."""
        user_bots = get_bot_names_by_category("user_request")
        assert "ChatGPT-User" in user_bots
        assert "Claude-User" in user_bots
        assert "Perplexity-User" in user_bots
        # PerplexityBot is search, not user_request
        assert "PerplexityBot" not in user_bots
        # bingbot is search, not user_request
        assert "bingbot" not in user_bots
        # Training bots should not be in user_request
        assert "GPTBot" not in user_bots
        assert "ClaudeBot" not in user_bots

    def test_get_openai_bots(self):
        """Should return all OpenAI bot names."""
        openai_bots = get_bot_names_by_provider("OpenAI")
        assert "GPTBot" in openai_bots
        assert "ChatGPT-User" in openai_bots
        assert "OAI-SearchBot" in openai_bots
        # Other providers should not be included
        assert "ClaudeBot" not in openai_bots

    def test_get_anthropic_bots(self):
        """Should return all Anthropic bot names."""
        anthropic_bots = get_bot_names_by_provider("Anthropic")
        assert "ClaudeBot" in anthropic_bots
        assert "Claude-User" in anthropic_bots
        assert "Claude-SearchBot" in anthropic_bots


class TestBotClassificationDataclass:
    """Tests for BotClassification dataclass."""

    def test_to_dict(self):
        """to_dict should return proper dictionary."""
        classification = BotClassification(
            bot_name="TestBot",
            bot_provider="TestProvider",
            bot_category="training",
        )
        assert classification.to_dict() == {
            "bot_name": "TestBot",
            "bot_provider": "TestProvider",
            "bot_category": "training",
        }
