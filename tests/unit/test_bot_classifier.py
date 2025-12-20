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

    def test_gptbot_training(self):
        """GPTBot should be classified as OpenAI training bot."""
        user_agent = "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.0; +https://openai.com/gptbot)"
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "GPTBot"
        assert result.bot_provider == "OpenAI"
        assert result.bot_category == "training"

    def test_chatgpt_user_request(self):
        """ChatGPT-User should be classified as OpenAI user_request bot."""
        user_agent = (
            "Mozilla/5.0 (compatible; ChatGPT-User/1.0; +https://openai.com/bot)"
        )
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "ChatGPT-User"
        assert result.bot_provider == "OpenAI"
        assert result.bot_category == "user_request"

    def test_claudebot_training(self):
        """ClaudeBot should be classified as Anthropic training bot."""
        user_agent = "Mozilla/5.0 (compatible; ClaudeBot/1.0; +https://anthropic.com)"
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "ClaudeBot"
        assert result.bot_provider == "Anthropic"
        assert result.bot_category == "training"

    def test_claude_user_request(self):
        """Claude-User should be classified as Anthropic user_request bot."""
        user_agent = "Mozilla/5.0 (compatible; Claude-User/1.0)"
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "Claude-User"
        assert result.bot_provider == "Anthropic"
        assert result.bot_category == "user_request"

    def test_google_extended_training(self):
        """Google-Extended should be classified as Google training bot."""
        user_agent = "Mozilla/5.0 (compatible; Google-Extended)"
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "Google-Extended"
        assert result.bot_provider == "Google"
        assert result.bot_category == "training"

    def test_perplexitybot_user_request(self):
        """PerplexityBot should be classified as Perplexity user_request bot."""
        user_agent = "Mozilla/5.0 (compatible; PerplexityBot/1.0)"
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "PerplexityBot"
        assert result.bot_provider == "Perplexity"
        assert result.bot_category == "user_request"

    def test_bingbot_search_engine(self):
        """bingbot should be classified as Microsoft user_request bot."""
        user_agent = (
            "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"
        )
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "bingbot"
        assert result.bot_provider == "Microsoft"
        assert result.bot_category == "search_engine"

    def test_applebot_extended_training(self):
        """Applebot-Extended should be classified as Apple training bot."""
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Applebot-Extended/0.1"
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "Applebot-Extended"
        assert result.bot_provider == "Apple"
        assert result.bot_category == "training"

    def test_oai_searchbot_user_request(self):
        """OAI-SearchBot should be classified as OpenAI user_request bot."""
        user_agent = "Mozilla/5.0 (compatible; OAI-SearchBot/1.0)"
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "OAI-SearchBot"
        assert result.bot_provider == "OpenAI"
        assert result.bot_category == "user_request"

    def test_claude_searchbot_user_request(self):
        """Claude-SearchBot should be classified as Anthropic user_request bot."""
        user_agent = "Mozilla/5.0 (compatible; Claude-SearchBot/1.0)"
        result = classify_bot(user_agent)

        assert result is not None
        assert result.bot_name == "Claude-SearchBot"
        assert result.bot_provider == "Anthropic"
        assert result.bot_category == "user_request"


class TestUnknownBots:
    """Tests for handling unknown or invalid user-agents."""

    def test_regular_browser_returns_none(self):
        """Regular browser user-agent should return None."""
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0"
        result = classify_bot(user_agent)
        assert result is None

    def test_empty_string_returns_none(self):
        """Empty string should return None."""
        result = classify_bot("")
        assert result is None

    def test_none_returns_none(self):
        """None input should return None."""
        result = classify_bot(None)
        assert result is None

    def test_unknown_bot_returns_none(self):
        """Unknown bot should return None."""
        user_agent = "Mozilla/5.0 (compatible; SomeOtherBot/1.0)"
        result = classify_bot(user_agent)
        assert result is None


class TestCaseInsensitiveMatching:
    """Tests for case-insensitive bot matching."""

    def test_lowercase_gptbot(self):
        """Bot names should match case-insensitively."""
        user_agent = "Mozilla/5.0 (compatible; gptbot/1.0)"
        result = classify_bot(user_agent)
        assert result is not None
        assert result.bot_name == "GPTBot"

    def test_uppercase_bingbot(self):
        """bingbot should match even in uppercase."""
        user_agent = "Mozilla/5.0 (compatible; BINGBOT/2.0)"
        result = classify_bot(user_agent)
        assert result is not None
        assert result.bot_name == "bingbot"


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

    def test_is_training_bot_true(self):
        """GPTBot should be identified as training bot."""
        assert is_training_bot("GPTBot/1.0") is True

    def test_is_training_bot_false_for_user_request(self):
        """ChatGPT-User should not be identified as training bot."""
        assert is_training_bot("ChatGPT-User/1.0") is False

    def test_is_training_bot_false_for_unknown(self):
        """Unknown bot should not be identified as training bot."""
        assert is_training_bot("Chrome/120") is False

    def test_is_user_request_bot_true(self):
        """ChatGPT-User should be identified as user_request bot."""
        assert is_user_request_bot("ChatGPT-User/1.0") is True

    def test_is_user_request_bot_false_for_training(self):
        """GPTBot should not be identified as user_request bot."""
        assert is_user_request_bot("GPTBot/1.0") is False


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
        assert "PerplexityBot" in user_bots
        # bingbot is search_engine, not user_request
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
