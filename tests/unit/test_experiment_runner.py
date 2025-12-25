"""
Unit tests for the experiment runner.

Tests the ExperimentConfig and ExperimentRunner classes
that read from SQLite databases.
"""

import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import pytest

from llm_bot_pipeline.research.experiment_runner import (
    ExperimentConfig,
    ExperimentRunner,
)


class TestExperimentConfig:
    """Tests for ExperimentConfig dataclass."""

    def test_default_values(self):
        """ExperimentConfig should have sensible defaults for SQLite."""
        config = ExperimentConfig()

        assert config.db_path == "data/llm-bot-logs.db"
        assert config.table_name == "bot_requests_daily"
        assert config.timestamp_col == "request_timestamp"
        assert config.url_col == "request_uri"
        assert config.group_by == "bot_provider"
        assert config.filter_category == "user_request"
        assert "Microsoft" in config.exclude_providers

    def test_custom_values(self):
        """ExperimentConfig should accept custom values."""
        config = ExperimentConfig(
            db_path="custom/path.db",
            table_name="custom_table",
            timestamp_col="ts",
            url_col="uri",
        )

        assert config.db_path == "custom/path.db"
        assert config.table_name == "custom_table"
        assert config.timestamp_col == "ts"
        assert config.url_col == "uri"


class TestExperimentRunner:
    """Tests for ExperimentRunner class."""

    @pytest.fixture
    def sample_db(self, tmp_path):
        """Create a sample SQLite database with test data."""
        db_path = tmp_path / "test.db"

        # Create sample data matching bot_requests_daily schema
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        data = []

        # Create clusters of requests for OpenAI
        for i in range(10):
            # Cluster of 3 requests within 100ms
            for j in range(3):
                data.append(
                    {
                        "request_timestamp": (
                            base_time + timedelta(seconds=i * 2, milliseconds=j * 30)
                        ).isoformat(),
                        "request_uri": f"/page{i}_{j}",
                        "request_host": "example.com",
                        "bot_provider": "OpenAI",
                        "bot_category": "user_request",
                        "bot_name": "ChatGPT-User",
                        "response_status": 200,
                    }
                )

        # Add some Microsoft/bingbot requests (should be excluded)
        for i in range(5):
            data.append(
                {
                    "request_timestamp": (
                        base_time + timedelta(seconds=30 + i)
                    ).isoformat(),
                    "request_uri": f"/bing{i}",
                    "request_host": "example.com",
                    "bot_provider": "Microsoft",
                    "bot_category": "training",
                    "bot_name": "bingbot",
                    "response_status": 200,
                }
            )

        df = pd.DataFrame(data)

        with sqlite3.connect(str(db_path)) as conn:
            df.to_sql("bot_requests_daily", conn, index=False)

        return db_path

    def test_load_data_from_sqlite(self, sample_db):
        """ExperimentRunner should load data from SQLite database."""
        config = ExperimentConfig(
            db_path=str(sample_db),
            table_name="bot_requests_daily",
            filter_category=None,  # Don't filter for this test
            exclude_providers=[],  # Don't exclude for this test
        )
        runner = ExperimentRunner(config)

        df = runner.load_data()

        # Should load all 35 records (30 OpenAI + 5 Microsoft)
        assert len(df) == 35

    def test_load_data_filters_by_category(self, sample_db):
        """ExperimentRunner should filter by bot_category."""
        config = ExperimentConfig(
            db_path=str(sample_db),
            table_name="bot_requests_daily",
            filter_category="user_request",
            exclude_providers=[],
        )
        runner = ExperimentRunner(config)

        df = runner.load_data()

        # Should only have OpenAI user_request records (30)
        assert len(df) == 30
        assert all(df["bot_category"] == "user_request")

    def test_load_data_excludes_providers(self, sample_db):
        """ExperimentRunner should exclude specified providers."""
        config = ExperimentConfig(
            db_path=str(sample_db),
            table_name="bot_requests_daily",
            filter_category=None,
            exclude_providers=["Microsoft"],
        )
        runner = ExperimentRunner(config)

        df = runner.load_data()

        # Should exclude Microsoft (5 records)
        assert len(df) == 30
        assert "Microsoft" not in df["bot_provider"].values

    def test_load_data_file_not_found(self, tmp_path):
        """ExperimentRunner should raise FileNotFoundError for missing db."""
        config = ExperimentConfig(
            db_path=str(tmp_path / "nonexistent.db"),
        )
        runner = ExperimentRunner(config)

        with pytest.raises(FileNotFoundError, match="Database not found"):
            runner.load_data()

    def test_load_data_invalid_table_name(self, sample_db):
        """ExperimentRunner should reject invalid table names."""
        config = ExperimentConfig(
            db_path=str(sample_db),
            table_name="malicious_table; DROP TABLE--",
        )
        runner = ExperimentRunner(config)

        with pytest.raises(ValueError, match="Invalid table name"):
            runner.load_data()

    def test_split_data_temporal(self, sample_db):
        """split_data should split data temporally."""
        config = ExperimentConfig(
            db_path=str(sample_db),
            table_name="bot_requests_daily",
            filter_category=None,
            exclude_providers=[],
            validation_split=0.2,
        )
        runner = ExperimentRunner(config)

        df = runner.load_data()
        train_df, val_df = runner.split_data(df)

        # 80% train, 20% validation
        assert len(train_df) == int(35 * 0.8)
        assert len(val_df) == 35 - int(35 * 0.8)

        # Train data should be earlier than validation data
        train_max = pd.to_datetime(train_df[config.timestamp_col]).max()
        val_min = pd.to_datetime(val_df[config.timestamp_col]).min()
        assert train_max <= val_min
