"""Integration test for URL resource type filtering in local_sqlite pipeline.

Verifies that the end-to-end pipeline correctly drops non-user-facing
resources (JS, CSS, fonts) and keeps documents and images with the
correct resource_type classification.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from llm_bot_pipeline.config.constants import TABLE_CLEAN_BOT_REQUESTS
from llm_bot_pipeline.config.settings import UrlFilteringSettings, clear_settings_cache
from llm_bot_pipeline.pipeline.local_pipeline import LocalPipeline
from llm_bot_pipeline.storage import get_backend

GPTBOT_UA = "Mozilla/5.0 (compatible; GPTBot/1.2; +https://openai.com/gptbot)"
TEST_DATE = date(2024, 1, 14)
TEST_TIMESTAMP = datetime(2024, 1, 14, 10, 0, 0, tzinfo=timezone.utc).isoformat()


def _make_raw_record(uri: str, seq: int = 0) -> dict:
    """Build a raw record dict matching the raw_bot_requests schema."""
    ts = datetime(2024, 1, 14, 10, 0, seq, tzinfo=timezone.utc)
    return {
        "EdgeStartTimestamp": ts.isoformat(),
        "ClientRequestURI": uri,
        "ClientRequestHost": "example.com",
        "ClientRequestUserAgent": GPTBOT_UA,
        "ClientIP": f"10.0.0.{seq + 1}",
        "ClientCountry": "NL",
        "EdgeResponseStatus": 200,
    }


class TestUrlFilteringPipeline:
    """Verify URL filtering through the full local_sqlite pipeline."""

    def test_url_filtering_drops_assets_keeps_documents_and_images(self, tmp_path):
        """Pipeline should drop JS/CSS/font URLs and keep doc/image URLs."""
        clear_settings_cache()

        db_path = tmp_path / "url_filter_test.db"
        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()

        raw_records = [
            _make_raw_record("/zonnepanelen/advies", seq=0),
            _make_raw_record("/images/solar-panel.jpg", seq=1),
            _make_raw_record("/assets/js/chunks-es/Table.880558c.js", seq=2),
            _make_raw_record("/styles/main.css", seq=3),
            _make_raw_record("/fonts/roboto.woff2", seq=4),
        ]
        backend.insert_raw_records(raw_records)

        pipeline = LocalPipeline(backend=backend)
        result = pipeline.run(
            start_date=TEST_DATE,
            end_date=TEST_DATE,
            mode="full",
        )
        assert result.success, f"Pipeline failed: {result.errors}"

        clean = backend.query(f"SELECT * FROM {TABLE_CLEAN_BOT_REQUESTS}")
        assert len(clean) == 2, (
            f"Expected 2 clean records (document + image), got {len(clean)}: "
            f"{[r['url_path'] for r in clean]}"
        )

        by_path = {r["url_path"]: r for r in clean}

        assert "/zonnepanelen/advies" in by_path
        assert by_path["/zonnepanelen/advies"]["resource_type"] == "document"

        assert "/images/solar-panel.jpg" in by_path
        assert by_path["/images/solar-panel.jpg"]["resource_type"] == "image"

        dropped_paths = {
            "/assets/js/chunks-es/Table.880558c.js",
            "/styles/main.css",
            "/fonts/roboto.woff2",
        }
        assert dropped_paths.isdisjoint(
            by_path.keys()
        ), f"Asset URLs should have been filtered: {dropped_paths & by_path.keys()}"

        pipeline.close()

    def test_url_filtering_counts_match_pipeline_result(self, tmp_path):
        """Pipeline result transformed_rows should match clean table count."""
        db_path = tmp_path / "url_filter_counts.db"
        backend = get_backend("sqlite", db_path=db_path)
        backend.initialize()

        raw_records = [
            _make_raw_record("/page/one", seq=0),
            _make_raw_record("/page/two", seq=1),
            _make_raw_record("/bundle.js", seq=2),
        ]
        backend.insert_raw_records(raw_records)

        pipeline = LocalPipeline(backend=backend)
        result = pipeline.run(
            start_date=TEST_DATE,
            end_date=TEST_DATE,
            mode="full",
        )
        assert result.success

        clean = backend.query(f"SELECT COUNT(*) as cnt FROM {TABLE_CLEAN_BOT_REQUESTS}")
        assert clean[0]["cnt"] == result.transformed_rows
        assert result.transformed_rows == 2

        pipeline.close()
