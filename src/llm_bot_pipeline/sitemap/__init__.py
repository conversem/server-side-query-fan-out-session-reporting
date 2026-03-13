"""Sitemap fetching and parsing for URL freshness analysis."""

import logging
from typing import Optional

from .parser import SitemapEntry, fetch_sitemap, normalize_url_path

__all__ = [
    "SitemapEntry",
    "fetch_sitemap",
    "normalize_url_path",
    "fetch_and_store_sitemaps",
    "run_sitemap_pipeline",
]

logger = logging.getLogger(__name__)


def fetch_and_store_sitemaps(
    sitemap_urls: list[str],
    backend,
) -> dict:
    """Fetch sitemaps and store entries in the backend.

    Args:
        sitemap_urls: List of sitemap XML URLs to fetch
        backend: Storage backend with insert_sitemap_urls method

    Returns:
        Dict with keys: success, urls_stored, errors
    """
    errors = []
    all_entries = []

    for url in sitemap_urls:
        try:
            entries = fetch_sitemap(url)
            all_entries.extend(entries)
            logger.info(f"Fetched {len(entries)} URLs from {url}")
        except Exception as e:
            logger.warning(f"Failed to fetch sitemap {url}: {e}")
            errors.append(f"{url}: {e}")

    if not all_entries:
        logger.info("No sitemap URLs found")
        return {"success": True, "urls_stored": 0, "errors": errors}

    entry_dicts = [
        {
            "url": e.url,
            "url_path": e.url_path,
            "lastmod": e.lastmod,
            "lastmod_month": e.lastmod_month,
            "sitemap_source": e.sitemap_source,
        }
        for e in all_entries
    ]

    count = backend.insert_sitemap_urls(entry_dicts)
    logger.info(f"Stored {count} sitemap URLs")

    return {"success": len(errors) == 0, "urls_stored": count, "errors": errors}


def run_sitemap_pipeline(
    backend,
    sitemap_urls: Optional[list[str]] = None,
    reference_date=None,
    lookback_days: int = 365,
) -> dict:
    """Run the full sitemap pipeline: fetch, store, and aggregate.

    Loads sitemap_urls from settings if not provided.

    Args:
        backend: Storage backend instance
        sitemap_urls: Override sitemap URLs (loaded from settings if None)
        reference_date: Reference date for freshness calc (default: today)
        lookback_days: Days to look back for decay analysis

    Returns:
        Dict with keys: success, urls_stored, aggregation_results, skipped
    """
    from ..config.settings import get_settings
    from ..reporting.sitemap_aggregations import SitemapAggregator

    if sitemap_urls is None:
        settings = get_settings()
        sitemap_urls = settings.sitemap_urls

    if not sitemap_urls:
        logger.info("No sitemap URLs configured — skipping sitemap pipeline")
        return {"success": True, "urls_stored": 0, "skipped": True}

    # Fetch and store
    fetch_result = fetch_and_store_sitemaps(sitemap_urls, backend)

    # Aggregate
    aggregator = SitemapAggregator(backend=backend)
    agg_results = aggregator.run_all(
        reference_date=reference_date,
        lookback_days=lookback_days,
    )

    return {
        "success": fetch_result["success"] and all(r.success for r in agg_results),
        "urls_stored": fetch_result["urls_stored"],
        "aggregation_results": agg_results,
        "skipped": False,
    }
