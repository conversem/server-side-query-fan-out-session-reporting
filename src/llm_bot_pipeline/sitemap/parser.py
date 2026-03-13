"""
Fetch and parse XML sitemaps for URL freshness analysis.

Handles both <urlset> (flat sitemaps) and <sitemapindex> (index files)
with recursive fetching. Extracts <loc> and <lastmod> from each URL entry.
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse
from xml.etree.ElementTree import Element, ParseError

import requests
from defusedxml.ElementTree import fromstring as safe_fromstring

logger = logging.getLogger(__name__)

XML_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Matches YYYY-MM-DD (optionally with time), YYYY-MM, or YYYY
_DATE_PATTERN = re.compile(r"^(\d{4})-(\d{2})(?:-(\d{2}))?")


@dataclass
class SitemapEntry:
    """A single URL extracted from a sitemap."""

    url: str
    url_path: str
    lastmod: Optional[str]
    lastmod_month: Optional[str]
    sitemap_source: str


def normalize_url_path(url: str) -> str:
    """Extract and normalize the path from a full URL.

    Strips scheme/host, lowercases. Preserves trailing slash to match
    server log URL format.
    E.g. ``https://www.example.com/some/path/`` -> ``/some/path/``
    """
    parsed = urlparse(url)
    path = parsed.path or "/"
    return path.lower()


def normalize_lastmod(raw: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Parse a lastmod string into (YYYY-MM-DD, YYYY-MM).

    Accepts ISO-8601 dates/datetimes, partial dates (YYYY-MM), etc.
    Returns (None, None) when parsing fails.
    """
    if not raw:
        return None, None

    raw = raw.strip()
    m = _DATE_PATTERN.match(raw)
    if not m:
        return None, None

    year, month = m.group(1), m.group(2)
    day = m.group(3)
    lastmod_month = f"{year}-{month}"
    lastmod = f"{year}-{month}-{day}" if day else None
    return lastmod, lastmod_month


def fetch_sitemap(
    url: str,
    *,
    timeout: int = 30,
    _depth: int = 0,
    _max_depth: int = 3,
) -> list[SitemapEntry]:
    """Fetch and parse an XML sitemap, recursing into sitemap indexes.

    Args:
        url: Sitemap URL to fetch.
        timeout: HTTP request timeout in seconds.

    Returns:
        Flat list of SitemapEntry objects from all referenced sitemaps.
    """
    if _depth > _max_depth:
        logger.warning("Max sitemap recursion depth reached at %s", url)
        return []

    logger.info("Fetching sitemap: %s", url)

    try:
        resp = requests.get(
            url, timeout=timeout, headers={"User-Agent": "SitemapParser/1.0"}
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to fetch sitemap %s: %s", url, e)
        return []

    try:
        root = safe_fromstring(resp.content)
    except ParseError as e:
        logger.error("Failed to parse XML from %s: %s", url, e)
        return []

    tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

    if tag == "sitemapindex":
        return _parse_sitemap_index(root, url, timeout, _depth, _max_depth)

    if tag == "urlset":
        return _parse_urlset(root, url)

    logger.warning("Unknown sitemap root element <%s> in %s", tag, url)
    return []


def _parse_sitemap_index(
    root: Element,
    source_url: str,
    timeout: int,
    depth: int,
    max_depth: int,
) -> list[SitemapEntry]:
    """Recurse into each <sitemap><loc> in a sitemap index."""
    entries: list[SitemapEntry] = []
    for sitemap_el in root.findall("sm:sitemap", XML_NS):
        loc_el = sitemap_el.find("sm:loc", XML_NS)
        if loc_el is not None and loc_el.text:
            child_entries = fetch_sitemap(
                loc_el.text.strip(),
                timeout=timeout,
                _depth=depth + 1,
                _max_depth=max_depth,
            )
            entries.extend(child_entries)
    logger.info("Sitemap index %s yielded %d URLs total", source_url, len(entries))
    return entries


def _parse_urlset(root: Element, source_url: str) -> list[SitemapEntry]:
    """Extract SitemapEntry objects from a <urlset>."""
    entries: list[SitemapEntry] = []
    for url_el in root.findall("sm:url", XML_NS):
        loc_el = url_el.find("sm:loc", XML_NS)
        if loc_el is None or not loc_el.text:
            continue

        full_url = loc_el.text.strip()
        lastmod_el = url_el.find("sm:lastmod", XML_NS)
        raw_lastmod = (
            lastmod_el.text.strip()
            if lastmod_el is not None and lastmod_el.text
            else None
        )

        lastmod, lastmod_month = normalize_lastmod(raw_lastmod)

        entries.append(
            SitemapEntry(
                url=full_url,
                url_path=normalize_url_path(full_url),
                lastmod=lastmod,
                lastmod_month=lastmod_month,
                sitemap_source=source_url,
            )
        )

    logger.info("Parsed %d URLs from %s", len(entries), source_url)
    return entries
