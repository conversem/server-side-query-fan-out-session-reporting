"""Discover calendar dates that the nightly job should refill."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

from ...cloudflare.logpull import RETENTION_DAYS

if TYPE_CHECKING:
    from ...config.settings import DomainConfig
    from ...storage.base import StorageBackend

logger = logging.getLogger(__name__)

REASON_EMPTY_SOURCE = "empty_source"
REASON_FAILED_TRANSFORM = "failed_transform"


def has_cloudflare_domains(domains: Sequence[DomainConfig]) -> bool:
    """True if any configured domain uses the Cloudflare log source."""
    return any(dc.provider == "cloudflare" for dc in domains)


@dataclass(frozen=True)
class MissingDay:
    day: date
    domain: str
    raw_count: int
    clean_count: int
    reason: str


def list_missing_days(
    backend: StorageBackend,
    domains: list[str],
    start_date: date,
    end_date: date,
    min_raw_rows: int = 100,
    raw_table: str = "raw_bot_requests",
    clean_table: str = "bot_requests_daily",
    host_suffixes: list[str] | None = None,
) -> list[MissingDay]:
    if not domains:
        return []
    if end_date < start_date:
        raise ValueError("end_date is before start_date")
    suffixes = list(host_suffixes) if host_suffixes is not None else [""] * len(domains)
    if len(suffixes) != len(domains):
        raise ValueError("host_suffixes must be the same length as domains")

    raw_id = backend.get_full_table_id(raw_table)
    clean_id = backend.get_full_table_id(clean_table)
    # Contract: raw in 1..min_raw_rows-1 is trivial and not a missing day.
    # Predicate: clean_count = 0 AND (raw_count = 0 OR raw_count >= @min_raw_rows)
    # Empty suffix: match the stamped domain label. Non-empty: apex + *.suffix hosts.
    sql = f"""
    WITH dates AS (
      SELECT d FROM UNNEST(GENERATE_DATE_ARRAY(@start_date, @end_date)) AS d
    ),
    domain_list AS (
      SELECT domain, suffix
      FROM UNNEST(@domains) AS domain WITH OFFSET off
      JOIN UNNEST(@host_suffixes) AS suffix WITH OFFSET off2
        ON off = off2
    ),
    grid AS (
      SELECT dates.d, domain_list.domain
      FROM dates CROSS JOIN domain_list
    ),
    raw_days AS (
      SELECT DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) AS d,
             dl.domain AS domain, COUNT(*) AS raw_count
      FROM `{raw_id}` r
      CROSS JOIN domain_list dl
      WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)))
            BETWEEN @start_date AND @end_date
        AND (
          (dl.suffix = '' AND r.domain = dl.domain)
          OR (
            dl.suffix != ''
            AND (
              LOWER(IFNULL(r.ClientRequestHost, '')) = dl.suffix
              OR ENDS_WITH(
                LOWER(IFNULL(r.ClientRequestHost, '')),
                CONCAT('.', dl.suffix)
              )
            )
          )
        )
      GROUP BY d, domain
    ),
    clean_days AS (
      SELECT c.request_date AS d, dl.domain AS domain, COUNT(*) AS clean_count
      FROM `{clean_id}` c
      CROSS JOIN domain_list dl
      WHERE c.request_date BETWEEN @start_date AND @end_date
        AND (
          (dl.suffix = '' AND c.domain = dl.domain)
          OR (
            dl.suffix != ''
            AND (
              LOWER(IFNULL(c.request_host, '')) = dl.suffix
              OR ENDS_WITH(
                LOWER(IFNULL(c.request_host, '')),
                CONCAT('.', dl.suffix)
              )
            )
          )
        )
      GROUP BY d, domain
    ),
    counted AS (
      SELECT g.d AS d, g.domain AS domain,
             IFNULL(r.raw_count, 0) AS raw_count,
             IFNULL(c.clean_count, 0) AS clean_count
      FROM grid g
      LEFT JOIN raw_days r ON g.d = r.d AND g.domain = r.domain
      LEFT JOIN clean_days c ON g.d = c.d AND g.domain = c.domain
    )
    SELECT d, domain, raw_count, clean_count,
           CASE
             WHEN raw_count = 0 THEN '{REASON_EMPTY_SOURCE}'
             ELSE '{REASON_FAILED_TRANSFORM}'
           END AS reason
    FROM counted
    WHERE clean_count = 0
      AND (raw_count = 0 OR raw_count >= @min_raw_rows)
    ORDER BY d, domain
    """
    rows = backend.query(
        sql,
        params={
            "start_date": start_date,
            "end_date": end_date,
            "domains": domains,
            "host_suffixes": suffixes,
            "min_raw_rows": int(min_raw_rows),
        },
    )
    return [
        MissingDay(
            day=(
                row["d"]
                if isinstance(row["d"], date)
                else date.fromisoformat(str(row["d"]))
            ),
            domain=str(row["domain"]),
            raw_count=int(row["raw_count"]),
            clean_count=int(row["clean_count"]),
            reason=str(row["reason"]),
        )
        for row in rows
    ]


def effective_lookback_days(
    requested: int,
    has_cloudflare: bool,
    respect_cloudflare_retention: bool = True,
) -> int:
    if requested < 1:
        raise ValueError("lookback_days must be >= 1")
    if has_cloudflare and respect_cloudflare_retention and requested > RETENTION_DAYS:
        logger.warning(
            "auto_backfill.lookback_days=%s clamped to Cloudflare retention %s",
            requested,
            RETENTION_DAYS,
        )
        return RETENTION_DAYS
    return requested


def _as_utc(now_utc: datetime) -> datetime:
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    return now_utc.astimezone(timezone.utc)


def _retention_floor_utc(now_utc: datetime) -> datetime:
    return _as_utc(now_utc) - timedelta(days=RETENTION_DAYS)


def _first_retained_date(now_utc: datetime) -> date:
    floor = _retention_floor_utc(now_utc)
    candidate = floor.date()
    midnight = datetime.combine(candidate, datetime.min.time(), tzinfo=timezone.utc)
    if midnight < floor:
        return candidate + timedelta(days=1)
    return candidate


def nightly_window_start(
    yesterday: date,
    now_utc: datetime,
    lookback: int,
    has_cloudflare: bool,
    respect: bool = True,
) -> date:
    calendar_start = yesterday - timedelta(days=lookback - 1)
    if not has_cloudflare or not respect:
        return calendar_start
    return max(calendar_start, _first_retained_date(now_utc))


def plan_nightly_dates(
    yesterday: date, missing: list[MissingDay], window_start: date
) -> list[date]:
    """Yesterday first, then in-window holes, ascending."""
    holes = sorted({m.day for m in missing if window_start <= m.day < yesterday})
    return [yesterday, *holes]


def schedule_nightly_dates(
    yesterday: date, missing: list[MissingDay], lookback_days: int
) -> list[date]:
    """Calendar-only wrapper around plan_nightly_dates.

    Production plans via nightly_window_start + plan_nightly_dates.
    This wrapper does not apply the Cloudflare retention floor.
    """
    window_start = yesterday - timedelta(days=lookback_days - 1)
    return plan_nightly_dates(yesterday, missing, window_start)
