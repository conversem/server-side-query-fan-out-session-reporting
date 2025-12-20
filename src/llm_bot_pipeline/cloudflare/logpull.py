"""
Cloudflare Logpull API integration for local data ingestion.

Provides functions to pull HTTP request logs directly from Cloudflare's API,
enabling local development and testing without requiring Logpull to SQLite.

Key features:
- Pull logs for date ranges (with 7-day retention limit)
- Automatic pagination for large time ranges (1-hour chunks)
- Rate limiting with exponential backoff
- Filter for verified LLM bots
- Direct ingestion to SQLite backend
"""

import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

import httpx
from cloudflare import Cloudflare

from ..config.constants import OUTPUT_FIELDS, OUTPUT_FIELDS_BASIC
from ..config.settings import Settings, get_settings
from ..utils.bot_classifier import classify_bot

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Cloudflare Logpull limits
MAX_TIME_RANGE_HOURS = 1  # Maximum time range per request
RETENTION_DAYS = 7  # Cloudflare log retention period
DEFAULT_BATCH_SIZE = 1000  # Records per batch for SQLite insert

# Rate limiting
DEFAULT_REQUESTS_PER_MINUTE = 15
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_BASE_DELAY_SECONDS = 2.0


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class LogpullResult:
    """Result of a logpull operation."""

    success: bool
    records_pulled: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class IngestionResult:
    """Result of ingesting logs to storage."""

    success: bool
    records_ingested: int = 0
    records_failed: int = 0
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    error: Optional[str] = None
    duration_seconds: float = 0.0
    chunks_processed: int = 0


@dataclass
class RateLimiter:
    """Simple rate limiter for API calls."""

    requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE
    _request_times: list = field(default_factory=list)

    def wait_if_needed(self) -> None:
        """
        Wait if we've exceeded the rate limit, then record the request.

        This method both waits (if needed) and records the request timestamp.
        Do not call record_request() separately after calling this method.
        """
        now = time.time()
        # Remove requests older than 1 minute
        self._request_times = [t for t in self._request_times if now - t < 60]

        if len(self._request_times) >= self.requests_per_minute:
            # Wait until the oldest request is 1 minute old
            sleep_time = 60 - (now - self._request_times[0]) + 0.1
            if sleep_time > 0:
                logger.debug(f"Rate limit reached, sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)

        # Record this request
        self._request_times.append(time.time())


# =============================================================================
# Core Functions
# =============================================================================


def _ensure_utc(dt: datetime) -> datetime:
    """
    Ensure datetime has UTC timezone.

    Args:
        dt: Datetime (may be naive or timezone-aware)

    Returns:
        Timezone-aware datetime in UTC

    Raises:
        ValueError: If datetime is naive (no timezone info)
    """
    if dt.tzinfo is None:
        raise ValueError(
            f"Datetime {dt} has no timezone. Please provide timezone-aware datetime "
            f"(e.g., datetime(..., tzinfo=timezone.utc))"
        )
    return dt.astimezone(timezone.utc)


def get_cloudflare_client(settings: Optional[Settings] = None) -> Cloudflare:
    """
    Create authenticated Cloudflare client.

    Args:
        settings: Application settings (uses default if None)

    Returns:
        Authenticated Cloudflare client
    """
    if settings is None:
        settings = get_settings()
    return Cloudflare(api_token=settings.cloudflare_api_token)


def pull_logs(
    start_time: datetime,
    end_time: datetime,
    zone_id: Optional[str] = None,
    settings: Optional[Settings] = None,
    fields: Optional[list[str]] = None,
    filter_verified_bots: bool = True,
    filter_llm_bots: bool = True,
    rate_limiter: Optional[RateLimiter] = None,
) -> Iterator[dict]:
    """
    Pull logs from Cloudflare Logpull API.

    Automatically handles pagination by breaking large time ranges into
    1-hour chunks (Cloudflare's limit).

    Args:
        start_time: Start time (UTC)
        end_time: End time (UTC)
        zone_id: Cloudflare zone ID (uses settings if None)
        settings: Application settings (uses default if None)
        fields: Fields to retrieve (uses OUTPUT_FIELDS if None)
        filter_verified_bots: If True, only return verified bot traffic
        filter_llm_bots: If True, only return LLM bot traffic (by user-agent)
        rate_limiter: Rate limiter instance (creates default if None)

    Yields:
        Log record dictionaries

    Raises:
        ValueError: If time range exceeds retention period

    Note:
        Cloudflare has a 7-day retention limit for Logpull.
        Times should be in UTC.
    """
    if settings is None:
        settings = get_settings()

    if zone_id is None:
        zone_id = settings.cloudflare_zone_id

    if fields is None:
        fields = OUTPUT_FIELDS

    if rate_limiter is None:
        rate_limiter = RateLimiter()

    # Ensure timezone-aware datetimes in UTC
    start_time = _ensure_utc(start_time)
    end_time = _ensure_utc(end_time)

    # Validate time range
    now = datetime.now(timezone.utc)
    retention_limit = now - timedelta(days=RETENTION_DAYS)

    if start_time < retention_limit:
        raise ValueError(
            f"Start time {start_time} exceeds {RETENTION_DAYS}-day retention limit. "
            f"Earliest available: {retention_limit}"
        )

    if end_time > now:
        end_time = now
        logger.warning(f"End time adjusted to current time: {end_time}")

    # Create client
    client = get_cloudflare_client(settings)

    # Break into 1-hour chunks
    current_start = start_time
    chunks_failed = 0

    while current_start < end_time:
        current_end = min(
            current_start + timedelta(hours=MAX_TIME_RANGE_HOURS), end_time
        )

        # Apply rate limiting
        rate_limiter.wait_if_needed()

        # Fetch logs for this chunk
        try:
            yield from _fetch_logs_chunk(
                client=client,
                zone_id=zone_id,
                start_time=current_start,
                end_time=current_end,
                fields=fields,
                filter_verified_bots=filter_verified_bots,
                filter_llm_bots=filter_llm_bots,
            )
        except Exception as e:
            chunks_failed += 1
            logger.error(
                f"Error fetching logs for {current_start} - {current_end}: {e}. "
                f"Chunk {chunks_failed} failed, continuing with next chunk."
            )
            # Continue with next chunk instead of failing entirely
            # Data for this chunk will be missing

        current_start = current_end

    if chunks_failed > 0:
        logger.warning(
            f"Completed with {chunks_failed} failed chunk(s). "
            f"Some data may be missing."
        )


def _fetch_logs_chunk(
    client: Cloudflare,
    zone_id: str,
    start_time: datetime,
    end_time: datetime,
    fields: list[str],
    filter_verified_bots: bool = True,
    filter_llm_bots: bool = True,
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
) -> Iterator[dict]:
    """
    Fetch logs for a single time chunk with retry logic.

    Args:
        client: Cloudflare client
        zone_id: Zone ID
        start_time: Chunk start time
        end_time: Chunk end time
        fields: Fields to retrieve
        filter_verified_bots: Filter for verified bots
        filter_llm_bots: Filter for LLM bots by user-agent
        retry_attempts: Number of retry attempts
        base_delay: Base delay for exponential backoff

    Yields:
        Log record dictionaries
    """
    # Use Unix epoch timestamps (seconds) - more reliable with Cloudflare API
    start_epoch = int(start_time.timestamp())
    end_epoch = int(end_time.timestamp())
    now_epoch = int(datetime.now(timezone.utc).timestamp())

    logger.debug(
        f"Fetching logs: {start_time} to {end_time} (epoch: {start_epoch}-{end_epoch})"
    )

    # Get API token for direct HTTP requests
    from ..config.settings import get_settings as _get_settings

    _settings = _get_settings()
    _api_token = _settings.cloudflare_api_token

    last_error = None
    current_fields = fields  # Start with requested fields

    for attempt in range(retry_attempts):
        try:
            # Use direct HTTP request instead of SDK method
            # The SDK's logs.received.get() fails with 403 for Bot Management fields
            # but direct HTTP requests with Bearer token work correctly
            api_url = (
                f"https://api.cloudflare.com/client/v4/zones/{zone_id}/logs/received"
            )
            headers = {
                "Authorization": f"Bearer {_api_token}",
                "Content-Type": "application/json",
            }
            params = {
                "start": start_epoch,
                "end": end_epoch,
                "fields": ",".join(current_fields),
            }

            response = httpx.get(api_url, headers=headers, params=params, timeout=60.0)

            # If 403 with full fields, try basic fields (without Bot Management)
            if response.status_code == 403 and current_fields != OUTPUT_FIELDS_BASIC:
                logger.warning(
                    "Full fields failed (403), falling back to basic fields (no Bot Management)"
                )
                current_fields = OUTPUT_FIELDS_BASIC
                continue  # Retry with basic fields

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text[:500]}")

            # Process NDJSON response (one JSON object per line)
            record_count = 0
            filtered_count = 0
            using_basic_fields = current_fields == OUTPUT_FIELDS_BASIC
            for line in response.text.strip().split("\n"):
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Apply verified bot filter if requested
                # Skip this filter when using basic fields (no VerifiedBot field available)
                if filter_verified_bots and not using_basic_fields:
                    if not record.get("VerifiedBot", False):
                        continue

                # Apply LLM bot filter by user-agent pattern
                if filter_llm_bots:
                    user_agent = record.get("ClientRequestUserAgent", "")
                    bot_info = classify_bot(user_agent)
                    if bot_info is None:
                        filtered_count += 1
                        continue
                    # Enrich record with bot classification
                    record["_bot_name"] = bot_info.bot_name
                    record["_bot_provider"] = bot_info.bot_provider
                    record["_bot_category"] = bot_info.bot_category

                record_count += 1
                yield record

            if filter_llm_bots and filtered_count > 0:
                logger.debug(
                    f"Fetched {record_count} LLM bot records for chunk (filtered {filtered_count} non-LLM requests)"
                )
            else:
                logger.debug(f"Fetched {record_count} records for chunk")
            return  # Success, exit retry loop

        except Exception as e:
            last_error = e
            if attempt < retry_attempts - 1:
                # Exponential backoff with jitter to avoid thundering herd
                delay = base_delay * (2**attempt)
                jitter = random.uniform(0, delay * 0.1)  # Up to 10% jitter
                total_delay = delay + jitter
                logger.warning(
                    f"Logpull attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {total_delay:.1f}s..."
                )
                time.sleep(total_delay)
            else:
                logger.error(f"Logpull failed after {retry_attempts} attempts: {e}")

    # All retries exhausted
    if last_error:
        raise last_error


def pull_logs_for_date_range(
    start_date: date,
    end_date: date,
    settings: Optional[Settings] = None,
    filter_verified_bots: bool = True,
    filter_llm_bots: bool = True,
) -> Iterator[dict]:
    """
    Convenience function to pull logs for a date range.

    Converts dates to datetimes (midnight to midnight UTC).

    Args:
        start_date: Start date
        end_date: End date (inclusive)
        settings: Application settings
        filter_verified_bots: Filter for verified bots
        filter_llm_bots: Filter for LLM bots by user-agent

    Yields:
        Log record dictionaries
    """
    # Convert dates to datetime (UTC midnight)
    start_time = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    end_time = datetime.combine(
        end_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
    )

    yield from pull_logs(
        start_time=start_time,
        end_time=end_time,
        settings=settings,
        filter_verified_bots=filter_verified_bots,
        filter_llm_bots=filter_llm_bots,
    )


# =============================================================================
# SQLite Ingestion
# =============================================================================


def ingest_to_sqlite(
    start_date: date,
    end_date: date,
    db_path: Optional[Path | str] = None,
    settings: Optional[Settings] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    filter_verified_bots: bool = True,
    filter_llm_bots: bool = True,
) -> IngestionResult:
    """
    Pull logs from Cloudflare and insert into SQLite.

    Provides a complete workflow for local data ingestion:
    1. Pulls logs from Cloudflare Logpull API
    2. Batches records for efficient insertion
    3. Inserts into SQLite backend

    Args:
        start_date: Start date
        end_date: End date (inclusive)
        db_path: Path to SQLite database (uses settings default if None)
        settings: Application settings
        batch_size: Number of records per insert batch
        filter_verified_bots: Filter for verified bots only
        filter_llm_bots: Filter for LLM bots only (by user-agent)

    Returns:
        IngestionResult with statistics
    """
    from ..storage import get_backend

    start_time_total = time.time()

    if settings is None:
        settings = get_settings()

    # Initialize SQLite backend
    backend_kwargs = {}
    if db_path:
        backend_kwargs["db_path"] = Path(db_path)

    try:
        backend = get_backend("sqlite", **backend_kwargs)
        backend.initialize()
    except Exception as e:
        return IngestionResult(
            success=False,
            start_date=start_date,
            end_date=end_date,
            error=f"Failed to initialize SQLite backend: {e}",
        )

    records_ingested = 0
    records_failed = 0
    chunks_processed = 0
    batch: list[dict] = []

    try:
        # Pull and ingest logs
        for record in pull_logs_for_date_range(
            start_date=start_date,
            end_date=end_date,
            settings=settings,
            filter_verified_bots=filter_verified_bots,
            filter_llm_bots=filter_llm_bots,
        ):
            batch.append(record)

            # Insert batch when full
            if len(batch) >= batch_size:
                try:
                    inserted = backend.insert_raw_records(batch)
                    records_ingested += inserted
                    chunks_processed += 1
                    logger.debug(f"Inserted batch of {inserted} records")
                except Exception as e:
                    logger.error(f"Failed to insert batch: {e}")
                    records_failed += len(batch)
                batch = []

        # Insert remaining records
        if batch:
            try:
                inserted = backend.insert_raw_records(batch)
                records_ingested += inserted
                chunks_processed += 1
            except Exception as e:
                logger.error(f"Failed to insert final batch: {e}")
                records_failed += len(batch)

        duration = time.time() - start_time_total

        return IngestionResult(
            success=True,
            records_ingested=records_ingested,
            records_failed=records_failed,
            start_date=start_date,
            end_date=end_date,
            duration_seconds=duration,
            chunks_processed=chunks_processed,
        )

    except ValueError as e:
        # Date range validation errors
        return IngestionResult(
            success=False,
            start_date=start_date,
            end_date=end_date,
            error=str(e),
            duration_seconds=time.time() - start_time_total,
        )

    except Exception as e:
        return IngestionResult(
            success=False,
            records_ingested=records_ingested,
            records_failed=records_failed,
            start_date=start_date,
            end_date=end_date,
            error=f"Ingestion failed: {e}",
            duration_seconds=time.time() - start_time_total,
            chunks_processed=chunks_processed,
        )

    finally:
        backend.close()


# =============================================================================
# Utility Functions
# =============================================================================


def check_log_retention(
    zone_id: Optional[str] = None, settings: Optional[Settings] = None
) -> dict:
    """
    Check if log retention is enabled for the zone.

    Args:
        zone_id: Cloudflare zone ID
        settings: Application settings

    Returns:
        Dictionary with retention status information
    """
    if settings is None:
        settings = get_settings()

    if zone_id is None:
        zone_id = settings.cloudflare_zone_id

    try:
        client = get_cloudflare_client(settings)

        # Try to get retention flag
        # Note: This endpoint may require Enterprise plan
        retention = client.logs.control.retention.flag.get(zone_id=zone_id)

        return {
            "zone_id": zone_id,
            "retention_enabled": getattr(retention, "flag", None),
            "error": None,
        }
    except Exception as e:
        return {
            "zone_id": zone_id,
            "retention_enabled": None,
            "error": str(e),
        }


def get_available_date_range() -> tuple[date, date]:
    """
    Get the available date range for log retrieval.

    Returns:
        Tuple of (earliest_date, latest_date) available for retrieval
    """
    now = datetime.now(timezone.utc)
    latest = now.date()
    earliest = (now - timedelta(days=RETENTION_DAYS)).date()
    return earliest, latest


def estimate_log_volume(
    start_date: date,
    end_date: date,
) -> dict:
    """
    Estimate the number of API calls needed for a date range.

    Useful for planning and progress reporting.

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        Dictionary with estimation details
    """
    start_time = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    end_time = datetime.combine(
        end_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
    )

    total_hours = (end_time - start_time).total_seconds() / 3600
    api_calls = int(total_hours / MAX_TIME_RANGE_HOURS) + 1
    estimated_time_minutes = api_calls / DEFAULT_REQUESTS_PER_MINUTE

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_hours": total_hours,
        "api_calls_needed": api_calls,
        "estimated_time_minutes": round(estimated_time_minutes, 1),
        "rate_limit_per_minute": DEFAULT_REQUESTS_PER_MINUTE,
    }
