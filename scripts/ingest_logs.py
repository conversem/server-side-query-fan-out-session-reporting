#!/usr/bin/env python3
"""
Enhanced CLI script for multi-provider log ingestion.

Supports ingestion from multiple CDN and cloud providers:
- Universal (CSV/JSON/NDJSON)
- AWS CloudFront (W3C format)
- AWS ALB (space-separated access logs)
- Azure CDN / Front Door (JSON logs)
- Cloudflare (API or Logpush files)
- Fastly (configurable JSON/CSV/NDJSON)
- Akamai DataStream (JSON/NDJSON)
- GCP Cloud CDN (JSON logs)

Usage:
    # Universal CSV
    python scripts/ingest_logs.py --provider universal --input data/logs.csv

    # AWS CloudFront (auto-detects W3C format)
    python scripts/ingest_logs.py --provider aws_cloudfront --input data/cloudfront-logs/

    # AWS ALB access logs
    python scripts/ingest_logs.py --provider aws_alb --input data/alb-logs/

    # Azure CDN / Front Door
    python scripts/ingest_logs.py --provider azure_cdn --input data/azure-logs.json

    # Cloudflare API (requires settings configuration)
    python scripts/ingest_logs.py --provider cloudflare --input api://zone_id

    # Cloudflare Logpush file
    python scripts/ingest_logs.py --provider cloudflare --input data/logpush.json

    # Fastly log streaming
    python scripts/ingest_logs.py --provider fastly --input data/fastly-logs.json

    # Akamai DataStream
    python scripts/ingest_logs.py --provider akamai --input data/datastream.json

    # GCP Cloud CDN
    python scripts/ingest_logs.py --provider gcp_cdn --input data/gcp-cdn.json

    # Auto-detect provider from file
    python scripts/ingest_logs.py --input data/logs.csv

    # List available providers
    python scripts/ingest_logs.py --list-providers

    # Validate without ingesting
    python scripts/ingest_logs.py --provider universal --input data/logs.csv --validate-only
"""

import argparse
import gzip
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.ingestion import (
    IngestionSource,
    ProviderNotFoundError,
    SourceValidationError,
    get_adapter,
    list_providers,
)
from llm_bot_pipeline.ingestion.exceptions import ParseError

# Import providers to ensure they're registered
from llm_bot_pipeline.ingestion.providers import (  # noqa: F401
    AkamaiAdapter,
    ALBAdapter,
    AzureCDNAdapter,
    CloudflareAdapter,
    CloudFrontAdapter,
    FastlyAdapter,
    GCPCDNAdapter,
    UniversalAdapter,
)
from llm_bot_pipeline.pipeline import setup_logging
from llm_bot_pipeline.storage import get_backend

logger = logging.getLogger(__name__)


def parse_datetime(datetime_str: str) -> datetime:
    """Parse datetime string in ISO 8601 or YYYY-MM-DD format."""
    try:
        # Try ISO 8601 format first
        return datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
    except ValueError:
        pass

    try:
        # Try date-only format (assume midnight UTC)
        dt = datetime.strptime(datetime_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid datetime format: {datetime_str}. Use ISO 8601 (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD"
        )


def parse_file_size(size_str: str) -> int:
    """
    Parse a file size string to bytes.

    Supports units: B, KB, MB, GB, TB (case-insensitive).

    Args:
        size_str: Size string like "10GB", "500MB", "1TB"

    Returns:
        Size in bytes

    Raises:
        argparse.ArgumentTypeError: If format is invalid
    """
    size_str = size_str.strip().upper()

    units = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 * 1024,
        "GB": 1024 * 1024 * 1024,
        "TB": 1024 * 1024 * 1024 * 1024,
    }

    # Try to parse with unit
    for unit, multiplier in sorted(units.items(), key=lambda x: -len(x[0])):
        if size_str.endswith(unit):
            try:
                value = float(size_str[: -len(unit)])
                return int(value * multiplier)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"Invalid file size format: {size_str}. "
                    f"Use format like '10GB', '500MB', '1TB'"
                )

    # Try to parse as plain number (bytes)
    try:
        return int(size_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid file size format: {size_str}. "
            f"Use format like '10GB', '500MB', '1TB'"
        )


def _detect_json_provider(data: dict) -> Optional[tuple[str, str]]:
    """
    Detect provider from parsed JSON object based on field patterns.

    Args:
        data: Parsed JSON object (single record or first record from array)

    Returns:
        Tuple of (provider_name, source_type) or None if cannot detect
    """
    keys = set(data.keys())
    keys_lower = {k.lower() for k in keys}

    # Akamai DataStream: camelCase fields like clientIP, requestMethod, responseStatus
    akamai_indicators = {"clientip", "requestmethod", "responsestatus", "requestpath"}
    if len(akamai_indicators & keys_lower) >= 3:
        return ("akamai", "akamai_json_file")

    # Azure CDN/Front Door: operationName, properties, or category fields
    if "operationName" in keys or "properties" in keys or "category" in keys:
        return ("azure_cdn", "json_file")

    # GCP Cloud CDN: httpRequest object or resource.type
    if "httpRequest" in keys:
        return ("gcp_cdn", "json_file")
    if "resource" in keys and isinstance(data.get("resource"), dict):
        if "type" in data["resource"]:
            return ("gcp_cdn", "json_file")

    # Cloudflare: EdgeStartTimestamp, CacheCacheStatus, ClientRequestURI
    cloudflare_indicators = {
        "edgestarttimestamp",
        "clientrequesturi",
        "clientrequesthost",
        "edgeresponsestatus",
    }
    if len(cloudflare_indicators & keys_lower) >= 2:
        return ("cloudflare", "ndjson_file")

    # Fastly: cache_status with pop/datacenter, or specific field combos
    fastly_indicators = {"client_ip", "cache_status", "pop", "datacenter"}
    if len(fastly_indicators & keys_lower) >= 2:
        return ("fastly", "fastly_json_file")

    # Universal schema fallback: timestamp + client_ip + status_code
    universal_indicators = {"timestamp", "client_ip", "status_code", "user_agent"}
    if len(universal_indicators & keys_lower) >= 3:
        return ("universal", "json_file")

    return None


def detect_provider_from_file(file_path: Path) -> Optional[tuple[str, str]]:
    """
    Auto-detect provider and source type from file.

    Detection priority:
    1. W3C format (CloudFront) - check for #Version: and #Fields: headers
    2. AWS ALB - space-separated logs starting with http/https type
    3. JSON/NDJSON - parse and detect by field patterns
    4. CSV - check for universal schema headers
    5. Extension-based fallback

    Args:
        file_path: Path to log file

    Returns:
        Tuple of (provider_name, source_type) or None if cannot detect
    """
    # Check file extension
    ext = file_path.suffix.lower()
    if ext == ".gz":
        # Get base extension
        ext = "." + file_path.stem.split(".")[-1].lower()

    # Try to read file header for format detection
    try:
        # Check for gzip
        is_gzip = file_path.suffix.lower() == ".gz"
        opener = gzip.open if is_gzip else open

        with opener(file_path, "rt", encoding="utf-8", errors="ignore") as f:
            # Read first few lines
            lines = []
            for i, line in enumerate(f):
                if i >= 10:  # Read up to 10 lines
                    break
                lines.append(line.strip())

            if not lines:
                return None

            first_line = lines[0]

            # Check for W3C format (CloudFront)
            if first_line.startswith("#Version:") and any(
                "#Fields:" in line for line in lines
            ):
                return ("aws_cloudfront", "w3c_file")

            # Check for AWS ALB access logs (space-separated, starts with type)
            # ALB logs start with: http, https, h2, grpcs, ws, wss
            alb_types = ("http ", "https ", "h2 ", "grpcs ", "ws ", "wss ")
            if any(first_line.startswith(t) for t in alb_types):
                # Verify it looks like ALB by checking field count
                parts = first_line.split(" ")
                if len(parts) >= 20:  # ALB has 29+ fields
                    return ("aws_alb", "alb_log_file")

            # Check for JSON/NDJSON
            if first_line.startswith("{"):
                try:
                    data = json.loads(first_line)
                    provider_result = _detect_json_provider(data)
                    if provider_result:
                        # It's NDJSON format
                        provider, _ = provider_result
                        if provider == "akamai":
                            return ("akamai", "akamai_ndjson_file")
                        elif provider == "fastly":
                            return ("fastly", "fastly_ndjson_file")
                        else:
                            return (provider, "ndjson_file")
                except json.JSONDecodeError:
                    pass

            elif first_line.startswith("["):
                # For JSON arrays, we need to read more content
                # Reopen and read enough to get first object
                try:
                    with opener(
                        file_path, "rt", encoding="utf-8", errors="ignore"
                    ) as f2:
                        # Read up to 64KB to find first complete object
                        content = f2.read(65536)
                        data = json.loads(content)
                        if isinstance(data, list) and len(data) > 0:
                            provider_result = _detect_json_provider(data[0])
                            if provider_result:
                                provider, _ = provider_result
                                if provider == "akamai":
                                    return ("akamai", "akamai_json_file")
                                elif provider == "fastly":
                                    return ("fastly", "fastly_json_file")
                                else:
                                    return (provider, "json_file")
                except json.JSONDecodeError:
                    pass

            # Check for CSV (has header row with commas)
            if "," in first_line and len(lines) > 1:
                header = first_line.lower()
                # Check for universal schema fields
                universal_fields = [
                    "timestamp",
                    "client_ip",
                    "method",
                    "host",
                    "path",
                    "status_code",
                    "user_agent",
                ]
                if sum(1 for f in universal_fields if f in header) >= 3:
                    return ("universal", "csv_file")

                # Check for Fastly CSV (may have different field names)
                fastly_csv_fields = [
                    "client_ip",
                    "cache_status",
                    "pop",
                    "response_time",
                ]
                if sum(1 for f in fastly_csv_fields if f in header) >= 2:
                    return ("fastly", "fastly_csv_file")

    except Exception as e:
        logger.debug(f"Error detecting provider from file {file_path}: {e}")

    # Fallback to extension-based detection
    if ext == ".csv":
        return ("universal", "csv_file")
    elif ext in [".json", ".jsonl", ".ndjson"]:
        # Default to universal for generic JSON
        return ("universal", "json_file")
    elif ext == ".log":
        # .log files could be ALB or W3C - prefer ALB for .log
        return ("aws_alb", "alb_log_file")
    elif ext == ".txt":
        return ("aws_cloudfront", "w3c_file")

    return None


def detect_provider_from_path(input_path: str) -> Optional[tuple[str, str]]:
    """
    Auto-detect provider from input path.

    Args:
        input_path: File path, directory path, or API URI

    Returns:
        Tuple of (provider_name, source_type) or None if cannot detect
    """
    # Check for API source
    if input_path.startswith("api://"):
        return ("cloudflare", "api")

    path = Path(input_path)

    if path.is_file():
        return detect_provider_from_file(path)
    elif path.is_dir():
        # Find first matching file in directory (limit search to avoid slowness)
        try:
            for ext in [".csv", ".json", ".ndjson", ".log", ".txt", ".gz"]:
                for file_path in path.rglob(f"*{ext}"):
                    if file_path.is_file():
                        result = detect_provider_from_file(file_path)
                        if result:
                            return result
                        break  # Use first file found
        except (PermissionError, OSError) as e:
            logger.debug(f"Error searching directory {path}: {e}")
            return None

    return None


def format_progress(current: int, total: Optional[int] = None) -> str:
    """Format progress indicator."""
    if total:
        percentage = (current / total) * 100 if total > 0 else 0
        return f"{current:,}/{total:,} ({percentage:.1f}%)"
    return f"{current:,}"


def convert_to_backend_record(record, source_provider: str = None) -> dict:
    """
    Convert IngestionRecord to backend storage format.

    Maps universal schema fields to Cloudflare-specific schema expected by backend.

    Args:
        record: IngestionRecord instance
        source_provider: Name of the provider that ingested this record

    Returns:
        Dictionary compatible with backend insert_raw_records()
    """
    # Convert timestamp to nanoseconds (Cloudflare format)
    timestamp_ns = None
    if record.timestamp:
        timestamp_ns = int(record.timestamp.timestamp() * 1_000_000_000)

    # Build URI from path and query_string
    # Handle edge cases: empty path defaults to "/"
    path = record.path if record.path else "/"
    uri = path
    if record.query_string:
        uri = f"{path}?{record.query_string}"

    return {
        "EdgeStartTimestamp": timestamp_ns,
        "ClientRequestURI": uri,
        "ClientRequestHost": record.host or "",
        "ClientRequestUserAgent": record.user_agent or "",
        "BotScore": None,  # Not available in universal schema
        "BotScoreSrc": None,
        "VerifiedBot": None,
        "BotTags": None,
        "ClientIP": record.client_ip or "",
        "ClientCountry": None,  # Not available in universal schema
        "EdgeResponseStatus": record.status_code or 0,
        "_ingestion_time": datetime.now(timezone.utc).isoformat(),
        "source_provider": source_provider,
    }


def ingest_records(
    adapter,
    source: IngestionSource,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    filter_bots: bool,
    validate_only: bool,
    db_path: Optional[Path] = None,
    batch_size: int = 1000,
) -> dict:
    """
    Ingest records using the adapter.

    Args:
        adapter: IngestionAdapter instance
        source: IngestionSource configuration
        start_time: Optional start time filter
        end_time: Optional end time filter
        filter_bots: If True, filter for LLM bots only
        validate_only: If True, validate without inserting
        db_path: Optional database path
        batch_size: Records per batch for insertion

    Returns:
        Dictionary with ingestion statistics
    """
    provider_name = adapter.provider_name
    logger.info(f"Ingesting from {source.provider} ({source.source_type})")

    results = {
        "records_processed": 0,
        "records_skipped": 0,
        "records_failed": 0,
        "files_processed": 0,
        "errors": [],
        "start_time": time.time(),
    }

    # Initialize backend if not validate-only
    backend = None
    if not validate_only:
        try:
            backend_kwargs = {}
            if db_path:
                backend_kwargs["db_path"] = db_path
            backend = get_backend("sqlite", **backend_kwargs)
            backend.initialize()
        except Exception as e:
            results["errors"].append(f"Failed to initialize backend: {e}")
            return results

    try:
        # Ingest records
        batch = []
        last_progress_time = time.time()

        for record in adapter.ingest(
            source, start_time=start_time, end_time=end_time, filter_bots=filter_bots
        ):
            results["records_processed"] += 1

            if validate_only:
                # Just count records for validation
                continue

            # Convert to backend format with source provider tracking
            backend_record = convert_to_backend_record(
                record, source_provider=provider_name
            )
            batch.append(backend_record)

            # Insert batch when full
            if len(batch) >= batch_size:
                try:
                    inserted = backend.insert_raw_records(batch)
                    results["records_skipped"] += len(batch) - inserted
                    batch = []
                except Exception as e:
                    logger.error(f"Failed to insert batch: {e}")
                    results["records_failed"] += len(batch)
                    results["errors"].append(f"Batch insertion failed: {e}")
                    batch = []

            # Progress reporting every 5 seconds
            if time.time() - last_progress_time >= 5:
                progress_msg = f"Processed {format_progress(results['records_processed'])} records..."
                logger.info(progress_msg)
                # Also print to stdout for better visibility
                print(f"  {progress_msg}", flush=True)
                last_progress_time = time.time()

        # Insert remaining records
        if batch and not validate_only:
            try:
                inserted = backend.insert_raw_records(batch)
                results["records_skipped"] += len(batch) - inserted
            except Exception as e:
                logger.error(f"Failed to insert final batch: {e}")
                results["records_failed"] += len(batch)
                results["errors"].append(f"Final batch insertion failed: {e}")

    except (ParseError, SourceValidationError) as e:
        results["errors"].append(str(e))
    except Exception as e:
        results["errors"].append(f"Unexpected error: {e}")
        logger.exception("Unexpected error during ingestion")

    finally:
        if backend:
            backend.close()

    results["duration_seconds"] = time.time() - results["start_time"]
    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Multi-provider log ingestion CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Universal CSV
  python scripts/ingest_logs.py --provider universal --input data/logs.csv

  # AWS CloudFront (W3C format)
  python scripts/ingest_logs.py --provider aws_cloudfront --input data/cloudfront-logs/

  # AWS ALB access logs
  python scripts/ingest_logs.py --provider aws_alb --input data/alb-logs/

  # Azure CDN / Front Door
  python scripts/ingest_logs.py --provider azure_cdn --input data/azure-logs.json

  # Cloudflare Logpush file
  python scripts/ingest_logs.py --provider cloudflare --input data/logpush.json

  # Fastly log streaming
  python scripts/ingest_logs.py --provider fastly --input data/fastly-logs.json

  # Akamai DataStream
  python scripts/ingest_logs.py --provider akamai --input data/datastream.json

  # GCP Cloud CDN
  python scripts/ingest_logs.py --provider gcp_cdn --input data/gcp-cdn.json

  # Auto-detect provider from file
  python scripts/ingest_logs.py --input data/logs.csv

  # List available providers
  python scripts/ingest_logs.py --list-providers

  # Validate without ingesting
  python scripts/ingest_logs.py --provider universal --input data/logs.csv --validate-only
        """,
    )

    parser.add_argument(
        "--provider",
        type=str,
        help="Provider name. Available: universal, aws_cloudfront, aws_alb, azure_cdn, "
        "cloudflare, fastly, akamai, gcp_cdn. Auto-detected if omitted.",
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        required=False,
        help="Input file, directory, or API URI (e.g., api://zone_id for Cloudflare)",
    )
    parser.add_argument(
        "--filter-bots",
        action="store_true",
        default=True,
        help="Filter for LLM bot traffic only (default: True)",
    )
    parser.add_argument(
        "--no-filter-bots",
        dest="filter_bots",
        action="store_false",
        help="Disable bot filtering (include all traffic)",
    )
    parser.add_argument(
        "--start-date",
        type=parse_datetime,
        help="Start time filter (ISO 8601 or YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=parse_datetime,
        help="End time filter (ISO 8601 or YYYY-MM-DD)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: from settings)",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        help="Base directory to restrict file access within (security: prevents path traversal)",
    )
    parser.add_argument(
        "--max-file-size",
        type=str,
        default="10GB",
        help="Maximum file size limit (default: 10GB). Supports units: B, KB, MB, GB, TB",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Records per batch for insertion (default: 1000)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate source without inserting data",
    )
    parser.add_argument(
        "--list-providers",
        action="store_true",
        help="List all available providers and exit",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    # List providers if requested
    if args.list_providers:
        providers = list_providers()
        print("Available providers:")
        for provider in providers:
            try:
                adapter = get_adapter(provider)
                source_types = ", ".join(adapter.supported_source_types)
                print(f"  {provider}: {source_types}")
            except Exception:
                print(f"  {provider}")
        return 0

    # Validate arguments
    if not args.input:
        parser.error("--input is required (unless using --list-providers)")

    # Validate batch size
    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than 0")

    # Validate time range
    if args.start_date and args.end_date:
        if args.start_date > args.end_date:
            parser.error(
                f"Invalid time range: start_date ({args.start_date}) > end_date ({args.end_date})"
            )

    # Parse max file size
    try:
        max_file_size = parse_file_size(args.max_file_size)
    except argparse.ArgumentTypeError as e:
        parser.error(str(e))

    # Auto-detect provider if not specified
    provider = args.provider
    source_type = None

    if not provider:
        logger.info("Auto-detecting provider from input...")
        try:
            detection_result = detect_provider_from_path(args.input)
            if detection_result:
                provider, source_type = detection_result
                logger.info(
                    f"Auto-detected provider: {provider}, source_type: {source_type}"
                )
            else:
                parser.error(
                    "Could not auto-detect provider. Please specify --provider explicitly.\n"
                    f"Available providers: {', '.join(list_providers())}"
                )
        except Exception as e:
            logger.debug(f"Error during auto-detection: {e}")
            parser.error(
                f"Failed to auto-detect provider: {e}. Please specify --provider explicitly."
            )

    # Get adapter early to validate provider and get supported source types
    try:
        adapter = get_adapter(provider)
    except ProviderNotFoundError as e:
        parser.error(
            f"Provider '{provider}' not found. Available providers: {', '.join(e.available_providers)}"
        )

    # Determine source type if not detected
    if not source_type:
        if args.input.startswith("api://"):
            source_type = "api"
        elif Path(args.input).is_file():
            # Infer from file extension
            ext = Path(args.input).suffix.lower()
            if ext == ".gz":
                ext = "." + Path(args.input).stem.split(".")[-1].lower()

            # Map extension to potential source types
            extension_source_types = {
                ".csv": ["csv_file", "fastly_csv_file"],
                ".json": [
                    "json_file",
                    "fastly_json_file",
                    "akamai_json_file",
                    "ndjson_file",
                ],
                ".jsonl": ["ndjson_file", "fastly_ndjson_file", "akamai_ndjson_file"],
                ".ndjson": ["ndjson_file", "fastly_ndjson_file", "akamai_ndjson_file"],
                ".log": ["alb_log_file", "w3c_file"],
                ".txt": ["w3c_file"],
            }

            potential_types = extension_source_types.get(ext, ["csv_file"])

            # Find first source type that provider supports
            source_type = None
            for st in potential_types:
                if st in adapter.supported_source_types:
                    source_type = st
                    break

            # If no matching type, use provider's default
            if not source_type:
                source_type = adapter.supported_source_types[0]
                logger.debug(
                    f"No matching source type for extension {ext}, "
                    f"using provider default: {source_type}"
                )
        else:
            # Directory - use provider's default
            source_type = adapter.supported_source_types[0]

    # Validate source type is supported by adapter
    if source_type not in adapter.supported_source_types:
        parser.error(
            f"Source type '{source_type}' not supported by provider '{provider}'. "
            f"Supported types: {', '.join(adapter.supported_source_types)}"
        )

    # Create source
    source = IngestionSource(
        provider=provider,
        source_type=source_type,
        path_or_uri=args.input,
    )

    # Print configuration
    print()
    print("📥 Multi-Provider Log Ingestion")
    print("=" * 50)
    print(f"  Provider: {provider}")
    print(f"  Source Type: {source_type}")
    print(f"  Input: {args.input}")
    if args.start_date:
        print(f"  Start Time: {args.start_date}")
    if args.end_date:
        print(f"  End Time: {args.end_date}")
    print(f"  Filter Bots: {args.filter_bots}")
    if args.base_dir:
        print(f"  Base Directory: {args.base_dir} (security restriction)")
    print(f"  Max File Size: {args.max_file_size}")
    if args.validate_only:
        print("  ⚠️  VALIDATE ONLY - no data will be written")
    print()

    # Validate file size for file sources (before source validation)
    input_path = Path(args.input)
    if input_path.is_file():
        try:
            file_size = input_path.stat().st_size
            if file_size > max_file_size:
                from llm_bot_pipeline.ingestion import format_file_size

                print(
                    f"❌ File size ({format_file_size(file_size)}) exceeds maximum limit "
                    f"({format_file_size(max_file_size)})"
                )
                print("   Increase --max-file-size or use a smaller file.")
                return 1
        except (OSError, PermissionError) as e:
            print(f"❌ Cannot access file: {e}")
            return 1

    # Validate source (including path traversal protection)
    try:
        is_valid, error_msg = adapter.validate_source(source, base_dir=args.base_dir)
        if not is_valid:
            print(f"❌ Source validation failed: {error_msg}")
            return 1
    except Exception as e:
        print(f"❌ Source validation error: {e}")
        return 1

    # Ingest records
    try:
        results = ingest_records(
            adapter=adapter,
            source=source,
            start_time=args.start_date,
            end_time=args.end_date,
            filter_bots=args.filter_bots,
            validate_only=args.validate_only,
            db_path=args.db_path,
            batch_size=args.batch_size,
        )

        # Print summary
        print()
        print("📊 Ingestion Summary")
        print("=" * 50)
        print(f"  Records Processed: {results['records_processed']:,}")
        if not args.validate_only:
            records_inserted = (
                results["records_processed"]
                - results["records_skipped"]
                - results["records_failed"]
            )
            print(f"  Records Inserted: {records_inserted:,}")
            if results["records_skipped"] > 0:
                print(f"  Records Skipped: {results['records_skipped']:,}")
            if results["records_failed"] > 0:
                print(f"  Records Failed: {results['records_failed']:,}")
        print(f"  Duration: {results['duration_seconds']:.1f}s")
        if results["records_processed"] > 0 and results["duration_seconds"] > 0:
            throughput = results["records_processed"] / results["duration_seconds"]
            print(f"  Throughput: {throughput:.0f} records/sec")

        if results["errors"]:
            print()
            print("❌ Errors:")
            for error in results["errors"]:
                print(f"  - {error}")
            return 1

        return 0

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
