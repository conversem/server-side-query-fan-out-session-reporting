"""
Universal adapter for standard CSV/JSON log formats.

Handles ingestion from any provider using standard CSV, JSON, or NDJSON
formats that match the universal schema. This adapter is provider-agnostic
and works with any log file that follows the universal schema field names.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from ....utils.bot_classifier import classify_bot
from ...base import IngestionAdapter, IngestionRecord, IngestionSource
from ...exceptions import ParseError, SourceValidationError
from ...parsers import (
    parse_csv_file,
    parse_json_file,
    parse_ndjson_file,
    parse_tsv_file,
)
from ...registry import IngestionRegistry
from ...security import validate_path_safe

logger = logging.getLogger(__name__)


@IngestionRegistry.register("universal")
class UniversalAdapter(IngestionAdapter):
    """
    Universal adapter for standard CSV/JSON log formats.

    Supports ingestion from any provider using standard CSV, JSON, or NDJSON
    formats that match the universal schema. This adapter is provider-agnostic
    and works with any log file that follows the universal schema field names.

    Supported source types:
        - csv_file: Comma-separated values with header
        - tsv_file: Tab-separated values with header
        - json_file: Single JSON object or array
        - ndjson_file: Newline-delimited JSON (JSON Lines)

    The adapter expects column/field names to match the universal schema
    field names directly (e.g., "timestamp", "client_ip", "method", etc.).
    Optional fields can be omitted.

    Example:
        source = IngestionSource(
            provider="universal",
            source_type="csv_file",
            path_or_uri="/path/to/logs.csv",
        )
        adapter = UniversalAdapter()
        for record in adapter.ingest(source, filter_bots=True):
            print(record)
    """

    # Cache for identity field mapping (computed once, reused)
    _identity_mapping: Optional[dict[str, str]] = None

    @property
    def provider_name(self) -> str:
        """Return the provider name identifier."""
        return "universal"

    @property
    def supported_source_types(self) -> list[str]:
        """Return list of supported source types."""
        return ["csv_file", "tsv_file", "json_file", "ndjson_file"]

    def ingest(
        self,
        source: IngestionSource,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_bots: bool = True,
        **kwargs,
    ) -> Iterator[IngestionRecord]:
        """
        Ingest logs from the source.

        Reads log data from CSV/JSON files and yields normalized IngestionRecord
        objects. Supports both single files and directories.

        Args:
            source: Ingestion source configuration
            start_time: Optional start time filter (UTC)
            end_time: Optional end time filter (UTC)
            filter_bots: If True, only yield records from known LLM bots
            **kwargs: Additional options:
                - strict_validation: If True, reject invalid records (default: False)

        Yields:
            IngestionRecord objects in universal format

        Raises:
            SourceValidationError: If source is invalid or inaccessible
            ParseError: If log data cannot be parsed
        """
        # Validate source first
        is_valid, error_msg = self.validate_source(source)
        if not is_valid:
            raise SourceValidationError(
                f"Source validation failed: {error_msg}",
                source_type=source.source_type,
                reason=error_msg,
            )

        path = Path(source.path_or_uri)
        strict_validation = kwargs.get("strict_validation", False)

        # Ensure timezone-aware datetimes for filtering
        if start_time is not None:
            start_time = self._ensure_utc(start_time)
        if end_time is not None:
            end_time = self._ensure_utc(end_time)

        # Validate time range
        if start_time is not None and end_time is not None:
            if start_time > end_time:
                raise ValueError(
                    f"Invalid time range: start_time ({start_time}) > end_time ({end_time})"
                )

        # Determine if source is a file or directory
        if path.is_file():
            yield from self._ingest_file(
                path,
                source.source_type,
                start_time,
                end_time,
                filter_bots,
                strict_validation,
            )
        elif path.is_dir():
            yield from self._ingest_directory(
                path,
                source.source_type,
                start_time,
                end_time,
                filter_bots,
                strict_validation,
            )
        else:
            raise SourceValidationError(
                f"Path does not exist or is not accessible: {path}",
                source_type=source.source_type,
                reason="Path not found",
            )

    def validate_source(
        self, source: IngestionSource, base_dir: Optional[Path] = None
    ) -> tuple[bool, str]:
        """
        Validate that the source is accessible and properly formatted.

        Performs pre-flight checks to ensure the source can be read
        before attempting full ingestion. Includes path traversal protection.

        Args:
            source: Ingestion source configuration
            base_dir: Optional base directory to constrain file access within

        Returns:
            Tuple of (is_valid, error_message)
            - is_valid: True if source passes validation
            - error_message: Empty string if valid, error details if invalid
        """
        # Check source type is supported
        if not self.supports_source_type(source.source_type):
            return (
                False,
                f"Unsupported source type: {source.source_type}. "
                f"Supported types: {', '.join(self.supported_source_types)}",
            )

        path = Path(source.path_or_uri)

        # Security: Validate path is safe from traversal attacks
        is_safe, security_error = validate_path_safe(
            path, base_dir=base_dir, allow_symlinks=True
        )
        if not is_safe:
            return (False, f"Security validation failed: {security_error}")

        # Check if path exists
        if not path.exists():
            return (False, f"Path does not exist: {path}")

        # Check if path is readable
        if path.is_file():
            try:
                # Check if file is readable before checking size
                if path.stat().st_size == 0:
                    return (False, f"File is empty: {path}")
            except (OSError, PermissionError) as e:
                return (False, f"Cannot access file {path}: {e}")
        elif path.is_dir():
            # Check if directory has any matching files
            matching_files = list(self._find_matching_files(path, source.source_type))
            if not matching_files:
                return (
                    False,
                    f"No matching files found in directory: {path}",
                )

        return (True, "")

    def _ingest_file(
        self,
        file_path: Path,
        source_type: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from a single file."""
        logger.info(f"Ingesting from file: {file_path}")

        # Get identity field mapping (column names match schema directly)
        field_mapping = self._get_identity_mapping()

        # Parse based on source type
        try:
            if source_type == "csv_file":
                records = parse_csv_file(
                    file_path, field_mapping, strict_validation=strict_validation
                )
            elif source_type == "tsv_file":
                records = parse_tsv_file(
                    file_path, field_mapping, strict_validation=strict_validation
                )
            elif source_type == "json_file":
                records = parse_json_file(
                    file_path, field_mapping, strict_validation=strict_validation
                )
            elif source_type == "ndjson_file":
                records = parse_ndjson_file(
                    file_path, field_mapping, strict_validation=strict_validation
                )
            else:
                raise SourceValidationError(
                    f"Unsupported source type: {source_type}",
                    source_type=source_type,
                )

            # Apply filters
            for record in records:
                # Time filtering (inclusive range)
                if start_time is not None and record.timestamp < start_time:
                    continue
                if end_time is not None and record.timestamp > end_time:
                    continue

                # Bot filtering
                if filter_bots:
                    bot_info = classify_bot(record.user_agent)
                    if bot_info is None:
                        continue  # Skip non-bot records

                yield record

        except (ParseError, SourceValidationError):
            # Re-raise parsing and validation errors as-is
            raise
        except Exception as e:
            # Wrap other exceptions (IOError, etc.) in ParseError
            raise ParseError(
                f"Failed to parse file {file_path}: {e}",
            ) from e

    def _ingest_directory(
        self,
        dir_path: Path,
        source_type: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        filter_bots: bool,
        strict_validation: bool,
    ) -> Iterator[IngestionRecord]:
        """Ingest records from all matching files in a directory."""
        logger.info(f"Ingesting from directory: {dir_path}")

        matching_files = list(self._find_matching_files(dir_path, source_type))
        logger.info(f"Found {len(matching_files)} matching files")

        for file_path in matching_files:
            try:
                yield from self._ingest_file(
                    file_path,
                    source_type,
                    start_time,
                    end_time,
                    filter_bots,
                    strict_validation,
                )
            except Exception as e:
                logger.warning(f"Failed to ingest {file_path}: {e}")
                if strict_validation:
                    raise
                continue

    def _find_matching_files(self, dir_path: Path, source_type: str) -> Iterator[Path]:
        """
        Find all files matching the source type in directory.

        Recursively searches for files with matching extensions, including
        gzip-compressed variants. Handles errors gracefully.

        Args:
            dir_path: Directory to search
            source_type: Source type to match extensions for

        Yields:
            Path objects for matching files

        Raises:
            PermissionError: If directory is not readable
        """
        # Map source types to file extensions (including gzip variants)
        extension_map = {
            "csv_file": [".csv", ".csv.gz"],
            "tsv_file": [".tsv", ".tsv.gz", ".txt", ".txt.gz"],
            "json_file": [".json", ".json.gz"],
            "ndjson_file": [".ndjson", ".ndjson.gz", ".jsonl", ".jsonl.gz"],
        }

        extensions = extension_map.get(source_type, [])
        if not extensions:
            return

        # Recursively find matching files
        seen = set()  # Track seen files to avoid duplicates
        try:
            for ext in extensions:
                for file_path in dir_path.rglob(f"*{ext}"):
                    # Use resolved path to avoid duplicates (handles symlinks)
                    # Handle case where file might be deleted between listing and resolving
                    try:
                        resolved_path = file_path.resolve()
                        if resolved_path not in seen:
                            seen.add(resolved_path)
                            yield file_path
                    except (OSError, RuntimeError):
                        # File deleted or symlink broken, skip it
                        logger.debug(f"Skipping inaccessible file: {file_path}")
                        continue
        except PermissionError:
            logger.error(f"Permission denied accessing directory: {dir_path}")
            raise
        except Exception as e:
            logger.warning(f"Error searching directory {dir_path}: {e}")
            raise

    def _get_identity_mapping(self) -> dict[str, str]:
        """
        Get identity field mapping (column names match schema directly).

        For universal format, we expect column/field names to match the
        universal schema field names directly, so this returns a mapping
        where each field maps to itself.

        The mapping is cached as a class variable after first creation for efficiency.

        Returns:
            Dictionary mapping schema field names to themselves
        """
        if UniversalAdapter._identity_mapping is None:
            from ...parsers import get_optional_field_names, get_required_field_names

            mapping = {}
            # Map all required and optional fields to themselves
            for field_name in get_required_field_names() + get_optional_field_names():
                mapping[field_name] = field_name
            UniversalAdapter._identity_mapping = mapping
        return UniversalAdapter._identity_mapping

    @staticmethod
    def _ensure_utc(dt: datetime) -> datetime:
        """
        Ensure datetime is timezone-aware and in UTC.

        Args:
            dt: Datetime object (may be naive or timezone-aware)

        Returns:
            Timezone-aware datetime in UTC

        Raises:
            ValueError: If datetime cannot be converted to UTC
        """
        if dt.tzinfo is None:
            # Assume naive datetime is UTC
            return dt.replace(tzinfo=timezone.utc)
        # Convert to UTC if in different timezone
        return dt.astimezone(timezone.utc)
