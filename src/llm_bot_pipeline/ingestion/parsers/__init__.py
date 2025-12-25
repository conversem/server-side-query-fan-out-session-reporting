"""
Format parsers for multi-provider log ingestion.

Provides parsers for various log file formats:
- CSV/TSV with configurable delimiters
- JSON (single object or array)
- NDJSON (newline-delimited JSON / JSON Lines)
- W3C extended log format (AWS CloudFront, DigitalOcean)

Planned (not yet implemented):
- Apache Combined log format

All parsers yield IngestionRecord objects following the universal schema.

Usage:
    from llm_bot_pipeline.ingestion.parsers import (
        CSVParser,
        JSONParser,
        W3CParser,
        parse_csv_file,
        parse_json_file,
        parse_ndjson_file,
        parse_w3c_file,
    )

    # Parse a CSV file
    for record in parse_csv_file('/path/to/logs.csv', field_mapping):
        print(record.timestamp, record.client_ip)

    # Parse NDJSON with streaming
    for record in parse_ndjson_file('/path/to/logs.ndjson', field_mapping):
        process(record)
"""

from .csv_parser import CSVParser, parse_csv_file, parse_tsv_file
from .json_parser import JSONParser, parse_json_file, parse_ndjson_file
from .schema import (
    OPTIONAL_FIELDS,
    REQUIRED_FIELDS,
    UNIVERSAL_SCHEMA,
    FieldDefinition,
    FieldType,
    get_optional_field_names,
    get_required_field_names,
    validate_field,
    validate_http_method,
    validate_ip_address,
    validate_non_empty_string,
    validate_optional_string,
    validate_positive_integer,
    validate_record,
    validate_status_code,
    validate_timestamp,
)
from .w3c_parser import W3CParser, parse_w3c_file

__all__ = [
    # Schema
    "UNIVERSAL_SCHEMA",
    "REQUIRED_FIELDS",
    "OPTIONAL_FIELDS",
    "FieldDefinition",
    "FieldType",
    "validate_field",
    "validate_record",
    "get_required_field_names",
    "get_optional_field_names",
    # Validators
    "validate_timestamp",
    "validate_ip_address",
    "validate_http_method",
    "validate_status_code",
    "validate_positive_integer",
    "validate_non_empty_string",
    "validate_optional_string",
    # CSV Parser
    "CSVParser",
    "parse_csv_file",
    "parse_tsv_file",
    # JSON Parser
    "JSONParser",
    "parse_json_file",
    "parse_ndjson_file",
    # W3C Parser
    "W3CParser",
    "parse_w3c_file",
]
