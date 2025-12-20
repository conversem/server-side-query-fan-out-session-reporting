"""
Raw table schema for Cloudflare log data.

This schema matches the fields selected in the log ingestion configuration.
"""

from google.cloud import database

RAW_SCHEMA = [
    database.SchemaField("EdgeStartTimestamp", "TIMESTAMP", mode="REQUIRED"),
    database.SchemaField("ClientRequestURI", "STRING", mode="NULLABLE"),
    database.SchemaField("ClientRequestHost", "STRING", mode="NULLABLE"),
    database.SchemaField("ClientRequestUserAgent", "STRING", mode="NULLABLE"),
    database.SchemaField("BotScore", "INTEGER", mode="NULLABLE"),
    database.SchemaField("BotScoreSrc", "STRING", mode="NULLABLE"),
    database.SchemaField("VerifiedBot", "BOOLEAN", mode="NULLABLE"),
    database.SchemaField("BotTags", "STRING", mode="REPEATED"),
    database.SchemaField("ClientIP", "STRING", mode="NULLABLE"),
    database.SchemaField("ClientCountry", "STRING", mode="NULLABLE"),
    database.SchemaField("EdgeResponseStatus", "INTEGER", mode="NULLABLE"),
    # Ingestion metadata - added by Database streaming insert
    database.SchemaField("_ingestion_time", "TIMESTAMP", mode="NULLABLE"),
]

