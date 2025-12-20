#!/usr/bin/env python3
"""
CLI script to run the LLM bot traffic ETL pipeline.

Usage:
    # Run for yesterday with SQLite (local mode)
    python scripts/run_pipeline.py --backend sqlite --daily

    # Run for yesterday with SQLite (production mode)
    )

    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: data/llm-bot-logs.db)",
    )

    # Date range
    parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date (YYYY-MM-DD)",
    )

    # Mode options
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Run for yesterday only",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Run in backfill mode (process in batches)",
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=7,
        help="Days per batch in backfill mode (default: 7)",
    )

    # Processing options
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="full",
        help="Processing mode: 'incremental' (append) or 'full' (replace) [default: full]",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and preview without writing data",
    )
    parser.add_argument(
        "--skip-sessions",
        action="store_true",
        help="Skip session aggregation step after ETL",
    )

    # Optional
    parser.add_argument(
        "--credentials",
        type=str,
        default=None,
        help="Path to service account JSON key file (uses ADC if not provided)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show pipeline status and exit",
    )

    args = parser.parse_args()

    # Validate batch_days early
    if args.batch_days < 1:
        parser.error("--batch-days must be >= 1")

    # Validate backend-specific requirements
    if args.backend == "sqlite" and not args.project_id:
