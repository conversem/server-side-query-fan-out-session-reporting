#!/usr/bin/env python3
"""
CLI script to run reporting aggregations.

Usage:
    # Run all aggregations for yesterday (SQLite)
    python scripts/run_aggregations.py --backend sqlite --daily

    # Run all aggregations for yesterday (SQLite)
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
        "--table",
        choices=["daily_summary", "url_performance", "all"],
        default="all",
        help="Which aggregation to run (default: all)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Delete existing data before inserting (default: append)",
    )

    # Info options
    parser.add_argument(
        "--freshness",
        action="store_true",
        help="Show data freshness stats and exit",
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

    args = parser.parse_args()

    # Validate backend-specific requirements
    if args.backend == "sqlite" and not args.project_id:
