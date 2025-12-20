#!/usr/bin/env python3
"""
CLI script to run dashboard KPI queries.

Usage:
    # Run all KPIs (SQLite)
    python scripts/run_dashboard_queries.py --backend sqlite --all

    # Run all KPIs (SQLite)
    )

    parser.add_argument(
        "--db-path",
        type=Path,
        help="Path to SQLite database (default: data/llm-bot-logs.db)",
    )

    # Query selection
    parser.add_argument(
        "--query",
        choices=list(AVAILABLE_QUERIES.keys()),
        help="Specific query to run",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all KPI queries",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show executive summary only",
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
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days for analysis (default: 7)",
    )

    # Output options
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit for top-N queries (default: 20)",
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

    # Validate arguments
    if not (args.query or args.all or args.summary):
        parser.error("Must specify --query, --all, or --summary")

    if args.days < 1:
        parser.error("--days must be >= 1")

    if args.limit < 1:
        parser.error("--limit must be >= 1")

    # Validate backend-specific requirements
    if args.backend == "sqlite" and not args.project_id:
