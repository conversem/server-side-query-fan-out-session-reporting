#!/usr/bin/env python3
"""
Run query fan-out window optimization experiment.

Executes the full experiment protocol to determine the optimal
time window for bundling LLM bot requests into query fan-out sessions.

Usage:
    python scripts/run_window_experiment.py [options]

Examples:
    # Run with default settings (uses data/llm-bot-logs.db)
    python scripts/run_window_experiment.py

    # Custom database and windows
    python scripts/run_window_experiment.py \
        --db-path data/my-logs.db \
        --windows 100,500,1000,2000,3000,5000

    # Use sentence transformers for embeddings
    python scripts/run_window_experiment.py --embedding-method transformer
"""

import argparse
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.research.experiment_runner import (
    ExperimentConfig,
    ExperimentRunner,
)
from llm_bot_pipeline.research.window_optimizer import OptimizationWeights

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run query fan-out window optimization experiment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --db-path data/llm-bot-logs.db
  %(prog)s --windows 100,500,1000,3000,5000
  %(prog)s --embedding-method transformer --output-dir data/exp_transformer
        """,
    )

    # Data settings - SQLite source
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/llm-bot-logs.db",
        help="Path to SQLite database file",
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default="bot_requests_daily",
        help="Table to read from (default: bot_requests_daily)",
    )
    parser.add_argument(
        "--filter-category",
        type=str,
        default="user_request",
        help="Bot category to filter (default: user_request)",
    )
    parser.add_argument(
        "--exclude-providers",
        type=str,
        default="Microsoft",
        help="Comma-separated providers to exclude (default: Microsoft/bingbot)",
    )
    parser.add_argument(
        "--timestamp-col",
        type=str,
        default="request_timestamp",
        help="Name of timestamp column (default: request_timestamp)",
    )
    parser.add_argument(
        "--url-col",
        type=str,
        default="request_uri",
        help="Name of URL column (default: request_uri)",
    )
    parser.add_argument(
        "--group-by",
        type=str,
        default="bot_provider",
        help="Column to group by for per-provider analysis",
    )

    # Window settings
    parser.add_argument(
        "--windows",
        type=str,
        default="100,500,1000,3000,5000",
        help="Comma-separated candidate window sizes in ms",
    )

    # Optimization settings
    parser.add_argument(
        "--embedding-method",
        type=str,
        choices=["tfidf", "transformer"],
        default="tfidf",
        help="Embedding method for URL similarity",
    )
    parser.add_argument(
        "--purity-threshold",
        type=float,
        default=0.3,
        help="Minimum similarity threshold for bundle purity",
    )

    # Validation settings
    parser.add_argument(
        "--validation-split",
        type=float,
        default=0.2,
        help="Fraction of data for hold-out validation (0-1)",
    )

    # Weight overrides
    parser.add_argument(
        "--weight-mibcs",
        type=float,
        default=0.30,
        help="Weight for MIBCS in OptScore (alpha)",
    )
    parser.add_argument(
        "--weight-silhouette",
        type=float,
        default=0.25,
        help="Weight for silhouette score in OptScore (beta)",
    )
    parser.add_argument(
        "--weight-bps",
        type=float,
        default=0.25,
        help="Weight for bundle purity score in OptScore (gamma)",
    )
    parser.add_argument(
        "--weight-singleton",
        type=float,
        default=0.10,
        help="Penalty for singleton rate in OptScore (delta)",
    )
    parser.add_argument(
        "--weight-giant",
        type=float,
        default=0.05,
        help="Penalty for giant bundle rate in OptScore (epsilon)",
    )
    parser.add_argument(
        "--weight-variance",
        type=float,
        default=0.05,
        help="Penalty for thematic variance in OptScore (zeta)",
    )

    # Output settings
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/experiments",
        help="Directory for experiment output",
    )
    parser.add_argument(
        "--experiment-name",
        type=str,
        default=None,
        help="Name for this experiment run (auto-generated if not specified)",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save results to files",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress detailed output",
    )

    return parser.parse_args()


def main() -> int:
    """Run the experiment."""
    args = parse_args()

    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    # Parse candidate windows
    candidate_windows = [float(w) for w in args.windows.split(",")]

    # Parse excluded providers
    exclude_providers = (
        [p.strip() for p in args.exclude_providers.split(",")]
        if args.exclude_providers
        else []
    )

    # Build optimization weights
    weights = OptimizationWeights(
        alpha=args.weight_mibcs,
        beta=args.weight_silhouette,
        gamma=args.weight_bps,
        delta=args.weight_singleton,
        epsilon=args.weight_giant,
        zeta=args.weight_variance,
    )

    # Build experiment config
    config = ExperimentConfig(
        db_path=args.db_path,
        table_name=args.table_name,
        timestamp_col=args.timestamp_col,
        url_col=args.url_col,
        group_by=args.group_by,
        filter_category=args.filter_category,
        exclude_providers=exclude_providers,
        candidate_windows=candidate_windows,
        embedding_method=args.embedding_method,
        purity_threshold=args.purity_threshold,
        weights=weights,
        validation_split=args.validation_split,
        output_dir=args.output_dir,
        experiment_name=args.experiment_name,
    )

    # Check database file exists
    if not Path(config.db_path).exists():
        logger.error(f"Database not found: {config.db_path}")
        return 1

    # Run experiment
    logger.info("Starting query fan-out window optimization experiment")
    logger.info(f"Candidate windows: {candidate_windows}")

    runner = ExperimentRunner(config)

    try:
        result = runner.run()
    except Exception as e:
        logger.exception(f"Experiment failed: {e}")
        return 1

    # Print report
    if not args.quiet:
        runner.print_report()

    # Save results
    if not args.no_save:
        output_path = runner.save_results()
        print(f"\nResults saved to: {output_path}")

    # Print summary
    print("\n" + "=" * 50)
    print(" EXPERIMENT COMPLETE")
    print("=" * 50)
    rec = result.recommendation
    print(f"\nRecommended window: {rec['recommended_window_ms']:,.0f} ms")
    print(f"OptScore: {rec['opt_score']:.4f}")
    print(f"Confidence: {rec['confidence'].upper()}")
    print(f"Validation Agreement: {rec['validation_agreement']:.1%}")

    # Return based on confidence
    if rec["confidence"] == "high":
        print("\nResult: HIGH confidence recommendation ready for production use.")
        return 0
    elif rec["confidence"] == "medium":
        print("\nResult: MEDIUM confidence. Consider additional validation.")
        return 0
    else:
        print("\nResult: LOW confidence. Review data and consider manual analysis.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
