#!/usr/bin/env python3
"""
Exploratory Data Analysis for temporal patterns in LLM bot requests.

This script analyzes inter-request time deltas to understand query fan-out
patterns and identify candidate time windows for session bundling.

Usage:
    python scripts/analyze_temporal_patterns.py [--data-path PATH] [--output-dir DIR]

Output:
    - Delta distribution statistics per bot provider
    - Histogram plots of time deltas
    - Candidate window thresholds
    - Session statistics for different windows
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.research.temporal_analysis import (
    TemporalAnalyzer,
    compute_bundle_statistics,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Analyze temporal patterns in LLM bot requests"
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default="data/llm_bot_requests.csv",
        help="Path to CSV file with request data",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/temporal_analysis",
        help="Directory for output files",
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
        "--candidate-windows",
        type=str,
        default="100,500,1000,3000,5000",
        help="Comma-separated candidate window sizes in ms",
    )
    return parser.parse_args()


def print_delta_stats(stats: dict, title: str = "Delta Statistics") -> None:
    """Pretty print delta statistics."""
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print("=" * 60)

    for provider, delta_stats in stats.items():
        print(f"\n[{provider}]")
        print(f"  Count:      {delta_stats.count:,}")
        print(f"  Mean:       {delta_stats.mean_ms:,.2f} ms")
        print(f"  Median:     {delta_stats.median_ms:,.2f} ms")
        print(f"  Std Dev:    {delta_stats.std_ms:,.2f} ms")
        print(f"  Min:        {delta_stats.min_ms:,.2f} ms")
        print(f"  Max:        {delta_stats.max_ms:,.2f} ms")
        print("  Percentiles:")
        for pct, val in delta_stats.percentiles.items():
            print(f"    {pct}: {val:,.2f} ms")


def print_bundle_stats(stats: dict, window_ms: float) -> None:
    """Pretty print bundle statistics."""
    print(f"\n[Window: {window_ms:,.0f} ms]")
    print(f"  Total sessions:     {stats['total_bundles']:,}")
    print(f"  Total requests:     {stats['total_requests']:,}")
    print(f"  Mean session size:  {stats['mean_bundle_size']:.2f}")
    print(f"  Median session size: {stats['median_bundle_size']:.1f}")
    print(
        f"  Min/Max size:       {stats['min_bundle_size']} / {stats['max_bundle_size']}"
    )
    print(f"  Singleton rate:     {stats['singleton_rate']:.1%}")
    print(f"  Giant rate (>10):   {stats['giant_rate']:.1%}")
    print("  Size distribution:")
    for size, count in sorted(stats["size_distribution"].items()):
        print(f"    {size}: {count}")


def save_results(
    output_dir: Path,
    delta_stats: dict,
    candidate_windows: list[float],
    window_results: dict,
) -> None:
    """Save analysis results to files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save delta stats
    delta_data = {}
    for provider, stats in delta_stats.items():
        delta_data[provider] = {
            "count": stats.count,
            "mean_ms": stats.mean_ms,
            "median_ms": stats.median_ms,
            "std_ms": stats.std_ms,
            "min_ms": stats.min_ms,
            "max_ms": stats.max_ms,
            "percentiles": stats.percentiles,
        }

    with open(output_dir / "delta_statistics.json", "w") as f:
        json.dump(delta_data, f, indent=2)

    # Save candidate windows
    with open(output_dir / "candidate_windows.json", "w") as f:
        json.dump(
            {
                "windows_ms": candidate_windows,
                "generated_at": datetime.now().isoformat(),
            },
            f,
            indent=2,
        )

    # Save window comparison results
    with open(output_dir / "window_comparison.json", "w") as f:
        json.dump(window_results, f, indent=2)

    logger.info(f"Results saved to {output_dir}")


def main() -> None:
    """Run temporal pattern analysis."""
    args = parse_args()

    data_path = Path(args.data_path)
    output_dir = Path(args.output_dir)
    candidate_windows = [float(w) for w in args.candidate_windows.split(",")]

    if not data_path.exists():
        logger.error(f"Data file not found: {data_path}")
        sys.exit(1)

    # Load and preprocess data
    logger.info(f"Loading data from {data_path}")
    df = pd.read_csv(data_path)
    logger.info(f"Loaded {len(df):,} records")

    # Filter by category if specified
    if args.filter_category and "bot_category" in df.columns:
        original_count = len(df)
        df = df[df["bot_category"] == args.filter_category]
        logger.info(
            f"Filtered to {args.filter_category}: {len(df):,} records "
            f"({len(df)/original_count:.1%})"
        )

    # Exclude specific providers (e.g., bingbot is a crawler, not LLM chat)
    if args.exclude_providers and "bot_provider" in df.columns:
        exclude_list = [p.strip() for p in args.exclude_providers.split(",")]
        original_count = len(df)
        df = df[~df["bot_provider"].isin(exclude_list)]
        excluded = ", ".join(exclude_list)
        logger.info(
            f"Excluded providers [{excluded}]: {len(df):,} records "
            f"(removed {original_count - len(df):,})"
        )

    # Initialize analyzer
    analyzer = TemporalAnalyzer(
        timestamp_col="datetime",
        url_col="url",
        group_by="bot_provider",
    )
    analyzer.load_data(df)

    # Compute delta statistics
    logger.info("Computing inter-request delta statistics...")
    delta_stats = analyzer.get_delta_stats(by_provider=True)
    print_delta_stats(delta_stats, "Inter-Request Delta Statistics")

    # Find natural gaps
    logger.info("Finding natural gap thresholds...")
    natural_gaps = analyzer.find_candidate_windows(method="percentile")
    print("\n" + "=" * 60)
    print(" Natural Gap Thresholds (Percentile Method)")
    print("=" * 60)
    for i, gap in enumerate(natural_gaps):
        pct = ["p75", "p90", "p95", "p99"][i] if i < 4 else f"gap_{i}"
        print(f"  {pct}: {gap:,.2f} ms")

    # Evaluate candidate windows
    logger.info("Evaluating candidate time windows...")
    print("\n" + "=" * 60)
    print(" Session Statistics by Window Size")
    print("=" * 60)

    window_results = {}
    for window_ms in candidate_windows:
        bundles = analyzer.create_bundles(window_ms)
        stats = compute_bundle_statistics(bundles)
        window_results[str(int(window_ms))] = stats
        print_bundle_stats(stats, window_ms)

    # Save results
    save_results(output_dir, delta_stats, candidate_windows, window_results)

    # Summary recommendations
    print("\n" + "=" * 60)
    print(" Recommendations")
    print("=" * 60)
    print("\nBased on the analysis:")
    print("1. Review the percentile thresholds to identify natural session boundaries")
    print("2. Compare singleton rates - lower is better (avoid under-bundling)")
    print("3. Compare giant rates - lower is better (avoid over-bundling)")
    print("4. Run semantic similarity analysis to validate thematic coherence")
    print(f"\nDetailed results saved to: {output_dir}/")


if __name__ == "__main__":
    main()

