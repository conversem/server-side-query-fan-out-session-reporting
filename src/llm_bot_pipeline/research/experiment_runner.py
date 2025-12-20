"""
Experiment runner for query fan-out window optimization.

Orchestrates the full experiment workflow:
1. Data loading and preparation
2. Candidate window evaluation
3. Statistical comparison
4. Result reporting and persistence
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .semantic_embeddings import URLEmbedder
from .temporal_analysis import TemporalAnalyzer, compute_bundle_statistics
from .window_optimizer import (
    OptimizationMetrics,
    OptimizationWeights,
    WindowOptimizer,
)

logger = logging.getLogger(__name__)

# Try to import scipy for statistical tests
try:
    from scipy import stats as scipy_stats

    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logger.debug("scipy not available for statistical tests")


@dataclass
class ExperimentConfig:
    """Configuration for a window optimization experiment."""

    # Data settings
    data_path: str
    timestamp_col: str = "datetime"
    url_col: str = "url"
    group_by: str = "bot_provider"
    filter_category: Optional[str] = "user_request"
    exclude_providers: list[str] = field(
        default_factory=lambda: ["Microsoft"]  # Exclude bingbot by default
    )

    # Candidate windows (in milliseconds)
    candidate_windows: list[float] = field(
        default_factory=lambda: [100, 500, 1000, 3000, 5000]
    )

    # Optimization settings
    embedding_method: str = "tfidf"
    purity_threshold: float = 0.3
    weights: OptimizationWeights = field(default_factory=OptimizationWeights)

    # Validation settings
    validation_split: float = 0.2  # Fraction for hold-out validation
    significance_level: float = 0.05  # p-value threshold

    # Output settings
    output_dir: str = "data/experiments"
    experiment_name: Optional[str] = None


@dataclass
class StatisticalComparison:
    """Result of statistical comparison between windows."""

    window_a_ms: float
    window_b_ms: float
    metric: str
    value_a: float
    value_b: float
    difference: float
    t_statistic: Optional[float]
    p_value: Optional[float]
    significant: bool


@dataclass
class ExperimentResult:
    """Complete result of a window optimization experiment."""

    config: ExperimentConfig
    timestamp: str

    # Data summary
    total_records: int
    filtered_records: int
    train_records: int
    validation_records: int

    # Window results
    window_metrics: dict[float, OptimizationMetrics]
    best_window_ms: float
    best_opt_score: float

    # Statistical comparisons
    statistical_tests: list[StatisticalComparison]

    # Validation results
    validation_metrics: Optional[dict[float, OptimizationMetrics]] = None
    validation_agreement: Optional[float] = None

    # Recommendation
    recommendation: Optional[dict] = None


class ExperimentRunner:
    """
    Run window optimization experiments with full protocol.

    Implements the experiment procedure from the PRD:
    1. Data preparation (load, filter, split)
    2. Window evaluation on training data
    3. Statistical comparison between windows
    4. Validation on hold-out data
    5. Result reporting
    """

    def __init__(self, config: ExperimentConfig):
        """
        Initialize experiment runner.

        Args:
            config: Experiment configuration
        """
        self.config = config
        self.analyzer: Optional[TemporalAnalyzer] = None
        self.optimizer: Optional[WindowOptimizer] = None
        self._train_df: Optional[pd.DataFrame] = None
        self._val_df: Optional[pd.DataFrame] = None
        self._result: Optional[ExperimentResult] = None

    def load_data(self) -> pd.DataFrame:
        """
        Load and preprocess experiment data.

        Returns:
            Preprocessed DataFrame
        """
        logger.info(f"Loading data from {self.config.data_path}")
        df = pd.read_csv(self.config.data_path)
        logger.info(f"Loaded {len(df):,} records")

        # Filter by category if specified
        if self.config.filter_category and "bot_category" in df.columns:
            original_count = len(df)
            df = df[df["bot_category"] == self.config.filter_category]
            logger.info(
                f"Filtered to {self.config.filter_category}: {len(df):,} records "
                f"({len(df)/original_count:.1%})"
            )

        # Exclude specific providers (e.g., bingbot is a crawler, not LLM chat)
        if self.config.exclude_providers and self.config.group_by in df.columns:
            original_count = len(df)
            df = df[~df[self.config.group_by].isin(self.config.exclude_providers)]
            excluded = ", ".join(self.config.exclude_providers)
            logger.info(
                f"Excluded providers [{excluded}]: {len(df):,} records "
                f"(removed {original_count - len(df):,})"
            )

        return df

    def split_data(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split data into training and validation sets.

        Uses temporal split (earlier data for training, later for validation).

        Args:
            df: Full dataset

        Returns:
            Tuple of (train_df, validation_df)
        """
        # Ensure timestamp is datetime
        ts_col = self.config.timestamp_col
        if df[ts_col].dtype == "object":
            df[ts_col] = pd.to_datetime(df[ts_col], format="ISO8601")

        # Sort by timestamp
        df = df.sort_values(ts_col).reset_index(drop=True)

        # Split by time
        split_idx = int(len(df) * (1 - self.config.validation_split))
        train_df = df.iloc[:split_idx].copy()
        val_df = df.iloc[split_idx:].copy()

        logger.info(f"Split data: {len(train_df):,} train, {len(val_df):,} validation")

        return train_df, val_df

    def evaluate_windows(
        self,
        df: pd.DataFrame,
        windows: list[float],
    ) -> dict[float, OptimizationMetrics]:
        """
        Evaluate multiple candidate windows.

        Args:
            df: DataFrame with request data
            windows: List of window sizes in milliseconds

        Returns:
            Dictionary mapping window_ms to OptimizationMetrics
        """
        # Initialize analyzer
        analyzer = TemporalAnalyzer(
            timestamp_col=self.config.timestamp_col,
            url_col=self.config.url_col,
            group_by=self.config.group_by,
        )
        analyzer.load_data(df)

        # Initialize optimizer
        embedder = URLEmbedder(method=self.config.embedding_method)
        optimizer = WindowOptimizer(
            embedder=embedder,
            weights=self.config.weights,
            purity_threshold=self.config.purity_threshold,
        )

        results = {}
        for window_ms in windows:
            logger.info(f"Evaluating window: {window_ms:,.0f} ms")

            # Create bundles
            bundles = analyzer.create_bundles(window_ms)
            bundle_stats = compute_bundle_statistics(bundles)

            logger.info(
                f"  Created {bundle_stats['total_bundles']:,} sessions "
                f"(mean size: {bundle_stats['mean_bundle_size']:.1f})"
            )

            # Evaluate with optimizer
            metrics = optimizer.evaluate_window(
                bundles,
                window_ms,
                include_bundle_metrics=False,
            )
            results[window_ms] = metrics

            logger.info(
                f"  OptScore: {metrics.opt_score:.4f}, "
                f"MIBCS: {metrics.mibcs:.4f}, "
                f"BPS: {metrics.bundle_purity_score:.4f}"
            )

        self.analyzer = analyzer
        self.optimizer = optimizer
        return results

    def run_statistical_tests(
        self,
        window_metrics: dict[float, OptimizationMetrics],
    ) -> list[StatisticalComparison]:
        """
        Run statistical tests comparing adjacent windows.

        Args:
            window_metrics: Dictionary of window metrics

        Returns:
            List of StatisticalComparison results
        """
        if not SCIPY_AVAILABLE:
            logger.warning("scipy not available, skipping statistical tests")
            return []

        comparisons = []
        sorted_windows = sorted(window_metrics.keys())

        for i in range(len(sorted_windows) - 1):
            window_a = sorted_windows[i]
            window_b = sorted_windows[i + 1]
            metrics_a = window_metrics[window_a]
            metrics_b = window_metrics[window_b]

            # Compare OptScores
            # Note: For proper paired t-test, we'd need per-bundle scores
            # Here we use a simple comparison since we have aggregate metrics
            diff = metrics_b.opt_score - metrics_a.opt_score

            comparison = StatisticalComparison(
                window_a_ms=window_a,
                window_b_ms=window_b,
                metric="opt_score",
                value_a=metrics_a.opt_score,
                value_b=metrics_b.opt_score,
                difference=diff,
                t_statistic=None,  # Would need per-bundle data
                p_value=None,
                significant=abs(diff) > 0.05,  # Heuristic threshold
            )
            comparisons.append(comparison)

        return comparisons

    def validate_results(
        self,
        train_metrics: dict[float, OptimizationMetrics],
        val_metrics: dict[float, OptimizationMetrics],
    ) -> float:
        """
        Validate results by comparing train and validation metrics.

        Args:
            train_metrics: Metrics from training data
            val_metrics: Metrics from validation data

        Returns:
            Agreement score (0-1, higher is better)
        """
        if not train_metrics or not val_metrics:
            return 0.0

        # Find best window in each set
        train_best = max(train_metrics.items(), key=lambda x: x[1].opt_score)
        val_best = max(val_metrics.items(), key=lambda x: x[1].opt_score)

        # Check if same window is best
        same_best = train_best[0] == val_best[0]

        # Compare OptScore correlation
        common_windows = set(train_metrics.keys()) & set(val_metrics.keys())
        if len(common_windows) >= 2:
            train_scores = [train_metrics[w].opt_score for w in sorted(common_windows)]
            val_scores = [val_metrics[w].opt_score for w in sorted(common_windows)]

            # Rank correlation
            train_ranks = np.argsort(np.argsort(train_scores)[::-1])
            val_ranks = np.argsort(np.argsort(val_scores)[::-1])

            # Compute rank agreement
            rank_diff = np.abs(train_ranks - val_ranks)
            max_possible_diff = len(common_windows) - 1
            rank_agreement = 1 - (np.mean(rank_diff) / max_possible_diff)
        else:
            rank_agreement = 0.5

        # Combine metrics
        agreement = 0.5 * (1.0 if same_best else 0.0) + 0.5 * rank_agreement

        return agreement

    def run(self) -> ExperimentResult:
        """
        Run the complete experiment.

        Returns:
            ExperimentResult with all findings
        """
        timestamp = datetime.now().isoformat()
        logger.info(f"Starting experiment at {timestamp}")

        # Load and split data
        df = self.load_data()
        train_df, val_df = self.split_data(df)
        self._train_df = train_df
        self._val_df = val_df

        # Evaluate windows on training data
        logger.info("Evaluating windows on training data...")
        train_metrics = self.evaluate_windows(
            train_df,
            self.config.candidate_windows,
        )

        # Find best window
        best_window, best_metrics = max(
            train_metrics.items(),
            key=lambda x: x[1].opt_score,
        )

        # Run statistical tests
        logger.info("Running statistical comparisons...")
        stat_tests = self.run_statistical_tests(train_metrics)

        # Validate on hold-out data
        logger.info("Validating on hold-out data...")
        val_metrics = self.evaluate_windows(
            val_df,
            self.config.candidate_windows,
        )
        validation_agreement = self.validate_results(train_metrics, val_metrics)
        logger.info(f"Validation agreement: {validation_agreement:.1%}")

        # Build recommendation
        recommendation = {
            "recommended_window_ms": best_window,
            "opt_score": best_metrics.opt_score,
            "mibcs": best_metrics.mibcs,
            "bundle_purity_score": best_metrics.bundle_purity_score,
            "silhouette_score": best_metrics.silhouette_score,
            "validation_agreement": validation_agreement,
            "confidence": (
                "high"
                if validation_agreement > 0.8
                else "medium" if validation_agreement > 0.6 else "low"
            ),
        }

        # Build result
        result = ExperimentResult(
            config=self.config,
            timestamp=timestamp,
            total_records=len(df),
            filtered_records=len(train_df) + len(val_df),
            train_records=len(train_df),
            validation_records=len(val_df),
            window_metrics=train_metrics,
            best_window_ms=best_window,
            best_opt_score=best_metrics.opt_score,
            statistical_tests=stat_tests,
            validation_metrics=val_metrics,
            validation_agreement=validation_agreement,
            recommendation=recommendation,
        )

        self._result = result
        return result

    def save_results(self, output_dir: Optional[Path] = None) -> Path:
        """
        Save experiment results to files.

        Args:
            output_dir: Output directory (uses config default if not specified)

        Returns:
            Path to output directory
        """
        if self._result is None:
            raise ValueError("No results to save. Run experiment first.")

        output_dir = Path(output_dir or self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate experiment ID
        exp_name = self.config.experiment_name or datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )
        exp_dir = output_dir / exp_name
        exp_dir.mkdir(parents=True, exist_ok=True)

        # Save configuration
        config_data = {
            "data_path": self.config.data_path,
            "timestamp_col": self.config.timestamp_col,
            "url_col": self.config.url_col,
            "group_by": self.config.group_by,
            "filter_category": self.config.filter_category,
            "candidate_windows": self.config.candidate_windows,
            "embedding_method": self.config.embedding_method,
            "purity_threshold": self.config.purity_threshold,
            "validation_split": self.config.validation_split,
            "significance_level": self.config.significance_level,
        }
        with open(exp_dir / "config.json", "w") as f:
            json.dump(config_data, f, indent=2)

        # Save window metrics
        metrics_data = {}
        for window_ms, metrics in self._result.window_metrics.items():
            metrics_data[str(int(window_ms))] = {
                "opt_score": metrics.opt_score,
                "mibcs": metrics.mibcs,
                "silhouette_score": metrics.silhouette_score,
                "bundle_purity_score": metrics.bundle_purity_score,
                "singleton_rate": metrics.singleton_rate,
                "giant_rate": metrics.giant_rate,
                "thematic_variance": metrics.thematic_variance,
                "total_bundles": metrics.total_bundles,
                "total_requests": metrics.total_requests,
                "mean_bundle_size": metrics.mean_bundle_size,
            }
        with open(exp_dir / "window_metrics.json", "w") as f:
            json.dump(metrics_data, f, indent=2)

        # Save recommendation
        with open(exp_dir / "recommendation.json", "w") as f:
            json.dump(self._result.recommendation, f, indent=2)

        # Save summary
        summary = {
            "timestamp": self._result.timestamp,
            "total_records": self._result.total_records,
            "filtered_records": self._result.filtered_records,
            "train_records": self._result.train_records,
            "validation_records": self._result.validation_records,
            "best_window_ms": self._result.best_window_ms,
            "best_opt_score": self._result.best_opt_score,
            "validation_agreement": self._result.validation_agreement,
        }
        with open(exp_dir / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Results saved to {exp_dir}")
        return exp_dir

    def print_report(self) -> None:
        """Print formatted experiment report."""
        if self._result is None:
            print("No results available. Run experiment first.")
            return

        r = self._result

        print("\n" + "=" * 70)
        print(" QUERY FAN-OUT WINDOW OPTIMIZATION EXPERIMENT REPORT")
        print("=" * 70)

        print(f"\nExperiment Time: {r.timestamp}")
        print(f"Data Source: {self.config.data_path}")

        print("\n--- DATA SUMMARY ---")
        print(f"Total Records:      {r.total_records:,}")
        print(f"Filtered Records:   {r.filtered_records:,}")
        print(f"Training Set:       {r.train_records:,}")
        print(f"Validation Set:     {r.validation_records:,}")

        print("\n--- WINDOW EVALUATION RESULTS ---")
        print(
            f"{'Window (ms)':<12} {'OptScore':<10} {'MIBCS':<8} {'BPS':<8} {'Silh.':<8} {'SR':<8} {'GR':<8}"
        )
        print("-" * 70)

        for window_ms in sorted(r.window_metrics.keys()):
            m = r.window_metrics[window_ms]
            silh = f"{m.silhouette_score:.4f}" if m.silhouette_score else "N/A"
            best_marker = " *" if window_ms == r.best_window_ms else ""
            print(
                f"{window_ms:<12,.0f} {m.opt_score:<10.4f} {m.mibcs:<8.4f} "
                f"{m.bundle_purity_score:<8.4f} {silh:<8} {m.singleton_rate:<8.2%} "
                f"{m.giant_rate:<8.2%}{best_marker}"
            )

        print("\n--- RECOMMENDATION ---")
        rec = r.recommendation
        print(f"Recommended Window:  {rec['recommended_window_ms']:,.0f} ms")
        print(f"OptScore:           {rec['opt_score']:.4f}")
        print(f"MIBCS:              {rec['mibcs']:.4f}")
        print(f"Bundle Purity:      {rec['bundle_purity_score']:.4f}")
        print(f"Validation Agreement: {rec['validation_agreement']:.1%}")
        print(f"Confidence:         {rec['confidence'].upper()}")

        print("\n" + "=" * 70)
