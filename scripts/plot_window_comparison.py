#!/usr/bin/env python3
"""
Generate scatter plot comparing session bundle sizes across different time windows.

Creates visualization showing how bundle size distribution changes with window size.
"""

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def load_window_metrics(metrics_path: str) -> dict:
    """Load window metrics from JSON file."""
    with open(metrics_path) as f:
        return json.load(f)


def create_scatter_plot(metrics: dict, output_path: str) -> None:
    """Create scatter plot of window size vs key metrics."""
    windows = sorted([int(w) for w in metrics.keys()])

    # Extract metrics
    opt_scores = [metrics[str(w)]["opt_score"] for w in windows]
    mibcs = [metrics[str(w)]["mibcs"] for w in windows]
    bps = [metrics[str(w)]["bundle_purity_score"] for w in windows]
    mean_sizes = [metrics[str(w)]["mean_bundle_size"] for w in windows]
    total_bundles = [metrics[str(w)]["total_bundles"] for w in windows]
    singleton_rates = [metrics[str(w)]["singleton_rate"] * 100 for w in windows]
    giant_rates = [metrics[str(w)]["giant_rate"] * 100 for w in windows]

    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(
        "Query Fan-Out Session Analysis: Window Size Comparison",
        fontsize=14,
        fontweight="bold",
    )

    # Plot 1: Bundle Size vs Window (scatter with trend)
    ax1 = axes[0, 0]
    scatter1 = ax1.scatter(
        windows,
        mean_sizes,
        c=opt_scores,
        cmap="RdYlGn",
        s=200,
        edgecolors="black",
        linewidths=1.5,
    )
    ax1.plot(windows, mean_sizes, "k--", alpha=0.5, linewidth=1)
    ax1.set_xlabel("Time Window (ms)", fontsize=11)
    ax1.set_ylabel("Mean Session Size (requests)", fontsize=11)
    ax1.set_title("Session Size Growth with Window Width", fontsize=12)
    ax1.set_xscale("log")
    ax1.set_xticks(windows)
    ax1.set_xticklabels([str(w) for w in windows])
    cbar1 = plt.colorbar(scatter1, ax=ax1)
    cbar1.set_label("OptScore", fontsize=10)

    # Add annotations
    for i, w in enumerate(windows):
        ax1.annotate(
            f"{mean_sizes[i]:.2f}",
            (w, mean_sizes[i]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=9,
        )

    # Plot 2: Coherence Metrics vs Window
    ax2 = axes[0, 1]
    ax2.scatter(
        windows,
        mibcs,
        c="green",
        s=150,
        label="MIBCS (Coherence)",
        marker="o",
        edgecolors="black",
    )
    ax2.scatter(
        windows,
        bps,
        c="blue",
        s=150,
        label="Bundle Purity Score",
        marker="s",
        edgecolors="black",
    )
    ax2.plot(windows, mibcs, "g--", alpha=0.5)
    ax2.plot(windows, bps, "b--", alpha=0.5)
    ax2.set_xlabel("Time Window (ms)", fontsize=11)
    ax2.set_ylabel("Score (0-1)", fontsize=11)
    ax2.set_title("Semantic Coherence Degradation", fontsize=12)
    ax2.set_xscale("log")
    ax2.set_xticks(windows)
    ax2.set_xticklabels([str(w) for w in windows])
    ax2.set_ylim(0.6, 1.0)
    ax2.legend(loc="lower left")
    ax2.axhline(
        y=0.7, color="red", linestyle=":", alpha=0.7, label="High confidence threshold"
    )

    # Plot 3: Number of Sessions vs Window
    ax3 = axes[1, 0]
    bars = ax3.bar(
        range(len(windows)),
        total_bundles,
        color=plt.cm.viridis(np.linspace(0.2, 0.8, len(windows))),
    )
    ax3.set_xlabel("Time Window (ms)", fontsize=11)
    ax3.set_ylabel("Total Sessions Created", fontsize=11)
    ax3.set_title("Session Count by Window Size", fontsize=12)
    ax3.set_xticks(range(len(windows)))
    ax3.set_xticklabels([str(w) for w in windows])

    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, total_bundles)):
        ax3.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 500,
            f"{val:,}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    # Plot 4: Singleton vs Giant Rate
    ax4 = axes[1, 1]
    x_pos = np.arange(len(windows))
    width = 0.35
    bars1 = ax4.bar(
        x_pos - width / 2,
        singleton_rates,
        width,
        label="Singleton Rate (%)",
        color="orange",
        edgecolor="black",
    )
    bars2 = ax4.bar(
        x_pos + width / 2,
        giant_rates,
        width,
        label="Giant Rate (>10 req) (%)",
        color="red",
        edgecolor="black",
    )
    ax4.set_xlabel("Time Window (ms)", fontsize=11)
    ax4.set_ylabel("Rate (%)", fontsize=11)
    ax4.set_title("Under-bundling vs Over-bundling Indicators", fontsize=12)
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels([str(w) for w in windows])
    ax4.legend(loc="upper right")

    # Highlight optimal window
    for ax in axes.flat:
        (
            ax.axvline(
                x=0 if ax in [axes[1, 0], axes[1, 1]] else 100,
                color="green",
                linestyle="-",
                alpha=0.3,
                linewidth=8,
            )
            if ax in [axes[0, 0], axes[0, 1]]
            else None
        )

    # Add optimal marker
    axes[0, 0].annotate(
        "OPTIMAL",
        xy=(100, mean_sizes[0]),
        xytext=(200, mean_sizes[0] + 0.3),
        arrowprops=dict(arrowstyle="->", color="green"),
        fontsize=10,
        color="green",
        fontweight="bold",
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.savefig(output_path.replace(".png", ".svg"), format="svg", bbox_inches="tight")
    print(f"Saved plot to {output_path}")
    print(f"Saved SVG to {output_path.replace('.png', '.svg')}")


def create_size_distribution_plot(output_path: str) -> None:
    """Create a simple scatter plot showing size vs window relationship."""
    # Data from experiment (December 2025 with extended windows 50-500ms)
    # 88,802 user_request records (Dec 10-17, 2025)
    windows = [50, 100, 200, 300, 400, 500, 1000, 2000, 3000, 5000]
    mean_sizes = [2.24, 2.30, 2.36, 2.40, 2.43, 2.45, 2.59, 2.77, 2.94, 3.24]
    bps = [
        0.919,
        0.912,
        0.901,
        0.892,
        0.885,
        0.876,
        0.833,
        0.788,
        0.747,
        0.672,
    ]  # Bundle purity
    opt_scores = [0.444, 0.436, 0.425, 0.417, 0.411, 0.404, 0.368, 0.331, 0.298, 0.243]

    fig, ax = plt.subplots(figsize=(12, 7))

    # Create scatter with color based on BPS
    scatter = ax.scatter(
        windows,
        mean_sizes,
        c=bps,
        cmap="RdYlGn",
        s=300,
        edgecolors="black",
        linewidths=2,
        vmin=0.65,
        vmax=0.95,
    )

    # Connect points
    ax.plot(windows, mean_sizes, "k--", alpha=0.4, linewidth=1.5)

    # Highlight optimal windows (50ms and 100ms both good)
    ax.scatter([50], [2.24], s=400, facecolors="none", edgecolors="blue", linewidths=3)
    ax.scatter(
        [100], [2.30], s=400, facecolors="none", edgecolors="green", linewidths=3
    )

    # Labels - only for key windows to avoid clutter
    key_windows = [50, 100, 200, 300, 400, 500, 1000, 5000]
    for i, (w, s, m) in enumerate(zip(windows, mean_sizes, bps)):
        if w in key_windows:
            label = f"{s:.2f}\n({m:.0%})"
            offset = (0, 15) if i % 2 == 0 else (0, -25)
            ax.annotate(
                label,
                (w, s),
                textcoords="offset points",
                xytext=offset,
                ha="center",
                fontsize=9,
                fontweight="bold",
            )

    ax.set_xlabel("Time Window (ms)", fontsize=12)
    ax.set_ylabel("Mean Session Bundle Size", fontsize=12)
    ax.set_title(
        "Session Bundle Size vs Time Window\n(Color = High-Confidence Session Rate)",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xscale("log")
    ax.set_xticks(windows)
    ax.set_xticklabels([str(w) for w in windows], rotation=45)
    ax.set_ylim(2.0, 3.5)
    ax.grid(True, alpha=0.3)

    # Colorbar
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("High-Confidence Rate", fontsize=11)

    # Add annotations for optimal windows
    ax.annotate(
        "50ms\n(91.9%)",
        xy=(50, 2.24),
        xytext=(30, 2.7),
        arrowprops=dict(arrowstyle="->", color="blue", lw=2),
        fontsize=10,
        color="blue",
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.7),
    )
    ax.annotate(
        "100ms\n(91.2%)",
        xy=(100, 2.30),
        xytext=(180, 2.9),
        arrowprops=dict(arrowstyle="->", color="green", lw=2),
        fontsize=10,
        color="green",
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.7),
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved scatter plot to {output_path}")


def create_window_comparison_plot(output_path: str) -> None:
    """Create 4-panel comparison plot with extended windows (50-500ms)."""
    # Data from experiment (December 2025 with extended windows)
    # 88,802 user_request records (Dec 10-17, 2025)
    windows = [50, 100, 200, 300, 400, 500, 1000, 2000, 3000, 5000]
    mean_sizes = [2.24, 2.30, 2.36, 2.40, 2.43, 2.45, 2.59, 2.77, 2.94, 3.24]
    mibcs = [0.929, 0.923, 0.915, 0.909, 0.904, 0.898, 0.869, 0.837, 0.809, 0.757]
    bps = [
        0.919,
        0.912,
        0.901,
        0.892,
        0.885,
        0.876,
        0.833,
        0.788,
        0.747,
        0.672,
    ]  # Bundle purity
    opt_scores = [0.444, 0.436, 0.425, 0.417, 0.411, 0.404, 0.368, 0.331, 0.298, 0.243]
    total_bundles = [
        31732,
        30879,
        30090,
        29652,
        29288,
        28957,
        27445,
        25663,
        24204,
        21893,
    ]
    # Singleton = sessions with 1 unique URL; Giant = sessions with >10 unique URLs
    singleton_rates = [
        89.77,
        88.56,
        87.01,
        85.99,
        85.15,
        84.17,
        79.53,
        74.99,
        70.81,
        63.39,
    ]
    giant_rates = [0.02, 0.04, 0.05, 0.05, 0.06, 0.07, 0.08, 0.10, 0.12, 0.18]

    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(
        "Query Fan-Out Session Analysis: Window Size Comparison",
        fontsize=14,
        fontweight="bold",
    )

    # Plot 1: Bundle Size vs Window (scatter with trend)
    ax1 = axes[0, 0]
    scatter1 = ax1.scatter(
        windows,
        mean_sizes,
        c=opt_scores,
        cmap="RdYlGn",
        s=200,
        edgecolors="black",
        linewidths=1.5,
    )
    ax1.plot(windows, mean_sizes, "k--", alpha=0.5, linewidth=1)
    ax1.set_xlabel("Time Window (ms)", fontsize=11)
    ax1.set_ylabel("Mean Session Size (requests)", fontsize=11)
    ax1.set_title("Session Size Growth with Window Width", fontsize=12)
    ax1.set_xscale("log")
    ax1.set_xticks(windows)
    ax1.set_xticklabels([str(w) for w in windows])
    cbar1 = plt.colorbar(scatter1, ax=ax1)
    cbar1.set_label("OptScore", fontsize=10)

    # Add annotations
    for i, w in enumerate(windows):
        ax1.annotate(
            f"{mean_sizes[i]:.2f}",
            (w, mean_sizes[i]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=9,
        )

    # Highlight 50ms and 100ms
    ax1.scatter([50], [2.24], s=300, facecolors="none", edgecolors="blue", linewidths=2)
    ax1.scatter(
        [100], [2.30], s=300, facecolors="none", edgecolors="green", linewidths=2
    )

    # Plot 2: Coherence Metrics vs Window
    ax2 = axes[0, 1]
    ax2.scatter(
        windows,
        mibcs,
        c="green",
        s=150,
        label="High Confidence Rate",
        marker="o",
        edgecolors="black",
    )
    ax2.scatter(
        windows,
        bps,
        c="blue",
        s=150,
        label="Bundle Purity Score",
        marker="s",
        edgecolors="black",
    )
    ax2.plot(windows, mibcs, "g--", alpha=0.5)
    ax2.plot(windows, bps, "b--", alpha=0.5)
    ax2.set_xlabel("Time Window (ms)", fontsize=11)
    ax2.set_ylabel("Score (0-1)", fontsize=11)
    ax2.set_title("Semantic Coherence Degradation", fontsize=12)
    ax2.set_xscale("log")
    ax2.set_xticks(windows)
    ax2.set_xticklabels([str(w) for w in windows])
    ax2.set_ylim(0.6, 1.0)
    ax2.legend(loc="lower left")
    ax2.axhline(y=0.9, color="green", linestyle=":", alpha=0.7, label="90% threshold")

    # Plot 3: Number of Sessions vs Window
    ax3 = axes[1, 0]
    colors = ["blue", "green"] + ["lightgreen"] * 4 + ["gray"] * 4
    bars = ax3.bar(range(len(windows)), total_bundles, color=colors, edgecolor="black")
    ax3.set_xlabel("Time Window (ms)", fontsize=11)
    ax3.set_ylabel("Total Sessions Created", fontsize=11)
    ax3.set_title("Session Count by Window Size", fontsize=12)
    ax3.set_xticks(range(len(windows)))
    ax3.set_xticklabels([str(w) for w in windows])

    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, total_bundles)):
        ax3.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 500,
            f"{val:,}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    # Plot 4: Multi-URL Session Rate (topical authority indicator)
    ax4 = axes[1, 1]
    x_pos = np.arange(len(windows))
    # Multi-URL rate = 100% - singleton rate (sessions with 2+ unique URLs from your site)
    multi_url_rates = [100 - sr for sr in singleton_rates]
    colors_multi = ["blue", "green"] + ["lightgreen"] * 4 + ["gray"] * 4
    bars4 = ax4.bar(x_pos, multi_url_rates, color=colors_multi, edgecolor="black")
    ax4.set_xlabel("Time Window (ms)", fontsize=11)
    ax4.set_ylabel("Multi-URL Sessions (%)", fontsize=11)
    ax4.set_title("Sessions with 2+ Unique URLs (Topical Authority)", fontsize=12)
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels([str(w) for w in windows])
    ax4.set_ylim(0, 50)

    # Add value labels
    for bar, val in zip(bars4, multi_url_rates):
        ax4.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1,
            f"{val:.1f}%",
            ha="center",
            va="bottom",
            fontsize=8,
        )

    # Add note about giant rate
    ax4.annotate(
        "Giant sessions (>10 URLs): <0.2% across all windows",
        xy=(0.5, 0.02),
        xycoords="axes fraction",
        fontsize=9,
        ha="center",
        style="italic",
        color="gray",
    )

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved comparison plot to {output_path}")


def main():
    # Output to docs/research/images for markdown reports
    output_dir = Path("docs/research/images")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Generating plots with 50ms window included...")
    create_window_comparison_plot(str(output_dir / "window_comparison.png"))
    create_size_distribution_plot(str(output_dir / "bundle_size_scatter.png"))
    print("\nPlots saved to docs/research/images/")


if __name__ == "__main__":
    main()
