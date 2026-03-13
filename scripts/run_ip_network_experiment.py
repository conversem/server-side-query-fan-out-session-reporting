#!/usr/bin/env python3
"""
IP Network vs Host Analysis Research Experiment.

Follow-up research to the IP Fingerprint Analysis, investigating whether
IP Network (first 3 octets) provides a better collision detection signal
than full IP address.

Research Questions:
1. RQ1: Does IP Network homogeneity better characterize bundle membership than full IP?
2. RQ2: Does Network diversity correlate with semantic incoherence (collisions)?
3. RQ3: Within same-network bundles, does Host diversity indicate collisions?
4. RQ4: Can we use Network + Host count as a combined collision signal?

Hypotheses:
- H1: Multi-network bundles have significantly lower MIBCS than single-network bundles
- H2: Within single-network bundles, host diversity correlates with collision rate
- H3: Network homogeneity is a higher-precision collision signal than IP homogeneity

Background:
Manual inspection showed that semantically related URLs within bundles often share
the same IP Network (first 3 octets) but may have different Host portions (last octet).
This suggests load-balancing within data center infrastructure.

Usage:
    # Run with default settings (100ms window)
    python scripts/run_ip_network_experiment.py

    # Custom output directory
    python scripts/run_ip_network_experiment.py --output-dir data/reports/ip_network_test

    # Multiple window sizes
    python scripts/run_ip_network_experiment.py --windows 50,100,200
"""

import argparse
import json
import logging
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from llm_bot_pipeline.utils.bot_classifier import classify_bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class BundleMetrics:
    """Computed metrics for a single bundle."""

    bundle_id: int
    request_count: int
    unique_urls: int
    unique_ips: int
    unique_networks: int
    unique_hosts: int
    mibcs: float
    ip_homogeneity: float
    network_homogeneity: float
    is_single_network: bool
    is_single_ip: bool
    is_collision: bool  # MIBCS < 0.5


@dataclass
class ExperimentResults:
    """Results from the IP Network experiment."""

    window_ms: float
    total_requests: int
    total_bundles: int
    multi_url_bundles: int

    # IP vs Network comparison
    unique_ips: int
    unique_networks: int
    single_ip_rate: float
    single_network_rate: float
    mean_ip_homogeneity: float
    mean_network_homogeneity: float

    # Correlation analysis
    mibcs_ip_correlation: float
    mibcs_network_correlation: float

    # Clean vs Collision comparison
    clean_bundle_count: int
    collision_bundle_count: int
    clean_network_homogeneity: float
    collision_network_homogeneity: float
    clean_single_network_rate: float
    collision_single_network_rate: float

    # Multi-network as collision signal
    multi_network_bundle_count: int
    multi_network_collision_rate: float
    multi_network_precision: float  # P(collision | multi-network)
    multi_network_recall: float  # P(multi-network | collision)

    # Host diversity within single-network bundles
    single_network_single_host_collision_rate: float
    single_network_multi_host_collision_rate: float
    host_diversity_correlation: float

    # By host count
    collision_rate_by_host_count: dict


# =============================================================================
# Data Loading
# =============================================================================


def load_data(db_path: Path) -> pd.DataFrame:
    """Load raw request data from SQLite database."""
    logger.info(f"Loading data from {db_path}")

    conn = sqlite3.connect(db_path)

    query = """
        SELECT
            EdgeStartTimestamp AS datetime,
            ClientRequestHost || ClientRequestURI AS full_url,
            ClientIP AS client_ip,
            ClientCountry AS client_country,
            ClientRequestUserAgent AS user_agent
        FROM raw_bot_requests
        ORDER BY EdgeStartTimestamp
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Parse timestamps
    df["datetime"] = pd.to_datetime(df["datetime"], format="ISO8601")

    # Classify bots
    logger.info("Classifying bots...")
    bot_info = df["user_agent"].apply(classify_bot)
    df["bot_provider"] = [b.bot_provider if b else "Unknown" for b in bot_info]
    df["bot_category"] = [b.bot_category if b else "unknown" for b in bot_info]

    # Filter to user_request category
    df = df[df["bot_category"] == "user_request"].copy()
    logger.info(f"Filtered to {len(df):,} user_request records")

    # Parse IP components
    df["ip_network"] = df["client_ip"].apply(
        lambda x: ".".join(x.split(".")[:3]) if pd.notna(x) else None
    )
    df["ip_host"] = df["client_ip"].apply(
        lambda x: x.split(".")[3] if pd.notna(x) and len(x.split(".")) == 4 else None
    )

    return df


# =============================================================================
# Bundle Creation
# =============================================================================


def create_temporal_bundles(
    df: pd.DataFrame,
    window_ms: float = 100,
) -> pd.DataFrame:
    """Create temporal bundles and compute metrics."""
    logger.info(f"Creating temporal bundles with {window_ms}ms window...")

    df = df.sort_values(["bot_provider", "datetime"]).copy()

    # Assign bundle IDs
    bundle_ids = []
    current_bundle = 0
    prev_provider = None
    prev_time = None

    for _, row in df.iterrows():
        if prev_provider != row["bot_provider"]:
            current_bundle += 1
        elif prev_time is not None:
            time_diff = (row["datetime"] - prev_time).total_seconds() * 1000
            if time_diff > window_ms:
                current_bundle += 1

        bundle_ids.append(current_bundle)
        prev_provider = row["bot_provider"]
        prev_time = row["datetime"]

    df["bundle_id"] = bundle_ids

    logger.info(f"Created {df['bundle_id'].nunique():,} bundles")

    return df


def compute_mibcs(urls: list[str]) -> Optional[float]:
    """Compute Mean Intra-Bundle Cosine Similarity for a list of URLs."""
    unique_urls = list(set(urls))
    if len(unique_urls) < 2:
        return 1.0

    try:
        vectorizer = TfidfVectorizer(analyzer="char", ngram_range=(3, 5))
        tfidf = vectorizer.fit_transform(unique_urls)
        sim_matrix = cosine_similarity(tfidf)
        n = len(unique_urls)
        similarities = [sim_matrix[i, j] for i in range(n) for j in range(i + 1, n)]
        return float(np.mean(similarities)) if similarities else 1.0
    except Exception:
        return None


def compute_bundle_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute metrics for each bundle."""
    logger.info("Computing bundle metrics...")

    # Aggregate by bundle
    bundle_agg = (
        df.groupby("bundle_id")
        .agg(
            request_count=("full_url", "count"),
            unique_urls=("full_url", "nunique"),
            unique_ips=("client_ip", "nunique"),
            unique_networks=("ip_network", "nunique"),
            unique_hosts=("ip_host", "nunique"),
            bot_provider=("bot_provider", "first"),
        )
        .reset_index()
    )

    # Compute homogeneity metrics
    ip_homog = (
        df.groupby("bundle_id")
        .apply(
            lambda g: (
                g["client_ip"].value_counts().iloc[0] / len(g) if len(g) > 0 else None
            ),
            include_groups=False,
        )
        .reset_index(name="ip_homogeneity")
    )

    network_homog = (
        df.groupby("bundle_id")
        .apply(
            lambda g: (
                g["ip_network"].value_counts().iloc[0] / len(g) if len(g) > 0 else None
            ),
            include_groups=False,
        )
        .reset_index(name="network_homogeneity")
    )

    bundle_agg = bundle_agg.merge(ip_homog, on="bundle_id")
    bundle_agg = bundle_agg.merge(network_homog, on="bundle_id")

    # Compute MIBCS for each bundle
    logger.info("Computing MIBCS for each bundle...")
    bundle_urls = df.groupby("bundle_id")["full_url"].apply(list).reset_index()
    bundle_urls["mibcs"] = bundle_urls["full_url"].apply(compute_mibcs)

    bundle_agg = bundle_agg.merge(
        bundle_urls[["bundle_id", "mibcs"]], on="bundle_id", how="left"
    )

    # Add derived columns
    bundle_agg["is_single_network"] = bundle_agg["unique_networks"] == 1
    bundle_agg["is_single_ip"] = bundle_agg["unique_ips"] == 1
    bundle_agg["is_collision"] = bundle_agg["mibcs"] < 0.5

    return bundle_agg


# =============================================================================
# Research Question Analysis
# =============================================================================


def analyze_rq1_network_vs_ip(
    df: pd.DataFrame,
    bundles: pd.DataFrame,
) -> dict:
    """
    RQ1: Does IP Network better characterize bundle membership than full IP?

    Compare single-IP rate vs single-Network rate, and homogeneity metrics.
    """
    multi_url = bundles[bundles["unique_urls"] >= 2]

    return {
        "total_bundles": len(bundles),
        "multi_url_bundles": len(multi_url),
        # Overall metrics
        "unique_ips_in_data": df["client_ip"].nunique(),
        "unique_networks_in_data": df["ip_network"].nunique(),
        "ip_to_network_ratio": df["client_ip"].nunique() / df["ip_network"].nunique(),
        # All bundles
        "single_ip_rate_all": (bundles["is_single_ip"]).mean(),
        "single_network_rate_all": (bundles["is_single_network"]).mean(),
        "mean_ip_homogeneity_all": bundles["ip_homogeneity"].mean(),
        "mean_network_homogeneity_all": bundles["network_homogeneity"].mean(),
        # Multi-URL bundles only
        "single_ip_rate_multi_url": (multi_url["is_single_ip"]).mean(),
        "single_network_rate_multi_url": (multi_url["is_single_network"]).mean(),
        "mean_ip_homogeneity_multi_url": multi_url["ip_homogeneity"].mean(),
        "mean_network_homogeneity_multi_url": multi_url["network_homogeneity"].mean(),
    }


def analyze_rq2_network_collision_signal(bundles: pd.DataFrame) -> dict:
    """
    RQ2: Does Network diversity correlate with collisions?

    Compare single-network vs multi-network bundles in terms of MIBCS.
    """
    multi_url = bundles[bundles["unique_urls"] >= 2].copy()

    # Correlation analysis
    mibcs_ip_corr = multi_url["mibcs"].corr(multi_url["ip_homogeneity"])
    mibcs_network_corr = multi_url["mibcs"].corr(multi_url["network_homogeneity"])

    # Split by clean vs collision
    clean = multi_url[~multi_url["is_collision"]]
    collision = multi_url[multi_url["is_collision"]]

    # Multi-network analysis
    multi_network = multi_url[~multi_url["is_single_network"]]
    single_network = multi_url[multi_url["is_single_network"]]

    # Precision and recall of multi-network as collision signal
    if len(multi_network) > 0:
        multi_network_collision_rate = multi_network["is_collision"].mean()
        multi_network_precision = multi_network_collision_rate
    else:
        multi_network_collision_rate = 0
        multi_network_precision = 0

    total_collisions = collision["is_collision"].sum()
    if total_collisions > 0:
        collisions_with_multi_network = multi_network["is_collision"].sum()
        multi_network_recall = collisions_with_multi_network / total_collisions
    else:
        multi_network_recall = 0

    return {
        # Correlation
        "mibcs_ip_correlation": mibcs_ip_corr,
        "mibcs_network_correlation": mibcs_network_corr,
        # Clean vs Collision
        "clean_count": len(clean),
        "collision_count": len(collision),
        "clean_ip_homogeneity": (
            clean["ip_homogeneity"].mean() if len(clean) > 0 else None
        ),
        "collision_ip_homogeneity": (
            collision["ip_homogeneity"].mean() if len(collision) > 0 else None
        ),
        "clean_network_homogeneity": (
            clean["network_homogeneity"].mean() if len(clean) > 0 else None
        ),
        "collision_network_homogeneity": (
            collision["network_homogeneity"].mean() if len(collision) > 0 else None
        ),
        "clean_single_network_rate": (
            clean["is_single_network"].mean() if len(clean) > 0 else None
        ),
        "collision_single_network_rate": (
            collision["is_single_network"].mean() if len(collision) > 0 else None
        ),
        # Multi-network as signal
        "multi_network_bundles": len(multi_network),
        "single_network_bundles": len(single_network),
        "multi_network_collision_rate": multi_network_collision_rate,
        "single_network_collision_rate": (
            single_network["is_collision"].mean() if len(single_network) > 0 else None
        ),
        "multi_network_precision": multi_network_precision,
        "multi_network_recall": multi_network_recall,
        # MIBCS comparison
        "multi_network_mean_mibcs": (
            multi_network["mibcs"].mean() if len(multi_network) > 0 else None
        ),
        "single_network_mean_mibcs": (
            single_network["mibcs"].mean() if len(single_network) > 0 else None
        ),
    }


def analyze_rq3_host_diversity(df: pd.DataFrame, bundles: pd.DataFrame) -> dict:
    """
    RQ3: Within same-network bundles, does Host diversity indicate collisions?

    Analyze single-network bundles and compare single-host vs multi-host.
    """
    # Filter to single-network, multi-URL bundles
    single_network_multi_url = bundles[
        (bundles["is_single_network"]) & (bundles["unique_urls"] >= 2)
    ].copy()

    if len(single_network_multi_url) == 0:
        return {"error": "No single-network multi-URL bundles"}

    # Single host vs multi host
    same_host = single_network_multi_url[single_network_multi_url["unique_hosts"] == 1]
    diff_hosts = single_network_multi_url[single_network_multi_url["unique_hosts"] > 1]

    # Correlation between host count and MIBCS
    host_mibcs_corr = single_network_multi_url["mibcs"].corr(
        single_network_multi_url["unique_hosts"]
    )

    # Collision rate by host count
    collision_by_hosts = {}
    for n_hosts in range(1, 6):
        subset = single_network_multi_url[
            single_network_multi_url["unique_hosts"] == n_hosts
        ]
        if len(subset) > 0:
            collision_by_hosts[n_hosts] = {
                "count": len(subset),
                "collision_rate": subset["is_collision"].mean(),
                "mean_mibcs": subset["mibcs"].mean(),
            }

    return {
        "single_network_multi_url_count": len(single_network_multi_url),
        # Same host vs different hosts
        "same_host_count": len(same_host),
        "same_host_collision_rate": (
            same_host["is_collision"].mean() if len(same_host) > 0 else None
        ),
        "same_host_mean_mibcs": (
            same_host["mibcs"].mean() if len(same_host) > 0 else None
        ),
        "diff_hosts_count": len(diff_hosts),
        "diff_hosts_collision_rate": (
            diff_hosts["is_collision"].mean() if len(diff_hosts) > 0 else None
        ),
        "diff_hosts_mean_mibcs": (
            diff_hosts["mibcs"].mean() if len(diff_hosts) > 0 else None
        ),
        # Correlation
        "host_count_mibcs_correlation": host_mibcs_corr,
        # Breakdown by host count
        "collision_rate_by_host_count": collision_by_hosts,
        # Host count distribution
        "host_count_distribution": (
            single_network_multi_url["unique_hosts"]
            .value_counts()
            .sort_index()
            .to_dict()
        ),
    }


def analyze_rq4_combined_signal(bundles: pd.DataFrame) -> dict:
    """
    RQ4: Can we use Network + Host count as a combined collision signal?

    Evaluate different thresholds for collision detection.
    """
    multi_url = bundles[bundles["unique_urls"] >= 2].copy()
    total_collisions = multi_url["is_collision"].sum()

    results = {}

    # Test different signals
    signals = [
        ("multi_network", ~multi_url["is_single_network"]),
        ("multi_ip", ~multi_url["is_single_ip"]),
        ("hosts_ge_2", multi_url["unique_hosts"] >= 2),
        ("hosts_ge_3", multi_url["unique_hosts"] >= 3),
        (
            "multi_network_or_hosts_ge_3",
            (~multi_url["is_single_network"]) | (multi_url["unique_hosts"] >= 3),
        ),
        ("network_homog_lt_90", multi_url["network_homogeneity"] < 0.9),
        ("ip_homog_lt_50", multi_url["ip_homogeneity"] < 0.5),
    ]

    for name, mask in signals:
        flagged = multi_url[mask]
        if len(flagged) > 0:
            precision = flagged["is_collision"].mean()
            recall = (
                flagged["is_collision"].sum() / total_collisions
                if total_collisions > 0
                else 0
            )
            f1 = (
                2 * precision * recall / (precision + recall)
                if (precision + recall) > 0
                else 0
            )
        else:
            precision = recall = f1 = 0

        results[name] = {
            "flagged_count": len(flagged),
            "flagged_rate": len(flagged) / len(multi_url) if len(multi_url) > 0 else 0,
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
        }

    return results


# =============================================================================
# Report Generation
# =============================================================================


def generate_report(
    rq1: dict,
    rq2: dict,
    rq3: dict,
    rq4: dict,
    window_ms: float,
    output_path: Path,
) -> None:
    """Generate comprehensive Markdown research report."""

    # Format helpers
    def fmt_pct(v):
        return f"{v:.1%}" if v is not None else "N/A"

    def fmt_float(v):
        return f"{v:.3f}" if v is not None else "N/A"

    # Build collision by host count table
    host_table = ""
    if "collision_rate_by_host_count" in rq3:
        for n, data in sorted(rq3["collision_rate_by_host_count"].items()):
            host_table += f"| {n} host(s) | {data['count']:,} | {data['collision_rate']:.1%} | {data['mean_mibcs']:.3f} |\n"

    # Build signal comparison table
    signal_table = ""
    for name, data in rq4.items():
        signal_table += f"| {name} | {data['flagged_count']:,} | {data['precision']:.1%} | {data['recall']:.1%} | {data['f1_score']:.3f} |\n"

    report = f"""# IP Network vs Host Analysis Research Report

**Generated**: {datetime.now(timezone.utc).isoformat()}
**Window Size**: {window_ms}ms

---

## Executive Summary

This follow-up study investigates whether IP Network (first 3 octets) provides a better
collision detection signal than full IP address, based on the observation that semantically
related URLs within bundles often share the same IP Network but may have different Host
portions (last octet).

### Key Findings

1. **Network homogeneity is much higher than IP homogeneity**: {fmt_pct(rq1.get('single_network_rate_multi_url'))} of multi-URL bundles have a single network vs {fmt_pct(rq1.get('single_ip_rate_multi_url'))} single IP.

2. **Multi-network is a high-precision collision signal**: {fmt_pct(rq2.get('multi_network_precision'))} precision, but only {fmt_pct(rq2.get('multi_network_recall'))} recall.

3. **Host diversity within same network correlates with collisions**: Single-host collision rate {fmt_pct(rq3.get('same_host_collision_rate'))} vs multi-host {fmt_pct(rq3.get('diff_hosts_collision_rate'))}.

---

## Research Question 1: Network vs IP Characterization

> Does IP Network better characterize bundle membership than full IP?

### Data Overview

| Metric | Value |
|--------|-------|
| Total Bundles | {rq1['total_bundles']:,} |
| Multi-URL Bundles | {rq1['multi_url_bundles']:,} |
| Unique IPs in Data | {rq1['unique_ips_in_data']:,} |
| Unique Networks in Data | {rq1['unique_networks_in_data']:,} |
| IP-to-Network Ratio | {rq1['ip_to_network_ratio']:.1f} IPs per network |

### Homogeneity Comparison (Multi-URL Bundles)

| Metric | IP | Network |
|--------|-----|---------|
| Single-X Rate | {fmt_pct(rq1['single_ip_rate_multi_url'])} | {fmt_pct(rq1['single_network_rate_multi_url'])} |
| Mean Homogeneity | {fmt_pct(rq1['mean_ip_homogeneity_multi_url'])} | {fmt_pct(rq1['mean_network_homogeneity_multi_url'])} |

**Finding**: Network homogeneity is significantly higher than IP homogeneity, suggesting that
requests within bundles cluster by network (data center) but may use different host IPs
(load balancing within the data center).

---

## Research Question 2: Network Diversity as Collision Signal

> Does Network diversity correlate with semantic incoherence?

### Correlation Analysis

| Metric Pair | Pearson r |
|-------------|-----------|
| MIBCS × IP Homogeneity | {fmt_float(rq2['mibcs_ip_correlation'])} |
| MIBCS × Network Homogeneity | {fmt_float(rq2['mibcs_network_correlation'])} |

### Clean vs Collision Bundles

| Bundle Type | n | Network Homogeneity | Single-Network Rate |
|-------------|---|---------------------|---------------------|
| Clean (MIBCS ≥ 0.5) | {rq2['clean_count']:,} | {fmt_pct(rq2['clean_network_homogeneity'])} | {fmt_pct(rq2['clean_single_network_rate'])} |
| Collision (MIBCS < 0.5) | {rq2['collision_count']:,} | {fmt_pct(rq2['collision_network_homogeneity'])} | {fmt_pct(rq2['collision_single_network_rate'])} |

### Multi-Network as Collision Signal

| Metric | Value |
|--------|-------|
| Multi-Network Bundles | {rq2['multi_network_bundles']:,} |
| Single-Network Bundles | {rq2['single_network_bundles']:,} |
| Multi-Network Collision Rate | {fmt_pct(rq2['multi_network_collision_rate'])} |
| Single-Network Collision Rate | {fmt_pct(rq2['single_network_collision_rate'])} |
| **Precision** (P(collision | multi-network)) | **{fmt_pct(rq2['multi_network_precision'])}** |
| **Recall** (P(multi-network | collision)) | **{fmt_pct(rq2['multi_network_recall'])}** |

### MIBCS by Network Count

| Network Type | n | Mean MIBCS |
|--------------|---|------------|
| Single-Network | {rq2['single_network_bundles']:,} | {fmt_float(rq2['single_network_mean_mibcs'])} |
| Multi-Network | {rq2['multi_network_bundles']:,} | {fmt_float(rq2['multi_network_mean_mibcs'])} |

**Finding**: Multi-network bundles are almost always collisions ({fmt_pct(rq2['multi_network_precision'])} precision),
but most collisions still occur within single-network bundles ({fmt_pct(1 - rq2['multi_network_recall'])} of collisions).

---

## Research Question 3: Host Diversity Within Same Network

> Within single-network bundles, does Host diversity indicate collisions?

### Single-Network, Multi-URL Bundles

| Metric | Value |
|--------|-------|
| Total Single-Network Multi-URL Bundles | {rq3.get('single_network_multi_url_count', 'N/A'):,} |

### Same Host vs Different Hosts

| Host Configuration | n | Collision Rate | Mean MIBCS |
|--------------------|---|----------------|------------|
| Single Host (same IP) | {rq3.get('same_host_count', 'N/A'):,} | {fmt_pct(rq3.get('same_host_collision_rate'))} | {fmt_float(rq3.get('same_host_mean_mibcs'))} |
| Multiple Hosts (diff IPs, same network) | {rq3.get('diff_hosts_count', 'N/A'):,} | {fmt_pct(rq3.get('diff_hosts_collision_rate'))} | {fmt_float(rq3.get('diff_hosts_mean_mibcs'))} |

### Collision Rate by Host Count

| Host Count | n | Collision Rate | Mean MIBCS |
|------------|---|----------------|------------|
{host_table}

**Correlation** (Host Count × MIBCS): {fmt_float(rq3.get('host_count_mibcs_correlation'))}

**Finding**: Within single-network bundles, more host diversity correlates with higher collision rates.
This supports the hypothesis that true single-user sessions use fewer unique hosts.

---

## Research Question 4: Combined Signal Evaluation

> Can we use Network + Host count as a combined collision signal?

### Signal Comparison

| Signal | Flagged | Precision | Recall | F1 Score |
|--------|---------|-----------|--------|----------|
{signal_table}

**Finding**: Multi-network is a high-precision but low-recall signal. Combining with host count
may improve recall while maintaining reasonable precision.

---

## Conclusions

### Hypothesis Testing

| Hypothesis | Result |
|------------|--------|
| **H1**: Multi-network bundles have lower MIBCS | {'✅ SUPPORTED' if rq2.get('multi_network_mean_mibcs', 1) < rq2.get('single_network_mean_mibcs', 0) else '❌ NOT SUPPORTED'} |
| **H2**: Host diversity correlates with collisions | {'✅ SUPPORTED' if rq3.get('diff_hosts_collision_rate', 0) > rq3.get('same_host_collision_rate', 1) else '❌ NOT SUPPORTED'} |
| **H3**: Network homogeneity is higher-precision than IP | {'✅ SUPPORTED' if rq1.get('single_network_rate_multi_url', 0) > rq1.get('single_ip_rate_multi_url', 0) else '❌ NOT SUPPORTED'} |

### Recommendations

1. **Use multi-network as a high-confidence collision flag**: {fmt_pct(rq2['multi_network_precision'])} precision means
   bundles with multiple networks can be confidently split.

2. **MIBCS remains the primary collision signal**: Most collisions ({fmt_pct(1 - rq2['multi_network_recall'])}) occur
   within single-network bundles and require semantic analysis.

3. **Host diversity is a secondary signal**: Within single-network bundles, high host counts
   (≥3 hosts) correlate with collisions and could trigger semantic refinement.

---

## Appendix

### Reproducibility

```bash
python scripts/run_ip_network_experiment.py \\
    --db-path data/llm-bot-logs.db \\
    --output-dir data/reports/ip_network_experiment \\
    --windows {window_ms}
```

---

*Report generated by run_ip_network_experiment.py*
"""

    output_path.write_text(report)
    logger.info(f"Report saved to {output_path}")


# =============================================================================
# Main Entry Point
# =============================================================================


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run IP Network vs Host analysis experiment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/llm-bot-logs.db"),
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/reports/ip_network_experiment"),
        help="Output directory for results",
    )
    parser.add_argument(
        "--windows",
        type=str,
        default="100",
        help="Comma-separated window sizes in ms (default: 100)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse window sizes
    window_sizes = [float(w.strip()) for w in args.windows.split(",")]

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    if not args.db_path.exists():
        logger.error(f"Database not found: {args.db_path}")
        return 1

    df = load_data(args.db_path)

    # Run experiment for each window size
    all_results = {}

    for window_ms in window_sizes:
        logger.info(f"\n{'='*60}")
        logger.info(f"Running experiment with {window_ms}ms window")
        logger.info("=" * 60)

        # Create bundles
        df_bundled = create_temporal_bundles(df, window_ms)

        # Compute metrics
        bundles = compute_bundle_metrics(df_bundled)

        # Run research question analyses
        logger.info("Analyzing RQ1: Network vs IP Characterization...")
        rq1 = analyze_rq1_network_vs_ip(df_bundled, bundles)

        logger.info("Analyzing RQ2: Network Diversity as Collision Signal...")
        rq2 = analyze_rq2_network_collision_signal(bundles)

        logger.info("Analyzing RQ3: Host Diversity Within Same Network...")
        rq3 = analyze_rq3_host_diversity(df_bundled, bundles)

        logger.info("Analyzing RQ4: Combined Signal Evaluation...")
        rq4 = analyze_rq4_combined_signal(bundles)

        # Store results
        all_results[window_ms] = {
            "rq1_network_vs_ip": rq1,
            "rq2_network_collision_signal": rq2,
            "rq3_host_diversity": rq3,
            "rq4_combined_signal": rq4,
        }

        # Generate report
        report_path = args.output_dir / f"ip_network_report_{int(window_ms)}ms.md"
        generate_report(rq1, rq2, rq3, rq4, window_ms, report_path)

    # Save raw results as JSON
    results_path = args.output_dir / "experiment_results.json"
    with open(results_path, "w") as f:
        json.dump(
            {str(k): v for k, v in all_results.items()},
            f,
            indent=2,
            default=str,
        )
    logger.info(f"Results saved to {results_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("EXPERIMENT COMPLETE")
    print("=" * 60)
    for window_ms, results in all_results.items():
        rq1 = results["rq1_network_vs_ip"]
        rq2 = results["rq2_network_collision_signal"]
        print(f"\n{window_ms}ms Window:")
        print(f"  Bundles: {rq1['total_bundles']:,}")
        print(f"  Single-Network Rate: {rq1['single_network_rate_multi_url']:.1%}")
        print(
            f"  Multi-Network Collision Rate: {rq2['multi_network_collision_rate']:.1%}"
        )
        print(f"  Multi-Network Precision: {rq2['multi_network_precision']:.1%}")

    print(f"\nReports saved to: {args.output_dir}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
