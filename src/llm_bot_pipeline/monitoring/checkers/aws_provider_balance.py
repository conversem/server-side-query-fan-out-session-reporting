"""Data-quality checker for AWS-source row counts.

Compares configured ``aws_s3`` domains against per-(source_provider, domain)
row counts in ``raw_bot_requests`` for a single day. Missing or zero counts
are FAIL (likely STS or Terraform misconfiguration). Deployments with no AWS
domains skip the check.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Optional

from ...config.constants import DATASET_RAW
from ...config.settings import DomainConfig
from .models import QualityCheckResult, QualityStatus, _error_result

if TYPE_CHECKING:
    from ...storage.base import StorageBackend


class AwsProviderBalanceChecker:
    """Reports configured AWS domains with zero rows for the check date as FAIL."""

    def __init__(
        self,
        backend: "StorageBackend",
        table_name: str = "raw_bot_requests",
        dataset_raw: str = DATASET_RAW,
        expected_aws_pairs: Optional[list[tuple[str, str]]] = None,
    ) -> None:
        self.backend = backend
        self.table_name = table_name
        self.dataset_raw = dataset_raw
        self.expected_aws_pairs = list(expected_aws_pairs or [])

    @staticmethod
    def expected_pairs_from_domains(
        domains: list[DomainConfig],
    ) -> list[tuple[str, str]]:
        """Build (domain, source_provider) pairs for every configured AWS domain."""
        pairs: list[tuple[str, str]] = []
        for domain_cfg in domains:
            if domain_cfg.provider == "aws_s3" and domain_cfg.aws is not None:
                pairs.append((domain_cfg.domain, f"aws_{domain_cfg.aws.source_type}"))
        return pairs

    def check_balance(self, check_date: Optional[date] = None) -> QualityCheckResult:
        if check_date is None:
            check_date = date.today()

        if not self.expected_aws_pairs:
            return QualityCheckResult(
                check_name="aws_provider_balance",
                status=QualityStatus.PASS,
                message="No AWS domains configured",
                details={},
            )

        table_id = self.backend.get_full_table_id(self.table_name)
        sql = f"""
            SELECT source_provider, domain, COUNT(*) AS rows
            FROM `{table_id}`
            WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)))
                  = DATE('{check_date.isoformat()}')
            GROUP BY source_provider, domain
        """
        try:
            rows = self.backend.query(sql)
        except Exception as exc:
            return _error_result("aws_provider_balance", exc)

        counts: dict[tuple[str, str], int] = {}
        for row in rows:
            source_provider = str(row["source_provider"])
            if not source_provider.startswith("aws_"):
                continue
            counts[(source_provider, str(row["domain"]))] = int(row["rows"])

        zero_domains: list[str] = []
        observed: list[dict] = []
        for domain, source_provider in self.expected_aws_pairs:
            row_count = counts.get((source_provider, domain), 0)
            observed.append(
                {
                    "domain": domain,
                    "source_provider": source_provider,
                    "rows": row_count,
                }
            )
            if row_count == 0:
                zero_domains.append(domain)

        if zero_domains:
            return QualityCheckResult(
                check_name="aws_provider_balance",
                status=QualityStatus.FAIL,
                message=f"AWS domains with zero rows: {zero_domains}",
                details={"domains_with_zero": zero_domains, "all_rows": observed},
            )
        return QualityCheckResult(
            check_name="aws_provider_balance",
            status=QualityStatus.PASS,
            message=f"All configured AWS domains have rows ({len(observed)})",
            details={"all_rows": observed},
        )

    def check_row_count_band(
        self,
        check_date: Optional[date] = None,
        band_fraction: float = 0.5,
    ) -> QualityCheckResult:
        """D9 watch: row count per (source_provider, domain) within ±band_fraction
        of the 7-day rolling median ending the day before `check_date`.

        Returns WARN when today's count is outside the band, PASS otherwise.
        A NULL rolling median (no prior history) returns PASS to avoid flapping
        on Day 1 of a new domain's lifecycle.
        """
        if check_date is None:
            check_date = date.today()

        table_id = self.backend.get_full_table_id(self.table_name)
        sql = f"""
        WITH per_day AS (
          SELECT
            DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000))) AS d,
            source_provider,
            domain,
            COUNT(*) AS rows
          FROM `{table_id}`
          WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)))
                BETWEEN DATE_SUB(DATE('{check_date.isoformat()}'), INTERVAL 7 DAY)
                    AND DATE('{check_date.isoformat()}')
          GROUP BY d, source_provider, domain
        ),
        today AS (
          SELECT source_provider, domain, rows AS rows_today
          FROM per_day WHERE d = DATE('{check_date.isoformat()}')
        ),
        history AS (
          SELECT source_provider, domain,
                 APPROX_QUANTILES(rows, 100)[OFFSET(50)] AS rolling_median_7d
          FROM per_day
          WHERE d < DATE('{check_date.isoformat()}')
          GROUP BY source_provider, domain
        )
        SELECT t.source_provider, t.domain, t.rows_today,
               h.rolling_median_7d
        FROM today t LEFT JOIN history h
          ON t.source_provider = h.source_provider AND t.domain = h.domain
        """
        try:
            rows = list(self.backend.query(sql))
        except Exception as exc:
            return _error_result("aws_row_count_band", exc)

        offenders = []
        for r in rows:
            median = r["rolling_median_7d"]
            if median is None or median == 0:
                continue
            lower = median * (1.0 - band_fraction)
            upper = median * (1.0 + band_fraction)
            if not (lower <= r["rows_today"] <= upper):
                offenders.append(r)

        if offenders:
            return QualityCheckResult(
                check_name="aws_row_count_band",
                status=QualityStatus.WARN,
                message=(
                    f"{len(offenders)} (source_provider, domain) pair(s) outside "
                    f"±{band_fraction:.0%} of 7-day median: "
                    f"{[(o['domain'], o['rows_today'], o['rolling_median_7d']) for o in offenders]}"
                ),
                details={"offenders": offenders, "band_fraction": band_fraction},
            )
        return QualityCheckResult(
            check_name="aws_row_count_band",
            status=QualityStatus.PASS,
            message=(
                f"All (source_provider, domain) row counts within "
                f"±{band_fraction:.0%} of 7-day median"
            ),
            details={"rows": rows},
        )

    def check_dedup_ratio(
        self,
        check_date: Optional[date] = None,
        threshold: float = 0.02,
    ) -> QualityCheckResult:
        """Phase-4 V12 watch-item.

        Reports WARN when any AWS source_provider's dedup ratio for the
        given date exceeds the threshold. Dedup ratio = (raw - unique) / raw,
        measured by ROW_NUMBER OVER (PARTITION BY EdgeStartTimestamp,
        ClientIP, ClientRequestURI, ClientRequestHost).
        """
        if check_date is None:
            check_date = date.today()

        table_id = self.backend.get_full_table_id(self.table_name)
        sql = f"""
        WITH raw_rows AS (
          SELECT source_provider, domain, COUNT(*) AS raw
          FROM `{table_id}`
          WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)))
                = DATE('{check_date.isoformat()}')
            AND STARTS_WITH(source_provider, 'aws_')
          GROUP BY source_provider, domain
        ),
        unique_rows AS (
          SELECT source_provider, domain, COUNT(*) AS unique_count
          FROM (
            SELECT source_provider, domain,
                   ROW_NUMBER() OVER (
                     PARTITION BY EdgeStartTimestamp, ClientIP,
                                  ClientRequestURI, ClientRequestHost
                   ) AS rn
            FROM `{table_id}`
            WHERE DATE(TIMESTAMP_MICROS(DIV(EdgeStartTimestamp, 1000)))
                  = DATE('{check_date.isoformat()}')
              AND STARTS_WITH(source_provider, 'aws_')
          ) WHERE rn = 1
          GROUP BY source_provider, domain
        )
        SELECT r.source_provider, r.domain, r.raw,
               u.unique_count AS unique,
               SAFE_DIVIDE(r.raw - u.unique_count, r.raw) AS ratio
        FROM raw_rows r JOIN unique_rows u USING (source_provider, domain)
        """
        try:
            rows = list(self.backend.query(sql))
        except Exception as exc:
            return _error_result("aws_dedup_ratio", exc)

        offenders = [r for r in rows if (r["ratio"] or 0) > threshold]

        if not rows:
            return QualityCheckResult(
                check_name="aws_dedup_ratio",
                status=QualityStatus.PASS,
                message="No AWS source_providers configured",
                details={},
            )
        if offenders:
            return QualityCheckResult(
                check_name="aws_dedup_ratio",
                status=QualityStatus.WARN,
                message=(
                    f"{len(offenders)} AWS source(s) over dedup threshold "
                    f"{threshold:.2%}: {[(o['domain'], o['ratio']) for o in offenders]}"
                ),
                details={
                    "offenders": offenders,
                    "all_rows": rows,
                    "threshold": threshold,
                },
            )
        return QualityCheckResult(
            check_name="aws_dedup_ratio",
            status=QualityStatus.PASS,
            message=f"All AWS sources under {threshold:.2%} dedup threshold",
            details={"all_rows": rows, "threshold": threshold},
        )
