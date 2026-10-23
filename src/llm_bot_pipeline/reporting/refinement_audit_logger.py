"""Audit logging for semantic session refinement runs."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from ..config.settings import SessionRefinementSettings
from ..storage import StorageBackend
from .session_models import RefinementMetrics

logger = logging.getLogger(__name__)


class RefinementAuditLogger:
    """Write non-blocking refinement audit rows."""

    TABLE_NAME = "session_refinement_log"

    def __init__(
        self,
        backend: StorageBackend,
        refinement_settings: SessionRefinementSettings,
    ) -> None:
        self._backend = backend
        self._settings = refinement_settings

    def log_run(
        self,
        metrics: RefinementMetrics,
        window_ms: float,
        duration_ms: float,
    ) -> None:
        """Log refinement run details without failing the main operation."""
        if not metrics.enabled:
            return

        sql = f"""
            INSERT INTO {self._table_ref()} (
                run_timestamp, window_ms, total_bundles, collision_candidates,
                bundles_split, sub_bundles_created, mean_mibcs_improvement,
                refinement_duration_ms, collision_ip_threshold,
                collision_homogeneity_threshold, similarity_threshold,
                min_sub_bundle_size, min_mibcs_improvement
            ) VALUES (
                :run_timestamp, :window_ms, :total_bundles, :collision_candidates,
                :bundles_split, :sub_bundles_created, :mean_mibcs_improvement,
                :refinement_duration_ms, :collision_ip_threshold,
                :collision_homogeneity_threshold, :similarity_threshold,
                :min_sub_bundle_size, :min_mibcs_improvement
            )
        """

        try:
            self._backend.execute(
                sql,
                {
                    "run_timestamp": datetime.now(timezone.utc),
                    "window_ms": window_ms,
                    "total_bundles": metrics.bundles_analyzed,
                    "collision_candidates": metrics.collision_candidates,
                    "bundles_split": metrics.bundles_split,
                    "sub_bundles_created": metrics.sub_bundles_created,
                    "mean_mibcs_improvement": metrics.mean_mibcs_improvement,
                    "refinement_duration_ms": duration_ms,
                    "collision_ip_threshold": self._settings.collision_ip_threshold,
                    "collision_homogeneity_threshold": (
                        self._settings.collision_homogeneity_threshold
                    ),
                    "similarity_threshold": self._settings.similarity_threshold,
                    "min_sub_bundle_size": self._settings.min_sub_bundle_size,
                    "min_mibcs_improvement": self._settings.min_mibcs_improvement,
                },
            )
            logger.debug("Logged refinement run to session_refinement_log")
        except Exception as e:
            logger.warning(f"Failed to log refinement run: {e}")

    def _table_ref(self) -> str:
        full_id = self._backend.get_full_table_id(self.TABLE_NAME)
        if self._backend.backend_type == "bigquery":
            return f"`{full_id}`"
        return full_id
