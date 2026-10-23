"""Factory for turning bundles into session records."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from ..research.semantic_embeddings import URLEmbedder, get_confidence_level
from ..research.temporal_analysis import Bundle, EnrichedBundle
from ..utils.url_utils import derive_session_name
from .session_models import SessionRecord


class SessionRecordFactory:
    """Convert temporal/refined bundles into storage records."""

    def __init__(self, embedder: URLEmbedder) -> None:
        self._embedder = embedder

    def bundles_to_records(
        self,
        bundles: list[Bundle],
        window_ms: float,
        df: pd.DataFrame,
        bot_name_col: Optional[str],
        parent_mibcs_map: Optional[dict[str, float]] = None,
        splitting_strategy: Optional[str] = None,
    ) -> list[SessionRecord]:
        """Convert Bundle objects to SessionRecord objects."""
        records = []
        now = datetime.now(timezone.utc).isoformat()
        parent_mibcs_map = parent_mibcs_map or {}

        for bundle in bundles:
            mean_sim, min_sim, max_sim, confidence = self._similarity(bundle)
            bot_name = self._bot_name(bundle, df, bot_name_col)
            domain = self._domain(bundle, df)
            unique_url_list = self._unique_urls(bundle.urls)
            parent_session_id, was_refined, reason, pre_mibcs = self._refinement_fields(
                bundle, parent_mibcs_map
            )

            records.append(
                SessionRecord(
                    session_id=bundle.bundle_id,
                    session_date=bundle.start_time.date().isoformat(),
                    domain=domain,
                    session_start_time=bundle.start_time.isoformat(),
                    session_end_time=bundle.end_time.isoformat(),
                    duration_ms=bundle.duration_ms,
                    bot_provider=bundle.bot_provider,
                    bot_name=bot_name,
                    request_count=bundle.request_count,
                    unique_urls=len(unique_url_list),
                    mean_cosine_similarity=mean_sim,
                    min_cosine_similarity=min_sim,
                    max_cosine_similarity=max_sim,
                    confidence_level=confidence,
                    fanout_session_name=(
                        derive_session_name(bundle.urls[0]) if bundle.urls else None
                    ),
                    url_list=json.dumps(unique_url_list),
                    window_ms=window_ms,
                    splitting_strategy=splitting_strategy,
                    _created_at=now,
                    parent_session_id=parent_session_id,
                    was_refined=was_refined,
                    refinement_reason=reason,
                    pre_refinement_mibcs=pre_mibcs,
                )
            )

        return records

    def _similarity(self, bundle: Bundle) -> tuple[float, float, float, str]:
        if len(bundle.urls) < 2:
            return 1.0, 1.0, 1.0, "high"
        sim_result = self._embedder.compute_similarity(bundle.urls)
        return (
            sim_result.mean_similarity,
            sim_result.min_similarity,
            sim_result.max_similarity,
            get_confidence_level(sim_result.mean_similarity, sim_result.min_similarity),
        )

    def _bot_name(
        self,
        bundle: Bundle,
        df: pd.DataFrame,
        bot_name_col: Optional[str],
    ) -> Optional[str]:
        if isinstance(bundle, EnrichedBundle) and bundle.bot_name:
            return bundle.bot_name
        if (
            not bot_name_col
            or not bundle.request_indices
            or "_split_" in bundle.bundle_id
        ):
            return None
        try:
            first_idx = bundle.request_indices[0]
            if first_idx < len(df) and bot_name_col in df.columns:
                return df.iloc[first_idx][bot_name_col]
        except (IndexError, KeyError):
            return None
        return None

    def _domain(self, bundle: Bundle, df: pd.DataFrame) -> Optional[str]:
        if "domain" not in df.columns or not bundle.request_indices:
            return None
        if "_split_" in bundle.bundle_id and isinstance(bundle, EnrichedBundle):
            return getattr(bundle, "domain", None)
        if "_split_" in bundle.bundle_id:
            return None
        try:
            first_idx = bundle.request_indices[0]
            if first_idx < len(df):
                return df.iloc[first_idx]["domain"]
        except (IndexError, KeyError):
            return None
        return None

    def _unique_urls(self, urls: list[str]) -> list[str]:
        seen = set()
        unique_url_list = []
        for url in urls:
            if url not in seen:
                seen.add(url)
                unique_url_list.append(url)
        return unique_url_list

    def _refinement_fields(
        self,
        bundle: Bundle,
        parent_mibcs_map: dict[str, float],
    ) -> tuple[Optional[str], bool, Optional[str], Optional[float]]:
        if "_split_" not in bundle.bundle_id:
            return None, False, None, None
        parent_session_id = bundle.bundle_id.rsplit("_split_", 1)[0]
        return (
            parent_session_id,
            True,
            "semantic_split",
            parent_mibcs_map.get(parent_session_id),
        )
