"""Cloud Run job alerting helpers for Cloud Monitoring policies."""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from urllib.parse import quote

from google.cloud import monitoring_v3
from google.protobuf import duration_pb2, field_mask_pb2

# Primary metrics confirmed for Cloud Run jobs. Fallback if
# completed_execution_count is ever missing on a project: use
# run.googleapis.com/job/completed_task_attempt_count with result=failed
# (see docs/monitoring-setup.md). Do not wire both at once.
METRIC_COMPLETED_EXECUTIONS = "run.googleapis.com/job/completed_execution_count"
METRIC_RUNNING_EXECUTIONS = "run.googleapis.com/job/running_executions"
# Alert only after 6 hours. Healthy runs stay well under that once a shared
# log prefix is scanned once; this leaves room for volume growth before a
# still-running execution is treated as stuck. The task timeout is 12 hours.
STUCK_DURATION_SECONDS = 6 * 60 * 60
_ALIGNMENT_PERIOD_SECONDS = 60
_SCHEDULER_NOTIFICATION_RATE_LIMIT_SECONDS = 300

_POLICY_UPDATE_MASK = field_mask_pb2.FieldMask(
    paths=[
        "enabled",
        "conditions",
        "notification_channels",
        "documentation",
        "combiner",
        "display_name",
        "alert_strategy",
        "user_labels",
    ]
)
_MANAGED_BY_LABEL = "managed_by"
_MANAGED_BY_VALUE = "llm-bot-pipeline-alerts"
_POLICY_KIND_LABEL = "policy_kind"
_MANAGED_POLICY_LABELS = frozenset(
    (
        _MANAGED_BY_LABEL,
        _POLICY_KIND_LABEL,
        "job_name",
        "location",
        "scheduler_job",
    )
)
_LABEL_VALUE_MAX_LENGTH = 63
_LABEL_VALUE_INVALID_PATTERN = re.compile(r"[^a-z0-9_-]+")


class PolicyKind(str, Enum):
    """Alert policy kinds; ``str(kind)`` is the stable registry key."""

    FAILED = "failed"
    STUCK = "stuck"
    SCHEDULER = "scheduler"

    @property
    def label(self) -> str:
        if self is PolicyKind.FAILED:
            return "execution failed"
        if self is PolicyKind.STUCK:
            return "stuck execution"
        if self is PolicyKind.SCHEDULER:
            return "scheduler invoke failed"
        raise ValueError(f"unknown policy kind: {self!r}")


class AlertPolicyConflictError(ValueError):
    """Existing policy cannot be updated safely (e.g. condition-count drift)."""


@dataclass(frozen=True)
class AlertTarget:
    """Validated Cloud Run job + scheduler identity for alert setup."""

    project_id: str
    job_name: str
    location: str
    scheduler_job: str

    def __post_init__(self) -> None:
        for field_name in (
            "project_id",
            "job_name",
            "location",
            "scheduler_job",
        ):
            value = getattr(self, field_name)
            normalized = _normalize_filter_value(value, field_name)
            object.__setattr__(self, field_name, normalized)


@dataclass(frozen=True)
class AlertSetupResult:
    """Result of ensuring email channels and alert policies."""

    channel_names: tuple[str, ...]
    policy_names: tuple[str, ...]


def _normalize_filter_value(value: str, field_name: str) -> str:
    """Reject values that would break quoted Monitoring/Logs filters."""
    value = value.strip()
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    if '"' in value or "\n" in value or "\r" in value:
        raise ValueError(f"Filter value must not contain quotes or newlines: {value!r}")
    return value


def _job_resource_filter(job_name: str, location: str) -> str:
    return (
        'resource.type = "cloud_run_job" AND '
        f'resource.labels.job_name = "{job_name}" AND '
        f'resource.labels.location = "{location}"'
    )


def _completed_filter(job_name: str, location: str, result: str) -> str:
    return (
        f"{_job_resource_filter(job_name, location)} AND "
        f'metric.type = "{METRIC_COMPLETED_EXECUTIONS}" AND '
        f'metric.labels.result = "{result}"'
    )


def _running_filter(job_name: str, location: str) -> str:
    return (
        f"{_job_resource_filter(job_name, location)} AND "
        f'metric.type = "{METRIC_RUNNING_EXECUTIONS}"'
    )


def _scheduler_log_filter(scheduler_job: str, location: str) -> str:
    return (
        'resource.type = "cloud_scheduler_job" AND '
        f'resource.labels.job_id = "{scheduler_job}" AND '
        f'resource.labels.location = "{location}" AND '
        "severity>=ERROR"
    )


def channel_display_name(email: str) -> str:
    """Project-global channel label — email only, never job-scoped."""
    return f"LLM bot pipeline alerts · {email}"


def policy_display_name(kind: PolicyKind, name: str) -> str:
    return f"{name} · {kind.label}"


def _logs_query_url(project_id: str, query: str) -> str:
    return (
        "https://console.cloud.google.com/logs/query;"
        f"query={quote(query, safe='')};"
        f"storageScope=project;duration=P1D?project={quote(project_id, safe='')}"
    )


def _job_documentation(target: AlertTarget) -> str:
    console_url = (
        f"https://console.cloud.google.com/run/jobs/details/"
        f"{quote(target.location, safe='')}/{quote(target.job_name, safe='')}/executions"
        f"?project={quote(target.project_id, safe='')}"
    )
    logs_query = (
        f'resource.type="cloud_run_job"\n'
        f'resource.labels.job_name="{target.job_name}"\n'
        f'resource.labels.location="{target.location}"'
    )
    return (
        f"Cloud Run job `{target.job_name}` ({target.location}) in project "
        f"`{target.project_id}`.\n"
        f"Executions: {console_url}\n"
        f"Logs Explorer: {_logs_query_url(target.project_id, logs_query)}"
    )


def _scheduler_documentation(target: AlertTarget) -> str:
    console_url = (
        "https://console.cloud.google.com/cloudscheduler/jobs/edit/"
        f"{quote(target.location, safe='')}/{quote(target.scheduler_job, safe='')}"
        f"?project={quote(target.project_id, safe='')}"
    )
    logs_query = (
        f'resource.type="cloud_scheduler_job"\n'
        f'resource.labels.job_id="{target.scheduler_job}"\n'
        f'resource.labels.location="{target.location}"'
    )
    return (
        f"Cloud Scheduler job `{target.scheduler_job}` ({target.location}) in project "
        f"`{target.project_id}`.\n"
        f"Scheduler console: {console_url}\n"
        f"Logs Explorer: {_logs_query_url(target.project_id, logs_query)}"
    )


def _metric_threshold_condition(
    *,
    display_name: str,
    metric_filter: str,
    aligner: monitoring_v3.Aggregation.Aligner,
    duration_seconds: int,
) -> monitoring_v3.AlertPolicy.Condition:
    return monitoring_v3.AlertPolicy.Condition(
        display_name=display_name,
        condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
            filter=metric_filter,
            comparison=monitoring_v3.ComparisonType.COMPARISON_GT,
            threshold_value=0,
            duration=duration_pb2.Duration(seconds=duration_seconds),
            aggregations=[
                monitoring_v3.Aggregation(
                    alignment_period=duration_pb2.Duration(
                        seconds=_ALIGNMENT_PERIOD_SECONDS
                    ),
                    per_series_aligner=aligner,
                )
            ],
        ),
    )


def _scheduler_rate_limit_strategy() -> monitoring_v3.AlertPolicy.AlertStrategy:
    return monitoring_v3.AlertPolicy.AlertStrategy(
        notification_rate_limit=(
            monitoring_v3.AlertPolicy.AlertStrategy.NotificationRateLimit(
                period=duration_pb2.Duration(
                    seconds=_SCHEDULER_NOTIFICATION_RATE_LIMIT_SECONDS
                ),
            )
        ),
    )


def _sanitize_label_value(value: str) -> str:
    sanitized = _LABEL_VALUE_INVALID_PATTERN.sub("-", value.lower()).strip("-_")
    if sanitized and len(sanitized) <= _LABEL_VALUE_MAX_LENGTH:
        return sanitized
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:_LABEL_VALUE_MAX_LENGTH]


def _policy_user_labels(kind: PolicyKind, target: AlertTarget) -> dict[str, str]:
    return {
        _MANAGED_BY_LABEL: _MANAGED_BY_VALUE,
        _POLICY_KIND_LABEL: kind.value,
        "job_name": _sanitize_label_value(target.job_name),
        "location": _sanitize_label_value(target.location),
        "scheduler_job": _sanitize_label_value(target.scheduler_job),
    }


def _policy_envelope(
    *,
    kind: PolicyKind,
    target: AlertTarget,
    subject_name: str,
    documentation: str,
    condition: monitoring_v3.AlertPolicy.Condition,
    channel_names: Sequence[str],
    alert_strategy: monitoring_v3.AlertPolicy.AlertStrategy | None = None,
) -> monitoring_v3.AlertPolicy:
    return monitoring_v3.AlertPolicy(
        display_name=policy_display_name(kind, subject_name),
        documentation=monitoring_v3.AlertPolicy.Documentation(
            content=documentation,
            mime_type="text/markdown",
        ),
        combiner=monitoring_v3.AlertPolicy.ConditionCombinerType.OR,
        enabled=True,
        notification_channels=list(channel_names),
        alert_strategy=(
            alert_strategy
            if alert_strategy is not None
            else monitoring_v3.AlertPolicy.AlertStrategy()
        ),
        conditions=[condition],
        user_labels=_policy_user_labels(kind, target),
    )


def build_failed_policy(
    target: AlertTarget,
    channel_names: Sequence[str],
) -> monitoring_v3.AlertPolicy:
    return _policy_envelope(
        kind=PolicyKind.FAILED,
        target=target,
        subject_name=target.job_name,
        documentation=_job_documentation(target),
        condition=_metric_threshold_condition(
            display_name="Cloud Run job failed execution",
            metric_filter=_completed_filter(target.job_name, target.location, "failed"),
            aligner=monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            duration_seconds=0,
        ),
        channel_names=channel_names,
    )


def build_stuck_policy(
    target: AlertTarget,
    channel_names: Sequence[str],
) -> monitoring_v3.AlertPolicy:
    return _policy_envelope(
        kind=PolicyKind.STUCK,
        target=target,
        subject_name=target.job_name,
        documentation=_job_documentation(target),
        condition=_metric_threshold_condition(
            display_name="Cloud Run job stuck execution",
            metric_filter=_running_filter(target.job_name, target.location),
            aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
            duration_seconds=STUCK_DURATION_SECONDS,
        ),
        channel_names=channel_names,
    )


def build_scheduler_policy(
    target: AlertTarget,
    channel_names: Sequence[str],
) -> monitoring_v3.AlertPolicy:
    return _policy_envelope(
        kind=PolicyKind.SCHEDULER,
        target=target,
        subject_name=target.scheduler_job,
        documentation=_scheduler_documentation(target),
        condition=monitoring_v3.AlertPolicy.Condition(
            display_name="Cloud Scheduler invoke failed",
            condition_matched_log=monitoring_v3.AlertPolicy.Condition.LogMatch(
                filter=_scheduler_log_filter(target.scheduler_job, target.location),
            ),
        ),
        channel_names=channel_names,
        alert_strategy=_scheduler_rate_limit_strategy(),
    )


def _project_parent(project_id: str) -> str:
    return f"projects/{project_id}"


def _email_channel_key(channel: monitoring_v3.NotificationChannel) -> str | None:
    if channel.type_ != "email":
        return None
    email_address = channel.labels.get("email_address", "").strip()
    if not email_address:
        return None
    return email_address.casefold()


def _normalize_email(email: str) -> str:
    email = email.strip()
    if not email:
        raise ValueError("Email address must not be empty")
    return email


def _dedupe_emails(emails: Sequence[str]) -> list[str]:
    """Dedupe emails case-insensitively, preserving first occurrence."""
    seen: set[str] = set()
    result: list[str] = []
    for email in emails:
        normalized = _normalize_email(email)
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(normalized)
    return result


def ensure_email_channels(
    project_id: str,
    emails: Sequence[str],
    *,
    channel_client: monitoring_v3.NotificationChannelServiceClient | None = None,
) -> tuple[str, ...]:
    """Find-or-create email channels by address. Never renames existing channels."""
    if not emails:
        raise ValueError("At least one email address is required")

    unique_emails = _dedupe_emails(emails)
    client = channel_client or monitoring_v3.NotificationChannelServiceClient()
    parent = _project_parent(project_id)

    # Prefer lexicographically smallest resource name when duplicates exist.
    existing_by_email: dict[str, monitoring_v3.NotificationChannel] = {}
    for channel in client.list_notification_channels(request={"name": parent}):
        key = _email_channel_key(channel)
        if key is None:
            continue
        prior = existing_by_email.get(key)
        if prior is None or channel.name < prior.name:
            existing_by_email[key] = channel

    channel_names: list[str] = []
    for email in unique_emails:
        key = email.casefold()
        existing = existing_by_email.get(key)
        if existing is not None:
            channel_names.append(existing.name)
            continue

        created = client.create_notification_channel(
            request={
                "name": parent,
                "notification_channel": monitoring_v3.NotificationChannel(
                    type_="email",
                    display_name=channel_display_name(email),
                    labels={"email_address": email},
                ),
            }
        )
        channel_names.append(created.name)
        existing_by_email[key] = created
    return tuple(channel_names)


def _merge_policy_for_update(
    existing: monitoring_v3.AlertPolicy,
    desired: monitoring_v3.AlertPolicy,
) -> monitoring_v3.AlertPolicy:
    if len(existing.conditions) != len(desired.conditions):
        raise AlertPolicyConflictError(
            f"Condition count mismatch for policy {desired.display_name!r}: "
            f"existing={len(existing.conditions)} desired={len(desired.conditions)}. "
            "Refusing to update without preserving condition resource names. "
            "Inspect or delete the drifted policy in Cloud Monitoring Console, "
            "then re-run (see docs/monitoring-setup.md)."
        )

    conditions: list[monitoring_v3.AlertPolicy.Condition] = []
    for existing_condition, desired_condition in zip(
        existing.conditions, desired.conditions
    ):
        merged_condition = monitoring_v3.AlertPolicy.Condition(desired_condition)
        merged_condition.name = existing_condition.name
        conditions.append(merged_condition)

    return monitoring_v3.AlertPolicy(
        name=existing.name,
        display_name=desired.display_name,
        documentation=desired.documentation,
        combiner=desired.combiner,
        enabled=True,
        notification_channels=list(desired.notification_channels),
        conditions=conditions,
        alert_strategy=desired.alert_strategy,
        user_labels=dict(desired.user_labels),
    )


def _managed_policy_kind(
    policy: monitoring_v3.AlertPolicy,
    target: AlertTarget,
) -> PolicyKind | None:
    labels = policy.user_labels
    if labels.get(_MANAGED_BY_LABEL) != _MANAGED_BY_VALUE:
        return None
    try:
        kind = PolicyKind(labels[_POLICY_KIND_LABEL])
    except (KeyError, ValueError):
        return None
    desired_labels = _policy_user_labels(kind, target)
    if any(labels.get(key) != value for key, value in desired_labels.items()):
        return None
    return kind


def _has_managed_policy_labels(policy: monitoring_v3.AlertPolicy) -> bool:
    return any(key in policy.user_labels for key in _MANAGED_POLICY_LABELS)


def _managed_policy_labels_match(
    policy: monitoring_v3.AlertPolicy,
    desired: monitoring_v3.AlertPolicy,
) -> bool:
    return all(
        policy.user_labels.get(key) == value
        for key, value in desired.user_labels.items()
    )


def _resolve_existing_policy(
    *,
    kind: PolicyKind,
    desired: monitoring_v3.AlertPolicy,
    existing_by_kind: dict[PolicyKind, list[monitoring_v3.AlertPolicy]],
    existing_by_display_name: dict[str, list[monitoring_v3.AlertPolicy]],
) -> monitoring_v3.AlertPolicy | None:
    display_matches = existing_by_display_name.get(desired.display_name, [])
    if len(display_matches) > 1:
        names = ", ".join(policy.name for policy in display_matches)
        raise AlertPolicyConflictError(
            f"Duplicate alert policies found for display name {desired.display_name!r}: "
            f"{names}. Delete or rename duplicates, then re-run."
        )

    managed_matches = existing_by_kind.get(kind, [])
    if len(managed_matches) > 1:
        names = ", ".join(policy.name for policy in managed_matches)
        raise AlertPolicyConflictError(
            f"Multiple managed alert policies found for kind {kind.value!r}: {names}. "
            "Delete or relabel the duplicate policies, then re-run."
        )
    if managed_matches:
        managed_match = managed_matches[0]
        if display_matches and display_matches[0].name != managed_match.name:
            raise AlertPolicyConflictError(
                f"Managed alert policy {managed_match.name} for "
                f"{desired.display_name!r} conflicts with existing display name on "
                f"{display_matches[0].name}. Delete, rename, or relabel one policy, "
                "then re-run."
            )
        return managed_match

    if display_matches:
        display_match = display_matches[0]
        if _has_managed_policy_labels(
            display_match
        ) and not _managed_policy_labels_match(display_match, desired):
            raise AlertPolicyConflictError(
                f"Alert policy {display_match.name} with display name "
                f"{desired.display_name!r} is managed for another target. "
                "Delete, rename, or relabel it, then re-run."
            )
        return display_match
    return None


def ensure_alert_policies(
    target: AlertTarget,
    channel_names: Sequence[str],
    *,
    alert_client: monitoring_v3.AlertPolicyServiceClient | None = None,
) -> tuple[str, ...]:
    client = alert_client or monitoring_v3.AlertPolicyServiceClient()
    parent = _project_parent(target.project_id)
    desired_policies = [
        (PolicyKind.FAILED, build_failed_policy(target, channel_names)),
        (PolicyKind.STUCK, build_stuck_policy(target, channel_names)),
        (PolicyKind.SCHEDULER, build_scheduler_policy(target, channel_names)),
    ]
    existing_by_display_name: dict[str, list[monitoring_v3.AlertPolicy]] = defaultdict(
        list
    )
    existing_by_kind: dict[PolicyKind, list[monitoring_v3.AlertPolicy]] = defaultdict(
        list
    )
    for policy in client.list_alert_policies(request={"name": parent}):
        existing_by_display_name[policy.display_name].append(policy)
        kind = _managed_policy_kind(policy, target)
        if kind is not None:
            existing_by_kind[kind].append(policy)

    policy_plan: list[
        tuple[monitoring_v3.AlertPolicy, monitoring_v3.AlertPolicy | None]
    ] = []
    for kind, desired in desired_policies:
        existing = _resolve_existing_policy(
            kind=kind,
            desired=desired,
            existing_by_kind=existing_by_kind,
            existing_by_display_name=existing_by_display_name,
        )
        if existing is not None:
            _merge_policy_for_update(existing, desired)
        policy_plan.append((desired, existing))

    policy_names: list[str] = []
    for desired, existing in policy_plan:
        if existing is not None:
            updated = client.update_alert_policy(
                request={
                    "alert_policy": _merge_policy_for_update(existing, desired),
                    "update_mask": _POLICY_UPDATE_MASK,
                }
            )
            policy_names.append(updated.name)
            continue

        created = client.create_alert_policy(
            request={"name": parent, "alert_policy": desired}
        )
        policy_names.append(created.name)
    return tuple(policy_names)


def setup_cloud_run_job_alerts(
    target: AlertTarget,
    emails: Sequence[str],
) -> AlertSetupResult:
    channel_names = ensure_email_channels(target.project_id, emails)
    policy_names = ensure_alert_policies(target, channel_names)
    return AlertSetupResult(channel_names=channel_names, policy_names=policy_names)
