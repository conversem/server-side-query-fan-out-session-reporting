from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import GoogleAPIError
from google.auth.exceptions import GoogleAuthError
from google.cloud import monitoring_v3

from llm_bot_pipeline.infrastructure.cloud_run_alerts import (
    STUCK_DURATION_SECONDS,
    AlertPolicyConflictError,
    AlertSetupResult,
    AlertTarget,
    PolicyKind,
    build_failed_policy,
    build_scheduler_policy,
    build_stuck_policy,
    channel_display_name,
    ensure_alert_policies,
    ensure_email_channels,
    policy_display_name,
    setup_cloud_run_job_alerts,
)
from scripts.setup_cloud_run_alerts import (
    DEFAULT_JOB_NAME,
    DEFAULT_LOCATION,
    DEFAULT_SCHEDULER_JOB,
    build_arg_parser,
    main,
)

_CLI_PROJECT = "client_gcp_project"
_CLI_EMAIL = "alerts@example.com"
_CLI_ARGV = ["--project-id", _CLI_PROJECT, "--emails", _CLI_EMAIL]


def _target(
    project_id: str = "client_gcp_project",
    job_name: str = "llm-bot-pipeline",
    location: str = "europe-west1",
    scheduler_job: str = "daily-llm-bot-pipeline",
) -> AlertTarget:
    return AlertTarget(project_id, job_name, location, scheduler_job)


def test_build_arg_parser_requires_project_and_emails():
    with pytest.raises(SystemExit):
        build_arg_parser().parse_args([])


def test_build_arg_parser_product_defaults():
    args = build_arg_parser().parse_args(_CLI_ARGV)
    assert args.project_id == _CLI_PROJECT
    assert args.job_name == DEFAULT_JOB_NAME
    assert args.region == DEFAULT_LOCATION
    assert args.scheduler_job == DEFAULT_SCHEDULER_JOB
    assert args.emails == [_CLI_EMAIL]


def _policy(kind: PolicyKind, target: AlertTarget, channel_names: list[str]):
    if kind is PolicyKind.FAILED:
        return build_failed_policy(target, channel_names)
    if kind is PolicyKind.STUCK:
        return build_stuck_policy(target, channel_names)
    if kind is PolicyKind.SCHEDULER:
        return build_scheduler_policy(target, channel_names)
    raise ValueError(f"unknown policy kind: {kind!r}")


@patch("scripts.setup_cloud_run_alerts.setup_cloud_run_job_alerts")
def test_main_success(mock_setup, capsys):
    mock_setup.return_value = AlertSetupResult(
        channel_names=("channel-a",),
        policy_names=("policy-a",),
    )
    exit_code = main(_CLI_ARGV)
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Notification channels:" in captured.out
    assert "  channel-a" in captured.out
    assert "Alert policies:" in captured.out
    assert "  policy-a" in captured.out
    assert (
        "IMPORTANT: Verify each email channel (alerting-noreply@google.com)"
        in captured.out
    )
    mock_setup.assert_called_once()
    target, emails = mock_setup.call_args.args
    assert target == _target(
        _CLI_PROJECT,
        DEFAULT_JOB_NAME,
        DEFAULT_LOCATION,
        DEFAULT_SCHEDULER_JOB,
    )
    assert emails == [_CLI_EMAIL]


@patch("scripts.setup_cloud_run_alerts.setup_cloud_run_job_alerts")
def test_main_google_api_error(mock_setup, capsys):
    mock_setup.side_effect = GoogleAPIError("API failed")
    assert main(_CLI_ARGV) == 1
    captured = capsys.readouterr()
    assert captured.err == "GCP error: API failed\n"


@patch("scripts.setup_cloud_run_alerts.setup_cloud_run_job_alerts")
def test_main_google_auth_error(mock_setup, capsys):
    mock_setup.side_effect = GoogleAuthError("No credentials found")
    assert main(_CLI_ARGV) == 1
    captured = capsys.readouterr()
    assert captured.err == "GCP error: No credentials found\n"


@patch("scripts.setup_cloud_run_alerts.setup_cloud_run_job_alerts")
def test_main_value_error(mock_setup, capsys):
    mock_setup.side_effect = ValueError("At least one email address is required")
    assert main(_CLI_ARGV) == 1
    captured = capsys.readouterr()
    assert captured.err == "Invalid input: At least one email address is required\n"


@patch("scripts.setup_cloud_run_alerts.setup_cloud_run_job_alerts")
def test_main_policy_conflict(mock_setup, capsys):
    mock_setup.side_effect = AlertPolicyConflictError("Condition count mismatch")
    assert main(_CLI_ARGV) == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("Conflict: Condition count mismatch")


def test_job_resource_filter():
    target = _target()
    policy = build_failed_policy(target, [])
    f = policy.conditions[0].condition_threshold.filter
    assert 'resource.type = "cloud_run_job"' in f
    assert 'resource.labels.job_name = "llm-bot-pipeline"' in f
    assert 'resource.labels.location = "europe-west1"' in f


def test_completed_filter_failed():
    target = _target()
    policy = build_failed_policy(target, [])
    f = policy.conditions[0].condition_threshold.filter
    assert "run.googleapis.com/job/completed_execution_count" in f
    assert 'metric.labels.result = "failed"' in f


def test_running_filter():
    target = _target()
    policy = build_stuck_policy(target, [])
    assert "running_executions" in policy.conditions[0].condition_threshold.filter


def test_scheduler_log_filter():
    target = _target()
    policy = build_scheduler_policy(target, [])
    f = policy.conditions[0].condition_matched_log.filter
    assert 'resource.type = "cloud_scheduler_job"' in f
    assert "daily-llm-bot-pipeline" in f
    assert "severity>=ERROR" in f


@pytest.mark.parametrize(
    "bad_value",
    ['job"name', "job\nname", "job\rname"],
)
def test_alert_target_rejects_dangerous_characters(bad_value):
    with pytest.raises(ValueError, match="quotes or newlines"):
        AlertTarget(bad_value, "llm-bot-pipeline", "europe-west1", "sched")
    with pytest.raises(ValueError, match="quotes or newlines"):
        AlertTarget("client_gcp_project", bad_value, "europe-west1", "sched")
    with pytest.raises(ValueError, match="quotes or newlines"):
        AlertTarget("client_gcp_project", "llm-bot-pipeline", bad_value, "sched")
    with pytest.raises(ValueError, match="quotes or newlines"):
        AlertTarget("client_gcp_project", "llm-bot-pipeline", "europe-west1", bad_value)


def test_alert_target_strips_values_and_rejects_blank():
    target = AlertTarget(
        " example ",
        " llm-bot-pipeline ",
        " europe-west1 ",
        " daily-llm-bot-pipeline ",
    )
    assert target.project_id == "example"
    assert target.job_name == "llm-bot-pipeline"
    assert target.location == "europe-west1"
    assert target.scheduler_job == "daily-llm-bot-pipeline"

    with pytest.raises(ValueError, match="must not be empty"):
        AlertTarget("example", " ", "europe-west1", "sched")


def test_display_names():
    assert channel_display_name("a@b.com") == "LLM bot pipeline alerts · a@b.com"
    assert (
        policy_display_name(PolicyKind.FAILED, "llm-bot-pipeline")
        == "llm-bot-pipeline · execution failed"
    )
    assert policy_display_name(PolicyKind.SCHEDULER, "daily-llm-bot-pipeline") == (
        "daily-llm-bot-pipeline · scheduler invoke failed"
    )
    assert (
        policy_display_name(PolicyKind.STUCK, "llm-bot-pipeline")
        == "llm-bot-pipeline · stuck execution"
    )


def test_policy_documentation_links():
    target = _target()
    doc = build_failed_policy(target, []).documentation.content
    assert "console.cloud.google.com" in doc
    assert "llm-bot-pipeline" in doc
    assert "project=client_gcp_project" in doc
    assert "/executions" in doc
    assert "cloud_run_job" in doc
    assert "europe-west1" in doc


def test_scheduler_documentation_links():
    target = _target()
    doc = build_scheduler_policy(target, []).documentation.content
    assert "console.cloud.google.com" in doc
    assert "daily-llm-bot-pipeline" in doc
    assert "project=client_gcp_project" in doc
    assert "cloudscheduler" in doc
    assert "cloud_scheduler_job" in doc
    assert "europe-west1" in doc


def test_policy_builders_order_and_kinds():
    target = _target()
    channel_names = ["projects/example/notificationChannels/1"]
    policies = [
        build_failed_policy(target, channel_names),
        build_stuck_policy(target, channel_names),
        build_scheduler_policy(target, channel_names),
    ]
    assert [policy.display_name for policy in policies] == [
        policy_display_name(PolicyKind.FAILED, target.job_name),
        policy_display_name(PolicyKind.STUCK, target.job_name),
        policy_display_name(PolicyKind.SCHEDULER, target.scheduler_job),
    ]
    assert [policy.user_labels["policy_kind"] for policy in policies] == [
        PolicyKind.FAILED.value,
        PolicyKind.STUCK.value,
        PolicyKind.SCHEDULER.value,
    ]
    assert all(
        policy.user_labels["managed_by"] == "llm-bot-pipeline-alerts"
        for policy in policies
    )


def test_policy_user_labels_include_sanitized_target_identity():
    target = _target(
        job_name="Daily LLM.Bot/Pipeline",
        location="Europe-West1",
        scheduler_job="Daily LLM.Bot Scheduler!",
    )

    failed = build_failed_policy(target, [])
    scheduler = build_scheduler_policy(target, [])

    assert failed.user_labels["job_name"] == "daily-llm-bot-pipeline"
    assert failed.user_labels["location"] == "europe-west1"
    assert failed.user_labels["scheduler_job"] == "daily-llm-bot-scheduler"
    assert dict(scheduler.user_labels) == dict(failed.user_labels) | {
        "policy_kind": PolicyKind.SCHEDULER.value
    }


@pytest.mark.parametrize(
    ("kind", "aligner", "duration", "expected_in_filter"),
    [
        (
            PolicyKind.FAILED,
            monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            0,
            (
                "run.googleapis.com/job/completed_execution_count",
                'metric.labels.result = "failed"',
            ),
        ),
        (
            PolicyKind.STUCK,
            monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
            STUCK_DURATION_SECONDS,
            ("run.googleapis.com/job/running_executions",),
        ),
    ],
)
def test_build_metric_policies(kind, aligner, duration, expected_in_filter):
    target = _target()
    channel_names = ["projects/client_gcp_project/notificationChannels/1"]
    policy = _policy(kind, target, channel_names)
    thresh = policy.conditions[0].condition_threshold
    assert thresh.aggregations[0].per_series_aligner == aligner
    assert thresh.aggregations[0].alignment_period.seconds == 60
    assert thresh.comparison == monitoring_v3.ComparisonType.COMPARISON_GT
    assert thresh.threshold_value == 0
    assert thresh.duration.seconds == duration
    for fragment in expected_in_filter:
        assert fragment in thresh.filter
    assert policy.enabled is True
    assert target.job_name in policy.documentation.content
    assert list(policy.notification_channels) == channel_names
    assert policy.display_name == policy_display_name(kind, target.job_name)


def test_build_scheduler_policy_log_match():
    target = _target()
    channel_names = ["projects/client_gcp_project/notificationChannels/1"]
    policy = _policy(PolicyKind.SCHEDULER, target, channel_names)
    matched = policy.conditions[0].condition_matched_log
    assert 'resource.type = "cloud_scheduler_job"' in matched.filter
    assert target.scheduler_job in matched.filter
    assert target.location in matched.filter
    assert "severity>=ERROR" in matched.filter
    assert policy.alert_strategy.notification_rate_limit.period.seconds == 300
    assert policy.enabled is True
    assert target.scheduler_job in policy.documentation.content
    assert list(policy.notification_channels) == channel_names


def test_ensure_email_reuses_by_address_case_insensitive():
    existing = monitoring_v3.NotificationChannel(
        name="projects/client_gcp_project/notificationChannels/9",
        type_="email",
        display_name="any-old-name",
        labels={"email_address": "operator@example.com"},
    )
    client = MagicMock()
    client.list_notification_channels.return_value = [existing]
    names = ensure_email_channels(
        "client_gcp_project", ["operator@example.com"], channel_client=client
    )
    assert names == (existing.name,)
    client.create_notification_channel.assert_not_called()
    client.update_notification_channel.assert_not_called()


def test_ensure_email_rejects_empty_list():
    client = MagicMock()
    with pytest.raises(ValueError, match="At least one email"):
        ensure_email_channels("client_gcp_project", [], channel_client=client)
    client.list_notification_channels.assert_not_called()


def test_ensure_email_rejects_blank_address():
    client = MagicMock()
    with pytest.raises(ValueError, match="must not be empty"):
        ensure_email_channels("client_gcp_project", ["  "], channel_client=client)
    client.list_notification_channels.assert_not_called()


def test_ensure_email_dedupes_case_insensitively():
    created = monitoring_v3.NotificationChannel(
        name="projects/client_gcp_project/notificationChannels/new",
        type_="email",
        display_name=channel_display_name("User@Example.com"),
        labels={"email_address": "User@Example.com"},
    )
    client = MagicMock()
    client.list_notification_channels.return_value = []
    client.create_notification_channel.return_value = created
    names = ensure_email_channels(
        "client_gcp_project",
        ["User@Example.com", "user@example.com", "USER@EXAMPLE.COM"],
        channel_client=client,
    )
    assert names == (created.name,)
    client.create_notification_channel.assert_called_once()
    sent = client.create_notification_channel.call_args.kwargs["request"][
        "notification_channel"
    ]
    assert sent.labels["email_address"] == "User@Example.com"


def test_ensure_email_strips_dedupes_and_rejects_blank():
    created = monitoring_v3.NotificationChannel(
        name="projects/example/notificationChannels/new",
        type_="email",
        display_name=channel_display_name("User@Example.com"),
        labels={"email_address": "User@Example.com"},
    )
    client = MagicMock()
    client.list_notification_channels.return_value = []
    client.create_notification_channel.return_value = created

    names = ensure_email_channels(
        "example",
        [" User@Example.com ", "user@example.com"],
        channel_client=client,
    )

    assert names == (created.name,)
    sent = client.create_notification_channel.call_args.kwargs["request"][
        "notification_channel"
    ]
    assert sent.labels["email_address"] == "User@Example.com"
    assert sent.display_name == channel_display_name("User@Example.com")

    with pytest.raises(ValueError, match="Email address must not be empty"):
        ensure_email_channels("example", [" "], channel_client=client)


def test_ensure_email_does_not_update_stale_display_name():
    """Reuse by email only — never rename an existing channel (cross-job safe)."""
    existing = monitoring_v3.NotificationChannel(
        name="projects/client_gcp_project/notificationChannels/9",
        type_="email",
        display_name="stale-name",
        labels={"email_address": "user@example.com"},
    )
    client = MagicMock()
    client.list_notification_channels.return_value = [existing]
    names = ensure_email_channels(
        "client_gcp_project",
        ["user@example.com"],
        channel_client=client,
    )
    assert names == (existing.name,)
    client.create_notification_channel.assert_not_called()
    client.update_notification_channel.assert_not_called()


def test_ensure_email_picks_lexicographically_smallest_duplicate():
    older = monitoring_v3.NotificationChannel(
        name="projects/client_gcp_project/notificationChannels/zzz",
        type_="email",
        display_name=channel_display_name("user@example.com"),
        labels={"email_address": "user@example.com"},
    )
    newer_smaller = monitoring_v3.NotificationChannel(
        name="projects/client_gcp_project/notificationChannels/aaa",
        type_="email",
        display_name=channel_display_name("user@example.com"),
        labels={"email_address": "User@Example.com"},
    )
    client = MagicMock()
    client.list_notification_channels.return_value = [older, newer_smaller]
    names = ensure_email_channels(
        "client_gcp_project",
        ["user@example.com"],
        channel_client=client,
    )
    assert names == (newer_smaller.name,)
    client.create_notification_channel.assert_not_called()


def test_ensure_email_ignores_non_email_channels():
    email = "user@example.com"
    slack_channel = monitoring_v3.NotificationChannel(
        name="projects/client_gcp_project/notificationChannels/slack",
        type_="slack",
        display_name="slack",
        labels={"email_address": email},
    )
    created = monitoring_v3.NotificationChannel(
        name="projects/client_gcp_project/notificationChannels/new",
        type_="email",
        display_name=channel_display_name(email),
        labels={"email_address": email},
    )
    client = MagicMock()
    client.list_notification_channels.return_value = [slack_channel]
    client.create_notification_channel.return_value = created
    names = ensure_email_channels("client_gcp_project", [email], channel_client=client)
    assert names == (created.name,)
    client.create_notification_channel.assert_called_once()
    sent = client.create_notification_channel.call_args.kwargs["request"][
        "notification_channel"
    ]
    assert sent.type_ == "email"


def test_ensure_email_creates_missing():
    created = monitoring_v3.NotificationChannel(
        name="projects/client_gcp_project/notificationChannels/new",
        type_="email",
        display_name=channel_display_name("new@example.com"),
        labels={"email_address": "new@example.com"},
    )
    client = MagicMock()
    client.list_notification_channels.return_value = []
    client.create_notification_channel.return_value = created
    names = ensure_email_channels(
        "client_gcp_project", ["new@example.com"], channel_client=client
    )
    assert names == (created.name,)
    client.create_notification_channel.assert_called_once()
    sent = client.create_notification_channel.call_args.kwargs["request"][
        "notification_channel"
    ]
    assert sent.type_ == "email"
    assert sent.labels["email_address"] == "new@example.com"
    assert sent.display_name == channel_display_name("new@example.com")


def test_ensure_policies_second_run_updates_preserves_condition_name():
    cond_name = "projects/client_gcp_project/alertPolicies/1/conditions/c1"
    existing_failed = monitoring_v3.AlertPolicy(
        name="projects/client_gcp_project/alertPolicies/1",
        display_name=policy_display_name(PolicyKind.FAILED, "llm-bot-pipeline"),
        enabled=False,
        conditions=[
            monitoring_v3.AlertPolicy.Condition(
                name=cond_name,
                display_name="old",
                condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
                    filter='metric.type = "run.googleapis.com/job/completed_execution_count"',
                ),
            )
        ],
    )
    client = MagicMock()
    client.list_alert_policies.return_value = [existing_failed]
    updated = monitoring_v3.AlertPolicy(name=existing_failed.name)
    client.update_alert_policy.return_value = updated
    created = monitoring_v3.AlertPolicy(
        name="projects/client_gcp_project/alertPolicies/new"
    )
    client.create_alert_policy.return_value = created

    names = ensure_alert_policies(
        _target(),
        ["projects/client_gcp_project/notificationChannels/1"],
        alert_client=client,
    )
    assert existing_failed.name in names
    assert client.update_alert_policy.called
    request = client.update_alert_policy.call_args.kwargs["request"]
    sent = request["alert_policy"]
    assert sent.enabled is True
    assert sent.conditions[0].name == cond_name
    update_mask = request["update_mask"]
    assert update_mask is not None
    assert set(update_mask.paths) == {
        "enabled",
        "conditions",
        "notification_channels",
        "documentation",
        "combiner",
        "display_name",
        "alert_strategy",
        "user_labels",
    }
    assert sent.user_labels["managed_by"] == "llm-bot-pipeline-alerts"
    assert sent.user_labels["policy_kind"] == PolicyKind.FAILED.value


def test_ensure_policies_scheduler_update_includes_rate_limit():
    channel = "projects/client_gcp_project/notificationChannels/1"
    target = _target()
    existing = _policy(PolicyKind.SCHEDULER, target, [channel])
    existing.name = "projects/client_gcp_project/alertPolicies/sched"
    existing.conditions[0].name = f"{existing.name}/conditions/c1"
    existing.alert_strategy = monitoring_v3.AlertPolicy.AlertStrategy()

    client = MagicMock()
    client.list_alert_policies.return_value = [existing]
    client.update_alert_policy.side_effect = lambda **kwargs: kwargs["request"][
        "alert_policy"
    ]
    client.create_alert_policy.side_effect = lambda **kwargs: kwargs["request"][
        "alert_policy"
    ]

    ensure_alert_policies(target, [channel], alert_client=client)

    scheduler_update = None
    for call in client.update_alert_policy.call_args_list:
        sent = call.kwargs["request"]["alert_policy"]
        if sent.display_name == policy_display_name(
            PolicyKind.SCHEDULER, target.scheduler_job
        ):
            scheduler_update = call
            break
    assert scheduler_update is not None
    sent = scheduler_update.kwargs["request"]["alert_policy"]
    assert "alert_strategy" in scheduler_update.kwargs["request"]["update_mask"].paths
    assert sent.alert_strategy.notification_rate_limit.period.seconds == 300


def test_ensure_policies_condition_count_mismatch_raises():
    existing_failed = monitoring_v3.AlertPolicy(
        name="projects/client_gcp_project/alertPolicies/1",
        display_name=policy_display_name(PolicyKind.FAILED, "llm-bot-pipeline"),
        enabled=True,
        conditions=[
            monitoring_v3.AlertPolicy.Condition(name="c1"),
            monitoring_v3.AlertPolicy.Condition(name="c2"),
        ],
    )
    client = MagicMock()
    client.list_alert_policies.return_value = [existing_failed]

    with pytest.raises(AlertPolicyConflictError, match="Condition count mismatch"):
        ensure_alert_policies(
            _target(),
            ["projects/client_gcp_project/notificationChannels/1"],
            alert_client=client,
        )
    client.update_alert_policy.assert_not_called()
    client.create_alert_policy.assert_not_called()


def test_ensure_policies_duplicate_display_name_raises_before_writes():
    target = _target()
    duplicate_name = policy_display_name(PolicyKind.STUCK, target.job_name)
    existing = [
        monitoring_v3.AlertPolicy(
            name="projects/example/alertPolicies/1",
            display_name=duplicate_name,
            conditions=[monitoring_v3.AlertPolicy.Condition(name="c1")],
        ),
        monitoring_v3.AlertPolicy(
            name="projects/example/alertPolicies/2",
            display_name=duplicate_name,
            conditions=[monitoring_v3.AlertPolicy.Condition(name="c1")],
        ),
    ]
    client = MagicMock()
    client.list_alert_policies.return_value = existing

    with pytest.raises(AlertPolicyConflictError, match="Duplicate alert policies"):
        ensure_alert_policies(
            target,
            ["projects/example/notificationChannels/1"],
            alert_client=client,
        )

    client.update_alert_policy.assert_not_called()
    client.create_alert_policy.assert_not_called()


def test_ensure_policies_prefers_managed_labels_over_display_name():
    channel = "projects/example/notificationChannels/1"
    target = _target()
    labelled = _policy(PolicyKind.FAILED, target, [channel])
    labelled.name = "projects/example/alertPolicies/labelled"
    labelled.display_name = "old display name"
    labelled.conditions[0].name = f"{labelled.name}/conditions/c1"
    fallback = _policy(PolicyKind.STUCK, target, [channel])
    fallback.name = "projects/example/alertPolicies/fallback"
    fallback.conditions[0].name = f"{fallback.name}/conditions/c1"

    client = MagicMock()
    client.list_alert_policies.return_value = [labelled, fallback]
    client.update_alert_policy.side_effect = lambda **kwargs: kwargs["request"][
        "alert_policy"
    ]
    client.create_alert_policy.side_effect = lambda **kwargs: kwargs["request"][
        "alert_policy"
    ]

    names = ensure_alert_policies(target, [channel], alert_client=client)

    assert labelled.name in names
    assert fallback.name in names
    sent_updates = [
        call.kwargs["request"]["alert_policy"]
        for call in client.update_alert_policy.call_args_list
    ]
    failed_update = next(
        policy
        for policy in sent_updates
        if policy.name == "projects/example/alertPolicies/labelled"
    )
    assert failed_update.display_name == policy_display_name(
        PolicyKind.FAILED, target.job_name
    )


def test_ensure_policies_ignores_managed_labels_for_different_target():
    channel = "projects/example/notificationChannels/1"
    target = _target()
    other_target = _target(job_name="other-job", scheduler_job="other-scheduler")
    other_policy = _policy(PolicyKind.FAILED, other_target, [channel])
    other_policy.name = "projects/example/alertPolicies/other"

    client = MagicMock()
    client.list_alert_policies.return_value = [other_policy]
    client.create_alert_policy.side_effect = lambda **kwargs: kwargs["request"][
        "alert_policy"
    ]

    names = ensure_alert_policies(target, [channel], alert_client=client)

    assert other_policy.name not in names
    client.update_alert_policy.assert_not_called()
    assert client.create_alert_policy.call_count == 3


def test_ensure_policies_display_name_with_different_location_labels_conflicts():
    channel = "projects/example/notificationChannels/1"
    target = _target(location="europe-west1")
    other_target = _target(location="us-central1")
    other_policy = _policy(PolicyKind.FAILED, other_target, [channel])
    other_policy.name = "projects/example/alertPolicies/other-location"

    client = MagicMock()
    client.list_alert_policies.return_value = [other_policy]

    with pytest.raises(AlertPolicyConflictError, match="managed for another target"):
        ensure_alert_policies(target, [channel], alert_client=client)

    client.update_alert_policy.assert_not_called()
    client.create_alert_policy.assert_not_called()


def test_ensure_policies_managed_display_name_conflict_raises_before_writes():
    channel = "projects/example/notificationChannels/1"
    target = _target()
    labelled = _policy(PolicyKind.FAILED, target, [channel])
    labelled.name = "projects/example/alertPolicies/labelled"
    labelled.display_name = "old display name"
    labelled.conditions[0].name = f"{labelled.name}/conditions/c1"
    display_owner = monitoring_v3.AlertPolicy(
        name="projects/example/alertPolicies/display-owner",
        display_name=policy_display_name(PolicyKind.FAILED, target.job_name),
        conditions=[monitoring_v3.AlertPolicy.Condition(name="c1")],
    )

    client = MagicMock()
    client.list_alert_policies.return_value = [labelled, display_owner]

    with pytest.raises(AlertPolicyConflictError, match="display name"):
        ensure_alert_policies(target, [channel], alert_client=client)

    client.update_alert_policy.assert_not_called()
    client.create_alert_policy.assert_not_called()


def test_ensure_policies_duplicate_managed_labels_raise_before_writes():
    channel = "projects/example/notificationChannels/1"
    target = _target()
    first = _policy(PolicyKind.FAILED, target, [channel])
    first.name = "projects/example/alertPolicies/1"
    second = _policy(PolicyKind.FAILED, target, [channel])
    second.name = "projects/example/alertPolicies/2"
    second.display_name = "renamed but still labelled"
    client = MagicMock()
    client.list_alert_policies.return_value = [first, second]

    with pytest.raises(AlertPolicyConflictError, match="Multiple managed"):
        ensure_alert_policies(target, [channel], alert_client=client)

    client.update_alert_policy.assert_not_called()
    client.create_alert_policy.assert_not_called()


def test_ensure_policies_second_run_no_creates():
    channel = "projects/client_gcp_project/notificationChannels/1"
    target = _target()
    existing = [
        _policy(PolicyKind.FAILED, target, [channel]),
        _policy(PolicyKind.STUCK, target, [channel]),
        _policy(PolicyKind.SCHEDULER, target, [channel]),
    ]
    expected_cond_names: list[str] = []
    for i, policy in enumerate(existing):
        policy.name = f"projects/client_gcp_project/alertPolicies/{i + 1}"
        cond_name = f"{policy.name}/conditions/c1"
        policy.conditions[0].name = cond_name
        expected_cond_names.append(cond_name)
    client = MagicMock()
    client.list_alert_policies.return_value = existing
    client.update_alert_policy.side_effect = lambda **kwargs: kwargs["request"][
        "alert_policy"
    ]

    names = ensure_alert_policies(target, [channel], alert_client=client)
    assert names == tuple(p.name for p in existing)
    client.create_alert_policy.assert_not_called()
    assert client.update_alert_policy.call_count == 3
    for call, expected_cond_name in zip(
        client.update_alert_policy.call_args_list, expected_cond_names
    ):
        sent = call.kwargs["request"]["alert_policy"]
        assert sent.conditions[0].name == expected_cond_name


def test_setup_wires_channels_into_policies():
    channel_names = ("projects/client_gcp_project/notificationChannels/1",)
    policy_names = ("projects/client_gcp_project/alertPolicies/1",)
    target = _target()
    with (
        patch(
            "llm_bot_pipeline.infrastructure.cloud_run_alerts.ensure_email_channels",
            return_value=channel_names,
        ) as mock_channels,
        patch(
            "llm_bot_pipeline.infrastructure.cloud_run_alerts.ensure_alert_policies",
            return_value=policy_names,
        ) as mock_policies,
    ):
        result = setup_cloud_run_job_alerts(target, ["a@b.com"])
    mock_channels.assert_called_once_with("client_gcp_project", ["a@b.com"])
    mock_policies.assert_called_once_with(target, channel_names)
    assert isinstance(result, AlertSetupResult)
    assert result.channel_names == channel_names
    assert result.policy_names == policy_names
    assert isinstance(result.channel_names, tuple)
    assert isinstance(result.policy_names, tuple)


def test_alert_setup_result_frozen_tuples():
    result = AlertSetupResult(channel_names=("a",), policy_names=("b",))
    with pytest.raises(Exception):
        result.channel_names = ("mutated",)  # type: ignore[misc]
    hash(result)
