#!/usr/bin/env python3
"""Verify the first natural VEVO GrowthBook Preview reconciliation run.

The verifier consumes only read-only AWS API responses collected by the
protected workflow.  It deliberately accepts no task identifier, date window,
or expected count from an operator, so a manual one-shot cannot be relabelled
as the scheduled run.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


EXPECTED_ACCOUNT_ID = "919341186960"
EXPECTED_REGION = "eu-central-1"
EXPECTED_STACK_NAME = "vevo-growthbook-reconciliation-preview"
EXPECTED_SCHEDULE_NAME = "vevo-growthbook-reconcile-preview"
EXPECTED_SOURCE_SCHEDULE = "vevo-daily-report-email"
EXPECTED_SOURCE_TASK_FAMILY = "vevo-reporting-daily"
EXPECTED_TASK_DEFINITION = "vevo-growthbook-reconcile-preview:4"
EXPECTED_IMAGE_DIGEST = (
    "sha256:cabba3b0bd57f6be322f3a5ff62f0327"
    "c7cf8e7bb2b6b5e78686305339fdd041"
)
EXPECTED_RUNTIME_PATH = "/app"
EXPECTED_SCHEDULE_EXPRESSION = "cron(30 3 * * ? *)"
EXPECTED_SCHEDULE_TIMEZONE = "Europe/Bratislava"
EXPECTED_EVENT_FROM = "2026-07-13"
EXPECTED_EVENT_THROUGH = "2026-08-21"
EXPECTED_PARTITIONS = 40
MAX_RAW_EVENTS = 50_000
FIRST_RUN_DUE_UTC = datetime(2026, 8, 22, 1, 30, tzinfo=timezone.utc)
VERIFY_NOT_BEFORE_UTC = datetime(2026, 8, 22, 1, 40, tzinfo=timezone.utc)
VERIFY_BEFORE_UTC = datetime(2026, 8, 23, 1, 30, tzinfo=timezone.utc)
EXPECTED_COMMAND = [
    "/bin/bash",
    "-lc",
    "cd /app && python scripts/run_scheduled_growthbook_reconciliation.py",
]
EXPECTED_ALARMS = {
    "vevo-growthbook-reconcile-preview-failure",
    "vevo-growthbook-reconcile-preview-missing-success",
    "vevo-growthbook-reconcile-preview-dlq",
}
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
SUCCESS_RE = re.compile(
    r"GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:"
    r"project=vevo:environment=preview:"
    r"event-from=(?P<event_from>\d{4}-\d{2}-\d{2}):"
    r"event-through=(?P<event_through>\d{4}-\d{2}-\d{2}):"
    r"partitions=(?P<partitions>\d+)"
)


class VerificationError(ValueError):
    """Raised when the natural-run evidence is incomplete or inconsistent."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def _load(path: str | Path) -> Mapping[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise VerificationError(f"unreadable verification artifact: {Path(path).name}") from exc
    if not isinstance(value, dict):
        raise VerificationError(f"verification artifact must be an object: {Path(path).name}")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise VerificationError(f"invalid timestamp: {field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise VerificationError(f"timestamp must be timezone-aware: {field}")
    return parsed.astimezone(timezone.utc)


def _stack_values(rows: Any, key_name: str, value_name: str) -> dict[str, str]:
    if not isinstance(rows, list):
        return {}
    return {
        str(row.get(key_name) or ""): str(row.get(value_name) or "")
        for row in rows
        if isinstance(row, dict) and row.get(key_name)
    }


def _messages(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows = payload.get("events") or []
    _require(isinstance(rows, list), "CloudWatch events must be a list")
    return [row for row in rows if isinstance(row, dict)]


def _normalized_override(value: Mapping[str, Any]) -> dict[str, Any]:
    containers = value.get("containerOverrides") or []
    _require(isinstance(containers, list) and len(containers) == 1, "one container override is required")
    container = containers[0]
    _require(isinstance(container, dict), "container override must be an object")
    environment = container.get("environment") or []
    _require(isinstance(environment, list), "container environment override must be a list")
    return {
        "name": str(container.get("name") or ""),
        "command": list(container.get("command") or []),
        "environment": {
            str(row.get("name") or ""): str(row.get("value") or "")
            for row in environment
            if isinstance(row, dict)
        },
    }


def _cloudtrail_detail(row: Mapping[str, Any]) -> Mapping[str, Any] | None:
    raw = row.get("CloudTrailEvent")
    if not isinstance(raw, str):
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def verify_natural_reconciliation(
    *,
    schedule_payload: Mapping[str, Any],
    stack_payload: Mapping[str, Any],
    task_definition_payload: Mapping[str, Any],
    marker_events_payload: Mapping[str, Any],
    task_state_payload: Mapping[str, Any],
    task_logs_payload: Mapping[str, Any],
    alarms_payload: Mapping[str, Any],
    dlq_payload: Mapping[str, Any],
    source_schedule_payload: Mapping[str, Any],
    cloudtrail_payload: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise VerificationError("verification clock must be timezone-aware")
    now_utc = now.astimezone(timezone.utc)
    _require(now_utc >= VERIFY_NOT_BEFORE_UTC, "first natural-run verification is not due yet")
    _require(now_utc < VERIFY_BEFORE_UTC, "first natural-run live ECS evidence window has closed")

    stacks = stack_payload.get("Stacks") or []
    _require(isinstance(stacks, list) and len(stacks) == 1, "exact reconciliation stack is required")
    stack = stacks[0]
    _require(isinstance(stack, dict), "reconciliation stack must be an object")
    _require(
        stack.get("StackName") == EXPECTED_STACK_NAME,
        "reconciliation stack identity drift",
    )
    _require(
        stack.get("StackStatus") in {"CREATE_COMPLETE", "UPDATE_COMPLETE"},
        "reconciliation stack is not complete",
    )
    parameters = _stack_values(stack.get("Parameters"), "ParameterKey", "ParameterValue")
    outputs = _stack_values(stack.get("Outputs"), "OutputKey", "OutputValue")
    expected_task_arn = parameters.get("TaskDefinitionArn", "")
    _require(
        expected_task_arn.endswith(f"task-definition/{EXPECTED_TASK_DEFINITION}"),
        "reconciliation task definition drift",
    )
    _require(parameters.get("Environment") == "preview", "reconciliation environment drift")
    _require(parameters.get("ScheduleState") == "ENABLED", "stack schedule state drift")
    _require(
        parameters.get("ScheduleExpression") == EXPECTED_SCHEDULE_EXPRESSION,
        "stack schedule expression drift",
    )
    _require(
        parameters.get("ScheduleTimezone") == EXPECTED_SCHEDULE_TIMEZONE,
        "stack schedule timezone drift",
    )
    _require(outputs.get("ScheduleName") == EXPECTED_SCHEDULE_NAME, "stack schedule output drift")
    _require(outputs.get("ScheduleState") == "ENABLED", "stack enabled output drift")
    _require(outputs.get("TargetTaskDefinitionArn") == expected_task_arn, "stack task output drift")
    scheduler_role_arn = outputs.get("SchedulerRoleArn", "")
    dlq_arn = outputs.get("DeadLetterQueueArn", "")
    _require(scheduler_role_arn.endswith("/vevo-growthbook-reconcile-preview-scheduler"), "scheduler role drift")
    _require(dlq_arn.endswith(":vevo-growthbook-reconcile-preview-dlq"), "DLQ output drift")

    schedule = schedule_payload
    _require(schedule.get("Name") == EXPECTED_SCHEDULE_NAME, "schedule identity drift")
    _require(schedule.get("GroupName", "default") == "default", "schedule group drift")
    _require(schedule.get("State") == "ENABLED", "schedule is not enabled")
    _require(schedule.get("ScheduleExpression") == EXPECTED_SCHEDULE_EXPRESSION, "schedule expression drift")
    _require(schedule.get("ScheduleExpressionTimezone") == EXPECTED_SCHEDULE_TIMEZONE, "schedule timezone drift")
    _require(schedule.get("FlexibleTimeWindow") == {"Mode": "OFF"}, "schedule flexible window drift")
    target = schedule.get("Target") or {}
    _require(isinstance(target, dict), "schedule target is missing")
    _require(target.get("Arn") == parameters.get("ClusterArn"), "schedule cluster drift")
    _require(target.get("RoleArn") == scheduler_role_arn, "schedule role drift")
    _require((target.get("DeadLetterConfig") or {}).get("Arn") == dlq_arn, "schedule DLQ drift")
    _require(
        target.get("RetryPolicy")
        == {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 2},
        "schedule retry policy drift",
    )
    ecs = target.get("EcsParameters") or {}
    _require(isinstance(ecs, dict), "schedule ECS target is missing")
    _require(ecs.get("TaskDefinitionArn") == expected_task_arn, "schedule task definition drift")
    _require(ecs.get("LaunchType") == "FARGATE", "schedule launch type drift")
    _require(ecs.get("TaskCount") == 1, "schedule task count drift")
    _require(ecs.get("Group") == EXPECTED_SCHEDULE_NAME, "schedule ECS group drift")
    _require(ecs.get("EnableExecuteCommand") is False, "schedule execute-command must be disabled")
    network = ((ecs.get("NetworkConfiguration") or {}).get("awsvpcConfiguration") or {})
    _require(len(network.get("Subnets") or []) >= 2, "schedule subnet boundary drift")
    _require(bool(network.get("SecurityGroups") or []), "schedule security-group boundary drift")
    _require(network.get("AssignPublicIp") == parameters.get("AssignPublicIp"), "public-IP boundary drift")
    try:
        schedule_input = json.loads(str(target.get("Input") or "{}"))
    except json.JSONDecodeError as exc:
        raise VerificationError("schedule input is invalid JSON") from exc
    expected_override = _normalized_override(schedule_input)
    _require(expected_override["command"] == EXPECTED_COMMAND, "scheduled command drift")
    _require(
        expected_override["environment"]
        == {
            "REPORT_PROJECT": "vevo",
            "GROWTHBOOK_ENVIRONMENT": "preview",
            "GROWTHBOOK_EVENT_BUCKET": parameters.get("EventBucketName", ""),
            "GROWTHBOOK_FACT_PUBLISH_ENABLED": "true",
            "AWS_REGION": EXPECTED_REGION,
        },
        "scheduled environment drift",
    )

    task_definition = task_definition_payload.get("taskDefinition") or {}
    _require(isinstance(task_definition, dict), "task definition is missing")
    _require(task_definition.get("taskDefinitionArn") == expected_task_arn, "task definition ARN drift")
    _require(task_definition.get("family") == "vevo-growthbook-reconcile-preview", "task family drift")
    _require(task_definition.get("revision") == 4, "task definition revision drift")
    _require(
        task_definition.get("taskRoleArn")
        == f"arn:aws:iam::{EXPECTED_ACCOUNT_ID}:role/BiznisWebReportingTaskRole-vevo",
        "task role drift",
    )
    containers = task_definition.get("containerDefinitions") or []
    _require(isinstance(containers, list) and len(containers) == 1, "one task container is required")
    container_definition = containers[0]
    _require(container_definition.get("name") == expected_override["name"], "container name drift")
    _require(container_definition.get("command") == EXPECTED_COMMAND, "task command drift")
    image = str(container_definition.get("image") or "")
    _require(image.endswith(f"@{EXPECTED_IMAGE_DIGEST}"), "task image digest drift")
    log_options = ((container_definition.get("logConfiguration") or {}).get("options") or {})
    log_group = str(log_options.get("awslogs-group") or "")
    log_prefix = str(log_options.get("awslogs-stream-prefix") or "")
    _require(log_group.startswith("/ecs/") and bool(log_prefix), "task log boundary drift")

    marker_events = _messages(marker_events_payload)
    successes = [
        row for row in marker_events if "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:" in str(row.get("message") or "")
    ]
    failures = [
        row for row in marker_events if "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE:" in str(row.get("message") or "")
    ]
    _require(not failures, "natural-run window contains a failure marker")
    _require(len(successes) == 1, "natural-run window must contain exactly one success marker")
    success_event = successes[0]
    success_time = datetime.fromtimestamp(int(success_event.get("timestamp", 0)) / 1000, tz=timezone.utc)
    _require(success_time >= FIRST_RUN_DUE_UTC, "success marker predates the first natural run")
    _require(success_time <= now_utc, "success marker is in the future")
    success_text = str(success_event.get("message") or "")
    marker = SUCCESS_RE.search(success_text)
    _require(marker is not None, "natural-run success marker schema drift")
    _require(marker.group("event_from") == EXPECTED_EVENT_FROM, "natural event-from drift")
    _require(marker.group("event_through") == EXPECTED_EVENT_THROUGH, "natural event-through drift")
    _require(int(marker.group("partitions")) == EXPECTED_PARTITIONS, "natural partition count drift")
    log_stream = str(success_event.get("logStreamName") or "")
    _require(log_stream.startswith(f"{log_prefix}/"), "natural log stream prefix drift")
    task_id = log_stream.rsplit("/", 1)[-1]
    _require(TASK_ID_RE.fullmatch(task_id) is not None, "natural task ID is invalid")

    tasks = task_state_payload.get("tasks") or []
    _require(not (task_state_payload.get("failures") or []), "ECS task read-back contains failures")
    _require(isinstance(tasks, list) and len(tasks) == 1, "exact natural ECS task is required")
    task = tasks[0]
    _require(str(task.get("taskArn") or "").endswith(f"/{task_id}"), "natural task ARN drift")
    _require(task.get("taskDefinitionArn") == expected_task_arn, "natural task definition drift")
    _require(task.get("group") == EXPECTED_SCHEDULE_NAME, "natural task group drift")
    _require(task.get("launchType") == "FARGATE", "natural task launch type drift")
    _require(task.get("enableExecuteCommand") is False, "natural task execute-command drift")
    _require(task.get("lastStatus") == "STOPPED", "natural task has not stopped")
    _require(task.get("desiredStatus") == "STOPPED", "natural task desired status drift")
    _require(task.get("stopCode") == "EssentialContainerExited", "natural task stop code drift")
    started_by = str(task.get("startedBy") or "")
    _require(not started_by.startswith("vevo-growthbook-reconcile-once-"), "manual one-shot cannot prove natural run")
    started_at = _parse_utc(task.get("startedAt"), "task.startedAt")
    stopped_at = _parse_utc(task.get("stoppedAt"), "task.stoppedAt")
    _require(FIRST_RUN_DUE_UTC <= started_at <= stopped_at <= now_utc, "natural task timing drift")
    actual_override = _normalized_override(task.get("overrides") or {})
    _require(actual_override == expected_override, "natural task overrides drift")
    task_containers = task.get("containers") or []
    _require(isinstance(task_containers, list) and len(task_containers) == 1, "natural task container missing")
    task_container = task_containers[0]
    _require(task_container.get("exitCode") == 0, "natural task exit code is not zero")
    _require(task_container.get("imageDigest") == EXPECTED_IMAGE_DIGEST, "natural task image digest drift")
    _require(task_container.get("logStreamName") == log_stream, "natural task log stream drift")
    details = [
        row
        for attachment in task.get("attachments") or []
        if isinstance(attachment, dict)
        for row in attachment.get("details") or []
        if isinstance(row, dict)
    ]
    private_ip = next(
        (str(row.get("value") or "") for row in details if row.get("name") == "privateIPv4Address"),
        "",
    )
    _require(bool(re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", private_ip)), "natural private IP missing")

    task_messages = _messages(task_logs_payload)
    task_successes = [
        row for row in task_messages if "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:" in str(row.get("message") or "")
    ]
    task_failures = [
        row for row in task_messages if "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE:" in str(row.get("message") or "")
    ]
    _require(len(task_successes) == 1 and not task_failures, "natural task marker set drift")
    summaries: list[Mapping[str, Any]] = []
    for row in task_messages:
        text = str(row.get("message") or "").strip()
        if text.startswith("{") and '"mode":"publish"' in text:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError as exc:
                raise VerificationError("natural publish summary is invalid JSON") from exc
            if isinstance(parsed, dict):
                summaries.append(parsed)
    _require(len(summaries) == 1, "exact natural publish summary is required")
    summary = summaries[0]
    raw_events = summary.get("raw_events")
    _require(summary.get("mode") == "publish", "natural reconciliation was not publishing")
    _require(summary.get("event_partitions") == EXPECTED_PARTITIONS, "natural summary partition drift")
    _require(type(raw_events) is int and 0 <= raw_events <= MAX_RAW_EVENTS, "natural raw-event bound drift")
    for key in ("device_facts", "performance_facts", "quality_reports"):
        _require(type(summary.get(key)) is int and summary[key] >= 0, f"natural {key} count drift")
    expected_published = {
        "device_facts": summary["device_facts"],
        "performance_facts": summary["performance_facts"],
        "quality_reports": summary["quality_reports"],
    }
    _require(summary.get("published") == expected_published, "natural generated/published counts differ")

    alarm_rows = alarms_payload.get("MetricAlarms") or []
    _require(isinstance(alarm_rows, list), "alarm read-back must be a list")
    _require({row.get("AlarmName") for row in alarm_rows} == EXPECTED_ALARMS, "reconciliation alarm set drift")
    _require(
        all(row.get("StateValue") in {"OK", "INSUFFICIENT_DATA"} for row in alarm_rows),
        "a reconciliation alarm is active",
    )
    attributes = dlq_payload.get("Attributes") or {}
    _require(attributes.get("SqsManagedSseEnabled") == "true", "reconciliation DLQ encryption drift")
    for key in (
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    ):
        _require(attributes.get(key) == "0", f"reconciliation DLQ is not empty: {key}")

    source = source_schedule_payload
    _require(source.get("Name") == EXPECTED_SOURCE_SCHEDULE, "source schedule identity drift")
    _require(source.get("State") == "ENABLED", "source reporting schedule is not enabled")
    source_task_definition = str(
        (((source.get("Target") or {}).get("EcsParameters") or {}).get("TaskDefinitionArn") or "")
    )
    _require(
        f"task-definition/{EXPECTED_SOURCE_TASK_FAMILY}:" in source_task_definition,
        "source reporting task family drift",
    )

    lookup_events = cloudtrail_payload.get("Events") or []
    _require(isinstance(lookup_events, list), "CloudTrail events must be a list")
    matching_cloudtrail: list[Mapping[str, Any]] = []
    for row in lookup_events:
        if not isinstance(row, dict):
            continue
        detail = _cloudtrail_detail(row)
        if detail is None:
            continue
        serialized = json.dumps(detail, separators=(",", ":"), sort_keys=True)
        identity = detail.get("userIdentity") or {}
        issuer = ((identity.get("sessionContext") or {}).get("sessionIssuer") or {})
        request = detail.get("requestParameters") or {}
        task_definition_request = str(request.get("taskDefinition") or "")
        scheduler_identity = (
            identity.get("invokedBy") == "scheduler.amazonaws.com"
            or issuer.get("arn") == scheduler_role_arn
        )
        if (
            detail.get("eventSource") == "ecs.amazonaws.com"
            and detail.get("eventName") == "RunTask"
            and task_id in serialized
            and scheduler_identity
            and request.get("group") == EXPECTED_SCHEDULE_NAME
            and (
                task_definition_request == expected_task_arn
                or task_definition_request == EXPECTED_TASK_DEFINITION
            )
        ):
            matching_cloudtrail.append(detail)
    _require(len(matching_cloudtrail) == 1, "exact Scheduler CloudTrail RunTask evidence is required")

    return {
        "task_id": task_id,
        "private_ip": private_ip,
        "service": EXPECTED_SCHEDULE_NAME,
        "runtime_path": EXPECTED_RUNTIME_PATH,
        "task_definition": EXPECTED_TASK_DEFINITION,
        "image_digest": EXPECTED_IMAGE_DIGEST,
        "event_from": EXPECTED_EVENT_FROM,
        "event_through": EXPECTED_EVENT_THROUGH,
        "raw_events": raw_events,
        "device_facts": summary["device_facts"],
        "performance_facts": summary["performance_facts"],
        "quality_reports": summary["quality_reports"],
        "generated_published_counts_match": True,
        "dlq_empty": True,
        "alarms_clear": True,
        "source_schedule_enabled": True,
        "cloudtrail_scheduler_run_task_verified": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--stack", required=True)
    parser.add_argument("--task-definition", required=True)
    parser.add_argument("--marker-events", required=True)
    parser.add_argument("--task-state", required=True)
    parser.add_argument("--task-logs", required=True)
    parser.add_argument("--alarms", required=True)
    parser.add_argument("--dlq", required=True)
    parser.add_argument("--source-schedule", required=True)
    parser.add_argument("--cloudtrail", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = verify_natural_reconciliation(
        schedule_payload=_load(args.schedule),
        stack_payload=_load(args.stack),
        task_definition_payload=_load(args.task_definition),
        marker_events_payload=_load(args.marker_events),
        task_state_payload=_load(args.task_state),
        task_logs_payload=_load(args.task_logs),
        alarms_payload=_load(args.alarms),
        dlq_payload=_load(args.dlq),
        source_schedule_payload=_load(args.source_schedule),
        cloudtrail_payload=_load(args.cloudtrail),
        now=datetime.now(timezone.utc),
    )
    print(
        "NATURAL_RUNTIME_IDENTITY_OK:"
        f"instance-id=N/A:Fargate:private-ip={result['private_ip']}:"
        f"service={result['service']}:path={result['runtime_path']}:task={result['task_id']}"
    )
    print(
        "GROWTHBOOK_NATURAL_RECONCILIATION_OK:"
        + json.dumps(result, separators=(",", ":"), sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
