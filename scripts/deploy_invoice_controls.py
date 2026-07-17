#!/usr/bin/env python3
"""Deploy and verify the shared VEVO/ROY invoice automation safety controls."""

from __future__ import annotations

import argparse
import copy
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import boto3
from botocore.exceptions import ClientError


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PROJECTS = ("vevo", "roy")
DEFAULT_REGION = "eu-central-1"
DEFAULT_ALERT_TOPIC_NAME = "vevo-reporting-alerts-mil-final"
DEFAULT_NAMESPACE = "BizniswebReporting"
EXPECTED_AUTOMATION_START_DATE = "2026-07-17"
DLQ_RETENTION_SECONDS = 14 * 24 * 60 * 60
LOG_RETENTION_DAYS = 90
HEARTBEAT_PERIOD_SECONDS = 60 * 60
HEARTBEAT_EVALUATION_PERIODS = 27
HEARTBEAT_DATAPOINTS_TO_ALARM = 27
FAILURE_FILTER_PATTERN = '"INVOICE_RUN_FAILED"'
TASK_DEFINITION_DIGEST_TAG = "InvoiceImageDigest"


@dataclass(frozen=True)
class InvoiceControlConfig:
    project: str
    primary_schedule_name: str
    primary_schedule_expression: str
    final_schedule_name: str
    final_schedule_expression: str
    timezone: str
    task_family: str

    @property
    def queue_name(self) -> str:
        return f"{self.project}-invoice-scheduler-dlq"

    @property
    def failure_metric_name(self) -> str:
        suffix = re.sub(r"[^A-Za-z0-9]", "", self.project.title())
        return f"InvoiceApplicationRunFailed{suffix}"

    @property
    def application_alarm_name(self) -> str:
        return f"invoice-{self.project}-application-run-failed"

    @property
    def heartbeat_alarm_name(self) -> str:
        return f"invoice-{self.project}-reconciliation-heartbeat-missing"

    @property
    def dlq_alarm_name(self) -> str:
        return f"invoice-{self.project}-scheduler-dlq-not-empty"

    @property
    def metric_filter_name(self) -> str:
        return f"invoice-{self.project}-run-failed"


@dataclass(frozen=True)
class InvoiceRuntime:
    container_name: str
    log_group: str
    task_definition_arn: str
    image: str


@dataclass(frozen=True)
class ProjectDeployment:
    config: InvoiceControlConfig
    primary_schedule: Dict[str, Any]
    final_schedule: Dict[str, Any]
    runtime: InvoiceRuntime
    queue_url: str
    queue_arn: str


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy or verify VEVO/ROY invoice Scheduler, DLQ, log, and alarm controls."
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--apply", action="store_true", help="Apply controls idempotently, then verify them.")
    action.add_argument("--verify", action="store_true", help="Read-only verification of existing controls.")
    action.add_argument(
        "--arm-bootstrap-failure-alerts",
        action="store_true",
        help=(
            "Enable heartbeat alarms for any project left with a disabled "
            "invoice schedule after an interrupted deployment."
        ),
    )
    parser.add_argument("--projects", nargs="+", default=list(DEFAULT_PROJECTS))
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--profile", default="")
    parser.add_argument("--alert-topic-name", default=DEFAULT_ALERT_TOPIC_NAME)
    parser.add_argument(
        "--image-digest",
        default="",
        help="Immutable sha256 ECR digest to pin in newly registered invoice task definitions.",
    )
    parser.add_argument(
        "--defer-heartbeat-alarm",
        action="store_true",
        help=(
            "Apply controls with the final schedule disabled and without the "
            "missing-heartbeat alarm. Use before a guarded first live reconciliation."
        ),
    )
    parser.add_argument(
        "--resume-deferred-bootstrap",
        action="store_true",
        help=(
            "Complete a previously deferred deployment using its immutable "
            "image digest even if the mutable latest tag has since advanced."
        ),
    )
    return parser.parse_args(argv)


def load_control_config(project: str) -> InvoiceControlConfig:
    settings_path = ROOT_DIR / "projects" / project / "settings.json"
    settings = json.loads(settings_path.read_text(encoding="utf-8"))
    invoice = settings.get("invoice_generation") or {}
    required = (
        "schedule_name",
        "schedule_expression",
        "final_sweep_schedule_name",
        "final_sweep_schedule_expression",
        "task_family",
        "automation_start_date",
    )
    missing = [key for key in required if not str(invoice.get(key) or "").strip()]
    if missing:
        raise ValueError(f"{project}: missing invoice_generation settings: {missing}")
    if not bool(invoice.get("enabled")):
        raise ValueError(f"{project}: invoice_generation must be enabled")
    if not bool(invoice.get("require_cod_payment")):
        raise ValueError(f"{project}: require_cod_payment must be enabled")
    if str(invoice.get("automation_start_date") or "") != EXPECTED_AUTOMATION_START_DATE:
        raise ValueError(
            f"{project}: automation_start_date must be "
            f"{EXPECTED_AUTOMATION_START_DATE}"
        )
    return InvoiceControlConfig(
        project=project,
        primary_schedule_name=str(invoice["schedule_name"]),
        primary_schedule_expression=str(invoice["schedule_expression"]),
        final_schedule_name=str(invoice["final_sweep_schedule_name"]),
        final_schedule_expression=str(invoice["final_sweep_schedule_expression"]),
        timezone=str(invoice.get("timezone") or "Europe/Bratislava"),
        task_family=str(invoice["task_family"]),
    )


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        result: Dict[str, Any] = {}
        for key, raw in value.items():
            cleaned = _clean(raw)
            if cleaned in (None, "", [], {}):
                continue
            result[key] = cleaned
        return result
    if isinstance(value, list):
        result = []
        for raw in value:
            cleaned = _clean(raw)
            if cleaned in (None, "", [], {}):
                continue
            result.append(cleaned)
        return result
    return value


def expected_invoice_command(project: str, *, reconcile: bool) -> List[str]:
    command = ["python", "invoice_runner.py", "--project", project]
    if reconcile:
        command.append("--reconcile")
    return command


def build_schedule_target(
    source_target: Mapping[str, Any],
    *,
    queue_arn: str,
    container_name: str,
    project: str,
    reconcile: bool,
    task_definition_arn: str = "",
) -> Dict[str, Any]:
    allowed_keys = (
        "Arn",
        "RoleArn",
        "RetryPolicy",
        "EcsParameters",
    )
    target = {
        key: copy.deepcopy(source_target[key])
        for key in allowed_keys
        if key in source_target
    }
    target["DeadLetterConfig"] = {"Arn": queue_arn}
    if task_definition_arn:
        target["EcsParameters"]["TaskDefinitionArn"] = task_definition_arn
    target["Input"] = json.dumps(
        {
            "containerOverrides": [
                {
                    "name": container_name,
                    "command": expected_invoice_command(project, reconcile=reconcile),
                }
            ]
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return _clean(target)


def build_queue_policy(queue_arn: str, schedule_arns: Iterable[str]) -> Dict[str, Any]:
    sources = sorted({str(value) for value in schedule_arns if str(value)})
    if len(sources) != 2:
        raise ValueError(f"Expected two distinct invoice schedule ARNs, got {sources}")
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowInvoiceSchedulers",
                "Effect": "Allow",
                "Principal": {"Service": "scheduler.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {"ArnEquals": {"aws:SourceArn": sources}},
            }
        ],
    }


def build_queue_policy_for_account(
    queue_arn: str,
    schedule_arns: Iterable[str],
    account_id: str,
) -> Dict[str, Any]:
    policy = build_queue_policy(queue_arn, schedule_arns)
    condition = policy["Statement"][0]["Condition"]
    condition["StringEquals"] = {"aws:SourceAccount": str(account_id)}
    return policy


def pinned_image_uri(image: str, image_digest: str) -> str:
    digest = str(image_digest or "").strip()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError(f"Invalid ECR image digest: {image_digest!r}")
    base = str(image or "").split("@", 1)[0]
    last_segment = base.rsplit("/", 1)[-1]
    if ":" in last_segment:
        base = base.rsplit(":", 1)[0]
    if not base:
        raise ValueError(f"Invalid task image URI: {image!r}")
    return f"{base}@{digest}"


def deployment_validation_image(image: str, image_digest: str) -> str:
    """Resolve the desired digest even when the current runtime is pinned older."""
    return pinned_image_uri(image, image_digest)


def task_definition_family(task_definition_arn: str) -> str:
    value = str(task_definition_arn or "").strip()
    marker = "task-definition/"
    if marker not in value:
        raise ValueError(
            f"Unexpected ECS task definition ARN: {task_definition_arn}"
        )
    family_and_revision = value.split(marker, 1)[1]
    family = family_and_revision.rsplit(":", 1)[0].strip()
    if not family:
        raise ValueError(
            f"Unexpected ECS task definition ARN: {task_definition_arn}"
        )
    return family


def parse_ecr_image_uri(image: str) -> tuple[str, str, str, str]:
    normalized = str(image or "").strip()
    if "/" not in normalized:
        raise ValueError(f"Invalid ECR task image URI: {image!r}")
    registry, repository_and_reference = normalized.split("/", 1)
    registry_match = re.fullmatch(
        r"(?P<account>[0-9]{12})\.dkr\.ecr\."
        r"(?P<region>[a-z0-9-]+)\.amazonaws\.com(?:\.cn)?",
        registry,
    )
    if not registry_match:
        raise ValueError(f"Invalid ECR registry in task image URI: {image!r}")
    digest = ""
    if "@" in repository_and_reference:
        repository, digest = repository_and_reference.rsplit("@", 1)
    else:
        repository = repository_and_reference
        last_segment = repository.rsplit("/", 1)[-1]
        if ":" in last_segment:
            repository = repository.rsplit(":", 1)[0]
    if digest and not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError(f"Invalid ECR digest in task image URI: {image!r}")
    last_segment = repository.rsplit("/", 1)[-1]
    if not repository or ":" in last_segment:
        raise ValueError(f"Invalid ECR task image URI: {image!r}")
    return (
        registry_match.group("account"),
        registry_match.group("region"),
        repository,
        digest,
    )


def ecr_repository_name(image: str) -> str:
    return parse_ecr_image_uri(image)[2]


def build_failure_metric_filter(config: InvoiceControlConfig) -> Dict[str, Any]:
    return {
        "filterPattern": FAILURE_FILTER_PATTERN,
        "metricTransformations": [
            {
                "metricName": config.failure_metric_name,
                "metricNamespace": DEFAULT_NAMESPACE,
                "metricValue": "1",
            }
        ],
    }


def build_dlq_alarm(
    config: InvoiceControlConfig,
    alert_topic_arn: str,
) -> Dict[str, Any]:
    return {
        "AlarmName": config.dlq_alarm_name,
        "AlarmDescription": (
            f"{config.project}: EventBridge Scheduler could not invoke an "
            "invoice Fargate target"
        ),
        "ActionsEnabled": True,
        "AlarmActions": [alert_topic_arn],
        "OKActions": [alert_topic_arn],
        "Namespace": "AWS/SQS",
        "MetricName": "ApproximateNumberOfMessagesVisible",
        "Dimensions": [{"Name": "QueueName", "Value": config.queue_name}],
        "Statistic": "Maximum",
        "Period": 60,
        "EvaluationPeriods": 1,
        "DatapointsToAlarm": 1,
        "Threshold": 1.0,
        "ComparisonOperator": "GreaterThanOrEqualToThreshold",
        "TreatMissingData": "notBreaching",
    }


def build_application_alarm(
    config: InvoiceControlConfig,
    alert_topic_arn: str,
) -> Dict[str, Any]:
    return {
        "AlarmName": config.application_alarm_name,
        "AlarmDescription": (
            f"{config.project}: invoice application scan, creation, or email "
            "step failed"
        ),
        "ActionsEnabled": True,
        "AlarmActions": [alert_topic_arn],
        "OKActions": [alert_topic_arn],
        "Namespace": DEFAULT_NAMESPACE,
        "MetricName": config.failure_metric_name,
        "Statistic": "Sum",
        "Period": 300,
        "EvaluationPeriods": 1,
        "DatapointsToAlarm": 1,
        "Threshold": 1.0,
        "ComparisonOperator": "GreaterThanOrEqualToThreshold",
        "TreatMissingData": "notBreaching",
    }


def build_heartbeat_alarm(
    config: InvoiceControlConfig,
    alert_topic_arn: str,
) -> Dict[str, Any]:
    return {
        "AlarmName": config.heartbeat_alarm_name,
        "AlarmDescription": (
            f"{config.project}: no successful live invoice reconciliation in "
            "27 hours"
        ),
        "ActionsEnabled": True,
        "AlarmActions": [alert_topic_arn],
        "OKActions": [alert_topic_arn],
        "Namespace": DEFAULT_NAMESPACE,
        "MetricName": "InvoiceReconciliationRunSucceeded",
        "Dimensions": [{"Name": "Project", "Value": config.project}],
        "Statistic": "Maximum",
        "Period": HEARTBEAT_PERIOD_SECONDS,
        "EvaluationPeriods": HEARTBEAT_EVALUATION_PERIODS,
        "DatapointsToAlarm": HEARTBEAT_DATAPOINTS_TO_ALARM,
        "Threshold": 1.0,
        "ComparisonOperator": "LessThanThreshold",
        "TreatMissingData": "breaching",
    }


def assert_alarm_matches(
    alarm: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> None:
    mismatches = {
        key: (alarm.get(key), value)
        for key, value in expected.items()
        if alarm.get(key) != value
    }
    if mismatches:
        raise AssertionError(
            f"{expected['AlarmName']}: alarm drift {mismatches}"
        )
    for key in (
        "Dimensions",
        "Unit",
        "Metrics",
        "ExtendedStatistic",
        "InsufficientDataActions",
    ):
        if key in expected:
            continue
        if alarm.get(key) not in (None, []):
            raise AssertionError(
                f"{expected['AlarmName']}: unexpected {key}="
                f"{alarm.get(key)!r}"
            )


def build_task_definition_registration(
    task_definition: Mapping[str, Any],
    *,
    container_name: str,
    image_digest: str,
    tags: Optional[Sequence[Mapping[str, str]]] = None,
) -> Dict[str, Any]:
    allowed_keys = (
        "family",
        "taskRoleArn",
        "executionRoleArn",
        "networkMode",
        "containerDefinitions",
        "volumes",
        "placementConstraints",
        "requiresCompatibilities",
        "cpu",
        "memory",
        "pidMode",
        "ipcMode",
        "proxyConfiguration",
        "inferenceAccelerators",
        "ephemeralStorage",
        "runtimePlatform",
        "enableFaultInjection",
    )
    request = {
        key: copy.deepcopy(task_definition[key])
        for key in allowed_keys
        if task_definition.get(key) not in (None, "")
    }
    containers = request.get("containerDefinitions") or []
    matches = [
        container
        for container in containers
        if container.get("name") == container_name
    ]
    if len(matches) != 1:
        raise ValueError(
            f"Expected one task container named {container_name!r}, got {len(matches)}"
        )
    matches[0]["image"] = pinned_image_uri(matches[0]["image"], image_digest)
    if tags:
        request["tags"] = [dict(tag) for tag in tags]
    return request


def build_scheduler_dlq_policy(queue_arns: Iterable[str]) -> Dict[str, Any]:
    resources = sorted({str(value) for value in queue_arns if str(value)})
    if not resources:
        raise ValueError("At least one invoice DLQ ARN is required")
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "SendInvoiceSchedulerFailuresToDlq",
                "Effect": "Allow",
                "Action": "sqs:SendMessage",
                "Resource": resources,
            }
        ],
    }


def _role_name(role_arn: str) -> str:
    if ":role/" not in role_arn:
        raise ValueError(f"Unexpected Scheduler role ARN: {role_arn}")
    return role_arn.rsplit("/", 1)[-1]


def _schedule_update_request(
    schedule: Mapping[str, Any],
    *,
    expression: str,
    timezone: str,
    target: Mapping[str, Any],
    state: str = "ENABLED",
) -> Dict[str, Any]:
    request: Dict[str, Any] = {
        "Name": schedule["Name"],
        "GroupName": schedule.get("GroupName") or "default",
        "ScheduleExpression": expression,
        "ScheduleExpressionTimezone": timezone,
        "FlexibleTimeWindow": copy.deepcopy(
            schedule.get("FlexibleTimeWindow") or {"Mode": "OFF"}
        ),
        "Target": copy.deepcopy(dict(target)),
        "State": state,
    }
    for key in (
        "Description",
        "StartDate",
        "EndDate",
        "KmsKeyArn",
        "ActionAfterCompletion",
    ):
        if schedule.get(key) not in (None, ""):
            request[key] = schedule[key]
    return request


class InvoiceControlDeployer:
    def __init__(
        self,
        *,
        region: str,
        profile: str = "",
        alert_topic_name: str = DEFAULT_ALERT_TOPIC_NAME,
    ) -> None:
        session_kwargs: Dict[str, Any] = {"region_name": region}
        if profile:
            session_kwargs["profile_name"] = profile
        self.session = boto3.Session(**session_kwargs)
        self.region = region
        self.scheduler = self.session.client("scheduler")
        self.ecs = self.session.client("ecs")
        self.ecr = self.session.client("ecr")
        self.sqs = self.session.client("sqs")
        self.iam = self.session.client("iam")
        self.logs = self.session.client("logs")
        self.cloudwatch = self.session.client("cloudwatch")
        self.sns = self.session.client("sns")
        self.sts = self.session.client("sts")
        self.account_id = self.sts.get_caller_identity()["Account"]
        self.alert_topic_arn = (
            f"arn:aws:sns:{region}:{self.account_id}:{alert_topic_name}"
        )

    def _get_schedule(self, name: str) -> Dict[str, Any]:
        return self.scheduler.get_schedule(Name=name)

    def _list_active_invoice_tasks(
        self,
        *,
        cluster_arn: str,
        task_family: str,
    ) -> set[str]:
        active: set[str] = set()
        for desired_status in ("PENDING", "RUNNING"):
            next_token = ""
            while True:
                request: Dict[str, Any] = {
                    "cluster": cluster_arn,
                    "family": task_family,
                    "desiredStatus": desired_status,
                }
                if next_token:
                    request["nextToken"] = next_token
                response = self.ecs.list_tasks(**request)
                active.update(
                    str(task_arn)
                    for task_arn in (response.get("taskArns") or [])
                    if str(task_arn or "").strip()
                )
                next_token = str(response.get("nextToken") or "")
                if not next_token:
                    break
        return active

    def _wait_for_invoice_tasks_to_quiesce(
        self,
        current: Sequence[
            tuple[
                InvoiceControlConfig,
                Mapping[str, Any],
                Mapping[str, Any],
                InvoiceRuntime,
            ]
        ],
        *,
        timeout_seconds: int = 600,
        poll_seconds: int = 5,
        stable_empty_polls: int = 18,
    ) -> None:
        targets: set[tuple[str, str]] = set()
        for _config, primary, final, _runtime in current:
            for schedule in (primary, final):
                target = schedule.get("Target") or {}
                cluster_arn = str(target.get("Arn") or "").strip()
                ecs_parameters = target.get("EcsParameters") or {}
                family = task_definition_family(
                    str(ecs_parameters.get("TaskDefinitionArn") or "")
                )
                if not cluster_arn:
                    raise ValueError(
                        f"{schedule.get('Name')}: missing ECS cluster ARN"
                    )
                targets.add((cluster_arn, family))

        deadline = time.monotonic() + max(1, int(timeout_seconds))
        consecutive_empty = 0
        stop_requested: set[str] = set()
        while True:
            active: set[str] = set()
            for cluster_arn, family in sorted(targets):
                target_active = self._list_active_invoice_tasks(
                    cluster_arn=cluster_arn,
                    task_family=family,
                )
                active.update(target_active)
                for task_arn in sorted(target_active - stop_requested):
                    self.ecs.stop_task(
                        cluster=cluster_arn,
                        task=task_arn,
                        reason=(
                            "Quiesce old invoice automation before "
                            "cutoff-protected deployment"
                        ),
                    )
                    stop_requested.add(task_arn)
                    print(
                        "INVOICE_TASK_STOP_REQUESTED "
                        f"task={task_arn}"
                    )
            if active:
                consecutive_empty = 0
                print(
                    "INVOICE_TASK_DRAIN_WAIT "
                    f"active={len(active)}"
                )
            else:
                consecutive_empty += 1
                if consecutive_empty >= max(1, int(stable_empty_polls)):
                    print(
                        "INVOICE_TASKS_QUIESCED "
                        f"stable_polls={consecutive_empty}"
                    )
                    return
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "Invoice tasks did not quiesce before deployment timeout"
                )
            time.sleep(max(0, int(poll_seconds)))

    def _assert_ecr_digest_exists(
        self,
        image: str,
        image_digest: str,
        *,
        require_latest: bool,
    ) -> None:
        registry_id, image_region, repository, embedded_digest = (
            parse_ecr_image_uri(image)
        )
        if registry_id != self.account_id or image_region != self.region:
            raise ValueError(
                "Invoice task image registry drift: "
                f"account={registry_id}, region={image_region}, "
                f"expected_account={self.account_id}, "
                f"expected_region={self.region}"
            )
        if embedded_digest and embedded_digest != image_digest:
            raise ValueError(
                f"Invoice task image digest {embedded_digest} does not match "
                f"expected {image_digest}"
            )
        image_details = self.ecr.describe_images(
            registryId=registry_id,
            repositoryName=repository,
            imageIds=[{"imageDigest": image_digest}],
        ).get("imageDetails") or []
        matching = [
            detail
            for detail in image_details
            if detail.get("imageDigest") == image_digest
        ]
        if not matching:
            raise ValueError(
                f"ECR digest {image_digest} is missing from "
                f"{registry_id}/{repository}"
            )
        if require_latest and not any(
            "latest" in (detail.get("imageTags") or [])
            for detail in matching
        ):
            raise ValueError(
                f"ECR latest no longer points to {image_digest} in "
                f"{registry_id}/{repository}"
            )

    def _runtime_from_schedule(
        self,
        config: InvoiceControlConfig,
        schedule: Mapping[str, Any],
    ) -> InvoiceRuntime:
        target = schedule.get("Target") or {}
        ecs_parameters = target.get("EcsParameters") or {}
        task_definition_arn = str(ecs_parameters.get("TaskDefinitionArn") or "")
        if f":task-definition/{config.task_family}:" not in task_definition_arn:
            raise ValueError(
                f"{config.project}: {schedule.get('Name')} targets "
                f"{task_definition_arn}, expected family {config.task_family}"
            )
        task = self.ecs.describe_task_definition(
            taskDefinition=task_definition_arn
        )["taskDefinition"]
        containers = task.get("containerDefinitions") or []
        if len(containers) != 1:
            raise ValueError(
                f"{config.project}: expected exactly one invoice container, got {len(containers)}"
            )
        container = containers[0]
        log_options = (
            (container.get("logConfiguration") or {}).get("options") or {}
        )
        log_group = str(log_options.get("awslogs-group") or "")
        if not log_group:
            raise ValueError(f"{config.project}: invoice task has no awslogs group")
        return InvoiceRuntime(
            container_name=str(container["name"]),
            log_group=log_group,
            task_definition_arn=task_definition_arn,
            image=str(container.get("image") or ""),
        )

    def _ensure_pinned_task_definition(
        self,
        runtime: InvoiceRuntime,
        *,
        image_digest: str,
    ) -> InvoiceRuntime:
        expected_image = pinned_image_uri(runtime.image, image_digest)
        if runtime.image == expected_image:
            return runtime
        response = self.ecs.describe_task_definition(
            taskDefinition=runtime.task_definition_arn,
            include=["TAGS"],
        )
        source_definition = response["taskDefinition"]
        source_tags = [
            dict(tag)
            for tag in (response.get("tags") or [])
            if tag.get("key") != TASK_DEFINITION_DIGEST_TAG
        ]
        source_tags.append(
            {
                "key": TASK_DEFINITION_DIGEST_TAG,
                "value": image_digest,
            }
        )
        registration = build_task_definition_registration(
            source_definition,
            container_name=runtime.container_name,
            image_digest=image_digest,
            tags=source_tags,
        )
        comparable_registration = {
            key: value
            for key, value in registration.items()
            if key != "tags"
        }
        family = str(source_definition.get("family") or "")
        if not family:
            raise ValueError(
                f"Task definition {runtime.task_definition_arn} has no family"
            )
        candidates = self.ecs.list_task_definitions(
            familyPrefix=family,
            status="ACTIVE",
            sort="DESC",
            maxResults=100,
        ).get("taskDefinitionArns") or []
        for candidate_arn in candidates:
            if candidate_arn == runtime.task_definition_arn:
                continue
            candidate_response = self.ecs.describe_task_definition(
                taskDefinition=candidate_arn,
                include=["TAGS"],
            )
            candidate_definition = candidate_response["taskDefinition"]
            if candidate_definition.get("family") != family:
                continue
            candidate_tags = {
                str(tag.get("key") or ""): str(tag.get("value") or "")
                for tag in (candidate_response.get("tags") or [])
            }
            if (
                candidate_tags.get(TASK_DEFINITION_DIGEST_TAG)
                != image_digest
            ):
                continue
            candidate_containers = [
                item
                for item in (
                    candidate_definition.get("containerDefinitions") or []
                )
                if item.get("name") == runtime.container_name
            ]
            if (
                len(candidate_containers) != 1
                or candidate_containers[0].get("image") != expected_image
            ):
                continue
            candidate_registration = build_task_definition_registration(
                candidate_definition,
                container_name=runtime.container_name,
                image_digest=image_digest,
            )
            if candidate_registration != comparable_registration:
                continue
            print(
                "INVOICE_TASK_DEFINITION_REUSED "
                f"task_definition={candidate_arn} digest={image_digest}"
            )
            return InvoiceRuntime(
                container_name=runtime.container_name,
                log_group=runtime.log_group,
                task_definition_arn=str(candidate_arn),
                image=expected_image,
            )
        registered = self.ecs.register_task_definition(**registration)[
            "taskDefinition"
        ]
        return InvoiceRuntime(
            container_name=runtime.container_name,
            log_group=runtime.log_group,
            task_definition_arn=registered["taskDefinitionArn"],
            image=expected_image,
        )

    def _queue_identity(self, queue_name: str) -> tuple[str, str]:
        response = self.sqs.get_queue_url(
            QueueName=queue_name,
            QueueOwnerAWSAccountId=self.account_id,
        )
        queue_url = response["QueueUrl"]
        attributes = self.sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]
        return queue_url, attributes["QueueArn"]

    def _ensure_queue(
        self,
        config: InvoiceControlConfig,
        schedule_arns: Sequence[str],
    ) -> tuple[str, str]:
        try:
            queue_url, queue_arn = self._queue_identity(config.queue_name)
        except ClientError as exc:
            code = (exc.response.get("Error") or {}).get("Code")
            if code not in (
                "AWS.SimpleQueueService.NonExistentQueue",
                "QueueDoesNotExist",
            ):
                raise
            response = self.sqs.create_queue(
                QueueName=config.queue_name,
                Attributes={
                    "MessageRetentionPeriod": str(DLQ_RETENTION_SECONDS),
                    "SqsManagedSseEnabled": "true",
                },
                tags={
                    "Project": config.project,
                    "Purpose": "invoice-scheduler-dlq",
                },
            )
            queue_url = response["QueueUrl"]
            queue_arn = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["QueueArn"],
            )["Attributes"]["QueueArn"]
        policy = build_queue_policy_for_account(
            queue_arn,
            schedule_arns,
            self.account_id,
        )
        self.sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={
                "MessageRetentionPeriod": str(DLQ_RETENTION_SECONDS),
                "SqsManagedSseEnabled": "true",
                "Policy": json.dumps(policy, separators=(",", ":"), sort_keys=True),
            },
        )
        return queue_url, queue_arn

    def _put_dlq_alarm(self, config: InvoiceControlConfig) -> None:
        self.cloudwatch.put_metric_alarm(
            **build_dlq_alarm(config, self.alert_topic_arn)
        )

    def _put_application_alarm(
        self,
        deployment: ProjectDeployment,
    ) -> None:
        config = deployment.config
        self.logs.put_retention_policy(
            logGroupName=deployment.runtime.log_group,
            retentionInDays=LOG_RETENTION_DAYS,
        )
        metric_filter = build_failure_metric_filter(config)
        self.logs.put_metric_filter(
            logGroupName=deployment.runtime.log_group,
            filterName=config.metric_filter_name,
            **metric_filter,
        )
        self.cloudwatch.put_metric_alarm(
            **build_application_alarm(config, self.alert_topic_arn)
        )

    def _put_heartbeat_alarm(self, config: InvoiceControlConfig) -> None:
        self.cloudwatch.put_metric_alarm(
            **build_heartbeat_alarm(config, self.alert_topic_arn)
        )

    def _wait_for_scheduler_dlq_permission(
        self,
        *,
        role_arn: str,
        queue_arn: str,
        attempts: int = 8,
    ) -> None:
        last_decision = ""
        for attempt in range(1, attempts + 1):
            response = self.iam.simulate_principal_policy(
                PolicySourceArn=role_arn,
                ActionNames=["sqs:SendMessage"],
                ResourceArns=[queue_arn],
            )
            results = response.get("EvaluationResults") or []
            last_decision = (
                str(results[0].get("EvalDecision") or "")
                if results
                else "missing-result"
            )
            if last_decision == "allowed":
                return
            if attempt < attempts:
                time.sleep(min(attempt * 2, 10))
        raise AssertionError(
            f"{role_arn}: sqs:SendMessage to {queue_arn} is {last_decision}, expected allowed"
        )

    def apply(
        self,
        configs: Sequence[InvoiceControlConfig],
        *,
        defer_heartbeat_alarm: bool,
        image_digest: str,
        resume_deferred_bootstrap: bool = False,
    ) -> None:
        if defer_heartbeat_alarm and resume_deferred_bootstrap:
            raise ValueError(
                "defer_heartbeat_alarm and resume_deferred_bootstrap are "
                "mutually exclusive"
            )
        self.sns.get_topic_attributes(TopicArn=self.alert_topic_arn)
        current: List[
            tuple[InvoiceControlConfig, Dict[str, Any], Dict[str, Any], InvoiceRuntime]
        ] = []
        images_to_validate: set[str] = set()
        for config in configs:
            primary = self._get_schedule(config.primary_schedule_name)
            final = self._get_schedule(config.final_schedule_name)
            if (
                resume_deferred_bootstrap
                and (
                    str(primary.get("State") or "").upper() != "DISABLED"
                    or str(final.get("State") or "").upper() != "DISABLED"
                )
            ):
                raise ValueError(
                    f"{config.project}: expected both invoice schedules to be "
                    "DISABLED before resuming deferred bootstrap"
                )
            primary_runtime = self._runtime_from_schedule(config, primary)
            final_runtime = self._runtime_from_schedule(config, final)
            if (
                primary_runtime.container_name != final_runtime.container_name
                or primary_runtime.log_group != final_runtime.log_group
            ):
                raise ValueError(
                    f"{config.project}: primary and final schedules do not share "
                    "one container/log runtime"
                )
            primary_repository = parse_ecr_image_uri(primary_runtime.image)[:3]
            final_repository = parse_ecr_image_uri(final_runtime.image)[:3]
            if primary_repository != final_repository:
                raise ValueError(
                    f"{config.project}: primary/final ECR repository mismatch "
                    f"{primary_repository} != {final_repository}"
                )
            desired_primary_image = pinned_image_uri(
                primary_runtime.image,
                image_digest,
            )
            desired_final_image = pinned_image_uri(
                final_runtime.image,
                image_digest,
            )
            if primary_runtime.image == desired_primary_image:
                source_runtime = primary_runtime
            elif final_runtime.image == desired_final_image:
                source_runtime = final_runtime
            else:
                source_runtime = primary_runtime
            images_to_validate.add(
                deployment_validation_image(source_runtime.image, image_digest)
            )
            current.append((config, primary, final, source_runtime))

        for image in sorted(images_to_validate):
            self._assert_ecr_digest_exists(
                image,
                image_digest,
                require_latest=not resume_deferred_bootstrap,
            )

        if defer_heartbeat_alarm:
            for config, primary, final, _runtime in current:
                for schedule, schedule_name in (
                    (primary, config.primary_schedule_name),
                    (final, config.final_schedule_name),
                ):
                    if str(schedule.get("State") or "").upper() != "DISABLED":
                        self.scheduler.update_schedule(
                            **_schedule_update_request(
                                schedule,
                                expression=str(schedule["ScheduleExpression"]),
                                timezone=str(
                                    schedule.get("ScheduleExpressionTimezone")
                                    or config.timezone
                                ),
                                target=schedule["Target"],
                                state="DISABLED",
                            )
                        )
                    disabled = self._get_schedule(schedule_name)
                    if str(disabled.get("State") or "").upper() != "DISABLED":
                        raise AssertionError(
                            f"{schedule_name}: failed to enter DISABLED "
                            "bootstrap state"
                        )
                    schedule.clear()
                    schedule.update(disabled)
            self._wait_for_invoice_tasks_to_quiesce(current)
            print(
                "INVOICE_SCHEDULES_DEFERRED "
                f"projects={','.join(config.project for config in configs)}"
            )

        raw: List[
            tuple[InvoiceControlConfig, Dict[str, Any], Dict[str, Any], InvoiceRuntime]
        ] = []
        for config, primary, final, runtime in current:
            pinned_runtime = self._ensure_pinned_task_definition(
                runtime,
                image_digest=image_digest,
            )
            raw.append((config, primary, final, pinned_runtime))

        queue_details: Dict[str, tuple[str, str]] = {}
        role_policies: List[tuple[str, InvoiceControlConfig, str]] = []
        for config, primary, final, _runtime in raw:
            schedule_arns = [str(primary["Arn"]), str(final["Arn"])]
            queue_url, queue_arn = self._ensure_queue(config, schedule_arns)
            queue_details[config.project] = (queue_url, queue_arn)
            role_arns = {
                str((primary.get("Target") or {}).get("RoleArn") or ""),
                str((final.get("Target") or {}).get("RoleArn") or ""),
            }
            role_arns.discard("")
            if len(role_arns) != 1:
                raise ValueError(
                    f"{config.project}: expected one shared Scheduler role, got {role_arns}"
                )
            role_policies.append((next(iter(role_arns)), config, queue_arn))

        for role_arn, config, queue_arn in role_policies:
            policy_name = f"InvoiceSchedulerDlqSendMessage-{config.project}"
            expected_policy = build_scheduler_dlq_policy([queue_arn])
            self.iam.put_role_policy(
                RoleName=_role_name(role_arn),
                PolicyName=policy_name,
                PolicyDocument=json.dumps(
                    expected_policy,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            )
            stored_policy = self.iam.get_role_policy(
                RoleName=_role_name(role_arn),
                PolicyName=policy_name,
            )["PolicyDocument"]
            if stored_policy != expected_policy:
                raise AssertionError(f"{role_arn}: invoice DLQ IAM policy did not persist")
            self._wait_for_scheduler_dlq_permission(
                role_arn=role_arn,
                queue_arn=queue_arn,
            )

        prepared: List[
            tuple[ProjectDeployment, Dict[str, Any], Dict[str, Any]]
        ] = []
        for config, primary, final, runtime in raw:
            queue_url, queue_arn = queue_details[config.project]
            deployment = ProjectDeployment(
                config=config,
                primary_schedule=primary,
                final_schedule=final,
                runtime=runtime,
                queue_url=queue_url,
                queue_arn=queue_arn,
            )
            primary_target = build_schedule_target(
                primary["Target"],
                queue_arn=queue_arn,
                container_name=runtime.container_name,
                project=config.project,
                reconcile=False,
                task_definition_arn=runtime.task_definition_arn,
            )
            final_target = build_schedule_target(
                final["Target"],
                queue_arn=queue_arn,
                container_name=runtime.container_name,
                project=config.project,
                reconcile=True,
                task_definition_arn=runtime.task_definition_arn,
            )
            prepared.append((deployment, primary_target, final_target))

        for deployment, _primary_target, _final_target in prepared:
            self._put_dlq_alarm(deployment.config)
            self._put_application_alarm(deployment)
            if not defer_heartbeat_alarm:
                self._put_heartbeat_alarm(deployment.config)

        if not defer_heartbeat_alarm:
            for deployment, _primary_target, _final_target in prepared:
                self._assert_ecr_digest_exists(
                    deployment.runtime.image,
                    image_digest,
                    require_latest=not resume_deferred_bootstrap,
                )

        for deployment, primary_target, final_target in prepared:
            config = deployment.config
            self.scheduler.update_schedule(
                **_schedule_update_request(
                    deployment.primary_schedule,
                    expression=config.primary_schedule_expression,
                    timezone=config.timezone,
                    target=primary_target,
                    state=(
                        "DISABLED"
                        if defer_heartbeat_alarm
                        else "ENABLED"
                    ),
                )
            )
            self.scheduler.update_schedule(
                **_schedule_update_request(
                    deployment.final_schedule,
                    expression=config.final_schedule_expression,
                    timezone=config.timezone,
                    target=final_target,
                    state=(
                        "DISABLED"
                        if defer_heartbeat_alarm
                        else "ENABLED"
                    ),
                )
            )

        self.verify(
            configs,
            require_heartbeat_alarm=not defer_heartbeat_alarm,
            image_digest=image_digest,
            primary_schedule_enabled=not defer_heartbeat_alarm,
            final_schedule_enabled=not defer_heartbeat_alarm,
        )
        print(
            "INVOICE_CONTROLS_APPLIED "
            f"projects={','.join(config.project for config in configs)} "
            f"heartbeat_alarm={'deferred' if defer_heartbeat_alarm else 'enabled'}"
        )

    def verify(
        self,
        configs: Sequence[InvoiceControlConfig],
        *,
        require_heartbeat_alarm: bool = True,
        image_digest: str = "",
        primary_schedule_enabled: bool = True,
        final_schedule_enabled: bool = True,
    ) -> None:
        self.sns.get_topic_attributes(TopicArn=self.alert_topic_arn)
        alarm_names: List[str] = []
        expected_queue_arns: Dict[str, str] = {}
        role_policies: List[tuple[str, InvoiceControlConfig, str]] = []

        for config in configs:
            primary = self._get_schedule(config.primary_schedule_name)
            final = self._get_schedule(config.final_schedule_name)
            runtime = self._runtime_from_schedule(config, primary)
            if self._runtime_from_schedule(config, final) != runtime:
                raise AssertionError(
                    f"{config.project}: primary/final runtime mismatch"
                )
            runtime_digest = parse_ecr_image_uri(runtime.image)[3]
            if not runtime_digest:
                raise AssertionError(
                    f"{config.project}: task image is not pinned by digest"
                )
            if image_digest:
                if runtime_digest != image_digest:
                    raise AssertionError(
                        f"{config.project}: image digest drift "
                        f"{runtime_digest} != {image_digest}"
                    )
            self._assert_ecr_digest_exists(
                runtime.image,
                runtime_digest,
                require_latest=False,
            )
            queue_url, queue_arn = self._queue_identity(config.queue_name)
            expected_queue_arns[config.project] = queue_arn

            for schedule, expression, reconcile, expected_state in (
                (
                    primary,
                    config.primary_schedule_expression,
                    False,
                    "ENABLED" if primary_schedule_enabled else "DISABLED",
                ),
                (
                    final,
                    config.final_schedule_expression,
                    True,
                    "ENABLED" if final_schedule_enabled else "DISABLED",
                ),
            ):
                if schedule.get("State") != expected_state:
                    raise AssertionError(
                        f"{schedule['Name']}: state "
                        f"{schedule.get('State')} != {expected_state}"
                    )
                if schedule.get("ScheduleExpression") != expression:
                    raise AssertionError(
                        f"{schedule['Name']}: expression drift "
                        f"{schedule.get('ScheduleExpression')} != {expression}"
                    )
                if schedule.get("ScheduleExpressionTimezone") != config.timezone:
                    raise AssertionError(f"{schedule['Name']}: timezone drift")
                target = schedule.get("Target") or {}
                if (target.get("DeadLetterConfig") or {}).get("Arn") != queue_arn:
                    raise AssertionError(f"{schedule['Name']}: DLQ drift")
                raw_input = target.get("Input")
                if not isinstance(raw_input, str):
                    raise AssertionError(f"{schedule['Name']}: missing command override")
                overrides = json.loads(raw_input)
                containers = overrides.get("containerOverrides") or []
                if len(containers) != 1:
                    raise AssertionError(
                        f"{schedule['Name']}: expected one container override"
                    )
                if containers[0].get("name") != runtime.container_name:
                    raise AssertionError(f"{schedule['Name']}: container name drift")
                expected_command = expected_invoice_command(
                    config.project,
                    reconcile=reconcile,
                )
                if containers[0].get("command") != expected_command:
                    raise AssertionError(
                        f"{schedule['Name']}: command drift "
                        f"{containers[0].get('command')} != {expected_command}"
                    )
                role_arn = str(target.get("RoleArn") or "")
                role_policies.append((role_arn, config, queue_arn))

            queue_attributes = self.sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "QueueArn",
                    "MessageRetentionPeriod",
                    "SqsManagedSseEnabled",
                    "Policy",
                ],
            )["Attributes"]
            if int(queue_attributes.get("MessageRetentionPeriod", "0")) != DLQ_RETENTION_SECONDS:
                raise AssertionError(f"{config.queue_name}: retention drift")
            if queue_attributes.get("SqsManagedSseEnabled") != "true":
                raise AssertionError(f"{config.queue_name}: SSE drift")
            queue_policy = json.loads(queue_attributes.get("Policy") or "{}")
            expected_policy = build_queue_policy_for_account(
                queue_arn,
                [str(primary["Arn"]), str(final["Arn"])],
                self.account_id,
            )
            if queue_policy != expected_policy:
                raise AssertionError(f"{config.queue_name}: queue policy drift")

            groups = self.logs.describe_log_groups(
                logGroupNamePrefix=runtime.log_group,
            ).get("logGroups") or []
            exact_groups = [
                group
                for group in groups
                if group.get("logGroupName") == runtime.log_group
            ]
            if len(exact_groups) != 1:
                raise AssertionError(f"{config.project}: log group missing")
            if exact_groups[0].get("retentionInDays") != LOG_RETENTION_DAYS:
                raise AssertionError(f"{config.project}: log retention drift")

            filters = self.logs.describe_metric_filters(
                logGroupName=runtime.log_group,
                filterNamePrefix=config.metric_filter_name,
            ).get("metricFilters") or []
            exact_filters = [
                item
                for item in filters
                if item.get("filterName") == config.metric_filter_name
            ]
            if len(exact_filters) != 1:
                raise AssertionError(f"{config.project}: failure metric filter missing")
            expected_filter = build_failure_metric_filter(config)
            if (
                exact_filters[0].get("filterPattern")
                != expected_filter["filterPattern"]
                or exact_filters[0].get("metricTransformations")
                != expected_filter["metricTransformations"]
            ):
                raise AssertionError(f"{config.project}: failure metric filter drift")

            alarm_names.extend(
                [
                    config.dlq_alarm_name,
                    config.application_alarm_name,
                    config.heartbeat_alarm_name,
                ]
            )

        checked_role_policies = set()
        for role_arn, config, queue_arn in role_policies:
            if not role_arn:
                raise AssertionError("Invoice schedule is missing RoleArn")
            identity = (role_arn, config.project)
            if identity in checked_role_policies:
                continue
            checked_role_policies.add(identity)
            policy = self.iam.get_role_policy(
                RoleName=_role_name(role_arn),
                PolicyName=f"InvoiceSchedulerDlqSendMessage-{config.project}",
            )["PolicyDocument"]
            expected_policy = build_scheduler_dlq_policy([queue_arn])
            if policy != expected_policy:
                raise AssertionError(f"{role_arn}: invoice DLQ IAM policy drift")
            self._wait_for_scheduler_dlq_permission(
                role_arn=role_arn,
                queue_arn=queue_arn,
            )

        alarms = self.cloudwatch.describe_alarms(
            AlarmNames=alarm_names,
        ).get("MetricAlarms") or []
        by_name = {alarm["AlarmName"]: alarm for alarm in alarms}
        required_alarm_names = {
            name
            for config in configs
            for name in (
                config.dlq_alarm_name,
                config.application_alarm_name,
            )
        }
        if require_heartbeat_alarm:
            required_alarm_names.update(
                config.heartbeat_alarm_name for config in configs
            )
        missing_alarms = sorted(required_alarm_names - set(by_name))
        if missing_alarms:
            raise AssertionError(f"Missing invoice alarms: {missing_alarms}")
        for config in configs:
            dlq_alarm = by_name[config.dlq_alarm_name]
            application_alarm = by_name[config.application_alarm_name]
            assert_alarm_matches(
                dlq_alarm,
                build_dlq_alarm(config, self.alert_topic_arn),
            )
            assert_alarm_matches(
                application_alarm,
                build_application_alarm(config, self.alert_topic_arn),
            )
            heartbeat = by_name.get(config.heartbeat_alarm_name)
            if require_heartbeat_alarm:
                assert heartbeat is not None
                assert_alarm_matches(
                    heartbeat,
                    build_heartbeat_alarm(config, self.alert_topic_arn),
                )

        print(
            "INVOICE_CONTROLS_VERIFIED "
            + " ".join(
                (
                    f"{config.project}:primary={config.primary_schedule_name},"
                    f"final={config.final_schedule_name},"
                    f"dlq={expected_queue_arns[config.project]}"
                )
                for config in configs
            )
        )

    def arm_bootstrap_failure_alerts(
        self,
        configs: Sequence[InvoiceControlConfig],
    ) -> None:
        self.sns.get_topic_attributes(TopicArn=self.alert_topic_arn)
        armed: List[str] = []
        for config in configs:
            primary = self._get_schedule(config.primary_schedule_name)
            final = self._get_schedule(config.final_schedule_name)
            if all(
                str(schedule.get("State") or "").upper() == "ENABLED"
                for schedule in (primary, final)
            ):
                continue
            self._put_heartbeat_alarm(config)
            alarms = self.cloudwatch.describe_alarms(
                AlarmNames=[config.heartbeat_alarm_name],
            ).get("MetricAlarms") or []
            exact = [
                alarm
                for alarm in alarms
                if alarm.get("AlarmName") == config.heartbeat_alarm_name
            ]
            if len(exact) != 1:
                raise AssertionError(
                    f"{config.heartbeat_alarm_name}: failed to arm"
                )
            assert_alarm_matches(
                exact[0],
                build_heartbeat_alarm(config, self.alert_topic_arn),
            )
            armed.append(config.project)
        print(
            "INVOICE_BOOTSTRAP_FAILURE_ALERTS_ARMED "
            f"projects={','.join(armed) if armed else 'none'}"
        )


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    projects = list(dict.fromkeys(str(value).strip() for value in args.projects))
    if not projects or any(not value for value in projects):
        raise ValueError("At least one non-empty project is required")
    configs = [load_control_config(project) for project in projects]
    deployer = InvoiceControlDeployer(
        region=args.region,
        profile=args.profile,
        alert_topic_name=args.alert_topic_name,
    )
    if args.apply:
        if not args.image_digest:
            raise ValueError("--apply requires --image-digest for immutable task pinning")
        if args.defer_heartbeat_alarm and args.resume_deferred_bootstrap:
            raise ValueError(
                "--defer-heartbeat-alarm and --resume-deferred-bootstrap "
                "cannot be used together"
            )
        deployer.apply(
            configs,
            defer_heartbeat_alarm=bool(args.defer_heartbeat_alarm),
            image_digest=args.image_digest,
            resume_deferred_bootstrap=bool(args.resume_deferred_bootstrap),
        )
    elif args.verify:
        if args.defer_heartbeat_alarm or args.resume_deferred_bootstrap:
            raise ValueError(
                "--defer-heartbeat-alarm and --resume-deferred-bootstrap "
                "are valid only with --apply"
            )
        deployer.verify(configs, image_digest=args.image_digest)
    else:
        if (
            args.defer_heartbeat_alarm
            or args.resume_deferred_bootstrap
            or args.image_digest
        ):
            raise ValueError(
                "bootstrap failure alert arming does not accept image or "
                "deployment phase arguments"
            )
        deployer.arm_bootstrap_failure_alerts(configs)


if __name__ == "__main__":
    main()
