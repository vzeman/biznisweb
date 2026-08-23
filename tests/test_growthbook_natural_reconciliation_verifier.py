from __future__ import annotations

import copy
import hashlib
import json
import unittest
from datetime import datetime, timedelta

from scripts.verify_growthbook_natural_reconciliation import (
    EXPECTED_IMAGE_DIGEST,
    TARGET_RUN_DUE_UTC,
    VERIFY_NOT_BEFORE_UTC,
    VERIFY_BEFORE_UTC,
    VerificationError,
    build_natural_reconciliation_evidence,
    verify_natural_reconciliation,
)


ACCOUNT = "919341186960"
REGION = "eu-central-1"
CLUSTER_ARN = f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/vevo-reporting-cluster"
TASK_DEFINITION_ARN = (
    f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/"
    "vevo-growthbook-reconcile-preview:4"
)
TASK_ID = "a" * 32
TASK_ARN = (
    f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/vevo-reporting-cluster/{TASK_ID}"
)
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/vevo-growthbook-reconcile-preview-scheduler"
DLQ_ARN = f"arn:aws:sqs:{REGION}:{ACCOUNT}:vevo-growthbook-reconcile-preview-dlq"
LOG_GROUP = "/ecs/vevo-reporting-daily"
LOG_PREFIX = "ecs"
LOG_STREAM = f"{LOG_PREFIX}/reporting/{TASK_ID}"
COMMAND = [
    "/bin/bash",
    "-lc",
    "cd /app && python scripts/run_scheduled_growthbook_reconciliation.py",
]
EVENT_BUCKET = "vevo-growthbook-preview-events"


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _base_payloads() -> dict[str, object]:
    schedule_input = {
        "containerOverrides": [
            {
                "name": "reporting",
                "command": COMMAND,
                "environment": [
                    {"name": "REPORT_PROJECT", "value": "vevo"},
                    {"name": "GROWTHBOOK_ENVIRONMENT", "value": "preview"},
                    {"name": "GROWTHBOOK_EVENT_BUCKET", "value": EVENT_BUCKET},
                    {"name": "GROWTHBOOK_FACT_PUBLISH_ENABLED", "value": "true"},
                    {"name": "AWS_REGION", "value": REGION},
                ],
            }
        ]
    }
    schedule = {
        "Name": "vevo-growthbook-reconcile-preview",
        "GroupName": "default",
        "State": "ENABLED",
        "ScheduleExpression": "cron(30 3 * * ? *)",
        "ScheduleExpressionTimezone": "Europe/Bratislava",
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "Target": {
            "Arn": CLUSTER_ARN,
            "RoleArn": ROLE_ARN,
            "DeadLetterConfig": {"Arn": DLQ_ARN},
            "RetryPolicy": {
                "MaximumEventAgeInSeconds": 3600,
                "MaximumRetryAttempts": 2,
            },
            "EcsParameters": {
                "TaskDefinitionArn": TASK_DEFINITION_ARN,
                "LaunchType": "FARGATE",
                "TaskCount": 1,
                "EnableExecuteCommand": False,
                "Group": "vevo-growthbook-reconcile-preview",
                "NetworkConfiguration": {
                    "awsvpcConfiguration": {
                        "AssignPublicIp": "DISABLED",
                        "Subnets": ["subnet-a", "subnet-b"],
                        "SecurityGroups": ["sg-a"],
                    }
                },
            },
            "Input": json.dumps(schedule_input, separators=(",", ":")),
        },
    }
    stack = {
        "Stacks": [
            {
                "StackName": "vevo-growthbook-reconciliation-preview",
                "StackStatus": "UPDATE_COMPLETE",
                "Parameters": [
                    {"ParameterKey": "Environment", "ParameterValue": "preview"},
                    {"ParameterKey": "ClusterArn", "ParameterValue": CLUSTER_ARN},
                    {"ParameterKey": "TaskDefinitionArn", "ParameterValue": TASK_DEFINITION_ARN},
                    {"ParameterKey": "EventBucketName", "ParameterValue": EVENT_BUCKET},
                    {"ParameterKey": "AssignPublicIp", "ParameterValue": "DISABLED"},
                    {"ParameterKey": "ScheduleState", "ParameterValue": "ENABLED"},
                    {
                        "ParameterKey": "ScheduleExpression",
                        "ParameterValue": "cron(30 3 * * ? *)",
                    },
                    {
                        "ParameterKey": "ScheduleTimezone",
                        "ParameterValue": "Europe/Bratislava",
                    },
                ],
                "Outputs": [
                    {
                        "OutputKey": "ScheduleName",
                        "OutputValue": "vevo-growthbook-reconcile-preview",
                    },
                    {"OutputKey": "ScheduleState", "OutputValue": "ENABLED"},
                    {"OutputKey": "TargetTaskDefinitionArn", "OutputValue": TASK_DEFINITION_ARN},
                    {"OutputKey": "SchedulerRoleArn", "OutputValue": ROLE_ARN},
                    {"OutputKey": "DeadLetterQueueArn", "OutputValue": DLQ_ARN},
                ],
            }
        ]
    }
    task_definition = {
        "taskDefinition": {
            "taskDefinitionArn": TASK_DEFINITION_ARN,
            "family": "vevo-growthbook-reconcile-preview",
            "revision": 4,
            "taskRoleArn": f"arn:aws:iam::{ACCOUNT}:role/BiznisWebReportingTaskRole-vevo",
            "containerDefinitions": [
                {
                    "name": "reporting",
                    "image": f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/vevo-reporting@{EXPECTED_IMAGE_DIGEST}",
                    "command": COMMAND,
                    "logConfiguration": {
                        "options": {
                            "awslogs-group": LOG_GROUP,
                            "awslogs-stream-prefix": LOG_PREFIX,
                        }
                    },
                }
            ],
        }
    }
    success_marker = (
        "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:"
        "project=vevo:environment=preview:"
        "event-from=2026-07-14:event-through=2026-08-22:partitions=40"
    )
    marker_timestamp = int((TARGET_RUN_DUE_UTC + timedelta(minutes=6)).timestamp() * 1000)
    marker_events = {
        "events": [
            {
                "timestamp": marker_timestamp,
                "message": success_marker,
                "logStreamName": LOG_STREAM,
            }
        ]
    }
    summary = {
        "mode": "publish",
        "event_partitions": 40,
        "raw_events": 25,
        "completed_transaction_ids": 1,
        "authoritative_orders": 1,
        "device_facts": 5,
        "performance_facts": 8,
        "quality_reports": 2,
        "published": {
            "device_facts": 5,
            "performance_facts": 8,
            "quality_reports": 2,
        },
    }
    task_logs = {
        "events": [
            {
                "timestamp": marker_timestamp - 1,
                "message": json.dumps(summary, separators=(",", ":"), sort_keys=True),
            },
            {"timestamp": marker_timestamp, "message": success_marker},
        ]
    }
    task_state = {
        "tasks": [
            {
                "taskArn": TASK_ARN,
                "taskDefinitionArn": TASK_DEFINITION_ARN,
                "group": "vevo-growthbook-reconcile-preview",
                "launchType": "FARGATE",
                "enableExecuteCommand": False,
                "lastStatus": "STOPPED",
                "desiredStatus": "STOPPED",
                "stopCode": "EssentialContainerExited",
                "startedAt": _iso(TARGET_RUN_DUE_UTC + timedelta(minutes=1)),
                "stoppedAt": _iso(TARGET_RUN_DUE_UTC + timedelta(minutes=6)),
                "overrides": schedule_input,
                "attachments": [
                    {
                        "details": [
                            {"name": "privateIPv4Address", "value": "172.31.10.20"}
                        ]
                    }
                ],
                "containers": [
                    {
                        "exitCode": 0,
                        "imageDigest": EXPECTED_IMAGE_DIGEST,
                        "logStreamName": LOG_STREAM,
                    }
                ],
            }
        ],
        "failures": [],
    }
    alarms = {
        "MetricAlarms": [
            {
                "AlarmName": "vevo-growthbook-reconcile-preview-failure",
                "StateValue": "OK",
            },
            {
                "AlarmName": "vevo-growthbook-reconcile-preview-missing-success",
                "StateValue": "OK",
            },
            {
                "AlarmName": "vevo-growthbook-reconcile-preview-dlq",
                "StateValue": "OK",
            },
        ]
    }
    dlq = {
        "Attributes": {
            "SqsManagedSseEnabled": "true",
            "ApproximateNumberOfMessages": "0",
            "ApproximateNumberOfMessagesNotVisible": "0",
            "ApproximateNumberOfMessagesDelayed": "0",
        }
    }
    source_schedule = {
        "Name": "vevo-daily-report-email",
        "State": "ENABLED",
        "Target": {
            "EcsParameters": {
                "TaskDefinitionArn": (
                    f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/vevo-reporting-daily:33"
                )
            }
        },
    }
    cloudtrail_detail = {
        "eventSource": "ecs.amazonaws.com",
        "eventName": "RunTask",
        "userIdentity": {
            "type": "AssumedRole",
            "sessionContext": {"sessionIssuer": {"arn": ROLE_ARN}},
        },
        "requestParameters": {
            "group": "vevo-growthbook-reconcile-preview",
            "taskDefinition": TASK_DEFINITION_ARN,
        },
        "responseElements": {"tasks": [{"taskArn": TASK_ARN}]},
    }
    cloudtrail = {
        "Events": [
            {
                "EventName": "RunTask",
                "CloudTrailEvent": json.dumps(cloudtrail_detail),
            }
        ]
    }
    return {
        "schedule_payload": schedule,
        "stack_payload": stack,
        "task_definition_payload": task_definition,
        "marker_events_payload": marker_events,
        "task_state_payload": task_state,
        "task_logs_payload": task_logs,
        "alarms_payload": alarms,
        "dlq_payload": dlq,
        "source_schedule_payload": source_schedule,
        "cloudtrail_payload": cloudtrail,
    }


class GrowthBookNaturalReconciliationVerifierTests(unittest.TestCase):
    def verify(self, payloads: dict[str, object], *, now: datetime | None = None) -> dict[str, object]:
        return verify_natural_reconciliation(
            **payloads,
            now=now or VERIFY_NOT_BEFORE_UTC + timedelta(minutes=20),
        )

    def test_accepts_exact_natural_scheduler_evidence(self) -> None:
        result = self.verify(_base_payloads())
        self.assertEqual(TASK_ID, result["task_id"])
        self.assertEqual("172.31.10.20", result["private_ip"])
        self.assertTrue(result["generated_published_counts_match"])
        self.assertTrue(result["cloudtrail_scheduler_run_task_verified"])

    def test_builds_exact_sanitized_versioned_handoff_evidence(self) -> None:
        verified_at = VERIFY_NOT_BEFORE_UTC + timedelta(minutes=20)
        result = self.verify(_base_payloads(), now=verified_at)
        evidence = build_natural_reconciliation_evidence(
            result,
            verified_at=verified_at,
            workflow_run_id="32470000000",
            main_commit="f" * 40,
        )
        self.assertEqual(2, evidence["schema_version"])
        self.assertEqual(
            "vevo_growthbook_natural_reconciliation_retention_recovery",
            evidence["evidence_type"],
        )
        self.assertEqual("passed", evidence["status"])
        self.assertEqual("vzeman/biznisweb", evidence["repository"])
        self.assertEqual("32470000000", evidence["workflow_run_id"])
        self.assertEqual(TASK_ID, evidence["runtime"]["task_id"])
        self.assertEqual(25, evidence["reconciliation"]["raw_events"])
        self.assertEqual(40, evidence["reconciliation"]["partitions"])
        self.assertEqual(
            {
                "contains_raw_aws_payloads": False,
                "contains_cloudwatch_messages": False,
                "contains_cloudtrail_payloads": False,
                "contains_credentials": False,
                "contains_customer_or_order_data": False,
                "aws_mutations": False,
                "growthbook_mutations": False,
                "gtm_mutations": False,
                "meta_ads_mutations": False,
                "biznisweb_mutations": False,
            },
            evidence["safety"],
        )
        serialized = json.dumps(evidence, sort_keys=True)
        canonical_file = (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
        self.assertRegex(hashlib.sha256(canonical_file).hexdigest(), r"^[0-9a-f]{64}$")
        for forbidden in (
            "CloudTrailEvent",
            "AccessKeyId",
            "SecretAccessKey",
            "customer_email",
            "order_num",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_rejects_untrusted_evidence_identity_and_result_shape(self) -> None:
        verified_at = VERIFY_NOT_BEFORE_UTC + timedelta(minutes=20)
        result = self.verify(_base_payloads(), now=verified_at)
        with self.assertRaisesRegex(VerificationError, "workflow run ID"):
            build_natural_reconciliation_evidence(
                result,
                verified_at=verified_at,
                workflow_run_id="0",
                main_commit="f" * 40,
            )
        with self.assertRaisesRegex(VerificationError, "main commit"):
            build_natural_reconciliation_evidence(
                result,
                verified_at=verified_at,
                workflow_run_id="32470000000",
                main_commit="main",
            )
        drifted = copy.deepcopy(result)
        drifted["unexpected"] = True
        with self.assertRaisesRegex(VerificationError, "field set drift"):
            build_natural_reconciliation_evidence(
                drifted,
                verified_at=verified_at,
                workflow_run_id="32470000000",
                main_commit="f" * 40,
            )

    def test_rejects_verification_before_hard_time_gate(self) -> None:
        with self.assertRaisesRegex(VerificationError, "not due yet"):
            self.verify(_base_payloads(), now=VERIFY_NOT_BEFORE_UTC - timedelta(seconds=1))

    def test_rejects_verification_after_live_evidence_window(self) -> None:
        with self.assertRaisesRegex(VerificationError, "evidence window has closed"):
            self.verify(_base_payloads(), now=VERIFY_BEFORE_UTC)

    def test_rejects_wrong_consistent_cluster_identity(self) -> None:
        payloads = _base_payloads()
        wrong_cluster = f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/vevo-reporting"
        payloads["stack_payload"]["Stacks"][0]["Parameters"][1][
            "ParameterValue"
        ] = wrong_cluster
        payloads["schedule_payload"]["Target"]["Arn"] = wrong_cluster
        with self.assertRaisesRegex(VerificationError, "cluster identity"):
            self.verify(payloads)

    def test_rejects_wrong_log_boundary(self) -> None:
        payloads = _base_payloads()
        payloads["task_definition_payload"]["taskDefinition"][
            "containerDefinitions"
        ][0]["logConfiguration"]["options"]["awslogs-group"] = "/ecs/other"
        with self.assertRaisesRegex(VerificationError, "log boundary"):
            self.verify(payloads)

    def test_rejects_private_ip_outside_reporting_vpc(self) -> None:
        payloads = _base_payloads()
        payloads["task_state_payload"]["tasks"][0]["attachments"][0]["details"][
            0
        ]["value"] = "10.0.0.1"
        with self.assertRaisesRegex(VerificationError, "private IP boundary"):
            self.verify(payloads)

    def test_rejects_duplicate_at_least_once_delivery(self) -> None:
        payloads = _base_payloads()
        duplicate = copy.deepcopy(payloads["marker_events_payload"]["events"][0])
        duplicate["logStreamName"] = f"{LOG_PREFIX}/reporting/{'b' * 32}"
        payloads["marker_events_payload"]["events"].append(duplicate)
        with self.assertRaisesRegex(VerificationError, "exactly one success"):
            self.verify(payloads)

    def test_rejects_failure_marker_in_first_window(self) -> None:
        payloads = _base_payloads()
        payloads["marker_events_payload"]["events"].append(
            {
                "timestamp": int(TARGET_RUN_DUE_UTC.timestamp() * 1000),
                "message": "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE:RuntimeError",
                "logStreamName": LOG_STREAM,
            }
        )
        with self.assertRaisesRegex(VerificationError, "failure marker"):
            self.verify(payloads)

    def test_accepts_optional_absent_ecs_log_stream_with_exact_task_binding(self) -> None:
        payloads = _base_payloads()
        del payloads["task_state_payload"]["tasks"][0]["containers"][0][
            "logStreamName"
        ]
        result = self.verify(payloads)
        self.assertEqual("a" * 32, result["task_id"])

    def test_rejects_contradictory_nonempty_ecs_log_stream(self) -> None:
        payloads = _base_payloads()
        payloads["task_state_payload"]["tasks"][0]["containers"][0][
            "logStreamName"
        ] = f"{LOG_PREFIX}/reporting/{'b' * 32}"
        with self.assertRaisesRegex(VerificationError, "log stream drift"):
            self.verify(payloads)

    def test_rejects_known_manual_one_shot_identity(self) -> None:
        payloads = _base_payloads()
        payloads["task_state_payload"]["tasks"][0]["startedBy"] = (
            "vevo-growthbook-reconcile-once-32459100570"
        )
        with self.assertRaisesRegex(VerificationError, "manual one-shot"):
            self.verify(payloads)

    def test_rejects_generated_published_count_drift(self) -> None:
        payloads = _base_payloads()
        summary = json.loads(payloads["task_logs_payload"]["events"][0]["message"])
        summary["published"]["device_facts"] += 1
        payloads["task_logs_payload"]["events"][0]["message"] = json.dumps(
            summary, separators=(",", ":"), sort_keys=True
        )
        with self.assertRaisesRegex(VerificationError, "generated/published"):
            self.verify(payloads)

    def test_rejects_non_scheduler_cloudtrail_identity(self) -> None:
        payloads = _base_payloads()
        detail = json.loads(payloads["cloudtrail_payload"]["Events"][0]["CloudTrailEvent"])
        detail["userIdentity"]["sessionContext"]["sessionIssuer"]["arn"] = (
            f"arn:aws:iam::{ACCOUNT}:role/manual-operator"
        )
        payloads["cloudtrail_payload"]["Events"][0]["CloudTrailEvent"] = json.dumps(detail)
        with self.assertRaisesRegex(VerificationError, "Scheduler CloudTrail"):
            self.verify(payloads)


if __name__ == "__main__":
    unittest.main()
