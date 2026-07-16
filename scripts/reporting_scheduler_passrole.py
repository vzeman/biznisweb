#!/usr/bin/env python3
"""Build the least-privilege PassRole policy for a reporting schedule.

EventBridge Scheduler calls ECS with its own IAM role.  Whenever a reporting
task definition moves to a project-specific task role, the scheduler role must
be allowed to pass both that task role and the ECS execution role.  This helper
validates the AWS documents and emits an exact, account-local policy.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Tuple


ROLE_ARN_RE = re.compile(
    r"^arn:(?P<partition>aws(?:-us-gov|-cn)?):iam::"
    r"(?P<account>\d{12}):role/(?P<path>[A-Za-z0-9+=,.@_/-]+)$"
)
ROLE_NAME_RE = re.compile(r"^[A-Za-z0-9+=,.@_-]{1,64}$")


def _load_json(path: Path) -> Dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def validate_role_arn(role_arn: str, account_id: str) -> Tuple[str, str]:
    """Return ``(partition, role_name)`` for an exact local IAM role ARN."""

    match = ROLE_ARN_RE.fullmatch(str(role_arn or ""))
    if not match:
        raise ValueError(f"Invalid IAM role ARN: {role_arn!r}")
    if match.group("account") != account_id:
        raise ValueError(
            f"IAM role {role_arn!r} is not in expected account {account_id}"
        )
    role_path = match.group("path")
    if "*" in role_path or role_path.endswith("/"):
        raise ValueError(f"IAM role ARN must identify one exact role: {role_arn!r}")
    return match.group("partition"), role_path.rsplit("/", 1)[-1]


def build_passrole_documents(
    schedule: Dict[str, Any],
    task_definition_document: Dict[str, Any],
    scheduler_role_document: Dict[str, Any],
    project_task_role_arn: str,
    account_id: str,
    expected_scheduler_role_name: str,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Build the policy and metadata used by the deployment workflow."""

    target = schedule.get("Target")
    if not isinstance(target, dict):
        raise ValueError("Schedule document is missing Target")
    scheduler_role_arn = str(target.get("RoleArn") or "")
    scheduler_partition, scheduler_role_name = validate_role_arn(
        scheduler_role_arn, account_id
    )
    if not ROLE_NAME_RE.fullmatch(expected_scheduler_role_name):
        raise ValueError(
            f"Invalid expected scheduler role name: {expected_scheduler_role_name!r}"
        )
    expected_scheduler_role_arn = (
        f"arn:{scheduler_partition}:iam::{account_id}:role/"
        f"{expected_scheduler_role_name}"
    )
    if scheduler_role_arn != expected_scheduler_role_arn:
        raise ValueError(
            "Schedule uses an unexpected scheduler role: "
            f"{scheduler_role_arn!r} != {expected_scheduler_role_arn!r}"
        )
    role = scheduler_role_document.get("Role", scheduler_role_document)
    if not isinstance(role, dict) or role.get("Arn") != scheduler_role_arn:
        raise ValueError("Scheduler role document does not match Schedule.Target.RoleArn")
    trust = role.get("AssumeRolePolicyDocument")
    statements = trust.get("Statement") if isinstance(trust, dict) else None
    if not isinstance(statements, list) or len(statements) != 1:
        raise ValueError("Scheduler role must have one exact trust statement")
    trust_statement = statements[0]
    if not isinstance(trust_statement, dict):
        raise ValueError("Scheduler role trust statement is invalid")
    if (
        trust_statement.get("Effect") != "Allow"
        or trust_statement.get("Action") != "sts:AssumeRole"
        or trust_statement.get("Principal")
        != {"Service": "scheduler.amazonaws.com"}
        or trust_statement.get("Condition") not in (None, {})
    ):
        raise ValueError(
            "Scheduler role trust must allow only scheduler.amazonaws.com to assume it"
        )

    raw_task_definition = task_definition_document.get(
        "taskDefinition", task_definition_document
    )
    if not isinstance(raw_task_definition, dict):
        raise ValueError("Task-definition document is invalid")
    actual_task_role_arn = str(raw_task_definition.get("taskRoleArn") or "")
    execution_role_arn = str(raw_task_definition.get("executionRoleArn") or "")
    task_partition, _ = validate_role_arn(actual_task_role_arn, account_id)
    execution_partition, _ = validate_role_arn(execution_role_arn, account_id)
    validate_role_arn(project_task_role_arn, account_id)

    if actual_task_role_arn != project_task_role_arn:
        raise ValueError(
            "Candidate task definition does not use the expected project task role: "
            f"{actual_task_role_arn!r} != {project_task_role_arn!r}"
        )
    if len({scheduler_partition, task_partition, execution_partition}) != 1:
        raise ValueError("Scheduler, task, and execution roles use different partitions")

    resources = list(dict.fromkeys([project_task_role_arn, execution_role_arn]))
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PassReportingTaskRolesToEcs",
                "Effect": "Allow",
                "Action": "iam:PassRole",
                "Resource": resources,
                "Condition": {
                    "StringEquals": {
                        "iam:PassedToService": "ecs-tasks.amazonaws.com"
                    }
                },
            }
        ],
    }
    metadata = {
        "scheduler_role_arn": scheduler_role_arn,
        "scheduler_role_name": scheduler_role_name,
        "task_role_arn": project_task_role_arn,
        "execution_role_arn": execution_role_arn,
    }
    return policy, metadata


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule-json", type=Path, required=True)
    parser.add_argument("--task-definition-json", type=Path, required=True)
    parser.add_argument("--scheduler-role-json", type=Path, required=True)
    parser.add_argument("--project-task-role-arn", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--expected-scheduler-role-name", required=True)
    parser.add_argument("--policy-output", type=Path, required=True)
    parser.add_argument("--metadata-output", type=Path, required=True)
    args = parser.parse_args()

    policy, metadata = build_passrole_documents(
        schedule=_load_json(args.schedule_json),
        task_definition_document=_load_json(args.task_definition_json),
        scheduler_role_document=_load_json(args.scheduler_role_json),
        project_task_role_arn=args.project_task_role_arn,
        account_id=args.account_id,
        expected_scheduler_role_name=args.expected_scheduler_role_name,
    )
    args.policy_output.write_text(
        json.dumps(policy, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    args.metadata_output.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
