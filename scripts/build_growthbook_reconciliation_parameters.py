from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence


PARAMETER_KEYS = (
    "Environment",
    "ClusterArn",
    "TaskDefinitionArn",
    "TaskRoleArn",
    "ExecutionRoleArn",
    "ContainerName",
    "EventBucketName",
    "LogGroupName",
    "SubnetIds",
    "SecurityGroupIds",
    "AssignPublicIp",
    "PlatformVersion",
    "ScheduleState",
    "ScheduleExpression",
    "ScheduleTimezone",
)

ENVIRONMENT_CONTRACTS = {
    "preview": "cron(30 3 * * ? *)",
    "production": "cron(45 3 * * ? *)",
}


def _required(environ: Mapping[str, str], key: str) -> str:
    value = environ.get(key, "")
    if not value or "\n" in value or "\r" in value:
        raise ValueError(f"missing or invalid reconciliation environment value: {key}")
    return value


def build_candidate_parameters(environ: Mapping[str, str]) -> list[dict[str, str]]:
    environment = _required(environ, "GROWTHBOOK_ENVIRONMENT").strip().lower()
    schedule_expression = ENVIRONMENT_CONTRACTS.get(environment)
    if schedule_expression is None:
        raise ValueError("unsupported reconciliation environment")
    values = {
        "Environment": environment,
        "ClusterArn": _required(environ, "CLUSTER_ARN"),
        "TaskDefinitionArn": _required(environ, "CANDIDATE_TASK_DEFINITION"),
        "TaskRoleArn": _required(environ, "TASK_ROLE_ARN"),
        "ExecutionRoleArn": _required(environ, "EXECUTION_ROLE_ARN"),
        "ContainerName": _required(environ, "CONTAINER_NAME"),
        "EventBucketName": _required(environ, "EVENT_BUCKET"),
        "LogGroupName": _required(environ, "LOG_GROUP"),
        "SubnetIds": _required(environ, "SUBNET_IDS"),
        "SecurityGroupIds": _required(environ, "SECURITY_GROUP_IDS"),
        "AssignPublicIp": _required(environ, "ASSIGN_PUBLIC_IP"),
        "PlatformVersion": _required(environ, "PLATFORM_VERSION"),
        "ScheduleState": "DISABLED",
        "ScheduleExpression": schedule_expression,
        "ScheduleTimezone": "Europe/Bratislava",
    }
    if tuple(values) != PARAMETER_KEYS:
        raise ValueError("candidate reconciliation parameter key/order drift")
    return [
        {"ParameterKey": key, "ParameterValue": values[key]}
        for key in PARAMETER_KEYS
    ]


def build_activation_parameters() -> list[dict[str, str | bool]]:
    return [
        {"ParameterKey": key, "ParameterValue": "ENABLED"}
        if key == "ScheduleState"
        else {"ParameterKey": key, "UsePreviousValue": True}
        for key in PARAMETER_KEYS
    ]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", required=True, choices=("candidate", "activate"))
    args = parser.parse_args(argv)
    parameters = (
        build_candidate_parameters(os.environ)
        if args.phase == "candidate"
        else build_activation_parameters()
    )
    json.dump(parameters, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
