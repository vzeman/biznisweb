#!/usr/bin/env python3
"""Resolve a stopped GrowthBook Fargate host-gate runtime without raw output.

ECS may omit the optional container ``logStreamName`` field after a task stops.
This fail-closed resolver accepts only that absence and reconstructs the exact
awslogs stream from the reviewed task definition, container name, and task ID.
Any non-empty contradictory ECS value is rejected.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Mapping


TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
ENV_PREFIX_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
LOG_PREFIX_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class HostGateRuntimeError(ValueError):
    """Raised when host-gate runtime identity is incomplete or contradictory."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise HostGateRuntimeError(message)


def resolve_host_gate_runtime(
    task_state: Mapping[str, Any],
    task_definition_state: Mapping[str, Any],
    *,
    expected_cluster_arn: str,
    expected_task_definition_arn: str,
    expected_container_name: str,
    expected_log_group: str,
    expected_log_region: str,
    expected_log_prefix: str,
    expected_image_digest: str,
    expected_private_cidr: str,
) -> dict[str, str]:
    """Return only the sanitized task identity needed by a host-gate workflow."""

    _require((task_state.get("failures") or []) == [], "host-gate task read-back failed")
    tasks = task_state.get("tasks") or []
    _require(len(tasks) == 1, "exactly one host-gate task is required")
    task = tasks[0]
    _require(isinstance(task, dict), "host-gate task shape drift")

    expected_task_prefix = expected_cluster_arn.replace(":cluster/", ":task/", 1) + "/"
    task_arn = str(task.get("taskArn") or "")
    _require(task_arn.startswith(expected_task_prefix), "host-gate task cluster drift")
    task_id = task_arn.rsplit("/", 1)[-1]
    _require(TASK_ID_RE.fullmatch(task_id) is not None, "host-gate task ID drift")
    _require(
        task.get("clusterArn") == expected_cluster_arn,
        "host-gate task cluster identity drift",
    )
    _require(
        task.get("taskDefinitionArn") == expected_task_definition_arn,
        "host-gate task definition drift",
    )
    _require(
        task.get("lastStatus") == "STOPPED" and task.get("desiredStatus") == "STOPPED",
        "host-gate task is not stopped",
    )

    containers = task.get("containers") or []
    _require(len(containers) == 1, "exactly one host-gate container is required")
    container = containers[0]
    _require(
        isinstance(container, dict) and container.get("name") == expected_container_name,
        "host-gate container identity drift",
    )
    _require(container.get("exitCode") == 0, "host-gate container did not exit zero")
    _require(
        container.get("imageDigest") == expected_image_digest,
        "host-gate image digest drift",
    )

    task_definition = task_definition_state.get("taskDefinition") or {}
    _require(isinstance(task_definition, dict), "task definition shape drift")
    _require(
        task_definition.get("taskDefinitionArn") == expected_task_definition_arn,
        "task definition identity drift",
    )
    definitions = task_definition.get("containerDefinitions") or []
    _require(len(definitions) == 1, "exactly one task-definition container is required")
    definition = definitions[0]
    _require(
        isinstance(definition, dict) and definition.get("name") == expected_container_name,
        "task-definition container identity drift",
    )
    _require(
        str(definition.get("image") or "").endswith("@" + expected_image_digest),
        "task-definition image digest drift",
    )
    log_configuration = definition.get("logConfiguration") or {}
    options = log_configuration.get("options") or {}
    _require(
        log_configuration.get("logDriver") == "awslogs"
        and options.get("awslogs-group") == expected_log_group
        and options.get("awslogs-region") == expected_log_region
        and options.get("awslogs-stream-prefix") == expected_log_prefix
        and LOG_PREFIX_RE.fullmatch(expected_log_prefix) is not None,
        "host-gate awslogs boundary drift",
    )

    details = [
        row
        for attachment in task.get("attachments") or []
        for row in attachment.get("details") or []
    ]
    private_ip_text = str(
        next(
            (
                row.get("value")
                for row in details
                if row.get("name") == "privateIPv4Address"
            ),
            "",
        )
    )
    try:
        private_ip = ipaddress.ip_address(private_ip_text)
        private_network = ipaddress.ip_network(expected_private_cidr)
    except ValueError as exc:
        raise HostGateRuntimeError("host-gate private network identity is invalid") from exc
    _require(
        isinstance(private_ip, ipaddress.IPv4Address) and private_ip in private_network,
        "host-gate private IP boundary drift",
    )

    constructed_log_stream = f"{expected_log_prefix}/{expected_container_name}/{task_id}"
    ecs_log_stream = str(container.get("logStreamName") or "")
    if ecs_log_stream:
        _require(
            ecs_log_stream == constructed_log_stream,
            "host-gate ECS log stream contradicts task definition",
        )
        log_stream_source = "exact_ecs"
    else:
        log_stream_source = "optional_absent_taskdef_bound"

    return {
        "task_id": task_id,
        "private_ip": private_ip_text,
        "log_stream": constructed_log_stream,
        "log_stream_source": log_stream_source,
    }


def write_env_file(path: Path, *, env_prefix: str, runtime: Mapping[str, str]) -> None:
    _require(ENV_PREFIX_RE.fullmatch(env_prefix) is not None, "environment prefix is invalid")
    values = {
        f"{env_prefix}_TASK_ID": runtime["task_id"],
        f"{env_prefix}_PRIVATE_IP": runtime["private_ip"],
        f"{env_prefix}_LOG_STREAM": runtime["log_stream"],
        f"{env_prefix}_LOG_STREAM_SOURCE": runtime["log_stream_source"],
    }
    _require(
        all(value and "\n" not in value and "\r" not in value for value in values.values()),
        "environment value boundary drift",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=True,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            for key, value in values.items():
                stream.write(f"{key}={value}\n")
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-state", required=True, type=Path)
    parser.add_argument("--task-definition", required=True, type=Path)
    parser.add_argument("--expected-cluster-arn", required=True)
    parser.add_argument("--expected-task-definition-arn", required=True)
    parser.add_argument("--expected-container-name", required=True)
    parser.add_argument("--expected-log-group", required=True)
    parser.add_argument("--expected-log-region", required=True)
    parser.add_argument("--expected-log-prefix", required=True)
    parser.add_argument("--expected-image-digest", required=True)
    parser.add_argument("--expected-private-cidr", required=True)
    parser.add_argument("--env-prefix", required=True)
    parser.add_argument("--env-output", required=True, type=Path)
    args = parser.parse_args()

    task_state = json.loads(args.task_state.read_text(encoding="utf-8"))
    task_definition = json.loads(args.task_definition.read_text(encoding="utf-8"))
    runtime = resolve_host_gate_runtime(
        task_state,
        task_definition,
        expected_cluster_arn=args.expected_cluster_arn,
        expected_task_definition_arn=args.expected_task_definition_arn,
        expected_container_name=args.expected_container_name,
        expected_log_group=args.expected_log_group,
        expected_log_region=args.expected_log_region,
        expected_log_prefix=args.expected_log_prefix,
        expected_image_digest=args.expected_image_digest,
        expected_private_cidr=args.expected_private_cidr,
    )
    write_env_file(args.env_output, env_prefix=args.env_prefix, runtime=runtime)
    print(
        "GROWTHBOOK_HOST_GATE_RUNTIME_OK:"
        f"task={runtime['task_id']}:instance-id=N/A:Fargate:"
        f"private-ip={runtime['private_ip']}:"
        f"log-stream={runtime['log_stream_source']}:raw=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
