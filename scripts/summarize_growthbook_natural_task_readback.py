#!/usr/bin/env python3
"""Print one sanitized status for the first natural ECS task read-back.

The script never prints the AWS payload or failure detail. It distinguishes an
exact task response from the documented ECS retention outcome where
``DescribeTasks`` returns one ``MISSING`` failure for the already known task ARN.
Neither state is promoted to natural reconciliation evidence by this script.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
from typing import Any, Mapping, Sequence


TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
EXPECTED_CLUSTER_ARN = (
    "arn:aws:ecs:eu-central-1:919341186960:cluster/vevo-reporting-cluster"
)
EXPECTED_ROOT_KEYS = {"failures", "tasks"}


class TaskReadbackSummaryError(ValueError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise TaskReadbackSummaryError(message)


def _object(value: Any, field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    return value


def summarize_task_readback(
    payload: Mapping[str, Any],
    *,
    expected_task_id: str,
    expected_cluster_arn: str,
) -> str:
    root = _object(payload, "task read-back")
    _require(set(root) == EXPECTED_ROOT_KEYS, "task read-back root keys drift")
    _require(
        TASK_ID_RE.fullmatch(expected_task_id) is not None,
        "expected task ID is invalid",
    )
    _require(
        expected_cluster_arn == EXPECTED_CLUSTER_ARN,
        "expected cluster ARN is invalid",
    )
    expected_task_arn = (
        f"arn:aws:ecs:eu-central-1:919341186960:"
        f"task/vevo-reporting-cluster/{expected_task_id}"
    )

    tasks = root["tasks"]
    failures = root["failures"]
    _require(isinstance(tasks, list), "task read-back tasks must be a list")
    _require(isinstance(failures, list), "task read-back failures must be a list")
    if len(tasks) == 1 and not failures:
        task = _object(tasks[0], "task read-back task")
        _require(task.get("taskArn") == expected_task_arn, "available task ARN drift")
        return (
            "NATURAL_ECS_TASK_READBACK_AVAILABLE:"
            f"task={expected_task_id}:raw=false:evidence=false"
        )

    _require(not tasks and len(failures) == 1, "task read-back state is ambiguous")
    failure = _object(failures[0], "task read-back failure")
    _require(
        set(failure).issubset({"arn", "detail", "reason"}),
        "task read-back failure keys drift",
    )
    _require(failure.get("arn") == expected_task_arn, "missing task ARN drift")
    _require(
        failure.get("reason") == "MISSING",
        "task read-back failure is not MISSING",
    )
    return (
        "NATURAL_ECS_TASK_READBACK_EXPIRED:reason=MISSING:"
        f"task={expected_task_id}:raw=false:evidence=false"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-state", required=True, type=pathlib.Path)
    parser.add_argument("--expected-task-id", required=True)
    parser.add_argument("--expected-cluster-arn", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        payload = json.loads(args.task_state.read_text(encoding="utf-8"))
        _require(isinstance(payload, dict), "task read-back must contain an object")
        marker = summarize_task_readback(
            payload,
            expected_task_id=args.expected_task_id,
            expected_cluster_arn=args.expected_cluster_arn,
        )
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TaskReadbackSummaryError,
    ) as exc:
        print(f"NATURAL_ECS_TASK_READBACK_INVALID:{exc}")
        return 2
    print(marker)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
