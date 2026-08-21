#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EXPECTED_CREATE_RESOURCES = {
    "ReconciliationDeadLetterAlarm": "AWS::CloudWatch::Alarm",
    "ReconciliationDeadLetterQueue": "AWS::SQS::Queue",
    "ReconciliationFailureAlarm": "AWS::CloudWatch::Alarm",
    "ReconciliationFailureMetricFilter": "AWS::Logs::MetricFilter",
    "ReconciliationMissingSuccessAlarm": "AWS::CloudWatch::Alarm",
    "ReconciliationSchedule": "AWS::Scheduler::Schedule",
    "ReconciliationSchedulerRole": "AWS::IAM::Role",
    "ReconciliationSuccessMetricFilter": "AWS::Logs::MetricFilter",
}
CANDIDATE_UPDATE_RESOURCES = {
    "ReconciliationSchedule": "AWS::Scheduler::Schedule",
    "ReconciliationSchedulerRole": "AWS::IAM::Role",
}


def _load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("change set must be a JSON object")
    return payload


def with_change_set_type(
    payload: dict[str, Any], expected_change_set_type: str
) -> dict[str, Any]:
    if expected_change_set_type not in {"CREATE", "UPDATE"}:
        raise AssertionError("unsupported reconciliation change set type")
    api_value = payload.get("ChangeSetType")
    if api_value is not None and str(api_value) != expected_change_set_type:
        raise AssertionError("reconciliation change set type mismatch")
    normalized = dict(payload)
    normalized["ChangeSetType"] = expected_change_set_type
    return normalized


def _changes(payload: dict[str, Any]) -> tuple[str, list[dict[str, str]]]:
    if str(payload.get("Status") or "") != "CREATE_COMPLETE":
        raise AssertionError("reconciliation change set is not ready")
    change_set_type = str(payload.get("ChangeSetType") or "")
    if change_set_type not in {"CREATE", "UPDATE"}:
        raise AssertionError("reconciliation change set type is missing or unsupported")
    raw_changes = payload.get("Changes")
    if not isinstance(raw_changes, list) or not raw_changes:
        raise AssertionError("reconciliation change set contains no changes")
    normalized = []
    for wrapper in raw_changes:
        change = wrapper.get("ResourceChange") if isinstance(wrapper, dict) else None
        if not isinstance(change, dict):
            raise AssertionError("reconciliation change set contains a malformed change")
        row = {
            "action": str(change.get("Action") or ""),
            "logical_id": str(change.get("LogicalResourceId") or ""),
            "replacement": str(change.get("Replacement") or "False"),
            "resource_type": str(change.get("ResourceType") or ""),
        }
        if row["action"] not in {"Add", "Modify"}:
            raise AssertionError(
                f"destructive reconciliation change rejected: {row['action']} {row['logical_id']}"
            )
        if not row["logical_id"] or not row["resource_type"]:
            raise AssertionError("reconciliation resource identity is incomplete")
        normalized.append(row)
    if len({row["logical_id"] for row in normalized}) != len(normalized):
        raise AssertionError("reconciliation change set repeats a logical resource")
    return change_set_type, normalized


def validate(payload: dict[str, Any], phase: str) -> list[dict[str, str]]:
    change_set_type, normalized = _changes(payload)
    resources = {row["logical_id"]: row["resource_type"] for row in normalized}
    if phase == "candidate":
        if change_set_type == "CREATE":
            if resources != EXPECTED_CREATE_RESOURCES:
                raise AssertionError(
                    "reconciliation CREATE resource contract mismatch: "
                    f"actual={sorted(resources)}"
                )
            if any(
                row["action"] != "Add" or row["replacement"] != "False"
                for row in normalized
            ):
                raise AssertionError("reconciliation CREATE must be non-replacement adds only")
        else:
            if not resources or not set(resources).issubset(CANDIDATE_UPDATE_RESOURCES):
                raise AssertionError(
                    f"unrelated reconciliation update rejected: {sorted(resources)}"
                )
            for row in normalized:
                if row["resource_type"] != CANDIDATE_UPDATE_RESOURCES[row["logical_id"]]:
                    raise AssertionError("reconciliation update resource type mismatch")
                if row["action"] != "Modify" or row["replacement"] != "False":
                    raise AssertionError("reconciliation candidate update must not replace resources")
    elif phase == "activate":
        expected = [{
            "action": "Modify",
            "logical_id": "ReconciliationSchedule",
            "replacement": "False",
            "resource_type": "AWS::Scheduler::Schedule",
        }]
        if change_set_type != "UPDATE" or normalized != expected:
            raise AssertionError(f"unexpected reconciliation activation change set: {normalized}")
    else:
        raise AssertionError("unsupported reconciliation validation phase")
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fail closed on unsafe GrowthBook reconciliation change sets"
    )
    parser.add_argument("--change-set", type=Path, required=True)
    parser.add_argument("--change-set-type", choices=("CREATE", "UPDATE"), required=True)
    parser.add_argument("--phase", choices=("candidate", "activate"), required=True)
    args = parser.parse_args()
    payload = with_change_set_type(_load(args.change_set), args.change_set_type)
    print(json.dumps(validate(payload, args.phase), separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
