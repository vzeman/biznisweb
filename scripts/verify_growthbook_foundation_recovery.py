#!/usr/bin/env python3
"""Verify the one-time VEVO Production foundation evidence-recovery provenance."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

try:
    from validate_growthbook_changeset import EXPECTED_CREATE_RESOURCES
except ModuleNotFoundError:  # Imported as scripts.verify_growthbook_foundation_recovery.
    from scripts.validate_growthbook_changeset import EXPECTED_CREATE_RESOURCES


EXPECTED_REPOSITORY = "vzeman/biznisweb"
EXPECTED_CREATION_RUN_ID = 32612205628
EXPECTED_CREATION_MAIN_COMMIT = "82d1f04c85f43d007f03090eefbeb0feb09fc140"
EXPECTED_CREATION_WORKFLOW = (
    ".github/workflows/deploy-vevo-growthbook-production-foundation.yml"
)
EXPECTED_JOB = "deploy-production-foundation"
EXPECTED_STEP_CONCLUSIONS = {
    "Enforce natural-run, empty-registry, zero-allocation, and route-disabled gates": "success",
    "Configure authenticated AWS deployment identity": "success",
    "Confirm exact Preview instance, IP, service, path, image, and Production absence": "success",
    "Prove exact source image is fail-closed in Production mode before stack create": "success",
    "Create, validate, and execute CREATE-only Production foundation change set": "success",
    "Verify route-disabled Production service and immutable runtime": "success",
    "Run exact Production Fargate localhost and marker hard gate": "success",
    "Verify service task, target health, absent route, and empty data bucket": "failure",
    "Upload sanitized Production foundation evidence only": "skipped",
}


class FoundationRecoveryVerificationError(ValueError):
    """Raised when the historical or live foundation contract drifts."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FoundationRecoveryVerificationError(message)


def _load_object(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FoundationRecoveryVerificationError(f"{field} is unreadable") from exc
    _require(isinstance(value, dict), f"{field} must be an object")
    return value


def validate_creation_run(
    run: Mapping[str, Any], jobs_payload: Mapping[str, Any]
) -> None:
    """Bind recovery to the exact failed-after-CREATE GitHub Actions run."""

    _require(run.get("id") == EXPECTED_CREATION_RUN_ID, "creation run ID drift")
    _require(run.get("event") == "workflow_dispatch", "creation run event drift")
    _require(run.get("status") == "completed", "creation run is not completed")
    _require(run.get("conclusion") == "failure", "creation run conclusion drift")
    _require(run.get("head_branch") == "main", "creation run branch drift")
    _require(
        run.get("head_sha") == EXPECTED_CREATION_MAIN_COMMIT,
        "creation run main commit drift",
    )
    _require(run.get("path") == EXPECTED_CREATION_WORKFLOW, "creation workflow drift")
    repository = run.get("repository") or {}
    _require(
        isinstance(repository, dict)
        and repository.get("full_name") == EXPECTED_REPOSITORY,
        "creation run repository drift",
    )

    jobs = jobs_payload.get("jobs")
    _require(isinstance(jobs, list) and len(jobs) == 1, "creation job count drift")
    job = jobs[0]
    _require(isinstance(job, dict), "creation job shape drift")
    _require(job.get("name") == EXPECTED_JOB, "creation job identity drift")
    _require(job.get("status") == "completed", "creation job is not completed")
    _require(job.get("conclusion") == "failure", "creation job conclusion drift")
    steps = job.get("steps")
    _require(isinstance(steps, list), "creation job steps are missing")
    conclusions: dict[str, str] = {}
    for step in steps:
        _require(isinstance(step, dict), "creation step shape drift")
        name = step.get("name")
        conclusion = step.get("conclusion")
        _require(isinstance(name, str) and name, "creation step name drift")
        _require(name not in conclusions, "creation step is duplicated")
        _require(isinstance(conclusion, str) and conclusion, "creation step conclusion drift")
        conclusions[name] = conclusion
    for name, conclusion in EXPECTED_STEP_CONCLUSIONS.items():
        _require(conclusions.get(name) == conclusion, f"creation step drift: {name}")


def validate_live_stack_resources(payload: Mapping[str, Any]) -> None:
    """Require the exact 31-resource CREATE allowlist and no route resource."""

    _require(not payload.get("NextToken"), "live stack resource listing is truncated")
    rows = payload.get("StackResourceSummaries")
    _require(isinstance(rows, list), "live stack resources are missing")
    resources: dict[str, str] = {}
    for row in rows:
        _require(isinstance(row, dict), "live stack resource shape drift")
        logical_id = row.get("LogicalResourceId")
        resource_type = row.get("ResourceType")
        status = row.get("ResourceStatus")
        _require(
            isinstance(logical_id, str) and logical_id,
            "live stack logical ID drift",
        )
        _require(logical_id not in resources, "live stack resource is duplicated")
        _require(status == "CREATE_COMPLETE", f"live stack resource status drift: {logical_id}")
        resources[logical_id] = str(resource_type or "")
    _require(resources == EXPECTED_CREATE_RESOURCES, "live stack resource allowlist drift")
    _require("CollectorPostRoute" not in resources, "public route resource unexpectedly exists")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creation-run", required=True, type=Path)
    parser.add_argument("--creation-jobs", required=True, type=Path)
    parser.add_argument("--stack-resources", required=True, type=Path)
    args = parser.parse_args()
    validate_creation_run(
        _load_object(args.creation_run, "creation run"),
        _load_object(args.creation_jobs, "creation jobs"),
    )
    validate_live_stack_resources(
        _load_object(args.stack_resources, "live stack resources")
    )
    print(
        "FOUNDATION_RECOVERY_PROVENANCE_OK:"
        f"creation-run={EXPECTED_CREATION_RUN_ID}:"
        f"creation-commit={EXPECTED_CREATION_MAIN_COMMIT}:"
        f"resources={len(EXPECTED_CREATE_RESOURCES)}:route=false:raw=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
