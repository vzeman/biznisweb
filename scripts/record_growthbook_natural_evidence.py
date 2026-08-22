#!/usr/bin/env python3
"""Validate and record the first natural VEVO GrowthBook evidence artifact.

This is an offline, fail-closed manifest transformation.  It has no AWS,
GrowthBook, GTM, Meta Ads, BiznisWeb, or network client and it never dispatches
a workflow.  The caller must independently obtain the exact successful GitHub
workflow run ID and main commit, then review the resulting git diff.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import ipaddress
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"

EXPECTED_SCHEMA_VERSION = 2
EXPECTED_EVIDENCE_TYPE = "vevo_growthbook_natural_reconciliation_retention_recovery"
EXPECTED_REPOSITORY = "vzeman/biznisweb"
EXPECTED_WORKFLOW = ".github/workflows/verify-vevo-growthbook-natural-reconciliation.yml"
EXPECTED_ACCOUNT_ID = "919341186960"
EXPECTED_REGION = "eu-central-1"
EXPECTED_STACK_NAME = "vevo-growthbook-reconciliation-preview"
EXPECTED_SCHEDULE_NAME = "vevo-growthbook-reconcile-preview"
EXPECTED_SOURCE_SCHEDULE = "vevo-daily-report-email"
EXPECTED_TASK_DEFINITION = "vevo-growthbook-reconcile-preview:4"
EXPECTED_IMAGE_DIGEST = (
    "sha256:cabba3b0bd57f6be322f3a5ff62f0327"
    "c7cf8e7bb2b6b5e78686305339fdd041"
)
EXPECTED_EVENT_FROM = "2026-07-14"
EXPECTED_EVENT_THROUGH = "2026-08-22"
EXPECTED_PARTITIONS = 40
MAX_RAW_EVENTS = 50_000
EXPECTED_VERIFICATION_WINDOW = {
    "target_run_due_utc": "2026-08-23T01:30:00Z",
    "not_before_utc": "2026-08-23T01:40:00Z",
    "before_utc": "2026-08-23T02:20:00Z",
}
EXPECTED_SAFETY = {
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
}

RUN_ID_RE = re.compile(r"^[1-9][0-9]*$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
VERIFIED_AT_RE = re.compile(r"^2026-08-23T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
EXPECTED_ROOT_KEYS = {
    "schema_version",
    "evidence_type",
    "status",
    "repository",
    "workflow",
    "workflow_run_id",
    "main_commit",
    "verified_at_utc",
    "verification_window",
    "aws",
    "runtime",
    "reconciliation",
    "control_plane",
    "safety",
}
EXPECTED_RUNTIME_KEYS = {
    "instance_id",
    "private_ip",
    "service",
    "runtime_path",
    "task_id",
    "task_definition",
    "image_digest",
}
EXPECTED_RECONCILIATION_KEYS = {
    "event_from",
    "event_through",
    "partitions",
    "raw_events",
    "device_facts",
    "performance_facts",
    "quality_reports",
    "generated_published_counts_match",
}
EXPECTED_CONTROL_KEYS = {
    "dlq_empty",
    "alarms_clear",
    "source_schedule_enabled",
    "cloudtrail_scheduler_run_task_verified",
}
ALLOWED_CHANGED_PATHS = {
    "reconciliation_checkpoint.recurring_schedule_status",
    "reconciliation_checkpoint.recurring_schedule.first_natural_run_status",
    "reconciliation_checkpoint.recurring_schedule.natural_verifier_status",
    "reconciliation_checkpoint.recurring_schedule.natural_evidence_artifact_status",
    "reconciliation_checkpoint.recurring_schedule.natural_verifier_run_id",
    "reconciliation_checkpoint.recurring_schedule.natural_verifier_main_commit",
    "reconciliation_checkpoint.recurring_schedule.natural_evidence_artifact_sha256",
    "reconciliation_checkpoint.recurring_schedule.natural_verifier_evidence",
    "athena.production.status",
    "athena.production.deployment_allowed",
    "athena.production.foundation_deployment_status",
    "athena.production.foundation_deployment_allowed",
    "athena.production.next_gate",
}


class EvidenceRecordingError(ValueError):
    """Raised when evidence or manifest state is unsafe or inconsistent."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceRecordingError(message)


def canonical_evidence_bytes(evidence: Mapping[str, Any]) -> bytes:
    return (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _parse_utc(text: Any, field: str) -> datetime:
    value = str(text or "")
    _require(VERIFIED_AT_RE.fullmatch(value) is not None, f"{field} schema drift")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceRecordingError(f"{field} is invalid") from exc
    return parsed.astimezone(timezone.utc)


def _require_exact_keys(value: Any, expected: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == expected, f"{field} field set drift")
    return value


def validate_natural_evidence(
    evidence: Mapping[str, Any],
    *,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> None:
    """Validate the exact safe schema and its independent GitHub identity."""

    run_id = str(expected_workflow_run_id or "").strip()
    main_commit = str(expected_main_commit or "").strip()
    _require(RUN_ID_RE.fullmatch(run_id) is not None, "expected workflow run ID is invalid")
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "expected main commit is invalid")

    root = _require_exact_keys(evidence, EXPECTED_ROOT_KEYS, "evidence")
    _require(type(root["schema_version"]) is int, "schema version type drift")
    _require(root["schema_version"] == EXPECTED_SCHEMA_VERSION, "schema version drift")
    _require(root["evidence_type"] == EXPECTED_EVIDENCE_TYPE, "evidence type drift")
    _require(root["status"] == "passed", "evidence status is not passed")
    _require(root["repository"] == EXPECTED_REPOSITORY, "repository identity drift")
    _require(root["workflow"] == EXPECTED_WORKFLOW, "workflow identity drift")
    _require(root["workflow_run_id"] == run_id, "workflow run ID mismatch")
    _require(root["main_commit"] == main_commit, "main commit mismatch")

    _require(
        root["verification_window"] == EXPECTED_VERIFICATION_WINDOW,
        "verification window drift",
    )
    verified_at = _parse_utc(root["verified_at_utc"], "verified_at_utc")
    not_before = datetime.fromisoformat(
        EXPECTED_VERIFICATION_WINDOW["not_before_utc"].replace("Z", "+00:00")
    )
    before = datetime.fromisoformat(
        EXPECTED_VERIFICATION_WINDOW["before_utc"].replace("Z", "+00:00")
    )
    _require(not_before <= verified_at < before, "verified_at_utc is outside the gate")

    _require(
        root["aws"]
        == {
            "account_id": EXPECTED_ACCOUNT_ID,
            "region": EXPECTED_REGION,
            "stack_name": EXPECTED_STACK_NAME,
            "schedule_name": EXPECTED_SCHEDULE_NAME,
            "source_schedule": EXPECTED_SOURCE_SCHEDULE,
        },
        "AWS identity drift",
    )

    runtime = _require_exact_keys(root["runtime"], EXPECTED_RUNTIME_KEYS, "runtime")
    _require(runtime["instance_id"] == "N/A:Fargate", "runtime instance identity drift")
    _require(runtime["service"] == EXPECTED_SCHEDULE_NAME, "runtime service drift")
    _require(runtime["runtime_path"] == "/app", "runtime path drift")
    _require(TASK_ID_RE.fullmatch(str(runtime["task_id"])) is not None, "runtime task ID drift")
    _require(runtime["task_definition"] == EXPECTED_TASK_DEFINITION, "task definition drift")
    _require(runtime["image_digest"] == EXPECTED_IMAGE_DIGEST, "image digest drift")
    try:
        private_ip = ipaddress.ip_address(str(runtime["private_ip"]))
    except ValueError as exc:
        raise EvidenceRecordingError("runtime private IP is invalid") from exc
    _require(
        isinstance(private_ip, ipaddress.IPv4Address)
        and private_ip in ipaddress.ip_network("172.31.0.0/16"),
        "runtime private IP boundary drift",
    )

    reconciliation = _require_exact_keys(
        root["reconciliation"], EXPECTED_RECONCILIATION_KEYS, "reconciliation"
    )
    _require(reconciliation["event_from"] == EXPECTED_EVENT_FROM, "event-from drift")
    _require(reconciliation["event_through"] == EXPECTED_EVENT_THROUGH, "event-through drift")
    _require(type(reconciliation["partitions"]) is int, "partition count type drift")
    _require(reconciliation["partitions"] == EXPECTED_PARTITIONS, "partition count drift")
    for key in ("raw_events", "device_facts", "performance_facts", "quality_reports"):
        _require(type(reconciliation[key]) is int, f"{key} type drift")
        _require(reconciliation[key] >= 0, f"{key} must be nonnegative")
    _require(reconciliation["raw_events"] <= MAX_RAW_EVENTS, "raw-event bound drift")
    _require(
        reconciliation["generated_published_counts_match"] is True,
        "generated/published parity is not verified",
    )

    control = _require_exact_keys(root["control_plane"], EXPECTED_CONTROL_KEYS, "control plane")
    _require(all(control[key] is True for key in EXPECTED_CONTROL_KEYS), "control-plane gate failed")
    _require(root["safety"] == EXPECTED_SAFETY, "evidence safety boundary drift")


def _changed_leaf_paths(before: Any, after: Any, prefix: str = "") -> set[str]:
    if isinstance(before, dict) and isinstance(after, dict):
        paths: set[str] = set()
        for key in set(before) | set(after):
            child = f"{prefix}.{key}" if prefix else str(key)
            if key not in before or key not in after:
                paths.add(child)
            else:
                paths.update(_changed_leaf_paths(before[key], after[key], child))
        return paths
    return set() if before == after else {prefix}


def record_natural_evidence(
    workspace: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> dict[str, Any]:
    """Return a copied manifest with only the reviewed foundation gates opened."""

    _require(SHA256_RE.fullmatch(evidence_sha256) is not None, "evidence SHA-256 is invalid")
    validate_natural_evidence(
        evidence,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
    )
    expected_sha256 = hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest()
    _require(evidence_sha256 == expected_sha256, "evidence SHA-256 mismatch")

    result = copy.deepcopy(dict(workspace))
    checkpoint = result.get("reconciliation_checkpoint")
    athena = result.get("athena")
    _require(isinstance(checkpoint, dict), "reconciliation checkpoint is missing")
    _require(isinstance(athena, dict), "Athena state is missing")
    recurring = checkpoint.get("recurring_schedule")
    production = athena.get("production")
    _require(isinstance(recurring, dict), "recurring schedule state is missing")
    _require(isinstance(production, dict), "Production state is missing")

    already_recorded = (
        recurring.get("first_natural_run_status")
        == "verified_via_second_natural_run"
        and recurring.get("natural_verifier_status")
        == "passed_retention_recovery_run"
        and recurring.get("natural_evidence_artifact_status")
        == "verified_downloaded_sha256_recorded"
    )
    if already_recorded:
        _require(
            recurring.get("natural_verifier_run_id") == expected_workflow_run_id
            and recurring.get("natural_verifier_main_commit") == expected_main_commit
            and recurring.get("natural_evidence_artifact_sha256") == evidence_sha256
            and recurring.get("natural_verifier_evidence") == evidence,
            "a different natural evidence artifact is already recorded",
        )
        _require(
            checkpoint.get("recurring_schedule_status")
            == "enabled_one_shot_and_natural_retention_recovery_verified"
            and production.get("status")
            == "natural_run_verified_foundation_deployment_ready"
            and production.get("deployment_allowed") is True
            and production.get("foundation_deployment_status")
            == "natural_run_verified_ready_for_reviewed_dispatch"
            and production.get("foundation_deployment_allowed") is True
            and production.get("next_gate")
            == "dispatch_route_disabled_production_foundation_after_review"
            and production.get("credentials_created") is False
            and production.get("reader_provisioning_allowed") is False
            and isinstance(production.get("growthbook_clone"), dict)
            and production["growthbook_clone"].get("clone_allowed") is False,
            "recorded natural evidence gate state drift",
        )
        return result

    _require(
        checkpoint.get("recurring_schedule_status")
        == "enabled_one_shot_verified_natural_retention_recovery_pending",
        "recurring schedule checkpoint is not pending",
    )
    _require(
        recurring.get("first_natural_run_status")
        == "success_marker_observed_ecs_state_expired_recovery_pending",
        "natural retention recovery is not pending",
    )
    _require(
        recurring.get("natural_verifier_status")
        == "prepared_second_natural_run_retention_recovery",
        "natural verifier state is not pending",
    )
    _require(
        recurring.get("natural_evidence_artifact_status")
        == "code_prepared_retention_recovery_pending",
        "natural evidence artifact state is not pending",
    )
    for key in (
        "natural_verifier_run_id",
        "natural_verifier_main_commit",
        "natural_evidence_artifact_sha256",
        "natural_verifier_evidence",
    ):
        _require(recurring.get(key) is None, f"pending natural evidence field is already populated: {key}")
    _require(recurring.get("natural_verifier_mutation_allowed") is False, "verifier mutation gate drift")
    _require(
        production.get("status") == "read_only_preflight_passed_natural_run_gate_pending",
        "Production status is not pending",
    )
    _require(production.get("deployment_allowed") is False, "Production deployment gate already open")
    _require(
        production.get("foundation_deployment_status")
        == "code_prepared_natural_run_gate_pending",
        "Production foundation status is not pending",
    )
    _require(
        production.get("foundation_deployment_allowed") is False,
        "Production foundation gate already open",
    )
    _require(production.get("credentials_created") is False, "Production credentials state drift")
    _require(production.get("reader_provisioning_allowed") is False, "reader gate must remain closed")
    clone = production.get("growthbook_clone") or {}
    _require(isinstance(clone, dict) and clone.get("clone_allowed") is False, "clone gate must remain closed")

    checkpoint["recurring_schedule_status"] = (
        "enabled_one_shot_and_natural_retention_recovery_verified"
    )
    recurring["first_natural_run_status"] = "verified_via_second_natural_run"
    recurring["natural_verifier_status"] = "passed_retention_recovery_run"
    recurring["natural_evidence_artifact_status"] = "verified_downloaded_sha256_recorded"
    recurring["natural_verifier_run_id"] = expected_workflow_run_id
    recurring["natural_verifier_main_commit"] = expected_main_commit
    recurring["natural_evidence_artifact_sha256"] = evidence_sha256
    recurring["natural_verifier_evidence"] = copy.deepcopy(dict(evidence))
    production["status"] = "natural_run_verified_foundation_deployment_ready"
    production["deployment_allowed"] = True
    production["foundation_deployment_status"] = "natural_run_verified_ready_for_reviewed_dispatch"
    production["foundation_deployment_allowed"] = True
    production["next_gate"] = "dispatch_route_disabled_production_foundation_after_review"

    changed_paths = _changed_leaf_paths(workspace, result)
    _require(changed_paths == ALLOWED_CHANGED_PATHS, "manifest change-set boundary drift")
    return result


def load_validate_and_record(
    *,
    evidence_path: Path,
    workspace_path: Path,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> tuple[dict[str, Any], str]:
    try:
        evidence_bytes = evidence_path.read_bytes()
        evidence = json.loads(evidence_bytes.decode("utf-8"))
        workspace = json.loads(workspace_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceRecordingError("evidence or workspace JSON is unreadable") from exc
    _require(isinstance(evidence, dict), "evidence must be a JSON object")
    _require(isinstance(workspace, dict), "workspace must be a JSON object")
    _require(evidence_bytes == canonical_evidence_bytes(evidence), "evidence bytes are not canonical")
    evidence_sha256 = hashlib.sha256(evidence_bytes).hexdigest()
    result = record_natural_evidence(
        workspace,
        evidence,
        evidence_sha256=evidence_sha256,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
    )
    return result, evidence_sha256


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        os.replace(temporary_path, path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--expected-workflow-run-id", required=True)
    parser.add_argument("--expected-main-commit", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result, evidence_sha256 = load_validate_and_record(
        evidence_path=args.evidence,
        workspace_path=args.workspace,
        expected_workflow_run_id=args.expected_workflow_run_id,
        expected_main_commit=args.expected_main_commit,
    )
    _write_json_atomic(args.output, result)
    print(
        "GROWTHBOOK_NATURAL_EVIDENCE_RECORDED:"
        f"run={args.expected_workflow_run_id}:"
        f"commit={args.expected_main_commit}:sha256={evidence_sha256}:"
        f"output={args.output.name}:production-allocation=0:reader=false:clone=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
