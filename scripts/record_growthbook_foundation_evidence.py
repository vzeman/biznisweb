#!/usr/bin/env python3
"""Validate and record VEVO's route-disabled Production foundation evidence.

The recorder is offline and fail closed.  It accepts only the sanitized,
canonical artifact emitted after the CREATE-only foundation workflow passes
both localhost markers and the route-disabled service checks.  It cannot call
AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, or any network service.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import ipaddress
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from record_growthbook_natural_evidence import (
        _changed_leaf_paths,
        _write_json_atomic,
        canonical_evidence_bytes,
        validate_natural_evidence,
    )
except ModuleNotFoundError:  # Imported as scripts.record_growthbook_foundation_evidence.
    from scripts.record_growthbook_natural_evidence import (
        _changed_leaf_paths,
        _write_json_atomic,
        canonical_evidence_bytes,
        validate_natural_evidence,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"

EXPECTED_SCHEMA_VERSION = 1
EXPECTED_EVIDENCE_TYPE = "vevo_growthbook_route_disabled_production_foundation"
EXPECTED_REPOSITORY = "vzeman/biznisweb"
EXPECTED_WORKFLOW = ".github/workflows/deploy-vevo-growthbook-production-foundation.yml"
EXPECTED_ACCOUNT_ID = "919341186960"
EXPECTED_REGION = "eu-central-1"
EXPECTED_STACK_NAME = "vevo-growthbook-production"
EXPECTED_SERVICE_NAME = "vevo-growthbook-collector-production"
EXPECTED_RUNTIME_PATH = "/app"
EXPECTED_IMAGE_DIGEST = (
    "sha256:9478acd98a8caf06374b018c563ee51"
    "fa896b9cc92148238579f04aa28a134e1"
)
EXPECTED_DEPLOYMENT = {
    "change_set_type": "CREATE",
    "resource_allowlist_verified": True,
    "public_route_enabled": False,
    "api_route_count": 0,
    "external_route_status": 404,
    "event_bucket_empty": True,
    "reader_credentials_created": False,
    "experiment_registry_empty": True,
    "production_allocation_percent": 0,
    "gtm_publish_status": "not_published",
    "production_activation_allowed": False,
    "source_preview_image_reused": True,
}
EXPECTED_SAFETY = {
    "contains_raw_aws_payloads": False,
    "contains_cloudwatch_messages": False,
    "contains_credentials": False,
    "contains_customer_or_order_data": False,
    "allowed_aws_mutation": "cloudformation_create_route_disabled_foundation_only",
    "public_route_mutations": False,
    "reader_credential_mutations": False,
    "growthbook_mutations": False,
    "gtm_mutations": False,
    "meta_ads_mutations": False,
    "biznisweb_mutations": False,
}

RUN_ID_RE = re.compile(r"^[1-9][0-9]*$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
TASK_DEFINITION_RE = re.compile(r"^vevo-growthbook-collector-production:[1-9][0-9]*$")
VERIFIED_AT_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
NOT_BEFORE_UTC = datetime(2026, 8, 22, 1, 40, tzinfo=timezone.utc)

EXPECTED_ROOT_KEYS = {
    "schema_version",
    "evidence_type",
    "status",
    "repository",
    "workflow",
    "workflow_run_id",
    "main_commit",
    "verified_at_utc",
    "natural_evidence_provenance",
    "aws",
    "deployment",
    "host_gate",
    "service_runtime",
    "safety",
}
EXPECTED_PROVENANCE_KEYS = {
    "workflow_run_id",
    "main_commit",
    "artifact_sha256",
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
EXPECTED_HOST_KEYS = EXPECTED_RUNTIME_KEYS | {
    "localhost_health_marker_verified",
    "localhost_runtime_marker_verified",
}
EXPECTED_SERVICE_KEYS = EXPECTED_RUNTIME_KEYS | {"target_health"}
ALLOWED_CHANGED_PATHS = {
    "athena.production.status",
    "athena.production.deployment_allowed",
    "athena.production.foundation_deployment_status",
    "athena.production.foundation_deployment_allowed",
    "athena.production.foundation_evidence_artifact_status",
    "athena.production.foundation_deployment_run_id",
    "athena.production.foundation_deployment_main_commit",
    "athena.production.foundation_evidence_artifact_sha256",
    "athena.production.successful_foundation_deployment",
    "athena.production.reader_provisioning_status",
    "athena.production.reader_provisioning_allowed",
    "athena.production.next_gate",
}


class FoundationEvidenceRecordingError(ValueError):
    """Raised when foundation evidence or manifest state fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FoundationEvidenceRecordingError(message)


def _require_exact_keys(value: Any, expected: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == expected, f"{field} field set drift")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(VERIFIED_AT_RE.fullmatch(text) is not None, f"{field} schema drift")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError as exc:
        raise FoundationEvidenceRecordingError(f"{field} is invalid") from exc


def _validate_runtime(value: Any, expected_keys: set[str], field: str) -> Mapping[str, Any]:
    runtime = _require_exact_keys(value, expected_keys, field)
    _require(runtime["instance_id"] == "N/A:Fargate", f"{field} instance identity drift")
    _require(runtime["service"] == EXPECTED_SERVICE_NAME, f"{field} service drift")
    _require(runtime["runtime_path"] == EXPECTED_RUNTIME_PATH, f"{field} path drift")
    _require(TASK_ID_RE.fullmatch(str(runtime["task_id"])) is not None, f"{field} task ID drift")
    _require(
        TASK_DEFINITION_RE.fullmatch(str(runtime["task_definition"])) is not None,
        f"{field} task definition drift",
    )
    _require(runtime["image_digest"] == EXPECTED_IMAGE_DIGEST, f"{field} image digest drift")
    try:
        private_ip = ipaddress.ip_address(str(runtime["private_ip"]))
    except ValueError as exc:
        raise FoundationEvidenceRecordingError(f"{field} private IP is invalid") from exc
    _require(
        isinstance(private_ip, ipaddress.IPv4Address)
        and private_ip in ipaddress.ip_network("172.31.0.0/16"),
        f"{field} private IP boundary drift",
    )
    return runtime


def validate_foundation_evidence(
    evidence: Mapping[str, Any],
    *,
    expected_workflow_run_id: str,
    expected_main_commit: str,
    expected_natural_run_id: str,
    expected_natural_main_commit: str,
    expected_natural_sha256: str,
) -> None:
    """Validate exact foundation proof and its independently supplied identities."""

    run_id = str(expected_workflow_run_id or "").strip()
    main_commit = str(expected_main_commit or "").strip()
    natural_run_id = str(expected_natural_run_id or "").strip()
    natural_main_commit = str(expected_natural_main_commit or "").strip()
    natural_sha256 = str(expected_natural_sha256 or "").strip()
    _require(RUN_ID_RE.fullmatch(run_id) is not None, "expected workflow run ID is invalid")
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "expected main commit is invalid")
    _require(RUN_ID_RE.fullmatch(natural_run_id) is not None, "natural workflow run ID is invalid")
    _require(COMMIT_RE.fullmatch(natural_main_commit) is not None, "natural main commit is invalid")
    _require(SHA256_RE.fullmatch(natural_sha256) is not None, "natural evidence SHA-256 is invalid")

    root = _require_exact_keys(evidence, EXPECTED_ROOT_KEYS, "foundation evidence")
    _require(type(root["schema_version"]) is int, "foundation schema type drift")
    _require(root["schema_version"] == EXPECTED_SCHEMA_VERSION, "foundation schema drift")
    _require(root["evidence_type"] == EXPECTED_EVIDENCE_TYPE, "foundation evidence type drift")
    _require(root["status"] == "passed", "foundation evidence status is not passed")
    _require(root["repository"] == EXPECTED_REPOSITORY, "foundation repository drift")
    _require(root["workflow"] == EXPECTED_WORKFLOW, "foundation workflow drift")
    _require(root["workflow_run_id"] == run_id, "foundation workflow run ID mismatch")
    _require(root["main_commit"] == main_commit, "foundation main commit mismatch")
    _require(_parse_utc(root["verified_at_utc"], "verified_at_utc") >= NOT_BEFORE_UTC, "foundation evidence predates the natural gate")

    provenance = _require_exact_keys(
        root["natural_evidence_provenance"],
        EXPECTED_PROVENANCE_KEYS,
        "natural evidence provenance",
    )
    _require(
        provenance
        == {
            "workflow_run_id": natural_run_id,
            "main_commit": natural_main_commit,
            "artifact_sha256": natural_sha256,
        },
        "natural evidence provenance mismatch",
    )
    _require(
        root["aws"]
        == {
            "account_id": EXPECTED_ACCOUNT_ID,
            "region": EXPECTED_REGION,
            "stack_name": EXPECTED_STACK_NAME,
            "stack_status": "CREATE_COMPLETE",
        },
        "foundation AWS identity drift",
    )
    _require(root["deployment"] == EXPECTED_DEPLOYMENT, "foundation deployment boundary drift")
    host = _validate_runtime(root["host_gate"], EXPECTED_HOST_KEYS, "host gate")
    _require(host["localhost_health_marker_verified"] is True, "localhost health marker missing")
    _require(host["localhost_runtime_marker_verified"] is True, "localhost runtime marker missing")
    service = _validate_runtime(
        root["service_runtime"], EXPECTED_SERVICE_KEYS, "service runtime"
    )
    _require(service["target_health"] == "healthy", "service target is not healthy")
    _require(
        host["task_definition"] == service["task_definition"],
        "foundation runtime task definitions differ",
    )
    _require(root["safety"] == EXPECTED_SAFETY, "foundation safety boundary drift")


def build_foundation_evidence(
    *,
    verified_at: datetime,
    workflow_run_id: str,
    main_commit: str,
    natural_workflow_run_id: str,
    natural_main_commit: str,
    natural_evidence_sha256: str,
    host_task_id: str,
    host_private_ip: str,
    service_task_id: str,
    service_private_ip: str,
    task_definition: str,
) -> dict[str, Any]:
    _require(
        verified_at.tzinfo is not None and verified_at.utcoffset() is not None,
        "foundation evidence clock must be timezone-aware",
    )
    verified_utc = verified_at.astimezone(timezone.utc).replace(microsecond=0)
    runtime_common = {
        "instance_id": "N/A:Fargate",
        "service": EXPECTED_SERVICE_NAME,
        "runtime_path": EXPECTED_RUNTIME_PATH,
        "task_definition": task_definition,
        "image_digest": EXPECTED_IMAGE_DIGEST,
    }
    evidence = {
        "schema_version": EXPECTED_SCHEMA_VERSION,
        "evidence_type": EXPECTED_EVIDENCE_TYPE,
        "status": "passed",
        "repository": EXPECTED_REPOSITORY,
        "workflow": EXPECTED_WORKFLOW,
        "workflow_run_id": str(workflow_run_id),
        "main_commit": str(main_commit),
        "verified_at_utc": verified_utc.isoformat().replace("+00:00", "Z"),
        "natural_evidence_provenance": {
            "workflow_run_id": str(natural_workflow_run_id),
            "main_commit": str(natural_main_commit),
            "artifact_sha256": str(natural_evidence_sha256),
        },
        "aws": {
            "account_id": EXPECTED_ACCOUNT_ID,
            "region": EXPECTED_REGION,
            "stack_name": EXPECTED_STACK_NAME,
            "stack_status": "CREATE_COMPLETE",
        },
        "deployment": copy.deepcopy(EXPECTED_DEPLOYMENT),
        "host_gate": {
            **runtime_common,
            "private_ip": host_private_ip,
            "task_id": host_task_id,
            "localhost_health_marker_verified": True,
            "localhost_runtime_marker_verified": True,
        },
        "service_runtime": {
            **runtime_common,
            "private_ip": service_private_ip,
            "task_id": service_task_id,
            "target_health": "healthy",
        },
        "safety": copy.deepcopy(EXPECTED_SAFETY),
    }
    validate_foundation_evidence(
        evidence,
        expected_workflow_run_id=str(workflow_run_id),
        expected_main_commit=str(main_commit),
        expected_natural_run_id=str(natural_workflow_run_id),
        expected_natural_main_commit=str(natural_main_commit),
        expected_natural_sha256=str(natural_evidence_sha256),
    )
    return evidence


def record_foundation_evidence(
    workspace: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> dict[str, Any]:
    _require(SHA256_RE.fullmatch(evidence_sha256) is not None, "foundation evidence SHA-256 is invalid")
    result = copy.deepcopy(dict(workspace))
    recurring = (result.get("reconciliation_checkpoint") or {}).get("recurring_schedule")
    production = (result.get("athena") or {}).get("production")
    _require(isinstance(recurring, dict), "verified natural schedule state is missing")
    _require(isinstance(production, dict), "Production state is missing")

    natural_run_id = recurring.get("natural_verifier_run_id")
    natural_main_commit = recurring.get("natural_verifier_main_commit")
    natural_sha256 = recurring.get("natural_evidence_artifact_sha256")
    natural_evidence = recurring.get("natural_verifier_evidence")
    validate_natural_evidence(
        natural_evidence,
        expected_workflow_run_id=natural_run_id,
        expected_main_commit=natural_main_commit,
    )
    _require(
        hashlib.sha256(canonical_evidence_bytes(natural_evidence)).hexdigest()
        == natural_sha256,
        "recorded natural evidence SHA-256 drift",
    )
    validate_foundation_evidence(
        evidence,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
        expected_natural_run_id=natural_run_id,
        expected_natural_main_commit=natural_main_commit,
        expected_natural_sha256=natural_sha256,
    )
    _require(
        hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest() == evidence_sha256,
        "foundation evidence SHA-256 mismatch",
    )

    already_recorded = production.get("successful_foundation_deployment") is not None
    if already_recorded:
        _require(
            production.get("foundation_evidence_artifact_status")
            == "verified_downloaded_sha256_recorded"
            and production.get("foundation_deployment_run_id")
            == str(expected_workflow_run_id)
            and production.get("foundation_deployment_main_commit")
            == str(expected_main_commit)
            and production.get("foundation_evidence_artifact_sha256") == evidence_sha256
            and production.get("successful_foundation_deployment") == evidence,
            "a different foundation evidence artifact is already recorded",
        )
        _require(
            production.get("status") == "route_disabled_foundation_deployed_verified"
            and production.get("deployment_allowed") is False
            and production.get("foundation_deployment_status")
            == "deployed_route_disabled_verified"
            and production.get("foundation_deployment_allowed") is False
            and production.get("reader_provisioning_status")
            == "foundation_verified_ready_for_reviewed_dispatch"
            and production.get("reader_provisioning_allowed") is True
            and production.get("credentials_created") is False
            and isinstance(production.get("growthbook_clone"), dict)
            and production["growthbook_clone"].get("clone_allowed") is False,
            "recorded foundation gate state drift",
        )
        return result

    _require(
        production.get("status") == "natural_run_verified_foundation_deployment_ready"
        and production.get("deployment_allowed") is True
        and production.get("foundation_deployment_status")
        == "natural_run_verified_ready_for_reviewed_dispatch"
        and production.get("foundation_deployment_allowed") is True,
        "Production foundation is not in the reviewed ready state",
    )
    _require(
        production.get("foundation_evidence_artifact_status")
        == "code_prepared_deployment_pending",
        "foundation evidence artifact state is not pending",
    )
    for key in (
        "foundation_deployment_run_id",
        "foundation_deployment_main_commit",
        "foundation_evidence_artifact_sha256",
        "successful_foundation_deployment",
    ):
        _require(production.get(key) is None, f"pending foundation evidence field is populated: {key}")
    _require(production.get("credentials_created") is False, "Production credentials state drift")
    _require(production.get("reader_provisioning_allowed") is False, "reader gate already open")
    _require(
        isinstance(production.get("growthbook_clone"), dict)
        and production["growthbook_clone"].get("clone_allowed") is False,
        "GrowthBook clone gate must remain closed",
    )

    production["status"] = "route_disabled_foundation_deployed_verified"
    production["deployment_allowed"] = False
    production["foundation_deployment_status"] = "deployed_route_disabled_verified"
    production["foundation_deployment_allowed"] = False
    production["foundation_evidence_artifact_status"] = (
        "verified_downloaded_sha256_recorded"
    )
    production["foundation_deployment_run_id"] = str(expected_workflow_run_id)
    production["foundation_deployment_main_commit"] = str(expected_main_commit)
    production["foundation_evidence_artifact_sha256"] = evidence_sha256
    production["successful_foundation_deployment"] = copy.deepcopy(dict(evidence))
    production["reader_provisioning_status"] = (
        "foundation_verified_ready_for_reviewed_dispatch"
    )
    production["reader_provisioning_allowed"] = True
    production["next_gate"] = "dispatch_production_reader_after_review"

    _require(
        _changed_leaf_paths(workspace, result) == ALLOWED_CHANGED_PATHS,
        "foundation manifest change-set boundary drift",
    )
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
        raise FoundationEvidenceRecordingError(
            "foundation evidence or workspace JSON is unreadable"
        ) from exc
    _require(isinstance(evidence, dict), "foundation evidence must be a JSON object")
    _require(isinstance(workspace, dict), "workspace must be a JSON object")
    _require(
        evidence_bytes == canonical_evidence_bytes(evidence),
        "foundation evidence bytes are not canonical",
    )
    evidence_sha256 = hashlib.sha256(evidence_bytes).hexdigest()
    result = record_foundation_evidence(
        workspace,
        evidence,
        evidence_sha256=evidence_sha256,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
    )
    return result, evidence_sha256


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
        "GROWTHBOOK_FOUNDATION_EVIDENCE_RECORDED:"
        f"run={args.expected_workflow_run_id}:commit={args.expected_main_commit}:"
        f"sha256={evidence_sha256}:output={args.output.name}:"
        "route=false:allocation=0:reader-ready=true:clone=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
