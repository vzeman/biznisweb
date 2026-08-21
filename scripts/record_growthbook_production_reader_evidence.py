#!/usr/bin/env python3
"""Build, validate, and record VEVO Production reader evidence offline.

The protected provisioning workflow creates one dedicated IAM reader and a
one-day CMS-encrypted credential handoff.  Its canonical evidence contains no
access-key ID or secret.  This recorder binds that evidence to the independently
read workflow run/main commit and opens only the separately reviewed GrowthBook
clone gate.  It has no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, or network
client.
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

try:
    from record_growthbook_foundation_evidence import (
        FoundationEvidenceRecordingError,
        validate_foundation_evidence,
    )
    from record_growthbook_natural_evidence import (
        _changed_leaf_paths,
        _write_json_atomic,
        canonical_evidence_bytes,
    )
except ModuleNotFoundError:  # Imported as scripts.record_growthbook_production_reader_evidence.
    from scripts.record_growthbook_foundation_evidence import (
        FoundationEvidenceRecordingError,
        validate_foundation_evidence,
    )
    from scripts.record_growthbook_natural_evidence import (
        _changed_leaf_paths,
        _write_json_atomic,
        canonical_evidence_bytes,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"

EXPECTED_SCHEMA_VERSION = 1
EXPECTED_EVIDENCE_TYPE = "vevo_growthbook_production_reader_evidence"
EXPECTED_REPOSITORY = "vzeman/biznisweb"
EXPECTED_WORKFLOW = ".github/workflows/provision-vevo-growthbook-production-reader.yml"
EXPECTED_ACCOUNT_ID = "919341186960"
EXPECTED_REGION = "eu-central-1"
EXPECTED_STACK_NAME = "vevo-growthbook-production"
EXPECTED_SERVICE_NAME = "vevo-growthbook-collector-production"
EXPECTED_RUNTIME_PATH = "/app"
EXPECTED_DATABASE = "vevo_growthbook_production"
EXPECTED_WORKGROUP = "vevo-growthbook-readonly-production"
EXPECTED_IAM_USER_NAME = "vevo-growthbook-production-reader"
EXPECTED_IAM_USER_PATH = "/vevo/growthbook/production/"
EXPECTED_POLICY_ARN = (
    "arn:aws:iam::919341186960:policy/vevo-growthbook-readonly-production"
)
EXPECTED_IMAGE_DIGEST = (
    "sha256:9478acd98a8caf06374b018c563ee51"
    "fa896b9cc92148238579f04aa28a134e1"
)
EXPECTED_TAGS = {
    "Environment": "production",
    "ManagedBy": "GitHubActions",
    "Project": "VEVO",
    "Purpose": "GrowthBookAthena",
}
EXPECTED_SAFETY = {
    "encrypted_handoff_created": True,
    "artifact_retention_days": 1,
    "contains_plaintext_credentials": False,
    "contains_access_key_id": False,
    "contains_secret_access_key": False,
    "contains_customer_or_order_data": False,
    "growthbook_control_plane_mutated": False,
    "gtm_mutated": False,
    "meta_ads_mutated": False,
    "biznisweb_mutated": False,
    "production_allocation_mutated": False,
}

RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
TASK_DEFINITION_RE = re.compile(r"^vevo-growthbook-collector-production:[1-9][0-9]*$")
VERIFIED_AT_RE = re.compile(
    r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)
S3_RESULTS_RE = re.compile(
    r"^s3://vevo-growthbook-production-experimentdatabucket-[a-z0-9]+/"
    r"athena-results/growthbook/$"
)

ROOT_KEYS = {
    "schema_version",
    "evidence_type",
    "status",
    "repository",
    "workflow",
    "workflow_run_id",
    "main_commit",
    "verified_at_utc",
    "foundation_evidence_provenance",
    "aws",
    "iam_identity",
    "host_gate",
    "data_access",
    "safety",
}
PROVENANCE_KEYS = {"workflow_run_id", "main_commit", "artifact_sha256"}
AWS_KEYS = {"account_id", "region", "stack_name"}
IAM_KEYS = {
    "user_name",
    "user_path",
    "policy_arn",
    "attached_policy_count",
    "inline_policy_count",
    "group_count",
    "active_access_key_count",
    "exact_tags",
}
HOST_KEYS = {
    "instance_id",
    "private_ip",
    "service",
    "runtime_path",
    "task_id",
    "task_definition",
    "image_digest",
    "localhost_health_marker_verified",
    "localhost_runtime_marker_verified",
}
DATA_ACCESS_KEYS = {
    "database",
    "workgroup",
    "s3_results_url",
    "curated_prefix_only",
    "raw_prefix_access",
}
ALLOWED_CHANGED_PATHS = {
    "athena.production.credentials_created",
    "athena.production.reader_provisioning_status",
    "athena.production.reader_provisioning_allowed",
    "athena.production.reader_evidence_artifact_status",
    "athena.production.reader_provisioning_run_id",
    "athena.production.reader_provisioning_main_commit",
    "athena.production.reader_evidence_artifact_sha256",
    "athena.production.successful_reader_provisioning",
    "athena.production.growthbook_clone.status",
    "athena.production.growthbook_clone.clone_allowed",
    "athena.production.next_gate",
}


class ReaderEvidenceRecordingError(ValueError):
    """Raised when Production reader evidence or state fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ReaderEvidenceRecordingError(message)


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(VERIFIED_AT_RE.fullmatch(text) is not None, f"{field} schema drift")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError as exc:
        raise ReaderEvidenceRecordingError(f"{field} is invalid") from exc


def _validate_provenance(
    value: Any, *, expected_run_id: str, expected_commit: str, expected_sha256: str
) -> Mapping[str, Any]:
    row = _exact(value, PROVENANCE_KEYS, "foundation evidence provenance")
    _require(row["workflow_run_id"] == expected_run_id, "foundation run ID drift")
    _require(row["main_commit"] == expected_commit, "foundation main commit drift")
    _require(row["artifact_sha256"] == expected_sha256, "foundation SHA-256 drift")
    return row


def validate_reader_evidence(
    evidence: Mapping[str, Any],
    *,
    expected_workflow_run_id: str,
    expected_main_commit: str,
    expected_foundation_run_id: str,
    expected_foundation_main_commit: str,
    expected_foundation_sha256: str,
) -> None:
    root = _exact(evidence, ROOT_KEYS, "reader evidence")
    _require(root["schema_version"] == EXPECTED_SCHEMA_VERSION, "reader schema drift")
    _require(root["evidence_type"] == EXPECTED_EVIDENCE_TYPE, "reader evidence type drift")
    _require(root["status"] == "passed", "reader evidence did not pass")
    _require(root["repository"] == EXPECTED_REPOSITORY, "reader repository drift")
    _require(root["workflow"] == EXPECTED_WORKFLOW, "reader workflow drift")
    _require(
        RUN_ID_RE.fullmatch(expected_workflow_run_id) is not None,
        "expected reader workflow run ID is invalid",
    )
    _require(
        COMMIT_RE.fullmatch(expected_main_commit) is not None,
        "expected reader main commit is invalid",
    )
    _require(root["workflow_run_id"] == expected_workflow_run_id, "reader run ID drift")
    _require(root["main_commit"] == expected_main_commit, "reader main commit drift")
    _parse_utc(root["verified_at_utc"], "reader verified_at_utc")
    _validate_provenance(
        root["foundation_evidence_provenance"],
        expected_run_id=expected_foundation_run_id,
        expected_commit=expected_foundation_main_commit,
        expected_sha256=expected_foundation_sha256,
    )

    aws = _exact(root["aws"], AWS_KEYS, "reader AWS identity")
    _require(aws == {
        "account_id": EXPECTED_ACCOUNT_ID,
        "region": EXPECTED_REGION,
        "stack_name": EXPECTED_STACK_NAME,
    }, "reader AWS identity drift")

    identity = _exact(root["iam_identity"], IAM_KEYS, "reader IAM identity")
    _require(identity["user_name"] == EXPECTED_IAM_USER_NAME, "reader IAM user drift")
    _require(identity["user_path"] == EXPECTED_IAM_USER_PATH, "reader IAM path drift")
    _require(identity["policy_arn"] == EXPECTED_POLICY_ARN, "reader policy ARN drift")
    for field, expected in (
        ("attached_policy_count", 1),
        ("inline_policy_count", 0),
        ("group_count", 0),
        ("active_access_key_count", 1),
    ):
        _require(identity[field] == expected and type(identity[field]) is int, f"reader {field} drift")
    _require(identity["exact_tags"] == EXPECTED_TAGS, "reader IAM tags drift")

    host = _exact(root["host_gate"], HOST_KEYS, "reader host gate")
    _require(host["instance_id"] == "N/A:Fargate", "reader instance ID drift")
    _require(host["service"] == EXPECTED_SERVICE_NAME, "reader service drift")
    _require(host["runtime_path"] == EXPECTED_RUNTIME_PATH, "reader runtime path drift")
    _require(TASK_ID_RE.fullmatch(str(host["task_id"])) is not None, "reader task ID drift")
    _require(
        TASK_DEFINITION_RE.fullmatch(str(host["task_definition"])) is not None,
        "reader task definition drift",
    )
    _require(host["image_digest"] == EXPECTED_IMAGE_DIGEST, "reader image digest drift")
    _require(host["localhost_health_marker_verified"] is True, "reader localhost health missing")
    _require(host["localhost_runtime_marker_verified"] is True, "reader runtime marker missing")
    try:
        private_ip = ipaddress.ip_address(str(host["private_ip"]))
    except ValueError as exc:
        raise ReaderEvidenceRecordingError("reader private IP is invalid") from exc
    _require(
        private_ip.version == 4 and private_ip.is_private and str(private_ip).startswith("172.31."),
        "reader private IP is outside the VEVO VPC",
    )

    data = _exact(root["data_access"], DATA_ACCESS_KEYS, "reader data access")
    _require(data["database"] == EXPECTED_DATABASE, "reader database drift")
    _require(data["workgroup"] == EXPECTED_WORKGROUP, "reader workgroup drift")
    _require(
        isinstance(data["s3_results_url"], str)
        and S3_RESULTS_RE.fullmatch(data["s3_results_url"]) is not None,
        "reader S3 results URL drift",
    )
    _require(data["curated_prefix_only"] is True, "reader curated-only boundary drift")
    _require(data["raw_prefix_access"] is False, "reader raw-prefix boundary drift")
    _require(root["safety"] == EXPECTED_SAFETY, "reader safety boundary drift")


def build_reader_evidence(
    *,
    verified_at: datetime,
    workflow_run_id: str,
    main_commit: str,
    foundation_workflow_run_id: str,
    foundation_main_commit: str,
    foundation_sha256: str,
    host_task_id: str,
    host_private_ip: str,
    task_definition: str,
    policy_arn: str,
    database: str,
    workgroup: str,
    s3_results_url: str,
) -> dict[str, Any]:
    _require(
        verified_at.tzinfo is not None and verified_at.utcoffset() is not None,
        "reader evidence clock must be timezone-aware",
    )
    evidence = {
        "schema_version": EXPECTED_SCHEMA_VERSION,
        "evidence_type": EXPECTED_EVIDENCE_TYPE,
        "status": "passed",
        "repository": EXPECTED_REPOSITORY,
        "workflow": EXPECTED_WORKFLOW,
        "workflow_run_id": str(workflow_run_id),
        "main_commit": str(main_commit),
        "verified_at_utc": verified_at.astimezone(timezone.utc).replace(
            microsecond=0
        ).isoformat().replace("+00:00", "Z"),
        "foundation_evidence_provenance": {
            "workflow_run_id": str(foundation_workflow_run_id),
            "main_commit": str(foundation_main_commit),
            "artifact_sha256": str(foundation_sha256),
        },
        "aws": {
            "account_id": EXPECTED_ACCOUNT_ID,
            "region": EXPECTED_REGION,
            "stack_name": EXPECTED_STACK_NAME,
        },
        "iam_identity": {
            "user_name": EXPECTED_IAM_USER_NAME,
            "user_path": EXPECTED_IAM_USER_PATH,
            "policy_arn": policy_arn,
            "attached_policy_count": 1,
            "inline_policy_count": 0,
            "group_count": 0,
            "active_access_key_count": 1,
            "exact_tags": copy.deepcopy(EXPECTED_TAGS),
        },
        "host_gate": {
            "instance_id": "N/A:Fargate",
            "private_ip": host_private_ip,
            "service": EXPECTED_SERVICE_NAME,
            "runtime_path": EXPECTED_RUNTIME_PATH,
            "task_id": host_task_id,
            "task_definition": task_definition,
            "image_digest": EXPECTED_IMAGE_DIGEST,
            "localhost_health_marker_verified": True,
            "localhost_runtime_marker_verified": True,
        },
        "data_access": {
            "database": database,
            "workgroup": workgroup,
            "s3_results_url": s3_results_url,
            "curated_prefix_only": True,
            "raw_prefix_access": False,
        },
        "safety": copy.deepcopy(EXPECTED_SAFETY),
    }
    validate_reader_evidence(
        evidence,
        expected_workflow_run_id=str(workflow_run_id),
        expected_main_commit=str(main_commit),
        expected_foundation_run_id=str(foundation_workflow_run_id),
        expected_foundation_main_commit=str(foundation_main_commit),
        expected_foundation_sha256=str(foundation_sha256),
    )
    return evidence


def record_reader_evidence(
    workspace: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> dict[str, Any]:
    _require(SHA256_RE.fullmatch(evidence_sha256) is not None, "reader evidence SHA-256 is invalid")
    result = copy.deepcopy(dict(workspace))
    production = (result.get("athena") or {}).get("production")
    _require(isinstance(production, dict), "Production state is missing")
    foundation = production.get("successful_foundation_deployment")
    _require(isinstance(foundation, dict), "verified Production foundation is missing")
    foundation_run_id = str(production.get("foundation_deployment_run_id") or "")
    foundation_main_commit = str(production.get("foundation_deployment_main_commit") or "")
    foundation_sha256 = str(production.get("foundation_evidence_artifact_sha256") or "")
    try:
        validate_foundation_evidence(
            foundation,
            expected_workflow_run_id=foundation_run_id,
            expected_main_commit=foundation_main_commit,
            expected_natural_run_id=(foundation.get("natural_evidence_provenance") or {}).get(
                "workflow_run_id"
            ),
            expected_natural_main_commit=(foundation.get("natural_evidence_provenance") or {}).get(
                "main_commit"
            ),
            expected_natural_sha256=(foundation.get("natural_evidence_provenance") or {}).get(
                "artifact_sha256"
            ),
        )
    except (FoundationEvidenceRecordingError, TypeError) as exc:
        raise ReaderEvidenceRecordingError("verified Production foundation drift") from exc
    _require(
        hashlib.sha256(canonical_evidence_bytes(foundation)).hexdigest() == foundation_sha256,
        "verified Production foundation SHA-256 drift",
    )
    validate_reader_evidence(
        evidence,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
        expected_foundation_run_id=foundation_run_id,
        expected_foundation_main_commit=foundation_main_commit,
        expected_foundation_sha256=foundation_sha256,
    )
    _require(
        hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest() == evidence_sha256,
        "reader evidence SHA-256 mismatch",
    )

    already_recorded = production.get("successful_reader_provisioning") is not None
    if already_recorded:
        _require(
            production.get("reader_evidence_artifact_status")
            == "verified_downloaded_sha256_recorded"
            and production.get("reader_provisioning_run_id") == str(expected_workflow_run_id)
            and production.get("reader_provisioning_main_commit") == str(expected_main_commit)
            and production.get("reader_evidence_artifact_sha256") == evidence_sha256
            and production.get("successful_reader_provisioning") == evidence,
            "a different Production reader evidence artifact is already recorded",
        )
        _require(
            production.get("credentials_created") is True
            and production.get("reader_provisioning_status")
            == "verified_active_encrypted_handoff_ready_for_growthbook"
            and production.get("reader_provisioning_allowed") is False
            and production.get("growthbook_clone", {}).get("status")
            == "reader_verified_ready_for_reviewed_growthbook_clone"
            and production.get("growthbook_clone", {}).get("clone_allowed") is True,
            "recorded Production reader gate state drift",
        )
        return result

    _require(
        production.get("status") == "route_disabled_foundation_deployed_verified"
        and production.get("foundation_deployment_allowed") is False
        and production.get("reader_provisioning_status")
        == "foundation_verified_ready_for_reviewed_dispatch"
        and production.get("reader_provisioning_allowed") is True,
        "Production reader is not in the reviewed ready state",
    )
    _require(production.get("credentials_created") is False, "Production credentials already recorded")
    _require(
        production.get("reader_evidence_artifact_status")
        == "code_prepared_provisioning_pending",
        "Production reader evidence state is not pending",
    )
    for key in (
        "reader_provisioning_run_id",
        "reader_provisioning_main_commit",
        "reader_evidence_artifact_sha256",
        "successful_reader_provisioning",
    ):
        _require(production.get(key) is None, f"pending reader evidence field is populated: {key}")
    clone = production.get("growthbook_clone")
    _require(
        isinstance(clone, dict)
        and clone.get("status") == "code_prepared_foundation_reader_gate_pending"
        and clone.get("clone_allowed") is False
        and clone.get("mutation_status") == "not_started",
        "GrowthBook clone gate state drift",
    )
    _require(
        (result.get("workspace") or {}).get("production_allocation_percent") == 0,
        "Production allocation must remain zero",
    )
    _require(
        (result.get("gtm_preview_workspace") or {}).get("publish_status") == "not_published",
        "GTM must remain unpublished",
    )
    _require(
        (result.get("decision_gates") or {}).get("production_activation_allowed") is False,
        "Production activation must remain disabled",
    )

    production["credentials_created"] = True
    production["reader_provisioning_status"] = (
        "verified_active_encrypted_handoff_ready_for_growthbook"
    )
    production["reader_provisioning_allowed"] = False
    production["reader_evidence_artifact_status"] = (
        "verified_downloaded_sha256_recorded"
    )
    production["reader_provisioning_run_id"] = str(expected_workflow_run_id)
    production["reader_provisioning_main_commit"] = str(expected_main_commit)
    production["reader_evidence_artifact_sha256"] = evidence_sha256
    production["successful_reader_provisioning"] = copy.deepcopy(dict(evidence))
    clone["status"] = "reader_verified_ready_for_reviewed_growthbook_clone"
    clone["clone_allowed"] = True
    production["next_gate"] = "connect_production_reader_and_clone_growthbook_after_review"
    _require(
        _changed_leaf_paths(workspace, result) == ALLOWED_CHANGED_PATHS,
        "Production reader manifest change-set boundary drift",
    )
    return result


def _write_canonical_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_evidence_bytes(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def load_validate_and_record(
    *,
    evidence_path: Path,
    workspace_path: Path,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> tuple[dict[str, Any], str]:
    try:
        raw = evidence_path.read_bytes()
        evidence = json.loads(raw.decode("utf-8"))
        workspace = json.loads(workspace_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReaderEvidenceRecordingError("reader evidence or workspace is unreadable") from exc
    _require(isinstance(evidence, dict), "reader evidence must be an object")
    _require(isinstance(workspace, dict), "workspace must be an object")
    _require(raw == canonical_evidence_bytes(evidence), "reader evidence bytes are not canonical")
    evidence_sha256 = hashlib.sha256(raw).hexdigest()
    return (
        record_reader_evidence(
            workspace,
            evidence,
            evidence_sha256=evidence_sha256,
            expected_workflow_run_id=expected_workflow_run_id,
            expected_main_commit=expected_main_commit,
        ),
        evidence_sha256,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build", help="Build canonical sanitized workflow evidence")
    build.add_argument("--workflow-run-id", required=True)
    build.add_argument("--main-commit", required=True)
    build.add_argument("--foundation-workflow-run-id", required=True)
    build.add_argument("--foundation-main-commit", required=True)
    build.add_argument("--foundation-sha256", required=True)
    build.add_argument("--host-task-id", required=True)
    build.add_argument("--host-private-ip", required=True)
    build.add_argument("--task-definition", required=True)
    build.add_argument("--policy-arn", required=True)
    build.add_argument("--database", required=True)
    build.add_argument("--workgroup", required=True)
    build.add_argument("--s3-results-url", required=True)
    build.add_argument("--output", required=True, type=Path)

    record = subparsers.add_parser("record", help="Record reviewed evidence in workspace")
    record.add_argument("--evidence", required=True, type=Path)
    record.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)
    record.add_argument("--output", required=True, type=Path)
    record.add_argument("--expected-workflow-run-id", required=True)
    record.add_argument("--expected-main-commit", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "build":
        evidence = build_reader_evidence(
            verified_at=datetime.now(timezone.utc),
            workflow_run_id=args.workflow_run_id,
            main_commit=args.main_commit,
            foundation_workflow_run_id=args.foundation_workflow_run_id,
            foundation_main_commit=args.foundation_main_commit,
            foundation_sha256=args.foundation_sha256,
            host_task_id=args.host_task_id,
            host_private_ip=args.host_private_ip,
            task_definition=args.task_definition,
            policy_arn=args.policy_arn,
            database=args.database,
            workgroup=args.workgroup,
            s3_results_url=args.s3_results_url,
        )
        _write_canonical_atomic(args.output, evidence)
        print(
            "GROWTHBOOK_PRODUCTION_READER_EVIDENCE_READY:"
            f"run={args.workflow_run_id}:output={args.output.name}:"
            "encrypted=true:credentials=false:allocation=0:clone=false"
        )
        return 0
    result, evidence_sha256 = load_validate_and_record(
        evidence_path=args.evidence,
        workspace_path=args.workspace,
        expected_workflow_run_id=args.expected_workflow_run_id,
        expected_main_commit=args.expected_main_commit,
    )
    _write_json_atomic(args.output, result)
    print(
        "GROWTHBOOK_PRODUCTION_READER_EVIDENCE_RECORDED:"
        f"run={args.expected_workflow_run_id}:commit={args.expected_main_commit}:"
        f"sha256={evidence_sha256}:output={args.output.name}:"
        "allocation=0:gtm=not_published:clone-ready=true"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
