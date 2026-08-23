#!/usr/bin/env python3
"""Validate and record sanitized VEVO Production A/A collector evidence offline."""

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
    )
except ModuleNotFoundError:  # Imported as scripts.record_growthbook_production_aa_collector_evidence.
    from scripts.record_growthbook_natural_evidence import (
        _changed_leaf_paths,
        _write_json_atomic,
        canonical_evidence_bytes,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ACTIVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_production_aa_activation.json"
)
DEFAULT_WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"
DEFAULT_REGISTRY_PATH = ROOT / "growthbook_collector" / "experiments.json"

EXPECTED_SCHEMA_VERSION = 1
EXPECTED_EVIDENCE_TYPE = "vevo_growthbook_production_aa_collector_activation"
EXPECTED_REPOSITORY = "vzeman/biznisweb"
EXPECTED_WORKFLOW = (
    ".github/workflows/deploy-vevo-growthbook-production-aa-collector.yml"
)
EXPECTED_ACCOUNT_ID = "919341186960"
EXPECTED_REGION = "eu-central-1"
EXPECTED_STACK_NAME = "vevo-growthbook-production"
EXPECTED_SERVICE = "vevo-growthbook-collector-production"
EXPECTED_RUNTIME_PATH = "/app"
EXPECTED_TRACKING_KEY = "vevo-sk-aa-001"
NOT_BEFORE_UTC = datetime(2026, 8, 22, 1, 40, tzinfo=timezone.utc)

LEGACY_COMPACT_WORKFLOW_RUN_ID = "32644408714"
LEGACY_COMPACT_MAIN_COMMIT = "57b29c3b166eabbbabee4d3b8e69d1b56e2ae8e2"
LEGACY_COMPACT_EVIDENCE_SHA256 = (
    "1e156ebdd94f88f7858c0e0b2ddb443fdabe01787ee6f7d673ac80197492ab88"
)

RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
TASK_DEFINITION_RE = re.compile(r"^vevo-growthbook-collector-production:[1-9][0-9]*$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
TIMESTAMP_RE = re.compile(
    r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
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
    "aws",
    "deployment",
    "host_gate",
    "service_runtime",
    "safety",
}
AWS_KEYS = {"account_id", "region", "stack_name", "stack_status"}
DEPLOYMENT_KEYS = {
    "registry_tracking_keys",
    "image_digest",
    "task_definition",
    "public_route_enabled",
    "endpoint_host_sha256",
    "invalid_probe_raw_snapshot_unchanged",
    "growthbook_started",
    "gtm_published",
    "production_allocation_percent",
    "cta_started",
}
RUNTIME_KEYS = {"instance_id", "private_ip", "service", "runtime_path", "task_id"}
HOST_KEYS = RUNTIME_KEYS | {
    "localhost_health_marker_verified",
    "localhost_runtime_marker_verified",
}
SERVICE_KEYS = RUNTIME_KEYS | {"target_health"}
SAFETY_KEYS = {
    "contains_credentials",
    "contains_raw_aws_payloads",
    "contains_cloudwatch_messages",
    "contains_event_or_device_ids",
    "contains_customer_or_order_data",
    "growthbook_mutations",
    "gtm_mutations",
    "meta_ads_mutations",
    "biznisweb_mutations",
    "commerce_mutations",
}
EXPECTED_SAFETY = {key: False for key in SAFETY_KEYS}
ALLOWED_CHANGED_PATHS = {
    "status",
    "collector.deployment_allowed",
    "collector.public_route_enabled",
    "collector.workflow_run_id",
    "collector.main_commit",
    "collector.image_digest",
    "collector.task_definition",
    "collector.host_gate_task_id",
    "collector.host_gate_private_ip",
    "collector.endpoint_host_sha256",
    "collector.evidence_sha256",
    "next_gate",
}


class CollectorActivationEvidenceError(ValueError):
    """Raised when collector activation evidence or state fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CollectorActivationEvidenceError(message)


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _parse_timestamp(value: Any) -> datetime:
    text = str(value or "")
    _require(TIMESTAMP_RE.fullmatch(text) is not None, "collector evidence timestamp drift")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError as exc:
        raise CollectorActivationEvidenceError(
            "collector evidence timestamp is invalid"
        ) from exc
    _require(parsed >= NOT_BEFORE_UTC, "collector evidence predates the natural-run gate")
    _require(parsed <= datetime.now(timezone.utc), "collector evidence timestamp is in the future")
    return parsed


def _legacy_compact_evidence_bytes(evidence: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            evidence,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _accepted_evidence_serializations(
    evidence: Mapping[str, Any],
    *,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> tuple[bytes, ...]:
    canonical = canonical_evidence_bytes(evidence)
    legacy = _legacy_compact_evidence_bytes(evidence)
    if (
        expected_workflow_run_id == LEGACY_COMPACT_WORKFLOW_RUN_ID
        and expected_main_commit == LEGACY_COMPACT_MAIN_COMMIT
        and evidence.get("workflow_run_id") == LEGACY_COMPACT_WORKFLOW_RUN_ID
        and evidence.get("main_commit") == LEGACY_COMPACT_MAIN_COMMIT
        and hashlib.sha256(legacy).hexdigest() == LEGACY_COMPACT_EVIDENCE_SHA256
    ):
        return canonical, legacy
    return (canonical,)


def _validate_runtime(
    value: Any,
    keys: set[str],
    field: str,
) -> Mapping[str, Any]:
    runtime = _exact(value, keys, field)
    _require(runtime["instance_id"] == "N/A:Fargate", f"{field} instance drift")
    _require(runtime["service"] == EXPECTED_SERVICE, f"{field} service drift")
    _require(runtime["runtime_path"] == EXPECTED_RUNTIME_PATH, f"{field} path drift")
    _require(TASK_ID_RE.fullmatch(str(runtime["task_id"])) is not None, f"{field} task drift")
    try:
        private_ip = ipaddress.ip_address(str(runtime["private_ip"]))
    except ValueError as exc:
        raise CollectorActivationEvidenceError(f"{field} private IP is invalid") from exc
    _require(
        isinstance(private_ip, ipaddress.IPv4Address)
        and private_ip in ipaddress.ip_network("172.31.0.0/16"),
        f"{field} private IP boundary drift",
    )
    return runtime


def validate_collector_activation_evidence(
    evidence: Mapping[str, Any],
    *,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> None:
    run_id = str(expected_workflow_run_id or "").strip()
    main_commit = str(expected_main_commit or "").strip()
    _require(RUN_ID_RE.fullmatch(run_id) is not None, "collector workflow run ID is invalid")
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "collector main commit is invalid")

    root = _exact(evidence, ROOT_KEYS, "collector activation evidence")
    _require(type(root["schema_version"]) is int, "collector evidence schema type drift")
    _require(root["schema_version"] == EXPECTED_SCHEMA_VERSION, "collector evidence schema drift")
    _require(root["evidence_type"] == EXPECTED_EVIDENCE_TYPE, "collector evidence type drift")
    _require(root["status"] == "passed", "collector evidence did not pass")
    _require(root["repository"] == EXPECTED_REPOSITORY, "collector evidence repository drift")
    _require(root["workflow"] == EXPECTED_WORKFLOW, "collector evidence workflow drift")
    _require(root["workflow_run_id"] == run_id, "collector evidence run ID mismatch")
    _require(root["main_commit"] == main_commit, "collector evidence main commit mismatch")
    _parse_timestamp(root["verified_at_utc"])

    aws = _exact(root["aws"], AWS_KEYS, "collector evidence AWS")
    _require(
        aws
        == {
            "account_id": EXPECTED_ACCOUNT_ID,
            "region": EXPECTED_REGION,
            "stack_name": EXPECTED_STACK_NAME,
            "stack_status": "UPDATE_COMPLETE",
        },
        "collector evidence AWS identity drift",
    )

    deployment = _exact(
        root["deployment"], DEPLOYMENT_KEYS, "collector evidence deployment"
    )
    _require(
        deployment["registry_tracking_keys"] == [EXPECTED_TRACKING_KEY],
        "collector Production registry evidence drift",
    )
    _require(
        IMAGE_DIGEST_RE.fullmatch(str(deployment["image_digest"])) is not None,
        "collector image digest drift",
    )
    _require(
        TASK_DEFINITION_RE.fullmatch(str(deployment["task_definition"])) is not None,
        "collector task definition drift",
    )
    _require(
        SHA256_RE.fullmatch(str(deployment["endpoint_host_sha256"])) is not None,
        "collector endpoint host hash drift",
    )
    expected_deployment_flags = {
        "public_route_enabled": True,
        "invalid_probe_raw_snapshot_unchanged": True,
        "growthbook_started": False,
        "gtm_published": False,
        "production_allocation_percent": 0,
        "cta_started": False,
    }
    for key, expected in expected_deployment_flags.items():
        _require(deployment[key] == expected, f"collector deployment flag drift: {key}")

    host = _validate_runtime(root["host_gate"], HOST_KEYS, "collector host gate")
    _require(host["localhost_health_marker_verified"] is True, "collector health marker missing")
    _require(host["localhost_runtime_marker_verified"] is True, "collector runtime marker missing")
    service = _validate_runtime(
        root["service_runtime"], SERVICE_KEYS, "collector service runtime"
    )
    _require(service["target_health"] == "healthy", "collector target is not healthy")
    _require(host["task_id"] != service["task_id"], "host gate and service task must be distinct")

    safety = _exact(root["safety"], SAFETY_KEYS, "collector evidence safety")
    _require(safety == EXPECTED_SAFETY, "collector evidence safety drift")


def record_collector_activation_evidence(
    activation: Mapping[str, Any],
    workspace: Mapping[str, Any],
    registry: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> dict[str, Any]:
    _require(SHA256_RE.fullmatch(evidence_sha256) is not None, "collector evidence SHA-256 is invalid")
    validate_collector_activation_evidence(
        evidence,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
    )
    accepted_serializations = _accepted_evidence_serializations(
        evidence,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
    )
    _require(
        evidence_sha256
        in {hashlib.sha256(payload).hexdigest() for payload in accepted_serializations},
        "collector evidence SHA-256 mismatch",
    )

    result = copy.deepcopy(dict(activation))
    collector = result.get("collector")
    _require(isinstance(collector, dict), "collector activation state is missing")
    production = (workspace.get("athena") or {}).get("production")
    _require(isinstance(production, dict), "workspace Production state is missing")
    clone = production.get("growthbook_clone")
    _require(isinstance(clone, dict), "workspace Production clone state is missing")

    _require(
        result.get("status") == "clone_verified_collector_deploy_ready",
        "collector activation is not in the reviewed ready state",
    )
    _require(
        result.get("preconditions")
        == {
            "natural_reconciliation_verified": True,
            "route_disabled_foundation_verified": True,
            "production_reader_verified": True,
            "growthbook_clone_verified": True,
        },
        "collector activation preconditions are incomplete",
    )
    _require(
        collector.get("deployment_allowed") is True
        and collector.get("registry_entry_present") is True
        and collector.get("public_route_enabled") is False,
        "collector deployment gate is not open and pending",
    )
    for key in (
        "workflow_run_id",
        "main_commit",
        "image_digest",
        "task_definition",
        "host_gate_task_id",
        "host_gate_private_ip",
        "endpoint_host_sha256",
        "evidence_sha256",
    ):
        _require(collector.get(key) is None, f"pending collector field is populated: {key}")

    _require(
        production.get("status") == "route_disabled_foundation_deployed_verified"
        and production.get("reader_provisioning_status")
        == "verified_active_encrypted_handoff_ready_for_growthbook",
        "workspace Production foundation/reader state is not verified",
    )
    _require(
        clone.get("status") == "verified_complete"
        and clone.get("mutation_status") == "created_and_query_verified"
        and isinstance(clone.get("target_data_source_id"), str),
        "workspace Production clone is not verified",
    )
    growthbook = result.get("growthbook") or {}
    _require(
        growthbook.get("data_source_id") == clone.get("target_data_source_id")
        and growthbook.get("sdk_connection_created") is False
        and growthbook.get("experiment_created") is False
        and growthbook.get("status") == "not_started"
        and growthbook.get("allocation_percent") == 0,
        "GrowthBook must remain stopped at the collector evidence boundary",
    )
    _require(
        (workspace.get("workspace") or {}).get("production_allocation_percent") == 0
        and (workspace.get("decision_gates") or {}).get("production_activation_allowed")
        is False
        and (workspace.get("gtm_preview_workspace") or {}).get("publish_status")
        == "not_published",
        "workspace traffic/GTM gate drift",
    )
    environments = registry.get("environments") or {}
    preview_aa = (environments.get("preview") or {}).get(EXPECTED_TRACKING_KEY)
    _require(
        environments.get("production") == {EXPECTED_TRACKING_KEY: preview_aa},
        "Production registry must contain only the exact A/A contract",
    )
    _require(
        (result.get("traffic") or {})
        == {
            "activation_allowed": False,
            "production_allocation_percent": 0,
            "active_production_experiments": [],
            "cta_experiment_started": False,
        },
        "traffic must remain stopped after collector evidence",
    )
    _require(
        (result.get("gtm") or {}).get("publish_status") == "not_published"
        and (result.get("gtm") or {}).get("production_tag_created") is False,
        "GTM must remain unprepared after collector evidence",
    )

    deployment = evidence["deployment"]
    host = evidence["host_gate"]
    result["status"] = "collector_verified_ui_preparation_ready"
    collector["deployment_allowed"] = False
    collector["public_route_enabled"] = True
    collector["workflow_run_id"] = str(expected_workflow_run_id)
    collector["main_commit"] = str(expected_main_commit)
    collector["image_digest"] = deployment["image_digest"]
    collector["task_definition"] = deployment["task_definition"]
    collector["host_gate_task_id"] = host["task_id"]
    collector["host_gate_private_ip"] = host["private_ip"]
    collector["endpoint_host_sha256"] = deployment["endpoint_host_sha256"]
    collector["evidence_sha256"] = evidence_sha256
    result["next_gate"] = "prepare_growthbook_and_gtm_zero_allocation_after_review"
    _require(
        _changed_leaf_paths(activation, result) == ALLOWED_CHANGED_PATHS,
        "collector activation manifest change-set boundary drift",
    )
    return result


def load_validate_and_record(
    *,
    evidence_path: Path,
    activation_path: Path,
    workspace_path: Path,
    registry_path: Path,
    expected_evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> dict[str, Any]:
    try:
        raw = evidence_path.read_bytes()
        evidence = json.loads(raw.decode("utf-8"))
        activation = json.loads(activation_path.read_text(encoding="utf-8"))
        workspace = json.loads(workspace_path.read_text(encoding="utf-8"))
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CollectorActivationEvidenceError(
            "collector evidence or manifest is unreadable"
        ) from exc
    _require(isinstance(evidence, dict), "collector evidence must contain an object")
    _require(isinstance(activation, dict), "activation manifest must contain an object")
    _require(isinstance(workspace, dict), "workspace manifest must contain an object")
    _require(isinstance(registry, dict), "registry manifest must contain an object")
    accepted_serializations = _accepted_evidence_serializations(
        evidence,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
    )
    _require(
        raw in accepted_serializations,
        "collector evidence bytes are not canonical or the pinned legacy artifact",
    )
    _require(
        hashlib.sha256(raw).hexdigest() == expected_evidence_sha256,
        "collector evidence independent SHA-256 mismatch",
    )
    return record_collector_activation_evidence(
        activation,
        workspace,
        registry,
        evidence,
        evidence_sha256=expected_evidence_sha256,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--evidence-sha256", required=True)
    parser.add_argument("--workflow-run-id", required=True)
    parser.add_argument("--main-commit", required=True)
    parser.add_argument("--activation", type=Path, default=DEFAULT_ACTIVATION_PATH)
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = load_validate_and_record(
        evidence_path=args.evidence.resolve(),
        activation_path=args.activation.resolve(),
        workspace_path=args.workspace.resolve(),
        registry_path=args.registry.resolve(),
        expected_evidence_sha256=str(args.evidence_sha256),
        expected_workflow_run_id=str(args.workflow_run_id),
        expected_main_commit=str(args.main_commit),
    )
    _write_json_atomic(args.output.resolve(), result)
    print("record_growthbook_production_aa_collector_evidence.py: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
