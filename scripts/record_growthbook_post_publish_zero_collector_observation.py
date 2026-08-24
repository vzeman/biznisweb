#!/usr/bin/env python3
"""Record sanitized VEVO post-publish zero-collector evidence offline.

This module has no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, browser, or
network client. It accepts only the canonical aggregate artifact produced by
the protected main workflow. A successful record opens only the separate
GrowthBook Production A/A start review while prices, commerce, Meta Ads, CTA,
and collector infrastructure remain closed.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import ipaddress
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from record_growthbook_natural_evidence import (
        _changed_leaf_paths,
        _write_json_atomic,
        canonical_evidence_bytes,
    )
    import validate_growthbook_production_aa_activation as activation_validator
except ModuleNotFoundError:  # Imported as scripts.*
    from scripts.record_growthbook_natural_evidence import (
        _changed_leaf_paths,
        _write_json_atomic,
        canonical_evidence_bytes,
    )
    from scripts import validate_growthbook_production_aa_activation as activation_validator


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ACTIVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_production_aa_activation.json"
)

EXPECTED_SCHEMA_VERSION = 1
EXPECTED_EVIDENCE_TYPE = (
    "vevo_growthbook_post_publish_zero_collector_observation"
)
EXPECTED_FROM_UTC = "2026-08-24T14:34:30Z"
EXPECTED_THROUGH_UTC = "2026-08-24T14:38:00Z"
EXPECTED_ROUTE_KEY = "POST /v1/events"
EXPECTED_SERVICE = "vevo-growthbook-collector-production"
EXPECTED_RUNTIME_PATH = "/app"
EXPECTED_RUNTIME_PATH_VERIFICATION = "immutable_image_prior_localhost_marker"
EXPECTED_TASK_DEFINITION = "vevo-growthbook-collector-production:2"
EXPECTED_IMAGE_DIGEST = (
    "sha256:e9aeee45f457dca5e7cb8f6a80f37763de0bb7f61c96f614d79e222fe4707058"
)
EXPECTED_SOURCE = {
    "workspace_id": "17",
    "gtm_publish_status": "published_zero_allocation",
    "gtm_live_container_version_id": "15",
    "growthbook_status": "draft_not_started",
    "production_allocation_percent": 0,
}
EXPECTED_SAFETY = {
    "aws_mutations": False,
    "biznisweb_mutations": False,
    "commerce_mutations": False,
    "contains_cloudwatch_messages": False,
    "contains_credentials": False,
    "contains_customer_or_order_data": False,
    "contains_event_or_request_ids": False,
    "growthbook_mutations": False,
    "gtm_mutations": False,
    "meta_ads_mutations": False,
}

ROOT_KEYS = {
    "schema_version",
    "evidence_type",
    "from_utc",
    "through_utc",
    "api_request_count",
    "accepted_receipt_count",
    "route_key",
    "runtime",
    "source",
    "workflow_run_id",
    "main_commit",
    "observed_at_utc",
    "safety",
}
RUNTIME_KEYS = {
    "instance_id",
    "private_ip",
    "service",
    "runtime_path",
    "runtime_path_verification",
    "task_id",
    "task_definition",
    "image_digest",
    "target_health",
}
RUNTIME_RECORD_KEYS = tuple(sorted(RUNTIME_KEYS))
OBSERVATION_RECORD_KEYS = {
    "status",
    "workflow_run_id",
    "main_commit",
    "artifact_sha256",
    "from_utc",
    "through_utc",
    "observed_at_utc",
    "api_request_count",
    "accepted_receipt_count",
    "runtime",
}
ALLOWED_CHANGED_PATHS = {
    "schema_version",
    "status",
    "activation_preflight.status",
    "activation_preflight.post_publish_readback.status",
    "activation_preflight.post_publish_readback.zero_collector_request_verified",
    "activation_preflight.post_publish_readback.zero_collector_observation",
    "activation_preflight.post_publish_readback.growthbook_start_allowed",
    "activation_preflight.mutation_scope.start_growthbook_experiment_exp_19g6mmt5wugpk",
    "activation_preflight.mutation_scope.publish_growthbook_feature_revision_3",
    "activation_preflight.ordered_operations",
    "next_gate",
}

RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
UTC_RE = re.compile(
    r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)


class PostPublishZeroCollectorEvidenceError(ValueError):
    """Raised when post-publish aggregate evidence or its gate drifts."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PostPublishZeroCollectorEvidenceError(message)


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(UTC_RE.fullmatch(text) is not None, f"{field} schema drift")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError as exc:
        raise PostPublishZeroCollectorEvidenceError(f"{field} is invalid") from exc


def expected_pending_activation() -> dict[str, Any]:
    """Return the exact schema-8 post-publish collector-pending gate."""

    return copy.deepcopy(activation_validator.EXPECTED_ACTIVATION)


def validate_post_publish_zero_collector_observation(
    evidence: Mapping[str, Any],
    *,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> None:
    """Fail closed unless evidence is the exact post-publish zero proof."""

    _require(
        RUN_ID_RE.fullmatch(expected_workflow_run_id) is not None,
        "expected workflow run ID is invalid",
    )
    _require(
        COMMIT_RE.fullmatch(expected_main_commit) is not None,
        "expected main commit is invalid",
    )
    root = _exact(evidence, ROOT_KEYS, "post-publish zero-collector evidence")
    _require(root["schema_version"] == EXPECTED_SCHEMA_VERSION, "evidence schema drift")
    _require(root["evidence_type"] == EXPECTED_EVIDENCE_TYPE, "evidence type drift")
    _require(root["workflow_run_id"] == expected_workflow_run_id, "workflow run drift")
    _require(root["main_commit"] == expected_main_commit, "main commit drift")
    _require(root["from_utc"] == EXPECTED_FROM_UTC, "observation start drift")
    _require(root["through_utc"] == EXPECTED_THROUGH_UTC, "observation end drift")
    _require(root["route_key"] == EXPECTED_ROUTE_KEY, "collector route drift")
    _require(
        type(root["api_request_count"]) is int and root["api_request_count"] == 0,
        "collector API request count must be exactly zero",
    )
    _require(
        type(root["accepted_receipt_count"]) is int
        and root["accepted_receipt_count"] == 0,
        "accepted collector receipt count must be exactly zero",
    )

    start = _parse_utc(root["from_utc"], "observation start")
    through = _parse_utc(root["through_utc"], "observation end")
    observed = _parse_utc(root["observed_at_utc"], "observed at")
    _require(start < through <= observed, "observation time order drift")
    _require(observed <= through + timedelta(hours=2), "observation was recorded too late")

    runtime = _exact(root["runtime"], RUNTIME_KEYS, "runtime")
    _require(runtime["instance_id"] == "N/A:Fargate", "runtime instance drift")
    _require(runtime["service"] == EXPECTED_SERVICE, "runtime service drift")
    _require(runtime["runtime_path"] == EXPECTED_RUNTIME_PATH, "runtime path drift")
    _require(
        runtime["runtime_path_verification"] == EXPECTED_RUNTIME_PATH_VERIFICATION,
        "runtime path verification drift",
    )
    _require(
        runtime["task_definition"] == EXPECTED_TASK_DEFINITION,
        "runtime task definition drift",
    )
    _require(runtime["image_digest"] == EXPECTED_IMAGE_DIGEST, "runtime image drift")
    _require(runtime["target_health"] == "healthy", "runtime target is not healthy")
    _require(
        TASK_ID_RE.fullmatch(str(runtime["task_id"])) is not None,
        "runtime task ID drift",
    )
    try:
        private_ip = ipaddress.ip_address(str(runtime["private_ip"]))
    except ValueError as exc:
        raise PostPublishZeroCollectorEvidenceError(
            "runtime private IP is invalid"
        ) from exc
    _require(
        private_ip in ipaddress.ip_network("172.31.0.0/16"),
        "runtime private IP boundary drift",
    )
    _require(root["source"] == EXPECTED_SOURCE, "GrowthBook/GTM source boundary drift")
    _require(root["safety"] == EXPECTED_SAFETY, "evidence safety boundary drift")


def _observation_record(
    evidence: Mapping[str, Any], evidence_sha256: str
) -> dict[str, Any]:
    runtime = evidence["runtime"]
    return {
        "status": "verified_zero_requests_and_receipts",
        "workflow_run_id": evidence["workflow_run_id"],
        "main_commit": evidence["main_commit"],
        "artifact_sha256": evidence_sha256,
        "from_utc": evidence["from_utc"],
        "through_utc": evidence["through_utc"],
        "observed_at_utc": evidence["observed_at_utc"],
        "api_request_count": evidence["api_request_count"],
        "accepted_receipt_count": evidence["accepted_receipt_count"],
        "runtime": {
            key: copy.deepcopy(runtime[key]) for key in RUNTIME_RECORD_KEYS
        },
    }


def expected_post_observation_activation(
    record: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the exact schema-9 GrowthBook-start review gate."""

    result = expected_pending_activation()
    result["schema_version"] = 9
    result["status"] = "gtm_live_zero_allocation_verified_growthbook_start_review_pending"
    preflight = result["activation_preflight"]
    preflight["status"] = (
        "gtm_live_zero_allocation_verified_growthbook_start_review_pending"
    )
    post_publish = preflight["post_publish_readback"]
    post_publish["status"] = "verified_zero_requests_and_receipts"
    post_publish["zero_collector_request_verified"] = True
    post_publish["zero_collector_observation"] = copy.deepcopy(dict(record))
    post_publish["growthbook_start_allowed"] = True
    scope = preflight["mutation_scope"]
    scope["start_growthbook_experiment_exp_19g6mmt5wugpk"] = True
    scope["publish_growthbook_feature_revision_3"] = True
    preflight["ordered_operations"] = [
        "start_only_growthbook_experiment_exp_19g6mmt5wugpk",
        "publish_only_growthbook_feature_revision_3_with_production_aa_rule",
        "verify_live_100_percent_aa_50_50_sticky_assignment_and_collector_delivery",
        "record_production_aa_activation_readback_in_git",
    ]
    result["next_gate"] = "review_growthbook_production_aa_start"
    return result


def record_post_publish_zero_collector_observation(
    activation: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> dict[str, Any]:
    """Open only the separately reviewed GrowthBook Production start gate."""

    _require(SHA256_RE.fullmatch(evidence_sha256) is not None, "evidence SHA-256 is invalid")
    validate_post_publish_zero_collector_observation(
        evidence,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
    )
    _require(
        hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest()
        == evidence_sha256,
        "evidence SHA-256 mismatch",
    )
    record = _observation_record(evidence, evidence_sha256)
    _require(
        set(record) == OBSERVATION_RECORD_KEYS,
        "post-publish observation record field set drift",
    )
    expected_result = expected_post_observation_activation(record)
    current = copy.deepcopy(dict(activation))
    if current == expected_result:
        return current
    _require(
        current == expected_pending_activation(),
        "activation manifest is not at the exact post-publish pending gate",
    )
    _require(
        current["traffic"]
        == {
            "activation_allowed": False,
            "production_allocation_percent": 0,
            "active_production_experiments": [],
            "cta_experiment_started": False,
        },
        "Production traffic gate must remain closed",
    )
    result = expected_result
    _require(
        _changed_leaf_paths(current, result) == ALLOWED_CHANGED_PATHS,
        "post-publish activation change-set boundary drift",
    )
    return result


def load_validate_and_record(
    *,
    evidence_path: Path,
    activation_path: Path,
    expected_evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> dict[str, Any]:
    try:
        raw = evidence_path.read_bytes()
        evidence = json.loads(raw.decode("utf-8"))
        activation = json.loads(activation_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PostPublishZeroCollectorEvidenceError(
            "post-publish evidence or activation manifest is unreadable"
        ) from exc
    _require(isinstance(evidence, dict), "post-publish evidence must contain an object")
    _require(isinstance(activation, dict), "activation manifest must contain an object")
    _require(raw == canonical_evidence_bytes(evidence), "evidence bytes are not canonical")
    _require(
        hashlib.sha256(raw).hexdigest() == expected_evidence_sha256,
        "evidence independent SHA-256 mismatch",
    )
    return record_post_publish_zero_collector_observation(
        activation,
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
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = load_validate_and_record(
        evidence_path=args.evidence.resolve(),
        activation_path=args.activation.resolve(),
        expected_evidence_sha256=str(args.evidence_sha256),
        expected_workflow_run_id=str(args.workflow_run_id),
        expected_main_commit=str(args.main_commit),
    )
    _write_json_atomic(args.output.resolve(), result)
    print("record_growthbook_post_publish_zero_collector_observation.py: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
