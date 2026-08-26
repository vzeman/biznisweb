#!/usr/bin/env python3
"""Record VEVO Production A/A completion transitions offline.

``record-pass`` accepts only an exact canonical snapshot, matching canonical
decision, and workflow provenance from the protected snapshot workflow. It
independently recomputes the decision and opens only the reviewed manual A/A
stop gate.

``record-stop`` accepts only a canonical reviewed post-stop readback. It closes
the stop gate and updates a supplied workspace copy to the post-A/A, zero-
allocation CTA-draft state. The tool has no browser, network, AWS, GrowthBook,
GTM, Meta Ads, BiznisWeb, collector, reporting, or commerce client.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scripts.evaluate_growthbook_aa import (
        DEFAULT_CONFIG_PATH as AA_CONFIG_PATH,
        AaEvaluationError,
        evaluate as evaluate_aa,
        load_config as load_aa_config,
    )
    from scripts.validate_growthbook_aa_measurement_window import (
        ACCEPTANCE_PATH,
        ACTIVATION_PATH,
        RECONCILIATION_EVIDENCE_PATH,
        SNAPSHOT_PATH,
        MeasurementWindowError,
        validate_measurement_window,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from evaluate_growthbook_aa import (
        DEFAULT_CONFIG_PATH as AA_CONFIG_PATH,
        AaEvaluationError,
        evaluate as evaluate_aa,
        load_config as load_aa_config,
    )
    from validate_growthbook_aa_measurement_window import (
        ACCEPTANCE_PATH,
        ACTIVATION_PATH,
        RECONCILIATION_EVIDENCE_PATH,
        SNAPSHOT_PATH,
        MeasurementWindowError,
        validate_measurement_window,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_COMPLETION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_production_aa_completion.json"
)
DEFAULT_WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"
DEFAULT_OBSERVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_aa_completion_observation.json"
)

RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(
    r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)
PROVENANCE_TYPE = "vevo_growthbook_aa_snapshot_provenance"
PROVENANCE_WORKFLOW = (
    ".github/workflows/build-vevo-growthbook-production-aa-snapshot.yml"
)
PROVENANCE_ARTIFACT = "vevo-growthbook-aa-snapshot"
SNAPSHOT_FILE_NAME = "vevo-growthbook-aa-snapshot.json"
DECISION_FILE_NAME = "vevo-growthbook-aa-decision.json"
PROVENANCE_FILE_NAME = "vevo-growthbook-aa-provenance.json"
PROVENANCE_KEYS = {
    "schema_version",
    "evidence_type",
    "repository",
    "workflow",
    "workflow_run_id",
    "workflow_run_attempt",
    "main_commit",
    "artifact_name",
    "files",
    "source_components",
    "safety",
}
PROVENANCE_SOURCE_KEYS = {
    "workflow",
    "workflow_run_id",
    "main_commit",
    "artifact_name",
    "artifact_sha256",
}
PROVENANCE_SAFETY = {
    "contains_component_artifacts": False,
    "contains_raw_aws_payloads": False,
    "contains_event_or_device_ids": False,
    "contains_customer_or_order_data": False,
    "external_or_automatic_mutation": False,
    "winner_calls_allowed": False,
}

ROOT_KEYS = {
    "schema_version",
    "completion_type",
    "experiment_id",
    "source_activation_manifest",
    "source_activation_sha256",
    "source_snapshot_manifest",
    "status",
    "aa_pass",
    "stop_readback",
    "next_state",
    "release_boundaries",
    "next_gate",
}
AA_PASS_KEYS = {
    "status",
    "snapshot_workflow",
    "artifact_name",
    "snapshot_file",
    "decision_file",
    "provenance_file",
    "workflow_run_id",
    "main_commit",
    "snapshot_sha256",
    "decision_sha256",
    "provenance_sha256",
    "evaluated_at_utc",
    "verdict",
    "winner_calls_allowed",
}
STOP_READBACK_KEYS = {
    "status",
    "observation_file",
    "observation_sha256",
    "observed_at_utc",
    "growthbook_build",
    "feature_revision",
    "gtm_container_version_id",
}
NEXT_STATE = {
    "workspace_state": (
        "production_aa_completed_cta_sample_freeze_pending_pro_quantiles_blocked"
    ),
    "production_allocation_percent": 0,
    "cta_status": "draft",
    "cta_feature_rule_status": "draft",
    "cta_production_allocation_percent": 0,
    "cta_activation_allowed": False,
}
BOUNDARY_KEYS = {
    "manual_growthbook_stop_allowed",
    "automatic_growthbook_mutation_allowed",
    "gtm_mutation_allowed",
    "meta_ads_mutation_allowed",
    "biznisweb_mutation_allowed",
    "collector_or_reporting_mutation_allowed",
    "price_cart_checkout_order_mutation_allowed",
    "cta_activation_allowed",
}
OBSERVATION_KEYS = {
    "schema_version",
    "observation_type",
    "experiment_id",
    "observed_at_utc",
    "aa_pass_snapshot_sha256",
    "aa_pass_decision_sha256",
    "aa_pass_provenance_sha256",
    "growthbook",
    "gtm",
    "storefront",
    "mutation_boundaries",
}
GROWTHBOOK_OBSERVATION_KEYS = {
    "build",
    "project_id",
    "environment",
    "aa_experiment_id",
    "aa_experiment_status",
    "aa_production_live_rule_count",
    "aa_production_allocation_percent",
    "aa_feature_live_revision",
    "aa_feature_production_enabled",
    "aa_feature_staging_enabled",
    "aa_feature_live_rule_count_by_environment",
    "cta_experiment_id",
    "cta_experiment_status",
    "cta_production_live_rule_count",
    "cta_production_allocation_percent",
}
GTM_OBSERVATION_KEYS = {
    "account_id",
    "container_id",
    "public_container_id",
    "container_version_id",
    "growthbook_loader_active",
    "unprocessed_changes",
}
STOREFRONT_OBSERVATION_KEYS = {
    "product_path",
    "desktop_verified",
    "mobile_verified",
    "aa_assignment_present",
    "cta_class_applied",
    "add_to_cart_text_unchanged",
    "console_error_count",
    "price_mutated",
    "cart_mutated",
    "checkout_or_order_mutated",
}
MUTATION_OBSERVATION_KEYS = {
    "growthbook_manual_mutation_performed",
    "growthbook_manual_mutation_scope",
    "automatic_growthbook_mutation_performed",
    "gtm_mutation_performed",
    "meta_ads_mutation_performed",
    "biznisweb_mutation_performed",
    "collector_or_reporting_mutation_performed",
    "price_cart_checkout_order_mutation_performed",
}


class AaCompletionRecordingError(ValueError):
    """Raised when an A/A completion transition fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AaCompletionRecordingError(message)


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(UTC_RE.fullmatch(text) is not None, f"{field} must be whole-second UTC Z")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AaCompletionRecordingError(f"{field} is invalid") from exc
    _require(parsed.tzinfo == UTC and not parsed.microsecond, f"{field} is invalid")
    return parsed


def _load(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AaCompletionRecordingError(f"{field} is unreadable") from exc
    _require(isinstance(value, dict), f"{field} must contain an object")
    return value


def load_canonical(path: Path, expected_sha256: str, field: str) -> dict[str, Any]:
    digest = str(expected_sha256 or "").strip()
    _require(SHA256_RE.fullmatch(digest) is not None, f"{field} SHA-256 is invalid")
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AaCompletionRecordingError(f"{field} is unreadable") from exc
    _require(isinstance(value, dict), f"{field} must contain an object")
    _require(raw == canonical_json_bytes(value), f"{field} is not canonical JSON")
    _require(hashlib.sha256(raw).hexdigest() == digest, f"{field} SHA-256 mismatch")
    return value


def _activation_digest(activation: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(activation)).hexdigest()


def validate_provenance(
    provenance: Mapping[str, Any],
    snapshot_manifest: Mapping[str, Any],
    *,
    provenance_sha256: str,
    snapshot_sha256: str,
    decision_sha256: str,
    workflow_run_id: str,
    main_commit: str,
) -> None:
    root = _exact(provenance, PROVENANCE_KEYS, "A/A snapshot provenance")
    _require(
        root["schema_version"] == 1
        and root["evidence_type"] == PROVENANCE_TYPE,
        "A/A snapshot provenance identity drift",
    )
    _require(
        root["repository"] == "vzeman/biznisweb"
        and root["workflow"] == PROVENANCE_WORKFLOW,
        "A/A snapshot provenance source drift",
    )
    _require(
        root["workflow_run_id"] == workflow_run_id
        and RUN_ID_RE.fullmatch(workflow_run_id) is not None,
        "A/A snapshot provenance workflow run mismatch",
    )
    _require(
        root["workflow_run_attempt"] == 1,
        "A/A snapshot provenance must come from the first workflow attempt",
    )
    _require(
        root["main_commit"] == main_commit
        and COMMIT_RE.fullmatch(main_commit) is not None,
        "A/A snapshot provenance main commit mismatch",
    )
    _require(
        root["artifact_name"] == PROVENANCE_ARTIFACT,
        "A/A snapshot provenance artifact drift",
    )
    files = _exact(
        root["files"],
        {SNAPSHOT_FILE_NAME, DECISION_FILE_NAME},
        "A/A snapshot provenance files",
    )
    for file_name, expected_sha256 in (
        (SNAPSHOT_FILE_NAME, snapshot_sha256),
        (DECISION_FILE_NAME, decision_sha256),
    ):
        row = _exact(
            files[file_name], {"sha256"}, f"A/A snapshot provenance {file_name}"
        )
        _require(
            row["sha256"] == expected_sha256
            and SHA256_RE.fullmatch(str(row["sha256"] or "")) is not None,
            f"A/A snapshot provenance hash mismatch: {file_name}",
        )

    sources = _exact(
        root["source_components"],
        {"automated_evidence", "manual_qa_evidence"},
        "A/A snapshot provenance source components",
    )
    for component_name in ("automated_evidence", "manual_qa_evidence"):
        component = snapshot_manifest.get(component_name) or {}
        expected = {
            "workflow": component.get("workflow"),
            "workflow_run_id": str(component.get("run_id") or ""),
            "main_commit": component.get("main_commit"),
            "artifact_name": component.get("artifact_name"),
            "artifact_sha256": component.get("sha256"),
        }
        row = _exact(
            sources[component_name],
            PROVENANCE_SOURCE_KEYS,
            f"A/A snapshot provenance {component_name}",
        )
        _require(
            row == expected,
            f"A/A snapshot provenance source mismatch: {component_name}",
        )
        _require(
            RUN_ID_RE.fullmatch(str(row["workflow_run_id"] or "")) is not None
            and COMMIT_RE.fullmatch(str(row["main_commit"] or "")) is not None
            and SHA256_RE.fullmatch(str(row["artifact_sha256"] or "")) is not None,
            f"A/A snapshot provenance source identity invalid: {component_name}",
        )
    _require(
        _exact(
            root["safety"], set(PROVENANCE_SAFETY), "A/A snapshot provenance safety"
        )
        == PROVENANCE_SAFETY,
        "A/A snapshot provenance safety drift",
    )
    _require(
        SHA256_RE.fullmatch(provenance_sha256) is not None
        and hashlib.sha256(canonical_json_bytes(provenance)).hexdigest()
        == provenance_sha256,
        "A/A snapshot provenance SHA-256 mismatch",
    )


def _validate_snapshot_manifest(
    snapshot_manifest: Mapping[str, Any], activation: Mapping[str, Any]
) -> None:
    try:
        validate_measurement_window(
            snapshot_manifest,
            activation,
            _load(ACCEPTANCE_PATH, "A/A acceptance"),
            _load(RECONCILIATION_EVIDENCE_PATH, "reconciliation evidence"),
        )
    except MeasurementWindowError as exc:
        raise AaCompletionRecordingError(f"A/A snapshot manifest is invalid: {exc}") from exc


def validate_observation(
    observation: Mapping[str, Any], completion: Mapping[str, Any]
) -> None:
    row = _exact(observation, OBSERVATION_KEYS, "A/A stop observation")
    _require(row["schema_version"] == 1, "A/A stop observation schema drift")
    _require(
        row["observation_type"] == "vevo_growthbook_production_aa_stop_readback",
        "A/A stop observation type drift",
    )
    _require(row["experiment_id"] == "vevo-sk-aa-001", "A/A stop experiment drift")
    observed_at = _parse_utc(row["observed_at_utc"], "observed_at_utc")
    aa_pass = completion["aa_pass"]
    _require(
        row["aa_pass_snapshot_sha256"] == aa_pass["snapshot_sha256"],
        "A/A stop observation snapshot binding drift",
    )
    _require(
        row["aa_pass_decision_sha256"] == aa_pass["decision_sha256"],
        "A/A stop observation decision binding drift",
    )
    _require(
        row["aa_pass_provenance_sha256"] == aa_pass["provenance_sha256"],
        "A/A stop observation provenance binding drift",
    )
    _require(
        observed_at >= _parse_utc(aa_pass["evaluated_at_utc"], "aa_pass.evaluated_at_utc"),
        "A/A stop was observed before the PASS decision",
    )

    growthbook = _exact(
        row["growthbook"], GROWTHBOOK_OBSERVATION_KEYS, "growthbook readback"
    )
    revision = growthbook["aa_feature_live_revision"]
    _require(isinstance(growthbook["build"], str) and growthbook["build"], "GrowthBook build is missing")
    _require(growthbook["project_id"] == "prj_2CeEJc6J9FwQFix9UhsnKr", "GrowthBook project drift")
    _require(growthbook["environment"] == "production", "GrowthBook environment drift")
    _require(growthbook["aa_experiment_id"] == "exp_19g6mmt5wugpk", "A/A GrowthBook ID drift")
    _require(growthbook["aa_experiment_status"] == "stopped", "A/A experiment is not stopped")
    _require(growthbook["aa_production_live_rule_count"] == 0, "A/A Production rule remains live")
    _require(growthbook["aa_production_allocation_percent"] == 0, "A/A Production allocation is nonzero")
    _require(type(revision) is int and revision >= 4, "A/A feature revision was not advanced")
    _require(growthbook["aa_feature_production_enabled"] is False, "A/A feature remains enabled in Production")
    _require(growthbook["aa_feature_staging_enabled"] is True, "A/A staging rule was unexpectedly removed")
    _require(
        growthbook["aa_feature_live_rule_count_by_environment"]
        == {"production": 0, "staging": 1},
        "A/A feature environment rule counts drift",
    )
    _require(growthbook["cta_experiment_id"] == "exp_19g6mmt1qxzrp", "CTA GrowthBook ID drift")
    _require(growthbook["cta_experiment_status"] == "draft_not_started", "CTA experiment is not a draft")
    _require(growthbook["cta_production_live_rule_count"] == 0, "CTA Production rule exists")
    _require(growthbook["cta_production_allocation_percent"] == 0, "CTA Production allocation is nonzero")

    gtm = _exact(row["gtm"], GTM_OBSERVATION_KEYS, "GTM readback")
    _require(gtm["account_id"] == "6254499282", "GTM account drift")
    _require(gtm["container_id"] == "198135331", "GTM container drift")
    _require(gtm["public_container_id"] == "GTM-5ZB5LFGB", "public GTM container drift")
    _require(gtm["container_version_id"] == "15", "GTM live version changed")
    _require(gtm["growthbook_loader_active"] is True, "GrowthBook loader was removed")
    _require(
        gtm["unprocessed_changes"] == {"added": 0, "modified": 0, "removed": 0},
        "GTM has unprocessed changes",
    )

    storefront = _exact(
        row["storefront"], STOREFRONT_OBSERVATION_KEYS, "storefront readback"
    )
    _require(
        storefront["product_path"]
        == "/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute",
        "storefront product path drift",
    )
    for field in ("desktop_verified", "mobile_verified", "add_to_cart_text_unchanged"):
        _require(storefront[field] is True, f"storefront {field} must be true")
    for field in (
        "aa_assignment_present",
        "cta_class_applied",
        "price_mutated",
        "cart_mutated",
        "checkout_or_order_mutated",
    ):
        _require(storefront[field] is False, f"storefront {field} must be false")
    _require(storefront["console_error_count"] == 0, "storefront console errors observed")

    mutations = _exact(
        row["mutation_boundaries"],
        MUTATION_OBSERVATION_KEYS,
        "stop mutation boundaries",
    )
    _require(mutations["growthbook_manual_mutation_performed"] is True, "manual GrowthBook stop was not recorded")
    _require(
        mutations["growthbook_manual_mutation_scope"]
        == "stop_exact_aa_experiment_and_remove_only_its_production_live_rule",
        "manual GrowthBook mutation scope drift",
    )
    for field in MUTATION_OBSERVATION_KEYS - {
        "growthbook_manual_mutation_performed",
        "growthbook_manual_mutation_scope",
    }:
        _require(mutations[field] is False, f"unsafe mutation observed: {field}")


def validate_manifest(
    completion: Mapping[str, Any],
    activation: Mapping[str, Any],
    snapshot_manifest: Mapping[str, Any],
    *,
    observation: Mapping[str, Any] | None = None,
) -> None:
    root = _exact(completion, ROOT_KEYS, "A/A completion manifest")
    _require(root["schema_version"] == 1, "A/A completion schema drift")
    _require(
        root["completion_type"] == "vevo_growthbook_production_aa_completion",
        "A/A completion type drift",
    )
    _require(root["experiment_id"] == "vevo-sk-aa-001", "A/A completion experiment drift")
    _require(
        root["source_activation_manifest"]
        == "projects/vevo/growthbook_production_aa_activation.json",
        "A/A source activation path drift",
    )
    _require(
        root["source_snapshot_manifest"] == "projects/vevo/growthbook_aa_snapshot.json",
        "A/A source snapshot path drift",
    )
    _require(
        root["source_activation_sha256"] == _activation_digest(activation),
        "A/A source activation SHA-256 drift",
    )
    _validate_snapshot_manifest(snapshot_manifest, activation)
    _require(root["next_state"] == NEXT_STATE, "A/A completion next-state drift")

    aa_pass = _exact(root["aa_pass"], AA_PASS_KEYS, "A/A PASS binding")
    fixed_pass = {
        "snapshot_workflow": ".github/workflows/build-vevo-growthbook-production-aa-snapshot.yml",
        "artifact_name": "vevo-growthbook-aa-snapshot",
        "snapshot_file": "vevo-growthbook-aa-snapshot.json",
        "decision_file": "vevo-growthbook-aa-decision.json",
        "provenance_file": "vevo-growthbook-aa-provenance.json",
        "winner_calls_allowed": False,
    }
    _require(
        all(aa_pass.get(key) == value for key, value in fixed_pass.items()),
        "A/A PASS artifact contract drift",
    )
    stop = _exact(root["stop_readback"], STOP_READBACK_KEYS, "A/A stop readback")
    _require(
        stop["observation_file"]
        == "projects/vevo/growthbook_aa_completion_observation.json",
        "A/A stop observation path drift",
    )
    boundaries = _exact(root["release_boundaries"], BOUNDARY_KEYS, "release boundaries")
    status = root["status"]
    _require(
        status
        in {
            "waiting_for_verified_aa_pass",
            "aa_pass_recorded_manual_stop_review_allowed",
            "production_aa_stopped_verified_cta_activation_blocked",
        },
        "A/A completion status drift",
    )
    expected_manual_stop = status == "aa_pass_recorded_manual_stop_review_allowed"
    _require(
        boundaries["manual_growthbook_stop_allowed"] is expected_manual_stop,
        "manual GrowthBook stop gate drift",
    )
    for field in BOUNDARY_KEYS - {"manual_growthbook_stop_allowed"}:
        _require(boundaries[field] is False, f"release boundary opened: {field}")

    if status == "waiting_for_verified_aa_pass":
        _require(aa_pass["status"] == "not_recorded", "A/A PASS was partially recorded")
        _require(
            all(
                aa_pass[field] is None
                for field in (
                    "workflow_run_id",
                    "main_commit",
                    "snapshot_sha256",
                    "decision_sha256",
                    "provenance_sha256",
                    "evaluated_at_utc",
                    "verdict",
                )
            ),
            "pending A/A PASS binding contains values",
        )
        _require(stop["status"] == "not_recorded", "A/A stop was recorded before PASS")
        _require(
            all(
                stop[field] is None
                for field in (
                    "observation_sha256",
                    "observed_at_utc",
                    "growthbook_build",
                    "feature_revision",
                    "gtm_container_version_id",
                )
            ),
            "pending A/A stop readback contains values",
        )
        _require(observation is None, "pending A/A completion has an observation")
        _require(
            root["next_gate"]
            == "record_exact_aa_pass_before_reviewed_manual_growthbook_stop",
            "pending A/A completion next gate drift",
        )
        return

    _require(aa_pass["status"] == "verified_pass", "A/A PASS binding is incomplete")
    _require(RUN_ID_RE.fullmatch(str(aa_pass["workflow_run_id"] or "")) is not None, "A/A PASS run ID is invalid")
    _require(COMMIT_RE.fullmatch(str(aa_pass["main_commit"] or "")) is not None, "A/A PASS main commit is invalid")
    for field in ("snapshot_sha256", "decision_sha256", "provenance_sha256"):
        _require(SHA256_RE.fullmatch(str(aa_pass[field] or "")) is not None, f"A/A PASS {field} is invalid")
    _parse_utc(aa_pass["evaluated_at_utc"], "aa_pass.evaluated_at_utc")
    _require(aa_pass["verdict"] == "PASS", "A/A completion is not bound to PASS")
    window = snapshot_manifest.get("measurement_window") or {}
    _require(
        snapshot_manifest.get("snapshot_build_allowed") is True
        and window.get("resolution_status") == "resolved"
        and window.get("resolved_through_utc") == aa_pass["evaluated_at_utc"],
        "A/A PASS differs from the resolved snapshot manifest",
    )

    if status == "aa_pass_recorded_manual_stop_review_allowed":
        _require(stop["status"] == "not_recorded", "A/A stop readback was partially recorded")
        _require(observation is None, "pre-stop A/A completion has an observation")
        _require(
            root["next_gate"]
            == "manually_stop_exact_aa_and_remove_only_its_production_live_rule_then_record_readback",
            "A/A manual-stop next gate drift",
        )
        return

    _require(stop["status"] == "verified_zero_allocation", "A/A stop readback is incomplete")
    _require(SHA256_RE.fullmatch(str(stop["observation_sha256"] or "")) is not None, "A/A stop observation SHA-256 is invalid")
    _parse_utc(stop["observed_at_utc"], "stop_readback.observed_at_utc")
    _require(isinstance(stop["growthbook_build"], str) and stop["growthbook_build"], "A/A stop GrowthBook build is missing")
    _require(type(stop["feature_revision"]) is int and stop["feature_revision"] >= 4, "A/A stop feature revision is invalid")
    _require(stop["gtm_container_version_id"] == "15", "A/A stop GTM version drift")
    _require(observation is not None, "completed A/A stop observation is missing")
    validate_observation(observation, root)
    _require(
        hashlib.sha256(canonical_json_bytes(observation)).hexdigest()
        == stop["observation_sha256"],
        "A/A stop observation SHA-256 drift",
    )
    _require(stop["observed_at_utc"] == observation["observed_at_utc"], "A/A stop observed timestamp drift")
    _require(stop["growthbook_build"] == observation["growthbook"]["build"], "A/A stop GrowthBook build binding drift")
    _require(stop["feature_revision"] == observation["growthbook"]["aa_feature_live_revision"], "A/A stop feature revision binding drift")
    _require(stop["gtm_container_version_id"] == observation["gtm"]["container_version_id"], "A/A stop GTM version binding drift")
    _require(
        root["next_gate"]
        == "record_hash_bound_product_page_baseline_and_freeze_cta_sample_activation_still_blocked",
        "completed A/A next gate drift",
    )


def record_pass(
    completion: Mapping[str, Any],
    activation: Mapping[str, Any],
    snapshot_manifest: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    decision: Mapping[str, Any],
    provenance: Mapping[str, Any],
    *,
    workflow_run_id: str,
    main_commit: str,
    snapshot_sha256: str,
    decision_sha256: str,
    provenance_sha256: str,
) -> dict[str, Any]:
    validate_manifest(completion, activation, snapshot_manifest)
    _require(
        completion["status"]
        in {"waiting_for_verified_aa_pass", "aa_pass_recorded_manual_stop_review_allowed"},
        "A/A PASS cannot be recorded in the current completion state",
    )
    _require(RUN_ID_RE.fullmatch(str(workflow_run_id or "")) is not None, "snapshot workflow run ID is invalid")
    _require(COMMIT_RE.fullmatch(str(main_commit or "")) is not None, "snapshot workflow main commit is invalid")
    _require(SHA256_RE.fullmatch(str(snapshot_sha256 or "")) is not None, "snapshot SHA-256 is invalid")
    _require(SHA256_RE.fullmatch(str(decision_sha256 or "")) is not None, "decision SHA-256 is invalid")
    _require(
        SHA256_RE.fullmatch(str(provenance_sha256 or "")) is not None,
        "provenance SHA-256 is invalid",
    )
    _require(hashlib.sha256(canonical_json_bytes(snapshot)).hexdigest() == snapshot_sha256, "snapshot SHA-256 mismatch")
    _require(hashlib.sha256(canonical_json_bytes(decision)).hexdigest() == decision_sha256, "decision SHA-256 mismatch")
    validate_provenance(
        provenance,
        snapshot_manifest,
        provenance_sha256=provenance_sha256,
        snapshot_sha256=snapshot_sha256,
        decision_sha256=decision_sha256,
        workflow_run_id=workflow_run_id,
        main_commit=main_commit,
    )
    try:
        expected_decision = evaluate_aa(snapshot, load_aa_config(AA_CONFIG_PATH))
    except AaEvaluationError as exc:
        raise AaCompletionRecordingError(f"A/A snapshot is invalid: {exc}") from exc
    _require(decision == expected_decision, "A/A decision differs from independent evaluation")
    _require(decision.get("verdict") == "PASS", "A/A completion requires PASS")
    _require(decision.get("winner_calls_allowed") is False, "A/A decision attempted a winner call")
    window = snapshot_manifest["measurement_window"]
    _require(snapshot_manifest.get("snapshot_build_allowed") is True, "A/A snapshot build gate is closed")
    _require(window.get("resolution_status") == "resolved", "A/A measurement window is unresolved")
    _require(snapshot.get("full_allocation_started_at_utc") == window.get("from_utc"), "A/A snapshot start differs from the frozen window")
    _require(decision.get("evaluated_at_utc") == window.get("resolved_through_utc"), "A/A decision end differs from the frozen window")

    if completion["status"] == "aa_pass_recorded_manual_stop_review_allowed":
        existing = completion["aa_pass"]
        _require(
            existing["workflow_run_id"] == workflow_run_id
            and existing["main_commit"] == main_commit
            and existing["snapshot_sha256"] == snapshot_sha256
            and existing["decision_sha256"] == decision_sha256
            and existing["provenance_sha256"] == provenance_sha256,
            "A/A PASS is already bound to a different artifact",
        )
        return copy.deepcopy(dict(completion))

    recorded = copy.deepcopy(completion)
    recorded["status"] = "aa_pass_recorded_manual_stop_review_allowed"
    recorded["aa_pass"].update(
        {
            "status": "verified_pass",
            "workflow_run_id": workflow_run_id,
            "main_commit": main_commit,
            "snapshot_sha256": snapshot_sha256,
            "decision_sha256": decision_sha256,
            "provenance_sha256": provenance_sha256,
            "evaluated_at_utc": decision["evaluated_at_utc"],
            "verdict": "PASS",
        }
    )
    recorded["release_boundaries"]["manual_growthbook_stop_allowed"] = True
    recorded["next_gate"] = (
        "manually_stop_exact_aa_and_remove_only_its_production_live_rule_then_record_readback"
    )
    validate_manifest(recorded, activation, snapshot_manifest)
    return recorded


def _post_aa_workspace(
    workspace: Mapping[str, Any], *, feature_revision: int
) -> dict[str, Any]:
    updated = copy.deepcopy(workspace)
    post_state = NEXT_STATE["workspace_state"]
    if updated.get("state") == post_state:
        return updated
    _require(
        updated.get("state")
        == "production_aa_running_activation_verified_pro_quantiles_blocked",
        "GrowthBook workspace is not in the running Production A/A state",
    )
    _require(updated.get("workspace", {}).get("production_allocation_percent") == 100, "workspace A/A allocation drift")
    _require(updated.get("decision_gates", {}).get("production_activation_allowed") is True, "workspace A/A gate is closed unexpectedly")
    experiments = {
        row.get("tracking_key"): row
        for row in updated.get("experiments", [])
        if isinstance(row, dict)
    }
    _require(set(experiments) == {"vevo-sk-aa-001", "vevo-sk-product-cta-color-001"}, "workspace experiment set drift")
    aa = experiments["vevo-sk-aa-001"]
    cta = experiments["vevo-sk-product-cta-color-001"]
    _require(aa.get("status") == "running_production_aa_only", "workspace A/A is not running")
    _require(aa.get("feature_rule_revision") == 3, "workspace A/A feature revision drift")
    _require(aa.get("production_allocation_percent") == 100, "workspace A/A allocation is not 100")
    _require(cta.get("status") == "unstarted_draft", "workspace CTA state drift")
    _require(cta.get("production_allocation_percent") == 0, "workspace CTA allocation is nonzero")

    updated["state"] = post_state
    updated["workspace"]["production_allocation_percent"] = 0
    updated["decision_gates"]["production_activation_allowed"] = False
    aa.update(
        {
            "status": "stopped_production_aa_pass_verified",
            "feature_rule_status": "staging_only",
            "feature_rule_revision": feature_revision,
            "feature_rule_environments": ["staging"],
            "production_allocation_percent": 0,
        }
    )
    cta.update(
        {
            "status": "draft",
            "feature_rule_status": "draft",
            "feature_rule_environments": ["staging"],
            "production_allocation_percent": 0,
        }
    )
    return updated


def record_stop(
    completion: Mapping[str, Any],
    activation: Mapping[str, Any],
    snapshot_manifest: Mapping[str, Any],
    workspace: Mapping[str, Any],
    observation: Mapping[str, Any],
    *,
    observation_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    existing_observation = observation if completion.get("status") == "production_aa_stopped_verified_cta_activation_blocked" else None
    validate_manifest(
        completion,
        activation,
        snapshot_manifest,
        observation=existing_observation,
    )
    _require(
        completion["status"]
        in {
            "aa_pass_recorded_manual_stop_review_allowed",
            "production_aa_stopped_verified_cta_activation_blocked",
        },
        "A/A stop cannot be recorded before PASS",
    )
    digest = str(observation_sha256 or "").strip()
    _require(SHA256_RE.fullmatch(digest) is not None, "A/A stop observation SHA-256 is invalid")
    _require(hashlib.sha256(canonical_json_bytes(observation)).hexdigest() == digest, "A/A stop observation SHA-256 mismatch")
    validate_observation(observation, completion)
    updated_workspace = _post_aa_workspace(
        workspace,
        feature_revision=observation["growthbook"]["aa_feature_live_revision"],
    )
    recorded = copy.deepcopy(completion)
    recorded["status"] = "production_aa_stopped_verified_cta_activation_blocked"
    recorded["stop_readback"].update(
        {
            "status": "verified_zero_allocation",
            "observation_sha256": digest,
            "observed_at_utc": observation["observed_at_utc"],
            "growthbook_build": observation["growthbook"]["build"],
            "feature_revision": observation["growthbook"]["aa_feature_live_revision"],
            "gtm_container_version_id": observation["gtm"]["container_version_id"],
        }
    )
    recorded["release_boundaries"]["manual_growthbook_stop_allowed"] = False
    recorded["next_gate"] = (
        "record_hash_bound_product_page_baseline_and_freeze_cta_sample_activation_still_blocked"
    )
    validate_manifest(
        recorded,
        activation,
        snapshot_manifest,
        observation=observation,
    )
    return recorded, updated_workspace


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--completion", type=Path, default=DEFAULT_COMPLETION_PATH)
    parser.add_argument("--activation", type=Path, default=ACTIVATION_PATH)
    parser.add_argument("--snapshot-manifest", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--output", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    pass_parser = subparsers.add_parser("record-pass")
    pass_parser.add_argument("--snapshot", type=Path, required=True)
    pass_parser.add_argument("--decision", type=Path, required=True)
    pass_parser.add_argument("--provenance", type=Path, required=True)
    pass_parser.add_argument("--snapshot-sha256", required=True)
    pass_parser.add_argument("--decision-sha256", required=True)
    pass_parser.add_argument("--provenance-sha256", required=True)
    pass_parser.add_argument("--workflow-run-id", required=True)
    pass_parser.add_argument("--main-commit", required=True)

    stop_parser = subparsers.add_parser("record-stop")
    stop_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)
    stop_parser.add_argument("--observation", type=Path, default=DEFAULT_OBSERVATION_PATH)
    stop_parser.add_argument("--observation-sha256", required=True)
    stop_parser.add_argument("--workspace-output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        completion = _load(args.completion, "A/A completion manifest")
        activation = _load(args.activation, "A/A activation manifest")
        snapshot_manifest = _load(args.snapshot_manifest, "A/A snapshot manifest")
        if args.command == "record-pass":
            snapshot = load_canonical(args.snapshot, args.snapshot_sha256, "A/A snapshot")
            decision = load_canonical(args.decision, args.decision_sha256, "A/A decision")
            provenance = load_canonical(
                args.provenance, args.provenance_sha256, "A/A snapshot provenance"
            )
            recorded = record_pass(
                completion,
                activation,
                snapshot_manifest,
                snapshot,
                decision,
                provenance,
                workflow_run_id=args.workflow_run_id,
                main_commit=args.main_commit,
                snapshot_sha256=args.snapshot_sha256,
                decision_sha256=args.decision_sha256,
                provenance_sha256=args.provenance_sha256,
            )
            _write_json(args.output, recorded)
            print("VEVO_AA_PASS_RECORDED:manual_stop_review_allowed=true:automatic_mutation=false:cta=false")
            return 0

        observation = load_canonical(
            args.observation, args.observation_sha256, "A/A stop observation"
        )
        workspace = _load(args.workspace, "GrowthBook workspace")
        _require(args.output.resolve() != args.workspace_output.resolve(), "output paths must differ")
        recorded, updated_workspace = record_stop(
            completion,
            activation,
            snapshot_manifest,
            workspace,
            observation,
            observation_sha256=args.observation_sha256,
        )
        _write_json(args.output, recorded)
        _write_json(args.workspace_output, updated_workspace)
        print("VEVO_AA_STOP_RECORDED:production_allocation=0:cta_activation=false")
        return 0
    except (AaCompletionRecordingError, OSError) as exc:
        print(f"record_growthbook_aa_completion.py: FAIL: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
