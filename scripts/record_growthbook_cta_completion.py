#!/usr/bin/env python3
"""Record the reviewed VEVO CTA assignment stop and frozen follow-up offline.

The command accepts only a canonical, independently hashed post-stop readback.
It has no browser, network, AWS, GrowthBook, GTM, Meta Ads, BiznisWeb,
collector, reporting, price, cart, checkout, payment, stock, or order client.
All output documents are fully built and validated before any output is written.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scripts import build_growthbook_cta_final_snapshot as final_snapshot_builder
    from scripts.evaluate_growthbook_cta import (
        validate_contract,
        validate_lifecycle_manifest,
    )
    from scripts.freeze_growthbook_cta_sample import validate_plan
    from scripts.record_growthbook_cta_activation import (
        RUNNING as CTA_RUNNING,
        STOPPED as CTA_STOPPED,
        validate_manifest as validate_activation_manifest,
    )
    from scripts.validate_growthbook_cta_measurement_window import (
        RESOLVED as WINDOW_RESOLVED,
        STOPPED as WINDOW_STOPPED,
        canonical_evidence_bytes,
        validate_manifest as validate_measurement_manifest,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import build_growthbook_cta_final_snapshot as final_snapshot_builder

    from evaluate_growthbook_cta import validate_contract, validate_lifecycle_manifest
    from freeze_growthbook_cta_sample import validate_plan
    from record_growthbook_cta_activation import (
        RUNNING as CTA_RUNNING,
        STOPPED as CTA_STOPPED,
        validate_manifest as validate_activation_manifest,
    )
    from validate_growthbook_cta_measurement_window import (
        RESOLVED as WINDOW_RESOLVED,
        STOPPED as WINDOW_STOPPED,
        canonical_evidence_bytes,
        validate_manifest as validate_measurement_manifest,
    )


ROOT = Path(__file__).resolve().parents[1]
VEVO = ROOT / "projects" / "vevo"
COMPLETION_PATH = VEVO / "growthbook_cta_completion.json"
ACTIVATION_PATH = VEVO / "growthbook_cta_activation.json"
START_OBSERVATION_PATH = VEVO / "growthbook_cta_activation_observation.json"
MEASUREMENT_PATH = VEVO / "growthbook_cta_measurement_window.json"
STOP_OBSERVATION_PATH = VEVO / "growthbook_cta_assignment_stop_observation.json"
SAMPLE_PLAN_PATH = VEVO / "growthbook_cta_sample_plan.json"
DECISION_CONTRACT_PATH = VEVO / "growthbook_cta_decision_contract.json"
LIFECYCLE_PATH = VEVO / "growthbook_cta_lifecycle_reconciliation.json"
RECONCILIATION_PATH = VEVO / "growthbook_production_reconciliation_deploy_evidence.json"
WORKSPACE_PATH = VEVO / "growthbook_workspace.json"
FINAL_SNAPSHOT_PATH = VEVO / "growthbook_cta_final_snapshot.json"

WAITING = "waiting_for_assignment_stop_review"
FOLLOWUP = "cta_assignment_stopped_verified_followup_pending"
EXPERIMENT_ID = "vevo-sk-product-cta-color-001"
FEATURE_KEY = "vevo-sk-product-cta-color"
EXPECTED_DECISION_SHA256 = (
    "ced267f0152a97e8a25c3cf70e23cbdcebec2ecd6761f05134bf2c9507518183"
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")

ROOT_KEYS = {
    "schema_version",
    "completion_type",
    "experiment_id",
    "status",
    "source_bindings",
    "stop_readback",
    "followup",
    "release_boundaries",
    "next_gate",
}
BINDING_KEYS = {
    "activation_path",
    "activation_sha256",
    "measurement_window_path",
    "measurement_window_sha256",
    "sample_plan_path",
    "sample_plan_sha256",
    "decision_contract_path",
    "decision_contract_sha256",
    "lifecycle_reconciliation_path",
    "lifecycle_reconciliation_sha256",
}
STOP_KEYS = {
    "status",
    "observation_path",
    "observation_sha256",
    "observed_at_utc",
    "assignment_ended_at_utc",
    "growthbook_build",
    "feature_revision",
    "gtm_container_version_id",
}
FOLLOWUP_KEYS = {
    "timezone",
    "required_days_after_assignment_stop",
    "assignment_ended_at_utc",
    "final_snapshot_due_utc",
    "status",
    "protected_final_snapshot_workflow_allowed",
    "one_final_look_only",
}
BOUNDARY_KEYS = {
    "manual_growthbook_stop_allowed",
    "automatic_growthbook_mutation_allowed",
    "automatic_gtm_mutation_allowed",
    "automatic_meta_ads_mutation_allowed",
    "automatic_biznisweb_mutation_allowed",
    "automatic_collector_or_reporting_mutation_allowed",
    "price_product_cart_checkout_order_mutation_allowed",
    "outcome_metrics_read_outside_protected_final_snapshot_allowed",
    "winner_calls_outside_offline_final_evaluator_allowed",
}
OBSERVATION_KEYS = {
    "schema_version",
    "evidence_type",
    "experiment_id",
    "feature_key",
    "observed_at_utc",
    "assignment_ended_at_utc",
    "window_checkpoint_evidence_sha256",
    "activation_start_observation_sha256",
    "growthbook",
    "gtm",
    "storefront",
    "collector",
    "mutation_boundaries",
    "safety",
}
GROWTHBOOK_KEYS = {
    "build",
    "project_id",
    "environment",
    "experiment_id",
    "experiment_status",
    "production_live_rule_count",
    "production_allocation_percent",
    "feature_revision",
    "feature_revision_status",
    "feature_production_enabled",
    "feature_staging_enabled",
    "feature_live_rule_count_by_environment",
    "active_production_experiments",
    "aa_status",
}
GTM_KEYS = {
    "account_id",
    "container_id",
    "public_container_id",
    "container_version_id",
    "growthbook_loader_active",
    "unprocessed_changes",
}
STOREFRONT_KEYS = {
    "product_path",
    "desktop_verified",
    "mobile_verified",
    "cta_assignment_present",
    "brand_contrast_class_applied",
    "add_to_cart_text_unchanged",
    "console_error_count",
    "price_mutated",
    "cart_mutated",
    "checkout_or_order_mutated",
}
COLLECTOR_KEYS = {
    "post_stop_observation_window_seconds",
    "post_stop_cta_exposure_count",
    "post_stop_assignment_count",
    "stop_boundary_verified",
}
MUTATION_KEYS = {
    "growthbook_manual_mutation_performed",
    "growthbook_manual_mutation_scope",
    "automatic_growthbook_mutation_performed",
    "gtm_mutation_performed",
    "meta_ads_mutation_performed",
    "biznisweb_mutation_performed",
    "collector_or_reporting_mutation_performed",
    "price_product_cart_checkout_order_mutation_performed",
}
SAFETY_KEYS = {
    "contains_credentials",
    "contains_event_or_device_ids",
    "contains_customer_or_order_data",
    "winner_called",
}


class CtaCompletionRecordingError(ValueError):
    """Raised when the CTA stop/follow-up transition fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaCompletionRecordingError(message)


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(UTC_RE.fullmatch(text) is not None, f"{field} must use whole-second UTC Z")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CtaCompletionRecordingError(f"{field} is invalid") from exc
    _require(parsed.tzinfo == UTC and parsed.microsecond == 0, f"{field} is invalid")
    return parsed


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return canonical_evidence_bytes(value)


def pretty_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _hash_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _hash_path(path: Path) -> str:
    return _hash_bytes(path.read_bytes())


def _load(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CtaCompletionRecordingError(f"{field} is unreadable") from exc
    _require(isinstance(value, dict), f"{field} must contain an object")
    return value


def load_canonical(path: Path, expected_sha256: str, field: str) -> dict[str, Any]:
    digest = str(expected_sha256 or "").strip()
    _require(SHA256_RE.fullmatch(digest) is not None, f"{field} SHA-256 is invalid")
    raw = path.read_bytes()
    value = json.loads(raw.decode("utf-8"))
    _require(isinstance(value, dict), f"{field} must contain an object")
    _require(raw == canonical_json_bytes(value), f"{field} is not canonical JSON")
    _require(_hash_bytes(raw) == digest, f"{field} SHA-256 mismatch")
    return value


def _source_hashes(
    *,
    activation_bytes: bytes | None = None,
    measurement_bytes: bytes | None = None,
    activation_path: Path = ACTIVATION_PATH,
    measurement_path: Path = MEASUREMENT_PATH,
    sample_plan_path: Path = SAMPLE_PLAN_PATH,
    decision_contract_path: Path = DECISION_CONTRACT_PATH,
    lifecycle_path: Path = LIFECYCLE_PATH,
    start_observation_path: Path = START_OBSERVATION_PATH,
    reconciliation_path: Path = RECONCILIATION_PATH,
) -> dict[str, str]:
    return {
        "activation": _hash_bytes(activation_bytes) if activation_bytes is not None else _hash_path(activation_path),
        "measurement_window": _hash_bytes(measurement_bytes) if measurement_bytes is not None else _hash_path(measurement_path),
        "sample_plan": _hash_path(sample_plan_path),
        "decision_contract": _hash_path(decision_contract_path),
        "lifecycle_reconciliation": _hash_path(lifecycle_path),
        "start_observation": (
            _hash_path(start_observation_path)
            if start_observation_path.exists()
            else ""
        ),
        "reconciliation_evidence": _hash_path(reconciliation_path),
    }


def validate_stop_observation(
    observation: Mapping[str, Any],
    activation: Mapping[str, Any],
    measurement: Mapping[str, Any],
) -> None:
    root = _exact(observation, OBSERVATION_KEYS, "CTA stop observation")
    _require(root["schema_version"] == 1, "CTA stop observation schema drift")
    _require(root["evidence_type"] == "vevo_growthbook_cta_assignment_stop_readback", "CTA stop evidence type drift")
    _require(root["experiment_id"] == EXPERIMENT_ID, "CTA stop experiment drift")
    _require(root["feature_key"] == FEATURE_KEY, "CTA stop feature drift")
    observed = _parse_utc(root["observed_at_utc"], "observed_at_utc")
    ended = _parse_utc(root["assignment_ended_at_utc"], "assignment_ended_at_utc")
    _require(ended <= observed, "CTA stop observation predates assignment end")
    window = measurement["measurement_window"]
    _require(
        _parse_utc(window["resolved_at_utc"], "measurement.resolved_at_utc") <= ended,
        "CTA assignment ended before the outcome-blind stopping rule resolved",
    )
    history = window["checkpoint_history"]
    _require(bool(history), "CTA stop observation has no resolved checkpoint")
    _require(
        root["window_checkpoint_evidence_sha256"] == history[-1]["evidence_sha256"],
        "CTA stop checkpoint binding drift",
    )
    _require(
        root["activation_start_observation_sha256"]
        == activation["start_readback"]["observation_sha256"],
        "CTA stop/start binding drift",
    )

    growthbook = _exact(root["growthbook"], GROWTHBOOK_KEYS, "GrowthBook stop readback")
    _require(isinstance(growthbook["build"], str) and growthbook["build"], "GrowthBook build is missing")
    _require(growthbook["project_id"] == "prj_2CeEJc6J9FwQFix9UhsnKr", "GrowthBook project drift")
    _require(growthbook["environment"] == "production", "GrowthBook stop environment drift")
    _require(growthbook["experiment_id"] == "exp_19g6mmt1qxzrp", "GrowthBook CTA ID drift")
    _require(growthbook["experiment_status"] == "stopped", "CTA experiment is not stopped")
    _require(growthbook["production_live_rule_count"] == 0, "CTA Production rule remains live")
    _require(growthbook["production_allocation_percent"] == 0, "CTA Production allocation is nonzero")
    revision = growthbook["feature_revision"]
    _require(type(revision) is int and revision > activation["start_readback"]["feature_revision"], "CTA feature revision was not advanced")
    _require(growthbook["feature_revision_status"] == "live", "CTA stop feature revision is not live")
    _require(growthbook["feature_production_enabled"] is False, "CTA feature remains enabled in Production")
    _require(growthbook["feature_staging_enabled"] is True, "CTA staging rule was not preserved")
    _require(
        growthbook["feature_live_rule_count_by_environment"]
        == {"production": 0, "staging": 1},
        "CTA feature environment rule counts drift",
    )
    _require(growthbook["active_production_experiments"] == [], "another Production experiment is active")
    _require(growthbook["aa_status"] == "stopped_zero_allocation", "A/A stop state drift")

    gtm = _exact(root["gtm"], GTM_KEYS, "GTM stop readback")
    _require(
        gtm
        == {
            "account_id": "6254499282",
            "container_id": "198135331",
            "public_container_id": "GTM-5ZB5LFGB",
            "container_version_id": "15",
            "growthbook_loader_active": True,
            "unprocessed_changes": {"added": 0, "modified": 0, "removed": 0},
        },
        "CTA stop GTM readback drift",
    )
    storefront = _exact(root["storefront"], STOREFRONT_KEYS, "CTA stop storefront readback")
    _require(
        storefront["product_path"]
        == "/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute",
        "CTA stop storefront path drift",
    )
    for field in ("desktop_verified", "mobile_verified", "add_to_cart_text_unchanged"):
        _require(storefront[field] is True, f"CTA stop storefront {field} must be true")
    for field in (
        "cta_assignment_present",
        "brand_contrast_class_applied",
        "price_mutated",
        "cart_mutated",
        "checkout_or_order_mutated",
    ):
        _require(storefront[field] is False, f"CTA stop storefront {field} must be false")
    _require(storefront["console_error_count"] == 0, "CTA stop storefront console errors observed")

    collector = _exact(root["collector"], COLLECTOR_KEYS, "CTA stop collector boundary")
    _require(collector["post_stop_observation_window_seconds"] >= 300, "CTA post-stop observation window is too short")
    _require(collector["post_stop_cta_exposure_count"] == 0, "CTA exposures continued after stop")
    _require(collector["post_stop_assignment_count"] == 0, "CTA assignments continued after stop")
    _require(collector["stop_boundary_verified"] is True, "CTA collector stop boundary is unverified")

    mutations = _exact(root["mutation_boundaries"], MUTATION_KEYS, "CTA stop mutation boundaries")
    _require(mutations["growthbook_manual_mutation_performed"] is True, "manual CTA stop was not recorded")
    _require(
        mutations["growthbook_manual_mutation_scope"]
        == "stop_exact_cta_experiment_remove_only_production_rule_preserve_staging",
        "manual CTA stop scope drift",
    )
    for field in MUTATION_KEYS - {
        "growthbook_manual_mutation_performed",
        "growthbook_manual_mutation_scope",
    }:
        _require(mutations[field] is False, f"unsafe CTA stop mutation observed: {field}")
    safety = _exact(root["safety"], SAFETY_KEYS, "CTA stop safety")
    _require(not any(safety.values()), "CTA stop evidence contains unsafe data or winner call")


def _validate_stopped_workspace(
    workspace: Mapping[str, Any],
    activation: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> None:
    _require(
        workspace.get("state")
        == "production_cta_stopped_followup_pending_pro_quantiles_blocked",
        "CTA stopped workspace state drift",
    )
    _require(workspace.get("workspace", {}).get("production_allocation_percent") == 0, "CTA stopped workspace allocation drift")
    _require(workspace.get("decision_gates", {}).get("production_activation_allowed") is False, "CTA stopped workspace activation gate remains open")
    experiments = {
        row.get("tracking_key"): row
        for row in workspace.get("experiments", [])
        if isinstance(row, dict)
    }
    _require(set(experiments) == {"vevo-sk-aa-001", EXPERIMENT_ID}, "CTA stopped workspace experiment set drift")
    aa = experiments["vevo-sk-aa-001"]
    cta = experiments[EXPERIMENT_ID]
    _require(aa.get("status") == "stopped_production_aa_pass_verified", "A/A is not stopped after CTA")
    _require(aa.get("production_allocation_percent") == 0, "A/A allocation changed after CTA")
    _require(cta.get("status") == "stopped_production_cta_followup_pending", "CTA workspace is not stopped")
    _require(cta.get("feature_rule_status") == "staging_only", "CTA Production feature rule remains live")
    _require(cta.get("feature_rule_environments") == ["staging"], "CTA staging preservation drift")
    _require(cta.get("production_allocation_percent") == 0, "CTA stopped allocation is nonzero")
    _require(cta.get("feature_rule_revision") == observation["growthbook"]["feature_revision"], "CTA stopped feature revision drift")
    evidence = cta.get("completion_evidence")
    _require(
        evidence
        == {
            "stop_observation_sha256": _hash_bytes(canonical_json_bytes(observation)),
            "assignment_ended_at_utc": observation["assignment_ended_at_utc"],
            "followup_due_utc": _utc_text(
                _parse_utc(observation["assignment_ended_at_utc"], "assignment_ended_at_utc")
                + timedelta(days=14)
            ),
            "winner_called": False,
            "commerce_unchanged": True,
        },
        "CTA workspace completion evidence drift",
    )
    _require(activation.get("status") == CTA_STOPPED, "CTA stopped activation state drift")


def validate_manifest(
    completion: Mapping[str, Any],
    activation: Mapping[str, Any],
    measurement: Mapping[str, Any],
    sample: Mapping[str, Any],
    contract: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    *,
    lifecycle_observation: Mapping[str, Any] | None = None,
    start_observation: Mapping[str, Any] | None = None,
    stop_observation: Mapping[str, Any] | None = None,
    workspace: Mapping[str, Any] | None = None,
    source_hashes: Mapping[str, str] | None = None,
) -> None:
    validate_activation_manifest(activation)
    validate_plan(sample)
    validate_contract(contract)
    validate_lifecycle_manifest(lifecycle, lifecycle_observation)
    root = _exact(completion, ROOT_KEYS, "CTA completion manifest")
    _require(root["schema_version"] == 1, "CTA completion schema drift")
    _require(root["completion_type"] == "vevo_growthbook_product_cta_assignment_completion", "CTA completion type drift")
    _require(root["experiment_id"] == EXPERIMENT_ID, "CTA completion experiment drift")
    _require(root["status"] in {WAITING, FOLLOWUP}, "CTA completion status drift")
    bindings = _exact(root["source_bindings"], BINDING_KEYS, "CTA completion source bindings")
    expected_paths = {
        "activation_path": "projects/vevo/growthbook_cta_activation.json",
        "measurement_window_path": "projects/vevo/growthbook_cta_measurement_window.json",
        "sample_plan_path": "projects/vevo/growthbook_cta_sample_plan.json",
        "decision_contract_path": "projects/vevo/growthbook_cta_decision_contract.json",
        "lifecycle_reconciliation_path": "projects/vevo/growthbook_cta_lifecycle_reconciliation.json",
    }
    for field, expected in expected_paths.items():
        _require(bindings[field] == expected, f"CTA completion source path drift: {field}")
    all_hashes = dict(source_hashes or _source_hashes())
    actual_hashes = {
        key: all_hashes[key]
        for key in (
            "activation",
            "measurement_window",
            "sample_plan",
            "decision_contract",
            "lifecycle_reconciliation",
        )
    }
    _require(
        set(actual_hashes)
        == {"activation", "measurement_window", "sample_plan", "decision_contract", "lifecycle_reconciliation"},
        "CTA completion source hash set drift",
    )
    _require(bindings["decision_contract_sha256"] == actual_hashes["decision_contract"] == EXPECTED_DECISION_SHA256, "CTA completion decision hash drift")
    stop = _exact(root["stop_readback"], STOP_KEYS, "CTA completion stop readback")
    _require(stop["observation_path"] == "projects/vevo/growthbook_cta_assignment_stop_observation.json", "CTA completion observation path drift")
    followup = _exact(root["followup"], FOLLOWUP_KEYS, "CTA completion follow-up")
    _require(followup["timezone"] == "Europe/Bratislava", "CTA completion timezone drift")
    _require(followup["required_days_after_assignment_stop"] == 14, "CTA completion follow-up duration drift")
    _require(followup["one_final_look_only"] is True, "CTA completion final-look rule drift")
    boundaries = _exact(root["release_boundaries"], BOUNDARY_KEYS, "CTA completion boundaries")
    _require(not any(boundaries.values()), "CTA completion forbidden release boundary opened")

    if root["status"] == WAITING:
        for field in (
            "activation_sha256",
            "measurement_window_sha256",
            "sample_plan_sha256",
            "lifecycle_reconciliation_sha256",
        ):
            _require(bindings[field] is None, f"waiting CTA completion bound {field}")
        _require(
            stop
            == {
                "status": "not_recorded",
                "observation_path": "projects/vevo/growthbook_cta_assignment_stop_observation.json",
                "observation_sha256": None,
                "observed_at_utc": None,
                "assignment_ended_at_utc": None,
                "growthbook_build": None,
                "feature_revision": None,
                "gtm_container_version_id": None,
            },
            "waiting CTA completion stop readback drift",
        )
        _require(
            followup
            == {
                "timezone": "Europe/Bratislava",
                "required_days_after_assignment_stop": 14,
                "assignment_ended_at_utc": None,
                "final_snapshot_due_utc": None,
                "status": "not_started",
                "protected_final_snapshot_workflow_allowed": False,
                "one_final_look_only": True,
            },
            "waiting CTA completion follow-up drift",
        )
        _require(stop_observation is None and workspace is None, "waiting CTA completion has stopped-state evidence")
        _require(
            root["next_gate"]
            == "after_outcome_blind_window_resolution_manually_stop_only_exact_cta_then_record_canonical_readback",
            "waiting CTA completion next gate drift",
        )
        return

    _require(activation.get("status") == CTA_STOPPED, "CTA completion activation is not stopped")
    _require(measurement.get("status") == WINDOW_STOPPED, "CTA completion measurement is not stopped")
    _require(start_observation is not None, "CTA completion start observation missing")
    _require(stop_observation is not None, "CTA completion stop observation missing")
    _require(workspace is not None, "CTA completion stopped workspace missing")
    for name in ("activation", "measurement_window", "sample_plan", "lifecycle_reconciliation"):
        _require(bindings[f"{name}_sha256"] == actual_hashes[name], f"CTA completion {name} hash drift")
    _require(sample.get("status") == "sample_frozen_activation_still_blocked", "CTA completion sample is not frozen")
    _require(lifecycle.get("verified") is True, "CTA completion lifecycle reconciliation is not verified")
    measurement_hashes = {
        "activation": actual_hashes["activation"],
        "start_observation": all_hashes["start_observation"],
        "sample_plan": actual_hashes["sample_plan"],
        "decision_contract": actual_hashes["decision_contract"],
        "reconciliation_evidence": all_hashes["reconciliation_evidence"],
    }
    validate_measurement_manifest(
        measurement,
        activation,
        sample,
        contract,
        reconciliation,
        start_observation,
        stop_observation,
        source_hashes=measurement_hashes,
    )
    validate_stop_observation(stop_observation, activation, measurement)
    observation_sha256 = _hash_bytes(canonical_json_bytes(stop_observation))
    ended = _parse_utc(stop_observation["assignment_ended_at_utc"], "assignment_ended_at_utc")
    due = ended + timedelta(days=14)
    _require(
        stop
        == {
            "status": "verified_zero_production_allocation",
            "observation_path": "projects/vevo/growthbook_cta_assignment_stop_observation.json",
            "observation_sha256": observation_sha256,
            "observed_at_utc": stop_observation["observed_at_utc"],
            "assignment_ended_at_utc": _utc_text(ended),
            "growthbook_build": stop_observation["growthbook"]["build"],
            "feature_revision": stop_observation["growthbook"]["feature_revision"],
            "gtm_container_version_id": "15",
        },
        "CTA completion stopped readback drift",
    )
    _require(
        followup
        == {
            "timezone": "Europe/Bratislava",
            "required_days_after_assignment_stop": 14,
            "assignment_ended_at_utc": _utc_text(ended),
            "final_snapshot_due_utc": _utc_text(due),
            "status": "waiting_for_complete_14_day_outcome_maturity",
            "protected_final_snapshot_workflow_allowed": True,
            "one_final_look_only": True,
        },
        "CTA completion stopped follow-up drift",
    )
    _validate_stopped_workspace(workspace, activation, stop_observation)
    _require(
        root["next_gate"]
        == "after_final_snapshot_due_run_one_protected_outcome_read_and_offline_evaluation",
        "CTA completion stopped next gate drift",
    )


def _stopped_workspace(
    workspace: Mapping[str, Any],
    activation: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> dict[str, Any]:
    updated = copy.deepcopy(workspace)
    _require(
        updated.get("state")
        == "production_cta_running_activation_verified_pro_quantiles_blocked",
        "GrowthBook workspace is not in the running CTA state",
    )
    experiments = {
        row.get("tracking_key"): row
        for row in updated.get("experiments", [])
        if isinstance(row, dict)
    }
    cta = experiments.get(EXPERIMENT_ID)
    _require(isinstance(cta, dict) and cta.get("status") == "running_production_cta_only", "CTA workspace is not running")
    _require(cta.get("feature_rule_revision") == activation["start_readback"]["feature_revision"], "CTA running revision drift")
    updated["state"] = "production_cta_stopped_followup_pending_pro_quantiles_blocked"
    updated["workspace"]["production_allocation_percent"] = 0
    updated["decision_gates"]["production_activation_allowed"] = False
    ended = _parse_utc(observation["assignment_ended_at_utc"], "assignment_ended_at_utc")
    cta.update(
        {
            "status": "stopped_production_cta_followup_pending",
            "feature_rule_status": "staging_only",
            "feature_rule_revision": observation["growthbook"]["feature_revision"],
            "feature_rule_environments": ["staging"],
            "production_allocation_percent": 0,
            "completion_evidence": {
                "stop_observation_sha256": _hash_bytes(canonical_json_bytes(observation)),
                "assignment_ended_at_utc": _utc_text(ended),
                "followup_due_utc": _utc_text(ended + timedelta(days=14)),
                "winner_called": False,
                "commerce_unchanged": True,
            },
        }
    )
    return updated


def record_stop(
    completion: Mapping[str, Any],
    activation: Mapping[str, Any],
    measurement: Mapping[str, Any],
    sample: Mapping[str, Any],
    contract: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    lifecycle_observation: Mapping[str, Any] | None,
    reconciliation: Mapping[str, Any],
    workspace: Mapping[str, Any],
    final_snapshot_manifest: Mapping[str, Any],
    start_observation: Mapping[str, Any],
    stop_observation: Mapping[str, Any],
    *,
    stop_observation_sha256: str,
    source_hashes: Mapping[str, str] | None = None,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    validate_manifest(
        completion,
        activation,
        measurement,
        sample,
        contract,
        lifecycle,
        reconciliation,
        lifecycle_observation=lifecycle_observation,
        source_hashes=source_hashes,
    )
    _require(completion.get("status") == WAITING, "CTA stop is already recorded")
    _require(activation.get("status") == CTA_RUNNING, "CTA activation is not running")
    _require(measurement.get("status") == WINDOW_RESOLVED, "CTA outcome-blind stop rule is unresolved")
    _require(measurement.get("assignment_stop", {}).get("manual_review_allowed") is True, "CTA manual stop review gate is closed")
    digest = str(stop_observation_sha256 or "").strip()
    _require(SHA256_RE.fullmatch(digest) is not None, "CTA stop observation SHA-256 is invalid")
    _require(_hash_bytes(canonical_json_bytes(stop_observation)) == digest, "CTA stop observation SHA-256 mismatch")
    validate_stop_observation(stop_observation, activation, measurement)

    updated_activation = copy.deepcopy(activation)
    updated_activation["status"] = CTA_STOPPED
    updated_activation["next_gate"] = "use_growthbook_cta_completion_for_followup_and_one_final_look"
    validate_activation_manifest(updated_activation)
    activation_bytes = pretty_json_bytes(updated_activation)

    updated_measurement = copy.deepcopy(measurement)
    updated_measurement["status"] = WINDOW_STOPPED
    updated_measurement["source_bindings"]["activation_sha256"] = _hash_bytes(activation_bytes)
    updated_measurement["assignment_stop"].update(
        {
            "status": "verified_manual_stop_readback_followup_pending",
            "manual_review_allowed": False,
            "observation_sha256": digest,
            "assignment_ended_at_utc": stop_observation["assignment_ended_at_utc"],
        }
    )
    updated_measurement["next_gate"] = "wait_exact_14_day_followup_then_run_one_protected_final_snapshot"
    all_hashes = dict(source_hashes or _source_hashes())
    measurement_source_hashes = {
        "activation": _hash_bytes(activation_bytes),
        "start_observation": all_hashes["start_observation"],
        "sample_plan": all_hashes["sample_plan"],
        "decision_contract": all_hashes["decision_contract"],
        "reconciliation_evidence": all_hashes["reconciliation_evidence"],
    }
    validate_measurement_manifest(
        updated_measurement,
        updated_activation,
        sample,
        contract,
        reconciliation,
        start_observation,
        stop_observation,
        source_hashes=measurement_source_hashes,
    )
    measurement_bytes = pretty_json_bytes(updated_measurement)
    updated_workspace = _stopped_workspace(workspace, activation, stop_observation)

    updated_completion = copy.deepcopy(completion)
    updated_completion["status"] = FOLLOWUP
    bindings = updated_completion["source_bindings"]
    final_source_hashes = dict(all_hashes)
    final_source_hashes["activation"] = _hash_bytes(activation_bytes)
    final_source_hashes["measurement_window"] = _hash_bytes(measurement_bytes)
    for name in ("activation", "measurement_window", "sample_plan", "lifecycle_reconciliation"):
        bindings[f"{name}_sha256"] = final_source_hashes[name]
    ended = _parse_utc(stop_observation["assignment_ended_at_utc"], "assignment_ended_at_utc")
    updated_completion["stop_readback"].update(
        {
            "status": "verified_zero_production_allocation",
            "observation_sha256": digest,
            "observed_at_utc": stop_observation["observed_at_utc"],
            "assignment_ended_at_utc": _utc_text(ended),
            "growthbook_build": stop_observation["growthbook"]["build"],
            "feature_revision": stop_observation["growthbook"]["feature_revision"],
            "gtm_container_version_id": "15",
        }
    )
    updated_completion["followup"].update(
        {
            "assignment_ended_at_utc": _utc_text(ended),
            "final_snapshot_due_utc": _utc_text(ended + timedelta(days=14)),
            "status": "waiting_for_complete_14_day_outcome_maturity",
            "protected_final_snapshot_workflow_allowed": True,
        }
    )
    updated_completion["next_gate"] = "after_final_snapshot_due_run_one_protected_outcome_read_and_offline_evaluation"
    validate_manifest(
        updated_completion,
        updated_activation,
        updated_measurement,
        sample,
        contract,
        lifecycle,
        reconciliation,
        lifecycle_observation=lifecycle_observation,
        start_observation=start_observation,
        stop_observation=stop_observation,
        workspace=updated_workspace,
        source_hashes=final_source_hashes,
    )
    completion_bytes = pretty_json_bytes(updated_completion)
    updated_final_snapshot = final_snapshot_builder.opened_manifest(
        final_snapshot_manifest,
        completion_bytes=completion_bytes,
        activation_bytes=activation_bytes,
        measurement_bytes=measurement_bytes,
        sample_plan_bytes=pretty_json_bytes(sample),
        lifecycle_bytes=pretty_json_bytes(lifecycle),
        stop_observation_bytes=canonical_json_bytes(stop_observation),
    )
    return (
        updated_completion,
        updated_activation,
        updated_measurement,
        updated_workspace,
        updated_final_snapshot,
    )


def _write_atomic(path: Path, value: Mapping[str, Any]) -> None:
    payload = pretty_json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--completion", type=Path, default=COMPLETION_PATH)
    parser.add_argument("--activation", type=Path, default=ACTIVATION_PATH)
    parser.add_argument("--measurement-window", type=Path, default=MEASUREMENT_PATH)
    parser.add_argument("--sample-plan", type=Path, default=SAMPLE_PLAN_PATH)
    parser.add_argument("--decision-contract", type=Path, default=DECISION_CONTRACT_PATH)
    parser.add_argument("--lifecycle", type=Path, default=LIFECYCLE_PATH)
    parser.add_argument("--lifecycle-observation", type=Path)
    parser.add_argument("--reconciliation", type=Path, default=RECONCILIATION_PATH)
    parser.add_argument("--workspace", type=Path, default=WORKSPACE_PATH)
    parser.add_argument(
        "--final-snapshot-manifest", type=Path, default=FINAL_SNAPSHOT_PATH
    )
    parser.add_argument("--start-observation", type=Path, default=START_OBSERVATION_PATH)
    parser.add_argument("--stop-observation", type=Path, default=STOP_OBSERVATION_PATH)
    parser.add_argument("--stop-observation-sha256", required=True)
    parser.add_argument("--completion-output", type=Path, required=True)
    parser.add_argument("--activation-output", type=Path, required=True)
    parser.add_argument("--measurement-output", type=Path, required=True)
    parser.add_argument("--workspace-output", type=Path, required=True)
    parser.add_argument("--final-snapshot-output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        outputs = {
            args.completion_output.resolve(),
            args.activation_output.resolve(),
            args.measurement_output.resolve(),
            args.workspace_output.resolve(),
            args.final_snapshot_output.resolve(),
        }
        _require(len(outputs) == 5, "CTA stop output paths must be distinct")
        stop_observation = load_canonical(
            args.stop_observation,
            args.stop_observation_sha256,
            "CTA stop observation",
        )
        lifecycle = _load(args.lifecycle, "CTA lifecycle reconciliation")
        lifecycle_observation = None
        if lifecycle.get("verified") is True:
            lifecycle_observation_path = args.lifecycle_observation
            if lifecycle_observation_path is None:
                lifecycle_observation_path = ROOT / str(lifecycle["observation_path"])
            lifecycle_observation = load_canonical(
                lifecycle_observation_path,
                lifecycle["observation_sha256"],
                "CTA lifecycle observation",
            )
        source_hashes = _source_hashes(
            activation_path=args.activation,
            measurement_path=args.measurement_window,
            sample_plan_path=args.sample_plan,
            decision_contract_path=args.decision_contract,
            lifecycle_path=args.lifecycle,
            start_observation_path=args.start_observation,
            reconciliation_path=args.reconciliation,
        )
        recorded = record_stop(
            _load(args.completion, "CTA completion"),
            _load(args.activation, "CTA activation"),
            _load(args.measurement_window, "CTA measurement window"),
            _load(args.sample_plan, "CTA sample plan"),
            _load(args.decision_contract, "CTA decision contract"),
            lifecycle,
            lifecycle_observation,
            _load(args.reconciliation, "Production reconciliation evidence"),
            _load(args.workspace, "GrowthBook workspace"),
            _load(args.final_snapshot_manifest, "CTA final snapshot manifest"),
            _load(args.start_observation, "CTA start observation"),
            stop_observation,
            stop_observation_sha256=args.stop_observation_sha256,
            source_hashes=source_hashes,
        )
        for path, value in zip(
            (
                args.completion_output,
                args.activation_output,
                args.measurement_output,
                args.workspace_output,
                args.final_snapshot_output,
            ),
            recorded,
            strict=True,
        ):
            _write_atomic(path, value)
        print("VEVO_CTA_STOP_RECORDED:allocation=0:followup=14d:final-look=protected:mutation=manual-growthbook-only")
        return 0
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(f"record_growthbook_cta_completion.py: FAIL: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
