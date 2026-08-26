#!/usr/bin/env python3
"""Open and record the reviewed VEVO CTA activation without external mutation.

The recorder is intentionally offline. It has no browser, network, AWS,
GrowthBook, GTM, Meta Ads, BiznisWeb, traffic, price, cart, checkout, or order
client. ``open-review`` binds the exact post-A/A sources and a separately
host-verified CTA-only collector runtime before allowing a manual GrowthBook
start. ``record-start`` accepts only a canonical, independently hashed manual
readback and updates the versioned activation/workspace state.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import ipaddress
import json
import math
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

try:
    from scripts import record_growthbook_pro_upgrade as pro_upgrade_recorder
except ModuleNotFoundError:  # Direct execution from scripts/.
    import record_growthbook_pro_upgrade as pro_upgrade_recorder  # type: ignore


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ACTIVATION_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_activation.json"
DEFAULT_COMPLETION_PATH = ROOT / "projects" / "vevo" / "growthbook_production_aa_completion.json"
DEFAULT_AA_SNAPSHOT_PATH = ROOT / "projects" / "vevo" / "growthbook_aa_snapshot.json"
DEFAULT_PRO_UPGRADE_PATH = ROOT / "projects" / "vevo" / "growthbook_pro_upgrade.json"
DEFAULT_PRO_OBSERVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_pro_upgrade_observation.json"
)
DEFAULT_SAMPLE_PLAN_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_sample_plan.json"
DEFAULT_LIFECYCLE_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_lifecycle_reconciliation.json"
DEFAULT_DESIGN_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_design.json"
DEFAULT_DECISION_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_decision_contract.json"
DEFAULT_META_REPORTING_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_meta_reporting_contract.json"
)
DEFAULT_REGISTRY_PATH = ROOT / "growthbook_collector" / "experiments.json"
DEFAULT_WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"

WAITING = "waiting_for_verified_aa_completion_sample_lifecycle_and_runtime"
REVIEW_OPEN = "manual_cta_start_review_allowed"
RUNNING = "production_cta_running_activation_verified"
STOPPED = "production_cta_start_recorded_assignment_stopped_verified"
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
RUN_RE = re.compile(r"^[1-9][0-9]*$")
TASK_RE = re.compile(r"^vevo-growthbook-collector-production:[1-9][0-9]*$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
CTA_GUARDRAILS = [
    "vevo_client_error_device_rate_24h",
    "vevo_lcp_p75_24h",
    "vevo_inp_p75_24h",
    "vevo_cls_p75_milli_24h",
]

EXPECTED_PATHS = {
    "aa_completion": "projects/vevo/growthbook_production_aa_completion.json",
    "aa_snapshot": "projects/vevo/growthbook_aa_snapshot.json",
    "pro_upgrade": "projects/vevo/growthbook_pro_upgrade.json",
    "pro_upgrade_observation": "projects/vevo/growthbook_pro_upgrade_observation.json",
    "sample_plan": "projects/vevo/growthbook_cta_sample_plan.json",
    "lifecycle_reconciliation": "projects/vevo/growthbook_cta_lifecycle_reconciliation.json",
    "design_contract": "projects/vevo/growthbook_cta_design.json",
    "decision_contract": "projects/vevo/growthbook_cta_decision_contract.json",
    "meta_reporting_contract": "projects/vevo/growthbook_meta_reporting_contract.json",
    "collector_registry": "growthbook_collector/experiments.json",
}
EXPECTED_STATIC_HASHES = {
    "design_contract": "fef74b323f154f9476db4fd00c171c297043b7662f39d871284d6c417d2ece6d",
    "decision_contract": "ced267f0152a97e8a25c3cf70e23cbdcebec2ecd6761f05134bf2c9507518183",
    "meta_reporting_contract": "0578cf48c00485071cb535670e113ad9dcfaf5ac09b2fa4308ea5bd0b3c19bd3",
}


class CtaActivationRecordingError(ValueError):
    """Raised when the CTA activation contract or evidence is unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaActivationRecordingError(message)


def _exact_object(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} fields drift")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    _require(isinstance(value, str) and value.endswith("Z"), f"{field} must be UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise CtaActivationRecordingError(f"{field} is invalid") from exc
    _require(parsed.isoformat().replace("+00:00", "Z") == value, f"{field} is not canonical")
    return parsed


def _positive_int(value: Any, field: str) -> int:
    _require(isinstance(value, int) and not isinstance(value, bool) and value > 0, f"{field} must be positive")
    return value


def _finite_number(value: Any, field: str) -> float:
    _require(isinstance(value, (int, float)) and not isinstance(value, bool), f"{field} must be numeric")
    number = float(value)
    _require(math.isfinite(number), f"{field} must be finite")
    return number


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_hash(value: Any, field: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    _require(isinstance(value, str) and HASH_RE.fullmatch(value) is not None, f"{field} is invalid")


def validate_manifest(manifest: Mapping[str, Any]) -> None:
    root = _exact_object(
        manifest,
        {
            "schema_version", "activation_type", "experiment_id", "feature_key", "status",
            "runbook", "source_bindings", "launch_contract", "runtime_readiness_requirements",
            "start_readback", "release_boundaries", "next_gate",
        },
        "CTA activation manifest",
    )
    _require(root["schema_version"] == 1, "CTA activation schema drift")
    _require(root["activation_type"] == "vevo_growthbook_product_cta", "CTA activation type drift")
    _require(root["experiment_id"] == "vevo-sk-product-cta-color-001", "CTA experiment drift")
    _require(root["feature_key"] == "vevo-sk-product-cta-color", "CTA feature drift")
    _require(root["runbook"] == "projects/vevo/GROWTHBOOK_CTA_ACTIVATION_RUNBOOK.md", "CTA activation runbook drift")
    _require(
        root["status"] in {WAITING, REVIEW_OPEN, RUNNING, STOPPED},
        "CTA activation status drift",
    )

    bindings = _exact_object(
        root["source_bindings"],
        {
            "aa_completion", "aa_snapshot", "pro_upgrade",
            "pro_upgrade_observation", "sample_plan", "lifecycle_reconciliation",
            "design_contract", "decision_contract", "meta_reporting_contract",
            "collector_registry", "runtime_readiness",
        },
        "source_bindings",
    )
    for name, expected_path in EXPECTED_PATHS.items():
        binding = _exact_object(bindings[name], {"path", "sha256"}, f"source_bindings.{name}")
        _require(binding["path"] == expected_path, f"{name} path drift")
        _validate_hash(binding["sha256"], f"{name} SHA-256", nullable=name not in EXPECTED_STATIC_HASHES)
        if name in EXPECTED_STATIC_HASHES:
            _require(binding["sha256"] == EXPECTED_STATIC_HASHES[name], f"{name} SHA-256 drift")
    runtime_binding = _exact_object(
        bindings["runtime_readiness"],
        {"observation_path", "observation_sha256", "workflow_run_id", "main_commit"},
        "source_bindings.runtime_readiness",
    )
    _require(
        runtime_binding["observation_path"]
        == "projects/vevo/growthbook_cta_runtime_readiness_observation.json",
        "runtime readiness observation path drift",
    )

    launch = _exact_object(
        root["launch_contract"],
        {
            "environment", "assignment_attribute", "traffic_percent", "variation_weights",
            "target_total_sample", "minimum_full_calendar_days", "maximum_full_calendar_days",
            "assignment_stop_rule", "checkpoint_policy", "gtm_container_version_id",
            "growthbook_experiment_id", "data_source_id", "production_registry_experiments",
        },
        "launch_contract",
    )
    _require(launch["environment"] == "production", "CTA environment drift")
    _require(launch["assignment_attribute"] == "id", "CTA assignment attribute drift")
    _require(launch["traffic_percent"] == 100, "CTA traffic contract drift")
    _require(launch["variation_weights"] == {"control": 0.5, "brand_contrast": 0.5}, "CTA weights drift")
    _require(launch["minimum_full_calendar_days"] == 14, "CTA minimum days drift")
    _require(launch["maximum_full_calendar_days"] == 42, "CTA maximum days drift")
    _require(
        launch["assignment_stop_rule"]
        == "first_successful_post_reconciliation_checkpoint_with_target_first_n_or_42_full_calendar_days",
        "CTA stopping rule drift",
    )
    _require(launch["checkpoint_policy"] == "outcome_blind_eligible_device_count_only", "CTA checkpoint policy drift")
    _require(launch["gtm_container_version_id"] == "15", "CTA GTM version drift")
    _require(launch["growthbook_experiment_id"] == "exp_19g6mmt1qxzrp", "CTA GrowthBook ID drift")
    _require(launch["data_source_id"] == "ds_19g6mmt5stlp6", "CTA data source drift")
    _require(launch["production_registry_experiments"] == [root["experiment_id"]], "CTA-only registry contract drift")

    requirements = _exact_object(
        root["runtime_readiness_requirements"],
        {
            "instance_id", "service", "runtime_path", "localhost_marker_required", "target_health",
            "cta_only_production_registry_required", "zero_cta_events_before_start_required",
            "gtm_unprocessed_changes_required", "aa_production_allocation_percent_required",
            "cta_production_allocation_percent_required",
        },
        "runtime_readiness_requirements",
    )
    _require(requirements == {
        "instance_id": "N/A:Fargate",
        "service": "vevo-growthbook-collector-production",
        "runtime_path": "/app",
        "localhost_marker_required": True,
        "target_health": "healthy",
        "cta_only_production_registry_required": True,
        "zero_cta_events_before_start_required": True,
        "gtm_unprocessed_changes_required": 0,
        "aa_production_allocation_percent_required": 0,
        "cta_production_allocation_percent_required": 0,
    }, "CTA runtime requirements drift")

    readback = _exact_object(
        root["start_readback"],
        {"status", "observation_path", "observation_sha256", "observed_at_utc", "assignment_started_at_utc", "feature_revision"},
        "start_readback",
    )
    _require(readback["observation_path"] == "projects/vevo/growthbook_cta_activation_observation.json", "CTA start observation path drift")
    boundaries = _exact_object(
        root["release_boundaries"],
        {
            "manual_growthbook_start_allowed", "automatic_growthbook_mutation_allowed",
            "automatic_gtm_mutation_allowed", "automatic_meta_ads_mutation_allowed",
            "automatic_biznisweb_mutation_allowed", "automatic_collector_or_reporting_mutation_allowed",
            "price_product_cart_checkout_order_mutation_allowed", "winner_calls_allowed",
        },
        "release_boundaries",
    )
    for field in (
        "automatic_growthbook_mutation_allowed", "automatic_gtm_mutation_allowed",
        "automatic_meta_ads_mutation_allowed", "automatic_biznisweb_mutation_allowed",
        "automatic_collector_or_reporting_mutation_allowed",
        "price_product_cart_checkout_order_mutation_allowed", "winner_calls_allowed",
    ):
        _require(boundaries[field] is False, f"CTA forbidden boundary opened: {field}")

    dynamic_hashes = (
        "aa_completion", "aa_snapshot", "pro_upgrade", "pro_upgrade_observation",
        "sample_plan", "lifecycle_reconciliation", "collector_registry",
    )
    if root["status"] == WAITING:
        for name in dynamic_hashes:
            _require(bindings[name]["sha256"] is None, f"waiting CTA manifest binds {name} too early")
        _require(all(runtime_binding[field] is None for field in ("observation_sha256", "workflow_run_id", "main_commit")), "waiting CTA runtime binding is populated")
        _require(launch["target_total_sample"] is None, "waiting CTA target is populated")
        _require(readback == {
            "status": "not_recorded",
            "observation_path": "projects/vevo/growthbook_cta_activation_observation.json",
            "observation_sha256": None,
            "observed_at_utc": None,
            "assignment_started_at_utc": None,
            "feature_revision": None,
        }, "waiting CTA readback drift")
        _require(boundaries["manual_growthbook_start_allowed"] is False, "waiting CTA manual gate is open")
        _require(root["next_gate"] == "after_aa_pass_stop_sample_freeze_lifecycle_and_cta_only_host_gate_open_manual_start_review", "waiting CTA next gate drift")
        return

    for name in dynamic_hashes:
        _validate_hash(bindings[name]["sha256"], f"{name} SHA-256")
    _validate_hash(runtime_binding["observation_sha256"], "runtime observation SHA-256")
    _require(isinstance(runtime_binding["workflow_run_id"], str) and RUN_RE.fullmatch(runtime_binding["workflow_run_id"]) is not None, "runtime workflow run ID is invalid")
    _require(isinstance(runtime_binding["main_commit"], str) and COMMIT_RE.fullmatch(runtime_binding["main_commit"]) is not None, "runtime main commit is invalid")
    target = _positive_int(launch["target_total_sample"], "target_total_sample")
    _require(target % 2 == 0, "CTA target sample must preserve equal allocation")

    if root["status"] == REVIEW_OPEN:
        _require(boundaries["manual_growthbook_start_allowed"] is True, "reviewed CTA manual gate is closed")
        _require(readback["status"] == "not_recorded", "CTA start was recorded before manual start")
        _require(all(readback[field] is None for field in ("observation_sha256", "observed_at_utc", "assignment_started_at_utc", "feature_revision")), "CTA readback fields populated before start")
        _require(root["next_gate"] == "manually_start_only_exact_cta_then_record_canonical_readback", "CTA reviewed next gate drift")
        return

    _require(boundaries["manual_growthbook_start_allowed"] is False, "running CTA manual start gate remains open")
    _require(readback["status"] == "verified_running_production_cta", "CTA running readback status drift")
    _validate_hash(readback["observation_sha256"], "CTA start observation SHA-256")
    observed = _parse_utc(readback["observed_at_utc"], "start_readback.observed_at_utc")
    started = _parse_utc(readback["assignment_started_at_utc"], "start_readback.assignment_started_at_utc")
    _require(started <= observed, "CTA start readback predates assignment start")
    _positive_int(readback["feature_revision"], "start_readback.feature_revision")
    expected_next_gate = (
        "monitor_outcome_blind_first_n_and_safety_stop_conditions"
        if root["status"] == RUNNING
        else "use_growthbook_cta_completion_for_followup_and_one_final_look"
    )
    _require(root["next_gate"] == expected_next_gate, "CTA post-start next gate drift")


def validate_runtime_observation(observation: Mapping[str, Any], manifest: Mapping[str, Any]) -> None:
    root = _exact_object(
        observation,
        {"schema_version", "evidence_type", "experiment_id", "observed_at_utc", "workflow", "runtime", "control_plane", "safety"},
        "CTA runtime readiness observation",
    )
    _require(root["schema_version"] == 1, "CTA runtime observation schema drift")
    _require(root["evidence_type"] == "vevo_growthbook_cta_runtime_readiness", "CTA runtime evidence type drift")
    _require(root["experiment_id"] == manifest["experiment_id"], "CTA runtime experiment drift")
    _parse_utc(root["observed_at_utc"], "runtime observed_at_utc")
    workflow = _exact_object(root["workflow"], {"run_id", "main_commit", "conclusion"}, "runtime workflow")
    _require(isinstance(workflow["run_id"], str) and RUN_RE.fullmatch(workflow["run_id"]) is not None, "runtime workflow run ID is invalid")
    _require(isinstance(workflow["main_commit"], str) and COMMIT_RE.fullmatch(workflow["main_commit"]) is not None, "runtime workflow main commit is invalid")
    _require(workflow["conclusion"] == "success", "CTA runtime workflow did not succeed")
    runtime = _exact_object(root["runtime"], {"instance_id", "private_ip", "service", "runtime_path", "task_definition", "image_digest", "host_gate_task_id", "host_gate_private_ip", "localhost_marker_verified", "target_health"}, "runtime")
    _require(runtime["instance_id"] == "N/A:Fargate", "CTA runtime instance drift")
    try:
        ipaddress.IPv4Address(runtime["private_ip"])
    except (ipaddress.AddressValueError, TypeError) as exc:
        raise CtaActivationRecordingError("CTA runtime private IP is invalid") from exc
    try:
        ipaddress.IPv4Address(runtime["host_gate_private_ip"])
    except (ipaddress.AddressValueError, TypeError) as exc:
        raise CtaActivationRecordingError("CTA host-gate private IP is invalid") from exc
    _require(runtime["service"] == "vevo-growthbook-collector-production", "CTA runtime service drift")
    _require(runtime["runtime_path"] == "/app", "CTA runtime path drift")
    _require(isinstance(runtime["task_definition"], str) and TASK_RE.fullmatch(runtime["task_definition"]) is not None, "CTA task definition is invalid")
    _require(isinstance(runtime["host_gate_task_id"], str) and TASK_ID_RE.fullmatch(runtime["host_gate_task_id"]) is not None, "CTA host-gate task ID is invalid")
    _require(isinstance(runtime["image_digest"], str) and IMAGE_RE.fullmatch(runtime["image_digest"]) is not None, "CTA runtime image digest is invalid")
    _require(runtime["localhost_marker_verified"] is True, "CTA localhost marker is not verified")
    _require(runtime["target_health"] == "healthy", "CTA runtime target is not healthy")
    control = _exact_object(root["control_plane"], {"registry_sha256", "production_registry_experiments", "cta_events_before_start", "aa_production_allocation_percent", "cta_production_allocation_percent", "gtm_container_version_id", "gtm_unprocessed_changes"}, "control_plane")
    _validate_hash(control["registry_sha256"], "runtime registry SHA-256")
    _require(control["production_registry_experiments"] == [manifest["experiment_id"]], "runtime Production registry is not CTA-only")
    _require(control["cta_events_before_start"] == 0, "CTA events exist before activation")
    _require(control["aa_production_allocation_percent"] == 0, "A/A Production allocation is nonzero")
    _require(control["cta_production_allocation_percent"] == 0, "CTA Production allocation is nonzero before start")
    _require(control["gtm_container_version_id"] == "15", "runtime GTM version drift")
    _require(control["gtm_unprocessed_changes"] == 0, "GTM has unprocessed changes")
    safety = _exact_object(root["safety"], {"contains_credentials", "contains_event_or_device_ids", "contains_customer_or_order_data", "meta_ads_mutated", "biznisweb_mutated", "price_product_cart_checkout_order_mutated"}, "runtime safety")
    _require(not any(safety.values()), "CTA runtime observation contains unsafe data or mutation")


def validate_start_observation(observation: Mapping[str, Any], manifest: Mapping[str, Any]) -> None:
    root = _exact_object(
        observation,
        {"schema_version", "evidence_type", "experiment_id", "feature_key", "observed_at_utc", "assignment_started_at_utc", "growthbook", "gtm", "tag_assistant", "collector", "commerce", "safety"},
        "CTA start observation",
    )
    _require(root["schema_version"] == 1, "CTA start observation schema drift")
    _require(root["evidence_type"] == "vevo_growthbook_cta_activation_readback", "CTA start evidence type drift")
    _require(root["experiment_id"] == manifest["experiment_id"], "CTA start experiment drift")
    _require(root["feature_key"] == manifest["feature_key"], "CTA start feature drift")
    observed = _parse_utc(root["observed_at_utc"], "observed_at_utc")
    started = _parse_utc(root["assignment_started_at_utc"], "assignment_started_at_utc")
    _require(started <= observed, "CTA observation predates assignment start")
    growthbook = _exact_object(root["growthbook"], {"build", "experiment_id", "experiment_status", "environment", "traffic_percent", "variation_weights", "feature_revision", "feature_revision_status", "active_production_experiments", "aa_status", "data_source_id", "goal_metrics", "secondary_metrics", "guardrail_metrics"}, "growthbook")
    _require(isinstance(growthbook["build"], str) and growthbook["build"], "GrowthBook build is absent")
    _require(growthbook["experiment_id"] == manifest["launch_contract"]["growthbook_experiment_id"], "GrowthBook CTA ID drift")
    _require(growthbook["experiment_status"] == "running", "CTA experiment is not running")
    _require(growthbook["environment"] == "production_only", "CTA experiment environment drift")
    _require(growthbook["traffic_percent"] == 100, "CTA traffic is not 100 percent")
    _require(growthbook["variation_weights"] == {"control": 0.5, "brand_contrast": 0.5}, "CTA live weights drift")
    _positive_int(growthbook["feature_revision"], "growthbook.feature_revision")
    _require(growthbook["feature_revision_status"] == "live", "CTA feature revision is not live")
    _require(growthbook["active_production_experiments"] == [manifest["experiment_id"]], "CTA is not the only active Production experiment")
    _require(growthbook["aa_status"] == "stopped_zero_allocation", "A/A is not stopped at zero allocation")
    _require(growthbook["data_source_id"] == "ds_19g6mmt5stlp6", "CTA data source drift")
    _require(growthbook["goal_metrics"] == ["vevo_add_to_cart_24h"], "CTA goal metric drift")
    _require(growthbook["secondary_metrics"] == ["vevo_average_order_value_7d", "vevo_cancelled_order_rate_14d", "vevo_cm1_per_exposed_device_7d", "vevo_revenue_per_exposed_device_7d", "vevo_purchase_conversion_7d", "vevo_refunded_order_rate_14d"], "CTA secondary metrics drift")
    _require(growthbook["guardrail_metrics"] == CTA_GUARDRAILS, "CTA Pro guardrail metrics drift")
    gtm = _exact_object(root["gtm"], {"container_id", "container_version_id", "unprocessed_changes"}, "gtm")
    _require(gtm == {"container_id": "GTM-5ZB5LFGB", "container_version_id": "15", "unprocessed_changes": 0}, "CTA GTM readback drift")
    qa = _exact_object(root["tag_assistant"], {"connected", "desktop_verified", "mobile_verified", "consent_accept_reject_withdrawal_verified", "control_observed", "brand_contrast_observed", "cta_css_matches_design_contract", "console_error_count"}, "tag_assistant")
    _require(all(qa[field] is True for field in ("connected", "desktop_verified", "mobile_verified", "consent_accept_reject_withdrawal_verified", "control_observed", "brand_contrast_observed", "cta_css_matches_design_contract")), "CTA browser QA is incomplete")
    _require(qa["console_error_count"] == 0, "CTA browser QA has console errors")
    collector = _exact_object(root["collector"], {"accepted_receipt_count", "target_exposure_count", "repeat_exposed_device_count", "sticky_consistent_repeat_device_count", "sticky_inconsistent_device_count", "observed_variations"}, "collector")
    _positive_int(collector["accepted_receipt_count"], "collector.accepted_receipt_count")
    _require(_positive_int(collector["target_exposure_count"], "collector.target_exposure_count") >= 2, "CTA collector must observe both variations")
    _require(_positive_int(collector["repeat_exposed_device_count"], "collector.repeat_exposed_device_count") >= 1, "CTA sticky repeat is absent")
    _require(collector["sticky_consistent_repeat_device_count"] == collector["repeat_exposed_device_count"], "CTA sticky repeat parity drift")
    _require(collector["sticky_inconsistent_device_count"] == 0, "CTA sticky assignment is inconsistent")
    _require(collector["observed_variations"] == ["brand_contrast", "control"], "CTA observed variation set drift")
    commerce = _exact_object(root["commerce"], {"cta_text_unchanged", "cta_dimensions_layout_placement_unchanged", "price_unchanged", "cart_checkout_order_mutated", "probe"}, "commerce")
    _require(
        {key: commerce[key] for key in ("cta_text_unchanged", "cta_dimensions_layout_placement_unchanged", "price_unchanged", "cart_checkout_order_mutated")}
        == {"cta_text_unchanged": True, "cta_dimensions_layout_placement_unchanged": True, "price_unchanged": True, "cart_checkout_order_mutated": False},
        "CTA commerce readback failed",
    )
    probe = _exact_object(
        commerce["probe"],
        {"product_url", "product_code", "cart_url", "cta_text", "price_text"},
        "commerce.probe",
    )
    _require(
        probe["product_url"] == "https://www.vevo.sk/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute"
        and probe["product_code"] == "07500"
        and probe["cart_url"] == "https://www.vevo.sk/e/cart/index"
        and probe["cta_text"] == "Pridať do košíka",
        "CTA commerce probe target drift",
    )
    _require(
        isinstance(probe["price_text"], str)
        and re.fullmatch(r"[0-9]{1,3}(?: [0-9]{3})*,[0-9]{2} €", probe["price_text"]),
        "CTA commerce probe price baseline invalid",
    )
    safety = _exact_object(root["safety"], {"contains_credentials", "contains_event_or_device_ids", "contains_customer_or_order_data", "meta_ads_mutated", "biznisweb_mutated", "collector_or_reporting_mutated"}, "safety")
    _require(not any(safety.values()), "CTA start observation contains unsafe data or mutation")


def _validate_post_aa_sources(
    completion: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    pro_upgrade: Mapping[str, Any],
    pro_observation: Mapping[str, Any],
    sample: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    workspace: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> None:
    _require(completion.get("status") == "production_aa_stopped_verified_cta_activation_blocked", "CTA activation requires verified A/A PASS and stop readback")
    _require(completion.get("aa_pass", {}).get("verdict") == "PASS", "CTA activation requires A/A PASS")
    _require(completion.get("stop_readback", {}).get("status") == "verified_zero_allocation", "CTA activation requires verified zero-allocation A/A stop")
    _require(snapshot.get("snapshot_build_allowed") is True, "A/A protected snapshot gate was not opened")
    try:
        pro_upgrade_recorder.validate_manifest(pro_upgrade, workspace)
        pro_upgrade_recorder.validate_observation(
            pro_observation,
            pro_upgrade,
            workspace,
        )
    except pro_upgrade_recorder.ProUpgradeError as exc:
        raise CtaActivationRecordingError(
            f"GrowthBook Pro source is invalid: {exc}"
        ) from exc
    _require(
        pro_upgrade.get("status") == pro_upgrade_recorder.VERIFIED,
        "CTA activation requires verified GrowthBook Pro metrics",
    )
    _require(
        hashlib.sha256(
            pro_upgrade_recorder.canonical_json_bytes(pro_observation)
        ).hexdigest()
        == pro_upgrade.get("verification", {}).get("observation_sha256"),
        "GrowthBook Pro observation binding drift",
    )
    _require(sample.get("status") == "sample_frozen_activation_still_blocked", "CTA activation requires a frozen sample")
    _require(sample.get("activation_allowed") is False, "CTA sample freeze may not activate CTA")
    _require(lifecycle.get("status") == "verified_production_14d_refund_creditnote_value_reconciliation", "CTA activation requires verified lifecycle reconciliation")
    _require(lifecycle.get("verified") is True and lifecycle.get("activation_allowed") is False, "CTA lifecycle gate is not safely verified")
    _require(workspace.get("state") == "production_aa_completed_cta_sample_freeze_pro_quantiles_verified", "GrowthBook workspace is not post-A/A with verified Pro quantiles")
    _require(workspace.get("workspace", {}).get("production_allocation_percent") == 0, "GrowthBook workspace allocation is nonzero")
    _require(
        workspace.get("workspace", {}).get("plan_type") == "pro"
        and workspace.get("workspace", {}).get("subscription_or_trial_status")
        == "pro_active_paid_monthly_one_seat",
        "GrowthBook Pro subscription is not verified",
    )
    _require(workspace.get("decision_gates", {}).get("production_activation_allowed") is False, "GrowthBook Production gate is already open")
    experiments = {row.get("tracking_key"): row for row in workspace.get("experiments", []) if isinstance(row, dict)}
    _require(set(experiments) == {"vevo-sk-aa-001", "vevo-sk-product-cta-color-001"}, "GrowthBook experiment set drift")
    _require(experiments["vevo-sk-aa-001"].get("production_allocation_percent") == 0, "A/A workspace allocation is nonzero")
    cta = experiments["vevo-sk-product-cta-color-001"]
    _require(cta.get("status") == "draft" and cta.get("feature_rule_status") == "draft", "CTA is not a draft")
    _require(cta.get("production_allocation_percent") == 0, "CTA allocation is nonzero before start")
    _require(
        cta.get("pro_guardrail_metrics") == CTA_GUARDRAILS
        and isinstance(cta.get("pro_quantile_metrics_verified_date"), str),
        "CTA Pro p75 guardrails are not verified",
    )
    metrics = {
        row.get("key"): row
        for row in workspace.get("metrics", [])
        if isinstance(row, dict)
    }
    for key in CTA_GUARDRAILS[1:]:
        metric = metrics.get(key) or {}
        _require(
            metric.get("status")
            == "growthbook_pro_preview_and_production_created_query_verified"
            and isinstance(metric.get("growthbook_id"), str)
            and isinstance(metric.get("production_growthbook_id"), str),
            f"CTA Pro metric is not query-verified: {key}",
        )
    clone = workspace.get("athena", {}).get("production", {}).get("growthbook_clone", {})
    _require(clone.get("paid_pro_upgrade_authorized") is True, "GrowthBook paid Pro upgrade is not recorded")
    production = registry.get("environments", {}).get("production", {})
    preview = registry.get("environments", {}).get("preview", {})
    _require(set(production) == {"vevo-sk-product-cta-color-001"}, "collector Production registry is not CTA-only")
    _require(production["vevo-sk-product-cta-color-001"] == preview.get("vevo-sk-product-cta-color-001"), "collector CTA registry differs from Preview contract")


def open_review(
    manifest: Mapping[str, Any],
    *,
    completion: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    pro_upgrade: Mapping[str, Any],
    pro_observation: Mapping[str, Any],
    sample: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    workspace: Mapping[str, Any],
    registry: Mapping[str, Any],
    runtime_observation: Mapping[str, Any],
    source_hashes: Mapping[str, str],
    runtime_observation_sha256: str,
) -> dict[str, Any]:
    validate_manifest(manifest)
    _require(manifest["status"] == WAITING, "CTA activation review is already opened")
    for name in EXPECTED_PATHS:
        _validate_hash(source_hashes.get(name), f"{name} SHA-256")
        expected_static = EXPECTED_STATIC_HASHES.get(name)
        if expected_static is not None:
            _require(source_hashes[name] == expected_static, f"{name} checked-in SHA-256 drift")
    _require(
        source_hashes["pro_upgrade"]
        == hashlib.sha256(
            pro_upgrade_recorder.canonical_json_bytes(pro_upgrade)
        ).hexdigest(),
        "GrowthBook Pro manifest is not canonical or hash-bound",
    )
    _require(
        source_hashes["pro_upgrade_observation"]
        == hashlib.sha256(
            pro_upgrade_recorder.canonical_json_bytes(pro_observation)
        ).hexdigest()
        == pro_upgrade.get("verification", {}).get("observation_sha256"),
        "GrowthBook Pro observation is not canonical or hash-bound",
    )
    _validate_post_aa_sources(
        completion,
        snapshot,
        pro_upgrade,
        pro_observation,
        sample,
        lifecycle,
        workspace,
        registry,
    )
    validate_runtime_observation(runtime_observation, manifest)
    _require(hashlib.sha256(canonical_json_bytes(runtime_observation)).hexdigest() == runtime_observation_sha256, "CTA runtime observation SHA-256 mismatch")
    runtime_workflow = runtime_observation["workflow"]
    _require(runtime_observation["control_plane"]["registry_sha256"] == source_hashes["collector_registry"], "runtime/checked-in registry SHA-256 drift")
    _require(
        sample["final"]["aa_snapshot_sha256"]
        == completion["aa_pass"]["snapshot_sha256"],
        "CTA sample/A/A PASS snapshot artifact SHA-256 drift",
    )
    _require(lifecycle["observation_sha256"] is not None, "CTA lifecycle observation is not bound")
    target = _positive_int(sample["final"]["total_sample"], "sample.final.total_sample")
    _require(target % 2 == 0, "CTA frozen sample does not preserve equal allocation")

    updated = copy.deepcopy(manifest)
    for name in (
        "aa_completion",
        "aa_snapshot",
        "pro_upgrade",
        "pro_upgrade_observation",
        "sample_plan",
        "lifecycle_reconciliation",
        "collector_registry",
    ):
        updated["source_bindings"][name]["sha256"] = source_hashes[name]
    runtime = updated["source_bindings"]["runtime_readiness"]
    runtime.update({
        "observation_sha256": runtime_observation_sha256,
        "workflow_run_id": runtime_workflow["run_id"],
        "main_commit": runtime_workflow["main_commit"],
    })
    updated["launch_contract"]["target_total_sample"] = target
    updated["status"] = REVIEW_OPEN
    updated["release_boundaries"]["manual_growthbook_start_allowed"] = True
    updated["next_gate"] = "manually_start_only_exact_cta_then_record_canonical_readback"
    validate_manifest(updated)
    return updated


def _running_workspace(workspace: Mapping[str, Any], observation: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    updated = copy.deepcopy(workspace)
    _require(updated.get("state") == "production_aa_completed_cta_sample_freeze_pro_quantiles_verified", "GrowthBook workspace is not ready for CTA start with verified Pro quantiles")
    experiments = {row.get("tracking_key"): row for row in updated.get("experiments", []) if isinstance(row, dict)}
    cta = experiments.get("vevo-sk-product-cta-color-001")
    _require(isinstance(cta, dict) and cta.get("status") == "draft", "GrowthBook CTA workspace draft drift")
    _require(cta.get("production_allocation_percent") == 0, "GrowthBook CTA workspace allocation is nonzero")
    started = _parse_utc(observation["assignment_started_at_utc"], "assignment_started_at_utc")
    growthbook = observation["growthbook"]
    updated["state"] = "production_cta_running_activation_verified_pro_quantiles_verified"
    updated["workspace"]["production_allocation_percent"] = 100
    updated["decision_gates"]["production_activation_allowed"] = True
    cta.update({
        "status": "running_production_cta_only",
        "started_date": started.astimezone(ZoneInfo("Europe/Bratislava")).date().isoformat(),
        "feature_rule_status": "live",
        "feature_rule_revision": growthbook["feature_revision"],
        "feature_rule_environments": ["production"],
        "production_allocation_percent": 100,
        "final_sample_status": "frozen_from_hash_bound_aa_running_exact_first_n",
        "activation_evidence": {
            "observation_sha256": manifest["start_readback"]["observation_sha256"],
            "assignment_started_at_utc": observation["assignment_started_at_utc"],
            "target_total_sample": manifest["launch_contract"]["target_total_sample"],
            "sticky_assignment_verified": True,
            "commerce_unchanged": True,
        },
        "analysis_settings": {
            "verified_date": observation["observed_at_utc"][:10],
            "data_source_id": growthbook["data_source_id"],
            "data_source_name": "VEVO Production Experiment Facts",
            "assignment_query_name": "VEVO consented devices",
            "statistics_engine": "bayesian_default",
            "cuped_enabled": False,
            "post_stratification_enabled": False,
            "activation_metric": None,
            "goal_metrics": growthbook["goal_metrics"],
            "secondary_metrics": growthbook["secondary_metrics"],
            "guardrail_metrics": growthbook["guardrail_metrics"],
        },
    })
    return updated


def record_start(
    manifest: Mapping[str, Any],
    workspace: Mapping[str, Any],
    registry: Mapping[str, Any],
    observation: Mapping[str, Any],
    *,
    observation_sha256: str,
    source_hashes: Mapping[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_manifest(manifest)
    _require(manifest["status"] == REVIEW_OPEN, "CTA start cannot be recorded before reviewed release")
    for name in EXPECTED_PATHS:
        _validate_hash(source_hashes.get(name), f"{name} SHA-256")
        _require(
            source_hashes[name] == manifest["source_bindings"][name]["sha256"],
            f"{name} changed after CTA start review",
        )
    _validate_hash(observation_sha256, "CTA start observation SHA-256")
    _require(hashlib.sha256(canonical_json_bytes(observation)).hexdigest() == observation_sha256, "CTA start observation SHA-256 mismatch")
    validate_start_observation(observation, manifest)
    production = registry.get("environments", {}).get("production", {})
    _require(set(production) == {manifest["experiment_id"]}, "CTA start registry is not CTA-only")

    recorded = copy.deepcopy(manifest)
    recorded["status"] = RUNNING
    recorded["start_readback"].update({
        "status": "verified_running_production_cta",
        "observation_sha256": observation_sha256,
        "observed_at_utc": observation["observed_at_utc"],
        "assignment_started_at_utc": observation["assignment_started_at_utc"],
        "feature_revision": observation["growthbook"]["feature_revision"],
    })
    recorded["release_boundaries"]["manual_growthbook_start_allowed"] = False
    recorded["next_gate"] = "monitor_outcome_blind_first_n_and_safety_stop_conditions"
    validate_manifest(recorded)
    return recorded, _running_workspace(workspace, observation, recorded)


def validate_running_handoff(
    manifest: Mapping[str, Any],
    workspace: Mapping[str, Any],
    registry: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> None:
    """Validate the redundant checked-in state after the manual CTA start."""

    validate_manifest(manifest)
    _require(manifest["status"] == RUNNING, "CTA activation handoff is not running")
    validate_start_observation(observation, manifest)
    digest = hashlib.sha256(canonical_json_bytes(observation)).hexdigest()
    _require(digest == manifest["start_readback"]["observation_sha256"], "CTA start handoff observation SHA-256 drift")
    _require(
        workspace.get("state")
        == "production_cta_running_activation_verified_pro_quantiles_verified",
        "CTA running workspace state drift",
    )
    _require(workspace.get("workspace", {}).get("production_allocation_percent") == 100, "CTA running workspace allocation drift")
    _require(workspace.get("decision_gates", {}).get("production_activation_allowed") is True, "CTA running workspace gate is closed")
    experiments = {row.get("tracking_key"): row for row in workspace.get("experiments", []) if isinstance(row, dict)}
    _require(set(experiments) == {"vevo-sk-aa-001", "vevo-sk-product-cta-color-001"}, "CTA running workspace experiment set drift")
    aa = experiments["vevo-sk-aa-001"]
    cta = experiments["vevo-sk-product-cta-color-001"]
    _require(aa.get("status") == "stopped_production_aa_pass_verified", "A/A is not stopped in CTA handoff")
    _require(aa.get("production_allocation_percent") == 0, "A/A allocation is nonzero in CTA handoff")
    _require(cta.get("status") == "running_production_cta_only", "CTA workspace experiment is not running")
    _require(cta.get("production_allocation_percent") == 100, "CTA workspace allocation is not 100 percent")
    _require(cta.get("feature_rule_revision") == observation["growthbook"]["feature_revision"], "CTA workspace feature revision drift")
    evidence = _exact_object(
        cta.get("activation_evidence"),
        {"observation_sha256", "assignment_started_at_utc", "target_total_sample", "sticky_assignment_verified", "commerce_unchanged"},
        "CTA workspace activation_evidence",
    )
    _require(evidence == {
        "observation_sha256": digest,
        "assignment_started_at_utc": observation["assignment_started_at_utc"],
        "target_total_sample": manifest["launch_contract"]["target_total_sample"],
        "sticky_assignment_verified": True,
        "commerce_unchanged": True,
    }, "CTA workspace activation evidence drift")
    production = registry.get("environments", {}).get("production", {})
    preview = registry.get("environments", {}).get("preview", {})
    _require(set(production) == {manifest["experiment_id"]}, "CTA running collector registry is not CTA-only")
    _require(production[manifest["experiment_id"]] == preview.get(manifest["experiment_id"]), "CTA running collector contract differs from Preview")


def _load(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CtaActivationRecordingError(f"unable to load {field}") from exc
    _require(isinstance(value, dict), f"{field} must contain an object")
    return value


def _load_canonical(path: Path, expected_sha256: str, field: str) -> dict[str, Any]:
    value = _load(path, field)
    _require(path.read_bytes() == canonical_json_bytes(value), f"{field} is not canonical JSON")
    _require(hashlib.sha256(path.read_bytes()).hexdigest() == expected_sha256, f"{field} SHA-256 mismatch")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--activation", type=Path, default=DEFAULT_ACTIVATION_PATH)
    parser.add_argument("--output", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)

    open_parser = commands.add_parser("open-review")
    open_parser.add_argument("--completion", type=Path, default=DEFAULT_COMPLETION_PATH)
    open_parser.add_argument("--aa-snapshot", type=Path, default=DEFAULT_AA_SNAPSHOT_PATH)
    open_parser.add_argument("--pro-upgrade", type=Path, default=DEFAULT_PRO_UPGRADE_PATH)
    open_parser.add_argument(
        "--pro-observation", type=Path, default=DEFAULT_PRO_OBSERVATION_PATH
    )
    open_parser.add_argument("--sample-plan", type=Path, default=DEFAULT_SAMPLE_PLAN_PATH)
    open_parser.add_argument("--lifecycle", type=Path, default=DEFAULT_LIFECYCLE_PATH)
    open_parser.add_argument("--design", type=Path, default=DEFAULT_DESIGN_PATH)
    open_parser.add_argument("--decision", type=Path, default=DEFAULT_DECISION_PATH)
    open_parser.add_argument(
        "--meta-reporting", type=Path, default=DEFAULT_META_REPORTING_PATH
    )
    open_parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    open_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)
    open_parser.add_argument("--runtime-observation", type=Path, required=True)
    open_parser.add_argument("--runtime-observation-sha256", required=True)

    start_parser = commands.add_parser("record-start")
    start_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE_PATH)
    start_parser.add_argument("--workspace-output", type=Path, required=True)
    start_parser.add_argument("--completion", type=Path, default=DEFAULT_COMPLETION_PATH)
    start_parser.add_argument("--aa-snapshot", type=Path, default=DEFAULT_AA_SNAPSHOT_PATH)
    start_parser.add_argument("--pro-upgrade", type=Path, default=DEFAULT_PRO_UPGRADE_PATH)
    start_parser.add_argument(
        "--pro-observation", type=Path, default=DEFAULT_PRO_OBSERVATION_PATH
    )
    start_parser.add_argument("--sample-plan", type=Path, default=DEFAULT_SAMPLE_PLAN_PATH)
    start_parser.add_argument("--lifecycle", type=Path, default=DEFAULT_LIFECYCLE_PATH)
    start_parser.add_argument("--design", type=Path, default=DEFAULT_DESIGN_PATH)
    start_parser.add_argument("--decision", type=Path, default=DEFAULT_DECISION_PATH)
    start_parser.add_argument(
        "--meta-reporting", type=Path, default=DEFAULT_META_REPORTING_PATH
    )
    start_parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    start_parser.add_argument("--observation", type=Path, required=True)
    start_parser.add_argument("--observation-sha256", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        manifest = _load(args.activation, "CTA activation manifest")
        if args.command == "open-review":
            paths = {
                "aa_completion": args.completion,
                "aa_snapshot": args.aa_snapshot,
                "pro_upgrade": args.pro_upgrade,
                "pro_upgrade_observation": args.pro_observation,
                "sample_plan": args.sample_plan,
                "lifecycle_reconciliation": args.lifecycle,
                "design_contract": args.design,
                "decision_contract": args.decision,
                "meta_reporting_contract": args.meta_reporting,
                "collector_registry": args.registry,
            }
            runtime = _load_canonical(args.runtime_observation, args.runtime_observation_sha256, "CTA runtime observation")
            recorded = open_review(
                manifest,
                completion=_load(args.completion, "A/A completion"),
                snapshot=_load(args.aa_snapshot, "A/A snapshot manifest"),
                pro_upgrade=_load(args.pro_upgrade, "GrowthBook Pro manifest"),
                pro_observation=_load(
                    args.pro_observation, "GrowthBook Pro observation"
                ),
                sample=_load(args.sample_plan, "CTA sample plan"),
                lifecycle=_load(args.lifecycle, "CTA lifecycle manifest"),
                workspace=_load(args.workspace, "GrowthBook workspace"),
                registry=_load(args.registry, "collector registry"),
                runtime_observation=runtime,
                source_hashes={name: _file_sha256(path) for name, path in paths.items()},
                runtime_observation_sha256=args.runtime_observation_sha256,
            )
            _write_json(args.output, recorded)
            print("VEVO_CTA_START_REVIEW_OPENED:manual_growthbook_start=true:automatic_mutation=false")
            return 0

        _require(args.output.resolve() != args.workspace_output.resolve(), "activation and workspace output paths must differ")
        observation = _load_canonical(args.observation, args.observation_sha256, "CTA start observation")
        recorded, workspace = record_start(
            manifest,
            _load(args.workspace, "GrowthBook workspace"),
            _load(args.registry, "collector registry"),
            observation,
            observation_sha256=args.observation_sha256,
            source_hashes={
                "aa_completion": _file_sha256(args.completion),
                "aa_snapshot": _file_sha256(args.aa_snapshot),
                "pro_upgrade": _file_sha256(args.pro_upgrade),
                "pro_upgrade_observation": _file_sha256(args.pro_observation),
                "sample_plan": _file_sha256(args.sample_plan),
                "lifecycle_reconciliation": _file_sha256(args.lifecycle),
                "design_contract": _file_sha256(args.design),
                "decision_contract": _file_sha256(args.decision),
                "meta_reporting_contract": _file_sha256(args.meta_reporting),
                "collector_registry": _file_sha256(args.registry),
            },
        )
        _write_json(args.output, recorded)
        _write_json(args.workspace_output, workspace)
        print("VEVO_CTA_START_RECORDED:production_allocation=100:first_n_frozen=true:automatic_mutation=false")
        return 0
    except (
        CtaActivationRecordingError,
        pro_upgrade_recorder.ProUpgradeError,
        OSError,
        KeyError,
    ) as exc:
        print(f"record_growthbook_cta_activation.py: FAIL: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
