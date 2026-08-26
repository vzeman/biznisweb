#!/usr/bin/env python3
"""Evaluate a VEVO CTA safety-only checkpoint without primary outcome access."""

from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_safety_monitoring.json"
)

WAITING = "waiting_for_verified_cta_start"
MONITORING = "cta_running_safety_checkpoint_pending"
STOP_REVIEW = "cta_safety_stop_review_open"
STOPPED = "cta_safety_stop_recorded_followup_pending"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RUN_ID_RE = re.compile(r"^[1-9][0-9]*$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
PRICE_RE = re.compile(r"^[0-9]{1,3}(?: [0-9]{3})*,[0-9]{2} €$")
VARIATIONS = ("control", "brand_contrast")
VARIATION_FIELDS = {
    "eligible_devices",
    "measured_page_loads",
    "client_error_devices",
    "lcp_p75_ms",
    "inp_p75_ms",
    "cls_p75_milli",
}
THRESHOLDS = {
    "minimum_measured_page_loads_per_arm": 200,
    "lcp_degradation_absolute_ms": 200,
    "lcp_degradation_relative_percent": 10,
    "inp_degradation_absolute_ms": 20,
    "inp_degradation_relative_percent": 10,
    "cls_degradation_absolute_milli": 20,
    "client_error_rate_increase_max_percentage_points": 0.5,
}
FORBIDDEN_TOKENS = {
    "add_to_cart_devices",
    "purchase_devices",
    "conversion_rate",
    "revenue",
    "cm1",
    "average_order_value",
    "cancelled_order",
    "refunded_order",
    "meta_campaign",
    "meta_adset",
    "meta_ad",
    "meta_placement",
    "winner",
}


class CtaSafetyEvaluationError(ValueError):
    """Raised when a safety checkpoint is malformed or exceeds its remit."""


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaSafetyEvaluationError(message)


def _exact(value: Any, keys: set[str], label: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{label} must be an object")
    _require(set(value) == keys, f"{label} fields drift")
    return value


def _integer(value: Any, label: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0,
        f"{label} must be a non-negative integer",
    )
    return value


def _number(value: Any, label: str, *, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    _require(
        isinstance(value, (int, float)) and not isinstance(value, bool),
        f"{label} must be numeric",
    )
    numeric = float(value)
    _require(math.isfinite(numeric) and numeric >= 0, f"{label} must be finite")
    return numeric


def _validate_timestamp(value: Any, label: str) -> datetime:
    _require(
        isinstance(value, str)
        and len(value) == 20
        and value.startswith("20")
        and value.endswith("Z"),
        f"{label} must be canonical UTC seconds",
    )
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise CtaSafetyEvaluationError(f"{label} is invalid") from exc
    _require(
        parsed.isoformat(timespec="seconds").replace("+00:00", "Z") == value,
        f"{label} must be canonical UTC seconds",
    )
    return parsed


def _walk_forbidden(value: Any, path: tuple[str, ...] = ()) -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            key_text = str(key)
            lowered = key_text.lower()
            is_required_false_safety_marker = (
                path == ("safety",) and lowered == "winner_call_made"
            )
            _require(
                is_required_false_safety_marker
                or not any(token in lowered for token in FORBIDDEN_TOKENS),
                f"safety checkpoint contains forbidden outcome field: {key}",
            )
            _walk_forbidden(nested, (*path, key_text))
    elif isinstance(value, list):
        for nested in value:
            _walk_forbidden(nested, path)


def validate_contract(contract: Mapping[str, Any]) -> None:
    root = _exact(
        contract,
        {
            "schema_version",
            "monitoring_type",
            "status",
            "experiment_id",
            "assignment_started_at_utc",
            "source_bindings",
            "checkpoint_policy",
            "thresholds",
            "evidence_contract",
            "commerce_probe",
            "latest_checkpoint",
            "stop_handoff",
            "release_boundaries",
            "next_gate",
        },
        "CTA safety monitoring contract",
    )
    _require(
        root["schema_version"] == 1
        and root["monitoring_type"] == "vevo_growthbook_cta_safety_only",
        "CTA safety monitoring identity drift",
    )
    _require(
        root["status"] in {WAITING, MONITORING, STOP_REVIEW, STOPPED},
        "CTA safety monitoring status drift",
    )
    _require(
        root["experiment_id"] == "vevo-sk-product-cta-color-001",
        "CTA safety experiment drift",
    )
    sources = _exact(
        root["source_bindings"],
        {"activation", "start_observation", "decision_contract", "safety_query"},
        "CTA safety source bindings",
    )
    expected_sources = {
        "activation": "projects/vevo/growthbook_cta_activation.json",
        "start_observation": "projects/vevo/growthbook_cta_activation_observation.json",
        "decision_contract": "projects/vevo/growthbook_cta_decision_contract.json",
        "safety_query": "projects/vevo/growthbook_sql/cta_safety_checkpoint_production.sql",
    }
    for name, path in expected_sources.items():
        binding = _exact(sources[name], {"path", "sha256"}, f"source {name}")
        _require(binding["path"] == path, f"CTA safety source path drift: {name}")
        if name == "decision_contract":
            _require(
                binding["sha256"]
                == "62d9eb905a05b6273a7395905bc73f815e130155af1a32d896195facd442a07a",
                "CTA safety decision contract hash drift",
            )
        elif name == "safety_query":
            _require(
                binding["sha256"]
                == "b6be6c5a19c16a6b6e802b8c6dc83885458d63b8b0799e5f4eb98919e88c3adf",
                "CTA safety query hash drift",
            )
        elif root["status"] == WAITING:
            _require(binding["sha256"] is None, f"CTA safety source bound early: {name}")
        else:
            _require(
                SHA256_RE.fullmatch(str(binding["sha256"])) is not None,
                f"CTA safety source hash invalid: {name}",
            )
    policy = _exact(
        root["checkpoint_policy"],
        {
            "timezone",
            "cadence_hours",
            "first_checkpoint_after_start_hours",
            "maximum_checkpoint_lateness_minutes",
            "performance_requires_minimum_sample",
            "commerce_or_reproducible_runtime_error_stops_immediately",
            "primary_or_business_outcome_read_allowed",
        },
        "CTA safety checkpoint policy",
    )
    _require(
        policy
        == {
            "timezone": "Europe/Bratislava",
            "cadence_hours": 24,
            "first_checkpoint_after_start_hours": 24,
            "maximum_checkpoint_lateness_minutes": 60,
            "performance_requires_minimum_sample": True,
            "commerce_or_reproducible_runtime_error_stops_immediately": True,
            "primary_or_business_outcome_read_allowed": False,
        },
        "CTA safety checkpoint policy drift",
    )
    _require(root["thresholds"] == THRESHOLDS, "CTA safety thresholds drift")
    evidence = _exact(
        root["evidence_contract"],
        {
            "variation_keys",
            "variation_fields",
            "commerce_fields",
            "forbidden_metric_families",
            "contains_event_or_device_ids",
            "contains_customer_or_order_data",
            "contains_primary_or_business_outcomes",
        },
        "CTA safety evidence contract",
    )
    _require(evidence["variation_keys"] == list(VARIATIONS), "variation order drift")
    _require(
        evidence["variation_fields"]
        == [
            "eligible_devices",
            "measured_page_loads",
            "client_error_devices",
            "lcp_p75_ms",
            "inp_p75_ms",
            "cls_p75_milli",
        ],
        "CTA safety variation fields drift",
    )
    _require(
        evidence["commerce_fields"]
        == [
            "add_to_cart_text_unchanged",
            "price_unchanged",
            "cart_checkout_order_mutated",
            "reproducible_cart_or_checkout_runtime_error",
        ],
        "CTA safety commerce fields drift",
    )
    _require(
        evidence["forbidden_metric_families"]
        == [
            "add_to_cart",
            "purchase",
            "conversion",
            "revenue",
            "cm1",
            "average_order_value",
            "cancelled_order",
            "refunded_order",
            "meta_dimension",
            "winner",
        ],
        "CTA safety forbidden metric families drift",
    )
    _require(
        evidence["contains_event_or_device_ids"] is False
        and evidence["contains_customer_or_order_data"] is False
        and evidence["contains_primary_or_business_outcomes"] is False,
        "CTA safety evidence boundary opened",
    )
    commerce_probe = _exact(
        root["commerce_probe"],
        {"product_url", "product_code", "cart_url", "cta_text", "price_text"},
        "CTA safety commerce probe",
    )
    _require(
        commerce_probe["product_url"]
        == "https://www.vevo.sk/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute"
        and commerce_probe["product_code"] == "07500"
        and commerce_probe["cart_url"] == "https://www.vevo.sk/e/cart/index"
        and commerce_probe["cta_text"] == "Pridať do košíka",
        "CTA safety commerce probe target drift",
    )
    if root["status"] == WAITING:
        _require(
            commerce_probe["price_text"] is None,
            "CTA safety commerce price bound before verified start",
        )
    else:
        _require(
            PRICE_RE.fullmatch(str(commerce_probe["price_text"])) is not None,
            "CTA safety commerce price baseline invalid",
        )
    latest = _exact(
        root["latest_checkpoint"],
        {
            "status",
            "checkpoint_index",
            "observed_at_utc",
            "eligible_devices_seen",
            "evidence_sha256",
            "decision_sha256",
            "provenance_sha256",
            "workflow_run_id",
            "main_commit",
            "verdict",
            "stop_reasons",
        },
        "CTA safety latest checkpoint",
    )
    empty_latest = {
        "status": "not_recorded",
        "checkpoint_index": None,
        "observed_at_utc": None,
        "eligible_devices_seen": None,
        "evidence_sha256": None,
        "decision_sha256": None,
        "provenance_sha256": None,
        "workflow_run_id": None,
        "main_commit": None,
        "verdict": None,
        "stop_reasons": [],
    }
    handoff = _exact(
        root["stop_handoff"],
        {
            "status",
            "trigger_evidence_sha256",
            "trigger_decision_sha256",
            "trigger_provenance_sha256",
            "trigger_observed_at_utc",
            "stop_reasons",
            "stop_observation_sha256",
            "assignment_ended_at_utc",
        },
        "CTA safety stop handoff",
    )
    empty_handoff = {
        "status": "not_open",
        "trigger_evidence_sha256": None,
        "trigger_decision_sha256": None,
        "trigger_provenance_sha256": None,
        "trigger_observed_at_utc": None,
        "stop_reasons": [],
        "stop_observation_sha256": None,
        "assignment_ended_at_utc": None,
    }
    boundaries = _exact(
        root["release_boundaries"],
        {
            "safety_checkpoint_collection_allowed",
            "safety_checkpoint_recording_allowed",
            "protected_safety_collection_workflow_allowed",
            "manual_growthbook_stop_allowed",
            "automatic_growthbook_mutation_allowed",
            "automatic_gtm_mutation_allowed",
            "automatic_meta_ads_mutation_allowed",
            "automatic_biznisweb_mutation_allowed",
            "automatic_collector_or_reporting_mutation_allowed",
            "price_product_stock_cart_checkout_payment_or_order_mutation_allowed",
            "primary_or_business_outcome_read_allowed",
            "winner_calls_allowed",
        },
        "CTA safety release boundaries",
    )
    for field in (
        "automatic_growthbook_mutation_allowed",
        "automatic_gtm_mutation_allowed",
        "automatic_meta_ads_mutation_allowed",
        "automatic_biznisweb_mutation_allowed",
        "automatic_collector_or_reporting_mutation_allowed",
        "price_product_stock_cart_checkout_payment_or_order_mutation_allowed",
        "primary_or_business_outcome_read_allowed",
        "winner_calls_allowed",
    ):
        _require(boundaries[field] is False, f"CTA safety boundary opened: {field}")

    if root["status"] == WAITING:
        _require(root["assignment_started_at_utc"] is None, "CTA safety start bound early")
        _require(latest == empty_latest, "CTA safety checkpoint recorded early")
        _require(handoff == empty_handoff, "CTA safety stop handoff opened early")
        _require(not any(boundaries.values()), "CTA safety release boundary opened")
        _require(
            root["next_gate"] == "after_verified_cta_start_initialize_safety_monitoring",
            "CTA safety next gate drift",
        )
        return

    _validate_timestamp(root["assignment_started_at_utc"], "assignment_started_at_utc")
    _require(
        latest["status"] in {"not_recorded", "recorded"},
        "CTA safety latest checkpoint status drift",
    )
    if latest["status"] == "not_recorded":
        _require(latest == empty_latest, "CTA safety empty checkpoint drift")
    else:
        _require(
            isinstance(latest["checkpoint_index"], int)
            and not isinstance(latest["checkpoint_index"], bool)
            and latest["checkpoint_index"] >= 1,
            "CTA safety checkpoint index invalid",
        )
        _validate_timestamp(latest["observed_at_utc"], "latest_checkpoint.observed_at_utc")
        _require(
            isinstance(latest["eligible_devices_seen"], int)
            and not isinstance(latest["eligible_devices_seen"], bool)
            and latest["eligible_devices_seen"] >= 0,
            "CTA safety eligible-device total invalid",
        )
        for field in ("evidence_sha256", "decision_sha256", "provenance_sha256"):
            _require(
                SHA256_RE.fullmatch(str(latest[field])) is not None,
                f"CTA safety latest {field} invalid",
            )
        _require(
            RUN_ID_RE.fullmatch(str(latest["workflow_run_id"])) is not None,
            "CTA safety workflow run ID invalid",
        )
        _require(
            COMMIT_RE.fullmatch(str(latest["main_commit"])) is not None,
            "CTA safety main commit invalid",
        )
        _require(
            latest["verdict"] in {"CONTINUE", "CONTINUE_NOT_MATURE", "STOP_REQUIRED"},
            "CTA safety verdict drift",
        )
        _require(
            isinstance(latest["stop_reasons"], list)
            and all(isinstance(reason, str) and reason for reason in latest["stop_reasons"]),
            "CTA safety stop reasons drift",
        )
        _require(
            (latest["verdict"] == "STOP_REQUIRED") == bool(latest["stop_reasons"]),
            "CTA safety verdict/reason contradiction",
        )

    collection_fields = (
        "safety_checkpoint_collection_allowed",
        "safety_checkpoint_recording_allowed",
        "protected_safety_collection_workflow_allowed",
    )
    if root["status"] == MONITORING:
        _require(handoff == empty_handoff, "CTA safety monitoring stop handoff drift")
        _require(
            latest["verdict"] != "STOP_REQUIRED",
            "CTA safety breach did not open stop review",
        )
        _require(boundaries["manual_growthbook_stop_allowed"] is False, "CTA safety stop opened without breach")
        _require(
            all(boundaries[field] is True for field in collection_fields),
            "CTA safety collection/recording gate closed while monitoring",
        )
        _require(
            root["next_gate"] == "record_next_hash_bound_safety_checkpoint",
            "CTA safety monitoring next gate drift",
        )
        return

    _require(
        all(boundaries[field] is False for field in collection_fields),
        "CTA safety collection remains open after stop review",
    )
    if root["status"] == STOP_REVIEW:
        _require(latest["status"] == "recorded" and latest["verdict"] == "STOP_REQUIRED", "CTA safety stop review lacks STOP_REQUIRED")
        _require(
            handoff
            == {
                "status": "manual_stop_review_open",
                "trigger_evidence_sha256": latest["evidence_sha256"],
                "trigger_decision_sha256": latest["decision_sha256"],
                "trigger_provenance_sha256": latest["provenance_sha256"],
                "trigger_observed_at_utc": latest["observed_at_utc"],
                "stop_reasons": latest["stop_reasons"],
                "stop_observation_sha256": None,
                "assignment_ended_at_utc": None,
            },
            "CTA safety stop review handoff drift",
        )
        _require(boundaries["manual_growthbook_stop_allowed"] is True, "CTA safety manual stop gate closed")
        _require(root["next_gate"] == "manually_stop_only_exact_cta_then_record_canonical_readback", "CTA safety stop next gate drift")
        return

    _require(boundaries["manual_growthbook_stop_allowed"] is False, "CTA safety manual stop gate remains open")
    _require(handoff["status"] == "verified_manual_stop_readback", "CTA safety stop readback missing")
    for field in ("stop_observation_sha256",):
        _require(SHA256_RE.fullmatch(str(handoff[field])) is not None, f"CTA safety {field} invalid")
    ended_at = _validate_timestamp(
        handoff["assignment_ended_at_utc"],
        "stop_handoff.assignment_ended_at_utc",
    )
    _require(
        ended_at
        >= _validate_timestamp(
            root["assignment_started_at_utc"], "assignment_started_at_utc"
        ),
        "CTA safety stop predates assignment start",
    )
    if latest["verdict"] == "STOP_REQUIRED":
        _require(
            handoff["trigger_evidence_sha256"] == latest["evidence_sha256"]
            and handoff["trigger_decision_sha256"] == latest["decision_sha256"]
            and handoff["trigger_provenance_sha256"]
            == latest["provenance_sha256"]
            and handoff["trigger_observed_at_utc"] == latest["observed_at_utc"]
            and handoff["stop_reasons"] == latest["stop_reasons"],
            "CTA safety stopped trigger drift",
        )
        _require(
            ended_at
            >= _validate_timestamp(
                handoff["trigger_observed_at_utc"],
                "stop_handoff.trigger_observed_at_utc",
            ),
            "CTA safety stop predates safety trigger",
        )
    else:
        _require(
            handoff["trigger_evidence_sha256"] is None
            and handoff["trigger_decision_sha256"] is None
            and handoff["trigger_provenance_sha256"] is None
            and handoff["trigger_observed_at_utc"] is None
            and handoff["stop_reasons"] == [],
            "CTA non-safety stop gained a safety trigger",
        )
    _require(root["next_gate"] == "wait_exact_21_day_followup_then_one_protected_final_look", "CTA safety stopped next gate drift")


def validate_snapshot(snapshot: Mapping[str, Any]) -> None:
    _walk_forbidden(snapshot)
    root = _exact(
        snapshot,
        {
            "schema_version",
            "evidence_type",
            "experiment_id",
            "checkpoint_index",
            "assignment_started_at_utc",
            "observed_at_utc",
            "variation_health",
            "commerce_readback",
            "data_quality",
            "safety",
        },
        "CTA safety checkpoint",
    )
    _require(
        root["schema_version"] == 1
        and root["evidence_type"] == "vevo_growthbook_cta_safety_checkpoint",
        "CTA safety checkpoint identity drift",
    )
    _require(
        root["experiment_id"] == "vevo-sk-product-cta-color-001",
        "CTA safety checkpoint experiment drift",
    )
    _require(
        _integer(root["checkpoint_index"], "checkpoint_index") >= 1,
        "checkpoint_index must be positive",
    )
    assignment_started_at = _validate_timestamp(
        root["assignment_started_at_utc"], "assignment_started_at_utc"
    )
    observed_at = _validate_timestamp(root["observed_at_utc"], "observed_at_utc")
    _require(
        observed_at >= assignment_started_at,
        "CTA safety checkpoint predates assignment start",
    )
    variations = _exact(root["variation_health"], set(VARIATIONS), "variation_health")
    for variation in VARIATIONS:
        row = _exact(variations[variation], VARIATION_FIELDS, variation)
        eligible = _integer(row["eligible_devices"], f"{variation}.eligible_devices")
        measured = _integer(
            row["measured_page_loads"], f"{variation}.measured_page_loads"
        )
        errors = _integer(
            row["client_error_devices"], f"{variation}.client_error_devices"
        )
        _require(errors <= eligible, f"{variation} client errors exceed devices")
        _require(
            eligible > 0 or measured == 0,
            f"{variation} measured page loads require eligible devices",
        )
        for field in ("lcp_p75_ms", "inp_p75_ms", "cls_p75_milli"):
            _number(row[field], f"{variation}.{field}", nullable=True)
    commerce = _exact(
        root["commerce_readback"],
        {
            "add_to_cart_text_unchanged",
            "price_unchanged",
            "cart_checkout_order_mutated",
            "reproducible_cart_or_checkout_runtime_error",
        },
        "commerce_readback",
    )
    _require(
        all(isinstance(value, bool) for value in commerce.values()),
        "CTA safety commerce values must be booleans",
    )
    quality = _exact(
        root["data_quality"],
        {
            "query_complete",
            "exact_two_variations",
            "assignment_source_match",
            "duplicate_or_conflicting_assignment_detected",
        },
        "data_quality",
    )
    _require(
        all(isinstance(value, bool) for value in quality.values()),
        "CTA safety data quality values must be booleans",
    )
    safety = _exact(
        root["safety"],
        {
            "contains_event_or_device_ids",
            "contains_customer_or_order_data",
            "primary_metric_read",
            "business_outcome_read",
            "meta_dimensions_read",
            "winner_call_made",
            "external_or_automatic_mutation",
        },
        "safety",
    )
    _require(not any(safety.values()), "CTA safety checkpoint exceeded read boundary")


def evaluate(
    snapshot: Mapping[str, Any], contract: Mapping[str, Any]
) -> dict[str, Any]:
    validate_contract(contract)
    validate_snapshot(snapshot)
    thresholds = contract["thresholds"]
    control = snapshot["variation_health"]["control"]
    variant = snapshot["variation_health"]["brand_contrast"]
    performance_fields = ("lcp_p75_ms", "inp_p75_ms", "cls_p75_milli")
    for name, row in (("control", control), ("brand_contrast", variant)):
        if (
            row["measured_page_loads"]
            >= thresholds["minimum_measured_page_loads_per_arm"]
        ):
            _require(
                all(row[field] is not None for field in performance_fields),
                f"{name} mature performance evidence is incomplete",
            )
    performance_mature = all(
        row["measured_page_loads"]
        >= thresholds["minimum_measured_page_loads_per_arm"]
        for row in (control, variant)
    )

    deltas: dict[str, float | None] = {
        "lcp_increase_ms": None,
        "inp_increase_ms": None,
        "cls_increase_milli": None,
        "client_error_rate_increase_percentage_points": None,
    }
    reasons: list[str] = []
    quality = snapshot["data_quality"]
    if not quality["query_complete"]:
        reasons.append("query_incomplete")
    if not quality["exact_two_variations"]:
        reasons.append("variation_set_invalid")
    if not quality["assignment_source_match"]:
        reasons.append("assignment_source_mismatch")
    if quality["duplicate_or_conflicting_assignment_detected"]:
        reasons.append("duplicate_or_conflicting_assignment")
    commerce = snapshot["commerce_readback"]
    if not commerce["add_to_cart_text_unchanged"]:
        reasons.append("add_to_cart_text_changed")
    if not commerce["price_unchanged"]:
        reasons.append("price_changed")
    if commerce["cart_checkout_order_mutated"]:
        reasons.append("cart_checkout_or_order_mutated")
    if commerce["reproducible_cart_or_checkout_runtime_error"]:
        reasons.append("reproducible_cart_or_checkout_runtime_error")

    if performance_mature:
        deltas["lcp_increase_ms"] = round(
            float(variant["lcp_p75_ms"]) - float(control["lcp_p75_ms"]), 6
        )
        deltas["inp_increase_ms"] = round(
            float(variant["inp_p75_ms"]) - float(control["inp_p75_ms"]), 6
        )
        deltas["cls_increase_milli"] = round(
            float(variant["cls_p75_milli"])
            - float(control["cls_p75_milli"]),
            6,
        )
        control_rate = (
            control["client_error_devices"] / control["eligible_devices"] * 100
            if control["eligible_devices"]
            else 0.0
        )
        variant_rate = (
            variant["client_error_devices"] / variant["eligible_devices"] * 100
            if variant["eligible_devices"]
            else 0.0
        )
        deltas["client_error_rate_increase_percentage_points"] = round(
            variant_rate - control_rate,
            6,
        )
        lcp_limit = max(
            thresholds["lcp_degradation_absolute_ms"],
            float(control["lcp_p75_ms"])
            * thresholds["lcp_degradation_relative_percent"]
            / 100,
        )
        inp_limit = max(
            thresholds["inp_degradation_absolute_ms"],
            float(control["inp_p75_ms"])
            * thresholds["inp_degradation_relative_percent"]
            / 100,
        )
        if deltas["lcp_increase_ms"] > lcp_limit:
            reasons.append("lcp_regression")
        if deltas["inp_increase_ms"] > inp_limit:
            reasons.append("inp_regression")
        if (
            deltas["cls_increase_milli"]
            > thresholds["cls_degradation_absolute_milli"]
        ):
            reasons.append("cls_regression")
        if (
            deltas["client_error_rate_increase_percentage_points"]
            > thresholds["client_error_rate_increase_max_percentage_points"]
        ):
            reasons.append("client_error_rate_regression")

    verdict = (
        "STOP_REQUIRED"
        if reasons
        else "CONTINUE"
        if performance_mature
        else "CONTINUE_NOT_MATURE"
    )
    return {
        "schema_version": 1,
        "decision_type": "vevo_growthbook_cta_safety_only_decision",
        "experiment_id": snapshot["experiment_id"],
        "checkpoint_index": snapshot["checkpoint_index"],
        "observed_at_utc": snapshot["observed_at_utc"],
        "verdict": verdict,
        "manual_stop_required": verdict == "STOP_REQUIRED",
        "performance_mature": performance_mature,
        "stop_reasons": reasons,
        "deltas": deltas,
        "thresholds": dict(thresholds),
        "safety": {
            "primary_metric_read": False,
            "business_outcome_read": False,
            "meta_dimensions_read": False,
            "winner_call_made": False,
            "automatic_or_external_mutation": False,
        },
    }


def _load(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CtaSafetyEvaluationError(f"{label} is unreadable") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT_PATH)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        decision = evaluate(
            _load(args.snapshot, "CTA safety snapshot"),
            _load(args.contract, "CTA safety contract"),
        )
        args.output.write_bytes(canonical_json_bytes(decision))
    except (CtaSafetyEvaluationError, OSError) as exc:
        print(f"evaluate_growthbook_cta_safety.py: FAIL: {exc}")
        return 2
    print(
        "VEVO_CTA_SAFETY_EVALUATED:"
        f"verdict={decision['verdict']}:primary=false:winner=false:automatic=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
