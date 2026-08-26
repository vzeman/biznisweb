#!/usr/bin/env python3
"""Build the one protected VEVO CTA final snapshot from aggregate Athena rows.

The module is deliberately offline. It validates the checked-in final-look gate,
renders one frozen aggregate query, reduces its two identity-free result rows,
and emits the exact snapshot accepted by ``evaluate_growthbook_cta.py``. It has
no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, browser, or network client.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import re
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from evaluate_growthbook_cta import (
        CtaEvaluationError,
        canonical_json_bytes,
        validate_contract,
        validate_snapshot,
    )
    from freeze_growthbook_cta_sample import (
        CtaSampleFreezeError,
        canonical_json_bytes as canonical_sample_bytes,
        validate_plan,
    )
except ModuleNotFoundError:  # Imported as scripts.build_growthbook_cta_final_snapshot.
    from scripts.evaluate_growthbook_cta import (
        CtaEvaluationError,
        canonical_json_bytes,
        validate_contract,
        validate_snapshot,
    )
    from scripts.freeze_growthbook_cta_sample import (
        CtaSampleFreezeError,
        canonical_json_bytes as canonical_sample_bytes,
        validate_plan,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_final_snapshot.json"
)
DEFAULT_COMPLETION_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_completion.json"
DEFAULT_ACTIVATION_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_activation.json"
DEFAULT_MEASUREMENT_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_measurement_window.json"
)
DEFAULT_SAMPLE_PLAN_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_sample_plan.json"
DEFAULT_DECISION_CONTRACT_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_decision_contract.json"
)
DEFAULT_LIFECYCLE_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_lifecycle_reconciliation.json"
)
DEFAULT_STOP_OBSERVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_assignment_stop_observation.json"
)

WAITING = "waiting_for_verified_cta_stop_and_followup"
FOLLOWUP = "followup_pending_final_look_locked_until_due"
RECORDED = "final_snapshot_recorded_manual_action_pending"
EXPERIMENT_ID = "vevo-sk-product-cta-color-001"
METRIC_VERSION = "vevo_cm1_v1_2026-08-20"
UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")

MANIFEST_KEYS = {
    "schema_version",
    "producer_type",
    "experiment_id",
    "status",
    "workflow",
    "workflow_name",
    "source_bindings",
    "final_look",
    "query_contract",
    "runtime_gate",
    "output",
    "release_boundaries",
    "next_gate",
}
BINDING_KEYS = {
    "completion_path",
    "completion_sha256",
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
    "stop_observation_path",
    "stop_observation_sha256",
}
FINAL_LOOK_KEYS = {
    "timezone",
    "required_followup_days",
    "assignment_started_at_utc",
    "assignment_ended_at_utc",
    "snapshot_due_utc",
    "target_total_sample",
    "eligible_devices_seen_before_stop",
    "one_final_look_only",
    "protected_workflow_allowed",
    "successful_run_id",
    "main_commit",
    "snapshot_sha256",
    "decision_sha256",
    "provenance_sha256",
    "hypothesis_registry_sha256",
    "verdict",
    "recommended_variation",
}
QUERY_KEYS = {
    "template_path",
    "template_sha256",
    "database",
    "workgroup",
    "source_tables",
    "metric_contract_version",
    "selection_method",
    "result_variations",
    "aggregate_rows_only",
    "identity_columns_in_result_allowed",
}
RUNTIME_KEYS = {
    "instance_id",
    "service",
    "runtime_path",
    "successful_scheduled_reconciliation_required",
    "localhost_health_marker_required",
    "localhost_runtime_marker_required",
    "generated_published_parity_required",
    "alarms_clear_required",
    "dlq_empty_required",
}
OUTPUT_KEYS = {
    "artifact_name",
    "snapshot_file_name",
    "decision_file_name",
    "provenance_file_name",
    "retention_days",
    "canonical_json_required",
    "contains_raw_aws_payloads",
    "contains_event_or_device_ids",
    "contains_customer_or_order_data",
}
BOUNDARY_KEYS = {
    "main_only",
    "diagnostic_host_gate_task_allowed",
    "aws_aggregate_reads_allowed",
    "outcome_metrics_read_allowed",
    "automatic_growthbook_mutation_allowed",
    "automatic_gtm_mutation_allowed",
    "automatic_meta_ads_mutation_allowed",
    "automatic_biznisweb_mutation_allowed",
    "automatic_collector_or_reporting_mutation_allowed",
    "price_product_cart_checkout_order_mutation_allowed",
    "automatic_winner_application_allowed",
}
RESULT_COLUMNS = [
    "variation_id",
    "eligible_devices",
    "add_to_cart_devices",
    "purchase_devices",
    "joined_order_count",
    "net_revenue_sum_eur",
    "net_revenue_sum_squares_eur2",
    "cm1_sum_eur",
    "cm1_sum_squares_eur2",
    "cancelled_order_count",
    "refunded_order_count",
    "immature_order_count",
    "client_error_devices",
    "unmatched_transaction_count",
    "ambiguous_transaction_count",
    "measured_page_loads",
    "lcp_p75_ms",
    "inp_p75_ms",
    "cls_p75_milli",
    "reporting_device_count",
    "eligible_devices_seen_before_stop",
    "raw_event_count",
    "unique_event_count",
    "contaminated_device_count",
    "pii_finding_count",
    "full_url_stored_count",
    "click_identifier_stored_count",
    "non_analytical_consent_exposure_count",
]
INTEGER_COLUMNS = {
    "eligible_devices",
    "add_to_cart_devices",
    "purchase_devices",
    "joined_order_count",
    "cancelled_order_count",
    "refunded_order_count",
    "immature_order_count",
    "client_error_devices",
    "unmatched_transaction_count",
    "ambiguous_transaction_count",
    "measured_page_loads",
    "reporting_device_count",
    "eligible_devices_seen_before_stop",
    "raw_event_count",
    "unique_event_count",
    "contaminated_device_count",
    "pii_finding_count",
    "full_url_stored_count",
    "click_identifier_stored_count",
    "non_analytical_consent_exposure_count",
}
NUMBER_COLUMNS = {
    "net_revenue_sum_eur",
    "net_revenue_sum_squares_eur2",
    "cm1_sum_eur",
    "cm1_sum_squares_eur2",
    "lcp_p75_ms",
    "inp_p75_ms",
    "cls_p75_milli",
}


class CtaFinalSnapshotError(ValueError):
    """Raised when the protected final-look contract fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaFinalSnapshotError(message)


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
        raise CtaFinalSnapshotError(f"{field} is invalid") from exc
    _require(parsed.tzinfo == UTC and parsed.microsecond == 0, f"{field} is invalid")
    return parsed


def _load(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CtaFinalSnapshotError(f"{field} is unreadable") from exc
    _require(isinstance(value, dict), f"{field} must contain an object")
    return value


def _load_bytes(body: bytes, field: str) -> dict[str, Any]:
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CtaFinalSnapshotError(f"{field} bytes are unreadable") from exc
    _require(isinstance(value, dict), f"{field} bytes must contain an object")
    return value


def _repo_path(value: Any, field: str) -> Path:
    _require(isinstance(value, str) and value, f"{field} must be a repository path")
    candidate = (ROOT / value).resolve()
    _require(ROOT.resolve() in candidate.parents, f"{field} escapes the repository")
    _require(candidate.is_file(), f"{field} does not exist")
    return candidate


def _hash_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_query_contract(query: Mapping[str, Any]) -> None:
    _exact(query, QUERY_KEYS, "CTA final query contract")
    _require(
        query
        == {
            "template_path": "projects/vevo/growthbook_sql/cta_final_snapshot_production.sql",
            "template_sha256": query["template_sha256"],
            "database": "vevo_growthbook_production",
            "workgroup": "vevo-growthbook-reporting-production",
            "source_tables": [
                "experiment_events_raw",
                "experiment_device_facts",
                "experiment_performance_facts",
            ],
            "metric_contract_version": METRIC_VERSION,
            "selection_method": "first_n_eligible_devices_ordered_by_first_exposure_then_device_id",
            "result_variations": ["control", "brand_contrast"],
            "aggregate_rows_only": True,
            "identity_columns_in_result_allowed": False,
        },
        "CTA final query contract drift",
    )
    _require(
        SHA256_RE.fullmatch(str(query["template_sha256"])) is not None,
        "CTA final SQL hash is invalid",
    )
    path = _repo_path(query["template_path"], "query.template_path")
    _require(_hash_path(path) == query["template_sha256"], "CTA final SQL hash mismatch")
    sql = path.read_text(encoding="utf-8")
    for marker in (
        "ROW_NUMBER() OVER",
        "ORDER BY from_iso8601_timestamp(first_exposure_at), device_id",
        "sample_ordinal <= __TARGET_TOTAL_SAMPLE__",
        "experiment_id = 'vevo-sk-product-cta-color-001'",
        "eligible = 1",
        "contaminated = 0",
        "GROUP BY variation_id",
        "ORDER BY outcomes.variation_id",
    ):
        _require(marker in sql, f"CTA final SQL marker missing: {marker}")


def validate_manifest(
    manifest: Mapping[str, Any],
    *,
    completion_path: Path = DEFAULT_COMPLETION_PATH,
    activation_path: Path = DEFAULT_ACTIVATION_PATH,
    measurement_path: Path = DEFAULT_MEASUREMENT_PATH,
    sample_plan_path: Path = DEFAULT_SAMPLE_PLAN_PATH,
    decision_contract_path: Path = DEFAULT_DECISION_CONTRACT_PATH,
    lifecycle_path: Path = DEFAULT_LIFECYCLE_PATH,
    stop_observation_path: Path = DEFAULT_STOP_OBSERVATION_PATH,
    source_bytes: Mapping[str, bytes] | None = None,
) -> None:
    root = _exact(manifest, MANIFEST_KEYS, "CTA final snapshot manifest")
    _require(root["schema_version"] == 1, "CTA final snapshot schema drift")
    _require(
        root["producer_type"] == "vevo_growthbook_cta_protected_final_snapshot",
        "CTA final producer drift",
    )
    _require(root["experiment_id"] == EXPERIMENT_ID, "CTA final experiment drift")
    _require(root["status"] in {WAITING, FOLLOWUP, RECORDED}, "CTA final status drift")
    _require(
        root["workflow"]
        == ".github/workflows/build-vevo-growthbook-production-cta-final-snapshot.yml",
        "CTA final workflow drift",
    )
    _require(
        root["workflow_name"] == "Build VEVO GrowthBook Production CTA Final Snapshot",
        "CTA final workflow name drift",
    )
    bindings = _exact(root["source_bindings"], BINDING_KEYS, "CTA final source bindings")
    expected_paths = {
        "completion_path": "projects/vevo/growthbook_cta_completion.json",
        "activation_path": "projects/vevo/growthbook_cta_activation.json",
        "measurement_window_path": "projects/vevo/growthbook_cta_measurement_window.json",
        "sample_plan_path": "projects/vevo/growthbook_cta_sample_plan.json",
        "decision_contract_path": "projects/vevo/growthbook_cta_decision_contract.json",
        "lifecycle_reconciliation_path": "projects/vevo/growthbook_cta_lifecycle_reconciliation.json",
        "stop_observation_path": "projects/vevo/growthbook_cta_assignment_stop_observation.json",
    }
    for field, expected in expected_paths.items():
        _require(bindings[field] == expected, f"CTA final {field} drift")
    _require(
        bindings["decision_contract_sha256"] == _hash_path(decision_contract_path),
        "CTA final decision contract hash drift",
    )
    _validate_query_contract(root["query_contract"])
    runtime = _exact(root["runtime_gate"], RUNTIME_KEYS, "CTA final runtime gate")
    _require(
        runtime
        == {
            "instance_id": "N/A:Fargate",
            "service": "vevo-growthbook-reconcile-production",
            "runtime_path": "/app",
            "successful_scheduled_reconciliation_required": True,
            "localhost_health_marker_required": True,
            "localhost_runtime_marker_required": True,
            "generated_published_parity_required": True,
            "alarms_clear_required": True,
            "dlq_empty_required": True,
        },
        "CTA final runtime gate drift",
    )
    output = _exact(root["output"], OUTPUT_KEYS, "CTA final output")
    _require(
        output
        == {
            "artifact_name": "vevo-growthbook-cta-final-snapshot",
            "snapshot_file_name": "vevo-growthbook-cta-final-snapshot.json",
            "decision_file_name": "vevo-growthbook-cta-final-decision.json",
            "provenance_file_name": "vevo-growthbook-cta-final-provenance.json",
            "retention_days": 90,
            "canonical_json_required": True,
            "contains_raw_aws_payloads": False,
            "contains_event_or_device_ids": False,
            "contains_customer_or_order_data": False,
        },
        "CTA final output drift",
    )
    boundaries = _exact(root["release_boundaries"], BOUNDARY_KEYS, "CTA final boundaries")
    _require(boundaries["main_only"] is True, "CTA final main-only boundary drift")
    for field in BOUNDARY_KEYS - {
        "main_only",
        "diagnostic_host_gate_task_allowed",
        "aws_aggregate_reads_allowed",
        "outcome_metrics_read_allowed",
    }:
        _require(boundaries[field] is False, f"CTA final mutation boundary opened: {field}")
    final = _exact(root["final_look"], FINAL_LOOK_KEYS, "CTA final look")
    _require(final["timezone"] == "Europe/Bratislava", "CTA final timezone drift")
    _require(final["required_followup_days"] == 14, "CTA final follow-up drift")
    _require(final["one_final_look_only"] is True, "CTA final one-look rule drift")

    if root["status"] == WAITING:
        nullable = FINAL_LOOK_KEYS - {
            "timezone",
            "required_followup_days",
            "one_final_look_only",
            "protected_workflow_allowed",
        }
        _require(all(final[field] is None for field in nullable), "CTA final waiting look is populated")
        _require(final["protected_workflow_allowed"] is False, "CTA final workflow opened early")
        dynamic_hashes = BINDING_KEYS - {
            "completion_path",
            "activation_path",
            "measurement_window_path",
            "sample_plan_path",
            "decision_contract_path",
            "decision_contract_sha256",
            "lifecycle_reconciliation_path",
            "stop_observation_path",
        }
        _require(
            all(bindings[field] is None for field in dynamic_hashes),
            "CTA final waiting source hashes are populated",
        )
        _require(
            boundaries["aws_aggregate_reads_allowed"] is False
            and boundaries["diagnostic_host_gate_task_allowed"] is False
            and boundaries["outcome_metrics_read_allowed"] is False,
            "CTA final outcome reads opened early",
        )
        _require(
            root["next_gate"]
            == "after_verified_manual_cta_stop_wait_exact_14_day_followup_then_open_one_protected_final_look",
            "CTA final waiting next gate drift",
        )
        return

    source_paths = {
        "completion_sha256": completion_path,
        "activation_sha256": activation_path,
        "measurement_window_sha256": measurement_path,
        "sample_plan_sha256": sample_plan_path,
        "decision_contract_sha256": decision_contract_path,
        "lifecycle_reconciliation_sha256": lifecycle_path,
        "stop_observation_sha256": stop_observation_path,
    }
    byte_keys = {
        "completion_sha256": "completion",
        "activation_sha256": "activation",
        "measurement_window_sha256": "measurement_window",
        "sample_plan_sha256": "sample_plan",
        "lifecycle_reconciliation_sha256": "lifecycle_reconciliation",
        "stop_observation_sha256": "stop_observation",
    }
    supplied = dict(source_bytes or {})
    if supplied:
        _require(set(supplied) == set(byte_keys.values()), "CTA final source byte set drift")
    resolved_bytes: dict[str, bytes] = {}
    for field, path in source_paths.items():
        byte_key = byte_keys.get(field)
        if byte_key and supplied:
            body = supplied[byte_key]
            _require(isinstance(body, bytes), f"CTA final source bytes are invalid: {byte_key}")
        else:
            _require(path.is_file(), f"CTA final source missing for {field}")
            body = path.read_bytes()
        resolved_bytes[field] = body
        _require(
            bindings[field] == hashlib.sha256(body).hexdigest(),
            f"CTA final source hash drift: {field}",
        )
    completion = _load_bytes(resolved_bytes["completion_sha256"], "CTA completion")
    activation = _load_bytes(resolved_bytes["activation_sha256"], "CTA activation")
    measurement = _load_bytes(
        resolved_bytes["measurement_window_sha256"], "CTA measurement"
    )
    sample = _load_bytes(resolved_bytes["sample_plan_sha256"], "CTA sample plan")
    contract = _load(decision_contract_path, "CTA decision contract")
    lifecycle = _load_bytes(
        resolved_bytes["lifecycle_reconciliation_sha256"],
        "CTA lifecycle reconciliation",
    )
    stop = _load_bytes(
        resolved_bytes["stop_observation_sha256"], "CTA stop observation"
    )
    try:
        validate_plan(sample)
        validate_contract(contract)
    except (CtaSampleFreezeError, CtaEvaluationError) as exc:
        raise CtaFinalSnapshotError(str(exc)) from exc
    _require(completion.get("status") == "cta_assignment_stopped_verified_followup_pending", "CTA completion is not stopped")
    _require(
        activation.get("status") == "production_cta_start_recorded_assignment_stopped_verified",
        "CTA activation stop state drift",
    )
    _require(
        measurement.get("status") == "cta_assignment_stopped_verified_followup_pending",
        "CTA measurement stop state drift",
    )
    _require(sample.get("status") == "sample_frozen_activation_still_blocked", "CTA sample is not frozen")
    _require(lifecycle.get("verified") is True, "CTA lifecycle reconciliation is not verified")
    followup = completion.get("followup") or {}
    started = activation.get("start_readback", {}).get("assignment_started_at_utc")
    ended = completion.get("stop_readback", {}).get("assignment_ended_at_utc")
    due = followup.get("final_snapshot_due_utc")
    _require(
        final["assignment_started_at_utc"] == started
        and final["assignment_ended_at_utc"] == ended
        and final["snapshot_due_utc"] == due,
        "CTA final timing binding drift",
    )
    _require(
        _parse_utc(due, "final_look.snapshot_due_utc")
        == _parse_utc(ended, "final_look.assignment_ended_at_utc") + timedelta(days=14),
        "CTA final due time is not stop plus 14 days",
    )
    target = sample.get("final", {}).get("total_sample")
    seen = measurement.get("measurement_window", {}).get("resolved_eligible_devices")
    _require(type(target) is int and target >= 2, "CTA final target sample is invalid")
    _require(type(seen) is int and seen >= 1, "CTA final eligible count is invalid")
    _require(
        final["target_total_sample"] == target
        and final["eligible_devices_seen_before_stop"] == seen,
        "CTA final cohort binding drift",
    )
    _require(stop.get("assignment_ended_at_utc") == ended, "CTA final stop observation timing drift")

    if root["status"] == FOLLOWUP:
        _require(final["protected_workflow_allowed"] is True, "CTA final workflow gate is closed")
        for field in (
            "successful_run_id",
            "main_commit",
            "snapshot_sha256",
            "decision_sha256",
            "provenance_sha256",
            "hypothesis_registry_sha256",
            "verdict",
            "recommended_variation",
        ):
            _require(final[field] is None, f"CTA final pending field is already recorded: {field}")
        _require(
            boundaries["aws_aggregate_reads_allowed"] is True
            and boundaries["diagnostic_host_gate_task_allowed"] is True
            and boundaries["outcome_metrics_read_allowed"] is True,
            "CTA final protected reads are not open",
        )
        _require(
            root["next_gate"]
            == "at_or_after_snapshot_due_run_exactly_one_main_only_protected_final_snapshot",
            "CTA final follow-up next gate drift",
        )
        return

    _require(final["protected_workflow_allowed"] is False, "CTA final workflow remains open")
    _require(RUN_ID_RE.fullmatch(str(final["successful_run_id"] or "")) is not None, "CTA final run ID is invalid")
    _require(COMMIT_RE.fullmatch(str(final["main_commit"] or "")) is not None, "CTA final main commit is invalid")
    for field in (
        "snapshot_sha256",
        "decision_sha256",
        "provenance_sha256",
        "hypothesis_registry_sha256",
    ):
        _require(SHA256_RE.fullmatch(str(final[field] or "")) is not None, f"CTA final {field} is invalid")
    _require(final["verdict"] in {"WIN", "LOSE", "INCONCLUSIVE"}, "CTA final verdict drift")
    _require(final["recommended_variation"] in {"control", "brand_contrast"}, "CTA final recommendation drift")
    _require(
        boundaries["aws_aggregate_reads_allowed"] is False
        and boundaries["diagnostic_host_gate_task_allowed"] is False
        and boundaries["outcome_metrics_read_allowed"] is False,
        "CTA final reads remain open after recording",
    )
    _require(
        root["next_gate"] == "manual_review_decision_before_any_external_mutation",
        "CTA final recorded next gate drift",
    )


def render_query(
    manifest: Mapping[str, Any],
    *,
    completion_path: Path = DEFAULT_COMPLETION_PATH,
    activation_path: Path = DEFAULT_ACTIVATION_PATH,
    measurement_path: Path = DEFAULT_MEASUREMENT_PATH,
    sample_plan_path: Path = DEFAULT_SAMPLE_PLAN_PATH,
    decision_contract_path: Path = DEFAULT_DECISION_CONTRACT_PATH,
    lifecycle_path: Path = DEFAULT_LIFECYCLE_PATH,
    stop_observation_path: Path = DEFAULT_STOP_OBSERVATION_PATH,
) -> str:
    validate_manifest(
        manifest,
        completion_path=completion_path,
        activation_path=activation_path,
        measurement_path=measurement_path,
        sample_plan_path=sample_plan_path,
        decision_contract_path=decision_contract_path,
        lifecycle_path=lifecycle_path,
        stop_observation_path=stop_observation_path,
    )
    _require(manifest["status"] == FOLLOWUP, "CTA final query gate is not open")
    final = manifest["final_look"]
    started = _parse_utc(final["assignment_started_at_utc"], "assignment_started_at_utc")
    ended = _parse_utc(final["assignment_ended_at_utc"], "assignment_ended_at_utc")
    due = _parse_utc(final["snapshot_due_utc"], "snapshot_due_utc")
    replacements = {
        "__CTA_STARTED_AT_UTC__": final["assignment_started_at_utc"],
        "__CTA_ENDED_AT_UTC__": final["assignment_ended_at_utc"],
        "__FOLLOWUP_THROUGH_UTC__": final["snapshot_due_utc"],
        "__CTA_START_DATE__": started.date().isoformat(),
        "__FOLLOWUP_LAST_DATE__": (due - timedelta(seconds=1)).date().isoformat(),
        "__TARGET_TOTAL_SAMPLE__": str(final["target_total_sample"]),
    }
    _require(started < ended < due, "CTA final render timing drift")
    query = _repo_path(
        manifest["query_contract"]["template_path"], "query.template_path"
    ).read_text(encoding="utf-8")
    for marker, value in replacements.items():
        _require(query.count(marker) >= 1, f"CTA final placeholder missing: {marker}")
        query = query.replace(marker, value)
    _require("__CTA_" not in query and "__FOLLOWUP_" not in query and "__TARGET_" not in query, "CTA final SQL has unresolved placeholders")
    return query


def _cell(row: Mapping[str, Any], index: int) -> str | None:
    data = row.get("Data")
    _require(isinstance(data, list) and len(data) == len(RESULT_COLUMNS), "CTA final Athena row width drift")
    cell = data[index]
    _require(isinstance(cell, Mapping), "CTA final Athena cell drift")
    value = cell.get("VarCharValue")
    _require(value is None or isinstance(value, str), "CTA final Athena value drift")
    return value


def _integer(value: str | None, field: str) -> int:
    _require(value is not None and re.fullmatch(r"[0-9]+", value) is not None, f"{field} must be a non-negative integer")
    return int(value)


def _number(value: str | None, field: str, *, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    _require(value is not None, f"{field} is missing")
    try:
        number = float(value)
    except ValueError as exc:
        raise CtaFinalSnapshotError(f"{field} must be numeric") from exc
    _require(math.isfinite(number), f"{field} must be finite")
    return number


def parse_athena_results(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    _require(not payload.get("NextToken"), "CTA final Athena result is paginated")
    result_set = payload.get("ResultSet")
    _require(isinstance(result_set, Mapping), "CTA final Athena result set is missing")
    rows = result_set.get("Rows")
    _require(isinstance(rows, list) and len(rows) == 3, "CTA final Athena result must contain header plus two rows")
    header = [_cell(rows[0], index) for index in range(len(RESULT_COLUMNS))]
    _require(header == RESULT_COLUMNS, "CTA final Athena header drift")
    parsed: dict[str, dict[str, Any]] = {}
    for row_index, row in enumerate(rows[1:], start=1):
        values = {
            field: _cell(row, index) for index, field in enumerate(RESULT_COLUMNS)
        }
        variation = values["variation_id"]
        _require(variation in {"control", "brand_contrast"}, "CTA final variation drift")
        _require(variation not in parsed, "CTA final duplicate variation row")
        converted: dict[str, Any] = {"variation_id": variation}
        for field in INTEGER_COLUMNS:
            converted[field] = _integer(values[field], f"row[{row_index}].{field}")
        for field in NUMBER_COLUMNS:
            converted[field] = _number(
                values[field],
                f"row[{row_index}].{field}",
                nullable=field in {"lcp_p75_ms", "inp_p75_ms", "cls_p75_milli"},
            )
        parsed[str(variation)] = converted
    _require(set(parsed) == {"control", "brand_contrast"}, "CTA final variation set drift")
    quality_fields = [
        "reporting_device_count",
        "eligible_devices_seen_before_stop",
        "raw_event_count",
        "unique_event_count",
        "contaminated_device_count",
        "pii_finding_count",
        "full_url_stored_count",
        "click_identifier_stored_count",
        "non_analytical_consent_exposure_count",
    ]
    _require(
        all(parsed["control"][field] == parsed["brand_contrast"][field] for field in quality_fields),
        "CTA final repeated quality columns differ by variation",
    )
    return parsed


def build_snapshot(
    manifest: Mapping[str, Any],
    athena_results: Mapping[str, Any],
    *,
    evaluated_at_utc: str,
    completion_path: Path = DEFAULT_COMPLETION_PATH,
    activation_path: Path = DEFAULT_ACTIVATION_PATH,
    measurement_path: Path = DEFAULT_MEASUREMENT_PATH,
    sample_plan_path: Path = DEFAULT_SAMPLE_PLAN_PATH,
    decision_contract_path: Path = DEFAULT_DECISION_CONTRACT_PATH,
    lifecycle_path: Path = DEFAULT_LIFECYCLE_PATH,
    stop_observation_path: Path = DEFAULT_STOP_OBSERVATION_PATH,
) -> dict[str, Any]:
    validate_manifest(
        manifest,
        completion_path=completion_path,
        activation_path=activation_path,
        measurement_path=measurement_path,
        sample_plan_path=sample_plan_path,
        decision_contract_path=decision_contract_path,
        lifecycle_path=lifecycle_path,
        stop_observation_path=stop_observation_path,
    )
    _require(manifest["status"] == FOLLOWUP, "CTA final snapshot gate is not open")
    evaluated = _parse_utc(evaluated_at_utc, "evaluated_at_utc")
    final = manifest["final_look"]
    due = _parse_utc(final["snapshot_due_utc"], "snapshot_due_utc")
    _require(evaluated >= due, "CTA final snapshot is before the frozen due time")
    rows = parse_athena_results(athena_results)
    control = rows["control"]
    variant = rows["brand_contrast"]
    included = control["eligible_devices"] + variant["eligible_devices"]
    target = final["target_total_sample"]
    seen = final["eligible_devices_seen_before_stop"]
    expected_included = min(target, seen)
    _require(included == expected_included, "CTA final first-N cohort size drift")
    _require(control["reporting_device_count"] == included, "CTA final reporting cohort count drift")
    _require(control["eligible_devices_seen_before_stop"] == seen, "CTA final stopped eligible count drift")
    raw_count = control["raw_event_count"]
    unique_count = control["unique_event_count"]
    _require(raw_count >= unique_count >= included, "CTA final raw/unique event counts are inconsistent")
    duplicate_rate = ((raw_count - unique_count) / raw_count * 100) if raw_count else 0.0
    exact = control["joined_order_count"] + variant["joined_order_count"]
    unmatched = control["unmatched_transaction_count"] + variant["unmatched_transaction_count"]
    ambiguous = control["ambiguous_transaction_count"] + variant["ambiguous_transaction_count"]
    join_denominator = exact + unmatched + ambiguous
    exact_join_rate = (exact / join_denominator * 100) if join_denominator else 0.0
    privacy_ok = all(
        control[field] == 0
        for field in (
            "pii_finding_count",
            "full_url_stored_count",
            "click_identifier_stored_count",
            "non_analytical_consent_exposure_count",
        )
    )
    stop = _load(stop_observation_path, "CTA stop observation")
    storefront = stop.get("storefront") or {}
    collector = stop.get("collector") or {}
    price_ok = storefront.get("price_mutated") is False
    commerce_ok = (
        storefront.get("cart_mutated") is False
        and storefront.get("checkout_or_order_mutated") is False
        and storefront.get("add_to_cart_text_unchanged") is True
    )
    rollback_ready = (
        collector.get("stop_boundary_verified") is True
        and collector.get("post_stop_cta_exposure_count") == 0
        and collector.get("post_stop_assignment_count") == 0
    )

    variation_payload: dict[str, dict[str, Any]] = {}
    for variation, row in rows.items():
        variation_payload[variation] = {
            "eligible_devices": row["eligible_devices"],
            "add_to_cart_devices": row["add_to_cart_devices"],
            "purchase_devices": row["purchase_devices"],
            "joined_order_count": row["joined_order_count"],
            "net_revenue_sum_eur": row["net_revenue_sum_eur"],
            "net_revenue_sum_squares_eur2": row["net_revenue_sum_squares_eur2"],
            "cm1_sum_eur": row["cm1_sum_eur"],
            "cm1_sum_squares_eur2": row["cm1_sum_squares_eur2"],
            "cancelled_order_count": row["cancelled_order_count"],
            "refunded_order_count": row["refunded_order_count"],
            "client_error_devices": row["client_error_devices"],
            "measured_page_loads": row["measured_page_loads"],
            "lcp_p75_ms": row["lcp_p75_ms"],
            "inp_p75_ms": row["inp_p75_ms"],
            "cls_p75_milli": row["cls_p75_milli"],
        }
    sample = _load(sample_plan_path, "CTA sample plan")
    lifecycle = _load(lifecycle_path, "CTA lifecycle reconciliation")
    contract = _load(decision_contract_path, "CTA decision contract")
    snapshot = {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_cta_aggregate_snapshot",
        "experiment_id": EXPERIMENT_ID,
        "metric_contract_version": METRIC_VERSION,
        "sample_plan_sha256": hashlib.sha256(canonical_sample_bytes(sample)).hexdigest(),
        "aa_snapshot_sha256": sample["final"]["aa_snapshot_sha256"],
        "lifecycle_reconciliation_sha256": lifecycle["observation_sha256"],
        "assignment_started_at_utc": final["assignment_started_at_utc"],
        "assignment_ended_at_utc": final["assignment_ended_at_utc"],
        "evaluated_at_utc": evaluated_at_utc,
        "assignment_stopped": True,
        "production_allocation_percent": 100,
        "decision_cohort": {
            "selection_method": manifest["query_contract"]["selection_method"],
            "target_total_sample": target,
            "eligible_devices_seen_before_stop": seen,
            "included_devices": included,
        },
        "quality": {
            "reporting_device_count": included,
            "growthbook_device_count": included,
            "duplicate_event_rate_percent": round(duplicate_rate, 6),
            "exact_joined_transaction_count": exact,
            "exact_join_rate_percent": round(exact_join_rate, 6),
            "unmatched_transaction_count": unmatched,
            "ambiguous_transaction_count": ambiguous,
            "contaminated_device_count": control["contaminated_device_count"],
            "privacy_audit_passed": privacy_ok,
            "first_n_selection_query_verified": True,
            "all_exposures_24h_mature": evaluated >= _parse_utc(final["assignment_ended_at_utc"], "assignment_ended_at_utc") + timedelta(hours=24),
            "all_orders_7d_mature": evaluated >= _parse_utc(final["assignment_ended_at_utc"], "assignment_ended_at_utc") + timedelta(days=7),
            "all_lifecycles_14d_mature": control["immature_order_count"] + variant["immature_order_count"] == 0,
            "price_integrity_passed": price_ok,
            "cart_checkout_health_passed": commerce_ok,
            "rollback_ready": rollback_ready,
        },
        "variations": variation_payload,
    }
    try:
        validate_snapshot(snapshot, contract)
    except CtaEvaluationError as exc:
        raise CtaFinalSnapshotError(f"CTA final snapshot is invalid: {exc}") from exc
    return snapshot


def opened_manifest(
    manifest: Mapping[str, Any],
    *,
    completion_bytes: bytes,
    activation_bytes: bytes,
    measurement_bytes: bytes,
    sample_plan_bytes: bytes,
    lifecycle_bytes: bytes,
    stop_observation_bytes: bytes,
) -> dict[str, Any]:
    """Return the future post-stop manifest; used by the stop recorder and tests."""

    validate_manifest(manifest)
    _require(manifest["status"] == WAITING, "CTA final snapshot manifest is already opened")
    completion = json.loads(completion_bytes)
    activation = json.loads(activation_bytes)
    measurement = json.loads(measurement_bytes)
    sample = json.loads(sample_plan_bytes)
    stop = json.loads(stop_observation_bytes)
    updated = copy.deepcopy(dict(manifest))
    hashes = {
        "completion_sha256": hashlib.sha256(completion_bytes).hexdigest(),
        "activation_sha256": hashlib.sha256(activation_bytes).hexdigest(),
        "measurement_window_sha256": hashlib.sha256(measurement_bytes).hexdigest(),
        "sample_plan_sha256": hashlib.sha256(sample_plan_bytes).hexdigest(),
        "lifecycle_reconciliation_sha256": hashlib.sha256(lifecycle_bytes).hexdigest(),
        "stop_observation_sha256": hashlib.sha256(stop_observation_bytes).hexdigest(),
    }
    updated["source_bindings"].update(hashes)
    updated["status"] = FOLLOWUP
    updated["final_look"].update(
        {
            "assignment_started_at_utc": activation["start_readback"]["assignment_started_at_utc"],
            "assignment_ended_at_utc": completion["stop_readback"]["assignment_ended_at_utc"],
            "snapshot_due_utc": completion["followup"]["final_snapshot_due_utc"],
            "target_total_sample": sample["final"]["total_sample"],
            "eligible_devices_seen_before_stop": measurement["measurement_window"]["resolved_eligible_devices"],
            "protected_workflow_allowed": True,
        }
    )
    updated["release_boundaries"]["aws_aggregate_reads_allowed"] = True
    updated["release_boundaries"]["diagnostic_host_gate_task_allowed"] = True
    updated["release_boundaries"]["outcome_metrics_read_allowed"] = True
    updated["next_gate"] = "at_or_after_snapshot_due_run_exactly_one_main_only_protected_final_snapshot"
    _require(stop["assignment_ended_at_utc"] == updated["final_look"]["assignment_ended_at_utc"], "CTA final stop bytes drift")
    validate_manifest(
        updated,
        source_bytes={
            "completion": completion_bytes,
            "activation": activation_bytes,
            "measurement_window": measurement_bytes,
            "sample_plan": sample_plan_bytes,
            "lifecycle_reconciliation": lifecycle_bytes,
            "stop_observation": stop_observation_bytes,
        },
    )
    return updated


def _write_atomic(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def _common_paths(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    validate_parser = commands.add_parser("validate")
    _common_paths(validate_parser)
    render_parser = commands.add_parser("render-query")
    _common_paths(render_parser)
    render_parser.add_argument("--output", type=Path, required=True)
    build_parser = commands.add_parser("build")
    _common_paths(build_parser)
    build_parser.add_argument("--athena-results", type=Path, required=True)
    build_parser.add_argument("--evaluated-at-utc", required=True)
    build_parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        manifest = _load(args.manifest, "CTA final snapshot manifest")
        if args.command == "validate":
            validate_manifest(manifest)
            print("VEVO_CTA_FINAL_SNAPSHOT_CONTRACT_OK:one-look=true:aggregate-only=true:mutation=false")
            return 0
        if args.command == "render-query":
            query = render_query(manifest)
            _write_atomic(args.output, query.encode("utf-8"))
            print("VEVO_CTA_FINAL_QUERY_READY:first-n=true:aggregate-only=true:identity-output=false")
            return 0
        athena = _load(args.athena_results, "CTA final Athena result")
        snapshot = build_snapshot(
            manifest, athena, evaluated_at_utc=args.evaluated_at_utc
        )
        _write_atomic(args.output, canonical_json_bytes(snapshot))
        print(
            "VEVO_CTA_FINAL_SNAPSHOT_READY:"
            f"devices={snapshot['decision_cohort']['included_devices']}:"
            "rows=2:identities=false:mutation=false"
        )
        return 0
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        CtaFinalSnapshotError,
    ) as exc:
        print(f"VEVO_CTA_FINAL_SNAPSHOT_INVALID:{exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
