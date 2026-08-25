#!/usr/bin/env python3
"""Validate the frozen outcome-blind VEVO CTA assignment stopping window.

The manifest is intentionally valid before CTA start.  After the reviewed CTA
start readback, the offline recorder binds the exact start and frozen sample.
Daily checkpoints may then read only one cumulative eligible-device count.
They can open a manual stop review, but can never inspect arms/outcomes, stop
GrowthBook automatically, call a winner, or mutate another external system.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

try:
    from scripts.evaluate_growthbook_cta import validate_contract
    from scripts.freeze_growthbook_cta_sample import validate_plan
    from scripts.record_growthbook_cta_activation import (
        RUNNING as CTA_RUNNING_STATUS,
        canonical_json_bytes as canonical_activation_bytes,
        validate_manifest as validate_activation_manifest,
        validate_start_observation,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from evaluate_growthbook_cta import validate_contract
    from freeze_growthbook_cta_sample import validate_plan
    from record_growthbook_cta_activation import (
        RUNNING as CTA_RUNNING_STATUS,
        canonical_json_bytes as canonical_activation_bytes,
        validate_manifest as validate_activation_manifest,
        validate_start_observation,
    )


ROOT = Path(__file__).resolve().parents[1]
VEVO = ROOT / "projects" / "vevo"
MANIFEST_PATH = VEVO / "growthbook_cta_measurement_window.json"
ACTIVATION_PATH = VEVO / "growthbook_cta_activation.json"
START_OBSERVATION_PATH = VEVO / "growthbook_cta_activation_observation.json"
SAMPLE_PLAN_PATH = VEVO / "growthbook_cta_sample_plan.json"
DECISION_CONTRACT_PATH = VEVO / "growthbook_cta_decision_contract.json"
RECONCILIATION_EVIDENCE_PATH = (
    VEVO / "growthbook_production_reconciliation_deploy_evidence.json"
)

WAITING = "waiting_for_verified_cta_start"
RUNNING = "cta_running_outcome_blind_checkpoint_pending"
RESOLVED = "cta_assignment_stop_review_open_by_preregistered_rule"
EXPERIMENT_ID = "vevo-sk-product-cta-color-001"
TIMEZONE = "Europe/Bratislava"
CHECKPOINT_TIME = time(hour=3, minute=45)
CHECKPOINT_WORKFLOW = ".github/workflows/check-vevo-growthbook-production-cta-window.yml"
CHECKPOINT_ARTIFACT = "vevo-growthbook-cta-window-checkpoint"
CHECKPOINT_FILE = "vevo-growthbook-cta-window-checkpoint.json"
EXPECTED_RECONCILIATION_SHA256 = (
    "21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb"
)
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
RUN_ID_RE = re.compile(r"^[1-9][0-9]*$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IP_RE = re.compile(
    r"^(?:(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\.){3}"
    r"(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})$"
)

ROOT_KEYS = {
    "schema_version",
    "experiment_id",
    "status",
    "source_bindings",
    "measurement_window",
    "assignment_stop",
    "release_boundaries",
    "next_gate",
}
BINDING_KEYS = {
    "activation_path",
    "activation_sha256",
    "start_observation_path",
    "start_observation_sha256",
    "sample_plan_path",
    "sample_plan_sha256",
    "decision_contract_path",
    "decision_contract_sha256",
    "reconciliation_evidence_path",
    "reconciliation_evidence_sha256",
}
WINDOW_KEYS = {
    "timezone",
    "assignment_started_at_utc",
    "first_full_local_date",
    "minimum_last_full_local_date",
    "maximum_last_full_local_date",
    "minimum_full_calendar_days",
    "maximum_full_calendar_days",
    "target_total_sample",
    "minimum_through_utc",
    "maximum_through_utc",
    "earliest_checkpoint_due_local",
    "checkpoint_time_local",
    "checkpoint_workflow",
    "checkpoint_artifact_name",
    "checkpoint_file_name",
    "stopping_rule",
    "population_metric",
    "outcome_blind",
    "whole_local_day_extensions_only",
    "resolution_status",
    "resolved_reason",
    "resolved_checkpoint_through_utc",
    "resolved_last_full_local_date",
    "resolved_full_calendar_days",
    "resolved_eligible_devices",
    "resolved_at_utc",
    "checkpoint_history",
    "post_hoc_window_change_allowed",
}
STOP_KEYS = {
    "status",
    "manual_review_allowed",
    "automatic_stop_allowed",
    "observation_path",
    "observation_sha256",
    "assignment_ended_at_utc",
}
BOUNDARY_KEYS = {
    "main_only",
    "read_only_checkpoint_allowed",
    "arm_counts_read_allowed",
    "outcome_metrics_read_allowed",
    "automatic_growthbook_mutation_allowed",
    "automatic_gtm_mutation_allowed",
    "automatic_meta_ads_mutation_allowed",
    "automatic_biznisweb_mutation_allowed",
    "automatic_collector_or_reporting_mutation_allowed",
    "commerce_mutation_allowed",
    "winner_calls_allowed",
}
CHECKPOINT_ROOT_KEYS = {
    "schema_version",
    "evidence_type",
    "status",
    "experiment_id",
    "repository",
    "workflow",
    "workflow_run_id",
    "main_commit",
    "observed_at_utc",
    "window",
    "runtime",
    "control_plane",
    "population",
    "decision",
    "safety",
}
CHECKPOINT_WINDOW_KEYS = {
    "timezone",
    "checkpoint_index",
    "assignment_started_at_utc",
    "candidate_through_utc",
    "candidate_last_full_local_date",
    "full_calendar_days",
    "resolution_due_local",
}
RUNTIME_KEYS = {
    "instance_id",
    "private_ip",
    "service",
    "runtime_path",
    "task_id",
    "task_definition",
    "image_digest",
    "host_gate_evidence_sha256",
    "localhost_health_marker_inherited_from_deploy_evidence",
    "localhost_runtime_marker_inherited_from_deploy_evidence",
}
CONTROL_KEYS = {
    "schedule_name",
    "schedule_due_local",
    "schedule_succeeded",
    "success_marker_sha256",
    "publish_summary_sha256",
    "generated_published_counts_match",
    "dlq_empty",
    "alarms_clear",
    "source_schedule_name",
    "source_schedule_unchanged",
}
POPULATION_KEYS = {
    "metric",
    "eligible_devices",
    "target_total_sample",
    "database",
    "workgroup",
    "source_table",
    "aggregate_query_sha256",
    "aggregate_result_sha256",
    "only_aggregate_count_retained",
    "arm_counts_read",
    "arm_outcomes_read",
    "outcome_metrics_read",
    "contains_event_or_device_ids",
    "contains_customer_or_order_data",
}
SAFETY_KEYS = {
    "contains_raw_aws_payloads",
    "contains_cloudwatch_messages",
    "contains_credentials",
    "aws_mutations",
    "growthbook_mutations",
    "gtm_mutations",
    "meta_ads_mutations",
    "biznisweb_mutations",
    "collector_or_reporting_mutations",
    "commerce_mutations",
    "winner_calls",
    "assignment_stopped",
}


class CtaMeasurementWindowError(ValueError):
    """Raised when a CTA assignment checkpoint boundary is unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaMeasurementWindowError(message)


def _exact_object(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{field} must be an object")
    _require(set(value) == keys, f"{field} field set drift")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(UTC_RE.fullmatch(text) is not None, f"{field} must use whole-second UTC Z")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError as exc:
        raise CtaMeasurementWindowError(f"{field} is invalid") from exc


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_evidence_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def expected_measurement_window(
    activation: Mapping[str, Any],
    start_observation: Mapping[str, Any],
    sample_plan: Mapping[str, Any],
    contract: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
) -> dict[str, Any]:
    validate_activation_manifest(activation)
    _require(activation.get("status") == CTA_RUNNING_STATUS, "CTA start is not verified")
    validate_start_observation(start_observation, activation)
    validate_plan(sample_plan)
    validate_contract(contract)
    _require(
        sample_plan.get("status") == "sample_frozen_activation_still_blocked",
        "CTA sample is not frozen",
    )
    _require(
        contract["decision_timing"]
        == {
            "minimum_full_calendar_days": 14,
            "maximum_full_calendar_days": 42,
            "required_followup_days_after_assignment_stop": 14,
            "assignment_must_be_stopped": True,
            "one_final_look_only": True,
            "safety_guardrails_may_stop_early": True,
        },
        "CTA decision timing drift",
    )
    target = sample_plan["final"].get("total_sample")
    _require(type(target) is int and target >= 2, "CTA frozen sample target is invalid")
    started = _parse_utc(
        start_observation.get("assignment_started_at_utc"),
        "assignment_started_at_utc",
    )
    _require(
        activation["start_readback"].get("assignment_started_at_utc")
        == _utc_text(started),
        "CTA activation start binding drift",
    )
    local_timezone = ZoneInfo(TIMEZONE)
    local_started = started.astimezone(local_timezone)
    local_midnight = datetime.combine(local_started.date(), time.min, tzinfo=local_timezone)
    first_full = local_midnight if local_started == local_midnight else local_midnight + timedelta(days=1)
    minimum_through = first_full + timedelta(days=14)
    maximum_through = first_full + timedelta(days=42)
    due = datetime.combine(minimum_through.date(), CHECKPOINT_TIME, tzinfo=local_timezone)

    schedule = reconciliation.get("schedule") or {}
    _require(
        schedule
        == {
            "dlq": "vevo-growthbook-reconcile-production-dlq",
            "dlq_alarm": "vevo-growthbook-reconcile-production-dlq",
            "enabled": True,
            "expression": "cron(45 3 * * ? *)",
            "failure_alarm": "vevo-growthbook-reconcile-production-failure",
            "missing_success_alarm": "vevo-growthbook-reconcile-production-missing-success",
            "name": "vevo-growthbook-reconcile-production",
            "timezone": TIMEZONE,
        },
        "Production reconciliation schedule drift",
    )
    return {
        "timezone": TIMEZONE,
        "assignment_started_at_utc": _utc_text(started),
        "first_full_local_date": first_full.date().isoformat(),
        "minimum_last_full_local_date": (minimum_through.date() - timedelta(days=1)).isoformat(),
        "maximum_last_full_local_date": (maximum_through.date() - timedelta(days=1)).isoformat(),
        "minimum_full_calendar_days": 14,
        "maximum_full_calendar_days": 42,
        "target_total_sample": target,
        "minimum_through_utc": _utc_text(minimum_through),
        "maximum_through_utc": _utc_text(maximum_through),
        "earliest_checkpoint_due_local": due.isoformat(timespec="seconds"),
        "checkpoint_time_local": "03:45:00",
        "checkpoint_workflow": CHECKPOINT_WORKFLOW,
        "checkpoint_artifact_name": CHECKPOINT_ARTIFACT,
        "checkpoint_file_name": CHECKPOINT_FILE,
        "stopping_rule": "after_14_full_local_days_open_manual_stop_at_first_post_reconciliation_checkpoint_with_target_first_n_or_at_42_full_local_days",
        "population_metric": "cumulative_eligible_first_exposed_devices_without_arm_or_outcome_readback",
        "outcome_blind": True,
        "whole_local_day_extensions_only": True,
        "resolution_status": "pending_target_or_42_full_local_days",
        "resolved_reason": None,
        "resolved_checkpoint_through_utc": None,
        "resolved_last_full_local_date": None,
        "resolved_full_calendar_days": None,
        "resolved_eligible_devices": None,
        "resolved_at_utc": None,
        "checkpoint_history": [],
        "post_hoc_window_change_allowed": False,
    }


def checkpoint_boundaries(
    expected: Mapping[str, Any], checkpoint_index: int
) -> tuple[datetime, datetime, str, int]:
    _require(type(checkpoint_index) is int and checkpoint_index >= 1, "checkpoint index invalid")
    local_timezone = ZoneInfo(str(expected["timezone"]))
    minimum_through = _parse_utc(
        expected["minimum_through_utc"], "minimum_through_utc"
    ).astimezone(local_timezone)
    candidate_date = minimum_through.date() + timedelta(days=checkpoint_index - 1)
    _require(
        candidate_date <= _parse_utc(expected["maximum_through_utc"], "maximum_through_utc").astimezone(local_timezone).date(),
        "checkpoint exceeds 42 full local days",
    )
    candidate = datetime.combine(candidate_date, time.min, tzinfo=local_timezone)
    due = datetime.combine(candidate_date, CHECKPOINT_TIME, tzinfo=local_timezone)
    full_days = 14 + checkpoint_index - 1
    return candidate, due, (candidate_date - timedelta(days=1)).isoformat(), full_days


def validate_checkpoint_evidence(
    evidence: Mapping[str, Any], expected: Mapping[str, Any], checkpoint_index: int
) -> None:
    root = _exact_object(evidence, CHECKPOINT_ROOT_KEYS, "CTA checkpoint evidence")
    _require(root["schema_version"] == 1, "CTA checkpoint schema drift")
    _require(root["evidence_type"] == "vevo_growthbook_cta_window_checkpoint", "CTA checkpoint type drift")
    _require(root["status"] == "passed", "CTA checkpoint did not pass")
    _require(root["experiment_id"] == EXPERIMENT_ID, "CTA checkpoint experiment drift")
    _require(root["repository"] == "vzeman/biznisweb", "CTA checkpoint repository drift")
    _require(root["workflow"] == CHECKPOINT_WORKFLOW, "CTA checkpoint workflow drift")
    _require(RUN_ID_RE.fullmatch(str(root["workflow_run_id"])) is not None, "CTA checkpoint run ID invalid")
    _require(COMMIT_RE.fullmatch(str(root["main_commit"])) is not None, "CTA checkpoint commit invalid")
    observed = _parse_utc(root["observed_at_utc"], "observed_at_utc")
    window = _exact_object(root["window"], CHECKPOINT_WINDOW_KEYS, "CTA checkpoint window")
    candidate, due, last_date, full_days = checkpoint_boundaries(expected, checkpoint_index)
    _require(
        window
        == {
            "timezone": TIMEZONE,
            "checkpoint_index": checkpoint_index,
            "assignment_started_at_utc": expected["assignment_started_at_utc"],
            "candidate_through_utc": _utc_text(candidate),
            "candidate_last_full_local_date": last_date,
            "full_calendar_days": full_days,
            "resolution_due_local": due.isoformat(timespec="seconds"),
        },
        "CTA checkpoint window drift",
    )
    _require(
        due.astimezone(UTC) <= observed < (due + timedelta(days=1)).astimezone(UTC),
        "CTA checkpoint observed outside daily gate",
    )

    runtime = _exact_object(root["runtime"], RUNTIME_KEYS, "CTA checkpoint runtime")
    _require(runtime["instance_id"] == "N/A:Fargate", "CTA checkpoint instance drift")
    _require(IP_RE.fullmatch(str(runtime["private_ip"])) is not None, "CTA checkpoint IP invalid")
    _require(runtime["service"] == "vevo-growthbook-reconcile-production", "CTA checkpoint service drift")
    _require(runtime["runtime_path"] == "/app", "CTA checkpoint path drift")
    _require(re.fullmatch(r"[0-9a-f]{32}", str(runtime["task_id"])) is not None, "CTA checkpoint task invalid")
    _require(str(runtime["task_definition"]).startswith("vevo-growthbook-reconcile-production:"), "CTA checkpoint task definition drift")
    _require(re.fullmatch(r"sha256:[0-9a-f]{64}", str(runtime["image_digest"])) is not None, "CTA checkpoint image invalid")
    _require(runtime["host_gate_evidence_sha256"] == EXPECTED_RECONCILIATION_SHA256, "CTA checkpoint host-gate evidence drift")
    _require(runtime["localhost_health_marker_inherited_from_deploy_evidence"] is True, "CTA checkpoint localhost health missing")
    _require(runtime["localhost_runtime_marker_inherited_from_deploy_evidence"] is True, "CTA checkpoint runtime marker missing")

    control = _exact_object(root["control_plane"], CONTROL_KEYS, "CTA checkpoint control plane")
    _require(control["schedule_name"] == "vevo-growthbook-reconcile-production", "CTA checkpoint schedule drift")
    _require(control["schedule_due_local"] == due.isoformat(timespec="seconds"), "CTA checkpoint due drift")
    _require(control["source_schedule_name"] == "vevo-daily-report-email", "CTA source schedule drift")
    for field in ("schedule_succeeded", "generated_published_counts_match", "dlq_empty", "alarms_clear", "source_schedule_unchanged"):
        _require(control[field] is True, f"CTA checkpoint control gate failed: {field}")
    for field in ("success_marker_sha256", "publish_summary_sha256"):
        _require(SHA256_RE.fullmatch(str(control[field])) is not None, f"CTA checkpoint {field} invalid")

    population = _exact_object(root["population"], POPULATION_KEYS, "CTA checkpoint population")
    _require(population["metric"] == expected["population_metric"], "CTA checkpoint population metric drift")
    eligible = population["eligible_devices"]
    _require(type(eligible) is int and eligible >= 0, "CTA checkpoint eligible count invalid")
    _require(population["target_total_sample"] == expected["target_total_sample"], "CTA checkpoint target drift")
    _require(population["database"] == "vevo_growthbook_production", "CTA checkpoint database drift")
    _require(population["workgroup"] == "vevo-growthbook-reporting-production", "CTA checkpoint workgroup drift")
    _require(population["source_table"] == "experiment_device_facts", "CTA checkpoint table drift")
    for field in ("aggregate_query_sha256", "aggregate_result_sha256"):
        _require(SHA256_RE.fullmatch(str(population[field])) is not None, f"CTA checkpoint {field} invalid")
    _require(population["only_aggregate_count_retained"] is True, "CTA checkpoint retained non-aggregate data")
    for field in ("arm_counts_read", "arm_outcomes_read", "outcome_metrics_read", "contains_event_or_device_ids", "contains_customer_or_order_data"):
        _require(population[field] is False, f"CTA checkpoint forbidden population read: {field}")

    if full_days >= expected["maximum_full_calendar_days"]:
        decision = "open_manual_stop_review_maximum_duration_reached"
    elif eligible >= expected["target_total_sample"]:
        decision = "open_manual_stop_review_target_reached"
    else:
        decision = "extend_one_full_local_day"
    _require(root["decision"] == decision, "CTA checkpoint decision drift")
    safety = _exact_object(root["safety"], SAFETY_KEYS, "CTA checkpoint safety")
    _require(not any(safety.values()), "CTA checkpoint safety boundary drift")


def validate_manifest(
    manifest: Mapping[str, Any],
    activation: Mapping[str, Any],
    sample_plan: Mapping[str, Any],
    contract: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    start_observation: Mapping[str, Any] | None = None,
    *,
    source_hashes: Mapping[str, str] | None = None,
) -> None:
    validate_activation_manifest(activation)
    validate_plan(sample_plan)
    validate_contract(contract)
    root = _exact_object(manifest, ROOT_KEYS, "CTA measurement manifest")
    _require(root["schema_version"] == 1, "CTA measurement schema drift")
    _require(root["experiment_id"] == EXPERIMENT_ID, "CTA measurement experiment drift")
    _require(root["status"] in {WAITING, RUNNING, RESOLVED}, "CTA measurement status drift")
    bindings = _exact_object(root["source_bindings"], BINDING_KEYS, "CTA source bindings")
    expected_paths = {
        "activation_path": "projects/vevo/growthbook_cta_activation.json",
        "start_observation_path": "projects/vevo/growthbook_cta_activation_observation.json",
        "sample_plan_path": "projects/vevo/growthbook_cta_sample_plan.json",
        "decision_contract_path": "projects/vevo/growthbook_cta_decision_contract.json",
        "reconciliation_evidence_path": "projects/vevo/growthbook_production_reconciliation_deploy_evidence.json",
    }
    for field, value in expected_paths.items():
        _require(bindings[field] == value, f"CTA source path drift: {field}")
    actual_hashes = dict(source_hashes or {})
    if not actual_hashes:
        actual_hashes = {
            "activation": _sha256(ACTIVATION_PATH),
            "start_observation": (
                _sha256(START_OBSERVATION_PATH)
                if START_OBSERVATION_PATH.exists()
                else ""
            ),
            "sample_plan": _sha256(SAMPLE_PLAN_PATH),
            "decision_contract": _sha256(DECISION_CONTRACT_PATH),
            "reconciliation_evidence": _sha256(RECONCILIATION_EVIDENCE_PATH),
        }
    _require(set(actual_hashes) == {"activation", "start_observation", "sample_plan", "decision_contract", "reconciliation_evidence"}, "CTA source hash set drift")
    _require(bindings["decision_contract_sha256"] == actual_hashes["decision_contract"], "CTA decision hash drift")
    _require(bindings["reconciliation_evidence_sha256"] == actual_hashes["reconciliation_evidence"], "CTA reconciliation hash drift")
    _require(bindings["reconciliation_evidence_sha256"] == EXPECTED_RECONCILIATION_SHA256, "CTA reconciliation evidence boundary drift")
    window = _exact_object(root["measurement_window"], WINDOW_KEYS, "CTA measurement window")
    stop = _exact_object(root["assignment_stop"], STOP_KEYS, "CTA assignment stop")
    boundaries = _exact_object(root["release_boundaries"], BOUNDARY_KEYS, "CTA release boundaries")
    _require(boundaries["main_only"] is True, "CTA checkpoint is not main-only")
    for field in BOUNDARY_KEYS - {"main_only", "read_only_checkpoint_allowed"}:
        _require(boundaries[field] is False, f"CTA forbidden release boundary opened: {field}")
    _require(stop["automatic_stop_allowed"] is False, "CTA automatic stop gate opened")
    _require(stop["observation_path"] == "projects/vevo/growthbook_cta_assignment_stop_observation.json", "CTA stop observation path drift")

    if root["status"] == WAITING:
        for field in ("activation_sha256", "start_observation_sha256", "sample_plan_sha256"):
            _require(bindings[field] is None, f"waiting CTA source binding populated: {field}")
        nullable = WINDOW_KEYS - {
            "timezone", "minimum_full_calendar_days", "maximum_full_calendar_days",
            "checkpoint_time_local", "checkpoint_workflow", "checkpoint_artifact_name",
            "checkpoint_file_name", "stopping_rule", "population_metric", "outcome_blind",
            "whole_local_day_extensions_only", "resolution_status", "checkpoint_history",
            "post_hoc_window_change_allowed",
        }
        for field in nullable:
            _require(window[field] is None, f"waiting CTA window field populated: {field}")
        _require(window["timezone"] == TIMEZONE, "waiting CTA timezone drift")
        _require(window["minimum_full_calendar_days"] == 14 and window["maximum_full_calendar_days"] == 42, "waiting CTA duration drift")
        _require(window["checkpoint_time_local"] == "03:45:00", "waiting CTA checkpoint time drift")
        _require(window["checkpoint_workflow"] == CHECKPOINT_WORKFLOW, "waiting CTA workflow drift")
        _require(window["checkpoint_artifact_name"] == CHECKPOINT_ARTIFACT and window["checkpoint_file_name"] == CHECKPOINT_FILE, "waiting CTA artifact drift")
        _require(
            window["stopping_rule"]
            == "after_14_full_local_days_open_manual_stop_at_first_post_reconciliation_checkpoint_with_target_first_n_or_at_42_full_local_days",
            "waiting CTA stopping rule drift",
        )
        _require(
            window["population_metric"]
            == "cumulative_eligible_first_exposed_devices_without_arm_or_outcome_readback",
            "waiting CTA population metric drift",
        )
        _require(window["outcome_blind"] is True and window["whole_local_day_extensions_only"] is True, "waiting CTA outcome-blind rule drift")
        _require(window["resolution_status"] == WAITING and window["checkpoint_history"] == [], "waiting CTA resolution drift")
        _require(window["post_hoc_window_change_allowed"] is False, "waiting CTA post-hoc change opened")
        _require(stop == {"status": "not_open", "manual_review_allowed": False, "automatic_stop_allowed": False, "observation_path": "projects/vevo/growthbook_cta_assignment_stop_observation.json", "observation_sha256": None, "assignment_ended_at_utc": None}, "waiting CTA stop state drift")
        _require(boundaries["read_only_checkpoint_allowed"] is False, "waiting CTA checkpoint gate opened")
        _require(root["next_gate"] == "after_verified_cta_start_initialize_frozen_outcome_blind_window", "waiting CTA next gate drift")
        return

    _require(start_observation is not None, "running CTA start observation missing")
    expected = expected_measurement_window(
        activation, start_observation, sample_plan, contract, reconciliation
    )
    _require(bindings["activation_sha256"] == actual_hashes["activation"], "CTA activation hash drift")
    _require(bindings["sample_plan_sha256"] == actual_hashes["sample_plan"], "CTA sample hash drift")
    _require(
        activation["source_bindings"]["sample_plan"]["sha256"]
        == actual_hashes["sample_plan"],
        "CTA activation/sample provenance drift",
    )
    _require(
        activation["launch_contract"]["target_total_sample"]
        == sample_plan["final"]["total_sample"],
        "CTA activation/sample target drift",
    )
    observation_bytes = canonical_activation_bytes(start_observation)
    _require(bindings["start_observation_sha256"] == actual_hashes["start_observation"], "CTA start observation source hash drift")
    _require(bindings["start_observation_sha256"] == hashlib.sha256(observation_bytes).hexdigest(), "CTA start observation canonical hash drift")
    history = window["checkpoint_history"]
    _require(isinstance(history, list), "CTA checkpoint history must be a list")
    expected_static = dict(expected)
    expected_static["checkpoint_history"] = history
    for index, row in enumerate(history, start=1):
        _require(isinstance(row, Mapping) and set(row) == {"evidence_sha256", "evidence"}, "CTA checkpoint history row drift")
        _require(SHA256_RE.fullmatch(str(row["evidence_sha256"])) is not None, "CTA checkpoint history hash invalid")
        _require(hashlib.sha256(canonical_evidence_bytes(row["evidence"])).hexdigest() == row["evidence_sha256"], "CTA checkpoint history hash mismatch")
        validate_checkpoint_evidence(row["evidence"], expected, index)
    eligible_history = [
        row["evidence"]["population"]["eligible_devices"] for row in history
    ]
    _require(
        eligible_history == sorted(eligible_history),
        "CTA cumulative eligible-device count decreased",
    )

    if root["status"] == RUNNING:
        _require(
            all(
                row["evidence"]["decision"] == "extend_one_full_local_day"
                for row in history
            ),
            "running CTA history already contains a stop decision",
        )
        _require(window == expected_static, "running CTA measurement window drift")
        _require(stop == {"status": "not_open", "manual_review_allowed": False, "automatic_stop_allowed": False, "observation_path": "projects/vevo/growthbook_cta_assignment_stop_observation.json", "observation_sha256": None, "assignment_ended_at_utc": None}, "running CTA stop state drift")
        _require(boundaries["read_only_checkpoint_allowed"] is True, "running CTA checkpoint gate closed")
        _require(root["next_gate"] == "run_first_due_outcome_blind_checkpoint_without_arm_or_outcome_readback", "running CTA next gate drift")
        return

    _require(bool(history), "resolved CTA has no checkpoint")
    _require(
        all(
            row["evidence"]["decision"] == "extend_one_full_local_day"
            for row in history[:-1]
        ),
        "resolved CTA has a stop decision before its final checkpoint",
    )
    final_evidence = history[-1]["evidence"]
    _require(final_evidence["decision"] in {"open_manual_stop_review_target_reached", "open_manual_stop_review_maximum_duration_reached"}, "resolved CTA final checkpoint did not open stop")
    expected_resolved = dict(expected_static)
    final_window = final_evidence["window"]
    expected_resolved.update({
        "resolution_status": "resolved_waiting_for_manual_assignment_stop",
        "resolved_reason": "target_total_sample_reached" if final_evidence["decision"].endswith("target_reached") else "maximum_duration_reached",
        "resolved_checkpoint_through_utc": final_window["candidate_through_utc"],
        "resolved_last_full_local_date": final_window["candidate_last_full_local_date"],
        "resolved_full_calendar_days": final_window["full_calendar_days"],
        "resolved_eligible_devices": final_evidence["population"]["eligible_devices"],
        "resolved_at_utc": final_evidence["observed_at_utc"],
    })
    _require(window == expected_resolved, "resolved CTA measurement window drift")
    _require(stop == {"status": "manual_stop_review_open_assignment_still_running", "manual_review_allowed": True, "automatic_stop_allowed": False, "observation_path": "projects/vevo/growthbook_cta_assignment_stop_observation.json", "observation_sha256": None, "assignment_ended_at_utc": None}, "resolved CTA stop review drift")
    _require(boundaries["read_only_checkpoint_allowed"] is False, "resolved CTA checkpoint gate remains open")
    _require(root["next_gate"] == "manually_stop_only_cta_assignment_then_record_canonical_readback", "resolved CTA next gate drift")


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    _require(isinstance(value, dict), f"{path.name} must contain an object")
    return value


def main() -> int:
    try:
        manifest = _load(MANIFEST_PATH)
        start_observation = None
        if manifest.get("status") != WAITING:
            raw = START_OBSERVATION_PATH.read_bytes()
            start_observation = json.loads(raw.decode("utf-8"))
            _require(raw == canonical_activation_bytes(start_observation), "CTA start observation is not canonical JSON")
        validate_manifest(
            manifest,
            _load(ACTIVATION_PATH),
            _load(SAMPLE_PLAN_PATH),
            _load(DECISION_CONTRACT_PATH),
            _load(RECONCILIATION_EVIDENCE_PATH),
            start_observation,
        )
        print("validate_growthbook_cta_measurement_window.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(f"validate_growthbook_cta_measurement_window.py: FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
