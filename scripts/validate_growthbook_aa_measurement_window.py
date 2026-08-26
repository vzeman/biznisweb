#!/usr/bin/env python3
"""Validate the pre-registered VEVO Production A/A measurement window."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import sys
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
ACTIVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_production_aa_activation.json"
)
ACCEPTANCE_PATH = ROOT / "projects" / "vevo" / "growthbook_aa_acceptance.json"
SNAPSHOT_PATH = ROOT / "projects" / "vevo" / "growthbook_aa_snapshot.json"
RECONCILIATION_EVIDENCE_PATH = (
    ROOT
    / "projects"
    / "vevo"
    / "growthbook_production_reconciliation_deploy_evidence.json"
)


class MeasurementWindowError(ValueError):
    """Raised when the pre-registered window or its provenance drifts."""


CHECKPOINT_WORKFLOW = ".github/workflows/check-vevo-growthbook-production-aa-window.yml"
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
    "from_utc",
    "candidate_through_utc",
    "candidate_last_full_local_date",
    "full_calendar_days",
    "resolution_due_local",
}
CHECKPOINT_RUNTIME_KEYS = {
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
CHECKPOINT_CONTROL_KEYS = {
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
CHECKPOINT_POPULATION_KEYS = {
    "metric",
    "eligible_devices",
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
CHECKPOINT_SAFETY = {
    "contains_raw_aws_payloads": False,
    "contains_cloudwatch_messages": False,
    "contains_credentials": False,
    "aws_mutations": False,
    "growthbook_mutations": False,
    "gtm_mutations": False,
    "meta_ads_mutations": False,
    "biznisweb_mutations": False,
    "commerce_mutations": False,
    "winner_calls": False,
    "cta_activation": False,
}
RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")
TASK_DEFINITION_RE = re.compile(r"^vevo-growthbook-reconcile-production:[1-9][0-9]*$")
IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MUTABLE_WINDOW_KEYS = {
    "status",
    "resolution_status",
    "resolved_last_full_local_date",
    "resolved_through_utc",
    "resolved_full_calendar_days",
    "resolved_eligible_devices",
    "resolved_at_utc",
    "checkpoint_history",
}

QUALITY_KEY_RE = re.compile(
    r"^experiment-events/curated/quality/experiment_id=vevo-sk-aa-001/"
    r"facts_generated_at=20[2-9][0-9]{5}T[0-9]{6}Z[.]json$"
)


def _load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise MeasurementWindowError(f"{path.name} must contain an object")
    return payload


def _parse_utc(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise MeasurementWindowError(f"{field} must be an explicit UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise MeasurementWindowError(f"{field} is invalid") from exc
    if parsed.tzinfo != UTC or parsed.microsecond:
        raise MeasurementWindowError(f"{field} must be second-precision UTC")
    return parsed


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical_evidence_bytes(evidence: Mapping[str, Any]) -> bytes:
    return (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _exact_object(value: object, keys: set[str], field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise MeasurementWindowError(f"{field} field set drift")
    return value


def expected_measurement_window(
    activation: Mapping[str, Any],
    acceptance: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
) -> dict[str, Any]:
    if (
        activation.get("activation_type") != "vevo_growthbook_production_aa"
        or activation.get("tracking_key") != "vevo-sk-aa-001"
    ):
        raise MeasurementWindowError("Production A/A activation identity drift")

    timezone_name = acceptance.get("timezone")
    if timezone_name != "Europe/Bratislava":
        raise MeasurementWindowError("A/A acceptance timezone drift")
    local_timezone = ZoneInfo(timezone_name)
    minimum_days = acceptance.get("minimum_full_calendar_days")
    minimum_devices = acceptance.get("minimum_eligible_devices")
    if type(minimum_days) is not int or minimum_days != 7:
        raise MeasurementWindowError("A/A minimum full-calendar-day contract drift")
    if type(minimum_devices) is not int or minimum_devices != 1000:
        raise MeasurementWindowError("A/A minimum eligible-device contract drift")

    readback = activation.get("activation_readback") or {}
    readback_growthbook = readback.get("growthbook") or {}
    if (
        readback.get("status") != "verified_running_production_aa"
        or readback_growthbook.get("experiment_status") != "running"
        or readback_growthbook.get("environment") != "production_only"
        or readback_growthbook.get("traffic_percent") != 100
        or readback_growthbook.get("variation_weights") != [0.5, 0.5]
        or readback_growthbook.get("feature_revision") != 3
        or readback_growthbook.get("feature_revision_status") != "live"
    ):
        raise MeasurementWindowError("Production A/A activation readback drift")
    observed_at = _parse_utc(
        readback.get("observed_at_utc"), "activation_readback.observed_at_utc"
    )
    first_full_local_date = observed_at.astimezone(local_timezone).date() + timedelta(
        days=1
    )
    window_start_local = datetime.combine(
        first_full_local_date, time.min, tzinfo=local_timezone
    )
    minimum_through_local = window_start_local + timedelta(days=minimum_days)
    last_required_full_local_date = minimum_through_local.date() - timedelta(days=1)

    schedule = reconciliation.get("schedule") or {}
    if schedule != {
        "dlq": "vevo-growthbook-reconcile-production-dlq",
        "dlq_alarm": "vevo-growthbook-reconcile-production-dlq",
        "enabled": True,
        "expression": "cron(45 3 * * ? *)",
        "failure_alarm": "vevo-growthbook-reconcile-production-failure",
        "missing_success_alarm": "vevo-growthbook-reconcile-production-missing-success",
        "name": "vevo-growthbook-reconcile-production",
        "timezone": timezone_name,
    }:
        raise MeasurementWindowError("Production reconciliation schedule drift")
    first_resolution_check = datetime.combine(
        minimum_through_local.date(), time(hour=3, minute=45), tzinfo=local_timezone
    )

    binding = activation.get("production_reconciliation") or {}
    if binding.get("workflow_run_id") != reconciliation.get(
        "source_run_id"
    ) or binding.get("main_commit") != reconciliation.get("source_main_commit"):
        raise MeasurementWindowError("Production reconciliation provenance drift")

    return {
        "status": "frozen_start_and_stopping_rule_before_outcome_readback",
        "timezone": timezone_name,
        "activation_observed_at_utc": _utc_text(observed_at),
        "source_activation_workflow_run_id": readback.get("workflow_run_id"),
        "source_activation_main_commit": readback.get("main_commit"),
        "source_reconciliation_workflow_run_id": reconciliation.get("source_run_id"),
        "source_reconciliation_main_commit": reconciliation.get("source_main_commit"),
        "checkpoint_workflow": CHECKPOINT_WORKFLOW,
        "checkpoint_artifact_name": "vevo-growthbook-aa-window-checkpoint",
        "checkpoint_file_name": "vevo-growthbook-aa-window-checkpoint.json",
        "first_full_local_date": first_full_local_date.isoformat(),
        "last_required_full_local_date": last_required_full_local_date.isoformat(),
        "minimum_full_calendar_days": minimum_days,
        "minimum_eligible_devices": minimum_devices,
        "from_utc": _utc_text(window_start_local),
        "minimum_through_utc": _utc_text(minimum_through_local),
        "earliest_resolution_check_due_local": (
            first_resolution_check.isoformat(timespec="seconds")
        ),
        "resolution_checkpoint_time_local": "03:45:00",
        "stopping_rule": (
            "after_minimum_days_resolve_at_first_successful_daily_reconciliation_"
            "with_minimum_eligible_devices"
        ),
        "stopping_rule_population_metric": (
            "cumulative_eligible_devices_without_arm_outcome_readback"
        ),
        "whole_local_day_extensions_only": True,
        "outcome_blind_resolution_required": True,
        "resolution_status": "pending_minimum_window_and_sample",
        "resolved_last_full_local_date": None,
        "resolved_through_utc": None,
        "resolved_full_calendar_days": None,
        "resolved_eligible_devices": None,
        "resolved_at_utc": None,
        "checkpoint_history": [],
        "post_hoc_window_change_allowed": False,
    }


def _checkpoint_boundaries(
    expected: Mapping[str, Any], checkpoint_index: int
) -> tuple[datetime, datetime, str, int]:
    if type(checkpoint_index) is not int or checkpoint_index < 1:
        raise MeasurementWindowError("checkpoint index is invalid")
    local_timezone = ZoneInfo(str(expected["timezone"]))
    minimum_through = _parse_utc(
        expected["minimum_through_utc"], "minimum_through_utc"
    ).astimezone(local_timezone)
    candidate_date = minimum_through.date() + timedelta(days=checkpoint_index - 1)
    candidate_through = datetime.combine(
        candidate_date, time.min, tzinfo=local_timezone
    )
    due = datetime.combine(
        candidate_date, time(hour=3, minute=45), tzinfo=local_timezone
    )
    last_full_date = candidate_date - timedelta(days=1)
    full_days = int(expected["minimum_full_calendar_days"]) + checkpoint_index - 1
    return candidate_through, due, last_full_date.isoformat(), full_days


def validate_checkpoint_evidence(
    evidence: Mapping[str, Any],
    expected: Mapping[str, Any],
    checkpoint_index: int,
) -> None:
    schema_version = (
        evidence.get("schema_version") if isinstance(evidence, dict) else None
    )
    root_keys = set(CHECKPOINT_ROOT_KEYS)
    if schema_version == 3:
        root_keys.add("collection_mode")
    root = _exact_object(evidence, root_keys, "checkpoint evidence")
    if (
        type(schema_version) is not int
        or schema_version not in {1, 2, 3}
        or root["evidence_type"] != "vevo_growthbook_aa_window_checkpoint"
        or root["status"] != "passed"
        or root["experiment_id"] != "vevo-sk-aa-001"
        or root["repository"] != "vzeman/biznisweb"
        or root["workflow"] != CHECKPOINT_WORKFLOW
        or RUN_ID_RE.fullmatch(str(root["workflow_run_id"])) is None
        or COMMIT_RE.fullmatch(str(root["main_commit"])) is None
    ):
        raise MeasurementWindowError("checkpoint evidence identity drift")

    observed_at = _parse_utc(root["observed_at_utc"], "checkpoint observed_at_utc")
    candidate_through, due, last_full_date, full_days = _checkpoint_boundaries(
        expected, checkpoint_index
    )
    due_utc = due.astimezone(UTC)
    daily_gate_end_utc = (due + timedelta(days=1)).astimezone(UTC)
    if schema_version in {1, 2}:
        if not due_utc <= observed_at < daily_gate_end_utc:
            raise MeasurementWindowError(
                "checkpoint observation is outside its daily gate"
            )
    else:
        collection_mode = root["collection_mode"]
        if collection_mode in {"scheduled_daily", "manual_same_window"}:
            if not due_utc <= observed_at < daily_gate_end_utc:
                raise MeasurementWindowError(
                    "same-window checkpoint observation is outside its daily gate"
                )
        elif collection_mode == "manual_historical_backfill":
            if observed_at < daily_gate_end_utc:
                raise MeasurementWindowError(
                    "historical backfill was recorded before its daily gate closed"
                )
        else:
            raise MeasurementWindowError("checkpoint collection mode drift")

    window = _exact_object(root["window"], CHECKPOINT_WINDOW_KEYS, "checkpoint window")
    if window != {
        "timezone": expected["timezone"],
        "checkpoint_index": checkpoint_index,
        "from_utc": expected["from_utc"],
        "candidate_through_utc": _utc_text(candidate_through),
        "candidate_last_full_local_date": last_full_date,
        "full_calendar_days": full_days,
        "resolution_due_local": due.isoformat(timespec="seconds"),
    }:
        raise MeasurementWindowError("checkpoint window drift")

    runtime_keys = set(CHECKPOINT_RUNTIME_KEYS)
    if schema_version in {2, 3}:
        runtime_keys.add("identity_source")
    runtime = _exact_object(root["runtime"], runtime_keys, "checkpoint runtime")
    if (
        runtime["instance_id"] != "N/A:Fargate"
        or runtime["service"] != "vevo-growthbook-reconcile-production"
        or runtime["runtime_path"] != "/app"
        or TASK_ID_RE.fullmatch(str(runtime["task_id"])) is None
        or TASK_DEFINITION_RE.fullmatch(str(runtime["task_definition"])) is None
        or IMAGE_DIGEST_RE.fullmatch(str(runtime["image_digest"])) is None
        or runtime["host_gate_evidence_sha256"]
        != "21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb"
        or runtime["localhost_health_marker_inherited_from_deploy_evidence"] is not True
        or runtime["localhost_runtime_marker_inherited_from_deploy_evidence"]
        is not True
    ):
        raise MeasurementWindowError("checkpoint runtime hard gate drift")

    control_keys = set(CHECKPOINT_CONTROL_KEYS)
    if schema_version in {2, 3}:
        control_keys.update({"runtime_state_retained", "scheduler_run_task_verified"})
    control = _exact_object(
        root["control_plane"], control_keys, "checkpoint control plane"
    )
    expected_control = {
        "schedule_name": "vevo-growthbook-reconcile-production",
        "schedule_due_local": due.isoformat(timespec="seconds"),
        "schedule_succeeded": True,
        "success_marker_sha256": control["success_marker_sha256"],
        "publish_summary_sha256": control["publish_summary_sha256"],
        "generated_published_counts_match": True,
        "dlq_empty": True,
        "alarms_clear": True,
        "source_schedule_name": "vevo-daily-report-email",
        "source_schedule_unchanged": True,
    }
    if schema_version in {2, 3}:
        expected_control.update(
            {
                "scheduler_run_task_verified": control["scheduler_run_task_verified"],
                "runtime_state_retained": control["runtime_state_retained"],
            }
        )
    if control != expected_control:
        raise MeasurementWindowError("checkpoint control-plane gate failed")
    for field in ("success_marker_sha256", "publish_summary_sha256"):
        if SHA256_RE.fullmatch(str(control[field])) is None:
            raise MeasurementWindowError("checkpoint control-plane hash drift")

    def validate_private_ip() -> None:
        try:
            private_ip = ipaddress.ip_address(str(runtime["private_ip"]))
        except ValueError as exc:
            raise MeasurementWindowError(
                "checkpoint runtime private IP is invalid"
            ) from exc
        if private_ip.version != 4 or private_ip not in ipaddress.ip_network(
            "172.31.0.0/16"
        ):
            raise MeasurementWindowError("checkpoint runtime private IP boundary drift")

    if schema_version == 1:
        validate_private_ip()
    else:
        identity_source = runtime["identity_source"]
        if identity_source not in {
            "ecs_stopped_task",
            "cloudtrail_run_task_retention_recovery",
        }:
            raise MeasurementWindowError("checkpoint runtime identity source drift")
        if control["scheduler_run_task_verified"] is not True:
            raise MeasurementWindowError(
                "checkpoint Scheduler RunTask verification drift"
            )
        if identity_source == "ecs_stopped_task":
            validate_private_ip()
            if control["runtime_state_retained"] is not True:
                raise MeasurementWindowError("checkpoint runtime retention drift")
        else:
            if runtime["private_ip"] is not None:
                raise MeasurementWindowError(
                    "checkpoint expired runtime private IP must be null"
                )
            if control["runtime_state_retained"] is not False:
                raise MeasurementWindowError("checkpoint runtime retention drift")

    population = _exact_object(
        root["population"], CHECKPOINT_POPULATION_KEYS, "checkpoint population"
    )
    eligible_devices = population["eligible_devices"]
    if type(eligible_devices) is not int or eligible_devices < 0:
        raise MeasurementWindowError("checkpoint eligible-device count is invalid")
    if population != {
        "metric": expected["stopping_rule_population_metric"],
        "eligible_devices": eligible_devices,
        "database": "vevo_growthbook_production",
        "workgroup": "vevo-growthbook-reporting-production",
        "source_table": "experiment_device_facts",
        "aggregate_query_sha256": population["aggregate_query_sha256"],
        "aggregate_result_sha256": population["aggregate_result_sha256"],
        "only_aggregate_count_retained": True,
        "arm_counts_read": False,
        "arm_outcomes_read": False,
        "outcome_metrics_read": False,
        "contains_event_or_device_ids": False,
        "contains_customer_or_order_data": False,
    }:
        raise MeasurementWindowError(
            "checkpoint outcome-blind population boundary drift"
        )
    for field in ("aggregate_query_sha256", "aggregate_result_sha256"):
        if SHA256_RE.fullmatch(str(population[field])) is None:
            raise MeasurementWindowError("checkpoint aggregate hash drift")

    expected_decision = (
        "resolve"
        if eligible_devices >= int(expected["minimum_eligible_devices"])
        else "extend_one_full_local_day"
    )
    if root["decision"] != expected_decision:
        raise MeasurementWindowError("checkpoint stopping-rule decision drift")
    if root["safety"] != CHECKPOINT_SAFETY:
        raise MeasurementWindowError("checkpoint safety boundary drift")


def validate_checkpoint_history(
    history: object, expected: Mapping[str, Any]
) -> list[Mapping[str, Any]]:
    if not isinstance(history, list):
        raise MeasurementWindowError("checkpoint history must be an array")
    validated: list[Mapping[str, Any]] = []
    resolved = False
    for index, item in enumerate(history, start=1):
        record = _exact_object(
            item, {"evidence_sha256", "evidence"}, f"checkpoint history item {index}"
        )
        evidence = record["evidence"]
        if not isinstance(evidence, dict):
            raise MeasurementWindowError(
                "checkpoint history evidence must be an object"
            )
        digest = hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest()
        if record["evidence_sha256"] != digest:
            raise MeasurementWindowError("checkpoint evidence SHA-256 mismatch")
        validate_checkpoint_evidence(evidence, expected, index)
        if resolved:
            raise MeasurementWindowError("checkpoint exists after window resolution")
        resolved = evidence["decision"] == "resolve"
        validated.append(record)
    return validated


def validate_measurement_window(
    manifest: Mapping[str, Any],
    activation: Mapping[str, Any],
    acceptance: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
) -> None:
    if manifest.get("schema_version") != 2:
        raise MeasurementWindowError("A/A snapshot manifest schema drift")
    if manifest.get("experiment_id") != "vevo-sk-aa-001":
        raise MeasurementWindowError("A/A snapshot experiment drift")
    expected = expected_measurement_window(activation, acceptance, reconciliation)
    actual = manifest.get("measurement_window")
    if not isinstance(actual, dict) or set(actual) != set(expected):
        raise MeasurementWindowError("A/A pre-registered measurement window drift")
    for key in set(expected) - MUTABLE_WINDOW_KEYS:
        if actual[key] != expected[key]:
            raise MeasurementWindowError("A/A pre-registered measurement window drift")

    history = validate_checkpoint_history(actual["checkpoint_history"], expected)
    resolution_evidence = history[-1]["evidence"] if history else None
    is_resolved = bool(
        resolution_evidence and resolution_evidence["decision"] == "resolve"
    )
    if is_resolved:
        resolution_window = resolution_evidence["window"]
        population = resolution_evidence["population"]
        expected_lifecycle = {
            "status": "resolved_by_preregistered_sample_stopping_rule",
            "resolution_status": "resolved",
            "resolved_last_full_local_date": resolution_window[
                "candidate_last_full_local_date"
            ],
            "resolved_through_utc": resolution_window["candidate_through_utc"],
            "resolved_full_calendar_days": resolution_window["full_calendar_days"],
            "resolved_eligible_devices": population["eligible_devices"],
            "resolved_at_utc": resolution_evidence["observed_at_utc"],
        }
    else:
        expected_lifecycle = {
            key: expected[key]
            for key in MUTABLE_WINDOW_KEYS
            if key != "checkpoint_history"
        }
    if any(actual[key] != value for key, value in expected_lifecycle.items()):
        raise MeasurementWindowError("A/A measurement-window lifecycle drift")

    for component_name in ("automated_evidence", "manual_qa_evidence"):
        component = manifest.get(component_name) or {}
        expected_through = actual["resolved_through_utc"] if is_resolved else None
        if (
            component.get("from_utc") != expected["from_utc"]
            or component.get("through_utc") != expected_through
        ):
            raise MeasurementWindowError(
                f"{component_name} differs from deterministic window resolution"
            )

    automated = manifest.get("automated_evidence") or {}
    manual = manifest.get("manual_qa_evidence") or {}
    components = (automated, manual)
    for component_name, component in zip(
        ("automated_evidence", "manual_qa_evidence"), components, strict=True
    ):
        status = component.get("status")
        if status == "not_recorded":
            if any(
                component.get(field) is not None
                for field in ("run_id", "main_commit", "sha256")
            ):
                raise MeasurementWindowError(
                    f"{component_name} has provenance before artifact verification"
                )
        elif status == "verified":
            if (
                RUN_ID_RE.fullmatch(str(component.get("run_id") or "")) is None
                or COMMIT_RE.fullmatch(str(component.get("main_commit") or "")) is None
                or SHA256_RE.fullmatch(str(component.get("sha256") or "")) is None
            ):
                raise MeasurementWindowError(
                    f"{component_name} verified artifact provenance is invalid"
                )
            if component.get("producer_allowed") is not False:
                raise MeasurementWindowError(
                    f"{component_name} producer remained open after artifact verification"
                )
        else:
            raise MeasurementWindowError(f"{component_name} artifact status drift")

    if not is_resolved:
        if manifest.get("snapshot_build_allowed") is not False:
            raise MeasurementWindowError(
                "snapshot gate opened before window resolution"
            )
        expected_automated = {
            "producer_allowed": False,
            "window_status": "frozen_waiting_for_completion",
            "quality_report_status": "not_recorded",
            "quality_report_key": None,
            "quality_report_sha256": None,
            "status": "not_recorded",
        }
        expected_manual = {
            "producer_allowed": False,
            "window_status": "frozen_waiting_for_completion",
            "observation_status": "not_recorded",
            "observation_sha256": None,
            "status": "not_recorded",
        }
        for component, expected_state, component_name in (
            (automated, expected_automated, "automated_evidence"),
            (manual, expected_manual, "manual_qa_evidence"),
        ):
            if any(
                component.get(key) != value for key, value in expected_state.items()
            ):
                raise MeasurementWindowError(
                    f"{component_name} opened before deterministic window resolution"
                )
        return

    automated_opened = automated.get("quality_report_status") == (
        "verified_canonical_reporting_quality"
    )
    if automated_opened:
        if (
            automated.get("window_status")
            != "verified_complete_reconciled_production_aa"
            or QUALITY_KEY_RE.fullmatch(str(automated.get("quality_report_key") or ""))
            is None
            or SHA256_RE.fullmatch(str(automated.get("quality_report_sha256") or ""))
            is None
        ):
            raise MeasurementWindowError("automated evidence producer gate drift")
    elif (
        automated.get("quality_report_status") != "not_recorded"
        or automated.get("quality_report_key") is not None
        or automated.get("quality_report_sha256") is not None
        or automated.get("window_status")
        != "resolved_waiting_for_reviewed_producer_open"
    ):
        raise MeasurementWindowError("automated evidence producer gate drift")

    manual_opened = manual.get("observation_status") == ("verified_reviewed_browser_qa")
    if manual_opened:
        if (
            manual.get("window_status") != "verified_complete_reconciled_production_aa"
            or SHA256_RE.fullmatch(str(manual.get("observation_sha256") or "")) is None
        ):
            raise MeasurementWindowError("manual QA evidence producer gate drift")
    elif (
        manual.get("observation_status") != "not_recorded"
        or manual.get("observation_sha256") is not None
        or manual.get("window_status") != "resolved_waiting_for_reviewed_producer_open"
    ):
        raise MeasurementWindowError("manual QA evidence producer gate drift")

    for opened, component, component_name in (
        (automated_opened, automated, "automated_evidence"),
        (manual_opened, manual, "manual_qa_evidence"),
    ):
        if component.get("status") == "verified" and not opened:
            raise MeasurementWindowError(
                f"{component_name} artifact exists without a reviewed producer source"
            )
        expected_producer = opened and component.get("status") == "not_recorded"
        if component.get("producer_allowed") is not expected_producer:
            raise MeasurementWindowError(f"{component_name} producer lifecycle drift")

    expected_snapshot_gate = all(
        component.get("status") == "verified" for component in components
    )
    if manifest.get("snapshot_build_allowed") is not expected_snapshot_gate:
        raise MeasurementWindowError(
            "snapshot gate differs from component verification"
        )


def validate() -> None:
    validate_measurement_window(
        _load(SNAPSHOT_PATH),
        _load(ACTIVATION_PATH),
        _load(ACCEPTANCE_PATH),
        _load(RECONCILIATION_EVIDENCE_PATH),
    )


def main() -> int:
    try:
        validate()
        print("validate_growthbook_aa_measurement_window.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(
            f"validate_growthbook_aa_measurement_window.py: FAIL: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
