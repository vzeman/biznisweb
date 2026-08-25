#!/usr/bin/env python3
"""Validate the pre-registered VEVO Production A/A measurement window."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
ACTIVATION_PATH = ROOT / "projects" / "vevo" / "growthbook_production_aa_activation.json"
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
    if (
        binding.get("workflow_run_id") != reconciliation.get("source_run_id")
        or binding.get("main_commit") != reconciliation.get("source_main_commit")
    ):
        raise MeasurementWindowError("Production reconciliation provenance drift")

    return {
        "status": "frozen_start_and_stopping_rule_before_outcome_readback",
        "timezone": timezone_name,
        "activation_observed_at_utc": _utc_text(observed_at),
        "source_activation_workflow_run_id": readback.get("workflow_run_id"),
        "source_activation_main_commit": readback.get("main_commit"),
        "source_reconciliation_workflow_run_id": reconciliation.get("source_run_id"),
        "source_reconciliation_main_commit": reconciliation.get("source_main_commit"),
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
        "post_hoc_window_change_allowed": False,
    }


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
    if manifest.get("measurement_window") != expected:
        raise MeasurementWindowError("A/A pre-registered measurement window drift")

    for component_name in ("automated_evidence", "manual_qa_evidence"):
        component = manifest.get(component_name) or {}
        if (
            component.get("from_utc") != expected["from_utc"]
            or component.get("through_utc") is not None
        ):
            raise MeasurementWindowError(
                f"{component_name} opened before deterministic window resolution"
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
        print(f"validate_growthbook_aa_measurement_window.py: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
