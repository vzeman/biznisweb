#!/usr/bin/env python3
"""Validate sanitized VEVO Production A/A infrastructure-health evidence."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.record_growthbook_natural_evidence import (  # noqa: E402
    canonical_evidence_bytes,
)


EXPECTED_ALARMS = {
    "vevo-growthbook-reconcile-production-dlq",
    "vevo-growthbook-reconcile-production-failure",
    "vevo-growthbook-reconcile-production-missing-success",
}
EXPECTED_FIRST_DUE_LOCAL = "2026-08-26T03:45:00+02:00"
EXPECTED_SCHEDULE = "vevo-growthbook-reconcile-production"
EXPECTED_SOURCE_SCHEDULE = "vevo-daily-report-email"
HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
RUN_ID_RE = re.compile(r"^[1-9][0-9]*$")
TASK_ID_RE = re.compile(r"^[0-9a-f]{32}$")


class InfraHealthEvidenceError(ValueError):
    """Raised when infrastructure-health evidence is unsafe or inconsistent."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InfraHealthEvidenceError(message)


def _exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    _require(set(value) == expected, f"{field} keys drift")


def _parse_timestamp(value: Any, field: str) -> datetime:
    text = str(value or "")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InfraHealthEvidenceError(f"{field} is invalid") from exc
    _require(parsed.tzinfo is not None, f"{field} must be timezone-aware")
    return parsed


def _require_all_false(value: Mapping[str, Any], field: str) -> None:
    _require(value and all(item is False for item in value.values()), f"{field} drift")


def validate_health_evidence(
    evidence: Mapping[str, Any],
    deploy_evidence: Mapping[str, Any],
    *,
    deploy_evidence_bytes: bytes,
) -> None:
    """Validate one canonical, identity-free infrastructure-health snapshot."""

    _exact_keys(
        evidence,
        {
            "aws",
            "boundaries",
            "control",
            "environment",
            "evidence_type",
            "observed_at_utc",
            "phase",
            "privacy",
            "provenance",
            "runtime",
            "schema_version",
        },
        "evidence",
    )
    schema_version = evidence["schema_version"]
    _require(schema_version in {1, 2}, "schema version drift")
    _require(
        evidence["evidence_type"]
        == "vevo_growthbook_production_aa_infra_health",
        "evidence type drift",
    )
    _require(evidence["environment"] == "production", "environment drift")
    _parse_timestamp(evidence["observed_at_utc"], "observed_at_utc")

    provenance = evidence["provenance"]
    _exact_keys(
        provenance,
        {"deploy_evidence_sha256", "main_commit", "workflow", "workflow_run_id"},
        "provenance",
    )
    _require(
        provenance["workflow"]
        == ".github/workflows/monitor-vevo-growthbook-production-aa-infra.yml",
        "workflow provenance drift",
    )
    _require(
        RUN_ID_RE.fullmatch(str(provenance["workflow_run_id"] or "")) is not None,
        "workflow run ID drift",
    )
    _require(
        SHA_RE.fullmatch(str(provenance["main_commit"] or "")) is not None,
        "main commit drift",
    )
    expected_deploy_sha = hashlib.sha256(deploy_evidence_bytes).hexdigest()
    _require(
        provenance["deploy_evidence_sha256"] == expected_deploy_sha,
        "deploy evidence SHA-256 drift",
    )
    _require(
        expected_deploy_sha
        == "21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb",
        "unexpected checked-in deploy evidence",
    )

    aws = evidence["aws"]
    _exact_keys(aws, {"account_id", "collector_stack", "reconciliation_stack", "region"}, "aws")
    _require(aws == deploy_evidence["aws"], "AWS identity drift")

    phase = evidence["phase"]
    _exact_keys(
        phase,
        {"checked_due_local", "first_natural_run_due_local", "status"},
        "phase",
    )
    _require(
        phase["first_natural_run_due_local"] == EXPECTED_FIRST_DUE_LOCAL,
        "first natural run due drift",
    )

    runtime = evidence["runtime"]
    runtime_keys = {
            "image_digest",
            "instance_id",
            "localhost_marker_source",
            "private_ip",
            "runtime_path",
            "service",
            "task_definition",
            "task_id",
    }
    if schema_version == 2:
        runtime_keys.add("identity_source")
    _exact_keys(runtime, runtime_keys, "runtime")
    expected_task_definition = deploy_evidence["reconciliation"]["task_definition"].rsplit("/", 1)[-1]
    _require(runtime["instance_id"] == "N/A:Fargate", "instance marker drift")
    _require(runtime["service"] == EXPECTED_SCHEDULE, "runtime service drift")
    _require(runtime["runtime_path"] == "/app", "runtime path drift")
    _require(runtime["task_definition"] == expected_task_definition, "task definition drift")
    _require(
        runtime["image_digest"] == deploy_evidence["reconciliation"]["image_digest"],
        "image digest drift",
    )
    _require(
        runtime["localhost_marker_source"]
        == "hash_bound_deploy_evidence_host_gate",
        "localhost marker source drift",
    )

    control = evidence["control"]
    control_keys = {
            "alarm_states",
            "alarms_clear",
            "dlq_empty",
            "generated_published_parity_verified",
            "publish_summary_sha256",
            "schedule_expression",
            "schedule_name",
            "schedule_state",
            "schedule_succeeded",
            "schedule_timezone",
            "source_schedule_name",
            "source_schedule_state",
            "source_schedule_unchanged",
            "source_task_definition",
            "success_marker_sha256",
    }
    if schema_version == 2:
        control_keys.update({"runtime_state_retained", "scheduler_run_task_verified"})
    _exact_keys(control, control_keys, "control")
    _require(control["schedule_name"] == EXPECTED_SCHEDULE, "schedule name drift")
    _require(control["schedule_state"] == "ENABLED", "schedule state drift")
    _require(control["schedule_expression"] == "cron(45 3 * * ? *)", "schedule expression drift")
    _require(control["schedule_timezone"] == "Europe/Bratislava", "schedule timezone drift")
    _require(set(control["alarm_states"]) == EXPECTED_ALARMS, "alarm identity drift")
    _require(
        all(state in {"OK", "INSUFFICIENT_DATA"} for state in control["alarm_states"].values()),
        "an alarm is active",
    )
    _require(control["alarms_clear"] is True, "alarm clear gate failed")
    _require(control["dlq_empty"] is True, "DLQ gate failed")
    _require(control["source_schedule_name"] == EXPECTED_SOURCE_SCHEDULE, "source schedule name drift")
    _require(control["source_schedule_state"] == "ENABLED", "source schedule state drift")
    _require(
        control["source_task_definition"]
        == deploy_evidence["source_runtime"]["task_definition"].rsplit("/", 1)[-1],
        "source task definition drift",
    )
    _require(control["source_schedule_unchanged"] is True, "source schedule drift")

    if phase["status"] == "waiting_for_first_natural_run":
        _require(phase["checked_due_local"] is None, "pre-first-run due must be null")
        _require(runtime["task_id"] is None, "pre-first-run task ID must be null")
        _require(runtime["private_ip"] is None, "pre-first-run private IP must be null")
        _require(control["schedule_succeeded"] is False, "pre-first-run success drift")
        _require(control["success_marker_sha256"] is None, "pre-first-run marker drift")
        _require(control["publish_summary_sha256"] is None, "pre-first-run summary drift")
        _require(
            control["generated_published_parity_verified"] is None,
            "pre-first-run parity drift",
        )
        if schema_version == 2:
            _require(runtime["identity_source"] is None, "pre-first-run identity source drift")
            _require(
                control["scheduler_run_task_verified"] is False,
                "pre-first-run Scheduler evidence drift",
            )
            _require(
                control["runtime_state_retained"] is None,
                "pre-first-run runtime retention drift",
            )
    elif phase["status"] == "natural_reconciliation_verified":
        _parse_timestamp(phase["checked_due_local"], "checked_due_local")
        _require(
            TASK_ID_RE.fullmatch(str(runtime["task_id"] or "")) is not None,
            "scheduled task ID drift",
        )
        if schema_version == 1 or runtime["private_ip"] is not None:
            try:
                private_ip = ipaddress.ip_address(str(runtime["private_ip"] or ""))
            except ValueError as exc:
                raise InfraHealthEvidenceError("scheduled task private IP invalid") from exc
            _require(
                private_ip in ipaddress.ip_network("172.31.0.0/16"),
                "scheduled task private IP drift",
            )
        _require(control["schedule_succeeded"] is True, "schedule success drift")
        _require(
            HEX_64_RE.fullmatch(str(control["success_marker_sha256"] or "")) is not None,
            "success marker hash drift",
        )
        _require(
            HEX_64_RE.fullmatch(str(control["publish_summary_sha256"] or "")) is not None,
            "publish summary hash drift",
        )
        _require(
            control["generated_published_parity_verified"] is True,
            "generated/published parity drift",
        )
        _require(
            set(control["alarm_states"].values()) == {"OK"},
            "post-run alarms must all be OK",
        )
        if schema_version == 2:
            identity_source = runtime["identity_source"]
            _require(
                identity_source
                in {"ecs_stopped_task", "cloudtrail_run_task_retention_recovery"},
                "runtime identity source drift",
            )
            _require(
                control["scheduler_run_task_verified"] is True,
                "Scheduler RunTask evidence drift",
            )
            if identity_source == "ecs_stopped_task":
                _require(runtime["private_ip"] is not None, "retained runtime IP missing")
                _require(control["runtime_state_retained"] is True, "runtime retention drift")
            else:
                _require(runtime["private_ip"] is None, "expired runtime IP must be null")
                _require(
                    control["runtime_state_retained"] is False,
                    "expired runtime retention drift",
                )
    else:
        raise InfraHealthEvidenceError("phase status drift")

    privacy = evidence["privacy"]
    _exact_keys(
        privacy,
        {
            "arm_assignment_read",
            "contains_cloudwatch_messages",
            "contains_credentials",
            "contains_event_device_customer_or_order_ids",
            "contains_raw_aws_payloads",
            "experimental_population_read",
            "meta_dimensions_read",
            "outcome_metrics_read",
            "performance_values_read",
            "reporting_row_counts_emitted",
        },
        "privacy",
    )
    _require_all_false(privacy, "privacy boundary")

    boundaries = evidence["boundaries"]
    _exact_keys(
        boundaries,
        {
            "aws_resource_mutated",
            "biznisweb_mutated",
            "collector_or_reporting_mutated",
            "growthbook_mutated",
            "gtm_mutated",
            "meta_ads_mutated",
            "price_product_stock_cart_checkout_payment_or_order_mutated",
            "workflow_or_experiment_gate_changed",
        },
        "boundaries",
    )
    _require_all_false(boundaries, "mutation boundary")


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InfraHealthEvidenceError(f"cannot read {path}") from exc
    _require(isinstance(value, dict), f"{path} must contain an object")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument(
        "--deploy-evidence",
        type=Path,
        default=Path("projects/vevo/growthbook_production_reconciliation_deploy_evidence.json"),
    )
    args = parser.parse_args(argv)
    try:
        evidence_bytes = args.evidence.read_bytes()
        evidence = _read_json(args.evidence)
        deploy_bytes = args.deploy_evidence.read_bytes()
        deploy_evidence = _read_json(args.deploy_evidence)
        _require(
            evidence_bytes == canonical_evidence_bytes(evidence),
            "health evidence is not canonical JSON",
        )
        validate_health_evidence(
            evidence,
            deploy_evidence,
            deploy_evidence_bytes=deploy_bytes,
        )
    except (OSError, InfraHealthEvidenceError) as exc:
        print(f"validate_growthbook_aa_infra_health_evidence.py: FAIL: {exc}")
        return 1
    print("validate_growthbook_aa_infra_health_evidence.py: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
