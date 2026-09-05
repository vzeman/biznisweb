#!/usr/bin/env python3
"""Record reviewed VEVO Production A/A evidence-gate transitions offline.

The three supported transitions are deliberately separate:

* ``open-automated`` binds the exact canonical reporting-quality object after
  the pre-registered A/A window resolves.
* ``open-manual`` binds the exact reviewed browser-QA observation for that
  same window.
* ``record-component`` binds one independently downloaded successful workflow
  artifact to its run, main commit, and SHA-256. The final snapshot gate opens
  only after both components are verified.

This module has no AWS, GitHub, GrowthBook, GTM, Meta Ads, BiznisWeb, browser,
commerce, or network client. Every transition is fail-closed, canonical,
atomic, idempotent, and must be reviewed as a Git diff before merge.
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
    from scripts.assemble_growthbook_aa_snapshot import (
        AUTOMATED_KEYS,
        MANUAL_KEYS,
        SnapshotAssemblyError,
        _exact,
        _load_canonical,
        _validate_manual,
        _validate_nested_automated,
        _validate_window,
    )
    from scripts.build_growthbook_aa_manual_qa_evidence import (
        ManualQaEvidenceError,
        build_manual_qa_evidence,
        load_canonical_observation,
    )
    from scripts.validate_growthbook_aa_measurement_window import (
        ACCEPTANCE_PATH,
        ACTIVATION_PATH,
        RECONCILIATION_EVIDENCE_PATH,
        SNAPSHOT_PATH,
        MeasurementWindowError,
        canonical_evidence_bytes,
        validate_measurement_window,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from assemble_growthbook_aa_snapshot import (
        AUTOMATED_KEYS,
        MANUAL_KEYS,
        SnapshotAssemblyError,
        _exact,
        _load_canonical,
        _validate_manual,
        _validate_nested_automated,
        _validate_window,
    )
    from build_growthbook_aa_manual_qa_evidence import (
        ManualQaEvidenceError,
        build_manual_qa_evidence,
        load_canonical_observation,
    )
    from validate_growthbook_aa_measurement_window import (
        ACCEPTANCE_PATH,
        ACTIVATION_PATH,
        RECONCILIATION_EVIDENCE_PATH,
        SNAPSHOT_PATH,
        MeasurementWindowError,
        canonical_evidence_bytes,
        validate_measurement_window,
    )


ROOT = Path(__file__).resolve().parents[1]
# Remains false until this recorder AND the automated consumer require the
# managed exact-window capture. The prepared source workflow checks this before
# configuring AWS credentials; deploying its code alone cannot authorize reads.
EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED = False
RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
QUALITY_KEY_RE = re.compile(
    r"^experiment-events/curated/quality/experiment_id=vevo-sk-aa-001/"
    r"facts_generated_at=(20[2-9][0-9]{5}T[0-9]{6}Z)[.]json$"
)
QUALITY_KEYS = {
    "metric_contract_version",
    "experiment_id",
    "facts_generated_at",
    "raw_event_count",
    "unique_event_count",
    "duplicate_event_count",
    "orphan_event_count",
    "exposed_device_count",
    "eligible_device_count",
    "contaminated_device_count",
    "variation_counts",
    "srm_chi_square",
    "srm_p_value",
    "srm_alert",
    "unique_transaction_count",
    "exact_joined_transaction_count",
    "exact_join_rate_pct",
    "unmatched_transaction_count",
    "ambiguous_transaction_count",
    "attributed_transaction_count",
    "performance_duplicate_count",
    "variation_health",
}
QUALITY_HEALTH_KEYS = {
    "eligible_devices",
    "client_error_devices",
    "client_error_device_rate_pct",
    "measured_page_loads",
    "lcp_p75_ms",
    "inp_p75_ms",
    "cls_p75_milli",
}
VARIATIONS = {"control", "variant"}


class EvidenceGateRecordingError(ValueError):
    """Raised when a reviewed evidence-gate transition is unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceGateRecordingError(message)


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceGateRecordingError(f"{path.name} is unreadable") from exc
    _require(isinstance(value, dict), f"{path.name} must contain an object")
    return value


def _integer(value: Any, field: str) -> int:
    _require(
        type(value) is int and value >= 0, f"{field} must be a non-negative integer"
    )
    return value


def _number(
    value: Any, field: str, *, nullable: bool = False, maximum: float | None = None
) -> float | None:
    if value is None and nullable:
        return None
    _require(type(value) in (int, float), f"{field} must be numeric")
    result = float(value)
    _require(
        result == result and result not in (float("inf"), float("-inf")),
        f"{field} must be finite",
    )
    _require(result >= 0, f"{field} must be non-negative")
    if maximum is not None:
        _require(result <= maximum, f"{field} exceeds its maximum")
    return result


def _load_canonical_mapping(
    path: Path, expected_sha256: str, field: str
) -> dict[str, Any]:
    digest = str(expected_sha256 or "").strip()
    _require(SHA256_RE.fullmatch(digest) is not None, f"{field} SHA-256 is invalid")
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceGateRecordingError(f"{field} is unreadable") from exc
    _require(isinstance(value, dict), f"{field} must contain an object")
    _require(
        raw == canonical_evidence_bytes(value),
        f"{field} must use canonical JSON bytes",
    )
    _require(
        hashlib.sha256(raw).hexdigest() == digest,
        f"independently supplied {field} SHA-256 mismatch",
    )
    return value


def _validate_source_manifest(snapshot: Mapping[str, Any]) -> None:
    try:
        validate_measurement_window(
            snapshot,
            _load(ACTIVATION_PATH),
            _load(ACCEPTANCE_PATH),
            _load(RECONCILIATION_EVIDENCE_PATH),
        )
    except MeasurementWindowError as exc:
        raise EvidenceGateRecordingError(f"source manifest is invalid: {exc}") from exc


def _require_resolved_window(snapshot: Mapping[str, Any]) -> tuple[str, str]:
    _validate_source_manifest(snapshot)
    window = snapshot.get("measurement_window") or {}
    _require(
        window.get("resolution_status") == "resolved", "A/A window is not resolved"
    )
    from_utc = str(window.get("from_utc") or "")
    through_utc = str(window.get("resolved_through_utc") or "")
    _require(
        UTC_RE.fullmatch(from_utc) is not None
        and UTC_RE.fullmatch(through_utc) is not None
        and through_utc > from_utc,
        "resolved A/A window is invalid",
    )
    return from_utc, through_utc


def validate_quality_report(
    quality: Mapping[str, Any],
    *,
    quality_report_key: str,
    resolved_through_utc: str,
    resolved_eligible_devices: int,
) -> None:
    _require(set(quality) == QUALITY_KEYS, "quality report field set drift")
    _require(
        quality["metric_contract_version"] == "vevo_cm1_v1_2026-08-20",
        "quality report metric contract drift",
    )
    _require(
        quality["experiment_id"] == "vevo-sk-aa-001", "quality report experiment drift"
    )
    generated_text = str(quality["facts_generated_at"] or "")
    _require(
        UTC_RE.fullmatch(generated_text) is not None,
        "quality report timestamp is invalid",
    )
    generated_at = datetime.fromisoformat(generated_text.replace("Z", "+00:00"))
    through = datetime.fromisoformat(resolved_through_utc.replace("Z", "+00:00"))
    _require(generated_at >= through, "quality report predates the resolved A/A window")
    key_match = QUALITY_KEY_RE.fullmatch(quality_report_key)
    _require(key_match is not None, "quality report object key is invalid")
    expected_marker = generated_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    _require(
        key_match.group(1) == expected_marker, "quality report key timestamp drift"
    )

    integer_fields = QUALITY_KEYS - {
        "metric_contract_version",
        "experiment_id",
        "facts_generated_at",
        "variation_counts",
        "srm_chi_square",
        "srm_p_value",
        "srm_alert",
        "exact_join_rate_pct",
        "variation_health",
    }
    for field in integer_fields:
        _integer(quality[field], f"quality report {field}")
    _require(
        type(quality["srm_alert"]) is bool, "quality report srm_alert must be boolean"
    )
    _number(quality["srm_chi_square"], "quality report srm_chi_square")
    _number(quality["srm_p_value"], "quality report srm_p_value", maximum=1)
    _number(
        quality["exact_join_rate_pct"],
        "quality report exact_join_rate_pct",
        nullable=True,
        maximum=100,
    )

    variation_counts = quality["variation_counts"]
    _require(
        isinstance(variation_counts, dict) and set(variation_counts) == VARIATIONS,
        "quality report variation counts drift",
    )
    for variation in sorted(VARIATIONS):
        _integer(variation_counts[variation], f"variation_counts.{variation}")
    _require(
        sum(variation_counts.values()) == quality["exposed_device_count"],
        "quality report exposed-device identity drift",
    )
    _require(
        quality["raw_event_count"]
        == quality["unique_event_count"] + quality["duplicate_event_count"],
        "quality report raw/unique/duplicate identity drift",
    )

    health = quality["variation_health"]
    _require(
        isinstance(health, dict) and set(health) == VARIATIONS,
        "quality report variation health drift",
    )
    eligible_sum = 0
    for variation in sorted(VARIATIONS):
        row = health[variation]
        _require(
            isinstance(row, dict) and set(row) == QUALITY_HEALTH_KEYS,
            f"variation_health.{variation} field set drift",
        )
        eligible = _integer(
            row["eligible_devices"], f"variation_health.{variation}.eligible_devices"
        )
        errors = _integer(
            row["client_error_devices"],
            f"variation_health.{variation}.client_error_devices",
        )
        _require(
            errors <= eligible,
            f"variation_health.{variation} client errors exceed devices",
        )
        _integer(
            row["measured_page_loads"],
            f"variation_health.{variation}.measured_page_loads",
        )
        rate = _number(
            row["client_error_device_rate_pct"],
            f"variation_health.{variation}.client_error_device_rate_pct",
            nullable=True,
            maximum=100,
        )
        expected_rate = round(100.0 * errors / eligible, 4) if eligible else None
        _require(
            rate == expected_rate, f"variation_health.{variation} error-rate drift"
        )
        for field in ("lcp_p75_ms", "inp_p75_ms", "cls_p75_milli"):
            _number(row[field], f"variation_health.{variation}.{field}", nullable=True)
        eligible_sum += eligible
    _require(
        eligible_sum == quality["eligible_device_count"],
        "quality report eligible-device identity drift",
    )
    _require(
        quality["eligible_device_count"] == resolved_eligible_devices,
        "quality report eligible devices differ from the stopping checkpoint",
    )
    _require(
        quality["eligible_device_count"] <= quality["exposed_device_count"]
        and quality["contaminated_device_count"] <= quality["exposed_device_count"],
        "quality report device counts are inconsistent",
    )
    unique_transactions = quality["unique_transaction_count"]
    exact_joined = quality["exact_joined_transaction_count"]
    _require(
        exact_joined <= unique_transactions, "quality report joined transaction drift"
    )
    expected_join_rate = (
        round(100.0 * exact_joined / unique_transactions, 4)
        if unique_transactions
        else None
    )
    _require(
        quality["exact_join_rate_pct"] == expected_join_rate,
        "quality report exact join rate drift",
    )
    _require(
        quality["attributed_transaction_count"] <= exact_joined,
        "quality report attributed transaction drift",
    )


def open_automated_producer(
    snapshot: Mapping[str, Any],
    quality: Mapping[str, Any],
    *,
    quality_report_key: str,
    quality_report_sha256: str,
) -> dict[str, Any]:
    """Open only the automated producer after exact quality-source review."""

    from_utc, through_utc = _require_resolved_window(snapshot)
    component = snapshot["automated_evidence"]
    expected_digest = str(quality_report_sha256 or "").strip()
    _require(
        SHA256_RE.fullmatch(expected_digest) is not None,
        "quality report SHA-256 is invalid",
    )
    _require(
        hashlib.sha256(canonical_evidence_bytes(quality)).hexdigest()
        == expected_digest,
        "quality report SHA-256 mismatch",
    )
    validate_quality_report(
        quality,
        quality_report_key=quality_report_key,
        resolved_through_utc=through_utc,
        resolved_eligible_devices=snapshot["measurement_window"][
            "resolved_eligible_devices"
        ],
    )
    expected_open_state = {
        "producer_allowed": True,
        "window_status": "verified_complete_reconciled_production_aa",
        "from_utc": from_utc,
        "through_utc": through_utc,
        "quality_report_status": "verified_canonical_reporting_quality",
        "quality_report_key": quality_report_key,
        "quality_report_sha256": expected_digest,
    }
    if all(component.get(key) == value for key, value in expected_open_state.items()):
        return copy.deepcopy(snapshot)
    _require(
        component.get("producer_allowed") is False, "automated producer is already open"
    )
    _require(
        component.get("window_status") == "resolved_waiting_for_reviewed_producer_open",
        "automated producer window state drift",
    )
    _require(
        component.get("quality_report_status") == "not_recorded"
        and component.get("quality_report_key") is None
        and component.get("quality_report_sha256") is None,
        "automated quality source was already recorded",
    )
    _require(
        component.get("status") == "not_recorded"
        and all(
            component.get(field) is None
            for field in ("run_id", "main_commit", "sha256")
        ),
        "automated artifact was already recorded",
    )
    recorded = copy.deepcopy(snapshot)
    recorded["automated_evidence"].update(expected_open_state)
    _validate_source_manifest(recorded)
    _require(recorded["snapshot_build_allowed"] is False, "snapshot gate opened early")
    return recorded


def open_manual_producer(
    snapshot: Mapping[str, Any],
    observation: Mapping[str, Any],
    *,
    observation_sha256: str,
) -> dict[str, Any]:
    """Open only the manual producer after exact reviewed browser-QA binding."""

    from_utc, through_utc = _require_resolved_window(snapshot)
    digest = str(observation_sha256 or "").strip()
    _require(
        SHA256_RE.fullmatch(digest) is not None,
        "manual QA observation SHA-256 is invalid",
    )
    _require(
        hashlib.sha256(canonical_evidence_bytes(observation)).hexdigest() == digest,
        "manual QA observation SHA-256 mismatch",
    )
    try:
        build_manual_qa_evidence(
            observation,
            workflow_run_id="999999",
            main_commit="0" * 40,
        )
    except (ManualQaEvidenceError, SnapshotAssemblyError) as exc:
        raise EvidenceGateRecordingError(
            f"manual QA observation is invalid: {exc}"
        ) from exc
    _require(
        observation.get("from_utc") == from_utc
        and observation.get("through_utc") == through_utc,
        "manual QA observation differs from the resolved A/A window",
    )
    component = snapshot["manual_qa_evidence"]
    expected_open_state = {
        "producer_allowed": True,
        "window_status": "verified_complete_reconciled_production_aa",
        "from_utc": from_utc,
        "through_utc": through_utc,
        "observation_status": "verified_reviewed_browser_qa",
        "observation_sha256": digest,
    }
    if all(component.get(key) == value for key, value in expected_open_state.items()):
        return copy.deepcopy(snapshot)
    _require(
        component.get("producer_allowed") is False, "manual QA producer is already open"
    )
    _require(
        component.get("window_status") == "resolved_waiting_for_reviewed_producer_open",
        "manual QA producer window state drift",
    )
    _require(
        component.get("observation_status") == "not_recorded"
        and component.get("observation_sha256") is None,
        "manual QA observation was already recorded",
    )
    _require(
        component.get("status") == "not_recorded"
        and all(
            component.get(field) is None
            for field in ("run_id", "main_commit", "sha256")
        ),
        "manual QA artifact was already recorded",
    )
    recorded = copy.deepcopy(snapshot)
    recorded["manual_qa_evidence"].update(expected_open_state)
    _validate_source_manifest(recorded)
    _require(recorded["snapshot_build_allowed"] is False, "snapshot gate opened early")
    return recorded


def _validate_component(
    component_name: str,
    evidence: Mapping[str, Any],
    *,
    expected_run_id: str,
    expected_main_commit: str,
    from_utc: str,
    through_utc: str,
) -> None:
    expected_keys = AUTOMATED_KEYS if component_name == "automated" else MANUAL_KEYS
    try:
        evidence = _exact(evidence, expected_keys, f"{component_name} evidence")
        _validate_window(evidence, component_name)
        if component_name == "automated":
            _validate_nested_automated(evidence)
        else:
            _validate_manual(evidence)
    except SnapshotAssemblyError as exc:
        raise EvidenceGateRecordingError(str(exc)) from exc
    _require(evidence["schema_version"] == 1, f"{component_name} evidence schema drift")
    _require(
        evidence["experiment_id"] == "vevo-sk-aa-001",
        f"{component_name} experiment drift",
    )
    expected_type = f"vevo_growthbook_aa_{'automated' if component_name == 'automated' else 'manual_qa'}_evidence"
    _require(
        evidence["evidence_type"] == expected_type,
        f"{component_name} evidence type drift",
    )
    _require(
        (evidence["from_utc"], evidence["through_utc"]) == (from_utc, through_utc),
        f"{component_name} evidence differs from the resolved A/A window",
    )
    _require(
        evidence["source_run_id"] == expected_run_id,
        f"{component_name} run ID mismatch",
    )
    _require(
        evidence["source_main_commit"] == expected_main_commit,
        f"{component_name} main commit mismatch",
    )
    if component_name == "automated":
        _require(
            evidence["source_read_only"] is True, "automated source must be read-only"
        )
        false_fields = (
            "contains_raw_aws_payloads",
            "contains_cloudwatch_messages",
            "contains_event_or_device_ids",
            "contains_customer_or_order_data",
            "mutation_observed",
        )
    else:
        _require(
            evidence["production_allocation_percent"] == 100,
            "manual QA Production allocation drift",
        )
        for field in (
            "tag_assistant_connected",
            "production_storefront_observed",
            "growthbook_read_only",
        ):
            _require(evidence[field] is True, f"manual QA {field} must be true")
        false_fields = (
            "contains_event_or_device_ids",
            "contains_customer_or_order_data",
            "unplanned_mutation_observed",
        )
    for field in false_fields:
        _require(evidence[field] is False, f"{component_name} {field} must be false")


def record_component(
    snapshot: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    component_name: str,
    evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> dict[str, Any]:
    """Bind one reviewed component and open snapshot only after both exist."""

    _require(component_name in {"automated", "manual"}, "component name is invalid")
    from_utc, through_utc = _require_resolved_window(snapshot)
    run_id = str(expected_workflow_run_id or "").strip()
    main_commit = str(expected_main_commit or "").strip()
    digest = str(evidence_sha256 or "").strip()
    _require(
        RUN_ID_RE.fullmatch(run_id) is not None, "expected workflow run ID is invalid"
    )
    _require(
        COMMIT_RE.fullmatch(main_commit) is not None, "expected main commit is invalid"
    )
    _require(
        SHA256_RE.fullmatch(digest) is not None, "expected evidence SHA-256 is invalid"
    )
    _require(
        hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest() == digest,
        "evidence SHA-256 mismatch",
    )
    _validate_component(
        component_name,
        evidence,
        expected_run_id=run_id,
        expected_main_commit=main_commit,
        from_utc=from_utc,
        through_utc=through_utc,
    )
    manifest_key = (
        "automated_evidence" if component_name == "automated" else "manual_qa_evidence"
    )
    component = snapshot[manifest_key]
    exact_record = {
        "status": "verified",
        "run_id": run_id,
        "main_commit": main_commit,
        "sha256": digest,
        "producer_allowed": False,
    }
    if all(component.get(key) == value for key, value in exact_record.items()):
        return copy.deepcopy(snapshot)
    _require(
        component.get("producer_allowed") is True,
        f"{component_name} producer gate is closed",
    )
    _require(
        component.get("status") == "not_recorded"
        and all(
            component.get(field) is None
            for field in ("run_id", "main_commit", "sha256")
        ),
        f"{component_name} artifact state drift",
    )
    recorded = copy.deepcopy(snapshot)
    recorded[manifest_key].update(exact_record)
    recorded["snapshot_build_allowed"] = all(
        recorded[key].get("status") == "verified"
        for key in ("automated_evidence", "manual_qa_evidence")
    )
    _validate_source_manifest(recorded)
    return recorded


def _write_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    output = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(output)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--output", type=Path, default=SNAPSHOT_PATH)
    subparsers = parser.add_subparsers(dest="action", required=True)

    automated = subparsers.add_parser("open-automated")
    automated.add_argument("--quality-report", required=True, type=Path)
    automated.add_argument("--quality-report-key", required=True)
    automated.add_argument("--expected-quality-report-sha256", required=True)

    manual = subparsers.add_parser("open-manual")
    manual.add_argument("--observation", required=True, type=Path)
    manual.add_argument("--expected-observation-sha256", required=True)

    component = subparsers.add_parser("record-component")
    component.add_argument(
        "--component", required=True, choices=("automated", "manual")
    )
    component.add_argument("--evidence", required=True, type=Path)
    component.add_argument("--expected-evidence-sha256", required=True)
    component.add_argument("--expected-workflow-run-id", required=True)
    component.add_argument("--expected-main-commit", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        snapshot = _load(args.snapshot)
        if args.action == "open-automated":
            quality = _load_canonical_mapping(
                args.quality_report,
                args.expected_quality_report_sha256,
                "quality report",
            )
            recorded = open_automated_producer(
                snapshot,
                quality,
                quality_report_key=args.quality_report_key,
                quality_report_sha256=args.expected_quality_report_sha256,
            )
        elif args.action == "open-manual":
            expected_path = ROOT / snapshot["manual_qa_evidence"]["observation_file"]
            _require(
                args.observation.resolve() == expected_path.resolve(),
                "manual QA observation path differs from the manifest",
            )
            observation = load_canonical_observation(
                args.observation, args.expected_observation_sha256
            )
            recorded = open_manual_producer(
                snapshot,
                observation,
                observation_sha256=args.expected_observation_sha256,
            )
        else:
            evidence = _load_canonical(
                args.evidence,
                args.expected_evidence_sha256,
                args.component,
            )
            recorded = record_component(
                snapshot,
                evidence,
                component_name=args.component,
                evidence_sha256=args.expected_evidence_sha256,
                expected_workflow_run_id=args.expected_workflow_run_id,
                expected_main_commit=args.expected_main_commit,
            )
        _write_atomic(args.output, recorded)
    except (
        OSError,
        EvidenceGateRecordingError,
        ManualQaEvidenceError,
        MeasurementWindowError,
        SnapshotAssemblyError,
    ) as exc:
        print(f"record_growthbook_aa_evidence_gates.py: FAIL: {exc}")
        return 1
    print(
        "record_growthbook_aa_evidence_gates.py: OK:"
        f"action={args.action}:snapshot={recorded['snapshot_build_allowed']}:"
        "aws=false:network=false:biznisweb=false:meta=false:commerce=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
