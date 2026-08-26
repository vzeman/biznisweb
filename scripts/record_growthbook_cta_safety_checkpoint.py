#!/usr/bin/env python3
"""Record a hash-bound CTA safety checkpoint and open only a manual stop.

This offline transformer has no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb,
browser, network, reporting, or commerce mutation client.  It independently
re-evaluates canonical aggregate safety evidence and can only update Git
manifests.  A STOP_REQUIRED verdict opens review; it never stops assignment.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scripts.evaluate_growthbook_cta_safety import (
        COMMIT_RE,
        MONITORING,
        RUN_ID_RE,
        SHA256_RE,
        STOP_REVIEW,
        WAITING,
        CtaSafetyEvaluationError,
        canonical_json_bytes,
        evaluate,
        validate_contract,
    )
    from scripts.record_growthbook_cta_activation import (
        RUNNING as CTA_RUNNING,
        canonical_json_bytes as canonical_activation_bytes,
        validate_manifest as validate_activation_manifest,
        validate_start_observation,
    )
    from scripts.validate_growthbook_cta_measurement_window import (
        RESOLVED as WINDOW_RESOLVED,
        RUNNING as WINDOW_RUNNING,
        CtaMeasurementWindowError,
        validate_manifest as validate_measurement_manifest,
    )
except ModuleNotFoundError:  # pragma: no cover - direct execution from scripts/
    from evaluate_growthbook_cta_safety import (
        COMMIT_RE,
        MONITORING,
        RUN_ID_RE,
        SHA256_RE,
        STOP_REVIEW,
        WAITING,
        CtaSafetyEvaluationError,
        canonical_json_bytes,
        evaluate,
        validate_contract,
    )
    from record_growthbook_cta_activation import (
        RUNNING as CTA_RUNNING,
        canonical_json_bytes as canonical_activation_bytes,
        validate_manifest as validate_activation_manifest,
        validate_start_observation,
    )
    from validate_growthbook_cta_measurement_window import (
        RESOLVED as WINDOW_RESOLVED,
        RUNNING as WINDOW_RUNNING,
        CtaMeasurementWindowError,
        validate_manifest as validate_measurement_manifest,
    )


ROOT = Path(__file__).resolve().parents[1]
VEVO = ROOT / "projects" / "vevo"
SAFETY_PATH = VEVO / "growthbook_cta_safety_monitoring.json"
ACTIVATION_PATH = VEVO / "growthbook_cta_activation.json"
START_OBSERVATION_PATH = VEVO / "growthbook_cta_activation_observation.json"
DECISION_CONTRACT_PATH = VEVO / "growthbook_cta_decision_contract.json"
MEASUREMENT_PATH = VEVO / "growthbook_cta_measurement_window.json"
SAMPLE_PLAN_PATH = VEVO / "growthbook_cta_sample_plan.json"
RECONCILIATION_PATH = VEVO / "growthbook_production_reconciliation_deploy_evidence.json"

WORKFLOW = ".github/workflows/check-vevo-growthbook-production-cta-safety.yml"
ARTIFACT_NAME = "vevo-growthbook-cta-safety-checkpoint"
EVIDENCE_FILE = "vevo-growthbook-cta-safety-evidence.json"
DECISION_FILE = "vevo-growthbook-cta-safety-decision.json"
PROVENANCE_FILE = "vevo-growthbook-cta-safety-provenance.json"
EXPECTED_DECISION_SHA256 = (
    "62d9eb905a05b6273a7395905bc73f815e130155af1a32d896195facd442a07a"
)

PROVENANCE_KEYS = {
    "schema_version",
    "provenance_type",
    "repository",
    "workflow",
    "workflow_run_id",
    "main_commit",
    "artifact_name",
    "files",
    "safety",
}
FILE_KEYS = {
    "evidence_file",
    "evidence_sha256",
    "decision_file",
    "decision_sha256",
    "provenance_file",
}
PROVENANCE_SAFETY_KEYS = {
    "contains_event_or_device_ids",
    "contains_customer_or_order_data",
    "contains_primary_or_business_outcomes",
    "contains_meta_dimensions",
    "winner_call_made",
    "external_or_automatic_mutation",
}


class CtaSafetyRecordingError(ValueError):
    """Raised when a CTA safety lifecycle transition is unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaSafetyRecordingError(message)


def _exact(value: Any, keys: set[str], label: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{label} must be an object")
    _require(set(value) == keys, f"{label} fields drift")
    return value


def pretty_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _load(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CtaSafetyRecordingError(f"{label} is unreadable") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _load_canonical(path: Path, expected_sha256: str, label: str) -> dict[str, Any]:
    digest = str(expected_sha256 or "").strip()
    _require(SHA256_RE.fullmatch(digest) is not None, f"{label} SHA-256 is invalid")
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CtaSafetyRecordingError(f"{label} is unreadable") from exc
    _require(isinstance(value, dict), f"{label} must be an object")
    _require(raw == canonical_json_bytes(value), f"{label} is not canonical JSON")
    _require(_sha256_bytes(raw) == digest, f"{label} SHA-256 mismatch")
    return value


def source_hashes(
    activation: Mapping[str, Any],
    start_observation: Mapping[str, Any],
    decision_contract_bytes: bytes,
) -> dict[str, str]:
    return {
        "activation": _sha256_bytes(pretty_json_bytes(activation)),
        "start_observation": _sha256_bytes(
            canonical_activation_bytes(start_observation)
        ),
        "decision_contract": _sha256_bytes(decision_contract_bytes),
    }


def validate_provenance(
    provenance: Mapping[str, Any],
    *,
    evidence_sha256: str,
    decision_sha256: str,
) -> None:
    root = _exact(provenance, PROVENANCE_KEYS, "CTA safety provenance")
    _require(
        root["schema_version"] == 1
        and root["provenance_type"] == "vevo_growthbook_cta_safety_checkpoint",
        "CTA safety provenance identity drift",
    )
    _require(root["repository"] == "vzeman/biznisweb", "CTA safety repository drift")
    _require(root["workflow"] == WORKFLOW, "CTA safety workflow drift")
    _require(
        RUN_ID_RE.fullmatch(str(root["workflow_run_id"])) is not None,
        "CTA safety workflow run ID invalid",
    )
    _require(
        COMMIT_RE.fullmatch(str(root["main_commit"])) is not None,
        "CTA safety main commit invalid",
    )
    _require(root["artifact_name"] == ARTIFACT_NAME, "CTA safety artifact drift")
    files = _exact(root["files"], FILE_KEYS, "CTA safety provenance files")
    _require(
        files
        == {
            "evidence_file": EVIDENCE_FILE,
            "evidence_sha256": evidence_sha256,
            "decision_file": DECISION_FILE,
            "decision_sha256": decision_sha256,
            "provenance_file": PROVENANCE_FILE,
        },
        "CTA safety provenance file binding drift",
    )
    safety = _exact(
        root["safety"], PROVENANCE_SAFETY_KEYS, "CTA safety provenance boundary"
    )
    _require(not any(safety.values()), "CTA safety provenance exceeded its boundary")


def initialize_monitoring(
    manifest: Mapping[str, Any],
    activation: Mapping[str, Any],
    start_observation: Mapping[str, Any],
    *,
    source_hashes: Mapping[str, str],
) -> dict[str, Any]:
    """Bind the verified CTA start and open only the protected collector."""

    try:
        validate_contract(manifest)
        validate_activation_manifest(activation)
        validate_start_observation(start_observation, activation)
    except (CtaSafetyEvaluationError, ValueError) as exc:
        raise CtaSafetyRecordingError(
            f"CTA safety initialization source invalid: {exc}"
        ) from exc
    _require(
        manifest.get("status") == WAITING,
        "CTA safety monitoring is already initialized",
    )
    _require(activation.get("status") == CTA_RUNNING, "CTA activation is not running")
    _require(
        set(source_hashes) == {"activation", "start_observation", "decision_contract"},
        "CTA safety source hash set drift",
    )
    _require(
        source_hashes["activation"] == _sha256_bytes(pretty_json_bytes(activation)),
        "CTA safety activation source hash mismatch",
    )
    _require(
        source_hashes["start_observation"]
        == _sha256_bytes(canonical_activation_bytes(start_observation)),
        "CTA safety start source hash mismatch",
    )
    _require(
        source_hashes["decision_contract"] == EXPECTED_DECISION_SHA256,
        "CTA safety decision contract hash mismatch",
    )
    started = start_observation["assignment_started_at_utc"]
    _require(
        activation["start_readback"]["assignment_started_at_utc"] == started,
        "CTA safety assignment start drift",
    )
    recorded = copy.deepcopy(manifest)
    recorded["status"] = MONITORING
    recorded["assignment_started_at_utc"] = started
    for name in ("activation", "start_observation", "decision_contract"):
        recorded["source_bindings"][name]["sha256"] = source_hashes[name]
    start_probe = start_observation["commerce"]["probe"]
    _require(
        {
            "product_url": start_probe["product_url"],
            "product_code": start_probe["product_code"],
            "cart_url": start_probe["cart_url"],
            "cta_text": start_probe["cta_text"],
        }
        == {
            "product_url": recorded["commerce_probe"]["product_url"],
            "product_code": recorded["commerce_probe"]["product_code"],
            "cart_url": recorded["commerce_probe"]["cart_url"],
            "cta_text": recorded["commerce_probe"]["cta_text"],
        },
        "CTA safety commerce probe target drift",
    )
    recorded["commerce_probe"]["price_text"] = start_probe["price_text"]
    for field in (
        "safety_checkpoint_collection_allowed",
        "safety_checkpoint_recording_allowed",
        "protected_safety_collection_workflow_allowed",
    ):
        recorded["release_boundaries"][field] = True
    recorded["next_gate"] = "record_next_hash_bound_safety_checkpoint"
    validate_contract(recorded)
    return recorded


def _open_measurement_safety_stop(
    measurement: Mapping[str, Any],
    safety: Mapping[str, Any],
    *,
    activation: Mapping[str, Any],
    start_observation: Mapping[str, Any],
    sample_plan: Mapping[str, Any],
    decision_contract: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    measurement_source_hashes: Mapping[str, str],
) -> dict[str, Any]:
    _validate_running_measurement_source(
        measurement,
        activation=activation,
        start_observation=start_observation,
        sample_plan=sample_plan,
        decision_contract=decision_contract,
        reconciliation=reconciliation,
        measurement_source_hashes=measurement_source_hashes,
    )
    latest = safety["latest_checkpoint"]
    safety_hash = _sha256_bytes(pretty_json_bytes(safety))
    updated = copy.deepcopy(measurement)
    updated["status"] = WINDOW_RESOLVED
    updated["measurement_window"].update(
        {
            "resolution_status": "resolved_waiting_for_manual_assignment_stop",
            "resolution_trigger": "safety_guardrail",
            "resolved_reason": "safety_guardrail_stop_required",
            "resolved_checkpoint_through_utc": None,
            "resolved_last_full_local_date": None,
            "resolved_full_calendar_days": None,
            "resolved_eligible_devices": latest["eligible_devices_seen"],
            "resolved_at_utc": latest["observed_at_utc"],
        }
    )
    updated["assignment_stop"].update(
        {
            "status": "manual_stop_review_open_assignment_still_running",
            "manual_review_allowed": True,
            "review_trigger_type": "safety_guardrail",
            "review_trigger_evidence_sha256": latest["evidence_sha256"],
            "review_trigger_decision_sha256": latest["decision_sha256"],
            "review_trigger_provenance_sha256": latest["provenance_sha256"],
            "review_trigger_observed_at_utc": latest["observed_at_utc"],
        }
    )
    updated["release_boundaries"]["read_only_checkpoint_allowed"] = False
    updated["next_gate"] = (
        "manually_stop_only_cta_assignment_then_record_canonical_readback"
    )
    try:
        validate_measurement_manifest(
            updated,
            activation,
            sample_plan,
            decision_contract,
            reconciliation,
            start_observation,
            source_hashes=measurement_source_hashes,
            safety_monitoring=safety,
            safety_monitoring_sha256=safety_hash,
        )
    except CtaMeasurementWindowError as exc:
        raise CtaSafetyRecordingError(f"CTA safety stop bridge invalid: {exc}") from exc
    return updated


def _validate_running_measurement_source(
    measurement: Mapping[str, Any],
    *,
    activation: Mapping[str, Any],
    start_observation: Mapping[str, Any],
    sample_plan: Mapping[str, Any],
    decision_contract: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    measurement_source_hashes: Mapping[str, str],
) -> None:
    try:
        validate_measurement_manifest(
            measurement,
            activation,
            sample_plan,
            decision_contract,
            reconciliation,
            start_observation,
            source_hashes=measurement_source_hashes,
        )
    except CtaMeasurementWindowError as exc:
        raise CtaSafetyRecordingError(f"CTA measurement source invalid: {exc}") from exc
    _require(
        measurement.get("status") == WINDOW_RUNNING,
        "CTA assignment window is not running",
    )


def record_checkpoint(
    manifest: Mapping[str, Any],
    measurement: Mapping[str, Any],
    evidence: Mapping[str, Any],
    decision: Mapping[str, Any],
    provenance: Mapping[str, Any],
    *,
    evidence_sha256: str,
    decision_sha256: str,
    provenance_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
    activation: Mapping[str, Any],
    start_observation: Mapping[str, Any],
    sample_plan: Mapping[str, Any],
    decision_contract: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    measurement_source_hashes: Mapping[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Record one protected artifact and open a manual stop only on breach."""

    validate_contract(manifest)
    _require(manifest.get("status") == MONITORING, "CTA safety monitoring is not open")
    _validate_running_measurement_source(
        measurement,
        activation=activation,
        start_observation=start_observation,
        sample_plan=sample_plan,
        decision_contract=decision_contract,
        reconciliation=reconciliation,
        measurement_source_hashes=measurement_source_hashes,
    )
    for field in (
        "safety_checkpoint_collection_allowed",
        "safety_checkpoint_recording_allowed",
        "protected_safety_collection_workflow_allowed",
    ):
        _require(
            manifest["release_boundaries"][field] is True,
            f"CTA safety gate closed: {field}",
        )
    expected_run_id = str(expected_workflow_run_id or "").strip()
    expected_commit = str(expected_main_commit or "").strip()
    _require(
        RUN_ID_RE.fullmatch(expected_run_id) is not None,
        "expected CTA safety workflow run ID invalid",
    )
    _require(
        COMMIT_RE.fullmatch(expected_commit) is not None,
        "expected CTA safety main commit invalid",
    )
    hashes = {
        "evidence": str(evidence_sha256 or "").strip(),
        "decision": str(decision_sha256 or "").strip(),
        "provenance": str(provenance_sha256 or "").strip(),
    }
    for name, digest in hashes.items():
        _require(
            SHA256_RE.fullmatch(digest) is not None,
            f"CTA safety {name} SHA-256 invalid",
        )
    _require(
        _sha256_bytes(canonical_json_bytes(evidence)) == hashes["evidence"],
        "CTA safety evidence SHA-256 mismatch",
    )
    recomputed = evaluate(evidence, manifest)
    _require(
        decision == recomputed,
        "CTA safety decision differs from independent evaluation",
    )
    _require(
        _sha256_bytes(canonical_json_bytes(decision)) == hashes["decision"],
        "CTA safety decision SHA-256 mismatch",
    )
    validate_provenance(
        provenance,
        evidence_sha256=hashes["evidence"],
        decision_sha256=hashes["decision"],
    )
    _require(
        provenance["workflow_run_id"] == expected_run_id,
        "CTA safety provenance workflow run mismatch",
    )
    _require(
        provenance["main_commit"] == expected_commit,
        "CTA safety provenance main commit mismatch",
    )
    _require(
        _sha256_bytes(canonical_json_bytes(provenance)) == hashes["provenance"],
        "CTA safety provenance SHA-256 mismatch",
    )
    previous_index = (
        0
        if manifest["latest_checkpoint"]["status"] == "not_recorded"
        else manifest["latest_checkpoint"]["checkpoint_index"]
    )
    _require(
        evidence["checkpoint_index"] > previous_index,
        "CTA safety checkpoint sequence did not advance",
    )
    _require(
        evidence["assignment_started_at_utc"] == manifest["assignment_started_at_utc"],
        "CTA safety checkpoint start binding drift",
    )
    assignment_started = datetime.fromisoformat(
        evidence["assignment_started_at_utc"].replace("Z", "+00:00")
    )
    observed_at = datetime.fromisoformat(
        evidence["observed_at_utc"].replace("Z", "+00:00")
    )
    due_at = assignment_started + timedelta(
        hours=(
            manifest["checkpoint_policy"]["first_checkpoint_after_start_hours"]
            + (evidence["checkpoint_index"] - 1)
            * manifest["checkpoint_policy"]["cadence_hours"]
        )
    )
    maximum_observed_at = due_at + timedelta(
        minutes=manifest["checkpoint_policy"]["maximum_checkpoint_lateness_minutes"]
    )
    _require(
        due_at <= observed_at <= maximum_observed_at,
        "CTA safety checkpoint outside exact timing gate",
    )
    _require(
        decision["checkpoint_index"] == evidence["checkpoint_index"]
        and decision["observed_at_utc"] == evidence["observed_at_utc"],
        "CTA safety decision/evidence binding drift",
    )
    recorded = copy.deepcopy(manifest)
    recorded["latest_checkpoint"] = {
        "status": "recorded",
        "checkpoint_index": evidence["checkpoint_index"],
        "observed_at_utc": evidence["observed_at_utc"],
        "eligible_devices_seen": sum(
            row["eligible_devices"] for row in evidence["variation_health"].values()
        ),
        "evidence_sha256": hashes["evidence"],
        "decision_sha256": hashes["decision"],
        "provenance_sha256": hashes["provenance"],
        "workflow_run_id": provenance["workflow_run_id"],
        "main_commit": provenance["main_commit"],
        "verdict": decision["verdict"],
        "stop_reasons": list(decision["stop_reasons"]),
    }
    updated_measurement = copy.deepcopy(measurement)
    if decision["verdict"] == "STOP_REQUIRED":
        recorded["status"] = STOP_REVIEW
        recorded["stop_handoff"].update(
            {
                "status": "manual_stop_review_open",
                "trigger_evidence_sha256": hashes["evidence"],
                "trigger_decision_sha256": hashes["decision"],
                "trigger_provenance_sha256": hashes["provenance"],
                "trigger_observed_at_utc": evidence["observed_at_utc"],
                "stop_reasons": list(decision["stop_reasons"]),
            }
        )
        for field in (
            "safety_checkpoint_collection_allowed",
            "safety_checkpoint_recording_allowed",
            "protected_safety_collection_workflow_allowed",
        ):
            recorded["release_boundaries"][field] = False
        recorded["release_boundaries"]["manual_growthbook_stop_allowed"] = True
        recorded["next_gate"] = (
            "manually_stop_only_exact_cta_then_record_canonical_readback"
        )
        validate_contract(recorded)
        updated_measurement = _open_measurement_safety_stop(
            measurement,
            recorded,
            activation=activation,
            start_observation=start_observation,
            sample_plan=sample_plan,
            decision_contract=decision_contract,
            reconciliation=reconciliation,
            measurement_source_hashes=measurement_source_hashes,
        )
    else:
        recorded["next_gate"] = "record_next_hash_bound_safety_checkpoint"
        validate_contract(recorded)
    return recorded, updated_measurement


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=SAFETY_PATH)
    parser.add_argument("--output", type=Path, default=SAFETY_PATH)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("initialize")
    record = commands.add_parser("record-checkpoint")
    record.add_argument("--measurement", type=Path, default=MEASUREMENT_PATH)
    record.add_argument("--measurement-output", type=Path, default=MEASUREMENT_PATH)
    record.add_argument("--evidence", type=Path, required=True)
    record.add_argument("--evidence-sha256", required=True)
    record.add_argument("--decision", type=Path, required=True)
    record.add_argument("--decision-sha256", required=True)
    record.add_argument("--provenance", type=Path, required=True)
    record.add_argument("--provenance-sha256", required=True)
    record.add_argument("--expected-workflow-run-id", required=True)
    record.add_argument("--expected-main-commit", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        manifest = _load(args.manifest, "CTA safety manifest")
        activation = _load(ACTIVATION_PATH, "CTA activation")
        start = _load(START_OBSERVATION_PATH, "CTA start observation")
        decision_contract_bytes = DECISION_CONTRACT_PATH.read_bytes()
        hashes = source_hashes(activation, start, decision_contract_bytes)
        if args.command == "initialize":
            recorded = initialize_monitoring(
                manifest,
                activation,
                start,
                source_hashes=hashes,
            )
            _write_atomic(args.output, recorded)
        else:
            measurement = _load(args.measurement, "CTA measurement manifest")
            sample = _load(SAMPLE_PLAN_PATH, "CTA sample plan")
            decision_contract = json.loads(decision_contract_bytes.decode("utf-8"))
            reconciliation = _load(RECONCILIATION_PATH, "CTA reconciliation")
            evidence = _load_canonical(
                args.evidence, args.evidence_sha256, "CTA safety evidence"
            )
            decision = _load_canonical(
                args.decision, args.decision_sha256, "CTA safety decision"
            )
            provenance = _load_canonical(
                args.provenance, args.provenance_sha256, "CTA safety provenance"
            )
            measurement_hashes = {
                "activation": _sha256_bytes(pretty_json_bytes(activation)),
                "start_observation": _sha256_bytes(canonical_activation_bytes(start)),
                "sample_plan": _sha256_bytes(SAMPLE_PLAN_PATH.read_bytes()),
                "decision_contract": _sha256_bytes(decision_contract_bytes),
                "reconciliation_evidence": _sha256_bytes(
                    RECONCILIATION_PATH.read_bytes()
                ),
            }
            recorded, updated_measurement = record_checkpoint(
                manifest,
                measurement,
                evidence,
                decision,
                provenance,
                evidence_sha256=args.evidence_sha256,
                decision_sha256=args.decision_sha256,
                provenance_sha256=args.provenance_sha256,
                expected_workflow_run_id=args.expected_workflow_run_id,
                expected_main_commit=args.expected_main_commit,
                activation=activation,
                start_observation=start,
                sample_plan=sample,
                decision_contract=decision_contract,
                reconciliation=reconciliation,
                measurement_source_hashes=measurement_hashes,
            )
            _require(
                args.output.resolve() != args.measurement_output.resolve(),
                "CTA safety outputs must be distinct",
            )
            _write_atomic(args.output, recorded)
            _write_atomic(args.measurement_output, updated_measurement)
        print("VEVO_CTA_SAFETY_RECORDED:automatic=false:winner=false:outcomes=false")
        return 0
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(f"record_growthbook_cta_safety_checkpoint.py: FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
