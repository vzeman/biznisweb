#!/usr/bin/env python3
"""Initialize and record outcome-blind VEVO CTA assignment checkpoints.

This is an offline, fail-closed Git manifest transformer.  It has no AWS,
GrowthBook, GTM, Meta Ads, BiznisWeb, browser, or network client.  A checkpoint
can only extend the whole-local-day window or open a reviewed manual CTA stop.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scripts.record_growthbook_cta_activation import canonical_json_bytes as canonical_activation_bytes
    from scripts.validate_growthbook_cta_measurement_window import (
        ACTIVATION_PATH,
        DECISION_CONTRACT_PATH,
        MANIFEST_PATH,
        RECONCILIATION_EVIDENCE_PATH,
        RESOLVED,
        RUNNING,
        SAMPLE_PLAN_PATH,
        START_OBSERVATION_PATH,
        WAITING,
        CtaMeasurementWindowError,
        canonical_evidence_bytes,
        expected_measurement_window,
        validate_checkpoint_evidence,
        validate_manifest,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from record_growthbook_cta_activation import canonical_json_bytes as canonical_activation_bytes
    from validate_growthbook_cta_measurement_window import (
        ACTIVATION_PATH,
        DECISION_CONTRACT_PATH,
        MANIFEST_PATH,
        RECONCILIATION_EVIDENCE_PATH,
        RESOLVED,
        RUNNING,
        SAMPLE_PLAN_PATH,
        START_OBSERVATION_PATH,
        WAITING,
        CtaMeasurementWindowError,
        canonical_evidence_bytes,
        expected_measurement_window,
        validate_checkpoint_evidence,
        validate_manifest,
    )


RUN_ID_RE = re.compile(r"^[1-9][0-9]*$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CtaCheckpointRecordingError(ValueError):
    """Raised when a CTA checkpoint transition is unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaCheckpointRecordingError(message)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    _require(isinstance(value, dict), f"{path.name} must contain an object")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source_hashes(
    *,
    activation_path: Path = ACTIVATION_PATH,
    start_observation_path: Path = START_OBSERVATION_PATH,
    sample_plan_path: Path = SAMPLE_PLAN_PATH,
    decision_contract_path: Path = DECISION_CONTRACT_PATH,
    reconciliation_path: Path = RECONCILIATION_EVIDENCE_PATH,
) -> dict[str, str]:
    return {
        "activation": _sha256(activation_path),
        "start_observation": _sha256(start_observation_path),
        "sample_plan": _sha256(sample_plan_path),
        "decision_contract": _sha256(decision_contract_path),
        "reconciliation_evidence": _sha256(reconciliation_path),
    }


def initialize_window(
    manifest: Mapping[str, Any],
    *,
    activation: Mapping[str, Any],
    start_observation: Mapping[str, Any],
    sample_plan: Mapping[str, Any],
    contract: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    source_hashes: Mapping[str, str],
) -> dict[str, Any]:
    """Bind the verified CTA start and freeze the exact stopping window."""

    try:
        validate_manifest(
            manifest,
            activation,
            sample_plan,
            contract,
            reconciliation,
            None,
            source_hashes=source_hashes,
        )
    except CtaMeasurementWindowError as exc:
        raise CtaCheckpointRecordingError(f"source CTA window is invalid: {exc}") from exc
    _require(manifest.get("status") == WAITING, "CTA window is already initialized")
    actual_observation_hash = hashlib.sha256(
        canonical_activation_bytes(start_observation)
    ).hexdigest()
    _require(
        source_hashes.get("start_observation") == actual_observation_hash,
        "CTA start observation source hash mismatch",
    )
    expected = expected_measurement_window(
        activation, start_observation, sample_plan, contract, reconciliation
    )
    recorded = copy.deepcopy(manifest)
    recorded["status"] = RUNNING
    bindings = recorded["source_bindings"]
    bindings["activation_sha256"] = source_hashes["activation"]
    bindings["start_observation_sha256"] = source_hashes["start_observation"]
    bindings["sample_plan_sha256"] = source_hashes["sample_plan"]
    recorded["measurement_window"] = expected
    recorded["release_boundaries"]["read_only_checkpoint_allowed"] = True
    recorded["next_gate"] = (
        "run_first_due_outcome_blind_checkpoint_without_arm_or_outcome_readback"
    )
    try:
        validate_manifest(
            recorded,
            activation,
            sample_plan,
            contract,
            reconciliation,
            start_observation,
            source_hashes=source_hashes,
        )
    except CtaMeasurementWindowError as exc:
        raise CtaCheckpointRecordingError(f"initialized CTA window is invalid: {exc}") from exc
    return recorded


def record_checkpoint(
    manifest: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
    activation: Mapping[str, Any],
    start_observation: Mapping[str, Any],
    sample_plan: Mapping[str, Any],
    contract: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    source_hashes: Mapping[str, str],
) -> dict[str, Any]:
    """Record one independently verified, aggregate-only checkpoint."""

    run_id = str(expected_workflow_run_id or "").strip()
    main_commit = str(expected_main_commit or "").strip()
    digest = str(evidence_sha256 or "").strip()
    _require(RUN_ID_RE.fullmatch(run_id) is not None, "expected workflow run ID invalid")
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "expected main commit invalid")
    _require(SHA256_RE.fullmatch(digest) is not None, "checkpoint SHA-256 invalid")
    _require(evidence.get("workflow_run_id") == run_id, "checkpoint workflow run mismatch")
    _require(evidence.get("main_commit") == main_commit, "checkpoint main commit mismatch")
    _require(
        hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest() == digest,
        "checkpoint SHA-256 mismatch",
    )
    try:
        validate_manifest(
            manifest,
            activation,
            sample_plan,
            contract,
            reconciliation,
            start_observation,
            source_hashes=source_hashes,
        )
    except CtaMeasurementWindowError as exc:
        raise CtaCheckpointRecordingError(f"source CTA window is invalid: {exc}") from exc
    _require(manifest.get("status") == RUNNING, "CTA window no longer accepts checkpoints")
    expected = expected_measurement_window(
        activation, start_observation, sample_plan, contract, reconciliation
    )
    history = manifest["measurement_window"]["checkpoint_history"]
    if history and history[-1].get("evidence_sha256") == digest:
        _require(history[-1].get("evidence") == evidence, "same CTA checkpoint hash has different evidence")
        return copy.deepcopy(manifest)
    checkpoint_index = len(history) + 1
    try:
        validate_checkpoint_evidence(evidence, expected, checkpoint_index)
    except CtaMeasurementWindowError as exc:
        raise CtaCheckpointRecordingError(str(exc)) from exc

    recorded = copy.deepcopy(manifest)
    window = recorded["measurement_window"]
    window["checkpoint_history"].append(
        {"evidence_sha256": digest, "evidence": copy.deepcopy(evidence)}
    )
    decision = evidence["decision"]
    if decision != "extend_one_full_local_day":
        observed_window = evidence["window"]
        recorded["status"] = RESOLVED
        window.update(
            {
                "resolution_status": "resolved_waiting_for_manual_assignment_stop",
                "resolution_trigger": "outcome_blind_window_checkpoint",
                "resolved_reason": (
                    "target_total_sample_reached"
                    if decision == "open_manual_stop_review_target_reached"
                    else "maximum_duration_reached"
                ),
                "resolved_checkpoint_through_utc": observed_window[
                    "candidate_through_utc"
                ],
                "resolved_last_full_local_date": observed_window[
                    "candidate_last_full_local_date"
                ],
                "resolved_full_calendar_days": observed_window[
                    "full_calendar_days"
                ],
                "resolved_eligible_devices": evidence["population"][
                    "eligible_devices"
                ],
                "resolved_at_utc": evidence["observed_at_utc"],
            }
        )
        recorded["assignment_stop"].update(
            {
                "status": "manual_stop_review_open_assignment_still_running",
                "manual_review_allowed": True,
                "review_trigger_type": "outcome_blind_window_checkpoint",
                "review_trigger_evidence_sha256": digest,
                "review_trigger_decision_sha256": None,
                "review_trigger_provenance_sha256": None,
                "review_trigger_observed_at_utc": evidence["observed_at_utc"],
            }
        )
        recorded["release_boundaries"]["read_only_checkpoint_allowed"] = False
        recorded["next_gate"] = (
            "manually_stop_only_cta_assignment_then_record_canonical_readback"
        )
    try:
        validate_manifest(
            recorded,
            activation,
            sample_plan,
            contract,
            reconciliation,
            start_observation,
            source_hashes=source_hashes,
        )
    except CtaMeasurementWindowError as exc:
        raise CtaCheckpointRecordingError(f"recorded CTA checkpoint is invalid: {exc}") from exc
    return recorded


def _write_atomic(path: Path, value: Mapping[str, Any]) -> None:
    payload = (json.dumps(value, indent=2) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _sources() -> tuple[dict[str, Any], ...]:
    return (
        _load(ACTIVATION_PATH),
        _load(START_OBSERVATION_PATH),
        _load(SAMPLE_PLAN_PATH),
        _load(DECISION_CONTRACT_PATH),
        _load(RECONCILIATION_EVIDENCE_PATH),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=MANIFEST_PATH)
    parser.add_argument("--output", type=Path, default=MANIFEST_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("initialize")
    checkpoint = subparsers.add_parser("record-checkpoint")
    checkpoint.add_argument("--evidence", required=True, type=Path)
    checkpoint.add_argument("--expected-evidence-sha256", required=True)
    checkpoint.add_argument("--expected-workflow-run-id", required=True)
    checkpoint.add_argument("--expected-main-commit", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        manifest = _load(args.manifest)
        activation, start_observation, sample, contract, reconciliation = _sources()
        hashes = _source_hashes()
        if args.command == "initialize":
            recorded = initialize_window(
                manifest,
                activation=activation,
                start_observation=start_observation,
                sample_plan=sample,
                contract=contract,
                reconciliation=reconciliation,
                source_hashes=hashes,
            )
        else:
            raw = args.evidence.read_bytes()
            evidence = json.loads(raw.decode("utf-8"))
            _require(raw == canonical_evidence_bytes(evidence), "checkpoint evidence is not canonical JSON")
            _require(
                hashlib.sha256(raw).hexdigest()
                == str(args.expected_evidence_sha256).strip(),
                "independently supplied checkpoint SHA-256 mismatch",
            )
            recorded = record_checkpoint(
                manifest,
                evidence,
                evidence_sha256=args.expected_evidence_sha256,
                expected_workflow_run_id=args.expected_workflow_run_id,
                expected_main_commit=args.expected_main_commit,
                activation=activation,
                start_observation=start_observation,
                sample_plan=sample,
                contract=contract,
                reconciliation=reconciliation,
                source_hashes=hashes,
            )
        _write_atomic(args.output, recorded)
        print("record_growthbook_cta_window_checkpoint.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(f"record_growthbook_cta_window_checkpoint.py: FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
