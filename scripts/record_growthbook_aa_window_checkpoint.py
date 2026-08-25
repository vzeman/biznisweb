#!/usr/bin/env python3
"""Record one outcome-blind VEVO Production A/A window checkpoint.

This tool is an offline, fail-closed manifest transformation. It has no AWS,
GrowthBook, GTM, Meta Ads, BiznisWeb, browser, or network client. The caller
must independently obtain a successful main-branch workflow run ID and commit,
verify the canonical sanitized artifact hash, and review the resulting Git diff.
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
    from scripts.validate_growthbook_aa_measurement_window import (
        ACCEPTANCE_PATH,
        ACTIVATION_PATH,
        RECONCILIATION_EVIDENCE_PATH,
        SNAPSHOT_PATH,
        MeasurementWindowError,
        canonical_evidence_bytes,
        expected_measurement_window,
        validate_checkpoint_evidence,
        validate_measurement_window,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from validate_growthbook_aa_measurement_window import (
        ACCEPTANCE_PATH,
        ACTIVATION_PATH,
        RECONCILIATION_EVIDENCE_PATH,
        SNAPSHOT_PATH,
        MeasurementWindowError,
        canonical_evidence_bytes,
        expected_measurement_window,
        validate_checkpoint_evidence,
        validate_measurement_window,
    )


RUN_ID_RE = re.compile(r"^[1-9][0-9]*$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CheckpointRecordingError(ValueError):
    """Raised when checkpoint evidence or manifest transition is unsafe."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CheckpointRecordingError(message)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    _require(isinstance(value, dict), f"{path.name} must contain an object")
    return value


def record_checkpoint(
    snapshot: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
    activation: Mapping[str, Any],
    acceptance: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the exact next manifest state for one reviewed checkpoint."""

    run_id = str(expected_workflow_run_id or "").strip()
    main_commit = str(expected_main_commit or "").strip()
    digest = str(evidence_sha256 or "").strip()
    _require(RUN_ID_RE.fullmatch(run_id) is not None, "expected workflow run ID is invalid")
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "expected main commit is invalid")
    _require(SHA256_RE.fullmatch(digest) is not None, "evidence SHA-256 is invalid")
    _require(evidence.get("workflow_run_id") == run_id, "workflow run ID mismatch")
    _require(evidence.get("main_commit") == main_commit, "main commit mismatch")
    actual_digest = hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest()
    _require(actual_digest == digest, "evidence SHA-256 mismatch")

    try:
        validate_measurement_window(
            snapshot, activation, acceptance, reconciliation
        )
    except MeasurementWindowError as exc:
        raise CheckpointRecordingError(f"source manifest is invalid: {exc}") from exc

    recorded = copy.deepcopy(snapshot)
    window = recorded["measurement_window"]
    history = window["checkpoint_history"]
    if history and history[-1].get("evidence_sha256") == digest:
        _require(
            history[-1].get("evidence") == evidence,
            "same checkpoint hash has different evidence",
        )
        return recorded
    _require(window["resolution_status"] != "resolved", "A/A window is already resolved")

    expected = expected_measurement_window(activation, acceptance, reconciliation)
    checkpoint_index = len(history) + 1
    try:
        validate_checkpoint_evidence(evidence, expected, checkpoint_index)
    except MeasurementWindowError as exc:
        raise CheckpointRecordingError(str(exc)) from exc

    history.append(
        {
            "evidence_sha256": digest,
            "evidence": copy.deepcopy(evidence),
        }
    )
    if evidence["decision"] == "resolve":
        checkpoint_window = evidence["window"]
        population = evidence["population"]
        window.update(
            {
                "status": "resolved_by_preregistered_sample_stopping_rule",
                "resolution_status": "resolved",
                "resolved_last_full_local_date": checkpoint_window[
                    "candidate_last_full_local_date"
                ],
                "resolved_through_utc": checkpoint_window[
                    "candidate_through_utc"
                ],
                "resolved_full_calendar_days": checkpoint_window[
                    "full_calendar_days"
                ],
                "resolved_eligible_devices": population["eligible_devices"],
                "resolved_at_utc": evidence["observed_at_utc"],
            }
        )
        for component_name in ("automated_evidence", "manual_qa_evidence"):
            component = recorded[component_name]
            component["window_status"] = "resolved_waiting_for_reviewed_producer_open"
            component["through_utc"] = window["resolved_through_utc"]

    try:
        validate_measurement_window(
            recorded, activation, acceptance, reconciliation
        )
    except MeasurementWindowError as exc:
        raise CheckpointRecordingError(f"recorded manifest is invalid: {exc}") from exc
    _require(recorded.get("snapshot_build_allowed") is False, "snapshot gate opened")
    for component_name in ("automated_evidence", "manual_qa_evidence"):
        _require(
            recorded[component_name].get("producer_allowed") is False,
            f"{component_name} producer gate opened",
        )
    return recorded


def load_validate_and_record(
    *,
    evidence_path: Path,
    snapshot_path: Path,
    output_path: Path,
    expected_evidence_sha256: str,
    expected_workflow_run_id: str,
    expected_main_commit: str,
) -> dict[str, Any]:
    raw = evidence_path.read_bytes()
    evidence = json.loads(raw.decode("utf-8"))
    _require(isinstance(evidence, dict), "evidence must contain an object")
    canonical = canonical_evidence_bytes(evidence)
    _require(raw == canonical, "checkpoint evidence is not canonical JSON")
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    _require(
        SHA256_RE.fullmatch(str(expected_evidence_sha256 or "").strip()) is not None,
        "expected evidence SHA-256 is invalid",
    )
    _require(
        actual_sha256 == str(expected_evidence_sha256).strip(),
        "independently supplied evidence SHA-256 mismatch",
    )
    recorded = record_checkpoint(
        _load(snapshot_path),
        evidence,
        evidence_sha256=actual_sha256,
        expected_workflow_run_id=expected_workflow_run_id,
        expected_main_commit=expected_main_commit,
        activation=_load(ACTIVATION_PATH),
        acceptance=_load(ACCEPTANCE_PATH),
        reconciliation=_load(RECONCILIATION_EVIDENCE_PATH),
    )
    output = (json.dumps(recorded, indent=2) + "\n").encode("utf-8")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(output)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return recorded


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--output", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--expected-evidence-sha256", required=True)
    parser.add_argument("--expected-workflow-run-id", required=True)
    parser.add_argument("--expected-main-commit", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        load_validate_and_record(
            evidence_path=args.evidence,
            snapshot_path=args.snapshot,
            output_path=args.output,
            expected_evidence_sha256=args.expected_evidence_sha256,
            expected_workflow_run_id=args.expected_workflow_run_id,
            expected_main_commit=args.expected_main_commit,
        )
        print("record_growthbook_aa_window_checkpoint.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(f"record_growthbook_aa_window_checkpoint.py: FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
