#!/usr/bin/env python3
"""Record one successful protected VEVO CTA final snapshot offline.

The recorder accepts only canonical, independently hashed snapshot, decision,
and workflow-provenance artifacts. It recomputes the decision, closes the
final-look read gate, and records provenance without calling any external
service or applying a winner.
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
    from build_growthbook_cta_final_snapshot import (
        COMMIT_RE,
        FOLLOWUP,
        RECORDED,
        RUN_ID_RE,
        CtaFinalSnapshotError,
        DEFAULT_ACTIVATION_PATH,
        DEFAULT_COMPLETION_PATH,
        DEFAULT_DECISION_CONTRACT_PATH,
        DEFAULT_LIFECYCLE_PATH,
        DEFAULT_MANIFEST_PATH,
        DEFAULT_MEASUREMENT_PATH,
        DEFAULT_SAMPLE_PLAN_PATH,
        DEFAULT_STOP_OBSERVATION_PATH,
        _load,
        _parse_utc,
        validate_manifest,
    )
    from evaluate_growthbook_cta import (
        CtaEvaluationError,
        canonical_json_bytes,
        evaluate,
        validate_lifecycle_manifest,
    )
    from validate_growthbook_hypothesis_registry import (
        DEFAULT_REGISTRY_PATH,
        HypothesisRegistryError,
        pretty_json_bytes as registry_json_bytes,
        record_final_decision,
        validate_registry,
    )
except ModuleNotFoundError:  # Imported as scripts.record_growthbook_cta_final_snapshot.
    from scripts.build_growthbook_cta_final_snapshot import (
        COMMIT_RE,
        FOLLOWUP,
        RECORDED,
        RUN_ID_RE,
        CtaFinalSnapshotError,
        DEFAULT_ACTIVATION_PATH,
        DEFAULT_COMPLETION_PATH,
        DEFAULT_DECISION_CONTRACT_PATH,
        DEFAULT_LIFECYCLE_PATH,
        DEFAULT_MANIFEST_PATH,
        DEFAULT_MEASUREMENT_PATH,
        DEFAULT_SAMPLE_PLAN_PATH,
        DEFAULT_STOP_OBSERVATION_PATH,
        _load,
        _parse_utc,
        validate_manifest,
    )
    from scripts.evaluate_growthbook_cta import (
        CtaEvaluationError,
        canonical_json_bytes,
        evaluate,
        validate_lifecycle_manifest,
    )
    from scripts.validate_growthbook_hypothesis_registry import (
        DEFAULT_REGISTRY_PATH,
        HypothesisRegistryError,
        pretty_json_bytes as registry_json_bytes,
        record_final_decision,
        validate_registry,
    )


ROOT = Path(__file__).resolve().parents[1]
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PROVENANCE_TYPE = "vevo_growthbook_cta_final_provenance"
PROVENANCE_WORKFLOW = (
    ".github/workflows/build-vevo-growthbook-production-cta-final-snapshot.yml"
)
PROVENANCE_ARTIFACT = "vevo-growthbook-cta-final-snapshot"
SNAPSHOT_FILE_NAME = "vevo-growthbook-cta-final-snapshot.json"
DECISION_FILE_NAME = "vevo-growthbook-cta-final-decision.json"
PROVENANCE_KEYS = {
    "schema_version",
    "evidence_type",
    "repository",
    "workflow",
    "workflow_run_id",
    "workflow_run_attempt",
    "main_commit",
    "artifact_name",
    "files",
    "safety",
}
PROVENANCE_SAFETY = {
    "contains_raw_aws_payloads": False,
    "contains_credentials": False,
    "contains_event_or_device_ids": False,
    "contains_customer_or_order_data": False,
    "external_or_automatic_mutation": False,
}


class CtaFinalSnapshotRecordingError(ValueError):
    """Raised when final snapshot provenance or decision fails closed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaFinalSnapshotRecordingError(message)


def _exact(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, Mapping), f"{field} must be an object")
    _require(set(value) == keys, f"{field} keys drift")
    return value


def validate_provenance(
    provenance: Mapping[str, Any],
    *,
    provenance_sha256: str,
    snapshot_sha256: str,
    decision_sha256: str,
    workflow_run_id: str,
    main_commit: str,
) -> None:
    root = _exact(provenance, PROVENANCE_KEYS, "CTA final provenance")
    _require(
        root["schema_version"] == 1
        and root["evidence_type"] == PROVENANCE_TYPE,
        "CTA final provenance identity drift",
    )
    _require(
        root["repository"] == "vzeman/biznisweb"
        and root["workflow"] == PROVENANCE_WORKFLOW,
        "CTA final provenance source drift",
    )
    _require(
        root["workflow_run_id"] == workflow_run_id
        and RUN_ID_RE.fullmatch(workflow_run_id) is not None,
        "CTA final provenance workflow run mismatch",
    )
    _require(
        root["workflow_run_attempt"] == 1,
        "CTA final provenance must come from the first workflow attempt",
    )
    _require(
        root["main_commit"] == main_commit
        and COMMIT_RE.fullmatch(main_commit) is not None,
        "CTA final provenance main commit mismatch",
    )
    _require(
        root["artifact_name"] == PROVENANCE_ARTIFACT,
        "CTA final provenance artifact drift",
    )
    files = _exact(
        root["files"],
        {SNAPSHOT_FILE_NAME, DECISION_FILE_NAME},
        "CTA final provenance files",
    )
    expected_hashes = {
        SNAPSHOT_FILE_NAME: snapshot_sha256,
        DECISION_FILE_NAME: decision_sha256,
    }
    for file_name, expected_sha256 in expected_hashes.items():
        row = _exact(
            files[file_name], {"sha256"}, f"CTA final provenance {file_name}"
        )
        _require(
            row["sha256"] == expected_sha256
            and SHA256_RE.fullmatch(str(row["sha256"] or "")) is not None,
            f"CTA final provenance hash mismatch: {file_name}",
        )
    _require(
        _exact(
            root["safety"], set(PROVENANCE_SAFETY), "CTA final provenance safety"
        )
        == PROVENANCE_SAFETY,
        "CTA final provenance safety drift",
    )
    _require(
        SHA256_RE.fullmatch(provenance_sha256) is not None
        and hashlib.sha256(canonical_json_bytes(provenance)).hexdigest()
        == provenance_sha256,
        "CTA final provenance SHA-256 mismatch",
    )


def _load_canonical(path: Path, expected_sha256: str, field: str) -> dict[str, Any]:
    _require(SHA256_RE.fullmatch(expected_sha256) is not None, f"{field} SHA-256 is invalid")
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CtaFinalSnapshotRecordingError(f"{field} is unreadable") from exc
    _require(isinstance(value, dict), f"{field} must contain an object")
    _require(raw == canonical_json_bytes(value), f"{field} is not canonical JSON")
    _require(hashlib.sha256(raw).hexdigest() == expected_sha256, f"{field} SHA-256 mismatch")
    return value


def _load_lifecycle_observation(
    lifecycle: Mapping[str, Any], lifecycle_observation_path: Path | None
) -> Mapping[str, Any] | None:
    if lifecycle.get("verified") is not True:
        return None
    selected = lifecycle_observation_path
    if selected is None:
        relative = lifecycle.get("observation_path")
        _require(isinstance(relative, str) and relative, "CTA lifecycle observation path is missing")
        selected = (ROOT / relative).resolve()
        _require(ROOT.resolve() in selected.parents, "CTA lifecycle observation escapes repository")
    digest = str(lifecycle.get("observation_sha256") or "")
    return _load_canonical(selected, digest, "CTA lifecycle observation")


def record_final_snapshot(
    manifest: Mapping[str, Any],
    registry: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    decision: Mapping[str, Any],
    provenance: Mapping[str, Any],
    contract: Mapping[str, Any],
    sample: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    lifecycle_observation: Mapping[str, Any] | None,
    *,
    snapshot_sha256: str,
    decision_sha256: str,
    provenance_sha256: str,
    workflow_run_id: str,
    main_commit: str,
    completion_path: Path = DEFAULT_COMPLETION_PATH,
    activation_path: Path = DEFAULT_ACTIVATION_PATH,
    measurement_path: Path = DEFAULT_MEASUREMENT_PATH,
    sample_plan_path: Path = DEFAULT_SAMPLE_PLAN_PATH,
    decision_contract_path: Path = DEFAULT_DECISION_CONTRACT_PATH,
    lifecycle_path: Path = DEFAULT_LIFECYCLE_PATH,
    stop_observation_path: Path = DEFAULT_STOP_OBSERVATION_PATH,
    source_bytes: Mapping[str, bytes] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        validate_manifest(
            manifest,
            completion_path=completion_path,
            activation_path=activation_path,
            measurement_path=measurement_path,
            sample_plan_path=sample_plan_path,
            decision_contract_path=decision_contract_path,
            lifecycle_path=lifecycle_path,
            stop_observation_path=stop_observation_path,
            source_bytes=source_bytes,
        )
    except CtaFinalSnapshotError as exc:
        raise CtaFinalSnapshotRecordingError(str(exc)) from exc
    _require(manifest.get("status") == FOLLOWUP, "CTA final snapshot is already recorded or not open")
    _require(RUN_ID_RE.fullmatch(workflow_run_id) is not None, "CTA final workflow run ID is invalid")
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "CTA final main commit is invalid")
    _require(SHA256_RE.fullmatch(snapshot_sha256) is not None, "CTA final snapshot SHA-256 is invalid")
    _require(SHA256_RE.fullmatch(decision_sha256) is not None, "CTA final decision SHA-256 is invalid")
    _require(SHA256_RE.fullmatch(provenance_sha256) is not None, "CTA final provenance SHA-256 is invalid")
    _require(
        hashlib.sha256(canonical_json_bytes(snapshot)).hexdigest() == snapshot_sha256,
        "CTA final snapshot SHA-256 mismatch",
    )
    _require(
        hashlib.sha256(canonical_json_bytes(decision)).hexdigest() == decision_sha256,
        "CTA final decision SHA-256 mismatch",
    )
    validate_provenance(
        provenance,
        provenance_sha256=provenance_sha256,
        snapshot_sha256=snapshot_sha256,
        decision_sha256=decision_sha256,
        workflow_run_id=workflow_run_id,
        main_commit=main_commit,
    )
    try:
        validate_lifecycle_manifest(lifecycle, lifecycle_observation)
        expected_decision = evaluate(
            snapshot,
            contract,
            sample,
            lifecycle,
            lifecycle_observation,
        )
    except CtaEvaluationError as exc:
        raise CtaFinalSnapshotRecordingError(f"CTA final evaluation is invalid: {exc}") from exc
    _require(
        canonical_json_bytes(decision) == canonical_json_bytes(expected_decision),
        "CTA final decision differs from offline recomputation",
    )
    _require(decision.get("final_decision") is True, "CTA final decision is not final")
    _require(decision.get("verdict") in {"WIN", "LOSE", "INCONCLUSIVE"}, "CTA final verdict drift")
    _require(decision.get("automatic_mutation_allowed") is False, "CTA final decision opened mutation")
    _require(
        decision.get("recommended_variation") in {"control", "brand_contrast"},
        "CTA final recommendation drift",
    )
    _require(
        snapshot.get("assignment_started_at_utc")
        == manifest["final_look"]["assignment_started_at_utc"],
        "CTA final snapshot start binding drift",
    )
    _require(
        snapshot.get("assignment_ended_at_utc")
        == manifest["final_look"]["assignment_ended_at_utc"],
        "CTA final snapshot end binding drift",
    )
    _require(
        _parse_utc(snapshot.get("evaluated_at_utc"), "snapshot.evaluated_at_utc")
        >= _parse_utc(manifest["final_look"]["snapshot_due_utc"], "final_look.snapshot_due_utc"),
        "CTA final snapshot predates its due time",
    )

    try:
        updated_registry = record_final_decision(
            registry,
            snapshot,
            decision,
            snapshot_sha256=snapshot_sha256,
            decision_sha256=decision_sha256,
            provenance_sha256=provenance_sha256,
            workflow_run_id=workflow_run_id,
            main_commit=main_commit,
        )
    except HypothesisRegistryError as exc:
        raise CtaFinalSnapshotRecordingError(
            f"CTA hypothesis registry is invalid: {exc}"
        ) from exc
    registry_sha256 = hashlib.sha256(registry_json_bytes(updated_registry)).hexdigest()

    updated = copy.deepcopy(dict(manifest))
    updated["status"] = RECORDED
    updated["final_look"].update(
        {
            "protected_workflow_allowed": False,
            "successful_run_id": workflow_run_id,
            "main_commit": main_commit,
            "snapshot_sha256": snapshot_sha256,
            "decision_sha256": decision_sha256,
            "provenance_sha256": provenance_sha256,
            "hypothesis_registry_sha256": registry_sha256,
            "verdict": decision["verdict"],
            "recommended_variation": decision["recommended_variation"],
        }
    )
    updated["release_boundaries"]["aws_aggregate_reads_allowed"] = False
    updated["release_boundaries"]["diagnostic_host_gate_task_allowed"] = False
    updated["release_boundaries"]["outcome_metrics_read_allowed"] = False
    updated["next_gate"] = "manual_review_decision_before_any_external_mutation"
    try:
        validate_manifest(
            updated,
            completion_path=completion_path,
            activation_path=activation_path,
            measurement_path=measurement_path,
            sample_plan_path=sample_plan_path,
            decision_contract_path=decision_contract_path,
            lifecycle_path=lifecycle_path,
            stop_observation_path=stop_observation_path,
            source_bytes=source_bytes,
        )
    except CtaFinalSnapshotError as exc:
        raise CtaFinalSnapshotRecordingError(str(exc)) from exc
    try:
        validate_registry(updated_registry, updated)
    except HypothesisRegistryError as exc:
        raise CtaFinalSnapshotRecordingError(
            f"CTA hypothesis registry/final snapshot binding is invalid: {exc}"
        ) from exc
    return updated, updated_registry


def _write_atomic(path: Path, value: Mapping[str, Any]) -> None:
    body = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--snapshot-sha256", required=True)
    parser.add_argument("--decision", type=Path, required=True)
    parser.add_argument("--decision-sha256", required=True)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--provenance-sha256", required=True)
    parser.add_argument("--workflow-run-id", required=True)
    parser.add_argument("--main-commit", required=True)
    parser.add_argument("--contract", type=Path, default=DEFAULT_DECISION_CONTRACT_PATH)
    parser.add_argument("--sample-plan", type=Path, default=DEFAULT_SAMPLE_PLAN_PATH)
    parser.add_argument("--lifecycle", type=Path, default=DEFAULT_LIFECYCLE_PATH)
    parser.add_argument("--lifecycle-observation", type=Path)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--registry-output", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        _require(
            args.output.resolve() != args.registry_output.resolve(),
            "CTA final manifest and hypothesis registry outputs must differ",
        )
        snapshot = _load_canonical(args.snapshot, args.snapshot_sha256, "CTA final snapshot")
        decision = _load_canonical(args.decision, args.decision_sha256, "CTA final decision")
        provenance = _load_canonical(
            args.provenance, args.provenance_sha256, "CTA final provenance"
        )
        lifecycle = _load(args.lifecycle, "CTA lifecycle reconciliation")
        recorded, recorded_registry = record_final_snapshot(
            _load(args.manifest, "CTA final snapshot manifest"),
            _load(args.registry, "CTA hypothesis registry"),
            snapshot,
            decision,
            provenance,
            _load(args.contract, "CTA decision contract"),
            _load(args.sample_plan, "CTA sample plan"),
            lifecycle,
            _load_lifecycle_observation(lifecycle, args.lifecycle_observation),
            snapshot_sha256=args.snapshot_sha256,
            decision_sha256=args.decision_sha256,
            provenance_sha256=args.provenance_sha256,
            workflow_run_id=args.workflow_run_id,
            main_commit=args.main_commit,
        )
        _write_atomic(args.registry_output, recorded_registry)
        _write_atomic(args.output, recorded)
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        CtaFinalSnapshotError,
        CtaFinalSnapshotRecordingError,
        HypothesisRegistryError,
    ) as exc:
        print(f"VEVO_CTA_FINAL_RECORD_INVALID:{exc}")
        return 2
    print(
        "VEVO_CTA_FINAL_RECORDED:"
        f"run={recorded['final_look']['successful_run_id']}:"
        f"verdict={recorded['final_look']['verdict']}:"
        "reads-closed=true:automatic-mutation=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
