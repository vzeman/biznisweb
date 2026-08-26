#!/usr/bin/env python3
"""Record one canonical, identity-free CTA lifecycle reconciliation observation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import tempfile
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

try:
    from evaluate_growthbook_cta import (
        CtaEvaluationError,
        validate_lifecycle_manifest,
        validate_lifecycle_observation,
    )
except (
    ModuleNotFoundError
):  # Imported as scripts.record_growthbook_cta_lifecycle_reconciliation.
    from scripts.evaluate_growthbook_cta import (
        CtaEvaluationError,
        validate_lifecycle_manifest,
        validate_lifecycle_observation,
    )


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_lifecycle_reconciliation.json"
)
DEFAULT_OBSERVATION_OUTPUT = (
    ROOT / "projects" / "vevo" / "growthbook_cta_lifecycle_observation.json"
)
ALLOWED_MANIFEST_CHANGES = {
    "status",
    "verified",
    "observation_path",
    "observation_sha256",
    "workflow_run_id",
    "main_commit",
    "source_completion_sha256",
    "source_aa_snapshot_sha256",
    "reporting_quality_object_key",
    "reporting_quality_object_sha256",
    "verified_at_utc",
    "refund_creditnote_value_parity_verified",
    "non_realized_value_policy_verified",
}
RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


class LifecycleRecordingError(ValueError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise LifecycleRecordingError(message)


def canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _parse_utc(value: str, field: str) -> datetime:
    _require(
        isinstance(value, str) and value.endswith("Z"),
        f"{field} must use UTC Z format",
    )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError as exc:
        raise LifecycleRecordingError(f"{field} is invalid") from exc
    _require(
        parsed.strftime("%Y-%m-%dT%H:%M:%SZ") == value,
        f"{field} must use whole-second UTC Z format",
    )
    return parsed


def record(
    *,
    observation_bytes: bytes,
    expected_sha256: str,
    current_manifest: Mapping[str, Any],
    verified_at_utc: str,
    workflow_run_id: str,
    main_commit: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    _require(
        current_manifest.get("verified") is not True,
        "lifecycle reconciliation is already recorded",
    )
    try:
        validate_lifecycle_manifest(current_manifest)
    except CtaEvaluationError as exc:
        raise LifecycleRecordingError(
            f"current lifecycle manifest is invalid: {exc}"
        ) from exc
    _require(
        current_manifest["verified"] is False, "lifecycle verification state is invalid"
    )

    try:
        observation = json.loads(observation_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LifecycleRecordingError(
            "lifecycle observation is not valid UTF-8 JSON"
        ) from exc
    _require(
        isinstance(observation, dict), "lifecycle observation must contain an object"
    )
    _require(
        observation_bytes == canonical_json_bytes(observation),
        "lifecycle observation must use canonical JSON encoding",
    )
    try:
        validate_lifecycle_observation(observation)
    except CtaEvaluationError as exc:
        raise LifecycleRecordingError(
            f"lifecycle observation is invalid: {exc}"
        ) from exc

    actual_sha256 = hashlib.sha256(observation_bytes).hexdigest()
    _require(expected_sha256 == actual_sha256, "lifecycle observation SHA-256 mismatch")
    _require(
        RUN_ID_RE.fullmatch(workflow_run_id) is not None
        and workflow_run_id == observation["workflow_run_id"],
        "lifecycle workflow run ID mismatch",
    )
    _require(
        COMMIT_RE.fullmatch(main_commit) is not None
        and main_commit == observation["main_commit"],
        "lifecycle main commit mismatch",
    )
    verified_at = _parse_utc(verified_at_utc, "verified_at_utc")
    observed_at = _parse_utc(observation["observed_at_utc"], "observed_at_utc")
    _require(
        verified_at >= observed_at, "lifecycle verification predates the observation"
    )

    updated = dict(current_manifest)
    updated.update(
        {
            "status": "verified_completed_aa_21d_lifecycle_preflight",
            "verified": True,
            "observation_path": (
                "projects/vevo/growthbook_cta_lifecycle_observation.json"
            ),
            "observation_sha256": actual_sha256,
            "workflow_run_id": workflow_run_id,
            "main_commit": main_commit,
            "source_completion_sha256": observation["source_completion_sha256"],
            "source_aa_snapshot_sha256": observation["source_aa_snapshot_sha256"],
            "reporting_quality_object_key": observation["reporting_quality_object_key"],
            "reporting_quality_object_sha256": observation[
                "reporting_quality_object_sha256"
            ],
            "verified_at_utc": verified_at_utc,
            "refund_creditnote_value_parity_verified": True,
            "non_realized_value_policy_verified": True,
        }
    )
    changed = {key for key in updated if updated.get(key) != current_manifest.get(key)}
    _require(
        changed == ALLOWED_MANIFEST_CHANGES,
        f"unexpected lifecycle manifest changes: {sorted(changed)}",
    )
    try:
        validate_lifecycle_manifest(updated, observation)
    except CtaEvaluationError as exc:
        raise LifecycleRecordingError(
            f"updated lifecycle manifest is invalid: {exc}"
        ) from exc
    return updated, observation


def _write_atomic(path: pathlib.Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=str(path.parent)
    )
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
    parser.add_argument("--observation", required=True, type=pathlib.Path)
    parser.add_argument("--observation-sha256", required=True)
    parser.add_argument("--verified-at-utc", required=True)
    parser.add_argument("--workflow-run-id", required=True)
    parser.add_argument("--main-commit", required=True)
    parser.add_argument("--manifest", type=pathlib.Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument(
        "--observation-output",
        type=pathlib.Path,
        default=DEFAULT_OBSERVATION_OUTPUT,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        current_manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        _require(
            isinstance(current_manifest, dict), "lifecycle manifest must be an object"
        )
        observation_bytes = args.observation.read_bytes()
        updated, observation = record(
            observation_bytes=observation_bytes,
            expected_sha256=args.observation_sha256,
            current_manifest=current_manifest,
            verified_at_utc=args.verified_at_utc,
            workflow_run_id=args.workflow_run_id,
            main_commit=args.main_commit,
        )
        _require(
            args.manifest.resolve() == DEFAULT_MANIFEST_PATH.resolve(),
            "lifecycle manifest output path must be the repository source of truth",
        )
        _require(
            args.observation_output.resolve() == DEFAULT_OBSERVATION_OUTPUT.resolve(),
            "lifecycle observation output path must be the repository source of truth",
        )
        _write_atomic(args.observation_output, canonical_json_bytes(observation))
        _write_atomic(args.manifest, canonical_json_bytes(updated))
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        LifecycleRecordingError,
    ) as exc:
        print(f"VEVO_GROWTHBOOK_CTA_LIFECYCLE_RECORD_INVALID:{exc}")
        return 2
    print(
        "VEVO_GROWTHBOOK_CTA_LIFECYCLE_RECORDED:"
        f"observation_sha256={updated['observation_sha256']}:activation=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
