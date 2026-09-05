#!/usr/bin/env python3
"""Build canonical automated A/A evidence from sanitized Production reads.

The observation is assembled only by the protected Production evidence
workflow after its bounded CloudWatch, Athena, curated-reporting, schema, and
runtime checks pass.  This offline builder validates the exact aggregate
schema and injects the successful workflow run/main commit provenance.  It has
no AWS, GrowthBook, GTM, Meta, BiznisWeb, browser, or network client.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts.assemble_growthbook_aa_snapshot import (
    AUTOMATED_KEYS,
    SnapshotAssemblyError,
    _exact,
    _validate_nested_automated,
)


RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^20[2-9][0-9]-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
OBSERVATION_KEYS = (AUTOMATED_KEYS - {"source_run_id", "source_main_commit"}) | {
    "observation_type"
}


class AutomatedEvidenceError(ValueError):
    """Raised when sanitized Production reads cannot support evidence."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AutomatedEvidenceError(message)


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def load_canonical_observation(path: Path, expected_sha256: str) -> dict[str, Any]:
    _require(SHA256_RE.fullmatch(expected_sha256) is not None, "observation SHA-256 is invalid")
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AutomatedEvidenceError("automated observation is unreadable") from exc
    _require(isinstance(payload, dict), "automated observation must contain an object")
    _require(raw == _canonical_json(payload), "automated observation must use canonical JSON bytes")
    _require(hashlib.sha256(raw).hexdigest() == expected_sha256, "observation SHA-256 mismatch")
    return payload


def build_automated_evidence(
    observation: Mapping[str, Any], *, workflow_run_id: str, main_commit: str
) -> dict[str, Any]:
    """Validate one sanitized observation and bind it to producer provenance."""

    try:
        observation = _exact(observation, OBSERVATION_KEYS, "automated observation")
    except SnapshotAssemblyError as exc:
        raise AutomatedEvidenceError(str(exc)) from exc
    _require(type(observation["schema_version"]) is int and observation["schema_version"] == 2, "automated observation schema drift")
    _require(
        observation["observation_type"]
        == "vevo_growthbook_aa_automated_observation",
        "automated observation type drift",
    )
    _require(
        observation["evidence_type"] == "pending_workflow_provenance",
        "automated observation evidence type drift",
    )
    _require(observation["experiment_id"] == "vevo-sk-aa-001", "automated experiment drift")
    _require(UTC_RE.fullmatch(str(observation["from_utc"])) is not None, "automated from_utc drift")
    _require(UTC_RE.fullmatch(str(observation["through_utc"])) is not None, "automated through_utc drift")
    _require(observation["through_utc"] > observation["from_utc"], "automated window is empty")
    _require(RUN_ID_RE.fullmatch(workflow_run_id) is not None, "workflow run ID is invalid")
    _require(COMMIT_RE.fullmatch(main_commit) is not None, "main commit is invalid")

    evidence = {
        key: value
        for key, value in observation.items()
        if key not in {"observation_type", "evidence_type"}
    }
    evidence.update(
        {
            "evidence_type": "vevo_growthbook_aa_automated_evidence",
            "source_run_id": workflow_run_id,
            "source_main_commit": main_commit,
        }
    )
    _exact(evidence, AUTOMATED_KEYS, "automated evidence")
    try:
        _validate_nested_automated(evidence)
    except SnapshotAssemblyError as exc:
        raise AutomatedEvidenceError(f"automated evidence is invalid: {exc}") from exc
    _require(evidence["source_read_only"] is True, "automated source must be read-only")
    for field in (
        "contains_raw_aws_payloads",
        "contains_cloudwatch_messages",
        "contains_event_or_device_ids",
        "contains_customer_or_order_data",
        "mutation_observed",
    ):
        _require(evidence[field] is False, f"automated {field} must be false")
    return evidence


def _write_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical_json(payload))
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
    parser.add_argument("--observation", required=True, type=Path)
    parser.add_argument("--observation-sha256", required=True)
    parser.add_argument("--workflow-run-id", required=True)
    parser.add_argument("--main-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        observation = load_canonical_observation(args.observation, args.observation_sha256)
        evidence = build_automated_evidence(
            observation,
            workflow_run_id=args.workflow_run_id,
            main_commit=args.main_commit,
        )
        _write_atomic(args.output, evidence)
    except (OSError, AutomatedEvidenceError, SnapshotAssemblyError) as exc:
        print(f"VEVO_GROWTHBOOK_AA_AUTOMATED_INVALID:{exc}")
        return 2
    print(
        "VEVO_GROWTHBOOK_AA_AUTOMATED_READY:"
        f"run={evidence['source_run_id']}:experiment={evidence['experiment_id']}:"
        "source-read-only=true:raw=false:identities=false:winner=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
