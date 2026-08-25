#!/usr/bin/env python3
"""Validate the checked-in VEVO CTA stop and follow-up completion state."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

try:
    from scripts.record_growthbook_cta_completion import (
        ACTIVATION_PATH,
        COMPLETION_PATH,
        DECISION_CONTRACT_PATH,
        FOLLOWUP,
        LIFECYCLE_PATH,
        MEASUREMENT_PATH,
        RECONCILIATION_PATH,
        SAMPLE_PLAN_PATH,
        START_OBSERVATION_PATH,
        STOP_OBSERVATION_PATH,
        WORKSPACE_PATH,
        canonical_json_bytes,
        validate_manifest,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from record_growthbook_cta_completion import (
        ACTIVATION_PATH,
        COMPLETION_PATH,
        DECISION_CONTRACT_PATH,
        FOLLOWUP,
        LIFECYCLE_PATH,
        MEASUREMENT_PATH,
        RECONCILIATION_PATH,
        SAMPLE_PLAN_PATH,
        START_OBSERVATION_PATH,
        STOP_OBSERVATION_PATH,
        WORKSPACE_PATH,
        canonical_json_bytes,
        validate_manifest,
    )


ROOT = Path(__file__).resolve().parents[1]


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain an object")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    try:
        completion = _load(COMPLETION_PATH)
        activation = _load(ACTIVATION_PATH)
        measurement = _load(MEASUREMENT_PATH)
        sample = _load(SAMPLE_PLAN_PATH)
        contract = _load(DECISION_CONTRACT_PATH)
        lifecycle = _load(LIFECYCLE_PATH)
        reconciliation = _load(RECONCILIATION_PATH)
        source_hashes = {
            "activation": _sha256(ACTIVATION_PATH),
            "measurement_window": _sha256(MEASUREMENT_PATH),
            "sample_plan": _sha256(SAMPLE_PLAN_PATH),
            "decision_contract": _sha256(DECISION_CONTRACT_PATH),
            "lifecycle_reconciliation": _sha256(LIFECYCLE_PATH),
            "start_observation": "",
            "reconciliation_evidence": _sha256(RECONCILIATION_PATH),
        }
        lifecycle_observation = None
        start_observation = None
        stop_observation = None
        workspace = None
        if completion.get("status") == FOLLOWUP:
            start_raw = START_OBSERVATION_PATH.read_bytes()
            start_observation = json.loads(start_raw.decode("utf-8"))
            if start_raw != canonical_json_bytes(start_observation):
                raise ValueError("CTA start observation is not canonical JSON")
            stop_raw = STOP_OBSERVATION_PATH.read_bytes()
            stop_observation = json.loads(stop_raw.decode("utf-8"))
            if stop_raw != canonical_json_bytes(stop_observation):
                raise ValueError("CTA stop observation is not canonical JSON")
            source_hashes["start_observation"] = _sha256(START_OBSERVATION_PATH)
            workspace = _load(WORKSPACE_PATH)
            if lifecycle.get("verified") is True:
                lifecycle_path = (ROOT / str(lifecycle["observation_path"])).resolve()
                lifecycle_raw = lifecycle_path.read_bytes()
                lifecycle_observation = json.loads(lifecycle_raw.decode("utf-8"))
                if lifecycle_raw != canonical_json_bytes(lifecycle_observation):
                    raise ValueError("CTA lifecycle observation is not canonical JSON")
        validate_manifest(
            completion,
            activation,
            measurement,
            sample,
            contract,
            lifecycle,
            reconciliation,
            lifecycle_observation=lifecycle_observation,
            start_observation=start_observation,
            stop_observation=stop_observation,
            workspace=workspace,
            source_hashes=source_hashes,
        )
        print("validate_growthbook_cta_completion.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(f"validate_growthbook_cta_completion.py: FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
