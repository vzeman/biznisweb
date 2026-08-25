#!/usr/bin/env python3
"""Validate the checked-in VEVO Production A/A completion lifecycle."""

from __future__ import annotations

import json
from pathlib import Path

try:
    from scripts.record_growthbook_aa_completion import (
        DEFAULT_COMPLETION_PATH,
        DEFAULT_OBSERVATION_PATH,
        AaCompletionRecordingError,
        canonical_json_bytes,
        validate_manifest,
    )
    from scripts.validate_growthbook_aa_measurement_window import (
        ACTIVATION_PATH,
        SNAPSHOT_PATH,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from record_growthbook_aa_completion import (
        DEFAULT_COMPLETION_PATH,
        DEFAULT_OBSERVATION_PATH,
        AaCompletionRecordingError,
        canonical_json_bytes,
        validate_manifest,
    )
    from validate_growthbook_aa_measurement_window import ACTIVATION_PATH, SNAPSHOT_PATH


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AaCompletionRecordingError(f"{path.name} must contain an object")
    return value


def validate() -> None:
    completion = _load(DEFAULT_COMPLETION_PATH)
    activation = _load(ACTIVATION_PATH)
    snapshot = _load(SNAPSHOT_PATH)
    observation = None
    completed = (
        completion.get("status")
        == "production_aa_stopped_verified_cta_activation_blocked"
    )
    if completed:
        raw = DEFAULT_OBSERVATION_PATH.read_bytes()
        observation = json.loads(raw.decode("utf-8"))
        if raw != canonical_json_bytes(observation):
            raise AaCompletionRecordingError(
                "A/A stop observation is not canonical JSON"
            )
    elif DEFAULT_OBSERVATION_PATH.exists():
        raise AaCompletionRecordingError(
            "A/A stop observation exists before completion"
        )
    validate_manifest(
        completion,
        activation,
        snapshot,
        observation=observation,
    )


def main() -> int:
    try:
        validate()
        print("validate_growthbook_aa_completion.py: OK")
        return 0
    except (AaCompletionRecordingError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        print(f"validate_growthbook_aa_completion.py: FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
