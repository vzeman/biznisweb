"""Validate the current VEVO GrowthBook Pro transition without external access."""

from __future__ import annotations

import hashlib
import json

try:
    from scripts.record_growthbook_pro_upgrade import (
        COMPLETION_PATH,
        MANIFEST_PATH,
        OBSERVATION_PATH,
        REVIEW_OPEN,
        VERIFIED,
        WORKSPACE_PATH,
        ProUpgradeError,
        canonical_json_bytes,
        validate_manifest,
        validate_observation,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from record_growthbook_pro_upgrade import (
        COMPLETION_PATH,
        MANIFEST_PATH,
        OBSERVATION_PATH,
        REVIEW_OPEN,
        VERIFIED,
        WORKSPACE_PATH,
        ProUpgradeError,
        canonical_json_bytes,
        validate_manifest,
        validate_observation,
    )


def main() -> int:
    try:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        workspace = json.loads(WORKSPACE_PATH.read_text(encoding="utf-8"))
        completion = json.loads(COMPLETION_PATH.read_text(encoding="utf-8"))
        validate_manifest(manifest, workspace)
        status = manifest["status"]
        if status == REVIEW_OPEN:
            if hashlib.sha256(canonical_json_bytes(completion)).hexdigest() != manifest["source_bindings"]["aa_completion"]["sha256"]:
                raise ProUpgradeError("A/A completion changed after GrowthBook Pro review")
            if hashlib.sha256(canonical_json_bytes(workspace)).hexdigest() != manifest["source_bindings"]["workspace_before_upgrade"]["sha256"]:
                raise ProUpgradeError("GrowthBook workspace changed after GrowthBook Pro review")
        elif status == VERIFIED:
            observation_bytes = OBSERVATION_PATH.read_bytes()
            observation = json.loads(observation_bytes)
            if observation_bytes != canonical_json_bytes(observation):
                raise ProUpgradeError("GrowthBook Pro observation is not canonical JSON")
            if hashlib.sha256(observation_bytes).hexdigest() != manifest["verification"]["observation_sha256"]:
                raise ProUpgradeError("GrowthBook Pro observation SHA-256 drift")
            validate_observation(observation, manifest, workspace)
    except (OSError, json.JSONDecodeError, ProUpgradeError, TypeError, KeyError) as exc:
        print(f"validate_growthbook_pro_upgrade.py: FAIL: {exc}")
        return 1
    print("validate_growthbook_pro_upgrade.py: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
