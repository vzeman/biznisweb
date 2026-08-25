#!/usr/bin/env python3
"""Validate the checked-in VEVO CTA protected final-snapshot contract."""

from __future__ import annotations

import json

try:
    from build_growthbook_cta_final_snapshot import (
        CtaFinalSnapshotError,
        DEFAULT_MANIFEST_PATH,
        _load,
        validate_manifest,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_cta_final_snapshot.
    from scripts.build_growthbook_cta_final_snapshot import (
        CtaFinalSnapshotError,
        DEFAULT_MANIFEST_PATH,
        _load,
        validate_manifest,
    )

try:
    from validate_growthbook_hypothesis_registry import (
        DEFAULT_REGISTRY_PATH,
        HypothesisRegistryError,
        pretty_json_bytes,
        validate_registry,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_cta_final_snapshot.
    from scripts.validate_growthbook_hypothesis_registry import (
        DEFAULT_REGISTRY_PATH,
        HypothesisRegistryError,
        pretty_json_bytes,
        validate_registry,
    )


def main() -> None:
    try:
        manifest = _load(DEFAULT_MANIFEST_PATH, "CTA final snapshot manifest")
        registry_bytes = DEFAULT_REGISTRY_PATH.read_bytes()
        registry = json.loads(registry_bytes.decode("utf-8"))
        if registry_bytes != pretty_json_bytes(registry):
            raise HypothesisRegistryError(
                "hypothesis registry is not canonical pretty JSON"
            )
        validate_manifest(manifest)
        validate_registry(registry, manifest)
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        CtaFinalSnapshotError,
        HypothesisRegistryError,
    ) as exc:
        raise SystemExit(f"VEVO_CTA_FINAL_SNAPSHOT_INVALID:{exc}") from exc
    print(
        "VEVO_CTA_FINAL_SNAPSHOT_VALID:"
        "one-look=true:aggregate-only=true:identities=false:mutation=false"
    )


if __name__ == "__main__":
    main()
