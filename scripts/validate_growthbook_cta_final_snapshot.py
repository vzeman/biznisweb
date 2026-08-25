#!/usr/bin/env python3
"""Validate the checked-in VEVO CTA protected final-snapshot contract."""

from __future__ import annotations

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


def main() -> None:
    try:
        validate_manifest(_load(DEFAULT_MANIFEST_PATH, "CTA final snapshot manifest"))
    except (OSError, CtaFinalSnapshotError) as exc:
        raise SystemExit(f"VEVO_CTA_FINAL_SNAPSHOT_INVALID:{exc}") from exc
    print(
        "VEVO_CTA_FINAL_SNAPSHOT_VALID:"
        "one-look=true:aggregate-only=true:identities=false:mutation=false"
    )


if __name__ == "__main__":
    main()
