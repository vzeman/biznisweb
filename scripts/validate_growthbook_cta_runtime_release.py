#!/usr/bin/env python3
"""Fail closed before any VEVO CTA-only collector runtime deployment.

This validator is offline. It proves that the versioned A/A PASS/stop, frozen
sample, lifecycle reconciliation, workspace, and CTA-only registry agree. It
does not contain an AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, or browser client.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

try:
    from scripts import record_growthbook_aa_completion as aa_completion
    from scripts import record_growthbook_cta_activation as cta_activation
    from scripts import record_growthbook_pro_upgrade as pro_upgrade_recorder
    from scripts.validate_growthbook_meta_reporting_contract import (
        MetaReportingContractError,
        validate_contract as validate_meta_reporting_contract,
    )
except ModuleNotFoundError:  # Direct execution from scripts/.
    import record_growthbook_aa_completion as aa_completion  # type: ignore
    import record_growthbook_cta_activation as cta_activation  # type: ignore
    import record_growthbook_pro_upgrade as pro_upgrade_recorder  # type: ignore
    from validate_growthbook_meta_reporting_contract import (  # type: ignore
        MetaReportingContractError,
        validate_contract as validate_meta_reporting_contract,
    )


ROOT = Path(__file__).resolve().parents[1]
VEVO = ROOT / "projects" / "vevo"
PATHS = {
    "cta_activation": VEVO / "growthbook_cta_activation.json",
    "aa_activation": VEVO / "growthbook_production_aa_activation.json",
    "aa_completion": VEVO / "growthbook_production_aa_completion.json",
    "aa_snapshot": VEVO / "growthbook_aa_snapshot.json",
    "pro_upgrade": VEVO / "growthbook_pro_upgrade.json",
    "pro_observation": VEVO / "growthbook_pro_upgrade_observation.json",
    "sample_plan": VEVO / "growthbook_cta_sample_plan.json",
    "lifecycle": VEVO / "growthbook_cta_lifecycle_reconciliation.json",
    "lifecycle_observation": VEVO / "growthbook_cta_lifecycle_observation.json",
    "design": VEVO / "growthbook_cta_design.json",
    "decision": VEVO / "growthbook_cta_decision_contract.json",
    "meta_reporting": VEVO / "growthbook_meta_reporting_contract.json",
    "workspace": VEVO / "growthbook_workspace.json",
    "registry": ROOT / "growthbook_collector" / "experiments.json",
    "storefront": ROOT / "storefront" / "vevo-growthbook" / "vevo-growthbook.js",
}


class CtaRuntimeReleaseError(ValueError):
    """Raised when the future CTA runtime deployment gate is not exact."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaRuntimeReleaseError(message)


def _load(path: Path, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CtaRuntimeReleaseError(f"unable to load {field}") from exc
    _require(isinstance(value, dict), f"{field} must be an object")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_release_state(
    *,
    manifest: Mapping[str, Any],
    completion: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    pro_upgrade: Mapping[str, Any],
    pro_upgrade_raw: bytes,
    pro_observation: Mapping[str, Any],
    pro_observation_raw: bytes,
    sample: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    lifecycle_observation: Mapping[str, Any],
    lifecycle_observation_raw: bytes,
    workspace: Mapping[str, Any],
    registry: Mapping[str, Any],
    stop_observation: Mapping[str, Any],
    stop_observation_raw: bytes,
    design_sha256: str,
    decision_sha256: str,
    meta_reporting: Mapping[str, Any],
    meta_reporting_sha256: str,
    registry_sha256: str,
    storefront_source: str,
) -> None:
    cta_activation.validate_manifest(manifest)
    _require(
        manifest["status"] == cta_activation.WAITING,
        "CTA activation manifest is not waiting",
    )
    cta_activation._validate_post_aa_sources(  # noqa: SLF001 - shared release contract
        completion,
        snapshot,
        pro_upgrade,
        pro_observation,
        sample,
        lifecycle,
        lifecycle_observation,
        workspace,
        registry,
    )
    _require(
        lifecycle_observation_raw
        == cta_activation.canonical_json_bytes(lifecycle_observation)
        and hashlib.sha256(lifecycle_observation_raw).hexdigest()
        == lifecycle["observation_sha256"],
        "CTA lifecycle observation file/hash binding drift",
    )
    pro_bindings = manifest["source_bindings"]
    _require(
        pro_upgrade_raw == pro_upgrade_recorder.canonical_json_bytes(pro_upgrade)
        and (
            manifest["status"] == cta_activation.WAITING
            or hashlib.sha256(pro_upgrade_raw).hexdigest()
            == pro_bindings["pro_upgrade"]["sha256"]
        ),
        "GrowthBook Pro manifest file/hash binding drift",
    )
    _require(
        pro_observation_raw
        == pro_upgrade_recorder.canonical_json_bytes(pro_observation)
        and hashlib.sha256(pro_observation_raw).hexdigest()
        == pro_upgrade["verification"]["observation_sha256"],
        "GrowthBook Pro observation file/hash binding drift",
    )
    if manifest["status"] != cta_activation.WAITING:
        _require(
            hashlib.sha256(pro_observation_raw).hexdigest()
            == pro_bindings["pro_upgrade_observation"]["sha256"],
            "GrowthBook Pro observation CTA binding drift",
        )
    aa_completion.validate_observation(stop_observation, completion)
    expected_stop_hash = completion["stop_readback"]["observation_sha256"]
    _require(
        hashlib.sha256(stop_observation_raw).hexdigest() == expected_stop_hash,
        "A/A stop observation file/hash binding drift",
    )
    _require(
        stop_observation_raw == aa_completion.canonical_json_bytes(stop_observation),
        "A/A stop observation is not canonical JSON",
    )
    _require(
        design_sha256 == cta_activation.EXPECTED_STATIC_HASHES["design_contract"],
        "CTA design contract SHA-256 drift",
    )
    _require(
        decision_sha256 == cta_activation.EXPECTED_STATIC_HASHES["decision_contract"],
        "CTA decision contract SHA-256 drift",
    )
    validate_meta_reporting_contract(meta_reporting)
    _require(
        meta_reporting_sha256
        == cta_activation.EXPECTED_STATIC_HASHES["meta_reporting_contract"]
        == manifest["source_bindings"]["meta_reporting_contract"]["sha256"],
        "CTA Meta reporting contract SHA-256 drift",
    )
    _require(
        len(registry_sha256) == 64
        and all(character in "0123456789abcdef" for character in registry_sha256),
        "CTA registry SHA-256 is invalid",
    )
    _require(
        storefront_source.count("var PRODUCTION_ACTIVATION = false;") == 1
        and "var PRODUCTION_ACTIVATION = true;" not in storefront_source,
        "checked-in storefront must remain compile-time Production-disabled",
    )


def validate_checked_in_release() -> str:
    completion = _load(PATHS["aa_completion"], "A/A completion")
    stop_path = VEVO / str(
        completion.get("stop_readback", {}).get("observation_file") or ""
    ).removeprefix("projects/vevo/")
    _require(stop_path.parent == VEVO, "A/A stop observation path drift")
    stop_raw = stop_path.read_bytes()
    stop_observation = _load(stop_path, "A/A stop observation")
    snapshot = _load(PATHS["aa_snapshot"], "A/A snapshot manifest")
    pro_upgrade_raw = PATHS["pro_upgrade"].read_bytes()
    pro_upgrade = _load(PATHS["pro_upgrade"], "GrowthBook Pro manifest")
    pro_observation_raw = PATHS["pro_observation"].read_bytes()
    pro_observation = _load(PATHS["pro_observation"], "GrowthBook Pro observation")
    aa_completion.validate_manifest(
        completion,
        _load(PATHS["aa_activation"], "A/A activation"),
        snapshot,
        observation=stop_observation,
    )
    registry_hash = _sha256(PATHS["registry"])
    lifecycle_observation_raw = PATHS["lifecycle_observation"].read_bytes()
    validate_release_state(
        manifest=_load(PATHS["cta_activation"], "CTA activation"),
        completion=completion,
        snapshot=snapshot,
        pro_upgrade=pro_upgrade,
        pro_upgrade_raw=pro_upgrade_raw,
        pro_observation=pro_observation,
        pro_observation_raw=pro_observation_raw,
        sample=_load(PATHS["sample_plan"], "CTA sample plan"),
        lifecycle=_load(PATHS["lifecycle"], "CTA lifecycle reconciliation"),
        lifecycle_observation=_load(
            PATHS["lifecycle_observation"], "CTA lifecycle observation"
        ),
        lifecycle_observation_raw=lifecycle_observation_raw,
        workspace=_load(PATHS["workspace"], "GrowthBook workspace"),
        registry=_load(PATHS["registry"], "collector registry"),
        stop_observation=stop_observation,
        stop_observation_raw=stop_raw,
        design_sha256=_sha256(PATHS["design"]),
        decision_sha256=_sha256(PATHS["decision"]),
        meta_reporting=_load(PATHS["meta_reporting"], "Meta reporting contract"),
        meta_reporting_sha256=_sha256(PATHS["meta_reporting"]),
        registry_sha256=registry_hash,
        storefront_source=PATHS["storefront"].read_text(encoding="utf-8"),
    )
    return registry_hash


def main() -> int:
    try:
        registry_hash = validate_checked_in_release()
    except (
        CtaRuntimeReleaseError,
        aa_completion.AaCompletionRecordingError,
        cta_activation.CtaActivationRecordingError,
        pro_upgrade_recorder.ProUpgradeError,
        MetaReportingContractError,
        OSError,
        KeyError,
    ) as exc:
        print(f"validate_growthbook_cta_runtime_release.py: FAIL: {exc}")
        return 2
    print(
        "VEVO_CTA_RUNTIME_RELEASE_GATE_OK:"
        f"registry_sha256={registry_hash}:growthbook=zero:gtm=15-clean:automatic-ui-mutation=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
