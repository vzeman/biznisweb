#!/usr/bin/env python3
"""Validate the hard-disabled VEVO Production A/A activation handoff."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
ACTIVATION_PATH = ROOT / "projects" / "vevo" / "growthbook_production_aa_activation.json"
WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"
REGISTRY_PATH = ROOT / "growthbook_collector" / "experiments.json"
RUNBOOK_PATH = ROOT / "projects" / "vevo" / "GROWTHBOOK_PRODUCTION_AA_ACTIVATION_RUNBOOK.md"
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "deploy-vevo-growthbook-production-aa-collector.yml"
STOREFRONT_PATH = ROOT / "storefront" / "vevo-growthbook" / "vevo-growthbook.js"


EXPECTED_ACTIVATION = {
    "schema_version": 1,
    "activation_type": "vevo_growthbook_production_aa",
    "tracking_key": "vevo-sk-aa-001",
    "feature_key": "vevo-sk-aa-assignment",
    "variations": ["control", "variant"],
    "variation_weights": [0.5, 0.5],
    "runbook": "projects/vevo/GROWTHBOOK_PRODUCTION_AA_ACTIVATION_RUNBOOK.md",
    "status": "collector_verified_ui_preparation_ready",
    "preconditions": {
        "natural_reconciliation_verified": True,
        "route_disabled_foundation_verified": True,
        "production_reader_verified": True,
        "growthbook_clone_verified": True,
    },
    "collector": {
        "deployment_workflow": (
            ".github/workflows/deploy-vevo-growthbook-production-aa-collector.yml"
        ),
        "deployment_allowed": False,
        "registry_entry_present": True,
        "public_route_enabled": True,
        "workflow_run_id": "32644408714",
        "main_commit": "57b29c3b166eabbbabee4d3b8e69d1b56e2ae8e2",
        "image_digest": (
            "sha256:e9aeee45f457dca5e7cb8f6a80f37763de0bb7f61c96f614d79e222fe4707058"
        ),
        "task_definition": "vevo-growthbook-collector-production:2",
        "host_gate_task_id": "53b11cab55a94f69938121ce61243015",
        "host_gate_private_ip": "172.31.13.22",
        "service": "vevo-growthbook-collector-production",
        "runtime_path": "/app",
        "endpoint_host_sha256": (
            "679ab80bc9487c08c6c2e6782abc4492a59cd04d920885082f38e8e6e29e95dc"
        ),
        "evidence_sha256": (
            "1e156ebdd94f88f7858c0e0b2ddb443fdabe01787ee6f7d673ac80197492ab88"
        ),
    },
    "growthbook": {
        "environment": "production",
        "sdk_connection_created": False,
        "sdk_client_key_committed": False,
        "experiment_created": False,
        "experiment_id": None,
        "feature_rule_revision": None,
        "status": "not_started",
        "data_source_id": "ds_19g6mmt5stlp6",
        "allocation_percent": 0,
    },
    "gtm": {
        "account_id": "6254499282",
        "container_id": "198135331",
        "public_container_id": "GTM-5ZB5LFGB",
        "source_workspace_id": "16",
        "production_tag_created": False,
        "production_tag_id": None,
        "artifact_sha256": None,
        "publish_status": "not_published",
        "container_version_id": None,
    },
    "traffic": {
        "activation_allowed": False,
        "production_allocation_percent": 0,
        "active_production_experiments": [],
        "cta_experiment_started": False,
    },
    "safety": {
        "price_tests_allowed": False,
        "meta_ads_mutated": False,
        "biznisweb_mutated": False,
        "cart_checkout_mutated": False,
        "contains_credentials": False,
        "contains_customer_or_order_data": False,
    },
    "rollback": {
        "order": [
            "set_growthbook_allocation_to_zero",
            "restore_previous_gtm_container_version",
            "disable_collector_public_route",
        ],
        "drill_status": "not_run",
        "verified_at_utc": None,
    },
    "next_gate": "prepare_growthbook_and_gtm_zero_allocation_after_review",
}


def _load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path.name} must contain an object")
    return payload


def validate_activation_handoff(
    activation: Mapping[str, Any],
    workspace: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> None:
    if dict(activation) != EXPECTED_ACTIVATION:
        raise AssertionError(
            "Production A/A activation must match the reviewed collector deployment gate"
        )

    if workspace.get("workspace", {}).get("production_allocation_percent") != 0:
        raise AssertionError("Production A/A activation requires zero workspace allocation")
    if workspace.get("decision_gates", {}).get("production_activation_allowed") is not False:
        raise AssertionError("Production A/A activation decision gate must remain false")
    if workspace.get("gtm_preview_workspace", {}).get("publish_status") != "not_published":
        raise AssertionError("Production A/A activation requires unpublished GTM")

    experiments = {
        row.get("tracking_key"): row
        for row in workspace.get("experiments", [])
        if isinstance(row, dict)
    }
    aa = experiments.get("vevo-sk-aa-001") or {}
    cta = experiments.get("vevo-sk-product-cta-color-001") or {}
    if (
        aa.get("feature_key") != EXPECTED_ACTIVATION["feature_key"]
        or aa.get("variations") != EXPECTED_ACTIVATION["variations"]
        or aa.get("variation_weights") != EXPECTED_ACTIVATION["variation_weights"]
        or aa.get("production_allocation_percent") != 0
    ):
        raise AssertionError("Production A/A activation experiment contract drift")
    if cta.get("status") != "draft" or cta.get("production_allocation_percent") != 0:
        raise AssertionError("CTA experiment must remain stopped before Production A/A")

    natural = workspace.get("reconciliation_checkpoint", {}).get(
        "recurring_schedule", {}
    )
    production = workspace.get("athena", {}).get("production", {})
    clone = production.get("growthbook_clone", {})
    if (
        natural.get("natural_verifier_status") != "passed_retention_recovery_run"
        or natural.get("natural_evidence_artifact_status")
        != "verified_downloaded_sha256_recorded"
    ):
        raise AssertionError("Natural reconciliation evidence is not verified")
    if (
        production.get("status") != "route_disabled_foundation_deployed_verified"
        or production.get("foundation_evidence_artifact_status")
        != "verified_downloaded_sha256_recorded"
    ):
        raise AssertionError("Production foundation evidence is not verified")
    if (
        production.get("reader_provisioning_status")
        != "verified_active_encrypted_handoff_ready_for_growthbook"
        or production.get("reader_evidence_artifact_status")
        != "verified_downloaded_sha256_recorded"
    ):
        raise AssertionError("Production reader evidence is not verified")
    if (
        clone.get("status") != "verified_complete"
        or clone.get("mutation_status") != "created_and_query_verified"
        or clone.get("observation_status")
        != "verified_canonical_sha256_recorded"
    ):
        raise AssertionError("Production GrowthBook clone evidence is not verified")
    if EXPECTED_ACTIVATION["growthbook"]["data_source_id"] != clone.get(
        "target_data_source_id"
    ):
        raise AssertionError("Production GrowthBook data source ID drift")

    environments = registry.get("environments") or {}
    preview_aa = (environments.get("preview") or {}).get("vevo-sk-aa-001")
    if environments.get("production") != {"vevo-sk-aa-001": preview_aa}:
        raise AssertionError(
            "Production collector registry must contain only the exact A/A contract"
        )
    if "vevo-sk-product-cta-color-001" in environments.get("production", {}):
        raise AssertionError("CTA experiment is forbidden in the Production registry")

    storefront = STOREFRONT_PATH.read_text(encoding="utf-8")
    if storefront.count("var PRODUCTION_ACTIVATION = false;") != 1:
        raise AssertionError("storefront Production activation compile-time gate drift")
    if "var PRODUCTION_ACTIVATION = true;" in storefront:
        raise AssertionError("storefront Production activation is unexpectedly enabled")

    runbook = RUNBOOK_PATH.read_text(encoding="utf-8")
    required_runbook_markers = (
        "## Phase 1 — reviewed collector preparation",
        "## Phase 2 — deploy and verify the Production collector",
        "## Phase 3 — prepare GrowthBook and GTM at zero allocation",
        "## Phase 4 — Tag Assistant zero-traffic QA",
        "## Phase 5 — controlled A/A activation",
        "## Rollback",
        "Set GrowthBook Production A/A allocation to `0%`",
        "Restore the previous GTM container version",
        "Disable only `CollectorPostRoute`",
        "An A/A pass never declares a winner",
    )
    for marker in required_runbook_markers:
        if marker not in runbook:
            raise AssertionError(f"Production A/A runbook marker missing: {marker}")

    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    ordered_workflow_markers = (
        "Fail closed on the reviewed activation gate before AWS credentials",
        "Configure AWS credentials",
        "Confirm exact Production instance, IP, service, and path before image deployment",
        "Build and publish the immutable Production A/A collector image",
        "Update only the route-disabled collector runtime",
        "Run the exact Production Fargate localhost hard gate",
        "Activate only the public collector route after the host gate",
        "Verify public isolation and byte-identical raw data after invalid probes",
        "Upload sanitized Production collector activation evidence",
    )
    positions = [workflow.find(marker) for marker in ordered_workflow_markers]
    if any(position < 0 for position in positions) or positions != sorted(positions):
        raise AssertionError("Production A/A collector workflow gate order drift")
    required_workflow_markers = (
        "--phase candidate",
        "--phase activate",
        "--phase deactivate",
        "COLLECTOR_LOCALHOST_HEALTH_OK:production",
        "COLLECTOR_LOCALHOST_MARKER_OK:/app",
        "set -a\n          source production-stack.env\n          set +a",
        "ensure_ascii=False",
        "indent=2",
        "failure() && env.ROUTE_ACTIVATED == 'true'",
        "growthbook_mutations': False",
        "gtm_mutations': False",
        "meta_ads_mutations': False",
        "biznisweb_mutations': False",
        "commerce_mutations': False",
    )
    for marker in required_workflow_markers:
        if marker not in workflow:
            raise AssertionError(f"Production A/A collector workflow marker missing: {marker}")


def validate() -> None:
    validate_activation_handoff(
        _load(ACTIVATION_PATH),
        _load(WORKSPACE_PATH),
        _load(REGISTRY_PATH),
    )


def main() -> int:
    try:
        validate()
        print("validate_growthbook_production_aa_activation.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - command failure path
        print(f"validate_growthbook_production_aa_activation.py: FAIL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
