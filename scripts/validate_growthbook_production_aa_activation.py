#!/usr/bin/env python3
"""Validate the hard-disabled VEVO Production A/A activation handoff."""

from __future__ import annotations

import copy
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
    "schema_version": 5,
    "activation_type": "vevo_growthbook_production_aa",
    "tracking_key": "vevo-sk-aa-001",
    "feature_key": "vevo-sk-aa-assignment",
    "variations": ["control", "variant"],
    "variation_weights": [0.5, 0.5],
    "runbook": "projects/vevo/GROWTHBOOK_PRODUCTION_AA_ACTIVATION_RUNBOOK.md",
    "status": "zero_traffic_qa_verified_activation_review_pending",
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
        "sdk_connection_created": True,
        "sdk_connection_id": "sdk_19g6lmt5wnngy",
        "sdk_client_key_committed": False,
        "experiment_created": True,
        "experiment_id": "exp_19g6mmt5wugpk",
        "feature_rule_revision": 3,
        "production_rule_publish_status": "draft_not_published",
        "status": "draft_not_started",
        "data_source_id": "ds_19g6mmt5stlp6",
        "allocation_percent": 0,
    },
    "gtm": {
        "account_id": "6254499282",
        "container_id": "198135331",
        "public_container_id": "GTM-5ZB5LFGB",
        "source_workspace_id": "16",
        "production_workspace_id": "17",
        "production_workspace_name": "VEVO GrowthBook Production A/A",
        "production_tag_created": True,
        "production_tag_ids": {
            "loader": "54",
            "consent_bridge": "51",
            "add_to_cart_bridge": "55",
            "purchase_bridge": "53",
        },
        "artifact_source_commit": "1a24b4fe657c546b6fcf71a336b9d4220622a74e",
        "artifact_sha256": (
            "d6861bcbe002a96f82a4a29882723002cd6c797177194bdd93f67e6cf2eba8df"
        ),
        "setup_tag_sequencing_verified": True,
        "unprocessed_changes": {
            "added": 5,
            "modified": 0,
            "removed": 0,
        },
        "publish_status": "not_published",
        "container_version_id": None,
    },
    "tag_assistant_qa": {
        "status": "zero_traffic_qa_verified",
        "observed_at_local_date": "2026-08-24",
        "workspace_id": "17",
        "session_connected": True,
        "desktop_consent_cycle_observed": True,
        "original_consent_categories_restored": True,
        "sdk_connection_status_after_grant": "connected",
        "experiment_status_after_grant": "draft_not_started",
        "console_error_count": 0,
        "product_path": (
            "/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute"
        ),
        "cta_class_applied": False,
        "add_to_cart_text_unchanged": True,
        "preexisting_cart_item_count": 2,
        "cart_mutated": False,
        "mobile_viewport_verified": True,
        "zero_assignment_verified": True,
        "zero_collector_request_verified": True,
        "zero_collector_observation": {
            "status": "verified_zero_requests_and_receipts",
            "workflow_run_id": "32692688625",
            "main_commit": "bed02cd3176c960d7423d97486bc67d649601241",
            "artifact_sha256": (
                "43140aa030225ac927fd6ddd92904fe8d730230174afe7525371c235accfb745"
            ),
            "from_utc": "2026-08-24T04:30:00Z",
            "through_utc": "2026-08-24T04:50:00Z",
            "observed_at_utc": "2026-08-24T05:12:49Z",
            "api_request_count": 0,
            "accepted_receipt_count": 0,
            "runtime": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.21.213",
                "service": "vevo-growthbook-collector-production",
                "runtime_path": "/app",
                "runtime_path_verification": (
                    "immutable_image_prior_localhost_marker"
                ),
                "task_id": "a3abdbcdd3914c95bb08f03b83eab5fe",
                "task_definition": "vevo-growthbook-collector-production:2",
                "image_digest": (
                    "sha256:e9aeee45f457dca5e7cb8f6a80f37763de0bb7f61c96f614d79e222fe4707058"
                ),
                "target_health": "healthy",
            },
        },
        "owned_storage_cleanup_verified": True,
        "ga4_meta_consent_behavior_verified": True,
    },
    "activation_preflight": {
        "status": "reviewed_ready_for_ordered_phase_5",
        "reviewed_at_local_date": "2026-08-24",
        "source_main_commit": "a37ac43189898550e7fa2cf31f842c1985704bd7",
        "live_readback": {
            "growthbook_build": "5.0.1+8f1db44",
            "feature_live_revision": 2,
            "feature_live_production_enabled": False,
            "feature_live_staging_enabled": True,
            "feature_live_rule_count_by_environment": {
                "production": 0,
                "staging": 1,
            },
            "preview_experiment_id": "exp_19g6mmt1qsqm9",
            "preview_experiment_status": "running_staging_only",
            "draft_feature_revision": 3,
            "draft_feature_enabled_environments": ["production", "staging"],
            "draft_rule_count_by_environment": {
                "production": 1,
                "staging": 1,
            },
            "production_experiment_id": "exp_19g6mmt5wugpk",
            "production_experiment_status": "draft_not_started",
            "production_experiment_environment": "production_only",
            "production_experiment_traffic_percent": 100,
            "production_experiment_variation_weights": [0.5, 0.5],
            "gtm_live_container_version_id": "14",
            "gtm_workspace_id": "17",
            "gtm_workspace_name": "VEVO GrowthBook Production A/A",
            "gtm_unprocessed_changes": {
                "added": 5,
                "modified": 0,
                "removed": 0,
            },
            "gtm_growthbook_objects": {
                "trigger": "50",
                "loader": "54",
                "consent_bridge": "51",
                "add_to_cart_bridge": "55",
                "purchase_bridge": "53",
            },
        },
        "evidence_bindings": {
            "zero_traffic_workflow_run_id": "32692688625",
            "zero_traffic_artifact_sha256": (
                "43140aa030225ac927fd6ddd92904fe8d730230174afe7525371c235accfb745"
            ),
            "gtm_artifact_source_commit": (
                "1a24b4fe657c546b6fcf71a336b9d4220622a74e"
            ),
            "gtm_artifact_sha256": (
                "d6861bcbe002a96f82a4a29882723002cd6c797177194bdd93f67e6cf2eba8df"
            ),
            "production_clone_observation_sha256": (
                "b2f96b7047321f11da4f00c7886c4b9422d7759428534f8fd5534ee1299f2030"
            ),
        },
        "mutation_scope": {
            "publish_gtm_workspace_17": True,
            "start_growthbook_experiment_exp_19g6mmt5wugpk": True,
            "publish_growthbook_feature_revision_3": True,
            "meta_ads": False,
            "biznisweb": False,
            "prices_or_product_content": False,
            "cart_checkout_or_orders": False,
            "cta_experiment": False,
            "collector_infrastructure": False,
        },
        "ordered_operations": [
            (
                "publish_gtm_workspace_17_from_live_version_14_while_"
                "growthbook_live_revision_2_keeps_production_disabled"
            ),
            "verify_new_gtm_live_version_and_zero_production_exposures",
            (
                "start_only_growthbook_experiment_exp_19g6mmt5wugpk_and_"
                "auto_publish_feature_revision_3"
            ),
            (
                "verify_production_100_percent_traffic_50_50_split_and_one_"
                "consented_sticky_exposure"
            ),
            (
                "record_activation_observation_in_git_before_marking_"
                "production_running"
            ),
        ],
        "rollback": {
            "first": "stop_growthbook_production_aa_and_verify_zero_assignment",
            "second": "restore_gtm_container_version_14_and_verify_loader_absent",
            "third": (
                "disable_only_collector_public_route_after_loader_is_absent"
            ),
            "delete_data_or_objects": False,
        },
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
    "next_gate": "review_controlled_production_aa_activation",
}


def _collector_verified_activation() -> dict[str, Any]:
    """Return the immutable pre-UI boundary used by the collector recorder."""

    activation = copy.deepcopy(EXPECTED_ACTIVATION)
    activation["schema_version"] = 1
    activation.pop("activation_preflight", None)
    activation["status"] = "collector_verified_ui_preparation_ready"
    activation["growthbook"] = {
        "environment": "production",
        "sdk_connection_created": False,
        "sdk_client_key_committed": False,
        "experiment_created": False,
        "experiment_id": None,
        "feature_rule_revision": None,
        "status": "not_started",
        "data_source_id": "ds_19g6mmt5stlp6",
        "allocation_percent": 0,
    }
    activation["gtm"] = {
        "account_id": "6254499282",
        "container_id": "198135331",
        "public_container_id": "GTM-5ZB5LFGB",
        "source_workspace_id": "16",
        "production_tag_created": False,
        "production_tag_id": None,
        "artifact_sha256": None,
        "publish_status": "not_published",
        "container_version_id": None,
    }
    activation.pop("tag_assistant_qa", None)
    activation["next_gate"] = (
        "prepare_growthbook_and_gtm_zero_allocation_after_review"
    )
    return activation


EXPECTED_COLLECTOR_VERIFIED_ACTIVATION = _collector_verified_activation()


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
            "Production A/A activation must match the reviewed zero-allocation UI gate"
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
