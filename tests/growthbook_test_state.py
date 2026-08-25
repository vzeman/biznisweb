from __future__ import annotations

import copy
from typing import Any, Mapping


def pre_activation_workspace(workspace: Mapping[str, Any]) -> dict[str, Any]:
    """Reconstruct the exact zero-allocation workspace used by historical tests."""

    result = copy.deepcopy(dict(workspace))
    result["state"] = (
        "preview_aa_runtime_and_reconciliation_verified_"
        "recurring_schedule_pending_pro_quantiles_blocked"
    )
    result["workspace"]["production_allocation_percent"] = 0
    experiments = {
        row["tracking_key"]: row for row in result["experiments"]
    }
    aa = experiments["vevo-sk-aa-001"]
    aa.update(
        {
            "name": "VEVO SK A/A measurement validation",
            "growthbook_id": "exp_19g6mmt1qsqm9",
            "status": "running_preview_staging_only",
            "started_date": "2026-08-21",
            "feature_rule_revision": 2,
            "feature_rule_environments": ["staging"],
            "production_allocation_percent": 0,
            "analysis_settings": {
                "verified_date": "2026-08-21",
                "data_source_id": "ds_19g6mmt2c4dmn",
                "data_source_name": "VEVO Preview Experiment Facts",
                "assignment_query_id": "tbl_mt2c74ol",
                "assignment_query_name": "VEVO consented devices",
                "statistics_engine": "bayesian_default",
                "cuped_enabled": False,
                "post_stratification_enabled": False,
                "activation_metric": None,
                "goal_metrics": ["vevo_add_to_cart_24h"],
                "secondary_metrics": [
                    "vevo_average_order_value_7d",
                    "vevo_cancelled_order_rate_14d",
                    "vevo_cm1_per_exposed_device_7d",
                    "vevo_revenue_per_exposed_device_7d",
                    "vevo_purchase_conversion_7d",
                    "vevo_refunded_order_rate_14d",
                ],
                "guardrail_metrics": ["vevo_client_error_device_rate_24h"],
            },
        }
    )
    aa.pop("activation_evidence", None)
    cta = experiments["vevo-sk-product-cta-color-001"]
    cta["status"] = "draft"
    cta["feature_rule_status"] = "draft"
    result["decision_gates"]["production_activation_allowed"] = False
    return result


def cta_sample_freeze_ready_workspace(
    workspace: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the post-A/A, pre-CTA-freeze safety state."""

    result = copy.deepcopy(dict(workspace))
    experiments = {
        row["tracking_key"]: row for row in result["experiments"]
    }
    cta = experiments["vevo-sk-product-cta-color-001"]
    cta["status"] = "draft"
    cta["feature_rule_status"] = "draft"
    result["decision_gates"]["production_activation_allowed"] = False
    return result


def pending_natural_evidence_workspace(
    workspace: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the historical pre-natural-evidence state used by recorder tests."""

    result = pre_activation_workspace(workspace)
    checkpoint = result["reconciliation_checkpoint"]
    recurring = checkpoint["recurring_schedule"]
    production = result["athena"]["production"]

    checkpoint["recurring_schedule_status"] = (
        "enabled_one_shot_verified_natural_retention_recovery_pending"
    )
    recurring["first_natural_run_status"] = (
        "success_marker_observed_ecs_state_expired_recovery_pending"
    )
    recurring["natural_verifier_status"] = (
        "prepared_second_natural_run_retention_recovery"
    )
    recurring["natural_evidence_artifact_status"] = (
        "code_prepared_retention_recovery_pending"
    )
    recurring["natural_verifier_run_id"] = None
    recurring["natural_verifier_main_commit"] = None
    recurring["natural_evidence_artifact_sha256"] = None
    recurring["natural_verifier_evidence"] = None

    production["status"] = "read_only_preflight_passed_natural_run_gate_pending"
    production["deployment_allowed"] = False
    production["foundation_deployment_status"] = (
        "code_prepared_natural_run_gate_pending"
    )
    production["foundation_deployment_allowed"] = False
    production["foundation_evidence_schema_version"] = 1
    production["foundation_evidence_artifact_status"] = (
        "code_prepared_deployment_pending"
    )
    production["foundation_deployment_run_id"] = None
    production["foundation_deployment_main_commit"] = None
    production["foundation_evidence_artifact_sha256"] = None
    production["successful_foundation_deployment"] = None
    production["credentials_created"] = False
    production["reader_provisioning_status"] = (
        "code_prepared_foundation_gate_pending"
    )
    production["reader_provisioning_allowed"] = False
    production["reader_evidence_artifact_status"] = (
        "code_prepared_provisioning_pending"
    )
    production["reader_provisioning_run_id"] = None
    production["reader_provisioning_main_commit"] = None
    production["reader_evidence_artifact_sha256"] = None
    production["successful_reader_provisioning"] = None
    clone = production["growthbook_clone"]
    clone["status"] = "code_prepared_foundation_reader_gate_pending"
    clone["clone_allowed"] = False
    clone["mutation_status"] = "not_started"
    clone["observation_status"] = "not_recorded"
    clone["observation_sha256"] = None
    clone["successful_clone_verification"] = None
    clone["target_data_source_id"] = None
    clone["target_fact_table_ids"] = {
        key: None for key in clone["source_fact_table_ids"]
    }
    clone["target_metric_ids"] = {
        key: None for key in clone["source_metric_ids"]
    }
    production["next_gate"] = (
        "verify_first_natural_reconciliation_then_prepare_route_disabled_deployment"
    )
    return result
