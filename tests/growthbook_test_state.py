from __future__ import annotations

import copy
from typing import Any, Mapping


def pending_natural_evidence_workspace(
    workspace: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the historical pre-natural-evidence state used by recorder tests."""

    result = copy.deepcopy(dict(workspace))
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
