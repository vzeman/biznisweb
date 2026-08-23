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
    production["next_gate"] = (
        "verify_first_natural_reconciliation_then_prepare_route_disabled_deployment"
    )
    return result
