#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import pathlib
import re
import sys
from typing import Any

try:
    from record_growthbook_natural_evidence import (
        EvidenceRecordingError,
        canonical_evidence_bytes,
        validate_natural_evidence,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.record_growthbook_natural_evidence import (
        EvidenceRecordingError,
        canonical_evidence_bytes,
        validate_natural_evidence,
    )

try:
    from record_growthbook_foundation_evidence import (
        FoundationEvidenceRecordingError,
        validate_foundation_evidence,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.record_growthbook_foundation_evidence import (
        FoundationEvidenceRecordingError,
        validate_foundation_evidence,
    )

try:
    from record_growthbook_production_reader_evidence import (
        ReaderEvidenceRecordingError,
        validate_reader_evidence,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.record_growthbook_production_reader_evidence import (
        ReaderEvidenceRecordingError,
        validate_reader_evidence,
    )

try:
    from record_growthbook_production_clone_evidence import (
        CloneEvidenceRecordingError,
        validate_clone_observation,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.record_growthbook_production_clone_evidence import (
        CloneEvidenceRecordingError,
        validate_clone_observation,
    )

try:
    from freeze_growthbook_cta_sample import (
        CtaSampleFreezeError,
        validate_plan as validate_cta_sample_plan,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.freeze_growthbook_cta_sample import (
        CtaSampleFreezeError,
        validate_plan as validate_cta_sample_plan,
    )

try:
    from validate_growthbook_cta_design import (
        CtaDesignContractError,
        validate_contract as validate_cta_design_contract,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.validate_growthbook_cta_design import (
        CtaDesignContractError,
        validate_contract as validate_cta_design_contract,
    )

try:
    from evaluate_growthbook_cta import (
        CtaEvaluationError,
        canonical_json_bytes as canonical_cta_lifecycle_bytes,
        validate_contract as validate_cta_decision_contract,
        validate_lifecycle_manifest as validate_cta_lifecycle_manifest,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.evaluate_growthbook_cta import (
        CtaEvaluationError,
        canonical_json_bytes as canonical_cta_lifecycle_bytes,
        validate_contract as validate_cta_decision_contract,
        validate_lifecycle_manifest as validate_cta_lifecycle_manifest,
    )

try:
    from record_growthbook_aa_completion import (
        AaCompletionRecordingError,
        canonical_json_bytes as canonical_aa_completion_bytes,
        validate_manifest as validate_aa_completion_manifest,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.record_growthbook_aa_completion import (
        AaCompletionRecordingError,
        canonical_json_bytes as canonical_aa_completion_bytes,
        validate_manifest as validate_aa_completion_manifest,
    )

try:
    from build_growthbook_cta_baseline_observation import (
        CtaBaselineError,
        validate_manifest as validate_cta_baseline_manifest,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.build_growthbook_cta_baseline_observation import (
        CtaBaselineError,
        validate_manifest as validate_cta_baseline_manifest,
    )

try:
    from record_growthbook_cta_activation import (
        CtaActivationRecordingError,
        RUNNING as CTA_RUNNING_STATUS,
        STOPPED as CTA_STOPPED_STATUS,
        canonical_json_bytes as canonical_cta_activation_bytes,
        validate_runtime_observation as validate_cta_runtime_observation,
        validate_running_handoff as validate_cta_running_handoff,
        validate_manifest as validate_cta_activation_manifest,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.record_growthbook_cta_activation import (
        CtaActivationRecordingError,
        RUNNING as CTA_RUNNING_STATUS,
        STOPPED as CTA_STOPPED_STATUS,
        canonical_json_bytes as canonical_cta_activation_bytes,
        validate_runtime_observation as validate_cta_runtime_observation,
        validate_running_handoff as validate_cta_running_handoff,
        validate_manifest as validate_cta_activation_manifest,
    )

try:
    from validate_growthbook_cta_measurement_window import (
        CtaMeasurementWindowError,
        validate_manifest as validate_cta_measurement_manifest,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.validate_growthbook_cta_measurement_window import (
        CtaMeasurementWindowError,
        validate_manifest as validate_cta_measurement_manifest,
    )

try:
    from record_growthbook_cta_completion import (
        CtaCompletionRecordingError,
        FOLLOWUP as CTA_FOLLOWUP_STATUS,
        canonical_json_bytes as canonical_cta_completion_bytes,
        validate_manifest as validate_cta_completion_manifest,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.record_growthbook_cta_completion import (
        CtaCompletionRecordingError,
        FOLLOWUP as CTA_FOLLOWUP_STATUS,
        canonical_json_bytes as canonical_cta_completion_bytes,
        validate_manifest as validate_cta_completion_manifest,
    )

try:
    from build_growthbook_cta_final_snapshot import (
        CtaFinalSnapshotError,
        FOLLOWUP as CTA_FINAL_FOLLOWUP_STATUS,
        RECORDED as CTA_FINAL_RECORDED_STATUS,
        WAITING as CTA_FINAL_WAITING_STATUS,
        validate_manifest as validate_cta_final_snapshot_manifest,
    )
except ModuleNotFoundError:  # Imported as scripts.validate_growthbook_workspace.
    from scripts.build_growthbook_cta_final_snapshot import (
        CtaFinalSnapshotError,
        FOLLOWUP as CTA_FINAL_FOLLOWUP_STATUS,
        RECORDED as CTA_FINAL_RECORDED_STATUS,
        WAITING as CTA_FINAL_WAITING_STATUS,
        validate_manifest as validate_cta_final_snapshot_manifest,
    )


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"
REPORTING_PATH = ROOT / "projects" / "vevo" / "growthbook_reporting.json"
AA_ACCEPTANCE_PATH = ROOT / "projects" / "vevo" / "growthbook_aa_acceptance.json"
CTA_SAMPLE_PLAN_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_sample_plan.json"
CTA_BASELINE_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_baseline.json"
CTA_ACTIVATION_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_activation.json"
CTA_MEASUREMENT_WINDOW_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_measurement_window.json"
)
CTA_COMPLETION_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_completion.json"
CTA_FINAL_SNAPSHOT_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_final_snapshot.json"
)
CTA_STOP_OBSERVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_assignment_stop_observation.json"
)
CTA_ACTIVATION_OBSERVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_activation_observation.json"
)
CTA_RUNTIME_READINESS_OBSERVATION_PATH = (
    ROOT
    / "projects"
    / "vevo"
    / "growthbook_cta_runtime_readiness_observation.json"
)
CTA_DESIGN_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_design.json"
CTA_STOREFRONT_PATH = ROOT / "storefront" / "vevo-growthbook" / "vevo-growthbook.js"
CTA_DECISION_CONTRACT_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_decision_contract.json"
)
CTA_LIFECYCLE_RECONCILIATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_lifecycle_reconciliation.json"
)
CTA_LIFECYCLE_OBSERVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_lifecycle_observation.json"
)
REGISTRY_PATH = ROOT / "growthbook_collector" / "experiments.json"
ACTIVATION_PATH = ROOT / "projects" / "vevo" / "growthbook_production_aa_activation.json"
AA_SNAPSHOT_PATH = ROOT / "projects" / "vevo" / "growthbook_aa_snapshot.json"
AA_COMPLETION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_production_aa_completion.json"
)
AA_COMPLETION_OBSERVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_aa_completion_observation.json"
)

EXPECTED_AA_ACCEPTANCE = {
    "schema_version": 2,
    "experiment_id": "vevo-sk-aa-001",
    "timezone": "Europe/Bratislava",
    "variations": ["control", "variant"],
    "expected_variation_weights": {"control": 0.5, "variant": 0.5},
    "required_production_allocation_percent": 100,
    "minimum_full_calendar_days": 7,
    "minimum_eligible_devices": 1000,
    "minimum_measured_page_loads_per_arm": 200,
    "minimum_exact_joined_transactions": 1,
    "minimum_meta_exposures": 1,
    "minimum_complete_stable_meta_dimension_exposures": 1,
    "privacy_full_window_required": True,
    "srm_p_value_min": 0.001,
    "split_percent_min": 48,
    "split_percent_max": 52,
    "pipeline_count_difference_max_percent": 2,
    "growthbook_reporting_count_difference_max_percent": 2,
    "duplicate_event_rate_max_percent": 0.5,
    "exact_order_join_rate_min_percent": 98,
    "lcp_degradation_absolute_ms": 200,
    "lcp_degradation_relative_percent": 10,
    "inp_degradation_absolute_ms": 20,
    "inp_degradation_relative_percent": 10,
    "cls_degradation_absolute_milli": 20,
    "client_error_rate_increase_max_percentage_points": 0.5,
}

EXPECTED_FACT_TABLES = {
    "vevo_device_outcomes_v1": "projects/vevo/growthbook_sql/device_outcomes.sql",
    "vevo_performance_vitals_v1": "projects/vevo/growthbook_sql/performance_vitals.sql",
}
EXPECTED_PRODUCTION_FACT_TABLES = {
    "vevo_device_outcomes_v1": "projects/vevo/growthbook_sql/device_outcomes_production.sql",
    "vevo_performance_vitals_v1": "projects/vevo/growthbook_sql/performance_vitals_production.sql",
}
EXPECTED_METRICS = {
    "vevo_add_to_cart_24h",
    "vevo_purchase_conversion_7d",
    "vevo_revenue_per_exposed_device_7d",
    "vevo_cm1_per_exposed_device_7d",
    "vevo_average_order_value_7d",
    "vevo_cancelled_order_rate_14d",
    "vevo_refunded_order_rate_14d",
    "vevo_client_error_device_rate_24h",
    "vevo_lcp_p75_24h",
    "vevo_inp_p75_24h",
    "vevo_cls_p75_milli_24h",
}
EXPECTED_CREATED_METRICS = {
    "vevo_add_to_cart_24h": "fact__2CeFBdeQA6SLEMaRKd563q",
    "vevo_purchase_conversion_7d": "fact__2CeFBgoFnykxPTRotYTDnB",
    "vevo_revenue_per_exposed_device_7d": "fact__2CeFBwmv6pJ39DJbDfwwRQ",
    "vevo_cm1_per_exposed_device_7d": "fact__2CeFBxixXV2373bgTrsXzS",
    "vevo_average_order_value_7d": "fact__2CeFC1mPhy2axfQ61NCz45",
    "vevo_cancelled_order_rate_14d": "fact__2CeFC3CdXkdH8zEzFu2CjP",
    "vevo_refunded_order_rate_14d": "fact__2CeFCDRWDEiKt6p8C8mixz",
    "vevo_client_error_device_rate_24h": "fact__2CeFCH8i2mUPmYAj1pmLZ2",
}
EXPECTED_PRO_BLOCKED_METRICS = {
    "vevo_lcp_p75_24h",
    "vevo_inp_p75_24h",
    "vevo_cls_p75_milli_24h",
}
EXPECTED_OUTCOME_NUMERIC_COLUMNS = [
    "add_to_cart_24h",
    "purchase_converted",
    "joined_order_count",
    "net_revenue_eur",
    "cm1_eur",
    "cancelled_order_count",
    "refunded_order_count",
    "immature_order_count",
    "lifecycle_mature",
    "client_error_observed",
    "order_attribution_eligible",
    "unmatched_transaction_count",
    "ambiguous_transaction_count",
]
EXPECTED_SCHEMA_PROBE_EXPERIMENT_ID = "__growthbook_schema_only__"
EXPECTED_SCHEMA_PROBE_DEVICE_ID = "00000000-0000-4000-8000-000000000000"
EXPECTED_FEATURES = {
    "vevo-sk-aa-001": "vevo-sk-aa-assignment",
    "vevo-sk-product-cta-color-001": "vevo-sk-product-cta-color",
}
FORBIDDEN_SQL_IDENTIFIERS = {
    "email",
    "phone",
    "address",
    "customer_id",
    "order_num",
    "transaction_id",
    "fbclid",
    "_fbp",
    "_fbc",
}


def _load(path: pathlib.Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"{path.name} must contain a JSON object")
    return payload


def _read_repo_path(value: str) -> str:
    path = ROOT / value
    if not path.is_file() or ROOT not in path.resolve().parents:
        raise AssertionError(f"missing or unsafe repository path: {value}")
    return path.read_text(encoding="utf-8")


def _validate_sql(
    name: str,
    sql: str,
    *,
    table: str,
    experiment_filter: bool,
    schema_probe: bool = False,
) -> None:
    lowered = sql.lower()
    if not re.search(r"\bselect\b", lowered) or not re.search(r"\bfrom\b", lowered):
        raise AssertionError(f"{name} must be a SELECT query")
    if re.search(r"\bselect\s+\*", lowered):
        raise AssertionError(f"{name} must not select every column")
    if table not in lowered:
        raise AssertionError(f"{name} must query {table}")
    if "experiment_events_raw" in lowered or "experiment-events/raw" in lowered:
        raise AssertionError(f"{name} must never query raw events")
    for identifier in FORBIDDEN_SQL_IDENTIFIERS:
        if re.search(rf"(?<![a-z0-9_]){re.escape(identifier)}(?![a-z0-9_])", lowered):
            raise AssertionError(f"{name} contains forbidden identifier {identifier}")
    required = {
        "device_id",
        " as timestamp",
        "metric_contract_version = 'vevo_cm1_v1_2026-08-20'",
        "eligible = 1",
        "contaminated = 0" if table == "experiment_device_facts" else "eligible = 1",
        "{{startdateiso}}",
        "{{enddateiso}}",
    }
    missing = sorted(marker for marker in required if marker not in lowered)
    if missing:
        raise AssertionError(f"{name} is missing SQL contract markers: {missing}")
    has_experiment_filter = "experiment_id like '{{ experimentid }}'" in lowered
    if has_experiment_filter != experiment_filter:
        raise AssertionError(f"{name} experiment filter does not match its contract")
    if schema_probe:
        probe_marker_counts = {
            "union all": 1,
            EXPECTED_SCHEMA_PROBE_EXPERIMENT_ID: 1,
            EXPECTED_SCHEMA_PROBE_DEVICE_ID: (
                1 if table == "experiment_device_facts" else 2
            ),
            "cast(current_timestamp as timestamp)": 1,
            "from (values (1)) as schema_seed(x)": 1,
            "where '{{ experimentid }}' = '%'": 1,
        }
        for marker, expected_count in probe_marker_counts.items():
            if lowered.count(marker) != expected_count:
                raise AssertionError(f"{name} schema probe marker drift: {marker}")
        union_index = lowered.index("union all")
        guard_index = lowered.index("where '{{ experimentid }}' = '%'")
        if guard_index <= union_index:
            raise AssertionError(f"{name} schema probe is not isolated after UNION ALL")
        if "or '{{ experimentid }}' = '%'" in lowered:
            raise AssertionError(f"{name} schema probe may not widen the curated fact branch")
    elif "__growthbook_schema_only__" in lowered or "where '{{ experimentid }}' = '%'" in lowered:
        raise AssertionError(f"{name} must not contain a Production schema probe")


def validate() -> None:
    workspace = _load(WORKSPACE_PATH)
    reporting = _load(REPORTING_PATH)
    registry = _load(REGISTRY_PATH)
    activation = json.loads(ACTIVATION_PATH.read_text(encoding="utf-8"))
    aa_snapshot_manifest = json.loads(AA_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    aa_completion = json.loads(AA_COMPLETION_PATH.read_text(encoding="utf-8"))
    aa_completion_observation = None
    if (
        aa_completion.get("status")
        == "production_aa_stopped_verified_cta_activation_blocked"
    ):
        observation_bytes = AA_COMPLETION_OBSERVATION_PATH.read_bytes()
        aa_completion_observation = json.loads(observation_bytes.decode("utf-8"))
        if observation_bytes != canonical_aa_completion_bytes(
            aa_completion_observation
        ):
            raise AssertionError("A/A completion observation is not canonical JSON")
    elif AA_COMPLETION_OBSERVATION_PATH.exists():
        raise AssertionError("A/A completion observation exists before verified stop")
    try:
        validate_aa_completion_manifest(
            aa_completion,
            activation,
            aa_snapshot_manifest,
            observation=aa_completion_observation,
        )
    except AaCompletionRecordingError as exc:
        raise AssertionError(f"A/A completion contract is invalid: {exc}") from exc
    aa_acceptance = json.loads(AA_ACCEPTANCE_PATH.read_text(encoding="utf-8"))
    cta_sample_plan = json.loads(CTA_SAMPLE_PLAN_PATH.read_text(encoding="utf-8"))
    cta_baseline = json.loads(CTA_BASELINE_PATH.read_text(encoding="utf-8"))
    cta_activation = json.loads(CTA_ACTIVATION_PATH.read_text(encoding="utf-8"))
    cta_measurement_window = json.loads(
        CTA_MEASUREMENT_WINDOW_PATH.read_text(encoding="utf-8")
    )
    cta_completion = json.loads(CTA_COMPLETION_PATH.read_text(encoding="utf-8"))
    cta_final_snapshot = json.loads(
        CTA_FINAL_SNAPSHOT_PATH.read_text(encoding="utf-8")
    )
    cta_activation_observation = None
    if cta_activation.get("status") in {CTA_RUNNING_STATUS, CTA_STOPPED_STATUS}:
        observation_bytes = CTA_ACTIVATION_OBSERVATION_PATH.read_bytes()
        cta_activation_observation = json.loads(observation_bytes.decode("utf-8"))
        if observation_bytes != canonical_cta_activation_bytes(
            cta_activation_observation
        ):
            raise AssertionError("CTA activation observation is not canonical JSON")
    elif CTA_ACTIVATION_OBSERVATION_PATH.exists():
        raise AssertionError("CTA activation observation exists before verified start")
    cta_stop_observation = None
    if cta_completion.get("status") == CTA_FOLLOWUP_STATUS:
        stop_bytes = CTA_STOP_OBSERVATION_PATH.read_bytes()
        cta_stop_observation = json.loads(stop_bytes.decode("utf-8"))
        if stop_bytes != canonical_cta_completion_bytes(cta_stop_observation):
            raise AssertionError("CTA stop observation is not canonical JSON")
    elif CTA_STOP_OBSERVATION_PATH.exists():
        raise AssertionError("CTA stop observation exists before verified stop")
    cta_design = json.loads(CTA_DESIGN_PATH.read_text(encoding="utf-8"))
    cta_decision_contract = json.loads(
        CTA_DECISION_CONTRACT_PATH.read_text(encoding="utf-8")
    )
    cta_lifecycle_reconciliation = json.loads(
        CTA_LIFECYCLE_RECONCILIATION_PATH.read_text(encoding="utf-8")
    )
    cta_lifecycle_observation = None
    if cta_lifecycle_reconciliation.get("verified") is True:
        observation_bytes = CTA_LIFECYCLE_OBSERVATION_PATH.read_bytes()
        cta_lifecycle_observation = json.loads(observation_bytes.decode("utf-8"))
        if observation_bytes != canonical_cta_lifecycle_bytes(
            cta_lifecycle_observation
        ):
            raise AssertionError("CTA lifecycle observation is not canonical JSON")
    elif CTA_LIFECYCLE_OBSERVATION_PATH.exists():
        raise AssertionError(
            "pending CTA lifecycle manifest must not have an observation file"
        )
    if aa_acceptance != EXPECTED_AA_ACCEPTANCE:
        raise AssertionError("A/A acceptance thresholds changed")
    try:
        validate_cta_sample_plan(cta_sample_plan)
    except CtaSampleFreezeError as exc:
        raise AssertionError(f"CTA sample plan is invalid: {exc}") from exc
    try:
        validate_cta_baseline_manifest(cta_baseline)
    except CtaBaselineError as exc:
        raise AssertionError(f"CTA baseline contract is invalid: {exc}") from exc
    try:
        validate_cta_activation_manifest(cta_activation)
    except CtaActivationRecordingError as exc:
        raise AssertionError(f"CTA activation contract is invalid: {exc}") from exc
    cta_bindings = cta_activation["source_bindings"]
    for binding_name, binding_path in (
        ("design_contract", CTA_DESIGN_PATH),
        ("decision_contract", CTA_DECISION_CONTRACT_PATH),
    ):
        if hashlib.sha256(binding_path.read_bytes()).hexdigest() != cta_bindings[
            binding_name
        ]["sha256"]:
            raise AssertionError(f"CTA activation {binding_name} file hash drift")
    if cta_activation["status"] != "waiting_for_verified_aa_completion_sample_lifecycle_and_runtime":
        for binding_name, binding_path in (
            ("aa_completion", AA_COMPLETION_PATH),
            ("aa_snapshot", AA_SNAPSHOT_PATH),
            ("sample_plan", CTA_SAMPLE_PLAN_PATH),
            ("lifecycle_reconciliation", CTA_LIFECYCLE_RECONCILIATION_PATH),
            ("collector_registry", REGISTRY_PATH),
        ):
            if hashlib.sha256(binding_path.read_bytes()).hexdigest() != cta_bindings[
                binding_name
            ]["sha256"]:
                raise AssertionError(
                    f"CTA activation bound source changed: {binding_name}"
                )
        runtime_bytes = CTA_RUNTIME_READINESS_OBSERVATION_PATH.read_bytes()
        runtime_observation = json.loads(runtime_bytes.decode("utf-8"))
        if runtime_bytes != canonical_cta_activation_bytes(runtime_observation):
            raise AssertionError("CTA runtime readiness observation is not canonical JSON")
        runtime_binding = cta_bindings["runtime_readiness"]
        if hashlib.sha256(runtime_bytes).hexdigest() != runtime_binding[
            "observation_sha256"
        ]:
            raise AssertionError("CTA runtime readiness observation SHA-256 drift")
        try:
            validate_cta_runtime_observation(runtime_observation, cta_activation)
        except CtaActivationRecordingError as exc:
            raise AssertionError(
                f"CTA runtime readiness observation is invalid: {exc}"
            ) from exc
    elif CTA_RUNTIME_READINESS_OBSERVATION_PATH.exists():
        raise AssertionError("CTA runtime readiness observation exists before review")
    try:
        validate_cta_design_contract(
            cta_design,
            CTA_STOREFRONT_PATH.read_text(encoding="utf-8"),
        )
    except CtaDesignContractError as exc:
        raise AssertionError(f"CTA design contract is invalid: {exc}") from exc
    try:
        validate_cta_decision_contract(cta_decision_contract)
        validate_cta_lifecycle_manifest(
            cta_lifecycle_reconciliation,
            cta_lifecycle_observation,
        )
    except CtaEvaluationError as exc:
        raise AssertionError(f"CTA decision contract is invalid: {exc}") from exc
    cta_reconciliation_evidence_path = (
        ROOT
        / "projects"
        / "vevo"
        / "growthbook_production_reconciliation_deploy_evidence.json"
    )
    cta_reconciliation_evidence = json.loads(
        cta_reconciliation_evidence_path.read_text(encoding="utf-8")
    )
    try:
        validate_cta_measurement_manifest(
            cta_measurement_window,
            cta_activation,
            cta_sample_plan,
            cta_decision_contract,
            cta_reconciliation_evidence,
            cta_activation_observation,
            cta_stop_observation,
        )
    except CtaMeasurementWindowError as exc:
        raise AssertionError(f"CTA measurement window is invalid: {exc}") from exc
    completion_source_hashes = {
        "activation": hashlib.sha256(CTA_ACTIVATION_PATH.read_bytes()).hexdigest(),
        "measurement_window": hashlib.sha256(
            CTA_MEASUREMENT_WINDOW_PATH.read_bytes()
        ).hexdigest(),
        "sample_plan": hashlib.sha256(CTA_SAMPLE_PLAN_PATH.read_bytes()).hexdigest(),
        "decision_contract": hashlib.sha256(
            CTA_DECISION_CONTRACT_PATH.read_bytes()
        ).hexdigest(),
        "lifecycle_reconciliation": hashlib.sha256(
            CTA_LIFECYCLE_RECONCILIATION_PATH.read_bytes()
        ).hexdigest(),
        "start_observation": (
            hashlib.sha256(CTA_ACTIVATION_OBSERVATION_PATH.read_bytes()).hexdigest()
            if CTA_ACTIVATION_OBSERVATION_PATH.exists()
            else ""
        ),
        "reconciliation_evidence": hashlib.sha256(
            cta_reconciliation_evidence_path.read_bytes()
        ).hexdigest(),
    }
    try:
        validate_cta_completion_manifest(
            cta_completion,
            cta_activation,
            cta_measurement_window,
            cta_sample_plan,
            cta_decision_contract,
            cta_lifecycle_reconciliation,
            cta_reconciliation_evidence,
            lifecycle_observation=cta_lifecycle_observation,
            start_observation=cta_activation_observation,
            stop_observation=cta_stop_observation,
            workspace=(
                workspace
                if cta_completion.get("status") == CTA_FOLLOWUP_STATUS
                else None
            ),
            source_hashes=completion_source_hashes,
        )
    except CtaCompletionRecordingError as exc:
        raise AssertionError(f"CTA completion contract is invalid: {exc}") from exc
    try:
        validate_cta_final_snapshot_manifest(cta_final_snapshot)
    except CtaFinalSnapshotError as exc:
        raise AssertionError(f"CTA final snapshot contract is invalid: {exc}") from exc
    final_snapshot_status = cta_final_snapshot.get("status")
    completion_is_followup = cta_completion.get("status") == CTA_FOLLOWUP_STATUS
    if completion_is_followup:
        if final_snapshot_status not in {
            CTA_FINAL_FOLLOWUP_STATUS,
            CTA_FINAL_RECORDED_STATUS,
        }:
            raise AssertionError(
                "CTA stopped completion must open or record the protected final snapshot"
            )
    elif final_snapshot_status != CTA_FINAL_WAITING_STATUS:
        raise AssertionError("CTA final snapshot opened before verified stop")
    if (
        cta_decision_contract["decision_timing"]["minimum_full_calendar_days"]
        != cta_sample_plan["minimum_full_calendar_days"]
        or cta_decision_contract["decision_timing"]["maximum_full_calendar_days"]
        != cta_sample_plan["maximum_full_calendar_days"]
        or cta_decision_contract["expected_variation_weights"]
        != cta_sample_plan["expected_variation_weights"]
    ):
        raise AssertionError("CTA decision and sample contracts differ")

    workspace_state = workspace.get("state")
    production_aa_running = workspace_state == (
        "production_aa_running_activation_verified_pro_quantiles_blocked"
    )
    production_aa_completed = workspace_state == (
        "production_aa_completed_cta_sample_freeze_pending_pro_quantiles_blocked"
    )
    production_cta_running = workspace_state == (
        "production_cta_running_activation_verified_pro_quantiles_blocked"
    )
    production_cta_stopped = workspace_state == (
        "production_cta_stopped_followup_pending_pro_quantiles_blocked"
    )
    if production_cta_stopped is not completion_is_followup:
        raise AssertionError("GrowthBook workspace/CTA completion lifecycle drift")
    production_post_aa = (
        production_aa_completed or production_cta_running or production_cta_stopped
    )
    production_aa_lifecycle = production_aa_running or production_post_aa
    if workspace.get("schema_version") != 1 or workspace_state not in {
        "preview_aa_runtime_and_reconciliation_verified_"
        "recurring_schedule_pending_pro_quantiles_blocked",
        "production_aa_running_activation_verified_pro_quantiles_blocked",
        "production_aa_completed_cta_sample_freeze_pending_pro_quantiles_blocked",
        "production_cta_running_activation_verified_pro_quantiles_blocked",
        "production_cta_stopped_followup_pending_pro_quantiles_blocked",
    }:
        raise AssertionError("GrowthBook workspace lifecycle state drift")
    completion_finished = (
        aa_completion.get("status")
        == "production_aa_stopped_verified_cta_activation_blocked"
    )
    if production_post_aa is not completion_finished:
        raise AssertionError("GrowthBook workspace/A/A completion lifecycle drift")
    workspace_config = workspace.get("workspace", {})
    if workspace_config.get("organization_name") != "Vevo":
        raise AssertionError("GrowthBook organization read-back changed")
    if workspace_config.get("project_name") != "VEVO SK Web":
        raise AssertionError("GrowthBook project read-back changed")
    if workspace_config.get("environments") != ["staging", "production"]:
        raise AssertionError("GrowthBook environment read-back changed")
    if workspace_config.get("environment_aliases") != {"preview": "staging"}:
        raise AssertionError("GrowthBook Preview must remain mapped to the Starter staging environment")
    if workspace_config.get("plan_type") != "starter":
        raise AssertionError("GrowthBook plan must match the authenticated workspace read-back")
    if workspace_config.get("subscription_or_trial_status") != "starter_active_no_paid_upgrade_accepted":
        raise AssertionError("GrowthBook paid-upgrade status is not safely recorded")
    expected_workspace_allocation = (
        100 if production_aa_running or production_cta_running else 0
    )
    if (
        workspace_config.get("production_allocation_percent")
        != expected_workspace_allocation
    ):
        raise AssertionError("GrowthBook Production A/A allocation state drift")
    if (
        workspace_config.get("actual_region") is not None
        or workspace_config.get("region_status") != "not_exposed_in_current_workspace_ui"
    ):
        raise AssertionError("GrowthBook region must remain explicitly unknown until exposed by the UI")

    sdk_connection = workspace.get("sdk_connection", {})
    expected_sdk = {
        "name": "VEVO SK Web Preview",
        "status": "runtime_verified_unpublished",
        "environment": "staging",
        "project": "VEVO SK Web",
        "language": "javascript",
        "sdk_version": "1.7.0",
        "api_host": "https://cdn.growthbook.io",
        "client_key_status": "configured_in_unpublished_gtm_not_committed",
        "include_draft_experiment_rules": True,
        "include_feature_rule_ids": True,
        "hide_feature_names": True,
        "visual_editor_enabled": False,
        "url_redirects_enabled": False,
        "production_connection_created": False,
    }
    if sdk_connection != expected_sdk:
        raise AssertionError("GrowthBook Preview SDK connection read-back changed")

    gtm_preview = workspace.get("gtm_preview_workspace", {})
    expected_gtm_identity = {
        "account_id": "6254499282",
        "container_id": "198135331",
        "public_container_id": "GTM-5ZB5LFGB",
        "workspace_id": "16",
        "workspace_name": "VEVO GrowthBook Preview",
        "status": "unpublished_draft_preview_runtime_accepted",
        "created_verified_date": "2026-08-21",
        "artifact_sha256": "f6b4972641efb7cc99d05b64b2c365c45eec20a6e5600ce9dade1dcaec694de1",
        "runtime_secret_material_committed": False,
        "temporary_artifact_status": (
            "not_persisted_browser_repl_only_clipboard_cleared_after_exact_sha_readback"
        ),
        "publish_status": "not_published",
        "workspace_changes": {"added": 5, "modified": 0, "deleted": 0},
    }
    for key, value in expected_gtm_identity.items():
        if gtm_preview.get(key) != value:
            raise AssertionError(f"GTM Preview workspace safety state drift: {key}")
    expected_tag_ids = {
        "loader": "44",
        "consent_bridge": "46",
        "add_to_cart_bridge": "47",
        "purchase_bridge": "48",
    }
    gtm_tags = gtm_preview.get("tags", {})
    if {key: row.get("id") for key, row in gtm_tags.items()} != expected_tag_ids:
        raise AssertionError("GTM Preview tag identity set changed")
    for key in ("consent_bridge", "add_to_cart_bridge", "purchase_bridge"):
        if gtm_tags.get(key, {}).get("loader_sequence_fail_closed_verified") is not True:
            raise AssertionError(f"GTM Preview bridge is not fail closed: {key}")
    if gtm_preview.get("triggers", {}).get("add_to_cart") != {
        "id": "45",
        "name": "CE - add_to_cart - VEVO GrowthBook Preview",
        "custom_event": "add_to_cart",
    }:
        raise AssertionError("GTM Preview add-to-cart trigger state drift")
    tag_assistant = gtm_preview.get("tag_assistant_preview", {})
    expected_tag_assistant = {
        "target_url": "https://www.vevo.sk/",
        "browser": "Comet",
        "extension_installed": True,
        "extension_version": "26.216.2.45",
        "extension_site_access": "all_sites_user_confirmed",
        "vevo_adblock_exceptions_user_confirmed": [
            "https://vevo.flox.sk",
            "https://www.vevo.sk",
        ],
        "growthbook_adblock_exception_user_confirmed": "https://cdn.growthbook.io",
        "comet_global_adblock_temporary_pause_verified": True,
        "comet_restart_completed": True,
        "comet_adblock_reenable_requested": True,
        "gtm_script_present_in_dom": True,
        "connection_result": "connected_three_google_tags_found",
        "detected_google_tag_count": 3,
        "draft_container_evaluated": True,
        "loader_fired": True,
        "consent_bridge_fired": True,
        "tag_assistant_console_error_count": 0,
        "no_analytics_consent_growthbook_dom_marker_count": 0,
        "no_analytics_consent_sdk_dom_count": 0,
        "no_analytics_consent_growthbook_or_collector_asset_count": 0,
        "analytics_only_sdk_dom_count": 1,
        "analytics_only_feature_request_observed": True,
        "analytics_only_feature_request_result": (
            "accepted_after_preview_no_cache_and_temporary_global_adblock_pause"
        ),
        "analytics_only_exposure_delivery_result": "accepted",
        "collector_request_observed": True,
        "collector_delivery_result": "accepted",
        "preview_state_marker": {
            "status": "active",
            "reason": "assigned",
            "consent": "granted",
            "consent_value_type": "number",
            "analytic_value_type": "number",
            "consent_bitwise": "granted",
        },
        "aa_variation_observed": "control",
        "assignment_reload_variation_stable": True,
        "withdrawal_fail_closed_verified": True,
        "regrant_accepted_verified": True,
        "cta_style_applied": False,
        "historical_feature_payload_blocker_host": "cdn.growthbook.io",
        "control_plane_feature_payload_http_status": 200,
        "control_plane_aa_rule_count": 1,
        "control_plane_cta_rule_count": 0,
        "official_troubleshooting_reference": (
            "https://support.google.com/tagmanager/answer/10039345"
        ),
        "comet_adblock_reference": (
            "https://www.perplexity.ai/help-center/comet/en/articles/11734702-adblock"
        ),
        "blocker": "none_in_temporary_preview_conditions",
        "next_gate": (
            "verify_same_population_meta_dimensions_and_first_natural_reconciliation"
        ),
    }
    if tag_assistant != expected_tag_assistant:
        raise AssertionError("GTM Tag Assistant Preview blocker state drift")

    expected_reconciliation_checkpoint = {
        "status": "controlled_real_preview_partition_verified",
        "event_date": "2026-08-21",
        "workflow_run_id": "32453223068",
        "workflow_run_url": (
            "https://github.com/vzeman/biznisweb/actions/runs/32453223068"
        ),
        "main_commit": "521472cac27b779f6bd1b969cadd1e4dfd8870fd",
        "allow_existing_partition_events": True,
        "reporting_image_digest": (
            "sha256:194d97bc159e59678cf184cdad3c33c0f5b2ddf501fa31d1d3422c6a7b5d2f68"
        ),
        "host_gate": {
            "instance_id": "N/A:Fargate",
            "private_ip": "172.31.8.58",
            "service": "vevo-daily-report-email",
            "path": "/app",
            "task_id": "2f4894451b6b40b0a2e7210f8ec18a08",
        },
        "reconciliation_task": {
            "instance_id": "N/A:Fargate",
            "private_ip": "172.31.22.22",
            "service": "vevo-daily-report-email",
            "path": "/app",
            "task_id": "f39633f1ab4e4a6ab2c01eb650e67b32",
        },
        "sanitized_partition_counts": {
            "raw_events": 23,
            "device_facts": 5,
            "performance_facts": 11,
            "transaction_facts": 0,
            "order_facts": 0,
        },
        "published_counts_match_generated_counts": True,
        "synthetic_identity_verified_without_browser_device_ids": True,
        "athena_assignment_query_id": "ef981af5-3c3f-4d32-813a-a546be77b79b",
        "raw_curated_reporting_athena_identity_verified": True,
        "production_allocation_percent": 0,
        "recurring_schedule_status": (
            "enabled_one_shot_verified_natural_retention_recovery_pending"
        ),
        "recurring_schedule": {
            "stack_name": "vevo-growthbook-reconciliation-preview",
            "workflow_run_id": "32459100570",
            "workflow_run_url": (
                "https://github.com/vzeman/biznisweb/actions/runs/32459100570"
            ),
            "main_commit": "4e4443beea2a2da466d80f781199bd4684dfac0c",
            "reporting_image_digest": (
                "sha256:cabba3b0bd57f6be322f3a5ff62f0327c7cf8e7bb2b6b5e78686305339fdd041"
            ),
            "task_definition": "vevo-growthbook-reconcile-preview:4",
            "schedule_name": "vevo-growthbook-reconcile-preview",
            "schedule_state": "ENABLED",
            "schedule_expression": "cron(30 3 * * ? *)",
            "schedule_timezone": "Europe/Bratislava",
            "host_gate": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.25.184",
                "service": "vevo-growthbook-reconcile-preview",
                "path": "/app",
                "task_id": "29d5e5d3fed349d79dec1384f5aff32a",
            },
            "one_shot_task": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.18.86",
                "service": "vevo-growthbook-reconcile-preview",
                "path": "/app",
                "task_id": "668418a08c504e078288f407df44a15e",
            },
            "one_shot_sanitized_counts": {
                "raw_events": 0,
                "device_facts": 0,
                "performance_facts": 0,
                "quality_reports": 2,
            },
            "generated_published_counts_match": True,
            "encrypted_retained_dlq_verified": True,
            "three_alarms_verified": True,
            "source_reporting_schedule_unchanged": True,
            "first_natural_run_local": "2026-08-22 03:30 Europe/Bratislava",
            "first_natural_run_status": (
                "success_marker_observed_ecs_state_expired_recovery_pending"
            ),
            "natural_verifier_workflow": (
                ".github/workflows/verify-vevo-growthbook-natural-reconciliation.yml"
            ),
            "natural_verifier_status": (
                "prepared_second_natural_run_retention_recovery"
            ),
            "natural_evidence_schema_version": 2,
            "natural_evidence_file": (
                "vevo-growthbook-natural-reconciliation-evidence.json"
            ),
            "natural_evidence_retention_days": 14,
            "natural_evidence_artifact_status": (
                "code_prepared_retention_recovery_pending"
            ),
            "natural_evidence_contains_raw_aws_payloads": False,
            "natural_evidence_contains_credentials": False,
            "natural_verifier_run_id": None,
            "natural_verifier_main_commit": None,
            "natural_evidence_artifact_sha256": None,
            "natural_verifier_evidence": None,
            "natural_verifier_not_before_utc": "2026-08-23T01:40:00Z",
            "natural_verifier_before_utc": "2026-08-23T02:20:00Z",
            "natural_verifier_mutation_allowed": False,
        },
    }
    actual_reconciliation_checkpoint = workspace.get("reconciliation_checkpoint") or {}
    actual_recurring_schedule = actual_reconciliation_checkpoint.get("recurring_schedule") or {}
    natural_evidence_verified = (
        actual_recurring_schedule.get("first_natural_run_status")
        == "verified_via_second_natural_run"
    )
    if natural_evidence_verified:
        run_id = actual_recurring_schedule.get("natural_verifier_run_id")
        main_commit = actual_recurring_schedule.get("natural_verifier_main_commit")
        evidence_sha256 = actual_recurring_schedule.get("natural_evidence_artifact_sha256")
        evidence = actual_recurring_schedule.get("natural_verifier_evidence")
        try:
            validate_natural_evidence(
                evidence,
                expected_workflow_run_id=run_id,
                expected_main_commit=main_commit,
            )
        except (EvidenceRecordingError, TypeError) as exc:
            raise AssertionError("GrowthBook natural evidence validation failed") from exc
        if (
            not isinstance(evidence_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", evidence_sha256) is None
            or hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest()
            != evidence_sha256
        ):
            raise AssertionError("GrowthBook natural evidence SHA-256 drift")
        expected_reconciliation_checkpoint["recurring_schedule_status"] = (
            "enabled_one_shot_and_natural_retention_recovery_verified"
        )
        expected_reconciliation_checkpoint["recurring_schedule"].update(
            {
                "first_natural_run_status": "verified_via_second_natural_run",
                "natural_verifier_status": "passed_retention_recovery_run",
                "natural_evidence_artifact_status": (
                    "verified_downloaded_sha256_recorded"
                ),
                "natural_verifier_run_id": run_id,
                "natural_verifier_main_commit": main_commit,
                "natural_evidence_artifact_sha256": evidence_sha256,
                "natural_verifier_evidence": evidence,
            }
        )
    if workspace.get("reconciliation_checkpoint") != expected_reconciliation_checkpoint:
        raise AssertionError("GrowthBook reconciliation checkpoint state drift")

    expected_population_audit = {
        "status": "audit_passed_reporting_parity_meta_delivery_contract_gap_proven",
        "workflow": ".github/workflows/audit-vevo-growthbook-meta-population.yml",
        "meta_audit_script": "scripts/audit_vevo_meta_dimensions.py",
        "population_sql": "projects/vevo/growthbook_sql/population_audit.sql",
        "variation_sql": "projects/vevo/growthbook_sql/population_variations.sql",
        "meta_window_complete_utc_days": 30,
        "meta_api_mode": "get_only",
        "output_mode": "aggregate_sanitized_only",
        "meta_delivery_mutation_allowed": False,
        "biznisweb_mutation_allowed": False,
        "production_allocation_percent": 0,
        "first_runtime_attempt": {
            "workflow_run_id": "32461687307",
            "workflow_run_url": (
                "https://github.com/vzeman/biznisweb/actions/runs/32461687307"
            ),
            "main_commit": "d4ed6e276855c71cec91e0827b2619af020ad524",
            "reporting_image_digest": (
                "sha256:8cbae67d93fd2181924abe31971a53c9ef3144ac5334c92e3da138f2623c699c"
            ),
            "candidate_task_definition": "vevo-growthbook-meta-audit-preview:1",
            "host_gate": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.43.218",
                "service": "vevo-growthbook-meta-audit-preview",
                "path": "/app",
                "task_id": "6912ec37bdce4aee9945739fa208298d",
            },
            "meta_step_status": "stopped_on_hidden_marker_exit_combination",
            "athena_step_started": False,
            "external_mutation_observed": False,
        },
        "second_runtime_attempt": {
            "workflow_run_id": "32462783153",
            "workflow_run_url": (
                "https://github.com/vzeman/biznisweb/actions/runs/32462783153"
            ),
            "main_commit": "8f9b09d9c590e16b910a28bfd82bc782f711a7f8",
            "reporting_image_digest": (
                "sha256:2c20f38b1206458529749313b3ee643307c34dc5b73d6e0763e561882ac5b4a2"
            ),
            "candidate_task_definition": "vevo-growthbook-meta-audit-preview:2",
            "host_gate": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.17.16",
                "service": "vevo-growthbook-meta-audit-preview",
                "path": "/app",
                "task_id": "0173cd46a9264bd78025bf12925cfb26",
            },
            "meta_task": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.9.105",
                "service": "vevo-growthbook-meta-audit-preview",
                "path": "/app",
                "task_id": "767537f2aaba4b8992d1790f930d5882",
                "exit_code": 1,
                "stop_code": "EssentialContainerExited",
            },
            "sanitized_log_counts": {
                "events": 4,
                "start_markers": 0,
                "success_markers": 0,
                "failure_markers": 0,
            },
            "root_cause": (
                "script_path_entrypoint_excluded_repo_root_from_python_import_path"
            ),
            "athena_step_started": False,
            "external_mutation_observed": False,
        },
        "successful_runtime": {
            "implementation_pr": 316,
            "build_workflow_run_id": "32463854583",
            "workflow_run_id": "32464046045",
            "workflow_run_url": (
                "https://github.com/vzeman/biznisweb/actions/runs/32464046045"
            ),
            "main_commit": "9ef741b328cb5709f1e3e7e78c2f4b7afeadc066",
            "reporting_image_digest": (
                "sha256:95efe5fffa2f4a3c7ded6c710697b0d5f6f6b45fbc525ad6a848a069753234ef"
            ),
            "candidate_task_definition": "vevo-growthbook-meta-audit-preview:3",
            "host_gate": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.16.54",
                "service": "vevo-growthbook-meta-audit-preview",
                "path": "/app",
                "task_id": "cd74ee3a3b5d43e999c45b15b0fdec1a",
            },
            "meta_task": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.19.87",
                "service": "vevo-growthbook-meta-audit-preview",
                "path": "/app",
                "task_id": "3b1e00828825416da1f5c1422e6d1cac",
                "exit_code": 0,
                "stop_code": "EssentialContainerExited",
            },
            "meta_summary": {
                "window_since": "2026-07-22",
                "window_until": "2026-08-20",
                "traffic_campaigns": 3,
                "traffic_adsets": 3,
                "traffic_ads": 19,
                "total_clicks": 2210,
                "total_spend_eur": 523.13,
                "complete_contract_ads": 0,
                "forbidden_click_identifier_parameter_ads": 0,
                "coverage_percent": {
                    "utm_source": 100.0,
                    "utm_medium": 100.0,
                    "utm_id": 100.0,
                    "utm_content": 0.0,
                    "meta_adset_id": 0.0,
                    "meta_placement": 0.0,
                },
            },
            "athena_summary": {
                "assignment_rows": 5,
                "outcome_rows": 5,
                "assignment_keys": 5,
                "outcome_keys": 5,
                "duplicate_assignment_keys": 0,
                "duplicate_outcome_keys": 0,
                "assignments_missing_outcomes": 0,
                "outcomes_missing_assignments": 0,
                "complete_meta_dimension_rows": 3,
                "invalid_meta_dimension_rows": 0,
                "population_query_id": "0acd8e02-d081-4cec-a85e-71034321f8de",
                "variations_query_id": "5ef71971-9ea2-4511-8d49-517a1c00318a",
            },
            "external_mutation_observed": False,
        },
        "next_gate": "verify_first_natural_reconciliation_before_measured_aa_publish",
    }
    if workspace.get("population_audit") != expected_population_audit:
        raise AssertionError("GrowthBook Meta/population audit state drift")

    expected_meta_parameter_rollout = {
        "status": "runbook_verified_existing_live_ads_unchanged",
        "runbook": "projects/vevo/META_ADS_GROWTHBOOK_PARAMETER_RUNBOOK.md",
        "owner_of_randomization": "growthbook",
        "meta_split_for_same_hypothesis_allowed": False,
        "analyzed_dimensions": [
            "utm_source",
            "utm_medium",
            "utm_id",
            "utm_content",
            "meta_adset_id",
            "meta_placement",
        ],
        "diagnostic_label": {"utm_campaign": "{{campaign.name}}"},
        "canonical_url_parameters": (
            "utm_source=meta&utm_medium=paid_social&utm_id={{campaign.id}}"
            "&utm_campaign={{campaign.name}}&utm_content={{ad.id}}"
            "&meta_adset_id={{adset.id}}&meta_placement={{placement}}"
        ),
        "existing_live_ads_policy": "do_not_edit_only_for_tracking_during_aa",
        "new_or_otherwise_edited_ads_policy": "apply_before_publish",
        "bulk_live_edit_allowed": False,
        "pii_or_configured_click_ids_allowed": False,
        "baseline_audit_run_id": "32464046045",
        "baseline_complete_contract_ads": 0,
        "current_meta_mutation_observed": False,
        "next_gate": "verify_first_natural_reconciliation_before_measured_aa_publish",
    }
    if workspace.get("meta_parameter_rollout") != expected_meta_parameter_rollout:
        raise AssertionError("GrowthBook Meta parameter rollout state drift")
    meta_runbook = _read_repo_path(expected_meta_parameter_rollout["runbook"])
    required_meta_runbook_markers = {
        expected_meta_parameter_rollout["canonical_url_parameters"],
        "Do not edit an existing live ad only to add this tracking during A/A.",
        "Do not enable Meta's A/B-test split for the same hypothesis.",
        "Existing live ads were not changed.",
        "Audit VEVO GrowthBook and Meta Population",
    }
    missing_meta_runbook_markers = sorted(
        marker for marker in required_meta_runbook_markers if marker not in meta_runbook
    )
    if missing_meta_runbook_markers:
        raise AssertionError(
            f"GrowthBook Meta parameter runbook drift: {missing_meta_runbook_markers}"
        )

    feature_flags = workspace.get("feature_flags", [])
    feature_map = {row.get("key"): row for row in feature_flags if isinstance(row, dict)}
    if set(feature_map) != set(EXPECTED_FEATURES.values()):
        raise AssertionError("GrowthBook feature-flag set changed")
    expected_feature_flags = {
        "vevo-sk-aa-assignment": {
            "key": "vevo-sk-aa-assignment",
            "type": "string",
            "default_value": "control",
            "project": "VEVO SK Web",
            "staging_enabled": True,
            "production_enabled": False,
            "live_rule_count": 1,
            "live_rule_experiment": "vevo-sk-aa-001",
            "live_rule_environments": ["staging"],
            "live_rule_revision": 2,
            "live_rule_published_date": "2026-08-21",
            "draft_rule_experiment": None,
        },
        "vevo-sk-product-cta-color": {
            "key": "vevo-sk-product-cta-color",
            "type": "string",
            "default_value": "control",
            "project": "VEVO SK Web",
            "staging_enabled": True,
            "production_enabled": False,
            "live_rule_count": 0,
            "draft_rule_experiment": "vevo-sk-product-cta-color-001",
        },
    }
    for feature_key, expected_feature in expected_feature_flags.items():
        if feature_map[feature_key] != expected_feature:
            raise AssertionError(f"GrowthBook feature-flag safety contract changed: {feature_key}")

    athena = workspace.get("athena", {})
    if athena.get("authentication") != "dedicated_readonly_iam_user_access_key":
        raise AssertionError("GrowthBook Cloud Athena must keep a dedicated read-only identity")
    expected_reader = {
        "credentials_status": "created_active_stored_in_growthbook_cloud",
        "iam_user_name": "vevo-growthbook-preview-reader",
        "iam_user_arn": (
            "arn:aws:iam::919341186960:user/vevo/growthbook/preview/"
            "vevo-growthbook-preview-reader"
        ),
        "policy_arn": "arn:aws:iam::919341186960:policy/vevo-growthbook-readonly-preview",
        "access_key_count": 1,
        "credential_material_committed": False,
        "local_credential_handoff_status": "deleted_after_successful_connection",
    }
    for key, value in expected_reader.items():
        if athena.get(key) != value:
            raise AssertionError(f"GrowthBook Preview reader state drift: {key}")
    preview_athena = athena.get("preview", {})
    expected_preview_connection = {
        "data_source_id": "ds_19g6mmt2c4dmn",
        "data_source_name": "VEVO Preview Experiment Facts",
        "identifier_type": "device_id",
        "assignment_query_name": "VEVO consented devices",
        "assignment_query_test_status": (
            "executed_one_exact_synthetic_row_via_growthbook_ui"
        ),
        "connection_verified_date": "2026-08-21",
        "assignment_verified_date": "2026-08-21",
        "verification_source_run_id": 32441597178,
        "verification_recovery_run_id": 32442114254,
        "verification_athena_query_id": "934c938d-bc55-42c7-b89c-247337e9e2b1",
        "verification_row_count": 1,
        "performance_verification_run_id": 32443149425,
        "performance_verification_athena_query_id": "c41e0f2c-690d-4fd5-8081-8868fe8c6876",
        "performance_verification_event_id": "071b28e4-6177-48ca-86d4-47936cd15a3c",
        "performance_verification_page_load_id": "a2240ea5-a6f0-416d-a1c2-515b609f8e2c",
        "status": "growthbook_datasource_and_synthetic_assignment_verified",
    }
    for key, value in expected_preview_connection.items():
        if preview_athena.get(key) != value:
            raise AssertionError(f"GrowthBook Preview connection state drift: {key}")
    if preview_athena.get("s3_results_url") != (
        "s3://vevo-growthbook-preview-experimentdatabucket-pj7zod15wpyr/"
        "athena-results/growthbook/"
    ):
        raise AssertionError("Preview Athena results location drift")
    expected_production_connection = {
        "data_source_name": "VEVO Production Experiment Facts",
        "workgroup": "vevo-growthbook-readonly-production",
        "database": "vevo_growthbook_production",
        "s3_results_url": "from_production_stack_output",
        "status": "read_only_preflight_passed_natural_run_gate_pending",
        "preflight_workflow": (
            ".github/workflows/preflight-vevo-growthbook-production-foundation.yml"
        ),
        "expected_stack": "vevo-growthbook-production",
        "expected_service": "vevo-growthbook-collector-production",
        "expected_runtime_path": "/app",
        "public_route_enabled": False,
        "experiment_registry_empty": True,
        "deployment_allowed": False,
        "foundation_deployment_workflow": (
            ".github/workflows/deploy-vevo-growthbook-production-foundation.yml"
        ),
        "foundation_deployment_status": "code_prepared_natural_run_gate_pending",
        "foundation_deployment_allowed": False,
        "foundation_evidence_schema_version": 1,
        "foundation_evidence_file": (
            "vevo-growthbook-production-foundation-evidence.json"
        ),
        "foundation_evidence_retention_days": 14,
        "foundation_evidence_artifact_status": "code_prepared_deployment_pending",
        "foundation_evidence_contains_raw_aws_payloads": False,
        "foundation_evidence_contains_credentials": False,
        "foundation_deployment_run_id": None,
        "foundation_deployment_main_commit": None,
        "foundation_evidence_artifact_sha256": None,
        "credentials_created": False,
        "reader_provisioning_workflow": (
            ".github/workflows/provision-vevo-growthbook-production-reader.yml"
        ),
        "reader_provisioning_status": "code_prepared_foundation_gate_pending",
        "reader_provisioning_allowed": False,
        "reader_evidence_schema_version": 1,
        "reader_evidence_file": "vevo-growthbook-production-reader-evidence.json",
        "reader_evidence_artifact_status": "code_prepared_provisioning_pending",
        "reader_evidence_contains_credentials": False,
        "reader_provisioning_run_id": None,
        "reader_provisioning_main_commit": None,
        "reader_evidence_artifact_sha256": None,
        "successful_reader_provisioning": None,
        "expected_iam_user_name": "vevo-growthbook-production-reader",
        "expected_iam_user_path": "/vevo/growthbook/production/",
        "successful_foundation_deployment": None,
        "growthbook_clone": {
            "status": "code_prepared_foundation_reader_gate_pending",
            "clone_allowed": False,
            "mutation_status": "not_started",
            "observation_schema_version": 2,
            "observation_file": (
                "vevo-growthbook-production-clone-observation.json"
            ),
            "observation_status": "not_recorded",
            "observation_sha256": None,
            "successful_clone_verification": None,
            "source_data_source_id": "ds_19g6mmt2c4dmn",
            "target_data_source_id": None,
            "assignment_query_name": "VEVO consented devices",
            "assignment_query_path": "projects/vevo/growthbook_sql/assignment.sql",
            "source_fact_table_ids": {
                "vevo_device_outcomes_v1": "ftb_19g6mmt2dhrdi",
                "vevo_performance_vitals_v1": "ftb_19g6mmt2e0otd",
            },
            "target_fact_table_query_paths": EXPECTED_PRODUCTION_FACT_TABLES,
            "target_fact_table_ids": {
                "vevo_device_outcomes_v1": None,
                "vevo_performance_vitals_v1": None,
            },
            "source_metric_ids": {
                "vevo_add_to_cart_24h": "fact__2CeFBdeQA6SLEMaRKd563q",
                "vevo_purchase_conversion_7d": "fact__2CeFBgoFnykxPTRotYTDnB",
                "vevo_revenue_per_exposed_device_7d": "fact__2CeFBwmv6pJ39DJbDfwwRQ",
                "vevo_cm1_per_exposed_device_7d": "fact__2CeFBxixXV2373bgTrsXzS",
                "vevo_average_order_value_7d": "fact__2CeFC1mPhy2axfQ61NCz45",
                "vevo_cancelled_order_rate_14d": "fact__2CeFC3CdXkdH8zEzFu2CjP",
                "vevo_refunded_order_rate_14d": "fact__2CeFCDRWDEiKt6p8C8mixz",
                "vevo_client_error_device_rate_24h": "fact__2CeFCH8i2mUPmYAj1pmLZ2",
            },
            "target_metric_ids": {
                "vevo_add_to_cart_24h": None,
                "vevo_purchase_conversion_7d": None,
                "vevo_revenue_per_exposed_device_7d": None,
                "vevo_cm1_per_exposed_device_7d": None,
                "vevo_average_order_value_7d": None,
                "vevo_cancelled_order_rate_14d": None,
                "vevo_refunded_order_rate_14d": None,
                "vevo_client_error_device_rate_24h": None,
            },
            "paid_pro_metric_keys": [
                "vevo_lcp_p75_24h",
                "vevo_inp_p75_24h",
                "vevo_cls_p75_milli_24h",
            ],
            "paid_pro_upgrade_authorized": False,
            "object_creation_order": [
                "production_data_source",
                "device_id_identifier",
                "assignment_query",
                "fact_tables",
                "starter_compatible_metrics",
                "paid_pro_quantile_metrics_after_authorized_upgrade",
            ],
            "preview_connection_repoint_allowed": False,
        },
        "first_preflight_attempt": {
            "workflow_run_id": "32465911390",
            "workflow_run_url": (
                "https://github.com/vzeman/biznisweb/actions/runs/32465911390"
            ),
            "main_commit": "19adc326d676212df6b410ac96eadeea47655c21",
            "failure_boundary": (
                "local_manifest_publish_status_literal_before_aws_credentials"
            ),
            "expected_literal": "unpublished_draft",
            "actual_literal": "not_published",
            "aws_credentials_step_started": False,
            "external_mutation_observed": False,
        },
        "second_preflight_attempt": {
            "workflow_run_id": "32466261505",
            "workflow_run_url": (
                "https://github.com/vzeman/biznisweb/actions/runs/32466261505"
            ),
            "main_commit": "edd6e24772819f9cd087d4e3fb7e01b2085e6c4b",
            "failure_boundary": (
                "local_manifest_activation_gate_path_before_aws_credentials"
            ),
            "expected_object": "release_gates",
            "actual_object": "decision_gates",
            "aws_credentials_step_started": False,
            "external_mutation_observed": False,
        },
        "successful_preflight": {
            "workflow_run_id": "32466708456",
            "workflow_run_url": (
                "https://github.com/vzeman/biznisweb/actions/runs/32466708456"
            ),
            "main_commit": "f50e5712b039b991e2bc986552af8d8a54a6d551",
            "aws_account_id": "919341186960",
            "aws_region": "eu-central-1",
            "production_stack_state": "absent",
            "preview_task_id": "4a2cdbe240794f439b68ad674f9bb2d6",
            "preview_private_ip": "172.31.34.243",
            "preview_service": "vevo-growthbook-collector-preview",
            "preview_runtime_path": "/app",
            "preview_image_digest": (
                "sha256:9478acd98a8caf06374b018c563ee51"
                "fa896b9cc92148238579f04aa28a134e1"
            ),
            "external_mutation_observed": False,
        },
        "next_gate": (
            "verify_first_natural_reconciliation_then_prepare_"
            "route_disabled_deployment"
        ),
    }
    if natural_evidence_verified:
        expected_production_connection.update(
            {
                "status": "natural_run_verified_foundation_deployment_ready",
                "deployment_allowed": True,
                "foundation_deployment_status": (
                    "natural_run_verified_ready_for_reviewed_dispatch"
                ),
                "foundation_deployment_allowed": True,
                "next_gate": "dispatch_route_disabled_production_foundation_after_review",
            }
        )
    actual_production_connection = athena.get("production") or {}
    foundation_evidence = actual_production_connection.get(
        "successful_foundation_deployment"
    )
    foundation_evidence_verified = isinstance(foundation_evidence, dict)
    if foundation_evidence_verified:
        if not natural_evidence_verified:
            raise AssertionError(
                "GrowthBook foundation evidence cannot precede natural evidence"
            )
        foundation_run_id = actual_production_connection.get(
            "foundation_deployment_run_id"
        )
        foundation_main_commit = actual_production_connection.get(
            "foundation_deployment_main_commit"
        )
        foundation_sha256 = actual_production_connection.get(
            "foundation_evidence_artifact_sha256"
        )
        try:
            validate_foundation_evidence(
                foundation_evidence,
                expected_workflow_run_id=foundation_run_id,
                expected_main_commit=foundation_main_commit,
                expected_natural_run_id=actual_recurring_schedule.get(
                    "natural_verifier_run_id"
                ),
                expected_natural_main_commit=actual_recurring_schedule.get(
                    "natural_verifier_main_commit"
                ),
                expected_natural_sha256=actual_recurring_schedule.get(
                    "natural_evidence_artifact_sha256"
                ),
            )
        except (FoundationEvidenceRecordingError, TypeError) as exc:
            raise AssertionError(
                "GrowthBook foundation evidence validation failed"
            ) from exc
        if (
            not isinstance(foundation_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", foundation_sha256) is None
            or hashlib.sha256(canonical_evidence_bytes(foundation_evidence)).hexdigest()
            != foundation_sha256
        ):
            raise AssertionError("GrowthBook foundation evidence SHA-256 drift")
        expected_production_connection.update(
            {
                "status": "route_disabled_foundation_deployed_verified",
                "deployment_allowed": False,
                "foundation_deployment_status": "deployed_route_disabled_verified",
                "foundation_deployment_allowed": False,
                "foundation_evidence_artifact_status": (
                    "verified_downloaded_sha256_recorded"
                ),
                "foundation_evidence_schema_version": foundation_evidence.get(
                    "schema_version"
                ),
                "foundation_deployment_run_id": foundation_run_id,
                "foundation_deployment_main_commit": foundation_main_commit,
                "foundation_evidence_artifact_sha256": foundation_sha256,
                "reader_provisioning_status": (
                    "foundation_verified_ready_for_reviewed_dispatch"
                ),
                "reader_provisioning_allowed": True,
                "successful_foundation_deployment": foundation_evidence,
                "next_gate": "dispatch_production_reader_after_review",
            }
        )
    reader_evidence = actual_production_connection.get(
        "successful_reader_provisioning"
    )
    reader_evidence_verified = isinstance(reader_evidence, dict)
    if reader_evidence_verified:
        if not foundation_evidence_verified:
            raise AssertionError(
                "GrowthBook reader evidence cannot precede foundation evidence"
            )
        reader_run_id = actual_production_connection.get(
            "reader_provisioning_run_id"
        )
        reader_main_commit = actual_production_connection.get(
            "reader_provisioning_main_commit"
        )
        reader_sha256 = actual_production_connection.get(
            "reader_evidence_artifact_sha256"
        )
        try:
            validate_reader_evidence(
                reader_evidence,
                expected_workflow_run_id=reader_run_id,
                expected_main_commit=reader_main_commit,
                expected_foundation_run_id=actual_production_connection.get(
                    "foundation_deployment_run_id"
                ),
                expected_foundation_main_commit=actual_production_connection.get(
                    "foundation_deployment_main_commit"
                ),
                expected_foundation_sha256=actual_production_connection.get(
                    "foundation_evidence_artifact_sha256"
                ),
            )
        except (ReaderEvidenceRecordingError, TypeError) as exc:
            raise AssertionError(
                "GrowthBook reader evidence validation failed"
            ) from exc
        if (
            not isinstance(reader_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", reader_sha256) is None
            or hashlib.sha256(canonical_evidence_bytes(reader_evidence)).hexdigest()
            != reader_sha256
        ):
            raise AssertionError("GrowthBook reader evidence SHA-256 drift")
        expected_clone = {
            **expected_production_connection["growthbook_clone"],
            "status": "reader_verified_ready_for_reviewed_growthbook_clone",
            "clone_allowed": True,
        }
        expected_production_connection.update(
            {
                "credentials_created": True,
                "reader_provisioning_status": (
                    "verified_active_encrypted_handoff_ready_for_growthbook"
                ),
                "reader_provisioning_allowed": False,
                "reader_evidence_artifact_status": (
                    "verified_downloaded_sha256_recorded"
                ),
                "reader_provisioning_run_id": reader_run_id,
                "reader_provisioning_main_commit": reader_main_commit,
                "reader_evidence_artifact_sha256": reader_sha256,
                "successful_reader_provisioning": reader_evidence,
                "growthbook_clone": expected_clone,
                "next_gate": (
                    "connect_production_reader_and_clone_growthbook_after_review"
                ),
            }
        )
    actual_clone = actual_production_connection.get("growthbook_clone") or {}
    clone_observation = actual_clone.get("successful_clone_verification")
    clone_observation_verified = isinstance(clone_observation, dict)
    if clone_observation_verified:
        if not reader_evidence_verified:
            raise AssertionError(
                "GrowthBook clone evidence cannot precede reader evidence"
            )
        clone_sha256 = actual_clone.get("observation_sha256")
        try:
            validate_clone_observation(clone_observation, workspace)
        except (CloneEvidenceRecordingError, TypeError) as exc:
            cm1_metric = next(
                (
                    row
                    for row in workspace.get("metrics", [])
                    if isinstance(row, dict)
                    and row.get("key") == "vevo_cm1_per_exposed_device_7d"
                ),
                {},
            )
            if cm1_metric.get("metric_contract_version") != reporting.get(
                "metric_contract_version"
            ):
                raise AssertionError(
                    "GrowthBook and reporting CM1 contracts differ"
                ) from exc
            raise AssertionError(
                "GrowthBook clone observation validation failed"
            ) from exc
        if (
            not isinstance(clone_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", clone_sha256) is None
            or hashlib.sha256(
                canonical_evidence_bytes(clone_observation)
            ).hexdigest()
            != clone_sha256
        ):
            raise AssertionError("GrowthBook clone observation SHA-256 drift")
        target_fact_tables = {
            key: row["id"]
            for key, row in clone_observation["production_fact_tables"].items()
        }
        target_metrics = {
            key: row["id"]
            for key, row in clone_observation["production_metrics"].items()
        }
        expected_clone = {
            **expected_production_connection["growthbook_clone"],
            "status": "verified_complete",
            "clone_allowed": False,
            "mutation_status": "created_and_query_verified",
            "observation_status": "verified_canonical_sha256_recorded",
            "observation_sha256": clone_sha256,
            "successful_clone_verification": clone_observation,
            "target_data_source_id": clone_observation[
                "production_data_source"
            ]["id"],
            "target_fact_table_ids": target_fact_tables,
            "target_metric_ids": target_metrics,
        }
        expected_production_connection.update(
            {
                "growthbook_clone": expected_clone,
                "next_gate": "prepare_production_aa_activation_after_review",
            }
        )
    if athena.get("production") != expected_production_connection:
        raise AssertionError("GrowthBook Production connection preflight state drift")
    assignment_path = athena.get("assignment_query")
    assignment_sql = _read_repo_path(assignment_path)
    _validate_sql(
        assignment_path,
        assignment_sql,
        table="experiment_device_facts",
        experiment_filter=False,
        schema_probe=False,
    )
    for dimension in ("meta_campaign_id", "meta_adset_id", "meta_ad_id", "meta_placement"):
        if dimension not in assignment_sql:
            raise AssertionError(f"assignment query is missing the approved dimension {dimension}")

    fact_tables = workspace.get("fact_tables", [])
    fact_map = {row.get("key"): row for row in fact_tables if isinstance(row, dict)}
    if set(fact_map) != set(EXPECTED_FACT_TABLES):
        raise AssertionError("GrowthBook fact-table set changed")
    for key, expected_path in EXPECTED_FACT_TABLES.items():
        row = fact_map[key]
        if row.get("query") != expected_path or row.get("identifier") != "device_id":
            raise AssertionError(f"GrowthBook fact-table contract changed for {key}")
        if row.get("outcome_windows_enforced_upstream") is not True:
            raise AssertionError(f"GrowthBook fact table must preserve upstream windows: {key}")
        sql = _read_repo_path(expected_path)
        table = (
            "experiment_device_facts"
            if key == "vevo_device_outcomes_v1"
            else "experiment_performance_facts"
        )
        _validate_sql(
            expected_path,
            sql,
            table=table,
            experiment_filter=True,
            schema_probe=False,
        )
    production_fact_paths = (
        ((athena.get("production") or {}).get("growthbook_clone") or {}).get(
            "target_fact_table_query_paths"
        )
    )
    if production_fact_paths != EXPECTED_PRODUCTION_FACT_TABLES:
        raise AssertionError("Production fact-table query-path contract changed")
    for key, expected_path in EXPECTED_PRODUCTION_FACT_TABLES.items():
        table = (
            "experiment_device_facts"
            if key == "vevo_device_outcomes_v1"
            else "experiment_performance_facts"
        )
        _validate_sql(
            expected_path,
            _read_repo_path(expected_path),
            table=table,
            experiment_filter=True,
            schema_probe=True,
        )
    if (
        fact_map["vevo_device_outcomes_v1"].get("growthbook_id") != "ftb_19g6mmt2dhrdi"
        or fact_map["vevo_device_outcomes_v1"].get("status")
        != "growthbook_created_query_verified_one_row"
        or fact_map["vevo_device_outcomes_v1"].get("query_verified_date") != "2026-08-21"
    ):
        raise AssertionError("GrowthBook device-outcome fact-table verification state drift")
    if (
        fact_map["vevo_device_outcomes_v1"].get("growthbook_numeric_columns_verified")
        != EXPECTED_OUTCOME_NUMERIC_COLUMNS
        or fact_map["vevo_device_outcomes_v1"].get("column_types_verified_date")
        != "2026-08-21"
    ):
        raise AssertionError("GrowthBook device-outcome column-type state drift")
    if (
        fact_map["vevo_performance_vitals_v1"].get("growthbook_id")
        != "ftb_19g6mmt2e0otd"
        or fact_map["vevo_performance_vitals_v1"].get("status")
        != "growthbook_created_query_verified_one_row"
        or fact_map["vevo_performance_vitals_v1"].get("query_verified_date")
        != "2026-08-21"
    ):
        raise AssertionError("GrowthBook performance fact-table verification state drift")
    if (
        fact_map["vevo_performance_vitals_v1"].get("growthbook_numeric_columns_verified")
        != ["vital_value"]
        or fact_map["vevo_performance_vitals_v1"].get("column_types_verified_date")
        != "2026-08-21"
    ):
        raise AssertionError("GrowthBook performance column-type state drift")

    metrics = workspace.get("metrics", [])
    metric_map = {row.get("key"): row for row in metrics if isinstance(row, dict)}
    if set(metric_map) != EXPECTED_METRICS:
        raise AssertionError("GrowthBook metric set changed")
    for key, metric in metric_map.items():
        if metric.get("fact_table") not in EXPECTED_FACT_TABLES:
            raise AssertionError(f"GrowthBook metric uses unknown fact table: {key}")
        if metric.get("goal") not in {"increase", "decrease"}:
            raise AssertionError(f"GrowthBook metric has no direction: {key}")
        roles = metric.get("roles", {})
        if set(roles) != set(EXPECTED_FEATURES):
            raise AssertionError(f"GrowthBook metric roles are incomplete: {key}")
    for key, growthbook_id in EXPECTED_CREATED_METRICS.items():
        metric = metric_map[key]
        if (
            metric.get("growthbook_id") != growthbook_id
            or metric.get("status") != "growthbook_created_query_verified"
            or metric.get("created_verified_date") != "2026-08-21"
            or metric.get("analysis_query_verified_date") != "2026-08-21"
            or metric.get("analysis_query_synthetic_device_count") != 1
        ):
            raise AssertionError(f"GrowthBook created metric state drift: {key}")
    for key in EXPECTED_PRO_BLOCKED_METRICS:
        metric = metric_map[key]
        if (
            metric.get("growthbook_id") is not None
            or metric.get("status") != "blocked_pending_paid_pro_upgrade"
            or metric.get("blocker") != "quantile_metric_requires_paid_pro"
            or metric.get("blocker_observed_date") != "2026-08-21"
        ):
            raise AssertionError(f"GrowthBook Pro metric blocker state drift: {key}")
    if metric_map["vevo_add_to_cart_24h"]["roles"]["vevo-sk-product-cta-color-001"] != "primary":
        raise AssertionError("CTA A/B must keep add-to-cart as its only primary metric")
    primary_metrics = [
        key
        for key, metric in metric_map.items()
        if metric["roles"]["vevo-sk-product-cta-color-001"] == "primary"
    ]
    if primary_metrics != ["vevo_add_to_cart_24h"]:
        raise AssertionError("CTA A/B must have exactly one primary metric")
    if (
        metric_map["vevo_cm1_per_exposed_device_7d"]["roles"][
            "vevo-sk-product-cta-color-001"
        ]
        != "business_guardrail"
    ):
        raise AssertionError("CTA A/B must keep CM1 per exposed device as business guardrail")
    if metric_map["vevo_cm1_per_exposed_device_7d"].get("metric_contract_version") != reporting.get(
        "metric_contract_version"
    ):
        raise AssertionError("GrowthBook and reporting CM1 contracts differ")
    for key in (
        "vevo_add_to_cart_24h",
        "vevo_purchase_conversion_7d",
        "vevo_revenue_per_exposed_device_7d",
        "vevo_cm1_per_exposed_device_7d",
        "vevo_average_order_value_7d",
        "vevo_cancelled_order_rate_14d",
        "vevo_refunded_order_rate_14d",
        "vevo_client_error_device_rate_24h",
    ):
        if metric_map[key].get("growthbook_window") != "none":
            raise AssertionError(f"precomputed outcome metric must not be re-windowed: {key}")
    for key in ("vevo_lcp_p75_24h", "vevo_inp_p75_24h", "vevo_cls_p75_milli_24h"):
        metric = metric_map[key]
        if (
            metric.get("type") != "quantile"
            or metric.get("quantile") != 0.75
            or metric.get("group_by_experiment_user") is not False
            or metric.get("ignore_zeros") is not False
            or metric.get("growthbook_window_hours") != reporting.get("health_window_hours")
        ):
            raise AssertionError(f"performance p75 contract changed: {key}")

    experiments = workspace.get("experiments", [])
    experiment_map = {row.get("tracking_key"): row for row in experiments if isinstance(row, dict)}
    if set(experiment_map) != set(EXPECTED_FEATURES):
        raise AssertionError("GrowthBook experiment set changed")
    preview_registry = registry.get("environments", {}).get("preview", {})
    reporting_weights = reporting.get("expected_variation_weights", {})
    for experiment_id, feature_key in EXPECTED_FEATURES.items():
        experiment = experiment_map[experiment_id]
        if experiment.get("feature_key") != feature_key:
            raise AssertionError(f"GrowthBook feature key changed for {experiment_id}")
        if (
            experiment.get("assignment_attribute") != "id"
            or experiment.get("traffic_percent") != 100
        ):
            raise AssertionError(f"GrowthBook experiment assignment drift: {experiment_id}")
        experiment_running = (
            experiment_id == "vevo-sk-aa-001" and production_aa_running
        ) or (
            experiment_id == "vevo-sk-product-cta-color-001"
            and production_cta_running
        )
        expected_environment = ["production"] if experiment_running else ["staging"]
        expected_allocation = 100 if experiment_running else 0
        if experiment.get("feature_rule_environments") != expected_environment:
            raise AssertionError(
                f"GrowthBook experiment environment drift: {experiment_id}"
            )
        if experiment.get("production_allocation_percent") != expected_allocation:
            raise AssertionError(
                f"GrowthBook Production allocation drift: {experiment_id}"
            )
        variations = experiment.get("variations", [])
        if variations != preview_registry.get(experiment_id, {}).get("variations"):
            raise AssertionError(f"GrowthBook/collector variation order differs for {experiment_id}")
        expected_weights = reporting_weights.get(experiment_id, {})
        if variations != list(expected_weights) or experiment.get("variation_weights") != list(
            expected_weights.values()
        ):
            raise AssertionError(f"GrowthBook/reporting weights differ for {experiment_id}")
    aa_experiment = experiment_map["vevo-sk-aa-001"]
    expected_aa_analysis = {
        "verified_date": "2026-08-25" if production_aa_lifecycle else "2026-08-21",
        "data_source_id": (
            "ds_19g6mmt5stlp6" if production_aa_lifecycle else "ds_19g6mmt2c4dmn"
        ),
        "data_source_name": (
            "VEVO Production Experiment Facts"
            if production_aa_lifecycle
            else "VEVO Preview Experiment Facts"
        ),
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
    }
    if not production_aa_lifecycle:
        expected_aa_analysis["assignment_query_id"] = "tbl_mt2c74ol"
    expected_aa_status = (
        "running_production_aa_only"
        if production_aa_running
        else (
            "stopped_production_aa_pass_verified"
            if production_post_aa
            else "running_preview_staging_only"
        )
    )
    expected_aa_feature_status = (
        "live"
        if production_aa_running or not production_post_aa
        else "staging_only"
    )
    expected_aa_revision = (
        3
        if production_aa_running
        else (
            aa_completion.get("stop_readback", {}).get("feature_revision")
            if production_post_aa
            else 2
        )
    )
    if (
        aa_experiment.get("name")
        != (
            "VEVO SK Production A/A measurement validation"
            if production_aa_lifecycle
            else "VEVO SK A/A measurement validation"
        )
        or aa_experiment.get("growthbook_id")
        != (
            "exp_19g6mmt5wugpk"
            if production_aa_lifecycle
            else "exp_19g6mmt1qsqm9"
        )
        or aa_experiment.get("status") != expected_aa_status
        or aa_experiment.get("started_date")
        != ("2026-08-25" if production_aa_lifecycle else "2026-08-21")
        or aa_experiment.get("feature_rule_status") != expected_aa_feature_status
        or aa_experiment.get("feature_rule_revision") != expected_aa_revision
        or aa_experiment.get("analysis_settings") != expected_aa_analysis
    ):
        raise AssertionError("GrowthBook A/A lifecycle running state drift")
    expected_activation_evidence = {
        "browser_observation_sha256": (
            "451e6489df351cc2318751a9d4bd727d107d8aaf9763e406c595fc5824ec8705"
        ),
        "smoke_evidence_sha256": (
            "c21d7418656ad0841851a8afbc642a6ea39328e2151e6dc647ce8c59c06c1823"
        ),
        "workflow_run_id": "32815955896",
        "main_commit": "1965091059e5a35518265aafd282db842f8ea5d3",
        "accepted_collector_delivery_verified": True,
        "sticky_assignment_verified": True,
        "cta_experiment_started": False,
    }
    if production_aa_lifecycle:
        if aa_experiment.get("activation_evidence") != expected_activation_evidence:
            raise AssertionError("GrowthBook A/A activation evidence binding drift")
    elif "activation_evidence" in aa_experiment:
        raise AssertionError("Historical Preview state contains activation evidence")
    cta_experiment = experiment_map["vevo-sk-product-cta-color-001"]
    expected_cta_status = (
        "stopped_production_cta_followup_pending"
        if production_cta_stopped
        else (
            "running_production_cta_only"
            if production_cta_running
            else ("unstarted_draft" if production_aa_running else "draft")
        )
    )
    expected_cta_rule_status = (
        "staging_only"
        if production_cta_stopped
        else (
            "live"
            if production_cta_running
            else ("no_live_rules" if production_aa_running else "draft")
        )
    )
    if (
        cta_experiment.get("status") != expected_cta_status
        or cta_experiment.get("feature_rule_status") != expected_cta_rule_status
        or (("analysis_settings" in cta_experiment) is not (production_cta_running or production_cta_stopped))
    ):
        raise AssertionError(
            "GrowthBook CTA A/B must remain an unstarted draft or match the verified running handoff"
        )
    if (
        cta_sample_plan["experiment_id"] != cta_experiment.get("tracking_key")
        or list(cta_sample_plan["expected_variation_weights"])
        != cta_experiment.get("variations")
        or list(cta_sample_plan["expected_variation_weights"].values())
        != cta_experiment.get("variation_weights")
        or cta_sample_plan["minimum_full_calendar_days"]
        != cta_experiment.get("minimum_days")
        or cta_sample_plan["maximum_full_calendar_days"]
        != cta_experiment.get("maximum_days")
        or cta_sample_plan["provisional"]["total_sample"]
        != cta_experiment.get("provisional_total_sample")
    ):
        raise AssertionError("CTA sample plan/workspace contract differs")
    if cta_sample_plan["status"] == "pending_aa_pass_and_final_sample_freeze":
        if cta_experiment.get("final_sample_status") != "recompute_and_freeze_from_aa_before_launch":
            raise AssertionError("CTA sample pending state differs from workspace")
        forbidden_final_fields = {
            "final_sample_per_arm",
            "final_total_sample",
            "sample_observation_sha256",
            "aa_snapshot_sha256",
        }
        if forbidden_final_fields.intersection(cta_experiment):
            raise AssertionError("CTA pending workspace contains frozen sample fields")
    else:
        final = cta_sample_plan["final"]
        expected_final = {
            "final_sample_status": (
                "frozen_from_hash_bound_aa_running_exact_first_n"
                if production_cta_running or production_cta_stopped
                else "frozen_from_hash_bound_aa_activation_still_blocked"
            ),
            "final_sample_per_arm": final["sample_per_arm"],
            "final_total_sample": final["total_sample"],
            "sample_observation_sha256": final["observation_sha256"],
            "aa_snapshot_sha256": final["aa_snapshot_sha256"],
        }
        if any(cta_experiment.get(key) != value for key, value in expected_final.items()):
            raise AssertionError("CTA frozen sample state differs from workspace")
    if aa_experiment.get("winner_calls_allowed") is not False:
        raise AssertionError("A/A must never be used for a winner call")
    if (
        aa_acceptance["variations"] != aa_experiment.get("variations")
        or aa_acceptance["expected_variation_weights"]
        != reporting_weights.get("vevo-sk-aa-001")
        or aa_acceptance["minimum_full_calendar_days"]
        != aa_experiment.get("minimum_days")
        or aa_acceptance["minimum_eligible_devices"]
        != aa_experiment.get("minimum_eligible_devices")
    ):
        raise AssertionError("A/A evaluator/workspace contract differs")

    gates = workspace.get("decision_gates", {})
    if gates.get("maturity_checkpoint_days") != reporting.get("maturity_checkpoint_days"):
        raise AssertionError("GrowthBook/reporting maturity gates differ")
    if gates.get("price_tests_allowed") is not False:
        raise AssertionError("price testing must remain disabled")
    if gates.get("production_activation_allowed") is not (
        production_aa_running or production_cta_running
    ):
        raise AssertionError("Production experiment activation gate state drift")
    if (
        aa_acceptance["growthbook_reporting_count_difference_max_percent"]
        != gates.get("growthbook_count_difference_max_percent")
        or aa_acceptance["exact_order_join_rate_min_percent"]
        != gates.get("exact_order_join_min_percent")
    ):
        raise AssertionError("A/A evaluator/workspace release gates differ")
    try:
        from scripts.validate_growthbook_production_aa_activation import (
            validate_activation_handoff,
        )
    except ModuleNotFoundError:
        from validate_growthbook_production_aa_activation import (
            validate_activation_handoff,
        )
    # Recorder tests validate earlier, synthetic transition states. The redundant
    # cross-system activation handoff applies to the checked-in workspace and
    # registry only; dedicated activation tests cover altered handoff payloads.
    checked_in_workspace = json.loads(WORKSPACE_PATH.read_text(encoding="utf-8"))
    checked_in_registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    if workspace == checked_in_workspace and registry == checked_in_registry:
        if production_aa_running:
            validate_activation_handoff(activation, workspace, registry)
        if production_cta_running:
            if cta_activation_observation is None:
                raise AssertionError("CTA running state has no activation observation")
            validate_cta_running_handoff(
                cta_activation,
                workspace,
                registry,
                cta_activation_observation,
            )
        try:
            from scripts.validate_growthbook_aa_measurement_window import (
                validate as validate_aa_measurement_window,
            )
        except ModuleNotFoundError:
            from validate_growthbook_aa_measurement_window import (
                validate as validate_aa_measurement_window,
            )
        validate_aa_measurement_window()


def main() -> int:
    try:
        validate()
        print("validate_growthbook_workspace.py: OK")
        return 0
    except Exception as exc:  # pragma: no cover - command failure path
        print(f"validate_growthbook_workspace.py: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
