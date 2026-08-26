#!/usr/bin/env python3
"""Evaluate the frozen VEVO CTA A/B from one aggregate, PII-free final snapshot.

The evaluator is intentionally offline. It never queries or mutates GrowthBook, AWS,
GTM, Meta Ads, BiznisWeb, traffic, prices, cart, checkout, or orders.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import re
import tempfile
from datetime import datetime, time, timedelta, timezone
from statistics import NormalDist
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

try:
    from freeze_growthbook_cta_sample import (
        CtaSampleFreezeError,
        canonical_json_bytes as canonical_sample_plan_bytes,
        validate_plan as validate_sample_plan,
    )
except ModuleNotFoundError:  # Imported as scripts.evaluate_growthbook_cta.
    from scripts.freeze_growthbook_cta_sample import (
        CtaSampleFreezeError,
        canonical_json_bytes as canonical_sample_plan_bytes,
        validate_plan as validate_sample_plan,
    )


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_decision_contract.json"
)
DEFAULT_SAMPLE_PLAN_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_sample_plan.json"
)
DEFAULT_LIFECYCLE_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_cta_lifecycle_reconciliation.json"
)

HASH_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SAFE_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=/+-]{0,511}$")
RUN_ID_RE = re.compile(r"^[1-9][0-9]{5,19}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
CTA_LIFECYCLE_QUERY_SHA256 = (
    "5a3548fa877d206e666c369fd19c4c4da121ccd2059354da55361fae86ecf9d5"
)

CONTRACT_KEYS = {
    "schema_version",
    "experiment_id",
    "metric_contract_version",
    "timezone",
    "variations",
    "expected_variation_weights",
    "decision_cohort",
    "primary_metric",
    "primary_test",
    "decision_timing",
    "quality_thresholds",
    "business_guardrail",
    "lifecycle_guardrails",
    "performance_guardrails",
    "decision_rules",
    "mutation_policy",
}
SNAPSHOT_KEYS = {
    "schema_version",
    "evidence_type",
    "experiment_id",
    "metric_contract_version",
    "sample_plan_sha256",
    "aa_snapshot_sha256",
    "lifecycle_reconciliation_sha256",
    "assignment_started_at_utc",
    "assignment_ended_at_utc",
    "evaluated_at_utc",
    "assignment_stopped",
    "production_allocation_percent",
    "decision_cohort",
    "quality",
    "variations",
}
COHORT_KEYS = {
    "selection_method",
    "target_total_sample",
    "eligible_devices_seen_before_stop",
    "included_devices",
}
QUALITY_KEYS = {
    "reporting_device_count",
    "growthbook_device_count",
    "duplicate_event_rate_percent",
    "exact_joined_transaction_count",
    "exact_join_rate_percent",
    "unmatched_transaction_count",
    "ambiguous_transaction_count",
    "contaminated_device_count",
    "privacy_audit_passed",
    "first_n_selection_query_verified",
    "all_exposures_24h_mature",
    "all_orders_7d_mature",
    "all_lifecycles_14d_mature",
    "price_integrity_passed",
    "cart_checkout_health_passed",
    "rollback_ready",
}
VARIATION_KEYS = {
    "eligible_devices",
    "add_to_cart_devices",
    "purchase_devices",
    "joined_order_count",
    "net_revenue_sum_eur",
    "net_revenue_sum_squares_eur2",
    "cm1_sum_eur",
    "cm1_sum_squares_eur2",
    "cancelled_order_count",
    "refunded_order_count",
    "client_error_devices",
    "measured_page_loads",
    "lcp_p75_ms",
    "inp_p75_ms",
    "cls_p75_milli",
}
LIFECYCLE_KEYS = {
    "schema_version",
    "target_experiment_id",
    "source_experiment_id",
    "metric_contract_version",
    "status",
    "workflow",
    "artifact_name",
    "artifact_file",
    "source_completion_manifest",
    "source_snapshot_manifest",
    "query_template_path",
    "query_template_sha256",
    "order_window_days",
    "lifecycle_checkpoint_days",
    "minimum_followup_days_after_source_end",
    "verified",
    "observation_path",
    "observation_sha256",
    "workflow_run_id",
    "main_commit",
    "source_completion_sha256",
    "source_aa_snapshot_sha256",
    "reporting_quality_object_key",
    "reporting_quality_object_sha256",
    "verified_at_utc",
    "refund_creditnote_value_parity_verified",
    "non_realized_value_policy_verified",
    "cta_outcome_data_read",
    "customer_or_order_identity_in_evidence",
    "activation_allowed",
}
LIFECYCLE_OBSERVATION_KEYS = {
    "schema_version",
    "evidence_type",
    "target_experiment_id",
    "source_experiment_id",
    "metric_contract_version",
    "workflow_run_id",
    "main_commit",
    "observed_at_utc",
    "source_from_utc",
    "source_through_utc",
    "order_window_days",
    "lifecycle_checkpoint_days",
    "minimum_followup_days_after_source_end",
    "source_completion_sha256",
    "source_aa_snapshot_sha256",
    "query_template_sha256",
    "reporting_quality_object_key",
    "reporting_quality_object_sha256",
    "eligible_devices_checked",
    "joined_orders_checked",
    "cm1_absolute_difference_eur",
    "mature_orders_checked",
    "immature_orders_checked",
    "cancelled_orders_checked",
    "refunded_or_creditnoted_orders_checked",
    "direct_curated_cm1_sum_eur",
    "athena_reporting_cm1_sum_eur",
    "lifecycle_counts_match",
    "refund_creditnote_value_parity_verified",
    "non_realized_value_policy",
    "non_realized_value_policy_verified",
    "cta_outcome_data_read",
    "contains_event_or_device_identity",
    "customer_or_order_identity_in_evidence",
    "source_read_only",
    "no_external_mutation",
}


class CtaEvaluationError(ValueError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CtaEvaluationError(message)


def _exact_object(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    actual = set(value)
    _require(
        actual == keys,
        f"{field} keys drift: missing={sorted(keys - actual)} extra={sorted(actual - keys)}",
    )
    return value


def _integer(
    value: Any, field: str, *, minimum: int = 0, maximum: int | None = None
) -> int:
    _require(type(value) is int, f"{field} must be an integer")
    _require(value >= minimum, f"{field} is below {minimum}")
    if maximum is not None:
        _require(value <= maximum, f"{field} exceeds {maximum}")
    return value


def _number(
    value: Any,
    field: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    _require(
        type(value) in {int, float} and math.isfinite(float(value)),
        f"{field} must be finite",
    )
    result = float(value)
    if minimum is not None:
        _require(result >= minimum, f"{field} is below {minimum}")
    if maximum is not None:
        _require(result <= maximum, f"{field} exceeds {maximum}")
    return result


def _boolean(value: Any, field: str) -> bool:
    _require(type(value) is bool, f"{field} must be boolean")
    return value


def _parse_utc(value: Any, field: str) -> datetime:
    text = str(value or "")
    _require(
        UTC_RE.fullmatch(text) is not None,
        f"{field} must use whole-second UTC Z format",
    )
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError as exc:
        raise CtaEvaluationError(f"{field} is invalid") from exc


def _full_calendar_days(started: datetime, ended: datetime, zone: ZoneInfo) -> int:
    _require(ended >= started, "assignment end predates assignment start")
    local_start = started.astimezone(zone)
    local_end = ended.astimezone(zone)
    start_midnight = datetime.combine(local_start.date(), time.min, tzinfo=zone)
    first_full_day = (
        start_midnight
        if local_start == start_midnight
        else start_midnight + timedelta(days=1)
    )
    if local_end < first_full_day:
        return 0
    return max(0, (local_end.date() - first_full_day.date()).days)


def canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def validate_contract(contract: Mapping[str, Any]) -> None:
    root = _exact_object(contract, CONTRACT_KEYS, "CTA decision contract")
    _require(root["schema_version"] == 1, "CTA decision contract schema drift")
    _require(
        root["experiment_id"] == "vevo-sk-product-cta-color-001", "CTA experiment drift"
    )
    _require(
        root["metric_contract_version"] == "vevo_cm1_v1_2026-08-20",
        "CTA metric contract drift",
    )
    _require(root["timezone"] == "Europe/Bratislava", "CTA timezone drift")
    _require(
        root["variations"] == ["control", "brand_contrast"], "CTA variations drift"
    )
    _require(
        root["expected_variation_weights"] == {"control": 0.5, "brand_contrast": 0.5},
        "CTA weights drift",
    )
    _require(
        root["decision_cohort"]
        == "first_n_eligible_devices_ordered_by_first_exposure_then_device_id",
        "CTA cohort drift",
    )
    _require(
        root["primary_metric"]
        == "add_to_cart_within_24h_per_first_rendered_cta_exposed_device",
        "CTA primary metric drift",
    )

    primary = _exact_object(
        root["primary_test"],
        {
            "method",
            "two_sided_alpha_percent",
            "confidence_interval",
            "minimum_effect_percentage_points",
        },
        "primary_test",
    )
    _require(
        primary["method"] == "fixed_horizon_pooled_two_proportion_z_test",
        "CTA primary method drift",
    )
    _require(
        primary["confidence_interval"] == "unpooled_wald_95_percent",
        "CTA primary interval drift",
    )
    _require(primary["two_sided_alpha_percent"] == 5, "CTA alpha drift")
    _require(
        primary["minimum_effect_percentage_points"] == 0, "CTA minimum effect drift"
    )

    timing = _exact_object(
        root["decision_timing"],
        {
            "minimum_full_calendar_days",
            "maximum_full_calendar_days",
            "required_followup_days_after_assignment_stop",
            "assignment_must_be_stopped",
            "one_final_look_only",
            "safety_guardrails_may_stop_early",
        },
        "decision_timing",
    )
    _require(
        timing
        == {
            "minimum_full_calendar_days": 14,
            "maximum_full_calendar_days": 42,
            "required_followup_days_after_assignment_stop": 21,
            "assignment_must_be_stopped": True,
            "one_final_look_only": True,
            "safety_guardrails_may_stop_early": True,
        },
        "CTA decision timing drift",
    )

    quality = _exact_object(
        root["quality_thresholds"],
        {
            "srm_p_value_min",
            "growthbook_reporting_count_difference_max_percent",
            "duplicate_event_rate_max_percent",
            "exact_order_join_rate_min_percent",
            "minimum_exact_joined_transactions",
            "maximum_contaminated_devices",
            "maximum_ambiguous_transactions",
        },
        "quality_thresholds",
    )
    _require(
        quality
        == {
            "srm_p_value_min": 0.001,
            "growthbook_reporting_count_difference_max_percent": 2,
            "duplicate_event_rate_max_percent": 0.5,
            "exact_order_join_rate_min_percent": 98,
            "minimum_exact_joined_transactions": 1,
            "maximum_contaminated_devices": 0,
            "maximum_ambiguous_transactions": 0,
        },
        "CTA quality thresholds drift",
    )

    business = _exact_object(
        root["business_guardrail"],
        {
            "metric",
            "method",
            "confidence_level_percent",
            "non_inferiority_margin_relative_percent",
            "minimum_control_mean_eur",
        },
        "business_guardrail",
    )
    _require(
        business
        == {
            "metric": "vevo_cm1_per_exposed_device_7d",
            "method": "normal_mean_difference_from_device_sum_squares",
            "confidence_level_percent": 95,
            "non_inferiority_margin_relative_percent": 10,
            "minimum_control_mean_eur": 0.01,
        },
        "CTA business guardrail drift",
    )

    lifecycle = _exact_object(
        root["lifecycle_guardrails"],
        {
            "minimum_mature_joined_orders_per_arm",
            "cancelled_rate_max_increase_percentage_points",
            "refunded_rate_max_increase_percentage_points",
            "reconciliation_manifest",
        },
        "lifecycle_guardrails",
    )
    _require(
        lifecycle
        == {
            "minimum_mature_joined_orders_per_arm": 20,
            "cancelled_rate_max_increase_percentage_points": 5,
            "refunded_rate_max_increase_percentage_points": 5,
            "reconciliation_manifest": "projects/vevo/growthbook_cta_lifecycle_reconciliation.json",
        },
        "CTA lifecycle guardrails drift",
    )

    performance = _exact_object(
        root["performance_guardrails"],
        {
            "minimum_measured_page_loads_per_arm",
            "lcp_degradation_absolute_ms",
            "lcp_degradation_relative_percent",
            "inp_degradation_absolute_ms",
            "inp_degradation_relative_percent",
            "cls_degradation_absolute_milli",
            "client_error_rate_increase_max_percentage_points",
        },
        "performance_guardrails",
    )
    _require(
        performance
        == {
            "minimum_measured_page_loads_per_arm": 200,
            "lcp_degradation_absolute_ms": 200,
            "lcp_degradation_relative_percent": 10,
            "inp_degradation_absolute_ms": 20,
            "inp_degradation_relative_percent": 10,
            "cls_degradation_absolute_milli": 20,
            "client_error_rate_increase_max_percentage_points": 0.5,
        },
        "CTA performance thresholds drift",
    )

    rules = _exact_object(
        root["decision_rules"],
        {"win", "lose", "inconclusive", "not_ready"},
        "decision_rules",
    )
    _require(
        rules
        == {
            "win": "fixed_sample_primary_significant_positive_and_all_safety_guardrails_pass",
            "lose": "fixed_sample_primary_significant_negative_or_material_safety_harm",
            "inconclusive": "fixed_sample_or_max_duration_reached_without_win_or_lose",
            "not_ready": "assignment_running_followup_open_sample_unfrozen_or_final_gate_missing",
        },
        "CTA decision rules drift",
    )
    mutation = _exact_object(
        root["mutation_policy"],
        {
            "automatic_growthbook_mutation_allowed",
            "automatic_gtm_mutation_allowed",
            "automatic_meta_mutation_allowed",
            "automatic_biznisweb_mutation_allowed",
            "price_test_allowed",
        },
        "mutation_policy",
    )
    _require(
        all(value is False for value in mutation.values()),
        "CTA evaluator must never authorize mutation or price tests",
    )


def validate_lifecycle_observation(observation: Mapping[str, Any]) -> None:
    root = _exact_object(
        observation,
        LIFECYCLE_OBSERVATION_KEYS,
        "CTA lifecycle observation",
    )
    _require(root["schema_version"] == 2, "CTA lifecycle observation schema drift")
    _require(
        root["evidence_type"]
        == "vevo_growthbook_cta_prelaunch_lifecycle_reconciliation",
        "CTA lifecycle observation type drift",
    )
    _require(
        root["target_experiment_id"] == "vevo-sk-product-cta-color-001"
        and root["source_experiment_id"] == "vevo-sk-aa-001",
        "CTA lifecycle observation source/target experiment drift",
    )
    _require(
        root["metric_contract_version"] == "vevo_cm1_v1_2026-08-20",
        "CTA lifecycle observation metric drift",
    )
    _require(
        RUN_ID_RE.fullmatch(str(root["workflow_run_id"] or "")) is not None,
        "CTA lifecycle workflow run ID is invalid",
    )
    _require(
        COMMIT_RE.fullmatch(str(root["main_commit"] or "")) is not None,
        "CTA lifecycle main commit is invalid",
    )
    observed = _parse_utc(
        root["observed_at_utc"], "lifecycle_observation.observed_at_utc"
    )
    source_from = _parse_utc(
        root["source_from_utc"], "lifecycle_observation.source_from_utc"
    )
    source_through = _parse_utc(
        root["source_through_utc"], "lifecycle_observation.source_through_utc"
    )
    _require(source_through > source_from, "CTA lifecycle source window is invalid")
    _require(
        root["order_window_days"] == 7
        and root["lifecycle_checkpoint_days"] == 14
        and root["minimum_followup_days_after_source_end"] == 21,
        "CTA lifecycle observation maturity drift",
    )
    _require(
        observed >= source_through + timedelta(days=21),
        "CTA lifecycle observation predates order plus lifecycle maturity",
    )
    for field in (
        "source_completion_sha256",
        "source_aa_snapshot_sha256",
        "query_template_sha256",
        "reporting_quality_object_sha256",
    ):
        _require(
            HASH_RE.fullmatch(str(root[field] or "")) is not None,
            f"CTA lifecycle observation {field} is invalid",
        )
    _require(
        SAFE_KEY_RE.fullmatch(str(root["reporting_quality_object_key"] or ""))
        is not None,
        "CTA lifecycle observation reporting object key is invalid",
    )
    _require(
        str(root["reporting_quality_object_key"]).startswith(
            "experiment-events/curated/quality/experiment_id=vevo-sk-aa-001/"
        ),
        "CTA lifecycle observation reporting source is not completed A/A",
    )
    direct_cm1 = _number(
        root["direct_curated_cm1_sum_eur"],
        "lifecycle_observation.direct_curated_cm1_sum_eur",
    )
    athena_cm1 = _number(
        root["athena_reporting_cm1_sum_eur"],
        "lifecycle_observation.athena_reporting_cm1_sum_eur",
    )
    difference = _number(
        root["cm1_absolute_difference_eur"],
        "lifecycle_observation.cm1_absolute_difference_eur",
        minimum=0,
    )
    for value, field in (
        (direct_cm1, "direct_curated_cm1_sum_eur"),
        (athena_cm1, "athena_reporting_cm1_sum_eur"),
        (difference, "cm1_absolute_difference_eur"),
    ):
        _require(
            value == round(value, 2),
            f"CTA lifecycle observation {field} must use exact cent precision",
        )
    _require(
        difference == abs(direct_cm1 - athena_cm1),
        "CTA lifecycle observation CM1 difference is inconsistent",
    )
    _require(difference == 0, "CTA lifecycle observation CM1 parity failed")
    _integer(
        root["eligible_devices_checked"],
        "lifecycle_observation.eligible_devices_checked",
        minimum=1,
    )
    joined = _integer(
        root["joined_orders_checked"],
        "lifecycle_observation.joined_orders_checked",
        minimum=1,
    )
    mature = _integer(
        root["mature_orders_checked"],
        "lifecycle_observation.mature_orders_checked",
        minimum=1,
    )
    immature = _integer(
        root["immature_orders_checked"],
        "lifecycle_observation.immature_orders_checked",
    )
    cancelled = _integer(
        root["cancelled_orders_checked"],
        "lifecycle_observation.cancelled_orders_checked",
    )
    refunded = _integer(
        root["refunded_or_creditnoted_orders_checked"],
        "lifecycle_observation.refunded_or_creditnoted_orders_checked",
    )
    lifecycle_rows = cancelled + refunded
    _require(
        immature == 0 and mature == joined, "CTA lifecycle cohort is not fully mature"
    )
    _require(lifecycle_rows >= 1, "CTA lifecycle observation lacks a lifecycle case")
    _require(
        mature >= lifecycle_rows, "CTA lifecycle observation counts are inconsistent"
    )
    _require(
        root["lifecycle_counts_match"] is True, "CTA lifecycle counts do not match"
    )
    _require(
        root["refund_creditnote_value_parity_verified"] is True,
        "CTA lifecycle value parity is not verified",
    )
    _require(
        root["non_realized_value_policy"]
        == "zero_value_until_realized_with_explicit_lifecycle_counts",
        "CTA lifecycle non-realized value policy drift",
    )
    _require(
        root["non_realized_value_policy_verified"] is True,
        "CTA lifecycle non-realized value policy is not verified",
    )
    _require(
        root["cta_outcome_data_read"] is False,
        "CTA lifecycle preflight read CTA outcome data",
    )
    _require(
        root["contains_event_or_device_identity"] is False,
        "CTA lifecycle observation exposed event/device identity",
    )
    _require(
        root["customer_or_order_identity_in_evidence"] is False,
        "CTA lifecycle observation must remain identity-free",
    )
    _require(root["source_read_only"] is True, "CTA lifecycle source was not read-only")
    _require(
        root["no_external_mutation"] is True,
        "CTA lifecycle evidence mutated an external system",
    )


def validate_lifecycle_manifest(
    manifest: Mapping[str, Any],
    observation: Mapping[str, Any] | None = None,
) -> None:
    root = _exact_object(manifest, LIFECYCLE_KEYS, "CTA lifecycle reconciliation")
    _require(root["schema_version"] == 2, "CTA lifecycle schema drift")
    _require(
        root["target_experiment_id"] == "vevo-sk-product-cta-color-001"
        and root["source_experiment_id"] == "vevo-sk-aa-001",
        "CTA lifecycle source/target experiment drift",
    )
    _require(
        root["metric_contract_version"] == "vevo_cm1_v1_2026-08-20",
        "CTA lifecycle metric drift",
    )
    _require(
        root["workflow"]
        == ".github/workflows/collect-vevo-growthbook-cta-lifecycle-preflight.yml"
        and root["artifact_name"] == "vevo-growthbook-cta-lifecycle-preflight"
        and root["artifact_file"] == "vevo-growthbook-cta-lifecycle-preflight.json",
        "CTA lifecycle producer identity drift",
    )
    _require(
        root["source_completion_manifest"]
        == "projects/vevo/growthbook_production_aa_completion.json"
        and root["source_snapshot_manifest"]
        == "projects/vevo/growthbook_aa_snapshot.json",
        "CTA lifecycle source manifest drift",
    )
    _require(
        root["query_template_path"]
        == "projects/vevo/growthbook_sql/cta_lifecycle_preflight_production.sql"
        and root["query_template_sha256"] == CTA_LIFECYCLE_QUERY_SHA256,
        "CTA lifecycle query contract drift",
    )
    _require(
        root["order_window_days"] == 7
        and root["lifecycle_checkpoint_days"] == 14
        and root["minimum_followup_days_after_source_end"] == 21,
        "CTA lifecycle maturity contract drift",
    )
    _require(
        root["activation_allowed"] is False,
        "lifecycle reconciliation must never activate CTA",
    )
    _require(
        root["cta_outcome_data_read"] is False,
        "lifecycle preflight must not read CTA outcomes",
    )
    _require(
        root["customer_or_order_identity_in_evidence"] is False,
        "lifecycle evidence must remain identity-free",
    )
    if root["verified"] is False:
        _require(
            root["status"] == "pending_completed_aa_21d_lifecycle_preflight",
            "pending lifecycle status drift",
        )
        for field in (
            "observation_path",
            "observation_sha256",
            "workflow_run_id",
            "main_commit",
            "source_completion_sha256",
            "source_aa_snapshot_sha256",
            "reporting_quality_object_key",
            "reporting_quality_object_sha256",
            "verified_at_utc",
        ):
            _require(root[field] is None, f"pending lifecycle {field} must be null")
        _require(
            root["refund_creditnote_value_parity_verified"] is False,
            "pending lifecycle parity must be false",
        )
        _require(
            root["non_realized_value_policy_verified"] is False,
            "pending lifecycle policy must be false",
        )
        return
    _require(
        root["status"] == "verified_completed_aa_21d_lifecycle_preflight",
        "verified lifecycle status drift",
    )
    _require(
        root["observation_path"]
        == "projects/vevo/growthbook_cta_lifecycle_observation.json",
        "verified lifecycle observation path drift",
    )
    for field in (
        "observation_sha256",
        "source_completion_sha256",
        "source_aa_snapshot_sha256",
        "reporting_quality_object_sha256",
    ):
        _require(
            HASH_RE.fullmatch(str(root[field] or "")) is not None,
            f"lifecycle {field} is invalid",
        )
    _require(
        RUN_ID_RE.fullmatch(str(root["workflow_run_id"] or "")) is not None,
        "lifecycle workflow run ID is invalid",
    )
    _require(
        COMMIT_RE.fullmatch(str(root["main_commit"] or "")) is not None,
        "lifecycle main commit is invalid",
    )
    _require(
        SAFE_KEY_RE.fullmatch(str(root["reporting_quality_object_key"] or ""))
        is not None,
        "lifecycle reporting object key is invalid",
    )
    _parse_utc(root["verified_at_utc"], "lifecycle.verified_at_utc")
    _require(
        root["refund_creditnote_value_parity_verified"] is True,
        "lifecycle value parity is not verified",
    )
    _require(
        root["non_realized_value_policy_verified"] is True,
        "lifecycle non-realized value policy is not verified",
    )
    _require(observation is not None, "verified lifecycle observation is missing")
    validate_lifecycle_observation(observation)
    _require(
        root["observation_sha256"] == _sha256(observation),
        "verified lifecycle observation SHA-256 mismatch",
    )
    _require(
        root["reporting_quality_object_key"]
        == observation["reporting_quality_object_key"],
        "verified lifecycle reporting object key mismatch",
    )
    _require(
        root["reporting_quality_object_sha256"]
        == observation["reporting_quality_object_sha256"],
        "verified lifecycle reporting object SHA-256 mismatch",
    )
    for field in (
        "workflow_run_id",
        "main_commit",
        "source_completion_sha256",
        "source_aa_snapshot_sha256",
    ):
        _require(
            root[field] == observation[field], f"verified lifecycle {field} mismatch"
        )
    _require(
        root["query_template_sha256"] == observation["query_template_sha256"],
        "verified lifecycle query template SHA-256 mismatch",
    )
    _require(
        _parse_utc(root["verified_at_utc"], "lifecycle.verified_at_utc")
        >= _parse_utc(
            observation["observed_at_utc"],
            "lifecycle_observation.observed_at_utc",
        ),
        "lifecycle was verified before the observation",
    )


def _validate_variation(row: Any, variation: str) -> Mapping[str, Any]:
    item = _exact_object(row, VARIATION_KEYS, f"variations.{variation}")
    devices = _integer(
        item["eligible_devices"], f"{variation}.eligible_devices", minimum=1
    )
    for field in ("add_to_cart_devices", "purchase_devices", "client_error_devices"):
        _integer(item[field], f"{variation}.{field}", maximum=devices)
    orders = _integer(item["joined_order_count"], f"{variation}.joined_order_count")
    _require(
        item["purchase_devices"] <= orders <= devices * 7,
        f"{variation}.joined_order_count is inconsistent",
    )
    for field in ("cancelled_order_count", "refunded_order_count"):
        _integer(item[field], f"{variation}.{field}", maximum=orders)
    _number(item["net_revenue_sum_eur"], f"{variation}.net_revenue_sum_eur", minimum=0)
    revenue_squares = _number(
        item["net_revenue_sum_squares_eur2"],
        f"{variation}.net_revenue_sum_squares_eur2",
        minimum=0,
    )
    cm1_sum = _number(item["cm1_sum_eur"], f"{variation}.cm1_sum_eur")
    cm1_squares = _number(
        item["cm1_sum_squares_eur2"], f"{variation}.cm1_sum_squares_eur2", minimum=0
    )
    _require(
        revenue_squares + 1e-8 >= float(item["net_revenue_sum_eur"]) ** 2 / devices,
        f"{variation} revenue sum squares are inconsistent",
    )
    _require(
        cm1_squares + 1e-8 >= cm1_sum**2 / devices,
        f"{variation} CM1 sum squares are inconsistent",
    )
    measured = _integer(item["measured_page_loads"], f"{variation}.measured_page_loads")
    for field in ("lcp_p75_ms", "inp_p75_ms", "cls_p75_milli"):
        value = item[field]
        if value is not None:
            _number(value, f"{variation}.{field}", minimum=0)
        elif measured:
            raise CtaEvaluationError(
                f"{variation}.{field} is missing with measured page loads"
            )
    return item


def validate_snapshot(snapshot: Mapping[str, Any], contract: Mapping[str, Any]) -> None:
    root = _exact_object(snapshot, SNAPSHOT_KEYS, "CTA snapshot")
    _require(root["schema_version"] == 1, "CTA snapshot schema drift")
    _require(
        root["evidence_type"] == "vevo_growthbook_cta_aggregate_snapshot",
        "CTA snapshot type drift",
    )
    _require(
        root["experiment_id"] == contract["experiment_id"],
        "CTA snapshot experiment mismatch",
    )
    _require(
        root["metric_contract_version"] == contract["metric_contract_version"],
        "CTA snapshot metric mismatch",
    )
    for field in (
        "sample_plan_sha256",
        "aa_snapshot_sha256",
        "lifecycle_reconciliation_sha256",
    ):
        _require(
            HASH_RE.fullmatch(str(root[field] or "")) is not None,
            f"CTA snapshot {field} is invalid",
        )
    started = _parse_utc(root["assignment_started_at_utc"], "assignment_started_at_utc")
    ended = _parse_utc(root["assignment_ended_at_utc"], "assignment_ended_at_utc")
    evaluated = _parse_utc(root["evaluated_at_utc"], "evaluated_at_utc")
    _require(started <= ended <= evaluated, "CTA snapshot timestamps are out of order")
    _boolean(root["assignment_stopped"], "assignment_stopped")
    _integer(
        root["production_allocation_percent"],
        "production_allocation_percent",
        maximum=100,
    )

    cohort = _exact_object(root["decision_cohort"], COHORT_KEYS, "decision_cohort")
    _require(
        cohort["selection_method"] == contract["decision_cohort"],
        "CTA cohort selection drift",
    )
    target = _integer(
        cohort["target_total_sample"], "decision_cohort.target_total_sample", minimum=2
    )
    seen = _integer(
        cohort["eligible_devices_seen_before_stop"],
        "decision_cohort.eligible_devices_seen_before_stop",
        minimum=1,
    )
    included = _integer(
        cohort["included_devices"],
        "decision_cohort.included_devices",
        minimum=1,
        maximum=target,
    )
    _require(seen >= included, "CTA cohort includes unseen devices")

    quality = _exact_object(root["quality"], QUALITY_KEYS, "quality")
    for field in (
        "reporting_device_count",
        "growthbook_device_count",
        "exact_joined_transaction_count",
        "unmatched_transaction_count",
        "ambiguous_transaction_count",
        "contaminated_device_count",
    ):
        _integer(quality[field], f"quality.{field}")
    for field in ("duplicate_event_rate_percent", "exact_join_rate_percent"):
        _number(quality[field], f"quality.{field}", minimum=0, maximum=100)
    for field in QUALITY_KEYS - {
        "reporting_device_count",
        "growthbook_device_count",
        "exact_joined_transaction_count",
        "unmatched_transaction_count",
        "ambiguous_transaction_count",
        "contaminated_device_count",
        "duplicate_event_rate_percent",
        "exact_join_rate_percent",
    }:
        _boolean(quality[field], f"quality.{field}")

    variations = _exact_object(
        root["variations"], set(contract["variations"]), "variations"
    )
    rows = {
        variation: _validate_variation(variations[variation], variation)
        for variation in contract["variations"]
    }
    _require(
        sum(row["eligible_devices"] for row in rows.values()) == included,
        "CTA variation counts do not equal the decision cohort",
    )
    _require(
        quality["reporting_device_count"] == included,
        "CTA reporting count does not equal the decision cohort",
    )


def _srm_p_value(counts: Mapping[str, int], weights: Mapping[str, float]) -> float:
    total = sum(counts.values())
    if total == 0:
        return 1.0
    chi_square = sum(
        (counts[key] - total * weight) ** 2 / (total * weight)
        for key, weight in weights.items()
    )
    return math.erfc(math.sqrt(chi_square / 2.0))


def _proportion_stats(
    control_success: int,
    control_n: int,
    variant_success: int,
    variant_n: int,
    alpha: float,
) -> dict[str, float]:
    control_rate = control_success / control_n
    variant_rate = variant_success / variant_n
    difference = variant_rate - control_rate
    pooled = (control_success + variant_success) / (control_n + variant_n)
    pooled_se = math.sqrt(pooled * (1 - pooled) * (1 / control_n + 1 / variant_n))
    z_score = difference / pooled_se if pooled_se else 0.0
    p_value = math.erfc(abs(z_score) / math.sqrt(2.0)) if pooled_se else 1.0
    unpooled_se = math.sqrt(
        control_rate * (1 - control_rate) / control_n
        + variant_rate * (1 - variant_rate) / variant_n
    )
    critical = NormalDist().inv_cdf(1 - alpha / 2)
    lower = difference - critical * unpooled_se
    upper = difference + critical * unpooled_se
    relative_lift = (difference / control_rate * 100) if control_rate else None
    return {
        "control_rate_percent": round(control_rate * 100, 6),
        "variant_rate_percent": round(variant_rate * 100, 6),
        "difference_percentage_points": round(difference * 100, 6),
        "relative_lift_percent": round(relative_lift, 6)
        if relative_lift is not None
        else None,
        "z_score": round(z_score, 8),
        "two_sided_p_value": round(p_value, 12),
        "difference_ci_lower_percentage_points": round(lower * 100, 6),
        "difference_ci_upper_percentage_points": round(upper * 100, 6),
    }


def _mean_difference_stats(
    control: Mapping[str, Any],
    variant: Mapping[str, Any],
    *,
    sum_field: str,
    squares_field: str,
    confidence: float,
) -> dict[str, float]:
    control_n = control["eligible_devices"]
    variant_n = variant["eligible_devices"]
    control_sum = float(control[sum_field])
    variant_sum = float(variant[sum_field])
    control_mean = control_sum / control_n
    variant_mean = variant_sum / variant_n
    control_variance = (
        max(
            0.0,
            (float(control[squares_field]) - control_sum**2 / control_n)
            / (control_n - 1),
        )
        if control_n > 1
        else 0.0
    )
    variant_variance = (
        max(
            0.0,
            (float(variant[squares_field]) - variant_sum**2 / variant_n)
            / (variant_n - 1),
        )
        if variant_n > 1
        else 0.0
    )
    standard_error = math.sqrt(
        control_variance / control_n + variant_variance / variant_n
    )
    critical = NormalDist().inv_cdf(0.5 + confidence / 2)
    difference = variant_mean - control_mean
    lower = difference - critical * standard_error
    upper = difference + critical * standard_error
    return {
        "control_mean_eur": round(control_mean, 6),
        "variant_mean_eur": round(variant_mean, 6),
        "difference_eur": round(difference, 6),
        "difference_ci_lower_eur": round(lower, 6),
        "difference_ci_upper_eur": round(upper, 6),
        "standard_error_eur": round(standard_error, 8),
    }


def _gate(
    gates: list[dict[str, Any]],
    name: str,
    status: str,
    observed: Mapping[str, Any],
    required: Mapping[str, Any],
) -> None:
    _require(status in {"pass", "fail", "not_ready"}, f"invalid gate status for {name}")
    gates.append(
        {
            "name": name,
            "status": status,
            "observed": dict(observed),
            "required": dict(required),
        }
    )


def evaluate(
    snapshot: Mapping[str, Any],
    contract: Mapping[str, Any],
    sample_plan: Mapping[str, Any],
    lifecycle_manifest: Mapping[str, Any],
    lifecycle_observation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    validate_contract(contract)
    try:
        validate_sample_plan(sample_plan)
    except CtaSampleFreezeError as exc:
        raise CtaEvaluationError(f"CTA sample plan is invalid: {exc}") from exc
    validate_lifecycle_manifest(lifecycle_manifest, lifecycle_observation)
    validate_snapshot(snapshot, contract)

    gates: list[dict[str, Any]] = []
    final_plan = sample_plan["final"]
    sample_frozen = sample_plan["status"] == "sample_frozen_activation_still_blocked"
    sample_hash = hashlib.sha256(canonical_sample_plan_bytes(sample_plan)).hexdigest()
    sample_provenance_ok = bool(
        sample_frozen
        and final_plan["total_sample"] is not None
        and snapshot["sample_plan_sha256"] == sample_hash
        and snapshot["aa_snapshot_sha256"] == final_plan["aa_snapshot_sha256"]
        and snapshot["decision_cohort"]["target_total_sample"]
        == final_plan["total_sample"]
    )
    _gate(
        gates,
        "sample_plan_provenance",
        "pass" if sample_provenance_ok else "not_ready",
        {
            "sample_frozen": sample_frozen,
            "sample_plan_sha256_matches": snapshot["sample_plan_sha256"] == sample_hash,
            "aa_snapshot_sha256_matches": snapshot["aa_snapshot_sha256"]
            == final_plan["aa_snapshot_sha256"],
            "target_total_sample": snapshot["decision_cohort"]["target_total_sample"],
        },
        {
            "sample_frozen": True,
            "hashes_match": True,
            "target_matches_frozen_plan": True,
        },
    )

    lifecycle_verified = bool(
        lifecycle_manifest["verified"]
        and lifecycle_manifest["refund_creditnote_value_parity_verified"]
        and lifecycle_manifest["non_realized_value_policy_verified"]
        and snapshot["lifecycle_reconciliation_sha256"]
        == lifecycle_manifest["observation_sha256"]
    )
    _gate(
        gates,
        "lifecycle_value_reconciliation",
        "pass" if lifecycle_verified else "not_ready",
        {
            "verified": lifecycle_manifest["verified"],
            "hash_matches": snapshot["lifecycle_reconciliation_sha256"]
            == lifecycle_manifest["observation_sha256"],
        },
        {"verified": True, "hash_matches": True, "identity_free": True},
    )

    started = _parse_utc(
        snapshot["assignment_started_at_utc"], "assignment_started_at_utc"
    )
    ended = _parse_utc(snapshot["assignment_ended_at_utc"], "assignment_ended_at_utc")
    evaluated = _parse_utc(snapshot["evaluated_at_utc"], "evaluated_at_utc")
    full_days = _full_calendar_days(started, ended, ZoneInfo(contract["timezone"]))
    timing = contract["decision_timing"]
    target = snapshot["decision_cohort"]["target_total_sample"]
    included = snapshot["decision_cohort"]["included_devices"]
    target_reached = included == target
    maximum_duration_reached = full_days >= timing["maximum_full_calendar_days"]
    followup_complete = evaluated >= ended + timedelta(
        days=timing["required_followup_days_after_assignment_stop"]
    )
    timing_ready = bool(
        snapshot["assignment_stopped"]
        and followup_complete
        and full_days >= timing["minimum_full_calendar_days"]
        and (target_reached or maximum_duration_reached)
    )
    _gate(
        gates,
        "fixed_horizon_timing",
        "pass" if timing_ready else "not_ready",
        {
            "full_calendar_days": full_days,
            "assignment_stopped": snapshot["assignment_stopped"],
            "followup_complete": followup_complete,
            "included_devices": included,
            "target_total_sample": target,
            "target_reached": target_reached,
            "maximum_duration_reached": maximum_duration_reached,
        },
        {
            "minimum_days": timing["minimum_full_calendar_days"],
            "maximum_days": timing["maximum_full_calendar_days"],
            "assignment_stopped": True,
            "followup_days": timing["required_followup_days_after_assignment_stop"],
            "target_or_maximum_reached": True,
        },
    )

    control = snapshot["variations"]["control"]
    variant = snapshot["variations"]["brand_contrast"]
    quality = snapshot["quality"]
    thresholds = contract["quality_thresholds"]
    reporting_count = quality["reporting_device_count"]
    growthbook_count = quality["growthbook_device_count"]
    count_difference = (
        abs(growthbook_count - reporting_count) / reporting_count * 100
        if reporting_count
        else 100.0
    )
    srm_p_value = _srm_p_value(
        {
            "control": control["eligible_devices"],
            "brand_contrast": variant["eligible_devices"],
        },
        contract["expected_variation_weights"],
    )
    quality_ok = bool(
        srm_p_value >= thresholds["srm_p_value_min"]
        and count_difference
        <= thresholds["growthbook_reporting_count_difference_max_percent"]
        and quality["duplicate_event_rate_percent"]
        <= thresholds["duplicate_event_rate_max_percent"]
        and quality["exact_joined_transaction_count"]
        >= thresholds["minimum_exact_joined_transactions"]
        and quality["exact_join_rate_percent"]
        >= thresholds["exact_order_join_rate_min_percent"]
        and quality["contaminated_device_count"]
        <= thresholds["maximum_contaminated_devices"]
        and quality["ambiguous_transaction_count"]
        <= thresholds["maximum_ambiguous_transactions"]
        and quality["privacy_audit_passed"]
        and quality["first_n_selection_query_verified"]
    )
    _gate(
        gates,
        "data_quality",
        "pass" if quality_ok else "fail",
        {
            "srm_p_value": round(srm_p_value, 12),
            "growthbook_reporting_count_difference_percent": round(count_difference, 6),
            "duplicate_event_rate_percent": quality["duplicate_event_rate_percent"],
            "exact_joined_transactions": quality["exact_joined_transaction_count"],
            "exact_join_rate_percent": quality["exact_join_rate_percent"],
            "contaminated_devices": quality["contaminated_device_count"],
            "ambiguous_transactions": quality["ambiguous_transaction_count"],
            "privacy_audit_passed": quality["privacy_audit_passed"],
            "first_n_selection_query_verified": quality[
                "first_n_selection_query_verified"
            ],
        },
        dict(thresholds),
    )

    maturity_ok = all(
        quality[field]
        for field in (
            "all_exposures_24h_mature",
            "all_orders_7d_mature",
            "all_lifecycles_14d_mature",
        )
    )
    _gate(
        gates,
        "outcome_maturity",
        "pass" if maturity_ok else "not_ready",
        {
            field: quality[field]
            for field in (
                "all_exposures_24h_mature",
                "all_orders_7d_mature",
                "all_lifecycles_14d_mature",
            )
        },
        {"all": True},
    )

    commerce_integrity = bool(
        quality["price_integrity_passed"]
        and quality["cart_checkout_health_passed"]
        and quality["rollback_ready"]
    )
    commerce_ok = bool(
        commerce_integrity and snapshot["production_allocation_percent"] == 100
    )
    _gate(
        gates,
        "commerce_safety",
        "pass" if commerce_ok else "fail",
        {
            "price_integrity_passed": quality["price_integrity_passed"],
            "cart_checkout_health_passed": quality["cart_checkout_health_passed"],
            "rollback_ready": quality["rollback_ready"],
            "production_allocation_percent": snapshot["production_allocation_percent"],
        },
        {"all_checks": True, "production_allocation_percent": 100},
    )

    primary = _proportion_stats(
        control["add_to_cart_devices"],
        control["eligible_devices"],
        variant["add_to_cart_devices"],
        variant["eligible_devices"],
        contract["primary_test"]["two_sided_alpha_percent"] / 100,
    )

    cm1 = _mean_difference_stats(
        control,
        variant,
        sum_field="cm1_sum_eur",
        squares_field="cm1_sum_squares_eur2",
        confidence=contract["business_guardrail"]["confidence_level_percent"] / 100,
    )
    cm1_margin = (
        cm1["control_mean_eur"]
        * contract["business_guardrail"]["non_inferiority_margin_relative_percent"]
        / 100
    )
    cm1_ready = (
        lifecycle_verified
        and cm1["control_mean_eur"]
        >= contract["business_guardrail"]["minimum_control_mean_eur"]
    )
    cm1_harm = cm1_ready and cm1["difference_ci_upper_eur"] < -cm1_margin
    cm1_noninferior = cm1_ready and cm1["difference_ci_lower_eur"] >= -cm1_margin
    cm1_status = "fail" if cm1_harm else ("pass" if cm1_noninferior else "not_ready")
    _gate(
        gates,
        "cm1_business_guardrail",
        cm1_status,
        cm1,
        {
            "non_inferiority_margin_eur": round(cm1_margin, 6),
            "confidence_level_percent": contract["business_guardrail"][
                "confidence_level_percent"
            ],
            "minimum_control_mean_eur": contract["business_guardrail"][
                "minimum_control_mean_eur"
            ],
        },
    )

    lifecycle_thresholds = contract["lifecycle_guardrails"]
    lifecycle_sample_ready = (
        control["joined_order_count"]
        >= lifecycle_thresholds["minimum_mature_joined_orders_per_arm"]
        and variant["joined_order_count"]
        >= lifecycle_thresholds["minimum_mature_joined_orders_per_arm"]
        and lifecycle_verified
        and quality["all_lifecycles_14d_mature"]
    )
    cancellation_increase = (
        (
            variant["cancelled_order_count"] / variant["joined_order_count"]
            - control["cancelled_order_count"] / control["joined_order_count"]
        )
        * 100
        if control["joined_order_count"] and variant["joined_order_count"]
        else 0.0
    )
    refund_increase = (
        (
            variant["refunded_order_count"] / variant["joined_order_count"]
            - control["refunded_order_count"] / control["joined_order_count"]
        )
        * 100
        if control["joined_order_count"] and variant["joined_order_count"]
        else 0.0
    )
    lifecycle_harm = lifecycle_sample_ready and (
        cancellation_increase
        > lifecycle_thresholds["cancelled_rate_max_increase_percentage_points"]
        or refund_increase
        > lifecycle_thresholds["refunded_rate_max_increase_percentage_points"]
    )
    lifecycle_status = (
        "fail"
        if lifecycle_harm
        else ("pass" if lifecycle_sample_ready else "not_ready")
    )
    _gate(
        gates,
        "cancel_refund_guardrails",
        lifecycle_status,
        {
            "control_joined_orders": control["joined_order_count"],
            "variant_joined_orders": variant["joined_order_count"],
            "cancellation_rate_increase_percentage_points": round(
                cancellation_increase, 6
            ),
            "refund_rate_increase_percentage_points": round(refund_increase, 6),
        },
        {
            "minimum_orders_per_arm": lifecycle_thresholds[
                "minimum_mature_joined_orders_per_arm"
            ],
            "cancellation_increase_max_pp": lifecycle_thresholds[
                "cancelled_rate_max_increase_percentage_points"
            ],
            "refund_increase_max_pp": lifecycle_thresholds[
                "refunded_rate_max_increase_percentage_points"
            ],
        },
    )

    performance = contract["performance_guardrails"]
    performance_ready = all(
        row["measured_page_loads"] >= performance["minimum_measured_page_loads_per_arm"]
        and all(
            row[field] is not None
            for field in ("lcp_p75_ms", "inp_p75_ms", "cls_p75_milli")
        )
        for row in (control, variant)
    )
    lcp_increase = float(variant["lcp_p75_ms"] or 0) - float(control["lcp_p75_ms"] or 0)
    inp_increase = float(variant["inp_p75_ms"] or 0) - float(control["inp_p75_ms"] or 0)
    cls_increase = float(variant["cls_p75_milli"] or 0) - float(
        control["cls_p75_milli"] or 0
    )
    control_error_rate = (
        control["client_error_devices"] / control["eligible_devices"] * 100
    )
    variant_error_rate = (
        variant["client_error_devices"] / variant["eligible_devices"] * 100
    )
    error_increase = variant_error_rate - control_error_rate
    performance_harm = performance_ready and (
        lcp_increase
        > max(
            performance["lcp_degradation_absolute_ms"],
            float(control["lcp_p75_ms"])
            * performance["lcp_degradation_relative_percent"]
            / 100,
        )
        or inp_increase
        > max(
            performance["inp_degradation_absolute_ms"],
            float(control["inp_p75_ms"])
            * performance["inp_degradation_relative_percent"]
            / 100,
        )
        or cls_increase > performance["cls_degradation_absolute_milli"]
        or error_increase
        > performance["client_error_rate_increase_max_percentage_points"]
    )
    performance_status = (
        "fail" if performance_harm else ("pass" if performance_ready else "not_ready")
    )
    _gate(
        gates,
        "performance_guardrails",
        performance_status,
        {
            "control_measured_page_loads": control["measured_page_loads"],
            "variant_measured_page_loads": variant["measured_page_loads"],
            "lcp_increase_ms": round(lcp_increase, 6),
            "inp_increase_ms": round(inp_increase, 6),
            "cls_increase_milli": round(cls_increase, 6),
            "client_error_rate_increase_percentage_points": round(error_increase, 6),
        },
        dict(performance),
    )

    fixed_final_look = bool(
        sample_provenance_ok
        and timing_ready
        and maturity_ok
        and snapshot["assignment_stopped"]
    )
    data_valid = bool(quality_ok and lifecycle_verified)
    safety_harm = bool(
        not commerce_ok or cm1_harm or lifecycle_harm or performance_harm
    )
    operational_harm = bool(not commerce_integrity or performance_harm)
    safety_ready = bool(
        cm1_noninferior
        and lifecycle_status == "pass"
        and performance_status == "pass"
        and commerce_ok
    )
    alpha = contract["primary_test"]["two_sided_alpha_percent"] / 100
    significant_positive = (
        primary["two_sided_p_value"] <= alpha
        and primary["difference_ci_lower_percentage_points"]
        > contract["primary_test"]["minimum_effect_percentage_points"]
    )
    significant_negative = (
        primary["two_sided_p_value"] <= alpha
        and primary["difference_ci_upper_percentage_points"] < 0
    )

    early_safety_stop = bool(
        sample_provenance_ok
        and timing["safety_guardrails_may_stop_early"]
        and operational_harm
    )
    if early_safety_stop:
        verdict = "LOSE"
    elif fixed_final_look and not lifecycle_verified:
        verdict = "NOT_READY"
    elif fixed_final_look and data_valid and safety_harm:
        verdict = "LOSE"
    elif fixed_final_look and data_valid and significant_negative:
        verdict = "LOSE"
    elif (
        fixed_final_look
        and data_valid
        and target_reached
        and significant_positive
        and safety_ready
    ):
        verdict = "WIN"
    elif fixed_final_look and (target_reached or maximum_duration_reached):
        verdict = "INCONCLUSIVE"
    else:
        verdict = "NOT_READY"

    return {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_cta_decision",
        "experiment_id": contract["experiment_id"],
        "evaluated_at_utc": snapshot["evaluated_at_utc"],
        "verdict": verdict,
        "final_decision": verdict in {"WIN", "LOSE", "INCONCLUSIVE"},
        "recommended_variation": "brand_contrast" if verdict == "WIN" else "control",
        "automatic_mutation_allowed": False,
        "summary": {
            "full_calendar_days": full_days,
            "included_devices": included,
            "target_total_sample": target,
            "target_reached": target_reached,
            "maximum_duration_reached": maximum_duration_reached,
            "early_safety_stop": early_safety_stop,
            "srm_p_value": round(srm_p_value, 12),
        },
        "primary_metric": primary,
        "supporting_diagnostics": {
            "purchase_conversion": _proportion_stats(
                control["purchase_devices"],
                control["eligible_devices"],
                variant["purchase_devices"],
                variant["eligible_devices"],
                alpha,
            ),
            "net_revenue_per_exposed_device": _mean_difference_stats(
                control,
                variant,
                sum_field="net_revenue_sum_eur",
                squares_field="net_revenue_sum_squares_eur2",
                confidence=0.95,
            ),
            "average_order_value_eur": {
                "control": round(
                    control["net_revenue_sum_eur"] / control["joined_order_count"], 6
                )
                if control["joined_order_count"]
                else None,
                "brand_contrast": round(
                    variant["net_revenue_sum_eur"] / variant["joined_order_count"], 6
                )
                if variant["joined_order_count"]
                else None,
            },
        },
        "gates": gates,
    }


def _load(path: pathlib.Path, field: str) -> Mapping[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    _require(isinstance(payload, dict), f"{field} must contain an object")
    return payload


def _load_canonical(path: pathlib.Path, field: str) -> Mapping[str, Any]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CtaEvaluationError(f"{field} is not valid UTF-8 JSON") from exc
    _require(isinstance(payload, dict), f"{field} must contain an object")
    _require(
        raw == canonical_json_bytes(payload),
        f"{field} must use canonical JSON encoding",
    )
    return payload


def _repo_path(value: Any, field: str) -> pathlib.Path:
    _require(isinstance(value, str) and value, f"{field} must be a repository path")
    candidate = (ROOT / value).resolve()
    _require(ROOT.resolve() in candidate.parents, f"{field} escapes the repository")
    _require(candidate.is_file(), f"{field} does not exist")
    return candidate


def _write_atomic(path: pathlib.Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json_bytes(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=pathlib.Path)
    parser.add_argument("--contract", type=pathlib.Path, default=DEFAULT_CONTRACT_PATH)
    parser.add_argument(
        "--sample-plan", type=pathlib.Path, default=DEFAULT_SAMPLE_PLAN_PATH
    )
    parser.add_argument(
        "--lifecycle-reconciliation", type=pathlib.Path, default=DEFAULT_LIFECYCLE_PATH
    )
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--require-final", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        lifecycle_manifest = _load(
            args.lifecycle_reconciliation,
            "CTA lifecycle reconciliation",
        )
        lifecycle_observation = None
        if lifecycle_manifest.get("verified") is True:
            lifecycle_observation = _load_canonical(
                _repo_path(
                    lifecycle_manifest.get("observation_path"),
                    "lifecycle.observation_path",
                ),
                "CTA lifecycle observation",
            )
        result = evaluate(
            _load(args.snapshot, "CTA snapshot"),
            _load(args.contract, "CTA decision contract"),
            _load(args.sample_plan, "CTA sample plan"),
            lifecycle_manifest,
            lifecycle_observation,
        )
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        CtaEvaluationError,
    ) as exc:
        print(f"VEVO_GROWTHBOOK_CTA_EVALUATION_INVALID:{exc}")
        return 2
    if args.output:
        _write_atomic(args.output, result)
    else:
        print(canonical_json_bytes(result).decode("utf-8"), end="")
    print(
        f"VEVO_GROWTHBOOK_CTA_EVALUATED:verdict={result['verdict']}:devices={result['summary']['included_devices']}:days={result['summary']['full_calendar_days']}:mutation=false"
    )
    if args.require_final and not result["final_decision"]:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
