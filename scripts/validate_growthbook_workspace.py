#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import re
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKSPACE_PATH = ROOT / "projects" / "vevo" / "growthbook_workspace.json"
REPORTING_PATH = ROOT / "projects" / "vevo" / "growthbook_reporting.json"
REGISTRY_PATH = ROOT / "growthbook_collector" / "experiments.json"

EXPECTED_FACT_TABLES = {
    "vevo_device_outcomes_v1": "projects/vevo/growthbook_sql/device_outcomes.sql",
    "vevo_performance_vitals_v1": "projects/vevo/growthbook_sql/performance_vitals.sql",
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


def _validate_sql(name: str, sql: str, *, table: str, experiment_filter: bool) -> None:
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


def validate() -> None:
    workspace = _load(WORKSPACE_PATH)
    reporting = _load(REPORTING_PATH)
    registry = _load(REGISTRY_PATH)

    if (
        workspace.get("schema_version") != 1
        or workspace.get("state") != "preview_assignment_verified"
    ):
        raise AssertionError("GrowthBook workspace must remain a connected Preview-only v1 contract")
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
    if workspace_config.get("production_allocation_percent") != 0:
        raise AssertionError("GrowthBook Production allocation must remain 0%")
    if (
        workspace_config.get("actual_region") is not None
        or workspace_config.get("region_status") != "not_exposed_in_current_workspace_ui"
    ):
        raise AssertionError("GrowthBook region must remain explicitly unknown until exposed by the UI")

    sdk_connection = workspace.get("sdk_connection", {})
    expected_sdk = {
        "name": "VEVO SK Web Preview",
        "status": "created_not_connected",
        "environment": "staging",
        "project": "VEVO SK Web",
        "language": "javascript",
        "sdk_version": "1.7.0",
        "api_host": "https://cdn.growthbook.io",
        "client_key_status": "created_not_committed_pending_runtime_config",
        "include_draft_experiment_rules": True,
        "include_feature_rule_ids": True,
        "hide_feature_names": True,
        "visual_editor_enabled": False,
        "url_redirects_enabled": False,
        "production_connection_created": False,
    }
    if sdk_connection != expected_sdk:
        raise AssertionError("GrowthBook Preview SDK connection read-back changed")

    feature_flags = workspace.get("feature_flags", [])
    feature_map = {row.get("key"): row for row in feature_flags if isinstance(row, dict)}
    if set(feature_map) != set(EXPECTED_FEATURES.values()):
        raise AssertionError("GrowthBook feature-flag set changed")
    for experiment_id, feature_key in EXPECTED_FEATURES.items():
        feature = feature_map[feature_key]
        if (
            feature.get("type") != "string"
            or feature.get("default_value") != "control"
            or feature.get("staging_enabled") is not True
            or feature.get("production_enabled") is not False
            or feature.get("live_rule_count") != 0
            or feature.get("draft_rule_experiment") != experiment_id
        ):
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
    assignment_path = athena.get("assignment_query")
    assignment_sql = _read_repo_path(assignment_path)
    _validate_sql(
        assignment_path,
        assignment_sql,
        table="experiment_device_facts",
        experiment_filter=False,
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
        _validate_sql(expected_path, sql, table=table, experiment_filter=True)

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
            experiment.get("status") != "draft"
            or experiment.get("assignment_attribute") != "id"
            or experiment.get("traffic_percent") != 100
            or experiment.get("feature_rule_status") != "draft"
            or experiment.get("feature_rule_environments") != ["staging"]
        ):
            raise AssertionError(f"GrowthBook experiment must remain staging-only draft: {experiment_id}")
        if experiment.get("production_allocation_percent") != 0:
            raise AssertionError(f"GrowthBook Production allocation must remain zero: {experiment_id}")
        variations = experiment.get("variations", [])
        if variations != preview_registry.get(experiment_id, {}).get("variations"):
            raise AssertionError(f"GrowthBook/collector variation order differs for {experiment_id}")
        expected_weights = reporting_weights.get(experiment_id, {})
        if variations != list(expected_weights) or experiment.get("variation_weights") != list(
            expected_weights.values()
        ):
            raise AssertionError(f"GrowthBook/reporting weights differ for {experiment_id}")
    if experiment_map["vevo-sk-aa-001"].get("winner_calls_allowed") is not False:
        raise AssertionError("A/A must never be used for a winner call")

    gates = workspace.get("decision_gates", {})
    if gates.get("maturity_checkpoint_days") != reporting.get("maturity_checkpoint_days"):
        raise AssertionError("GrowthBook/reporting maturity gates differ")
    if gates.get("price_tests_allowed") is not False:
        raise AssertionError("price testing must remain disabled")
    if gates.get("production_activation_allowed") is not False:
        raise AssertionError("Production activation must remain blocked")


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
