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
        or workspace.get("state")
        != "preview_outcome_metrics_query_verified_pro_quantiles_blocked"
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

    gtm_preview = workspace.get("gtm_preview_workspace", {})
    expected_gtm_identity = {
        "account_id": "6254499282",
        "container_id": "198135331",
        "public_container_id": "GTM-5ZB5LFGB",
        "workspace_id": "16",
        "workspace_name": "VEVO GrowthBook Preview",
        "status": "unpublished_draft_preview_feature_payload_blocked",
        "created_verified_date": "2026-08-21",
        "artifact_sha256": "e4dab7ad37432c255c9552eff953bfb0c80c48035db06b814e6d5a58af29532f",
        "runtime_secret_material_committed": False,
        "temporary_artifact_status": "deleted_after_ui_paste_and_readback",
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
        "analytics_only_feature_request_result": "blocked_by_comet_adblock",
        "analytics_only_exposure_delivery_result": "not_attempted_feature_payload_blocked",
        "collector_request_observed": False,
        "feature_payload_blocker_host": "cdn.growthbook.io",
        "official_troubleshooting_reference": (
            "https://support.google.com/tagmanager/answer/10039345"
        ),
        "comet_adblock_reference": (
            "https://www.perplexity.ai/help-center/comet/en/articles/11734702-adblock"
        ),
        "blocker": "comet_adblock_blocks_growthbook_feature_payload",
        "next_gate": (
            "user_allowlists_https_cdn_growthbook_io_then_repeat_analytics_only_preview_"
            "and_verify_exposure_delivery"
        ),
    }
    if tag_assistant != expected_tag_assistant:
        raise AssertionError("GTM Tag Assistant Preview blocker state drift")

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
