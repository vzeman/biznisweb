#!/usr/bin/env python3
"""Validate the VEVO Meta Ads -> GrowthBook/reporting CTA release contract.

The validator is deliberately offline. It proves the checked-in mapping and
release boundaries only; it has no Meta, GrowthBook, AWS, GTM, BiznisWeb,
browser, traffic, price, cart, checkout, or order mutation client.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_meta_reporting_contract.json"
)
EXPECTED_CANONICAL_PARAMETERS = (
    "utm_source=meta&utm_medium=paid_social&utm_id={{campaign.id}}"
    "&utm_campaign={{campaign.name}}&utm_content={{ad.id}}"
    "&meta_adset_id={{adset.id}}&meta_placement={{placement}}"
)
EXPECTED_MAPPINGS = [
    {
        "url_parameter": "utm_id",
        "meta_macro": "{{campaign.id}}",
        "collector_field": "meta_campaign_id",
        "growthbook_assignment_column": "meta_campaign_id",
        "reporting_fact_column": "meta_campaign_id",
    },
    {
        "url_parameter": "meta_adset_id",
        "meta_macro": "{{adset.id}}",
        "collector_field": "meta_adset_id",
        "growthbook_assignment_column": "meta_adset_id",
        "reporting_fact_column": "meta_adset_id",
    },
    {
        "url_parameter": "utm_content",
        "meta_macro": "{{ad.id}}",
        "collector_field": "meta_ad_id",
        "growthbook_assignment_column": "meta_ad_id",
        "reporting_fact_column": "meta_ad_id",
    },
    {
        "url_parameter": "meta_placement",
        "meta_macro": "{{placement}}",
        "collector_field": "meta_placement",
        "growthbook_assignment_column": "meta_placement",
        "reporting_fact_column": "meta_placement",
    },
]
EXPECTED_FORBIDDEN = [
    "fbclid",
    "_fbp",
    "_fbc",
    "email",
    "phone",
    "name",
    "address",
    "customer_id",
    "account_id",
    "ip_address",
]
EXPECTED_FORBIDDEN_CONFIGURED_CLICK_IDS = ["fbclid", "_fbp", "_fbc"]


class MetaReportingContractError(ValueError):
    """Raised when the versioned Meta/reporting release contract drifts."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise MetaReportingContractError(message)


def _exact_object(value: Any, keys: set[str], field: str) -> Mapping[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    _require(set(value) == keys, f"{field} fields drift")
    return value


def _repo_path(root: Path, relative: Any, field: str) -> Path:
    _require(isinstance(relative, str) and relative, f"{field} path is invalid")
    candidate = (root / relative).resolve()
    _require(candidate.is_relative_to(root.resolve()), f"{field} escapes repository")
    _require(candidate.is_file(), f"{field} file is missing")
    return candidate


def _read(root: Path, relative: Any, field: str) -> str:
    return _repo_path(root, relative, field).read_text(encoding="utf-8")


def validate_contract(contract: Mapping[str, Any], *, root: Path = ROOT) -> None:
    top = _exact_object(
        contract,
        {
            "schema_version",
            "contract_type",
            "experiment_id",
            "status",
            "runbook",
            "traffic_assignment",
            "meta_url_parameter_contract",
            "verified_data_chain",
            "analysis_policy",
            "release_boundaries",
        },
        "Meta reporting contract",
    )
    _require(top["schema_version"] == 1, "Meta reporting schema drift")
    _require(
        top["contract_type"] == "vevo_growthbook_meta_reporting",
        "Meta reporting type drift",
    )
    _require(
        top["experiment_id"] == "vevo-sk-product-cta-color-001",
        "Meta reporting experiment drift",
    )
    _require(
        top["status"] == "verified_offline_activation_gate",
        "Meta reporting gate is not verified",
    )
    _require(
        top["runbook"] == "projects/vevo/META_ADS_GROWTHBOOK_PARAMETER_RUNBOOK.md",
        "Meta reporting runbook drift",
    )

    assignment = _exact_object(
        top["traffic_assignment"],
        {
            "owner",
            "meta_ab_split_for_same_hypothesis_allowed",
            "first_cta_routing_mode",
            "canonical_destination_origin",
            "arm_specific_destination_or_query_parameter_allowed",
            "future_landing_page_redirect_requires_separate_review",
        },
        "traffic_assignment",
    )
    _require(assignment["owner"] == "growthbook", "GrowthBook must own randomization")
    _require(
        assignment["meta_ab_split_for_same_hypothesis_allowed"] is False,
        "Meta A/B split must remain disabled for the same hypothesis",
    )
    _require(
        assignment["first_cta_routing_mode"]
        == "single_canonical_destination_with_on_site_growthbook_assignment",
        "CTA routing mode drift",
    )
    _require(
        assignment["canonical_destination_origin"] == "https://www.vevo.sk",
        "CTA canonical destination drift",
    )
    _require(
        assignment["arm_specific_destination_or_query_parameter_allowed"] is False,
        "Meta must not select a CTA arm",
    )
    _require(
        assignment["future_landing_page_redirect_requires_separate_review"] is True,
        "future landing-page review boundary is missing",
    )

    parameters = _exact_object(
        top["meta_url_parameter_contract"],
        {
            "canonical_value",
            "diagnostic_label",
            "required_source_medium",
            "stable_dimension_mappings",
            "forbidden_persisted_identifiers",
            "forbidden_configured_click_identifiers",
            "existing_live_ads_policy",
            "new_or_otherwise_edited_ads_policy",
        },
        "meta_url_parameter_contract",
    )
    _require(
        parameters["canonical_value"] == EXPECTED_CANONICAL_PARAMETERS,
        "canonical Meta URL parameters drift",
    )
    _require(
        parameters["diagnostic_label"] == {"utm_campaign": "{{campaign.name}}"},
        "Meta diagnostic label drift",
    )
    _require(
        parameters["required_source_medium"]
        == {"utm_source": "meta", "utm_medium": "paid_social"},
        "Meta source/medium drift",
    )
    _require(
        parameters["stable_dimension_mappings"] == EXPECTED_MAPPINGS,
        "Meta stable dimension mapping drift",
    )
    _require(
        parameters["forbidden_persisted_identifiers"]
        == EXPECTED_FORBIDDEN,
        "Meta forbidden persisted identifier boundary drift",
    )
    _require(
        parameters["forbidden_configured_click_identifiers"]
        == EXPECTED_FORBIDDEN_CONFIGURED_CLICK_IDS,
        "Meta forbidden configured click identifier boundary drift",
    )
    _require(
        not any(
            marker in parameters["canonical_value"].lower()
            for marker in ("fbclid", "_fbp", "_fbc", "variant", "variation")
        ),
        "canonical Meta parameters contain an identity or arm selector",
    )
    _require(
        parameters["existing_live_ads_policy"] == "do_not_edit_only_for_tracking"
        and parameters["new_or_otherwise_edited_ads_policy"]
        == "apply_canonical_value_before_publish",
        "Meta ad edit boundary drift",
    )

    chain = _exact_object(
        top["verified_data_chain"],
        {
            "storefront_source",
            "collector_source",
            "reporting_builder",
            "growthbook_assignment_query",
            "growthbook_outcome_query",
            "cta_final_snapshot_query",
            "meta_audit_script",
            "production_clone_observation",
        },
        "verified_data_chain",
    )
    expected_paths = {
        "storefront_source": "storefront/vevo-growthbook/vevo-growthbook.js",
        "collector_source": "growthbook_collector/handler.py",
        "reporting_builder": "reporting_core/experiments.py",
        "growthbook_assignment_query": "projects/vevo/growthbook_sql/assignment.sql",
        "growthbook_outcome_query": (
            "projects/vevo/growthbook_sql/device_outcomes_production.sql"
        ),
        "cta_final_snapshot_query": (
            "projects/vevo/growthbook_sql/cta_final_snapshot_production.sql"
        ),
        "meta_audit_script": "scripts/audit_vevo_meta_dimensions.py",
    }
    for field, expected in expected_paths.items():
        _require(chain[field] == expected, f"{field} path drift")

    clone = _exact_object(
        chain["production_clone_observation"],
        {
            "path",
            "sha256",
            "data_source_id",
            "assignment_query_sha256",
            "assignment_query_test_passed",
            "configuration_readback_match",
        },
        "production_clone_observation",
    )
    clone_path = _repo_path(root, clone["path"], "production clone observation")
    clone_raw = clone_path.read_bytes()
    _require(
        hashlib.sha256(clone_raw).hexdigest() == clone["sha256"],
        "Production clone observation SHA-256 drift",
    )
    try:
        clone_observation = json.loads(clone_raw)
    except json.JSONDecodeError as exc:
        raise MetaReportingContractError("Production clone observation is invalid") from exc
    data_source = clone_observation.get("production_data_source", {})
    assignment_path = _repo_path(
        root,
        chain["growthbook_assignment_query"],
        "GrowthBook assignment query",
    )
    assignment_sha256 = hashlib.sha256(assignment_path.read_bytes()).hexdigest()
    _require(
        clone["data_source_id"] == data_source.get("id") == "ds_19g6mmt5stlp6",
        "Production GrowthBook data source readback drift",
    )
    _require(
        clone["assignment_query_sha256"]
        == data_source.get("assignment_query_sha256")
        == assignment_sha256,
        "Production assignment query hash/readback drift",
    )
    _require(
        clone["assignment_query_test_passed"] is True
        and data_source.get("assignment_query_test_passed") is True
        and clone["configuration_readback_match"] is True,
        "Production assignment query was not query-tested and read back",
    )

    source_texts = {
        field: _read(root, relative, field) for field, relative in expected_paths.items()
    }
    for mapping in EXPECTED_MAPPINGS:
        url_parameter = mapping["url_parameter"]
        collector_field = mapping["collector_field"]
        assignment_column = mapping["growthbook_assignment_column"]
        reporting_column = mapping["reporting_fact_column"]
        _require(
            f'"{collector_field}"' in source_texts["collector_source"],
            f"collector mapping is missing {collector_field}",
        )
        _require(
            f'params.get("{url_parameter}")' in source_texts["storefront_source"]
            and f"{collector_field}:" in source_texts["storefront_source"],
            f"storefront mapping is missing {url_parameter} -> {collector_field}",
        )
        _require(
            assignment_column in source_texts["growthbook_assignment_query"],
            f"GrowthBook assignment query is missing {assignment_column}",
        )
        _require(
            reporting_column in source_texts["growthbook_outcome_query"]
            and f'"{reporting_column}"' in source_texts["reporting_builder"],
            f"reporting chain is missing {reporting_column}",
        )
        _require(
            reporting_column not in source_texts["cta_final_snapshot_query"],
            f"CTA primary snapshot must remain unsegmented: {reporting_column}",
        )
    for marker in ("utm_id", "utm_content", "meta_adset_id", "meta_placement"):
        _require(
            f'"{marker}"' in source_texts["meta_audit_script"],
            f"Meta audit is missing {marker}",
        )

    runbook = _read(root, top["runbook"], "Meta runbook")
    _require(
        EXPECTED_CANONICAL_PARAMETERS in runbook
        and "GrowthBook remains the sole 50/50 assignment system." in runbook
        and "dimension coverage is diagnostic" in runbook.lower(),
        "Meta runbook no longer preserves routing and analysis boundaries",
    )

    policy = _exact_object(
        top["analysis_policy"],
        {
            "primary_decision_population",
            "meta_dimensions_role",
            "dimension_result_may_declare_winner",
            "dimension_result_may_replace_primary_decision",
            "minimum_complete_stable_meta_dimension_exposures_in_aa",
            "cta_final_decision_remains_unsegmented",
        },
        "analysis_policy",
    )
    _require(
        policy
        == {
            "primary_decision_population": "all_eligible_traffic",
            "meta_dimensions_role": "diagnostic_only",
            "dimension_result_may_declare_winner": False,
            "dimension_result_may_replace_primary_decision": False,
            "minimum_complete_stable_meta_dimension_exposures_in_aa": 1,
            "cta_final_decision_remains_unsegmented": True,
        },
        "Meta diagnostic analysis policy drift",
    )

    boundaries = _exact_object(
        top["release_boundaries"],
        {
            "automatic_meta_ads_mutation_allowed",
            "automatic_growthbook_mutation_allowed",
            "automatic_gtm_mutation_allowed",
            "automatic_biznisweb_mutation_allowed",
            "automatic_traffic_routing_mutation_allowed",
            "price_product_cart_checkout_order_mutation_allowed",
        },
        "release_boundaries",
    )
    _require(not any(boundaries.values()), "Meta/reporting mutation boundary is open")


def load_contract(path: Path = DEFAULT_CONTRACT_PATH) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MetaReportingContractError("unable to load Meta reporting contract") from exc
    _require(isinstance(value, dict), "Meta reporting contract must be an object")
    return value


def main() -> int:
    try:
        validate_contract(load_contract())
    except (MetaReportingContractError, OSError, KeyError) as exc:
        print(f"validate_growthbook_meta_reporting_contract.py: FAIL: {exc}")
        return 2
    print(
        "VEVO_META_GROWTHBOOK_REPORTING_CONTRACT_OK:"
        "randomization=growthbook:meta_dimensions=diagnostic:automatic_mutation=false"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
