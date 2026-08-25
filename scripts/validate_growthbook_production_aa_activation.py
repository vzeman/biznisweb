#!/usr/bin/env python3
"""Validate the hard-disabled VEVO Production A/A activation handoff."""

from __future__ import annotations

import copy
import hashlib
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
ACTIVATION_SMOKE_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_aa_activation_smoke_evidence.json"
)
BROWSER_OBSERVATION_PATH = (
    ROOT / "projects" / "vevo" / "growthbook_aa_activation_browser_observation.json"
)
PRODUCTION_RECONCILIATION_EVIDENCE_PATH = (
    ROOT
    / "projects"
    / "vevo"
    / "growthbook_production_reconciliation_deploy_evidence.json"
)


EXPECTED_PRE_ACTIVATION = {
    "schema_version": 9,
    "activation_type": "vevo_growthbook_production_aa",
    "tracking_key": "vevo-sk-aa-001",
    "feature_key": "vevo-sk-aa-assignment",
    "variations": ["control", "variant"],
    "variation_weights": [0.5, 0.5],
    "runbook": "projects/vevo/GROWTHBOOK_PRODUCTION_AA_ACTIVATION_RUNBOOK.md",
    "status": "gtm_live_zero_allocation_verified_growthbook_start_review_pending",
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
            "added": 0,
            "modified": 0,
            "removed": 0,
        },
        "publish_status": "published_zero_allocation",
        "container_version_id": "15",
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
        "status": "gtm_live_zero_allocation_verified_growthbook_start_review_pending",
        "reviewed_at_local_date": "2026-08-24",
        "source_main_commit": "b00b76244c6758a8af4009aff3523966f1ec4b22",
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
            "gtm_live_container_version_id": "15",
            "gtm_workspace_id": "17",
            "gtm_workspace_name": "VEVO GrowthBook Production A/A",
            "gtm_unprocessed_changes": {
                "added": 0,
                "modified": 0,
                "removed": 0,
            },
            "gtm_consent_warning_unconfigured_tag_count": 1,
            "gtm_consent_unconfigured_tag_ids": ["43"],
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
        "gtm_consent_metadata": {
            "observed_unconfigured_tag_ids": ["43", "54", "51", "55", "53"],
            "unrelated_existing_tag_ids": ["43"],
            "growthbook_target_tag_ids": ["54", "51", "55", "53"],
            "required_setting": "no_additional_consent_required",
            "verified_target_tag_ids": ["54", "51", "55", "53"],
            "verified_setting_by_tag_id": {
                "54": "no_additional_consent_required",
                "51": "no_additional_consent_required",
                "55": "no_additional_consent_required",
                "53": "no_additional_consent_required",
            },
            "expected_remaining_unconfigured_tag_ids": ["43"],
            "verified_remaining_unconfigured_tag_ids": ["43"],
            "verified_at_local_datetime": "2026-08-24T15:43:07+02:00",
            "preview_qa": {
                "workspace_id": "17",
                "consent_signal_types": [
                    "ad_storage",
                    "analytics_storage",
                    "ad_user_data",
                    "ad_personalization",
                    "functionality_storage",
                    "personalization_storage",
                    "security_storage",
                ],
                "denied_signal_count": 7,
                "granted_signal_count": 7,
                "original_consent_categories_restored": True,
                "loader_success_on_denied": True,
                "loader_success_on_granted": True,
                "growthbook_sdk_script_count_on_denied": 0,
                "meta_pageview_tag_ids_blocked_on_denied": ["29", "31"],
                "meta_pageview_tag_ids_success_on_granted": ["29", "31"],
                "tag_assistant_console_error_count": 0,
                "unattributed_consent_timing_diagnostic_observed": True,
                "growthbook_client_uses_gtm_consent_api": False,
                "add_to_cart_text_unchanged": True,
                "cta_experiment_class_applied": False,
                "preexisting_cart_item_count": 2,
                "cart_mutated": False,
                "growthbook_live_production_rule_count": 0,
                "growthbook_draft_production_experiment_status": (
                    "draft_not_started"
                ),
            },
            "publish_allowed": False,
        },
        "post_publish_readback": {
            "status": "verified_zero_requests_and_receipts",
            "observed_at_utc": "2026-08-24T14:34:24Z",
            "source_main_commit": "aa1d4a17a24f64808de3ebdd6441ddc375a0f15c",
            "gtm_live_container_version_id": "15",
            "gtm_rollback_container_version_id": "14",
            "public_gtm_http_status": 200,
            "public_gtm_bytes": 499401,
            "public_gtm_sha256": (
                "48816d60331c6df39c15161df4b6b0222b0313382c5b0600fdfd34dfbd11b481"
            ),
            "public_gtm_marker_counts": {
                "vevo_growthbook": 13,
                "client_version": 2,
                "consent_bridge": 3,
                "add_to_cart_bridge": 3,
                "purchase_bridge": 3,
                "growthbook_api_host": 2,
            },
            "production_sdk_key_count": 1,
            "production_sdk_key_recorded": False,
            "growthbook_feature_payload_http_status": 200,
            "growthbook_feature_payload_bytes": 69,
            "growthbook_feature_payload_sha256": (
                "8a85bd5f83d171e3906117b8b6d8fc5d58fea784ad2e1f8fc27745a911537b89"
            ),
            "growthbook_feature_count": 0,
            "target_feature_present": False,
            "target_feature_rule_count": 0,
            "production_assignment_possible": False,
            "zero_collector_request_verified": True,
            "zero_collector_observation": {
                "status": "verified_zero_requests_and_receipts",
                "workflow_run_id": "32741487449",
                "main_commit": "cfe10bd1f53b0b3f41433cd503b543cf242c95e3",
                "artifact_sha256": (
                    "1cbfcbe6673822210cf36f771c1449c4bafa83d0ef2f8c84102285e5296e6a8b"
                ),
                "from_utc": "2026-08-24T14:34:30Z",
                "through_utc": "2026-08-24T14:38:00Z",
                "observed_at_utc": "2026-08-24T14:53:50Z",
                "api_request_count": 0,
                "accepted_receipt_count": 0,
                "runtime": {
                    "image_digest": (
                        "sha256:e9aeee45f457dca5e7cb8f6a80f37763de0bb7f61c96f614d79e222fe4707058"
                    ),
                    "instance_id": "N/A:Fargate",
                    "private_ip": "172.31.21.213",
                    "runtime_path": "/app",
                    "runtime_path_verification": (
                        "immutable_image_prior_localhost_marker"
                    ),
                    "service": "vevo-growthbook-collector-production",
                    "target_health": "healthy",
                    "task_definition": "vevo-growthbook-collector-production:2",
                    "task_id": "a3abdbcdd3914c95bb08f03b83eab5fe",
                },
            },
            "growthbook_start_allowed": True,
        },
        "mutation_scope": {
            "configure_gtm_consent_metadata_for_tags_54_51_55_53": False,
            "publish_gtm_workspace_17": False,
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
            "start_only_growthbook_experiment_exp_19g6mmt5wugpk",
            "publish_only_growthbook_feature_revision_3_with_production_aa_rule",
            "verify_live_100_percent_aa_50_50_sticky_assignment_and_collector_delivery",
            "record_production_aa_activation_readback_in_git",
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
    "next_gate": "review_growthbook_production_aa_start",
}


FINAL_ACTIVATION_READBACK = {
    "status": "verified_running_production_aa",
    "observed_at_utc": "2026-08-25T05:43:54Z",
    "browser_observation_path": (
        "projects/vevo/growthbook_aa_activation_browser_observation.json"
    ),
    "browser_observation_sha256": (
        "451e6489df351cc2318751a9d4bd727d107d8aaf9763e406c595fc5824ec8705"
    ),
    "smoke_evidence_path": (
        "projects/vevo/growthbook_aa_activation_smoke_evidence.json"
    ),
    "smoke_evidence_sha256": (
        "c21d7418656ad0841851a8afbc642a6ea39328e2151e6dc647ce8c59c06c1823"
    ),
    "workflow_run_id": "32815955896",
    "main_commit": "1965091059e5a35518265aafd282db842f8ea5d3",
    "growthbook": {
        "experiment_status": "running",
        "environment": "production_only",
        "traffic_percent": 100,
        "variation_weights": [0.5, 0.5],
        "feature_revision": 3,
        "feature_revision_status": "live",
        "statistics_engine": "bayesian_default",
        "cuped_enabled": False,
        "post_stratification_enabled": False,
        "activation_metric": None,
        "goal_metric_count": 1,
        "secondary_metric_count": 6,
        "guardrail_metric_count": 1,
    },
    "tag_assistant": {
        "connected": True,
        "container_id": "GTM-5ZB5LFGB",
        "detected_google_tag_count": 4,
        "production_loader_fired": True,
        "console_error_count": 0,
    },
    "collector": {
        "api_request_count": 8,
        "accepted_receipt_count": 8,
        "raw_event_count": 8,
        "target_exposure_count": 4,
        "product_exposure_count": 3,
        "repeat_exposed_device_count": 1,
        "sticky_consistent_repeat_device_count": 1,
        "sticky_inconsistent_device_count": 0,
        "observed_variations": ["variant"],
    },
    "commerce": {
        "cta_text_unchanged": True,
        "cta_experiment_class_applied": False,
        "preexisting_cart_item_count": 2,
        "cart_checkout_or_order_mutated": False,
    },
}


PRODUCTION_RECONCILIATION_BINDING = {
    "status": "deployed_one_shot_verified_schedule_enabled",
    "evidence_path": (
        "projects/vevo/growthbook_production_reconciliation_deploy_evidence.json"
    ),
    "evidence_sha256": (
        "21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb"
    ),
    "workflow_run_id": "32821210244",
    "workflow_run_url": (
        "https://github.com/vzeman/biznisweb/actions/runs/32821210244"
    ),
    "main_commit": "cf92eb0e007fb9a9163068a2735e5becc0327f03",
    "first_scheduled_run_due_local": "2026-08-26T03:45:00+02:00",
    "bootstrap_counts_are_not_population_acceptance": True,
}


EXPECTED_PRODUCTION_RECONCILIATION_EVIDENCE = {
    "aws": {
        "account_id": "919341186960",
        "collector_stack": "vevo-growthbook-production",
        "reconciliation_stack": "vevo-growthbook-reconciliation-production",
        "region": "eu-central-1",
    },
    "boundaries": {
        "biznisweb_mutated": False,
        "collector_mutated": False,
        "growthbook_experiment_mutated": False,
        "gtm_mutated": False,
        "meta_ads_mutated": False,
        "price_product_cart_checkout_or_order_mutated": False,
        "raw_event_delete_performed": False,
    },
    "contains_cloudwatch_messages": False,
    "contains_credentials": False,
    "contains_event_device_customer_or_order_ids": False,
    "contains_raw_aws_payloads": False,
    "environment": "production",
    "evidence_type": "vevo_growthbook_reconciliation_deploy",
    "host_gate": {
        "instance_id": "N/A:Fargate",
        "localhost_health_verified": True,
        "localhost_marker_verified": True,
        "private_ip": "172.31.39.76",
        "runtime_path": "/app",
        "service": "vevo-growthbook-reconcile-production",
        "task_id": "17d2ea85e2304d2ca0f16ef3ad32913d",
    },
    "observed_at_utc": "2026-08-25T07:28:18Z",
    "reconciliation": {
        "curated_fact_publish_verified": True,
        "device_facts": 0,
        "image_digest": (
            "sha256:51d70f4976083f86a0d7c5e542c21d93e5bbeff3d75d2af31f620b42df1a1b92"
        ),
        "one_shot_private_ip": "172.31.38.184",
        "one_shot_task_id": "496df38886674a8885866016e82c5ae6",
        "performance_facts": 0,
        "quality_reports": 2,
        "raw_events": 0,
        "task_definition": (
            "arn:aws:ecs:eu-central-1:919341186960:task-definition/"
            "vevo-growthbook-reconcile-production:3"
        ),
    },
    "reporting_policy": {
        "arn": (
            "arn:aws:iam::919341186960:policy/"
            "vevo-growthbook-reporting-production"
        ),
        "attached_by_run": False,
        "attachment_readback_verified": True,
        "document_exactly_verified": True,
        "task_role": (
            "arn:aws:iam::919341186960:role/BiznisWebReportingTaskRole-vevo"
        ),
    },
    "schedule": {
        "dlq": "vevo-growthbook-reconcile-production-dlq",
        "dlq_alarm": "vevo-growthbook-reconcile-production-dlq",
        "enabled": True,
        "expression": "cron(45 3 * * ? *)",
        "failure_alarm": "vevo-growthbook-reconcile-production-failure",
        "missing_success_alarm": (
            "vevo-growthbook-reconcile-production-missing-success"
        ),
        "name": "vevo-growthbook-reconcile-production",
        "timezone": "Europe/Bratislava",
    },
    "schema_version": 1,
    "source_main_commit": "cf92eb0e007fb9a9163068a2735e5becc0327f03",
    "source_run_id": "32821210244",
    "source_runtime": {
        "instance_id": "N/A:Fargate",
        "service": "vevo-daily-report-email",
        "source_schedule_unchanged": True,
        "task_definition": (
            "arn:aws:ecs:eu-central-1:919341186960:task-definition/"
            "vevo-reporting-daily:33"
        ),
    },
}


def _running_activation() -> dict[str, Any]:
    activation = copy.deepcopy(EXPECTED_PRE_ACTIVATION)
    activation["schema_version"] = 11
    activation["status"] = "production_aa_running_activation_verified"
    activation["growthbook"].update(
        {
            "production_rule_publish_status": "live_published",
            "status": "running",
            "allocation_percent": 100,
        }
    )
    activation["gtm"]["publish_status"] = "published_production_loader_active"
    activation["activation_readback"] = copy.deepcopy(FINAL_ACTIVATION_READBACK)
    activation["production_reconciliation"] = copy.deepcopy(
        PRODUCTION_RECONCILIATION_BINDING
    )
    activation["traffic"].update(
        {
            "activation_allowed": True,
            "production_allocation_percent": 100,
            "active_production_experiments": ["vevo-sk-aa-001"],
        }
    )
    activation["next_gate"] = "collect_7_full_days_and_1000_eligible_devices"
    return activation


EXPECTED_ACTIVATION = _running_activation()


def _collector_verified_activation() -> dict[str, Any]:
    """Return the immutable pre-UI boundary used by the collector recorder."""

    activation = copy.deepcopy(EXPECTED_PRE_ACTIVATION)
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


def _validate_activation_evidence(activation: Mapping[str, Any]) -> None:
    readback = activation.get("activation_readback") or {}
    smoke_raw = ACTIVATION_SMOKE_PATH.read_bytes()
    browser_raw = BROWSER_OBSERVATION_PATH.read_bytes()
    if hashlib.sha256(smoke_raw).hexdigest() != readback.get(
        "smoke_evidence_sha256"
    ):
        raise AssertionError("Production A/A smoke evidence SHA-256 drift")
    if hashlib.sha256(browser_raw).hexdigest() != readback.get(
        "browser_observation_sha256"
    ):
        raise AssertionError("Production A/A browser observation SHA-256 drift")

    smoke = json.loads(smoke_raw)
    browser = json.loads(browser_raw)
    if (
        smoke.get("source_run_id") != readback.get("workflow_run_id")
        or smoke.get("source_main_commit") != readback.get("main_commit")
        or smoke.get("browser_observation_sha256")
        != readback.get("browser_observation_sha256")
    ):
        raise AssertionError("Production A/A activation evidence provenance drift")
    if smoke.get("collector") != {
        "api_request_count": 8,
        "accepted_receipt_count": 8,
        "raw_event_count": 8,
    }:
        raise AssertionError("Production A/A collector smoke counts drift")
    assignment = smoke.get("assignment") or {}
    expected_assignment = {
        "target_exposure_count": 4,
        "product_exposure_count": 3,
        "repeat_exposed_device_count": 1,
        "sticky_consistent_repeat_device_count": 1,
        "sticky_inconsistent_device_count": 0,
        "observed_variations": ["variant"],
    }
    if any(assignment.get(key) != value for key, value in expected_assignment.items()):
        raise AssertionError("Production A/A sticky assignment evidence drift")
    if (
        smoke.get("accepted_collector_delivery_verified") is not True
        or smoke.get("sticky_assignment_verified") is not True
        or smoke.get("cta_experiment_started") is not False
        or smoke.get("cart_checkout_or_order_mutated") is not False
    ):
        raise AssertionError("Production A/A smoke safety state drift")
    for boundary in (
        "contains_raw_aws_payloads",
        "contains_cloudwatch_messages",
        "contains_event_or_device_ids",
        "contains_customer_or_order_data",
        "aws_mutation_performed",
        "growthbook_mutation_performed",
        "gtm_mutation_performed",
        "meta_ads_mutation_performed",
        "biznisweb_mutation_performed",
    ):
        if smoke.get(boundary) is not False:
            raise AssertionError(f"Production A/A smoke boundary drift: {boundary}")
    if (
        browser.get("growthbook", {}).get("status") != "running"
        or browser.get("growthbook", {}).get("feature_revision") != 3
        or browser.get("growthbook", {}).get("traffic_percent") != 100
        or browser.get("growthbook", {}).get("cta_experiment_status") != "draft"
        or browser.get("browser_qa", {}).get("console_error_count") != 0
        or browser.get("browser_qa", {}).get("cart_mutated") is not False
    ):
        raise AssertionError("Production A/A browser readback drift")


def _validate_production_reconciliation_evidence(
    activation: Mapping[str, Any],
) -> None:
    binding = activation.get("production_reconciliation") or {}
    raw = PRODUCTION_RECONCILIATION_EVIDENCE_PATH.read_bytes()
    if hashlib.sha256(raw).hexdigest() != binding.get("evidence_sha256"):
        raise AssertionError("Production reconciliation evidence SHA-256 drift")
    evidence = json.loads(raw)
    if evidence != EXPECTED_PRODUCTION_RECONCILIATION_EVIDENCE:
        raise AssertionError("Production reconciliation evidence content drift")
    if (
        evidence.get("source_run_id") != binding.get("workflow_run_id")
        or evidence.get("source_main_commit") != binding.get("main_commit")
    ):
        raise AssertionError("Production reconciliation evidence provenance drift")


def validate_activation_handoff(
    activation: Mapping[str, Any],
    workspace: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> None:
    if dict(activation) != EXPECTED_ACTIVATION:
        raise AssertionError(
            "Production A/A activation must match the reviewed running evidence gate"
        )
    _validate_activation_evidence(activation)
    _validate_production_reconciliation_evidence(activation)

    if workspace.get("workspace", {}).get("production_allocation_percent") != 100:
        raise AssertionError("Production A/A activation requires full workspace allocation")
    if workspace.get("decision_gates", {}).get("production_activation_allowed") is not True:
        raise AssertionError("Production A/A activation decision gate must be true")
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
        or aa.get("production_allocation_percent") != 100
        or aa.get("status") != "running_production_aa_only"
        or aa.get("feature_rule_environments") != ["production"]
        or aa.get("feature_rule_revision") != 3
    ):
        raise AssertionError("Production A/A activation experiment contract drift")
    if (
        cta.get("status") != "unstarted_draft"
        or cta.get("feature_rule_status") != "no_live_rules"
        or cta.get("production_allocation_percent") != 0
    ):
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
