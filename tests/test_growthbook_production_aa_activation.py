from __future__ import annotations

import copy
import json
import pathlib
import textwrap
import unittest

import yaml

from scripts import validate_growthbook_production_aa_activation as validator
from scripts.validate_growthbook_changeset import validate as validate_change_set

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    ROOT
    / ".github"
    / "workflows"
    / "deploy-vevo-growthbook-production-aa-collector.yml"
)
WORKFLOW = WORKFLOW_PATH.read_text(encoding="utf-8")


class GrowthBookProductionAaActivationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.activation = json.loads(validator.ACTIVATION_PATH.read_text(encoding="utf-8"))
        self.workspace = json.loads(validator.WORKSPACE_PATH.read_text(encoding="utf-8"))
        self.registry = json.loads(validator.REGISTRY_PATH.read_text(encoding="utf-8"))

    def test_checked_in_handoff_is_published_at_zero_allocation(self) -> None:
        validator.validate_activation_handoff(
            self.activation,
            self.workspace,
            self.registry,
        )

    def test_growthbook_draft_and_live_gtm_evidence_is_exact(self) -> None:
        self.assertEqual("draft_not_started", self.activation["growthbook"]["status"])
        self.assertEqual(
            "draft_not_published",
            self.activation["growthbook"]["production_rule_publish_status"],
        )
        self.assertEqual(
            "published_zero_allocation", self.activation["gtm"]["publish_status"]
        )
        self.assertEqual(
            {"added": 0, "modified": 0, "removed": 0},
            self.activation["gtm"]["unprocessed_changes"],
        )
        self.assertEqual("15", self.activation["gtm"]["container_version_id"])
        self.assertTrue(self.activation["gtm"]["setup_tag_sequencing_verified"])
        self.assertEqual(0, self.activation["traffic"]["production_allocation_percent"])
        self.assertFalse(self.activation["traffic"]["activation_allowed"])

    def test_tag_assistant_zero_traffic_qa_is_verified_but_activation_is_closed(self) -> None:
        qa = self.activation["tag_assistant_qa"]
        self.assertEqual("zero_traffic_qa_verified", qa["status"])
        self.assertTrue(qa["desktop_consent_cycle_observed"])
        self.assertTrue(qa["original_consent_categories_restored"])
        self.assertEqual("connected", qa["sdk_connection_status_after_grant"])
        self.assertEqual(0, qa["console_error_count"])
        self.assertFalse(qa["cta_class_applied"])
        self.assertFalse(qa["cart_mutated"])
        for verified in (
            "mobile_viewport_verified",
            "zero_assignment_verified",
            "owned_storage_cleanup_verified",
            "ga4_meta_consent_behavior_verified",
        ):
            self.assertTrue(qa[verified])
        self.assertTrue(qa["zero_collector_request_verified"])
        observation = qa["zero_collector_observation"]
        self.assertEqual("32692688625", observation["workflow_run_id"])
        self.assertEqual(0, observation["api_request_count"])
        self.assertEqual(0, observation["accepted_receipt_count"])
        self.assertEqual(
            "43140aa030225ac927fd6ddd92904fe8d730230174afe7525371c235accfb745",
            observation["artifact_sha256"],
        )
        self.assertFalse(self.activation["traffic"]["activation_allowed"])
        self.assertEqual(0, self.activation["traffic"]["production_allocation_percent"])
        self.assertEqual(
            "published_zero_allocation", self.activation["gtm"]["publish_status"]
        )
        self.assertEqual("draft_not_started", self.activation["growthbook"]["status"])
        self.assertEqual(
            "review_growthbook_production_aa_start", self.activation["next_gate"]
        )

    def test_controlled_activation_preflight_is_exact_and_narrow(self) -> None:
        preflight = self.activation["activation_preflight"]
        live = preflight["live_readback"]
        self.assertEqual(
            "gtm_live_zero_allocation_verified_growthbook_start_review_pending",
            preflight["status"],
        )
        self.assertEqual(2, live["feature_live_revision"])
        self.assertFalse(live["feature_live_production_enabled"])
        self.assertTrue(live["feature_live_staging_enabled"])
        self.assertEqual(
            {"production": 0, "staging": 1},
            live["feature_live_rule_count_by_environment"],
        )
        self.assertEqual(3, live["draft_feature_revision"])
        self.assertEqual(
            "production_only", live["production_experiment_environment"]
        )
        self.assertEqual(100, live["production_experiment_traffic_percent"])
        self.assertEqual(
            [0.5, 0.5], live["production_experiment_variation_weights"]
        )
        self.assertEqual("15", live["gtm_live_container_version_id"])
        self.assertEqual(
            self.activation["gtm"]["unprocessed_changes"],
            live["gtm_unprocessed_changes"],
        )
        self.assertEqual(1, live["gtm_consent_warning_unconfigured_tag_count"])
        self.assertEqual(["43"], live["gtm_consent_unconfigured_tag_ids"])

        consent = preflight["gtm_consent_metadata"]
        self.assertEqual(
            ["43", "54", "51", "55", "53"],
            consent["observed_unconfigured_tag_ids"],
        )
        self.assertEqual(["43"], consent["unrelated_existing_tag_ids"])
        self.assertEqual(
            ["54", "51", "55", "53"], consent["growthbook_target_tag_ids"]
        )
        self.assertEqual(
            "no_additional_consent_required", consent["required_setting"]
        )
        self.assertEqual(
            ["54", "51", "55", "53"], consent["verified_target_tag_ids"]
        )
        self.assertEqual(
            {
                "54": "no_additional_consent_required",
                "51": "no_additional_consent_required",
                "55": "no_additional_consent_required",
                "53": "no_additional_consent_required",
            },
            consent["verified_setting_by_tag_id"],
        )
        self.assertEqual(
            ["43"], consent["expected_remaining_unconfigured_tag_ids"]
        )
        self.assertEqual(
            ["43"], consent["verified_remaining_unconfigured_tag_ids"]
        )
        qa = consent["preview_qa"]
        self.assertEqual(7, qa["denied_signal_count"])
        self.assertEqual(7, qa["granted_signal_count"])
        self.assertTrue(qa["original_consent_categories_restored"])
        self.assertTrue(qa["loader_success_on_denied"])
        self.assertTrue(qa["loader_success_on_granted"])
        self.assertEqual(0, qa["growthbook_sdk_script_count_on_denied"])
        self.assertEqual(["29", "31"], qa["meta_pageview_tag_ids_blocked_on_denied"])
        self.assertEqual(["29", "31"], qa["meta_pageview_tag_ids_success_on_granted"])
        self.assertEqual(0, qa["tag_assistant_console_error_count"])
        self.assertTrue(qa["unattributed_consent_timing_diagnostic_observed"])
        self.assertFalse(qa["growthbook_client_uses_gtm_consent_api"])
        self.assertFalse(qa["cta_experiment_class_applied"])
        self.assertFalse(qa["cart_mutated"])
        self.assertFalse(consent["publish_allowed"])

        post_publish = preflight["post_publish_readback"]
        self.assertEqual("verified_zero_requests_and_receipts", post_publish["status"])
        self.assertEqual("15", post_publish["gtm_live_container_version_id"])
        self.assertEqual("14", post_publish["gtm_rollback_container_version_id"])
        self.assertEqual(200, post_publish["public_gtm_http_status"])
        self.assertEqual(499401, post_publish["public_gtm_bytes"])
        self.assertEqual(1, post_publish["production_sdk_key_count"])
        self.assertFalse(post_publish["production_sdk_key_recorded"])
        self.assertEqual(0, post_publish["growthbook_feature_count"])
        self.assertFalse(post_publish["target_feature_present"])
        self.assertEqual(0, post_publish["target_feature_rule_count"])
        self.assertFalse(post_publish["production_assignment_possible"])
        self.assertTrue(post_publish["zero_collector_request_verified"])
        observation = post_publish["zero_collector_observation"]
        self.assertEqual("32741487449", observation["workflow_run_id"])
        self.assertEqual(
            "cfe10bd1f53b0b3f41433cd503b543cf242c95e3",
            observation["main_commit"],
        )
        self.assertEqual(
            "1cbfcbe6673822210cf36f771c1449c4bafa83d0ef2f8c84102285e5296e6a8b",
            observation["artifact_sha256"],
        )
        self.assertEqual(0, observation["api_request_count"])
        self.assertEqual(0, observation["accepted_receipt_count"])
        self.assertEqual("N/A:Fargate", observation["runtime"]["instance_id"])
        self.assertEqual(
            "vevo-growthbook-collector-production",
            observation["runtime"]["service"],
        )
        self.assertEqual("/app", observation["runtime"]["runtime_path"])
        self.assertTrue(post_publish["growthbook_start_allowed"])

        scope = preflight["mutation_scope"]
        self.assertFalse(
            scope["configure_gtm_consent_metadata_for_tags_54_51_55_53"]
        )
        self.assertFalse(scope["publish_gtm_workspace_17"])
        self.assertTrue(
            scope["start_growthbook_experiment_exp_19g6mmt5wugpk"]
        )
        self.assertTrue(scope["publish_growthbook_feature_revision_3"])
        for forbidden_scope in (
            "meta_ads",
            "biznisweb",
            "prices_or_product_content",
            "cart_checkout_or_orders",
            "cta_experiment",
            "collector_infrastructure",
        ):
            self.assertFalse(scope[forbidden_scope])

        self.assertFalse(self.activation["traffic"]["activation_allowed"])
        self.assertEqual(0, self.activation["traffic"]["production_allocation_percent"])

    def test_activation_preflight_drift_is_rejected(self) -> None:
        altered = copy.deepcopy(self.activation)
        altered["activation_preflight"]["live_readback"][
            "gtm_live_container_version_id"
        ] = "14"
        with self.assertRaisesRegex(AssertionError, "reviewed zero-allocation UI gate"):
            validator.validate_activation_handoff(
                altered,
                self.workspace,
                self.registry,
            )

    def test_workflow_yaml_and_every_inline_python_block_compile(self) -> None:
        payload = yaml.safe_load(WORKFLOW)
        self.assertIsInstance(payload, dict)
        lines = WORKFLOW.splitlines()
        blocks: list[str] = []
        index = 0
        while index < len(lines):
            if "python - <<'PY'" not in lines[index]:
                index += 1
                continue
            index += 1
            body: list[str] = []
            while index < len(lines) and lines[index].strip() != "PY":
                body.append(lines[index])
                index += 1
            self.assertLess(index, len(lines), "unterminated inline Python block")
            blocks.append(textwrap.dedent("\n".join(body)))
            index += 1
        self.assertGreaterEqual(len(blocks), 12)
        for block_index, source in enumerate(blocks):
            compile(source, f"production-aa-collector-inline-{block_index}.py", "exec")

    def test_local_gate_precedes_credentials_and_all_external_mutations(self) -> None:
        gate = WORKFLOW.index("PRODUCTION_AA_COLLECTOR_LOCAL_GATE_OK:")
        credentials = WORKFLOW.index(
            "uses: aws-actions/configure-aws-credentials@v6.1.0"
        )
        image_push = WORKFLOW.index("docker push")
        stack_update = WORKFLOW.index("aws cloudformation execute-change-set")
        self.assertLess(gate, credentials)
        self.assertLess(credentials, image_push)
        self.assertLess(credentials, stack_update)

    def test_predeploy_identity_variables_are_exported_to_python(self) -> None:
        export_block = "set -a\n          source production-stack.env\n          set +a"
        self.assertEqual(1, WORKFLOW.count(export_block))
        self.assertLess(
            WORKFLOW.index(export_block),
            WORKFLOW.index("workspace = json.load", WORKFLOW.index(export_block)),
        )

    def test_workflow_keeps_growthbook_gtm_meta_and_commerce_out_of_mutation_path(self) -> None:
        for forbidden in (
            "api.growthbook.io",
            "tagmanager.googleapis.com",
            "graph.facebook.com",
            "vevo.flox.sk/erp",
            "updateProduct",
        ):
            self.assertNotIn(forbidden, WORKFLOW)
        for marker in (
            "growthbook_mutations': False",
            "gtm_mutations': False",
            "meta_ads_mutations': False",
            "biznisweb_mutations': False",
            "commerce_mutations': False",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_status_only_activation_is_rejected(self) -> None:
        altered = copy.deepcopy(self.activation)
        altered["status"] = "clone_verified_collector_deploy_ready"
        with self.assertRaisesRegex(AssertionError, "reviewed zero-allocation UI gate"):
            validator.validate_activation_handoff(altered, self.workspace, self.registry)

    def test_any_unreviewed_gate_change_is_rejected(self) -> None:
        for path, value in (
            (("collector", "deployment_allowed"), True),
            (("collector", "public_route_enabled"), False),
            (("traffic", "activation_allowed"), True),
            (("traffic", "cta_experiment_started"), True),
        ):
            with self.subTest(path=path):
                altered = copy.deepcopy(self.activation)
                altered[path[0]][path[1]] = value
                with self.assertRaisesRegex(
                    AssertionError, "reviewed zero-allocation UI gate"
                ):
                    validator.validate_activation_handoff(
                        altered,
                        self.workspace,
                        self.registry,
                    )

    def test_production_registry_or_workspace_allocation_cannot_drift(self) -> None:
        registry = copy.deepcopy(self.registry)
        registry["environments"]["production"] = {}
        with self.assertRaisesRegex(AssertionError, "only the exact A/A contract"):
            validator.validate_activation_handoff(
                self.activation,
                self.workspace,
                registry,
            )

        workspace = copy.deepcopy(self.workspace)
        workspace["workspace"]["production_allocation_percent"] = 1
        with self.assertRaisesRegex(AssertionError, "zero workspace allocation"):
            validator.validate_activation_handoff(
                self.activation,
                workspace,
                self.registry,
            )

    def test_manifest_preconditions_require_matching_workspace_evidence(self) -> None:
        workspace = copy.deepcopy(self.workspace)
        workspace["athena"]["production"]["growthbook_clone"][
            "observation_status"
        ] = "not_recorded"
        with self.assertRaisesRegex(AssertionError, "clone evidence is not verified"):
            validator.validate_activation_handoff(
                self.activation,
                workspace,
                self.registry,
            )

    def test_deactivation_change_set_cannot_touch_the_service(self) -> None:
        payload = {
            "Status": "CREATE_COMPLETE",
            "ChangeSetType": "UPDATE",
            "Changes": [
                {
                    "ResourceChange": {
                        "Action": "Remove",
                        "LogicalResourceId": "CollectorPostRoute",
                        "Replacement": "False",
                        "ResourceType": "AWS::ApiGatewayV2::Route",
                    }
                },
                {
                    "ResourceChange": {
                        "Action": "Modify",
                        "LogicalResourceId": "CollectorService",
                        "Replacement": "False",
                        "ResourceType": "AWS::ECS::Service",
                    }
                },
            ],
        }
        with self.assertRaisesRegex(AssertionError, "unrelated changes"):
            validate_change_set(payload, "deactivate")


if __name__ == "__main__":
    unittest.main()
