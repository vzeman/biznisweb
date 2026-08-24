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

    def test_checked_in_handoff_is_zero_allocation_prepared(self) -> None:
        validator.validate_activation_handoff(
            self.activation,
            self.workspace,
            self.registry,
        )

    def test_growthbook_and_gtm_draft_evidence_is_exact(self) -> None:
        self.assertEqual("draft_not_started", self.activation["growthbook"]["status"])
        self.assertEqual(
            "draft_not_published",
            self.activation["growthbook"]["production_rule_publish_status"],
        )
        self.assertEqual("not_published", self.activation["gtm"]["publish_status"])
        self.assertEqual(
            {"added": 5, "modified": 0, "removed": 0},
            self.activation["gtm"]["unprocessed_changes"],
        )
        self.assertTrue(self.activation["gtm"]["setup_tag_sequencing_verified"])
        self.assertEqual(0, self.activation["traffic"]["production_allocation_percent"])
        self.assertFalse(self.activation["traffic"]["activation_allowed"])

    def test_tag_assistant_qa_remains_fail_closed_until_all_gates_pass(self) -> None:
        qa = self.activation["tag_assistant_qa"]
        self.assertEqual(
            "mobile_zero_assignment_consent_and_storage_observed_"
            "collector_pending",
            qa["status"],
        )
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
        self.assertFalse(qa["zero_collector_request_verified"])
        self.assertFalse(self.activation["traffic"]["activation_allowed"])
        self.assertEqual(0, self.activation["traffic"]["production_allocation_percent"])

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
