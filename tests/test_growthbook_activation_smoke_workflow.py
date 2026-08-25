from __future__ import annotations

import json
import pathlib
import textwrap
import unittest

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    ROOT
    / ".github"
    / "workflows"
    / "verify-vevo-growthbook-production-aa-activation-smoke.yml"
)
WORKFLOW = WORKFLOW_PATH.read_text(encoding="utf-8")
OBSERVATION = json.loads(
    (ROOT / "projects" / "vevo" / "growthbook_aa_activation_browser_observation.json").read_text(
        encoding="utf-8"
    )
)


class GrowthBookActivationSmokeWorkflowTests(unittest.TestCase):
    def test_workflow_is_main_only_read_only_and_frozen(self) -> None:
        payload = yaml.safe_load(WORKFLOW)
        self.assertEqual({"contents": "read"}, payload["permissions"])
        job = payload["jobs"]["verify-activation-smoke"]
        self.assertEqual("${{ github.ref == 'refs/heads/main' }}", job["if"])
        self.assertIn("OBSERVATION_FROM_UTC: '2026-08-25T05:34:30Z'", WORKFLOW)
        self.assertIn("OBSERVATION_THROUGH_UTC: '2026-08-25T05:44:30Z'", WORKFLOW)
        self.assertIn("schema-9 activation gate is required", WORKFLOW)
        self.assertIn("exp_19g6mmt5wugpk", WORKFLOW)

    def test_runtime_hard_gate_is_exact(self) -> None:
        for marker in (
            "EXPECTED_ACCOUNT_ID: '919341186960'",
            "EXPECTED_SERVICE: vevo-growthbook-collector-production",
            "EXPECTED_TASK_DEFINITION: vevo-growthbook-collector-production:2",
            "EXPECTED_COLLECTOR_VERSION: git-57b29c3b166eabbbabee4d3b8e69d1b56e2ae8e2",
            "EXPECTED_RUNTIME_PATH: /app",
            "instance_id': 'N/A:Fargate'",
            "immutable_image_prior_localhost_marker",
            "target_health': 'healthy'",
            "POST /v1/events",
            "Production collector version parameter drift",
            "Production task-definition collector version drift",
            '--expected-collector-version "${EXPECTED_COLLECTOR_VERSION}"',
        ):
            self.assertIn(marker, WORKFLOW)

    def test_local_gate_precedes_credentials_and_raw_reads(self) -> None:
        local_gate = WORKFLOW.index("PRODUCTION_AA_ACTIVATION_LOCAL_GATE_OK:")
        credentials = WORKFLOW.index("uses: aws-actions/configure-aws-credentials@v6.1.0")
        raw_read = WORKFLOW.index("aws s3api get-object")
        self.assertLess(local_gate, credentials)
        self.assertLess(credentials, raw_read)

    def test_runner_local_gate_has_no_optional_pyyaml_dependency(self) -> None:
        gate = WORKFLOW.split(
            "- name: Enforce frozen activation gate before AWS credentials", 1
        )[1].split("- name: Configure AWS credentials", 1)[0]
        self.assertIn("tests.test_growthbook_activation_smoke_reducer", gate)
        self.assertNotIn("tests.test_growthbook_activation_smoke_workflow", gate)
        self.assertNotIn("tests.test_growthbook_production_aa_activation", gate)
        self.assertNotIn("pip install", gate)

    def test_workflow_has_no_external_mutation_path(self) -> None:
        for forbidden in (
            "aws s3api put-object",
            "aws ecs update-service",
            "aws cloudformation update-stack",
            "aws cloudformation execute-change-set",
            "aws apigatewayv2 create-route",
            "aws apigatewayv2 delete-route",
            "docker push",
            "api.growthbook.io/api/v1",
            "tagmanager.googleapis.com/tagmanager/v2",
            "graph.facebook.com",
            "updateProduct",
        ):
            self.assertNotIn(forbidden, WORKFLOW)
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        self.assertIn("path: vevo-growthbook-production-aa-activation-smoke.json", WORKFLOW)
        self.assertNotIn("path: ${TEMP_DIR}", WORKFLOW)

    def test_raw_identity_is_reduced_and_cleaned_before_upload(self) -> None:
        self.assertIn("scripts/summarize_growthbook_activation_smoke.py", WORKFLOW)
        self.assertIn("--page-size 500", WORKFLOW)
        self.assertIn("--query 'Contents[].{Key:Key,LastModified:LastModified}'", WORKFLOW)
        self.assertIn("Production raw-object paginated listing is malformed", WORKFLOW)
        self.assertNotIn("Production raw-object listing is truncated", WORKFLOW)
        self.assertIn("contains_event_or_device_ids': False", WORKFLOW)
        self.assertIn("contains_raw_aws_payloads': False", WORKFLOW)
        self.assertIn('trap cleanup EXIT', WORKFLOW)
        self.assertIn('rm -rf "${TEMP_DIR}"', WORKFLOW)
        self.assertLess(WORKFLOW.index("trap cleanup EXIT"), WORKFLOW.index("aws s3api get-object"))

    def test_browser_observation_is_exact_and_has_pending_backend_proof(self) -> None:
        growthbook = OBSERVATION["growthbook"]
        browser = OBSERVATION["browser_qa"]
        self.assertEqual("running", growthbook["status"])
        self.assertEqual(3, growthbook["feature_revision"])
        self.assertEqual(100, growthbook["traffic_percent"])
        self.assertEqual([0.5, 0.5], growthbook["variation_weights"])
        self.assertEqual("draft", growthbook["cta_experiment_status"])
        self.assertTrue(browser["tag_assistant_connected"])
        self.assertEqual(2, browser["observed_page_load_count"])
        self.assertEqual(0, browser["console_error_count"])
        self.assertFalse(browser["cart_mutated"])
        self.assertEqual(
            {
                "accepted_collector_delivery_verified": False,
                "sticky_assignment_verified": False,
                "variation_value_recorded": False,
            },
            OBSERVATION["pending_backend_proof"],
        )

    def test_every_inline_python_block_compiles(self) -> None:
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
        self.assertGreaterEqual(len(blocks), 7)
        for block_index, source in enumerate(blocks):
            compile(source, f"activation-smoke-inline-{block_index}.py", "exec")


if __name__ == "__main__":
    unittest.main()
