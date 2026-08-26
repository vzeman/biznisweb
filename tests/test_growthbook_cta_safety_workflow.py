from __future__ import annotations

import pathlib
import re
import textwrap
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    ROOT
    / ".github"
    / "workflows"
    / "check-vevo-growthbook-production-cta-safety.yml"
)
WORKFLOW = WORKFLOW_PATH.read_text(encoding="utf-8")
BUILDER = (
    ROOT / "scripts" / "build_growthbook_cta_safety_checkpoint.py"
).read_text(encoding="utf-8")
SQL = (
    ROOT
    / "projects"
    / "vevo"
    / "growthbook_sql"
    / "cta_safety_checkpoint_production.sql"
).read_text(encoding="utf-8")


def inline_python_blocks() -> list[str]:
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
        if index >= len(lines):
            raise AssertionError("unterminated inline Python block")
        blocks.append(textwrap.dedent("\n".join(body)))
        index += 1
    return blocks


class GrowthBookCtaSafetyWorkflowTests(unittest.TestCase):
    def test_every_inline_python_block_compiles(self) -> None:
        blocks = inline_python_blocks()
        self.assertGreaterEqual(len(blocks), 7)
        for index, source in enumerate(blocks):
            compile(source, f"cta-safety-inline-{index}.py", "exec")

    def test_main_only_hourly_gate_precedes_credentials(self) -> None:
        for marker in (
            "name: Check VEVO GrowthBook Production CTA Safety",
            "- cron: '5 * * * *'",
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_checkpoint:",
            "Validate safety gate before AWS credentials",
            "build_growthbook_cta_safety_checkpoint.py prepare",
            "uses: aws-actions/configure-aws-credentials@v6.1.0",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertLess(
            WORKFLOW.index("build_growthbook_cta_safety_checkpoint.py prepare"),
            WORKFLOW.index("uses: aws-actions/configure-aws-credentials@v6.1.0"),
        )

    def test_every_credential_or_post_gate_step_is_conditioned(self) -> None:
        names = (
            "Configure AWS credentials for bounded read-only checkpoint",
            "Verify exact Fargate host gate and current control plane",
            "Verify latest scheduled marker alarms and DLQ",
            "Query only aggregate CTA safety fields",
            "Probe storefront with GET only and build canonical bundle",
            "Upload only the canonical three-file safety bundle",
            "Publish safety-only checkpoint summary",
        )
        for name in names:
            block = WORKFLOW[WORKFLOW.index(f"- name: {name}") :]
            next_step = block.find("\n      - name:", 1)
            if next_step >= 0:
                block = block[:next_step]
            self.assertIn(
                "if: ${{ env.RUN_CHECKPOINT == 'true' }}",
                block,
                name,
            )
        cleanup = WORKFLOW[
            WORKFLOW.index(
                "- name: Remove every temporary AWS query and storefront response"
            ) :
        ]
        self.assertIn(
            "if: ${{ always() && env.RUN_CHECKPOINT == 'true' }}", cleanup
        )

    def test_exact_host_identity_localhost_marker_and_control_plane_are_hard_gated(self) -> None:
        for marker in (
            "'instance_id': 'N/A:Fargate'",
            "'private_ip': '172.31.39.76'",
            "'service': 'vevo-growthbook-reconcile-production'",
            "'runtime_path': '/app'",
            "'localhost_health_verified': True",
            "'localhost_marker_verified': True",
            "21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:",
            "Production reconciliation alarm gate failed",
            "Production reconciliation DLQ is not empty",
            "scheduled runtime/source identity drift",
            "PRODUCTION_CTA_SAFETY_HOST_GATE_OK:instance-id=N/A:Fargate",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_only_bounded_aggregate_safety_query_is_available(self) -> None:
        for marker in (
            "experiment_device_facts",
            "experiment_performance_facts",
            "variation_id IN ('control', 'brand_contrast')",
            "client_error_devices",
            "lcp_p75_ms",
            "inp_p75_ms",
            "cls_p75_milli",
            "__CTA_STARTED_AT_UTC__",
            "__CHECKPOINT_THROUGH_UTC__",
        ):
            self.assertIn(marker, SQL)
        lowered = SQL.lower()
        for forbidden in (
            "add_to_cart_devices",
            "purchase_devices",
            "conversion_rate",
            "revenue_eur",
            "cm1_eur",
            "meta_campaign",
            "meta_adset",
            "winner",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn("--max-results 3", WORKFLOW)
        self.assertIn("{'query_failed':true}".replace("'", '"'), WORKFLOW)

    def test_storefront_probe_is_get_only_and_never_changes_commerce(self) -> None:
        self.assertEqual(2, WORKFLOW.count("curl --request GET"))
        lowered = WORKFLOW.lower()
        for forbidden in (
            "--request post",
            "--request put",
            "--request patch",
            "--request delete",
            "add-to-cart",
            "add_to_cart_devices",
            "checkout submit",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn('"cart_checkout_order_mutated": False', BUILDER)

    def test_workflow_has_no_infrastructure_or_experiment_mutation_client(self) -> None:
        lowered = WORKFLOW.lower()
        for forbidden in (
            "aws cloudformation create-",
            "aws cloudformation update-",
            "aws cloudformation execute-",
            "aws cloudformation delete-",
            "aws ecs run-task",
            "aws ecs register-task-definition",
            "aws scheduler create-",
            "aws scheduler update-",
            "aws scheduler delete-",
            "aws sqs send-message",
            "aws sqs delete-",
            "aws s3api put-",
            "aws s3api delete-",
            "growthbook api",
            "meta graph",
            "biznisweb admin",
            "gtm publish",
        ):
            self.assertNotIn(forbidden, lowered)
        for forbidden in ("import boto3", "import requests", "import subprocess"):
            self.assertNotIn(forbidden, BUILDER)

    def test_artifact_is_exact_three_file_hash_bound_bundle(self) -> None:
        self.assertIn("name: vevo-growthbook-cta-safety-checkpoint", WORKFLOW)
        self.assertEqual(
            2,
            WORKFLOW.count("vevo-growthbook-cta-safety-evidence.json"),
        )
        self.assertEqual(
            2,
            WORKFLOW.count("vevo-growthbook-cta-safety-decision.json"),
        )
        self.assertEqual(
            2,
            WORKFLOW.count("vevo-growthbook-cta-safety-provenance.json"),
        )
        self.assertIn("retention-days: 90", WORKFLOW)
        self.assertIn('"evidence_sha256": evidence_sha256', BUILDER)
        self.assertIn('"decision_sha256": decision_sha256', BUILDER)
        self.assertIn('"workflow_run_id": workflow_run_id', BUILDER)
        self.assertIn('"main_commit": main_commit', BUILDER)
        cleanup = WORKFLOW.index(
            "Remove every temporary AWS query and storefront response"
        )
        upload = WORKFLOW.index(
            "Upload only the canonical three-file safety bundle"
        )
        self.assertLess(cleanup, upload)

    def test_builder_contains_no_external_client_or_raw_identity_output(self) -> None:
        for forbidden in (
            "urllib",
            "socket",
            "requests",
            "boto3",
            "subprocess",
            '"device_id":',
            '"event_id":',
            '"customer_id":',
            '"order_id":',
        ):
            self.assertNotIn(forbidden, BUILDER)
        self.assertIsNone(
            re.search(r'"winner_call_made"\s*:\s*True', BUILDER)
        )


if __name__ == "__main__":
    unittest.main()
