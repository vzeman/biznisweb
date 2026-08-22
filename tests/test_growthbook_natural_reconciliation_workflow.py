from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "verify-vevo-growthbook-natural-reconciliation.yml"
)


class GrowthBookNaturalReconciliationWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_is_main_only_explicit_and_time_gated_before_aws(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_verification:",
            "TARGET_RUN_DUE_UTC: '2026-08-23T01:30:00Z'",
            "VERIFY_NOT_BEFORE_UTC: '2026-08-23T01:40:00Z'",
            "VERIFY_BEFORE_UTC: '2026-08-23T02:20:00Z'",
            "2026-08-23T01:29:00+00:00",
            "--start-time '2026-08-23T01:29:00Z'",
            "natural retention-recovery verification is not due until",
            "natural retention-recovery live ECS evidence window has closed",
            "workspace.get('reconciliation_checkpoint', {}).get(",
            "EXPECTED_CLUSTER_ARN: arn:aws:ecs:eu-central-1:919341186960:cluster/vevo-reporting-cluster",
            "EXPECTED_CONTAINER_NAME: reporting",
            "EXPECTED_LOG_GROUP: /ecs/vevo-reporting-daily",
            "EXPECTED_LOG_PREFIX: ecs",
            "values[0] != os.environ['EXPECTED_CLUSTER_ARN']",
            "Configure AWS credentials for read-only evidence",
        ):
            self.assertIn(marker, self.workflow)
        gate = self.workflow.index(
            "natural retention-recovery verification is not due until"
        )
        credentials = self.workflow.index("Configure AWS credentials for read-only evidence")
        self.assertLess(gate, credentials)
        self.assertNotIn(
            "workspace.get('workspace', {}).get('recurring_schedule'", self.workflow
        )

    def test_requires_full_natural_scheduler_evidence_chain(self) -> None:
        for marker in (
            "cloudformation describe-stacks",
            "scheduler get-schedule",
            "ecs describe-task-definition",
            "logs filter-log-events",
            "ecs describe-tasks",
            "logs get-log-events",
            "cloudtrail lookup-events",
            "sqs get-queue-attributes",
            "cloudwatch describe-alarms",
            "scripts/summarize_growthbook_natural_task_readback.py",
            "scripts/verify_growthbook_natural_reconciliation.py",
            "retention-recovery window requires exactly one success and zero failures",
            "NATURAL_RUNTIME_IDENTITY_OK:",
            "GROWTHBOOK_NATURAL_RECONCILIATION_OK:",
            "GROWTHBOOK_NATURAL_EVIDENCE_READY:",
        ):
            self.assertIn(marker, self.workflow + (ROOT / "scripts" / "verify_growthbook_natural_reconciliation.py").read_text(encoding="utf-8"))
        summary = self.workflow.index(
            "scripts/summarize_growthbook_natural_task_readback.py"
        )
        verifier = self.workflow.index(
            "scripts/verify_growthbook_natural_reconciliation.py"
        )
        self.assertLess(summary, verifier)

    def test_uploads_only_the_sanitized_versioned_evidence_file_after_verification(self) -> None:
        verifier = self.workflow.index("scripts/verify_growthbook_natural_reconciliation.py")
        upload = self.workflow.index("Upload sanitized natural reconciliation evidence only")
        for marker in (
            "--evidence-output \"${EVIDENCE_FILE}\"",
            "--workflow-run-id \"${GITHUB_RUN_ID}\"",
            "--main-commit \"${GITHUB_SHA}\"",
            "uses: actions/upload-artifact@v4.6.2",
            "path: vevo-growthbook-natural-reconciliation-evidence.json",
            "retention-days: 14",
            "sanitized schema v2 only; no raw AWS payloads or credentials",
        ):
            self.assertIn(marker, self.workflow)
        self.assertLess(verifier, upload)
        self.assertEqual(1, self.workflow.count("uses: actions/upload-artifact@v4.6.2"))
        self.assertNotIn("path: natural-", self.workflow)

    def test_workflow_contains_no_runtime_or_external_mutation(self) -> None:
        lowered = self.workflow.lower()
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
            "aws logs put-",
            "aws logs delete-",
            "aws cloudwatch put-",
            "aws cloudwatch delete-",
            "aws cloudwatch set-alarm-state",
            "aws s3api put-",
            "aws s3api delete-",
            "aws athena start-query-execution",
            "ads_update",
            "submit",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn("AWS mutations: `none`", self.workflow)
        self.assertIn("Meta Ads and BiznisWeb commerce state: `unchanged`", self.workflow)


if __name__ == "__main__":
    unittest.main()
