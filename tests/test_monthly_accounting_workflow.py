import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT_DIR / ".github" / "workflows" / "deploy-monthly-creditnote-export.yml"


class MonthlyAccountingWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    def test_builds_and_pins_the_exact_commit_image(self) -> None:
        self.assertIn('DEPLOY_IMAGE_TAG="monthly-creditnote-export-${GITHUB_SHA}"', self.workflow)
        self.assertIn('docker build -t "${IMAGE_URI}" .', self.workflow)
        self.assertIn('docker push "${IMAGE_URI}"', self.workflow)
        self.assertIn('IMAGE_IDENTIFIER="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}@${IMAGE_DIGEST}"', self.workflow)
        self.assertNotIn('imageTag="latest"', self.workflow)

    def test_promotes_scheduler_only_after_the_host_marker(self) -> None:
        marker_gate = self.workflow.index('grep -q "${EXPECTED_MARKER}" smoke-task.log')
        schedule_update = self.workflow.index("aws scheduler update-schedule", marker_gate)
        current_tag = self.workflow.index(
            'protect_ecr_image_tag "${IMAGE_IDENTIFIER}" "${ECR_CURRENT_TAG}"',
            marker_gate,
        )
        self.assertLess(marker_gate, schedule_update)
        self.assertLess(schedule_update, current_tag)

    def test_validates_all_four_accounting_attachments(self) -> None:
        self.assertIn('output_pdfs = summary["output_pdfs"]', self.workflow)
        self.assertIn('invoice_export = summary["invoice_export"]', self.workflow)
        self.assertIn('root.tag.rsplit("}", 1)[-1] == "MoneyData"', self.workflow)
        self.assertIn('"attachment_count": len(output_pdfs) + len(invoice_export["output_files"])', self.workflow)

    def test_real_smoke_uses_explicit_send_mode(self) -> None:
        self.assertIn('EMAIL_MODE="send"', self.workflow)
        self.assertIn('dry-run) RUN_ARGS+=(--dry-run-email)', self.workflow)
        self.assertIn('send) ;;', self.workflow)

    def test_scheduler_has_dlq_and_failure_alarms(self) -> None:
        self.assertIn('target["DeadLetterConfig"] = {"Arn": dlq_arn}', self.workflow)
        self.assertIn("monthly-creditnote-export-dlq-not-empty", self.workflow)
        self.assertIn("monthly-creditnote-export-run-failed", self.workflow)
        self.assertIn('assert target["DeadLetterConfig"]["Arn"] == expected_dlq', self.workflow)


if __name__ == "__main__":
    unittest.main()
