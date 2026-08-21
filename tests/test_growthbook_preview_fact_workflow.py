from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "verify-vevo-growthbook-preview-facts.yml"


class GrowthBookPreviewFactWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_performance_publish_is_explicit_and_recovery_only(self) -> None:
        self.assertIn("confirm_performance_publish:", self.workflow)
        self.assertIn('default: "false"', self.workflow)
        self.assertIn(
            '[[ "${CONFIRM_PERFORMANCE_PUBLISH}" == "true" && -z "${RECOVERY_SOURCE_RUN_ID}" ]]',
            self.workflow,
        )
        self.assertIn("Recovery requires recovery_source_event_date", self.workflow)

    def test_performance_identity_is_deterministic_and_idempotent(self) -> None:
        self.assertIn('deterministic_uuid4("performance-event")', self.workflow)
        self.assertIn('deterministic_uuid4("performance-page-load")', self.workflow)
        self.assertIn("SYNTHETIC_PERFORMANCE_RAW_REUSED", self.workflow)
        self.assertIn("Recovered exposure event date is not today", self.workflow)
        self.assertIn("recovered exposure is outside the 24-hour health window", self.workflow)
        self.assertIn("existing synthetic performance event identity drift", self.workflow)
        self.assertIn("curated performance fact identity drift", self.workflow)

    def test_performance_contract_is_exact_and_read_back_through_athena(self) -> None:
        for marker in (
            '"event_name": "performance_vital"',
            '"vital_name": "lcp_ms"',
            '"vital_value": 1300',
            '"performance_facts": 1 if performance_mode else 0',
            "FROM experiment_performance_facts",
            "GROWTHBOOK_PREVIEW_PERFORMANCE_FACT_CHAIN_OK",
        ):
            self.assertIn(marker, self.workflow)

    def test_workflow_does_not_promote_or_delete_runtime_data(self) -> None:
        lowered = self.workflow.lower()
        self.assertNotIn("s3api delete-object", lowered)
        self.assertNotIn("scheduler update-schedule", lowered)
        self.assertIn('echo "- Production allocation: \\`0%\\` (unchanged)"', self.workflow)


if __name__ == "__main__":
    unittest.main()
