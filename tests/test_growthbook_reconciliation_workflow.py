from __future__ import annotations

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "deploy-vevo-growthbook-reconciliation.yml"


class GrowthBookReconciliationWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_is_main_only_explicit_and_disabled_first(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_deploy:",
            "ParameterKey=ScheduleState,ParameterValue=DISABLED",
            'schedule.get("State") != "DISABLED"',
            "--phase candidate",
            "--phase activate",
        ):
            self.assertIn(marker, self.workflow)

    def test_hard_gate_and_one_shot_precede_schedule_activation(self) -> None:
        host = self.workflow.index("HARD_GATE_OK:")
        disabled = self.workflow.index("GROWTHBOOK_RECONCILIATION_DISABLED_STACK_OK", host)
        one_shot = self.workflow.index("GROWTHBOOK_RECONCILIATION_ONE_SHOT_OK", disabled)
        activation = self.workflow.index("ParameterKey=ScheduleState,ParameterValue=ENABLED", one_shot)
        readback = self.workflow.index("GROWTHBOOK_RECONCILIATION_SCHEDULE_READBACK_OK", activation)
        self.assertLess(host, disabled)
        self.assertLess(disabled, one_shot)
        self.assertLess(one_shot, activation)
        self.assertLess(activation, readback)

    def test_exact_runtime_identity_command_and_limits_are_preserved(self) -> None:
        for marker in (
            "SOURCE_SCHEDULE: vevo-daily-report-email",
            "SOURCE_TASK_FAMILY: vevo-reporting-daily",
            "TARGET_TASK_FAMILY: vevo-growthbook-reconcile-preview",
            "EXPECTED_TASK_ROLE: BiznisWebReportingTaskRole-vevo",
            "service=${SCHEDULE_NAME}:path=${RUNTIME_PATH}",
            "scripts/run_scheduled_growthbook_reconciliation.py",
            'summary.get("event_partitions") != 40',
            'not 0 <= raw_events <= 50000',
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:",
        ):
            self.assertIn(marker, self.workflow)

    def test_source_schedule_and_production_boundaries_are_unchanged(self) -> None:
        lowered = self.workflow.lower()
        self.assertNotIn("scheduler update-schedule", lowered)
        self.assertNotIn("s3api delete-object", lowered)
        self.assertNotIn("submit", lowered)
        self.assertIn("VEVO reporting source schedule changed", self.workflow)
        self.assertIn("Production GrowthBook allocation: \\`0%\\` (unchanged)", self.workflow)
        self.assertIn("GTM: unpublished (unchanged)", self.workflow)

    def test_monitoring_and_no_payload_logs_are_required(self) -> None:
        for marker in (
            "vevo-growthbook-reconcile-preview-failure",
            "vevo-growthbook-reconcile-preview-missing-success",
            "vevo-growthbook-reconcile-preview-dlq",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE",
        ):
            self.assertIn(marker, (ROOT / "infra/vevo-growthbook-reconciliation/template.yaml").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
