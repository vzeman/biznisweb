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
            "python -m pip install -r requirements.txt cfn-lint==1.55.1",
            "--phase candidate > candidate-parameters.json",
            "--parameters file://candidate-parameters.json",
            'schedule.get("State") != "DISABLED"',
            "--phase candidate",
            "--phase activate",
        ):
            self.assertIn(marker, self.workflow)

    def test_hard_gate_and_one_shot_precede_schedule_activation(self) -> None:
        host = self.workflow.index("HARD_GATE_OK:")
        disabled = self.workflow.index("GROWTHBOOK_RECONCILIATION_DISABLED_STACK_OK", host)
        one_shot = self.workflow.index("GROWTHBOOK_RECONCILIATION_ONE_SHOT_OK", disabled)
        activation = self.workflow.index("--phase activate > activation-parameters.json", one_shot)
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

    def test_validated_runtime_values_are_available_in_the_resolve_step(self) -> None:
        exact_keys = self.workflow.index('raise SystemExit("runtime.env key set is not exact")')
        export_loop = self.workflow.index("while IFS='=' read -r key value; do", exact_keys)
        export_assignment = self.workflow.index('export "${key}=${value}"', export_loop)
        first_consumer = self.workflow.index('TASK_ROLE_NAME="${TASK_ROLE_ARN##*/}"', export_assignment)
        self.assertLess(exact_keys, export_loop)
        self.assertLess(export_loop, export_assignment)
        self.assertLess(export_assignment, first_consumer)

    def test_cloudformation_parameters_use_json_without_shorthand_list_coercion(self) -> None:
        self.assertIn("--parameters file://candidate-parameters.json", self.workflow)
        self.assertIn("--parameters file://activation-parameters.json", self.workflow)
        self.assertNotIn("ParameterKey=SubnetIds,ParameterValue=", self.workflow)
        self.assertIn("tests.test_growthbook_reconciliation_parameters", self.workflow)

    def test_failed_stack_is_diagnosed_read_only_before_another_host_task(self) -> None:
        preflight = self.workflow.index("reconciliation-stack-preflight.json")
        failed_status = self.workflow.index(
            "reconciliation stack requires read-only diagnosis before deploy", preflight
        )
        host_task = self.workflow.index("HARD_GATE_OK:", failed_status)
        diagnostic = self.workflow.index("SANITIZED_RECONCILIATION_STACK_DIAGNOSTIC:", host_task)
        self.assertLess(preflight, failed_status)
        self.assertLess(failed_status, host_task)
        self.assertLess(host_task, diagnostic)
        self.assertNotIn("cloudformation delete-stack", self.workflow.lower())

    def test_source_schedule_and_production_boundaries_are_unchanged(self) -> None:
        lowered = self.workflow.lower()
        self.assertNotIn("scheduler update-schedule", lowered)
        self.assertNotIn("s3api delete-object", lowered)
        self.assertNotIn("submit", lowered)
        self.assertIn("VEVO reporting source schedule changed", self.workflow)
        self.assertIn("Production GrowthBook allocation: \\`0%\\` (unchanged)", self.workflow)
        self.assertIn("GTM: unpublished (unchanged)", self.workflow)

    def test_monitoring_and_no_payload_logs_are_required(self) -> None:
        template = (ROOT / "infra/vevo-growthbook-reconciliation/template.yaml").read_text(encoding="utf-8")
        for marker in (
            "vevo-growthbook-reconcile-preview-failure",
            "vevo-growthbook-reconcile-preview-missing-success",
            "vevo-growthbook-reconcile-preview-dlq",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE",
        ):
            self.assertIn(marker, template)

    def test_scheduler_trust_is_scoped_to_the_exact_default_schedule_group(self) -> None:
        template = (ROOT / "infra/vevo-growthbook-reconciliation/template.yaml").read_text(encoding="utf-8")
        self.assertIn("schedule-group/default", template)
        self.assertNotIn("schedule/default/vevo-growthbook-reconcile-preview", template)


if __name__ == "__main__":
    unittest.main()
