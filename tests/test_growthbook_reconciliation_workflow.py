from __future__ import annotations

import pathlib
import re
import textwrap
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
            "environment:",
            "- production",
            "- preview",
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
            "TARGET_TASK_FAMILY: ${{ format('vevo-growthbook-reconcile-{0}', inputs.environment) }}",
            "EXPECTED_TASK_ROLE: BiznisWebReportingTaskRole-vevo",
            "COLLECTOR_STACK_NAME: ${{ inputs.environment == 'production'",
            "IMAGE_TAG: git-${{ github.sha }}",
            "service=${SCHEDULE_NAME}:path=${RUNTIME_PATH}",
            "scripts/run_scheduled_growthbook_reconciliation.py",
            "scripts/growthbook_reconcile_host_gate.sh",
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

    def test_source_schedule_and_external_boundaries_are_unchanged(self) -> None:
        lowered = self.workflow.lower()
        self.assertNotIn("scheduler update-schedule", lowered)
        self.assertNotIn("s3api delete-object", lowered)
        self.assertNotIn("submit", lowered)
        self.assertIn("VEVO reporting source schedule changed", self.workflow)
        self.assertIn("GrowthBook experiment allocation: unchanged", self.workflow)
        self.assertIn("GTM: unchanged", self.workflow)

    def test_monitoring_and_no_payload_logs_are_required(self) -> None:
        template = (ROOT / "infra/vevo-growthbook-reconciliation/template.yaml").read_text(encoding="utf-8")
        for marker in (
            "vevo-growthbook-reconcile-${Environment}-failure",
            "vevo-growthbook-reconcile-${Environment}-missing-success",
            "vevo-growthbook-reconcile-${Environment}-dlq",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE",
        ):
            self.assertIn(marker, template)

    def test_preview_and_production_names_and_times_are_isolated(self) -> None:
        for marker in (
            "vevo-growthbook-reconciliation-{0}",
            "vevo-growthbook-reconcile-{0}",
            "GROWTHBOOK_ENVIRONMENT: ${{ inputs.environment }}",
            "growthbook_reconciliation_production",
            "cron(45 3 * * ? *)",
            "cron(30 3 * * ? *)",
        ):
            self.assertIn(marker, self.workflow)

    def test_evidence_is_sanitized_and_uploaded_without_raw_aws_files(self) -> None:
        readback = self.workflow.index(
            "GROWTHBOOK_RECONCILIATION_SCHEDULE_READBACK_OK"
        )
        evidence = self.workflow.index(
            "GROWTHBOOK_RECONCILIATION_EVIDENCE_OK", readback
        )
        upload = self.workflow.index(
            "Upload sanitized deployment evidence only", evidence
        )
        self.assertLess(readback, evidence)
        self.assertLess(evidence, upload)
        upload_block = self.workflow[upload : self.workflow.index(
            "Deployment summary", upload
        )]
        self.assertIn("uses: actions/upload-artifact@v4.6.2", upload_block)
        self.assertIn("path: ${{ env.EVIDENCE_FILE }}", upload_block)
        for forbidden in (
            "collector-stack.json",
            "host-gate-logs.json",
            "reconciliation-logs.json",
            "source-schedule-before.json",
            "deployed-schedule.json",
        ):
            self.assertNotIn(forbidden, upload_block)
        for marker in (
            '"contains_raw_aws_payloads": False',
            '"contains_cloudwatch_messages": False',
            '"contains_event_device_customer_or_order_ids": False',
            '"contains_credentials": False',
            '"raw_event_delete_performed": False',
            '"growthbook_experiment_mutated": False',
            '"meta_ads_mutated": False',
            '"biznisweb_mutated": False',
        ):
            self.assertIn(marker, self.workflow)

    def test_exact_reporting_policy_is_attached_before_task_registration(self) -> None:
        document_gate = self.workflow.index("REPORTING_POLICY_DOCUMENT_EXACT_OK")
        attachment = self.workflow.index("aws iam attach-role-policy", document_gate)
        readback = self.workflow.index("REPORTING_POLICY_ATTACHED_OK", attachment)
        task_registration = self.workflow.index("aws ecs register-task-definition", readback)
        self.assertLess(document_gate, attachment)
        self.assertLess(attachment, readback)
        self.assertLess(readback, task_registration)
        for marker in (
            '"ReportingWorkGroupName"',
            '"ExperimentDatabaseName"',
            '"document_exactly_verified": True',
            '"attached_by_run": policy_attached_by_run == "true"',
            '"attachment_readback_verified": True',
        ):
            self.assertIn(marker, self.workflow)
        self.assertNotIn("aws iam detach-role-policy", self.workflow)

    def test_generic_host_gate_keeps_the_legacy_preview_entrypoint(self) -> None:
        generic = (
            ROOT / "scripts/growthbook_reconcile_host_gate.sh"
        ).read_text(encoding="utf-8")
        legacy = (
            ROOT / "scripts/growthbook_preview_reconcile_host_gate.sh"
        ).read_text(encoding="utf-8")
        self.assertIn('"preview" && "${ENVIRONMENT}" != "production"', generic)
        self.assertIn('ThreadingHTTPServer(("127.0.0.1", 8000)', generic)
        self.assertIn('urlopen("http://127.0.0.1:8000/health"', generic)
        self.assertIn('urlopen("http://127.0.0.1:8000/marker.json"', generic)
        self.assertIn("growthbook_reconcile_host_gate.sh", legacy)
        self.assertIn("Legacy Preview host-gate entrypoint requires preview", legacy)

    def test_every_inline_python_block_compiles(self) -> None:
        lines = self.workflow.splitlines()
        blocks: list[str] = []
        index = 0
        while index < len(lines):
            if not re.search(r"python3?(?:\s+[^<]+)?\s+<<'PY'", lines[index]):
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
        self.assertGreaterEqual(len(blocks), 10)
        for block_index, source in enumerate(blocks):
            compile(source, f"reconciliation-workflow-inline-{block_index}.py", "exec")

    def test_runtime_changes_trigger_an_exact_image_build(self) -> None:
        build_workflow = (
            ROOT / ".github/workflows/build-and-push-ecr.yml"
        ).read_text(encoding="utf-8")
        for path in (
            "scripts/growthbook_reconcile_host_gate.sh",
            "scripts/growthbook_preview_reconcile_host_gate.sh",
            "scripts/reconcile_growthbook_facts.py",
            "scripts/run_scheduled_growthbook_reconciliation.py",
        ):
            self.assertIn(f"- {path}", build_workflow)

    def test_scheduler_trust_is_scoped_to_the_exact_default_schedule_group(self) -> None:
        template = (ROOT / "infra/vevo-growthbook-reconciliation/template.yaml").read_text(encoding="utf-8")
        self.assertIn("schedule-group/default", template)
        self.assertNotIn("schedule/default/vevo-growthbook-reconcile-preview", template)


if __name__ == "__main__":
    unittest.main()
