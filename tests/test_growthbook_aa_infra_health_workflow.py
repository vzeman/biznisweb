from __future__ import annotations

import pathlib
import textwrap
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "monitor-vevo-growthbook-production-aa-infra.yml"
).read_text(encoding="utf-8")


class GrowthBookAaInfraHealthWorkflowTests(unittest.TestCase):
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
        self.assertGreaterEqual(len(blocks), 8)
        for block_index, source in enumerate(blocks):
            compile(source, f"infra-health-workflow-inline-{block_index}.py", "exec")

    def test_main_confirmation_and_dst_gate_precede_credentials(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_health:",
            "15 2 * * *",
            "15 3 * * *",
            "ZoneInfo('Europe/Bratislava')",
            "EVENT_SCHEDULE",
            "RUN_INFRA_HEALTH",
            "population-read=false:outcome-read=false:mutation=none",
            "uses: aws-actions/configure-aws-credentials@v6.1.0",
        ):
            self.assertIn(marker, WORKFLOW)
        gate = WORKFLOW.index("PRODUCTION_AA_INFRA_LOCAL_GATE_OK:")
        credentials = WORKFLOW.index(
            "uses: aws-actions/configure-aws-credentials@v6.1.0"
        )
        self.assertLess(gate, credentials)

    def test_exact_fargate_schedule_marker_alarm_and_dlq_gates(self) -> None:
        for marker in (
            "21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb",
            "919341186960",
            "vevo-growthbook-reconcile-production",
            "vevo-growthbook-reconciliation-production",
            "vevo-growthbook-production",
            "cron(45 3 * * ? *)",
            "Europe/Bratislava",
            "vevo-reporting-daily:33",
            "collector.get('StackStatus') != 'UPDATE_COMPLETE'",
            "reconciliation.get('StackStatus') != 'UPDATE_COMPLETE'",
            "schedule_drift = sorted(key for key, passed in schedule_checks.items() if not passed)",
            "Production reconciliation schedule drift:' + ','.join(schedule_drift)",
            "reconciliation_parameters.get('ClusterArn')",
            "source.get('Target', {}).get('Arn') != reconciliation_parameters.get('ClusterArn')",
            "'RECONCILIATION_CLUSTER_ARN': reconciliation_parameters['ClusterArn']",
            '--cluster "${RECONCILIATION_CLUSTER_ARN}"',
            "scheduled reconciliation image differs from localhost-gated deploy evidence",
            'source "${TEMP_HEALTH_DIR}/selected-task.env"',
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:",
            "scheduled reconciliation generated/published parity drift",
            "Production reconciliation alarm gate failed",
            "Production reconciliation DLQ is not empty",
            "PRODUCTION_AA_INFRA_RUNTIME_OK:instance-id=N/A:Fargate",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn("collector_outputs['CollectorClusterArn']", WORKFLOW)

    def test_recovers_exact_runtime_identity_after_stopped_task_retention_expires(self) -> None:
        for marker in (
            "aws logs filter-log-events",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE:",
            "aws cloudtrail lookup-events",
            "AttributeKey=EventName,AttributeValue=RunTask",
            "identity.get('invokedBy') == 'scheduler.amazonaws.com'",
            "RECONCILIATION_SCHEDULER_ROLE_ARN",
            "selected-cloudtrail-event.json",
            "cloudtrail_run_task_retention_recovery",
            "privateIPv4Address",
            "privateIpv4Address",
            "networkInterfaces",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn("aws ecs list-tasks", WORKFLOW)
        self.assertLess(
            WORKFLOW.index("aws logs filter-log-events"),
            WORKFLOW.index("aws ecs describe-tasks"),
        )

    def test_contains_no_population_outcome_or_data_query(self) -> None:
        lowered = WORKFLOW.lower()
        for forbidden in (
            "start-query-execution",
            "get-query-results",
            "experiment_device_facts",
            "experiment_performance_facts",
            "count(distinct",
            "select variation_id",
            "eligible_devices",
            "meta_campaign",
            "cm1_eur",
        ):
            self.assertNotIn(forbidden, lowered)
        for marker in (
            "experimental_population_read': False",
            "arm_assignment_read': False",
            "outcome_metrics_read': False",
            "meta_dimensions_read': False",
            "performance_values_read': False",
            "reporting_row_counts_emitted': False",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_uploads_only_canonical_evidence_after_raw_cleanup(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        cleanup = WORKFLOW.index("Remove every temporary AWS response and log message")
        upload = WORKFLOW.index("Upload only canonical sanitized infrastructure-health evidence")
        self.assertLess(cleanup, upload)
        for marker in (
            "name: vevo-growthbook-production-aa-infra-health",
            "path: vevo-growthbook-production-aa-infra-health.json",
            "retention-days: 14",
            "canonical_evidence_bytes(evidence)",
            "validate_growthbook_aa_infra_health_evidence.py",
        ):
            self.assertIn(marker, WORKFLOW)
        for forbidden in (
            "path: ${TEMP_HEALTH_DIR}",
            "path: task-logs.json",
            "path: tasks.json",
            "path: alarms.json",
        ):
            self.assertNotIn(forbidden, WORKFLOW)

    def test_has_no_external_or_commerce_mutation_path(self) -> None:
        lowered = WORKFLOW.lower()
        for forbidden in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "register-task-definition",
            "scheduler update-schedule",
            "scheduler create-schedule",
            "s3api put-object",
            "s3api delete-object",
            "tagmanager",
            "ads_update",
            "adcreatives_create",
            "biznisweb_api_token",
            "curl ",
            "wget ",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn("'aws_resource_mutated': False", WORKFLOW)
        self.assertIn("'growthbook_mutated': False", WORKFLOW)
        self.assertIn("'workflow_or_experiment_gate_changed': False", WORKFLOW)


if __name__ == "__main__":
    unittest.main()
