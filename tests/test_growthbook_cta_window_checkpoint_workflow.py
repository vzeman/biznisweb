from __future__ import annotations

import contextlib
import io
import json
import os
import pathlib
import tempfile
import textwrap
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "check-vevo-growthbook-production-cta-window.yml"
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


def inline_python_block_containing(marker: str) -> str:
    matches = [source for source in inline_python_blocks() if marker in source]
    if len(matches) != 1:
        raise AssertionError(f"expected one inline Python block containing {marker!r}")
    return matches[0]


class GrowthBookCtaWindowCheckpointWorkflowTests(unittest.TestCase):
    def run_local_gate(
        self,
        *,
        event_name: str,
        now_utc: str,
        event_schedule: str = "",
        running_window: bool = False,
        manifest_status: str | None = None,
        checkpoint_history: list[dict[str, object]] | None = None,
    ) -> tuple[int | None, str, str]:
        source = inline_python_block_containing("expected_schedule =")
        source = source.replace(
            "now = datetime.now(UTC)",
            "now = datetime.fromisoformat(os.environ['TEST_NOW_UTC'].replace('Z', '+00:00'))",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            vevo = temporary / "projects" / "vevo"
            vevo.mkdir(parents=True)
            if running_window:
                from tests.test_growthbook_cta_window_checkpoint import (
                    GrowthBookCtaWindowCheckpointTests,
                )

                fixture = GrowthBookCtaWindowCheckpointTests(methodName="runTest")
                fixture.setUp()
                manifest = json.loads(json.dumps(fixture.running))
                files = {
                    "growthbook_cta_activation.json": fixture.activation,
                    "growthbook_cta_activation_observation.json": fixture.start_observation,
                    "growthbook_cta_sample_plan.json": fixture.sample,
                    "growthbook_cta_decision_contract.json": fixture.contract,
                    "growthbook_production_reconciliation_deploy_evidence.json": fixture.reconciliation,
                    "growthbook_workspace.json": fixture.running_workspace,
                }
            else:
                manifest = json.loads(
                    (
                        ROOT
                        / "projects/vevo/growthbook_cta_measurement_window.json"
                    ).read_text(encoding="utf-8")
                )
                files = {
                    "growthbook_production_reconciliation_deploy_evidence.json": json.loads(
                        (
                            ROOT
                            / "projects/vevo/growthbook_production_reconciliation_deploy_evidence.json"
                        ).read_text(encoding="utf-8")
                    )
                }
            if manifest_status is not None:
                manifest["status"] = manifest_status
            if checkpoint_history is not None:
                manifest["measurement_window"]["checkpoint_history"] = checkpoint_history
            for name, value in files.items():
                (vevo / name).write_text(json.dumps(value), encoding="utf-8")
            manifest_path = vevo / "growthbook_cta_measurement_window.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            github_env = temporary / "github.env"
            github_env.write_text("", encoding="utf-8")
            env = {
                "CTA_WINDOW_MANIFEST": str(manifest_path),
                "DEPLOY_EVIDENCE": str(
                    vevo
                    / "growthbook_production_reconciliation_deploy_evidence.json"
                ),
                "EVENT_NAME": event_name,
                "EVENT_SCHEDULE": event_schedule,
                "TEST_NOW_UTC": now_utc,
                "RUNNER_TEMP": str(temporary),
                "GITHUB_RUN_ID": "52345678901",
                "GITHUB_ENV": str(github_env),
            }
            output = io.StringIO()
            exit_code: int | None = None
            with mock.patch.dict(os.environ, env, clear=False):
                with contextlib.chdir(temporary), contextlib.redirect_stdout(output):
                    try:
                        namespace: dict[str, object] = {"__name__": "__main__"}
                        exec(
                            compile(source, "cta-checkpoint-local-gate.py", "exec"),
                            namespace,
                            namespace,
                        )
                    except SystemExit as exc:
                        exit_code = exc.code if isinstance(exc.code, int) else 1
            return exit_code, output.getvalue(), github_env.read_text(encoding="utf-8")

    def test_every_inline_python_block_compiles(self) -> None:
        blocks = inline_python_blocks()
        self.assertGreaterEqual(len(blocks), 8)
        for block_index, source in enumerate(blocks):
            compile(source, f"cta-checkpoint-inline-{block_index}.py", "exec")

    def test_main_only_frozen_stop_rule_gate_precedes_aws(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_checkpoint:",
            "- cron: '30 2 * * *'",
            "- cron: '30 3 * * *'",
            "EVENT_NAME: ${{ github.event_name }}",
            "EVENT_SCHEDULE: ${{ github.event.schedule }}",
            "PRODUCTION_CTA_WINDOW_SCHEDULE_SKIP:reason=window-not-open:aws=false",
            "PRODUCTION_CTA_WINDOW_SCHEDULE_SKIP:reason=wrong-dst-slot:aws=false",
            "PRODUCTION_CTA_WINDOW_SCHEDULE_SKIP:reason=before-first-due:aws=false",
            "PRODUCTION_CTA_WINDOW_SCHEDULE_SKIP:reason=after-maximum-due:aws=false",
            "PRODUCTION_CTA_WINDOW_SCHEDULE_SKIP:reason=already-recorded:aws=false",
            "validate_growthbook_cta_measurement_window.py",
            "CTA outcome-blind window is not open",
            "CTA assignment stop boundary opened before stopping rule",
            "A/A must remain stopped during CTA checkpointing",
            "CTA running Production allocation drift",
            "outcome-blind CTA checkpoint is outside its daily gate",
            "PRODUCTION_CTA_WINDOW_LOCAL_GATE_OK:assignment=running:arm-read=false:outcome-read=false:mutation=none",
            "uses: aws-actions/configure-aws-credentials@v6.1.0",
        ):
            self.assertIn(marker, WORKFLOW)
        gate = WORKFLOW.index("PRODUCTION_CTA_WINDOW_LOCAL_GATE_OK:")
        credentials = WORKFLOW.index(
            "uses: aws-actions/configure-aws-credentials@v6.1.0"
        )
        self.assertLess(gate, credentials)

    def test_schedule_skips_closed_window_before_aws(self) -> None:
        exit_code, output, github_env = self.run_local_gate(
            event_name="schedule",
            now_utc="2026-09-19T02:30:00Z",
            event_schedule="30 2 * * *",
        )
        self.assertEqual(0, exit_code)
        self.assertIn("reason=window-not-open:aws=false", output)
        self.assertEqual("RUN_CHECKPOINT=false\n", github_env)

    def test_schedule_skips_pre_due_wrong_dst_and_resolved_without_aws(self) -> None:
        scenarios = (
            {
                "now_utc": "2026-09-18T02:30:00Z",
                "event_schedule": "30 2 * * *",
                "manifest_status": None,
                "reason": "before-first-due",
            },
            {
                "now_utc": "2026-09-19T03:30:00Z",
                "event_schedule": "30 3 * * *",
                "manifest_status": None,
                "reason": "wrong-dst-slot",
            },
            {
                "now_utc": "2026-09-19T02:30:00Z",
                "event_schedule": "30 2 * * *",
                "manifest_status": "cta_assignment_stop_review_open_by_preregistered_rule",
                "reason": "window-not-open",
            },
        )
        for scenario in scenarios:
            with self.subTest(reason=scenario["reason"]):
                exit_code, output, github_env = self.run_local_gate(
                    event_name="schedule",
                    now_utc=str(scenario["now_utc"]),
                    event_schedule=str(scenario["event_schedule"]),
                    running_window=True,
                    manifest_status=scenario["manifest_status"],
                )
                self.assertEqual(0, exit_code)
                self.assertIn(f"reason={scenario['reason']}:aws=false", output)
                self.assertEqual("RUN_CHECKPOINT=false\n", github_env)

    def test_schedule_derives_index_from_frozen_local_date(self) -> None:
        exit_code, output, github_env = self.run_local_gate(
            event_name="schedule",
            now_utc="2026-09-21T02:30:00Z",
            event_schedule="30 2 * * *",
            running_window=True,
            checkpoint_history=[],
        )
        self.assertIsNone(exit_code)
        self.assertIn(
            "PRODUCTION_CTA_WINDOW_LOCAL_GATE_OK:assignment=running:arm-read=false:outcome-read=false:mutation=none",
            output,
        )
        self.assertIn("RUN_CHECKPOINT=false\n", github_env)
        self.assertIn("RUN_CHECKPOINT=true\n", github_env)
        self.assertIn("CHECKPOINT_INDEX=3\n", github_env)
        self.assertIn("CANDIDATE_THROUGH_UTC=2026-09-20T22:00:00Z\n", github_env)
        self.assertIn("CHECKPOINT_DUE_LOCAL=2026-09-21T03:45:00+02:00\n", github_env)

    def test_schedule_skips_recorded_index_and_post_maximum_winter_slot(self) -> None:
        exit_code, output, github_env = self.run_local_gate(
            event_name="schedule",
            now_utc="2026-09-19T02:30:00Z",
            event_schedule="30 2 * * *",
            running_window=True,
            checkpoint_history=[
                {"evidence": {"window": {"checkpoint_index": 1}}}
            ],
        )
        self.assertEqual(0, exit_code)
        self.assertIn("reason=already-recorded:aws=false", output)
        self.assertEqual("RUN_CHECKPOINT=false\n", github_env)

        exit_code, output, github_env = self.run_local_gate(
            event_name="schedule",
            now_utc="2026-11-02T03:30:00Z",
            event_schedule="30 3 * * *",
            running_window=True,
        )
        self.assertEqual(0, exit_code)
        self.assertIn("reason=after-maximum-due:aws=false", output)
        self.assertEqual("RUN_CHECKPOINT=false\n", github_env)

    def test_manual_gate_remains_next_history_index_and_exact_daily_window(self) -> None:
        exit_code, _, github_env = self.run_local_gate(
            event_name="workflow_dispatch",
            now_utc="2026-09-19T02:30:00Z",
            running_window=True,
        )
        self.assertIsNone(exit_code)
        self.assertIn("CHECKPOINT_INDEX=1\n", github_env)
        exit_code, _, github_env = self.run_local_gate(
            event_name="workflow_dispatch",
            now_utc="2026-09-21T02:30:00Z",
            running_window=True,
        )
        self.assertNotEqual(0, exit_code)
        self.assertEqual("RUN_CHECKPOINT=false\n", github_env)

    def test_all_credential_and_post_gate_steps_require_run_checkpoint(self) -> None:
        step_names = (
            "Configure AWS credentials for bounded read-only checkpoint",
            "Read exact stack schedule and reconciliation task identity",
            "Verify exact scheduled success marker alarms and DLQ",
            "Query only the cumulative eligible-device count",
            "Build and independently validate sanitized checkpoint evidence",
            "Upload only sanitized outcome-blind checkpoint evidence",
            "Publish checkpoint summary",
        )
        for step_name in step_names:
            start = WORKFLOW.index(f"- name: {step_name}")
            next_step = WORKFLOW.find("\n      - name:", start + 1)
            block = WORKFLOW[start:] if next_step == -1 else WORKFLOW[start:next_step]
            self.assertIn("if: ${{ env.RUN_CHECKPOINT == 'true' }}", block)
        cleanup_start = WORKFLOW.index(
            "- name: Remove every temporary AWS response and query file"
        )
        cleanup_end = WORKFLOW.index("\n      - name:", cleanup_start + 1)
        self.assertIn(
            "if: ${{ always() && env.RUN_CHECKPOINT == 'true' }}",
            WORKFLOW[cleanup_start:cleanup_end],
        )

    def test_runtime_schedule_marker_alarm_and_dlq_gates_are_exact(self) -> None:
        for marker in (
            "21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb",
            "cron(45 3 * * ? *)",
            "Europe/Bratislava",
            "vevo-reporting-daily:33",
            "collector.get('StackStatus') != 'UPDATE_COMPLETE'",
            "reconciliation.get('StackStatus') != 'UPDATE_COMPLETE'",
            "reconciliation_parameters.get('ClusterArn')",
            "source.get('Target', {}).get('Arn') != reconciliation_parameters.get('ClusterArn')",
            "'RECONCILIATION_CLUSTER_ARN': reconciliation_parameters['ClusterArn']",
            '--cluster "${RECONCILIATION_CLUSTER_ARN}"',
            "task.get('group') == os.environ['EXPECTED_SERVICE']",
            "scheduled reconciliation image differs from localhost-gated deploy evidence",
            "aws logs filter-log-events",
            "aws cloudtrail lookup-events",
            "expected one exact Scheduler CloudTrail RunTask event",
            "cloudtrail_run_task_retention_recovery",
            "RECONCILIATION_RUNTIME_IDENTITY_SOURCE",
            "RECONCILIATION_RUNTIME_STATE_RETAINED",
            "'scheduler_run_task_verified': True",
            "'runtime_state_retained':",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:",
            "scheduled reconciliation generated/published parity drift",
            "Production reconciliation alarm gate failed",
            "Production reconciliation DLQ is not empty",
            "PRODUCTION_CTA_WINDOW_RUNTIME_GATE_OK:",
            "PRODUCTION_CTA_WINDOW_CONTROL_GATE_OK:",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn("collector_outputs['CollectorClusterArn']", WORKFLOW)
        self.assertNotIn("aws ecs list-tasks", WORKFLOW)

    def test_expired_runtime_state_is_not_misrepresented_as_a_live_ip_gate(
        self,
    ) -> None:
        for marker in (
            "'schema_version': 2",
            "'private_ip': os.environ['RECONCILIATION_PRIVATE_IP'] or None",
            "'identity_source': os.environ['RECONCILIATION_RUNTIME_IDENTITY_SOURCE']",
            "runtime_source = 'cloudtrail_run_task_retention_recovery'",
            "runtime_retained = False",
            "runtime_source = 'ecs_stopped_task'",
            "runtime_retained = True",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_runtime_selection_handles_retained_and_expired_ecs_state(self) -> None:
        source = inline_python_block_containing(
            "runtime_source = 'cloudtrail_run_task_retention_recovery'"
        )
        task_id = "b" * 32
        task_definition_arn = (
            "arn:aws:ecs:eu-central-1:919341186960:task-definition/"
            "vevo-growthbook-reconcile-production:3"
        )
        task_arn = (
            "arn:aws:ecs:eu-central-1:919341186960:task/cluster/" + task_id
        )
        digest = "sha256:" + "c" * 64
        cloudtrail = {
            "responseElements": {
                "tasks": [
                    {
                        "taskArn": task_arn,
                        "taskDefinitionArn": task_definition_arn,
                        "group": "vevo-growthbook-reconcile-production",
                    }
                ]
            }
        }
        base_env = {
            "CHECKPOINT_START_UTC": "2026-09-19T01:45:00Z",
            "CHECKPOINT_END_UTC": "2026-09-19T03:45:00Z",
            "RECONCILIATION_TASK_ID": task_id,
            "RECONCILIATION_TASK_DEFINITION_ARN": task_definition_arn,
            "EXPECTED_SERVICE": "vevo-growthbook-reconcile-production",
            "RECONCILIATION_IMAGE_DIGEST": digest,
            "RECONCILIATION_LOG_STREAM": f"service/container/{task_id}",
        }
        fixtures = (
            (
                {"tasks": []},
                (
                    "RECONCILIATION_PRIVATE_IP=",
                    "RECONCILIATION_RUNTIME_IDENTITY_SOURCE="
                    "cloudtrail_run_task_retention_recovery",
                    "RECONCILIATION_RUNTIME_STATE_RETAINED=false",
                ),
            ),
            (
                {
                    "tasks": [
                        {
                            "taskArn": task_arn,
                            "taskDefinitionArn": task_definition_arn,
                            "group": "vevo-growthbook-reconcile-production",
                            "startedAt": "2026-09-19T01:45:02Z",
                            "attachments": [
                                {
                                    "details": [
                                        {
                                            "name": "privateIPv4Address",
                                            "value": "172.31.10.20",
                                        }
                                    ]
                                }
                            ],
                            "containers": [
                                {"exitCode": 0, "imageDigest": digest}
                            ],
                        }
                    ]
                },
                (
                    "RECONCILIATION_PRIVATE_IP=172.31.10.20",
                    "RECONCILIATION_RUNTIME_IDENTITY_SOURCE=ecs_stopped_task",
                    "RECONCILIATION_RUNTIME_STATE_RETAINED=true",
                ),
            ),
        )
        for tasks, expected_lines in fixtures:
            with self.subTest(runtime_retained=bool(tasks["tasks"])):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    root = pathlib.Path(temporary_directory)
                    (root / "tasks.json").write_text(
                        json.dumps(tasks), encoding="utf-8"
                    )
                    (root / "selected-cloudtrail-event.json").write_text(
                        json.dumps(cloudtrail), encoding="utf-8"
                    )
                    output = io.StringIO()
                    env = {**base_env, "TEMP_CHECKPOINT_DIR": temporary_directory}
                    with mock.patch.dict(os.environ, env, clear=False):
                        with contextlib.redirect_stdout(output):
                            exec(compile(source, "cta-runtime-selection.py", "exec"))
                    actual_lines = output.getvalue().splitlines()
                    for expected_line in expected_lines:
                        self.assertIn(expected_line, actual_lines)

    def test_population_query_is_one_cta_count_without_arm_or_outcome(self) -> None:
        query_start = WORKFLOW.index(
            "SELECT COUNT(DISTINCT device_id) AS eligible_devices"
        )
        query_end = WORKFLOW.index('""".strip()', query_start)
        query = WORKFLOW[query_start:query_end]
        self.assertIn("FROM experiment_device_facts", query)
        self.assertIn("experiment_id = 'vevo-sk-product-cta-color-001'", query)
        self.assertIn("eligible = 1", query)
        self.assertIn("contaminated = 0", query)
        self.assertIn("CTA_STARTED_AT_UTC", query)
        for forbidden in (
            "variation_id",
            "add_to_cart",
            "purchase",
            "revenue",
            "cm1_eur",
            "lcp",
            "inp",
            "cls",
            "meta_campaign",
        ):
            self.assertNotIn(forbidden, query.lower())
        for marker in (
            "header != ['eligible_devices']",
            "target_total_sample': int(os.environ['TARGET_TOTAL_SAMPLE'])",
            "only_aggregate_count_retained': True",
            "arm_counts_read': False",
            "arm_outcomes_read': False",
            "outcome_metrics_read': False",
            "PRODUCTION_CTA_WINDOW_POPULATION_OK:aggregate-only=true:arm-outcomes=false",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_decision_is_target_or_day_42_and_only_opens_manual_review(self) -> None:
        for marker in (
            "open_manual_stop_review_target_reached",
            "open_manual_stop_review_maximum_duration_reached",
            "if 14 + index - 1 >= 42",
            "if population['eligible_devices'] >= population['target_total_sample']",
            "else 'extend_one_full_local_day'",
            "assignment_stopped': False",
            "winner_calls': False",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn("evaluate_growthbook_cta.py", WORKFLOW)

    def test_uploads_only_canonical_identity_free_artifact_after_cleanup(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        cleanup = WORKFLOW.index("Remove every temporary AWS response and query file")
        upload = WORKFLOW.index(
            "Upload only sanitized outcome-blind checkpoint evidence"
        )
        self.assertLess(cleanup, upload)
        for marker in (
            "name: vevo-growthbook-cta-window-checkpoint",
            "path: vevo-growthbook-cta-window-checkpoint.json",
            "retention-days: 90",
            "canonical_evidence_bytes(evidence)",
            "validate_checkpoint_evidence(evidence, expected, index)",
        ):
            self.assertIn(marker, WORKFLOW)
        for forbidden in (
            "path: ${TEMP_CHECKPOINT_DIR}",
            "path: task-logs.json",
            "path: eligible-count-result.json",
            "path: tasks.json",
        ):
            self.assertNotIn(forbidden, WORKFLOW)

    def test_has_no_external_assignment_or_commerce_mutation_path(self) -> None:
        lowered = WORKFLOW.lower()
        for forbidden in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "ecs stop-task",
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
        self.assertIn(
            "GrowthBook, GTM, Meta Ads, BiznisWeb and commerce mutation: `none`",
            WORKFLOW,
        )
        self.assertIn(
            "Assignment stop/winner/external mutation gates changed: `none`",
            WORKFLOW,
        )


if __name__ == "__main__":
    unittest.main()
