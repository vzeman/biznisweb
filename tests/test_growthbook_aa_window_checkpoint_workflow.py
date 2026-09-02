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
    ROOT / ".github" / "workflows" / "check-vevo-growthbook-production-aa-window.yml"
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


class GrowthBookAaWindowCheckpointWorkflowTests(unittest.TestCase):
    def run_local_gate(
        self,
        *,
        event_name: str,
        now_utc: str,
        event_schedule: str = "",
        resolution_status: str | None = None,
        checkpoint_history: list[dict[str, object]] | None = None,
        historical_backfill: str = "false",
    ) -> tuple[int | None, str, str]:
        source = inline_python_block_containing("expected_schedule =")
        source = source.replace(
            "now = datetime.now(UTC)",
            "now = datetime.fromisoformat(os.environ['TEST_NOW_UTC'].replace('Z', '+00:00'))",
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            snapshot = json.loads(
                (ROOT / "projects/vevo/growthbook_aa_snapshot.json").read_text(
                    encoding="utf-8"
                )
            )
            if resolution_status is not None:
                snapshot["measurement_window"]["resolution_status"] = resolution_status
            if checkpoint_history is not None:
                snapshot["measurement_window"]["checkpoint_history"] = (
                    checkpoint_history
                )
            snapshot_path = temporary / "snapshot.json"
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
            github_env = temporary / "github.env"
            github_env.write_text("", encoding="utf-8")
            env = {
                "SNAPSHOT_MANIFEST": str(snapshot_path),
                "DEPLOY_EVIDENCE": str(
                    ROOT
                    / "projects/vevo/growthbook_production_reconciliation_deploy_evidence.json"
                ),
                "EVENT_NAME": event_name,
                "EVENT_SCHEDULE": event_schedule,
                "CONFIRM_HISTORICAL_BACKFILL": historical_backfill,
                "TEST_NOW_UTC": now_utc,
                "RUNNER_TEMP": str(temporary),
                "GITHUB_RUN_ID": "123456789",
                "GITHUB_ENV": str(github_env),
            }
            output = io.StringIO()
            exit_code: int | None = None
            with mock.patch.dict(os.environ, env, clear=False):
                with contextlib.chdir(ROOT), contextlib.redirect_stdout(output):
                    try:
                        exec(compile(source, "checkpoint-local-gate.py", "exec"))
                    except SystemExit as exc:
                        exit_code = exc.code if isinstance(exc.code, int) else 1
            return exit_code, output.getvalue(), github_env.read_text(encoding="utf-8")

    def test_every_inline_python_block_compiles(self) -> None:
        blocks = inline_python_blocks()
        self.assertGreaterEqual(len(blocks), 8)
        for block_index, source in enumerate(blocks):
            compile(source, f"checkpoint-workflow-inline-{block_index}.py", "exec")

    def test_main_only_time_and_closed_producer_gates_precede_aws(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_checkpoint:",
            "confirm_historical_backfill:",
            "- cron: '30 2 * * *'",
            "- cron: '30 3 * * *'",
            "EVENT_NAME: ${{ github.event_name }}",
            "EVENT_SCHEDULE: ${{ github.event.schedule }}",
            "PRODUCTION_AA_WINDOW_SCHEDULE_SKIP:reason=wrong-dst-slot:aws=false",
            "PRODUCTION_AA_WINDOW_SCHEDULE_SKIP:reason=before-first-due:aws=false",
            "PRODUCTION_AA_WINDOW_SCHEDULE_SKIP:reason=already-recorded:aws=false",
            "PRODUCTION_AA_WINDOW_SCHEDULE_SKIP:reason=window-resolved:aws=false",
            "validate_growthbook_aa_measurement_window.py",
            "exact historical backfill confirmation is required after the daily gate",
            "snapshot build opened before A/A window resolution",
            "producer opened before A/A window resolution",
            "CTA must remain unstarted during A/A checkpointing",
            "PRODUCTION_AA_WINDOW_LOCAL_GATE_OK:",
            "collection={collection_mode}:outcome-read=false:mutation=none",
            "uses: aws-actions/configure-aws-credentials@v6.1.0",
        ):
            self.assertIn(marker, WORKFLOW)
        gate = WORKFLOW.index("PRODUCTION_AA_WINDOW_LOCAL_GATE_OK:")
        credentials = WORKFLOW.index(
            "uses: aws-actions/configure-aws-credentials@v6.1.0"
        )
        self.assertLess(gate, credentials)

    def test_scheduled_gate_skips_before_due_wrong_dst_and_resolved_without_aws(
        self,
    ) -> None:
        scenarios = (
            {
                "now_utc": "2026-08-26T02:30:00Z",
                "event_schedule": "30 2 * * *",
                "resolution_status": None,
                "reason": "before-first-due",
            },
            {
                "now_utc": "2026-09-02T03:30:00Z",
                "event_schedule": "30 3 * * *",
                "resolution_status": None,
                "reason": "wrong-dst-slot",
            },
            {
                "now_utc": "2026-09-02T02:30:00Z",
                "event_schedule": "30 2 * * *",
                "resolution_status": "resolved",
                "reason": "window-resolved",
            },
        )
        for scenario in scenarios:
            with self.subTest(reason=scenario["reason"]):
                exit_code, output, github_env = self.run_local_gate(
                    event_name="schedule",
                    now_utc=str(scenario["now_utc"]),
                    event_schedule=str(scenario["event_schedule"]),
                    resolution_status=scenario["resolution_status"],
                )
                self.assertEqual(0, exit_code)
                self.assertIn(f"reason={scenario['reason']}:aws=false", output)
                self.assertEqual("RUN_CHECKPOINT=false\n", github_env)

    def test_scheduled_gate_derives_frozen_index_from_local_date(self) -> None:
        exit_code, output, github_env = self.run_local_gate(
            event_name="schedule",
            now_utc="2026-09-04T02:30:00Z",
            event_schedule="30 2 * * *",
            checkpoint_history=[],
        )
        self.assertIsNone(exit_code)
        self.assertIn(
            "PRODUCTION_AA_WINDOW_LOCAL_GATE_OK:collection=scheduled_daily:outcome-read=false:mutation=none",
            output,
        )
        self.assertIn("RUN_CHECKPOINT=false\n", github_env)
        self.assertIn("RUN_CHECKPOINT=true\n", github_env)
        self.assertIn("CHECKPOINT_INDEX=3\n", github_env)
        self.assertIn("CANDIDATE_THROUGH_UTC=2026-09-03T22:00:00Z\n", github_env)
        self.assertIn("CHECKPOINT_DUE_LOCAL=2026-09-04T03:45:00+02:00\n", github_env)

    def test_scheduled_gate_accepts_the_winter_utc_slot(self) -> None:
        exit_code, output, github_env = self.run_local_gate(
            event_name="schedule",
            now_utc="2026-11-02T03:30:00Z",
            event_schedule="30 3 * * *",
            checkpoint_history=[],
        )
        self.assertIsNone(exit_code)
        self.assertIn(
            "PRODUCTION_AA_WINDOW_LOCAL_GATE_OK:collection=scheduled_daily:outcome-read=false:mutation=none",
            output,
        )
        self.assertIn("CHECKPOINT_INDEX=62\n", github_env)
        self.assertIn("CANDIDATE_THROUGH_UTC=2026-11-01T23:00:00Z\n", github_env)
        self.assertIn("CHECKPOINT_DUE_LOCAL=2026-11-02T03:45:00+01:00\n", github_env)

    def test_scheduled_gate_skips_an_index_already_recorded(self) -> None:
        exit_code, output, github_env = self.run_local_gate(
            event_name="schedule",
            now_utc="2026-09-02T02:30:00Z",
            event_schedule="30 2 * * *",
            checkpoint_history=[{"evidence": {"window": {"checkpoint_index": 1}}}],
        )
        self.assertEqual(0, exit_code)
        self.assertIn("reason=already-recorded:aws=false", output)
        self.assertEqual("RUN_CHECKPOINT=false\n", github_env)

    def test_manual_gate_remains_next_history_index_and_requires_explicit_late_backfill(
        self,
    ) -> None:
        exit_code, _, github_env = self.run_local_gate(
            event_name="workflow_dispatch",
            now_utc="2026-09-02T02:30:00Z",
        )
        self.assertIsNone(exit_code)
        self.assertIn("CHECKPOINT_INDEX=1\n", github_env)
        self.assertIn("CHECKPOINT_COLLECTION_MODE=manual_same_window\n", github_env)
        exit_code, _, github_env = self.run_local_gate(
            event_name="workflow_dispatch",
            now_utc="2026-09-04T02:30:00Z",
        )
        self.assertNotEqual(0, exit_code)
        self.assertEqual("RUN_CHECKPOINT=false\n", github_env)

        exit_code, output, github_env = self.run_local_gate(
            event_name="workflow_dispatch",
            now_utc="2026-09-04T02:30:00Z",
            historical_backfill="true",
        )
        self.assertIsNone(exit_code)
        self.assertIn("collection=manual_historical_backfill", output)
        self.assertIn("CHECKPOINT_INDEX=1\n", github_env)
        self.assertIn("CANDIDATE_THROUGH_UTC=2026-09-01T22:00:00Z\n", github_env)
        self.assertIn(
            "CHECKPOINT_COLLECTION_MODE=manual_historical_backfill\n", github_env
        )

    def test_manual_gate_rejects_backfill_confirmation_inside_original_window(
        self,
    ) -> None:
        exit_code, _, github_env = self.run_local_gate(
            event_name="workflow_dispatch",
            now_utc="2026-09-02T02:30:00Z",
            historical_backfill="true",
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
            "PRODUCTION_AA_WINDOW_RUNTIME_GATE_OK:",
            "PRODUCTION_AA_WINDOW_CONTROL_GATE_OK:",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn("collector_outputs['CollectorClusterArn']", WORKFLOW)
        self.assertNotIn("aws ecs list-tasks", WORKFLOW)

    def test_expired_runtime_state_is_not_misrepresented_as_a_live_ip_gate(
        self,
    ) -> None:
        for marker in (
            "'schema_version': 3",
            "'collection_mode': os.environ['CHECKPOINT_COLLECTION_MODE']",
            "'private_ip': os.environ['RECONCILIATION_PRIVATE_IP'] or None",
            "'identity_source': os.environ['RECONCILIATION_RUNTIME_IDENTITY_SOURCE']",
            "runtime_source = 'cloudtrail_run_task_retention_recovery'",
            "runtime_retained = False",
            "runtime_source = 'ecs_stopped_task'",
            "runtime_retained = True",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn(
            "detail_value(cloudtrail_task, 'privateIPv4Address')", WORKFLOW
        )

    def test_runtime_selection_handles_retained_and_expired_ecs_state(self) -> None:
        source = inline_python_block_containing(
            "runtime_source = 'cloudtrail_run_task_retention_recovery'"
        )
        task_id = "b" * 32
        task_definition_arn = (
            "arn:aws:ecs:eu-central-1:919341186960:task-definition/"
            "vevo-growthbook-reconcile-production:3"
        )
        task_arn = "arn:aws:ecs:eu-central-1:919341186960:task/cluster/" + task_id
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
            "CHECKPOINT_START_UTC": "2026-09-02T01:45:00Z",
            "CHECKPOINT_END_UTC": "2026-09-02T03:45:00Z",
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
                            "startedAt": "2026-09-02T01:45:02Z",
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
                            "containers": [{"exitCode": 0, "imageDigest": digest}],
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
                            exec(
                                compile(
                                    source, "checkpoint-runtime-selection.py", "exec"
                                )
                            )
                    actual_lines = output.getvalue().splitlines()
                    for expected_line in expected_lines:
                        self.assertIn(expected_line, actual_lines)

    def test_population_query_returns_only_one_outcome_blind_aggregate(self) -> None:
        query_start = WORKFLOW.index(
            "SELECT COUNT(DISTINCT device_id) AS eligible_devices"
        )
        query_end = WORKFLOW.index('""".strip()', query_start)
        query = WORKFLOW[query_start:query_end]
        self.assertIn("FROM experiment_device_facts", query)
        self.assertIn("eligible = 1", query)
        self.assertIn("contaminated = 0", query)
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
            "only_aggregate_count_retained': True",
            "arm_counts_read': False",
            "arm_outcomes_read': False",
            "outcome_metrics_read': False",
            "PRODUCTION_AA_WINDOW_POPULATION_OK:aggregate-only=true:arm-outcomes=false",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_athena_failure_emits_only_structured_sanitized_metadata(self) -> None:
        for marker in (
            "athena-query-start.json",
            "athena-query-status.json",
            "PRODUCTION_AA_WINDOW_ATHENA_QUERY_SUBMITTED:id-valid=true:raw-id-emitted=false",
            "re.fullmatch(r'[0-9a-f-]{36}', query_id)",
            "state not in {'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'}",
            "PRODUCTION_AA_WINDOW_ATHENA_ERROR:",
            "reason_classes = (",
            "reason_sha256 = hashlib.sha256(reason.encode()).hexdigest()",
            "reason-class={reason_class}:reason-sha256={reason_sha256}",
            "raw-reason-emitted=false",
            "structured_available = (",
        ):
            self.assertIn(marker, WORKFLOW)
        for forbidden in (
            "print(reason)",
            "cat \"${TEMP_CHECKPOINT_DIR}/athena-query-status.json\"",
            "echo \"${QUERY_ID}\"",
            "StateChangeReason: text",
        ):
            self.assertNotIn(forbidden, WORKFLOW)

    def test_uploads_only_one_canonical_sanitized_artifact_after_cleanup(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        cleanup = WORKFLOW.index("Remove every temporary AWS response and query file")
        upload = WORKFLOW.index(
            "Upload only sanitized outcome-blind checkpoint evidence"
        )
        self.assertLess(cleanup, upload)
        for marker in (
            "name: vevo-growthbook-aa-window-checkpoint",
            "path: vevo-growthbook-aa-window-checkpoint.json",
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
        self.assertIn(
            "GrowthBook, GTM, Meta Ads, BiznisWeb and commerce mutation: `none`",
            WORKFLOW,
        )
        self.assertIn("Snapshot/producer/CTA/winner gates changed: `none`", WORKFLOW)


if __name__ == "__main__":
    unittest.main()
