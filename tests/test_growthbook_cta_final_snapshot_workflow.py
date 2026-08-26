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

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "build-vevo-growthbook-production-cta-final-snapshot.yml"
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


class GrowthBookCtaFinalSnapshotWorkflowTests(unittest.TestCase):
    def test_yaml_and_every_inline_python_block_are_valid(self) -> None:
        payload = yaml.safe_load(WORKFLOW)
        self.assertEqual(
            {"contents": "read", "actions": "read"}, payload["permissions"]
        )
        self.assertEqual(
            "${{ github.ref == 'refs/heads/main' }}",
            payload["jobs"]["build-final-snapshot"]["if"],
        )
        blocks = inline_python_blocks()
        self.assertGreaterEqual(len(blocks), 10)
        for block_index, source in enumerate(blocks):
            compile(source, f"cta-final-workflow-inline-{block_index}.py", "exec")

    def test_single_final_look_and_due_gate_precede_credentials(self) -> None:
        for marker in (
            "confirm_final_snapshot:",
            "[[ \"${GITHUB_RUN_ATTEMPT}\" == '1' ]]",
            "followup_pending_final_look_locked_until_due",
            "CTA 14-day outcome maturity is not complete",
            "diagnostic_host_gate_task_allowed",
            "outcome_metrics_read_allowed",
            "PRODUCTION_CTA_FINAL_LOCAL_GATE_OK:due=true:one-look=true:mutation=none",
            "a prior CTA final outcome-query attempt already exists",
            "uses: aws-actions/configure-aws-credentials@v6.1.0",
        ):
            self.assertIn(marker, WORKFLOW)
        local_gate = WORKFLOW.index("PRODUCTION_CTA_FINAL_LOCAL_GATE_OK:")
        prior_run_gate = WORKFLOW.index("PRODUCTION_CTA_FINAL_SINGLE_LOOK_OK:")
        credentials = WORKFLOW.index(
            "uses: aws-actions/configure-aws-credentials@v6.1.0"
        )
        self.assertLess(local_gate, prior_run_gate)
        self.assertLess(prior_run_gate, credentials)

    def test_host_identity_localhost_markers_and_reconciliation_precede_query(self) -> None:
        for marker in (
            "CTA_FINAL_PREQUERY_CONTEXT_OK:instance-id=N/A:Fargate:private-ip=",
            "service=${EXPECTED_SERVICE}:path=${EXPECTED_RUNTIME_PATH}",
            "no successful post-due Production reconciliation marker exists",
            "source reporting schedule drift",
            "collector.get('StackStatus') != 'UPDATE_COMPLETE'",
            "reconciliation.get('StackStatus') != 'UPDATE_COMPLETE'",
            "reconciliation_parameters.get('ClusterArn')",
            "source.get('Target', {}).get('Arn') != reconciliation_parameters.get('ClusterArn')",
            "'RECONCILIATION_CLUSTER_ARN': reconciliation_parameters['ClusterArn']",
            '--cluster "${RECONCILIATION_CLUSTER_ARN}"',
            "aws logs filter-log-events",
            "aws cloudtrail lookup-events",
            "expected one exact Scheduler CloudTrail RunTask event",
            "cloudtrail_run_task_retention_recovery",
            "RECONCILIATION_RUNTIME_IDENTITY_SOURCE",
            "RECONCILIATION_RUNTIME_STATE_RETAINED",
            "str(exact_task.get('startedBy') or '').startswith('cta-final-')",
            "scheduler-run-task=true",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:",
            "CTA_FINAL_RECONCILIATION_GATE_OK:marker=true:parity=true:alarms=clear:dlq=empty",
            "aws ecs run-task",
            "/app/scripts/growthbook_reconcile_host_gate.sh",
            "GROWTHBOOK_RECONCILE_LOCALHOST_HEALTH_OK:production:",
            "GROWTHBOOK_RECONCILE_LOCALHOST_MARKER_OK:/app:",
            "CTA_FINAL_HOST_GATE_OK:instance-id=N/A:Fargate:private-ip=",
            "localhost-health=true:localhost-marker=true",
            "CTA_FINAL_GLUE_SCHEMA_OK:pii-fields=absent:customer-fields=absent:ip-fields=absent",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn("collector_outputs['CollectorClusterArn']", WORKFLOW)
        self.assertNotIn("aws ecs list-tasks", WORKFLOW)
        host_gate = WORKFLOW.index("CTA_FINAL_HOST_GATE_OK:")
        schema_gate = WORKFLOW.index("CTA_FINAL_GLUE_SCHEMA_OK:")
        query = WORKFLOW.index("aws athena start-query-execution")
        self.assertLess(host_gate, schema_gate)
        self.assertLess(schema_gate, query)

    def test_expired_historical_state_cannot_replace_the_live_host_gate(self) -> None:
        for marker in (
            "private-ip=${RECONCILIATION_PRIVATE_IP:-expired}",
            "runtime_source = 'cloudtrail_run_task_retention_recovery'",
            "runtime_retained = False",
            "runtime_source = 'ecs_stopped_task'",
            "runtime_retained = True",
            "private-ip=${HOST_PRIVATE_IP}",
            "localhost-health=true:localhost-marker=true",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertLess(
            WORKFLOW.index("CTA_FINAL_PREQUERY_CONTEXT_OK:"),
            WORKFLOW.index("aws ecs run-task"),
        )
        self.assertLess(
            WORKFLOW.index("CTA_FINAL_HOST_GATE_OK:"),
            WORKFLOW.index("aws athena start-query-execution"),
        )

    def test_historical_runtime_selection_handles_retained_and_expired_ecs_state(
        self,
    ) -> None:
        source = inline_python_block_containing(
            "runtime_source = 'cloudtrail_run_task_retention_recovery'"
        )
        task_id = "d" * 32
        task_definition_arn = (
            "arn:aws:ecs:eu-central-1:919341186960:task-definition/"
            "vevo-growthbook-reconcile-production:3"
        )
        task_arn = (
            "arn:aws:ecs:eu-central-1:919341186960:task/cluster/" + task_id
        )
        digest = "sha256:" + "e" * 64
        log_stream = f"service/container/{task_id}"
        cloudtrail = {
            "responseElements": {
                "tasks": [
                    {
                        "taskArn": task_arn,
                        "clusterArn": (
                            "arn:aws:ecs:eu-central-1:919341186960:cluster/"
                            "vevo-reporting"
                        ),
                        "taskDefinitionArn": task_definition_arn,
                        "group": "vevo-growthbook-reconcile-production",
                    }
                ]
            }
        }
        base_env = {
            "FINAL_RECONCILIATION_START_UTC": "2026-10-20T01:45:00Z",
            "FINAL_RECONCILIATION_END_UTC": "2026-10-21T03:45:00Z",
            "RECONCILIATION_TASK_ID": task_id,
            "RECONCILIATION_CLUSTER_ARN": (
                "arn:aws:ecs:eu-central-1:919341186960:cluster/vevo-reporting"
            ),
            "RECONCILIATION_TASK_DEFINITION_ARN": task_definition_arn,
            "EXPECTED_SERVICE": "vevo-growthbook-reconcile-production",
            "RECONCILIATION_IMAGE_DIGEST": digest,
            "RECONCILIATION_LOG_STREAM": log_stream,
        }
        fixtures = (
            (
                {"tasks": [], "failures": [{"reason": "MISSING"}]},
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
                            "clusterArn": (
                                "arn:aws:ecs:eu-central-1:919341186960:cluster/"
                                "vevo-reporting"
                            ),
                            "taskDefinitionArn": task_definition_arn,
                            "group": "vevo-growthbook-reconcile-production",
                            "launchType": "FARGATE",
                            "startedBy": "scheduler",
                            "startedAt": "2026-10-21T01:45:02Z",
                            "stoppedAt": "2026-10-21T01:46:02Z",
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
                                {
                                    "exitCode": 0,
                                    "imageDigest": digest,
                                    "logStreamName": log_stream,
                                }
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
                    env = {**base_env, "TEMP_FINAL_DIR": temporary_directory}
                    with mock.patch.dict(os.environ, env, clear=False):
                        with contextlib.redirect_stdout(output):
                            exec(
                                compile(
                                    source,
                                    "cta-final-runtime-selection.py",
                                    "exec",
                                )
                            )
                    actual_lines = output.getvalue().splitlines()
                    for expected_line in expected_lines:
                        self.assertIn(expected_line, actual_lines)

    def test_runs_one_aggregate_query_and_offline_final_evaluator(self) -> None:
        self.assertEqual(1, WORKFLOW.count("aws athena start-query-execution"))
        for marker in (
            "build_growthbook_cta_final_snapshot.py render-query",
            "--max-results 3",
            "build_growthbook_cta_final_snapshot.py build",
            "evaluate_growthbook_cta.py",
            "--require-final",
            "two aggregate variation rows only",
            "automatic-mutation=false",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_uploads_only_two_canonical_identity_free_artifacts_after_cleanup(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        cleanup = WORKFLOW.index(
            "Remove every temporary AWS response query and host-gate file"
        )
        upload = WORKFLOW.index(
            "Upload only the aggregate snapshot and offline decision"
        )
        self.assertLess(cleanup, upload)
        for marker in (
            "name: vevo-growthbook-cta-final-snapshot",
            "vevo-growthbook-cta-final-snapshot.json",
            "vevo-growthbook-cta-final-decision.json",
            "retention-days: 90",
            "CTA final artifact contains an identity field",
        ):
            self.assertIn(marker, WORKFLOW)
        for forbidden in (
            "path: ${TEMP_FINAL_DIR}",
            "path: athena-results.json",
            "path: host-gate-logs.json",
            "path: scheduled-logs.json",
        ):
            self.assertNotIn(forbidden, WORKFLOW)

    def test_has_no_external_control_plane_or_commerce_mutation_path(self) -> None:
        lowered = WORKFLOW.lower()
        self.assertEqual(1, lowered.count("aws ecs run-task"))
        for forbidden in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "ecs update-service",
            "ecs stop-task",
            "register-task-definition",
            "scheduler update-schedule",
            "scheduler create-schedule",
            "s3api put-object",
            "s3api delete-object",
            "glue create-",
            "glue update-",
            "glue delete-",
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
        self.assertIn("Automatic winner application: `false`", WORKFLOW)


if __name__ == "__main__":
    unittest.main()
