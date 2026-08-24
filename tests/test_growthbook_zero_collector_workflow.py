from __future__ import annotations

import pathlib
import textwrap
import unittest

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    ROOT / ".github" / "workflows" / "verify-vevo-growthbook-zero-collector.yml"
)
WORKFLOW = WORKFLOW_PATH.read_text(encoding="utf-8")


class GrowthBookZeroCollectorWorkflowTests(unittest.TestCase):
    def test_is_main_only_confirmed_and_frozen_before_aws(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_observation:",
            '[[ "${CONFIRM_OBSERVATION}" == \'true\' ]]',
            "OBSERVATION_FROM_UTC: '2026-08-24T04:30:00Z'",
            "OBSERVATION_THROUGH_UTC: '2026-08-24T04:50:00Z'",
            "owned-storage cleanup evidence is missing",
            "zero-collector observation is already recorded",
            "python scripts/validate_growthbook_production_aa_activation.py",
            "ZERO_COLLECTOR_LOCAL_GATE_OK:",
            "Configure AWS credentials for bounded read-only observation",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn("python -m unittest", WORKFLOW)
        self.assertLess(
            WORKFLOW.index("ZERO_COLLECTOR_LOCAL_GATE_OK:"),
            WORKFLOW.index("Configure AWS credentials for bounded read-only observation"),
        )

    def test_hard_gates_exact_runtime_before_bounded_log_reads(self) -> None:
        for marker in (
            "EXPECTED_ACCOUNT_ID: '919341186960'",
            "EXPECTED_SERVICE: vevo-growthbook-collector-production",
            "EXPECTED_TASK_DEFINITION: vevo-growthbook-collector-production:2",
            "EXPECTED_RUNTIME_PATH: /app",
            "working_directory not in {None, os.environ['EXPECTED_RUNTIME_PATH']}",
            "cloudformation describe-stacks",
            "ecs describe-services",
            "ecs list-tasks",
            "ecs describe-tasks",
            "ecs describe-task-definition",
            "elbv2 describe-target-health",
            "apigatewayv2 get-routes",
            "ZERO_COLLECTOR_RUNTIME_GATE_OK:",
            "instance-id=N/A:Fargate:private-ip=",
            "runtime-path-source=immutable-image-prior-localhost-marker:",
            "logs filter-log-events",
            "'{ $.routeKey = \"POST /v1/events\" }'",
            "'\"VEVO_GROWTHBOOK_COLLECTOR_RECEIPT\"'",
            "--query 'events[].eventId'",
            "ZERO_COLLECTOR_OBSERVATION_OK:",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertLess(
            WORKFLOW.index("ZERO_COLLECTOR_RUNTIME_GATE_OK:"),
            WORKFLOW.index("logs filter-log-events"),
        )

    def test_uploads_one_sanitized_artifact_and_no_raw_aws_payload(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        self.assertIn("path: vevo-growthbook-zero-collector-observation.json", WORKFLOW)
        for marker in (
            "'contains_cloudwatch_messages': False",
            "'contains_event_or_request_ids': False",
            "'contains_credentials': False",
            "'contains_customer_or_order_data': False",
            "'runtime_path_verification': 'immutable_image_prior_localhost_marker'",
            "aggregate counts only; no messages or IDs",
        ):
            self.assertIn(marker, WORKFLOW)
        for forbidden_path in (
            "path: stack.json",
            "path: service.json",
            "path: task.json",
            "path: routes.json",
            "path: api-event-ids.json",
            "path: receipt-event-ids.json",
        ):
            self.assertNotIn(forbidden_path, WORKFLOW)

    def test_contains_no_aws_or_external_mutation(self) -> None:
        lowered = WORKFLOW.lower()
        for forbidden in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation execute-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "logs put-",
            "logs delete-",
            "s3api put-",
            "s3api delete-",
            "iam create-",
            "iam delete-",
            "athena start-query-execution",
            "scheduler update-",
            "ads_update",
            "submit",
        ):
            self.assertNotIn(forbidden, lowered)
        self.assertIn("'aws_mutations': False", WORKFLOW)
        self.assertIn("AWS mutations: `none`", WORKFLOW)

    def test_yaml_and_inline_python_are_valid(self) -> None:
        payload = yaml.safe_load(WORKFLOW)
        self.assertIsInstance(payload, dict)
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
        self.assertGreaterEqual(len(blocks), 6)
        for block_index, source in enumerate(blocks):
            compile(source, f"zero-collector-inline-{block_index}.py", "exec")


if __name__ == "__main__":
    unittest.main()
