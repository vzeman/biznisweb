from __future__ import annotations

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
    / "collect-vevo-growthbook-cta-lifecycle-preflight.yml"
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


class GrowthBookCtaLifecyclePreflightWorkflowTests(unittest.TestCase):
    def test_yaml_and_inline_python_are_valid(self) -> None:
        payload = yaml.safe_load(WORKFLOW)
        self.assertEqual({"contents": "read"}, payload["permissions"])
        self.assertEqual(
            "${{ github.ref == 'refs/heads/main' }}",
            payload["jobs"]["collect-lifecycle-preflight"]["if"],
        )
        blocks = inline_python_blocks()
        self.assertGreaterEqual(len(blocks), 5)
        for block_index, source in enumerate(blocks):
            compile(source, f"cta-lifecycle-workflow-inline-{block_index}.py", "exec")

    def test_daily_schedule_skips_before_aa_pass_without_aws(self) -> None:
        payload = yaml.safe_load(WORKFLOW)
        self.assertIn({"cron": "20 5 * * *"}, payload[True]["schedule"])
        gate = next(
            block
            for block in inline_python_blocks()
            if "VEVO_CTA_LIFECYCLE_SCHEDULE_GATE" in block
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            env_path = pathlib.Path(temporary_directory) / "github.env"
            with mock.patch.dict(
                os.environ,
                {
                    "EVENT_NAME": "schedule",
                    "CONFIRM_COLLECTION": "",
                    "GITHUB_ENV": str(env_path),
                },
                clear=False,
            ):
                exec(compile(gate, "cta-lifecycle-schedule-gate.py", "exec"), {})
            self.assertEqual("RUN_COLLECTION=false\n", env_path.read_text())

        credentials = WORKFLOW.index(
            "uses: aws-actions/configure-aws-credentials@v6.1.0"
        )
        schedule_gate = WORKFLOW.index("VEVO_CTA_LIFECYCLE_SCHEDULE_GATE:")
        self.assertLess(schedule_gate, credentials)

    def test_scheduled_collection_is_bounded_to_one_due_day(self) -> None:
        self.assertIn(
            "run_collection = due <= current < due + timedelta(days=1)", WORKFLOW
        )
        self.assertIn("scheduled-window-missed-manual-confirmation-required", WORKFLOW)

    def test_manual_schedule_gate_requires_exact_confirmation(self) -> None:
        gate = next(
            block
            for block in inline_python_blocks()
            if "VEVO_CTA_LIFECYCLE_SCHEDULE_GATE" in block
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            env_path = pathlib.Path(temporary_directory) / "github.env"
            with mock.patch.dict(
                os.environ,
                {
                    "EVENT_NAME": "workflow_dispatch",
                    "CONFIRM_COLLECTION": "false",
                    "GITHUB_ENV": str(env_path),
                },
                clear=False,
            ):
                with self.assertRaisesRegex(SystemExit, "confirmation"):
                    exec(compile(gate, "cta-lifecycle-manual-gate.py", "exec"), {})

    def test_is_source_explicit_mature_and_cta_outcome_blind(self) -> None:
        for marker in (
            "Require completed A/A and full order plus lifecycle maturity before AWS",
            "source=completed-aa:followup=21d:cta-outcomes=false",
            "VEVO_CTA_LIFECYCLE_HOST_GATE_OK:",
            "instance-id=N/A:Fargate:private-ip=",
            "service={os.environ['EXPECTED_SERVICE']}:path=",
            "experiment_id=${SOURCE_EXPERIMENT_ID}",
            "Bind the exact retained quality object to the direct cohort generation",
            "select-quality-context",
            "Run one aggregate-only Athena lifecycle parity query",
            "Direct curated S3 vs Athena CM1/lifecycle parity: `required`",
            "CTA outcome data: `not read`",
            "UI test: `not applicable; the workflow makes no UI change`",
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn("vevo-sk-product-cta-color-001/", WORKFLOW)
        self.assertNotIn("aws s3api list-objects-v2", WORKFLOW)

    def test_uploads_only_the_identity_free_artifact_and_cleans_raw_facts(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        self.assertIn("name: vevo-growthbook-cta-lifecycle-preflight", WORKFLOW)
        self.assertIn("path: vevo-growthbook-cta-lifecycle-preflight.json", WORKFLOW)
        self.assertIn("retention-days: 90", WORKFLOW)
        self.assertIn('find "${TARGET}" -type f -exec shred', WORKFLOW)
        self.assertNotIn("path: ${TEMP_LIFECYCLE_DIR}", WORKFLOW)

    def test_has_no_external_mutation_or_browser_path(self) -> None:
        lowered = WORKFLOW.lower()
        for forbidden in (
            "cloudformation create-",
            "cloudformation update-",
            "cloudformation delete-",
            "ecs run-task",
            "ecs update-service",
            "register-task-definition",
            "iam create-",
            "iam update-",
            "iam delete-",
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


if __name__ == "__main__":
    unittest.main()
