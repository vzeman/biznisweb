from __future__ import annotations

import pathlib
import textwrap
import unittest

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT / ".github" / "workflows" / "collect-vevo-growthbook-cta-baseline.yml"
).read_text(encoding="utf-8")


class GrowthBookCtaBaselineWorkflowTests(unittest.TestCase):
    def test_yaml_and_every_inline_python_block_are_valid(self) -> None:
        payload = yaml.safe_load(WORKFLOW)
        self.assertEqual({"contents": "read"}, payload["permissions"])
        self.assertEqual(
            "${{ github.ref == 'refs/heads/main' }}",
            payload["jobs"]["collect-cta-baseline"]["if"],
        )
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
        self.assertGreaterEqual(len(blocks), 5)
        for block_index, source in enumerate(blocks):
            compile(source, f"cta-baseline-workflow-inline-{block_index}.py", "exec")

    def test_is_main_only_completion_gated_and_aggregate_only(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "Require verified A/A completion before AWS credentials",
            "build_growthbook_cta_baseline_observation.py render-query",
            "PRODUCTION_CTA_BASELINE_LOCAL_GATE_OK:",
            "PRODUCTION_CTA_BASELINE_RUNTIME_HARD_GATE_OK:",
            "instance-id=N/A:Fargate:private-ip=",
            "service={os.environ['EXPECTED_SERVICE']}:path=",
            "localhost-marker=inherited-verified",
            "Run one aggregate-only Athena CTA baseline query",
            "--max-results 2",
            "variation-breakdown=false:activation=false",
            "Remove all temporary AWS responses and aggregate query files",
            "UI test: `not applicable; read-only aggregate workflow makes no UI change`",
        ):
            self.assertIn(marker, WORKFLOW)

    def test_uploads_exactly_one_sanitized_artifact(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        self.assertIn("name: vevo-growthbook-cta-baseline", WORKFLOW)
        self.assertIn("path: vevo-growthbook-cta-baseline.json", WORKFLOW)
        self.assertIn("retention-days: 90", WORKFLOW)
        self.assertNotIn("path: ${TEMP_BASELINE_DIR}", WORKFLOW)

    def test_has_no_external_mutation_path(self) -> None:
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
