from __future__ import annotations

import json
import pathlib
import textwrap
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "collect-vevo-growthbook-production-aa-evidence.yml"
).read_text(encoding="utf-8")
MANIFEST = json.loads(
    (ROOT / "projects" / "vevo" / "growthbook_aa_snapshot.json").read_text(
        encoding="utf-8"
    )
)


class GrowthBookAaAutomatedWorkflowTests(unittest.TestCase):
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
            compile(source, f"automated-workflow-inline-{block_index}.py", "exec")

    def test_producer_is_closed_while_window_is_pre_registered(self) -> None:
        automated = MANIFEST["automated_evidence"]
        self.assertFalse(automated["producer_allowed"])
        self.assertEqual("frozen_waiting_for_completion", automated["window_status"])
        self.assertEqual("2026-08-25T22:00:00Z", automated["from_utc"])
        self.assertIsNone(automated["through_utc"])
        self.assertEqual("not_recorded", automated["quality_report_status"])
        self.assertIsNone(automated["quality_report_key"])
        self.assertIsNone(automated["quality_report_sha256"])
        self.assertEqual("not_recorded", automated["status"])

    def test_main_only_gate_precedes_aws_credentials_and_every_source_read(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_collection:",
            "automated evidence producer gate is closed",
            "validate_growthbook_aa_measurement_window.py",
            "frozen Production A/A evidence window is not recorded",
            "pre-registered A/A stopping rule is not resolved",
            "canonical Production reporting quality is not recorded",
            "Production localhost and marker hard gate is missing",
            "Production reader evidence is missing",
            "Production GrowthBook clone must be complete and re-closed",
            "row.get('tracking_key'): row",
            "Production A/A is not the only running experiment",
            "CTA A/B must remain unstarted during automated A/A evidence",
            "PRODUCTION_AA_AUTOMATED_LOCAL_GATE_OK:",
            "uses: aws-actions/configure-aws-credentials@v6.1.0",
        ):
            self.assertIn(marker, WORKFLOW)
        gate = WORKFLOW.index("PRODUCTION_AA_AUTOMATED_LOCAL_GATE_OK:")
        credentials = WORKFLOW.index("uses: aws-actions/configure-aws-credentials@v6.1.0")
        first_aws = WORKFLOW.index("aws sts get-caller-identity")
        self.assertLess(gate, credentials)
        self.assertLess(credentials, first_aws)

    def test_reads_bounded_authoritative_sources_and_strips_temporary_payloads(self) -> None:
        for marker in (
            "aws cloudformation describe-stacks",
            "aws ecs describe-services",
            "aws ecs describe-tasks",
            "PRODUCTION_AA_RUNTIME_HARD_GATE_OK:",
            "current_task_definition = str(definition.get('taskDefinitionArn') or '').rsplit('/', 1)[-1]",
            "aws glue get-table",
            "PRODUCTION_AA_GLUE_SCHEMA_OK:",
            "aws logs filter-log-events",
            "scripts/summarize_growthbook_receipts.py",
            '"${QUALITY_REPORT_SHA256}"',
            "aws s3api get-object",
            "aws athena start-query-execution",
            "aws athena get-query-results",
            "COUNT(DISTINCT event_id)",
            "(SELECT COUNT(*) FROM raw_window) AS audited_row_count",
            "Remove all temporary AWS responses and aggregate query files",
            'rm -rf -- "${TARGET}"',
        ):
            self.assertIn(marker, WORKFLOW)
        self.assertNotIn("privacy_sample AS", WORKFLOW)
        self.assertNotIn("LIMIT 100", WORKFLOW)
        self.assertEqual(8, WORKFLOW.count("FROM raw_window"))
        self.assertLess(
            WORKFLOW.index("scripts/summarize_growthbook_receipts.py"),
            WORKFLOW.index("scripts/build_growthbook_aa_automated_evidence.py"),
        )
        self.assertLess(
            WORKFLOW.index("Remove all temporary AWS responses and aggregate query files"),
            WORKFLOW.index("Upload sanitized automated evidence only"),
        )

    def test_uploads_exactly_one_sanitized_component(self) -> None:
        self.assertEqual(1, WORKFLOW.count("uses: actions/upload-artifact@v4.6.2"))
        upload = WORKFLOW.index("Upload sanitized automated evidence only")
        for marker in (
            "name: vevo-growthbook-aa-automated-evidence",
            "path: vevo-growthbook-aa-automated-evidence.json",
            "retention-days: 14",
        ):
            self.assertIn(marker, WORKFLOW[upload:])
        for forbidden in (
            "path: ${TEMP_EVIDENCE_DIR}",
            "path: receipt-events.json",
            "path: reporting-quality.json",
            "path: athena-aggregate-results.json",
            "path: access-reject-events.json",
        ):
            self.assertNotIn(forbidden, WORKFLOW)

    def test_has_no_external_control_plane_or_commerce_mutation_path(self) -> None:
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
        self.assertIn("GrowthBook, GTM, Meta Ads and BiznisWeb mutation: `none`", WORKFLOW)
        self.assertIn("Winner calls allowed: `false`", WORKFLOW)


if __name__ == "__main__":
    unittest.main()
