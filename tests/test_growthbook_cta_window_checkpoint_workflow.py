from __future__ import annotations

import pathlib
import textwrap
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "check-vevo-growthbook-production-cta-window.yml"
).read_text(encoding="utf-8")


class GrowthBookCtaWindowCheckpointWorkflowTests(unittest.TestCase):
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
            compile(source, f"cta-checkpoint-inline-{block_index}.py", "exec")

    def test_main_only_frozen_stop_rule_gate_precedes_aws(self) -> None:
        for marker in (
            "if: ${{ github.ref == 'refs/heads/main' }}",
            "confirm_checkpoint:",
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

    def test_runtime_schedule_marker_alarm_and_dlq_gates_are_exact(self) -> None:
        for marker in (
            "21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb",
            "cron(45 3 * * ? *)",
            "Europe/Bratislava",
            "vevo-reporting-daily:33",
            "task.get('group') == os.environ['EXPECTED_SERVICE']",
            "scheduled reconciliation image differs from localhost-gated deploy evidence",
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:",
            "scheduled reconciliation generated/published parity drift",
            "Production reconciliation alarm gate failed",
            "Production reconciliation DLQ is not empty",
            "PRODUCTION_CTA_WINDOW_RUNTIME_GATE_OK:",
            "PRODUCTION_CTA_WINDOW_CONTROL_GATE_OK:",
        ):
            self.assertIn(marker, WORKFLOW)

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
            "retention-days: 14",
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
