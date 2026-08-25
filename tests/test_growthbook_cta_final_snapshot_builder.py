from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from scripts import build_growthbook_cta_final_snapshot as builder
from scripts import evaluate_growthbook_cta as evaluator
from scripts import record_growthbook_cta_completion as completion_recorder
from tests import test_growthbook_cta_completion_recorder as completion_fixtures
from tests import test_growthbook_cta_evaluator as evaluator_fixtures


ROOT = Path(__file__).resolve().parents[1]


def load(relative: str) -> dict:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


class GrowthBookCtaFinalSnapshotBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        completion = completion_fixtures.GrowthBookCtaCompletionRecorderTests(
            methodName="runTest"
        )
        completion.setUp()
        (
            self.completion,
            self.activation,
            self.measurement,
            _workspace,
            recorded_final_snapshot,
        ) = completion._record()
        self.sample = completion.sample
        self.contract = completion.contract
        self.lifecycle = completion.lifecycle
        self.lifecycle_observation = completion.lifecycle_observation
        self.stop = completion.stop_observation
        self.waiting = load("projects/vevo/growthbook_cta_final_snapshot.json")
        self.completion_bytes = completion_recorder.pretty_json_bytes(self.completion)
        self.activation_bytes = completion_recorder.pretty_json_bytes(self.activation)
        self.measurement_bytes = completion_recorder.pretty_json_bytes(self.measurement)
        self.sample_bytes = completion_recorder.pretty_json_bytes(self.sample)
        self.lifecycle_bytes = completion_recorder.pretty_json_bytes(self.lifecycle)
        self.stop_bytes = completion_recorder.canonical_json_bytes(self.stop)
        self.opened = recorded_final_snapshot
        evaluation = evaluator_fixtures.GrowthBookCtaEvaluatorTests(
            methodName="runTest"
        )
        evaluation.setUp()
        self.expected_snapshot = evaluation._snapshot()

    def _write_sources(self, directory: Path) -> dict[str, Path]:
        sources = {
            "completion_path": ("completion.json", self.completion_bytes),
            "activation_path": ("activation.json", self.activation_bytes),
            "measurement_path": ("measurement.json", self.measurement_bytes),
            "sample_plan_path": ("sample.json", self.sample_bytes),
            "lifecycle_path": ("lifecycle.json", self.lifecycle_bytes),
            "stop_observation_path": ("stop.json", self.stop_bytes),
        }
        paths: dict[str, Path] = {}
        for field, (name, body) in sources.items():
            path = directory / name
            path.write_bytes(body)
            paths[field] = path
        paths["decision_contract_path"] = builder.DEFAULT_DECISION_CONTRACT_PATH
        return paths

    @staticmethod
    def _athena_cell(value: object) -> dict[str, str]:
        if value is None:
            return {}
        if isinstance(value, bool):
            return {"VarCharValue": "true" if value else "false"}
        return {"VarCharValue": str(value)}

    def _athena_results(self) -> dict:
        quality = {
            "reporting_device_count": 1084,
            "eligible_devices_seen_before_stop": 1084,
            "raw_event_count": 2200,
            "unique_event_count": 2198,
            "contaminated_device_count": 0,
            "pii_finding_count": 0,
            "full_url_stored_count": 0,
            "click_identifier_stored_count": 0,
            "non_analytical_consent_exposure_count": 0,
        }
        rows = []
        for variation in ("brand_contrast", "control"):
            expected = self.expected_snapshot["variations"][variation]
            values = {
                "variation_id": variation,
                **expected,
                "immature_order_count": 0,
                "unmatched_transaction_count": 0,
                "ambiguous_transaction_count": 0,
                **quality,
            }
            rows.append(
                {
                    "Data": [
                        self._athena_cell(values[column])
                        for column in builder.RESULT_COLUMNS
                    ]
                }
            )
        return {
            "ResultSet": {
                "Rows": [
                    {
                        "Data": [
                            {"VarCharValue": column}
                            for column in builder.RESULT_COLUMNS
                        ]
                    },
                    *rows,
                ]
            }
        }

    def test_checked_in_manifest_is_fail_closed_and_hash_bound(self) -> None:
        builder.validate_manifest(self.waiting)
        self.assertEqual(builder.WAITING, self.waiting["status"])
        self.assertFalse(self.waiting["final_look"]["protected_workflow_allowed"])
        self.assertFalse(self.waiting["release_boundaries"]["outcome_metrics_read_allowed"])
        self.assertEqual(
            self.waiting["query_contract"]["template_sha256"],
            builder._hash_path(
                ROOT / self.waiting["query_contract"]["template_path"]
            ),
        )

    def test_stop_opens_only_the_hash_bound_future_final_look(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self._write_sources(Path(temporary))
            builder.validate_manifest(self.opened, **paths)

        final = self.opened["final_look"]
        self.assertEqual(builder.FOLLOWUP, self.opened["status"])
        self.assertEqual("2026-10-03T02:00:00Z", final["snapshot_due_utc"])
        self.assertTrue(final["protected_workflow_allowed"])
        self.assertTrue(
            self.opened["release_boundaries"]["outcome_metrics_read_allowed"]
        )
        forbidden = set(self.opened["release_boundaries"]) - {
            "main_only",
            "diagnostic_host_gate_task_allowed",
            "aws_aggregate_reads_allowed",
            "outcome_metrics_read_allowed",
        }
        self.assertTrue(
            all(self.opened["release_boundaries"][field] is False for field in forbidden)
        )

    def test_rendered_query_is_exact_first_n_and_has_no_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self._write_sources(Path(temporary))
            query = builder.render_query(self.opened, **paths)

        self.assertIn("sample_ordinal <= 1084", query)
        self.assertIn("2026-09-19T02:00:00Z", query)
        self.assertIn("2026-10-03T02:00:00Z", query)
        self.assertNotIn("__CTA_", query)
        self.assertNotIn("__FOLLOWUP_", query)
        self.assertNotIn("__TARGET_", query)
        self.assertNotIn("privacy_sample AS", query)
        self.assertNotIn("LIMIT 100", query)
        self.assertEqual(6, query.count("FROM raw_window"))

    def test_builds_one_canonical_identity_free_snapshot_and_final_decision(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self._write_sources(Path(temporary))
            snapshot = builder.build_snapshot(
                self.opened,
                self._athena_results(),
                evaluated_at_utc="2026-10-03T02:00:00Z",
                **paths,
            )

        self.assertEqual(1084, snapshot["decision_cohort"]["included_devices"])
        self.assertEqual({"control", "brand_contrast"}, set(snapshot["variations"]))
        serialized = builder.canonical_json_bytes(snapshot).decode("utf-8")
        for forbidden in (
            '"device_id":',
            '"event_id":',
            '"transaction_id":',
            '"customer_email":',
        ):
            self.assertNotIn(forbidden, serialized)
        result = evaluator.evaluate(
            snapshot,
            self.contract,
            self.sample,
            self.lifecycle,
            self.lifecycle_observation,
        )
        self.assertEqual("WIN", result["verdict"])
        self.assertTrue(result["final_decision"])
        self.assertFalse(result["automatic_mutation_allowed"])

    def test_rejects_result_schema_drift_or_identity_column(self) -> None:
        altered = self._athena_results()
        altered["ResultSet"]["Rows"][0]["Data"].append(
            {"VarCharValue": "device_id"}
        )
        for row in altered["ResultSet"]["Rows"][1:]:
            row["Data"].append({"VarCharValue": "forbidden"})

        with self.assertRaisesRegex(builder.CtaFinalSnapshotError, "row width"):
            builder.parse_athena_results(altered)

    def test_rejects_early_or_repeat_final_look(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self._write_sources(Path(temporary))
            with self.assertRaisesRegex(
                builder.CtaFinalSnapshotError, "before the frozen due time"
            ):
                builder.build_snapshot(
                    self.opened,
                    self._athena_results(),
                    evaluated_at_utc="2026-10-03T01:59:59Z",
                    **paths,
                )
            recorded = copy.deepcopy(self.opened)
            recorded["status"] = builder.RECORDED
            recorded["final_look"].update(
                {
                    "protected_workflow_allowed": False,
                    "successful_run_id": "32843957284",
                    "main_commit": "a" * 40,
                    "snapshot_sha256": "b" * 64,
                    "decision_sha256": "c" * 64,
                    "verdict": "WIN",
                    "recommended_variation": "brand_contrast",
                }
            )
            recorded["release_boundaries"]["aws_aggregate_reads_allowed"] = False
            recorded["release_boundaries"]["diagnostic_host_gate_task_allowed"] = False
            recorded["release_boundaries"]["outcome_metrics_read_allowed"] = False
            recorded["next_gate"] = "manual_review_decision_before_any_external_mutation"
            builder.validate_manifest(recorded, **paths)
            with self.assertRaisesRegex(builder.CtaFinalSnapshotError, "gate is not open"):
                builder.build_snapshot(
                    recorded,
                    self._athena_results(),
                    evaluated_at_utc="2026-10-03T02:00:00Z",
                    **paths,
                )


if __name__ == "__main__":
    unittest.main()
