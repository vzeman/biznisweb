from __future__ import annotations

import copy
import hashlib
import tempfile
import unittest
from pathlib import Path

from scripts import build_growthbook_cta_final_snapshot as builder
from scripts import evaluate_growthbook_cta as evaluator
from scripts import record_growthbook_cta_final_snapshot as recorder
from tests import test_growthbook_cta_final_snapshot_builder as builder_fixtures


class GrowthBookCtaFinalSnapshotRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        fixture = builder_fixtures.GrowthBookCtaFinalSnapshotBuilderTests(
            methodName="runTest"
        )
        fixture.setUp()
        self.fixture = fixture

    def _artifacts(self, paths: dict[str, Path]) -> tuple[dict, dict]:
        snapshot = builder.build_snapshot(
            self.fixture.opened,
            self.fixture._athena_results(),
            evaluated_at_utc="2026-10-03T02:00:00Z",
            **paths,
        )
        decision = evaluator.evaluate(
            snapshot,
            self.fixture.contract,
            self.fixture.sample,
            self.fixture.lifecycle,
            self.fixture.lifecycle_observation,
        )
        return snapshot, decision

    @staticmethod
    def _sha(value: dict) -> str:
        return hashlib.sha256(evaluator.canonical_json_bytes(value)).hexdigest()

    def test_records_exact_recomputed_final_decision_and_closes_reads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.fixture._write_sources(Path(temporary))
            snapshot, decision = self._artifacts(paths)
            recorded = recorder.record_final_snapshot(
                self.fixture.opened,
                snapshot,
                decision,
                self.fixture.contract,
                self.fixture.sample,
                self.fixture.lifecycle,
                self.fixture.lifecycle_observation,
                snapshot_sha256=self._sha(snapshot),
                decision_sha256=self._sha(decision),
                workflow_run_id="32843957284",
                main_commit="a" * 40,
                **paths,
            )

        self.assertEqual(builder.RECORDED, recorded["status"])
        self.assertEqual("WIN", recorded["final_look"]["verdict"])
        self.assertEqual(
            "brand_contrast", recorded["final_look"]["recommended_variation"]
        )
        self.assertFalse(recorded["final_look"]["protected_workflow_allowed"])
        self.assertFalse(
            recorded["release_boundaries"]["outcome_metrics_read_allowed"]
        )
        self.assertFalse(
            recorded["release_boundaries"]["automatic_winner_application_allowed"]
        )

    def test_rejects_repeat_recording_or_decision_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.fixture._write_sources(Path(temporary))
            snapshot, decision = self._artifacts(paths)
            recorded = recorder.record_final_snapshot(
                self.fixture.opened,
                snapshot,
                decision,
                self.fixture.contract,
                self.fixture.sample,
                self.fixture.lifecycle,
                self.fixture.lifecycle_observation,
                snapshot_sha256=self._sha(snapshot),
                decision_sha256=self._sha(decision),
                workflow_run_id="32843957284",
                main_commit="a" * 40,
                **paths,
            )
            with self.assertRaisesRegex(
                recorder.CtaFinalSnapshotRecordingError,
                "already recorded or not open",
            ):
                recorder.record_final_snapshot(
                    recorded,
                    snapshot,
                    decision,
                    self.fixture.contract,
                    self.fixture.sample,
                    self.fixture.lifecycle,
                    self.fixture.lifecycle_observation,
                    snapshot_sha256=self._sha(snapshot),
                    decision_sha256=self._sha(decision),
                    workflow_run_id="32843957285",
                    main_commit="b" * 40,
                    **paths,
                )

            altered = copy.deepcopy(decision)
            altered["verdict"] = "LOSE"
            with self.assertRaisesRegex(
                recorder.CtaFinalSnapshotRecordingError,
                "differs from offline recomputation",
            ):
                recorder.record_final_snapshot(
                    self.fixture.opened,
                    snapshot,
                    altered,
                    self.fixture.contract,
                    self.fixture.sample,
                    self.fixture.lifecycle,
                    self.fixture.lifecycle_observation,
                    snapshot_sha256=self._sha(snapshot),
                    decision_sha256=self._sha(altered),
                    workflow_run_id="32843957284",
                    main_commit="a" * 40,
                    **paths,
                )


if __name__ == "__main__":
    unittest.main()
