from __future__ import annotations

import copy
import hashlib
import tempfile
import unittest
from pathlib import Path

from scripts import build_growthbook_cta_final_snapshot as builder
from scripts import evaluate_growthbook_cta as evaluator
from scripts import record_growthbook_cta_final_snapshot as recorder
from scripts import validate_growthbook_hypothesis_registry as registry_validator
from tests import test_growthbook_cta_final_snapshot_builder as builder_fixtures


class GrowthBookCtaFinalSnapshotRecorderTests(unittest.TestCase):
    workflow_run_id = "32843957284"
    main_commit = "a" * 40

    def setUp(self) -> None:
        fixture = builder_fixtures.GrowthBookCtaFinalSnapshotBuilderTests(
            methodName="runTest"
        )
        fixture.setUp()
        self.fixture = fixture
        self.registry = builder_fixtures.load(
            "projects/vevo/growthbook_hypothesis_registry.json"
        )

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

    def _provenance(
        self,
        snapshot: dict,
        decision: dict,
        *,
        workflow_run_id: str | None = None,
        main_commit: str | None = None,
    ) -> dict:
        return {
            "artifact_name": "vevo-growthbook-cta-final-snapshot",
            "evidence_type": "vevo_growthbook_cta_final_provenance",
            "files": {
                "vevo-growthbook-cta-final-decision.json": {
                    "sha256": self._sha(decision)
                },
                "vevo-growthbook-cta-final-snapshot.json": {
                    "sha256": self._sha(snapshot)
                },
            },
            "main_commit": main_commit or self.main_commit,
            "repository": "vzeman/biznisweb",
            "safety": {
                "contains_credentials": False,
                "contains_customer_or_order_data": False,
                "contains_event_or_device_ids": False,
                "contains_raw_aws_payloads": False,
                "external_or_automatic_mutation": False,
            },
            "schema_version": 1,
            "workflow": (
                ".github/workflows/"
                "build-vevo-growthbook-production-cta-final-snapshot.yml"
            ),
            "workflow_run_attempt": 1,
            "workflow_run_id": workflow_run_id or self.workflow_run_id,
        }

    def test_records_exact_recomputed_final_decision_and_closes_reads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.fixture._write_sources(Path(temporary))
            snapshot, decision = self._artifacts(paths)
            provenance = self._provenance(snapshot, decision)
            recorded, recorded_registry = recorder.record_final_snapshot(
                self.fixture.opened,
                self.registry,
                snapshot,
                decision,
                provenance,
                self.fixture.contract,
                self.fixture.sample,
                self.fixture.lifecycle,
                self.fixture.lifecycle_observation,
                snapshot_sha256=self._sha(snapshot),
                decision_sha256=self._sha(decision),
                provenance_sha256=self._sha(provenance),
                workflow_run_id=self.workflow_run_id,
                main_commit=self.main_commit,
                **paths,
            )

        self.assertEqual(builder.RECORDED, recorded["status"])
        self.assertEqual("WIN", recorded["final_look"]["verdict"])
        self.assertEqual(
            "brand_contrast", recorded["final_look"]["recommended_variation"]
        )
        self.assertFalse(recorded["final_look"]["protected_workflow_allowed"])
        self.assertEqual(
            self._sha(provenance), recorded["final_look"]["provenance_sha256"]
        )
        self.assertFalse(
            recorded["release_boundaries"]["outcome_metrics_read_allowed"]
        )
        self.assertFalse(
            recorded["release_boundaries"]["automatic_winner_application_allowed"]
        )
        self.assertEqual(
            registry_validator.RECORDED,
            recorded_registry["experiments"][0]["status"],
        )
        final_decision = recorded_registry["experiments"][0]["final_decision"]
        self.assertEqual(decision, final_decision["aggregate_evidence"])
        self.assertEqual(self._sha(provenance), final_decision["provenance_sha256"])
        self.assertEqual(
            1084,
            final_decision["aggregate_evidence"]["summary"]["included_devices"],
        )
        self.assertEqual(
            hashlib.sha256(
                registry_validator.pretty_json_bytes(recorded_registry)
            ).hexdigest(),
            recorded["final_look"]["hypothesis_registry_sha256"],
        )
        registry_validator.validate_registry(recorded_registry, recorded)

        tampered = copy.deepcopy(recorded_registry)
        tampered["experiments"][0]["final_decision"]["aggregate_evidence"][
            "verdict"
        ] = "LOSE"
        with self.assertRaisesRegex(
            registry_validator.HypothesisRegistryError,
            "aggregate evidence hash drift",
        ):
            registry_validator.validate_registry(tampered)

    def test_rejects_repeat_recording_or_decision_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.fixture._write_sources(Path(temporary))
            snapshot, decision = self._artifacts(paths)
            provenance = self._provenance(snapshot, decision)
            recorded, _recorded_registry = recorder.record_final_snapshot(
                self.fixture.opened,
                self.registry,
                snapshot,
                decision,
                provenance,
                self.fixture.contract,
                self.fixture.sample,
                self.fixture.lifecycle,
                self.fixture.lifecycle_observation,
                snapshot_sha256=self._sha(snapshot),
                decision_sha256=self._sha(decision),
                provenance_sha256=self._sha(provenance),
                workflow_run_id=self.workflow_run_id,
                main_commit=self.main_commit,
                **paths,
            )
            with self.assertRaisesRegex(
                recorder.CtaFinalSnapshotRecordingError,
                "already recorded or not open",
            ):
                repeat_provenance = self._provenance(
                    snapshot,
                    decision,
                    workflow_run_id="32843957285",
                    main_commit="b" * 40,
                )
                recorder.record_final_snapshot(
                    recorded,
                    self.registry,
                    snapshot,
                    decision,
                    repeat_provenance,
                    self.fixture.contract,
                    self.fixture.sample,
                    self.fixture.lifecycle,
                    self.fixture.lifecycle_observation,
                    snapshot_sha256=self._sha(snapshot),
                    decision_sha256=self._sha(decision),
                    provenance_sha256=self._sha(repeat_provenance),
                    workflow_run_id="32843957285",
                    main_commit="b" * 40,
                    **paths,
                )

            altered = copy.deepcopy(decision)
            altered["verdict"] = "LOSE"
            altered_provenance = self._provenance(snapshot, altered)
            with self.assertRaisesRegex(
                recorder.CtaFinalSnapshotRecordingError,
                "differs from offline recomputation",
            ):
                recorder.record_final_snapshot(
                    self.fixture.opened,
                    self.registry,
                    snapshot,
                    altered,
                    altered_provenance,
                    self.fixture.contract,
                    self.fixture.sample,
                    self.fixture.lifecycle,
                    self.fixture.lifecycle_observation,
                    snapshot_sha256=self._sha(snapshot),
                    decision_sha256=self._sha(altered),
                    provenance_sha256=self._sha(altered_provenance),
                    workflow_run_id=self.workflow_run_id,
                    main_commit=self.main_commit,
                    **paths,
                )

    def test_rejects_swapped_provenance_run_commit_or_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.fixture._write_sources(Path(temporary))
            snapshot, decision = self._artifacts(paths)
            cases = (
                (
                    "workflow run mismatch",
                    lambda value: value.update({"workflow_run_id": "32843957285"}),
                ),
                (
                    "main commit mismatch",
                    lambda value: value.update({"main_commit": "b" * 40}),
                ),
                (
                    "hash mismatch: vevo-growthbook-cta-final-decision.json",
                    lambda value: value["files"][
                        "vevo-growthbook-cta-final-decision.json"
                    ].update({"sha256": "f" * 64}),
                ),
            )
            for expected, mutate in cases:
                with self.subTest(expected=expected):
                    provenance = self._provenance(snapshot, decision)
                    mutate(provenance)
                    with self.assertRaisesRegex(
                        recorder.CtaFinalSnapshotRecordingError, expected
                    ):
                        recorder.record_final_snapshot(
                            self.fixture.opened,
                            self.registry,
                            snapshot,
                            decision,
                            provenance,
                            self.fixture.contract,
                            self.fixture.sample,
                            self.fixture.lifecycle,
                            self.fixture.lifecycle_observation,
                            snapshot_sha256=self._sha(snapshot),
                            decision_sha256=self._sha(decision),
                            provenance_sha256=self._sha(provenance),
                            workflow_run_id=self.workflow_run_id,
                            main_commit=self.main_commit,
                            **paths,
                        )


if __name__ == "__main__":
    unittest.main()
