from __future__ import annotations

import hashlib
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch
from tests.growthbook_aa_source_fixtures import source_bundle

from scripts.record_growthbook_aa_evidence_gates import (
    EvidenceGateRecordingError,
    _load_canonical_mapping,
    main,
    open_automated_producer,
    open_manual_producer,
    record_component,
)
from scripts.record_growthbook_aa_window_checkpoint import record_checkpoint
from scripts.validate_growthbook_aa_measurement_window import (
    canonical_evidence_bytes,
    validate_measurement_window,
)
from tests.growthbook_aa_fixtures import initial_snapshot
from tests.test_growthbook_aa_snapshot_assembler import (
    automated_evidence,
    manual_evidence,
)
from tests.test_growthbook_aa_window_checkpoint_recorder import checkpoint_evidence


ROOT = pathlib.Path(__file__).resolve().parents[1]
AUTOMATED_RUN_ID = "32840000001"
AUTOMATED_COMMIT = "3" * 40
MANUAL_RUN_ID = "32840000002"
MANUAL_COMMIT = "4" * 40
FROM_UTC = "2026-08-25T22:00:00Z"
THROUGH_UTC = "2026-09-01T22:00:00Z"


def load(name: str) -> dict[str, object]:
    return json.loads((ROOT / "projects" / "vevo" / name).read_text(encoding="utf-8"))


def resolved_snapshot() -> dict[str, object]:
    snapshot = initial_snapshot()
    evidence = checkpoint_evidence(eligible_devices=1000)
    digest = hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest()
    return record_checkpoint(
        snapshot,
        evidence,
        evidence_sha256=digest,
        expected_workflow_run_id=evidence["workflow_run_id"],
        expected_main_commit=evidence["main_commit"],
        activation=load("growthbook_production_aa_activation.json"),
        acceptance=load("growthbook_aa_acceptance.json"),
        reconciliation=load(
            "growthbook_production_reconciliation_deploy_evidence.json"
        ),
    )


def quality_report() -> dict[str, object]:
    return {
        "metric_contract_version": "vevo_cm1_v1_2026-08-20",
        "experiment_id": "vevo-sk-aa-001",
        "facts_generated_at": "2026-09-02T01:50:00Z",
        "raw_event_count": 5010,
        "unique_event_count": 5000,
        "duplicate_event_count": 10,
        "orphan_event_count": 0,
        "exposed_device_count": 1000,
        "eligible_device_count": 1000,
        "contaminated_device_count": 0,
        "variation_counts": {"control": 510, "variant": 490},
        "srm_chi_square": 0.4,
        "srm_p_value": 0.5271,
        "srm_alert": False,
        "unique_transaction_count": 60,
        "exact_joined_transaction_count": 60,
        "exact_join_rate_pct": 100.0,
        "unmatched_transaction_count": 0,
        "ambiguous_transaction_count": 0,
        "attributed_transaction_count": 60,
        "performance_duplicate_count": 0,
        "variation_health": {
            "control": {
                "eligible_devices": 510,
                "client_error_devices": 0,
                "client_error_device_rate_pct": 0.0,
                "measured_page_loads": 250,
                "lcp_p75_ms": 1300.0,
                "inp_p75_ms": 100.0,
                "cls_p75_milli": 5.0,
            },
            "variant": {
                "eligible_devices": 490,
                "client_error_devices": 0,
                "client_error_device_rate_pct": 0.0,
                "measured_page_loads": 240,
                "lcp_p75_ms": 1350.0,
                "inp_p75_ms": 105.0,
                "cls_p75_milli": 6.0,
            },
        },
    }


def quality_key() -> str:
    return (
        "experiment-events/curated/quality/experiment_id=vevo-sk-aa-001/"
        "facts_generated_at=20260902T015000Z.json"
    )


def observation() -> dict[str, object]:
    value = manual_evidence()
    value.pop("source_run_id")
    value.pop("source_main_commit")
    value["from_utc"] = FROM_UTC
    value["through_utc"] = THROUGH_UTC
    value["evidence_type"] = "pending_workflow_provenance"
    value["observation_type"] = "vevo_growthbook_aa_manual_qa_observation"
    return value


def component_evidence(component: str) -> dict[str, object]:
    if component == "automated":
        value = automated_evidence()
        value["source_run_id"] = AUTOMATED_RUN_ID
        value["source_main_commit"] = AUTOMATED_COMMIT
        value["quality_source_sha256"] = source_bundle()["expected_evidence_sha256"]
    else:
        value = manual_evidence()
        value["source_run_id"] = MANUAL_RUN_ID
        value["source_main_commit"] = MANUAL_COMMIT
    value["from_utc"] = FROM_UTC
    value["through_utc"] = THROUGH_UTC
    return value


class GrowthBookAaEvidenceGateRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshot = resolved_snapshot()
        self.activation = load("growthbook_production_aa_activation.json")
        self.acceptance = load("growthbook_aa_acceptance.json")
        self.reconciliation = load(
            "growthbook_production_reconciliation_deploy_evidence.json"
        )

    def validate(self, snapshot: dict[str, object]) -> None:
        validate_measurement_window(
            snapshot, self.activation, self.acceptance, self.reconciliation
        )

    def open_automated(self) -> dict[str, object]:
        return open_automated_producer(self.snapshot, **source_bundle())

    def open_manual(
        self, snapshot: dict[str, object] | None = None
    ) -> dict[str, object]:
        reviewed = observation()
        digest = hashlib.sha256(canonical_evidence_bytes(reviewed)).hexdigest()
        return open_manual_producer(
            snapshot or self.snapshot,
            reviewed,
            observation_sha256=digest,
        )

    def test_opens_automated_only_for_exact_resolved_quality_source(self) -> None:
        recorded = self.open_automated()
        automated = recorded["automated_evidence"]
        self.assertTrue(automated["producer_allowed"])
        self.assertEqual(
            3,
            recorded["schema_version"],
        )
        self.assertEqual(source_bundle()["expected_evidence_sha256"], automated["quality_source"]["json_sha256"])
        self.assertNotIn("quality_report_key", automated)
        self.assertFalse(recorded["snapshot_build_allowed"])
        self.validate(recorded)
        self.assertEqual(
            recorded,
            open_automated_producer(
                recorded,
                **source_bundle(),
            ),
        )

        mismatched = source_bundle()
        mismatched["expected_evidence_sha256"] = "f" * 64
        with self.assertRaisesRegex(EvidenceGateRecordingError, "source verification failed"):
            open_automated_producer(self.snapshot, **mismatched)
        with self.assertRaises(TypeError):
            open_automated_producer(self.snapshot, quality_report(),
                quality_report_key=quality_key(), quality_report_sha256="f" * 64)

    def test_opens_manual_only_for_exact_reviewed_window(self) -> None:
        recorded = self.open_manual()
        manual = recorded["manual_qa_evidence"]
        self.assertTrue(manual["producer_allowed"])
        self.assertEqual("verified_reviewed_browser_qa", manual["observation_status"])
        self.assertFalse(recorded["snapshot_build_allowed"])
        self.validate(recorded)

        wrong_window = observation()
        wrong_window["through_utc"] = "2026-09-02T22:00:00Z"
        with self.assertRaisesRegex(EvidenceGateRecordingError, "resolved A/A window"):
            open_manual_producer(
                self.snapshot,
                wrong_window,
                observation_sha256=hashlib.sha256(
                    canonical_evidence_bytes(wrong_window)
                ).hexdigest(),
            )

        unsafe = observation()
        unsafe["unplanned_mutation_observed"] = True
        with self.assertRaisesRegex(EvidenceGateRecordingError, "must be false"):
            open_manual_producer(
                self.snapshot,
                unsafe,
                observation_sha256=hashlib.sha256(
                    canonical_evidence_bytes(unsafe)
                ).hexdigest(),
            )

    def test_records_both_components_before_opening_snapshot(self) -> None:
        opened = self.open_manual(self.open_automated())
        automated = component_evidence("automated")
        automated_digest = hashlib.sha256(
            canonical_evidence_bytes(automated)
        ).hexdigest()
        first = record_component(
            opened,
            automated,
            component_name="automated",
            evidence_sha256=automated_digest,
            expected_workflow_run_id=AUTOMATED_RUN_ID,
            expected_main_commit=AUTOMATED_COMMIT,
        )
        self.assertEqual("verified", first["automated_evidence"]["status"])
        self.assertFalse(first["automated_evidence"]["producer_allowed"])
        self.assertFalse(first["snapshot_build_allowed"])
        self.validate(first)

        manual = component_evidence("manual")
        manual_digest = hashlib.sha256(canonical_evidence_bytes(manual)).hexdigest()
        second = record_component(
            first,
            manual,
            component_name="manual",
            evidence_sha256=manual_digest,
            expected_workflow_run_id=MANUAL_RUN_ID,
            expected_main_commit=MANUAL_COMMIT,
        )
        self.assertEqual("verified", second["manual_qa_evidence"]["status"])
        self.assertFalse(second["manual_qa_evidence"]["producer_allowed"])
        self.assertTrue(second["snapshot_build_allowed"])
        self.validate(second)
        self.assertEqual(
            second,
            record_component(
                second,
                manual,
                component_name="manual",
                evidence_sha256=manual_digest,
                expected_workflow_run_id=MANUAL_RUN_ID,
                expected_main_commit=MANUAL_COMMIT,
            ),
        )

    def test_rejects_tampered_component_identity_and_canonical_source(self) -> None:
        opened = self.open_automated()
        automated = component_evidence("automated")
        digest = hashlib.sha256(canonical_evidence_bytes(automated)).hexdigest()
        with self.assertRaisesRegex(EvidenceGateRecordingError, "run ID mismatch"):
            record_component(
                opened,
                automated,
                component_name="automated",
                evidence_sha256=digest,
                expected_workflow_run_id="32840000009",
                expected_main_commit=AUTOMATED_COMMIT,
            )

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = pathlib.Path(temporary_directory) / "quality.json"
            path.write_text(json.dumps(quality_report()), encoding="utf-8")
            raw_digest = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(EvidenceGateRecordingError, "canonical JSON"):
                _load_canonical_mapping(path, raw_digest, "quality report")

    def test_cli_writes_only_the_reviewed_automated_transition(self) -> None:
        bundle = source_bundle()
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            snapshot_path = temporary / "snapshot.json"
            output_path = temporary / "output.json"
            snapshot_path.write_text(
                json.dumps(self.snapshot, indent=2) + "\n", encoding="utf-8"
            )
            argv = ["--snapshot", str(snapshot_path), "--output", str(output_path), "open-automated"]
            for field, value in bundle.items():
                if field == "source_inputs":
                    continue
                option = "--" + field.replace("_", "-")
                if field.startswith("expected_"):
                    argv.extend([option, value])
                else:
                    path = temporary / field
                    path.write_bytes(value if isinstance(value, bytes) else json.dumps(value).encode())
                    argv.extend([option, str(path)])
            with patch("scripts.growthbook_aa_source_binding.read_git_source_inputs", return_value=bundle["source_inputs"]) as reader:
                self.assertEqual(0, main(argv))
                reader.assert_called_once_with(bundle["expected_main_commit"])
            recorded = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertTrue(recorded["automated_evidence"]["producer_allowed"])
            self.assertFalse(recorded["manual_qa_evidence"]["producer_allowed"])
            self.assertFalse(recorded["snapshot_build_allowed"])
            self.validate(recorded)


if __name__ == "__main__":
    unittest.main()
