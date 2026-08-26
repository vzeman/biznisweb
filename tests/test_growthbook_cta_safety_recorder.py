from __future__ import annotations

import copy
import hashlib
import json
import unittest
from pathlib import Path

from scripts import record_growthbook_cta_safety_checkpoint as recorder
from scripts import validate_growthbook_cta_measurement_window as window_validator
from scripts.evaluate_growthbook_cta_safety import (
    MONITORING,
    STOP_REVIEW,
    canonical_json_bytes,
    evaluate,
    validate_contract,
)
from tests import test_growthbook_cta_window_checkpoint as window_fixtures


ROOT = Path(__file__).resolve().parents[1]


def load(relative: str) -> dict:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


class GrowthBookCtaSafetyRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        window = window_fixtures.GrowthBookCtaWindowCheckpointTests(
            methodName="runTest"
        )
        window.setUp()
        self.activation = window.activation
        self.start_observation = window.start_observation
        self.sample = window.sample
        self.decision_contract = window.contract
        self.reconciliation = window.reconciliation
        self.measurement = window.running
        self.measurement_source_hashes = window.source_hashes
        self.initial = load("projects/vevo/growthbook_cta_safety_monitoring.json")
        decision_contract_bytes = (
            ROOT / "projects/vevo/growthbook_cta_decision_contract.json"
        ).read_bytes()
        self.safety_source_hashes = recorder.source_hashes(
            self.activation,
            self.start_observation,
            decision_contract_bytes,
        )
        self.prepared = recorder.initialize_monitoring(
            self.initial,
            self.activation,
            self.start_observation,
            source_hashes=self.safety_source_hashes,
        )
        self.enabled = copy.deepcopy(self.prepared)
        validate_contract(self.enabled)

    def evidence(self, *, stop: str | None = None) -> dict:
        result = {
            "schema_version": 1,
            "evidence_type": "vevo_growthbook_cta_safety_checkpoint",
            "experiment_id": "vevo-sk-product-cta-color-001",
            "checkpoint_index": 1,
            "assignment_started_at_utc": self.prepared[
                "assignment_started_at_utc"
            ],
            "observed_at_utc": "2026-09-05T07:00:00Z",
            "variation_health": {
                "control": {
                    "eligible_devices": 400,
                    "measured_page_loads": 250,
                    "client_error_devices": 4,
                    "lcp_p75_ms": 1300,
                    "inp_p75_ms": 150,
                    "cls_p75_milli": 5,
                },
                "brand_contrast": {
                    "eligible_devices": 400,
                    "measured_page_loads": 250,
                    "client_error_devices": 4,
                    "lcp_p75_ms": 1350,
                    "inp_p75_ms": 155,
                    "cls_p75_milli": 6,
                },
            },
            "commerce_readback": {
                "add_to_cart_text_unchanged": True,
                "price_unchanged": True,
                "cart_checkout_order_mutated": False,
                "reproducible_cart_or_checkout_runtime_error": False,
            },
            "data_quality": {
                "query_complete": True,
                "exact_two_variations": True,
                "assignment_source_match": True,
                "duplicate_or_conflicting_assignment_detected": False,
            },
            "safety": {
                "contains_event_or_device_ids": False,
                "contains_customer_or_order_data": False,
                "primary_metric_read": False,
                "business_outcome_read": False,
                "meta_dimensions_read": False,
                "winner_call_made": False,
                "external_or_automatic_mutation": False,
            },
        }
        if stop == "commerce":
            result["commerce_readback"][
                "reproducible_cart_or_checkout_runtime_error"
            ] = True
        elif stop == "performance":
            result["variation_health"]["brand_contrast"]["lcp_p75_ms"] = 1501
        return result

    def artifact(self, evidence: dict) -> tuple[dict, dict, dict[str, str]]:
        decision = evaluate(evidence, self.enabled)
        evidence_hash = hashlib.sha256(canonical_json_bytes(evidence)).hexdigest()
        decision_hash = hashlib.sha256(canonical_json_bytes(decision)).hexdigest()
        provenance = {
            "schema_version": 1,
            "provenance_type": "vevo_growthbook_cta_safety_checkpoint",
            "repository": "vzeman/biznisweb",
            "workflow": recorder.WORKFLOW,
            "workflow_run_id": "60000000001",
            "main_commit": "a" * 40,
            "artifact_name": recorder.ARTIFACT_NAME,
            "files": {
                "evidence_file": recorder.EVIDENCE_FILE,
                "evidence_sha256": evidence_hash,
                "decision_file": recorder.DECISION_FILE,
                "decision_sha256": decision_hash,
                "provenance_file": recorder.PROVENANCE_FILE,
            },
            "safety": {
                "contains_event_or_device_ids": False,
                "contains_customer_or_order_data": False,
                "contains_primary_or_business_outcomes": False,
                "contains_meta_dimensions": False,
                "winner_call_made": False,
                "external_or_automatic_mutation": False,
            },
        }
        provenance_hash = hashlib.sha256(
            canonical_json_bytes(provenance)
        ).hexdigest()
        return decision, provenance, {
            "evidence": evidence_hash,
            "decision": decision_hash,
            "provenance": provenance_hash,
        }

    def record(self, evidence: dict):
        decision, provenance, hashes = self.artifact(evidence)
        return recorder.record_checkpoint(
            self.enabled,
            self.measurement,
            evidence,
            decision,
            provenance,
            evidence_sha256=hashes["evidence"],
            decision_sha256=hashes["decision"],
            provenance_sha256=hashes["provenance"],
            expected_workflow_run_id=provenance["workflow_run_id"],
            expected_main_commit=provenance["main_commit"],
            activation=self.activation,
            start_observation=self.start_observation,
            sample_plan=self.sample,
            decision_contract=self.decision_contract,
            reconciliation=self.reconciliation,
            measurement_source_hashes=self.measurement_source_hashes,
        )

    def test_initialize_binds_start_and_opens_only_protected_path(self) -> None:
        self.assertEqual(MONITORING, self.prepared["status"])
        self.assertEqual(
            self.start_observation["assignment_started_at_utc"],
            self.prepared["assignment_started_at_utc"],
        )
        self.assertTrue(
            self.prepared["release_boundaries"][
                "protected_safety_collection_workflow_allowed"
            ]
        )
        self.assertEqual("25,90 €", self.prepared["commerce_probe"]["price_text"])
        self.assertFalse(self.prepared["release_boundaries"]["manual_growthbook_stop_allowed"])

    def test_rejects_checkpoint_outside_exact_timing_gate(self) -> None:
        for observed_at in ("2026-09-05T06:59:59Z", "2026-09-05T08:00:01Z"):
            evidence = self.evidence()
            evidence["observed_at_utc"] = observed_at
            with self.subTest(observed_at=observed_at):
                with self.assertRaisesRegex(
                    recorder.CtaSafetyRecordingError,
                    "outside exact timing gate",
                ):
                    self.record(evidence)

    def test_accepts_current_due_index_after_an_unrecorded_prior_day(self) -> None:
        evidence = self.evidence()
        evidence["checkpoint_index"] = 3
        evidence["observed_at_utc"] = "2026-09-07T07:05:00Z"
        safety, measurement = self.record(evidence)
        self.assertEqual(3, safety["latest_checkpoint"]["checkpoint_index"])
        self.assertEqual(self.measurement, measurement)

    def test_continue_records_provenance_without_changing_stop_lifecycle(self) -> None:
        safety, measurement = self.record(self.evidence())
        self.assertEqual(MONITORING, safety["status"])
        self.assertEqual("CONTINUE", safety["latest_checkpoint"]["verdict"])
        self.assertEqual(self.measurement, measurement)
        self.assertFalse(
            safety["release_boundaries"]["manual_growthbook_stop_allowed"]
        )
        self.assertFalse(safety["latest_checkpoint"]["stop_reasons"])

    def test_commerce_breach_opens_only_manual_stop_and_bridges_measurement(self) -> None:
        safety, measurement = self.record(self.evidence(stop="commerce"))
        self.assertEqual(STOP_REVIEW, safety["status"])
        self.assertTrue(
            safety["release_boundaries"]["manual_growthbook_stop_allowed"]
        )
        self.assertFalse(
            safety["release_boundaries"]["automatic_growthbook_mutation_allowed"]
        )
        self.assertEqual(window_validator.RESOLVED, measurement["status"])
        self.assertEqual(
            "safety_guardrail",
            measurement["assignment_stop"]["review_trigger_type"],
        )
        self.assertEqual(
            safety["latest_checkpoint"]["decision_sha256"],
            measurement["assignment_stop"]["review_trigger_decision_sha256"],
        )
        self.assertTrue(measurement["assignment_stop"]["manual_review_allowed"])
        self.assertFalse(measurement["assignment_stop"]["automatic_stop_allowed"])

    def test_performance_breach_uses_same_manual_stop_path(self) -> None:
        safety, measurement = self.record(self.evidence(stop="performance"))
        self.assertEqual(["lcp_regression"], safety["stop_handoff"]["stop_reasons"])
        self.assertEqual(
            "safety_guardrail_stop_required",
            measurement["measurement_window"]["resolved_reason"],
        )

    def test_rejects_drifted_measurement_even_for_continue(self) -> None:
        evidence = self.evidence()
        decision, provenance, hashes = self.artifact(evidence)
        drifted = copy.deepcopy(self.measurement)
        drifted["measurement_window"]["target_total_sample"] += 1
        with self.assertRaisesRegex(
            recorder.CtaSafetyRecordingError,
            "measurement source invalid",
        ):
            recorder.record_checkpoint(
                self.enabled,
                drifted,
                evidence,
                decision,
                provenance,
                evidence_sha256=hashes["evidence"],
                decision_sha256=hashes["decision"],
                provenance_sha256=hashes["provenance"],
                expected_workflow_run_id=provenance["workflow_run_id"],
                expected_main_commit=provenance["main_commit"],
                activation=self.activation,
                start_observation=self.start_observation,
                sample_plan=self.sample,
                decision_contract=self.decision_contract,
                reconciliation=self.reconciliation,
                measurement_source_hashes=self.measurement_source_hashes,
            )

    def test_rejects_swapped_independent_workflow_identity(self) -> None:
        evidence = self.evidence()
        decision, provenance, hashes = self.artifact(evidence)
        for field, value, message in (
            ("expected_workflow_run_id", "60000000002", "workflow run mismatch"),
            ("expected_main_commit", "b" * 40, "main commit mismatch"),
        ):
            arguments = {
                "evidence_sha256": hashes["evidence"],
                "decision_sha256": hashes["decision"],
                "provenance_sha256": hashes["provenance"],
                "expected_workflow_run_id": provenance["workflow_run_id"],
                "expected_main_commit": provenance["main_commit"],
                "activation": self.activation,
                "start_observation": self.start_observation,
                "sample_plan": self.sample,
                "decision_contract": self.decision_contract,
                "reconciliation": self.reconciliation,
                "measurement_source_hashes": self.measurement_source_hashes,
            }
            arguments[field] = value
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    recorder.CtaSafetyRecordingError,
                    message,
                ):
                    recorder.record_checkpoint(
                        self.enabled,
                        self.measurement,
                        evidence,
                        decision,
                        provenance,
                        **arguments,
                    )

    def test_rejects_forged_decision_or_provenance(self) -> None:
        evidence = self.evidence(stop="commerce")
        decision, provenance, hashes = self.artifact(evidence)
        forged_decision = copy.deepcopy(decision)
        forged_decision["verdict"] = "CONTINUE"
        with self.assertRaisesRegex(
            recorder.CtaSafetyRecordingError,
            "independent evaluation",
        ):
            recorder.record_checkpoint(
                self.enabled,
                self.measurement,
                evidence,
                forged_decision,
                provenance,
                evidence_sha256=hashes["evidence"],
                decision_sha256=hashes["decision"],
                provenance_sha256=hashes["provenance"],
                expected_workflow_run_id=provenance["workflow_run_id"],
                expected_main_commit=provenance["main_commit"],
                activation=self.activation,
                start_observation=self.start_observation,
                sample_plan=self.sample,
                decision_contract=self.decision_contract,
                reconciliation=self.reconciliation,
                measurement_source_hashes=self.measurement_source_hashes,
            )
        forged_provenance = copy.deepcopy(provenance)
        forged_provenance["main_commit"] = "b" * 40
        with self.assertRaisesRegex(
            recorder.CtaSafetyRecordingError,
            "main commit mismatch",
        ):
            recorder.record_checkpoint(
                self.enabled,
                self.measurement,
                evidence,
                decision,
                forged_provenance,
                evidence_sha256=hashes["evidence"],
                decision_sha256=hashes["decision"],
                provenance_sha256=hashes["provenance"],
                expected_workflow_run_id=provenance["workflow_run_id"],
                expected_main_commit=provenance["main_commit"],
                activation=self.activation,
                start_observation=self.start_observation,
                sample_plan=self.sample,
                decision_contract=self.decision_contract,
                reconciliation=self.reconciliation,
                measurement_source_hashes=self.measurement_source_hashes,
            )


if __name__ == "__main__":
    unittest.main()
