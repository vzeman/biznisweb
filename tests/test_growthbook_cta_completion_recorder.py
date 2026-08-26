from __future__ import annotations

import copy
import hashlib
import json
import unittest
from pathlib import Path

from scripts import record_growthbook_cta_completion as recorder
from scripts import record_growthbook_cta_safety_checkpoint as safety_recorder
from scripts import validate_growthbook_cta_measurement_window as window_validator
from scripts.evaluate_growthbook_cta_safety import STOPPED as SAFETY_STOPPED
from tests import test_growthbook_cta_evaluator as evaluator_fixtures
from tests import test_growthbook_cta_safety_recorder as safety_fixtures
from tests import test_growthbook_cta_window_checkpoint as window_fixtures


ROOT = Path(__file__).resolve().parents[1]


def load(relative: str) -> dict:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


class GrowthBookCtaCompletionRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        window = window_fixtures.GrowthBookCtaWindowCheckpointTests(
            methodName="runTest"
        )
        window.setUp()
        self.activation = window.activation
        self.start_observation = window.start_observation
        self.sample = window.sample
        self.contract = window.contract
        self.reconciliation = window.reconciliation
        self.workspace = window.running_workspace
        self.measurement = window.record(
            window.running,
            window.evidence(
                1,
                window.expected["target_total_sample"],
                "open_manual_stop_review_target_reached",
            ),
        )
        self.safety = safety_recorder.initialize_monitoring(
            load("projects/vevo/growthbook_cta_safety_monitoring.json"),
            self.activation,
            self.start_observation,
            source_hashes=safety_recorder.source_hashes(
                self.activation,
                self.start_observation,
                (
                    ROOT
                    / "projects/vevo/growthbook_cta_decision_contract.json"
                ).read_bytes(),
            ),
        )
        evaluator = evaluator_fixtures.GrowthBookCtaEvaluatorTests(
            methodName="runTest"
        )
        evaluator.setUp()
        self.lifecycle = evaluator.lifecycle
        self.lifecycle_observation = evaluator.lifecycle_observation
        self.completion = load("projects/vevo/growthbook_cta_completion.json")
        self.final_snapshot = load(
            "projects/vevo/growthbook_cta_final_snapshot.json"
        )
        self.stop_observation = self._stop_observation()
        self.stop_hash = hashlib.sha256(
            recorder.canonical_json_bytes(self.stop_observation)
        ).hexdigest()
        self.source_hashes = {
            "activation": hashlib.sha256(
                recorder.pretty_json_bytes(self.activation)
            ).hexdigest(),
            "measurement_window": hashlib.sha256(
                recorder.pretty_json_bytes(self.measurement)
            ).hexdigest(),
            "sample_plan": hashlib.sha256(
                recorder.pretty_json_bytes(self.sample)
            ).hexdigest(),
            "decision_contract": hashlib.sha256(
                (ROOT / "projects/vevo/growthbook_cta_decision_contract.json").read_bytes()
            ).hexdigest(),
            "safety_monitoring": hashlib.sha256(
                recorder.pretty_json_bytes(self.safety)
            ).hexdigest(),
            "lifecycle_reconciliation": hashlib.sha256(
                recorder.pretty_json_bytes(self.lifecycle)
            ).hexdigest(),
            "start_observation": hashlib.sha256(
                recorder.canonical_json_bytes(self.start_observation)
            ).hexdigest(),
            "reconciliation_evidence": hashlib.sha256(
                (
                    ROOT
                    / "projects/vevo/growthbook_production_reconciliation_deploy_evidence.json"
                ).read_bytes()
            ).hexdigest(),
        }

    def _stop_observation(self) -> dict:
        assignment_stop = self.measurement["assignment_stop"]
        return {
            "schema_version": 1,
            "evidence_type": "vevo_growthbook_cta_assignment_stop_readback",
            "experiment_id": "vevo-sk-product-cta-color-001",
            "feature_key": "vevo-sk-product-cta-color",
            "observed_at_utc": "2026-09-19T02:10:00Z",
            "assignment_ended_at_utc": "2026-09-19T02:00:00Z",
            "stop_trigger": {
                "type": assignment_stop["review_trigger_type"],
                "evidence_sha256": assignment_stop[
                    "review_trigger_evidence_sha256"
                ],
                "decision_sha256": assignment_stop[
                    "review_trigger_decision_sha256"
                ],
                "provenance_sha256": assignment_stop[
                    "review_trigger_provenance_sha256"
                ],
                "observed_at_utc": assignment_stop[
                    "review_trigger_observed_at_utc"
                ],
            },
            "activation_start_observation_sha256": self.activation[
                "start_readback"
            ]["observation_sha256"],
            "growthbook": {
                "build": "5.0.1+8f1db44",
                "project_id": "prj_2CeEJc6J9FwQFix9UhsnKr",
                "environment": "production",
                "experiment_id": "exp_19g6mmt1qxzrp",
                "experiment_status": "stopped",
                "production_live_rule_count": 0,
                "production_allocation_percent": 0,
                "feature_revision": 5,
                "feature_revision_status": "live",
                "feature_production_enabled": False,
                "feature_staging_enabled": True,
                "feature_live_rule_count_by_environment": {
                    "production": 0,
                    "staging": 1,
                },
                "active_production_experiments": [],
                "aa_status": "stopped_zero_allocation",
            },
            "gtm": {
                "account_id": "6254499282",
                "container_id": "198135331",
                "public_container_id": "GTM-5ZB5LFGB",
                "container_version_id": "15",
                "growthbook_loader_active": True,
                "unprocessed_changes": {"added": 0, "modified": 0, "removed": 0},
            },
            "storefront": {
                "product_path": "/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute",
                "desktop_verified": True,
                "mobile_verified": True,
                "cta_assignment_present": False,
                "brand_contrast_class_applied": False,
                "add_to_cart_text_unchanged": True,
                "console_error_count": 0,
                "price_mutated": False,
                "cart_mutated": False,
                "checkout_or_order_mutated": False,
            },
            "collector": {
                "post_stop_observation_window_seconds": 300,
                "post_stop_cta_exposure_count": 0,
                "post_stop_assignment_count": 0,
                "stop_boundary_verified": True,
            },
            "mutation_boundaries": {
                "growthbook_manual_mutation_performed": True,
                "growthbook_manual_mutation_scope": "stop_exact_cta_experiment_remove_only_production_rule_preserve_staging",
                "automatic_growthbook_mutation_performed": False,
                "gtm_mutation_performed": False,
                "meta_ads_mutation_performed": False,
                "biznisweb_mutation_performed": False,
                "collector_or_reporting_mutation_performed": False,
                "price_product_cart_checkout_order_mutation_performed": False,
            },
            "safety": {
                "contains_credentials": False,
                "contains_event_or_device_ids": False,
                "contains_customer_or_order_data": False,
                "winner_called": False,
            },
        }

    def _record(self, observation: dict | None = None):
        selected = observation or self.stop_observation
        digest = hashlib.sha256(recorder.canonical_json_bytes(selected)).hexdigest()
        return recorder.record_stop(
            self.completion,
            self.activation,
            self.measurement,
            self.safety,
            self.sample,
            self.contract,
            self.lifecycle,
            self.lifecycle_observation,
            self.reconciliation,
            self.workspace,
            self.final_snapshot,
            self.start_observation,
            selected,
            stop_observation_sha256=digest,
            source_hashes=self.source_hashes,
        )

    def test_checked_in_completion_is_fail_closed(self) -> None:
        recorder.validate_manifest(
            self.completion,
            load("projects/vevo/growthbook_cta_activation.json"),
            load("projects/vevo/growthbook_cta_measurement_window.json"),
            load("projects/vevo/growthbook_cta_sample_plan.json"),
            self.contract,
            load("projects/vevo/growthbook_cta_lifecycle_reconciliation.json"),
            self.reconciliation,
        )
        self.assertEqual(recorder.WAITING, self.completion["status"])
        self.assertFalse(any(self.completion["release_boundaries"].values()))

    def test_stop_records_zero_allocation_and_exact_followup(self) -> None:
        (
            completion,
            activation,
            measurement,
            safety,
            workspace,
            final_snapshot,
        ) = self._record()
        self.assertEqual(recorder.FOLLOWUP, completion["status"])
        self.assertEqual(recorder.CTA_STOPPED, activation["status"])
        self.assertEqual(window_validator.STOPPED, measurement["status"])
        self.assertEqual(SAFETY_STOPPED, safety["status"])
        self.assertEqual(
            "2026-10-03T02:00:00Z",
            completion["followup"]["final_snapshot_due_utc"],
        )
        self.assertTrue(
            completion["followup"]["protected_final_snapshot_workflow_allowed"]
        )
        self.assertFalse(any(completion["release_boundaries"].values()))
        self.assertEqual(
            "production_cta_stopped_followup_pro_quantiles_verified",
            workspace["state"],
        )
        self.assertEqual(0, workspace["workspace"]["production_allocation_percent"])
        self.assertEqual(
            "followup_pending_final_look_locked_until_due",
            final_snapshot["status"],
        )
        self.assertEqual(
            "2026-10-03T02:00:00Z",
            final_snapshot["final_look"]["snapshot_due_utc"],
        )
        self.assertTrue(
            final_snapshot["final_look"]["protected_workflow_allowed"]
        )
        self.assertTrue(
            final_snapshot["release_boundaries"][
                "diagnostic_host_gate_task_allowed"
            ]
        )

    def test_updated_measurement_binds_stopped_activation_hash(self) -> None:
        (
            completion,
            activation,
            measurement,
            _safety,
            _workspace,
            _final_snapshot,
        ) = self._record()
        activation_hash = hashlib.sha256(
            recorder.pretty_json_bytes(activation)
        ).hexdigest()
        measurement_hash = hashlib.sha256(
            recorder.pretty_json_bytes(measurement)
        ).hexdigest()
        self.assertEqual(
            activation_hash,
            measurement["source_bindings"]["activation_sha256"],
        )
        self.assertEqual(
            measurement_hash,
            completion["source_bindings"]["measurement_window_sha256"],
        )

    def test_verified_safety_breach_uses_same_manual_stop_followup(self) -> None:
        safety_case = safety_fixtures.GrowthBookCtaSafetyRecorderTests(
            methodName="runTest"
        )
        safety_case.setUp()
        safety, measurement = safety_case.record(
            safety_case.evidence(stop="commerce")
        )
        self.assertEqual(self.activation, safety_case.activation)
        stop_observation = copy.deepcopy(self.stop_observation)
        trigger = measurement["assignment_stop"]
        stop_observation["stop_trigger"] = {
            "type": trigger["review_trigger_type"],
            "evidence_sha256": trigger["review_trigger_evidence_sha256"],
            "decision_sha256": trigger["review_trigger_decision_sha256"],
            "provenance_sha256": trigger["review_trigger_provenance_sha256"],
            "observed_at_utc": trigger["review_trigger_observed_at_utc"],
        }
        source_hashes = dict(self.source_hashes)
        source_hashes["measurement_window"] = hashlib.sha256(
            recorder.pretty_json_bytes(measurement)
        ).hexdigest()
        source_hashes["safety_monitoring"] = hashlib.sha256(
            recorder.pretty_json_bytes(safety)
        ).hexdigest()
        digest = hashlib.sha256(
            recorder.canonical_json_bytes(stop_observation)
        ).hexdigest()
        (
            completion,
            _activation,
            stopped_measurement,
            stopped_safety,
            _workspace,
            final_snapshot,
        ) = recorder.record_stop(
            self.completion,
            safety_case.activation,
            measurement,
            safety,
            safety_case.sample,
            safety_case.decision_contract,
            self.lifecycle,
            self.lifecycle_observation,
            safety_case.reconciliation,
            self.workspace,
            self.final_snapshot,
            safety_case.start_observation,
            stop_observation,
            stop_observation_sha256=digest,
            source_hashes=source_hashes,
        )
        self.assertEqual(SAFETY_STOPPED, stopped_safety["status"])
        self.assertEqual(window_validator.STOPPED, stopped_measurement["status"])
        self.assertEqual(
            "safety_guardrail",
            stopped_measurement["assignment_stop"]["review_trigger_type"],
        )
        self.assertEqual(recorder.FOLLOWUP, completion["status"])
        self.assertTrue(
            final_snapshot["final_look"]["protected_workflow_allowed"]
        )

    def test_rejects_stop_before_reviewed_resolution(self) -> None:
        unresolved = copy.deepcopy(self.measurement)
        unresolved["status"] = window_validator.RUNNING
        unresolved["assignment_stop"] = {
            "status": "not_open",
            "manual_review_allowed": False,
            "automatic_stop_allowed": False,
            "review_trigger_type": None,
            "review_trigger_evidence_sha256": None,
            "review_trigger_decision_sha256": None,
            "review_trigger_provenance_sha256": None,
            "review_trigger_observed_at_utc": None,
            "observation_path": "projects/vevo/growthbook_cta_assignment_stop_observation.json",
            "observation_sha256": None,
            "assignment_ended_at_utc": None,
        }
        with self.assertRaisesRegex(
            recorder.CtaCompletionRecordingError,
            "reviewed stop rule is unresolved",
        ):
            recorder.record_stop(
                self.completion,
                self.activation,
                unresolved,
                self.safety,
                self.sample,
                self.contract,
                self.lifecycle,
                self.lifecycle_observation,
                self.reconciliation,
                self.workspace,
                self.final_snapshot,
                self.start_observation,
                self.stop_observation,
                stop_observation_sha256=self.stop_hash,
                source_hashes=self.source_hashes,
            )

    def test_rejects_continued_assignment_or_external_mutation(self) -> None:
        for target, field in (
            ("collector", "post_stop_assignment_count"),
            ("mutation_boundaries", "gtm_mutation_performed"),
            ("safety", "winner_called"),
        ):
            altered = copy.deepcopy(self.stop_observation)
            altered[target][field] = 1 if target == "collector" else True
            with self.subTest(target=target, field=field):
                with self.assertRaises(recorder.CtaCompletionRecordingError):
                    self._record(altered)

    def test_rejects_stop_before_resolved_checkpoint_or_without_revision(self) -> None:
        early = copy.deepcopy(self.stop_observation)
        early["assignment_ended_at_utc"] = "2026-09-19T01:00:00Z"
        with self.assertRaisesRegex(
            recorder.CtaCompletionRecordingError,
            "before the reviewed stopping rule resolved",
        ):
            self._record(early)
        stale = copy.deepcopy(self.stop_observation)
        stale["growthbook"]["feature_revision"] = self.activation["start_readback"][
            "feature_revision"
        ]
        with self.assertRaisesRegex(
            recorder.CtaCompletionRecordingError,
            "feature revision was not advanced",
        ):
            self._record(stale)


if __name__ == "__main__":
    unittest.main()
