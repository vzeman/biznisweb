from __future__ import annotations

import copy
import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone

from scripts import evaluate_growthbook_cta as evaluator
from scripts import freeze_growthbook_cta_sample as freezer


def _sum_squares(total: float, count: int, sample_variance: float) -> float:
    return ((count - 1) * sample_variance) + (total * total / count)


class GrowthBookCtaEvaluatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = json.loads(
            evaluator.DEFAULT_CONTRACT_PATH.read_text(encoding="utf-8")
        )
        self.pending_plan = json.loads(
            evaluator.DEFAULT_SAMPLE_PLAN_PATH.read_text(encoding="utf-8")
        )
        self.pending_lifecycle = json.loads(
            evaluator.DEFAULT_LIFECYCLE_PATH.read_text(encoding="utf-8")
        )
        self.plan = self._frozen_plan()
        self.lifecycle_observation = self._verified_lifecycle_observation()
        self.lifecycle = self._verified_lifecycle()

    def _frozen_plan(self) -> dict:
        plan = copy.deepcopy(self.pending_plan)
        baseline = 148 / 451
        target = baseline * 1.25
        plan["status"] = "sample_frozen_activation_still_blocked"
        plan["final"] = {
            "observation_sha256": "d" * 64,
            "aa_snapshot_sha256": "a" * 64,
            "aa_window_started_at_utc": "2026-07-01T00:00:00Z",
            "aa_window_ended_at_utc": "2026-07-08T00:00:00Z",
            "exposed_devices": 451,
            "converted_devices": 148,
            "baseline_rate_percent": round(100 * baseline, 6),
            "target_rate_percent": round(100 * target, 6),
            "sample_per_arm": 542,
            "total_sample": 1084,
            "frozen_at_utc": "2026-07-08T01:00:00Z",
        }
        plan["next_gate"] = (
            "verify_lifecycle_reconciliation_and_review_activation_before_launch"
        )
        freezer.validate_plan(plan)
        return plan

    def _verified_lifecycle_observation(self) -> dict:
        observation = {
            "schema_version": 2,
            "evidence_type": "vevo_growthbook_cta_prelaunch_lifecycle_reconciliation",
            "target_experiment_id": "vevo-sk-product-cta-color-001",
            "source_experiment_id": "vevo-sk-aa-001",
            "metric_contract_version": "vevo_cm1_v1_2026-08-20",
            "workflow_run_id": "12345678901",
            "main_commit": "d" * 40,
            "observed_at_utc": "2026-09-24T03:45:00Z",
            "source_from_utc": "2026-08-25T22:00:00Z",
            "source_through_utc": "2026-09-02T22:00:00Z",
            "order_window_days": 7,
            "lifecycle_checkpoint_days": 14,
            "minimum_followup_days_after_source_end": 21,
            "source_completion_sha256": "a" * 64,
            "source_aa_snapshot_sha256": "b" * 64,
            "query_template_sha256": "5a3548fa877d206e666c369fd19c4c4da121ccd2059354da55361fae86ecf9d5",
            "reporting_quality_object_key": (
                "experiment-events/curated/quality/experiment_id="
                "vevo-sk-aa-001/facts_generated_at=20260924T034500Z.json"
            ),
            "reporting_quality_object_sha256": "c" * 64,
            "eligible_devices_checked": 75,
            "joined_orders_checked": 75,
            "cm1_absolute_difference_eur": 0,
            "mature_orders_checked": 75,
            "immature_orders_checked": 0,
            "cancelled_orders_checked": 2,
            "refunded_or_creditnoted_orders_checked": 1,
            "direct_curated_cm1_sum_eur": 1192.4,
            "athena_reporting_cm1_sum_eur": 1192.4,
            "lifecycle_counts_match": True,
            "refund_creditnote_value_parity_verified": True,
            "non_realized_value_policy": (
                "zero_value_until_realized_with_explicit_lifecycle_counts"
            ),
            "non_realized_value_policy_verified": True,
            "cta_outcome_data_read": False,
            "contains_event_or_device_identity": False,
            "customer_or_order_identity_in_evidence": False,
            "source_read_only": True,
            "no_external_mutation": True,
        }
        evaluator.validate_lifecycle_observation(observation)
        return observation

    def _verified_lifecycle(self) -> dict:
        manifest = copy.deepcopy(self.pending_lifecycle)
        manifest.update(
            {
                "status": "verified_completed_aa_21d_lifecycle_preflight",
                "verified": True,
                "observation_path": (
                    "projects/vevo/growthbook_cta_lifecycle_observation.json"
                ),
                "observation_sha256": evaluator._sha256(self.lifecycle_observation),
                "workflow_run_id": "12345678901",
                "main_commit": "d" * 40,
                "source_completion_sha256": "a" * 64,
                "source_aa_snapshot_sha256": "b" * 64,
                "reporting_quality_object_key": (
                    "experiment-events/curated/quality/experiment_id="
                    "vevo-sk-aa-001/facts_generated_at=20260924T034500Z.json"
                ),
                "reporting_quality_object_sha256": "c" * 64,
                "verified_at_utc": "2026-09-24T04:00:00Z",
                "refund_creditnote_value_parity_verified": True,
                "non_realized_value_policy_verified": True,
            }
        )
        evaluator.validate_lifecycle_manifest(manifest, self.lifecycle_observation)
        return manifest

    def _variation(
        self,
        *,
        devices: int,
        carts: int,
        purchases: int,
        revenue_total: float,
        cm1_mean: float,
        cm1_variance: float = 1.0,
        lcp: float = 1300,
        inp: float = 150,
        cls: float = 0,
    ) -> dict:
        cm1_total = cm1_mean * devices
        return {
            "eligible_devices": devices,
            "add_to_cart_devices": carts,
            "purchase_devices": purchases,
            "joined_order_count": purchases,
            "net_revenue_sum_eur": revenue_total,
            "net_revenue_sum_squares_eur2": _sum_squares(revenue_total, devices, 100.0),
            "cm1_sum_eur": cm1_total,
            "cm1_sum_squares_eur2": _sum_squares(cm1_total, devices, cm1_variance),
            "cancelled_order_count": 1,
            "refunded_order_count": 0,
            "client_error_devices": 5,
            "measured_page_loads": max(200, devices - 20),
            "lcp_p75_ms": lcp,
            "inp_p75_ms": inp,
            "cls_p75_milli": cls,
        }

    def _snapshot(
        self,
        *,
        control_devices: int = 542,
        variant_devices: int = 542,
        control_carts: int = 178,
        variant_carts: int = 220,
        assignment_days: int = 14,
        assignment_stopped: bool = True,
        cm1_variant_mean: float = 1.2,
        cm1_variant_variance: float = 1.0,
        variant_lcp: float = 1350,
    ) -> dict:
        included = control_devices + variant_devices
        sample_hash = hashlib.sha256(
            freezer.canonical_json_bytes(self.plan)
        ).hexdigest()
        started = datetime(2026, 7, 1, 22, tzinfo=timezone.utc)
        ended = started + timedelta(days=assignment_days)
        evaluated = ended + timedelta(days=21)
        return {
            "schema_version": 1,
            "evidence_type": "vevo_growthbook_cta_aggregate_snapshot",
            "experiment_id": "vevo-sk-product-cta-color-001",
            "metric_contract_version": "vevo_cm1_v1_2026-08-20",
            "sample_plan_sha256": sample_hash,
            "aa_snapshot_sha256": "a" * 64,
            "lifecycle_reconciliation_sha256": self.lifecycle["observation_sha256"],
            "assignment_started_at_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "assignment_ended_at_utc": ended.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "evaluated_at_utc": evaluated.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "assignment_stopped": assignment_stopped,
            "production_allocation_percent": 100,
            "decision_cohort": {
                "selection_method": self.contract["decision_cohort"],
                "target_total_sample": 1084,
                "eligible_devices_seen_before_stop": included,
                "included_devices": included,
            },
            "quality": {
                "reporting_device_count": included,
                "growthbook_device_count": included,
                "duplicate_event_rate_percent": 0.1,
                "exact_joined_transaction_count": 75,
                "exact_join_rate_percent": 100,
                "unmatched_transaction_count": 0,
                "ambiguous_transaction_count": 0,
                "contaminated_device_count": 0,
                "privacy_audit_passed": True,
                "first_n_selection_query_verified": True,
                "all_exposures_24h_mature": True,
                "all_orders_7d_mature": True,
                "all_lifecycles_14d_mature": True,
                "price_integrity_passed": True,
                "cart_checkout_health_passed": True,
                "rollback_ready": True,
            },
            "variations": {
                "control": self._variation(
                    devices=control_devices,
                    carts=control_carts,
                    purchases=35,
                    revenue_total=2100,
                    cm1_mean=1.0,
                ),
                "brand_contrast": self._variation(
                    devices=variant_devices,
                    carts=variant_carts,
                    purchases=40,
                    revenue_total=2400,
                    cm1_mean=cm1_variant_mean,
                    cm1_variance=cm1_variant_variance,
                    lcp=variant_lcp,
                    inp=155,
                    cls=2,
                ),
            },
        }

    def _evaluate(self, snapshot: dict) -> dict:
        return evaluator.evaluate(
            snapshot,
            self.contract,
            self.plan,
            self.lifecycle,
            self.lifecycle_observation,
        )

    def test_checked_in_contract_and_pending_lifecycle_are_valid(self) -> None:
        evaluator.validate_contract(self.contract)
        evaluator.validate_lifecycle_manifest(self.pending_lifecycle)

    def test_declares_win_only_at_the_fixed_final_look_with_safe_guardrails(
        self,
    ) -> None:
        result = self._evaluate(self._snapshot())

        self.assertEqual("WIN", result["verdict"])
        self.assertTrue(result["final_decision"])
        self.assertEqual("brand_contrast", result["recommended_variation"])
        self.assertFalse(result["automatic_mutation_allowed"])
        self.assertLessEqual(result["primary_metric"]["two_sided_p_value"], 0.05)
        self.assertGreater(
            result["primary_metric"]["difference_ci_lower_percentage_points"], 0
        )
        self.assertTrue(all(gate["status"] == "pass" for gate in result["gates"]))

    def test_declares_lose_for_significant_primary_harm(self) -> None:
        result = self._evaluate(self._snapshot(variant_carts=140))

        self.assertEqual("LOSE", result["verdict"])
        self.assertLess(
            result["primary_metric"]["difference_ci_upper_percentage_points"], 0
        )
        self.assertEqual("control", result["recommended_variation"])

    def test_declares_inconclusive_at_fixed_sample_without_primary_evidence(
        self,
    ) -> None:
        result = self._evaluate(self._snapshot(variant_carts=180))

        self.assertEqual("INCONCLUSIVE", result["verdict"])
        self.assertTrue(result["final_decision"])

    def test_stays_not_ready_before_the_one_final_look(self) -> None:
        result = self._evaluate(
            self._snapshot(assignment_days=10, assignment_stopped=False)
        )

        self.assertEqual("NOT_READY", result["verdict"])
        self.assertFalse(result["final_decision"])

    def test_maximum_duration_with_insufficient_sample_is_inconclusive(self) -> None:
        snapshot = self._snapshot(
            control_devices=400,
            variant_devices=400,
            control_carts=131,
            variant_carts=150,
            assignment_days=42,
        )
        snapshot["quality"]["exact_joined_transaction_count"] = 75

        result = self._evaluate(snapshot)

        self.assertEqual("INCONCLUSIVE", result["verdict"])
        self.assertFalse(result["summary"]["target_reached"])
        self.assertTrue(result["summary"]["maximum_duration_reached"])

    def test_material_performance_harm_loses_even_when_primary_wins(self) -> None:
        result = self._evaluate(self._snapshot(variant_lcp=1700))

        self.assertEqual("LOSE", result["verdict"])
        performance_gate = next(
            gate for gate in result["gates"] if gate["name"] == "performance_guardrails"
        )
        self.assertEqual("fail", performance_gate["status"])

    def test_operational_safety_harm_can_stop_before_the_primary_look(self) -> None:
        result = self._evaluate(
            self._snapshot(
                assignment_days=5,
                assignment_stopped=False,
                variant_lcp=1700,
            )
        )

        self.assertEqual("LOSE", result["verdict"])
        self.assertTrue(result["summary"]["early_safety_stop"])
        self.assertEqual("control", result["recommended_variation"])

    def test_commerce_safety_stop_does_not_wait_for_quality_sample(self) -> None:
        snapshot = self._snapshot(assignment_days=1, assignment_stopped=False)
        snapshot["quality"]["exact_joined_transaction_count"] = 0
        snapshot["quality"]["exact_join_rate_percent"] = 0
        snapshot["quality"]["price_integrity_passed"] = False

        result = self._evaluate(snapshot)

        self.assertEqual("LOSE", result["verdict"])
        self.assertTrue(result["summary"]["early_safety_stop"])

    def test_uncertain_cm1_noninferiority_prevents_a_win(self) -> None:
        result = self._evaluate(
            self._snapshot(cm1_variant_mean=1.2, cm1_variant_variance=100.0)
        )

        self.assertEqual("INCONCLUSIVE", result["verdict"])
        cm1_gate = next(
            gate for gate in result["gates"] if gate["name"] == "cm1_business_guardrail"
        )
        self.assertEqual("not_ready", cm1_gate["status"])

    def test_pending_sample_and_lifecycle_cannot_produce_a_decision(self) -> None:
        snapshot = self._snapshot()
        snapshot["sample_plan_sha256"] = hashlib.sha256(
            freezer.canonical_json_bytes(self.pending_plan)
        ).hexdigest()

        result = evaluator.evaluate(
            snapshot,
            self.contract,
            self.pending_plan,
            self.pending_lifecycle,
        )

        self.assertEqual("NOT_READY", result["verdict"])
        self.assertFalse(result["final_decision"])

    def test_missing_lifecycle_evidence_stays_not_ready_at_fixed_look(self) -> None:
        snapshot = self._snapshot()
        snapshot["lifecycle_reconciliation_sha256"] = "0" * 64

        result = evaluator.evaluate(
            snapshot,
            self.contract,
            self.plan,
            self.pending_lifecycle,
        )

        self.assertEqual("NOT_READY", result["verdict"])
        self.assertFalse(result["final_decision"])

    def test_lifecycle_observation_rejects_sub_cent_parity(self) -> None:
        observation = copy.deepcopy(self.lifecycle_observation)
        observation["direct_curated_cm1_sum_eur"] = 1192.401
        observation["cm1_absolute_difference_eur"] = 0

        with self.assertRaisesRegex(evaluator.CtaEvaluationError, "cent precision"):
            evaluator.validate_lifecycle_observation(observation)

    def test_rejects_extra_identity_or_free_form_fields(self) -> None:
        snapshot = self._snapshot()
        snapshot["customer_email"] = "forbidden@example.com"

        with self.assertRaisesRegex(evaluator.CtaEvaluationError, "extra"):
            self._evaluate(snapshot)


if __name__ == "__main__":
    unittest.main()
