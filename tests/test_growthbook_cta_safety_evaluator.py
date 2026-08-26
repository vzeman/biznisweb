from __future__ import annotations

import copy
import hashlib
import json
import math
import tempfile
import unittest
from pathlib import Path

from scripts.evaluate_growthbook_cta_safety import (
    CtaSafetyEvaluationError,
    canonical_json_bytes,
    evaluate,
    main,
    validate_contract,
    validate_snapshot,
)
from scripts.validate_growthbook_cta_safety_monitoring import main as validate_main


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_safety_monitoring.json"
DECISION_PATH = ROOT / "projects" / "vevo" / "growthbook_cta_decision_contract.json"


def load_contract() -> dict:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def health(
    *,
    eligible: int = 400,
    measured: int = 250,
    errors: int = 4,
    lcp: float | None = 1300,
    inp: float | None = 150,
    cls: float | None = 5,
) -> dict:
    return {
        "eligible_devices": eligible,
        "measured_page_loads": measured,
        "client_error_devices": errors,
        "lcp_p75_ms": lcp,
        "inp_p75_ms": inp,
        "cls_p75_milli": cls,
    }


def snapshot() -> dict:
    return {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_cta_safety_checkpoint",
        "experiment_id": "vevo-sk-product-cta-color-001",
        "checkpoint_index": 1,
        "assignment_started_at_utc": "2026-09-04T07:00:00Z",
        "observed_at_utc": "2026-09-05T07:00:00Z",
        "variation_health": {
            "control": health(),
            "brand_contrast": health(lcp=1350, inp=155, cls=6),
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


class GrowthBookCtaSafetyEvaluatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_contract()

    def test_checked_in_contract_is_fail_closed_and_hash_bound(self) -> None:
        validate_contract(self.contract)
        self.assertEqual(0, validate_main())
        self.assertEqual(
            hashlib.sha256(DECISION_PATH.read_bytes()).hexdigest(),
            self.contract["source_bindings"]["decision_contract"]["sha256"],
        )
        self.assertFalse(any(self.contract["release_boundaries"].values()))

    def test_safe_mature_checkpoint_continues_without_winner_or_mutation(self) -> None:
        decision = evaluate(snapshot(), self.contract)
        self.assertEqual("CONTINUE", decision["verdict"])
        self.assertTrue(decision["performance_mature"])
        self.assertFalse(decision["manual_stop_required"])
        self.assertEqual([], decision["stop_reasons"])
        self.assertFalse(any(decision["safety"].values()))

    def test_immature_performance_continues_but_does_not_compute_deltas(self) -> None:
        evidence = snapshot()
        for row in evidence["variation_health"].values():
            row.update(
                {
                    "measured_page_loads": 199,
                    "lcp_p75_ms": None,
                    "inp_p75_ms": None,
                    "cls_p75_milli": None,
                }
            )
        decision = evaluate(evidence, self.contract)
        self.assertEqual("CONTINUE_NOT_MATURE", decision["verdict"])
        self.assertFalse(decision["performance_mature"])
        self.assertTrue(all(value is None for value in decision["deltas"].values()))

    def test_reproducible_commerce_error_stops_before_performance_maturity(self) -> None:
        evidence = snapshot()
        for row in evidence["variation_health"].values():
            row.update(
                {
                    "measured_page_loads": 1,
                    "lcp_p75_ms": None,
                    "inp_p75_ms": None,
                    "cls_p75_milli": None,
                }
            )
        evidence["commerce_readback"][
            "reproducible_cart_or_checkout_runtime_error"
        ] = True
        decision = evaluate(evidence, self.contract)
        self.assertEqual("STOP_REQUIRED", decision["verdict"])
        self.assertTrue(decision["manual_stop_required"])
        self.assertEqual(
            ["reproducible_cart_or_checkout_runtime_error"],
            decision["stop_reasons"],
        )

    def test_each_commerce_integrity_breach_requires_immediate_manual_stop(self) -> None:
        cases = {
            "add_to_cart_text_changed": ("add_to_cart_text_unchanged", False),
            "price_changed": ("price_unchanged", False),
            "cart_checkout_or_order_mutated": (
                "cart_checkout_order_mutated",
                True,
            ),
        }
        for expected_reason, (field, value) in cases.items():
            with self.subTest(expected_reason=expected_reason):
                evidence = snapshot()
                for row in evidence["variation_health"].values():
                    row.update(
                        {
                            "measured_page_loads": 0,
                            "lcp_p75_ms": None,
                            "inp_p75_ms": None,
                            "cls_p75_milli": None,
                        }
                    )
                evidence["commerce_readback"][field] = value
                decision = evaluate(evidence, self.contract)
                self.assertEqual("STOP_REQUIRED", decision["verdict"])
                self.assertTrue(decision["manual_stop_required"])
                self.assertEqual([expected_reason], decision["stop_reasons"])

    def test_each_performance_threshold_is_strict_and_fail_closed(self) -> None:
        cases = {
            "lcp_regression": {"lcp_p75_ms": 1501},
            "inp_regression": {"inp_p75_ms": 171},
            "cls_regression": {"cls_p75_milli": 26},
            "client_error_rate_regression": {"client_error_devices": 7},
        }
        for expected_reason, changes in cases.items():
            with self.subTest(expected_reason=expected_reason):
                evidence = snapshot()
                evidence["variation_health"]["brand_contrast"].update(changes)
                decision = evaluate(evidence, self.contract)
                self.assertEqual("STOP_REQUIRED", decision["verdict"])
                self.assertIn(expected_reason, decision["stop_reasons"])

        boundary = snapshot()
        boundary["variation_health"]["brand_contrast"].update(
            {
                "lcp_p75_ms": 1500,
                "inp_p75_ms": 170,
                "cls_p75_milli": 25,
                "client_error_devices": 6,
            }
        )
        decision = evaluate(boundary, self.contract)
        self.assertEqual("CONTINUE", decision["verdict"])

    def test_primary_business_meta_and_identity_fields_are_rejected(self) -> None:
        for field in (
            "add_to_cart_devices",
            "purchase_devices",
            "revenue_eur",
            "cm1_eur",
            "meta_campaign_id",
            "winner",
        ):
            with self.subTest(field=field):
                evidence = snapshot()
                evidence[field] = 1
                with self.assertRaisesRegex(
                    CtaSafetyEvaluationError,
                    "forbidden outcome field",
                ):
                    validate_snapshot(evidence)

        evidence = snapshot()
        evidence["safety"]["contains_event_or_device_ids"] = True
        with self.assertRaisesRegex(
            CtaSafetyEvaluationError,
            "exceeded read boundary",
        ):
            validate_snapshot(evidence)

    def test_malformed_non_finite_or_incomplete_evidence_is_rejected(self) -> None:
        cases = []
        missing = snapshot()
        del missing["variation_health"]["control"]["measured_page_loads"]
        cases.append(missing)
        non_finite = snapshot()
        non_finite["variation_health"]["control"]["lcp_p75_ms"] = math.inf
        cases.append(non_finite)
        too_many_errors = snapshot()
        too_many_errors["variation_health"]["control"]["client_error_devices"] = 401
        cases.append(too_many_errors)
        bad_quality_type = snapshot()
        bad_quality_type["data_quality"]["query_complete"] = 0
        cases.append(bad_quality_type)
        measured_without_devices = snapshot()
        measured_without_devices["variation_health"]["control"].update(
            {"eligible_devices": 0, "client_error_devices": 0}
        )
        cases.append(measured_without_devices)
        timestamp_before_start = snapshot()
        timestamp_before_start["observed_at_utc"] = "2026-09-04T06:59:59Z"
        cases.append(timestamp_before_start)
        for evidence in cases:
            with self.subTest(evidence=evidence):
                with self.assertRaises(CtaSafetyEvaluationError):
                    validate_snapshot(evidence)

        mature_missing_metric = snapshot()
        mature_missing_metric["variation_health"]["control"]["lcp_p75_ms"] = None
        with self.assertRaisesRegex(
            CtaSafetyEvaluationError,
            "mature performance evidence is incomplete",
        ):
            evaluate(mature_missing_metric, self.contract)

    def test_each_data_quality_breach_requires_immediate_manual_stop(self) -> None:
        cases = {
            "query_incomplete": ("query_complete", False),
            "variation_set_invalid": ("exact_two_variations", False),
            "assignment_source_mismatch": ("assignment_source_match", False),
            "duplicate_or_conflicting_assignment": (
                "duplicate_or_conflicting_assignment_detected",
                True,
            ),
        }
        for expected_reason, (field, value) in cases.items():
            with self.subTest(expected_reason=expected_reason):
                evidence = snapshot()
                for row in evidence["variation_health"].values():
                    row.update(
                        {
                            "measured_page_loads": 0,
                            "lcp_p75_ms": None,
                            "inp_p75_ms": None,
                            "cls_p75_milli": None,
                        }
                    )
                evidence["data_quality"][field] = value
                decision = evaluate(evidence, self.contract)
                self.assertEqual("STOP_REQUIRED", decision["verdict"])
                self.assertEqual([expected_reason], decision["stop_reasons"])

    def test_cli_writes_only_canonical_safety_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence_path = root / "evidence.json"
            output_path = root / "decision.json"
            evidence_path.write_text(json.dumps(snapshot()), encoding="utf-8")
            self.assertEqual(
                0,
                main(
                    [
                        "--snapshot",
                        str(evidence_path),
                        "--contract",
                        str(CONTRACT_PATH),
                        "--output",
                        str(output_path),
                    ]
                ),
            )
            decision = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(output_path.read_bytes(), canonical_json_bytes(decision))
            lowered = output_path.read_text(encoding="utf-8").lower()
            for forbidden in (
                "add_to_cart_devices",
                "purchase_devices",
                "revenue",
                "cm1",
                "meta_campaign",
                "winner\":true",
            ):
                self.assertNotIn(forbidden, lowered)

    def test_contract_threshold_drift_fails(self) -> None:
        altered = copy.deepcopy(self.contract)
        altered["thresholds"]["lcp_degradation_absolute_ms"] = 201
        with self.assertRaisesRegex(CtaSafetyEvaluationError, "thresholds drift"):
            validate_contract(altered)


if __name__ == "__main__":
    unittest.main()
