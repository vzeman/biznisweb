from __future__ import annotations

import contextlib
import io
import json
import math
import pathlib
import tempfile
import unittest

from scripts.evaluate_growthbook_aa import (
    AaEvaluationError,
    _srm,
    evaluate,
    load_config,
    main,
)


CONFIG_PATH = pathlib.Path("projects/vevo/growthbook_aa_acceptance.json")


def snapshot() -> dict[str, object]:
    reporting_counts = {"control": 510, "variant": 490}
    return {
        "schema_version": 1,
        "experiment_id": "vevo-sk-aa-001",
        "full_allocation_started_at_utc": "2026-08-22T22:00:00Z",
        "evaluated_at_utc": "2026-08-29T22:00:00Z",
        "production_allocation_percent": 100,
        "identical_variations_verified": True,
        "growthbook_srm_warning": False,
        "pipeline_counts": {
            "collector_received_event_count": 5010,
            "collector_unique_accepted_event_count": 5000,
            "collector_duplicate_event_count": 10,
            "athena_unique_event_count": 4995,
            "reporting_unique_event_count": 4990,
        },
        "growthbook_variation_counts": {"control": 509, "variant": 491},
        "reporting_quality": {
            "raw_event_count": 4995,
            "unique_event_count": 4990,
            "duplicate_event_count": 5,
            "orphan_event_count": 0,
            "eligible_device_count": 1000,
            "contaminated_device_count": 0,
            "srm_p_value": _srm(reporting_counts, {"control": 0.5, "variant": 0.5}),
            "unique_transaction_count": 60,
            "exact_joined_transaction_count": 60,
            "unmatched_transaction_count": 0,
            "ambiguous_transaction_count": 0,
            "variation_health": {
                "control": {
                    "eligible_devices": 510,
                    "measured_page_loads": 250,
                    "lcp_p75_ms": 1300,
                    "inp_p75_ms": 100,
                    "cls_p75_milli": 5,
                    "client_error_device_rate_pct": 0.2,
                },
                "variant": {
                    "eligible_devices": 490,
                    "measured_page_loads": 240,
                    "lcp_p75_ms": 1350,
                    "inp_p75_ms": 105,
                    "cls_p75_milli": 6,
                    "client_error_device_rate_pct": 0.3,
                },
            },
        },
        "meta_dimension_audit": {
            "meta_exposure_count": 320,
            "complete_stable_dimension_exposure_count": 120,
            "invalid_dimension_row_count": 0,
            "forbidden_click_identifier_count": 0,
        },
        "privacy_audit": {
            "total_stored_row_count": 5990,
            "sampled_row_count": 100,
            "pii_finding_count": 0,
            "forbidden_field_finding_count": 0,
            "raw_ip_address_stored_count": 0,
            "full_url_stored_count": 0,
            "click_identifier_stored_count": 0,
            "customer_field_stored_count": 0,
        },
        "consent_audit": {
            "pre_consent_request_count": 0,
            "non_analytical_consent_exposure_count": 0,
            "post_withdrawal_event_count": 0,
        },
        "commerce_health": {
            "checkout_runtime_error_count": 0,
            "duplicate_ga4_purchase_event_count": 0,
            "duplicate_meta_purchase_event_count": 0,
            "price_cart_checkout_mutation_observed": False,
            "add_to_cart_behavior_regression_observed": False,
            "rollback_test_passed": True,
        },
        "qa_checklist": {
            "desktop_passed": True,
            "mobile_passed": True,
            "consent_accept_passed": True,
            "consent_reject_passed": True,
            "consent_withdrawal_passed": True,
        },
    }


def gate(result: dict[str, object], key: str) -> dict[str, object]:
    return next(row for row in result["gates"] if row["key"] == key)


class GrowthBookAaEvaluatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(CONFIG_PATH)

    def test_passes_only_complete_sanitized_aa_evidence(self) -> None:
        result = evaluate(snapshot(), self.config)
        self.assertEqual("PASS", result["verdict"])
        self.assertFalse(result["winner_calls_allowed"])
        self.assertEqual(7, result["summary"]["full_calendar_days"])
        self.assertEqual(1000, result["summary"]["eligible_devices"])
        self.assertTrue(all(row["status"] == "pass" for row in result["gates"]))
        serialized = json.dumps(result, sort_keys=True)
        for forbidden in ("email", "phone", "address", "fbclid", "transaction_id"):
            self.assertNotIn(forbidden, serialized.lower())

    def test_not_ready_until_duration_population_performance_purchase_and_meta_samples(
        self,
    ) -> None:
        evidence = snapshot()
        evidence["full_allocation_started_at_utc"] = "2026-08-29T10:00:00Z"
        evidence["reporting_quality"]["eligible_device_count"] = 20
        for variation in ("control", "variant"):
            evidence["reporting_quality"]["variation_health"][variation][
                "eligible_devices"
            ] = 10
            evidence["reporting_quality"]["variation_health"][variation][
                "measured_page_loads"
            ] = 10
            evidence["growthbook_variation_counts"][variation] = 10
        counts = {"control": 10, "variant": 10}
        evidence["reporting_quality"]["srm_p_value"] = _srm(
            counts, self.config["expected_variation_weights"]
        )
        evidence["reporting_quality"]["unique_transaction_count"] = 0
        evidence["reporting_quality"]["exact_joined_transaction_count"] = 0
        evidence["meta_dimension_audit"]["meta_exposure_count"] = 0
        evidence["meta_dimension_audit"]["complete_stable_dimension_exposure_count"] = 0
        result = evaluate(evidence, self.config)
        self.assertEqual("NOT_READY", result["verdict"])
        for key in (
            "minimum_full_calendar_days",
            "minimum_eligible_devices",
            "eligible_exposure_split",
            "independent_srm",
            "exact_order_join",
            "meta_dimension_contract",
            "performance_guardrails",
        ):
            self.assertEqual("not_ready", gate(result, key)["status"])

    def test_hard_safety_and_data_quality_failures_never_become_not_ready(self) -> None:
        mutations = {
            "variation_contamination": lambda value: value[
                "reporting_quality"
            ].__setitem__("contaminated_device_count", 1),
            "privacy_sample": lambda value: value["privacy_audit"].__setitem__(
                "pii_finding_count", 1
            ),
            "consent_boundary": lambda value: value["consent_audit"].__setitem__(
                "pre_consent_request_count", 1
            ),
            "commerce_health_and_rollback": lambda value: value[
                "commerce_health"
            ].__setitem__("checkout_runtime_error_count", 1),
            "desktop_mobile_consent_qa": lambda value: value[
                "qa_checklist"
            ].__setitem__("mobile_passed", False),
            "meta_dimension_contract": lambda value: value[
                "meta_dimension_audit"
            ].__setitem__("forbidden_click_identifier_count", 1),
        }
        for expected_gate, mutate in mutations.items():
            with self.subTest(gate=expected_gate):
                evidence = snapshot()
                mutate(evidence)
                result = evaluate(evidence, self.config)
                self.assertEqual("FAIL", result["verdict"])
                self.assertEqual("fail", gate(result, expected_gate)["status"])

    def test_meta_gate_requires_at_least_one_complete_stable_dimension_exposure(
        self,
    ) -> None:
        evidence = snapshot()
        evidence["meta_dimension_audit"]["meta_exposure_count"] = 320
        evidence["meta_dimension_audit"]["complete_stable_dimension_exposure_count"] = 0
        result = evaluate(evidence, self.config)
        self.assertEqual("NOT_READY", result["verdict"])
        meta_gate = gate(result, "meta_dimension_contract")
        self.assertEqual("not_ready", meta_gate["status"])
        self.assertEqual(
            1,
            meta_gate["requirement"]["minimum_complete_stable_dimension_exposures"],
        )

    def test_fails_srm_split_pipeline_duplicate_join_population_and_performance_gates(
        self,
    ) -> None:
        cases = []

        evidence = snapshot()
        evidence["reporting_quality"]["variation_health"]["control"][
            "eligible_devices"
        ] = 900
        evidence["reporting_quality"]["variation_health"]["variant"][
            "eligible_devices"
        ] = 100
        evidence["reporting_quality"]["srm_p_value"] = _srm(
            {"control": 900, "variant": 100}, self.config["expected_variation_weights"]
        )
        evidence["growthbook_variation_counts"] = {"control": 900, "variant": 100}
        cases.append((evidence, "independent_srm"))

        evidence = snapshot()
        evidence["pipeline_counts"]["athena_unique_event_count"] = 4500
        cases.append((evidence, "collector_athena_reporting_count_parity"))

        evidence = snapshot()
        evidence["pipeline_counts"].update(
            {
                "collector_received_event_count": 5100,
                "collector_unique_accepted_event_count": 5000,
                "collector_duplicate_event_count": 100,
            }
        )
        cases.append((evidence, "duplicate_accepted_event_rate"))

        evidence = snapshot()
        evidence["reporting_quality"]["exact_joined_transaction_count"] = 50
        evidence["reporting_quality"]["unmatched_transaction_count"] = 10
        cases.append((evidence, "exact_order_join"))

        evidence = snapshot()
        evidence["growthbook_variation_counts"] = {"control": 450, "variant": 550}
        cases.append((evidence, "growthbook_reporting_variation_parity"))

        evidence = snapshot()
        evidence["reporting_quality"]["variation_health"]["variant"]["lcp_p75_ms"] = (
            1700
        )
        cases.append((evidence, "performance_guardrails"))

        for value, expected_gate in cases:
            with self.subTest(gate=expected_gate):
                result = evaluate(value, self.config)
                self.assertEqual("FAIL", result["verdict"])
                self.assertEqual("fail", gate(result, expected_gate)["status"])

    def test_rejects_schema_drift_inconsistent_counts_and_false_srm_evidence(
        self,
    ) -> None:
        extra = snapshot()
        extra["customer_email"] = "forbidden@example.test"
        with self.assertRaisesRegex(AaEvaluationError, "field set drift"):
            evaluate(extra, self.config)

        inconsistent = snapshot()
        inconsistent["pipeline_counts"]["collector_duplicate_event_count"] = 9
        with self.assertRaisesRegex(AaEvaluationError, "count identity drift"):
            evaluate(inconsistent, self.config)

        false_srm = snapshot()
        false_srm["reporting_quality"]["srm_p_value"] = 0.5
        with self.assertRaisesRegex(AaEvaluationError, "independent recomputation"):
            evaluate(false_srm, self.config)

        non_finite = snapshot()
        non_finite["reporting_quality"]["variation_health"]["control"]["lcp_p75_ms"] = (
            math.inf
        )
        with self.assertRaisesRegex(AaEvaluationError, "finite"):
            evaluate(non_finite, self.config)

    def test_cli_require_pass_is_fail_closed_and_writes_canonical_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            snapshot_path = temporary / "snapshot.json"
            output_path = temporary / "decision.json"
            snapshot_path.write_text(json.dumps(snapshot()), encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    0,
                    main(
                        [
                            "--snapshot",
                            str(snapshot_path),
                            "--output",
                            str(output_path),
                            "--require-pass",
                        ]
                    ),
                )
            payload = output_path.read_bytes()
            self.assertTrue(payload.endswith(b"\n"))
            self.assertEqual("PASS", json.loads(payload)["verdict"])

            not_ready = snapshot()
            not_ready["production_allocation_percent"] = 0
            snapshot_path.write_text(json.dumps(not_ready), encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    3,
                    main(["--snapshot", str(snapshot_path), "--require-pass"]),
                )


if __name__ == "__main__":
    unittest.main()
