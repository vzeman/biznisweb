from __future__ import annotations

import copy
import hashlib
import json
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

from reporting_core.experiments import (
    ExperimentBuildConfig,
    ExperimentDataError,
    ExperimentReceiptWindow,
    build_experiment_facts,
    publish_experiment_facts,
)
from scripts.build_growthbook_aa_quality_source import (
    QualitySourceError,
    build_quality_source,
    canonical_source_bytes,
    validate_quality_source,
    validate_quality_source_bytes,
)
from tests.test_growthbook_pipeline import event, uid


START = datetime(2026, 8, 25, 22, tzinfo=UTC)
END = START + timedelta(days=1)
WINDOW = ExperimentReceiptWindow(START - timedelta(days=1), START, END)
CONFIG = ExperimentBuildConfig(
    "vevo_cm1_v1_2026-08-20", {"vevo-sk-aa-001": {"control": 0.5, "variant": 0.5}}
)


def order(number: str, received_at: datetime) -> dict[str, object]:
    return {
        "order_num": number, "order_at": received_at.isoformat().replace("+00:00", "Z"),
        "net_revenue_eur": 20.0, "cm1_eur": 10.0, "lifecycle_state": "realized",
        "mature": False, "excluded": False,
    }


def build(events, orders=(), *, window=WINDOW):
    return build_experiment_facts(
        events, orders, config=CONFIG, generated_at=END + timedelta(hours=4),
        measurement_window=window,
    )


class ExactReceiptWindowTests(unittest.TestCase):
    def test_start_is_inclusive_and_through_is_exclusive_on_receipt_clock(self):
        included = event(received_at=START)
        included["occurred_at"] = (START - timedelta(days=2)).isoformat()
        excluded = event(received_at=END)
        excluded["occurred_at"] = (START + timedelta(minutes=1)).isoformat()
        before = event(received_at=START - timedelta(seconds=1))
        result = build([before, included, excluded])
        self.assertEqual([included["device_id"]], [row["device_id"] for row in result.device_facts])
        self.assertEqual(1, result.quality_reports[0]["raw_event_count"])
        self.assertEqual(WINDOW, result.measurement_window)

    def test_prior_exposure_context_prevents_reentering_the_cohort(self):
        prior = event(received_at=START - timedelta(hours=1))
        repeat = event(received_at=START, device_id=prior["device_id"])
        new = event(received_at=START + timedelta(seconds=1))
        result = build([prior, repeat, new])
        self.assertEqual([new["device_id"]], [row["device_id"] for row in result.device_facts])
        quality = result.quality_reports[0]
        self.assertEqual(2, quality["raw_event_count"])
        self.assertEqual(1, quality["exposed_device_count"])
        self.assertEqual(0, quality["orphan_event_count"])

    def test_post_through_contamination_outcomes_and_vitals_are_excluded(self):
        exposure = event(received_at=END - timedelta(minutes=1))
        same_device = {"device_id": exposure["device_id"]}
        rows = [exposure,
                event(received_at=END, variation_id="variant", **same_device),
                event("add_to_cart", received_at=END, product_id="test", **same_device),
                event("performance_vital", received_at=END, page_load_id=uid(),
                      vital_name="lcp_ms", vital_value=60000, **same_device),
                event("order_completed", received_at=END, transaction_id="later", **same_device)]
        result = build(rows)
        self.assertEqual(1, result.device_facts[0]["eligible"])
        self.assertEqual(0, result.device_facts[0]["add_to_cart_24h"])
        self.assertEqual((), result.performance_facts)
        self.assertEqual(0, result.quality_reports[0]["unique_transaction_count"])

    def test_within_window_contamination_is_not_hidden(self):
        first = event(received_at=START)
        changed = event(received_at=START + timedelta(seconds=1),
                        device_id=first["device_id"], variation_id="variant")
        result = build([first, changed])
        self.assertEqual(0, result.quality_reports[0]["eligible_device_count"])
        self.assertEqual(1, result.quality_reports[0]["contaminated_device_count"])

    def test_context_preserves_cross_cohort_transaction_ambiguity(self):
        prior = event(received_at=START - timedelta(hours=1))
        current = event(received_at=START)
        rows = [prior, current]
        for exposure in (prior, current):
            rows.append(event("order_completed", received_at=START + timedelta(minutes=1),
                              transaction_id="shared", device_id=exposure["device_id"]))
        quality = build(rows, [order("shared", START + timedelta(minutes=1))]).quality_reports[0]
        self.assertEqual(1, quality["unique_transaction_count"])
        self.assertEqual(1, quality["exact_joined_transaction_count"])
        self.assertEqual(1, quality["ambiguous_transaction_count"])
        self.assertEqual(0, quality["attributed_transaction_count"])

    def test_pre_window_subject_order_is_not_counted_as_cohort_order(self):
        prior = event(received_at=START - timedelta(hours=1))
        receipt = event("order_completed", received_at=START + timedelta(minutes=1),
                        transaction_id="prior", device_id=prior["device_id"])
        quality = build([prior, receipt, event(received_at=START)],
                        [order("prior", START + timedelta(minutes=1))]).quality_reports[0]
        self.assertEqual(2, quality["raw_event_count"])
        self.assertEqual(0, quality["unique_transaction_count"])
        self.assertEqual(0, quality["exact_joined_transaction_count"])

    def test_duplicate_and_orphan_counts_cover_only_window_receipts(self):
        prior = event("add_to_cart", received_at=START - timedelta(minutes=1), product_id="prior")
        inside = event("add_to_cart", received_at=START, product_id="inside")
        after = event("add_to_cart", received_at=END, product_id="after")
        quality = build([prior, prior, inside, inside, after, after]).quality_reports[0]
        self.assertEqual(2, quality["raw_event_count"])
        self.assertEqual(1, quality["unique_event_count"])
        self.assertEqual(1, quality["duplicate_event_count"])
        self.assertEqual(1, quality["orphan_event_count"])

    def test_in_window_vitals_and_exact_join_use_existing_metric_semantics(self):
        exposure = event(received_at=START)
        same = {"device_id": exposure["device_id"]}
        rows = [exposure, event("add_to_cart", received_at=START + timedelta(seconds=1), product_id="p", **same),
                event("performance_vital", received_at=START + timedelta(seconds=2),
                      page_load_id=uid(), vital_name="lcp_ms", vital_value=1234, **same),
                event("order_completed", received_at=START + timedelta(minutes=1),
                      transaction_id="included", **same)]
        orders = [order("included", START + timedelta(minutes=1))]
        windowed = build(rows, orders)
        ordinary = build(rows, orders, window=None)
        self.assertEqual(ordinary.quality_reports, windowed.quality_reports)
        self.assertEqual(ordinary.device_facts, windowed.device_facts)
        self.assertEqual(ordinary.performance_facts, windowed.performance_facts)

    def test_windowed_output_cannot_overwrite_ordinary_curated_data(self):
        s3 = Mock()
        with self.assertRaisesRegex(ExperimentDataError, "cannot overwrite"):
            publish_experiment_facts(s3, bucket="vevo-test", bundle=build([event(received_at=START)]))
        self.assertEqual([], s3.mock_calls)

    def test_invalid_or_incomplete_boundaries_fail_closed(self):
        for bounds in ((START, END, START), (END, START, END),
                       (START.replace(tzinfo=None), START, END),
                       (START, START.replace(microsecond=1), END)):
            with self.subTest(bounds=bounds), self.assertRaises(ExperimentDataError):
                ExperimentReceiptWindow(*bounds)
        with self.assertRaisesRegex(ExperimentDataError, "not complete"):
            build_experiment_facts([], [], config=CONFIG, generated_at=START, measurement_window=WINDOW)
        with self.assertRaisesRegex(ExperimentDataError, "predates"):
            build([event(received_at=WINDOW.context_from_utc - timedelta(seconds=1))])

    def test_partition_edge_schema_and_conflicting_ids_are_not_silently_ignored(self):
        invalid = event(received_at=END)
        invalid["email"] = "synthetic@example.invalid"
        with self.assertRaises(ExperimentDataError):
            build([invalid])
        first = event(received_at=START)
        conflict = event(received_at=END, event_id=first["event_id"])
        with self.assertRaisesRegex(ExperimentDataError, "conflicting"):
            build([first, conflict])


class QualitySourceContractTests(unittest.TestCase):
    def setUp(self):
        self.events = [event(received_at=START)]
        self.expected = {
            "expected_window": WINDOW, "expected_eligible_devices": 1,
            "expected_snapshot_manifest_sha256": "a" * 64,
            "expected_checkpoint_evidence_sha256": "b" * 64,
            "expected_workflow_run_id": "33952215185", "expected_main_commit": "c" * 40,
        }

    def source(self, events=None, orders=(), **overrides):
        args = {
            "config": CONFIG, "window": WINDOW, "generated_at": END + timedelta(hours=4),
            "expected_eligible_devices": 1, "snapshot_manifest_sha256": "a" * 64,
            "checkpoint_evidence_sha256": "b" * 64, "workflow_run_id": "33952215185",
            "main_commit": "c" * 40,
        }
        args.update(overrides)
        return build_quality_source(self.events if events is None else events, orders, **args)

    def test_builds_exact_identity_free_source_with_independent_provenance(self):
        source = self.source()
        validate_quality_source(source, **self.expected)
        text = canonical_source_bytes(source).decode()
        self.assertNotIn(self.events[0]["device_id"], text)
        self.assertNotIn(self.events[0]["event_id"], text)
        self.assertNotIn("device_facts", text)
        self.assertEqual(WINDOW.as_dict(), source["window"])
        self.assertEqual(1, source["quality"]["eligible_device_count"])

    def test_equal_counts_cannot_hide_missing_or_shifted_window(self):
        for kind in ("missing", "from", "through", "context"):
            source = self.source()
            if kind == "missing":
                del source["window"]
            else:
                key = {"from": "from_utc", "through": "through_utc", "context": "context_from_utc"}[kind]
                source["window"][key] = "2026-08-20T00:00:00Z"
            with self.subTest(kind=kind), self.assertRaises(QualitySourceError):
                validate_quality_source(source, **self.expected)

    def test_unbound_legacy_quality_is_not_an_exact_source(self):
        with self.assertRaisesRegex(QualitySourceError, "field set"):
            validate_quality_source(self.source()["quality"], **self.expected)

    def test_snapshot_checkpoint_run_commit_and_input_hashes_are_checked(self):
        for key in ("snapshot_manifest_sha256", "checkpoint_evidence_sha256", "workflow_run_id",
                    "main_commit", "workflow", "raw_extract_sha256", "authoritative_orders_sha256"):
            source = self.source()
            source["provenance"][key] = "wrong"
            with self.subTest(key=key), self.assertRaises(QualitySourceError):
                validate_quality_source(source, **self.expected)

    def test_wrong_population_or_unsafe_extras_fail_closed(self):
        with self.assertRaises(QualitySourceError):
            self.source(expected_eligible_devices=2)
        for part in ("quality", "provenance", "safety"):
            source = self.source()
            source[part]["customer"] = "forbidden"
            with self.subTest(part=part), self.assertRaises(QualitySourceError):
                validate_quality_source(source, **self.expected)
        source = self.source()
        source["safety"]["contains_customer_or_order_data"] = 0
        with self.assertRaises(QualitySourceError):
            validate_quality_source(source, **self.expected)

    def test_reordering_preserves_source_bytes_but_duplicate_input_changes_digest(self):
        second = event(received_at=START + timedelta(seconds=1), variation_id="variant")
        forward = self.source(self.events + [second], expected_eligible_devices=2)
        reverse = self.source([second] + self.events, expected_eligible_devices=2)
        self.assertEqual(canonical_source_bytes(forward), canonical_source_bytes(reverse))
        duplicate = self.source(self.events * 2)
        self.assertNotEqual(self.source()["provenance"]["raw_extract_sha256"],
                            duplicate["provenance"]["raw_extract_sha256"])

    def test_generation_must_be_complete_consistent_and_well_formed(self):
        for generated in (START, END.replace(tzinfo=None), END.replace(microsecond=1)):
            with self.subTest(generated=generated), self.assertRaises(QualitySourceError):
                self.source(generated_at=generated)
        source = self.source()
        source["provenance"]["generated_at_utc"] = "2026-99-99T01:00:00Z"
        with self.assertRaises(QualitySourceError):
            validate_quality_source(source, **self.expected)

    def test_order_inputs_are_minimal_exact_join_schema_and_not_exported(self):
        receipt = event("order_completed", received_at=START + timedelta(seconds=1),
                        transaction_id="private-order-reference", device_id=self.events[0]["device_id"])
        orders = [order("private-order-reference", START + timedelta(seconds=1))]
        source = self.source(self.events + [receipt], orders)
        self.assertEqual(1, source["quality"]["exact_joined_transaction_count"])
        self.assertNotIn("private-order-reference", canonical_source_bytes(source).decode())
        with self.assertRaisesRegex(QualitySourceError, "unrelated order"):
            self.source(orders=orders)
        unsafe = copy.deepcopy(orders)
        unsafe[0]["customer"] = "forbidden"
        with self.assertRaises(QualitySourceError):
            self.source(self.events + [receipt], unsafe)

    def test_metric_contract_cannot_change_to_make_source_pass(self):
        altered = ExperimentBuildConfig(CONFIG.metric_contract_version,
                                        CONFIG.expected_variation_weights, health_window_hours=48)
        with self.assertRaisesRegex(QualitySourceError, "metric contract"):
            self.source(config=altered)

    def test_other_experiment_is_rejected_even_at_partition_edge(self):
        other = event(received_at=END, experiment_id="different-experiment")
        with self.assertRaisesRegex(QualitySourceError, "different experiment"):
            self.source(self.events + [other])

    def test_download_requires_canonical_bytes_and_independently_supplied_hash(self):
        source = self.source()
        raw = canonical_source_bytes(source)
        digest = hashlib.sha256(raw).hexdigest()
        self.assertEqual(source, validate_quality_source_bytes(
            raw, expected_sha256=digest, **self.expected
        ))
        with self.assertRaisesRegex(QualitySourceError, "SHA-256"):
            validate_quality_source_bytes(raw, expected_sha256="0" * 64, **self.expected)
        pretty = json.dumps(source, indent=2).encode()
        with self.assertRaisesRegex(QualitySourceError, "canonical"):
            validate_quality_source_bytes(pretty, expected_sha256=hashlib.sha256(pretty).hexdigest(),
                                          **self.expected)


if __name__ == "__main__":
    unittest.main()
