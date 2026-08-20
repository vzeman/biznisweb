import json
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from reporting_core.experiments import (
    DEVICE_FACT_FIELDS,
    ExperimentBuildConfig,
    ExperimentDataError,
    build_experiment_facts,
    load_experiment_build_config,
    publish_experiment_facts,
)


BASE = datetime(2026, 8, 20, 12, 0, 0, tzinfo=timezone.utc)
EXPERIMENT = "vevo-sk-product-cta-color-001"


def uid() -> str:
    return str(uuid.uuid4())


def iso(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def event(
    event_name="experiment_exposure",
    *,
    device_id=None,
    variation_id="control",
    received_at=BASE,
    experiment_id=EXPERIMENT,
    event_id=None,
    **specific,
):
    result = {
        "schema_version": 1,
        "event_id": event_id or uid(),
        "event_name": event_name,
        "occurred_at": iso(received_at - timedelta(seconds=1)),
        "received_at": iso(received_at),
        "event_date": received_at.date().isoformat(),
        "device_id": device_id or uid(),
        "page_path": "/p-1/example",
        "page_type": "product",
        "consent_state": "analytics_granted",
        "experiment_id": experiment_id,
        "variation_id": variation_id,
        "utm_source": "meta",
        "utm_medium": "paid_social",
        "meta_campaign_id": "123",
        "meta_adset_id": "456",
        "meta_ad_id": "789",
        "meta_placement": "instagram_feed",
        "collector_version": "test",
        "risk_result": "accepted",
    }
    result.update(specific)
    return result


def order(
    order_num="2602000001",
    *,
    order_at=BASE + timedelta(hours=1),
    revenue=50,
    cm1=20,
    state="realized",
    mature=True,
    excluded=False,
):
    return {
        "order_num": order_num,
        "order_at": iso(order_at),
        "net_revenue_eur": revenue,
        "cm1_eur": cm1,
        "lifecycle_state": state,
        "mature": mature,
        "excluded": excluded,
    }


def config():
    return ExperimentBuildConfig(
        metric_contract_version="vevo_cm1_v1_2026-08-20",
        expected_variation_weights={EXPERIMENT: {"control": 0.5, "brand_contrast": 0.5}},
    )


class FakeS3:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


class GrowthBookReportingTests(unittest.TestCase):
    def build(self, events, orders=()):
        return build_experiment_facts(
            events,
            orders,
            config=config(),
            generated_at=BASE + timedelta(days=15),
        )

    def test_builds_joined_device_and_health_facts(self):
        device_id = uid()
        page_load_id = uid()
        events = [
            event(device_id=device_id),
            event(
                "add_to_cart",
                device_id=device_id,
                received_at=BASE + timedelta(minutes=5),
                product_id="1531",
            ),
            event(
                "order_completed",
                device_id=device_id,
                received_at=BASE + timedelta(hours=1),
                transaction_id="2602000001",
            ),
            event(
                "performance_vital",
                device_id=device_id,
                received_at=BASE + timedelta(minutes=1),
                page_load_id=page_load_id,
                vital_name="lcp_ms",
                vital_value=1300,
            ),
            event(
                "client_error_observed",
                device_id=device_id,
                received_at=BASE + timedelta(minutes=2),
                page_load_id=page_load_id,
                error_kind="runtime_error",
            ),
        ]

        bundle = self.build(events, [order()])

        self.assertEqual(1, len(bundle.device_facts))
        fact = bundle.device_facts[0]
        self.assertEqual(DEVICE_FACT_FIELDS, set(fact))
        self.assertEqual(1, fact["add_to_cart_24h"])
        self.assertEqual(1, fact["purchase_converted"])
        self.assertEqual(50.0, fact["net_revenue_eur"])
        self.assertEqual(20.0, fact["cm1_eur"])
        self.assertEqual(1, fact["client_error_observed"])
        self.assertEqual(1, fact["eligible"])
        self.assertEqual(1, fact["order_attribution_eligible"])
        self.assertEqual(1300, bundle.performance_facts[0]["vital_value"])
        quality = bundle.quality_reports[0]
        self.assertEqual(100.0, quality["exact_join_rate_pct"])
        self.assertEqual(1300.0, quality["variation_health"]["control"]["lcp_p75_ms"])

    def test_nonbuyer_is_retained_with_zero_value_and_late_cart_is_zero(self):
        device_id = uid()
        bundle = self.build(
            [
                event(device_id=device_id),
                event(
                    "add_to_cart",
                    device_id=device_id,
                    received_at=BASE + timedelta(hours=24, seconds=1),
                    product_id="1531",
                ),
            ]
        )
        fact = bundle.device_facts[0]
        self.assertEqual(0, fact["add_to_cart_24h"])
        self.assertEqual(0, fact["purchase_converted"])
        self.assertEqual(0.0, fact["net_revenue_eur"])
        self.assertEqual(1, fact["order_attribution_eligible"])

    def test_cross_variation_exposure_is_contaminated_and_excluded(self):
        device_id = uid()
        bundle = self.build(
            [
                event(device_id=device_id, variation_id="control"),
                event(
                    device_id=device_id,
                    variation_id="brand_contrast",
                    received_at=BASE + timedelta(minutes=1),
                ),
            ]
        )
        fact = bundle.device_facts[0]
        self.assertEqual(1, fact["contaminated"])
        self.assertEqual(0, fact["eligible"])
        self.assertEqual("variation_contamination", fact["exclusion_reason"])

    def test_duplicate_event_is_deduplicated_but_conflict_fails_closed(self):
        duplicate = event()
        bundle = self.build([duplicate, dict(duplicate)])
        self.assertEqual(1, len(bundle.device_facts))
        self.assertEqual(1, bundle.quality_reports[0]["duplicate_event_count"])

        conflicting = dict(duplicate)
        conflicting["variation_id"] = "brand_contrast"
        with self.assertRaises(ExperimentDataError):
            self.build([duplicate, conflicting])

    def test_transaction_seen_on_two_devices_is_not_double_attributed(self):
        first_device = uid()
        second_device = uid()
        events = []
        for device_id in (first_device, second_device):
            events.extend(
                [
                    event(device_id=device_id),
                    event(
                        "order_completed",
                        device_id=device_id,
                        received_at=BASE + timedelta(hours=1),
                        transaction_id="2602000001",
                    ),
                ]
            )
        bundle = self.build(events, [order()])
        self.assertEqual([0, 0], [row["joined_order_count"] for row in bundle.device_facts])
        self.assertEqual([1, 1], [row["ambiguous_transaction_count"] for row in bundle.device_facts])
        self.assertEqual(1, bundle.quality_reports[0]["ambiguous_transaction_count"])
        self.assertEqual(0, bundle.quality_reports[0]["attributed_transaction_count"])

    def test_unmatched_transaction_is_zero_value_and_reduces_join_rate(self):
        device_id = uid()
        bundle = self.build(
            [
                event(device_id=device_id),
                event(
                    "order_completed",
                    device_id=device_id,
                    received_at=BASE + timedelta(minutes=3),
                    transaction_id="missing-order",
                ),
            ]
        )
        fact = bundle.device_facts[0]
        self.assertEqual(0, fact["purchase_converted"])
        self.assertEqual(1, fact["unmatched_transaction_count"])
        self.assertEqual(0.0, bundle.quality_reports[0]["exact_join_rate_pct"])

    def test_raw_and_order_boundaries_reject_extra_pii_fields(self):
        raw = event()
        raw["email"] = "forbidden@example.com"
        with self.assertRaises(ExperimentDataError):
            self.build([raw])

        safe_order = order()
        safe_order["customer_email"] = "forbidden@example.com"
        with self.assertRaises(ExperimentDataError):
            self.build([event()], [safe_order])

        with self.assertRaises(ExperimentDataError):
            self.build([event(variation_id="unknown")])

    def test_performance_keeps_first_measurement_per_page_load_and_vital(self):
        device_id = uid()
        page_load_id = uid()
        bundle = self.build(
            [
                event(device_id=device_id),
                event(
                    "performance_vital",
                    device_id=device_id,
                    received_at=BASE + timedelta(minutes=1),
                    page_load_id=page_load_id,
                    vital_name="lcp_ms",
                    vital_value=1000,
                ),
                event(
                    "performance_vital",
                    device_id=device_id,
                    received_at=BASE + timedelta(minutes=2),
                    page_load_id=page_load_id,
                    vital_name="lcp_ms",
                    vital_value=9999,
                ),
            ]
        )
        self.assertEqual(1, len(bundle.performance_facts))
        self.assertEqual(1000, bundle.performance_facts[0]["vital_value"])
        self.assertEqual(1, bundle.quality_reports[0]["performance_duplicate_count"])

    def test_srm_alert_uses_frozen_point_zero_zero_one_threshold(self):
        events = [event(device_id=uid(), variation_id="control") for _ in range(90)]
        events.extend(event(device_id=uid(), variation_id="brand_contrast") for _ in range(10))
        report = self.build(events).quality_reports[0]
        self.assertTrue(report["srm_alert"])
        self.assertLess(report["srm_p_value"], 0.001)
        self.assertEqual({"control": 90, "brand_contrast": 10}, report["variation_counts"])

    def test_lifecycle_and_maturity_are_preserved_without_customer_data(self):
        device_id = uid()
        events = [event(device_id=device_id)]
        orders = []
        for offset, state in enumerate(("cancelled", "refunded"), start=1):
            order_num = f"260200000{offset}"
            events.append(
                event(
                    "order_completed",
                    device_id=device_id,
                    received_at=BASE + timedelta(hours=offset),
                    transaction_id=order_num,
                )
            )
            orders.append(
                order(
                    order_num,
                    order_at=BASE + timedelta(hours=offset),
                    state=state,
                    mature=False,
                )
            )
        fact = self.build(events, orders).device_facts[0]
        self.assertEqual(1, fact["cancelled_order_count"])
        self.assertEqual(1, fact["refunded_order_count"])
        self.assertEqual(2, fact["immature_order_count"])
        self.assertNotIn("customer", json.dumps(fact).lower())

    def test_config_file_is_exact_and_matches_frozen_windows(self):
        loaded = load_experiment_build_config(Path("projects/vevo/growthbook_reporting.json"))
        self.assertEqual(24, loaded.cart_window_hours)
        self.assertEqual(7, loaded.order_window_days)
        self.assertEqual(14, loaded.maturity_checkpoint_days)
        self.assertEqual(0.5, loaded.expected_variation_weights[EXPERIMENT]["brand_contrast"])

        with tempfile.TemporaryDirectory() as temp_dir:
            invalid_path = Path(temp_dir) / "invalid.json"
            invalid_path.write_text('{"schema_version":1,"secret":"x"}', encoding="utf-8")
            with self.assertRaises(ExperimentDataError):
                load_experiment_build_config(invalid_path)

    def test_publisher_uses_partitioned_curated_paths_and_sse(self):
        device_id = uid()
        page_load_id = uid()
        bundle = self.build(
            [
                event(device_id=device_id),
                event(
                    "performance_vital",
                    device_id=device_id,
                    received_at=BASE + timedelta(minutes=1),
                    page_load_id=page_load_id,
                    vital_name="inp_ms",
                    vital_value=100,
                ),
            ]
        )
        s3 = FakeS3()
        counts = publish_experiment_facts(s3, bucket="test-bucket", bundle=bundle)
        self.assertEqual({"device_facts": 1, "performance_facts": 1, "quality_reports": 1}, counts)
        self.assertEqual(3, len(s3.calls))
        self.assertIn(f"device_facts/experiment_id={EXPERIMENT}/{device_id}.json", s3.calls[0]["Key"])
        self.assertIn(
            f"performance_facts/experiment_id={EXPERIMENT}/{page_load_id}-inp_ms.json",
            s3.calls[1]["Key"],
        )
        self.assertEqual("AES256", s3.calls[0]["ServerSideEncryption"])
        stored_device = json.loads(s3.calls[0]["Body"])
        self.assertNotIn("experiment_id", stored_device)


if __name__ == "__main__":
    unittest.main()
