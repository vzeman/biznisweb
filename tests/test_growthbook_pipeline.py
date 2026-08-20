import io
import json
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

from reporting_core.experiment_io import load_raw_experiment_events
from reporting_core.experiment_orders import build_biznisweb_authoritative_orders
from reporting_core.experiments import (
    ORDER_FIELDS,
    ExperimentDataError,
    order_completion_receipts,
)


BASE = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)


def uid():
    return str(uuid.uuid4())


def event(event_name="experiment_exposure", *, received_at=BASE, **specific):
    row = {
        "schema_version": 1,
        "event_id": uid(),
        "event_name": event_name,
        "occurred_at": (received_at - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
        "received_at": received_at.isoformat().replace("+00:00", "Z"),
        "event_date": received_at.date().isoformat(),
        "device_id": uid(),
        "page_path": "/p-1/example",
        "page_type": "product",
        "consent_state": "analytics_granted",
        "experiment_id": "vevo-sk-aa-001",
        "variation_id": "control",
        "utm_source": "meta",
        "utm_medium": "paid_social",
        "meta_campaign_id": "123",
        "meta_adset_id": "456",
        "meta_ad_id": "789",
        "meta_placement": "instagram_feed",
        "collector_version": "test",
        "risk_result": "accepted",
    }
    row.update(specific)
    return row


class FakeS3:
    def __init__(self, pages, objects):
        self.pages = {key: list(value) for key, value in pages.items()}
        self.objects = dict(objects)
        self.list_calls = []
        self.get_calls = []

    def list_objects_v2(self, **kwargs):
        self.list_calls.append(kwargs)
        prefix = kwargs["Prefix"]
        pages = self.pages.get(prefix, [])
        index = 0 if "ContinuationToken" not in kwargs else int(kwargs["ContinuationToken"])
        if index >= len(pages):
            return {"Contents": [], "IsTruncated": False}
        contents = pages[index]
        result = {
            "Contents": [
                {"Key": key, "Size": len(self.objects[key])}
                for key in contents
            ],
            "IsTruncated": index + 1 < len(pages),
        }
        if result["IsTruncated"]:
            result["NextContinuationToken"] = str(index + 1)
        return result

    def get_object(self, **kwargs):
        self.get_calls.append(kwargs)
        payload = self.objects[kwargs["Key"]]
        return {"ContentLength": len(payload), "Body": io.BytesIO(payload)}


class FakeExporter:
    @staticmethod
    def _realized_revenue_decision(order):
        return bool(order.get("realized")), "test"

    @staticmethod
    def _classify_lifecycle_bucket(status_name):
        normalized = str(status_name).lower()
        if "refund" in normalized:
            return "refunded_returned", "Refunded", 60
        if "cancel" in normalized:
            return "failed_cancelled", "Cancelled", 10
        return "fulfilled", "Fulfilled", 40

    @staticmethod
    def flatten_order(order):
        # The source intentionally contains PII; the returned order boundary
        # must drop it before leaving the adapter.
        assert "customer" in order
        return list(order.get("rows") or [])


class GrowthBookPipelineTests(unittest.TestCase):
    def test_raw_loader_reads_only_exact_date_partitions_with_pagination(self):
        first_prefix = "experiment-events/raw/event_date=2026-08-20/"
        second_prefix = "experiment-events/raw/event_date=2026-08-21/"
        objects = {
            f"{first_prefix}a.json": json.dumps(event()).encode(),
            f"{first_prefix}b.json": json.dumps(event()).encode(),
            f"{second_prefix}c.json": json.dumps(event(received_at=BASE + timedelta(days=1))).encode(),
        }
        s3 = FakeS3(
            {
                first_prefix: [[f"{first_prefix}a.json"], [f"{first_prefix}b.json"]],
                second_prefix: [[f"{second_prefix}c.json"]],
            },
            objects,
        )

        rows = load_raw_experiment_events(
            s3,
            bucket="vevo-growthbook-test",
            start_date=date(2026, 8, 20),
            end_date=date(2026, 8, 21),
        )

        self.assertEqual(3, len(rows))
        self.assertEqual(
            [first_prefix, first_prefix, second_prefix],
            [call["Prefix"] for call in s3.list_calls],
        )
        self.assertTrue(all(call["Bucket"] == "vevo-growthbook-test" for call in s3.get_calls))

    def test_raw_loader_fails_closed_on_escape_invalid_json_and_limits(self):
        prefix = "experiment-events/raw/event_date=2026-08-20/"
        escaped_key = "experiment-events/raw/event_date=2026-08-20/nested/a.json"
        s3 = FakeS3({prefix: [[escaped_key]]}, {escaped_key: b"{}"})
        with self.assertRaises(ExperimentDataError):
            load_raw_experiment_events(
                s3,
                bucket="vevo-growthbook-test",
                start_date=date(2026, 8, 20),
                end_date=date(2026, 8, 20),
            )

        bad_key = f"{prefix}bad.json"
        malformed = FakeS3({prefix: [[bad_key]]}, {bad_key: b"not-json"})
        with self.assertRaises(ExperimentDataError):
            load_raw_experiment_events(
                malformed,
                bucket="vevo-growthbook-test",
                start_date=date(2026, 8, 20),
                end_date=date(2026, 8, 20),
            )

        with self.assertRaises(ExperimentDataError):
            load_raw_experiment_events(
                malformed,
                bucket="vevo-growthbook-test",
                start_date=date(2026, 1, 1),
                end_date=date(2026, 8, 20),
            )

    def test_completion_receipts_are_validated_and_use_first_server_receipt(self):
        later = event(
            "order_completed",
            received_at=BASE + timedelta(minutes=2),
            transaction_id="2602000001",
        )
        earlier = event(
            "order_completed",
            received_at=BASE + timedelta(minutes=1),
            transaction_id="2602000001",
        )
        receipts = order_completion_receipts([later, earlier])
        self.assertEqual(BASE + timedelta(minutes=1), receipts["2602000001"])

        invalid = dict(earlier)
        invalid["customer_email"] = "forbidden@example.com"
        with self.assertRaises(ExperimentDataError):
            order_completion_receipts([invalid])

    def test_biznisweb_adapter_emits_only_safe_realized_order_values(self):
        receipt = BASE + timedelta(minutes=1)
        raw_order = {
            "order_num": "2602000001",
            "status": {"name": "Paid"},
            "realized": True,
            "customer": {"email": "forbidden@example.com"},
            "rows": [
                {"item_total_without_tax": 30.25, "total_expense": 10.10},
                {"item_total_without_tax": 19.75, "total_expense": 5.40},
            ],
        }

        facts = build_biznisweb_authoritative_orders(
            FakeExporter(),
            [raw_order],
            completion_receipts={"2602000001": receipt, "missing-order": receipt},
            generated_at=BASE + timedelta(days=15),
            maturity_checkpoint_days=14,
            packaging_cost_eur=0.30,
            shipping_net_cost_eur=0.20,
        )

        self.assertEqual(1, len(facts))
        self.assertEqual(ORDER_FIELDS, set(facts[0]))
        self.assertEqual(50.0, facts[0]["net_revenue_eur"])
        self.assertEqual(34.0, facts[0]["cm1_eur"])
        self.assertEqual("realized", facts[0]["lifecycle_state"])
        self.assertTrue(facts[0]["mature"])
        self.assertNotIn("customer", json.dumps(facts[0]))
        self.assertNotIn("email", json.dumps(facts[0]))

    def test_biznisweb_adapter_keeps_nonrealized_lifecycle_with_zero_value(self):
        receipt = BASE
        orders = [
            {
                "order_num": "cancelled-1",
                "status": {"name": "Cancelled"},
                "realized": False,
                "customer": {},
                "rows": [{"item_total_without_tax": 50, "total_expense": 20}],
            },
            {
                "order_num": "refunded-1",
                "status": {"name": "Refunded"},
                "realized": False,
                "customer": {},
                "rows": [{"item_total_without_tax": 50, "total_expense": 20}],
            },
        ]
        facts = build_biznisweb_authoritative_orders(
            FakeExporter(),
            orders,
            completion_receipts={order["order_num"]: receipt for order in orders},
            generated_at=BASE + timedelta(days=2),
            maturity_checkpoint_days=14,
            packaging_cost_eur=0.30,
            shipping_net_cost_eur=0.20,
            excluded_order_nums=("cancelled-1",),
        )

        by_num = {row["order_num"]: row for row in facts}
        self.assertEqual("cancelled", by_num["cancelled-1"]["lifecycle_state"])
        self.assertEqual("refunded", by_num["refunded-1"]["lifecycle_state"])
        self.assertEqual(0.0, by_num["refunded-1"]["cm1_eur"])
        self.assertFalse(by_num["refunded-1"]["mature"])
        self.assertTrue(by_num["cancelled-1"]["excluded"])

    def test_biznisweb_adapter_fails_on_conflicting_source_order(self):
        receipt = BASE
        first = {
            "order_num": "2602000001",
            "status": {"name": "Paid"},
            "realized": True,
            "customer": {},
            "rows": [],
        }
        second = dict(first, realized=False)
        with self.assertRaises(ExperimentDataError):
            build_biznisweb_authoritative_orders(
                FakeExporter(),
                [first, second],
                completion_receipts={"2602000001": receipt},
                generated_at=BASE,
                maturity_checkpoint_days=14,
                packaging_cost_eur=0.30,
                shipping_net_cost_eur=0.20,
            )


if __name__ == "__main__":
    unittest.main()
