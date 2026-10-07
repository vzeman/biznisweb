"""Synthetic proof for one reviewed, fingerprint-bound compensation exception."""
import copy
import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from export_orders import BizniWebExporter, OrderRevenueReconciliationError, ORDER_CACHE_SCHEMA_VERSION


class CompensationRevenueReconciliationTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        self.exporter = BizniWebExporter(
            api_url="https://example.invalid/graphql", api_token="synthetic",
            project_name="vevo", order_facts_only=True, enable_period_bundle=False,
        )
        self.exporter._rebuild_product_expense_indexes({"Synthetic compensation": 7.0})
        self.order = {
            "order_num": "SYNTHETIC-COMPENSATION", "pur_date": "2026-01-03 10:00:00",
            "status": {"id": "4", "name": "Odoslana"}, "sum": self.money(20),
            "items": [{"item_label": "Synthetic compensation", "ean": None, "import_code": "SYN-COMP",
                       "warehouse_number": "SYN-WH", "quantity": 1, "tax_rate": 0,
                       "price": self.money(20), "sum": self.money(20), "sum_with_tax": self.money(20)}],
        }
        settings = self.exporter.project_settings["realized_revenue"]
        settings["missing_payment_metadata_realized_order_overrides"] = {
            self.order["order_num"]: "Synthetic reviewed compensation, no delivery/payment",
        }
        self.approve(self.order)

    @staticmethod
    def money(value):
        return {"value": value, "raw_value": value, "is_net_price": True, "currency": {"code": "EUR"}}

    def approve(self, order):
        self.exporter.project_settings["realized_revenue"]["missing_price_elements_compensation_reconciliation"] = {
            order["order_num"]: {
                "source_sha256": self.exporter._compensation_source_fingerprint(order, "vevo"),
                "audit_reason": "Synthetic independent source audit",
            },
        }
        self.exporter.realized_revenue_settings = self.exporter._resolve_realized_revenue_settings()

    def test_exact_reviewed_source_skips_unavailable_metadata_without_inventing_elements(self):
        before = copy.deepcopy(self.order)
        with patch.object(self.exporter, "_fetch_order_payment_metadata", side_effect=AssertionError("must not fetch")):
            self.exporter._enrich_payment_metadata_for_realized_revenue([self.order])
        row = self.exporter.flatten_order(self.order)[0]
        self.assertEqual(before, self.order)
        self.assertNotIn("price_elements", self.order)
        self.assertEqual(20, row["item_total_without_tax"])
        self.assertEqual(20, row["item_total_with_tax"])
        self.assertEqual(7, row["total_expense"])
        self.assertEqual(13, row["profit_before_ads"])
        self.assertEqual("audited_compensation_source_total:zero_vat_net_grand_total", row["order_revenue_reconciliation"])

    def test_fingerprint_drift_is_critical_in_both_metadata_and_accounting_paths(self):
        changes = {
            "quantity": lambda o: o["items"][0].update(quantity=2),
            "sku": lambda o: o["items"][0].update(import_code="OTHER"),
            "label": lambda o: o["items"][0].update(item_label="Other compensation"),
            "grand": lambda o: o["sum"].update(value=19),
            "raw": lambda o: o["items"][0]["sum"].update(raw_value=19.999),
            "currency": lambda o: o["sum"]["currency"].update(code="USD"),
            "vat": lambda o: o["items"][0].update(tax_rate=23),
            "status_id": lambda o: o["status"].update(id="6"),
            "date": lambda o: o.update(pur_date="2026-01-04 10:00:00"),
        }
        for name, mutate in changes.items():
            with self.subTest(name=name):
                order = copy.deepcopy(self.order)
                mutate(order)
                for method in (self.exporter._needs_payment_metadata_for_realized_revenue, self.exporter.flatten_order):
                    with self.assertRaisesRegex(OrderRevenueReconciliationError, "audited_compensation_source_changed"):
                        method(order)

    def test_missing_raw_value_cannot_match_reviewed_evidence(self):
        self.order["items"][0]["price"].pop("raw_value")
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "compensation_source_value_missing"):
            self.exporter._needs_payment_metadata_for_realized_revenue(self.order)

    def test_status_change_cannot_be_promoted_by_exception(self):
        self.order["status"] = {"id": "17", "name": "Storno"}
        self.assertFalse(self.exporter._realized_revenue_decision(self.order)[0])
        self.assertEqual("not_financially_included", self.exporter.flatten_order(self.order)[0]["order_revenue_reconciliation"])
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "audited_compensation_status_changed"):
            self.exporter._reconcile_order_item_revenue(self.order)
        self.order["status"] = {"id": "31", "name": self.exporter.realized_revenue_settings["paid_statuses"][0]}
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "audited_compensation_status_changed"):
            self.exporter._needs_payment_metadata_for_realized_revenue(self.order)

    def test_other_order_and_project_keep_missing_elements_guard(self):
        another = copy.deepcopy(self.order)
        another["order_num"] = "SYNTHETIC-OTHER"
        self.assertTrue(self.exporter._needs_payment_metadata_for_realized_revenue(another))
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "missing_price_elements"):
            self.exporter._reconcile_order_item_revenue(another)
        self.exporter.project_name = "roy"
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "audited_compensation_source_changed"):
            self.exporter._reconcile_order_item_revenue(self.order)

    def test_present_elements_are_used_normally_and_never_replaced(self):
        self.order["price_elements"] = [{"type": "payment", "title": "Dobierkou", "reference_id": "7", "price": self.money(0)}]
        self.assertIsNone(self.exporter._audited_compensation_reconciliation(self.order))
        self.assertNotIn("audited_compensation", self.exporter.flatten_order(self.order)[0]["order_revenue_reconciliation"])
        self.order["price_elements"].append({"type": "shipping", "price": self.money(2)})
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "grand_total_not_reconciled"):
            self.exporter.flatten_order(self.order)

    def test_malformed_elements_are_not_an_absent_metadata_exception(self):
        self.order["price_elements"] = {"shipping": 0}
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "invalid_compensation_price_elements"):
            self.exporter._reconcile_order_item_revenue(self.order)

    def test_even_fingerprinted_unsupported_shapes_are_rejected(self):
        for name in ("quantity", "vat", "currency", "grand_net", "line_difference", "zero"):
            with self.subTest(name=name):
                order = copy.deepcopy(self.order)
                if name == "quantity": order["items"][0]["quantity"] = 2
                elif name == "vat": order["items"][0]["tax_rate"] = 23
                elif name == "currency": order["sum"]["currency"]["code"] = "USD"
                elif name == "grand_net": order["sum"]["is_net_price"] = False
                elif name == "line_difference": order["items"][0]["sum"]["value"] = 19.99
                elif name == "zero": order["sum"]["value"] = 0
                self.approve(order)
                with self.assertRaises(OrderRevenueReconciliationError):
                    self.exporter._reconcile_order_item_revenue(order)

    def test_binary_raw_noise_remains_bound_to_its_exact_snapshot(self):
        for key in ("price", "sum", "sum_with_tax"):
            self.order["items"][0][key]["raw_value"] = 20.0000000000006
        self.approve(self.order)
        self.assertEqual(20, self.exporter.flatten_order(self.order)[0]["item_total_without_tax"])

    def test_fingerprint_excludes_customer_and_formatting_fields(self):
        before = self.exporter._compensation_source_fingerprint(self.order, "vevo")
        self.order["customer"] = {"name": "Synthetic customer"}
        self.order["sum"]["formatted"] = "Displayed amount"
        self.order["last_change"] = "2026-01-07 10:00:00"
        self.assertEqual(before, self.exporter._compensation_source_fingerprint(self.order, "vevo"))

    def test_cache_accepts_only_exact_evidence_with_original_missing_elements(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = Path(tmp)/"day.json"
            cache.write_text(json.dumps({"schema_version": ORDER_CACHE_SCHEMA_VERSION, "orders": [self.order]}), encoding="utf-8")
            with patch.object(self.exporter, "get_cache_filename", return_value=cache):
                self.assertEqual([self.order], self.exporter.load_from_cache(datetime(2026, 1, 3)))
                self.order["items"][0]["quantity"] = 2
                cache.write_text(json.dumps({"schema_version": ORDER_CACHE_SCHEMA_VERSION, "orders": [self.order]}), encoding="utf-8")
                with self.assertRaisesRegex(OrderRevenueReconciliationError, "audited_compensation_source_changed"):
                    self.exporter.load_from_cache(datetime(2026, 1, 3))

    def test_configuration_requires_exact_existing_override_and_evidence(self):
        valid = copy.deepcopy(self.exporter.project_settings["realized_revenue"]["missing_price_elements_compensation_reconciliation"])
        invalid = [[], None, {"OTHER": next(iter(valid.values()))}, {"*": next(iter(valid.values()))},
                   {self.order["order_num"]: {"source_sha256": "not-a-hash", "audit_reason": "reviewed"}},
                   {self.order["order_num"]: {"source_sha256": "0"*64, "audit_reason": ""}}]
        for value in invalid:
            with self.subTest(value=value):
                self.exporter.project_settings["realized_revenue"]["missing_price_elements_compensation_reconciliation"] = value
                with self.assertRaises(ValueError):
                    self.exporter._resolve_realized_revenue_settings()


if __name__ == "__main__":
    unittest.main()
