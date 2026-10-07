"""Exact service policy: tips/insurance cost zero without matching other products."""

import copy
import os
import unittest
from unittest.mock import patch

from export_orders import BizniWebExporter, OrderRevenueReconciliationError
from reporting_core import load_project_settings


SERVICE_LABELS = (
    "Tringelt",
    "Spropitné",
    "Borravaló",
    "Poistenie proti rozbitiu",
    "Pojištění proti rozbití",
    "Törés elleni biztosítás",
)


class VevoZeroCostServiceTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        self.exporter = self.make_exporter()
        # Revenue reconciliation has its own tests; these cases isolate cost policy,
        # including negative/zero lines, without provider order-total assumptions.
        self.exporter.project_settings["order_revenue_reconciliation_enabled"] = False

    @staticmethod
    def make_exporter(project="vevo"):
        return BizniWebExporter(
            api_url="https://example.com/api/graphql",
            api_token="synthetic-token",
            project_name=project,
            enable_period_bundle=False,
            order_facts_only=True,
        )

    @staticmethod
    def order(label, revenue=10.0, quantity=2):
        gross = round(revenue * 1.23, 2)
        return {
            "order_num": "SYNTHETIC-SERVICE-POLICY",
            "pur_date": "2026-09-01 10:00:00",
            "sum": {"value": gross, "currency": {"code": "EUR"}},
            "items": [{
                "item_label": label,
                "quantity": quantity,
                "tax_rate": 23,
                "price": {"value": revenue / quantity, "currency": {"code": "EUR"}},
                "sum": {"value": revenue, "currency": {"code": "EUR"}},
                "sum_with_tax": {"value": gross, "currency": {"code": "EUR"}},
            }],
        }

    def test_every_verified_shop_language_has_an_exact_configured_identity(self):
        actual = set(self.exporter.project_settings["zero_cost_service_product_skus"])
        expected = {self.exporter.get_reporting_product_sku("", label) for label in SERVICE_LABELS}
        self.assertEqual(expected, actual)
        self.assertEqual(set(SERVICE_LABELS), set(self.exporter.project_settings["zero_cost_service_labels"]))
        self.assertNotIn("zero_cost_service_product_skus", load_project_settings("roy"))

    def test_zero_cost_wins_over_mapped_cost_and_margin_policy_at_every_revenue_sign(self):
        self.exporter._rebuild_product_expense_indexes({label: 7.0 for label in SERVICE_LABELS})
        overrides = {self.exporter.get_reporting_product_sku("", label): 90 for label in SERVICE_LABELS}
        with patch("export_orders.AUTHORITATIVE_MARGIN_OVERRIDE_SKUS", overrides):
            for label in SERVICE_LABELS:
                for revenue in (10.0, 0.0, -10.0):
                    with self.subTest(label=label, revenue=revenue):
                        row = self.exporter.flatten_order(self.order(label, revenue))[0]
                        self.assertEqual(0.0, row["expense_per_item"])
                        self.assertEqual(0.0, row["total_expense"])
                        self.assertEqual("zero_cost_service_override", row["expense_source"])
                        self.assertEqual(7.0, row["purchase_cost_reference_per_item"])
                        self.assertIsNotNone(row["purchase_cost_reference_source"])
                        self.assertEqual(revenue, row["profit_before_ads"])

    def test_missing_reference_does_not_apply_fallback_cost_to_a_service(self):
        self.exporter._rebuild_product_expense_indexes({})
        for label in SERVICE_LABELS:
            with self.subTest(label=label):
                row = self.exporter.flatten_order(self.order(label))[0]
                self.assertEqual(0.0, row["total_expense"])
                self.assertEqual("zero_cost_service_override", row["expense_source"])
                self.assertIsNone(row["purchase_cost_reference_per_item"])

    def test_service_words_do_not_match_unrelated_products_or_bundles(self):
        labels = ["Synthetic insurance gift product", "Spropitné scented candle", "Poistenie proti rozbitiu + product bundle"]
        self.exporter._rebuild_product_expense_indexes({label: 7.0 for label in labels})
        for label in labels:
            with self.subTest(label=label):
                row = self.exporter.flatten_order(self.order(label))[0]
                self.assertEqual(14.0, row["total_expense"])
                self.assertNotEqual("zero_cost_service_override", row["expense_source"])

    def test_known_service_with_a_changed_explicit_identity_fails_closed(self):
        self.exporter._rebuild_product_expense_indexes({"Tringelt": 7.0})
        order = self.order("Tringelt")
        order["items"][0]["import_code"] = "SYNTHETIC-NON-SERVICE"
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "zero_cost_service_identity_drift"):
            self.exporter.flatten_order(order)

    def test_canonical_service_label_also_rejects_identity_drift(self):
        self.exporter.product_name_aliases_exact["Synthetic service translation"] = "Tringelt"
        order = self.order("Synthetic service translation")
        order["items"][0]["import_code"] = "SYNTHETIC-CHANGED-ID"
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "zero_cost_service_identity_drift"):
            self.exporter.flatten_order(order)

    def test_roy_does_not_inherit_vevo_zero_cost_services(self):
        exporter = self.make_exporter("roy")
        exporter._rebuild_product_expense_indexes({"Spropitné": 7.0})
        row = exporter.flatten_order(self.order("Spropitné"))[0]
        self.assertEqual(14.0, row["total_expense"])
        self.assertNotEqual("zero_cost_service_override", row["expense_source"])

    def test_malformed_service_policy_fails_at_startup(self):
        settings = load_project_settings("vevo")
        for invalid in ("H-SYNTHETIC", [""], ["H-SYNTHETIC", "h-synthetic"]):
            with self.subTest(invalid=invalid):
                malformed = copy.deepcopy(settings)
                malformed["zero_cost_service_product_skus"] = invalid
                with patch("export_orders.load_project_settings", return_value=malformed):
                    with self.assertRaises(ValueError):
                        self.make_exporter()

    def test_malformed_service_label_guard_fails_at_startup(self):
        settings = load_project_settings("vevo")
        for invalid in ("Tringelt", [""], ["Tringelt", "tringelt"]):
            with self.subTest(invalid=invalid):
                malformed = copy.deepcopy(settings)
                malformed["zero_cost_service_labels"] = invalid
                with patch("export_orders.load_project_settings", return_value=malformed):
                    with self.assertRaises(ValueError):
                        self.make_exporter()

    def test_label_guard_without_service_identities_fails_at_startup(self):
        malformed = copy.deepcopy(load_project_settings("vevo"))
        malformed["zero_cost_service_product_skus"] = []
        with patch("export_orders.load_project_settings", return_value=malformed):
            with self.assertRaises(ValueError):
                self.make_exporter()


if __name__ == "__main__":
    unittest.main()
