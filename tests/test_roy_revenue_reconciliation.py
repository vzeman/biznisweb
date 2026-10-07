"""Synthetic coverage for independently observed ROY source rounding models."""

import copy
import os
import unittest
from decimal import Decimal
from unittest.mock import patch

import pandas as pd

from export_orders import BizniWebExporter, OrderRevenueReconciliationError


class RoyRevenueReconciliationTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        self.exporter = BizniWebExporter(
            api_url="https://example.invalid/graphql", api_token="synthetic",
            project_name="roy", order_facts_only=True, enable_period_bundle=False,
        )
        self.exporter.project_settings["order_revenue_reconciliation_enabled"] = True
        self.exporter.project_settings["order_currency_rounding_precision"] = {"EUR": 2, "CZK": 1, "HUF": 0}
        self.exporter._rebuild_product_expense_indexes({"SYNTHETIC": 7.0})
        included = patch.object(self.exporter, "_realized_revenue_decision", return_value=(True, "synthetic_paid"))
        included.start()
        self.addCleanup(included.stop)

    @staticmethod
    def money(value, raw=None, currency="EUR", net=True):
        return {"value": value, "raw_value": value if raw is None else raw,
                "is_net_price": net, "currency": {"code": currency}}

    def item(self, *, net=100, raw_net=None, gross=123, raw_gross=None, unit=None,
             raw_unit=None, quantity=1, vat=23, currency="EUR", label="Synthetic", sku="SYNTHETIC"):
        return {"item_label": label, "import_code": sku, "quantity": quantity, "tax_rate": vat,
                "price": self.money(net if unit is None else unit, raw_unit, currency),
                "sum": self.money(net, raw_net, currency),
                "sum_with_tax": self.money(gross, raw_gross, currency, net=False)}

    def order(self, items, total, *, net=False, currency="EUR", elements=None):
        return {"order_num": "SYNTHETIC-ROY", "items": items,
                "sum": self.money(total, currency=currency, net=net), "price_elements": elements or []}

    def element(self, kind, amount, *, raw=None, value=""):
        return {"type": kind, "value": value, "price": self.money(amount, raw)}

    def test_free_net_order_preserves_mapped_cost_for_non_gift_product(self):
        order = self.order([self.item(net=0, gross=0)], 0, net=True)
        row = self.exporter.flatten_order(order)[0]
        self.assertEqual(0, row["item_total_without_tax"])
        self.assertEqual(7, row["total_expense"])
        self.assertEqual(-7, row["profit_before_ads"])
        self.assertIn("net_grand_total", row["order_revenue_reconciliation"])

    def test_zero_vat_net_order_uses_existing_line_basis_when_it_reconciles(self):
        order = self.order([self.item(net=100, gross=100, vat=0)], 100, net=True)
        row = self.exporter.flatten_order(order)[0]
        self.assertEqual(100, row["item_total_without_tax"])
        self.assertNotIn("source_unit_rounding", row["order_revenue_reconciliation"])

    def test_positive_vat_net_grand_total_remains_unsupported_without_evidence(self):
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "unsupported_net_order_grand_total"):
            self.exporter.flatten_order(self.order([self.item()], 100, net=True))

    def rounded_unit_order(self):
        item = self.item(net=300.4, raw_net="300.42", gross=300.4, raw_gross="300.42",
                         unit=100.1, raw_unit="100.14", quantity=3, vat=0, currency="CZK")
        elements = [self.element("shipping", 10.1, raw="10.14"),
                    self.element("percent_discount", -30, value="10")]
        return self.order([item], 280.4, net=True, currency="CZK", elements=elements)

    def test_unit_rounding_requires_grand_total_and_preserves_source_cost_and_discount(self):
        order = self.rounded_unit_order()
        before = copy.deepcopy(order)
        lines, method = self.exporter._reconcile_order_item_revenue(order)
        self.assertEqual(Decimal("270.30"), lines[0]["net"])
        self.assertEqual(Decimal("30.00"), lines[0]["discount_net"])
        row = self.exporter.flatten_order(order)[0]
        self.assertEqual(21, row["total_expense"])
        self.assertEqual(300.4, row["item_line_sum_original"])
        self.assertEqual(before, order)
        self.assertIn("source_unit_rounding", method)

    def test_unit_rounding_never_accepts_other_unexplained_total(self):
        order = self.rounded_unit_order()
        order["sum"]["value"] = 280
        order["sum"]["raw_value"] = 280
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "grand_total_not_reconciled"):
            self.exporter.flatten_order(order)

    def test_unit_rounding_requires_raw_unit_quantity_and_configured_precision(self):
        for change in ("raw_unit", "line_quantity", "precision", "service"):
            with self.subTest(change=change):
                order = self.rounded_unit_order()
                if change == "raw_unit":
                    order["items"][0]["price"].pop("raw_value")
                elif change == "line_quantity":
                    order["items"][0]["sum"]["raw_value"] = 300.43
                elif change == "service":
                    order["price_elements"][0]["price"]["value"] = 10.2
                else:
                    self.exporter.project_settings["order_currency_rounding_precision"] = {}
                with self.assertRaises(OrderRevenueReconciliationError):
                    self.exporter.flatten_order(order)
                self.exporter.project_settings["order_currency_rounding_precision"] = {"EUR": 2, "CZK": 1, "HUF": 0}

    def test_native_currency_rounding_proves_line_vat_without_widening_tolerance(self):
        item = self.item(net=200, raw_net="200.02", gross=242, raw_gross=242,
                         unit=200, raw_unit="200.02", vat=21, currency="CZK")
        order = self.order([item], 242, currency="CZK")
        lines, method = self.exporter._reconcile_order_item_revenue(order)
        self.assertEqual(Decimal(200), lines[0]["net"])
        self.assertIn("currency_line_rounding", method)
        item["sum"].update(value=200.1, raw_value=200.1)
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "vat_mismatch"):
            self.exporter.flatten_order(order)

    def test_unit_vat_rounding_is_proved_before_quantity_multiplication(self):
        item = self.item(net=3.2, raw_net=3.2, gross=4, raw_gross=4,
                         unit=.16, raw_unit=.16, quantity=20)
        lines, method = self.exporter._reconcile_order_item_revenue(self.order([item], 4))
        self.assertEqual(Decimal("3.2"), lines[0]["net"])
        self.assertIn("currency_unit_vat_rounding", method)
        item["sum_with_tax"].update(value=4.1, raw_value=4.1)
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "vat_mismatch"):
            self.exporter.flatten_order(self.order([item], 4.1))

    def test_raw_rounding_proof_rejects_unrelated_displayed_money(self):
        for field in ("price", "sum", "sum_with_tax"):
            with self.subTest(field=field):
                item = self.item(net=200, raw_net="200.02", gross=242, raw_gross=242,
                                 unit=200, raw_unit="200.02", vat=21, currency="CZK")
                item[field]["value"] += .1
                with self.assertRaisesRegex(OrderRevenueReconciliationError, "source_raw_display_rounding_mismatch"):
                    self.exporter.flatten_order(self.order([item], 242, currency="CZK"))

    def test_binary_float_noise_does_not_change_half_quantum_rounding(self):
        item = self.item(net=204.1, raw_net="204.090909090909", gross=247, raw_gross=247,
                         unit=204.1, raw_unit="204.090909090909", vat=21, currency="CZK")
        _, method = self.exporter._reconcile_order_item_revenue(self.order([item], 247, currency="CZK"))
        self.assertIn("currency_line_rounding", method)

    def test_only_exact_half_boundary_allows_both_source_unit_rounding_directions(self):
        raw_unit = Decimal("1.235") / Decimal("1.23")
        for gross in (9.84, 9.92):
            with self.subTest(gross=gross):
                item = self.item(net=8.03, raw_net=str(raw_unit * 8), gross=gross,
                                 unit=1, raw_unit=str(raw_unit), quantity=8)
                _, method = self.exporter._reconcile_order_item_revenue(self.order([item], gross))
                self.assertIn("currency_unit_vat_rounding", method)
        item = self.item(net=8.03, raw_net="8.0328", gross=9.84,
                         unit=1, raw_unit="1.0041", quantity=8)
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "vat_mismatch"):
            self.exporter.flatten_order(self.order([item], 9.84))

    def test_huf_net_fees_and_autoround_use_native_precision(self):
        item = self.item(net=100, gross=100, vat=0, currency="HUF")
        elements = [self.element("shipping", 10, raw="10.4"), self.element("payment", 1, raw="1.1"),
                    self.element("autoround", 0, raw=".4")]
        lines, method = self.exporter._reconcile_order_item_revenue(self.order([item], 111, net=True, currency="HUF", elements=elements))
        self.assertEqual(100, lines[0]["net"])
        self.assertNotIn("source_unit_rounding", method)
        elements[0]["price"]["value"] = 11
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "source_raw_display_rounding_mismatch"):
            self.exporter.flatten_order(self.order([item], 111, net=True, currency="HUF", elements=elements))

    def test_huf_gross_fees_and_negative_rounding_keep_goods_revenue(self):
        item = self.item(net=1000, gross=1270, vat=27, currency="HUF")
        elements = [self.element("shipping", 10, raw="10.2"), self.element("autoround", -1, raw="-1.1")]
        lines, _ = self.exporter._reconcile_order_item_revenue(self.order([item], 1282, currency="HUF", elements=elements))
        self.assertEqual(1000, lines[0]["net"])
        self.assertEqual(1270, lines[0]["gross"])

    def test_bundle_parent_csv_and_expanded_rows_share_component_cost_basis(self):
        self.exporter.project_settings["product_component_expansion_rules"] = [{
            "key": "synthetic_bundle", "bundle_patterns": ["Synthetic bundle"], "components": [
                {"item_label": "Synthetic component A", "item_import_code": "COMP-A", "quantity": 1},
                {"item_label": "Synthetic component B", "item_import_code": "COMP-B", "quantity": 2},
            ],
        }]
        self.exporter._rebuild_product_expense_indexes({"BUNDLE": 12, "COMP-A": 5, "COMP-B": 4})
        item = self.item(net=200, gross=246, quantity=2, label="Synthetic bundle", sku="BUNDLE")
        order = self.order([item], 221.4, elements=[self.element("percent_discount", -24.6, value="10")])
        parent = self.exporter.flatten_order(order)[0]
        components = self.exporter.add_reporting_product_identity_columns(pd.DataFrame([parent]))
        self.assertEqual(12, parent["purchase_cost_reference_per_item"])
        self.assertEqual(26, parent["total_expense"])
        self.assertEqual(parent["total_expense"], components["total_expense"].sum())
        self.assertEqual(parent["item_total_without_tax"], components["item_total_without_tax"].sum())
        self.assertEqual(parent["item_order_discount_without_tax"], components["item_order_discount_without_tax"].sum())
        self.assertEqual(parent["profit_before_ads"], components["profit_before_ads"].sum())
        self.assertTrue(parent["expense_source"].startswith("reporting_bundle_components:"))


if __name__ == "__main__":
    unittest.main()
