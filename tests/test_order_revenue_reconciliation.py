"""Synthetic primary-source arithmetic for separate order discounts."""

import copy
import json
import os
import tempfile
import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from export_orders import BizniWebExporter, OrderRevenueReconciliationError, ORDER_CACHE_SCHEMA_VERSION, ORDER_QUERY, ORDER_QUERY_WITHOUT_PRICE_ELEMENTS
from graphql import print_ast
import pandas as pd


class OrderRevenueReconciliationTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        self.exporter = BizniWebExporter(
            api_url="https://example.com/api/graphql", api_token="synthetic",
            project_name="vevo", enable_period_bundle=False, order_facts_only=True,
        )
        self.exporter.project_settings["order_revenue_reconciliation_enabled"] = True
        realized = patch.object(self.exporter, "_realized_revenue_decision", return_value=(True, "synthetic_paid"))
        realized.start()
        self.addCleanup(realized.stop)
        self.exporter._rebuild_product_expense_indexes({"Synthetic product": 7.0})

    @staticmethod
    def item(net=100, vat=20, label="Synthetic product", currency="EUR"):
        gross = float((Decimal(str(net)) * (1 + Decimal(str(vat)) / 100)).quantize(Decimal(".01")))
        return {
            "item_label": label, "quantity": 1, "tax_rate": vat,
            "price": {"value": net, "raw_value": net, "currency": {"code": currency}},
            "sum": {"value": net, "raw_value": net, "currency": {"code": currency}},
            "sum_with_tax": {"value": gross, "raw_value": gross, "currency": {"code": currency}},
        }

    @staticmethod
    def element(kind, amount, *, net=True, value=""):
        displayed = Decimal(str(amount)).quantize(Decimal(".01"))
        return {"type": kind, "value": value, "price": {"value": displayed, "raw_value": amount, "is_net_price": net}}

    def order(self, *, total=108, items=None, elements=None, currency="EUR"):
        return {
            "order_num": "SYNTHETIC-DISCOUNT", "sum": {"value": total, "raw_value": total, "is_net_price": False, "currency": {"code": currency}},
            "items": items if items is not None else [self.item(currency=currency)],
            "price_elements": elements if elements is not None else [self.element("percent_discount", -12, value="10")],
        }

    def test_gross_discount_ignores_contradictory_net_flag_and_preserves_source(self):
        order = self.order()
        before = copy.deepcopy(order)
        row = self.exporter.flatten_order(order)[0]
        self.assertEqual(before, order)
        self.assertEqual(90, row["item_total_without_tax"])
        self.assertEqual(108, row["item_total_with_tax"])
        self.assertEqual(18, row["item_tax_amount"])
        self.assertEqual(7, row["total_expense"])
        self.assertEqual(83, row["profit_before_ads"])
        self.assertEqual(100, row["item_line_sum_original"])
        self.assertEqual(10, row["item_order_discount_without_tax"])
        self.assertEqual("header_discount_allocated", row["order_revenue_reconciliation"])

    def test_actual_net_discount_is_verified_by_grand_total(self):
        order = self.order(elements=[self.element("percent_discount", -10, net=False, value="10")])
        row = self.exporter.flatten_order(order)[0]
        self.assertEqual(90, row["item_total_without_tax"])

    def test_already_discounted_line_totals_are_not_discounted_twice(self):
        row = self.exporter.flatten_order(self.order(items=[self.item(90)]))[0]
        self.assertEqual(90, row["item_total_without_tax"])
        self.assertEqual(0, row["item_order_discount_without_tax"])
        self.assertEqual("already_in_item_totals", row["order_revenue_reconciliation"])

    def test_absolute_and_signed_adjustments_with_no_paid_services(self):
        for kind, value in [("absolute_discount", -12), ("fixed_discount", 12), ("discount", -10), ("manual_adjustment", -12)]:
            with self.subTest(kind=kind):
                row = self.exporter.flatten_order(self.order(elements=[self.element(kind, value)]))[0]
                self.assertEqual(90, row["item_total_without_tax"])

    def test_shipping_payment_and_rounding_are_excluded_from_goods_revenue(self):
        elements = [self.element("shipping", 5), self.element("payment", 1), self.element("autoround", Decimal(".0083333333"))]
        row = self.exporter.flatten_order(self.order(total=127.21, elements=elements))[0]
        self.assertEqual(100, row["item_total_without_tax"])
        self.assertEqual(120, row["item_total_with_tax"])

    def test_percentage_on_goods_or_entire_cart_preserves_shipping_semantics(self):
        for amount, total in [(12, 120), (13.2, 118.8)]:
            with self.subTest(amount=amount):
                elements = [self.element("shipping", 10), self.element("percent_discount", -amount, value="10")]
                row = self.exporter.flatten_order(self.order(total=total, elements=elements))[0]
                self.assertEqual(90, row["item_total_without_tax"])
                self.assertEqual(108, row["item_total_with_tax"])

    def test_mixed_vat_and_free_lines_allocate_discount_by_tax_group(self):
        items = [self.item(100, 20), self.item(100, 10), self.item(0, 20)]
        order = self.order(total=207, items=items, elements=[self.element("percent_discount", -23, value="10")])
        rows = self.exporter.flatten_order(order)
        self.assertEqual([90, 90, 0], [row["item_total_without_tax"] for row in rows])
        self.assertEqual([108, 99, 0], [row["item_total_with_tax"] for row in rows])
        self.assertEqual(20, sum(row["item_order_discount_without_tax"] for row in rows))
        self.assertEqual(23, sum(row["item_order_discount_with_tax"] for row in rows))

    def test_multiple_percentages_can_use_original_goods_basis_when_source_proves_it(self):
        elements = [self.element("shipping", 5), self.element("percent_discount", -12, value="10"),
                    self.element("percent_discount", -24, value="20")]
        order = self.order(total=90, elements=elements)
        before = copy.deepcopy(order)
        row = self.exporter.flatten_order(order)[0]
        self.assertEqual(70, row["item_total_without_tax"])
        self.assertEqual(84, row["item_total_with_tax"])
        self.assertEqual(30, row["item_order_discount_without_tax"])
        self.assertEqual(7, row["total_expense"])
        self.assertEqual(63, row["profit_before_ads"])
        self.assertEqual(before, order)

    def test_original_goods_and_remaining_cart_ambiguity_is_rejected(self):
        # The second amount can be 20% of original goods or the remaining cart;
        # both explain the total but assign a different part to merchandise.
        elements = [self.element("shipping", 10), self.element("percent_discount", -12, value="10"),
                    self.element("percent_discount", -24, value="20")]
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "ambiguous_discount_or_service_allocation"):
            self.exporter.flatten_order(self.order(total=96, elements=elements))

    def test_original_then_remaining_percentage_chain_is_independently_proved(self):
        elements = [self.element("shipping", 5), self.element("percent_discount", -12, value="10"),
                    self.element("percent_discount", -24, value="20"), self.element("percent_discount", -42, value="50")]
        row = self.exporter.flatten_order(self.order(total=48, elements=elements))[0]
        self.assertEqual(35, row["item_total_without_tax"])
        self.assertEqual(42, row["item_total_with_tax"])
        self.assertEqual(7, row["total_expense"])

    def test_original_percentage_discounts_preserve_mixed_vat_allocation(self):
        items = [self.item(100, 20), self.item(100, 10), self.item(0, 20)]
        elements = [self.element("percent_discount", -23, value="10"), self.element("percent_discount", -46, value="20")]
        rows = self.exporter.flatten_order(self.order(total=161, items=items, elements=elements))
        self.assertEqual([70, 70, 0], [row["item_total_without_tax"] for row in rows])
        self.assertEqual([84, 77, 0], [row["item_total_with_tax"] for row in rows])

    def test_multiple_compounded_percentages_keep_remaining_goods_basis(self):
        elements = [self.element("shipping", 10), self.element("percent_discount", -12, value="10"),
                    self.element("percent_discount", -21.6, value="20")]
        row = self.exporter.flatten_order(self.order(total=98.4, elements=elements))[0]
        self.assertEqual(72, row["item_total_without_tax"])
        self.assertEqual(86.4, row["item_total_with_tax"])
        self.assertEqual(7, row["total_expense"])

    def test_original_cart_percentages_preserve_goods_and_service_allocation(self):
        elements = [self.element("shipping", 10), self.element("percent_discount", -13.2, value="10"),
                    self.element("percent_discount", -26.4, value="20")]
        rows = self.exporter.flatten_order(self.order(total=92.4, elements=elements))
        self.assertEqual(70, rows[0]["item_total_without_tax"])
        self.assertEqual(84, rows[0]["item_total_with_tax"])
        self.assertEqual(36, rows[0]["item_order_discount_with_tax"])

    def test_multiple_discounts_cannot_explain_arbitrary_amount_or_grand_total(self):
        for amount, total in [(-23, 97), (-24, 95)]:
            with self.subTest(amount=amount, total=total):
                elements = [self.element("shipping", 10), self.element("percent_discount", -12, value="10"),
                            self.element("percent_discount", amount, value="20")]
                with self.assertRaisesRegex(OrderRevenueReconciliationError, "grand_total_not_reconciled"):
                    self.exporter.flatten_order(self.order(total=total, elements=elements))

    def test_largest_remainder_preserves_exact_cents_and_never_touches_free_line(self):
        parts = self.exporter._allocate_decimal_money(Decimal(".05"), [Decimal(1), Decimal(1), Decimal(1), Decimal(0)])
        self.assertEqual([Decimal(".02"), Decimal(".02"), Decimal(".01"), Decimal(0)], parts)

    def test_header_discount_does_not_reduce_margin_estimated_acquisition_cost(self):
        self.exporter._rebuild_product_expense_indexes({})
        with patch("export_orders.MISSING_COST_MARGIN_PCT", 35):
            row = self.exporter.flatten_order(self.order())[0]
        self.assertEqual(65, row["total_expense"])
        self.assertEqual(25, row["profit_before_ads"])

    def test_authoritative_margin_estimate_keeps_its_pre_header_basis(self):
        sku = self.exporter.get_reporting_product_sku("", "Synthetic product")
        with patch("export_orders.AUTHORITATIVE_MARGIN_OVERRIDE_SKUS", {sku: 90}):
            row = self.exporter.flatten_order(self.order())[0]
        self.assertEqual(10, row["total_expense"])
        self.assertEqual(80, row["profit_before_ads"])

    def test_bundle_components_preserve_discount_and_pre_discount_cost_estimates(self):
        self.exporter._rebuild_product_expense_indexes({})
        rule = {"key": "synthetic", "bundle_patterns": ["Synthetic product"], "components": [
            {"item_label": "Synthetic component A", "quantity": 1, "revenue_weight": 1},
            {"item_label": "Synthetic component B", "quantity": 2, "revenue_weight": 1},
        ]}
        with patch("export_orders.MISSING_COST_MARGIN_PCT", 35), patch.object(self.exporter, "_product_component_expansion_rules", return_value=[rule]):
            rows = self.exporter.flatten_order(self.order())
            components = self.exporter.expand_reporting_product_component_rows(pd.DataFrame(rows))
        self.assertEqual(90, components["item_total_without_tax"].sum())
        self.assertEqual(10, components["item_order_discount_without_tax"].sum())
        self.assertEqual(12, components["item_order_discount_with_tax"].sum())
        self.assertEqual(65, components["total_expense"].sum())
        self.assertEqual(25, components["profit_before_ads"].sum())

    def test_missing_or_unexplained_source_never_passes(self):
        cases = []
        order = self.order(); order.pop("price_elements"); cases.append(order)
        order = self.order(); order["price_elements"] = []; cases.append(order)
        order = self.order(total=1); cases.append(order)
        order = self.order(); order["sum"].pop("value"); cases.append(order)
        order = self.order(); order["sum"]["value"] = float("nan"); cases.append(order)
        order = self.order(); order["items"][0]["sum"]["currency"]["code"] = "CZK"; cases.append(order)
        order = self.order(total=125, elements=[self.element("unknown_fee", 5, net=False)]); cases.append(order)
        for order in cases:
            with self.subTest(order=order):
                with self.assertRaises(OrderRevenueReconciliationError):
                    self.exporter.flatten_order(order)

    def test_absolute_discount_with_paid_services_has_no_invented_scope(self):
        order = self.order(total=120, elements=[self.element("shipping", 10), self.element("absolute_discount", -12)])
        with self.assertRaises(OrderRevenueReconciliationError):
            self.exporter.flatten_order(order)

    def test_tiny_discount_with_two_possible_accounting_results_fails_closed(self):
        order = self.order(total=119.99, elements=[self.element("percent_discount", -.01, value=".01")])
        with self.assertRaisesRegex(OrderRevenueReconciliationError, "ambiguous"):
            self.exporter.flatten_order(order)

    def test_missing_elements_on_paid_card_orders_are_enriched_too(self):
        order = self.order(); order.pop("price_elements")
        self.assertTrue(self.exporter._needs_payment_metadata_for_realized_revenue(order))
        self.exporter.project_settings["order_revenue_reconciliation_enabled"] = False
        self.assertFalse(self.exporter._needs_payment_metadata_for_realized_revenue(order))

    def test_raw_currency_precision_validates_vat_without_changing_reported_net(self):
        for currency, vat, displayed_net, raw_net, expected_eur in [
            ("CZK", 21, 82.6, "82.64462809917355", 3.30),
            ("HUF", 27, 79, "78.74015748031496", .20),
        ]:
            with self.subTest(currency=currency):
                item = self.item(displayed_net, vat, currency=currency)
                item["sum"]["raw_value"] = raw_net
                item["sum_with_tax"].update(value=100, raw_value=100)
                order = self.order(total=100, items=[item], elements=[], currency=currency)
                row = self.exporter.flatten_order(order)[0]
                self.assertEqual(expected_eur, row["item_total_without_tax"])
                item["sum"].pop("raw_value")
                with self.assertRaisesRegex(OrderRevenueReconciliationError, "missing_or_invalid_monetary_value"):
                    self.exporter.flatten_order(order)

    def test_full_and_fallback_queries_request_raw_money_fields(self):
        for query in (ORDER_QUERY, ORDER_QUERY_WITHOUT_PRICE_ELEMENTS):
            text = print_ast(getattr(query, "document", query))
            self.assertIn("sum {\n        value\n        raw_value", text)
            self.assertIn("sum_with_tax {\n          value\n          raw_value", text)
            self.assertIn("price {\n          value\n          raw_value", text)

    def test_legacy_value_only_eligible_cache_is_refreshed_under_strict_policy(self):
        order = self.order()
        for item in order["items"]:
            for key in ("price", "sum", "sum_with_tax"):
                item[key].pop("raw_value")
        with tempfile.TemporaryDirectory() as directory:
            cache_file = Path(directory) / "synthetic.json"
            cache_file.write_text(json.dumps({"schema_version": ORDER_CACHE_SCHEMA_VERSION, "orders": [order]}, default=float), encoding="utf-8")
            with patch.object(self.exporter, "get_cache_filename", return_value=cache_file), patch.object(self.exporter, "_reporting_order_context", side_effect=lambda value: value):
                self.assertIsNone(self.exporter.load_from_cache(datetime(2026, 9, 1)))
                self.exporter.project_settings["order_revenue_reconciliation_enabled"] = False
                self.assertEqual([order], self.exporter.load_from_cache(datetime(2026, 9, 1)))

    def test_other_projects_keep_legacy_contract_without_opt_in(self):
        exporter = BizniWebExporter(api_url="https://example.com", api_token="synthetic", project_name="roy", enable_period_bundle=False, order_facts_only=True)
        exporter.project_settings["order_revenue_reconciliation_enabled"] = False
        row = exporter.flatten_order(self.order())[0]
        self.assertEqual(100, row["item_total_without_tax"])
        self.assertEqual("not_enabled", row["order_revenue_reconciliation"])


if __name__ == "__main__":
    unittest.main()
