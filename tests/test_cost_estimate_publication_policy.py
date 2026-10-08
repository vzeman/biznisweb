import copy
import unittest
from unittest.mock import patch

import pandas as pd

from export_orders import BizniWebExporter
from reporting_core.cost_estimate_policy import assess_configured_cost_estimates


SETTINGS = {"cost_estimate_publication": {"allow_configured_margin_fallback": True}}


def row(**updates):
    return {"order_num": "synthetic", "purchase_date": "2026-10-07", "item_label": "Synthetic product",
            "product_sku": "SYNTHETIC", "expense_source": "missing_cost_margin_35_fallback",
            "item_quantity": 1, "item_total_without_tax": 100.0, "item_order_discount_without_tax": 0.0,
            "expense_per_item": 65.0, "total_expense": 65.0, "profit_before_ads": 35.0, **updates}


def assess(rows, settings=SETTINGS, margin=35):
    return assess_configured_cost_estimates(rows, settings=settings, margin_pct=margin)


def qa(rows, settings=SETTINGS):
    exporter = object.__new__(BizniWebExporter)
    exporter.project_settings = copy.deepcopy(settings)
    with patch("export_orders.MISSING_COST_MARGIN_PCT", 35.0):
        return exporter._build_product_expense_coverage_qa(pd.DataFrame(rows), "2026-10-01", "2026-10-07")


class CostEstimatePolicyTests(unittest.TestCase):
    @staticmethod
    def signed_source_row():
        return row(item_quantity=2, item_total_without_tax=-100, expense_per_item=-32.5,
                   total_expense=-65, profit_before_ads=-35, item_currency="EUR",
                   item_line_sum_original=-100, item_line_sum_with_tax_original=-123,
                   item_unit_price_original=-50, item_unit_price=-50,
                   item_total_with_tax=-123, item_tax_amount=-23, item_tax_rate=23,
                   item_order_discount_with_tax=0, order_revenue_reconciliation="source_total_verified")

    def test_signed_native_source_can_preserve_existing_margin_cost_without_money_changes(self):
        item = self.signed_source_row()
        before = copy.deepcopy(item)
        settings = {**SETTINGS, "currency_rates_to_eur": {"EUR": 1}}
        result = assess([item], settings=settings)
        self.assertTrue(result["approved"])
        self.assertEqual(1, result["verified_signed_source_rows"])
        self.assertEqual(before, item)
        self.assertEqual(0, assess([item], settings={})["verified_signed_source_rows"])

    def test_signed_cost_without_native_evidence_and_coupled_tampering_stays_critical(self):
        settings = {**SETTINGS, "currency_rates_to_eur": {"EUR": 1}}
        mutations = [
            {"item_line_sum_original": None}, {"item_line_sum_original": 100},
            {"item_line_sum_with_tax_original": -122}, {"item_unit_price_original": -49},
            {"item_unit_price": -49}, {"item_total_with_tax": -122}, {"item_tax_amount": -22},
            {"item_tax_rate": 20}, {"item_order_discount_without_tax": 1},
            {"item_order_discount_with_tax": 1}, {"order_revenue_reconciliation": "not_financially_included"},
            {"item_currency": "USD"}, {"total_expense": -64, "profit_before_ads": -36},
            {"item_total_without_tax": 100}, {"expense_per_item": 32.5},
            {"expense_per_item": -32, "total_expense": -64, "profit_before_ads": -36},
            {"expense_source": "bundle_component:bundle_component_missing_cost_margin_35_fallback", "bundle_component_flag": True},
            {"expense_source": "unrecognized_provider_cost"},
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                item = {**self.signed_source_row(), **mutation}
                self.assertFalse(assess([item], settings=settings)["approved"])
                self.assertEqual("critical", qa([item], settings=settings)["status"])
        for rate in (0, -1, "NaN", True):
            with self.subTest(rate=rate):
                self.assertFalse(assess([self.signed_source_row()], settings={**SETTINGS, "currency_rates_to_eur": {"EUR": rate}})["approved"])

    def test_explicit_accepted_model_changes_coverage_severity_not_money_or_threshold(self):
        rows = [row()]
        before = copy.deepcopy(rows)
        strict, accepted = qa(rows, {}), qa(rows)
        self.assertEqual("critical", strict["status"])
        self.assertEqual("warning", accepted["status"])
        self.assertEqual(0, accepted["failure_count"])
        self.assertTrue(accepted["estimate_validation"]["approved"])
        for name in ("total_revenue", "total_profit_before_ads", "fallback_revenue_share_pct", "fallback_profit_share_pct"):
            self.assertEqual(strict[name], accepted[name])
        self.assertEqual(100.0, accepted["fallback_revenue_share_pct"])
        self.assertTrue(any("remain estimates" in text for text in accepted["warnings"]))
        self.assertEqual(before, rows)

    def test_only_literal_opt_in_can_relax_coverage(self):
        for value in [None, False, "true", 1, [], {}]:
            with self.subTest(value=value):
                self.assertEqual("critical", qa([row()], {"cost_estimate_publication": {"allow_configured_margin_fallback": value}})["status"])

    def test_header_discount_keeps_pre_discount_acquisition_basis(self):
        discounted = row(item_total_without_tax=80, item_order_discount_without_tax=20, profit_before_ads=15)
        self.assertTrue(assess([discounted])["approved"])
        self.assertFalse(assess([{**discounted, "expense_per_item": 52, "total_expense": 52, "profit_before_ads": 28}])["approved"])

    def test_native_revenue_and_cost_half_cent_rounding(self):
        for raw_net in [5.555, 4.115384615384615, 7.995, 0.015]:
            raw_cost = raw_net * .65
            net, total = round(raw_net, 2), round(raw_cost, 2)
            item = row(item_total_without_tax=net, expense_per_item=raw_cost, total_expense=total,
                       profit_before_ads=round(net - total, 2))
            with self.subTest(raw_net=raw_net):
                self.assertTrue(assess([item])["approved"])
        self.assertFalse(assess([row(item_total_without_tax=100.01)])["approved"])

    def test_exact_bundle_source_requires_component_flag_and_same_equations(self):
        component = row(expense_source="bundle_component:bundle_component_missing_cost_margin_35_fallback", bundle_component_flag=True)
        self.assertTrue(assess([component])["approved"])
        self.assertFalse(assess([{**component, "bundle_component_flag": False}])["approved"])
        self.assertFalse(assess([{**component, "total_expense": 64.99}])["approved"])

    def test_zero_margin_supported_only_for_exact_configured_margin(self):
        item = row(expense_source="missing_cost_zero_margin_fallback", expense_per_item=100, total_expense=100, profit_before_ads=0)
        self.assertTrue(assess([item], margin=0)["approved"])
        self.assertFalse(assess([item], margin=35)["approved"])
        for margin in [None, True, float("nan"), -1, 101]:
            self.assertFalse(assess([row()], margin=margin)["approved"])

    def test_tampered_missing_nonfinite_and_unrecognized_inputs_fail_before_coercion(self):
        variants = [row(expense_per_item=64), row(total_expense=65.01), row(profit_before_ads=35.01),
                    row(item_quantity=0), row(item_quantity=-1), row(item_quantity=True),
                    row(item_total_without_tax=float("nan")), row(expense_per_item=float("inf")),
                    row(total_expense=None), row(expense_source="missing_cost_margin_15_fallback"),
                    row(expense_source="fallback_default"), row(expense_source="unrecognized_provider_cost"),
                    row(expense_source=None), row(item_order_discount_without_tax=float("nan"))]
        for item in variants:
            with self.subTest(item=item):
                result = assess([item])
                self.assertFalse(result["approved"])
                self.assertEqual(1, result["invalid_rows"])
        incomplete = row()
        del incomplete["total_expense"]
        self.assertFalse(assess([incomplete])["approved"])
        self.assertEqual("critical", qa([incomplete])["status"])

    def test_low_fallback_concentration_cannot_hide_invalid_cost(self):
        mapped = row(expense_source="mapped_product_identifier", item_total_without_tax=10000, expense_per_item=5000,
                     total_expense=5000, profit_before_ads=5000)
        for bad in [row(total_expense=64), row(item_total_without_tax=float("nan")), row(expense_source="unrecognized_provider_cost")]:
            result = qa([mapped, bad])
            self.assertEqual("critical", result["status"])
            self.assertGreater(result["failure_count"], 0)

    def test_approved_roy_physical_gift_is_outside_fallback_exception(self):
        gift = row(expense_source="zero_revenue_gift_missing_cost", item_total_without_tax=0,
                   expense_per_item=0, total_expense=0, profit_before_ads=0)
        result = assess([gift])
        self.assertEqual(0, result["fallback_rows"])
        self.assertEqual(0, result["invalid_rows"])


if __name__ == "__main__":
    unittest.main()
