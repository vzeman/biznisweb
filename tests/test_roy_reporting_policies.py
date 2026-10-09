"""Synthetic tests for ROY's reviewed service, payment and monetary contracts."""

import copy
import os
import unittest
from unittest.mock import patch

from export_orders import BizniWebExporter, OrderRevenueReconciliationError
from reporting_core import load_project_settings


SERVICE_LABELS = ("Tringelt", "Poistenie balíka proti strate a rozbitiu")


class RoyReportingPolicyTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        self.exporter = BizniWebExporter(
            api_url="https://example.com/api/graphql", api_token="synthetic",
            project_name="roy", enable_period_bundle=False, order_facts_only=True,
        )
        # Monetary source reconciliation is tested separately; these cases isolate
        # service cost precedence and payment/status classification.
        self.exporter.project_settings["order_revenue_reconciliation_enabled"] = False

    @staticmethod
    def order(label="Synthetic product", revenue=10.0, country="SK"):
        gross = round(revenue * 1.23, 2)
        return {
            "order_num": "SYNTHETIC-ROY-POLICY",
            "pur_date": "2026-09-01 10:00:00",
            "status": {"id": "4", "name": "Odoslaná"},
            "invoice_address": {"country": country},
            "delivery_address": {"country": country},
            "sum": {"value": gross, "currency": {"code": "EUR"}},
            "items": [{
                "item_label": label, "quantity": 2, "tax_rate": 23,
                "price": {"value": revenue / 2, "currency": {"code": "EUR"}},
                "sum": {"value": revenue, "currency": {"code": "EUR"}},
                "sum_with_tax": {"value": gross, "currency": {"code": "EUR"}},
            }],
            "price_elements": [{"type": "shipping", "price": {"value": 0}}],
        }

    def test_exact_verified_checkout_service_identities_are_project_scoped(self):
        expected = {self.exporter.get_reporting_product_sku("", label) for label in SERVICE_LABELS}
        self.assertEqual(expected, set(self.exporter.project_settings["zero_cost_service_product_skus"]))
        self.assertEqual(set(SERVICE_LABELS), set(self.exporter.project_settings["zero_cost_service_labels"]))
        self.assertNotEqual(expected, set(load_project_settings("vevo")["zero_cost_service_product_skus"]))

    def test_zero_cost_rule_is_country_independent_and_wins_over_reference_and_margin(self):
        self.exporter._rebuild_product_expense_indexes({label: 7.0 for label in SERVICE_LABELS})
        overrides = {self.exporter.get_reporting_product_sku("", label): 90 for label in SERVICE_LABELS}
        with patch("export_orders.AUTHORITATIVE_MARGIN_OVERRIDE_SKUS", overrides):
            for country in ("SK", "CZ", "HU", "RO", "BG", "PL", "DE"):
                for label in SERVICE_LABELS:
                    for revenue in (10.0, 0.0, -10.0):
                        with self.subTest(country=country, label=label, revenue=revenue):
                            row = self.exporter.flatten_order(self.order(label, revenue, country))[0]
                            self.assertEqual(0.0, row["total_expense"])
                            self.assertEqual("zero_cost_service_override", row["expense_source"])
                            self.assertEqual(7.0, row["purchase_cost_reference_per_item"])
                            self.assertEqual(revenue, row["profit_before_ads"])

    def test_service_identity_drift_is_critical_and_unrelated_products_keep_costs(self):
        for label in SERVICE_LABELS:
            order = self.order(label)
            order["items"][0]["import_code"] = "SYNTHETIC-CHANGED-SKU"
            with self.subTest(label=label):
                with self.assertRaisesRegex(OrderRevenueReconciliationError, "zero_cost_service_identity_drift"):
                    self.exporter.flatten_order(order)
        # In particular, Slovak 'poistka' can refer to a knife locking mechanism.
        for label in ("Synthetic knife s poistkou", "Synthetic insurance gift product"):
            self.exporter._rebuild_product_expense_indexes({label: 7.0})
            row = self.exporter.flatten_order(self.order(label))[0]
            self.assertEqual(14.0, row["total_expense"])
            self.assertNotEqual("zero_cost_service_override", row["expense_source"])

    def test_current_and_verified_historical_cod_methods_include_shipped_and_waiting_orders(self):
        expected = {"7", "10", "16", "23", "26", "28", "31", "33", "36"}
        self.assertEqual(expected, self.exporter.realized_revenue_settings["cod_payment_ids"])
        for payment_id in expected:
            for status in ("Odoslaná", "Čaká na vybavenie"):
                order = self.order()
                order["status"]["name"] = status
                order["price_elements"].append({"type": "payment", "reference_id": payment_id, "title": "Synthetic neutral method"})
                with self.subTest(payment_id=payment_id, status=status):
                    self.assertEqual((True, "cod_status_and_payment"), self.exporter._realized_revenue_decision(order))

    def test_current_and_verified_historical_prepaid_methods_require_paid_or_fulfilled_status(self):
        expected = {"6", "11", "17", "18", "20", "21", "22", "27", "29", "30", "32", "34", "35"}
        self.assertEqual(expected, self.exporter.realized_revenue_settings["prepaid_payment_ids"])
        for payment_id in expected:
            order = self.order()
            order["price_elements"].append({"type": "payment", "reference_id": payment_id, "title": "Synthetic neutral method"})
            with self.subTest(payment_id=payment_id):
                self.assertEqual((True, "prepaid_fulfilled_status"), self.exporter._realized_revenue_decision(order))
                order["status"]["name"] = "Čaká na vybavenie"
                self.assertEqual((False, "cod_status_without_cod_payment"), self.exporter._realized_revenue_decision(order))

    def test_unknown_ids_alone_cannot_qualify_as_cod_or_prepaid(self):
        for payment_id in ("SYNTHETIC-UNKNOWN-A", "SYNTHETIC-UNKNOWN-B"):
            order = self.order()
            order["price_elements"].append({"type": "payment", "reference_id": payment_id, "title": "Synthetic unknown method"})
            with self.subTest(payment_id=payment_id):
                self.assertFalse(self.exporter._is_cod_payment(order))
                self.assertFalse(self.exporter._is_prepaid_payment(order))
        order["price_elements"][-1]["title"] = "Dobierkou"
        self.assertTrue(self.exporter._is_cod_payment(order))

    def test_reviewed_missing_metadata_non_realized_exception_is_retained(self):
        settings = copy.deepcopy(load_project_settings("roy"))
        self.assertTrue(settings["realized_revenue"]["missing_payment_metadata_non_realized_order_overrides"])
        settings["realized_revenue"]["missing_payment_metadata_non_realized_order_overrides"] = {
            "SYNTHETIC-LICENSE-EXCEPTION": "Synthetic reviewed unpaid order"
        }
        with patch("export_orders.load_project_settings", return_value=settings):
            exporter = BizniWebExporter(api_url="https://example.com/api/graphql", api_token="synthetic", project_name="roy", enable_period_bundle=False, order_facts_only=True)
        order = self.order()
        order["order_num"] = "SYNTHETIC-LICENSE-EXCEPTION"
        order.pop("price_elements")
        self.assertEqual((False, "configured_missing_payment_metadata_non_realized"), exporter._realized_revenue_decision(order))

    def test_measurement_and_rounding_contracts_cover_only_configured_currencies(self):
        settings = load_project_settings("roy")
        self.assertIs(settings["measured_country_ads_enabled"], True)
        self.assertIs(settings["order_revenue_reconciliation_enabled"], True)
        self.assertEqual({"EUR": 2, "CZK": 1, "HUF": 0, "PLN": 2, "RON": 2, "USD": 2}, settings["order_currency_rounding_precision"])
        self.assertEqual(set(settings["currency_rates_to_eur"]), set(settings["order_currency_rounding_precision"]))


if __name__ == "__main__":
    unittest.main()
