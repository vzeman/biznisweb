"""Synthetic integration contracts for order credits and country completeness."""
import copy
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from export_orders import BizniWebExporter


class ReportingFinancialAdjustmentTests(unittest.TestCase):
    def setUp(self):
        self.exporter = BizniWebExporter(
            api_url="https://example.test/api/graphql", api_token="synthetic",
            project_name="roy", enable_period_bundle=False, order_facts_only=True,
        )
        self.exporter.project_settings["creditnote_fulfillment_costs"] = {"enabled": False}

    @staticmethod
    def source():
        order = {
            "id": "101", "order_num": "SYNTHETIC-101", "pur_date": "2026-09-02 10:00:00",
            "status": {"id": "4", "name": "Odoslaná"},
            "sum": {"value": 123.0, "currency": {"code": "EUR"}},
            "vat_summary": [{"tax_base": 100.0, "amount": 23.0, "tax_rate": 23}],
            "invoices": [{"id": "101", "invoice_num": "SYNTHETIC-INVOICE"}],
        }
        document = {
            "creditnote_id": "501", "number": "SYNTHETIC-CREDIT", "order_id": "101",
            "order_num": order["order_num"], "inv_id": "SYNTHETIC-INVOICE",
            "price": "10 EUR", "taxed_price": "12.30 EUR", "open": False, "storno": False,
            "created": "2026-10-01 10:00:00",
        }
        return order, document

    def frame(self):
        rows = []
        for number, country, email, prices in (
            ("SYNTHETIC-101", "SK", "a@example.test", [60.0, 40.0]),
            ("SYNTHETIC-102", "CZ", None, [30.0]),
        ):
            for index, revenue in enumerate(prices):
                rows.append({
                    "order_num": number, "customer_email": email, "purchase_date": "2026-09-02 10:00:00",
                    "delivery_country": country, "delivery_city": "City", "total_items_in_order": len(prices),
                    "product_sku": str(index), "item_label": f"Item {index}", "item_quantity": 1,
                    "item_total_without_tax": revenue, "item_total_with_tax": revenue * 1.23,
                    "item_unit_price": revenue, "item_line_sum_original": revenue,
                    "item_line_sum_with_tax_original": revenue * 1.23, "item_unit_price_original": revenue,
                    "total_expense": revenue * .4, "profit_before_ads": revenue * .6,
                    "fb_ads_daily_spend": 6.0, "google_ads_daily_spend": 4.0,
                })
        return pd.DataFrame(rows)

    def test_one_snapshot_and_later_credit_restates_original_order_once(self):
        order, document = self.source()
        with patch("creditnote_export.fetch_project_creditnotes", return_value=([document], 1)) as fetch:
            with patch("order_status_safety.fetch_order_safety_context", return_value=order) as detail:
                for _ in range(2):
                    self.exporter._prepare_order_credit_adjustments([order], datetime(2026, 10, 7))
                self.assertEqual(1, fetch.call_count)
                self.assertEqual(1, detail.call_count)
            snapshot = self.exporter._creditnote_snapshot()
            snapshot[0][0]["price"] = "999 EUR"
            self.assertEqual("10 EUR", self.exporter._creditnote_snapshot()[0][0]["price"])
        adjustment = self.exporter._credit_adjustments_by_order[order["order_num"]]
        self.assertEqual(Decimal("-10.00"), adjustment["revenue_credit_adjustment"])
        self.assertEqual("2026-09-02", adjustment["purchase_date"])
        self.assertEqual(0, adjustment["cogs_adjustment"])

    def test_source_failure_cannot_be_overwritten_by_later_success(self):
        with patch("creditnote_export.fetch_project_creditnotes", side_effect=[RuntimeError("temporary"), ([], 0)]) as fetch:
            with self.assertRaisesRegex(RuntimeError, "temporary"):
                self.exporter._creditnote_snapshot()
            with self.assertRaisesRegex(RuntimeError, "failed earlier"):
                self.exporter._creditnote_snapshot()
            self.assertEqual(1, fetch.call_count)

    def test_stale_order_total_and_unknown_issued_date_fail_closed(self):
        order, document = self.source()
        for mutation in ("total", "date", "status"):
            self.exporter._creditnote_source_snapshot = None
            changed = copy.deepcopy(order)
            changed_doc = dict(document)
            if mutation == "total":
                changed["sum"]["value"] = 100.0
            elif mutation == "status":
                changed["status"]["id"] = "74"
            else:
                changed_doc["created"] = ""
            with patch("creditnote_export.fetch_project_creditnotes", return_value=([changed_doc], 1)):
                with patch("order_status_safety.fetch_order_safety_context", return_value=changed):
                    with self.assertRaises(ValueError):
                        self.exporter._prepare_order_credit_adjustments([order], datetime(2026, 10, 7))

    def test_draft_without_created_date_is_ignored_and_future_credit_is_not_applied(self):
        order, document = self.source()
        draft = {**document, "number": "", "open": True, "created": ""}
        for row, cutoff in ((draft, datetime(2026, 10, 7)), (document, datetime(2026, 9, 30))):
            self.exporter._creditnote_source_snapshot = None
            with patch("creditnote_export.fetch_project_creditnotes", return_value=([row], 1)):
                with patch("order_status_safety.fetch_order_safety_context") as detail:
                    self.exporter._prepare_order_credit_adjustments([order], cutoff)
                    detail.assert_not_called()
        self.assertEqual({}, self.exporter._credit_adjustments_by_order)

    def test_money_country_counts_and_shared_costs_reconcile_without_fake_items(self):
        self.exporter._credit_adjustments_by_order = {"SYNTHETIC-101": {
            "revenue_credit_adjustment": Decimal("-10"), "credit_gross_adjustment": Decimal("-12.30"),
            "credit_tax_adjustment": Decimal("-2.30"),
        }}
        frame = self.exporter._add_order_credit_adjustment_columns(self.frame())
        frame = self.exporter.add_order_revenue_net_column(frame)
        self.assertEqual(3, len(frame))
        self.assertEqual(130, frame["item_total_without_tax"].sum())
        self.assertEqual(120, frame.drop_duplicates("order_num")["order_revenue_net"].sum())
        with patch.object(self.exporter, "get_daily_fixed_cost", return_value=20):
            with tempfile.TemporaryDirectory() as directory:
                self.exporter.data_dir = Path(directory)
                _, days, products, months, _ = self.exporter.create_aggregated_reports(
                    frame, datetime(2026, 9, 2), datetime(2026, 9, 2), {"2026-09-02": 6}, {"2026-09-02": 4},
                )
            country, _ = self.exporter.analyze_geographic(frame)
            self.assertEqual(2, country["orders"].sum())
            self.assertEqual(120, country["revenue"].sum())
            self.assertEqual(20, self.exporter._build_growth_order_item_frames(frame, require_customer_email=False)[0]["allocated_fixed_overhead"].sum())
        self.assertEqual(2, days["unique_orders"].sum())
        self.assertEqual(-10, days["revenue_credit_adjustment"].sum())
        self.assertEqual(120, days["total_revenue"].sum())
        self.assertEqual(130, products["total_revenue"].sum())
        self.assertEqual(52, days["product_expense"].sum())
        self.assertEqual(120, months["total_revenue"].sum())
        self.assertAlmostEqual(days["net_profit"].sum(), country["profit_with_fixed"].sum(), places=2)

    def test_creditnote_rate_counts_unique_orders_in_the_same_purchase_cohort(self):
        order, document = self.source()
        first = {**order, "price_elements": [{"type": "shipping", "title": "Packeta", "reference_id": "1"}]}
        second = {**first, "id": "102", "order_num": "SYNTHETIC-102", "pur_date": "2026-09-05 10:00:00"}
        old = {**first, "id": "103", "order_num": "SYNTHETIC-103", "pur_date": "2026-08-25 10:00:00"}
        documents = [document, {**document, "creditnote_id": "502", "number": "SYNTHETIC-CREDIT-2"},
                     {**document, "creditnote_id": "503", "number": "SYNTHETIC-CREDIT-3", "order_num": old["order_num"], "order_id": "103"}]
        context = {"project": "roy", "available": True, "included_orders": [first, second],
                   "all_orders": [first, second, old], "carrier_denominator_orders": [first, second],
                   "creditnote_order_decisions": {}, "creditnote_order_errors": {}, "status_change_audit": {},
                   "shipped_statuses": ("odoslana",)}
        with patch("creditnote_export.fetch_project_creditnotes", return_value=(documents, 3)):
            with patch.object(self.exporter, "_build_creditnote_reporting_context", return_value=context):
                result = self.exporter.analyze_creditnote_reporting_metrics(
                    [first, second], datetime(2026, 9, 1), datetime(2026, 10, 7), pd.DataFrame([{"unique_orders": 2}]),
                )
        self.assertEqual(3, result["summary"]["creditnotes"])
        self.assertEqual(2, result["summary"]["cohort_orders"])
        self.assertEqual(1, result["summary"]["cohort_creditnoted_orders"])
        self.assertEqual(50.0, result["summary"]["creditnote_rate_pct"])
        self.assertEqual(50.0, result["carrier_rows"][0]["creditnote_rate_pct"])


if __name__ == "__main__":
    unittest.main()
