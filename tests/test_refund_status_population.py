from datetime import datetime
import unittest
from unittest.mock import Mock

import pandas as pd

from export_orders import BizniWebExporter


class RefundStatusPopulationTests(unittest.TestCase):
    def setUp(self):
        self.exporter = object.__new__(BizniWebExporter)
        self.exporter.excluded_status_orders = []
        self.exporter._reporting_order_context = Mock(side_effect=lambda order: order)
        self.exporter.flatten_order = Mock(side_effect=lambda order: order["synthetic_rows"])
        self.start = datetime(2026, 1, 1)
        self.end = datetime(2026, 1, 31)

    def included(self):
        return pd.DataFrame([
            dict(order_num="SYNTHETIC-PAID", purchase_date="2026-01-05", status_name="Paid",
                 item_total_without_tax=amount, order_credit_net_adjustment=-10)
            for amount in (40, 60)
        ])

    def excluded(self, ref="SYNTHETIC-RETURN", status="Vrátené", date="2026-01-06"):
        return dict(order_num=ref, pur_date=date, status={"name": status},
                    synthetic_rows=[{"item_total_without_tax": 25}, {"item_total_without_tax": 15}])

    def test_all_status_denominator_and_return_amount_are_order_deduplicated(self):
        returned = self.excluded()
        self.exporter.excluded_status_orders = [returned, returned, self.excluded("CANCEL", "Storno")]
        original = self.included()
        population = self.exporter._build_refund_population_frame(original, self.start, self.end)
        result = self.exporter.analyze_refunds(population)
        self.assertEqual(3, result["summary"]["total_orders"])
        self.assertEqual(1, result["summary"]["refund_orders"])
        self.assertEqual(33.33, result["summary"]["refund_rate_pct"])
        self.assertEqual(40, result["summary"]["refund_amount"])
        self.assertEqual(100, population.loc[population.order_num == "SYNTHETIC-PAID", "refund_proxy_order_net"].iloc[0])
        self.assertFalse(result["summary"]["financial_deduction_applied"])
        self.assertFalse(result["summary"]["cash_refund_confirmed"])
        self.assertEqual("all_order_statuses_purchase_date_cohort", result["summary"]["population_basis"])
        self.assertEqual(2, len(original))

    def test_period_and_source_canonical_status_are_used(self):
        inside = self.excluded(status="Provider renamed return")
        self.exporter.excluded_status_orders = [inside, self.excluded("OUTSIDE", date="2025-12-31")]
        self.exporter._reporting_order_context = Mock(side_effect=lambda order: {**order, "status": {"name": "Refunded"}})
        frame = self.exporter._build_refund_population_frame(pd.DataFrame(), self.start, self.end)
        self.assertEqual(1, len(frame))
        self.assertEqual("Refunded", frame.iloc[0]["status_name"])
        self.assertEqual(1, self.exporter.analyze_refunds(frame)["summary"]["refund_orders"])
        self.exporter.flatten_order.assert_called_once_with(inside)

    def test_empty_population_is_safe_and_has_explicit_proxy_basis(self):
        frame = self.exporter._build_refund_population_frame(pd.DataFrame(), self.start, self.end)
        result = self.exporter.analyze_refunds(frame)
        self.assertEqual(0, result["summary"]["total_orders"])
        self.assertEqual(0, result["summary"]["refund_amount"])
        self.assertEqual("original_merchandise_net_of_returned_status_orders", result["summary"]["amount_basis"])

    def test_partial_credit_keeps_paid_order_out_of_return_status_numerator(self):
        result = self.exporter.analyze_refunds(
            self.exporter._build_refund_population_frame(self.included(), self.start, self.end)
        )
        self.assertEqual(1, result["summary"]["total_orders"])
        self.assertEqual(0, result["summary"]["refund_orders"])
        self.assertEqual(0, result["summary"]["refund_amount"])


if __name__ == "__main__":
    unittest.main()
