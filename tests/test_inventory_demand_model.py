import unittest

import pandas as pd

from inventory_demand_model import (
    build_robust_demand_summary,
    poisson_tail_probability,
    update_m_of_n_signal_history,
)


class RobustInventoryDemandModelTests(unittest.TestCase):
    anchor = pd.Timestamp("2026-07-01")

    @staticmethod
    def _row(order_num, days_ago, quantity, *, sku="SKU-1", customer="a@example.com"):
        return {
            "product_sku": sku,
            "order_num": order_num,
            "customer_email": customer,
            "item_label": "Test product",
            "purchase_datetime": RobustInventoryDemandModelTests.anchor
            - pd.Timedelta(days=days_ago),
            "item_quantity": quantity,
        }

    def test_one_large_order_is_capped_in_recurring_baseline(self):
        rows = [
            self._row("old-1", 300, 1),
            self._row("old-2", 200, 1),
            self._row("old-3", 100, 1),
            self._row("spike", 0, 40),
        ]

        result = build_robust_demand_summary(pd.DataFrame(rows), self.anchor)
        row = result.iloc[0]

        self.assertEqual(40.0, row["raw_recent_30d_units"])
        self.assertEqual(5.0, row["adjusted_recent_30d_units"])
        self.assertLess(row["robust_baseline_30d_units"], 5.0)
        self.assertTrue(row["unusual_large_order_flag"])
        self.assertEqual(35.0, row["unusual_large_order_adjustment_units_30d"])
        self.assertEqual("one_off_large_order", row["demand_signal_code"])
        self.assertEqual("tsb_intermittent", row["demand_model"])

    def test_signed_lines_are_netted_before_order_outlier_detection(self):
        rows = [
            self._row("old-1", 300, 1),
            self._row("old-2", 200, 1),
            self._row("old-3", 100, 1),
            self._row("corrected", 0, 40),
            self._row("corrected", 0, -39),
        ]

        result = build_robust_demand_summary(pd.DataFrame(rows), self.anchor)
        row = result.iloc[0]

        self.assertEqual(1.0, row["raw_recent_30d_units"])
        self.assertFalse(row["unusual_large_order_flag"])

    def test_repeated_large_orders_are_confirmed_and_not_capped(self):
        rows = [
            self._row(f"old-{index}", 350 - index * 20, 1)
            for index in range(12)
        ]
        rows.extend(
            [
                self._row("bulk-1", 28, 40),
                self._row("bulk-2", 14, 40),
                self._row("bulk-3", 0, 40),
            ]
        )

        result = build_robust_demand_summary(pd.DataFrame(rows), self.anchor)
        row = result.iloc[0]

        self.assertTrue(row["confirmed_repeated_bulk_flag"])
        self.assertFalse(row["unusual_large_order_flag"])
        self.assertEqual(120.0, row["adjusted_recent_30d_units"])
        self.assertEqual("confirmed_repeated_bulk", row["demand_model"])
        self.assertGreater(row["robust_baseline_30d_units"], 40.0)

    def test_missing_order_numbers_do_not_collapse_into_one_order(self):
        rows = [
            self._row("", 7, 1),
            self._row("", 3, 1),
            self._row("", 0, 1),
        ]

        result = build_robust_demand_summary(pd.DataFrame(rows), self.anchor)

        self.assertEqual(3, int(result.iloc[0]["order_count_30d"]))

    def test_m_of_n_history_is_same_day_idempotent(self):
        first = update_m_of_n_signal_history(
            [],
            {"SKU-1": True},
            check_date="2026-07-01",
        )
        same_day = update_m_of_n_signal_history(
            first.to_dict("records"),
            {"SKU-1": False},
            check_date="2026-07-01",
        )
        second_day = update_m_of_n_signal_history(
            same_day.to_dict("records"),
            {"SKU-1": True},
            check_date="2026-07-02",
        )
        third_day = update_m_of_n_signal_history(
            second_day.to_dict("records"),
            {"SKU-1": True},
            check_date="2026-07-03",
        )

        self.assertEqual("0", same_day.iloc[0]["trend_candidate_checks"])
        self.assertFalse(bool(second_day.iloc[0]["trend_confirmed_flag"]))
        self.assertEqual("011", third_day.iloc[0]["trend_candidate_checks"])
        self.assertTrue(bool(third_day.iloc[0]["trend_confirmed_flag"]))

    def test_poisson_tail_probability_is_bounded_and_monotonic(self):
        low = poisson_tail_probability(0.5, 3)
        high = poisson_tail_probability(3.0, 3)

        self.assertGreaterEqual(low, 0.0)
        self.assertLessEqual(high, 1.0)
        self.assertGreater(high, low)


if __name__ == "__main__":
    unittest.main()
