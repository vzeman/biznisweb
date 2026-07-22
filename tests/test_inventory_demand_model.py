import unittest

import pandas as pd

from inventory_demand_model import (
    build_robust_demand_summary,
    poisson_expected_shortage_units,
    poisson_tail_probability,
    resolve_typical_order_units,
    stockout_business_impact,
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

    def test_history_outside_forecast_lookback_still_establishes_order_baseline(self):
        rows = [
            self._row("old-1", 900, 1),
            self._row("old-2", 700, 1),
            self._row("old-3", 500, 1),
            self._row("spike", 0, 40),
        ]

        result = build_robust_demand_summary(pd.DataFrame(rows), self.anchor)
        row = result.iloc[0]

        self.assertEqual(40.0, row["raw_recent_30d_units"])
        self.assertEqual(5.0, row["adjusted_recent_30d_units"])
        self.assertTrue(row["unusual_large_order_flag"])
        self.assertEqual(4, int(row["positive_order_count_history"]))
        self.assertEqual(365, int(row["history_observation_days"]))
        self.assertLess(row["robust_baseline_30d_units"], 5.0)

    def test_two_similar_bulk_orders_are_not_capped(self):
        rows = [
            self._row("bulk-old", 120, 40, sku="BULK-SKU"),
            self._row("bulk-new", 0, 40, sku="BULK-SKU"),
        ]

        result = build_robust_demand_summary(pd.DataFrame(rows), self.anchor)
        row = result.loc[result["product_sku"] == "BULK-SKU"].iloc[0]

        self.assertFalse(row["unusual_large_order_flag"])
        self.assertEqual(40.0, row["adjusted_recent_30d_units"])
        self.assertEqual(80.0, row["adjusted_recent_180d_units"])
        self.assertGreaterEqual(float(row["outlier_threshold_units"]), 40.0)

    def test_two_sparse_orders_still_detect_clear_size_jump(self):
        rows = [
            self._row("target-old", 120, 1, sku="TARGET-SKU"),
            self._row("target-spike", 0, 40, sku="TARGET-SKU"),
        ]

        result = build_robust_demand_summary(pd.DataFrame(rows), self.anchor)
        row = result.loc[result["product_sku"] == "TARGET-SKU"].iloc[0]

        self.assertTrue(row["unusual_large_order_flag"])
        self.assertEqual(5.0, row["adjusted_recent_30d_units"])
        self.assertEqual(5.0, row["outlier_threshold_units"])

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

    def test_poisson_expected_shortage_decreases_with_available_stock(self):
        no_stock = poisson_expected_shortage_units(3.0, 0, 2)
        one_order_covered = poisson_expected_shortage_units(3.0, 2, 2)
        two_orders_covered = poisson_expected_shortage_units(3.0, 4, 2)

        self.assertAlmostEqual(6.0, no_stock, places=6)
        self.assertGreater(no_stock, one_order_covered)
        self.assertGreater(one_order_covered, two_orders_covered)
        self.assertGreaterEqual(two_orders_covered, 0.0)

    def test_stockout_business_impact_uses_positive_cm2_and_revenue(self):
        impact = stockout_business_impact(
            expected_orders=2.0,
            available_units=0,
            typical_order_units=3,
            alert_units=12,
            alert_revenue=120,
            alert_contribution=48,
        )

        self.assertAlmostEqual(6.0, impact["lead_time_expected_shortage_units"], places=6)
        self.assertAlmostEqual(60.0, impact["stockout_revenue_at_risk"], places=6)
        self.assertAlmostEqual(24.0, impact["stockout_contribution_at_risk"], places=6)

    def test_stockout_business_impact_does_not_reward_negative_contribution(self):
        impact = stockout_business_impact(
            expected_orders=1.0,
            available_units=0,
            typical_order_units=1,
            alert_units=10,
            alert_revenue=100,
            alert_contribution=-20,
        )

        self.assertEqual(0.0, impact["stockout_contribution_at_risk"])
        self.assertGreater(impact["stockout_revenue_at_risk"], 0.0)

    def test_stockout_business_impact_uses_shared_typical_order_fallback(self):
        self.assertEqual(5.0, resolve_typical_order_units(None, 20))

        impact = stockout_business_impact(
            expected_orders=1.0,
            available_units=0,
            typical_order_units=None,
            alert_units=20,
            alert_revenue=200,
            alert_contribution=80,
        )

        self.assertAlmostEqual(5.0, impact["lead_time_expected_shortage_units"], places=6)
        self.assertAlmostEqual(50.0, impact["stockout_revenue_at_risk"], places=6)
        self.assertAlmostEqual(20.0, impact["stockout_contribution_at_risk"], places=6)


if __name__ == "__main__":
    unittest.main()
