import unittest

import pandas as pd

from reporting_core.cfo_kpis import build_cfo_kpi_payload, build_order_records_from_export_df


class CfoCreditAdjustmentTests(unittest.TestCase):
    def test_repeated_order_credit_is_not_subtracted_again_from_adjusted_daily_totals(self):
        # Two product rows remain 60+40. The order-level credit is repeated for
        # audit, whereas date_agg has already applied its one EUR20 reduction.
        items = pd.DataFrame([
            dict(order_num="SYNTHETIC", purchase_date="2026-01-03", customer_email="synthetic@example.test",
                 item_total_without_tax=value, order_credit_net_adjustment=-20, order_revenue_net=80)
            for value in (60, 40)
        ])
        daily = pd.DataFrame([dict(
            date="2026-01-03", total_revenue=80, unique_orders=1, product_expense=30,
            packaging_cost=1, shipping_net_cost=2, fb_ads_spend=10, google_ads_spend=10,
            contribution_profit=27, fixed_daily_cost=5, pre_ad_contribution_profit=47,
            pre_ad_contribution_margin_pct=58.75, revenue_credit_adjustment=-20,
        )])
        before = items.copy(deep=True)
        payload = build_cfo_kpi_payload(daily, items, fixed_daily_cost_eur=5)
        for period in ("daily", "weekly", "monthly", "all_time"):
            value = payload["windows"][period]
            self.assertEqual(80, value["metrics"]["revenue"])
            self.assertEqual(80, value["metrics"]["aov"])
            self.assertEqual(4, value["metrics"]["roas"])
            self.assertEqual(27, value["metrics"]["profit"])
            self.assertEqual(-20, value["secondary_metrics"]["revenue_credit_adjustment"])
        self.assertEqual(1, len(build_order_records_from_export_df(items)))
        pd.testing.assert_frame_equal(before, items)

    def test_credit_disclosure_respects_window_dates_and_legacy_absence(self):
        daily = pd.DataFrame([
            dict(date="2026-01-01", total_revenue=80, unique_orders=1, revenue_credit_adjustment=-20),
            dict(date="2026-02-01", total_revenue=60, unique_orders=1, revenue_credit_adjustment=-10),
        ])
        payload = build_cfo_kpi_payload(daily, None, fixed_daily_cost_eur=0)
        self.assertEqual(-10, payload["windows"]["monthly"]["secondary_metrics"]["revenue_credit_adjustment"])
        self.assertEqual(-30, payload["windows"]["all_time"]["secondary_metrics"]["revenue_credit_adjustment"])
        legacy = build_cfo_kpi_payload(daily.drop(columns="revenue_credit_adjustment"), None, fixed_daily_cost_eur=0)
        self.assertEqual(0, legacy["windows"]["all_time"]["secondary_metrics"]["revenue_credit_adjustment"])


if __name__ == "__main__":
    unittest.main()
