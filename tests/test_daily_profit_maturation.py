import unittest
from datetime import datetime

import pandas as pd

from dashboard_modern import (
    extract_embedded_dashboard_payload,
    generate_modern_dashboard,
)
from export_orders import (
    PACKAGING_COST_PER_ORDER,
    PROFIT_MATURATION_MIN_CURVE_SAMPLE,
    SHIPPING_NET_PER_ORDER,
    BizniWebExporter,
)


def make_exporter() -> BizniWebExporter:
    return BizniWebExporter(
        api_url="https://example.com/api/graphql",
        api_token="token",
        project_name="vevo",
        output_tag="unit",
        enable_period_bundle=False,
    )


def item_row(order_num, email, day, revenue, cost, first_purchase=None):
    return {
        "order_num": order_num,
        "customer_email": email,
        "purchase_date_only": day,
        "customer_first_purchase_date": first_purchase or f"{day} 10:00:00",
        "item_total_without_tax": revenue,
        "total_expense": cost,
    }


class DailyProfitMaturationTests(unittest.TestCase):
    def test_loss_day_turns_green_once_the_cohort_returns(self) -> None:
        # One customer acquired on 01-01 at a loss, who comes back twice afterwards.
        df = pd.DataFrame([
            item_row("A1", "a@example.com", "2026-01-01", 100.0, 40.0),
            item_row("A2", "a@example.com", "2026-01-11", 100.0, 40.0),
            item_row("A3", "a@example.com", "2026-01-21", 100.0, 40.0),
        ])
        date_agg = pd.DataFrame([
            {"date": "2026-01-01", "net_profit": -90.0},
            {"date": "2026-01-11", "net_profit": 20.0},
            {"date": "2026-01-21", "net_profit": 20.0},
            {"date": "2026-01-31", "net_profit": 5.0},
        ])

        result = make_exporter().analyze_daily_profit_maturation(df, date_agg)

        self.assertTrue(result["available"])
        acquisition_day = result["rows"][0]
        self.assertEqual("2026-01-01", acquisition_day["date"])
        self.assertEqual(-90.0, acquisition_day["day_one_profit"])

        # Each repeat order contributes 100 - 40 - packaging - shipping.
        per_repeat = round(100.0 - 40.0 - PACKAGING_COST_PER_ORDER - SHIPPING_NET_PER_ORDER, 2)
        self.assertEqual(round(-90.0 + 2 * per_repeat, 2), acquisition_day["current_profit"])
        self.assertEqual(round(2 * per_repeat, 2), acquisition_day["maturation_uplift"])

        # -90 + 59.5 is still red on 01-11; the second repeat on 01-21 crosses zero.
        self.assertEqual(20, acquisition_day["days_to_green"])
        self.assertEqual("2026-01-21", acquisition_day["green_date"])
        self.assertEqual(2, acquisition_day["repeat_orders"])
        self.assertEqual(1, acquisition_day["repeat_customers"])
        self.assertEqual(1, acquisition_day["customers_acquired"])

    def test_repeat_revenue_is_not_credited_to_the_day_it_was_booked_on(self) -> None:
        df = pd.DataFrame([
            item_row("A1", "a@example.com", "2026-01-01", 100.0, 40.0),
            item_row("A2", "a@example.com", "2026-01-11", 100.0, 40.0),
        ])
        date_agg = pd.DataFrame([
            {"date": "2026-01-01", "net_profit": -90.0},
            {"date": "2026-01-11", "net_profit": 20.0},
        ])

        rows = make_exporter().analyze_daily_profit_maturation(df, date_agg)["rows"]
        booking_day = next(row for row in rows if row["date"] == "2026-01-11")

        # 01-11 acquired nobody, so it keeps its booked value untouched.
        self.assertEqual(20.0, booking_day["day_one_profit"])
        self.assertEqual(20.0, booking_day["current_profit"])
        self.assertEqual(0.0, booking_day["maturation_uplift"])
        self.assertEqual(0, booking_day["customers_acquired"])

    def test_day_already_green_reports_zero_days_to_green(self) -> None:
        df = pd.DataFrame([item_row("A1", "a@example.com", "2026-01-01", 100.0, 40.0)])
        date_agg = pd.DataFrame([{"date": "2026-01-01", "net_profit": 12.0}])

        row = make_exporter().analyze_daily_profit_maturation(df, date_agg)["rows"][0]

        self.assertEqual(0, row["days_to_green"])
        self.assertEqual("2026-01-01", row["green_date"])

    def test_still_red_day_without_a_cohort_is_counted_separately(self) -> None:
        df = pd.DataFrame([item_row("A1", "a@example.com", "2026-01-01", 100.0, 40.0)])
        date_agg = pd.DataFrame([
            {"date": "2026-01-01", "net_profit": 12.0},
            {"date": "2026-01-02", "net_profit": -30.0},
        ])

        summary = make_exporter().analyze_daily_profit_maturation(df, date_agg)["summary"]

        # 01-02 sold nothing, so recurring revenue can never rescue it.
        self.assertEqual(1, summary["still_red_days"])
        self.assertEqual(1, summary["no_cohort_still_red_days"])
        self.assertEqual(0, summary["waiting_still_red_days"])
        self.assertIsNone(summary["median_days_to_green"])

    def test_customer_acquired_before_the_window_does_not_start_a_cohort(self) -> None:
        df = pd.DataFrame([
            item_row(
                "A2", "a@example.com", "2026-01-05", 100.0, 40.0,
                first_purchase="2025-11-02 09:00:00",
            ),
        ])
        date_agg = pd.DataFrame([
            {"date": "2026-01-05", "net_profit": -10.0},
            {"date": "2026-01-06", "net_profit": -10.0},
        ])

        result = make_exporter().analyze_daily_profit_maturation(df, date_agg)
        summary = result["summary"]

        # The order is a repeat of a pre-window cohort, so no in-window day may claim it.
        self.assertEqual(1, summary["out_of_window_orders"])
        self.assertEqual(0.0, summary["repeat_contribution"])
        self.assertTrue(all(row["maturation_uplift"] == 0.0 for row in result["rows"]))

    def test_summary_totals_reconcile_with_the_rows(self) -> None:
        df = pd.DataFrame([
            item_row("A1", "a@example.com", "2026-01-01", 100.0, 40.0),
            item_row("A2", "a@example.com", "2026-01-11", 100.0, 40.0),
            item_row("B1", "b@example.com", "2026-01-02", 80.0, 30.0),
        ])
        date_agg = pd.DataFrame([
            {"date": "2026-01-01", "net_profit": -90.0},
            {"date": "2026-01-02", "net_profit": 15.0},
            {"date": "2026-01-11", "net_profit": 20.0},
        ])

        result = make_exporter().analyze_daily_profit_maturation(df, date_agg)
        rows, summary = result["rows"], result["summary"]

        self.assertEqual(
            round(sum(row["day_one_profit"] for row in rows), 2),
            summary["total_day_one_profit"],
        )
        self.assertEqual(
            round(sum(row["current_profit"] for row in rows), 2),
            summary["total_current_profit"],
        )
        self.assertEqual(
            round(summary["total_current_profit"] - summary["total_day_one_profit"], 2),
            summary["total_maturation_uplift"],
        )
        self.assertEqual(summary["repeat_contribution"], summary["total_maturation_uplift"])

    def test_ageing_curve_drops_points_with_too_few_tracked_days(self) -> None:
        dates = pd.date_range("2026-01-01", periods=40, freq="D").date
        df = pd.DataFrame([
            item_row(f"O{i}", f"c{i}@example.com", day.isoformat(), 100.0, 40.0)
            for i, day in enumerate(dates)
        ])
        date_agg = pd.DataFrame([{"date": day, "net_profit": -5.0} for day in dates])

        curve = make_exporter().analyze_daily_profit_maturation(df, date_agg)["curve"]

        self.assertEqual(0, curve[0]["age_days"])
        self.assertEqual(40, curve[0]["days_tracked"])
        self.assertTrue(all(point["days_tracked"] >= PROFIT_MATURATION_MIN_CURVE_SAMPLE for point in curve))
        self.assertLess(curve[-1]["age_days"], 39)

    def test_maturation_is_rendered_in_the_dashboard_ledger_and_payload(self) -> None:
        df = pd.DataFrame([
            item_row("A1", "a@example.com", "2026-01-01", 100.0, 40.0),
            item_row("A2", "a@example.com", "2026-01-11", 100.0, 40.0),
            item_row("A3", "a@example.com", "2026-01-21", 100.0, 40.0),
        ])
        date_agg = pd.DataFrame([
            {
                "date": day,
                "total_revenue": 100.0,
                "net_profit": profit,
                "contribution_profit": profit + 10.0,
                "unique_orders": 1,
                "fb_ads_spend": 10.0,
                "google_ads_spend": 0.0,
                "total_items": 1,
                "product_expense": 40.0,
                "total_cost": 60.0,
                "pre_ad_contribution_profit": profit + 20.0,
            }
            for day, profit in (
                ("2026-01-01", -90.0),
                ("2026-01-11", 20.0),
                ("2026-01-21", 20.0),
            )
        ])
        items_agg = pd.DataFrame([
            {"item_label": "Test", "total_quantity": 3, "total_revenue": 300.0}
        ])

        maturation = make_exporter().analyze_daily_profit_maturation(df, date_agg)
        html = generate_modern_dashboard(
            date_agg=date_agg,
            items_agg=items_agg,
            date_from=datetime(2026, 1, 1),
            date_to=datetime(2026, 1, 21),
            report_title="Vevo reporting",
            daily_profit_maturation=maturation,
        )

        self.assertIn("How long until a day turns green", html)
        self.assertIn('id="profitMaturationChart"', html)
        self.assertIn('id="profitPaybackCurveChart"', html)
        self.assertIn("Days to green", html)
        self.assertIn("Today (matured)", html)
        self.assertIn("Day-1 profit", html)
        self.assertIn("20 d", html)

        payload = extract_embedded_dashboard_payload(html)
        embedded = payload["daily_profit_maturation"]
        self.assertTrue(embedded["available"])
        self.assertEqual(3, len(embedded["rows"]))
        self.assertEqual(20, embedded["summary"]["median_days_to_green"])

        ledger_row = next(
            row for row in payload["daily_profit_loss"]["rows"] if row["date"] == "2026-01-01"
        )
        self.assertEqual(20, ledger_row["maturation"]["days_to_green"])

    def test_dashboard_stays_intact_without_maturation_input(self) -> None:
        date_agg = pd.DataFrame([
            {
                "date": "2026-01-01",
                "total_revenue": 100.0,
                "net_profit": -90.0,
                "contribution_profit": -80.0,
                "unique_orders": 1,
                "fb_ads_spend": 10.0,
                "google_ads_spend": 0.0,
                "total_items": 1,
                "product_expense": 40.0,
                "total_cost": 190.0,
                "pre_ad_contribution_profit": -70.0,
            }
        ])
        items_agg = pd.DataFrame([
            {"item_label": "Test", "total_quantity": 1, "total_revenue": 100.0}
        ])

        html = generate_modern_dashboard(
            date_agg=date_agg,
            items_agg=items_agg,
            date_from=datetime(2026, 1, 1),
            date_to=datetime(2026, 1, 1),
            report_title="Vevo reporting",
        )

        self.assertIn("Daily profit / loss ledger", html)
        self.assertNotIn("How long until a day turns green", html)
        # The chart code is always emitted but guarded at runtime, so the canvas is the tell.
        self.assertNotIn('id="profitMaturationChart"', html)
        self.assertFalse(
            extract_embedded_dashboard_payload(html)["daily_profit_maturation"]["available"]
        )

    def test_empty_inputs_degrade_without_raising(self) -> None:
        exporter = make_exporter()
        empty_frame = pd.DataFrame()

        self.assertFalse(exporter.analyze_daily_profit_maturation(empty_frame, empty_frame)["available"])
        self.assertFalse(
            exporter.analyze_daily_profit_maturation(
                pd.DataFrame([item_row("A1", "a@example.com", "2026-01-01", 100.0, 40.0)]),
                empty_frame,
            )["available"]
        )


if __name__ == "__main__":
    unittest.main()
