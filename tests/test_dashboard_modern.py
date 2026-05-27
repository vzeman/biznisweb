import unittest
from datetime import datetime

import pandas as pd

from dashboard_modern import (
    _build_fb_daily_payload,
    _frame_rows,
    extract_embedded_dashboard_payload,
    generate_modern_dashboard,
)


class DashboardModernTests(unittest.TestCase):
    def test_fb_daily_payload_zero_fills_missing_report_dates(self) -> None:
        payload = _build_fb_daily_payload(
            datetime(2026, 5, 1),
            datetime(2026, 5, 4),
            {
                "2026-05-01": {"spend": 10.24, "clicks": 80, "impressions": 3924},
                "2026-05-03": {"spend": 10.09, "clicks": 120, "impressions": 5630},
            },
        )

        self.assertEqual(
            ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04"],
            payload["dates"],
        )
        self.assertEqual([10.24, 0.0, 10.09, 0.0], payload["spend"])
        self.assertEqual([80.0, 0.0, 120.0, 0.0], payload["clicks"])
        self.assertEqual([3924.0, 0.0, 5630.0, 0.0], payload["impressions"])

    def test_frame_rows_serializes_nested_country_top_products(self) -> None:
        rows = _frame_rows(
            pd.DataFrame(
                [
                    {
                        "country": "sk",
                        "top_products": [
                            {"sku": "12474", "revenue": 100.5, "stockout_date": pd.NaT},
                            {"sku": "sd32", "revenue": 20, "note": None},
                        ],
                    }
                ]
            ),
            ["country", "top_products"],
        )

        self.assertEqual("sk", rows[0]["country"])
        self.assertEqual("12474", rows[0]["top_products"][0]["sku"])
        self.assertIsNone(rows[0]["top_products"][0]["stockout_date"])

    def test_roy_loss_product_payload_keeps_gross_loss_fields(self) -> None:
        date_agg = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-05-01"),
                    "total_revenue": 100.0,
                    "net_profit": 20.0,
                    "contribution_profit": 25.0,
                    "unique_orders": 1,
                    "fb_ads_spend": 0.0,
                    "google_ads_spend": 0.0,
                    "total_items": 1,
                    "product_expense": 80.0,
                    "total_cost": 80.0,
                    "pre_ad_contribution_profit": 20.0,
                }
            ]
        )
        items_agg = pd.DataFrame([{"item_label": "Test", "total_quantity": 1, "total_revenue": 100.0}])
        loss_product_rows = pd.DataFrame(
            [
                {
                    "sku": "LOSS-1",
                    "product": "Loss product",
                    "orders": 3,
                    "units": 3,
                    "revenue": 30.0,
                    "gross_profit": -3.0,
                    "profit_without_fixed": 5.0,
                    "profit_with_fixed": 10.0,
                    "gross_margin_pct": -10.0,
                    "margin_without_fixed_pct": 16.7,
                    "margin_with_fixed_pct": 33.3,
                    "first_sale": "2026-05-01",
                    "last_sale": "2026-05-02",
                }
            ]
        )

        html = generate_modern_dashboard(
            date_agg,
            items_agg,
            datetime(2026, 5, 1),
            datetime(2026, 5, 1),
            report_title="ROY test",
            advanced_dtc_metrics={
                "roy_product_demand": {
                    "summary": {"inventory_status": "ok"},
                    "loss_product_rows": loss_product_rows,
                }
            },
            source_health={"project": "roy"},
        )

        payload = extract_embedded_dashboard_payload(html)
        row = payload["roy_product_demand"]["loss_product_rows"][0]

        self.assertEqual(-3.0, row["gross_profit"])
        self.assertEqual(-10.0, row["gross_margin_pct"])


if __name__ == "__main__":
    unittest.main()
