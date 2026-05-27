import unittest
from datetime import datetime

import pandas as pd

from dashboard_modern import _build_fb_daily_payload, _frame_rows


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


if __name__ == "__main__":
    unittest.main()
