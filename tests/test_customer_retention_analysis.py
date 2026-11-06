"""Boundary checks for the standalone, offline analysis (no provider calls)."""

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.analyze_customer_retention import analyze


class CustomerRetentionAnalysisTests(unittest.TestCase):
    def test_identity_item_dedup_and_complete_observation_windows(self):
        # A returns exactly 30 days later with changed casing. B is too recent
        # for a 30-day window. C returns after 31 days, outside that window.
        rows = [
            ("a1", "A@example.test", "2026-01-01 12:00:00", "2026-01-01 12:00:00", False),
            ("a1", "A@example.test", "2026-01-01 12:00:00", "2026-01-01 12:00:00", False),
            ("a2", "a@example.test", "2026-01-31 12:00:00", "2026-01-01 12:00:00", True),
            ("b1", "b@example.test", "2026-02-25 12:00:00", "2026-02-25 12:00:00", False),
            ("c1", "c@example.test", "2026-01-01 13:00:00", "2026-01-01 13:00:00", False),
            ("c2", "c@example.test", "2026-02-01 13:00:00", "2026-01-01 13:00:00", True),
        ]
        frame = pd.DataFrame(rows, columns=["order_num", "customer_email", "purchase_date",
                                          "customer_first_purchase_date", "is_returning_customer_order"])
        frame["order_revenue_net"] = "10.00"
        frame["delivery_country"] = ""
        frame["invoice_country"] = "sk"
        payload = {
            "date_from": "2026-01-01", "date_to": "2026-02-28", "generated_at": "2026-03-01",
            "dashboard": {"cohort_summary": {"total_customers": 4, "repeat_customers": 1},
                          "kpis": {"windows": {"all_time": {"metrics": {"orders": 5, "revenue": 50}}}}},
            "source_health": {"is_partial": False, "qa_failure_count": 0,
                              "qa_warning_count": 0, "summary": "Fixture"},
        }
        with tempfile.TemporaryDirectory() as directory:
            csv_path, payload_path = Path(directory) / "orders.csv", Path(directory) / "payload.json"
            frame.to_csv(csv_path, index=False)
            payload_path.write_text(json.dumps(payload), encoding="utf-8")
            result = analyze(csv_path, payload_path)
        self.assertEqual(result["summary"]["customers"], 3)
        self.assertEqual(result["summary"]["repeat_customers"], 2)
        self.assertEqual(result["summary"]["orders"], 5)
        self.assertEqual(result["summary"]["fixed_windows"]["30"], {"eligible": 2, "repeat": 1, "pct": 50.0})
        self.assertIsNone(result["cohorts"][1]["pct_30d"])
        self.assertIsNone(result["cohorts"][0]["pct_90d"])
        self.assertEqual(result["monthly"][0]["prior_month_customers"], 0)
        self.assertEqual(result["monthly"][0]["repeat_orders"], 1)
        self.assertEqual(result["monthly"][1]["prior_month_customers"], 1)
        self.assertEqual(result["markets_by_first_order"][0]["first_order_market"], "sk")


if __name__ == "__main__":
    unittest.main()
