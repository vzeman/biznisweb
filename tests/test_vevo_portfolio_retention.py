"""Behavioral contracts for portfolio sensitivity cohorts and uncertainty."""
import unittest
from datetime import date, datetime

from scripts.analyze_vevo_portfolio_retention import (
    bootstrap_difference, build_sequences, calculate, entry_records, post_both,
)
from tests.test_vevo_laundry_series_analysis import item


class PortfolioRetentionTests(unittest.TestCase):
    def test_controlled_entry_keeps_unrelated_items_and_requires_one_perfume_total(self):
        rows = [
            item("a1", "a", "2025-01-01T10:00:00"),
            item("a1", "a", "2025-01-01T10:00:00", item_number="2",
                 item_label="Vevo Shot 200ml", item_warehouse_number=""),
            item("a2", "a", "2025-02-01T10:00:00"),
            item("b1", "b", "2025-01-01T10:00:00", "Premium"),
            item("b1", "b", "2025-01-01T10:00:00", item_number="2"),
        ]
        result = calculate(rows, date(2025, 8, 1), bootstrap_draws=20)
        controlled = result["windows"]["90"]["sensitivities"]["one_standalone_bottle_total_shared_scents"]
        self.assertEqual(1, controlled["Natural"]["customers"])
        self.assertEqual(1, controlled["Natural"]["same"])
        self.assertEqual(0, controlled["Premium"]["customers"])

    def test_prior_other_and_first_mixed_are_distinct_and_earlier_entry_is_not_reset(self):
        seqs = build_sequences([
            item("a1", "a", "2024-12-01T10:00:00"),
            item("a2", "a", "2025-01-01T10:00:00", "Premium"),
            item("a3", "a", "2025-02-01T10:00:00"),
            item("b1", "b", "2025-01-01T10:00:00"),
            item("b1", "b", "2025-01-01T10:00:00", "Premium", item_number="2"),
        ])
        result = entry_records(seqs, datetime(2025, 8, 1), datetime(2025, 1, 1), 90)
        self.assertEqual(1, len(result["Natural"]))
        self.assertEqual("first_bottle_both_series", result["Natural"][0]["prior"])
        self.assertEqual(["prior_other_series", "first_bottle_both_series"],
                         [row["prior"] for row in result["Premium"]])
        self.assertTrue(result["Premium"][0]["any"])
        self.assertFalse(result["Premium"][0]["same"])

    def test_post_both_requires_later_order_and_respects_maturity_and_window(self):
        seqs = build_sequences([
            item("a1", "a", "2025-01-01T10:00:00"),
            item("a2", "a", "2025-02-01T10:00:00", "Premium"),
            item("a3", "a", "2025-03-01T10:00:00"),
            item("a4", "a", "2025-06-01T10:00:00", "Premium"),
            item("b1", "b", "2025-07-20T10:00:00"),
            item("b1", "b", "2025-07-20T10:00:00", "Premium", item_number="2"),
        ])
        all_time = post_both(seqs, datetime(2025, 8, 1))
        window = post_both(seqs, datetime(2025, 8, 1), 90)
        self.assertEqual(2, all_time["customers"])
        self.assertEqual({"Natural": 1}, all_time["next"])
        self.assertEqual({"Premium": 1}, all_time["last"])
        self.assertEqual(1, window["customers"])
        self.assertEqual({"Natural": 1}, window["last"])

    def test_cluster_bootstrap_preserves_perfectly_paired_outcomes(self):
        records = [{"customer_index": i, "same": bool(i % 2), "any": bool(i % 2),
                    "ml": 200, "bottles": 1} for i in range(8)]
        result = bootstrap_difference({"Natural": records, "Premium": records}, draws=100)
        self.assertEqual(8, result["overlapping_customers"])
        self.assertEqual([0.0, 0.0], result["ci95_percentile_pp"])
        self.assertEqual(0.0, result["difference_pp"])


if __name__ == "__main__":
    unittest.main()
