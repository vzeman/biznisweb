"""Synthetic, non-PII cases for the Natural/Premium comparison contract."""

import json
import unittest
from datetime import date
from decimal import Decimal

from scripts.analyze_vevo_laundry_series import analyze, serial


def item(order, customer, purchased, series="Natural", size=200, item_number="1", **changes):
    """Create a realized item; reserved .invalid addresses are synthetic only."""
    net = Decimal(str(changes.pop("net", "10.00")))
    gross = Decimal(str(changes.pop("gross", "12.00")))
    cost = Decimal(str(changes.pop("cost", "4.00")))
    row = {
        "order_num": f"synthetic-order-{order}",
        "order_id": f"synthetic-id-{order}",
        "item_number": str(item_number),
        "realized_revenue": "true",
        "purchase_date": purchased,
        "customer_email": f"{customer}@example.invalid",
        "order_credit_net_adjustment": "0.00",
        "item_label": f"Parfum do prania Vevo {series} No.07 Ylang Absolute ({size}ml)",
        "item_warehouse_number": f"07{size}{'p' if series == 'Premium' else ''}",
        "item_quantity": "1",
        "item_total_without_tax": str(net),
        "item_total_with_tax": str(gross),
        "total_expense": str(cost),
        "profit_before_ads": str(net - cost),
        "expense_source": "mapped_item_label_exact",
        "delivery_country": "SK",
        "invoice_country": "SK",
    }
    row.update(changes)
    return row


def premium_control():
    return item("control", "control", "2025-01-01T00:00:00", "Premium")


class VevoLaundrySeriesAnalysisTests(unittest.TestCase):
    def test_latest_choice_credits_premium_without_erasing_earlier_natural_return(self):
        result = analyze([
            item("a1", "a", "2025-01-01T10:00:00"),
            item("a2", "a", "2025-02-01T10:00:00"),
            item("a3", "a", "2025-03-01T10:00:00", "Premium"),
            item("b1", "b", "2025-01-01T10:00:00"),
        ], date(2025, 7, 1))

        natural = result["all_time"]["Natural"]
        self.assertEqual(2, natural["customers"])
        self.assertEqual(1, natural["repeat_any"])
        self.assertEqual(50.0, natural["repeat_any_pct"])
        self.assertEqual(50.0, natural["same_any_pct"])
        self.assertEqual({"Natural": 1, "Premium": 0, "Both": 0}, natural["next"])
        self.assertEqual({"Natural": 0, "Premium": 1, "Both": 0}, natural["last"])
        self.assertEqual(0, result["all_time"]["Premium"]["customers"])
        self.assertIsNone(result["all_time"]["Premium"]["same_any_pct"])
        self.assertEqual(1, result["financial"]["Premium"]["buyers"])
        self.assertEqual(0, result["financial"]["Premium"]["buyers_repeat_same"])

    def test_mixed_first_and_following_orders_stay_in_separate_matrix_cells(self):
        result = analyze([
            item("a1", "a", "2025-01-01T10:00:00", item_number="1"),
            item("a1", "a", "2025-01-01T10:00:00", "Premium", item_number="2"),
            item("a2", "a", "2025-02-01T10:00:00", "Premium"),
            item("b1", "b", "2025-01-02T10:00:00"),
            item("b2", "b", "2025-02-02T10:00:00", item_number="1"),
            item("b2", "b", "2025-02-02T10:00:00", "Premium", item_number="2"),
        ], date(2025, 7, 1))

        self.assertEqual(4, result["selected_orders"])
        self.assertEqual(2, result["selected_customers"])
        both = result["all_time"]["Both"]
        self.assertEqual(1, both["customers"])
        self.assertEqual({"Natural": 0, "Premium": 1, "Both": 0}, both["last"])
        self.assertIsNone(both["same_any_pct"])
        natural = result["all_time"]["Natural"]
        self.assertEqual(1, natural["same_any"])
        self.assertEqual({"Natural": 0, "Premium": 0, "Both": 1}, natural["last"])
        self.assertEqual(0.0, natural["next_same_exclusive_pct"])
        self.assertEqual(2, sum(c["customers"] for c in result["all_time"].values()))
        self.assertEqual(2, sum(sum(c["last"].values()) for c in result["all_time"].values()))

    def test_two_bottles_in_one_order_are_not_a_return(self):
        result = analyze([
            item("a1", "a", "2025-01-01T10:00:00", item_number="1"),
            item("a1", "a", "2025-01-01T10:00:00", item_number="2"),
            premium_control(),
        ], date(2025, 7, 1))

        self.assertEqual(Decimal("2"), result["financial"]["Natural"]["bottles"])
        self.assertEqual(1, result["financial"]["Natural"]["orders"])
        self.assertEqual(0, result["all_time"]["Natural"]["repeat_any"])
        self.assertEqual(0, result["financial"]["Natural"]["buyers_repeat_same"])

    def test_volume_change_and_normalized_email_count_as_one_returning_customer(self):
        result = analyze([
            item("a1", "a", "2025-01-01T10:00:00", size=200,
                 customer_email="  A@EXAMPLE.INVALID "),
            item("a2", "a", "2025-02-01T10:00:00", size=500),
            premium_control(),
        ], date(2025, 7, 1))

        natural = result["all_time"]["Natural"]
        self.assertEqual(1, natural["customers"])
        self.assertEqual(100.0, natural["same_any_pct"])
        self.assertEqual(1, natural["next"]["Natural"])
        self.assertEqual(1, result["entry_size_90d"]["200"]["Natural"]["same_any"])
        self.assertEqual(0, result["entry_size_90d"]["500"]["Natural"]["customers"])

    def test_ninety_day_window_requires_maturity_and_has_exact_elapsed_boundary(self):
        result = analyze([
            item("a1", "a", "2025-01-01T00:00:00"),
            item("a2", "a", "2025-04-01T00:00:00"),
            item("b1", "b", "2025-01-01T00:00:00"),
            item("b2", "b", "2025-04-01T00:00:01"),
            item("c1", "c", "2025-01-02T00:00:00"),
            item("c2", "c", "2025-01-03T00:00:00"),
            premium_control(),
        ], date(2025, 4, 1))

        self.assertEqual(3, result["all_time"]["Natural"]["repeat_any"])
        mature = result["windows"]["90"]["Natural"]
        self.assertEqual(2, mature["customers"])
        self.assertEqual(1, mature["repeat_any"])
        self.assertEqual(50.0, mature["same_any_pct"])
        self.assertEqual(0, result["windows"]["180"]["Natural"]["customers"])

    def test_common_start_filters_original_cohorts_without_resetting_old_customers(self):
        result = analyze([
            item("old1", "old", "2025-01-01T10:00:00"),
            item("old2", "old", "2025-02-10T10:00:00"),
            item("new1", "new", "2025-02-05T10:00:00"),
            item("new2", "new", "2025-03-01T10:00:00", size=500),
            item("p1", "p", "2025-02-01T10:00:00", "Premium"),
        ], date(2025, 5, 10))

        self.assertEqual("2025-02-01", result["common_start"])
        self.assertEqual(2, result["all_time"]["Natural"]["customers"])
        common = result["common_launch_windows"]["90"]["Natural"]
        self.assertEqual(1, common["customers"])
        self.assertEqual(1, common["same_any"])
        self.assertEqual(100.0, common["same_any_pct"])

    def test_series_entry_windows_include_switchers_and_mixed_buyers_once_per_series(self):
        result = analyze([
            item("a1", "a", "2025-01-03T10:00:00"),
            item("a2", "a", "2025-01-31T10:00:00", "Premium"),
            item("a3", "a", "2025-02-15T10:00:00", "Premium"),
            item("b1", "b", "2025-01-01T10:00:00", item_number="1"),
            item("b1", "b", "2025-01-01T10:00:00", "Premium", item_number="2"),
            item("b2", "b", "2025-02-02T10:00:00"),
        ], date(2025, 5, 15))

        self.assertEqual(0, result["all_time"]["Premium"]["customers"])
        self.assertEqual(2, result["selected_customers"])
        fair = result["all_buyer_series_windows"]["90"]
        for series in ("Natural", "Premium"):
            with self.subTest(series=series):
                self.assertEqual(2, fair[series]["customers"])
                self.assertEqual(1, fair[series]["same_any"])
                self.assertEqual(50.0, fair[series]["same_any_pct"])
                self.assertEqual(2, fair[series]["repeat_any"])
                self.assertEqual(100.0, fair[series]["repeat_any_pct"])

    def test_series_entry_before_common_start_is_not_reset_by_a_later_same_series_order(self):
        result = analyze([
            item("old1", "old", "2025-01-01T10:00:00"),
            item("old2", "old", "2025-02-10T10:00:00"),
            item("old3", "old", "2025-02-15T10:00:00", "Premium"),
            item("old4", "old", "2025-03-01T10:00:00", "Premium"),
            item("new1", "new", "2025-02-05T10:00:00"),
            item("p1", "p", "2025-02-01T10:00:00", "Premium"),
        ], date(2025, 5, 31))

        self.assertEqual("2025-02-01", result["common_start"])
        fair = result["all_buyer_series_windows"]["90"]
        self.assertEqual(1, fair["Natural"]["customers"])
        self.assertEqual(0, fair["Natural"]["same_any"])
        self.assertEqual(2, fair["Premium"]["customers"])
        self.assertEqual(1, fair["Premium"]["same_any"])
        self.assertEqual(50.0, fair["Premium"]["same_any_pct"])

    def test_series_maturity_and_followup_begin_at_its_own_first_purchase(self):
        result = analyze([
            item("a1", "a", "2025-01-01T00:00:00"),
            item("a2", "a", "2025-02-01T00:00:00", "Premium"),
            item("a3", "a", "2025-05-02T00:00:00", "Premium"),
            item("b1", "b", "2025-01-02T00:00:00"),
            item("b2", "b", "2025-02-02T00:00:00", "Premium"),
            item("b3", "b", "2025-02-03T00:00:00", "Premium"),
            item("c1", "c", "2025-01-03T00:00:00"),
            item("c2", "c", "2025-02-01T00:00:00", "Premium"),
            item("c3", "c", "2025-05-02T00:00:01", "Premium"),
        ], date(2025, 5, 2))

        self.assertEqual(3, result["financial"]["Premium"]["buyers_repeat_same"])
        fair = result["all_buyer_series_windows"]["90"]
        self.assertEqual(0, fair["Natural"]["customers"])
        self.assertEqual(2, fair["Premium"]["customers"])
        self.assertEqual(1, fair["Premium"]["same_any"])
        self.assertEqual(1, fair["Premium"]["repeat_any"])
        self.assertEqual(50.0, fair["Premium"]["same_any_pct"])

    def test_financial_totals_use_decimal_and_exclude_unrelated_merchandise(self):
        sample = item("sample", "sample", "2025-01-01T10:00:00", net="999", gross="1198.8", cost="100")
        sample["item_label"] = "Parfum do prania Vevo Natural No.07 Ylang Absolute (Vzorka 10ml)"
        result = analyze([
            item("a1", "a", "2025-01-01T10:00:00", net="0.10", gross="0.12", cost="0.03"),
            item("a2", "a", "2025-02-01T10:00:00", net="0.20", gross="0.24", cost="0.07"),
            premium_control(), sample,
        ], date(2025, 7, 1))

        natural = result["financial"]["Natural"]
        self.assertEqual(Decimal("0.30"), natural["net"])
        self.assertEqual(Decimal("0.36"), natural["gross"])
        self.assertEqual(Decimal("0.10"), natural["cogs"])
        self.assertEqual(Decimal("0.20"), natural["profit_before_other_costs"])
        self.assertEqual(4, result["source_orders"])
        self.assertEqual(3, result["selected_orders"])

    def test_free_bottle_keeps_cost_but_does_not_create_a_paid_return(self):
        result = analyze([
            item("a1", "a", "2025-01-01T10:00:00"),
            item("a2", "a", "2025-02-01T10:00:00", net="0", gross="0", cost="4.29"),
            item("free", "free", "2025-01-01T10:00:00", net="0", gross="0", cost="4.29"),
            premium_control(),
        ], date(2025, 7, 1))

        natural = result["financial"]["Natural"]
        self.assertEqual(Decimal("10"), natural["net"])
        self.assertEqual(Decimal("12.58"), natural["cogs"])
        self.assertEqual(Decimal("-2.58"), natural["profit_before_other_costs"])
        self.assertEqual(Decimal("1"), natural["bottles"])
        self.assertEqual(Decimal("2"), natural["free_bottles"])
        self.assertEqual(1, natural["buyers"])
        self.assertEqual(0, result["all_time"]["Natural"]["repeat_any"])
        self.assertEqual(2, result["selected_customers"])

    def test_duplicate_export_lines_fail_instead_of_double_counting(self):
        row = item("a1", "a", "2025-01-01T10:00:00")
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            analyze([row, dict(row), premium_control()], date(2025, 7, 1))

    def test_unallocated_credit_blocks_product_profitability(self):
        row = item("a1", "a", "2025-01-01T10:00:00", order_credit_net_adjustment="-2.00")
        with self.assertRaisesRegex(ValueError, "credits"):
            analyze([row, premium_control()], date(2025, 7, 1))

    def test_negative_or_nonfinite_selected_values_and_unreconciled_profit_fail(self):
        cases = (
            {"item_quantity": "-1"},
            {"item_total_without_tax": "-1"},
            {"item_total_with_tax": "-1"},
            {"total_expense": "-1"},
            {"item_total_without_tax": "NaN"},
            {"total_expense": "Infinity"},
            {"profit_before_ads": "7.00"},
        )
        for change in cases:
            with self.subTest(change=change), self.assertRaises(ValueError):
                analyze([item("a1", "a", "2025-01-01T10:00:00", **change), premium_control()], date(2025, 7, 1))

    def test_missing_identity_and_ambiguous_purchase_order_fail(self):
        cases = (
            [item("a1", "a", "2025-01-01T10:00:00", customer_email="")],
            [item("a1", "a", "2025-01-01T10:00:00"),
             item("a2", "a", "2025-01-01T10:00:00")],
        )
        for rows in cases:
            with self.subTest(rows=len(rows)), self.assertRaises(ValueError):
                analyze([*rows, premium_control()], date(2025, 7, 1))

    def test_aggregate_output_contains_no_customer_or_order_identifiers(self):
        rows = [item("a1", "a", "2025-01-01T10:00:00"), premium_control()]
        payload = json.dumps(analyze(rows, date(2025, 7, 1)), default=serial)
        for row in rows:
            for field in ("customer_email", "order_num", "order_id"):
                self.assertNotIn(row[field], payload)


if __name__ == "__main__":
    unittest.main()
