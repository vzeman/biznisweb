"""Decision-scenario invariants using synthetic customers only."""
from datetime import date
from decimal import Decimal
import unittest

from scripts.analyze_vevo_portfolio_economics import analyze


def item(order, series="Natural", size=200, net="10", cost="2", customer=None,
         quantity="1", number="1", scent="07", **extra):
    names = {"01": "Cotton Paradise", "07": "Ylang Absolute", "08": "Cotton Dream"}
    row = {"order_num": str(order), "order_id": str(order), "item_number": number,
        "realized_revenue": "true", "order_credit_net_adjustment": "0",
        "purchase_date": "2026-09-01T10:00:00",
        "customer_email": f"{customer or order}@example.invalid",
        "item_quantity": quantity, "item_total_without_tax": net,
        "item_total_with_tax": str(Decimal(net) * Decimal("1.23")),
        "total_expense": cost, "profit_before_ads": str(Decimal(net) - Decimal(cost)),
        "item_label": f"Parfum do prania Vevo {series} No.{scent} {names[scent]} ({size}ml)",
        "item_warehouse_number": f"{scent}{size}{'p' if series == 'Premium' else ('c' if scent == '01' else '')}",
        "expense_source": "mapped_item_label_exact"}
    row.update(extra)
    return row


class PortfolioEconomicsTests(unittest.TestCase):
    def summary(self, rows):
        return analyze(rows, date(2026, 10, 18))

    def test_spending_and_quantity_have_distinct_break_even_denominators(self):
        result = self.summary([item(1, net="10", cost="2"),
            item(2, "Premium", net="20", cost="8")])["periods"]["all_time"]
        scenario = result["remove"]["Premium"]
        self.assertEqual(Decimal("0.75"), scenario["same_spending"]["break_even_retained_fraction"])
        self.assertEqual(Decimal("1.5"), scenario["same_bottle_quantity_by_size"]["break_even_retained_fraction"])
        self.assertEqual(Decimal("-4"), scenario["same_bottle_quantity_by_size"]["contribution_change_by_retention_fraction"]["1"])

    def test_unrelated_basket_line_does_not_remove_premium_and_is_separate(self):
        shot = item(2, number="2", net="5", cost="1", item_label="Vevo Shot 100ml",
                    item_warehouse_number="shot")
        result = self.summary([item(1), item(2, "Premium"), shot])["periods"]["all_time"]
        self.assertEqual(1, result["financial"]["Premium"]["retention_scope_orders"])
        self.assertEqual({"Premium_only_series_with_other_lines": 1}, result["premium_baskets"])
        self.assertEqual(Decimal("4"), result["associated_lines_on_premium_orders"]["other_merchandise_including_samples"]["contribution"])
        self.assertEqual(Decimal("10"), result["financial"]["Premium"]["net"])

    def test_full_history_customer_groups_ignore_samples(self):
        sample = item(3, customer="p", item_label="Natural vzorka 10ml", item_warehouse_number="")
        result = self.summary([item(1, "Premium", customer="p"), sample,
            item(4, customer="both"), item(5, "Premium", customer="both")])
        self.assertEqual({"Premium_only": 1, "Natural_only": 0, "Both": 1}, result["lifetime_buyers"])

    def test_unavailable_exact_scent_does_not_silently_assume_replacement(self):
        result = self.summary([item(1, scent="01"), item(2, "Premium")])["periods"]["all_time"]
        match = result["remove"]["Natural"]["same_size_same_scent_singles_only"]
        self.assertEqual(Decimal("1"), match["unmatched_removed_paid_bottles"])
        self.assertIsNone(match["break_even_retained_fraction_for_full_removed_contribution"])

    def test_price_cost_alternative_matches_absolute_euro_contribution(self):
        result = self.summary([item(1, net="10", cost="2"),
            item(2, "Premium", net="10", cost="5")])["periods"]["all_time"]
        match = result["premium_price_cost_to_match_natural_absolute_contribution"]["200:07"]
        self.assertEqual(Decimal("3"), match["contribution_gap_to_natural"])
        self.assertEqual(Decimal("13"), match["required_premium_net_per_bottle_at_same_cogs"])
        self.assertEqual(Decimal("2"), match["required_premium_cogs_per_bottle_at_same_net"])

    def test_period_includes_all_90_calendar_days(self):
        result = self.summary([item(1, purchase_date="2026-07-21T00:00:00"),
            item(2, "Premium", purchase_date="2026-07-20T23:59:59")])["periods"]["last_90_days"]
        self.assertEqual("2026-07-21", result["from"])
        self.assertEqual(Decimal("10"), result["financial"]["Natural"]["net"])
        self.assertEqual(Decimal("0"), result["financial"]["Premium"]["net"])

    def test_size_scenarios_match_scent_without_mixing_200_and_500_margins(self):
        result = self.summary([item(1, net="10", cost="2"),
            item(2, "Premium", net="20", cost="8"),
            item(3, size=500, net="20", cost="5"),
            item(4, "Premium", size=500, net="20", cost="10")])["periods"]["all_time"]
        sizes = result["premium_size_transfer_sensitivity"]
        self.assertEqual(Decimal("1.5"), sizes["200"]["break_even_retained_bottle_fraction"])
        self.assertEqual(Decimal(2) / Decimal(3), sizes["500"]["break_even_retained_bottle_fraction"])
        self.assertEqual(Decimal("5"), sizes["500"]["delta_at_100pct_transfer"])
        self.assertEqual(Decimal("0"), sizes["500"]["missing_coverage_fraction"])

    def test_missing_scent_blocks_full_size_scenario(self):
        result = self.summary([item(1, size=500, scent="07"),
            item(2, "Premium", size=500, scent="08")])["periods"]["all_time"]
        size = result["premium_size_transfer_sensitivity"]["500"]
        self.assertEqual(Decimal("1"), size["unmatched_bottles"])
        self.assertEqual(Decimal("1"), size["missing_coverage_fraction"])
        self.assertIsNone(size["delta_at_100pct_transfer"])
        self.assertIsNone(size["break_even_retained_bottle_fraction"])


if __name__ == "__main__":
    unittest.main()
