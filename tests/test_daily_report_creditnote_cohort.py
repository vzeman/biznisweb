"""The email describes the existing rate's purchase cohort, not shipped totals."""

from copy import deepcopy
import unittest
from unittest.mock import patch

import daily_report_runner as runner


COHORT_BASIS = "unique_issued_credited_orders_over_all_purchase_cohort_orders"


def sample_payload():
    return {"dashboard": {"creditnotes": {
        "summary": {
            "available": True, "creditnotes": 12, "creditnoted_orders": 8,
            "all_creditnoted_orders": 10, "realized_orders": 80,
            "cohort_orders": 200, "cohort_creditnoted_orders": 10,
            "rate_basis": COHORT_BASIS, "creditnote_rate_pct": 5.0,
            "credited_gross_eur": 246.0, "credited_net_eur": 200.0,
            "revenue_excluded_orders": 7, "revenue_included_orders": 1,
            "order_not_found": 2, "fulfillment_cost_eur": 4.0,
            "fulfillment_orders": 8,
        },
        "carrier_rows": [{
            "carrier": "Packeta", "creditnoted_orders": 7, "realized_orders": 30,
            "cohort_orders": 50, "cohort_creditnoted_orders": 6,
            "rate_basis": COHORT_BASIS, "creditnote_rate_pct": 12.0,
            "rate_index": 2.4, "credited_gross_eur": 123.0, "outlier": True,
        }],
    }}}


def summary_text(payload):
    with patch.object(runner, "_load_dashboard_payload", return_value=payload):
        return runner._build_creditnote_summary({})


class DailyReportCreditnoteCohortTests(unittest.TestCase):
    def test_summary_uses_existing_purchase_cohort_and_preserves_document_money(self):
        payload = sample_payload()
        original = deepcopy(payload)
        text = summary_text(payload)
        self.assertIn("Dobropis rate: 5.00% (10/200 objednavok z rovnakej nakupnej kohorty)", text)
        self.assertNotIn("5.00% z 80", text)
        self.assertIn("12 dobropisov / 8 odoslanych dobropisovanych objednavok (celkovo 10)", text)
        self.assertIn("246.00", text)
        self.assertIn("200.00", text)
        self.assertIn("7 dobropisovanych objednavok je mimo revenue", text)
        self.assertIn("1 este ostava v revenue", text)
        self.assertIn("(8 odoslanych balikov)", text)
        self.assertEqual(original, payload)

    def test_carrier_uses_cohort_counts_without_changing_rate_index_amount_or_flag(self):
        text = summary_text(sample_payload())
        self.assertIn("Packeta: 12.00%, 6/50 objednavok kohorty, index 2.40x", text)
        self.assertNotIn("7/30", text)
        self.assertIn("suma 123.00 EUR OUTLIER", text)

    def test_explicit_zero_cohort_and_zero_rate_do_not_fall_back_to_shipped_counts(self):
        payload = sample_payload()
        summary = payload["dashboard"]["creditnotes"]["summary"]
        summary.update(cohort_orders=0, cohort_creditnoted_orders=0, creditnote_rate_pct=0.0)
        text = summary_text(payload)
        self.assertIn("Dobropis rate: 0.00% (0/0 objednavok z rovnakej nakupnej kohorty)", text)
        self.assertNotIn("0.00% z 80", text)

    def test_none_rate_is_na_rather_than_invented_zero_percent(self):
        payload = sample_payload()
        payload["dashboard"]["creditnotes"]["summary"].update(
            cohort_orders=0, cohort_creditnoted_orders=0, creditnote_rate_pct=None,
        )
        text = summary_text(payload)
        self.assertIn("Dobropis rate: N/A (0/0 objednavok", text)
        self.assertNotIn("Dobropis rate: 0.00%", text)

    def test_legacy_payload_keeps_legacy_count_basis_and_wording(self):
        payload = sample_payload()
        creditnotes = payload["dashboard"]["creditnotes"]
        for row in [creditnotes["summary"], *creditnotes["carrier_rows"]]:
            for key in ("cohort_orders", "cohort_creditnoted_orders", "rate_basis"):
                row.pop(key)
        text = summary_text(payload)
        self.assertIn("Dobropis rate: 5.00% z 80 odoslanych objednavok.", text)
        self.assertIn("Packeta: 12.00%, 7/30 odosl. obj., index 2.40x", text)
        self.assertNotIn("kohort", text)

    def test_carrier_filter_uses_same_cohort_numerator_as_the_displayed_rate(self):
        payload = sample_payload()
        carrier = payload["dashboard"]["creditnotes"]["carrier_rows"][0]
        carrier.update(cohort_creditnoted_orders=0)
        text = summary_text(payload)
        self.assertNotIn("  - Packeta:", text)
        carrier.update(cohort_creditnoted_orders=6, creditnoted_orders=0)
        text = summary_text(payload)
        self.assertIn("Packeta: 12.00%, 6/50 objednavok kohorty", text)

    def test_partial_cohort_metadata_never_mixes_in_shipped_denominator(self):
        payload = sample_payload()
        payload["dashboard"]["creditnotes"]["summary"]["cohort_orders"] = None
        payload["dashboard"]["creditnotes"]["carrier_rows"][0]["cohort_creditnoted_orders"] = None
        text = summary_text(payload)
        self.assertIn("5.00% (10/N/A objednavok z rovnakej nakupnej kohorty)", text)
        self.assertIn("Packeta: 12.00%, N/A/50 objednavok kohorty", text)
        self.assertNotIn("7/50", text)

    def test_display_does_not_recalculate_or_override_the_supplied_rate(self):
        payload = sample_payload()
        payload["dashboard"]["creditnotes"]["summary"]["creditnote_rate_pct"] = 5.01
        payload["dashboard"]["creditnotes"]["carrier_rows"][0]["creditnote_rate_pct"] = 12.01
        text = summary_text(payload)
        self.assertIn("Dobropis rate: 5.01% (10/200", text)
        self.assertIn("Packeta: 12.01%, 6/50", text)


if __name__ == "__main__":
    unittest.main()
