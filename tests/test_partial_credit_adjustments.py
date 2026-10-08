"""Synthetic credit-note evidence; no private identities, amounts or network."""

import copy
from decimal import Decimal
import unittest

from reporting_core.credit_adjustments import CreditAdjustmentError, build_order_credit_adjustment


class PartialCreditAdjustmentTests(unittest.TestCase):
    def order(self, currency="EUR"):
        return {
            "id": "SYNTHETIC-ID", "order_num": "SYNTHETIC-ORDER",
            "pur_date": "2026-01-03 14:25:00",
            "sum": {"value": 144, "is_net_price": True, "currency": {"code": currency}},
            "vat_summary": [{"tax_base": 120, "amount": 24, "tax_rate": 20}],
            "invoices": [{"id": "SYNTHETIC-ID", "invoice_num": "SYNTHETIC-INVOICE"}],
        }

    def document(self, currency="EUR", **updates):
        result = {
            "id": "SYNTHETIC-CREDIT", "number": "SYNTHETIC-NUMBER",
            "numbered": True, "state": "issued", "order_num": "SYNTHETIC-ORDER",
            "order_id": "SYNTHETIC-ID", "invoice_number": "SYNTHETIC-INVOICE",
            "currency": currency, "amount": 24, "net_amount": 20,
        }
        result.update(updates)
        return result

    def calculate(self, order=None, documents=None, **updates):
        arguments = {
            "project": "synthetic", "source_project": "synthetic", "source_complete": True,
            "financially_included": True, "currency_rates_to_eur": {"EUR": 1, "CZK": ".04", "HUF": ".0025"},
        }
        arguments.update(updates)
        return build_order_credit_adjustment(
            self.order() if order is None else order,
            [self.document()] if documents is None else documents,
            **arguments,
        )

    def assertRejected(self, reason, **arguments):
        with self.assertRaises(CreditAdjustmentError) as caught:
            self.calculate(**arguments)
        self.assertEqual(reason, caught.exception.reason)

    def test_issued_partial_credit_is_order_level_and_does_not_return_cogs(self):
        order, docs = self.order(), [self.document()]
        original = copy.deepcopy((order, docs))
        result = self.calculate(order, docs)
        self.assertEqual(-20, result["revenue_credit_adjustment"])
        self.assertEqual(-24, result["credit_gross_adjustment"])
        self.assertEqual(-4, result["credit_tax_adjustment"])
        self.assertEqual(0, result["cogs_adjustment"])
        self.assertIsNone(result["product_allocation"])
        self.assertEqual("unallocated_order_credit", result["allocation"])
        self.assertEqual("2026-01-03", result["purchase_date"])
        self.assertEqual("issued_document_not_cash_repayment", result["settlement_basis"])
        self.assertEqual(original, (order, docs))

    def test_customer_shipping_amount_never_infers_merchandise_allocation(self):
        order = self.order()
        order["items"] = [{"sum_with_tax": {"value": 120}}]
        order["price_elements"] = [{"type": "shipping", "price": {"value": 24}}]
        result = self.calculate(order)
        self.assertEqual(-20, result["revenue_credit_adjustment"])
        self.assertIsNone(result["product_allocation"])
        self.assertNotIn("merchandise_credit", result)

    def test_empty_complete_document_set_is_verified_no_adjustment(self):
        order = self.order()
        del order["vat_summary"]
        del order["invoices"]
        self.assertEqual("no_issued_credit", self.calculate(order, [])['status'])

    def test_void_open_documents_are_ignored_explicitly(self):
        docs = [self.document(state="voided"), self.document(id="OTHER", state="open", number="", numbered=False)]
        result = self.calculate(documents=docs)
        self.assertEqual(0, result["revenue_credit_adjustment"])
        self.assertEqual(["voided", "open"], [d["reason"] for d in result["ignored_documents"]])

    def test_excluded_full_credit_is_not_deducted_twice(self):
        order = self.order()
        del order["vat_summary"]
        del order["invoices"]
        result = self.calculate(order, [self.document(amount=144, net_amount=120)], financially_included=False)
        self.assertEqual("order_excluded_no_adjustment", result["status"])
        self.assertEqual(0, result["revenue_credit_adjustment"])

    def test_full_credit_on_included_order_requires_eligibility_reconciliation(self):
        self.assertRejected("full_credit_on_included_order_requires_reconciliation",
                            documents=[self.document(amount=144, net_amount=120)])

    def test_full_credit_uses_payable_boundary_despite_vat_summary_cent(self):
        order = self.order()
        order["vat_summary"][0]["amount"] = "23.99"
        self.assertRejected("full_credit_on_included_order_requires_reconciliation", order=order,
                            documents=[self.document(amount=144, net_amount=120)])

    def test_multiple_credits_are_added_once_and_order_independent(self):
        docs = [self.document(), self.document(id="SECOND", number="SECOND-NUMBER", amount=12, net_amount=10)]
        first = self.calculate(documents=docs)
        second = self.calculate(documents=list(reversed(docs)))
        self.assertEqual(-30, first["revenue_credit_adjustment"])
        self.assertEqual(-36, first["credit_gross_adjustment"])
        self.assertEqual(first["source_fingerprint"], second["source_fingerprint"])

    def test_cumulative_full_and_excess_credit_are_rejected(self):
        for gross, net, reason in [(120, 100, "full_credit_on_included_order_requires_reconciliation"),
                                   (121, 101, "credit_exceeds_original_order")]:
            with self.subTest(gross=gross):
                docs = [self.document(), self.document(id="SECOND", number="SECOND-NUMBER", amount=gross, net_amount=net)]
                self.assertRejected(reason, documents=docs)

    def test_document_and_number_duplicates_rejected(self):
        self.assertRejected("credit_document_duplicate", documents=[self.document(), self.document()])
        self.assertRejected("credit_document_number_duplicate",
                            documents=[self.document(), self.document(id="DIFFERENT")])

    def test_incomplete_cross_project_and_unknown_eligibility_rejected(self):
        for changes, reason in [({"source_complete": False}, "credit_source_incomplete"),
                                ({"source_complete": 1}, "credit_source_incomplete"),
                                ({"source_project": "another"}, "credit_source_project_mismatch"),
                                ({"financially_included": "yes"}, "credit_order_eligibility_unknown")]:
            with self.subTest(changes=changes):
                self.assertRejected(reason, **changes)

    def test_order_invoice_and_currency_identity_tampering_rejected(self):
        cases = [("order_num", "OTHER", "credit_document_order_mismatch"),
                 ("order_id", "OTHER", "credit_document_order_mismatch"),
                 ("invoice_number", "OTHER", "credit_document_invoice_mismatch"),
                 ("currency", "CZK", "credit_document_currency_mismatch")]
        for field, value, reason in cases:
            with self.subTest(field=field):
                self.assertRejected(reason, documents=[self.document(**{field: value})])

    def test_issued_state_number_and_invoice_required(self):
        for changes, reason in [({"state": "unknown"}, "credit_document_state_unknown"),
                                ({"numbered": False}, "credit_document_state_unknown"),
                                ({"number": ""}, "credit_document_number_unknown")]:
            with self.subTest(changes=changes):
                self.assertRejected(reason, documents=[self.document(**changes)])
        order = self.order()
        order["invoices"][0]["id"] = "NOT-PARENT-ORDER"
        self.assertRejected("credit_order_invoice_identity_mismatch", order=order)

    def test_missing_or_malformed_money_fails_closed(self):
        for value in (None, True, "NaN", "Infinity", "11,20", "bad"):
            with self.subTest(value=value):
                self.assertRejected("credit_document_amount_unknown", documents=[self.document(net_amount=value)])
        for changes in ({"net_amount": -20}, {"amount": 0}, {"net_amount": 25}):
            with self.subTest(changes=changes):
                self.assertRejected("credit_document_amount_invalid", documents=[self.document(**changes)])

    def test_tax_and_net_cannot_exceed_original_order(self):
        self.assertRejected("credit_tax_exceeds_original_order", documents=[self.document(amount=80, net_amount=40)])
        self.assertRejected("credit_exceeds_original_order", documents=[self.document(amount=140, net_amount=125)])

    def test_missing_tax_basis_is_not_inferred_from_invoice_or_amount_match(self):
        order = self.order()
        del order["vat_summary"]
        self.assertRejected("credit_order_tax_basis_unknown", order=order)

    def test_source_bound_equivalent_totals_can_replace_absent_vat_summary(self):
        order = self.order()
        del order["vat_summary"]
        proof = {"project": "synthetic", "order_num": order["order_num"], "order_id": order["id"],
                 "currency": "EUR", "net_amount": 120, "gross_amount": 144,
                 "basis": "verified_item_service_totals"}
        result = self.calculate(order, verified_order_totals=proof)
        self.assertEqual(-20, result["revenue_credit_adjustment"])
        for key, value in (("project", "other"), ("order_num", "other"), ("order_id", "other"),
                           ("currency", "CZK"), ("basis", "estimate")):
            with self.subTest(key=key):
                self.assertRejected("credit_order_totals_proof_unbound", order=order,
                                    verified_order_totals={**proof, key: value})

    def test_conflicting_totals_and_wrong_grand_total_rejected(self):
        order = self.order()
        proof = {"project": "synthetic", "order_num": order["order_num"], "order_id": order["id"],
                 "currency": "EUR", "net_amount": 119, "gross_amount": 144, "basis": "explicit_native_totals"}
        self.assertRejected("credit_order_totals_proof_conflict", verified_order_totals=proof)
        order["sum"]["value"] = 150
        self.assertRejected("credit_order_totals_not_reconciled", order=order)

    def test_zero_tax_order_cannot_acquire_tax_credit(self):
        order = self.order()
        order["sum"]["value"] = 120
        order["vat_summary"][0]["amount"] = 0
        result = self.calculate(order, [self.document(amount=20, net_amount=20)])
        self.assertEqual(0, result["credit_tax_adjustment"])
        self.assertRejected("credit_zero_vat_order_tax_mismatch", order=order,
                            documents=[self.document(amount="20.01", net_amount=20)])

    def test_mixed_vat_total_preserved_without_product_allocation(self):
        order = self.order()
        order["vat_summary"] = [{"tax_base": 60, "amount": 6}, {"tax_base": 60, "amount": 12}]
        order["sum"]["value"] = 138
        result = self.calculate(order, [self.document(amount=22, net_amount=20)])
        self.assertEqual(-2, result["credit_tax_adjustment"])
        self.assertIsNone(result["product_allocation"])

    def test_foreign_fx_is_explicit_and_eur_tax_identity_conserved(self):
        result = self.calculate(self.order("HUF"), [self.document("HUF", amount="25.01", net_amount="20.03")])
        self.assertEqual(Decimal("-.05"), result["revenue_credit_adjustment"])
        self.assertEqual(Decimal("-.06"), result["credit_gross_adjustment"])
        self.assertEqual(result["credit_gross_adjustment"], result["revenue_credit_adjustment"] + result["credit_tax_adjustment"])
        self.assertRejected("credit_currency_rate_missing", currency_rates_to_eur={})
        self.assertRejected("credit_currency_rate_invalid", currency_rates_to_eur={"EUR": 0})

    def test_document_date_does_not_replace_preserved_order_date_policy(self):
        result = self.calculate(documents=[self.document(issue_date="2026-03-15", already_repaid=0)])
        self.assertEqual("2026-01-03", result["purchase_date"])
        self.assertEqual(-20, result["revenue_credit_adjustment"])


if __name__ == "__main__":
    unittest.main()
