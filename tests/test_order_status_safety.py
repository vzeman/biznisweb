import copy
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from gql import gql

from order_status_safety import (
    ORDER_SAFETY_QUERY,
    assess_fulfillment_evidence,
    assess_payment_evidence,
    change_status_verified,
    creditnote_coverage_reason,
    decide_recovery,
    execute_read,
    fetch_order_safety_context,
    status_write_block_reason,
)


def money(value=100, currency="EUR", net=False):
    return {"value": value, "currency": {"code": currency}, "is_net_price": net}


def receipt(identity="receipt-1", amount=100):
    return {"id": identity, "pay_date": "2026-05-01", "sum": money(amount)}


def order(*, paid=False, receipts=None, shipments=None):
    return {
        "id": "order-1", "order_num": "ORDER-1", "blocked": False,
        "status": {"id": 69, "name": "Stripe - expired"},
        "sum": money(), "shipments": [] if shipments is None else shipments,
        "invoices": [{"id": "invoice-1", "paid": paid, "sum": money(),
                      "payments": [] if receipts is None else receipts, "preinvoice": None}],
        "preinvoices": [],
    }


class MemoryAutomationStore:
    def __init__(self, orders=None):
        self.orders = copy.deepcopy(orders or {})
        self.acquired = 0
        self.owned = False

    @contextmanager
    def lease(self, *, owner):
        self.acquired += 1
        self.owned = True
        try:
            yield self
        finally:
            self.owned = False

    def assert_owned(self):
        if not self.owned:
            raise RuntimeError("lease not owned")

    def get_order(self, number):
        return copy.deepcopy(self.orders.get(number, {}))

    def update_order(self, number, **fields):
        self.assert_owned()
        self.orders.setdefault(number, {}).update(copy.deepcopy(fields))


class PaymentEvidenceTests(unittest.TestCase):
    def test_explicit_null_collections_are_empty_but_missing_keys_are_unknown(self):
        value = order()
        value.update(invoices=None, preinvoices=None, shipments=None)
        self.assertEqual("unpaid", assess_payment_evidence(value).state)
        self.assertEqual("none", assess_fulfillment_evidence(value).state)
        value.pop("preinvoices")
        self.assertEqual("unknown", assess_payment_evidence(value).state)
        value.pop("shipments")
        self.assertEqual("unknown", assess_fulfillment_evidence(value).state)
        value = order(paid=True)
        value["invoices"][0]["payments"] = None
        self.assertEqual("confirmed", assess_payment_evidence(value).state)

    def test_invoice_existence_and_bank_payment_method_are_not_settlement(self):
        value = order()
        value["price_elements"] = [{"type": "payment", "reference_id": "6", "title": "Bank transfer"}]
        self.assertEqual("unknown", assess_payment_evidence(value).state)

    def test_full_invoice_marked_paid_without_receipt_can_confirm_payment(self):
        self.assertEqual("confirmed", assess_payment_evidence(order(paid=True)).state)

    def test_receipts_allow_verified_bank_payment_with_paid_false(self):
        result = assess_payment_evidence(order(receipts=[receipt()]))
        self.assertEqual(("confirmed", "full_receipt_settlement"), (result.state, result.reason))

    def test_partial_paid_invoice_does_not_cover_whole_order(self):
        value = order(paid=True)
        value["invoices"][0]["sum"] = money(10)
        self.assertNotEqual("confirmed", assess_payment_evidence(value).state)

    def test_shared_receipt_is_deduplicated_across_invoice_and_preinvoice(self):
        value = order(receipts=[receipt(amount=60)])
        value["preinvoices"] = [{"id": "pre-1", "payments": [receipt(amount=60)]}]
        value["invoices"][0]["preinvoice"] = copy.deepcopy(value["preinvoices"][0])
        self.assertEqual("partial", assess_payment_evidence(value).state)

    def test_conflicting_duplicate_receipt_is_unknown(self):
        value = order(receipts=[receipt(amount=60)])
        value["preinvoices"] = [{"id": "pre-1", "payments": [receipt(amount=100)]}]
        self.assertEqual("conflicting_duplicate_receipt", assess_payment_evidence(value).reason)

    def test_negative_receipt_requires_review_even_if_invoice_says_paid(self):
        value = order(paid=True, receipts=[receipt(amount=150), receipt("refund", -10)])
        self.assertEqual("payment_reversal_requires_review", assess_payment_evidence(value).reason)

    def test_currency_tax_basis_missing_fields_and_nonfinite_values_are_rejected(self):
        for mutation in (
            lambda x: x["invoices"][0]["sum"].update(currency={"code": "USD"}),
            lambda x: x["invoices"][0]["sum"].update(is_net_price=True),
            lambda x: x["sum"].pop("is_net_price"),
            lambda x: x["sum"].update(value=float("nan")),
            lambda x: x["sum"].update(value=float("inf")),
            lambda x: x["sum"].update(value=True),
            lambda x: x.pop("preinvoices"),
            lambda x: x["invoices"][0].pop("payments"),
        ):
            value = order(paid=True)
            mutation(value)
            self.assertNotEqual("confirmed", assess_payment_evidence(value).state)

    def test_partial_receipts_override_stale_paid_flag(self):
        self.assertEqual("partial", assess_payment_evidence(order(paid=True, receipts=[receipt(amount=40)])).state)


class RecoveryPolicyTests(unittest.TestCase):
    def decide(self, value, **kwargs):
        return decide_recovery(value, ["Stripe - expired"], creditnote_status="clear", **kwargs)

    def test_empty_shipments_cannot_prove_no_historical_dispatch(self):
        self.assertEqual("shipment_history_unavailable", self.decide(order(paid=True)).reason)

    def test_tracking_and_carrier_registration_cannot_prove_dispatch(self):
        value = order(paid=True, shipments=[{"shipment_number": "TRACK-1", "status": "carrier_registered"}])
        self.assertEqual("review", self.decide(value).action)

    def test_carrier_placeholder_without_status_or_number_is_not_a_report(self):
        value = order(shipments=[{"carrier": "test", "status": None, "shipment_number": None}])
        self.assertEqual("none", assess_fulfillment_evidence(value).state)

    def test_delivered_evidence_preserves_fulfillment(self):
        value = order(paid=True, shipments=[{"shipment_number": "TRACK-1", "status": "delivered"}])
        result = self.decide(value)
        self.assertEqual(("shipped", "Odoslaná"), (result.action, result.target_status_name))

    def test_private_prior_shipped_evidence_allows_restoration_but_paid_does_not(self):
        value = order(receipts=[receipt()])
        self.assertEqual("shipped", self.decide(value, verified_previous_status="Odoslaná").action)
        self.assertEqual("review", self.decide(value, verified_previous_status="Platba online - zaplatené").action)

    def test_returned_shipment_blocks_even_verified_prior_shipped(self):
        value = order(paid=True, shipments=[{"shipment_number": "TRACK-1", "status": "returned"}])
        self.assertEqual("review", self.decide(value, verified_previous_status="Odoslaná").action)

    def test_later_returned_shipment_cannot_hide_behind_earlier_pending_shipment(self):
        value = order(paid=True, shipments=[{"shipment_number": "A", "status": "pickup_order"},
                                          {"shipment_number": "B", "status": "returned"}])
        self.assertEqual("exception", assess_fulfillment_evidence(value).state)
        self.assertEqual("review", self.decide(value, verified_previous_status="Odoslaná").action)

    def test_prior_dispatch_cannot_override_missing_or_malformed_current_shipment_context(self):
        value = order(paid=True)
        value.pop("shipments")
        self.assertEqual("review", self.decide(value, verified_previous_status="Odoslaná").action)
        value["shipments"] = [{}]
        self.assertEqual("review", self.decide(value, verified_previous_status="Odoslaná").action)

    def test_all_reported_shipments_must_be_delivered(self):
        value = order(paid=True, shipments=[{"shipment_number": "A", "status": "delivered"},
                                          {"shipment_number": "B", "status": "inventory"}])
        self.assertEqual("unknown", assess_fulfillment_evidence(value).state)

    def test_blocked_missing_block_flag_and_creditnotes_prevent_recovery(self):
        for blocked in (True, None):
            value = order(paid=True)
            value["blocked"] = blocked
            self.assertEqual("review", self.decide(value).action)
        for context in ("present", "unknown"):
            result = decide_recovery(order(paid=True), ["Stripe - expired"], creditnote_status=context)
            self.assertEqual("review", result.action)

    def test_terminal_status_is_protected_even_if_misconfigured_as_source(self):
        for status in ("Odoslaná", "Storno", "Vrátené", "Stripe - refunded"):
            value = order(paid=True)
            value["status"]["name"] = status
            self.assertEqual("skip", decide_recovery(value, [status], creditnote_status="clear").action)


class CreditnoteCoverageTests(unittest.TestCase):
    def test_verified_recovery_allows_later_proven_full_creditnote_only(self):
        for previous_target in ("Odoslaná", "Platba online - zaplatené"):
            prior = {"status_mutation": {"state": "verified", "target_status_name": previous_target}}
            self.assertEqual("repeated_status_regression", status_write_block_reason(prior))
            self.assertEqual("", status_write_block_reason(prior, next_reason="full_creditnote", next_target_status_name="Storno"))
            self.assertEqual("repeated_status_regression", status_write_block_reason(prior, next_reason="partial_creditnote", next_target_status_name="Storno"))

    def test_unresolved_writes_and_repeated_terminal_transitions_always_block(self):
        for state in ("pending", "uncertain", "invalid"):
            prior = {"status_mutation": {"state": state, "target_status_name": "Odoslaná"}}
            self.assertEqual("previous_status_mutation_unresolved", status_write_block_reason(prior, next_reason="full_creditnote", next_target_status_name="Storno"))
        prior = {"status_mutation": {"state": "verified", "target_status_name": "Storno"}}
        self.assertEqual("repeated_status_regression", status_write_block_reason(prior, next_reason="full_creditnote", next_target_status_name="Storno"))

    def test_only_exact_full_credit_permits_whole_order_cancellation(self):
        value = order()
        document = {"id": "credit-1", "invoice_id": "invoice-1", "amount": 100,
                    "net_amount": 100, "currency": "EUR"}
        self.assertEqual("full_creditnote", creditnote_coverage_reason(value, [document]))
        self.assertEqual("partial_creditnote", creditnote_coverage_reason(value, [{**document, "amount": 10}]))
        self.assertEqual("creditnote_exceeds_current_order", creditnote_coverage_reason(value, [{**document, "amount": 110}]))

    def test_duplicate_wrong_invoice_currency_and_missing_amount_are_rejected(self):
        document = {"id": "credit-1", "invoice_id": "invoice-1", "amount": 100, "currency": "EUR"}
        for docs in (
            [document, document], [{**document, "invoice_id": "unrelated"}],
            [{**document, "currency": "USD"}], [{**document, "amount": None}],
            [{**document, "amount": float("nan")}],
        ):
            self.assertNotEqual("full_creditnote", creditnote_coverage_reason(order(), docs))

    def test_split_partial_creditnotes_can_cover_whole_invoice_without_double_counting(self):
        docs = [{"id": "a", "invoice_id": "invoice-1", "amount": 40, "currency": "EUR"},
                {"id": "b", "invoice_id": "invoice-1", "amount": 60, "currency": "EUR"}]
        self.assertEqual("full_creditnote", creditnote_coverage_reason(order(), docs))

    def test_net_order_requires_net_credit_amount(self):
        value = order()
        value["sum"]["is_net_price"] = True
        document = {"id": "credit-1", "invoice_id": "invoice-1", "amount": 123, "currency": "EUR"}
        self.assertEqual("creditnote_amount_unknown", creditnote_coverage_reason(value, [document]))
        self.assertEqual("full_creditnote", creditnote_coverage_reason(value, [{**document, "net_amount": 100}]))


class FakeClient:
    def __init__(self, *, returned=None, readback=None, retries=0, error=None):
        self.transport = SimpleNamespace(retries=retries)
        self.calls = []
        self.returned = returned or {"order_num": "ORDER-1", "status": {"id": 4, "name": "Odoslaná"}}
        self.readback = self.returned if readback is None else readback
        self.error = error

    def execute(self, query, variable_values=None):
        self.calls.append(variable_values)
        if "status_id" in variable_values:
            if self.error:
                raise self.error
            return {"changeOrderStatus": self.returned}
        return {"getOrder": self.readback}


class VerifiedMutationTests(unittest.TestCase):
    def test_silent_mutation_is_single_attempt_and_independently_read_back(self):
        client = FakeClient()
        change_status_verified(client, "ORDER-1", 4, "Odoslaná", silent=True)
        self.assertEqual(2, len(client.calls))
        self.assertEqual([{"type": "EMAIL_CUSTOMER", "if": ["NONE"]}], client.calls[0]["send_notification"])

    def test_retrying_transport_is_rejected_before_any_request(self):
        client = FakeClient(retries=3)
        with self.assertRaisesRegex(RuntimeError, "no-retry"):
            change_status_verified(client, "ORDER-1", 4, "Odoslaná")
        self.assertEqual([], client.calls)

    def test_ambiguous_mutation_is_never_replayed(self):
        client = FakeClient(error=TimeoutError("ambiguous"), readback={"order_num": "ORDER-1", "status": {"id": 69, "name": "Stripe - expired"}})
        with self.assertRaises(RuntimeError):
            change_status_verified(client, "ORDER-1", 4, "Odoslaná")
        self.assertEqual(2, len(client.calls))
        self.assertEqual(1, sum("status_id" in call for call in client.calls))

    def test_ambiguous_mutation_with_verified_target_needs_no_replay(self):
        client = FakeClient(error=TimeoutError("ambiguous"))
        result = change_status_verified(client, "ORDER-1", 4, "Odoslaná")
        self.assertEqual(4, result["status"]["id"])
        self.assertEqual(1, sum("status_id" in call for call in client.calls))

    def test_wrong_identity_target_or_readback_is_not_success(self):
        for result in (
            {"order_num": "OTHER", "status": {"id": 4, "name": "Odoslaná"}},
            {"order_num": "ORDER-1", "status": {"id": 67, "name": "Odoslaná"}},
            {"order_num": "ORDER-1", "status": {"id": 4, "name": "Storno"}},
        ):
            for client in (FakeClient(returned=result), FakeClient(readback=result)):
                with self.assertRaises(RuntimeError):
                    change_status_verified(client, "ORDER-1", 4, "Odoslaná")

    def test_missing_mutation_response_is_not_success(self):
        client = FakeClient()
        client.returned = {}
        with self.assertRaises(RuntimeError):
            change_status_verified(client, "ORDER-1", 4, "Odoslaná")

    def test_read_rejects_mutation_and_wrong_order(self):
        client = FakeClient(readback={"order_num": "OTHER"})
        with self.assertRaises(ValueError):
            execute_read(client, gql("mutation { x }"), variable_values={})
        with self.assertRaises(RuntimeError):
            fetch_order_safety_context(client, "ORDER-1")

    def test_read_retries_transient_error_but_not_partial_graphql_data(self):
        class ApiError(Exception):
            code = 429
        client = FakeClient()
        with patch.object(client, "execute", side_effect=[ApiError(), {"getOrder": {"order_num": "ORDER-1"}}]) as request, patch("order_status_safety.time.sleep"):
            self.assertEqual("ORDER-1", fetch_order_safety_context(client, "ORDER-1")["order_num"])
        self.assertEqual(2, request.call_count)
        with patch.object(client, "execute", side_effect=ValueError("GraphQL partial response")) as request:
            with self.assertRaises(ValueError):
                execute_read(client, ORDER_SAFETY_QUERY, variable_values={"order_num": "ORDER-1"})
        self.assertEqual(1, request.call_count)


if __name__ == "__main__":
    unittest.main()
