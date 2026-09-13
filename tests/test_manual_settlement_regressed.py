"""Record a confirmed payment first; the existing guarded runtime repairs later."""
from copy import deepcopy
from types import SimpleNamespace
import unittest

from graphql import print_ast

from generate_invoices import reconcile_existing_invoice_statuses
from order_status_identity import bind_status_identity, canonical_order
from scripts.record_verified_manual_settlement import record_proof
from tests.test_manual_settlement import current_order, reference_for, store_for


class SettlementClient:
    def __init__(self):
        self.transport = SimpleNamespace(url="https://vevo.flox.sk/api/graphql", retries=0)
        self.order = current_order()
        self.order["status"] = {"id": "33", "name": "Payment online - expired"}
        self.catalogue = [
            {"id": "4", "name": "Shipped"}, {"id": "17", "name": "Cancelled"},
            {"id": "31", "name": "Payment online - paid"},
            {"id": "33", "name": "Payment online - expired"},
            {"id": "34", "name": "Payment online - expired"},
        ]
        self.calls, self.mutations = [], []
        self.allow_status_mutation = False
        bind_status_identity(self, "vevo")

    def execute(self, query, variable_values=None):
        text = print_ast(getattr(query, "document", query))
        variables = variable_values or {}
        self.calls.append((text, deepcopy(variables)))
        if text.lstrip().startswith("mutation"):
            if not self.allow_status_mutation or "status_id" not in variables:
                raise AssertionError("Recorder must never issue provider mutations")
            self.mutations.append(deepcopy(variables))
            self.order["status"] = deepcopy(next(row for row in self.catalogue
                                                  if int(row["id"]) == variables["status_id"]))
            self.order["last_change"] = "2026-05-02 10:00:00"
            return {"changeOrderStatus": deepcopy(self.order)}
        if "lang_code" in variables:
            return {"listOrderStatuses": deepcopy(self.catalogue)}
        if set(variables) != {"order_num"}:
            raise AssertionError("Unexpected provider read")
        return {"getOrder": deepcopy(self.order)}


class RegressedSettlementPipelineTests(unittest.TestCase):
    def prepare(self):
        client = SettlementClient()
        reference = reference_for(client.order)
        previous = {"order_num": client.order["order_num"], "phase": "complete", "email_state": "sent",
                    "invoice_id": "historical", "attempted_at": "2026-04-01T00:00:00Z",
                    "status_review": {"state": "open", "reason": "no_settlement_evidence"}}
        store = store_for({client.order["order_num"]: previous})
        return client, reference, previous, store

    def record(self, client, reference, store, apply):
        def forbidden(*args, **kwargs):
            raise AssertionError("Optional mode must freshly read its own catalogue and detail")
        return record_proof(store=store, reference=reference, project="vevo", apply=apply,
                            read_order=forbidden, expected_current_negative_status_id="33", status_client=client)

    def test_preview_and_apply_leave_provider_unchanged_then_existing_runtime_recovers_once(self):
        client, reference, previous, store = self.prepare()
        before, state = deepcopy(client.order), store.read()
        self.assertTrue(self.record(client, reference, store, False)["ok"])
        self.assertEqual(state, store.read())
        self.assertEqual(4, len(client.calls))
        self.assertEqual(0, self.record(client, reference, store, True)["provider_writes"])
        self.assertEqual(before, client.order)
        self.assertEqual([], client.mutations)
        self.assertEqual(8, len(client.calls))
        recorded = store.read()[0]["orders"][client.order["order_num"]]
        self.assertEqual(previous, {key: recorded[key] for key in previous})
        self.assertEqual(reference["proof"], recorded["manual_settlement"]["proof"])
        # Only the already deployed recovery consumer may write after its own
        # fresh checks and explicit complete native-creditnote context.
        client.allow_status_mutation = True
        settings = {"enabled": True, "target_status_name": "Platba online - zaplatené", "target_status_id": 31}
        with store.lease(owner="existing-runtime") as journal:
            result = reconcile_existing_invoice_statuses(
                client, [canonical_order(client, deepcopy(client.order))], settings,
                project="vevo", journal=journal, dry_run=False, creditnote_order_numbers=set())
        self.assertEqual((1, 0), (result["reconciled"], result["failed"]))
        self.assertEqual(1, len(client.mutations))
        self.assertEqual(4, client.mutations[0]["status_id"])
        self.assertEqual([{"type": "EMAIL_CUSTOMER", "if": ["NONE"]}], client.mutations[0]["send_notification"])
        with store.lease(owner="existing-runtime-second") as journal:
            result = reconcile_existing_invoice_statuses(
                client, [canonical_order(client, deepcopy(client.order))], settings,
                project="vevo", journal=journal, dry_run=False, creditnote_order_numbers=set())
        self.assertEqual(0, result["reconciled"])
        self.assertEqual(1, len(client.mutations))

    def test_recorded_fact_cannot_override_unknown_or_present_creditnotes_in_runtime(self):
        for creditnotes in (None, {"900001"}):
            with self.subTest(creditnotes=creditnotes):
                client, reference, _, store = self.prepare()
                self.record(client, reference, store, True)
                with store.lease(owner="existing-runtime") as journal:
                    result = reconcile_existing_invoice_statuses(
                        client, [canonical_order(client, deepcopy(client.order))], {"enabled": True},
                        project="vevo", journal=journal, dry_run=False, creditnote_order_numbers=creditnotes)
                self.assertEqual(0, result["reconciled"])
                self.assertEqual(1, result["review_required"])
                self.assertEqual([], client.mutations)


if __name__ == "__main__":
    unittest.main()
