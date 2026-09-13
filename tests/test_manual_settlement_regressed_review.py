"""Independent journal-only checks for an explicitly selected negative status."""
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import order_status_identity as identity
from scripts import record_verified_manual_settlement as recorder
from tests import test_manual_settlement as fixtures
from tests.test_order_status_safety import receipt


class ReadOnlyStatusClient:
    def __init__(self, store):
        self.transport = SimpleNamespace(url="https://vevo.flox.sk/api/graphql", retries=0)
        self.catalogue = [{"id": key, "name": observed[-1]} for key, (_, observed)
                          in identity.REVIEWED_RENAMES["vevo"].items()]
        self.order = fixtures.current_order()
        self.order["status"] = {"id": "33", "name": "Payment online - expired"}
        self.calls = []
        self.before = lambda: None
        self.store, self.expect_lease = store, True
        identity.bind_status_identity(self, "vevo", {"biznisweb_api_url": self.transport.url})

    def execute(self, query, variable_values=None):
        assert all(node.operation.value == "query" for node in getattr(query, "document", query).definitions)
        assert bool(self.store.read()[0].get("lease")) is self.expect_lease
        self.calls.append(deepcopy(variable_values))
        self.before()
        if "lang_code" in variable_values:
            return {"listOrderStatuses": deepcopy(self.catalogue)}
        assert variable_values == {"order_num": self.order["order_num"]}
        return {"getOrder": deepcopy(self.order)}


class RegressedRecorderIndependentTests(unittest.TestCase):
    def setup_case(self, fields=None):
        original = {"order_num": "900001", "phase": "complete", "email_state": "held",
                    "email_policy": "hold", "invoice_id": "retained", "attempted_at": "retained",
                    "status_mutation": {"state": "verified", "attempt_id": "retained"},
                    "private_extension": {"keep": [1, 2]}, **(fields or {})}
        store = fixtures.store_for({"900001": original})
        client = ReadOnlyStatusClient(store)
        callback = Mock(side_effect=AssertionError("optional mode must not trust the old callback"))
        return store, client, callback, original

    def record(self, store, client, callback, *, apply=True, expected="33"):
        return recorder.record_proof(store=store, reference=fixtures.reference_for(), project="vevo",
            read_order=callback, apply=apply, expected_current_negative_status_id=expected, status_client=client)

    def test_actual_preview_and_apply_use_fresh_bound_queries_and_preserve_all_original_fields(self):
        store, client, callback, original = self.setup_case()
        client.expect_lease = False
        before = len(store.client.writes)
        self.assertEqual(0, self.record(store, client, callback, apply=False)["provider_writes"])
        self.assertEqual(before, len(store.client.writes))
        client.expect_lease = True
        for _ in range(2):
            self.assertEqual(0, self.record(store, client, callback)["provider_writes"])
        callback.assert_not_called()
        self.assertEqual([{"lang_code": "SK"}, {"order_num": "900001"}] * 6, client.calls)
        stored = store.read()[0]["orders"]["900001"]
        self.assertEqual(original, {key: stored[key] for key in original})
        self.assertEqual(fixtures.confirmed(), stored["manual_settlement"])
        self.assertEqual("33", client.order["status"]["id"])
        self.assertIsNone(store.read()[0]["lease"])

    def test_identical_visible_declined_status_and_other_source_ids_are_rejected(self):
        for current_id, expected in (("34", "33"), ("34", "34"), ("17", "17"), ("31", "31")):
            store, client, callback, original = self.setup_case()
            client.order["status"] = deepcopy(next(row for row in client.catalogue if row["id"] == current_id))
            with self.subTest(current_id=current_id, expected=expected), self.assertRaises(ValueError):
                self.record(store, client, callback, expected=expected)
            self.assertEqual(original, store.read()[0]["orders"]["900001"])

    def test_uncertain_attempts_and_closed_obligations_are_not_overwritten(self):
        for fields in ({"phase": "create_ambiguous"}, {"phase": "prepare_ambiguous"},
                       {"email_state": "ambiguous"}, {"email_state": "sending"},
                       {"status_mutation": {"state": "pending"}}, {"status_mutation": {"state": "uncertain"}},
                       {"reviewed_uncollected_closure": None}):
            store, client, callback, original = self.setup_case(fields)
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                self.record(store, client, callback)
            self.assertEqual(original, store.read()[0]["orders"]["900001"])

    def test_native_conflicts_and_observed_context_or_catalogue_drift_fail_before_recording(self):
        for kind in ("blocked", "partial", "reversal", "returned", "missing-payment", "generation", "catalogue"):
            store, client, callback, original = self.setup_case()
            if kind == "blocked":
                client.order["blocked"] = True
            elif kind in {"partial", "reversal"}:
                client.order["invoices"][0]["payments"] = [receipt(amount=10 if kind == "partial" else -100)]
            elif kind == "returned":
                client.order["shipments"] = [{"status": "returned", "shipment_number": "fixture"}]
            elif kind == "missing-payment":
                client.order.pop("invoices")
            else:
                def drift():
                    if len(client.calls) == 3:
                        if kind == "generation":
                            client.order["last_change"] = "2026-05-01 11:00:00"
                        else:
                            next(row for row in client.catalogue if row["id"] == "4")["name"] = "Unexpected rename"
                client.before = drift
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                self.record(store, client, callback)
            self.assertEqual(original, store.read()[0]["orders"]["900001"])

    def test_foreign_client_and_unchanged_default_cannot_use_the_new_exception(self):
        store, client, callback, original = self.setup_case()
        client.transport.url = "https://roy.flox.sk/api/graphql"
        with self.assertRaises(ValueError):
            self.record(store, client, callback)
        self.assertEqual([], client.calls)
        with self.assertRaises(ValueError):
            recorder.record_proof(store=store, reference=fixtures.reference_for(), project="vevo",
                                  read_order=lambda *a, **k: deepcopy(client.order), apply=True)
        self.assertEqual(original, store.read()[0]["orders"]["900001"])
