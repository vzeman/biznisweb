"""Independent exact-target regression: provider calls are synthetic only."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import generate_invoices as invoices
import order_status_identity as identity
import order_status_safety as safety
import unpaid_order_cancellation as cancellation


PAID = "Platba online - zaplaten\u00e9"


class DuplicatePaidClient:
    def __init__(self):
        self.transport = SimpleNamespace(url="https://roy.flox.sk/api/graphql", retries=0, close=Mock())
        self.rows = [{"id": "31", "name": PAID}, {"id": "67", "name": PAID}]
        self.order = {"id": "101", "order_num": "900001", "status": {"id": "33", "name": "Pending"}}
        self.calls, self.writes = [], []
        self.wrong_ack = False
        self.wrong_commit = False
        self.lost_response = False
        self.no_commit = False
        self.after_write = lambda *_: None

    def execute(self, query, variable_values=None):
        values = deepcopy(variable_values or {})
        self.calls.append(values)
        if "lang_code" in values:
            return {"listOrderStatuses": deepcopy(self.rows)}
        if "status_id" in values:
            self.writes.append(values)
            if not self.no_commit:
                target = "31" if self.wrong_commit else str(values["status_id"])
                self.order["status"] = deepcopy(next(row for row in self.rows if str(row["id"]) == target))
            self.after_write(self)
            if self.lost_response:
                raise TimeoutError("synthetic lost status acknowledgement")
            changed = deepcopy(self.order)
            if self.wrong_ack:
                changed["status"] = {"id": "31", "name": PAID}
            return {"changeOrderStatus": changed}
        return {"getOrder": deepcopy(self.order)}


class ExplicitTargetIntegrationTests(unittest.TestCase):
    def setUp(self):
        # Use the settings belonging to the imported implementation so this test
        # can independently review an isolated candidate source checkout.
        root = Path(invoices.__file__).resolve().parent
        self.settings = json.loads((root / "projects/roy/settings.json").read_text(encoding="utf-8"))
        self.client = DuplicatePaidClient()
        identity.bind_status_identity(self.client, "roy", self.settings)

    def invoice_target(self):
        settings = invoices.resolve_invoice_generation_settings(self.settings)["existing_invoice_status_reconciliation"]
        return invoices._resolve_existing_invoice_target_status_id(self.client, settings)

    def cancellation_target(self):
        settings = cancellation.resolve_unpaid_cancellation_settings(self.settings)
        return cancellation.resolve_recovery_target_status_id(self.client, settings)

    def write_target(self, target=67):
        return safety.change_status_verified(self.client, "900001", target, PAID, silent=True)

    def assert_one_silent_67(self):
        self.assertEqual(1, len(self.client.writes))
        self.assertEqual(67, self.client.writes[0]["status_id"])
        self.assertEqual([{"type": "EMAIL_CUSTOMER", "if": ["NONE"]}], self.client.writes[0]["send_notification"])

    def enable_synthetic_refresh_policy(self):
        # Only this test's synthetic shop has a renamed unrelated shipped role;
        # actual ROY labels are not claimed to be renamed by this fixture.
        self.enterContext(patch.object(identity, "REVIEWED_RENAMES", {"roy": {
            "4": ("Odoslan\u00e1", ("Odoslan\u00e1", "Shipped")),
        }}))
        self.client.rows.append({"id": "4", "name": "Shipped"})

    def test_both_real_resolvers_write_only_configured_67_and_read_it_back(self):
        for resolver in (self.invoice_target, self.cancellation_target):
            with self.subTest(resolver=resolver.__name__):
                self.client.calls.clear()
                self.client.writes.clear()
                self.client.rows.reverse()
                selected = resolver()
                self.assertEqual(67, selected)
                self.assertEqual({"id": "67", "name": PAID}, self.write_target(selected)["status"])
                self.assert_one_silent_67()
                self.assertIn({"order_num": "900001"}, self.client.calls)

    def test_configured_id_with_wrong_label_does_not_fall_back_to_other_paid_id(self):
        self.client.rows[1]["name"] = "Stripe - unpaid"
        for resolver in (self.invoice_target, self.cancellation_target):
            with self.subTest(resolver=resolver.__name__), self.assertRaises(ValueError):
                resolver()
        self.assertEqual([], self.client.writes)

    def test_no_configured_id_remains_ambiguous_in_both_resolvers(self):
        invoice = invoices.resolve_invoice_generation_settings(self.settings)["existing_invoice_status_reconciliation"]
        invoice = {**invoice, "target_status_id": None}
        cancelled = replace(cancellation.resolve_unpaid_cancellation_settings(self.settings), recovery_target_status_id=None)
        with self.assertRaises(ValueError):
            invoices._resolve_existing_invoice_target_status_id(self.client, invoice)
        with self.assertRaises(ValueError):
            cancellation.resolve_recovery_target_status_id(self.client, cancelled)
        self.assertEqual([], self.client.writes)

    def test_same_label_wrong_31_acknowledgement_is_unresolved_without_replay(self):
        self.assertEqual(67, self.invoice_target())
        self.client.wrong_ack = True
        with self.assertRaises(RuntimeError):
            self.write_target()
        self.assert_one_silent_67()

    def test_lost_response_resolves_exact_67_readback_with_one_request(self):
        self.assertEqual(67, self.invoice_target())
        self.client.lost_response = True
        self.assertEqual("67", self.write_target()["status"]["id"])
        self.assert_one_silent_67()

    def test_lost_response_at_same_label_wrong_31_or_uncommitted_source_cannot_resolve(self):
        for no_commit in (False, True):
            with self.subTest(no_commit=no_commit):
                self.client.writes.clear()
                self.client.order["status"] = {"id": "33", "name": "Pending"}
                self.client.lost_response = True
                self.client.wrong_commit = not no_commit
                self.client.no_commit = no_commit
                with self.assertRaises(RuntimeError):
                    self.write_target()
                self.assert_one_silent_67()

    def test_inner_catalogue_refresh_selects_exact_67_before_and_after_one_write(self):
        self.enable_synthetic_refresh_policy()
        result = self.write_target()
        self.assertEqual("67", result["status"]["id"])
        self.assert_one_silent_67()
        self.assertIn("lang_code", self.client.calls[0])
        self.assertIn("status_id", self.client.calls[1])
        self.assertIn("lang_code", self.client.calls[2])
        self.assertEqual({"order_num": "900001"}, self.client.calls[3])

    def test_inner_fresh_catalogue_label_drift_rejects_before_request(self):
        self.enable_synthetic_refresh_policy()
        self.assertEqual(67, self.invoice_target())
        self.client.rows[1]["name"] = "Stripe - unpaid"
        with self.assertRaises(ValueError):
            self.write_target()
        self.assertEqual([], self.client.writes)

    def test_catalogue_becomes_invalid_after_request_is_unresolved_without_replay(self):
        self.enable_synthetic_refresh_policy()
        self.client.after_write = lambda client: client.rows.append(deepcopy(client.rows[1]))
        with self.assertRaises(ValueError):
            self.write_target()
        self.assert_one_silent_67()
        self.assertFalse(identity.identity_for(self.client).catalogue)
