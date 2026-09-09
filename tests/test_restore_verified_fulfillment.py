from copy import deepcopy
from datetime import datetime, timedelta, timezone
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from invoice_automation_state import S3AutomationStateStore
from scripts.restore_verified_fulfillment import (
    RestorationBlocked, restore_one, runtime_environment, validate_evidence,
)
from tests.test_invoice_automation_state import MemoryS3
from tests.test_order_status_safety import order


class OrderClient:
    def __init__(self):
        self.order = order(paid=True)
        self.statuses = [{"id": 10, "name": "Odoslaná"}, {"id": 69, "name": "Stripe - expired"}]
        self.transport = SimpleNamespace(retries=0)
        self.mutations = []
        self.reads = 0
        self.before_read = None
        self.failure = ""

    def execute(self, query, *, variable_values):
        document = getattr(query, "document", query)
        name = document.definitions[0].name.value
        if name == "ListOrderStatuses":
            return {"listOrderStatuses": deepcopy(self.statuses)}
        if name == "GetOrderAutomationSafetyContext":
            self.reads += 1
            if self.before_read:
                self.before_read(self)
            return {"getOrder": deepcopy(self.order)}
        if name != "ChangeOrderStatusSafely":
            raise AssertionError("Unexpected operation")
        self.mutations.append(deepcopy(variable_values))
        if self.failure == "uncommitted_timeout":
            raise TimeoutError("Synthetic request outcome unknown")
        self.order["status"] = {"id": variable_values["status_id"], "name": "Odoslaná"}
        if self.failure == "committed_timeout":
            raise TimeoutError("Synthetic committed response timeout")
        return {"changeOrderStatus": deepcopy(self.order)}


class VerifiedFulfillmentRestorationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.s3 = MemoryS3()
        self.store = S3AutomationStateStore(self.s3, "private", "data/roy/order-automation/state.json", "roy", now=lambda: self.now)
        self.client = OrderClient()
        self.evidence = {"project": "roy", "order_number": "ORDER-1", "previous_status": "Odoslaná",
                         "observed_at": self.now.isoformat(), "source": "authenticated_admin_order_history"}
        self.settings = {"unpaid_order_cancellation": {
            "recovery_target_status_name": "Platba online - zaplatené", "recovery_source_statuses": ["Stripe - expired"],
        }}
        self.creditnote_calls = []

    def creditnotes(self, *, progress_callback):
        self.creditnote_calls.append(progress_callback)
        if progress_callback:
            progress_callback()
        return {}

    def restore(self, **kwargs):
        values = dict(client=self.client, store=self.store, project_settings=self.settings, project="roy",
                      order_number="ORDER-1", expected_status_id=69, evidence=self.evidence,
                      creditnote_loader=self.creditnotes, now=self.now)
        values.update(kwargs)
        return restore_one(**values)

    def test_dry_run_reads_evidence_without_journal_or_status_writes(self):
        result = self.restore()
        self.assertEqual(result, {"project": "roy", "dry_run": True, "eligible_orders": 1, "restored_orders": 0})
        self.assertEqual([], self.s3.writes)
        self.assertEqual([], self.client.mutations)
        self.assertEqual([None], self.creditnote_calls)

    def test_silent_single_write_has_durable_intent_and_verified_readback(self):
        before = self.client.execute
        def execute(query, *, variable_values):
            if "status_id" in variable_values:
                state = self.store.read()[0]
                self.assertTrue(state["lease"])
                self.assertEqual("pending", state["orders"]["ORDER-1"]["status_mutation"]["state"])
            return before(query, variable_values=variable_values)
        self.client.execute = execute
        result = self.restore(apply=True)
        self.assertEqual(1, result["restored_orders"])
        self.assertEqual(1, len(self.client.mutations))
        self.assertEqual([{"type": "EMAIL_CUSTOMER", "if": ["NONE"]}], self.client.mutations[0]["send_notification"])
        self.assertGreaterEqual(self.client.reads, 3)
        state = self.store.read()[0]
        self.assertIsNone(state["lease"])
        record = state["orders"]["ORDER-1"]
        self.assertEqual("verified", record["status_mutation"]["state"])
        self.assertEqual("Odoslaná", record["verified_fulfillment_status"])
        self.assertEqual(self.evidence, record["status_mutation"]["historical_evidence"])
        self.assertTrue(callable(self.creditnote_calls[0]))

    def test_committed_timeout_resolves_by_read_without_write_retry(self):
        self.client.failure = "committed_timeout"
        self.assertEqual(1, self.restore(apply=True)["restored_orders"])
        self.assertEqual(1, len(self.client.mutations))

    def test_unverified_timeout_is_sticky_and_blocks_second_attempt(self):
        self.client.failure = "uncommitted_timeout"
        with self.assertRaises(RestorationBlocked):
            self.restore(apply=True)
        state = self.store.read()[0]
        self.assertEqual("uncertain", state["orders"]["ORDER-1"]["status_mutation"]["state"])
        self.assertIsNone(state["lease"])
        with self.assertRaises(RestorationBlocked):
            self.restore(apply=True)
        self.assertEqual(1, len(self.client.mutations))

    def test_native_status_change_at_final_read_prevents_mutation(self):
        def change(client):
            if client.reads == 2:
                client.order["status"] = {"id": 80, "name": "Storno"}
        self.client.before_read = change
        with self.assertRaises(RestorationBlocked):
            self.restore(apply=True)
        self.assertEqual([], self.client.mutations)
        self.assertEqual({}, self.store.read()[0]["orders"])

    def test_any_existing_lease_or_uncertain_document_prevents_work(self):
        for field in ({"lease": {"expires_at": "2020-01-01T00:00:00Z"}},
                      {"orders": {"ORDER-1": {"phase": "create_ambiguous"}}},
                      {"orders": {"ORDER-1": {"email_state": "sending"}}},
                      {"orders": {"ORDER-1": {"status_mutation": {"state": "verified"}}}}):
            self.setUp()
            self.store.put({**self.store.empty_state(), **field}, "")
            writes = len(self.s3.writes)
            with self.assertRaises(RestorationBlocked):
                self.restore(apply=True)
            self.assertEqual(writes, len(self.s3.writes))
            self.assertEqual([], self.client.mutations)

    def test_incomplete_or_present_creditnote_context_prevents_mutation(self):
        for context in (None, [], {"ORDER-1": []}, {"ORDER-1": [{"id": "CREDIT-1"}]}):
            with self.assertRaises(RestorationBlocked):
                self.restore(apply=True, creditnote_loader=lambda **kwargs: context)
        self.assertEqual([], self.client.mutations)
        def failed_scan(**kwargs):
            raise RuntimeError("Synthetic incomplete scan")
        with self.assertRaises(RuntimeError):
            self.restore(apply=True, creditnote_loader=failed_scan)
        self.assertIsNone(self.store.read()[0]["lease"])

    def test_unpaid_partial_payment_blocked_or_returned_order_never_restores(self):
        for change in (
            lambda row: row["invoices"][0].update(paid=False),
            lambda row: row["invoices"][0]["sum"].update(value=50),
            lambda row: row.update(blocked=True),
            lambda row: row.update(shipments=[{"status": "returned", "shipment_number": "TRACK-FIXTURE"}]),
            lambda row: row.pop("shipments"),
        ):
            self.client.order = order(paid=True)
            change(self.client.order)
            with self.assertRaises(RestorationBlocked):
                self.restore(apply=True)
        self.assertEqual([], self.client.mutations)

    def test_configured_cancel_refund_source_is_never_an_override(self):
        for name in ("Stripe - cancelled", "Stripe - refunded", "Storno", "Vrátené", "Nezaplatená - zrušená objednávka"):
            self.settings["unpaid_order_cancellation"]["recovery_source_statuses"] = [name]
            self.client.order["status"]["name"] = name
            with self.assertRaises(RestorationBlocked):
                self.restore(apply=True)
        self.assertEqual([], self.client.mutations)

    def test_target_identity_must_be_unique_and_exact(self):
        for statuses in ([], [{"id": 10, "name": "Odoslana"}],
                         [{"id": 10, "name": "Odoslaná"}, {"id": 11, "name": "Odoslaná"}],
                         [{"id": 10, "name": "Odoslaná"}, {"id": 10, "name": "Other"}]):
            self.client.statuses = statuses
            with self.assertRaises(RestorationBlocked):
                self.restore(apply=True)
        self.assertEqual([], self.client.mutations)

    def test_historical_evidence_requires_identity_source_freshness_and_timezone(self):
        for replacement in ({"project": "vevo"}, {"order_number": "OTHER"}, {"previous_status": "Paid"},
                            {"source": "customer_note"}, {"observed_at": "2026-01-01"},
                            {"observed_at": (self.now - timedelta(hours=7)).isoformat()},
                            {"observed_at": (self.now + timedelta(seconds=1)).isoformat()}):
            with self.assertRaises(RestorationBlocked):
                validate_evidence({**self.evidence, **replacement}, "roy", "ORDER-1", self.now)
        self.assertEqual(self.evidence, validate_evidence({**self.evidence, "customer": "omit"}, "roy", "ORDER-1", self.now))

    def test_credential_scope_overrides_stale_local_env_and_restores_afterwards(self):
        secret = {"BIZNISWEB_API_URL": "https://roy.flox.sk/api/graphql", "BIZNISWEB_API_TOKEN": "fixture-token",
                  "BIZNISWEB_USERNAME": "fixture-user", "BIZNISWEB_PASSWORD": "fixture-password"}
        settings = {"biznisweb_api_url": "https://roy.flox.sk/api/graphql"}
        with patch.dict(os.environ, {"ROY_BIZNISWEB_API_TOKEN": "stale", "REPORT_SKIP_PROJECT_ENV": "false"}):
            with runtime_environment("roy", secret, settings):
                self.assertEqual("fixture-token", os.environ["ROY_BIZNISWEB_API_TOKEN"])
                self.assertEqual("true", os.environ["REPORT_SKIP_PROJECT_ENV"])
            self.assertEqual("stale", os.environ["ROY_BIZNISWEB_API_TOKEN"])
            self.assertEqual("false", os.environ["REPORT_SKIP_PROJECT_ENV"])
        for url in ("https://vevo.flox.sk/api/graphql", "http://roy.flox.sk/api/graphql", "https://roy.flox.sk:8080/api/graphql"):
            with self.assertRaises(RestorationBlocked):
                with runtime_environment("roy", {**secret, "BIZNISWEB_API_URL": url}, settings):
                    self.fail("Unexpected credentials accepted")


if __name__ == "__main__":
    unittest.main()
