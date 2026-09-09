from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch

from generate_invoices import InvoiceCreationResult, InvoiceGenerator
from invoice_automation_state import AutomationLeaseBusy, AutomationStateError, S3AutomationStateStore
from scripts.retry_seeded_invoice import (
    SOURCE, SeededRetryBlocked, require_retry_source, retry_one_seed, safe_creation_failure,
)
from tests.test_invoice_automation_state import MemoryS3
from tests.test_invoice_generation import InvoiceApiFake, InvoiceWebFake, invoice_order


class SeededInvoiceRetryTests(unittest.TestCase):
    def setUp(self):
        self.s3 = MemoryS3()
        self.store = S3AutomationStateStore(self.s3, "private", "data/roy/order-automation/state.json", "roy",
                                            now=lambda: datetime(2026, 9, 9, tzinfo=timezone.utc))
        state = self.store.empty_state()
        # Reverse insertion order proves selection does not depend on JSON order.
        state["orders"] = {str(i): {"order_num": str(i), "source": SOURCE, "phase": "pending", "email_policy": "hold"}
                           for i in range(1005, 999, -1)}
        self.store.put(state, "")
        self.s3.writes.clear()
        self.api = InvoiceApiFake(invoice_order("1000"))
        self.web = InvoiceWebFake(self.api)
        self.web.close = Mock()
        self.generator = InvoiceGenerator("https://example.test/api/graphql", "synthetic-token", "https://example.test",
                                           page_delay_seconds=0, read_attempts=1)
        self.generator.client = self.api
        self.generator.validate_session = lambda: True
        self.factories = []
        self.create = Mock(wraps=self.generator.create_invoice)
        self.generator.create_invoice = self.create
        sleep = patch("generate_invoices.time.sleep")
        sleep.start()
        self.addCleanup(sleep.stop)

    def factory(self, *, web_login):
        self.factories.append(bool(self.store.read()[0]["lease"]))
        self.assertEqual(web_login, self.factories[-1])
        self.generator.web_session = self.web if web_login else None
        return self.generator

    def run_retry(self, **kwargs):
        values = {"store": self.store, "project": "roy", "expected_count": 6, "generator_factory": self.factory}
        values.update(kwargs)
        return retry_one_seed(**values)

    def replace_state(self, change):
        state, etag = self.store.read()
        change(state)
        self.store.put(state, etag)
        self.s3.writes.clear()

    def test_default_preview_has_no_web_session_document_or_journal_writes(self):
        result = self.run_retry()
        self.assertTrue(result["ok"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(0, result["attempted_orders"])
        self.assertEqual([False], self.factories)
        self.assertEqual([], self.s3.writes)
        self.assertEqual([], self.web.post_urls)
        self.create.assert_not_called()
        self.assertNotIn("1000", json.dumps(result))

    def test_vevo_preview_requires_its_exact_five_seed_batch(self):
        records = self.store.read()[0]["orders"]
        records.pop("1005")
        self.store = S3AutomationStateStore(self.s3, "private", "data/vevo/order-automation/state.json", "vevo")
        self.store.put({**self.store.empty_state(), "orders": records}, "")
        self.s3.writes.clear()
        result = self.run_retry(project="vevo", expected_count=5)
        self.assertTrue(result["ok"])
        self.assertEqual("vevo", result["project"])
        self.assertEqual(5, result["seeded_orders"])
        self.assertEqual([], self.s3.writes)
        self.create.assert_not_called()

    def test_apply_delegates_one_existing_creation_under_lease_and_preserves_hold(self):
        result = self.run_retry(apply=True)
        self.assertTrue(result["ok"])
        self.assertEqual(1, result["attempted_orders"])
        self.assertEqual(1, result["created_invoices"])
        self.assertEqual(0, result["emailed_invoices"])
        self.assertEqual([True], self.factories)
        self.create.assert_called_once()
        self.assertEqual("1000", self.create.call_args.args[0]["order_num"])
        state = self.store.read()[0]
        self.assertIsNone(state["lease"])
        self.assertEqual("complete", state["orders"]["1000"]["phase"])
        self.assertEqual("held", state["orders"]["1000"]["email_state"])
        self.assertTrue(all(row["email_policy"] == "hold" for row in state["orders"].values()))
        self.assertTrue(all(row["phase"] == "pending" for number, row in state["orders"].items() if number != "1000"))
        self.assertEqual([], self.api.preparation_calls)
        self.assertEqual(1, sum("/finalize/" in url for url in self.web.get_urls))
        self.assertFalse(any("sendEmail" in url for url in self.web.post_urls + getattr(self.web, "get_urls", [])))
        self.web.close.assert_called_once()
        self.assertNotIn("1000", json.dumps(result))
        self.assertNotIn("fixture-invoice", json.dumps(result))

    def test_one_definite_failure_stops_without_trying_the_next_seed(self):
        self.web.finalize_rejected = True
        result = self.run_retry(apply=True)
        self.assertFalse(result["ok"])
        self.assertEqual(1, result["failed_invoices"])
        self.assertEqual(1, result["attempted_orders"])
        self.assertEqual({"stage": "finalization", "http_status": 429, "kind": "rejected"}, result["creation_failure"])
        self.create.assert_called_once()
        state = self.store.read()[0]
        self.assertEqual("create_failed", state["orders"]["1000"]["phase"])
        self.assertTrue(all(row["phase"] == "pending" for number, row in state["orders"].items() if number != "1000"))
        self.assertIsNone(state["lease"])

    def test_missing_preinvoice_delegates_one_preparation_and_finalization_with_hold(self):
        for response_lost_after_commit in (False, True):
            with self.subTest(response_lost_after_commit=response_lost_after_commit):
                self.setUp()
                self.api.order["preinvoices"] = []
                self.api.preparation_timeout_after_commit = response_lost_after_commit
                result = self.run_retry(apply=True)
                self.assertTrue(result["ok"])
                self.create.assert_called_once()
                self.assertEqual(1, len(self.api.preparation_calls))
                self.assertEqual(1, sum("/finalize/" in url for url in self.web.get_urls))
                self.assertFalse(any("sendEmail" in url for url in self.web.get_urls + self.web.post_urls))
                record = self.store.read()[0]["orders"]["1000"]
                self.assertEqual("complete", record["phase"])
                self.assertEqual("held", record["email_state"])

    def test_unconfirmed_preparation_stops_without_finalizing_or_replaying(self):
        self.api.order["preinvoices"] = []
        self.api.preparation_timeout = True
        result = self.run_retry(apply=True)
        self.assertFalse(result["ok"])
        self.assertEqual(1, result["ambiguous_operations"])
        self.assertEqual({"stage": "preparation", "http_status": None, "kind": "unconfirmed"},
                         result["creation_failure"])
        self.assertEqual("prepare_ambiguous", self.store.read()[0]["orders"]["1000"]["phase"])
        self.assertEqual(1, len(self.api.preparation_calls))
        self.assertEqual([], self.web.get_urls)
        writes = len(self.s3.writes)
        with self.assertRaisesRegex(SeededRetryBlocked, "requires review"):
            self.run_retry(apply=True)
        self.assertEqual(writes, len(self.s3.writes))
        self.assertEqual(1, len(self.api.preparation_calls))
        self.create.assert_called_once()

    def test_unconfirmed_readback_is_sticky_and_blocks_a_second_attempt(self):
        with patch.object(self.generator, "_confirm_created_invoice", return_value=(None, None)):
            result = self.run_retry(apply=True)
        self.assertFalse(result["ok"])
        self.assertEqual(1, result["ambiguous_operations"])
        self.assertEqual("create_ambiguous", self.store.read()[0]["orders"]["1000"]["phase"])
        writes = len(self.s3.writes)
        with self.assertRaisesRegex(SeededRetryBlocked, "requires review"):
            self.run_retry(apply=True)
        self.assertEqual(writes, len(self.s3.writes))
        self.create.assert_called_once()

    def test_any_seed_with_unresolved_document_status_or_email_blocks_all_work(self):
        changes = [{"phase": phase} for phase in (
            "preparing", "prepare_ambiguous", "creating", "create_ambiguous", "email", "unknown",
        )]
        changes += [{"email_state": state} for state in ("sending", "ambiguous", "pending", "failed", "sent")]
        changes += [{"status_mutation": {"state": state}} for state in ("pending", "uncertain")]
        changes += [{"invoice_id": "already-known-document"}]
        for change in changes:
            with self.subTest(change=change):
                self.setUp()
                self.replace_state(lambda state: state["orders"]["1005"].update(change))
                with self.assertRaises(SeededRetryBlocked):
                    self.run_retry(apply=True)
                self.assertEqual([], self.factories)
                self.assertEqual([], self.s3.writes)
                self.create.assert_not_called()

    def test_project_count_source_identity_and_hold_mismatches_fail_before_login(self):
        changes = [
            lambda state: state.update(project="vevo"),
            lambda state: state["orders"].pop("1005"),
            lambda state: state["orders"]["1005"].update(source="unreviewed"),
            lambda state: state["orders"]["1005"].pop("email_policy"),
            lambda state: state["orders"]["1005"].update(project="vevo"),
            lambda state: state["orders"]["1005"].update(order_num="different"),
        ]
        for change in changes:
            with self.subTest(change=change):
                self.setUp()
                self.replace_state(change)
                with self.assertRaises((SeededRetryBlocked, AutomationStateError)):
                    self.run_retry(apply=True)
                self.assertEqual([], self.factories)
                self.assertEqual([], self.s3.writes)
        self.setUp()
        with self.assertRaises(SeededRetryBlocked):
            self.run_retry(expected_count=5, apply=True)
        self.assertEqual([], self.factories)

    def test_existing_lease_and_acquisition_race_are_not_retried(self):
        self.replace_state(lambda state: state.update(lease={"owner": "another-run"}))
        with self.assertRaises(SeededRetryBlocked):
            self.run_retry(apply=True)
        self.assertEqual([], self.s3.writes)
        self.assertEqual([], self.factories)
        self.setUp()
        with patch.object(self.store, "lease", side_effect=AutomationLeaseBusy("synthetic busy")) as lease:
            with self.assertRaises(AutomationLeaseBusy):
                self.run_retry(apply=True)
        lease.assert_called_once()
        self.assertEqual([], self.factories)

    def test_unverified_apply_session_stops_before_any_invoice_attempt(self):
        self.generator.validate_session = lambda: False
        with self.assertRaisesRegex(SeededRetryBlocked, "session is unverified"):
            self.run_retry(apply=True)
        self.create.assert_not_called()
        self.assertEqual([], self.web.post_urls)
        self.web.close.assert_called_once()
        self.assertIsNone(self.store.read()[0]["lease"])

    def test_preview_refuses_an_accidental_authenticated_session_without_writes(self):
        def authenticated_factory(*, web_login):
            self.assertFalse(web_login)
            self.generator.web_session = self.web
            return self.generator
        with self.assertRaisesRegex(SeededRetryBlocked, "preview must not open"):
            self.run_retry(generator_factory=authenticated_factory)
        self.create.assert_not_called()
        self.assertEqual([], self.s3.writes)
        self.web.close.assert_called_once()

    def test_selection_is_revalidated_after_acquisition_without_switching_orders(self):
        original = self.store.lease
        @contextmanager
        def altered_lease(owner):
            with original(owner) as journal:
                journal.update_order("1000", phase="complete")
                yield journal
        with patch.object(self.store, "lease", side_effect=altered_lease):
            with self.assertRaisesRegex(SeededRetryBlocked, "selection changed"):
                self.run_retry(apply=True)
        self.assertEqual([], self.factories)
        self.assertEqual([], self.web.post_urls)
        self.assertIsNone(self.store.read()[0]["lease"])

    def test_fresh_order_ineligibility_prevents_existing_create_call(self):
        for change in ({"blocked": True}, {"status": {"id": 99, "name": "Storno"}},
                       {"invoices": [{"id": "existing-document"}]}):
            with self.subTest(change=change):
                self.setUp()
                self.api.order.update(change)
                with self.assertRaisesRegex(SeededRetryBlocked, "no longer eligible"):
                    self.run_retry(apply=True)
                self.create.assert_not_called()
                self.assertEqual([], self.web.post_urls)
                self.assertIsNone(self.store.read()[0]["lease"])

    def test_hold_loss_during_inspection_blocks_the_attempt(self):
        original = self.generator.fetch_order_for_invoice
        def lose_hold(number):
            order = original(number)
            self.generator.operation_journal.update_order(number, email_policy="send")
            return order
        with patch.object(self.generator, "fetch_order_for_invoice", side_effect=lose_hold):
            with self.assertRaisesRegex(SeededRetryBlocked, "hold mismatch"):
                self.run_retry(apply=True)
        self.create.assert_not_called()
        self.assertEqual([], self.web.post_urls)

    def test_claimed_creation_requires_final_document_and_held_journal_readback(self):
        self.create.return_value = InvoiceCreationResult(created=True, invoice_id="unverified-document", email_required=False)
        with self.assertRaisesRegex(SeededRetryBlocked, "readback is unverified"):
            self.run_retry(apply=True)
        self.create.assert_called_once()
        self.assertEqual([], self.web.post_urls)

    def test_unnumbered_or_mismatched_document_is_not_reported_as_verified_success(self):
        for result_number, record_number, current_number in ((None, None, None), ("", "", ""),
                                                           ("invoice-one", "invoice-two", "invoice-one"),
                                                           ("invoice-one", "invoice-one", "invoice-two")):
            with self.subTest(result_number=result_number, record_number=record_number, current_number=current_number):
                self.setUp()
                def unverified_creation(order):
                    self.api.order["invoices"] = [{"id": "existing-document", "invoice_num": current_number}]
                    self.generator.operation_journal.update_order(
                        "1000", phase="complete", email_state="held", invoice_id="existing-document",
                        invoice_num=record_number,
                    )
                    return InvoiceCreationResult(created=True, invoice_id="existing-document",
                                                 invoice_num=result_number, email_required=False)
                self.create.side_effect = unverified_creation
                with self.assertRaisesRegex(SeededRetryBlocked, "readback is unverified"):
                    self.run_retry(apply=True)
                self.create.assert_called_once()
                self.assertEqual([], self.web.post_urls)

    def test_complete_seeds_are_not_reopened_and_unseeded_records_are_not_selected(self):
        def completed(state):
            for row in state["orders"].values():
                row.update(phase="complete", email_state="held", invoice_id="existing-document")
            state["orders"]["0000"] = {"order_num": "0000", "phase": "pending", "email_policy": "hold"}
        self.replace_state(completed)
        with self.assertRaisesRegex(SeededRetryBlocked, "No unambiguous"):
            self.run_retry(apply=True)
        self.assertEqual([], self.factories)
        self.assertEqual([], self.s3.writes)

    def test_failure_summary_never_copies_arbitrary_provider_fields(self):
        self.assertEqual({"stage": "unknown", "http_status": None, "kind": "unknown"}, safe_creation_failure({
            "last_creation_failure": {"stage": "private-order", "http_status": True, "kind": "private-response", "body": "secret"},
        }))
        self.assertEqual({}, safe_creation_failure({"last_creation_failure": "private-response"}))

    def test_reviewed_preflight_and_readback_diagnostics_remain_aggregate_only(self):
        for stage, kind in (("preflight", "preinvoice_missing"), ("readback", "readback_failed"),
                            ("preparation", "unconfirmed"), ("finalization", "rejected")):
            with self.subTest(stage=stage, kind=kind):
                self.assertEqual({"stage": stage, "http_status": 200, "kind": kind}, safe_creation_failure({
                    "last_creation_failure": {"stage": stage, "kind": kind, "http_status": 200,
                                              "order_num": "private-order", "body": "private-response"},
                }))


class SeededRetrySourceGateTests(unittest.TestCase):
    def test_own_script_must_be_tracked_before_shared_clean_pushed_source_guard(self):
        root = Path("synthetic-repository")
        with patch("scripts.retry_seeded_invoice.subprocess.run") as tracked, \
             patch("scripts.retry_seeded_invoice.require_pushed_source", return_value="reviewed-commit") as pushed:
            self.assertEqual("reviewed-commit", require_retry_source(root))
        self.assertIn("scripts/retry_seeded_invoice.py", tracked.call_args.args[0])
        pushed.assert_called_once_with(root)
        with patch("scripts.retry_seeded_invoice.subprocess.run", side_effect=subprocess.CalledProcessError(1, "git")), \
             patch("scripts.retry_seeded_invoice.require_pushed_source") as pushed:
            with self.assertRaises(subprocess.CalledProcessError):
                require_retry_source(root)
        pushed.assert_not_called()


if __name__ == "__main__":
    unittest.main()
