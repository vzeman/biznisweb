from copy import deepcopy
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from generate_invoices import InvoiceCreationResult
from invoice_automation_state import AutomationStateError, S3AutomationStateStore
from order_status_safety import status_write_block_reason
import reviewed_invoice_obligations as obligations
from tests import test_invoice_generation as invoice_fixtures
from tests.test_reviewed_invoice_obligations import confirmation_fixture, closure_fixture


class ReviewedInvoiceRuntimeTests(unittest.TestCase):
    def setUp(self):
        invoice_fixtures.InvoiceSafetyRegressionTests.setUp(self)
        self.document = confirmation_fixture()
        self.fixed_now = datetime(2026, 9, 13, 8, tzinfo=timezone.utc)
        self.store = S3AutomationStateStore(self.s3, "private", "data/vevo/order-automation/state.json", "vevo",
                                            now=lambda: self.fixed_now)
        self.key = "data/vevo/order-automation/audits/fixture.json"
        self.sha = "e" * 64
        self.enterContext(patch.multiple(obligations, CONFIRMATION_KEY=self.key, CONFIRMATION_SHA256=self.sha,
                          CONFIRMATION_CANONICAL_SHA256=obligations.canonical_sha256(self.document)))
        self.record = closure_fixture(self.document, self.key, self.sha)
        self.record.update(verified_fulfillment_status="Odoslaná", status_observed_at="2026-01-01T00:00:00+00:00")
        self.number = self.document["order_num"]
        self.order.update(id=self.document["order_id"], order_num=self.number, preinvoices=[],
                          status=deepcopy(self.record[obligations.CLOSURE_FIELD]["status_mutation"]["target_status"]))
        self.order["sum"]["value"] = self.document["total"]["value"]

    def seed(self):
        with self.store.lease("seed") as journal:
            journal.update_order(self.number, **{key: value for key, value in self.record.items() if key != "order_num"})

    def run_fixture(self, **kwargs):
        return invoice_fixtures.InvoiceSafetyRegressionTests.run_fixture(self, project="vevo", **kwargs)

    def assert_original_preserved(self):
        record = self.store.read()[0]["orders"][self.number]
        self.assertEqual(obligations.financial_projection(self.record), obligations.financial_projection(record))
        self.assertEqual(self.record[obligations.CLOSURE_FIELD], record[obligations.CLOSURE_FIELD])
        self.assertEqual([], self.web.get_urls)
        self.assertEqual([], self.web.post_urls)
        self.assertEqual([], self.api.preparation_calls)
        return record

    def test_closed_obligation_is_separate_from_active_work_in_full_and_incremental_runs(self):
        self.seed()
        for full in (True, False):
            with self.subTest(full=full):
                result = self.run_fixture(full_backlog=full)
                self.assertEqual((0, 0, 0, 0), (result.matched_orders, result.failed_invoices,
                                 result.ambiguous_invoice_operations, result.pending_invoice_operations))
                self.assertEqual(1, result.reviewed_closed_invoice_obligations)
                self.assertEqual(0, result.reviewed_invoice_obligation_reviews)
                self.assert_original_preserved()

    def test_status_document_amount_and_blocked_regressions_require_review_without_reopening(self):
        for update in ({"status": {"id": "4", "name": "Odoslaná"}},
                       {"invoices": [{"id": "301", "invoice_num": "fixture-final"}]},
                       {"preinvoices": [{"id": "201"}]}, {"blocked": True},
                       {"sum": {"value": 999, "formatted": "999 EUR", "is_net_price": False, "currency": {"code": "EUR"}}}):
            with self.subTest(update=update):
                self.setUp()
                self.order.update(update)
                self.seed()
                result = self.run_fixture()
                self.assertEqual(0, result.matched_orders)
                self.assertEqual(1, result.reviewed_closed_invoice_obligations)
                self.assertEqual(1, result.reviewed_invoice_obligation_reviews)
                saved = self.assert_original_preserved()
                self.assertEqual("open", saved["status_review"]["state"])
                self.assertEqual("2026-01-01T00:00:00+00:00", saved["status_observed_at"])

    def test_pending_uncertain_and_malformed_closures_remain_active_review(self):
        for marker in (None, True, {}, "closed", {**self.record[obligations.CLOSURE_FIELD], "state": "uncertain",
                       "invoice_obligation": "closing"}):
            with self.subTest(marker=marker):
                self.setUp()
                self.record[obligations.CLOSURE_FIELD] = marker
                self.seed()
                result = self.run_fixture()
                self.assertEqual(0, result.reviewed_closed_invoice_obligations)
                self.assertEqual(1, result.reviewed_invoice_obligation_reviews)
                self.assertEqual(1, result.pending_invoice_operations)
                self.assertEqual(0, result.matched_orders)
                self.assert_original_preserved()

    def test_direct_financial_entrypoints_cannot_recover_late_document_or_complete_held_email(self):
        self.seed()
        self.order["preinvoices"] = [{"id": "201"}]
        self.order["invoices"] = [{"id": "301", "invoice_num": "fixture-final"}]
        with self.store.lease("direct-test") as journal:
            self.generator.operation_journal = journal
            before = journal.get_order(self.number)
            self.assertTrue(self.generator.create_invoice(self.order).skipped)
            result = InvoiceCreationResult(email_required=False, invoice_id="301", invoice_num="fixture-final")
            self.assertIsNone(self.generator._prepare_invoice(self.order, self.document["order_id"], result))
            self.generator._send_journaled_email(self.number, result)
            with self.assertRaises(AutomationStateError):
                self.generator.send_invoice_email(self.document["order_id"])
            self.assertEqual(before, journal.get_order(self.number))
        self.assert_original_preserved()
        self.assertEqual([], self.api.calls)

    def test_no_journal_financial_calls_are_blocked_while_readonly_fetch_stays_usable(self):
        self.generator.operation_journal = None
        for action in (lambda: self.generator.create_invoice(self.order),
                       lambda: self.generator._prepare_invoice(self.order, self.document["order_id"], InvoiceCreationResult()),
                       lambda: self.generator._send_journaled_email(self.number, InvoiceCreationResult(email_required=False)),
                       lambda: self.generator.send_invoice_email(self.document["order_id"]),
                       lambda: self.generator.process_orders(self.fixed_now, self.fixed_now)):
            with self.assertRaisesRegex(AutomationStateError, "owned project journal"):
                action()
        self.assertEqual(self.number, self.generator.fetch_order_for_invoice(self.number)["order_num"])
        self.assertEqual([], self.web.get_urls)
        self.assertEqual([], self.api.preparation_calls)

    def test_enqueue_pending_and_shipment_memory_do_not_change_closed_or_invalid_records(self):
        for marker in (self.record[obligations.CLOSURE_FIELD], None):
            self.record[obligations.CLOSURE_FIELD] = marker
            self.seed()
            self.order["status"] = {"id": "4", "name": "Odoslaná"}
            with self.store.lease("enqueue-test") as journal:
                before = journal.get_order(self.number)
                journal.enqueue_orders([self.order])
                journal.enqueue_status_reviews([self.order])
                journal.remember_shipped_orders([self.order], {4})
                self.assertEqual(before, journal.get_order(self.number))
                self.assertEqual(0 if marker else 1, len(journal.pending_orders()))

    def test_status_write_gate_blocks_all_marker_presence_even_full_creditnote(self):
        for marker in (None, False, {}, self.record[obligations.CLOSURE_FIELD]):
            for arguments in ({}, {"next_reason": "full_creditnote", "next_target_status_name": "Storno"}):
                self.assertEqual("reviewed_noncollection_requires_manual_review",
                                 status_write_block_reason({obligations.CLOSURE_FIELD: marker}, **arguments))

    def test_dry_run_surfaces_closure_and_regression_without_journal_writes(self):
        self.seed()
        before = self.store.read()[0]
        self.order["status"] = {"id": "4", "name": "Odoslaná"}
        result = self.run_fixture(dry_run=True)
        self.assertEqual((0, 1, 1), (result.matched_orders, result.reviewed_closed_invoice_obligations,
                                   result.reviewed_invoice_obligation_reviews))
        self.assertEqual(before, self.store.read()[0])
        self.assert_original_preserved()

    def test_durable_send_intent_without_new_authorization_never_replays_directly(self):
        with self.store.lease("direct-test") as journal:
            self.generator.operation_journal = journal
            journal.update_order(self.number, order_id=self.document["order_id"], phase="email", email_state="sending")
            before = journal.get_order(self.number)
            for _ in range(2):
                with self.assertRaisesRegex(AutomationStateError, "newly authorized"):
                    self.generator.send_invoice_email(self.document["order_id"])
            self.assertEqual(before, journal.get_order(self.number))
        self.assertEqual([], self.web.get_urls)

    def test_journal_cannot_overwrite_original_financial_fields_after_any_closure_marker(self):
        self.seed()
        with self.store.lease("direct-test") as journal:
            before = journal.get_order(self.number)
            for field in obligations.FINANCIAL_FIELDS:
                if field == "order_num":
                    continue
                with self.assertRaisesRegex(AutomationStateError, "preserve original"):
                    journal.update_order(self.number, **{field: "changed"})
            self.assertEqual(before, journal.get_order(self.number))

    def test_regression_review_remains_open_after_order_returns_to_target(self):
        self.seed()
        target = deepcopy(self.order["status"])
        self.order["status"] = {"id": "4", "name": "Odoslaná"}
        self.assertEqual(1, self.run_fixture().reviewed_invoice_obligation_reviews)
        self.order["status"] = target
        for _ in range(3):
            self.assertEqual(1, self.run_fixture(full_backlog=False).reviewed_invoice_obligation_reviews)
            self.assertEqual("reviewed_closure_regression",
                             self.store.read()[0]["orders"][self.number]["status_review"]["reason"])
        self.assert_original_preserved()
