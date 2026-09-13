from copy import deepcopy
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from invoice_automation_state import AutomationStateError, S3AutomationStateStore
import reviewed_invoice_obligations as obligations
from scripts import close_reviewed_uncollected_obligation as helper
from tests.test_invoice_automation_state import MemoryS3
from tests.test_reviewed_invoice_obligations import confirmation_fixture


class FakeWeb:
    def __init__(self):
        self.calls = []
        self.closed = False
        self.responses = []
        self.payload = {"total": "0", "rows": []}
        self.receipts = {"rows": []}
        self.http_status = 200
        self.text_override = None
        self.retries = 0

    def get_adapter(self, prefix):
        return SimpleNamespace(max_retries=SimpleNamespace(total=self.retries))

    def post(self, url, **kwargs):
        self.calls.append((url, deepcopy(kwargs)))
        receipt_read = url.endswith('/erp/orders/receipts/getListByOrderJson/101')
        assert receipt_read or url.endswith('/erp/orders/invoices/getListJson'), "Unexpected native write route"
        response = SimpleNamespace(status_code=self.http_status,
                                   text=self.text_override if self.text_override is not None else json.dumps(self.receipts if receipt_read else self.payload),
                                   close=Mock())
        self.responses.append(response)
        return response

    def close(self):
        self.closed = True


class FakeGenerator:
    def __init__(self, document):
        self.order = {
            "id": document["order_id"], "order_num": document["order_num"], "blocked": False,
            "status": deepcopy(document["source_status"]), "last_change": "2026-09-13 06:40:00",
            "invoices": [], "preinvoices": [], "shipments": [],
            "sum": {"value": document["total"]["value"], "currency": {"code": "EUR"}, "is_net_price": False},
            "price_elements": [{"type": "payment", "reference_id": document["payment_reference_id"], "title": "Dobierka"}],
        }
        self.statuses = [deepcopy(document["source_status"]), {"id": "74", "name": "Neprevzaté - Storno"}]
        self.client = SimpleNamespace(transport=SimpleNamespace(retries=0, close=Mock()), execute=self.execute)
        self.web_session = FakeWeb()
        self.base_url = "https://vevo.flox.sk"
        self.arf_token = "fixtureToken"
        self.reads = 0
        self.before_read = lambda *_: None
        self.before_mutation = lambda *_: None
        self.fail_mutation = False
        self.commit_before_error = False
        self.mutations = []
        self.operation_journal = None

    def _require_native_token(self):
        if not self.arf_token:
            raise ValueError("native_session_token_missing")

    def execute_read(self, query, variables):
        assert variables == {"lang_code": "SK"}
        return {"listOrderStatuses": deepcopy(self.statuses)}

    def fetch_order_safety_context(self, number):
        assert number == self.order["order_num"]
        self.reads += 1
        self.before_read(self, self.reads)
        if self.operation_journal:
            self.operation_journal.assert_owned()
        return deepcopy(self.order)

    def execute(self, query, variable_values=None):
        values = variable_values or {}
        if "status_id" not in values:
            return {"getOrder": deepcopy(self.order)}
        assert values["order_num"] == self.order["order_num"]
        assert values["send_notification"] == [{"type": "EMAIL_CUSTOMER", "if": ["NONE"]}]
        self.before_mutation(self)
        self.mutations.append(deepcopy(values))
        if not self.fail_mutation or self.commit_before_error:
            self.order["status"] = deepcopy(next(row for row in self.statuses if int(row["id"]) == values["status_id"]))
            self.order["last_change"] = "2026-09-13 07:10:00"
        if self.fail_mutation:
            raise RuntimeError("synthetic uncertain response")
        return {"changeOrderStatus": deepcopy(self.order)}


class ReviewedUncollectedClosureTests(unittest.TestCase):
    def setUp(self):
        self.document = confirmation_fixture()
        self.key = "data/vevo/order-automation/audits/2026-09-13/fixture-confirmation.json"
        digest = obligations.canonical_sha256(self.document)
        pins = patch.multiple(obligations, CONFIRMATION_KEY=self.key,
                              CONFIRMATION_SHA256=digest, CONFIRMATION_CANONICAL_SHA256=digest)
        pins.start()
        self.addCleanup(pins.stop)
        receipt_pins = patch.multiple(helper, RECEIPT_CONTRACT_KEY="data/vevo/order-automation/audits/2026-09-13/fixture-receipt-contract.json",
                                      RECEIPT_CONTRACT_SHA256="f" * 64)
        receipt_pins.start()
        self.addCleanup(receipt_pins.stop)
        self.clock = datetime(2026, 9, 13, 7, 0, tzinfo=timezone.utc)
        self.s3 = MemoryS3()
        self.store = S3AutomationStateStore(self.s3, "private", "data/vevo/order-automation/state.json", "vevo", now=lambda: self.clock)
        state = self.store.empty_state()
        self.original = deepcopy(self.document["original_financial_operation"])
        state["orders"][self.document["order_num"]] = deepcopy(self.original)
        self.store.put(state, "")
        self.generator = FakeGenerator(self.document)
        self.factory = Mock(return_value=self.generator)
        self.gate = Mock()

    def run_helper(self, apply=False):
        return helper.close_obligation(store=self.store, document=self.document, generator_factory=self.factory,
                                       apply=apply, source_commit="d" * 40, release_gate=self.gate)

    def record(self):
        return self.store.read()[0]["orders"][self.document["order_num"]]

    def assert_original(self):
        self.assertEqual(self.original, obligations.financial_projection(self.record()))

    def assert_no_business_write(self):
        self.assertEqual([], self.generator.mutations)
        self.assert_original()

    def test_preview_reads_exact_order_and_native_grid_without_journal_or_business_writes(self):
        before = len(self.s3.writes)
        result = self.run_helper()
        self.assertTrue(result["ok"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(before, len(self.s3.writes))
        self.assert_no_business_write()
        self.gate.assert_not_called()
        self.assertEqual(2, len(self.generator.web_session.calls))
        _, request = self.generator.web_session.calls[0]
        self.assertEqual({"find": "o#900001", "start": 0, "limit": 20, "arf": "fixtureToken"}, request["data"])
        self.assertFalse(request["allow_redirects"])
        self.assertTrue(self.generator.web_session.closed)
        self.generator.client.transport.close.assert_called_once()
        self.generator.web_session.responses[0].close.assert_called_once()
        self.assertEqual({"arf": "fixtureToken"}, self.generator.web_session.calls[1][1]["data"])
        self.generator.web_session.responses[1].close.assert_called_once()

    def test_one_silent_status_request_consumes_intent_before_transmission_preserves_financial_unknown(self):
        def before(_):
            record = self.record()
            marker = record[obligations.CLOSURE_FIELD]
            self.assertEqual(("intent", "closing", "unknown"),
                             (marker["state"], marker["invoice_obligation"], marker["financial_outcome"]))
            self.assert_original()
        self.generator.before_mutation = before
        result = self.run_helper(True)
        self.assertEqual((True, 1, 1, 0, 0), (result["ok"], result["status_requests"], result["closed_obligations"],
                                            result["financial_requests"], result["emailed_invoices"]))
        self.assertEqual(1, len(self.generator.mutations))
        self.assertEqual(3, self.gate.call_count)
        self.assertTrue(obligations.closure_decision(self.record(), project="vevo").closed)
        self.assert_original()
        record = deepcopy(self.record())
        again = self.run_helper(True)
        self.assertEqual(0, again["status_requests"])
        self.assertEqual(record, self.record())
        self.assertEqual(1, len(self.generator.mutations))

    def test_uncertain_status_with_unchanged_source_cannot_be_replayed(self):
        self.generator.fail_mutation = True
        result = self.run_helper(True)
        self.assertFalse(result["ok"])
        record = deepcopy(self.record())
        self.assertEqual("uncertain", record[obligations.CLOSURE_FIELD]["state"])
        self.generator.fail_mutation = False
        second = self.run_helper(True)
        self.assertFalse(second["ok"])
        self.assertEqual(0, second["status_requests"])
        self.assertEqual(1, len(self.generator.mutations))
        self.assertEqual(record, self.record())
        self.assert_original()

    def test_committed_status_with_lost_response_is_resolved_by_readback_without_replay(self):
        self.generator.fail_mutation = True
        self.generator.commit_before_error = True
        result = self.run_helper(True)
        self.assertTrue(result["ok"])
        self.assertEqual(1, len(self.generator.mutations))
        self.assertTrue(obligations.closure_decision(self.record(), project="vevo").closed)

    def test_consumed_intent_can_close_only_by_later_target_readback(self):
        self.generator.fail_mutation = True
        self.run_helper(True)
        intent = self.record()[obligations.CLOSURE_FIELD]["status_mutation"]["intent_id"]
        self.generator.order["status"] = deepcopy(self.generator.statuses[1])
        result = self.run_helper(True)
        self.assertTrue(result["ok"])
        self.assertEqual(0, result["status_requests"])
        self.assertEqual(1, len(self.generator.mutations))
        self.assertEqual(intent, self.record()[obligations.CLOSURE_FIELD]["status_mutation"]["intent_id"])

    def test_release_rejection_occurs_before_any_new_intent(self):
        self.gate.side_effect = helper.ClosureBlocked("release-rejected")
        with self.assertRaises(helper.ClosureBlocked):
            self.run_helper(True)
        self.factory.assert_not_called()
        self.assertNotIn(obligations.CLOSURE_FIELD, self.record())
        self.assert_no_business_write()

    def test_second_release_check_failure_also_cannot_consume_an_intent(self):
        self.gate.side_effect = [None, helper.ClosureBlocked("release-drift")]
        with self.assertRaises(helper.ClosureBlocked):
            self.run_helper(True)
        self.assertNotIn(obligations.CLOSURE_FIELD, self.record())
        self.assert_no_business_write()

    def test_release_drift_after_intent_blocks_status_and_permanently_consumes_it(self):
        self.gate.side_effect = [None, None, helper.ClosureBlocked("release-drift")]
        with self.assertRaises(helper.ClosureBlocked):
            self.run_helper(True)
        self.assertEqual("intent", self.record()[obligations.CLOSURE_FIELD]["state"])
        self.assert_no_business_write()
        self.gate.side_effect = None
        self.assertFalse(self.run_helper(True)["ok"])
        self.assert_no_business_write()

    def test_preview_of_consumed_intent_at_target_reports_readback_ready_but_not_durable_closed(self):
        self.generator.fail_mutation = True
        self.run_helper(True)
        self.generator.order["status"] = deepcopy(self.generator.statuses[1])
        prior = deepcopy(self.record())
        result = self.run_helper(False)
        self.assertTrue(result["readback_ready"])
        self.assertEqual(0, result["closed_obligations"])
        self.assertEqual(prior, self.record())

    def test_changed_amount_before_intent_is_blocked(self):
        def drift(generator, count):
            if count == 3:
                generator.order["sum"]["value"] = "16"
        self.generator.before_read = drift
        with self.assertRaises(helper.ClosureBlocked):
            self.run_helper(True)
        self.assertNotIn(obligations.CLOSURE_FIELD, self.record())
        self.assert_no_business_write()

    def test_context_change_after_intent_permanently_consumes_attempt_without_status_request(self):
        def drift(generator, count):
            if count == 4:
                generator.order["last_change"] = "2026-09-13 06:41:00"
        self.generator.before_read = drift
        with self.assertRaises(helper.ClosureBlocked):
            self.run_helper(True)
        self.assertEqual("intent", self.record()[obligations.CLOSURE_FIELD]["state"])
        second = self.run_helper(True)
        self.assertFalse(second["ok"])
        self.assert_no_business_write()

    def test_invalid_local_clock_cannot_persist_invalid_intent(self):
        self.clock = datetime(2026, 9, 13, 6, 44, tzinfo=timezone.utc)
        with self.assertRaisesRegex(helper.ClosureBlocked, "proposed-closure-marker-invalid"):
            self.run_helper(True)
        self.assertNotIn(obligations.CLOSURE_FIELD, self.record())
        self.assert_no_business_write()

    def test_current_target_appearing_after_intent_closes_without_status_request(self):
        def drift(generator, count):
            if count == 4:
                generator.order["status"] = deepcopy(generator.statuses[1])
        self.generator.before_read = drift
        result = self.run_helper(True)
        self.assertTrue(result["ok"])
        self.assertEqual(0, result["status_requests"])
        self.assert_no_business_write()

    def test_native_nonzero_missing_malformed_and_http_error_are_not_empty_proof(self):
        for payload in ({"rows": []}, {"total": True, "rows": []}, {"total": 0, "rows": None},
                        {"total": "1", "rows": [{"pre_inv_id": "1"}]},
                        {"total": 0, "rows": [], "success": False}, {"total": 0, "rows": [], "errors": ["bad"]}):
            with self.subTest(payload=payload):
                self.generator.web_session.payload = payload
                with self.assertRaises(helper.ClosureBlocked):
                    self.run_helper(True)
                self.assert_no_business_write()
                self.assertNotIn(obligations.CLOSURE_FIELD, self.record())
        self.generator.web_session.payload = {"total": 0, "rows": []}
        self.generator.web_session.http_status = 429
        with self.assertRaises(helper.ClosureBlocked):
            self.run_helper(True)
        self.assert_no_business_write()

    def test_missing_arf_or_retry_enabled_transport_cannot_consume_intent(self):
        for field in ("arf", "api", "web"):
            with self.subTest(field=field):
                self.generator.arf_token = "" if field == "arf" else "fixtureToken"
                self.generator.client.transport.retries = 1 if field == "api" else 0
                self.generator.web_session.retries = 1 if field == "web" else 0
                with self.assertRaises((ValueError, helper.ClosureBlocked)):
                    self.run_helper(True)
                self.assertNotIn(obligations.CLOSURE_FIELD, self.record())
                self.assert_no_business_write()

    def test_standalone_receipt_presence_or_unknown_shape_blocks_even_without_api_documents(self):
        for payload in ({"rows": [{"number": "1", "price": "15"}]}, {"rows": [], "total": 0},
                        {"rows": None}, {}, {"success": False, "rows": []}):
            with self.subTest(payload=payload):
                self.generator.web_session.receipts = payload
                with self.assertRaises(helper.ClosureBlocked):
                    self.run_helper(True)
                self.assertNotIn(obligations.CLOSURE_FIELD, self.record())
                self.assert_no_business_write()

    def test_current_order_scope_payment_and_document_drift_block(self):
        original = deepcopy(self.generator.order)
        changes = (
            ("id", "102"), ("order_num", "900002"), ("blocked", True),
            ("invoices", [{"id": "101"}]), ("preinvoices", [{"id": "101"}]),
            ("price_elements", [{"type": "payment", "reference_id": "8"}]),
            ("sum", {"value": "15", "currency": {"code": "USD"}, "is_net_price": False}),
            ("sum", {"value": "15", "currency": {"code": "EUR"}}),
            ("status", {"id": "55", "name": "Platba online - zaplatené"}),
            ("shipments", [{"status": "delivered", "shipment_number": "fixture"}]),
            ("shipments", [{"status": "returned", "shipment_number": "a"}, {"status": "delivered", "shipment_number": "b"}]),
            ("shipments", [{"status": "returned"}]),
        )
        for field, value in changes:
            with self.subTest(field=field):
                self.generator.order = {**deepcopy(original), field: value}
                with self.assertRaises((AssertionError, helper.ClosureBlocked)):
                    self.run_helper(True)
                self.assert_no_business_write()

    def test_incomplete_tracking_is_not_misclassified_as_delivered_against_user_confirmation(self):
        self.generator.order["shipments"] = [{"status": "pending", "shipment_number": "fixture"}]
        self.assertTrue(self.run_helper()["ok"])
        self.assert_no_business_write()

    def test_verified_closure_later_regressed_to_source_is_reviewed_without_new_request(self):
        self.run_helper(True)
        self.generator.order["status"] = deepcopy(self.document["source_status"])
        prior = deepcopy(self.record())
        result = self.run_helper(True)
        self.assertFalse(result["ok"])
        self.assertEqual(1, result["closed_obligations"])
        self.assertTrue(result["regression"])
        self.assertEqual(0, result["status_requests"])
        self.assertEqual(1, len(self.generator.mutations))
        self.assertEqual(prior, self.record())

    def test_readback_closure_resolves_only_its_exact_unconfirmed_runtime_review(self):
        self.generator.fail_mutation = True
        self.run_helper(True)
        state, etag = self.store.read()
        state["orders"]["900001"]["status_review"] = {"state": "open", "reason": "reviewed_closure_unconfirmed"}
        self.store.put(state, etag)
        self.generator.order["status"] = deepcopy(self.generator.statuses[1])
        self.assertTrue(self.run_helper(True)["ok"])
        self.assertEqual({"state": "closed", "reason": "reviewed_obligation_closed"}, self.record()["status_review"])
        self.assert_original()

    def test_unrelated_or_invalid_review_is_not_cleared_by_successful_closure(self):
        for reason in ("reviewed_closure_regression", "reviewed_closure_invalid", "payment_reversal_requires_review"):
            with self.subTest(reason=reason):
                state, etag = self.store.read()
                state["orders"]["900001"] = deepcopy(self.original)
                review = {"state": "open", "reason": reason}
                state["orders"]["900001"]["status_review"] = review
                self.store.put(state, etag)
                self.generator.order["status"] = deepcopy(self.document["source_status"])
                self.assertTrue(self.run_helper(True)["ok"])
                self.assertEqual(review, self.record()["status_review"])

    def test_aws_clients_are_bounded_cached_and_all_closed_on_failure(self):
        clients = {name: Mock() for name in ("s3", "ecs", "sts")}
        session = SimpleNamespace(client=Mock(side_effect=lambda name, **kwargs: clients[name]))
        config = object()
        with self.assertRaises(RuntimeError):
            with helper.bounded_aws_clients(session, config) as bounded:
                for name in clients:
                    self.assertIs(clients[name], bounded.client(name))
                    self.assertIs(clients[name], bounded.client(name))
                raise RuntimeError("synthetic exit")
        self.assertEqual(3, session.client.call_count)
        for call in session.client.call_args_list:
            self.assertIs(config, call.kwargs["config"])
        for client in clients.values():
            client.close.assert_called_once()

    def test_manual_payment_or_prior_status_intent_conflicts_with_closure(self):
        for field, value in (("manual_settlement", {}), ("status_mutation", {"state": "verified"})):
            with self.subTest(field=field):
                state, etag = self.store.read()
                state["orders"]["900001"] = {**deepcopy(self.original), field: value}
                self.store.put(state, etag)
                with self.assertRaises(helper.ClosureBlocked):
                    self.run_helper(True)
                self.assert_no_business_write()

    def test_target_catalogue_requires_unique_same_shop_noncollection_target(self):
        for statuses in ([{"id": "4", "name": "Odoslaná"}],
                         [self.generator.statuses[1], {"id": "75", "name": "NEPREVZATÉ STORNO"}],
                         [self.generator.statuses[1], {"id": "74", "name": "Other"}]):
            with self.subTest(statuses=statuses):
                self.generator.statuses = deepcopy(statuses)
                with self.assertRaises(helper.ClosureBlocked):
                    self.run_helper(True)
                self.assert_no_business_write()

    def test_s3_failure_after_status_transmission_retains_consumed_intent_and_forbids_retry(self):
        def fail_storage(_):
            self.s3.fail_write = True
        self.generator.before_mutation = fail_storage
        with self.assertRaises(AutomationStateError):
            self.run_helper(True)
        self.assertEqual(1, len(self.generator.mutations))
        self.assertEqual("intent", self.record()[obligations.CLOSURE_FIELD]["state"])
        self.s3.fail_write = False
        self.clock += timedelta(hours=1)
        # The expired retained lease must be explicitly observed; the helper
        # conservatively refuses even expired lease presence until ownership is reconciled.
        with self.assertRaises(helper.ClosureBlocked):
            self.run_helper(True)
        self.assertEqual(1, len(self.generator.mutations))
        self.assert_original()

    def test_unpinned_cli_apply_fails_before_credentials_source_or_network(self):
        with patch.object(helper, "REVIEWED_RELEASE_COMMIT", ""), patch.object(helper, "require_source") as source:
            with self.assertRaisesRegex(helper.ClosureBlocked, "compatible-release-not-pinned"):
                helper.main(["--apply"])
            source.assert_not_called()

    def test_original_six_case_release_constants_are_not_changed_by_new_gate(self):
        from scripts import reconcile_reviewed_invoice_outcomes as original
        expected = (original.REVIEWED_RELEASE_COMMIT, original.REVIEWED_RELEASE_DIGEST)
        with self.assertRaises(helper.ClosureBlocked):
            helper.verify_release(Mock(), commit=expected[0], digest=expected[1], evidence_key="unused")
        self.assertEqual(expected, (original.REVIEWED_RELEASE_COMMIT, original.REVIEWED_RELEASE_DIGEST))

    def test_confirmation_loader_checks_all_raw_private_hashes_and_closes_bodies(self):
        raw = json.dumps(self.document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        calls = []
        body = BytesIO(raw)
        s3 = SimpleNamespace(get_object=Mock(return_value={"Body": body, "ServerSideEncryption": "AES256"}))
        with patch.object(helper, "private_json", side_effect=lambda client, key, **kw: (
                calls.append((key, kw["expected_sha"])), deepcopy(self.document) if key == self.key else {
                    "read_only": True, "project": "vevo", "order_id": "101", "order_num": "900001",
                    "financial_requests": 0, "status_writes": 0, "journal_mutations": 0,
                    "native_receipts": {"method": "POST", "parameters": ["arf"],
                        "route": "/erp/orders/receipts/getListByOrderJson/101", "response": {"rows": []}},
                } if key == helper.RECEIPT_CONTRACT_KEY else {})[1]):
            self.assertEqual(self.document, helper.load_confirmation(s3))
        self.assertEqual(5, len(calls))
        self.assertEqual((self.key, obligations.CONFIRMATION_SHA256), calls[0])
        # The imported bounded reader owns/ closes the stream on success and hash failure.
        with self.assertRaises(Exception):
            helper.private_json(s3, self.key, expected_sha="0" * 64)
        self.assertTrue(body.closed)


if __name__ == "__main__":
    unittest.main()
