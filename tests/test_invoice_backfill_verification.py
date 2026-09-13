from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from scripts.verify_invoice_backfill import (
    SOURCE, publish_private_report, report_markdown, verify_backfill, write_private_report,
)


def record(**updates):
    return {"order_num": "ORDER-A", "source": SOURCE, "phase": "complete", "email_policy": "hold",
            "email_state": "held", "invoice_id": "INVOICE-A", **updates}


def order(**updates):
    return {"id": "INTERNAL-A", "order_num": "ORDER-A", "blocked": False,
            "status": {"id": "4", "name": "Versandt"}, "invoices": [{"id": "INVOICE-A", "invoice_num": "NUMBER-A"}],
            "sum": {"value": 12, "currency": {"code": "EUR"}}, **updates}


class ReadOnlyStore:
    def __init__(self, records=None):
        self.state = {"schema_version": 1, "project": "roy", "lease": None,
                      "orders": records if records is not None else {"ORDER-A": record()}}
        self.reads = 0
        self.change = False

    def read(self):
        self.reads += 1
        return deepcopy(self.state), str(self.reads if self.change else 1)

    def lease(self, *args, **kwargs):
        raise AssertionError("Verifier must never lease")

    def put(self, *args, **kwargs):
        raise AssertionError("Verifier must never update journal")


class InvoiceBackfillVerificationTests(unittest.TestCase):
    def verify(self, store=None, read_order=None, **kwargs):
        return verify_backfill(store=store or ReadOnlyStore(), project="roy", expected_count=kwargs.pop("expected_count", 1),
                               read_order=read_order or (lambda _: order()), shipped_status_ids={4},
                               source_commit="fixture-commit", now=datetime(2026, 1, 1, tzinfo=timezone.utc), **kwargs)

    def test_exact_seeded_batch_completes_from_fresh_invoice_and_held_email(self):
        store = ReadOnlyStore()
        original = deepcopy(store.state)
        report = self.verify(store)
        self.assertTrue(report["ok"])
        self.assertEqual({"verified_invoice": 1}, report["outcome_counts"])
        self.assertEqual(2, store.reads)
        self.assertEqual(original, store.state)

    def test_wrong_count_and_unseeded_orders_cannot_make_partial_pass(self):
        store = ReadOnlyStore({"ORDER-A": record(), "ORDER-B": record(order_num="ORDER-B", source="ordinary-run")})
        report = self.verify(store, expected_count=2)
        self.assertFalse(report["ok"])
        self.assertEqual(1, report["selected_count"])
        self.assertIn("seeded_order_count_mismatch", report["global_issues"])
        self.assertFalse(self.verify(ReadOnlyStore({}))["ok"])

    def test_unfinished_uncertain_and_emailed_journal_records_fail(self):
        for updates in ({"phase": "pending"}, {"phase": "create_ambiguous"}, {"email_state": "ambiguous"},
                        {"email_state": "sent"}, {"email_policy": "send"},
                        {"status_mutation": {}}, {"status_mutation": {"state": "pending"}},
                        {"status_mutation": {"state": "uncertain"}}):
            with self.subTest(updates=updates):
                self.assertFalse(self.verify(ReadOnlyStore({"ORDER-A": record(**updates)}))["ok"])

    def test_current_invoice_must_be_unique_and_match_journal(self):
        for invoices in ([], None, [{"id": "OTHER"}], [{"id": "INVOICE-A"}, {"id": "SECOND"}], [{}]):
            with self.subTest(invoices=invoices):
                self.assertFalse(self.verify(read_order=lambda _: order(invoices=invoices))["ok"])

    def test_missing_fresh_fields_or_wrong_identity_never_pass(self):
        for field in ("invoices", "sum", "blocked", "id", "status"):
            current = order()
            current.pop(field)
            with self.subTest(field=field):
                self.assertFalse(self.verify(read_order=lambda _: current)["ok"])
        self.assertFalse(self.verify(read_order=lambda _: order(order_num="ANOTHER"))["ok"])
        self.assertFalse(self.verify(read_order=lambda _: order(sum={"value": "NaN"}))["ok"])

    def test_explicit_external_creation_can_complete_without_an_email_attempt(self):
        prior = record(invoice_id=None, email_state=None, reason="invoice_created_elsewhere")
        report = self.verify(ReadOnlyStore({"ORDER-A": prior}))
        self.assertTrue(report["ok"])
        self.assertEqual("externally_invoiced", report["orders"][0]["outcome"])
        prior.pop("reason")
        self.assertFalse(self.verify(ReadOnlyStore({"ORDER-A": prior}))["ok"])

    def test_fresh_ineligibility_requires_explicit_completed_disposition(self):
        prior = record(invoice_id=None, email_state=None, reason="eligibility_changed")
        current = order(invoices=None, blocked=True, status={"id": 99, "name": "Storno"})
        report = self.verify(ReadOnlyStore({"ORDER-A": prior}), read_order=lambda _: current)
        self.assertTrue(report["ok"])
        self.assertEqual("no_longer_eligible", report["orders"][0]["outcome"])
        self.assertEqual(["blocked", "not_shipped"], report["orders"][0]["ineligible_reasons"])
        prior.pop("reason")
        self.assertFalse(self.verify(ReadOnlyStore({"ORDER-A": prior}), read_order=lambda _: current)["ok"])

    def test_completed_invoice_now_ineligible_is_reported_explicitly(self):
        report = self.verify(read_order=lambda _: order(status={"id": 99, "name": "Storno"}))
        self.assertTrue(report["ok"])
        self.assertEqual("invoiced_now_ineligible", report["orders"][0]["outcome"])

    def test_api_failure_does_not_emit_exception_body_or_claim_partial_pass(self):
        report = self.verify(read_order=Mock(side_effect=RuntimeError("PRIVATE-TOKEN-AND-RESPONSE")))
        self.assertFalse(report["ok"])
        self.assertEqual(1, report["failed_orders"])
        self.assertNotIn("PRIVATE-TOKEN", json.dumps(report))

    def test_changed_journal_or_retained_lease_prevents_pass(self):
        store = ReadOnlyStore()
        store.change = True
        self.assertIn("journal_changed_during_verification", self.verify(store)["global_issues"])
        store = ReadOnlyStore()
        store.state["lease"] = {"owner": "running"}
        self.assertFalse(self.verify(store)["ok"])

    def test_final_journal_read_failure_and_identity_mismatch_are_not_passes(self):
        store = ReadOnlyStore()
        before = store.read()
        store.read = Mock(side_effect=[before, RuntimeError("private response")])
        self.assertIn("journal_final_read_failed", self.verify(store)["global_issues"])
        store = ReadOnlyStore()
        store.state["project"] = "vevo"
        self.assertFalse(self.verify(store)["ok"])

    def test_private_report_omits_customer_data_and_escapes_markdown(self):
        report = self.verify(read_order=lambda _: order(customer={"email": "private@example.invalid"}))
        self.assertNotIn("private@example", json.dumps(report))
        report["orders"][0]["order_num"] = "<script>|unsafe"
        markdown = report_markdown(report)
        self.assertIn("&lt;script&gt;\\|unsafe", markdown)
        self.assertIn("**PASS**", markdown)

    def test_artifacts_are_written_only_to_ignored_untracked_project_directory(self):
        report = self.verify()
        with TemporaryDirectory() as folder, patch("scripts.verify_invoice_backfill.subprocess.run") as run, \
             patch("scripts.verify_invoice_backfill.subprocess.check_output", return_value=""):
            run.return_value.returncode = 0
            paths = write_private_report(Path(folder), report)
            self.assertEqual({".json", ".md"}, {path.suffix for path in paths})
            # Windows runner temp directories can use a short-name alias while
            # the helper intentionally returns canonical resolved paths.
            expected_folder = (Path(folder) / "data" / "roy" / "order-automation").resolve()
            self.assertTrue(all(path.is_relative_to(expected_folder) for path in paths))
            self.assertEqual(report, json.loads(paths[0].read_text(encoding="utf-8")))
            run.return_value.returncode = 1
            with self.assertRaises(RuntimeError):
                write_private_report(Path(folder), report)

    def test_optional_publication_uses_only_private_immutable_report_keys(self):
        with TemporaryDirectory() as folder:
            paths = [Path(folder) / "backfill-verification-test.json", Path(folder) / "backfill-verification-test.md"]
            for path in paths:
                path.write_text("private report", encoding="utf-8")
            client = Mock()
            client.get_public_access_block.return_value = {"PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True}}
            client.get_object.side_effect = lambda **kwargs: {"Body": BytesIO(b"private report")}
            publish_private_report(client, bucket="private-existing", project="roy", account="fixture-account", paths=paths)
            self.assertEqual(2, client.put_object.call_count)
            for call in client.put_object.call_args_list:
                self.assertTrue(call.kwargs["Key"].startswith("data/roy/order-automation/verification/"))
                self.assertNotIn("state.json", call.kwargs["Key"])
                self.assertEqual("*", call.kwargs["IfNoneMatch"])
                self.assertEqual("AES256", call.kwargs["ServerSideEncryption"])
            client.reset_mock()
            client.get_public_access_block.return_value = {"PublicAccessBlockConfiguration": {}}
            with self.assertRaises(RuntimeError):
                publish_private_report(client, bucket="private-existing", project="roy", account="fixture-account", paths=paths)
            client.put_object.assert_not_called()


if __name__ == "__main__":
    unittest.main()


class ReviewedClosureBackfillTests(unittest.TestCase):
    def setUp(self):
        import reviewed_invoice_obligations as obligations
        from tests.test_reviewed_invoice_obligations import confirmation_fixture, closure_fixture
        self.obligations = obligations
        self.document = confirmation_fixture()
        self.key, self.sha = "data/vevo/order-automation/audits/fixture.json", "e" * 64
        self.enterContext(patch.multiple(obligations, CONFIRMATION_KEY=self.key, CONFIRMATION_SHA256=self.sha,
                          CONFIRMATION_CANONICAL_SHA256=obligations.canonical_sha256(self.document)))
        self.record = closure_fixture(self.document, self.key, self.sha)
        self.record["source"] = SOURCE
        self.current = {"id": self.document["order_id"], "order_num": self.document["order_num"], "blocked": False,
                        "status": deepcopy(self.record[obligations.CLOSURE_FIELD]["status_mutation"]["target_status"]),
                        "invoices": None, "preinvoices": None,
                        "sum": {"value": self.document["total"]["value"], "currency": {"code": "EUR"}}}

    def verify(self, project="vevo"):
        store = ReadOnlyStore({self.document["order_num"]: self.record})
        store.state["project"] = project
        before = deepcopy(store.state)
        result = verify_backfill(store=store, project=project, expected_count=1,
                                 read_order=lambda _: self.current, shipped_status_ids={4})
        self.assertEqual(before, store.state)
        return result

    def test_confirmed_noncollection_closes_obligation_while_original_preparation_stays_unknown(self):
        result = self.verify()
        self.assertTrue(result["ok"])
        row = result["orders"][0]
        self.assertEqual("reviewed_uncollected_obligation_closed", row["outcome"])
        self.assertEqual("prepare_ambiguous", row["journal"]["phase"])
        self.assertEqual("unknown", row["financial_outcome"])
        self.assertEqual("closed", row["invoice_obligation"])

    def test_arbitrary_uncertainty_invalid_marker_and_wrong_project_cannot_pass(self):
        self.assertFalse(self.verify(project="roy")["ok"])
        for marker in (None, True, {}, {**self.record[self.obligations.CLOSURE_FIELD], "state": "uncertain"}):
            self.record[self.obligations.CLOSURE_FIELD] = marker
            self.assertFalse(self.verify()["ok"])

    def test_fresh_status_identity_document_or_missing_detail_is_not_an_accepted_closure(self):
        baseline = deepcopy(self.current)
        for updates in ({"id": "999"}, {"order_num": "999"}, {"status": {"id": "4", "name": "Odoslaná"}},
                        {"invoices": [{"id": "901"}]}, {"preinvoices": [{"id": "902"}]}, {"blocked": True}):
            self.current = {**baseline, **updates}
            self.assertFalse(self.verify()["ok"])
        for field in baseline:
            self.current = deepcopy(baseline)
            self.current.pop(field)
            self.assertFalse(self.verify()["ok"])
        self.current = None
        self.assertFalse(self.verify()["ok"])

    def test_changed_financial_projection_and_open_review_remain_unresolved(self):
        self.record["status_review"] = {"state": "open", "reason": "reviewed_closure_regression"}
        self.assertFalse(self.verify()["ok"])
        self.record.pop("status_review")
        self.record["phase"] = "complete"
        self.assertFalse(self.verify()["ok"])
