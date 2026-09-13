from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import unittest
from unittest.mock import patch

import reviewed_invoice_obligations as obligations


def confirmation_fixture():
    original = dict.fromkeys(obligations.FINANCIAL_FIELDS)
    original.update(
        order_num="900001", order_id="101", phase="prepare_ambiguous",
        attempted_at="2026-09-13T06:31:00+00:00", email_policy="hold",
        last_creation_failure={"stage": "preparation", "http_status": None, "kind": "unconfirmed"},
    )
    return {
        "schema_version": 1, "kind": "reviewed_uncollected_cod", "project": "vevo",
        "order_id": "101", "order_num": "900001", "payment_reference_id": "7",
        "source_status": {"id": "4", "name": "Odoslaná"},
        "total": {"value": "15", "currency": "EUR"},
        "confirmation": {
            "source": "explicit_user_confirmation", "outcome": "uncollected",
            "resolution": "correct_status_and_close_invoice_obligation",
            "confirmed_at": "2026-09-13T06:45:00+00:00",
        },
        "evidence_refs": [
            {"role": role, "key": f"data/vevo/order-automation/audits/2026-09-13/fixture-{role}.json",
             "sha256": str(index) * 64}
            for index, role in enumerate(("api", "history", "preparation_uncertainty"), 1)
        ],
        "original_financial_operation": original,
    }


def closure_fixture(document, key, raw_sha):
    operation = {
        "schema_version": 1, "intent_id": "a" * 32, "state": "verified", "silent": True,
        "attempted_at": "2026-09-13T07:00:00+00:00",
        "source_status": deepcopy(document["source_status"]),
        "target_status": {"id": "74", "name": "Neprevzaté - Storno"},
    }
    marker = {
        "schema_version": 1, "state": "verified", "invoice_obligation": "closed",
        "financial_outcome": "unknown", "verified_at": "2026-09-13T07:01:00+00:00",
        "confirmation": {"key": key, "sha256": raw_sha, "document": deepcopy(document)},
        "original_financial_sha256": obligations.canonical_sha256(document["original_financial_operation"]),
        "status_mutation": deepcopy(operation),
    }
    return {**deepcopy(document["original_financial_operation"]),
            "status_mutation": operation, obligations.CLOSURE_FIELD: marker}


class ReviewedInvoiceObligationTests(unittest.TestCase):
    def setUp(self):
        self.document = confirmation_fixture()
        self.key = "data/vevo/order-automation/audits/2026-09-13/fixture-confirmation.json"
        self.raw_sha = hashlib.sha256(json.dumps(self.document, indent=2).encode()).hexdigest()
        self.pins = patch.multiple(
            obligations, CONFIRMATION_KEY=self.key, CONFIRMATION_SHA256=self.raw_sha,
            CONFIRMATION_CANONICAL_SHA256=obligations.canonical_sha256(self.document),
        )
        self.pins.start()
        self.addCleanup(self.pins.stop)
        self.record = closure_fixture(self.document, self.key, self.raw_sha)
        self.current = {
            "id": "101", "order_num": "900001", "status": {"id": 74, "name": "Neprevzaté - Storno"},
            "blocked": False, "sum": {"value": "15", "currency": {"code": "EUR"}},
            "invoices": [], "preinvoices": [],
        }

    def decision(self, record=None, **kwargs):
        return obligations.closure_decision(self.record if record is None else record,
                                            project="vevo", order_num="900001", **kwargs)

    def assert_invalid(self, record):
        decision = self.decision(record)
        self.assertTrue(decision.present)
        self.assertFalse(decision.closed)
        self.assertEqual("reviewed_closure_invalid", decision.reason)

    def test_sealed_confirmation_and_verified_closure_preserve_original_uncertainty(self):
        before = deepcopy(self.record)
        self.assertTrue(obligations.validate_confirmation(self.document))
        decision = self.decision(current_order=self.current)
        self.assertEqual(obligations.ClosureDecision(True, True, "reviewed_obligation_closed"), decision)
        self.assertEqual(before, self.record)
        self.assertEqual("prepare_ambiguous", self.record["phase"])
        self.assertEqual("unknown", self.record[obligations.CLOSURE_FIELD]["financial_outcome"])
        self.assertEqual(self.document["original_financial_operation"], obligations.financial_projection(self.record))

    def test_no_marker_is_distinct_from_invalid_marker_presence(self):
        for value in (None, False, [], {}, {"phase": "prepare_ambiguous"}):
            with self.subTest(value=value):
                self.assertFalse(obligations.has_reviewed_closure(value))
                result = obligations.closure_decision(value, project="vevo")
                self.assertFalse(result.present)
                self.assertFalse(result.closed)
        for marker in (None, False, True, [], "closed", 1, {}):
            with self.subTest(marker=marker):
                record = {**self.record, obligations.CLOSURE_FIELD: marker}
                self.assertTrue(obligations.has_reviewed_closure(record))
                self.assert_invalid(record)

    def test_empty_or_changed_pins_cannot_close(self):
        for field, value in (
            ("CONFIRMATION_KEY", ""), ("CONFIRMATION_SHA256", ""),
            ("CONFIRMATION_CANONICAL_SHA256", ""), ("CONFIRMATION_SHA256", "f" * 64),
            ("CONFIRMATION_KEY", self.key.replace("vevo", "roy")),
        ):
            with self.subTest(field=field, value=value), patch.object(obligations, field, value):
                self.assert_invalid(self.record)

    def test_mutated_confirmation_scope_or_proof_cannot_use_old_canonical_pin(self):
        changes = {
            "project": "roy", "order_id": "102", "order_num": "900002", "payment_reference_id": "8",
            "total": {"value": "16", "currency": "EUR"},
            "confirmation": {**self.document["confirmation"], "outcome": "paid"},
            "source_status": {"id": "5", "name": "Odoslaná"},
            "evidence_refs": list(reversed(self.document["evidence_refs"])),
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                record = deepcopy(self.record)
                record[obligations.CLOSURE_FIELD]["confirmation"]["document"][field] = value
                self.assert_invalid(record)

    def test_confirmation_references_require_exact_raw_hash_and_key(self):
        for field, value in (("key", self.key + ".other"), ("sha256", "0" * 64),
                             ("sha256", obligations.canonical_sha256(self.document))):
            with self.subTest(field=field, value=value):
                record = deepcopy(self.record)
                record[obligations.CLOSURE_FIELD]["confirmation"][field] = value
                self.assert_invalid(record)

    def test_every_original_financial_field_remains_bound(self):
        for field in obligations.FINANCIAL_FIELDS:
            with self.subTest(field=field):
                record = deepcopy(self.record)
                record[field] = "changed"
                self.assert_invalid(record)
        record = deepcopy(self.record)
        record[obligations.CLOSURE_FIELD]["original_financial_sha256"] = "f" * 64
        self.assert_invalid(record)

    def test_nonfinancial_journal_metadata_does_not_reopen_closure(self):
        record = deepcopy(self.record)
        record.update(updated_at="2026-09-14T00:00:00+00:00", unrelated_audit={"seen": True})
        self.assertTrue(self.decision(record).closed)

    def test_duplicate_read_is_idempotent_but_cross_order_or_project_marker_is_invalid(self):
        self.assertEqual(self.decision(), self.decision(deepcopy(self.record)))
        for project, number in (("roy", "900001"), (None, "900001"), ("vevo", "900002")):
            with self.subTest(project=project, number=number):
                result = obligations.closure_decision(self.record, project=project, order_num=number)
                self.assertTrue(result.present)
                self.assertFalse(result.closed)
        record = deepcopy(self.record)
        record["order_num"] = "900002"
        self.assert_invalid(record)

    def test_intent_and_uncertain_outcomes_never_hide_active_obligation(self):
        for state in ("intent", "uncertain"):
            with self.subTest(state=state):
                record = deepcopy(self.record)
                marker = record[obligations.CLOSURE_FIELD]
                marker.update(state=state, invoice_obligation="closing")
                marker["status_mutation"]["state"] = record["status_mutation"]["state"] = state
                decision = self.decision(record)
                self.assertEqual(obligations.ClosureDecision(True, False, "reviewed_closure_unconfirmed"), decision)
                self.assertEqual("prepare_ambiguous", record["phase"])

    def test_verified_marker_requires_same_verified_silent_status_intent(self):
        for field, value in (
            ("state", "uncertain"), ("state", "intent"), ("state", "failed"),
            ("silent", False), ("silent", 1), ("schema_version", True),
            ("intent_id", None), ("intent_id", "a" * 31),
            ("source_status", {"id": "5", "name": "Odoslaná"}),
            ("target_status", {"id": "4", "name": "Storno"}),
            ("target_status", {"id": "74", "name": "Odoslaná"}),
        ):
            with self.subTest(field=field, value=value):
                record = deepcopy(self.record)
                record[obligations.CLOSURE_FIELD]["status_mutation"][field] = value
                record["status_mutation"][field] = value
                self.assert_invalid(record)
        record = deepcopy(self.record)
        record["status_mutation"]["intent_id"] = "b" * 32
        self.assert_invalid(record)

    def test_closure_marker_cannot_claim_financial_success_or_mismatched_business_outcome(self):
        for field, value in (
            ("schema_version", True), ("financial_outcome", "confirmed"),
            ("state", "complete"), ("invoice_obligation", "pending"),
            ("confirmation", None), ("original_financial_sha256", None),
        ):
            with self.subTest(field=field, value=value):
                record = deepcopy(self.record)
                record[obligations.CLOSURE_FIELD][field] = value
                self.assert_invalid(record)

    def test_confirmation_intent_and_readback_timestamps_require_aware_ordering(self):
        for stamp in (None, "bad", "2026-09-13T07:01:00", "2026-09-13T06:59:59+00:00"):
            with self.subTest(verified_at=stamp):
                record = deepcopy(self.record)
                record[obligations.CLOSURE_FIELD]["verified_at"] = stamp
                self.assert_invalid(record)
        for stamp in ("2026-09-13T06:44:59+00:00", "2026-09-13T07:00:00", None):
            with self.subTest(attempted_at=stamp):
                record = deepcopy(self.record)
                record[obligations.CLOSURE_FIELD]["status_mutation"]["attempted_at"] = stamp
                record["status_mutation"]["attempted_at"] = stamp
                self.assert_invalid(record)

    def test_status_or_late_document_regressions_remain_closed_and_visible(self):
        for field, value in (
            ("status", {"id": "4", "name": "Odoslaná"}),
            ("invoices", [{"id": "101", "invoice_num": "990001"}]),
            ("preinvoices", [{"id": "101"}]),
        ):
            with self.subTest(field=field):
                current = {**deepcopy(self.current), field: value}
                result = self.decision(current_order=current)
                self.assertTrue(result.closed)
                self.assertTrue(result.regression)
                self.assertEqual("reviewed_closure_regression", result.reason)

    def test_malformed_current_context_fails_closed_without_exception(self):
        for status in ([], ["bad"], "Storno", 74, True):
            with self.subTest(status=status):
                result = self.decision(current_order={**self.current, "status": status})
                self.assertTrue(result.present)
                self.assertTrue(result.regression or not result.closed)

    def test_current_identity_drift_keeps_original_closed_but_marks_context_unsafe(self):
        for field, value in (("id", "102"), ("order_num", "900002")):
            with self.subTest(field=field):
                result = self.decision(current_order={**self.current, field: value})
                self.assertEqual(obligations.ClosureDecision(True, True, "reviewed_closure_regression", True), result)
        for current in (False, [], "wrong context"):
            with self.subTest(current=current):
                result = self.decision(current_order=current)
                self.assertTrue(result.closed)
                self.assertTrue(result.regression)

    def test_confirmation_schema_rejects_malformed_values_even_when_canonical_pin_matches(self):
        changes = (
            ("schema_version", True), ("order_id", True), ("order_num", "0900001"),
            ("payment_reference_id", 7), ("project", "roy"),
            ("total", {"value": "NaN", "currency": "EUR"}),
            ("total", {"value": "Infinity", "currency": "EUR"}),
            ("total", {"value": "0", "currency": "EUR"}),
            ("total", {"value": True, "currency": "EUR"}),
            ("total", {"value": "15", "currency": "USD"}),
            ("source_status", {"id": 4, "name": "Odoslaná"}),
            ("confirmation", {**self.document["confirmation"], "confirmed_at": "2026-09-13"}),
            ("evidence_refs", self.document["evidence_refs"][:2]),
            ("original_financial_operation", {**self.document["original_financial_operation"], "phase": "complete"}),
        )
        for field, value in changes:
            with self.subTest(field=field, value=value):
                document = {**deepcopy(self.document), field: value}
                with patch.object(obligations, "CONFIRMATION_CANONICAL_SHA256", obligations.canonical_sha256(document)):
                    self.assertFalse(obligations.validate_confirmation(document))

    def test_reference_roles_scope_path_and_hash_are_validated_before_seal(self):
        for field, value in (("role", "history"), ("key", "data/roy/order-automation/test.json"),
                             ("key", "data/vevo/order-automation/../test.json"),
                             ("key", "data/vevo/order-automation/test.txt"), ("sha256", None),
                             ("sha256", "not-a-hash")):
            with self.subTest(field=field, value=value):
                document = deepcopy(self.document)
                document["evidence_refs"][0][field] = value
                with patch.object(obligations, "CONFIRMATION_CANONICAL_SHA256", obligations.canonical_sha256(document)):
                    self.assertFalse(obligations.validate_confirmation(document))

    def test_noncollection_target_normalization_is_narrow(self):
        for name in ("Storno", "Neprevzaté - Storno", " NEPREVZATÉ – STORNO "):
            self.assertTrue(obligations.is_noncollection_target({"id": "74", "name": name}))
        for value in (None, "Storno", {"id": True, "name": "Storno"}, {"id": "74", "name": "Odoslaná"},
                      {"id": "74", "name": "Storno refund paid"}):
            self.assertFalse(obligations.is_noncollection_target(value))

    def test_datetime_input_cannot_bypass_serializable_timestamp_contract(self):
        record = deepcopy(self.record)
        record[obligations.CLOSURE_FIELD]["verified_at"] = datetime.now(timezone.utc)
        self.assert_invalid(record)


if __name__ == "__main__":
    unittest.main()
