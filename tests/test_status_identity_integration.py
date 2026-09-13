"""Independent status-ID integration checks using synthetic provider responses."""
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from generate_invoices import InvoiceGenerator
from invoice_automation_state import S3AutomationStateStore
import order_status_identity as identities
import order_status_safety as safety
import reviewed_invoice_obligations as obligations
from scripts import close_reviewed_uncollected_obligation as closure
from tests.test_invoice_automation_state import MemoryS3
from tests.test_reviewed_invoice_obligations import confirmation_fixture
from tests import test_reviewed_uncollected_closure as closure_fixtures


# Synthetic presentation names deliberately collide while status roles differ.
RENAMES = {
    "4": ("Odoslaná", ("Odoslaná", "Shipped")),
    "33": ("GoPay - platba selhala", ("GoPay - platba selhala", "Same visible payment label")),
    "34": ("GoPay - zrušeno", ("GoPay - zrušeno", "Same visible payment label")),
    "74": ("Storno", ("Storno", "Cancelled")),
}


def catalogue():
    return [{"id": key, "name": value[1][-1]} for key, value in RENAMES.items()]


class RawStatusClient:
    def __init__(self, document=None):
        document = document or confirmation_fixture()
        self.transport = SimpleNamespace(url="https://vevo.flox.sk/api/graphql", retries=0, close=Mock())
        self.catalogue = catalogue()
        self.calls = []
        self.mutations = []
        self.commit_then_error = False
        self.no_commit_error = False
        self.after_mutation = lambda *_: None
        self.order = {
            "id": document["order_id"], "order_num": document["order_num"], "blocked": False,
            "status": {"id": "4", "name": "Shipped"}, "last_change": "2026-09-13 06:40:00",
            "pur_date": "2026-01-01 10:00:00", "invoices": [], "preinvoices": [], "shipments": [],
            "sum": {"value": document["total"]["value"], "formatted": "fixture EUR",
                    "currency": {"code": "EUR"}, "is_net_price": False},
            "price_elements": [{"type": "payment", "reference_id": document["payment_reference_id"], "title": "Dobierka"}],
        }

    def execute(self, query, variable_values=None):
        values = variable_values or {}
        self.calls.append(deepcopy(values))
        if "lang_code" in values:
            return {"listOrderStatuses": deepcopy(self.catalogue)}
        if "status_id" in values:
            self.mutations.append(deepcopy(values))
            if not self.no_commit_error:
                self.order["status"] = deepcopy(next(row for row in self.catalogue if str(row["id"]) == str(values["status_id"])))
                self.order["last_change"] = "2026-09-13 07:10:00"
            self.after_mutation(self)
            if self.commit_then_error or self.no_commit_error:
                raise TimeoutError("synthetic lost status response")
            return {"changeOrderStatus": deepcopy(self.order)}
        return {"getOrder": deepcopy(self.order)}


class StatusIdentityIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(identities, "REVIEWED_RENAMES", {"vevo": deepcopy(RENAMES)}))
        self.client = RawStatusClient()
        identities.bind_status_identity(self.client, "vevo", {"biznisweb_api_url": self.client.transport.url})

    def bind(self):
        return identities.bind_catalogue(self.client, self.client.catalogue)

    def test_colliding_visible_labels_keep_distinct_reviewed_roles_and_raw_evidence(self):
        canonical = self.bind()
        self.assertEqual(33, identities.unique_target(canonical, "GoPay - platba selhala"))
        self.assertEqual(34, identities.unique_target(canonical, "GoPay - zrušeno"))
        for identity, canonical_name in (("33", "GoPay - platba selhala"), ("34", "GoPay - zrušeno")):
            raw = {**self.client.order, "status": {"id": identity, "name": "Same visible payment label"}}
            before = deepcopy(raw)
            result = identities.canonical_order(self.client, raw)
            self.assertEqual(canonical_name, result["status"]["name"])
            self.assertEqual(before["status"], result["raw_status"])
            self.assertEqual(identity, str(result["status"]["id"]))
            self.assertEqual(before, raw)
            self.assertEqual(result, identities.canonical_order(self.client, result))
        with self.assertRaises(ValueError):
            identities.unique_target(canonical, "Same visible payment label")

    def test_catalogue_duplicate_id_and_unknown_managed_label_fail_closed(self):
        for bad in (self.client.catalogue + [self.client.catalogue[0]], self.client.catalogue[1:],
                    [{**row, "name": "Unsupported new name"} if row["id"] == "4" else row for row in self.client.catalogue]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                identities.bind_catalogue(self.client, bad)

    def test_failed_catalogue_refresh_invalidates_old_authority(self):
        self.bind()
        with self.assertRaises(ValueError):
            identities.bind_catalogue(self.client, self.client.catalogue[1:])
        with self.assertRaisesRegex(ValueError, "catalogue"):
            identities.canonical_order(self.client, self.client.order)

    def test_foreign_project_cannot_accept_precanonicalized_vevo_row(self):
        self.bind()
        row = identities.canonical_order(self.client, self.client.order)
        other = RawStatusClient()
        other.transport.url = "https://roy.flox.sk/api/graphql"
        identities.bind_status_identity(other, "roy", {"biznisweb_api_url": other.transport.url})
        with self.assertRaises(ValueError):
            identities.canonical_order(other, row)

    def test_transport_project_and_repeated_configuration_binding_cannot_drift(self):
        with self.assertRaises(ValueError):
            identities.bind_status_identity(self.client, "roy")
        with self.assertRaises(ValueError):
            identities.bind_status_identity(self.client, "vevo", {"biznisweb_api_url": "https://roy.flox.sk/api/graphql"})
        self.client.transport = SimpleNamespace(url="https://vevo.flox.sk/api/graphql", retries=0)
        with self.assertRaises(ValueError):
            identities.identity_for(self.client)

    def test_canonical_metadata_and_raw_identity_cannot_be_rebound(self):
        self.bind()
        before = identities.canonical_order(self.client, self.client.order)
        for update in ({"raw_status": {"id": "34", "name": "Same visible payment label"}},
                       {"status": {"id": "4", "name": "Storno"}},
                       {"status_identity_contract": {"project": "roy", "version": identities.CONTRACT_VERSION}}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                identities.canonical_order(self.client, {**before, **update})

    def test_shared_reader_canonicalizes_copy_while_retaining_provider_status(self):
        before = deepcopy(self.client.order)
        result = safety.fetch_order_safety_context(self.client, before["order_num"])
        self.assertEqual({"id": "4", "name": "Odoslaná"}, result["status"])
        self.assertEqual(before["status"], result["raw_status"])
        self.assertEqual(before, self.client.order)
        self.assertTrue(any("lang_code" in call for call in self.client.calls))

    def test_actual_status_ack_and_independent_readback_use_same_bound_identity(self):
        result = safety.change_status_verified(self.client, self.client.order["order_num"], 74, "Storno", silent=True)
        self.assertEqual({"id": "74", "name": "Storno"}, result["status"])
        self.assertEqual({"id": "74", "name": "Cancelled"}, result["raw_status"])
        self.assertEqual(1, len(self.client.mutations))
        self.assertEqual([{"type": "EMAIL_CUSTOMER", "if": ["NONE"]}], self.client.mutations[0]["send_notification"])

    def test_lost_status_response_reads_back_once_without_second_mutation(self):
        self.client.commit_then_error = True
        result = safety.change_status_verified(self.client, self.client.order["order_num"], 74, "Storno", silent=True)
        self.assertEqual("Storno", result["status"]["name"])
        self.assertEqual(1, len(self.client.mutations))

    def test_catalogue_drift_after_committed_request_stays_unresolved_without_replay(self):
        self.client.after_mutation = lambda client: client.catalogue[-1].update(name="Unsupported post-request name")
        with self.assertRaises(ValueError):
            safety.change_status_verified(self.client, self.client.order["order_num"], 74, "Storno", silent=True)
        self.assertEqual(1, len(self.client.mutations))
        self.assertFalse(identities.identity_for(self.client).catalogue)

    def test_generator_own_readers_retain_raw_status_without_changing_provider_rows(self):
        generator = InvoiceGenerator(self.client.transport.url, "fixture-token", "https://vevo.flox.sk",
                                     eligible_statuses=["Odoslaná"], page_delay_seconds=0, read_attempts=1)
        generator.client = self.client
        before = deepcopy(self.client.order)
        self.assertEqual({4}, generator.resolve_eligible_status_ids())
        for reader in (generator.fetch_order_for_invoice, generator.fetch_order_safety_context):
            current = reader(before["order_num"])
            self.assertEqual({"id": "4", "name": "Odoslaná"}, current["status"])
            self.assertEqual(before["status"], current["raw_status"])
            self.assertTrue(generator._status_is_eligible(current))
        self.assertEqual(before, self.client.order)

    def test_generator_cannot_qualify_unrelated_id_reusing_reviewed_canonical_role(self):
        generator = InvoiceGenerator(self.client.transport.url, "fixture-token", "https://vevo.flox.sk",
                                     eligible_statuses=["Odoslaná"], page_delay_seconds=0, read_attempts=1)
        generator.client = self.client
        self.client.catalogue.append({"id": "99", "name": "Odoslaná"})
        unrelated = {**deepcopy(self.client.order), "status": {"id": "99", "name": "Odoslaná"}}
        try:
            eligible = generator._status_is_eligible(unrelated)
        except ValueError:
            eligible = False
        self.assertFalse(eligible)
        self.assertEqual([], self.client.mutations)

    def test_unlisted_inventory_status_preserves_row_but_never_qualifies_by_name(self):
        self.bind()
        raw = {**deepcopy(self.client.order), "status": {"id": "99", "name": "Odoslaná"}}
        before = deepcopy(raw)
        inventory = identities.canonical_order(self.client, raw, inventory=True)
        self.assertTrue(inventory["status_identity_unbound"])
        self.assertEqual(before["status"], inventory["status"])
        self.assertEqual(before["order_num"], inventory["order_num"])
        self.assertEqual(before, raw)
        generator = InvoiceGenerator(self.client.transport.url, "fixture-token", "https://vevo.flox.sk",
                                     eligible_statuses=["Odoslaná"], page_delay_seconds=0, read_attempts=1)
        generator.client = self.client
        self.assertFalse(generator._status_is_eligible(inventory))
        with self.assertRaises(ValueError):
            identities.canonical_order(self.client, raw)

    def test_unapproved_target_catalogue_before_request_does_not_write(self):
        self.bind()
        self.client.catalogue[-1]["name"] = "Unexpected target rename"
        with self.assertRaises(ValueError):
            safety.change_status_verified(self.client, self.client.order["order_num"], 74, "Storno", silent=True)
        self.assertEqual([], self.client.mutations)

    def test_wrong_same_visible_role_in_ack_is_unresolved_without_replay(self):
        self.client.after_mutation = lambda client: client.order.update(status={"id": "34", "name": "Same visible payment label"})
        with self.assertRaises((ValueError, RuntimeError)):
            safety.change_status_verified(self.client, self.client.order["order_num"], 33, "GoPay - platba selhala", silent=True)
        self.assertEqual(1, len(self.client.mutations))


class StatusIdentityClosureIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.document = confirmation_fixture()
        self.enterContext(patch.object(identities, "REVIEWED_RENAMES", {"vevo": deepcopy(RENAMES)}))
        self.enterContext(patch.multiple(obligations, CONFIRMATION_KEY="data/vevo/order-automation/audits/fixture.json",
                          CONFIRMATION_SHA256="e" * 64,
                          CONFIRMATION_CANONICAL_SHA256=obligations.canonical_sha256(self.document)))
        self.client = RawStatusClient(self.document)
        identities.bind_status_identity(self.client, "vevo", {"biznisweb_api_url": self.client.transport.url})
        self.generator = InvoiceGenerator(self.client.transport.url, "fixture-token", "https://vevo.flox.sk",
                                          page_delay_seconds=0, read_attempts=1, send_invoice_email=False)
        self.generator.client = self.client
        self.generator.web_session = closure_fixtures.FakeWeb()
        self.generator.arf_token = "fixtureToken"
        self.store = S3AutomationStateStore(MemoryS3(), "private", "data/vevo/order-automation/state.json", "vevo",
                                            now=lambda: datetime(2026, 9, 13, 8, tzinfo=timezone.utc))
        state = self.store.empty_state()
        state["orders"][self.document["order_num"]] = deepcopy(self.document["original_financial_operation"])
        self.store.put(state, "")

    def run_helper(self, apply=False):
        return closure.close_obligation(store=self.store, document=self.document,
                                        generator_factory=lambda: self.generator, apply=apply,
                                        source_commit="d" * 40, release_gate=Mock())

    def test_closure_preview_binds_renamed_source_without_journal_writes(self):
        before = self.store.read()[0]
        result = self.run_helper()
        self.assertTrue(result["ok"])
        self.assertEqual(before, self.store.read()[0])
        self.assertEqual([], self.client.mutations)

    def test_closure_once_only_status_write_and_readback_preserve_original_unknown(self):
        result = self.run_helper(True)
        self.assertTrue(result["ok"])
        self.assertEqual((1, 0, 0), (result["status_requests"], result["financial_requests"], result["emailed_invoices"]))
        record = self.store.read()[0]["orders"][self.document["order_num"]]
        self.assertTrue(obligations.closure_decision(record, project="vevo").closed)
        marker = record[obligations.CLOSURE_FIELD]
        mutation = marker["status_mutation"]
        self.assertEqual({"id": "4", "name": "Shipped"}, mutation["source_raw_status"])
        self.assertEqual({"id": "74", "name": "Cancelled"}, mutation["target_raw_status"])
        self.assertEqual({"project": "vevo", "version": identities.CONTRACT_VERSION}, mutation["status_identity_contract"])
        self.assertEqual(self.document, marker["confirmation"]["document"])
        self.assertEqual(self.document["original_financial_operation"], obligations.financial_projection(record))
        repeated = self.run_helper(True)
        self.assertTrue(repeated["ok"])
        self.assertEqual(0, repeated["status_requests"])
        self.assertEqual(1, len(self.client.mutations))

    def test_closed_marker_fresh_renamed_target_remains_closed_and_regression_visible(self):
        self.assertTrue(self.run_helper(True)["ok"])
        record = self.store.read()[0]["orders"][self.document["order_num"]]
        current = self.generator.fetch_order_for_invoice(self.document["order_num"])
        decision = obligations.closure_decision(record, project="vevo", current_order=current)
        self.assertTrue(decision.closed)
        self.assertFalse(decision.regression)
        self.client.order["status"] = {"id": "4", "name": "Shipped"}
        current = self.generator.fetch_order_for_invoice(self.document["order_num"])
        regressed = obligations.closure_decision(record, project="vevo", current_order=current)
        self.assertTrue(regressed.closed)
        self.assertTrue(regressed.regression)
        self.assertEqual("reviewed_noncollection_requires_manual_review", safety.status_write_block_reason(
            record, current_order=current, project="vevo"))
        self.assertEqual(self.document["original_financial_operation"], obligations.financial_projection(record))
        self.assertEqual(1, len(self.client.mutations))

    def test_uncertain_closure_under_renamed_status_is_never_replayed(self):
        self.client.no_commit_error = True
        result = self.run_helper(True)
        self.assertFalse(result["ok"])
        repeated = self.run_helper(True)
        self.assertFalse(repeated["ok"])
        self.assertEqual(0, repeated["status_requests"])
        self.assertEqual(1, len(self.client.mutations))
