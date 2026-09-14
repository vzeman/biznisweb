from copy import deepcopy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import order_status_identity as identity
from order_status_safety import fetch_order_safety_context, refresh_status_catalogue, change_status_verified
from scripts import record_verified_manual_settlement as recorder


def current_catalogue():
    return [{"id": key, "name": labels[-1]} for key, (_, labels) in identity.REVIEWED_RENAMES["vevo"].items()] + [
        {"id": "80", "name": "Untouched"}]


class Client:
    def __init__(self):
        self.transport = SimpleNamespace(url="https://vevo.flox.sk/api/graphql", retries=0)
        self.rows = current_catalogue()
        self.calls = []

    def execute(self, query, variable_values=None):
        self.calls.append(deepcopy(variable_values))
        if "lang_code" in variable_values:
            return {"listOrderStatuses": deepcopy(self.rows)}
        return {"getOrder": {"id": "101", "order_num": variable_values["order_num"],
                             "status": {"id": "4", "name": "Shipped"}}}


class ReviewedContractTests(unittest.TestCase):
    def setUp(self):
        self.client = Client()
        identity.bind_status_identity(self.client, "vevo")

    def test_confirmed_roles_and_distinct_gateway_identities(self):
        rows = identity.bind_catalogue(self.client, current_catalogue())
        self.assertEqual({"1", "4", "17", "31", "33", "34"}, set(identity.REVIEWED_RENAMES["vevo"]))
        self.assertEqual("Platba online - platnosť vypršala", next(row["name"] for row in rows if row["id"] == "33"))
        self.assertEqual("Platba online - platba zamietnutá", next(row["name"] for row in rows if row["id"] == "34"))
        self.assertEqual("Čaká na vybavenie", next(row["name"] for row in rows if row["id"] == "1"))
        self.assertEqual(4, identity.unique_target(rows, "Odoslaná"))

    def test_only_observed_label_or_documented_transliteration_is_accepted(self):
        for key, (canonical, observed) in identity.REVIEWED_RENAMES["vevo"].items():
            for name in (*observed, canonical):
                rows = current_catalogue()
                next(row for row in rows if row["id"] == key)["name"] = name
                bound = identity.bind_catalogue(self.client, rows)
                self.assertEqual(canonical, next(row["name"] for row in bound if row["id"] == key))
        for label in ("shipped", "SHIPPED", "Shipped now", " Odoslaná", "Payment online - paid"):
            rows = current_catalogue()
            next(row for row in rows if row["id"] == "4")["name"] = label
            with self.subTest(label=label), self.assertRaises(ValueError):
                identity.bind_catalogue(self.client, rows)

    def test_complete_scan_can_keep_inactive_unbound_row_but_detail_cannot_use_it(self):
        identity.bind_catalogue(self.client, current_catalogue())
        original = {"order_num": "900001", "status": {"id": "900", "name": "Odoslaná"}}
        result = identity.canonical_order(self.client, original, inventory=True)
        self.assertTrue(result["status_identity_unbound"])
        self.assertEqual(original["status"], result["status"])
        self.assertNotIn("status_identity_unbound", original)
        with self.assertRaises(ValueError):
            identity.canonical_order(self.client, original)
        with self.assertRaises(ValueError):
            identity.canonical_order(self.client, result)

    def test_many_fresh_orders_use_one_catalogue_and_refresh_failure_invalidates_authority(self):
        for index in range(20):
            value = fetch_order_safety_context(self.client, str(index + 1))
            self.assertEqual("Odoslaná", value["status"]["name"])
            self.assertEqual("Shipped", value["raw_status"]["name"])
        self.assertEqual(1, sum("lang_code" in row for row in self.client.calls))
        self.client.rows = self.client.rows[:-3]  # A reviewed role disappears.
        with self.assertRaises(ValueError):
            refresh_status_catalogue(self.client)
        self.assertFalse(identity.identity_for(self.client).catalogue)
        with self.assertRaises(ValueError):
            identity.canonical_order(self.client, value)

    def test_missing_project_policy_blocks_real_transport_before_status_request(self):
        client = Client()
        with self.assertRaisesRegex(RuntimeError, "project identity"):
            change_status_verified(client, "900001", 4, "Odoslaná")
        self.assertEqual([], client.calls)

    def test_cross_project_url_and_changed_transport_are_rejected(self):
        with self.assertRaises(ValueError):
            identity.bind_status_identity(Client(), "roy")
        self.client.transport.url = "https://roy.flox.sk/api/graphql"
        with self.assertRaises(ValueError):
            identity.bind_catalogue(self.client, current_catalogue())


class ExplicitTargetIdentityTests(unittest.TestCase):
    def setUp(self):
        self.label = "Platba online - zaplatené"
        self.rows = [{"id": "31", "name": self.label}, {"id": "67", "name": self.label}]

    def test_reviewed_id_disambiguates_duplicate_names_independently_of_catalogue_order(self):
        for rows in (self.rows, list(reversed(self.rows))):
            self.assertEqual(67, identity.unique_target(rows, self.label, 67))
            self.assertEqual(31, identity.unique_target(rows, self.label, "31"))
            with self.assertRaisesRegex(ValueError, "target_missing_or_ambiguous"):
                identity.unique_target(rows, self.label)

    def test_pinned_id_still_requires_matching_label_and_valid_unique_catalogue(self):
        for rows, name, target in (
            (self.rows, self.label, 99),
            (self.rows, "Odoslaná", 67),
            (self.rows, self.label, True),
            (self.rows, self.label, "067"),
            (self.rows + [self.rows[1]], self.label, 67),
            ([{"id": "31", "name": self.label}, {"id": "67", "name": "Stripe - unpaid"}], self.label, 67),
        ):
            with self.subTest(rows=rows, target=target), self.assertRaises(ValueError):
                identity.unique_target(rows, name, target)

    def test_both_real_roy_recovery_resolvers_use_reviewed_stripe_target(self):
        from generate_invoices import _resolve_existing_invoice_target_status_id, resolve_invoice_generation_settings
        from unpaid_order_cancellation import resolve_recovery_target_status_id, resolve_unpaid_cancellation_settings
        settings = json.loads((Path(__file__).resolve().parents[1] / "projects/roy/settings.json").read_text(encoding="utf-8"))
        client = SimpleNamespace(transport=SimpleNamespace(url="https://roy.flox.sk/api/graphql"),
                                 execute=Mock(return_value={"listOrderStatuses": self.rows}))
        identity.bind_status_identity(client, "roy", settings)
        invoice = resolve_invoice_generation_settings(settings)["existing_invoice_status_reconciliation"]
        cancellation = resolve_unpaid_cancellation_settings(settings)
        self.assertEqual(67, _resolve_existing_invoice_target_status_id(client, invoice))
        self.assertEqual(67, resolve_recovery_target_status_id(client, cancellation))
        self.assertEqual(2, client.execute.call_count)
        self.assertTrue(all("ListOrderStatuses" in str(call.args[0]) for call in client.execute.call_args_list))


class RecorderClientLifecycleTests(unittest.TestCase):
    def test_every_created_aws_client_and_order_transport_close_on_success_or_failure(self):
        for failure in (None, "account", "proof", "record"):
            with self.subTest(failure=failure):
                clients = {name: Mock() for name in ("sts", "s3", "secretsmanager")}
                clients["sts"].get_caller_identity.return_value = {"Account": "foreign" if failure == "account" else recorder.ACCOUNT}
                clients["secretsmanager"].get_secret_value.return_value = {"SecretString": json.dumps({
                    "BIZNISWEB_API_URL": "https://vevo.flox.sk/api/graphql", "BIZNISWEB_API_TOKEN": "fixture"})}
                session = Mock(client=Mock(side_effect=lambda name, **kwargs: clients[name]))
                api = SimpleNamespace(transport=SimpleNamespace(close=Mock()))
                with patch("boto3.Session", return_value=session), patch.object(recorder, "require_source"), \
                        patch.object(recorder, "require_private_bucket"), \
                        patch.object(recorder, "load_reference", side_effect=ValueError("fixture") if failure == "proof" else None, return_value={}), \
                        patch.object(recorder, "resolve_automation_state_location", return_value=(recorder.BUCKET, "data/vevo/order-automation/state.json")), \
                        patch.object(recorder, "record_proof", side_effect=ValueError("fixture") if failure == "record" else None, return_value={"ok": True}), \
                        patch("unpaid_order_cancellation.build_client", return_value=api), redirect_stdout(io.StringIO()):
                    if failure:
                        with self.assertRaises(ValueError):
                            recorder.main(["--project", "vevo", "--proof-sha256", "a" * 64])
                    else:
                        self.assertEqual(0, recorder.main(["--project", "vevo", "--proof-sha256", "a" * 64]))
                created = [call.args[0] for call in session.client.call_args_list]
                for name, client in clients.items():
                    self.assertEqual(int(name in created), client.close.call_count)
                self.assertEqual(int(failure not in {"account", "proof"}), api.transport.close.call_count)
