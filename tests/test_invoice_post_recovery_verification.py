from copy import deepcopy
from io import BytesIO, StringIO
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

from gql.transport.exceptions import TransportQueryError, TransportServerError

from generate_invoices import InvoiceGenerator
from scripts import verify_invoice_post_recovery as verifier


def order(identity=1, **changes):
    return {"id": identity, "order_num": f"FIXTURE-{identity}", "pur_date": "2019-01-01",
            "status": {"id": 4, "name": "Odoslaná"}, "blocked": False,
            "invoices": [], "sum": {"value": "100.50"}, **changes}


class ReadOnlyClient:
    def __init__(self, inventory):
        self.inventory = inventory
        self.fresh = {row["order_num"]: deepcopy(row) for row in inventory}
        self.calls = []
        self.scan_error = None
        self.transport = SimpleNamespace(close=Mock())

    def execute(self, query, variable_values):
        document = getattr(query, "document", query)
        if any(definition.operation.value != "query" for definition in document.definitions):
            raise AssertionError("Mutation attempted")
        self.calls.append(deepcopy(variable_values))
        if "lang_code" in variable_values:
            return {"listOrderStatuses": [{"id": 4, "name": "Odoslaná"}]}
        if "order_num" in variable_values:
            value = self.fresh[variable_values["order_num"]]
            if isinstance(value, Exception):
                raise value
            return {"getOrder": deepcopy(value)}
        params = variable_values["params"]
        if params["sort"] == "DESC":
            rows = self.inventory[-1:]
        else:
            if self.scan_error:
                raise self.scan_error
            rows = self.inventory[params["cursor"]:params["cursor"] + params["limit"]]
        end = params["cursor"] + len(rows)
        return {"getOrderList": {"data": deepcopy(rows), "pageInfo": {
            "totalRecords": len(self.inventory), "hasNextPage": end < len(self.inventory),
            "nextCursor": end if end < len(self.inventory) else None, "totalPages": 1}}}


class PostRecoveryVerificationTests(unittest.TestCase):
    def setUp(self):
        self.addCleanup(logging.disable, logging.root.manager.disable)
        logging.disable(logging.CRITICAL)

    def generator(self, inventory):
        generator = InvoiceGenerator("https://roy.flox.sk/api/graphql", "fixture-token", "https://roy.flox.sk",
                                     send_invoice_email=False, page_delay_seconds=0, read_attempts=1)
        generator.client = ReadOnlyClient(inventory)
        return generator

    def verify(self, generator):
        return verifier.verify_post_recovery(generator=generator, project="roy", source_commit="a" * 40)

    def test_empty_complete_inventory_needs_no_baseline_and_closes_transport(self):
        generator = self.generator([])
        report = self.verify(generator)
        self.assertTrue(report["complete"])
        self.assertTrue(report["candidate_rechecks_complete"])
        self.assertTrue(report["ok"])
        self.assertEqual([], report["orders"])
        self.assertEqual(2, len(generator.client.calls))
        generator.client.transport.close.assert_called_once_with()

    def test_all_age_scan_rechecks_only_current_candidates_without_date_filter(self):
        generator = self.generator([order(1), order(2), order(3), order(4, blocked=True),
                                    order(5, sum={"value": 0}), order(6, status=None)])
        generator.client.fresh["FIXTURE-2"]["invoices"] = [{"id": "402", "invoice_num": "2026-FIXTURE"}]
        generator.client.fresh["FIXTURE-3"]["status"] = {"id": 9, "name": "Storno"}
        report = self.verify(generator)
        self.assertFalse(report["ok"])
        self.assertTrue(report["complete"])
        self.assertTrue(report["candidate_rechecks_complete"])
        self.assertEqual(6, report["scanned_orders"])
        self.assertEqual(3, report["eligible_orders"])
        self.assertEqual(1, report["unknown_status_orders"])
        self.assertEqual({"still_eligible": 1, "already_invoiced": 1, "no_longer_eligible": 1}, report["outcome_counts"])
        self.assertEqual(["FIXTURE-1", "FIXTURE-2", "FIXTURE-3"],
                         [call["order_num"] for call in generator.client.calls if "order_num" in call])
        self.assertTrue(all(set(call) == {"params"} for call in generator.client.calls if "params" in call))
        self.assertTrue(all(row["pur_date"] == "2019-01-01" for row in report["orders"]))
        self.assertIsNone(generator.web_session)
        self.assertIsNone(generator.operation_journal)

    def test_readback_can_prove_zero_remaining_without_claiming_empty_initial_scan(self):
        generator = self.generator([order()])
        generator.client.fresh["FIXTURE-1"]["invoices"] = [{"id": "401", "invoice_num": "FINAL-FIXTURE"}]
        report = self.verify(generator)
        self.assertTrue(report["ok"])
        self.assertEqual(1, report["eligible_orders"])
        self.assertEqual({"already_invoiced": 1}, report["outcome_counts"])

    def test_exhausted_candidate_read_stops_further_reads_and_retains_all_unchecked_ids(self):
        generator = self.generator([order(1), order(2), order(3)])
        generator.client.fresh["FIXTURE-1"] = TransportServerError("private-fixture-do-not-publish", code=509)
        report = self.verify(generator)
        self.assertTrue(report["complete"])
        self.assertFalse(report["candidate_rechecks_complete"])
        self.assertFalse(report["ok"])
        self.assertEqual(1, report["checked_count"])
        self.assertEqual(3, report["failed_orders"])
        self.assertEqual({"unverified": 1, "not_checked": 2}, report["outcome_counts"])
        self.assertEqual(["fresh_candidate_read_failed"], report["global_issues"])
        self.assertEqual(1, sum("order_num" in call for call in generator.client.calls))
        self.assertNotIn("private-fixture", json.dumps(report))
        generator.client.transport.close.assert_called_once_with()

    def test_partial_graphql_page_never_becomes_empty_success_or_triggers_candidate_reads(self):
        generator = self.generator([order()])
        generator.client.scan_error = TransportQueryError("private-fixture", data={"getOrderList": {"data": []}})
        report = self.verify(generator)
        self.assertFalse(report["complete"])
        self.assertFalse(report["ok"])
        self.assertEqual(0, report["checked_count"])
        self.assertEqual("TransportQueryError", report["error_type"])
        self.assertNotIn("private-fixture", json.dumps(report))

    def test_missing_schema_invalid_numbers_and_invalid_ids_cannot_pass_full_inventory(self):
        bad_rows = [order(sum={"value": value}) for value in (True, "NaN", "Infinity", None, {})]
        bad_rows += [order(id=value) for value in (True, 0, -1, "")]
        bad_rows += [order(invoices=[{"id": 0}]), order(blocked=None)]
        for field in ("sum", "invoices", "status", "order_num", "id"):
            row = order()
            row.pop(field)
            bad_rows.append(row)
        for row in bad_rows:
            with self.subTest(row=row):
                generator = self.generator([])
                generator.client.inventory = [row]
                report = self.verify(generator)
                self.assertFalse(report["complete"])
                self.assertFalse(report["ok"])
                self.assertEqual(0, report["checked_count"])

    def test_explicit_null_empty_invoice_collection_is_valid(self):
        generator = self.generator([order(invoices=None)])
        report = self.verify(generator)
        self.assertTrue(report["complete"])
        self.assertEqual({"still_eligible": 1}, report["outcome_counts"])

    def test_fresh_internal_or_public_identity_change_and_invalid_money_fail_closed(self):
        for changes in ({"id": 2}, {"order_num": "FOREIGN-FIXTURE"}, {"sum": {"value": True}},
                        {"sum": {"value": "NaN"}}, {"invoices": [{"id": 0}]}):
            with self.subTest(changes=changes):
                generator = self.generator([order()])
                generator.client.fresh["FIXTURE-1"].update(changes)
                report = self.verify(generator)
                self.assertFalse(report["ok"])
                self.assertFalse(report["candidate_rechecks_complete"])
                self.assertEqual({"unverified": 1}, report["outcome_counts"])

    def test_multiple_or_unnumbered_final_documents_require_review(self):
        for invoices, outcome in (([{"id": "401"}, {"id": "402"}], "multiple_final_invoices"),
                                  ([{"id": "401", "invoice_num": ""}], "final_invoice_number_missing")):
            generator = self.generator([order()])
            generator.client.fresh["FIXTURE-1"]["invoices"] = invoices
            report = self.verify(generator)
            self.assertFalse(report["ok"])
            self.assertEqual({outcome: 1}, report["outcome_counts"])

    def test_duplicate_inventory_identity_cannot_be_successful(self):
        for rows in ([order(1), order(1)], [order(1), order(2, order_num="FIXTURE-1")]):
            report = self.verify(self.generator(rows))
            self.assertFalse(report["complete"])
            self.assertFalse(report["ok"])

    def test_mutation_capabilities_are_rejected_before_any_reads(self):
        for attribute in ("web_session", "operation_journal"):
            generator = self.generator([])
            setattr(generator, attribute, object())
            report = self.verify(generator)
            self.assertFalse(report["ok"])
            self.assertEqual([], generator.client.calls)
            generator.client.transport.close.assert_called_once_with()

    def test_transport_close_failure_preserves_completed_evidence_as_failure(self):
        generator = self.generator([])
        generator.client.transport.close.side_effect = RuntimeError("private-close-fixture")
        report = self.verify(generator)
        self.assertTrue(report["complete"])
        self.assertFalse(report["ok"])
        self.assertEqual(["transport_close_failed"], report["global_issues"])
        self.assertNotIn("private-close-fixture", json.dumps(report))

    def test_exact_project_url_is_independent_of_configured_host(self):
        for url in ("https://roy.flox.sk/api/graphql", "https://roy.sk/api/graphql", "https://www.roy.sk:443/api/graphql"):
            verifier.verify_project_url("roy", url)
        for url in ("https://vevo.flox.sk/api/graphql", "http://roy.flox.sk/api/graphql",
                    "https://roy.flox.sk:8443/api/graphql", "https://user@roy.flox.sk/api/graphql",
                    "https://roy.flox.sk/api/graphql?token=fixture", "https://roy.flox.sk/api/graphql#fixture"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                verifier.verify_project_url("roy", url)

    def test_bucket_privacy_region_and_owner_are_required(self):
        client = Mock()
        client.get_bucket_location.return_value = {"LocationConstraint": verifier.REGION}
        block = dict.fromkeys(("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"), True)
        client.get_public_access_block.return_value = {"PublicAccessBlockConfiguration": block}
        verifier.verify_bucket(client)
        for call in (client.head_bucket.call_args, client.get_bucket_location.call_args,
                     client.get_public_access_block.call_args):
            self.assertEqual(verifier.ACCOUNT, call.kwargs["ExpectedBucketOwner"])
            self.assertEqual(verifier.BUCKET, call.kwargs["Bucket"])
        for field in block:
            block[field] = False
            with self.assertRaises(ValueError):
                verifier.verify_bucket(client)
            block[field] = True
        client.get_bucket_location.return_value = {"LocationConstraint": "us-east-1"}
        with self.assertRaises(ValueError):
            verifier.verify_bucket(client)

    def test_publisher_is_create_only_and_checks_exact_encrypted_readback_with_closure(self):
        client = Mock()
        client.get_bucket_location.return_value = {"LocationConstraint": verifier.REGION}
        client.get_public_access_block.return_value = {"PublicAccessBlockConfiguration": dict.fromkeys(
            ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"), True)}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "invoice-discovery-fixture.json"
            payload = b'{"project":"roy"}'
            path.write_bytes(payload)
            for encryption, readback in (("AES256", payload), (None, payload), ("AES256", b"changed")):
                body = BytesIO(readback)
                client.get_object.return_value = {"Body": body, "ServerSideEncryption": encryption}
                if encryption == "AES256" and readback == payload:
                    self.assertIn("data/roy/order-automation/verification/", verifier.publish_report(client, project="roy", path=path))
                else:
                    with self.assertRaises(ValueError):
                        verifier.publish_report(client, project="roy", path=path)
                self.assertTrue(body.closed)
            request = client.put_object.call_args.kwargs
            self.assertEqual("*", request["IfNoneMatch"])
            self.assertEqual("AES256", request["ServerSideEncryption"])
            self.assertEqual(verifier.ACCOUNT, request["ExpectedBucketOwner"])
            self.assertNotIn("state.json", request["Key"])

    def test_unpushed_source_fails_before_any_aws_or_provider_access(self):
        with patch.object(verifier.subprocess, "run") as run, \
             patch.object(verifier, "require_pushed_source", side_effect=ValueError("unpushed")), \
             patch("boto3.Session") as session, patch.object(verifier, "InvoiceGenerator") as generator:
            with self.assertRaisesRegex(ValueError, "unpushed"):
                verifier.main(["--project", "roy"])
            self.assertIn("scripts/verify_invoice_post_recovery.py", run.call_args.args[0])
            session.assert_not_called()
            generator.assert_not_called()

    def test_cli_uses_runtime_settings_no_web_and_persists_failed_scan_without_public_ids(self):
        generator = self.generator([order()])
        generator.client.scan_error = RuntimeError("private-fixture")
        secret = {"BIZNISWEB_API_URL": "https://roy.flox.sk/api/graphql", "BIZNISWEB_API_TOKEN": "fixture-token",
                  "BIZNISWEB_USERNAME": "fixture-user", "BIZNISWEB_PASSWORD": "fixture-password"}
        clients = {name: MagicMock() for name in ("sts", "s3", "secretsmanager")}
        for client in clients.values():
            client.__enter__.return_value = client
        clients["sts"].get_caller_identity.return_value = {"Account": verifier.ACCOUNT}
        clients["secretsmanager"].get_secret_value.return_value = {"SecretString": json.dumps(secret)}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "invoice-discovery-fixture.json"
            reports = []

            def write(_root, report):
                reports.append(deepcopy(report))
                path.write_text(json.dumps(report), encoding="utf-8")
                return path

            output = StringIO()
            with patch.object(verifier.subprocess, "run"), \
                 patch.object(verifier, "require_pushed_source", return_value="a" * 40), \
                 patch("boto3.Session") as session, patch.object(verifier, "verify_bucket"), \
                 patch.object(verifier, "InvoiceGenerator", return_value=generator) as factory, \
                 patch.object(verifier, "write_private_report", side_effect=write), \
                 patch.object(verifier, "publish_report") as publish, patch("sys.stdout", output):
                session.return_value.client.side_effect = lambda name, **kwargs: clients[name]
                self.assertEqual(1, verifier.main(["--project", "roy"]))
                self.assertFalse(reports[0]["complete"])
                self.assertIsNone(factory.call_args.kwargs["username"])
                self.assertIsNone(factory.call_args.kwargs["password"])
                self.assertFalse(factory.call_args.kwargs["send_invoice_email"])
                self.assertEqual(5000, factory.call_args.kwargs["scan_max_pages"])
                self.assertEqual(2, factory.call_args.kwargs["page_delay_seconds"])
                self.assertEqual(["Odoslaná"], factory.call_args.kwargs["eligible_statuses"])
                publish.assert_not_called()
            self.assertNotIn("FIXTURE-1", output.getvalue())
            self.assertNotIn("private-fixture", output.getvalue())
            clients["secretsmanager"].get_secret_value.assert_called_once_with(SecretId="roy/reporting/runtime-env")


if __name__ == "__main__":
    unittest.main()
