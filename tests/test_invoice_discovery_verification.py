from copy import deepcopy
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts.verify_invoice_discovery import (
    attach_scan_progress, audit_candidates, publish_private_report, require_pushed_source, verify_discovery, write_private_report,
)


def order(number, *, invoices=None, status_id=10, blocked=False):
    return {"order_num": number, "id": number, "status": {"id": status_id}, "blocked": blocked,
            "sum": {"value": 100}, "invoices": invoices or []}


class ReadOnlyGenerator:
    def __init__(self):
        self.web_session = self.operation_journal = None
        self.scan_pages = 0
        self.inventory = [order("FIXTURE-A"), order("FIXTURE-B"), order("FIXTURE-C", status_id=20)]
        self.fresh = {row["order_num"]: deepcopy(row) for row in self.inventory}
        self.reads = []
        self.scan_error = None

    def resolve_eligible_status_ids(self):
        return {10}

    def fetch_all_eligible_orders(self):
        self.scan_pages = 3
        if self.scan_error:
            raise self.scan_error
        return deepcopy(self.inventory)

    def fetch_order_for_invoice(self, number):
        self.reads.append(number)
        return deepcopy(self.fresh[number])

    def filter_orders_for_invoice(self, rows):
        return [row for row in rows if row["status"]["id"] == 10 and not row["blocked"] and not row["invoices"]], {}


class InvoiceDiscoveryVerificationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.audit = {"project": "vevo", "complete": True, "observed_at": self.now.isoformat(),
                      "candidates": [{"order_num": number, "blocked": False, "invoices": [], "older_than_7_days": True}
                                     for number in ("FIXTURE-A", "FIXTURE-B")]}
        self.generator = ReadOnlyGenerator()

    def verify(self):
        return verify_discovery(generator=self.generator, audit=self.audit, project="vevo",
                                expected_count=2, source_commit="a" * 40, now=self.now)

    def test_complete_inventory_is_filtered_and_historical_candidates_are_freshly_proven(self):
        report = self.verify()
        self.assertTrue(report["ok"])
        self.assertTrue(report["complete"])
        self.assertEqual(3, report["scanned_orders"])
        self.assertEqual(3, report["scan_pages"])
        self.assertEqual(2, report["eligible_orders"])
        self.assertEqual({"discovered_and_still_eligible": 2}, report["outcome_counts"])
        self.assertEqual(["FIXTURE-A", "FIXTURE-B"], self.generator.reads)
        self.assertIsNone(self.generator.web_session)
        self.assertIsNone(self.generator.operation_journal)

    def test_missing_still_eligible_candidate_fails_despite_complete_scan(self):
        self.generator.inventory = [self.generator.inventory[0]]
        report = self.verify()
        self.assertFalse(report["ok"])
        self.assertTrue(report["complete"])
        self.assertEqual(1, report["failed_orders"])
        self.assertEqual(["fresh_candidate_missing_from_discovery"], report["orders"][1]["issues"])

    def test_external_invoice_and_changed_eligibility_do_not_require_membership(self):
        self.generator.inventory = []
        self.generator.fresh["FIXTURE-A"]["invoices"] = [{"id": "INVOICE-FIXTURE"}]
        self.generator.fresh["FIXTURE-B"]["status"]["id"] = 20
        report = self.verify()
        self.assertTrue(report["ok"])
        self.assertEqual({"already_invoiced": 1, "no_longer_eligible": 1}, report["outcome_counts"])

    def test_duplicate_or_failed_scan_never_claims_completion_or_checks_candidates(self):
        self.generator.inventory.append(deepcopy(self.generator.inventory[0]))
        report = self.verify()
        self.assertFalse(report["complete"])
        self.assertFalse(report["ok"])
        self.assertEqual([], self.generator.reads)
        self.assertEqual(["full_discovery_failed"], report["global_issues"])
        self.generator.scan_error = RuntimeError("private-error-content-must-not-be-persisted")
        report = self.verify()
        self.assertNotIn("private-error-content", json.dumps(report))
        self.assertEqual("RuntimeError", report["error_type"])

    def test_wrong_fresh_identity_or_multiple_invoices_fail(self):
        self.generator.fresh["FIXTURE-A"]["order_num"] = "FOREIGN-FIXTURE"
        self.generator.fresh["FIXTURE-B"]["invoices"] = [{"id": "ONE"}, {"id": "TWO"}]
        report = self.verify()
        self.assertFalse(report["ok"])
        self.assertEqual(2, report["failed_orders"])
        self.assertIn("fresh_order_read_failed", report["orders"][0]["issues"])
        self.assertIn("multiple_final_invoices", report["orders"][1]["issues"])

    def test_web_session_or_journal_is_rejected_before_discovery(self):
        for attribute in ("web_session", "operation_journal"):
            self.generator = ReadOnlyGenerator()
            setattr(self.generator, attribute, object())
            with self.assertRaises(ValueError):
                self.verify()
            self.assertEqual(0, self.generator.scan_pages)

    def test_audit_must_be_complete_fresh_project_matched_and_exact_count(self):
        for change in ({"project": "roy"}, {"complete": False}, {"candidates": []},
                       {"observed_at": (self.now - timedelta(hours=25)).isoformat()},
                       {"observed_at": (self.now + timedelta(seconds=1)).isoformat()}):
            with self.assertRaises(ValueError):
                audit_candidates({**self.audit, **change}, "vevo", self.now, 2)
        with self.assertRaises(ValueError):
            audit_candidates(self.audit, "vevo", self.now, 5)
        self.audit["candidates"].append(self.audit["candidates"][0])
        with self.assertRaises(ValueError):
            audit_candidates(self.audit, "vevo", self.now)

    def test_execution_evidence_requires_clean_tracked_and_pushed_source(self):
        with patch("scripts.verify_invoice_discovery.subprocess.run") as run, \
             patch("scripts.verify_invoice_discovery.subprocess.check_output", return_value=" M generate_invoices.py"):
            with self.assertRaisesRegex(ValueError, "committed"):
                require_pushed_source(Path.cwd())
            self.assertIn("--error-unmatch", run.call_args.args[0])
        outputs = ["", "codex/fixture", "origin/codex/fixture", "a" * 40, "b" * 40 + "\trefs/heads/codex/fixture\n"]
        with patch("scripts.verify_invoice_discovery.subprocess.run"), \
             patch("scripts.verify_invoice_discovery.subprocess.check_output", side_effect=outputs):
            with self.assertRaisesRegex(ValueError, "pushed"):
                require_pushed_source(Path.cwd())
        outputs[-1] = "a" * 40 + "\trefs/heads/codex/fixture\n"
        with patch("scripts.verify_invoice_discovery.subprocess.run"), \
             patch("scripts.verify_invoice_discovery.subprocess.check_output", side_effect=outputs):
            self.assertEqual("a" * 40, require_pushed_source(Path.cwd()))

    def test_progress_only_reports_aggregate_counts_and_preserves_read_results(self):
        self.generator.execute_read = Mock(return_value={"private": "fixture-content"})
        original = self.generator.execute_read
        emit = Mock()
        attach_scan_progress(self.generator, "vevo", emit)
        for index in range(100):
            self.generator.scan_pages = index
            self.assertEqual({"private": "fixture-content"}, self.generator.execute_read("query", {"cursor": index}))
        self.assertEqual(100, original.call_count)
        self.assertEqual([{"project": "vevo", "scan_reads": 50, "completed_pages": 49},
                          {"project": "vevo", "scan_reads": 100, "completed_pages": 99}],
                         [call.args[0] for call in emit.call_args_list])

    def test_private_report_only_writes_ignored_project_path(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with patch("scripts.verify_invoice_discovery.subprocess.run", return_value=Mock(returncode=0)), \
                 patch("scripts.verify_invoice_discovery.subprocess.check_output", return_value=""):
                path = write_private_report(root, self.verify())
                self.assertTrue(path.is_relative_to((root / "data" / "vevo" / "order-automation").resolve()))
                self.assertEqual("vevo", json.loads(path.read_text(encoding="utf-8"))["project"])
            with patch("scripts.verify_invoice_discovery.subprocess.run", return_value=Mock(returncode=1)), \
                 patch("scripts.verify_invoice_discovery.subprocess.check_output", return_value=""):
                with self.assertRaisesRegex(ValueError, "ignored"):
                    write_private_report(root, self.verify())

    def test_optional_publication_is_private_immutable_and_independently_verified(self):
        client = Mock()
        client.get_public_access_block.return_value = {"PublicAccessBlockConfiguration": dict.fromkeys(
            ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets"), True)}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "invoice-discovery-fixture.json"
            payload = b'{"project":"vevo"}'
            path.write_bytes(payload)
            client.get_object.return_value = {"Body": BytesIO(payload)}
            publish_private_report(client, bucket="private-fixture", project="vevo", account="123456789012", path=path)
            request = client.put_object.call_args.kwargs
            self.assertEqual("*", request["IfNoneMatch"])
            self.assertEqual("AES256", request["ServerSideEncryption"])
            self.assertTrue(request["Key"].startswith("data/vevo/order-automation/verification/invoice-discovery-"))
            self.assertNotIn("state.json", request["Key"])
            client.get_object.return_value = {"Body": BytesIO(b"different")}
            with self.assertRaisesRegex(ValueError, "readback"):
                publish_private_report(client, bucket="private-fixture", project="vevo", account="123456789012", path=path)


if __name__ == "__main__":
    unittest.main()
