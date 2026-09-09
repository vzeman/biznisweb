import unittest
import json
import os
from io import BytesIO
from pathlib import Path
import tempfile
from unittest.mock import Mock, patch

from creditnote_export import (fetch_project_creditnotes, normalize_creditnote_automation_context,
                              load_creditnote_status_change_audit, save_creditnote_status_change_audit, parse_money)
from order_status_safety import creditnote_coverage_reason


def document(**extra):
    return {"creditnote_id": "1", "number": "CN-1", "order_num": "ORDER-A",
            "taxed_price": "12,30 €", "price": "10,00", "inv_id": "INV-1", **extra}


class CreditnoteAutomationContextTests(unittest.TestCase):
    def test_normalizes_document_without_customer_data(self):
        context = normalize_creditnote_automation_context([document(customer="private", email="private")])
        row = context["ORDER-A"][0]
        self.assertEqual(row["currency"], "EUR")
        self.assertEqual(row["amount"], 12.3)
        self.assertEqual(row["net_amount"], 10)
        self.assertEqual(row["invoice_id"], "INV-1")
        self.assertNotIn("customer", row)
        self.assertNotIn("email", row)

    def test_duplicate_and_unattributed_documents_cannot_prove_clear(self):
        for rows in ([document(), document(order_num="ORDER-B")], [document(order_num=None)],
                     [document(taxed_price="invalid")]):
            with self.subTest(rows=rows), self.assertRaises(RuntimeError):
                normalize_creditnote_automation_context(rows)

    def test_open_unnumbered_zero_document_is_attributed_but_cannot_prove_refund(self):
        context = normalize_creditnote_automation_context([
            document(number="", taxed_price="0 €", price="0", open="1", storno="0"),
            document(creditnote_id="2", number="CN-2", order_num="ORDER-B"),
        ])
        pending = context["ORDER-A"][0]
        self.assertFalse(pending["numbered"])
        self.assertEqual("", pending["number"])
        self.assertIsNone(pending["amount"])
        self.assertIsNone(pending["net_amount"])
        self.assertEqual(12.3, context["ORDER-B"][0]["amount"])
        order = {"sum": {"value": 12.3, "currency": {"code": "EUR"}, "is_net_price": False},
                 "vat_summary": [{"tax_rate": 23, "tax_base": 10, "amount": 2.3}],
                 "invoices": [{"id": "INV-1"}]}
        self.assertEqual("creditnote_amount_unknown", creditnote_coverage_reason(order, context["ORDER-A"]))

    def test_unnumbered_positive_document_cannot_prove_refund_either(self):
        row = normalize_creditnote_automation_context([document(number="")])["ORDER-A"][0]
        self.assertIsNone(row["amount"])
        self.assertIsNone(row["net_amount"])

    def test_numeric_zero_is_a_valid_amount(self):
        self.assertEqual((0.0, ""), parse_money(0))
        row = normalize_creditnote_automation_context([
            document(taxed_price=0, price=0, currencied_price="0 €")
        ])["ORDER-A"][0]
        self.assertEqual(0.0, row["amount"])

    def test_page_total_and_identity_must_be_complete(self):
        for pages in ([{"total": 2, "rows": [document()]}, {"total": 2, "rows": []}],
                      [{"total": 2, "rows": [document()]}, {"total": 2, "rows": [document()]}],
                      [{"rows": []}],
                      [{"total": 2, "rows": [document()]}, {"total": 3, "rows": [document(creditnote_id="2")]}]):
            with self.subTest(pages=pages), \
                 patch("creditnote_export._login_admin", return_value=("https://example.invalid", Mock(), "csrf")), \
                 patch("creditnote_export.parse_biznisweb_js_object", side_effect=pages), \
                 patch("creditnote_export.time_module.sleep"), self.assertRaises(RuntimeError):
                fetch_project_creditnotes("roy", page_limit=1)

    def test_verified_empty_creditnote_list_is_valid(self):
        with patch("creditnote_export._login_admin", return_value=("https://example.invalid", Mock(), "csrf")), \
             patch("creditnote_export.parse_biznisweb_js_object", return_value={"total": 0, "rows": []}):
            self.assertEqual(fetch_project_creditnotes("roy"), ([], 0))

    def test_strict_audit_rejects_wrong_project_or_invalid_schema_before_recovery(self):
        for payload in ([], {}, {"project": "vevo", "orders": []},
                        {"project": "roy", "orders": [None]}):
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as folder, \
                 patch("creditnote_export.project_data_dir", return_value=Path(folder)), \
                 patch.dict(os.environ, {"REPORT_S3_BUCKET": "test-private"}):
                Path(folder, "creditnote_status_change_audit.json").write_text(json.dumps(payload), encoding="utf-8")
                with self.assertRaises(RuntimeError):
                    load_creditnote_status_change_audit("roy", strict=True)
                with self.assertRaises(RuntimeError):
                    save_creditnote_status_change_audit("roy", payload, strict=True)

    def test_strict_audit_does_not_hide_durable_write_failure(self):
        with tempfile.TemporaryDirectory() as folder, \
             patch("creditnote_export.project_data_dir", return_value=Path(folder)), \
             patch.dict(os.environ, {"REPORT_S3_BUCKET": "test-private"}), \
             patch("boto3.client") as client:
            client.return_value.put_object.side_effect = RuntimeError("unavailable")
            with self.assertRaises(RuntimeError):
                save_creditnote_status_change_audit("roy", {"project": "roy", "orders": []}, strict=True)

    def test_strict_audit_uses_configured_storage_when_runtime_secret_is_blank(self):
        settings = {"live_dashboard_artifacts": {"s3_bucket": "verified-existing-bucket", "s3_prefix": "daily-reports/shop"}}
        audit = {"project": "shop", "orders": []}
        with tempfile.TemporaryDirectory() as folder, \
             patch("creditnote_export.project_data_dir", return_value=Path(folder)), \
             patch.dict(os.environ, {"REPORT_S3_BUCKET": "", "REPORT_S3_PREFIX": ""}, clear=True), \
             patch("boto3.client") as client:
            client.return_value.get_object.return_value = {"Body": BytesIO(json.dumps(audit).encode())}
            self.assertEqual(audit, load_creditnote_status_change_audit("shop", settings, strict=True))
            save_creditnote_status_change_audit("shop", audit, settings, strict=True)
            expected = {"Bucket": "verified-existing-bucket", "Key": "daily-reports/shop/state/creditnote_status_change_audit.json"}
            client.return_value.get_object.assert_called_once_with(**expected)
            sent = client.return_value.put_object.call_args.kwargs
            self.assertEqual(expected, {key: sent[key] for key in expected})

    def test_strict_audit_has_no_unconfigured_bucket_fallback(self):
        with tempfile.TemporaryDirectory() as folder, \
             patch("creditnote_export.project_data_dir", return_value=Path(folder)), \
             patch.dict(os.environ, {"REPORT_S3_BUCKET": ""}, clear=True), \
             patch("boto3.client") as client:
            with self.assertRaises(RuntimeError):
                load_creditnote_status_change_audit("unconfigured", {}, strict=True)
            with self.assertRaises(RuntimeError):
                save_creditnote_status_change_audit("unconfigured", {"project": "unconfigured", "orders": []}, {}, strict=True)
            client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
