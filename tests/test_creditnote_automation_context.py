import unittest
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import Mock, patch

from creditnote_export import (fetch_project_creditnotes, normalize_creditnote_automation_context,
                              load_creditnote_status_change_audit, save_creditnote_status_change_audit)


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


if __name__ == "__main__":
    unittest.main()
