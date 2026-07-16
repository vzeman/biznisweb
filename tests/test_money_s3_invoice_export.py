import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from money_s3_invoice_export import (
    _validate_money_s3_xml,
    biznisweb_php_serialize,
    money_s3_invoice_filters,
    run_money_s3_invoice_export,
)


class MoneyS3InvoiceExportTests(unittest.TestCase):
    def test_money_s3_filter_uses_invoice_issue_dates(self) -> None:
        filters = money_s3_invoice_filters(date(2026, 5, 1), date(2026, 5, 31))

        self.assertEqual(
            {"inv_date_from": "1.5.2026", "inv_date_to": "31.5.2026"},
            filters,
        )
        self.assertEqual(
            'a:2:{s:13:"inv_date_from";s:8:"1.5.2026";s:11:"inv_date_to";s:9:"31.5.2026";}',
            biznisweb_php_serialize(filters),
        )

    def test_money_s3_validation_rejects_non_xml_response(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "did not return MoneyData XML"):
            _validate_money_s3_xml(b"<html>login</html>", "roy")

    @patch("money_s3_invoice_export.download_money_s3_invoice_export")
    def test_export_writes_one_xml_per_project(self, download_mock) -> None:
        download_mock.side_effect = [
            (b"<MoneyData><ROY /></MoneyData>", "roy.xml", 12),
            (b"<?xml version='1.0'?><MoneyData><VEVO /></MoneyData>", "vevo.xml", 15),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            result = run_money_s3_invoice_export(
                projects=("roy", "vevo"),
                date_from="2026-05-01",
                date_to="2026-05-31",
                output_dir=Path(tmp),
                output_tag="test",
            )

            self.assertEqual({"ROY": 12, "VEVO": 15}, result.invoice_counts)
            self.assertEqual(27, result.total_invoices)
            self.assertEqual({"ROY", "VEVO"}, set(result.output_files))
            for project, path in result.output_files.items():
                self.assertTrue(path.exists(), project)
                self.assertIn(f"faktury_money_s3_{project.lower()}_2026-05_issued_test.xml", path.name)
                self.assertIn(b"<MoneyData", path.read_bytes())


if __name__ == "__main__":
    unittest.main()
