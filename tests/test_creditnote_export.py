import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from creditnote_export import (
    build_creditnote_export_rows,
    parse_biznisweb_js_object,
    parse_project_list,
    previous_calendar_month,
    write_creditnote_pdf,
)
from monthly_creditnote_export_runner import (
    build_creditnote_email_body,
    resolve_creditnote_export_window,
    run_creditnote_export_runner,
)
from money_s3_invoice_export import (
    MoneyS3InvoiceExportResult,
    biznisweb_php_serialize,
    money_s3_invoice_filters,
    run_money_s3_invoice_export,
)


ROOT_DIR = Path(__file__).resolve().parents[1]


class FakeMoneyS3Response:
    def __init__(self, text: str = "", content: bytes = b"", headers: dict | None = None) -> None:
        self.text = text
        self.content = content or text.encode("utf-8")
        self.headers = headers or {}
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None


class FakeMoneyS3Session:
    def __init__(self) -> None:
        self.posts: list[tuple[str, dict]] = []

    def post(self, url: str, data=None, **kwargs):
        payload = dict(data or {})
        self.posts.append((url, payload))
        if url.endswith("/erp/orders/invoices/getListJson"):
            self.last_count_filter = payload["massfilter"]
            return FakeMoneyS3Response("{ 'rows': [], 'total': 2 }")
        if url.endswith("/erp/impexp/export/index/invoices/moneys3"):
            self.last_export_payload = payload
            return FakeMoneyS3Response(
                content=b'<MoneyData KodAgendy="invoices"></MoneyData>',
                headers={"content-disposition": 'attachment; filename="Flox_invoices_test.xml"'},
            )
        raise AssertionError(f"Unexpected URL: {url}")


class CreditnoteExportTests(unittest.TestCase):
    def test_parse_biznisweb_creditnote_response(self) -> None:
        payload = parse_biznisweb_js_object("{ 'rows': [ { 'number': '2626070101' } ], 'total': 1 }")
        self.assertEqual(1, payload["total"])
        self.assertEqual("2626070101", payload["rows"][0]["number"])

    def test_parse_biznisweb_bare_keys_response(self) -> None:
        payload = parse_biznisweb_js_object("{success: true, data: { 'number': '2626070101', 'missing': null }}")
        self.assertTrue(payload["success"])
        self.assertIsNone(payload["data"]["missing"])

    def test_previous_calendar_month(self) -> None:
        self.assertEqual((date(2026, 5, 1), date(2026, 5, 31)), previous_calendar_month(date(2026, 6, 14)))
        self.assertEqual((date(2025, 12, 1), date(2025, 12, 31)), previous_calendar_month(date(2026, 1, 14)))

    def test_runner_window_defaults_to_previous_month(self) -> None:
        self.assertEqual(
            ("2026-05-01", "2026-05-31"),
            resolve_creditnote_export_window("Europe/Bratislava", reference_date="2026-06-14"),
        )

    def test_runner_window_accepts_explicit_dates(self) -> None:
        self.assertEqual(
            ("2026-05-01", "2026-05-31"),
            resolve_creditnote_export_window(
                "Europe/Bratislava",
                from_date="2026-05-01",
                to_date="2026-05-31",
            ),
        )

    def test_money_s3_invoice_filter_serialization_uses_invoice_issue_dates(self) -> None:
        filters = money_s3_invoice_filters(date(2026, 5, 1), date(2026, 5, 31))

        self.assertEqual({"inv_date_from": "1.5.2026", "inv_date_to": "31.5.2026"}, filters)
        self.assertEqual(
            'a:2:{s:13:"inv_date_from";s:8:"1.5.2026";s:11:"inv_date_to";s:9:"31.5.2026";}',
            biznisweb_php_serialize(filters),
        )

    @patch("money_s3_invoice_export._login_admin")
    def test_money_s3_invoice_export_downloads_project_xml(self, login_mock) -> None:
        fake_session = FakeMoneyS3Session()
        login_mock.return_value = ("https://example.test", fake_session, "csrf")

        with tempfile.TemporaryDirectory() as tmp:
            result = run_money_s3_invoice_export(
                projects=("roy",),
                date_from=date(2026, 5, 1),
                date_to=date(2026, 5, 31),
                output_dir=Path(tmp),
                output_tag="unit",
            )

            output_file = result.output_files["ROY"]
            self.assertTrue(output_file.exists())
            self.assertEqual(b"<MoneyData", output_file.read_bytes()[:10])
            self.assertEqual({"ROY": 2}, result.invoice_counts)
            self.assertEqual("Flox_invoices_test.xml", result.source_filenames["ROY"])
            self.assertIn("inv_date_from", fake_session.last_count_filter)
            self.assertEqual("invoices", fake_session.last_export_payload["data"])
            self.assertEqual(fake_session.last_count_filter, fake_session.last_export_payload["massFilter"])

    def test_build_creditnote_rows_filters_created_window_and_signs_amounts(self) -> None:
        rows = build_creditnote_export_rows(
            "roy",
            [
                {
                    "number": "2626070101",
                    "creditnote_id": "160",
                    "created": "2026-05-14 10:15:50",
                    "issue_date": "2026-05-14",
                    "order_num": "2677003012",
                    "inv_id": "2677002752",
                    "customer": "Test Customer",
                    "email": "test@example.test",
                    "price": "43.49",
                    "currencied_price": "43,49 €",
                    "taxed_price": "53,49 €",
                },
                {
                    "number": "2626070102",
                    "created": "2026-06-01 00:00:00",
                    "price": "1",
                    "taxed_price": "1,23 €",
                },
                {
                    "number": "",
                    "created": "2026-05-15 00:00:00",
                    "price": "1",
                    "taxed_price": "1,23 €",
                },
            ],
            date(2026, 5, 1),
            date(2026, 5, 31),
        )

        self.assertEqual(1, len(rows))
        self.assertEqual("ROY", rows[0]["Eshop"])
        self.assertEqual("2626070101", rows[0]["Dobropis cislo"])
        self.assertEqual(-43.49, rows[0]["Suma bez DPH"])
        self.assertEqual(-53.49, rows[0]["Suma s DPH"])
        self.assertEqual("€", rows[0]["Mena"])

    def test_write_pdf_outputs_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            result = write_creditnote_pdf(
                export_rows=[
                    {
                        "Eshop": "ROY",
                        "Dobropis cislo": "2626070101",
                        "Mena": "€",
                        "Suma bez DPH": -43.49,
                        "Suma s DPH": -53.49,
                    }
                ],
                fetch_totals={"roy": {"reported_total": 1, "fetched_rows": 1, "exported_rows": 1}},
                output_pdf=tmp_path / "dobropisy.pdf",
                date_from=date(2026, 5, 1),
                date_to=date(2026, 5, 31),
                projects=("roy",),
            )

            self.assertTrue(result.output_pdf.exists())
            self.assertEqual(b"%PDF-", result.output_pdf.read_bytes()[:5])
            self.assertFalse((tmp_path / "dobropisy.xlsx").exists())
            self.assertFalse((tmp_path / "dobropisy_source.json").exists())
            self.assertEqual(1, result.exported_rows)
            self.assertEqual({"ROY": 1}, result.project_counts)
            self.assertEqual(-53.49, result.summary_rows[0]["Suma_s_DPH"])

    def test_email_body_includes_summary_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = write_creditnote_pdf(
                export_rows=[
                    {
                        "Eshop": "VEVO",
                        "Dobropis cislo": "260001",
                        "Mena": "€",
                        "Suma bez DPH": -10.0,
                        "Suma s DPH": -12.3,
                    }
                ],
                fetch_totals={"vevo": {"reported_total": 1, "fetched_rows": 1, "exported_rows": 1}},
                output_pdf=Path(tmp) / "dobropisy.pdf",
                date_from=date(2026, 5, 1),
                date_to=date(2026, 5, 31),
                projects=("vevo",),
            )

            body = build_creditnote_email_body(result)
            self.assertIn("mesacny PDF export", body)
            self.assertIn("Pocet dobropisov: 1", body)
            self.assertIn("VEVO €", body)

    def test_email_body_includes_money_s3_invoice_exports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            creditnote_result = write_creditnote_pdf(
                export_rows=[],
                fetch_totals={"roy": {"reported_total": 0, "fetched_rows": 0, "exported_rows": 0}},
                output_pdf=Path(tmp) / "dobropisy.pdf",
                date_from=date(2026, 5, 1),
                date_to=date(2026, 5, 31),
                projects=("roy",),
            )
            invoice_path = Path(tmp) / "faktury_money_s3_roy_2026-05_issued.xml"
            invoice_path.write_text("<MoneyData />", encoding="utf-8")
            invoice_result = MoneyS3InvoiceExportResult(
                projects=("roy",),
                date_from="2026-05-01",
                date_to="2026-05-31",
                output_files={"ROY": invoice_path},
                invoice_counts={"ROY": 384},
                source_filenames={"ROY": "Flox_invoices.xml"},
            )

            body = build_creditnote_email_body(creditnote_result, invoice_result=invoice_result)

            self.assertIn("Money S3 export faktur", body)
            self.assertIn("podla datumu vystavenia: 2026-05-01 az 2026-05-31", body)
            self.assertIn("ROY: 384 ks", body)

    def test_parse_project_list_defaults_and_normalizes(self) -> None:
        self.assertEqual(("roy", "vevo"), parse_project_list(None))
        self.assertEqual(("roy", "vevo"), parse_project_list(" ROY, vevo ,,"))

    @patch("monthly_creditnote_export_runner.put_metric")
    @patch("monthly_creditnote_export_runner.run_money_s3_invoice_export")
    @patch("monthly_creditnote_export_runner.run_monthly_creditnote_export")
    def test_runner_skip_email_uses_previous_month(self, export_mock, invoice_export_mock, put_metric_mock) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = write_creditnote_pdf(
                export_rows=[],
                fetch_totals={"roy": {"reported_total": 0, "fetched_rows": 0, "exported_rows": 0}},
                output_pdf=Path(tmp) / "dobropisy.pdf",
                date_from=date(2026, 5, 1),
                date_to=date(2026, 5, 31),
                projects=("roy", "vevo"),
            )
            export_mock.return_value = result
            invoice_path = Path(tmp) / "faktury_money_s3_roy_2026-05_issued.xml"
            invoice_path.write_text("<MoneyData />", encoding="utf-8")
            invoice_export_mock.return_value = MoneyS3InvoiceExportResult(
                projects=("roy", "vevo"),
                date_from="2026-05-01",
                date_to="2026-05-31",
                output_files={"ROY": invoice_path, "VEVO": invoice_path},
                invoice_counts={"ROY": 2, "VEVO": 3},
                source_filenames={"ROY": "roy.xml", "VEVO": "vevo.xml"},
            )

            args = type(
                "Args",
                (),
                {
                    "owner_project": "roy",
                    "projects": "roy,vevo",
                    "timezone": "Europe/Bratislava",
                    "reference_date": "2026-06-14",
                    "from_date": "",
                    "to_date": "",
                    "output_dir": "",
                    "output_tag": "",
                    "email_subject": "",
                    "email_from": "reports@example.test",
                    "email_to": "mil.terem@gmail.com",
                    "skip_email": True,
                    "dry_run_email": False,
                    "skip_invoice_export": False,
                    "include_creditnote_reporting_audit": False,
                },
            )()

            summary = run_creditnote_export_runner(args)

            export_mock.assert_called_once()
            _, kwargs = export_mock.call_args
            self.assertEqual("2026-05-01", kwargs["date_from"])
            self.assertEqual("2026-05-31", kwargs["date_to"])
            invoice_export_mock.assert_called_once()
            _, invoice_kwargs = invoice_export_mock.call_args
            self.assertEqual("2026-05-01", invoice_kwargs["date_from"])
            self.assertEqual("2026-05-31", invoice_kwargs["date_to"])
            self.assertTrue(summary["email_skipped"])
            self.assertFalse(summary["invoice_export_skipped"])
            self.assertEqual(5, summary["invoice_export"]["total_invoices"])
            self.assertGreaterEqual(put_metric_mock.call_count, 2)

    def test_monthly_creditnote_settings_are_configured(self) -> None:
        roy = json.loads((ROOT_DIR / "projects" / "roy" / "settings.json").read_text(encoding="utf-8"))
        raw = roy["monthly_creditnote_export"]

        self.assertTrue(raw["enabled"])
        self.assertEqual("monthly-creditnote-export", raw["schedule_name"])
        self.assertEqual("cron(0 6 14 * ? *)", raw["schedule_expression"])
        self.assertEqual("Europe/Bratislava", raw["timezone"])
        self.assertEqual("monthly-creditnote-export", raw["task_family"])
        self.assertEqual(["roy", "vevo"], raw["projects"])
        self.assertTrue(raw["include_invoice_money_s3"])
        self.assertEqual("moneys3", raw["invoice_export_format"])
        self.assertEqual("inv_date", raw["invoice_date_field"])
        self.assertEqual("mil.terem@gmail.com", raw["email_to"])


if __name__ == "__main__":
    unittest.main()
