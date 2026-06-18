import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from creditnote_export import (
    build_creditnote_reporting_audit,
    build_creditnote_export_rows,
    parse_biznisweb_js_object,
    parse_project_list,
    previous_calendar_month,
    write_creditnote_pdf,
)
from daily_report_runner import _build_creditnote_summary
from monthly_creditnote_export_runner import (
    build_creditnote_email_body,
    resolve_creditnote_export_window,
    run_creditnote_export_runner,
)


ROOT_DIR = Path(__file__).resolve().parents[1]


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
            self.assertEqual(53.49, result.total_rows[0]["Suma_s_DPH"])

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
            self.assertIn("Dobropisovana suma spolu", body)
            self.assertIn("VEVO €", body)

    def test_reporting_audit_flags_creditnoted_orders_still_in_revenue_by_carrier(self) -> None:
        export_rows = [
            {
                "Eshop": "ROY",
                "Dobropis cislo": "2626070101",
                "Objednavka": "2677003012",
                "Mena": "EUR",
                "Suma bez DPH": -40.0,
                "Suma s DPH": -49.2,
            },
            {
                "Eshop": "ROY",
                "Dobropis cislo": "2626070102",
                "Objednavka": "2677003013",
                "Mena": "EUR",
                "Suma bez DPH": -10.0,
                "Suma s DPH": -12.3,
            },
        ]
        packeta = {"type": "shipping", "title": "Packeta - vydajne miesto/box - (100) Foo", "reference_id": "9"}
        packeta_other = {"type": "shipping", "title": "Packeta - vydajne miesto/box - (200) Bar", "reference_id": "12"}
        courier = {"type": "shipping", "title": "Kurier", "reference_id": "1"}
        context = {
            "roy": {
                "available": True,
                "included_orders": [
                    {"order_num": "2677003012", "status": {"name": "Odoslana"}, "price_elements": [packeta]},
                    {"order_num": "2677003999", "status": {"name": "Odoslana"}, "price_elements": [packeta_other]},
                    {"order_num": "2677004000", "status": {"name": "Odoslana"}, "price_elements": [courier]},
                ],
                "all_orders": [
                    {"order_num": "2677003012", "status": {"name": "Odoslana"}, "price_elements": [packeta]},
                    {"order_num": "2677003013", "status": {"name": "Vratene"}, "price_elements": [packeta_other]},
                    {"order_num": "2677003999", "status": {"name": "Odoslana"}, "price_elements": [packeta_other]},
                    {"order_num": "2677004000", "status": {"name": "Odoslana"}, "price_elements": [courier]},
                ],
                "status_change_audit": {
                    "orders": [{"order_num": "2677003013", "previous_status": "Odoslana"}],
                },
            }
        }

        enriched, carrier_rows, audit = build_creditnote_reporting_audit(export_rows, context)

        self.assertEqual("included", enriched[0]["Reporting revenue"])
        self.assertEqual("excluded", enriched[1]["Reporting revenue"])
        self.assertEqual(1, audit["included_in_revenue"])
        self.assertEqual(1, audit["excluded_from_revenue"])
        packeta_row = next(row for row in carrier_rows if row["Prepravca"] == "Packeta")
        self.assertEqual("9, 12", packeta_row["Prepravca ID"])
        self.assertEqual(3, packeta_row["Realized objednavky"])
        self.assertEqual(3, packeta_row["Odoslane objednavky"])
        self.assertEqual(2, packeta_row["Dobropisovane objednavky"])
        self.assertEqual(66.67, packeta_row["Dobropis rate %"])
        self.assertEqual(61.5, packeta_row["Suma_s_DPH"])

    def test_daily_email_summary_includes_creditnote_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            payload_path = Path(tmp) / "dashboard_payload_latest.json"
            payload_path.write_text(
                json.dumps(
                    {
                        "dashboard": {
                            "creditnotes": {
                                "summary": {
                                    "available": True,
                                    "credited_gross_eur": 123.0,
                                    "credited_net_eur": 100.0,
                                    "creditnotes": 2,
                                    "creditnoted_orders": 2,
                                    "creditnote_rate_pct": 10.0,
                                    "realized_orders": 20,
                                    "revenue_excluded_orders": 2,
                                    "revenue_included_orders": 0,
                                    "order_not_found": 0,
                                    "fulfillment_cost_eur": 1.0,
                                    "fulfillment_orders": 2,
                                },
                                "carrier_rows": [
                                    {
                                        "carrier": "Packeta",
                                        "creditnote_rate_pct": 10.0,
                                        "creditnoted_orders": 2,
                                        "realized_orders": 20,
                                        "rate_index": 1.5,
                                        "credited_gross_eur": 123.0,
                                        "outlier": True,
                                    }
                                ],
                            }
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            summary = _build_creditnote_summary({"dashboard_payload_json": payload_path})

        self.assertIn("DOBROPISY", summary)
        self.assertIn("Dobropisovana suma spolu", summary)
        self.assertIn("Packeta", summary)
        self.assertIn("OUTLIER", summary)

    def test_parse_project_list_defaults_and_normalizes(self) -> None:
        self.assertEqual(("roy", "vevo"), parse_project_list(None))
        self.assertEqual(("roy", "vevo"), parse_project_list(" ROY, vevo ,,"))

    @patch("monthly_creditnote_export_runner.put_metric")
    @patch("monthly_creditnote_export_runner.run_monthly_creditnote_export")
    def test_runner_skip_email_uses_previous_month(self, export_mock, put_metric_mock) -> None:
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
                },
            )()

            summary = run_creditnote_export_runner(args)

            export_mock.assert_called_once()
            _, kwargs = export_mock.call_args
            self.assertEqual("2026-05-01", kwargs["date_from"])
            self.assertEqual("2026-05-31", kwargs["date_to"])
            self.assertTrue(summary["email_skipped"])
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
        self.assertEqual("mil.terem@gmail.com", raw["email_to"])


if __name__ == "__main__":
    unittest.main()
