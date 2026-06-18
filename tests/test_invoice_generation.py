import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import daily_report_runner as daily_runner
from daily_report_runner import maybe_run_invoice_automation, parse_args as parse_daily_report_args
from generate_invoices import (
    InvoiceGenerator,
    InvoiceRunSummary,
    _status_matches_invoice_generation,
    resolve_invoice_date_window,
    resolve_invoice_generation_settings,
)
from invoice_runner import resolve_invoice_runner_window


ROOT_DIR = Path(__file__).resolve().parents[1]


class _FakeInvoiceResponse:
    def __init__(self, url: str, payload: dict | None = None, status_code: int = 200) -> None:
        self.url = url
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload or {})

    def json(self) -> dict:
        if self._payload is None:
            raise json.JSONDecodeError("no json", self.text, 0)
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeInvoiceWebSession:
    def __init__(self) -> None:
        self.post_urls: list[str] = []
        self.get_urls: list[str] = []

    def post(self, url: str, headers: dict | None = None) -> _FakeInvoiceResponse:
        self.post_urls.append(url)
        if "/erp/orders/invoices/create/" in url:
            return _FakeInvoiceResponse(url, {"success": True})
        if "/erp/orders/invoices/finalize/" in url:
            return _FakeInvoiceResponse(url, {"success": True, "invoice_num": "FV-123"})
        if "/erp/orders/invoices/sendEmail/" in url:
            return _FakeInvoiceResponse(url, {"success": True})
        return _FakeInvoiceResponse(url, {"success": False}, status_code=404)

    def get(self, url: str, headers: dict | None = None) -> _FakeInvoiceResponse:
        self.get_urls.append(url)
        return _FakeInvoiceResponse(url, {"success": True})


class _FakeInvoiceClient:
    def __init__(self, invoices: list[dict]) -> None:
        self.invoices = invoices

    def execute(self, query, variable_values=None):
        return {
            "getOrder": {
                "order_num": (variable_values or {}).get("order_num"),
                "invoices": self.invoices,
            }
        }


class InvoiceGenerationTests(unittest.TestCase):
    def test_invoice_generation_settings_default_zero_total_exclusion(self) -> None:
        settings = resolve_invoice_generation_settings({"invoice_generation": {"enabled": True}})
        self.assertTrue(settings["enabled"])
        self.assertEqual(settings["lookback_days"], 7)
        self.assertTrue(settings["exclude_zero_total_orders"])
        self.assertTrue(settings["send_invoice_email"])
        self.assertEqual(["Odoslan\u00e1"], settings["eligible_statuses"])

    def test_invoice_generation_settings_can_disable_invoice_email(self) -> None:
        settings = resolve_invoice_generation_settings(
            {"invoice_generation": {"enabled": True, "send_invoice_email": False}}
        )
        self.assertFalse(settings["send_invoice_email"])

    def test_resolve_invoice_date_window_uses_rolling_lookback(self) -> None:
        from_date, to_date = resolve_invoice_date_window("2026-04-24", 7)
        self.assertEqual(("2026-04-18", "2026-04-24"), (from_date, to_date))

    def test_invoice_status_matching_handles_slovak_diacritics(self) -> None:
        self.assertTrue(_status_matches_invoice_generation("Odoslan\u00e1"))
        self.assertTrue(_status_matches_invoice_generation("ODOSLANA"))
        self.assertFalse(_status_matches_invoice_generation("\u010cak\u00e1 na vybavenie"))
        self.assertFalse(_status_matches_invoice_generation("\u010cak\u00e1 na \u00fahradu"))
        self.assertFalse(_status_matches_invoice_generation("madfrog stara odoslana"))
        self.assertFalse(_status_matches_invoice_generation("Platba online - platnos\u0165 vypr\u0161ala"))

    def test_invoice_runner_uses_current_day_reference_window(self) -> None:
        settings = {"invoice_generation": {"enabled": True, "lookback_days": 7}}
        from_date, to_date = resolve_invoice_runner_window(
            settings,
            timezone_name="Europe/Bratislava",
            reference_date="2026-04-28",
        )
        self.assertEqual(("2026-04-22", "2026-04-28"), (from_date, to_date))

    def test_daily_report_runner_skips_invoices_by_default(self) -> None:
        args = parse_daily_report_args([])
        self.assertTrue(args.skip_invoices)

    def test_daily_report_runner_restores_output_tag_after_creditnote_guard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            required_outputs = {
                "report_html": tmp_path / "report.html",
                "export_csv": tmp_path / "export.csv",
                "date_csv": tmp_path / "date.csv",
                "month_csv": tmp_path / "month.csv",
            }
            for path in required_outputs.values():
                path.write_text("ok", encoding="utf-8")

            class FakeArtifactSet:
                def as_dict(self):
                    return {
                        "report_html": required_outputs["report_html"],
                        "data_quality_json": tmp_path / "data_quality.json",
                    }

                def required_daily_runner_outputs(self):
                    return required_outputs

            def guard_side_effect(**_kwargs):
                os.environ["REPORT_OUTPUT_TAG"] = "creditnote_storno_guard"
                return {"updated_orders": 1}

            def export_side_effect(**kwargs):
                self.assertEqual("", kwargs["output_tag"])
                self.assertEqual("", os.environ.get("REPORT_OUTPUT_TAG"))

            with patch.object(daily_runner.sys, "argv", [
                "daily_report_runner.py",
                "--project",
                "vevo",
                "--from-date",
                "2026-06-17",
                "--to-date",
                "2026-06-17",
                "--skip-email",
                "--skip-invoices",
            ]), patch.dict(os.environ, {"REPORT_OUTPUT_TAG": ""}, clear=False), \
                patch.object(daily_runner, "load_dotenv"), \
                patch.object(daily_runner, "load_project_env"), \
                patch.object(daily_runner, "load_project_settings", return_value={}), \
                patch.object(daily_runner, "resolve_reporting_defaults", return_value={}), \
                patch.object(daily_runner, "maybe_run_creditnote_storno_guard", side_effect=guard_side_effect), \
                patch.object(daily_runner, "run_export", side_effect=export_side_effect) as run_export_mock, \
                patch.object(daily_runner, "build_artifact_set", return_value=FakeArtifactSet()), \
                patch.object(daily_runner, "s3_upload_outputs"), \
                patch.object(daily_runner, "load_data_quality", return_value={}), \
                patch.object(daily_runner, "put_metric"):
                daily_runner.main()

            run_export_mock.assert_called_once()

    def test_vevo_and_roy_have_separate_schedule_settings(self) -> None:
        vevo = json.loads((ROOT_DIR / "projects" / "vevo" / "settings.json").read_text(encoding="utf-8"))
        roy = json.loads((ROOT_DIR / "projects" / "roy" / "settings.json").read_text(encoding="utf-8"))

        self.assertEqual("vevo-daily-report-email", vevo["report_schedule"]["schedule_name"])
        self.assertEqual("roy-daily-report-email", roy["report_schedule"]["schedule_name"])
        self.assertEqual("vevo-daily-invoice-generation", vevo["invoice_generation"]["schedule_name"])
        self.assertEqual("roy-daily-invoice-generation", roy["invoice_generation"]["schedule_name"])
        self.assertEqual("cron(0/15 6-23 * * ? *)", vevo["invoice_generation"]["schedule_expression"])
        self.assertEqual("cron(5/15 6-23 * * ? *)", roy["invoice_generation"]["schedule_expression"])
        self.assertEqual("vevo-same-day-invoice-sweep", vevo["invoice_generation"]["final_sweep_schedule_name"])
        self.assertEqual("roy-same-day-invoice-sweep", roy["invoice_generation"]["final_sweep_schedule_name"])
        self.assertEqual("cron(58 23 * * ? *)", vevo["invoice_generation"]["final_sweep_schedule_expression"])
        self.assertEqual("cron(59 23 * * ? *)", roy["invoice_generation"]["final_sweep_schedule_expression"])
        self.assertEqual(["Odoslan\u00e1"], vevo["invoice_generation"]["eligible_statuses"])
        self.assertEqual(["Odoslan\u00e1"], roy["invoice_generation"]["eligible_statuses"])
        self.assertTrue(vevo["invoice_generation"]["send_invoice_email"])
        self.assertTrue(roy["invoice_generation"]["send_invoice_email"])

        self.assertNotEqual(vevo["report_schedule"]["task_family"], vevo["invoice_generation"]["task_family"])
        self.assertNotEqual(roy["report_schedule"]["task_family"], roy["invoice_generation"]["task_family"])
        self.assertNotEqual(vevo["invoice_generation"]["schedule_name"], roy["invoice_generation"]["schedule_name"])

    def test_filter_excludes_zero_total_orders(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            exclude_zero_total_orders=True,
        )
        filtered, stats = generator.filter_orders_for_invoice(
            [
                {
                    "order_num": "A-1",
                    "status": {"name": "Odoslaná"},
                    "invoices": [],
                    "sum": {"value": 0},
                },
                {
                    "order_num": "A-2",
                    "status": {"name": "Odoslaná"},
                    "invoices": [],
                    "sum": {"value": 12.5},
                },
                {
                    "order_num": "A-3",
                    "status": {"name": "Čaká na vybavenie"},
                    "invoices": [],
                    "sum": {"value": 19},
                },
            ]
        )
        self.assertEqual(["A-2"], [order["order_num"] for order in filtered])
        self.assertEqual(1, stats["skipped_zero_total_orders"])

    @patch("time.sleep", return_value=None)
    def test_create_invoice_sends_email_using_graphql_invoice_fallback(self, _sleep_mock) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            send_invoice_email=True,
        )
        generator.web_session = _FakeInvoiceWebSession()
        generator.client = _FakeInvoiceClient([{"id": "INV-123", "invoice_num": "FV-123"}])
        generator.arf_token = "arf123"

        result = generator.create_invoice(
            {
                "id": "ORDER-ID",
                "order_num": "1001",
                "customer": {"email": "customer@example.test"},
                "status": {"name": "Odoslana"},
                "sum": {"value": 12.5, "formatted": "12.50 EUR"},
            }
        )

        self.assertTrue(result)
        self.assertTrue(result.created)
        self.assertEqual("INV-123", result.invoice_id)
        self.assertTrue(result.email_sent)
        self.assertTrue(any("/erp/orders/invoices/sendEmail/INV-123" in url for url in generator.web_session.post_urls))

    @patch("time.sleep", return_value=None)
    def test_create_invoice_requires_invoice_id_when_email_enabled(self, _sleep_mock) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            send_invoice_email=True,
        )
        generator.web_session = _FakeInvoiceWebSession()
        generator.client = _FakeInvoiceClient([])
        generator.arf_token = "arf123"

        result = generator.create_invoice(
            {
                "id": "ORDER-ID",
                "order_num": "1001",
                "customer": {"email": "customer@example.test"},
                "status": {"name": "Odoslana"},
                "sum": {"value": 12.5, "formatted": "12.50 EUR"},
            }
        )

        self.assertFalse(result)
        self.assertTrue(result.created)
        self.assertFalse(result.email_sent)
        self.assertEqual("missing_invoice_id", result.email_error)

    @patch("daily_report_runner.put_metric")
    @patch("daily_report_runner.run_invoice_generation")
    @patch("daily_report_runner.load_project_settings")
    def test_daily_runner_invoice_hook_uses_configured_window(
        self,
        load_project_settings_mock,
        run_invoice_generation_mock,
        put_metric_mock,
    ) -> None:
        load_project_settings_mock.return_value = {
            "invoice_generation": {
                "enabled": True,
                "lookback_days": 7,
                "exclude_zero_total_orders": True,
            }
        }
        run_invoice_generation_mock.return_value = InvoiceRunSummary(
            project="vevo",
            date_from="2026-04-18",
            date_to="2026-04-24",
            dry_run=True,
            matched_orders=3,
            skipped_zero_total_orders=2,
        )

        result = maybe_run_invoice_automation(
            project="vevo",
            report_to_date="2026-04-24",
            reporting_defaults={},
            dry_run=True,
        )

        run_invoice_generation_mock.assert_called_once_with(
            project_name="vevo",
            date_from="2026-04-18",
            date_to="2026-04-24",
            dry_run=True,
        )
        self.assertEqual(
            {
                "from_date": "2026-04-18",
                "to_date": "2026-04-24",
                "matched_orders": 3,
                "created_invoices": 0,
                "failed_invoices": 0,
                "emailed_invoices": 0,
                "failed_invoice_emails": 0,
                "missing_invoice_ids": 0,
                "skipped_zero_total_orders": 2,
                "dry_run": True,
            },
            result,
        )
        self.assertEqual(8, put_metric_mock.call_count)


if __name__ == "__main__":
    unittest.main()
