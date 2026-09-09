import hashlib
import json
import os
import sys
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import daily_report_runner as daily_runner
from daily_report_runner import maybe_run_invoice_automation, parse_args as parse_daily_report_args
from generate_invoices import (
    InvoiceGenerator,
    InvoiceRunSummary,
    _status_matches_invoice_generation,
    reconcile_existing_invoice_statuses,
    resolve_invoice_date_window,
    resolve_invoice_generation_settings,
    run_invoice_generation,
)
from invoice_automation_state import S3AutomationStateStore
from tests.test_invoice_automation_state import MemoryS3
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

    def post(self, url: str, headers: dict | None = None, **kwargs) -> _FakeInvoiceResponse:
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
        self.calls = 0

    def execute(self, query, variable_values=None):
        self.calls += 1
        return {
            "getOrder": {
                "order_num": (variable_values or {}).get("order_num"),
                "id": "fixture-order-id",
                "blocked": False,
                "status": {"id": 4, "name": "Odoslaná"},
                "sum": {"value": 12.5, "formatted": "12.50 EUR"},
                "invoices": self.invoices if self.calls > 2 else [],
            }
        }


class _FakeStatusReconciliationClient:
    def __init__(self, orders: list[dict]) -> None:
        self.orders = {str(order["order_num"]): order for order in orders}
        self.mutations: list[tuple[str, int]] = []

    def execute(self, _query, variable_values=None):
        variables = variable_values or {}
        if "lang_code" in variables:
            return {
                "listOrderStatuses": [
                    {"id": 55, "name": "Platba online - zaplatené"},
                    {"id": 4, "name": "Odoslaná"},
                ]
            }
        if "order_num" in variables and "status_id" in variables:
            order_num = str(variables["order_num"])
            status_id = int(variables["status_id"])
            self.mutations.append((order_num, status_id))
            return {
                "changeOrderStatus": {
                    "order_num": order_num,
                    "status": {"id": status_id, "name": "Platba online - zaplatené"},
                }
            }
        if "order_num" in variables:
            return {"getOrder": self.orders.get(str(variables["order_num"]))}
        raise AssertionError(f"Unexpected GraphQL variables: {variables}")


class InvoiceGenerationTests(unittest.TestCase):
    def test_daily_report_s3_upload_publishes_each_period_under_exact_stable_key(self) -> None:
        class FakeS3:
            def __init__(self) -> None:
                self.uploads: list[str] = []
                self.operations: list[tuple[str, str]] = []
                self.objects: dict[str, bytes] = {}

            def upload_file(self, path, _bucket, key, ExtraArgs=None) -> None:
                self.uploads.append(key)
                self.operations.append(("upload_file", key))
                self.objects[key] = Path(path).read_bytes()

            def put_object(self, *, Bucket, Key, Body, **_kwargs):
                self.operations.append(("put_object", Key))
                self.objects[Key] = bytes(Body)

            def generate_presigned_url(self, _operation, Params, ExpiresIn):
                return f"https://example.test/{Params['Key']}"

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            report_latest = data_dir / "report_latest.html"
            payload_latest = data_dir / "dashboard_payload_latest.json"
            report_latest.write_text("full", encoding="utf-8")

            embedded_specs = []
            for period in ("7d", "30d", "90d"):
                period_dir = data_dir / "_periods" / period
                period_dir.mkdir(parents=True)
                report_path = period_dir / f"report_20260701-20260714_{period}.html"
                payload_path = period_dir / f"dashboard_payload_20260701-20260714_{period}.json"
                report_path.write_text(period, encoding="utf-8")
                payload_path.write_text(
                    json.dumps(
                        {
                            "project": "vevo",
                            "period_switcher": {"current_key": period},
                        }
                    ),
                    encoding="utf-8",
                )
                embedded_specs.append({"key": period, "report_path": str(report_path)})

            payload_latest.write_text(
                json.dumps(
                    {
                        "project": "vevo",
                        "period_switcher": {
                            "current_key": "full",
                            "_embedded_specs": embedded_specs,
                        },
                    }
                ),
                encoding="utf-8",
            )
            fake_s3 = FakeS3()
            fake_boto3 = SimpleNamespace(client=lambda *_args, **_kwargs: fake_s3)

            with patch.dict(
                os.environ,
                {
                    "REPORT_S3_BUCKET": "reporting-bucket",
                    "REPORT_S3_PREFIX": "daily-reports/vevo",
                },
                clear=False,
            ), patch.dict(sys.modules, {"boto3": fake_boto3}):
                daily_runner.s3_upload_outputs(
                    "vevo",
                    {
                        "report_latest_html": report_latest,
                        "dashboard_payload_latest_json": payload_latest,
                    },
                )

        stable_keys = {
            key for key in fake_s3.uploads if "/latest/" in key
        }
        self.assertEqual(
            {
                "daily-reports/vevo/latest/report_latest.html",
                "daily-reports/vevo/latest/dashboard_payload_latest.json",
                "daily-reports/vevo/latest/report_7d.html",
                "daily-reports/vevo/latest/dashboard_payload_7d.json",
                "daily-reports/vevo/latest/report_30d.html",
                "daily-reports/vevo/latest/dashboard_payload_30d.json",
                "daily-reports/vevo/latest/report_90d.html",
                "daily-reports/vevo/latest/dashboard_payload_90d.json",
            },
            stable_keys,
        )
        manifest_key = "daily-reports/vevo/latest/generation.json"
        self.assertEqual(("put_object", manifest_key), fake_s3.operations[-1])
        manifest = json.loads(fake_s3.objects[manifest_key].decode("utf-8"))
        self.assertEqual(1, manifest["schema_version"])
        self.assertEqual("vevo", manifest["project"])
        self.assertEqual(
            {
                "report_latest.html",
                "dashboard_payload_latest.json",
                "report_7d.html",
                "dashboard_payload_7d.json",
                "report_30d.html",
                "dashboard_payload_30d.json",
                "report_90d.html",
                "dashboard_payload_90d.json",
            },
            set(manifest["artifacts"]),
        )
        for filename, metadata in manifest["artifacts"].items():
            self.assertEqual(
                f"daily-reports/vevo/{manifest['generation_id']}/{filename}",
                metadata["key"],
            )
            self.assertEqual(len(fake_s3.objects[metadata["key"]]), metadata["size"])
            self.assertEqual(
                hashlib.sha256(fake_s3.objects[metadata["key"]]).hexdigest(),
                metadata["sha256"],
            )

    def test_daily_report_subject_and_body_hard_mark_critical_qa(self) -> None:
        defaults = {
            "display_name": "Vevo",
            "reporting_system_name": "Vevo reporting",
            "email_subject": "Daily Vevo report",
        }
        quality = {
            "overall_status": "degraded",
            "qa_status": "critical",
            "qa_failure_count": 1,
            "qa_warning_count": 0,
        }

        subject = daily_runner.build_email_subject(defaults, quality)
        body = daily_runner.build_email_body(
            "2026-07-01",
            "2026-07-14",
            "summary",
            defaults,
            quality,
        )

        self.assertEqual("[CRITICAL QA] Daily Vevo report", subject)
        self.assertIn("NEPOUZIVAJTE HO NA RIADENIE ANI ROZHODOVANIE", body)

    def test_daily_report_subject_marks_partial_data(self) -> None:
        defaults = {"email_subject": "Daily Vevo report"}
        quality = {"is_partial": True, "qa_status": "warning"}

        self.assertEqual(
            "[PARTIAL DATA] Daily Vevo report",
            daily_runner.build_email_subject(defaults, quality),
        )

    def test_invoice_generation_settings_default_zero_total_exclusion(self) -> None:
        settings = resolve_invoice_generation_settings({"invoice_generation": {"enabled": True}})
        self.assertTrue(settings["enabled"])
        self.assertEqual(settings["lookback_days"], 7)
        self.assertTrue(settings["exclude_zero_total_orders"])
        self.assertTrue(settings["send_invoice_email"])
        self.assertEqual(["Odoslan\u00e1"], settings["eligible_statuses"])
        self.assertFalse(settings["existing_invoice_status_reconciliation"]["enabled"])

    def test_existing_invoice_reconciliation_defaults_to_paid_status(self) -> None:
        settings = resolve_invoice_generation_settings(
            {
                "invoice_generation": {
                    "enabled": True,
                    "existing_invoice_status_reconciliation": {"enabled": True},
                }
            }
        )["existing_invoice_status_reconciliation"]

        self.assertTrue(settings["enabled"])
        self.assertEqual("Platba online - zaplatené", settings["target_status_name"])
        self.assertIsNone(settings["target_status_id"])
        self.assertIn("Stripe - expired", settings["source_statuses"])
        self.assertNotIn("Odoslaná", settings["source_statuses"])

    def test_existing_invoice_reconciliation_does_not_infer_payment_from_invoice(self) -> None:
        settings = resolve_invoice_generation_settings(
            {
                "invoice_generation": {
                    "enabled": True,
                    "existing_invoice_status_reconciliation": {"enabled": True},
                }
            }
        )["existing_invoice_status_reconciliation"]
        expired = {
            "order_num": "R-EXPIRED",
            "status": {"id": 69, "name": "Stripe - expired"},
            "invoices": [{"id": "INV-1", "invoice_num": "FV-1"}],
        }
        shipped = {
            "order_num": "R-SHIPPED",
            "status": {"id": 4, "name": "Odoslaná"},
            "invoices": [{"id": "INV-2", "invoice_num": "FV-2"}],
        }
        expired_without_invoice = {
            "order_num": "R-UNPAID",
            "status": {"id": 69, "name": "Stripe - expired"},
            "invoices": [],
        }
        client = _FakeStatusReconciliationClient([expired, shipped, expired_without_invoice])

        result = reconcile_existing_invoice_statuses(
            client,
            [expired, shipped, expired_without_invoice],
            settings,
            dry_run=False,
        )

        self.assertEqual(1, result["candidates"])
        self.assertEqual(0, result["reconciled"])
        self.assertEqual(1, result["review_required"])
        self.assertEqual(0, result["failed"])
        self.assertEqual("Platba online - zaplatené", result["target_status_name"])
        self.assertEqual(55, result["target_status_id"])
        self.assertEqual([], client.mutations)

    def test_existing_invoice_reconciliation_dry_run_never_mutates(self) -> None:
        settings = resolve_invoice_generation_settings(
            {
                "invoice_generation": {
                    "enabled": True,
                    "existing_invoice_status_reconciliation": {"enabled": True},
                }
            }
        )["existing_invoice_status_reconciliation"]
        expired = {
            "order_num": "R-EXPIRED",
            "status": {"id": 69, "name": "Stripe - expired"},
            "invoices": [{"id": "INV-1", "invoice_num": "FV-1"}],
        }
        client = _FakeStatusReconciliationClient([expired])

        result = reconcile_existing_invoice_statuses(client, [expired], settings, dry_run=True)

        self.assertEqual(1, result["candidates"])
        self.assertEqual(0, result["reconciled"])
        self.assertEqual([], client.mutations)

    def test_existing_invoice_reconciliation_does_not_downgrade_order_shipped_during_recheck(self) -> None:
        settings = resolve_invoice_generation_settings(
            {
                "invoice_generation": {
                    "enabled": True,
                    "existing_invoice_status_reconciliation": {"enabled": True},
                }
            }
        )["existing_invoice_status_reconciliation"]
        initially_expired = {
            "order_num": "R-RACE",
            "status": {"id": 69, "name": "Stripe - expired"},
            "invoices": [{"id": "INV-1", "invoice_num": "FV-1"}],
        }
        already_shipped = {
            "order_num": "R-RACE",
            "status": {"id": 4, "name": "Odoslaná"},
            "invoices": [{"id": "INV-1", "invoice_num": "FV-1"}],
        }
        client = _FakeStatusReconciliationClient([already_shipped])

        result = reconcile_existing_invoice_statuses(
            client,
            [initially_expired],
            settings,
            dry_run=False,
        )

        self.assertEqual(1, result["candidates"])
        self.assertEqual(0, result["reconciled"])
        self.assertEqual(1, result["skipped_after_recheck"])
        self.assertEqual([], client.mutations)

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
        self.assertEqual("cron(0/15 * * * ? *)", vevo["invoice_generation"]["schedule_expression"])
        self.assertEqual("cron(5/15 * * * ? *)", roy["invoice_generation"]["schedule_expression"])
        self.assertEqual("vevo-same-day-invoice-sweep", vevo["invoice_generation"]["final_sweep_schedule_name"])
        self.assertEqual("roy-same-day-invoice-sweep", roy["invoice_generation"]["final_sweep_schedule_name"])
        self.assertEqual("cron(58 23 * * ? *)", vevo["invoice_generation"]["final_sweep_schedule_expression"])
        self.assertEqual("cron(59 23 * * ? *)", roy["invoice_generation"]["final_sweep_schedule_expression"])
        self.assertEqual(["Odoslan\u00e1"], vevo["invoice_generation"]["eligible_statuses"])
        self.assertEqual(["Odoslan\u00e1"], roy["invoice_generation"]["eligible_statuses"])
        self.assertTrue(vevo["invoice_generation"]["send_invoice_email"])
        self.assertTrue(roy["invoice_generation"]["send_invoice_email"])
        for project_settings in (vevo, roy):
            reconciliation = project_settings["invoice_generation"][
                "existing_invoice_status_reconciliation"
            ]
            self.assertTrue(reconciliation["enabled"])
            self.assertEqual(
                "Platba online - zaplaten\u00e9",
                reconciliation["target_status_name"],
            )
            self.assertIsNone(reconciliation["target_status_id"])

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
    def test_create_invoice_requires_verified_invoice_after_finalization(self, _sleep_mock) -> None:
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
        self.assertFalse(result.created)
        self.assertFalse(result.email_sent)
        self.assertTrue(result.ambiguous)

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
            invoice_scan_complete=True,
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
                "invoice_status_reconciliation_enabled": False,
                "invoice_status_reconciliation_candidates": 0,
                "reconciled_invoice_statuses": 0,
                "failed_invoice_status_reconciliations": 0,
                "skipped_invoice_status_reconciliations_after_recheck": 0,
                "invoice_status_reconciliation_target_name": "",
                "invoice_status_reconciliation_target_id": None,
                "skipped_zero_total_orders": 2,
                "dry_run": True,
            },
            result,
        )
        self.assertEqual(11, put_metric_mock.call_count)


def invoice_order(number="fixture-order", **updates):
    result = {"id": "fixture-id", "order_num": number, "pur_date": "2020-01-01 10:00:00",
              "last_change": "2020-02-01 10:00:00", "blocked": False,
              "status": {"id": "4", "name": "Odoslaná"}, "invoices": [],
              "sum": {"value": 20, "formatted": "20 EUR", "is_net_price": False, "currency": {"code": "EUR"}}}
    result.update(updates)
    return result


def invoice_page(rows, cursor=None):
    return {"getOrderList": {"data": rows, "pageInfo": {"hasNextPage": cursor is not None, "nextCursor": cursor}}}


class InvoiceApiFake:
    def __init__(self, order):
        self.order = order
        self.calls = []
        self.page_error = None

    def execute(self, query, variable_values=None):
        variables = variable_values or {}
        self.calls.append(deepcopy(variables))
        if "lang_code" in variables:
            return {"listOrderStatuses": [{"id": "4", "name": "Odoslaná"},
                                          {"id": "55", "name": "Platba online - zaplatené"}]}
        if "order_num" in variables:
            return {"getOrder": deepcopy(self.order)}
        if self.page_error:
            raise self.page_error
        rows = [deepcopy(self.order)]
        if "status" in variables and str(self.order["status"]["id"]) != str(variables["status"]):
            rows = []
        if variables.get("changed_from", "") > self.order["last_change"]:
            rows = []
        return invoice_page(rows)


class InvoiceWebFake(_FakeInvoiceWebSession):
    def __init__(self, api):
        super().__init__()
        self.api = api
        self.email_success = True
        self.email_timeout = False
        self.finalize_rejected = False
        self.finalize_timeout_after_commit = False
        self.change_status_during_prepare = False

    def post(self, url, headers=None, **kwargs):
        self.post_urls.append(url)
        if "/create/" in url:
            if self.change_status_during_prepare:
                self.api.order["status"] = {"id": 99, "name": "Storno"}
            return _FakeInvoiceResponse(url, {"success": True})
        if "/finalize/" in url:
            if self.finalize_rejected:
                return _FakeInvoiceResponse(url, {"success": False}, 429)
            self.api.order["invoices"] = [{"id": "fixture-invoice", "invoice_num": "fixture-number"}]
            if self.finalize_timeout_after_commit:
                raise TimeoutError("simulated response loss after commit")
            return _FakeInvoiceResponse(url, {"success": True})
        if "/sendEmail/" in url:
            if self.email_timeout:
                raise TimeoutError("simulated uncertain send")
            return _FakeInvoiceResponse(url, {"success": self.email_success})
        raise AssertionError("Unexpected invoice web request")


class InvoiceSafetyRegressionTests(unittest.TestCase):
    def setUp(self):
        self.fixed_now = datetime(2026, 6, 15, 12, tzinfo=timezone.utc)
        self.s3 = MemoryS3()
        self.store = S3AutomationStateStore(self.s3, "private", "data/shop/order-automation/state.json", "shop",
                                           now=lambda: self.fixed_now)
        self.order = invoice_order()
        self.api = InvoiceApiFake(self.order)
        self.generator = InvoiceGenerator("https://example.test/api/graphql", "fixture-token", "https://example.test",
                                          page_delay_seconds=0, read_attempts=1)
        self.generator.client = self.api
        self.web = InvoiceWebFake(self.api)
        self.generator.web_session = self.web
        self.generator.validate_session = lambda: True
        self.settings = {"invoice_generation": {
            "enabled": True, "lookback_days": 90, "all_age_backlog_enabled": True,
            "safety_state_enabled": True, "page_delay_seconds": 0,
        }}

    def run_fixture(self, **kwargs):
        self.generator.scan_pages = 0
        with patch("generate_invoices.load_project_env"), \
             patch("generate_invoices.load_project_settings", return_value=self.settings), \
             patch("generate_invoices.resolve_biznisweb_api_url", return_value="https://example.test/api/graphql"), \
             patch("generate_invoices.InvoiceGenerator", return_value=self.generator), \
             patch("generate_invoices.utc_now", return_value=self.fixed_now), \
             patch.dict(os.environ, {"BIZNISWEB_API_TOKEN": "fixture-token"}), \
             patch("generate_invoices.time.sleep"):
            return run_invoice_generation("shop", "2026-06-09", "2026-06-15", state_store=self.store, **kwargs)

    def test_all_age_scan_includes_purchase_years_before_window(self):
        rows = self.generator.fetch_all_eligible_orders()
        self.assertEqual(["fixture-order"], [row["order_num"] for row in rows])
        request = self.api.calls[-1]
        self.assertEqual(4, request["status"])
        self.assertIsInstance(request["status"], int)
        self.assertNotIn("filter", request)
        self.assertNotIn("lang_code", request)

    def test_lease_renews_during_fewer_than_twenty_slow_reads(self):
        elapsed = [0]

        def slow_read(*args, **kwargs):
            elapsed[0] += 120
            self.fixed_now += timedelta(seconds=120)
            return {"getOrder": deepcopy(self.order)}

        self.generator.client = SimpleNamespace(execute=slow_read)
        with self.store.lease("slow-api-run") as journal:
            self.generator.operation_journal = journal
            with patch("generate_invoices.time.monotonic", side_effect=lambda: elapsed[0]):
                for _ in range(3):
                    self.generator.fetch_order_for_invoice("fixture-order")
                self.assertEqual(journal.token, journal.snapshot()["lease"]["token"])
        self.assertEqual(360, elapsed[0])

    def test_foreign_language_uses_verified_shared_status_id(self):
        self.order["status"]["name"] = "Versandt"
        rows = self.generator.fetch_all_eligible_orders()
        self.assertEqual(1, len(self.generator.filter_orders_for_invoice(rows)[0]))
        self.order["status"]["id"] = 9
        self.order["status"]["name"] = "Odoslaná"
        self.assertEqual([], self.generator.filter_orders_for_invoice([self.order])[0])

    def test_present_null_invoice_collection_is_empty_but_missing_is_not(self):
        self.order["invoices"] = None
        self.assertEqual([], self.generator.fetch_order_for_invoice("fixture-order")["invoices"])
        del self.order["invoices"]
        with self.assertRaisesRegex(RuntimeError, "Incomplete"):
            self.generator.fetch_order_for_invoice("fixture-order")

    def test_blocked_order_is_counted_and_never_invoiced(self):
        self.order["blocked"] = True
        result = self.run_fixture()
        self.assertEqual(1, result.skipped_blocked_orders)
        self.assertEqual([], self.web.post_urls)

    def test_fetch_error_does_not_report_empty_success_or_mutate(self):
        self.api.page_error = RuntimeError("HTTP 429 fixture")
        with self.assertRaises(RuntimeError):
            self.run_fixture()
        self.assertEqual([], self.web.post_urls)
        self.assertEqual({}, self.store.read()[0]["last_scan"])

    def test_null_partial_page_and_nonadvancing_cursor_fail(self):
        for pages in ([invoice_page([None])],
                      [invoice_page([invoice_order("one")], 30), invoice_page([invoice_order("two")], 30)]):
            with self.subTest(pages=len(pages)):
                self.generator.scan_pages = 0
                self.generator.client = SimpleNamespace(execute=lambda *a, **k: pages.pop(0))
                with self.assertRaises(RuntimeError):
                    self.generator._fetch_order_pages()

    def test_partial_graphql_exception_is_not_accepted(self):
        error = RuntimeError("partial response")
        error.data = invoice_page([self.order])
        self.api.page_error = error
        with self.assertRaises(RuntimeError):
            self.generator._fetch_order_pages()

    def test_page_limit_does_not_silently_truncate(self):
        self.generator.scan_max_pages = 1
        self.generator.client = SimpleNamespace(execute=lambda *a, **k: invoice_page([self.order], 30))
        with self.assertRaisesRegex(RuntimeError, "limit"):
            self.generator._fetch_order_pages()

    @patch("generate_invoices.time.sleep")
    def test_retry_reads_recovers_transient_failure_and_is_bounded(self, _sleep):
        self.generator.read_attempts = 3
        attempts = [RuntimeError("429"), invoice_page([self.order])]
        def execute(*args, **kwargs):
            result = attempts.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        self.generator.client = SimpleNamespace(execute=execute)
        self.assertEqual(1, len(self.generator._fetch_order_pages()))
        self.assertEqual([], attempts)

    def test_dry_run_has_no_s3_or_web_writes(self):
        summary = self.run_fixture(dry_run=True, full_backlog=True)
        self.assertTrue(summary.invoice_scan_complete)
        self.assertEqual(1, summary.matched_orders)
        self.assertEqual([], self.web.post_urls)
        self.assertEqual([], self.s3.writes)

    def test_existing_invoice_and_status_change_stop_creation(self):
        self.order["invoices"] = [{"id": "existing", "invoice_num": "existing-number"}]
        self.assertTrue(self.generator.create_invoice(self.order).skipped)
        self.assertEqual([], self.web.post_urls)
        self.order["invoices"] = []
        self.web.change_status_during_prepare = True
        self.assertTrue(self.generator.create_invoice(self.order).skipped)
        self.assertEqual(0, sum("/finalize/" in url for url in self.web.post_urls))

    def test_creation_timeout_reads_back_without_repeating_mutation(self):
        self.web.finalize_timeout_after_commit = True
        summary = self.run_fixture()
        self.assertEqual(1, summary.recovered_invoices)
        self.assertEqual(0, summary.failed_invoices)
        self.assertEqual(1, sum("/finalize/" in url for url in self.web.post_urls))
        self.assertEqual(1, sum("/sendEmail/" in url for url in self.web.post_urls))

    def test_failed_old_creation_remains_in_backlog_after_watermark_moves(self):
        self.web.finalize_rejected = True
        first = self.run_fixture()
        self.assertEqual(1, first.failed_invoices)
        self.assertEqual(1, first.pending_invoice_operations)
        self.assertTrue(first.invoice_scan_all_ages)
        self.web.finalize_rejected = False
        second = self.run_fixture()
        self.assertFalse(second.invoice_scan_all_ages)
        self.assertEqual(1, second.created_invoices)
        self.assertEqual(0, second.pending_invoice_operations)

    def test_email_failure_is_retried_next_run_without_recreating_invoice(self):
        self.web.email_success = False
        first = self.run_fixture()
        self.assertEqual(1, first.failed_invoice_emails)
        self.assertEqual(1, first.pending_invoice_emails)
        self.web.email_success = True
        second = self.run_fixture()
        self.assertEqual(1, second.emailed_invoices)
        self.assertEqual(0, second.pending_invoice_operations)
        self.assertEqual(1, sum("/finalize/" in url for url in self.web.post_urls))

    def test_historical_email_hold_survives_creation_and_later_runs(self):
        with self.store.lease("verified-backlog-seed") as journal:
            journal.update_order("fixture-order", phase="pending", email_policy="hold")
        first = self.run_fixture()
        self.assertEqual(1, first.created_invoices)
        self.assertEqual(0, first.emailed_invoices)
        self.assertEqual(0, first.failed_invoice_emails)
        self.assertEqual(0, first.pending_invoice_operations)
        second = self.run_fixture()
        self.assertEqual(0, second.emailed_invoices)
        self.assertEqual(1, sum("/finalize/" in url for url in self.web.post_urls))
        self.assertEqual(0, sum("/sendEmail/" in url for url in self.web.post_urls))
        self.assertEqual("held", self.store.read()[0]["orders"]["fixture-order"]["email_state"])

    def test_email_hold_cannot_clear_an_already_ambiguous_send(self):
        with self.store.lease("prior-run") as journal:
            journal.update_order("fixture-order", phase="email", email_state="ambiguous",
                                 invoice_id="fixture-invoice", email_policy="hold")
        self.order["invoices"] = [{"id": "fixture-invoice", "invoice_num": "fixture-number"}]
        result = self.run_fixture()
        self.assertEqual(1, result.ambiguous_invoice_operations)
        self.assertEqual(1, result.pending_invoice_operations)
        self.assertEqual([], self.web.post_urls)

    def test_ambiguous_email_is_persistent_and_not_resent(self):
        self.web.email_timeout = True
        first = self.run_fixture()
        self.assertEqual(1, first.ambiguous_invoice_operations)
        second = self.run_fixture()
        self.assertEqual(1, second.ambiguous_invoice_operations)
        self.assertEqual(1, second.pending_invoice_operations)
        self.assertEqual(1, sum("/sendEmail/" in url for url in self.web.post_urls))

    def test_login_html_cannot_claim_email_success(self):
        response = _FakeInvoiceResponse("https://example.test/login", None)
        response.text = "<html><form>Please sign in</form></html>"
        self.generator.web_session = SimpleNamespace(post=lambda *a, **k: response)
        self.assertFalse(self.generator.send_invoice_email("fixture-invoice"))
        self.assertEqual("ambiguous", self.generator.last_email_outcome)

    def test_uncertain_creation_with_no_invoice_is_not_replayed(self):
        with self.store.lease("prior-run") as journal:
            journal.update_order("fixture-order", phase="create_ambiguous")
        summary = self.run_fixture()
        self.assertEqual(1, summary.ambiguous_invoice_operations)
        self.assertEqual([], self.web.post_urls)

    def test_uncertain_creation_with_invoice_is_recovered_from_journal(self):
        self.order["invoices"] = [{"id": "fixture-invoice", "invoice_num": "fixture-number"}]
        with self.store.lease("prior-run") as journal:
            journal.update_order("fixture-order", phase="creating")
        summary = self.run_fixture()
        self.assertEqual(1, summary.recovered_invoices)
        self.assertEqual(1, summary.emailed_invoices)
        self.assertEqual(0, summary.pending_invoice_operations)
        self.assertEqual(0, sum("/finalize/" in url for url in self.web.post_urls))

    def test_pending_order_cancelled_before_retry_is_retired_without_creation(self):
        with self.store.lease("prior-run") as journal:
            journal.update_order("fixture-order", phase="create_failed")
        self.order["status"] = {"id": 99, "name": "Storno"}
        result = self.run_fixture()
        self.assertEqual(1, result.skipped_after_recheck_orders)
        self.assertEqual(0, result.pending_invoice_operations)
        self.assertEqual([], self.web.post_urls)

    def test_status_review_survives_creditnote_failure_and_watermark(self):
        self.settings["invoice_generation"]["existing_invoice_status_reconciliation"] = {"enabled": True}
        self.order.update(status={"id": 69, "name": "Stripe - expired"},
                          invoices=[{"id": "fixture-invoice", "invoice_num": "fixture-number"}],
                          last_change="2026-06-15 10:30:00")
        with patch("creditnote_export.fetch_creditnote_automation_context", side_effect=RuntimeError("incomplete context")):
            with self.assertRaises(RuntimeError):
                self.run_fixture()
        saved = self.store.read()[0]
        self.assertEqual("open", saved["orders"]["fixture-order"]["status_review"]["state"])
        self.assertTrue(saved["last_scan"]["changed_watermark"])
        with patch("creditnote_export.fetch_creditnote_automation_context", return_value={}) as context:
            second = self.run_fixture()
        self.assertEqual("shop", context.call_args.args[0])
        self.assertTrue(callable(context.call_args.kwargs["progress_callback"]))
        self.assertFalse(second.invoice_scan_all_ages)
        self.assertEqual(1, second.invoice_status_review_required)
        self.assertEqual([], self.web.post_urls)


if __name__ == "__main__":
    unittest.main()
