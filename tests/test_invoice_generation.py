import argparse
import hashlib
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import daily_report_runner as daily_runner
from daily_report_runner import maybe_run_invoice_automation, parse_args as parse_daily_report_args
from generate_invoices import (
    IncompleteInvoiceScanError,
    InvoiceGenerator,
    InvoiceRunSummary,
    ORDER_QUERY,
    _status_matches_invoice_generation,
    resolve_invoice_date_window,
    resolve_invoice_generation_settings,
    run_invoice_generation,
    validate_invoice_creation_limit,
)
from invoice_runner import (
    resolve_default_invoice_reference_date,
    resolve_invoice_runner_window,
    run_invoice_runner,
)


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
        self.execute_count = 0

    def execute(self, query, variable_values=None):
        self.execute_count += 1
        return {
            "getOrder": {
                "order_num": (variable_values or {}).get("order_num"),
                "price_elements": [
                    {
                        "type": "payment",
                        "title": "Dobierkou",
                        "reference_id": "7",
                    }
                ],
                "status": {"id": "4", "name": "Odoslaná"},
                "sum": {"value": 12.5, "formatted": "12.50 EUR"},
                "invoices": [] if self.execute_count == 1 else self.invoices,
            }
        }


class _FakeOrderListClient:
    def __init__(self, responses: list[dict | Exception]) -> None:
        self.responses = list(responses)
        self.variables: list[dict] = []

    def execute(self, query, variable_values=None):
        self.variables.append(variable_values or {})
        if not self.responses:
            raise AssertionError("Unexpected extra GraphQL request")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class _StaticOrderGuardClient:
    def __init__(self, order: dict) -> None:
        self.order = order
        self.execute_count = 0

    def execute(self, query, variable_values=None):
        self.execute_count += 1
        return {"getOrder": self.order}


def _invoice_order(
    order_num: str,
    *,
    pur_date: str,
    last_change: str,
    total: float | None = 12.5,
    invoices: list[dict] | None = None,
    payment_reference_id: str | None = "7",
    payment_title: str | None = "Dobierkou",
) -> dict:
    return {
        "id": f"id-{order_num}",
        "order_num": order_num,
        "pur_date": pur_date,
        "last_change": last_change,
        "price_elements": (
            [
                {
                    "type": "payment",
                    "title": payment_title,
                    "reference_id": payment_reference_id,
                }
            ]
            if payment_reference_id is not None or payment_title is not None
            else []
        ),
        "status": {"id": "4", "name": "Odoslaná"},
        "invoices": [] if invoices is None else invoices,
        "sum": {"value": total, "formatted": "" if total is None else str(total)},
    }


def _order_page(
    orders: list[dict | None],
    *,
    has_next: bool = False,
    next_cursor: str | int | None = None,
) -> dict:
    return {
        "getOrderList": {
            "data": orders,
            "pageInfo": {
                "hasNextPage": has_next,
                "nextCursor": next_cursor,
            },
        }
    }


class InvoiceGenerationTests(unittest.TestCase):
    def test_invoice_creation_limit_allows_dry_run_preview_above_limit(self) -> None:
        validate_invoice_creation_limit(4, 0, dry_run=True)

    def test_invoice_creation_limit_allows_live_run_at_limit(self) -> None:
        validate_invoice_creation_limit(4, 4, dry_run=False)

    def test_invoice_creation_limit_blocks_live_run_before_mutation(self) -> None:
        with self.assertRaisesRegex(
            RuntimeError,
            "matched=4, max_creations=0",
        ):
            validate_invoice_creation_limit(4, 0, dry_run=False)

    def test_invoice_creation_limit_rejects_negative_limit(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot be negative"):
            validate_invoice_creation_limit(0, -1, dry_run=True)

    @patch("generate_invoices.InvoiceGenerator")
    @patch(
        "generate_invoices.resolve_biznisweb_api_url",
        return_value="https://example.com/api/graphql",
    )
    @patch(
        "generate_invoices.load_project_settings",
        return_value={
            "invoice_generation": {
                "enabled": True,
                "automation_start_date": "2026-07-17",
                "eligible_statuses": ["odoslaná"],
                "send_invoice_email": True,
            }
        },
    )
    @patch("generate_invoices.load_project_env")
    def test_live_creation_limit_blocks_before_create_loop(
        self,
        _load_env_mock,
        _load_settings_mock,
        _resolve_api_mock,
        generator_class_mock,
    ) -> None:
        generator = MagicMock()
        generator.web_session = object()
        generator.validate_session.return_value = True
        generator.fetch_orders_for_invoice_scan.return_value = (
            [{"order_num": "A"}, {"order_num": "B"}],
            {
                "scan_complete": True,
                "purchase_date_orders_fetched": 2,
                "recent_change_orders_fetched": 0,
                "pages_fetched": 1,
                "page_retry_count": 0,
            },
        )
        generator.filter_orders_for_invoice.return_value = (
            [{"order_num": "A"}, {"order_num": "B"}],
            {
                "skipped_zero_total_orders": 0,
                "skipped_non_cod_orders": 0,
            },
        )
        generator_class_mock.return_value = generator

        with patch.dict(
            os.environ,
            {"BIZNISWEB_API_TOKEN": "token"},
            clear=False,
        ), self.assertRaisesRegex(
            RuntimeError,
            "matched=2, max_creations=0",
        ):
            run_invoice_generation(
                "vevo",
                "2026-07-17",
                "2026-07-17",
                max_creations=0,
            )

        generator.create_invoice.assert_not_called()
        self.assertEqual(
            "2026-07-17",
            generator_class_mock.call_args.kwargs["automation_start_date"],
        )

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
        settings = resolve_invoice_generation_settings(
            {
                "invoice_generation": {
                    "enabled": True,
                    "automation_start_date": "2026-07-17",
                }
            }
        )
        self.assertTrue(settings["enabled"])
        self.assertEqual(settings["lookback_days"], 7)
        self.assertEqual(settings["status_change_lookback_days"], 7)
        self.assertEqual(settings["reconciliation_lookback_days"], 120)
        self.assertTrue(settings["include_recent_changes"])
        self.assertEqual(settings["page_retry_attempts"], 3)
        self.assertEqual(settings["max_pages"], 1000)
        self.assertEqual(settings["rollover_grace_hours"], 3)
        self.assertTrue(settings["exclude_zero_total_orders"])
        self.assertFalse(settings["require_cod_payment"])
        self.assertTrue(settings["send_invoice_email"])
        self.assertEqual("2026-07-17", settings["automation_start_date"])
        self.assertEqual(["Odoslan\u00e1"], settings["eligible_statuses"])

    def test_invoice_generation_settings_validate_automation_start_date(self) -> None:
        settings = resolve_invoice_generation_settings(
            {
                "invoice_generation": {
                    "enabled": True,
                    "automation_start_date": "2026-07-17",
                }
            }
        )
        self.assertEqual("2026-07-17", settings["automation_start_date"])

        for invalid_value in ("2026-7-17", "2026-02-30", "17.07.2026"):
            with self.subTest(invalid_value=invalid_value), self.assertRaises(ValueError):
                resolve_invoice_generation_settings(
                    {
                        "invoice_generation": {
                            "enabled": True,
                            "automation_start_date": invalid_value,
                        }
                    }
                )

        with self.assertRaisesRegex(ValueError, "is required when"):
            resolve_invoice_generation_settings(
                {"invoice_generation": {"enabled": True}}
            )

        disabled = resolve_invoice_generation_settings(
            {"invoice_generation": {"enabled": False}}
        )
        self.assertEqual("", disabled["automation_start_date"])

    def test_invoice_generation_settings_can_disable_invoice_email(self) -> None:
        settings = resolve_invoice_generation_settings(
            {
                "invoice_generation": {
                    "enabled": True,
                    "automation_start_date": "2026-07-17",
                    "send_invoice_email": False,
                }
            }
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
        settings = {
            "invoice_generation": {
                "enabled": True,
                "automation_start_date": "2026-07-17",
                "lookback_days": 7,
                "status_change_lookback_days": 7,
            }
        }
        from_date, to_date = resolve_invoice_runner_window(
            settings,
            timezone_name="Europe/Bratislava",
            reference_date="2026-04-28",
        )
        self.assertEqual(("2026-04-22", "2026-04-28"), (from_date, to_date))

    def test_invoice_runner_reconciliation_uses_extended_window(self) -> None:
        settings = {
            "invoice_generation": {
                "enabled": True,
                "automation_start_date": "2026-07-17",
                "lookback_days": 7,
                "status_change_lookback_days": 7,
                "reconciliation_lookback_days": 120,
            }
        }
        from_date, to_date = resolve_invoice_runner_window(
            settings,
            timezone_name="Europe/Bratislava",
            reference_date="2026-06-30",
            reconcile=True,
        )
        self.assertEqual(("2026-03-03", "2026-06-30"), (from_date, to_date))

    def test_invoice_runner_midnight_grace_uses_previous_local_day(self) -> None:
        self.assertEqual(
            "2026-06-30",
            resolve_default_invoice_reference_date(
                "Europe/Bratislava",
                3,
                current_datetime=datetime.fromisoformat("2026-07-01T00:00:17+02:00"),
            ),
        )

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
        self.assertEqual("cron(30 0 * * ? *)", vevo["invoice_generation"]["final_sweep_schedule_expression"])
        self.assertEqual("cron(45 0 * * ? *)", roy["invoice_generation"]["final_sweep_schedule_expression"])
        self.assertEqual(["Odoslan\u00e1"], vevo["invoice_generation"]["eligible_statuses"])
        self.assertEqual(["Odoslan\u00e1"], roy["invoice_generation"]["eligible_statuses"])
        self.assertTrue(vevo["invoice_generation"]["send_invoice_email"])
        self.assertTrue(roy["invoice_generation"]["send_invoice_email"])
        for project in (vevo, roy):
            invoice_settings = project["invoice_generation"]
            self.assertEqual(7, invoice_settings["status_change_lookback_days"])
            self.assertEqual(120, invoice_settings["reconciliation_lookback_days"])
            self.assertTrue(invoice_settings["include_recent_changes"])
            self.assertEqual(4, invoice_settings["page_retry_attempts"])
            self.assertEqual(1000, invoice_settings["max_pages"])
            self.assertEqual(3, invoice_settings["rollover_grace_hours"])
            self.assertTrue(invoice_settings["require_cod_payment"])
            self.assertEqual("2026-07-17", invoice_settings["automation_start_date"])
            resolved = resolve_invoice_generation_settings(project)
            self.assertIn("7", resolved["cod_payment_ids"])
            self.assertEqual("2026-07-17", resolved["automation_start_date"])

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

    def test_filter_excludes_orders_purchased_before_automation_start(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            automation_start_date="2026-07-17",
        )
        filtered, stats = generator.filter_orders_for_invoice(
            [
                _invoice_order(
                    "OLD",
                    pur_date="2026-07-16 23:59:59",
                    last_change="2026-07-17 08:00:00",
                ),
                _invoice_order(
                    "START",
                    pur_date="2026-07-17 00:00:00",
                    last_change="2026-07-17 08:00:00",
                ),
                _invoice_order(
                    "NEW",
                    pur_date="2026-07-18 09:00:00",
                    last_change="2026-07-18 09:01:00",
                ),
            ]
        )

        self.assertEqual(
            ["START", "NEW"],
            [order["order_num"] for order in filtered],
        )
        self.assertEqual(1, stats["skipped_before_automation_start"])

    def test_filter_fails_closed_on_invalid_purchase_date_with_cutoff(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            automation_start_date="2026-07-17",
        )
        for invalid_value in ("", "2026/07/17", "2026-02-30"):
            order = _invoice_order(
                f"INVALID-{invalid_value}",
                pur_date=invalid_value,
                last_change="2026-07-17 08:00:00",
            )
            with self.subTest(invalid_value=invalid_value), self.assertRaisesRegex(
                ValueError,
                "Invalid pur_date",
            ):
                generator.filter_orders_for_invoice([order])

    def test_regular_and_reconciliation_runs_apply_automation_start_cutoff(self) -> None:
        old_order = _invoice_order(
            "OLD-IN-BOTH-MODES",
            pur_date="2026-07-16 23:59:59",
            last_change="2026-07-17 08:00:00",
        )
        scan_stats = {
            "scan_complete": True,
            "purchase_date_orders_fetched": 1,
            "recent_change_orders_fetched": 1,
            "pages_fetched": 1,
            "page_retry_count": 0,
        }
        settings = {
            "invoice_generation": {
                "enabled": True,
                "automation_start_date": "2026-07-17",
                "require_cod_payment": False,
            }
        }

        with (
            patch("generate_invoices.load_project_env"),
            patch("generate_invoices.load_project_settings", return_value=settings),
            patch(
                "generate_invoices.resolve_biznisweb_api_url",
                return_value="https://example.com/api/graphql",
            ),
            patch.object(
                InvoiceGenerator,
                "fetch_orders_for_invoice_scan",
                return_value=([old_order], scan_stats),
            ) as fetch_mock,
            patch.dict(os.environ, {"BIZNISWEB_API_TOKEN": "token"}, clear=False),
        ):
            for reconcile in (False, True):
                with self.subTest(reconcile=reconcile):
                    summary = run_invoice_generation(
                        "vevo",
                        "2026-06-01",
                        "2026-07-17",
                        dry_run=True,
                        no_web_login=True,
                        reconcile=reconcile,
                    )
                    self.assertEqual(0, summary.matched_orders)
                    self.assertEqual(1, summary.skipped_before_automation_start)
            self.assertEqual(2, fetch_mock.call_count)
            for call in fetch_mock.call_args_list:
                self.assertEqual(
                    datetime(2026, 7, 17),
                    call.args[0],
                )

    def test_pre_create_guard_blocks_order_before_automation_start(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            automation_start_date="2026-07-17",
        )
        old_order = _invoice_order(
            "OLD-GUARD",
            pur_date="2026-07-16 23:59:59",
            last_change="2026-07-17 08:00:00",
        )
        generator.client = _StaticOrderGuardClient(old_order)
        generator.web_session = _FakeInvoiceWebSession()

        result = generator.create_invoice(old_order)

        self.assertTrue(result.skipped_before_automation_start)
        self.assertFalse(result.created)
        self.assertEqual([], generator.web_session.post_urls)

    def test_filter_requires_cash_on_delivery_when_configured(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            require_cod_payment=True,
            cod_payment_ids=["7"],
            cod_payment_patterns=["cash on delivery"],
        )
        filtered, stats = generator.filter_orders_for_invoice(
            [
                _invoice_order(
                    "COD-ID",
                    pur_date="2026-06-30 10:00:00",
                    last_change="2026-06-30 10:01:00",
                ),
                _invoice_order(
                    "PREPAID",
                    pur_date="2026-06-30 10:00:00",
                    last_change="2026-06-30 10:01:00",
                    payment_reference_id="6",
                    payment_title="Bankovym prevodom",
                ),
                _invoice_order(
                    "COD-TITLE",
                    pur_date="2026-06-30 10:00:00",
                    last_change="2026-06-30 10:01:00",
                    payment_reference_id="999",
                    payment_title="Cash on delivery",
                ),
            ]
        )

        self.assertEqual(
            ["COD-ID", "COD-TITLE"],
            [order["order_num"] for order in filtered],
        )
        self.assertEqual(1, stats["skipped_non_cod_orders"])

    def test_filter_fails_closed_when_cod_payment_metadata_is_missing(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            require_cod_payment=True,
            cod_payment_ids=["7"],
        )
        order = _invoice_order(
            "PAYMENT-UNKNOWN",
            pur_date="2026-06-30 10:00:00",
            last_change="2026-06-30 10:01:00",
            payment_reference_id=None,
            payment_title=None,
        )

        with self.assertRaises(IncompleteInvoiceScanError):
            generator.filter_orders_for_invoice([order])

    def test_filter_fetches_cod_detail_only_for_an_eligible_candidate(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            require_cod_payment=True,
            cod_payment_ids=["7"],
        )
        generator.client = _FakeInvoiceClient([])
        order = _invoice_order(
            "COD-DETAIL",
            pur_date="2026-06-30 10:00:00",
            last_change="2026-06-30 10:01:00",
        )
        del order["price_elements"]

        filtered, stats = generator.filter_orders_for_invoice([order])

        self.assertEqual(["COD-DETAIL"], [item["order_num"] for item in filtered])
        self.assertEqual(0, stats["skipped_non_cod_orders"])
        self.assertEqual(1, generator.client.execute_count)

    def test_filter_does_not_fetch_payment_detail_for_ineligible_orders(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            require_cod_payment=True,
            cod_payment_ids=["7"],
        )
        generator.client = _FakeOrderListClient([])
        wrong_status = _invoice_order(
            "WRONG-STATUS",
            pur_date="2026-06-30 10:00:00",
            last_change="2026-06-30 10:01:00",
        )
        wrong_status["status"] = {"id": "1", "name": "Nová"}
        already_invoiced = _invoice_order(
            "INVOICED",
            pur_date="2026-06-30 10:00:00",
            last_change="2026-06-30 10:01:00",
            invoices=[{"id": "INV-1"}],
        )
        zero_total = _invoice_order(
            "ZERO",
            pur_date="2026-06-30 10:00:00",
            last_change="2026-06-30 10:01:00",
            total=0,
        )
        for order in (wrong_status, already_invoiced, zero_total):
            del order["price_elements"]

        filtered, stats = generator.filter_orders_for_invoice(
            [wrong_status, already_invoiced, zero_total]
        )

        self.assertEqual([], filtered)
        self.assertEqual(1, stats["skipped_zero_total_orders"])
        self.assertEqual(0, len(generator.client.variables))

    def test_filter_skips_candidate_when_detail_refresh_finds_invoice(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            require_cod_payment=True,
            cod_payment_ids=["7"],
            page_retry_attempts=1,
        )
        generator.client = _StaticOrderGuardClient(
            {
                "order_num": "RACE",
                "price_elements": [
                    {
                        "type": "payment",
                        "title": "Dobierkou",
                        "reference_id": "7",
                    }
                ],
                "status": {"id": "4", "name": "Odoslaná"},
                "sum": {"value": 12.5, "formatted": "12.50 EUR"},
                "invoices": [{"id": "INV-RACE"}],
            }
        )
        order = _invoice_order(
            "RACE",
            pur_date="2026-06-30 10:00:00",
            last_change="2026-06-30 10:01:00",
        )
        del order["price_elements"]

        filtered, _stats = generator.filter_orders_for_invoice([order])

        self.assertEqual([], filtered)
        self.assertEqual(1, generator.client.execute_count)

    def test_bulk_invoice_scan_does_not_request_payment_metadata(self) -> None:
        self.assertNotIn("price_elements", str(ORDER_QUERY))

    def test_status_change_scan_finds_old_purchase_changed_recently(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=1,
        )
        generator.client = _FakeOrderListClient(
            [
                _order_page(
                    [
                        _invoice_order(
                            "OLD-1",
                            pur_date="2026-05-17 13:48:02",
                            last_change="2026-06-02 11:46:10",
                        )
                    ]
                )
            ]
        )

        orders, stats = generator.fetch_orders_for_invoice_scan(
            datetime(2026, 5, 27),
            datetime(2026, 6, 2),
            include_purchase_dates=False,
            include_recent_changes=True,
        )

        self.assertEqual(["OLD-1"], [order["order_num"] for order in orders])
        self.assertEqual(0, stats["purchase_date_orders_fetched"])
        self.assertEqual(1, stats["recent_change_orders_fetched"])
        self.assertEqual("last_change", generator.client.variables[0]["params"]["order_by"])

    @patch("generate_invoices.time.sleep", return_value=None)
    def test_strict_scan_raises_on_first_page_failure(self, _sleep_mock) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=2,
        )
        generator.client = _FakeOrderListClient(
            [RuntimeError("quota exceeded"), RuntimeError("quota exceeded")]
        )

        with self.assertRaises(IncompleteInvoiceScanError):
            generator.fetch_orders(
                datetime(2026, 6, 24),
                datetime(2026, 6, 30),
                date_field="pur_date",
            )

        self.assertEqual(2, len(generator.client.variables))
        self.assertFalse(generator.last_fetch_stats["scan_complete"])

    @patch("generate_invoices.time.sleep", return_value=None)
    def test_strict_scan_raises_on_later_page_failure(self, _sleep_mock) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=2,
        )
        generator.client = _FakeOrderListClient(
            [
                _order_page(
                    [
                        _invoice_order(
                            "RECENT-1",
                            pur_date="2026-06-30 10:00:00",
                            last_change="2026-06-30 10:01:00",
                        )
                    ],
                    has_next=True,
                    next_cursor="cursor-2",
                ),
                RuntimeError("non-json response"),
                RuntimeError("non-json response"),
            ]
        )

        with self.assertRaises(IncompleteInvoiceScanError):
            generator.fetch_orders(
                datetime(2026, 6, 24),
                datetime(2026, 6, 30),
                date_field="pur_date",
            )

        self.assertEqual(3, len(generator.client.variables))
        self.assertEqual("cursor-2", generator.client.variables[1]["params"]["cursor"])

    @patch("generate_invoices.time.sleep", return_value=None)
    def test_strict_scan_rejects_partial_graphql_rows(self, _sleep_mock) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=1,
        )
        generator.client = _FakeOrderListClient(
            [
                _order_page(
                    [
                        None,
                        _invoice_order(
                            "VALID-1",
                            pur_date="2026-06-30 10:00:00",
                            last_change="2026-06-30 10:01:00",
                        ),
                    ]
                )
            ]
        )

        with self.assertRaises(IncompleteInvoiceScanError):
            generator.fetch_orders(
                datetime(2026, 6, 24),
                datetime(2026, 6, 30),
                date_field="pur_date",
            )

    @patch("generate_invoices.time.sleep", return_value=None)
    def test_strict_scan_rejects_incomplete_invoice_entries(self, _sleep_mock) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=1,
        )
        generator.client = _FakeOrderListClient(
            [
                _order_page(
                    [
                        _invoice_order(
                            "INVALID-INVOICE",
                            pur_date="2026-06-30 10:00:00",
                            last_change="2026-06-30 10:01:00",
                            invoices=[{}],
                        )
                    ]
                )
            ]
        )

        with self.assertRaises(IncompleteInvoiceScanError):
            generator.fetch_orders(
                datetime(2026, 6, 24),
                datetime(2026, 6, 30),
                date_field="pur_date",
            )

    def test_pre_create_guard_rejects_mismatched_order_identity(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=1,
        )
        generator.client = _StaticOrderGuardClient(
            {
                "order_num": "WRONG",
                "status": {"name": "Odoslaná"},
                "sum": {"value": 12.5},
                "invoices": [],
            }
        )

        self.assertIsNone(generator.fetch_order_invoice_guard("EXPECTED"))
        self.assertEqual(1, generator.client.execute_count)

    def test_pre_create_guard_rejects_incomplete_invoice_entries(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=1,
        )
        generator.client = _StaticOrderGuardClient(
            {
                "order_num": "EXPECTED",
                "status": {"name": "Odoslaná"},
                "sum": {"value": 12.5},
                "invoices": [None],
            }
        )

        self.assertIsNone(generator.fetch_order_invoice_guard("EXPECTED"))
        self.assertEqual(1, generator.client.execute_count)

    def test_invoice_fallback_rejects_mismatched_order_identity(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
        )
        generator.client = _StaticOrderGuardClient(
            {
                "order_num": "WRONG",
                "invoices": [{"id": "INV-WRONG"}],
            }
        )

        self.assertEqual(
            (None, None),
            generator.fetch_latest_invoice_for_order("EXPECTED"),
        )

    @patch("generate_invoices.time.sleep", return_value=None)
    def test_strict_scan_rejects_malformed_boundary_date(self, _sleep_mock) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=1,
        )
        malformed = _invoice_order(
            "MALFORMED",
            pur_date="2026-06-30 10:00:00",
            last_change="2026-06-30 10:01:00",
        )
        malformed["pur_date"] = "0000"
        generator.client = _FakeOrderListClient(
            [
                _order_page(
                    [malformed],
                    has_next=True,
                    next_cursor="cursor-2",
                ),
                _order_page(
                    [
                        _invoice_order(
                            "HIDDEN",
                            pur_date="2026-06-29 10:00:00",
                            last_change="2026-06-29 10:01:00",
                        )
                    ]
                ),
            ]
        )

        with self.assertRaises(IncompleteInvoiceScanError):
            generator.fetch_orders(
                datetime(2026, 6, 24),
                datetime(2026, 6, 30),
                date_field="pur_date",
            )

        self.assertEqual(1, len(generator.client.variables))

    @patch("generate_invoices.time.sleep", return_value=None)
    def test_strict_scan_rejects_desc_order_drift_between_pages(
        self,
        _sleep_mock,
    ) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=1,
        )
        generator.client = _FakeOrderListClient(
            [
                _order_page(
                    [
                        _invoice_order(
                            "PAGE-1",
                            pur_date="2026-06-29 10:00:00",
                            last_change="2026-06-29 10:01:00",
                        )
                    ],
                    has_next=True,
                    next_cursor="cursor-2",
                ),
                _order_page(
                    [
                        _invoice_order(
                            "PAGE-2-NEWER",
                            pur_date="2026-06-30 10:00:00",
                            last_change="2026-06-30 10:01:00",
                        )
                    ]
                ),
            ]
        )

        with self.assertRaises(IncompleteInvoiceScanError):
            generator.fetch_orders(
                datetime(2026, 6, 24),
                datetime(2026, 6, 30),
                date_field="pur_date",
            )

        self.assertEqual(2, len(generator.client.variables))

    @patch("generate_invoices.time.sleep", return_value=None)
    def test_strict_scan_rejects_missing_has_next_page(self, _sleep_mock) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=1,
        )
        generator.client = _FakeOrderListClient(
            [
                {
                    "getOrderList": {
                        "data": [
                            _invoice_order(
                                "VALID-1",
                                pur_date="2026-06-30 10:00:00",
                                last_change="2026-06-30 10:01:00",
                            )
                        ],
                        "pageInfo": {},
                    }
                }
            ]
        )

        with self.assertRaises(IncompleteInvoiceScanError):
            generator.fetch_orders(
                datetime(2026, 6, 24),
                datetime(2026, 6, 30),
                date_field="pur_date",
            )

    def test_strict_scan_fails_closed_at_max_pages(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            page_retry_attempts=1,
            max_pages=1,
        )
        generator.client = _FakeOrderListClient(
            [
                _order_page(
                    [
                        _invoice_order(
                            "RECENT-1",
                            pur_date="2026-06-30 10:00:00",
                            last_change="2026-06-30 10:01:00",
                        )
                    ],
                    has_next=True,
                    next_cursor=30,
                )
            ]
        )

        with self.assertRaises(IncompleteInvoiceScanError):
            generator.fetch_orders(
                datetime(2026, 6, 24),
                datetime(2026, 6, 30),
                date_field="pur_date",
            )

    def test_eligible_order_with_unknown_total_fails_closed(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
        )
        with self.assertRaises(IncompleteInvoiceScanError):
            generator.filter_orders_for_invoice(
                [
                    _invoice_order(
                        "UNKNOWN-TOTAL",
                        pur_date="2026-06-30 10:00:00",
                        last_change="2026-06-30 10:01:00",
                        total=None,
                    )
                ]
            )

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

    def test_create_invoice_is_idempotent_when_guard_finds_existing_invoice(self) -> None:
        generator = InvoiceGenerator(
            api_url="https://example.com/api/graphql",
            api_token="token",
            base_url="https://example.com",
            send_invoice_email=True,
        )
        generator.web_session = _FakeInvoiceWebSession()
        generator.client = _FakeInvoiceClient([{"id": "INV-EXISTING", "invoice_num": "FV-EXISTING"}])
        generator.client.execute_count = 1

        result = generator.create_invoice(
            {
                "id": "ORDER-ID",
                "order_num": "1001",
                "status": {"name": "Odoslaná"},
                "sum": {"value": 12.5, "formatted": "12.50 EUR"},
            }
        )

        self.assertTrue(result.already_present)
        self.assertFalse(result.created)
        self.assertEqual("INV-EXISTING", result.invoice_id)
        self.assertEqual([], generator.web_session.post_urls)

    @patch("invoice_runner.put_metric")
    @patch("invoice_runner.run_invoice_generation")
    @patch("invoice_runner.resolve_reporting_defaults", return_value={"cloudwatch_namespace": "Test"})
    @patch(
        "invoice_runner.load_project_settings",
        return_value={
            "invoice_generation": {
                "enabled": True,
                "automation_start_date": "2026-07-17",
                "lookback_days": 7,
                "status_change_lookback_days": 7,
                "reconciliation_lookback_days": 120,
            }
        },
    )
    @patch("invoice_runner.load_project_env")
    def test_failed_summary_emits_failed_without_succeeded(
        self,
        _load_env_mock,
        _load_settings_mock,
        _resolve_defaults_mock,
        run_generation_mock,
        put_metric_mock,
    ) -> None:
        run_generation_mock.return_value = InvoiceRunSummary(
            project="vevo",
            date_from="2026-06-24",
            date_to="2026-06-30",
            matched_orders=1,
            failed_invoices=1,
            scan_complete=True,
        )
        args = argparse.Namespace(
            project="vevo",
            reference_date="2026-06-30",
            from_date="",
            to_date="",
            timezone="Europe/Bratislava",
            dry_run=False,
            no_web_login=False,
            reconcile=False,
        )

        with self.assertRaises(RuntimeError):
            run_invoice_runner(args)

        metric_names = [call.args[0] for call in put_metric_mock.call_args_list]
        self.assertIn("InvoiceStandaloneRunFailed", metric_names)
        self.assertNotIn("InvoiceStandaloneRunSucceeded", metric_names)

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
                "automation_start_date": "2026-07-17",
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
            skipped_before_automation_start=4,
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
                "skipped_before_automation_start": 4,
                "dry_run": True,
            },
            result,
        )
        self.assertEqual(9, put_metric_mock.call_count)


if __name__ == "__main__":
    unittest.main()
