from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import importlib
import io
import json
import logging
import os
import unittest
from unittest.mock import Mock, patch

import creditnote_storno_runner as runner
from creditnote_storno_guard import CreditnoteStornoSummary, run_creditnote_storno_guard
from invoice_automation_state import AutomationLeaseBusy, AutomationStateError
from tests.test_creditnote_storno_guard import FakeExporter, creditnote_row, order_row


class CreditnoteStandaloneRunnerTests(unittest.TestCase):
    def setUp(self):
        self.settings = {"biznisweb_api_url": "https://roy.flox.sk/api/graphql", "creditnote_storno_guard": {
            "enabled": True, "target_status_name": "Storno", "target_status_id": 99,
        }}
        self.result = CreditnoteStornoSummary(
            project="roy", enabled=True, dry_run=False, target_status_name="Storno",
            fetched_creditnotes=3, exported_creditnotes=3, creditnoted_orders=2,
            checked_orders=2, eligible_orders=1, updated_orders=1,
        )
        self.store = Mock()
        self.store.read.return_value = ({"project": "roy"}, "synthetic-etag")
        self.events = []
        self.environment = patch.dict(os.environ, {"BIZNISWEB_API_TOKEN": "synthetic-token"}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.patches = [
            patch.object(runner, "load_project_env", side_effect=lambda project: self.events.append(("env", project))),
            patch.object(runner, "load_project_settings", side_effect=lambda project: self.events.append(("settings", project)) or self.settings),
            patch.object(runner, "build_automation_state_store", return_value=self.store),
            patch.object(runner, "run_creditnote_storno_guard", return_value=self.result),
            patch.object(runner, "put_metric"),
        ]
        self.load_env, self.load_settings, self.build_store, self.guard, self.metric = [p.start() for p in self.patches]
        for item in self.patches:
            self.addCleanup(item.stop)

    def invoke(self, *, project="roy", dry_run=False):
        arguments = runner.parse_args(["--project", project] + (["--dry-run"] if dry_run else []))
        output = io.StringIO()
        error = None
        with redirect_stdout(output):
            try:
                result = runner.run_creditnote_runner(arguments)
            except RuntimeError as exc:
                result, error = None, exc
        summaries = [json.loads(line.removeprefix(runner.SUMMARY_PREFIX)) for line in output.getvalue().splitlines()
                     if line.startswith(runner.SUMMARY_PREFIX)]
        self.assertEqual(1, len(summaries))
        return result, error, summaries[0], output.getvalue()

    def assert_failed(self, error, summary):
        self.assertIsNotNone(error)
        self.assertFalse(summary["ok"])
        names = [call.args[0] for call in self.metric.call_args_list]
        self.assertNotIn("CreditnoteStandaloneRunSucceeded", names)
        self.assertEqual(1, names.count("CreditnoteStandaloneRunFailed"))

    def test_live_uses_selected_runtime_and_existing_guard_with_durable_state(self):
        result, error, printed, _ = self.invoke()
        self.assertIsNone(error)
        self.assertEqual(result, printed)
        self.assertEqual([("env", "roy"), ("settings", "roy")], self.events)
        self.assertEqual("roy", os.environ["REPORT_PROJECT"])
        self.store.read.assert_called_once()
        self.guard.assert_called_once_with(project_name="roy", project_settings=self.settings,
                                           dry_run=False, automation_state_store=self.store)
        self.assertTrue(result["creditnote_scan_complete"])
        self.assertFalse(result["skipped_locked"])
        self.assertEqual(1, result["updated_orders"])
        self.assertEqual("CreditnoteStandaloneRunSucceeded", self.metric.call_args_list[-1].args[0])
        self.assertTrue(all(call.args[3]["metric_run_mode"] == "live" for call in self.metric.call_args_list))

    def test_vevo_dry_run_has_separate_metrics_and_never_opens_state(self):
        self.settings["biznisweb_api_url"] = "https://vevo.flox.sk/api/graphql"
        self.result.project, self.result.dry_run, self.result.updated_orders = "vevo", True, 0
        result, error, _, _ = self.invoke(project="vevo", dry_run=True)
        self.assertIsNone(error)
        self.assertEqual("vevo", result["project"])
        self.build_store.assert_not_called()
        self.store.read.assert_not_called()
        self.assertIsNone(self.guard.call_args.kwargs["automation_state_store"])
        self.assertTrue(self.guard.call_args.kwargs["dry_run"])
        self.assertTrue(all(call.args[3]["metric_run_mode"] == "dry-run" for call in self.metric.call_args_list))

    def test_zero_candidates_still_require_readable_durable_state_live(self):
        for name in runner.COUNTERS:
            if name != "audit_error_orders":
                setattr(self.result, name, 0)
        self.store.read.side_effect = AutomationStateError("private storage detail")
        _, error, summary, text = self.invoke()
        self.assert_failed(error, summary)
        self.assertEqual("durable_state", summary["failure_stage"])
        self.guard.assert_not_called()
        self.assertNotIn("private storage detail", text)
        self.assertNotIn("private storage detail", str(error))

    def test_disabled_or_non_boolean_enabled_never_returns_success(self):
        for value in (False, None, "true", 1):
            with self.subTest(value=value):
                self.metric.reset_mock()
                self.settings["creditnote_storno_guard"]["enabled"] = value
                _, error, summary, _ = self.invoke()
                self.assert_failed(error, summary)
                self.build_store.assert_not_called()
                self.guard.assert_not_called()

    def test_project_endpoint_and_token_mismatch_stop_before_guard(self):
        for environment in ({"BIZNISWEB_API_URL": "https://vevo.sk/api/graphql"},
                            {"BIZNISWEB_API_URL": "http://roy.sk/api/graphql"},
                            {"BIZNISWEB_API_URL": "https://roy.sk/api/graphql?secret=private"},
                            {"BIZNISWEB_API_TOKEN": ""}, {"ROY_BIZNISWEB_API_TOKEN": "different-token"}):
            with self.subTest(environment=tuple(environment)):
                self.metric.reset_mock()
                with patch.dict(os.environ, environment):
                    _, error, summary, text = self.invoke()
                self.assert_failed(error, summary)
                self.assertNotIn("different-token", text)
                self.build_store.assert_not_called()
                self.guard.assert_not_called()

    def test_failed_review_or_audit_outcome_never_emits_success_in_either_mode(self):
        for field in ("failed_orders", "review_required_orders", "audit_errors"):
            for dry in (False, True):
                with self.subTest(field=field, dry=dry):
                    self.metric.reset_mock()
                    result = deepcopy(self.result)
                    result.dry_run, result.updated_orders = dry, 0
                    setattr(result, field, {"private-order": "private-reason"} if field == "audit_errors" else 1)
                    self.guard.return_value = result
                    _, error, summary, text = self.invoke(dry_run=dry)
                    self.assert_failed(error, summary)
                    self.assertTrue(summary["creditnote_scan_complete"])
                    self.assertNotIn("private-order", text)
                    self.assertNotIn("private-reason", text)

    def test_guard_context_audit_and_busy_lease_exceptions_are_failed_not_retried(self):
        for failure in (RuntimeError("private incomplete context"), AutomationLeaseBusy("private lock owner"),
                        AutomationStateError("private audit failure")):
            with self.subTest(kind=type(failure).__name__):
                self.metric.reset_mock()
                self.guard.reset_mock()
                self.guard.side_effect = failure
                _, error, summary, text = self.invoke()
                self.assert_failed(error, summary)
                self.assertFalse(summary["creditnote_scan_complete"])
                self.guard.assert_called_once()
                self.assertNotIn(str(failure), text)

    def test_invalid_or_incomplete_summary_cannot_satisfy_host_gate(self):
        for fields in ({"project": "vevo"}, {"dry_run": True}, {"enabled": False},
                       {"checked_orders": 1}, {"fetched_creditnotes": 1}, {"updated_orders": 2},
                       {"checked_orders": True}, {"review_required_orders": -1},
                       {"creditnote_scan_complete": False}, {"skipped_locked": True}):
            with self.subTest(fields=fields):
                self.metric.reset_mock()
                result = deepcopy(self.result)
                for name, value in fields.items():
                    setattr(result, name, value)
                self.guard.return_value = result
                _, error, summary, _ = self.invoke()
                self.assert_failed(error, summary)
                self.assertFalse(summary["creditnote_scan_complete"])

    def test_summary_omits_private_fields_and_restores_logging(self):
        self.result.eligible_order_nums = ["private-order"]
        self.result.updated_order_statuses = [{"email": "private-email"}]
        self.result.status_audit_path = "private-path"
        previous = logging.root.manager.disable
        def guarded(**kwargs):
            logging.error("private-provider-message")
            return self.result
        self.guard.side_effect = guarded
        _, error, _, text = self.invoke()
        self.assertIsNone(error)
        self.assertNotIn("private-", text)
        self.assertEqual(previous, logging.root.manager.disable)

    def test_real_guard_dry_run_performs_no_business_journal_or_audit_writes(self):
        exporter = FakeExporter([order_row("synthetic-order", "Odoslana")])
        def existing_guard(**kwargs):
            return run_creditnote_storno_guard(**kwargs, exporter=exporter,
                raw_creditnote_rows=[creditnote_row("synthetic-credit", "synthetic-order")])
        self.guard.side_effect = existing_guard
        with patch("creditnote_storno_guard.load_creditnote_status_change_audit") as load_audit, \
             patch("creditnote_storno_guard.save_creditnote_status_change_audit") as save_audit:
            result, error, _, _ = self.invoke(dry_run=True)
        self.assertIsNone(error)
        self.assertEqual(1, result["eligible_orders"])
        self.assertEqual(0, result["updated_orders"])
        self.assertEqual([], exporter.client.mutations)
        self.build_store.assert_not_called()
        load_audit.assert_not_called()
        save_audit.assert_not_called()

    def test_real_guard_incomplete_creditnote_scan_fails_before_business_inspection(self):
        self.guard.side_effect = run_creditnote_storno_guard
        with patch("creditnote_storno_guard.fetch_project_creditnotes", return_value=([
                creditnote_row("synthetic-credit", "synthetic-order")], 2)), \
             patch("creditnote_storno_guard._build_exporter") as exporter:
            _, error, summary, _ = self.invoke(dry_run=True)
        self.assert_failed(error, summary)
        self.assertFalse(summary["creditnote_scan_complete"])
        exporter.assert_not_called()
        self.build_store.assert_not_called()


class CreditnoteStandaloneEntrypointTests(unittest.TestCase):
    def test_cli_requires_project_and_uses_explicit_dry_run_only(self):
        with redirect_stderr(io.StringIO()):
            for argv in ([], ["--project", "other"], ["--project", "roy", "--date-from", "2026-01-01"]):
                with self.assertRaises(SystemExit):
                    runner.parse_args(argv)
        with patch.dict(os.environ, {"REPORT_CREDITNOTE_STORNO_DRY_RUN": "true"}):
            self.assertFalse(runner.parse_args(["--project", "roy"]).dry_run)
        self.assertTrue(runner.parse_args(["--project", "vevo", "--dry-run"]).dry_run)

    def test_main_returns_nonzero_without_exposing_provider_exception(self):
        text = io.StringIO()
        with patch.object(runner, "run_creditnote_runner", side_effect=RuntimeError("private response")), redirect_stdout(text):
            self.assertEqual(1, runner.main(["--project", "roy"]))
        self.assertNotIn("private response", text.getvalue())
        with patch.object(runner, "run_creditnote_runner", return_value={"ok": True}):
            self.assertEqual(0, runner.main(["--project", "vevo"]))

    def test_import_has_no_business_or_state_side_effects(self):
        try:
            with patch("creditnote_storno_guard.run_creditnote_storno_guard") as guard, \
                 patch("invoice_automation_state.build_automation_state_store") as state:
                importlib.reload(runner)
                guard.assert_not_called()
                state.assert_not_called()
        finally:
            importlib.reload(runner)


if __name__ == "__main__":
    unittest.main()
