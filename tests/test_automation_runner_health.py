import unittest
from types import SimpleNamespace
from unittest.mock import patch

import invoice_runner
from reporting_core.metrics import automation_metric_defaults, put_metric


def summary(**overrides):
    fields = dict(project="roy", date_from="2026-01-01", date_to="2026-04-01",
                  matched_orders=0, skipped_zero_total_orders=0, created_invoices=0,
                  failed_invoices=0, emailed_invoices=0, failed_invoice_emails=0,
                  missing_invoice_ids=0, invoice_status_reconciliation_candidates=0,
                  reconciled_invoice_statuses=0, failed_invoice_status_reconciliations=0,
                  invoice_status_reconciliation_enabled=False,
                  skipped_invoice_status_reconciliations_after_recheck=0,
                  invoice_status_reconciliation_target_name="", invoice_status_reconciliation_target_id=None,
                  dry_run=False, invoice_scan_complete=True)
    return SimpleNamespace(**{**fields, **overrides})


class AutomationRunnerHealthTests(unittest.TestCase):
    def run_summary(self, result, dry_run=False):
        args = invoice_runner.parse_args(["--project", "roy"] + (["--dry-run"] if dry_run else []))
        with patch.object(invoice_runner, "load_project_env"), \
             patch.object(invoice_runner, "load_project_settings", return_value={"invoice_generation": {"enabled": True}}), \
             patch.object(invoice_runner, "resolve_reporting_defaults", return_value={"cloudwatch_namespace": "Test"}), \
             patch.object(invoice_runner, "run_invoice_generation", return_value=result), \
             patch.object(invoice_runner, "put_metric") as metric:
            try:
                returned = invoice_runner.run_invoice_runner(args)
                error = None
            except RuntimeError as exc:
                returned, error = None, exc
            return returned, error, metric.call_args_list

    def test_incomplete_or_failed_run_never_emits_success_even_in_dry_run(self):
        for failure in (dict(failed_invoices=1), dict(failed_invoice_emails=1),
                        dict(missing_invoice_ids=1), dict(invoice_scan_complete=False),
                        dict(ambiguous_invoice_operations=1)):
            for dry in (False, True):
                with self.subTest(failure=failure, dry=dry):
                    _, error, calls = self.run_summary(summary(**failure), dry)
                    names = [c.args[0] for c in calls]
                    self.assertIsNotNone(error)
                    self.assertNotIn("InvoiceStandaloneRunSucceeded", names)
                    self.assertEqual(names.count("InvoiceStandaloneRunFailed"), 1)
                    self.assertTrue(all(c.args[3]["metric_run_mode"] == ("dry-run" if dry else "live") for c in calls))

    def test_busy_lease_is_neither_success_nor_failed_run(self):
        result, error, calls = self.run_summary(summary(skipped_locked=True, invoice_scan_complete=False))
        self.assertIsNone(error)
        self.assertTrue(result["skipped_locked"])
        self.assertEqual([c.args[0] for c in calls], ["InvoiceStandaloneLeaseBusy"])

    def test_success_is_last_and_reports_backlog(self):
        _, error, calls = self.run_summary(summary(pending_invoice_operations=2, invoice_status_review_required=3))
        self.assertIsNone(error)
        self.assertEqual(calls[-1].args[0], "InvoiceStandaloneRunSucceeded")
        values = {c.args[0]: c.args[1] for c in calls}
        self.assertEqual(values["InvoiceStandaloneBacklogPending"], 2)
        self.assertEqual(values["InvoiceStandaloneReviewRequired"], 3)

    @patch("boto3.client")
    def test_diagnostic_metrics_cannot_satisfy_live_alarm(self, client):
        defaults = {"cloudwatch_namespace": "Test"}
        for dry in (False, True):
            put_metric("InvoiceStandaloneRunSucceeded", 1, "roy", automation_metric_defaults(defaults, dry))
        dimensions = [call.kwargs["MetricData"][0]["Dimensions"] for call in client.return_value.put_metric_data.call_args_list]
        self.assertNotEqual(dimensions[0], dimensions[1])
        self.assertEqual(defaults, {"cloudwatch_namespace": "Test"})


if __name__ == "__main__":
    unittest.main()
