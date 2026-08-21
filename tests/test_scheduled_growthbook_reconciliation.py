from __future__ import annotations

import copy
import io
import os
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from unittest import mock

from reporting_core.experiments import ExperimentDataError
from scripts import run_scheduled_growthbook_reconciliation as scheduled


BASE_SETTINGS = {
    "environment": "preview",
    "stack_name": "vevo-growthbook-reconciliation-preview",
    "schedule_name": "vevo-growthbook-reconcile-preview",
    "schedule_expression": "cron(30 3 * * ? *)",
    "timezone": "Europe/Bratislava",
    "rolling_partition_days": 40,
    "max_raw_events": 50_000,
    "source_task_family": "vevo-reporting-daily",
}


class ScheduledGrowthBookReconciliationTests(unittest.TestCase):
    def test_window_uses_only_forty_complete_utc_partitions(self) -> None:
        start, through = scheduled.resolve_complete_utc_window(
            now=datetime(2026, 8, 21, 1, 15, tzinfo=timezone.utc),
            partition_days=40,
        )
        self.assertEqual("2026-07-12", start.isoformat())
        self.assertEqual("2026-08-20", through.isoformat())
        self.assertEqual(40, (through - start).days + 1)

    def test_window_rejects_naive_clock(self) -> None:
        with self.assertRaisesRegex(ExperimentDataError, "timezone-aware"):
            scheduled.resolve_complete_utc_window(
                now=datetime(2026, 8, 21, 1, 15),
                partition_days=40,
            )

    def test_checked_in_schedule_identity_and_bounds_are_exact(self) -> None:
        config = scheduled._load_schedule_settings()
        self.assertEqual(BASE_SETTINGS, dict(config))

        drifted = copy.deepcopy(BASE_SETTINGS)
        drifted["rolling_partition_days"] = 21
        with mock.patch("json.loads", return_value={"growthbook_reconciliation": drifted}):
            with self.assertRaisesRegex(ExperimentDataError, "between 22 and 60"):
                scheduled._load_schedule_settings()

    def test_run_requires_all_runtime_publish_gates(self) -> None:
        safe_env = {
            "REPORT_PROJECT": "vevo",
            "GROWTHBOOK_ENVIRONMENT": "preview",
            "GROWTHBOOK_FACT_PUBLISH_ENABLED": "true",
            "GROWTHBOOK_EVENT_BUCKET": "vevo-growthbook-test",
            "AWS_REGION": "eu-central-1",
        }
        for missing in safe_env:
            altered = dict(safe_env)
            altered.pop(missing)
            with self.subTest(missing=missing), mock.patch.dict(os.environ, altered, clear=True):
                with self.assertRaises(ExperimentDataError):
                    scheduled.run(now=datetime(2026, 8, 21, tzinfo=timezone.utc))

    def test_run_passes_only_bounded_reviewed_arguments_and_prints_marker(self) -> None:
        safe_env = {
            "REPORT_PROJECT": "vevo",
            "GROWTHBOOK_ENVIRONMENT": "preview",
            "GROWTHBOOK_FACT_PUBLISH_ENABLED": "true",
            "GROWTHBOOK_EVENT_BUCKET": "vevo-growthbook-test",
            "AWS_REGION": "eu-central-1",
        }
        output = io.StringIO()
        with (
            mock.patch.dict(os.environ, safe_env, clear=True),
            mock.patch.object(scheduled, "_load_schedule_settings", return_value=BASE_SETTINGS),
            mock.patch.object(scheduled.reconcile_growthbook_facts, "run", return_value=0) as run,
            redirect_stdout(output),
        ):
            self.assertEqual(
                0,
                scheduled.run(now=datetime(2026, 8, 21, 1, 15, tzinfo=timezone.utc)),
            )

        args = run.call_args.args[0]
        self.assertEqual("2026-07-12", args[args.index("--event-from") + 1])
        self.assertEqual("2026-08-20", args[args.index("--event-through") + 1])
        self.assertEqual("40", args[args.index("--max-partitions") + 1])
        self.assertEqual("50000", args[args.index("--max-raw-events") + 1])
        self.assertIn("--publish", args)
        self.assertIn("GROWTHBOOK_SCHEDULED_RECONCILIATION_OK", output.getvalue())

    def test_run_rejects_all_user_arguments(self) -> None:
        with self.assertRaisesRegex(ExperimentDataError, "accepts no arguments"):
            scheduled.run(["--event-from", "2026-01-01"])


if __name__ == "__main__":
    unittest.main()
