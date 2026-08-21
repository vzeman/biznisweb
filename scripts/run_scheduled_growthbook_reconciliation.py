#!/usr/bin/env python3
"""Run the reviewed VEVO GrowthBook Preview rolling reconciliation window.

The schedule has no user-controlled date arguments. It always processes a
bounded set of complete UTC receipt partitions and retains the reconciler's
independent two-part publish gate.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from reporting_core.experiments import ExperimentDataError
from scripts import reconcile_growthbook_facts


PROJECT = "vevo"
ENVIRONMENT = "preview"
SETTINGS_PATH = REPO_ROOT / "projects" / PROJECT / "settings.json"


def _load_schedule_settings(path: Path = SETTINGS_PATH) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExperimentDataError("scheduled reconciliation settings are unreadable") from exc
    config = payload.get("growthbook_reconciliation") if isinstance(payload, dict) else None
    if not isinstance(config, dict):
        raise ExperimentDataError("scheduled reconciliation settings are missing")
    expected_identity = {
        "environment": ENVIRONMENT,
        "stack_name": "vevo-growthbook-reconciliation-preview",
        "schedule_name": "vevo-growthbook-reconcile-preview",
        "schedule_expression": "cron(30 3 * * ? *)",
        "timezone": "Europe/Bratislava",
        "source_task_family": "vevo-reporting-daily",
    }
    for key, expected in expected_identity.items():
        if config.get(key) != expected:
            raise ExperimentDataError(f"scheduled reconciliation identity drift: {key}")
    partition_days = config.get("rolling_partition_days")
    max_raw_events = config.get("max_raw_events")
    if type(partition_days) is not int or not 22 <= partition_days <= 60:
        raise ExperimentDataError("rolling partition days must be between 22 and 60")
    if type(max_raw_events) is not int or not 1_000 <= max_raw_events <= 100_000:
        raise ExperimentDataError("scheduled raw event limit is invalid")
    return config


def resolve_complete_utc_window(*, now: datetime, partition_days: int) -> tuple[date, date]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ExperimentDataError("scheduled reconciliation clock must be timezone-aware")
    through = now.astimezone(timezone.utc).date() - timedelta(days=1)
    start = through - timedelta(days=partition_days - 1)
    return start, through


def build_reconcile_args(
    *,
    now: datetime,
    settings: Mapping[str, Any],
    bucket: str,
    region: str,
) -> list[str]:
    partition_days = settings["rolling_partition_days"]
    event_from, event_through = resolve_complete_utc_window(
        now=now,
        partition_days=partition_days,
    )
    return [
        "--project",
        PROJECT,
        "--event-from",
        event_from.isoformat(),
        "--event-through",
        event_through.isoformat(),
        "--bucket",
        bucket,
        "--region",
        region,
        "--max-partitions",
        str(partition_days),
        "--max-raw-events",
        str(settings["max_raw_events"]),
        "--publish",
    ]


def run(argv: Sequence[str] | None = None, *, now: datetime | None = None) -> int:
    if argv:
        raise ExperimentDataError("scheduled reconciliation accepts no arguments")
    if os.getenv("REPORT_PROJECT") != PROJECT:
        raise ExperimentDataError("scheduled reconciliation requires REPORT_PROJECT=vevo")
    if os.getenv("GROWTHBOOK_ENVIRONMENT") != ENVIRONMENT:
        raise ExperimentDataError("scheduled reconciliation requires Preview")
    if os.getenv("GROWTHBOOK_FACT_PUBLISH_ENABLED", "").strip().lower() != "true":
        raise ExperimentDataError("scheduled reconciliation publish gate is disabled")
    bucket = os.getenv("GROWTHBOOK_EVENT_BUCKET", "").strip()
    if not bucket:
        raise ExperimentDataError("scheduled reconciliation event bucket is missing")
    region = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "").strip()
    if region != "eu-central-1":
        raise ExperimentDataError("scheduled reconciliation requires eu-central-1")

    settings = _load_schedule_settings()
    clock = now or datetime.now(timezone.utc)
    reconcile_args = build_reconcile_args(
        now=clock,
        settings=settings,
        bucket=bucket,
        region=region,
    )
    result = reconcile_growthbook_facts.run(reconcile_args)
    event_from = reconcile_args[reconcile_args.index("--event-from") + 1]
    event_through = reconcile_args[reconcile_args.index("--event-through") + 1]
    print(
        "GROWTHBOOK_SCHEDULED_RECONCILIATION_OK:"
        f"project={PROJECT}:environment={ENVIRONMENT}:"
        f"event-from={event_from}:event-through={event_through}:"
        f"partitions={settings['rolling_partition_days']}"
    )
    return result


def main() -> None:
    try:
        raise SystemExit(run(sys.argv[1:]))
    except Exception as exc:
        print(
            "GROWTHBOOK_SCHEDULED_RECONCILIATION_FAILURE:"
            f"{type(exc).__name__}",
            file=sys.stderr,
        )
        if isinstance(exc, ExperimentDataError):
            print(f"GrowthBook scheduled reconciliation stopped: {exc}", file=sys.stderr)
            raise SystemExit(2) from exc
        raise


if __name__ == "__main__":
    main()
