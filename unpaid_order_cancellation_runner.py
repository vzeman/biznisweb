#!/usr/bin/env python3
"""Standalone scheduled runner for stale unpaid order cancellation."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from reporting_core import BASE_DEFAULT_PROJECT, load_project_settings, put_metric, resolve_reporting_defaults
from unpaid_order_cancellation import resolve_unpaid_cancellation_settings, run_unpaid_order_cancellation


DEFAULT_PROJECT = os.getenv("REPORT_PROJECT", BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cancel stale unpaid BizniWeb orders")
    parser.add_argument(
        "--project",
        default=os.getenv("REPORT_PROJECT", DEFAULT_PROJECT),
        help="Project name (uses projects/<project>/.env and settings.json)",
    )
    parser.add_argument(
        "--reference-date",
        default=os.getenv("UNPAID_CANCELLATION_REFERENCE_DATE", ""),
        help="Reference date in YYYY-MM-DD format. Default: today in --timezone.",
    )
    parser.add_argument(
        "--timezone",
        default=os.getenv("REPORT_TIMEZONE", "Europe/Bratislava"),
        help="Timezone used for default current-day reference date.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=_env_bool("REPORT_UNPAID_CANCELLATION_DRY_RUN", False),
        help="Preview eligible orders without changing their status.",
    )
    return parser.parse_args(argv)


def _reference_date(args: argparse.Namespace) -> str:
    if args.reference_date:
        try:
            return datetime.strptime(str(args.reference_date), "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"reference_date must use YYYY-MM-DD format, got '{args.reference_date}'") from exc
    return datetime.now(ZoneInfo(args.timezone)).date().strftime("%Y-%m-%d")


def run_unpaid_cancellation_runner(args: argparse.Namespace) -> Dict[str, Any]:
    project = (args.project or DEFAULT_PROJECT).strip() or DEFAULT_PROJECT
    os.environ["REPORT_PROJECT"] = project

    project_settings = load_project_settings(project)
    reporting_defaults = resolve_reporting_defaults(project, project_settings)
    cancellation_settings = resolve_unpaid_cancellation_settings(project_settings)

    if not cancellation_settings.enabled:
        print(f"Unpaid order cancellation disabled for project={project}")
        put_metric("UnpaidCancellationDisabled", 1, project, reporting_defaults)
        return {
            "project": project,
            "enabled": False,
        }

    reference_date = _reference_date(args)
    print(
        f"Running unpaid order cancellation for project={project} "
        f"reference_date={reference_date} dry_run={args.dry_run}"
    )

    try:
        summary = run_unpaid_order_cancellation(
            project_name=project,
            reference_date=reference_date,
            dry_run=args.dry_run,
            project_settings=project_settings,
        )
    except Exception:
        put_metric("UnpaidCancellationRunFailed", 1, project, reporting_defaults)
        raise

    put_metric("UnpaidCancellationOrdersScanned", summary.total_orders_scanned, project, reporting_defaults)
    put_metric("UnpaidCancellationEligibleOrders", summary.eligible_orders, project, reporting_defaults)
    put_metric("UnpaidCancellationUpdatedOrders", summary.updated_orders, project, reporting_defaults)
    put_metric("UnpaidCancellationFailedOrders", summary.failed_orders, project, reporting_defaults)
    put_metric("UnpaidCancellationRunSucceeded", 1, project, reporting_defaults)

    print("UNPAID_CANCELLATION_SUMMARY " + json.dumps(summary.as_dict(), ensure_ascii=False, sort_keys=True))

    if not args.dry_run and summary.failed_orders:
        raise RuntimeError(f"Unpaid order cancellation failed for {summary.failed_orders} order(s) in project '{project}'")

    return summary.as_dict()


def main() -> None:
    load_dotenv(encoding="utf-8-sig")
    run_unpaid_cancellation_runner(parse_args())


if __name__ == "__main__":
    main()
