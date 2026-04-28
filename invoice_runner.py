#!/usr/bin/env python3
"""
Standalone daily invoice automation runner.

This runner is intentionally separate from daily_report_runner.py so reports can
run after the prior day is complete while invoices can run on the current day.
"""

import argparse
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from generate_invoices import resolve_invoice_date_window, resolve_invoice_generation_settings, run_invoice_generation
from reporting_core import BASE_DEFAULT_PROJECT, load_project_env, load_project_settings, put_metric, resolve_reporting_defaults


DEFAULT_PROJECT = os.getenv("REPORT_PROJECT", BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate invoices as a standalone scheduled job")
    parser.add_argument(
        "--project",
        default=os.getenv("REPORT_PROJECT", DEFAULT_PROJECT),
        help="Project name (uses projects/<project>/.env and settings.json)",
    )
    parser.add_argument(
        "--reference-date",
        default=os.getenv("INVOICE_REFERENCE_DATE", ""),
        help="Reference date in YYYY-MM-DD format. Default: today in --timezone.",
    )
    parser.add_argument(
        "--from-date",
        default=os.getenv("INVOICE_FROM_DATE", ""),
        help="Explicit invoice window start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--to-date",
        default=os.getenv("INVOICE_TO_DATE", ""),
        help="Explicit invoice window end date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--timezone",
        default=os.getenv("REPORT_TIMEZONE", "Europe/Bratislava"),
        help="Timezone used for default current-day invoice reference date.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=_env_bool("REPORT_INVOICE_DRY_RUN", False),
        help="Preview invoice candidates without creating invoices.",
    )
    parser.add_argument(
        "--no-web-login",
        action="store_true",
        help="Skip BizniWeb web login. Useful only for dry-run/API diagnostics.",
    )
    return parser.parse_args(argv)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def _normalize_date(value: str, label: str) -> str:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{label} must use YYYY-MM-DD format, got '{value}'") from exc


def resolve_invoice_runner_window(
    settings: Dict[str, Any],
    timezone_name: str,
    reference_date: str = "",
    from_date: str = "",
    to_date: str = "",
) -> Tuple[str, str]:
    if bool(from_date) != bool(to_date):
        raise ValueError("Use both --from-date and --to-date, or neither.")

    if from_date and to_date:
        start = _normalize_date(from_date, "from_date")
        end = _normalize_date(to_date, "to_date")
        if start > end:
            raise ValueError(f"from_date ({start}) cannot be after to_date ({end})")
        return start, end

    invoice_settings = resolve_invoice_generation_settings(settings)
    if reference_date:
        ref = _normalize_date(reference_date, "reference_date")
    else:
        ref = datetime.now(ZoneInfo(timezone_name)).date().strftime("%Y-%m-%d")

    return resolve_invoice_date_window(ref, invoice_settings["lookback_days"])


def run_invoice_runner(args: argparse.Namespace) -> Dict[str, Any]:
    project = (args.project or DEFAULT_PROJECT).strip() or DEFAULT_PROJECT
    os.environ["REPORT_PROJECT"] = project
    load_project_env(project)

    settings = load_project_settings(project)
    reporting_defaults = resolve_reporting_defaults(project, settings)
    invoice_settings = resolve_invoice_generation_settings(settings)

    if not invoice_settings["enabled"]:
        print(f"Invoice automation disabled for project={project}")
        put_metric("InvoiceStandaloneDisabled", 1, project, reporting_defaults)
        return {
            "project": project,
            "enabled": False,
        }

    invoice_from_date, invoice_to_date = resolve_invoice_runner_window(
        settings=settings,
        timezone_name=args.timezone,
        reference_date=args.reference_date,
        from_date=args.from_date,
        to_date=args.to_date,
    )
    print(
        f"Running standalone invoice automation for project={project} "
        f"window={invoice_from_date}..{invoice_to_date} dry_run={args.dry_run}"
    )

    try:
        summary = run_invoice_generation(
            project_name=project,
            date_from=invoice_from_date,
            date_to=invoice_to_date,
            dry_run=args.dry_run,
            no_web_login=args.no_web_login,
        )
    except Exception:
        put_metric("InvoiceStandaloneRunFailed", 1, project, reporting_defaults)
        raise

    put_metric("InvoiceStandaloneMatchedOrders", summary.matched_orders, project, reporting_defaults)
    put_metric("InvoiceStandaloneSkippedZeroTotal", summary.skipped_zero_total_orders, project, reporting_defaults)
    put_metric("InvoiceStandaloneCreated", summary.created_invoices, project, reporting_defaults)
    put_metric("InvoiceStandaloneCreateFailures", summary.failed_invoices, project, reporting_defaults)
    put_metric("InvoiceStandaloneRunSucceeded", 1, project, reporting_defaults)

    print(
        "Standalone invoice summary: "
        f"matched={summary.matched_orders} "
        f"created={summary.created_invoices} "
        f"failed={summary.failed_invoices} "
        f"skipped_zero_total={summary.skipped_zero_total_orders}"
    )

    if not args.dry_run and summary.failed_invoices:
        raise RuntimeError(f"Invoice automation failed for {summary.failed_invoices} order(s) in project '{project}'")

    return {
        "project": summary.project,
        "enabled": True,
        "from_date": summary.date_from,
        "to_date": summary.date_to,
        "matched_orders": summary.matched_orders,
        "created_invoices": summary.created_invoices,
        "failed_invoices": summary.failed_invoices,
        "skipped_zero_total_orders": summary.skipped_zero_total_orders,
        "dry_run": summary.dry_run,
    }


def main() -> None:
    load_dotenv(encoding="utf-8-sig")
    run_invoice_runner(parse_args())


if __name__ == "__main__":
    main()
