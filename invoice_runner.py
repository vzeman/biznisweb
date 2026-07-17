#!/usr/bin/env python3
"""
Standalone daily invoice automation runner.

This runner is intentionally separate from daily_report_runner.py so reports can
run after the prior day is complete while invoices can run on the current day.
"""

import argparse
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from generate_invoices import (
    IncompleteInvoiceScanError,
    resolve_invoice_date_window,
    resolve_invoice_generation_settings,
    run_invoice_generation,
)
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
    parser.add_argument(
        "--reconcile",
        action="store_true",
        default=_env_bool("REPORT_INVOICE_RECONCILE", False),
        help="Use the extended reconciliation window instead of the frequent rolling window.",
    )
    parser.add_argument(
        "--max-creations",
        type=int,
        default=None,
        help="Fail before mutation when more than this many invoice candidates match.",
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
    reconcile: bool = False,
    current_datetime: Optional[datetime] = None,
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
        ref = resolve_default_invoice_reference_date(
            timezone_name,
            invoice_settings["rollover_grace_hours"],
            current_datetime=current_datetime,
        )

    lookback_days = (
        invoice_settings["reconciliation_lookback_days"]
        if reconcile
        else invoice_settings["status_change_lookback_days"]
    )
    return resolve_invoice_date_window(ref, lookback_days)


def resolve_default_invoice_reference_date(
    timezone_name: str,
    rollover_grace_hours: int,
    *,
    current_datetime: Optional[datetime] = None,
) -> str:
    """Keep a just-after-midnight final sweep anchored to its intended prior day."""
    timezone = ZoneInfo(timezone_name)
    current = current_datetime or datetime.now(timezone)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone)
    else:
        current = current.astimezone(timezone)
    if current.hour < max(0, int(rollover_grace_hours)):
        current -= timedelta(days=1)
    return current.date().strftime("%Y-%m-%d")


def run_invoice_runner(args: argparse.Namespace) -> Dict[str, Any]:
    project = (args.project or DEFAULT_PROJECT).strip() or DEFAULT_PROJECT
    reconcile = bool(getattr(args, "reconcile", False))
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
        reconcile=reconcile,
    )
    print(
        f"Running standalone invoice automation for project={project} "
        f"window={invoice_from_date}..{invoice_to_date} "
        f"scan_mode={'reconciliation' if reconcile else 'regular'} dry_run={args.dry_run}"
    )

    try:
        summary = run_invoice_generation(
            project_name=project,
            date_from=invoice_from_date,
            date_to=invoice_to_date,
            dry_run=args.dry_run,
            no_web_login=args.no_web_login,
            reconcile=reconcile,
            max_creations=getattr(args, "max_creations", None),
        )
    except IncompleteInvoiceScanError:
        put_metric("InvoiceStandaloneScanIncomplete", 1, project, reporting_defaults)
        put_metric("InvoiceStandaloneRunFailed", 1, project, reporting_defaults)
        if reconcile and not args.dry_run:
            put_metric("InvoiceReconciliationRunFailed", 1, project, reporting_defaults)
        raise
    except Exception:
        put_metric("InvoiceStandaloneRunFailed", 1, project, reporting_defaults)
        if reconcile and not args.dry_run:
            put_metric("InvoiceReconciliationRunFailed", 1, project, reporting_defaults)
        raise

    put_metric("InvoiceStandaloneMatchedOrders", summary.matched_orders, project, reporting_defaults)
    put_metric("InvoiceStandaloneSkippedZeroTotal", summary.skipped_zero_total_orders, project, reporting_defaults)
    put_metric(
        "InvoiceStandaloneSkippedBeforeAutomationStart",
        summary.skipped_before_automation_start,
        project,
        reporting_defaults,
    )
    put_metric("InvoiceStandaloneCreated", summary.created_invoices, project, reporting_defaults)
    put_metric("InvoiceStandaloneCreateFailures", summary.failed_invoices, project, reporting_defaults)
    put_metric("InvoiceStandaloneEmailed", summary.emailed_invoices, project, reporting_defaults)
    put_metric("InvoiceStandaloneEmailFailures", summary.failed_invoice_emails, project, reporting_defaults)
    put_metric("InvoiceStandaloneMissingInvoiceIds", summary.missing_invoice_ids, project, reporting_defaults)
    put_metric("InvoiceStandaloneScanComplete", int(summary.scan_complete), project, reporting_defaults)
    put_metric("InvoiceStandaloneOrdersFetched", summary.total_orders_fetched, project, reporting_defaults)
    put_metric("InvoiceStandalonePagesFetched", summary.pages_fetched, project, reporting_defaults)
    put_metric("InvoiceStandalonePageRetries", summary.page_retry_count, project, reporting_defaults)
    put_metric(
        "InvoiceStandaloneAlreadyPresent",
        summary.already_present_invoices,
        project,
        reporting_defaults,
    )
    put_metric(
        "InvoiceStandaloneSkippedStale",
        summary.skipped_stale_orders,
        project,
        reporting_defaults,
    )
    put_metric(
        "InvoiceStandaloneSkippedNonCod",
        summary.skipped_non_cod_orders,
        project,
        reporting_defaults,
    )

    print(
        "Standalone invoice summary: "
        f"matched={summary.matched_orders} "
        f"created={summary.created_invoices} "
        f"failed={summary.failed_invoices} "
        f"emailed={summary.emailed_invoices} "
        f"email_failed={summary.failed_invoice_emails} "
        f"missing_invoice_ids={summary.missing_invoice_ids} "
        f"skipped_zero_total={summary.skipped_zero_total_orders} "
        f"skipped_before_automation_start={summary.skipped_before_automation_start} "
        f"already_present={summary.already_present_invoices} "
        f"skipped_stale={summary.skipped_stale_orders} "
        f"skipped_non_cod={summary.skipped_non_cod_orders} "
        f"scan_mode={summary.scan_mode} "
        f"scan_complete={summary.scan_complete} "
        f"orders_fetched={summary.total_orders_fetched} "
        f"purchase_fetched={summary.purchase_date_orders_fetched} "
        f"recent_change_fetched={summary.recent_change_orders_fetched} "
        f"pages_fetched={summary.pages_fetched} "
        f"page_retries={summary.page_retry_count}"
    )

    run_failed = (
        not summary.scan_complete
        or summary.failed_invoices > 0
        or summary.failed_invoice_emails > 0
        or summary.missing_invoice_ids > 0
    )
    if not args.dry_run and run_failed:
        put_metric("InvoiceStandaloneRunFailed", 1, project, reporting_defaults)
        if reconcile:
            put_metric("InvoiceReconciliationRunFailed", 1, project, reporting_defaults)
        raise RuntimeError(
            (
                f"Invoice automation failed for project '{project}': "
                f"scan_complete={summary.scan_complete}, "
                f"create_failures={summary.failed_invoices}, "
                f"email_failures={summary.failed_invoice_emails}, "
                f"missing_invoice_ids={summary.missing_invoice_ids}"
            )
        )

    if not args.dry_run:
        put_metric("InvoiceStandaloneRunSucceeded", 1, project, reporting_defaults)
        if reconcile:
            put_metric("InvoiceReconciliationRunSucceeded", 1, project, reporting_defaults)
            put_metric(
                "InvoiceReconciliationRecovered",
                summary.created_invoices,
                project,
                reporting_defaults,
            )

    return {
        "project": summary.project,
        "enabled": True,
        "from_date": summary.date_from,
        "to_date": summary.date_to,
        "matched_orders": summary.matched_orders,
        "created_invoices": summary.created_invoices,
        "failed_invoices": summary.failed_invoices,
        "emailed_invoices": summary.emailed_invoices,
        "failed_invoice_emails": summary.failed_invoice_emails,
        "missing_invoice_ids": summary.missing_invoice_ids,
        "already_present_invoices": summary.already_present_invoices,
        "skipped_stale_orders": summary.skipped_stale_orders,
        "skipped_zero_total_orders": summary.skipped_zero_total_orders,
        "skipped_before_automation_start": summary.skipped_before_automation_start,
        "skipped_non_cod_orders": summary.skipped_non_cod_orders,
        "scan_mode": summary.scan_mode,
        "scan_complete": summary.scan_complete,
        "total_orders_fetched": summary.total_orders_fetched,
        "purchase_date_orders_fetched": summary.purchase_date_orders_fetched,
        "recent_change_orders_fetched": summary.recent_change_orders_fetched,
        "pages_fetched": summary.pages_fetched,
        "page_retry_count": summary.page_retry_count,
        "reconcile": reconcile,
        "dry_run": summary.dry_run,
    }


def main() -> None:
    load_dotenv(encoding="utf-8-sig")
    args = parse_args()
    try:
        result = run_invoice_runner(args)
    except Exception as exc:
        project = (args.project or DEFAULT_PROJECT).strip() or DEFAULT_PROJECT
        print(
            "INVOICE_RUN_FAILED "
            f"project={project} "
            f"scan_mode={'reconciliation' if args.reconcile else 'regular'} "
            f"error_type={type(exc).__name__}"
        )
        raise
    print(
        "INVOICE_RUN_SUCCEEDED "
        f"project={result.get('project')} "
        f"scan_mode={result.get('scan_mode', 'disabled')} "
        f"scan_complete={result.get('scan_complete', False)} "
        f"matched={result.get('matched_orders', 0)} "
        f"created={result.get('created_invoices', 0)}"
    )


if __name__ == "__main__":
    main()
