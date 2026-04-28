#!/usr/bin/env python3
"""
Daily report automation runner.

Runs export_orders.py for a fixed historical range ending at "yesterday"
in the configured timezone, then sends report links/attachments via AWS SES.
Optional S3 upload is supported.
"""

import argparse
import calendar
import csv
import json
import mimetypes
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from generate_invoices import resolve_invoice_date_window, resolve_invoice_generation_settings, run_invoice_generation
from reporting_core import (
    BASE_DEFAULT_PROJECT,
    build_artifact_set,
    load_project_env,
    load_project_settings,
    put_metric,
    project_data_dir,
    resolve_reporting_defaults,
    sanitize_output_tag,
)


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_PROJECT = os.getenv("REPORT_PROJECT", BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
STABLE_LIVE_ARTIFACT_NAMES = {
    "report_latest.html",
    "dashboard_payload_latest.json",
}


def bootstrap_project_from_argv(argv: List[str]) -> str:
    for idx, arg in enumerate(argv):
        if arg == "--project" and idx + 1 < len(argv):
            return argv[idx + 1].strip() or DEFAULT_PROJECT
        if arg.startswith("--project="):
            return arg.split("=", 1)[1].strip() or DEFAULT_PROJECT
    return DEFAULT_PROJECT


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and email daily report")
    parser.add_argument(
        "--project",
        default=os.getenv("REPORT_PROJECT", DEFAULT_PROJECT),
        help="Project name (uses projects/<project>/.env and data/<project>/ outputs)",
    )
    parser.add_argument(
        "--from-date",
        default=os.getenv("REPORT_FROM_DATE", "2025-05-03"),
        help="Start date in YYYY-MM-DD format (default: REPORT_FROM_DATE or 2025-05-03)",
    )
    parser.add_argument(
        "--to-date",
        default=os.getenv("REPORT_TO_DATE", ""),
        help="End date in YYYY-MM-DD format. If empty, uses yesterday in REPORT_TIMEZONE.",
    )
    parser.add_argument(
        "--timezone",
        default=os.getenv("REPORT_TIMEZONE", "Europe/Bratislava"),
        help="Timezone used for 'yesterday' calculation",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="Force fresh fetch from API",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        default=False,
        help="Clear cache before export",
    )
    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip export step and only send latest files",
    )
    parser.add_argument(
        "--skip-email",
        action="store_true",
        help="Skip email sending",
    )
    parser.add_argument(
        "--skip-invoices",
        action="store_true",
        default=env_bool("REPORT_SKIP_INVOICES", True),
        help="Skip automatic invoice generation for this run. Default: true; use invoice_runner.py for scheduled invoices.",
    )
    parser.add_argument(
        "--invoice-dry-run",
        action="store_true",
        default=env_bool("REPORT_INVOICE_DRY_RUN", False),
        help="Preview invoice candidates without creating invoices.",
    )
    parser.add_argument(
        "--output-tag",
        default=os.getenv("REPORT_OUTPUT_TAG", ""),
        help="Optional output tag for side-by-side test artifacts (e.g. ui_test).",
    )
    return parser.parse_args(argv)


def resolve_to_date(to_date_arg: str, tz_name: str) -> str:
    if to_date_arg:
        datetime.strptime(to_date_arg, "%Y-%m-%d")
        return to_date_arg

    tz = ZoneInfo(tz_name)
    yesterday = (datetime.now(tz).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    return yesterday


def normalize_date(value: str) -> str:
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format '{value}'. Use YYYY-MM-DD.")


def maybe_run_invoice_automation(
    project: str,
    report_to_date: str,
    reporting_defaults: Dict[str, Any],
    dry_run: bool = False,
) -> Optional[Dict[str, Any]]:
    invoice_settings = resolve_invoice_generation_settings(load_project_settings(project))
    if not invoice_settings["enabled"]:
        print(f"Invoice automation disabled for project={project}")
        return None

    invoice_from_date, invoice_to_date = resolve_invoice_date_window(
        report_to_date,
        invoice_settings["lookback_days"],
    )
    print(
        f"Running invoice automation for project={project} "
        f"window={invoice_from_date}..{invoice_to_date} dry_run={dry_run}"
    )
    try:
        summary = run_invoice_generation(
            project_name=project,
            date_from=invoice_from_date,
            date_to=invoice_to_date,
            dry_run=dry_run,
        )
    except Exception:
        put_metric("InvoiceAutomationRunFailed", 1, project, reporting_defaults)
        raise

    put_metric("InvoiceAutomationMatchedOrders", summary.matched_orders, project, reporting_defaults)
    put_metric("InvoiceAutomationSkippedZeroTotal", summary.skipped_zero_total_orders, project, reporting_defaults)
    put_metric("InvoiceAutomationCreated", summary.created_invoices, project, reporting_defaults)
    put_metric("InvoiceAutomationCreateFailures", summary.failed_invoices, project, reporting_defaults)
    put_metric("InvoiceAutomationRunSucceeded", 1, project, reporting_defaults)

    if not dry_run and summary.failed_invoices:
        raise RuntimeError(
            f"Invoice automation failed for {summary.failed_invoices} order(s) in project '{project}'"
        )

    return {
        "from_date": summary.date_from,
        "to_date": summary.date_to,
        "matched_orders": summary.matched_orders,
        "created_invoices": summary.created_invoices,
        "failed_invoices": summary.failed_invoices,
        "skipped_zero_total_orders": summary.skipped_zero_total_orders,
        "dry_run": summary.dry_run,
    }


def run_export(
    project: str,
    from_date: str,
    to_date: str,
    clear_cache: bool,
    no_cache: bool,
    output_tag: str = "",
) -> None:
    cmd: List[str] = [
        sys.executable,
        str(ROOT_DIR / "export_orders.py"),
        "--project",
        project,
        "--from-date",
        from_date,
        "--to-date",
        to_date,
    ]
    if clear_cache:
        cmd.append("--clear-cache")
    if no_cache:
        cmd.append("--no-cache")
    if output_tag:
        cmd.extend(["--output-tag", output_tag])

    print("Running export:", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(ROOT_DIR), check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"export_orders.py failed with exit code {proc.returncode}")


def load_data_quality(path: Optional[Path]) -> Dict[str, Any]:
    if not path or not path.exists():
        return {
            "overall_status": "partial",
            "is_partial": True,
            "summary": "Data quality metadata file is missing for this run. Source completeness cannot be verified.",
            "sources": {},
        }

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "overall_status": "partial",
            "is_partial": True,
            "summary": f"Data quality metadata could not be parsed ({exc}). Source completeness cannot be verified.",
            "sources": {},
        }


def build_data_quality_summary(data_quality: Dict[str, Any]) -> str:
    overall_status = str(data_quality.get("overall_status") or "unknown").upper()
    qa_status = str(data_quality.get("qa_status") or "unknown").upper()
    summary = str(data_quality.get("summary") or "No data quality summary is available.")
    qa_failure_count = int(data_quality.get("qa_failure_count") or 0)
    qa_warning_count = int(data_quality.get("qa_warning_count") or 0)
    partial_sources = list(data_quality.get("partial_sources") or [])
    qa_errors = list(data_quality.get("qa_errors") or [])
    qa_warnings = list(data_quality.get("qa_warnings") or [])

    lines = [
        "DATA QUALITY",
        f"- Overall status: {overall_status}",
        f"- QA status: {qa_status}",
        f"- QA failures: {qa_failure_count}",
        f"- QA warnings: {qa_warning_count}",
        f"- Summary: {summary}",
    ]
    if partial_sources:
        lines.append(f"- Partial sources: {', '.join(map(str, partial_sources[:5]))}")
    if qa_errors:
        lines.append(f"- Critical QA checks: {', '.join(map(str, qa_errors[:5]))}")
    if qa_warnings:
        lines.append(f"- Warning QA checks: {', '.join(map(str, qa_warnings[:5]))}")
    return "\n".join(lines)


def s3_upload_outputs(project: str, paths: Dict[str, Path]) -> Dict[str, str]:
    bucket = os.getenv("REPORT_S3_BUCKET", "").strip()
    prefix = os.getenv("REPORT_S3_PREFIX", "").strip().strip("/")
    if not prefix:
        prefix = f"daily-reports/{project}"
    region = os.getenv("AWS_REGION", "eu-central-1").strip()

    if not bucket:
        return {}

    try:
        import boto3  # type: ignore
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 uploads. Install dependencies from requirements.txt.") from exc

    s3 = boto3.client("s3", region_name=region)
    uploaded_links: Dict[str, str] = {}
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for key, path in paths.items():
        if not path.exists():
            continue
        object_key = f"{prefix}/{timestamp}/{path.name}"
        content_type, _ = mimetypes.guess_type(path.name)
        extra_args = {"ContentType": content_type} if content_type else {}

        s3.upload_file(str(path), bucket, object_key, ExtraArgs=extra_args)

        expires = int(os.getenv("REPORT_S3_PRESIGN_EXPIRES_SEC", "604800"))
        presigned = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": object_key},
            ExpiresIn=expires,
        )
        uploaded_links[key] = presigned

        if path.name in STABLE_LIVE_ARTIFACT_NAMES:
            stable_key = f"{prefix}/latest/{path.name}"
            s3.upload_file(str(path), bucket, stable_key, ExtraArgs=extra_args)
            stable_presigned = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": stable_key},
                ExpiresIn=expires,
            )
            uploaded_links[f"{key}_latest"] = stable_presigned

    return uploaded_links


def build_email_subject(reporting_defaults: Dict[str, Any]) -> str:
    return os.getenv("REPORT_EMAIL_SUBJECT", reporting_defaults["email_subject"]).strip()


def _to_float(value: str) -> float:
    try:
        return float((value or "").strip())
    except (ValueError, TypeError, AttributeError):
        return 0.0


def _to_int(value: str) -> int:
    try:
        return int(float((value or "").strip()))
    except (ValueError, TypeError, AttributeError):
        return 0


def _fmt_eur(value: float) -> str:
    return f"{value:,.2f} EUR".replace(",", " ")


def _fmt_pct(value: float) -> str:
    return f"{value:.2f}%"


def _pct_change(current: float, previous: float) -> Optional[float]:
    if previous == 0:
        if current == 0:
            return 0.0
        return None
    return ((current - previous) / abs(previous)) * 100


def _format_change(change: Optional[float]) -> str:
    if change is None:
        return "N/A"
    sign = "+" if change >= 0 else ""
    return f"{sign}{change:.2f}%"


def _classify_comparison(change: Optional[float]) -> str:
    if change is None:
        return "Nedostatok dat"
    if change > 20:
        return "Strong Growth"
    if change > 5:
        return "Moderate Growth"
    if -5 <= change <= 5:
        return "Stable"
    if change >= -20:
        return "Moderate Decline"
    return "Strong Decline"


def _classify_trend(
    daily_change: Optional[float],
    change_7d: Optional[float],
    change_30d: Optional[float],
    lower_is_better: bool = False,
) -> str:
    score = 0

    def score_from_change(value: Optional[float], threshold: float) -> int:
        if value is None:
            return 0
        adjusted = -value if lower_is_better else value
        if adjusted > threshold:
            return 1
        if adjusted < -threshold:
            return -1
        return 0

    score += score_from_change(daily_change, 3.0)
    score += score_from_change(change_7d, 5.0)
    score += score_from_change(change_30d, 5.0)

    if score >= 2:
        return "Improving"
    if score <= -2:
        return "Deteriorating"
    return "Stable"


def _shift_months(d: date, months: int) -> date:
    month_index = (d.month - 1) + months
    year = d.year + (month_index // 12)
    month = (month_index % 12) + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(d.day, last_day))


def _shift_years(d: date, years: int) -> date:
    target_year = d.year + years
    try:
        return d.replace(year=target_year)
    except ValueError:
        return d.replace(year=target_year, day=28)


def _load_daily_rows(date_csv: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with date_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_str = (row.get("date") or "").strip()
            if not date_str:
                continue
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            revenue = _to_float(row.get("total_revenue", ""))
            orders = _to_int(row.get("unique_orders", ""))
            product_costs = _to_float(row.get("product_expense", ""))
            packaging_costs = _to_float(row.get("packaging_cost", ""))
            shipping_subsidy = _to_float(row.get("shipping_net_cost", row.get("shipping_subsidy_cost", "")))
            facebook_ads = _to_float(row.get("fb_ads_spend", ""))
            google_ads = _to_float(row.get("google_ads_spend", ""))
            total_ads = facebook_ads + google_ads
            profit = _to_float(row.get("net_profit", ""))
            pre_ad_contribution = _to_float(row.get("pre_ad_contribution_profit", ""))
            contribution_margin_percent = _to_float(row.get("pre_ad_contribution_margin_pct", ""))
            fixed_daily_cost = _to_float(row.get("fixed_daily_cost", ""))

            aov = (revenue / orders) if orders > 0 else 0.0
            roas = (revenue / total_ads) if total_ads > 0 else 0.0
            contribution_per_order = (pre_ad_contribution / orders) if orders > 0 else 0.0
            post_ad_contribution_per_order = (profit / orders) if orders > 0 else 0.0

            rows.append(
                {
                    "date": d,
                    "revenue": revenue,
                    "orders": orders,
                    "units_sold": _to_int(row.get("total_quantity", "")),
                    "aov": aov,
                    "product_costs": product_costs,
                    "packaging_costs": packaging_costs,
                    "shipping_subsidy": shipping_subsidy,
                    "shipping_net": shipping_subsidy,
                    "facebook_ads": facebook_ads,
                    "google_ads": google_ads,
                    "total_ads": total_ads,
                    "profit": profit,
                    "fixed_daily_cost": fixed_daily_cost,
                    "roas": roas,
                    "contribution_margin_percent": contribution_margin_percent,
                    "pre_ad_contribution": pre_ad_contribution,
                    "contribution_per_order": contribution_per_order,
                    "post_ad_contribution_per_order": post_ad_contribution_per_order,
                }
            )
    return sorted(rows, key=lambda r: r["date"])


def _load_order_records(export_csv: Optional[Path]) -> List[Dict[str, Any]]:
    if not export_csv or not export_csv.exists():
        return []

    orders_map: Dict[str, Dict[str, Any]] = {}
    with export_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            order_num = (row.get("order_num") or "").strip()
            if not order_num or order_num in orders_map:
                continue
            date_str = (row.get("purchase_date") or "").split(" ")[0].strip()
            if not date_str:
                continue
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            email = (row.get("customer_email") or "").strip().lower()
            if not email:
                continue
            first_date = None
            first_date_raw = (row.get("customer_first_purchase_date") or "").split(" ")[0].strip()
            if first_date_raw:
                try:
                    first_date = datetime.strptime(first_date_raw, "%Y-%m-%d").date()
                except ValueError:
                    first_date = None
            orders_map[order_num] = {"date": d, "email": email, "first_date": first_date}
    return list(orders_map.values())


def _load_customer_dynamics(order_records: List[Dict[str, Any]]) -> Dict[date, Dict[str, int]]:
    if not order_records:
        return {}

    first_order_date: Dict[str, date] = {}
    for order in order_records:
        email = order["email"]
        d = order.get("first_date") or order["date"]
        if email not in first_order_date or d < first_order_date[email]:
            first_order_date[email] = d

    buckets: Dict[date, Dict[str, Any]] = {}
    for order in order_records:
        d = order["date"]
        email = order["email"]
        if d not in buckets:
            buckets[d] = {
                "new_customers_set": set(),
                "returning_customers_set": set(),
                "new_orders": 0,
                "returning_orders": 0,
            }
        if first_order_date[email] == d:
            buckets[d]["new_customers_set"].add(email)
            buckets[d]["new_orders"] += 1
        else:
            buckets[d]["returning_customers_set"].add(email)
            buckets[d]["returning_orders"] += 1

    result: Dict[date, Dict[str, int]] = {}
    for d, data in buckets.items():
        result[d] = {
            "new_customers": len(data["new_customers_set"]),
            "returning_customers": len(data["returning_customers_set"]),
            "new_orders": int(data["new_orders"]),
            "returning_orders": int(data["returning_orders"]),
        }
    return result


def _window_unique_customers(order_records: List[Dict[str, Any]], end_date: date, days: int) -> int:
    start_date = end_date - timedelta(days=days - 1)
    customers = {
        rec["email"]
        for rec in order_records
        if start_date <= rec["date"] <= end_date
    }
    return len(customers)


def _window_aggregate(
    row_by_date: Dict[date, Dict[str, Any]],
    end_date: date,
    days: int,
    customer_by_date: Dict[date, Dict[str, int]],
    order_records: List[Dict[str, Any]],
) -> Dict[str, Optional[float]]:
    revenue = 0.0
    orders = 0
    ads = 0.0
    fb_ads = 0.0
    google_ads = 0.0
    profit = 0.0
    pre_ad_contribution = 0.0
    fixed_overhead = 0.0
    new_customers = 0
    returning_orders = 0
    returning_customers = 0

    for i in range(days):
        d = end_date - timedelta(days=(days - 1 - i))
        row = row_by_date.get(d)
        if row:
            revenue += float(row["revenue"])
            orders += int(row["orders"])
            ads += float(row["total_ads"])
            fb_ads += float(row["facebook_ads"])
            google_ads += float(row["google_ads"])
            profit += float(row["profit"])
            pre_ad_contribution += float(row["pre_ad_contribution"])
            fixed_overhead += float(row.get("fixed_daily_cost", 0.0) or 0.0)

        customer = customer_by_date.get(d, {})
        new_customers += int(customer.get("new_customers", 0))
        returning_orders += int(customer.get("returning_orders", 0))
        returning_customers += int(customer.get("returning_customers", 0))

    aov = (revenue / orders) if orders > 0 else 0.0
    roas = (revenue / ads) if ads > 0 else 0.0
    contribution_margin = (pre_ad_contribution / revenue * 100) if revenue > 0 else 0.0
    post_ad_margin = (profit / revenue * 100) if revenue > 0 else 0.0
    contribution_per_order = (pre_ad_contribution / orders) if orders > 0 else 0.0
    profit_per_order = (profit / orders) if orders > 0 else 0.0
    cac = (ads / new_customers) if new_customers > 0 else None
    returning_customer_rate = (returning_orders / orders * 100) if orders > 0 else None
    payback_orders = (cac / contribution_per_order) if (cac is not None and contribution_per_order > 0) else None
    unique_customers = _window_unique_customers(order_records, end_date, days)
    ltv = (revenue / unique_customers) if unique_customers > 0 else None
    # aggregate_by_date CSV rows already use net_profit, which includes fixed overhead
    company_profit_with_fixed = profit
    company_margin_with_fixed = (company_profit_with_fixed / revenue * 100) if revenue > 0 else 0.0

    return {
        "revenue": revenue,
        "orders": float(orders),
        "ads": ads,
        "fb_ads": fb_ads,
        "google_ads": google_ads,
        "profit": profit,
        "pre_ad_contribution": pre_ad_contribution,
        "aov": aov,
        "roas": roas,
        "pre_ad_contribution_margin": contribution_margin,
        "contribution_margin": contribution_margin,
        "post_ad_margin": post_ad_margin,
        "company_margin_with_fixed": company_margin_with_fixed,
        "company_profit_with_fixed": company_profit_with_fixed,
        "fixed_overhead": fixed_overhead,
        "contribution_per_order": contribution_per_order,
        "profit_per_order": profit_per_order,
        "new_customers": float(new_customers),
        "returning_orders": float(returning_orders),
        "returning_customers": float(returning_customers),
        "returning_customer_rate": returning_customer_rate,
        "cac": cac,
        "payback_orders": payback_orders,
        "unique_customers": float(unique_customers),
        "ltv": ltv,
    }


def _comparison_line(label: str, current: float, previous: float) -> str:
    change = _pct_change(current, previous)
    return f"- {label}: {_format_change(change)} ({_classify_comparison(change)})"


def _comparison_line_optional(label: str, current: Optional[float], previous: Optional[float]) -> str:
    if current is None or previous is None:
        return f"- {label}: Nedostatok dat"
    return _comparison_line(label, float(current), float(previous))


def _load_dashboard_payload(path: Optional[Path]) -> Dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _build_roy_inventory_alert_summary(file_paths: Dict[str, Path]) -> str:
    payload = _load_dashboard_payload(file_paths.get("dashboard_payload_json"))
    dashboard = payload.get("dashboard") if isinstance(payload.get("dashboard"), dict) else payload
    if not isinstance(dashboard, dict):
        return ""

    roy_inventory = dashboard.get("roy_product_demand") or {}
    if not isinstance(roy_inventory, dict):
        return ""

    summary = roy_inventory.get("summary") or {}
    if not isinstance(summary, dict):
        summary = {}
    alert_rows = list(roy_inventory.get("alert_rows") or [])
    if not summary and not alert_rows:
        return ""

    inventory_status = str(summary.get("inventory_status") or "unavailable").strip().lower()
    snapshot_date = str(summary.get("inventory_snapshot_date") or "-")
    horizon_days = max(1, int(float(summary.get("alert_delivery_horizon_days") or 30)))
    alert_count = int(float(summary.get("alert_delivery_count") or 0))
    hero_count = int(float(summary.get("alert_delivery_hero_count") or 0))
    reorder_now_count = int(float(summary.get("alert_reorder_now_count") or 0))
    prepare_po_count = int(float(summary.get("alert_prepare_po_count") or 0))
    excluded_count = int(float(summary.get("alert_excluded_count") or 0))
    lead_time_count = int(float(summary.get("lead_time_configured_alert_count") or 0))
    bundle_rule_count = int(float(summary.get("bundle_component_rule_count") or 0))
    bundle_shift_30d = float(summary.get("bundle_component_adjustment_30d_units") or 0.0)
    bundle_shift_90d = float(summary.get("bundle_component_adjustment_90d_units") or 0.0)
    inbound_status = str(summary.get("inbound_stock_status") or "not_modeled").replace("_", " ")
    inbound_source = str(summary.get("inbound_stock_source") or "not_modeled")
    inventory_cost_value = float(summary.get("inventory_cost_value") or 0.0)
    revenue_at_risk_30d = float(summary.get("revenue_at_risk_30d") or 0.0)
    profit_at_risk_30d = float(summary.get("profit_at_risk_30d") or 0.0)
    risk_45d_count = int(float(summary.get("stock_risk_45d_count") or 0))

    lines = ["SKLADOVE ALERTY"]
    if inventory_status != "ok":
        lines.append(
            f"- Inventory snapshot status: {inventory_status.upper()} (snapshot {snapshot_date})."
        )
        if inbound_status != "configured":
            lines.append(f"- Inbound stock zatial nie je modelovany ({inbound_source}).")
        return "\n".join(lines)

    lines.extend(
        [
            (
                f"- Snapshot skladu: {snapshot_date}. Akcne {horizon_days}d alerty: {alert_count}, "
                f"z toho hero SKU {hero_count}. 45d watchlist: {risk_45d_count}."
            ),
            (
                f"- Objednat teraz: {reorder_now_count}; pripravit PO: {prepare_po_count}; "
                f"lead-time nastavene alerty: {lead_time_count}."
            ),
            (
                f"- Hodnota skladu v nakupnych cenach: {_fmt_eur(inventory_cost_value)}. "
                f"Trzby v riziku {horizon_days}d: {_fmt_eur(revenue_at_risk_30d)}; "
                f"zisk v riziku: {_fmt_eur(profit_at_risk_30d)}."
            ),
        ]
    )
    if excluded_count > 0:
        lines.append(
            f"- Z alertov je odfiltrovanych {excluded_count} poloziek (servis/reklamacie/darceky/test SKU a bundle noise)."
        )
    if bundle_rule_count > 0:
        lines.append(
            f"- Bundle dopyt sa presuva na komponenty cez {bundle_rule_count} pravidla: +{bundle_shift_30d:.1f} ks za 30d a +{bundle_shift_90d:.1f} ks za 90d."
        )
    if inbound_status != "configured":
        lines.append(f"- Inbound stock zatial nie je modelovany ({inbound_source}), takze alerty su konzervativnejsie.")

    top_alerts = alert_rows[:5]
    if top_alerts:
        lines.append("- Top alerty na dnes:")
        for row in top_alerts:
            product = str(row.get("product") or "Unknown product")
            risk = str(row.get("stock_risk_level") or "-")
            available = float(row.get("available_quantity") or 0.0)
            days_of_cover = float(row.get("days_of_cover") or 0.0)
            lead_time = int(float(row.get("lead_time_working_days") or 0))
            reorder_by = str(row.get("reorder_by_date") or "-")
            reorder_units = int(round(float(row.get("suggested_reorder_units") or 0.0)))
            action = str(row.get("reorder_action_label") or "-")
            revenue_risk = float(row.get("alert_30d_revenue") or 0.0)
            hero_flag = " HERO" if bool(row.get("strategic_stock_flag")) else ""
            lines.append(
                f"  - {product}{hero_flag}: {risk}, sklad {available:.1f} ks, cover {days_of_cover:.1f} dna, "
                f"LT {lead_time} wd, objednat do {reorder_by}, odporucane {reorder_units} ks, akcia {action}, "
                f"trzba v riziku {_fmt_eur(revenue_risk)}."
            )
    else:
        lines.append("- Dnes nie su ziadne akcne 30-dnove stock alerty.")

    return "\n".join(lines)


def build_report_summary(file_paths: Dict[str, Path]) -> str:
    date_csv = file_paths.get("aggregate_by_date_csv")
    if not date_csv or not date_csv.exists():
        return "EXECUTIVE SUMMARY\nNie je mozne vytvorit slovny suhrn, chyba aggregate_by_date CSV."

    daily_rows = _load_daily_rows(date_csv)
    if not daily_rows:
        return "EXECUTIVE SUMMARY\nNie je mozne vytvorit slovny suhrn, aggregate_by_date CSV je prazdny."

    export_csv = file_paths.get("export_csv")
    order_records = _load_order_records(export_csv)
    customer_by_date = _load_customer_dynamics(order_records)
    row_by_date: Dict[date, Dict[str, Any]] = {row["date"]: row for row in daily_rows}

    last_row = daily_rows[-1]
    last_date: date = last_row["date"]
    prev_day = last_date - timedelta(days=1)
    d_prev = row_by_date.get(prev_day)

    total = _window_aggregate(row_by_date, last_date, len(daily_rows), customer_by_date, order_records)
    w7 = _window_aggregate(row_by_date, last_date, 7, customer_by_date, order_records)
    w7_prev = _window_aggregate(row_by_date, last_date - timedelta(days=7), 7, customer_by_date, order_records)
    w30 = _window_aggregate(row_by_date, last_date, 30, customer_by_date, order_records)
    w30_prev = _window_aggregate(row_by_date, last_date - timedelta(days=30), 30, customer_by_date, order_records)

    same_weekday_last_week = last_date - timedelta(days=7)
    same_day_last_month = _shift_months(last_date, -1)
    same_day_last_year = _shift_years(last_date, -1)

    d_week = row_by_date.get(same_weekday_last_week)
    d_month = row_by_date.get(same_day_last_month)
    d_year = row_by_date.get(same_day_last_year)

    w7_month = _window_aggregate(row_by_date, _shift_months(last_date, -1), 7, customer_by_date, order_records)
    w7_year = _window_aggregate(row_by_date, _shift_years(last_date, -1), 7, customer_by_date, order_records)
    w30_year = _window_aggregate(row_by_date, _shift_years(last_date, -1), 30, customer_by_date, order_records)

    revenue_daily_change = _pct_change(float(last_row["revenue"]), float(d_prev["revenue"])) if d_prev else None
    revenue_7_change = _pct_change(float(w7["revenue"] or 0), float(w7_prev["revenue"] or 0))
    revenue_30_change = _pct_change(float(w30["revenue"] or 0), float(w30_prev["revenue"] or 0))

    orders_daily_change = _pct_change(float(last_row["orders"]), float(d_prev["orders"])) if d_prev else None
    orders_7_change = _pct_change(float(w7["orders"] or 0), float(w7_prev["orders"] or 0))
    orders_30_change = _pct_change(float(w30["orders"] or 0), float(w30_prev["orders"] or 0))

    aov_daily_change = _pct_change(float(last_row["aov"]), float(d_prev["aov"])) if d_prev else None
    aov_7_change = _pct_change(float(w7["aov"] or 0), float(w7_prev["aov"] or 0))
    aov_30_change = _pct_change(float(w30["aov"] or 0), float(w30_prev["aov"] or 0))

    cac_daily_change = None
    if d_prev:
        prev_new = int(customer_by_date.get(prev_day, {}).get("new_customers", 0))
        today_new = int(customer_by_date.get(last_date, {}).get("new_customers", 0))
        prev_cac = (float(d_prev["total_ads"]) / prev_new) if prev_new > 0 else None
        today_cac = (float(last_row["total_ads"]) / today_new) if today_new > 0 else None
        if today_cac is not None and prev_cac is not None:
            cac_daily_change = _pct_change(today_cac, prev_cac)

    cac_7_change = _pct_change(float(w7["cac"]), float(w7_prev["cac"])) if (w7["cac"] is not None and w7_prev["cac"] is not None) else None
    cac_30_change = _pct_change(float(w30["cac"]), float(w30_prev["cac"])) if (w30["cac"] is not None and w30_prev["cac"] is not None) else None

    roas_daily_change = _pct_change(float(last_row["roas"]), float(d_prev["roas"])) if d_prev else None
    roas_7_change = _pct_change(float(w7["roas"] or 0), float(w7_prev["roas"] or 0))
    roas_30_change = _pct_change(float(w30["roas"] or 0), float(w30_prev["roas"] or 0))

    cm_daily_change_pp = (float(last_row["contribution_margin_percent"]) - float(d_prev["contribution_margin_percent"])) if d_prev else None
    cm_7_change = _pct_change(float(w7["contribution_margin"] or 0), float(w7_prev["contribution_margin"] or 0))
    cm_30_change = _pct_change(float(w30["contribution_margin"] or 0), float(w30_prev["contribution_margin"] or 0))

    profit_daily_change = _pct_change(float(last_row["profit"]), float(d_prev["profit"])) if d_prev else None
    profit_7_change = _pct_change(float(w7["profit"] or 0), float(w7_prev["profit"] or 0))
    profit_30_change = _pct_change(float(w30["profit"] or 0), float(w30_prev["profit"] or 0))

    ret_7_change = _pct_change(float(w7["returning_customer_rate"] or 0), float(w7_prev["returning_customer_rate"] or 0))
    ret_30_change = _pct_change(float(w30["returning_customer_rate"] or 0), float(w30_prev["returning_customer_rate"] or 0))

    today_new = int(customer_by_date.get(last_date, {}).get("new_customers", 0))
    daily_cac = (float(last_row["total_ads"]) / today_new) if today_new > 0 else None
    daily_payback = (daily_cac / float(last_row["contribution_per_order"])) if (daily_cac is not None and float(last_row["contribution_per_order"]) > 0) else None
    if orders_7_change is not None and aov_7_change is not None:
        if abs(orders_7_change) >= abs(aov_7_change):
            revenue_driver = (
                f"Zmenu obratu tahne skor pocet objednavok ({_format_change(orders_7_change)}) "
                f"nez hodnota kosika ({_format_change(aov_7_change)})."
            )
        else:
            revenue_driver = (
                f"Zmenu obratu taha skor hodnota kosika ({_format_change(aov_7_change)}) "
                f"nez pocet objednavok ({_format_change(orders_7_change)})."
            )
    else:
        revenue_driver = "Na urcenie hlavneho dovodu zmeny obratu chyba dostatok dat."

    overview_lines = [
        "RYCHLY PREHLAD",
        f"- Cele obdobie: {int(total['orders'] or 0)} objednavok, obrat bez DPH {_fmt_eur(float(total['revenue'] or 0))}, cisty zisk po reklamach a fixoch {_fmt_eur(float(total['profit'] or 0))}.",
        f"- Poslednych 7 dni: obrat {_fmt_eur(float(w7['revenue'] or 0))}, cisty zisk {_fmt_eur(float(w7['profit'] or 0))}, spend {_fmt_eur(float(w7['ads'] or 0))}, ROAS {float(w7['roas'] or 0):.2f}x.",
        f"- Posledny den ({last_date.isoformat()}): {int(last_row['orders'] or 0)} objednavok, obrat {_fmt_eur(float(last_row['revenue'] or 0))}, zisk {_fmt_eur(float(last_row['profit'] or 0))}, AOV {_fmt_eur(float(last_row['aov'] or 0))}.",
    ]
    if w7["cac"] is not None:
        overview_lines.append(
            f"- Cena za noveho zakaznika za 7 dni je {_fmt_eur(float(w7['cac']))}; na jednu objednavku pred reklamou ostava {_fmt_eur(float(w7['contribution_per_order'] or 0))}."
        )

    good_lines = ["CO JE DOBRE"]
    if (w7["profit"] or 0) > 0:
        good_lines.append(f"- Poslednych 7 dni je biznis stale v pluse: {_fmt_eur(float(w7['profit'] or 0))}.")
    if (w7["roas"] or 0) > 2.5:
        good_lines.append(
            f"- Reklama stale funguje: z 1 EUR do reklam sa vracia {float(w7['roas'] or 0):.2f} EUR v obrate."
        )
    if (w30["returning_customer_rate"] or 0) > 20:
        good_lines.append(
            f"- V poslednych 30 dnoch tvorili opakovane objednavky {_fmt_pct(float(w30['returning_customer_rate'] or 0))}, co pomaha stabilite."
        )
    if (w30["contribution_margin"] or 0) > 40:
        good_lines.append(
            f"- Pred reklamou ostava z obratu {_fmt_pct(float(w30['contribution_margin'] or 0))}, takze je tam priestor na akviziciu."
        )
    if len(good_lines) == 1:
        good_lines.append("- V datoch nie je momentalne jeden extra silny pozitivny signal, skor zmiesany obraz.")

    weaker_lines = ["CO SA ZHORSILO"]
    if revenue_7_change is not None and revenue_7_change < -3:
        weaker_lines.append(
            f"- Oproti predchadzajucim 7 dnom klesol obrat o {_fmt_pct(abs(revenue_7_change))}."
        )
    if profit_7_change is not None and profit_7_change < -10:
        weaker_lines.append(
            f"- Zisk za poslednych 7 dni klesol o {_fmt_pct(abs(profit_7_change))}, co je vyrazne rychlejsi pokles ako samotny obrat."
        )
    if aov_7_change is not None and aov_7_change < -8:
        weaker_lines.append(
            f"- Priemerna hodnota objednavky padla o {_fmt_pct(abs(aov_7_change))}, takze kosik je mensi ako minuly tyzden."
        )
    if cac_7_change is not None and cac_7_change > 15:
        weaker_lines.append(
            f"- Ziskanie noveho zakaznika je drahsie: CAC narastol o {_fmt_pct(cac_7_change)}."
        )
    if roas_7_change is not None and roas_7_change < -15:
        weaker_lines.append(
            f"- Reklama je slabsia ako minuly tyzden: 7d ROAS klesol o {_fmt_pct(abs(roas_7_change))}."
        )
    if ret_30_change is not None and ret_30_change < -8:
        weaker_lines.append(
            f"- Podiel opakovanych objednavok v 30d okne klesa ({_format_change(ret_30_change)}), co moze zhorsovat kvalitu obratu."
        )
    if len(weaker_lines) == 1:
        weaker_lines.append("- Nevidim ziadny kriticky problem, skor bezne kolisanie.")

    insight_lines = [
        "CO TO PRAVDEPODOBNE SPOSOBILO",
        f"- {revenue_driver}",
    ]
    if orders_7_change is not None and aov_7_change is not None and orders_7_change > 0 and aov_7_change < 0:
        insight_lines.append(
            "- Objednavok je viac, ale ludia nechavaju menej penazi v jednom kosiku. Problem nie je dopyt, ale hodnota objednavky."
        )
    if (w7["ads"] or 0) > 0 and (w7_prev["ads"] or 0) > 0:
        ads_change_7 = _pct_change(float(w7["ads"] or 0), float(w7_prev["ads"] or 0))
        if ads_change_7 is not None and revenue_7_change is not None and ads_change_7 > 10 and revenue_7_change < ads_change_7:
            insight_lines.append(
                f"- Spend za 7 dni sa zmenil o {_format_change(ads_change_7)}, ale obrat len o {_format_change(revenue_7_change)}. Reklama momentalne taha slabsi efekt."
            )
    if w30["payback_orders"] is not None:
        insight_lines.append(
            f"- Navratnost akvizicie vychadza na {float(w30['payback_orders']):.2f} objednavky. Cim blizsie k 1, tym bezpecnejsie sa da skalovat reklama."
        )
    elif daily_payback is not None:
        insight_lines.append(f"- Denny odhad navratnosti vychadza na {daily_payback:.2f} objednavky.")

    action_lines = ["ODPORUCANIE NA DALSIE DNI"]
    if aov_7_change is not None and aov_7_change < -8:
        action_lines.append(
            "- Priorita je zvysit kosik: bundle ponuky, doplnky do kosika, upsell po vlozeni do kosika a jasne prahy pre dopravu zdarma."
        )
    if (roas_7_change is not None and roas_7_change < -15) or (cac_7_change is not None and cac_7_change > 15):
        action_lines.append(
            "- Nescalovat reklamu naslepo. Najprv pozriet kampane s najslabsim ROAS/CAC a obmedzit tie, ktore neprinasaju dost hodnoty."
        )
    if ret_30_change is not None and ret_30_change < -8:
        action_lines.append(
            "- Posilnit retenciu: email/SMS flow na druhy nakup, remarketing na nedavne objednavky a pracu s top produktmi po prvej objednavke."
        )
    if revenue_daily_change is not None and abs(revenue_daily_change) > 25:
        action_lines.append(
            f"- Posledny den spravil vykyv {_format_change(revenue_daily_change)}. Oplati sa skontrolovat, ci za tym nie je promo, vypinanie kampani alebo posun v trackingu."
        )
    if len(action_lines) == 1:
        action_lines.append("- Pokracovat bez vacsej zmeny, len sledovat dalsich 7 dni, ci sa trend potvrdi alebo otoci.")

    context_lines = [
        "POZNAMKA K DATAM",
        "- Tento text sa pocita priamo z aktualneho aggregate_by_date a export CSV z danej behovej sady, nie zo starej sablony summary.",
        "- Obrat aj nakupne ceny su v tomto reportingu bez DPH.",
    ]
    if w30["cac"] is None:
        context_lines.append("- CAC v casti porovnani moze byt miestami prazdne, ak v danom okne nebolo dost novych zakaznikov.")

    sections = [
        "\n".join(overview_lines),
        "\n".join(good_lines),
        "\n".join(weaker_lines),
        "\n".join(insight_lines),
        "\n".join(action_lines),
        "\n".join(context_lines),
    ]
    inventory_alert_summary = _build_roy_inventory_alert_summary(file_paths)
    if inventory_alert_summary:
        sections.append(inventory_alert_summary)
    return "\n\n".join(sections)


def build_email_body(
    from_date: str,
    to_date: str,
    summary_text: str,
    reporting_defaults: Dict[str, Any],
    data_quality: Optional[Dict[str, Any]] = None,
) -> str:
    display_name = reporting_defaults["display_name"]
    reporting_system_name = reporting_defaults["reporting_system_name"]
    dq = data_quality or {}
    overall_status = str(dq.get("overall_status") or "unknown").upper()
    qa_status = str(dq.get("qa_status") or "unknown").upper()
    qa_failure_count = int(dq.get("qa_failure_count") or 0)
    qa_warning_count = int(dq.get("qa_warning_count") or 0)
    is_partial = bool(dq.get("is_partial"))

    lines = [
        "Dobry den,",
        "",
        f"v prilohe posielam denny {display_name} report v HTML formate.",
        f"Sledovane obdobie: {from_date} az {to_date}.",
        "",
        f"Stav dat: {overall_status} / QA {qa_status}",
    ]

    if is_partial or qa_failure_count > 0 or qa_warning_count > 0:
        lines.append(
            "Poznamka: report obsahuje datove upozornenia. "
            f"QA failures: {qa_failure_count}, QA warnings: {qa_warning_count}. "
            "Detail je priamo v reporte v sekciach Source health a QA."
        )
    else:
        lines.append("Data presli kontrolami bez kritickeho upozornenia.")

    lines.extend(
        [
            "",
            summary_text,
            "",
            f"Tento email bol odoslany automaticky zo systemu {reporting_system_name}.",
        ]
    )

    return "\n".join(lines)


def send_email_ses(
    subject: str,
    body_text: str,
    file_paths: Dict[str, Path],
    reporting_defaults: Dict[str, Any],
    extra_attachments: Optional[List[Path]] = None,
) -> str:
    region = os.getenv("AWS_REGION", "eu-central-1").strip()
    configuration_set = os.getenv("SES_CONFIGURATION_SET", reporting_defaults["ses_configuration_set"]).strip()
    source = os.getenv("REPORT_EMAIL_FROM", "").strip()
    to_raw = os.getenv("REPORT_EMAIL_TO", "").strip()

    if not source:
        raise ValueError("REPORT_EMAIL_FROM is required")
    if not to_raw:
        raise ValueError("REPORT_EMAIL_TO is required")

    destinations = [email.strip() for email in to_raw.split(",") if email.strip()]
    if not destinations:
        raise ValueError("REPORT_EMAIL_TO has no valid recipients")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = source
    msg["To"] = ", ".join(destinations)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="amazonses.com")
    msg["Reply-To"] = source

    # Keep a short non-empty body to reduce spam score.
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    def attach_file(path: Path) -> None:
        if path.suffix.lower() in {".html", ".htm"}:
            text = path.read_text(encoding="utf-8", errors="replace")
            part = MIMEText(text, "html", "utf-8")
        else:
            with path.open("rb") as f:
                part = MIMEApplication(f.read(), Name=path.name)
        part["Content-Disposition"] = f'attachment; filename="{path.name}"'
        msg.attach(part)

    if not file_paths["report_html"].exists():
        raise FileNotFoundError(f"Missing HTML report attachment: {file_paths['report_html']}")
    attach_file(file_paths["report_html"])
    for extra in extra_attachments or []:
        if not extra.exists():
            raise FileNotFoundError(f"Missing extra attachment: {extra}")
        attach_file(extra)

    try:
        import boto3  # type: ignore
        from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
    except ImportError as exc:
        raise RuntimeError("boto3 is required for SES email sending. Install dependencies from requirements.txt.") from exc

    ses = boto3.client("ses", region_name=region)
    try:
        send_args: Dict[str, Any] = {
            "Source": source,
            "Destinations": destinations,
            "RawMessage": {"Data": msg.as_string()},
        }
        if configuration_set:
            send_args["ConfigurationSetName"] = configuration_set

        response = ses.send_raw_email(**send_args)
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(f"SES send failed: {exc}") from exc

    return response.get("MessageId", "")


def main() -> None:
    # Load root env first.
    load_dotenv(encoding="utf-8-sig")
    # Then load project env selected via CLI (or REPORT_PROJECT) before full arg parsing.
    bootstrap_project = bootstrap_project_from_argv(sys.argv[1:])
    os.environ["REPORT_PROJECT"] = bootstrap_project
    load_project_env(bootstrap_project)

    args = parse_args()
    project = (args.project or bootstrap_project).strip() or DEFAULT_PROJECT
    output_tag = sanitize_output_tag(args.output_tag)
    os.environ["REPORT_PROJECT"] = project
    os.environ["REPORT_DATA_DIR"] = str(project_data_dir(project).resolve())
    os.environ["REPORT_OUTPUT_TAG"] = output_tag
    reporting_defaults = resolve_reporting_defaults(project, load_project_settings(project))

    to_date = normalize_date(resolve_to_date(args.to_date, args.timezone))
    from_date = normalize_date(args.from_date)

    if from_date > to_date:
        raise ValueError(f"from_date ({from_date}) cannot be after to_date ({to_date})")

    use_clear_cache = args.clear_cache or env_bool("REPORT_FORCE_CLEAR_CACHE", False)
    use_no_cache = args.no_cache or env_bool("REPORT_FORCE_NO_CACHE", False)

    if not args.skip_export:
        run_export(
            project=project,
            from_date=from_date,
            to_date=to_date,
            clear_cache=use_clear_cache,
            no_cache=use_no_cache,
            output_tag=output_tag,
        )

    artifact_set = build_artifact_set(project, from_date, to_date, output_tag=output_tag)
    output_paths = artifact_set.as_dict()
    missing = [
        str(path)
        for path in artifact_set.required_daily_runner_outputs().values()
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError(f"Expected output files not found: {missing}")

    s3_upload_outputs(project, output_paths)
    data_quality = load_data_quality(output_paths.get("data_quality_json"))
    put_metric("ReportQaWarnings", int(data_quality.get("qa_warning_count") or 0), project, reporting_defaults)
    put_metric("ReportQaFailures", int(data_quality.get("qa_failure_count") or 0), project, reporting_defaults)
    put_metric(
        "ReportQaCritical",
        1 if str(data_quality.get("qa_status") or "").lower() == "critical" else 0,
        project,
        reporting_defaults,
    )
    put_metric(
        "ReportPartialData",
        1 if bool(data_quality.get("is_partial")) else 0,
        project,
        reporting_defaults,
    )

    if args.skip_email:
        print("Email sending skipped by flag.")
    else:
        subject = build_email_subject(reporting_defaults)
        summary_text = build_report_summary(output_paths)
        body_text = build_email_body(from_date, to_date, summary_text, reporting_defaults, data_quality)
        message_id = send_email_ses(
            subject=subject,
            body_text=body_text,
            file_paths=output_paths,
            reporting_defaults=reporting_defaults,
        )
        put_metric("ReportEmailSent", 1, project, reporting_defaults)
        print(f"SES message sent. MessageId={message_id}")

    if args.skip_invoices:
        print("Invoice generation skipped by flag.")
    else:
        invoice_summary = maybe_run_invoice_automation(
            project=project,
            report_to_date=to_date,
            reporting_defaults=reporting_defaults,
            dry_run=args.invoice_dry_run,
        )
        if invoice_summary:
            print(
                "Invoice automation summary: "
                f"matched={invoice_summary['matched_orders']} "
                f"created={invoice_summary['created_invoices']} "
                f"failed={invoice_summary['failed_invoices']} "
                f"skipped_zero_total={invoice_summary['skipped_zero_total_orders']}"
            )

    put_metric("ReportRunSucceeded", 1, project, reporting_defaults)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        failed_project = os.getenv("REPORT_PROJECT", DEFAULT_PROJECT)
        put_metric(
            "ReportRunFailed",
            1,
            failed_project,
            resolve_reporting_defaults(failed_project, load_project_settings(failed_project)),
        )
        raise
