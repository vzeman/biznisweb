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
from datetime import date, datetime, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from reporting_core import (
    BASE_DEFAULT_PROJECT,
    build_artifact_set,
    load_project_env,
    load_project_settings,
    project_data_dir,
    resolve_reporting_defaults,
    sanitize_output_tag,
)


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_PROJECT = os.getenv("REPORT_PROJECT", BASE_DEFAULT_PROJECT).strip() or BASE_DEFAULT_PROJECT
CFO_FIXED_DAILY_COST_EUR = float(os.getenv("CFO_FIXED_DAILY_COST_EUR", "70"))


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


def parse_args() -> argparse.Namespace:
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
        "--output-tag",
        default=os.getenv("REPORT_OUTPUT_TAG", ""),
        help="Optional output tag for side-by-side test artifacts (e.g. ui_test).",
    )
    return parser.parse_args()


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
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

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
            shipping_subsidy = _to_float(row.get("shipping_subsidy_cost", ""))
            facebook_ads = _to_float(row.get("fb_ads_spend", ""))
            google_ads = _to_float(row.get("google_ads_spend", ""))
            total_ads = facebook_ads + google_ads
            profit = _to_float(row.get("net_profit", ""))
            pre_ad_contribution = _to_float(row.get("pre_ad_contribution_profit", ""))
            contribution_margin_percent = _to_float(row.get("pre_ad_contribution_margin_pct", ""))

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
                    "facebook_ads": facebook_ads,
                    "google_ads": google_ads,
                    "total_ads": total_ads,
                    "profit": profit,
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
            orders_map[order_num] = {"date": d, "email": email}
    return list(orders_map.values())


def _load_customer_dynamics(order_records: List[Dict[str, Any]]) -> Dict[date, Dict[str, int]]:
    if not order_records:
        return {}

    first_order_date: Dict[str, date] = {}
    for order in order_records:
        email = order["email"]
        d = order["date"]
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
    company_profit_with_fixed = profit - (CFO_FIXED_DAILY_COST_EUR * days)
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

    anomalies: List[str] = []

    def add_anomaly(metric_name: str, change: Optional[float], threshold: float, extra: str = "") -> None:
        if change is None:
            return
        if abs(change) > threshold:
            direction = "narastol" if change > 0 else "klesol"
            anomalies.append(f"- {metric_name} {direction} o {_fmt_pct(abs(change))}. {extra}".strip())

    add_anomaly("Revenue (last day vs previous day)", revenue_daily_change, 20, "Signal je nad anomaly prahom 20%.")
    add_anomaly("Orders (last day vs previous day)", orders_daily_change, 20, "Signal je nad anomaly prahom 20%.")
    add_anomaly("AOV (last day vs previous day)", aov_daily_change, 10, "Signal je nad anomaly prahom 10%.")
    add_anomaly("CAC (7d vs previous 7d)", cac_7_change, 15, "Rast CAC nad 15% zhorsuje akvizicnu efektivitu.")
    add_anomaly("ROAS (7d vs previous 7d)", roas_7_change, 20, "Pokles ROAS nad 20% znizuje navratnost reklamy.")
    if cm_daily_change_pp is not None and abs(cm_daily_change_pp) > 5:
        direction = "narastla" if cm_daily_change_pp > 0 else "klesla"
        anomalies.append(f"- Contribution margin (daily) {direction} o {abs(cm_daily_change_pp):.2f} p.b., co je nad prahom 5 p.b.")

    positive_signals: List[str] = []
    if (w7["roas"] or 0) > 2.5:
        positive_signals.append(f"- 7d ROAS je {w7['roas']:.2f}x, co podporuje financnu udrzatelnost platenych kanalov.")
    if (w7["profit"] or 0) > 0:
        positive_signals.append(f"- Poslednych 7 dni je netto zisk {_fmt_eur(float(w7['profit'] or 0))}.")
    if (w30["contribution_margin"] or 0) > 40:
        positive_signals.append(f"- 30d pre-ad contribution margin je {_fmt_pct(float(w30['contribution_margin'] or 0))}, co vytvara priestor pre akviziciu.")
    if (w30["returning_customer_rate"] or 0) > 20:
        positive_signals.append(f"- Podiel returning objednavok za 30 dni je {_fmt_pct(float(w30['returning_customer_rate'] or 0))}, co zlepsuje kvalitu revenue.")
    if not positive_signals:
        positive_signals.append("- V analyzovanom horizonte nevidim silny pozitivny signal nad beznym sumom.")

    cfo_warnings: List[str] = []
    if w30["cac"] is not None and (w30["contribution_per_order"] or 0) > 0:
        if float(w30["cac"]) >= float(w30["contribution_per_order"] or 0):
            cfo_warnings.append(
                f"- CAC {_fmt_eur(float(w30['cac']))} je >= contribution/order {_fmt_eur(float(w30['contribution_per_order'] or 0))}. "
                "Skalovanie paid traffic je rizikove."
            )
    if w30["payback_orders"] is not None and float(w30["payback_orders"]) > 1:
        cfo_warnings.append(f"- Payback period je {float(w30['payback_orders']):.2f} objednavky (>1), navratnost je pomalsia.")
    if (w30["profit"] or 0) < 0:
        cfo_warnings.append("- Poslednych 30 dni je netto strata, treba znizit spend alebo zvysit contribution/order.")
    if (w7["ads"] or 0) > 0 and (w7_prev["ads"] or 0) > 0:
        ads_change_7 = _pct_change(float(w7["ads"] or 0), float(w7_prev["ads"] or 0))
        rev_change_7 = _pct_change(float(w7["revenue"] or 0), float(w7_prev["revenue"] or 0))
        if ads_change_7 is not None and rev_change_7 is not None and ads_change_7 > 20 and rev_change_7 < 5:
            cfo_warnings.append("- Rast trzieb je zavisly od rastuceho ad spendu bez adekvatneho revenue multiplikatora.")
    if not cfo_warnings:
        cfo_warnings.append("- Kriticky warning signal nebol detegovany pri aktualnych thresholdoch.")

    if orders_7_change is not None and aov_7_change is not None:
        if abs(orders_7_change) >= abs(aov_7_change):
            revenue_driver = (
                f"Rast/pokles revenue je primarne tahany objemom objednavok ({_format_change(orders_7_change)}) "
                f"viac ako AOV ({_format_change(aov_7_change)})."
            )
        else:
            revenue_driver = (
                f"Rast/pokles revenue je primarne tahany AOV ({_format_change(aov_7_change)}) "
                f"viac ako poctom objednavok ({_format_change(orders_7_change)})."
            )
    else:
        revenue_driver = "Nedostatok dat pre attribution revenue quality."

    trend_lines = [
        f"- Revenue: {_classify_trend(revenue_daily_change, revenue_7_change, revenue_30_change)} (D {_format_change(revenue_daily_change)}, 7D {_format_change(revenue_7_change)}, 30D {_format_change(revenue_30_change)})",
        f"- Orders: {_classify_trend(orders_daily_change, orders_7_change, orders_30_change)} (D {_format_change(orders_daily_change)}, 7D {_format_change(orders_7_change)}, 30D {_format_change(orders_30_change)})",
        f"- AOV: {_classify_trend(aov_daily_change, aov_7_change, aov_30_change)} (D {_format_change(aov_daily_change)}, 7D {_format_change(aov_7_change)}, 30D {_format_change(aov_30_change)})",
        f"- CAC: {_classify_trend(cac_daily_change, cac_7_change, cac_30_change, lower_is_better=True)} (D {_format_change(cac_daily_change)}, 7D {_format_change(cac_7_change)}, 30D {_format_change(cac_30_change)})",
        f"- ROAS: {_classify_trend(roas_daily_change, roas_7_change, roas_30_change)} (D {_format_change(roas_daily_change)}, 7D {_format_change(roas_7_change)}, 30D {_format_change(roas_30_change)})",
        f"- Contribution Margin: {_classify_trend(cm_daily_change_pp, cm_7_change, cm_30_change)} (D {('+' if (cm_daily_change_pp or 0) >= 0 else '')}{(cm_daily_change_pp or 0):.2f} p.b., 7D {_format_change(cm_7_change)}, 30D {_format_change(cm_30_change)})",
        f"- Profit: {_classify_trend(profit_daily_change, profit_7_change, profit_30_change)} (D {_format_change(profit_daily_change)}, 7D {_format_change(profit_7_change)}, 30D {_format_change(profit_30_change)})",
    ]

    today_new = int(customer_by_date.get(last_date, {}).get("new_customers", 0))
    daily_cac = (float(last_row["total_ads"]) / today_new) if today_new > 0 else None
    daily_payback = (daily_cac / float(last_row["contribution_per_order"])) if (daily_cac is not None and float(last_row["contribution_per_order"]) > 0) else None

    def has_window_data(end_date: date, days: int) -> bool:
        for i in range(days):
            d = end_date - timedelta(days=(days - 1 - i))
            if d in row_by_date:
                return True
        return False

    day_cur = _window_aggregate(row_by_date, last_date, 1, customer_by_date, order_records)
    day_prev_agg = _window_aggregate(row_by_date, prev_day, 1, customer_by_date, order_records) if d_prev else None
    day_week_agg = _window_aggregate(row_by_date, same_weekday_last_week, 1, customer_by_date, order_records) if d_week else None
    day_month_agg = _window_aggregate(row_by_date, same_day_last_month, 1, customer_by_date, order_records) if d_month else None
    day_year_agg = _window_aggregate(row_by_date, same_day_last_year, 1, customer_by_date, order_records) if d_year else None

    w7_month_cmp = w7_month if has_window_data(_shift_months(last_date, -1), 7) else None
    w7_year_cmp = w7_year if has_window_data(_shift_years(last_date, -1), 7) else None
    w30_year_cmp = w30_year if has_window_data(_shift_years(last_date, -1), 30) else None

    metric_defs = [
        ("Revenue", "revenue"),
        ("Orders", "orders"),
        ("AOV", "aov"),
        ("CAC", "cac"),
        ("ROAS", "roas"),
        ("Pre-Ad Contribution Margin", "pre_ad_contribution_margin"),
        ("Post-Ad Margin", "post_ad_margin"),
        ("Company Margin (incl. fixed)", "company_margin_with_fixed"),
        ("Profit", "profit"),
        ("LTV", "ltv"),
    ]

    comparisons = ["Daily comparisons:"]
    for metric_name, metric_key in metric_defs:
        comparisons.append(
            _comparison_line_optional(
                f"Last day vs previous day ({metric_name})",
                day_cur.get(metric_key),
                day_prev_agg.get(metric_key) if day_prev_agg else None,
            )
        )
        comparisons.append(
            _comparison_line_optional(
                f"Last day vs same weekday last week ({metric_name})",
                day_cur.get(metric_key),
                day_week_agg.get(metric_key) if day_week_agg else None,
            )
        )
        comparisons.append(
            _comparison_line_optional(
                f"Last day vs same day last month ({metric_name})",
                day_cur.get(metric_key),
                day_month_agg.get(metric_key) if day_month_agg else None,
            )
        )
        comparisons.append(
            _comparison_line_optional(
                f"Last day vs same day last year ({metric_name})",
                day_cur.get(metric_key),
                day_year_agg.get(metric_key) if day_year_agg else None,
            )
        )

    comparisons.append("Weekly comparisons:")
    for metric_name, metric_key in metric_defs:
        comparisons.append(
            _comparison_line_optional(
                f"Last 7 days vs previous 7 days ({metric_name})",
                w7.get(metric_key),
                w7_prev.get(metric_key),
            )
        )
        comparisons.append(
            _comparison_line_optional(
                f"Last 7 days vs same week last month ({metric_name})",
                w7.get(metric_key),
                w7_month_cmp.get(metric_key) if w7_month_cmp else None,
            )
        )
        comparisons.append(
            _comparison_line_optional(
                f"Last 7 days vs same week last year ({metric_name})",
                w7.get(metric_key),
                w7_year_cmp.get(metric_key) if w7_year_cmp else None,
            )
        )

    comparisons.append("Monthly comparisons:")
    for metric_name, metric_key in metric_defs:
        comparisons.append(
            _comparison_line_optional(
                f"Last 30 days vs previous 30 days ({metric_name})",
                w30.get(metric_key),
                w30_prev.get(metric_key),
            )
        )
        comparisons.append(
            _comparison_line_optional(
                f"Last 30 days vs same month last year ({metric_name})",
                w30.get(metric_key),
                w30_year_cmp.get(metric_key) if w30_year_cmp else None,
            )
        )

    exec_summary = (
        "EXECUTIVE SUMMARY\n"
        f"Biznis v sledovanom obdobi generoval {int(total['orders'] or 0)} objednavok, revenue {_fmt_eur(float(total['revenue'] or 0))} "
        f"a netto profit {_fmt_eur(float(total['profit'] or 0))}. "
        f"Poslednych 7 dni dosiahli revenue {_fmt_eur(float(w7['revenue'] or 0))} a profit {_fmt_eur(float(w7['profit'] or 0))}, "
        f"co znamena zmenu vs predchadzajucich 7 dni: revenue {_format_change(revenue_7_change)}, profit {_format_change(profit_7_change)}. "
        f"Marketingova efektivita je na 7d ROAS {float(w7['roas'] or 0):.2f}x a CAC "
        f"{_fmt_eur(float(w7['cac'])) if w7['cac'] is not None else 'N/A'}, "
        f"pri contribution/order {_fmt_eur(float(w7['contribution_per_order'] or 0))}. "
        f"Trend returning order rate je 7D {_format_change(ret_7_change)} a 30D {_format_change(ret_30_change)}."
    )

    positive_section = "POSITIVE SIGNALS\n" + "\n".join(positive_signals)
    risk_section = "RISKS & ANOMALIES\n" + ("\n".join(anomalies) if anomalies else "- Nad definovanymi anomaly thresholdmi sa nenasiel kriticky vykyv.")

    key_insights_lines = [
        "KEY INSIGHTS",
        f"- Revenue quality: {revenue_driver}",
        f"- Unit economics: contribution/order (pre-ads) {_fmt_eur(float(w30['contribution_per_order'] or 0))}, contribution po ads/profit per order {_fmt_eur(float(w30['profit_per_order'] or 0))}.",
    ]
    if w30["payback_orders"] is not None:
        daily_part = f", daily estimate {float(daily_payback):.2f} objednavky" if daily_payback is not None else ""
        key_insights_lines.append(f"- Payback: {float(w30['payback_orders']):.2f} objednavky (30D){daily_part}.")
    else:
        key_insights_lines.append("- Payback: Nedostatok dat pre vypocet.")
    key_insights = "\n".join(key_insights_lines)

    trends_section = "METRIC TRENDS\n" + "\n".join(trend_lines)
    cfo_warning_section = "CFO WARNING SIGNALS\n" + "\n".join(cfo_warnings)
    comparison_section = "COMPARISON FRAMEWORK\n" + "\n".join(comparisons)

    data_gaps = [
        "DATA LIMITS",
        "- Refund rate a cancellation rate momentalne nie su v dostupnom datasete, preto nie su vyhodnotene.",
    ]
    if w30["cac"] is None:
        data_gaps.append("- CAC pre 30D nemoze byt spolahlivo vyhodnotene, lebo chyba dostatok new customer datapointov.")
    gaps_section = "\n".join(data_gaps)

    return "\n\n".join([
        exec_summary,
        positive_section,
        risk_section,
        key_insights,
        trends_section,
        cfo_warning_section,
        comparison_section,
        gaps_section,
    ])


def build_email_body(from_date: str, to_date: str, summary_text: str, reporting_defaults: Dict[str, Any]) -> str:
    display_name = reporting_defaults["display_name"]
    reporting_system_name = reporting_defaults["reporting_system_name"]
    return (
        "Dobry den,\n\n"
        f"v prilohe posielam denny {display_name} report v HTML formate.\n"
        f"Sledovane obdobie: {from_date} az {to_date}.\n\n"
        f"{summary_text}\n\n"
        f"Tento email bol odoslany automaticky zo systemu {reporting_system_name}.\n"
    )


def put_metric(metric_name: str, value: float, project: str, reporting_defaults: Dict[str, Any], unit: str = "Count") -> None:
    """Publish a custom CloudWatch metric for reporting job observability."""
    region = os.getenv("AWS_REGION", "eu-central-1").strip()
    try:
        import boto3  # type: ignore
        from botocore.exceptions import BotoCoreError, ClientError  # type: ignore
    except ImportError:
        print("WARN: boto3 not available, skipping CloudWatch metric publishing.")
        return

    try:
        cloudwatch = boto3.client("cloudwatch", region_name=region)
        cloudwatch.put_metric_data(
            Namespace=reporting_defaults["cloudwatch_namespace"],
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": [{"Name": "Project", "Value": project}],
                    "Value": float(value),
                    "Unit": unit,
                }
            ],
        )
    except (ClientError, BotoCoreError) as exc:
        print(f"WARN: failed to publish CloudWatch metric {metric_name}: {exc}")


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

    if args.skip_email:
        print("Email sending skipped by flag.")
        return

    subject = build_email_subject(reporting_defaults)
    summary_text = build_report_summary(output_paths)
    body_text = build_email_body(from_date, to_date, summary_text, reporting_defaults)
    message_id = send_email_ses(
        subject=subject,
        body_text=body_text,
        file_paths=output_paths,
        reporting_defaults=reporting_defaults,
    )
    put_metric("ReportEmailSent", 1, project, reporting_defaults)
    put_metric("ReportRunSucceeded", 1, project, reporting_defaults)
    print(f"SES message sent. MessageId={message_id}")


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
