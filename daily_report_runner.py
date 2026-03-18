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
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent
ROOT_DATA_DIR = ROOT_DIR / "data"
PROJECTS_DIR = ROOT_DIR / "projects"
DEFAULT_PROJECT = os.getenv("REPORT_PROJECT", "vevo").strip() or "vevo"
CFO_FIXED_DAILY_COST_EUR = float(os.getenv("CFO_FIXED_DAILY_COST_EUR", "70"))


def project_data_dir(project: str) -> Path:
    # Backward compatibility: default project historically writes to data/ (no subfolder).
    if project == DEFAULT_PROJECT:
        data_dir = ROOT_DATA_DIR
    else:
        data_dir = ROOT_DATA_DIR / project
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def bootstrap_project_from_argv(argv: List[str]) -> str:
    for idx, arg in enumerate(argv):
        if arg == "--project" and idx + 1 < len(argv):
            return argv[idx + 1].strip() or DEFAULT_PROJECT
        if arg.startswith("--project="):
            return arg.split("=", 1)[1].strip() or DEFAULT_PROJECT
    return DEFAULT_PROJECT


def load_project_env(project: str) -> None:
    env_path = PROJECTS_DIR / project / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        print(f"Loaded project env: {env_path}")
    elif project != DEFAULT_PROJECT:
        raise FileNotFoundError(
            f"Project env file not found: {env_path}. "
            f"Create projects/{project}/.env from projects/{project}/.env.example."
        )


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


def run_export(project: str, from_date: str, to_date: str, clear_cache: bool, no_cache: bool) -> None:
    cmd: List[str] = [
        sys.executable,
        str(ROOT_DIR / "export_orders.py"),
        "--from-date",
        from_date,
        "--to-date",
        to_date,
    ]
    # Keep compatibility with older export_orders.py that has no --project arg.
    if project != DEFAULT_PROJECT:
        cmd[2:2] = ["--project", project]
    if clear_cache:
        cmd.append("--clear-cache")
    if no_cache:
        cmd.append("--no-cache")

    print("Running export:", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(ROOT_DIR), check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"export_orders.py failed with exit code {proc.returncode}")


def get_output_paths(project: str, from_date: str, to_date: str) -> Dict[str, Path]:
    compact_range = f"{from_date.replace('-', '')}-{to_date.replace('-', '')}"
    data_dir = project_data_dir(project)
    return {
        "report_html": data_dir / f"report_{compact_range}.html",
        "email_strategy_html": data_dir / f"email_strategy_{compact_range}.html",
        "export_csv": data_dir / f"export_{compact_range}.csv",
        "aggregate_by_date_csv": data_dir / f"aggregate_by_date_{compact_range}.csv",
        "aggregate_by_month_csv": data_dir / f"aggregate_by_month_{compact_range}.csv",
    }


def get_cfo_graph_path(project: str, from_date: str, to_date: str) -> Path:
    compact_range = f"{from_date.replace('-', '')}-{to_date.replace('-', '')}"
    return project_data_dir(project) / f"cfo_graphs_{compact_range}.html"


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


def build_email_subject(project: str) -> str:
    return os.getenv("REPORT_EMAIL_SUBJECT", f"Daily {project} report").strip()


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


def generate_cfo_graph_html(project: str, file_paths: Dict[str, Path], from_date: str, to_date: str) -> Path:
    date_csv = file_paths.get("aggregate_by_date_csv")
    if not date_csv or not date_csv.exists():
        raise FileNotFoundError("Cannot build CFO graph HTML, missing aggregate_by_date CSV.")

    daily_rows = _load_daily_rows(date_csv)
    if not daily_rows:
        raise ValueError("Cannot build CFO graph HTML, aggregate_by_date CSV is empty.")

    export_csv = file_paths.get("export_csv")
    order_records = _load_order_records(export_csv)
    customer_by_date = _load_customer_dynamics(order_records)
    row_by_date: Dict[date, Dict[str, Any]] = {row["date"]: row for row in daily_rows}

    last_date = daily_rows[-1]["date"]
    prev_day = last_date - timedelta(days=1)
    same_weekday_last_week = last_date - timedelta(days=7)
    same_day_last_month = last_date - timedelta(days=30)
    same_day_last_year = last_date - timedelta(days=365)

    weekly_prev_end = last_date - timedelta(days=7)
    weekly_last_month_end = last_date - timedelta(days=30)
    weekly_last_year_end = last_date - timedelta(days=365)

    monthly_prev_end = last_date - timedelta(days=30)
    monthly_last_year_end = last_date - timedelta(days=365)

    def _has_window_data(end_date: date, days: int) -> bool:
        for i in range(days):
            d = end_date - timedelta(days=(days - 1 - i))
            if d in row_by_date:
                return True
        return False

    day_cur = _window_aggregate(row_by_date, last_date, 1, customer_by_date, order_records)
    day_prev = _window_aggregate(row_by_date, prev_day, 1, customer_by_date, order_records) if prev_day in row_by_date else None
    day_week = _window_aggregate(row_by_date, same_weekday_last_week, 1, customer_by_date, order_records) if same_weekday_last_week in row_by_date else None
    day_month = _window_aggregate(row_by_date, same_day_last_month, 1, customer_by_date, order_records) if same_day_last_month in row_by_date else None
    day_year = _window_aggregate(row_by_date, same_day_last_year, 1, customer_by_date, order_records) if same_day_last_year in row_by_date else None

    w7 = _window_aggregate(row_by_date, last_date, 7, customer_by_date, order_records)
    w7_prev = _window_aggregate(row_by_date, weekly_prev_end, 7, customer_by_date, order_records) if _has_window_data(weekly_prev_end, 7) else None
    w7_month = _window_aggregate(row_by_date, weekly_last_month_end, 7, customer_by_date, order_records) if _has_window_data(weekly_last_month_end, 7) else None
    w7_year = _window_aggregate(row_by_date, weekly_last_year_end, 7, customer_by_date, order_records) if _has_window_data(weekly_last_year_end, 7) else None

    w30 = _window_aggregate(row_by_date, last_date, 30, customer_by_date, order_records)
    w30_prev = _window_aggregate(row_by_date, monthly_prev_end, 30, customer_by_date, order_records) if _has_window_data(monthly_prev_end, 30) else None
    w30_year = _window_aggregate(row_by_date, monthly_last_year_end, 30, customer_by_date, order_records) if _has_window_data(monthly_last_year_end, 30) else None

    metric_defs = [
        ("Revenue", "revenue"),
        ("Orders", "orders"),
        ("AOV", "aov"),
        ("CAC", "cac"),
        ("ROAS", "roas"),
        ("Pre-Ad Contribution Margin", "pre_ad_contribution_margin"),
        ("Post-Ad Margin", "post_ad_margin"),
        ("Profit", "profit"),
        ("LTV", "ltv"),
    ]
    kpi_metric_defs = [
        {"key": "revenue", "label": "Revenue", "direction": "up"},
        {"key": "profit", "label": "Profit", "direction": "up"},
        {"key": "orders", "label": "Orders", "direction": "up"},
        {"key": "aov", "label": "AOV", "direction": "up"},
        {"key": "cac", "label": "CAC", "direction": "down"},
        {"key": "roas", "label": "ROAS", "direction": "up"},
        {"key": "pre_ad_contribution_margin", "label": "Pre-Ad Contribution Margin", "direction": "up"},
        {"key": "post_ad_margin", "label": "Post-Ad Margin", "direction": "up"},
        {"key": "company_margin_with_fixed", "label": f"Company Margin (incl. EUR {int(CFO_FIXED_DAILY_COST_EUR)}/day fixed)", "direction": "up"},
    ]
    all_metric_keys = list(dict.fromkeys(
        [metric_key for _, metric_key in metric_defs] +
        [m["key"] for m in kpi_metric_defs] +
        ["company_profit_with_fixed"]
    ))
    kpi_metric_keys = [m["key"] for m in kpi_metric_defs]

    def _safe_kpi_value(metric_key: str, aggregate: Optional[Dict[str, Optional[float]]], window_days: int) -> Optional[float]:
        if not aggregate:
            return None
        value = aggregate.get(metric_key)
        if value is None:
            return None

        if metric_key == "roas":
            return min(float(value), 15.0)

        if metric_key == "cac":
            ads_spend = float(aggregate.get("ads") or 0.0)
            if ads_spend <= 0:
                return None
            if aggregate.get("cac") is None:
                return None
            return float(aggregate["cac"])

        return float(value)

    def _snapshot(
        aggregate: Optional[Dict[str, Optional[float]]],
        window_days: int,
        metric_keys: List[str],
    ) -> Dict[str, Optional[float]]:
        return {metric_key: _safe_kpi_value(metric_key, aggregate, window_days) for metric_key in metric_keys}

    daily_labels: List[str] = []
    revenue_series: List[float] = []
    profit_series: List[float] = []
    orders_series: List[int] = []
    aov_series: List[float] = []
    cac_series: List[Optional[float]] = []
    roas_series: List[float] = []
    cm_series: List[float] = []
    ltv30_series: List[Optional[float]] = []
    pre_margin30_series: List[Optional[float]] = []
    post_margin30_series: List[Optional[float]] = []

    for row in daily_rows:
        d = row["date"]
        daily_labels.append(d.strftime("%Y-%m-%d"))
        revenue_series.append(round(float(row["revenue"]), 2))
        profit_series.append(round(float(row["profit"]), 2))
        orders_series.append(int(row["orders"]))
        aov_series.append(round(float(row["aov"]), 2))
        roas_series.append(round(min(float(row["roas"]), 15.0), 3))
        cm_series.append(round(float(row["contribution_margin_percent"]), 2))

        new_customers = int(customer_by_date.get(d, {}).get("new_customers", 0))
        day_cac = (float(row["total_ads"]) / new_customers) if new_customers > 0 else None
        cac_series.append(round(day_cac, 2) if day_cac is not None else None)

        rolling30 = _window_aggregate(row_by_date, d, 30, customer_by_date, order_records)
        ltv30 = rolling30.get("ltv")
        pre_m30 = rolling30.get("pre_ad_contribution_margin")
        post_m30 = rolling30.get("post_ad_margin")
        ltv30_series.append(round(float(ltv30), 2) if ltv30 is not None else None)
        pre_margin30_series.append(round(float(pre_m30), 2) if pre_m30 is not None else None)
        post_margin30_series.append(round(float(post_m30), 2) if post_m30 is not None else None)

    day_vals = _snapshot(day_cur, 1, all_metric_keys)
    day_prev_vals = _snapshot(day_prev, 1, all_metric_keys) if day_prev else {}
    day_week_vals = _snapshot(day_week, 1, all_metric_keys) if day_week else {}
    day_month_vals = _snapshot(day_month, 1, all_metric_keys) if day_month else {}
    day_year_vals = _snapshot(day_year, 1, all_metric_keys) if day_year else {}

    w7_vals = _snapshot(w7, 7, all_metric_keys)
    w7_prev_vals = _snapshot(w7_prev, 7, all_metric_keys) if w7_prev else {}
    w7_month_vals = _snapshot(w7_month, 7, all_metric_keys) if w7_month else {}
    w7_year_vals = _snapshot(w7_year, 7, all_metric_keys) if w7_year else {}

    w30_vals = _snapshot(w30, 30, all_metric_keys)
    w30_prev_vals = _snapshot(w30_prev, 30, all_metric_keys) if w30_prev else {}
    w30_year_vals = _snapshot(w30_year, 30, all_metric_keys) if w30_year else {}

    def _delta(current: Optional[float], reference: Optional[float]) -> Optional[float]:
        if current is None or reference is None:
            return None
        return _pct_change(float(current), float(reference))

    daily_comparison: Dict[str, Dict[str, Optional[float]]] = {}
    weekly_comparison: Dict[str, Dict[str, Optional[float]]] = {}
    monthly_comparison: Dict[str, Dict[str, Optional[float]]] = {}
    kpi_comparisons: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {
        "daily": {},
        "weekly": {},
        "monthly": {},
    }

    for metric_name, metric_key in metric_defs:
        day_current = day_vals.get(metric_key)
        day_prev_value = day_prev_vals.get(metric_key)
        day_week_value = day_week_vals.get(metric_key)
        day_month_value = day_month_vals.get(metric_key)
        day_year_value = day_year_vals.get(metric_key)

        daily_comparison[metric_name] = {
            "vs_prev_day": _delta(day_current, day_prev_value),
            "vs_week": _delta(day_current, day_week_value),
            "vs_month": _delta(day_current, day_month_value),
            "vs_year": _delta(day_current, day_year_value),
        }

        w7_current = w7_vals.get(metric_key)
        w7_previous = w7_prev_vals.get(metric_key)
        w7_month_value = w7_month_vals.get(metric_key)
        w7_year_value = w7_year_vals.get(metric_key)
        weekly_comparison[metric_name] = {
            "vs_prev_7d": _delta(w7_current, w7_previous),
            "vs_month": _delta(w7_current, w7_month_value),
            "vs_year": _delta(w7_current, w7_year_value),
        }

        w30_current = w30_vals.get(metric_key)
        w30_previous = w30_prev_vals.get(metric_key)
        w30_year_value = w30_year_vals.get(metric_key)
        monthly_comparison[metric_name] = {
            "vs_prev_30d": _delta(w30_current, w30_previous),
            "vs_year": _delta(w30_current, w30_year_value),
        }

        if metric_key in kpi_metric_keys:
            kpi_comparisons["daily"][metric_key] = daily_comparison[metric_name]
            kpi_comparisons["weekly"][metric_key] = weekly_comparison[metric_name]
            kpi_comparisons["monthly"][metric_key] = monthly_comparison[metric_name]

    anomaly_thresholds = {
        "Revenue": 20.0,
        "Orders": 20.0,
        "AOV": 10.0,
        "CAC": 15.0,
        "ROAS": 20.0,
        "Pre-Ad Contribution Margin": 5.0,
        "Post-Ad Margin": 5.0,
        "Company Margin (incl. fixed)": 5.0,
        "Profit": 20.0,
        "LTV": 20.0,
    }
    anomaly_current = []
    for metric_name, metric_changes in daily_comparison.items():
        current_change = metric_changes.get("vs_prev_day")
        threshold = anomaly_thresholds[metric_name]
        ratio = None
        if current_change is not None and threshold > 0:
            ratio = abs(current_change) / threshold
        anomaly_current.append({
            "metric": metric_name,
            "daily_change": round(float(current_change), 2) if current_change is not None else None,
            "threshold": threshold,
            "ratio": round(float(ratio), 3) if ratio is not None else None,
        })

    graph_payload = {
        "meta": {
            "from_date": from_date,
            "to_date": to_date,
            "generated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "series": {
            "labels": daily_labels,
            "revenue": revenue_series,
            "profit": profit_series,
            "orders": orders_series,
            "aov": aov_series,
            "cac": cac_series,
            "roas": roas_series,
            "contribution_margin": cm_series,
            "ltv30": ltv30_series,
            "pre_margin30": pre_margin30_series,
            "post_margin30": post_margin30_series,
        },
        "windows": {
            "w7": {k: (round(float(v), 4) if v is not None else None) for k, v in w7.items()},
            "w30": {k: (round(float(v), 4) if v is not None else None) for k, v in w30.items()},
        },
        "kpi": {
            "default_window": "monthly",
            "metric_defs": kpi_metric_defs,
            "windows": {
                "daily": {
                    "label": "Last day",
                    "metrics": {
                        **{k: day_vals.get(k) for k in kpi_metric_keys},
                        "company_profit_with_fixed": day_vals.get("company_profit_with_fixed"),
                    },
                },
                "weekly": {
                    "label": "Last 7 days",
                    "metrics": {
                        **{k: w7_vals.get(k) for k in kpi_metric_keys},
                        "company_profit_with_fixed": w7_vals.get("company_profit_with_fixed"),
                    },
                },
                "monthly": {
                    "label": "Last 30 days",
                    "metrics": {
                        **{k: w30_vals.get(k) for k in kpi_metric_keys},
                        "company_profit_with_fixed": w30_vals.get("company_profit_with_fixed"),
                    },
                },
            },
            "comparisons": kpi_comparisons,
        },
        "comparisons": {
            "daily": daily_comparison,
            "weekly": weekly_comparison,
            "monthly": monthly_comparison,
        },
        "anomalies": anomaly_current,
    }

    payload_json = json.dumps(graph_payload, ensure_ascii=False)
    html = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CFO Analytics Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {
      --revenue: #2563EB;
      --profit: #10B981;
      --orders: #7C3AED;
      --aov: #EC4899;
      --cac: #EF4444;
      --roas: #F59E0B;
      --margin: #06B6D4;
      --ltv: #8B5CF6;
      --bg: #F8FAFC;
      --card: #FFFFFF;
      --border: #E5E7EB;
      --ink: #0F172A;
      --muted: #64748B;
    }
    body {
      margin: 0;
      padding: 28px;
      background: var(--bg);
      color: var(--ink);
      font-family: "IBM Plex Sans", "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }
    .container {
      max-width: 1600px;
      margin: 0 auto;
    }
    .header {
      margin-bottom: 20px;
    }
    .header h1 {
      margin: 0;
      font-size: 30px;
      letter-spacing: -0.02em;
    }
    .header p {
      margin: 6px 0 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .section-title {
      margin: 24px 0 10px;
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(12, minmax(0, 1fr));
      gap: 12px;
    }
    .kpi-window-switch {
      display: inline-flex;
      gap: 6px;
      margin-bottom: 12px;
      background: #F1F5F9;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 4px;
    }
    .kpi-window-btn {
      border: 0;
      background: transparent;
      color: #475569;
      font-size: 12px;
      font-weight: 600;
      border-radius: 8px;
      padding: 6px 10px;
      cursor: pointer;
    }
    .kpi-window-btn.active {
      background: #FFFFFF;
      color: #0F172A;
      box-shadow: 0 1px 2px rgba(15, 23, 42, 0.12);
    }
    .kpi-card {
      grid-column: span 12;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 16px;
    }
    .kpi-title {
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 8px;
    }
    .kpi-value {
      font-size: 28px;
      font-weight: 700;
      letter-spacing: -0.02em;
      line-height: 1.1;
    }
    .kpi-period {
      font-size: 11px;
      color: var(--muted);
      margin-top: 6px;
      margin-bottom: 8px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .kpi-comparisons {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .kpi-cmp-row {
      font-size: 12px;
      line-height: 1.25;
      color: #64748B;
    }
    .kpi-cmp-row .delta {
      font-weight: 700;
      margin-right: 4px;
    }
    .tone-good { color: #10B981; }
    .tone-bad { color: #EF4444; }
    .tone-neutral { color: #64748B; }
    .kpi-cmp-split {
      display: inline-flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .chart-card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 16px 18px 12px;
      margin-bottom: 16px;
      box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
    }
    .chart-title {
      margin: 0 0 4px 0;
      font-size: 18px;
      letter-spacing: -0.01em;
    }
    .chart-subtitle {
      margin: 0 0 10px 0;
      font-size: 13px;
      color: var(--muted);
    }
    .chart-wrap {
      width: 100%;
    }
    .h420 { height: 420px; }
    .h320 { height: 320px; }
    .h260 { height: 260px; }
    @media (min-width: 920px) {
      .kpi-card { grid-column: span 4; }
    }
    @media (min-width: 1240px) {
      .kpi-grid { grid-template-columns: repeat(14, minmax(0, 1fr)); }
      .kpi-card { grid-column: span 2; }
    }
    @media (max-width: 780px) {
      body { padding: 14px; }
      .kpi-value { font-size: 24px; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>CFO Executive Dashboard</h1>
      <p id="meta"></p>
    </div>

    <div class="section-title">Core KPIs</div>
    <div class="kpi-window-switch" id="kpiWindowSwitch">
      <button class="kpi-window-btn" data-window="daily">Daily</button>
      <button class="kpi-window-btn" data-window="weekly">Weekly</button>
      <button class="kpi-window-btn" data-window="monthly">Monthly</button>
    </div>
    <div class="kpi-grid">
      <div class="kpi-card" data-metric="revenue"><div class="kpi-title">Revenue</div><div class="kpi-value"></div><div class="kpi-period"></div><div class="kpi-comparisons"></div></div>
      <div class="kpi-card" data-metric="profit"><div class="kpi-title">Profit</div><div class="kpi-value"></div><div class="kpi-period"></div><div class="kpi-comparisons"></div></div>
      <div class="kpi-card" data-metric="orders"><div class="kpi-title">Orders</div><div class="kpi-value"></div><div class="kpi-period"></div><div class="kpi-comparisons"></div></div>
      <div class="kpi-card" data-metric="aov"><div class="kpi-title">AOV</div><div class="kpi-value"></div><div class="kpi-period"></div><div class="kpi-comparisons"></div></div>
      <div class="kpi-card" data-metric="cac"><div class="kpi-title">CAC</div><div class="kpi-value"></div><div class="kpi-period"></div><div class="kpi-comparisons"></div></div>
      <div class="kpi-card" data-metric="roas"><div class="kpi-title">ROAS</div><div class="kpi-value"></div><div class="kpi-period"></div><div class="kpi-comparisons"></div></div>
      <div class="kpi-card" data-metric="pre_ad_contribution_margin"><div class="kpi-title">Pre-Ad Contribution Margin</div><div class="kpi-value"></div><div class="kpi-period"></div><div class="kpi-comparisons"></div></div>
      <div class="kpi-card" data-metric="post_ad_margin"><div class="kpi-title">Post-Ad Margin</div><div class="kpi-value"></div><div class="kpi-period"></div><div class="kpi-comparisons"></div></div>
      <div class="kpi-card" data-metric="company_margin_with_fixed"><div class="kpi-title">Company Margin (incl. fixed)</div><div class="kpi-value"></div><div class="kpi-period"></div><div class="kpi-comparisons"></div></div>
    </div>

    <div class="section-title">Revenue</div>
    <div class="chart-card">
      <h3 class="chart-title">Revenue vs Profit</h3>
      <p class="chart-subtitle">Daily revenue and profit with 7-day smoothing.</p>
      <div class="chart-wrap h420"><canvas id="revenueProfitChart"></canvas></div>
    </div>

    <div class="section-title">Sales Dynamics</div>
    <div class="chart-card">
      <h3 class="chart-title">Orders and AOV</h3>
      <p class="chart-subtitle">Orders (daily bars), AOV rolling weekly signal, and orders 7-day average.</p>
      <div class="chart-wrap h420"><canvas id="ordersAovChart"></canvas></div>
    </div>

    <div class="section-title">Marketing Efficiency</div>
    <div class="chart-card">
      <h3 class="chart-title">Marketing Efficiency (Weekly)</h3>
      <p class="chart-subtitle">Weekly-smoothed CAC and ROAS to reduce daily volatility.</p>
      <div class="chart-wrap h420"><canvas id="cacRoasChart"></canvas></div>
    </div>

    <div class="section-title">Unit Economics</div>
    <div class="chart-card">
      <h3 class="chart-title">Unit Economics</h3>
      <p class="chart-subtitle">Rolling 30-day pre-ad and post-ad margins.</p>
      <div class="chart-wrap h420"><canvas id="marginLtvChart"></canvas></div>
    </div>

    <div class="section-title">Profit Trajectory</div>
    <div class="chart-card">
      <h3 class="chart-title">Profit Trend</h3>
      <p class="chart-subtitle">Daily profit, 7-day smoothing, and cumulative trend.</p>
      <div class="chart-wrap h420"><canvas id="profitTrendChart"></canvas></div>
    </div>
  </div>

  <script>
    const DATA = __DATA__;
    const COLORS = {
      revenue: '#2563EB',
      profit: '#10B981',
      orders: '#7C3AED',
      aov: '#EC4899',
      cac: '#EF4444',
      roas: '#F59E0B',
      margin: '#06B6D4',
      ltv: '#8B5CF6',
      hGrid: 'rgba(229, 231, 235, 0.12)'
    };

    Chart.defaults.font.family = '"IBM Plex Sans", "Segoe UI", Roboto, Helvetica, Arial, sans-serif';
    Chart.defaults.font.size = 13;
    Chart.defaults.color = '#334155';
    Chart.defaults.elements.line.tension = 0.35;
    Chart.defaults.elements.point.radius = 0;
    Chart.defaults.elements.point.hoverRadius = 4;

    function movingAverage(series, windowSize) {
      return series.map((_, idx) => {
        const from = Math.max(0, idx - windowSize + 1);
        const values = series
          .slice(from, idx + 1)
          .filter((v) => Number.isFinite(v));
        if (!values.length) return null;
        return values.reduce((a, b) => a + b, 0) / values.length;
      });
    }

    function mean(values) {
      const arr = values.filter((v) => Number.isFinite(v));
      if (!arr.length) return null;
      return arr.reduce((a, b) => a + b, 0) / arr.length;
    }

    function std(values) {
      const m = mean(values);
      const arr = values.filter((v) => Number.isFinite(v));
      if (m === null || !arr.length) return null;
      const variance = arr.reduce((acc, v) => acc + ((v - m) ** 2), 0) / arr.length;
      return Math.sqrt(variance);
    }

    function formatNum(value, digits = 2) {
      if (!Number.isFinite(value)) return 'N/A';
      return value.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
    }

    function safeSeries(name) {
      return (DATA.series[name] || []).map((v) => (Number.isFinite(v) ? Number(v) : null));
    }

    const labels = DATA.series.labels;
    const revenue = safeSeries('revenue');
    const profit = safeSeries('profit');
    const orders = safeSeries('orders');
    const aov = safeSeries('aov');
    const cac = safeSeries('cac');
    const roas = safeSeries('roas');
    const preMargin30 = safeSeries('pre_margin30');
    const postMargin30 = safeSeries('post_margin30');

    const revenue7 = movingAverage(revenue, 7);
    const profit7 = movingAverage(profit, 7);
    const orders7 = movingAverage(orders, 7);
    const aov7 = movingAverage(aov, 7);
    const cac7 = movingAverage(cac, 7);
    const roas7 = movingAverage(roas, 7);

    function anomalyPoints(series, zThreshold = 2.5) {
      const points = series.map(() => null);
      const changes = [];
      const idxByChange = [];
      for (let i = 1; i < series.length; i++) {
        const prev = series[i - 1];
        const cur = series[i];
        if (!Number.isFinite(prev) || !Number.isFinite(cur) || Math.abs(prev) < 1e-9) continue;
        const change = ((cur - prev) / Math.abs(prev)) * 100;
        if (Number.isFinite(change)) {
          changes.push(change);
          idxByChange.push(i);
        }
      }
      const m = mean(changes);
      const s = std(changes);
      if (!Number.isFinite(m) || !Number.isFinite(s) || s <= 0) return points;
      const limit = zThreshold * s;
      for (let j = 0; j < changes.length; j++) {
        if (Math.abs(changes[j] - m) > limit) {
          const idx = idxByChange[j];
          points[idx] = series[idx];
        }
      }
      return points;
    }

    const revenueAnomalies = anomalyPoints(revenue);
    const profitAnomalies = anomalyPoints(profit);
    const ordersAnomalies = anomalyPoints(orders);
    const cacAnomalies = anomalyPoints(cac7);
    const roasAnomalies = anomalyPoints(roas7);
    const preMarginAnomalies = anomalyPoints(preMargin30);
    const postMarginAnomalies = anomalyPoints(postMargin30);

    document.getElementById("meta").textContent =
      `Range: ${DATA.meta.from_date} -> ${DATA.meta.to_date} | Generated UTC: ${DATA.meta.generated_at_utc}`;
    const KPI = DATA.kpi || {};

    const commonOptions = {
      responsive: true,
      maintainAspectRatio: false,
      normalized: true,
      interaction: { mode: "index", intersect: false },
      animation: { duration: 650, easing: 'easeOutQuart' },
      plugins: {
        legend: { position: "bottom", labels: { boxWidth: 16, usePointStyle: true, pointStyle: 'line', font: { size: 13 } } },
        tooltip: {
          titleFont: { size: 14 },
          bodyFont: { size: 14 },
          padding: 10
        }
      },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 12 } },
        y: { grid: { color: COLORS.hGrid, lineWidth: 1 }, title: { display: true, font: { size: 13 } } }
      }
    };

    function fmtEur(value, digits = 2) {
      return Number.isFinite(value) ? `${formatNum(value, digits)} EUR` : 'N/A';
    }

    function fmtMetricValue(metricKey, value, allMetricValues) {
      if (!Number.isFinite(value)) return 'N/A';
      if (metricKey === 'orders') return formatNum(value, 0);
      if (metricKey === 'roas') return `${formatNum(value, 2)}x`;
      if (metricKey === 'company_margin_with_fixed') {
        const nominal = allMetricValues?.company_profit_with_fixed;
        if (Number.isFinite(nominal)) {
          return `${formatNum(value, 2)}% (${fmtEur(nominal, 2)})`;
        }
        return `${formatNum(value, 2)}%`;
      }
      if (metricKey === 'contribution_margin' || metricKey === 'pre_ad_contribution_margin' || metricKey === 'post_ad_margin') return `${formatNum(value, 2)}%`;
      return fmtEur(value, 2);
    }

    function metricDirection(metricKey) {
      return metricKey === 'cac' ? 'down' : 'up';
    }

    function toneForDelta(metricKey, delta) {
      if (!Number.isFinite(delta) || Math.abs(delta) < 0.5) return 'tone-neutral';
      const isGood = metricDirection(metricKey) === 'down' ? delta < 0 : delta > 0;
      return isGood ? 'tone-good' : 'tone-bad';
    }

    function deltaLabel(delta) {
      if (!Number.isFinite(delta)) return 'N/A';
      if (delta > 0) return `UP +${formatNum(delta, 1)}%`;
      if (delta < 0) return `DOWN ${formatNum(delta, 1)}%`;
      return `FLAT ${formatNum(delta, 1)}%`;
    }

    function comparisonRows(windowKey, metricKey, cmpObj) {
      const cmp = cmpObj || {};
      if (windowKey === 'daily') {
        return [
          { label: 'vs previous day', delta: cmp.vs_prev_day },
          { label: 'vs same weekday last week', delta: cmp.vs_week },
        ];
      }
      if (windowKey === 'weekly') {
        return [
          { label: 'vs previous 7d', delta: cmp.vs_prev_7d },
          { label: 'vs same week last month', delta: cmp.vs_month },
          { label: 'vs same week last year', delta: cmp.vs_year },
        ];
      }
      return [
        { label: 'vs previous 30d', delta: cmp.vs_prev_30d },
        { label: 'vs same month last year', delta: cmp.vs_year },
      ];
    }

    function renderKpiCards(windowKey) {
      const windowData = KPI.windows?.[windowKey] || {};
      const metricValues = windowData.metrics || {};
      const comparisons = KPI.comparisons?.[windowKey] || {};
      const periodLabel = windowData.label || 'Selected period';

      document.querySelectorAll('.kpi-window-btn').forEach((btn) => {
        btn.classList.toggle('active', btn.dataset.window === windowKey);
      });

      document.querySelectorAll('.kpi-card').forEach((card) => {
        const metricKey = card.dataset.metric;
        if (!metricKey) return;
        const valueEl = card.querySelector('.kpi-value');
        const periodEl = card.querySelector('.kpi-period');
        const rowsEl = card.querySelector('.kpi-comparisons');
        if (!valueEl || !periodEl || !rowsEl) return;

        valueEl.textContent = fmtMetricValue(metricKey, metricValues?.[metricKey], metricValues);
        periodEl.textContent = periodLabel;
        rowsEl.innerHTML = '';

        const rows = comparisonRows(windowKey, metricKey, comparisons?.[metricKey]).slice(0, 3);
        rows.forEach((row) => {
          const div = document.createElement('div');
          div.className = 'kpi-cmp-row';
          if (row.split) {
            const month = row.split[0];
            const year = row.split[1];
            const monthTone = toneForDelta(metricKey, month);
            const yearTone = toneForDelta(metricKey, year);
            div.innerHTML = `<span>${row.label}</span>: <span class="kpi-cmp-split"><span class="delta ${monthTone}">M ${deltaLabel(month)}</span><span class="delta ${yearTone}">Y ${deltaLabel(year)}</span></span>`;
          } else {
            const tone = toneForDelta(metricKey, row.delta);
            div.innerHTML = `<span class="delta ${tone}">${deltaLabel(row.delta)}</span><span>${row.label}</span>`;
          }
          rowsEl.appendChild(div);
        });
      });
    }

    const activeKpiWindow = KPI.default_window || 'monthly';
    renderKpiCards(activeKpiWindow);
    document.querySelectorAll('.kpi-window-btn').forEach((btn) => {
      btn.addEventListener('click', () => renderKpiCards(btn.dataset.window || 'monthly'));
    });

    new Chart(document.getElementById("revenueProfitChart"), {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Revenue (EUR)",
            data: revenue,
            borderColor: COLORS.revenue,
            backgroundColor: 'rgba(37, 99, 235, 0.08)',
            borderWidth: 3.4,
            fill: false
          },
          {
            label: "Profit (EUR)",
            data: profit,
            borderColor: COLORS.profit,
            backgroundColor: 'rgba(16, 185, 129, 0.16)',
            borderWidth: 2.2,
            fill: 'origin'
          },
          {
            label: "Revenue 7d MA",
            data: revenue7,
            borderColor: 'rgba(37, 99, 235, 0.65)',
            borderDash: [6, 4],
            borderWidth: 2,
            fill: false
          },
          {
            label: "Profit 7d MA",
            data: profit7,
            borderColor: 'rgba(16, 185, 129, 0.72)',
            borderDash: [6, 4],
            borderWidth: 2,
            fill: false
          },
          {
            label: "Revenue Anomaly (>2.5 SD)",
            data: revenueAnomalies,
            borderColor: "transparent",
            backgroundColor: "rgba(37, 99, 235, 0.95)",
            pointRadius: 4,
            pointHoverRadius: 5,
            showLine: false,
            spanGaps: true
          },
          {
            label: "Profit Anomaly (>2.5 SD)",
            data: profitAnomalies,
            borderColor: "transparent",
            backgroundColor: "rgba(239, 68, 68, 0.95)",
            pointRadius: 4,
            pointHoverRadius: 5,
            showLine: false,
            spanGaps: true
          }
        ]
      },
      options: {
        ...commonOptions,
        plugins: {
          ...commonOptions.plugins,
          tooltip: {
            ...commonOptions.plugins.tooltip,
            callbacks: {
              label: (ctx) => `${ctx.dataset.label}: ${formatNum(ctx.parsed.y, 2)} EUR`,
              afterBody: (items) => {
                if (!items.length) return '';
                const idx = items[0].dataIndex;
                return [
                  `Revenue 7d MA: ${Number.isFinite(revenue7[idx]) ? `${formatNum(revenue7[idx], 2)} EUR` : 'N/A'}`,
                  `Profit 7d MA: ${Number.isFinite(profit7[idx]) ? `${formatNum(profit7[idx], 2)} EUR` : 'N/A'}`
                ];
              }
            }
          }
        },
        scales: {
          x: commonOptions.scales.x,
          y: { ...commonOptions.scales.y, title: { ...commonOptions.scales.y.title, text: 'EUR' } }
        }
      }
    });

    new Chart(document.getElementById("ordersAovChart"), {
      data: {
        labels,
        datasets: [
          {
            type: "bar",
            label: "Orders",
            data: orders,
            yAxisID: "yOrders",
            backgroundColor: 'rgba(124, 58, 237, 0.5)',
            borderColor: COLORS.orders,
            borderWidth: 0,
            borderRadius: 4
          },
          {
            type: "line",
            label: "Orders 7d MA",
            data: orders7,
            yAxisID: "yOrders",
            borderColor: 'rgba(124, 58, 237, 0.9)',
            borderDash: [5, 4],
            fill: false
          },
          {
            type: "line",
            label: "AOV 7d Avg (EUR)",
            data: aov7,
            yAxisID: "yAov",
            borderColor: COLORS.aov,
            borderWidth: 2.4,
            fill: false
          },
          {
            type: "line",
            label: "Orders Anomaly (>2.5 SD)",
            data: ordersAnomalies,
            yAxisID: "yOrders",
            borderColor: "transparent",
            backgroundColor: "rgba(239, 68, 68, 0.95)",
            pointRadius: 4,
            pointHoverRadius: 5,
            showLine: false,
            spanGaps: true
          }
        ]
      },
      options: {
        ...commonOptions,
        plugins: {
          ...commonOptions.plugins,
          tooltip: {
            ...commonOptions.plugins.tooltip,
            callbacks: {
              label: (ctx) => {
                if (ctx.dataset.yAxisID === 'yAov') return `${ctx.dataset.label}: ${formatNum(ctx.parsed.y)} EUR`;
                return `${ctx.dataset.label}: ${formatNum(ctx.parsed.y, 0)}`;
              },
              afterBody: (items) => {
                if (!items.length) return '';
                const idx = items[0].dataIndex;
                return [
                  `Orders 7d MA: ${Number.isFinite(orders7[idx]) ? formatNum(orders7[idx], 2) : 'N/A'}`,
                  `AOV 7d Avg: ${Number.isFinite(aov7[idx]) ? `${formatNum(aov7[idx], 2)} EUR` : 'N/A'}`
                ];
              },
            }
          }
        },
        scales: {
          x: commonOptions.scales.x,
          yOrders: {
            type: "linear",
            position: "left",
            beginAtZero: true,
            grid: { color: COLORS.hGrid },
            title: { display: true, text: 'Orders', font: { size: 13 } }
          },
          yAov: {
            type: "linear",
            position: "right",
            grid: { display: false },
            beginAtZero: true,
            title: { display: true, text: 'AOV (EUR)', font: { size: 13 } }
          }
        }
      }
    });

    new Chart(document.getElementById("cacRoasChart"), {
      data: {
        labels,
        datasets: [
          {
            type: "line",
            label: "CAC Weekly Avg (7d)",
            data: cac7,
            yAxisID: "yCac",
            borderColor: COLORS.cac,
            spanGaps: true
          },
          {
            type: "line",
            label: "ROAS Weekly Avg (7d)",
            data: roas7,
            yAxisID: "yRoas",
            borderColor: COLORS.roas,
            spanGaps: true
          },
          {
            type: "line",
            label: "CAC Anomaly (>2.5 SD)",
            data: cacAnomalies,
            yAxisID: "yCac",
            borderColor: 'transparent',
            backgroundColor: 'rgba(239, 68, 68, 0.95)',
            pointRadius: 4,
            pointHoverRadius: 5,
            showLine: false,
            spanGaps: true
          },
          {
            type: "line",
            label: "ROAS Anomaly (>2.5 SD)",
            data: roasAnomalies,
            yAxisID: "yRoas",
            borderColor: 'transparent',
            backgroundColor: 'rgba(245, 158, 11, 0.95)',
            pointRadius: 4,
            pointHoverRadius: 5,
            showLine: false,
            spanGaps: true
          }
        ]
      },
      options: {
        ...commonOptions,
        plugins: {
          ...commonOptions.plugins,
          tooltip: {
            ...commonOptions.plugins.tooltip,
            callbacks: {
              label: (ctx) => {
                if (ctx.dataset.label.includes('ROAS')) return `${ctx.dataset.label}: ${formatNum(ctx.parsed.y, 3)}x`;
                return `${ctx.dataset.label}: ${formatNum(ctx.parsed.y)} EUR`;
              },
              afterBody: (items) => {
                if (!items.length) return '';
                const idx = items[0].dataIndex;
                return [
                  `CAC 7d Avg: ${Number.isFinite(cac7[idx]) ? `${formatNum(cac7[idx], 2)} EUR` : 'N/A'}`,
                  `ROAS 7d Avg: ${Number.isFinite(roas7[idx]) ? `${formatNum(roas7[idx], 3)}x` : 'N/A'}`
                ];
              }
            }
          }
        },
        scales: {
          x: commonOptions.scales.x,
          yCac: {
            type: "linear",
            position: "left",
            beginAtZero: true,
            grid: { color: COLORS.hGrid },
            title: { display: true, text: 'CAC (EUR)', font: { size: 13 } }
          },
          yRoas: {
            type: "linear",
            position: "right",
            grid: { drawOnChartArea: false },
            beginAtZero: true,
            title: { display: true, text: 'ROAS (x)', font: { size: 13 } }
          }
        }
      }
    });

    const window30 = DATA.windows?.w30 || {};

    new Chart(document.getElementById("marginLtvChart"), {
      data: {
        labels,
        datasets: [
          {
            type: "line",
            label: "Pre-Ad Contribution Margin 30d (%)",
            data: preMargin30,
            yAxisID: "yPct",
            borderColor: COLORS.margin,
            spanGaps: true
          },
          {
            type: "line",
            label: "Post-Ad Margin 30d (%)",
            data: postMargin30,
            yAxisID: "yPct",
            borderColor: COLORS.profit,
            spanGaps: true
          },
          {
            type: "line",
            label: "Pre-Ad Margin Anomaly (>2.5 SD)",
            data: preMarginAnomalies,
            yAxisID: "yPct",
            borderColor: 'transparent',
            backgroundColor: 'rgba(239, 68, 68, 0.95)',
            pointRadius: 4,
            pointHoverRadius: 5,
            showLine: false,
            spanGaps: true
          },
          {
            type: "line",
            label: "Post-Ad Margin Anomaly (>2.5 SD)",
            data: postMarginAnomalies,
            yAxisID: "yPct",
            borderColor: 'transparent',
            backgroundColor: 'rgba(245, 158, 11, 0.95)',
            pointRadius: 4,
            pointHoverRadius: 5,
            showLine: false,
            spanGaps: true
          }
        ]
      },
      options: {
        ...commonOptions,
        plugins: {
          ...commonOptions.plugins,
          tooltip: {
            ...commonOptions.plugins.tooltip,
            callbacks: {
              label: (ctx) => {
                return `${ctx.dataset.label}: ${formatNum(ctx.parsed.y)}%`;
              },
              afterBody: (items) => {
                if (!items.length) return '';
                const idx = items[0].dataIndex;
                return [
                  `Pre-Ad Margin 30d: ${Number.isFinite(preMargin30[idx]) ? `${formatNum(preMargin30[idx], 2)}%` : 'N/A'}`,
                  `Post-Ad Margin 30d: ${Number.isFinite(postMargin30[idx]) ? `${formatNum(postMargin30[idx], 2)}%` : 'N/A'}`
                ];
              },
              footer: () => {
                const cpo = Number.isFinite(window30.contribution_per_order) ? formatNum(window30.contribution_per_order) : 'N/A';
                const ppo = Number.isFinite(window30.profit_per_order) ? formatNum(window30.profit_per_order) : 'N/A';
                return `30d Contribution/Order: ${cpo} EUR | 30d Profit/Order: ${ppo} EUR`;
              }
            }
          }
        },
        scales: {
          x: commonOptions.scales.x,
          yPct: {
            type: "linear",
            position: "right",
            beginAtZero: true,
            grid: { color: COLORS.hGrid },
            title: { display: true, text: 'Margin (%)', font: { size: 13 } }
          }
        }
      }
    });

    const cumulativeProfit = profit.reduce((acc, value, idx) => {
      const prev = idx === 0 ? 0 : (acc[idx - 1] ?? 0);
      acc.push(Number.isFinite(value) ? prev + value : prev);
      return acc;
    }, []);

    new Chart(document.getElementById("profitTrendChart"), {
      data: {
        labels,
        datasets: [
          {
            type: "line",
            label: "Daily Profit (EUR)",
            data: profit,
            yAxisID: "yProfit",
            borderColor: COLORS.profit,
            backgroundColor: 'rgba(16, 185, 129, 0.14)',
            fill: 'origin'
          },
          {
            type: "line",
            label: "Profit 7d MA",
            data: profit7,
            yAxisID: "yProfit",
            borderColor: 'rgba(16, 185, 129, 0.65)',
            borderDash: [6, 4],
            fill: false
          },
          {
            type: "line",
            label: "Cumulative Profit (EUR)",
            data: cumulativeProfit,
            yAxisID: "yCumProfit",
            borderColor: 'rgba(37, 99, 235, 0.7)',
            borderDash: [3, 3],
            fill: false
          },
          {
            type: "line",
            label: "Profit Anomaly (>2.5 SD)",
            data: profitAnomalies,
            yAxisID: "yProfit",
            borderColor: "transparent",
            backgroundColor: "rgba(239, 68, 68, 0.95)",
            pointRadius: 4,
            pointHoverRadius: 5,
            showLine: false,
            spanGaps: true
          }
        ]
      },
      options: {
        ...commonOptions,
        plugins: {
          ...commonOptions.plugins,
          tooltip: {
            ...commonOptions.plugins.tooltip,
            callbacks: {
              label: (ctx) => `${ctx.dataset.label}: ${formatNum(ctx.parsed.y)} EUR`,
              afterBody: (items) => {
                if (!items.length) return '';
                const idx = items[0].dataIndex;
                return `Profit 7d MA: ${Number.isFinite(profit7[idx]) ? `${formatNum(profit7[idx], 2)} EUR` : 'N/A'}`;
              }
            }
          }
        },
        scales: {
          x: commonOptions.scales.x,
          yProfit: {
            type: "linear",
            position: "left",
            beginAtZero: false,
            grid: { color: COLORS.hGrid },
            title: { display: true, text: 'Daily Profit (EUR)', font: { size: 13 } }
          },
          yCumProfit: {
            type: "linear",
            position: "right",
            beginAtZero: false,
            grid: { drawOnChartArea: false },
            title: { display: true, text: 'Cumulative Profit (EUR)', font: { size: 13 } }
          }
        }
      }
    });

  </script>
</body>
</html>
""".replace("__DATA__", payload_json)

    output_path = get_cfo_graph_path(project, from_date, to_date)
    output_path.write_text(html, encoding="utf-8")
    return output_path


def build_email_body(from_date: str, to_date: str, summary_text: str) -> str:
    return (
        "Dobry den,\n\n"
        "v prilohe posielam denny Vevo report v HTML formate.\n"
        f"Sledovane obdobie: {from_date} az {to_date}.\n\n"
        f"{summary_text}\n\n"
        "Tento email bol odoslany automaticky zo systemu Vevo reporting.\n"
    )


def put_metric(metric_name: str, value: float, project: str, unit: str = "Count") -> None:
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
            Namespace="VevoReporting",
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
    extra_attachments: Optional[List[Path]] = None,
) -> str:
    region = os.getenv("AWS_REGION", "eu-central-1").strip()
    configuration_set = os.getenv("SES_CONFIGURATION_SET", "vevo-reporting").strip()
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
    load_dotenv()
    # Then load project env selected via CLI (or REPORT_PROJECT) before full arg parsing.
    bootstrap_project = bootstrap_project_from_argv(sys.argv[1:])
    os.environ["REPORT_PROJECT"] = bootstrap_project
    load_project_env(bootstrap_project)

    args = parse_args()
    project = (args.project or bootstrap_project).strip() or DEFAULT_PROJECT
    os.environ["REPORT_PROJECT"] = project
    os.environ["REPORT_DATA_DIR"] = str(project_data_dir(project).resolve())

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
        )

    output_paths = get_output_paths(project, from_date, to_date)
    missing = [str(path) for path in output_paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Expected output files not found: {missing}")

    s3_upload_outputs(project, output_paths)

    if args.skip_email:
        print("Email sending skipped by flag.")
        return

    subject = build_email_subject(project)
    summary_text = build_report_summary(output_paths)
    cfo_graph_html = generate_cfo_graph_html(project, output_paths, from_date, to_date)
    body_text = build_email_body(from_date, to_date, summary_text)
    message_id = send_email_ses(
        subject=subject,
        body_text=body_text,
        file_paths=output_paths,
        extra_attachments=[cfo_graph_html],
    )
    put_metric("ReportEmailSent", 1, project)
    put_metric("ReportRunSucceeded", 1, project)
    print(f"SES message sent. MessageId={message_id}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        put_metric("ReportRunFailed", 1, os.getenv("REPORT_PROJECT", DEFAULT_PROJECT))
        raise
