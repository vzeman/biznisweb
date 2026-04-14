#!/usr/bin/env python3
"""
Reusable CFO KPI payload helpers for reporting surfaces.

The goal is to keep windowed KPI calculations consistent between the
standalone CFO dashboard and the main reporting HTML.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd


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


def _pct_change(current: float, previous: float) -> Optional[float]:
    if previous == 0:
        if current == 0:
            return 0.0
        return None
    return ((current - previous) / abs(previous)) * 100.0


def build_order_records_from_export_df(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    orders_map: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        order_num = str(row.get("order_num") or "").strip()
        if not order_num or order_num in orders_map:
            continue

        date_str = str(row.get("purchase_date") or "").split(" ")[0].strip()
        if not date_str:
            continue
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        email = str(row.get("customer_email") or "").strip().lower()
        if not email or email == "nan":
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
        bucket = buckets.setdefault(
            d,
            {
                "new_customers_set": set(),
                "returning_customers_set": set(),
                "new_orders": 0,
                "returning_orders": 0,
            },
        )
        if first_order_date[email] == d:
            bucket["new_customers_set"].add(email)
            bucket["new_orders"] += 1
        else:
            bucket["returning_customers_set"].add(email)
            bucket["returning_orders"] += 1

    return {
        d: {
            "new_customers": len(data["new_customers_set"]),
            "returning_customers": len(data["returning_customers_set"]),
            "new_orders": int(data["new_orders"]),
            "returning_orders": int(data["returning_orders"]),
        }
        for d, data in buckets.items()
    }


def _window_unique_customers(order_records: List[Dict[str, Any]], end_date: date, days: int) -> int:
    start_date = end_date - timedelta(days=days - 1)
    customers = {
        rec["email"]
        for rec in order_records
        if start_date <= rec["date"] <= end_date
    }
    return len(customers)


def _build_daily_rows_from_date_agg(date_agg: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if date_agg is None or date_agg.empty:
        return rows

    for _, row in date_agg.iterrows():
        date_value = row.get("date")
        if pd.isna(date_value):
            continue
        try:
            if isinstance(date_value, pd.Timestamp):
                d = date_value.date()
            elif isinstance(date_value, datetime):
                d = date_value.date()
            elif isinstance(date_value, date):
                d = date_value
            else:
                d = pd.to_datetime(str(date_value).strip()).date()
        except (TypeError, ValueError):
            continue

        revenue = float(row.get("total_revenue", 0) or 0)
        orders = int(float(row.get("unique_orders", 0) or 0))
        product_costs = float(row.get("product_expense", 0) or 0)
        packaging_costs = float(row.get("packaging_cost", 0) or 0)
        shipping_subsidy = float(row.get("shipping_net_cost", row.get("shipping_subsidy_cost", 0)) or 0)
        facebook_ads = float(row.get("fb_ads_spend", 0) or 0)
        google_ads = float(row.get("google_ads_spend", 0) or 0)
        total_ads = facebook_ads + google_ads
        profit = float(row.get("contribution_profit", row.get("net_profit", 0)) or 0)
        fixed_overhead = float(row.get("fixed_daily_cost", 0) or 0)
        pre_ad_contribution = float(row.get("pre_ad_contribution_profit", 0) or 0)
        contribution_margin_percent = float(row.get("pre_ad_contribution_margin_pct", 0) or 0)

        aov = (revenue / orders) if orders > 0 else 0.0
        roas = (revenue / total_ads) if total_ads > 0 else 0.0
        contribution_per_order = (pre_ad_contribution / orders) if orders > 0 else 0.0
        post_ad_contribution_per_order = (profit / orders) if orders > 0 else 0.0

        rows.append(
            {
                "date": d,
                "revenue": revenue,
                "orders": orders,
                "units_sold": int(float(row.get("total_quantity", 0) or 0)),
                "aov": aov,
                "product_costs": product_costs,
                "packaging_costs": packaging_costs,
                "shipping_subsidy": shipping_subsidy,
                "shipping_net": shipping_subsidy,
                "facebook_ads": facebook_ads,
                "google_ads": google_ads,
                "total_ads": total_ads,
                "profit": profit,
                "fixed_overhead": fixed_overhead,
                "roas": roas,
                "contribution_margin_percent": contribution_margin_percent,
                "pre_ad_contribution": pre_ad_contribution,
                "contribution_per_order": contribution_per_order,
                "post_ad_contribution_per_order": post_ad_contribution_per_order,
            }
        )

    return sorted(rows, key=lambda r: r["date"])


def _window_aggregate(
    row_by_date: Dict[date, Dict[str, Any]],
    end_date: date,
    days: int,
    customer_by_date: Dict[date, Dict[str, int]],
    order_records: List[Dict[str, Any]],
    fixed_daily_cost_eur: float,
) -> Dict[str, Optional[float]]:
    revenue = 0.0
    orders = 0
    ads = 0.0
    fb_ads = 0.0
    google_ads = 0.0
    profit = 0.0
    fixed_overhead = 0.0
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
            fixed_overhead += float(row.get("fixed_overhead", 0.0) or 0.0)
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
    fallback_fixed_total = fixed_daily_cost_eur * days
    fixed_overhead_total = fixed_overhead if fixed_overhead > 0 else fallback_fixed_total
    company_profit_with_fixed = profit - fixed_overhead_total
    company_margin_with_fixed = (company_profit_with_fixed / revenue * 100) if revenue > 0 else 0.0

    return {
        "revenue": revenue,
        "orders": float(orders),
        "ads": ads,
        "fb_ads": fb_ads,
        "google_ads": google_ads,
        "profit": profit,
        "fixed_overhead": fixed_overhead_total,
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


def _all_time_window_days(sorted_dates: List[date]) -> int:
    if not sorted_dates:
        return 0
    return max((sorted_dates[-1] - sorted_dates[0]).days + 1, 1)


def build_cfo_kpi_payload(
    date_agg: pd.DataFrame,
    export_df: Optional[pd.DataFrame],
    fixed_daily_cost_eur: float = 70.0,
) -> Dict[str, Any]:
    daily_rows = _build_daily_rows_from_date_agg(date_agg)
    if not daily_rows:
        return {}

    order_records = build_order_records_from_export_df(export_df)
    customer_by_date = _load_customer_dynamics(order_records)
    row_by_date: Dict[date, Dict[str, Any]] = {row["date"]: row for row in daily_rows}

    last_date = daily_rows[-1]["date"]
    prev_day = last_date - timedelta(days=1)
    same_weekday_last_week = last_date - timedelta(days=7)
    same_day_last_month = _shift_months(last_date, -1)
    same_day_last_year = _shift_years(last_date, -1)

    weekly_prev_end = last_date - timedelta(days=7)
    weekly_last_month_end = _shift_months(last_date, -1)
    weekly_last_year_end = _shift_years(last_date, -1)
    monthly_prev_end = last_date - timedelta(days=30)
    monthly_last_year_end = _shift_years(last_date, -1)

    def has_window_data(end_date: date, days: int) -> bool:
        for i in range(days):
            d = end_date - timedelta(days=(days - 1 - i))
            if d in row_by_date:
                return True
        return False

    sorted_dates = sorted(row_by_date.keys())
    all_time_days = _all_time_window_days(sorted_dates)
    all_time_prev_end = sorted_dates[0] - timedelta(days=1) if sorted_dates else last_date
    all_time_year_end = _shift_years(last_date, -1)

    day_cur = _window_aggregate(row_by_date, last_date, 1, customer_by_date, order_records, fixed_daily_cost_eur)
    day_prev = _window_aggregate(row_by_date, prev_day, 1, customer_by_date, order_records, fixed_daily_cost_eur) if prev_day in row_by_date else None
    day_week = _window_aggregate(row_by_date, same_weekday_last_week, 1, customer_by_date, order_records, fixed_daily_cost_eur) if same_weekday_last_week in row_by_date else None
    day_month = _window_aggregate(row_by_date, same_day_last_month, 1, customer_by_date, order_records, fixed_daily_cost_eur) if same_day_last_month in row_by_date else None
    day_year = _window_aggregate(row_by_date, same_day_last_year, 1, customer_by_date, order_records, fixed_daily_cost_eur) if same_day_last_year in row_by_date else None

    w7 = _window_aggregate(row_by_date, last_date, 7, customer_by_date, order_records, fixed_daily_cost_eur)
    w7_prev = _window_aggregate(row_by_date, weekly_prev_end, 7, customer_by_date, order_records, fixed_daily_cost_eur) if has_window_data(weekly_prev_end, 7) else None
    w7_month = _window_aggregate(row_by_date, weekly_last_month_end, 7, customer_by_date, order_records, fixed_daily_cost_eur) if has_window_data(weekly_last_month_end, 7) else None
    w7_year = _window_aggregate(row_by_date, weekly_last_year_end, 7, customer_by_date, order_records, fixed_daily_cost_eur) if has_window_data(weekly_last_year_end, 7) else None

    w30 = _window_aggregate(row_by_date, last_date, 30, customer_by_date, order_records, fixed_daily_cost_eur)
    w30_prev = _window_aggregate(row_by_date, monthly_prev_end, 30, customer_by_date, order_records, fixed_daily_cost_eur) if has_window_data(monthly_prev_end, 30) else None
    w30_year = _window_aggregate(row_by_date, monthly_last_year_end, 30, customer_by_date, order_records, fixed_daily_cost_eur) if has_window_data(monthly_last_year_end, 30) else None

    all_time = _window_aggregate(row_by_date, last_date, all_time_days, customer_by_date, order_records, fixed_daily_cost_eur)
    all_time_prev = (
        _window_aggregate(row_by_date, all_time_prev_end, all_time_days, customer_by_date, order_records, fixed_daily_cost_eur)
        if all_time_days > 0 and has_window_data(all_time_prev_end, all_time_days)
        else None
    )
    all_time_year = (
        _window_aggregate(row_by_date, all_time_year_end, all_time_days, customer_by_date, order_records, fixed_daily_cost_eur)
        if all_time_days > 0 and has_window_data(all_time_year_end, all_time_days)
        else None
    )

    metric_defs = [
        {"key": "revenue", "label": "Revenue", "direction": "up"},
        {"key": "profit", "label": "Profit", "direction": "up"},
        {"key": "orders", "label": "Orders", "direction": "up"},
        {"key": "aov", "label": "AOV", "direction": "up"},
        {"key": "cac", "label": "CAC", "direction": "down"},
        {"key": "roas", "label": "ROAS", "direction": "up"},
        {"key": "pre_ad_contribution_margin", "label": "Pre-Ad Contribution Margin", "direction": "up"},
        {"key": "post_ad_margin", "label": "Post-Ad Margin", "direction": "up"},
        {"key": "company_margin_with_fixed", "label": f"Company Margin (incl. EUR {int(fixed_daily_cost_eur)}/day fixed)", "direction": "up"},
    ]

    metric_keys = [metric["key"] for metric in metric_defs]
    trend_labels = {
        "daily": {"en": "14d trend", "sk": "Trend 14 dni"},
        "weekly": {"en": "8x 7d trend", "sk": "Trend 8x 7 dni"},
        "monthly": {"en": "8x 30d trend", "sk": "Trend 8x 30 dni"},
        "all_time": {"en": "12x 30d trend", "sk": "Trend 12x 30 dni"},
    }

    def safe_kpi_value(metric_key: str, aggregate: Optional[Dict[str, Optional[float]]]) -> Optional[float]:
        if not aggregate:
            return None
        value = aggregate.get(metric_key)
        if value is None:
            return None
        if metric_key == "roas":
            return min(float(value), 15.0)
        if metric_key == "cac":
            ads_spend = float(aggregate.get("ads") or 0.0)
            if ads_spend <= 0 or aggregate.get("cac") is None:
                return None
            return float(aggregate["cac"])
        return float(value)

    def snapshot(aggregate: Optional[Dict[str, Optional[float]]]) -> Dict[str, Optional[float]]:
        return {metric_key: safe_kpi_value(metric_key, aggregate) for metric_key in metric_keys}

    def secondary_snapshot(aggregate: Optional[Dict[str, Optional[float]]]) -> Dict[str, Optional[float]]:
        if not aggregate:
            return {}
        return {
            "company_margin_with_fixed": aggregate.get("company_profit_with_fixed"),
        }

    def trend_snapshot(window_days: int, points: int, window_key: str) -> Dict[str, Any]:
        if not sorted_dates:
            return {
                "label_en": trend_labels[window_key]["en"],
                "label_sk": trend_labels[window_key]["sk"],
                "dates": [],
                "metrics": {},
            }

        end_dates = sorted_dates[-points:]
        metric_series: Dict[str, List[Optional[float]]] = {metric_key: [] for metric_key in metric_keys}
        for end_date in end_dates:
            aggregate = _window_aggregate(row_by_date, end_date, window_days, customer_by_date, order_records, fixed_daily_cost_eur)
            for metric_key in metric_keys:
                metric_series[metric_key].append(safe_kpi_value(metric_key, aggregate))

        return {
            "label_en": trend_labels[window_key]["en"],
            "label_sk": trend_labels[window_key]["sk"],
            "dates": [end_date.isoformat() for end_date in end_dates],
            "metrics": metric_series,
        }

    day_vals = snapshot(day_cur)
    day_prev_vals = snapshot(day_prev) if day_prev else {}
    day_week_vals = snapshot(day_week) if day_week else {}
    day_month_vals = snapshot(day_month) if day_month else {}
    day_year_vals = snapshot(day_year) if day_year else {}

    w7_vals = snapshot(w7)
    w7_prev_vals = snapshot(w7_prev) if w7_prev else {}
    w7_month_vals = snapshot(w7_month) if w7_month else {}
    w7_year_vals = snapshot(w7_year) if w7_year else {}

    w30_vals = snapshot(w30)
    w30_prev_vals = snapshot(w30_prev) if w30_prev else {}
    w30_year_vals = snapshot(w30_year) if w30_year else {}

    all_time_vals = snapshot(all_time)
    all_time_prev_vals = snapshot(all_time_prev) if all_time_prev else {}
    all_time_year_vals = snapshot(all_time_year) if all_time_year else {}

    def delta(current: Optional[float], reference: Optional[float]) -> Optional[float]:
        if current is None or reference is None:
            return None
        return _pct_change(float(current), float(reference))

    comparisons: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {
        "daily": {},
        "weekly": {},
        "monthly": {},
        "all_time": {},
    }

    for metric in metric_defs:
        metric_key = metric["key"]
        comparisons["daily"][metric_key] = {
            "vs_prev_day": delta(day_vals.get(metric_key), day_prev_vals.get(metric_key)),
            "vs_week": delta(day_vals.get(metric_key), day_week_vals.get(metric_key)),
            "vs_month": delta(day_vals.get(metric_key), day_month_vals.get(metric_key)),
            "vs_year": delta(day_vals.get(metric_key), day_year_vals.get(metric_key)),
        }
        comparisons["weekly"][metric_key] = {
            "vs_prev_7d": delta(w7_vals.get(metric_key), w7_prev_vals.get(metric_key)),
            "vs_month": delta(w7_vals.get(metric_key), w7_month_vals.get(metric_key)),
            "vs_year": delta(w7_vals.get(metric_key), w7_year_vals.get(metric_key)),
        }
        comparisons["monthly"][metric_key] = {
            "vs_prev_30d": delta(w30_vals.get(metric_key), w30_prev_vals.get(metric_key)),
            "vs_year": delta(w30_vals.get(metric_key), w30_year_vals.get(metric_key)),
        }
        comparisons["all_time"][metric_key] = {
            "vs_prev_span": delta(all_time_vals.get(metric_key), all_time_prev_vals.get(metric_key)),
            "vs_year": delta(all_time_vals.get(metric_key), all_time_year_vals.get(metric_key)),
        }

    return {
        "default_window": "monthly",
        "metric_defs": metric_defs,
        "windows": {
            "daily": {
                "label": "Last day",
                "metrics": day_vals,
                "secondary_metrics": secondary_snapshot(day_cur),
                "trend": trend_snapshot(1, 14, "daily"),
            },
            "weekly": {
                "label": "Last 7 days",
                "metrics": w7_vals,
                "secondary_metrics": secondary_snapshot(w7),
                "trend": trend_snapshot(7, 8, "weekly"),
            },
            "monthly": {
                "label": "Last 30 days",
                "metrics": w30_vals,
                "secondary_metrics": secondary_snapshot(w30),
                "trend": trend_snapshot(30, 8, "monthly"),
            },
            "all_time": {
                "label": "All-time",
                "metrics": all_time_vals,
                "secondary_metrics": secondary_snapshot(all_time),
                "trend": trend_snapshot(30, min(12, len(sorted_dates)), "all_time"),
            },
        },
        "comparisons": comparisons,
    }
