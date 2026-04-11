#!/usr/bin/env python3
"""
Production dashboard renderer used by the main HTML reporting output.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from html import escape
from typing import Any, Dict, List, Optional

import pandas as pd


METRIC_LABELS = {
    "revenue": {"en": "Revenue (net)", "sk": "Tržby (netto)"},
    "profit": {"en": "Profit", "sk": "Zisk"},
    "orders": {"en": "Orders", "sk": "Objednávky"},
    "aov": {"en": "AOV (net)", "sk": "Priemerná objednávka (netto)"},
    "cac": {"en": "CAC", "sk": "CAC"},
    "roas": {"en": "ROAS", "sk": "ROAS"},
    "pre_ad_contribution_margin": {"en": "Pre-ad contribution", "sk": "Pre-ad kontribučná marža"},
    "post_ad_margin": {"en": "Post-ad margin", "sk": "Post-ad marža"},
    "company_margin_with_fixed": {"en": "Company margin (incl. fixed)", "sk": "Firemná marža (s fixom)"},
}

WINDOW_LABELS = {
    "daily": {"en": "Last day", "sk": "Posledný deň"},
    "weekly": {"en": "Last 7 days", "sk": "Posledných 7 dní"},
    "monthly": {"en": "Last 30 days", "sk": "Posledných 30 dní"},
}

COMPARISON_LABELS = {
    "daily": {
        "vs_prev_day": {"en": "vs previous day", "sk": "vs predošlý deň"},
        "vs_week": {"en": "vs same weekday last week", "sk": "vs rovnaký deň minulý týždeň"},
    },
    "weekly": {
        "vs_prev_7d": {"en": "vs previous 7d", "sk": "vs predošlých 7 dní"},
        "vs_month": {"en": "vs same week last month", "sk": "vs rovnaký týždeň minulý mesiac"},
    },
    "monthly": {
        "vs_prev_30d": {"en": "vs previous 30d", "sk": "vs predošlých 30 dní"},
        "vs_year": {"en": "vs same month last year", "sk": "vs rovnaké obdobie minulý rok"},
    },
}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _maybe_num(value: Any) -> Optional[float]:
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ma(values: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = []
    bucket: List[float] = []
    running = 0.0
    for value in values:
        bucket.append(value)
        running += value
        if len(bucket) > window:
            running -= bucket.pop(0)
        out.append(round(running / len(bucket), 4) if bucket else None)
    return out


def _series(date_agg: pd.DataFrame) -> Dict[str, List[Any]]:
    dates = date_agg["date"].astype(str).tolist()
    revenue = [_num(v) for v in date_agg["total_revenue"].tolist()]
    profit = [_num(v) for v in date_agg["net_profit"].tolist()]
    orders = [int(round(_num(v))) for v in date_agg["unique_orders"].tolist()]
    aov = [round((rev / ords) if ords > 0 else 0.0, 4) for rev, ords in zip(revenue, orders)]
    fb_ads = [_num(v) for v in date_agg.get("fb_ads_spend", pd.Series([0] * len(date_agg))).tolist()]
    google_ads = [_num(v) for v in date_agg.get("google_ads_spend", pd.Series([0] * len(date_agg))).tolist()]
    total_cost = [_num(v) for v in date_agg.get("total_cost", pd.Series([0] * len(date_agg))).tolist()]
    product_cost = [_num(v) for v in date_agg.get("product_expense", pd.Series([0] * len(date_agg))).tolist()]
    packaging = [_num(v) for v in date_agg.get("packaging_cost", pd.Series([0] * len(date_agg))).tolist()]
    shipping = [_num(v) for v in date_agg.get("shipping_net_cost", date_agg.get("shipping_subsidy_cost", pd.Series([0] * len(date_agg)))).tolist()]
    fixed = [_num(v) for v in date_agg.get("fixed_daily_cost", pd.Series([0] * len(date_agg))).tolist()]
    items = [int(round(_num(v))) for v in date_agg.get("total_items", pd.Series([0] * len(date_agg))).tolist()]
    avg_items = [round((itm / ords) if ords > 0 else 0.0, 4) for itm, ords in zip(items, orders)]
    total_ads = [round(f + g, 4) for f, g in zip(fb_ads, google_ads)]
    roas = [round((rev / ads) if ads > 0 else 0.0, 4) for rev, ads in zip(revenue, total_ads)]
    pre_margin = [_num(v) for v in date_agg.get("pre_ad_contribution_margin_pct", pd.Series([0] * len(date_agg))).tolist()]
    post_margin = [_num(v) for v in date_agg.get("post_ad_contribution_margin_pct", pd.Series([0] * len(date_agg))).tolist()]
    pre_contribution_per_order = [_num(v) for v in date_agg.get("pre_ad_contribution_profit_per_order", pd.Series([0] * len(date_agg))).tolist()]
    post_contribution_per_order = [_num(v) for v in date_agg.get("contribution_profit_per_order", pd.Series([0] * len(date_agg))).tolist()]
    roi = [_num(v) for v in date_agg.get("roi_percent", pd.Series([0] * len(date_agg))).tolist()]
    gross_margin = [
        round(((rev - cost) / rev * 100) if rev > 0 else 0.0, 4)
        for rev, cost in zip(revenue, product_cost)
    ]
    cumulative_avg_revenue = []
    cumulative_avg_profit = []
    running_revenue = 0.0
    running_profit = 0.0
    for idx, (rev, prof) in enumerate(zip(revenue, profit), 1):
        running_revenue += rev
        running_profit += prof
        cumulative_avg_revenue.append(round(running_revenue / idx, 4))
        cumulative_avg_profit.append(round(running_profit / idx, 4))
    return {
        "dates": dates,
        "revenue": revenue,
        "profit": profit,
        "orders": orders,
        "aov": aov,
        "items": items,
        "avg_items": avg_items,
        "total_cost": total_cost,
        "product_cost": product_cost,
        "packaging": packaging,
        "shipping": shipping,
        "fixed": fixed,
        "fb_ads": fb_ads,
        "google_ads": google_ads,
        "total_ads": total_ads,
        "roas": roas,
        "pre_margin": pre_margin,
        "post_margin": post_margin,
        "pre_contribution_per_order": pre_contribution_per_order,
        "post_contribution_per_order": post_contribution_per_order,
        "roi": roi,
        "gross_margin": gross_margin,
        "revenue_ma7": _ma(revenue, 7),
        "profit_ma7": _ma(profit, 7),
        "orders_ma7": _ma([float(v) for v in orders], 7),
        "aov_ma7": _ma(aov, 7),
        "items_ma7": _ma([float(v) for v in items], 7),
        "avg_items_ma7": _ma(avg_items, 7),
        "gross_margin_ma7": _ma(gross_margin, 7),
        "roi_ma7": _ma(roi, 7),
        "cumulative_avg_revenue": cumulative_avg_revenue,
        "cumulative_avg_profit": cumulative_avg_profit,
    }


def _kpis(payload: Optional[dict]) -> Dict[str, Any]:
    payload = payload or {}
    metric_defs = []
    for metric in payload.get("metric_defs") or []:
        key = str(metric.get("key") or "")
        if not key:
            continue
        metric_defs.append(
            {
                "key": key,
                "direction": metric.get("direction") or "up",
                "label_en": METRIC_LABELS.get(key, {}).get("en", key.replace("_", " ").title()),
                "label_sk": METRIC_LABELS.get(key, {}).get("sk", key.replace("_", " ").title()),
            }
        )
    windows = {}
    for window_key, window_payload in (payload.get("windows") or {}).items():
        windows[window_key] = {
            "label_en": WINDOW_LABELS.get(window_key, {}).get("en", window_key.title()),
            "label_sk": WINDOW_LABELS.get(window_key, {}).get("sk", window_key.title()),
            "metrics": {k: _maybe_num(v) for k, v in (window_payload.get("metrics") or {}).items()},
            "secondary_metrics": {k: _maybe_num(v) for k, v in (window_payload.get("secondary_metrics") or {}).items()},
            "trend": {
                "label_en": str(((window_payload.get("trend") or {}).get("label_en")) or ""),
                "label_sk": str(((window_payload.get("trend") or {}).get("label_sk")) or ""),
                "dates": [str(v) for v in (((window_payload.get("trend") or {}).get("dates")) or [])],
                "metrics": {
                    metric_key: [_maybe_num(point) for point in (series or [])]
                    for metric_key, series in (((window_payload.get("trend") or {}).get("metrics")) or {}).items()
                },
            },
        }
    comparisons = {}
    for window_key, comp_payload in (payload.get("comparisons") or {}).items():
        comparisons[window_key] = {}
        for metric_key, metric_comp in comp_payload.items():
            comparisons[window_key][metric_key] = {k: _maybe_num(v) for k, v in (metric_comp or {}).items()}
    return {
        "default_window": str(payload.get("default_window") or "monthly"),
        "metric_defs": metric_defs,
        "windows": windows,
        "comparisons": comparisons,
        "comparison_labels": COMPARISON_LABELS,
    }


def _cost_mix(date_agg: pd.DataFrame) -> Dict[str, Any]:
    return {
            "labels": ["Product", "Packaging", "Net shipping", "Facebook Ads", "Google Ads", "Fixed"],
            "values": [
                round(_num(date_agg.get("product_expense", pd.Series(dtype=float)).sum()), 2),
                round(_num(date_agg.get("packaging_cost", pd.Series(dtype=float)).sum()), 2),
                round(_num(date_agg.get("shipping_net_cost", date_agg.get("shipping_subsidy_cost", pd.Series(dtype=float))).sum()), 2),
                round(_num(date_agg.get("fb_ads_spend", pd.Series(dtype=float)).sum()), 2),
                round(_num(date_agg.get("google_ads_spend", pd.Series(dtype=float)).sum()), 2),
                round(_num(date_agg.get("fixed_daily_cost", pd.Series(dtype=float)).sum()), 2),
        ],
    }


def _period_switcher_html(period_switcher: Optional[dict]) -> str:
    switcher = period_switcher or {}
    options = switcher.get("options") or []
    if not options:
        return ""
    current_key = str(switcher.get("current_key") or "")
    current_range_en = escape(str(switcher.get("current_range_en") or ""))
    current_range_sk = escape(str(switcher.get("current_range_sk") or current_range_en))
    links = []
    for option in options:
        key = str(option.get("key") or "")
        active = "active" if key == current_key else ""
        href = escape(str(option.get("href") or "#"))
        label = escape(str(option.get("label") or key.upper()))
        links.append(
            f'<a class="pill global-period-link {active}" data-period-key="{escape(key)}" data-base-href="{href}" href="{href}">{label}</a>'
        )
    return (
        '<div class="panel controls global-period-panel">'
        '<div class="label"><span class="lang-en">Analytics window</span><span class="lang-sk hidden">Analyticke okno</span></div>'
        '<div class="period-summary">'
        f'<strong><span class="lang-en">{current_range_en}</span><span class="lang-sk hidden">{current_range_sk}</span></strong>'
        '<small><span class="lang-en">Applies to all chart sections below. Executive KPI deck keeps its own Daily / Weekly / Monthly switch.</span>'
        '<span class="lang-sk hidden">Plati pre vsetky sekcie s grafmi nizsie. Executive KPI deck ma vlastne prepinanie Denne / Tyzdenne / Mesacne.</span></small>'
        '</div>'
        '<div class="pill-row pill-row-wrap">' + "".join(links) + '</div>'
        '</div>'
    )


def _top_rows(frame: Optional[pd.DataFrame], columns: List[str], limit: int = 8) -> List[Dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    rows = []
    for _, row in frame.head(limit).iterrows():
        rows.append({key: _json_safe(row.get(key)) for key in columns})
    return rows


def _frame_rows(frame: Optional[pd.DataFrame], columns: List[str], limit: Optional[int] = None) -> List[Dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    rows: List[Dict[str, Any]] = []
    source = frame if limit is None else frame.head(limit)
    for _, row in source.iterrows():
        rows.append({key: _json_safe(row.get(key)) for key in columns})
    return rows


def _to_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if value is None:
        return pd.DataFrame()
    return pd.DataFrame(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _rename_keys(row: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    normalized = dict(row)
    for target, source in mapping.items():
        if normalized.get(target) is None and normalized.get(source) is not None:
            normalized[target] = normalized.get(source)
    return normalized


def _normalize_dow_effectiveness_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized = _rename_keys(
            row,
            {
                "day_name": "day_of_week",
                "avg_fb_spend": "fb_spend",
                "avg_orders": "orders",
                "avg_revenue": "revenue",
                "avg_profit": "profit",
            },
        )
        normalized_rows.append(normalized)
    return normalized_rows


def _normalize_attach_rate_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized = _rename_keys(
            row,
            {
                "anchor_item": "key_product",
                "attached_item": "attached_product",
                "anchor_orders": "key_orders",
            },
        )
        normalized_rows.append(normalized)
    return normalized_rows


def _normalize_daily_margin_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized = _rename_keys(
            row,
            {
                "pre_ad_contribution_margin_pct": "pre_ad_margin_pct",
            },
        )
        normalized_rows.append(normalized)
    return normalized_rows


def _normalize_sku_pareto_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        normalized = _rename_keys(
            row,
            {
                "cum_contribution_pct": "cum_contribution_share_pct",
            },
        )
        normalized_rows.append(normalized)
    return normalized_rows


def _source_entry(source_health: Optional[dict], key: str) -> Dict[str, Any]:
    return ((source_health or {}).get("sources") or {}).get(key) or {}


def _source_has_metric_coverage(source_health: Optional[dict], key: str) -> bool:
    entry = _source_entry(source_health, key)
    status = entry.get("status")
    if status not in {"ok", "manual"}:
        return False
    if entry.get("healthy") is False:
        return False

    # Distinguish a connected source from one that actually covers the selected window.
    # Example: Google Ads API can authenticate successfully while returning 0 active days.
    for coverage_key in ("active_days", "detailed_days", "orders", "hourly_rows", "campaign_count"):
        if coverage_key in entry:
            try:
                return float(entry.get(coverage_key) or 0) > 0
            except (TypeError, ValueError):
                return False

    return True


def _resolve_refund_summary(
    financial_metrics: Optional[dict],
    refunds_analysis: Optional[dict],
) -> Dict[str, Any]:
    registry = financial_metrics or {}
    summary = ((refunds_analysis or {}).get("summary") or {}).copy()
    resolved = {
        "refund_orders": summary.get("refund_orders"),
        "refund_rate_pct": summary.get("refund_rate_pct"),
        "refund_amount": summary.get("refund_amount"),
    }
    for key in resolved:
        if resolved[key] is None:
            resolved[key] = registry.get(key)
    return resolved


def _sanitize_dashboard_html(text: str) -> str:
    if not text:
        return text
    replacements = {
        "â‚¬": "&euro;",
        "Ă˘â€šÂ¬": "&euro;",
        "???": "&euro;",
        "Kalendarn?": "Kalendarne",
        "DennĂ˝": "Denny",
        "DennĂˇ": "Denna",
        "Kvalita zĂˇkaznĂ­kov": "Kvalita zakaznikov",
        "rozĹˇiruje": "rozsiruje",
        "koncentrĂˇciu": "koncentraciu",
        "Hlbsi": "Hlbsi",
        "DennĂ˝ source spend": "Denny source spend",
        "kvalita dĂˇt": "kvalita dat",
        "dĂ´vera": "dovera",
        "ProblĂ©my": "Problemy",
        "byĹĄ": "byt",
        "neĂşplnĂ©": "neuplne",
        "signĂˇlom": "signalom",
    }
    fixed = text
    for bad, good in replacements.items():
        fixed = fixed.replace(bad, good)
    return fixed


def _format_library_tile_value(value: Any, kind: str = "number", decimals: Optional[int] = None) -> str:
    numeric = _maybe_num(value)
    if numeric is None:
        return "N/A"
    if kind == "currency":
        precision = 2 if decimals is None else decimals
        return f"&euro;{numeric:,.{precision}f}"
    if kind == "percent":
        precision = 1 if decimals is None else decimals
        return f"{numeric:,.{precision}f}%"
    if kind == "multiple":
        precision = 2 if decimals is None else decimals
        return f"{numeric:,.{precision}f}x"
    if kind == "integer":
        return f"{int(round(numeric)):,}"
    if kind == "delta":
        precision = 4 if decimals is None else decimals
        return f"{numeric:+.{precision}f}"
    precision = 2 if decimals is None else decimals
    return f"{numeric:,.{precision}f}"


def _format_mini_value_html(value: Any, kind: str = "number", decimals: Optional[int] = None) -> str:
    return _format_library_tile_value(value, kind=kind, decimals=decimals)


def _library_tile_html(
    label_en: str,
    label_sk: str,
    value_html: str,
    tone: str = "neutral",
    note_en: str = "",
    note_sk: str = "",
) -> str:
    note_html = ""
    if note_en or note_sk:
        note_html = (
            '<div class="library-tile-note">'
            f'<span class="lang-en">{escape(note_en)}</span>'
            f'<span class="lang-sk hidden">{escape(note_sk or note_en)}</span>'
            '</div>'
        )
    return (
        f'<div class="library-tile tone-{escape(tone)}">'
        f'<small><span class="lang-en">{escape(label_en)}</span><span class="lang-sk hidden">{escape(label_sk)}</span></small>'
        f'<div class="library-tile-value">{value_html}</div>'
        f'{note_html}'
        '</div>'
    )


def _geo_confidence_badge_html(status: Any) -> str:
    normalized = str(status or "observe").strip().lower()
    if normalized not in {"ready", "observe", "ignore"}:
        normalized = "observe"
    labels = {
        "ready": ("Ready", "Pripravene"),
        "observe": ("Observe", "Sledovat"),
        "ignore": ("Ignore", "Ignorovat"),
    }
    en_label, sk_label = labels[normalized]
    return (
        f'<span class="confidence-badge {escape(normalized)}">'
        f'<span class="lang-en">{escape(en_label)}</span>'
        f'<span class="lang-sk hidden">{escape(sk_label)}</span>'
        '</span>'
    )


def generate_modern_dashboard(
    date_agg: pd.DataFrame,
    items_agg: pd.DataFrame,
    date_from: datetime,
    date_to: datetime,
    report_title: str = "BizniWeb reporting",
    day_of_week_analysis: Optional[pd.DataFrame] = None,
    week_of_month_analysis: Optional[pd.DataFrame] = None,
    day_of_month_analysis: Optional[pd.DataFrame] = None,
    weather_analysis: Optional[dict] = None,
    country_analysis: Optional[pd.DataFrame] = None,
    city_analysis: Optional[pd.DataFrame] = None,
    geo_profitability: Optional[dict] = None,
    product_margins: Optional[pd.DataFrame] = None,
    product_trends: Optional[pd.DataFrame] = None,
    new_vs_returning_revenue: Optional[dict] = None,
    refunds_analysis: Optional[dict] = None,
    customer_concentration: Optional[dict] = None,
    cohort_analysis: Optional[dict] = None,
    returning_customers_analysis: Optional[pd.DataFrame] = None,
    clv_return_time_analysis: Optional[pd.DataFrame] = None,
    order_size_distribution: Optional[pd.DataFrame] = None,
    item_combinations: Optional[pd.DataFrame] = None,
    advanced_dtc_metrics: Optional[dict] = None,
    day_hour_heatmap: Optional[pd.DataFrame] = None,
    b2b_analysis: Optional[pd.DataFrame] = None,
    order_status: Optional[pd.DataFrame] = None,
    ads_effectiveness: Optional[dict] = None,
    customer_email_segments: Optional[dict] = None,
    first_item_retention: Optional[dict] = None,
    same_item_repurchase: Optional[dict] = None,
    time_to_nth_by_first_item: Optional[dict] = None,
    sample_funnel_analysis: Optional[dict] = None,
    refill_cohort_analysis: Optional[dict] = None,
    fb_detailed_metrics: Optional[dict] = None,
    fb_campaigns: Optional[list] = None,
    cost_per_order: Optional[dict] = None,
    fb_hourly_stats: Optional[list] = None,
    fb_dow_stats: Optional[list] = None,
    ltv_by_date: Optional[pd.DataFrame] = None,
    consistency_checks: Optional[dict] = None,
    financial_metrics: Optional[dict] = None,
    cfo_kpi_payload: Optional[dict] = None,
    source_health: Optional[dict] = None,
    period_switcher: Optional[dict] = None,
    embedded_period_reports: Optional[dict] = None,
) -> str:
    raw_title = (report_title or "BizniWeb reporting").strip()
    title = escape(raw_title)
    brand_mark = escape((raw_title[:1] or "B").upper())
    series = _series(date_agg)
    kpi_payload = _kpis(cfo_kpi_payload)
    cost_mix = _cost_mix(date_agg)
    cities = _top_rows(
        city_analysis,
        ["city", "country", "orders", "revenue", "profit", "confidence_status", "confidence_label", "low_sample"],
        limit=8,
    )
    products = _top_rows(
        product_margins.sort_values(["profit", "revenue"], ascending=[False, False]) if product_margins is not None and not product_margins.empty else product_margins,
        ["product", "sku", "orders", "revenue", "profit", "margin_pct"],
        limit=8,
    )
    product_margin_chart_rows = _top_rows(
        product_margins.sort_values(["margin_pct", "profit"], ascending=[False, False]) if product_margins is not None and not product_margins.empty else product_margins,
        ["product", "margin_pct", "profit", "revenue"],
        limit=10,
    )
    trend_rows = _top_rows(
        product_trends.sort_values(["total_revenue", "revenue_growth_pct"], ascending=[False, False]) if product_trends is not None and not product_trends.empty else product_trends,
        ["product", "trend", "revenue_growth_pct", "qty_growth_pct", "total_revenue"],
        limit=10,
    )
    countries = _top_rows(
        country_analysis,
        ["country", "orders", "revenue", "confidence_status", "confidence_label", "low_sample"],
        limit=6,
    )
    geo_rows = _top_rows(
        (geo_profitability or {}).get("table"),
        [
            "country",
            "orders",
            "revenue",
            "contribution_profit",
            "contribution_profit_guarded",
            "contribution_margin_pct",
            "contribution_margin_pct_guarded",
            "fb_cpo",
            "fb_cpo_guarded",
            "confidence_status",
            "confidence_label",
            "confidence_score",
            "low_sample",
            "hide_economics",
        ],
        limit=6,
    )
    geo_qa = (((source_health or {}).get("qa") or {}).get("geo") or {})
    data_assertions_qa = (((source_health or {}).get("qa") or {}).get("data_assertions") or {})
    margin_stability_qa = (((source_health or {}).get("qa") or {}).get("margin_stability") or {})
    source_rows = list(((source_health or {}).get("sources") or {}).values())
    qa_rows = []
    for key, value in (((source_health or {}).get("qa") or {}).items()):
        if not isinstance(value, dict):
            continue
        qa_rows.append(
            {
                "key": key,
                "label": value.get("label") or key,
                "status": value.get("status") or "unknown",
                "healthy": value.get("healthy", value.get("status") == "ok"),
                "message": value.get("message") or "-",
            }
        )
    embedded_period_reports = embedded_period_reports or {}
    customer_top_rows = _top_rows(
        (customer_concentration or {}).get("top_10_customers"),
        ["customer", "orders", "revenue", "profit", "revenue_pct"],
        limit=8,
    )
    cohort_summary = (cohort_analysis or {}).get("summary", {}) if cohort_analysis else {}
    cohort_retention_rows = _top_rows(
        (cohort_analysis or {}).get("cohort_retention"),
        ["cohort", "retention_2nd_pct", "retention_3rd_pct", "retention_4th_pct", "retention_5th_pct"],
        limit=8,
    )
    cohort_order_frequency_rows = _frame_rows(
        (cohort_analysis or {}).get("order_frequency"),
        ["frequency", "customer_count", "total_orders", "customer_pct", "orders_pct"],
        limit=10,
    )
    cohort_time_between_rows = _frame_rows(
        (cohort_analysis or {}).get("time_between_orders"),
        ["time_bucket", "count", "percentage"],
        limit=10,
    )
    cohort_time_by_order_rows = _frame_rows(
        (cohort_analysis or {}).get("time_between_by_order_num"),
        ["transition", "count", "avg_days", "median_days", "min_days", "max_days"],
        limit=10,
    )
    cohort_time_to_nth_rows = _frame_rows(
        (cohort_analysis or {}).get("time_to_nth_order"),
        ["order_number", "customer_count", "avg_days_from_first", "median_days_from_first", "avg_days_from_prev", "avg_order_value"],
        limit=10,
    )
    cohort_revenue_by_order_rows = _frame_rows(
        (cohort_analysis or {}).get("revenue_by_order_num"),
        ["order_number", "avg_order_value", "total_revenue", "order_count", "avg_items_per_order", "avg_price_per_item"],
        limit=10,
    )
    mature_cohort_rows = _frame_rows(
        (cohort_analysis or {}).get("mature_cohort_retention"),
        ["cohort", "cohort_age_days", "retention_2nd_pct", "retention_3rd_pct", "retention_4th_pct", "retention_5th_pct"],
        limit=12,
    )

    customer_daily = (new_vs_returning_revenue or {}).get("daily")
    if customer_daily is not None and not getattr(customer_daily, "empty", True):
        customer_mix = {
            "dates": customer_daily["date"].astype(str).tolist(),
            "new": [round(_num(v), 2) for v in customer_daily["new_revenue"].tolist()],
            "returning": [round(_num(v), 2) for v in customer_daily["returning_revenue"].tolist()],
        }
    else:
        customer_mix = {"dates": [], "new": [], "returning": []}

    if day_of_week_analysis is not None and not day_of_week_analysis.empty:
        day_of_week = {
            "labels": day_of_week_analysis["day_name"].astype(str).tolist(),
            "orders": [round(_num(v), 2) for v in day_of_week_analysis["orders"].tolist()],
            "revenue": [round(_num(v), 2) for v in day_of_week_analysis["revenue"].tolist()],
            "profit": [round(_num(v), 2) for v in day_of_week_analysis.get("profit", pd.Series([0] * len(day_of_week_analysis))).tolist()],
            "aov": [round(_num(v), 2) for v in day_of_week_analysis.get("aov", pd.Series([0] * len(day_of_week_analysis))).tolist()],
            "fb_spend": [round(_num(v), 2) for v in day_of_week_analysis.get("fb_spend", pd.Series([0] * len(day_of_week_analysis))).tolist()],
        }
    else:
        day_of_week = {"labels": [], "orders": [], "revenue": [], "profit": [], "aov": [], "fb_spend": []}

    if week_of_month_analysis is not None and not week_of_month_analysis.empty:
        week_of_month = {
            "labels": week_of_month_analysis["week_label"].astype(str).tolist(),
            "revenue": [round(_num(v), 2) for v in week_of_month_analysis["revenue"].tolist()],
            "profit": [round(_num(v), 2) for v in week_of_month_analysis["profit"].tolist()],
            "avg_daily_revenue": [round(_num(v), 2) for v in week_of_month_analysis["avg_daily_revenue"].tolist()],
            "avg_daily_profit": [round(_num(v), 2) for v in week_of_month_analysis["avg_daily_profit"].tolist()],
        }
    else:
        week_of_month = {"labels": [], "revenue": [], "profit": [], "avg_daily_revenue": [], "avg_daily_profit": []}

    if day_of_month_analysis is not None and not day_of_month_analysis.empty:
        day_of_month = {
            "labels": day_of_month_analysis["day_label"].astype(str).tolist(),
            "orders": [round(_num(v), 2) for v in day_of_month_analysis["orders"].tolist()],
            "revenue": [round(_num(v), 2) for v in day_of_month_analysis["revenue"].tolist()],
            "avg_revenue": [round(_num(v), 2) for v in day_of_month_analysis["avg_revenue_per_occurrence"].tolist()],
            "avg_profit": [round(_num(v), 2) for v in day_of_month_analysis["avg_profit_per_occurrence"].tolist()],
        }
    else:
        day_of_month = {"labels": [], "orders": [], "revenue": [], "avg_revenue": [], "avg_profit": []}

    weather_daily = (weather_analysis or {}).get("daily")
    weather_bucket = (weather_analysis or {}).get("bucket_summary")
    if weather_daily is not None and not getattr(weather_daily, "empty", True):
        weather_payload = {
            "dates": pd.to_datetime(weather_daily["date"]).dt.strftime("%Y-%m-%d").tolist(),
            "revenue": [round(_num(v), 2) for v in weather_daily["total_revenue"].tolist()],
            "profit": [round(_num(v), 2) for v in weather_daily["net_profit"].tolist()],
            "precipitation": [round(_num(v), 2) for v in weather_daily["precipitation_sum"].tolist()],
            "bad_score": [round(_num(v), 2) for v in weather_daily["weather_bad_score"].tolist()],
        }
    else:
        weather_payload = {"dates": [], "revenue": [], "profit": [], "precipitation": [], "bad_score": []}

    if weather_bucket is not None and not getattr(weather_bucket, "empty", True):
        weather_bucket_payload = {
            "labels": weather_bucket["weather_bucket"].astype(str).tolist(),
            "revenue_delta": [round(_num(v), 2) for v in weather_bucket["revenue_vs_weekday_baseline"].tolist()],
            "profit_delta": [round(_num(v), 2) for v in weather_bucket["profit_vs_weekday_baseline"].tolist()],
        }
    else:
        weather_bucket_payload = {"labels": [], "revenue_delta": [], "profit_delta": []}

    refunds_daily = (refunds_analysis or {}).get("daily")
    if refunds_daily is not None and not getattr(refunds_daily, "empty", True):
        refunds_payload = {
            "dates": pd.to_datetime(refunds_daily["date"]).dt.strftime("%Y-%m-%d").tolist(),
            "rate": [round(_num(v), 2) for v in refunds_daily["refund_rate_pct"].tolist()],
            "amount": [round(_num(v), 2) for v in refunds_daily["refund_amount"].tolist()],
        }
    else:
        refunds_payload = {"dates": [], "rate": [], "amount": []}

    if returning_customers_analysis is not None and not getattr(returning_customers_analysis, "empty", True):
        returning_payload = {
            "labels": returning_customers_analysis["week_start"].astype(str).tolist(),
            "returning_pct": [round(_num(v), 2) for v in returning_customers_analysis["returning_percentage"].tolist()],
            "new_pct": [round(_num(v), 2) for v in returning_customers_analysis["new_percentage"].tolist()],
            "returning_orders": [round(_num(v), 2) for v in returning_customers_analysis["returning_orders"].tolist()],
            "new_orders": [round(_num(v), 2) for v in returning_customers_analysis["new_orders"].tolist()],
            "unique_customers": [round(_num(v), 2) for v in returning_customers_analysis["unique_customers"].tolist()],
        }
    else:
        returning_payload = {"labels": [], "returning_pct": [], "new_pct": [], "returning_orders": [], "new_orders": [], "unique_customers": []}

    if clv_return_time_analysis is not None and not getattr(clv_return_time_analysis, "empty", True):
        clv_payload = {
            "labels": clv_return_time_analysis["week_start"].astype(str).tolist(),
            "avg_clv": [round(_num(v), 2) for v in clv_return_time_analysis["avg_clv"].tolist()],
            "cac": [round(_num(v), 2) for v in clv_return_time_analysis["cac"].tolist()],
            "ltv_cac_ratio": [round(_num(v), 2) for v in clv_return_time_analysis["ltv_cac_ratio"].tolist()],
            "avg_return_time_days": [round(_num(v), 2) for v in clv_return_time_analysis["avg_return_time_days"].fillna(0).tolist()],
            "cumulative_avg_clv": [round(_num(v), 2) for v in clv_return_time_analysis["cumulative_avg_clv"].tolist()],
            "cumulative_avg_cac": [round(_num(v), 2) for v in clv_return_time_analysis["cumulative_avg_cac"].tolist()],
        }
    else:
        clv_payload = {"labels": [], "avg_clv": [], "cac": [], "ltv_cac_ratio": [], "avg_return_time_days": [], "cumulative_avg_clv": [], "cumulative_avg_cac": []}

    if order_size_distribution is not None and not getattr(order_size_distribution, "empty", True):
        order_size_payload = {
            "labels": pd.to_datetime(order_size_distribution["purchase_date_only"]).dt.strftime("%Y-%m-%d").tolist(),
            "one": [round(_num(v), 2) for v in order_size_distribution.get("1 item", pd.Series([0] * len(order_size_distribution))).tolist()],
            "two": [round(_num(v), 2) for v in order_size_distribution.get("2 items", pd.Series([0] * len(order_size_distribution))).tolist()],
            "three": [round(_num(v), 2) for v in order_size_distribution.get("3 items", pd.Series([0] * len(order_size_distribution))).tolist()],
            "four": [round(_num(v), 2) for v in order_size_distribution.get("4 items", pd.Series([0] * len(order_size_distribution))).tolist()],
            "five_plus": [round(_num(v), 2) for v in order_size_distribution.get("5+ items", pd.Series([0] * len(order_size_distribution))).tolist()],
        }
    else:
        order_size_payload = {"labels": [], "one": [], "two": [], "three": [], "four": [], "five_plus": []}

    ltv_payload = {"labels": [], "ltv_revenue": []}
    if ltv_by_date is not None and not getattr(ltv_by_date, "empty", True):
        ltv_payload = {
            "labels": ltv_by_date["date"].astype(str).tolist(),
            "ltv_revenue": [round(_num(v), 2) for v in ltv_by_date["ltv_revenue"].tolist()],
            "customers_acquired": [round(_num(v), 2) for v in ltv_by_date.get("customers_acquired", pd.Series([0] * len(ltv_by_date))).tolist()],
            "lifetime_orders": [round(_num(v), 2) for v in ltv_by_date.get("total_lifetime_orders", pd.Series([0] * len(ltv_by_date))).tolist()],
        }

    fb_daily_payload = {"dates": [], "spend": [], "impressions": [], "clicks": [], "ctr": [], "cpc": [], "cpm": [], "reach": []}
    if fb_detailed_metrics:
        sorted_rows = sorted(fb_detailed_metrics.items(), key=lambda item: item[0])
        fb_daily_payload = {
            "dates": [str(k) for k, _ in sorted_rows],
            "spend": [round(_num(v.get("spend")), 2) for _, v in sorted_rows],
            "impressions": [round(_num(v.get("impressions")), 2) for _, v in sorted_rows],
            "clicks": [round(_num(v.get("clicks")), 2) for _, v in sorted_rows],
            "ctr": [round(_num(v.get("ctr")), 2) for _, v in sorted_rows],
            "cpc": [round(_num(v.get("cpc")), 2) for _, v in sorted_rows],
            "cpm": [round(_num(v.get("cpm")), 2) for _, v in sorted_rows],
            "reach": [round(_num(v.get("reach")), 2) for _, v in sorted_rows],
        }

    fb_campaign_rows = []
    if fb_campaigns:
        campaign_frame = pd.DataFrame(fb_campaigns)
        if not campaign_frame.empty:
            campaign_frame = campaign_frame.sort_values("spend", ascending=False)
            fb_campaign_rows = _frame_rows(
                campaign_frame,
                ["campaign_name", "spend", "clicks", "impressions", "ctr", "cpc", "cpm", "reach", "platform_conversions", "conversions", "cost_per_platform_conversion", "cost_per_conversion"],
                limit=12,
            )

    cpo_daily = _frame_rows(_to_frame((cost_per_order or {}).get("daily_cpo")), ["date", "orders", "fb_spend", "revenue", "cpo", "roas"], limit=120)
    weekly_cpo = _frame_rows(_to_frame((cost_per_order or {}).get("weekly_cpo")), ["week_start", "orders", "fb_spend", "cpo"], limit=60)
    campaign_cpo = _frame_rows(_to_frame((cost_per_order or {}).get("campaign_attribution")), ["campaign_name", "spend", "attributed_orders_est", "estimated_orders", "cost_per_attributed_order", "estimated_cpo", "estimated_revenue", "estimated_roas", "attribution_method"], limit=12)
    hourly_orders = _frame_rows(_to_frame((cost_per_order or {}).get("hourly_orders")), ["hour", "orders", "revenue"], limit=24)
    fb_hourly_payload = _frame_rows(_to_frame(fb_hourly_stats), ["hour", "spend", "clicks", "impressions", "ctr", "cpc"], limit=24)
    fb_dow_payload = _frame_rows(_to_frame(fb_dow_stats), ["day_of_week", "total_spend", "total_clicks", "ctr", "cpc", "cpm"], limit=7)

    ads_daily = _to_frame((ads_effectiveness or {}).get("daily_data"))
    if not ads_daily.empty:
        ads_effectiveness_payload = {
            "labels": ads_daily["date"].astype(str).tolist(),
            "orders": [round(_num(v), 2) for v in ads_daily["orders"].tolist()],
            "revenue": [round(_num(v), 2) for v in ads_daily["revenue"].tolist()],
            "fb_spend": [round(_num(v), 2) for v in ads_daily["fb_spend"].tolist()],
            "google_spend": [round(_num(v), 2) for v in ads_daily["google_spend"].tolist()],
            "profit": [round(_num(v), 2) for v in ads_daily["profit"].tolist()],
        }
    else:
        ads_effectiveness_payload = {"labels": [], "orders": [], "revenue": [], "fb_spend": [], "google_spend": [], "profit": []}
    spend_effectiveness_rows = _frame_rows(_to_frame((ads_effectiveness or {}).get("spend_effectiveness")), ["spend_range", "avg_orders", "avg_revenue", "avg_spend", "avg_profit", "roas"], limit=20)
    dow_effectiveness_rows = _normalize_dow_effectiveness_rows(
        _frame_rows(
            _to_frame((ads_effectiveness or {}).get("dow_effectiveness")),
            ["day_name", "day_of_week", "avg_orders", "orders", "avg_revenue", "revenue", "avg_profit", "profit", "avg_fb_spend", "fb_spend"],
            limit=7,
        )
    )

    advanced_summary = (advanced_dtc_metrics or {}).get("summary", {}) if advanced_dtc_metrics else {}
    bundle_accessory_model = (advanced_dtc_metrics or {}).get("bundle_accessory_model", {}) if advanced_dtc_metrics else {}
    basket_contribution_rows = _frame_rows((advanced_dtc_metrics or {}).get("basket_contribution"), ["basket_size", "orders", "revenue", "pre_ad_contribution", "contribution_per_order", "contribution_margin_pct"], limit=10)
    sku_pareto_rows = _normalize_sku_pareto_rows(
        _frame_rows((advanced_dtc_metrics or {}).get("sku_pareto"), ["sku", "product", "orders", "revenue", "pre_ad_contribution", "cum_contribution_pct", "cum_contribution_share_pct"], limit=12)
    )
    attach_rate_rows = _normalize_attach_rate_rows(
        _frame_rows((advanced_dtc_metrics or {}).get("attach_rate"), ["anchor_item", "key_product", "attached_item", "attached_product", "anchor_orders", "key_orders", "attached_orders", "attach_rate_pct"], limit=12)
    )
    daily_margin_rows = _normalize_daily_margin_rows(
        _frame_rows((advanced_dtc_metrics or {}).get("daily_margin"), ["date", "pre_ad_contribution_margin_pct", "pre_ad_margin_pct"], limit=120)
    )
    payday_window_rows = _frame_rows((advanced_dtc_metrics or {}).get("payday_window"), ["window", "orders", "revenue", "profit", "avg_daily_revenue", "avg_daily_profit"], limit=20)
    cohort_payback_rows = _frame_rows((advanced_dtc_metrics or {}).get("cohort_payback"), ["cohort_month", "new_customers", "cohort_cac", "recovery_rate_pct", "avg_payback_days", "median_payback_days"], limit=24)
    cohort_unit_economics_rows = _frame_rows(
        (advanced_dtc_metrics or {}).get("cohort_unit_economics"),
        [
            "cohort_month",
            "new_customers",
            "cohort_age_days",
            "cohort_paid_spend",
            "cohort_blended_cac",
            "revenue_ltv_30d",
            "revenue_ltv_60d",
            "revenue_ltv_90d",
            "revenue_ltv_180d",
            "contribution_ltv_30d",
            "contribution_ltv_60d",
            "contribution_ltv_90d",
            "contribution_ltv_180d",
            "contribution_ltv_cac_30d",
            "contribution_ltv_cac_60d",
            "contribution_ltv_cac_90d",
            "contribution_ltv_cac_180d",
            "payback_recovery_30d_pct",
            "payback_recovery_60d_pct",
            "payback_recovery_90d_pct",
            "payback_recovery_180d_pct",
        ],
        limit=24,
    )
    bundle_accessory_pair_rows = _frame_rows(
        (bundle_accessory_model or {}).get("pair_rows"),
        [
            "anchor_group_label",
            "accessory_group_label",
            "anchor_orders",
            "attached_orders",
            "attach_rate_pct",
            "avg_order_value_with_accessory",
            "avg_order_value_without_accessory",
            "avg_pre_ad_contribution_with_accessory",
            "avg_pre_ad_contribution_without_accessory",
            "revenue_uplift_per_order",
            "contribution_uplift_per_order",
        ],
        limit=18,
    )
    bundle_accessory_device_rows = _frame_rows(
        (bundle_accessory_model or {}).get("device_family_rows"),
        [
            "anchor_group_label",
            "anchor_orders",
            "anchor_avg_order_value",
            "anchor_avg_pre_ad_contribution",
            "best_accessory_group_label",
            "best_attach_rate_pct",
            "best_contribution_uplift_per_order",
            "best_revenue_uplift_per_order",
        ],
        limit=12,
    )
    bundle_accessory_group_rows = _frame_rows(
        (bundle_accessory_model or {}).get("accessory_group_rows"),
        [
            "accessory_group_label",
            "covered_anchor_groups",
            "pair_rows",
            "attached_orders_total",
            "weighted_attach_rate_pct",
            "avg_contribution_uplift_per_order",
            "best_anchor_group_label",
        ],
        limit=10,
    )
    acquisition_family_cube = (advanced_dtc_metrics or {}).get("acquisition_product_family_cube", {}) if advanced_dtc_metrics else {}
    acquisition_family_cube_rows = _frame_rows(
        (acquisition_family_cube or {}).get("cube_rows"),
        [
            "source_proxy_key",
            "source_proxy_label",
            "product_family_key",
            "product_family_label",
            "new_customers",
            "first_order_revenue",
            "first_order_contribution",
            "first_order_aov",
            "first_order_contribution_per_order",
            "repeat_60d_rate_pct",
            "repeat_90d_rate_pct",
            "revenue_ltv_90d",
            "contribution_ltv_90d",
            "revenue_ltv_90d_per_customer",
            "contribution_ltv_90d_per_customer",
        ],
        limit=40,
    )
    acquisition_family_source_rows = _frame_rows(
        (acquisition_family_cube or {}).get("source_rows"),
        [
            "source_proxy_key",
            "source_proxy_label",
            "new_customers",
            "revenue_ltv_90d",
            "contribution_ltv_90d",
            "contribution_ltv_90d_per_customer",
        ],
        limit=10,
    )
    acquisition_family_family_rows = _frame_rows(
        (acquisition_family_cube or {}).get("family_rows"),
        [
            "product_family_key",
            "product_family_label",
            "new_customers",
            "first_order_revenue",
            "contribution_ltv_90d",
            "repeat_90d_rate_pct",
            "contribution_ltv_90d_per_customer",
        ],
        limit=20,
    )

    heatmap_rows = _frame_rows(day_hour_heatmap, ["day_name", "hour", "orders"], limit=None)
    b2b_rows = _frame_rows(b2b_analysis, ["customer_type", "orders", "revenue", "profit", "unique_customers", "aov", "orders_pct", "revenue_pct"], limit=10)
    order_status_rows = _frame_rows(order_status, ["status", "orders", "revenue", "orders_pct"], limit=20)

    item_retention_rows = _frame_rows((first_item_retention or {}).get("item_retention"), ["item_name", "first_order_customers", "retention_2nd_pct", "retention_3rd_pct", "avg_orders_per_customer"], limit=12)
    same_item_rows = _frame_rows((same_item_repurchase or {}).get("item_repurchase"), ["item_name", "unique_customers", "repurchase_2x_pct", "repurchase_3x_pct", "avg_days_between_repurchase"], limit=12)
    same_item_frequency_rows = _frame_rows((same_item_repurchase or {}).get("customer_item_frequency"), ["purchase_frequency", "customer_count", "percentage"], limit=12)
    time_to_nth_rows = _frame_rows((time_to_nth_by_first_item or {}).get("time_to_nth_by_item"), ["item_name", "first_order_customers", "avg_days_to_2nd", "median_days_to_2nd", "avg_days_to_3nd", "avg_days_to_4nd", "avg_days_to_5nd"], limit=12)
    sample_funnel_summary = (sample_funnel_analysis or {}).get("summary", {}) if sample_funnel_analysis else {}
    sample_funnel_window_rows = _frame_rows(
        (sample_funnel_analysis or {}).get("window_conversion"),
        [
            "window_days",
            "cohort_customers",
            "repeat_customers",
            "repeat_pct",
            "fullsize_any_customers",
            "fullsize_any_pct",
            "fullsize_200_customers",
            "fullsize_200_pct",
            "fullsize_500_customers",
            "fullsize_500_pct",
        ],
        limit=None,
    )
    sample_funnel_entry_rows = _frame_rows(
        (sample_funnel_analysis or {}).get("entry_product_conversion"),
        [
            "item_name",
            "item_sku",
            "entry_customers",
            "repeat_30d_pct",
            "fullsize_any_30d_pct",
            "fullsize_any_60d_pct",
            "fullsize_200_60d_pct",
            "fullsize_500_60d_pct",
        ],
        limit=12,
    )
    refill_cohort_summary = (refill_cohort_analysis or {}).get("summary", {}) if refill_cohort_analysis else {}
    refill_cohort_bucket_rows = _frame_rows(
        (refill_cohort_analysis or {}).get("bucket_rows"),
        [
            "entry_bucket_label",
            "customers",
            "second_orders",
            "refill_60d_pct",
            "refill_90d_pct",
            "avg_days_to_2nd",
            "second_order_aov",
        ],
        limit=12,
    )
    refill_cohort_window_rows = _frame_rows(
        (refill_cohort_analysis or {}).get("window_rows"),
        [
            "entry_bucket_key",
            "entry_bucket_label",
            "window_days",
            "customers",
            "refill_pct",
        ],
        limit=None,
    )
    refill_cohort_rows = _frame_rows(
        (refill_cohort_analysis or {}).get("cohort_rows"),
        [
            "cohort_month",
            "entry_bucket_key",
            "entry_bucket_label",
            "customers",
            "refill_60d_pct",
            "refill_90d_pct",
            "avg_days_to_2nd",
            "second_order_aov",
        ],
        limit=None,
    )
    direct_assisted = (advanced_dtc_metrics or {}).get("vevo_direct_assisted_profitability", {}) if advanced_dtc_metrics else {}
    direct_assisted_entry_rows = _frame_rows(
        (direct_assisted or {}).get("entry_rows"),
        [
            "entry_product",
            "customers",
            "direct_cm3_per_customer",
            "downstream_cm3_90d_per_customer",
            "total_cm3_90d_per_customer",
            "assisted_share_90d_pct",
            "repeat_90d_pct",
        ],
        limit=12,
    )
    direct_assisted_window_rows = _frame_rows(
        (direct_assisted or {}).get("window_rows"),
        [
            "window_days",
            "customers",
            "repeat_customers",
            "repeat_pct",
            "downstream_cm3_per_customer",
            "total_cm3_per_customer",
            "assisted_share_pct",
        ],
        limit=None,
    )
    crm_funnel = (advanced_dtc_metrics or {}).get("vevo_crm_funnel_kpis", {}) if advanced_dtc_metrics else {}
    crm_funnel_rows = _frame_rows(
        (crm_funnel or {}).get("segment_rows"),
        [
            "segment",
            "count",
            "priority",
            "goal_label",
            "target_metric_key",
            "baseline_value",
            "baseline_kind",
            "send_timing_en",
            "send_timing_sk",
        ],
        limit=12,
    )
    scent_size = (advanced_dtc_metrics or {}).get("vevo_scent_size_refill_matrix", {}) if advanced_dtc_metrics else {}
    scent_same_rows = _frame_rows(
        (scent_size or {}).get("same_scent_rows"),
        [
            "scent_label",
            "sample_customers",
            "sample_to_200_pct",
            "sample_to_500_pct",
            "200_to_500_pct",
            "500_repeat_pct",
        ],
        limit=12,
    )
    scent_migration_rows = _frame_rows(
        (scent_size or {}).get("migration_rows"),
        [
            "base_scent_label",
            "sample_customers",
            "cross_scent_customers",
            "cross_scent_pct",
            "avg_days_to_cross_scent",
            "top_target_scent",
        ],
        limit=12,
    )
    bundle_recommender = (advanced_dtc_metrics or {}).get("vevo_bundle_recommender", {}) if advanced_dtc_metrics else {}
    bundle_recommender_rows = _frame_rows(
        (bundle_recommender or {}).get("recommendation_rows"),
        [
            "anchor_family_label",
            "attached_family_label",
            "anchor_orders",
            "attach_rate_pct",
            "cm2_uplift_per_order",
            "recommendation_score",
        ],
        limit=18,
    )
    bundle_recommender_anchor_rows = _frame_rows(
        (bundle_recommender or {}).get("anchor_rows"),
        [
            "anchor_family_label",
            "anchor_orders",
            "top_attached_family_label",
            "top_attach_rate_pct",
            "top_cm2_uplift_per_order",
        ],
        limit=12,
    )
    promo_discount = (advanced_dtc_metrics or {}).get("promo_discount_quality", {}) if advanced_dtc_metrics else {}
    promo_discount_rows = _frame_rows(
        (promo_discount or {}).get("bucket_rows"),
        [
            "bucket",
            "orders",
            "revenue",
            "detected_discount_net",
            "discount_penetration_pct",
            "avg_discount_per_order",
            "cm2_margin_pct",
        ],
        limit=12,
    )
    combinations_rows = _frame_rows(item_combinations, ["combination_size", "combination", "count", "price"], limit=12)

    segment_rows = []
    for key, value in (customer_email_segments or {}).items():
        if not isinstance(value, dict):
            continue
        segment_rows.append({
            "segment": key,
            "count": int(_num(value.get("count"))),
            "priority": int(_num(value.get("priority"))),
            "description_en": value.get("description_en") or value.get("description") or key,
            "description_sk": value.get("description") or value.get("description_en") or key,
            "timing_en": value.get("send_timing_en") or value.get("send_timing") or "-",
            "timing_sk": value.get("send_timing") or value.get("send_timing_en") or "-",
        })
    segment_rows = sorted(segment_rows, key=lambda row: (row["priority"], -row["count"]))

    consistency_payload = {
        "roas_delta": _maybe_num((consistency_checks or {}).get("roas_check_delta")),
        "margin_delta": _maybe_num((consistency_checks or {}).get("margin_check_delta")),
        "cac_delta": _maybe_num((consistency_checks or {}).get("cac_check_delta")),
        "roas_ok": bool((consistency_checks or {}).get("roas_ok")),
        "margin_ok": bool((consistency_checks or {}).get("margin_ok")),
        "cac_ok": bool((consistency_checks or {}).get("cac_ok")),
    }
    financial_payload = {
        "payback_weekly_labels": list((financial_metrics or {}).get("payback_weekly_labels") or []),
        "payback_weekly_orders": [round(_num(v), 2) for v in list((financial_metrics or {}).get("payback_weekly_orders") or [])],
        "current_fb_cac": _maybe_num((financial_metrics or {}).get("current_fb_cac")),
        "blended_cac": _maybe_num((financial_metrics or {}).get("blended_cac")),
        "avg_customer_ltv": _maybe_num((financial_metrics or {}).get("avg_customer_ltv")),
        "ltv_cac_ratio": _maybe_num((financial_metrics or {}).get("ltv_cac_ratio")),
        "payback_orders": _maybe_num((financial_metrics or {}).get("payback_orders")),
        "avg_return_cycle_days": _maybe_num((financial_metrics or {}).get("avg_return_cycle_days")),
    }

    total_revenue = round(_num(date_agg["total_revenue"].sum()), 2)
    total_profit = round(_num(date_agg["net_profit"].sum()), 2)
    total_orders = int(round(_num(date_agg["unique_orders"].sum())))
    total_fb_ads = round(_num(date_agg.get("fb_ads_spend", pd.Series(dtype=float)).sum()), 2)
    total_google_ads = round(_num(date_agg.get("google_ads_spend", pd.Series(dtype=float)).sum()), 2)
    total_ads = round(total_fb_ads + total_google_ads, 2)
    total_product_cost = round(_num(date_agg.get("product_expense", pd.Series(dtype=float)).sum()), 2)
    total_packaging_cost = round(_num(date_agg.get("packaging_cost", pd.Series(dtype=float)).sum()), 2)
    total_shipping_subsidy = round(_num(date_agg.get("shipping_net_cost", date_agg.get("shipping_subsidy_cost", pd.Series(dtype=float))).sum()), 2)
    total_fixed_overhead = round(_num(date_agg.get("fixed_daily_cost", pd.Series(dtype=float)).sum()), 2)
    total_costs = round(_num(date_agg.get("total_cost", pd.Series(dtype=float)).sum()), 2)
    total_items = int(round(_num(date_agg.get("total_items", pd.Series(dtype=float)).sum())))
    blended_roas = round((total_revenue / total_ads) if total_ads > 0 else 0.0, 2)
    top_city = str(cities[0].get("city") or "-") if cities else "-"
    top_product = str(products[0].get("product") or "-") if products else "-"
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    active_days = max(len(date_agg.index), 1)
    avg_daily_revenue = round(total_revenue / active_days, 2)
    avg_daily_profit = round(total_profit / active_days, 2)
    avg_fb_cost_per_order = round((total_fb_ads / total_orders) if total_orders > 0 else 0.0, 2)
    refund_summary = _resolve_refund_summary(financial_metrics, refunds_analysis)
    google_source_available = _source_has_metric_coverage(source_health, "google_ads")
    google_cpo_value = _maybe_num((cost_per_order or {}).get("google_cpo")) if google_source_available else None
    ads_correlation_source = ((ads_effectiveness or {}).get("correlations") or {})

    avg_customer_ltv = _maybe_num((financial_metrics or {}).get("avg_customer_ltv"))
    if avg_customer_ltv is None:
        avg_customer_ltv = _maybe_num((financial_metrics or {}).get("revenue_per_customer"))

    returning_customer_rate = _maybe_num((customer_concentration or {}).get("repeat_purchase_rate"))
    if returning_customer_rate is None:
        returning_customer_rate = _maybe_num(cohort_summary.get("repeat_rate_pct"))

    break_even_cac = _maybe_num((financial_metrics or {}).get("break_even_cac"))
    current_fb_cac = _maybe_num((financial_metrics or {}).get("current_fb_cac"))
    shell_pre_ad_per_order = _maybe_num((financial_metrics or {}).get("pre_ad_contribution_per_order"))
    shell_payback_orders = _maybe_num((financial_metrics or {}).get("payback_orders"))
    shell_contribution_ltv_cac = _maybe_num((financial_metrics or {}).get("contribution_ltv_cac"))
    cm1_profit = _maybe_num((financial_metrics or {}).get("cm1_profit"))
    cm1_margin_pct = _maybe_num((financial_metrics or {}).get("cm1_margin_pct"))
    cm1_profit_per_order = _maybe_num((financial_metrics or {}).get("cm1_profit_per_order"))
    cm1_profit_per_customer = _maybe_num((financial_metrics or {}).get("cm1_profit_per_customer"))
    cm2_profit = _maybe_num((financial_metrics or {}).get("cm2_profit"))
    cm2_margin_pct = _maybe_num((financial_metrics or {}).get("cm2_margin_pct"))
    cm2_profit_per_order = _maybe_num((financial_metrics or {}).get("cm2_profit_per_order"))
    cm3_profit = _maybe_num((financial_metrics or {}).get("cm3_profit"))
    cm3_margin_pct = _maybe_num((financial_metrics or {}).get("cm3_margin_pct"))
    cm3_profit_per_order = _maybe_num((financial_metrics or {}).get("cm3_profit_per_order"))
    cm_taxonomy_note = str((financial_metrics or {}).get("cm_taxonomy_note") or "")
    cac_break_even_ratio = None
    if break_even_cac not in (None, 0) and current_fb_cac is not None:
        cac_break_even_ratio = current_fb_cac / break_even_cac
    roi_total_pct = round((total_profit / total_costs * 100), 1) if total_costs > 0 else None
    revenue_ltv_cac = None
    if avg_customer_ltv not in (None, 0) and current_fb_cac not in (None, 0):
        revenue_ltv_cac = avg_customer_ltv / current_fb_cac

    repeat_purchase_rate = _maybe_num((customer_concentration or {}).get("repeat_purchase_rate"))
    if repeat_purchase_rate is None:
        repeat_purchase_rate = _maybe_num(cohort_summary.get("repeat_rate_pct"))

    ads_correlations = {
        **ads_correlation_source,
        "spend_orders_correlation": _maybe_num(
            ads_correlation_source.get("spend_orders_correlation", ads_correlation_source.get("total_ads_orders"))
        ),
        "spend_revenue_correlation": _maybe_num(
            ads_correlation_source.get("spend_revenue_correlation", ads_correlation_source.get("total_ads_revenue"))
        ),
        "spend_profit_correlation": _maybe_num(
            ads_correlation_source.get("spend_profit_correlation")
        ),
    }

    shipping_tone = "positive" if total_shipping_subsidy < 0 else ("neutral" if total_shipping_subsidy == 0 else "negative")

    library_tiles = [
        {"en": "Total revenue (net)", "sk": "Celkove trzby (net)", "value": total_revenue, "kind": "currency", "tone": "neutral"},
        {"en": "Product costs", "sk": "Naklady na produkty", "value": total_product_cost, "kind": "currency", "tone": "negative"},
        {"en": "Packaging costs", "sk": "Naklady na balenie", "value": total_packaging_cost, "kind": "currency", "tone": "negative"},
        {"en": "Net shipping", "sk": "Ciste shipping", "value": total_shipping_subsidy, "kind": "currency", "tone": shipping_tone, "note_en": "positive = cost, negative = shipping profit", "note_sk": "kladne = naklad, zaporne = shipping zisk"},
        {"en": "Fixed overhead", "sk": "Fixny overhead", "value": total_fixed_overhead, "kind": "currency", "tone": "negative"},
        {"en": "Facebook ads", "sk": "Facebook reklama", "value": total_fb_ads, "kind": "currency", "tone": "negative"},
        {"en": "Google ads", "sk": "Google reklama", "value": total_google_ads, "kind": "currency", "tone": "negative"},
        {"en": "Total costs", "sk": "Celkove naklady", "value": total_costs, "kind": "currency", "tone": "negative"},
        {"en": "Net profit", "sk": "Cisty zisk", "value": total_profit, "kind": "currency", "tone": "positive"},
        {"en": "Avg daily revenue", "sk": "Priemerna denna trzba", "value": avg_daily_revenue, "kind": "currency", "tone": "neutral"},
        {"en": "Avg daily profit/loss", "sk": "Priemerny denny zisk/strata", "value": avg_daily_profit, "kind": "currency", "tone": "positive"},
        {"en": "ROI", "sk": "ROI", "value": roi_total_pct, "kind": "percent", "tone": "positive"},
        {"en": "Total orders", "sk": "Celkove objednavky", "value": total_orders, "kind": "integer", "tone": "neutral"},
        {"en": "Total items", "sk": "Celkove kusy", "value": total_items, "kind": "integer", "tone": "neutral"},
        {"en": "Avg order value", "sk": "Priemerna hodnota objednavky", "value": round((total_revenue / total_orders), 2) if total_orders > 0 else 0.0, "kind": "currency", "tone": "neutral"},
        {"en": "Avg FB cost/order", "sk": "Priemer FB naklad/objednavka", "value": avg_fb_cost_per_order, "kind": "currency", "tone": "negative"},
        {"en": "Returning customers", "sk": "Vracajuci sa zakaznici", "value": returning_customer_rate, "kind": "percent", "tone": "positive"},
        {"en": "Avg customer LTV (revenue)", "sk": "Priemerne customer LTV (trzby)", "value": avg_customer_ltv, "kind": "currency", "tone": "neutral"},
        {"en": "Customer acq. cost", "sk": "Naklad na akviziciu zakaznika", "value": current_fb_cac, "kind": "currency", "tone": "negative"},
        {"en": "Revenue LTV/CAC", "sk": "Revenue LTV/CAC", "value": revenue_ltv_cac, "kind": "multiple", "tone": "positive"},
        {"en": "ROAS (all ads)", "sk": "ROAS (vsetky reklamy)", "value": _maybe_num((financial_metrics or {}).get("roas")) if _maybe_num((financial_metrics or {}).get("roas")) is not None else blended_roas, "kind": "multiple", "tone": "positive"},
        {"en": "MER", "sk": "MER", "value": _maybe_num((financial_metrics or {}).get("mer")), "kind": "multiple", "tone": "positive"},
        {"en": "Revenue/customer (net)", "sk": "Trzby/zakaznik (net)", "value": _maybe_num((financial_metrics or {}).get("revenue_per_customer")), "kind": "currency", "tone": "neutral"},
        {"en": "Orders/customer", "sk": "Objednavky/zakaznik", "value": _maybe_num((financial_metrics or {}).get("orders_per_customer")), "kind": "number", "decimals": 2, "tone": "neutral"},
        {"en": "Company profit margin", "sk": "Firemna ziskova marza", "value": _maybe_num((financial_metrics or {}).get("company_profit_margin_pct")), "kind": "percent", "tone": "positive", "note_en": "Includes fixed cost", "note_sk": "Vrata fixnych nakladov"},
        {"en": "Product gross margin", "sk": "Hruba marza produktu", "value": _maybe_num((financial_metrics or {}).get("product_gross_margin_pct")), "kind": "percent", "tone": "positive"},
        {"en": "CM1 profit", "sk": "CM1 profit", "value": cm1_profit, "kind": "currency", "tone": "positive", "note_en": "Revenue - COGS - packaging - net shipping", "note_sk": "Trzby - COGS - balenie - ciste shipping"},
        {"en": "CM1 margin", "sk": "CM1 marza", "value": cm1_margin_pct, "kind": "percent", "tone": "positive"},
        {"en": "CM1 / order", "sk": "CM1 / objednavka", "value": cm1_profit_per_order, "kind": "currency", "tone": "positive"},
        {"en": "CM1 / customer", "sk": "CM1 / zakaznik", "value": cm1_profit_per_customer, "kind": "currency", "tone": "positive"},
        {"en": "CM2 profit", "sk": "CM2 profit", "value": cm2_profit, "kind": "currency", "tone": "positive", "note_en": "CM1 - paid ads", "note_sk": "CM1 - platene reklamy"},
        {"en": "CM2 margin", "sk": "CM2 marza", "value": cm2_margin_pct, "kind": "percent", "tone": "positive"},
        {"en": "CM2 / order", "sk": "CM2 / objednavka", "value": cm2_profit_per_order, "kind": "currency", "tone": "positive"},
        {"en": "CM3 profit", "sk": "CM3 profit", "value": cm3_profit, "kind": "currency", "tone": "positive", "note_en": "CM2 - fixed overhead", "note_sk": "CM2 - fixny overhead"},
        {"en": "CM3 margin", "sk": "CM3 marza", "value": cm3_margin_pct, "kind": "percent", "tone": "positive"},
        {"en": "CM3 / order", "sk": "CM3 / objednavka", "value": cm3_profit_per_order, "kind": "currency", "tone": "positive"},
        {"en": "Pre-ad contribution profit", "sk": "Pre-ad contribution profit", "value": _maybe_num((financial_metrics or {}).get("pre_ad_contribution_profit")), "kind": "currency", "tone": "positive"},
        {"en": "Pre-ad contribution margin", "sk": "Pre-ad contribution marza", "value": _maybe_num((financial_metrics or {}).get("pre_ad_contribution_margin_pct")), "kind": "percent", "tone": "positive"},
        {"en": "Pre-ad contribution / order", "sk": "Pre-ad contribution / objednavka", "value": _maybe_num((financial_metrics or {}).get("pre_ad_contribution_per_order")), "kind": "currency", "tone": "positive", "note_en": "Break-even order contribution", "note_sk": "Break-even contribution na objednavku"},
        {"en": "Post-ad contribution profit", "sk": "Post-ad contribution profit", "value": _maybe_num((financial_metrics or {}).get("post_ad_contribution_profit")), "kind": "currency", "tone": "positive"},
        {"en": "Post-ad contribution margin", "sk": "Post-ad contribution marza", "value": _maybe_num((financial_metrics or {}).get("post_ad_contribution_margin_pct")), "kind": "percent", "tone": "positive"},
        {"en": "Post-ad contribution / order", "sk": "Post-ad contribution / objednavka", "value": _maybe_num((financial_metrics or {}).get("post_ad_contribution_profit_per_order")), "kind": "currency", "tone": "positive", "note_en": "Excludes fixed overhead", "note_sk": "Bez fixneho overheadu"},
        {"en": "Break-even CAC", "sk": "Break-even CAC", "value": break_even_cac, "kind": "currency", "tone": "positive"},
        {"en": "Pre-ad contribution / customer", "sk": "Pre-ad contribution / zakaznik", "value": _maybe_num((financial_metrics or {}).get("pre_ad_contribution_per_customer")), "kind": "currency", "tone": "positive"},
        {"en": "Current FB CAC", "sk": "Aktualne FB CAC", "value": current_fb_cac, "kind": "currency", "tone": "negative"},
        {"en": "Paid CAC (FB)", "sk": "Paid CAC (FB)", "value": _maybe_num((financial_metrics or {}).get("paid_cac")), "kind": "currency", "tone": "negative"},
        {"en": "Blended CAC (tracked ads)", "sk": "Blended CAC (trackovane ads)", "value": _maybe_num((financial_metrics or {}).get("blended_cac")), "kind": "currency", "tone": "negative", "note_en": "FB + Google", "note_sk": "FB + Google"},
        {"en": "CAC headroom", "sk": "CAC headroom", "value": _maybe_num((financial_metrics or {}).get("cac_headroom")), "kind": "currency", "tone": "positive"},
        {"en": "CAC / break-even", "sk": "CAC / break-even", "value": cac_break_even_ratio, "kind": "multiple", "tone": "neutral"},
        {"en": "Contribution LTV/CAC", "sk": "Contribution LTV/CAC", "value": _maybe_num((financial_metrics or {}).get("contribution_ltv_cac")), "kind": "multiple", "tone": "positive"},
        {"en": "New cust. revenue", "sk": "Trzby novych zakaznikov", "value": _maybe_num((financial_metrics or {}).get("new_revenue")), "kind": "currency", "tone": "neutral"},
        {"en": "Returning cust. revenue", "sk": "Trzby vracajucich sa zakaznikov", "value": _maybe_num((financial_metrics or {}).get("returning_revenue")), "kind": "currency", "tone": "neutral"},
        {"en": "Payback period", "sk": "Payback period", "value": _maybe_num((financial_metrics or {}).get("payback_orders")), "kind": "number", "decimals": 2, "tone": "positive", "note_en": "orders", "note_sk": "objednavky"},
        {"en": "Payback period (days est.)", "sk": "Payback period (odhad dni)", "value": _maybe_num((financial_metrics or {}).get("payback_days_estimated")), "kind": "number", "decimals": 0, "tone": "positive", "note_en": "days", "note_sk": "dni"},
        {"en": "Post-ad payback orders est.", "sk": "Post-ad payback objednavky", "value": _maybe_num((financial_metrics or {}).get("post_ad_payback_orders")), "kind": "number", "decimals": 2, "tone": "positive", "note_en": "orders", "note_sk": "objednavky"},
        {"en": "Post-ad payback (days est.)", "sk": "Post-ad payback (odhad dni)", "value": _maybe_num((financial_metrics or {}).get("post_ad_payback_days_estimated")), "kind": "number", "decimals": 0, "tone": "positive", "note_en": "days", "note_sk": "dni"},
        {"en": "ROAS check delta", "sk": "ROAS check delta", "value": consistency_payload.get("roas_delta"), "kind": "delta", "tone": "positive"},
        {"en": "Margin check delta", "sk": "Margin check delta", "value": consistency_payload.get("margin_delta"), "kind": "delta", "tone": "positive"},
        {"en": "CAC check delta", "sk": "CAC check delta", "value": consistency_payload.get("cac_delta"), "kind": "delta", "tone": "negative"},
        {"en": "CAC (FB/new cust.)", "sk": "CAC (FB/novy zakaznik)", "value": current_fb_cac, "kind": "currency", "tone": "negative"},
        {"en": "FB spend / orders", "sk": "FB spend / objednavky", "value": avg_fb_cost_per_order, "kind": "currency", "tone": "negative"},
        {"en": "Refund orders", "sk": "Refund objednavky", "value": _maybe_num(refund_summary.get("refund_orders")), "kind": "integer", "tone": "negative"},
        {"en": "Refund rate", "sk": "Refund rate", "value": _maybe_num(refund_summary.get("refund_rate_pct")), "kind": "percent", "tone": "negative"},
        {"en": "Refund amount", "sk": "Refund amount", "value": _maybe_num(refund_summary.get("refund_amount")), "kind": "currency", "tone": "negative"},
        {"en": "Repeat purchase rate", "sk": "Repeat purchase rate", "value": repeat_purchase_rate, "kind": "percent", "tone": "positive"},
    ]
    full_library_tiles_html = "".join(
        _library_tile_html(
            tile["en"],
            tile["sk"],
            _format_library_tile_value(tile.get("value"), tile.get("kind", "number"), tile.get("decimals")),
            tone=tile.get("tone", "neutral"),
            note_en=tile.get("note_en", ""),
            note_sk=tile.get("note_sk", ""),
        )
        for tile in library_tiles
    )

    payload = {
        "series": series,
        "kpis": kpi_payload,
        "cost_mix": cost_mix,
        "cities": cities,
        "countries": countries,
        "geo_rows": geo_rows,
        "products": products,
        "product_margin_chart_rows": product_margin_chart_rows,
        "trend_rows": trend_rows,
        "customer_mix": customer_mix,
        "day_of_week": day_of_week,
        "week_of_month": week_of_month,
        "day_of_month": day_of_month,
        "weather": weather_payload,
        "weather_bucket": weather_bucket_payload,
        "refunds": refunds_payload,
        "customer_top_rows": customer_top_rows,
        "customer_concentration_summary": {
            "top_10_pct_revenue_share": _num((customer_concentration or {}).get("top_10_pct_revenue_share")),
            "top_20_pct_revenue_share": _num((customer_concentration or {}).get("top_20_pct_revenue_share")),
            "top_10_pct_profit_share": _num((customer_concentration or {}).get("top_10_pct_profit_share")),
            "top_20_pct_profit_share": _num((customer_concentration or {}).get("top_20_pct_profit_share")),
        },
        "cohort_summary": cohort_summary,
        "cohort_retention_rows": cohort_retention_rows,
        "cohort_order_frequency_rows": cohort_order_frequency_rows,
        "cohort_time_between_rows": cohort_time_between_rows,
        "cohort_time_by_order_rows": cohort_time_by_order_rows,
        "cohort_time_to_nth_rows": cohort_time_to_nth_rows,
        "cohort_revenue_by_order_rows": cohort_revenue_by_order_rows,
        "mature_cohort_rows": mature_cohort_rows,
        "refund_rate": _maybe_num(refund_summary.get("refund_rate_pct")),
        "returning_customers": returning_payload,
        "clv": clv_payload,
        "order_size": order_size_payload,
        "ltv": ltv_payload,
        "fb_daily": fb_daily_payload,
        "fb_campaign_rows": fb_campaign_rows,
        "cpo_daily": cpo_daily,
        "weekly_cpo": weekly_cpo,
        "campaign_cpo": campaign_cpo,
        "hourly_orders": hourly_orders,
        "fb_hourly": fb_hourly_payload,
        "fb_dow": fb_dow_payload,
        "ads_effectiveness": ads_effectiveness_payload,
        "ads_correlations": ads_correlations,
        "spend_effectiveness_rows": spend_effectiveness_rows,
        "dow_effectiveness_rows": dow_effectiveness_rows,
        "basket_contribution_rows": basket_contribution_rows,
        "sku_pareto_rows": sku_pareto_rows,
        "attach_rate_rows": attach_rate_rows,
        "bundle_accessory": {
            "summary": {k: _maybe_num(v) if isinstance(v, (int, float)) else _json_safe(v) for k, v in ((bundle_accessory_model or {}).get("summary") or {}).items()},
            "pair_rows": bundle_accessory_pair_rows,
            "device_rows": bundle_accessory_device_rows,
            "group_rows": bundle_accessory_group_rows,
        },
        "acquisition_family": {
            "summary": {k: _maybe_num(v) if isinstance(v, (int, float)) else _json_safe(v) for k, v in ((acquisition_family_cube or {}).get("summary") or {}).items()},
            "cube_rows": acquisition_family_cube_rows,
            "source_rows": acquisition_family_source_rows,
            "family_rows": acquisition_family_family_rows,
        },
        "daily_margin_rows": daily_margin_rows,
        "payday_window_rows": payday_window_rows,
        "cohort_payback_rows": cohort_payback_rows,
        "cohort_unit_economics_rows": cohort_unit_economics_rows,
        "advanced_summary": {k: _maybe_num(v) for k, v in advanced_summary.items()},
        "heatmap_rows": heatmap_rows,
        "b2b_rows": b2b_rows,
        "order_status_rows": order_status_rows,
        "item_retention_rows": item_retention_rows,
        "same_item_rows": same_item_rows,
        "same_item_frequency_rows": same_item_frequency_rows,
        "time_to_nth_rows": time_to_nth_rows,
        "sample_funnel": {
            "summary": {k: _json_safe(v) for k, v in sample_funnel_summary.items()},
            "windows": sample_funnel_window_rows,
            "entry_rows": sample_funnel_entry_rows,
        },
        "refill_cohorts": {
            "summary": {k: _json_safe(v) for k, v in refill_cohort_summary.items()},
            "bucket_rows": refill_cohort_bucket_rows,
            "window_rows": refill_cohort_window_rows,
            "cohort_rows": refill_cohort_rows,
        },
        "direct_assisted": {
            "summary": {k: _json_safe(v) for k, v in ((direct_assisted or {}).get("summary") or {}).items()},
            "entry_rows": direct_assisted_entry_rows,
            "window_rows": direct_assisted_window_rows,
        },
        "crm_funnel": {
            "summary": {k: _json_safe(v) for k, v in ((crm_funnel or {}).get("summary") or {}).items()},
            "segment_rows": crm_funnel_rows,
        },
        "scent_size": {
            "summary": {k: _json_safe(v) for k, v in ((scent_size or {}).get("summary") or {}).items()},
            "same_rows": scent_same_rows,
            "migration_rows": scent_migration_rows,
        },
        "bundle_recommender": {
            "summary": {k: _json_safe(v) for k, v in ((bundle_recommender or {}).get("summary") or {}).items()},
            "recommendation_rows": bundle_recommender_rows,
            "anchor_rows": bundle_recommender_anchor_rows,
        },
        "promo_discount": {
            "summary": {k: _json_safe(v) for k, v in ((promo_discount or {}).get("summary") or {}).items()},
            "bucket_rows": promo_discount_rows,
        },
        "combinations_rows": combinations_rows,
        "segment_rows": segment_rows,
        "consistency": consistency_payload,
        "financial": financial_payload,
        "cpo_summary": {
            "overall_cpo": _maybe_num((cost_per_order or {}).get("overall_cpo")),
            "fb_cpo": _maybe_num((cost_per_order or {}).get("fb_cpo")),
            "google_cpo": google_cpo_value,
            "best_lag_correlation": _maybe_num((cost_per_order or {}).get("best_lag_correlation")),
            "best_attribution_lag": (cost_per_order or {}).get("best_attribution_lag"),
            "reconciliation": (cost_per_order or {}).get("fb_spend_reconciliation") or {},
        },
    }
    payload_json = json.dumps(payload, ensure_ascii=False)

    product_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('product') or 'Unknown'))}</td><td>{escape(str(row.get('sku') or ''))}</td><td>€{_num(row.get('revenue')):,.2f}</td><td>€{_num(row.get('profit')):,.2f}</td><td>{_num(row.get('margin_pct')):.1f}%</td><td>{int(round(_num(row.get('orders'))))}</td></tr>"
        for row in products
    ) or '<tr><td colspan="6"><span class="lang-en">No product data available.</span><span class="lang-sk hidden">Produktové dáta nie sú dostupné.</span></td></tr>'

    product_trend_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('product') or 'Unknown'))}</td><td>{escape(str(row.get('trend') or '-'))}</td><td>{_num(row.get('revenue_growth_pct')):+.1f}%</td><td>{_num(row.get('qty_growth_pct')):+.1f}%</td><td>€{_num(row.get('total_revenue')):,.2f}</td></tr>"
        for row in trend_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No product trend data available.</span><span class="lang-sk hidden">Produktové trendy nie sú dostupné.</span></td></tr>'

    geo_rows_html = "".join(
        (
            "<tr>"
            f"<td><div class=\"table-label-stack\"><strong>{escape(str(row.get('country') or 'Unknown')).upper()}</strong>"
            f"{_geo_confidence_badge_html(row.get('confidence_status'))}"
            f"<div class=\"muted-note\"><span class=\"lang-en\">Score {int(round(_num(row.get('confidence_score'))))}%</span><span class=\"lang-sk hidden\">Skore {int(round(_num(row.get('confidence_score'))))}%</span></div>"
            "</div></td>"
            f"<td>{int(round(_num(row.get('orders'))))}</td>"
            f"<td>{_format_library_tile_value(row.get('revenue'), kind='currency')}</td>"
            f"<td>{_format_library_tile_value(row.get('contribution_profit_guarded'), kind='currency')}</td>"
            f"<td>{_format_library_tile_value(row.get('contribution_margin_pct_guarded'), kind='percent')}</td>"
            f"<td>{_format_library_tile_value(row.get('fb_cpo_guarded'), kind='currency')}</td>"
            "</tr>"
        )
        for row in geo_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No geo profitability data available.</span><span class="lang-sk hidden">Geo profitabilita nie je dostupna.</span></td></tr>'
    geo_warning_items = list(geo_qa.get("warnings") or [])
    geo_warning_items_html = "".join(f"<li>{escape(str(item))}</li>" for item in geo_warning_items)
    geo_warning_block_html = (
        f'<ul class="warning-list">{geo_warning_items_html}</ul>'
        if geo_warning_items_html
        else '<p class="muted-note"><span class="lang-en">No low-sample geo warnings for the current report window.</span><span class="lang-sk hidden">V aktualnom okne nie su ziadne geo warningy pre malu vzorku.</span></p>'
    )
    data_assertion_warning_items = list(data_assertions_qa.get("warnings") or [])
    data_assertion_failure_items = list(data_assertions_qa.get("failures") or [])
    data_assertion_warning_items_html = "".join(f"<li>{escape(str(item))}</li>" for item in data_assertion_warning_items)
    data_assertion_failure_items_html = "".join(f"<li>{escape(str(item))}</li>" for item in data_assertion_failure_items)
    data_assertion_warning_block_html = (
        (
            f'<div class="warning-block"><p class="muted-note"><span class="lang-en">Critical QA failures</span><span class="lang-sk hidden">Kriticke QA chyby</span></p><ul class="warning-list">{data_assertion_failure_items_html}</ul></div>'
            if data_assertion_failure_items_html else ""
        )
        + (
            f'<div class="warning-block"><p class="muted-note"><span class="lang-en">Warnings</span><span class="lang-sk hidden">Warningy</span></p><ul class="warning-list">{data_assertion_warning_items_html}</ul></div>'
            if data_assertion_warning_items_html else ""
        )
    ) or '<p class="muted-note"><span class="lang-en">Data assertions passed for the current report window.</span><span class="lang-sk hidden">Datove assertions pre aktualne okno presli bez warningov.</span></p>'
    margin_stability_warning_items = list(margin_stability_qa.get("warnings") or [])
    margin_stability_warning_items_html = "".join(f"<li>{escape(str(item))}</li>" for item in margin_stability_warning_items)
    margin_stability_warning_block_html = (
        f'<ul class="warning-list">{margin_stability_warning_items_html}</ul>'
        if margin_stability_warning_items_html
        else '<p class="muted-note"><span class="lang-en">Smoothed fixed-margin alerts are within tolerance for this report window.</span><span class="lang-sk hidden">Vyhladene alerty fixnej marze su v tolerancii pre toto okno.</span></p>'
    )

    customer_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('customer') or 'Unknown'))}</td><td>{int(round(_num(row.get('orders'))))}</td><td>€{_num(row.get('revenue')):,.2f}</td><td>€{_num(row.get('profit')):,.2f}</td><td>{_num(row.get('revenue_pct')):.1f}%</td></tr>"
        for row in customer_top_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No customer concentration data available.</span><span class="lang-sk hidden">Koncentrácia zákazníkov nie je dostupná.</span></td></tr>'

    cohort_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('cohort') or '-'))}</td><td>{_num(row.get('retention_2nd_pct')):.1f}%</td><td>{_num(row.get('retention_3rd_pct')):.1f}%</td><td>{_num(row.get('retention_4th_pct')):.1f}%</td><td>{_num(row.get('retention_5th_pct')):.1f}%</td></tr>"
        for row in cohort_retention_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No cohort retention data available.</span><span class="lang-sk hidden">Kohortná retencia nie je dostupná.</span></td></tr>'


    order_frequency_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('frequency') or '-'))}</td><td>{int(round(_num(row.get('customer_count'))))}</td><td>{int(round(_num(row.get('total_orders'))))}</td><td>{_num(row.get('customer_pct')):.1f}%</td><td>{_num(row.get('orders_pct')):.1f}%</td></tr>"
        for row in cohort_order_frequency_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No order frequency data available.</span><span class="lang-sk hidden">Data frekvencie objednavok nie su dostupne.</span></td></tr>'

    time_between_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('time_bucket') or '-'))}</td><td>{int(round(_num(row.get('count'))))}</td><td>{_num(row.get('percentage')):.1f}%</td></tr>"
        for row in cohort_time_between_rows
    ) or '<tr><td colspan="3"><span class="lang-en">No time-between-orders data available.</span><span class="lang-sk hidden">Data o case medzi objednavkami nie su dostupne.</span></td></tr>'

    time_between_by_order_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('transition') or '-'))}</td><td>{int(round(_num(row.get('count'))))}</td><td>{_num(row.get('avg_days')):.1f}</td><td>{_num(row.get('median_days')):.1f}</td><td>{int(round(_num(row.get('min_days'))))}</td><td>{int(round(_num(row.get('max_days'))))}</td></tr>"
        for row in cohort_time_by_order_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No order-transition timing data available.</span><span class="lang-sk hidden">Data o case medzi prechodmi objednavok nie su dostupne.</span></td></tr>'

    time_to_nth_order_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('order_number') or '-'))}</td><td>{int(round(_num(row.get('customer_count'))))}</td><td>{_num(row.get('avg_days_from_first')):.1f}</td><td>{_num(row.get('median_days_from_first')):.1f}</td><td>{_num(row.get('avg_days_from_prev')):.1f}</td><td>&euro;{_num(row.get('avg_order_value')):,.2f}</td></tr>"
        for row in cohort_time_to_nth_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No time-to-nth-order data available.</span><span class="lang-sk hidden">Data o case do n-tej objednavky nie su dostupne.</span></td></tr>'

    revenue_by_order_rows_html = "".join(
        f"<tr><td>{int(round(_num(row.get('order_number'))))}</td><td>&euro;{_num(row.get('avg_order_value')):,.2f}</td><td>&euro;{_num(row.get('total_revenue')):,.2f}</td><td>{int(round(_num(row.get('order_count'))))}</td><td>{_num(row.get('avg_items_per_order')):.2f}</td><td>&euro;{_num(row.get('avg_price_per_item')):,.2f}</td></tr>"
        for row in cohort_revenue_by_order_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No order-sequence value data available.</span><span class="lang-sk hidden">Data hodnoty podla poradia objednavky nie su dostupne.</span></td></tr>'

    mature_cohort_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('cohort') or '-'))}</td><td>{int(round(_num(row.get('cohort_age_days'))))}</td><td>{_num(row.get('retention_2nd_pct')):.1f}%</td><td>{_num(row.get('retention_3rd_pct')):.1f}%</td><td>{_num(row.get('retention_4th_pct')):.1f}%</td><td>{_num(row.get('retention_5th_pct')):.1f}%</td></tr>"
        for row in mature_cohort_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No mature-cohort retention data available.</span><span class="lang-sk hidden">Data zrelych kohort nie su dostupne.</span></td></tr>'

    fb_campaign_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('campaign_name') or 'Unknown'))}</td><td>?{_num(row.get('spend')):,.2f}</td><td>{int(round(_num(row.get('clicks'))))}</td><td>{_num(row.get('ctr')):.2f}%</td><td>?{_num(row.get('cpc')):,.2f}</td><td>{int(round(_num(row.get('platform_conversions', row.get('conversions')))))}</td></tr>"
        for row in fb_campaign_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No campaign data available.</span><span class="lang-sk hidden">Kampaňové dáta nie sú dostupné.</span></td></tr>'

    spend_effectiveness_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('spend_range') or '-'))}</td><td>€{_num(row.get('avg_spend')):,.2f}</td><td>{_num(row.get('avg_orders')):.1f}</td><td>€{_num(row.get('avg_revenue')):,.2f}</td><td>€{_num(row.get('avg_profit')):,.2f}</td><td>{_num(row.get('roas')):.2f}x</td></tr>"
        for row in spend_effectiveness_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No spend effectiveness data available.</span><span class="lang-sk hidden">Dáta spend efektivity nie sú dostupné.</span></td></tr>'
    dow_effectiveness_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('day_name') or '-'))}</td><td>&euro;{_num(row.get('avg_fb_spend')):,.2f}</td><td>{_num(row.get('avg_orders')):.1f}</td><td>&euro;{_num(row.get('avg_revenue')):,.2f}</td><td>&euro;{_num(row.get('avg_profit')):,.2f}</td></tr>"
        for row in dow_effectiveness_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No day-of-week effectiveness data available.</span><span class="lang-sk hidden">Day-of-week efektivita nie je dostupna.</span></td></tr>'


    basket_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('basket_size') or '-'))}</td><td>{int(round(_num(row.get('orders'))))}</td><td>€{_num(row.get('revenue')):,.2f}</td><td>€{_num(row.get('pre_ad_contribution')):,.2f}</td><td>€{_num(row.get('contribution_per_order')):,.2f}</td><td>{_num(row.get('contribution_margin_pct')):.1f}%</td></tr>"
        for row in basket_contribution_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No basket contribution data available.</span><span class="lang-sk hidden">Dáta kontribúcie košíka nie sú dostupné.</span></td></tr>'

    sku_pareto_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('product') or 'Unknown'))}</td><td>{escape(str(row.get('sku') or ''))}</td><td>€{_num(row.get('revenue')):,.2f}</td><td>€{_num(row.get('pre_ad_contribution')):,.2f}</td><td>{_num(row.get('cum_contribution_pct')):.1f}%</td></tr>"
        for row in sku_pareto_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No SKU Pareto data available.</span><span class="lang-sk hidden">SKU Pareto dáta nie sú dostupné.</span></td></tr>'

    attach_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('anchor_item') or '-'))}</td><td>{escape(str(row.get('attached_item') or '-'))}</td><td>{int(round(_num(row.get('anchor_orders'))))}</td><td>{int(round(_num(row.get('attached_orders'))))}</td><td>{_num(row.get('attach_rate_pct')):.1f}%</td></tr>"
        for row in attach_rate_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No attach-rate data available.</span><span class="lang-sk hidden">Attach-rate dáta nie sú dostupné.</span></td></tr>'

    b2b_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('customer_type') or '-'))}</td><td>{int(round(_num(row.get('orders'))))}</td><td>€{_num(row.get('revenue')):,.2f}</td><td>€{_num(row.get('profit')):,.2f}</td><td>{int(round(_num(row.get('unique_customers'))))}</td><td>€{_num(row.get('aov')):,.2f}</td></tr>"
        for row in b2b_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No B2B/B2C split available.</span><span class="lang-sk hidden">B2B/B2C split nie je dostupný.</span></td></tr>'

    order_status_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('status') or '-'))}</td><td>{int(round(_num(row.get('orders'))))}</td><td>€{_num(row.get('revenue')):,.2f}</td><td>{_num(row.get('orders_pct')):.1f}%</td></tr>"
        for row in order_status_rows
    ) or '<tr><td colspan="4"><span class="lang-en">No order status data available.</span><span class="lang-sk hidden">Dáta stavov objednávok nie sú dostupné.</span></td></tr>'

    item_retention_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('item_name') or '-'))}</td><td>{int(round(_num(row.get('first_order_customers'))))}</td><td>{_num(row.get('retention_2nd_pct')):.1f}%</td><td>{_num(row.get('retention_3rd_pct')):.1f}%</td><td>{_num(row.get('avg_orders_per_customer')):.2f}</td></tr>"
        for row in item_retention_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No first-item retention data available.</span><span class="lang-sk hidden">Retencia podľa prvého produktu nie je dostupná.</span></td></tr>'

    same_item_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('item_name') or '-'))}</td><td>{int(round(_num(row.get('unique_customers'))))}</td><td>{_num(row.get('repurchase_2x_pct')):.1f}%</td><td>{_num(row.get('repurchase_3x_pct')):.1f}%</td><td>{('%.1f' % _num(row.get('avg_days_between_repurchase'))) if row.get('avg_days_between_repurchase') not in (None, '') else 'N/A'}</td></tr>"
        for row in same_item_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No same-item repurchase data available.</span><span class="lang-sk hidden">Repurchase rovnakého produktu nie je dostupný.</span></td></tr>'

    time_to_nth_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('item_name') or '-'))}</td><td>{int(round(_num(row.get('first_order_customers'))))}</td><td>{('%.1f' % _num(row.get('avg_days_to_2nd'))) if row.get('avg_days_to_2nd') not in (None, '') else 'N/A'}</td><td>{('%.1f' % _num(row.get('median_days_to_2nd'))) if row.get('median_days_to_2nd') not in (None, '') else 'N/A'}</td><td>{('%.1f' % _num(row.get('avg_days_to_3nd'))) if row.get('avg_days_to_3nd') not in (None, '') else 'N/A'}</td></tr>"
        for row in time_to_nth_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No time-to-next-order data available.</span><span class="lang-sk hidden">Dáta času do ďalšej objednávky nie sú dostupné.</span></td></tr>'

    combinations_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('combination_size') or '-'))}</td><td>{escape(str(row.get('combination') or '-'))}</td><td>{int(round(_num(row.get('count'))))}</td><td>€{_num(row.get('price')):,.2f}</td></tr>"
        for row in combinations_rows
    ) or '<tr><td colspan="4"><span class="lang-en">No item combination data available.</span><span class="lang-sk hidden">Dáta kombinácií produktov nie sú dostupné.</span></td></tr>'

    segment_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('segment') or '-'))}</td><td>{int(round(_num(row.get('count'))))}</td><td>{int(round(_num(row.get('priority'))))}</td><td><span class='lang-en'>{escape(str(row.get('description_en') or '-'))}</span><span class='lang-sk hidden'>{escape(str(row.get('description_sk') or '-'))}</span></td><td><span class='lang-en'>{escape(str(row.get('timing_en') or '-'))}</span><span class='lang-sk hidden'>{escape(str(row.get('timing_sk') or '-'))}</span></td></tr>"
        for row in segment_rows
    ) or '<tr><td colspan="5"><span class="lang-en">No email segmentation data available.</span><span class="lang-sk hidden">Email segmentácia nie je dostupná.</span></td></tr>'

    health_cards = source_rows + qa_rows
    health_html = "".join(
        f'<div class="health-item"><div class="health-title">{escape(str(row.get("label") or row.get("key") or "Source"))}</div><div class="health-status {("good" if row.get("healthy") else ("warn" if row.get("status") in {"warning", "degraded"} else "bad"))}">{escape(str(row.get("status") or "unknown"))}</div><p>{escape(str(row.get("message") or row.get("mode") or "-"))}</p></div>'
        for row in health_cards
    ) or '<div class="health-item"><div class="health-title"><span class="lang-en">Source health</span><span class="lang-sk hidden">Stav zdrojov</span></div><div class="health-status good">ok</div><p><span class="lang-en">No source warnings attached to this run.</span><span class="lang-sk hidden">K tomuto behu nie su pripojene ziadne varovania zdrojov.</span></p></div>'

    period_switcher_html = _period_switcher_html(period_switcher)
    refund_summary = _resolve_refund_summary(financial_metrics, refunds_analysis)
    repeat_rate = _num(cohort_summary.get("repeat_rate_pct"))
    repeat_customers = int(round(_num(cohort_summary.get("repeat_customers"))))
    avg_days_to_2nd = _maybe_num(cohort_summary.get("avg_days_to_2nd_order"))
    avg_days_between = _maybe_num(cohort_summary.get("avg_days_between_orders"))
    top_10_share = _num((customer_concentration or {}).get("top_10_pct_revenue_share"))
    top_20_share = _num((customer_concentration or {}).get("top_20_pct_revenue_share"))
    sample_summary = sample_funnel_summary or {}
    sample_entry_customers = int(round(_num(sample_summary.get("entry_customers"))))
    sample_repeat_30d = _maybe_num(sample_summary.get("repeat_30d_pct"))
    sample_fullsize_30d = _maybe_num(sample_summary.get("fullsize_any_30d_pct"))
    sample_fullsize_60d = _maybe_num(sample_summary.get("fullsize_any_60d_pct"))
    sample_median_days_fullsize = _maybe_num(sample_summary.get("median_days_to_fullsize"))
    sample_top_entry_product = escape(str(sample_summary.get("top_entry_product") or "-"))
    sample_entry_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('item_name') or '-'))}</td><td>{int(round(_num(row.get('entry_customers'))))}</td><td>{_num(row.get('repeat_30d_pct')):.1f}%</td><td>{_num(row.get('fullsize_any_30d_pct')):.1f}%</td><td>{_num(row.get('fullsize_any_60d_pct')):.1f}%</td><td>{_num(row.get('fullsize_200_60d_pct')):.1f}%</td><td>{_num(row.get('fullsize_500_60d_pct')):.1f}%</td></tr>"
        for row in sample_funnel_entry_rows
    ) or '<tr><td colspan="7"><span class="lang-en">No sample funnel data available.</span><span class="lang-sk hidden">Sample funnel data nie su dostupne.</span></td></tr>'
    refill_summary = refill_cohort_summary or {}
    refill_entry_customers = int(round(_num(refill_summary.get("entry_customers"))))
    refill_sample_60d = _maybe_num(refill_summary.get("sample_refill_60d_pct"))
    refill_sample_90d = _maybe_num(refill_summary.get("sample_refill_90d_pct"))
    refill_sample_days_to_2nd = _maybe_num(refill_summary.get("sample_avg_days_to_2nd"))
    refill_second_order_aov = _maybe_num(refill_summary.get("avg_second_order_aov"))
    refill_dominant_bucket = escape(str(refill_summary.get("dominant_entry_bucket") or "-"))
    refill_bucket_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('entry_bucket_label') or '-'))}</td><td>{int(round(_num(row.get('customers'))))}</td><td>{_num(row.get('refill_60d_pct')):.1f}%</td><td>{_num(row.get('refill_90d_pct')):.1f}%</td><td>{(_format_mini_value_html(row.get('avg_days_to_2nd'), kind='number', decimals=1))}</td><td>{_format_mini_value_html(row.get('second_order_aov'), kind='currency')}</td></tr>"
        for row in refill_cohort_bucket_rows
    ) or '<tr><td colspan="6"><span class="lang-en">No refill cohort data available.</span><span class="lang-sk hidden">Refill kohortne data nie su dostupne.</span></td></tr>'
    attribution_qa = (((source_health or {}).get("qa") or {}).get("attribution") or {})
    attribution_warnings = [str(item) for item in list(attribution_qa.get("warnings") or []) if str(item).strip()]
    attribution_warning_items_html = "".join(
        f"<li>{escape(item)}</li>"
        for item in attribution_warnings
    ) or '<li><span class="lang-en">Attribution QA passed for this period.</span><span class="lang-sk hidden">Attribution QA pre toto obdobie presla bez warningov.</span></li>'
    attribution_coverage_ratio = _maybe_num(attribution_qa.get("coverage_ratio"))
    attribution_oversubscription_ratio = _maybe_num(attribution_qa.get("oversubscription_ratio"))
    attribution_campaign_rows = int(round(_num(attribution_qa.get("campaign_rows"))))
    attribution_cpa_mismatch_count = int(round(_num(attribution_qa.get("platform_cost_mismatch_count"))))
    attribution_banner_severity: Optional[str] = None
    attribution_banner_title_en = ""
    attribution_banner_title_sk = ""
    attribution_banner_summary_en = ""
    attribution_banner_summary_sk = ""
    if attribution_warnings:
        severe_coverage = (
            attribution_coverage_ratio is None
            or attribution_coverage_ratio < 0.75
            or attribution_coverage_ratio > 1.25
        )
        severe_oversubscription = (
            attribution_oversubscription_ratio is not None
            and attribution_oversubscription_ratio > 1.10
        )
        missing_campaign_spend = any("Campaign-level Facebook spend is missing" in item for item in attribution_warnings)
        empty_campaign_table = any("Campaign attribution table is empty" in item for item in attribution_warnings)
        severe_cpa_mismatch = attribution_cpa_mismatch_count > 0
        is_critical = (
            missing_campaign_spend
            or empty_campaign_table
            or severe_coverage
            or severe_oversubscription
            or severe_cpa_mismatch
        )
        attribution_banner_severity = "critical" if is_critical else "warning"
        if is_critical:
            attribution_banner_title_en = "Attribution warning"
            attribution_banner_title_sk = "Varovanie atribucie"
            attribution_banner_summary_en = (
                "Campaign attribution is not fully trustworthy for this period. Treat ROAS, campaign CPO and campaign output as directional until coverage is fixed."
            )
            attribution_banner_summary_sk = (
                "Atribucia kampani nie je pre toto obdobie plne doveryhodna. ROAS, kampanove CPO a vykon kampani ber ako orientacne, kym sa coverage neopraví."
            )
        else:
            attribution_banner_title_en = "Attribution needs review"
            attribution_banner_title_sk = "Atribuciu treba preverit"
            attribution_banner_summary_en = (
                "Campaign attribution produced warnings. Top-level business KPIs remain usable, but campaign-level comparisons need extra caution."
            )
            attribution_banner_summary_sk = (
                "Atribucia kampani ma warningy. Hlavne business KPI su pouzitelne, ale kampanove porovnania treba citat opatrnejsie."
            )
    attribution_banner_html = ""
    if attribution_banner_severity:
        attribution_banner_html = (
            f'<section class="hero-alert {escape(attribution_banner_severity)}">'
            '<div class="hero-alert-copy">'
            f'<div class="hero-alert-badge {escape(attribution_banner_severity)}">'
            f'<span class="lang-en">{escape(attribution_banner_title_en)}</span>'
            f'<span class="lang-sk hidden">{escape(attribution_banner_title_sk)}</span>'
            '</div>'
            f'<h3><span class="lang-en">{escape(attribution_banner_title_en)}</span>'
            f'<span class="lang-sk hidden">{escape(attribution_banner_title_sk)}</span></h3>'
            f'<p><span class="lang-en">{escape(attribution_banner_summary_en)}</span>'
            f'<span class="lang-sk hidden">{escape(attribution_banner_summary_sk)}</span></p>'
            '</div>'
            '<div class="hero-alert-metrics">'
            f'<div class="hero-alert-metric"><small><span class="lang-en">Coverage ratio</span><span class="lang-sk hidden">Coverage ratio</span></small><strong>{_format_mini_value_html(attribution_coverage_ratio, kind="multiple")}</strong></div>'
            f'<div class="hero-alert-metric"><small><span class="lang-en">Oversubscription</span><span class="lang-sk hidden">Oversubscription</span></small><strong>{_format_mini_value_html(attribution_oversubscription_ratio, kind="multiple")}</strong></div>'
            f'<div class="hero-alert-metric"><small><span class="lang-en">CPA mismatches</span><span class="lang-sk hidden">CPA nezrovnalosti</span></small><strong>{attribution_cpa_mismatch_count}</strong></div>'
            f'<div class="hero-alert-metric"><small><span class="lang-en">Campaign rows</span><span class="lang-sk hidden">Pocet kampani</span></small><strong>{attribution_campaign_rows}</strong></div>'
            '</div>'
            '<ul class="warning-list">'
            f'{attribution_warning_items_html}'
            '</ul>'
            '</section>'
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        :root {{
            --bg: #f5efe6;
            --panel: #fffdfa;
            --line: #eadfce;
            --text: #241f19;
            --muted: #7f776f;
            --accent: #ff8a1f;
            --accent-soft: #fff0df;
            --green: #1f9d66;
            --red: #cf5060;
            --blue: #4766ff;
            --shadow: 0 18px 48px rgba(61, 43, 18, 0.08);
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif;
            color: var(--text);
            background: radial-gradient(circle at top left, rgba(255,138,31,.14), transparent 18%), linear-gradient(180deg, #fbf7f0 0%, #f2ece3 100%);
        }}
        .layout {{ display: grid; grid-template-columns: 240px 1fr; min-height: 100vh; }}
        .sidebar {{ position: sticky; top: 0; height: 100vh; padding: 26px 18px; background: rgba(255,253,249,.85); border-right: 1px solid var(--line); backdrop-filter: blur(16px); }}
        .brand, .panel {{ background: var(--panel); border: 1px solid var(--line); box-shadow: var(--shadow); }}
        .brand {{ border-radius: 18px; padding: 12px 14px; display: flex; gap: 12px; align-items: center; margin-bottom: 24px; }}
        .brand-mark {{ width: 38px; height: 38px; border-radius: 12px; display: grid; place-items: center; color: #fff; font-weight: 800; background: linear-gradient(135deg, var(--accent), #ffb96d); }}
        .brand small {{ color: var(--muted); display: block; margin-top: 2px; }}
        .nav-label {{ margin: 0 10px 12px; color: var(--muted); font-size: 11px; font-weight: 800; letter-spacing: .08em; text-transform: uppercase; }}
        .nav-link {{ display:flex; align-items:center; gap:10px; padding: 12px 14px; text-decoration:none; color:#554d45; border-radius:14px; margin-bottom:6px; }}
        .nav-link:hover {{ background: rgba(255,138,31,.08); }}
        .nav-link.active {{ color:#fff; background: linear-gradient(135deg, var(--accent), #ff9e46); }}
        .nav-dot {{ width: 26px; height: 26px; border-radius: 9px; display:grid; place-items:center; font-size: 12px; font-weight: 800; background: rgba(255,138,31,.14); color: var(--accent); }}
        .nav-link.active .nav-dot {{ color:#fff; background: rgba(255,255,255,.18); }}
        .global-period-panel {{ margin: 18px 0 22px; }}
        .period-summary {{ display:flex; flex-direction:column; gap: 6px; margin-bottom: 12px; }}
        .period-summary strong {{ font-size: 15px; font-weight: 800; color: var(--text); }}
        .period-summary small {{ color: var(--muted); font-size: 12px; line-height: 1.45; }}
        .content {{ padding: 28px 28px 72px; }}
        .shell {{ max-width: 1500px; margin: 0 auto; }}
        .hero {{ display:grid; grid-template-columns: 1.45fr .95fr; gap: 18px; }}
        .panel {{ border-radius: 28px; }}
        .hero-main {{ padding: 28px 30px; }}
        .badge {{ display:inline-flex; padding:8px 12px; border-radius:999px; background: var(--accent-soft); color:#b35d00; font-size:12px; font-weight:800; text-transform:uppercase; letter-spacing:.06em; margin-bottom: 14px; }}
        h1 {{ margin:0; font-size: clamp(34px, 4vw, 52px); line-height:1.02; letter-spacing:-.04em; }}
        .subtitle {{ margin: 14px 0 0; color: var(--muted); font-size: 16px; line-height: 1.6; max-width: 720px; }}
        .meta-row {{ display:flex; flex-wrap:wrap; gap: 12px; margin-top: 24px; }}
        .meta-card {{ min-width: 160px; padding: 12px 14px; background: #fff; border:1px solid var(--line); border-radius: 15px; }}
        .meta-card small {{ display:block; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .08em; margin-bottom: 5px; }}
        .meta-card strong {{ font-size: 18px; }}
        .side-stack {{ display:grid; gap: 16px; }}
        .controls {{ padding: 18px 20px; }}
        .label {{ color: var(--muted); font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing: .08em; margin-bottom: 10px; }}
        .pill-row {{ display:flex; flex-wrap:wrap; gap:8px; }}
        .pill, .lang-btn, .window-btn {{ padding: 9px 14px; border-radius: 999px; border:1px solid var(--line); background: #fff; color:#4f473f; text-decoration:none; cursor:pointer; font-size: 13px; font-weight: 800; }}
        .pill.active, .lang-btn.active, .window-btn.active {{ color:#fff; background: linear-gradient(135deg, var(--accent), #ff9d42); border-color: transparent; }}
        .hero-kpis {{ padding: 20px; display:grid; grid-template-columns: repeat(2, 1fr); gap: 14px; }}
        .hero-kpi {{ padding: 18px; border-radius: 20px; background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(255,245,233,.92)); border:1px solid rgba(255,138,31,.16); }}
        .hero-kpi small {{ display:block; color: var(--muted); margin-bottom: 8px; }}
        .hero-kpi strong {{ font-size: 28px; }}
        .hero-alert {{ margin-top: 18px; padding: 20px 22px; border-radius: 24px; border: 1px solid var(--line); box-shadow: var(--shadow); background: var(--panel); }}
        .hero-alert.warning {{ background: linear-gradient(180deg, rgba(255,252,247,.98), rgba(255,243,227,.96)); border-color: rgba(255,138,31,.20); }}
        .hero-alert.critical {{ background: linear-gradient(180deg, rgba(255,250,250,.98), rgba(255,238,241,.96)); border-color: rgba(207,80,96,.20); }}
        .hero-alert-copy {{ display:flex; flex-direction:column; gap: 8px; margin-bottom: 16px; }}
        .hero-alert-copy h3 {{ margin: 0; font-size: 20px; letter-spacing: -.03em; }}
        .hero-alert-copy p {{ margin: 0; color: var(--muted); line-height: 1.6; max-width: 920px; }}
        .hero-alert-badge {{ display:inline-flex; align-self:flex-start; padding: 8px 12px; border-radius: 999px; font-size: 11px; font-weight: 900; text-transform: uppercase; letter-spacing: .08em; }}
        .hero-alert-badge.warning {{ color:#a75300; background: rgba(255,138,31,.12); }}
        .hero-alert-badge.critical {{ color:#a22d40; background: rgba(207,80,96,.14); }}
        .hero-alert-metrics {{ display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }}
        .hero-alert-metric {{ padding: 14px 16px; border-radius: 16px; background: rgba(255,255,255,.72); border:1px solid rgba(255,138,31,.12); }}
        .hero-alert.critical .hero-alert-metric {{ border-color: rgba(207,80,96,.14); }}
        .hero-alert-metric small {{ display:block; color: var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.08em; margin-bottom:6px; }}
        .hero-alert-metric strong {{ font-size: 22px; }}
        .section {{ margin-top: 24px; }}
        .section-head h2 {{ margin:0; font-size: 26px; letter-spacing: -.03em; }}
        .section-head p {{ margin: 6px 0 14px; color: var(--muted); line-height: 1.55; max-width: 760px; }}
        .kpi-band {{ padding: 22px; }}
        .kpi-grid {{ display:grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-top: 16px; }}
        .kpi-card {{ padding: 20px; border-radius: 22px; background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(255,245,233,.92)); border:1px solid rgba(255,138,31,.14); min-height: 228px; display:flex; flex-direction:column; }}
        .kpi-card small {{ color: var(--muted); font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing:.08em; }}
        .kpi-value {{ font-size: 36px; font-weight: 900; line-height: 1; letter-spacing: -.05em; margin: 10px 0 6px; }}
        .kpi-secondary {{ color: var(--text); font-size: 18px; font-weight: 800; line-height: 1.2; margin: -2px 0 8px; }}
        .kpi-period {{ color: var(--muted); font-size: 12px; font-weight: 700; }}
        .compare-list {{ margin-top: auto; display:grid; gap:4px; }}
        .compare-row {{ font-size: 13px; font-weight: 800; }}
        .compare-row.good {{ color: var(--green); }}
        .compare-row.bad {{ color: var(--red); }}
        .compare-row.neutral {{ color: var(--muted); }}
        .kpi-trend {{ margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(255,138,31,.10); }}
        .kpi-trend-head {{ display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom: 8px; }}
        .kpi-trend-label {{ color: var(--muted); font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing: .08em; }}
        .kpi-trend-delta {{ font-size: 12px; font-weight: 900; }}
        .kpi-trend-delta.good {{ color: var(--green); }}
        .kpi-trend-delta.bad {{ color: var(--red); }}
        .kpi-trend-delta.neutral {{ color: var(--muted); }}
        .kpi-sparkline {{ height: 44px; }}
        .kpi-sparkline svg {{ display:block; width: 100%; height: 44px; overflow: visible; }}
        .chart-card, .table-card, .health-card {{ padding: 20px; }}
        .card-head {{ display:flex; justify-content:space-between; gap:12px; align-items:start; margin-bottom: 16px; }}
        .card-head h3 {{ margin:0; font-size:18px; }}
        .card-head p {{ margin: 6px 0 0; color: var(--muted); font-size: 13px; line-height: 1.5; }}
        .grid-2 {{ display:grid; grid-template-columns: repeat(2, 1fr); gap: 18px; }}
        .chart-shell {{ height: 340px; position: relative; }}
        .chart-shell.tall {{ height: 420px; }}
        .chart-shell.compact {{ height: 300px; }}
        .mini-grid {{ display:grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-top: 12px; }}
        .mini-card {{ padding: 14px 16px; border-radius: 16px; background: var(--accent-soft); border:1px solid rgba(255,138,31,.12); }}
        .mini-card small {{ display:block; color: var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.08em; margin-bottom:6px; }}
        .mini-card strong {{ font-size: 20px; }}
        .library-tile-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-top: 12px; }}
        .library-tile {{ padding: 16px; border-radius: 18px; background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(255,245,233,.92)); border:1px solid rgba(255,138,31,.12); min-height: 122px; box-shadow: 0 8px 20px rgba(115, 82, 22, .05); }}
        .library-tile small {{ display:block; color: var(--muted); font-size: 10px; font-weight: 800; text-transform: uppercase; letter-spacing: .08em; margin-bottom: 8px; }}
        .library-tile-value {{ font-size: 18px; font-weight: 900; line-height: 1.15; letter-spacing: -.04em; color: var(--text); word-break: break-word; }}
        .library-tile-note {{ margin-top: 8px; color: var(--muted); font-size: 11px; line-height: 1.4; }}
        .library-tile.tone-positive .library-tile-value {{ color: var(--green); }}
        .library-tile.tone-negative .library-tile-value {{ color: var(--red); }}
        .health-grid {{ display:grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }}
        .health-item {{ padding: 16px; border-radius: 18px; background: #fff; border:1px solid var(--line); }}
        .health-title {{ font-weight: 800; margin-bottom: 8px; }}
        .health-status {{ display:inline-flex; padding: 7px 10px; border-radius: 999px; font-size: 12px; font-weight: 800; text-transform: uppercase; }}
        .health-status.good {{ color:#11633f; background: rgba(31,157,102,.12); }}
        .health-status.warn {{ color:#a75300; background: rgba(255,138,31,.12); }}
        .health-status.bad {{ color:#a22d40; background: rgba(207,80,96,.12); }}
        .confidence-badge {{ display:inline-flex; align-items:center; padding:6px 10px; border-radius:999px; font-size:11px; font-weight:800; text-transform:uppercase; letter-spacing:.08em; }}
        .confidence-badge.ready {{ color:#11633f; background: rgba(31,157,102,.12); }}
        .confidence-badge.observe {{ color:#a75300; background: rgba(255,138,31,.12); }}
        .confidence-badge.ignore {{ color:#a22d40; background: rgba(207,80,96,.12); }}
        .warning-list {{ margin: 16px 0 0; padding-left: 18px; }}
        .warning-list li {{ margin: 8px 0; color: var(--muted); }}
        .muted-note {{ color: var(--muted); font-size: 12px; line-height: 1.55; }}
        .table-label-stack {{ display:flex; flex-direction:column; gap:8px; }}
        table {{ width:100%; border-collapse: collapse; }}
        th, td {{ text-align:left; padding: 11px 8px; border-bottom: 1px solid rgba(234,223,206,.85); font-size: 13px; }}
        th {{ color: var(--muted); font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing:.08em; }}
        .lang-en.hidden, .lang-sk.hidden {{ display:none !important; }}
        @media (max-width: 1280px) {{ .layout {{ grid-template-columns: 1fr; }} .sidebar {{ position: static; height:auto; }} }}
        @media (max-width: 1080px) {{ .hero, .grid-2, .kpi-grid, .health-grid, .mini-grid, .hero-alert-metrics {{ grid-template-columns: 1fr; }} }}
    </style>
</head>
<body>
    <div class="layout">
        <aside class="sidebar">
            <div class="brand">
                <div class="brand-mark">{brand_mark}</div>
                <div>
                    <strong>{title}</strong>
                    <small><span class="lang-en">executive reporting dashboard</span><span class="lang-sk hidden">hlavny reporting dashboard</span></small>
                </div>
            </div>
            {period_switcher_html}
            <div class="nav-label"><span class="lang-en">Navigate</span><span class="lang-sk hidden">Navigácia</span></div>
            <a class="nav-link active" href="#overview"><span class="nav-dot">01</span><span class="lang-en">Overview</span><span class="lang-sk hidden">Prehľad</span></a>
            <a class="nav-link" href="#sales"><span class="nav-dot">02</span><span class="lang-en">Sales</span><span class="lang-sk hidden">Predaj</span></a>
            <a class="nav-link" href="#economics"><span class="nav-dot">03</span><span class="lang-en">Economics</span><span class="lang-sk hidden">Ekonomika</span></a>
            <a class="nav-link" href="#marketing"><span class="nav-dot">04</span><span class="lang-en">Marketing</span><span class="lang-sk hidden">Marketing</span></a>
            <a class="nav-link" href="#customers"><span class="nav-dot">05</span><span class="lang-en">Customers</span><span class="lang-sk hidden">Zákazníci</span></a>
            <a class="nav-link" href="#patterns"><span class="nav-dot">06</span><span class="lang-en">Patterns</span><span class="lang-sk hidden">Patterny</span></a>
            <a class="nav-link" href="#geography"><span class="nav-dot">07</span><span class="lang-en">Geography</span><span class="lang-sk hidden">Geografia</span></a>
            <a class="nav-link" href="#products"><span class="nav-dot">08</span><span class="lang-en">Products</span><span class="lang-sk hidden">Produkty</span></a>
            <a class="nav-link" href="#operations"><span class="nav-dot">09</span><span class="lang-en">Operations</span><span class="lang-sk hidden">Operativa</span></a>
            <a class="nav-link" href="#library"><span class="nav-dot">10</span><span class="lang-en">Full library</span><span class="lang-sk hidden">Plna kniznica</span></a>
            <a class="nav-link" href="#health"><span class="nav-dot">11</span><span class="lang-en">Data health</span><span class="lang-sk hidden">Kvalita dát</span></a>
        </aside>
        <main class="content">
            <div class="shell">
                <section class="hero" id="overview">
                    <div class="panel hero-main">
                        <div class="badge"><span class="lang-en">research-driven concept</span><span class="lang-sk hidden">research-driven koncept</span></div>
                        <h1>{title}</h1>
                        <p class="subtitle"><span class="lang-en">A modern ecommerce reporting dashboard built around executive KPIs first, grouped business questions, clearer charts, and explicit source confidence.</span><span class="lang-sk hidden">Moderny ecommerce reporting dashboard postaveny okolo executive KPI, business otazok, citatelnejsich grafov a explicitneho stavu datovych zdrojov.</span></p>
                        <div class="meta-row">
                            <div class="meta-card"><small><span class="lang-en">Range</span><span class="lang-sk hidden">Obdobie</span></small><strong>{escape(date_from.strftime("%Y-%m-%d"))} → {escape(date_to.strftime("%Y-%m-%d"))}</strong></div>
                            <div class="meta-card"><small><span class="lang-en">Generated</span><span class="lang-sk hidden">Vygenerované</span></small><strong>{escape(generated_at)}</strong></div>
                            <div class="meta-card"><small><span class="lang-en">Top city</span><span class="lang-sk hidden">Top mesto</span></small><strong>{escape(top_city)}</strong></div>
                            <div class="meta-card"><small><span class="lang-en">Top product</span><span class="lang-sk hidden">Top produkt</span></small><strong>{escape(top_product)}</strong></div>
                        </div>
                    </div>
                    <div class="side-stack">
                        <div class="panel controls">
                            <div class="label"><span class="lang-en">Language</span><span class="lang-sk hidden">Jazyk</span></div>
                            <div class="pill-row">
                                <button type="button" class="lang-btn active" data-lang="en">EN</button>
                                <button type="button" class="lang-btn" data-lang="sk">SK</button>
                            </div>
                        </div>
                        <div class="panel hero-kpis">
                            <div class="hero-kpi"><small><span class="lang-en">Revenue</span><span class="lang-sk hidden">Tržby</span></small><strong>€{total_revenue:,.0f}</strong></div>
                            <div class="hero-kpi"><small><span class="lang-en">Profit</span><span class="lang-sk hidden">Zisk</span></small><strong>€{total_profit:,.0f}</strong></div>
                            <div class="hero-kpi"><small><span class="lang-en">Orders</span><span class="lang-sk hidden">Objednávky</span></small><strong>{total_orders:,}</strong></div>
                            <div class="hero-kpi"><small><span class="lang-en">Blended ROAS</span><span class="lang-sk hidden">Blended ROAS</span></small><strong>{blended_roas:.2f}x</strong></div>
                        </div>
                    </div>
                </section>
                {attribution_banner_html}

                <section class="section">
                    <div class="section-head">
                        <h2><span class="lang-en">Executive KPI deck</span><span class="lang-sk hidden">Executive KPI prehľad</span></h2>
                        <p><span class="lang-en">The same CFO-grade KPI logic as the existing CFO report, but rendered inside the new dashboard shell.</span><span class="lang-sk hidden">Rovnaká CFO KPI logika ako v existujúcom CFO reporte, ale vykreslená v novom dashboard shelle.</span></p>
                    </div>
                    <div class="panel kpi-band">
                        <div class="pill-row">
                            <button type="button" class="window-btn" data-window="daily"><span class="lang-en">Daily</span><span class="lang-sk hidden">Denne</span></button>
                            <button type="button" class="window-btn" data-window="weekly"><span class="lang-en">Weekly</span><span class="lang-sk hidden">Týždenne</span></button>
                            <button type="button" class="window-btn" data-window="monthly"><span class="lang-en">Monthly</span><span class="lang-sk hidden">Mesačne</span></button>
                        </div>
                        <div id="kpiGrid" class="kpi-grid"></div>
                    </div>
                </section>

                <section class="section" id="sales">
                    <div class="section-head">
                        <h2><span class="lang-en">Sales engine</span><span class="lang-sk hidden">Predajný engine</span></h2>
                        <p><span class="lang-en">Professional ecommerce dashboards answer the main question first: are revenue and profit moving in the same direction?</span><span class="lang-sk hidden">Profesionálny ecommerce dashboard má najprv odpovedať na hlavnú otázku: idú tržby a zisk rovnakým smerom?</span></p>
                    </div>
                    <div class="panel chart-card">
                        <div class="card-head">
                            <div>
                                <h3><span class="lang-en">Revenue and profit trajectory</span><span class="lang-sk hidden">Trajektória tržieb a zisku</span></h3>
                                <p><span class="lang-en">Daily values with 7-day smoothing. This is the primary business pulse view.</span><span class="lang-sk hidden">Denné hodnoty so 7-dňovým vyhladením. Toto je primárny pulzný pohľad na biznis.</span></p>
                            </div>
                        </div>
                        <div class="chart-shell tall"><canvas id="revenueProfitChart"></canvas></div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head">
                                <div>
                                    <h3><span class="lang-en">Orders and AOV</span><span class="lang-sk hidden">Objednávky a AOV</span></h3>
                                    <p><span class="lang-en">Order volume as bars, basket size as a line.</span><span class="lang-sk hidden">Objem objednávok ako stĺpce, veľkosť košíka ako línia.</span></p>
                                </div>
                            </div>
                            <div class="chart-shell"><canvas id="ordersAovChart"></canvas></div>
                        </div>
                        <div class="panel chart-card" id="economics-overview">
                            <div class="card-head">
                                <div>
                                    <h3><span class="lang-en">Cost structure</span><span class="lang-sk hidden">Štruktúra nákladov</span></h3>
                                    <p><span class="lang-en">Where the money goes across product, logistics, ads and fixed overhead.</span><span class="lang-sk hidden">Kam odchádzajú peniaze medzi produkt, logistiku, reklamu a fix.</span></p>
                                </div>
                            </div>
                            <div class="chart-shell compact"><canvas id="costMixChart"></canvas></div>
                            <div class="mini-grid">
                                <div class="mini-card"><small><span class="lang-en">Total ads</span><span class="lang-sk hidden">Spolu reklama</span></small><strong>€{total_ads:,.0f}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Refund rate</span><span class="lang-sk hidden">Refund rate</span></small><strong>{_format_mini_value_html(refund_summary.get("refund_rate_pct"), kind="percent")}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Top city</span><span class="lang-sk hidden">Top mesto</span></small><strong>{escape(top_city)}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Top product</span><span class="lang-sk hidden">Top produkt</span></small><strong>{escape(top_product)}</strong></div>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="section" id="economics">
                    <div class="section-head">
                        <h2><span class="lang-en">Economics deep dive</span><span class="lang-sk hidden">Hlbsia ekonomika</span></h2>
                        <p><span class="lang-en">Full daily economics from the original report: revenue, costs, gross margin, contribution and LTV overlays.</span><span class="lang-sk hidden">Plna denna ekonomika z povodneho reportu: trzby, naklady, hruba marza, kontribucia a LTV overlay.</span></p>
                    </div>
                    <div class="panel chart-card" style="margin-bottom:18px;">
                        <div class="mini-grid">
                            <div class="mini-card"><small><span class="lang-en">Pre-ad / order</span><span class="lang-sk hidden">Pre-ad / objednavka</span></small><strong>{_format_mini_value_html(shell_pre_ad_per_order, kind="currency")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Break-even CAC</span><span class="lang-sk hidden">Break-even CAC</span></small><strong>{_format_mini_value_html(break_even_cac, kind="currency")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Payback orders</span><span class="lang-sk hidden">Payback objednavky</span></small><strong>{_format_mini_value_html(shell_payback_orders, kind="number", decimals=2)}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Contribution LTV/CAC</span><span class="lang-sk hidden">Contribution LTV/CAC</span></small><strong>{_format_mini_value_html(shell_contribution_ltv_cac, kind="multiple")}</strong></div>
                        </div>
                    </div>
                    <div class="panel chart-card" style="margin-bottom:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">CM1 / CM2 / CM3 taxonomy</span><span class="lang-sk hidden">CM1 / CM2 / CM3 taxonomia</span></h3><p><span class="lang-en">Normalized margin waterfall: CM1 before paid ads, CM2 after paid ads, CM3 after fixed overhead.</span><span class="lang-sk hidden">Normalizovana marzova waterfall logika: CM1 pred platenou reklamou, CM2 po platenej reklame, CM3 po fixnom overheade.</span></p></div></div>
                        <div class="mini-grid">
                            <div class="mini-card"><small>CM1</small><strong>{_format_mini_value_html(cm1_profit, kind="currency")}</strong><span class="delta up">{_format_mini_value_html(cm1_margin_pct, kind="percent")}</span></div>
                            <div class="mini-card"><small>CM1 / order</small><strong>{_format_mini_value_html(cm1_profit_per_order, kind="currency")}</strong><span class="delta neutral">{_format_mini_value_html(cm1_profit_per_customer, kind="currency")}</span></div>
                            <div class="mini-card"><small>CM2</small><strong>{_format_mini_value_html(cm2_profit, kind="currency")}</strong><span class="delta up">{_format_mini_value_html(cm2_margin_pct, kind="percent")}</span></div>
                            <div class="mini-card"><small>CM2 / order</small><strong>{_format_mini_value_html(cm2_profit_per_order, kind="currency")}</strong><span class="delta neutral"><span class="lang-en">after ads</span><span class="lang-sk hidden">po reklamach</span></span></div>
                            <div class="mini-card"><small>CM3</small><strong>{_format_mini_value_html(cm3_profit, kind="currency")}</strong><span class="delta up">{_format_mini_value_html(cm3_margin_pct, kind="percent")}</span></div>
                            <div class="mini-card"><small>CM3 / order</small><strong>{_format_mini_value_html(cm3_profit_per_order, kind="currency")}</strong><span class="delta neutral"><span class="lang-en">after fixed overhead</span><span class="lang-sk hidden">po fixnom overheade</span></span></div>
                        </div>
                        <p class="muted-note">{escape(cm_taxonomy_note) if cm_taxonomy_note else '<span class="lang-en">CM1 currently excludes payment fees because the reporting model does not ingest them separately.</span><span class="lang-sk hidden">CM1 zatial vylucuje payment fees, pretoze reporting ich zatial nenasava samostatne.</span>'}</p>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Revenue vs total costs</span><span class="lang-sk hidden">Trzby vs celkove naklady</span></h3><p><span class="lang-en">Daily revenue compared with full cost base.</span><span class="lang-sk hidden">Denne trzby oproti plnej nakladovej baze.</span></p></div></div>
                            <div class="chart-shell"><canvas id="dailyEconomicsChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Cost component trend</span><span class="lang-sk hidden">Trend zloziek nakladov</span></h3><p><span class="lang-en">Product, packaging, net shipping, fixed and ads in one view.</span><span class="lang-sk hidden">Produkt, balenie, ciste shipping, fix a reklama v jednom pohlade.</span></p></div></div>
                            <div class="chart-shell"><canvas id="costBreakoutChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Gross margin and ROI</span><span class="lang-sk hidden">Hruba marza a ROI</span></h3><p><span class="lang-en">Tracks product gross margin next to after-cost ROI.</span><span class="lang-sk hidden">Sleduje produktovu hrubu marzu vedla ROI po nakladoch.</span></p></div></div>
                            <div class="chart-shell"><canvas id="grossMarginRoiChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Contribution per order</span><span class="lang-sk hidden">Kontribucia na objednavku</span></h3><p><span class="lang-en">Pre-ad and post-ad order economics side by side.</span><span class="lang-sk hidden">Pre-ad a post-ad ekonomika objednavky vedla seba.</span></p></div></div>
                            <div class="chart-shell"><canvas id="contributionPerOrderChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Items and basket depth</span><span class="lang-sk hidden">Polozky a hlbka kosika</span></h3><p><span class="lang-en">Items sold and average items per order.</span><span class="lang-sk hidden">Predane polozky a priemer poloziek na objednavku.</span></p></div></div>
                            <div class="chart-shell"><canvas id="itemsBasketChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">LTV revenue and running averages</span><span class="lang-sk hidden">LTV trzby a bezne priemery</span></h3><p><span class="lang-en">Blends acquisition LTV with cumulative revenue/profit trend.</span><span class="lang-sk hidden">Spaja akvizicne LTV s kumulativnym trendom trzby a zisku.</span></p></div></div>
                            <div class="chart-shell"><canvas id="ltvRevenueTrendChart"></canvas></div>
                        </div>
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Basket contribution table</span><span class="lang-sk hidden">Tabulka kontribucie kosika</span></h3><p><span class="lang-en">All basket-size economics in one table.</span><span class="lang-sk hidden">Cely pohlad na ekonomiku podla velkosti kosika.</span></p></div></div>
                        <table>
                            <thead><tr><th><span class="lang-en">Basket</span><span class="lang-sk hidden">Kosik</span></th><th><span class="lang-en">Orders</span><span class="lang-sk hidden">Obj.</span></th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th><span class="lang-en">Contribution</span><span class="lang-sk hidden">Kontribucia</span></th><th><span class="lang-en">Per order</span><span class="lang-sk hidden">Na obj.</span></th><th><span class="lang-en">Margin</span><span class="lang-sk hidden">Marza</span></th></tr></thead>
                            <tbody>{basket_rows_html}</tbody>
                        </table>
                    </div>
                </section>

                <section class="section" id="marketing">
                    <div class="section-head">
                        <h2><span class="lang-en">Marketing and ads deep dive</span><span class="lang-sk hidden">Hlbsi marketing a reklama</span></h2>
                        <p><span class="lang-en">Full Facebook/CPO/attribution family brought over from the original richer report.</span><span class="lang-sk hidden">Kompletna Facebook/CPO/atribucna rodina z povodneho bohatsieho reportu.</span></p>
                    </div>
                    <div class="panel chart-card" style="margin-bottom:18px;">
                        <div class="mini-grid">
                            <div class="mini-card"><small><span class="lang-en">Overall CPO</span><span class="lang-sk hidden">Celkove CPO</span></small><strong>€{_num((cost_per_order or {}).get("overall_cpo")):,.2f}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">FB CPO</span><span class="lang-sk hidden">FB CPO</span></small><strong>€{_num((cost_per_order or {}).get("fb_cpo")):,.2f}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Google CPO</span><span class="lang-sk hidden">Google CPO</span></small><strong>{_format_mini_value_html(google_cpo_value, kind="currency")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Best lag</span><span class="lang-sk hidden">Najlepsi lag</span></small><strong>{escape(str((cost_per_order or {}).get("best_attribution_lag") or "N/A"))}</strong></div>
                        </div>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Daily spend, clicks and impressions</span><span class="lang-sk hidden">Denný spend, kliky a impresie</span></h3><p><span class="lang-en">Core Facebook delivery metrics in time.</span><span class="lang-sk hidden">Hlavne Facebook delivery metriky v case.</span></p></div></div>
                            <div class="chart-shell"><canvas id="fbDailyPerformanceChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">CTR, CPC and CPM</span><span class="lang-sk hidden">CTR, CPC a CPM</span></h3><p><span class="lang-en">Traffic quality and pricing layer.</span><span class="lang-sk hidden">Vrstva kvality trafficu a ceny.</span></p></div></div>
                            <div class="chart-shell"><canvas id="fbEfficiencyChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Weekly CPO and ROAS</span><span class="lang-sk hidden">Tyzdenne CPO a ROAS</span></h3><p><span class="lang-en">Weekly smoothing of cost per order and ROAS.</span><span class="lang-sk hidden">Tyzdenne vyhladenie ceny objednavky a ROAS.</span></p></div></div>
                            <div class="chart-shell"><canvas id="weeklyCpoChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Hourly ads vs orders</span><span class="lang-sk hidden">Hodiny: reklama vs objednavky</span></h3><p><span class="lang-en">Hourly ad spend, clicks and order demand together.</span><span class="lang-sk hidden">Hodinovy spend, kliky a dopyt objednavok spolu.</span></p></div></div>
                            <div class="chart-shell"><canvas id="hourlyAdsOrdersChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Ads effectiveness overlay</span><span class="lang-sk hidden">Overlay efektivity reklam</span></h3><p><span class="lang-en">Spend, orders, revenue and profit in one overlay.</span><span class="lang-sk hidden">Spend, objednavky, trzby a zisk v jednom overlay.</span></p></div></div>
                            <div class="chart-shell"><canvas id="adsEffectivenessChart"></canvas></div>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Spend bucket effectiveness</span><span class="lang-sk hidden">Efektivita spend bucketov</span></h3><p><span class="lang-en">Average output by spend range.</span><span class="lang-sk hidden">Priemerny vystup podla spend rozsahu.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Range</span><span class="lang-sk hidden">Rozsah</span></th><th><span class="lang-en">Spend</span><span class="lang-sk hidden">Spend</span></th><th><span class="lang-en">Orders</span><span class="lang-sk hidden">Obj.</span></th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th><span class="lang-en">Profit</span><span class="lang-sk hidden">Zisk</span></th><th>ROAS</th></tr></thead>
                                <tbody>{spend_effectiveness_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Campaign performance</span><span class="lang-sk hidden">Vykon kampani</span></h3><p><span class="lang-en">Campaign-level Facebook delivery and platform conversions.</span><span class="lang-sk hidden">Facebook delivery a platformove konverzie na urovni kampani.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Campaign</span><span class="lang-sk hidden">Kampan</span></th><th><span class="lang-en">Spend</span><span class="lang-sk hidden">Spend</span></th><th><span class="lang-en">Clicks</span><span class="lang-sk hidden">Kliky</span></th><th>CTR</th><th>CPC</th><th><span class="lang-en">Platform conv.</span><span class="lang-sk hidden">Platform konv.</span></th></tr></thead>
                                <tbody>{fb_campaign_rows_html}</tbody>
                            </table>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Campaign attribution estimate</span><span class="lang-sk hidden">Odhad atribucie kampani</span></h3><p><span class="lang-en">Estimated attributed orders, attributed CPO and ROAS by campaign.</span><span class="lang-sk hidden">Odhad atribuovanych objednavok, atribucneho CPO a ROAS podla kampane.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Campaign</span><span class="lang-sk hidden">Kampan</span></th><th><span class="lang-en">Spend</span><span class="lang-sk hidden">Spend</span></th><th><span class="lang-en">Attributed orders est.</span><span class="lang-sk hidden">Odhad atrib. obj.</span></th><th><span class="lang-en">Cost / attributed order</span><span class="lang-sk hidden">Naklad / atrib. obj.</span></th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th>ROAS</th></tr></thead>
                                <tbody>{"".join(f"<tr><td>{escape(str(row.get('campaign_name') or '-'))}</td><td>€{_num(row.get('spend')):,.2f}</td><td>{_num(row.get('attributed_orders_est', row.get('estimated_orders'))):.1f}</td><td>€{_num(row.get('cost_per_attributed_order', row.get('estimated_cpo'))):,.2f}</td><td>€{_num(row.get('estimated_revenue')):,.2f}</td><td>{_num(row.get('estimated_roas')):.2f}x</td></tr>" for row in campaign_cpo) or '<tr><td colspan=\"6\"><span class=\"lang-en\">No campaign attribution data available.</span><span class=\"lang-sk hidden\">Atribucne data kampani nie su dostupne.</span></td></tr>'}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Day-of-week effectiveness</span><span class="lang-sk hidden">Day-of-week efektivita</span></h3><p><span class="lang-en">Average spend, orders, revenue and profit by weekday.</span><span class="lang-sk hidden">Priemerny spend, objednavky, trzby a zisk podla dna v tyzdni.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Day</span><span class="lang-sk hidden">Den</span></th><th><span class="lang-en">FB spend</span><span class="lang-sk hidden">FB spend</span></th><th><span class="lang-en">Orders</span><span class="lang-sk hidden">Obj.</span></th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th><span class="lang-en">Profit</span><span class="lang-sk hidden">Zisk</span></th></tr></thead>
                                <tbody>{dow_effectiveness_rows_html}</tbody>
                            </table>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">FB spend reconciliation</span><span class="lang-sk hidden">FB spend reconciliation</span></h3><p><span class="lang-en">Daily-source spend versus campaign-source spend from the original reconciliation check.</span><span class="lang-sk hidden">Denny source spend oproti campaign source spendu z povodnej reconciliation kontroly.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Metric</span><span class="lang-sk hidden">Metrika</span></th><th><span class="lang-en">Value</span><span class="lang-sk hidden">Hodnota</span></th></tr></thead>
                                <tbody>
                                    <tr><td><span class="lang-en">Daily source spend</span><span class="lang-sk hidden">Denny source spend</span></td><td>&euro;{_num(((cost_per_order or {}).get('fb_spend_reconciliation') or {}).get('daily_source_spend')):,.2f}</td></tr>
                                    <tr><td><span class="lang-en">Campaign source spend</span><span class="lang-sk hidden">Campaign source spend</span></td><td>&euro;{_num(((cost_per_order or {}).get('fb_spend_reconciliation') or {}).get('campaign_source_spend')):,.2f}</td></tr>
                                    <tr><td><span class="lang-en">Difference</span><span class="lang-sk hidden">Rozdiel</span></td><td>&euro;{_num(((cost_per_order or {}).get('fb_spend_reconciliation') or {}).get('difference')):,.2f}</td></tr>
                                    <tr><td><span class="lang-en">Difference %</span><span class="lang-sk hidden">Rozdiel %</span></td><td>{_num(((cost_per_order or {}).get('fb_spend_reconciliation') or {}).get('difference_pct')):.2f}%</td></tr>
                                </tbody>
                            </table>
                        </div>
                    </div>

                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Attribution QA guardrails</span><span class="lang-sk hidden">Attribution QA guardrails</span></h3><p><span class="lang-en">Coverage and oversubscription checks that catch attribution fallback problems early.</span><span class="lang-sk hidden">Kontroly coverage a oversubscription, ktore skoro zachytia problemy s attribution fallbackom.</span></p></div></div>
                        <div class="mini-grid">
                            <div class="mini-card"><small><span class="lang-en">Coverage ratio</span><span class="lang-sk hidden">Coverage ratio</span></small><strong>{_format_mini_value_html(attribution_qa.get("coverage_ratio"), kind="multiple")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Oversubscription</span><span class="lang-sk hidden">Oversubscription</span></small><strong>{_format_mini_value_html(attribution_qa.get("oversubscription_ratio"), kind="multiple")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Campaign rows</span><span class="lang-sk hidden">Pocet kampani</span></small><strong>{int(round(_num(attribution_qa.get("campaign_rows"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">CPA mismatches</span><span class="lang-sk hidden">CPA nezrovnalosti</span></small><strong>{int(round(_num(attribution_qa.get("platform_cost_mismatch_count"))))}</strong></div>
                        </div>
                        <ul class="warning-list">{attribution_warning_items_html}</ul>
                    </div>

                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Reach, clicks and CTR</span><span class="lang-sk hidden">Reach, kliky a CTR</span></h3><p><span class="lang-en">Extra Facebook delivery view from the detailed ad metrics.</span><span class="lang-sk hidden">Dalsi Facebook delivery pohlad z detailnych reklamnych metrik.</span></p></div></div>
                            <div class="chart-shell"><canvas id="fbReachClicksChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Campaign spend mix</span><span class="lang-sk hidden">Mix spendu kampani</span></h3><p><span class="lang-en">How total Facebook spend is concentrated across campaigns.</span><span class="lang-sk hidden">Ako sa cely Facebook spend koncentruje medzi kampanami.</span></p></div></div>
                            <div class="chart-shell compact"><canvas id="campaignSpendMixChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Campaign efficiency comparison</span><span class="lang-sk hidden">Porovnanie efektivity kampani</span></h3><p><span class="lang-en">CTR, CPC and cost per conversion on one campaign view.</span><span class="lang-sk hidden">CTR, CPC a cost per conversion v jednom kampanovom pohlade.</span></p></div></div>
                            <div class="chart-shell"><canvas id="campaignEfficiencyChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Hourly CPO and ROAS</span><span class="lang-sk hidden">Hodinove CPO a ROAS</span></h3><p><span class="lang-en">Estimated hourly order economics from spend and hourly order demand.</span><span class="lang-sk hidden">Odhad hodinovej ekonomiky objednavok zo spendu a hodinoveho dopytu.</span></p></div></div>
                            <div class="chart-shell"><canvas id="hourlyEfficiencyChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Facebook day-of-week efficiency</span><span class="lang-sk hidden">Facebook efektivita podla dna v tyzdni</span></h3><p><span class="lang-en">Spend, CTR and CPC across weekdays.</span><span class="lang-sk hidden">Spend, CTR a CPC napriec dnami v tyzdni.</span></p></div></div>
                            <div class="chart-shell"><canvas id="fbDowEfficiencyChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Ads correlation diagnostics</span><span class="lang-sk hidden">Korelacie reklamnych vydavkov</span></h3><p><span class="lang-en">Correlation layer from the full ads effectiveness analysis.</span><span class="lang-sk hidden">Korelacna vrstva z plnej analyzy efektivity reklam.</span></p></div></div>
                            <div class="mini-grid">
                            <div class="mini-card"><small><span class="lang-en">Spend vs orders</span><span class="lang-sk hidden">Spend vs objednavky</span></small><strong>{_format_mini_value_html(ads_correlations.get('spend_orders_correlation'), kind="number", decimals=2)}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Spend vs revenue</span><span class="lang-sk hidden">Spend vs trzby</span></small><strong>{_format_mini_value_html(ads_correlations.get('spend_revenue_correlation'), kind="number", decimals=2)}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Spend vs profit</span><span class="lang-sk hidden">Spend vs zisk</span></small><strong>{_format_mini_value_html(ads_correlations.get('spend_profit_correlation'), kind="number", decimals=2)}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Best lag corr.</span><span class="lang-sk hidden">Best lag corr.</span></small><strong>{_num((cost_per_order or {}).get('best_lag_correlation')):.2f}</strong></div>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="section" id="customers">
                    <div class="section-head">
                        <h2><span class="lang-en">Customer quality and retention</span><span class="lang-sk hidden">Kvalita zákazníkov a retencia</span></h2>
                        <p><span class="lang-en">This extends the main dashboard shell with retention, refunds and concentration data from the richer reporting build.</span><span class="lang-sk hidden">Toto rozsiruje hlavny dashboard shell o retenciu, refundy a koncentraciu z bohatsieho reportingu.</span></p>
                    </div>
                    <div class="panel chart-card" style="margin-bottom:18px;">
                        <div class="mini-grid">
                            <div class="mini-card"><small><span class="lang-en">Repeat rate</span><span class="lang-sk hidden">Repeat rate</span></small><strong>{repeat_rate:.1f}%</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Repeat customers</span><span class="lang-sk hidden">Vracajuci sa zakaznici</span></small><strong>{repeat_customers}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Avg days to 2nd order</span><span class="lang-sk hidden">Priemer dni do 2. objednavky</span></small><strong>{(f"{avg_days_to_2nd:.0f}" if avg_days_to_2nd is not None else "N/A")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Top 10% revenue share</span><span class="lang-sk hidden">Podiel top 10% zakaznikov</span></small><strong>{top_10_share:.1f}%</strong></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Sample funnel</span><span class="lang-sk hidden">Sample funnel</span></h3><p><span class="lang-en">First-order sample customers tracked into repeat and full-size conversion windows.</span><span class="lang-sk hidden">Zakaznici vstupujuci cez sample v prvej objednavke sledovani do repeat a full-size konverznych okien.</span></p></div></div>
                            <div class="mini-grid">
                                <div class="mini-card"><small><span class="lang-en">Entry customers</span><span class="lang-sk hidden">Vstupni zakaznici</span></small><strong>{sample_entry_customers}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Repeat 30d</span><span class="lang-sk hidden">Repeat 30 dni</span></small><strong>{_format_mini_value_html(sample_repeat_30d, kind="percent", decimals=1)}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Full-size 30d</span><span class="lang-sk hidden">Full-size 30 dni</span></small><strong>{_format_mini_value_html(sample_fullsize_30d, kind="percent", decimals=1)}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Full-size 60d</span><span class="lang-sk hidden">Full-size 60 dni</span></small><strong>{_format_mini_value_html(sample_fullsize_60d, kind="percent", decimals=1)}</strong></div>
                            </div>
                            <div class="mini-grid" style="margin-top:12px;">
                                <div class="mini-card"><small><span class="lang-en">Median days to full-size</span><span class="lang-sk hidden">Median dni do full-size</span></small><strong>{(f"{sample_median_days_fullsize:.0f}" if sample_median_days_fullsize is not None else "N/A")}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Top entry product</span><span class="lang-sk hidden">Top vstupny produkt</span></small><strong>{sample_top_entry_product}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Entry revenue</span><span class="lang-sk hidden">Vstupne trzby</span></small><strong>{_format_mini_value_html(sample_summary.get("entry_revenue"), kind="currency")}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Sample first-order share</span><span class="lang-sk hidden">Podiel sample prvej objednavky</span></small><strong>{_format_mini_value_html(sample_summary.get("sample_first_order_share_pct"), kind="percent", decimals=1)}</strong></div>
                            </div>
                            <div class="chart-shell"><canvas id="sampleFunnelChart"></canvas></div>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Sample entry product quality</span><span class="lang-sk hidden">Kvalita sample vstupnych produktov</span></h3><p><span class="lang-en">Top sample entry products ranked by 30d and 60d conversion into repeat and full-size orders.</span><span class="lang-sk hidden">Top sample vstupne produkty podla 30d a 60d konverzie do repeat a full-size objednavok.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Sample</span><span class="lang-sk hidden">Sample</span></th><th><span class="lang-en">Customers</span><span class="lang-sk hidden">Zakaznici</span></th><th><span class="lang-en">Repeat 30d</span><span class="lang-sk hidden">Repeat 30d</span></th><th><span class="lang-en">Full-size 30d</span><span class="lang-sk hidden">Full-size 30d</span></th><th><span class="lang-en">Full-size 60d</span><span class="lang-sk hidden">Full-size 60d</span></th><th>200ml 60d</th><th>500ml 60d</th></tr></thead>
                                <tbody>{sample_entry_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Refill cohort timing</span><span class="lang-sk hidden">Casovanie refill kohort</span></h3><p><span class="lang-en">Second-order refill speed by first-order entry bucket so refill timing is measured by cohort, not only global repeat rate.</span><span class="lang-sk hidden">Rychlost druhej objednavky podla vstupneho bucketu prvej objednavky, aby sa refill meral kohortne a nie len globalnym repeat rate.</span></p></div></div>
                            <div class="mini-grid">
                                <div class="mini-card"><small><span class="lang-en">Entry customers</span><span class="lang-sk hidden">Vstupni zakaznici</span></small><strong>{refill_entry_customers}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Sample refill 60d</span><span class="lang-sk hidden">Sample refill 60d</span></small><strong>{_format_mini_value_html(refill_sample_60d, kind="percent", decimals=1)}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Sample refill 90d</span><span class="lang-sk hidden">Sample refill 90d</span></small><strong>{_format_mini_value_html(refill_sample_90d, kind="percent", decimals=1)}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Avg days to 2nd</span><span class="lang-sk hidden">Priemer dni do 2. objednavky</span></small><strong>{_format_mini_value_html(refill_sample_days_to_2nd, kind="number", decimals=1)}</strong></div>
                            </div>
                            <div class="mini-grid" style="margin-top:12px;">
                                <div class="mini-card"><small><span class="lang-en">Avg 2nd order AOV</span><span class="lang-sk hidden">Priemerna hodnota 2. objednavky</span></small><strong>{_format_mini_value_html(refill_second_order_aov, kind="currency")}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Dominant entry bucket</span><span class="lang-sk hidden">Dominantny vstupny bucket</span></small><strong>{refill_dominant_bucket}</strong></div>
                            </div>
                            <div class="chart-shell"><canvas id="refillCohortWindowChart"></canvas></div>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Refill bucket quality</span><span class="lang-sk hidden">Kvalita refill bucketov</span></h3><p><span class="lang-en">Compare second-order speed and value across sample-only and full-size entry cohorts.</span><span class="lang-sk hidden">Porovnanie rychlosti a hodnoty druhej objednavky medzi sample-only a full-size vstupnymi kohortami.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Entry bucket</span><span class="lang-sk hidden">Vstupny bucket</span></th><th><span class="lang-en">Customers</span><span class="lang-sk hidden">Zakaznici</span></th><th>60d</th><th>90d</th><th><span class="lang-en">Avg days</span><span class="lang-sk hidden">Priemer dni</span></th><th><span class="lang-en">2nd AOV</span><span class="lang-sk hidden">2. AOV</span></th></tr></thead>
                                <tbody>{refill_bucket_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head">
                                <div>
                                    <h3><span class="lang-en">Margin corridor</span><span class="lang-sk hidden">Koridor marze</span></h3>
                                    <p><span class="lang-en">Pre-ad and post-ad margins shown together to see if growth quality is holding.</span><span class="lang-sk hidden">Pre-ad a post-ad marza spolu, aby bolo vidno kvalitu rastu.</span></p>
                                </div>
                            </div>
                            <div class="chart-shell"><canvas id="marginChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head">
                                <div>
                                    <h3><span class="lang-en">New vs returning revenue</span><span class="lang-sk hidden">Novi vs vracajuci sa</span></h3>
                                    <p><span class="lang-en">Acquisition and repeat demand split out in the same timeline.</span><span class="lang-sk hidden">Akvizicia a opakovany dopyt na jednej casovej osi.</span></p>
                                </div>
                            </div>
                            <div class="chart-shell"><canvas id="customerMixChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head">
                                <div>
                                    <h3><span class="lang-en">Refund trend</span><span class="lang-sk hidden">Trend refundov</span></h3>
                                    <p><span class="lang-en">Daily refund rate makes operational friction visible instead of hiding it in totals.</span><span class="lang-sk hidden">Denná miera refundov odhaľuje operačné problémy, nie len súčet.</span></p>
                                </div>
                            </div>
                            <div class="chart-shell"><canvas id="refundRateChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head">
                                <div>
                                    <h3><span class="lang-en">Cohort retention</span><span class="lang-sk hidden">Kohortna retencia</span></h3>
                                    <p><span class="lang-en">Retention by acquisition cohort to show whether repeats are stable or weakening.</span><span class="lang-sk hidden">Retencia podla kohort, aby bolo vidno ci sa opakovane nakupy drzia.</span></p>
                                </div>
                            </div>
                            <div class="chart-shell"><canvas id="cohortRetentionChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Top customers by revenue</span><span class="lang-sk hidden">Top zakaznici podla trzby</span></h3><p><span class="lang-en">Customer concentration tells you how dependent revenue is on a small group.</span><span class="lang-sk hidden">Koncentracia zakaznikov ukaze zavislost trzby od malej skupiny.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Customer</span><span class="lang-sk hidden">Zakaznik</span></th><th><span class="lang-en">Orders</span><span class="lang-sk hidden">Objednavky</span></th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th><span class="lang-en">Profit</span><span class="lang-sk hidden">Zisk</span></th><th><span class="lang-en">Share</span><span class="lang-sk hidden">Podiel</span></th></tr></thead>
                                <tbody>{customer_rows_html}</tbody>
                            </table>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Cohort retention table</span><span class="lang-sk hidden">Tabulka kohortnej retencie</span></h3><p><span class="lang-en">Detailed cohort read for 2nd to 5th order retention.</span><span class="lang-sk hidden">Detailny pohlad na retenciu od 2. po 5. objednavku.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Cohort</span><span class="lang-sk hidden">Kohorta</span></th><th>2nd</th><th>3rd</th><th>4th</th><th>5th</th></tr></thead>
                                <tbody>{cohort_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Returning share by week</span><span class="lang-sk hidden">Podiel vracajucich sa podla tyzdna</span></h3><p><span class="lang-en">Weekly new vs returning split from the richer analysis.</span><span class="lang-sk hidden">Tyzdenny split novych a vracajucich sa z bohatsiej analyzy.</span></p></div></div>
                            <div class="chart-shell"><canvas id="returningShareChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">CLV, CAC and return time</span><span class="lang-sk hidden">CLV, CAC a cas navratu</span></h3><p><span class="lang-en">Weekly customer value and acquisition economics.</span><span class="lang-sk hidden">Tyzdenna hodnota zakaznika a ekonomika akvizicie.</span></p></div></div>
                            <div class="chart-shell"><canvas id="clvCacTrendChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Order size distribution</span><span class="lang-sk hidden">Rozdelenie velkosti objednavok</span></h3><p><span class="lang-en">Daily order mix by number of items.</span><span class="lang-sk hidden">Denný mix objednavok podla poctu poloziek.</span></p></div></div>
                            <div class="chart-shell"><canvas id="orderSizeChart"></canvas></div>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Product combinations</span><span class="lang-sk hidden">Kombinacie produktov</span></h3><p><span class="lang-en">Most frequent multi-item combinations.</span><span class="lang-sk hidden">Najcastejsie viacpolozkove kombinacie.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Size</span><span class="lang-sk hidden">Velkost</span></th><th><span class="lang-en">Combination</span><span class="lang-sk hidden">Kombinacia</span></th><th><span class="lang-en">Count</span><span class="lang-sk hidden">Pocet</span></th><th><span class="lang-en">Price</span><span class="lang-sk hidden">Cena</span></th></tr></thead>
                                <tbody>{combinations_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Retention by first item</span><span class="lang-sk hidden">Retencia podla prveho produktu</span></h3><p><span class="lang-en">How the first purchased product influences repeat behavior.</span><span class="lang-sk hidden">Ako prvy kupeny produkt ovplyvnuje opakovane nakupy.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Item</span><span class="lang-sk hidden">Produkt</span></th><th><span class="lang-en">Customers</span><span class="lang-sk hidden">Zakaznici</span></th><th>2nd</th><th>3rd</th><th><span class="lang-en">Avg orders</span><span class="lang-sk hidden">Priem. obj.</span></th></tr></thead>
                                <tbody>{item_retention_rows_html}</tbody>
                            </table>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Same-item repurchase</span><span class="lang-sk hidden">Opakovany nakup rovnakeho produktu</span></h3><p><span class="lang-en">How often customers buy the same item again.</span><span class="lang-sk hidden">Ako casto zakaznik kupi ten isty produkt znovu.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Item</span><span class="lang-sk hidden">Produkt</span></th><th><span class="lang-en">Customers</span><span class="lang-sk hidden">Zak.</span></th><th>2x</th><th>3x</th><th><span class="lang-en">Days between</span><span class="lang-sk hidden">Dni medzi</span></th></tr></thead>
                                <tbody>{same_item_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Time to next order by first item</span><span class="lang-sk hidden">Cas do dalsej objednavky podla prveho produktu</span></h3><p><span class="lang-en">Average and median days to 2nd/3rd order for the first purchased item group.</span><span class="lang-sk hidden">Priemerny a medianovy cas do 2./3. objednavky podla prveho kupeneho produktu.</span></p></div></div>
                        <table>
                            <thead><tr><th><span class="lang-en">Item</span><span class="lang-sk hidden">Produkt</span></th><th><span class="lang-en">Customers</span><span class="lang-sk hidden">Zakaznici</span></th><th><span class="lang-en">Avg 2nd</span><span class="lang-sk hidden">Priem. 2.</span></th><th><span class="lang-en">Median 2nd</span><span class="lang-sk hidden">Median 2.</span></th><th><span class="lang-en">Avg 3rd</span><span class="lang-sk hidden">Priem. 3.</span></th></tr></thead>
                            <tbody>{time_to_nth_rows_html}</tbody>
                        </table>
                    </div>

                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Customer concentration</span><span class="lang-sk hidden">Koncentracia zakaznikov</span></h3><p><span class="lang-en">Revenue share captured by the top customers.</span><span class="lang-sk hidden">Podiel trzby zachyteny top zakaznikmi.</span></p></div></div>
                            <div class="chart-shell"><canvas id="customerConcentrationChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Return time and LTV/CAC</span><span class="lang-sk hidden">Cas navratu a LTV/CAC</span></h3><p><span class="lang-en">Average days to return next to the weekly LTV/CAC ratio.</span><span class="lang-sk hidden">Priemer dni do navratu vedla tyzdenneho pomeru LTV/CAC.</span></p></div></div>
                            <div class="chart-shell"><canvas id="returnTimeLtvChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Order frequency distribution</span><span class="lang-sk hidden">Rozdelenie frekvencie objednavok</span></h3><p><span class="lang-en">How many customers stop at 1 order versus 2, 3 and more.</span><span class="lang-sk hidden">Kolko zakaznikov skonci pri 1 objednavke versus 2, 3 a viac.</span></p></div></div>
                            <div class="chart-shell"><canvas id="orderFrequencyChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Time between repeat orders</span><span class="lang-sk hidden">Cas medzi opakovanymi objednavkami</span></h3><p><span class="lang-en">Distribution of the delay between consecutive repeat orders.</span><span class="lang-sk hidden">Rozdelenie casu medzi po sebe iducimi opakovanymi objednavkami.</span></p></div></div>
                            <div class="chart-shell"><canvas id="timeBetweenOrdersChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Time between order transitions</span><span class="lang-sk hidden">Cas medzi prechodmi objednavok</span></h3><p><span class="lang-en">How fast customers move from 1?2, 2?3, 3?4 and beyond.</span><span class="lang-sk hidden">Ako rychlo sa zakaznici posuvaju z 1?2, 2?3, 3?4 a dalej.</span></p></div></div>
                            <div class="chart-shell"><canvas id="timeBetweenByOrderChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Time to nth order</span><span class="lang-sk hidden">Cas do n-tej objednavky</span></h3><p><span class="lang-en">Average days from first order to 2nd, 3rd, 4th and later orders.</span><span class="lang-sk hidden">Priemerny cas od prveho nakupu po 2., 3., 4. a dalsiu objednavku.</span></p></div></div>
                            <div class="chart-shell"><canvas id="timeToNthOrderChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Value by order sequence</span><span class="lang-sk hidden">Hodnota podla poradia objednavky</span></h3><p><span class="lang-en">Average order value, items per order and price per item by order number.</span><span class="lang-sk hidden">Priemerna objednavka, polozky na objednavku a cena za polozku podla poradia objednavky.</span></p></div></div>
                            <div class="chart-shell"><canvas id="orderSequenceValueChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Mature cohort retention</span><span class="lang-sk hidden">Retencia zrelych kohort</span></h3><p><span class="lang-en">Time-bias-free view using only cohorts old enough for repeat behavior.</span><span class="lang-sk hidden">Pohlad bez casoveho biasu len na kohorty dost stare na opakovane nakupy.</span></p></div></div>
                            <div class="chart-shell"><canvas id="matureCohortChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Order frequency table</span><span class="lang-sk hidden">Tabulka frekvencie objednavok</span></h3></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Frequency</span><span class="lang-sk hidden">Frekvencia</span></th><th><span class="lang-en">Customers</span><span class="lang-sk hidden">Zakaznici</span></th><th><span class="lang-en">Orders</span><span class="lang-sk hidden">Objednavky</span></th><th><span class="lang-en">Cust. %</span><span class="lang-sk hidden">Zak. %</span></th><th><span class="lang-en">Orders %</span><span class="lang-sk hidden">Obj. %</span></th></tr></thead>
                                <tbody>{order_frequency_rows_html}</tbody>
                            </table>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Time-between-orders table</span><span class="lang-sk hidden">Tabulka casu medzi objednavkami</span></h3></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Bucket</span><span class="lang-sk hidden">Bucket</span></th><th><span class="lang-en">Count</span><span class="lang-sk hidden">Pocet</span></th><th><span class="lang-en">Share</span><span class="lang-sk hidden">Podiel</span></th></tr></thead>
                                <tbody>{time_between_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Order transition timing table</span><span class="lang-sk hidden">Tabulka casu medzi prechodmi</span></h3></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Transition</span><span class="lang-sk hidden">Prechod</span></th><th><span class="lang-en">Count</span><span class="lang-sk hidden">Pocet</span></th><th><span class="lang-en">Avg days</span><span class="lang-sk hidden">Priem. dni</span></th><th><span class="lang-en">Median</span><span class="lang-sk hidden">Median</span></th><th><span class="lang-en">Min</span><span class="lang-sk hidden">Min</span></th><th><span class="lang-en">Max</span><span class="lang-sk hidden">Max</span></th></tr></thead>
                                <tbody>{time_between_by_order_rows_html}</tbody>
                            </table>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Value by order number table</span><span class="lang-sk hidden">Tabulka hodnoty podla poradia objednavky</span></h3></div></div>
                            <table>
                                <thead><tr><th>#</th><th><span class="lang-en">AOV</span><span class="lang-sk hidden">AOV</span></th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th><span class="lang-en">Orders</span><span class="lang-sk hidden">Obj.</span></th><th><span class="lang-en">Items / order</span><span class="lang-sk hidden">Polozky / obj.</span></th><th><span class="lang-en">Price / item</span><span class="lang-sk hidden">Cena / polozku</span></th></tr></thead>
                                <tbody>{revenue_by_order_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Mature cohort retention table</span><span class="lang-sk hidden">Tabulka retencie zrelych kohort</span></h3></div></div>
                        <table>
                            <thead><tr><th><span class="lang-en">Cohort</span><span class="lang-sk hidden">Kohorta</span></th><th><span class="lang-en">Age days</span><span class="lang-sk hidden">Vek dni</span></th><th>2nd</th><th>3rd</th><th>4th</th><th>5th</th></tr></thead>
                            <tbody>{mature_cohort_rows_html}</tbody>
                        </table>
                    </div>
                </section>

                <section class="section" id="patterns">
                    <div class="section-head">
                        <h2><span class="lang-en">Calendar patterns and weather</span><span class="lang-sk hidden">Kalendarn? patterny a pocasie</span></h2>
                        <p><span class="lang-en">This brings the richer pattern analysis from the legacy report into the cleaner main dashboard shell.</span><span class="lang-sk hidden">Sem prenasame bohatsie patterny zo starsieho reportu do cistejsieho hlavneho dashboard shellu.</span></p>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Day of week pattern</span><span class="lang-sk hidden">Pattern dna v tyzdni</span></h3><p><span class="lang-en">Revenue, orders and FB spend by weekday.</span><span class="lang-sk hidden">Trzby, objednavky a FB spend podla dna v tyzdni.</span></p></div></div>
                            <div class="chart-shell"><canvas id="dayOfWeekChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Week of month pattern</span><span class="lang-sk hidden">Pattern tyzdna v mesiaci</span></h3><p><span class="lang-en">Equalized week-in-month view using average daily revenue and profit.</span><span class="lang-sk hidden">Equalizovany pohlad tyzdna v mesiaci cez priemerne denne trzby a zisk.</span></p></div></div>
                            <div class="chart-shell"><canvas id="weekOfMonthChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Day of month pattern</span><span class="lang-sk hidden">Pattern dna v mesiaci</span></h3><p><span class="lang-en">Normalized phase-of-month signal using average revenue and profit per occurrence.</span><span class="lang-sk hidden">Normalizovany signal fazy mesiaca cez priemer na vyskyt dna.</span></p></div></div>
                            <div class="chart-shell"><canvas id="dayOfMonthChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Weather uplift</span><span class="lang-sk hidden">Vplyv pocasia</span></h3><p><span class="lang-en">Bucket-based revenue and profit delta versus weekday baseline.</span><span class="lang-sk hidden">Rozdiel trzby a zisku podla pocasia oproti weekday baseline.</span></p></div></div>
                            <div class="chart-shell"><canvas id="weatherImpactChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Weather daily overlay</span><span class="lang-sk hidden">Denný overlay počasia</span></h3><p><span class="lang-en">Daily weather severity against revenue and profit.</span><span class="lang-sk hidden">Denná sila zleho pocasia oproti trzbe a zisku.</span></p></div></div>
                            <div class="chart-shell"><canvas id="weatherDailyChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Payday window</span><span class="lang-sk hidden">Vyplatne okno</span></h3><p><span class="lang-en">Average daily revenue/profit by salary window.</span><span class="lang-sk hidden">Priemerne denne trzby/zisk podla vyplatneho okna.</span></p></div></div>
                            <div class="chart-shell"><canvas id="paydayWindowChart"></canvas></div>
                        </div>
                    </div>
                    <div class="panel chart-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Day-hour heatmap</span><span class="lang-sk hidden">Heatmap dna a hodiny</span></h3><p><span class="lang-en">Order concentration by weekday and hour.</span><span class="lang-sk hidden">Koncentracia objednavok podla dna a hodiny.</span></p></div></div>
                        <div class="chart-shell compact"><canvas id="heatmapChart"></canvas></div>
                    </div>
                </section>

                <section class="section" id="geography">
                    <div class="section-head">
                        <h2><span class="lang-en">Geography</span><span class="lang-sk hidden">Geografia</span></h2>
                        <p><span class="lang-en">Concentration reads help explain where demand is strongest and whether economics differ by market.</span><span class="lang-sk hidden">Geografia ukazuje kde je najsilnejsi dopyt a ako sa lisi ekonomika trhu.</span></p>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Top cities by revenue</span><span class="lang-sk hidden">Top mesta podla trzby</span></h3></div></div>
                            <div class="chart-shell compact"><canvas id="cityChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Country split</span><span class="lang-sk hidden">Rozdelenie krajin</span></h3></div></div>
                            <div class="chart-shell compact"><canvas id="countryChart"></canvas></div>
                        </div>
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Geo confidence guardrails</span><span class="lang-sk hidden">Geo confidence guardrails</span></h3><p><span class="lang-en">Small-sample countries stay visible, but low-confidence economics should not drive strategic market decisions.</span><span class="lang-sk hidden">Krajiny s malou vzorkou ostavaju viditelne, ale ich ekonomika nema sluzit ako strategicky insight.</span></p></div></div>
                        <div class="mini-grid">
                            <div class="mini-card"><small><span class="lang-en">Ready countries</span><span class="lang-sk hidden">Pripravene krajiny</span></small><strong>{int(round(_num(geo_qa.get("ready_count"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Observe countries</span><span class="lang-sk hidden">Sledovane krajiny</span></small><strong>{int(round(_num(geo_qa.get("observe_count"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Ignore countries</span><span class="lang-sk hidden">Ignorovane krajiny</span></small><strong>{int(round(_num(geo_qa.get("ignore_count"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Unknown country rate</span><span class="lang-sk hidden">Podiel neznamej krajiny</span></small><strong>{_format_mini_value_html(geo_qa.get("unknown_country_rate"), kind="percent")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Low-conf. order share</span><span class="lang-sk hidden">Podiel objednavok s nizkou istotou</span></small><strong>{_format_mini_value_html(geo_qa.get("low_confidence_order_share_pct"), kind="percent")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Low-conf. revenue share</span><span class="lang-sk hidden">Podiel trzby s nizkou istotou</span></small><strong>{_format_mini_value_html(geo_qa.get("low_confidence_revenue_share_pct"), kind="percent")}</strong></div>
                        </div>
                        {geo_warning_block_html}
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Data assertions</span><span class="lang-sk hidden">Datove assertions</span></h3><p><span class="lang-en">Pipeline-level parity, arithmetic and dimension completeness checks.</span><span class="lang-sk hidden">Kontroly parity, aritmetiky a uplnosti dimenzii priamo v pipeline.</span></p></div></div>
                        <div class="mini-grid">
                            <div class="mini-card"><small><span class="lang-en">Critical failures</span><span class="lang-sk hidden">Kriticke chyby</span></small><strong>{int(round(_num(data_assertions_qa.get("failure_count"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Warnings</span><span class="lang-sk hidden">Warningy</span></small><strong>{int(round(_num(data_assertions_qa.get("warning_count"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Shell parity failures</span><span class="lang-sk hidden">Shell parity chyby</span></small><strong>{int(round(_num(data_assertions_qa.get("shell_parity_failures"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Platform CPA mismatches</span><span class="lang-sk hidden">Platform CPA nezrovnalosti</span></small><strong>{int(round(_num(data_assertions_qa.get("platform_cpa_mismatches"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Attributed CPA mismatches</span><span class="lang-sk hidden">Attributed CPA nezrovnalosti</span></small><strong>{int(round(_num(data_assertions_qa.get("attributed_cpa_mismatches"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Null label rate</span><span class="lang-sk hidden">Podiel null labelov</span></small><strong>{_format_mini_value_html(data_assertions_qa.get("null_label_rate_pct"), kind="percent")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Attributed orders ratio</span><span class="lang-sk hidden">Pomer attributed objednavok</span></small><strong>{_format_mini_value_html(_num(data_assertions_qa.get("attributed_orders_ratio")) * 100 if data_assertions_qa.get("attributed_orders_ratio") is not None else None, kind="percent")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Missing country labels</span><span class="lang-sk hidden">Chybajuce country labely</span></small><strong>{int(round(_num(data_assertions_qa.get("country_missing"))))}</strong></div>
                        </div>
                        {data_assertion_warning_block_html}
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Smoothed fixed-margin alerts</span><span class="lang-sk hidden">Vyhladene alerty fixnej marze</span></h3><p><span class="lang-en">Tracks whether extreme fixed-margin days remain after 7-day smoothing.</span><span class="lang-sk hidden">Sleduje, ci po 7-dnovom smoothingu stale ostavaju extremne dni s fixnou marzou.</span></p></div></div>
                        <div class="mini-grid">
                            <div class="mini-card"><small><span class="lang-en">Raw extreme days</span><span class="lang-sk hidden">Surove extremne dni</span></small><strong>{int(round(_num(margin_stability_qa.get("raw_extreme_days"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Smoothed extreme days</span><span class="lang-sk hidden">Vyhladene extremne dni</span></small><strong>{int(round(_num(margin_stability_qa.get("smoothed_extreme_days"))))}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Min smoothed margin</span><span class="lang-sk hidden">Min vyhladena marza</span></small><strong>{_format_mini_value_html(margin_stability_qa.get("min_smoothed_margin_pct"), kind="percent")}</strong></div>
                            <div class="mini-card"><small><span class="lang-en">Max smoothed margin</span><span class="lang-sk hidden">Max vyhladena marza</span></small><strong>{_format_mini_value_html(margin_stability_qa.get("max_smoothed_margin_pct"), kind="percent")}</strong></div>
                        </div>
                        {margin_stability_warning_block_html}
                    </div>
                    <div class="panel chart-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Geo profitability chart</span><span class="lang-sk hidden">Graf geo profitability</span></h3><p><span class="lang-en">Country-level revenue, contribution and CPO in one view.</span><span class="lang-sk hidden">Krajiny: trzby, contribution a CPO v jednom pohlade.</span></p></div></div>
                        <div class="chart-shell"><canvas id="geoProfitabilityChart"></canvas></div>
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Geo profitability</span><span class="lang-sk hidden">Geo profitabilita</span></h3><p><span class="lang-en">Country-level contribution view from the richer report.</span><span class="lang-sk hidden">Country-level contribution pohlad z bohatsieho reportu.</span></p></div></div>
                        <table>
                            <thead><tr><th><span class="lang-en">Country</span><span class="lang-sk hidden">Krajina</span></th><th><span class="lang-en">Orders</span><span class="lang-sk hidden">Objednavky</span></th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th><span class="lang-en">Contribution</span><span class="lang-sk hidden">Contribution</span></th><th><span class="lang-en">Margin</span><span class="lang-sk hidden">Marza</span></th><th>FB CPO</th></tr></thead>
                            <tbody>{geo_rows_html}</tbody>
                        </table>
                    </div>
                </section>

                <section class="section" id="products">
                    <div class="section-head">
                        <h2><span class="lang-en">Products</span><span class="lang-sk hidden">Produkty</span></h2>
                        <p><span class="lang-en">Top products by profit contribution plus the richer margin and trend views from the earlier report.</span><span class="lang-sk hidden">Top produkty podla zisku, plus bohatsie margin a trend pohlady zo starsieho reportu.</span></p>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Top product margins</span><span class="lang-sk hidden">Top produktove marze</span></h3><p><span class="lang-en">Highest-margin products among meaningful revenue contributors.</span><span class="lang-sk hidden">Produkty s najlepsou marzou medzi relevantnymi polozkami.</span></p></div></div>
                            <div class="chart-shell"><canvas id="productMarginBreakoutChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Product trend direction</span><span class="lang-sk hidden">Smer produktovych trendov</span></h3><p><span class="lang-en">Revenue growth in the second half of the period versus the first half.</span><span class="lang-sk hidden">Rast trzby v druhej polovici obdobia oproti prvej polovici.</span></p></div></div>
                            <div class="chart-shell"><canvas id="productTrendChart"></canvas></div>
                        </div>
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Top products by contribution</span><span class="lang-sk hidden">Top produkty podla contribution</span></h3></div></div>
                        <table>
                            <thead>
                                <tr>
                                    <th><span class="lang-en">Product</span><span class="lang-sk hidden">Produkt</span></th>
                                    <th>SKU</th>
                                    <th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th>
                                    <th><span class="lang-en">Profit</span><span class="lang-sk hidden">Zisk</span></th>
                                    <th><span class="lang-en">Margin</span><span class="lang-sk hidden">Marza</span></th>
                                    <th><span class="lang-en">Orders</span><span class="lang-sk hidden">Objednavky</span></th>
                                </tr>
                            </thead>
                            <tbody>{product_rows_html}</tbody>
                        </table>
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Product trend table</span><span class="lang-sk hidden">Tabulka produktovych trendov</span></h3><p><span class="lang-en">Combines trend label, revenue growth and quantity growth for quick merchandising decisions.</span><span class="lang-sk hidden">Spaja trend, rast trzby a rast kusov pre rychle merchandising rozhodnutia.</span></p></div></div>
                        <table>
                            <thead><tr><th><span class="lang-en">Product</span><span class="lang-sk hidden">Produkt</span></th><th><span class="lang-en">Trend</span><span class="lang-sk hidden">Trend</span></th><th><span class="lang-en">Revenue growth</span><span class="lang-sk hidden">Rast trzby</span></th><th><span class="lang-en">Qty growth</span><span class="lang-sk hidden">Rast kusov</span></th><th><span class="lang-en">Total revenue</span><span class="lang-sk hidden">Spolu trzby</span></th></tr></thead>
                            <tbody>{product_trend_rows_html}</tbody>
                        </table>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">SKU Pareto contribution</span><span class="lang-sk hidden">SKU Pareto contribution</span></h3><p><span class="lang-en">Cumulative contribution concentration by SKU.</span><span class="lang-sk hidden">Kumulovana koncentracia contribution podla SKU.</span></p></div></div>
                            <div class="chart-shell"><canvas id="skuParetoChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Attach rate pairs</span><span class="lang-sk hidden">Attach rate dvojice</span></h3><p><span class="lang-en">Products most often bought together from anchor orders.</span><span class="lang-sk hidden">Produkty najcastejsie kupovane spolu z anchor objednavok.</span></p></div></div>
                            <div class="chart-shell"><canvas id="attachRateChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">SKU Pareto table</span><span class="lang-sk hidden">SKU Pareto tabulka</span></h3><p><span class="lang-en">Contribution concentration by top SKUs.</span><span class="lang-sk hidden">Koncentracia contribution podla top SKU.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Product</span><span class="lang-sk hidden">Produkt</span></th><th>SKU</th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th><span class="lang-en">Contribution</span><span class="lang-sk hidden">Contribution</span></th><th><span class="lang-en">Cum. share</span><span class="lang-sk hidden">Kumul. podiel</span></th></tr></thead>
                                <tbody>{sku_pareto_rows_html}</tbody>
                            </table>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Attach rate table</span><span class="lang-sk hidden">Attach rate tabulka</span></h3><p><span class="lang-en">Cross-sell pair strength by anchor item.</span><span class="lang-sk hidden">Sila cross-sell dvojic podla anchor produktu.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Anchor item</span><span class="lang-sk hidden">Anchor produkt</span></th><th><span class="lang-en">Attached item</span><span class="lang-sk hidden">Pridany produkt</span></th><th><span class="lang-en">Anchor orders</span><span class="lang-sk hidden">Anchor obj.</span></th><th><span class="lang-en">Attached</span><span class="lang-sk hidden">Pridane</span></th><th><span class="lang-en">Rate</span><span class="lang-sk hidden">Miera</span></th></tr></thead>
                                <tbody>{attach_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                </section>

                <section class="section" id="operations">
                    <div class="section-head">
                        <h2><span class="lang-en">Operations and diagnostics</span><span class="lang-sk hidden">Operativa a diagnostika</span></h2>
                        <p><span class="lang-en">Operational mix, status structure, email segments and reconciliation markers from the full reporting logic.</span><span class="lang-sk hidden">Operativny mix, stavy objednavok, email segmenty a reconciliation markery z plnej reporting logiky.</span></p>
                    </div>
                    <div class="mini-grid" style="margin-bottom:18px;">
                        <div class="mini-card"><small><span class="lang-en">ROAS check delta</span><span class="lang-sk hidden">ROAS check delta</span></small><strong>{_num(consistency_payload.get('roas_delta')):+.4f}</strong></div>
                        <div class="mini-card"><small><span class="lang-en">Margin check delta</span><span class="lang-sk hidden">Margin check delta</span></small><strong>{_num(consistency_payload.get('margin_delta')):+.4f}</strong></div>
                        <div class="mini-card"><small><span class="lang-en">CAC check delta</span><span class="lang-sk hidden">CAC check delta</span></small><strong>{_num(consistency_payload.get('cac_delta')):+.4f}</strong></div>
                        <div class="mini-card"><small><span class="lang-en">Top segment</span><span class="lang-sk hidden">Top segment</span></small><strong>{escape(str(segment_rows[0].get('segment') if segment_rows else 'N/A'))}</strong></div>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">B2B vs B2C mix</span><span class="lang-sk hidden">B2B vs B2C mix</span></h3><p><span class="lang-en">Revenue and profit split by customer type.</span><span class="lang-sk hidden">Rozdelenie trzby a zisku podla typu zakaznika.</span></p></div></div>
                            <div class="chart-shell"><canvas id="b2bMixChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Order status structure</span><span class="lang-sk hidden">Struktura stavov objednavok</span></h3><p><span class="lang-en">How order volume is distributed across statuses.</span><span class="lang-sk hidden">Ako sa objem objednavok rozdeluje medzi stavy.</span></p></div></div>
                            <div class="chart-shell"><canvas id="orderStatusChart"></canvas></div>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">B2B/B2C table</span><span class="lang-sk hidden">B2B/B2C tabulka</span></h3><p><span class="lang-en">Revenue, profit and customer count by segment.</span><span class="lang-sk hidden">Trzby, zisk a pocet zakaznikov podla segmentu.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Type</span><span class="lang-sk hidden">Typ</span></th><th><span class="lang-en">Orders</span><span class="lang-sk hidden">Obj.</span></th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th><span class="lang-en">Profit</span><span class="lang-sk hidden">Zisk</span></th><th><span class="lang-en">Customers</span><span class="lang-sk hidden">Zakaznici</span></th><th>AOV</th></tr></thead>
                                <tbody>{b2b_rows_html}</tbody>
                            </table>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Order status table</span><span class="lang-sk hidden">Tabulka stavov objednavok</span></h3><p><span class="lang-en">Order count and revenue by status.</span><span class="lang-sk hidden">Pocet objednavok a trzba podla stavu.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Status</span><span class="lang-sk hidden">Stav</span></th><th><span class="lang-en">Orders</span><span class="lang-sk hidden">Obj.</span></th><th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Trzby</span></th><th><span class="lang-en">Share</span><span class="lang-sk hidden">Podiel</span></th></tr></thead>
                                <tbody>{order_status_rows_html}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="grid-2" style="margin-top:18px;">
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Email segment priority</span><span class="lang-sk hidden">Priorita email segmentov</span></h3><p><span class="lang-en">CRM targeting opportunities from the full segmentation logic.</span><span class="lang-sk hidden">CRM prilezitosti z plnej segmentacnej logiky.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Segment</span><span class="lang-sk hidden">Segment</span></th><th><span class="lang-en">Count</span><span class="lang-sk hidden">Pocet</span></th><th><span class="lang-en">Priority</span><span class="lang-sk hidden">Priorita</span></th><th><span class="lang-en">Description</span><span class="lang-sk hidden">Popis</span></th><th><span class="lang-en">Timing</span><span class="lang-sk hidden">Casovanie</span></th></tr></thead>
                                <tbody>{segment_rows_html}</tbody>
                            </table>
                        </div>
                        <div class="panel table-card">
                            <div class="card-head"><div><h3><span class="lang-en">Same-item purchase frequency</span><span class="lang-sk hidden">Frekvencia opakovanych nakupov rovnakeho produktu</span></h3><p><span class="lang-en">How many customers buy the same item 2x, 3x and more.</span><span class="lang-sk hidden">Kolko zakaznikov kupuje ten isty produkt 2x, 3x a viac.</span></p></div></div>
                            <table>
                                <thead><tr><th><span class="lang-en">Frequency</span><span class="lang-sk hidden">Frekvencia</span></th><th><span class="lang-en">Customers</span><span class="lang-sk hidden">Zakaznici</span></th><th><span class="lang-en">Share</span><span class="lang-sk hidden">Podiel</span></th></tr></thead>
                                <tbody>{"".join(f"<tr><td>{int(round(_num(row.get('purchase_frequency'))))}x</td><td>{int(round(_num(row.get('customer_count'))))}</td><td>{_num(row.get('percentage')):.1f}%</td></tr>" for row in same_item_frequency_rows) or '<tr><td colspan=\"3\"><span class=\"lang-en\">No frequency data available.</span><span class=\"lang-sk hidden\">Frekvencne data nie su dostupne.</span></td></tr>'}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="panel table-card" style="margin-top:18px;">
                        <div class="card-head"><div><h3><span class="lang-en">Cohort payback table</span><span class="lang-sk hidden">Kohortna payback tabulka</span></h3><p><span class="lang-en">Acquisition payback by cohort from the advanced DTC block.</span><span class="lang-sk hidden">Payback akvizicie podla kohort z advanced DTC bloku.</span></p></div></div>
                        <table>
                            <thead><tr><th><span class="lang-en">Cohort</span><span class="lang-sk hidden">Kohorta</span></th><th><span class="lang-en">New customers</span><span class="lang-sk hidden">Novi zakaznici</span></th><th><span class="lang-en">CAC</span><span class="lang-sk hidden">CAC</span></th><th><span class="lang-en">Recovery</span><span class="lang-sk hidden">Recovery</span></th><th><span class="lang-en">Avg payback days</span><span class="lang-sk hidden">Priem. payback dni</span></th><th><span class="lang-en">Median days</span><span class="lang-sk hidden">Median dni</span></th></tr></thead>
                            <tbody>{"".join(f"<tr><td>{escape(str(row.get('cohort_month') or '-'))}</td><td>{int(round(_num(row.get('new_customers'))))}</td><td>€{_num(row.get('cohort_cac')):,.2f}</td><td>{_num(row.get('recovery_rate_pct')):.1f}%</td><td>{_num(row.get('avg_payback_days')):.1f}</td><td>{_num(row.get('median_payback_days')):.1f}</td></tr>" for row in cohort_payback_rows) or '<tr><td colspan=\"6\"><span class=\"lang-en\">No cohort payback data available.</span><span class=\"lang-sk hidden\">Kohortne payback data nie su dostupne.</span></td></tr>'}</tbody>
                        </table>
                    </div>
                </section>

                <section class="section" id="library">
                    <div class="section-head">
                        <h2><span class="lang-en">Full metric library</span><span class="lang-sk hidden">Plna kniznica metrik</span></h2>
                        <p><span class="lang-en">This keeps the new dashboard shell and restores the wider metric surface from the original reporting build. Use it when you want full analytical depth without leaving the new layout.</span><span class="lang-sk hidden">Toto zachovava novy dashboard shell, ale vracia sirsiu plochu metrik z povodneho reportingu. Pouzi to vtedy, ked chces plnu analyticku hlbku bez odchodu z noveho layoutu.</span></p>
                    </div>
                    <div class="section-head" style="margin-top:10px;">
                        <h2><span class="lang-en">Daily economics library</span><span class="lang-sk hidden">Kniznica dennej ekonomiky</span></h2>
                        <p><span class="lang-en">Single-metric economics views from the original report.</span><span class="lang-sk hidden">Jednometriove ekonomicke pohlady z povodneho reportu.</span></p>
                    </div>
                    <div class="grid-2" id="libraryEconomics"></div>

                    <div class="section-head" style="margin-top:26px;">
                        <h2><span class="lang-en">Restored standalone economics</span><span class="lang-sk hidden">Obnovene standalone ekonomicke metriky</span></h2>
                        <p><span class="lang-en">Explicit single-metric charts that existed in the original test report and are still useful for operator-style review.</span><span class="lang-sk hidden">Explicitne samostatne grafy z povodneho test reportu, ktore su stale uzitocne pre operatorovy review.</span></p>
                    </div>
                    <div class="grid-2" id="libraryEconomicsStandalone"></div>

                    <div class="section-head" style="margin-top:26px;">
                        <h2><span class="lang-en">Marketing drilldown library</span><span class="lang-sk hidden">Kniznica marketingoveho drilldownu</span></h2>
                        <p><span class="lang-en">Detailed Facebook, campaign, hourly and weekday efficiency views.</span><span class="lang-sk hidden">Detailne Facebook, kampanove, hodinove a weekday pohlady na efektivitu.</span></p>
                    </div>
                    <div class="grid-2" id="libraryMarketing"></div>

                    <div class="section-head" style="margin-top:26px;">
                        <h2><span class="lang-en">Restored standalone marketing</span><span class="lang-sk hidden">Obnovene standalone marketingove metriky</span></h2>
                        <p><span class="lang-en">Legacy ad-delivery and campaign efficiency views restored without leaving the new dashboard layout.</span><span class="lang-sk hidden">Povodne delivery a kampanove pohľady obnovene bez opustenia noveho dashboard layoutu.</span></p>
                    </div>
                    <div class="grid-2" id="libraryMarketingStandalone"></div>

                    <div class="section-head" style="margin-top:26px;">
                        <h2><span class="lang-en">Customer and retention library</span><span class="lang-sk hidden">Kniznica zakaznikov a retencie</span></h2>
                        <p><span class="lang-en">Revenue split, refund detail, CLV/CAC, retention and repeat-product behavior.</span><span class="lang-sk hidden">Revenue split, detail refundov, CLV/CAC, retencia a spravanie pri opakovanych produktoch.</span></p>
                    </div>
                    <div class="grid-2" id="libraryCustomers"></div>

                    <div class="section-head" style="margin-top:26px;">
                        <h2><span class="lang-en">Restored standalone customer value</span><span class="lang-sk hidden">Obnovene standalone customer value metriky</span></h2>
                        <p><span class="lang-en">Explicit LTV, CAC, refund amount and payback trend views carried over from the original report.</span><span class="lang-sk hidden">Explicitne LTV, CAC, refund amount a payback trend pohľady prenesene z povodneho reportu.</span></p>
                    </div>
                    <div class="grid-2" id="libraryCustomersStandalone"></div>

                    <div class="section-head" style="margin-top:26px;">
                        <h2><span class="lang-en">Calendar and weather library</span><span class="lang-sk hidden">Kniznica kalendara a pocasia</span></h2>
                        <p><span class="lang-en">Separate weekday, week-of-month, day-of-month and weather overlays.</span><span class="lang-sk hidden">Samostatne weekday, tyzden v mesiaci, den v mesiaci a weather overlay pohlady.</span></p>
                    </div>
                    <div class="grid-2" id="libraryPatterns"></div>

                    <div class="section-head" style="margin-top:26px;">
                        <h2><span class="lang-en">Products and operations library</span><span class="lang-sk hidden">Kniznica produktov a operativy</span></h2>
                        <p><span class="lang-en">Product combinations, spend-response diagnostics and operational status drilldown.</span><span class="lang-sk hidden">Produktove kombinacie, spend-response diagnostika a operativny drilldown stavov.</span></p>
                    </div>
                    <div class="grid-2" id="libraryProductsOps"></div>

                    <div class="section-head" style="margin-top:26px;">
                        <h2><span class="lang-en">Executive metrics tile deck</span><span class="lang-sk hidden">Executive deck metrik v dlazdiciach</span></h2>
                        <p><span class="lang-en">All core summary metrics in one compact tile view for quick scanning at the end of the full library.</span><span class="lang-sk hidden">Vsetky hlavne sumarne metriky pokope v kompaktnych dlazdiciach na rychle preletanie na konci full library.</span></p>
                    </div>
                    <div class="library-tile-grid">{full_library_tiles_html}</div>
                </section>

                <section class="section" id="health">
                    <div class="section-head">
                        <h2><span class="lang-en">Data health and source confidence</span><span class="lang-sk hidden">Kvalita dát a dôvera v zdroje</span></h2>
                        <p><span class="lang-en">Source problems must be explicit so partial data is not mistaken for business signal.</span><span class="lang-sk hidden">Problémy zdrojov musia byť explicitné, aby sa neúplné dáta nepomýlili s biznis signálom.</span></p>
                    </div>
                    <div class="health-grid">{health_html}</div>
                </section>
            </div>
        </main>
    </div>
    <script>
        const DATA = {payload_json};
        const INLINE_EMBEDDED_PERIOD_REPORTS = window.__EMBEDDED_PERIOD_REPORTS__ || {json.dumps(embedded_period_reports)};
        const KPI_TYPES = {{
            revenue: 'currency', profit: 'currency', orders: 'integer', aov: 'currency',
            cac: 'currency', roas: 'multiple', pre_ad_contribution_margin: 'percent',
            post_ad_margin: 'percent', company_margin_with_fixed: 'percent'
        }};
        function fmtCurrency(v) {{ if (v === null || v === undefined || Number.isNaN(Number(v))) return 'N/A'; return new Intl.NumberFormat('en-US', {{ style: 'currency', currency: 'EUR', maximumFractionDigits: 2 }}).format(Number(v)); }}
        function fmtInt(v) {{ if (v === null || v === undefined || Number.isNaN(Number(v))) return 'N/A'; return new Intl.NumberFormat('en-US', {{ maximumFractionDigits: 0 }}).format(Number(v)); }}
        function fmtPercent(v) {{ if (v === null || v === undefined || Number.isNaN(Number(v))) return 'N/A'; return `${{Number(v).toFixed(1)}}%`; }}
        function fmtMultiple(v) {{ if (v === null || v === undefined || Number.isNaN(Number(v))) return 'N/A'; return `${{Number(v).toFixed(2)}}x`; }}
        function nullableNumber(v) {{ return (v === null || v === undefined || Number.isNaN(Number(v))) ? null : Number(v); }}
        function fmtMetric(key, value) {{
            const type = KPI_TYPES[key] || 'text';
            if (type === 'currency') return fmtCurrency(value);
            if (type === 'integer') return fmtInt(value);
            if (type === 'percent') return fmtPercent(value);
            if (type === 'multiple') return fmtMultiple(value);
            return value ?? 'N/A';
        }}
        function cleanTrendValues(values) {{
            return (values || [])
                .map(v => (v === null || v === undefined || Number.isNaN(Number(v))) ? null : Number(v))
                .filter(v => v !== null);
        }}
        function trendDelta(values) {{
            const cleaned = cleanTrendValues(values);
            if (cleaned.length < 2) return null;
            const first = cleaned[0];
            const last = cleaned[cleaned.length - 1];
            if (first === 0) {{
                if (last === 0) return 0;
                return null;
            }}
            return ((last - first) / Math.abs(first)) * 100;
        }}
        function sparklineSvg(values, direction) {{
            const cleaned = cleanTrendValues(values);
            if (cleaned.length < 2) return '';
            const width = 240;
            const height = 44;
            const pad = 3;
            let min = Math.min(...cleaned);
            let max = Math.max(...cleaned);
            if (min === max) {{
                min -= 1;
                max += 1;
            }}
            const points = cleaned.map((value, idx) => {{
                const x = (idx / Math.max(cleaned.length - 1, 1)) * width;
                const y = height - pad - (((value - min) / (max - min)) * (height - pad * 2));
                return [x, y];
            }});
            const linePath = points.map(([x, y], idx) => `${{idx === 0 ? 'M' : 'L'}}${{x.toFixed(2)}},${{y.toFixed(2)}}`).join(' ');
            const areaPath = `${{linePath}} L${{width.toFixed(2)}},${{(height - pad).toFixed(2)}} L0,${{(height - pad).toFixed(2)}} Z`;
            const cls = compClass(trendDelta(cleaned), direction);
            const stroke = cls === 'good' ? '#18b07a' : (cls === 'bad' ? '#e25d4d' : '#ff8a1f');
            return `<svg viewBox=\"0 0 ${{width}} ${{height}}\" preserveAspectRatio=\"none\" aria-hidden=\"true\"><path d=\"${{areaPath}}\" fill=\"${{stroke}}\" opacity=\"0.10\"></path><path d=\"${{linePath}}\" fill=\"none\" stroke=\"${{stroke}}\" stroke-width=\"3\" stroke-linecap=\"round\" stroke-linejoin=\"round\"></path></svg>`;
        }}
        function loadStoredJson(key) {{
            try {{
                const raw = sessionStorage.getItem(key);
                if (!raw) return {{}};
                const parsed = JSON.parse(raw);
                return parsed && typeof parsed === 'object' ? parsed : {{}};
            }} catch (_error) {{
                return {{}};
            }}
        }}
        const EMBEDDED_PERIOD_REPORTS = Object.keys(INLINE_EMBEDDED_PERIOD_REPORTS || {{}}).length
            ? INLINE_EMBEDDED_PERIOD_REPORTS
            : loadStoredJson('embeddedPeriodReports');
        const STORED_PERIOD_BASE_HREFS = Object.keys(window.__PERIOD_HREF_BASE_MAP__ || {{}}).length
            ? window.__PERIOD_HREF_BASE_MAP__
            : loadStoredJson('periodHrefBaseMap');
        const BOOTSTRAP_PENDING_SECTION_ID = window.__PENDING_SECTION_ID__ || '';
        function lang() {{ return localStorage.getItem('reportLang') || 'en'; }}
        function applyLang(next) {{
            document.querySelectorAll('.lang-en').forEach(el => el.classList.toggle('hidden', next !== 'en'));
            document.querySelectorAll('.lang-sk').forEach(el => el.classList.toggle('hidden', next !== 'sk'));
            document.querySelectorAll('.lang-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.lang === next));
            localStorage.setItem('reportLang', next);
            renderKpis(currentWindow);
        }}
        function compClass(delta, direction) {{
            if (delta === null || delta === undefined || Number.isNaN(Number(delta))) return 'neutral';
            const n = Number(delta);
            if (Math.abs(n) < 0.01) return 'neutral';
            return (direction === 'down' ? n < 0 : n > 0) ? 'good' : 'bad';
        }}
        function compText(delta) {{
            if (delta === null || delta === undefined || Number.isNaN(Number(delta))) return 'N/A';
            const n = Number(delta);
            return `${{n > 0 ? '+' : ''}}${{n.toFixed(1)}}%`;
        }}
        let currentWindow = DATA.kpis.default_window || 'monthly';
        function renderKpis(windowKey) {{
            currentWindow = windowKey;
            const grid = document.getElementById('kpiGrid');
            const currentLang = lang();
            document.querySelectorAll('.window-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.window === windowKey));
            const defs = DATA.kpis.metric_defs || [];
            const windows = DATA.kpis.windows || {{}};
            const comps = DATA.kpis.comparisons || {{}};
            const compLabels = DATA.kpis.comparison_labels || {{}};
            const current = windows[windowKey] || {{ metrics: {{}} }};
            grid.innerHTML = defs.map(def => {{
                const label = currentLang === 'sk' ? def.label_sk : def.label_en;
                const period = currentLang === 'sk' ? current.label_sk : current.label_en;
                const secondaryMetrics = current.secondary_metrics || {{}};
                const secondaryValue = secondaryMetrics[def.key];
                const trend = current.trend || {{}};
                const trendValues = ((trend.metrics || {{}})[def.key]) || [];
                const trendLabel = currentLang === 'sk' ? (trend.label_sk || '') : (trend.label_en || '');
                const trendDeltaValue = trendDelta(trendValues);
                const trendDeltaClass = compClass(trendDeltaValue, def.direction);
                const trendDeltaText = trendDeltaValue === null || trendDeltaValue === undefined || Number.isNaN(Number(trendDeltaValue))
                    ? 'N/A'
                    : `${{trendDeltaValue > 0 ? '+' : ''}}${{Number(trendDeltaValue).toFixed(1)}}%`;
                const trendHtml = trendValues.length >= 2
                    ? `<div class=\"kpi-trend\"><div class=\"kpi-trend-head\"><span class=\"kpi-trend-label\">${{trendLabel}}</span><span class=\"kpi-trend-delta ${{trendDeltaClass}}\">${{trendDeltaText}}</span></div><div class=\"kpi-sparkline\">${{sparklineSvg(trendValues, def.direction)}}</div></div>`
                    : '';
                const secondaryHtml = def.key === 'company_margin_with_fixed' && secondaryValue !== null && secondaryValue !== undefined
                    ? `<div class=\"kpi-secondary\">${{fmtCurrency(secondaryValue)}}</div>`
                    : '';
                const metricComps = ((comps[windowKey] || {{}})[def.key]) || {{}};
                const rows = Object.entries(metricComps).slice(0, 2).map(([compKey, compVal]) => {{
                    const names = (compLabels[windowKey] || {{}})[compKey] || {{ en: compKey, sk: compKey }};
                    return `<div class=\"compare-row ${{compClass(compVal, def.direction)}}\">${{compText(compVal)}} <span style=\"opacity:.85;\">${{currentLang === 'sk' ? names.sk : names.en}}</span></div>`;
                }}).join('');
                return `<article class=\"kpi-card\"><small>${{label}}</small><div class=\"kpi-value\">${{fmtMetric(def.key, (current.metrics || {{}})[def.key])}}</div>${{secondaryHtml}}<div class=\"kpi-period\">${{period}}</div><div class=\"compare-list\">${{rows}}</div>${{trendHtml}}</article>`;
            }}).join('');
        }}
        function gradient(ctx, top, bottom) {{
            const area = ctx.chart.chartArea;
            if (!area) return top;
            const g = ctx.chart.ctx.createLinearGradient(0, area.top, 0, area.bottom);
            g.addColorStop(0, top);
            g.addColorStop(1, bottom);
            return g;
        }}
        function baseOptions() {{
            return {{
                responsive: true,
                maintainAspectRatio: false,
                interaction: {{ mode: 'index', intersect: false }},
                plugins: {{
                    legend: {{
                        position: 'top',
                        align: 'start',
                        labels: {{ usePointStyle: true, pointStyle: 'circle', boxWidth: 10, color: '#675f56', font: {{ size: 12 }} }},
                    }},
                    tooltip: {{
                        backgroundColor: 'rgba(30,24,19,.92)',
                        titleColor: '#fff',
                        bodyColor: '#f8f2eb',
                        borderColor: 'rgba(255,255,255,.08)',
                        borderWidth: 1,
                        padding: 12,
                    }},
                }},
                scales: {{
                    x: {{ grid: {{ display: false }}, ticks: {{ color: '#8a8178', maxTicksLimit: 8, font: {{ size: 11 }} }}, border: {{ display: false }} }},
                    y: {{ grid: {{ color: 'rgba(140,122,99,.12)' }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }},
                }},
            }};
        }}
        function dualAxisOptions() {{
            const opts = baseOptions();
            opts.scales.y1 = {{
                position: 'right',
                grid: {{ display: false }},
                ticks: {{ color: '#8a8178', font: {{ size: 11 }} }},
                border: {{ display: false }},
            }};
            return opts;
        }}
        function horizontalBarOptions() {{
            const opts = baseOptions();
            opts.indexAxis = 'y';
            opts.plugins = {{ ...opts.plugins, legend: {{ display: false }} }};
            return opts;
        }}
        function doughnutOptions() {{
            return {{
                responsive: true,
                maintainAspectRatio: false,
                cutout: '64%',
                plugins: {{
                    legend: {{
                        position: 'bottom',
                        labels: {{ usePointStyle: true, pointStyle: 'circle', color: '#675f56', padding: 14 }},
                    }},
                    tooltip: {{
                        backgroundColor: 'rgba(30,24,19,.92)',
                        titleColor: '#fff',
                        bodyColor: '#f8f2eb',
                        borderColor: 'rgba(255,255,255,.08)',
                        borderWidth: 1,
                        padding: 12,
                    }},
                }},
            }};
        }}
        function hasRows(rows) {{
            return Array.isArray(rows) && rows.length > 0;
        }}
        function hasSeries(values) {{
            return Array.isArray(values) && values.length > 0;
        }}
        function createChartCard(item) {{
            const shellClass = item.shellClass ? `chart-shell ${{item.shellClass}}` : 'chart-shell';
            return `
                <div class="panel chart-card">
                    <div class="card-head">
                        <div>
                            <h3><span class="lang-en">${{item.title.en}}</span><span class="lang-sk hidden">${{item.title.sk}}</span></h3>
                            <p><span class="lang-en">${{item.desc.en}}</span><span class="lang-sk hidden">${{item.desc.sk}}</span></p>
                        </div>
                    </div>
                    <div class="${{shellClass}}"><canvas id="${{item.id}}"></canvas></div>
                </div>
            `;
        }}
        function renderGalleryCards(containerId, items) {{
            const container = document.getElementById(containerId);
            if (!container) return;
            container.innerHTML = items.length ? items.map(createChartCard).join('') : '';
        }}
        function buildCharts() {{
            const s = DATA.series;
            new Chart(document.getElementById('revenueProfitChart'), {{
                type: 'line',
                data: {{
                    labels: s.dates,
                    datasets: [
                        {{ label: 'Revenue', data: s.revenue, borderColor: '#ff8a1f', backgroundColor: (ctx) => gradient(ctx, 'rgba(255,138,31,.28)', 'rgba(255,138,31,.02)'), fill: true, tension: .38, borderWidth: 3, pointRadius: 0 }},
                        {{ label: 'Profit', data: s.profit, borderColor: '#1f9d66', tension: .34, borderWidth: 2.4, pointRadius: 0 }},
                        {{ label: 'Revenue 7d MA', data: s.revenue_ma7, borderColor: '#d95c00', borderDash: [8, 6], tension: .35, borderWidth: 2, pointRadius: 0 }},
                        {{ label: 'Profit 7d MA', data: s.profit_ma7, borderColor: '#0f6b44', borderDash: [8, 6], tension: .35, borderWidth: 2, pointRadius: 0 }},
                    ],
                }},
                options: baseOptions(),
            }});
            const ordersOpts = baseOptions();
            ordersOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
            new Chart(document.getElementById('ordersAovChart'), {{
                data: {{
                    labels: s.dates,
                    datasets: [
                        {{ type: 'bar', label: 'Orders', data: s.orders, backgroundColor: 'rgba(255,166,92,.68)', borderRadius: 8, barPercentage: .82, categoryPercentage: .82, yAxisID: 'y' }},
                        {{ type: 'line', label: 'AOV', data: s.aov, borderColor: '#4766ff', tension: .34, borderWidth: 2.5, pointRadius: 0, yAxisID: 'y1' }},
                        {{ type: 'line', label: 'Orders 7d MA', data: s.orders_ma7, borderColor: '#9b5f28', borderDash: [8, 6], tension: .35, borderWidth: 2, pointRadius: 0, yAxisID: 'y' }},
                    ],
                }},
                options: ordersOpts,
            }});
            new Chart(document.getElementById('costMixChart'), {{
                type: 'doughnut',
                data: {{
                    labels: DATA.cost_mix.labels,
                    datasets: [{{ data: DATA.cost_mix.values, backgroundColor: ['#ff8a1f','#ffb15d','#ffd59d','#4766ff','#97a8ff','#40382f'], borderColor: '#fff9f3', borderWidth: 4, hoverOffset: 8 }}],
                }},
                options: {{ responsive: true, maintainAspectRatio: false, cutout: '68%', plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, pointStyle: 'circle', color: '#6f665c', padding: 16 }} }} }} }},
            }});
            new Chart(document.getElementById('marginChart'), {{
                type: 'line',
                data: {{
                    labels: s.dates,
                    datasets: [
                        {{ label: 'Pre-ad margin', data: s.pre_margin, borderColor: '#ff8a1f', backgroundColor: 'rgba(255,138,31,.10)', fill: true, tension: .35, borderWidth: 2.5, pointRadius: 0 }},
                        {{ label: 'Post-ad margin', data: s.post_margin, borderColor: '#1f9d66', tension: .35, borderWidth: 2.5, pointRadius: 0 }},
                    ],
                }},
                options: baseOptions(),
            }});
            new Chart(document.getElementById('customerMixChart'), {{
                type: 'bar',
                data: {{
                    labels: DATA.customer_mix.dates,
                    datasets: [
                        {{ label: 'New revenue', data: DATA.customer_mix.new, backgroundColor: 'rgba(255,138,31,.75)', borderRadius: 6, stack: 'mix' }},
                        {{ label: 'Returning revenue', data: DATA.customer_mix.returning, backgroundColor: 'rgba(71,102,255,.72)', borderRadius: 6, stack: 'mix' }},
                    ],
                }},
                options: baseOptions(),
            }});
            if (document.getElementById('sampleFunnelChart') && hasRows(DATA.sample_funnel.windows)) {{
                const sampleFunnelOpts = dualAxisOptions();
                new Chart(document.getElementById('sampleFunnelChart'), {{
                    data: {{
                        labels: DATA.sample_funnel.windows.map(x => `${{x.window_days || '-'}}d`),
                        datasets: [
                            {{ type: 'line', label: 'Repeat %', data: DATA.sample_funnel.windows.map(x => Number(x.repeat_pct || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Any full-size %', data: DATA.sample_funnel.windows.map(x => Number(x.fullsize_any_pct || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: '200ml %', data: DATA.sample_funnel.windows.map(x => Number(x.fullsize_200_pct || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.1, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: '500ml %', data: DATA.sample_funnel.windows.map(x => Number(x.fullsize_500_pct || 0)), borderColor: '#8b5cf6', tension: .30, borderWidth: 2.1, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: sampleFunnelOpts,
                }});
            }}
            new Chart(document.getElementById('cityChart'), {{
                type: 'bar',
                data: {{ labels: DATA.cities.map(x => `${{x.city || 'Unknown'}}${{x.low_sample ? ' *' : ''}}`), datasets: [{{ label: 'Revenue', data: DATA.cities.map(x => Number(x.revenue || 0)), backgroundColor: 'rgba(255,138,31,.78)', borderRadius: 8 }}] }},
                options: {{ ...baseOptions(), indexAxis: 'y', plugins: {{ ...baseOptions().plugins, legend: {{ display: false }} }} }},
            }});
            const countryOpts = baseOptions();
            countryOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
            new Chart(document.getElementById('countryChart'), {{
                data: {{
                    labels: DATA.countries.map(x => `${{(x.country || 'Unknown').toUpperCase()}}${{x.low_sample ? ' *' : ''}}`),
                    datasets: [
                        {{ type: 'bar', label: 'Revenue', data: DATA.countries.map(x => Number(x.revenue || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                        {{ type: 'line', label: 'Orders', data: DATA.countries.map(x => Number(x.orders || 0)), borderColor: '#4766ff', tension: .35, borderWidth: 2.5, pointRadius: 3, yAxisID: 'y1' }},
                    ],
                }},
                options: countryOpts,
            }});
            if (DATA.geo_rows.length) {{
                const geoOpts = baseOptions();
                geoOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('geoProfitabilityChart'), {{
                    data: {{
                        labels: DATA.geo_rows.map(x => `${{(x.country || 'Unknown').toUpperCase()}}${{x.low_sample ? ' *' : ''}}`),
                        datasets: [
                            {{ type: 'bar', label: 'Revenue', data: DATA.geo_rows.map(x => Number(x.revenue || 0)), backgroundColor: 'rgba(255,138,31,.62)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'bar', label: 'Contribution', data: DATA.geo_rows.map(x => x.contribution_profit_guarded == null ? null : Number(x.contribution_profit_guarded || 0)), backgroundColor: 'rgba(31,157,102,.58)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'FB CPO', data: DATA.geo_rows.map(x => x.fb_cpo_guarded == null ? null : Number(x.fb_cpo_guarded || 0)), borderColor: '#4766ff', tension: .28, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: geoOpts,
                }});
            }}
            if (DATA.refunds.dates.length) {{
                new Chart(document.getElementById('refundRateChart'), {{
                    type: 'line',
                    data: {{ labels: DATA.refunds.dates, datasets: [
                        {{ label: 'Refund rate %', data: DATA.refunds.rate, borderColor: '#cf5060', backgroundColor: 'rgba(207,80,96,.14)', fill: true, tension: .34, borderWidth: 2.5, pointRadius: 0 }},
                        {{ label: 'Refund amount', data: DATA.refunds.amount, borderColor: '#8a2c3d', borderDash: [8, 6], tension: .34, borderWidth: 2, pointRadius: 0, yAxisID: 'y1' }},
                    ] }},
                    options: {{ ...baseOptions(), scales: {{ ...baseOptions().scales, y1: {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }} }} }},
                }});
            }}
            if (DATA.cohort_retention_rows.length) {{
                new Chart(document.getElementById('cohortRetentionChart'), {{
                    type: 'line',
                    data: {{
                        labels: DATA.cohort_retention_rows.map(x => x.cohort || '-'),
                        datasets: [
                            {{ label: '2nd order', data: DATA.cohort_retention_rows.map(x => Number(x.retention_2nd_pct || 0)), borderColor: '#ff8a1f', tension: .34, borderWidth: 2.5, pointRadius: 0 }},
                            {{ label: '3rd order', data: DATA.cohort_retention_rows.map(x => Number(x.retention_3rd_pct || 0)), borderColor: '#4766ff', tension: .34, borderWidth: 2.5, pointRadius: 0 }},
                            {{ label: '4th order', data: DATA.cohort_retention_rows.map(x => Number(x.retention_4th_pct || 0)), borderColor: '#1f9d66', tension: .34, borderWidth: 2.5, pointRadius: 0 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (DATA.day_of_week.labels.length) {{
                const dowOpts = baseOptions();
                dowOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('dayOfWeekChart'), {{
                    data: {{
                        labels: DATA.day_of_week.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Revenue', data: DATA.day_of_week.revenue, backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Orders', data: DATA.day_of_week.orders, borderColor: '#4766ff', tension: .35, borderWidth: 2.4, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'FB spend', data: DATA.day_of_week.fb_spend, borderColor: '#8a2c3d', borderDash: [8, 6], tension: .35, borderWidth: 2, pointRadius: 3, yAxisID: 'y' }},
                        ],
                    }},
                    options: dowOpts,
                }});
            }}
            if (DATA.week_of_month.labels.length) {{
                const womOpts = baseOptions();
                womOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('weekOfMonthChart'), {{
                    data: {{
                        labels: DATA.week_of_month.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Avg daily revenue', data: DATA.week_of_month.avg_daily_revenue, backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg daily profit', data: DATA.week_of_month.avg_daily_profit, borderColor: '#1f9d66', tension: .35, borderWidth: 2.5, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: womOpts,
                }});
            }}
            if (DATA.day_of_month.labels.length) {{
                const domOpts = baseOptions();
                domOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('dayOfMonthChart'), {{
                    data: {{
                        labels: DATA.day_of_month.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Avg revenue / occurrence', data: DATA.day_of_month.avg_revenue, backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg profit / occurrence', data: DATA.day_of_month.avg_profit, borderColor: '#1f9d66', tension: .35, borderWidth: 2.5, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: domOpts,
                }});
            }}
            if (DATA.weather_bucket.labels.length) {{
                new Chart(document.getElementById('weatherImpactChart'), {{
                    data: {{
                        labels: DATA.weather_bucket.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Revenue delta', data: DATA.weather_bucket.revenue_delta, backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8 }},
                            {{ type: 'line', label: 'Profit delta', data: DATA.weather_bucket.profit_delta, borderColor: '#1f9d66', tension: .35, borderWidth: 2.5, pointRadius: 3 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (DATA.product_margin_chart_rows.length) {{
                new Chart(document.getElementById('productMarginBreakoutChart'), {{
                    type: 'bar',
                    data: {{ labels: DATA.product_margin_chart_rows.map(x => (x.product || 'Unknown').slice(0, 28)), datasets: [{{ label: 'Margin %', data: DATA.product_margin_chart_rows.map(x => Number(x.margin_pct || 0)), backgroundColor: DATA.product_margin_chart_rows.map(x => Number(x.margin_pct || 0) >= 0 ? 'rgba(31,157,102,.72)' : 'rgba(207,80,96,.72)'), borderRadius: 8 }}] }},
                    options: {{ ...baseOptions(), indexAxis: 'y', plugins: {{ ...baseOptions().plugins, legend: {{ display: false }} }} }},
                }});
            }}
            if (DATA.trend_rows.length) {{
                new Chart(document.getElementById('productTrendChart'), {{
                    type: 'bar',
                    data: {{ labels: DATA.trend_rows.map(x => (x.product || 'Unknown').slice(0, 28)), datasets: [{{ label: 'Revenue growth %', data: DATA.trend_rows.map(x => Number(x.revenue_growth_pct || 0)), backgroundColor: DATA.trend_rows.map(x => Number(x.revenue_growth_pct || 0) >= 0 ? 'rgba(31,157,102,.72)' : 'rgba(207,80,96,.72)'), borderRadius: 8 }}] }},
                    options: {{ ...baseOptions(), indexAxis: 'y', plugins: {{ ...baseOptions().plugins, legend: {{ display: false }} }} }},
                }});
            }}
            const economicsOpts = baseOptions();
            economicsOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
            new Chart(document.getElementById('dailyEconomicsChart'), {{
                data: {{
                    labels: s.dates,
                    datasets: [
                        {{ type: 'line', label: 'Revenue', data: s.revenue, borderColor: '#ff8a1f', backgroundColor: 'rgba(255,138,31,.10)', fill: true, tension: .34, borderWidth: 2.6, pointRadius: 0, yAxisID: 'y' }},
                        {{ type: 'line', label: 'Profit', data: s.profit, borderColor: '#1f9d66', tension: .34, borderWidth: 2.4, pointRadius: 0, yAxisID: 'y' }},
                        {{ type: 'line', label: 'Total cost', data: s.total_cost, borderColor: '#8a2c3d', tension: .28, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y' }},
                        {{ type: 'line', label: 'Ads', data: s.total_ads, borderColor: '#4766ff', borderDash: [8, 6], tension: .28, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                    ],
                }},
                options: economicsOpts,
            }});
            new Chart(document.getElementById('costBreakoutChart'), {{
                data: {{
                    labels: s.dates,
                    datasets: [
                        {{ type: 'bar', label: 'Product', data: s.product_cost, backgroundColor: 'rgba(255,138,31,.72)', stack: 'costs', borderRadius: 6 }},
                        {{ type: 'bar', label: 'Packaging', data: s.packaging, backgroundColor: 'rgba(255,177,93,.72)', stack: 'costs', borderRadius: 6 }},
                        {{ type: 'bar', label: 'Net shipping', data: s.shipping, backgroundColor: 'rgba(255,213,157,.9)', stack: 'costs', borderRadius: 6 }},
                        {{ type: 'bar', label: 'Fixed', data: s.fixed, backgroundColor: 'rgba(64,56,47,.72)', stack: 'costs', borderRadius: 6 }},
                        {{ type: 'line', label: 'Ads', data: s.total_ads, borderColor: '#4766ff', tension: .28, borderWidth: 2.1, pointRadius: 0, yAxisID: 'y1' }},
                    ],
                }},
                options: economicsOpts,
            }});
            new Chart(document.getElementById('grossMarginRoiChart'), {{
                type: 'line',
                data: {{
                    labels: s.dates,
                    datasets: [
                        {{ label: 'Gross margin %', data: s.gross_margin, borderColor: '#ff8a1f', tension: .32, borderWidth: 2.4, pointRadius: 0 }},
                        {{ label: 'Gross margin 7d MA', data: s.gross_margin_ma7, borderColor: '#d95c00', borderDash: [8, 6], tension: .32, borderWidth: 2.0, pointRadius: 0 }},
                        {{ label: 'ROI %', data: s.roi, borderColor: '#1f9d66', tension: .32, borderWidth: 2.4, pointRadius: 0 }},
                        {{ label: 'ROI 7d MA', data: s.roi_ma7, borderColor: '#0f6b44', borderDash: [8, 6], tension: .32, borderWidth: 2.0, pointRadius: 0 }},
                    ],
                }},
                options: baseOptions(),
            }});
            new Chart(document.getElementById('contributionPerOrderChart'), {{
                type: 'line',
                data: {{
                    labels: s.dates,
                    datasets: [
                        {{ label: 'Pre-ad / order', data: s.pre_contribution_per_order, borderColor: '#ff8a1f', backgroundColor: 'rgba(255,138,31,.10)', fill: true, tension: .32, borderWidth: 2.4, pointRadius: 0 }},
                        {{ label: 'Post-ad / order', data: s.post_contribution_per_order, borderColor: '#1f9d66', tension: .32, borderWidth: 2.4, pointRadius: 0 }},
                    ],
                }},
                options: baseOptions(),
            }});
            const itemsOpts = baseOptions();
            itemsOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
            new Chart(document.getElementById('itemsBasketChart'), {{
                data: {{
                    labels: s.dates,
                    datasets: [
                        {{ type: 'bar', label: 'Items sold', data: s.items, backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 6, yAxisID: 'y' }},
                        {{ type: 'line', label: 'Avg items / order', data: s.avg_items, borderColor: '#4766ff', tension: .34, borderWidth: 2.4, pointRadius: 0, yAxisID: 'y1' }},
                        {{ type: 'line', label: 'Avg items 7d MA', data: s.avg_items_ma7, borderColor: '#8b5cf6', borderDash: [8, 6], tension: .34, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                    ],
                }},
                options: itemsOpts,
            }});
            if (DATA.ltv.labels.length) {{
                const ltvOpts = baseOptions();
                ltvOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('ltvRevenueTrendChart'), {{
                    data: {{
                        labels: DATA.ltv.labels,
                        datasets: [
                            {{ type: 'line', label: 'LTV revenue', data: DATA.ltv.ltv_revenue, borderColor: '#8b5cf6', tension: .34, borderWidth: 2.6, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Cum. avg revenue', data: s.cumulative_avg_revenue, borderColor: '#ff8a1f', borderDash: [8, 6], tension: .34, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'bar', label: 'Customers acquired', data: DATA.ltv.customers_acquired || [], backgroundColor: 'rgba(71,102,255,.38)', borderRadius: 6, yAxisID: 'y1' }},
                        ],
                    }},
                    options: ltvOpts,
                }});
            }}
            if (DATA.fb_daily.dates.length) {{
                const fbPerfOpts = baseOptions();
                fbPerfOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('fbDailyPerformanceChart'), {{
                    data: {{
                        labels: DATA.fb_daily.dates,
                        datasets: [
                            {{ type: 'bar', label: 'Spend', data: DATA.fb_daily.spend, backgroundColor: 'rgba(71,102,255,.36)', borderRadius: 6, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Clicks', data: DATA.fb_daily.clicks, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Impressions', data: DATA.fb_daily.impressions, borderColor: '#8b5cf6', tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: fbPerfOpts,
                }});
                const fbEffOpts = baseOptions();
                fbEffOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('fbEfficiencyChart'), {{
                    data: {{
                        labels: DATA.fb_daily.dates,
                        datasets: [
                            {{ type: 'line', label: 'CTR %', data: DATA.fb_daily.ctr, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'CPC', data: DATA.fb_daily.cpc, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CPM', data: DATA.fb_daily.cpm, borderColor: '#8a2c3d', tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y' }},
                        ],
                    }},
                    options: fbEffOpts,
                }});
            }}
            if (DATA.weekly_cpo.length) {{
                const weeklyLabels = DATA.weekly_cpo.map(x => x.week_start || '-');
                const weeklyOpts = baseOptions();
                weeklyOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('weeklyCpoChart'), {{
                    data: {{
                        labels: weeklyLabels,
                        datasets: [
                            {{ type: 'bar', label: 'CPO', data: DATA.weekly_cpo.map(x => Number(x.cpo || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 6, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Spend', data: DATA.weekly_cpo.map(x => Number(x.fb_spend || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Orders', data: DATA.weekly_cpo.map(x => Number(x.orders || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: weeklyOpts,
                }});
            }}
            if (DATA.fb_hourly.length || DATA.hourly_orders.length) {{
                const hourMap = new Map();
                DATA.fb_hourly.forEach(row => hourMap.set(String(row.hour), {{ spend: Number(row.spend || 0), clicks: Number(row.clicks || 0) }}));
                DATA.hourly_orders.forEach(row => {{
                    const key = String(row.hour);
                    const existing = hourMap.get(key) || {{ spend: 0, clicks: 0 }};
                    existing.orders = Number(row.orders || 0);
                    existing.revenue = Number(row.revenue || 0);
                    hourMap.set(key, existing);
                }});
                const hours = Array.from({{ length: 24 }}, (_, idx) => String(idx));
                const hourlyOpts = baseOptions();
                hourlyOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('hourlyAdsOrdersChart'), {{
                    data: {{
                        labels: hours,
                        datasets: [
                            {{ type: 'bar', label: 'Spend', data: hours.map(h => (hourMap.get(h) || {{}}).spend || 0), backgroundColor: 'rgba(71,102,255,.38)', borderRadius: 6, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Orders', data: hours.map(h => (hourMap.get(h) || {{}}).orders || 0), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Clicks', data: hours.map(h => (hourMap.get(h) || {{}}).clicks || 0), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: hourlyOpts,
                }});
            }}
            if (DATA.ads_effectiveness.labels.length) {{
                const adsOpts = baseOptions();
                adsOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('adsEffectivenessChart'), {{
                    data: {{
                        labels: DATA.ads_effectiveness.labels,
                        datasets: [
                            {{ type: 'bar', label: 'FB spend', data: DATA.ads_effectiveness.fb_spend, backgroundColor: 'rgba(71,102,255,.32)', borderRadius: 6, yAxisID: 'y' }},
                            {{ type: 'bar', label: 'Google spend', data: DATA.ads_effectiveness.google_spend, backgroundColor: 'rgba(151,168,255,.32)', borderRadius: 6, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Revenue', data: DATA.ads_effectiveness.revenue, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Profit', data: DATA.ads_effectiveness.profit, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: adsOpts,
                }});
            }}
            if (DATA.fb_daily.dates.length) {{
                const reachOpts = baseOptions();
                reachOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('fbReachClicksChart'), {{
                    data: {{
                        labels: DATA.fb_daily.dates,
                        datasets: [
                            {{ type: 'bar', label: 'Reach', data: DATA.fb_daily.reach, backgroundColor: 'rgba(71,102,255,.28)', borderRadius: 6, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Clicks', data: DATA.fb_daily.clicks, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'CTR %', data: DATA.fb_daily.ctr, borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: reachOpts,
                }});
            }}
            if (DATA.fb_campaign_rows.length) {{
                const spendMixRows = DATA.fb_campaign_rows.slice(0, 6);
                new Chart(document.getElementById('campaignSpendMixChart'), {{
                    type: 'doughnut',
                    data: {{
                        labels: spendMixRows.map(x => (x.campaign_name || 'Unknown').slice(0, 28)),
                        datasets: [{{ data: spendMixRows.map(x => Number(x.spend || 0)), backgroundColor: ['#ff8a1f','#ffb15d','#ffd59d','#4766ff','#8b5cf6','#1f9d66'], borderColor: '#fff9f3', borderWidth: 4, hoverOffset: 8 }}],
                    }},
                    options: {{ responsive: true, maintainAspectRatio: false, cutout: '65%', plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, pointStyle: 'circle', color: '#6f665c', padding: 14 }} }} }} }},
                }});
                const campEffOpts = baseOptions();
                campEffOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('campaignEfficiencyChart'), {{
                    data: {{
                        labels: DATA.fb_campaign_rows.map(x => (x.campaign_name || 'Unknown').slice(0, 24)),
                        datasets: [
                            {{ type: 'bar', label: 'Spend', data: DATA.fb_campaign_rows.map(x => Number(x.spend || 0)), backgroundColor: 'rgba(255,138,31,.62)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CTR %', data: DATA.fb_campaign_rows.map(x => Number(x.ctr || 0)), borderColor: '#1f9d66', tension: .28, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'CPC', data: DATA.fb_campaign_rows.map(x => Number(x.cpc || 0)), borderColor: '#4766ff', tension: .28, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y' }},
                        ],
                    }},
                    options: campEffOpts,
                }});
            }}
            if (DATA.fb_hourly.length || DATA.hourly_orders.length) {{
                const hourlyMap = new Map();
                DATA.fb_hourly.forEach(row => hourlyMap.set(String(row.hour), {{ spend: Number(row.spend || 0), clicks: Number(row.clicks || 0) }}));
                DATA.hourly_orders.forEach(row => {{
                    const key = String(row.hour);
                    const existing = hourlyMap.get(key) || {{ spend: 0, clicks: 0 }};
                    existing.orders = Number(row.orders || 0);
                    existing.revenue = Number(row.revenue || 0);
                    hourlyMap.set(key, existing);
                }});
                const hours = Array.from({{ length: 24 }}, (_, idx) => String(idx));
                const hourlyEffOpts = baseOptions();
                hourlyEffOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('hourlyEfficiencyChart'), {{
                    data: {{
                        labels: hours,
                        datasets: [
                            {{ type: 'line', label: 'Hourly CPO', data: hours.map(h => {{ const row = hourlyMap.get(h) || {{}}; const orders = Number(row.orders || 0); const spend = Number(row.spend || 0); return orders > 0 ? spend / orders : 0; }}), borderColor: '#cf5060', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Hourly ROAS', data: hours.map(h => {{ const row = hourlyMap.get(h) || {{}}; const spend = Number(row.spend || 0); const revenue = Number(row.revenue || 0); return spend > 0 ? revenue / spend : 0; }}), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y1' }},
                        ],
                    }},
                    options: hourlyEffOpts,
                }});
            }}
            if (DATA.fb_dow.length) {{
                const fbDowOpts = baseOptions();
                fbDowOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('fbDowEfficiencyChart'), {{
                    data: {{
                        labels: DATA.fb_dow.map(x => x.day_of_week || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Spend', data: DATA.fb_dow.map(x => Number(x.total_spend || 0)), backgroundColor: 'rgba(71,102,255,.30)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CTR %', data: DATA.fb_dow.map(x => Number(x.ctr || 0)), borderColor: '#1f9d66', tension: .28, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'CPC', data: DATA.fb_dow.map(x => Number(x.cpc || 0)), borderColor: '#ff8a1f', tension: .28, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y' }},
                        ],
                    }},
                    options: fbDowOpts,
                }});
            }}
            if (DATA.returning_customers.labels.length) {{
                const retOpts = baseOptions();
                retOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('returningShareChart'), {{
                    data: {{
                        labels: DATA.returning_customers.labels,
                        datasets: [
                            {{ type: 'bar', label: 'New %', data: DATA.returning_customers.new_pct, backgroundColor: 'rgba(255,138,31,.62)', borderRadius: 6, stack: 'share', yAxisID: 'y' }},
                            {{ type: 'bar', label: 'Returning %', data: DATA.returning_customers.returning_pct, backgroundColor: 'rgba(71,102,255,.62)', borderRadius: 6, stack: 'share', yAxisID: 'y' }},
                            {{ type: 'line', label: 'Unique customers', data: DATA.returning_customers.unique_customers, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: retOpts,
                }});
            }}
            if (DATA.clv.labels.length) {{
                const clvOpts = baseOptions();
                clvOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('clvCacTrendChart'), {{
                    data: {{
                        labels: DATA.clv.labels,
                        datasets: [
                            {{ type: 'line', label: 'Avg CLV', data: DATA.clv.avg_clv, borderColor: '#8b5cf6', tension: .30, borderWidth: 2.4, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CAC', data: DATA.clv.cac, borderColor: '#cf5060', tension: .30, borderWidth: 2.4, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'LTV/CAC', data: DATA.clv.ltv_cac_ratio, borderColor: '#ff8a1f', borderDash: [8, 6], tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: clvOpts,
                }});
            }}
            if (DATA.order_size.labels.length) {{
                new Chart(document.getElementById('orderSizeChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.order_size.labels,
                        datasets: [
                            {{ label: '1 item', data: DATA.order_size.one, backgroundColor: 'rgba(255,138,31,.45)', stack: 'size', borderRadius: 6 }},
                            {{ label: '2 items', data: DATA.order_size.two, backgroundColor: 'rgba(255,177,93,.55)', stack: 'size', borderRadius: 6 }},
                            {{ label: '3 items', data: DATA.order_size.three, backgroundColor: 'rgba(255,213,157,.65)', stack: 'size', borderRadius: 6 }},
                            {{ label: '4 items', data: DATA.order_size.four, backgroundColor: 'rgba(71,102,255,.45)', stack: 'size', borderRadius: 6 }},
                            {{ label: '5+ items', data: DATA.order_size.five_plus, backgroundColor: 'rgba(31,157,102,.45)', stack: 'size', borderRadius: 6 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (DATA.customer_top_rows.length) {{
                new Chart(document.getElementById('customerConcentrationChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.customer_top_rows.map(x => (x.customer || 'Unknown').slice(0, 26)),
                        datasets: [{{ label: 'Revenue share %', data: DATA.customer_top_rows.map(x => Number(x.revenue_pct || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8 }}],
                    }},
                    options: {{ ...baseOptions(), indexAxis: 'y', plugins: {{ ...baseOptions().plugins, legend: {{ display: false }} }} }},
                }});
            }}
            if (DATA.clv.labels.length) {{
                const returnOpts = baseOptions();
                returnOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('returnTimeLtvChart'), {{
                    data: {{
                        labels: DATA.clv.labels,
                        datasets: [
                            {{ type: 'line', label: 'Avg return days', data: DATA.clv.avg_return_time_days, borderColor: '#4766ff', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'LTV/CAC', data: DATA.clv.ltv_cac_ratio, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Avg CLV', data: DATA.clv.avg_clv, borderColor: '#8b5cf6', borderDash: [8, 6], tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: returnOpts,
                }});
            }}
            if (DATA.cohort_order_frequency_rows.length) {{
                const freqOpts = baseOptions();
                freqOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('orderFrequencyChart'), {{
                    data: {{
                        labels: DATA.cohort_order_frequency_rows.map(x => x.frequency || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Customers', data: DATA.cohort_order_frequency_rows.map(x => Number(x.customer_count || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Customer %', data: DATA.cohort_order_frequency_rows.map(x => Number(x.customer_pct || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: freqOpts,
                }});
            }}
            if (DATA.cohort_time_between_rows.length) {{
                const betweenOpts = baseOptions();
                betweenOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('timeBetweenOrdersChart'), {{
                    data: {{
                        labels: DATA.cohort_time_between_rows.map(x => x.time_bucket || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Count', data: DATA.cohort_time_between_rows.map(x => Number(x.count || 0)), backgroundColor: 'rgba(71,102,255,.55)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Share %', data: DATA.cohort_time_between_rows.map(x => Number(x.percentage || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: betweenOpts,
                }});
            }}
            if (DATA.cohort_time_by_order_rows.length) {{
                new Chart(document.getElementById('timeBetweenByOrderChart'), {{
                    data: {{
                        labels: DATA.cohort_time_by_order_rows.map(x => x.transition || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Avg days', data: DATA.cohort_time_by_order_rows.map(x => Number(x.avg_days || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8 }},
                            {{ type: 'line', label: 'Median days', data: DATA.cohort_time_by_order_rows.map(x => Number(x.median_days || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (DATA.cohort_time_to_nth_rows.length) {{
                const nthOpts = baseOptions();
                nthOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('timeToNthOrderChart'), {{
                    data: {{
                        labels: DATA.cohort_time_to_nth_rows.map(x => String(x.order_number || '-')),
                        datasets: [
                            {{ type: 'line', label: 'Avg days from first', data: DATA.cohort_time_to_nth_rows.map(x => Number(x.avg_days_from_first || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg days from previous', data: DATA.cohort_time_to_nth_rows.map(x => Number(x.avg_days_from_prev || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Customers', data: DATA.cohort_time_to_nth_rows.map(x => Number(x.customer_count || 0)), borderColor: '#4766ff', borderDash: [8, 6], tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: nthOpts,
                }});
            }}
            if (DATA.cohort_revenue_by_order_rows.length) {{
                const seqOpts = baseOptions();
                seqOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('orderSequenceValueChart'), {{
                    data: {{
                        labels: DATA.cohort_revenue_by_order_rows.map(x => String(x.order_number || '-')),
                        datasets: [
                            {{ type: 'bar', label: 'AOV', data: DATA.cohort_revenue_by_order_rows.map(x => Number(x.avg_order_value || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg items / order', data: DATA.cohort_revenue_by_order_rows.map(x => Number(x.avg_items_per_order || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Price / item', data: DATA.cohort_revenue_by_order_rows.map(x => Number(x.avg_price_per_item || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y' }},
                        ],
                    }},
                    options: seqOpts,
                }});
            }}
            if (DATA.mature_cohort_rows.length) {{
                new Chart(document.getElementById('matureCohortChart'), {{
                    type: 'line',
                    data: {{
                        labels: DATA.mature_cohort_rows.map(x => x.cohort || '-'),
                        datasets: [
                            {{ label: '2nd order', data: DATA.mature_cohort_rows.map(x => Number(x.retention_2nd_pct || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3 }},
                            {{ label: '3rd order', data: DATA.mature_cohort_rows.map(x => Number(x.retention_3rd_pct || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3 }},
                            {{ label: '4th order', data: DATA.mature_cohort_rows.map(x => Number(x.retention_4th_pct || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (DATA.weather.dates.length) {{
                const weatherDailyOpts = baseOptions();
                weatherDailyOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('weatherDailyChart'), {{
                    data: {{
                        labels: DATA.weather.dates,
                        datasets: [
                            {{ type: 'line', label: 'Revenue', data: DATA.weather.revenue, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.4, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Profit', data: DATA.weather.profit, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'bar', label: 'Bad weather score', data: DATA.weather.bad_score, backgroundColor: 'rgba(71,102,255,.30)', borderRadius: 6, yAxisID: 'y1' }},
                        ],
                    }},
                    options: weatherDailyOpts,
                }});
            }}
            if (DATA.payday_window_rows.length) {{
                const paydayOpts = baseOptions();
                paydayOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('paydayWindowChart'), {{
                    data: {{
                        labels: DATA.payday_window_rows.map(x => x.window || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Avg daily revenue', data: DATA.payday_window_rows.map(x => Number(x.avg_daily_revenue || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg daily profit', data: DATA.payday_window_rows.map(x => Number(x.avg_daily_profit || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: paydayOpts,
                }});
            }}
            if (DATA.heatmap_rows.length) {{
                const dayOrder = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
                const skOrder = ['Pondelok','Utorok','Streda','Stvrtok','Piatok','Sobota','Nedela'];
                const labels = Array.from({{ length: 24 }}, (_, idx) => idx);
                new Chart(document.getElementById('heatmapChart'), {{
                    type: 'bubble',
                    data: {{
                        datasets: [{{
                            label: 'Orders',
                            data: DATA.heatmap_rows.map(row => {{
                                const dayName = row.day_name || '';
                                const y = Math.max(dayOrder.indexOf(dayName), skOrder.indexOf(dayName), 0);
                                const orders = Number(row.orders || 0);
                                return {{ x: Number(row.hour || 0), y: y, r: Math.max(4, Math.min(18, orders * 1.1)), orders: orders, day: dayName }};
                            }}),
                            backgroundColor: 'rgba(255,138,31,.42)',
                            borderColor: 'rgba(255,138,31,.85)',
                        }}],
                    }},
                    options: {{
                        ...baseOptions(),
                        plugins: {{
                            ...baseOptions().plugins,
                            legend: {{ display: false }},
                            tooltip: {{
                                ...baseOptions().plugins.tooltip,
                                callbacks: {{
                                    label: (ctx) => `${{ctx.raw.day}} ${{ctx.raw.x}}:00 -> ${{ctx.raw.orders}} orders`
                                }}
                            }}
                        }},
                        scales: {{
                            x: {{ type: 'linear', min: 0, max: 23, ticks: {{ stepSize: 1, color: '#8a8178' }}, grid: {{ display: false }}, border: {{ display: false }} }},
                            y: {{
                                min: -0.5,
                                max: 6.5,
                                ticks: {{
                                    stepSize: 1,
                                    color: '#8a8178',
                                    callback: (value) => dayOrder[value] || ''
                                }},
                                grid: {{ color: 'rgba(140,122,99,.12)' }},
                                border: {{ display: false }}
                            }}
                        }}
                    }},
                }});
            }}
            if (DATA.sku_pareto_rows.length) {{
                const paretoOpts = baseOptions();
                paretoOpts.scales.y1 = {{ position: 'right', min: 0, max: 100, grid: {{ display: false }}, ticks: {{ color: '#8a8178', callback: (v) => `${{v}}%`, font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('skuParetoChart'), {{
                    data: {{
                        labels: DATA.sku_pareto_rows.map(x => (x.product || 'Unknown').slice(0, 26)),
                        datasets: [
                            {{ type: 'bar', label: 'Contribution', data: DATA.sku_pareto_rows.map(x => Number(x.pre_ad_contribution || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Cum. contribution %', data: DATA.sku_pareto_rows.map(x => Number(x.cum_contribution_pct || 0)), borderColor: '#4766ff', tension: .28, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: paretoOpts,
                }});
            }}
            if (DATA.attach_rate_rows.length) {{
                new Chart(document.getElementById('attachRateChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.attach_rate_rows.map(x => `${{(x.anchor_item || '').slice(0, 14)}} -> ${{(x.attached_item || '').slice(0, 14)}}`),
                        datasets: [{{ label: 'Attach rate %', data: DATA.attach_rate_rows.map(x => Number(x.attach_rate_pct || 0)), backgroundColor: 'rgba(71,102,255,.72)', borderRadius: 8 }}],
                    }},
                    options: {{ ...baseOptions(), indexAxis: 'y', plugins: {{ ...baseOptions().plugins, legend: {{ display: false }} }} }},
                }});
            }}
            if (DATA.b2b_rows.length) {{
                const b2bOpts = baseOptions();
                b2bOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('b2bMixChart'), {{
                    data: {{
                        labels: DATA.b2b_rows.map(x => x.customer_type || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Revenue', data: DATA.b2b_rows.map(x => Number(x.revenue || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Profit', data: DATA.b2b_rows.map(x => Number(x.profit || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'AOV', data: DATA.b2b_rows.map(x => Number(x.aov || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: b2bOpts,
                }});
            }}
            if (DATA.order_status_rows.length) {{
                new Chart(document.getElementById('orderStatusChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.order_status_rows.map(x => x.status || '-'),
                        datasets: [{{ label: 'Orders', data: DATA.order_status_rows.map(x => Number(x.orders || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8 }}],
                    }},
                    options: {{ ...baseOptions(), indexAxis: 'y', plugins: {{ ...baseOptions().plugins, legend: {{ display: false }} }} }},
                }});
            }}
        }}
        function buildLibraryEconomicsMarketing() {{
            const s = DATA.series;
            const economicsItems = hasSeries(s.dates) ? [
                {{ id: 'econRevenueDetailChart', title: {{ en: 'Revenue detail', sk: 'Detail trzby' }}, desc: {{ en: 'Single-metric revenue trend with smoothing.', sk: 'Samostatny trend trzby s vyhladenim.' }} }},
                {{ id: 'econProfitDetailChart', title: {{ en: 'Profit detail', sk: 'Detail zisku' }}, desc: {{ en: 'Profit development isolated from the mixed chart.', sk: 'Vyvoj zisku oddeleny od ostatnych serii.' }} }},
                {{ id: 'econAovDetailChart', title: {{ en: 'AOV detail', sk: 'Detail AOV' }}, desc: {{ en: 'Basket value trend and 7-day moving average.', sk: 'Trend hodnoty kosika a 7-dnovy priemer.' }} }},
                {{ id: 'econCostStackDetailChart', title: {{ en: 'Cost stack detail', sk: 'Detail stacku nakladov' }}, desc: {{ en: 'Total cost, product cost and ad spend on one timeline.', sk: 'Total cost, produktovy cost a reklamny spend na jednej osi.' }} }},
                {{ id: 'econLogisticsDetailChart', title: {{ en: 'Logistics and fixed costs', sk: 'Logistika a fixne naklady' }}, desc: {{ en: 'Packaging, net shipping and fixed overhead in one view.', sk: 'Balenie, ciste shipping a fixny overhead v jednom pohlade.' }} }},
                {{ id: 'econAverageTrendDetailChart', title: {{ en: 'Running averages', sk: 'Bezace priemery' }}, desc: {{ en: 'Cumulative average revenue and profit for stability reading.', sk: 'Kumulativny priemer trzby a zisku pre citanie stability.' }} }},
                {{ id: 'econRoiRoasDetailChart', title: {{ en: 'ROI and ROAS detail', sk: 'Detail ROI a ROAS' }}, desc: {{ en: 'Daily ROI against daily blended ROAS.', sk: 'Denne ROI oproti dennemu blended ROAS.' }} }},
                {{ id: 'econMarginsDetailChart', title: {{ en: 'Margin stack', sk: 'Stack marzi' }}, desc: {{ en: 'Gross, pre-ad and post-ad margins in one line view.', sk: 'Hruba, pre-ad a post-ad marza v jednom pohlade.' }} }},
            ] : [];
            renderGalleryCards('libraryEconomics', economicsItems);

            if (document.getElementById('econRevenueDetailChart')) {{
                new Chart(document.getElementById('econRevenueDetailChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ label: 'Revenue', data: s.revenue, borderColor: '#ff8a1f', backgroundColor: (ctx) => gradient(ctx, 'rgba(255,138,31,.22)', 'rgba(255,138,31,.02)'), fill: true, tension: .36, borderWidth: 2.8, pointRadius: 0 }},
                            {{ label: 'Revenue 7d MA', data: s.revenue_ma7, borderColor: '#d95c00', borderDash: [8, 6], tension: .34, borderWidth: 2.0, pointRadius: 0 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econProfitDetailChart')) {{
                new Chart(document.getElementById('econProfitDetailChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ label: 'Profit', data: s.profit, borderColor: '#1f9d66', backgroundColor: (ctx) => gradient(ctx, 'rgba(31,157,102,.20)', 'rgba(31,157,102,.02)'), fill: true, tension: .34, borderWidth: 2.6, pointRadius: 0 }},
                            {{ label: 'Profit 7d MA', data: s.profit_ma7, borderColor: '#0f6b44', borderDash: [8, 6], tension: .34, borderWidth: 2.0, pointRadius: 0 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econAovDetailChart')) {{
                new Chart(document.getElementById('econAovDetailChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ label: 'AOV', data: s.aov, borderColor: '#4766ff', tension: .34, borderWidth: 2.4, pointRadius: 0 }},
                            {{ label: 'AOV 7d MA', data: s.aov_ma7, borderColor: '#1b46e5', borderDash: [8, 6], tension: .34, borderWidth: 2.0, pointRadius: 0 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econCostStackDetailChart')) {{
                const costOpts = dualAxisOptions();
                new Chart(document.getElementById('econCostStackDetailChart'), {{
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ type: 'bar', label: 'Total cost', data: s.total_cost, backgroundColor: 'rgba(207,80,96,.42)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Product cost', data: s.product_cost, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Total ads', data: s.total_ads, borderColor: '#4766ff', tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: costOpts,
                }});
            }}
            if (document.getElementById('econLogisticsDetailChart')) {{
                const logisticsOpts = dualAxisOptions();
                new Chart(document.getElementById('econLogisticsDetailChart'), {{
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ type: 'bar', label: 'Packaging', data: s.packaging, backgroundColor: 'rgba(255,177,93,.55)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Net shipping', data: s.shipping, borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Fixed', data: s.fixed, borderColor: '#cf5060', tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: logisticsOpts,
                }});
            }}
            if (document.getElementById('econAverageTrendDetailChart')) {{
                new Chart(document.getElementById('econAverageTrendDetailChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ label: 'Cum. avg revenue', data: s.cumulative_avg_revenue, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.4, pointRadius: 0 }},
                            {{ label: 'Cum. avg profit', data: s.cumulative_avg_profit, borderColor: '#1f9d66', tension: .30, borderWidth: 2.4, pointRadius: 0 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econRoiRoasDetailChart')) {{
                const roiOpts = dualAxisOptions();
                new Chart(document.getElementById('econRoiRoasDetailChart'), {{
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ type: 'line', label: 'ROI %', data: s.roi, borderColor: '#1f9d66', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'ROAS', data: s.roas, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: roiOpts,
                }});
            }}
            if (document.getElementById('econMarginsDetailChart')) {{
                new Chart(document.getElementById('econMarginsDetailChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ label: 'Gross margin %', data: s.gross_margin, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 0 }},
                            {{ label: 'Pre-ad margin %', data: s.pre_margin, borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 0 }},
                            {{ label: 'Post-ad margin %', data: s.post_margin, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 0 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}

            const marketingItems = [];
            if (hasRows(DATA.cpo_daily)) marketingItems.push({{ id: 'mktDailyCpoRoasChart', title: {{ en: 'Daily CPO and ROAS', sk: 'Denne CPO a ROAS' }}, desc: {{ en: 'Daily cost per order against attributed ROAS.', sk: 'Denne CPO oproti atribucnemu ROAS.' }} }});
            if (hasRows(DATA.weekly_cpo)) marketingItems.push({{ id: 'mktWeeklyCpoChart', title: {{ en: 'Weekly CPO', sk: 'Tyzdenne CPO' }}, desc: {{ en: 'Weekly order acquisition cost and spend.', sk: 'Tyzdenny naklad na objednavku a spend.' }} }});
            if (hasRows(DATA.campaign_cpo)) marketingItems.push({{ id: 'mktCampaignCpoRoasChart', title: {{ en: 'Campaign attribution economics', sk: 'Ekonomika atribucie kampani' }}, desc: {{ en: 'Estimated campaign CPO and ROAS.', sk: 'Odhadovane kampanove CPO a ROAS.' }} }});
            if (hasSeries(DATA.fb_daily.dates)) {{
                marketingItems.push(
                    {{ id: 'mktReachImpressionsChart', title: {{ en: 'Reach and impressions', sk: 'Reach a impresie' }}, desc: {{ en: 'Daily Meta reach compared with impressions.', sk: 'Denn y Meta reach oproti impresiam.' }} }},
                    {{ id: 'mktClicksCtrChart', title: {{ en: 'Clicks and CTR', sk: 'Kliky a CTR' }}, desc: {{ en: 'Click volume with CTR overlay.', sk: 'Objem klikov s CTR overlayom.' }} }},
                    {{ id: 'mktCpcCpmChart', title: {{ en: 'CPC and CPM', sk: 'CPC a CPM' }}, desc: {{ en: 'Efficiency pricing trend for Meta delivery.', sk: 'Trend ceny efektivity pre Meta delivery.' }} }},
                );
            }}
            if (hasRows(DATA.hourly_orders) || hasRows(DATA.fb_hourly)) {{
                marketingItems.push(
                    {{ id: 'mktHourlySpendOrdersChart', title: {{ en: 'Hourly spend and orders', sk: 'Hodinovy spend a objednavky' }}, desc: {{ en: 'Hour-by-hour response between spend and orders.', sk: 'Hodinova odozva medzi spendom a objednavkami.' }} }},
                    {{ id: 'mktHourlyRoasCpoChart', title: {{ en: 'Hourly ROAS and CPO', sk: 'Hodinove ROAS a CPO' }}, desc: {{ en: 'Hour-level efficiency by return and cost per order.', sk: 'Hodinova efektivita podla navratnosti a CPO.' }} }},
                );
            }}
            if (hasRows(DATA.fb_dow)) marketingItems.push({{ id: 'mktDowCtrCpcChart', title: {{ en: 'Weekday CTR and CPC', sk: 'CTR a CPC podla dna' }}, desc: {{ en: 'Meta efficiency by day of week.', sk: 'Meta efektivita podla dna v tyzdni.' }} }});
            if (hasRows(DATA.spend_effectiveness_rows)) {{
                marketingItems.push(
                    {{ id: 'mktSpendRangeRoasChart', title: {{ en: 'Spend range ROAS', sk: 'ROAS podla spend bucketu' }}, desc: {{ en: 'ROAS by daily spend bucket.', sk: 'ROAS podla bucketu denneho spendu.' }} }},
                    {{ id: 'mktSpendRangeRevenueChart', title: {{ en: 'Spend range revenue and profit', sk: 'Trzba a zisk podla spend bucketu' }}, desc: {{ en: 'Revenue, profit and orders by spend band.', sk: 'Trzba, zisk a objednavky podla spend pasma.' }} }},
                );
            }}
            if (hasRows(DATA.dow_effectiveness_rows)) marketingItems.push({{ id: 'mktDowRevenueSpendChart', title: {{ en: 'Weekday revenue and spend', sk: 'Trzba a spend podla dna' }}, desc: {{ en: 'Average weekday business output versus FB spend.', sk: 'Priemerny vykon dna oproti FB spendu.' }} }});
            if (hasRows(DATA.acquisition_family.cube_rows)) {{
                marketingItems.push(
                    {{ id: 'mktSourceFamilyMixChart', title: {{ en: 'Source proxy x product family', sk: 'Source proxy x produktova family' }}, desc: {{ en: 'New-customer mix by paid-day source proxy and dominant first-order family.', sk: 'Mix novych zakaznikov podla paid-day source proxy a dominantnej family prveho nakupu.' }} }},
                    {{ id: 'mktSourceFamilyContributionChart', title: {{ en: '90d contribution by source proxy x family', sk: '90d contribution podla source proxy x family' }}, desc: {{ en: 'Downstream 90d contribution quality by source proxy and first-order family.', sk: 'Kvalita downstream 90d contribution podla source proxy a family prveho nakupu.' }} }},
                );
            }}
            if (hasRows(DATA.acquisition_family.source_rows)) marketingItems.push({{ id: 'mktSourceProxySummaryChart', title: {{ en: 'Source proxy summary', sk: 'Sumar source proxy' }}, desc: {{ en: 'New-customer volume and contribution LTV per customer by source proxy.', sk: 'Objem novych zakaznikov a contribution LTV na zakaznika podla source proxy.' }} }});
            renderGalleryCards('libraryMarketing', marketingItems);

            if (document.getElementById('mktDailyCpoRoasChart')) {{
                const dailyCpoOpts = dualAxisOptions();
                new Chart(document.getElementById('mktDailyCpoRoasChart'), {{
                    data: {{
                        labels: DATA.cpo_daily.map(x => x.date || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'FB spend', data: DATA.cpo_daily.map(x => Number(x.fb_spend || 0)), backgroundColor: 'rgba(255,138,31,.42)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CPO', data: DATA.cpo_daily.map(x => Number(x.cpo || 0)), borderColor: '#cf5060', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'ROAS', data: DATA.cpo_daily.map(x => Number(x.roas || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: dailyCpoOpts,
                }});
            }}
            if (document.getElementById('mktWeeklyCpoChart')) {{
                const weeklyOpts = dualAxisOptions();
                new Chart(document.getElementById('mktWeeklyCpoChart'), {{
                    data: {{
                        labels: DATA.weekly_cpo.map(x => x.week_start || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'CPO', data: DATA.weekly_cpo.map(x => Number(x.cpo || 0)), backgroundColor: 'rgba(255,138,31,.65)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'FB spend', data: DATA.weekly_cpo.map(x => Number(x.fb_spend || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: weeklyOpts,
                }});
            }}
            if (document.getElementById('mktCampaignCpoRoasChart')) {{
                const campaignOpts = dualAxisOptions();
                new Chart(document.getElementById('mktCampaignCpoRoasChart'), {{
                    data: {{
                        labels: DATA.campaign_cpo.map(x => (x.campaign_name || 'Unknown').slice(0, 24)),
                        datasets: [
                            {{ type: 'bar', label: 'Estimated CPO', data: DATA.campaign_cpo.map(x => Number(x.estimated_cpo || 0)), backgroundColor: 'rgba(255,138,31,.65)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Estimated ROAS', data: DATA.campaign_cpo.map(x => Number(x.estimated_roas || 0)), borderColor: '#1f9d66', tension: .28, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Spend', data: DATA.campaign_cpo.map(x => Number(x.spend || 0)), borderColor: '#4766ff', tension: .28, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y' }},
                        ],
                    }},
                    options: campaignOpts,
                }});
            }}
            if (document.getElementById('mktReachImpressionsChart')) {{
                const reachOpts = dualAxisOptions();
                new Chart(document.getElementById('mktReachImpressionsChart'), {{
                    data: {{
                        labels: DATA.fb_daily.dates,
                        datasets: [
                            {{ type: 'bar', label: 'Impressions', data: DATA.fb_daily.impressions, backgroundColor: 'rgba(255,138,31,.36)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Reach', data: DATA.fb_daily.reach, borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: reachOpts,
                }});
            }}
            if (document.getElementById('mktClicksCtrChart')) {{
                const clickOpts = dualAxisOptions();
                new Chart(document.getElementById('mktClicksCtrChart'), {{
                    data: {{
                        labels: DATA.fb_daily.dates,
                        datasets: [
                            {{ type: 'bar', label: 'Clicks', data: DATA.fb_daily.clicks, backgroundColor: 'rgba(71,102,255,.34)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CTR %', data: DATA.fb_daily.ctr, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: clickOpts,
                }});
            }}
            if (document.getElementById('mktCpcCpmChart')) {{
                const cpcOpts = dualAxisOptions();
                new Chart(document.getElementById('mktCpcCpmChart'), {{
                    data: {{
                        labels: DATA.fb_daily.dates,
                        datasets: [
                            {{ type: 'line', label: 'CPC', data: DATA.fb_daily.cpc, borderColor: '#cf5060', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CPM', data: DATA.fb_daily.cpm, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: cpcOpts,
                }});
            }}
            if (document.getElementById('mktHourlySpendOrdersChart') || document.getElementById('mktHourlyRoasCpoChart')) {{
                const hourlyMap = new Map();
                DATA.fb_hourly.forEach(row => hourlyMap.set(String(row.hour), {{ spend: Number(row.spend || 0), clicks: Number(row.clicks || 0), ctr: Number(row.ctr || 0), cpc: Number(row.cpc || 0) }}));
                DATA.hourly_orders.forEach(row => {{
                    const key = String(row.hour);
                    const existing = hourlyMap.get(key) || {{ spend: 0, clicks: 0, ctr: 0, cpc: 0 }};
                    existing.orders = Number(row.orders || 0);
                    existing.revenue = Number(row.revenue || 0);
                    hourlyMap.set(key, existing);
                }});
                const hours = Array.from({{ length: 24 }}, (_, idx) => String(idx));
                if (document.getElementById('mktHourlySpendOrdersChart')) {{
                    const hourlySpendOpts = dualAxisOptions();
                    new Chart(document.getElementById('mktHourlySpendOrdersChart'), {{
                        data: {{
                            labels: hours,
                            datasets: [
                                {{ type: 'bar', label: 'Spend', data: hours.map(h => (hourlyMap.get(h) || {{}}).spend || 0), backgroundColor: 'rgba(255,138,31,.42)', borderRadius: 8, yAxisID: 'y' }},
                                {{ type: 'line', label: 'Orders', data: hours.map(h => (hourlyMap.get(h) || {{}}).orders || 0), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y1' }},
                            ],
                        }},
                        options: hourlySpendOpts,
                    }});
                }}
                if (document.getElementById('mktHourlyRoasCpoChart')) {{
                    const hourlyEffOpts = dualAxisOptions();
                    new Chart(document.getElementById('mktHourlyRoasCpoChart'), {{
                        data: {{
                            labels: hours,
                            datasets: [
                                {{ type: 'line', label: 'Hourly CPO', data: hours.map(h => {{ const row = hourlyMap.get(h) || {{}}; const orders = Number(row.orders || 0); const spend = Number(row.spend || 0); return orders > 0 ? spend / orders : 0; }}), borderColor: '#cf5060', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y' }},
                                {{ type: 'line', label: 'Hourly ROAS', data: hours.map(h => {{ const row = hourlyMap.get(h) || {{}}; const spend = Number(row.spend || 0); const revenue = Number(row.revenue || 0); return spend > 0 ? revenue / spend : 0; }}), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y1' }},
                            ],
                        }},
                        options: hourlyEffOpts,
                    }});
                }}
            }}
            if (document.getElementById('mktDowCtrCpcChart')) {{
                const dowOpts = dualAxisOptions();
                new Chart(document.getElementById('mktDowCtrCpcChart'), {{
                    data: {{
                        labels: DATA.fb_dow.map(x => x.day_of_week || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Spend', data: DATA.fb_dow.map(x => Number(x.total_spend || 0)), backgroundColor: 'rgba(71,102,255,.34)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CTR %', data: DATA.fb_dow.map(x => Number(x.ctr || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'CPC', data: DATA.fb_dow.map(x => Number(x.cpc || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y' }},
                        ],
                    }},
                    options: dowOpts,
                }});
            }}
            if (document.getElementById('mktSpendRangeRoasChart')) {{
                const spendOpts = dualAxisOptions();
                new Chart(document.getElementById('mktSpendRangeRoasChart'), {{
                    data: {{
                        labels: DATA.spend_effectiveness_rows.map(x => x.spend_range || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Avg spend', data: DATA.spend_effectiveness_rows.map(x => Number(x.avg_spend || 0)), backgroundColor: 'rgba(255,138,31,.52)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'ROAS', data: DATA.spend_effectiveness_rows.map(x => Number(x.roas || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: spendOpts,
                }});
            }}
            if (document.getElementById('mktSpendRangeRevenueChart')) {{
                const spendValueOpts = dualAxisOptions();
                new Chart(document.getElementById('mktSpendRangeRevenueChart'), {{
                    data: {{
                        labels: DATA.spend_effectiveness_rows.map(x => x.spend_range || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Avg revenue', data: DATA.spend_effectiveness_rows.map(x => Number(x.avg_revenue || 0)), backgroundColor: 'rgba(255,138,31,.52)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg profit', data: DATA.spend_effectiveness_rows.map(x => Number(x.avg_profit || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg orders', data: DATA.spend_effectiveness_rows.map(x => Number(x.avg_orders || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: spendValueOpts,
                }});
            }}
            if (document.getElementById('mktDowRevenueSpendChart')) {{
                const dowSpendOpts = dualAxisOptions();
                new Chart(document.getElementById('mktDowRevenueSpendChart'), {{
                    data: {{
                        labels: DATA.dow_effectiveness_rows.map(x => x.day_name || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Avg revenue', data: DATA.dow_effectiveness_rows.map(x => Number(x.avg_revenue || 0)), backgroundColor: 'rgba(255,138,31,.52)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg profit', data: DATA.dow_effectiveness_rows.map(x => Number(x.avg_profit || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg FB spend', data: DATA.dow_effectiveness_rows.map(x => Number(x.avg_fb_spend || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: dowSpendOpts,
                }});
            }}
            if (document.getElementById('mktSourceFamilyMixChart')) {{
                const cubeRows = DATA.acquisition_family.cube_rows || [];
                const familyLabels = [...new Set(cubeRows.map(x => x.product_family_label || 'Other / unclassified'))];
                const sourceLabels = [...new Set(cubeRows.map(x => x.source_proxy_label || 'Unknown source'))];
                const colorMap = {{
                    'Facebook-paid day': 'rgba(255,138,31,.72)',
                    'Google-paid day': 'rgba(71,102,255,.68)',
                    'Mixed paid day': 'rgba(31,157,102,.68)',
                    'Organic / unknown day': 'rgba(143,130,120,.62)',
                }};
                const mixOpts = baseOptions();
                mixOpts.scales.x.stacked = true;
                mixOpts.scales.y.stacked = true;
                new Chart(document.getElementById('mktSourceFamilyMixChart'), {{
                    type: 'bar',
                    data: {{
                        labels: familyLabels,
                        datasets: sourceLabels.map(source => ({{
                            label: source,
                            data: familyLabels.map(family => {{
                                const row = cubeRows.find(x => (x.source_proxy_label || 'Unknown source') === source && (x.product_family_label || 'Other / unclassified') === family);
                                return Number((row && row.new_customers) || 0);
                            }}),
                            backgroundColor: colorMap[source] || 'rgba(255,138,31,.55)',
                            borderRadius: 8,
                        }})),
                    }},
                    options: mixOpts,
                }});
            }}
            if (document.getElementById('mktSourceFamilyContributionChart')) {{
                const rows = [...(DATA.acquisition_family.cube_rows || [])]
                    .sort((a, b) => Number(b.contribution_ltv_90d_per_customer || 0) - Number(a.contribution_ltv_90d_per_customer || 0))
                    .slice(0, 12);
                const familyContributionOpts = dualAxisOptions();
                new Chart(document.getElementById('mktSourceFamilyContributionChart'), {{
                    data: {{
                        labels: rows.map(x => `${{x.source_proxy_label || 'Unknown'}} • ${{x.product_family_label || 'Other'}}`.slice(0, 32)),
                        datasets: [
                            {{ type: 'bar', label: 'Contribution LTV 90d / customer', data: rows.map(x => Number(x.contribution_ltv_90d_per_customer || 0)), backgroundColor: 'rgba(31,157,102,.68)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Repeat 90d %', data: rows.map(x => Number(x.repeat_90d_rate_pct || 0)), borderColor: '#cf5060', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: familyContributionOpts,
                }});
            }}
            if (document.getElementById('mktSourceProxySummaryChart')) {{
                const rows = DATA.acquisition_family.source_rows || [];
                const sourceSummaryOpts = dualAxisOptions();
                new Chart(document.getElementById('mktSourceProxySummaryChart'), {{
                    data: {{
                        labels: rows.map(x => x.source_proxy_label || 'Unknown'),
                        datasets: [
                            {{ type: 'bar', label: 'New customers', data: rows.map(x => Number(x.new_customers || 0)), backgroundColor: 'rgba(255,138,31,.66)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Contribution LTV 90d / customer', data: rows.map(x => Number(x.contribution_ltv_90d_per_customer || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: sourceSummaryOpts,
                }});
            }}
        }}
        function buildLibraryCustomersPatternsProducts() {{
            const customerItems = [];
            if (hasSeries(DATA.customer_mix.dates)) {{
                customerItems.push(
                    {{ id: 'custNewReturningRevenueChart', title: {{ en: 'New vs returning revenue', sk: 'Nova vs vratena trzba' }}, desc: {{ en: 'Daily revenue split between new and returning customers.', sk: 'Denny split trzby medzi novymi a vracajucimi sa zakaznikmi.' }} }},
                    {{ id: 'custNewReturningRevenuePieChart', title: {{ en: 'Revenue mix share', sk: 'Podiel revenue mixu' }}, desc: {{ en: 'Overall share of new and returning revenue.', sk: 'Celkovy podiel novej a vracajucej sa trzby.' }} }},
                );
            }}
            if (hasSeries(DATA.returning_customers.labels)) {{
                customerItems.push({{ id: 'custReturningVolumeChart', title: {{ en: 'Returning customer volume', sk: 'Objem vracajucich sa zakaznikov' }}, desc: {{ en: 'Share and order volume of returning customers.', sk: 'Podiel a objem objednavok vracajucich sa zakaznikov.' }} }});
            }}
            if (hasSeries(DATA.refunds.dates)) {{
                customerItems.push({{ id: 'custRefundRateAmountChart', title: {{ en: 'Refund pressure', sk: 'Refund pressure' }}, desc: {{ en: 'Refund rate and refunded amount through time.', sk: 'Refund rate a refundovana suma v case.' }} }});
            }}
            if (hasSeries(DATA.clv.labels)) {{
                customerItems.push(
                    {{ id: 'custClvCacDetailChart', title: {{ en: 'CLV vs CAC detail', sk: 'Detail CLV vs CAC' }}, desc: {{ en: 'Weekly CLV, CAC and LTV/CAC ratio.', sk: 'Tyzdenne CLV, CAC a pomer LTV/CAC.' }} }},
                    {{ id: 'custCumulativeClvCacChart', title: {{ en: 'Cumulative CLV vs CAC', sk: 'Kumulativne CLV vs CAC' }}, desc: {{ en: 'Cumulative average CLV compared with cumulative CAC.', sk: 'Kumulativne priemerne CLV oproti kumulativnemu CAC.' }} }},
                );
            }}
            if (hasRows(DATA.cohort_unit_economics_rows)) {{
                customerItems.push(
                    {{ id: 'custCohortContributionLtvCacChart', title: {{ en: 'Cohort contribution LTV/CAC', sk: 'Kohortne contribution LTV/CAC' }}, desc: {{ en: '30/60/90/180-day contribution LTV/CAC by acquisition cohort.', sk: '30/60/90/180-dnove contribution LTV/CAC podla akvizicnej kohorty.' }} }},
                    {{ id: 'custCohortPaybackRecoveryChart', title: {{ en: 'Cohort payback recovery', sk: 'Kohortna payback recovery' }}, desc: {{ en: 'Share of customers that recover blended CAC within each horizon.', sk: 'Podiel zakaznikov, ktori vratia blended CAC v danom horizonte.' }} }},
                    {{ id: 'custCohortCacVsContributionChart', title: {{ en: 'Cohort CAC vs contribution LTV', sk: 'Kohortny CAC vs contribution LTV' }}, desc: {{ en: 'Blended CAC versus mature contribution LTV by cohort.', sk: 'Blended CAC oproti zrelemu contribution LTV podla kohorty.' }} }},
                );
            }}
            if (hasSeries(DATA.order_size.labels)) {{
                customerItems.push({{ id: 'custOrderSizeMixDetailChart', title: {{ en: 'Order size mix', sk: 'Mix velkosti objednavok' }}, desc: {{ en: 'Daily order size distribution by basket size.', sk: 'Denny mix velkosti objednavok podla poctu poloziek.' }} }});
            }}
            if (Number(DATA.customer_concentration_summary.top_10_pct_revenue_share || 0) || Number(DATA.customer_concentration_summary.top_20_pct_revenue_share || 0) || Number(DATA.customer_concentration_summary.top_10_pct_profit_share || 0) || Number(DATA.customer_concentration_summary.top_20_pct_profit_share || 0)) {{
                customerItems.push({{ id: 'custCustomerConcentrationDetailChart', title: {{ en: 'Customer concentration', sk: 'Koncentracia zakaznikov' }}, desc: {{ en: 'How much revenue and profit is concentrated in top customer cohorts.', sk: 'Ako velmi je trzba a zisk koncentrovany v top zakaznickych kohortach.' }} }});
            }}
            if (hasRows(DATA.cohort_order_frequency_rows)) customerItems.push({{ id: 'custOrderFrequencyDetailChart', title: {{ en: 'Order frequency', sk: 'Frekvencia objednavok' }}, desc: {{ en: 'Customer distribution by order frequency.', sk: 'Rozdelenie zakaznikov podla frekvencie objednavok.' }} }});
            if (hasRows(DATA.cohort_time_between_rows)) customerItems.push({{ id: 'custTimeBetweenOrdersDetailChart', title: {{ en: 'Time between orders', sk: 'Cas medzi objednavkami' }}, desc: {{ en: 'Bucketed distribution of time between purchases.', sk: 'Bucketovy rozklad casu medzi nakupmi.' }} }});
            if (hasRows(DATA.cohort_time_by_order_rows)) customerItems.push({{ id: 'custTimeBetweenByOrderDetailChart', title: {{ en: 'Time between order transitions', sk: 'Cas medzi prechodmi objednavok' }}, desc: {{ en: 'Average and median days between order transitions.', sk: 'Priemerny a medianovy cas medzi prechodmi objednavok.' }} }});
            if (hasRows(DATA.cohort_time_to_nth_rows)) customerItems.push({{ id: 'custTimeToNthOrderDetailChart', title: {{ en: 'Time to nth order', sk: 'Cas do n-tej objednavky' }}, desc: {{ en: 'How quickly customers progress to later orders.', sk: 'Ako rychlo zakaznici postupuju k dalsim objednavkam.' }} }});
            if (hasRows(DATA.cohort_revenue_by_order_rows)) customerItems.push({{ id: 'custOrderSequenceValueDetailChart', title: {{ en: 'Value by order sequence', sk: 'Hodnota podla poradia objednavky' }}, desc: {{ en: 'Average order value and total revenue by order number.', sk: 'Priemerna hodnota a celkova trzba podla poradia objednavky.' }} }});
            if (hasRows(DATA.mature_cohort_rows)) customerItems.push({{ id: 'custMatureCohortDetailChart', title: {{ en: 'Mature cohort retention', sk: 'Retencia zrelych kohort' }}, desc: {{ en: 'Retention depth of older cohorts.', sk: 'Hlba retencie starsich kohort.' }} }});
            if (hasRows(DATA.item_retention_rows)) customerItems.push({{ id: 'custFirstItemRetentionDetailChart', title: {{ en: 'First item retention', sk: 'Retencia prveho produktu' }}, desc: {{ en: 'Retention quality of first purchased items.', sk: 'Kvalita retencie podla prveho kupeneho produktu.' }} }});
            if (hasRows(DATA.same_item_rows)) customerItems.push({{ id: 'custSameItemRepurchaseDetailChart', title: {{ en: 'Same-item repurchase', sk: 'Opakovany nakup rovnakeho produktu' }}, desc: {{ en: 'Repurchase rate of the same item among buyers.', sk: 'Miera opakovaneho nakupu rovnakeho produktu.' }} }});
            if (hasRows(DATA.time_to_nth_rows)) customerItems.push({{ id: 'custTimeToNthByFirstItemDetailChart', title: {{ en: 'Time to nth by first item', sk: 'Cas do n-tej podla prveho produktu' }}, desc: {{ en: 'How quickly customers reorder depending on first purchased item.', sk: 'Ako rychlo sa vracaju zakaznici podla prveho produktu.' }} }});
            if (hasRows(DATA.sample_funnel.windows)) {{
                customerItems.push(
                    {{ id: 'custSampleFunnelWindowChart', title: {{ en: 'Sample funnel windows', sk: 'Sample funnel okna' }}, desc: {{ en: 'Conversion from sample-entry cohort into repeat and full-size orders.', sk: 'Konverzia zo sample vstupnej kohorty do repeat a full-size objednavok.' }} }},
                    {{ id: 'custSampleEntryProductChart', title: {{ en: 'Sample entry product quality', sk: 'Kvalita sample vstupnych produktov' }}, desc: {{ en: 'Top sample entry products by 60d full-size conversion.', sk: 'Top sample vstupne produkty podla 60d full-size konverzie.' }} }},
                );
            }}
            if (hasRows(DATA.refill_cohorts.window_rows)) {{
                customerItems.push(
                    {{ id: 'custRefillWindowChart', title: {{ en: 'Refill windows by entry bucket', sk: 'Refill okna podla vstupneho bucketu' }}, desc: {{ en: 'How quickly sample and full-size entry cohorts come back for the second order.', sk: 'Ako rychlo sa sample a full-size vstupne kohorty vracaju na druhu objednavku.' }} }},
                    {{ id: 'custRefillBucketChart', title: {{ en: 'Refill bucket quality', sk: 'Kvalita refill bucketov' }}, desc: {{ en: 'Second-order AOV and refill rates by entry bucket.', sk: 'AOV druhej objednavky a refill rates podla vstupneho bucketu.' }} }},
                );
            }}
            if (hasRows(DATA.refill_cohorts.cohort_rows)) {{
                customerItems.push(
                    {{ id: 'custRefillCohortChart', title: {{ en: 'Refill cohort trend', sk: 'Trend refill kohort' }}, desc: {{ en: '90d refill rate and days-to-second-order by cohort month.', sk: '90d refill rate a cas do druhej objednavky podla kohortneho mesiaca.' }} }},
                );
            }}
            if (hasRows(DATA.direct_assisted.entry_rows)) {{
                customerItems.push(
                    {{ id: 'custDirectAssistedEntryChart', title: {{ en: 'Direct vs assisted CM3 by entry product', sk: 'Direct vs assisted CM3 podla vstupneho produktu' }}, desc: {{ en: 'How much CM3 comes directly from the first order versus downstream orders.', sk: 'Kolko CM3 vznikne priamo v prvej objednavke a kolko downstream.' }} }},
                );
            }}
            if (hasRows(DATA.direct_assisted.window_rows)) {{
                customerItems.push(
                    {{ id: 'custDirectAssistedWindowChart', title: {{ en: 'Assisted profitability windows', sk: 'Okna asistovanej profitability' }}, desc: {{ en: 'Downstream CM3 recovery and assisted share over 30/60/90/180 day windows.', sk: 'Downstream CM3 recovery a assisted share v 30/60/90/180 dnoch.' }} }},
                );
            }}
            if (hasRows(DATA.crm_funnel.segment_rows)) {{
                customerItems.push(
                    {{ id: 'custCrmFunnelChart', title: {{ en: 'CRM funnel KPI layer', sk: 'CRM funnel KPI vrstva' }}, desc: {{ en: 'Operational CRM segments tied to measurable KPI baselines.', sk: 'Operativne CRM segmenty naviazane na meratelne KPI baseline.' }} }},
                );
            }}
            renderGalleryCards('libraryCustomers', customerItems);

            if (document.getElementById('custNewReturningRevenueChart')) {{
                new Chart(document.getElementById('custNewReturningRevenueChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.customer_mix.dates,
                        datasets: [
                            {{ label: 'New revenue', data: DATA.customer_mix.new, backgroundColor: 'rgba(255,138,31,.75)', borderRadius: 6, stack: 'mix' }},
                            {{ label: 'Returning revenue', data: DATA.customer_mix.returning, backgroundColor: 'rgba(71,102,255,.72)', borderRadius: 6, stack: 'mix' }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custNewReturningRevenuePieChart')) {{
                const newTotal = DATA.customer_mix.new.reduce((sum, value) => sum + Number(value || 0), 0);
                const returningTotal = DATA.customer_mix.returning.reduce((sum, value) => sum + Number(value || 0), 0);
                new Chart(document.getElementById('custNewReturningRevenuePieChart'), {{
                    type: 'doughnut',
                    data: {{
                        labels: ['New', 'Returning'],
                        datasets: [{{ data: [newTotal, returningTotal], backgroundColor: ['#ff8a1f', '#4766ff'], borderColor: '#fff9f3', borderWidth: 4, hoverOffset: 8 }}],
                    }},
                    options: doughnutOptions(),
                }});
            }}
            if (document.getElementById('custReturningVolumeChart')) {{
                const returningOpts = dualAxisOptions();
                new Chart(document.getElementById('custReturningVolumeChart'), {{
                    data: {{
                        labels: DATA.returning_customers.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Unique customers', data: DATA.returning_customers.unique_customers, backgroundColor: 'rgba(255,138,31,.52)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Returning orders', data: DATA.returning_customers.returning_orders, borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Returning %', data: DATA.returning_customers.returning_pct, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y1' }},
                        ],
                    }},
                    options: returningOpts,
                }});
            }}
            if (document.getElementById('custRefundRateAmountChart')) {{
                const refundOpts = dualAxisOptions();
                new Chart(document.getElementById('custRefundRateAmountChart'), {{
                    data: {{
                        labels: DATA.refunds.dates,
                        datasets: [
                            {{ type: 'line', label: 'Refund rate %', data: DATA.refunds.rate, borderColor: '#cf5060', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y1' }},
                            {{ type: 'bar', label: 'Refund amount', data: DATA.refunds.amount, backgroundColor: 'rgba(138,44,61,.45)', borderRadius: 8, yAxisID: 'y' }},
                        ],
                    }},
                    options: refundOpts,
                }});
            }}
            if (document.getElementById('custClvCacDetailChart')) {{
                const clvOpts = dualAxisOptions();
                new Chart(document.getElementById('custClvCacDetailChart'), {{
                    data: {{
                        labels: DATA.clv.labels,
                        datasets: [
                            {{ type: 'line', label: 'Avg CLV', data: DATA.clv.avg_clv, borderColor: '#8b5cf6', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CAC', data: DATA.clv.cac, borderColor: '#cf5060', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'LTV/CAC', data: DATA.clv.ltv_cac_ratio, borderColor: '#ff8a1f', borderDash: [8, 6], tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: clvOpts,
                }});
            }}
            if (document.getElementById('custCumulativeClvCacChart')) {{
                const cumOpts = dualAxisOptions();
                new Chart(document.getElementById('custCumulativeClvCacChart'), {{
                    data: {{
                        labels: DATA.clv.labels,
                        datasets: [
                            {{ type: 'line', label: 'Cum. avg CLV', data: DATA.clv.cumulative_avg_clv, borderColor: '#8b5cf6', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Cum. avg CAC', data: DATA.clv.cumulative_avg_cac, borderColor: '#cf5060', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg return days', data: DATA.clv.avg_return_time_days, borderColor: '#4766ff', borderDash: [8, 6], tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: cumOpts,
                }});
            }}
            if (document.getElementById('custCohortContributionLtvCacChart')) {{
                const cohortRows = DATA.cohort_unit_economics_rows;
                new Chart(document.getElementById('custCohortContributionLtvCacChart'), {{
                    type: 'line',
                    data: {{
                        labels: cohortRows.map(x => x.cohort_month || '-'),
                        datasets: [
                            {{ label: '30d', data: cohortRows.map(x => nullableNumber(x.contribution_ltv_cac_30d)), borderColor: '#ffb96d', tension: .30, borderWidth: 2.0, pointRadius: 2, spanGaps: true }},
                            {{ label: '60d', data: cohortRows.map(x => nullableNumber(x.contribution_ltv_cac_60d)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 2, spanGaps: true }},
                            {{ label: '90d', data: cohortRows.map(x => nullableNumber(x.contribution_ltv_cac_90d)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 2, spanGaps: true }},
                            {{ label: '180d', data: cohortRows.map(x => nullableNumber(x.contribution_ltv_cac_180d)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.4, pointRadius: 2, spanGaps: true }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custCohortPaybackRecoveryChart')) {{
                const cohortRows = DATA.cohort_unit_economics_rows;
                new Chart(document.getElementById('custCohortPaybackRecoveryChart'), {{
                    type: 'bar',
                    data: {{
                        labels: cohortRows.map(x => x.cohort_month || '-'),
                        datasets: [
                            {{ label: '30d recovery %', data: cohortRows.map(x => nullableNumber(x.payback_recovery_30d_pct)), backgroundColor: 'rgba(255,185,109,.70)', borderRadius: 6 }},
                            {{ label: '60d recovery %', data: cohortRows.map(x => nullableNumber(x.payback_recovery_60d_pct)), backgroundColor: 'rgba(255,138,31,.68)', borderRadius: 6 }},
                            {{ label: '90d recovery %', data: cohortRows.map(x => nullableNumber(x.payback_recovery_90d_pct)), backgroundColor: 'rgba(71,102,255,.62)', borderRadius: 6 }},
                            {{ label: '180d recovery %', data: cohortRows.map(x => nullableNumber(x.payback_recovery_180d_pct)), backgroundColor: 'rgba(31,157,102,.62)', borderRadius: 6 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custCohortCacVsContributionChart')) {{
                const cohortRows = DATA.cohort_unit_economics_rows;
                const cohortOpts = dualAxisOptions();
                new Chart(document.getElementById('custCohortCacVsContributionChart'), {{
                    data: {{
                        labels: cohortRows.map(x => x.cohort_month || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Blended CAC', data: cohortRows.map(x => nullableNumber(x.cohort_blended_cac)), backgroundColor: 'rgba(207,80,96,.52)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: '90d contribution LTV', data: cohortRows.map(x => nullableNumber(x.contribution_ltv_90d)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y1', spanGaps: true }},
                            {{ type: 'line', label: '180d contribution LTV', data: cohortRows.map(x => nullableNumber(x.contribution_ltv_180d)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.4, pointRadius: 2, yAxisID: 'y1', spanGaps: true }},
                        ],
                    }},
                    options: cohortOpts,
                }});
            }}
            if (document.getElementById('custOrderSizeMixDetailChart')) {{
                new Chart(document.getElementById('custOrderSizeMixDetailChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.order_size.labels,
                        datasets: [
                            {{ label: '1 item', data: DATA.order_size.one, backgroundColor: 'rgba(255,138,31,.45)', stack: 'size', borderRadius: 6 }},
                            {{ label: '2 items', data: DATA.order_size.two, backgroundColor: 'rgba(255,177,93,.55)', stack: 'size', borderRadius: 6 }},
                            {{ label: '3 items', data: DATA.order_size.three, backgroundColor: 'rgba(255,213,157,.65)', stack: 'size', borderRadius: 6 }},
                            {{ label: '4 items', data: DATA.order_size.four, backgroundColor: 'rgba(71,102,255,.45)', stack: 'size', borderRadius: 6 }},
                            {{ label: '5+ items', data: DATA.order_size.five_plus, backgroundColor: 'rgba(31,157,102,.45)', stack: 'size', borderRadius: 6 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custCustomerConcentrationDetailChart')) {{
                const cc = DATA.customer_concentration_summary;
                new Chart(document.getElementById('custCustomerConcentrationDetailChart'), {{
                    type: 'bar',
                    data: {{
                        labels: ['Top10 Revenue', 'Top20 Revenue', 'Top10 Profit', 'Top20 Profit'],
                        datasets: [{{ label: 'Share %', data: [Number(cc.top_10_pct_revenue_share || 0), Number(cc.top_20_pct_revenue_share || 0), Number(cc.top_10_pct_profit_share || 0), Number(cc.top_20_pct_profit_share || 0)], backgroundColor: ['rgba(255,138,31,.72)', 'rgba(255,177,93,.72)', 'rgba(71,102,255,.72)', 'rgba(31,157,102,.72)'], borderRadius: 8 }}],
                    }},
                    options: {{ ...baseOptions(), plugins: {{ ...baseOptions().plugins, legend: {{ display: false }} }} }},
                }});
            }}
            if (document.getElementById('custOrderFrequencyDetailChart')) {{
                const freqOpts = dualAxisOptions();
                new Chart(document.getElementById('custOrderFrequencyDetailChart'), {{
                    data: {{
                        labels: DATA.cohort_order_frequency_rows.map(x => `${{x.frequency || '-'}}x`),
                        datasets: [
                            {{ type: 'bar', label: 'Customers', data: DATA.cohort_order_frequency_rows.map(x => Number(x.customer_count || 0)), backgroundColor: 'rgba(255,138,31,.62)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Orders share %', data: DATA.cohort_order_frequency_rows.map(x => Number(x.orders_pct || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y1' }},
                        ],
                    }},
                    options: freqOpts,
                }});
            }}
            if (document.getElementById('custTimeBetweenOrdersDetailChart')) {{
                new Chart(document.getElementById('custTimeBetweenOrdersDetailChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.cohort_time_between_rows.map(x => x.time_bucket || '-'),
                        datasets: [
                            {{ label: 'Count', data: DATA.cohort_time_between_rows.map(x => Number(x.count || 0)), backgroundColor: 'rgba(71,102,255,.64)', borderRadius: 8 }},
                            {{ label: 'Share %', data: DATA.cohort_time_between_rows.map(x => Number(x.percentage || 0)), type: 'line', borderColor: '#ff8a1f', tension: .30, borderWidth: 2.0, pointRadius: 2, yAxisID: 'y1' }},
                        ],
                    }},
                    options: dualAxisOptions(),
                }});
            }}
            if (document.getElementById('custTimeBetweenByOrderDetailChart')) {{
                const transOpts = dualAxisOptions();
                new Chart(document.getElementById('custTimeBetweenByOrderDetailChart'), {{
                    data: {{
                        labels: DATA.cohort_time_by_order_rows.map(x => x.transition || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Count', data: DATA.cohort_time_by_order_rows.map(x => Number(x.count || 0)), backgroundColor: 'rgba(255,138,31,.55)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg days', data: DATA.cohort_time_by_order_rows.map(x => Number(x.avg_days || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Median days', data: DATA.cohort_time_by_order_rows.map(x => Number(x.median_days || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 2, yAxisID: 'y1' }},
                        ],
                    }},
                    options: transOpts,
                }});
            }}
            if (document.getElementById('custTimeToNthOrderDetailChart')) {{
                const nthOpts = dualAxisOptions();
                new Chart(document.getElementById('custTimeToNthOrderDetailChart'), {{
                    data: {{
                        labels: DATA.cohort_time_to_nth_rows.map(x => String(x.order_number || '-')),
                        datasets: [
                            {{ type: 'line', label: 'Avg days from first', data: DATA.cohort_time_to_nth_rows.map(x => Number(x.avg_days_from_first || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg days from previous', data: DATA.cohort_time_to_nth_rows.map(x => Number(x.avg_days_from_prev || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Customers', data: DATA.cohort_time_to_nth_rows.map(x => Number(x.customer_count || 0)), borderColor: '#4766ff', borderDash: [8, 6], tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: nthOpts,
                }});
            }}
            if (document.getElementById('custOrderSequenceValueDetailChart')) {{
                const seqOpts = dualAxisOptions();
                new Chart(document.getElementById('custOrderSequenceValueDetailChart'), {{
                    data: {{
                        labels: DATA.cohort_revenue_by_order_rows.map(x => `${{x.order_number || '-'}} order`),
                        datasets: [
                            {{ type: 'bar', label: 'Total revenue', data: DATA.cohort_revenue_by_order_rows.map(x => Number(x.total_revenue || 0)), backgroundColor: 'rgba(255,138,31,.62)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg order value', data: DATA.cohort_revenue_by_order_rows.map(x => Number(x.avg_order_value || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Avg items/order', data: DATA.cohort_revenue_by_order_rows.map(x => Number(x.avg_items_per_order || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: seqOpts,
                }});
            }}
            if (document.getElementById('custMatureCohortDetailChart')) {{
                new Chart(document.getElementById('custMatureCohortDetailChart'), {{
                    data: {{
                        labels: DATA.mature_cohort_rows.map(x => x.cohort || '-'),
                        datasets: [
                            {{ type: 'line', label: '2nd retention %', data: DATA.mature_cohort_rows.map(x => Number(x.retention_2nd_pct || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3 }},
                            {{ type: 'line', label: '3rd retention %', data: DATA.mature_cohort_rows.map(x => Number(x.retention_3rd_pct || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3 }},
                            {{ type: 'line', label: '4th retention %', data: DATA.mature_cohort_rows.map(x => Number(x.retention_4th_pct || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custFirstItemRetentionDetailChart')) {{
                new Chart(document.getElementById('custFirstItemRetentionDetailChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.item_retention_rows.map(x => (x.item_name || '-').slice(0, 24)),
                        datasets: [
                            {{ label: '2nd retention %', data: DATA.item_retention_rows.map(x => Number(x.retention_2nd_pct || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8 }},
                            {{ label: '3rd retention %', data: DATA.item_retention_rows.map(x => Number(x.retention_3rd_pct || 0)), backgroundColor: 'rgba(71,102,255,.72)', borderRadius: 8 }},
                        ],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('custSameItemRepurchaseDetailChart')) {{
                new Chart(document.getElementById('custSameItemRepurchaseDetailChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.same_item_rows.map(x => (x.item_name || '-').slice(0, 24)),
                        datasets: [
                            {{ label: '2x repurchase %', data: DATA.same_item_rows.map(x => Number(x.repurchase_2x_pct || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8 }},
                            {{ label: '3x repurchase %', data: DATA.same_item_rows.map(x => Number(x.repurchase_3x_pct || 0)), backgroundColor: 'rgba(31,157,102,.72)', borderRadius: 8 }},
                        ],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('custTimeToNthByFirstItemDetailChart')) {{
                const firstItemOpts = dualAxisOptions();
                new Chart(document.getElementById('custTimeToNthByFirstItemDetailChart'), {{
                    data: {{
                        labels: DATA.time_to_nth_rows.map(x => (x.item_name || '-').slice(0, 18)),
                        datasets: [
                            {{ type: 'bar', label: '1st order customers', data: DATA.time_to_nth_rows.map(x => Number(x.first_order_customers || 0)), backgroundColor: 'rgba(255,138,31,.55)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg days to 2nd', data: DATA.time_to_nth_rows.map(x => Number(x.avg_days_to_2nd || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Avg days to 3rd', data: DATA.time_to_nth_rows.map(x => Number(x.avg_days_to_3nd || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: firstItemOpts,
                }});
            }}
            if (document.getElementById('custSampleFunnelWindowChart')) {{
                const funnelOpts = dualAxisOptions();
                new Chart(document.getElementById('custSampleFunnelWindowChart'), {{
                    data: {{
                        labels: DATA.sample_funnel.windows.map(x => `${{x.window_days || '-'}}d`),
                        datasets: [
                            {{ type: 'line', label: 'Repeat %', data: DATA.sample_funnel.windows.map(x => Number(x.repeat_pct || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Any full-size %', data: DATA.sample_funnel.windows.map(x => Number(x.fullsize_any_pct || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: '200ml %', data: DATA.sample_funnel.windows.map(x => Number(x.fullsize_200_pct || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.1, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: '500ml %', data: DATA.sample_funnel.windows.map(x => Number(x.fullsize_500_pct || 0)), borderColor: '#8b5cf6', tension: .30, borderWidth: 2.1, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: funnelOpts,
                }});
            }}
            if (document.getElementById('custSampleEntryProductChart')) {{
                new Chart(document.getElementById('custSampleEntryProductChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.sample_funnel.entry_rows.map(x => (x.item_name || '-').slice(0, 26)),
                        datasets: [
                            {{ label: 'Repeat 30d %', data: DATA.sample_funnel.entry_rows.map(x => Number(x.repeat_30d_pct || 0)), backgroundColor: 'rgba(71,102,255,.72)', borderRadius: 8 }},
                            {{ label: 'Any full-size 60d %', data: DATA.sample_funnel.entry_rows.map(x => Number(x.fullsize_any_60d_pct || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8 }},
                        ],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('refillCohortWindowChart')) {{
                const refillWindowRows = DATA.refill_cohorts.window_rows || [];
                const refillWindowLabels = [...new Set(refillWindowRows.map(x => `${{x.window_days || '-'}}d`))];
                const refillBuckets = [...new Set(refillWindowRows.map(x => x.entry_bucket_label || '-'))].slice(0, 4);
                const refillColors = ['#ff8a1f', '#4766ff', '#1f9d66', '#8b5cf6'];
                new Chart(document.getElementById('refillCohortWindowChart'), {{
                    type: 'line',
                    data: {{
                        labels: refillWindowLabels,
                        datasets: refillBuckets.map((bucket, idx) => ({{
                            label: bucket,
                            data: refillWindowLabels.map(label => {{
                                const found = refillWindowRows.find(x => `${{x.window_days || '-'}}d` === label && (x.entry_bucket_label || '-') === bucket);
                                return found ? Number(found.refill_pct || 0) : null;
                            }}),
                            borderColor: refillColors[idx % refillColors.length],
                            tension: .30,
                            borderWidth: 2.3,
                            pointRadius: 3,
                            spanGaps: true,
                        }})),
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custRefillWindowChart')) {{
                const refillWindowRows = DATA.refill_cohorts.window_rows || [];
                const refillWindowLabels = [...new Set(refillWindowRows.map(x => `${{x.window_days || '-'}}d`))];
                const refillBuckets = [...new Set(refillWindowRows.map(x => x.entry_bucket_label || '-'))].slice(0, 4);
                const refillColors = ['#ff8a1f', '#4766ff', '#1f9d66', '#8b5cf6'];
                new Chart(document.getElementById('custRefillWindowChart'), {{
                    type: 'line',
                    data: {{
                        labels: refillWindowLabels,
                        datasets: refillBuckets.map((bucket, idx) => ({{
                            label: bucket,
                            data: refillWindowLabels.map(label => {{
                                const found = refillWindowRows.find(x => `${{x.window_days || '-'}}d` === label && (x.entry_bucket_label || '-') === bucket);
                                return found ? Number(found.refill_pct || 0) : null;
                            }}),
                            borderColor: refillColors[idx % refillColors.length],
                            tension: .30,
                            borderWidth: 2.3,
                            pointRadius: 3,
                            spanGaps: true,
                        }})),
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custRefillBucketChart')) {{
                const refillBucketRows = DATA.refill_cohorts.bucket_rows || [];
                const refillBucketOpts = dualAxisOptions();
                new Chart(document.getElementById('custRefillBucketChart'), {{
                    data: {{
                        labels: refillBucketRows.map(x => x.entry_bucket_label || '-'),
                        datasets: [
                            {{ type: 'bar', label: '90d refill %', data: refillBucketRows.map(x => Number(x.refill_90d_pct || 0)), backgroundColor: 'rgba(255,138,31,.62)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg days to 2nd', data: refillBucketRows.map(x => nullableNumber(x.avg_days_to_2nd)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1', spanGaps: true }},
                            {{ type: 'line', label: '2nd AOV', data: refillBucketRows.map(x => nullableNumber(x.second_order_aov)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1', spanGaps: true }},
                        ],
                    }},
                    options: refillBucketOpts,
                }});
            }}
            if (document.getElementById('custRefillCohortChart')) {{
                const refillCohortRows = DATA.refill_cohorts.cohort_rows || [];
                const refillCohortOpts = dualAxisOptions();
                new Chart(document.getElementById('custRefillCohortChart'), {{
                    data: {{
                        labels: refillCohortRows.map(x => `${{x.cohort_month || '-'}} • ${{(x.entry_bucket_label || '-').slice(0, 12)}}`),
                        datasets: [
                            {{ type: 'bar', label: '90d refill %', data: refillCohortRows.map(x => Number(x.refill_90d_pct || 0)), backgroundColor: 'rgba(255,138,31,.58)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg days to 2nd', data: refillCohortRows.map(x => nullableNumber(x.avg_days_to_2nd)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 2, yAxisID: 'y1', spanGaps: true }},
                        ],
                    }},
                    options: refillCohortOpts,
                }});
            }}
            if (document.getElementById('custDirectAssistedEntryChart')) {{
                const directEntryOpts = dualAxisOptions();
                new Chart(document.getElementById('custDirectAssistedEntryChart'), {{
                    data: {{
                        labels: DATA.direct_assisted.entry_rows.map(x => (x.entry_product || '-').slice(0, 24)),
                        datasets: [
                            {{ type: 'bar', label: 'Direct CM3 / customer', data: DATA.direct_assisted.entry_rows.map(x => Number(x.direct_cm3_per_customer || 0)), backgroundColor: 'rgba(71,102,255,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'bar', label: 'Total CM3 90d / customer', data: DATA.direct_assisted.entry_rows.map(x => Number(x.total_cm3_90d_per_customer || 0)), backgroundColor: 'rgba(31,157,102,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Assisted share 90d %', data: DATA.direct_assisted.entry_rows.map(x => Number(x.assisted_share_90d_pct || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: directEntryOpts,
                }});
            }}
            if (document.getElementById('custDirectAssistedWindowChart')) {{
                const directWindowOpts = dualAxisOptions();
                new Chart(document.getElementById('custDirectAssistedWindowChart'), {{
                    data: {{
                        labels: DATA.direct_assisted.window_rows.map(x => `${{x.window_days || '-'}}d`),
                        datasets: [
                            {{ type: 'bar', label: 'Downstream CM3 / customer', data: DATA.direct_assisted.window_rows.map(x => Number(x.downstream_cm3_per_customer || 0)), backgroundColor: 'rgba(31,157,102,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Total CM3 / customer', data: DATA.direct_assisted.window_rows.map(x => Number(x.total_cm3_per_customer || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Assisted share %', data: DATA.direct_assisted.window_rows.map(x => Number(x.assisted_share_pct || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: directWindowOpts,
                }});
            }}
            if (document.getElementById('custCrmFunnelChart')) {{
                const crmRows = [...(DATA.crm_funnel.segment_rows || [])].sort((a, b) => Number(a.priority || 999) - Number(b.priority || 999));
                const crmOpts = dualAxisOptions();
                new Chart(document.getElementById('custCrmFunnelChart'), {{
                    data: {{
                        labels: crmRows.map(x => (x.segment || '-').slice(0, 22)),
                        datasets: [
                            {{ type: 'bar', label: 'Customers', data: crmRows.map(x => Number(x.count || 0)), backgroundColor: 'rgba(71,102,255,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Priority', data: crmRows.map(x => Number(x.priority || 0)), borderColor: '#cf5060', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Baseline KPI', data: crmRows.map(x => Number(x.baseline_value || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: crmOpts,
                }});
            }}

            const patternItems = [];
            if (hasSeries(DATA.day_of_week.labels)) {{
                patternItems.push(
                    {{ id: 'patDowOrdersRevenueChart', title: {{ en: 'Weekday orders and revenue', sk: 'Objednavky a trzba podla dna' }}, desc: {{ en: 'Average weekday order volume and revenue.', sk: 'Priemerne objednavky a trzba podla dna v tyzdni.' }} }},
                    {{ id: 'patDowProfitSpendChart', title: {{ en: 'Weekday profit and spend', sk: 'Zisk a spend podla dna' }}, desc: {{ en: 'Average weekday profit against FB spend.', sk: 'Priemerny zisk dna oproti FB spendu.' }} }},
                );
            }}
            if (hasSeries(DATA.week_of_month.labels)) {{
                patternItems.push(
                    {{ id: 'patWomRevenueProfitChart', title: {{ en: 'Week of month revenue and profit', sk: 'Trzba a zisk podla tyzdna v mesiaci' }}, desc: {{ en: 'Revenue and profit by balanced week-of-month bucket.', sk: 'Trzba a zisk podla vybalansovaneho tyzdna v mesiaci.' }} }},
                    {{ id: 'patWomAvgDailyChart', title: {{ en: 'Week of month average daily output', sk: 'Priemerny denny vykon podla tyzdna v mesiaci' }}, desc: {{ en: 'Average daily revenue and profit by month phase.', sk: 'Priemerna denna trzba a zisk podla fazy mesiaca.' }} }},
                );
            }}
            if (hasSeries(DATA.day_of_month.labels)) {{
                patternItems.push(
                    {{ id: 'patDomOrdersRevenueChart', title: {{ en: 'Day of month orders and revenue', sk: 'Objednavky a trzba podla dna v mesiaci' }}, desc: {{ en: 'Which dates in the month perform best.', sk: 'Ktore datumy v mesiaci maju najlepsi vykon.' }} }},
                    {{ id: 'patDomAvgDailyChart', title: {{ en: 'Day of month average revenue and profit', sk: 'Priemerna trzba a zisk podla dna v mesiaci' }}, desc: {{ en: 'Average performance per calendar day-of-month.', sk: 'Priemerny vykon podla kalendarneho dna v mesiaci.' }} }},
                );
            }}
            if (hasSeries(DATA.weather_bucket.labels)) patternItems.push({{ id: 'patWeatherBucketChart', title: {{ en: 'Weather bucket impact', sk: 'Dopad weather bucketov' }}, desc: {{ en: 'Revenue and profit delta versus weekday baseline by weather bucket.', sk: 'Odchylka trzby a zisku oproti weekday baseline podla weather bucketu.' }} }});
            if (hasSeries(DATA.weather.dates)) patternItems.push({{ id: 'patWeatherPrecipChart', title: {{ en: 'Weather overlay', sk: 'Weather overlay' }}, desc: {{ en: 'Revenue and profit versus precipitation through time.', sk: 'Trzba a zisk oproti zrazkam v case.' }} }});
            renderGalleryCards('libraryPatterns', patternItems);
            if (document.getElementById('patDowOrdersRevenueChart')) {{
                const dowRevOpts = dualAxisOptions();
                new Chart(document.getElementById('patDowOrdersRevenueChart'), {{
                    data: {{
                        labels: DATA.day_of_week.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Revenue', data: DATA.day_of_week.revenue, backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Orders', data: DATA.day_of_week.orders, borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'AOV', data: DATA.day_of_week.aov, borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: dowRevOpts,
                }});
            }}
            if (document.getElementById('patDowProfitSpendChart')) {{
                const dowProfitOpts = dualAxisOptions();
                new Chart(document.getElementById('patDowProfitSpendChart'), {{
                    data: {{
                        labels: DATA.day_of_week.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Profit', data: DATA.day_of_week.profit, backgroundColor: 'rgba(31,157,102,.64)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'FB spend', data: DATA.day_of_week.fb_spend, borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: dowProfitOpts,
                }});
            }}
            if (document.getElementById('patWomRevenueProfitChart')) {{
                const womOpts = dualAxisOptions();
                new Chart(document.getElementById('patWomRevenueProfitChart'), {{
                    data: {{
                        labels: DATA.week_of_month.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Revenue', data: DATA.week_of_month.revenue, backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Profit', data: DATA.week_of_month.profit, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: womOpts,
                }});
            }}
            if (document.getElementById('patWomAvgDailyChart')) {{
                const womAvgOpts = dualAxisOptions();
                new Chart(document.getElementById('patWomAvgDailyChart'), {{
                    data: {{
                        labels: DATA.week_of_month.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Avg daily revenue', data: DATA.week_of_month.avg_daily_revenue, backgroundColor: 'rgba(255,138,31,.58)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg daily profit', data: DATA.week_of_month.avg_daily_profit, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: womAvgOpts,
                }});
            }}
            if (document.getElementById('patDomOrdersRevenueChart')) {{
                const domOpts = dualAxisOptions();
                new Chart(document.getElementById('patDomOrdersRevenueChart'), {{
                    data: {{
                        labels: DATA.day_of_month.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Orders', data: DATA.day_of_month.orders, backgroundColor: 'rgba(71,102,255,.58)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Revenue', data: DATA.day_of_month.revenue, borderColor: '#ff8a1f', tension: .28, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: domOpts,
                }});
            }}
            if (document.getElementById('patDomAvgDailyChart')) {{
                const domAvgOpts = dualAxisOptions();
                new Chart(document.getElementById('patDomAvgDailyChart'), {{
                    data: {{
                        labels: DATA.day_of_month.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Avg revenue', data: DATA.day_of_month.avg_revenue, backgroundColor: 'rgba(255,138,31,.58)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg profit', data: DATA.day_of_month.avg_profit, borderColor: '#1f9d66', tension: .28, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: domAvgOpts,
                }});
            }}
            if (document.getElementById('patWeatherBucketChart')) {{
                new Chart(document.getElementById('patWeatherBucketChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.weather_bucket.labels,
                        datasets: [
                            {{ label: 'Revenue delta', data: DATA.weather_bucket.revenue_delta, backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8 }},
                            {{ label: 'Profit delta', data: DATA.weather_bucket.profit_delta, backgroundColor: 'rgba(31,157,102,.72)', borderRadius: 8 }},
                        ],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('patWeatherPrecipChart')) {{
                const weatherOpts = dualAxisOptions();
                new Chart(document.getElementById('patWeatherPrecipChart'), {{
                    data: {{
                        labels: DATA.weather.dates,
                        datasets: [
                            {{ type: 'line', label: 'Revenue', data: DATA.weather.revenue, borderColor: '#ff8a1f', tension: .30, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Profit', data: DATA.weather.profit, borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'bar', label: 'Precipitation', data: DATA.weather.precipitation, backgroundColor: 'rgba(71,102,255,.34)', borderRadius: 6, yAxisID: 'y1' }},
                        ],
                    }},
                    options: weatherOpts,
                }});
            }}

            const productItems = [];
            if (hasRows(DATA.combinations_rows)) productItems.push({{ id: 'prodCombinationCountChart', title: {{ en: 'Top product combinations', sk: 'Top kombinacie produktov' }}, desc: {{ en: 'Most frequent product combinations by count and basket value.', sk: 'Najcastejsie kombinacie produktov podla poctu a hodnoty kosika.' }} }});
            if (hasRows(DATA.basket_contribution_rows)) productItems.push({{ id: 'prodBasketContributionChart', title: {{ en: 'Basket contribution economics', sk: 'Kontribucia podla velkosti kosika' }}, desc: {{ en: 'Contribution per order by basket size.', sk: 'Kontribucia na objednavku podla velkosti kosika.' }} }});
            if (hasRows(DATA.cohort_payback_rows)) productItems.push({{ id: 'prodCohortPaybackChart', title: {{ en: 'Cohort payback', sk: 'Kohortny payback' }}, desc: {{ en: 'Recovery speed and CAC quality by acquisition cohort.', sk: 'Rychlost navratnosti a kvalita CAC podla akvizicnej kohorty.' }} }});
            if (hasRows(DATA.daily_margin_rows)) productItems.push({{ id: 'prodMarginStabilityChart', title: {{ en: 'Daily margin stability', sk: 'Stabilita denneho marginu' }}, desc: {{ en: 'Pre-ad contribution margin through time.', sk: 'Pre-ad contribution margin v case.' }} }});
            if (hasRows(DATA.sku_pareto_rows)) productItems.push({{ id: 'prodParetoLibraryChart', title: {{ en: 'SKU Pareto', sk: 'SKU Pareto' }}, desc: {{ en: 'Contribution concentration across top SKUs.', sk: 'Koncentracia kontribucie napriec top SKU.' }} }});
            if (hasRows(DATA.attach_rate_rows)) productItems.push({{ id: 'prodAttachRateLibraryChart', title: {{ en: 'Attach rate', sk: 'Attach rate' }}, desc: {{ en: 'Most common anchor -> attached item pairs.', sk: 'Najcastejsie dvojice anchor -> attached item.' }} }});
            if (hasRows(DATA.bundle_accessory.pair_rows)) productItems.push({{ id: 'prodBundleAccessoryAttachChart', title: {{ en: 'Device -> accessory attach rate', sk: 'Attach rate zariadenie -> doplnok' }}, desc: {{ en: 'Config-driven Roy device family attach-rate by accessory group.', sk: 'Konfigurovany Roy attach-rate podla device family a accessory group.' }} }});
            if (hasRows(DATA.bundle_accessory.pair_rows)) productItems.push({{ id: 'prodBundleAccessoryUpliftChart', title: {{ en: 'Accessory contribution uplift', sk: 'Contribution uplift doplnkov' }}, desc: {{ en: 'Incremental pre-ad contribution per order when the accessory is present.', sk: 'Inkrementalna pre-ad contribution na objednavku pri pritomnosti doplnku.' }} }});
            if (hasRows(DATA.bundle_accessory.device_rows)) productItems.push({{ id: 'prodBundleAccessoryFamilyChart', title: {{ en: 'Device family summary', sk: 'Sumar device family' }}, desc: {{ en: 'Best accessory group per device family by uplift and attach rate.', sk: 'Najlepsi accessory group pre device family podla upliftu a attach rate.' }} }});
            if (hasRows(DATA.bundle_accessory.group_rows)) productItems.push({{ id: 'prodBundleAccessoryGroupChart', title: {{ en: 'Accessory group quality', sk: 'Kvalita accessory groups' }}, desc: {{ en: 'Weighted attach-rate and uplift by accessory category.', sk: 'Vazeny attach-rate a uplift podla kategorie doplnkov.' }} }});
            if (hasRows(DATA.scent_size.same_rows)) productItems.push({{ id: 'prodScentRefillChart', title: {{ en: 'Scent-size refill matrix', sk: 'Scent-size refill matrix' }}, desc: {{ en: 'Same-scent migration from sample to 200ml/500ml and 200ml to 500ml.', sk: 'Same-scent migracia zo sample na 200ml/500ml a z 200ml na 500ml.' }} }});
            if (hasRows(DATA.scent_size.migration_rows)) productItems.push({{ id: 'prodScentMigrationChart', title: {{ en: 'Cross-scent migration', sk: 'Cross-scent migracia' }}, desc: {{ en: 'How often sample customers move into a different scent family.', sk: 'Ako casto sample zakaznici prejdu do ineho scentu.' }} }});
            if (hasRows(DATA.bundle_recommender.recommendation_rows)) productItems.push({{ id: 'prodBundleRecommenderChart', title: {{ en: 'Bundle recommender score', sk: 'Bundle recommender score' }}, desc: {{ en: 'Top family recommendations by attach-rate and CM2 uplift.', sk: 'Top family odporucania podla attach-rate a CM2 upliftu.' }} }});
            if (hasRows(DATA.bundle_recommender.anchor_rows)) productItems.push({{ id: 'prodBundleAnchorChart', title: {{ en: 'Anchor family recommendations', sk: 'Odporucania anchor family' }}, desc: {{ en: 'Best next-family recommendation per anchor family.', sk: 'Najlepsie next-family odporucanie pre kazdu anchor family.' }} }});
            if (hasRows(DATA.promo_discount.bucket_rows)) productItems.push({{ id: 'opsPromoDiscountChart', title: {{ en: 'Promo / discount quality', sk: 'Kvalita promo / discountov' }}, desc: {{ en: 'Detected discount penetration and CM2 quality by new/returning buckets.', sk: 'Detegovana discount penetracia a CM2 kvalita podla new/returning bucketov.' }} }});
            if (hasRows(DATA.b2b_rows)) productItems.push({{ id: 'opsB2bRevenueProfitChart', title: {{ en: 'B2B vs B2C economics', sk: 'Ekonomika B2B vs B2C' }}, desc: {{ en: 'Revenue, profit and AOV by customer type.', sk: 'Trzba, zisk a AOV podla typu zakaznika.' }} }});
            if (hasRows(DATA.order_status_rows)) productItems.push({{ id: 'opsStatusRevenueChart', title: {{ en: 'Order status mix', sk: 'Mix stavov objednavok' }}, desc: {{ en: 'Orders and revenue by final order status.', sk: 'Objednavky a trzba podla finalneho statusu.' }} }});
            if (hasRows(DATA.segment_rows)) productItems.push({{ id: 'opsSegmentPriorityChart', title: {{ en: 'Email segment volume', sk: 'Objem email segmentov' }}, desc: {{ en: 'Size and priority of lifecycle email segments.', sk: 'Velkost a priorita lifecycle email segmentov.' }} }});
            if (DATA.consistency && (DATA.consistency.roas_delta !== null || DATA.consistency.margin_delta !== null || DATA.consistency.cac_delta !== null)) productItems.push({{ id: 'opsConsistencyChart', title: {{ en: 'Consistency checks', sk: 'Konzistencne kontroly' }}, desc: {{ en: 'Sanity check deltas across ROAS, margin and CAC.', sk: 'Sanity check odchylky pre ROAS, margin a CAC.' }} }});
            renderGalleryCards('libraryProductsOps', productItems);
            if (document.getElementById('prodCombinationCountChart')) {{
                const comboOpts = dualAxisOptions();
                new Chart(document.getElementById('prodCombinationCountChart'), {{
                    data: {{
                        labels: DATA.combinations_rows.map(x => (x.combination || '-').slice(0, 28)),
                        datasets: [
                            {{ type: 'bar', label: 'Count', data: DATA.combinations_rows.map(x => Number(x.count || 0)), backgroundColor: 'rgba(255,138,31,.66)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Price', data: DATA.combinations_rows.map(x => Number(x.price || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: comboOpts,
                }});
            }}
            if (document.getElementById('prodBasketContributionChart')) {{
                const basketOpts = dualAxisOptions();
                new Chart(document.getElementById('prodBasketContributionChart'), {{
                    data: {{
                        labels: DATA.basket_contribution_rows.map(x => x.basket_size || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Orders', data: DATA.basket_contribution_rows.map(x => Number(x.orders || 0)), backgroundColor: 'rgba(71,102,255,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Contribution/order', data: DATA.basket_contribution_rows.map(x => Number(x.contribution_per_order || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Contribution margin %', data: DATA.basket_contribution_rows.map(x => Number(x.contribution_margin_pct || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: basketOpts,
                }});
            }}
            if (document.getElementById('prodCohortPaybackChart')) {{
                const paybackOpts = dualAxisOptions();
                new Chart(document.getElementById('prodCohortPaybackChart'), {{
                    data: {{
                        labels: DATA.cohort_payback_rows.map(x => x.cohort_month || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Recovery rate %', data: DATA.cohort_payback_rows.map(x => Number(x.recovery_rate_pct || 0)), backgroundColor: 'rgba(31,157,102,.60)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Cohort CAC', data: DATA.cohort_payback_rows.map(x => Number(x.cohort_cac || 0)), borderColor: '#cf5060', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Avg payback days', data: DATA.cohort_payback_rows.map(x => Number(x.avg_payback_days || 0)), borderColor: '#4766ff', borderDash: [8, 6], tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: paybackOpts,
                }});
            }}
            if (document.getElementById('prodMarginStabilityChart')) {{
                new Chart(document.getElementById('prodMarginStabilityChart'), {{
                    type: 'line',
                    data: {{
                        labels: DATA.daily_margin_rows.map(x => x.date || '-'),
                        datasets: [{{ label: 'Pre-ad margin %', data: DATA.daily_margin_rows.map(x => Number(x.pre_ad_contribution_margin_pct || 0)), borderColor: '#ff8a1f', backgroundColor: 'rgba(255,138,31,.10)', fill: true, tension: .30, borderWidth: 2.3, pointRadius: 0 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('prodParetoLibraryChart')) {{
                const paretoOpts = dualAxisOptions();
                paretoOpts.scales.y1.max = 100;
                new Chart(document.getElementById('prodParetoLibraryChart'), {{
                    data: {{
                        labels: DATA.sku_pareto_rows.map(x => (x.product || 'Unknown').slice(0, 26)),
                        datasets: [
                            {{ type: 'bar', label: 'Contribution', data: DATA.sku_pareto_rows.map(x => Number(x.pre_ad_contribution || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Cum. contribution %', data: DATA.sku_pareto_rows.map(x => Number(x.cum_contribution_pct || 0)), borderColor: '#4766ff', tension: .28, borderWidth: 2.3, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: paretoOpts,
                }});
            }}
            if (document.getElementById('prodAttachRateLibraryChart')) {{
                new Chart(document.getElementById('prodAttachRateLibraryChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.attach_rate_rows.map(x => `${{(x.anchor_item || '').slice(0, 14)}} -> ${{(x.attached_item || '').slice(0, 14)}}`),
                        datasets: [{{ label: 'Attach rate %', data: DATA.attach_rate_rows.map(x => Number(x.attach_rate_pct || 0)), backgroundColor: 'rgba(71,102,255,.72)', borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('prodBundleAccessoryAttachChart')) {{
                new Chart(document.getElementById('prodBundleAccessoryAttachChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.bundle_accessory.pair_rows.map(x => `${{(x.anchor_group_label || '').slice(0, 18)}} -> ${{(x.accessory_group_label || '').slice(0, 16)}}`),
                        datasets: [{{ label: 'Attach rate %', data: DATA.bundle_accessory.pair_rows.map(x => Number(x.attach_rate_pct || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('prodBundleAccessoryUpliftChart')) {{
                new Chart(document.getElementById('prodBundleAccessoryUpliftChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.bundle_accessory.pair_rows.map(x => `${{(x.anchor_group_label || '').slice(0, 18)}} -> ${{(x.accessory_group_label || '').slice(0, 16)}}`),
                        datasets: [{{ label: 'Contribution uplift / order', data: DATA.bundle_accessory.pair_rows.map(x => Number(x.contribution_uplift_per_order || 0)), backgroundColor: 'rgba(31,157,102,.72)', borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('prodBundleAccessoryFamilyChart')) {{
                const familyOpts = dualAxisOptions();
                new Chart(document.getElementById('prodBundleAccessoryFamilyChart'), {{
                    data: {{
                        labels: DATA.bundle_accessory.device_rows.map(x => x.anchor_group_label || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Anchor orders', data: DATA.bundle_accessory.device_rows.map(x => Number(x.anchor_orders || 0)), backgroundColor: 'rgba(71,102,255,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Best attach rate %', data: DATA.bundle_accessory.device_rows.map(x => Number(x.best_attach_rate_pct || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Best uplift / order', data: DATA.bundle_accessory.device_rows.map(x => Number(x.best_contribution_uplift_per_order || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: familyOpts,
                }});
            }}
            if (document.getElementById('prodBundleAccessoryGroupChart')) {{
                const groupOpts = dualAxisOptions();
                new Chart(document.getElementById('prodBundleAccessoryGroupChart'), {{
                    data: {{
                        labels: DATA.bundle_accessory.group_rows.map(x => x.accessory_group_label || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Attached orders', data: DATA.bundle_accessory.group_rows.map(x => Number(x.attached_orders_total || 0)), backgroundColor: 'rgba(71,102,255,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Weighted attach rate %', data: DATA.bundle_accessory.group_rows.map(x => Number(x.weighted_attach_rate_pct || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Avg uplift / order', data: DATA.bundle_accessory.group_rows.map(x => Number(x.avg_contribution_uplift_per_order || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: groupOpts,
                }});
            }}
            if (document.getElementById('prodScentRefillChart')) {{
                const scentOpts = dualAxisOptions();
                new Chart(document.getElementById('prodScentRefillChart'), {{
                    data: {{
                        labels: DATA.scent_size.same_rows.map(x => (x.scent_label || '-').slice(0, 20)),
                        datasets: [
                            {{ type: 'bar', label: 'Sample -> 200ml %', data: DATA.scent_size.same_rows.map(x => Number(x.sample_to_200_pct || 0)), backgroundColor: 'rgba(255,138,31,.66)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'bar', label: 'Sample -> 500ml %', data: DATA.scent_size.same_rows.map(x => Number(x.sample_to_500_pct || 0)), backgroundColor: 'rgba(71,102,255,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: '200ml -> 500ml %', data: DATA.scent_size.same_rows.map(x => Number(x['200_to_500_pct'] || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: '500ml repeat %', data: DATA.scent_size.same_rows.map(x => Number(x['500_repeat_pct'] || 0)), borderColor: '#8b5cf6', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: scentOpts,
                }});
            }}
            if (document.getElementById('prodScentMigrationChart')) {{
                const migrationOpts = dualAxisOptions();
                new Chart(document.getElementById('prodScentMigrationChart'), {{
                    data: {{
                        labels: DATA.scent_size.migration_rows.map(x => (x.base_scent_label || '-').slice(0, 20)),
                        datasets: [
                            {{ type: 'bar', label: 'Cross-scent %', data: DATA.scent_size.migration_rows.map(x => Number(x.cross_scent_pct || 0)), backgroundColor: 'rgba(255,138,31,.66)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Avg days', data: DATA.scent_size.migration_rows.map(x => Number(x.avg_days_to_cross_scent || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: migrationOpts,
                }});
            }}
            if (document.getElementById('prodBundleRecommenderChart')) {{
                const recOpts = dualAxisOptions();
                new Chart(document.getElementById('prodBundleRecommenderChart'), {{
                    data: {{
                        labels: DATA.bundle_recommender.recommendation_rows.map(x => `${{(x.anchor_family_label || '').slice(0, 14)}} -> ${{(x.attached_family_label || '').slice(0, 14)}}`),
                        datasets: [
                            {{ type: 'bar', label: 'Recommendation score', data: DATA.bundle_recommender.recommendation_rows.map(x => Number(x.recommendation_score || 0)), backgroundColor: 'rgba(31,157,102,.60)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CM2 uplift / order', data: DATA.bundle_recommender.recommendation_rows.map(x => Number(x.cm2_uplift_per_order || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Attach rate %', data: DATA.bundle_recommender.recommendation_rows.map(x => Number(x.attach_rate_pct || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: recOpts,
                }});
            }}
            if (document.getElementById('prodBundleAnchorChart')) {{
                const anchorOpts = dualAxisOptions();
                new Chart(document.getElementById('prodBundleAnchorChart'), {{
                    data: {{
                        labels: DATA.bundle_recommender.anchor_rows.map(x => x.anchor_family_label || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Anchor orders', data: DATA.bundle_recommender.anchor_rows.map(x => Number(x.anchor_orders || 0)), backgroundColor: 'rgba(71,102,255,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Top attach rate %', data: DATA.bundle_recommender.anchor_rows.map(x => Number(x.top_attach_rate_pct || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Top CM2 uplift', data: DATA.bundle_recommender.anchor_rows.map(x => Number(x.top_cm2_uplift_per_order || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: anchorOpts,
                }});
            }}
            if (document.getElementById('opsPromoDiscountChart')) {{
                const promoOpts = dualAxisOptions();
                new Chart(document.getElementById('opsPromoDiscountChart'), {{
                    data: {{
                        labels: DATA.promo_discount.bucket_rows.map(x => x.bucket || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Orders', data: DATA.promo_discount.bucket_rows.map(x => Number(x.orders || 0)), backgroundColor: 'rgba(71,102,255,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CM2 margin %', data: DATA.promo_discount.bucket_rows.map(x => Number(x.cm2_margin_pct || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Avg discount / order', data: DATA.promo_discount.bucket_rows.map(x => Number(x.avg_discount_per_order || 0)), borderColor: '#ff8a1f', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: promoOpts,
                }});
            }}
            if (document.getElementById('opsB2bRevenueProfitChart')) {{
                const b2bOpts = dualAxisOptions();
                new Chart(document.getElementById('opsB2bRevenueProfitChart'), {{
                    data: {{
                        labels: DATA.b2b_rows.map(x => x.customer_type || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Revenue', data: DATA.b2b_rows.map(x => Number(x.revenue || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Profit', data: DATA.b2b_rows.map(x => Number(x.profit || 0)), borderColor: '#1f9d66', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y' }},
                            {{ type: 'line', label: 'AOV', data: DATA.b2b_rows.map(x => Number(x.aov || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.0, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: b2bOpts,
                }});
            }}
            if (document.getElementById('opsStatusRevenueChart')) {{
                const statusOpts = dualAxisOptions();
                new Chart(document.getElementById('opsStatusRevenueChart'), {{
                    data: {{
                        labels: DATA.order_status_rows.map(x => x.status || '-'),
                        datasets: [
                            {{ type: 'bar', label: 'Orders', data: DATA.order_status_rows.map(x => Number(x.orders || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Revenue', data: DATA.order_status_rows.map(x => Number(x.revenue || 0)), borderColor: '#4766ff', tension: .30, borderWidth: 2.2, pointRadius: 3, yAxisID: 'y1' }},
                        ],
                    }},
                    options: statusOpts,
                }});
            }}
            if (document.getElementById('opsSegmentPriorityChart')) {{
                new Chart(document.getElementById('opsSegmentPriorityChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.segment_rows.map(x => x.segment || '-'),
                        datasets: [{{ label: 'Customers', data: DATA.segment_rows.map(x => Number(x.count || 0)), backgroundColor: DATA.segment_rows.map(x => Number(x.priority || 9) <= 2 ? 'rgba(207,80,96,.72)' : Number(x.priority || 9) <= 4 ? 'rgba(255,138,31,.72)' : 'rgba(71,102,255,.72)'), borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('opsConsistencyChart')) {{
                const consistencyRows = [
                    {{ label: 'ROAS Delta', value: Number(DATA.consistency.roas_delta || 0) }},
                    {{ label: 'Margin Delta', value: Number(DATA.consistency.margin_delta || 0) }},
                    {{ label: 'CAC Delta', value: Number(DATA.consistency.cac_delta || 0) }},
                ];
                new Chart(document.getElementById('opsConsistencyChart'), {{
                    type: 'bar',
                    data: {{
                        labels: consistencyRows.map(x => x.label),
                        datasets: [{{ label: 'Delta', data: consistencyRows.map(x => x.value), backgroundColor: consistencyRows.map(x => Math.abs(x.value) <= 0.05 ? 'rgba(31,157,102,.72)' : Math.abs(x.value) <= 0.15 ? 'rgba(255,138,31,.72)' : 'rgba(207,80,96,.72)'), borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
        }}

        function alignSeries(sourceLabels, sourceValues, targetLabels) {{
            const source = Array.isArray(sourceLabels) ? sourceLabels : [];
            const values = Array.isArray(sourceValues) ? sourceValues : [];
            const targets = Array.isArray(targetLabels) ? targetLabels : [];
            const map = new Map(source.map((label, idx) => [String(label), Number(values[idx] || 0)]));
            return targets.map(label => Number(map.get(String(label)) || 0));
        }}

        function initNavigation() {{
            const navLinks = Array.from(document.querySelectorAll('.nav-link'));
            const periodLinks = Array.from(document.querySelectorAll('.global-period-link'));
            const sections = Array.from(document.querySelectorAll('section[id]'));
            if (!navLinks.length || !sections.length) return;
            let canonicalBaseHrefs = Object.assign({{}}, STORED_PERIOD_BASE_HREFS);
            if (!Object.keys(canonicalBaseHrefs).length && periodLinks.length) {{
                periodLinks.forEach(link => {{
                    const periodKey = link.dataset.periodKey || '';
                    const baseHref = link.dataset.baseHref || (link.getAttribute('href') || '').split('#')[0];
                    if (periodKey && baseHref) canonicalBaseHrefs[periodKey] = baseHref;
                }});
            }}
            try {{
                if (Object.keys(canonicalBaseHrefs).length) {{
                    sessionStorage.setItem('periodHrefBaseMap', JSON.stringify(canonicalBaseHrefs));
                }}
                if (Object.keys(EMBEDDED_PERIOD_REPORTS).length) {{
                    sessionStorage.setItem('embeddedPeriodReports', JSON.stringify(EMBEDDED_PERIOD_REPORTS));
                }}
            }} catch (_error) {{
                // best-effort only; keep file usable even when browser storage is unavailable
            }}

            function updatePeriodLinks(sectionId) {{
                if (!periodLinks.length || !sectionId) return;
                periodLinks.forEach(link => {{
                    const periodKey = link.dataset.periodKey || '';
                    const baseHref = canonicalBaseHrefs[periodKey] || link.dataset.baseHref || (link.getAttribute('href') || '').split('#')[0];
                    if (!baseHref) return;
                    link.dataset.baseHref = baseHref;
                    link.setAttribute('href', `${{baseHref}}#${{sectionId}}`);
                }});
            }}

            function setActiveNav(sectionId) {{
                navLinks.forEach(link => {{
                    const active = (link.getAttribute('href') || '') === `#${{sectionId}}`;
                    link.classList.toggle('active', active);
                }});
                updatePeriodLinks(sectionId);
            }}

            function resolveActiveSection() {{
                const probe = window.scrollY + 180;
                let current = sections[0].id;
                sections.forEach(section => {{
                    if (section.offsetTop <= probe) current = section.id;
                }});
                setActiveNav(current);
            }}

            navLinks.forEach(link => {{
                link.addEventListener('click', (event) => {{
                    const href = link.getAttribute('href') || '';
                    if (!href.startsWith('#')) return;
                    const target = document.querySelector(href);
                    if (!target) return;
                    event.preventDefault();
                    const top = Math.max(target.offsetTop - 24, 0);
                    window.scrollTo({{ top, behavior: 'smooth' }});
                    history.replaceState(null, '', href);
                    setActiveNav(target.id);
                }});
            }});

            periodLinks.forEach(link => {{
                link.addEventListener('click', (event) => {{
                    const periodKey = link.dataset.periodKey || '';
                    const sectionId = (document.querySelector('.nav-link.active')?.getAttribute('href') || '#overview').replace(/^#/, '') || 'overview';
                    const baseHref = canonicalBaseHrefs[periodKey] || link.dataset.baseHref || (link.getAttribute('href') || '').split('#')[0];
                    if (!periodKey) return;

                    if (periodKey !== 'full' && EMBEDDED_PERIOD_REPORTS[periodKey]) {{
                        event.preventDefault();
                        try {{
                            sessionStorage.setItem('periodHrefBaseMap', JSON.stringify(canonicalBaseHrefs));
                            sessionStorage.setItem('embeddedPeriodReports', JSON.stringify(EMBEDDED_PERIOD_REPORTS));
                            sessionStorage.setItem('pendingSectionId', sectionId);
                        }} catch (_error) {{
                            // continue without persistence
                        }}
                        const binary = atob(String(EMBEDDED_PERIOD_REPORTS[periodKey]));
                        const bytes = Uint8Array.from(binary, ch => ch.charCodeAt(0));
                        const bootstrapScript = `<script>window.__EMBEDDED_PERIOD_REPORTS__ = ${{
                            JSON.stringify(EMBEDDED_PERIOD_REPORTS).replace(/</g, '\\\\u003c')
                        }};window.__PERIOD_HREF_BASE_MAP__ = ${{
                            JSON.stringify(canonicalBaseHrefs).replace(/</g, '\\\\u003c')
                        }};window.__PENDING_SECTION_ID__ = ${{
                            JSON.stringify(sectionId).replace(/</g, '\\\\u003c')
                        }};<\\/script>`;
                        const html = new TextDecoder('utf-8').decode(bytes).replace('</head>', `${{bootstrapScript}}</head>`);
                        document.open();
                        document.write(html);
                        document.close();
                        return;
                    }}

                    if (periodKey === 'full' && baseHref) {{
                        event.preventDefault();
                        try {{
                            sessionStorage.setItem('pendingSectionId', sectionId);
                        }} catch (_error) {{
                            // ignore
                        }}
                        window.location.href = `${{baseHref}}#${{sectionId}}`;
                    }}
                }});
            }});

            let ticking = false;
            window.addEventListener('scroll', () => {{
                if (ticking) return;
                ticking = true;
                window.requestAnimationFrame(() => {{
                    resolveActiveSection();
                    ticking = false;
                }});
            }}, {{ passive: true }});

            resolveActiveSection();
            const pendingSectionId = BOOTSTRAP_PENDING_SECTION_ID || sessionStorage.getItem('pendingSectionId');
            if (pendingSectionId) {{
                if (!BOOTSTRAP_PENDING_SECTION_ID) sessionStorage.removeItem('pendingSectionId');
                const pendingTarget = document.getElementById(pendingSectionId);
                if (pendingTarget) {{
                    const top = Math.max(pendingTarget.offsetTop - 24, 0);
                    window.scrollTo({{ top, behavior: 'auto' }});
                    setActiveNav(pendingSectionId);
                }}
            }}
            if (window.location.hash) {{
                const initialTarget = document.querySelector(window.location.hash);
                if (initialTarget) setActiveNav(initialTarget.id);
            }}
        }}

        function buildStandaloneLibraries() {{
            const s = DATA.series;
            const economicsStandaloneItems = [];
            if (hasSeries(s.dates)) {{
                economicsStandaloneItems.push(
                    {{ id: 'econRevenueTotalCostsChart', title: {{ en: 'Revenue vs total costs', sk: 'Trzba vs celkove naklady' }}, desc: {{ en: 'Direct view of daily revenue against total daily costs.', sk: 'Priamy pohlad na dennu trzbu oproti celkovym dennym nakladom.' }} }},
                    {{ id: 'econTotalCostsStandaloneChart', title: {{ en: 'Total costs', sk: 'Celkove naklady' }}, desc: {{ en: 'Standalone timeline of total daily costs.', sk: 'Samostatna casova os celkovych dennych nakladov.' }} }},
                    {{ id: 'econProductCostsStandaloneChart', title: {{ en: 'Product costs', sk: 'Produktove naklady' }}, desc: {{ en: 'Cost of goods sold through time.', sk: 'Naklady na tovar v case.' }} }},
                    {{ id: 'econGrossMarginStandaloneChart', title: {{ en: 'Gross margin', sk: 'Hruba marza' }}, desc: {{ en: 'Daily gross margin with smoothing.', sk: 'Denne hruba marza s vyhladenim.' }} }},
                    {{ id: 'econPackagingStandaloneChart', title: {{ en: 'Packaging costs', sk: 'Naklady na balenie' }}, desc: {{ en: 'Daily packaging cost load.', sk: 'Denne naklady na balenie.' }} }},
                    {{ id: 'econShippingStandaloneChart', title: {{ en: 'Net shipping', sk: 'Ciste shipping' }}, desc: {{ en: 'Daily net shipping line: positive = cost, negative = shipping profit.', sk: 'Denna krivka cisteho shippingu: kladne = naklad, zaporne = shipping zisk.' }} }},
                    {{ id: 'econFixedStandaloneChart', title: {{ en: 'Fixed costs', sk: 'Fixne naklady' }}, desc: {{ en: 'Daily allocation of fixed overhead.', sk: 'Denne alokovane fixne naklady.' }} }},
                    {{ id: 'econItemsSoldStandaloneChart', title: {{ en: 'Items sold', sk: 'Predane kusy' }}, desc: {{ en: 'Total item units sold per day.', sk: 'Celkovy pocet predanych kusov za den.' }} }},
                    {{ id: 'econAvgItemsStandaloneChart', title: {{ en: 'Average items per order', sk: 'Priemer poloziek na objednavku' }}, desc: {{ en: 'Basket depth by number of sold items.', sk: 'Hl bka kosika podla poctu poloziek.' }} }},
                    {{ id: 'econCostRevenueScatterChart', title: {{ en: 'Revenue vs cost scatter', sk: 'Scatter trzba vs naklady' }}, desc: {{ en: 'How daily revenue scales with daily cost base.', sk: 'Ako sa denna trzba meni oproti dennej nakladovej baze.' }} }},
                    {{ id: 'econAllMetricsOverviewChart', title: {{ en: 'All metrics overview', sk: 'Prehlad vsetkych metrik' }}, desc: {{ en: 'Compressed multi-series operator overview of the economics stack.', sk: 'Kompresny multi-seriovy operatorovy prehlad ekonomickeho stacku.' }} }},
                    {{ id: 'econLtvByAcquisitionStandaloneChart', title: {{ en: 'LTV by acquisition date', sk: 'LTV podla datumu akvizicie' }}, desc: {{ en: 'Daily realized LTV revenue and newly acquired customers.', sk: 'Denn a realizovana LTV trzba a novoziskani zakaznici.' }} }},
                    {{ id: 'econLtvProfitStandaloneChart', title: {{ en: 'LTV-based profit', sk: 'LTV profit' }}, desc: {{ en: 'LTV revenue minus daily total cost.', sk: 'LTV trzba minus denne celkove naklady.' }} }},
                );
            }}
            renderGalleryCards('libraryEconomicsStandalone', economicsStandaloneItems);

            if (document.getElementById('econRevenueTotalCostsChart')) {{
                const opts = dualAxisOptions();
                new Chart(document.getElementById('econRevenueTotalCostsChart'), {{
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ type: 'bar', label: 'Revenue', data: s.revenue, backgroundColor: 'rgba(255,138,31,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Total cost', data: s.total_cost, borderColor: '#cf5060', tension: .30, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: opts,
                }});
            }}
            if (document.getElementById('econTotalCostsStandaloneChart')) {{
                new Chart(document.getElementById('econTotalCostsStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [{{ label: 'Total cost', data: s.total_cost, borderColor: '#cf5060', backgroundColor: (ctx) => gradient(ctx, 'rgba(207,80,96,.18)', 'rgba(207,80,96,.02)'), fill: true, tension: .32, borderWidth: 2.5, pointRadius: 0 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econProductCostsStandaloneChart')) {{
                new Chart(document.getElementById('econProductCostsStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: s.dates,
                        datasets: [{{ label: 'Product cost', data: s.product_cost, backgroundColor: 'rgba(255,138,31,.66)', borderRadius: 8 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econGrossMarginStandaloneChart')) {{
                new Chart(document.getElementById('econGrossMarginStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ label: 'Gross margin %', data: s.gross_margin, borderColor: '#ff8a1f', tension: .32, borderWidth: 2.4, pointRadius: 0 }},
                            {{ label: 'Gross margin 7d MA', data: s.gross_margin_ma7, borderColor: '#d95c00', borderDash: [8, 6], tension: .32, borderWidth: 2.0, pointRadius: 0 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econPackagingStandaloneChart')) {{
                new Chart(document.getElementById('econPackagingStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: s.dates,
                        datasets: [{{ label: 'Packaging', data: s.packaging, backgroundColor: 'rgba(255,177,93,.72)', borderRadius: 8 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econShippingStandaloneChart')) {{
                new Chart(document.getElementById('econShippingStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [{{ label: 'Net shipping', data: s.shipping, borderColor: '#4766ff', tension: .32, borderWidth: 2.3, pointRadius: 0 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econFixedStandaloneChart')) {{
                new Chart(document.getElementById('econFixedStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: s.dates,
                        datasets: [{{ label: 'Fixed', data: s.fixed, backgroundColor: 'rgba(207,80,96,.58)', borderRadius: 8 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econItemsSoldStandaloneChart')) {{
                new Chart(document.getElementById('econItemsSoldStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: s.dates,
                        datasets: [{{ label: 'Items sold', data: s.items, backgroundColor: 'rgba(71,102,255,.62)', borderRadius: 8 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econAvgItemsStandaloneChart')) {{
                new Chart(document.getElementById('econAvgItemsStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ label: 'Avg items/order', data: s.avg_items, borderColor: '#4766ff', tension: .32, borderWidth: 2.3, pointRadius: 0 }},
                            {{ label: '7d MA', data: s.avg_items_ma7, borderColor: '#1b46e5', borderDash: [8, 6], tension: .32, borderWidth: 2.0, pointRadius: 0 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econCostRevenueScatterChart')) {{
                const scatterOpts = baseOptions();
                scatterOpts.scales.x = {{ type: 'linear', grid: {{ color: 'rgba(138,129,120,.10)', drawBorder: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                scatterOpts.scales.y = {{ grid: {{ color: 'rgba(138,129,120,.10)', drawBorder: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
                new Chart(document.getElementById('econCostRevenueScatterChart'), {{
                    type: 'scatter',
                    data: {{
                        datasets: [{{ label: 'Daily revenue vs cost', data: s.total_cost.map((cost, idx) => ({{ x: Number(cost || 0), y: Number(s.revenue[idx] || 0) }})), backgroundColor: 'rgba(255,138,31,.58)', borderColor: '#ff8a1f', pointRadius: 4 }}],
                    }},
                    options: scatterOpts,
                }});
            }}
            if (document.getElementById('econAllMetricsOverviewChart')) {{
                new Chart(document.getElementById('econAllMetricsOverviewChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ label: 'Revenue', data: s.revenue, borderColor: '#ff8a1f', tension: .28, borderWidth: 2.2, pointRadius: 0 }},
                            {{ label: 'Total cost', data: s.total_cost, borderColor: '#cf5060', tension: .28, borderWidth: 2.0, pointRadius: 0 }},
                            {{ label: 'Product cost', data: s.product_cost, borderColor: '#b35d00', tension: .28, borderWidth: 1.8, pointRadius: 0 }},
                            {{ label: 'FB ads', data: s.fb_ads, borderColor: '#4766ff', tension: .28, borderWidth: 1.8, pointRadius: 0 }},
                            {{ label: 'Google ads', data: s.google_ads, borderColor: '#8b5cf6', tension: .28, borderWidth: 1.8, pointRadius: 0 }},
                            {{ label: 'Profit', data: s.profit, borderColor: '#1f9d66', tension: .28, borderWidth: 2.0, pointRadius: 0 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('econLtvByAcquisitionStandaloneChart')) {{
                const ltvOpts = dualAxisOptions();
                new Chart(document.getElementById('econLtvByAcquisitionStandaloneChart'), {{
                    data: {{
                        labels: DATA.ltv.labels,
                        datasets: [
                            {{ type: 'line', label: 'LTV revenue', data: DATA.ltv.ltv_revenue, borderColor: '#8b5cf6', tension: .32, borderWidth: 2.4, pointRadius: 0, yAxisID: 'y' }},
                            {{ type: 'bar', label: 'Customers acquired', data: DATA.ltv.customers_acquired || [], backgroundColor: 'rgba(71,102,255,.36)', borderRadius: 6, yAxisID: 'y1' }},
                            {{ type: 'line', label: 'Lifetime orders', data: DATA.ltv.lifetime_orders || [], borderColor: '#ff8a1f', tension: .30, borderWidth: 2.0, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: ltvOpts,
                }});
            }}
            if (document.getElementById('econLtvProfitStandaloneChart')) {{
                const alignedLtvRevenue = alignSeries(DATA.ltv.labels, DATA.ltv.ltv_revenue, s.dates);
                const ltvProfitSeries = alignedLtvRevenue.map((value, idx) => Number(value || 0) - Number(s.total_cost[idx] || 0));
                new Chart(document.getElementById('econLtvProfitStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: s.dates,
                        datasets: [{{ label: 'LTV profit', data: ltvProfitSeries, borderColor: '#8b5cf6', backgroundColor: (ctx) => gradient(ctx, 'rgba(139,92,246,.16)', 'rgba(139,92,246,.02)'), fill: true, tension: .32, borderWidth: 2.5, pointRadius: 0 }}],
                    }},
                    options: baseOptions(),
                }});
            }}

            const marketingStandaloneItems = [];
            if (hasSeries(DATA.fb_daily.dates)) {{
                marketingStandaloneItems.push(
                    {{ id: 'mktFbSpendStandaloneChart', title: {{ en: 'Facebook ads spend', sk: 'Facebook ads spend' }}, desc: {{ en: 'Standalone daily Meta spend.', sk: 'Samostatny denny Meta spend.' }} }},
                    {{ id: 'mktFbSpendClicksStandaloneChart', title: {{ en: 'Spend vs clicks', sk: 'Spend vs kliky' }}, desc: {{ en: 'Meta spend against delivered clicks.', sk: 'Meta spend oproti dorucenym klikom.' }} }},
                );
            }}
            if (hasSeries(s.dates)) {{
                marketingStandaloneItems.push(
                    {{ id: 'mktGoogleSpendStandaloneChart', title: {{ en: 'Google ads spend', sk: 'Google ads spend' }}, desc: {{ en: 'Standalone daily Google spend.', sk: 'Samostatny denny Google spend.' }} }},
                    {{ id: 'mktAdsComparisonStandaloneChart', title: {{ en: 'FB vs Google spend', sk: 'FB vs Google spend' }}, desc: {{ en: 'Daily paid media comparison.', sk: 'Denne porovnanie platenych kanalov.' }} }},
                );
            }}
            if (hasRows(DATA.fb_campaign_rows)) {{
                marketingStandaloneItems.push(
                    {{ id: 'mktCampaignConvRateStandaloneChart', title: {{ en: 'Campaign conversion rate', sk: 'Konverzny pomer kampani' }}, desc: {{ en: 'Conversions divided by clicks by campaign.', sk: 'Konverzie delene klikmi podla kampane.' }} }},
                    {{ id: 'mktCampaignCostPerConvStandaloneChart', title: {{ en: 'Campaign cost per conversion', sk: 'Naklad na konverziu kampani' }}, desc: {{ en: 'Cost per conversion by campaign.', sk: 'Naklad na konverziu podla kampane.' }} }},
                    {{ id: 'mktCampaignCtrStandaloneChart', title: {{ en: 'Campaign CTR', sk: 'CTR kampani' }}, desc: {{ en: 'CTR comparison by campaign.', sk: 'Porovnanie CTR podla kampani.' }} }},
                    {{ id: 'mktCampaignCpcStandaloneChart', title: {{ en: 'Campaign CPC', sk: 'CPC kampani' }}, desc: {{ en: 'CPC comparison by campaign.', sk: 'Porovnanie CPC podla kampani.' }} }},
                    {{ id: 'mktCampaignSpendPieStandaloneChart', title: {{ en: 'Campaign spend share', sk: 'Podiel spendu kampani' }}, desc: {{ en: 'Share of Meta spend by campaign.', sk: 'Podiel Meta spendu podla kampani.' }} }},
                );
            }}
            if (hasRows(DATA.campaign_cpo)) {{
                marketingStandaloneItems.push(
                    {{ id: 'mktCampaignCpoStandaloneChart', title: {{ en: 'Campaign CPO', sk: 'CPO kampani' }}, desc: {{ en: 'Estimated CPO by campaign.', sk: 'Odhadovane CPO podla kampani.' }} }},
                    {{ id: 'mktCampaignRoasStandaloneChart', title: {{ en: 'Campaign ROAS', sk: 'ROAS kampani' }}, desc: {{ en: 'Estimated ROAS by campaign.', sk: 'Odhadovane ROAS podla kampani.' }} }},
                );
            }}
            if (hasRows(DATA.spend_effectiveness_rows)) {{
                marketingStandaloneItems.push(
                    {{ id: 'mktSpendRangeOrdersStandaloneChart', title: {{ en: 'Spend bucket orders', sk: 'Objednavky podla spend bucketu' }}, desc: {{ en: 'Average order count by daily spend band.', sk: 'Priemerny pocet objednavok podla denneho spend pasma.' }} }},
                );
            }}
            renderGalleryCards('libraryMarketingStandalone', marketingStandaloneItems);

            if (document.getElementById('mktFbSpendStandaloneChart')) {{
                new Chart(document.getElementById('mktFbSpendStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.fb_daily.dates,
                        datasets: [{{ label: 'FB spend', data: DATA.fb_daily.spend, backgroundColor: 'rgba(255,138,31,.66)', borderRadius: 8 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('mktGoogleSpendStandaloneChart')) {{
                new Chart(document.getElementById('mktGoogleSpendStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: s.dates,
                        datasets: [{{ label: 'Google spend', data: s.google_ads, backgroundColor: 'rgba(71,102,255,.64)', borderRadius: 8 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('mktAdsComparisonStandaloneChart')) {{
                new Chart(document.getElementById('mktAdsComparisonStandaloneChart'), {{
                    data: {{
                        labels: s.dates,
                        datasets: [
                            {{ type: 'bar', label: 'FB ads', data: s.fb_ads, backgroundColor: 'rgba(255,138,31,.62)', borderRadius: 8 }},
                            {{ type: 'bar', label: 'Google ads', data: s.google_ads, backgroundColor: 'rgba(71,102,255,.56)', borderRadius: 8 }},
                        ],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('mktFbSpendClicksStandaloneChart')) {{
                const opts = dualAxisOptions();
                new Chart(document.getElementById('mktFbSpendClicksStandaloneChart'), {{
                    data: {{
                        labels: DATA.fb_daily.dates,
                        datasets: [
                            {{ type: 'bar', label: 'FB spend', data: DATA.fb_daily.spend, backgroundColor: 'rgba(255,138,31,.58)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'Clicks', data: DATA.fb_daily.clicks, borderColor: '#4766ff', tension: .32, borderWidth: 2.2, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: opts,
                }});
            }}
            if (document.getElementById('mktCampaignConvRateStandaloneChart')) {{
                const labels = DATA.fb_campaign_rows.map(x => (x.campaign_name || 'Unknown').slice(0, 24));
                const convRate = DATA.fb_campaign_rows.map(x => {{
                    const clicks = Number(x.clicks || 0);
                    const conversions = Number(x.platform_conversions || x.conversions || 0);
                    return clicks > 0 ? (conversions / clicks) * 100 : 0;
                }});
                new Chart(document.getElementById('mktCampaignConvRateStandaloneChart'), {{
                    type: 'bar',
                    data: {{ labels, datasets: [{{ label: 'Conversion rate %', data: convRate, backgroundColor: 'rgba(31,157,102,.68)', borderRadius: 8 }}] }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('mktCampaignCostPerConvStandaloneChart')) {{
                new Chart(document.getElementById('mktCampaignCostPerConvStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.fb_campaign_rows.map(x => (x.campaign_name || 'Unknown').slice(0, 24)),
                        datasets: [{{ label: 'Cost / platform conversion', data: DATA.fb_campaign_rows.map(x => Number(x.cost_per_platform_conversion || x.cost_per_conversion || 0)), backgroundColor: 'rgba(207,80,96,.68)', borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('mktCampaignCtrStandaloneChart')) {{
                new Chart(document.getElementById('mktCampaignCtrStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.fb_campaign_rows.map(x => (x.campaign_name || 'Unknown').slice(0, 24)),
                        datasets: [{{ label: 'CTR %', data: DATA.fb_campaign_rows.map(x => Number(x.ctr || 0)), backgroundColor: 'rgba(71,102,255,.68)', borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('mktCampaignCpcStandaloneChart')) {{
                new Chart(document.getElementById('mktCampaignCpcStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.fb_campaign_rows.map(x => (x.campaign_name || 'Unknown').slice(0, 24)),
                        datasets: [{{ label: 'CPC', data: DATA.fb_campaign_rows.map(x => Number(x.cpc || 0)), backgroundColor: 'rgba(255,138,31,.68)', borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('mktCampaignCpoStandaloneChart')) {{
                new Chart(document.getElementById('mktCampaignCpoStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.campaign_cpo.map(x => (x.campaign_name || 'Unknown').slice(0, 24)),
                        datasets: [{{ label: 'Estimated CPO', data: DATA.campaign_cpo.map(x => Number(x.estimated_cpo || 0)), backgroundColor: 'rgba(207,80,96,.68)', borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('mktCampaignRoasStandaloneChart')) {{
                new Chart(document.getElementById('mktCampaignRoasStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.campaign_cpo.map(x => (x.campaign_name || 'Unknown').slice(0, 24)),
                        datasets: [{{ label: 'Estimated ROAS', data: DATA.campaign_cpo.map(x => Number(x.estimated_roas || 0)), backgroundColor: 'rgba(31,157,102,.68)', borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}
            if (document.getElementById('mktCampaignSpendPieStandaloneChart')) {{
                new Chart(document.getElementById('mktCampaignSpendPieStandaloneChart'), {{
                    type: 'doughnut',
                    data: {{
                        labels: DATA.fb_campaign_rows.map(x => (x.campaign_name || 'Unknown').slice(0, 22)),
                        datasets: [{{ data: DATA.fb_campaign_rows.map(x => Number(x.spend || 0)), backgroundColor: ['#ff8a1f', '#4766ff', '#1f9d66', '#8b5cf6', '#cf5060', '#ffb96d', '#7c8cff', '#6ac89a', '#b994ff', '#ff8fa0', '#ffc97e', '#98a6ff'], borderColor: '#fff9f3', borderWidth: 4, hoverOffset: 8 }}],
                    }},
                    options: doughnutOptions(),
                }});
            }}
            if (document.getElementById('mktSpendRangeOrdersStandaloneChart')) {{
                new Chart(document.getElementById('mktSpendRangeOrdersStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.spend_effectiveness_rows.map(x => x.spend_range || '-'),
                        datasets: [{{ label: 'Avg orders', data: DATA.spend_effectiveness_rows.map(x => Number(x.avg_orders || 0)), backgroundColor: 'rgba(255,138,31,.68)', borderRadius: 8 }}],
                    }},
                    options: horizontalBarOptions(),
                }});
            }}

            const customerStandaloneItems = [];
            if (hasSeries(DATA.refunds.dates)) {{
                customerStandaloneItems.push({{ id: 'custRefundAmountStandaloneChart', title: {{ en: 'Refund amount', sk: 'Refund amount' }}, desc: {{ en: 'Standalone refunded amount timeline.', sk: 'Samostatna casova os refundovanej sumy.' }} }});
            }}
            if (hasSeries(DATA.clv.labels)) {{
                customerStandaloneItems.push(
                    {{ id: 'custClvStandaloneChart', title: {{ en: 'CLV', sk: 'CLV' }}, desc: {{ en: 'Standalone weekly CLV.', sk: 'Samostatne tyzdenne CLV.' }} }},
                    {{ id: 'custCacStandaloneChart', title: {{ en: 'CAC', sk: 'CAC' }}, desc: {{ en: 'Standalone weekly CAC.', sk: 'Samostatne tyzdenne CAC.' }} }},
                    {{ id: 'custClvCacComparisonStandaloneChart', title: {{ en: 'CLV vs CAC comparison', sk: 'Porovnanie CLV vs CAC' }}, desc: {{ en: 'Direct value comparison between CLV and CAC.', sk: 'Priame porovnanie hodnot CLV a CAC.' }} }},
                    {{ id: 'custLtvCacRatioStandaloneChart', title: {{ en: 'LTV/CAC ratio', sk: 'Pomer LTV/CAC' }}, desc: {{ en: 'Standalone LTV/CAC ratio view.', sk: 'Samostatny pohlad na pomer LTV/CAC.' }} }},
                    {{ id: 'custReturnTimeStandaloneChart', title: {{ en: 'Return time', sk: 'Cas navratu' }}, desc: {{ en: 'Average return time in days.', sk: 'Priemerny cas navratu v dnoch.' }} }},
                );
            }}
            if (hasSeries(DATA.financial.payback_weekly_labels)) {{
                customerStandaloneItems.push({{ id: 'custPaybackStandaloneChart', title: {{ en: 'Payback trend', sk: 'Payback trend' }}, desc: {{ en: 'Weekly estimated payback period in orders.', sk: 'Tyzdenny odhad payback periody v objednavkach.' }} }});
            }}
            renderGalleryCards('libraryCustomersStandalone', customerStandaloneItems);

            if (document.getElementById('custRefundAmountStandaloneChart')) {{
                new Chart(document.getElementById('custRefundAmountStandaloneChart'), {{
                    type: 'bar',
                    data: {{
                        labels: DATA.refunds.dates,
                        datasets: [{{ label: 'Refund amount', data: DATA.refunds.amount, backgroundColor: 'rgba(138,44,61,.52)', borderRadius: 8 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custClvStandaloneChart')) {{
                new Chart(document.getElementById('custClvStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: DATA.clv.labels,
                        datasets: [{{ label: 'Avg CLV', data: DATA.clv.avg_clv, borderColor: '#8b5cf6', tension: .32, borderWidth: 2.4, pointRadius: 0 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custCacStandaloneChart')) {{
                new Chart(document.getElementById('custCacStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: DATA.clv.labels,
                        datasets: [{{ label: 'CAC', data: DATA.clv.cac, borderColor: '#cf5060', tension: .32, borderWidth: 2.4, pointRadius: 0 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custClvCacComparisonStandaloneChart')) {{
                const opts = dualAxisOptions();
                new Chart(document.getElementById('custClvCacComparisonStandaloneChart'), {{
                    data: {{
                        labels: DATA.clv.labels,
                        datasets: [
                            {{ type: 'bar', label: 'Avg CLV', data: DATA.clv.avg_clv, backgroundColor: 'rgba(139,92,246,.56)', borderRadius: 8, yAxisID: 'y' }},
                            {{ type: 'line', label: 'CAC', data: DATA.clv.cac, borderColor: '#cf5060', tension: .32, borderWidth: 2.3, pointRadius: 0, yAxisID: 'y1' }},
                        ],
                    }},
                    options: opts,
                }});
            }}
            if (document.getElementById('custLtvCacRatioStandaloneChart')) {{
                new Chart(document.getElementById('custLtvCacRatioStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: DATA.clv.labels,
                        datasets: [{{ label: 'LTV/CAC', data: DATA.clv.ltv_cac_ratio, borderColor: '#ff8a1f', tension: .32, borderWidth: 2.4, pointRadius: 0 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custReturnTimeStandaloneChart')) {{
                new Chart(document.getElementById('custReturnTimeStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: DATA.clv.labels,
                        datasets: [{{ label: 'Avg return days', data: DATA.clv.avg_return_time_days, borderColor: '#4766ff', tension: .32, borderWidth: 2.4, pointRadius: 0 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
            if (document.getElementById('custPaybackStandaloneChart')) {{
                new Chart(document.getElementById('custPaybackStandaloneChart'), {{
                    type: 'line',
                    data: {{
                        labels: DATA.financial.payback_weekly_labels,
                        datasets: [{{ label: 'Payback orders', data: DATA.financial.payback_weekly_orders, borderColor: '#1f9d66', backgroundColor: (ctx) => gradient(ctx, 'rgba(31,157,102,.16)', 'rgba(31,157,102,.02)'), fill: true, tension: .32, borderWidth: 2.5, pointRadius: 0 }}],
                    }},
                    options: baseOptions(),
                }});
            }}
        }}

        function buildDetailGalleries() {{
            buildLibraryEconomicsMarketing();
            buildLibraryCustomersPatternsProducts();
            buildStandaloneLibraries();
        }}
        document.querySelectorAll('.lang-btn').forEach(btn => btn.addEventListener('click', () => applyLang(btn.dataset.lang)));
        document.querySelectorAll('.window-btn').forEach(btn => btn.addEventListener('click', () => renderKpis(btn.dataset.window)));
        buildCharts();
        buildDetailGalleries();
        initNavigation();
        applyLang(lang());
    </script>
</body>
</html>"""

    return _sanitize_dashboard_html(html)
