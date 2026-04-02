#!/usr/bin/env python3
"""
Isolated test2 dashboard renderer.
"""

from __future__ import annotations

import json
from datetime import datetime
from html import escape
from typing import Any, Dict, List, Optional

import pandas as pd


METRIC_LABELS = {
    "revenue": {"en": "Revenue", "sk": "Trzby"},
    "profit": {"en": "Profit", "sk": "Zisk"},
    "orders": {"en": "Orders", "sk": "Objednávky"},
    "aov": {"en": "AOV", "sk": "Priemerná objednávka"},
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
    total_ads = [round(f + g, 4) for f, g in zip(fb_ads, google_ads)]
    roas = [round((rev / ads) if ads > 0 else 0.0, 4) for rev, ads in zip(revenue, total_ads)]
    pre_margin = [_num(v) for v in date_agg.get("pre_ad_contribution_margin_pct", pd.Series([0] * len(date_agg))).tolist()]
    post_margin = [_num(v) for v in date_agg.get("post_ad_contribution_margin_pct", pd.Series([0] * len(date_agg))).tolist()]
    return {
        "dates": dates,
        "revenue": revenue,
        "profit": profit,
        "orders": orders,
        "aov": aov,
        "roas": roas,
        "pre_margin": pre_margin,
        "post_margin": post_margin,
        "revenue_ma7": _ma(revenue, 7),
        "profit_ma7": _ma(profit, 7),
        "orders_ma7": _ma([float(v) for v in orders], 7),
        "aov_ma7": _ma(aov, 7),
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
        "labels": ["Product", "Packaging", "Shipping", "Facebook Ads", "Google Ads", "Fixed"],
        "values": [
            round(_num(date_agg.get("product_expense", pd.Series(dtype=float)).sum()), 2),
            round(_num(date_agg.get("packaging_cost", pd.Series(dtype=float)).sum()), 2),
            round(_num(date_agg.get("shipping_subsidy_cost", pd.Series(dtype=float)).sum()), 2),
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
    links = []
    for option in options:
        key = str(option.get("key") or "")
        active = "active" if key == current_key else ""
        links.append(f'<a class="pill {active}" href="{escape(str(option.get("href") or "#"))}">{escape(str(option.get("label") or key.upper()))}</a>')
    return '<div class="panel controls"><div class="label"><span class="lang-en">Period</span><span class="lang-sk hidden">Obdobie</span></div><div class="pill-row">' + "".join(links) + '</div></div>'


def _top_rows(frame: Optional[pd.DataFrame], columns: List[str], limit: int = 8) -> List[Dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    rows = []
    for _, row in frame.head(limit).iterrows():
        rows.append({key: row.get(key) for key in columns})
    return rows


def generate_test2_dashboard(
    date_agg: pd.DataFrame,
    items_agg: pd.DataFrame,
    date_from: datetime,
    date_to: datetime,
    report_title: str = "BizniWeb reporting",
    country_analysis: Optional[pd.DataFrame] = None,
    city_analysis: Optional[pd.DataFrame] = None,
    product_margins: Optional[pd.DataFrame] = None,
    new_vs_returning_revenue: Optional[dict] = None,
    refunds_analysis: Optional[dict] = None,
    cfo_kpi_payload: Optional[dict] = None,
    source_health: Optional[dict] = None,
    period_switcher: Optional[dict] = None,
) -> str:
    title = escape((report_title or "BizniWeb reporting").strip())
    series = _series(date_agg)
    kpi_payload = _kpis(cfo_kpi_payload)
    cost_mix = _cost_mix(date_agg)
    cities = _top_rows(city_analysis, ["city", "country", "orders", "revenue", "profit"], limit=8)
    products = _top_rows(
        product_margins.sort_values(["profit", "revenue"], ascending=[False, False]) if product_margins is not None and not product_margins.empty else product_margins,
        ["product", "sku", "orders", "revenue", "profit", "margin_pct"],
        limit=8,
    )
    countries = _top_rows(country_analysis, ["country", "orders", "revenue"], limit=6)
    source_rows = list(((source_health or {}).get("sources") or {}).values())

    customer_daily = (new_vs_returning_revenue or {}).get("daily")
    if customer_daily is not None and not getattr(customer_daily, "empty", True):
        customer_mix = {
            "dates": customer_daily["date"].astype(str).tolist(),
            "new": [round(_num(v), 2) for v in customer_daily["new_revenue"].tolist()],
            "returning": [round(_num(v), 2) for v in customer_daily["returning_revenue"].tolist()],
        }
    else:
        customer_mix = {"dates": [], "new": [], "returning": []}

    total_revenue = round(_num(date_agg["total_revenue"].sum()), 2)
    total_profit = round(_num(date_agg["net_profit"].sum()), 2)
    total_orders = int(round(_num(date_agg["unique_orders"].sum())))
    total_ads = round(_num(date_agg.get("fb_ads_spend", pd.Series(dtype=float)).sum()) + _num(date_agg.get("google_ads_spend", pd.Series(dtype=float)).sum()), 2)
    blended_roas = round((total_revenue / total_ads) if total_ads > 0 else 0.0, 2)
    top_city = str(cities[0].get("city") or "-") if cities else "-"
    top_product = str(products[0].get("product") or "-") if products else "-"
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    payload = {
        "series": series,
        "kpis": kpi_payload,
        "cost_mix": cost_mix,
        "cities": cities,
        "countries": countries,
        "products": products,
        "customer_mix": customer_mix,
        "refund_rate": round(_num((refunds_analysis or {}).get("refund_rate_pct")), 2),
    }
    payload_json = json.dumps(payload, ensure_ascii=False)

    product_rows_html = "".join(
        f"<tr><td>{escape(str(row.get('product') or 'Unknown'))}</td><td>{escape(str(row.get('sku') or ''))}</td><td>€{_num(row.get('revenue')):,.2f}</td><td>€{_num(row.get('profit')):,.2f}</td><td>{_num(row.get('margin_pct')):.1f}%</td><td>{int(round(_num(row.get('orders'))))}</td></tr>"
        for row in products
    ) or '<tr><td colspan="6"><span class="lang-en">No product data available.</span><span class="lang-sk hidden">Produktové dáta nie sú dostupné.</span></td></tr>'

    health_html = "".join(
        f'<div class="health-item"><div class="health-title">{escape(str(row.get("label") or row.get("key") or "Source"))}</div><div class="health-status {("good" if row.get("healthy") else ("warn" if row.get("status") == "degraded" else "bad"))}">{escape(str(row.get("status") or "unknown"))}</div><p>{escape(str(row.get("message") or row.get("mode") or "-"))}</p></div>'
        for row in source_rows
    ) or '<div class="health-item"><div class="health-title"><span class="lang-en">Source health</span><span class="lang-sk hidden">Stav zdrojov</span></div><div class="health-status good">ok</div><p><span class="lang-en">No source warnings attached to this run.</span><span class="lang-sk hidden">K tomuto behu nie sú pripojené žiadne varovania zdrojov.</span></p></div>'

    period_switcher_html = _period_switcher_html(period_switcher)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title} - test2</title>
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
        .section {{ margin-top: 24px; }}
        .section-head h2 {{ margin:0; font-size: 26px; letter-spacing: -.03em; }}
        .section-head p {{ margin: 6px 0 14px; color: var(--muted); line-height: 1.55; max-width: 760px; }}
        .kpi-band {{ padding: 22px; }}
        .kpi-grid {{ display:grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-top: 16px; }}
        .kpi-card {{ padding: 20px; border-radius: 22px; background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(255,245,233,.92)); border:1px solid rgba(255,138,31,.14); min-height: 170px; display:flex; flex-direction:column; }}
        .kpi-card small {{ color: var(--muted); font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing:.08em; }}
        .kpi-value {{ font-size: 36px; font-weight: 900; line-height: 1; letter-spacing: -.05em; margin: 10px 0 6px; }}
        .kpi-period {{ color: var(--muted); font-size: 12px; font-weight: 700; }}
        .compare-list {{ margin-top: auto; display:grid; gap:4px; }}
        .compare-row {{ font-size: 13px; font-weight: 800; }}
        .compare-row.good {{ color: var(--green); }}
        .compare-row.bad {{ color: var(--red); }}
        .compare-row.neutral {{ color: var(--muted); }}
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
        .health-grid {{ display:grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }}
        .health-item {{ padding: 16px; border-radius: 18px; background: #fff; border:1px solid var(--line); }}
        .health-title {{ font-weight: 800; margin-bottom: 8px; }}
        .health-status {{ display:inline-flex; padding: 7px 10px; border-radius: 999px; font-size: 12px; font-weight: 800; text-transform: uppercase; }}
        .health-status.good {{ color:#11633f; background: rgba(31,157,102,.12); }}
        .health-status.warn {{ color:#a75300; background: rgba(255,138,31,.12); }}
        .health-status.bad {{ color:#a22d40; background: rgba(207,80,96,.12); }}
        table {{ width:100%; border-collapse: collapse; }}
        th, td {{ text-align:left; padding: 11px 8px; border-bottom: 1px solid rgba(234,223,206,.85); font-size: 13px; }}
        th {{ color: var(--muted); font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing:.08em; }}
        .lang-en.hidden, .lang-sk.hidden {{ display:none !important; }}
        @media (max-width: 1280px) {{ .layout {{ grid-template-columns: 1fr; }} .sidebar {{ position: static; height:auto; }} }}
        @media (max-width: 1080px) {{ .hero, .grid-2, .kpi-grid, .health-grid, .mini-grid {{ grid-template-columns: 1fr; }} }}
    </style>
</head>
<body>
    <div class="layout">
        <aside class="sidebar">
            <div class="brand">
                <div class="brand-mark">R</div>
                <div>
                    <strong>{title}</strong>
                    <small><span class="lang-en">test2 isolated dashboard</span><span class="lang-sk hidden">test2 izolovaný dashboard</span></small>
                </div>
            </div>
            <div class="nav-label"><span class="lang-en">Navigate</span><span class="lang-sk hidden">Navigácia</span></div>
            <a class="nav-link active" href="#overview"><span class="nav-dot">01</span><span class="lang-en">Overview</span><span class="lang-sk hidden">Prehľad</span></a>
            <a class="nav-link" href="#sales"><span class="nav-dot">02</span><span class="lang-en">Sales</span><span class="lang-sk hidden">Predaj</span></a>
            <a class="nav-link" href="#economics"><span class="nav-dot">03</span><span class="lang-en">Economics</span><span class="lang-sk hidden">Ekonomika</span></a>
            <a class="nav-link" href="#customers"><span class="nav-dot">04</span><span class="lang-en">Customers</span><span class="lang-sk hidden">Zákazníci</span></a>
            <a class="nav-link" href="#geography"><span class="nav-dot">05</span><span class="lang-en">Geography</span><span class="lang-sk hidden">Geografia</span></a>
            <a class="nav-link" href="#products"><span class="nav-dot">06</span><span class="lang-en">Products</span><span class="lang-sk hidden">Produkty</span></a>
            <a class="nav-link" href="#health"><span class="nav-dot">07</span><span class="lang-en">Data health</span><span class="lang-sk hidden">Kvalita dát</span></a>
        </aside>
        <main class="content">
            <div class="shell">
                <section class="hero" id="overview">
                    <div class="panel hero-main">
                        <div class="badge"><span class="lang-en">research-driven concept</span><span class="lang-sk hidden">research-driven koncept</span></div>
                        <h1>{title}</h1>
                        <p class="subtitle"><span class="lang-en">A new test2 dashboard built from scratch around ecommerce dashboard best practices: executive KPIs first, grouped business questions, fewer but clearer charts, and explicit source confidence.</span><span class="lang-sk hidden">Nový test2 dashboard postavený od nuly podľa ecommerce dashboard best practices: najprv executive KPI, potom business otázky, menej ale čitateľnejších grafov a explicitný stav dátových zdrojov.</span></p>
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
                        {period_switcher_html}
                        <div class="panel hero-kpis">
                            <div class="hero-kpi"><small><span class="lang-en">Revenue</span><span class="lang-sk hidden">Tržby</span></small><strong>€{total_revenue:,.0f}</strong></div>
                            <div class="hero-kpi"><small><span class="lang-en">Profit</span><span class="lang-sk hidden">Zisk</span></small><strong>€{total_profit:,.0f}</strong></div>
                            <div class="hero-kpi"><small><span class="lang-en">Orders</span><span class="lang-sk hidden">Objednávky</span></small><strong>{total_orders:,}</strong></div>
                            <div class="hero-kpi"><small><span class="lang-en">Blended ROAS</span><span class="lang-sk hidden">Blended ROAS</span></small><strong>{blended_roas:.2f}x</strong></div>
                        </div>
                    </div>
                </section>

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
                        <div class="panel chart-card" id="economics">
                            <div class="card-head">
                                <div>
                                    <h3><span class="lang-en">Cost structure</span><span class="lang-sk hidden">Štruktúra nákladov</span></h3>
                                    <p><span class="lang-en">Where the money goes across product, logistics, ads and fixed overhead.</span><span class="lang-sk hidden">Kam odchádzajú peniaze medzi produkt, logistiku, reklamu a fix.</span></p>
                                </div>
                            </div>
                            <div class="chart-shell compact"><canvas id="costMixChart"></canvas></div>
                            <div class="mini-grid">
                                <div class="mini-card"><small><span class="lang-en">Total ads</span><span class="lang-sk hidden">Spolu reklama</span></small><strong>€{total_ads:,.0f}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Refund rate</span><span class="lang-sk hidden">Refund rate</span></small><strong>{round(_num((refunds_analysis or {}).get("refund_rate_pct")), 2):.1f}%</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Top city</span><span class="lang-sk hidden">Top mesto</span></small><strong>{escape(top_city)}</strong></div>
                                <div class="mini-card"><small><span class="lang-en">Top product</span><span class="lang-sk hidden">Top produkt</span></small><strong>{escape(top_product)}</strong></div>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="section" id="customers">
                    <div class="section-head">
                        <h2><span class="lang-en">Customer quality and margin behavior</span><span class="lang-sk hidden">Kvalita zákazníkov a správanie marže</span></h2>
                        <p><span class="lang-en">Growth quality is clearer when you separate margin behavior from new vs returning revenue.</span><span class="lang-sk hidden">Kvalita rastu je čitateľnejšia, keď oddelíš správanie marže od nových vs vracajúcich sa tržieb.</span></p>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head">
                                <div>
                                    <h3><span class="lang-en">Margin corridor</span><span class="lang-sk hidden">Koridor marže</span></h3>
                                    <p><span class="lang-en">Pre-ad and post-ad margins shown together.</span><span class="lang-sk hidden">Pre-ad a post-ad marža v jednom pohľade.</span></p>
                                </div>
                            </div>
                            <div class="chart-shell"><canvas id="marginChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head">
                                <div>
                                    <h3><span class="lang-en">New vs returning revenue</span><span class="lang-sk hidden">Nové vs vracajúce sa tržby</span></h3>
                                    <p><span class="lang-en">Acquisition and repeat demand shown separately.</span><span class="lang-sk hidden">Akvizícia a opakovaný dopyt zobrazené oddelene.</span></p>
                                </div>
                            </div>
                            <div class="chart-shell"><canvas id="customerMixChart"></canvas></div>
                        </div>
                    </div>
                </section>

                <section class="section" id="geography">
                    <div class="section-head">
                        <h2><span class="lang-en">Geography</span><span class="lang-sk hidden">Geografia</span></h2>
                        <p><span class="lang-en">Concentration reads help explain where demand is strongest.</span><span class="lang-sk hidden">Pohľad na koncentráciu pomáha vysvetliť, kde je dopyt najsilnejší.</span></p>
                    </div>
                    <div class="grid-2">
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Top cities by revenue</span><span class="lang-sk hidden">Top mestá podľa tržieb</span></h3></div></div>
                            <div class="chart-shell compact"><canvas id="cityChart"></canvas></div>
                        </div>
                        <div class="panel chart-card">
                            <div class="card-head"><div><h3><span class="lang-en">Country split</span><span class="lang-sk hidden">Rozdelenie podľa krajín</span></h3></div></div>
                            <div class="chart-shell compact"><canvas id="countryChart"></canvas></div>
                        </div>
                    </div>
                </section>

                <section class="section" id="products">
                    <div class="section-head">
                        <h2><span class="lang-en">Products</span><span class="lang-sk hidden">Produkty</span></h2>
                        <p><span class="lang-en">Top products by profit contribution, not just by volume.</span><span class="lang-sk hidden">Top produkty podľa prínosu na zisku, nie len podľa objemu.</span></p>
                    </div>
                    <div class="panel table-card">
                        <table>
                            <thead>
                                <tr>
                                    <th><span class="lang-en">Product</span><span class="lang-sk hidden">Produkt</span></th>
                                    <th>SKU</th>
                                    <th><span class="lang-en">Revenue</span><span class="lang-sk hidden">Tržby</span></th>
                                    <th><span class="lang-en">Profit</span><span class="lang-sk hidden">Zisk</span></th>
                                    <th><span class="lang-en">Margin</span><span class="lang-sk hidden">Marža</span></th>
                                    <th><span class="lang-en">Orders</span><span class="lang-sk hidden">Objednávky</span></th>
                                </tr>
                            </thead>
                            <tbody>{product_rows_html}</tbody>
                        </table>
                    </div>
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
        const KPI_TYPES = {{
            revenue: 'currency', profit: 'currency', orders: 'integer', aov: 'currency',
            cac: 'currency', roas: 'multiple', pre_ad_contribution_margin: 'percent',
            post_ad_margin: 'percent', company_margin_with_fixed: 'percent'
        }};
        function fmtCurrency(v) {{ if (v === null || v === undefined || Number.isNaN(Number(v))) return 'N/A'; return new Intl.NumberFormat('en-US', {{ style: 'currency', currency: 'EUR', maximumFractionDigits: 2 }}).format(Number(v)); }}
        function fmtInt(v) {{ if (v === null || v === undefined || Number.isNaN(Number(v))) return 'N/A'; return new Intl.NumberFormat('en-US', {{ maximumFractionDigits: 0 }}).format(Number(v)); }}
        function fmtPercent(v) {{ if (v === null || v === undefined || Number.isNaN(Number(v))) return 'N/A'; return `${{Number(v).toFixed(1)}}%`; }}
        function fmtMultiple(v) {{ if (v === null || v === undefined || Number.isNaN(Number(v))) return 'N/A'; return `${{Number(v).toFixed(2)}}x`; }}
        function fmtMetric(key, value) {{
            const type = KPI_TYPES[key] || 'text';
            if (type === 'currency') return fmtCurrency(value);
            if (type === 'integer') return fmtInt(value);
            if (type === 'percent') return fmtPercent(value);
            if (type === 'multiple') return fmtMultiple(value);
            return value ?? 'N/A';
        }}
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
                const metricComps = ((comps[windowKey] || {{}})[def.key]) || {{}};
                const rows = Object.entries(metricComps).slice(0, 2).map(([compKey, compVal]) => {{
                    const names = (compLabels[windowKey] || {{}})[compKey] || {{ en: compKey, sk: compKey }};
                    return `<div class=\"compare-row ${{compClass(compVal, def.direction)}}\">${{compText(compVal)}} <span style=\"opacity:.85;\">${{currentLang === 'sk' ? names.sk : names.en}}</span></div>`;
                }}).join('');
                return `<article class=\"kpi-card\"><small>${{label}}</small><div class=\"kpi-value\">${{fmtMetric(def.key, (current.metrics || {{}})[def.key])}}</div><div class=\"kpi-period\">${{period}}</div><div class=\"compare-list\">${{rows}}</div></article>`;
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
            new Chart(document.getElementById('cityChart'), {{
                type: 'bar',
                data: {{ labels: DATA.cities.map(x => x.city || 'Unknown'), datasets: [{{ label: 'Revenue', data: DATA.cities.map(x => Number(x.revenue || 0)), backgroundColor: 'rgba(255,138,31,.78)', borderRadius: 8 }}] }},
                options: {{ ...baseOptions(), indexAxis: 'y', plugins: {{ ...baseOptions().plugins, legend: {{ display: false }} }} }},
            }});
            const countryOpts = baseOptions();
            countryOpts.scales.y1 = {{ position: 'right', grid: {{ display: false }}, ticks: {{ color: '#8a8178', font: {{ size: 11 }} }}, border: {{ display: false }} }};
            new Chart(document.getElementById('countryChart'), {{
                data: {{
                    labels: DATA.countries.map(x => x.country || 'Unknown'),
                    datasets: [
                        {{ type: 'bar', label: 'Revenue', data: DATA.countries.map(x => Number(x.revenue || 0)), backgroundColor: 'rgba(255,138,31,.72)', borderRadius: 8, yAxisID: 'y' }},
                        {{ type: 'line', label: 'Orders', data: DATA.countries.map(x => Number(x.orders || 0)), borderColor: '#4766ff', tension: .35, borderWidth: 2.5, pointRadius: 3, yAxisID: 'y1' }},
                    ],
                }},
                options: countryOpts,
            }});
        }}
        document.querySelectorAll('.lang-btn').forEach(btn => btn.addEventListener('click', () => applyLang(btn.dataset.lang)));
        document.querySelectorAll('.window-btn').forEach(btn => btn.addEventListener('click', () => renderKpis(btn.dataset.window)));
        applyLang(lang());
        renderKpis(currentWindow);
        buildCharts();
    </script>
</body>
</html>"""
