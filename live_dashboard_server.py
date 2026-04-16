#!/usr/bin/env python3
"""Read-only live dashboard server for BizniWeb reporting."""

from __future__ import annotations

import argparse
import json
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import parse_qs, quote, urlparse


ROOT_DIR = Path(__file__).resolve().parent
PROJECTS_DIR = ROOT_DIR / "projects"


def available_projects() -> List[str]:
    return sorted(
        entry.name
        for entry in PROJECTS_DIR.iterdir()
        if entry.is_dir() and (entry / "settings.json").exists()
    )


def _sorted_file_candidates(paths: Iterable[Path]) -> List[Path]:
    return sorted(
        (path for path in paths if path.is_file()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _latest_directory_artifact(directory: Path, *patterns: str) -> Optional[Path]:
    seen: set[str] = set()
    candidates: List[Path] = []
    for pattern in patterns:
        for path in directory.glob(pattern):
            resolved = str(path.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(path)
    ordered = _sorted_file_candidates(candidates)
    return ordered[0] if ordered else None


def resolve_latest_report_path(project: str) -> Optional[Path]:
    data_dir = ROOT_DIR / "data" / project
    return _latest_directory_artifact(
        data_dir,
        "report_latest.html",
        "report_latest__*.html",
        "report_*.html",
    )


def resolve_latest_payload_path(project: str) -> Optional[Path]:
    data_dir = ROOT_DIR / "data" / project
    return _latest_directory_artifact(
        data_dir,
        "dashboard_payload_latest.json",
        "dashboard_payload_latest__*.json",
        "dashboard_payload_*.json",
    )


def _read_json_file(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON payload at '{path}' must decode to an object.")
    return payload


def _normalize_period_key(period_key: Optional[str]) -> str:
    normalized = str(period_key or "full").strip().lower()
    return normalized or "full"


def _path_within_root(candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(ROOT_DIR.resolve())
        return True
    except ValueError:
        return False


def _coerce_generated_path(raw_path: Any) -> Optional[Path]:
    if raw_path in (None, ""):
        return None
    candidate = Path(str(raw_path))
    if not candidate.is_absolute():
        candidate = ROOT_DIR / candidate
    candidate = candidate.resolve()
    if not _path_within_root(candidate):
        return None
    return candidate if candidate.exists() else None


def _resolve_period_report_from_latest(project: str, period_key: str) -> Optional[Path]:
    if period_key == "full":
        return resolve_latest_report_path(project)

    latest_payload = resolve_latest_payload_path(project)
    if latest_payload is None or not latest_payload.exists():
        return None

    snapshot = _read_json_file(latest_payload)
    switcher = snapshot.get("period_switcher") or {}

    for spec in switcher.get("_embedded_specs") or []:
        if _normalize_period_key(spec.get("key")) != period_key:
            continue
        report_path = _coerce_generated_path(spec.get("report_path"))
        if report_path is not None:
            return report_path

    for option in switcher.get("options") or []:
        if _normalize_period_key(option.get("key")) != period_key:
            continue
        href = option.get("href")
        if not href:
            continue
        candidate = (latest_payload.parent / str(href)).resolve()
        if _path_within_root(candidate) and candidate.exists():
            return candidate
    return None


def resolve_period_report_path(project: str, period_key: Optional[str]) -> Optional[Path]:
    return _resolve_period_report_from_latest(project, _normalize_period_key(period_key))


def resolve_period_payload_path(project: str, period_key: Optional[str]) -> Optional[Path]:
    normalized = _normalize_period_key(period_key)
    if normalized == "full":
        return resolve_latest_payload_path(project)

    report_path = _resolve_period_report_from_latest(project, normalized)
    if report_path is None:
        return None

    payload_path = _latest_directory_artifact(
        report_path.parent,
        "dashboard_payload_latest.json",
        "dashboard_payload_latest__*.json",
        "dashboard_payload_*.json",
    )
    if payload_path is not None:
        return payload_path

    derived = report_path.with_name(report_path.name.replace("report_", "dashboard_payload_").replace(".html", ".json"))
    if derived.exists():
        return derived
    return None


def _json_script_content(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False).replace("<", "\\u003c")


def build_index_html(projects: List[str]) -> str:
    cards = []
    for project in projects:
        report_path = resolve_latest_report_path(project)
        payload_path = resolve_latest_payload_path(project)
        project_q = quote(project)
        cards.append(
            "<article class='card'>"
            f"<h2>{escape(project)}</h2>"
            f"<p>Live dashboard: {'ready' if payload_path else 'missing'}</p>"
            f"<p>HTML report: {'ready' if report_path else 'missing'}</p>"
            f"<p>JSON payload: {'ready' if payload_path else 'missing'}</p>"
            f"<p><a href='/dashboard/{project_q}'>Open live dashboard</a></p>"
            f"<p><a href='/report/{project_q}'>Open full HTML report</a></p>"
            f"<p><a href='/api/{project_q}/latest'>Open latest JSON</a></p>"
            "</article>"
        )
    cards_html = "".join(cards) or "<p>No reporting projects found.</p>"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BizniWeb Live Dashboards</title>
  <style>
    body {{ margin:0; font-family: Georgia, serif; background:#f6f1e8; color:#201a16; }}
    main {{ max-width:1100px; margin:0 auto; padding:40px 20px 60px; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:18px; }}
    .card {{ background:#fffaf3; border:1px solid #e7d7c6; border-radius:18px; padding:22px; }}
    a {{ color:#b65a2a; text-decoration:none; font-weight:700; }}
  </style>
</head>
<body>
  <main>
    <h1>BizniWeb Live Dashboards</h1>
    <p>Nightly email reporting stays unchanged. This server exposes the latest generated outputs online and now includes a period-aware read-only dashboard.</p>
    <section class="grid">{cards_html}</section>
  </main>
</body>
</html>"""


def build_live_dashboard_html(projects: List[str], initial_project: str, initial_period: str) -> str:
    bootstrap_json = _json_script_content(
        {
            "projects": projects,
            "project": initial_project,
            "period": _normalize_period_key(initial_period),
        }
    )
    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BizniWeb Live Dashboard</title>
  <style>
    :root {
      --bg:#f5efe4; --panel:#fffaf3; --line:#eadccc; --text:#231b16; --muted:#7f6a5b;
      --accent:#c8682d; --green:#147a57; --red:#b5483f;
    }
    * { box-sizing:border-box; }
    body { margin:0; font-family:Georgia,serif; color:var(--text); background:linear-gradient(180deg,#fcf8f2 0%,var(--bg) 100%); }
    a { color:inherit; text-decoration:none; }
    main { width:min(1240px,calc(100vw - 28px)); margin:0 auto; padding:22px 0 48px; }
    .hero,.panel { background:var(--panel); border:1px solid var(--line); border-radius:24px; padding:22px; box-shadow:0 18px 40px rgba(84,55,35,.08); }
    .hero { display:flex; justify-content:space-between; gap:18px; align-items:flex-start; }
    .eyebrow { display:inline-block; padding:6px 12px; border-radius:999px; background:rgba(200,104,45,.12); color:#a64f1c; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:.08em; }
    h1 { margin:14px 0 8px; font-size:clamp(34px,4vw,52px); line-height:1.02; }
    p { margin:0; color:var(--muted); line-height:1.5; }
    .actions,.chips,.facts { display:flex; flex-wrap:wrap; gap:10px; }
    .stack { display:grid; gap:18px; margin-top:18px; }
    .btn,.chip { display:inline-flex; align-items:center; justify-content:center; min-height:40px; padding:0 14px; border-radius:999px; border:1px solid rgba(200,104,45,.2); background:#fff; font-weight:700; cursor:pointer; }
    .btn.primary,.chip.active { background:linear-gradient(135deg,#d77236,#b9551d); color:#fff; border-color:transparent; }
    .meta,.cards { display:grid; gap:14px; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); }
    .card { background:#fff; border:1px solid rgba(200,104,45,.12); border-radius:18px; padding:16px; min-height:118px; }
    .label { margin:0 0 10px; color:var(--muted); font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.08em; }
    .value { margin:0; font-size:28px; font-weight:700; line-height:1.05; color:var(--text); }
    .value.pos { color:var(--green); }
    .value.neg { color:var(--red); }
    .note { margin:10px 0 0; font-size:13px; color:var(--muted); }
    .fact { background:#fff; border:1px solid rgba(200,104,45,.12); border-radius:16px; padding:14px 16px; min-width:180px; }
    .fact strong { display:block; margin-bottom:6px; font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; }
    .badge { display:inline-flex; align-items:center; min-height:28px; padding:0 12px; border-radius:999px; font-size:12px; font-weight:700; }
    .badge.scale { background:rgba(20,122,87,.12); color:var(--green); }
    .badge.cut { background:rgba(181,72,63,.12); color:var(--red); }
    .badge.hold,.badge.neutral { background:rgba(200,104,45,.12); color:#a64f1c; }
    .table-wrap { overflow:auto; border:1px solid rgba(200,104,45,.12); border-radius:18px; background:#fff; }
    table { width:100%; min-width:980px; border-collapse:collapse; }
    th,td { padding:14px 16px; text-align:left; border-bottom:1px solid rgba(200,104,45,.10); vertical-align:top; }
    th { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; background:#fffaf4; }
    .empty,.error { padding:16px 18px; border-radius:16px; background:#fff; border:1px dashed rgba(200,104,45,.24); color:var(--muted); }
    .error { color:var(--red); border-color:rgba(181,72,63,.24); }
    .loading { color:var(--muted); font-size:14px; }
    @media (max-width:860px) {
      main { width:min(100vw - 18px,1240px); padding-top:14px; }
      .hero { flex-direction:column; }
    }
  </style>
</head>
<body>
  <main data-marker="live-dashboard-app">
    <section class="hero">
      <div>
        <div class="eyebrow">BizniWeb live reporting</div>
        <h1 id="heroTitle">Loading live dashboard...</h1>
        <p id="heroLead">This read-only dashboard uses the same generated snapshots as the nightly email report, now with period-aware incrementality analysis.</p>
      </div>
      <div class="actions">
        <a id="reportLink" class="btn primary" href="#" target="_blank" rel="noopener">Open full HTML report</a>
        <a id="jsonLink" class="btn" href="#" target="_blank" rel="noopener">Open JSON snapshot</a>
      </div>
    </section>
    <div class="stack">
      <section class="panel">
        <div style="display:flex;justify-content:space-between;gap:16px;align-items:center;margin-bottom:14px;">
          <div>
            <h2 style="margin:0;font-size:22px;">Scope</h2>
            <p>Switch project and report period without touching the nightly email pipeline.</p>
          </div>
          <div id="loading" class="loading">Loading snapshot...</div>
        </div>
        <div id="projectNav" class="chips"></div>
        <div style="height:12px;"></div>
        <div id="periodNav" class="chips"></div>
        <div style="height:16px;"></div>
        <div id="scopeMeta" class="meta"></div>
      </section>
      <section class="panel">
        <h2 style="margin:0 0 6px;font-size:22px;">Selected period totals</h2>
        <p>Net totals for the exact selected report period.</p>
        <div style="height:14px;"></div>
        <div id="summaryGrid" class="cards"></div>
      </section>
      <section class="panel">
        <h2 style="margin:0 0 6px;font-size:22px;">KPI context</h2>
        <p>Quick management context from the same window logic used inside the generated HTML report.</p>
        <div style="height:14px;"></div>
        <div id="contextGrid" class="cards"></div>
      </section>
      <section class="panel">
        <h2 style="margin:0 0 6px;font-size:22px;">Ad impact / incrementality</h2>
        <p id="incrementalityLead">Loading incrementality view...</p>
        <div style="height:14px;"></div>
        <div id="incrementalityPrimary" class="cards"></div>
        <div style="height:14px;"></div>
        <div id="incrementalityFacts" class="facts"></div>
        <div style="height:14px;"></div>
        <div id="incrementalityEmpty" class="empty" hidden>Not enough comparable days in this selected range yet.</div>
        <div id="incrementalityTableWrap" class="table-wrap" hidden>
          <table>
            <thead>
              <tr>
                <th>View</th><th>Method</th><th>Confidence</th><th>Active days</th><th>Baseline days</th>
                <th>Inc spend / day</th><th>Inc revenue / day</th><th>Inc profit / day</th><th>Inc company / day</th>
                <th>Inc ROAS</th><th>Inc CAC</th><th>Verdict</th>
              </tr>
            </thead>
            <tbody id="incrementalityRows"></tbody>
          </table>
        </div>
      </section>
      <section id="errorPanel" class="panel" hidden>
        <h2 style="margin:0 0 8px;font-size:22px;">Status</h2>
        <div id="errorState" class="error"></div>
      </section>
    </div>
    <script id="live-dashboard-bootstrap" type="application/json">__BOOTSTRAP_JSON__</script>
    <script>
      const BOOTSTRAP = JSON.parse(document.getElementById('live-dashboard-bootstrap').textContent || '{}');
      const PROJECTS = BOOTSTRAP.projects || [];
      const state = { project: BOOTSTRAP.project, period: BOOTSTRAP.period || 'full' };
      const el = (id) => document.getElementById(id);
      const num = (value) => Number(value || 0);
      const sum = (values) => Array.isArray(values) ? values.reduce((acc, value) => acc + num(value), 0) : 0;
      const text = (value, fallback = 'N/A') => (value === null || value === undefined || value === '' ? fallback : String(value));
      const formatMoney = (value) => new Intl.NumberFormat('sk-SK', { style:'currency', currency:'EUR', maximumFractionDigits:2 }).format(num(value));
      const formatInt = (value) => new Intl.NumberFormat('sk-SK', { maximumFractionDigits:0 }).format(Math.round(num(value)));
      const formatNumber = (value, digits = 2) => new Intl.NumberFormat('sk-SK', { minimumFractionDigits:digits, maximumFractionDigits:digits }).format(num(value));
      const formatPercent = (value, digits = 1) => value === null || value === undefined || value === '' ? 'N/A' : `${formatNumber(value, digits)}%`;
      const formatRatio = (value, digits = 2) => value === null || value === undefined || value === '' ? 'N/A' : `${formatNumber(value, digits)}x`;
      const toneClass = (value) => {
        const t = String(value || '').toLowerCase();
        if (t.includes('scale')) return 'scale';
        if (t.includes('cut')) return 'cut';
        if (t.includes('hold')) return 'hold';
        return 'neutral';
      };
      const valueClass = (value) => value > 0 ? 'value pos' : value < 0 ? 'value neg' : 'value';
      function renderProjectNav() {
        el('projectNav').innerHTML = PROJECTS.map((project) => `<button class="chip ${project === state.project ? 'active' : ''}" data-project="${project}">${project.toUpperCase()}</button>`).join('');
        el('projectNav').querySelectorAll('[data-project]').forEach((button) => button.addEventListener('click', () => loadSnapshot(button.dataset.project, state.period)));
      }
      function renderPeriodNav(snapshot) {
        const options = ((snapshot.period_switcher || {}).options) || [];
        el('periodNav').innerHTML = options.map((option) => `<button class="chip ${option.key === state.period ? 'active' : ''}" data-period="${option.key}">${text(option.label, option.key.toUpperCase())}</button>`).join('');
        el('periodNav').querySelectorAll('[data-period]').forEach((button) => button.addEventListener('click', () => loadSnapshot(state.project, button.dataset.period)));
      }
      function renderCards(targetId, cards) {
        el(targetId).innerHTML = cards.map((card) => `<article class="card"><p class="label">${card.label}</p><p class="${card.className || valueClass(card.raw)}">${card.value}</p><p class="note">${card.note}</p>${card.badge ? `<div style="margin-top:10px;"><span class="badge ${card.badgeClass}">${card.badge}</span></div>` : ''}</article>`).join('');
      }
      function buildTotals(snapshot) {
        const series = ((snapshot.dashboard || {}).series) || {};
        const revenue = sum(series.revenue);
        const profitWithoutFixed = sum(series.profit_without_fixed);
        const profitWithFixed = sum(series.profit_with_fixed);
        const orders = sum(series.orders);
        const fbAds = sum(series.fb_ads);
        const googleAds = sum(series.google_ads);
        const totalAds = sum(series.total_ads);
        return {
          revenue, profitWithoutFixed, profitWithFixed, orders, fbAds, googleAds, totalAds,
          aov: orders > 0 ? revenue / orders : 0,
          productCost: sum(series.product_cost),
          packaging: sum(series.packaging),
          shipping: sum(series.shipping),
          fixed: sum(series.fixed),
          days: Array.isArray(series.dates) ? series.dates.length : 0,
          blendedRoas: totalAds > 0 ? revenue / totalAds : null,
        };
      }
      function renderScopeMeta(snapshot) {
        const switcher = snapshot.period_switcher || {};
        const cards = [
          { label:'Current range', value:text(switcher.current_range_sk, `${snapshot.date_from} - ${snapshot.date_to}`), note:text(switcher.current_range_en, '') },
          { label:'Selected view', value:text(state.period, 'full').toUpperCase(), note:'7D / 30D / FULL live switching' },
          { label:'Generated at', value:text(snapshot.generated_at, 'N/A'), note:'Same snapshot source as nightly email report' },
          { label:'Source span', value:`${snapshot.date_from} → ${snapshot.date_to}`, note:'Top-level reporting export range' },
        ];
        el('scopeMeta').innerHTML = cards.map((card) => `<div class="fact"><strong>${card.label}</strong><div>${card.value}</div><div class="note">${card.note}</div></div>`).join('');
      }
      function renderSummary(snapshot) {
        const totals = buildTotals(snapshot);
        renderCards('summaryGrid', [
          { label:'Revenue net', value:formatMoney(totals.revenue), note:`${formatInt(totals.orders)} orders in ${formatInt(totals.days)} days`, raw:totals.revenue },
          { label:'Profit (post-ad, pre-fixed)', value:formatMoney(totals.profitWithoutFixed), note:'Primary ad-scaling profit view', raw:totals.profitWithoutFixed },
          { label:'Company profit (incl. fixed)', value:formatMoney(totals.profitWithFixed), note:'Post-ad and post-fixed', raw:totals.profitWithFixed },
          { label:'Average order value', value:formatMoney(totals.aov), note:'Net AOV in selected period', raw:totals.aov },
          { label:'Total ad spend', value:formatMoney(totals.totalAds), note:`FB ${formatMoney(totals.fbAds)} / Google ${formatMoney(totals.googleAds)}`, raw:totals.totalAds },
          { label:'Blended ROAS', value:formatRatio(totals.blendedRoas), note:`Product cost ${formatMoney(totals.productCost)} / Fixed ${formatMoney(totals.fixed)}`, raw:totals.blendedRoas || 0 },
          { label:'Packaging + shipping', value:formatMoney(totals.packaging + totals.shipping), note:`Packaging ${formatMoney(totals.packaging)} / Shipping subsidy ${formatMoney(totals.shipping)}`, raw:totals.packaging + totals.shipping },
          { label:'Orders', value:formatInt(totals.orders), note:'Selected period order count', raw:totals.orders },
        ]);
      }
      function renderContext(snapshot) {
        const kpis = ((snapshot.dashboard || {}).kpis) || {};
        const windowKey = kpis.default_window || 'monthly';
        const windowPayload = (kpis.windows || {})[windowKey] || {};
        const metrics = windowPayload.metrics || {};
        const secondary = windowPayload.secondary_metrics || {};
        renderCards('contextGrid', [
          { label:text(windowPayload.label_en, 'Current window'), value:text(windowPayload.label_sk, ''), note:'Window definition from generated report', raw:0, className:'value' },
          { label:'Revenue', value:formatMoney(metrics.revenue), note:'Window metric', raw:metrics.revenue },
          { label:'Post-ad profit (€)', value:formatMoney(metrics.profit), note:'Excludes fixed overhead', raw:metrics.profit },
          { label:'Orders', value:formatInt(metrics.orders), note:'Window metric', raw:metrics.orders },
          { label:'AOV', value:formatMoney(metrics.aov), note:'Window metric', raw:metrics.aov },
          { label:'CAC', value:formatMoney(metrics.cac), note:'Window metric', raw:metrics.cac ? -metrics.cac : 0 },
          { label:'ROAS', value:formatRatio(metrics.roas), note:'Window metric', raw:metrics.roas },
          { label:'Company margin (incl. fixed)', value:formatPercent(metrics.company_margin_with_fixed), note:`Absolute ${formatMoney(secondary.company_margin_with_fixed)}`, raw:metrics.company_margin_with_fixed },
        ]);
      }
      function renderIncrementality(snapshot) {
        const dashboard = snapshot.dashboard || {};
        const primary = dashboard.incrementality_primary || {};
        const rows = Array.isArray(dashboard.incrementality_rows) ? dashboard.incrementality_rows : [];
        const empty = !rows.length || !primary.key;
        el('incrementalityLead').textContent = empty ? 'This selected period does not yet have enough comparable ad-active vs baseline days.' : text(primary.verdict_reason_en, 'Incrementality comparison ready.');
        if (empty) {
          el('incrementalityPrimary').innerHTML = '';
          el('incrementalityFacts').innerHTML = '';
          el('incrementalityRows').innerHTML = '';
          el('incrementalityEmpty').hidden = false;
          el('incrementalityTableWrap').hidden = true;
          return;
        }
        renderCards('incrementalityPrimary', [
          { label:'Verdict', value:text(primary.verdict, 'N/A'), note:text(primary.label_en, 'Selected comparison'), raw:0, className:'value', badge:text(primary.verdict_tone, 'neutral'), badgeClass:toneClass(primary.verdict_tone || primary.verdict) },
          { label:'Confidence', value:text(primary.confidence, 'N/A').toUpperCase(), note:text(primary.confidence_note_en, ''), raw:0, className:'value' },
          { label:'Incremental spend / day', value:formatMoney(primary.incremental_total_ad_spend_per_day), note:'Compared against baseline days', raw:primary.incremental_total_ad_spend_per_day },
          { label:'Incremental revenue / day', value:formatMoney(primary.incremental_revenue_per_day), note:'Net revenue lift per day', raw:primary.incremental_revenue_per_day },
          { label:'Incremental profit / day', value:formatMoney(primary.incremental_profit_without_fixed_per_day), note:'Post-ad, pre-fixed', raw:primary.incremental_profit_without_fixed_per_day },
          { label:'Incremental company / day', value:formatMoney(primary.incremental_profit_with_fixed_per_day), note:'Post-ad and post-fixed', raw:primary.incremental_profit_with_fixed_per_day },
          { label:'Incremental ROAS', value:formatRatio(primary.incremental_roas), note:text(primary.method, 'Method unavailable'), raw:primary.incremental_roas },
          { label:'Incremental CAC', value:primary.incremental_cac ? formatMoney(primary.incremental_cac) : 'N/A', note:`Break-even CAC ${primary.break_even_cac ? formatMoney(primary.break_even_cac) : 'N/A'}`, raw:primary.incremental_cac ? -primary.incremental_cac : 0 },
        ]);
        el('incrementalityFacts').innerHTML = [
          { label:'Active days', value:formatInt(primary.active_days) },
          { label:'Baseline days', value:formatInt(primary.control_days) },
          { label:'Matched days', value:formatInt(primary.effective_pair_days) },
          { label:'Overlap rate', value:formatPercent(num(primary.channel_overlap_rate) * 100, 1) },
        ].map((fact) => `<div class="fact"><strong>${fact.label}</strong><div>${fact.value}</div></div>`).join('');
        el('incrementalityRows').innerHTML = rows.map((row) => `<tr><td><strong>${text(row.label_en, row.key)}</strong><div class="note">${text(row.label_sk, '')}</div></td><td>${text(row.method, 'N/A')}</td><td>${text(row.confidence, 'N/A').toUpperCase()}</td><td>${formatInt(row.active_days)}</td><td>${formatInt(row.control_days)}</td><td>${formatMoney(row.incremental_total_ad_spend_per_day)}</td><td>${formatMoney(row.incremental_revenue_per_day)}</td><td>${formatMoney(row.incremental_profit_without_fixed_per_day)}</td><td>${formatMoney(row.incremental_profit_with_fixed_per_day)}</td><td>${formatRatio(row.incremental_roas)}</td><td>${row.incremental_cac ? formatMoney(row.incremental_cac) : 'N/A'}</td><td><span class="badge ${toneClass(row.verdict_tone || row.verdict)}">${text(row.verdict, 'N/A')}</span></td></tr>`).join('');
        el('incrementalityEmpty').hidden = true;
        el('incrementalityTableWrap').hidden = false;
      }
      function renderError(message) {
        el('errorState').textContent = message;
        el('errorPanel').hidden = false;
      }
      function clearError() {
        el('errorPanel').hidden = true;
        el('errorState').textContent = '';
      }
      function updateLocation() {
        const nextUrl = new URL(window.location.href);
        nextUrl.pathname = `/dashboard/${encodeURIComponent(state.project)}`;
        nextUrl.searchParams.set('period', state.period);
        window.history.replaceState({}, '', nextUrl);
      }
      function updateActionLinks() {
        el('reportLink').href = `/report/${encodeURIComponent(state.project)}?period=${encodeURIComponent(state.period)}`;
        el('jsonLink').href = `/api/${encodeURIComponent(state.project)}/latest?period=${encodeURIComponent(state.period)}`;
      }
      function renderSnapshot(snapshot) {
        state.period = text((snapshot.period_switcher || {}).current_key, state.period).toLowerCase();
        document.title = `${state.project.toUpperCase()} Live Dashboard`;
        el('heroTitle').textContent = `${state.project.toUpperCase()} live dashboard`;
        el('heroLead').textContent = `Read-only live view for ${text((snapshot.period_switcher || {}).current_range_sk, `${snapshot.date_from} - ${snapshot.date_to}`)}. Incrementality uses the same generated payload as the nightly report and respects the selected report period.`;
        renderProjectNav();
        renderPeriodNav(snapshot);
        renderScopeMeta(snapshot);
        renderSummary(snapshot);
        renderContext(snapshot);
        renderIncrementality(snapshot);
        updateActionLinks();
        updateLocation();
        clearError();
      }
      async function fetchSnapshot(project, period) {
        const response = await fetch(`/api/${encodeURIComponent(project)}/latest?period=${encodeURIComponent(period)}`, { cache:'no-store' });
        if (!response.ok) {
          const detail = await response.text();
          throw new Error(detail || `Snapshot request failed with status ${response.status}.`);
        }
        return response.json();
      }
      async function loadSnapshot(project, period, allowFallback = true) {
        state.project = project;
        state.period = period || 'full';
        el('loading').hidden = false;
        try {
          const snapshot = await fetchSnapshot(project, state.period);
          renderSnapshot(snapshot);
        } catch (error) {
          if (allowFallback && state.period !== 'full') return loadSnapshot(project, 'full', false);
          renderProjectNav();
          el('periodNav').innerHTML = '';
          renderError(error instanceof Error ? error.message : String(error));
        } finally {
          el('loading').hidden = true;
        }
      }
      renderProjectNav();
      loadSnapshot(state.project, state.period);
    </script>
  </main>
</body>
</html>"""
    return html.replace("__BOOTSTRAP_JSON__", bootstrap_json)


class LiveDashboardHandler(BaseHTTPRequestHandler):
    server_version = "BizniWebLiveDashboard/1.1"

    def _send_bytes(self, body: bytes, *, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, payload: Dict[str, object], status: int = 200) -> None:
        self._send_bytes(
            json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
            content_type="application/json; charset=utf-8",
            status=status,
        )

    def _send_text(self, text: str, *, content_type: str, status: int = 200) -> None:
        self._send_bytes(text.encode("utf-8"), content_type=content_type, status=status)

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path.rstrip("/") or "/"
        projects = available_projects()
        requested_period = _normalize_period_key(query.get("period", ["full"])[0] if query.get("period") else "full")

        if path == "/health":
            self._send_json({"ok": True, "projects": projects})
            return
        if path == "/":
            self._send_text(build_index_html(projects), content_type="text/html; charset=utf-8")
            return
        if path == "/api/projects":
            self._send_json({"projects": projects})
            return

        parts = [part for part in path.split("/") if part]

        if len(parts) == 3 and parts[0] == "api" and parts[2] == "latest":
            project = parts[1]
            if project not in projects:
                self._send_json({"error": f"Unknown project '{project}'."}, status=404)
                return
            payload_path = resolve_period_payload_path(project, requested_period)
            if payload_path is None or not payload_path.exists():
                self._send_json(
                    {"error": f"No dashboard payload found for '{project}' and period '{requested_period}'."},
                    status=404,
                )
                return
            self._send_bytes(payload_path.read_bytes(), content_type="application/json; charset=utf-8")
            return

        if len(parts) == 2 and parts[0] == "dashboard":
            project = parts[1]
            if project not in projects:
                self._send_text(f"Unknown project '{escape(project)}'.", content_type="text/plain; charset=utf-8", status=404)
                return
            self._send_text(
                build_live_dashboard_html(projects, project, requested_period),
                content_type="text/html; charset=utf-8",
            )
            return

        if len(parts) == 2 and parts[0] == "report":
            project = parts[1]
            if project not in projects:
                self._send_text(f"Unknown project '{escape(project)}'.", content_type="text/plain; charset=utf-8", status=404)
                return
            report_path = resolve_period_report_path(project, requested_period)
            if report_path is None or not report_path.exists():
                self._send_text(
                    f"No HTML report found for '{escape(project)}' and period '{escape(requested_period)}'.",
                    content_type="text/plain; charset=utf-8",
                    status=404,
                )
                return
            self._send_bytes(report_path.read_bytes(), content_type="text/html; charset=utf-8")
            return

        self._send_text("Not found.", content_type="text/plain; charset=utf-8", status=404)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve latest BizniWeb live dashboards")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=8787, help="Bind port")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), LiveDashboardHandler)
    print(f"Serving BizniWeb live dashboards on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
