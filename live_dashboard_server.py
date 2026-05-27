#!/usr/bin/env python3
"""Read-only live dashboard server for BizniWeb reporting."""

from __future__ import annotations

import argparse
import base64
import hmac
import json
import os
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, quote, unquote, urlparse

from production_board import get_cached_production_board_snapshot, resolve_production_board_settings
from roy_operations_dashboard import (
    acknowledge_loss_product,
    clear_inbound_stock_order,
    get_cached_roy_operations_snapshot,
    mark_personal_pickup_shipped,
    resolve_roy_operations_settings,
    set_inbound_stock_order,
)
from roy_picking_lists_pdf import build_roy_picking_lists_filename, build_roy_picking_lists_pdf
from reporting_core import load_project_settings


ROOT_DIR = Path(__file__).resolve().parent
PROJECTS_DIR = ROOT_DIR / "projects"


def live_dashboard_auth_credentials() -> Optional[Tuple[str, str]]:
    """Return Basic Auth credentials when protection is configured."""
    user = os.getenv("LIVE_DASHBOARD_AUTH_USER", "").strip()
    password = os.getenv("LIVE_DASHBOARD_AUTH_PASSWORD", "")
    if not user and not password:
        return None
    if not user or not password:
        return ("", "")
    return (user, password)


def is_authorized_basic_header(header: Optional[str], credentials: Optional[Tuple[str, str]]) -> bool:
    if credentials is None:
        return True
    expected_user, expected_password = credentials
    if not expected_user or not expected_password:
        return False
    if not header or not header.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header[6:].strip(), validate=True).decode("utf-8")
    except Exception:
        return False
    user, separator, password = decoded.partition(":")
    if not separator:
        return False
    return hmac.compare_digest(user, expected_user) and hmac.compare_digest(password, expected_password)


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


def _project_env_name(project: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in str(project or "").upper())


def _latest_s3_artifact_bytes(project: str, filename: str) -> Optional[bytes]:
    settings = load_project_settings(project)
    s3_settings = settings.get("live_dashboard_artifacts") or {}
    env_project = _project_env_name(project)
    bucket = (
        os.getenv(f"LIVE_DASHBOARD_S3_BUCKET_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_S3_BUCKET", "").strip()
        or os.getenv(f"REPORT_S3_BUCKET_{env_project}", "").strip()
        or os.getenv("REPORT_S3_BUCKET", "").strip()
        or str(s3_settings.get("s3_bucket") or "").strip()
    )
    prefix = (
        os.getenv(f"LIVE_DASHBOARD_S3_PREFIX_{env_project}", "").strip()
        or os.getenv("LIVE_DASHBOARD_S3_PREFIX", "").strip()
        or os.getenv(f"REPORT_S3_PREFIX_{env_project}", "").strip()
        or os.getenv("REPORT_S3_PREFIX", "").strip()
        or str(s3_settings.get("s3_prefix") or "").strip()
        or f"daily-reports/{project}"
    ).strip("/")
    if not bucket:
        return None

    try:
        import boto3  # type: ignore
    except ImportError:
        return None

    region = (
        os.getenv(f"AWS_REGION_{env_project}", "").strip()
        or os.getenv("AWS_REGION", "eu-central-1").strip()
        or "eu-central-1"
    )
    key = f"{prefix}/latest/{filename}"
    try:
        response = boto3.client("s3", region_name=region).get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except Exception:
        return None


def read_latest_dashboard_payload(project: str) -> Dict[str, Any]:
    payload_path = resolve_latest_payload_path(project)
    if payload_path is not None and payload_path.exists():
        return _read_json_file(payload_path)

    raw = _latest_s3_artifact_bytes(project, "dashboard_payload_latest.json")
    if raw is None:
        return {}
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"S3 dashboard payload for '{project}' must decode to an object.")
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
        production_enabled = False
        try:
            project_settings = load_project_settings(project)
            production_enabled = bool(resolve_production_board_settings(project_settings)["enabled"])
            if project == "roy":
                production_enabled = production_enabled or bool(resolve_roy_operations_settings(project_settings)["enabled"])
        except Exception:
            production_enabled = False
        project_q = quote(project)
        production_link = (
            f"<p><a href='/production/{project_q}'>Open production board</a></p>"
            if production_enabled
            else ""
        )
        cards.append(
            "<article class='card'>"
            f"<h2>{escape(project)}</h2>"
            f"<p>Live dashboard: {'ready' if payload_path else 'missing'}</p>"
            f"<p>HTML report: {'ready' if report_path else 'missing'}</p>"
            f"<p>JSON payload: {'ready' if payload_path else 'missing'}</p>"
            f"{production_link}"
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


def build_production_board_html(project: str) -> str:
    bootstrap_json = _json_script_content({"project": project})
    html = """<!doctype html>
<html lang="sk">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>VEVO Production Board</title>
  <style>
    :root {
      --bg:#f4f6f7; --panel:#ffffff; --line:#d8dee4; --text:#17202a; --muted:#64707d;
      --green:#157347; --green-bg:#e8f5ee; --red:#b42318; --red-bg:#fdebea;
      --amber:#9a6700; --amber-bg:#fff4d6; --blue:#1f5f99; --blue-bg:#eaf3fb;
    }
    * { box-sizing:border-box; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:Arial,Helvetica,sans-serif; }
    main { width:min(1440px,calc(100vw - 24px)); margin:0 auto; padding:16px 0 36px; }
    header { display:flex; justify-content:space-between; gap:16px; align-items:flex-start; padding:16px 0; }
    h1 { margin:0; font-size:28px; line-height:1.15; letter-spacing:0; }
    h2 { margin:0; font-size:18px; line-height:1.2; letter-spacing:0; }
    p { margin:0; color:var(--muted); line-height:1.45; }
    button,a.button { min-height:38px; border:1px solid var(--line); background:#fff; color:var(--text); border-radius:6px; padding:0 12px; font-weight:700; cursor:pointer; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; }
    button.primary { background:#17202a; color:#fff; border-color:#17202a; }
    .actions { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    .statusline { color:var(--muted); font-size:13px; min-height:18px; }
    .summary { display:grid; grid-template-columns:repeat(6,minmax(150px,1fr)); gap:10px; margin-bottom:12px; }
    .metric { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:12px; min-height:86px; }
    .metric .label { color:var(--muted); font-size:11px; text-transform:uppercase; font-weight:700; letter-spacing:.04em; }
    .metric .value { margin-top:8px; font-size:26px; font-weight:800; line-height:1; }
    .metric .note { margin-top:8px; font-size:12px; color:var(--muted); }
    .layout { display:grid; grid-template-columns:minmax(0,1.35fr) minmax(360px,.65fr); gap:12px; align-items:start; }
    .panel { background:var(--panel); border:1px solid var(--line); border-radius:8px; overflow:hidden; }
    .panel-head { display:flex; justify-content:space-between; gap:12px; align-items:center; padding:12px 14px; border-bottom:1px solid var(--line); background:#fafbfc; }
    .table-wrap { overflow:auto; }
    table { width:100%; border-collapse:collapse; min-width:900px; }
    th,td { padding:10px 12px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; font-size:13px; }
    th { color:var(--muted); background:#fafbfc; text-transform:uppercase; font-size:11px; letter-spacing:.04em; }
    tbody tr:hover { background:#f8fafc; }
    .qty { font-size:20px; font-weight:800; }
    .badge { display:inline-flex; align-items:center; min-height:24px; padding:0 8px; border-radius:999px; font-size:12px; font-weight:700; white-space:nowrap; }
    .badge.make { color:var(--green); background:var(--green-bg); }
    .badge.skip { color:var(--amber); background:var(--amber-bg); }
    .badge.warn { color:var(--red); background:var(--red-bg); }
    .badge.info { color:var(--blue); background:var(--blue-bg); }
    .muted { color:var(--muted); }
    .orders-list { display:grid; gap:8px; padding:12px; max-height:760px; overflow:auto; }
    .order { border:1px solid var(--line); border-radius:8px; padding:10px; background:#fff; }
    .order-top { display:flex; justify-content:space-between; gap:8px; align-items:flex-start; }
    .order-num { font-size:16px; font-weight:800; }
    .items { margin-top:8px; display:grid; gap:5px; }
    .item { display:flex; justify-content:space-between; gap:10px; font-size:13px; border-top:1px solid #eef1f4; padding-top:5px; }
    .product-orders { margin-top:8px; display:grid; gap:4px; color:var(--muted); font-size:12px; }
    .products-cards { display:none; }
    .product-card { border:1px solid var(--line); border-radius:8px; padding:10px; background:#fff; }
    .product-card-top { display:flex; justify-content:space-between; gap:10px; align-items:flex-start; }
    .product-title { font-weight:800; line-height:1.25; overflow-wrap:anywhere; }
    .product-meta { margin-top:4px; color:var(--muted); font-size:12px; overflow-wrap:anywhere; }
    .product-stats { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:6px; margin-top:10px; }
    .stat { border:1px solid #eef1f4; border-radius:6px; padding:7px; min-width:0; }
    .stat-label { color:var(--muted); font-size:10px; text-transform:uppercase; font-weight:700; letter-spacing:.04em; }
    .stat-value { margin-top:3px; font-size:14px; font-weight:800; line-height:1.2; overflow-wrap:anywhere; }
    .order-chips { margin-top:9px; display:flex; flex-wrap:wrap; gap:5px; }
    .order-chip { display:inline-flex; align-items:center; min-height:24px; max-width:100%; padding:0 7px; border-radius:999px; background:#f1f4f6; color:var(--text); font-size:11px; font-weight:700; overflow-wrap:anywhere; }
    .hidden { display:none !important; }
    .error { margin-bottom:12px; padding:10px 12px; border-radius:8px; color:var(--red); background:var(--red-bg); border:1px solid rgba(180,35,24,.25); }
    @media (max-width:1120px) {
      .summary { grid-template-columns:repeat(3,minmax(150px,1fr)); }
      .layout { grid-template-columns:1fr; }
    }
    @media (max-width:680px) {
      body { font-size:16px; }
      main { width:100%; padding:10px 8px 28px; }
      header { flex-direction:column; gap:10px; padding:10px 0; }
      h1 { font-size:23px; }
      h2 { font-size:17px; }
      p { font-size:13px; }
      button,a.button { min-height:44px; flex:1 1 auto; }
      .actions { width:100%; }
      .summary { grid-template-columns:repeat(2,minmax(130px,1fr)); }
      .metric { padding:10px; min-height:78px; }
      .metric .label { font-size:10px; letter-spacing:.02em; }
      .metric .value { font-size:21px; }
      .metric .note { font-size:11px; }
      .panel { border-radius:8px; }
      .panel-head { align-items:flex-start; padding:10px; }
      .desktop-products { display:none; }
      .products-cards { display:grid; gap:8px; padding:10px; }
      .product-card-top { display:grid; grid-template-columns:minmax(0,1fr) auto; }
      .product-stats { grid-template-columns:repeat(3,minmax(0,1fr)); }
      .orders-list { max-height:none; padding:10px; }
      .order-top { align-items:flex-start; }
      .item { display:grid; grid-template-columns:minmax(0,1fr) auto; align-items:start; }
    }
    @media (max-width:420px) {
      .summary { grid-template-columns:1fr; }
      .product-stats { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <main data-marker="vevo-production-board">
    <header>
      <div>
        <h1>VEVO výrobný board</h1>
        <p id="subtitle">Načítavam aktívne objednávky.</p>
      </div>
      <div class="actions">
        <button id="refreshBtn" class="primary" type="button">Refresh</button>
        <a class="button" href="/">Dashboardy</a>
      </div>
    </header>
    <div id="errorBox" class="error hidden"></div>
    <section class="summary" id="summary"></section>
    <section class="layout">
      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Produkty na výrobu</h2>
            <p id="productMeta">-</p>
          </div>
          <span id="cacheBadge" class="badge info">cache</span>
        </div>
        <div class="table-wrap desktop-products">
          <table>
            <thead>
              <tr>
                <th>Produkt</th>
                <th>Vyrobiť</th>
                <th>Obj.</th>
                <th>Najstaršia</th>
                <th>Statusy</th>
                <th>Objednávky</th>
              </tr>
            </thead>
            <tbody id="productsBody"></tbody>
          </table>
        </div>
        <div class="products-cards" id="productsCards"></div>
      </article>
      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Aktívne objednávky</h2>
            <p id="ordersMeta">-</p>
          </div>
        </div>
        <div class="orders-list" id="ordersList"></div>
      </article>
    </section>
  </main>
  <script id="production-bootstrap" type="application/json">__BOOTSTRAP_JSON__</script>
  <script>
    const BOOTSTRAP = JSON.parse(document.getElementById('production-bootstrap').textContent || '{}');
    const project = BOOTSTRAP.project || 'vevo';
    const el = (id) => document.getElementById(id);
    const fmtInt = (value) => new Intl.NumberFormat('sk-SK', { maximumFractionDigits: 0 }).format(Number(value || 0));
    const fmtQty = (value) => new Intl.NumberFormat('sk-SK', { maximumFractionDigits: 2 }).format(Number(value || 0));
    const text = (value, fallback = '-') => value === null || value === undefined || value === '' ? fallback : String(value);
    const safe = (value) => text(value, '').replace(/[&<>"']/g, (ch) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    let refreshTimer = null;

    function metric(label, value, note) {
      return `<article class="metric"><div class="label">${safe(label)}</div><div class="value">${safe(value)}</div><div class="note">${safe(note)}</div></article>`;
    }

    function statusBadges(statuses) {
      return Object.entries(statuses || {}).map(([name, count]) => `<span class="badge info">${safe(name)}: ${fmtInt(count)}</span>`).join(' ');
    }

    function productOrderLabel(order) {
      return `${safe(order.order_num)} · ${fmtQty(order.quantity)} ks · ${safe(order.status)}`;
    }

    function renderSummary(data) {
      const summary = data.summary || {};
      const scan = data.scan || {};
      el('summary').innerHTML = [
        metric('Aktívne objednávky', fmtInt(summary.active_orders), 'Čaká + online zaplatené'),
        metric('Objednávky s výrobou', fmtInt(summary.manufacturing_orders), 'Obsahujú VEVO výrobok'),
        metric('Produkty', fmtInt(summary.manufacturing_products), 'Zoskupené podľa EAN/kódu/názvu'),
        metric('Kusy na výrobu', fmtQty(summary.units_to_make), 'Súčet VEVO položiek'),
        metric('Ignorované kusy', fmtQty(summary.ignored_units), 'Iné značky + výnimky'),
        metric('Skenované', fmtInt(scan.orders_scanned), `${fmtInt(scan.pages_scanned)} strán, najstaršia ${text(scan.oldest_order_at_scanned)}`),
      ].join('');
      const generated = text(data.generated_at);
      const cache = data.cache || {};
      el('subtitle').textContent = `Posledná aktualizácia ${generated}. Auto refresh ${fmtInt(data.auto_refresh_seconds || 90)}s.`;
      el('cacheBadge').textContent = `${text(cache.status, 'live')} ${cache.age_seconds !== undefined ? `${cache.age_seconds}s` : ''}`;
      const stopReason = text(scan.stop_reason, '');
      let scanMessage = 'Scan prebehol bez dosiahnutia limitu.';
      if (scan.limit_reached) {
        scanMessage = 'Dosiahnutý scan limit, staršie aktívne objednávky môžu vyžadovať vyšší limit.';
      } else if (stopReason === 'empty_active_pages') {
        scanMessage = `Scan skončil po ${fmtInt(scan.empty_active_pages_at_stop)} stranách bez aktívnych objednávok.`;
      } else if (stopReason === 'api_exhausted') {
        scanMessage = 'BiznisWeb API vrátilo všetky dostupné strany.';
      }
      el('productMeta').textContent = scanMessage;
      el('cacheBadge').className = `badge ${cache.status === 'stale_after_error' || scan.limit_reached ? 'warn' : 'info'}`;
    }

    function renderProducts(data) {
      const products = data.products || [];
      el('productsBody').innerHTML = products.length ? products.map((product) => {
        const orderLines = (product.orders || []).slice(0, 8).map(productOrderLabel).join('<br>');
        const more = (product.orders || []).length > 8 ? `<br><span class="muted">+${fmtInt((product.orders || []).length - 8)} ďalších</span>` : '';
        return `<tr>
          <td><strong>${safe(product.label)}</strong><div class="muted">${safe(product.identifier || product.key)}</div></td>
          <td><span class="qty">${fmtQty(product.quantity_required)}</span></td>
          <td>${fmtInt(product.orders_count)}</td>
          <td>${safe(product.oldest_order_at)}</td>
          <td>${statusBadges(product.statuses)}</td>
          <td><div class="product-orders">${orderLines}${more}</div></td>
        </tr>`;
      }).join('') : `<tr><td colspan="6" class="muted">Aktuálne nie sú žiadne VEVO produkty na výrobu.</td></tr>`;
      el('productsCards').innerHTML = products.length ? products.map((product) => {
        const orders = product.orders || [];
        const chips = orders.slice(0, 4).map((order) => `<span class="order-chip">${productOrderLabel(order)}</span>`).join('');
        const more = orders.length > 4 ? `<span class="order-chip">+${fmtInt(orders.length - 4)} ďalších</span>` : '';
        return `<article class="product-card">
          <div class="product-card-top">
            <div>
              <div class="product-title">${safe(product.label)}</div>
              <div class="product-meta">${safe(product.identifier || product.key)}</div>
            </div>
            <span class="badge make">${fmtQty(product.quantity_required)} ks</span>
          </div>
          <div class="product-stats">
            <div class="stat"><div class="stat-label">Obj.</div><div class="stat-value">${fmtInt(product.orders_count)}</div></div>
            <div class="stat"><div class="stat-label">Najstaršia</div><div class="stat-value">${safe(product.oldest_order_at)}</div></div>
            <div class="stat"><div class="stat-label">Statusy</div><div class="stat-value">${statusBadges(product.statuses)}</div></div>
          </div>
          <div class="order-chips">${chips}${more}</div>
        </article>`;
      }).join('') : '<div class="product-card muted">Aktuálne nie sú žiadne VEVO produkty na výrobu.</div>';
    }

    function renderOrders(data) {
      const orders = data.orders || [];
      el('ordersMeta').textContent = `${fmtInt(orders.length)} aktívnych objednávok`;
      el('ordersList').innerHTML = orders.length ? orders.map((order) => {
        const items = (order.items || []).map((item) => `<div class="item"><span>${safe(item.label)} <span class="muted">(${safe(item.reason)})</span></span><strong>${fmtQty(item.quantity)} ks</strong></div>`).join('');
        const badgeClass = Number(order.manufacturing_units || 0) > 0 ? 'make' : 'skip';
        return `<article class="order">
          <div class="order-top">
            <div><div class="order-num">${safe(order.order_num)}</div><div class="muted">${safe(order.purchase_at)} · ${safe(order.sum)}</div></div>
            <span class="badge ${badgeClass}">${fmtQty(order.manufacturing_units)} ks</span>
          </div>
          <div class="muted">${safe(order.status)}</div>
          <div class="items">${items}</div>
        </article>`;
      }).join('') : '<p class="muted">Žiadne aktívne objednávky.</p>';
    }

    function showError(message) {
      el('errorBox').textContent = message;
      el('errorBox').classList.remove('hidden');
    }

    function clearError() {
      el('errorBox').classList.add('hidden');
      el('errorBox').textContent = '';
    }

    async function loadBoard(force = false) {
      el('refreshBtn').disabled = true;
      try {
        const url = `/api/production/${encodeURIComponent(project)}/live${force ? '?refresh=1' : ''}`;
        const response = await fetch(url, { cache: 'no-store' });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
        renderSummary(data);
        renderProducts(data);
        renderOrders(data);
        clearError();
        if (refreshTimer) clearInterval(refreshTimer);
        refreshTimer = setInterval(() => loadBoard(false), Math.max(30, Number(data.auto_refresh_seconds || 90)) * 1000);
      } catch (error) {
        showError(error instanceof Error ? error.message : String(error));
      } finally {
        el('refreshBtn').disabled = false;
      }
    }

    el('refreshBtn').addEventListener('click', () => loadBoard(true));
    loadBoard(false);
  </script>
</body>
</html>"""
    return html.replace("__BOOTSTRAP_JSON__", bootstrap_json)


def build_roy_operations_dashboard_html(project: str = "roy") -> str:
    bootstrap_json = _json_script_content({"project": project})
    html = """<!doctype html>
<html lang="sk">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ROY Operations Dashboard</title>
  <style>
    :root {
      --bg:#f6f7f4; --panel:#ffffff; --line:#d9ded5; --text:#18211b; --muted:#657163;
      --green:#11734b; --green-bg:#e6f3ec; --red:#aa2f2f; --red-bg:#fdeaea;
      --amber:#8a5b00; --amber-bg:#fff2cc; --blue:#245f8f; --blue-bg:#e8f1f8;
      --ink:#14211b;
    }
    * { box-sizing:border-box; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:Arial,Helvetica,sans-serif; }
    main { width:min(1500px,calc(100vw - 24px)); margin:0 auto; padding:14px 0 34px; }
    header { display:flex; justify-content:space-between; gap:16px; align-items:flex-start; padding:12px 0 14px; }
    h1 { margin:0; font-size:29px; line-height:1.12; letter-spacing:0; }
    h2 { margin:0; font-size:18px; line-height:1.2; letter-spacing:0; }
    h3 { margin:0; font-size:15px; line-height:1.2; letter-spacing:0; }
    p { margin:0; color:var(--muted); line-height:1.42; }
    button,a.button,select { min-height:38px; border:1px solid var(--line); background:#fff; color:var(--text); border-radius:6px; padding:0 11px; font-weight:700; cursor:pointer; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; }
    select { cursor:pointer; min-width:150px; }
    button.primary { background:var(--ink); color:#fff; border-color:var(--ink); }
    button.tab.active,button.chip.active { background:var(--ink); color:#fff; border-color:var(--ink); }
    button.sound.active { background:var(--green-bg); color:var(--green); border-color:rgba(17,115,75,.35); }
    button.sound.needs-arm { background:var(--amber-bg); color:var(--amber); border-color:rgba(138,91,0,.35); }
    button:disabled { opacity:.55; cursor:not-allowed; }
    .actions,.tabs,.chips { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    .statusline { color:var(--muted); font-size:13px; min-height:18px; }
    .alert-grid { display:grid; grid-template-columns:repeat(5,minmax(150px,1fr)); gap:10px; margin-bottom:12px; }
    .metric,.kpi-card { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:12px; min-height:86px; }
    .metric .label,.kpi-card .label { color:var(--muted); font-size:11px; text-transform:uppercase; font-weight:800; letter-spacing:.04em; }
    .metric .value,.kpi-card .value { margin-top:7px; font-size:25px; font-weight:850; line-height:1.05; }
    .metric .note,.kpi-card .note { margin-top:7px; font-size:12px; color:var(--muted); }
    .panel { background:var(--panel); border:1px solid var(--line); border-radius:8px; overflow:hidden; margin-bottom:12px; }
    .panel-head { display:flex; justify-content:space-between; gap:12px; align-items:center; padding:12px 14px; border-bottom:1px solid var(--line); background:#fafbf8; }
    .panel-body { padding:12px; }
    .kpi-controls { display:flex; justify-content:space-between; gap:12px; align-items:center; flex-wrap:wrap; margin-bottom:12px; }
    .kpi-grid { display:grid; grid-template-columns:repeat(3,minmax(220px,1fr)); gap:10px; }
    .kpi-card svg { width:100%; height:42px; margin-top:10px; display:block; }
    .positive { color:var(--green); }
    .negative { color:var(--red); }
    .neutral { color:var(--muted); }
    .badge { display:inline-flex; align-items:center; min-height:24px; padding:0 8px; border-radius:999px; font-size:12px; font-weight:800; white-space:nowrap; }
    .badge.good { color:var(--green); background:var(--green-bg); }
    .badge.warn { color:var(--amber); background:var(--amber-bg); }
    .badge.bad { color:var(--red); background:var(--red-bg); }
    .badge.info { color:var(--blue); background:var(--blue-bg); }
    .table-wrap { overflow:auto; }
    table { width:100%; min-width:980px; border-collapse:collapse; }
    th,td { padding:10px 12px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; font-size:13px; }
    th { color:var(--muted); background:#fafbf8; text-transform:uppercase; font-size:11px; letter-spacing:.04em; }
    tbody tr:hover { background:#f8faf7; }
    .muted { color:var(--muted); }
    .mono { font-family:Consolas,Monaco,monospace; }
    .items { display:grid; gap:3px; max-width:560px; }
    .item-line { display:flex; justify-content:space-between; gap:10px; border-top:1px solid #edf0eb; padding-top:3px; }
    .layout-2 { display:grid; grid-template-columns:minmax(0,1fr) minmax(420px,.42fr); gap:12px; align-items:start; }
    .performance-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; align-items:start; }
    .pickup-list { display:grid; gap:8px; max-height:620px; overflow:auto; }
    .pickup { border:1px solid var(--line); border-radius:8px; padding:10px; background:#fff; }
    .pickup-top { display:flex; justify-content:space-between; gap:10px; align-items:flex-start; }
    .pickup-title { font-weight:850; font-size:16px; }
    .checkline { display:flex; gap:8px; align-items:center; margin-top:9px; font-weight:800; }
    .checkline input { width:19px; height:19px; }
    .stock-action { display:grid; grid-template-columns:82px 138px auto; gap:6px; align-items:center; min-width:290px; }
    .stock-action input { min-height:34px; border:1px solid var(--line); border-radius:6px; padding:0 8px; font-weight:700; width:100%; }
    .stock-action button { min-height:34px; padding:0 9px; }
    .inbound-note { margin-top:5px; color:var(--green); font-size:12px; font-weight:800; }
    .country-products { display:grid; gap:5px; min-width:340px; }
    .country-product { display:grid; grid-template-columns:minmax(160px,1fr) auto auto; gap:8px; align-items:center; border-top:1px solid #edf0eb; padding-top:4px; }
    .country-product:first-child { border-top:0; padding-top:0; }
    .hidden { display:none !important; }
    .error,.ok { margin-bottom:12px; padding:10px 12px; border-radius:8px; border:1px solid rgba(170,47,47,.25); color:var(--red); background:var(--red-bg); }
    .ok { color:var(--green); background:var(--green-bg); border-color:rgba(17,115,75,.25); }
    @media (max-width:1180px) {
      .alert-grid { grid-template-columns:repeat(3,minmax(150px,1fr)); }
      .kpi-grid { grid-template-columns:repeat(2,minmax(220px,1fr)); }
      .layout-2 { grid-template-columns:1fr; }
      .performance-grid { grid-template-columns:1fr; }
    }
    @media (max-width:720px) {
      main { width:100%; padding:10px 8px 26px; }
      header { flex-direction:column; gap:10px; padding-top:8px; }
      h1 { font-size:23px; }
      .actions { width:100%; }
      .actions button,.actions a.button { flex:1 1 auto; min-height:44px; }
      .alert-grid,.kpi-grid { grid-template-columns:1fr; }
      .panel-head,.kpi-controls { align-items:flex-start; flex-direction:column; }
      button,a.button,select { min-height:42px; }
      .stock-action { grid-template-columns:1fr; min-width:220px; }
      table { min-width:860px; }
    }
  </style>
</head>
<body>
  <main data-marker="roy-operations-dashboard">
    <header>
      <div>
        <h1>ROY operations dashboard</h1>
        <p id="subtitle">Načítavam live stav.</p>
      </div>
      <div class="actions">
        <button id="soundToggleBtn" class="sound" type="button" aria-pressed="false">Zvuk vyp.</button>
        <a class="button" href="/api/operations/roy/picking-lists.pdf?refresh=1">Vysklad. PDF</a>
        <button id="refreshBtn" class="primary" type="button">Refresh</button>
        <a class="button" href="/">Dashboardy</a>
      </div>
    </header>
    <div id="messageBox" class="hidden"></div>
    <section class="alert-grid" id="alertGrid"></section>
    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Executive KPI deck</h2>
          <p id="kpiMeta">-</p>
        </div>
        <span id="cacheBadge" class="badge info">cache</span>
      </div>
      <div class="panel-body">
        <div class="kpi-controls">
          <div class="chips" id="kpiWindowNav"></div>
          <select id="monthSelect" aria-label="Kalendárny mesiac"></select>
        </div>
        <div class="kpi-grid" id="kpiGrid"></div>
      </div>
    </section>
    <nav class="tabs" style="margin-bottom:12px;">
      <button class="tab active" type="button" data-view="overview">Prehľad</button>
      <button class="tab" type="button" data-view="orders">Objednávky</button>
      <button class="tab" type="button" data-view="inventory">Sklad</button>
    </nav>
    <section id="view-overview">
      <div class="layout-2">
        <article class="panel">
          <div class="panel-head">
            <div>
              <h2>Objednávky na vybavenie</h2>
              <p id="ordersMeta">-</p>
            </div>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Objednávka</th><th>Status</th><th>Platba</th><th>Doprava</th><th>Suma</th><th>Položky</th></tr></thead>
              <tbody id="ordersBody"></tbody>
            </table>
          </div>
        </article>
        <article class="panel">
          <div class="panel-head">
            <div>
              <h2>Osobné odbery</h2>
              <p id="pickupMeta">-</p>
            </div>
          </div>
          <div class="panel-body pickup-list" id="pickupList"></div>
        </article>
      </div>
      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Skladové upozornenia</h2>
            <p id="inventoryAlertMeta">-</p>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Produkt</th><th>Riziko</th><th>Sklad</th><th>Objednané</th><th>30d dopyt</th><th>Cover</th><th>Vypredanie</th><th>Objednať do</th><th>Návrh</th></tr></thead>
            <tbody id="alertRowsBody"></tbody>
          </table>
        </div>
      </article>
      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Top značky</h2>
            <p id="brandPerformanceMeta">-</p>
          </div>
        </div>
        <div class="performance-grid panel-body">
          <div class="table-wrap">
            <table>
              <thead><tr><th>Značka podľa obratu</th><th>Obrat</th><th>Zisk</th><th>Marža</th></tr></thead>
              <tbody id="brandRevenueBody"></tbody>
            </table>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Značka podľa zisku</th><th>Zisk</th><th>Obrat</th><th>Marža</th></tr></thead>
              <tbody id="brandProfitBody"></tbody>
            </table>
          </div>
        </div>
      </article>
      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Top produkty</h2>
            <p id="productPerformanceMeta">-</p>
          </div>
        </div>
        <div class="performance-grid panel-body">
          <div class="table-wrap">
            <table>
              <thead><tr><th>Produkt podľa obratu</th><th>Obrat</th><th>Zisk</th><th>Kusy</th></tr></thead>
              <tbody id="productRevenueBody"></tbody>
            </table>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Produkt podľa zisku</th><th>Zisk</th><th>Obrat</th><th>Kusy</th></tr></thead>
              <tbody id="productProfitBody"></tbody>
            </table>
          </div>
        </div>
      </article>
      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Krajiny</h2>
            <p id="countryPerformanceMeta">-</p>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Krajina</th><th>Obj.</th><th>Obrat</th><th>Hrubý zisk</th><th>Spend</th><th>Čistý zisk</th><th>Marža</th><th>Top produkty v krajine</th></tr></thead>
            <tbody id="countryPerformanceBody"></tbody>
          </table>
        </div>
      </article>
      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Produkty v strate</h2>
            <p id="lossProductMeta">-</p>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Produkt</th><th>Obrat</th><th>Hrubý zisk/strata</th><th>Hrubá marža</th><th>Potvrdené</th></tr></thead>
            <tbody id="lossProductBody"></tbody>
          </table>
        </div>
      </article>
    </section>
    <section id="view-orders" class="hidden">
      <article class="panel">
        <div class="panel-head"><h2>Všetky vybaviteľné objednávky</h2><p id="ordersScanMeta">-</p></div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Objednávka</th><th>Dátum</th><th>Status</th><th>Platba</th><th>Doprava</th><th>Suma</th><th>Položky</th></tr></thead>
            <tbody id="ordersFullBody"></tbody>
          </table>
        </div>
      </article>
    </section>
    <section id="view-inventory" class="hidden">
      <article class="panel">
        <div class="panel-head">
          <div>
            <h2>Skladové zásoby</h2>
            <p id="inventoryMeta">-</p>
          </div>
        </div>
        <div class="panel-body">
          <div class="alert-grid" id="inventorySummaryGrid"></div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Objednaný produkt</th><th>Objednané kusy</th><th>ETA</th><th>Sklad pri zadaní</th><th>Aktuálny sklad</th><th>Akcia</th></tr></thead>
              <tbody id="inboundRowsBody"></tbody>
            </table>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Produkt</th><th>Sklad</th><th>Nákupná hodnota</th><th>Predajná hodnota</th><th>Cover</th><th>Vypredanie</th><th>Lead time</th><th>Objednané</th><th>Reorder</th></tr></thead>
            <tbody id="inventoryRowsBody"></tbody>
          </table>
        </div>
      </article>
    </section>
  </main>
  <script id="roy-operations-bootstrap" type="application/json">__BOOTSTRAP_JSON__</script>
  <script>
    const BOOTSTRAP = JSON.parse(document.getElementById('roy-operations-bootstrap').textContent || '{}');
    const project = BOOTSTRAP.project || 'roy';
    const el = (id) => document.getElementById(id);
    const fmtInt = (value) => new Intl.NumberFormat('sk-SK', { maximumFractionDigits:0 }).format(Math.round(Number(value || 0)));
    const fmtQty = (value) => new Intl.NumberFormat('sk-SK', { maximumFractionDigits:1 }).format(Number(value || 0));
    const fmtMoney = (value) => new Intl.NumberFormat('sk-SK', { style:'currency', currency:'EUR', maximumFractionDigits:2 }).format(Number(value || 0));
    const fmtPct = (value) => value === null || value === undefined ? 'N/A' : `${new Intl.NumberFormat('sk-SK', { maximumFractionDigits:1 }).format(Number(value || 0))}%`;
    const fmtRatio = (value) => value === null || value === undefined ? 'N/A' : `${new Intl.NumberFormat('sk-SK', { maximumFractionDigits:2 }).format(Number(value || 0))}x`;
    const text = (value, fallback='-') => value === null || value === undefined || value === '' ? fallback : String(value);
    const safe = (value) => text(value, '').replace(/[&<>"']/g, (ch) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    const cssEscape = (value) => window.CSS && CSS.escape ? CSS.escape(String(value)) : String(value).replace(/["\\\\]/g, '\\\\$&');
    let latestData = null;
    let refreshTimer = null;
    let kpiScope = 'monthly';
    let orderSoundEnabled = false;
    let orderSoundArmed = false;
    let orderAudioContext = null;
    let orderSoundInitialized = false;
    let seenFulfillableOrderKeys = new Set();
    const orderSoundStorageKey = `roy:${project}:new-order-sound`;

    const metricDefsFallback = [
      { key:'revenue', label_en:'Revenue (net)' },
      { key:'profit', label_en:'Post-ad profit (€)' },
      { key:'orders', label_en:'Orders' },
      { key:'aov', label_en:'AOV (net)' },
      { key:'cac', label_en:'CAC' },
      { key:'roas', label_en:'ROAS' },
      { key:'pre_ad_contribution_margin', label_en:'Pre-ad contribution' },
      { key:'post_ad_margin', label_en:'Post-ad margin' },
      { key:'company_margin_with_fixed', label_en:'Company margin (incl. fixed)' },
    ];

    function metric(label, value, note, tone='info') {
      return `<article class="metric"><div class="label">${safe(label)}</div><div class="value">${safe(value)}</div><div class="note">${safe(note)}</div></article>`;
    }
    function compactProductCell(row) {
      return `<strong>${safe(row.product || row.brand_label || row.group_label || '-')}</strong><div class="muted mono">${safe(row.sku || row.brand_key || row.group_key || '')}</div>`;
    }
    function soundButtonLabel() {
      if (!orderSoundEnabled) return 'Zvuk vyp.';
      return orderSoundArmed ? 'Zvuk zap.' : 'Zvuk čaká';
    }
    function updateSoundButton() {
      const button = el('soundToggleBtn');
      if (!button) return;
      button.textContent = soundButtonLabel();
      button.setAttribute('aria-pressed', orderSoundEnabled ? 'true' : 'false');
      button.classList.toggle('active', orderSoundEnabled && orderSoundArmed);
      button.classList.toggle('needs-arm', orderSoundEnabled && !orderSoundArmed);
    }
    function audioContext() {
      if (!('AudioContext' in window) && !('webkitAudioContext' in window)) return null;
      if (!orderAudioContext) {
        const AudioCtor = window.AudioContext || window.webkitAudioContext;
        orderAudioContext = new AudioCtor();
      }
      return orderAudioContext;
    }
    async function armOrderSound(playTest=false) {
      if (!orderSoundEnabled) {
        updateSoundButton();
        return false;
      }
      const ctx = audioContext();
      if (!ctx) {
        orderSoundArmed = false;
        updateSoundButton();
        return false;
      }
      try {
        if (ctx.state === 'suspended') await ctx.resume();
        orderSoundArmed = ctx.state === 'running';
        updateSoundButton();
        if (orderSoundArmed && playTest) playNewOrderSound(1, 0.35);
        return orderSoundArmed;
      } catch (error) {
        orderSoundArmed = false;
        updateSoundButton();
        return false;
      }
    }
    function playTone(ctx, frequency, start, duration, gainValue) {
      const oscillator = ctx.createOscillator();
      const gain = ctx.createGain();
      oscillator.type = 'sine';
      oscillator.frequency.setValueAtTime(frequency, start);
      gain.gain.setValueAtTime(0.0001, start);
      gain.gain.exponentialRampToValueAtTime(gainValue, start + 0.015);
      gain.gain.exponentialRampToValueAtTime(0.0001, start + duration);
      oscillator.connect(gain);
      gain.connect(ctx.destination);
      oscillator.start(start);
      oscillator.stop(start + duration + 0.02);
    }
    function playNewOrderSound(count=1, volume=0.7) {
      if (!orderSoundEnabled || !orderSoundArmed) return;
      const ctx = audioContext();
      if (!ctx || ctx.state !== 'running') return;
      const now = ctx.currentTime + 0.02;
      const repeats = Math.min(Math.max(Number(count || 1), 1), 3);
      for (let i = 0; i < repeats; i += 1) {
        const offset = now + (i * 0.18);
        playTone(ctx, 880, offset, 0.12, 0.12 * volume);
        playTone(ctx, 1175, offset + 0.08, 0.14, 0.10 * volume);
      }
    }
    function fulfillableOrderKeys(data) {
      return (((data.orders || {}).orders) || [])
        .map((order) => String(order.order_num || order.id || '').trim())
        .filter(Boolean);
    }
    function notifyAboutNewFulfillableOrders(data) {
      const keys = fulfillableOrderKeys(data);
      if (!orderSoundInitialized) {
        seenFulfillableOrderKeys = new Set(keys);
        orderSoundInitialized = true;
        return;
      }
      const newKeys = keys.filter((key) => !seenFulfillableOrderKeys.has(key));
      seenFulfillableOrderKeys = new Set(keys);
      if (!newKeys.length) return;
      showMessage(`${fmtInt(newKeys.length)} nová objednávka na odoslanie.`, true);
      if (orderSoundEnabled) {
        armOrderSound(false).then((armed) => {
          if (armed) playNewOrderSound(newKeys.length);
        });
      }
    }
    function initializeOrderSound() {
      orderSoundEnabled = localStorage.getItem(orderSoundStorageKey) === '1';
      updateSoundButton();
      el('soundToggleBtn').addEventListener('click', async () => {
        orderSoundEnabled = !orderSoundEnabled;
        localStorage.setItem(orderSoundStorageKey, orderSoundEnabled ? '1' : '0');
        if (!orderSoundEnabled) {
          orderSoundArmed = false;
          updateSoundButton();
          return;
        }
        await armOrderSound(true);
      });
      const armAfterUserAction = () => {
        if (orderSoundEnabled && !orderSoundArmed) {
          armOrderSound(false);
        }
      };
      window.addEventListener('pointerdown', armAfterUserAction, { passive:true });
      window.addEventListener('keydown', armAfterUserAction);
    }
    function inboundStatus(row) {
      const units = Number(row.inbound_ordered_units || 0);
      if (!units) return '<span class="muted">-</span>';
      return `<div><span class="badge good">${fmtQty(units)} ks</span><div class="inbound-note">ETA ${safe(row.inbound_expected_arrival_date || '-')}</div></div>`;
    }
    function inboundControls(row) {
      const sku = safe(row.sku);
      const product = safe(row.product);
      const available = Number(row.available_quantity || 0);
      const units = Number(row.inbound_ordered_units || 0);
      const eta = safe(row.inbound_expected_arrival_date || '');
      const clear = units > 0 ? `<button type="button" data-clear-inbound="${sku}">Zrušiť</button>` : '';
      return `<div>
        ${inboundStatus(row)}
        <div class="stock-action" style="margin-top:6px;">
          <input type="number" min="0.1" step="1" value="${units || ''}" placeholder="ks" data-inbound-units="${sku}">
          <input type="date" value="${eta}" data-inbound-eta="${sku}">
          <button type="button" data-save-inbound="${sku}" data-product="${product}" data-baseline="${available}">Uložiť</button>
        </div>
        ${clear}
      </div>`;
    }
    function badgeClass(value) {
      const v = String(value || '').toLowerCase();
      if (v.includes('negative') || v.includes('out of stock') || v.includes('critical') || v.includes('urgent') || v.includes('order now')) return 'bad';
      if (v.includes('partially') || v.includes('low') || v.includes('watch') || v.includes('prepare') || v.includes('plan')) return 'warn';
      if (v.includes('inbound') || v.includes('healthy') || v.includes('ok')) return 'good';
      return 'info';
    }
    function formatKpiValue(key, value) {
      if (['revenue','profit','aov','cac'].includes(key)) return value === null || value === undefined ? 'N/A' : fmtMoney(value);
      if (key === 'orders') return fmtInt(value);
      if (key === 'roas') return fmtRatio(value);
      if (key.includes('margin') || key.includes('contribution')) return fmtPct(value);
      return text(value, 'N/A');
    }
    function comparisonText(scope, metricKey, windowPayload) {
      const kpis = latestData.executive_kpis || {};
      if (scope.startsWith('month:')) {
        const comparison = ((windowPayload.comparisons || {})[metricKey] || {}).vs_previous_month;
        return comparison === null || comparison === undefined ? 'N/A vs previous month' : `${comparison >= 0 ? '+' : ''}${fmtPct(comparison)} vs previous month`;
      }
      const comparison = (((kpis.comparisons || {})[scope] || {})[metricKey]) || {};
      const key = Object.keys(comparison).find((name) => name.startsWith('vs_') && comparison[name] !== null && comparison[name] !== undefined);
      if (!key) return 'N/A vs comparable period';
      return `${comparison[key] >= 0 ? '+' : ''}${fmtPct(comparison[key])} ${key.replaceAll('_', ' ')}`;
    }
    function sparkline(values, key) {
      if (!Array.isArray(values) || values.length < 2) return '';
      const nums = values.map((v) => Number(v || 0));
      const min = Math.min(...nums);
      const max = Math.max(...nums);
      const spread = max - min || 1;
      const points = nums.map((v, i) => {
        const x = (i / Math.max(nums.length - 1, 1)) * 100;
        const y = 38 - ((v - min) / spread) * 32;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      }).join(' ');
      const cls = ['cac'].includes(key) ? '#aa2f2f' : '#11734b';
      return `<svg viewBox="0 0 100 42" preserveAspectRatio="none" aria-hidden="true"><polyline points="${points}" fill="none" stroke="${cls}" stroke-width="2.4" vector-effect="non-scaling-stroke"/><polygon points="0,40 ${points} 100,40" fill="${cls}" opacity=".10"/></svg>`;
    }
    function selectedKpiWindow() {
      const kpis = latestData.executive_kpis || {};
      if (kpiScope.startsWith('month:')) {
        const monthKey = kpiScope.slice(6);
        return (kpis.months || []).find((row) => row.key === monthKey) || null;
      }
      return (kpis.windows || {})[kpiScope] || null;
    }
    function renderKpiControls() {
      const labels = { daily:'Daily', weekly:'Weekly', monthly:'Monthly', all_time:'All-time' };
      el('kpiWindowNav').innerHTML = Object.keys(labels).map((key) => `<button class="chip ${kpiScope === key ? 'active' : ''}" type="button" data-kpi-window="${key}">${labels[key]}</button>`).join('');
      el('kpiWindowNav').querySelectorAll('[data-kpi-window]').forEach((btn) => btn.addEventListener('click', () => { kpiScope = btn.dataset.kpiWindow; renderKpis(); }));
      const months = ((latestData.executive_kpis || {}).months || []).slice().reverse();
      el('monthSelect').innerHTML = '<option value="">Kalendárny mesiac</option>' + months.map((m) => `<option value="${safe(m.key)}">${safe(m.label_sk || m.key)}</option>`).join('');
      el('monthSelect').value = kpiScope.startsWith('month:') ? kpiScope.slice(6) : '';
      el('monthSelect').onchange = () => { if (el('monthSelect').value) { kpiScope = `month:${el('monthSelect').value}`; renderKpis(); } };
    }
    function renderKpis() {
      if (!latestData) return;
      renderKpiControls();
      const kpis = latestData.executive_kpis || {};
      const defs = (kpis.metric_defs || []).length ? kpis.metric_defs : metricDefsFallback;
      const windowPayload = selectedKpiWindow() || {};
      const metrics = windowPayload.metrics || {};
      const trend = windowPayload.trend || {};
      const trendMetrics = trend.metrics || {};
      el('kpiMeta').textContent = `${text(windowPayload.label_sk || windowPayload.label_en || kpiScope)} · zdroj ${text(kpis.source_generated_at)}`;
      el('kpiGrid').innerHTML = defs.map((def) => {
        const key = def.key;
        const value = metrics[key];
        const compare = comparisonText(kpiScope, key, windowPayload);
        const tone = compare.startsWith('-') && key !== 'cac' ? 'negative' : compare.startsWith('+') ? 'positive' : 'neutral';
        const secondary = key === 'company_margin_with_fixed' && (windowPayload.secondary_metrics || {}).company_margin_with_fixed !== undefined
          ? ` · ${fmtMoney((windowPayload.secondary_metrics || {}).company_margin_with_fixed)}` : '';
        return `<article class="kpi-card"><div class="label">${safe(def.label_en || key)}</div><div class="value">${safe(formatKpiValue(key, value))}</div><div class="note ${tone}">${safe(compare)}${safe(secondary)}</div>${sparkline(trendMetrics[key], key)}</article>`;
      }).join('');
    }
    function renderAlerts(data) {
      const orders = (data.orders || {}).summary || {};
      const inv = (data.inventory || {}).summary || {};
      el('alertGrid').innerHTML = [
        metric('Na vybavenie', fmtInt(orders.fulfillable_orders), `${fmtInt(orders.paid_online_orders)} online + ${fmtInt(orders.cod_waiting_orders)} dobierka`),
        metric('Osobné odbery', fmtInt(orders.personal_pickups), `${fmtInt(orders.pickup_actions_available)} akcií dostupných`),
        metric('Kritický sklad', fmtInt(inv.stock_risk_critical_count), `${fmtInt(inv.stock_risk_30d_count)} položiek v 30d riziku`),
        metric('Objednať teraz', fmtInt(inv.alert_reorder_now_count), `${fmtInt(inv.alert_prepare_po_count)} pripraviť PO`),
        metric('Objednané na ceste', fmtQty(inv.inbound_ordered_units), `${fmtInt(inv.inbound_order_count)} položiek · ETA ${text(inv.inbound_next_arrival_date)}`),
        metric('Hodnota skladu', fmtMoney(inv.inventory_cost_value), `predajná ${fmtMoney(inv.inventory_retail_value)}`),
      ].join('');
    }
    function orderItemsHtml(order) {
      const items = order.items || [];
      if (!items.length) return '<span class="muted">bez položiek</span>';
      return `<div class="items">${items.slice(0, 6).map((item) => `<div class="item-line"><span>${safe(item.label)}</span><strong>${fmtQty(item.quantity)} ks</strong></div>`).join('')}${items.length > 6 ? `<div class="muted">+${fmtInt(items.length - 6)} ďalších</div>` : ''}</div>`;
    }
    function orderRow(order, includeDate=false) {
      return `<tr>
        <td><strong class="mono">${safe(order.order_num)}</strong>${includeDate ? `<div class="muted">${safe(order.purchase_at)}</div>` : ''}</td>
        <td><span class="badge ${order.fulfillment_reason === 'paid_online' ? 'good' : 'warn'}">${safe(order.status)}</span></td>
        <td>${safe((order.payment || {}).title)}</td>
        <td>${safe((order.shipping || {}).title)}</td>
        <td>${safe(order.sum)}</td>
        <td>${orderItemsHtml(order)}</td>
      </tr>`;
    }
    function renderOrders(data) {
      const ordersPayload = data.orders || {};
      const orders = ordersPayload.orders || [];
      const scan = ordersPayload.scan || {};
      el('ordersMeta').textContent = `${fmtInt(orders.length)} objednávok · hodnota ${fmtMoney((ordersPayload.summary || {}).fulfillable_value)}`;
      el('ordersScanMeta').textContent = `${fmtInt(scan.orders_scanned)} skenovaných objednávok, ${fmtInt(scan.pages_scanned)} strán, stop=${text(scan.stop_reason)}`;
      const limited = orders.slice(0, 24);
      el('ordersBody').innerHTML = limited.length ? limited.map((order) => orderRow(order)).join('') : '<tr><td colspan="6" class="muted">Žiadne vybaviteľné objednávky.</td></tr>';
      el('ordersFullBody').innerHTML = orders.length ? orders.map((order) => orderRow(order, true)).join('') : '<tr><td colspan="7" class="muted">Žiadne vybaviteľné objednávky.</td></tr>';
    }
    function renderPickups(data) {
      const pickups = ((data.orders || {}).personal_pickups) || [];
      el('pickupMeta').textContent = `${fmtInt(pickups.length)} osobných odberov`;
      el('pickupList').innerHTML = pickups.length ? pickups.map((order) => `
        <article class="pickup">
          <div class="pickup-top">
            <div><div class="pickup-title mono">${safe(order.order_num)}</div><div class="muted">${safe(order.purchase_at)} · ${safe(order.sum)}</div></div>
            <span class="badge ${badgeClass(order.status)}">${safe(order.status)}</span>
          </div>
          <div class="muted" style="margin-top:6px;">${safe((order.payment || {}).title)}</div>
          <div style="margin-top:8px;">${orderItemsHtml(order)}</div>
          <label class="checkline"><input type="checkbox" data-ship-pickup="${safe(order.order_num)}" ${order.pickup_action_allowed ? '' : 'disabled'}> Označiť ako odoslaná</label>
        </article>`).join('') : '<p class="muted">Žiadne osobné odbery.</p>';
      el('pickupList').querySelectorAll('[data-ship-pickup]').forEach((input) => input.addEventListener('change', () => markPickupShipped(input)));
    }
    function brandRevenueRow(row) {
      return `<tr><td>${compactProductCell(row)}</td><td>${fmtMoney(row.revenue)}</td><td>${fmtMoney(row.profit_with_fixed)}</td><td>${fmtPct(row.margin_with_fixed_pct)}</td></tr>`;
    }
    function brandProfitRow(row) {
      return `<tr><td>${compactProductCell(row)}</td><td>${fmtMoney(row.profit_with_fixed)}</td><td>${fmtMoney(row.revenue)}</td><td>${fmtPct(row.margin_with_fixed_pct)}</td></tr>`;
    }
    function productRevenueRow(row) {
      return `<tr><td>${compactProductCell(row)}</td><td>${fmtMoney(row.revenue)}</td><td>${fmtMoney(row.profit_with_fixed)}</td><td>${fmtQty(row.units)} ks</td></tr>`;
    }
    function productProfitRow(row) {
      return `<tr><td>${compactProductCell(row)}</td><td>${fmtMoney(row.profit_with_fixed)}</td><td>${fmtMoney(row.revenue)}</td><td>${fmtQty(row.units)} ks</td></tr>`;
    }
    function countryTopProducts(row) {
      const products = row.top_products || [];
      if (!products.length) return '<span class="muted">Top produkty zatiaľ nie sú dostupné.</span>';
      return `<div class="country-products">${products.slice(0, 5).map((product) => `
        <div class="country-product">
          <div><strong>${safe(product.product || '-')}</strong><div class="muted mono">${safe(product.sku || '')}</div></div>
          <span>${fmtMoney(product.revenue)}</span>
          <span class="${Number(product.profit_with_fixed || 0) < 0 ? 'negative' : 'positive'}">${fmtMoney(product.profit_with_fixed)}</span>
        </div>`).join('')}</div>`;
    }
    function countryPerformanceRow(row) {
      const spend = Number(row.spend ?? row.paid_ads_spend ?? 0);
      const netProfit = Number(row.profit_with_fixed ?? row.contribution_profit_with_fixed ?? row.contribution_profit ?? 0);
      const grossProfit = Number(row.gross_profit ?? row.profit_without_fixed ?? row.contribution_profit_without_fixed ?? 0);
      const margin = row.net_margin_pct ?? row.contribution_margin_with_fixed_pct ?? row.contribution_margin_pct;
      return `<tr>
        <td><strong>${safe(row.country_label || row.country || 'Unknown')}</strong><div class="muted mono">${safe(row.country || '')}</div></td>
        <td>${fmtInt(row.orders)}</td>
        <td>${fmtMoney(row.revenue)}</td>
        <td><span class="${grossProfit < 0 ? 'negative' : 'positive'}">${fmtMoney(grossProfit)}</span></td>
        <td>${fmtMoney(spend)}</td>
        <td><span class="${netProfit < 0 ? 'negative' : 'positive'}">${fmtMoney(netProfit)}</span></td>
        <td>${fmtPct(margin)}</td>
        <td>${countryTopProducts(row)}</td>
      </tr>`;
    }
    function lossProductRow(row) {
      const grossProfit = Number(row.gross_profit ?? row.cm1_profit ?? 0);
      const grossMargin = row.gross_margin_pct ?? (Number(row.revenue || 0) ? (grossProfit / Number(row.revenue || 0) * 100) : 0);
      return `<tr>
        <td>${compactProductCell(row)}</td>
        <td>${fmtMoney(row.revenue)}</td>
        <td><span class="${grossProfit < 0 ? 'negative' : 'neutral'}">${fmtMoney(grossProfit)}</span></td>
        <td>${fmtPct(grossMargin)}</td>
        <td><label class="checkline" style="margin-top:0;"><input type="checkbox" data-ack-loss="${safe(row.sku)}" data-product="${safe(row.product)}"> viem</label></td>
      </tr>`;
    }
    function renderPerformance(data) {
      const perf = data.performance || {};
      const brandRevenue = perf.brand_revenue_rows || [];
      const brandProfit = perf.brand_profit_rows || [];
      const productRevenue = perf.product_revenue_rows || [];
      const productProfit = perf.product_profit_rows || [];
      const countries = perf.country_rows || [];
      const losses = perf.loss_product_rows || [];
      el('brandPerformanceMeta').textContent = `${fmtInt(brandRevenue.length)} podľa obratu · ${fmtInt(brandProfit.length)} podľa zisku`;
      el('brandRevenueBody').innerHTML = brandRevenue.length ? brandRevenue.map(brandRevenueRow).join('') : '<tr><td colspan="4" class="muted">Značky zatiaľ nie sú dostupné.</td></tr>';
      el('brandProfitBody').innerHTML = brandProfit.length ? brandProfit.map(brandProfitRow).join('') : '<tr><td colspan="4" class="muted">Značky zatiaľ nie sú dostupné.</td></tr>';
      el('productPerformanceMeta').textContent = `${fmtInt(productRevenue.length)} podľa obratu · ${fmtInt(productProfit.length)} podľa zisku`;
      el('productRevenueBody').innerHTML = productRevenue.length ? productRevenue.map(productRevenueRow).join('') : '<tr><td colspan="4" class="muted">Produkty zatiaľ nie sú dostupné.</td></tr>';
      el('productProfitBody').innerHTML = productProfit.length ? productProfit.map(productProfitRow).join('') : '<tr><td colspan="4" class="muted">Produkty zatiaľ nie sú dostupné.</td></tr>';
      el('countryPerformanceMeta').textContent = countries.length ? `${fmtInt(countries.length)} krajín podľa obratu` : 'Krajiny zatiaľ nie sú dostupné';
      el('countryPerformanceBody').innerHTML = countries.length ? countries.map(countryPerformanceRow).join('') : '<tr><td colspan="8" class="muted">Krajiny zatiaľ nie sú dostupné.</td></tr>';
      const hidden = Number(perf.acknowledged_loss_product_count || 0);
      el('lossProductMeta').textContent = losses.length ? `${fmtInt(losses.length)} nepotvrdených · ${fmtInt(hidden)} potvrdených skrytých` : `${fmtInt(hidden)} potvrdených skrytých`;
      el('lossProductBody').innerHTML = losses.length ? losses.map(lossProductRow).join('') : '<tr><td colspan="5" class="muted">Bez nepotvrdených stratových produktov.</td></tr>';
      el('lossProductBody').querySelectorAll('[data-ack-loss]').forEach((input) => input.addEventListener('change', () => acknowledgeLossProduct(input)));
    }
    function inventoryAlertRow(row) {
      return `<tr>
        <td><strong>${safe(row.product)}</strong><div class="muted mono">${safe(row.sku)}</div></td>
        <td><span class="badge ${badgeClass(row.stock_risk_level || row.reorder_action_label)}">${safe(row.stock_risk_level || row.reorder_action_label)}</span></td>
        <td>${fmtQty(row.available_quantity)} ks</td>
        <td>${inboundControls(row)}</td>
        <td>${fmtQty(row.alert_30d_units)} ks</td>
        <td>${row.days_of_cover === null || row.days_of_cover === undefined ? 'N/A' : `${fmtQty(row.days_of_cover)} dní`}</td>
        <td>${safe(row.projected_stockout_date)}</td>
        <td>${safe(row.reorder_by_date)}</td>
        <td><strong>${safe(row.reorder_action_label)}</strong><div class="muted">${fmtQty(row.suggested_reorder_units)} ks · LT ${fmtInt(row.lead_time_working_days)}d</div></td>
      </tr>`;
    }
    function renderInventory(data) {
      const inv = data.inventory || {};
      const summary = inv.summary || {};
      const alerts = inv.alert_rows || [];
      const inventoryRows = inv.inventory_rows || [];
      const inboundRows = inv.inbound_order_rows || [];
      const visibleInventoryAlertLimit = 100;
      const visibleInventoryLimit = 100;
      el('inventoryAlertMeta').textContent = `${fmtInt(summary.alert_delivery_count)} alertov · snapshot ${text(summary.inventory_snapshot_date)}`;
      el('alertRowsBody').innerHTML = alerts.length ? alerts.slice(0, visibleInventoryAlertLimit).map(inventoryAlertRow).join('') : '<tr><td colspan="9" class="muted">Bez kritických skladových alertov.</td></tr>';
      el('inventoryMeta').textContent = `${fmtInt(summary.inventory_products_with_stock)} produktov so skladom · coverage ${fmtPct(summary.inventory_cost_coverage_units_pct)}`;
      el('inventorySummaryGrid').innerHTML = [
        metric('Nákupná hodnota bez DPH', fmtMoney(summary.inventory_cost_value), `${fmtQty(summary.inventory_available_units)} ks skladom`),
        metric('Predajná hodnota bez DPH', fmtMoney(summary.inventory_retail_value), `coverage ${fmtPct(summary.inventory_cost_coverage_retail_pct)}`),
        metric('45d watchlist', fmtInt(summary.stock_risk_45d_count), `${fmtInt(summary.out_of_stock_recent_demand_count)} vypredané s dopytom`),
        metric('Dead stock', fmtMoney(summary.dead_stock_cost_value), `${fmtInt(summary.dead_stock_count)} položiek`),
        metric('Tržby v riziku', fmtMoney(summary.revenue_at_risk_30d), `zisk ${fmtMoney(summary.profit_at_risk_30d)}`),
        metric('Inbound objednávky', fmtQty(summary.inbound_ordered_units), `${fmtInt(summary.inbound_order_count)} položiek · ETA ${text(summary.inbound_next_arrival_date)}`),
      ].join('');
      el('inboundRowsBody').innerHTML = inboundRows.length ? inboundRows.map((row) => `<tr>
        <td><strong>${safe(row.product)}</strong><div class="muted mono">${safe(row.sku)}</div></td>
        <td>${fmtQty(row.ordered_units)} ks</td>
        <td>${safe(row.expected_arrival_date)}</td>
        <td>${fmtQty(row.baseline_available_quantity)} ks</td>
        <td>${fmtQty(row.current_available_quantity)} ks</td>
        <td><button type="button" data-clear-inbound="${safe(row.sku)}">Zrušiť</button></td>
      </tr>`).join('') : '<tr><td colspan="6" class="muted">Žiadne ručne zadané inbound objednávky.</td></tr>';
      el('inventoryRowsBody').innerHTML = inventoryRows.length ? inventoryRows.slice(0, visibleInventoryLimit).map((row) => `<tr>
        <td><strong>${safe(row.product)}</strong><div class="muted mono">${safe(row.sku)}</div></td>
        <td>${fmtQty(row.available_quantity)} ks</td>
        <td>${fmtMoney(row.inventory_cost_value)}</td>
        <td>${fmtMoney(row.inventory_retail_value)}</td>
        <td>${row.days_of_cover === null || row.days_of_cover === undefined ? 'N/A' : `${fmtQty(row.days_of_cover)} dní`}</td>
        <td>${safe(row.projected_stockout_date)}</td>
        <td>${fmtInt(row.lead_time_working_days)} pracovných dní</td>
        <td>${inboundStatus(row)}</td>
        <td>${safe(row.reorder_action_label)}<div class="muted">${safe(row.reorder_by_date)} · ${fmtQty(row.suggested_reorder_units)} ks</div></td>
      </tr>`).join('') : '<tr><td colspan="9" class="muted">Skladový payload zatiaľ nie je dostupný.</td></tr>';
      document.querySelectorAll('[data-save-inbound]').forEach((button) => button.addEventListener('click', () => saveInboundOrder(button)));
      document.querySelectorAll('[data-clear-inbound]').forEach((button) => button.addEventListener('click', () => clearInboundOrder(button)));
    }
    function showMessage(message, ok=false) {
      el('messageBox').className = ok ? 'ok' : 'error';
      el('messageBox').textContent = message;
    }
    function clearMessage() { el('messageBox').className = 'hidden'; el('messageBox').textContent = ''; }
    function render(data) {
      latestData = data;
      const cache = data.cache || {};
      el('subtitle').textContent = `Posledná aktualizácia ${text(data.generated_at)}. Auto refresh ${fmtInt(data.auto_refresh_seconds || 90)}s.`;
      el('cacheBadge').textContent = `${text(cache.status, 'live')} ${cache.age_seconds !== undefined ? `${cache.age_seconds}s` : ''}`;
      el('cacheBadge').className = `badge ${cache.status === 'stale_after_error' ? 'bad' : 'info'}`;
      renderAlerts(data);
      renderKpis();
      renderOrders(data);
      renderPickups(data);
      renderInventory(data);
      renderPerformance(data);
      notifyAboutNewFulfillableOrders(data);
    }
    async function loadDashboard(force=false) {
      el('refreshBtn').disabled = true;
      try {
        const response = await fetch(`/api/operations/${encodeURIComponent(project)}/live${force ? '?refresh=1' : ''}`, { cache:'no-store' });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
        clearMessage();
        render(data);
        if (refreshTimer) clearInterval(refreshTimer);
        refreshTimer = setInterval(() => loadDashboard(false), Math.max(30, Number(data.auto_refresh_seconds || 90)) * 1000);
      } catch (error) {
        showMessage(error instanceof Error ? error.message : String(error));
      } finally {
        el('refreshBtn').disabled = false;
      }
    }
    async function markPickupShipped(input) {
      const orderNum = input.dataset.shipPickup;
      if (!window.confirm(`Zmeniť objednávku ${orderNum} v eshope na Odoslaná?`)) {
        input.checked = false;
        return;
      }
      input.disabled = true;
      try {
        const response = await fetch(`/api/operations/${encodeURIComponent(project)}/pickup/${encodeURIComponent(orderNum)}/ship`, { method:'POST', cache:'no-store' });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
        showMessage(`Objednávka ${orderNum} je zmenená na Odoslaná.`, true);
        await loadDashboard(true);
      } catch (error) {
        input.checked = false;
        input.disabled = false;
        showMessage(error instanceof Error ? error.message : String(error));
      }
    }
    async function saveInboundOrder(button) {
      const sku = button.dataset.saveInbound;
      const unitsInput = document.querySelector(`[data-inbound-units="${cssEscape(sku)}"]`);
      const etaInput = document.querySelector(`[data-inbound-eta="${cssEscape(sku)}"]`);
      const units = Number(unitsInput ? unitsInput.value : 0);
      const eta = etaInput ? etaInput.value : '';
      if (!units || units <= 0 || !eta) {
        showMessage('Zadaj počet objednaných kusov aj očakávaný dátum príchodu.');
        return;
      }
      button.disabled = true;
      try {
        const response = await fetch(`/api/operations/${encodeURIComponent(project)}/inbound/${encodeURIComponent(sku)}`, {
          method:'POST',
          cache:'no-store',
          headers:{ 'Content-Type':'application/json' },
          body: JSON.stringify({
            product: button.dataset.product || '',
            ordered_units: units,
            expected_arrival_date: eta,
            baseline_available_quantity: Number(button.dataset.baseline || 0),
          }),
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
        showMessage(`Inbound objednávka pre ${sku} je uložená.`, true);
        await loadDashboard(true);
      } catch (error) {
        showMessage(error instanceof Error ? error.message : String(error));
      } finally {
        button.disabled = false;
      }
    }
    async function clearInboundOrder(button) {
      const sku = button.dataset.clearInbound;
      button.disabled = true;
      try {
        const response = await fetch(`/api/operations/${encodeURIComponent(project)}/inbound/${encodeURIComponent(sku)}/clear`, { method:'POST', cache:'no-store' });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
        showMessage(`Inbound objednávka pre ${sku} je zrušená.`, true);
        await loadDashboard(true);
      } catch (error) {
        showMessage(error instanceof Error ? error.message : String(error));
      } finally {
        button.disabled = false;
      }
    }
    async function acknowledgeLossProduct(input) {
      const sku = input.dataset.ackLoss;
      input.disabled = true;
      try {
        const response = await fetch(`/api/operations/${encodeURIComponent(project)}/loss-product/${encodeURIComponent(sku)}/ack`, {
          method:'POST',
          cache:'no-store',
          headers:{ 'Content-Type':'application/json' },
          body: JSON.stringify({ product: input.dataset.product || '' }),
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
        showMessage(`Produkt ${sku} je potvrdený a skrytý zo stratových upozornení.`, true);
        await loadDashboard(true);
      } catch (error) {
        input.checked = false;
        input.disabled = false;
        showMessage(error instanceof Error ? error.message : String(error));
      }
    }
    el('refreshBtn').addEventListener('click', () => loadDashboard(true));
    initializeOrderSound();
    document.querySelectorAll('[data-view]').forEach((button) => button.addEventListener('click', () => {
      document.querySelectorAll('[data-view]').forEach((btn) => btn.classList.toggle('active', btn === button));
      ['overview','orders','inventory'].forEach((view) => el(`view-${view}`).classList.toggle('hidden', button.dataset.view !== view));
    }));
    loadDashboard(false);
  </script>
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

    def _send_download(self, body: bytes, *, content_type: str, filename: str, status: int = 200) -> None:
        ascii_filename = "".join(ch if ch.isalnum() or ch in ".-_" else "_" for ch in filename) or "download.pdf"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header(
            "Content-Disposition",
            f"attachment; filename=\"{ascii_filename}\"; filename*=UTF-8''{quote(filename)}",
        )
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

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length") or "0")
        if length <= 0:
            return {}
        if length > 32_768:
            raise ValueError("Request body is too large.")
        raw = self.rfile.read(length)
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("JSON request body must be an object.")
        return payload

    def _send_auth_required(self) -> None:
        body = b"Authentication required."
        self.send_response(401)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("WWW-Authenticate", 'Basic realm="BiznisWeb reporting", charset="UTF-8"')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path.rstrip("/") or "/"
        if path != "/health" and not is_authorized_basic_header(
            self.headers.get("Authorization"),
            live_dashboard_auth_credentials(),
        ):
            self._send_auth_required()
            return

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

        if len(parts) == 4 and parts[0] == "api" and parts[1] == "operations" and parts[3] == "live":
            project = parts[2]
            if project not in projects:
                self._send_json({"error": f"Unknown project '{project}'."}, status=404)
                return
            if project != "roy":
                self._send_json({"error": f"Operations dashboard is not enabled for '{project}'."}, status=404)
                return
            try:
                operations_settings = resolve_roy_operations_settings(load_project_settings(project))
            except Exception as exc:
                self._send_json({"error": f"Failed to load ROY operations settings: {exc}"}, status=500)
                return
            if not operations_settings["enabled"]:
                self._send_json({"error": f"Operations dashboard is not enabled for '{project}'."}, status=404)
                return

            force_refresh = (query.get("refresh", [""])[0] or "").strip().lower() in {"1", "true", "yes"}
            try:
                self._send_json(
                    get_cached_roy_operations_snapshot(
                        project,
                        report_payload=read_latest_dashboard_payload(project),
                        force_refresh=force_refresh,
                    )
                )
            except Exception as exc:
                self._send_json({"error": f"Failed to load ROY operations data: {exc}"}, status=500)
            return

        if len(parts) == 4 and parts[0] == "api" and parts[1] == "operations" and parts[3] == "picking-lists.pdf":
            project = parts[2]
            if project not in projects:
                self._send_text(f"Unknown project '{escape(project)}'.", content_type="text/plain; charset=utf-8", status=404)
                return
            if project != "roy":
                self._send_text(
                    f"Operations dashboard is not enabled for '{escape(project)}'.",
                    content_type="text/plain; charset=utf-8",
                    status=404,
                )
                return
            try:
                operations_settings = resolve_roy_operations_settings(load_project_settings(project))
            except Exception as exc:
                self._send_text(
                    f"Failed to load ROY operations settings: {escape(str(exc))}",
                    content_type="text/plain; charset=utf-8",
                    status=500,
                )
                return
            if not operations_settings["enabled"]:
                self._send_text(
                    f"Operations dashboard is not enabled for '{escape(project)}'.",
                    content_type="text/plain; charset=utf-8",
                    status=404,
                )
                return
            force_refresh = (query.get("refresh", ["1"])[0] or "").strip().lower() not in {"0", "false", "no"}
            try:
                payload = get_cached_roy_operations_snapshot(
                    project,
                    report_payload=read_latest_dashboard_payload(project),
                    force_refresh=force_refresh,
                )
                orders = ((payload.get("orders") or {}).get("orders") or [])
                pdf = build_roy_picking_lists_pdf(orders)
                filename = build_roy_picking_lists_filename(orders)
                self._send_download(pdf, content_type="application/pdf", filename=filename)
            except Exception as exc:
                self._send_text(
                    f"Failed to generate picking lists PDF: {escape(str(exc))}",
                    content_type="text/plain; charset=utf-8",
                    status=500,
                )
            return

        if len(parts) == 4 and parts[0] == "api" and parts[1] == "production" and parts[3] == "live":
            project = parts[2]
            if project not in projects:
                self._send_json({"error": f"Unknown project '{project}'."}, status=404)
                return

            try:
                board_settings = resolve_production_board_settings(load_project_settings(project))
            except Exception as exc:
                self._send_json({"error": f"Failed to load production board settings: {exc}"}, status=500)
                return
            if not board_settings["enabled"]:
                self._send_json({"error": f"Production board is not enabled for '{project}'."}, status=404)
                return

            force_refresh = (query.get("refresh", [""])[0] or "").strip().lower() in {"1", "true", "yes"}
            try:
                self._send_json(get_cached_production_board_snapshot(project, force_refresh=force_refresh))
            except Exception as exc:
                self._send_json({"error": f"Failed to load production board data: {exc}"}, status=500)
            return

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

        if len(parts) == 2 and parts[0] == "production":
            project = parts[1]
            if project not in projects:
                self._send_text(f"Unknown project '{escape(project)}'.", content_type="text/plain; charset=utf-8", status=404)
                return
            try:
                board_settings = resolve_production_board_settings(load_project_settings(project))
            except Exception as exc:
                self._send_text(
                    f"Failed to load production board settings: {escape(str(exc))}",
                    content_type="text/plain; charset=utf-8",
                    status=500,
                )
                return
            if not board_settings["enabled"]:
                if project == "roy":
                    try:
                        operations_settings = resolve_roy_operations_settings(load_project_settings(project))
                    except Exception as exc:
                        self._send_text(
                            f"Failed to load ROY operations settings: {escape(str(exc))}",
                            content_type="text/plain; charset=utf-8",
                            status=500,
                        )
                        return
                    if operations_settings["enabled"]:
                        self._send_text(build_roy_operations_dashboard_html(project), content_type="text/html; charset=utf-8")
                        return
                self._send_text(
                    f"Production board is not enabled for '{escape(project)}'.",
                    content_type="text/plain; charset=utf-8",
                    status=404,
                )
                return
            self._send_text(build_production_board_html(project), content_type="text/html; charset=utf-8")
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

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if not is_authorized_basic_header(
            self.headers.get("Authorization"),
            live_dashboard_auth_credentials(),
        ):
            self._send_auth_required()
            return

        projects = available_projects()
        parts = [part for part in path.split("/") if part]
        if (
            len(parts) == 6
            and parts[0] == "api"
            and parts[1] == "operations"
            and parts[3] == "pickup"
            and parts[5] == "ship"
        ):
            project = parts[2]
            order_num = unquote(parts[4])
            if project not in projects:
                self._send_json({"error": f"Unknown project '{project}'."}, status=404)
                return
            try:
                self._send_json(mark_personal_pickup_shipped(project, order_num))
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if (
            len(parts) == 5
            and parts[0] == "api"
            and parts[1] == "operations"
            and parts[3] == "inbound"
        ):
            project = parts[2]
            sku = unquote(parts[4])
            if project not in projects:
                self._send_json({"error": f"Unknown project '{project}'."}, status=404)
                return
            try:
                body = self._read_json_body()
                self._send_json(
                    set_inbound_stock_order(
                        project,
                        sku,
                        product=str(body.get("product") or ""),
                        ordered_units=body.get("ordered_units"),
                        expected_arrival_date=body.get("expected_arrival_date"),
                        baseline_available_quantity=body.get("baseline_available_quantity", 0),
                    )
                )
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if (
            len(parts) == 6
            and parts[0] == "api"
            and parts[1] == "operations"
            and parts[3] == "inbound"
            and parts[5] == "clear"
        ):
            project = parts[2]
            sku = unquote(parts[4])
            if project not in projects:
                self._send_json({"error": f"Unknown project '{project}'."}, status=404)
                return
            try:
                self._send_json(clear_inbound_stock_order(project, sku))
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        if (
            len(parts) == 6
            and parts[0] == "api"
            and parts[1] == "operations"
            and parts[3] == "loss-product"
            and parts[5] == "ack"
        ):
            project = parts[2]
            sku = unquote(parts[4])
            if project not in projects:
                self._send_json({"error": f"Unknown project '{project}'."}, status=404)
                return
            try:
                body = self._read_json_body()
                self._send_json(acknowledge_loss_product(project, sku, product=str(body.get("product") or "")))
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return

        self._send_json({"error": "Not found."}, status=404)


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
