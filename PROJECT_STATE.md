# PROJECT_STATE

Last updated: 2026-04-11
Owner: Patrik
Repository scope: BizniWeb reporting only
Purpose: repo-scoped handoff and execution state for this codebase.

## 0) How To Use This File

- This file is authoritative only for this repository.
- Do not use it as a shared state file for Doklady or OpenClaw.
- External projects may be mentioned only as integration notes.
- Update this file after each major implementation, deploy-relevant change, or workflow change.

## 1) Repository Purpose

- Product type: reporting/export automation for BizniWeb-based clients
- Current active clients in repo: VEVO, ROY
- Main responsibilities:
  - export orders from BizniWeb GraphQL API
  - generate invoice-related artifacts
  - build daily reports
  - optional Google Ads / Facebook Ads enrichment
  - scheduled email report delivery via SES/S3

## 2) Source Of Truth Rules

- GitHub is the only source of truth for code.
- No required script may live only on one local PC.
- No required runtime/deploy flow may depend on Desktop/Downloads files.
- Secrets stay outside git (`.env`, runtime env, AWS secrets).
- Every machine must be able to bootstrap from this repository alone.

## 3) Current Branching / Workflow Rules

- Reporting work stays on `main`
- OpenClaw work was moved out to the standalone `openclaw-agents-platform` repository
- `main` only through reviewed merge
- Before work: `git fetch --all --prune && git pull --rebase`
- After major step: commit + push immediately
- No force-push on shared branches

## 4) Environment Baseline

Required baseline keys:
- `BIZNISWEB_API_TOKEN`
- `BIZNISWEB_API_URL`

Enforcement in repo:
- `.env.required`
- `.githooks/pre-commit`
- `.github/workflows/env-check.yml`
- `scripts/check_env.sh`
- `scripts/check_env.ps1`

Bootstrap entrypoints:
- `scripts/bootstrap.sh`
- `scripts/bootstrap.ps1`

## 5) Current Verified State

- Env governance added for multi-PC workflow
- Pre-commit hook install script exists for Bash and PowerShell
- CI validates env contract and blocks tracked secret env files
- Repo-scoped `PROJECT_STATE.md` exists
- Bootstrap scripts now exist for macOS/Linux and Windows PowerShell
- VEVO ECS schedule `vevo-daily-report-email` is enabled for `01:00 Europe/Bratislava`
- VEVO production task definition `vevo-reporting-daily:3` uses full-history runtime range from `2025-05-03` to `yesterday`
- VEVO task role CloudWatch metric policy now allows the active namespace `BizniswebReporting` (and keeps backward-compatible `VevoReporting`)
- Manual ECS production-equivalent run succeeded on `2026-04-03` with:
  - HTML report saved as `data/vevo/report_20250503-20260402.html`
  - SES delivery confirmed in CloudWatch logs
  - no remaining `PutMetricData` warning in the verified log stream
- Fixed `html_report_generator.py` period-switcher syntax so `Env Check` / `reporting_qa_smoke.py` pass again on GitHub Actions and on local Python 3.11.

## 6) Integration Notes (External Systems)

### Doklady
- Integration is API-level only
- Doklady remains system-of-record for accounting document state
- Do not store Doklady runtime assumptions here beyond API contract references

### OpenClaw
- OpenClaw runs on separate infrastructure
- Any launcher/tunnel helper must live in the OpenClaw repo, not here
- This repo should only keep reporting-side integration notes, not server-specific local launcher paths

## 7) Current Risks / Gaps

- README is still primarily product/user oriented, not full operator documentation
- No formal API contract package yet for cross-project integrations
- No container/bootstrap parity check in CI yet
- Runtime/deploy docs for separate OpenClaw infra still belong in another repo and are not defined there yet
- Partial upstream failures (ads/weather/etc.) now surface explicit source-health metadata in HTML/CFO outputs and JSON sidecars; downstream email/ops policy still needs alert tightening.
- Main production HTML report now uses the modern dashboard shell.
- Standalone CFO HTML output was removed from the artifact contract and daily email flow; CFO KPI logic now lives only inside the main report.
- Daily SES email now attaches only the main HTML report.
- Legacy `__test` and `__test2` artifacts are no longer part of the active workflow.
- Env Check CI baseline now validates partial-data rendering in the active HTML layer (`html_report_generator.py` / `dashboard_modern.py`) instead of the retired daily runner rendering path.
- Production dashboard now keeps `Executive KPI deck` on its own `Daily / Weekly / Monthly` switch while the rest of the report uses a separate global analytics window switcher in the sidebar.
- Period bundle generation is enabled for plain production reports, so the sidebar analytics switch now works outside of test-tag exports too.
- Shipping semantics are now normalized to net shipping in runtime config and dashboard labels, but CM taxonomy naming is still mixed between legacy labels and CM1/CM2/CM3 terms in some views.
- Full QA assertions are now computed into `data_quality` sidecars and surfaced in dashboard/email/CloudWatch.
- Lifecycle remains a proxy because BiznisWeb reporting still exposes only current/final status, not full order-status history.
- Segment CAC/payback is still incomplete as a hard metric because payment fees and order-level attribution are not modeled deeply enough for final B2B/B2C CAC claims.
- Vevo growth model blocks are now wired into the active dashboard shell:
  - direct vs assisted profitability
  - CRM funnel KPI layer
  - scent-size refill matrix
  - bundle recommender
  - promo / discount quality
- Lifecycle is now visible as an explicit proxy layer built from final statuses plus tracked excluded payment-failure orders.
- B2B/B2C analytics now expose CM-based unit economics instead of only a raw revenue/profit split.
- Product cost coverage QA is now active in source-health and the modern dashboard:
  - VEVO March 2026 export now passes with `0.00%` fallback revenue share after re-importing the April 2026 Excel costs and restoring title-first / alias-aware expense matching
  - ROY March 2026 export is `warning` because fallback coverage still touches 3.20% of item revenue and 6.26% of pre-ad item profit
- VEVO now resolves ambiguous shared-EAN fragrance SKUs by exact item label / compound key before identifier fallback, so Natural vs Premium 500ml/200ml lines no longer collapse onto the same cost.

## 8) Next Exact Step

- Merge the restored VEVO April-cost pipeline into the production path (`main`) and then continue with the remaining ROY fallback-cost cleanup / payment-fee hardening.

## 9) Change Log

### 2026-03-30
- Added env governance baseline: `.env.required`, pre-commit hook, CI env check.
- Added cross-platform bootstrap scripts for macOS/Linux and Windows PowerShell.
- Narrowed `PROJECT_STATE.md` to this repository only.
- Removed cross-project state ownership from this repo; left only integration notes.

### 2026-03-31
- Completed `P3.1` reusable reporting core foundation:
  - added package `reporting_core/` as the shared source of truth for project config + runtime loading,
  - moved project config helpers behind `reporting_core.config` and kept `project_config.py` as a backward-compatible shim,
  - added `reporting_core.runtime` with `ProjectRuntime` and reusable runtime application/loading helpers,
  - added `reporting_core.contracts` with `ReportingArtifactSet` + canonical output artifact builder,
  - switched `export_orders.py`, `daily_report_runner.py`, and `generate_invoices.py` to import from `reporting_core`,

### 2026-04-10
- Added Roy bundle/accessory model as a first-class advanced DTC metric using project-configured anchor device families and accessory groups.
- Bundle/accessory outputs now include pair-level attach rate and contribution uplift, device family summary, and accessory group quality summary.
- Modern dashboard now renders Roy bundle/accessory charts in the Products/Operations library without changing the current production shell.
- Verified with real Roy March 2026 export: HTML report generated successfully and new bundle/accessory chart IDs are present in the rendered output.
  - updated daily runner to consume the shared artifact contract instead of rebuilding output paths ad hoc,
  - verified syntax with `python -m py_compile export_orders.py daily_report_runner.py generate_invoices.py project_config.py reporting_core\\__init__.py reporting_core\\config.py reporting_core\\runtime.py reporting_core\\contracts.py`,
  - verified ROY smoke export on `2026-03-01..2026-03-02`,
  - verified project-aware daily runner on `2026-03-01..2026-03-02` with `--skip-export --skip-email`.
- Added new Week-of-Month analytics (Week 1-4) into reporting pipeline in export_orders.py.
- Wired Week-of-Month outputs into HTML report generation (html_report_generator.py) with 2 charts and performance table.
- Added aggregation for week-level pattern visibility: orders, revenue, profit, margin, AOV, avg daily revenue/profit, active days/months.

### 2026-04-03
- Verified VEVO production scheduler and runtime wiring end-to-end on AWS:
  - Scheduler `vevo-daily-report-email` remains enabled at `01:00 Europe/Bratislava`
  - ECS cluster `vevo-reporting-cluster`
  - Task definition `vevo-reporting-daily:3`
  - Image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
- Confirmed runtime secret `vevo/reporting/runtime-env` still points to:
  - `REPORT_FROM_DATE=2025-05-03`
  - `REPORT_PROJECT=vevo`
  - `REPORT_EMAIL_TO=mil.terem@gmail.com,vzeman@gmail.com,maker.martuska@gmail.com`
- Fixed VEVO task-role CloudWatch metric permission drift:
  - previous inline IAM policy allowed only namespace `VevoReporting`
  - runtime writes metrics into `BizniswebReporting`
  - updated inline policy `vevo-reporting-put-metrics` to allow both namespaces
- Re-ran a manual ECS production-equivalent task and verified in `/ecs/vevo-reporting-daily` log stream:
  - `HTML report saved: data/vevo/report_20250503-20260402.html`
  - `SES message sent`
  - no `WARN: failed to publish CloudWatch metric ... PutMetricData`
- Verified syntax via python -m py_compile export_orders.py html_report_generator.py.
- Revised Week-of-Month methodology to remove day-count bias:
  - uses only days 1-28 (4x7 equal windows),

### 2026-04-03
- Retired the standalone `test2` dashboard variant after promoting it to production:
  - renamed `dashboard_test2.py` to `dashboard_modern.py`,
  - removed `test2` from the HTML renderer dispatch variants,
  - updated security CI to validate the renamed production dashboard module,
  - cleaned local `__test2` artifacts from the active workspace.
- Fixed false-positive `Env Check` / `security-baseline` CI failure after promoting the modern dashboard renderer:
  - `scripts/security_ci.py` no longer expects the `Partial Data` marker inside `daily_report_runner.py`,
  - CI now validates partial-data rendering in the actual HTML layer (`html_report_generator.py` and `dashboard_modern.py`),
  - retained a runner-level assertion that the main HTML report artifact is still attached by `daily_report_runner.py`.
- Verified locally with:
  - `python scripts/security_ci.py`
  - `python -m py_compile scripts/security_ci.py`
- Added modern timeframe UX split for the new production dashboard:
  - `Executive KPI deck` keeps its own independent `Daily / Weekly / Monthly` toggle,
  - all non-KPI chart sections now use a global sidebar `Analytics window` switch,
  - global period links preserve the currently active section anchor while switching report variant,
  - plain production reports now generate the `_periods/...` bundle needed for the sidebar switcher (previously this existed only for tagged/test outputs).
- Verified locally with:
  - `python -m py_compile dashboard_test2.py export_orders.py html_report_generator.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - uses full months only (drops partial first/last month for this metric),
  - daily normalization uses calendar_days (includes zero-order days).
- Added fairness diagnostics in table: `Calendar Days` and `Active Day Rate`.
- Added new Day-of-Month analytics (1-31) to reporting pipeline:

### 2026-04-10 (cohort-normalized unit economics)
- Added cohort-normalized CAC / LTV / payback views into `export_orders.py` so acquisition cohorts can be compared on mature horizons instead of only via global blended shortcuts.
- New cohort unit economics payload now exports, per acquisition cohort:
  - blended and FB CAC
  - 30/60/90/180-day revenue LTV
  - 30/60/90/180-day contribution LTV
  - 30/60/90/180-day contribution LTV/CAC
  - 30/60/90/180-day CAC recovery %
  - average and median payback days by horizon
- Added mature weighted summary fields for cohort-normalized contribution LTV/CAC and payback recovery into the advanced DTC summary layer.
- Wired the new cohort payload into `dashboard_modern.py` and added three customer analytics charts:
  - `custCohortContributionLtvCacChart`
  - `custCohortPaybackRecoveryChart`
  - `custCohortCacVsContributionChart`
- Added null-safe rendering for immature cohort horizons so missing maturity now renders as gaps instead of fake zeroes.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project vevo --from-date 2025-05-03 --to-date 2026-04-09`

### 2026-04-10 (geo confidence guardrails)
- Added project-level `geo_confidence` settings for VEVO and ROY with separate country/city thresholds.
- Export layer now computes confidence metadata per country/city:

### 2026-04-11
- Restored the missing VEVO April 2026 cost pipeline inside the active reporting line instead of the stale side branch:
  - added repo-local Excel importer `scripts/import_product_expenses_excel.py`,
  - imported the latest VEVO workbook from `D:\product_expense_rebuild_20250503-20260407 (4).xlsx`,
  - added `projects/vevo/product_name_aliases.json`,
  - enabled VEVO `expense_match_mode = title_first` in `projects/vevo/settings.json`,
  - extended `reporting_core.runtime` to load `expense_match_mode` and alias files,
  - extended `export_orders.py` to resolve costs by exact label / compound key before shared EAN fallback and to canonicalize VEVO reporting identities for analytics.
- Verified on fresh March 2026 exports:
  - VEVO `Parfum do prania Vevo Natural No.07 Ylang Absolute (500ml)` now uses `6.14 EUR`,
  - VEVO `Parfum do prania Vevo Premium No.07 Ylang Absolute (500ml)` now uses `13.9 EUR`,
  - VEVO `Parfum do prania Vevo Premium No.09 Pure Garden (500ml)` now uses `14.36 EUR`,
  - VEVO `Parfum do prania Vevo Premium No.08 Cotton Dream (200ml)` resolves via compound key at `6.69 EUR`,
  - VEVO product cost fallback share is now `0.00%` revenue / `0.00%` profit for March 2026.
- Verified locally with:
  - `python -m py_compile export_orders.py reporting_core/runtime.py dashboard_modern.py html_report_generator.py scripts/security_ci.py scripts/import_product_expenses_excel.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - `python scripts/security_ci.py`
  - `python scripts/reporting_qa_smoke.py`
  - `confidence_status`
  - `confidence_score`
  - `low_sample`
  - `hide_economics`
- Geo profitability output now exposes guarded fields:
  - `contribution_profit_guarded`
  - `contribution_margin_pct_guarded`
  - `fb_cpo_guarded`
- Source health now includes `qa.geo` summary with:
  - ready / observe / ignore counts
  - unknown country rate
  - warning list
- Modern dashboard geography section now renders:
  - Geo confidence guardrails panel
  - confidence badges for country rows
  - guarded geo profitability chart/table values (`N/A` on low-sample markets)

### 2026-04-11 (QA assertions + shipping semantics)
- Verified shared QA assertion layer end-to-end on real March 2026 VEVO and ROY exports.
- Export layer now computes `qa.assertions` with:
  - shell/library parity checks for critical economics metrics,
  - refund binding presence,
  - platform/attributed CPA arithmetic mismatch detection,
  - attributed orders tolerance checks,
  - missing dimension counts (`day_name`, `anchor_item`, `attached_item`, `anchor_orders`, `country`),
  - `null_label_rate_pct`, `qa_failure_count`, `qa_warning_count`.
- Daily runner now includes data-quality summary in email body and publishes CloudWatch QA metrics:
  - `ReportQaWarnings`
  - `ReportQaFailures`
  - `ReportQaCritical`
  - `ReportPartialData`
- Modern dashboard now renders both failure and warning assertion blocks plus richer geo confidence share cards.
- Shipping terminology was normalized from subsidy-style wording to `Net shipping` / `shipping_net_cost` in config, export, and dashboard labels.
- Verified locally with:
  - `python -m py_compile export_orders.py dashboard_modern.py daily_report_runner.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Added CI guardrails so geo QA metadata and the dashboard geo-confidence panel cannot disappear silently.
- Verification target:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`

### 2026-04-11 (product cost coverage QA)
- Added explicit `expense_source` tagging on item rows so item-level costs are classified as:
  - mapped product SKU
  - mapped item label
  - configured overrides
  - default 1.00 EUR fallback
- Added `qa.product_expense_coverage` into `source_health` / `data_quality` sidecars with:
  - fallback row/unit/revenue/profit shares
  - top fallback items by impact
  - expense-source mix summary
- Modern dashboard now renders:
  - `Product cost coverage`
  - `Expense source mix`
  - `Top default-cost items`
- Verified locally with:
  - `python -m py_compile export_orders.py dashboard_modern.py html_report_generator.py scripts\\reporting_qa_smoke.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python scripts\\reporting_qa_smoke.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Current decision on payment fees:
  - keep `excluded_not_modeled` for now
  - current `ORDER_QUERY` still does not ingest any payment-fee / payment-method fee field from BiznisWeb
  - next safe step is to fix product-cost coverage first, then decide whether fees should come from an expanded API payload or a reproducible config layer
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2025-09-24 --to-date 2026-04-09`
- Verification outcome:
  - VEVO full-history report `data\\vevo\\report_20250503-20260409.html` contains all three cohort chart IDs
  - ROY full-history report `data\\roy\\report_20250924-20260409.html` contains all three cohort chart IDs
  - both exports complete successfully with the new cohort-normalized views embedded in the modern dashboard.

### 2026-04-03
- Promoted the modern dashboard shell (`test2`) to the default production HTML renderer.
- Removed standalone CFO HTML from `reporting_core.contracts` and from `daily_report_runner.py`.
- Changed daily SES delivery to send only the main HTML report attachment.
- Cleaned local legacy `__test` artifacts and regenerated the plain VEVO March production report to verify the new default renderer.

### 2026-04-01
- Added side-by-side output variant support for safe UI redesign/testing without overwriting working report artifacts:
  - new optional `output_tag` support in `reporting_core.contracts.build_artifact_set(...)`,
  - tagged artifacts render as `__<tag>` before file extension, e.g. `report_...__ui_test.html`,
  - `export_orders.py` now accepts `--output-tag` and isolates cleanup to the active output variant only,
  - `daily_report_runner.py` now accepts `--output-tag` and generates tagged CFO outputs against the same tagged artifact set,
  - verified syntax with `python -m py_compile reporting_core\\contracts.py reporting_core\\__init__.py export_orders.py daily_report_runner.py`,
  - verified smoke exports for VEVO and ROY on `2026-03-30..2026-03-31` with `--output-tag ui_test`,
  - verified tagged CFO generation for both projects:
    - `data\\vevo\\cfo_graphs_20260330-20260331__ui_test.html`
    - `data\\roy\\cfo_graphs_20260330-20260331__ui_test.html`,
  - generated full-range side-by-side test artifacts without email sending:
    - VEVO:
      - `data\\vevo\\report_20250503-20260331__ui_test.html`
      - `data\\vevo\\cfo_graphs_20250503-20260331__ui_test.html`
      - `data\\vevo\\email_strategy_20250503-20260331__ui_test.html`
    - ROY:
      - `data\\roy\\report_20250924-20260331__ui_test.html`
      - `data\\roy\\cfo_graphs_20250924-20260331__ui_test.html`.
- Updated ROY project baseline `report_from_date` from `2025-08-06` to `2025-09-24` in `projects/roy/settings.json`.
- Re-generated ROY reporting outputs for `2025-09-24..2026-03-31` without email delivery so current artifacts match the new start boundary.
- Fixed report headings to use project-level `reporting_system_name` across generated HTML outputs.
- Main HTML reports now render `Vevo reporting`, `Roy reporting`, and future client names from project config instead of a hardcoded BizniWeb title.
- CFO dashboards now use the same project reporting title in the HTML `<title>` and visible header, with `CFO Executive Dashboard` kept as a subtitle.
- Email strategy HTML now also uses the project reporting title in the document title and main heading.
- Verified regeneration for VEVO (`2025-05-03..2026-03-31`) and ROY (`2025-08-06..2026-03-31`) without email delivery.

- Fixed Daily Orders chart visibility issue in shared HTML generator:
  - date coverage in `aggregate_by_date_*` was already complete, including zero-order days,
  - pure bar rendering made zero-order days look like "missing days" at the start of sparse client timelines,
  - `Daily Orders` now overlays a thin line series on top of bars so zero-order periods remain visually continuous instead of appearing absent.
- Re-generated ROY report for `2025-08-06..2026-03-31` after the chart fix.

### 2026-04-01
- Completed `P4.2` observability baseline for reporting:
  - added `scripts/observability_snapshot.py` for project-level artifact + source-health snapshots,
  - added `.github/workflows/observability-check.yml` to generate/upload a JSON observability artifact in CI,
  - extended `scripts/security_ci.py` to require and syntax-check the observability baseline.
- Completed `P4.4` reporting templates baseline:
  - added `templates/reporting-client/` with `settings.template.json`, `.env.example`, `product_expenses.json`, and onboarding README,
  - added `scripts/scaffold_client.py` to scaffold a new client bundle under `projects/<slug>/`,
  - updated `README_DEV.md` with observability and client-template usage.
  - integrated in export_orders.py (`analyze_day_of_month`) and HTML generation,
  - uses full months only for unbiased phase-of-month comparisons,
  - normalizes by calendar occurrences for each day number (1..31),
  - added 2 charts + normalized performance table in HTML report.
- Hardened geographic reporting for Top Cities:
  - country now prefers `delivery_country` and falls back to `invoice_country`,
  - city now prefers `delivery_city` and falls back to `invoice_city`,
  - empty cities are excluded from ranking.
- Hardened reporting repo hygiene for multi-PC work:
  - `.gitignore` now blocks local `.env.*` runtime files while preserving safe templates,
  - `.gitattributes` enforces LF for Python/Markdown/template files,
  - added safe tracked template `.env.roy.sk.template`,
  - cleared CRLF-only working tree noise before continuing.
- Completed `P1.4` partial-data handling for reporting outputs:
  - added source-health contract per run (`source_health`) with per-source status/mode/detail fields,
  - export now writes `data_quality_<range>.json` sidecar metadata next to report artifacts,
  - main HTML report renders a visible Data Quality banner/table before KPI cards,
  - CFO HTML runner loads the same sidecar and renders the same source-health banner,
  - runner keeps backward compatibility by not requiring the JSON sidecar for legacy artifact existence checks,
  - verified syntax with `python -m py_compile export_orders.py html_report_generator.py daily_report_runner.py google_ads.py weather_client.py facebook_ads.py generate_invoices.py http_client.py`,
  - verified ROY smoke run end-to-end on `2026-03-01..2026-03-03`, including generated `data_quality_*.json`, main HTML report, and CFO HTML banner rendering.
- Completed `P1.3` reporting integration hardening:
  - added shared `http_client.py` with default timeout + retry policy for external integrations,
  - moved Facebook Ads API auth to `Authorization: Bearer` header instead of query params,
  - removed direct `requests.get` usage from Facebook Ads client in favor of shared session helper,
  - added configurable timeouts for BizniWeb GraphQL transport in reporting and invoice flows,
  - moved weather client to shared retry/timeout session,
  - documented HTTP timeout/retry knobs in `.env.example`,
  - verified syntax with `python -m py_compile http_client.py weather_client.py facebook_ads.py export_orders.py generate_invoices.py`.
  - city now prefers `delivery_city` and falls back to `invoice_city`,
  - blank city values are excluded from ranking,
  - ties are sorted by revenue first and order count second.
- Added project-scoped weather configuration for VEVO and ROY in `projects/<project>/settings.json`.
- Added `weather_client.py`:
  - historical daily weather fetch from Open-Meteo archive API,
  - monthly local cache per project/location,
  - weighted location support prepared for future multi-city rollout.

### 2026-04-02
- Completed deep research pass for professional ecommerce dashboard structure using BI/dashboard best-practice sources and ecommerce analytics guides.
- Added fully isolated `test2` main-report renderer in `dashboard_test2.py`:
  - separate from the current production renderer and separate from the existing `__test` shell,
  - focused on executive KPI hierarchy, grouped business-question sections, and explicit source-health presentation,
  - uses the same reporting data and existing CFO KPI payload instead of inventing new business logic.
- Wired `generate_html_report(..., dashboard_variant=...)` so only `--output-tag test2` activates the new renderer.
- Kept existing production and `__test` report outputs untouched.
- Verified syntax with `python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py`.
- Generated isolated VEVO March artifacts for review:
  - `data\\vevo\\report_20260301-20260331__test2.html`
  - `data\\vevo\\email_strategy_20260301-20260331__test2.html`
  - period child reports under `data\\vevo\\_periods\\report_20260301-20260331__test2\\...`
- Added V1 weather impact analytics into `export_orders.py`:
  - merges daily weather with `date_agg`,
  - computes weather buckets (`Good / Neutral / Bad`),
  - computes weekday baseline deltas for revenue, profit, orders, AOV,
  - computes direct and lagged weather correlations,
  - exports project-scoped `weather_impact_<range>.csv`.
- Added Weather Impact section into `html_report_generator.py`:
  - correlation KPI cards,
  - precipitation vs revenue/profit time-series chart,
  - weather bucket uplift vs weekday baseline chart,
  - weather bucket performance table.
- Verified syntax with:
  - `python -m py_compile export_orders.py html_report_generator.py weather_client.py`
- Verified ROY runtime smoke test end-to-end on:
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-07`
  - confirmed Weather Impact section rendered in generated HTML.
- VEVO runtime smoke test remains blocked by expired Facebook token during ads fetch; weather implementation itself is not the blocker.
- Regenerated full-range client outputs without email sending:
  - ROY: `data/roy/report_20250922-20260330.html` and `data/roy/export_20250922-20260330.csv`
  - VEVO: `data/vevo/report_20250503-20260330.html` and `data/vevo/export_20250503-20260330.csv`
- Verified VEVO full-range regeneration again with working Facebook Ads enrichment after providing a valid runtime Meta token for the process.
- Added Advanced DTC metrics pack (1/2/3/4/7/8/9/10/11) into reporting pipeline:
  - new analyzer in export_orders.py: `analyze_advanced_dtc_metrics(df)`,
  - wired to `generate_html_report(..., advanced_dtc_metrics=...)`,
  - added summary KPI cards for first-order/repeat contribution, contribution LTV/CAC, margin stability, SKU Pareto concentration.
- Added new Advanced DTC visual outputs in html_report_generator.py:
  - Contribution by Basket Size chart + table,
  - Payday Window Index chart + table,
  - Cohort Payback Days chart + table,
  - Margin Stability chart,
  - SKU Contribution Pareto chart + table,
  - Attach Rate table for key products.
- Smoke-tested analyzer on synthetic dataset and verified syntax with:
  - `python -m py_compile export_orders.py html_report_generator.py`
- Hardened local repo hygiene for reporting runtime secrets:
  - `.gitignore` now ignores arbitrary local `.env.*` runtime variants while preserving tracked safe templates
  - `.gitattributes` now enforces LF for python/markdown/env-template files to avoid false CRLF-only diffs
  - added safe tracked template `/.env.roy.sk.template` for roy-specific local bootstrap without committing secrets
- Completed `P2.2` reporting client-boundary refactor:
  - added shared `project_config.py` to centralize per-project env loading, project settings, display-name/reporting defaults, API URL resolution, and BizniWeb base URL derivation,
  - removed remaining Vevo-specific runtime defaults from generic reporting flow in `export_orders.py`,
  - kept Vevo legacy product-cost fallback scoped only to Vevo; non-Vevo projects no longer inherit Vevo costs implicitly,
  - gated Vevo-only email strategy HTML behind per-project config (`enable_email_strategy_report`) so ROY and future clients do not generate Vevo-branded strategy output,
  - made `daily_report_runner.py` project-driven for email subject/body text, SES configuration-set fallback, and CloudWatch namespace selection,
  - removed `email_strategy_html` from required daily-runner outputs so non-Vevo projects can run cleanly without Vevo-only artifacts,
  - made `generate_invoices.py` project-aware via `--project`, per-project env bootstrap, and BizniWeb base URL derivation from the selected project API endpoint instead of hardcoded Vevo URLs,
  - extended project settings:
    - `projects/vevo/settings.json` now declares display/reporting defaults and explicitly enables the Vevo email-strategy artifact,
    - `projects/roy/settings.json` now declares project display/reporting defaults and explicitly disables the Vevo-only strategy artifact,
  - verified syntax with `python -m py_compile project_config.py export_orders.py daily_report_runner.py generate_invoices.py`,
  - verified ROY smoke export on `2026-03-01..2026-03-02`,
  - verified VEVO smoke export on `2026-03-01..2026-03-02`,
  - verified project-aware invoice bootstrap on ROY with `python generate_invoices.py --project roy --from-date 2026-03-01 --to-date 2026-03-02 --dry-run --no-web-login`.
- Completed `P2.4` reporting security CI baseline:
  - extended `.github/workflows/env-check.yml` to run on the active reporting branch and added `secret-scan` + `security-baseline` jobs,
  - added `scripts/security_ci.py` with repo-local assertions for shared HTTP hardening (`Authorization` header usage, shared retry session, partial-data/source-health invariants),
  - wired CI to fail fast if reporting core loses the partial-data markers or Meta auth hardening contract,
  - verified the local baseline script with `python scripts/security_ci.py`.

- Fixed VEVO local Meta token/bootstrap drift on 2026-04-01:
  - confirmed AWS runtime secret `vevo/reporting/runtime-env` still contains a valid Facebook Ads token,
  - synced local root `.env` VEVO token with the valid runtime token,
  - rewrote `.env` without UTF-8 BOM after a local PowerShell write introduced BOM and broke the first env key (`BIZNISWEB_API_TOKEN`),
  - hardened all reporting-side `load_dotenv(...)` calls to use `encoding="utf-8-sig"` so BOM-prefixed `.env` files no longer break the first key,
  - verified VEVO smoke export on `2026-03-31..2026-03-31` with successful Facebook Ads enrichment (`Successfully connected to Facebook Ads account: Wachman`, spend fetched, ROAS restored).
- Fixed VEVO Google Ads runtime hygiene on 2026-04-01:
  - normalized AWS Secrets Manager entry `vevo/reporting/runtime-env` from malformed pseudo-JSON into valid JSON,
  - aligned runtime `GOOGLE_ADS_LOGIN_CUSTOMER_ID` to an empty value because VEVO Google Ads API access works directly on customer `7592903323` and fails when the old MCC login header is forced,
  - verified Google Ads API connectivity locally with `test_connection=True` against `Vevo.sk (7592903323)`,
  - verified that March 2026 Google Ads spend is correctly `0.00` because both `Vevo.sk (7592903323)` and `Vevo.sk - old (1025163995)` return zero March campaign rows via GAQL,
  - confirmed the zero Google Ads spend in the VEVO March report is a real account state, not an integration bug.

- UI redesign baseline for main HTML reporting (test track) on 2026-04-01:
  - replaced legacy purple-gradient dashboard skin in `html_report_generator.py` with a modern analytics layout (neutral background, stronger typography hierarchy, denser KPI cards, cleaner tables, larger chart canvases),
  - increased chart readability (`max-height` up to 420px, improved spacing, better responsive behavior),
  - standardized euro symbol rendering by replacing mojibake `â‚¬` occurrences with HTML entity `&#8364;` in report output templates,
  - normalized collapsible toggle glyph to `&#9662;` to avoid encoding drift in generated HTML,
  - validated syntax with `python -m py_compile html_report_generator.py daily_report_runner.py`,
  - regenerated side-by-side test artifacts (no email) with `--output-tag ui_test`:
    - VEVO: `data/vevo/report_20250503-20260331__ui_test.html`
    - ROY: `data/roy/report_20250922-20260331__ui_test.html`.

- SK/EN full-translation + user-friendly pass completed on 2026-04-01:
  - strengthened bilingual rendering in `html_report_generator.py` with explicit `data-en`/`data-sk` coverage for guidance and quick-read sections so language switch is end-to-end usable,
  - added plain-language onboarding block for non-finance users (`metric-cheatsheet`) explaining Revenue, Net Profit, ROAS, and CAC vs Break-even CAC in business-friendly wording,
  - corrected Slovak readability/diacritics in key guidance text (`Ako čítať tento report (jednoducho)`),
  - normalized confusing delta KPI labels to explicit text:
    - `ROAS Check Delta`
    - `Margin Check Delta (pp)`
    - `CAC Check Delta`
    with corresponding SK mappings (`Kontrola ... delta`),
  - updated translation dictionaries and replacement maps to match the new labels and avoid previous symbol-encoding drift.
- Validation executed:
  - `python -m py_compile html_report_generator.py`,
  - full regenerate (no email) with final outputs tag `lang_full3`:
    - VEVO: `data/vevo/report_20250503-20260331__lang_full3.html`
    - ROY: `data/roy/report_20250922-20260331__lang_full3.html`
  - verified generated HTML contains:
    - project title headers (`Vevo reporting` / `Roy reporting`),
    - language switch texts with SK+EN variants,
    - user-friendly KPI cheat-sheet block,
    - updated Delta KPI labels.

- Sidebar/menu test track for main VEVO reporting completed on 2026-04-02:
  - redesigned the main report shell in `html_report_generator.py` to a dashboard-style layout with:
    - sticky left sidebar,
    - metric-group menu (`Overview`, `Revenue & profitability`, `Customers & retention`, `Marketing & ads`, `Geography`, `Products`, `Operations & diagnostics`),
    - section-level group switching without changing any business calculations,
    - warmer executive dashboard styling closer to modern admin dashboards.
  - wrapped the long report into navigable dashboard sections while preserving existing charts/tables and SK/EN language switching,
  - added client-side section filter persistence via `localStorage` (`reportMetricGroup`),
  - generated only one stable VEVO March test artifact:
    - `data/vevo/report_20260301-20260331__test.html`
  - cleaned previous VEVO/ROY tagged test artifacts (`__lang_*`, `__ui_*`, `__ui_test`, etc.) so only:
    - original untagged reports remain,
    - one current VEVO test HTML remains for UI review.

Next exact step:
- Review `data/vevo/report_20260301-20260331__test.html` visually and decide whether the new professional period switcher should stay as the baseline UX for the dashboard test track before deeper chart-visual redesign starts.

### 2026-04-02
- Extended the isolated VEVO March `test2` dashboard so it keeps the `test2` hero/intro shell while pulling in richer analytics previously available only in the fuller report/test track.
- `dashboard_test2.py` now renders additional data groups:
  - customer retention and concentration,
  - refund trend,
  - cohort retention chart + table,
  - calendar patterns (day-of-week, week-of-month, day-of-month),
  - weather uplift,
  - geo profitability table,
  - product margin breakout chart,
  - product trend chart + table.
- `html_report_generator.py` now passes the richer analytics payloads into the isolated `test2` renderer:
  - `day_of_week_analysis`
  - `week_of_month_analysis`
  - `day_of_month_analysis`
  - `weather_analysis`
  - `geo_profitability`
  - `product_trends`
  - `customer_concentration`
  - `cohort_analysis`
- Kept the `test2` top section intact:
  - hero header,
  - side language switcher,
  - period switcher,
  - executive KPI deck.
- Fixed the `test2` sidebar so the project badge uses the project initial dynamically and the navigation now includes the new `Patterns` section with correct ordering.
- Verified with:
  - `python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py`
  - successful VEVO March regenerate:
    - `data/vevo/report_20260301-20260331__test2.html`
  - HTML presence checks for:
    - `Executive KPI deck`
    - `Customer quality and retention`
    - `Calendar patterns and weather`
    - `Geo profitability`
    - `Product trend table`

### 2026-04-02
- Added reusable CFO KPI payload builder in `reporting_core/cfo_kpis.py` so the main report can reuse the same executive KPI logic as the standalone CFO dashboard.
- Wired `export_orders.py` to compute `cfo_kpi_payload` from the existing report data (`date_agg` + exported order rows) without changing the underlying financial calculations.
- Injected a new top-of-report CFO KPI panel into `html_report_generator.py`:
  - placed above the old summary cards,
  - uses the same KPI set as the CFO dashboard,
  - supports `Daily / Weekly / Monthly` switching,
  - respects the existing SK/EN language switch,
  - uses the new dashboard shell styling instead of the legacy card layout.
- Verified syntax with:
  - `python -m py_compile export_orders.py html_report_generator.py reporting_core\\__init__.py reporting_core\\cfo_kpis.py`
- Regenerated only the VEVO March test artifact (no email):
  - `data/vevo/report_20260301-20260331__test.html`
- Verified the generated HTML contains the new executive block and embedded KPI payload (`CFO_TOP_KPI`) with the expected metrics:
  - Revenue
  - Profit
  - Orders
  - AOV
  - CAC
  - ROAS
  - Pre-Ad Contribution Margin
  - Post-Ad Margin
  - Company Margin (incl. fixed)

### 2026-04-02
- Reverted the two latest test-only date-range UI experiments from the VEVO March dashboard prototype:
  - removed the global chart date-range filter,
  - removed the per-section chart date-range filters.
- Restored the test UI baseline to the previous state:
  - sidebar navigation stays,
  - top CFO KPI band stays,
  - no chart-range controls are rendered.
- Regenerated the VEVO March test artifact after the revert:
  - `data/vevo/report_20260301-20260331__test.html`
- Verified the regenerated HTML no longer contains the removed range UI markers (`chart-range-panel`, `chart-range-start`, `chart-range-end`).

### 2026-04-02
- Implemented a server-driven professional period switcher for the VEVO March dashboard test track without touching production outputs.
- `export_orders.py` now builds preset report variants for tagged/test exports and links them as full-report period views instead of doing client-side chart cropping:
  - `7D`
  - `30D`
  - `90D` when the selected range is long enough
  - `FULL`
- Added reusable helpers for:
  - period-range slicing from already fetched orders,
  - preset period-spec generation,
  - relative-link payload generation for parent/child report variants.
- `html_report_generator.py` now renders the same period switcher:
  - globally at the top,
  - inside every major dashboard section (`Overview`, `Business`, `Customers`, `Marketing`, `Geography`, `Customer structure`, `Products`, `Operations`).
- Section links preserve anchors (for example `#section-marketing`) and the dashboard JS reopens the correct sidebar metric group after cross-period navigation.
- Generated only test artifacts under the hidden bundle path for tagged outputs, keeping the visible top-level test report as the main entry point:
  - `data/vevo/report_20260301-20260331__test.html`
  - `data/vevo/_periods/report_20260301-20260331__test/7d/...`
  - `data/vevo/_periods/report_20260301-20260331__test/30d/...`
- Verified:
  - syntax with `python -m py_compile export_orders.py html_report_generator.py`,
  - successful VEVO March test regenerate with variant bundle creation,
  - zero leftover literal `{render_period_switcher(...)}`
  - working period-switcher links for all major sections in both the main report and child period variants.

### 2026-04-02
- Merged the richer analytics payload from the fuller reporting build into the isolated `test2` dashboard track while keeping the `test2` intro/hero shell unchanged.
- `dashboard_test2.py` now renders additional sections from the richer reporting data:
  - customer quality and retention,
  - calendar patterns and weather,
  - geo profitability,
  - product trend breakout/table.
- `html_report_generator.py` passes the richer analytics payload through to `generate_test2_dashboard(...)`.
- Fixed a `test2` serialization bug by replacing the raw `customer_concentration` DataFrame payload with a JSON-safe summary object in the dashboard bootstrap payload.
- Cleaned the visible Slovak labels in `test2` that were previously mojibake/broken:
  - `Kvalita zákazníkov a retencia`
  - `Toto rozširuje pekný ...`
  - `Denná miera refundov odhaľuje operačné problémy, nie len súčet.`
- Regenerated and verified the VEVO March `test2` artifact:
  - `data/vevo/report_20260301-20260331__test2.html`
- Verified:
  - syntax with `python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py`
  - successful VEVO March `test2` regenerate
  - expected sections present in HTML
  - cleaned Slovak strings present in final HTML output

### 2026-04-02
- Expanded the VEVO March `test2` dashboard so it keeps the preferred `test2` shell/hero design but now pulls in the much richer metric surface from the fuller `test` reporting line.
- `html_report_generator.py` now passes the full analytics payload families into `generate_test2_dashboard(...)`, including customer/retention, CLV/CAC, order-size, combinations, advanced DTC, B2B/B2C, order status, ads effectiveness, lifecycle segments, first-item retention, same-item repurchase, time-to-nth-by-first-item, detailed FB metrics, cost-per-order, hourly/day-of-week Meta stats, LTV by date, and consistency checks.
- `dashboard_test2.py` now renders a `Full metric library` layer inside the `test2` design shell with added chart galleries for:
  - customer quality and repeat behavior,
  - calendar and weather patterns,
  - product and operational drilldowns,
  - economics and marketing drilldowns.
- Verified syntax with:
  - `python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py`
- Regenerated only the VEVO March `test2` artifact (no email):
  - `data/vevo/report_20260301-20260331__test2.html`
- Verified:
  - export completed successfully,
  - inline dashboard script parses successfully in Node (`new Function(...)`),
  - new gallery chart ids and render calls are present in the generated `test2` HTML.
- Next exact step:
  - visually review `report_20260301-20260331__test2.html` in the browser and decide which `test2` sections/cards should replace the legacy report layout next.

### 2026-04-03
- Extended VEVO March 	est2 so the design shell stays unchanged but the metric coverage moves much closer to the original 	est report.
- dashboard_test2.py now fills the previously empty standalone library containers:
  - libraryEconomicsStandalone
  - libraryMarketingStandalone
  - libraryCustomersStandalone
- Added standalone charts for missing metric families from the legacy report, including:
  - economics: revenue vs total cost, total costs, product costs, gross margin, packaging, shipping, fixed costs, items sold, avg items per order, scatter revenue vs cost, all-metrics overview, LTV by acquisition date, LTV-based profit
  - marketing: FB spend, Google spend, FB vs Google spend, spend vs clicks, campaign conversion rate, cost per conversion, CTR, CPC, spend share, campaign CPO, campaign ROAS, spend bucket orders
  - customer value: refund amount, CLV, CAC, CLV vs CAC, LTV/CAC ratio, return time, payback trend
- Added scroll-aware sidebar navigation in 	est2:
  - the active menu item now switches based on the visible section instead of staying hardcoded on Overview
  - sidebar links now smooth-scroll to the relevant section and update browser hash
- Verified:
  - python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py
  - VEVO March 	est2 regenerate completed successfully
  - final inline dashboard script parses successfully in Node
  - standalone library containers and new chart ids are present in data/vevo/report_20260301-20260331__test2.html
- Next exact step:
  - visually review 
eport_20260301-20260331__test2.html and decide whether the remaining legacy tables should also be redesigned into 	est2 cards/panels or left outside the dashboard shell.
### 2026-04-04
- Added an `Executive metrics tile deck` to the end of section `10 Full library` in the modern production dashboard, keeping the current dashboard design while surfacing all major top-level KPI metrics in a compact tile grid.
- `dashboard_modern.py` now computes and renders a large summary tile set covering revenue, cost stack, profit, daily averages, orders/items, AOV, CAC/ROAS/MER, revenue per customer, contribution layers, break-even CAC, CAC headroom, payback, refund summary, repeat purchase rate, and related executive checks.
- Added reusable helpers for tile formatting/styling and new tile-grid CSS so the metrics render as readable dashboard cards instead of legacy summary boxes.
- Verified with:
  - `python -m py_compile dashboard_modern.py html_report_generator.py export_orders.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Verified in generated output:
  - `data/vevo/report_20260301-20260331.html`
  - tile deck heading is present in `Full library`
  - tile labels such as `Total revenue (net)`, `Revenue LTV/CAC`, `ROI`, and `Repeat purchase rate` render in the final HTML.
- Next exact step:
  - visually review the new tile deck in the March VEVO report and decide whether any low-signal tiles should be removed or regrouped.
### 2026-04-08
- Fixed the modern dashboard global period switcher architecture so it can work from a single emailed HTML attachment instead of depending only on sibling `_periods/...` files being present on disk.
- `dashboard_modern.py` now tags each global period link with a stable `data-period-key`, persists canonical period hrefs, and injects embedded report variants into non-full period transitions so 7D / 30D / 90D switching can work in a single-file/offline context.
- `html_report_generator.py` now passes `embedded_period_reports` through to the modern dashboard renderer.
- `export_orders.py` now builds a lightweight embedded period bundle (base64 child variants for non-full ranges) for the main/full report so the sidebar global time switcher has local content to swap to.
- `reporting_core/cfo_kpis.py` now includes `secondary_metrics` in KPI windows; the modern dashboard uses that to show nominal company profit beneath `Company margin (incl. fixed)` in the Executive KPI deck.
- Verified with:
  - `python -m py_compile dashboard_modern.py html_report_generator.py export_orders.py reporting_core/cfo_kpis.py`
  - a direct synthetic render smoke test through `generate_html_report(...)` confirming:
    - `data-period-key` is present,
    - embedded period report bootstrap is present,
    - Company margin KPI renders a secondary nominal profit value.
- Note:
  - live VEVO March export fetch timed out during API work, so runtime verification for the full real report should be rechecked in the next session after a successful export run.
- Next exact step:
  - run a full VEVO export successfully and verify that sidebar period switching works end-to-end from the generated emailed HTML artifact, not just from local disk bundle files.
### 2026-04-08 (runtime verification update)
- Re-ran a full real VEVO March export after the single-file period-switch fix:
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Runtime verification now succeeded end-to-end; the generated report is:
  - `data/vevo/report_20260301-20260331.html`
- Verified on the generated HTML artifact:
  - embedded period bundle is present (`INLINE_EMBEDDED_PERIOD_REPORTS`)
  - period switch links render with `data-period-key`
  - Executive KPI deck includes secondary nominal company profit rendering (`kpi-secondary`) for `Company margin (incl. fixed)`
  - sibling period HTML variants exist under `_periods/report_20260301-20260331/...`
- Outcome:
  - the previously incomplete verification from the earlier 2026-04-08 entry is now closed; full export/runtime generation works for the fixed implementation.
- Next exact step:
  - visually verify from the actual emailed HTML attachment context that 7D / 30D switching behaves correctly in the browser the user uses to open the attachment.
### 2026-04-08 (executive KPI trend strip)
- Added recent trend visualization directly into the `Executive KPI deck` so the top CFO cards show not only current KPI values and comparison deltas, but also short operational trend context.
- `reporting_core/cfo_kpis.py` now builds server-side trend payloads for each KPI window:
  - daily: last 14 daily points
  - weekly: last 8 rolling 7-day points
  - monthly: last 8 rolling 30-day points
- `dashboard_modern.py` now renders per-card sparkline strips with a compact trend delta label inside each Executive KPI card.
- The existing `Company margin (incl. fixed)` secondary nominal profit stays in place and now sits alongside the new trend strip.
- Verified with:
  - `python -m py_compile dashboard_modern.py reporting_core/cfo_kpis.py html_report_generator.py export_orders.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Verified in generated output:
  - `data/vevo/report_20260301-20260331.html`
  - KPI trend CSS/JS markers are present (`kpi-trend`, `sparklineSvg`)
  - report regeneration completed successfully end-to-end
- Next exact step:
  - visually review the Executive KPI deck in the latest VEVO report and decide whether the sparklines should be made denser/subtler or whether a separate mini trend row is needed for any specific KPI.
### 2026-04-08 (VEVO runtime build + scheduler verification)
- Verified that merging the Executive KPI trend-strip change into `main` did not automatically rebuild the VEVO runtime image because `build-and-push-ecr.yml` was not watching `dashboard_modern.py` or `reporting_core/**`.
- Manually dispatched the `Build and Push ECR` workflow and confirmed a fresh `latest` image in ECR:
  - repository: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - newest pushed digest tagged `latest`: `sha256:57a95c3fa57ea5d53e081fd48f340800585a2f4901dd118d039a816719fd090b`
- Confirmed production runtime identifiers before deploy:
  - scheduler: `vevo-daily-report-email`
  - cluster: `vevo-reporting-cluster`
  - task definition before fix: `vevo-reporting-daily:3`
  - log group: `/ecs/vevo-reporting-daily`
  - runtime secret: `vevo/reporting/runtime-env`
- Detected runtime drift in ECS task definition `:3`:
  - Google Ads credentials existed in Secrets Manager,
  - but were not mapped into the container secret env list,
  - which caused the container to log `Google Ads credentials not fully configured`.
- Registered new ECS task definition revision `vevo-reporting-daily:4` with all Google Ads secret mappings added:
  - `GOOGLE_ADS_DEVELOPER_TOKEN`
  - `GOOGLE_ADS_CLIENT_ID`
  - `GOOGLE_ADS_CLIENT_SECRET`
  - `GOOGLE_ADS_REFRESH_TOKEN`
  - `GOOGLE_ADS_CUSTOMER_ID`
  - `GOOGLE_ADS_LOGIN_CUSTOMER_ID`
- Updated scheduler `vevo-daily-report-email` to target `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:4`.
- Ran manual verification tasks:
  - revision `:3` task showed the missing-Google warning,
  - revision `:4` task started successfully from the new image digest and no longer emitted the early missing-Google-credentials warning.
- Regenerated a fresh local full-history VEVO report without sending email:
  - `python daily_report_runner.py --project vevo --from-date 2025-05-03 --to-date 2026-04-08 --skip-email`
  - generated artifact: `data/vevo/report_20250503-20260408.html`
- Next exact step:
  - merge the workflow path fix so future dashboard/runtime merges rebuild ECR automatically without requiring a manual dispatch.
### 2026-04-08 (weather archive cutoff fix)
- Fixed Open-Meteo archive integration in `weather_client.py`.
- Root cause:
  - weather cache/fetch logic requested whole calendar months,
  - for in-progress months that meant requests like `2026-04-01 -> 2026-04-30`,
  - Open-Meteo archive API rejects future days, so the request returned `400 Bad Request`.
- Implemented fix:
  - clamp weather fetches to the last historically available day (`UTC today - 1 day`),
  - return empty weather payload if the requested month starts after the archive cutoff,
  - use distinct cache keys for partial months (`_through_YYYYMMDD`) so incomplete current-month caches do not freeze and block later refreshes.
- Verified with:
  - `python -m py_compile weather_client.py`
  - direct WeatherClient fetch for `2026-04-01 -> 2026-04-07` returned 7 rows successfully
  - full real VEVO report run: `python daily_report_runner.py --project vevo --from-date 2025-05-03 --to-date 2026-04-07 --skip-email`
- Verification result:
  - weather warning `400 Client Error` is gone from the report run,
  - report generated successfully: `data/vevo/report_20250503-20260407.html`.
- Next exact step:
  - merge the weather fix branch into `main` so tomorrow's runtime image can include the corrected weather behavior on the next build/deploy cycle.
- Follow-up deployability fix:
  - `.github/workflows/build-and-push-ecr.yml` now also watches `weather_client.py` and `http_client.py`,
  - so future weather/runtime HTTP changes will automatically rebuild the VEVO ECR image after merge to `main`.
### 2026-04-08 (GitHub Actions Node 24 readiness)
- Upgraded GitHub Actions workflow dependencies that were still running on the deprecated Node 20 action runtime.
- Updated:
  - `.github/workflows/build-and-push-ecr.yml`
    - `actions/checkout@v4` -> `actions/checkout@v5`
    - `aws-actions/configure-aws-credentials@v4` -> `aws-actions/configure-aws-credentials@v5.1.1`
  - `.github/workflows/env-check.yml`
    - all `actions/checkout@v4` -> `actions/checkout@v5`
  - `.github/workflows/observability-check.yml`
    - `actions/checkout@v4` -> `actions/checkout@v5`
- Scope intentionally limited to the actions explicitly causing deprecation warnings in recent ECR workflow runs.
- Verified locally by reviewing all workflow YAML references after the upgrade.
- Next exact step:
  - push the branch, open a PR, and verify on GitHub Actions that the deprecation warning is gone from the next workflow run.
### 2026-04-08 (force Node 24 runtime for GitHub Actions)
- Verified that upgrading to the latest pinned workflow actions removed `actions/checkout@v4` but did not fully remove the GitHub deprecation annotation because `aws-actions/configure-aws-credentials@v5.1.1` still runs on the older JavaScript action runtime.
- Added `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: "true"` at workflow level in:
  - `.github/workflows/build-and-push-ecr.yml`
  - `.github/workflows/env-check.yml`
  - `.github/workflows/observability-check.yml`
- This uses GitHub's documented opt-in path so JavaScript actions execute on Node 24 now, instead of waiting for the future runner default switch.
- Next exact step:
  - push the branch, run/observe the next workflow execution, and confirm the deprecation annotation is gone.
### 2026-04-08 (GitHub Actions v6 action pins)
- After validating the GitHub release feeds, upgraded workflow pins further to the current major releases:
  - `actions/checkout@v6.0.2`
  - `aws-actions/configure-aws-credentials@v6.1.0`
- Kept the `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` workflow env override in place as an additional safety measure during the Node 24 transition window.
- Next exact step:
  - push the branch, merge it, and verify on the next `Build and Push ECR` run that the old Node 20 deprecation annotation no longer appears.
### 2026-04-10 (shared reporting P0 audit fixes)
- Audited the external recommendations against the real VEVO/ROY reporting code and prioritized only the issues that were actually reproducible in the current codebase.
- Confirmed and fixed shell-vs-library binding drift in the modern dashboard:
  - economics mini-cards now read the same financial registry values as the full-library tiles for:
    - `pre_ad_contribution_per_order`
    - `break_even_cac`
    - `payback_orders`
    - `contribution_ltv_cac`
- Fixed refund shell binding to use `refunds_analysis.summary` consistently across shell cards, payload and full-library tiles.
- Added shared render-time normalization for dimension hydration, so UI consumers stop rendering empty/placeholder labels when the producer already has equivalent fields:
  - `day_name <- day_of_week`
  - `anchor_item <- key_product`
  - `anchor_orders <- key_orders`
  - `attached_item <- attached_product`
  - `pre_ad_contribution_margin_pct <- pre_ad_margin_pct`
  - `cum_contribution_pct <- cum_contribution_share_pct`
- Hardened null propagation / source coverage semantics:
  - source health `status=ok/manual` is no longer treated as metric availability when coverage is zero,
  - VEVO `google_ads.active_days=0` now renders `Google CPO = N/A` instead of a misleading `€0.00`,
  - ROY keeps numeric Google CPO because its manual Google source has positive active-day coverage.
- Clarified campaign semantics in the Facebook ingestion/rendering path:
  - preserved platform fields separately from attribution estimates,
  - campaign rows now expose `platform_conversions` and `cost_per_platform_conversion`,
  - attribution rows now expose `attributed_orders_est`, `cost_per_attributed_order`, and `attribution_method`,
  - CPO analysis now emits `campaign_attribution_summary` with `coverage_ratio` and `oversubscription_ratio`.
- Verified with real March 2026 regenerations:
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Smoke verification outcome:
  - VEVO shell economics cards show real values instead of zero fallbacks,
  - VEVO shell `Google CPO` now shows `N/A`,
  - ROY shell economics cards show real values and keep numeric Google CPO,
  - no remaining `null` hydration symptoms were found in generated HTML for weekday / attach-rate / geo consumer labels during targeted checks.
- Follow-up hardening completed:
  - added explicit attribution QA metadata into `source_health.qa.attribution`,
  - QA now evaluates campaign spend coverage, oversubscription and platform CPA arithmetic mismatches,
  - `source_health.overall_status` now escalates to `warning` when QA warnings exist even if raw sources loaded cleanly,
  - modern dashboard now surfaces attribution QA twice:
    - as a health card in the source health grid,
    - as a dedicated marketing panel (`Attribution QA guardrails`) with coverage, oversubscription, campaign-row count and CPA mismatch count,
  - fixed mojibake / bad currency rendering inside the modern marketing section (`&euro;`, ASCII-safe SK copy for reconciliation text),
  - added CI assertions so regressions fail if:
    - attribution QA builder is removed from export,
    - campaign attribution summary disappears,
    - dashboard stops rendering the attribution QA panel.
- Verified with:
  - `python -m py_compile dashboard_modern.py export_orders.py facebook_ads.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO `data_quality_20260301-20260331.json` now contains attribution QA metadata with `qa_status=ok`,
  - ROY `data_quality_20260301-20260331.json` now contains attribution QA metadata with `qa_status=warning`,
  - VEVO and ROY modern reports render the new QA panel and reconciliation values with proper euro symbols,
  - ROY now explicitly warns in dashboard that campaign-level Facebook spend coverage is missing while daily spend exists.
- Next exact step:
  - add thresholded attribution warning banners to the hero/executive shell so severe coverage or oversubscription issues are visible before the user reaches the marketing section.
### 2026-04-10 (hero attribution warning banner)
- Added a thresholded attribution warning banner to the modern dashboard hero shell in `dashboard_modern.py`.
- The banner now appears before the Executive KPI deck whenever attribution QA emits warnings, with severity-aware styling:
  - `critical` for missing campaign spend coverage, empty campaign attribution tables, severe coverage drift, severe oversubscription, or platform CPA mismatches,
  - `warning` for softer attribution QA issues.
- The hero banner exposes the key QA diagnostics directly in the shell:
  - coverage ratio
  - oversubscription ratio
  - CPA mismatch count
  - campaign row count
- The existing raw warning list is reused in the hero banner so the same QA evidence is visible both:
  - in the shell,
  - and later in the marketing section.
- Added a CI assertion in `scripts/security_ci.py` so the build fails if the hero-level attribution warning surface is removed from the modern dashboard.
- Verified with:
  - `python -m py_compile dashboard_modern.py export_orders.py facebook_ads.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO March 2026 report stays clean with no hero attribution banner when QA is healthy,
  - ROY March 2026 report now shows a critical hero attribution banner before the Executive KPI deck,
  - CI passes with the new shell-level guard in place.
- Next exact step:
  - start the Vevo sample funnel model as the next shared business-modeling expansion after the P0 reporting hardening is now visible in the shell.
### 2026-04-10 (Vevo sample funnel model)
- Added a Vevo sample funnel model in `export_orders.py` to track first-order sample-entry customers into repeat and full-size conversion windows.
- Entry cohort definition is now explicit:
  - first order contains at least one sample item
  - first order does not contain a full-size item
- The model computes and exports:
  - repeat conversion by 7/14/30/60/90 day windows
  - any full-size conversion by 7/14/30/60/90 day windows
  - 200ml conversion by 7/14/30/60/90 day windows
  - 500ml conversion by 7/14/30/60/90 day windows
  - top sample entry-product quality rows ranked by downstream conversion
- Added the sample funnel payload pass-through to:
  - `html_report_generator.py`
  - `dashboard_modern.py`
- Added a dedicated sample funnel block in the main Customers section of the modern dashboard:
  - entry customers
  - repeat 30d
  - full-size 30d
  - full-size 60d
  - median days to full-size
  - top entry product
  - sample funnel window chart
  - entry-product quality table
- Added customer-library drilldowns in the modern dashboard:
  - `custSampleFunnelWindowChart`
  - `custSampleEntryProductChart`
- Hardened the sample funnel implementation so it uses robust revenue-field fallback selection instead of assuming item-level revenue columns exist in every export/sub-period bundle.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO March 2026 export completed successfully
  - sample funnel CSV outputs were generated for the 7d and full March bundles
  - `data\\vevo\\report_20260301-20260331.html` contains:
    - `sampleFunnelChart`
    - `custSampleFunnelWindowChart`
    - `custSampleEntryProductChart`
- Next exact step:
  - start the Roy bundle/accessory model as the next business-model expansion after the Vevo sample funnel model is now live in the modern dashboard.
### 2026-04-10 (Roy bundle and accessory model)
- Added Roy-specific anchor device families and accessory groups to `projects/roy/settings.json` so bundle economics no longer depends on ad-hoc string logic in dashboard code.
- Added a dedicated Roy bundle/accessory model in `export_orders.py` that computes:
  - pair-level attach rate
  - incremental order contribution uplift
  - anchor device family summary
  - accessory group summary
- Exposed the new Roy bundle/accessory payload through the modern dashboard renderer in `dashboard_modern.py`.
- Added new Roy charts and tables:
  - `prodBundleAccessoryAttachChart`
  - `prodBundleAccessoryUpliftChart`
  - `prodBundleAccessoryFamilyChart`
  - `prodBundleAccessoryGroupChart`
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - Roy March 2026 report regenerated successfully
  - new bundle/accessory charts render in `data\\roy\\report_20260301-20260331.html`
- Next exact step:
  - add cohort-normalized CAC / LTV / payback views so global shortcut metrics are complemented by acquisition-cohort recovery curves.
### 2026-04-10 (Cohort-normalized unit economics)
- Added cohort-normalized CAC / LTV / payback views in `export_orders.py` for both VEVO and ROY.
- The cohort model now computes 30/60/90/180-day acquisition-cohort views with:
  - customers
  - revenue LTV
  - contribution LTV
  - contribution LTV / CAC
  - recovery percentage
  - average / median payback days
- Added cohort-normalized charts to the modern dashboard in `dashboard_modern.py` for both projects:
  - `custCohortContributionLtvCacChart`
  - `custCohortPaybackRecoveryChart`
  - `custCohortCacVsContributionChart`
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project vevo --from-date 2025-05-03 --to-date 2026-04-09`
  - `python export_orders.py --project roy --from-date 2025-09-24 --to-date 2026-04-09`
- Verification outcome:
  - full-history VEVO and ROY reports regenerate successfully
  - both reports now contain cohort-normalized unit-economics charts in the customer section / full library
- Next exact step:
  - normalize shipping sign semantics so positive values always mean business cost and negative values mean shipping profit, then update labels and formulas consistently across runtime, export and dashboard layers.
### 2026-04-10 (Shipping net semantics cleanup)
- Replaced ambiguous `shipping_subsidy_per_order` semantics with canonical `shipping_net_per_order` in the runtime/config layer:
  - positive value = business shipping cost
  - negative value = shipping profit / over-recovery
- Added runtime alias handling in `reporting_core/runtime.py` so existing settings can still load, while new project configs and templates now use:
  - `shipping_net_per_order`
- Updated project settings:
  - `projects/vevo/settings.json` now uses `shipping_net_per_order: 0.2`
  - `projects/roy/settings.json` now uses `shipping_net_per_order: -0.2`
  - `templates/reporting-client/settings.template.json` now uses `shipping_net_per_order`
- Updated export math in `export_orders.py` to use canonical `shipping_net_cost` in:
  - daily aggregation
  - total cost
  - pre-ad contribution
  - post-ad contribution
  - geo profitability
  - financial summaries
- Preserved backward-compatible aliases where needed so existing consumers do not break, but all key formulas now read `shipping_net_cost` first.
- Updated downstream readers:
  - `reporting_core/cfo_kpis.py`
  - `daily_report_runner.py`
  - `dashboard_modern.py`
  - `html_report_generator.py`
- Dashboard/UI cleanup:
  - renamed visible labels from `Shipping Subsidy` to `Net shipping`
  - modern dashboard tiles and charts now explain that positive means cost and negative means shipping profit
  - legacy/shared generator fallbacks now read `shipping_net_cost` before old subsidy aliases
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py reporting_core\\runtime.py reporting_core\\cfo_kpis.py daily_report_runner.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO and ROY March 2026 reports regenerate successfully
  - modern HTML outputs now render `Net shipping` instead of the ambiguous subsidy label
  - shipping math stays stable while sign semantics are now explicit and consistent
- Next exact step:
  - add full QA assertions in pipeline for shell/library parity, campaign arithmetic integrity and normalized-dimension completeness (`day_name`, `anchor_item`, `country`).
### 2026-04-10 (CM taxonomy surfaced + full data QA assertions)
- Added explicit CM1 / CM2 / CM3 taxonomy aliases and dashboard cards so the economics section no longer depends only on legacy pre-ad/post-ad naming.
- Added pipeline-level `data_assertions` QA in `export_orders.py` covering:
  - shell vs library parity for key economics metrics
  - campaign CPA arithmetic integrity
  - normalized dimension completeness (`day_name`, `anchor_item`, `attached_item`, `anchor_orders`, `country`)
  - attributed orders tolerance vs total orders
  - refund registry presence and consistency deltas
- Added `margin_stability` QA with 7-day smoothing for fixed-margin alerting, including raw vs smoothed extreme-day counts and min/max smoothed margin bounds.
- Wired both QA builders into `source_health.qa` so they render in the modern dashboard and participate in warning propagation.
- Added modern dashboard sections:
  - `CM1 / CM2 / CM3 taxonomy`
  - `Data assertions`
  - `Smoothed fixed-margin alerts`
- Extended `scripts/security_ci.py` so CI now fails if these new QA builders / dashboard sections disappear.
- Fixed a runtime bug in monthly aggregation where `cm3_margin_pct` incorrectly referenced a non-existent `month_agg['profit_margin_pct']`; it now computes directly from `net_profit / total_revenue`.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py scripts\security_ci.py`
  - `python scripts\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO and ROY March 2026 reports regenerate successfully
  - new QA sections render without breaking period bundles
  - consistency checks remain green after CM taxonomy exposure
- Next exact step:
  - add acquisition-source x product-family cube for ROY and VEVO so channel efficiency can be evaluated by product family instead of only globally.
### 2026-04-11 (Acquisition-source x product-family cube)
- Added order-level ad spend hydration into the advanced DTC pipeline so first-order source proxies can be derived consistently from the first purchase day.
- Wired the existing `analyze_acquisition_source_product_family_cube(...)` model into `analyze_advanced_dtc_metrics(...)` and exposed it in the exported advanced metrics payload as:
  - `acquisition_product_family_cube`
- Extended the modern dashboard payload in `dashboard_modern.py` with:
  - `acquisition_family.cube_rows`
  - `acquisition_family.source_rows`
  - `acquisition_family.family_rows`
  - `acquisition_family.summary`
- Added three new marketing library charts to the modern dashboard for both VEVO and ROY:
  - `Source proxy x product family`
  - `90d contribution by source proxy x family`
  - `Source proxy summary`
- The new view is explicitly proxy-based, using paid-day presence (`facebook_paid_day`, `google_paid_day`, `mixed_paid_day`, `organic_unknown_day`) rather than pretending to be exact order-level attribution.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO and ROY March 2026 reports regenerate successfully
  - both rendered HTML reports contain the new acquisition-family charts and chart bindings
  - no regression in existing advanced DTC or marketing sections
- Next exact step:
  - add Vevo cohort refill model so refill timing is measured by first-item cohort and horizon, not only by generic repeat-purchase logic.

### 2026-04-11 (revenue + manual ads regression fix)
- Disabled manual ads totals as a fallback path unless a project explicitly opts into `prefer_manual_ads_totals=true`.
- Removed Roy manual FB/Google totals from project settings so Roy now relies only on live Meta/Google Ads sources.
- Namespaced Facebook and Google Ads caches by ad account/customer IDs to avoid cross-project cache pollution.
- Fixed order-item revenue sourcing to prefer BizniWeb explicit line totals (`items.sum` as net, `items.sum_with_tax` as gross) instead of inferring VAT from unreliable `is_net_price` flags.
- Modern dashboard KPI labels now explicitly mark revenue and AOV as net metrics.
- Verified with no-cache exports:
  - Roy full range `2025-09-24 .. 2026-04-10`
  - Vevo full range `2025-05-03 .. 2026-04-10`
- Roy now connects to the live Google Ads account `Roy.sk` (`5313708530` via MCC `6704852923`) and no longer uses the old fixed spend fallback.
- Vevo now connects to the live Google Ads account `Vevo.sk` (`7592903323`) with no fixed-spend fallback.

### 2026-04-11 (short SES email body regression fix)
- Identified that `daily_report_runner.py` still sent the old long-form executive summary in the SES plain-text body.
- Replaced the body template with a short production mail:
  - attachment notice
  - covered date range
  - concise data quality status
  - one short QA warning note only when needed
- Removed the old `build_report_summary(...)` output from the actual SES send path; the long CFO-style narrative is no longer injected into the mail body.
- Verified locally with:
  - `python -m py_compile daily_report_runner.py`
  - direct function render of `build_email_body(...)`
- Expected runtime effect:
  - the daily scheduled VEVO mail should again send the short, clear body once the updated image is built and pulled by the scheduled ECS task.

### 2026-04-11 (Vevo cohort refill model + reporting QA smoke)
- Added a dedicated Vevo refill cohort model in `export_orders.py` so refill timing is measured by first-order entry bucket and cohort month, not only by generic repeat-purchase logic.
- Export now produces refill artifacts:
  - `refill_cohort_buckets_<range>.csv`
  - `refill_cohort_windows_<range>.csv`
  - `refill_cohort_months_<range>.csv`
- Wired `refill_cohort_analysis` through `html_report_generator.py` into the modern dashboard renderer.
- Extended `dashboard_modern.py` with a refill cohort block in the Customers section and new full-library charts:
  - `Refill cohort timing`
  - `Refill bucket quality`
  - `custRefillWindowChart`
  - `custRefillBucketChart`
  - `custRefillCohortChart`
- Fixed refund QA parity checks so refund summary metrics are asserted against the shared financial registry instead of only checking presence.
- Added `scripts/reporting_qa_smoke.py` and wired it into `env-check.yml` plus `scripts/security_ci.py` so behavior-level reporting QA runs in CI, not only static checks.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py scripts\security_ci.py scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `python scripts\reporting_qa_smoke.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO March 2026 report regenerates successfully
  - refill cohort charts render in the modern dashboard without breaking existing sections
  - reporting QA smoke passes locally and is now enforced by CI
- Next exact step:
  - merge this step to `main`, then implement shared geo confidence scoring and low-sample geo guardrails for both VEVO and ROY.

### 2026-04-11 (B2B/B2C unit economics + lifecycle proxy)
- Fixed `_build_growth_order_item_frames(...)` so order-level fixed-overhead allocation now uses the same project/runtime daily fixed-cost logic as the main report instead of the old `CFO_FIXED_DAILY_COST_EUR` fallback.
- Added `excluded_status_orders` tracking in the fetch/filter pipeline so excluded payment-failure orders can be surfaced analytically without polluting reportable revenue exports.
- Expanded `analyze_b2b_vs_b2c(...)` from a raw split into a segment unit-economics view with:
  - CM1 / CM2 / CM3 profit
  - revenue per customer
  - repeat-customer rate
  - CM2 / CM3 per order
  - new vs returning order counts
- Expanded `analyze_order_status(...)` into two layers:
  - final-status mix
  - explicit lifecycle proxy buckets built from final statuses + tracked excluded payment failures
- Updated the modern dashboard to render the new analytics in the active shell and operations library:
  - lifecycle proxy chart + table
  - B2B/B2C unit economics table
  - B2B/B2C unit-economics library chart
  - lifecycle proxy library chart
  - final-status table now shows reportable CM2 per order
- Verified with:
  - `python -m py_compile export_orders.py dashboard_modern.py html_report_generator.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - `python scripts\\reporting_qa_smoke.py`
- Verification outcome:
  - VEVO and ROY March 2026 exports regenerate successfully on `codex/segment-unit-econ-lifecycle`
  - both `report_20260301-20260331.html` outputs contain:
    - `orderLifecycleProxyChart`
    - `opsLifecycleProxyChart`
    - `opsB2bUnitEconomicsChart`
