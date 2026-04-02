# PROJECT_STATE

Last updated: 2026-04-01
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

## 8) Next Exact Step

- Use the new `--output-tag ui_test` side-by-side variant flow for the upcoming CFO/main-report UI redesign so production artifacts stay untouched while layout changes are tested.

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
  - updated daily runner to consume the shared artifact contract instead of rebuilding output paths ad hoc,
  - verified syntax with `python -m py_compile export_orders.py daily_report_runner.py generate_invoices.py project_config.py reporting_core\\__init__.py reporting_core\\config.py reporting_core\\runtime.py reporting_core\\contracts.py`,
  - verified ROY smoke export on `2026-03-01..2026-03-02`,
  - verified project-aware daily runner on `2026-03-01..2026-03-02` with `--skip-export --skip-email`.
- Added new Week-of-Month analytics (Week 1-4) into reporting pipeline in export_orders.py.
- Wired Week-of-Month outputs into HTML report generation (html_report_generator.py) with 2 charts and performance table.
- Added aggregation for week-level pattern visibility: orders, revenue, profit, margin, AOV, avg daily revenue/profit, active days/months.
- Verified syntax via python -m py_compile export_orders.py html_report_generator.py.
- Revised Week-of-Month methodology to remove day-count bias:
  - uses only days 1-28 (4x7 equal windows),
  - uses full months only (drops partial first/last month for this metric),
  - daily normalization uses calendar_days (includes zero-order days).
- Added fairness diagnostics in table: `Calendar Days` and `Active Day Rate`.
- Added new Day-of-Month analytics (1-31) to reporting pipeline:

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
- Review `data/vevo/report_20260301-20260331__test.html` visually and decide which parts of the new dashboard shell to keep:
  - sidebar structure,
  - card/chart spacing,
  - palette,
  - section grouping,
  - mobile behavior.
