# PROJECT_STATE

Last updated: 2026-03-31
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
- Partial upstream failures (ads/weather/etc.) still need explicit product-level handling policy

## 8) Next Exact Step

- Implement explicit partial-data handling for upstream failures:
  - detect/report missing FB/Google/weather sources as partial data instead of silent zeros,
  - surface source-health markers in HTML/CFO outputs,
  - define when report generation should continue vs fail hard.

## 9) Change Log

### 2026-03-30
- Added env governance baseline: `.env.required`, pre-commit hook, CI env check.
- Added cross-platform bootstrap scripts for macOS/Linux and Windows PowerShell.
- Narrowed `PROJECT_STATE.md` to this repository only.
- Removed cross-project state ownership from this repo; left only integration notes.

### 2026-03-31
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
