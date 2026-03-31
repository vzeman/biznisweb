# PROJECT_STATE

Last updated: 2026-03-30
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

## 8) Next Exact Step

- Regenerate VEVO + ROY reports and validate both phase metrics in HTML outputs:
  - Week-of-Month (equalized 4x7)
  - Day-of-Month (1-31 normalized by calendar occurrences)

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
