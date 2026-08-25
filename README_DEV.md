# Development Workflow

This repository is the source of truth for the BizniWeb reporting stack.
Do not treat any local Desktop/Downloads scripts as authoritative.

## Rules

- Start every session with `git pull --rebase` on the active branch.
- End every significant step with `git push`.
- Keep all reusable scripts in this repository.
- Never keep required runtime/deploy logic only on one PC.
- Production/runtime secrets must not be committed.
- Update `PROJECT_STATE.md` after each major change.
- This repository owns Reporting only; Doklady and OpenClaw must live in their own repositories.
- Treat branches as short-lived work units, not as long-lived product buckets.

## Multi-PC Workflow

### On any machine before work

```bash
git fetch --all --prune
git status
git pull --rebase
```

### On any machine after work

```bash
git status
git add ...
git commit -m "..."
git push
```

## Bootstrap

### macOS / Linux

```bash
./scripts/bootstrap.sh
```

### Windows PowerShell

```powershell
./scripts/bootstrap.ps1
```

Bootstrap does:
- install git hooks
- create `.env` from `.env.example` if missing
- validate required env keys
- create `.venv` if missing
- install Python dependencies

## Env contract

Required baseline keys are listed in `.env.required`.
Feature-specific keys stay optional until the feature is used.

## Observability baseline

- Local snapshot:

```powershell
python scripts/observability_snapshot.py --pretty
```

- CI snapshot:
  - `.github/workflows/observability-check.yml`
  - emits an artifact with the latest project/artifact/source-health view

Use this before deploys when you want a fast view of:
- latest report HTML / export / CFO artifacts per project
- latest `data_quality_*.json`
- whether the newest run is partial and which source degraded

## VEVO GrowthBook CLI control plane

Chrome automation is not a Production control plane. The repository pins the official
GrowthBook CLI under `tools/growthbook-cli` and exposes a fail-closed VEVO wrapper.
The wrapper currently supports only authenticated read-only preflight and mutation
dry-run; it cannot start or publish an experiment.

Install the pinned CLI after bootstrap:

```powershell
npm ci --prefix tools/growthbook-cli
```

```bash
npm ci --prefix tools/growthbook-cli
```

Configure a GrowthBook Personal Access Token or Secret Key in the OS keychain. Do not
put it in Git, a committed config file, a command-line argument, or chat history:

```powershell
node tools/growthbook-cli/node_modules/growthbook/bin/growthbook.js auth login
```

Run the live read-only gate and the network-free mutation plan:

```powershell
python scripts/vevo_growthbook_cli.py preflight --pretty
python scripts/vevo_growthbook_cli.py plan --pretty
```

The preflight is bound to the schema-9 activation manifest, experiment
`exp_19g6mmt5wugpk`, feature `vevo-sk-aa-assignment`, live revision 2, draft revision
3, Production disabled at read time, 100% experiment coverage, a 50/50 split, and no
merge conflict. It stores only sanitized summaries and SHA-256 values. Mutation support
must be introduced by a separate reviewed change after this authenticated gate passes.

## Client scaffolding template

To scaffold a new reporting client from the internal template:

```powershell
python scripts/scaffold_client.py my-client --display-name "My Client"
```

This creates a new `projects/<slug>/` bundle from `templates/reporting-client/`.

## Current repo scope

This repo contains the reporting codebase.
OpenClaw and Doklady may integrate with it, but they are not managed here.
Canonical product split:
- Reporting: `vzeman/biznisweb`
- Doklady: `Terem21/doklady-saas`
- OpenClaw: `Terem21/openclaw-agents-platform`

## Branch discipline

- `main` is the source of truth for reporting.
- Use short-lived branches for concrete work only, for example `codex/roy-inventory-metrics`.
- Delete merged branches quickly so GitHub branch lists stay operationally readable.
- If a branch starts representing a separate product, stop and move that product into its own repository.

Use `PROJECT_STATE.md` only for this repo plus short integration notes.
