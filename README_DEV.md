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

## Client scaffolding template

To scaffold a new reporting client from the internal template:

```powershell
python scripts/scaffold_client.py my-client --display-name "My Client"
```

This creates a new `projects/<slug>/` bundle from `templates/reporting-client/`.

## Current repo scope

This repo contains the reporting codebase.
OpenClaw and Doklady may integrate with it, but they are not managed here.
Use `PROJECT_STATE.md` only for this repo plus short integration notes.
