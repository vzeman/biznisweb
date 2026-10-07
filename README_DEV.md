# Development Workflow

This repository is the source of truth for the BizniWeb reporting stack.
Do not treat any local Desktop/Downloads scripts as authoritative.

Order/invoice automation audits, historical backfill, immutable deployment and
runtime recovery are documented in [the operations runbook](projects/ORDER_AUTOMATION_OPERATIONS.md).

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

## VEVO report image release and dated regeneration

After a reviewed PR is merged and its exact `build-and-push-ecr.yml` run succeeds,
use a clean checkout whose HEAD equals fetched `origin/main`. Resolve the real
current Fargate task/IP (or confirmed absence), `vevo-daily-report-email`, and
`/app` before dispatch. The committed helper uses the current schedule target,
not the obsolete original revision pins in the first managed migration runbook.

```text
python scripts/vevo_report_image_release.py run --profile codex --commit <full-main-SHA> --to-date YYYY-MM-DD
python scripts/vevo_report_image_release.py status --profile codex --release-id <32-character-release-id>
```

`--profile` is optional when the usual AWS credential chain is configured.
`--timeout-seconds` bounds each report task (default 7200; allowed 300–14400).
An optional fresh `--release-id` binds dispatch idempotency and private evidence;
never reuse an ID to retry an uncertain run. `status` is read-only. It shows the
last retained phase and exact owned task ARNs, without financial data or secrets.

The helper proves the successful exact-main build and immutable image, snapshots
all default-group schedules, and takes the existing reporting migration lease.
It pauses only VEVO reporting, verifies a quiet dispatch window, then clones its
task definition with only the image changed. The diagnostic task uses an isolated
temporary role and the existing query-only report probe. Actual task/IP/image,
`curl localhost` marker, explicit authorization, all full/7d/30d/90d artifacts,
quality and requested end date must pass before promotion. A finite live entry
then repeats the local host marker and regenerates through the requested date
with fresh reads and explicit email/invoice/creditnote-guard skips. The helper
verifies the new live manifest and every artifact hash before restoring the
original enabled schedule. Its date, cache and email skips apply only to that
one-off task; normal schedule configuration is preserved. UI verification follows
these host/output gates.

Receipts are encrypted and read back under private
`data/vevo/reporting/image-releases/<release-id>/`; candidate evidence remains
under `data/vevo/reporting/runtime/probes/<release-id>/`. Record safe references,
hashes and the outcome in `PROJECT_STATE.md`. The helper does not update or claim
validity of historical `runtime/current.json`; its current-target proof is
separate, and the shared lease only provides deployment exclusion.

Before live dispatch, a known failure stops only the owned task, removes its
verified temporary role, restores the owned original schedule and deactivates
the unused candidate. An uncertain launch retains the exclusion lease and paused
schedule for inspection. Any failure after live dispatch preserves the proven
candidate image with VEVO reporting paused: output might already have published,
so the helper never automatically reruns, sends email, rewrites aliases or rolls
back that image. Inspect receipts, exact ECS tasks, logs and the live manifest
before a separately reviewed recovery. Never clear an uncertain lease or repeat
`run` merely to retry. Other schedules and foreign resources are never repaired
or rolled back by this helper.
