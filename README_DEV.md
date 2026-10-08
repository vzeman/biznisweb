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

## ROY and VEVO report image release and dated regeneration

After a reviewed PR is merged and its exact `build-and-push-ecr.yml` run succeeds,
use a clean checkout whose HEAD equals fetched `origin/main`. Resolve the real
current Fargate task/IP (or confirmed absence), the selected report service, and
`/app` before dispatch. The committed helper uses the current schedule target,
not the obsolete original revision pins in the first managed migration runbook.

```text
python scripts/vevo_report_image_release.py run --project roy --profile codex --commit <full-main-SHA> --to-date YYYY-MM-DD --independent-contract-sha256 <reviewed-source-contract-SHA256>
python scripts/vevo_report_image_release.py status --project roy --profile codex --release-id <32-character-release-id>
```

`--profile` is optional when the usual AWS credential chain is configured.
`--project` is restricted to `roy` and `vevo` (legacy default). The immutable
policy binds ROY to `roy-daily-report-email`, `roy-reporting-daily`, and
`daily-reports/roy-sk`; VEVO uses its existing service, family and sink. Wait for
every settings-triggered monthly accounting deployment to finish before starting
this controller, because all other schedules become protected baseline state.
`--timeout-seconds` bounds each report task (default 7200; allowed 300–14400).
An optional fresh `--release-id` binds dispatch idempotency and private evidence;
never reuse an ID to retry an uncertain run. `status` is read-only. It shows the
last retained phase and exact owned task ARNs, without financial data or secrets.

The helper proves the successful exact-main build and immutable image, snapshots
all default-group schedules, and takes the selected project's CAS lease. ROY uses
`data/roy/reporting/runtime/image-release.json`; VEVO retains its legacy migration
lease. Both mutation paths reject an active or uncertain ROY peer lease. The
controller pauses only the selected reporting schedule, verifies a quiet dispatch window, then clones its
task definition with only the image changed. The diagnostic task uses an isolated
temporary role and the existing query-only report probe. Actual task/IP/image,
`curl localhost` marker, explicit authorization, all full/7d/30d/90d artifacts,
quality and the exact full-history start and requested end date must pass before promotion. A finite live entry
then repeats the local host marker and regenerates through the requested date
with fresh reads and explicit email/invoice/creditnote-guard skips. The helper
verifies the new live manifest and every artifact hash before restoring the
original enabled schedule. Its date, cache and email skips apply only to that
one-off task; normal schedule configuration is preserved. UI verification follows
these host/output gates.

Receipts are encrypted and read back under private
`data/<project>/reporting/image-releases/<release-id>/`; candidate evidence remains
under `data/<project>/reporting/runtime/probes/<release-id>/`. Record safe references,
hashes and the outcome in the product's `PROJECT_STATE.md`. The helper does not update or claim
validity of historical `runtime/current.json`; its current-target proof is
separate, and the shared lease only provides deployment exclusion.

For releases checked against an independent primary-source audit, pass
`--independent-contract-sha256` for the exact private expected contract. Both
2026-10-08 corrective releases require it. After the isolated probe has stopped
and its temporary role has been removed, the controller keeps the schedule
disabled and renews its lease while awaiting
`data/<project>/reporting/runtime/probes/<release-id>/review/independent-source.json`.
The probe role cannot write this review. Missing evidence times out; rejected
evidence fails before image promotion or live dispatch. The existing pre-live
recovery logic then preserves the original schedule and recovery ownership.

Generate the review only after the independent verifier has checked all four
actual probe periods against the reviewed contract and provider evidence.
Archive its detailed evidence JSON first, using AES256 encryption, the expected
bucket owner, conditional creation and complete SHA-256 readback. The review
must contain exactly: `schema_version` (integer 1), `project`, `release_id`,
`source_commit`, `image_digest`, `report_from_date`, `report_to_date`,
`probe_manifest_sha256`, `source_contract_sha256`,
`verification_script_sha256`, `approved` (literal true), `period_checks`, and
`evidence` with `key` and `sha256`. Each of `latest`, `7d`, `30d` and `90d` must
have exactly four literal true checks: `financial_aggregates`,
`country_attribution`, `shared_fixed_costs`, and `advertising_reconciliation`.
The detailed evidence must repeat the same bindings and checks and contain no
errors. It is restricted to the selected project's private `analyses/` prefix
or `data/reporting/analyses/`; a report output or another project's prefix is
not valid evidence. Write the review conditionally and verify its full hash.

This gate proves the independent aggregate comparison, not individual order
membership from a probe without a financial CSV. The final live CSV comparison
is still required. Compare raw advertising sources at their documented precision;
daily values rounded for display are not an exact account aggregate. Preserve
the existing production reconciliation limits and explicitly record any source
delta; do not enlarge tolerances to approve a release.

Failed authorized probes retain only the exact project/release-tagged quality JSON
under their private diagnostic prefix, plus a separate hash-bound failure marker.
The controller preserves its key/hash/status before cleanup. Failure evidence is
never a successful artifact manifest, never changes live aliases, and never
permits promotion. Missing or malformed diagnostics do not weaken rejection.

Google measured country costs are reconciled to the same-period account total.
The provider's reported `unknown` country remains separate from `unallocated`
(account cost minus the full physical-country report). Neither residual is
assigned to SK/CZ/HU; both remain in company advertising costs. Reported country
values, account/currency/date identity and independent daily totals must still
reconcile. Coverage is visible beside country results, and unknown/unallocated
rows have no MER. Partial country coverage is a disclosed warning, while source
errors, negative residuals or inconsistent totals block publication.

The existing audited VEVO compensation exception is bound to its canonical
source fingerprint and zero-VAT, single-line native-total evidence. It does not
fabricate missing provider price elements or expand order eligibility; changed
source fields fail closed. Client settings hold this evidence separately from
the reusable validator and source currency rounding rules.

Before live dispatch, a known failure stops only the owned task, removes its
verified temporary role, restores the owned original schedule and deactivates
the unused candidate. An uncertain launch retains the exclusion lease and paused
schedule for inspection. Any failure after live dispatch preserves the proven
candidate image with the selected reporting schedule paused: output might already have published,
so the helper never automatically reruns, sends email, rewrites aliases or rolls
back that image. Inspect receipts, exact ECS tasks, logs and the live manifest
before a separately reviewed recovery. Never clear an uncertain lease or repeat
`run` merely to retry. Other schedules and foreign resources are never repaired
or rolled back by this helper.

ROY may proceed around an explicitly reviewed uncertain VEVO release only with
all three `--retained-peer-release`, `--retained-peer-lease-sha256` (SHA-256 of
canonical JSON bytes), and `--retained-peer-lease-etag` arguments. The exact peer
owner/body/ETag, disabled schedule and published outputs are protected before
and after lease acquisition and throughout the release. It never clears or
transfers that VEVO lease. An absent/released peer needs no retained arguments;
an active peer, including stale active, always blocks. Partial or irrelevant
proof arguments also block. Legacy deployment callers must have read access to
the ROY lease key: AccessDenied is uncertainty, never an absent lease.

For a separately reviewed, stopped VEVO release whose live outputs are unchanged,
the same `run` accepts `--recover-paused-release <old-id>` and
`--recovery-receipt-sha256 <reviewed-latest-failure-sha>`. This is an explicit
recovery contract, not a generic disabled-schedule override. It checks the
immutable receipt chain, exact paused schedule/definition, stopped owned task
identities, successful probe host proof, unchanged outputs, absent temporary
role and the previous uncertain lease. If ECS has expired a stopped task, supply
`--recovery-readback-key <private-S3-key>` and
`--recovery-readback-sha256 <reviewed-sha>` for the previously archived independent
terminal readback. That fallback applies only to ECS `MISSING`, never a failed
or conflicting read. Keep a durable private post-stop readback before ECS task
history expires.

Recovery conditionally transfers the lease without deleting it or enabling the
old target. The new probe and live run retain the paused state; only successful
new publication enables the schedule. A confirmed pre-live failure restores the
old disabled target and conditionally returns its uncertain lease. A live or
uncertain failure still requires inspection. Other schedules are captured anew
after concurrent CI finishes, with changes since the stopped release recorded
in recovery evidence, then strictly protected for the whole new release.

## Private VEVO order-audit workbook

`projects/vevo/manual_audit_workbook.mjs` presents the private order/item audit
without fetching data or modifying reporting. It reads the complete source and
classification JSON, the independent item-cost and overhead JSON, selected
examples, and measured Meta/Google country-day extracts. Input filenames are
explicit in the builder. Keep these business records in ignored `data/` and in
the encrypted private artifact archive, never public Git.

Use the Codex Spreadsheets skill and its bundled dependency loader. Copy the
committed builder into an ignored working directory, create a `node_modules`
junction/symlink there to the loader's bundled packages, and invoke the bundled
Node executable with the absolute input and output directory arguments. The
copy is generated from this repository; no local-only source is required. Do
not install packages into or modify the bundled runtime. The builder verifies
sample counts and overhead, scans formula errors, tests recalculation, renders
all sheets and exports one workbook. Review the rendered ranges and update the
stated audit/release status before sharing a final, recalculated version.
