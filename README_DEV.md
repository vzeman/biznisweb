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

For a VEVO data refresh on the already deployed image, use
`python scripts/vevo_report_current_rerun.py --help` from clean, synchronized
`main`. Bind the controller commit separately from the deployed image's source
commit, and supply the freshly verified expected task-definition ARN and image.
This rerun pauses only the VEVO report schedule under the existing lease,
uses the image-resident host/localhost gate with fresh provider reads, skips
email/invoices/inline credit-note actions, verifies all four periods, and restores
the exact original schedule. It never registers or deregisters a task definition
or creates a probe role. Verify the new order delta before a separately authorized
email resend. An uncertain or attempted-live failure retains a paused schedule
and uncertain lease for inspection; do not blindly retry or force-release it.

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
`--timeout-seconds` bounds each report task and the independent-review wait
(default 7200; allowed 300–14400).
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

The VEVO live host wrapper also retains pre-publication failures under its own
`daily-reports/vevo/source-inputs/<release-id>/diagnostics/` prefix. The bounded
projection includes all four periods' publication predicates and allowlisted
aggregate QA fields; it excludes customer rows, arbitrary provider messages and
exception text. The failure marker binds the host, localhost identity hash,
source and dates. Storage failure still re-raises the original report failure.
A failure after entering the uploader is never labeled a safe pre-publication
rejection. No diagnostic object authorizes publication or email.

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

ROY enables `realized_revenue.unsupported_shipped_merchandise_policy: "error"`.
Before status filtering, an unsupported payment on a fulfilled order with
nonzero native item amounts blocks reporting instead of silently omitting the
order. Ambiguous payment elements and invalid item amounts also block. This
does not make an unknown method eligible: verified payment IDs belong in the
project configuration. A zero product acquisition cost is not a zero-revenue
exception. For a present unsupported payment, only finite zero native item
values retain their existing exclusion. A valid price-element list without a
payment element keeps its existing eligibility decision; this preserves zero-total
exchanges with offsetting product lines. It is not a netting exception for a
present unknown payment. The existing exact missing-metadata overrides remain
separate. The default is
`"off"`, so VEVO's existing eligibility and historical legacy policy are unchanged.
The guard propagates through fresh, cached and fallback reads. Current caches
retain raw orders, and each read reapplies the active payment/status rules.

For a foreign-payment correction, audit native excluded merchandise independently
of the exporter decision. A report and an expected contract using the same
eligibility function can agree while both omit a valid order. Retain complete
provider-visible history and native payment/shipping identities, but distinguish
that source coverage from the configured financial report interval. Country is
delivery geography with invoice fallback; other destinations remain in company
totals and the measured `Other` profitability group.

Complete reporting inventories are an optional client policy in
`projects/<project>/order_inventory_policy.json`, with schema version 1 and a
strict boolean `complete_order_inventory_enabled`. VEVO enables this policy;
an absent file defaults to the existing path, including ROY. The reusable loader
anchors the highest internal order ID, scans ascending IDs with validated
overlap and bounded repair, and filters the requested calendar dates only after
the full inventory completes. It uses the same payment enrichment and existing
financial/status rules. The scanner has a shared one-hour deadline and 5,000-page
budget; transport retries are disabled only in its scope and restored afterward.

VEVO caches require completed-scan provenance plus a hash of the exact day rows.
Unmarked old caches refresh; an incomplete scan never writes new day caches.
The report probe wraps only `scan_reporting_order_inventory`: deliberate overlap
is allowed only inside the exact callback/client session whose original scanner
returns successfully. Failed or pending sessions and ordinary duplicate pages
continue to block publication. Invalid-page variables, predicate flags and native
responses are retained privately under the owned probe prefix, never in logs or Git.
The same complete path applies to the enabled month/period entry points. Tests
cover source insertion/deletion, a 940-order boundary day, payment fallback,
cache tampering, deadlines, legacy ROY paths and unchanged monetary rules.

BiznisWeb's [API guidance](https://www.biznisweb.sk/a/1382/best-practices)
recommends the public domain to avoid redirects. The VEVO installation redirects
its FLOX API URL to `https://www.vevo.sk/api/graphql`, which is already in the
committed native-host policy and has passed direct native reads. Private source
collectors use that verified endpoint explicitly, retaining routing evidence
without automatically forwarding a token to an unverified host. This does not
change the runtime secret or create a new financial policy.

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

For a separately reviewed, stopped ROY or VEVO release whose live outputs are unchanged,
the same `run` accepts `--recover-paused-release <old-id>` and
`--recovery-receipt-sha256 <reviewed-latest-failure-sha>`. This is an explicit
recovery contract, not a generic disabled-schedule override. It checks the
selected project's receipt prefix, explicit project identity (required for ROY),
task family, service, marker, temporary role and scoped lease. It then checks the
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

A failed same-image `vevo_report_current_rerun.py` has a different receipt
contract: it never registered a candidate or ran an isolated probe. Use the
image-release controller's separate `--recover-current-rerun <old-id>` argument
with the exact `--recovery-receipt-sha256`. Do not feed this chain to
`--recover-paused-release`. The admission checks the seven current-rerun
receipts, unchanged active definition/image, unchanged nine publication objects,
the original schedule with only State disabled, stopped owned live task and
uncertain lease. If ECS returns MISSING, the reviewed private failure ZIP and
its SHA256 supplied through `--recovery-readback-key` and
`--recovery-readback-sha256` bind the original observer projection, host marker,
observer implementation and receipt bytes. This projection is not a raw ECS
response; conflicting or unavailable reads cannot use it. The normal new-image
isolated probe, live verification, conditional ownership transfer and final
dynamic schedule restoration still apply. Neither recovery mode sends email.
This admission binds the old lease ETag as well as its owner. If a subsequent
pre-live recovery fails and returns that owner with a new ETag, the original
archive cannot authorize another attempt; review the new terminal/handoff
evidence first. Current-rerun recovery disables automatic SDK retries for its
mutations, retaining unknown outcomes for inspection.

A single proven failure before any probe dispatch can supply that new review
through `--recovery-preprobe-return-key` and
`--recovery-preprobe-return-sha256`, together with the original current-rerun
arguments. This private handoff binds the complete ten-receipt rollback chain,
the original failure archive and lease ETag, the inactive candidate, absent
probe role/tasks/artifacts, unchanged outputs and protected schedules, and the
exact returned uncertain lease value and new ETag. Fresh reads recheck these
conditions before conditional ownership transfer. It cannot admit a live or
unknown dispatch, another return chain, a new lease generation, or an arbitrary
ETag override. The original archive remains immutable.

GitHub inspection uses explicit GET requests with at most three 45-second
attempts and 2/5-second backoff for recognized transport timeouts, connection
resets, temporary DNS failures and HTTP 502/503/504. Authorization errors,
invalid data and real competing workflows still fail closed. Only the read is
retried; task dispatch, lease writes, schedule changes and the deployment itself
are never automatically replayed. Exhausted reads retain the existing owned
cleanup and paused/uncertain recovery behavior.

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
