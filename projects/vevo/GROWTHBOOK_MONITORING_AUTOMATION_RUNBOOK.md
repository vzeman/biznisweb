# VEVO GrowthBook Monitoring Automation Runbook

## Purpose

This file is the versioned contract for the Codex automation named
`vevo-production-a-a-monitoring`. The automation is a coordinator only. The
checked-in manifests, validators, offline recorders, and protected GitHub
workflows remain the executable source of truth.

The current phase is the invisible Production A/A experiment
`vevo-sk-aa-001`. The future CTA experiment must remain unstarted until every
preceding checked-in gate is satisfied through a reviewed pull request.

## Schedule And Repository Boundary

- The repository-owned GitHub monitor runs daily at `04:15
  Europe/Bratislava`, after the expected `03:45` reconciliation, and does not
  depend on the local PC.
- The Codex heartbeat runs daily at `09:00 Europe/Bratislava` to inspect and
  report the GitHub result when the local PC and desktop app are available. It
  is not the execution dependency for the overnight infrastructure check.
- Work only from the repository containing this file and synchronize exact
  `main` before reading a gate or dispatching a workflow.
- If the worktree is dirty, the branch is unexpected, or `main` differs from
  `origin/main`, do not modify or dispatch anything. Report the blocker.
- All state transitions must be made on a short-lived `codex/` branch, pass
  validation, and reach `main` only through a reviewed pull request.
- Never put credentials, raw AWS responses, CloudWatch messages, event or
  device identifiers, customer or order data, or downloaded temporary
  artifacts in Git.

## Current Frozen A/A Boundary

The authoritative state is
`projects/vevo/growthbook_aa_snapshot.json`:

- measurement start: `2026-08-25T22:00:00Z`;
- full local dates: at least `2026-08-26..2026-09-01`;
- minimum sample: `1,000` eligible devices;
- first resolution checkpoint: `2026-09-02 03:45 Europe/Bratislava`;
- stopping rule: resolve at the first successful daily reconciliation with at
  least `1,000` eligible devices, otherwise extend by exactly one whole local
  day.

Do not infer a replacement date from a missed local automation run. The frozen
manifest and its consecutive checkpoint history determine the next valid due
gate.

## Before The First Due Checkpoint

Before `2026-09-02 03:45 Europe/Bratislava`, the scheduled A/A checkpoint may
start only far enough to run its checked-in local gate; that gate must skip
before AWS credentials and before the population query. No manual checkpoint
may be dispatched, and no automation may read or report any experimental
population or result. In particular, it must not read:

- total or eligible device counts;
- arm counts, assignment split, or SRM;
- conversion, add-to-cart, revenue, CM1, or order outcomes;
- Meta campaign, ad-set, ad, or placement dimensions;
- performance values, guardrail values, or any other A/A result.

Only sanitized infrastructure health may be checked:

- AWS account `919341186960` and region `eu-central-1`;
- stacks `vevo-growthbook-production` and
  `vevo-growthbook-reconciliation-production`;
- Fargate instance marker `N/A:Fargate`, current private task IP, service
  `vevo-growthbook-reconcile-production`, runtime path `/app`, task definition,
  and immutable image digest, all matching the versioned deploy evidence;
- enabled reconciliation schedule `vevo-growthbook-reconcile-production` at
  `03:45 Europe/Bratislava` and its latest non-diagnostic success marker;
- generated/published reconciliation parity without reporting row counts;
- empty retained DLQ and all three reconciliation alarms clear;
- unchanged enabled source schedule `vevo-daily-report-email`.

The immutable comparison source is
`projects/vevo/growthbook_production_reconciliation_deploy_evidence.json`. If
any identity, marker, schedule, alarm, DLQ, or source-schedule invariant drifts,
stop and report it. Do not deploy a fix, open a data query, or continue to a UI
test from the monitoring run.

### Credential-independent daily monitor

The repository-owned AWS readback is
`.github/workflows/monitor-vevo-growthbook-production-aa-infra.yml`. It is
main-only and schedules both possible UTC equivalents of `04:15
Europe/Bratislava`; a pre-credential timezone gate executes exactly the
currently correct slot and skips the other across daylight-saving changes.
The Codex heartbeat observes this workflow at its later `09:00` readback slot
rather than requiring an AWS key on the local PC. It must not dispatch a
duplicate while the scheduled run is queued or in progress. This separation is
intentional because local scheduled tasks require the computer to be powered on
and the desktop app running, as documented in
[OpenAI Scheduled tasks](https://learn.chatgpt.com/docs/automations).

Before the first natural reconciliation is due, the workflow verifies only the
stacks, schedule, task definition/image inherited from the hash-bound localhost
gate, alarms, empty DLQ, and source schedule. After a natural run is due, it
additionally binds the one exact successful Fargate task, current private IP,
success-marker hash, and generated/published parity hash. Because GitHub
scheduled workflows can start after ECS has expired a short-lived stopped-task
record, the monitor discovers the exact task from the bounded CloudWatch
success marker and Scheduler-authenticated CloudTrail `RunTask` event first.
It prefers retained ECS state and may recover the original private IP from the
same exact CloudTrail response only when the task ID, task definition, group,
Scheduler role, time window, marker, and VPC range all match. It performs no
guessing when neither retained source contains the IP: schema v2 records a
`null` private IP together with the exact
`cloudtrail_run_task_retention_recovery` source and a false retained-runtime
flag. That result-blind proof is sufficient only for reconciliation monitoring;
it never satisfies the separate live IP plus localhost-marker hard gate needed
before an infrastructure mutation. The workflow performs no
Athena, S3 data, GrowthBook, Meta, GTM, or BiznisWeb request and emits no row
count.

Its only artifact is canonical
`vevo-growthbook-production-aa-infra-health.json`, validated by
`scripts/validate_growthbook_aa_infra_health_evidence.py`. Raw stack, ECS,
CloudWatch, alarm, queue, and schedule responses are deleted before the
identity-free artifact is uploaded. A missing or failed daily run is an
infrastructure-monitoring blocker; it does not authorize the A/A checkpoint or
any live mutation.

### PC-independent outcome-blind checkpoint capture

The repository-owned checkpoint workflow also schedules both possible UTC
equivalents of `04:30 Europe/Bratislava` (`30 2 * * *` and `30 3 * * *`). Its
pre-credential timezone gate admits only the currently correct DST slot. Before
the first due date, after a resolved window, on the wrong DST slot, or when the
same checkpoint is already committed, it exits successfully with
`RUN_CHECKPOINT=false`; every AWS, query, evidence, upload, cleanup, and summary
step is separately conditioned on `RUN_CHECKPOINT=true`.

For an admitted scheduled run, the checkpoint index comes from the frozen
local calendar date relative to the pre-registered first due date, not from the
length of the Git checkpoint history. Therefore GitHub can preserve each exact
daily checkpoint artifact even while the desktop PC is off and earlier
artifacts have not yet been recorded. The query and evidence contract is
unchanged: one cumulative eligible-device count, no arms and no outcomes. Each
artifact remains valid only inside its own existing due-to-due-plus-one-day
gate and is retained for 90 days.

## Due A/A Checkpoint

At the exact next due gate, and only then, the admitted `04:30` cloud slot
captures the checkpoint automatically. A manual dispatch with
`confirm_checkpoint=true` remains a same-gate fallback:

1. Synchronize clean `main` and re-run the checked-in workspace and measurement
   window validators.
2. Use only `.github/workflows/check-vevo-growthbook-production-aa-window.yml`
   from `main`; normally consume its scheduled artifact, or dispatch it manually
   with `confirm_checkpoint=true` only inside the exact same daily gate.
3. Let the protected workflow bind the exact reconciliation through the bounded
   success marker and Scheduler-authenticated CloudTrail `RunTask` event before
   its one permitted aggregate Athena query. It prefers retained ECS state; if
   that short-lived state has expired, schema `2` records a null private IP,
   `cloudtrail_run_task_retention_recovery`, and `runtime_state_retained=false`.
   This historical read-only proof never satisfies the live-IP plus localhost
   marker hard gate for an infrastructure mutation. The query may return only
   cumulative eligible-device count and may not select an arm or outcome.
4. Require a successful run, record its exact run ID and head commit, and
   download only artifact `vevo-growthbook-aa-window-checkpoint`.
5. Require the ZIP to contain only
   `vevo-growthbook-aa-window-checkpoint.json`, calculate the file SHA-256
   independently, and delete temporary downloads after recording.
6. On a new branch, record the canonical artifact with:

```text
python scripts/record_growthbook_aa_window_checkpoint.py --evidence <downloaded-artifact>/vevo-growthbook-aa-window-checkpoint.json --snapshot projects/vevo/growthbook_aa_snapshot.json --output projects/vevo/growthbook_aa_snapshot.json --expected-evidence-sha256 <sha256> --expected-workflow-run-id <run-id> --expected-main-commit <head-sha>
```

7. If the PC was offline across more than one checkpoint, process the retained
   artifacts in ascending checkpoint-index order. Stop at the earliest artifact
   whose count reaches `1,000`; ignore every later artifact after that qualifying
   checkpoint, because the pre-registered stopping rule resolves at the first
   qualifying boundary.
8. Run the focused checkpoint, workspace, measurement-window, and security
   validation plus `git diff --check`, then commit, push, and merge through a
   reviewed pull request.

A count below `1,000` may add only the hash-bound checkpoint history and the
next whole-local-day due boundary. The first count at or above `1,000` may
resolve only the A/A window. Neither case opens CTA traffic, stops GrowthBook,
or permits reading arms or outcomes.

## Post-Resolution Sequence

After the resolved checkpoint is merged, follow the exact checked-in gates in
this order:

1. Bind the resolved reporting-quality object and reviewed manual observation
   with `record_growthbook_aa_evidence_gates.py`.
2. Run and independently record both protected A/A evidence producers.
3. Build the protected A/A snapshot and require the offline evaluator decision
   `PASS`.
4. Record the PASS-bound completion, then perform and record the reviewed
   manual A/A stop/readback.
5. Reconfirm the exact one-seat `$40/month` recurring offer at action time,
   perform the manually authorized GrowthBook Pro upgrade, create and
   query-test all six Preview/Production p75 metrics, and record the canonical
   sanitized readback while CTA remains draft at `0%`.
6. Wait until the exact A/A end plus 24 hours, collect one protected product
   CTA baseline artifact, and freeze the CTA sample offline.
7. Wait through and record the completed-A/A source cohort's exact 21-day
   prelaunch lifecycle preflight, then complete the CTA-only runtime handoff.
8. On exact synchronized `main`, require the read-only CTA start-readiness gate
   to pass before the reviewed manual CTA start and activation readback.
9. During CTA collection, use only the separate outcome-blind CTA checkpoint
   path. Stop assignment only at its pre-registered sample/day-42 boundary.
10. Wait through the exact stop plus 21-day follow-up (7-day attribution plus
   14-day per-order lifecycle maturity) and execute exactly one
   protected final snapshot. Record the offline decision without applying it.

The full operational detail remains in
`projects/vevo/GROWTHBOOK_PRODUCTION_AA_ACTIVATION_RUNBOOK.md` and
`projects/vevo/GROWTHBOOK_CTA_ACTIVATION_RUNBOOK.md`.

## Permanent Safety Rules

- Never automatically start or stop a GrowthBook experiment, publish GTM,
  change Meta Ads routing, or mutate BiznisWeb, prices, product content, stock,
  cart, checkout, payments, or orders.
- Never call or apply a winner from an unresolved, running, or immature cohort.
- Never bypass a failed manifest, workflow, provenance, hash, host, marker,
  privacy, quality, or performance gate.
- For any future infrastructure change: first confirm exact instance marker,
  current private IP, service, task definition/image, and `/app`; only then
  change/deploy; verify `curl localhost` plus markers on the host; only then run
  the required UI test.
- If a required gate cannot be proven, stop safely and report the exact blocker
  instead of guessing or shifting the plan.
