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

- Run daily at `04:15 Europe/Bratislava`, after the expected `03:45`
  reconciliation.
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

Before `2026-09-02 03:45 Europe/Bratislava`, the automation must not dispatch
the A/A checkpoint workflow and must not read or report any experimental
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

## Due A/A Checkpoint

At the exact next due gate, and only then:

1. Synchronize clean `main` and re-run the checked-in workspace and measurement
   window validators.
2. Dispatch only
   `.github/workflows/check-vevo-growthbook-production-aa-window.yml` from
   `main` with `confirm_checkpoint=true`.
3. Let the protected workflow perform the AWS/Fargate hard gate before its one
   permitted aggregate Athena query. The query may return only cumulative
   eligible-device count and may not select an arm or outcome.
4. Require a successful run, record its exact run ID and head commit, and
   download only artifact `vevo-growthbook-aa-window-checkpoint`.
5. Require the ZIP to contain only
   `vevo-growthbook-aa-window-checkpoint.json`, calculate the file SHA-256
   independently, and delete temporary downloads after recording.
6. On a new branch, record the canonical artifact with:

```text
python scripts/record_growthbook_aa_window_checkpoint.py --evidence <downloaded-artifact>/vevo-growthbook-aa-window-checkpoint.json --snapshot projects/vevo/growthbook_aa_snapshot.json --output projects/vevo/growthbook_aa_snapshot.json --expected-evidence-sha256 <sha256> --expected-workflow-run-id <run-id> --expected-main-commit <head-sha>
```

7. Run the focused checkpoint, workspace, measurement-window, and security
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
5. Wait until the exact A/A end plus 24 hours, collect one protected product
   CTA baseline artifact, and freeze the CTA sample offline.
6. Complete the versioned CTA runtime and activation readbacks before any CTA
   start.
7. During CTA collection, use only the separate outcome-blind CTA checkpoint
   path. Stop assignment only at its pre-registered sample/day-42 boundary.
8. Wait through the exact stop plus 14-day follow-up and execute exactly one
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
