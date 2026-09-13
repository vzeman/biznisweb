# VEVO managed report image migration — proposed design

Date: 2026-09-13. Status: **offline proposal; no implementation or deployment authorized by this document**. Reviewed source baseline: main `b417caa736cfbd1f97b20821cacc39b144e1e460`. The separate reporting correction is being prepared on `codex/report-status-identity-20260913`.

The smallest safe migration changes only the VEVO report image and its explicitly reviewed skip configuration. It must first separate historical GrowthBook evidence from current report-runtime authority. Replacing every occurrence of task revision 33 would invalidate historical evidence and would not establish a verified replacement.

## Current dependencies established from source

The five live workflow gates are:

| Workflow under `.github/workflows/` | Current report dependency | Required change |
| --- | --- | --- |
| `monitor-vevo-growthbook-production-aa-infra.yml` | Line 34 pins revision 33; lines 238–246 compare the current source schedule to the historical deployment; generated health evidence repeats that historical source | Resolve the separately verified current binding; emit independently bound current health evidence |
| `check-vevo-growthbook-production-aa-window.yml` | Line 298 requires the revision-33 suffix | Exact current account/cluster/schedule/definition/image binding |
| `check-vevo-growthbook-production-cta-window.yml` | Line 324 requires the revision-33 suffix | Same shared gate |
| `check-vevo-growthbook-production-cta-safety.yml` | Line 172 requires the revision-33 suffix | Same shared gate |
| `build-vevo-growthbook-production-cta-final-snapshot.yml` | Line 231 requires the revision-33 suffix | Same shared gate |

The reconciliation task, collector, allocation, measurement windows and activation gates remain unchanged. Historical `projects/vevo/growthbook_production_reconciliation_deploy_evidence.json` has SHA-256 `21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb`. `scripts/validate_growthbook_production_aa_activation.py` validates its exact content and the exact activation document (lines 538–545, 696–718). Preserve those files and validations byte-for-byte.

Two dependent consumers need explicit treatment:

- `scripts/validate_growthbook_aa_infra_health_evidence.py` currently validates schemas 1/2 and compares `control_plane.source_task_definition` to the historical source. Keep old-schema validation unchanged; add a new current-health schema only with an independently supplied, validated current binding. Do not substitute a fake historical deployment object or rewrite an old health document to make validation pass.
- `scripts/collect_growthbook_aa_quality_source.py:366–370` also requires the historical source task. Its frozen capture and `scripts/growthbook_aa_source_binding.py` exact-source/archive verification remain unchanged for existing evidence. This migration must not recapture, republish or reinterpret that fixed historical A/A source. A future new capture would require a separately reviewed current-binding path and new evidence schema; merely updating the five workflows does not authorize it.

`production-reporting-smoke.yml` is unsuitable: it resolves `latest` and can update the scheduled target before the candidate runs. Its diagnostic command also uses a guard dry run rather than skipping the inline guard completely. Do not dispatch or extend it as the migration mechanism.

## Proposed source changes

| File | Responsibility |
| --- | --- |
| New `scripts/reporting_runtime_binding.py` | Strict pure binding/schema validation plus bounded private read adapter; one current-source gate for the five workflows |
| New `projects/vevo/reporting_runtime_policy.json` | Reviewed account/region/cluster, exact schedule/family/container/path, historical baseline, permitted transition scope and private binding namespace; no future unverified task revision |
| New `scripts/deploy_vevo_report.py` and `.github/workflows/deploy-vevo-report.yml` | Main-only exact-image transactional migration, finite candidate lifecycle, immutable receipts and conditional current-binding update |
| New `scripts/reporting_migration_host_gate.py` | Candidate-only host identity/localhost/report QA wrapper with hard-coded VEVO and explicit mutation/email skips |
| The five workflows above | Use one shared current-source gate; retain their unrelated historical, experiment and reconciliation checks |
| `scripts/validate_growthbook_aa_infra_health_evidence.py` and its CLI/callers | Add isolated current-health schema, retaining exact schema-1/2 behavior; bind new evidence to the verified runtime receipt hash |
| Focused new binding/deployment/host-gate tests and existing five workflow tests | Failure, rollback, immutable-evidence and no-write regressions |

Reuse `schedule_request`/schedule comparison and the explicit readback/rollback pattern from `scripts/deploy_order_automations.py`; reuse its exact-main and `git-<commit>` image gates, finite task observation and localhost-marker method. Do not inherit the order deployer's five-schedule mutation scope, `latest` pin mode, alarm creation or journal policy changes. The separately reviewed guard deployer supplies a useful bounded `no_capture`/task-identity pattern; its first-migration revision-33 assumptions cannot be reused unchanged after report promotion.

## Current authority, separate from history

Use one private namespace, for example `data/vevo/reporting/runtime/`, in the already verified reporting bucket. A small `current.json` pointer is updated conditionally against its exact prior ETag; release records under `releases/<unique-id>.json` are immutable, AES256, create-only, exact-byte read back and hash-bound. Close every streaming body and SDK client. Readers verify owner, region, public-access blocks and private ACL/policy; handle only explicit `NoSuchBucketPolicy` as policy absence.

Bootstrap is an explicit managed operation: independently verify the existing revision-33 schedule, exact image/default command and historical source, then publish a baseline record before switching the five consumers to the new gate. Existing consumers retain their exact legacy checks until that reviewed switch. New consumers always require the pointer; missing, malformed or unavailable authority fails closed, with no absence-based fallback to legacy or an arbitrary family revision.

A current release record binds: schema/project/account/region/cluster; full task-definition ARN and definition hash; image digest and source commit; exact effective command, container and `/app`; source secret references without values; complete normalized schedule configuration; unchanged protected configuration hashes; candidate task ARN/IP/Fargate identity; localhost marker, tagged-output manifest/QA hashes; immutable historical evidence hashes; predecessor hash; managed workflow/run/commit/build provenance; promotion and verification timestamps. Only a terminal `promotion-readback-verified` record can authorize a new current binding. A candidate registration or successful build cannot.

The pointer and receipt are not sufficient self-assertions: the live gate also reads the actual schedule and task definition and compares their exact values, confirms the receipt's managed workflow/main source provenance and exact build digest, and rereads the pointer after observation. Restrict pointer/release writes to the managed deployment identity; reporting, reconciliation and health tasks receive read access only if required. No secrets, order data or mutation journals belong in this namespace.

New health evidence records its current-binding receipt hash and actual source revision without claiming that the historical reconciliation was deployed from that revision. Existing source/health artifacts remain validated at their own exact Git commit and schema. No success is emitted while the source is paused, a transition is incomplete, the pointer changes during observation, or the runtime and pointer disagree.

## Preconditions and protected scope

1. Require the primary order deployment to finish and be independently signed off; capture the actual five current order schedule pins then. Do not copy the previous `47c3` pins into a new allowlist. Freeze main during the later managed report transaction.
2. Require the reporting correction, its actual payment-role contract and exact-head CI to pass. In the reviewed reporting draft, new Stripe-paid status 70 is still deliberately unbound until separately proven; this is a release blocker, not a reason to infer payment from the word `paid`.
3. Install and independently verify both standalone guards before disabling the VEVO report's inline guard in production. If guards are not installed, stop at candidate preparation; do not remove the existing production guard as a side effect of an image migration. Preserve both guard schedules, definitions, state policies and journal keys once installed.
4. Snapshot ROY report revision 71, the five current order schedules, both installed guards, the GrowthBook collector/reconciler controls, and VEVO report configuration. Assert protected snapshots unchanged throughout. VEVO schedule timing, network, roles, secrets, storage, DLQ/retries and existing email policy remain unchanged except the exact approved image and skip configuration. Preserve any already installed guard-skip Input; reject unreviewed command/environment overrides instead of dropping them.
5. Verify account `919341186960`, region `eu-central-1`, cluster `vevo-reporting-cluster`, family `vevo-reporting-daily`, container `reporting`, service `vevo-daily-report-email`, `/app`, and actual current/recent task plus private IP. Record EC2 instance ID as `N/A:Fargate`, not a fabricated instance. If no recent task remains available, use an explicitly reviewed diagnostic-only old-image identity probe before migration; never claim a fresh host check from source alone.
6. Exclude active GrowthBook captures, active competing deployments and overlapping provider-wide scans. Acquire a finite migration lease before the exclusion inventory and recheck immediately before pause/promotion. Current gates check the lease at entry and before publishing; a race becomes incomplete evidence, never a successful mixed-generation capture. Do not silently rerun fixed captures or cancel other agents' tasks.

## Managed sequence

1. Check clean exact main against remote again immediately before the first write. Resolve the successful exact `git-<commit>` build to an immutable digest; validate reviewed source/image metadata. Save immutable preflight snapshots and a rollback plan.
2. Pause **only** the VEVO report schedule, preserving every other field. Require exact disabled readback and a bounded quiet drain of all retained report-family generations, including late Scheduler launches. Recheck protected snapshots and capture exclusion during the wait. Do not pause, restart or repoint protected order/guard/report services.
3. Register an unscheduled candidate cloned from the verified VEVO definition. Pin the digest; preserve project/network/secret references and approved runtime resources. The report host wrapper and production definition have the same image and report acquisition/calculation path. Candidate-only differences are the reviewed diagnostic command, unique output destination, no-email setting and nonproduction metric handling.
4. Start one identified candidate with a unique `startedBy` token. It first exposes a finite localhost identity marker and waits for a bounded, hash-bound release signal from the managed verifier. The verifier checks actual task ARN, definition, digest, private IP, Fargate identity, command and `/app`; the task executes `curl` against its own marker. Only after that identity gate may the same task start provider reads. This two-stage handshake avoids proving identity only after an expensive report has already run. The signal is diagnostic coordination, never a business journal operation.
5. Run the ordinary report path with explicit `--project vevo --skip-email --skip-invoices --skip-creditnote-storno-guard --output-tag <unique-marker>` and equivalent skip environment values set to `true`. Assert resolved flags before execution; block unexpected native login, financial/status mutations and email code paths. Use production-equivalent dates/settings/periods; require fresh status binding and complete inventory/financial context. Do not accept partial reports, unknown payment-role omissions, critical QA or missing artifacts merely because the process exited zero.
6. Isolate candidate output under a unique private probe prefix. `daily_report_runner.s3_upload_outputs` can write stable aliases and `latest/generation.json` even for tagged artifacts; therefore a tag alone is not isolation. Verify the ordinary canonical output prefix is inaccessible to the candidate writer. Retain allowed provider reads and private diagnostic publication only. Suppress candidate metric publication through the host wrapper or use a separately validated diagnostic namespace; never emit production success/failure counters from this probe. The existing `put_metric` has no generic disable environment flag, so do not invent one.
7. Verify required daily/period HTML, payload and QA identity, completeness and file hashes. Require zero email/provider mutation attempts, exact source marker, task exit 0 and STOPPED. Verify the localhost server has shut down. Review the tagged UI only after these host checks; record that the live report pointer has not changed.
8. Recheck fresh main, exact build digest, protected snapshots, capture exclusion and a second quiet drain. Publish a candidate-verified receipt. Update the VEVO target while still disabled, verify the complete desired configuration, then enable it and verify again. Publish terminal immutable promotion proof; only then conditionally advance `current.json` to that proof. Readers fail closed during the short non-atomic target/pointer interval. Never announce a current release from a candidate receipt.
9. Independently read back the schedule, image, current pointer, all protected snapshots and candidate cleanup. Run the five live current-binding gates without reopening experiment outcome gates. Preserve existing scheduled email behavior; send no one-off email for deployment verification. A later ordinary scheduled report supplies separate natural-run evidence, not retroactive candidate proof.

## Failure and rollback

Before promotion, a failed probe changes no scheduled target: stop only the owned candidate after checking its exact identity, verify STOPPED, then restore the original VEVO schedule only if current configuration still matches the transaction's known state. Retain failed immutable evidence and tagged artifacts for review. Never terminate unrelated tasks.

After any possible target update, pause the known VEVO target, verify/drain both old and candidate generations, restore the exact original snapshot, and read it back before restoring the prior current binding with CAS. Keep rollback evidence append-only. If an unexpected schedule/pointer change, uncertain candidate lifetime or ambiguous restore prevents exact recovery, leave VEVO reporting paused, emit a specific failed deployment, retain the lease/incident evidence for recovery, and keep all protected scopes untouched. Do not blindly overwrite drift or delete proof to retry.

No invoice, status, payment, receipt, customer email, GrowthBook fact, activation or measurement-window write is part of this migration. Permitted writes are restricted infrastructure changes and isolated report/proof publication. A failed report read is never repaired by replaying a financial action.

## Required tests before implementation can be released

- Exact old historical files/hashes and schema-1/2 validation unchanged; new current evidence requires independent binding, rejects fabricated/missing/cross-project/changed-pointer authority, and cannot authorize old fixed source recapture.
- All five workflow gates use the same full-ARN/image/config validator. Revision suffix alone, `latest`, unsupported command, secret override, unknown project/Stripe role and stale main fail before writes.
- Probe ordering proves task/IP/path/localhost before report reads. Tagged output cannot touch live aliases; both skip flags and email suppression are asserted; provider mutation mocks fail the test immediately. Diagnostic metrics cannot alter live health.
- Partial/ambiguous schedule updates, pointer CAS failure, late old tasks, task timeout, protected-scope drift and post-promotion rollback retain exact evidence and stop only owned tasks. No protected schedule/configuration changes occur on success or failure.
- Required report, order/guard, GrowthBook historical/current-gate and security suites plus reporting smoke pass for the final integrated image. This document itself needs no executable tests; it adds no runtime code.

Next exact step: root reviews this specification and chooses the implementation scope. Bootstrap/current authority, standalone-guard readiness and the separately proven reporting payment-role contract must be settled before a managed report dispatch. No source implementation or live call was performed in this design worktree.
