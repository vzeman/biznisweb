# VEVO GrowthBook CTA activation runbook

This runbook covers only the first non-price experiment
`vevo-sk-product-cta-color-001` on Slovak VEVO product-detail pages. It changes
only the approved add-to-cart button background/color. It does not change the
button label, dimensions, layout, placement, product selector, price, product
content, cart, checkout, payment, stock, order handling, BiznisWeb settings, or
Meta Ads delivery.

The machine-readable source of truth is
`projects/vevo/growthbook_cta_activation.json`. Its current state is
`waiting_for_verified_aa_completion_sample_lifecycle_and_runtime`, so no CTA
start is authorized now.

## Hard stops

Do not start the CTA experiment when any one of these is true:

- the protected A/A decision is not exactly `PASS`;
- the A/A experiment or its Production live rule is still active;
- Production A/A or CTA allocation is not exactly `0%` before the start;
- the A/A stop/readback, frozen CTA sample, or completed-A/A 21-day
  order-attribution plus lifecycle preflight is missing or hash-mismatched;
- the verified GrowthBook Pro transition manifest or its canonical billing/
  six-metric observation is missing, non-canonical, hash-mismatched, or no
  longer validates against the current workspace;
- the checked-in and deployed Production collector registry is not CTA-only;
- the exact Production collector instance ID, private IP, service, runtime path,
  task definition, image digest, localhost marker, or target health is unknown;
- the collector has a CTA event before the reviewed GrowthBook start;
- GTM is not live version `15` with zero unprocessed changes;
- another Production experiment is active;
- the current GrowthBook draft differs from the frozen experiment, feature,
  variations, `50/50` weights, metrics, data source, or first-`N` sample;
- `growthbook_meta_reporting_contract.json` is missing, hash-mismatched, or no
  longer proves the exact Meta URL-parameter → collector → Athena → GrowthBook/
  reporting dimension chain;
- Meta would select an experiment arm through a separate destination, query
  parameter, or Meta A/B split instead of sending both arms to the same
  canonical VEVO destination for on-site GrowthBook assignment;
- any step would change Meta Ads, BiznisWeb, a price, product content, cart,
  checkout, payment, stock, or an order;
- the runtime observation or activation readback contains credentials,
  event/device IDs, customer/order data, or raw AWS responses.

## Gate 1 — finish and stop A/A

Follow `GROWTHBOOK_PRODUCTION_AA_ACTIVATION_RUNBOOK.md`. The reviewed repository
state must prove all of the following before CTA runtime preparation:

1. The outcome-blind A/A window resolved by the first qualifying daily
   checkpoint.
2. Both protected evidence components were independently hash/run/commit-bound.
3. The protected snapshot decision was independently recomputed as `PASS`.
4. Only the exact A/A was manually stopped and its Production live rule removed.
5. Reload/readback proved zero Production allocation, CTA still draft, staging
   preserved, GTM version `15` unchanged, and commerce unchanged.
6. The separately authorized GrowthBook Pro transition and its canonical
   readback prove the exact active one-seat monthly plan plus all six unique,
   query-tested Preview/Production p75 metric IDs while CTA remains at `0%`.
7. The A/A product-page baseline completed its 24-hour follow-up and the final
   CTA sample was frozen offline.
8. The completed-A/A source cohort has reached the full 7-day order-attribution
   plus 14-day lifecycle boundary, and its protected lifecycle preflight passed
   exact direct-curated-versus-Athena value/count parity without CTA outcomes or
   identities.

An A/A `PASS` is not a CTA winner and does not start the CTA automatically.

### Protected prelaunch lifecycle preflight

`growthbook_cta_lifecycle_reconciliation.json` is source-explicit: its target is
the future CTA, but its only evidence cohort is the completed and stopped
Production A/A `vevo-sk-aa-001`. This avoids the impossible circular requirement
to observe CTA orders before the CTA has started. The gate remains closed until
the protected A/A decision is verified `PASS`, the zero-allocation stop is
recorded, the A/A window is resolved, and 21 days have elapsed after its exact
end (7-day purchase attribution plus 14-day per-order lifecycle maturity).

The main-only `.github/workflows/collect-vevo-growthbook-cta-lifecycle-preflight.yml`
runs once daily at `05:20 UTC`, independently of the local PC. Before the A/A
PASS/stop/window and exact 21-day due boundary are recorded it exits successfully
before AWS credentials. During the first 24-hour due interval it collects the
preflight once; later scheduled runs skip and require an explicitly confirmed
manual recovery. `workflow_dispatch` with `confirm_collection=true` is the exact
manual fallback.
Before AWS credentials every admitted run revalidates all repository gates and
the due time. It then confirms account
`919341186960`, instance `N/A:Fargate`, the exact collector private IP, service
`vevo-growthbook-collector-production`, path `/app`, task definition, image and
inherited localhost markers. Its only data paths are temporary canonical curated
A/A device facts and one aggregate Athena query for the same frozen A/A cohort.
It binds the retained quality object from the exact generation recorded on the
direct frozen cohort rather than selecting an unrelated newer quality report.
It requires zero immature orders, at least one mature cancellation/refund/
credit-note case, exact lifecycle counts, one facts generation, and cent-exact
CM1 parity. CTA arms and CTA outcomes are never queried. All raw AWS responses
and identity-bearing fact files are shredded before the workflow uploads the
single canonical identity-free artifact.

Download that artifact independently, verify its successful run, exact `main`
commit and SHA-256, then record the byte-identical evidence on a reviewed branch:

```text
python scripts/record_growthbook_cta_lifecycle_reconciliation.py --observation <downloaded-vevo-growthbook-cta-lifecycle-preflight.json> --observation-sha256 <independent-sha256> --workflow-run-id <successful-run-id> --main-commit <exact-main-commit> --verified-at-utc <whole-second-UTC-Z>
python scripts/validate_growthbook_workspace.py
python scripts/security_ci.py
git diff --check
```

The recorder may update only its allowlisted manifest fields and the canonical
observation file. It cannot activate CTA, mutate GrowthBook/GTM/Meta Ads/
BiznisWeb/reporting/commerce, or copy event, device, customer, or order identity
into repository evidence.

## Gate 2 — prepare and host-verify the CTA-only collector

The Production collector registry must be changed through Git/PR from the old
A/A-only allowlist to exactly the reviewed Preview CTA contract. Build and
deploy the immutable collector image only through the main-only, explicitly
confirmed workflow
`.github/workflows/deploy-vevo-growthbook-production-cta-runtime.yml`. The
workflow is intentionally unusable while this repository still contains the
running-A/A state or A/A-only Production registry: its complete post-A/A gate
runs before AWS credentials. The existing public `POST /v1/events` route must
remain enabled and byte-identical; the rollout may change only the
CloudFormation-managed collector task definition/service runtime.
Before any UI action, apply the infrastructure hard gate and record:

- instance ID: `N/A:Fargate`;
- exact private IP of the healthy service task;
- service: `vevo-growthbook-collector-production`;
- runtime path: `/app`;
- exact task definition and immutable image digest;
- the separate exact host-gate task ID/private IP used for direct localhost
  `curl` to `/health` and `/marker.json`;
- marker/readback that the packaged Production registry contains only
  `vevo-sk-product-cta-color-001` and exactly matches the checked-in registry;
- healthy target readback;
- zero CTA collector events before GrowthBook start;
- A/A allocation `0%`, CTA allocation `0%`, GTM version `15`, and zero GTM
  unprocessed changes.

Only after those checks may a protected main-branch workflow emit the canonical,
identity-free file
`vevo-growthbook-cta-runtime-readiness.json` as its only uploaded artifact. Raw AWS,
CloudWatch, ECS, and query responses must remain temporary and must not be
committed or uploaded with the canonical artifact. The canonical artifact is
retained for 90 days so extended local-PC downtime cannot lose this one-time
deployment-to-activation handoff. Independently download that
artifact, verify the successful workflow run/main commit and SHA-256, then place
the byte-identical file at
`projects/vevo/growthbook_cta_runtime_readiness_observation.json` through a
separate reviewed PR; do not copy fields from the GitHub UI or reconstruct it.

If any post-update host, target, route, isolation, zero-event, canonical-build,
cleanup, or upload gate fails, the workflow restores the exact preceding image
and collector version through another validated CloudFormation candidate
change set, preserves the public route, waits for a healthy service, and runs
that preceding image's localhost health/`/app` marker gate. This rollback never
changes GrowthBook, GTM, Meta Ads, BiznisWeb, prices, cart, checkout, or orders.

## Gate 3 — open the manual start review offline

Independently obtain the successful workflow run ID, exact main commit, and
SHA-256 of the one canonical runtime observation. Place that exact canonical
file at its versioned path
`projects/vevo/growthbook_cta_runtime_readiness_observation.json`; do not
reformat or manually reconstruct it. On a new branch, run:

```text
python scripts/record_growthbook_cta_activation.py --output projects/vevo/growthbook_cta_activation.json open-review --pro-upgrade projects/vevo/growthbook_pro_upgrade.json --pro-observation projects/vevo/growthbook_pro_upgrade_observation.json --lifecycle projects/vevo/growthbook_cta_lifecycle_reconciliation.json --lifecycle-observation projects/vevo/growthbook_cta_lifecycle_observation.json --runtime-observation projects/vevo/growthbook_cta_runtime_readiness_observation.json --runtime-observation-sha256 <independent-sha256>
python scripts/validate_growthbook_meta_reporting_contract.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_meta_reporting_contract tests.test_growthbook_cta_activation_recorder tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

Review the diff and merge it through a PR. The recorder must bind the exact A/A
completion, snapshot manifest, verified Pro transition manifest and canonical
Pro observation, frozen sample, lifecycle manifest and canonical observation,
their exact A/A completion/snapshot source hashes, design, decision
contract, immutable Meta/reporting contract, checked-in collector registry,
runtime artifact, workflow run, and main commit. The Pro files are validated
again against the current workspace and both hashes must remain unchanged
between review and the recorded CTA start. The Meta/reporting
contract proves the stable campaign/ad-set/ad/placement mapping and preserves
one canonical destination; those dimensions are diagnostic and cannot replace
the primary all-traffic decision. The recorder may open only
`manual_growthbook_start_allowed=true`. Every automatic mutation boundary,
winner call, and commerce/Meta/BiznisWeb change remains false.

## Gate 4 — manually start only the frozen CTA

After the reviewed `manual_cta_start_review_allowed` state is on `main`:

1. Reload GrowthBook and confirm project `VEVO SK Web`, Production data source
   `ds_19g6mmt5stlp6`, experiment `exp_19g6mmt1qxzrp`, and feature
   `vevo-sk-product-cta-color`.
2. Confirm the experiment is still a draft, Production allocation is `0%`, the
   A/A is stopped, and no other Production experiment is active.
3. Confirm `100%` experiment traffic, exact `control`/`brand_contrast` `50/50`
   weights, assignment attribute `id`, the frozen first-`N` target, one goal,
   six secondary metrics, the client-error plus three verified Pro p75
   guardrails, Bayesian settings, and no activation metric.
4. Confirm Meta is not running a separate A/B split for this hypothesis and no
   ad destination or URL parameter selects `control` or `brand_contrast`.
5. Start only `vevo-sk-product-cta-color-001` and publish only its Production
   feature rule. Do not publish GTM, edit Meta Ads, or save BiznisWeb forms.
6. Reload GrowthBook and read back that CTA is the only active Production
   experiment at `100%`/`50-50`, while A/A remains stopped at `0%`.
7. With Tag Assistant, verify consent accept/reject/withdrawal, desktop and
   mobile, both variations, exact approved CSS, unchanged text/dimensions/layout/
   placement/price, zero console errors, accepted collector delivery, and one
   sticky-consistent anonymous repeat. Do not add to cart or place an order for
   this activation readback.

Create canonical compact JSON
`projects/vevo/growthbook_cta_activation_observation.json`, calculate its
SHA-256 independently, and record the start on a new branch:

```text
python scripts/record_growthbook_cta_activation.py --output projects/vevo/growthbook_cta_activation.json record-start --workspace projects/vevo/growthbook_workspace.json --workspace-output projects/vevo/growthbook_workspace.json --pro-upgrade projects/vevo/growthbook_pro_upgrade.json --pro-observation projects/vevo/growthbook_pro_upgrade_observation.json --registry growthbook_collector/experiments.json --observation projects/vevo/growthbook_cta_activation_observation.json --observation-sha256 <independent-sha256>
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_cta_activation_recorder tests.test_growthbook_workspace tests.test_growthbook_cta_evaluator
python scripts/security_ci.py
git diff --check
```

Merge only after the redundant manifest/workspace/registry/readback state agrees.
The recorder performs no external mutation.

## Assignment stopping rule

Do not inspect conversion, SRM, arm counts, revenue, CM1, performance, or any
other outcome to choose the assignment window. At the first successful daily
post-reconciliation checkpoint after each whole Europe/Bratislava day, inspect
only the cumulative count of eligible first-exposed devices:

- stop assignment at the first checkpoint where the frozen first-`N` target is
  reached; or
- stop after 42 full local calendar days if the target is still not reached.

Minimum assignment duration is 14 full local days. Safety guardrails may stop
early, but a safety stop can never declare a winner. After assignment stops,
wait the full 21-day final follow-up (7-day attribution plus 14-day per-order
lifecycle maturity) before the one final decision look.

## Safety-only checkpoint contract

`projects/vevo/growthbook_cta_safety_monitoring.json` freezes the separate
early-safety evidence boundary. The offline
`scripts/evaluate_growthbook_cta_safety.py` accepts only aggregate
`control`/`brand_contrast` device, measured-page-load, client-error, and p75
LCP/INP/CLS fields plus explicit unchanged-commerce and data-quality booleans.
It rejects event/device identifiers, customer/order data, primary or business
outcomes, Meta dimensions, winner fields, and any claimed automatic mutation.

Performance checks become mature only at 200 measured page loads in each arm.
The frozen stop thresholds are LCP degradation above the larger of 200 ms or
10%, INP degradation above the larger of 20 ms or 10%, CLS degradation above
20 milli, and client-error-rate degradation above 0.5 percentage points. A
changed CTA commerce label or price, a cart/checkout/order mutation, or a
reproducible cart/checkout runtime error returns `STOP_REQUIRED` immediately,
even before performance maturity. The evaluator can only return `CONTINUE`,
`CONTINUE_NOT_MATURE`, or `STOP_REQUIRED`; it cannot stop GrowthBook, call a
winner, or mutate any external service.

This checked-in contract is deliberately
`waiting_for_verified_cta_start`: collection and recording are disabled, both
start-source hashes and the commerce price baseline are null, and
`manual_growthbook_stop_allowed=false`. The canonical CTA start observation
must additionally capture the exact product URL/code, cart URL, CTA text, and
normalized displayed EUR price. The offline
`record_growthbook_cta_safety_checkpoint.py initialize` transition binds those
start sources and the price baseline, then opens only the three protected
safety collection/recording gates. It independently re-evaluates each
canonical checkpoint and requires separately supplied evidence, decision, and
provenance hashes plus the exact successful workflow run and main commit.
`CONTINUE` leaves the stop lifecycle unchanged. `STOP_REQUIRED` can only close
further checkpoints and open the same reviewed manual CTA stop and 21-day
follow-up path used by the outcome-blind first-`N`/day-42 rule; it never
performs the stop itself.

The PC-independent main-only workflow is
`.github/workflows/check-vevo-growthbook-production-cta-safety.yml`. It runs at
minute `05` of every UTC hour, but before AWS credentials it derives the exact
checkpoint index from the verified assignment start and admits only the first
60 minutes after each 24-hour boundary. Waiting, pre-due, late, closed, and
already-recorded states skip without AWS. Missed prior days do not block a
later due checkpoint. An admitted run verifies the exact checked-in Production
Fargate host gate (`N/A:Fargate`, deployment IP `172.31.39.76`, service
`vevo-growthbook-reconcile-production`, path `/app`), inherited localhost
health/marker evidence, current task definition/image/schedules, a recent
scheduled success marker, clear alarms, and an empty DLQ.

The hash-bound SQL reads only aggregate variation-level eligible-device,
measured-page-load, client-error and p75 LCP/INP/CLS fields plus aggregate
assignment-quality counts. It reads no primary/business outcome or Meta
dimension. The storefront probe performs only two idempotent HTTP GETs—product
and cart—and never adds an item or submits checkout. Query/data-quality drift,
duplicate assignments, changed CTA text/price, or reproducible product/cart
failure becomes `STOP_REQUIRED`; no automatic external action follows. Every
raw AWS/Athena/HTML response is removed before the exact canonical
evidence/decision/provenance bundle is retained for 90 days. Validate the
closed contract, builder, workflow, and lifecycle now:

```text
python scripts/validate_growthbook_cta_safety_monitoring.py
python -m unittest tests.test_growthbook_cta_safety_evaluator tests.test_growthbook_cta_safety_recorder tests.test_growthbook_cta_safety_checkpoint_builder tests.test_growthbook_cta_safety_workflow
```

The executable source of truth for this rule is
`projects/vevo/growthbook_cta_measurement_window.json`. It is deliberately
`waiting_for_verified_cta_start` now. After Gate 4 is recorded on `main`, bind
the canonical start readback and exact frozen sample in a separate reviewed PR:

```text
python scripts/record_growthbook_cta_window_checkpoint.py --output projects/vevo/growthbook_cta_measurement_window.json initialize
python scripts/validate_growthbook_cta_measurement_window.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_cta_window_checkpoint tests.test_growthbook_cta_window_checkpoint_workflow tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

Initialization computes the first complete local date, exact 14-day and 42-day
boundaries, and the first eligible `03:45 Europe/Bratislava` checkpoint from the
recorded assignment start. It binds the current activation, start observation,
sample, decision contract, and localhost-gated reconciliation evidence by
SHA-256. It does not query AWS or change traffic.

At each due checkpoint, use only
`.github/workflows/check-vevo-growthbook-production-cta-window.yml` from the
exact reviewed `main` commit. It schedules both UTC equivalents of `04:30
Europe/Bratislava` and admits only the correct DST slot after the frozen
`03:45` reconciliation. Before a verified CTA start, before the first due date,
on the wrong DST slot, after the 42-day maximum, after resolution, or for an
already committed index, it skips before AWS credentials and the population
query. An admitted scheduled run derives its checkpoint index from the frozen
local date rather than current Git history, so the canonical artifact is
captured even while the local PC is off. Manual `confirm_checkpoint=true`
remains an exact-daily-gate fallback. If the exact next artifact was never
created because both its scheduled and same-window runs failed, dispatch the
same workflow after that gate closes with both `confirm_checkpoint=true` and
`confirm_historical_backfill=true`. Schema `3` binds the run as
`manual_historical_backfill` and reconstructs only
`len(checkpoint_history) + 1` at its original preregistered cutoff. Never use
backfill if a successful artifact for that index already exists. Before AWS
credentials, every admitted
run requires the A/A stopped, only CTA running at `100%`, the
outcome/arm/winner gates closed, and the exact whole-local-day boundary.
It then binds instance `N/A:Fargate` and the exact reconciliation through the
bounded success marker plus Scheduler-authenticated CloudTrail `RunTask` event,
service `vevo-growthbook-reconcile-production`, path `/app`, task definition,
immutable image, inherited localhost marker evidence, generated/published
parity, three clear alarms, empty DLQ, and the unchanged source reporting
schedule. Retained ECS state and its private IP are preferred. If that state has
expired, the retained schema `2` compatibility path and schema `3` record
`cloudtrail_run_task_retention_recovery`,
`runtime_state_retained=false`, and `private_ip=null`; this read-only historical
proof never satisfies the live-IP plus localhost-marker hard gate for an
infrastructure mutation. Its only Athena result is one cumulative eligible-device
count. It never groups by variation or reads conversion, revenue, CM1,
performance, Meta dimensions, raw events, or identities.

Independently download and hash the sole canonical artifact, then record it on
a new branch:

```text
python scripts/record_growthbook_cta_window_checkpoint.py --output projects/vevo/growthbook_cta_measurement_window.json record-checkpoint --evidence <downloaded-vevo-growthbook-cta-window-checkpoint.json> --expected-evidence-sha256 <independent-sha256> --expected-workflow-run-id <successful-run-id> --expected-main-commit <exact-main-commit>
python scripts/validate_growthbook_cta_measurement_window.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_cta_window_checkpoint tests.test_growthbook_cta_window_checkpoint_workflow tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

Artifacts are retained for 90 days. If the PC was offline across several due
checkpoints, record them in ascending checkpoint-index order. Stop at the
earliest artifact that reaches the frozen first-`N` target or the day-42
boundary; ignore every later capture after that resolution boundary. The
workflow never stops assignment automatically.

Below the target and before day 42, the recorder can only extend one whole
local day. At the target or day 42 it closes further checkpoints and opens only
`manual_review_allowed=true` for stopping this CTA assignment. It cannot stop
GrowthBook automatically, read an outcome, declare a winner, or mutate GTM,
Meta Ads, BiznisWeb, collector/reporting, prices, cart, checkout, or orders.

After either a reviewed outcome-blind checkpoint or a verified hash-bound
`STOP_REQUIRED` safety checkpoint opens the manual stop gate, stop only GrowthBook
experiment `exp_19g6mmt1qxzrp`, remove only its Production live rule, preserve
the staging rule, and leave GTM version `15`, Meta Ads, BiznisWeb, collector,
reporting, prices, cart, checkout, payments, stock, and orders unchanged. Do not
open any arm or outcome result during this operation. The canonical readback at
`projects/vevo/growthbook_cta_assignment_stop_observation.json` must bind the
exact reviewed stop trigger (one outcome-blind evidence hash or all three
safety evidence/decision/provenance hashes) and original CTA start-observation
SHA-256, prove zero Production allocation/rules, an advanced feature revision,
no active Production experiment, unchanged desktop/mobile commerce behavior,
and at least 300 seconds with zero new CTA assignment or exposure.

Independently hash that canonical readback, then record all versioned stopped
states in one reviewed branch:

```text
python scripts/record_growthbook_cta_completion.py --stop-observation projects/vevo/growthbook_cta_assignment_stop_observation.json --stop-observation-sha256 <independent-sha256> --completion-output projects/vevo/growthbook_cta_completion.json --activation-output projects/vevo/growthbook_cta_activation.json --measurement-output projects/vevo/growthbook_cta_measurement_window.json --safety-output projects/vevo/growthbook_cta_safety_monitoring.json --workspace-output projects/vevo/growthbook_workspace.json --final-snapshot-output projects/vevo/growthbook_cta_final_snapshot.json
python scripts/validate_growthbook_cta_completion.py
python scripts/validate_growthbook_cta_final_snapshot.py
python scripts/validate_growthbook_cta_measurement_window.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_cta_completion_recorder tests.test_growthbook_cta_final_snapshot_builder tests.test_growthbook_cta_final_snapshot_recorder tests.test_growthbook_cta_window_checkpoint tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

The offline recorder first builds and validates the completion, historical
activation, measurement-window, safety-monitoring, workspace, and
final-snapshot outputs before it writes any of them. It then records zero Production allocation, freezes
`final_snapshot_due_utc` at exactly 21 days after `assignment_ended_at_utc`, and
opens only the hash-bound protected final-snapshot workflow. The current
completion and final-snapshot manifests are deliberately waiting; therefore no
manual stop, follow-up, arm read, outcome read, winner call, or external
mutation is currently authorized.

## Protected final snapshot and offline decision

At or after the exact recorded `final_snapshot_due_utc`, dispatch
`.github/workflows/build-vevo-growthbook-production-cta-final-snapshot.yml` from
the exact reviewed `main` commit with `confirm_final_snapshot=true`. Never run
it early or a second time. Before AWS credentials it validates the source hashes,
complete 21-day follow-up, main-only one-look gate, and absence of any prior
outcome-query attempt in any earlier workflow, including a failed or cancelled
run whose query step had already started. It then applies the infrastructure
hard gate in this order:

1. confirm account `919341186960`, instance `N/A:Fargate`, the exact
   `vevo-growthbook-reconciliation-production` stack, service
   `vevo-growthbook-reconcile-production`, runtime `/app`, schedule, task
   definition, immutable image, reporting database/workgroup, and source-table
   schemas;
2. bind the latest successful scheduled reconciliation after the due time
   through a bounded CloudWatch marker and Scheduler-authenticated CloudTrail
   `RunTask`, then require exact generated/published marker parity, three clear
   alarms, and an empty DLQ. Prefer retained ECS task/IP state; after ECS
   retention expiry record the explicit historical source and expired IP, which
   never satisfies the live infrastructure hard gate;
3. start one new diagnostic task from that exact task definition, require and
   record its live private IP, then verify direct localhost `/health` and
   `/marker.json` with the service and `/app` markers;
4. only then run one Athena query that internally selects the frozen first-`N`
   devices but returns exactly two aggregate variation rows.

The workflow reads outcomes once and uploads only canonical
`vevo-growthbook-cta-final-snapshot.json`,
`vevo-growthbook-cta-final-decision.json`, and the PII-free
`vevo-growthbook-cta-final-provenance.json`. The provenance binds the exact
repository, workflow, first run attempt, main commit, artifact name, and both
file hashes. The workflow deletes temporary AWS, log, and query payloads and
cannot deploy, edit GrowthBook/GTM/Meta Ads/BiznisWeb,
change reporting/collector infrastructure, alter commerce, or apply a winner.
Only the new diagnostic task's private IP and localhost markers satisfy the
runtime hard gate for this read-only operation. Historical CloudTrail recovery
only proves scheduled-task provenance. No UI test is applicable because the
workflow makes no storefront or control-plane change.

Independently download the sole artifact bundle, verify the successful run,
exact main commit, and all three SHA-256 values, then record the result through
a separate reviewed branch. The recorder rejects a provenance file whose run,
commit, file set, or file hashes differ from the supplied canonical files:

```text
python scripts/record_growthbook_cta_final_snapshot.py --snapshot <downloaded-vevo-growthbook-cta-final-snapshot.json> --snapshot-sha256 <independent-snapshot-sha256> --decision <downloaded-vevo-growthbook-cta-final-decision.json> --decision-sha256 <independent-decision-sha256> --provenance <downloaded-vevo-growthbook-cta-final-provenance.json> --provenance-sha256 <independent-provenance-sha256> --workflow-run-id <successful-run-id> --main-commit <exact-main-commit> --registry projects/vevo/growthbook_hypothesis_registry.json --registry-output projects/vevo/growthbook_hypothesis_registry.json --output projects/vevo/growthbook_cta_final_snapshot.json
python scripts/validate_growthbook_hypothesis_registry.py
python scripts/validate_growthbook_cta_final_snapshot.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_cta_final_snapshot_builder tests.test_growthbook_cta_final_snapshot_recorder tests.test_growthbook_cta_final_snapshot_workflow tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

The offline recorder verifies canonical bytes, provenance, source hashes, and
the lifecycle reconciliation, recomputes the decision byte-for-byte, and
pre-validates both outputs before writing. It records only `WIN`, `LOSE`, or
`INCONCLUSIVE`, stores the complete aggregate decision in the PII-free Git
hypothesis registry, binds that registry's SHA-256 into the final-snapshot
manifest, and immediately closes every final-look read gate. GrowthBook remains
the analytical UI; the registry is the durable audit source of truth after the
90-day workflow artifact expires. The same reviewed branch must summarize the
decision and next action in `PROJECT_STATE.md`. The recorder never applies the
recommendation. Any later GrowthBook, GTM, Meta Ads, BiznisWeb,
collector/reporting, or commerce action requires a new explicit manual review
and a separate versioned workflow.

## Rollback

For activation or runtime failure, use this order:

1. Set only the CTA Production allocation to `0%` and remove only its Production
   live rule; verify CTA and A/A assignment are both impossible.
2. Preserve GTM version `15` unless evidence identifies the unchanged loader as
   the fault. Never mix routine CTA stop with an unrelated GTM rollback.
3. If the collector/runtime is faulty, restore the preceding reviewed collector
   image/registry through Git and the protected deploy path, then repeat the
   exact Fargate localhost hard gate before UI verification.

Do not delete GrowthBook objects, collector data, curated facts, reporting
artifacts, orders, or experiment evidence as rollback cleanup.
