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
- the A/A stop/readback, frozen CTA sample, or 14-day lifecycle value
  reconciliation is missing or hash-mismatched;
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
6. The A/A product-page baseline completed its 24-hour follow-up and the final
   CTA sample was frozen offline.
7. The 14-day refund/credit-note lifecycle reconciliation passed exact value
   parity and was recorded without identities.

An A/A `PASS` is not a CTA winner and does not start the CTA automatically.

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
committed or uploaded with the canonical artifact. Independently download that
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
python scripts/record_growthbook_cta_activation.py --output projects/vevo/growthbook_cta_activation.json open-review --runtime-observation projects/vevo/growthbook_cta_runtime_readiness_observation.json --runtime-observation-sha256 <independent-sha256>
python scripts/validate_growthbook_meta_reporting_contract.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_meta_reporting_contract tests.test_growthbook_cta_activation_recorder tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

Review the diff and merge it through a PR. The recorder must bind the exact A/A
completion, snapshot manifest, frozen sample, lifecycle reconciliation, design,
decision contract, immutable Meta/reporting contract, checked-in collector
registry, runtime artifact, workflow run, and main commit. The Meta/reporting
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
python scripts/record_growthbook_cta_activation.py --output projects/vevo/growthbook_cta_activation.json record-start --workspace projects/vevo/growthbook_workspace.json --workspace-output projects/vevo/growthbook_workspace.json --registry growthbook_collector/experiments.json --observation projects/vevo/growthbook_cta_activation_observation.json --observation-sha256 <independent-sha256>
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
wait the frozen 14-day lifecycle follow-up before the one final decision look.

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
remains an exact-daily-gate fallback. Before AWS credentials, every admitted
run requires the A/A stopped, only CTA running at `100%`, the
outcome/arm/winner gates closed, and the exact whole-local-day boundary.
It then binds instance `N/A:Fargate` and the exact reconciliation through the
bounded success marker plus Scheduler-authenticated CloudTrail `RunTask` event,
service `vevo-growthbook-reconcile-production`, path `/app`, task definition,
immutable image, inherited localhost marker evidence, generated/published
parity, three clear alarms, empty DLQ, and the unchanged source reporting
schedule. Retained ECS state and its private IP are preferred. If that state has
expired, schema `2` records `cloudtrail_run_task_retention_recovery`,
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

After the reviewed checkpoint opens the manual stop gate, stop only GrowthBook
experiment `exp_19g6mmt1qxzrp`, remove only its Production live rule, preserve
the staging rule, and leave GTM version `15`, Meta Ads, BiznisWeb, collector,
reporting, prices, cart, checkout, payments, stock, and orders unchanged. Do not
open any arm or outcome result during this operation. The canonical readback at
`projects/vevo/growthbook_cta_assignment_stop_observation.json` must bind the
last outcome-blind checkpoint SHA-256 and original CTA start-observation
SHA-256, prove zero Production allocation/rules, an advanced feature revision,
no active Production experiment, unchanged desktop/mobile commerce behavior,
and at least 300 seconds with zero new CTA assignment or exposure.

Independently hash that canonical readback, then record all versioned stopped
states in one reviewed branch:

```text
python scripts/record_growthbook_cta_completion.py --stop-observation projects/vevo/growthbook_cta_assignment_stop_observation.json --stop-observation-sha256 <independent-sha256> --completion-output projects/vevo/growthbook_cta_completion.json --activation-output projects/vevo/growthbook_cta_activation.json --measurement-output projects/vevo/growthbook_cta_measurement_window.json --workspace-output projects/vevo/growthbook_workspace.json --final-snapshot-output projects/vevo/growthbook_cta_final_snapshot.json
python scripts/validate_growthbook_cta_completion.py
python scripts/validate_growthbook_cta_final_snapshot.py
python scripts/validate_growthbook_cta_measurement_window.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_cta_completion_recorder tests.test_growthbook_cta_final_snapshot_builder tests.test_growthbook_cta_final_snapshot_recorder tests.test_growthbook_cta_window_checkpoint tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

The offline recorder first builds and validates the completion, historical
activation, measurement-window, workspace, and final-snapshot outputs before it
writes any of them. It then records zero Production allocation, freezes
`final_snapshot_due_utc` at exactly 14 days after `assignment_ended_at_utc`, and
opens only the hash-bound protected final-snapshot workflow. The current
completion and final-snapshot manifests are deliberately waiting; therefore no
manual stop, follow-up, arm read, outcome read, winner call, or external
mutation is currently authorized.

## Protected final snapshot and offline decision

At or after the exact recorded `final_snapshot_due_utc`, dispatch
`.github/workflows/build-vevo-growthbook-production-cta-final-snapshot.yml` from
the exact reviewed `main` commit with `confirm_final_snapshot=true`. Never run
it early or a second time. Before AWS credentials it validates the source hashes,
complete 14-day follow-up, main-only one-look gate, and absence of any prior
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
`vevo-growthbook-cta-final-snapshot.json` and
`vevo-growthbook-cta-final-decision.json`. It deletes temporary AWS, log, and
query payloads and cannot deploy, edit GrowthBook/GTM/Meta Ads/BiznisWeb,
change reporting/collector infrastructure, alter commerce, or apply a winner.
Only the new diagnostic task's private IP and localhost markers satisfy the
runtime hard gate for this read-only operation. Historical CloudTrail recovery
only proves scheduled-task provenance. No UI test is applicable because the
workflow makes no storefront or control-plane change.

Independently download the sole artifact, verify the successful run, exact main
commit, and both SHA-256 values, then record the result through a separate
reviewed branch:

```text
python scripts/record_growthbook_cta_final_snapshot.py --snapshot <downloaded-vevo-growthbook-cta-final-snapshot.json> --snapshot-sha256 <independent-snapshot-sha256> --decision <downloaded-vevo-growthbook-cta-final-decision.json> --decision-sha256 <independent-decision-sha256> --workflow-run-id <successful-run-id> --main-commit <exact-main-commit> --registry projects/vevo/growthbook_hypothesis_registry.json --registry-output projects/vevo/growthbook_hypothesis_registry.json --output projects/vevo/growthbook_cta_final_snapshot.json
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
