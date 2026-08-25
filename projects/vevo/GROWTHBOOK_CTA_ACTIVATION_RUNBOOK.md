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
deploy the immutable collector image through the protected deployment path.
Before any UI action, apply the infrastructure hard gate and record:

- instance ID: `N/A:Fargate`;
- exact private IP of the running task;
- service: `vevo-growthbook-collector-production`;
- runtime path: `/app`;
- exact task definition and immutable image digest;
- direct `curl` to the task's localhost `/health` and `/marker.json`;
- marker/readback that the packaged Production registry contains only
  `vevo-sk-product-cta-color-001` and exactly matches the checked-in registry;
- healthy target readback;
- zero CTA collector events before GrowthBook start;
- A/A allocation `0%`, CTA allocation `0%`, GTM version `15`, and zero GTM
  unprocessed changes.

Only after those checks may a protected main-branch workflow emit the canonical,
identity-free file
`projects/vevo/growthbook_cta_runtime_readiness_observation.json`. Raw AWS,
CloudWatch, ECS, and query responses must remain temporary and must not be
committed or uploaded with the canonical artifact.

## Gate 3 — open the manual start review offline

Independently obtain the successful workflow run ID, exact main commit, and
SHA-256 of the one canonical runtime observation. Place that exact canonical
file at its versioned path
`projects/vevo/growthbook_cta_runtime_readiness_observation.json`; do not
reformat or manually reconstruct it. On a new branch, run:

```text
python scripts/record_growthbook_cta_activation.py --output projects/vevo/growthbook_cta_activation.json open-review --runtime-observation projects/vevo/growthbook_cta_runtime_readiness_observation.json --runtime-observation-sha256 <independent-sha256>
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_cta_activation_recorder tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

Review the diff and merge it through a PR. The recorder must bind the exact A/A
completion, snapshot manifest, frozen sample, lifecycle reconciliation, design,
decision contract, checked-in collector registry, runtime artifact, workflow
run, and main commit. It may open only
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
   six secondary metrics, one guardrail, Bayesian settings, and no activation
   metric.
4. Start only `vevo-sk-product-cta-color-001` and publish only its Production
   feature rule. Do not publish GTM, edit Meta Ads, or save BiznisWeb forms.
5. Reload GrowthBook and read back that CTA is the only active Production
   experiment at `100%`/`50-50`, while A/A remains stopped at `0%`.
6. With Tag Assistant, verify consent accept/reject/withdrawal, desktop and
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
