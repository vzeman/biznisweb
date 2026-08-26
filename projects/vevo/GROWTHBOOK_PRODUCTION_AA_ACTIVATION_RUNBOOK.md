# VEVO GrowthBook Production A/A activation runbook

Status: Production collector deployment and public-isolation evidence verified; the route is available without storefront traffic, while GrowthBook, GTM, and Production allocation remain disabled.

This runbook activates only the invisible A/A experiment `vevo-sk-aa-001`. It does not activate the CTA A/B, change a price, reorder products, edit BiznisWeb content, change Meta Ads delivery, or alter cart, checkout, payment, stock, or order behavior.

## Source of truth

- Activation state: `projects/vevo/growthbook_production_aa_activation.json`
- GrowthBook workspace and evidence state: `projects/vevo/growthbook_workspace.json`
- Collector registry: `growthbook_collector/experiments.json`
- Storefront client: `storefront/vevo-growthbook/vevo-growthbook.js`
- Collector deploy: `.github/workflows/deploy-vevo-growthbook-production-aa-collector.yml`
- A/A acceptance contract: `projects/vevo/growthbook_aa_acceptance.json`

The activation manifest is deliberately redundant across GrowthBook, GTM, collector, and traffic state. A reviewed transition must update all matching fields together; a status-only promotion is invalid.

## Stop conditions

Stop before any external mutation when any of these is true:

- the checked-out commit is not the exact reviewed `main` commit;
- the natural reconciliation, foundation, reader, or clone evidence is missing, unverified, or hash-mismatched;
- the Production stack is not route-disabled and healthy on the exact foundation runtime;
- the Production event registry contains anything except the single reviewed A/A entry;
- the CTA experiment is running, any other Production experiment is active, or A/A weights differ from `50/50`;
- GTM is published, Production allocation is nonzero, or `production_activation_allowed` is already true before the reviewed transition;
- the dedicated Production Athena data source, assignment query, fact tables, or eight Starter metrics differ from their recorded clone IDs and contracts;
- the GrowthBook plan prompts for a trial/upgrade or any credential, client key, event/device/order identity, customer data, or raw query result would enter Git or an evidence artifact.

If a step partially succeeds, record the exact non-secret object IDs and failure boundary in `PROJECT_STATE.md`. Do not delete retained evidence, rotate credentials, repoint Preview, or improvise a second activation path.

## Phase 1 — reviewed collector preparation

After the clone evidence is merged, prepare one separate PR that does only the following:

1. Add `vevo-sk-aa-001` to the Production collector registry with variations `control`, `variant`, the existing exposure/health page allowlists, and the existing event allowlist. Do not add the CTA experiment.
2. Change the activation manifest preconditions to verified, set `collector.registry_entry_present=true`, and open only `collector.deployment_allowed=true`.
3. Keep GrowthBook unstarted, GTM unpublished, traffic allocation `0%`, CTA stopped, Meta Ads unchanged, BiznisWeb unchanged, and `production_activation_allowed=false`.
4. Run the repository validators and merge through reviewed CI. Do not dispatch from a feature branch.

The Production collector workflow must fail before AWS credentials unless all four evidence gates, the exact registry, and the explicit deployment gate agree on the exact `main` commit.

## Phase 2 — deploy and verify the Production collector

Dispatch `Deploy VEVO GrowthBook Production A/A Collector` from the exact reviewed `main` commit.

The workflow must perform these operations in order:

1. Confirm AWS account `919341186960`, region `eu-central-1`, stack `vevo-growthbook-production`, service `vevo-growthbook-collector-production`, container `collector`, and runtime `/app` before code/image deployment.
2. Require the previously recorded route-disabled foundation stack and its exact current task definition/image before changing it.
3. Build and publish one immutable collector image from the reviewed commit.
4. Update only `CollectorTaskDefinition` and `CollectorService` while `PublicRouteEnabled=false`.
5. Resolve the new task definition, service task, private IP, and immutable digest; require a healthy `1/1` service target.
6. Run the exact Fargate task definition with `/app/growthbook_collector/host_gate.sh`; require `curl localhost` health and marker lines for `production` before any route activation.
7. Activate only `CollectorPostRoute` through a separately validated CloudFormation change set.
8. Verify exact CORS for `https://www.vevo.sk`, generic invalid-event rejection, attacker-origin rejection, public `404` for `/health` and `/marker.json`, and a byte-identical raw-S3 object snapshot before/after invalid probes.
9. Upload only sanitized, credential-free activation evidence. Do not publish GTM or start GrowthBook.

After the workflow succeeds, download the exact evidence artifact and independently calculate its SHA-256. Record it only with the offline recorder below, using the exact successful workflow run ID and exact `main` commit:

```text
python scripts/record_growthbook_production_aa_collector_evidence.py --evidence <downloaded-evidence.json> --evidence-sha256 <independent-sha256> --workflow-run-id <successful-run-id> --main-commit <exact-main-commit> --output projects/vevo/growthbook_production_aa_activation.json
```

Then run the activation/workspace validators and merge the resulting manifest through a reviewed PR. The recorder may change only the collector evidence fields, close `collector.deployment_allowed`, set the verified public-route state, and open the zero-allocation UI preparation gate. It cannot publish GTM, start GrowthBook, change allocation, mutate Meta Ads/BiznisWeb, or call a network service. Route availability by itself must receive no storefront traffic.

## Phase 3 — prepare GrowthBook and GTM at zero allocation

Perform one authenticated object at a time and reload each saved object before continuing:

1. Create a separate Production JavaScript SDK connection for project `VEVO SK Web`, environment `production`, API host `https://cdn.growthbook.io`, SDK version `1.7.0`, and the same safe feature-name/rule-ID settings. Never repoint the Preview connection.
2. Create or clone the Production A/A experiment using the recorded Production data source and metric IDs, assignment attribute `id`, variations `control` and `variant`, and weights `0.5/0.5`.
3. Keep the Production feature rule disabled or at `0%`. Do not start the experiment.
4. Build the Production GTM Custom HTML artifact only from the reviewed storefront source and task-scoped Production client key/collector URL. The key must never be committed or copied into the activation evidence.
5. Create new Production tags in the isolated GTM workspace. Do not modify the currently published Google/Meta tags and do not publish a container version.

Read back and record the Production SDK connection ID, experiment ID, feature-rule revision, GTM tag IDs, and artifact SHA-256. Preview objects must remain unchanged.

## Phase 4 — Tag Assistant zero-traffic QA

In Comet with Tag Assistant connected to `https://www.vevo.sk/`, preview the exact Production GTM workspace while GrowthBook remains at `0%`.

Require all of the following on desktop and mobile:

- reject analytical consent: no GrowthBook SDK, feature request, assignment, collector request, storage marker, or CTA style;
- accept analytical consent: SDK and feature request load, but no A/A assignment/exposure occurs at `0%`;
- withdraw analytical consent: GrowthBook is destroyed, sticky assignment/state is cleared, and no later collector request is sent;
- consent regrant restores the zero-allocation SDK state without changing cart or checkout;
- existing GA4 and Meta tags still fire according to their existing consent rules;
- add-to-cart, checkout, purchase, price, product content, and order behavior are unchanged;
- no new console error and no material LCP/INP/CLS regression versus the frozen thresholds.

Any failure stops the rollout. Restore the GTM workspace draft; do not compensate by changing consent categories or BiznisWeb commerce behavior.

### GTM consent-metadata hard gate

Before Phase 5, GTM Consent Overview must be read back for the exact four new
GrowthBook tags `54`, `51`, `55`, and `53`. Their custom code already owns the
consent checks, withdrawal handling, and owned-storage cleanup, so each must be
explicitly marked `no additional consent required`. This metadata does not grant
consent and must not change the tag triggers or code.

The pre-existing Microsoft Clarity tag `43` is unrelated and out of scope. Do
not modify it. Reload each GrowthBook tag after save, repeat the Preview consent
cycle, and require Consent Overview to retain at most the unrelated tag `43`
warning. Record that read-back through Git before GTM publishing is allowed.

## Phase 5 — controlled A/A activation

Use one reviewed maintenance window and this exact order:

0. Require a merged schema-`8` `activation_preflight` in `projects/vevo/growthbook_production_aa_activation.json` whose exact four-tag consent-metadata read-back, reject/regrant Preview QA, live GTM version `15`, public GTM payload hash, and empty Production GrowthBook feature payload are verified. Rollback remains GTM version `14`. This preflight authorizes only the frozen protected post-publish zero-collector observation. GrowthBook start remains closed.
1. **Completed:** publish the reviewed GTM container version while GrowthBook allocation remains `0%`; the recorded live result is version `15` with rollback target `14`.
2. **Completed:** verify live GTM version `15`, the byte-stable public container payload, and the empty Production GrowthBook feature payload. Protected run `32741487449` passed the exact Fargate host gate and found zero API requests and zero accepted receipts in its frozen UTC window; canonical artifact SHA-256 `1cbfcbe6673822210cf36f771c1449c4bafa83d0ef2f8c84102285e5296e6a8b` is recorded in schema `9`.
3. After schema `9` merges, start only `vevo-sk-aa-001` and publish only feature revision `3`, which sets Production to `100%` experiment traffic with the frozen `50/50` split. Treat the live start/publish as one separately confirmed action-time change.
4. Reload GrowthBook and read back the Production environment, tracking key, revision, allocation, weights, Production data source, goal/secondary/guardrail metrics, CUPED off, post-stratification off, activation metric empty, and CTA still stopped.
5. In a fresh consented Tag Assistant session, require one accepted A/A exposure, sticky variation across reload, exact Production collector delivery, unchanged CTA style, and no cart/checkout/order mutation.
6. Record the activation observation through Git. Only then may the manifest say `running_production_aa_only`, allocation `100`, and `production_activation_allowed=true`.

Publishing GTM is not evidence that A/A is running. A running GrowthBook rule is not evidence that GTM is live. The recorded state requires both read-backs plus collector delivery and commerce QA.

## Rollback

Rollback uses the frozen order below; do not reverse it:

1. Set GrowthBook Production A/A allocation to `0%` and verify the live feature payload no longer assigns the experiment.
2. Restore the previous GTM container version and verify the Production loader is absent from the live container.
3. Disable only `CollectorPostRoute` through the reviewed CloudFormation route-removal change set after the loader is gone.

Then verify the collector service remains healthy and retained data remains intact, public `/v1/events` returns `404`, existing GA4/Meta/BiznisWeb behavior is unchanged, and no new exposure is accepted. Never delete the event bucket, raw/curated data, Athena objects, GrowthBook data source, reader identity, or Preview objects as an automatic rollback action.

## Observation window and promotion

The A/A must run for at least seven full Europe/Bratislava calendar days and at least `1,000` eligible devices. The pre-outcome schema-`2` manifest freezes start `2026-08-25T22:00:00Z`, minimum local dates `2026-08-26..2026-09-01`, and the first resolution checkpoint at `2026-09-02 03:45 Europe/Bratislava`. At that and every later successful daily reconciliation, inspect only the cumulative eligible-device count: resolve the through-boundary at the first checkpoint with at least `1,000`; otherwise extend by exactly one whole local day. Do not inspect arm outcomes, split, SRM, conversion, revenue, or performance while resolving the window. `scripts/validate_growthbook_aa_measurement_window.py` recomputes and enforces the frozen provenance, boundaries, and outcome-blind stopping rule. Only after deterministic resolution may the same resolved interval be bound to both sanitized evidence components and the protected aggregate snapshot. Require the offline evaluator result `PASS`, including SRM, split, duplicate, reconciliation, exact-order-join, Meta dimensions, privacy, consent, purchase duplication, desktop/mobile, rollback, and performance gates.

Run `.github/workflows/check-vevo-growthbook-production-aa-window.yml` only at the due checkpoint with `confirm_checkpoint=true`. The workflow selects the exact reconciliation from the bounded success marker plus Scheduler-authenticated CloudTrail `RunTask` event. It prefers retained ECS state and its private IP, but schema `2` explicitly records `cloudtrail_run_task_retention_recovery`, `runtime_state_retained=false`, and `private_ip=null` when the short-lived stopped-task state has expired. That fallback is sufficient only for this read-only checkpoint and never satisfies the live-IP plus localhost-marker hard gate required before an infrastructure mutation. Independently read the successful run ID and exact main commit, download its single `vevo-growthbook-aa-window-checkpoint` artifact, verify that the ZIP contains only `vevo-growthbook-aa-window-checkpoint.json`, and record it offline on a new branch:

```text
python scripts/record_growthbook_aa_window_checkpoint.py --evidence <downloaded-artifact>/vevo-growthbook-aa-window-checkpoint.json --snapshot projects/vevo/growthbook_aa_snapshot.json --output projects/vevo/growthbook_aa_snapshot.json --expected-evidence-sha256 <sha256> --expected-workflow-run-id <run-id> --expected-main-commit <head-sha>
python scripts/validate_growthbook_aa_measurement_window.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_aa_measurement_window tests.test_growthbook_aa_window_checkpoint_recorder tests.test_growthbook_aa_window_checkpoint_workflow tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

Review and merge that manifest transition through PR. A checkpoint below `1,000` changes only the hash-bound history. The first qualifying checkpoint additionally resolves the exact through-boundary and copies it to both still-disabled component manifests; it does not open a producer or snapshot gate.

## Phase 6 — PASS-bound A/A completion and CTA handoff

Do not stop the Production A/A merely because the minimum date or sample was reached. First assemble the exact protected snapshot and require its independently recomputed decision to be `PASS`. Download the single `vevo-growthbook-aa-snapshot` artifact from its successful `main` workflow run, require exactly `vevo-growthbook-aa-snapshot.json` and `vevo-growthbook-aa-decision.json`, independently calculate both SHA-256 values, and run the offline transition on a new branch:

```text
python scripts/record_growthbook_aa_completion.py --output projects/vevo/growthbook_production_aa_completion.json record-pass --snapshot <downloaded-artifact>/vevo-growthbook-aa-snapshot.json --decision <downloaded-artifact>/vevo-growthbook-aa-decision.json --snapshot-sha256 <snapshot-sha256> --decision-sha256 <decision-sha256> --workflow-run-id <snapshot-run-id> --main-commit <snapshot-head-sha>
python scripts/validate_growthbook_aa_completion.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_aa_completion_recorder tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

The recorder accepts compact canonical artifacts only, independently runs the versioned A/A evaluator, requires byte-identical decision output, and rejects `FAIL`, `NOT_READY`, any winner call, a changed frozen window, or a closed snapshot gate. The reviewed PR may open only `manual_growthbook_stop_allowed`; automatic GrowthBook mutation, CTA activation, GTM, Meta Ads, BiznisWeb, collector/reporting, prices, cart, checkout, and orders remain closed.

Only after that exact PASS-binding PR merges, use the authenticated GrowthBook UI and confirm project `prj_2CeEJc6J9FwQFix9UhsnKr`, Production experiment `exp_19g6mmt5wugpk`, linked feature revision `3`, traffic `100%`, and GTM live version `15`. Stop only that A/A experiment, remove only its Production live feature rule, and publish the resulting feature revision. Preserve the staging rule, CTA experiment `exp_19g6mmt1qxzrp` as an unstarted draft at `0%`, GTM version `15`, the collector, reporting schedules, Meta Ads, and BiznisWeb.

Reload the UI and storefront, then create the exact canonical `projects/vevo/growthbook_aa_completion_observation.json` readback required by `record_growthbook_aa_completion.py`. It must prove A/A stopped, zero Production A/A/CTA live rules and allocation, the staging rule retained, CTA still draft, GTM version `15` unchanged with zero pending changes, no A/A assignment or CTA class on desktop/mobile, unchanged add-to-cart text, zero console errors, and no price/cart/checkout/order mutation. Record the post-stop transition on a separate branch:

```text
python scripts/record_growthbook_aa_completion.py --output projects/vevo/growthbook_production_aa_completion.json record-stop --observation projects/vevo/growthbook_aa_completion_observation.json --observation-sha256 <observation-sha256> --workspace projects/vevo/growthbook_workspace.json --workspace-output projects/vevo/growthbook_workspace.json
python scripts/validate_growthbook_aa_completion.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_aa_completion_recorder tests.test_growthbook_workspace tests.test_growthbook_cta_sample_freeze
python scripts/security_ci.py
git diff --check
```

This second transition closes the manual stop gate and records the workspace at zero Production allocation. It prepares only the CTA draft/sample-freeze state; CTA activation remains false. Next, produce the identity-free product-page baseline bound to the same A/A snapshot, freeze the final CTA sample through `freeze_growthbook_cta_sample.py`, and independently finish the 14-day lifecycle reconciliation before any CTA launch review.

### Protected CTA baseline and offline sample freeze

The machine-readable producer contract is `growthbook_cta_baseline.json`; its SHA-bound SQL is `growthbook_sql/cta_baseline_production.sql`. The query uses `received_at`, the exact resolved A/A window, eligible uncontaminated device facts, the first accepted product-page exposure, same-assignment integrity, and an accepted product-page `add_to_cart` no later than 24 hours afterward. It emits only `exposed_devices` and `converted_devices`. It never emits an arm/variation breakdown, raw row, event/device ID, customer/order data, or a winner call.

Do not dispatch `.github/workflows/collect-vevo-growthbook-cta-baseline.yml` until the completed A/A stop/readback transition is merged and the exact resolved through-boundary is at least 24 hours old. The workflow renders the query before AWS credentials; therefore a missing A/A `PASS`, missing zero-allocation stop readback, running allocation, non-draft CTA, or incomplete follow-up fails before AWS. After that local gate it re-verifies the exact Production Fargate task/private IP/service/`/app` identity against the already localhost-marker-gated immutable foundation, checks the two Glue schemas, runs one aggregate-only Athena query, deletes every temporary AWS/schema/query response, and uploads exactly one canonical identity-free artifact for 14 days. It makes no UI or external-system mutation, so a UI test is not applicable to this read-only collection.

After a successful `main` run, independently read its run ID and head SHA, download only artifact `vevo-growthbook-cta-baseline`, require that its ZIP contains only `vevo-growthbook-cta-baseline.json`, and calculate its SHA-256 outside the artifact. Separately download the exact A/A snapshot run already bound in `growthbook_production_aa_completion.json`, require its recorded canonical snapshot SHA-256, and freeze the sample offline on a new branch:

```text
python scripts/freeze_growthbook_cta_sample.py --observation <downloaded-baseline>/vevo-growthbook-cta-baseline.json --observation-sha256 <independent-baseline-sha256> --aa-snapshot <downloaded-aa-snapshot>/vevo-growthbook-aa-snapshot.json --aa-snapshot-sha256 <completion-manifest-snapshot-sha256> --frozen-at-utc <whole-second-UTC-Z> --plan-output projects/vevo/growthbook_cta_sample_plan.json --workspace-output projects/vevo/growthbook_workspace.json
python scripts/build_growthbook_cta_baseline_observation.py validate
python scripts/validate_growthbook_aa_completion.py
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_cta_baseline tests.test_growthbook_cta_baseline_workflow tests.test_growthbook_cta_sample_freeze tests.test_growthbook_workspace
python scripts/security_ci.py
git diff --check
```

Review and merge only the two versioned output files produced by the offline freeze. They may record the final sample and its hashes, but must keep CTA status/feature rule in draft, Production allocation `0%`, `activation_allowed=false`, prices unchanged, and all external mutation gates closed. CTA launch review remains separately blocked by lifecycle reconciliation and the rest of the pre-registered activation gates.

An A/A pass never declares a winner. It only permits a separate reviewed preparation of the non-price CTA A/B. Prices remain out of scope.
