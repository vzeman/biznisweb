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

0. Require a merged schema-`7` `activation_preflight` in `projects/vevo/growthbook_production_aa_activation.json` whose exact four-tag consent-metadata read-back and reject/regrant Preview QA are verified. Its exact source main commit, zero-traffic evidence SHA-256, GTM artifact SHA-256, GrowthBook clone evidence SHA-256, live feature revision `2`, draft revision `3`, GTM workspace `17`, and rollback target container version `14` must all match the authenticated UI read-back. This preflight authorizes only the zero-allocation GTM publish. GrowthBook start remains closed until the new live GTM version and zero Production exposures are recorded through a separate reviewed Git change.
1. Publish the reviewed GTM container version while GrowthBook allocation remains `0%`.
2. Verify the live container version and repeat reject/accept/withdrawal smoke. Confirm zero A/A exposures at `0%`.
3. After the post-publish read-back is merged and explicitly reopens the next gate, start only `vevo-sk-aa-001` and set its Production rule to `100%` experiment traffic with the frozen `50/50` split.
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

The A/A must run for at least seven full Europe/Bratislava calendar days and at least `1,000` eligible devices. Build the protected aggregate snapshot only after the frozen window closes. Require the offline evaluator result `PASS`, including SRM, split, duplicate, reconciliation, exact-order-join, Meta dimensions, privacy, consent, purchase duplication, desktop/mobile, rollback, and performance gates.

An A/A pass never declares a winner. It only permits a separate reviewed preparation of the non-price CTA A/B. Prices remain out of scope.
