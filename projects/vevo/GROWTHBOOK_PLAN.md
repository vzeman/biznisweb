# VEVO GrowthBook Pro rollout plan

Status: preflight completed, implementation is a conditional GO

Owner: VEVO

Last reviewed: 2026-08-25

## Goal

Safely deploy GrowthBook Pro on the Slovak VEVO BiznisWeb storefront, connect experiment exposures to Meta Ads traffic dimensions and the existing reporting pipeline, validate the complete measurement chain with an A/A test, and then finish one statistically evaluable non-price A/B test without changing checkout behavior, prices, privacy guarantees, or site reliability.

The rollout is complete only when the A/A acceptance gates pass and the first A/B test reaches a documented `WIN`, `LOSE`, or `INCONCLUSIVE` decision. Installing the GrowthBook script alone is not completion.

## Locked scope

| Area | Confirmed target |
| --- | --- |
| Storefront | `https://www.vevo.sk/` |
| Platform | BiznisWeb/FLOX, Verona template |
| Language | Slovak only (`data-lang-code="sk"`) |
| BiznisWeb root/page | root `79`; homepage `299` remains unchanged |
| Confirmed page baseline | homepage `267` active users/28d; product detail `759` `view_item` users/28d and `451`/7d |
| Existing GTM | configured for Slovak language and present in the page head |
| Existing GA4 | configured in BiznisWeb and present on the public site |
| Existing Meta measurement | browser Pixel loaded through GTM; native BiznisWeb Meta Pixel/CAPI fields are empty |
| First A/B surface | Slovak product-detail pages on which the add-to-cart CTA is actually rendered |
| First A/B change | CTA background/color only; label, size, layout, product selector, prices, cart behavior, and all other content stay identical |
| Primary decision metric | unique devices with `add_to_cart` within 24 hours / unique first-exposed product-viewer devices |
| Primary business guardrail | authoritative CM1 contribution per eligible exposed device under metric definition `vevo_cm1_v1_2026-08-20` |
| Purchase attribution window | 7 days from the first valid exposure, frozen before A/A |
| Cancellation/refund maturity checkpoint | 14 days after the first validated server receipt of the exact-joined order-completion event; immature rows remain explicitly flagged |
| Excluded initially | prices, discounts, cart, checkout, payments, stock, product duplication, personalized pricing, and non-Slovak storefronts |

## Architecture decision

GrowthBook Pro will manage assignment, sticky bucketing, preview, experiment state, and statistical analysis. It will not be the only event store.

```text
Meta ad / other traffic
        |
        v
www.vevo.sk (same canonical URL)
        |
        +--> GrowthBook SDK: deterministic 50/50 assignment
        |          |
        |          +--> approved DOM change for the active variant
        |
        +--> first-party experiment collector
                   |
                   v
             validated, PII-free events
                   |
                   v
             AWS raw event-only S3 prefix
                         |
                         v
              VEVO reporting reconciliation
             (exact order join; authoritative CM1)
                         |
                         v
             curated anonymous fact prefixes
                    /                 \
                   v                   v
          Athena read-only        VEVO reporting
                   |
                   v
          GrowthBook Pro results
```

One raw event dataset is therefore reconciled once by the existing reporting boundary and materialized as anonymous device/performance facts used by both GrowthBook and VEVO reporting. GrowthBook Cloud receives only Athena query results over the curated prefixes. Its IAM principal cannot read raw events, order exports, customer records, invoices, secrets, or unrelated client data.

The current Basic-Auth App Runner dashboard is not a public event collector. Ingestion must be a separate endpoint with its own rate limits, validation, CORS policy, logging, and rollback.

## Meta Ads operating model

The first test will not split traffic inside Meta. Included Meta ads keep their intended canonical VEVO destination and GrowthBook randomizes a visitor only after an eligible Slovak product-detail CTA is rendered. This preserves randomization and lets Meta campaign/ad/ad-set/placement remain analysis dimensions rather than competing assignment systems.

New or edited Meta destination URLs must use stable IDs, not only mutable names:

```text
utm_source=meta
utm_medium=paid_social
utm_id={{campaign.id}}
utm_campaign={{campaign.name}}
utm_content={{ad.id}}
meta_adset_id={{adset.id}}
meta_placement={{placement}}
```

The exact macro syntax must be verified in Meta Ads Manager preview before publishing. Existing live ad URLs are not changed during A/A. Raw `fbclid`, `_fbp`, `_fbc`, email, phone, name, address, account ID, and IP address are not stored in the experiment dataset.

The repeatable operator procedure, no-bulk-edit boundary, post-publish aggregate gate, and rollback are frozen in [`META_ADS_GROWTHBOOK_PARAMETER_RUNBOOK.md`](META_ADS_GROWTHBOOK_PARAMETER_RUNBOOK.md).

Future landing-page tests may use GrowthBook URL redirects or separate BiznisWeb pages, but only after the non-redirect A/A and first DOM test pass. Meta must never run its own A/B traffic split at the same time as GrowthBook for the same hypothesis.

## Consent decision

No experiment cookie or event is allowed before its BiznisWeb consent classification is approved and implemented consistently.

Default rollout rule:

1. Load GrowthBook with persistence disabled until the relevant consent signal exists.
2. Start assignment persistence and analytics delivery only after the approved BiznisWeb consent category is granted.
3. If the user withdraws consent, stop new event delivery and remove only the VEVO experiment identifiers owned by this integration.
4. Visitors without the required consent always receive control and are excluded from experiment analysis.

Whether the anonymous sticky-assignment cookie is classified as functional or analytical is a business/privacy decision and must be recorded before production activation. Until that decision exists, production traffic allocation remains `0%`.

A dedicated read-only admin inspection on `2026-08-20` confirmed that the Slovak storefront uses GTM container `GTM-5ZB5LFGB`, every native BiznisWeb Facebook Pixel ID/Access Token input shown for the language versions is empty, and the cookie manager has its Reject button enabled. The manager exposes Mandatory, Functional, Analytical, and Marketing categories; its current Analytical description explicitly covers browsing, component interaction, and conversion events. Public storefront source then confirmed the exact runtime signal `FloxSettings.options.consent & FloxSettings.options.ANALYTIC` and the existing `cookie_consent` data-layer event used for accept/withdraw synchronization. This supports the proposed analytical classification for experiment delivery/measurement, but it does not constitute legal approval; Preview must still prove accept, reject, withdrawal, and reload behavior before Production can move above `0%`.

## Execution plan and acceptance gates

### 1. Preflight — completed

- Confirm the exact Slovak BiznisWeb page and hero block.
- Confirm existing GTM, GA4, consent banner, Meta Pixel path, public headers, and GrowthBook absence.
- Confirm other language roots remain out of scope.
- Confirm no native BiznisWeb A/B engine is documented and no safe public price-variant API exists.
- Preserve all existing BiznisWeb admin content; no connector mutation is used for the hero because its mutation API cannot safely round-trip the full slider content.
- Confirm in the dedicated admin tab that SK GTM, native Meta inputs, and cookie-manager categories match the public preflight; do not press any Save/Confirm control.

Acceptance: read-only evidence is recorded in this file and `PROJECT_STATE.md`; no storefront mutation occurred. The dedicated admin check passed without saving any form.

### 2. Measurement baseline — traffic/funnel and join frozen; consent/performance gates open

The dated artifact `GROWTHBOOK_BASELINE_2026-08-20.md` freezes the observable GA4 traffic/funnel baseline for `2026-07-23..2026-08-19`. Before production allocation, finish the unavailable or non-authoritative parts:

- exact consent-eligible exposure population from the new event dataset;
- authoritative purchase conversion, CM1 per eligible device, average order value, cancellation/refund rate, and order contribution from the reporting join;
- Meta visitors by campaign, ad set, ad, and placement where URL IDs exist;
- p75 LCP and JavaScript error baseline if observable;
- purchase-event completeness and duplication across the currently observed GA4 measurement IDs.

The public GTM mapping plus a named GA4 transaction-ID Exploration and read-only BiznisWeb API audit verified `57/58` exact production joins (`98.28%`), with every GA4 transaction ID counted once. The single historical non-join remains excluded rather than manually mapped. Re-validate the implemented collector path during A/A; do not rely only on this historical sample.

Acceptance: the dated baseline artifact exists, metric definition `vevo_cm1_v1_2026-08-20` is frozen, the historical transaction join exceeds the `98%` gate, and public CrUX plus representative mobile/desktop Lighthouse measurements are recorded. These historical-baseline conditions pass. Consent coverage and purchase/performance reconciliation for the implemented event path remain downstream NO-GO gates.

### 3. GrowthBook workspace — Preview objects initialized

- Authenticated organization `Vevo` and project `VEVO SK Web` now exist. The current UI did not expose a workspace region, so no region is claimed.
- The current workspace is Starter. Its default `staging` environment is the Preview alias; `production` remains disabled for both VEVO flags. No paid Pro upgrade was accepted.
- Preview Web SDK connection `VEVO SK Web Preview` now exists for staging only; its client key was not committed and the connection is not connected until reviewed Preview installation.
- Configure `device_id`/anonymous ID as the single assignment identifier.
- Enable sticky bucketing only after consent.
- Create a dedicated read-only Athena data source limited to experiment tables.
- Keep the production feature at `0%` until all QA gates pass.

The exact, version-controlled object contract in `growthbook_workspace.json` now records the authenticated Starter workspace, staging-as-Preview alias, Preview SDK connection, two safe string flags, and both unstarted staging-only draft experiments. Paste-ready Athena queries under `growthbook_sql/` and the operator handoff in `GROWTHBOOK_PRO_WORKSPACE.md` remain blocked until the protected route-disabled Fargate candidate passes CI and its AWS localhost host gate. A validator rejects PII-bearing/raw-table SQL, metric-window drift, variation mismatches, a changed primary/business guardrail, a published/running experiment, a Production rule/connection, or any nonzero Production allocation. No GrowthBook data source/metric, AWS object, GTM tag, BiznisWeb change, or runtime mutation was created.

Acceptance: Preview can fetch the SDK payload, Production is still `0%`, and Athena can run approved `SELECT` queries but cannot read unrelated S3 prefixes or mutate data.

No GrowthBook subscription purchase or paid upgrade is executed without the account owner completing or explicitly authorizing the charge.

Production reader provisioning and Production GrowthBook cloning are two separate reviewed gates. A successful reader workflow must first produce both the one-day CMS-encrypted credential handoff and a canonical sanitized evidence artifact. The evidence is independently bound to the foundation artifact and exact workflow run/commit, contains no access-key ID or secret, and is recorded only by the offline hash-validating recorder. Recording may close the reader gate and make the clone contract reviewable, but it cannot create GrowthBook objects, repoint Preview, publish GTM, change Meta Ads or BiznisWeb, or move Production allocation above `0%`. The current evidence fields remain null/pending; no Production reader or clone is claimed.

The later clone uses the versioned `GROWTHBOOK_PRODUCTION_CLONE_RUNBOOK.md`, not an undocumented UI session. After the reader gate, the operator creates a separate Production Athena connection, assignment query, two fact tables, and only the eight Starter-compatible metrics, then performs exact UI read-back with zero-row query results while the route and traffic remain disabled. The offline clone recorder binds the new unique object IDs to repository query and metric-contract hashes, requires unchanged Preview IDs, skips the unauthorized p75 metrics, and can update only the clone evidence/target-ID fields. The current canonical observation does not exist and all target IDs remain null.

### 4. Event collector and data contract

- Implement the versioned events in `GROWTHBOOK_DATA_CONTRACT.md`.
- Use a separate public ingestion endpoint, strict schema allowlists, small request limits, origin allowlist, throttling, idempotent event IDs, and append-only storage.
- Never persist raw IP addresses, full URLs/query strings, user-agent strings, Meta click IDs, or customer data.
- Partition the S3 dataset by event date and expose it through a dedicated Glue/Athena database.
- Add reporting ingestion and reconciliation from the same dataset.
- Freeze the browser-to-order attribution window at seven days and keep the 14-day cancellation/refund maturity state explicit rather than silently treating immature orders as final.

Implemented locally on `codex/vevo-growthbook`: the bounded reader scans only explicit raw `event_date` partitions; validated transaction IDs select the matching BiznisWeb records; the existing VEVO realized-revenue and item-cost calculations produce an exact seven-field PII-free order boundary; and one deterministic builder creates the curated device/performance/quality facts used by both consumers. The reconciliation command is dry-run by default and requires a second runtime environment gate in addition to `--publish`. No AWS or shop mutation occurred.

Acceptance: local invalid, oversized, escaped-prefix, duplicate, conflicting, and PII-bearing cases fail closed, and synthetic reconciliation produces one deterministic fact set. The remaining Preview acceptance is to deploy the reviewed endpoint/runtime, prove one valid synthetic event appears exactly once in raw Athena and both curated/reporting consumers, and verify credit-note/refund cost handling before any final A/B CM1 decision.

### 5. Storefront integration

- Implement the SDK bootstrap and collector client as version-controlled code.
- Load it only on the Slovak storefront and make the page/experiment eligibility explicit.
- Preserve control when the SDK/payload, consent API, configuration, or variant selector fails. A collector/network failure must never block cart/checkout; it suppresses dependent cart/health facts and is a rollout `NO-GO` detected by Preview/A/A quality gates.
- Add a deterministic preview override that is unavailable to ordinary traffic.
- Keep price, cart, checkout, and product data untouched.

Because BiznisWeb currently loads GTM in the page head, A/A may begin through a versioned GTM custom template/tag after QA. If flicker or LCP is unacceptable, stop and obtain a supported early-head integration from BiznisWeb before any visual variant.

Implemented locally on `codex/vevo-growthbook`: `storefront/vevo-growthbook/vevo-growthbook.js` is a Preview-only, consent-aware client; `scripts/build_vevo_growthbook_gtm_tag.py` creates a reproducible exact Custom HTML artifact; and the three versioned bridge tags reuse BiznisWeb's existing `cookie_consent`, successful `add_to_cart`, and `purchase` events. Production is hard-disabled, arbitrary SDK/collector inputs fail closed, the manual GrowthBook SDK avoids duplicate GA4/GTM exposure forwarding, and the official SRI-pinned Web Vitals library forwards only bounded numeric LCP/INP/CLS values. No GTM container or storefront was mutated.

Acceptance: unit/security checks pass; the integration is deployed from a branch and PR; rollback disables the GrowthBook tag/feature without editing shop content.

### 6. Non-production and production QA

Test Preview first, then a zero-visual-difference production smoke:

- desktop and mobile;
- consent accept, reject, and withdraw;
- first visit, reload, and new session;
- control and forced variant;
- Meta URL parameters retained as allowlisted dimensions;
- add-to-cart and checkout paths unchanged;
- no duplicate GA4 or Meta purchase events;
- no console errors, broken navigation, layout overlap, or cumulative layout shift;
- collector unavailable: page remains control and checkout still works.

Performance stop thresholds after at least `200` measured page loads per arm are frozen in the baseline artifact: p75 LCP increase greater than `max(200 ms, 10%)`, p75 INP increase greater than `max(20 ms, 10%)`, CLS increase greater than `0.02`, or client-error device-rate increase greater than `0.5` percentage points. Any reproducible cart/checkout runtime error stops immediately without waiting for that sample.

For any AWS deploy, first record the exact instance/task identity, IP, service, and runtime path. Verify the service on-host with `curl localhost` and a build marker before browser UI testing. The reviewed implementation now uses a dedicated non-root ECS/Fargate collector behind an internal ALB and API Gateway VPC Link. Its first CloudFormation change set has no public collector route; the workflow resolves the exact task ID/private IP/service/`/app` runtime, executes `curl http://127.0.0.1:8080/health` and `/marker.json` inside the exact task definition, verifies the immutable image digest and target health, and only then may a second change set add exactly `POST /v1/events`. CI/lint success alone is still not deployment approval.

Acceptance: signed QA checklist and exact rollback test pass. Any checkout, consent, duplication, or performance regression is an immediate NO-GO.

### 7. A/A validation

Run the invisible, site-wide `vevo-sk-aa-001` with identical control and variant at 50/50 among eligible consented Slovak storefront visitors. Its broad eligibility is intentional so it validates assignment, consent, Meta dimensions, purchase joining, and reporting without waiting for a low-volume visual surface. Minimum duration is seven full calendar days and minimum sample is 1,000 unique eligible devices; both conditions must be met.

Pass gates:

- no GrowthBook SRM warning; independently calculated SRM p-value is at least `0.001`;
- control/variant exposure split is within `48%–52%` after at least 1,000 devices;
- collector-to-Athena and Athena-to-reporting count differences are each at most `2%`;
- duplicate accepted `event_id` rate is at most `0.5%`;
- at least `98%` of experiment-attributed purchases join exactly to one BiznisWeb order;
- no device is observed in both variations for the same experiment;
- GrowthBook and VEVO reporting use the same exposure population and agree on variation counts within `2%`;
- no PII is present in sampled payloads or stored rows;
- at least one Meta exposure contains the complete stable campaign, ad-set, ad, and placement contract; source/medium/campaign-only traffic is insufficient and leaves A/A `NOT_READY`;
- no material change in checkout health, Meta/GA4 purchase counts, or p75 LCP versus baseline.

An apparent A/A business-metric winner is investigated, never promoted. Any failed data-quality gate stops the rollout and restarts A/A only after a versioned fix.

The machine-readable decision contract is frozen in `growthbook_aa_acceptance.json` and evaluated only from an aggregate, PII-free snapshot by `scripts/evaluate_growthbook_aa.py`. The evaluator independently recomputes SRM, local-calendar duration, split, count differences, duplicate and exact-join rates, and performance deltas. It also requires the consent, privacy, Meta-dimension, desktop/mobile, commerce-health, purchase-duplication, and rollback gates. Its only valid conclusions are `PASS`, `FAIL`, and `NOT_READY`; `winner_calls_allowed` is always false. Production activation and the later CTA A/B remain separate reviewed actions even after an A/A `PASS`.

The authoritative accepted-duplicate numerator and denominator come from the collector's PII-free receipt markers, not from S3 row counts: an idempotent retry is intentionally absent as a second raw object. Each successfully handled request produces only `accepted=true` plus a boolean `duplicate` marker after persistence. A protected read-only workflow may temporarily export the exact bounded CloudWatch marker window, reduce it offline with `scripts/summarize_growthbook_receipts.py`, and retain only the three aggregate counts. Raw log events, event/device IDs, messages, stream names, timestamps, and customer/order data must never enter the evidence artifact or Git.

The final evaluator input is assembled only through `build-vevo-growthbook-production-aa-snapshot.yml`. It consumes two separately produced canonical artifacts: automated aggregate Production evidence and manual GrowthBook/Tag Assistant/commerce QA evidence. Both are bound to independently recorded successful main-branch workflow run IDs, commits, and SHA-256 digests and must cover the identical frozen window. The assembler validates but strips infrastructure/provenance fields, uploads no source component, and emits only the exact PII-free snapshot plus the offline decision. Its manifest remains disabled until all Production rollout gates and both evidence producers are verified; a local component file or workflow input can never open the gate.

Manual QA evidence is never a free-form workflow checkbox. After the real signed-in browser checks, record the exact canonical observation in `growthbook_aa_manual_qa_observation.json`, review its SHA-256 through Git, and only then open the separately versioned producer gate. The main-only producer verifies that Production A/A is the only running experiment, CTA still has no live rule, foundation/reader/clone are complete, allocation is 100%, and the observation contains no event/device/customer/order identity or unplanned mutation. It then injects its own run/commit provenance and uploads only the sanitized component needed by the snapshot assembler.

Automated evidence is also never accepted from workflow form numbers or a moving "latest" source. A reviewed manifest change must first freeze the complete Production A/A UTC window plus the exact curated reporting-quality object key and SHA-256. Only then may the main-only producer read the exact Production Fargate task and localhost-gated immutable image, verify the Glue schemas, reduce the bounded PII-free collector receipts, read that one hash-bound quality object, and run one aggregate-only Athena audit for pipeline, Meta, privacy, and consent counts. Temporary CloudWatch/AWS responses and query files are deleted before the single sanitized component is uploaded. The producer cannot deploy or mutate AWS resources, GrowthBook, GTM, Meta Ads, BiznisWeb, traffic, a winner, or the CTA experiment.

### 8. First A/B experiment

Experiment ID: `vevo-sk-product-cta-color-001`

Hypothesis: changing only the product-detail add-to-cart CTA background from the current control color to one approved, accessible, high-contrast VEVO brand color will increase the share of exposed product viewers who add a product to cart because the primary action is easier to notice.

- Population: eligible consented devices on Slovak product-detail pages where the add-to-cart CTA is actually rendered.
- Split: 50/50, sticky by anonymous device ID.
- Control: the current CTA color.
- Variant `brand_contrast`: VEVO's existing gold gradient `#c9a962` → `#b8956f` with dark brand text `#0f172a`; label, dimensions, placement, selectors, prices, cart behavior, and every other element remain unchanged.
- The exact visual boundary is machine-readable in `growthbook_cta_design.json` and enforced against the version-controlled storefront source by `scripts/validate_growthbook_cta_design.py`. WCAG 2.2 contrast is `7.9359:1` at `#c9a962` and `6.4325:1` at `#b8956f`, both above the frozen `4.5:1` normal-text minimum. CI permits only `background-color`, `background-image`, and `color`; label/content, dimensions, layout, placement, selector, price, cart, and checkout mutations fail closed.
- Primary metric: binary device-level `add_to_cart` within 24 hours of first valid exposure, divided by unique first-exposed product-viewer devices.
- Primary business guardrail: authoritative CM1 contribution per eligible exposed device under `vevo_cm1_v1_2026-08-20`.
- Other guardrails: purchase conversion, AOV, cancellation/refund rate, p75 LCP, JavaScript errors, and checkout health.
- Dimensions for diagnosis only: Meta campaign ID, ad-set ID, ad ID, placement, device type, and new/returning device. Dimension findings do not replace the primary all-traffic decision.
- Minimum run: 14 full days and two complete weekday cycles.
- Maximum planned run: 42 days.
- Provisional planning target: `1,084` total exposed devices (`542`/arm), calculated from the diagnostic seven-day GA4 ratio `148 add_to_cart users / 451 view_item users`, a `25%` relative MDE, `80%` power, and two-sided `5%` alpha. Because the GA4 ratio is not an exposure-linked cohort, recompute once from A/A event data, freeze the final target before launch, and never change it after observing A/B results.
- The machine-readable planning contract is `growthbook_cta_sample_plan.json`. `scripts/freeze_growthbook_cta_sample.py` reproduces the provisional `542`/arm result and may replace it only from a canonical, independently SHA-256-bound, PII-free aggregate plus the exact canonical A/A snapshot. It re-runs the versioned A/A evaluator itself and requires `PASS`; a manually typed verdict is never trusted. The baseline uses first valid A/A product-page exposure as a conservative planning proxy; the live CTA decision denominator remains the exact first exposure where the CTA is rendered. The tool updates only the sample plan and matching workspace fields, keeps the experiment and feature rule in draft, and leaves `activation_allowed=false` and Production allocation at `0%`.
- The protected baseline producer is versioned by `growthbook_cta_baseline.json`, `growthbook_sql/cta_baseline_production.sql`, and `.github/workflows/collect-vevo-growthbook-cta-baseline.yml`. It is main-only and fails before AWS credentials unless the checked-in A/A completion proves independently reproduced `PASS`, the reviewed manual stop, zero A/A/CTA Production rules and allocation, CTA still draft, and a complete 24-hour follow-up. Its Athena result has exactly two columns (`exposed_devices`, `converted_devices`); arm breakdowns, raw AWS payloads, identities, customer/order data, winner calls, and all external mutations are excluded. `freeze_growthbook_cta_sample.py` remains the separate offline hash-bound recorder, so collection cannot launch CTA.
- The A/A-to-CTA lifecycle boundary is machine-readable in `growthbook_production_aa_completion.json`. `scripts/record_growthbook_aa_completion.py` first binds the exact successful snapshot workflow run, main commit, canonical snapshot and decision hashes, and independently reproduced `PASS`; only that reviewed state permits the one manual GrowthBook stop. A second canonical readback must then prove the exact A/A experiment stopped, its Production live rule absent, both A/A and CTA at `0%`, CTA still draft, staging preserved, GTM version `15` unchanged, clean desktop/mobile storefront behavior, and no Meta Ads/BiznisWeb/commerce mutation. The recorder has no external clients and transitions the repo only to CTA sample-freeze readiness; it never activates CTA.
- The CTA launch boundary is separately machine-readable in `growthbook_cta_activation.json` and `GROWTHBOOK_CTA_ACTIVATION_RUNBOOK.md`. Its offline recorder cannot open the manual GrowthBook start until it binds the exact A/A completion/snapshot, frozen sample, verified lifecycle reconciliation, immutable design/decision hashes, CTA-only collector registry, and a successful canonical runtime observation. That observation must prove the exact Production Fargate private IP, service, `/app` path, task definition, image digest, localhost marker, healthy target, zero pre-start CTA events, zero A/A/CTA allocation, GTM version `15`, and zero pending GTM changes. A separate canonical start readback then requires CTA as the only active Production experiment plus consent, desktop/mobile, both-variation, sticky collector, exact-CSS, and unchanged-commerce verification. Automatic GrowthBook/GTM/Meta/BiznisWeb/collector/commerce mutation and winner calls remain false.
- Decision timing: assignment is stopped when the frozen first-`N` cohort reaches its target, or after 42 full local calendar days if the target is not reached. The one and only primary-metric final look occurs after the stop plus the complete 14-day lifecycle follow-up; no primary winner/loser call is made from a running or partially mature cohort. Independently pre-registered price/cart/checkout, client-error, and performance safety guardrails may stop the test early without inspecting the primary result.

Decision contract:

- `WIN`: primary metric clears the pre-registered statistical/economic threshold and no guardrail has material harm.
- `LOSE`: primary metric shows material harm or a guardrail crosses its stop threshold.
- `INCONCLUSIVE`: maximum duration is reached without enough evidence; control remains live.

The chosen decision, exact sample, interval/effect estimate, guardrails, and follow-up are written to the experiment registry and `PROJECT_STATE.md`.

The executable contract is `growthbook_cta_decision_contract.json`, evaluated only from one exact aggregate, PII-free snapshot by `scripts/evaluate_growthbook_cta.py`. The primary fixed-horizon two-proportion test uses two-sided α `5%`; `WIN` additionally requires the 95% CM1-per-exposed-device interval to rule out a relative decrease worse than `10%`, mature cancellation/refund coverage, performance/client-error safety, exact first-`N` selection, price/cart/checkout integrity, privacy, SRM, reconciliation, and join gates. Significant primary harm or material safety harm is `LOSE`; a complete fixed look without a safe significant result is `INCONCLUSIVE`; an open assignment, immature follow-up, unfrozen sample, or missing evidence remains `NOT_READY`. The evaluator always returns `automatic_mutation_allowed=false`.

`growthbook_cta_lifecycle_reconciliation.json` is a separate reviewed evidence gate for the known refund/credit-note value boundary. It is currently pending, false, identity-free, and activation-disabled. `scripts/record_growthbook_cta_lifecycle_reconciliation.py` accepts only canonical JSON plus an independently supplied SHA-256, exact cent-level CM1 parity, at least one mature cancellation/refund/credit-note case, matching lifecycle counts, the exact curated reporting-quality object key/hash, and explicit read-only/no-mutation/no-identity flags. It writes the versioned observation and may change only the allowlisted manifest fields; a verified manifest is rejected unless the observation file exists and its canonical SHA-256 matches. A final CTA decision cannot use CM1, cancellation, or refund outcomes until this 14-day gate passes, and a boolean copied into an experiment snapshot cannot bypass it.

## Rollback

Rollback order is deliberately independent of BiznisWeb content edits:

1. Set the GrowthBook production feature to control/`0%`.
2. Disable only the versioned VEVO GrowthBook GTM tag.
3. Confirm the public product-detail CTA returns to its original color and checkout is healthy.
4. Leave the append-only event dataset intact for audit; stop new ingestion only if required.
5. Revert infrastructure/code through Git and the documented deployment workflow.

Rollback must not disable the existing GA4, Meta Pixel, consent banner, or unrelated GTM tags.

## Current GO/NO-GO

`GO` for implementation and Preview/A/A preparation.

`NO-GO` for production allocation above `0%` until all of the following are true:

- GrowthBook Pro workspace/client key exists;
- consent category and retention are approved;
- the dated traffic/funnel baseline exists and its remaining measurement gaps are closed;
- the implemented `order_completed` path reproduces the verified `transaction_id` → `order_num` join at or above `98%`;
- isolated collector and Athena dataset pass security tests;
- Preview and rollback QA pass;
- exact deployment hard-gate evidence exists.
- the route-disabled Fargate candidate passes the exact task-ID/private-IP/service/path plus localhost marker gate before the single public route is added.

`NO-GO` for price testing until BiznisWeb provides a supported, server-authoritative per-visitor price mechanism that keeps product feed, cart, checkout, tax, stock, invoices, and legal display consistent.

## Official references

- GrowthBook script-tag SDK: <https://docs.growthbook.io/lib/script-tag>
- GrowthBook data sources and assignment queries: <https://docs.growthbook.io/app/datasources>
- GrowthBook Athena connection: <https://docs.growthbook.io/warehouses/athena>
- GrowthBook warehouse security model: <https://docs.growthbook.io/warehouses>
- GrowthBook URL redirects: <https://docs.growthbook.io/app/url-redirects>
- BiznisWeb Google Tag Manager: <https://www.biznisweb.sk/a/741/google-tag-manager-spravca-znaciek-google>
- BiznisWeb cookie categories: <https://www.biznisweb.sk/a/1302/cookies-lista>
- BiznisWeb Meta Pixel/CAPI: <https://www.biznisweb.sk/a/1409/nastavenie-facebook-conversion-api>
