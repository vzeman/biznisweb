# VEVO GrowthBook Pro rollout plan

Status: preflight completed, implementation is a conditional GO

Owner: VEVO

Last reviewed: 2026-08-20

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
| Cancellation/refund maturity checkpoint | 14 days after the authoritative order timestamp; immature rows remain explicitly flagged |
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

Future landing-page tests may use GrowthBook URL redirects or separate BiznisWeb pages, but only after the non-redirect A/A and first DOM test pass. Meta must never run its own A/B traffic split at the same time as GrowthBook for the same hypothesis.

## Consent decision

No experiment cookie or event is allowed before its BiznisWeb consent classification is approved and implemented consistently.

Default rollout rule:

1. Load GrowthBook with persistence disabled until the relevant consent signal exists.
2. Start assignment persistence and analytics delivery only after the approved BiznisWeb consent category is granted.
3. If the user withdraws consent, stop new event delivery and remove only the VEVO experiment identifiers owned by this integration.
4. Visitors without the required consent always receive control and are excluded from experiment analysis.

Whether the anonymous sticky-assignment cookie is classified as functional or analytical is a business/privacy decision and must be recorded before production activation. Until that decision exists, production traffic allocation remains `0%`.

## Execution plan and acceptance gates

### 1. Preflight — completed

- Confirm the exact Slovak BiznisWeb page and hero block.
- Confirm existing GTM, GA4, consent banner, Meta Pixel path, public headers, and GrowthBook absence.
- Confirm other language roots remain out of scope.
- Confirm no native BiznisWeb A/B engine is documented and no safe public price-variant API exists.
- Preserve all existing BiznisWeb admin content; no connector mutation is used for the hero because its mutation API cannot safely round-trip the full slider content.

Acceptance: read-only evidence is recorded in this file and `PROJECT_STATE.md`; no storefront mutation occurred.

### 2. Measurement baseline — traffic/funnel and join frozen; consent/performance gates open

The dated artifact `GROWTHBOOK_BASELINE_2026-08-20.md` freezes the observable GA4 traffic/funnel baseline for `2026-07-23..2026-08-19`. Before production allocation, finish the unavailable or non-authoritative parts:

- exact consent-eligible exposure population from the new event dataset;
- authoritative purchase conversion, CM1 per eligible device, average order value, cancellation/refund rate, and order contribution from the reporting join;
- Meta visitors by campaign, ad set, ad, and placement where URL IDs exist;
- p75 LCP and JavaScript error baseline if observable;
- purchase-event completeness and duplication across the currently observed GA4 measurement IDs.

The public GTM mapping plus a named GA4 transaction-ID Exploration and read-only BiznisWeb API audit verified `57/58` exact production joins (`98.28%`), with every GA4 transaction ID counted once. The single historical non-join remains excluded rather than manually mapped. Re-validate the implemented collector path during A/A; do not rely only on this historical sample.

Acceptance: the dated baseline artifact exists, metric definition `vevo_cm1_v1_2026-08-20` is frozen, the historical transaction join exceeds the `98%` gate, and public CrUX plus representative mobile/desktop Lighthouse measurements are recorded. These historical-baseline conditions pass. Consent coverage and purchase/performance reconciliation for the implemented event path remain downstream NO-GO gates.

### 3. GrowthBook Pro workspace

- Create organization `VEVO` and project `VEVO SK Web` in the EU data region when offered.
- Create Production and Preview environments.
- Create one Web SDK connection; store the client key as configuration, never a server secret.
- Configure `device_id`/anonymous ID as the single assignment identifier.
- Enable sticky bucketing only after consent.
- Create a dedicated read-only Athena data source limited to experiment tables.
- Keep the production feature at `0%` until all QA gates pass.

Acceptance: Preview can fetch the SDK payload, Production is still `0%`, and Athena can run approved `SELECT` queries but cannot read unrelated S3 prefixes or mutate data.

No GrowthBook subscription purchase or paid upgrade is executed without the account owner completing or explicitly authorizing the charge.

### 4. Event collector and data contract

- Implement the versioned events in `GROWTHBOOK_DATA_CONTRACT.md`.
- Use a separate public ingestion endpoint, strict schema allowlists, small request limits, origin allowlist, throttling, idempotent event IDs, and append-only storage.
- Never persist raw IP addresses, full URLs/query strings, user-agent strings, Meta click IDs, or customer data.
- Partition the S3 dataset by event date and expose it through a dedicated Glue/Athena database.
- Add reporting ingestion and reconciliation from the same dataset.
- Freeze the browser-to-order attribution window at seven days and keep the 14-day cancellation/refund maturity state explicit rather than silently treating immature orders as final.

Acceptance: invalid, oversized, cross-origin, duplicate, and PII-bearing payload tests fail closed; valid synthetic events appear once in Athena and reporting.

### 5. Storefront integration

- Implement the SDK bootstrap and collector client as version-controlled code.
- Load it only on the Slovak storefront and make the page/experiment eligibility explicit.
- Preserve control when the SDK, collector, consent API, or variant selector fails.
- Add a deterministic preview override that is unavailable to ordinary traffic.
- Keep price, cart, checkout, and product data untouched.

Because BiznisWeb currently loads GTM in the page head, A/A may begin through a versioned GTM custom template/tag after QA. If flicker or LCP is unacceptable, stop and obtain a supported early-head integration from BiznisWeb before any visual variant.

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

For any AWS deploy, first record the exact instance/task identity, IP, service, and runtime path. Verify the service on-host with `curl localhost` and a build marker before browser UI testing. The current proposed API Gateway/Lambda collector has no instance ID, host IP, service manager, or localhost surface, so it remains deployment-blocked under this hard-gate even though its CloudFormation can be reviewed and linted. Resolve the architecture/policy mismatch before creating a stack; do not reinterpret lint success as deployment approval.

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
- no material change in checkout health, Meta/GA4 purchase counts, or p75 LCP versus baseline.

An apparent A/A business-metric winner is investigated, never promoted. Any failed data-quality gate stops the rollout and restarts A/A only after a versioned fix.

### 8. First A/B experiment

Experiment ID: `vevo-sk-product-cta-color-001`

Hypothesis: changing only the product-detail add-to-cart CTA background from the current control color to one approved, accessible, high-contrast VEVO brand color will increase the share of exposed product viewers who add a product to cart because the primary action is easier to notice.

- Population: eligible consented devices on Slovak product-detail pages where the add-to-cart CTA is actually rendered.
- Split: 50/50, sticky by anonymous device ID.
- Control: the current CTA color.
- Variant: one approved high-contrast brand-palette CTA color; label, dimensions, placement, selectors, prices, cart behavior, and every other element remain unchanged.
- Primary metric: binary device-level `add_to_cart` within 24 hours of first valid exposure, divided by unique first-exposed product-viewer devices.
- Primary business guardrail: authoritative CM1 contribution per eligible exposed device under `vevo_cm1_v1_2026-08-20`.
- Other guardrails: purchase conversion, AOV, cancellation/refund rate, p75 LCP, JavaScript errors, and checkout health.
- Dimensions for diagnosis only: Meta campaign ID, ad-set ID, ad ID, placement, device type, and new/returning device. Dimension findings do not replace the primary all-traffic decision.
- Minimum run: 14 full days and two complete weekday cycles.
- Maximum planned run: 42 days.
- Provisional planning target: `1,084` total exposed devices (`542`/arm), calculated from the diagnostic seven-day GA4 ratio `148 add_to_cart users / 451 view_item users`, a `25%` relative MDE, `80%` power, and two-sided `5%` alpha. Because the GA4 ratio is not an exposure-linked cohort, recompute once from A/A event data, freeze the final target before launch, and never change it after observing A/B results.
- Decision timing: no winner/loser call before both minimum duration and planned sample are reached. Guardrail harm may stop the test early.

Decision contract:

- `WIN`: primary metric clears the pre-registered statistical/economic threshold and no guardrail has material harm.
- `LOSE`: primary metric shows material harm or a guardrail crosses its stop threshold.
- `INCONCLUSIVE`: maximum duration is reached without enough evidence; control remains live.

The chosen decision, exact sample, interval/effect estimate, guardrails, and follow-up are written to the experiment registry and `PROJECT_STATE.md`.

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
- the serverless collector design is reconciled with the mandatory host-local verification rule, or replaced by a compliant dedicated host design before any AWS mutation.

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
