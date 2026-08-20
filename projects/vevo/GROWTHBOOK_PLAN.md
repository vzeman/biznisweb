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
| BiznisWeb root/page | root `79`, homepage `299` |
| Current hero | slider block `1778`, four image slides |
| Existing GTM | configured for Slovak language and present in the page head |
| Existing GA4 | configured in BiznisWeb and present on the public site |
| Existing Meta measurement | browser Pixel loaded through GTM; native BiznisWeb Meta Pixel/CAPI fields are empty |
| First A/B surface | homepage hero only |
| First A/B change | control has no overlay headline; variant adds one value-proposition headline while image, link, price, products, checkout, and all other content stay identical |
| Primary business metric | realized contribution profit per eligible exposed visitor, using the version-frozen VEVO reporting formula |
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
             AWS event-only S3 prefix
                /            \
               v              v
        Athena read-only    VEVO reporting
               |
               v
        GrowthBook Pro results
```

One event dataset is therefore used by both GrowthBook and the existing reporting. GrowthBook Cloud receives only aggregate query results from Athena. Its IAM principal must have least-privilege read access to the experiment-only dataset and query-result location; it must not be able to read order exports, customer records, invoices, secrets, or unrelated client data.

The current Basic-Auth App Runner dashboard is not a public event collector. Ingestion must be a separate endpoint with its own rate limits, validation, CORS policy, logging, and rollback.

## Meta Ads operating model

The first test will not split traffic inside Meta. All included Meta ads use the same canonical VEVO homepage URL and GrowthBook randomizes visitors after arrival. This preserves randomization and lets Meta campaign/ad/ad-set/placement remain analysis dimensions rather than competing assignment systems.

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

### 2. Measurement baseline — next

Use the latest complete 28 days, excluding known outages and internal traffic, to freeze:

- eligible homepage visitors and sessions;
- purchase conversion rate;
- contribution profit per eligible visitor;
- average order value and order contribution;
- cancellation/refund rate;
- Meta visitors by campaign, ad set, ad, and placement where URL IDs exist;
- p75 LCP and JavaScript error baseline if observable;
- current purchase-event count and transaction-ID completeness.

Verify on one safe test order or a pre-approved existing order that BiznisWeb's confirmation-page `transactionId` equals the reporting API `order_num`. Do not infer equality from documentation.

Acceptance: a dated baseline artifact exists, metric formula version is frozen, and the transaction join succeeds exactly. Otherwise the rollout is NO-GO.

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

For any AWS deploy, first record the exact instance/task identity, IP, service, and runtime path. Verify the service on-host with `curl localhost` and a build marker before browser UI testing.

Acceptance: signed QA checklist and exact rollback test pass. Any checkout, consent, duplication, or performance regression is an immediate NO-GO.

### 7. A/A validation

Run `vevo-sk-aa-001` with identical control and variant at 50/50 among eligible consented visitors. Minimum duration is seven full calendar days and minimum sample is 1,000 unique eligible devices; both conditions must be met.

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

Experiment ID: `vevo-sk-home-hero-headline-001`

Hypothesis: adding one concise value-proposition headline to the existing first homepage hero will increase realized contribution profit per eligible exposed visitor because visitors understand VEVO's offer earlier.

- Population: eligible consented devices on the Slovak homepage.
- Split: 50/50, sticky by anonymous device ID.
- Control: current hero, no overlay headline.
- Variant: one approved overlay headline; no other difference.
- Primary metric: contribution profit per eligible exposed visitor.
- Guardrails: purchase conversion, AOV, cancellation/refund rate, add-to-cart rate, p75 LCP, JavaScript errors, and checkout health.
- Dimensions for diagnosis only: Meta campaign ID, ad-set ID, ad ID, placement, device type, and new/returning device. Dimension findings do not replace the primary all-traffic decision.
- Minimum run: 14 full days and two complete weekday cycles.
- Maximum planned run: 42 days.
- Sample target: calculated from the frozen baseline before launch at 80% power, two-sided 5% alpha, and the smallest economically meaningful effect. It is written into the experiment record before launch and is never reduced after seeing results.
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
3. Confirm the public site returns the original hero and checkout is healthy.
4. Leave the append-only event dataset intact for audit; stop new ingestion only if required.
5. Revert infrastructure/code through Git and the documented deployment workflow.

Rollback must not disable the existing GA4, Meta Pixel, consent banner, or unrelated GTM tags.

## Current GO/NO-GO

`GO` for implementation and Preview/A/A preparation.  
`NO-GO` for production allocation above `0%` until all of the following are true:

- GrowthBook Pro workspace/client key exists;
- consent category and retention are approved;
- 28-day baseline is frozen;
- `transactionId` → `order_num` is verified;
- isolated collector and Athena dataset pass security tests;
- Preview and rollback QA pass;
- exact deployment hard-gate evidence exists.

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

