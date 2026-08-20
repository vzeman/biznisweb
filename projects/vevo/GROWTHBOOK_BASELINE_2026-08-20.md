# VEVO GrowthBook baseline — 2026-08-20

Status: GA4 traffic/funnel and production transaction join frozen; consent coverage and performance baselines remain open production gates

Observation window: `2026-07-23..2026-08-19` (latest complete 28 days)

Recent-pace window: `2026-08-13..2026-08-19` (latest complete 7 days)

Scope: production property `Vevo.sk`, Slovak storefront intent; no storefront, admin, GTM, Meta, GrowthBook, AWS, or order mutation was made while collecting this evidence

## Frozen GA4 observations

| Measure | 28-day result |
| --- | ---: |
| Active users | 2,362 |
| Sessions | 2,971 |
| Transactions / purchases | 58 |
| GA4 revenue | EUR 1,360.45 |
| Session key-event rate | 1.95% |
| Homepage active users | 267 |
| Homepage views | 623 |
| `Parfum do prania` category active users | 471 |
| `Parfum do prania` category views | 1,444 |
| Users triggering `view_item` | 759 |
| `view_item` event count | 2,914 |
| Users triggering `add_to_cart` | 261 |
| `add_to_cart` event count | 575 |

The 28-day diagnostic ratio `261 / 759 = 34.39%` is not a joined exposed-user conversion rate: GA4 reported the two event populations separately. It is suitable for surface selection, not a final experiment baseline.

In the latest complete seven days, `451` users triggered `view_item` and `148` triggered `add_to_cart`. The same diagnostic ratio is `32.82%`. This stronger recent product-detail volume makes a product CTA test feasible, while a homepage test based on only `267` active users per 28 days would take several months.

## Frozen traffic-acquisition observations

| Channel | Sessions | Engaged sessions | Engagement rate | Key events | GA4 revenue |
| --- | ---: | ---: | ---: | ---: | ---: |
| All traffic | 2,971 | 2,374 | 79.91% | 58 | EUR 1,360.45 |
| Paid Social | 690 | 566 | 82.03% | 23 | EUR 325.54 |

Paid Social generated `11,912` events, a `3.33%` session key-event rate, and average engagement time of `1m05s`. The report also classified `183` sessions as Unassigned but only one as engaged; this is a diagnostic tracking/traffic-quality signal, not an experiment result.

## Provisional power plan

Assumptions: two-sided `5%` alpha, `80%` power, diagnostic control rate `148 / 451 = 32.82%`, and the current `451` weekly product-viewer pace. Consent and technical eligibility will reduce the actual exposure rate, so the calendar estimates are optimistic.

| Relative MDE | Required per arm | Required total | Optimistic pace |
| ---: | ---: | ---: | ---: |
| 20% | 840 | 1,680 | 26.1 days |
| 25% | 542 | 1,084 | 16.8 days |
| 30% | 380 | 760 | 11.8 days |

The first A/B therefore uses a provisional `25%` relative MDE and `1,084` total exposed devices. The final baseline and sample target must be recomputed from the exposure-linked A/A dataset, frozen before A/B launch, and left unchanged after launch.

## Authoritative metric definition

Metric-definition version: `vevo_cm1_v1_2026-08-20`

```text
CM1 contribution = net order revenue
                 - product expense
                 - packaging cost
                 - net shipping cost
```

CM1 is calculated only from authoritative BiznisWeb/reporting order data after an exact transaction join. It is before allocated Meta/Google ad spend and fixed overhead, which must not be allocated between randomized experiment arms. Browser-submitted money is never authoritative.

The first A/B primary decision metric is the device-level `add_to_cart` rate within 24 hours of first product-detail exposure. CM1 contribution per eligible exposed device is the primary business guardrail.

## Production transaction-join audit

A named GA4 Exploration, `Codex VEVO transaction join audit 2026-08-20`, listed all production-property transaction IDs for the frozen 28-day window. It contained `58` unique IDs and every row had exactly one transaction, so this property showed no duplicate transaction ID in the audited period.

The public GTM purchase configuration maps `ecommerce.transaction_id` to GA4 `transaction_id`. Read-only BiznisWeb API checks produced:

| Join result | Count | Share of 58 GA4 transactions |
| --- | ---: | ---: |
| Exact GA4 `transaction_id` = BiznisWeb `order_num` | 57 | 98.28% |
| Exact and currently `Odoslaná` | 55 | 94.83% |
| Exact and later online-payment-expired | 1 | 1.72% |
| Exact and currently waiting | 1 | 1.72% |
| Not found through the current BiznisWeb order API/search | 1 | 1.72% |

This passes the planned minimum `98%` exact-join gate for the audited production sample and confirms the identifier format required by the collector/reporting contract. The unmatched historical ID remains excluded from authoritative value until it joins exactly; it is not fabricated or manually remapped.

GA4 covered `55 / 165 = 33.33%` of the period's shipped-order aggregate and `58 / 184 = 31.52%` of all created-order aggregate. Consent gating is a plausible explanation, but it is not yet proven because the BiznisWeb aggregate is not consent-scoped and includes failed/expired statuses. Experiment purchases therefore come from the consent-eligible exposure population and are enriched by the authoritative order join; GA4 totals alone are not the source of truth for shop revenue.

## Measurement gaps and NO-GO gates

- GA4 recorded `58` purchases, while the BiznisWeb diagnostic aggregate reported `184` created orders and `165` shipped orders for the same dates. Exact joining is now verified, but the BiznisWeb aggregate also includes failed/expired payment statuses and is not consent-scoped. Neither total is the experiment denominator; the denominator is the consent-eligible exposed-device population.
- A production page and the public GTM configuration expose different GA4 measurement IDs. This is an audit lead only; duplication is not asserted. Tag ownership, destination routing, and purchase deduplication must be verified before A/A.
- Existing Meta URL coverage by stable campaign, ad-set, ad, and placement IDs has not been fully measured. These remain diagnostic dimensions, never the randomization key.
- JavaScript-error device rate, experiment-scoped checkout health, cancellation/refund rate, authoritative AOV, and CM1 per eligible device are not yet available under the experiment population definition. A public performance baseline is frozen below, but per-variation performance requires the new event path.
- Consent-eligible traffic is unknown until the consent-aware exposure event exists.

Production allocation must remain `0%` until consent classification/coverage, performance baselines, the isolated collector, and reporting reconciliation gates pass. The historical join audit passes; the new `order_completed` implementation must independently pass the same gate before A/A acceptance.

## Public performance baseline

Google PageSpeed Insights report time: `2026-08-20 16:59 Europe/Bratislava`.

The representative product URL did not have sufficient URL-level field data, so PageSpeed displayed the `vevo.sk` origin-level Chrome UX Report for the latest 28 days:

| Field metric (p75) | Mobile | Desktop |
| --- | ---: | ---: |
| Core Web Vitals assessment | passed | passed |
| LCP | 1.3 s | 1.3 s |
| INP | 152 ms | 50 ms |
| CLS | 0 | 0 |
| FCP | 1.2 s | 1.0 s |
| TTFB | 0.8 s | 0.7 s |

One Lighthouse run of the representative product detail `/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute` produced:

| Lab metric | Mobile | Desktop |
| --- | ---: | ---: |
| Performance score | 64 | 85 |
| FCP | 3.3 s | 0.5 s |
| LCP | 13.3 s | 1.4 s |
| Total Blocking Time | 150 ms | 260 ms |
| CLS | 0.028 | 0.001 |
| Transfer size | 17,973 KiB | 18,030 KiB |

The single Lighthouse results are diagnostics, not p75 estimates. They show why Preview must be compared under identical repeated conditions and why A/A/A/B must record per-variation web vitals. PageSpeed also observed browser-console errors, but it does not provide a decision-grade device error rate; the collector records only a boolean/error-kind signal and never an error message, stack, URL, or payload.

Locked performance stop thresholds for A/A and A/B after at least `200` measured page loads per arm:

- variant p75 LCP may not exceed control by more than the greater of `200 ms` or `10%`;
- variant p75 INP may not exceed control by more than the greater of `20 ms` or `10%`;
- variant p75 CLS may not exceed control by more than `0.02`;
- variant client-error device rate may not exceed control by more than `0.5` percentage points;
- any reproducible checkout or add-to-cart runtime error stops the rollout immediately regardless of sample.

## Surface decision

The registered first A/B is `vevo-sk-product-cta-color-001` on eligible Slovak product-detail pages. It changes one visual property only: CTA background/color. Homepage hero, product copy and images, labels, prices, discounts, selectors, stock, cart, checkout, payment, order handling, and non-Slovak storefronts remain unchanged.
