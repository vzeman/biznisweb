# VEVO GrowthBook baseline — 2026-08-20

Status: GA4 traffic/funnel baseline frozen; authoritative order join and performance baselines remain open production gates

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

## Measurement gaps and NO-GO gates

- GA4 recorded `58` purchases, while the BiznisWeb diagnostic aggregate reported `184` created orders and `165` shipped orders for the same dates. The BiznisWeb aggregate also included failed/expired payment statuses and may cover more than the intended Slovak production population. Neither count is accepted as the experiment denominator until population, status, consent, and tagging differences are reconciled.
- The public GTM purchase mapping supplies `ecommerce.transaction_id` as GA4 `transaction_id`, but equality with the authoritative BiznisWeb API `order_num` has not been demonstrated on one exact confirmation/order pair.
- A production page and the public GTM configuration expose different GA4 measurement IDs. This is an audit lead only; duplication is not asserted. Tag ownership, destination routing, and purchase deduplication must be verified before A/A.
- Existing Meta URL coverage by stable campaign, ad-set, ad, and placement IDs has not been fully measured. These remain diagnostic dimensions, never the randomization key.
- p75 LCP, JavaScript-error rate, checkout-health baseline, cancellation/refund rate, authoritative AOV, and CM1 per eligible device are not yet available under the experiment population definition.
- Consent-eligible traffic is unknown until the consent-aware exposure event exists.

Production allocation must remain `0%` until the exact transaction join, purchase reconciliation, consent classification, performance baselines, and collector/reporting gates pass.

## Surface decision

The registered first A/B is `vevo-sk-product-cta-color-001` on eligible Slovak product-detail pages. It changes one visual property only: CTA background/color. Homepage hero, product copy and images, labels, prices, discounts, selectors, stock, cart, checkout, payment, order handling, and non-Slovak storefronts remain unchanged.
