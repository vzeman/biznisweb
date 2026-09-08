# VEVO Meta campaign audit — 2026-09-08

This is the sanitized handoff from a read-only inspection of campaign settings, Events Manager, the advertised storefront destination and available reporting methodology. Private financial values and advertising performance remain in the account, private reporting and the user's conversation. This document contains no account, dataset or campaign identifiers, screenshots, customer information or private performance totals.

## Verified observations

The two inspected main ad sets optimize for website Purchase and use the intended VEVO dataset, Highest volume and Advantage+ placements. These core settings are consistent with a purchase objective. They do not, by themselves, establish efficient acquisition or profitable orders.

The attribution windows differ: one uses click attribution and the other also includes engagement attribution. Performance comparison needs a common attribution basis. Audience exclusions also differ between the inspected groups. An acquisition name does not demonstrate that the resulting purchases belong to customers who have never ordered before.

The inspected Events Manager all-events list showed PageView and Purchase. ViewContent, AddToCart and InitiateCheckout were not visible in that list. This establishes a measurement gap to investigate, not proof that those events can never fire or that their absence caused the sales problem.

CAPI is active. The displayed coverage, deduplication and freshness indicators looked healthy, while matching still had gaps. These indicators do not replace a controlled event-delivery check, browser/server reconciliation or validation of the domains supplying the dataset. No unrelated domain identities are recorded here.

The verified main ad destination is the general category path `/c/vevo-fragrance/parfum-do-prania/`. Its first product row emphasizes Natural sample offers and lower price anchors; full-size bundles appear farther down. The observed presentation supports a hypothesis that the destination and offer order influence product mix. A causal conclusion requires a controlled experiment.

All campaigns and pre-existing drafts were left unchanged. This audit did not publish an ad, change a budget, alter an audience, modify tracking, edit the storefront or change runtime infrastructure. No service, worker, watcher or tunnel was started.

## Economic evidence boundaries

The current generated reporting page was blocked by the Comet browser with `ERR_BLOCKED_BY_CLIENT`. No authentication or browser-protection change was attempted. The inspected historical export ends on 2026-08-28; historical customer contribution values must retain that cutoff and must not be described as current live economics.

Meta cost per Purchase and customer-acquisition cost answer different questions. Purchase results can include existing customers and depend on attribution settings. The historical reporting's paid-day cohort proxy is not click- or campaign-level acquisition attribution. Customer cohorts containing a given entry product may overlap and must not be summed as exclusive campaign cohorts.

Contribution thresholds should use the relevant new-customer and product mix, explicit payback horizon, mature cohorts and reconciled variable costs. The historical report notes that payment-provider fees are not a separate ingested cost source. Report revenue and Meta purchase value also need a common treatment of VAT, shipping and refunds before using a ROAS threshold.

A historical account-level marginal CAC derived from adjacent observation windows cannot establish the profitability of every individual product or campaign. Changing spend, promotions, seasonality and customer mix can affect such comparisons. Likewise, a long delay until a repeat purchase does not imply that every sampled customer will return or that delayed contribution necessarily repays acquisition cost.

## Next exact step

1. Validate the event funnel from product view through cart and checkout to Purchase. Check event definitions, timing, browser/server delivery, deduplication, matching inputs and domain-source ownership. Prepare any missing instrumentation as a separately scoped change after the evidence is clear.
2. Reconcile current reporting coverage and new-customer contribution economics. Use comparable periods, a common attribution basis, explicit new-versus-repeat definitions and a declared cohort maturity/payback horizon. Review audience-exclusion intent against those definitions.
3. Prepare a controlled landing-page and offer experiment: keep the creative comparable and test the general category against a specific full-size product or bundle. Assess contribution and full-size purchase share together with acquisition and funnel metrics. Select sample-size, duration and stop criteria before activation; change no live campaign on the basis of this handoff alone.

The immediate outcome is an evidence-backed diagnostic sequence. The observed settings do not prove a single cause of weak selling performance, and this document does not establish a causal budget optimum.
