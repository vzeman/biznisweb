# VEVO Meta Ads → GrowthBook parameter runbook

This runbook makes Meta campaign, ad-set, ad, and placement available as diagnostic dimensions in the same anonymous GrowthBook/reporting population. Meta does not own the experiment split: GrowthBook randomizes an eligible consented visitor only after the VEVO experiment surface renders.

## Canonical URL-parameter contract

Paste this exact value into the ad-level **URL parameters** field, without a leading `?`:

```text
utm_source=meta&utm_medium=paid_social&utm_id={{campaign.id}}&utm_campaign={{campaign.name}}&utm_content={{ad.id}}&meta_adset_id={{adset.id}}&meta_placement={{placement}}
```

The six analyzed dimensions are `utm_source`, `utm_medium`, `utm_id`, `utm_content`, `meta_adset_id`, and `meta_placement`. `utm_campaign={{campaign.name}}` is a human-readable diagnostic label only; mutable names never replace the stable IDs.

Never add email, phone, name, address, customer/account ID, `fbclid`, `_fbp`, or `_fbc` to this field. Meta may append its own click identifier in transit, but the VEVO experiment collector does not persist it, a full URL, a query string, raw IP, or raw user agent.

## Rollout boundary

- Do not edit an existing live ad only to add this tracking during A/A. Link edits may trigger another ad review, and significant edits can return delivery to preparing/learning.
- Apply the canonical value before the first publish of every new VEVO traffic ad.
- If an ad already has a legitimate planned edit, add the canonical value within that same reviewed change. Never create an extra edit solely for tracking.
- Do not bulk-edit active ads, change campaign/ad-set budget, targeting, bid strategy, optimization, creative, destination, schedule, or status as part of this runbook.
- Do not enable Meta's A/B-test split for the same hypothesis. GrowthBook remains the sole 50/50 assignment system.

## Pre-publish checklist

1. Work at the **Ad** level in Meta Ads Manager and confirm the ad is new or already has a separately authorized planned edit.
2. Confirm the canonical destination remains the intended HTTPS `www.vevo.sk` page. The parameter change must not redirect traffic or select a GrowthBook variation.
3. Preserve only reviewed, non-PII parameters that are still required. Ensure each canonical key occurs exactly once, then paste the exact contract into **Tracking → URL parameters**.
4. Read back the draft. Stable ID macros must remain exactly `{{campaign.id}}`, `{{adset.id}}`, and `{{ad.id}}`; placement must remain `{{placement}}`.
5. Review the complete draft diff. If any unrelated campaign, ad-set, creative, destination, budget, audience, or delivery field changed, discard the draft and investigate.
6. Publishing is a separate ad-operations decision. This runbook documents the measurement contract; it does not authorize a live ad edit by itself.

## Post-publish verification

1. Confirm only the intended ad entered review/processing and later returned to its expected delivery state. Use Meta activity history to verify the exact change boundary.
2. Open the ad preview destination and confirm VEVO loads over HTTPS. Do not copy or store a visitor click ID.
3. After at least one complete UTC day with delivery, run the protected read-only `Audit VEVO GrowthBook and Meta Population` workflow against the immutable image digest for its exact `main` commit.
4. Compare only sanitized aggregates. Relative to baseline run `32464046045`, the covered-ad counts for `utm_content`, `meta_adset_id`, and `meta_placement` must increase when a contract-compliant ad has delivered; invalid dimensions and forbidden configured click identifiers must remain zero.
5. Population parity must continue to show zero duplicate keys and zero assignment/outcome anti-join misses. Dimension coverage is diagnostic and never changes the primary all-traffic experiment decision.

## Safe rollback

- Before publish: discard the affected draft.
- After publish: pause only the newly created or already-planned edited ad if its destination or parameters are wrong. Do not mass-edit legacy ads or touch budgets/targeting as a tracking rollback.
- Correct the draft, re-check the full diff, and let Meta complete its normal review before resuming.
- Record the change and re-run the protected aggregate audit after a complete delivery day. Never delete reporting history to hide a bad parameter interval.

## Current verified baseline

Protected run `32464046045` covered `2026-07-22..2026-08-20`: 19 traffic ads, three campaigns, three ad sets, 2,210 clicks, and EUR 523.13 spend. `utm_source`, `utm_medium`, and `utm_id` covered 100% of clicks/spend; `utm_content`, `meta_adset_id`, `meta_placement`, and the complete six-field contract covered 0%. No forbidden configured click-identifier parameter was found. Existing live ads were not changed.

Official Meta references:

- [Create URL parameters for your ads](https://www.facebook.com/business/help/2360940870872492)
- [Meta ad review policy](https://www.facebook.com/business/ads/review-policy-guidelines)
- [Campaign, ad-set, and ad delivery status](https://www.facebook.com/help/messenger-app/650774041651557)
- [Ads Manager activity history](https://www.facebook.com/help/messenger-app/289211751238030)
