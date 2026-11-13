# VEVO laundry-perfume price review, 2026-11-13

This is a read-only analysis of the owner's reported price reduction on the afternoon
of 2026-11-09. The owner confirmed that only laundry-perfume prices changed;
campaigns, coupons and shipping terms were not deliberately changed. The exact
country/SKU scope is established only where transactions demonstrate a price change.

## Reproduce

From the repository root, with existing read-authorized AWS credentials and the
repository's boto3 dependency:

```text
python -B projects/vevo/analysis/review_price_change_20261113.py --profile codex
```

The script reads two existing immutable publication objects in memory, checks
their pinned SHA256 values and emits aggregate JSON to stdout. It performs no
AWS writes, report regeneration, email send or runtime change. It creates no raw
order files and emits no customer identifiers or addresses.

**The repository is public. Output contains confidential business aggregates and
must not be committed to Git.** Actual business figures are returned privately to
the owner. Runtime artifacts plus this analysis reproduce the result; no unique
local spreadsheet or untracked helper is needed.

## Evidence and design

- Snapshot: generation `20261113T003223Z`, through 2026-11-12.
- Email HTML and the immutable published HTML were independently byte-matched:
  SHA256 `1a0f524dd951600f7b71b63407044be911d8e9845867ebed6c4d91842ab2d29d`.
- Exclude 9 November (mixed old/new prices) and 13 November (incomplete).
- Compare complete 10-12 November with 3-5 November, plus the four preceding
  Tuesday-Thursday blocks as the primary broader baseline. Retain eight prior
  matched blocks for sensitivity, not as a randomized control group.
- Primary outcome: modeled contribution after product, packaging, shipping and
  advertising costs, before shared fixed costs. Use published daily totals,
  including credit-fulfillment adjustments, as the financial authority.
- Sum order revenue once per order number; it already includes credit adjustment.
  Sum line net sales for SKU economics. Never sum repeated daily advertising or
  per-order packaging columns across item rows.
- Unit price in original currency is before allocated header discounts. Effective
  realized EUR price is line net sales divided by quantity. Preserve SKU, label,
  currency and size; an SKU alone may collide across a gel and a bundle.
- Exact labels in `price_review_categories.json` were reviewed for these selected
  windows. They classify full-size laundry perfumes, perfume bundles, samples,
  a mixed discovery bundle, services and other items. They are not a reusable
  catalog taxonomy, nor proof that every perfume SKU was reduced.
- A parent bundle counts as one sold bundle, not one physical bottle. Category
  order counts overlap. Dashboard `items` counts analytical rows, not quantity.
- Reconcile CSV order count/revenue with published daily totals and reconcile
  all country values with company totals. Show any cost-sidecar difference.
- Separate first and returning orders using existing global customer-history
  flags. Do not drop orders without email from business totals.

## Interpretation boundaries

Results and country/cohort conclusions are provided privately to the owner.
Reproduce them from the pinned runtime sources; no derived business performance
is published in this public repository. Assess country sample adequacy separately,
compare both the immediate prior week and the broader baseline, and inspect
realized SKU prices, cost coverage and basket/cohort composition. Surface any
fallback costs without inventing replacement acquisition costs.

Three days can quantify observed economics but cannot establish durable demand,
conversion improvement, price elasticity or long-term customer value. No measured
traffic denominator or randomized pricing holdout was used. Seasonality, traffic
mix, market events and purchases brought forward remain plausible explanations
even when no deliberate campaign setting changed. Recent orders can also mature
through cancellations/returns under the existing accepted reporting policy.

Suggested checkpoints, not scheduled jobs or guarantees of statistical power:
two complete weeks for an initial stability check, four to six weeks for a
broader view, and later repeat-purchase cohorts to test stock-up/pull-forward
effects. Preserve comparison weekdays, country/variant mix, spend and first/
returning status; log later interventions.

## Verification and boundaries

The analysis checks pinned artifacts, eligibility, complete dates, duplicate
dates, order totals, line/credit reconciliation and country/company agreement.
Source QA warnings and recent fallback lines remain visible. No reporting rules,
catalog prices, advertising settings, production services or email behavior are
changed. No local server, worker or tunnel is started.

Methodological reference:
[Microsoft Research: External Validity of Online Experiments](https://www.microsoft.com/en-us/research/articles/external-validity-of-online-experiments-can-we-predict-the-future/).
