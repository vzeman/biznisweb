# Natural and Premium bottle analysis

## Scope and identity

This is an offline customer-retention and merchandise-margin analysis, not a
production reporting change. Only laundry perfumes in 200 ml and 500 ml bottles
are eligible. The primary view includes verified perfume-only multipacks; a
separate sensitivity view excludes every set. Samples, floor/dryer/interior
fragrances, gel, cleaners and mixed gift bundles are excluded. Mixed-bundle
revenue/costs are not attributed to perfume without component sales evidence.

`laundry_series_catalog.py` owns classification. Live catalogue verification on
2026-10-19 confirmed Natural product IDs 248/244/1525/1531/1532/1533 and Premium
1576/1577/1578. Warehouse codes distinguish the variants: Natural 01200c/01500c,
02200c/02500c, 06200/06500, 07200/07500, 08200/08500, 09200/09500; Premium
07200p/07500p, 08200p/08500p, 09200p/09500p.

EAN alone is unsafe: historical Natural and Premium rows share EANs. Historical
names without the series usually map to Natural through committed aliases, but
a verified Premium warehouse code outranks a legacy label lacking the series.
Explicit series/volume conflicts fail. A scent-only mismatch is disclosed but
does not change the series. Do not classify using marketing descriptions or
today's category alone. Fullsize inactive legacy fragrances have no rows in the
reviewed source. Pure-set composition comes from committed settings.

## Customer metrics

- Customer = trimmed, lowercased email; no identity appears in public outputs.
- Purchase = distinct realized order containing a paid eligible item. Multiple
  bottles in one order do not count as a return. Free items retain their cost in
  financial totals but create no purchase event.
- First-purchase cohorts are mutually exclusive Natural, Premium or both. The
  first purchase means first qualifying bottle purchase, not first shop order.
- Any repeat = any later qualifying order. Same-series repeat = at least one
  later order containing the original series, including a mixed order.
- First-to-next and first-to-last matrices distinguish loyalty, switching and
  mixed choices. Last means last observed, not proven permanent switching.
  Natural → Natural → Premium counts as a Natural repeat and as a final Premium
  destination; these metrics answer different questions.
- The all-buyer repeat rate also includes customers acquired from the other
  series. Its series denominators can overlap; cohort denominators cannot.
- The principal fair comparison observes **each series from its own first
  purchase**, including acquired switchers and mixed-entry buyers. It restricts
  that original series entry to the common period and requires full 60/90/180-day
  maturity. It does not reset an earlier entry. This is distinct from the
  mutually exclusive first-bottle-cohort comparison, which is also displayed.
- 60/90/180-day metrics include only fully observed windows. Common-launch
  comparisons restrict original entry dates to the first observed Premium sale
  (2025-11-10), without resetting existing customer history. This is an observed
  sale boundary, not a verified launch date. Size-specific 90-day views retain
  customers whose first order had exclusively 200 ml or exclusively 500 ml.
- Wilson 95% intervals describe sampling uncertainty. These are observational
  purchase metrics, not causal proof of satisfaction, product quality or pricing.
  Calendar mix, channels, geography, price and stock availability remain possible
  confounders. Finite follow-up is shown explicitly.

## Financial interpretation

Use the exported EUR item totals after order discounts, never today's catalogue
selling prices. Gross contribution here is item net revenue minus the export's
item purchase costs. It excludes advertising, packaging, shipping, shared fixed
costs and income tax; it is not accounting net profit. Mapped costs are the
reporting cost model, not proof of historic supplier invoices.

The realized export excludes cancelled/credited orders under existing reporting
policy. This is not a separate product-refund-rate analysis. Order-credit fields
repeat on item rows and may never be summed per item. This implementation stops
on nonzero unallocated credits. The 2026-10-18 snapshot has none; the source audit
records the credited orders as financially excluded. No duplicate subtraction
is applied. Selected rows have no margin-fallback or missing-cost estimate.

## Reproduce without customer data in Git

Use the source generation from the private reporting archive:

`s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/vevo/20261018T232902Z/export_20250503-20261018.csv`

SHA-256: `08bc1a79b146250090e94c1407d03bf0f6d7a7ddf51b0ddbdad19f5e756455f1`.

Retrieve it with an authorized AWS profile into ignored `data/`. Keep raw CSV,
source provenance, financial figures and generated reports private; this GitHub
repository is public. The immutable S3 export plus committed code is the
cross-PC recovery mechanism. Do not upload customer rows to Git or a public Page.

```sh
python scripts/analyze_vevo_laundry_series.py \
  --input data/vevo-natural-premium-20261019/export_20250503-20261018.csv \
  --output data/vevo-natural-premium-20261019/reports \
  --from-date 2025-05-03 --to-date 2026-10-18 \
  --source-uri s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/vevo/20261018T232902Z/export_20250503-20261018.csv
python -m unittest tests.test_vevo_laundry_catalog tests.test_vevo_laundry_series_analysis
```

The script writes aggregate JSON and separate Natural/Premium/comparison HTML.
It makes no network request, has no deployment side effect and starts no server.
Source completeness and the generation manifest must be verified before a new
snapshot is admitted. Duplicate lines, identity conflicts, timestamp ties,
negative selected amounts and unallocated credits stop the computation.

## Verification record

The source was fetched twice identically from its immutable generation with
expected AWS owner, AES256 and a stable latest pointer before/after. Dashboard
manifest hash, data-quality checks, financial eligibility and credit semantics
were checked. Catalogue selection includes exactly 4,491 item rows, including
56 pure-set rows and one free item. Two historical identity notes are disclosed,
with no unresolved series/volume conflicts. Focused synthetic tests and a
separate source-to-aggregate calculation verify the metrics; private evidence
lives beside the generated reports and can be reproduced from the source above.
