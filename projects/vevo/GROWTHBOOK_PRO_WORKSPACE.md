# VEVO GrowthBook Pro workspace contract

Status: GrowthBook Preview objects initialized as safe drafts; no AWS/runtime/shop object created

Last reviewed: 2026-08-20

This is the operator handoff for the target GrowthBook Pro rollout. The authenticated workspace currently reports the `Starter` plan; no paid upgrade was accepted. The machine-readable source of truth is `growthbook_workspace.json`; the SQL pasted into GrowthBook must come from `growthbook_sql/` without manual edits.

## Creation order

1. **Completed:** authenticated organization is `Vevo`; the default project was safely reused and renamed `VEVO SK Web` (`vevo-sk-web`). The current UI does not expose a workspace region, so the manifest records it as unknown instead of claiming EU residency.
2. **Completed:** use the Starter default `staging` environment as the Preview alias and keep the default `production` environment. A custom environment named `Preview` is a paid feature in the observed UI. Production allocation stays `0%`.
3. **Completed:** Web SDK connection `VEVO SK Web Preview` exists only for `staging`, targets JavaScript SDK `1.7.0`, and includes draft experiment rules. Its client key was created but is not committed; the connection remains `Not connected` until the reviewed Preview tag is installed.
4. **Completed:** string features `vevo-sk-aa-assignment` and `vevo-sk-product-cta-color` exist with default `control`, enabled for staging and disabled for production. The A/A and CTA experiments are unstarted drafts with 100% experiment traffic, a 50/50 split, exact string feature values, and rules limited to staging. Nothing was published.
5. Only after the AWS runtime hard gate is resolved and the Preview stack passes its host/marker checks, create the custom Athena data source `VEVO Preview Experiment Facts` with identifier type `device_id` and the exact connection fields from `growthbook_workspace.json`.
6. Paste `growthbook_sql/assignment.sql` as the assignment query. Its preview must return exactly `device_id`, `timestamp`, `experiment_id`, `variation_id`, and the four approved Meta dimensions.
7. Create the two fact tables from `growthbook_sql/device_outcomes.sql` and `growthbook_sql/performance_vitals.sql`.
8. Create the eleven metrics exactly as listed in `growthbook_workspace.json`. Outcome metrics use GrowthBook window `None` because the authoritative 24-hour and seven-day windows are already frozen upstream. Performance p75 metrics use an event-level quantile of `0.75`, do not group by experiment user, do not ignore zeros, and use a 24-hour conversion window.
9. **Completed as drafts only:** invisible A/A `vevo-sk-aa-001` and CTA A/B `vevo-sk-product-cta-color-001` exist. Neither experiment is started. The A/B experiment remains unstarted until A/A passes and its final sample is recomputed and frozen.
10. Before A/A Production traffic, clone the verified Preview data-source/fact/metric objects onto the separate Production Athena database and workgroup. Never repoint the Preview connection in place.

## Exact analytical behavior

- Assignment and conversion identity is the anonymous random `device_id`; no identifier join table exists.
- GrowthBook queries curated anonymous facts only. It cannot query raw events, orders, customers, invoices, or unrelated reporting data.
- The assignment table intentionally contains one eligible first-exposure row per experiment/device. Cross-variation contamination is detected from retained raw exposures by the reporting builder and contaminated devices are excluded before the table reaches GrowthBook.
- Non-buyers remain present with zero revenue and zero CM1, so GrowthBook Mean metrics correctly divide by every eligible exposed device.
- Purchase, revenue, and CM1 are computed by the authoritative BiznisWeb/reporting join within seven days. Browser money is never accepted.
- Cancellation and refund ratios cannot be used for a final decision until the 14-day maturity gate passes.
- Meta campaign, ad-set, ad, and placement IDs are analysis dimensions only; GrowthBook, not Meta, owns the 50/50 randomization.
- A/A results never produce a winner. They validate assignment, SRM, joins, privacy, performance, and agreement with reporting.

## Current blockers

- The authenticated GrowthBook workspace is on Starter. The observed upgrade flow requires a paid Pro upgrade; it was cancelled without accepting a charge.
- `staging` is the Preview alias until a paid plan is explicitly authorized and a custom `Preview` environment is genuinely needed.
- No AWS credential or Athena identity exists.
- The proposed Lambda/API Gateway collector remains deployment-blocked because it cannot satisfy the repository's mandatory instance/IP/service/path plus `curl localhost` hard gate.
- Production feature allocation and the collector Production registry remain hard-disabled.

The GrowthBook Web SDK returns the actual string feature value in `ExperimentResult.value`; its `key` is a variation tracking key that defaults to a numeric string such as `"0"` or `"1"`. The storefront therefore validates and stores `result.value` (`control`, `variant`, or `brand_contrast`) and uses numeric metadata only as a fail-closed fallback. See the official [GrowthBook JavaScript SDK source](https://github.com/growthbook/growthbook/blob/main/packages/sdk-js/src/types/growthbook.ts) and [feature implementation guide](https://docs.growthbook.io/lib/js#tracking-callback).

Validate this contract before any UI entry:

```text
python scripts/validate_growthbook_workspace.py
python -m unittest tests.test_growthbook_workspace
```

Official implementation references:

- <https://docs.growthbook.io/app/datasources>
- <https://docs.growthbook.io/app/metrics>
- <https://docs.growthbook.io/app/sql-templates>
- <https://docs.growthbook.io/warehouses/athena>
