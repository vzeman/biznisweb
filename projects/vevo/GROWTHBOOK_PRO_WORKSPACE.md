# VEVO GrowthBook Pro workspace contract

Status: GrowthBook Preview data source connected; synthetic curated-fact verification pending

Last reviewed: 2026-08-21

This is the operator handoff for the target GrowthBook Pro rollout. The authenticated workspace currently reports the `Starter` plan; no paid upgrade was accepted. The machine-readable source of truth is `growthbook_workspace.json`; the SQL pasted into GrowthBook must come from `growthbook_sql/` without manual edits.

## Creation order

1. **Completed:** authenticated organization is `Vevo`; the default project was safely reused and renamed `VEVO SK Web` (`vevo-sk-web`). The current UI does not expose a workspace region, so the manifest records it as unknown instead of claiming EU residency.
2. **Completed:** use the Starter default `staging` environment as the Preview alias and keep the default `production` environment. A custom environment named `Preview` is a paid feature in the observed UI. Production allocation stays `0%`.
3. **Completed:** Web SDK connection `VEVO SK Web Preview` exists only for `staging`, targets JavaScript SDK `1.7.0`, and includes draft experiment rules. Its client key was created but is not committed; the connection remains `Not connected` until the reviewed Preview tag is installed.
4. **Completed:** string features `vevo-sk-aa-assignment` and `vevo-sk-product-cta-color` exist with default `control`, enabled for staging and disabled for production. The A/A and CTA experiments are unstarted drafts with 100% experiment traffic, a 50/50 split, exact string feature values, and rules limited to staging. Nothing was published.
5. **Completed:** the dedicated non-root ECS/Fargate collector passed the direct localhost host gate, the public route was added through a route-only change set, and run `32400301619` independently verified the active task/digest, exact CORS and route isolation, plus an unchanged raw-S3 snapshot after invalid probes.
6. **Completed:** run `32401658468` repeated the exact Fargate localhost gate and created `vevo-growthbook-preview-reader` with only the stack's `vevo-growthbook-readonly-preview` policy and one active access key. The key was delivered only as a one-day CMS-encrypted artifact, decrypted locally, and the cloud artifact was deleted immediately. No credential material is committed or logged.
7. **Completed:** `VEVO Preview Experiment Facts` (`ds_19g6mmt2c4dmn`) is connected to the exact Preview region, workgroup, catalog, database, and S3 results URL and is scoped only to project `VEVO SK Web`. GrowthBook's connection test passed, and the temporary local credential/private-key handoff was deleted immediately afterward.
8. **Completed and verified:** the data source has exactly one `device_id` identifier and one `VEVO consented devices` assignment query copied from `growthbook_sql/assignment.sql`. Recovery run `32442114254` proved the single expected synthetic assignment row through the exact read-only Athena workgroup, and the same query then passed directly in the GrowthBook UI.
9. **Current step:** create and verify the two version-controlled Preview fact tables, then create their pre-registered metrics. The protected synthetic verification and the GrowthBook assignment-query readback have already passed with one exact anonymous row; Production allocation remains `0%`.
10. Create the two fact tables from `growthbook_sql/device_outcomes.sql` and `growthbook_sql/performance_vitals.sql`.
11. Create the eleven metrics exactly as listed in `growthbook_workspace.json`. Outcome metrics use GrowthBook window `None` because the authoritative 24-hour and seven-day windows are already frozen upstream. Performance p75 metrics use an event-level quantile of `0.75`, do not group by experiment user, do not ignore zeros, and use a 24-hour conversion window.
12. **Completed as drafts only:** invisible A/A `vevo-sk-aa-001` and CTA A/B `vevo-sk-product-cta-color-001` exist. Neither experiment is started. The A/B experiment remains unstarted until A/A passes and its final sample is recomputed and frozen.
13. Before A/A Production traffic, clone the verified Preview data-source/fact/metric objects onto the separate Production Athena database and workgroup. Never repoint the Preview connection in place.

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
- Curated fact tables are structurally deployed and contain one anonymous zero-value synthetic A/A device fact; real storefront traffic is still disabled.
- The GrowthBook data source and assignment query are verified, but the two version-controlled GrowthBook fact-table definitions and their metrics are not yet created in the workspace.
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
