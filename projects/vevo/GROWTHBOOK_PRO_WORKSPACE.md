# VEVO GrowthBook Pro workspace contract

Status: Preview data source, assignment, fact tables, eight Starter-compatible outcome metrics, and the unpublished GTM loader/consent runtime are verified; Comet still blocks the GrowthBook feature payload, and three paid-Pro p75 metrics remain blocked

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
9. **Completed:** protected run `32443149425` reused the exact synthetic A/A exposure/device, published one deterministic `lcp_ms=1300` event, reconciled it through the existing reporting runtime, and proved the exact raw, curated, and Athena identities after the Fargate localhost/marker hard gate. Production allocation remained `0%`.
10. **Completed:** `VEVO Device Outcomes v1` (`ftb_19g6mmt2dhrdi`) and `VEVO Performance Vitals v1` (`ftb_19g6mmt2e0otd`) were created from their version-controlled SQL, identified by `device_id`, and query-tested in GrowthBook with exactly one anonymous synthetic row each.
11. **Partially completed and query-verified:** all eight Starter-compatible outcome metrics were created and their exact GrowthBook IDs were read back into `growthbook_workspace.json`. A real GrowthBook metric-analysis run initially exposed the imported `integer = varchar(1)` mismatch on `client_error_observed`; all numeric outcome/quality columns were then corrected to GrowthBook Number, the three binary filters now render numeric `= 1`, and all eight metric analyses passed through the exact Preview Athena connection against one anonymous synthetic device. Outcome metrics use GrowthBook window `None` because the authoritative 24-hour and seven-day windows are already frozen upstream. The three performance p75 metrics remain intentionally uncreated: GrowthBook marks Quantile metrics as Pro, and no paid upgrade was authorized. After an authorized upgrade, create and query-test them with event-level quantile `0.75`, no grouping by experiment user, zeros retained, and a 24-hour conversion window.
12. **Completed as drafts only:** invisible A/A `vevo-sk-aa-001` and CTA A/B `vevo-sk-product-cta-color-001` exist. Neither experiment is started. The A/B experiment remains unstarted until A/A passes and its final sample is recomputed and frozen.
13. **Completed as an unpublished draft only:** isolated GTM workspace `VEVO GrowthBook Preview` (`16`) contains exactly four new Preview tags and one new custom-event trigger. Its overview reads `5` added, `0` modified, and `0` deleted. Tag IDs are loader `44`, consent bridge `46`, add-to-cart bridge `47`, and purchase bridge `48`; custom trigger `45` observes `add_to_cart`. All three bridges were read back with the loader-before sequence and fail-closed behavior. The temporary runtime-populated artifact matched SHA-256 `e4dab7ad37432c255c9552eff953bfb0c80c48035db06b814e6d5a58af29532f`, was pasted without committing its client key, and was deleted after UI read-back. Nothing was submitted or published.
14. **Runtime partially accepted; feature payload still blocked:** after explicit user approval, Google Tag Assistant extension `26.216.2.45` was installed and its user-visible site access was confirmed as `all sites`. Comet's built-in blocker initially hid the GTM container; the user added exact VEVO/VEVO-FLOX exceptions and a fresh reload then connected Tag Assistant, found three Google tags, evaluated Preview container `GTM-5ZB5LFGB`, fired the draft loader and consent bridge, and reported zero console errors. The reject/withdraw control passed with zero GrowthBook SDK DOM nodes and zero GrowthBook/collector assets. Analytical-only consent then loaded the pinned SDK and attempted the exact GrowthBook feature request, but Comet returned `net::ERR_BLOCKED_BY_CLIENT` for `cdn.growthbook.io`. No feature payload, experiment exposure, or collector request was accepted. The next browser-only Preview gate is an exact `https://cdn.growthbook.io` Comet exception followed by a clean analytical-only retry; GTM remains unpublished and both experiments remain unstarted.
15. Before A/A Production traffic, clone the verified Preview data-source/fact/metric objects onto the separate Production Athena database and workgroup. Never repoint the Preview connection in place.

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
- The GrowthBook data source, assignment query, both fact tables, and all eight Starter-compatible outcome metrics are query-verified. The LCP, INP, and CLS p75 Quantile metrics are blocked until a paid Pro upgrade is explicitly authorized; their GrowthBook IDs remain `null` rather than claiming objects that do not exist.
- GTM workspace `VEVO GrowthBook Preview` remains an unpublished draft with five added and no modified/deleted objects. Tag Assistant now attaches, the loader and consent bridge execute without console errors, and the no-Analytical network gate passes. Comet still blocks `cdn.growthbook.io`, so feature evaluation, exposure delivery, assignment stability, and collector delivery remain unverified.
- Production feature allocation and the collector Production registry remain hard-disabled.

## Next exact step

After the user adds the exact `https://cdn.growthbook.io` Comet exception, repeat the analytical-only unpublished Preview. Require an accepted feature payload, one allowlisted exposure delivery, the same A/A assignment after reload, consent-withdrawal cleanup, and zero cart/checkout mutation. Do not click GTM `Submit`, do not start either GrowthBook experiment, and keep Production allocation at `0%`.

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
