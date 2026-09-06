# Scaling decision methodology review

Date: 2026-09-06
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-scaling-analysis-20260906`
Reviewed source: `b9ac75cd` (`origin/main` at the start of this review).

This is a read-only source review, not a changed budget policy, deployed fix, validation of an MMM fit, or reproduction of a generated report. This repository is public: private report exports, customer data, financial period totals, credentials and signed report URLs must not be added to this review.

## Decision principle

CM1 is realized product revenue excluding VAT, less product costs, packaging and net shipping. CM2 subtracts advertising. CM3 subtracts fixed overhead. A decision to increase advertising should assess the incremental CM2 over an explicit payback horizon, less any actual increase in fixed capacity costs. Positive product margin alone does not establish profitable acquisition.

For equal-duration alternatives with identical fixed costs, subtracting those costs does not change their ordering by absolute profit. The change in CM3 equals the change in CM2. A requirement that an alternative's total CM3 be positive is a separate solvency/payback policy, not the same objective as maximizing incremental contribution.

When incremental contribution margin is `m`, the same-horizon revenue break-even for additional advertising is `incremental revenue / incremental advertising = 1 / m`. A blended historical shop revenue/ad-spend ratio is not a causal incremental ROAS. A scenario holding advertising constant must be distinguished from one scaling advertising with revenue. Both are conditional scenarios, not forecasts.

## Verified source behavior

- `export_orders.py:4970`: revenue uses `item_total_without_tax`; explicit net line totals and fallback VAT handling are at `6379` onward. CM1/CM2 and fixed-cost profit are assembled at `7328` onward.
- `projects/vevo/settings.json:63`: fulfillment allowances and daily overhead are configured management assumptions; `fixed_cost_reporting.actuals_configured` is false. They do not certify actual accounting profit or unchanged future capacity costs.
- The general Marketing decision (`dashboard_modern.py:2922`, `3033`) consumes `ads_effectiveness.incrementality.primary`. Selection at `export_orders.py:16446` prefers historical all-advertising on/off comparisons matched by weekday. Its verdict at `15426` uses contribution/profit differences. Constant fixed overhead cancels in those differences.
- The detailed Meta analysis uses adjacent recent windows and historical spend bands. Consequently a general historical advertising-positive signal and a recommendation to reduce current Meta spend can have different comparison baselines. The UI should explain those baselines instead of presenting either result as an unconditional scaling instruction.
- Spend-band eligibility at `export_orders.py:9327` requires a positive smoothed lower bound of CM3 (`9332`), in addition to observation/weekday coverage. This can leave no recommended corridor even when a band improves CM2 while the whole business remains below fixed-cost break-even. It is a conservative absolute-profit constraint, not an arithmetic subtraction error.
- The adjacent-band test ceiling at `9345` requires at least 85% of the best CM3 lower bound. This relative rule is also sensitive to a common fixed-cost subtraction even though absolute ranking is unchanged. Synthetic illustration: best/adjacent CM2 lower bounds of 100/90 pass an 85% rule; subtracting the same fixed cost of 50 yields 50/40 and the adjacent band fails.
- `9484` onward selects only the latest 7-day verdict for `account_action`, whereas the scale explanation and guardrail describe confirmation by both 7- and 14-day windows (`9508`, `9523`, `9581`). This is a verified implementation/explanation mismatch; no repair is part of this analysis.
- Positive 90-day adjusted contribution with non-positive immediate profit change still produces HOLD (`9222` onward). That is a separate conservative payback policy; constant fixed costs do not explain the immediate difference between equal windows.
- The corridor and its lower-bound estimates use observational historical groups. Matching weekdays and smoothing spend bands do not independently establish causal effects, remove all promotion/product-mix/seasonal confounding, or prove an optimal current budget.

## Customer economics boundaries

Order contribution (`4320` onward) and first/repeat-order economics (`11300` onward) already exclude advertising and fixed overhead. Mature customer contribution (`9063` onward) includes all eligible orders from the first purchase through the defined horizon, with a mature-customer denominator.

Sample-entry groups (`9410` onward) describe whole customer baskets and later purchases, not the isolated margin of a sample SKU. Customers can belong to more than one entry-product group. Grouping by Meta spend on the first-order date (`9367`; broader source proxy at `4474`) is not campaign attribution. Entry-product recommendations use an account-level marginal-CAC proxy, not measured product-specific incremental CAC.

Historical mature contribution can inform an acquisition-test ceiling only with compatible cost coverage, comparable customers, an explicit payback horizon and a contribution reserve for overhead/profit. It does not demonstrate that changing the promoted product will reproduce a cohort difference.

## Evidence and next exact step

Verified: independent source review of the formulas and decision paths above; synthetic algebra for fixed-cost invariance and the relative-threshold exception. No application code, configuration, advertisement, infrastructure or runtime was changed. No local service was started.

Pending: inspect the exact generated HTML report and its period/source metadata, reconcile displayed CM1/CM2/CM3 and both recommendation baselines, and distinguish those observations from the separate Robyn/MMM/TimesFM analyses whose fit diagnostics and outputs were not provided. The reported financial-period comparison is not independently reproduced by this source review. Direct browser navigation to the documented report route was blocked by the client; no local AWS credentials were available. The user indicated an HTML attachment would be supplied, but only the text reply had arrived at this checkpoint. No new export, credential change or infrastructure workaround was attempted.

If a follow-up implementation is requested, separately design and test the decision contract for incremental contribution, whole-business coverage, cash/payback constraints and 7/14-day confirmation. Do not remove financial guardrails or deploy a recommendation change as part of this read-only review.

Attachment follow-up: the user subsequently supplied a Windows path for the report ending 2026-09-05. Direct filesystem checks could not find that file. The available drive-root reports end earlier, and no matching report was found in the attachment or default download folders. Do not substitute an older export for the requested period; the exact generated-report verification remains pending receipt of an accessible file.

External methodological reference: [Meta Robyn analyst guide](https://facebookexperimental.github.io/Robyn/docs/analysts-guide-to-MMM/) describes incremental-effect questions, data quality and the need to choose a measurement method that fits the business question. It does not validate this installation's models.

## Year-to-date reconstruction

The follow-up review reads the available static HTML ending 2026-08-28, generated at the timestamp printed in that report. It contains 240 contiguous daily observations for 2026-01-01 through 2026-08-28. The report's application/json block is data; no embedded executable JavaScript is needed or executed.

Use `series` for daily revenue, product costs, packaging, shipping, channel advertising, fixed cost, orders and reported CM2/CM3. Recompute CM1/CM2/CM3 independently. Join `customer_mix` by date for first/repeat-order revenue and reconcile both segments to total revenue. Use `cohort_unit_economics_rows.cohort_month` and `new_customers` for monthly acquisition counts, according to the report's known-first-purchase/history fallback definition. Do not label this channel attribution or infer unavailable monthly segment contribution.

Aggregate by calendar month, retain day counts and flag the last partial month. Normalize per-day values when comparing different-duration months. The supplied comparison's earlier window is July 8 through August 6; its later window is August 7 through September 5. Only the earlier window is fully covered by this snapshot and independently reconciled. Never stitch the partial later window to rounded figures from another export to create an allegedly complete daily series.

Within-day-spend comparisons, monthly revenue variability, acquisition counts and repeat-order revenue composition are descriptive evidence. They do not by themselves identify advertising incrementality, rule out promotions/seasonality, or establish whether an old customer was originally acquired by paid advertising. Advertising carryover and later repeat purchases can move contribution into a different accounting month from acquisition spending; the [Robyn feature documentation](https://facebookexperimental.github.io/Robyn/docs/features/) describes lagged adstock effects as well as saturation. Without a model's actual inputs, fit and validation outputs, this review neither validates nor rejects that fitted model.

Private reconstructed financial totals belong in the generated workbook, not this public source review. The workbook must state its source filename, cutoff, generation timestamp, financial definitions and unverified tail. No new export, infrastructure mutation or advertising action is required to read the available report.

The reusable builder is `scripts/build_vevo_year_comparison.mjs`. Use the Node executable and `node_modules` directory supplied by Codex desktop's workspace dependency loader; the authoring dependency is `@oai/artifact-tool`, not an application/runtime dependency. Example invocation with caller-supplied paths:

```text
node scripts/build_vevo_year_comparison.mjs --input <report.html> --output <private-output.xlsx> --year 2026 --requested-through 2026-09-05 --dependencies <bundled-node_modules> --qa-dir <private-qa-directory>
```

The builder validates consecutive source dates, daily array lengths, numeric values, monthly cohort coverage, daily profit/customer-revenue reconciliations and independently aggregated workbook totals. Monthly calculated amounts remain formulas referencing the daily input sheet. The customer chart is formula-linked, and the last partial month plus unobserved tail remain explicit. The delivered workbook's formula scan and visual review of every sheet passed. Generated workbook/QA files contain private aggregates and must not be committed to this public repository.

## Follow-up claim verification

The source's `cohort_summary.avg_days_to_2nd_order` and `median_days_to_2nd_order` describe observed second-order customers; they are not an unconditional promise that every new customer returns. `sample_funnel.summary.median_days_to_repeat` and `median_days_to_fullsize` describe a different sample-entry subset and are medians, not means. Source at `export_orders.py:8424` selects first observed baskets containing a sample and no full-size product, while `8548` and `8549` take medians over non-null conversion times. All these displayed aggregates describe the available source history; do not relabel them as the current acquisition cohort's expected timing.

The sample funnel's window conversion denominators include the whole entry cohort, without a window-age eligibility filter. Its percentages therefore cannot establish mature or eventual return probability; use explicitly mature cohorts for such an inference.

Repeat-revenue dependence in a low-spend period is directly observable. Attributing a later repeat-revenue shortfall specifically to earlier reduced acquisition remains a hypothesis until customer cohorts and competing explanations are checked. Positive estimated fixed-cost coverage in a low-spend month also prevents using that month as evidence that low spend necessarily fails to cover fixed costs. Holding spend steady can still acquire customers. Neither a guaranteed failure without a budget increase nor an inevitable short-term loss after an increase follows from these observations. Longer follow-up is justified by observed return timing, but later repeat purchases do not themselves guarantee acquisition payback.
