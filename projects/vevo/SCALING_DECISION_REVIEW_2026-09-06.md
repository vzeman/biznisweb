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

External methodological reference: [Meta Robyn analyst guide](https://facebookexperimental.github.io/Robyn/docs/analysts-guide-to-MMM/) describes incremental-effect questions, data quality and the need to choose a measurement method that fits the business question. It does not validate this installation's models.
