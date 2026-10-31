# Unit production fee scenario

This is a read-only management analysis, separate from the production reporting
formulas. Its inputs and results can contain private commercial information;
never commit them to this public repository.

## Reproduce

1. Read the selected project's `daily-reports/<project>/latest/generation.json`
   from the private reporting bucket with owner-bound AWS reads.
2. Download the manifest's 30-day dashboard and the same generation's
   `aggregate_by_date_product_*.csv` and `aggregate_by_month_*.csv`.
   Preserve the generation manifest and downloaded byte hashes.
3. Run `scripts/analyze_unit_production_fee.py` with the snapshot directory,
   `--settings`, `--monthly-avoided-cost`, `--per-unit-fee`, and `--history-from`.
   Supply `--cost-in-fixed` only after confirming that the avoided cost is
   already included in the report's fixed overhead. Save JSON outside Git.
4. Independently reconcile goods quantity against the aggregate CSV and
   report QA coverage; retain the input and output hashes privately.

## Definitions and boundaries

- `dashboard.series.items` counts product rows, not sold quantities. Use QA
  `product_expense_coverage.total_units`, which sums `item_quantity`, or the
  aggregate CSV's `total_quantity`.
- Exclude `zero_cost_service_override` quantities and revenue when identifying
  goods. For CSV history require both the reviewed service SKU and exact label.
- A sold set counts once. Do not expand its physical components unless the
  fee contract explicitly charges each component. Do not assume every product
  is internally manufactured: this scenario charges every sold merchandise unit.
- Preserve product cost, packaging and net shipping. The proposed fee is added
  to those costs, while the confirmed avoidable fixed cost is removed once.
- Cost crossover quantity = avoidable monthly cost / unit fee. Its goods
  turnover = crossover quantity * weighted net goods revenue per unit.
- Fixed-ad-budget company break-even = (remaining fixed cost + current ad spend)
  / (1 - (product cost + packaging + net shipping + proposed fee) / revenue).
- Proportional-ad-spend break-even additionally subtracts current ads/revenue
  from that denominator. This requires ad spend to fall with revenue at the
  same efficiency and must not be presented as an unconditional threshold.
- Reporting overhead may be an estimate, not accounting actuals. Observe QA
  warnings, mapped/fallback product costs and absent payment fees.
- An incomplete month is not a closed monthly result. Comparisons against one
  full month's avoided cost are labelled accordingly. Daily run-rate projections
  are forecasts, and overhead must use the same day count as the scenario.
- A separately generated 30-day payload can differ from full-history CSV by
  cents due to ad rounding. Keep one consistent source for the primary scenario.

No campaigns, staffing, prices, production runtime, or accounting data are
changed by this utility.
