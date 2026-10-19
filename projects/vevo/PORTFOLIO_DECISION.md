# Natural/Premium portfolio decision analysis

Date: 2026-10-19
Repo: vzeman/biznisweb
Branch: codex/vevo-natural-premium-analysis-20261019

This offline follow-up assesses keeping either series, the uncertainty in
customer satisfaction, and the economics of removing Premium. It does not
change products, prices, inventory, reporting production or advertising.
Use the immutable source and scope documented in `LAUNDRY_SERIES_ANALYSIS.md`.
The source and all numerical business outputs stay in ignored `data/` because
this repository is public. The committed scripts and private immutable source
are the recovery mechanism on another computer.

## Interpretation and decisions

- Purchase behavior does not establish satisfaction or causal superiority.
  Lower same-series retention can also reflect price, availability, consumption
  or selection. An any-series return after Premium may be a purchase of Natural.
- Preserve the original first entry into each series. Compare mature windows
  within the common observed sale period and keep overlapping memberships.
  Split prior Natural buyers from first-ever Premium bottle buyers. Otherwise
  existing Natural loyalty can be mistaken for a benefit of Premium.
- Check first-purchase bottle quantity, 200/500 ml, and the three shared scents.
  Other goods in the basket remain allowed. The strict shared-scent sensitivity
  requires one standalone eligible bottle in the whole entry order; future
  purchases may be any scent of the entered series.
- The first purchase at which both series have ever been tried starts a separate
  post-both clock. Only later orders count as subsequent choice. Non-returners
  are not automatically dissatisfied, and last observed choice is not permanent.
- Retain Natural as the primary range on the observed evidence. Higher Premium
  satisfaction is not demonstrated. Full Premium delisting is not established
  as profit improving. Review size-specific unit economics, especially 500 ml,
  and test incremental demand before making an irreversible catalogue change.
- Reporting assigns an unusually low purchase cost to Premium Ylang 200 ml,
  identical to the configured Natural counterpart. Identity mapping is correct;
  supplier-invoice verification is unavailable. Treat the size-level economic
  interpretation as conditional on confirming the cost, not as a detected bug.
- No randomized merchandising experiment was launched. A recommendation test
  measures recommendation effects, not the complete removal of Premium.

## Financial scenarios

Financial scope is standalone 200/500 ml bottles plus perfume-only sets. Mixed
sets count for retention, but joint prices/COGS are not assigned to perfume.
Contribution means discounted net EUR sales less reporting COGS, before ads,
fulfillment and overhead. It is not accounting net profit.

Four reporting periods are retained: complete source, 2026 to observation end,
and the final 180 and 90 calendar dates, inclusive. Recent periods keep the
comparison relevant without mistaking unequal series histories for demand.

For each removed series, keep the other series' observed business unchanged and
apply these distinct, explicitly hypothetical replacement models:

1. Same spending: transferred net spending earns the target series' observed
   contribution rate in that period.
2. Same quantity by size: transferred bottle counts use the target series' paid
   standalone unit revenue and contribution for the same 200/500 ml size.
3. Same size and scent: use matching standalone scents and sizes, preserving the
   removed series' mix. Missing comparators are unavailable, never zero demand.

`delta = transferred fraction * contribution at full transfer - removed
contribution + genuinely avoidable operating costs - lost ancillary contribution`

`break-even fraction = (removed contribution - avoidable savings + ancillary
loss) / contribution at full transfer`

Generated base scenarios assume zero savings and zero ancillary loss. Fractions
mean spending or bottle quantity according to the model, **not customer counts**.
Actual dosage, visit conversion, order frequency, advertising, remaining stock,
write-offs, inventory financing and demand elasticity are not known. Do not
allocate common fixed costs as fictitious savings. Same-bottle replacement does
not mean equal wash counts or equal usage duration.

Premium 200/500 ml also have separate exact-scent replacement sensitivities.
These hold the other size unchanged; substitution between Premium sizes is not
modeled. Price/cost alternatives compare absolute contribution per bottle, not
percentage margin, and explicitly assume unchanged volume.

Other exported lines on Premium orders are quantified only as associated basket
context. They may include samples and modeled costs, are outside the focal
perfume totals, and must never be presented as losses caused by removing Premium.
Premium-only customers mean no qualifying Natural bottle in the complete source,
not no other shop purchases or unwillingness to substitute.

## Public catalogue observations

Official product/category pages were read on 2026-10-19:

- https://www.vevo.sk/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute
- https://www.vevo.sk/p-1576/parfum-do-prania-vevo-no07-ylang-absolute
- https://www.vevo.sk/p-1532/parfum-do-prania-vevo-no-08-cotton-dream
- https://www.vevo.sk/p-1577/parfum-do-prania-vevo-no08-cotton-dream
- https://www.vevo.sk/p-1533/parfum-do-prania-vevo-no-09-pure-garden
- https://www.vevo.sk/p-1578/parfum-do-prania-vevo-no09-pure-garden
- https://www.vevo.sk/c/vevo-fragrance/parfum-do-prania/vevo-natural
- https://www.vevo.sk/c/vevo-fragrance/parfum-do-prania/vevo-premium

Both ranges currently specify 5–20 ml per approximately 5 kg wash. The Premium
concentration wording gives no numeric consumption ratio. Premium Ylang 500 ml
was out of stock at the observation; its 200 ml variant and the other compared
variants were available. This is not evidence of historical stockouts. Natural
has additional scents absent from Premium. Reverify public observations before
using them for a later decision; historical sales always use export prices.

## Reproduction and checks

From the repository root, with the private input recovered as documented:

```sh
python scripts/analyze_vevo_portfolio_economics.py \
  --input data/vevo-natural-premium-20261019/export_20250503-20261018.csv \
  --output data/vevo-natural-premium-20261019/portfolio --to-date 2026-10-18
python scripts/analyze_vevo_portfolio_retention.py \
  --input data/vevo-natural-premium-20261019/export_20250503-20261018.csv \
  --output data/vevo-natural-premium-20261019/portfolio/retention.json \
  --end 2026-10-18
python scripts/render_vevo_portfolio_decision.py \
  --economics data/vevo-natural-premium-20261019/portfolio/portfolio-economics.json \
  --retention data/vevo-natural-premium-20261019/portfolio/retention.json \
  --output data/vevo-natural-premium-20261019/portfolio/vevo-rozhodnutie.html
python -m unittest tests.test_vevo_laundry_catalog \
  tests.test_vevo_laundry_series_analysis tests.test_vevo_portfolio_retention \
  tests.test_vevo_portfolio_economics
```

The decision renderer rejects mismatched source hashes and observation ends.
The retention builder reconciles its independently reconstructed events to the
original strict analysis. Its deterministic customer-cluster bootstrap preserves
overlapping memberships, but controls sampling uncertainty only. It cannot
remove confounding or prove causality. No customer identity is emitted.

The private independent audit reconstructs amounts, scenario arithmetic, cohort
counts, size/scent sensitivities and post-both choices from the source, without
importing either new analysis function. HTML is checked for valid content and
source-identity leakage; desktop preview is separate from browser visual QA.

Next exact step: review PR #638 and the private decision memo. Confirm actual
supplier costs and inventory economics before any product or price change.
Choose a measurable experiment only when the user requests implementation.
No local service is needed or left running.
