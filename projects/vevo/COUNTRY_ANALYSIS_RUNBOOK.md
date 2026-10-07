# VEVO country analysis — reproducible read-only workflow

This repository is public. Financial outputs and customer/order-level exports must stay in ignored `data/` and private artifact storage. Removing customer names does not make business results suitable for public Git. Never add these audit JSON/Markdown outputs to `docs/`.

The October 7, 2026 analysis covers August and September, with July context and October 1–6 separately. It changes no campaigns, orders, report aliases or runtime services. The private archive is in the reporting artifact bucket under `data/vevo/analyses/2026-10-07-country-performance/`; `manifest.json` records exact SHA-256 hashes and source generation.

## Source and definitions

- Frozen report source: `daily-reports/vevo/20261006T231826Z/`, including export CSV, daily aggregates and payload. Download into `data/country-analysis-20261007/` as `orders.csv`, `daily.csv` and `payload.json`.
- Export rows are realized-order items, not all order attempts. Deduplicate orders before counting; sum net merchandise item revenue/cost. Shipping/VAT are excluded and currency uses configured reporting rates.
- `analyze_vevo_country_orders.py` groups by the currency shop proxy, cross-checks invoice/OSS country, reconciles to daily report controls and produces mature cohort comparisons. Customer keys are used only in memory.
- `audit_vevo_country_coverage.py` makes bounded, paced GraphQL queries, binds the current status catalogue and compares current financial classification. Its minimal private facts contain order references, not customer names/addresses/emails. Exact same-shop HTTPS canonical redirection is allowlisted; provider failures stop safely with an optional checkpoint resume. Never run the reporting runner for this audit.
- Verified defect: financial `realized_revenue.cod_payment_ids` lacks HU payment 16 (`Utánvétes fizetés`); operations configuration already includes it. Do not change the production policy as part of analysis. A separate repair must test status/payment/country isolation and historical reprocessing.
- `audit_vevo_meta_performance.py` requests country, campaign and daily Insights, plus current configuration. Credentials remain in memory. `summarize_vevo_meta_performance.py` uses only the exact purchase action and explicit `7d_click`, retaining API `value`/`1d_view` separately. Do not add attribution windows or overlapping actions. Current targeting must match campaign country labels for daily grouping; current settings do not prove historical settings.
- `audit_vevo_google_ads.py` validates the configured account and queries account/campaign/user-location totals, purchase actions and current destinations. Physical country differs from destination shop. Do not sum secondary purchase trackers into unique orders.
- MER is all-shop merchandise revenue / advertising spend, not causal ROAS. Complete HU contribution is intentionally unavailable because omitted orders have no cost rows in the archived export. Do not invent their costs or extrapolate the card-only product mix to all HU orders.

## Reproduce

Use the repository Python environment with existing AWS/Google dependencies for API reads. Use the bundled Python standard library/runtime for offline CSV processing. Never install a local server or start invoice/email runners. Obtain the runtime Secret ARN from the existing task definition; pass the ARN, never a secret value.

```powershell
python scripts/analyze_vevo_country_orders.py --help
python scripts/audit_vevo_country_coverage.py --help
python scripts/audit_vevo_google_ads.py --help
python scripts/audit_vevo_meta_performance.py --help
python scripts/summarize_vevo_meta_performance.py --input data/country-analysis-20261007/meta.json --output data/country-analysis-20261007/aggregates/vevo_meta_performance_20261007.json
python scripts/combine_vevo_country_audit.py
python scripts/render_vevo_country_analysis.py
```

Google output must be named `vevo_google_ads_20261007.json` in the private `aggregates` directory. Pass July 1 through October 6 to its date arguments. The Meta extractor is explicitly scoped to those audit dates. All scripts are finite processes; none listen on a port.

`archive_vevo_country_analysis.py` is an explicit private evidence upload, not a read-only query. It verifies the AWS account and all four S3 public-access blocks, uploads only its fixed file allowlist under the analysis prefix with SSE-S3, then verifies every object/hash. It never updates production report aliases. Current bucket has no bucket policy and has every public-access block enabled. No public or presigned sharing link is created.

## Validation and known limits

Order/daily totals reconcile; live included counts/revenue reconcile with a small documented Decimal/float rounding tolerance. Meta account/country/campaign totals and Google physical-country totals reconcile. Syntax/Ruff and independent review cover the audit scripts and final interpretation. Revenue windows, platform attribution windows and mature cohort ages remain distinct. Provider GraphQL internal errors required bounded same-page/checkpoint recovery; no incomplete aggregate was accepted.

Financial outputs are private; the technical evidence ledger in `PROJECT_STATE.md` identifies the archive and next repair. No Git history rewrite is part of this workflow.
