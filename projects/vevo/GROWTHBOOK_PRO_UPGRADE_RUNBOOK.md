# VEVO GrowthBook Pro upgrade and quantile-metric runbook

Status: fail-closed preparation only. Production A/A is running, the account is
still on Starter, and no payment or Pro metric creation is authorized now.

The executable source of truth is `growthbook_pro_upgrade.json`. This runbook
may be used only after the protected A/A snapshot is exactly `PASS` and the
separate completion read-back proves the A/A stopped at zero Production
allocation while CTA remains a draft at zero allocation.

## Hard stops

Do not continue when any of the following is true:

- the A/A decision is absent, `FAIL`, or `NOT_READY`;
- the A/A experiment or Production rule is still active;
- CTA or another Production experiment is active;
- the authenticated organization/project is not `Vevo` / `VEVO SK Web`;
- the offer differs from one seat at a base price of `$40 USD` per month or
  introduces an unreviewed tax, discount, trial, annual commitment, seat, or
  usage commitment;
- action-time user confirmation of the recurring paid subscription is absent;
- a page requests payment-method, card, bank, tax-ID, invoice-address, password,
  OTP, or other sensitive data to be copied into Git, a browser log, an
  observation, or chat;
- the three Preview and three Production metric IDs are not all new, unique,
  and bound to their correct fact table/data source;
- GTM version `15`, Meta Ads, BiznisWeb, collector/reporting, product content,
  prices, stock, cart, checkout, payments, or orders would change.

## Gate 1 — record the reviewed paid action

Only after the verified A/A completion is merged, obtain action-time user
confirmation for this exact transaction: one GrowthBook Cloud Pro seat,
`$40 USD` base price per month, recurring monthly subscription. Then run the
offline recorder on a new branch:

```text
python scripts/record_growthbook_pro_upgrade.py open-review --authorized-at-utc <UTC-Z> --confirm-paid-upgrade true --confirmed-seat-count 1 --confirmed-base-monthly-price 40 --confirmed-recurring-subscription true
python scripts/validate_growthbook_pro_upgrade.py
python scripts/validate_growthbook_workspace.py
python scripts/security_ci.py
git diff --check
```

Merge that state through a reviewed PR. It opens only
`manual_paid_upgrade_allowed=true`; it cannot operate a browser, buy anything,
or start CTA.

## Gate 2 — manually upgrade and create exact metrics

From the reviewed state, reload GrowthBook Billing and re-read organization,
plan, seats, currency, base price, billing period, and recurring status. Stop on
any drift. Immediately before the final paid confirmation, request the required
browser action-time confirmation again. Do not save or expose payment details.

After Billing reads `Pro`/active, create and query-test each exact Quantile
metric twice: once against Preview `VEVO Performance Vitals v1` and once against
Production `VEVO Performance Vitals v1`.

For LCP, INP, and CLS use the versioned workspace contract exactly:

- quantile `0.75`;
- aggregation column `vital_value`;
- row filter `vital_name = lcp_ms`, `inp_ms`, or `cls_milli`;
- group by experiment user disabled;
- ignore zeros disabled;
- goal `decrease`;
- conversion window `24 hours`.

Add the three Production metric keys to the still-unstarted CTA draft as
guardrails together with `vevo_client_error_device_rate_24h`. Do not edit its
primary metric, six secondary metrics, variations, weights, targeting, traffic,
or feature rules. Do not attach the new metrics to the already completed A/A.

## Gate 3 — canonical read-back and offline recording

Create one canonical observation matching the strict schema enforced by
`record_growthbook_pro_upgrade.py`. It contains plan/seat/base-price status,
the six new non-secret metric IDs, contract hashes, query/read-back booleans,
the unchanged CTA draft and infrastructure boundaries, and no identity,
credential, payment-method, invoice, customer, or order data.

Record it on a new branch:

```text
python scripts/record_growthbook_pro_upgrade.py record --observation <canonical-observation.json> --expected-observation-sha256 <independent-sha256>
python scripts/validate_growthbook_pro_upgrade.py
python scripts/validate_growthbook_workspace.py
python scripts/build_growthbook_cta_baseline_observation.py validate
python scripts/security_ci.py
git diff --check
```

The recorder updates only the Pro transition manifest, canonical observation,
and GrowthBook workspace. CTA remains draft at `0%`; all automatic mutations
and activation gates remain closed. The later baseline, frozen sample, CTA-only
collector, and manual CTA start still require their existing separate reviewed
gates.
