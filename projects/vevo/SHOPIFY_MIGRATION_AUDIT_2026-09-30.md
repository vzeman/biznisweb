# VEVO: Shopify migration feasibility audit

Date: 2026-09-30 (Europe/Bratislava)
Status: Read-only discovery in progress; no migration or production changes authorized by this document.
Source baseline: `vzeman/biznisweb` main `c98d350ea0dbcdfdcb3dfddd689344c78b8b148b`.
Working branch: `codex/vevo-shopify-audit-20260930`.

## Objective and evidence boundary

Assess whether a new Shopify storefront can preserve VEVO's daily operations, including reporting, manufacturing, expedition in Chameleoon and Stripe payments. Separate inspected code, direct admin observations, vendor documentation and unverified runtime assumptions. Never include customer personal data, credentials or payment-session links in this document.

The existing Playground checkout has an unrelated branch and many untracked files/nested projects. It was left untouched. A managed isolated worktree was created; remote/repository/clean status were checked, `git fetch --all --prune` and `git pull --rebase` completed before documentation changes. No local service was started and no AWS action occurred.

## Direct live observations

- Authenticated `vevo.flox.sk/erp/main/orders` is accessible. The first order page reported 9,405 records at inspection; this is an admin snapshot count, not a reconciled accounting total.
- Current SK/EUR, CZ/CZK and HU/HUF orders coexist in one list, including COD, paid online, unpaid new, cancelled and shipped orders. Invoice references are present.
- Settings list 24 order statuses. Stock side effects include reserve for waiting/paid, release for cancelled/expired, deduct for shipped, and add back for returned/uncollected-cancelled. Stripe and older GoPay-labelled states coexist; labels alone do not prove active gateways.
- Settings list 15 payment definitions. Examples: card IDs 18/19/20, historical card definitions, COD IDs 7/10/16, bank transfer. Current COD labels show 1.90 EUR / 40 CZK / 500 HUF. Definition presence does not prove storefront availability.
- Settings list 38 delivery definitions: paid/free variants and historical/locale duplicates. Carriers include SPS Balikovo, Slovenska posta, Packeta, DPD, FoxPost, Express One and Magyar Posta; also personal pickup and same-day BB delivery.
- Settings list 12 locale definitions including SK, CZ, HU, EU, DE, BG and legacy Madfrog aliases. API root-page evidence shows populated SK/CZ/HU/EU/DE/BG and empty or legacy branches. Configured locale is not equivalent to active selling market.
- Chameleoon official login page is reachable but this Chrome session is signed out. Account-specific mappings, subscription and printer workflow remain unverified.

## Initial feasibility findings

- Chameleoon documents a Shopify connector with payment/COD classification, carrier mapping, tracking writeback and status rules: <https://docs.chameleoon.sk/shopify>.
- Shopify Payments supported-country list currently excludes Slovakia: <https://help.shopify.com/en/manual/payments/shopify-payments/supported-countries>.
- Stripe documents the replacement `Stripe Card Payments` connector. Availability for a new shop under VEVO's actual legal entity is still to be verified: <https://support.stripe.com/questions/update-shopify-payment-provider-from-stripe-to-stripe-card-payments>.
- Shopify states that local-currency checkout requires Shopify Payments or Adyen as primary gateway; another gateway converts checkout to shop base currency. A single Stripe-backed shop therefore cannot yet be promised as a drop-in EUR/CZK/HUF replacement: <https://help.shopify.com/en/manual/international/payments>.
- Slovak Shopify pricing observed: Basic/Grow/Advanced external-gateway surcharge 2%/1%/0.6%, separate from gateway charges: <https://www.shopify.com/sk/pricing>.
- Manufacturing and operations are modules of this reporting repository, not a separately discovered VEVO manufacturing repository. Code review in progress.

## Open work for this audit

1. Finish source-contract review of reporting, operations, manufacturing, invoice handling and GrowthBook.
2. Finish related GitHub project inventory, including unmerged branches where main contains only a scaffold.
3. Inspect remaining live admin catalogue, stock and document rules without saving changes.
4. Record migration options, acceptance scenarios, evidence limits and exact questions for the owner.

No Shopify account, app installation, payment, order, shipment, stock, notification or deployment change has been made.
