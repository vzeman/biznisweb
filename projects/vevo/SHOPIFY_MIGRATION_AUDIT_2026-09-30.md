# VEVO: Shopify migration feasibility audit

Date: 2026-09-30 (Europe/Bratislava)
Status: Feasibility assessment recorded; account inspection stopped after a credential-exposure incident. No migration or production changes authorized by this document.
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
- Chameleoon was initially signed out. The owner signed in during this audit; source/carrier/COD/status settings were subsequently inspected read-only (details below). Printer behavior and actual Shopify connector operation were not tested.
- Catalogue UI lists 236 records under all products and 202 in the SK category scope, including invisible products; these are not counts of active sellable Shopify variants. The category tree includes VEVO Fragrance, Home Care, Beauty, sets and samples, plus other market/legacy scopes.
- Stock general settings visually confirmed: stock enabled, quantity and quick-edit enabled, reserve on order placement, deduct on order confirmation. Quantity display is unchecked. Combined with status actions, this requires an explicit reservation/available/on-hand mapping.
- Document settings show Wachman s.r.o. SK/CZ/HU branches under the same company identity and separate order/variable-symbol, invoice and credit-note counters. Shopify order numbers must not be substituted for legal invoice numbers.

## Chameleoon: account-specific evidence

The authenticated account has one source, Biznisweb / Vevo, and 11 carrier configurations. Twelve populated name mappings (including a duplicate SPS label) were visible:

| FLOX delivery label | Chameleoon configuration |
|---|---|
| DPD - kuriér na adresu; DPD - kurýr na adresu | DPD - DPD |
| SPS Balíkovo - výdajné miesto/box/alzabox | SPS - SPS |
| Slovenská pošta - kuriér na adresu | Slovenská pošta - Na adresu |
| Slovenská pošta - balík na poštu | Slovenská pošta - Pošta |
| Packeta - výdajné miesto/box | Packeta - Packeta SK |
| Packeta - výdejní místo/box | Packeta - Packeta CZ |
| Packeta - kurýr na adresu | Packeta - Packeta CZ na adresu |
| Magyar Posta - a címre | Packeta - Magyar Posta a címre |
| Express One - a címre | Packeta - Express One a címre |
| FoxPost csomagpont | Packeta - FoxPost csomagpont |

COD is explicitly true for `Dobierkou`, `Dobírka` and `Utánvétes fizetés`; online card and bank-transfer labels are false. Historical numeric/unknown labels and duplicate transfer labels also exist as non-COD; their actual use was not established. Do not silently copy unknown-method defaults into a new fulfillment gate.

Automatic order-state change after shipment creation is enabled. Rule `Odoslaná` maps shipment state `Vytvorená` to source order state `Shipped`, with no carrier or starting-state filter selected. This is a creation event, not proof of physical handover or delivery. Preserve the current business trigger intentionally; do not map it to customer delivery.

No configuration was saved, no shipment was created, and no label was printed. Native Shopify integration is documented by Chameleoon but was not installed or tested for this account.

## Vendor constraints verified on 2026-09-30

- Follow-up clarification: one Shopify store can support multiple languages and domains. Current documentation allows up to 20 languages on Basic/Grow/Advanced and up to 30 on Plus/Enterprise. The VEVO limitation is not language support: distinguish translated content, displayed prices, actual checkout/charge currency and merchant payout currency. Source: <https://help.shopify.com/en/manual/international/localization-and-translation>.
- Chameleoon documents a Shopify connector with payment/COD classification, carrier mapping, tracking writeback and status rules: <https://docs.chameleoon.sk/shopify>.
- Shopify Payments supported-country list currently excludes Slovakia: <https://help.shopify.com/en/manual/payments/shopify-payments/supported-countries>.
- Stripe documents the replacement `Stripe Card Payments` connector. Availability for a new shop under VEVO's actual legal entity is still to be verified: <https://support.stripe.com/questions/update-shopify-payment-provider-from-stripe-to-stripe-card-payments>.
- Shopify states that local-currency checkout requires Shopify Payments or Adyen as primary gateway; another gateway converts checkout to shop base currency. A single Stripe-backed shop therefore cannot yet be promised as a drop-in EUR/CZK/HUF replacement: <https://help.shopify.com/en/manual/international/payments>.
- Slovak Shopify pricing observed: Basic/Grow/Advanced external-gateway surcharge 2%/1%/0.6%, separate from gateway charges: <https://www.shopify.com/sk/pricing>.
- Manufacturing and operations are modules of this reporting repository, not a separately discovered VEVO manufacturing repository. Source review completed at the recorded baseline.

- The Stripe connector's documented existence is not an account-specific guarantee. The actual merchant is Wachman s.r.o. according to public contact/admin evidence; confirm eligibility with this real entity and country. Do not use a fictitious country to unlock Shopify Payments.
- At annual billing, the Slovak public pricing page lists Basic 19 EUR/month, Grow 56 EUR/month, Advanced 289 EUR/month; gateway surcharges are additional to Stripe fees. Grow includes five additional staff accounts, Advanced fifteen, Basic none. Apps, connector costs, tax treatment, custom integration maintenance and existing AWS services are extra. This is a dated public price observation, not a binding quote.
- Shopify excludes manual COD/bank-transfer payments from third-party gateway transaction fees: <https://help.shopify.com/en/manual/your-account/manage-billing/billing-charges/types-of-charges/third-party-charges/third-party-transaction-fees>. Do not apply the card surcharge to all order revenue.
- Illustrative arithmetic only: 10,000 EUR/month of fee-eligible gateway volume adds 200/100/60 EUR under Basic/Grow/Advanced, plus subscription and Stripe. Actual volume and fee basis need reconciliation. At the above annual prices, Grow's extra 37 EUR versus Basic is offset by a 1-point fee saving at 3,700 EUR eligible monthly volume; this alone does not select the plan.
- Shopify separates order, financial, fulfillment and return states: <https://help.shopify.com/en/manual/fulfillment/managing-orders/order-status>. Custom production stages belong in the operating system/metafields, not in payment truth.
- Shopify supports GraphQL Admin API, metafields and webhooks. Webhooks require signature validation, deduplication, retries, out-of-order handling and reconciliation: <https://shopify.dev/docs/apps/build/webhooks>. Historical order access beyond 60 days requires appropriate access: <https://shopify.dev/docs/api/admin-graphql/latest/objects/Order>.
- Customer passwords are not portable through CSV, and customer CSV does not migrate order history: <https://help.shopify.com/en/manual/customers/import-export-customers>. Historical orders require an app/API route, not the normal admin import: <https://help.shopify.com/en/manual/shopify-admin/duplicate-store/>.
- URL redirects have platform restrictions; inventory and map product `/p-...`, article `/n/...`, categories, translations and campaign URLs: <https://help.shopify.com/en/manual/online-store/menus-and-links/url-redirect>.

## Git project inventory and source provenance

The authenticated GitHub inventory covered owner, collaborator and organization-member repositories. The following projects were inspected at branch/tree/source-document level. This is discovery of accessible repositories, not a claim that unknown/private accounts cannot contain another project.

| Repository | Inspected source | Role / migration consequence |
|---|---|---|
| `vzeman/biznisweb` | main `c98d350e` | Core reporting, operations, manufacturing, invoicing, credit notes, stock analytics and GrowthBook. Keep business logic/screens; replace source/writer adapters. |
| `Terem21/vevo-payment-reminders` | `codex/stripe-reminders`, `38a5ec0` | Production reminder service coupled to FLOX orders and Stripe Checkout metadata/callbacks. |
| `Terem21/vevo-sk-clanky` | `codex/natural-product-descriptions-20260916`, `4a488c0`; editorial branch `ef6aa9d` | Product HTML and 366-file content workflow; preserve evidence rules, replace FLOX publisher and URL identities. |
| `Terem21/ai-commerce-feeds` | `agent/initial-acp-pipeline`, `fa57858` | VEVO/ROY market-specific AI feeds sourced from FLOX XML. Replace reader; maintain market and product identity boundaries. |
| `Terem21/ugc-sales-boost` | main `ae2457b` | Independent ugc.sk/Supabase creator workflow with VEVO campaign; preserve attribution, links and return-adjusted evaluation. |
| `Terem21/aws-infrastructure` | main `528f007` | Runtime inventory and lifecycle evidence; distinguishes active reporting from retired game/legacy VEVO resources. |
| `Terem21/vevo-navsteva-game` | `codex/vevo-navsteva-game`, `15e5be9` | Historical game/loyalty prototype; newer infrastructure state records runtime retirement. Do not assume it is a live dependency. |
| `Terem21/video-factory` | main `05d3a80` | Creative video CLI; no order dependency established in inspected architecture. |
| `Terem21/domain-redirector` | `codex/domain-redirector-mvp`, `e6cebde` | Shared redirects; inspected configuration has no VEVO rule. |
| `vzeman/wachman` | main `f27675a` | Hugo trail-camera site; not VEVO manufacturing. |

Git hygiene issue discovered: reminder/content/feed default main branches are scaffolds; actual implementations are in open PR branches. Reminder PR 1 is open; content PRs 1/2 and feed PR 1 are drafts. Freeze actual deployed and source revisions before migration. Do not merge or reorganize them opportunistically during this audit.

## Existing reporting and manufacturing contracts

Source references below are relative to this repository at the baseline SHA.

| Contract | Evidence | Required preservation |
|---|---|---|
| Same service, two familiar screens | `VEVO_OPERATIONS.md:3`, `live_dashboard_server.py:2530` | `/production/vevo` and `/manufacturing/vevo` can keep their UI and navigation. |
| Manufacturing demand reader | `production_board.py:44,191,222` | FLOX orders/payment/line identity reader must be replaced. This is demand aggregation, not a batch/material-consumption manufacturing ERP. |
| Paid-or-COD eligibility | `projects/vevo/settings.json:44`, `roy_operations_dashboard.py:1126` | Paid statuses 31/70, or new1 plus recognized COD7/10/16. On Shopify express this as financial + fulfillment + cancellation truth, not translated labels. Unpaid cards must stay excluded. |
| Product classification | `projects/vevo/settings.json:52`, `production_board.py:152` | Current `vevo` label filter and gel exclusion need explicit stable classification; preserve EAN/import-code/warehouse identity. |
| Picking PDFs and print history | `live_dashboard_server.py:1327,1997,2025,2454`, `roy_operations_dashboard.py:641` | Preserve previews, batch prints, reprints and explicit acknowledgments. Import must not reset print state or create duplicate picks. |
| Personal pickup | `projects/vevo/settings.json:755`, `roy_operations_dashboard.py:3363` | Paid-only eligible pickup ID11; source status writer must become a verified fulfillment/pickup adapter. |
| Stock analytics | `operations_inventory.py:43,142` | Preserve immutable report inputs and complete live catalogue/stock reader. Reviewed operations module does not directly set provider stock. |
| Inbound/alert annotations | `roy_operations_dashboard.py:768,790,863,899` | Preserve internal S3 state and SKU mappings; inbound markers are annotations, not supplier purchase orders. |
| Costs, kits and historical identity | `projects/vevo/settings.json:318`, `projects/vevo/product_expenses.json`, `export_orders.py:2858,3607,3861` | 17 explicit bundle cost rules and 334 cost-map keys. These are not physical product counts or proof of manufacturing component expansion. |

Reporting includes cohorts, sample-to-full-size conversion, refill analysis, profit maturation/payback, product margins, attribution, stock forecasts and CRM recommendations. The daily report is configured for 01:00 Europe/Bratislava and history starts 2025-05-03. Preserve historical timestamps, customer continuity, original currencies, FX conventions, VAT, discounts, shipping, refunds and time-dependent cost meaning. Shopify native reports are not a replacement for these calculations.

Use a persistent identity map: source system + source order/line/variant ID + legacy reporting SKU + Shopify IDs. Import codes, EANs and title aliases all exist today; do not assume titles or a single barcode provide unique historical identity.

### Documents and payments

- `generate_invoices.py:154,720,1290` uses FLOX GraphQL plus native `/erp/orders/invoices/` endpoints for creating/finalizing/finding/sending documents. A Shopify API URL substitution cannot preserve it.
- VEVO config schedules invoice processing every 15 minutes, final sweep 23:58, with daily backlog checking (`projects/vevo/settings.json:116`). Preserve document creation, email and uncertain-result handling separately.
- Credit notes are read through FLOX ERP (`creditnote_export.py:265`); only finalized complete financial coverage permits whole-order cancellation. Partial/draft/unbound documents must not cancel the whole order (`projects/CREDITNOTE_AUTOMATION_OPERATIONS.md`). The VEVO guard is configured for 23:28 Bratislava.
- A shared ROY/VEVO monthly credit-note PDF/email exists (14th, 06:00 in README). VEVO migration must not break ROY or the accountant's export.
- Latest documented native Stripe mapping (2026-09-13, `PROJECT_STATE.md`) is paid31, cancelled34, expired33, refunded73, unpaid69. Older paid70 remains accepted for compatibility. This mapping was not reread from gateway-secret settings during this audit.
- Native FLOX Stripe callback handling is outside the repository. Existing report code is not itself a Stripe refund system.

### Reminders, content, feeds and marketing

- Reminder service checks FLOX GraphQL, exact domains/currencies/payment18/19/20 and Stripe Checkout metadata `order_id`, `variable_symbol`, `lang`, with FLOX callback paths. It reuses an existing open session rather than creating another order. Rules: reminder after 10 minutes from latest failure or 60 minutes abandonment, two-minute schedule, single-order claim, suppression after a later paid same-store order. Latest documented runtime check is 2026-09-24; it was not repeated here. Preserve old recovery links/claims and prevent duplicate Shopify reminders.
- Content publishing uses FLOX numeric page/product/block IDs, blog309 and `/n/` URLs. SK product descriptions currently fall back into EU according to September source evidence; define Shopify locale fallback intentionally. Some bundle prices are hardcoded in HTML and must stay consistent with catalogue prices.
- AI feeds read `/erp/impexp/googlenakupyexport/download` and `/cz`, `/hu`. They deliberately exclude Madfrog, which shares the FLOX installation. Preserve this boundary in products, history and customer migration. Feed merchant onboarding was pending in the last inspected state; publication today is unverified.
- GrowthBook currently depends on FLOX consent flags, DOM selectors, cart/purchase events and an exact order-number join. Rebuild the consent/event bridge, preserve Meta UTM meanings and freeze old experiment results. A different theme must not be pooled silently into the old experiment. Existing monitor evidence has a documented verification limitation; no current health claim is made.
- Public storefront has samples, sizes, sets, cross-sells, reviews, localized pages and long product content. A new theme can reproduce these, but static descriptions, media, review provenance, cart extras, coupons/free-shipping thresholds, stock alerts and emails require an explicit migration list. Existing Leadhub usage and all analytics/merchant integrations still need account-level inventory.

## Recommendation and architecture choices

**A new Shopify website is feasible. Unchanged daily work is a conditional integration objective, not something proven by a theme import.** Chameleoon and custom VEVO screens can remain; ordinary Shopify admin is a different UI. If identical BiznisWeb product/order-editing screens are mandatory, either retain a deliberate transitional back office or build equivalent controls. Do not promise identical clicks without observing staff use.

Recommended design: Shopify storefront/checkout -> durable integration layer -> existing operations/manufacturing/reporting; native Chameleoon connector for shipment handling; a selected document provider for invoices/credit notes. Maintain one authority for stock writes, one for fulfillment transitions and one for invoice numbering. Webhooks feed a durable queue with reconciliation; direct browser money is never financial truth. Preserve existing staff buttons and PDFs where possible.

Currency decision must precede implementation:

1. **One Shopify shop in EUR:** simplest operation, but CZ/HU local-currency checkout with standalone Stripe would change. This does not meet strict current behavior parity.
2. **Separate EUR/CZK/HUF shops behind a common operations screen:** technically plausible while retaining Stripe, subject to account eligibility. Adds subscriptions, inventory synchronization, cross-store identifiers and shared customer/history work. No longer one native Shopify admin.
3. **One shop with an eligible multi-currency provider:** not a ready replacement for VEVO. Follow-up official Adyen documentation now explicitly limits activation to organizations with existing Adyen approval, an existing Adyen account and Shopify Checkout. Organizations without that approval cannot activate it. The same page says manual payments including COD remain in the shop base currency even with this integration. No specific plan requirement was stated on the inspected page; do not infer that purchasing Plus unlocks it. Source: <https://help.shopify.com/en/manual/payments/third-party-providers/adyen-gateway>. Never select a false merchant country to obtain Shopify Payments.

Keeping BiznisWeb as a permanent mirrored operational backend is not the default recommendation: duplicated orders, stock, notifications and documents create two writers. A temporary bridge could be evaluated only with strict ownership and idempotency. If identical administration and one Stripe-based multi-currency shop are absolute requirements, Shopify is not yet a proven fit; redesigning the current storefront is a valid comparison.

## Public comparison: Bloom Robbins (2026-09-30)

The owner asked whether Bloom Robbins' gateway can be identified from public website code. The Slovak storefront and live guest checkout were inspected without entering contact, address or payment details, submitting an order or changing the existing cart contents.

- The SK checkout's DOM HTML explicitly names **Stripe Card Payments** as a `PaymentGateway` and as the available card payment method; its `availablePresentmentCurrencies` is `["EUR"]`. This is direct checkout configuration evidence, stronger than the Stripe logo also present in the cart. The checkout displays card payment, Google Pay and COD (2 EUR). No payment transaction was executed; merchant fees and account terms remain unknown.
- Public storefront HTML confirms separate Shopify stores: [SK](https://www.bloomrobbins.sk/) is `bloomsk.myshopify.com`, shop ID `25563398216`, EUR; [CZ](https://www.bloomrobbins.cz/) is `bloomcz.myshopify.com`, shop ID `10182197333`, CZK; [HU](https://www.bloomrobbins.hu/) is `bloomhu.myshopify.com`, shop ID `53294825639`, HUF. CZ/HU gateways were not verified in their checkouts.
- This is a concrete example of the separate-shops architecture above, not evidence of one Stripe-based Shopify shop charging all three currencies. Their internal order, inventory and fulfillment consolidation cannot be established from public HTML. Do not infer a Shopify plan, negotiated fees or internal integrations.

Only public gateway names, store identities and currencies are recorded here; checkout session URLs and identifiers are intentionally omitted. Existing VEVO/Chameleoon admin-access stop remains unchanged.

## Acceptance gates before any switch

Run test orders in a controlled environment with notification/payment/stock isolation. No tests below were executed by this audit.

| Scenario | Required parity |
|---|---|
| Paid card SK/CZ/HU | Correct charged currency, exactly one payment/order/document, appears once in production and reports. |
| Unpaid/failed/expired card | Excluded from picking/manufacturing, correct stock release, one appropriate reminder, legacy recovery remains valid. |
| COD SK/CZ/HU | Correct surcharge, COD amount and currency; eligible for picking, not misreported as received cash. |
| Pickup-point carriers | Exact point ID/provider/country survives cart -> order -> Chameleoon -> label. Test SPS/Packeta/FoxPost and postal/address variants. |
| Shipment creation | Preserve intended created->Shipped business event, tracking writeback, stock movement and invoice trigger once; no duplicate email. |
| Samples, gifts, bundles | Correct line identifiers, component demand where required, costs, discount allocation, weight and stock. |
| Cancellation, partial/full refund, returns | Payment, stock, credit note and profitability agree; partial refund does not cancel a whole order. |
| Personal pickup | Same paid eligibility and explicit completion, no second fulfillment. |
| Print/reprint and operator retry | Existing marks survive; retry does not duplicate shipment or document. |
| History and mixed transition period | Old/new orders appear once, cohorts and report totals reconcile, old documents/refunds remain accessible. |
| SEO/marketing | Redirects, locale domains, consent, purchase deduplication, analytics IDs, UGC attribution, feeds and emails verified. |

Then compare at least one complete operational cycle with staff, reconcile counts/amounts/stock and approve the actual screens. Cutover needs a bounded freeze, outstanding FLOX order list, opening stock baseline, rollback conditions and one writer per system. Do not cancel old services until unpaid sessions, returns and statutory document access are handled.

## Information still needed from the owner

1. Which daily steps occur in BiznisWeb, the custom operations/manufacturing screens and Chameleoon; who performs them and which screens/clicks must remain identical? The question was sent during audit; no answer received yet.
2. Migration scope: SK only first, or also CZ/HU/EU/DE/BG; are EUR/CZK/HUF charges mandatory? Which configured/legacy locales are actually active?
3. Where do recipes, batch numbers, raw-material stock and production completion live, if used? The discovered manufacturing code only aggregates demand.
4. Who owns stock receipts/corrections and returns today? Which printer/scanner/label formats and exceptional shipping scenarios must remain?
5. Required invoice/credit-note provider, accounting export and number-series rules; is keeping FLOX temporarily acceptable?
6. Number of staff, current subscriptions and monthly fee-eligible card volume for a meaningful full-cost comparison; desired deadline/budget and whether a Shopify shop already exists.

Stripe credentials are not requested. Verify connection eligibility in the real merchant account through its normal authenticated setup at the appropriate implementation stage.

## Credential incident and stop boundary

During Chameleoon inspection, a later full DOM output was read before navigation completed and included an unmasked BiznisWeb API credential from the prior source-configuration screen. Earlier reads had redacted input values; this read did not. The credential value is deliberately absent from this file, Git, PR text and final report.

The owner was notified immediately. Under the owner's plaintext-secret rule, treat that credential as compromised. Further administration inspection was stopped. No credential was changed, no source configuration was saved and no shipment/order/stock/payment action was taken.

Next security step: the owner must rotate the affected BiznisWeb credential through the provider's normal UI, update authorized consumers (at least the Chameleoon source; inventory any others before revoking), and verify connectivity. Do not blindly revoke a shared credential and break expedition. User entry/confirmation is required for credential changes by the computer-use policy. Never paste the replacement into chat or version control. This is a prerequisite for resumed account inspection.

## Verification and next exact step

Verified: public site/docs, authenticated admin observations listed above, authenticated Chameleoon mappings, Git repository and implementation branch inventory, selected source contracts. Documentation-only diff reviewed; no application tests were appropriate or run.

Not verified: current AWS runtime revision/health, Stripe live account and Shopify eligibility, Shopify test checkout, actual printing/scanning, full catalogue export, all marketing accounts, staff click-by-click workflow and complete accounting reconciliation. Historical deployment receipts in Git are not today's host proof. Existing runtime warnings were not repaired; no server was touched.

Next exact step: resolve the credential incident, obtain the workflow/currency/accounting inputs, then produce a fixed scope and proof-of-concept contract for the end-to-end acceptance scenarios. Implementation/deployment must follow the owner's instance-ID/IP/service/path -> host localhost+marker -> UI hard gate. Future standalone Shopify/integration projects need their own repository, PROJECT_STATE, cross-platform bootstrap and environment templates.

No Shopify account, app installation, payment, order, shipment, stock, notification or deployment change has been made.
