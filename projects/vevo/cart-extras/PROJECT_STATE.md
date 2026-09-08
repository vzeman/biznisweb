# VEVO cart extras localization

Date: 2026-09-08
Repo: vzeman/biznisweb
Branch: codex/vevo-cart-extras-cz-hu

## Authorized scope

Mirror the six SK "Odporúčame dokúpiť" cart extras into the existing CZ and HU charges programs, with local-language product text and rounded local-currency consumer prices. Preserve SK, stock, checkout/payment configuration and unrelated programs.

## Baseline before mutations

Admin: https://vevo.flox.sk/erp/main/settings → Objednávky → Bonusy a odmeny.
SK program: Doplnkové služby (charges), language Slovakia, heading Odporúčame dokúpiť.

| Order | Product ID | SK product | Bonus net EUR | Consumer EUR |
|---|---|---|---|---|
| 1 | 1549 | Vevo Shot - koncentrát na čistenie práčky 100ml | 7.235 | 8.90 |
| 2 | 1627 | Prací gél hypoalergénny Vevo Ylang Absolute 1L | 7.235 | 8.90 |
| 3 | 1630 | Univerzálny voňavý čistič Vevo Pure Harmony 500ml | 8.05 | 9.90 |
| 4 | 1551 | Prací gél hypoalergénny z Marseillského mydla 1L | 4.8 | 5.90 |
| 5 | 1542 | Poistenie proti rozbitiu | 0.731 | 0.90 |
| 6 | 1543 | Tringelt | 0.16 | 0.20 |

CZ program: Doplnkové služby CZ (charges), language Czech Republic, active, show_in_cart enabled, no date/order/category restriction, cart_autoinsert false, allow_singular false. Heading Doplňkové služby. Existing row order and net CZK: Shot 181.8; Spropitné 8.25; Pojištění proti rozbití 19. No edits saved yet.

## Verified implementation details

HU baseline: Doplnkové služby HU (charges), language Hungary, active and show_in_cart enabled, no restrictions, cart_autoinsert false, allow_singular false; heading Kiegészítő szolgáltatások; zero bonus rows. Admin languages confirm vevopure.cz/CZK and vevopure.hu/HUF. All six catalog IDs already have localized CZ/HU titles and short descriptions; connector get_product errors were not missing translations. No catalog mutation is needed.

Target consumer prices by source order: CZK 219, 209, 239, 139, 25, 5; HUF 3190, 3190, 3490, 2090, 350, 100. These are rounded from the shop's existing localized catalog pricing (HU physical baselines 3205, 3200, 3565, 2125 HUF; CZ Shot 220 and gels 215/145 CZK), with modest downward rounding for physical extras. Use the existing local VAT (CZ 21%, HU 27%) to derive net bonus prices and confirm actual cart totals. Headings: Doporučujeme dokoupit / Ajánljuk még a kosárba.

Bonus products are selected from the existing product catalog; their names are not editable in the bonus list. Bonus prices are editable and displayed without VAT in the program's currency. Existing CZ translations of 1627 and 1551 are present. HU get_product for those IDs returned a connector error, which does not establish whether translations exist.

Official sources read: https://www.biznisweb.sk/a/851/bonusovy-system-uvod ; https://www.biznisweb.sk/a/173/zaokruhlovanie-cien-meny ; https://www.biznisweb.sk/a/110/jazykove-verzie-webstranok-e-shopov . Products must be shared IDs localized through language categories, never independent duplicate catalog products; stock is shared.

## Completed live changes and verification

Both existing language-scoped programs are active, shown in the cart, and contain the same six shared product IDs in SK order. No catalog records, stock, currency settings, payments, shipping, SK program or unrelated programs were changed.

| Product ID | CZ bonus ID | CZ net | CZ consumer Kč | HU bonus ID | HU net | HU consumer Ft |
|---|---|---|---|---|---|---|
| 1549 | 13 | 180.992 | 219 | 25 | 2511.811 | 3190 |
| 1627 | 22 | 172.727 | 209 | 26 | 2511.811 | 3190 |
| 1630 | 23 | 197.521 | 239 | 27 | 2748.031 | 3490 |
| 1551 | 24 | 114.876 | 139 | 28 | 1645.669 | 2090 |
| 1542 | 15 | 20.661 | 25 | 29 | 275.591 | 350 |
| 1543 | 11 | 4.132 | 5 | 30 | 78.740 | 100 |

CZ heading: Doporučujeme dokoupit. HU heading: Ajánljuk még a kosárba. Existing local catalog titles and short descriptions render correctly in both carts. New rows automatically use the localized product records; the product selector itself shows SK titles.

Verified by closing/reopening both admin programs: headings, activation, all six saved net prices and order. Verified on https://www.vevopure.cz/e/cart/index and https://www.vevopure.hu/e/cart/index: all six translated names, descriptions, images where present, order and exact consumer prices above.

Functional tests used initially empty CZ and HU carts, one Sample Set (product 1621) per cart. CZ: added bonus Ylang at 209 Kč, total 230 + 209 = 439 Kč. HU: added bonus insurance at 350 Ft, total 3420 + 350 = 3770 Ft. Removed only these test additions; both carts independently confirmed empty afterwards. No order was submitted.

SK admin and public cart readback confirmed the baseline six products, order, heading and prices unchanged (8.90, 8.90, 9.90, 5.90, 0.90, 0.20 EUR). Existing user SK cart of one item / 32.90 EUR was preserved.

Known implementation behavior: ExtJS dialogs load asynchronously; do not fill the heading immediately after opening before reading loaded values, because a late load can overwrite it. One early HU heading attempt did not persist; corrected on fully loaded form and independently verified after reopen. Connector get_product can error for no-variant products; list_products and admin product picker supplied the required verification. Public CZ Shot catalog currently displays 215 Kč, while its separately configured bonus is 219 Kč (the prior bonus was about 220 Kč); catalog prices were not changed.

## Rollback

Only if requested: restore CZ heading Doplňkové služby, remove new bonus rows 22/23/24, restore existing net prices and order Shot (13) 181.8; Spropitné (11) 8.25; Pojištění (15) 19. Restore HU heading Kiegészítő szolgáltatások and remove new bonus rows 25–30, leaving program active with no bonuses. Preserve catalog products and shared stock. Reopen/read back and verify public carts after any rollback.

## Next exact step

Requested live configuration is complete and verified. Documentation is on codex/vevo-cart-extras-cz-hu for review. No local dev service was started or stopped; no local runtime remains from this task.
