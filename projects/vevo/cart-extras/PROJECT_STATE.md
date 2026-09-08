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

## Next exact step

Finish HU baseline and storefront/currency verification, add missing existing products to each program with immediate readback, localize any missing product text, set consumer-rounded bonus prices, and verify both carts plus unchanged SK. Record exact final values and rollback. No local dev service has been started.
