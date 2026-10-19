# Batch 32 - Robotické vysávače a mopovanie

Date: 2026-07-05
Project: VEVO_CONTENT
Target publish date: 2025-09-20

## Intent

Vytvoriť novú VEVO mini-sériu pre témy okolo robotických vysávačov, robotických vysávačov s mopom, údržby, offline stavu, párovania Xiaomi, nabíjania, reštartu, zapnutia, odvápnenia a umiestnenia dokovacej stanice.

Témy sú mimo pôvodného content planu, ale sú relevantné pre VEVO Home Care, upratovanie, čistenie podláh a produkt:

- `https://www.vevo.sk/c/vevo-home-care/upratovanie/cistiace-prostriedky/cistic-do-robotickeho-vysavaca`
- `https://www.vevo.sk/p-1635/vevo-cistic-podlah-pre-vsetky-vysavace-ylang-absolute`

## Guardrails

- Žiadne fixné ceny.
- Žiadne verejné interné výrazy typu `CTA`, `SEO`, `longtail`, `keyword`, `fan-out` alebo `sub-query`.
- Každý článok musí mať rýchlu odpoveď, praktickú diagnostiku, kroky, tabuľky, odporúčané riešenie, produktovú kartu, kategóriovú kartu, FAQ a odborný kontext.
- Každý článok musí obsahovať povinný odkaz na kategóriu čističov do robotického vysávača a produkt VEVO čistič podláh pre všetky vysávače.
- Publikovanie musí rešpektovať duplicate-safety pravidlá: direct `add_news_post` iba ako hidden draft; verejný publish len slug/date-safe postupom.

## Article Topics

1. Ako vybrať robotický vysávač
2. Ako vybrať robotický vysávač s mopom
3. Ako vyčistiť robotický vysávač
4. Ako reštartovať robotický vysávač
5. Ako spárovať robotický vysávač Xiaomi
6. Ako dlho sa nabíja robotický vysávač
7. Ako funguje robotický vysávač
8. Ako zapnúť robotický vysávač
9. Ako odvápniť robotický vysávač
10. Robotický vysávač je offline
11. Kam umiestniť robotický vysávač a kam ho schovať

## Source Direction

Use official or manufacturer-adjacent support pages for technical claims:

- Xiaomi support for pairing, Wi-Fi reset and offline/network issues.
- iRobot support for navigation, LiDAR/maps and charging.
- Ecovacs support/blog resources for robot mops, tanks and cleaning-solution cautions.

## Next Step

Generate batch JSON, run duplicate guard, public wording guard, depth guard and live link preflight. Do not publish publicly unless slug/date-safe workflow is available.
