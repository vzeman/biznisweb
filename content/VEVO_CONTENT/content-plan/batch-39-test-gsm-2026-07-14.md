# Batch 39 test: gramáž látky a GSM

Date: 2026-07-14
Project: VEVO_CONTENT
Target block: Blog / Novinky (`765`)

## Article

- Title: `Gramáž látky: čo znamená GSM pri uterákoch, obliečkach a tričkách`
- Slug: `gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach`
- Plan source: C09A material encyclopedia gap
- Publication mode: hidden-first through `biznisweb-vevo-content`, exact-slug readback, then explicit publication

## Scope

- plain-language quick answer followed by a detailed technical explanation;
- practical interpretation for towels, bed linen, and T-shirts;
- distinction between mass per unit area, thickness, density, absorbency, and quality;
- home calculation and its measurement limits;
- washing, load, rinsing, and drying implications without replacing the care label;
- two comparison tables, styled callouts, product card, category card, related VEVO guides, and FAQ;
- no fixed product price and no internal editorial terminology in public copy.

## Duplicate decision

- Full three-section inventory checked before drafting: glossary block `1905`, FAQ block `774`, Blog block `765`.
- No standalone article title or slug about `gramáž látky`, `GSM`, or `plošná hmotnosť` was found.
- Candidate duplicate guard result: `ok`, no cluster and no review issue.

## Sources

- ISO 3801 for mass per unit length and mass per unit area of woven fabrics;
- ASTM D3776/D3776M for mass per unit area test methods;
- GINETEX for the separate role of textile care symbols.

## Required verification

- Complete: link preflight confirmed the target-slug `404` before create and all `11` destinations healthy.
- Complete: full `scripts/check.ps1` passed with `37/37` regression tests and all article guards green.
- Complete: content MCP created hidden post `2334`, passed admin title/slug/body readback, and published only after explicit confirmation.
- Complete: independent public verification confirmed the exact URL, title, rich HTML, two tables, cards, all links, and no malformed-paragraph regression.

## Publication result

- Post ID: `2334`
- Public URL: `https://www.vevo.sk/n/gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach`
- MCP report: `exports/batch-39-test-gsm-2026-07-14-mcp-publication.json`, `all_ok=true`
- Public report: `exports/batch-39-test-gsm-2026-07-14-publication-verify.json`, `all_ok=true`
