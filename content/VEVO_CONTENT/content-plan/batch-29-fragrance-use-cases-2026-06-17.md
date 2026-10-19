# VEVO batch 29 fragrance use-case fan-out

Date: 2026-06-17
Project: VEVO_CONTENT
Purpose: Continue the small high-quality new-content cadence after batch 28 with three distinct fragrance/laundry use-cases.

## Source Cluster

- C01-C03: parfum do prania, vona bielizne, avivaz and fragrance use.
- C04/C14: cautious fragrance use, sensitive skin and fragrance boundaries.
- C16/C21/C30: situational scent problems around wardrobe, work clothing, travel and home routines.

## Duplicate Guard Outcome

Candidate file:

- `content/VEVO_CONTENT/batches/batch-29-candidates-2026-06-17.txt`

Guard export:

- `content/VEVO_CONTENT/exports/batch-29-2026-06-17-duplicate-guard.json`

Manual resolution:

- `Najcastejsie chyby pri parfumoch do prania...` returned `OK`.
- `Ako prevonat oblecenie do kancelarie...` returned `REVIEW` only because it belongs to the broad fragrance cluster. The closest live match was the batch 28 small-bathroom article with low title overlap. Intent is distinct: office clothing, collar/neck contact and low-intensity workplace scent.
- `Vona oblecenia v kufri...` returned `OK` with no issues.

## Batch Topics

| Title | Primary intent | Practical questions to answer | Product/category path |
|---|---|---|---|
| Najcastejsie chyby pri parfumoch do prania: privela vone, zly oplach a miesanie s avivazou | prevent misuse and reduce customer disappointment | too much scent, poor rinse, damp laundry, mixing with softener, towels/sportswear/sensitive skin | sample set + parfum do prania category |
| Ako prevonat oblecenie do kancelarie: jemna vona, kosela pri krku a pradlo bez tazkej parfumacie | subtle workwear scent without bothering wearer or colleagues | shirts, blouses, collars, deodorant residue, office etiquette, fragrance layering | sample set + parfum do prania category |
| Vona oblecenia v kufri: ako balit cistu bielizen na cestu, aby nezatuchla | travel packing and laundry scent retention | packing dry clothes, separating shoes/used laundry, humidity, hotel storage, return routine | sample set + fragrance samples category |

## Quality Requirements

- At least 1500 visible words per article.
- Quick answer, practical situation block, tables, step-by-step routine, caution section, expert context, sources, FAQ, internal links, product card and category card.
- No fixed prices.
- No public internal workflow wording such as `longtail`, `SEO`, `fan-out`, `sub-query`, `keyword`, or `CTA`.
- Public date target: `2025-09-23`, before the required `2025-10-12` cutoff.

## Source Direction

Use cautious, practical language:

- Fragrance is a supplement after clean washing, rinsing and drying.
- In office and skin-contact contexts, keep scent intensity low.
- For travel and suitcase storage, moisture control comes before scent.
- Use expert sources only as broad support, without medical or product claims.
