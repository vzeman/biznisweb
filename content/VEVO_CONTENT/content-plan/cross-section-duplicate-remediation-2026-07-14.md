# VEVO cross-section duplicate remediation - 2026-07-14

## Scope

- Sections audited through the authenticated admin inventory:
  - glossary block `1905`, public page `Slovník pojmov o praní a vôňach | Encyklopédia vôní`,
  - FAQ block `774`, public page `Často kladené otázky o praní a vôňach`,
  - blog block `765`, public page `Blog`.
- Initial inventory: 829 records, 808 active and 21 hidden.
- The audit compared normalized titles, exact normalized bodies, title token similarity, body cosine similarity, and five-word shingle overlap.
- All 88 generated near-title review pairs were editorially reviewed; automated similarity alone was not treated as proof of duplication.

## Exact duplicates

Seven public exact-title groups were confirmed. Six clean canonical URLs were preserved and expanded; the six secondary `...1` URLs were also preserved but their article titles and editorial scopes were changed to distinct subtopics. The exact duplicate of the curtain-ironing article on slug `111111111111111111` was hidden because the clean canonical post `1682` is public and contains the same body.

Post-remediation audit result:

- active records: 807,
- hidden records: 22,
- public exact-title groups: 0,
- public exact-body groups: 0,
- public bad slugs: 0.

## Semantic overlap selected for expansion

Titles and URLs in this phase remain unchanged. Each article is expanded around an explicit purpose so users and search engines can distinguish the result.

| Group | Post IDs | Distinct editorial scopes |
| --- | --- | --- |
| Laundry perfume by process | `2309`, `2310`, `2311` | hand washing; low-temperature 30 °C process; hygiene-oriented 60 °C process |
| Laundry gel by colour | `2313`, `2314`, `2315` | black residue and dye protection; white greying and bleaching; colour transfer and new garments |
| Sports towels | `1967`, `2069` | odour prevention after training; material and gym-use washing guide |
| Bathroom mats | `1990`, `2066` | moisture and odour prevention; backing type, machine balance and drying |
| Black clothing | `1881`, `1906` | diagnosis of marks after washing; preventive anti-fading routine |
| Best laundry fragrance | `1185`, `1193` | objective selection matrix; blinded personal-favourite test |
| Streak-free windows | `1500`, `1459` | linear cleaning procedure; symptom-based streak diagnosis and FAQ |
| Cost per wash | `1437`, `1375` | auditable cost formula; savings scenarios and annual sensitivity |
| What laundry perfume is | `1576`, `1186`, `1623` | technical glossary definition; beginner's first cycle; complete pillar and troubleshooting guide |
| Gel plus laundry perfume | `1921`, `1868` | compartment and dose decision tree; material, water, residue and odour troubleshooting |

Final content requirements:

- 23 posts,
- 1,510-2,977 visible words after preserving original content,
- minimum two tables per article,
- rich inline-styled information, diagnostic and product/category blocks,
- no fixed product prices,
- no internal planning terminology in public text,
- all new link destinations checked before publication.

The first live expansion pass was rejected during post-write review because shared generic decision prose increased body similarity in several pairs. The final pass was rebuilt from the immutable pre-remediation backup: generic prose was removed and every target received its own article-specific deep dive. No expansion was stacked onto the rejected pass.

## Similar-looking pairs retained as legitimate

The following patterns are intentionally separate and were not treated as duplicates:

- general robot-vacuum selection versus selection specifically for a robot with a mop,
- oil-based laundry-perfume buying guide versus its FAQ article,
- general spicy or citrus perfume versus separate women's and men's perfume intents,
- Marseille soap as a material/product concept versus Marseille soap specifically for laundry,
- general sink, curtain, cap, sunscreen, ketchup, towel, bedding or stain guides versus material-, room-, use-case- or failure-specific guides,
- general home-cleaning pages versus guides for a particular surface or appliance,
- odour guides with different sources, such as a wardrobe, cooking, sports bag, washing machine or seasonal storage.

These pairs remain under observation. A future audit should escalate them only when both the user intent and the substantive body overlap, not merely because titles share a common head phrase.

## Final live verification

- Final all-section inventory: 829 records, 807 active and 22 hidden.
- Public exact-title groups: 0.
- Public exact-body groups: 0.
- Public bad slugs: 0.
- Semantic remediation MCP result: 23 of 23 updates passed with unchanged titles and slugs.
- Independent public verifier: 35 of 35 exact and semantic remediation pages matched the prepared visible body, title and slug.
- The hidden repeated-`1` curtain clone remains public `404`.
- Final content, depth, HTML, public wording and project checks pass with 37 unit/regression tests.
- The similarity report still lists 14 high-priority title pairs because that score is intentionally title-led. They are covered by the manual classification above; a similar title alone is not treated as a duplicate after the bodies and user intents are verified as distinct.

## Permanent prevention

1. Scan the complete three-block admin inventory before every new batch.
2. Block exact active title or slug collisions before create or publish.
3. Review high title similarity together with body cosine and shingle overlap.
4. Keep one primary intent and an explicit scope boundary in every closely related article.
5. Preserve existing titles and URLs during ordinary expansion; only exact-duplicate remediation may retitle a secondary copy after a canonical is confirmed.
6. Require wording, depth, HTML, fixed-price, link and public-URL verification before completion.
7. Re-run this cross-section audit after every remediation or material content batch.
