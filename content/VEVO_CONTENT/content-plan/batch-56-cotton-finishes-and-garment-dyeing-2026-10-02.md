# VEVO batch 56: cotton finishes and garment dyeing

Date: 2026-10-02
Project: VEVO_CONTENT
Target: Blog page 309, News block 765, Slovak language 1
Status: selected after broad local, RSS and live-admin duplicate checks

## Selection method

Eighteen exploratory subjects and their trade-name aliases were checked against the merged VEVO Blog, FAQ, glossary, RSS and local article catalog. Exact live-admin scans of Blog block 765, FAQ block 774 and glossary block 1905 found no whole-word or whole-phrase match. The first scanner revision used substring matching and incorrectly found `ikat` inside `certifikáty` and `delikátne`; it was replaced by normalized phrase-boundary matching before any selection or mutation.

The final four subjects represent different manufacturing mechanisms: controlled compressive pre-shrinkage, crosslinking-based easy-care finishing, cellulase surface treatment and dyeing after a garment has been assembled. They are not alternative names for cotton, moleskin, general shrinkage, general colourfastness or ordinary home washing.

Mercerized cotton and batik remain in manual review because the title guard found superficial overlap with the published moleskin and Madras titles. Pigment-dyed garments, stonewashing, peach finishing, calendering, chintz, ikat, devoré, flocking, permanent pleats, smocking, broderie anglaise and brocade remain separate future candidates; none is silently treated as a synonym of the four selected subjects.

## 1. Sanforized or pre-shrunk cotton

Public title: Čo je sanforizovaná alebo predzrazená bavlna: zvyškové zrážanie a starostlivosť

Canonical boundary: one guide owns sanforized/Sanfor, sanforised, controlled compressive shrinkage, pre-shrunk cotton and Slovak predzrazená or sanforizovaná bavlna. It must distinguish the registered SANFOR standard from a generic marketing claim and must not promise zero dimensional change.

Reader decisions:

- understand how moisture, rubber-belt compression and drying reduce residual shrinkage before cutting;
- distinguish woven SANFOR limits from knit limits and from an unspecified `pre-shrunk` claim;
- measure length, width and skew only after a controlled wash/dry sequence and full relaxation;
- separate residual shrinkage from stretch recovery, seam twist, elastane damage and an incorrectly chosen size.

Separation from existing content: the general shrinkage article owns causes across fibres and household mistakes; denim and cotton articles own the base fibre and garment categories. This article owns the manufacturing claim, verification limits and practical interpretation of residual dimensional change.

Primary evidence: SANFOR GmbH process and shrinkage standards; Cotton Incorporated/CottonWorks dimensional-stability and mechanical-finishing guidance; AATCC TM135/TM150 references; GINETEX care symbols.

Product boundary: an ordinary liquid detergent may be recommended only for an explicitly washable compatible article. Pre-shrunk does not override temperature, tumble-drying, bleach, coating, elastane or professional-care restrictions.

## 2. Easy-care, non-iron and wrinkle-resistant cotton

Public title: Čo je nekrčivá bavlna a easy-care úprava: ako funguje a ako ju prať

Canonical boundary: one guide owns easy-care cotton, non-iron cotton, wrinkle-resistant cotton, durable-press cotton and Slovak nekrčivá bavlna. It must not state that every easy-care finish uses the same chemistry or that the garment can never wrinkle.

Reader decisions:

- distinguish fibre composition from a resin/crosslinking finish and from a synthetic blend;
- understand why laundering, tumble drying and prompt removal influence the rated appearance;
- inspect collar, cuff, seam and high-abrasion zones for finish loss, yellowing, brittleness or shade change;
- interpret formaldehyde-free claims without turning the article into diagnosis or chemical alarmism.

Separation from existing content: poplin owns a woven shirt fabric, cotton owns the fibre, ironing articles own technique and the wrinkle article owns general household causes. This article owns the easy-care manufacturing finish, its trade-offs and retained performance after repeated laundering.

Primary evidence: CottonWorks durable-press encyclopedia and PUREPRESS technical explanation; AATCC TM124/TM128/TM143 references; GINETEX symbols; European Commission and ECHA context for formaldehyde in consumer articles.

Product boundary: recommend a liquid detergent only when the label permits domestic washing. Do not imply that more detergent, hotter drying or concentrated product restores a worn finish.

## 3. Biopolished cotton

Public title: Čo je bioleštená bavlna: enzýmová úprava, žmolky a pranie

Canonical boundary: one guide owns biopolished cotton, bio-polished cotton, biopolishing, enzymatically polished cotton and Slovak bioleštená bavlna. It must distinguish an industrial cellulase finish from a consumer adding enzymes repeatedly at home.

Reader decisions:

- understand that controlled cellulase treatment removes protruding cellulosic microfibrils from the surface;
- distinguish the initial smoother appearance from permanent immunity to pilling or abrasion;
- recognise that excessive enzyme severity can cause measurable mass or strength loss;
- care for the finished article without promising that home washing can recreate industrial biopolishing.

Separation from existing content: pilling owns the general failure mechanism across fibres, cotton owns the base fibre and stonewashing owns denim ageing. This article owns the industrial biofinishing step, surface-fuzz mechanism and evidence limits.

Primary evidence: peer-reviewed cellulase and cotton-biopolishing research in PubMed/PMC, including measured surface, mass and strength effects; AATCC appearance and pilling test references; GINETEX care symbols.

Product boundary: a standard liquid detergent may be shown only for a compatible washable garment. Product copy must not claim enzymatic refinishing, depilling, fibre repair or prevention of all future pills.

## 4. Garment-dyed clothing

Public title: Čo je garment-dyed oblečenie: farbenie hotového odevu, blednutie a pranie

Canonical boundary: one guide owns garment dyed, garment dyeing, product dyed clothing and Slovak odev farbený po ušití. It must distinguish this production stage from piece dyeing before cutting, yarn dyeing before weaving, home overdyeing and the separate pigment-dyed subset.

Reader decisions:

- identify garment dyeing from construction clues without treating seam contrast as proof;
- understand why thread, labels, elastics, interfacing and layered seams may accept colour differently;
- separate intentional vintage variation from poor wet/dry crocking or uncontrolled colour transfer;
- wash dark or saturated garments with compatible colours and document abnormal change.

Separation from existing content: colourfastness owns test mechanisms across all textiles, Madras owns yarn-dyed checks and denim owns indigo ring dyeing. This article owns the stage at which a completed garment is coloured and the resulting component-to-component variation.

Primary evidence: CottonWorks garment-dyeing definition and dyeing guidance; AATCC colourfastness-to-laundering, water and crocking references; EU fibre-labelling context; GINETEX care symbols.

Product boundary: recommend ordinary liquid detergent only for an explicitly washable compatible article. Avoid claims that a detergent stops designed fading, repairs abraded pigment/binder or makes every garment-dyed item colourfast.

## Shared publication gates

- At least 3,000 visible words, 25 H2 headings, two responsive tables, ten styled blocks, two action buttons and twelve substantive FAQ questions per article.
- No fixed prices, internal editorial terminology, one-character paragraphs, escaped HTML or public claims about search strategy.
- Every internal, product, category and evidence URL must pass direct preflight without an exception allowlist.
- Seven-word-shingle overlap between any pair must remain below 0.13.
- Run the full project test suite and final duplicate guard before any mutation.
- Confirm account, public domain, language, page, block, live record count and zero exact title/slug matches; then run the hidden slug/rich-HTML smoke before one resumable publication batch.
- Independently verify active admin readback, public URL, visible depth, HTML structure, all outgoing links and a 390x844 mobile layout.
