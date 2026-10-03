# VEVO batch 27 quality fan-out

Date: 2026-06-16
Project: VEVO_CONTENT
Purpose: Quality reset before the next daily article batches.

## Why This Exists

The last batches correctly expanded the laundry-process cluster, but the article pattern started to become too uniform. Future VEVO batches should be smaller, deeper, and built from a documented fan-out before writing.

Daily batch target:

- 3 to 5 articles per batch.
- Each article must own one clear primary intent.
- Each article must include 8 to 14 useful sub-queries inside the structure, not only in a keyword sentence.
- Each article must contain practical depth, expert context, and a natural product/category path.
- No fixed prices in articles.
- No customer-facing use of the acronym that shoppers do not understand.

## Quality Gate For Each New Article

Minimum for normal longtail articles:

- 1 quick answer for a layperson.
- 1 diagnostic section explaining why the problem happens.
- 1 step-by-step section.
- 1 decision table or troubleshooting table.
- 1 "what not to do" section.
- 1 expert section with 2 to 4 credible sources where the topic is technical, health-adjacent, chemical, textile, appliance, or hygiene-related.
- 3 to 6 internal article links.
- 1 product card and 1 category card, both matched to the actual problem.
- 1 FAQ section with 3 to 5 questions.
- Target length: 1,800 to 2,800 words for standard longtails; 2,800 to 4,000 words for semi-pillars.

Do not publish a new article if the best page type is an expansion of an existing URL.

## Fan-Out From The Last Published Articles

### Parent 2255: Ako funguje praci gel

Parent URL:
`/n/ako-funguje-praci-gel-tenzidy-enzymy-ph-a-davkovanie-pri-beznom-prani`

What the parent should keep:

- Basic explanation of detergent chemistry.
- Tenzidy, enzymes, pH, dosing, residue, and rinse as one broad educational article.

Safe fan-out themes:

| New angle | Primary intent | Sub-queries to cover | Page type | Product/category path |
|---|---|---|---|---|
| Dosing by load and water | how much laundry gel to use | kolko ml pracieho gelu na 5 kg, davkovanie pri tvrdej vode, davkovanie pri kratkom programe, privela gelu v pracke, gel priamo do bubna alebo zasobnika, davkovanie pri uterakoch, davkovanie pri sportovej bielizni | longtail guide | praci gel product + praci gel category |
| Gel vs powder vs capsules | detergent format comparison | praci gel alebo prasok, praci gel vs kapsuly, co je lepsie na skvrny, co je lepsie na nizku teplotu, co je lepsie na citlivu pokozku, zvyky v zasobniku, biele smuhy na ciernom obleceni | comparison | praci gel category + hypoallergenic gel |
| Enzymes in laundry | enzyme-specific explainer | enzymy v pracom prostriedku, proteaza amylaza lipaza, enzymy a vlna, enzymy a hodvab, enzymy na krv, enzymy na pot, enzymy pri nizkej teplote | expert explainer | praci gel category, no overclaiming |
| pH and skin/textiles | pH detergent care | pH pracieho prostriedku, alkalicke pranie, citliva pokozka po prani, zle oplachnuta bielizen, hypoalergenny gel, parfumacia a pokozka | expert guide | hypoallergenic laundry category |
| Gel residue in washer drawer | appliance/use problem | praci gel ostava v zasobniku, husty gel v pracke, zaneseny zasobnik, voda netecie do priehradky, ako vycistit zasobnik | troubleshooting | praci gel + washer detox |

Avoid:

- Another article with the exact head term "ako funguje praci gel".
- Generic detergent chemistry without a specific practical problem.

### Parent 2256: Predpieranie v pracke

Parent URL:
`/n/predpieranie-v-pracke-kedy-ma-zmysel-a-kedy-len-mina-vodu-cas-a-praci-prostriedok`

What the parent should keep:

- Broad explanation of when prewash is useful and when it wastes resources.

Safe fan-out themes:

| New angle | Primary intent | Sub-queries to cover | Page type | Product/category path |
|---|---|---|---|---|
| Prewash for mud/workwear | predpieranie silno spinaveho oblecenia | predpierka na blato, monterky predpieranie, detske nohavice od blata, pelech predpieranie, pracovne tricko od hliny | practical guide | praci gel + washer cleaning if heavy dirt |
| Pre-treating stains vs prewash | predpieranie alebo predcistenie skvrny | mastna skvrna pred pranim, krv predpieranie, cokolada predpieranie, lokálne osetrenie skvrny, kedy nepouzivat predpierku | decision guide | praci gel + stain-related articles |
| Short program vs prewash | kratky program alebo predpieranie | kratky program na spotene oblecenie, rychle pranie a zvyky gelu, predpierka pri uterakoch, setrenie vody pri prani | comparison | praci gel + pranie category |
| Prewash and sensitive skin | extra rinse/prewash for skin | predpieranie detskeho oblecenia, citliva pokozka, zvyky pracieho prostriedku, extra oplach vs predpieranie | health-adjacent guide | hypoallergenic gel category |

Avoid:

- Repeating "kedy ma zmysel predpieranie" without a narrower use case.

### Parent 2257: Otacky pri odstredovani

Parent URL:
`/n/otacky-pri-odstredovani-ako-ovplyvnuju-vlhkost-krcenie-a-opotrebovanie-oblecenia`

What the parent should keep:

- General relationship between spin speed, moisture, wrinkles, and textile stress.

Safe fan-out themes:

| New angle | Primary intent | Sub-queries to cover | Page type | Product/category path |
|---|---|---|---|---|
| Spin speed by material | otacky podla materialu | kolko otacok na bavlnu, kolko otacok na viskozu, kolko otacok na vlnu, otacky na polyester, otacky na uteraky, otacky na obliecky | practical table guide | praci gel + material guides |
| Wet laundry after spin | bielizen je po odstredeni mokra | preco pracka neodstreduje, mokre obliecky po prani, preplneny bubon, filter pracky, nerovnovaha bubna, nizke otacky | troubleshooting | washer detox + filter article |
| Wrinkles from washing | preco je pradlo pokrcene po prani | vysoke otacky, preplnena pracka, viskoza krcenie, obliecky pokrcene, susenie v byte, zehlenie | problem guide | ironing category + praci gel |
| Spin before dryer | otacky pred susickou | kolko otacok pred susickou, uteraky do susicky, obliecky do susicky, setrenie energie, kedy znizit otacky | dryer-adjacent guide | dryer balls + dryer fragrance |

Avoid:

- General "800 alebo 1200 otacok" duplicate unless the article is material-specific.

### Parent 2258: Preplnena pracka

Parent URL:
`/n/preplnena-pracka-preco-sa-bielizen-nevyperie-neoplachne-a-zapacha`

What the parent should keep:

- General explanation of why overloading reduces washing and rinsing quality.

Safe fan-out themes:

| New angle | Primary intent | Sub-queries to cover | Page type | Product/category path |
|---|---|---|---|---|
| Load size by textile | kolko bielizne dat do pracky | kolko uterakov do pracky, kolko obliecok do pracky, polovica bubna, ruka nad pradlom, kg bielizne v praxi, plny bubon a zapach | practical guide | praci gel + pranie category |
| Large items | how to wash bulky items | deka v pracke, vankus v pracke, perina v pracke, obliecky sa zamotaju, plachta sa skruti, nerovnovaha bubna | longtail guide | praci gel + bedding articles |
| Family laundry workflow | how to split laundry loads | triedenie prania v rodine, detske oblecenie, uteraky samostatne, sportove veci samostatne, rychle pranie vs velka davka | workflow guide | pranie category + samples/fragrance if relevant |
| Overload and smell | why clothes still smell | preplneny bubon zapach, pot v syntetike, slaby oplach, zvyky gelu, bielizen smrdí po vysuseni | problem guide | praci gel + perfume after cleaning only |

Avoid:

- Another "preplnena pracka" head-term article.

### Parent 2259: Tvrda alebo lepkava bielizen

Parent URL:
`/n/preco-je-bielizen-po-prani-tvrda-alebo-lepkava-zvysky-gelu-davkovanie-a-oplach`

What the parent should keep:

- Broad troubleshooting for hard/sticky laundry after washing.

Safe fan-out themes:

| New angle | Primary intent | Sub-queries to cover | Page type | Product/category path |
|---|---|---|---|---|
| Extra rinse | kedy pouzit extra oplach | extra oplach pri citlivej pokozke, zvyky gelu, bielizen lepi, silna vona po prani, uteraky tvrde, detske oblecenie | problem guide | hypoallergenic gel + praci gel |
| Hard water dosing | washing in hard water | tvrda voda davkovanie, bielizen je tvrda, vodny kamen v pracke, zmakcenie uterakov, ocot do prania, octova avivaz | expert guide | octova avivaz + praci gel |
| Too much fragrance/product | too much scent/residue | privela parfumu do prania, silna vona z bielizne, bolest hlavy z vone, zle oplachnuty parfum, ako znizit intenzitu vone | fragrance safety guide | samples + perfume category, careful language |
| Drawer and machine residue | residues returning to laundry | usadeniny v zasobniku, sliz v pracke, guma pracky zapacha, praci gel v zasobniku, cistenie pracky | troubleshooting | Vevo Shot + detox pracky category |

Avoid:

- Generic "preco je bielizen tvrda" unless it is specifically hard-water, rinse, product-residue, or textile-type focused.

## Next Daily Batch Candidate Shortlist

Use these only after duplicate guard and manual review against Blog + FAQ:

1. Ako davkovat praci gel podla tvrdosti vody, naplne a znecistenia
2. Praci gel alebo praci prasok: kedy co funguje lepsie a preco
3. Extra oplach v pracke: kedy pomoze pri zapachu, tvrdej bielizni a citlivej pokozke
4. Kratky program v pracke: kedy staci a kedy zhorsuje zvysky pracieho prostriedku
5. Kolko bielizne dat do pracky: prakticka kapacita podla uterakov, obliecok a sportu

Backup candidates if any of the above collide:

6. Praci gel ostava v zasobniku: preco sa to deje a ako vycistit priehradku
7. Bielizen je po odstredeni mokra: priciny od preplnenia po filter pracky
8. Otacky pri prani podla materialu: bavlna, viskoza, vlna, polyester a uteraky
9. Predcistenie skvrny alebo predpieranie: ako sa rozhodnut pred spustenim pracky
10. Privela parfumu do prania: ako znizit intenzitu vone a vyprat zvyšky z textilu

## Internal Linking Rules For This Fan-Out

Every new child article must link back to the relevant parent from batch 26. Parent articles should later receive a "Súvisiace podrobné návody" block linking to the best child articles.

Required parent links:

- Detergent/dosing articles -> `2255`
- Prewash/stain-prep articles -> `2256`
- Spin/drying/wrinkle articles -> `2257`
- Load size articles -> `2258`
- Rinse/residue/hard-water articles -> `2259`

## Sales Block Direction

The product/category block must be matched to the cause:

- If the cause is poor washing or product choice: lead with laundry gel.
- If the cause is washer residue or odor: lead with washer detox, not fragrance.
- If the cause is hard water or scratchy towels: lead with vinegar-based laundry softening category, but keep textile warnings.
- If the cause is scent preference after clean laundry: lead with samples or fragrance category.
- If the cause is sensitive skin: lead with hypoallergenic laundry category and careful, non-medical language.

Do not use a generic identical product paragraph in all articles. The card must explain:

1. Why this product/category fits this exact problem.
2. When it is not the first step.
3. What the reader should fix before adding fragrance.
4. A clear button to product and a second button/link to category.
