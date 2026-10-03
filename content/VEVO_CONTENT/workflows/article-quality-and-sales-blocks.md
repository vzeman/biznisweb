# VEVO Article Quality And Sales Blocks

Date: 2026-06-16
Project: VEVO_CONTENT

## Daily Quality Rule

Future VEVO content should be produced in smaller batches of 2 to 3 articles by default, with 5 articles only when the topics are very close and the same generator can keep quality high. Each batch starts with a fan-out brief and ends with verification. The goal is fewer articles per day, but longer, more useful articles.

Before writing, define:

- parent article or cluster,
- primary search intent,
- 8 to 14 sub-queries,
- duplicate risk,
- internal links,
- product card,
- category card,
- source requirements.

## Article Structure

Use this structure for expert/practical laundry articles:

1. Quick answer for a layperson.
2. Why the problem happens.
3. Practical steps.
4. Decision table by material, textile type, or cause.
5. Common mistakes.
6. Expert section with credible sources where useful.
7. Product card matched to the cause.
8. Category card for broader shopping.
9. Related VEVO guides.
10. FAQ.

Minimum quality signals:

- Standard expert/practical articles should target at least 1500 visible words. Pillar articles should target 2200+ visible words.
- Do not publish short filler articles. A shorter article is acceptable only for a narrow FAQ/update where the user explicitly asks for a short answer.
- Each article must cover the main question, related practical situations, diagnosis, prevention, and when a product/category is or is not the right solution.
- At least two visual blocks beyond plain paragraphs: table, callout, decision grid, product card, category card, source box.
- At least two practical tables for standard expert/practical articles.
- At least one "when not to use this" or "when to be careful" note.
- No fixed prices.
- No public use of the acronym shoppers do not understand.
- No customer-facing internal SEO/workflow wording. Public article HTML must not contain words or phrases such as `longtail`, `keyword`, `SEO`, `search intent`, `sub-query`, `fan-out`, `CTA`, or wording like "cielene pokrývame". Write it naturally as practical questions, situations, causes, or examples.

Run this guard on each new article JSON before publication:

```powershell
python -X utf8 content\VEVO_CONTENT\tools\vevo_article_depth_guard.py content\VEVO_CONTENT\imports\batch-XX-articles.json
```

## Better Product Card Pattern

Use one concrete product. The text must explain why it fits the problem, not just list the product.

```html
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">Riešenie podľa príčiny problému</h2>
<p>Ak je problém v zvyškoch pracieho prostriedku, najprv upravte dávkovanie, veľkosť náplne a oplach. Produkt má pomôcť čistote, nie prekryť chybu v praní.</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
<p><strong>Kedy dáva zmysel:</strong> pri bežnom praní, keď chcete šetrný základ a jasné dávkovanie bez zbytočného prevoňania problému.</p>
<p><strong>Kedy najprv riešiť príčinu:</strong> ak práčka zapácha, zásobník je zanesený alebo je bielizeň lepkavá po príliš veľkej dávke.</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť prací gél</a></p>
</div>
</div>
```

## Better Category Card Pattern

The category card should help the reader compare options.

```html
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Vyberte si riešenie podľa typu prania</h2>
<ul>
<li><strong>Bežné pranie:</strong> začnite pracím gélom a správnym dávkovaním.</li>
<li><strong>Citlivá pokožka:</strong> vyberajte jemnejšie produkty a pridajte dôkladný oplach.</li>
<li><strong>Vôňa až na konci:</strong> parfum do prania používajte na čistú bielizeň, nie na prekrytie zatuchnutia.</li>
</ul>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Porovnať pracie gély</a></p>
</div>
```

## Product And Category Matching Matrix

| Problem in article | Product card | Category card | Important note |
|---|---|---|---|
| dosing, ordinary washing, residue from too much product | `/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l` | `/c/vevo-home-care/pranie/praci-gel` | Explain dose, rinse, and load size before product push. |
| washer odor, drawer residue, slimy seal, dirt returning to laundry | `/p-1549/vevo-shot-koncentrat-na-cistenie-pracky` | `/c/vevo-home-care/pranie/detox-pracky` | Do not recommend fragrance as the first fix. |
| fragrance discovery after clean laundry | `/p-1574/sada-vzoriek-najpredavanejsich-voni-vevo-3-x-10ml` or `/p-1621/vevo-essence-sample-set` | `/c/vevo-fragrance/parfum-do-prania` | Position samples as lower-risk choice. |
| gentle clean scent for laundry | `/p-1532/parfum-do-prania-vevo-no-08-cotton-dream` | `/c/vevo-fragrance/parfum-do-prania` | Use only after the article solves cleaning and rinse. |
| hard towels and vinegar-based softening | verify current best product before publishing | `/c/vevo-home-care/pranie/avivaz/octova-avivaz` | Warn about textile labels and not using softeners everywhere. |
| sensitive skin or baby laundry | `/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l` | `/c/vevo-home-care/pranie/hypoalergenne-pracie-prostriedky` | Use cautious language; no medical promises. |
| drying, static, bulky bedding | verify current dryer-ball product before publishing | `/c/vevo-home-care/pranie/gule-do-susicky` | Connect to drying and textile care, not washing chemistry. |
| room scent after cleaning | verify current room spray before publishing | `/c/vevo-fragrance/interierovy-sprej` | Fragrance follows cleaning and ventilation. |

## Stronger Sales Block Copy Rules

Good sales block copy:

- names the problem,
- explains the product fit,
- gives a practical use boundary,
- links to product and category,
- avoids pressure and fake certainty.

Weak copy to avoid:

- same paragraph in every article,
- "this is the best product" claims,
- recommending fragrance before removing odor source,
- product card without the category path,
- category link with no context.

## Verification Checklist

Before publication:

- product URL returns HTTP 200,
- category URL returns HTTP 200,
- product card and category card are both present,
- no fixed prices,
- no malformed hrefs,
- customer-facing text does not contain internal marketing jargon,
- customer-facing text passes `content/VEVO_CONTENT/tools/vevo_public_content_guard.py`,
- new batch JSON passes `content/VEVO_CONTENT/tools/vevo_article_depth_guard.py`,
- links match the article cause.
