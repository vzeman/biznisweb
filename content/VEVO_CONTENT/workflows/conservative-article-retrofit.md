# VEVO Conservative Article Retrofit Workflow

Date: 2026-06-16
Project: VEVO_CONTENT

## Goal

Improve older VEVO articles according to the longer article standard without making the public content look like a disruptive rewrite.

The retrofit is additive by default:

- never change the article topic, public title, slug, URL, or canonical intent during retrofit,
- keep existing URL, title, short description, publish date, and main answer,
- keep the original article structure unless it is broken,
- preserve the original wording where it is already accurate,
- add useful sections instead of deleting or replacing whole sections,
- update only broken links, internal wording leaks, factual issues, or weak sales blocks.

## Batch Size

Retrofit in small waves:

- 2 to 3 articles per wave for live updates,
- 5 articles only when they share the same pattern and can be verified together,
- do not mass-update dozens of live posts in one publication pass.

## Safe Change Budget

For each existing article:

- keep the original quick answer and core practical steps,
- avoid changing more than roughly 10-15% of existing prose unless it is wrong,
- add 500-1000+ visible words where the article is thin,
- add sections after the existing core content rather than before it,
- avoid changing headings that already rank for the article topic,
- never add internal workflow or SEO language to public text.

## Preferred Additive Blocks

Use these blocks to expand articles naturally:

1. Practical diagnosis table: symptom, likely cause, first step.
2. Material or situation table: textile/surface, safe action, what to avoid.
3. "Kedy postup neopakovat" or "Kedy byt opatrny" section.
4. Prevention section: how to avoid the problem next time.
5. Product card matched to the cause.
6. Category card for broader shopping.
7. Related VEVO guides with existing URLs only.
8. FAQ with 3 to 5 real customer questions.
9. Source/context box where the topic benefits from expert support.

## Do Not Do

- Do not rewrite the whole article just to hit word count.
- Do not rename, retitle, reslug, repurpose, or merge an article during retrofit.
- Do not change slugs during retrofit.
- Do not add fixed prices.
- Do not remove useful existing internal links.
- Do not publish a duplicate article when an existing article should be expanded.
- Do not use public terms such as `longtail`, `keyword`, `SEO`, `fan-out`, `sub-query`, or `CTA`.

## Required Checks

Before each retrofit wave:

```powershell
python -X utf8 content\VEVO_CONTENT\tools\vevo_retrofit_inventory.py --out content\VEVO_CONTENT\exports\retrofit-inventory-latest.json --markdown content\VEVO_CONTENT\content-plan\retrofit-priority-latest.md
python -X utf8 content\VEVO_CONTENT\tools\vevo_public_content_guard.py
```

For newly expanded source JSON, also run:

```powershell
python -X utf8 content\VEVO_CONTENT\tools\vevo_article_depth_guard.py content\VEVO_CONTENT\imports\RETROFIT_FILE.json
```

After live update:

- verify HTTP 200 for every updated public URL,
- verify public text still passes internal-wording guard,
- verify product/category URLs return HTTP 200,
- record post IDs, URLs, update exports, and verification exports in `PROJECT_STATE.md`.
