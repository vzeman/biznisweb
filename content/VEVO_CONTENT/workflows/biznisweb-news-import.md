# VEVO BiznisWeb News Import Workflow

Use this workflow for public VEVO blog articles when clean URL and date control matter.

## Required Order

1. Prepare candidate titles.
2. Prepare a short fan-out brief before writing articles:
   - parent article or cluster,
   - primary intent,
   - 8 to 14 sub-queries,
   - internal link targets,
   - product card and category card matched to the problem.
3. Run `tools/vevo_duplicate_guard.py`.
4. Run `tools/vevo_public_content_guard.py` before publication.
5. Run `tools/vevo_article_depth_guard.py` on the generated batch JSON. New expert/practical articles should normally be at least 1500 visible words each. This guard also blocks broken HTML structure such as long runs of one-character paragraphs.
6. Replace or narrow all `REVIEW` titles; stop on `BLOCK`.
7. Create XLS import with simple HTML and fields:
   - `title`
   - `short`
   - `long`
   - `date_posted`
   - `time_posted`
   - `active`
   - `link`
   - `commenting`
8. Import XLS into BiznisWeb news block `765`.
9. Resolve real public URLs and post IDs from the frontend/RSS.
10. Update each news post through the API with final rich HTML.
11. Delete temporary remote XLS file.
12. Verify every new article:
   - HTTP 200
   - correct title
   - styled HTML present
   - no escaped quote artifacts
   - no malformed `href`
   - all internal links return HTTP 200
13. Save mapping/export notes and update `PROJECT_STATE.md`.

## Hard Duplicate-Safety Rules

- Never run `biznisweb-add_news_post` twice for the same prepared article because the first response was hard to parse.
- After every create call, parse the response from `result.content[0].text`, extract the real `news_post.id`, and write that ID to a mapping/export file before doing anything else.
- If the create response is missing, null, malformed, or unclear, stop the batch and inspect the admin/API state before any retry.
- When using `biznisweb-add_news_post` outside the XLS import flow, create posts as hidden drafts only. Final public slug and date must be completed through the admin UI or another verified slug/date-safe workflow.
- Before creating any new post, check the current batch mapping/export files for the title and slug. If either already has a post ID, update that post or stop; do not create a second post.
- If a duplicate is discovered, delete the extra post ID immediately, record the cleanup in `exports/`, and update `PROJECT_STATE.md`.

## VEVO Article Rules

- No fixed product prices.
- No customer-facing wording for internal marketing acronyms.
- No customer-facing internal SEO/workflow words such as `longtail`, `keyword`, `SEO`, `search intent`, `sub-query`, `fan-out`, `CTA`, or phrasing like "cielene pokrývame".
- Include internal links to existing VEVO categories, products, and relevant articles.
- Include product/category recommendation cards where relevant.
- Use richer HTML: quick answer, callout, steps, table, recommendation card, FAQ.
- Do not publish if the article body contains repeated one-character paragraphs such as `<p>P</p><p>r</p><p>e</p>`; this means a generator rendered a string as individual characters.
- For daily batches, prefer 2 to 3 longer articles over 20 thin articles.
- Standard expert/practical articles should normally be at least 1500 visible words, with pillar articles at 2200+ visible words.
- Follow `workflows/article-quality-and-sales-blocks.md` for quality gates and product/category block patterns.
- Use the latest relevant fan-out file under `content-plan/` before creating a batch.
