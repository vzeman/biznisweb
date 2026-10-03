# VEVO_CONTENT

Brand: VEVO
Domain: vevo.sk
Purpose: blog, FAQ, SEO articles, product-led articles, internal linking, content batch workflow.

## Source Of Truth

The canonical Git remote is `https://github.com/vzeman/biznisweb.git`. This folder is the source of truth for VEVO content operations in that repository. Work on a dedicated `codex/vevo-content-*` branch and merge to `main` only through a pull request.

Project-owned files:

- Content plan: `content-plan/vevo-5000-content-plan.md`
- Duplicate guard: `tools/vevo_duplicate_guard.py`
- Project state: `PROJECT_STATE.md`
- Batch notes and candidate lists: `batches/`
- Import/export notes: `workflows/`, `imports/`, `exports/`

## Rules

- Do not use fixed product prices in articles.
- Run the VEVO duplicate guard before every new batch.
- Treat both `BLOCK` and `REVIEW` as stop conditions until the candidate is replaced or manually justified. The guard checks live RSS, the FAQ page, and locally prepared article batches.
- Use clean slugs and verify the real public URL after publishing.
- Keep VEVO products, categories, internal links, and workflow separate from ROY.
- Do not use customer-facing acronym `CTA` inside articles.
- Current publishing rule: current publish date is allowed unless the user explicitly asks for another date; clean URL slugs are mandatory.
- Preferred publication path: repo-local `tools/biznisweb_vevo_content_mcp.py` registered as `biznisweb-vevo-content`. It writes the explicit `link`, creates hidden first, performs admin readback, scans the full news block for exact duplicates, and requires a separate confirmed publish update.
- The legacy remote VEVO MCP `biznisweb-add_news_post`/`biznisweb-update_news_post` tools remain unsafe for final public article creation because they cannot preserve explicit slugs. Do not confuse them with `biznisweb-vevo-content`.
- Duplicate safety: never repeat a news-post create call for the same article after a null or malformed response. First resolve whether the post was created and record the post ID.
- Before the first live batch in a new environment, run the repo-local hidden MCP smoke. It must preserve the exact slug and rich HTML, remain publicly `404`, delete the test post, and leave no admin match.
- Use `imports/publish_vevo_batch_via_content_mcp.py` for resumable preflight and publication. It is read-only unless `--smoke` or `--publish` is explicit and persists every created post ID before continuing.
- Every script capable of creating, updating, or deleting live news must be read-only by default or require an explicit live flag such as `--execute-live`, `--update-live`, or a similarly unambiguous opt-in.
- For rich HTML in the old BiznisWeb admin, use TinyMCE source mode for the long body, paste the full HTML into the source textarea, exit source mode, fill SEO `title_tag`, `link`, and `description`, save, then verify the public URL.
- After publication, run the batch-specific public verifier. A successful admin save is not publication evidence.

## Batch Check

Run this before a VEVO batch import:

```powershell
.\content\VEVO_CONTENT\scripts\check.ps1 `
  -CandidatesFile content\VEVO_CONTENT\batches\<candidate-file>.txt `
  -ArticlesFile content\VEVO_CONTENT\imports\<article-file>.json
```

The batch check runs regression tests, project hygiene, live/local duplicate detection, public wording, article depth, and HTML safety. Link preflight and final public URL verification remain mandatory evidence files for each batch.

## MCP Publication

The complete workflow is documented in `workflows/mcp-publication.md`. Typical commands are:

```powershell
python -X utf8 content\VEVO_CONTENT\imports\publish_vevo_batch_via_content_mcp.py
python -X utf8 content\VEVO_CONTENT\imports\publish_vevo_batch_via_content_mcp.py --smoke
python -X utf8 content\VEVO_CONTENT\imports\publish_vevo_batch_via_content_mcp.py --publish
```

For another batch, pass explicit `--articles` and `--report` paths. A completed MCP report is still followed by the batch-specific public verifier.

## Handoff Template

Date:
Repo:
Branch:
Project: VEVO_CONTENT
What changed:
What is verified:
Known issues:
Next exact step:
