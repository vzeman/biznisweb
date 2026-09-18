# VEVO MCP Publication Workflow

## Scope

Use the repo-local `biznisweb-vevo-content` MCP server for VEVO Blog page `309`, news block `765`. It is separate from the legacy remote `biznisweb-vevo` news tools that generated repeated-`1` slugs and must not be used for new public articles.

## Secrets

Preferred local file: `content/VEVO_CONTENT/.env` based on `.env.example`. Never commit it.

Required for article writes:

- `BIZNISWEB_ADMIN_BASE_URL`
- `BIZNISWEB_USERNAME`
- `BIZNISWEB_PASSWORD`

`BIZNISWEB_API_URL` and `BIZNISWEB_API_TOKEN` are optional and only extend the read-only smoke with GraphQL language/product checks. `VEVO_CONTENT_ENV_FILE` may point the MCP server to an existing untracked VEVO env file.

## Gates

1. Run the normal VEVO batch check with candidate and article files.
2. Run the publisher without a live flag. It scans up to 2,000 admin rows and requires every new public slug to return `404`.
3. On a new machine or after changing the MCP helper, run `--smoke`. The disposable post must be hidden, preserve the exact slug and HTML marker, return public `404`, and be deleted with zero remaining admin matches.
4. Run `--publish`. Every article is created hidden, its post ID is persisted, admin title/slug/HTML is read back, and only then is the same post published with explicit confirmation.
5. Run the batch-specific public verifier. Publication is complete only when its report has `all_ok=true`.

The candidate duplicate check is a pre-publication gate. After successful publication, rerunning it against the same candidates must fail because those exact slugs now exist; use the independent public verifier for post-publication evidence.

## Commands

```powershell
# Read-only preflight for the default prepared batch
python -X utf8 content\VEVO_CONTENT\imports\publish_vevo_batch_via_content_mcp.py

# Disposable hidden create/readback/delete test
python -X utf8 content\VEVO_CONTENT\imports\publish_vevo_batch_via_content_mcp.py --smoke

# Live hidden-first publication
python -X utf8 content\VEVO_CONTENT\imports\publish_vevo_batch_via_content_mcp.py --publish
```

For a later batch:

```powershell
python -X utf8 content\VEVO_CONTENT\imports\publish_vevo_batch_via_content_mcp.py `
  --articles content\VEVO_CONTENT\imports\batch-NN-YYYY-MM-DD-articles.json `
  --report content\VEVO_CONTENT\exports\batch-NN-YYYY-MM-DD-mcp-publication.json `
  --publish
```

## Resume Rule

The publication report is the mapping from slug to admin post ID. If a run stops, rerun the same command with the same report. Never delete the report and retry creation. An exact admin match without a recorded mapping is a stop condition and must be resolved before another create call.

## Failure Rule

- Do not publish if the hidden readback changes the title, slug, rich HTML, or visibility.
- Do not repeat a create after an ambiguous response.
- Do not delete or repair unrelated historical duplicates as part of a new batch.
- Keep public URL verification independent from the MCP server response.
