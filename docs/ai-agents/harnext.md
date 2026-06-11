# Harnext integration

Harnext is an additional context and quality guard for pull requests. It does not replace Codex, local tests, human review, or the production hard-gates in `PROJECT_STATE.md`.

## Current mode

- Scope: Reporting repository only (`vzeman/biznisweb`).
- Workflow: `.github/workflows/harnext-context-review.yml`.
- Prompt: `.github/harnext/prompts/context-review.md`.
- Trigger: same-repository non-draft pull requests.
- Runner: GitHub-hosted Ubuntu runner.
- Harnext version: `1.6.1`.
- Provider: OpenAI via `OPENAI_API_KEY`.
- Permission mode: `plan`, so Harnext can read context but should not edit files or run shell commands.

## Safety rules

- Do not run Harnext workflows on fork pull requests with repository secrets.
- Do not use a self-hosted runner for Harnext until the repo approval gate and runner isolation are explicitly reviewed.
- Do not let Harnext deploy, rotate secrets, write runtime state, or mutate production data.
- Keep Harnext prompts and workflows in Git. Do not rely on local Desktop/Downloads scripts.
- Keep Harnext runtime output outside Git. `.harnext/` is local/runtime state.

## Required secret

The workflow requires the GitHub repository secret `OPENAI_API_KEY`.

```bash
gh secret set OPENAI_API_KEY --repo vzeman/biznisweb
```

Do not commit this value. If the key was ever pasted into a tracked file or chat, rotate it.

## Operating model

1. A same-repo pull request becomes ready for review.
2. GitHub Actions builds a small context pack under `.harnext/context/`.
3. Harnext reads `PROJECT_STATE.md`, `README_DEV.md`, the PR metadata, changed file list, and diff.
4. Harnext posts a concise PR comment with possible defects and verification gaps.
5. Humans decide whether to block, fix, or merge.

## Not enabled yet

Full `harnext setup` is intentionally not enabled for this repo yet. That wizard can write pipeline workflows, create labels, and configure runners. Enable it only from a clean short-lived branch after:

- `git fetch --all --prune` succeeds
- `git pull --rebase` or the equivalent explicit upstream rebase succeeds
- the branch is clean
- `OPENAI_API_KEY` is present as a repo secret
- self-hosted runner risk is reviewed if any stage would run on this PC

The local `Playground` directory is not a valid Harnext source of truth until it has a GitHub remote, upstream, and reconciled history.
