# OPERATING PROTOCOL

## Purpose

This file defines the mandatory working model for all development across Reporting, Doklady, and OpenClaw.
The goal is to keep one source of truth, avoid cross-PC drift, and prevent branch, runtime, or secret chaos.

## Sources Of Truth

- GitHub is the only source of truth for code.
- AWS servers are the source of truth for runtime.
- `PROJECT_STATE.md` is the source of truth for current repo state and next exact step.
- Local `Desktop`, `Downloads`, ad-hoc scripts, and copied folders are not sources of truth.

## Project Map

- Reporting: `biznisweb` repo, branch `main`
- Doklady: `biznisweb` repo, branch `codex/doklady-saas-clean`
- OpenClaw: `openclaw-agents-platform` repo, branch `main`
- Temporary legacy branch: `biznisweb` repo, branch `opan-claw`

## Session Start Rules

Before any work on any PC:

```bash
git fetch --all --prune
git status
git branch --show-current
git pull --rebase
```

Then:
- open `PROJECT_STATE.md`
- verify the current branch is correct
- continue only if the working tree is understood and clean enough for the task

## Session End Rules

After every meaningful change:

```bash
git status
git add ...
git commit -m "precise message"
git push
```

Then update `PROJECT_STATE.md` with:
- what changed
- what was verified
- known issues or blockers
- the next exact step

## Multi-PC Rules

- Do not use local folder sync as the source of truth.
- Do not keep critical scripts only on one PC.
- Do not keep required runtime knowledge only in chat.
- Do not start work on the second PC until the first PC has committed and pushed.
- Do not work on the same branch from both PCs at the same time.
- Switching PCs is allowed only after push on PC A and pull on PC B.

## Repo Hygiene Rules

- Anything required for build, deploy, tunnel, bootstrap, or runtime operations must live in Git.
- Secrets, private keys, and `.env` runtime files must not be committed.
- Only examples and env contracts belong in Git:
  - `.env.example`
  - `.env.required`
- Hooks and bootstrap must be repo-local and reproducible from the repository.

## Safety Rules

- Never trust tenant identity from browser-provided input in Doklady.
- Never allow cross-tenant data visibility.
- Never use force-push on shared working branches.
- Never deploy from files outside the repository.
- If tenant isolation or account separation looks wrong, stop implementation and fix that first.

## Bootstrap Rule

A new machine is considered ready only if:
- the repo can be cloned from GitHub
- bootstrap runs successfully
- hooks are installed
- env validation passes
- runtime access is documented and reproducible

If a machine cannot be rebuilt from repo plus documented secrets, the setup is not finished.

## Handoff Rule

Every handoff between PCs or sessions must include:
- repo
- branch
- latest commit
- what changed
- what is verified
- next exact step

## Short Principle

If the current state cannot be reconstructed from Git, `PROJECT_STATE.md`, bootstrap scripts, and documented secrets, it is not done.
