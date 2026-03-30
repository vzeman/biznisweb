# PROJECT_STATE

Last updated: 2026-03-30
Owner: Patrik
Purpose: Single source of truth pre priebezny stav, aby sa nestracal kontext medzi session.

## 0) How To Use This File

- Tento subor je centralny prehlad pre vsetky 3 produkty: OpenClaw, Doklady, Reporting.
- Po kazdom vacsom kroku aktualizuj primarne sekciu "Project Update Sections" pre dany projekt.
- Nezapisuj kazdy drobny commit, zapisuj rozhodnutia, zmeny smeru, overenia a dalsi presny krok.

## 1) Portfolio Map

### Doklady
- Type: multi-tenant web app (MVP), session-based
- Core purpose: zber, analyza, kontrola, archivacia uctovnych dokladov
- Inputs: manual upload + email intake
- Outputs: Google Drive originals + CSV evidence + app history + DPH sums
- Core files:
  - `server.js`
  - `imap-intake.js`
  - `doklady-v3 (2).html`

### OpenClaw
- Type: agent runtime/orchestration
- Current host:
  - EC2 instance: `i-00dc7eeb66278c47e`
  - Public IP: `98.88.77.70`
  - Gateway port: `18789`
- Local launcher:
  - `C:\Users\Patrik jankech\Desktop\openclaw-tunnel.bat`

### Reporting
- Type: reporting platform
- Active domains/clients: VEVO, ROY
- Goal: reusable reporting layer predajna aj pre dalsich klientov

## 2) Repositories And Branches

### biznisweb
- Local path: `C:\Users\Patrik jankech\Desktop\biznisweb`
- Remote: `https://github.com/vzeman/biznisweb.git`
- Active branch for OpenClaw work: `opan-claw`

### Playground
- Local path: `C:\Users\Patrik jankech\Documents\Playground`
- Note: lokalny workspace bez GitHub remote pre tento tok prace

## 3) Architecture Direction (Agenti + SaaS)

Target model:
- Doklady = system of record
- OpenClaw = execution layer (agents)
- Agenti nikdy nezapisuju data mimo Doklady backend API

Isolation baseline:
- 1 tenant = izolovane data, secrets, audit trail
- Tenant-sensitive data sa urcuju zo session/service identity, nie z client tenantId
- Cross-tenant leakage = blocker

## 4) Non-Negotiable Invariants (Do Not Break)

1. Google OAuth je login/session vrstva, nie email import vrstva.
2. 1 Google login = 1 tenant.
3. 1 tenant = 1 import email adresa (admin-managed).
4. 1 tenant = 1 mailbox config (v aktualnom MVP).
5. Tenant-sensitive endpointy nesmu verit client tenantId ako source of truth.
6. IMAP candidates musia byt filtrovane podla `tenantId` + route match:
   - `routing.importEmail`
   - `routing.mailboxUser`
7. Dedupe nesmie blokovat novy import len kvoli starym `skipped/processed` candidate.
8. Reset musi mazat aj serverovy tenant stav (nie len local browser state).

## 5) Current OpenClaw Ops State

- Gateway auth mode: token
- Known symptom fixed: `unauthorized: gateway token missing`
- Required access pattern:
  - `http://127.0.0.1:18789/#token=<token>`
- Service persistence:
  - `openclaw-gateway` is enabled/active
  - user linger enabled on host

## 6) Project Update Sections (Primary Working Area)

### OpenClaw - Last Updates
- Last update: 2026-03-30
- Status: operational baseline ready
- Recent changes:
  - fixed tunnel/token flow for dashboard auth
  - stabilized launcher script with health checks and gateway restart
  - enabled persistent user service behavior on server (`linger=yes`)
- Next exact step:
  - build first workflow `manual-document-collector`
- Blocking issues:
  - none at this moment
- Decision log:
  - OpenClaw stays on separate server from Doklady, integration over secured API

### Doklady - Last Updates
- Last update: 2026-03-30
- Status: MVP stable for current internal use
- Recent changes:
  - tenant isolation and IMAP route-matching invariants defined as hard rules
  - dedupe behavior clarified for re-import scenarios
- Next exact step:
  - add Agent Jobs API contract (`/agent/jobs`, `/agent/jobs/:id/events`, `/agent/jobs/:id/result`)
- Blocking issues:
  - no relational DB yet (JSON storage limitation)
- Decision log:
  - Doklady remains the only system of record for accounting document state

### Reporting - Last Updates
- Last update: 2026-03-30
- Status: active MVP usage on VEVO + ROY
- Recent changes:
  - strategic direction confirmed: reusable layer for additional clients
  - added env governance baseline: `.env.required` + local pre-commit + CI env-check
- Next exact step:
  - define shared reporting core vs tenant-specific adapters
- Blocking issues:
  - architecture boundaries between reusable core and client custom logic not fully formalized yet
- Decision log:
  - reporting must stay productized and separable from one-off client custom code

## 7) Execution Queues By Project

### OpenClaw Queue
1. Implement `manual-document-collector` agent workflow.
2. Add human approval gate before final write to Doklady.
3. Add per-tenant execution/audit metadata for each run.

### Doklady Queue
1. Add Agent Jobs API endpoints.
2. Add service-to-service auth with signed token incl. `tenant_id`.
3. Add immutable audit logs for agent actions.

### Reporting Queue
1. Map reusable KPI/reporting core modules.
2. Define tenant-specific config model for client rollout.
3. Prepare SaaS packaging boundaries and rollout checklist.

## 8) Risks / Watchlist (Global)

- Frontend in Doklady is monolithic single HTML + inline JS (maintenance risk)
- JSON file storage in Doklady (no relational DB yet)
- Browser-side Anthropic calls (security + governance risk)
- Mailbox credentials currently in JSON store (needs hardened secrets strategy)

## 9) Session Handoff Template (Copy Per Session)

Use this block at end of each major session:

```text
Date:
Project: OpenClaw | Doklady | Reporting
Repo + branch:
What was changed:
Why it was changed:
What is verified:
Known issues:
Next exact step:
```

## 10) Change Log

### 2026-03-30
- Created unified PROJECT_STATE baseline.
- Added per-project sections for OpenClaw, Doklady, Reporting.
- Confirmed branch strategy for OpenClaw work in `biznisweb` on `opan-claw`.
- Captured OpenClaw token/tunnel operational constraints.
- Added env governance (required env keys + pre-commit hooks + CI check).

## 11) Update Policy

- Rule: Po kazdom vacsom kroku alebo po ukonceni session aktualizujeme PROJECT_STATE.md.
- Minimum update: datum, co sa zmenilo, co je overene, dalsi presny krok.
- Ak sa zmeni infra/auth/tenant logika, update je povinny v ten isty den.
- Ak vznikne novy oddeleny projekt s vlastnym GitHub repom, musi mat vlastny PROJECT_STATE.md v danom repozitari.
- Tento PROJECT_STATE.md sa potom neexpanduje o detailny stav noveho projektu; ponecha iba kratke integration notes, ak su relevantne.

## 12) Git Hygiene Protocol (Multi-PC, Mandatory)

- Pracuj oddelene po repozitaroch: reporting, doklady, openclaw-agenti.
- Pred zaciatkom prace na kazdom PC: `git pull --rebase` na aktivnej vetve.
- Po kazdom vacsom kroku: maly commit + okamzity `git push`.
- Ziadny `force push` na zdielane vetvy.
- `main` iba cez PR a kontrolovane merge.
- Cross-project integraciu ries cez verziovany API kontrakt (nie neformalnou dohodou).
- V kazdom repozitari udrzuj vlastny `PROJECT_STATE.md` aktualny.
