# Reporting status identity audit

Date: 2026-09-13. Scope: VEVO reporting after order statuses were renamed. This is a source audit, not a production report verification.

## Source and evidence

Protected task: `vevo-reporting-daily:33`; source `e55ccd14b47c660b9b39a5788a1e65a63a98fc1a`; image `sha256:30a23fcd69eb2d7a41195bffa0bc055d38bc2dd706e9eb07d5126675a21a6add`. These identities also appear in the separately reviewed creditnote migration's `REPORT_PINS`. This audit read exact Git source and existing evidence without another AWS or provider query.

Private evidence is under `data/vevo/order-automation/audits/2026-09-13/` in the private reporting bucket. Customer data is not copied here.

| File | SHA-256 | Supported fact |
| --- | --- | --- |
| `status-review-user-restored-readback-20260913.json` | `c4621d2d3792a53320706f2bcfee8429b5d443e69a3eabd250ad14f7029c1aea` | Raw API ID 4 was `Odoslaná` at 06:14 UTC. |
| `native-creditnote-invoice-key-contract-20260913.json` | `523a2c2d5c7677f4e0889cdffed116e863865e814090c79207c7b904171dc70e` | Raw VEVO API ID 17 was `Storno`. |
| `status-review-two-case-api-20260913.json` | `f7b1b0ddd42a7fd038088e94e235fcc2e812affb6283a1362d95ad6b4f9dc5cc` | Raw API ID 33 was `Platba online - platnosť vypršala`. |
| `gopay-native-status-guard-inspection-20260913.json` | `4c28ff3ace67ae583249ef85a5f01aeef9c5128608b79a6d8e67ea70d00499bc` | Native events PAID→31, TIMEOUTED→33, CANCELED→34. Labels are explicitly transliterated; 34 was described as rejected payment, not proof of the literal label “cancelled”. |
| `current-native-status-catalogue-20260913.json` | `879d05d452b7a29842fca578278b841b63f9340a4ffc8d8bcbac195fcaf0952b` | Current same-shop catalogue has 4 `Shipped`, 17 `Cancelled`, 31 `Payment online - paid`, and both 33/34 `Payment online - expired`. New Stripe states use distinct IDs 69–73. |

Fresh native Stripe settings proof `current-stripe-status-map-ui-20260913.json`, SHA-256 `250a22f46db21b120611adf473ddfaba0d651b813800b688497715a0c4f68b2f`, was captured read-only at 08:24:30 UTC under the same private prefix. Its visible table binds PAID to ID 31, CANCELLED to 34, EXPIRED to 33, REFUNDED to 73 and UNPAID to 69. This is the current same-shop mapping, superseding earlier planned Stripe IDs; it does not prove a successful callback or transaction.

Unchanged order/status IDs with changed labels and matching current API/native catalogues support a shop status rename, not a client-language transport defect. No VEVO role is inferred from a ROY ID. Duplicate labels on 33/34 must not merge their roles. This audit did not independently prove the old ID 1 label.

## Regressions in immutable source e55ccd14

- `projects/vevo/settings.json:126` retains old paid, COD and fulfilled names. `export_orders.py:3243` compares normalized names and returns `non_realized_status` for unmatched labels, ignoring IDs. Renamed paid/shipped orders disappear from realized revenue, including historical orders fetched with current labels. Correct payment IDs do not rescue an unmatched status.
- `export_orders.py:5532` records failed-payment segmentation using exact old labels. Renamed 33/34 orders remain excluded from revenue but disappear from this segment.
- `creditnote_export.py:585` recognizes shipment from current or historical names; its defaults omit `Shipped`. Without an old-name audit entry, this changes sent-creditnote classification and fulfillment-cost inclusion at `export_orders.py:5918`. Historical evidence must remain readable.
- The inline guard resolves `Storno` before evaluating creditnoted orders (`creditnote_storno_guard.py:277`; imported resolver `unpaid_order_cancellation.py:552`). A missing target raises, and `daily_report_runner.py:245` propagates it before export. Its skip flag at line 1499 bypasses only that mutation step, not report classification.
- The lifecycle fallback at `export_orders.py:3820` recognizes English shipped/cancelled/expired labels, but its `paid` substring classifies `Stripe - unpaid` as `paid_processing`. An unpaid label is not settlement evidence.
- The production board is an independent affected reader: `production_board.py:317` builds its own API client, line 356 reads orders directly, and `_is_active_order` at line 159 compares only configured names. Renamed paid orders disappear from manufacturing orders/units. The same predicate controls active-page counts and early termination at line 364, so the scan can stop despite paid orders. `live_dashboard_server.py` calls this reader directly; it does not pass through the exporter. A report-only correction does not fix this live dashboard. Record a separate reader correction and verified dashboard release; do not silently broaden this branch into dashboard deployment.

Offline verification extracted and executed exact-source pure methods with real VEVO settings, without importing runtime clients. Three same-ID old/new pairs (paid, shipped COD, shipped prepaid) changed from included to `non_realized_status`. Both failed-payment pairs changed segment count from one to zero. The unpaid lifecycle defect was reproduced. Live numerical impact requires a later bounded report comparison.

## Compatible correction

Consume the independently reviewed project-scoped status identity contract after its clean pushed commit is available. Bind supported roles to their own shop and stable IDs while preserving display labels and the native catalogue. Apply it at realized-revenue decisions, failed-payment segmentation and current/historical shipped-creditnote classification. Preserve payment metadata/ID checks, exact audited overrides, cost rules and historical evidence. Global English aliases must not grant an unrelated status the paid/shipped role.

Tests must cover same-ID old/new parity, foreign IDs or another shop sharing a label, unknown/malformed identity, distinct 33/34 roles, current/historical shipped creditnotes and unpaid versus paid lifecycle classification. ROY behavior must remain unchanged. Reporting policy does not authorize payment changes or establish bank settlement from an invoice.

The branch now consumes the reviewed shared contract from `10d705eeb3d9549dd0383f9460468fa51fe6041d`. Live VEVO acquisition binds its same-shop catalogue before inventory or cached rows are classified. Period child exporters receive the same verified catalogue without extra catalogue reads. Raw order/cache/display rows remain unchanged; classification uses canonical copies. Unknown inactive IDs cannot receive a reviewed paid/shipped role by name. ROY keeps its existing project policy.

Realized revenue, failed-payment segmentation and shipped-creditnote decisions share the binding. Historical shipped evidence with a status ID uses the same contract; existing old-name-only evidence remains readable without treating a new unbound English label as proof. Internal creditnote contexts carry the bound client only for local classification; exported audit rows and summaries do not serialize it. Acquisition never invokes invoice, status or email mutations.

The `Stripe - unpaid` presentation correction uses a separate `_report_lifecycle_bucket` wrapper. The original `_classify_lifecycle_bucket` and the `order_facts_only=True` no-client path remain unchanged because frozen GrowthBook acquisition consumes them. This branch does not retroactively reinterpret that measurement evidence. Live report role classification and frozen experiment facts therefore remain explicit separate contracts; any future measurement correction needs its own reviewed version.

Current Stripe PAID is mapped to the already reviewed ID 31, so the repaired report classification covers that configured paid path. The new ID 70 `Stripe - paid` appears in the catalogue but is unused by the observed current mapping; it stays excluded instead of gaining a paid role from its label. No supported historical event mapping for ID 70 was established by this audit. Tests distinguish the configured 31 path from 70. Successful gateway callback processing and actual transaction delivery remain unverified; no gateway setting or payment is changed here.

A code change needs a separate immutable reporting image and managed release after the primary order fix and separate guard migration are verified. No report schedule, GrowthBook pin or production board is changed here.

## GrowthBook current-runtime boundary

At base main `4d5049805f1a8eabd5f7c642ccee7ca6867d3c08`, five live workflows pin source reporting to `vevo-reporting-daily:33`:

| File under `.github/workflows/` | Gate |
| --- | --- |
| `monitor-vevo-growthbook-production-aa-infra.yml` | Line 34 expected revision; lines 238–246 bind deployed source evidence. |
| `check-vevo-growthbook-production-aa-window.yml` | Line 298 exact source revision. |
| `check-vevo-growthbook-production-cta-window.yml` | Line 324 exact source revision. |
| `check-vevo-growthbook-production-cta-safety.yml` | Line 172 exact source revision. |
| `build-vevo-growthbook-production-cta-final-snapshot.yml` | Line 231 exact source revision. |

`scripts/validate_growthbook_production_aa_activation.py:538` includes revision 33 in historical reconciliation evidence; lines 696–705 verify its original hash/content, and line 718 checks exact activation evidence. Those receipts describe their original run. Do not rewrite them to pretend a later reporting image was present then.

The standalone guard migration adds only a scheduler environment override to skip the inline guard and preserves reporting task/image pins. A later reporting revision is a separate change and will fail the five current-runtime gates until a reviewed migration binding is integrated. Prefer one explicit current reporting-runtime binding backed by its managed release receipt and consumed consistently by live checks. Preserve historical activation/reconciliation proofs and all collector, reconciliation, experiment/window and result-blind boundaries.

Do not deploy through the existing `production-reporting-smoke.yml` path using `latest` and promotion before required verification. A future release must bind exact source/image, verify task ID + IP + service + `/app`, prove an actual localhost marker and read-only report comparison, then promote. That release is outside this initial audit.
