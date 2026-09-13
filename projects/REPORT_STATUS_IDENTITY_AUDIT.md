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
| `current-native-status-catalogue-20260913.json` | `879d05d452b7a29842fca578278b841b63f9340a4ffc8d8bcbac195fcaf0952b` | Current same-shop catalogue has 4 `Shipped`, 17 `Cancelled`, 31 `Payment online paid`, and both 33/34 `Payment online expired`. New Stripe states use distinct IDs 69–73. |

Unchanged order/status IDs with changed labels and matching current API/native catalogues support a shop status rename, not a client-language transport defect. No VEVO role is inferred from a ROY ID. Duplicate labels on 33/34 must not merge their roles. This audit did not independently prove the old ID 1 label.

## Regressions in immutable source e55ccd14

- `projects/vevo/settings.json:126` retains old paid, COD and fulfilled names. `export_orders.py:3243` compares normalized names and returns `non_realized_status` for unmatched labels, ignoring IDs. Renamed paid/shipped orders disappear from realized revenue, including historical orders fetched with current labels. Correct payment IDs do not rescue an unmatched status.
- `export_orders.py:5532` records failed-payment segmentation using exact old labels. Renamed 33/34 orders remain excluded from revenue but disappear from this segment.
- `creditnote_export.py:585` recognizes shipment from current or historical names; its defaults omit `Shipped`. Without an old-name audit entry, this changes sent-creditnote classification and fulfillment-cost inclusion at `export_orders.py:5918`. Historical evidence must remain readable.
- The inline guard resolves `Storno` before evaluating creditnoted orders (`creditnote_storno_guard.py:277`; imported resolver `unpaid_order_cancellation.py:552`). A missing target raises, and `daily_report_runner.py:245` propagates it before export. Its skip flag at line 1499 bypasses only that mutation step, not report classification.
- The lifecycle fallback at `export_orders.py:3820` recognizes English shipped/cancelled/expired labels, but its `paid` substring classifies `Stripe - unpaid` as `paid_processing`. An unpaid label is not settlement evidence.

Offline verification extracted and executed exact-source pure methods with real VEVO settings, without importing runtime clients. Three same-ID old/new pairs (paid, shipped COD, shipped prepaid) changed from included to `non_realized_status`. Both failed-payment pairs changed segment count from one to zero. The unpaid lifecycle defect was reproduced. Live numerical impact requires a later bounded report comparison.

## Compatible correction

Consume the independently reviewed project-scoped status identity contract after its clean pushed commit is available. Bind supported roles to their own shop and stable IDs while preserving display labels and the native catalogue. Apply it at realized-revenue decisions, failed-payment segmentation and current/historical shipped-creditnote classification. Preserve payment metadata/ID checks, exact audited overrides, cost rules and historical evidence. Global English aliases must not grant an unrelated status the paid/shipped role.

Tests must cover same-ID old/new parity, foreign IDs or another shop sharing a label, unknown/malformed identity, distinct 33/34 roles, current/historical shipped creditnotes and unpaid versus paid lifecycle classification. ROY behavior must remain unchanged. Reporting policy does not authorize payment changes or establish bank settlement from an invoice.

Initially this branch contains documentation only. The shared contract is an implementation dependency. A code change needs a separate immutable reporting image and managed release after the primary order fix and separate guard migration are verified.

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
