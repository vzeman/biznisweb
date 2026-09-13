# Reviewed noncollection and preserved financial uncertainty

This fixed incident tool closes one privately reviewed COD invoice obligation after the user explicitly confirms noncollection. It never interprets an absent invoice as proof that a prior preparation request failed. The original financial phase, attempt time, failure and document/email fields remain unchanged, with `financial_outcome: unknown` recorded separately.

The confirmation is private S3 evidence at `data/vevo/order-automation/audits/2026-09-13/reviewed-uncollected-confirmation-20260913.json`, pinned by raw and canonical SHA `5b6ea4ae7b557003a58d7848b880120169c893f5d7e027a3be581ed813e6a8d0`. Public fixtures use synthetic order identities. Scope changes require a new explicit review and code change; the CLI accepts no arbitrary order number, status or amount.

## Deployment order

1. Review and merge the runtime closure validator and all entrypoint/backlog/status gates together with compatible manual-settlement and creditnote changes. The earlier production image does not understand closure markers.
2. Build and promote that exact commit through the managed deployment workflow. Verify actual Fargate task identity, private IP, service, `/app`, localhost marker, stopped exits and immutable digest; preserve all five invoice/cancellation schedules and reporting pins.
3. Independently review the promotion receipt and direct AWS state. Only then replace the fixed helper's empty `REVIEWED_RELEASE_COMMIT` and `REVIEWED_RELEASE_DIGEST` constants in a reviewed, committed and pushed source change. Empty pins intentionally block `--apply` before credentials or provider calls.
4. Run the fixed helper preview from a clean pushed `codex/` branch. Preview uses authenticated read-only native/API routes, checks source/privacy and does not acquire the journal lease or change business records.
5. Apply only with the exact reviewed release and private deployment receipt. The helper repeats release checks, acquires the shared VEVO lease, verifies fresh context, records a consumed intent, and makes at most one silent status request. A crash after intent never permits a second request.
6. Read back the journal and native/API state, then run the original seeded-backfill audit and the all-age post-recovery audit. The former reports the closed obligation with financial outcome still unknown. Raw read-only discovery does not claim journal-aware completeness.

Example arguments, after the compatible release has been pinned:

```text
python scripts/close_reviewed_uncollected_obligation.py --profile codex
python scripts/close_reviewed_uncollected_obligation.py --profile codex --apply --expected-source-commit <reviewed-promoted-commit> --expected-image-digest <reviewed-sha256-digest> --deployment-evidence-key <private-promotion-receipt-key>
```

## Fresh gates and crash recovery

The private confirmation binds exact project, internal and public order identities, COD reference, original status, amount/currency and original financial projection. Current API documents must be explicitly empty (null or an empty list), with complete unpaid evidence. The native invoice grid must independently return zero documents. The native per-order cash-receipt route must return exactly `{rows: []}`; there is no invented `total` field. Its observed read-only contract is pinned by SHA `2104d03fe34ccaac3dd3a3c280a0e807179a959b12c9198ab1e17471613c75db`. Positive receipts, unknown responses, delivery contradictions, incomplete shipment context, changed totals/payment method/status or an unresolved prior status operation stop the tool.

The target is the unique supported noncollection/storno status, falling back to unique `Storno` only when the first target is absent. Catalog ambiguity stops the tool. The journal stores the exact source and target before transmission. Only independent fresh target/readback confirmation closes the obligation. Pending or uncertain intents remain active review and allow readback reconciliation only. The tool never sends an invoice, customer email, payment, receipt or refund request.

A valid closed marker is excluded from active invoice backlog and separately counted by `InvoiceStandaloneReviewedClosed`. Any marker, including malformed or cross-scope markers, blocks automatic financial and status writes. Invalid/pending markers remain active review; later document/status/amount drift is sticky review (`InvoiceStandaloneReviewedClosureReview`) and cannot restore fulfillment or reopen invoicing automatically. Clearing such review requires a separate explicit investigation.

Financial `InvoiceGenerator` calls now require an actual owned `AutomationJournal`; callers without it receive `AutomationStateError`. Direct email sends additionally require a fresh single-use authorization from the journaled send path, so an old `sending` record cannot be replayed. Native/API read-only inspection remains available without a journal. No long-lived local runtime is needed by this workflow.
