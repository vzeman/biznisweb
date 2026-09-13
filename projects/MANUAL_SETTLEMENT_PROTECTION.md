# Verified external bank settlement

Status, payment method, invoice existence and payment-gateway attempts are separate facts. A timed-out Stripe/GoPay attempt does not disprove a separately confirmed bank transfer. Conversely, a named history actor, bank-transfer method or earlier paid status cannot establish that a transfer arrived.

The preferred workflow records the actual received amount/date/type through FLOX **Faktúry → Uhradiť**, which creates the native receipt. Follow [BiznisWeb's invoice-management documentation](https://www.biznisweb.sk/a/90/sprava-faktur-fakturacia). Never invent a receipt or transfer date to accommodate a status-only workflow. Automatic recovery still requires current safe fulfillment and complete creditnote context.

When an authorized operator has an explicit user confirmation of full external bank settlement but no native receipt, record a private attestation. This records a fact, not a provider payment or status change. The generic rule works with both Stripe and GoPay and does not depend on the current selected payment method.

## Exact private confirmation schema

Use the exact fields validated by `manual_settlement.validate_proof`:

- `schema_version`: integer `1`; `operation`: `confirm`; `project`: `roy` or `vevo`.
- `order_id` and `order_num`: canonical positive decimal strings; the internal and public identities are separate.
- `source`: `explicit_user_confirmed_full_external_bank_transfer`.
- `confirmed_at`: timezone-aware ISO timestamp **when the operator records the user's confirmation**. It is not the bank-transfer date. The confirmation has no invented expiry.
- `total`: canonical positive decimal string without unnecessary trailing zeros; `currency`: uppercase three-letter code. Both must equal fresh API order values.
- `recovery_status`: exact object with positive-string `id` and `name`, either `Odoslaná` or `Platba online - zaplatené`. Confirmation requires that exact current status, independently re-read after the user restored it.
- `supporting_evidence`: one to four unique `{key, sha256}` objects, each an existing private JSON audit under `data/<project>/order-automation/audits/YYYY-MM-DD/`.

Canonical proof bytes are UTF-8 `json.dumps(proof, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)` with **no trailing newline**. Store them create-only, private and AES256-encrypted at `data/<project>/order-automation/manual-settlements/<sha256>.json`. Do not commit customer evidence, identifiers, amounts or receipts. The operator's explicit confirmation is the authority; the helper does not infer it from arbitrary evidence prose.

From a clean, pushed `codex/` checkout whose tracked source exactly matches its live remote branch:

```powershell
python scripts/record_verified_manual_settlement.py --project vevo --profile codex --proof-sha256 <canonical-proof-sha256>
python scripts/record_verified_manual_settlement.py --project vevo --profile codex --proof-sha256 <canonical-proof-sha256> --apply
```

Preview requires a free, stable journal. Apply acquires the shared lease, re-reads the exact current order and conditionally records only provenance in the journal, verifies it, and releases the lease. Both modes verify source, AWS account, canonical project API/storage destination, private bucket policy/ACL, AES256 objects and exact hashes of the proof and supporting evidence. No admin session, payment endpoint, invoice/email operation or provider status mutation is used. An interrupted apply is reconciled by reading the journal; repeating an identical proof is idempotent and never sends money or messages.

The default confirmation path requires the order already at the attested recovery target. If a gateway has regressed it, append `--expected-current-negative-status-id <reviewed-current-id>` to preview and apply. This opt-in path supports both shops and permits only exact canonical `Platba online - platnosť vypršala`, `Stripe - expired` or `Stripe - unpaid`. It freshly binds the source ID and proof target to the same-shop catalogue twice, with complete current order reads between them. Apply performs all four reads under the journal lease. Any observed detail, generation or catalogue drift blocks recording. VEVO's same visible expiry labels remain separate: canonical ID34 is rejected, not treated as ID33.

The optional path refuses revocation, unresolved invoice/email/status attempts, reviewed closed obligations, blocked orders, partial/reversed/malformed payment evidence and returned/incomplete shipment evidence. It preserves the immutable proof schema and all original journal effects. It does not establish native creditnote clearance or change the order: the existing runtime still needs its own fresh complete creditnote and order checks before any subsequent silent status correction. Neither mode invents a native bank receipt or bank settlement date; `confirmed_at` records the user's confirmation. There is no atomic lock over independent provider callbacks, so the later runtime must recheck.

To withdraw a confirmation, publish a separately hash-bound proof with the same schema/project/order identity/evidence fields, `operation: revoke`, `source: explicit_user_withdrawal`, and `revokes_sha256` equal to the original proof hash. Omit `total`, `currency` and `recovery_status`. Run the same CLI. Revocation retains the original confirmation and records the withdrawal; it never reverses an order. An existing proof cannot be silently replaced or re-enabled, including after revocation.

## Status priority and failure boundaries

Native full receipts or a consistently paid full invoice remain primary payment evidence. The manual proof fills only the exact complete `no_settlement_evidence` case. Partial receipts, reversals, missing/malformed financial fields and changed project/order/total/currency never become confirmed through this fallback. A revoked or stale manual record conservatively blocks recovery even if native payment later changes; review the disagreement rather than automatically re-enable it.

Every paid/shipped repair still refuses blocked orders, protected terminal/return/refund statuses, any known creditnote, unknown creditnotes and shipment return/uncertainty. Existing full-creditnote Storno logic remains separate: a real complete full return may cancel a paid, shipped or invoiced order; partial credit cannot cancel the whole order. A paid attestation cannot downgrade proven shipped fulfillment.

Invoice and unpaid-cancellation runtimes consume the same leased journal proof. They may restore an explicitly attested target even without an invoice. Cancellation also refuses a known manual settlement when recovery is disabled. Dry runs read the same provenance without taking a lease or writing the journal. Incremental invoice status candidates are journaled before the scan watermark advances, including manual-settlement orders without an invoice; unresolved reviews survive subsequent runs.

Repeated gateway regression is eligible only with the same verified proof/order and a strictly newer `last_change` than the previous verified correction. The target must remain the same, except for a one-way advance from paid to shipped with newly proven delivered fulfillment or the trusted same-order shipped observation. Shipped can never regress to paid. Pending/uncertain status, invoice or email operations still block. Every new pending status intent atomically archives the previous verified intent; ambiguous provider effects are never replayed. Recovery requires fresh complete payment/fulfillment detail immediately before the single status request and independent status readback afterward.

Without a bound manual-settlement proof, a repeated native-payment-only regression retains the existing review guard; this change does not promise automatic repeated recovery for every native receipt or paid invoice.

This mechanism supplies durable payment provenance and guarded eventual repair. It cannot prevent a native FLOX gateway handler from writing its own unconditional status mapping. Conditional gateway-side protection remains a separate provider issue; do not disable legitimate successful-payment handling or delete payment history as a workaround.
