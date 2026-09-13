# Final invoice verification after recovery

Run only after the release has actually been promoted, historical recovery has
finished, and the coordinating operator confirms there is no competing full
provider scan. A successful build or dry candidate is insufficient. Preserve the
production main freeze until the release is independently verified.

Use a clean, fetched/pulled, pushed review worktree containing the deployed source
and this procedure. Record the deployed commit/digest separately: each report's
`source_commit` identifies its verification worktree, not proof of deployment.
The source gate checks the tracked verifier, clean source and matching pushed
branch before AWS or provider access. The CLI uses only the selected project's
Secrets Manager runtime, an explicit same-shop API URL gate and current invoice
settings. It verifies account, canonical bucket, region and all public-access
blocks before any provider read.

Run one shop at a time from the repository root:

```powershell
python scripts/verify_invoice_post_recovery.py --project roy --profile codex --publish-report
python scripts/verify_invoice_post_recovery.py --project vevo --profile codex --publish-report
```

Inspect the first result before starting the second. On an exhausted/unknown
read, stop and coordinate a later retry; do not repeatedly restart a throttled
scan. A complete result with remaining eligible orders is a worklist, not proof
that recovery finished. The CLI returns a nonzero exit code for remaining or
unverified candidates. It saves sanitized failure evidence privately, including
identities of unchecked candidates after the first failed fresh read.

This is a dedicated zero-baseline verifier. `verify_invoice_discovery.py` remains
unchanged: its baseline audit must be at most 24 hours old and contain historical
candidates. Never reuse the September 9 audit, invent candidates or weaken those
gates to prove an empty backlog.

The new verifier calls `InvoiceGenerator.fetch_all_eligible_orders()` and the
current invoice filter. The inventory includes every age/status and retains the
20-minute/page budget, ID anchor/continuity and read-only retry handling. Every
selected candidate receives fresh, internal/public-ID-bound readback. Missing or
malformed fields, nonfinite/boolean money, multiple final invoices, absent final
numbers and failed reads cannot prove completion. `complete` describes the full
inventory; `candidate_rechecks_complete` separately describes fresh rechecks.
`eligible_orders` records the initial snapshot; `still_eligible` in the outcome
counts identifies the fresh remaining backlog. Both shops need `ok=true`.

`unknown_status_orders` is an informational inventory count; absent statuses do
not meet the configured eligible-status predicate. This is a sequential snapshot,
not an atomic view: orders beyond its initial anchor and subsequent status changes
belong to normal future runs. The verifier has no web session or operation journal
and never calls an invoice, email, status or journal mutation. The GraphQL
transport closes in `finally`, including failed scans. The only optional remote
writes are create-only AES256 private reports, verified by exact-byte/encryption
readback with closed S3 bodies. Console output contains aggregates and evidence
key/hash, never private order rows.

This scanner uses GraphQL nested document IDs only as API association references;
it never treats them as native route keys. An `already_invoiced` result means a
unique, numbered API document on the freshly bound order. Native/API final-number
agreement for each recovered write is enforced by the generator and fixed-case
reconciliation helper, separately from this all-age read-only backlog snapshot.

After both snapshots, freshly recheck the original eleven held records with the
existing verifier, sequentially while each journal has no lease and can remain
stable:

```powershell
python scripts/verify_invoice_backfill.py --project roy --profile codex --expected-count 6 --publish-report
python scripts/verify_invoice_backfill.py --project vevo --profile codex --expected-count 5 --publish-report
```

Keep the same clean/pushed source and independently verified project bucket/account
context. These finite CLIs perform no web login, business write or journal write.
They select `source == complete_historical_backlog_audit`, require the exact 6/5
counts and preserved email holds, and reject a lease or journal change between
their two reads. Contention requires a later coordinated retry, never clearing a
lease or bypassing the stable-ETag gate. Require both reports `ok=true`; retain the
private JSON/Markdown evidence, hashes and verified AES256 exact-byte S3 readbacks.
No local server, worker or persistent process is started by this procedure.
