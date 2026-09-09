# Order automation operations

The source of truth is the GitHub repository for code, ECS/Scheduler for runtime,
and each shop's private S3 automation journal for durable operations. Never copy
credentials, customer data or order-level evidence into this public repository.

## Invoice behavior

ROY and VEVO invoice runs operate every fifteen minutes, staggered by five minutes,
throughout the day and night. Once per day they scan all currently eligible shipped
orders without a purchase-date cutoff. Between full scans they read changed orders
from the last complete scan-start watermark with overlap. The cold-start fallback
is ninety days; it does not restrict the full historical scan.

Full discovery reads the unfiltered order inventory by ascending unique internal
ID and stops at the maximum ID captured at its start. This keeps status changes
from moving rows between pages. Adjacent pages overlap; a shifted offset requires
a bounded search for a response that proves the previous-ID boundary. That same
response must be consumed, without an unverified second fetch. New IDs and later
incremental membership changes are covered by the next scan-start watermark.
Purchase timestamps are not pagination keys and do not stop this all-age scan.

Only unblocked, positive-value orders in configured eligible statuses without a
final invoice may be invoiced. Every write rereads the order. A private encrypted
S3 document at `data/<project>/order-automation/state.json` provides a conditional
lease and operation journal. All candidates are saved before any document creation.
An interrupted run retains its unfinished work even after the watermark advances.

Read failures, incomplete pagination, repeated cursors and quota exhaustion are
failures, never proof of an empty backlog. Mutations have no transport-level retry.
An uncertain document creation is reconciled by reading the order; uncertain
email delivery requires review rather than blindly sending again. A confirmed
email failure can be retried without recreating its invoice.

Document preparation and finalization are separate journaled phases. Reuse the
unique preinvoice currently associated with the order. If none exists, call
`preinvoiceOrder` once, selecting only its ID and explicitly setting customer,
admin and salesperson notification conditions to `NONE`. Then reread the order's
associated preinvoice before the native single GET finalization by preinvoice ID.
The live installations expose broken preinvoice-number and nested-order API
resolvers, so neither field is used or inferred. Finalization must be confirmed
by a fresh final invoice on the same order. Transport failures never trigger an
alternate endpoint or an automatic repeat of an uncertain financial operation.
The vendor documents the suppression condition in
[NotificationCondition](https://www.biznisweb.sk/api/docs/notificationcondition.doc.html);
an empty notification list is not used as an undocumented substitute.

Payment settlement, invoicing and shipment are separate facts. Existing invoices
alone cannot promote an order to paid. Known shipment status is retained privately
and used only with fresh settlement/creditnote evidence. Insufficient history and
uncertain previous writes remain visible in a durable review queue. Corrections
suppress customer status emails. Partial creditnotes do not cancel whole orders.

## Unpaid-order discovery

The nightly ROY cancellation run uses the same bounded inventory scanner before
acquiring its mutation lease. It reads all orders, including blocked ones, with
its own payment-element query and selects configured statuses locally. Separate
status-filtered offset scans can silently omit orders when rows move between
statuses; an unfiltered unique-ID inventory removes that pagination dependency.
The maximum is 5,000 reads within twenty minutes, with two-second read pacing and
bounded transient-read retries. An incomplete inventory stops before any status
write. Existing eligibility checks and both fresh mutation checks still apply.
Orders whose status changes after their inventory row was read are evaluated
from fresh data if selected, or rediscovered in the next nightly inventory.

## Historical migration

Run the committed read-only auditor using the configured AWS profile:

```powershell
python scripts/audit_order_automation.py --project roy --profile codex
python scripts/audit_order_automation.py --project vevo --profile codex
```

It reads the existing Secrets Manager runtime credential in memory and produces
ignored private reports under `data/<project>/order-automation/backlog-audit.json`.
The standard API token permits thirty rows per page. A partial scan produces no
complete report. Audit outputs are generated artifacts, not a separate source of
code or truth.

Before releasing a discovery correction, independently exercise the actual
generator's complete read-only scan against the reviewed private audit baseline:

```powershell
python scripts/verify_invoice_discovery.py --project vevo --profile codex --expected-count 5 --publish-report
```

Use the selected project's reviewed count (six for ROY in this migration). This
helper has no web login and performs no invoice, email, status or journal writes.
A still-eligible baseline candidate missing from discovery is a failure. Its
private evidence is separate from final verification of created invoices below.

After reviewing a complete report, seed its old candidates into the private
journal before the first upgraded live run:

```powershell
python scripts/seed_invoice_backfill.py --project roy --profile codex
python scripts/seed_invoice_backfill.py --project roy --profile codex --apply
```

Repeat for VEVO. Seeding requires a fresh complete project-matched audit and a
committed/pushed migration branch. It never changes an existing operation record
and never creates an invoice itself. Historical candidates receive
`email_policy=hold`, so document creation does not send old customer emails.
Normal newly eligible invoices keep the configured email behavior. The runner
freshly checks every seeded order and skips invoices created elsewhere.

For this reviewed migration only, an operator can preview and make one normal
generator attempt for the next unambiguous held seed from clean, pushed source:

```powershell
python scripts/retry_seeded_invoice.py --project roy --profile codex --expected-count 6
python scripts/retry_seeded_invoice.py --project roy --profile codex --expected-count 6 --apply
```

Use count five for VEVO. This helper has no arbitrary-order option and does not
loop or wait through lease contention. Apply authenticates within the shared
lease, checks the whole seed batch again, attempts one selected order and verifies
its final invoice and retained email hold. Stop and inspect the private journal
after any failed or uncertain outcome; do not replay it by clearing a phase.
First promote and drain to a runtime that preserves every current journal phase.
An older runner that does not understand preparation uncertainty must not resume
after this helper creates such a record.

After a live run has released its lease, independently verify the seeded records:

```powershell
python scripts/verify_invoice_backfill.py --project roy --profile codex --expected-count 6 --publish-report
python scripts/verify_invoice_backfill.py --project vevo --profile codex --expected-count 5 --publish-report
```

These counts belong to the reviewed migration, not a default for future audits.
The helper checks each fresh order and the stable private journal, never changes
business data, and fails incomplete or uncertain outcomes. It writes ignored
JSON/Markdown evidence; the optional flag also saves immutable encrypted copies
under the same private bucket's project verification prefix.

## Release sequence and rollback

For a previously shipped order whose state was lost, first inspect its authenticated
admin history and record fresh, project-matched evidence under the ignored private
automation directory. Preview `scripts/restore_verified_fulfillment.py` with the
exact order number, expected current status ID and evidence path. Apply only after
the preview passes against current full payment and complete creditnote evidence.
The helper requires committed/pushed source, holds the shared lease, journals intent,
makes one silent status write and independently verifies the result. Uncertain
outcomes remain blocked for review. Invoice existence alone is never shipment proof.

1. Verify repository, clean branch, fetched/pulled upstream and PROJECT_STATE.
   Record current ECS task identity, private IP, exact image, service and `/app`
   runner path before infrastructure changes.
2. Before merging code that triggers the shared image build, run the committed
   `scripts/deploy_order_automations.py --pin-current --commit <pushed-branch-SHA>
   --current-image-digest <independently-verified-digest> --profile codex`.
   It preserves current behavior and changes only the four invoice schedule task
   references after two finite dry-run hosts pass actual curl localhost markers.
3. Complete regressions and PR checks, merge through the PR, wait for the exact
   `git-<merge-SHA>` ECR build. Never deploy `latest`.
4. Ensure old natural jobs have finished; they predate the shared lease. Seed
   reviewed historical records, then dispatch **Deploy Order Automations** on
   current main. Three identified candidate hosts must pass the existing runners
   in dry-run mode and a localhost marker before any schedule promotion. The
   new invoice candidates explicitly force complete historical discovery,
   regardless of the last saved full-scan watermark; legacy pin probes retain
   their existing compatible arguments. The
   deployer then pauses the five schedules, waits for existing tasks to finish
   and requires two continuously quiet minutes before promotion. It never stops
   natural tasks. The bounded drain also covers pending/stopping tasks; failed
   partial promotion drains any new generation before restoring old schedules.
5. Verify five schedule readbacks, immutable image/commands, live application
   completion, durable invoice/email outcomes and only then shop UI/history.
   Do not confuse a diagnostic success with a production success.

The deployer stores exact old schedules, definitions, candidate host evidence and
state-policy snapshots privately under `data/roy/order-automation/deployments/`.
Failed schedule promotion restores attempted schedule changes only after checking
for concurrent operator drift. A drift or failed rollback requires operator review.
Candidate failure restores changed state IAM policies. Monitoring provisioning can
leave harmless resource additions; this is not a transactional rollback of all AWS
resources. Never overwrite the journal as a rollback: it contains financial effects.
If concurrent changes or an unfinished rollback prevent safe restoration, leave
the protected schedules paused for operator review and use the private snapshot.
The managed workflow has a three-hour bound to leave room for all finite host and
rollback gates; normal deployment is expected to be substantially shorter.

## Monitoring and remaining external dependency

CloudWatch uses `Project` and `RunMode=live` dimensions, excluding dry-run successes.
Alarms cover failures, missing complete runs, pending/ambiguous operations, review
queues, stale full scans and nonempty Scheduler DLQs. They reuse the verified
existing reporting operations SNS route; no new subscriptions or direct messages
are created. A new series has no initial live baseline and can alarm until its
first verified live execution. Never manufacture successful metrics to silence it.

The native FLOX Stripe notification handler remains outside this repository.
The observed seven subscribed event types match the vendor's
[Stripe setup instructions](https://www.biznisweb.sk/a/1472/medzinarodna-platobna-brana-stripe).
An older unsuccessful attempt can still deliver cancellation/expiry after another
attempt succeeded. The admin screen exposes unconditional status mappings; no
supported conditional stale-attempt guard was found. Removing required events or
changing status semantics blindly is not a verified fix. The local guard prevents
unsafe invoice-only recovery and restores only independently supported fulfillment.
A provider fix is required to prevent the native downgrade itself.

The daily report tasks use separately pinned images. Their creditnote-guard source
changes are not deployed by this three-service release. VEVO's reporting task is
also a fixed A/A data source; its upgrade needs a separately reviewed source
transition. Keep this limitation explicit until both report runtimes have their
own verified deployment. Do not silently replace their images through this tool.

No local application server is required by this workflow. Finite diagnostic ECS
tasks are explicitly verified stopped; unrelated local processes are untouched.
