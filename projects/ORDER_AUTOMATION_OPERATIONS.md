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

Payment settlement, invoicing and shipment are separate facts. Existing invoices
alone cannot promote an order to paid. Known shipment status is retained privately
and used only with fresh settlement/creditnote evidence. Insufficient history and
uncertain previous writes remain visible in a durable review queue. Corrections
suppress customer status emails. Partial creditnotes do not cancel whole orders.

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

## Release sequence and rollback

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
   in dry-run mode and a localhost marker before any schedule promotion.
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

No local application server is required by this workflow. Finite diagnostic ECS
tasks are explicitly verified stopped; unrelated local processes are untouched.
