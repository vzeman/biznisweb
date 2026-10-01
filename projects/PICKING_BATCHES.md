# Picking PDF batch acknowledgements

Date: 2026-10-01
Repo: `vzeman/biznisweb`
Branch: `codex/pdf-print-batch-20261001`

## Behavior

Each successful dashboard download produces a server-held batch with an immutable
order list, random ID, creation time, filename and PDF SHA-256. The browser reads
the whole PDF response before making that batch confirmable. The dashboard shows
its count and unique filename. Confirmation sends only the batch ID; the server
never derives membership from the refreshed dashboard or client order numbers.
This records printing, not shipment, payment or provider status.

The last successful bulk or individual download in the current tab is remembered
in per-project session storage and survives reload. Other tabs do not replace it.
A failed/interrupted download leaves the previous successful batch unchanged and
visibly identified. Only one download/confirmation may run per tab at once.
Actions are buttons so opening a link in another tab cannot bypass tracking.
The old read-only GET PDF endpoint remains available for previews; legacy direct
order-number print acknowledgements are rejected with a request to download anew.

The exact batch is persisted through the existing conditional S3 operations-state
write before PDF delivery. Concurrent state changes cause a retryable user error,
never an overwrite or an acknowledgement of a different list. Repeated successful
acknowledgements return the saved result. An order already printed by another
batch retains its first print record. Membership remains accurate even if an
order has meanwhile left the fulfillment dashboard. Unconfirmed records expire
after seven days and only the latest 200 batch records are retained; expired or
missing records require another download. This does not prove physical printing;
the warehouse operator still confirms after printing.

## Verification and deployment

Focused tests exercise new arrivals during printing, removed orders, independent
tabs, reload, the latest individual reprint, interrupted/error downloads, repeated
acknowledgement, legacy/tampered requests, wrong project, expiry, PDF failures and
conditional state-write failure. No live order is used for mutation tests.

Run the order-automation regression suite from its GitHub workflow. Its new batch
tests run on Linux and Windows. Invoice query recovery tests separately verify
discarding partial GraphQL data; see `INVOICE_COD_ONLY.md`.

Deployment must use a clean checkout at the exact merged `main` SHA and its ECR
`git-<sha>` immutable digest. Account is `919341186960`, region `eu-central-1`.
ROY service `biznisweb-roy-operations-dashboard` has ID
`ff762bb1c93148638741c62e7abb45b2`; VEVO `biznisweb-vevo-production-board` has ID
`2711a253ae014a8aaf1a37929997496d`. App Runner instance/IP is managed; the finite
candidate must supply its task ID, private IP, service name, `/app` and localhost
marker. Production begins on digest
`sha256:1b1146a228fb63e09afea476beec74790603a0fb29d1788894d33e6c77252dd2`.

For each project run the committed CLI (PowerShell or shell, no local server):

```text
python scripts/deploy_picking_batch_dashboard.py probe --project <roy|vevo> --source-sha <merged-sha> --expected-current-digest <current-sha256> --receipt data/<project>-picking-release.json --profile codex
python scripts/deploy_picking_batch_dashboard.py promote --project <roy|vevo> --source-sha <merged-sha> --expected-current-digest <current-sha256> --receipt data/<project>-picking-release.json --profile codex
```

The candidate uses `scripts/picking_batch_host_gate.py`, actual read-only dashboard
and preview requests, and synthetic in-memory HTTP race tests. Its localhost curl
marker must confirm those checks. The task is stopped and its disposable task
definition is made inactive before promotion. A private encrypted S3 receipt is
written throughout. Promotion rejects source/configuration drift or stale proof,
updates only the image, verifies the recorded App Runner operation and reads back
health, HTML marker, operations API and PDF preview. The reporting schedule is
unchanged. After any ambiguous AWS result, inspect the receipt/operation rather
than rerunning a mutation. No generic process termination is used.

After host verification, check the real dashboard in Chrome: no confirmation is
enabled before a download, the downloaded count/filename is shown, and Refresh
does not change that batch. Do not acknowledge real warehouse orders as a test.
PR #593 passed all six CI checks, including Linux and Windows regression, and
merged as `73b6ba79307768192cc1fd9b0381f911b1c45fd3`. All 1,019 workflow regression
tests passed locally. Build `36834786881` is running; no runtime promotion yet.
Release evidence is maintained on `codex/picking-release-evidence-20261001` while
the exact merged checkout stays clean for deployment. Do not merge documentation
or advance `main` until all source-bound promotions finish.
The first rollout uses merged source `0e65d7d1bf1c42cb425e4c11ad88e6437215d3e1`
and immutable digest `sha256:06417f96e5eeaf3f77318c81ea27c57bf3a38d91f45a9738830736e871d0d78c`.
Both candidate host gates and App Runner promotions passed (see `PROJECT_STATE.md`).
ROY Chrome verified the exact downloaded filename/count surviving reload without
acknowledging any warehouse order. VEVO Chrome is client-blocked; both production
services passed authenticated HTTP and PDF-preview readback.

UI verification identified unnecessary full snapshot invalidation after recording
a pending download: the storage ETag changes even though displayed operations do
not. The follow-up cache fingerprint includes every normalized operational state
section except `picking_pdf_batches`. S3 ETag conditional writes are unchanged.
Actual print, inbound, exclusion and loss state changes still invalidate snapshots;
an unreadable state never validates a cached snapshot. Legacy ETag snapshots are
upgraded in memory only after their exact storage revision has been verified. Regression tests cover both cache reuse
after a download and invalidation after actual operational changes.
The follow-up dashboard build/host/UI rollout is pending; invoice release is still
source-bound to the first rollout and must finish before `main` advances.
