# COD-only invoice automation

Date: 2026-10-01
Repo: `vzeman/biznisweb`
Branch: `codex/picking-deployed-evidence-20261001`

Status: DEPLOYED for ROY and VEVO. Scoped release `36843666635`, attempt 2,
succeeded on source `7bce051d85019b37f2f0d4bfd44893f5d79b66ea` (PR #595), image
`sha256:4fec00b38699b2b182b87246f5de8c41716fe1d053454cf9a85923cddefe5665`.
The two ROY plans use `roy-invoice-daily:15`; both VEVO plans use
`vevo-invoice-daily:11`. All four are ENABLED, with timing and other parameters
unchanged. Independent comparison verified the remaining nine schedules and
the cancellation state policy unchanged; both invoice state policies match the
candidate. Full-backlog host gates passed for both shops with localhost
`payment_scope=cod_only` markers at `/app`, exit 0 and no financial writes.

Private release receipt:
`data/roy/order-automation/deployments/7bce051d85019b37f2f0d4bfd44893f5d79b66ea/03836f1c009e4edf82b6a99713a86827.json`.
The same prefix contains `independent-after-promotion.json`, SHA-256
`fcdc1c7fd3a065007b81d0a5078be23856296e2df822facabaa5d1437c1f4021`, stored with
AES256 and verified by readback. ROY's first regular scheduled task
`15f1a55225a14cc883ab8d2fc70a3014`, IP `172.31.23.197`, ran the new digest at
10:20 UTC and exited 0 at 10:21 UTC; matched, created and failed counts were zero.
The first regular VEVO run is the remaining verification step.

The release history below retains failed approaches and rollback evidence.
Statements about inactive policy or pending deployment describe those earlier
attempts, not the current production state above.

## Required behavior

The owner requires ROY and VEVO invoice automation to create invoices only for
cash-on-delivery orders. Card invoices are created by the existing gateway flow
after successful payment. Shipped status alone must not authorize a new invoice.
This change does not correct existing documents or change gateway settings,
dashboard picking eligibility, Chameleoon or order status reconciliation.

## Preflight and responsibility

Clean isolated worktree created from `origin/main` at `c98d350e`; fetch/prune and
pull/rebase completed. Unrelated original worktree files were untouched.
AWS account `919341186960`, region `eu-central-1`, ECS cluster
`vevo-reporting-cluster`, runtime `/app` were verified before implementation.
Sources were `roy-invoice-daily:11` and `vevo-invoice-daily:9`, both immutable
`sha256:29c0c8e4106438f204b0afaee097e90fab887a82e71327653e508a20574b90c0`.
Commands are `python invoice_runner.py --project roy|vevo`.
ROY task `58201e9b39e34794ad330b812267a0ec` was pending at private IP
`172.31.16.35`; VEVO had no active task during preflight. These are scheduled
Fargate jobs, so there is no persistent EC2 instance ID. Deployment must verify
each candidate's task ID, private IP, family, digest and `/app`, then its own
localhost curl marker before promotion. No user-facing UI is changed.

## Identity policy

Exact-order queries include `price_elements`. The inventory deliberately excludes
that nested resolver because FLOX fails to resolve it on some historical rows.
Only a shipped, unblocked, positive-value order without a final invoice triggers
the exact-order payment read; its identity and all eligibility conditions are
then rechecked. A failed exact-order read cannot be treated as an empty backlog.
Only one element of type `payment` may authorize creation; its `reference_id`
must match the explicit per-shop `invoice_generation.cod_payment_ids` allowlist.
Shipping references, free-text title matches, unknown IDs, missing payment data
and ambiguous multiple payment elements cannot authorize creation. Missing fields
in an API read fail the scan instead of reporting an empty successful backlog.
An empty configured allowlist permits no new invoice.

Current payment catalogues were read via `listLanguageVersions` followed by
`listPayments(only_active:false)` for every actual language code on each shop.
The verified IDs are:

| Shop | COD payment IDs | Catalogue names |
| --- | --- | --- |
| ROY | 7, 23, 26, 28, 31, 33, 36 | Dobierkou; Cash on Delivery; Dobírka; Plată ramburs; Utánvétes fizetés; Плащане при доставка |
| VEVO | 7, 8, 10, 14, 16 | Dobierkou; Dobírka; Utánvétes fizetés |

Historical IDs absent from the current catalogue are not guessed from dashboard
configuration. A future COD method needs a verified allowlist update. A language
code query using assumed codes was rejected; discovery was corrected to use the
installation's actual language catalogue. No business state was changed by reads.

All existing pre-creation rechecks share the same filter: discovery, resumed
pending work, immediately before preinvoice preparation and immediately before
native finalization. Existing ambiguous operations retain their readback/review
semantics; no uncertain write is replayed. A pending non-COD creation is retired
without creating a document. Already-created documents and their journals are
not deleted or rewritten by this scope change.

## Verification and release

Regression cases cover shipped online orders, bank transfer, missing/unknown or
ambiguous payment, shipping/payment ID collision, all configured COD identities,
payment change before preparation/finalization and a persisted non-COD retry.
The candidate invoice host gate runs a synthetic COD/card/unknown selection check
plus the existing read-only full-backlog check. Its localhost marker includes
`payment_scope=cod_only`; the deployer requires that marker for new invoice images.

Initial test failures were an outdated metrics fixture and a new assertion that
incorrectly assumed no shipment-observation journal record. The existing runner
legitimately records read-only shipment evidence; the corrected test asserts no
invoice identity or financial attempt instead. No production attempt occurred.

Validation before PR: all 1,005 tests in the order-automation CI command passed
locally, both projects' offline policy probes passed, and Ruff/diff checks passed.
All scheduled ECS consumers were checked to use immutable images before a shared
ECR build, so changing the `latest` alias cannot silently update another service.

PR #590 merged as `b220bc9e63748cf0d64dc23cf1a4da289fe00cef` after all six CI checks
passed (including Linux and Windows). Exact-image build `36822990728` succeeded:
`sha256:9014681bfb3db412e6659004ad157b5d2eea93e090507abc8c10cdedf5052c91`.
Managed deployment `36823714684` rejected its candidate on the exact merged source.
All five source schedules matched their independent before-deploy snapshot.
The private snapshot is `data/roy/order-automation/deployments/b220bc9e63748cf0d64dc23cf1a4da289fe00cef/cod-policy-independent-before.json`,
SHA-256 `a7d279f69532817ccfc087e7e20330e567a851d7114b67f40cec5948cb06dc47`.
Both daily reporting tasks explicitly set `REPORT_SKIP_INVOICES=true`, so their
unchanged images cannot bypass this standalone invoice policy.

The first managed candidate failed safely: ROY task
`c972efe0a6204ad0b97acffc534ec702`, IP `172.31.11.236`, definition
`roy-invoice-daily:12`, `/app`, exact new digest, exited 1 after FLOX returned
`Internal server error` at `getOrderList.data[4].price_elements`. Its synthetic
COD filter check passed before the inventory read failed. No candidate was
promoted and no financial mutation was attempted. Managed receipt
`data/roy/order-automation/deployments/b220bc9e63748cf0d64dc23cf1a4da289fe00cef/0188d0f4602c4811bfe3d2749dc4df29.json`
records `original-schedules-and-state-policies-restored`. Independent readback
confirmed all five originals ENABLED on invoice definitions ROY `:11`, VEVO `:9`
and cancellation `:42`.

The follow-up avoids the broken historical collection resolver while retaining
the mandatory fresh payment gate before every financial write. Added tests prove
the inventory omits that field, only potential candidates need a payment read,
online candidates are rejected, fresh status/identity changes are honored and
read failure is not silently swallowed. All 1,009 CI-suite tests passed locally.
Read-only production checks of the first 30 historical rows passed on both shops
with the corrected inventory query; no document was created.

Follow-up PR #591 passed all six CI checks and merged as
`754b0e342858497bcb9002f21214f7140bc2e9a7`. Build `36825205643` succeeded with
`sha256:870076898a6d7785e465a4e4c958079c350ec9f34e3d9ee691ae372f20d98d30`.
Second managed deployment `36825973341` failed on that corrected source.
Independent rollback readback verified all 13 schedules and three state policies.
The second private preflight receipt is `data/roy/order-automation/deployments/754b0e342858497bcb9002f21214f7140bc2e9a7/cod-policy-independent-before.json`,
SHA-256 `59dbca76506d4b77da5bc70e4e393f38548e5364842d952e9ed3222db4d114f4`.
Release handoff continues on `codex/cod-invoice-release-evidence-20261001`.

## Earlier release blocker and verified rollback

The second candidate's identity was verified before the application check:
ROY task `46233203baf24186a431a0b1d3379f01`, private IP `172.31.15.118`,
definition `roy-invoice-daily:13`, path `/app`, exact corrected digest above.
Its command was `python scripts/order_automation_host_gate.py --project roy
--kind invoice --full-backlog --require-cod-only`. The synthetic COD policy
probe passed. Historical inventory then failed with FLOX `Internal server error`
at `getOrderList.data[18].status`. This field was already in the original
inventory query; the failure is not evidence that payment filtering failed.
The full scan and final localhost marker did not complete, so promotion was
rejected. VEVO candidate verification was not reached. The task is STOPPED,
exit 1, and the dry run attempted no financial mutation.

Managed receipt
`data/roy/order-automation/deployments/754b0e342858497bcb9002f21214f7140bc2e9a7/8c22b4dfde6143dfaeede51816586b1b.json`
records `deployment-failed-check-rollback` and
`original-schedules-and-state-policies-restored`. Independent readback verified
all 13 schedules and all three state policies against the saved originals,
with no active host-gate candidate tasks. Both ROY invoice schedules are ENABLED
on `roy-invoice-daily:11`; both VEVO schedules are ENABLED on
`vevo-invoice-daily:9`. Their original image and previous creation policy remain
active. The COD-only restriction must not be represented as deployed.

The private independent after-release evidence is
`data/roy/order-automation/deployments/754b0e342858497bcb9002f21214f7140bc2e9a7/cod-policy-independent-after-failed-release.json`,
SHA-256 `69bdaeaa0e9daf6b3fc69160cd9bbb8dd64febc988109a6c8c603e3c2f2ac995`.
It was stored with AES256, create-only semantics and readback hash verification.
No local server, worker, watcher, tunnel or persistent process was started.

Next exact step: diagnose and resolve the FLOX historical status resolver failure,
then rerun the exact-image host gates, verify all four invoice schedule targets
and subsequent natural runs. Do not remove status validation, swallow failed
reads or bypass the full-backlog gate to force promotion. Work stopped at this
blocker under the owner's stop-on-error instruction. The shared managed release
also checks the unchanged cancellation service; no cancellation behavior was
changed by this invoice policy.

## Resumed release diagnosis

On the owner's explicit follow-up, `scripts/diagnose_invoice_inventory.py --project roy`
read all 5,183 orders across 180 pages successfully without a web login, journal
or mutation. The former resolver failure was not reproducible in that run.
Invoice query reads now opt into the existing bounded retry mechanism for partial
GraphQL errors only when all errors have FLOX's structured `category=internal`.
Every failed response is discarded; the complete query is read again and validated.
Persistent failures still abort the scan; authorization, permanent validation and
quota errors are never overridden. Other query clients retain their original
partial-error behavior. No mutation is accepted by the read retry path. Tests
cover both a fresh successful response and exhausting the finite attempt count.
Production release remains pending the complete exact-image host checks.
## Invoice-only release scope after shared cancellation review failure

On 2026-10-01, source `0e65d7d1bf1c42cb425e4c11ad88e6437215d3e1` and image
`sha256:06417f96e5eeaf3f77318c81ea27c57bf3a38d91f45a9738830736e871d0d78c`
passed full-backlog COD-only host gates for both invoice services. Managed run
`36838303200` then stopped on the unchanged cancellation host gate: three old
recovery cases require settlement review. No financial write occurred during
these dry runs. All 13 schedules and three state policies were independently
verified restored. The invoice policy is not yet active in production.

The managed workflow now offers explicit `scope=invoices` (CLI `--scope invoices`)
to update only ROY/VEVO invoice services. It registers/tests only those candidates,
changes only their monitoring/state permissions and promotes four invoice targets.
All five existing schedules still pause and drain together to prevent competing
readers; the original cancellation definition and every other schedule setting
are restored unchanged. The same transactional rollback and exact-main/image
checks apply. Default `scope=all` still requires all three host gates. This scope
does not override cancellation safety checks or resolve historical review cases.
Unit tests cover both scopes, preservation of cancellation at every schedule
write, invoice candidate failure rollback, and invalid-scope rejection.
