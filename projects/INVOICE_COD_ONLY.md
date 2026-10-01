# COD-only invoice automation

Date: 2026-10-01
Repo: `vzeman/biznisweb`
Branch: `codex/cod-only-invoice-automation-20261001`

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

Both discovery and exact-order queries now include `price_elements`.
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
passed (including Linux and Windows). Exact-image build `36822990728` is pending.
Both daily reporting tasks explicitly set `REPORT_SKIP_INVOICES=true`, so their
unchanged images cannot bypass this standalone invoice policy.

Next exact step: immutable image build and managed host-gated deployment; verify all four invoice schedule
targets and subsequent natural runs. The shared managed deployment also verifies
the unchanged ROY cancellation service; its business behavior is outside this
change. No local server, worker, watcher, tunnel or persistent process is needed.
