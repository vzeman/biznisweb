# ROY fulfillment incident audit — 2026-10-01

Date: 2026-10-01
Repo: `vzeman/biznisweb`
Branch: `codex/roy-4874-payment-audit-20261001`

## Scope and preservation

Read-only investigation of an unpaid online order reportedly admitted to dashboard printing, then shipped and invoiced while another duplicate order received payment. Order numbers, customer data and exact order-level evidence must remain outside this public repository in private evidence storage.

The owner subsequently cancelled the paid duplicate to prevent a second shipment. Treat that later manual intervention separately from the original incident. No investigator production mutation, payment, invoice, status change, print acknowledgement or email is authorized by this diagnostic record.

Owner-confirmed responsibility boundary: the ROY dashboard must admit only orders eligible for shipment into its bulk picking PDF. Chameleoon consumes that approved barcode and performs expedition/status propagation; it is not responsible for verifying payment in this workflow. Remediation must enforce the admission rule in the dashboard. Chameleoon observations below establish chronology and identity only.

## Verified so far

- Isolated clean worktree created from `origin/main` at `c98d350e`; fetch/prune and pull/rebase completed. Existing unrelated untracked files and worktrees were left intact.
- AWS account `919341186960`, region `eu-central-1`; service `biznisweb-roy-operations-dashboard` is RUNNING on immutable digest `sha256:1b1146a228fb63e09afea476beec74790603a0fb29d1788894d33e6c77252dd2`. Managed App Runner instance ID/IP are not exposed; documented runtime is `/app`, command `python live_dashboard_server.py --host 0.0.0.0 --port 8080`.
- Deployed source `376e3b68dc4bd977388e47253c530ba0fce452d4` has no diff versus audited main in the dashboard, PDF request handler, ROY configuration or status identity module.
- Native ERP history shows the unpaid order moved directly from waiting to shipped. There is no paid transition in its displayed history. The invoice and invoice email occurred later under the configured named ERP account; that account label does not distinguish human from automation.
- The paid duplicate has a successful Stripe payment and a paid transition. The owner's later credit note and cancellation are visible in its history.
- Private operations state contains a print acknowledgement for the paid duplicate, with no print record for either unpaid duplicate. That acknowledgement precedes the unpaid order's shipped transition by 39 seconds. This conflicts with the reported printing identity and requires examination of PDF access logs and printed document identity; acknowledgement is not proof of physical print contents.
- All three orders use online payment reference `18`, distinct from configured COD IDs `7`, `10`, `16`. Shipping reference `10` is a separate namespace.

## Completed evidence reconciliation

- Chameleoon shows a shipment for the unpaid order, from the ROY BiznisWeb source, without COD. Shipment creation precedes the ERP shipped transition by one second. Searching the paid duplicate shows no shipment within the displayed week. The account-level creator label does not identify an individual operator or the exact scanned string.
- Current Chameleoon configuration maps the online payment method to non-COD and enables automatic ERP status change after shipment creation, with target `Odoslaná`. This matches the owner-described workflow and explains propagation after acceptance, but does not prove which barcode/string entered its lookup or the historical lookup response. Absence of a Chameleoon payment check is not classified as a defect.
- The automation journal and CloudWatch identify the subsequent unpaid-order invoice as an automated invoice-runner action after shipped status. The named ERP account shown in history is therefore not evidence of a manual invoice action.
- Current invoice worker task definition is `roy-invoice-daily:11`, scheduled every 15 minutes starting minute 5. Immutable image `sha256:29c0c8e4106438f204b0afaee097e90fab887a82e71327653e508a20574b90c0` maps to `fda36b49e7610d289d6dcb1d0affe892d1ba5ce7`; `generate_invoices.py` and `invoice_runner.py` match audited HEAD. The dashboard PDF renderer also matches its deployed source.
- Recent descending-ID scan read 300 orders across 10 pages; 248 were dated on/after the requested two-week cutoff. Among 94 shipped orders with the same online payment reference, only the incident order had neither a paid invoice flag nor invoice payment entries. This is a bounded current-state check, not an all-history or all-payment-method audit, and not reconciliation against Stripe itself.
- All three provider records use online payment reference `18`. Reconstructing their initial states as waiting/waiting/paid yields only the paid duplicate in picking. Changing each copy to waiting, shipped or cancelled excludes it; changing it to the paid status includes it. These are controlled reconstructions, not recovered historical API responses.
- Independent raster decoding of Code128 from reconstructed PDFs at 150, 300 and 600 DPI returned the exact input order number for all three identifiers (9/9). No barcode swap was reproduced. The original downloaded/printed PDF remains unavailable, so this does not establish its contents or scanner behavior.

## Confirmed design gaps (not a proven initial trigger)

1. `roy_operations_dashboard.py::_is_paid_online` trusts the configured paid status; it does not independently verify settlement. `_price_element_info` correctly separates payment from shipping references. No customer-name deduplication was found in the picking selection path.
2. `live_dashboard_server.py` picking PDF handler selects from an operations snapshot, normally with `refresh=0`, without a fresh per-order payment/status check. The snapshot loader can serve cached/stale data and fall back to stale data after refresh errors. That can preserve formerly eligible orders; alone it does not explain an order whose visible history never entered paid status.
3. `markPickingPrinted` recalculates the current unprinted list at acknowledgement time instead of acknowledging an immutable generated PDF batch. A background refresh between download and acknowledgement can change the set. A stored acknowledgement is not proof of which PDF was generated, printed or scanned.
4. Print state lacks an immutable PDF hash/artifact, exact order versions/payment evidence, generation/acknowledgement actor and scan correlation. Request logging is disabled, S3 versioning is not enabled, and the incident-window log search did not recover a PDF request. Absence of a log cannot prove absence of printing.
5. Invoice generation accepts shipped, unblocked, positive-total orders lacking a final invoice; it does not require prepaid settlement. This explains the downstream invoice, not the initial printing admission. Document corrections and cross-order payment allocation require an explicit accounting/business decision.

## Required remediation and acceptance criteria

- Before every initial print and reprint, retrieve authoritative current order identity, status and settlement evidence. Reject cancellation, shipment already completed, unknown payment methods, ambiguous evidence and provider failures. Continue to permit explicitly configured COD under the intended policy. Do not use a stale fallback to authorize fulfillment.
- Introduce an immutable batch ID bound to exact order IDs/numbers, verified timestamps, order versions and PDF SHA-256. The same batch must drive the PDF and acknowledgement. Revalidation, idempotency and concurrency control must prevent a later refresh or another workstation from silently changing/reusing it.
- Keep Chameleoon's approved expedition workflow unchanged in this remediation. Use its historical scanned-input/returned-identity evidence only to resolve the original PDF identity discrepancy. Dashboard approval must be correct before any PDF reaches the warehouse.
- Keep payment, fulfillment and invoice state distinct. Detect shipped prepaid orders without settlement as an exception needing reconciliation; do not silently transfer a duplicate's payment or automatically alter accounting documents.
- Persist a private, bounded audit trail with PDF identity and source/decision metadata, excluding secrets and unnecessary personal information. Define retention, concurrency and access policy; do not enable bucket-wide changes as an incidental audit action.
- Validate provider amount semantics before reusing `assess_payment_evidence` as the print gate: the valid paid duplicate has an invoice paid flag and native Stripe acceptance, while its receipt amount is marked net against a gross order and lacks a receipt date. The strict assessor returns unknown. A naive reuse would block valid paid orders.
- Acceptance scenarios: unpaid online excluded from list and PDF; paid duplicate included with exact identity; COD allowed by explicit method; partial/ambiguous payment and stale/provider failure blocked; cancellation between listing and download blocked; state change between PDF and acknowledgement cannot change membership; concurrent/repeated printing cannot silently create another approved batch; PDF text and barcode identical; no customer-based payment cross-allocation. Reprints must be explicit, identified and revalidated.

## Reproducible checks

`scripts/audit_roy_fulfillment.py --profile <profile> --order <private-order-number> --since YYYY-MM-DD` performs explicit GraphQL queries and AWS reads only, verifies the AWS account, and writes ignored private evidence under `data/roy/order-automation/audits/`. Supply each relevant order with a repeated `--order`. Do not commit outputs.

`scripts/verify_picking_barcode_identity.py --order <private-order-number>` performs offline PDF/text/barcode identity checks. In addition to repository PDF dependencies it requires PyMuPDF, Pillow and zxing-cpp; zxing-cpp 3.1.1 was installed into an isolated ignored dependency directory for this run, without changing project/runtime dependencies.

Validation: both scripts passed Ruff and compilation; the read-only collector completed; barcode round trips passed 9/9; existing dashboard/PDF/auth suites passed 64 tests. No production endpoint that creates a PDF acknowledgement, order, invoice, shipment or email was invoked by the investigator.

Private evidence and the internal report were archived separately in the established reporting bucket under `data/roy/order-automation/audits/2026-10-01/incident-evidence-20261001/`. All public-access-block flags were verified before upload. New objects used AES256 encryption and `IfNoneMatch=*`; each was read back and hashed. Five evidence/report objects plus a manifest were preserved without modifying source runtime state. Manifest SHA-256: `a04cf6228d1b004ca378f1a69f32c0afa92afb77bc8d08ad8bc110c04ef36726`. The archive is private and has no generated sharing URL. Future evidence must use new object names, not overwrite this receipt.

Failed approaches retained for handoff: the initial read-only GraphQL query used an inline language scalar and an unsupported `order_by=id`; both were rejected. The corrected variable type is `CountryCodeAlpha2` and ordering is `order_id`. Rejected/incomplete reads were not treated as evidence. Default AWS credentials were unavailable; the existing named profile was used. No alternative browser/runtime was used.

## Evidence boundary / next exact step

The initial admission of the unpaid order to the actual warehouse printout is **unresolved**. The owner's reported workflow and the stored acknowledgement conflict on order identity. Neither the current filter nor successful reconstruction exonerates the dashboard; neither an acknowledgement nor subsequent shipment proves original PDF contents.

Obtain the original warehouse PDF/physical printout and, if available, the Chameleoon input/lookup audit for the shipment timestamp. Decode the actual barcode and compare printed text, submitted string and returned order ID. This is the next necessary evidence step for the specific historical trigger. Independently implement the confirmed dashboard admission/print gates and immutable PDF batches on a new reviewed remediation branch; no fix was deployed during this audit.

Security follow-up: a DOM diagnostic unintentionally included an integration API credential value in tool output. It is not in Git, report files or archived evidence. Treat it as exposed and rotate through the approved credential workflow before relying on it further. Further live configuration inspection stopped immediately. Do not copy the value into tickets or documentation.

No local server, worker, watcher, tunnel or persistent process was started, so none required cleanup.
