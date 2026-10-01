# ROY fulfillment incident audit — 2026-10-01

Date: 2026-10-01
Repo: `vzeman/biznisweb`
Branch: `codex/roy-4874-payment-audit-20261001`

## Scope and preservation

Read-only investigation of an unpaid online order reportedly admitted to dashboard printing, then shipped and invoiced while another duplicate order received payment. Order numbers, customer data and exact order-level evidence must remain outside this public repository in private evidence storage.

The owner subsequently cancelled the paid duplicate to prevent a second shipment. Treat that later manual intervention separately from the original incident. No investigator production mutation, payment, invoice, status change, print acknowledgement or email is authorized by this diagnostic record.

## Verified so far

- Isolated clean worktree created from `origin/main` at `c98d350e`; fetch/prune and pull/rebase completed. Existing unrelated untracked files and worktrees were left intact.
- AWS account `919341186960`, region `eu-central-1`; service `biznisweb-roy-operations-dashboard` is RUNNING on immutable digest `sha256:1b1146a228fb63e09afea476beec74790603a0fb29d1788894d33e6c77252dd2`. Managed App Runner instance ID/IP are not exposed; documented runtime is `/app`, command `python live_dashboard_server.py --host 0.0.0.0 --port 8080`.
- Deployed source `376e3b68dc4bd977388e47253c530ba0fce452d4` has no diff versus audited main in the dashboard, PDF request handler, ROY configuration or status identity module.
- Native ERP history shows the unpaid order moved directly from waiting to shipped. There is no paid transition in its displayed history. The invoice and invoice email occurred later under the configured named ERP account; that account label does not distinguish human from automation.
- The paid duplicate has a successful Stripe payment and a paid transition. The owner's later credit note and cancellation are visible in its history.
- Private operations state contains a print acknowledgement for the paid duplicate, with no print record for either unpaid duplicate. That acknowledgement precedes the unpaid order's shipped transition by 39 seconds. This conflicts with the reported printing identity and requires examination of PDF access logs and printed document identity; acknowledgement is not proof of physical print contents.
- All three orders use online payment reference `18`, distinct from configured COD IDs `7`, `10`, `16`. Shipping reference `10` is a separate namespace.

## Open questions / next exact step

Read App Runner application logs for the incident window, validate PDF order/barcode identity, review payment and invoice association data, and reproduce eligibility with read-only provider inputs. Identify the actor/integration behind the shipped transition if logs permit. Do not claim a root cause or production fix before resolving the evidence conflict.

No local server, worker, watcher, tunnel or persistent process has been started.
