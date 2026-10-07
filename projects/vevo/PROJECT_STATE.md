# VEVO reporting project state

## Implementation checkpoint: reviewed and ready for CI

Date: 2026-10-07
Repo: `vzeman/biznisweb`
Branch: `codex/reporting-production-rules-20261007`
What changed: Shared Google coverage now preserves measured countries and account totals with an explicit unallocated residual, identity/period/currency checks, visible coverage and no MER for unknown/unallocated rows. VEVO's existing compensation exception is fingerprint-bound across enrichment, cache and revenue reconciliation; source-confirmed currency rounding is enabled. Failed authorized probes preserve bounded private quality diagnostics before cleanup, without creating successful manifests or weakening gates.
What is verified: 1,380 tests across the 77-module CI union pass with unchanged source during execution; reporting smoke, critical Ruff and diff checks pass. All eight real Google project/period probes reconcile against independent daily spend. Fresh runtime and paused-recovery read-only gates pass. Full-history monetary source verification is still finishing; production has not changed.
Known issues: Original report generations and schedules remain in place until the full isolated and live release gates pass. Financial source data and operational diagnostic details remain in ignored private evidence and will be archived with verified hashes.
Next exact step: Commit/push this checkpoint, merge the PR after CI and source audit checks, wait for the exact ECR image and settings-triggered monthly workflow, then recover VEVO and release ROY sequentially. Verify each new generation, financial contract, protected resources, authenticated HTTP and future dynamic-date schedule before claiming completion.


Date: 2026-10-07
Repo: `vzeman/biznisweb`
Branch: `codex/reporting-production-rules-20261007`
What changed: The owner explicitly renewed authorization to finish both VEVO and ROY production releases, regenerate through 2026-10-06, and make future scheduled reports use the corrected rules. This supersedes the previous stop pending continuation. Root `PROJECT_STATE.md` retains the historical audit and failure receipts; this file now holds the product-specific current handoff.

What is verified: The session started with a clean checkout, fetched/pruned and pulled main `e13ba0f6cffa1e0dd8f795cc8e24c6a28baade13`, then created the dedicated branch. The previous verified VEVO state is schedule DISABLED on definition :44 with retained uncertain owner `92011ae652c645c1939124dc360ae2e0` and unchanged publication `20261006T231826Z`. Fresh read-only hard gate passed: AWS account `919341186960`, region `eu-central-1`, Fargate cluster `vevo-reporting-cluster`; no active reporting task, hence instance/IP are not applicable until candidate start. VEVO service `vevo-daily-report-email` is DISABLED on :44 and ROY `roy-daily-report-email` ENABLED on :77. Both immutable OCI configurations independently confirm `/app`. Both schedules use `Europe/Bratislava`, preserve their full-history start dates, and have no fixed end date; subsequent runs calculate yesterday. Private identity proof SHA-256 `ea6b38a5bb1f3eda62ef036ab589ef2311a168e94773640cc32cf569ea016d1c`. Candidate task/IP/image and localhost marker gates must still pass before provider access and publication.

Known issue: The strict monetary metadata path conflicts with an existing audited, status-bounded historical compensation-order exception. Repair only the evidenced exception while still proving its monetary totals; do not fabricate absent elements, broaden eligibility or omit the order. Shared zero-tax/net-total handling already landed with ROY, but VEVO needs complete-history verification. Shared Google country reporting must explicitly retain account spend whose physical country is unavailable without assigning it to a shop or bypassing total checks.

Next exact step: Repair the narrow source exception and country-spend coverage with regressions, and preserve private failure diagnostics. Run full local/CI and primary-source checks, merge through a PR, wait for the exact image and dependent monthly workflow, then run a fresh bound VEVO recovery and ROY release sequentially. Each full-history probe, live generation, future schedule target, financial reconciliation and HTTP/UI check must be verified. No business/provider writes, report emails or invoice/creditnote mutations are part of the one-off regeneration. No persistent local process has been started.
