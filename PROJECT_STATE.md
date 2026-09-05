# PROJECT_STATE

Last updated: 2026-09-05
Owner: Patrik
Repository scope: BizniWeb reporting only
Purpose: repo-scoped handoff and execution state for this codebase.

## 2026-09-05 — Authorized historical A/A checkpoint 2 reconstructed

Date: 2026-09-05
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-backfill-checkpoint-2`

What changed:

- The user explicitly authorized historical backfill of missing A/A checkpoints. After confirming no active or successful missing capture existed, dispatched the existing protected workflow with both confirmations true on exact clean main.
- Offline recorder appended checkpoint 2 for `2026-09-03T03:45:00+02:00`, from the frozen start through `2026-09-02T22:00:00Z` (eight full local days). The reconstructed cumulative eligible count is `920`; the stopping rule therefore extends by exactly one full local day.

What is verified:

- Successful run `33951971437`, exact main `14d673c1990ef4d1306697766ae77ac44d9df6e9`, sole artifact `9965116358`. Independently verified GitHub ZIP digest `89f1d6715cd462680210879adad23dc81731e1fd41ff302e86fbcf77ccfdf2da`, one canonical JSON and JSON SHA-256 `c2a7f20d28707960ec0bfb38c90304d0142b5e765ee692381a9b4d6e5a5ad850`.
- Observation at `2026-09-05T07:14:29Z` is explicitly `manual_historical_backfill`, not a contemporaneous capture. The protected workflow verified the original checkpoint's exact scheduled reconciliation identity and success marker, publish parity, alarms/DLQ and source schedule. The recorder independently validated provenance, canonical bytes, hashes and consecutive history.
- Only the aggregate cumulative eligible-device count was queried. No arms/outcomes, paid gate, A/A stop, CTA activation, Preview wake or external configuration mutation occurred.

Known issues:

- The next missing checkpoint is index 3, due `2026-09-04T03:45:00+02:00`. Reconstruct it only after this checkpoint is merged, retaining the original frozen time boundaries.

Next exact step:

- Validate and merge this record after CI, then use the same authorized protected historical fallback for checkpoint 3. Resolve at the first qualifying checkpoint and ignore later captures. Keep all result and experiment mutation gates closed during backfill.

## 2026-09-05 — A/A checkpoint pre-AWS regression isolated and repaired

Date: 2026-09-05
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-checkpoint-history-test-fix`

What changed:

- The 2026-09-03 and 2026-09-04 scheduled A/A checkpoint runs failed before AWS credentials because two unit tests still asserted the original empty checkpoint history after checkpoint 1 had been validly recorded.
- Updated only those stale assertions: immutable pre-registration fields are still compared with the independently recomputed window, while the already validator-checked mutable checkpoint history is required to be non-empty and consecutively indexed.

What is verified:

- The production A/A experiment remains running; the failure occurred in local tests before credentials, AWS, Athena, artifacts, or any external mutation.
- Existing validator code already cryptographically verifies every recorded checkpoint, the exact sequential index and the immutable stopping rule. No validator, workflow gate, threshold, traffic allocation, experiment outcome, or infrastructure behavior changed.
- Today's repository-owned infrastructure monitor run `33951120123` succeeded on exact main `c79db0bc0037d498153c7ce845db599770e839bb`. Its sole canonical artifact (ID `9964852958`) independently matched GitHub ZIP SHA-256 `db148e62d2000abb0abd41c2acc9b6db69a01e14a5d4c127368a16b970f17fb0`, JSON SHA-256 `868530d41fcbca713359110efe4f93911e7ad68ef4b42a648da27e04f1cb245c`, and the offline validator. Production reconciliation succeeded, generated/published parity held, the DLQ was empty, all three alarms were clear, and the source reporting schedule was unchanged. No population, arms, outcomes, Meta dimensions, performance values, identities, or raw payloads were read or retained; temporary downloads were removed.

Known issues:

- No checkpoint artifacts exist for 2026-09-03 or 2026-09-04 because their scheduled workflows stopped at the stale tests. Historical recovery requires its existing explicit protected confirmation and is not authorized by this monitoring run.

Next exact step:

- Merge this test-only repair after CI. Let the repository-owned schedule capture future eligible checkpoints; do not dispatch historical backfill, read arms/outcomes, stop A/A, or alter GrowthBook/Meta/GTM/BiznisWeb without the separately required gate.

## 2026-09-04 — VEVO GrowthBook Preview suspended and independently verified

Date: 2026-09-04
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-preview-suspended-readback`

What changed:

- Completed the user's no-deletion Preview sleep request. Both existing Preview stacks now use `PreviewSuspended=true`: collector desired/running count 0/0, Preview reconciliation schedule DISABLED. Only the two intentional-absence alarms were adjusted; all four CloudFormation changes were Modify without replacement.
- PR #506 preserved deployed YAML intrinsic spelling and merged as `595a39091f990cbe4028c9ea7e83185d08f771fe`. The strict resource/property allowlist was not widened. The earlier broad unexecuted plan was never executed.
- Lifecycle status is `suspended_verified`, closing replay of the original suspend transition. Ordinary Preview deploys remain blocked before AWS. Monitoring explicitly recognizes intentional Preview sleep and must not wake it automatically.

What is verified:

- Successful managed workflow run `33887188363` used the exact main commit above and produced only artifact `vevo-preview-suspended` (ID `9942521868`). Independent checks verified run/path/head, one-file ZIP, GitHub ZIP SHA-256 `c2a0a766d630a2cf92b1c6993826341ec2cab6bc3c6275ab3b2d36cda7606c37`, canonical JSON SHA-256 `512146c5178282bf421aa2335a414a108ec886befcc81c548cd56ba2ef6dba8f`, and approved manifest Git blob SHA-256 `ca2e5dfc2a7f144e5a2be291675ed89016255a503691ad39a25e6c7b90a8448d`.
- Final observation `2026-09-04T15:07:52Z` proves collector 0/0, schedule DISABLED, unchanged resource inventories and all four protected Production/source fingerprints. Production collector remains stable 1/1 with a healthy target; Production reconciliation and `vevo-daily-report-email` are unchanged.
- Exact Fargate identity and localhost health/marker gates passed before and after for both immutable images, runtime `/app`. Collector tasks: before `ddc1243c54ad401f85296327f208a45c` / `172.31.3.254`, after `76302ff319f54434a03535d499d083c2` / `172.31.18.66`. Reconciler tasks: before `5ee7b60780924d869bc8696d38a7bf8e` / `172.31.29.150`, after `2ef666ac067f4555ba3f4e4374dee183` / `172.31.30.99`. Instance ID is N/A:Fargate; service names and image digests remained the preflight identities. All four diagnostic tasks are STOPPED.
- No event/outcome query or data deletion occurred. ALB, API route, VPC link, images/task definitions, IAM, S3/retention, Glue/Athena, reader access, logs and DLQ remain. No local application server, worker, watcher, tunnel or persistent process was started.
- All 24 focused Preview tests, both CloudFormation template lints, security checks and diff checks pass. The six temporary sanitized downloads and their empty task directories were removed and absence verified; source artifacts remain retained in GitHub, with provenance and hashes recorded in the lifecycle manifest.

Known issues:

- Retained ALB, storage and monitoring remain billable; suspension does not eliminate the whole Preview monthly bill. Preview collection and scheduled refresh are intentionally unavailable while asleep.
- The AWS run reports only a non-blocking action-version deprecation warning; all execution, host, retention and Production-boundary gates passed.

Next exact step:

- Keep Preview asleep. Wake only after a separate user request through the reviewed inverse lifecycle procedure; retain the current images and data. Continue the existing Production A/A plan without changing its frozen checkpoint, quality, stop, paid or CTA gates. No A/A result was inspected as part of this task.

## 2026-09-04 — Fix cross-project links between separate ROY and VEVO dashboards

Date: 2026-09-04
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-latest-report-fix`

What changed:
- Project settings now declare each dashboard's verified public origin.
- Index links and dashboard project switching navigate to the selected project's
  own authenticated deployment. Old foreign-project report/dashboard bookmarks
  redirect while retaining the selected period. Foreign API reads return a
  navigation hint before any artifact access; permissions are not broadened.
- Removed misleading index labels that marked S3 reports missing based only on
  local filesystem checks. Local multi-project use keeps relative navigation.
- Added a read-only Fargate gate for localhost identity/marker, real stored HTML
  and JSON for four periods, cross-project redirects, auth and server shutdown.

What is verified:
- Reproduced the exact screenshot error: ROY host `qvfzvh82c3` returns 404 for
  `/report/vevo?period=full`, while VEVO host `2mhmsmgq3m` serves that HTML with
  HTTP 200 (4,046,293 bytes). ROY's own report also returns HTTP 200.
- Pre-change managed identity: instance ID N/A (App Runner), runtime `/app`, port
  8080, command `python live_dashboard_server.py --host 0.0.0.0 --port 8080`.
  ROY service `biznisweb-roy-operations-dashboard`, ARN suffix
  `ff762bb1c93148638741c62e7abb45b2`, digest `8c602cc5632e5da12cc3d07c21b4c312f4b1e49f8118d63af8133b174674fc92`,
  DNS IPs `3.66.161.94`, `3.68.0.57`, `3.74.6.217`.
  VEVO service `biznisweb-vevo-production-board`, ARN suffix
  `2711a253ae014a8aaf1a37929997496d`, digest `04b5039afe84aeebda08b3a46036cb1d1ecbcdc93661757d0b7c77b1ccb47feb`,
  DNS IPs `3.126.244.1`, `3.74.221.100`, `35.157.121.17`. IPs are dynamic frontends.
- Both use separate S3 prefixes and instance roles. Stored reports are intact;
  the previous cleanup's health-only check did not test cross-project navigation.
- All 17 focused routing/S3/mobile tests pass; no persistent local server started.
- Application changes merged through PRs #503 and #505. Build 33886411276 passed
  all 308 required tests and published the image for commit
  `449f1ca156572c7d5d2857dbdff81a5396f40556`:
  `sha256:19ab8ab8b1313dbf627808eafff42dffe557d12891fb401149fd0cd27aa2f3fd`.
- This exact image passed real localhost HTML/JSON checks for 7d, 30d, 90d and
  full, authenticated foreign-project redirects and unauthenticated rejection.
  ROY task `7fb2905caff048bb972fe066c2c10c6b`, private IP `172.31.34.216`;
  VEVO task `83786c73f18b4292a2edc91bb5ee00ff`, private IP `172.31.10.178`.
  Both ran in `/app`, emitted `DASHBOARD_ROUTING_HOST_OK` and
  `DASHBOARD_ROUTING_LOCAL_SERVER_CLOSED`, exited 0, and are STOPPED.
  No temporary routing-check definition remains ACTIVE.
- Both App Runner image-only operations succeeded: ROY
  `9a97557f0a094ed0a502ecc0a2f35ba1`, VEVO
  `32f565d0e923418aa3d638c4038dbcbf`. Both run the exact verified image above;
  existing source configuration, instance settings and business schedules are
  unchanged apart from the image identifier.
- Public authenticated checks passed for all eight HTML reports and both foreign
  report redirects. In Chrome, the original ROY-host VEVO full-report bookmark
  now redirects to VEVO and renders the report. The ROY index's VEVO report link
  also opens correctly. The ROY dashboard's VEVO switch preserves `period=30d`
  and loads `VEVO live dashboard` on the VEVO host.

Known issues:
- No remaining blocker for this navigation incident. Report data was preserved.
- Initial build run 33885896196 stopped at the full test gate because the ROY
  maintenance HTTP fixture inherited REPORT_PROJECT=vevo from an earlier test.
  The fixture now explicitly selects ROY, matching the authenticated service it
  tests. No image was published and no App Runner update was started by that run.

Next exact step:
- Routing incident is resolved. Keep each project's public origin in its own
  settings and include real HTML/navigation checks in future infrastructure gates.

## 2026-09-04 — ROY and VEVO invoiced-order status reconciliation

Date: 2026-09-04
Repo: `vzeman/biznisweb`
Branch: `codex/roy-recovery-paid-status`

What changed:

- The shared invoice runner now reconciles recent ROY and VEVO orders that already have a final invoice but remain in an unpaid, failed, or expired payment status to `Platba online - zaplatené`.
- The paid target status ID is resolved from each shop at runtime and its returned ID/name are verified after every mutation; no ROY-specific status ID is reused for VEVO.
- Reconciliation re-reads each order immediately before mutation. If fulfillment has already moved it to `Odoslaná`, it is skipped and never downgraded.
- ROY's 02:10 unpaid-order recovery remains enabled, but its target changed from `Odoslaná` to `Platba online - zaplatené`; invoice existence is the recovery condition. The real fulfillment/Chameleoon path remains responsible for the later `Odoslaná` transition.
- The production invoice dry-run gate and CloudWatch metrics now expose reconciliation candidates, successes, failures, and resolved target status.

What is verified:

- The pre-change production hard-gate passed in GitHub run `33864877913`. ROY identity: instance ID `N/A (scheduled ECS/Fargate task)`, private IP `172.31.39.74`, service `roy-daily-invoice-generation`, task definition `roy-invoice-daily:2`, runtime `/app`, localhost marker `http://127.0.0.1:8000/marker.json`. VEVO identity: instance ID `N/A (scheduled ECS/Fargate task)`, private IP `172.31.24.23`, service `vevo-daily-invoice-generation`, task definition `vevo-invoice-daily:2`, runtime `/app`, same localhost marker path.
- Both pre-change tasks used digest `sha256:8c602cc5632e5da12cc3d07c21b4c312f4b1e49f8118d63af8133b174674fc92`, emitted `PRODUCTION_INVOICE_SMOKE_OK`, and performed no writes.
- The exact ECR build regression suite passes all `300` tests, including the shipped-during-recheck race guard.
- `python scripts/reporting_qa_smoke.py`, `python scripts/security_ci.py`, Python compilation, Ruff for the touched files (with the repository's existing unrelated exclusions), and `git diff --check` pass.
- PR `#496` merged the change to `main` as `d32cc3e7eb8b4c4d39e78e5809b0b8266a639271`. ECR build run `33866222789` passed and published exact digest `sha256:de5c1f91cc8e95fb17dbf8a29fe85272df84499976b0541a242f9591b8f0b768`.
- The automatic ROY unpaid-cancellation deploy run `33866222839` registered the exact image but its smoke was stopped by a transient FLOX HTTP `429`; no order mutation ran. Isolated dry-run retry `33866792248` then passed with instance ID `N/A (scheduled ECS/Fargate task)`, private IP `172.31.29.200`, service `roy-unpaid-order-cancellation`, task definition `roy-unpaid-order-cancellation:37`, runtime `/app`, and localhost marker `http://127.0.0.1:8000/marker.json`.
- The ROY nightly marker confirmed `recovery_enabled=true`, `0` recovery candidates, `0` failures, and runtime resolution of `Platba online - zaplatené` to ROY status ID `67`; the retry used `execute_now=false`, so it performed no writes.
- Production invoice smoke run `33866982720` passed on the same image for both shops. VEVO identity: instance ID `N/A (scheduled ECS/Fargate task)`, private IP `172.31.46.64`, service `vevo-daily-invoice-generation`, task definition `vevo-invoice-daily:2`, runtime `/app`, localhost marker `http://127.0.0.1:8000/marker.json`, paid status ID `31`. ROY identity: instance ID `N/A (scheduled ECS/Fargate task)`, private IP `172.31.13.190`, service `roy-daily-invoice-generation`, task definition `roy-invoice-daily:2`, runtime `/app`, same marker path, paid status ID `67`.
- Both invoice markers used the exact merged digest, reported `0` reconciliation candidates and `0` failures, and ran in dry-run mode with no invoice, email, or status writes.
- The authenticated ROY UI check after all host markers found order `2678000210` still in `Odoslaná`, preserving the valid 2026-09-04 fulfillment transition.
- No local application server, worker, watcher, tunnel, or persistent process was started.

Known issues:

- The FLOX API can transiently return HTTP `429` when multiple protected smokes run concurrently. The isolated retry passed after the parallel accounting smoke completed; no functional blocker remains for this change.

Next exact step:

- Monitor normal scheduled runs. For an invoice-backed order in an unpaid/failed/expired status, verify the sequence is `Platba online - zaplatené` first and `Odoslaná` only after the fulfillment integration creates the real shipment.

## 2026-09-03 — ROY live dashboard GraphQL throttling and shared-cache fix

Date: 2026-09-03
Repo: `vzeman/biznisweb`
Branch: `codex/roy-shared-live-cache`

What changed:

- The ROY live operations GraphQL path now uses one process-wide serialized pacing boundary with at least `0.5 s` between requests.
- Read-only live queries treat HTTP `429` and BiznisWeb non-JSON GraphQL responses as transient and use `15 s` / `30 s` shared cooldowns before retrying.
- Mutation requests are paced but never automatically retried after an ambiguous response.
- A cold live cache no longer retries the entire multi-page snapshot three times; request-level retries handle transient errors without multiplying API traffic.
- The first protected deployment exposed a second availability defect: the operations snapshot cache was process-local, so a cold App Runner instance could exceed the gateway deadline even when another instance had valid data.
- A completed live snapshot is now stored in the existing encrypted reporting S3 bucket. Cold instances return that shared snapshot immediately and revalidate it in the background.
- Any order or inventory-state mutation also deletes the shared snapshot, so another instance cannot serve pre-mutation operational state.
- Every cached snapshot now carries the S3 operations-state ETag. Before serving RAM or shared cache, each instance compares that revision with the current state; stale per-instance RAM is rejected after any write.

What is verified:

- Pre-change production identity: instance-id `N/A (AWS App Runner managed)`, DNS IPs `3.120.216.162`, `3.75.104.192`, `3.126.228.15`, service `biznisweb-roy-operations-dashboard`, runtime `/app`, UI `/production/roy`, live API `/api/operations/roy/live`.
- The visible failure was reproduced on production: BiznisWeb returned a non-JSON GraphQL response; an earlier refresh also reported repeated HTTP `429` responses.
- PR `#491` merged the request pacing fix as `ad490172f5ba50427163fd555d43b40257331f89`; ECR build run `33735681844` passed.
- Protected deploy run `33735918229` deployed digest `sha256:05b77efed10e81353308b5f1d5a458dcd9aaf4f9557d4e2ed949e57a505e2ed9` and emitted `LOCALHOST_LIVE_DASHBOARD_OK`, `APP_RUNNER_ROY_OPERATIONS_OK`, `APP_RUNNER_MAINTENANCE_INACTIVE_OK`, and `APP_RUNNER_DEPLOY_OK`.
- The post-deploy browser check still reproduced gateway `502/504` behavior on a cold instance, which identified the missing shared cache.
- The shared-cache regression passes all `45` focused tests and all `158` dashboard/reporting tests. Ruff, Python compilation, and `git diff --check` pass.
- PR `#492` merged the initial shared-cache implementation as `9b53879b2b1444d8aa4e011a882ce926162d14b6`; ECR build run `33739246064` passed and published digest `sha256:0e55b98ffa13ac2adfd5217c81a3826dee646e152a3e6d5fb6863bd73628df2e`.
- Protected run `33739477526` deployed that digest and passed `LOCALHOST_LIVE_DASHBOARD_OK` plus `APP_RUNNER_ROY_OPERATIONS_OK`, but correctly failed the restock mutation roundtrip because the first implementation did not invalidate the shared snapshot after a write.
- PR `#493` merged shared-object invalidation as `9f892e1db27dec5ce1a8e1d1bb17b547f382c1fe`; build run `33740968121` passed and protected run `33741170936` deployed digest `sha256:723c83b4aa33f914e7d5c36cad785f7484380a318d88e8248c73ead0840215aa`.
- Run `33741170936` again passed localhost and live operations gates but exposed a second consistency layer: a different warm App Runner instance could still serve its old in-memory snapshot. ETag validation now closes that cross-instance gap.
- PR `#494` merged ETag validation as `1f9f0437bda190008433b0e5163d0c91364509bd`; ECR build run `33742590128` passed.
- Protected deploy run `33742820220` deployed digest `sha256:8c602cc5632e5da12cc3d07c21b4c312f4b1e49f8118d63af8133b174674fc92` and passed `LOCALHOST_LIVE_DASHBOARD_OK`, `APP_RUNNER_ROY_OPERATIONS_OK`, `APP_RUNNER_MAINTENANCE_INACTIVE_OK`, the full restock preference roundtrip with restoration, and `APP_RUNNER_DEPLOY_OK`.
- Final authenticated Chrome verification after reload showed no error banner, fresh operations timestamp `2026-09-03T10:25:47Z`, `5` fulfillable orders, `160` inventory rows, and MACO STOP Extreme 300 ml SKU `14832` at `168 ks` live stock.
- No local application server, worker, watcher, tunnel, or persistent process was started.

Known issues:

- No known blocker remains for the ROY live operations dashboard. App Runner has no stable instance ID or host IP; its managed identity remains the service ARN and digest.
- App Runner has no stable instance ID or host IP; the listed public DNS addresses are dynamic frontend addresses.

Next exact step:

- Monitor normal 90-second auto-refresh behavior and investigate only if a new visible error banner appears with a timestamp newer than the successful deployment.

## 2026-09-02 — ROY HTTP 429 recovery deployed; 7,600 EUR fixed cost is live

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/biznisweb-api-rate-limit-final-state`

What changed:

- Every BiznisWeb GraphQL request now passes through one pacing and retry boundary, including order pages, inventory pages, the `price_elements` fallback, and individual payment-metadata lookups.
- Requests keep at least `0.5 s` separation and pause for `5 s` after every `100` calls, matching the current BiznisWeb Partner API guidance.
- HTTP `429` receives explicit exponential cooldowns of `15 s` and `30 s` around the transport retry layer; HTTP `509` remains fail-fast because it represents a daily or monthly quota rather than a short-term rate limit.
- The transport backoff is raised to `2 s` and continues to respect a server-provided `Retry-After` header. Safe environment overrides are documented in `.env.example`.
- Product-inventory pagination was raised from `0.1 s` to the documented `0.5 s` minimum.
- PR `#489` merged the rate-limit hardening to `main` as `fcf6e26341b1e4c1d47f71f6f688b3e09dcbe14b`; this merge also contains the previously merged ROY `7600 EUR/month` fixed-cost configuration.

What is verified:

- Focused pacing, long-pause, `429` recovery, `509` fail-fast, order fallback, and payment fail-closed tests passed.
- Reporting, dashboard, and ROY inventory suites passed all `135` tests.
- `python scripts/reporting_qa_smoke.py` passed, including the ROY `7600 EUR/month` fixed-cost assertion.
- The exact ECR build regression suite passed all `291` tests.
- Python compilation and `git diff --check` passed. No local application server, worker, watcher, tunnel, or persistent runtime was started.
- ECR build run `33627015408` passed and published the exact merged image digest `sha256:9ff4738f998e3d80e7b76dc543f11bc36413d9d016e72b4a78eb3411433bc541`.
- Protected ROY production run `33627278825` completed successfully. Its hard-gate identity was ECS/Fargate task `66d639a15d624ee2a7330aa0580a992f`, private IP `172.31.33.77`, service `roy-daily-report-email`, task definition `roy-reporting-daily:71`, runtime `/app`, and localhost marker `http://127.0.0.1:8000/marker.json`; the task used the exact digest above and exited with code `0`.
- The same run emitted `LOCALHOST_MARKER_OK`, `UI_SMOKE_OK:roy:daily-profit-loss`, and `PRODUCTION_SMOKE_OK`, then promoted the complete live generation. The export contains `343` daily rows, `182` positive days, and `161` negative days.
- Read-only diagnostic run `33630468048` independently confirmed that the live S3 alias now points to generation `20260902T122926Z`, the scheduler is `ENABLED`, and it uses task definition `:71`.
- Browser verification of `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy` succeeded after the localhost marker: the authenticated dashboard loaded live data from `2026-09-02T12:29:25Z`, rendered accounting and operations KPIs, and produced no browser console warnings or errors.
- The ROY fixed overhead source of truth is therefore live at `7600 EUR/month`; September allocates approximately `253.33 EUR/day` before display rounding. Project configuration kept daily email disabled, so the smoke did not send a real report email.

Known issues:

- Direct browser navigation to the accounting JSON endpoint was blocked locally by the Chrome extension with `ERR_BLOCKED_BY_CLIENT`. This is not a production failure: the rendered authenticated dashboard, protected host/API/UI gates, task exit code, and independently refreshed S3 generation all passed.

Next exact step:

- No code or production action remains for the ROY `7600 EUR/month` fixed-cost change or the HTTP `429` blocker. Monitor the next scheduled ROY run; only tune the documented pacing overrides if a later run records new `429` cooldown warnings.

## 2026-09-02 — ROY 7,600 EUR fixed-cost image scheduled; live refresh blocked by ROY API rate limit

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/roy-monthly-fixed-expenses-7600`

What changed:

- `projects/roy/settings.json` now sets the ROY source-of-truth `fixed_monthly_cost` from `6500` to `7600` EUR.
- `scripts/reporting_qa_smoke.py` now guards the exact `7600.0` runtime value against future configuration drift.
- With no daily override, the runtime continues to divide the monthly amount by the calendar days in each month; September therefore uses approximately `253.33 EUR/day` before output rounding.
- PR `#486` merged the change to `main` as `a4622526ad74163af6b1f56c23556f2b39f13fcd`.

What is verified:

- ROY settings JSON parsing and Python compilation passed.
- `python scripts/reporting_qa_smoke.py` passed.
- `python -m unittest tests.test_reporting_calculation_fixes tests.test_dashboard_modern` passed all `108` tests.
- `git diff --check` passed. No local application server, worker, watcher, tunnel, or persistent runtime was started.
- PR checks `env-check`, `observability-baseline`, `secret-scan`, and `security-baseline` passed.
- ECR build run `33618684191` passed its reporting and invoice gates and published exact digest `sha256:e296ef75e24e263e0cd1e1f772997ac75c0514e87ad43864b7b4bc0b14f86b1f` for the merged commit.
- Two non-live predeploy smokes confirmed the current production identity before any image update: instance-id `N/A (scheduled ECS/Fargate task)`, service `roy-daily-report-email`, task definition `roy-reporting-daily:69`, runtime `/app`, marker path `http://127.0.0.1:8000/marker.json`, and current image digest `sha256:ea2b92e37761bac2c89d8aafa475a837e05e8b71d92e588c875bdf31c3ec6c9a`.
- Predeploy task `52ea0a3ecbd7423dbc701c8cc7c0edf6` used private IP `172.31.6.188`; retry task `efa28b91b64b4da196bcf4ad99dceb8a` used private IP `172.31.20.156`. Both explicitly reported `task-image-updated=false`.
- Protected retry run `33621380934` updated the enabled ROY scheduler to task definition `roy-reporting-daily:70` and exact digest `sha256:e296ef75e24e263e0cd1e1f772997ac75c0514e87ad43864b7b4bc0b14f86b1f`; the image contains the merged `7600 EUR/month` configuration and passed its build-time reporting QA assertion.
- Retry hard-gate identity: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.44.67`, service `roy-daily-report-email`, task `01af1c3efc3149c9bc528b566ad5782a`, task definition `:70`, runtime `/app`, and marker path `http://127.0.0.1:8000/marker.json`; the workflow explicitly reported `task-image-updated=true`.
- Read-only diagnostic run `33622383551` succeeded and independently confirmed the scheduler is `ENABLED` on task definition `:70`. The still-live S3 dashboard generation is `20260902T055120Z`, created before the fixed-cost source change merged.

Known issues:

- The first two predeploy tasks stopped with exit code `1` before localhost marker creation because `www.roy.sk/api/graphql` repeatedly returned HTTP `429` while the inventory snapshot was paginating.
- The protected retry on task definition `:70` also stopped with exit code `1` before localhost marker creation. It progressed further, but repeated HTTP `429` responses prevented payment-metadata enrichment for realized-revenue candidate order `2677001216`; fail-closed accounting correctly aborted the export.
- The scheduler now has the new `7600 EUR/month` image, but no successful host marker or UI gate exists for that image and the live dashboard artifact has not refreshed. Do not claim the live dashboard uses `7600` until a complete export succeeds.

Next exact step:

- After the ROY GraphQL rate limit clears, rerun the ROY-only protected production smoke from merged `main` with `send_email=true`, `update_task_image=false`, and a unique marker. Require task definition `:70`, exact digest `sha256:e296ef75e24e263e0cd1e1f772997ac75c0514e87ad43864b7b4bc0b14f86b1f`, exit code `0`, localhost marker, and UI smoke; verify the refreshed payload allocates September fixed overhead as approximately `253.33 EUR/day` and confirm no ROY report email is sent because project configuration keeps daily email disabled.

## 2026-09-02 — ROY overdue inbound valuation and stock alerts deployed

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/roy-inbound-fix-state`

What changed:

- PR `#477` fixed the business rules: overdue inbound remains visible and valued, is labelled `po termíne`, and no longer suppresses current stock alerts.
- PR `#478` completed production serialization for history-only zero-stock products through `inventory_reference_rows`, so operations valuation can use the mapped purchase costs for MACO STOP 300 ml and 150 ml.
- The exact inbound scenario is covered end to end: `250 × 18.40 EUR = 4,600 EUR`, `40 × 12.90 EUR = 516 EUR`, total known inbound cost `5,116 EUR`; the 10-unit SD-card row is the only unpriced item.

What is verified:

- PR `#477` merged as `fd1aef8c562cd26198d258245cfd5b08c1d8e07f`. PR `#478` merged as `d204dccff75d8a55ab1c6af09d9d69e94dd8dc33`.
- ECR build run `33594886080` published the exact merged image digest `sha256:ea2b92e37761bac2c89d8aafa475a837e05e8b71d92e588c875bdf31c3ec6c9a`.
- Protected deploy run `33595077546` completed successfully in `33m57s`. Its pre-code hard gate identified ECS/Fargate task `de1c23a2bead4851be24f40c1799eee4`, private IP `172.31.4.185`, service `roy-daily-report-email`, candidate task definition `roy-reporting-daily:69`, task role `BiznisWebReportingTaskRole-roy`, image digest `sha256:ea2b92e37761bac2c89d8aafa475a837e05e8b71d92e588c875bdf31c3ec6c9a`, runtime path `/app`, and localhost marker path `http://127.0.0.1:8000/marker.json`.
- The protected deploy step passed all mandatory generation, localhost-marker, live-artifact, App Runner operations/API, PDF, maintenance and production endpoint gates before promoting the image. App Runner service is `biznisweb-roy-operations-dashboard` at `/production/roy`.
- The generated production artifact before the serialization follow-up already showed both SKU `14832` and `622_M33` as zero/negative-stock alert rows, proving overdue inbound no longer hides MACO STOP 150 ml. The follow-up regression proves both serialized reference costs survive the same production-image path.
- ROY/dashboard suite passed `77` tests and the full production-image suite passed `287` tests; compilation and `git diff --check` passed. No local application runtime was started.

Known issues:

- The browser profile currently blocks the App Runner domain locally with Comet `ERR_BLOCKED_BY_CLIENT` after reload. A second user-visible reload produced the same client-side block, so no further requests were sent. This is separate from the successful protected App Runner/API smoke and is not evidence of a production failure.
- Because of that local browser block, the final visual DOM readback of the rendered `5,116 EUR` and the MACO STOP 150 ml row could not be captured after deploy. Do not repeat refreshes until the Comet block is cleared; the production fix itself is merged, digest-pinned and deployed.

Next exact step:

- After the local Comet block is cleared, perform one manual readback of `/production/roy`: expected known inbound value is `5,116 EUR`, one inbound row remains unpriced, all 300 inbound units are overdue and count as zero coverage, and MACO STOP 150 ml must be present in stock alerts at zero stock. No code or deploy action remains for this fix.

## 2026-09-02 — ROY overdue inbound valuation and stock-alert fix ready for PR

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/roy-inbound-overdue-valuation`

What changed:

- History-only ROY products now retain a single unambiguous mapped purchase cost from the order-item pipeline, so inbound orders can be valued even when the product is absent from the current warehouse snapshot.
- An inbound order whose ETA is before the current UTC date remains listed and valued, but no longer increases stock coverage, suppresses a low-stock alert, or changes `Order now` into an inbound-covered state.
- The dashboard now identifies overdue inbound rows as `po termíne` and explains that they are not counted toward stock coverage. Summary metrics separate overdue units from units that still count toward stock-risk coverage.

What is verified:

- Infra hard-gate was established from protected production smoke run `32447411582`: ECS/Fargate task IP `172.31.21.89`, service `roy-daily-report-email`, task definition `roy-reporting-daily:67`, runtime path `/app`, localhost marker `http://127.0.0.1:8000/marker.json`; marker and `roy:daily-profit-loss` UI smoke passed. App Runner service is `biznisweb-roy-operations-dashboard`, production path `/production/roy`.
- Focused ROY inventory/dashboard tests passed: `63` tests. The exact production-image test suite from `.github/workflows/build-and-push-ecr.yml` passed: `285` tests. Python compilation and `git diff --check` passed.
- Regression coverage proves mapped costs `18.40 EUR/ks` for SKU `14832` and `12.90 EUR/ks` for SKU `622_M33`, and proves that overdue inbound remains valued without hiding an out-of-stock alert.

Known issues:

- The SD-card inbound row remains intentionally unpriced because no unambiguous purchase cost exists; the dashboard must continue to report it as missing rather than guess.
- Production deploy and live UI verification are pending PR review and protected workflow completion.

Next exact step:

- Commit and push this branch, merge only after required PR checks pass, deploy the exact merged image through the protected ROY App Runner workflow, verify localhost marker first, then verify live inbound value and the MACO STOP 150 ml alert in `/production/roy`.

## 0) How To Use This File

- This file is authoritative only for this repository.
- Do not use it as a shared state file for Doklady or OpenClaw.
- External projects may be mentioned only as integration notes.
- Update this file after each major implementation, deploy-relevant change, or workflow change.

## 1) Repository Purpose

- Product type: reporting/export automation for BizniWeb-based clients
- Current active clients in repo: VEVO, ROY
- Main responsibilities:
  - export orders from BizniWeb GraphQL API
  - generate invoice-related artifacts
  - build daily reports
  - optional Google Ads / Facebook Ads enrichment
  - scheduled email report delivery via SES/S3

## 2) Source Of Truth Rules

- GitHub is the only source of truth for code.
- No required script may live only on one local PC.
- No required runtime/deploy flow may depend on Desktop/Downloads files.
- Secrets stay outside git (`.env`, runtime env, AWS secrets).
- Every machine must be able to bootstrap from this repository alone.

## 3) Current Branching / Workflow Rules

- Reporting work stays in this repository on `main`
- Doklady work moved out to the standalone `Terem21/doklady-saas` repository
- OpenClaw work was moved out to the standalone `openclaw-agents-platform` repository
- Use short-lived task branches only; branches are not product boundaries
- `main` only through reviewed merge
- Before work: `git fetch --all --prune && git pull --rebase`
- After major step: commit + push immediately
- No force-push on shared branches

## 4) Environment Baseline

Required baseline keys:
- `BIZNISWEB_API_TOKEN`
- `BIZNISWEB_API_URL`

Enforcement in repo:
- `.env.required`
- `.githooks/pre-commit`
- `.github/workflows/env-check.yml`
- `scripts/check_env.sh`
- `scripts/check_env.ps1`

Bootstrap entrypoints:
- `scripts/bootstrap.sh`
- `scripts/bootstrap.ps1`

## 5) Current Verified State

- GrowthBook plan and billing were reverified read-only on `2026-08-25`:
  - the authenticated `Vevo` organization is still on `Starter Plan` with exactly one active seat
  - the in-account Pro offer is `$40/month` for the current seat (`$40` per seat per month), including two million Global CDN requests and 20 GB bandwidth per month; listed overages are `$10` per additional million requests and `$1` per additional GB
  - the current official GrowthBook plan comparison independently lists Pro at `$40` per seat per month and places Quantile Metrics in Pro, matching the existing UI blocker on the three LCP/INP/CLS p75 metrics
  - the upgrade dialog was cancelled before `Continue`; no payment, subscription, trial, auto-renewal, GrowthBook object, experiment, traffic, GTM, Meta Ads, BiznisWeb, or commerce state was changed
  - Production A/A remains on the frozen schedule; the independent aggregate final performance snapshot remains the fail-closed path until a paid Pro upgrade is explicitly authorized

- VEVO GrowthBook Production clone is verified complete and still hard-disabled for traffic (`2026-08-23`):
  - authenticated GrowthBook connection `ds_19g6mmt5stlp6` passed against database `vevo_growthbook_production` and workgroup `vevo-growthbook-readonly-production`
  - the source retains exactly `device_id` and `VEVO consented devices`; GrowthBook-generated demo `user_id` and `Logged-in Users` objects were deleted after explicit confirmation and the surviving configuration was reloaded
  - assignment SQL executed without SQL error and returned exactly zero rows; Production allocation remains `0%`, GTM remains unpublished, and no Production experiment is running
  - Device Outcomes `ftb_19g6mmt5tg48t` and Performance Vitals `ftb_19g6lmt5ueyhu` store the exact merged Production SQL; each UI test returns the one metadata-only probe while an independent curated-only test returns zero rows without SQL error
  - every outcome column and `vital_value` was manually typed and read back; the source reports exactly two fact tables and eight Starter-compatible metrics, all with unique Production IDs and matching type/filter/aggregation/goal/window contracts
  - Preview data source `ds_19g6mmt2c4dmn`, fact-table IDs, metric IDs, and exact probe-free SQL were read back unchanged; no paid-Pro p75 metric was created or upgrade accepted
  - sanitized canonical evidence `vevo-growthbook-production-clone-observation.json` has SHA-256 `b2f96b7047321f11da4f00c7886c4b9422d7759428534f8fd5534ee1299f2030`; it contains no credential, query result, event/device ID, order, or customer data
  - the Production collector registry now contains only the exact Preview-matched invisible A/A contract `vevo-sk-aa-001`; all four reviewed evidence preconditions are true and only `collector.deployment_allowed` is open
  - GrowthBook remains unstarted, GTM remains unpublished, Production allocation remains `0%`, CTA remains stopped, and Meta Ads/BiznisWeb/cart/checkout remain unchanged
  - collector-gate preparation passes `589` Python tests, all `9` storefront JavaScript tests, activation/workspace/security validators, Ruff, and `git diff --check`
  - collector deploy run `32644089503` stopped before image build/deploy at the predeploy identity hard-gate because sourced stack variables were not exported to its Python verifier (`KeyError: PRODUCTION_TASK_DEFINITION_ARN`); no service, image, route, GrowthBook, GTM, Meta Ads, BiznisWeb, or traffic mutation occurred
  - branch `codex/vevo-growthbook-aa-hard-gate-env` exports only the generated non-secret stack identity variables for that verifier and adds regression/security assertions; the deploy must be retried only after reviewed CI from the new exact `main` commit
  - the hard-gate fix passes the full `590`-test Python suite, all `9` storefront JavaScript tests, `34` focused activation/collector/workspace tests, activation/workspace/security validators, Ruff, and `git diff --check`
  - collector deploy run `32644408714` then passed the exact predeploy identity gate, immutable image rollout, route-disabled runtime stabilization, distinct Fargate localhost health/`/app` marker task, healthy service target, single-route activation, exact CORS/private-path/attacker rejection, byte-identical invalid-probe raw snapshot, and sanitized artifact upload; GrowthBook/GTM remained unstarted/unpublished and allocation remained `0%`
  - the downloaded artifact is schema-valid and secret-free but uses deterministic compact JSON from the workflow while the offline recorder requires the shared pretty canonical encoding; its exact raw SHA-256 is `1e156ebdd94f88f7858c0e0b2ddb443fdabe01787ee6f7d673ac80197492ab88`, and no manifest field was recorded after the fail-closed rejection
  - branch `codex/vevo-growthbook-aa-evidence-format-recovery` changes future artifacts to the shared canonical format and permits the compact form only for the exact run `32644408714`, main commit `57b29c3b166eabbbabee4d3b8e69d1b56e2ae8e2`, and pinned raw SHA-256; other compact artifacts remain rejected
  - evidence-format recovery passes the full `591`-test Python suite, all `9` storefront JavaScript tests, `35` focused collector/activation/workspace tests, activation/workspace/security validators, Ruff, `git diff --check`, and an offline dry-run against the exact downloaded artifact
  - the exact downloaded artifact is now recorded in `growthbook_production_aa_activation.json`: deploy gate closed, route verified active, workflow run `32644408714`, main commit `57b29c3b166eabbbabee4d3b8e69d1b56e2ae8e2`, task definition `vevo-growthbook-collector-production:2`, host-gate task/IP, immutable image digest, endpoint-host hash, and evidence SHA-256 are bound; GrowthBook/GTM/traffic remain disabled
  - the local temporary artifact download and recorder dry-run output were deleted after the exact hash/run/commit boundary was recorded; the sanitized GitHub artifact `9494569621` remains retained until `2026-09-06`
  - GrowthBook Phase 3 zero-allocation preparation is now read back: Production SDK connection `sdk_19g6lmt5wnngy`, draft A/A experiment `exp_19g6mmt5wugpk`, Production data source `ds_19g6mmt5stlp6`, and draft feature-rule revision `3`; the live Production rule remains unpublished/disabled and the experiment remains unstarted at `0%`
  - isolated GTM workspace `VEVO GrowthBook Production A/A` (`17`) contains exactly five added objects, zero modified objects, and zero removed objects: loader `54`, consent bridge `51`, add-to-cart bridge `55`, purchase bridge `53`, and the dedicated `add_to_cart` trigger
  - GTM read-back proves the loader runs at `Initialization - All Pages`, precedes all three bridges with the fail-safe enabled, and has exact artifact SHA-256 `d6861bcbe002a96f82a4a29882723002cd6c797177194bdd93f67e6cf2eba8df` from reviewed main commit `1a24b4fe657c546b6fcf71a336b9d4220622a74e`
  - no GTM container version was submitted or published; Meta Ads, BiznisWeb, prices, product content, cart, checkout, orders, and CTA experiment remain unchanged, while Production traffic and active experiments remain `0%` / empty
  - the task-scoped SDK-key handoff, generated Production artifact, empty artifact directory, in-memory artifact copy, and managed-browser clipboard were removed after the exact GTM read-back; no SDK key or collector URL was committed
  - branch `codex/vevo-growthbook-production-ui-evidence` passes the full `593`-test Python suite, all `9` storefront JavaScript tests, `21` focused activation/collector/builder tests, activation/workspace/security validators, changed-file Ruff and Python compile, JSON parsing, and `git diff --check`
  - Phase 4 desktop Tag Assistant observation is recorded fail-closed: workspace `17` connected, analytical reject/grant/withdraw/regrant was exercised, all five original consent categories were restored, the Production SDK connection reached `Connected`, the experiment remained `Draft`, and Tag Assistant reported zero console errors
  - product `/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute` retained the exact `Pridať do košíka` text with no `vevo-gb-cta-brand-contrast` class; the pre-existing cart count remained `2` and no cart action was performed
  - Chrome communication was restored after reinstalling the ChatGPT browser extension; mobile Production Preview was verified at an explicit `390x844` viewport (`390` inner width, `375` document width, `844` inner height), with the product CTA still exactly `Pridať do košíka`, no experiment class, cart count `2`, and no cart mutation
  - GrowthBook Production was read back again as `Draft`; the linked feature change remains a draft with `Environments (0/2)`, so zero Production assignment is explicitly verified while traffic remains `0%`
  - the mobile consent cycle is complete: all seven Consent Mode signals became `Zamietnuté`, both Meta pageview tags were blocked, and GA4 continued only in consent-mode behavior; after regrant, every signal returned to `Udelené` and GA4 plus both Meta tags fired again, with all five user-facing consent categories restored
  - runtime owned-storage cleanup is now user-visibly verified in the exact Production workspace `17` Preview: with all five optional consent categories granted, the connected Preview target loaded the GrowthBook SDK once; analytical withdrawal was then applied from a separate same-origin tab while Tag Assistant stayed connected, and the reloaded product target contained zero GrowthBook SDK script elements
  - the post-withdrawal Chrome Application view filtered by `vevo_` showed only the unrelated `_lhis_history` entry; `vevo_exp_device_v1` and `vevo_gb_features_v1` were absent without manual deletion, proving the integration removed its owned experiment/device and feature-cache storage; all five original consent categories were restored afterward
  - the earlier `0 / 167` Network screenshot is intentionally not accepted as zero-collector evidence because it was captured after the workspace Preview session had disconnected; `zero_collector_request_verified` remains `false`, GTM remains unpublished, traffic remains at `0%`, and controlled activation remains closed
  - branch `codex/vevo-growthbook-owned-storage-qa` passes the full `594`-test Python suite, all `9` storefront JavaScript tests, the exact activation and workspace validators, focused Ruff and Python compile, JSON parsing, and `git diff --check`
  - branch `codex/vevo-growthbook-zero-collector-observation` freezes the exact `2026-08-24T04:30:00Z..2026-08-24T04:50:00Z` manual Preview window and prepares a main-only, explicitly confirmed, aggregate-only CloudWatch observation; it re-verifies the current Fargate task/IP/service/`/app` identity and healthy `POST /v1/events` route before counting both API access events and accepted collector receipts, uploads no messages or event/request IDs, contains no AWS or external mutation path, and passes all `599` Python tests plus focused activation validation, security CI, Ruff, Python compilation, YAML/inline-Python parsing, and `git diff --check`
  - the first main observation run `32692225886` stopped safely before AWS because its redundant in-workflow unit-test invocation expected locally installed PyYAML; branch `codex/vevo-growthbook-zero-collector-runner-fix` removes only that redundant invocation, retains the dependency-free activation validator plus exact inline fail-closed gates, and adds a regression assertion that the AWS observation workflow remains independent of the repository's optional test dependencies
  - second main observation run `32692435062` passed the local gate and AWS identity, then stopped before CloudWatch reduction because ECS correctly omits `workingDirectory` when `/app` is inherited from the immutable image; this is the same already documented boundary from reader run `32401314322`. Branch `codex/vevo-growthbook-zero-collector-runtime-path` accepts only absent or exact `/app` task-definition metadata while still requiring the exact previously localhost-marker-verified image digest, task definition, live task, service, private IP, healthy target, and route, and records that verification source explicitly in sanitized evidence
  - read-only observation run `32692688625` succeeded on main commit `bed02cd3176c960d7423d97486bc67d649601241`: the exact Fargate runtime was task `a3abdbcdd3914c95bb08f03b83eab5fe`, private IP `172.31.21.213`, service `vevo-growthbook-collector-production`, task definition `:2`, path `/app`, immutable digest `sha256:e9aeee45f457dca5e7cb8f6a80f37763de0bb7f61c96f614d79e222fe4707058`, and healthy target; the frozen `2026-08-24T04:30:00Z..2026-08-24T04:50:00Z` window contained exactly `0` API `POST /v1/events` requests and `0` accepted collector receipts
  - the downloaded aggregate-only evidence SHA-256 is `43140aa030225ac927fd6ddd92904fe8d730230174afe7525371c235accfb745`; it contains no CloudWatch messages, event/request IDs, credentials, customer/order data, or mutation. Branch `codex/vevo-growthbook-record-zero-collector` adds an offline, canonical, hash-bound recorder that closes only the zero-traffic QA evidence fields while keeping GrowthBook Draft, GTM unpublished, traffic/allocation `0%`, and controlled activation review pending; the recorded manifest passes all `606` Python tests, `24` focused tests, the exact activation validator, security CI, Ruff, Python compilation, and `git diff --check`
  - branch `codex/vevo-growthbook-mobile-consent-qa` passes the full `594`-test Python suite, all `9` storefront JavaScript tests, `12` focused activation tests, the exact activation validator, Ruff, Python compile, JSON parsing, and `git diff --check`
  - branch `codex/vevo-growthbook-desktop-qa-evidence` passes the full `594`-test Python suite, all `9` storefront JavaScript tests, `22` focused activation/collector/builder tests, activation/workspace/security validators, changed-file Ruff and Python compile, JSON parsing, and `git diff --check`
  - plaintext credentials, the RSA key/certificate, local encrypted credential artifact, and GitHub artifact `9486585740` were deleted after the successful connection; the sanitized evidence JSON was retained
  - no local dev server, worker, watcher, tunnel, or Docker process was started
  - authenticated activation preflight on `2026-08-24` read back GTM workspace `17` with exactly `5` added / `0` modified / `0` removed objects and current live container version `14`; GrowthBook build `5.0.1+8f1db44` has live feature revision `2` with Production disabled and staging enabled, while draft revision `3` contains one staging-only Preview rule and one production-only Draft rule for `exp_19g6mmt5wugpk` at `100%` experiment traffic and `50/50`
  - schema-`5` `activation_preflight` binds that UI read-back to source main `a37ac43189898550e7fa2cf31f842c1985704bd7`, zero-traffic evidence SHA-256 `43140aa030225ac927fd6ddd92904fe8d730230174afe7525371c235accfb745`, GTM artifact SHA-256 `d6861bcbe002a96f82a4a29882723002cd6c797177194bdd93f67e6cf2eba8df`, Production clone SHA-256 `b2f96b7047321f11da4f00c7886c4b9422d7759428534f8fd5534ee1299f2030`, and rollback target GTM version `14`; Production traffic and activation flags remain closed until post-start evidence is recorded
  - GTM pre-publish Consent Overview subsequently exposed five tags without explicit consent metadata: the four new GrowthBook tags `54`, `51`, `55`, and `53`, plus unrelated pre-existing Microsoft Clarity tag `43`; no tag was saved or published during the observation
  - schema-`6` closes GTM publishing and GrowthBook start fail-closed until exactly the four GrowthBook tags are marked `no additional consent required`, reloaded, and reverified through Preview; Clarity tag `43` is explicitly excluded from this change
  - exact saved-value read-back now verifies `no additional consent required` on tags `54`, `51`, `55`, and `53`; Consent Overview leaves only unrelated pre-existing Clarity tag `43` unconfigured, while workspace `17` remains exactly `5` added / `0` modified / `0` removed and live container version remains `14`
  - repeated Preview QA passed reject/regrant: all seven Consent Mode signals changed from denied back to granted; Loader succeeded in both states; the GrowthBook SDK script count was zero while denied; Meta pageview tags `29` and `31` were consent-blocked while denied and successful after regrant; all five user-facing consent categories were restored; Tag Assistant console stayed at zero errors
  - product QA retained the exact `Pridať do košíka` text, no `vevo-gb-cta-brand-contrast` class, pre-existing cart count `2`, and no cart action; GrowthBook live revision `2` still has zero Production rules, while draft revision `3` retains the Production experiment as Draft
  - Tag Assistant also displayed one unattributed consent-timing diagnostic; code inspection confirms the GrowthBook client reads only BiznisWeb's `FloxSettings` consent bitmask and does not invoke the GTM Consent API, while the exact deny/regrant behavior and console gate passed
  - schema-`7` recorded the consent read-back and authorized only a zero-allocation GTM publish; PR `#376` merged it as `aa1d4a17a24f64808de3ebdd6441ddc375a0f15c`
  - GTM workspace `17` was published as live container version `15` at zero allocation; public GTM read-back is byte-stable at SHA-256 `48816d60331c6df39c15161df4b6b0222b0313382c5b0600fdfd34dfbd11b481` and contains the Loader plus all three bridge markers
  - the task-scoped Production SDK key is present exactly once in the live container but was not recorded; its authenticated public feature endpoint returns HTTP `200`, byte-stable SHA-256 `8a85bd5f83d171e3906117b8b6d8fc5d58fea784ad2e1f8fc27745a911537b89`, zero features, no `vevo-sk-aa-assignment`, and zero rules, so Production assignment remains impossible
  - GrowthBook live revision `2` remains Production-disabled with zero Production rules; draft revision `3` and Production A/A remain unstarted; prices, product content, Meta Ads, BiznisWeb, cart, checkout, and orders were unchanged
  - schema-`8` and its read-only post-publish workflow merged through PR `#377` as main commit `cfe10bd1f53b0b3f41433cd503b543cf242c95e3`
  - protected run `32741487449` passed the exact AWS/Fargate hard gate at `2026-08-24T14:53:50Z`: instance `N/A:Fargate`, private IP `172.31.21.213`, service `vevo-growthbook-collector-production`, task definition `:2`, path `/app`, immutable image digest `sha256:e9aeee45f457dca5e7cb8f6a80f37763de0bb7f61c96f614d79e222fe4707058`, and healthy target
  - its frozen `2026-08-24T14:34:30Z..2026-08-24T14:38:00Z` window contains exactly `0` API `POST /v1/events` requests and `0` accepted collector receipts; the independently downloaded canonical artifact SHA-256 is `1cbfcbe6673822210cf36f771c1449c4bafa83d0ef2f8c84102285e5296e6a8b`
  - the artifact contains no CloudWatch messages, event/request IDs, credentials, customer/order data, or AWS/GrowthBook/GTM/Meta Ads/BiznisWeb/commerce mutation; schema-`9` binds that evidence and opens only the separate Production A/A start plus feature-revision-`3` review while actual traffic remains closed at `0%`
  - the explicitly authorized Production A/A activation was executed on `2026-08-25`: GrowthBook started only experiment `exp_19g6mmt5wugpk` and atomically published only linked feature revision `3`; no separate token or API key was created
  - authenticated live read-back shows experiment key `vevo-sk-aa-001` as `Running`, feature `vevo-sk-aa-assignment` revision `3` as `Live`, Production-only environment count `1/2`, `100%` experiment traffic, frozen `50/50` weights, Production data source `ds_19g6mmt5stlp6`, Bayesian default, CUPED off, post-stratification off, activation metric empty, one goal, six secondary metrics, and one guardrail; CTA experiment `exp_19g6mmt1qxzrp` remains `Draft`
  - a fresh Chrome Tag Assistant session connected to `GTM-5ZB5LFGB`, found four Google tags, fired the Production Loader once per observed page, and retained zero console errors; the product CTA text stayed `Pridať do košíka`, no CTA experiment class appeared, the pre-existing cart count stayed `2`, and no cart, checkout, or order action was performed
  - two same-session product loads were completed for sticky-assignment verification; Chrome's isolated automation context cannot directly read the page-world variation without exposing browser storage, so the value was intentionally left unrecorded and the exact `2026-08-25T05:34:30Z..2026-08-25T05:44:30Z` backend window was frozen for identity-free proof
  - branch `codex/vevo-growthbook-activation-smoke` adds a canonical browser observation, an offline raw-event reducer that emits no event/device IDs, and a main-only AWS read-only workflow that re-verifies instance `N/A:Fargate`, current private IP, service `vevo-growthbook-collector-production`, task definition `:2`, runtime `/app`, immutable image, healthy target, and `POST /v1/events` before proving accepted delivery plus a sticky-consistent repeat assignment; it has no AWS/GrowthBook/GTM/Meta Ads/BiznisWeb/commerce mutation path
  - focused activation smoke/activation tests (`25`), Ruff, Python compilation, YAML parsing, security CI, JSON parsing, and `git diff --check` pass; no local server, worker, watcher, tunnel, Docker process, or persistent runtime was started
  - PR `#381` merged the workflow as `976d4a16bab15df67a7b6dfd46b95a192b74d41c`; first main run `32814779233` stopped before AWS credentials because its redundant runner-side test list imported optional PyYAML, so it performed no AWS read, upload, or external mutation
  - the fail-closed recovery keeps the dependency-free activation validator and reducer tests before AWS, removes only the two redundant PyYAML-dependent test imports already covered by reviewed CI, and adds a regression test that the protected runner gate stays independent of optional PyYAML
  - recovery PR `#382` merged as `55574da7224f8878f05bd37553350832b0080db4`; its exact-main run `32815054224` passed the dependency-free local gate, obtained bounded read-only AWS credentials, and passed the Production runtime hard gate with Fargate private IP `172.31.21.213`, service `vevo-growthbook-collector-production`, path `/app`, healthy target, and exact `POST /v1/events` route
  - run `32815054224` then failed before any raw-object download or artifact upload because the AWS CLI had correctly auto-paginated a daily prefix containing more than 1,000 objects while the workflow incorrectly treated the retained first-page `IsTruncated=true` metadata as an incomplete result; no AWS/GrowthBook/GTM/Meta Ads/BiznisWeb/commerce mutation path existed or ran
  - the follow-up uses the AWS CLI paginator with a bounded page size and projects the fully aggregated `Key`/`LastModified` list before exact row validation, preserving read-only access, fail-closed key validation, temporary raw-data cleanup, and identity-free output
  - pagination recovery PR `#383` merged as `08b342363aee62d3a0a70a63177c6ab52f7bba9a`; exact-main run `32815535698` passed the local gate, paginator, runtime identity, healthy route, raw-object selection, and temporary download, then the offline reducer correctly stopped on `collector version drift` before artifact upload
  - the reducer had incorrectly frozen the application's unused default `vevo-growthbook-collector-v1`; the deployed task definition and stack are intentionally versioned as `git-57b29c3b166eabbbabee4d3b8e69d1b56e2ae8e2` by the immutable collector deployment. The follow-up binds that exact version independently in the stack parameter, task-definition environment, and every downloaded raw object; arbitrary or moving versions remain rejected
  - collector-version binding PR `#384` merged as `1965091059e5a35518265aafd282db842f8ea5d3`; exact-main run `32815955896` passed every gate and uploaded only the sanitized activation artifact
  - the successful frozen `2026-08-25T05:34:30Z..2026-08-25T05:44:30Z` smoke observed `8` API requests, `8` accepted receipts, `8` raw events, `4` target exposures, `3` product exposures, one repeat device, one sticky-consistent repeat device, zero sticky conflicts, and the `variant` arm for that single anonymous device; this one-device smoke proves delivery and stickiness, not the population 50/50 split
  - the independently downloaded artifact SHA-256 is `c21d7418656ad0841851a8afbc642a6ea39328e2151e6dc647ce8c59c06c1823`; it contains no raw AWS payloads, CloudWatch messages, event/device IDs, customer/order data, credentials, or external mutation
  - schema `10` now records GrowthBook `Running`, Production-only `100%`, exact `50/50`, feature revision `3` live, the browser-observation and smoke-artifact hashes, Tag Assistant/commerce read-back, and the collector aggregate; the workspace records `running_production_aa_only` at `100%` while CTA remains `unstarted_draft`, `no_live_rules`, and `0%`
  - the checked-in activation and workspace validators, historical recorder fixtures, `628` Python tests, `9` storefront JavaScript tests, security CI, scoped Ruff for every changed Python file, Python compilation, JSON/YAML checks, and `git diff --check` pass; the repository-wide Ruff baseline still has unrelated pre-existing findings, and no local server, worker, watcher, tunnel, Docker process, or persistent runtime was started
  - the separate Production reconciliation hard gate is now open: the Production collector remains `N/A:Fargate`, private IP `172.31.21.213`, service `vevo-growthbook-collector-production`, path `/app`; the latest unchanged source reporting host gate is run `32441607094`, `N/A:Fargate`, private IP `172.31.30.253`, service `vevo-daily-report-email`, task definition `vevo-reporting-daily:33`, path `/app`, with the localhost marker and exit `0` verified before implementation
  - branch `codex/vevo-growthbook-production-reconciliation` prepares one environment-parameterized disabled-first deploy boundary while retaining separate Preview/Production stacks, task families, schedules, buckets, curated facts, alarms, DLQs, and metric names; Production is fixed to `vevo-growthbook-reconcile-production` at `03:45 Europe/Bratislava`, Preview remains at `03:30`, and only an exact `git-${GITHUB_SHA}` reporting image may be deployed
  - the protected workflow must re-read the source schedule unchanged, prove the exact Production reporting policy, run `/app` localhost health and marker gates, create the Production stack disabled, complete one bounded 40-partition/50,000-event reconciliation with generated/published count parity, then enable only the verified schedule and upload one sanitized no-identity evidence artifact; no raw AWS/CloudWatch payload is uploaded
  - local verification passes: `635` Python tests, `25` focused Production/Preview reconciliation tests, repository security CI, CloudFormation lint, scoped Ruff, Python compilation, shell syntax, JSON/YAML parsing, `9` storefront JavaScript tests, and `git diff --check`; no local server, worker, watcher, tunnel, Docker process, or persistent runtime was started
  - implementation PR `#386` merged as `af17cb0572b1e35dae5d0ca242460e9601cf0c2a`; its exact ECR build `32819437141` succeeded with immutable digest `sha256:e737d7104e8f14028d7c964cd6355d7e866a3da191622992310cc6735d8db99d`
  - first protected Production deploy run `32819644845` stopped before the host gate, stack creation, schedule activation, one-shot reconciliation, or evidence upload because the existing exact VEVO reporting role did not yet have the Production reporting managed policy attached; it registered but did not run or schedule unreferenced task definition `vevo-growthbook-reconcile-production:1`
  - recovery branch `codex/vevo-growthbook-production-policy-gate` moves the IAM gate before task registration, requires an exact six-statement Production-only S3/Athena/Glue policy document and exact role/policy/workgroup/database/bucket identities, attaches only that verified managed policy idempotently, and requires attachment read-back before any subsequent task definition or stack change
  - recovery verification passes: `636` Python tests, `15` focused workflow tests, repository security CI, scoped Ruff, Python compilation, YAML parsing, and `git diff --check`
  - policy-gate recovery PR `#387` merged as `36f121157c011f55309d38ffddc02316ac8a946f`; exact ECR build `32820337649` succeeded with digest `sha256:3293b36aa9f78d86ea35b4a9ca0a053494c777341fc2cc8dc05c71d7f2ec8b2f`
  - second protected Production run `32820526125` exactly verified and attached the Production reporting policy, registered task definition `vevo-growthbook-reconcile-production:2`, and passed the `/app` Fargate host gate at private IP `172.31.8.183` with task `ca3a0a39283a40adbddc1d64f4678deb`; it then stopped before change-set creation because the parameter builder's exact `GROWTHBOOK_ENVIRONMENT` input was not exported under that name
  - sanitized failure read-back shows reconciliation stack `ABSENT`, target schedule absent, no alarms/queue/role resources, source schedule `vevo-daily-report-email` still `ENABLED` on `vevo-reporting-daily:33`, and no one-shot reconciliation or evidence upload; branch `codex/vevo-growthbook-production-env-gate` exports the selected environment under both reviewed internal contracts and adds regression/security markers
  - environment-contract recovery PR `#388` merged as `cf92eb0e007fb9a9163068a2735e5becc0327f03`; its exact image is pinned by digest `sha256:51d70f4976083f86a0d7c5e542c21d93e5bbeff3d75d2af31f620b42df1a1b92`
  - protected Production deploy run `32821210244` succeeded end to end: the exact reporting policy/document and attachment were re-read, task definition `vevo-growthbook-reconcile-production:3` was registered, and the Fargate localhost health plus `/app` marker hard gate passed on task `17d2ea85e2304d2ca0f16ef3ad32913d` at private IP `172.31.39.76`
  - stack `vevo-growthbook-reconciliation-production` was created disabled-first; one-shot task `496df38886674a8885866016e82c5ae6` at `172.31.38.184` then passed curated publish parity with `0` raw events, `0` device facts, `0` performance facts, and `2` quality reports. The zero bootstrap is expected because the A/A began during the still-open current UTC partition and is not population-acceptance evidence
  - only after the one-shot passed, schedule `vevo-growthbook-reconcile-production` was enabled on `cron(45 3 * * ? *)`, timezone `Europe/Bratislava`, with its retained encrypted DLQ and three alarms; source schedule `vevo-daily-report-email` remains unchanged on task definition `:33`
  - the exact canonical sanitized artifact is recorded at `projects/vevo/growthbook_production_reconciliation_deploy_evidence.json` with SHA-256 `21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb`; it contains no credentials, raw AWS/CloudWatch payloads, event/device/customer/order IDs, or commerce/Meta/GTM/GrowthBook/BiznisWeb mutations
  - Production A/A remains the only running Production experiment at `100%` traffic and frozen `50/50`; CTA remains stopped. The first server-side scheduled reconciliation is due `2026-08-26 03:45 Europe/Bratislava` and does not depend on this PC being powered on
  - the A/A measurement plan is now pre-registered from the immutable activation read-back before population outcomes are available: start `2026-08-25T22:00:00Z`, at least seven full Europe/Bratislava dates `2026-08-26..2026-09-01`, and at least `1,000` eligible devices. The first resolution checkpoint is the successful `2026-09-02 03:45 Europe/Bratislava` reconciliation
  - if the minimum sample is not present then, the plan extends by exactly one whole Europe/Bratislava calendar day at each successful `03:45` reconciliation until the first checkpoint with at least `1,000` eligible devices. Resolution may inspect only the cumulative eligible-device count, never arm outcomes, split, SRM, conversion, revenue, or performance; post-hoc window selection is explicitly forbidden
  - `growthbook_aa_snapshot.json` schema `2` keeps the resolved through-boundary null and both future evidence components closed until that deterministic stopping rule is satisfied. The offline validator independently recomputes the start, minimum boundary, provenance, and rule from activation run `32815955896`, reconciliation run `32821210244`, acceptance thresholds, timezone, and verified schedule
  - after resolution, the automated producer must additionally bind a canonical reporting-quality object generated after the resolved through-boundary; the manual producer rejects any observation that differs from that same resolved window or precedes its completion. Neither can expose raw AWS/CloudWatch payloads or identities, and neither can mutate GrowthBook, GTM, Meta Ads, BiznisWeb, or commerce
  - pre-registration verification passes the full `643`-test Python suite, all `9` storefront JavaScript tests, `43` focused A/A/workspace tests, activation/workspace/window/security validators, scoped Ruff, Python compilation, JSON/YAML parsing, and `git diff --check`; no local server, worker, watcher, tunnel, Docker process, or persistent runtime was started
  - active heartbeat `vevo-production-a-a-monitoring` is scheduled daily at `09:00 Europe/Bratislava` for local result readback; the repository-owned GitHub monitor remains the PC-independent `04:15` execution path. The heartbeat is bound to the protected checkpoint workflow, independent artifact ZIP/SHA/run/commit verification, offline recorder, and outcome-blind whole-local-day extension rule. It must not dispatch before the first due gate, read arm/outcome metrics, or start CTA/Meta changes automatically
  - `projects/vevo/GROWTHBOOK_MONITORING_AUTOMATION_RUNBOOK.md` now versions the heartbeat contract for multi-PC recovery: before `2026-09-02 03:45 Europe/Bratislava` it permits only sanitized AWS/Fargate/schedule/marker/alarm/DLQ/source-schedule health checks and explicitly forbids population counts, arms, SRM, outcomes, Meta dimensions, performance, checkpoint dispatch, or any external mutation. At the due gate it requires clean exact `main`, the single protected outcome-blind workflow, independent artifact ZIP/SHA/run/commit verification, an offline recorder transition on a short-lived branch, validation, and reviewed PR; later A/A and CTA transitions remain ordered by their checked-in fail-closed gates and no decision is auto-applied
  - the repository-owned daily infra monitor removes the remaining local-credential dependency: `.github/workflows/monitor-vevo-growthbook-production-aa-infra.yml` is main-only, uses the managed GitHub AWS secrets, and schedules both UTC equivalents of `04:15 Europe/Bratislava` behind a pre-credential DST gate so exactly one slot executes. Before the first natural run it can verify only stack/schedule/task-definition/image, alarm, empty-DLQ, and unchanged-source structure; afterward it binds the exact successful Fargate task/private IP/service/`/app`, marker hash, and generated/published parity hash without emitting counts. It contains no Athena/S3 data query or mutation client, deletes every raw AWS/CloudWatch response before uploading one canonical identity-free health artifact, and is enforced by a strict offline validator plus workflow/security regression tests. A direct local AWS readback stopped at `NoCredentials` before any API state change, confirming the monitor must use the managed GitHub boundary rather than copy credentials to a PC
  - first main infra-health run `32850492134` passed the pre-credential result-blind gate and managed AWS authentication, then stopped before artifact creation because the monitor and the three future A/A/CTA readback workflows expected stale `CREATE_COMPLETE` stack states. The protected collector deploy and reconciliation activation both intentionally leave their exact stacks at `UPDATE_COMPLETE`; the fix narrows all four readers to that exact verified terminal state, adds regression assertions, and changes no stack, runtime, schedule, experiment, data, or commerce state. The failed run made no Athena/S3 data query and uploaded no artifact
  - second infra-health run `32850976410` passed the corrected exact stack gate and then stopped on the still-composite schedule invariant before task/log access or artifact creation. The next recovery keeps every invariant closed but emits only the failed invariant labels, never raw schedule values or AWS responses, so the exact deployed contract can be reconciled without guessing or leaking data; the run again made no Athena/S3 data query and no mutation
  - third infra-health run `32851563308` on exact `main` `331a6b2a` isolated the only schedule mismatch as `cluster`; all other labeled schedule invariants passed and the workflow again stopped before task/log access or artifact creation, with no Athena/S3 data query and no mutation. The checked-in deploy contract proves the reconciliation stack parameter `ClusterArn` is sourced from the unchanged `vevo-daily-report-email` target, not from the separate collector ECS cluster. All four Production reconciliation readers now bind the schedule and ECS task lookups to that exact CloudFormation parameter and independently require the source schedule target to match it; `25` focused workflow tests, security validation, four YAML parses, scoped Ruff, `git diff --check`, and the full `746`-test suite pass
  - fourth infra-health run `32852262544` succeeded on exact `main` `8ee0602dff5b93a7320968ba27ade64033c9f313` before the first natural reconciliation. Its single canonical artifact `vevo-growthbook-production-aa-infra-health` has GitHub ZIP digest `sha256:559b35b62341d8d410423b7e93dfc33c1b824162d7118a2669cc2925da963bd2`, contains exactly one JSON, and passes the offline validator against the checked-in deploy evidence. The sanitized state is `waiting_for_first_natural_run`, schedule `ENABLED`, alarms clear, DLQ empty, source schedule unchanged, population/outcome reads false, and AWS mutation false; local verification files were removed. Heartbeat `vevo-production-a-a-monitoring` remains active daily at `09:00 Europe/Bratislava` and observes the PC-independent `04:15` managed GitHub workflow first, waits instead of duplicating queued/in-progress runs, verifies the single canonical artifact, and never attempts local AWS credentials
  - pre-CTA statistical audit independently reproduced the provisional planning rate `32.815965%`, the `25%` relative MDE target `41.019956%`, and the fixed sample `542` devices per arm / `1,084` total at two-sided alpha `5%` and power `80%`. The final-look privacy query was hardened before activation: PII, full-URL, and click-identifier checks now scan the entire frozen raw-event window instead of only its first `100` events. The query remains aggregate-only, identity-free, one-look, and mutation-free; its pinned SHA-256 is `d4a586e238e364c29281ffd1c6a736dd512fe25894c25bd36befe3014c46f913`
  - before any A/A population, arm, outcome, Meta-dimension, or performance result was opened, the A/A privacy contract was tightened from a fixed `100`-row sample to the entire frozen raw-event window. Acceptance schema `2` requires `audited_row_count == total_stored_row_count > 0`; incomplete coverage is `NOT_READY`, while any PII/full-URL/click-identifier finding remains `FAIL`. The protected workflow still emits one aggregate identity-free artifact and never uploads raw rows; all duration, population, split, SRM, Meta, commerce, and performance thresholds remain unchanged. The acceptance contract SHA-256 is `88d5dd6ef2d9b6d90360a0a6e5bd79eff39eff8f372d254aed03ebbd97434a77` and the protected evidence workflow SHA-256 is `e25044cd349d8b57d9d0135f2bd7490e66425197e60c977774043c070427dda5`. No workflow was dispatched and no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, storefront, price, cart, checkout, payment, stock, or order state changed
  - PR `#391` merged the missing deterministic checkpoint path to `main` as `fe976ac89f190be8beadadd262389239e1c1f938`: the main-only, explicitly confirmed workflow can run only inside the exact daily post-`03:45` gate, re-verifies account/stack/schedule/task-definition/image/Fargate task identity, binds the existing localhost host-gate evidence, requires the scheduled success marker, generated/published parity, three clear alarms, empty DLQ, and unchanged source schedule, then runs one Athena query returning only `COUNT(DISTINCT device_id)` for eligible uncontaminated facts
  - the checkpoint artifact is canonical, run/commit/hash-bound, identity-free, and records no arm counts, outcomes, conversion, revenue, CM1, performance, Meta dimensions, raw AWS payloads, or CloudWatch messages. The offline recorder accepts checkpoints only in consecutive whole-local-day order, extends below `1,000`, resolves at the first qualifying checkpoint, and keeps both evidence producers, final snapshot, CTA, winner, GrowthBook, GTM, Meta Ads, BiznisWeb, and commerce mutations closed
  - checkpoint-path verification passes the full `655`-test Python suite, all `9` storefront JavaScript tests, `55` focused A/A/workspace tests, activation/workspace/window/security validators, scoped Ruff, Python compilation, JSON/YAML and inline-Python parsing, and `git diff --check`; no local server, worker, watcher, tunnel, Docker process, or persistent runtime was started
  - main runs `32826255870` (Env Check), `32826255733` (Observability Baseline), and `32826255919` (Build and Push ECR) all passed for `fe976ac8`; the automatically published image was not deployed to any service or schedule
  - the post-resolution evidence path no longer requires manual manifest editing: `record_growthbook_aa_evidence_gates.py` opens the automated producer only from an exact canonical quality object whose key timestamp, generation boundary, metric contract, count identities, and eligible-device population match the resolved stopping checkpoint; it opens the manual producer only from the canonical reviewed browser observation for that exact window
  - the same offline recorder binds each independently downloaded component artifact to its successful workflow run ID, main commit, and SHA-256, closes the producer after recording, and opens `snapshot_build_allowed` only when both components are verified. It has no AWS, GitHub, GrowthBook, GTM, Meta Ads, BiznisWeb, browser, commerce, or network client
  - the lifecycle validator and security CI now accept only the deterministic pending, resolved, source-open, component-verified, and two-component snapshot-open states; the earlier CI assumption that every checked-in manifest must remain permanently pending was removed before the first checkpoint is due
  - post-resolution recorder verification passes the full `660`-test Python suite, all `9` storefront JavaScript tests, `17` focused recorder/window tests, activation/workspace/window/security validators, scoped Ruff, Python compilation, and `git diff --check`; no local server, worker, watcher, tunnel, Docker process, or persistent runtime was started
  - PR `#393` merged the post-resolution recorder path to `main` as `0233a5cdf5df0d7070a07dfdb21a939c70bafdb9`. Main runs `32828527694` (Env Check), `32828527688` (Observability Baseline), and `32828527673` (Build and Push ECR) all passed; the automatically published image was not deployed to any service or schedule
  - PR `#395` merged the previously missing audited boundary after A/A `PASS` to `main` as `9872daffc59c395a5069e3e2a9db3a2ee7a59fa0`. `growthbook_production_aa_completion.json` is currently closed at `waiting_for_verified_aa_pass`; no stop or CTA gate is open
  - the new offline completion recorder accepts only the exact canonical snapshot and matching decision, independently re-runs the versioned A/A evaluator, binds successful workflow run/main/hash provenance, and opens only a reviewed manual stop gate. A separate canonical readback must prove the exact A/A stopped at zero Production allocation, no Production A/A/CTA rule, CTA still draft, staging preserved, GTM version `15` unchanged, clean desktop/mobile storefront behavior, and no Meta Ads, BiznisWeb, collector/reporting, price, cart, checkout, or order mutation
  - after that readback, the recorder can move only the versioned workspace to `production_aa_completed_cta_sample_freeze_pending_pro_quantiles_blocked`; CTA activation and every automatic external mutation remain false. The activation manifest remains the immutable historical activation record, while the completion manifest becomes the current stop/readback audit trail
  - completion-contract verification passes the full `666`-test Python suite, all `9` storefront JavaScript tests, `27` focused completion/workspace tests, completion/workspace/measurement-window/security validators, scoped Ruff, Python compilation, JSON parsing, and `git diff --check`. No live GrowthBook, GTM, Meta Ads, BiznisWeb, collector, reporting, price, cart, checkout, or order change was performed, and no local runtime was started
  - all post-merge main workflows passed: Env Check `32830982845`, Observability Baseline `32830982857`, and Build and Push ECR `32830982838`. The commit tag and `latest` are byte-identical at `sha256:5632a60085c0ee19544145c81d9efd4fc8244667cf4c1c10a9c8f42f93076e62`; the image was built only and was not deployed to any service or schedule
  - the missing post-A/A product-page baseline path is now prepared fail-closed: `growthbook_cta_baseline.json` binds the exact SQL template SHA-256, the resolved A/A window, eligible uncontaminated first product exposure, same-assignment cart integrity, and a complete 24-hour follow-up. The result is exactly `exposed_devices` plus `converted_devices`; it cannot emit arms, identities, customer/order data, a winner, or a CTA activation
  - `collect-vevo-growthbook-cta-baseline.yml` renders and validates that query before AWS credentials and cannot pass while the current completion manifest remains `waiting_for_verified_aa_pass`. After the future verified stop, it re-checks the exact Production Fargate task/private IP/service/`/app` identity against the already localhost-marker-gated immutable foundation, verifies the two Glue schemas, removes all temporary AWS/query responses, and uploads one canonical identity-free artifact only
  - the final CTA sample remains a separate offline, independently hash-bound `freeze_growthbook_cta_sample.py` transition that updates only the versioned sample plan/workspace and keeps CTA draft, Production allocation `0%`, `activation_allowed=false`, prices/cart/checkout/orders, Meta Ads, GTM, BiznisWeb, collector, and reporting unchanged
  - CTA baseline preparation verification passes the full `677`-test Python suite, all `9` storefront JavaScript tests, `11` focused baseline tests, completion/measurement-window/workspace/baseline/security validators, scoped Ruff, Python compilation, JSON/YAML and inline-Python parsing, and `git diff --check`. The standalone `actionlint` binary is not installed on this Windows host, so workflow syntax is covered by the repository's YAML and inline-Python compilation tests. No local server, worker, watcher, tunnel, Docker process, AWS query, or live external mutation was started
  - PR `#397` merged the protected CTA baseline producer and handoff to `main` as `c9ba5a6a5439ae82fd280063e160e1e8b30a7195`. Post-merge Env Check `32833701850`, Observability Baseline `32833701825`, and Build and Push ECR `32833701817` all passed. Commit tag and `latest` are byte-identical at `sha256:bf91836a176de84dd661fa550487800f73ab655fee9bd82a6d207e89a6917d08`; the image was built only and was not deployed to any service or schedule
  - the next post-sample boundary is now machine-readable but remains closed: `growthbook_cta_activation.json` and its offline recorder require exact A/A PASS/stop, the protected snapshot hash, frozen sample, verified lifecycle value reconciliation, immutable design/decision hashes, a CTA-only checked-in Production registry, and a canonical successful runtime observation before opening a reviewed manual start. Runtime evidence must bind the exact Fargate private IP/service/`/app`/task definition/image, direct localhost marker, healthy target, zero pre-start CTA events, zero A/A/CTA allocation, GTM version `15`, and zero GTM changes. A separate canonical start readback requires CTA to be the only active Production experiment plus consent, desktop/mobile, both-variation, sticky collector, CSS, and unchanged-commerce verification. Both offline transitions forbid automatic GrowthBook/GTM/Meta/BiznisWeb/collector/reporting/commerce mutation and winner calls. The checked-in manifest is still waiting; no registry, runtime, UI, traffic, or commerce state changed
  - CTA activation-contract preparation on `codex/vevo-cta-activation-contract` passes the full `687`-test Python suite, all `9` storefront JavaScript tests, `10` focused activation-recorder tests, workspace/measurement-window/A/A-completion/CTA-baseline/security validators, scoped Ruff, Python compilation, GrowthBook JSON parsing, and `git diff --check`. No local server, worker, watcher, tunnel, Docker process, AWS query/deploy, browser/UI action, or live external mutation was started
  - PR `#399` merged the fail-closed CTA activation/readback contract to `main` as `f6011f979f9e37dc5095f3d62ce5870cdbfb2420`. All PR checks passed. Post-merge Env Check `32836310540`, Observability Baseline `32836309917`, and Build and Push ECR `32836310627` passed; commit tag and `latest` are byte-identical at `sha256:32c5f9b7564bcad320899845c4b6306a8f974ef386743f0f101a6890a040f8c2`. The reporting image was built only and was not deployed to the collector, reconciliation, schedule, App Runner, GrowthBook, GTM, Meta Ads, BiznisWeb, or storefront traffic
  - PR `#401` merged the missing protected CTA-only collector runtime bridge to `main` as `9dec2bde4e71e4d34cb53e5ebcf59ec753aa7a4a`, while leaving the current A/A untouched. The new main-only workflow is blocked before AWS credentials unless the future checked-in state proves A/A `PASS` plus verified stop, frozen sample, verified 14-day lifecycle reconciliation, CTA draft at `0%`, GTM version `15` with zero changes, and an exact CTA-only registry. Its predeploy hard gate identifies the existing Fargate task/private IP/service/`/app` and single public route before image build; its candidate changes only the CloudFormation-managed digest-pinned task definition/service while preserving that route. A distinct one-shot task directly verifies localhost `/health`, `/marker.json`, packaged registry SHA-256 and sole CTA key, followed by healthy service target, route/CORS/private-path isolation, and one aggregate-only all-history zero-CTA-event query. The only artifact is canonical and identity-free and records both the healthy service-task IP and the direct localhost host-gate task/IP. A post-update failure restores the exact prior image/version, preserves the route, validates the rollback change set, waits for a healthy target, and repeats the prior image's localhost markers. Local verification passed all `699` Python tests, all `9` storefront JavaScript tests, workspace/measurement-window/completion/security validators, Ruff, Python compilation, JSON/YAML parsing, and `git diff --check`; all PR checks passed. Post-merge Env Check `32838746641`, Observability Baseline `32838746542`, and Build and Push ECR `32838746559` passed; commit tag and `latest` are byte-identical at `sha256:479f5f38cc6e1ad1399659526fd52e3b05ecff4e00cf43e15f7242ea3447e637`. The reporting image was built only: the CTA collector workflow was not dispatched, no collector/runtime/query or UI action occurred, and no GrowthBook, GTM, Meta Ads, BiznisWeb, price, product, cart, checkout, payment, stock, or order mutation was performed. No local server, worker, watcher, tunnel, or Docker runtime was started
  - the post-start CTA assignment window is now separately machine-readable and outcome-blind but remains hard-disabled at `waiting_for_verified_cta_start`. After the future canonical start readback, the offline initializer binds the exact activation, start observation, frozen sample, decision contract, and reconciliation evidence hashes, then freezes the first full Europe/Bratislava day, day-14/day-42 boundaries, and first due `03:45` checkpoint. The main-only explicit-confirmation workflow can then hard-gate the exact stopped reconciliation Fargate task/private IP/service `vevo-growthbook-reconcile-production`/path `/app`/task definition/image, inherited localhost markers, successful publish parity, three clear alarms, empty DLQ, and unchanged source schedule before running one Athena `COUNT(DISTINCT device_id)` for only the exact CTA eligible uncontaminated population. It does not select `variation_id` or any outcome. The offline recorder requires canonical bytes plus independently supplied successful run/main/SHA-256 provenance and can only extend one whole local day or open a reviewed manual CTA stop at the frozen first-`N` target/day 42. It cannot stop GrowthBook automatically, read arms/outcomes, call a winner, or mutate AWS resources, GTM, Meta Ads, BiznisWeb, collector/reporting, prices, cart, checkout, payments, stock, or orders. The workflow was not dispatched and no live state changed
  - CTA outcome-blind window preparation on `codex/vevo-cta-window-checkpoint` passes the full `715`-test Python suite, all `9` storefront JavaScript tests, the CTA measurement-window/workspace/security validators, scoped Ruff, Python and workflow inline-Python compilation, JSON/YAML parsing, and `git diff --check`. No local server, worker, watcher, tunnel, Docker process, AWS query/workflow/deploy, browser/UI action, or live external mutation was started
  - PR `#403` merged the outcome-blind CTA assignment checkpoint path to `main` as `6f83c07bd6961a264592ab1f4531369288e54574`; all PR checks passed. Post-merge Env Check `32841734569`, Observability Baseline `32841734493`, and Build and Push ECR `32841734579` passed. Commit tag and `latest` are byte-identical at `sha256:ceb537e5655c109a24496588789ea7fe9b3835c0be47bbbd79ab9873971688d4`. The reporting image was built only and was not deployed; the CTA checkpoint workflow remains hard-disabled and was not dispatched, and no AWS query, collector/reconciliation/schedule, GrowthBook, GTM, Meta Ads, BiznisWeb, storefront, price, cart, checkout, payment, stock, or order state changed
  - the reviewed CTA stop/follow-up handoff is now machine-readable but remains closed at `waiting_for_assignment_stop_review`. Only the future resolved outcome-blind window can admit one canonical post-stop readback bound to its final checkpoint and original CTA start. The offline recorder requires the exact CTA stopped at zero Production allocation, only its Production rule removed, staging preserved, an advanced feature revision, GTM version `15` unchanged, desktop/mobile control behavior, at least 300 seconds with zero new CTA assignment/exposure, unchanged commerce, no identities, no outcome read, and no winner or non-GrowthBook mutation. It builds and validates the completion, activation, measurement, and workspace outputs before writing, then freezes `final_snapshot_due_utc` at exactly stop plus 14 days. Current live A/A collection, GrowthBook, GTM, Meta Ads, BiznisWeb, AWS, storefront, prices, cart, checkout, payments, stock, and orders remain unchanged; no browser, workflow, query, deploy, or local runtime was started
  - CTA stop/follow-up preparation on `codex/vevo-cta-stop-followup` passes the full `721`-test Python suite, all `9` storefront JavaScript tests, `48` focused completion/window/activation/workspace tests, CTA measurement/completion/workspace/security validators, scoped Ruff, Python compilation, and `git diff --check`
  - PR `#405` merged the reviewed CTA stop/follow-up handoff to `main` as `7c7e9332ed6b636b4fe1a48f942938b1f7d10455`; all four PR checks passed. Post-merge Env Check `32843957335`, Observability Baseline `32843957368`, and Build and Push ECR `32843957284` passed. Commit tag and `latest` are byte-identical at `sha256:eb0dd6d4eedde985edb5f80552c3f0be9aae8514214c51fb7481d0e09a50d67b`. The image was built only and was not deployed; the live A/A, CTA stop gate, GrowthBook, GTM, Meta Ads, BiznisWeb, AWS runtime, storefront, prices, cart, checkout, payments, stock, and orders remain unchanged, and no checkpoint or CTA workflow was dispatched
  - the missing CTA final-look path is now prepared fail-closed on `codex/vevo-cta-final-snapshot`: the stop recorder validates and writes a fifth hash-bound final-snapshot manifest that remains closed until the exact stop plus 14-day follow-up. The main-only workflow refuses every repeat after any earlier outcome-query step started, including failed/cancelled runs; it first verifies account `919341186960`, both exact stacks, the enabled reconciliation and unchanged `vevo-daily-report-email` schedules, digest-pinned Fargate task definition/image, a successful non-diagnostic post-due reconciliation task/private IP, publish parity, clear alarms, empty DLQ, and exact Glue schemas. It then runs one diagnostic Fargate task with direct localhost `/health` and `/app` markers before exactly one Athena query. The query uses raw device IDs only internally for the frozen first-`N` cohort and returns exactly two aggregate arm rows; the sole artifact contains only the canonical identity-free snapshot and offline decision. The separate offline recorder independently recomputes `WIN`/`LOSE`/`INCONCLUSIVE`, records run/commit/hash provenance, closes all read gates, and never applies a recommendation or mutates GrowthBook, GTM, Meta Ads, BiznisWeb, collector/reporting, prices, cart, checkout, payments, stock, or orders
  - final-look preparation passes the full official `735`-test Python suite, `20` focused completion/builder/recorder/workflow tests, CTA completion/measurement/final-snapshot/workspace/security validators, scoped Ruff, Python compilation, YAML plus every inline workflow Python block, and `git diff --check`. No browser, AWS query, workflow dispatch, deploy, local server, worker, watcher, tunnel, Docker process, or external mutation was started. A raw repository-wide Pytest invocation additionally passed all `735` collected real tests but reports the pre-existing interactive root helper `test_facebook_token.py` as a missing-fixture collection error; repository-wide Ruff likewise retains unrelated historical findings, while the official suite and all changed files pass
  - PR `#407` merged the protected CTA final-snapshot path to `main` as `8d2e26bdb846f12ea75a20a90db89dfde115ec84`; all four PR checks passed. Post-merge Env Check `32847313764`, Observability Baseline `32847314014`, and Build and Push ECR `32847313767` passed. Commit tag and `latest` are byte-identical at `sha256:02af3d108e9c21ae6625a8a3e6ff0a1d029521d12fb7d2107157a4939c16c24d`. The image was built only and was not deployed; the final-look workflow remains locked and was not dispatched, and the live A/A, GrowthBook, GTM, Meta Ads, BiznisWeb, AWS runtime, storefront, prices, cart, checkout, payments, stock, and orders remain unchanged
  - Next exact step: monitor daily reconciliation while collecting from `2026-08-25T22:00:00Z`. At the `2026-09-02 03:45 Europe/Bratislava` checkpoint, dispatch the protected workflow and record its independently downloaded canonical artifact through a reviewed PR. Resolve there if the count is at least `1,000`; otherwise repeat after exactly one more whole local day. Only after resolution, use the committed offline recorder in separate reviewed transitions to bind quality/manual sources, record both successful component artifacts, open the protected snapshot gate, require snapshot `PASS`, execute the PASS-bound manual stop/readback, wait through the exact A/A end plus 24 hours, collect one protected CTA baseline artifact, and freeze its sample offline. Do not activate CTA automatically

- ROY daily report email delivery is disabled, deployed, and host-verified as of `2026-08-21`:
  - PR `#300` merged to `main` as `cd0d9d6a53d66bc90ed5ca777d2c3a4612d0cf8b`
  - `projects/roy/settings.json` sets `"send_daily_report_email": false`; daily report generation and dashboard artifact publication remain enabled
  - `reporting_core/config.py` exposes the setting with a default of `true`, preserving email behavior for VEVO and other projects
  - `daily_report_runner.py` skips only the email dispatch when this project setting is false
  - ECR build `32447071929` passed and published exact digest `sha256:37ad85ee7dc737afcbdd0500232cbdc79ed9480d04c8e1a6f3ac9202ca1bb307`
  - PR `#302` merged to `main` as `2c89d3a02a02c10c9c7160a15f165664c7666bfa`; production smoke can refresh only the selected project task image
  - ROY-only production run `32447411582` passed with `project=roy`, `send_email=true`, and `update_task_image=true`; VEVO was not selected or updated
  - Fargate hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.21.89`, service `roy-daily-report-email`, task definition `roy-reporting-daily:67`, task `1cbdeb6f717a46b6b2416a65f5199255`, runtime `/app`, exact image digest above, and container exit code `0`
  - the untagged `data/roy/report_latest.html` and `data/roy/dashboard_payload_latest.json` were regenerated; marker `LOCALHOST_MARKER_OK` reported `331` daily-profit rows and the UI check returned `UI_SMOKE_OK:roy:daily-profit-loss`
  - the natural email-capable path logged `Daily report email sending disabled by project configuration.` and produced no SES `MessageId`; invoice automation was independently skipped for the smoke
  - external App Runner health returned `ok: true` after the host gate; no local server, worker, watcher, or tunnel was started
  - Next exact step: monitor the next natural `roy-daily-report-email` schedule run on task definition `:67` and confirm the live ROY dashboard refreshes without a new ROY report email

- ROY SD-card purchase-cost correction is merged, deployed, and host-verified (`2026-08-21`):
  - PR `#290` merged as `d9f4d4c3823d880179d8cd58a5cd45a967b57d59`; exact reporting SKUs now map `F_206` to `4.50 EUR` and `12876` to `13.50 EUR`
  - regression coverage proves the current import-code mappings take precedence over the older EAN mappings; the focused check and all `96` tests in `tests.test_reporting_calculation_fixes` passed, together with JSON parsing and `git diff --check`
  - the deploy used current main `e55ccd14b47c660b9b39a5788a1e65a63a98fc1a`, which contains PR `#290`, and immutable digest `sha256:30a23fcd69eb2d7a41195bffa0bc055d38bc2dd706e9eb07d5126675a21a6add`
  - protected workflow `32441607094` passed after a mismatched queued attempt `32441474240` was cancelled before host execution while a newer main image was still building
  - ROY hard-gate identity: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.23.146`, service `roy-daily-report-email`, task `fd8594dd477c485fadda207a0aabd1bf`, task definition `roy-reporting-daily:66`, runtime path `/app`, localhost marker `http://127.0.0.1:8000/marker.json`, and exact digest above
  - the host generated the tagged ROY report for `2025-09-24..2026-08-20`, returned `LOCALHOST_MARKER_OK`, passed `UI_SMOKE_OK:roy:daily-profit-loss`, exited normally, and left the ROY scheduler pinned to task definition `:66`; no report email was sent
  - post-host Chrome verification confirmed the authenticated ROY operations dashboard loads; its current live S3 artifact still shows the preceding `F_206` valuation (`75.90 EUR` for `23` units), because the safety smoke was tagged and intentionally did not promote or email a new daily artifact
  - no local server, worker, watcher, or tunnel was started
  - Next exact step: monitor the next natural `roy-daily-report-email` run on task definition `:66`, then verify the refreshed live inventory rows show unit purchase costs `4.50 EUR` for `F_206` and `13.50 EUR` for `12876`

- VEVO GrowthBook Pro rollout has a concrete, read-only preflight, dated baseline, and execution contract on branch `codex/vevo-growthbook` (`2026-08-20`):
  - the goal is to validate the complete GrowthBook → Meta dimensions → reporting chain with an invisible site-wide A/A and then finish one non-price product-detail CTA-color A/B test; script installation alone is explicitly not completion
  - confirmed scope is the Slovak `www.vevo.sk` storefront; all other languages, prices, product content, cart, checkout, payments, and stock remain out of scope
  - public/admin evidence confirms head-loaded Slovak GTM, GA4, cookie categories, and a Meta browser Pixel delivered through GTM; native BiznisWeb Meta Pixel/CAPI inputs are empty and GrowthBook is not currently present
  - a dedicated read-only VEVO admin check confirmed SK container `GTM-5ZB5LFGB`, empty native Facebook Pixel ID/Access Token inputs, an enabled Reject button, and visible Mandatory/Functional/Analytical/Marketing cookie categories; no Save/Confirm action occurred
  - public storefront source confirms the exact analytical-consent bitmask `FloxSettings.options.consent & FloxSettings.options.ANALYTIC` plus the existing `cookie_consent` data-layer event; BiznisWeb's public `flat.js` emits `add_to_cart` only after a successful cart result contains `items_added`, so the experiment bridge observes success without intercepting submit/cart behavior
  - the architecture keeps one source of truth: a separate PII-free first-party collector writes an experiment-only AWS dataset, GrowthBook Pro queries it read-only through Athena, and the existing VEVO reporting reads the same events
  - the existing Basic-Auth App Runner dashboard is not reused as a public collector; GrowthBook receives no access to order/customer/invoice exports and browser-submitted money is never authoritative
  - production allocation remains `0%` until consent classification/retention, a frozen 28-day baseline, exact `transactionId` → API `order_num` validation, collector/Athena security tests, Preview/rollback QA, and deployment hard-gate evidence all pass
  - the exact production GA4 baseline for `2026-07-23..2026-08-19` is frozen in `projects/vevo/GROWTHBOOK_BASELINE_2026-08-20.md`: `2,362` active users, `2,971` sessions, `58` purchases, `759` `view_item` users, and `261` `add_to_cart` users; homepage volume was only `267` active users
  - the latest complete seven days had `451` `view_item` users and `148` `add_to_cart` users; at the diagnostic `32.82%` rate, `25%` relative MDE, `80%` power, and two-sided `5%` alpha, the provisional target is `1,084` exposed devices, or about `16.8` days before consent/eligibility loss
  - metric definition `vevo_cm1_v1_2026-08-20` is frozen as net order revenue minus product expense, packaging cost, and net shipping cost; CM1 per exposed device is the primary business guardrail, while device-level `add_to_cart` within 24 hours is the A/B primary decision metric
  - A/A `vevo-sk-aa-001` is invisible and site-wide, and requires at least seven full days and 1,000 eligible devices plus SRM, reconciliation, deduplication, transaction-join, privacy, and performance gates
  - first A/B `vevo-sk-product-cta-color-001` changes only the eligible product-detail CTA background/color, runs for at least 14 days with a pre-registered final sample target, and ends as `WIN`, `LOSE`, or `INCONCLUSIVE`
  - a named production GA4 Exploration showed `58` unique transaction IDs, each counted exactly once; `57/58` (`98.28%`) resolve to the exact same BiznisWeb `order_num`, including `55` shipped, one expired-payment, and one waiting order, while one historical ID is unavailable through the current API/search and remains excluded from authoritative value
  - the exact join passes the planned `98%` gate and rules out same-ID duplication in the audited GA4 property; GA4 still covers only `55/165` (`33.33%`) of shipped-order aggregate, so consent coverage remains the leading but not yet proven explanation and GA4 totals are not authoritative shop revenue
  - PageSpeed's 28-day origin-level CrUX baseline passes Core Web Vitals: mobile p75 LCP `1.3 s`, INP `152 ms`, CLS `0`; desktop LCP `1.3 s`, INP `50 ms`, CLS `0`; a representative product Lighthouse run scored `64` mobile / `85` desktop with `13.3 s` / `1.4 s` lab LCP, so per-variation performance events and strict stop thresholds are required
  - the PII-free contract now includes bounded, detail-free `performance_vital` and `client_error_observed` events; it never stores error text, stacks, filenames, URLs, or rejected values
  - the isolated `growthbook_collector` core handler is versioned and locally verified: exact per-event field sets, origin/consent/UUID/time/registry checks, PII and raw-click-ID rejection, server receipt partitions, generic errors, optional KMS encryption, and atomic S3 `IfNoneMatch="*"` idempotency are enforced; only `412` is accepted as a proven duplicate, while `409` conflicts are retried twice and then fail closed
  - Preview registry entries exist for `vevo-sk-aa-001` and `vevo-sk-product-cta-color-001`; Production is deliberately `{}` and cannot accept an experiment until a separate reviewed activation commit
  - exposure-page and downstream health-page allowlists are separate, allowing anonymous checkout health after an earlier assignment without assigning a new checkout variation
  - the Lambda proposal was replaced before any AWS mutation by a review-only CloudFormation foundation for a dedicated non-root/read-only ECS/Fargate collector behind an internal ALB and API Gateway VPC Link; it also defines the private retained SSE-S3 bucket, immutable raw-write policy, partition-projected raw/device/performance Glue tables, isolated Athena workgroups, payload-free retained logs, and least-privilege reporting/GrowthBook policies
  - GrowthBook's managed policy can read only curated anonymous device/performance facts and its own Athena results; it cannot read the raw prefix, and no runtime policy has `s3:DeleteObject`
  - no IAM user/access key, committed GrowthBook client key, DNS record, GTM tag, BiznisWeb script, Meta change, or public collector route exists; the local AWS CLI has no active credentials
  - the Fargate infrastructure template passes `cfn-lint 1.55.1`, and CI repeats the pinned lint; proposed retention defaults are raw `180` days, curated `400` days, Athena results `30` days, and payload-free logs `30` days
  - protected deploy workflow `deploy-vevo-growthbook-preview.yml` is `main`-only and defaults to no public route; it resolves the approved VPC/subnets from `vevo-daily-report-email`, publishes an immutable dedicated image, validates a strict CloudFormation change-set allowlist, verifies the route-disabled service, then runs the exact task definition as a one-shot Fargate host gate that records task ID/private IP/service/`/app`, `curl localhost` health/marker, exact digest, exit code, logs, and target health
  - public activation is a separate optional change set that may add only `CollectorPostRoute`; rejected-origin/body, CORS, private-path isolation, and the complete raw-S3 object snapshot must remain unchanged before the endpoint is accepted
  - local host-adapter/collector/change-set/workspace tests pass (`34` focused tests), including five repeated oversized-body regressions; the full repository suite passes (`345` tests), as do the storefront Node suite (`9` tests), reporting QA smoke, security CI, attested `actionlint 1.7.12`, workflow YAML parse, `git diff --check`, and CloudFormation lint. Docker Desktop was not running, so no local container or process was created; the identical Docker localhost gate remains mandatory in PR CI and protected AWS deploy
  - PR `#282` initially triggered one Gitleaks `generic-api-key` false positive on the public metric identifier `vevo_cls_p75_milli_24h`; `.gitleaksignore` contains only its exact path/rule/line global fingerprint so all other paths, rules, and line positions remain scanned
  - follow-up commit `238769c1` narrowed the false-positive fingerprint and PR runs `32396904337` / `32396904357` are fully green: env, Gitleaks, observability, security, CloudFormation lint, tests, and the Linux Docker host gate all pass; the container emitted `COLLECTOR_LOCALHOST_HEALTH_OK:preview:git-ci-host-gate` and `COLLECTOR_LOCALHOST_MARKER_OK:/app:git-ci-host-gate`
  - PR `#282` merged to `main` as `cc925adf803537264d73b4ca93c9e4e48ef14fc3`; first protected route-disabled deploy `32397261208` confirmed AWS account `919341186960`, region `eu-central-1`, source cluster `vevo-reporting-cluster`, VPC `vpc-075f06decad37f610`, three source subnets/AZs, service `vevo-growthbook-collector-preview`, runtime `/app`, and immutable image digest `sha256:808410dad26f4fb72613963e295c9cca40872732c6b3dc2becdcb96eb5b7163e`
  - that first deploy created the dedicated immutable/scanning/encrypted ECR repository and image, then stopped before `execute-change-set` because AWS `describe-change-set` omitted `ChangeSetType`; no Fargate service/task, collector endpoint, Athena identity, GTM change, or public route was created. The CloudFormation placeholder remains route-less in `REVIEW_IN_PROGRESS`
  - the follow-up workflow now supplies the already-determined CREATE/UPDATE type explicitly, rejects an API mismatch, treats only `REVIEW_IN_PROGRESS`, `CREATE_COMPLETE`, or `UPDATE_COMPLETE` as deployable, and removes only exact stale `candidate-<run>-<attempt>` plans before retrying the same route-disabled stack
  - PR `#283` merged the placeholder fix to `main` as `27f759b374cad9db8f61ae502dcdfa01d639c09d`; protected route-disabled deploy `32397866060` then succeeded on exact image digest `sha256:e0a610787d8949068dd0b56b60854b73582b65abc1e674c0d76e05a580a7d062`
  - hard-gate identity from that run: instance-id `N/A (ECS/Fargate)`, host-gate task `425154769e104956ba59630d70d32df7`, private IP `172.31.16.51`, service `vevo-growthbook-collector-preview`, task definition `vevo-growthbook-collector-preview:1`, container `collector`, runtime `/app`, localhost health `http://127.0.0.1:8080/health`, and marker `http://127.0.0.1:8080/marker.json`; the exact health/marker strings were found in the task's CloudWatch stream and the container exited `0`
  - the continuously running service task `e0a367ff7b4449b28fa2c693ff704da6` uses the same digest and its internal ALB target is healthy; stack output and parameters prove `PublicRouteEnabled=false` and no `CollectorEndpoint` exists. GrowthBook Athena credentials/data source, GTM, Meta, BiznisWeb, and Production allocation remain unchanged
  - PR `#284` merged the explicit marker-log output as `92a315adbbfc4ed0f7bd175c9f3d3782f3146c4d`; activation run `32398885763` repeated the full host gate on task `c83ec368950242b1aee210ea014df40f`, private IP `172.31.6.205`, task definition `:2`, `/app`, and exact digest `sha256:9478acd98a8caf06374b018c563ee51fa896b9cc92148238579f04aa28a134e1`. Both localhost marker lines were printed, the container exited `0`, and service task `4a2cdbe240794f439b68ad674f9bb2d6` was healthy on the same digest
  - the activation validator accepted exactly one non-replacement Add for `CollectorPostRoute` and CloudFormation enabled `POST /v1/events`; the post-activation smoke then stopped only because it expected the internal reason `field_set_mismatch`, while the privacy boundary correctly returned the generic public body `{"accepted":false,"code":"invalid_event"}`
  - direct read-only public verification confirms OPTIONS `204` with exact `https://www.vevo.sk` origin and `OPTIONS,POST`, valid-origin `{}` returns `400 invalid_event`, attacker origin returns `403 origin_not_allowed`, and public `/health` plus `/marker.json` both return `404`. No SDK/GTM/Meta/BiznisWeb or Production activation exists, so no real storefront traffic is sent to the endpoint
  - PR `#285` merged the generic-public-rejection fix and dedicated read-only active verifier as `bcd9d77c918a7ce1681922070b09c3ae51246423`; workflow `32400301619` passed against service task `4a2cdbe240794f439b68ad674f9bb2d6`, task definition `:2`, and immutable digest `sha256:9478acd98a8caf06374b018c563ee51fa896b9cc92148238579f04aa28a134e1`
  - that verifier proved the stack is `UPDATE_COMPLETE`, the service is exactly `1/1` and healthy, `POST /v1/events` is the only explicit API route, CORS/origin/private-path rejection still matches the contract, and the complete raw-S3 object snapshot remained byte-identical before/after all invalid probes
  - PR `#286` merged the encrypted one-time Athena-reader handoff workflow as `4b2081d5f88bb035315733630fa2804348e9faa8`; first provisioning run `32401314322` stopped before IAM creation because ECS inherits `/app` from the image and does not expose a task-definition `workingDirectory`. The failed-run cleanup found no created marker, so no IAM user or access key existed to revoke
  - PR `#287` merged the direct reader-provisioning Fargate marker gate as `ad778bd2be20fc5ece2ab70498b3bc453e91da64`; successful run `32401658468` used one-shot task `f071aeb82f5a459691bf9acaca825d19`, private IP `172.31.3.87`, service `vevo-growthbook-collector-preview`, `/app`, and digest `sha256:9478acd98a8caf06374b018c563ee51fa896b9cc92148238579f04aa28a134e1`, with both exact localhost health/marker lines
  - that run created exact IAM user `vevo-growthbook-preview-reader` at `/vevo/growthbook/preview/`, attached only `arn:aws:iam::919341186960:policy/vevo-growthbook-readonly-preview`, verified no inline policy/group and exactly one active key, then produced only a CMS-encrypted one-day handoff. The credential was decrypted locally without printing it and GitHub artifact `vevo-growthbook-preview-reader-32401658468` was deleted immediately
  - GrowthBook Cloud data source `VEVO Preview Experiment Facts` (`ds_19g6mmt2c4dmn`) is now `Connected` to the dedicated read-only Preview Athena workgroup/database and scoped only to project `VEVO SK Web`; GrowthBook's connection test passed
  - the data source now has exactly one anonymous identifier (`device_id`) and one version-controlled assignment query (`VEVO consented devices`); the query executed through Athena without SQL/IAM errors and returned no rows, as expected before the first synthetic Preview event
  - the temporary local credential handoff, including plaintext credentials and the private decryption key, was deleted immediately after the successful GrowthBook connection; credential material was never committed or printed. Production, GTM, Meta, and both BiznisWeb admin tabs remain unchanged
  - a main-only, explicit-confirmation `Verify VEVO GrowthBook Preview Facts` workflow is prepared for the next gate: it resolves an immutable reporting image, runs an isolated Fargate `/app` localhost health/marker task, attaches only the stack's bounded reporting policy to the exact VEVO task role, accepts one deterministic anonymous synthetic A/A exposure, publishes it through the real reporting reconciler, and checks raw S3, curated S3, reporting summary, and the GrowthBook Athena workgroup before succeeding
  - PR `#291` merged the protected workflow as `e55ccd14b47c660b9b39a5788a1e65a63a98fc1a`; ECR build `32441438519` passed and published immutable reporting digest `sha256:30a23fcd69eb2d7a41195bffa0bc055d38bc2dd706e9eb07d5126675a21a6add`
  - first synthetic run `32441597178` passed the Fargate localhost hard gate on task `58105b0212ef489694a8ffaa1a7409ca`, private IP `172.31.0.224`, service `vevo-daily-report-email`, task definition `vevo-reporting-daily:32`, and `/app`; it attached only `vevo-growthbook-reporting-preview` to `BiznisWebReportingTaskRole-vevo`
  - the collector accepted exactly one anonymous synthetic A/A exposure (`event_id=b9630ac2-6bd0-4697-9067-531cb4bae7d6`, `device_id=9f8c0ed4-f23e-43d8-993e-de6bd28f0b5b`), and reconciliation task `f827b250c14a4d339bac745f1aa3f4f3` on private IP `172.31.7.155` published one curated device fact with zero order/cart value plus two quality reports
  - the run failed only in its final GitHub-runner readback because that step imported `reporting_core`, whose package initialization requires `python-dotenv` not installed on the runner; raw acceptance, host markers, IAM attachment, reconciliation summary, and curated publication had already passed. No second event/reconciliation is allowed until an idempotent recovery readback reuses run `32441597178`
  - PR `#292` merged the dependency-free, fail-closed recovery readback as `247dcac055ad8344e5991664ffbda24cfb48aaec`; recovery run `32442114254` reused the exact raw event/device from run `32441597178`, refused a new event, detected the existing curated fact, and skipped reconciliation
  - the recovery hard gate passed on isolated Fargate task `a3c8a939032a46c4aa220653121cfa1a`, private IP `172.31.0.106`, service `vevo-daily-report-email`, candidate task definition `vevo-reporting-daily:34`, runtime `/app`, and immutable digest `sha256:30a23fcd69eb2d7a41195bffa0bc055d38bc2dd706e9eb07d5126675a21a6add`; production scheduler allocation remained unchanged at `0%`
  - exact S3/Athena readback passed for the single synthetic device fact, and GrowthBook workgroup query `934c938d-bc55-42c7-b89c-247337e9e2b1` returned exactly the expected eight-column assignment row; the same assignment query was re-run in the GrowthBook UI without error and its data-source timestamp advanced to `2026-08-21 05:07` local time
  - Next exact step: create and query-test the version-controlled `VEVO Device Outcomes v1` and `VEVO Performance Vitals v1` fact tables in GrowthBook Preview, then create the pre-registered metrics; Production, GTM, Meta, and BiznisWeb remain unchanged
  - GrowthBook fact table `VEVO Device Outcomes v1` (`ftb_19g6mmt2dhrdi`) is now project-scoped to `VEVO SK Web`, automatically mapped to `device_id`, and query-tested through Athena with the exact 21-column anonymous synthetic outcome row
  - the version-controlled `VEVO Performance Vitals v1` query executed without SQL/IAM errors but returned no rows; GrowthBook did not persist the empty table. No unplanned event, direct S3 write, SDK/GTM activation, or production change was used to bypass that boundary
  - branch `codex/vevo-growthbook-fact-tables` adds an explicit main-only performance mode to the existing protected workflow: it requires the exact recovered exposure run/date, derives deterministic event/page-load UUIDs, publishes or safely reuses one `lcp_ms=1300` event, reconciles through the existing reporting runtime, and verifies the raw and curated identities plus exact Athena row after the same Fargate localhost/marker hard gate
  - Next exact step: merge that workflow through PR, dispatch it with source run `32441597178` and event date `2026-08-21`, then create/query-test `VEVO Performance Vitals v1` and proceed to the pre-registered metrics
  - PR `#294` merged the protected performance mode as `aff0119c5212fc36a2ef66010ecafa5f1f799457`; run `32443149425` succeeded on immutable reporting digest `sha256:2a52b4f95ba821cf5f10ddd89a3f731255ad2362241567a16bcc717ee47213e1`
  - performance hard-gate identity: instance-id `N/A (ECS/Fargate)`, service `vevo-daily-report-email`, runtime `/app`, host task `a1fea9950a634753a0670bee148a4a2c` at `172.31.39.121`, source task definition `vevo-reporting-daily:33`, and isolated candidate `:35`; reconciliation task `f7b7b2b84776470296cdca21c82c29c1` ran at `172.31.47.32`
  - the collector accepted deterministic performance event `071b28e4-6177-48ca-86d4-47936cd15a3c` for existing device `9f8c0ed4-f23e-43d8-993e-de6bd28f0b5b` and page load `a2240ea5-a6f0-416d-a1c2-515b609f8e2c`; reporting published exactly one eligible `lcp_ms=1300` performance fact, and GrowthBook workgroup query `c41e0f2c-690d-4fd5-8081-8868fe8c6876` returned exactly that row
  - GrowthBook fact table `VEVO Performance Vitals v1` (`ftb_19g6mmt2e0otd`) is now project-scoped to `VEVO SK Web`, mapped to `device_id`, and query-tested with the exact seven-column synthetic performance row; both version-controlled Preview fact tables are verified
  - Next exact step: create the eleven metrics exactly from `projects/vevo/growthbook_workspace.json`, query-check them against the synthetic rows, and keep Production allocation at `0%`
  - generic reporting reconciliation in `reporting_core/experiments.py` now deterministically creates PII-free device/performance facts from validated raw events plus an exact seven-field authoritative-order boundary
  - `reporting_core/experiment_io.py` now provides a bounded fail-closed reader that enumerates only exact server-receipt `event_date=YYYY-MM-DD` partitions, enforces pagination/object/byte limits, and rejects malformed, nested, escaped, or non-object JSON before reconciliation
  - `reporting_core/experiment_orders.py` now reuses the active BiznisWeb reporting exporter's realized-revenue, item-cost, packaging, and shipping logic, but immediately emits only the seven allowlisted order fields; server-received order-completion time drives the frozen attribution/maturity clock while BiznisWeb remains authoritative for order existence, lifecycle, and money
  - `scripts/reconcile_growthbook_facts.py` connects raw S3 events to the existing project runtime and curated fact builder; it is dry-run by default, reads only a narrow receipt-date order window, and requires both `--publish` and `GROWTHBOOK_FACT_PUBLISH_ENABLED=true` before any curated write
  - a version-controlled Preview-only storefront client and reproducible GTM Custom HTML builder now implement the exact consent gate, anonymous sticky assignment, strict Meta URL-ID allowlists, exposure/cart/order/health events, withdrawal cleanup, fail-closed SDK/selector behavior, and the frozen `brand_contrast` CTA class; Production activation is hard-coded `false`
  - the manual GrowthBook `1.7.0` and official Google `web-vitals 6.0.1` bundles are exact-version/SRI pinned and load only after consent; Visual Editor, JavaScript injection, URL redirects, automatic GA4/GTM exposure tracking, credentials/referrers, and arbitrary collector hosts/ports are blocked
  - the exact GTM bridges are versioned for BiznisWeb `cookie_consent`, successful `add_to_cart`, and existing `purchase` events; none has been pasted, previewed, or published in GTM
  - the exact GrowthBook Pro object contract is now versioned in `projects/vevo/growthbook_workspace.json`: organization/project/environment intent, anonymous `device_id`, separate Preview/Production Athena targets, one assignment query, two curated fact tables, eleven metrics, both experiment/feature keys, exact variation order/weights, and Production allocation `0%`
  - paste-ready Athena SQL under `projects/vevo/growthbook_sql/` uses official GrowthBook `startDateISO`/`endDateISO` and `experimentId` templates, reads only curated facts, excludes contaminated assignments, exposes only approved Meta dimensions, and never selects a transaction, order, customer, raw click ID, or other PII-bearing field
  - outcome metrics deliberately use GrowthBook window `None` because their 24-hour cart and seven-day purchase/value windows are enforced once in authoritative reporting; event-level LCP/INP/CLS p75 metrics use a 24-hour conversion window, and cancellation/refund ratios remain blocked by the 14-day maturity gate
  - the authenticated GrowthBook organization is `Vevo`; the default project was reused and renamed `VEVO SK Web` (`vevo-sk-web`) because the current Starter plan permits only one project, and the UI did not expose a workspace region, so the manifest keeps it explicitly unknown
  - GrowthBook `staging` is the Preview alias because a custom `Preview` environment is paid-only in the observed UI; staging defaults new features OFF and production remains excluded/disabled
  - staging-only SDK connection `VEVO SK Web Preview` now exists for JavaScript SDK `1.7.0`, includes draft experiment rules and feature rule IDs, and disables Visual Editor/URL Redirect support; its public client key is not committed and the connection is `Not connected` until Preview installation
  - string flags `vevo-sk-aa-assignment` and `vevo-sk-product-cta-color` exist with default `control`, no live rule, staging enabled, and production disabled
  - A/A `vevo-sk-aa-001` and CTA A/B `vevo-sk-product-cta-color-001` exist as unstarted 100%-traffic/50-50 staging-only drafts with exact string values; no Start/Publish action occurred and Production has no rule
  - the storefront now prefers GrowthBook `ExperimentResult.value` for the exact string variation contract because `ExperimentResult.key` defaults to numeric tracking metadata (`0`/`1`); numeric keys and unapproved values fail closed
  - `scripts/validate_growthbook_workspace.py`, focused tests, and CI reject raw/PII SQL, metric/variation/window drift, a changed A/B primary or CM1 business guardrail, a published/running experiment, any Production rule/SDK connection/allocation, or a committed client-key claim
  - non-realized order facts conservatively keep explicit pending/cancelled/refunded lifecycle counts with zero value; final A/B CM1 evaluation remains blocked until the 14-day credit-note/refund-cost reconciliation is proven in Preview, rather than inventing value in the browser or adapter
  - reporting freezes a 24-hour cart window, seven-day purchase-attribution window, 14-day cancellation/refund maturity checkpoint, and 50/50 expected allocations in `projects/vevo/growthbook_reporting.json`
  - the builder preserves non-buyers with zero value, uses first exposure, detects cross-variation contamination, deduplicates identical event IDs, fails closed on conflicting IDs/orders, prevents one transaction from being double-attributed across devices, calculates independent SRM/join/performance QA, and publishes only curated SSE-S3 objects
  - verification passed: focused collector suite `20` tests, combined GrowthBook builder/reporting/pipeline suite `20` tests, GrowthBook workspace contract suite `7` tests, storefront Node suite `9` tests, full repository suite `345` tests, Python/JavaScript syntax checks, registry/config JSON parse, reporting QA smoke, security CI, attested workflow lint, current Fargate CloudFormation lint, and `git diff --check`
  - exact rollout and rollback gates are in `projects/vevo/GROWTHBOOK_PLAN.md`; the strict versioned PII-free schema is in `projects/vevo/GROWTHBOOK_DATA_CONTRACT.md`
  - GrowthBook draft/UI mutations above occurred; no BiznisWeb, GTM, Meta, AWS, reporting-runtime, or storefront-runtime mutation occurred, and no local server/process was started, so no process cleanup was required
  - the current GrowthBook plan read-back is Starter; a paid Pro upgrade modal was reviewed and cancelled, so no charge, subscription upgrade, or auto-renewal was accepted
  - the dedicated AWS key/secret is stored only in GrowthBook Cloud; the connection, assignment query, and both fact tables are verified, and the temporary local credential/private-key handoff is deleted
  - eight Starter-compatible outcome metrics are now created with exact IDs recorded in `projects/vevo/growthbook_workspace.json`; a first real metric analysis correctly exposed `integer = varchar(1)` on a binary filter, all numeric outcome/quality columns were corrected from GrowthBook String to Number, and all eight metric analyses then passed through Preview Athena against one anonymous synthetic device
  - three performance p75 Quantile metrics remain intentionally uncreated because the authenticated plan is Starter and no paid Pro charge was authorized
  - the Preview GTM builder now accepts the public SDK client key and the host-verified collector endpoint only through task-scoped environment overrides, validates both, and no longer requires a local config copy containing runtime values; focused builder/workspace validation passes
  - isolated unpublished GTM workspace `VEVO GrowthBook Preview` (`16`) now contains exactly four new Preview tags and one new custom-event trigger: loader `44`, consent bridge `46`, add-to-cart bridge `47`, purchase bridge `48`, and `add_to_cart` trigger `45`; GTM read-back shows `5` added, `0` modified, and `0` deleted
  - all three bridge tags were read back with the loader-before sequence and fail-closed behavior; the current runtime-populated artifact matches SHA-256 `f6b4972641efb7cc99d05b64b2c365c45eec20a6e5600ce9dade1dcaec694de1`, the same checksum is recorded in the GTM workspace description, it was assembled only in the browser REPL, and its exact task-generated clipboard copy was cleared after read-back
  - nothing was submitted or published in GTM; GrowthBook A/A feature revision `2` is live only in `staging`, A/A `vevo-sk-aa-001` is running at 100% experiment traffic with a 50/50 split, CTA A/B remains an unstarted draft with zero live rules, Production allocation remains `0%`, and both BiznisWeb admin tabs were untouched
  - A/A analysis now uses `VEVO Preview Experiment Facts`, `VEVO consented devices`, add-to-cart within 24h as the diagnostic goal, six frozen outcome diagnostics as secondary metrics, and client-error device rate as the available guardrail; default Bayesian statistics remain selected, while CUPED, post-stratification, and activation metric are off
  - after explicit user approval, Google Tag Assistant extension `26.216.2.45` was installed and its user-visible site access was confirmed as `all sites`; the user added exact VEVO and VEVO-FLOX exceptions, temporarily disabled Comet's global blocker, and fully restarted Comet. GTM Quick Preview then reconnected, found the same three Google tags, and evaluated Preview container `GTM-5ZB5LFGB` without console warnings/errors
  - the no-Analytical withdrawal gate is verified with no GrowthBook SDK, Web Vitals, exposure marker, collector activity, or CTA style. Regranting Analytical consent produces the safe Preview state `active` / `assigned`, numeric BiznisWeb consent values, bitwise consent `granted`, A/A variation `control`, and collector delivery `accepted`; the same variation and accepted delivery survive reload, while CTA still has zero live rules and no style is applied
  - the storefront disables GrowthBook feature caching only in `preview` (`maxAge=0`, `disableCache=true`) while preserving future Production settings, and now emits a Preview-only, PII-free state marker for consent/runtime diagnosis. The user was told to re-enable Comet's global blocker immediately after the final accepted request
  - current branch is `codex/vevo-growthbook-preview-qa`; GTM remains unpublished with `5` added, `0` modified, and `0` deleted, GrowthBook Production allocation remains `0%`, and neither BiznisWeb admin nor Meta Ads was mutated
  - protected run `32452676654` used main commit `8caffb77394833d79481bc92a285f23479256636`, immutable reporting digest `sha256:194d97bc159e59678cf184cdad3c33c0f5b2ddf501fa31d1d3422c6a7b5d2f68`, and passed the required Fargate hard gate on task `efcb528173bc41529faab4abe7cc4f19`, private IP `172.31.25.197`, service `vevo-daily-report-email`, and runtime `/app`
  - that run accepted its exact synthetic control event and the reconciler produced a publish summary for the full `2026-08-21` partition, but the workflow stopped before Athena identity read-back because the legacy synthetic-only assertion expected one raw event and observed `22`; this is evidence that controlled real Preview events reached raw S3, not a collector failure
  - the workflow now has an explicit `allow_existing_partition_events` mode. Its default remains false/exact-synthetic; the mixed mode is bounded to one explicit partition and at most `1,000` raw events, requires generated/published fact counts to agree, bounds device/performance/order counts, and still verifies the exact synthetic control identity without logging browser device IDs
  - PR `#305` merged the bounded mixed-partition verifier and Preview no-cache runtime as main commit `521472cac27b779f6bd1b969cadd1e4dfd8870fd`; all protected checks passed
  - protected run `32453223068` then succeeded against immutable reporting digest `sha256:194d97bc159e59678cf184cdad3c33c0f5b2ddf501fa31d1d3422c6a7b5d2f68`; the host gate used task `2f4894451b6b40b0a2e7210f8ec18a08`, private IP `172.31.8.58`, service `vevo-daily-report-email`, and runtime `/app`, while the reconciliation task used `f39633f1ab4e4a6ab2c01eb650e67b32` at `172.31.22.22` with the same service/path identity
  - the controlled `2026-08-21` partition reconciled `23` raw events into `5` device facts and `11` performance facts, with `0` transaction and `0` order facts; generated/published counts matched, exact synthetic identity passed without logging browser device IDs, and GrowthBook Athena assignment query `ef981af5-3c3f-4d32-813a-a546be77b79b` completed the raw/curated/reporting/Athena chain
  - branch `codex/vevo-growthbook-recurring-reconciliation` adds a no-argument scheduled runner with exact VEVO/Preview/region/publish gates and a fixed 40-complete-UTC-partition rolling window; the window covers the seven-day order attribution plus late 14-day cancellation/refund maturity while remaining below the existing 90-partition loader ceiling
  - the runner passes explicit `40`-partition and `50,000`-raw-event bounds to the reconciler, rejects user-selected dates, and emits a payload-free success/failure marker; `24` focused schedule/pipeline/reporting tests, Python compile, security CI, JSON parse, and `git diff --check` pass
  - a dedicated reconciliation stack is now versioned under `infra/vevo-growthbook-reconciliation/`: it contains an initially disabled `03:30 Europe/Bratislava` schedule, retained encrypted DLQ, exact-task scheduler role, explicit failure alarm, two-day missing-success alarm, and DLQ alarm; it grants no delete or BiznisWeb mutation permission
  - the protected main-only deploy workflow pins a dedicated `vevo-growthbook-reconcile-preview` task definition to one immutable VEVO reporting image, records source schedule/cluster/roles/network/container/log identity, runs the localhost task/IP/`/app` marker gate before CloudFormation, creates the schedule disabled, requires one real bounded publish with generated/published count parity, then permits only the schedule-enable change and exact read-back; the existing `vevo-daily-report-email` schedule must remain byte-identical
  - local verification passes `93` GrowthBook/schedule/change-set/pipeline/reporting tests, Python compile, security CI, workflow/template YAML parse, `git diff --check`, and temporary isolated `cfn-lint 1.55.1`; the temporary lint runtime was removed. No local server, worker, watcher, tunnel, or AWS runtime was started
  - PR `#307` merged the recurring runner/stack/workflow as main commit `800456870d3ba52af1ea64a521237f3fd34cfbb0`; ECR build `32455578257` passed and published exact digest `sha256:c5326a514f0f10eb6b58929fb70841c8bc40c6f3c5fb40e90f35dda52f0fe253`
  - first protected deploy run `32455812794` stopped inside its pre-AWS validation step because the isolated runner had not installed `python-dotenv`; AWS credentials were never configured, no task/task-definition/stack/schedule/role/queue/alarm was created or updated, and no experiment facts were written. The workflow now installs the versioned `requirements.txt` before its focused tests, matching the ECR/reporting runtime dependency gate
  - dependency-fix PR `#308` merged as `fa01945def62ffa5173bb6a59a02170ab0811b0a`; exact ECR build `32456113381` passed with digest `sha256:692c2bbac08ea6cec8553f675d4e38c782de844161a74fc2e00f0897ef3c268d`
  - second protected deploy `32456288626` passed validation and AWS authentication, registered only unused/non-running candidate task definition `vevo-growthbook-reconcile-preview:1`, then stopped in runtime resolution with `TASK_ROLE_ARN: unbound variable`; Fargate host/reconciliation tasks, CloudFormation stack, scheduler, queue, alarms, and experiment facts were not started or created
  - root cause is contained to one GitHub Actions step: validated values were appended to `GITHUB_ENV`, which intentionally makes them available only to later steps, before the same step consumed `TASK_ROLE_ARN` and `REPORTING_POLICY_ARN`. Branch `codex/vevo-growthbook-reconciliation-runtime-env` validates the exact generated key set and exports assignments without shell evaluation before their first same-step consumer; regression coverage fixes this ordering boundary
  - runtime-environment fix PR `#309` merged as `d561c19cc8a49d0f77ee5fa1ee5c8381381af025`; ECR build `32456802552` passed with exact digest `sha256:20975ed79b57525cb563e3db07bc690a63b063acc88b22a14aa737bfd1f72cef`
  - protected deploy `32456954675` passed code/AWS/runtime resolution and the required host gate on task `f16e2b84cb83407ab42c35aa26ea9620`, private IP `172.31.40.199`, service `vevo-growthbook-reconcile-preview`, runtime `/app`, candidate task definition `:2`, and the exact digest above; the isolated host task stopped successfully
  - the run then stopped before its first CloudFormation API request because AWS CLI shorthand coerced the comma-delimited `SubnetIds` string into a list and rejected it locally. No stack, schedule, queue, alarm, one-shot reconciliation, or experiment fact was created; the source reporting schedule and GrowthBook/GTM production boundaries remain unchanged
  - branch `codex/vevo-growthbook-reconciliation-parameter-json` replaces candidate and activation shorthand with exact JSON parameter documents, preserves subnet/security-group lists as CloudFormation strings, and regression-tests that activation changes only `ScheduleState`
  - parameter-JSON PR `#310` merged as `2aaca30d70dfad0231c362b0b0b87685d7cdea3b`; ECR build `32457523457` passed with exact digest `sha256:24d7899f81d4615c0cfa605469419186a6953e305ec8920234d08193491264af`
  - protected deploy `32457687990` passed its host gate on task `6af1d87c570a43a888011b9bcc4b835a`, private IP `172.31.3.123`, service `vevo-growthbook-reconcile-preview`, runtime `/app`, candidate task definition `:3`, and the exact digest above; the isolated host task stopped successfully
  - the exact eight-resource candidate change set passed the allowlist and executed, but stack creation reached `ROLLBACK_COMPLETE` before the disabled-schedule read-back. One-shot reconciliation and activation did not run; because the DLQ is intentionally retained, resource presence and the exact failure event must be read back before any recovery or deletion is considered
  - branch `codex/vevo-growthbook-reconciliation-failure-diagnostic` adds a fail-closed preflight for non-healthy existing stack states and a read-only, payload-free failure diagnostic for stack events/resources, target/source schedule state, retained queue, scheduler role, and alarms; it explicitly has no delete operation
  - diagnostic PR `#311` merged as `6e20a9ea8cea07f9b329d0821be0b15949898bfd`; read-only run `32458420571` failed fast before image resolution/task registration/host execution and confirmed the exact rollback boundary
  - exact root cause: `ReconciliationSchedule` creation failed because the scheduler execution-role trust policy scoped `aws:SourceArn` to the individual schedule ARN; EventBridge Scheduler requires the schedule-group ARN. Rollback deleted the target schedule, scheduler role, three alarms, and two metric filters; only the intentionally retained encrypted DLQ remains. The target schedule is absent and `vevo-daily-report-email` remains `ENABLED` on `vevo-reporting-daily:33`
  - branch `codex/vevo-growthbook-reconciliation-rollback-recovery` scopes the trust policy to exact group `schedule-group/default`, matching the AWS confused-deputy guidance, and adds a one-purpose protected recovery workflow. Cleanup is permitted only after exact account/stack/resource/source-schedule checks plus empty/SSE/tagged retained-DLQ verification; it can delete only the failed stack record and that exact empty queue, then must prove the source schedule unchanged
  - trust/recovery PR `#312` merged as `4e4443beea2a2da466d80f781199bd4684dfac0c`; bounded recovery run `32458911281` passed its exact control-plane hard gate, verified the retained SSE queue had zero visible/delayed/in-flight messages and expected ownership tags, removed only the failed stack record plus empty retained DLQ, and proved `vevo-daily-report-email` byte-identical before/after
  - ECR build `32458905990` passed for the exact merge with digest `sha256:cabba3b0bd57f6be322f3a5ff62f0327c7cf8e7bb2b6b5e78686305339fdd041`
  - protected deploy `32459100570` succeeded end-to-end. Predeploy resolved task definition `vevo-growthbook-reconcile-preview:4`; host gate task `29d5e5d3fed349d79dec1384f5aff32a` ran at `172.31.25.184` with service `vevo-growthbook-reconcile-preview`, runtime `/app`, localhost health/marker, and the exact digest above
  - the exact eight-resource change set created the stack with the schedule first read back `DISABLED`; bounded one-shot task `668418a08c504e078288f407df44a15e` ran at `172.31.18.86`, reconciled the latest `40` complete UTC partitions with `0` raw/device/performance facts and `2` quality reports (current-day Preview events are intentionally outside the complete-partition window), and passed generated/published parity plus its payload-free success marker
  - the enable-only change set and final read-back passed: schedule `vevo-growthbook-reconcile-preview` is `ENABLED` on task definition `:4`, `cron(30 3 * * ? *)`, timezone `Europe/Bratislava`, exact target override/retry/network boundary, retained encrypted DLQ, exact scheduler role, and all three alarms. The source reporting schedule remained unchanged; GrowthBook Production stays `0%` and GTM stays unpublished
  - branch `codex/vevo-growthbook-meta-population-audit` now contains pushed implementation commit `40a53987`: a main-only, explicit-confirmation audit workflow resolves the exact main-commit ECR digest, copies the known VEVO Fargate runtime into a dedicated non-scheduled task family, requires the exact task/private-IP/service/`/app` localhost marker gate, and makes no scheduler, GTM, GrowthBook, Meta-delivery, BiznisWeb, order, price, cart, or checkout mutation
  - the Meta audit uses Graph GET only over the latest `30` complete UTC days and outputs aggregate ad/campaign/ad-set counts plus click/spend parameter coverage; it never emits ad/campaign/ad-set IDs, names, destination URLs, click IDs, or customer identifiers. Its accepted placement values now exactly match the storefront allowlist, and all expected Graph failures collapse to a sanitized payload-free failure marker
  - aggregate Athena SQL compares assignment/outcome row and anonymous experiment-device-key counts, duplicate keys, both anti-joins, variation totals, approved Meta-dimension coverage, ID shapes, and placement values under the exact frozen `vevo_cm1_v1_2026-08-20`, `eligible = 1`, `contaminated = 0` contract. Query output contains only experiment/variation aggregate counts and query IDs
  - focused tests, Python compile, workflow YAML parse, central security CI, workspace validation, and `git diff --check` pass. The recurring schedule live state and the still-pending runtime audit are now represented in `projects/vevo/growthbook_workspace.json`; no local server, worker, watcher, tunnel, or other persistent process was started
  - PR `#314` merged the audit as main commit `d4ed6e276855c71cec91e0827b2619af020ad524`; ECR build `32461487005` passed and read back exact digest `sha256:8cbae67d93fd2181924abe31971a53c9ef3144ac5334c92e3da138f2623c699c`
  - first runtime `32461687307` passed exact digest/runtime/network resolution and the Fargate host gate on task `6912ec37bdce4aee9945739fa208298d`, private IP `172.31.43.218`, service `vevo-growthbook-meta-audit-preview`, path `/app`, candidate task definition `:1`. The read-only Meta task then stopped on an unobservable completion-marker/exit combination; Athena never started, and no Meta delivery, GTM, GrowthBook, BiznisWeb, order, price, cart, or checkout mutation occurred
  - branch `codex/vevo-growthbook-meta-audit-diagnostic` pins the Meta client's ephemeral cache to writable `/tmp`, adds an immediate payload-free start marker, extends bounded CloudWatch propagation polling, and exposes only task/IP/service/path, numeric exit, allowlisted ECS stop code, and marker/event counts when completion fails. This closes the diagnostic gap without printing raw CloudWatch messages, ad IDs, names, URLs, or tokens
  - diagnostic PR `#315` merged as `8f9b09d9c590e16b910a28bfd82bc782f711a7f8`; ECR build `32462559849` passed with exact digest `sha256:2c20f38b1206458529749313b3ee643307c34dc5b73d6e0763e561882ac5b4a2`
  - second runtime `32462783153` passed the host gate on task `0173cd46a9264bd78025bf12925cfb26`, private IP `172.31.17.16`, service `vevo-growthbook-meta-audit-preview`, path `/app`, candidate task definition `:2`. Meta task `767537f2aaba4b8992d1790f930d5882` at `172.31.9.105` stopped with `exit=1`, `EssentialContainerExited`; sanitized counts were `events=4`, `start=0`, `ok=0`, `fail=0`. Athena did not start and no external mutation occurred
  - the zero-start-marker boundary proves the command failed before `main()`. Root cause is the script-path invocation: `python scripts/audit_vevo_meta_dimensions.py` makes `/app/scripts` the import root, so sibling root module `facebook_ads.py` is unavailable. Branch `codex/vevo-growthbook-meta-audit-module` switches only to `python -m scripts.audit_vevo_meta_dimensions`; a no-credentials subprocess regression reaches the exact sanitized start/failure markers without a network call
  - PR `#316` merged the module-entrypoint fix as `9ef741b328cb5709f1e3e7e78c2f4b7afeadc066`; ECR build `32463854583` verified exact digest `sha256:95efe5fffa2f4a3c7ded6c710697b0d5f6f6b45fbc525ad6a848a069753234ef`
  - protected audit `32464046045` passed the Fargate hard gate on task `cd74ee3a3b5d43e999c45b15b0fdec1`, private IP `172.31.16.54`, service `vevo-growthbook-meta-audit-preview`, path `/app`, candidate task definition `:3`; read-only Meta task `3b1e00828825416da1f5c1422e6d1cac` at `172.31.19.87` stopped normally with exit `0`
  - Meta aggregate for `2026-07-22..2026-08-20`: `19` traffic ads / `3` campaigns / `3` ad sets / `2,210` clicks / `523.13 EUR` spend. `utm_source`, `utm_medium`, and `utm_id` cover `100%`; `utm_content`, `meta_adset_id`, `meta_placement`, and the complete six-field contract cover `0%`; forbidden click-identifier parameters occur on `0` ads
  - Athena population query `0acd8e02-d081-4cec-a85e-71034321f8de` and variation query `5ef71971-9ea2-4511-8d49-517a1c00318a` proved exact `5` assignment rows/keys = `5` outcome rows/keys, zero duplicates, zero missing-side keys, `3` complete Meta-dimension rows, and zero invalid dimension rows. No external mutation occurred
  - `projects/vevo/META_ADS_GROWTHBOOK_PARAMETER_RUNBOOK.md` freezes the exact six analyzed fields plus diagnostic campaign-name label, applies them only before first publish or within an independently planned ad edit, forbids bulk retroactive live-ad edits and a concurrent Meta split, and defines aggregate post-publish and rollback gates. The checked-in workspace validator enforces the template and safety boundary; existing live ads remain unchanged
  - branch `codex/vevo-growthbook-production-foundation` prepares a main-only, explicit-confirmation Production foundation preflight. It locally runs the collector in `production` mode and performs only AWS STS/CloudFormation/Scheduler/ECS read-back; it records the healthy Preview task/IP/service/path plus the planned route-disabled Production identity and rejects any existing public Production endpoint
  - central security CI and a dedicated workflow contract test forbid CloudFormation/ECS/IAM/Glue/Athena/S3 mutation commands in the preflight. The Production registry stays empty, deployment remains disallowed, credentials are not created, GTM stays unpublished, and allocation stays `0%`
  - PR `#319` merged the read-only preflight as `19adc326d676212df6b410ac96eadeea47655c21`; first run `32465911390` passed all `51` tests and then stopped before AWS credentials because the workflow expected `unpublished_draft` while the authoritative manifest value is `not_published`. No AWS read/write step, Production resource, route, credential, GTM publish, or allocation change occurred
  - PR `#320` merged the exact status fix as `edd6e24772819f9cd087d4e3fb7e01b2085e6c4b`; second run `32466261505` again passed all `51` tests and stopped before AWS credentials because the workflow read nonexistent `release_gates` instead of the authoritative `decision_gates` object. No external read/write step or mutation occurred
  - PR `#321` merged the exact gate-path fix as `f50e5712b039b991e2bc986552af8d8a54a6d551`; protected run `32466708456` passed the complete local contract and read-only AWS inspection. It verified account `919341186960`, region `eu-central-1`, healthy Preview task `4a2cdbe240794f439b68ad674f9bb2d6`, private IP `172.31.34.243`, service `vevo-growthbook-collector-preview`, path `/app`, and image digest `sha256:9478acd98a8caf06374b018c563ee51fa896b9cc92148238579f04aa28a134e1`
  - the Production stack is absent; the planned service remains route-disabled, registry-empty, unpublished, and at `0%` allocation. No Production stack, route, credential, GrowthBook data source, traffic, or external mutation was created
  - branch `codex/vevo-growthbook-production-preflight-result` records that successful preflight evidence without changing runtime state
  - branch `codex/vevo-growthbook-natural-reconcile-verifier` prepares a main-only read-only verifier for the first natural schedule window. A hard `2026-08-22T01:40:00Z` gate executes before AWS credentials; the verifier requires the exact enabled schedule/stack/task definition and digest, precisely one success stream and zero failures, ECS task/IP/service/`/app` identity with exit `0`, Scheduler-role CloudTrail `RunTask`, generated/published parity, empty encrypted DLQ, clear alarms, and the enabled source reporting schedule. It contains no task start, task registration, schedule/stack update, data write, GTM, Meta Ads, or BiznisWeb mutation operation
  - branch `codex/vevo-growthbook-production-foundation-deploy` prepares the next main-only Production foundation workflow but keeps `foundation_deployment_allowed=false`. It reuses the exact host-verified Preview image digest, first proves the exact source task/image in `production` mode through direct localhost markers while the stack remains absent, accepts only an absent-stack `CREATE` against the 31-resource allowlist, hard-codes `Environment=production` and `PublicRouteEnabled=false`, then repeats the Production task/IP/service/`/app` localhost gate and requires a healthy target, zero API routes, `404` external behavior, and an empty bucket. It has no update/delete, route activation, ECR push, reader credential, GrowthBook, GTM, Meta Ads, or BiznisWeb commerce mutation path
  - after exact VEVO/ROY exceptions were added and the user restarted Comet, read-only browser QA proved Tag Assistant genuinely connected to `vevo.sk`, detected `GTM-5ZB5LFGB` plus both GA tags, and showed the unpublished Preview GrowthBook loader firing without browser warnings/errors. A fresh post-restart session at `2026-08-21 14:13 Europe/Bratislava` showed three Google tags, one Preview-loader firing, and live `view_item_list` plus consent/DOM/window lifecycle events. GTM remains unpublished and Production remains disabled
  - branch `codex/vevo-growthbook-production-reader-clone` prepares a separate main-only Production reader workflow and machine-readable clone contract. The workflow remains stopped before AWS credentials until the natural verifier and route-disabled foundation deployment are both recorded as verified and `reader_provisioning_allowed=true`; current values stay false/null
  - the planned identity is `vevo-growthbook-production-reader` under `/vevo/growthbook/production/`, never the Preview reader. Before IAM creation, the workflow must prove the exact Production task/private IP/service/`/app`, immutable digest, direct localhost health/marker, zero API routes, empty bucket, and exact curated-only Athena policy. Its single key is exposed only as a one-day RSA-3072/CMS-encrypted artifact with failed-run revocation; GrowthBook/GTM/Meta/BiznisWeb are not mutated by this workflow
  - Production reader dispatch `32614216513` on exact main `9fff28bf3e2e764b9a822e61fd798f9657fa3056` stopped before the temporary ECS host gate and before IAM creation because the reader still used the superseded direct `KeyCount` bucket assertion. The failure cleanup proved that no Production IAM identity or access key had been created. Follow-up diagnostic `32614283239` did not reach AWS because its intentionally one-time pre-evidence local gate rejected the now-recorded foundation evidence; it is not evidence of bucket drift
  - branch `codex/vevo-growthbook-reader-runtime-gates` replaces the reader's bucket check with the shared privacy-safe four-count summarizer, requires zero incomplete multipart uploads, proves IAM `NoSuchEntity` with exact fail-closed exit/error semantics both before the host gate and immediately before creation, and reuses the task-definition-bound host-log resolver that accepts only an absent optional ECS duplicate or an exact noncontradictory value. Both failure cleanup paths can revoke an identity only when the current run's creation marker exists, so a raced/pre-existing user is never deleted. Production allocation remains `0%`, the API route remains disabled, and no GrowthBook, GTM, Meta Ads, or BiznisWeb mutation is introduced
  - reader-gate correction verification passes `19` focused reader/bucket/runtime tests, the full `586`-test Python suite, all `9` storefront JavaScript tests, workspace validation, central security CI, Ruff, Python compile, workflow YAML/JSON parsing, all `7` embedded Bash blocks, all `6` embedded Python blocks, and `git diff --check`; no local server, worker, watcher, tunnel, or persistent process was started
  - PR `#354` merged the reader-gate correction as `79f1eb4b1b29bb65efbdbe310b0033e1a5a1f594`; all four protected checks passed and no review thread remained. Reader run `32614706434` then proved bucket counts `0/0/0/0` with no keys/content exposed, exact IAM `NoSuchEntity` before the host gate and immediately before creation, live service task `acaf0e0a472b416d8a9df927f06d1943` at `172.31.42.150`, and direct localhost/marker task `2c33ccdc1a1148758fe3c5fac162fb11` at `172.31.28.57`, service `vevo-growthbook-collector-production`, path `/app`, task definition `:1`, immutable digest, and task-definition-bound reconstructed log stream
  - the successful run created exactly one dedicated `vevo-growthbook-production-reader` identity with one active key, the exact reviewed policy/tags, no inline policy/group/raw-prefix access, and two separate artifacts: encrypted one-day credentials artifact `9486585740` with downloaded CMS SHA-256 `2f8c0a2941ce5c345fc518b2e562cabcc50d141ace9de71b61bffab74a7cd59f`, plus sanitized 14-day evidence artifact `9486585888` with canonical SHA-256 `1715f2b41a1bfd1d58524bdbad8369afc63b76a30d145f959a9cc742370b01d7`. Credential material and the temporary RSA private key remain outside Git pending GrowthBook insertion; they must be destroyed immediately after the connection succeeds
  - branch `codex/vevo-growthbook-record-production-reader` records only the canonical reader evidence and opens the separately reviewed GrowthBook clone gate. Production allocation remains `0%`, GTM remains unpublished, the Production registry remains empty, and GrowthBook/Meta Ads/BiznisWeb have not been mutated
  - reader-record verification passes `40` focused reader/clone/workspace tests, the full `586`-test Python suite, all `9` storefront JavaScript tests, workspace validation, central security CI, Ruff, Python compile, workspace JSON parsing, and `git diff --check`. Historical recorder fixtures now explicitly reconstruct the pre-reader state instead of depending on the evolving source manifest; central security requires the exact successful run/commit/SHA-256 and secret-free reader evidence while keeping the clone reader-ready but unexecuted
  - the clone contract freezes Preview data source `ds_19g6mmt2c4dmn`, both fact-table IDs, all eight query-verified Starter-compatible metric IDs, null Production target IDs, explicit creation order, and `preview_connection_repoint_allowed=false`. The three p75 metrics remain blocked until a paid Pro upgrade is explicitly authorized
  - branch `codex/vevo-growthbook-natural-evidence` makes the first natural verifier emit only one 14-day sanitized schema-v1 JSON artifact after every AWS/runtime/monitoring check succeeds. It includes the GitHub run/main commit, task/IP/service/`/app`, immutable digest, aggregate reconciliation counts, DLQ/alarms/Scheduler proof, and explicit no-mutation flags; raw AWS responses, CloudWatch/CloudTrail messages, credentials, and customer/order data are never uploaded
  - the route-disabled Production foundation gate now also requires the reviewed manifest to contain that exact downloaded evidence, matching run ID/main commit and a recorded 64-hex file SHA-256. It revalidates the evidence window, runtime, counts, control-plane state, and no-mutation contract before AWS credentials, preventing a status-only manual promotion
  - branch `codex/vevo-growthbook-evidence-recorder` prepares the remaining offline evidence handoff before the time gate. The deterministic recorder accepts only canonical schema-v1 bytes, independently supplied GitHub run ID/main commit, the exact verification window/AWS/runtime/count/control/safety contract, a private VEVO VPC task IP, and a computed SHA-256; it has no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, or network client
  - the recorder changes only the reviewed natural-verification and route-disabled-foundation gate paths. Production allocation remains `0%`, credentials remain absent, and reader provisioning plus GrowthBook cloning remain false. The workspace validator now accepts the future verified state only when the embedded evidence and its SHA-256 fully validate; malformed, noncanonical, PII-extended, identity-mismatched, unbounded, replaced, or partially opened states fail closed
  - local verification passes `47` focused evidence/workspace/verifier/workflow/foundation tests and the full `439`-test repository suite, together with workspace validation, central security CI, Python compile, and `git diff --check`
  - branch `codex/vevo-growthbook-foundation-evidence` closes the next manual boundary: only after the CREATE-only Production foundation proves its localhost health/marker, exact task/IP/service/`/app` identities, healthy target, zero API routes, external `404`, empty bucket, no reader credentials, empty registry, unpublished GTM, and `0%` allocation does it upload one 14-day canonical schema-v1 artifact
  - the foundation artifact contains only workflow/natural-evidence provenance, bounded AWS/runtime identities and aggregate gate outcomes; raw AWS payloads, CloudWatch messages, credentials, and customer/order data are excluded. Its offline recorder validates independent run/commit identity, SHA-256, route-disabled mutation scope, both private VPC runtimes, and exact natural provenance, then closes foundation redeployment and opens only the separately reviewed Production-reader gate while clone/allocation/GTM remain disabled
  - transition-state review found and fixed a P1 pre-AWS blocker in the natural verifier, foundation deploy, and Production reader workflows: all three read `recurring_schedule` from the nonexistent `workspace` object instead of the authoritative `reconciliation_checkpoint`. Exact-path regressions and central security CI now forbid the stale path; without this correction tomorrow's verifier would have failed safely but incorrectly before AWS credentials
  - local verification passes `59` focused transition/evidence/workflow tests and the full `446`-test repository suite, workspace validation, central security CI, Python compile, workspace JSON and three workflow YAML parses, all four edited inline-Python compiles, `git diff --check`, and a direct execution of the natural local gate against the checked-in pending manifest (`NATURAL_RECONCILIATION_LOCAL_GATE_OK`)
  - branch `codex/vevo-growthbook-aa-evaluator` closes a downstream decision gap while the natural run is time-gated. The source of truth now freezes all A/A readiness, SRM/split, collector/Athena/reporting parity, duplicate, same-population, exact-order-join, Meta dimension, privacy, consent, checkout/purchase-duplication, rollback, desktop/mobile, and performance thresholds in `projects/vevo/growthbook_aa_acceptance.json`
  - the offline evaluator independently recomputes full Europe/Bratislava calendar days, SRM, variation shares, count differences, duplicate/join rates, and p75/error deltas from an exact aggregate schema. It returns only `PASS`, `FAIL`, or `NOT_READY`, always emits `winner_calls_allowed=false`, rejects extra/PII-shaped fields, inconsistent identities and non-finite values, and has no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, or network client
  - the evaluator cannot start or stop traffic. A protected read-only Production snapshot workflow remains mandatory after Production A/A begins; a local snapshot can test the contract but can never authorize GTM publish, allocation, or CTA A/B
  - local verification passes `26` focused evaluator/workspace tests and the full `452`-test repository suite, workspace validation, central security CI, Ruff checks on all touched Python files, Python compile, and `git diff --check`
  - branch `codex/vevo-growthbook-receipt-evidence` closes the accepted-duplicate measurement gap before a Production A/A: S3 idempotency deliberately suppresses a second raw object, so stored-row counts alone cannot prove the frozen duplicate-request threshold
  - the source collector now emits one schema-v1 `VEVO_GROWTHBOOK_COLLECTOR_RECEIPT` only after successful persistence, with the exact fields `accepted=true` and boolean `duplicate`. The marker contains no event/device/order identity, URL, campaign dimensions, or payload, and logging failure cannot turn a persisted storefront event into a failed request; the deployed Preview image is unchanged and no Production collector exists yet
  - `scripts/summarize_growthbook_receipts.py` is an offline fail-closed reducer for a temporary bounded CloudWatch export. It validates unique log-event identities, the exact time window and marker schema, then writes only received, unique-accepted, and duplicate aggregate counts with explicit raw/identity exclusion flags; raw logs are never committed or uploaded
  - local verification passes `26` focused collector/receipt tests and the full `458`-test repository suite, workspace validation, central security CI, Ruff checks on all touched Python files, Python compile, and `git diff --check`
  - branch `codex/vevo-growthbook-aa-snapshot` prepares the protected Production A/A evidence assembly boundary. `projects/vevo/growthbook_aa_snapshot.json` keeps `snapshot_build_allowed=false`; automated and manual-QA run IDs, main commits, and SHA-256 values remain null and unrecorded
  - `scripts/assemble_growthbook_aa_snapshot.py` accepts only two canonical aggregate components bound to independently supplied successful-run provenance and hashes, requires identical frozen windows and exact privacy/runtime schemas, validates the finished snapshot through the existing offline evaluator, and strips component provenance/runtime identities from the output
  - `.github/workflows/build-vevo-growthbook-production-aa-snapshot.yml` is main-only and has no AWS credentials or API path. It can read only two exact GitHub artifacts after the Production foundation, reader, GrowthBook clone, 100% A/A allocation, and both evidence records are verified; it uploads only the sanitized snapshot and decision and can never authorize a winner or CTA activation
  - local verification passes `22` focused snapshot/evaluator/receipt tests and the full `469`-test repository suite, workspace validation, central security CI, Python compile, Ruff, snapshot JSON/workflow YAML parsing, and `git diff --check`
  - branch `codex/vevo-growthbook-aa-evidence-producers` prepares the first required snapshot component producer: manual GrowthBook/Tag Assistant/commerce browser QA. The producer gate is false, observation status is `not_recorded`, observation SHA-256 is null, and the canonical observation file does not exist before real Production QA
  - `scripts/build_growthbook_aa_manual_qa_evidence.py` is offline, requires canonical observation bytes plus an independently recorded SHA-256, injects only the current successful workflow run/main commit, enforces 100% Production A/A-only allocation and exact no-identity/no-unplanned-mutation flags, and emits the exact manual schema expected by the snapshot assembler
  - `.github/workflows/verify-vevo-growthbook-production-aa-manual-qa.yml` is main-only, stopped before evidence creation until foundation/reader/clone/A/A/browser gates are proven, keeps CTA unstarted, has no AWS credentials, browser automation, network, or external mutation path, and uploads only one 14-day sanitized manual evidence artifact
  - local verification passes `20` focused manual-QA/snapshot tests and the full `478`-test repository suite, workspace validation, central security CI, Python compile, Ruff, manual-QA JSON/workflow YAML parsing, and `git diff --check`
  - branch `codex/vevo-growthbook-aa-automated-evidence` prepares the second required snapshot component producer but keeps `producer_allowed=false`, both UTC window bounds null, the reporting-quality status/key/SHA-256 unrecorded, and the final snapshot gate closed
  - `scripts/build_growthbook_aa_automated_evidence.py` is an offline canonical-schema/hash/provenance boundary. It accepts only the sanitized exact aggregate observation, injects the successful main workflow run/commit, enforces the private Production Fargate runtime, receipt-count identity, reporting/Meta/privacy/consent schemas, read-only source, and no raw/identity/customer/mutation flags, and has no AWS, GrowthBook, GTM, Meta, BiznisWeb, browser, or network client
  - `.github/workflows/collect-vevo-growthbook-production-aa-evidence.yml` stops before AWS credentials until the reviewed window and exact reporting-quality S3 key/hash are frozen and foundation/reader/clone/100% A/A-only gates pass. It later verifies the current task against the previously localhost-gated immutable foundation image, exact Glue schemas, bounded receipt logs, one hash-bound reporting-quality file, one aggregate-only Athena audit, and conservative consent rejects; all raw AWS/CloudWatch responses and query files are temporary and deleted before exactly one 14-day sanitized artifact upload
  - transition review corrected the manual QA workflow's future experiment lookup from the nonexistent `id` field to the authoritative `tracking_key`; because its producer gate remains false and no observation exists, the defect never reached a workflow run or Production
  - local verification passes `31` focused automated/manual/snapshot tests and the full `489`-test repository suite, workspace validation, central security CI, Ruff, Python compile, inline-workflow Python compile, JSON/YAML parsing, and `git diff --check`
  - branch `codex/vevo-growthbook-production-clone-handoff` prepares the missing audited bridge from a future successful Production reader run to the separately reviewed GrowthBook clone gate. The reader workflow now emits a canonical sanitized evidence artifact in addition to its distinct one-day CMS-encrypted credential handoff; no credential material is copied into the evidence or Git
  - `scripts/record_growthbook_production_reader_evidence.py` is offline and hash-binds the exact reader workflow run/main commit to the already recorded foundation run/main commit/SHA-256. It accepts only the reviewed account/region/stack, private task/IP/service/`/app`/task-definition/image, curated-only Athena/IAM shape, and explicit no-secret/no-customer-data/no-mutation flags
  - the recorder can change only the exact Production reader evidence fields, close reader provisioning, and mark the clone contract ready for a later reviewed action. It leaves `growthbook_clone.mutation_status=not_started`, target IDs null, Production allocation `0%`, GTM unpublished, and every Production/Meta/BiznisWeb mutation disabled. Current evidence fields remain pending/null and no workflow was dispatched
  - local verification now passes `31` focused reader-evidence/reader-workflow/workspace tests and the full `495`-test repository suite, workspace validation, central security CI, Ruff, Python compile, inline-workflow Python compile, workspace JSON/workflow YAML parsing, and `git diff --check`; no local server, worker, watcher, tunnel, or persistent process was started
  - branch `codex/vevo-growthbook-production-clone-evidence` prepares the missing audited post-reader GrowthBook UI handoff. `GROWTHBOOK_PRODUCTION_CLONE_RUNBOOK.md` freezes the exact separate Production Athena connection, `device_id` assignment query, two fact tables, eight Starter-compatible metrics, reload/read-back procedure, zero-row-before-traffic requirement, unchanged Preview proof, and partial-failure stop boundary
  - `scripts/record_growthbook_production_clone_evidence.py` is an offline canonical observation/recorder with no GrowthBook, AWS, browser, GTM, Meta, BiznisWeb, or network client. It derives assignment/fact SQL and metric-contract hashes from Git, rejects reused Preview IDs, non-unique/malformed target IDs, nonempty Production queries, unverified read-back, any paid p75 metric, credential/customer/order/query-result content, nonzero allocation, or published GTM
  - only a future reader-verified, hash-bound manifest can record the observation. The exact allowed change set is limited to clone status, observation/hash, new target IDs, and the next reviewed gate; it closes `clone_allowed`, records `mutation_status=created_and_query_verified`, and still leaves Production A/A unstarted. The current observation is absent, every target ID is null, and no GrowthBook object or credential was touched
  - local verification passes `38` focused clone/reader/workspace tests and the full `502`-test repository suite, workspace validation, central security CI, Ruff, Python compile, workspace JSON parsing, and `git diff --check`; no local runtime was started
  - pre-time-gate audit found that the natural verifier workflow expected the stale cluster suffix `/vevo-reporting`, while successful protected deploy run `32459100570` proves exact runtime `arn:aws:ecs:eu-central-1:919341186960:cluster/vevo-reporting-cluster`, container `reporting`, log group `/ecs/vevo-reporting-daily`, and prefix `ecs`. The workflow and offline verifier now pin those identities, the exact task ARN cluster, and the `172.31.0.0/16` private-IP boundary; regression tests reject a mutually consistent but wrong stack/schedule cluster, wrong log boundary, and out-of-VPC IP
  - the natural-gate correction passes `44` focused verifier/workflow/evidence/workspace tests and the full `505`-test repository suite, workspace validation, central security CI, Ruff, Python compile, workflow YAML parsing, and `git diff --check`; no AWS/GrowthBook/GTM/Meta/BiznisWeb mutation or local runtime was started
  - follow-on foundation/reader audit confirmed the CREATE-only route-disabled foundation gates, then found the Production reader workflow bundled the encrypted credential and sanitized evidence files into one one-day artifact despite the intended distinct handoffs. The workflow now uploads a credential-only CMS artifact for one day and a separate credential-free evidence artifact for 14 days, and confirms provisioning only after both uploads succeed; failed-upload cleanup still revokes the created key/user
  - the reader handoff correction passes `38` focused reader/foundation/workspace tests and the full `505`-test repository suite, workspace validation, central security CI, Ruff, Python compile, workflow YAML parsing, and `git diff --check`; no workflow, IAM, AWS runtime, GrowthBook, GTM, Meta Ads, BiznisWeb mutation, or local process was started
  - PR `#338` merged the previously missing fail-closed bridge from a future verified Production clone to the A/A collector rollout and later browser activation as `b15c71470239dfc3a347b305308032ff802cd3e2` (`2026-08-21`). `growthbook_production_aa_activation.json` is the redundant cross-system source of truth and remains `prepared_hard_disabled_clone_gate_pending`: all four evidence preconditions are false, the Production registry is empty, collector deployment is forbidden, GrowthBook is unstarted, GTM is unpublished, traffic allocation is `0%`, CTA is stopped, and every evidence/runtime ID remains null
  - `GROWTHBOOK_PRODUCTION_AA_ACTIVATION_RUNBOOK.md` freezes the exact five-phase order: reviewed registry preparation; route-disabled immutable collector image rollout; Fargate task/IP/service/`/app` localhost health and marker gate; single-route activation and no-write public isolation; zero-allocation GrowthBook/GTM preparation and Tag Assistant QA; then GTM publish at `0%` before the A/A-only `100%`/`50:50` start. Rollback must set GrowthBook to `0%`, restore the previous GTM version, and only then remove `CollectorPostRoute`; it never deletes retained evidence/data or mutates Meta Ads/BiznisWeb commerce
  - `.github/workflows/deploy-vevo-growthbook-production-aa-collector.yml` is main-only and currently cannot pass its pre-AWS local gate. A later reviewed clone-ready transition must supply the exact commit, all four evidence gates, the exact single A/A Production registry, and `collector.deployment_allowed=true`. Only then can it verify the recorded route-disabled foundation identity, build an immutable image, update only the task definition/service with the route still disabled, require direct Production localhost markers and healthy service target, add only `CollectorPostRoute`, prove CORS/private-path/origin/no-write isolation, and upload sanitized 14-day evidence. Any failure after route activation runs a separately validated route-only removal; GrowthBook, GTM, Meta Ads, BiznisWeb, cart, checkout, prices, and orders have no mutation client in the workflow
  - the activation handoff validator cross-checks the manifest, workspace, registry, storefront compile-time `PRODUCTION_ACTIVATION=false`, runbook, workflow gate order, and mutation exclusions. CloudFormation validation now permits `Remove` only in the dedicated `deactivate` phase and only for non-replacement removal of `CollectorPostRoute`; all other destructive changes remain rejected
  - `record_growthbook_production_aa_collector_evidence.py` closes the post-workflow handoff: it accepts only canonical, independently hash-verified evidence bound to the exact successful run/main commit, exact account/region/stack, immutable image/task definition, distinct private host/service tasks, direct localhost markers, healthy target, single A/A registry, active public route with byte-identical raw snapshot after invalid probes, and explicit zero GrowthBook/GTM/CTA/traffic plus no-secret/no-identity/no-commerce mutations. It can change only the collector evidence fields, re-close collector deployment, and open the zero-allocation UI-preparation gate; it has no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, browser, or network client
  - local activation verification passes `519` full repository tests plus the focused activation/collector-evidence/change-set/workspace suite, activation/workspace validators, central security CI, Ruff, Python compile, JSON/YAML parsing, all embedded workflow Python compilation, Bash syntax, and `git diff --check`; all four PR checks (`env-check`, `secret-scan`, `observability-baseline`, and `security-baseline`) passed before merge. No workflow was dispatched and no local server, worker, watcher, tunnel, Docker container, or persistent process was started
  - branch `codex/vevo-growthbook-cta-sample-freeze` adds the machine-readable first-CTA sample plan and an offline fail-closed freezer. The frozen two-sample proportions method exactly reproduces the current diagnostic target `542` devices/arm (`1,084` total) from `148/451`, but a final target can be recorded only from a canonical independently SHA-256-bound PII-free product-page aggregate and the exact canonical A/A snapshot. The freezer re-runs the versioned A/A evaluator and requires `PASS` instead of trusting a manually typed verdict; the product-page cohort is explicitly a conservative planning proxy while the eventual CTA evaluator keeps exact rendered-CTA exposure as its denominator
  - the freezer can update only the sample plan and matching CTA workspace fields; it requires the experiment/rule to remain draft, Production allocation `0%`, prices disabled, and the Production activation gate closed. It never declares a winner or changes GrowthBook, GTM, Meta Ads, BiznisWeb, AWS, traffic, cart, checkout, prices, or orders
  - sample-freeze verification passes the full `527`-test repository suite, workspace and activation validators, central security CI, Ruff, Python compile, JSON parsing, and `git diff --check`; no local server, worker, watcher, tunnel, or persistent runtime was started
  - branch `codex/vevo-growthbook-meta-complete-gate` closes a Meta acceptance gap: Production A/A now stays `NOT_READY` until at least one accepted Meta exposure contains the complete stable campaign, ad-set, ad, and placement dimensions. A source/medium/campaign-only exposure no longer satisfies the Meta gate, while invalid dimensions or a forbidden click identifier remain a hard `FAIL`
  - the current protected Meta baseline still has zero complete-contract ads, so this gate must be satisfied only by a new traffic ad or a separately authorized planned edit using `META_ADS_GROWTHBOOK_PARAMETER_RUNBOOK.md`; bulk-editing an existing live ad only for tracking remains forbidden
  - the stricter Meta gate passes the full `528`-test repository suite, workspace and activation validators, central security CI, Ruff, Python compile, JSON parsing, and `git diff --check`
  - a read-only Comet post-restart check on `2026-08-21` confirmed the Tag Assistant extension is installed, `vevo.sk` remains an active debug domain, and the existing public tags `GTM-5ZB5LFGB`, `G-L1VHFFWECR`, and `G-G9WJJGNQD8` are detected; no GTM publish or site mutation occurred
  - branch `codex/vevo-growthbook-cta-accessibility` adds `growthbook_cta_design.json` as the machine-readable visual boundary for the first CTA A/B and a validator that fails CI if the source changes anything beyond `background-color`, `background-image`, and `color`, mutates the button label/behavior, or drifts from the exact selector/class/colors
  - WCAG 2.2 AA contrast is independently computed at both gradient stops: dark text `#0f172a` is `7.9359:1` against `#c9a962` and `6.4325:1` against `#b8956f`, both above the frozen `4.5:1` normal-text minimum; label, dimensions, layout, placement, product selector, price, cart behavior, and checkout behavior stay unchanged
  - CTA design verification passes the full `532`-test Python repository suite, all `9` storefront JavaScript tests, the focused `24`-test design/workspace suite, workspace validation, central security CI, Ruff, Python/Node syntax checks, JSON parsing, and `git diff --check`
  - branch `codex/vevo-growthbook-cta-decision` completes the offline, fail-closed first-CTA decision contract. `growthbook_cta_decision_contract.json` freezes the exact first-`N` cohort, single fixed-horizon two-sided primary look, 14/42-day assignment boundary, 14-day post-stop follow-up, CM1 non-inferiority, mature cancellation/refund coverage, performance/client-error limits, data-quality gates, and zero automatic mutation. `scripts/evaluate_growthbook_cta.py` independently validates provenance and recomputes the statistics, then returns only `WIN`, `LOSE`, `INCONCLUSIVE`, or `NOT_READY`; only a fully safe significant positive result recommends `brand_contrast`, while every other result recommends control
  - `growthbook_cta_lifecycle_reconciliation.json` remains pending, false, and activation-disabled; the required `growthbook_cta_lifecycle_observation.json` does not exist. Its offline recorder requires canonical no-identity bytes, an independently supplied SHA-256, exact cent-precision CM1 parity, at least one mature cancellation/refund/credit-note case, matching lifecycle counts, and a hash-bound reporting-quality object. A verified manifest is invalid unless the checked-in observation exists, is canonical, and matches its SHA-256; an experiment snapshot cannot self-assert this gate
  - final review closed three fail-closed edges before PR: missing lifecycle evidence at the fixed look remains `NOT_READY`, explicit price/cart/checkout harm can stop early even before the quality sample has its first exact joined transaction, and the future workspace validator reads and verifies the canonical observation bytes instead of trusting manifest booleans. Lifecycle monetary evidence now rejects sub-cent inputs rather than rounding them into apparent parity
  - CTA decision verification passes the full `552`-test Python repository suite, all `9` storefront JavaScript tests, the focused `48`-test CTA decision/lifecycle/sample/workspace suite, workspace validation, central security CI, Ruff, Python compile, JSON parsing, and `git diff --check`
  - protected read-only natural verifier run `32561021689` on exact main commit `c9a7c163e51c9112c594dcb3efdc3651bf68dcc1` passed its complete local/time gate and configured read-only AWS credentials, then failed closed before evidence creation because `DescribeTasks` returned a failure for the first scheduled task. The scheduled success marker exists, but the verifier did not claim success and uploaded no artifact; Production foundation, reader, GrowthBook, GTM, Meta Ads, and BiznisWeb remained unchanged
  - the failure occurred approximately 6.5 hours after the scheduled task stopped. AWS documents stopped-task `DescribeTasks` availability for at least one hour, so branch `codex/vevo-growthbook-natural-expiry-diagnostic` adds a sanitized exact-shape summarizer that can distinguish the known task's `MISSING` retention result from any other task, reason, ARN, or ambiguous response. It prints neither the AWS payload nor failure detail and explicitly emits `evidence=false`; the original verifier remains unchanged and fail-closed
  - expiry-diagnostic verification passes the full `556`-test Python repository suite, the focused `41`-test diagnostic/verifier/workflow/workspace suite, workspace validation, central security CI, Ruff, Python compile, workflow YAML parsing, and `git diff --check`
  - diagnostic rerun `32561439023` on exact main commit `fac82e6c61cafc4d07e020736c26e8a230729812` again passed the local/time/AWS-read gates and exposed only `missing task ARN drift`; no artifact was uploaded. Branch `codex/vevo-growthbook-natural-expiry-shape` keeps the exact account, region, and task ID fixed while recognizing only the three equivalent ECS failure identifiers that AWS can return for the same requested task: bare task ID, legacy task ARN, or cluster task ARN. The emitted marker names only the matched shape and still declares `raw=false:evidence=false`
  - bounded identifier-shape verification passes the full `557`-test Python repository suite, the focused `42`-test diagnostic/verifier/workflow/workspace suite, workspace validation, central security CI, Ruff, Python compile, and `git diff --check`
  - diagnostic verifier run `32561674107` on exact main commit `3b41760b431bb2581dc1fb9fe940f346f7535a5b` conclusively emitted only `NATURAL_ECS_TASK_READBACK_EXPIRED:reason=MISSING:identifier=legacy_task_arn:task=21a941e0a446410bb4b46742fce33d16:raw=false:evidence=false`, then the original verifier failed closed on the missing ECS read-back. No evidence artifact was uploaded and no external state changed
  - branch `codex/vevo-growthbook-second-natural-verifier` prepares schema-v2 retention recovery against the next genuine Scheduler run due `2026-08-23 03:30 Europe/Bratislava`. Its hard gate permits read-only verification only during `03:40–04:20` local (`01:40–02:20Z`) so the stopped ECS task remains inspectable. It preserves exact account/region/cluster/task/private-IP/service/`/app`/task-definition/image/exit-`0`, Scheduler CloudTrail, one-success-stream, `2026-07-14..2026-08-22` 40-partition parity, DLQ, alarm, source-schedule, privacy, canonical-artifact, and no-mutation gates. It cannot dispatch early, start or register a task, relabel the one-shot, or create evidence from the expired first run
  - retention-recovery preparation passes `75` focused natural/foundation/reader/clone/workspace tests and the full `557`-test Python repository suite, workspace validation, central security CI, Ruff, Python compile, workflow YAML parsing, workspace JSON parsing, and `git diff --check`
  - branch `codex/vevo-growthbook-cloud-verifier-schedule` replaces the insufficient local-PC heartbeat as the execution dependency. The verifier now has a GitHub-hosted `2026-08-23 01:40Z` cron trigger on default-branch `main`, while manual dispatch still requires `confirm_verification=true`; the existing exact `01:40–02:20Z` local gate remains before AWS credentials. The yearless cron must be removed in the successful evidence handoff PR, and any later occurrence remains fail-closed before AWS because the verifier accepts only the exact 2026 window
  - cloud-schedule verification passes `38` focused verifier/workflow/workspace tests and the full `558`-test Python repository suite, workspace validation, central security CI, Ruff, Python compile, workflow YAML parsing, and `git diff --check`; no workflow was dispatched and no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, commerce, or local runtime state changed
  - manual cloud fallback run `32611064118` on exact `main` commit `aa7d8e8065cae7efebf72c4f8a19d454a2863717` ran inside the `2026-08-23 03:40–04:20 Europe/Bratislava` evidence window. It passed the local contract/time gate and AWS identity, found the second natural ECS task `e62569dcfb7a44d38f386e25ca34b272` still available, then failed closed on `natural task log stream drift`; artifact upload was skipped and the run has zero artifacts
  - branch `codex/vevo-growthbook-optional-ecs-logstream` treats only an absent ECS container `logStreamName` as the documented optional duplicate it is. The CloudWatch success stream must still have the task-definition prefix and exact task ID matching the exact ECS task ARN; a nonempty contradictory ECS stream still fails. Exact task definition/image/exit/private-IP/CloudTrail/log/count/alarm/DLQ/source-schedule and all no-mutation gates remain unchanged
  - optional-log-stream verification passes `40` focused verifier/workflow/workspace tests and the full `560`-test Python repository suite, workspace validation, central security CI, Ruff, Python compile, and `git diff --check`
  - manual verifier run `32611298457` on exact `main` commit `be2d13f509c2438b1d569003417356c5b1c0ce75` completed successfully inside the exact retention window. It proved Fargate task `e62569dcfb7a44d38f386e25ca34b272`, private IP `172.31.13.165`, service `vevo-growthbook-reconcile-preview`, path `/app`, task definition `:4`, exact immutable digest and exit `0`; CloudWatch task binding, Scheduler CloudTrail execution, source schedule, alarms, DLQ, and generated/published parity all passed
  - the sanitized result covers `2026-07-14..2026-08-22`: `40` partitions, `28` raw events, `5` device facts, `15` performance facts, and `2` quality reports. Artifact ID `9485599216`, name `vevo-growthbook-natural-reconciliation-evidence-32611298457`, was independently downloaded and verified as SHA-256 `69e5ab22a61a464abf3a9c7c354099f7c1d037a1da58b1fba41fb12ac50222b1`; it contains no raw AWS payloads, credentials, customer, or order data
  - branch `codex/vevo-growthbook-record-natural-evidence` records the canonical artifact through the offline fail-closed recorder, opens only the route-disabled Production foundation gate, keeps Production allocation `0%`, reader/clone disabled, and removes the completed one-time GitHub cron while retaining an explicitly confirmed manual verifier path
  - evidence-handoff verification passes `78` focused natural/foundation/reader/clone/workspace tests and the full `560`-test Python repository suite, workspace validation, central security CI, changed-file Ruff, Python compile, workflow YAML/workspace JSON parsing, recorder idempotency, and `git diff --check`. A broad repository Ruff scan still reports the same `15` pre-existing `E402` import-placement findings in unrelated runtime scripts; none of those files was changed in this handoff
  - PR `#349` merged the evidence handoff as `d3e831bf96fa48a0225ea02fc1211b9ecaa93e1b`. Foundation dispatch `32611782561` passed the exact local/natural/AWS/Preview/Production-absence gates, then failed closed before parameter build or CloudFormation CREATE because the stopped pre-deploy Fargate task omitted the optional ECS container `logStreamName`; no artifact was uploaded and the Production stack remains absent
  - branch `codex/vevo-growthbook-foundation-logstream` adds one shared offline resolver for both foundation host gates. It reconstructs only `collector/collector/<exact-task-id>` from the exact task-definition awslogs group/region/prefix, exact cluster/task ARN, container, digest, exit `0`, and `172.31.0.0/16` IP. Only an absent ECS duplicate is accepted; any nonempty contradictory stream, identity, log boundary, digest, status, task count, or IP fails before CREATE
  - foundation-log-stream verification passes `40` focused resolver/foundation/evidence/workspace tests and the full `567`-test Python repository suite, workspace validation, central security CI, changed-file Ruff, Python compile, workflow YAML/workspace JSON parsing, and `git diff --check`
  - PR `#350` merged the resolver as `82d1f04c85f43d007f03090eefbeb0feb09fc140`. Foundation run `32612205628` passed the pre-CREATE Fargate hard gate on task `23516a043fb5403483db0503128ec21a`, IP `172.31.34.40`, service `vevo-growthbook-collector-production`, path `/app`, then completed the reviewed route-disabled CREATE. The deployed Production host gate passed on task `9cb1d826dd244eff9fb612cb76bfac03`, IP `172.31.19.203`, exact task definition `:1`, immutable digest, localhost health, and marker
  - the final read-only gate then found the new Production experiment bucket nonempty and failed closed before evidence upload. Route remains absent, the service target was healthy, Production allocation remains `0%`, and no reader credentials, GrowthBook clone, GTM publish, Meta delivery, or BiznisWeb commerce mutation occurred. The stack is intentionally left in `CREATE_COMPLETE` for read-only diagnosis; it is not blindly recreated or deleted
  - branch `codex/vevo-growthbook-foundation-bucket-diagnostic` adds a main-only read-only diagnostic that rechecks exact stack/service/task/IP/target/route, bucket encryption/public-access policy, and reports only object class counts (`raw events`, `Athena results`, `unexpected`) through a truncation/count-parity-checked offline summarizer. It uploads no artifact, exposes no keys/content, and has no AWS or external mutation path
  - bucket-diagnostic verification passes `36` focused diagnostic/foundation/workspace tests and the full `575`-test Python repository suite, workspace validation, central security CI, changed-file Ruff, Python compile, workflow YAML parsing, and `git diff --check`
  - PR `#351` merged the read-only diagnostic as `ea73094968b583cccd0333e41dfc0c1945d995e5`. Runs `32612900470` and `32612957004` independently returned the exact same sanitized bucket summary: total `0`, raw events `0`, Athena results `0`, unexpected `0`, with keys/content unexposed. Both also proved the live route-disabled Fargate service on task `acaf0e0a472b416d8a9df927f06d1943`, private IP `172.31.42.150`, service `vevo-growthbook-collector-production`, path `/app`, healthy target, nonpublic encrypted bucket, zero API routes, external `404`, and no mutations
  - branch `codex/vevo-growthbook-foundation-evidence-recovery` prepares a one-time main-only schema-v2 evidence recovery instead of rerunning or misattributing the completed CREATE. It binds GitHub run `32612205628` and main `82d1f04c85f43d007f03090eefbeb0feb09fc140` through Actions metadata: the exact CREATE, immutable-runtime, and post-CREATE localhost steps succeeded, the final state gate failed, and artifact upload was skipped. It then requires the exact 31 live `CREATE_COMPLETE` resources with no `CollectorPostRoute`, exact stack parameters/image/service/task definition/network, and one separate stopped Fargate task that directly proves localhost health and `/app` marker before route/bucket/UI checks
  - the recovery permits only that temporary `ecs run-task` host gate. It cannot create/update/delete CloudFormation, write/delete S3, create IAM credentials, run Athena, mutate GrowthBook/GTM/Meta/BiznisWeb, or alter prices/cart/checkout/orders. Reader absence is accepted only on exact IAM `NoSuchEntity`; permissions or other lookup failures stop the run. The only upload is a 14-day canonical credential-free schema-v2 artifact that preserves original CREATE provenance and separately records the recovery workflow run/main commit
  - evidence-recovery verification passes `35` focused recovery/evidence/workflow/workspace tests and the full `584`-test Python repository suite, all `9` storefront JavaScript tests, workspace validation, central security CI, changed-file Ruff, Python compile, workflow YAML plus all `7` embedded Bash and `9` embedded Python blocks, and `git diff --check`; no local runtime was started
  - PR `#352` merged the recovery as `4aefe8b43a3c17756ab127303e2c59bce71be546`. Exact main-only run `32613802790` succeeded on its first attempt: it bound all 31 original CREATE resources and historical step conclusions, then passed direct localhost health and `/app` markers on stopped Fargate task `2ec01f9200724e1997dbf3e7c989724e`, IP `172.31.12.218`, service `vevo-growthbook-collector-production`, immutable digest, and task definition `:1`. The live service task remained `acaf0e0a472b416d8a9df927f06d1943` at `172.31.42.150`, healthy, with zero routes and external `404`
  - the same run proved bucket counts `0` raw / `0` Athena / `0` unexpected, no incomplete multipart uploads, exact nonpublic AES256 boundary, and IAM reader absence through `NoSuchEntity`. Artifact ID `9486318599`, name `vevo-growthbook-production-foundation-evidence-32613802790`, was independently downloaded as canonical schema v2 and verified at SHA-256 `bfc6ea2f0033d6ff8ce07cf3131ce114e9e6ed4f64afb86141ad12d44e259828`; the temporary local artifact was deleted after its evidence was embedded in the workspace manifest
  - branch `codex/vevo-growthbook-record-foundation-recovery` records exact recovery run `32613802790` / main `4aefe8b43a3c17756ab127303e2c59bce71be546` and its hash through the offline recorder. The manifest closes foundation redeployment and opens only `reader_provisioning_allowed=true`; credentials remain absent, clone remains false, Production registry empty, allocation `0%`, route absent, and GTM unpublished. Transition-safe fixtures now explicitly reconstruct their historical pending state instead of depending on the live manifest remaining pre-foundation forever
  - foundation-record verification passes `55` focused transition/foundation/reader/clone/workspace tests and the full `584`-test Python repository suite, all `9` storefront JavaScript tests, workspace validation, central security CI, changed-test Ruff/Python compile, workspace JSON parsing, and `git diff --check`
  - the only AWS mutation after the reviewed route-disabled CREATE was the exact stopped recovery host-gate task above. No IAM user/access key, Production GrowthBook data source/object, GTM publish, Meta delivery, BiznisWeb order, price, cart, or checkout state was changed; no local server, worker, watcher, tunnel, or persistent process was started
  - Next exact step: complete full tests and reviewed PR merge for the hash-bound manifest record, then dispatch the separately gated Production reader workflow. Keep route absent, allocation `0%`, registry empty, clone disabled, and GTM unpublished while the reader is provisioned and its distinct credential/evidence artifacts are independently handled

- VEVO profit-first Meta spend and sample-customer decision system is implemented on branch `codex/vevo-meta-profit-scaling` (`2026-08-20`):
  - the reporting model now evaluates Meta scaling against nominal contribution profit, not a fixed ROAS target; it combines immediate company profit with mature 30/60/90/180-day contribution LTV, safety-buffered marginal CAC guardrails, and explicit scale/hold/reduce decisions
  - recent 7/14/28-day scale steps are compared with the immediately preceding equal windows and expose incremental spend, new customers, revenue, immediate profit, and LTV-adjusted profit at every maturity horizon
  - spend recommendations use EUR 10 daily-spend tiers with minimum sample and weekday coverage, median and win-rate diagnostics, and a smoothed conservative lower bound; small and one-day outliers cannot become the recommended tier
  - high-spend customer-quality cohorts expose mature contribution LTV, downstream contribution, repeat rate, and sample mix; the UI labels this as an observational paid-spend-day proxy rather than customer-level Meta attribution
  - product-family acquisition math now uses real first-order revenue/contribution and maturity-censored 60/90-day denominators, so recent customers are not incorrectly counted as failed repeat purchasers
  - the sample-product table separates the paid-acquisition action (`CUT_PAID`, `HOLD`, `ELIGIBLE_TEST`) from the shop action (`KEEP_ORGANIC`, `REVIEW_REMOVAL`) and reports direct contribution, mature 60/90/180-day contribution LTV, downstream 90-day value, 60-day full-size conversion, safe CAC, and current marginal CAC
  - VEVO settings now recognize the classic `Sada vzoriek` product names and define a 15% LTV safety buffer, 14-day minimum spend-tier sample, and 10% scale step
  - the modern marketing dashboard renders a dedicated VEVO `Meta spend: maximalizacia nominalneho zisku` section with the account decision, safe core corridor, recent scale-step audit, LTV curve, guardrails, robust spend tiers, future customer quality, product-level sample eligibility, and methodology limitations
  - local verification passed: Python compile, settings JSON parse, focused dashboard/calculation suite (`105` tests), full suite (`289` tests), reporting QA smoke, security CI, and `git diff --check`
  - initial PR CI correctly caught a Python 3.11 f-string parser incompatibility that Python 3.12 accepted; the badge rendering was moved into a helper, the Python 3.11 grammar parse plus Python 3.12 compile/focused/smoke checks pass, and the corrective real Python 3.11 CI rerun passed before merge
  - no local server, worker, watcher, tunnel, or persistent runtime was started; a synthetic local HTML preview was generated outside the repository, and Chrome correctly blocked opening a new automated `file://` tab
  - implementation PR `#269` merged as `52f1dd4c17088c6f06da7148a7a56bf6164669df`; immutable image build `32350940747` succeeded with digest `sha256:dc5edfbf6496e32a4a3da669bcadf8e0e98124769e2a827f549ab5db2c1cbc9b`
  - protected deploy `32351190171` resolved candidate task definition `vevo-reporting-daily:26`, Fargate task `dabaeb6a2f374b879eb462adf16ebd45`, private IP `172.31.43.114`, service `vevo-daily-report-email`, `/app` runtime, and the exact digest above, but stopped before localhost marker, immutable-generation, scheduler-promotion, or App Runner gates; the schedule therefore remained on task definition `:25`
  - the GitHub step log hit its large single-step output boundary before the CloudWatch failure tail, so the read-only repo-local `Diagnose Reporting ECS Task` workflow retrieves the exact stopped reason, container exit code, last 400 CloudWatch events, deployed aggregated profit snapshot, immutable generation, and scheduler target for an explicitly supplied VEVO/ROY task without starting, stopping, or promoting anything
  - diagnostic workflow PR `#271` merged as `05a461bfb7d8ab2206c66f83ac977676143c7ca0`; read-only run `32353397916` confirmed the failed task stopped normally with `EssentialContainerExited`, container exit code `1`, and no orphan runtime remained
  - root cause is exact: the generated 7-day subreport has no previous 7-day comparison window, so `recent_rows_df` is legitimately empty; the new account-summary lookup attempted to select its absent `window_days` column and raised `KeyError: 'window_days'`
  - fallback PR `#272` merged as `91032296b000cbe12cc6e87824ba321fa6a2fbd4`; the short-period path now treats the absent comparison as `EXPERIMENT` with an empty recent-window table, and a direct seven-day regression plus focused `106`-test suite, full `290`-test suite, compile, reporting QA smoke, security CI, workflow YAML parse, and `git diff --check` passed
  - immutable build `32353719802` succeeded with exact digest `sha256:2fce3b8629401e02456c061e217aac8fb9a2556b78b32acd19c959c69f9dc469`
  - protected deploy `32353935833` completed successfully: instance-id `N/A (ECS/Fargate)`, private IP `172.31.32.202`, service `vevo-daily-report-email`, task `d9099aca581444e0a778ea372b68db09`, task definition `vevo-reporting-daily:27`, runtime path `/app`, localhost marker `http://127.0.0.1:8000/marker.json`, and the exact digest above
  - direct post-host diagnostic `32356108836` confirmed the task stopped cleanly with exit code `0` and emitted `LOCALHOST_LIVE_DASHBOARD_OK:vevo:periods=7d,30d,90d,full` plus `LIVE_ARTIFACT_MARKER_OK`; immutable generation `20260820T094434Z` is live, and scheduler `vevo-daily-report-email` is `ENABLED` on task definition `:27`
  - the protected workflow's authenticated App Runner API and HTML gates passed; the local Chrome post-host visual attempt stopped at `ERR_NAME_NOT_RESOLVED` for the App Runner hostname, so server-side UI/data acceptance is verified but a local-browser visual pass is not claimed
  - exact live account verdict is `HOLD` at `53.98 EUR/day`; robust core corridor is `50-60 EUR/day`, tested ceiling is `70 EUR/day`, latest 7-day marginal CAC is `8.72 EUR`, safe 90-day CAC is `9.65 EUR`, and hard 180-day CAC ceiling is `13.07 EUR`
  - the latest 7-day scale step reduced immediate company profit by `15.11 EUR/day` but is approximately break-even after 90-day downstream LTV (`+0.39 EUR/day`); the 14-day comparison is positive immediately (`+11.48 EUR/day`) and after 90-day LTV (`+23.83 EUR/day`), so further spend growth requires a fresh positive 7-day and 14-day confirmation rather than a fixed ROAS target
  - high-spend-day customers are not better future customers in the observational cohort data: acquisition days above `84 EUR` Meta spend have 90-day contribution LTV `10.64 EUR` versus `12.18 EUR` in the mid-low bucket and 180-day LTV `12.17 EUR` versus `14.02 EUR`; this is a first-order-day paid-spend proxy, not click-level attribution
  - both classic Natural sample sets are `CUT_PAID` but `KEEP_ORGANIC`: 6x10ml has 90-day contribution LTV `6.97 EUR`, safe CAC `5.92 EUR`, 60-day full-size conversion `11.1%`, and `3,240` mature 90-day customers; 3x10ml has 90-day LTV `5.66 EUR`, safe CAC `4.81 EUR`, full-size conversion `4.7%`, and `113` mature 90-day customers, all below current marginal CAC `8.72 EUR`
  - profit-snapshot diagnostic PRs `#273` and `#274` merged as `7ee5d6e34e93a3c00af03f87009c74d9e7e2e923` and `1e1a02b342cba6fb3e36f15cd26d427dcce14cb6`; production snapshot run `32356794831` verified the exact live aggregate above without exposing order/customer rows
  - Next exact step: keep both Natural sample sets purchasable organically, exclude them from paid acquisition/creative destinations, and run a controlled 14-day holdout before considering any shop delisting; keep Meta at `50-60 EUR/day` and scale by at most `10%` only after both 7-day and 14-day nominal-profit gates pass

- ROY daily report / live-dashboard freshness incident is fixed and live (`2026-08-15`):
  - production incident run `31859338478` reproduced the common failure after a successful export through `2026-08-14`: SES rejected the `10,560,976` byte raw message because its hard limit is `10,485,760` bytes
  - because the protected Fargate workflow promotes the newly generated S3 artifacts only after the runner exits successfully, the same oversized email failure left both email delivery and the live dashboard stale
  - hard-gate identity: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.37.203`, service `roy-daily-report-email`, task `5ae77222ff0d4d0a88c001af839fe196`, task definition `roy-reporting-daily:64`, runtime path `/app`, localhost marker `http://127.0.0.1:8000/marker.json`, image digest `sha256:27f2017d29454a0ed297ac3da8cbc1aa0434ed526be4e773a97691b32a1f5f8b`
  - the email sender now preflights the exact serialized raw MIME size and transparently ZIP-compresses only the HTML attachment when the message exceeds a conservative `9 MiB` target; it fails locally before the SES API if even the compressed message exceeds the `10 MiB` hard limit
  - focused regression coverage verifies both the large-report ZIP fallback (including archive contents and final raw size) and unchanged direct HTML delivery for small reports
  - local verification passed: full `284`-test suite, reporting QA smoke, Python compile, and `git diff --check`
  - implementation PR `#267` merged as `1b4388c6779bc78bf0652bf029791e89082f23b7`; immutable build `31860902114` succeeded with digest `sha256:1204ecb4a111b3938e2fd277490039c1b1e6d56608fa7200a02e29b45138751e`
  - protected deploy `31861014269` succeeded: Fargate task `818f4498e5794d31a4d8cfd250dd2857`, private IP `172.31.45.234`, service `roy-daily-report-email`, candidate/promoted task definition `roy-reporting-daily:65`, exact digest above, localhost live-dashboard/artifact gates, immutable S3 generation gates, scheduler promotion, and App Runner release gates all passed
  - real production email smoke `31862536350` succeeded on task `b8fd9e80059149f48c6e3321fa4fb3e2`, private IP `172.31.35.210`, task definition `:65`, and the exact digest above; the oversized HTML MIME was automatically compressed to `1,405,987` bytes, SES returned MessageId `010701a0039d270d-f7b22b1a-bba0-4dcc-9e94-28b5626a3564-000000`, localhost marker passed, and the task exited `0`
  - post-host Chrome verification passed: selected ROY operations dashboard updated at `2026-08-15T04:10:36Z`, KPI source is `2026-08-15T03:28:52Z`, and cache status is `fresh` instead of `stale_revalidating`
  - no local server, worker, watcher, tunnel, or other temporary runtime was started
  - Next exact step: monitor the next natural `roy-daily-report-email` run on task definition `:65`; no further code or deploy action remains for this incident

- VEVO monthly Cohort LTV heatmap is deployed and live (`2026-08-13`):
  - reusable cohort analytics now calculate cumulative net revenue LTV per acquired customer for calendar months `M0..Mn`, together with cohort size, observed repeat rate, first-order LTV, and customer-weighted averages
  - customers whose known first purchase predates the visible report history are excluded from the matrix so rolling period slices cannot fabricate acquisition cohorts from returning customers
  - the modern customer dashboard now renders a responsive, horizontally scrollable heatmap with a sticky cohort column, `New`, `R-%`, a per-cohort trend sparkline, first-order LTV, cumulative `M0..Mn` values, explicit future-month blanks, and a customer-weighted average row
  - focused and full relevant verification passes: `96` dashboard/reporting-calculation tests, Python compile, payload null-preservation regression, and `git diff --check`
  - implementation merged through PR `#263` as `11a45bea4421b2d828aa7b53a074f4833bc00004`
  - initial protected generation `31713205621` stopped safely before localhost validation or promotion because BiznisWeb's `price_elements` resolver fails for order `2602007112`; read-only VEVO admin verification identified it as a shipped `21.15 EUR` net damage-compensation order for Slovak Parcel Service with no delivery/payment method by design
  - exact, audit-reasoned realized-revenue override merged through PR `#264` as `138a0d4e2adf9c82e1f7ec29a48a2c44a5c042b0`; it is status-bounded, wildcard-free, conflict-checked, and does not weaken the fail-closed metadata guard for any other order
  - override verification passes: `282` unit tests, reporting QA smoke, Python compile, both project settings JSON parses, and `git diff --check`
  - exact image build `31715345397` published tag `git-138a0d4e2adf9c82e1f7ec29a48a2c44a5c042b0` and digest `sha256:60a2c71290f4a902cb33efa361797d37ddd07e7599a5c3542cd6a66c0b153abf`
  - protected deploy `31715597027` succeeded on Fargate task `03dd0b29dc5f47d9a79d6ddde8191e55`, private IP `172.31.43.206`, service `vevo-daily-report-email`, candidate/promoted task definition `vevo-reporting-daily:25`, exact digest above, and localhost marker path `http://127.0.0.1:8000/marker.json`
  - immutable generation `20260813T154733Z` covers `2025-05-03..2026-08-12`; all `7d`, `30d`, `90d`, and full report artifacts passed the generation/marker gates, scheduler promotion, App Runner accounting checks, and production-board checks
  - full Cohort LTV artifact `cohort_ltv_20250503-20260812.csv` was generated for `5,440` customers and `1,088` repeat customers (`20.0%`); mature 90-day cohorts show `21.2%` second-order and `7.8%` third-order retention
  - automated Chrome visual inspection was attempted only after the host gates, but the local Comet client blocked the App Runner origin with `ERR_BLOCKED_BY_CLIENT`; server-side live UI/API validation still passed in the protected workflow and the user tab was restored without saving any admin changes
  - Next exact step: monitor the next scheduled `vevo-daily-report-email` run on task definition `:25`; use a browser without the local Comet App Runner block for manual visual spot-checking when needed

- ROY large bear-set product identity and missing-cost QA fix are merged, deployed, and live on `2026-08-11`:
  - identity PR `#260` merged as `05d8a1ee69218af1d89d6b3dfb9071e5e8562c3f`; QA-input PR `#261` merged as `dbcd65bfe054cedf16c5e8dc08d2f2be4b40a2bd`
  - `Velký set proti medvědům`, `Set against bears LARGE`, and `Set proti medvědům VEĽKÝ (miesto malého)` now use the existing `maco_stop_large_set` expansion together with `Set MACO STOP VEĽKÝ`; revenue, demand, and purchase cost are assigned to known-cost components `14832`, `12840`, and `F_482`
  - root cause of the initial post-deploy miss: product analyses used canonical `analytics_df`, but `_build_product_expense_coverage_qa` still received raw `df`; PR `#261` routes the same canonical frame into the purchase-cost QA and its regression requires zero fallback rows after component expansion
  - local verification passed: Python compile, focused identity/calculation/dashboard suite (`126` tests), full suite (`275` tests), reporting QA smoke, security CI, and `git diff --check`
  - exact merge-image build run `31461615820` published tag `git-dbcd65bfe054cedf16c5e8dc08d2f2be4b40a2bd` and digest `sha256:27f2017d29454a0ed297ac3da8cbc1aa0434ed526be4e773a97691b32a1f5f8b`
  - protected deploy `31461773028` succeeded for service `roy-daily-report-email` on Fargate task `4319a0992abe4c459ddf8c1904aaaa1b`, private IP `172.31.31.4`, candidate/promoted task definition `roy-reporting-daily:64`, exact digest above, and localhost marker path `http://127.0.0.1:8000/marker.json`; App Runner service `biznisweb-roy-operations-dashboard` completed its release gates
  - post-deploy Chrome verification loaded generation `2026-08-11 05:45:41`; all three parent aliases are absent globally and from the missing-cost table, while the canonical `Set MACO STOP VEĽKÝ` row contains `9` units
  - missing-cost coverage improved from `109` to `104` rows; recent 30-day fallback revenue fell from `€1,514.48` (`4.74%`) to `€1,267.20` (`3.96%`)
  - no local server, worker, watcher, or tunnel was started; two obsolete ROY API browser tabs were closed and the selected ROY report tab was retained for the user
  - Next exact step: add real purchase costs for the eight material fallback products (full-history net item revenue at least `€100`), starting with Holosun HS407C X2, XTAR AA 4150 mWh / 2500 mAh 4ks, and Box na Wachman Discovery

- ROY inbound inventory valuation is merged, deployed, and live on `2026-08-10`:
  - inventory valuation PR `#257` merged as `6079020cd25ea2df851c339c160c41effd956033`; deploy-gate fix PR `#258` merged as `c2b996a161c41a9f524b6a14b16c92496a0b4355`
  - the first full deploy `31410363837` successfully refreshed the production artifact but was blocked before promotion by a pre-existing IAM simulation parser bug; skip-refresh run `31413627782` reproduced that blocker after a successful localhost candidate marker
  - root cause: AWS returns one top-level result per simulated action and places the exact task/execution-role decisions under `ResourceSpecificResults`; the old gate incorrectly required two top-level results
  - the fixed gate now requires exactly one `iam:PassRole` action and the exact expected role ARN set, failing closed on missing, duplicate, extra, or non-allowed resource decisions; focused regression tests and the full `275`-test suite passed with reporting QA, security CI, workflow YAML parsing, the environment contract, and `git diff --check`
  - exact build run `31414400660` published merge tag `git-c2b996a161c41a9f524b6a14b16c92496a0b4355` and digest `sha256:161e1348815dd3c3d40cfc8d0eaa754bfb78ca87fba335af27ec6c5f82c8f6a4`, with `latest` verified on the same digest
  - protected full deploy `31414611250` succeeded: service `roy-daily-report-email`, Fargate task `8e8fee17605245bbb48e5f5dacfa7ef6`, private IP `172.31.33.78`, candidate task definition `roy-reporting-daily:62`, exact digest above, and localhost path `http://127.0.0.1:8000/marker.json`
  - host hard gates emitted `LOCALHOST_DASHBOARD_MAINTENANCE_OK`, `LOCALHOST_LIVE_DASHBOARD_OK`, and `LIVE_ARTIFACT_MARKER_OK`; the exact PassRole gate passed and scheduler `roy-daily-report-email` was promoted from task definition `:58` to `:62`
  - the new App Runner runtime passed active-maintenance and HTTP `423` write-block checks, authenticated ROY operations/API/PDF smoke, inactive-maintenance cleanup, reversible restock-state cleanup, and `APP_RUNNER_DEPLOY_OK`; public `/health` returned `ok=true`
  - automated visual reload in the selected Chrome/Comet tab was blocked locally with `ERR_BLOCKED_BY_CLIENT`; this was not used as a production failure signal because the host and authenticated live HTML/API release gates passed, but a manual browser reload is still useful for visual confirmation of the combined value
  - Next exact step: monitor the next natural `roy-daily-report-email` run on task definition `:62`; no code or deployment action remains for this change

- ROY inbound inventory valuation implementation details (`2026-08-10`):
  - branch: `codex/roy-inbound-inventory-value`
  - root cause: the operations dashboard used only the current on-hand purchase-cost total; manually recorded inbound units affected stock-risk coverage but contributed no value to the inventory KPI
  - the reporting inventory model now preserves a known mapped purchase cost and retail unit price even when the product currently has zero stock, while conflicting or missing unit values remain unpriced instead of being guessed
  - the live operations snapshot now exposes on-hand value, inbound value, and the combined owned-inventory value separately; inbound rows include unit cost, extended cost, retail value, and an explicit `costed` / `missing_cost` status
  - the ROY UI now shows `Hodnota skladu` and both detailed valuation cards as `skladom + na ceste`; each inbound row shows its purchase value, and products without a mapped purchase cost are visibly marked `chýba nákupná cena`
  - local verification passed: Python compile; focused ROY inventory/operations suite (`61` tests); live auth/mobile/modern dashboard suite (`21` tests); full suite (`273` tests); reporting QA smoke; security CI; generated ROY inline JavaScript syntax; and `git diff --check`
  - local `scripts/check_env.ps1` was not applicable in the isolated clean worktree because `.env` is intentionally absent and secrets were not copied
  - production deployment and scheduler promotion are complete as documented above

- ROY unpaid-order Stripe-expiry reconciliation is merged, deployed, and live (2026-08-10):
  - root cause: Stripe can overwrite an already fulfilled order back to `Stripe - expired` after an operator has matched an alternative bank transfer, issued the final invoice, and moved the order through the paid/sent states; the existing cancellation job inspected only the current status/payment and could later cancel that genuinely paid order
  - every prospective cancellation now loads the current order detail before planning and again immediately before mutation; any final invoice is a hard cancellation stop, so the job fails safe if invoice evidence appears between the scan and write
  - recovery is deliberately narrow: current status must be `Stripe - expired`, a final invoice must predate the current last-change timestamp, and payment resolution must be evidenced by either the invoice API `paid=true` flag or the configured bank-transfer payment identity; eligible orders are restored to `Odoslaná` (`status_id=4`) instead of cancelled
  - the cancellation scan now queries only configured candidate/recovery status IDs through BizniWeb's official status filter, avoiding the previous broad all-status scan; `Čaká na vybavenie` is an explicit ROY candidate so stale bank-transfer orders can be cancelled while COD remains excluded by the payment guard
  - CloudWatch and Fargate smoke summaries now include recovery candidates, successful/failed recoveries, pre-mutation rechecks, and the resolved recovery target status
  - live API work was read-only: order `2677002988` is currently `Odoslaná` after the operator's manual 2026-08-10 correction and is therefore outside the candidate set; its final invoice exists but BizniWeb exposes `paid=false`, which is why bank-transfer identity is an intentional second recovery proof; order `2677003434` is still `Čaká na vybavenie`, bank transfer, older than 14 days, and has no final invoice, so it is eligible for cancellation
  - the final read-only dry-run completed without API errors: `51` candidate-status rows scanned across `29` pages, `35` cancellation candidates, `0` recovery candidates because `2677002988` was already manually restored, and all `35` cancellation candidates had zero final invoices
  - implementation PR `#254` merged to `main` as `2d2f6a26c250cffa2c46b1467624881603f38f7b` after env, secret, security, and observability checks passed; ECR build run `31390416982` published digest `sha256:c2d8b752de9931e1910917d680922799de8c4fb321bd1da6f94db42ab2adcac0`
  - protected dry-run deploy `31390584855` succeeded on that exact digest with Fargate task `07ab5ade0d164f43b60db6d7a1e68a93`, private IP `172.31.8.67`, service `roy-unpaid-order-cancellation`, task definition `:31`, command `python unpaid_order_cancellation_runner.py --project roy --dry-run`, and marker path `http://127.0.0.1:8000/marker.json`; the task exited `0` with `UNPAID_CANCELLATION_MARKER_OK` and `DEPLOY_UNPAID_CANCELLATION_OK`
  - post-marker ROY UI verification confirmed that order `2677003434` remained `Čaká na vybavenie` with bank-transfer payment, proving the dry-run made no status write; no browser form was submitted
  - the simultaneous automatic push deploy `31390416979` correctly failed smoke because it resolved the previous mutable `latest` image before build `31390416982` completed; the successful manual rerun above superseded its earlier task definition and left the schedule on `:31`
  - exact-image hardening PR `#255` merged to `main` as `6428fc359bdcf6c5d00b0d04ef7fce68691f3a53`; it publishes immutable `git-${GITHUB_SHA}` ECR tags, requires the commit tag and `latest` to match at build completion, and makes the unpaid deploy wait up to ten minutes for that exact merge tag before any task-definition or schedule mutation
  - automatic ECR build `31391463423` emitted `ECR_EXACT_IMAGE_OK` for tag `git-6428fc359bdcf6c5d00b0d04ef7fce68691f3a53` and digest `sha256:64f13e0ef9ed138ad951244d87862b6339355402798812c9b1887a76343552b5`; the concurrent deploy visibly waited at 60 and 120 seconds and emitted `ECR_EXACT_IMAGE_RESOLVED` only after that exact tag appeared
  - final automatic dry-run deploy `31391464284` succeeded on task definition `roy-unpaid-order-cancellation:32`, Fargate task `918c5b2f3d4d44b19a60bb149e05d5e1`, private IP `172.31.45.129`, service `roy-unpaid-order-cancellation`, exact digest above, and `http://127.0.0.1:8000/marker.json`; it again scanned `51` rows / `29` pages, found `35` cancellations / `0` recoveries / `0` failures / `0` writes, and emitted `UNPAID_CANCELLATION_MARKER_OK` plus `DEPLOY_UNPAID_CANCELLATION_OK`
  - local verification passed: `266` CI-equivalent tests, reporting QA smoke, security CI, Python compile, ROY settings JSON, both workflow YAML files, exact-image workflow regression, and `git diff --check`
  - Next exact step: monitor the next natural `02:10 Europe/Bratislava` run on task definition `:32` for exit `0`, recovery/cancellation CloudWatch metrics, and zero failures; do not run `execute_now=true` merely as a smoke because the audited backlog contains `35` real cancellation candidates and the target status sends cancellation emails

- ROY individual picking-list reprint control is merged, deployed, and live (2026-07-22):
  - incident diagnosis for order `2677003601` confirmed that BizniWeb still exposed the order as paid and fulfillable, while the ROY operations state already marked it printed in batch `picking-20260720123200`; the default picking PDF therefore excluded it even though a preview PDF could still render it
  - every fulfillable order row now renders its own read-only `Vytlačiť` action; already marked rows render `Vytlačiť znova`
  - the individual action requests only that order number with `include_printed=1`, so an intentional reprint bypasses the historical print-state filter without clearing or mutating S3 operations state
  - the existing batch PDF and explicit `Označiť vytlačené` flow remain unchanged
  - local verification: Python compile passed; ROY operations/PDF/auth/maintenance focused suite passed (`67` tests); full suite passed (`267` tests); reporting QA, security CI, environment contract, workflow YAML, generated inline JavaScript, and `git diff --check` passed
  - implementation PR `#252` merged to `main` as `2bae90fb792c941015e2fead93147137ec03ae0f`; all PR checks passed
  - exact-merge ECR build run `29918754814` succeeded and published digest `sha256:04cc74ac93d9b74bdc0a8a5b813d65dec14c6338399fd1de2b7f44c8bcecb420`
  - protected deploy run `29918944128` succeeded with `skip_artifact_refresh=true`; Fargate task `bb8667aa3b0e4f359064cc2ae6498537`, private IP `172.31.7.111`, and candidate task definition `roy-reporting-daily:59` proved the exact digest at `http://127.0.0.1:8000/marker.json`, then exited `0`; Scheduler promotion was correctly skipped
  - the ordered maintenance/Fargate/App Runner chain passed: current-live maintenance active, S3 owner active, localhost maintenance/dashboard/artifact markers, live artifact checks, new-live maintenance active, write blocked with HTTP `423`, authenticated ROY operations/PDF smoke, maintenance inactive, restock sentinel restored, and `APP_RUNNER_DEPLOY_OK`
  - App Runner operation `67e5a42558704e668cb1d1ad986f30ff` finished `SUCCEEDED`; service `biznisweb-roy-operations-dashboard` is `RUNNING` on the exact digest above
  - independent production verification returned `/health=200`, inactive maintenance, authenticated dashboard HTML with the individual-print contract, and live order `2677003601` with `picking_printed=true`; its individual endpoint returned HTTP `200`, a one-page/one-order `89,146` byte PDF containing the order, while the unchanged batch filter returned a zero-order PDF and the S3 operations-state ETag remained identical
  - one concurrent forced refresh briefly received BizniWeb's upstream non-JSON error page; the deployment's bounded retries recovered, the final release gate and independent retry both passed, and no persistent runtime failure remained
  - the Codex in-app browser locally blocked the App Runner hostname with `ERR_BLOCKED_BY_CLIENT`; it was not used as a success signal, and the authenticated production HTML/API/PDF checks are the release proof
  - Next exact step: no release action is pending; use `Vytlačiť` or `Vytlačiť znova` on the required order row, and retain the existing explicit batch `Označiť vytlačené` flow for batch state changes

- ROY blocking dashboard maintenance mode is merged, deployed, and live (2026-07-22):
  - `/production/roy` now renders a server-side full-screen Slovak maintenance overlay before first paint, marks the dashboard root `inert`/`aria-hidden`, prevents pointer and keyboard use, and polls the authenticated same-origin status every 5 seconds; a 4-second request timeout and visibility/focus rechecks fail closed for already-open tabs
  - the maintenance source of truth is the separate `daily-reports/roy-sk/operations/maintenance.json` S3 object, isolated from business operations state; active leases require a versioned marker, owner, reason, phase, valid timestamps, a maximum 15-minute renewable TTL, and a 150-minute absolute lifetime
  - the repo-local controller uses S3 ETag conditional writes for start/renew/owner-only stop, has bounded AWS CLI timeouts, and the server returns `423 Locked` for every ROY operations POST while maintenance is active and `503` when production maintenance state cannot be verified
  - the deploy workflow acquires and verifies maintenance before Fargate, Scheduler, or App Runner mutation, heartbeats every 3 minutes, proves the old live overlay on normal deploys, proves the candidate through localhost/CloudWatch, proves the new live overlay plus server-side `423`, and only clears after read-only production gates; its unified trap and finite lease prevent a failed deploy from leaving a permanent lock
  - a one-time capability bootstrap is deliberately restricted to `skip_artifact_refresh=true`; only after the new live runtime proves the active contract is the immutable `maintenance-capability-v1.json` marker created, after which a missing state object fails closed
  - status-source failures keep server mutations fail-closed but cannot strand the visible UI forever: an open tab preserves the last valid lease expiry or uses one non-extending 15-minute safety window, continues polling, and removes the overlay after that bound; malformed remote state is rejected rather than normalized into a writable false-negative
  - verification passed: reporting QA smoke, `261` CI-equivalent tests, `68` focused final-review tests, Python compile, workflow YAML parse, extracted deploy Bash plus all embedded Python syntax, generated inline JavaScript syntax, and `git diff --check`; independent implementation, deploy, and security re-reviews report no remaining maintenance blocker
  - PR `#250` merged to `main` as `5277ee9984652fb150b56bb567d02eb70dbcedf9`; exact merge-image build run `29903497517` succeeded and published digest `sha256:9697f993778540f95c459be8204c15ee8de9a1a84bab22b81fb0ed23b2d58e3e`
  - one-time capability bootstrap deploy run `29903681722` succeeded with `skip_artifact_refresh=true`: lease owner `gha:29903681722:1`, Fargate task `db912f91d04949dd814db8f4e33747e3`, private IP `172.31.44.124`, and candidate task definition `roy-reporting-daily:57` proved the active localhost overlay plus dashboard/artifact markers; the new App Runner runtime proved active maintenance and server-side `423`, enabled `daily-reports/roy-sk/operations/maintenance-capability-v1.json`, cleared maintenance, restored the restock sentinel, and emitted `APP_RUNNER_DEPLOY_OK`
  - normal protected deploy run `29904865696` succeeded on the same digest and proved the non-bootstrap path before any mutation: App Runner service `biznisweb-roy-operations-dashboard` (instance-id/IP `N/A`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`) showed owner `gha:29904865696:1` active at `/production/roy` and `/api/operations/roy/maintenance`; the state path was `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/operations/maintenance.json`
  - the normal deploy's Fargate hard gate used service `roy-daily-report-email`, task `4d911ae9beb54b85ae2ea04f6894f9c8`, private IP `172.31.18.247`, candidate/promoted task definition `roy-reporting-daily:58`, exact image digest above, and marker `http://127.0.0.1:8000/marker.json`; localhost emitted active-maintenance, dashboard, and artifact markers with `160` inventory rows, `39` alerts, both demand/priority models, and `3` anomalies, immutable generation `20260722T090219Z` passed, and Scheduler promotion completed
  - the new live App Runner runtime proved active owner state, blocked a valid write with HTTP `423`, and passed the authenticated ROY gate with `160` inventory rows, `31` impact-sorted alerts, `3` anomalies, `8` restock exclusions, both model markers, and a valid picking-list PDF; it then cleared to inactive, restored sentinel `CODEX-RESTOCK-SMOKE-29904865696-1`, and emitted `APP_RUNNER_DEPLOY_OK`
  - direct public follow-up returned `/health=200`; unauthenticated `/production/roy` and `/api/operations/roy/maintenance` both returned the expected `401` with `no-store`; the only available in-app browser blocked the App Runner hostname locally with `ERR_BLOCKED_BY_CLIENT`, so this was not used as a success signal and the authenticated deploy gate remains the visual/DOM contract proof
  - Next exact step: no release action is pending; on every future ROY deploy require the current-live active, S3 active, Fargate localhost active, heartbeat, new-live active/`423`, inactive, restock-restore, and final deploy marker chain before treating it as successful

- ROY business-impact stock-alert priority and permanent no-restock controls are merged, deployed, and live (2026-07-22):
  - alert order now uses model marker `expected-shortage-cm2-v1`: expected positive CM2 lost during supplier lead time is primary, expected blocked revenue is secondary, and risk severity/strategic flag/cover/SKU are deterministic tie-breakers
  - expected shortage is calculated from the existing robust order-aware demand model with a Poisson order process; direct and bundle demand keep separate anomaly protection, economics, order rates, and typical order sizes before being combined, so one large set order cannot inflate component priority or erase stable direct demand
  - bundle-component blocking economics remain per-SKU marginal impact and are kept separate from additive revenue KPIs, preventing the same set revenue from being multiplied across its components
  - the ROY live dashboard exposes CM2, revenue, and expected shortage impact per alert; each row has a `Dokladňovať` checkbox, and excluded SKUs have a restore/audit panel while remaining visible in inventory and anomaly data
  - permanent exclusions are stored in the existing `daily-reports/roy-sk/operations/state.json` source of truth with state version `2`, normalized SKU identity, ETag/`If-Match` compare-and-swap writes, fail-closed configured S3 reads, Basic Auth, JSON-only writes, and a same-origin custom action header
  - local and App Runner release gates require the new model, combined order-shape fields, impact fields, sorted alert order, exclusion collection, and UI action markers; the production gate also performs a reversible sentinel `exclude -> read -> restore -> read` roundtrip and fails if the sentinel is not removed
  - verification passed: `246` full unit tests, `68` focused final-review tests, reporting QA smoke, security CI, environment contract, Python compile, all project JSON/workflow YAML, all workflow Python/Bash blocks, generated ROY inline JavaScript, and `git diff --check`; two independent read-only reviews report no remaining material finding
  - PR `#247` merged as `1f2621e606634b43589a6105f5b53fb974b2f7ea`; the production roundtrip gate in PR `#248` merged as `9a1b986c3a2aefd57dcfc8fd4ec3183afe2409ef`; exact merge-image build run `29897114705` published digest `sha256:3f2ec669a387fc71dfd018ab00baf999fd604c0f187c0199502fa4208ef41dbd`
  - hard-gated deploy run `29897529064` succeeded with Fargate task `32cb9843916f445d8444d4a3677a4d59`, private IP `172.31.41.253`, task role `BiznisWebReportingTaskRole-roy`, marker path `http://127.0.0.1:8000/marker.json`, and candidate/promoted task definition `roy-reporting-daily:56`; host checks emitted `LOCALHOST_LIVE_DASHBOARD_OK` and `LIVE_ARTIFACT_MARKER_OK` with `160` inventory rows, `40` alerts, `3` demand anomalies, `301` KPI days, and model `expected-shortage-cm2-v1`
  - immutable generation `20260722T070528Z` passed the exact-eight-artifact manifest gate before Scheduler `roy-daily-report-email` was promoted from task definition `:55` to `:56`
  - App Runner operation `2f4972777c1d49408e13989d57e55536` completed successfully for service ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2` on the same digest; the authenticated API/HTML gate confirmed `40` impact-sorted alerts, `160` rows, both model/UI action markers, and `restock_excluded=0`
  - production sentinel `CODEX-RESTOCK-SMOKE-29897529064-1` passed `exclude -> read -> restore -> read` and emitted `restored=true`, so no test exclusion remained; public follow-up returned `200` from `/health` and the protected `/production/roy` path returned the expected unauthenticated `401`
  - Next exact step: monitor the next natural `roy-daily-report-email` run on task definition `:56` for exit `0` and both localhost markers; review the first real no-restock exclusions and impact ordering, changing thresholds only from observed business outcomes through a reviewed PR

- ROY order-aware smart inventory alerts are merged, deployed, and live (2026-07-17):
  - PR `#244` merged to `main` as `0f98aaade55f9769b48dad6946c36ab442ef2419`; independent review finished with no P0/P1/P2 findings
  - replenishment demand now aggregates by SKU and order before forecasting, preserves raw physical sales, caps an unconfirmed large order only in the recurring-demand copy, and leaves the real stock reduction untouched
  - slow/intermittent SKUs use a TSB occurrence × robust order-size baseline; regular SKUs use a robust 30/90/180-day blend; three large orders across at least two weeks establish a repeat pattern, while ordinary acceleration requires the configured 2-of-3 daily-run persistence state
  - products with less than 90 days of observed history retain the exact legacy demand floor; zero/negative stock remains an immediate state alert even when the triggering order is classified as one-off
  - replenishment rows now expose raw demand, adjusted baseline, model/confidence, anomaly adjustment, lead-time stockout probability, service level, and a distinct alert reason; one-off orders also appear in a separate informational collection and live UI table
  - model/version marker `order-aware-tsb-v1` and required raw/adjusted/reason fields are enforced in the host refresh gate, S3 promotion gate, and authenticated App Runner API/UI gate; the ECR workflow now runs the model and dashboard serialization tests
  - independent review additionally proved and closed two sparse-history edges: sales older than the 365-day forecast window still establish the SKU order-size baseline, while two matching 40-unit orders remain uncapped as a natural bulk pattern; the distinct `1 -> 40` sparse jump is still capped and surfaced as an anomaly using only that SKU's history, independent of unrelated portfolio mix
  - persisted M-of-N trend history now honors its configured window instead of truncating to three checks, so future settings such as `5/3` remain valid; the active ROY setting stays `3/2`
  - verification completed locally: the exact reported regression (`1 + 1 + 1` historical units followed by one 40-unit order) changes from a raw `>=40`-unit alert basis to a TSB baseline below `5` units and no false restock alert with 20 units on hand; the same case with zero stock remains `Out of stock` with reason `low_stock_after_large_order`; all `225` CI-equivalent tests, `67` focused model/dashboard tests, reporting QA smoke, Python compile, settings JSON validation, Bash syntax, and `git diff --check` pass
  - the ECR build path filter explicitly includes `inventory_demand_model.py`, so a future model-only merge cannot leave the GitHub source ahead of the production image
  - exact merge-image build run `29561045219` passed and published digest `sha256:be118dcbd1d989e239486a126befc7a90a1c3f3ef73328309bdbe7b73e7f872f`; ECR `latest` was rechecked against that digest immediately before deployment
  - hard-gated deploy run `29561160744` used candidate Fargate task `be214cceb95e47b8b3930a22e99884b0`, private IP `172.31.10.184`, task definition `roy-reporting-daily:55`, log stream `ecs/reporting/be214cceb95e47b8b3930a22e99884b0`, and `/app/scripts/live_dashboard_refresh_gate.sh`; the task used the exact merge digest and exited `0`
  - the refresh completed `3,313` realized orders and one shared inventory scan with `109` pages / `3,753` warehouse rows; all four period analyzers reused that snapshot and emitted `LOCALHOST_LIVE_DASHBOARD_OK:roy:periods=7d,30d,90d,full`
  - immutable generation `20260717T071615Z` (`generated_at_utc=2026-07-17T07:16:16.988860Z`) contains all eight 7D/30D/90D/full payload and HTML artifacts; host/S3 gates confirmed `LIVE_ARTIFACT_MARKER_OK`, model `order-aware-tsb-v1`, `37` delivery alerts, and `5` separately classified demand anomalies before promotion
  - live Scheduler `roy-daily-report-email` is `ENABLED` on `roy-reporting-daily:55`; App Runner operation `03c7b9ee33424807b3df82e372e998ff` finished `SUCCEEDED` at `2026-07-17 09:23:27 Europe/Bratislava`, and service `biznisweb-roy-operations-dashboard` is `RUNNING`
  - Scheduler task definition, App Runner, ECR `latest`, and protected ECR tag `roy-reporting-daily-current` all resolve to the same digest `sha256:be118dcbd1d989e239486a126befc7a90a1c3f3ef73328309bdbe7b73e7f872f`
  - public `/health` returns `ok=true`; unauthenticated `/production/roy` correctly returns HTTP `401`; authenticated live API returns marker `roy-operations-dashboard`, model `order-aware-tsb-v1`, `160` inventory rows, `37` alerts, `5` demand anomalies, and zero missing required smart fields; authenticated HTML contains `Raw → baseline`, the `Neobvykle veľké objednávky` panel, and the picking-list PDF link
  - the in-app visual browser could not load the App Runner hostname because the local browser client returned `ERR_BLOCKED_BY_CLIENT`; this was not used as a success signal, while the authenticated deploy gate and direct post-deploy HTTP/API checks both passed
  - Next exact step: monitor the next natural `roy-daily-report-email` run on `:55` for exit `0`, the four localhost markers, and persistence-state carryover; after at least 7 daily runs, review the separate anomaly table versus true repeat demand and tune only the versioned `projects/roy/settings.json` thresholds through a reviewed PR if evidence warrants it

- Monthly ROY+VEVO accounting delivery is deployed, recovered, and proven with a real SES send (2026-07-16):
  - root cause of the missed `2026-07-14 06:00 Europe/Bratislava` delivery was an unrecoverable ECR reference: Scheduler did invoke task `95a2e43841c4408d94f02fa8b54b5d23` on task definition `monthly-creditnote-export:12`, but its digest had lost all tags and the repository's seven-day untagged-image lifecycle deleted it; no application log stream or SES attempt was created
  - PR `#238` merged as `b3076dac34a45abcefb8e6e781b4a24d5ae6818c`; the runner now creates one credit-note PDF per shop and one Money S3 invoice XML per shop, filters every invoice by issue date (`inv_date`), and uses the complete previous calendar month for all four attachments
  - deployment no longer consumes mutable `latest`: it builds the exact merge commit, protects candidate/current image tags, keeps the previous current image during smoke, validates both PDFs (`%PDF-`) and both XML roots (`MoneyData`) directly in Fargate, curls `http://127.0.0.1:8000/marker.json`, and promotes the scheduler only after the marker succeeds
  - automatic post-merge dry-run `29473377826` used task `c531a978eeb2426594a4b9d6fbbd4a28`, private IP `172.31.15.212`, task definition `:22`, and digest `sha256:d09988fcefbb1b404abfd00f879a6fac9268e8299c729e418df73e854a36d687`; exit `0` and `CREDITNOTE_EXPORT_MARKER_OK` confirmed `2026-06-01..2026-06-30`, `42` credit notes, `895` invoices, `2` PDFs, and `4` total attachments before promotion
  - controlled real-email run `29473768133` used task `2d3429da7e63477da7e3e7227aa216b6`, private IP `172.31.19.196`, task definition `:23`, and protected digest `sha256:cc4232ef6d571998d33676b8b2cfcd211f16715ed20c0f3052b2d08309ec3b8b`; exit `0` and `CREDITNOTE_EXPORT_EMAIL_MARKER_OK` confirmed ROY `20` credit notes / `468` invoices, VEVO `22` credit notes / `427` invoices, and SES `MessageId=0107019f6966cf0e-3ececafe-68eb-4a36-9b7c-a6f9ed745a7a-000000`
  - live Scheduler `monthly-creditnote-export` is `ENABLED` on `monthly-creditnote-export:23`, `cron(0 6 14 * ? *)`, timezone `Europe/Bratislava`; target DLQ `monthly-creditnote-export-dlq` is empty with 14-day retention, and alarms `monthly-creditnote-export-dlq-not-empty` plus `monthly-creditnote-export-run-failed` are both `OK` and notify `vevo-reporting-alerts-mil-final`
  - verification: `183` CI-equivalent tests, `24` focused accounting/workflow tests, reporting QA smoke, Python/JSON/YAML validation, extracted Bash `-n`, `git diff --check`, PR CI, exact ECR digest checks, direct Fargate localhost markers, and AWS runtime checks passed; direct Gmail inbox inspection was not possible because the connector lacks Gmail read scope and the in-app browser was signed out, so delivery is confirmed through SES acceptance rather than mailbox read state
  - Next exact step: monitor the next natural run on `2026-08-14 06:00 Europe/Bratislava` for the `2026-07-01..2026-07-31` window, require a fresh ECS task/log marker and SES MessageId, and confirm the DLQ remains empty and both alarms remain `OK`; reauthorize Gmail read scope only if direct inbox-state verification is required

- ROY exact zero-revenue gift allowlist is deployed and the complete history is regenerated (2026-07-16):
  - PR `#237` merged as `2a221d5075235f644b7b34b4de3363862d7947fb`; the exception is restricted to exact reporting SKUs `R99003` (`Sada nožov Roy 3-dielna Lux`), `11004` (`Roy Hunter Knife 11004`), and `11001` (`Roy Hunter Knife 11001`) and only applies to a positive-quantity line with exactly `EUR 0` net revenue; paid rows retain mapped unit costs `EUR 16.58`, `EUR 4.17`, and `EUR 4.00`
  - deploy/backfill run `29473167153` used exact digest `sha256:18660c4fc22978fab4399b686adf694e387ea1a86420c14d66c1dd7981a870dc`, Fargate task `a8dd3e87b80e4fdb9b7869ae25404516`, private IP `172.31.12.25`, task definition `roy-reporting-daily:51`, and `/app/scripts/live_dashboard_refresh_gate.sh`; localhost dashboard checks, `LIVE_ARTIFACT_MARKER_OK`, immutable manifest validation, schedule promotion, App Runner operation `db1647a23ebd4431af15101bf1b33796`, and authenticated UI/API/PDF smoke all passed
  - immutable audit generation `20260716T055512Z` proves the gift rule itself: `1,437` zero-revenue gift rows use `zero_revenue_gift_mapped_cost` only for the three allowlisted SKUs (`R99003=76`, `11004=1,100`, `11001=261`), all at zero applied cost; paid `11001` is `27` units / `EUR 302.95` revenue / `EUR 108.00` cost / `EUR 194.95` gross profit and no longer appears in `loss_product_rows`
  - final comparison found the same generation was not safe to keep live: a BizniWeb page fallback logged payment-metadata enrichment `attempted=29`, `succeeded=21`, `failed=8`; six orders `2677001221`-`2677001226` then disappeared from realized revenue, understating revenue by `EUR 459.87` and product gross profit by `EUR 207.60`, while `is_partial=false` and QA incorrectly remained green
  - latest aliases were therefore restored and byte-verified across all eight artifacts against complete generation `20260716T014642Z`; live `/health`, authenticated `/production/roy`, and full API return HTTP `200`, company revenue `EUR 238,565.15`, company profit `EUR 20,490.09`, `qa_failure_count=0`, and `is_partial=false`; immutable `20260716T055512Z` remains only as audit evidence
  - a manual retry on the same `:51` digest used task `14054e092d3e44bfacbf40f891ffcdd3` / private IP `172.31.33.31`; one unresolved lookup for order `2677001207` triggered the manual gate, so the task was stopped before S3 publication with reason `Cancelled before publish: payment metadata enrichment failed for order 2677001207`
  - payment-metadata hard gate PR `#240` merged as `8ddcc9e26a3bd38d0adb267502fba4bdefb3e777`; build run `29477815194` passed and published exact digest `sha256:ce7eca13dfd40202fe49d05705081abe8b4e3b025f5d53a70b0ef29188e3fb1f`
  - deploy attempt `29478002514` used Fargate task `ebc355e6267d422cb8d73595fd91701b`, private IP `172.31.3.0`, task definition `roy-reporting-daily:52`, exact digest above, and `/app/scripts/live_dashboard_refresh_gate.sh`; deterministic missing `price_elements` for `2677001207` exhausted all three retries and the new gate failed before localhost marker, S3 publish, schedule promotion, or App Runner rollout; `latest` remains byte-stable generation `20260716T014642Z`, scheduler remains on `:51`, and App Runner remains on the previous complete digest
  - read-only order audit proved `2677001207` is a legacy internal `EUR 1` trademark-license order and is correctly absent from the stable realized-revenue export: invoice `2677001128` is `paid=false`, `pay_date=null`, has no payment receipts, and its PDF payment form is blank; invoice paid state cannot be a general fallback because valid COD/prepaid cases can also remain invoice-unpaid
  - exact override PR `#241` merged as `50f9459e130d93e9aad065a566601189b6505052`; build run `29479415450` passed and published exact digest `sha256:2a1ce526f1968a3fe0cd3e3d821ce97d3f9307837ab60a34a344deee42be6d81`
  - deploy attempt `29479591721` used task `383d089fdda44e99b070d493a3c56c5a`, private IP `172.31.17.133`, task definition `roy-reporting-daily:53`, exact digest above, and the tracked host gate; payment fallback was clean (`candidates=28`, `succeeded=28`, `unresolved=0`), the configured non-realized override fired exactly once, and the export found the expected `3,298` realized orders
  - the same run exposed another pre-existing partial-report path: 7D inventory completed with `109` pages / `3,753` warehouse rows, but the independent 30D inventory fetch failed after page `80` and three API attempts; backend still saved the 30D payload with `inventory_rows=0`, so the task was manually stopped before S3 publish with reason `Cancelled before publish: 30D inventory snapshot failed after 3 API attempts`; `latest` remains generation `20260716T014642Z` and scheduler remains on `:51`
  - period-inventory gate PR `#242` merged as `6ef9d9e77deb69144b99f95c896df7d2f671d55a`; ROY now fetches one inventory snapshot before any period child export, fails closed on an exception or empty snapshot, shares the verified frame through a run-scoped cache, and gives every 7D/30D/90D/full analyzer a deep copy; this removes three redundant 109-page API scans and prevents period-specific inventory drift; `214` full tests, `83` focused tests, `13` inventory-model tests, reporting QA smoke, compile, JSON validation, and `git diff --check` passed
  - build run `29481748617` published the exact merge image `sha256:d635ffe5d9c5e6f18c6f18e8098a335e88c30cfe27852652b49a59407b1efd15`; final deploy/backfill run `29481976018` used Fargate task `160733d9618649d7a4ccd57c6220f80b`, private IP `172.31.2.120`, task definition `roy-reporting-daily:54`, log stream `ecs/reporting/160733d9618649d7a4ccd57c6220f80b`, and `/app/scripts/live_dashboard_refresh_gate.sh`
  - final runtime gates passed: payment enrichment `candidates=24`, `succeeded=24`, `unresolved=0`; exactly one configured missing-payment non-realized override; `3,298` realized orders; one inventory API scan with `109` pages / `3,753` warehouse rows; one `PERIOD_BUNDLE_INVENTORY_SNAPSHOT_READY`; exactly four cache-reuse markers; zero inventory fetch errors; `LOCALHOST_LIVE_DASHBOARD_OK` for all four periods; `LIVE_ARTIFACT_MARKER_OK`; task exit `0`
  - immutable generation `20260716T082153Z` passed a row-level business audit across all manifest artifacts, the full CSV, data-quality JSON, daily aggregation, and all four payloads: `7,068` item rows / `3,298` orders; the only zero-cost gift rows are exact SKU `R99003=76`, `11004=1,103`, and `11001=261`, always positive quantity with exactly `EUR 0` net revenue and `EUR 0` cost; the three additional `11004` gift rows versus the earlier estimate belong to restored orders `2677001221`, `2677001222`, and `2677001225`, not duplicates
  - paid rows retain mapped costs and reconcile exactly: `R99003` `11` units / `EUR 290.80` revenue / `EUR 182.38` cost / `EUR 108.42` gross profit; `11004` `50` units / `EUR 756.31` / `EUR 208.50` / `EUR 547.81`; `11001` `27` units / `EUR 302.95` / `EUR 108.00` / `EUR 194.95`; SKU `11001` is absent from both loss-product outputs
  - final company revenue/profit is `7d EUR 6,480.93 / 720.07`, `30d EUR 28,783.05 / 4,116.47`, `90d EUR 80,876.80 / 10,005.15`, full `EUR 238,565.15 / 21,531.53`; every period has `qa_failure_count=0`, `qa_errors=[]`, and `is_partial=false`, while all periods share the same non-empty `2026-07-16` inventory signature; the final profit is `EUR 2.53` below the earlier pre-run estimate solely because ad data refreshed by `EUR 2.52` Google spend plus `EUR 0.01` Facebook spend
  - Scheduler `roy-daily-report-email` is `ENABLED`, `cron(30 1 * * ? *)`, `Europe/Bratislava`, and promoted to `roy-reporting-daily:54`; App Runner operation `38f21fa545534e678b5c15d242b53331` succeeded and service `biznisweb-roy-operations-dashboard` is `RUNNING` on the exact digest; authenticated public health, operations board, full accounting API, and full HTML report all returned HTTP `200`, with live company profit `EUR 21,531.53`
  - Next exact step: monitor the next natural `01:30 Europe/Bratislava` ROY schedule on `roy-reporting-daily:54`, require a fresh complete generation with one inventory scan/four cache reuses and the normal SES delivery, then confirm the live scheduler and App Runner remain on the exact promoted digest

- VEVO missing daily email incident is restored and the scheduler authorization regression is fixed (2026-07-16):
  - EventBridge Scheduler invoked `vevo-daily-report-email` at `2026-07-16 01:00:13 Europe/Bratislava`, but CloudTrail event `56a898c9-3578-4c4d-9421-30f0be9934fb` failed before ECS task creation with `AccessDenied`: `vevo-reporting-scheduler-role` could not `iam:PassRole` to `BiznisWebReportingTaskRole-vevo`. The failed target was `vevo-reporting-daily:19`; no task ARN, private IP, CloudWatch application stream, or SES attempt existed for that scheduled invocation
  - root cause was deployment workflow drift: the live-dashboard workflow created and promoted the dedicated project task role but did not update the scheduler role's PassRole policy. SES was healthy independently: the same morning's ROY schedule sent successfully, while later VEVO backfills explicitly used `--skip-email`
  - controlled replacement email run `29470874253` used Fargate task `88a3fabc8bca4e19afe673f581da6aab`, private IP `172.31.14.157`, task definition `vevo-reporting-daily:22`, and digest `sha256:b98c28673113bbf686a481509cbbfabf18282bbe8fa4ac2cc64b3365df9df5d6`; the creditnote guard found `eligible_orders=0`, invoices were skipped, the task exited `0`, `LOCALHOST_MARKER_OK` reported `439` daily-profit rows / `280` creditnotes / `EUR 4,938.74` credited gross, and both production-board and daily-profit-loss UI smokes passed
  - SES accepted the replacement report as `MessageId=0107019f6937fb8a-3972df40-e789-4be8-891e-3db0cbc3c5f4-000000`; immutable S3 generation `20260716T043822Z` contains the eight manifest-verified 7d/30d/90d/full JSON/HTML artifacts
  - durable fix PR `#235` merged as `12b17a8234ac092f5d476dee3f1fcc4307b1f0ae`: the workflow now allowlists the repo-configured scheduler role, validates its exact `scheduler.amazonaws.com` trust policy, installs a separate least-privilege project PassRole policy before promotion, waits for effective authorization through IAM simulation, and fails closed on task-definition, role, or modification-date drift. Verification: `189` unit tests, focused workflow tests, YAML/Bash syntax, Python compile, live-document helper validation, CI, and independent P0/P1 review passed
  - live policy `ReportingSchedulePassRole-vevo` is installed on `vevo-reporting-scheduler-role`; simulation returns `allowed` only for `BiznisWebReportingTaskRole-vevo` and `ecsTaskExecutionRole` with `iam:PassedToService=ecs-tasks.amazonaws.com`. The schedule remains `ENABLED`, `cron(0 1 * * ? *)`, `Europe/Bratislava`, on `vevo-reporting-daily:22` without schedule drift
  - known monitoring debt remains deliberately unmutated: legacy `vevo-reporting-missing-email-heartbeat` / `vevo-reporting-run-failed` alarms still target obsolete namespace `VevoReporting`, no schedule-specific DLQ/pre-container alert exists, all eight current schedules share Scheduler group `default`, and manual sends are not distinguished from scheduled sends. A quick group-wide/24-hour alarm patch was rejected because unrelated failures or a manual resend could mask a later VEVO miss
  - Next exact step: verify the next natural `01:00 Europe/Bratislava` VEVO schedule creates an ECS task on `:22`, emits the localhost/report markers, and sends SES; separately design a schedule-specific DLQ or deadline canary with distinct scheduled/manual delivery metrics before replacing the stale alarms

- VEVO/ROY authoritative 90% product-margin policy is deployed and both complete histories are regenerated (2026-07-16):
  - added a strict exact-SKU `authoritative_margin_override_skus` policy that intentionally replaces a mapped purchase cost only for positive net revenue and positive reported quantity; the resolved purchase cost remains exported in `purchase_cost_reference_*` for audit, while zero/negative lines and the ROY zero-revenue knife-gift exception retain their existing economics
  - VEVO contains the exact catalog/reporting identities for the measuring cup, all six Santal car-fragrance variants, all six dryer perfumes, the six floor-perfume parents plus their 10/30 ml option identities, and Pure Harmony cleaner including the directly named 2x/3x bundles; mixed wool-ball bundles remain excluded because they have separate component economics
  - ROY contains the same six Santal identities so the same product has consistent economics across shops; invalid policy values, empty SKUs, broad label matching, and project leakage fail closed or are covered by regression tests
  - exact per-line cent-rounded immutable-history replay through `2026-07-14`: VEVO `1,056` rows / `1,152` units / `EUR 3,049.00` revenue, product/company profit delta `7d +EUR 22.00`, `30d +EUR 63.34`, `90d +EUR 183.14`, full `+EUR 340.57`; ROY Santal `5` rows / `7` units / `EUR 65.59` revenue, delta `7d +EUR 9.35`, `30d +EUR 14.70`, `90d +EUR 14.70`, full `+EUR 36.08`; combined full delta `+EUR 376.65`
  - VEVO full-history category deltas are measuring cup `+EUR 208.60`, Santal `+EUR 36.96`, dryer perfume `-EUR 15.77` (its prior mapped margin was about `93.8%`), floor perfume `+EUR 84.26`, and Pure Harmony `+EUR 26.52`; the replay has exact aggregate `revenue - cost = profit` after the added cent-rounding guard
  - product-cost QA now records authoritative-policy rows, units, revenue, applied cost/profit, mapped reference cost, policy delta, per-product detail, and the separate `authoritative_margin_90_override` source; the normal 35% missing-cost fallback and known-cost precedence for every non-policy product are unchanged
  - the first VEVO generation `20260715T212920Z` correctly exported `1,056` policy rows / `EUR 3,049.00` revenue / `EUR 304.75` cost / `EUR 2,744.25` product profit and exact row arithmetic, but post-deploy comparison caught that the newly added QA `authoritative_margin_applied_cost` field summed unrounded unit costs as `EUR 304.90`; the follow-up now sums exported `total_expense`, rounds mapped reference totals consistently, and regression-checks both aggregate and per-product QA cost
  - verification: final full suite `176` tests OK, focused boundary/config/rounding tests OK, reporting QA smoke, Python compile, both project JSON parses, and `git diff --check` OK; independent catalog, code, and historical-impact reviews found no P0/P1 after the rounding repair, while their normalization-performance P2 was also fixed with strict duplicate-normalized-SKU rejection and O(1) lookup
  - implementation and safety chain merged through PRs `#228`-`#233`; the final CPA precision PR `#233` merged as `727843b85dea8dc77323bc445aee0b70c4a8ae4b`, after `183` unit tests, `76` focused tests, reporting QA smoke, compile/config checks, CI, and independent no-P0/P1 review
  - build run `29462077709` published exact digest `sha256:b98c28673113bbf686a481509cbbfabf18282bbe8fa4ac2cc64b3365df9df5d6`; both schedules, both App Runner services, and all ECR protection now use that digest
  - VEVO deploy/backfill run `29462210962`: Fargate task `b6fbff5a22344f729b6edac8e07d1e76`, private IP `172.31.32.237`, task definition `vevo-reporting-daily:22`, gate `/app/scripts/live_dashboard_refresh_gate.sh`, localhost dashboard/artifact markers, exit `0`; immutable generation `20260716T010224Z` has `13,112` rows and all 8 manifest hashes/sizes verified
  - VEVO 90% result: full `1,057` rows / `1,153` units / `EUR 3,049.81` revenue / `EUR 304.83` cost / `EUR 2,744.98` product profit; company revenue/profit is `7d EUR 1,328.57 / 305.03`, `30d EUR 5,549.59 / 225.21`, `90d EUR 22,951.93 / 1,948.66`, full `EUR 118,807.61 / 17,875.88`
  - VEVO CPA is reconciled in production: Sandbox spend `EUR 0.28`, attributed orders `0.2305`, reported CPA `EUR 1.21`; all periods have zero QA failures, `is_partial=false`, zero credit-note `order_not_found`, exact fulfillment parity, and zero cent/policy mismatches
  - VEVO scheduler is `ENABLED`, `cron(0 1 * * ? *)`, `Europe/Bratislava`, task definition `:22`; App Runner operation `d96d3543d1af4fb898c77591ac014f8d` succeeded and service `biznisweb-vevo-production-board` is `RUNNING` on the exact digest; health, UI, live API, and all report-period APIs returned HTTP `200`
  - ROY deploy/backfill run `29463490671`: Fargate task `3cc699a80a3f4d8890b4bd138440888b`, private IP `172.31.34.180`, task definition `roy-reporting-daily:50`, the same gate and exact digest, localhost dashboard/artifact markers, exit `0`; immutable generation `20260716T014642Z` has `5,965` rows and all 8 manifest hashes/sizes verified
  - ROY 90% Santal-only result: full `5` rows / `7` units / `EUR 65.59` revenue / `EUR 6.56` cost / `EUR 59.03` product profit; uplift versus 35% is `7d +EUR 9.35`, `30d/90d +EUR 14.70`, full `+EUR 36.08`; company revenue/profit is `7d EUR 6,480.93 / 570.63`, `30d EUR 28,783.05 / 3,455.03`, `90d EUR 80,876.80 / 8,963.71`, full `EUR 238,565.15 / 20,490.09`
  - ROY rule audit: KIRVO is `11` units at exactly `EUR 1.90` net each; Micro SD has canonical `MICRO-SD-64GB` on `42` rows / `53` units at `EUR 3.30` each and no visible legacy hash; `76` zero-revenue knife-gift rows keep zero cost; `IS-Q6L` remains deliberately below cost at `EUR 154.47` revenue / `EUR 185.76` cost / `-EUR 31.29` profit; bundle and 35% sources match the prior healthy baseline
  - ROY has zero QA failures in every period, `is_partial=false`, zero credit-note `order_not_found`, exact fulfillment parity, and zero cent/policy mismatches; inventory remains `3,276` products / `17,396` units / `160` visible rows / `19` alerts
  - ROY scheduler is `ENABLED`, `cron(30 1 * * ? *)`, `Europe/Bratislava`, task definition `:50`; App Runner operation `cdc9741d14ac43e8b8fc9036c1608f77` succeeded and service `biznisweb-roy-operations-dashboard` is `RUNNING` on the exact digest; direct health/UI/API checks returned `200`, unauthenticated UI returned `401`, and the workflow verified the preview picking-list PDF plus all report APIs
  - simultaneous `ecommerce.ardanpreston.com` work remained isolated: these deployments touched only reporting ECR, ECS/Fargate, the VEVO/ROY S3 prefixes, reporting schedules, and the two reporting App Runner services; no ecommerce EC2 instance, service, port, process, or filesystem path was changed
  - Next exact step: verify the next regular VEVO and ROY scheduled runs stay on `:22` / `:50`, create fresh healthy generations, and send their normal reports; separately optimize ROY to reuse one inventory snapshot across the four period reports and replace the remaining `datetime.utcnow()` deprecation without changing accounting behavior

- VEVO bundle component-cost resolver is deployed and the full reporting history is regenerated (2026-07-15):
  - homogeneous bundle labels with a safe leading multiplier such as `2x` or `2×` now derive purchase cost from one unambiguous known single-product cost; direct SKU/EAN/import/warehouse/title costs always retain precedence, and mixed/gift-like or ambiguous labels fail closed to the configured missing-cost fallback
  - mixed bundles use explicit validated component rules with exact labels, positive quantities, existing finite non-negative component costs, and narrowly declared shared identifiers; every derived line keeps an auditable `bundle_components_inferred:*` or `bundle_components_configured:*` expense source
  - requested SKU `H-975E4FC5` (`2x Parfum do prania Vevo Natural No.07 Ylang Absolute 500ml`) is now `EUR 12.28` per bundle from `2 x EUR 6.14`; immutable live export has `2` rows / `2` bundles / `EUR 81.14` revenue / `EUR 24.56` cost / `EUR 56.58` pre-ad profit
  - configured mixed bundle results are also live: Ylang perfume + 1 gel `EUR 8.57` per bundle (`8` rows), Ylang perfume + 3 gels `EUR 13.43` (`2` rows), and wool balls + Royal Cotton 10 ml `EUR 2.10` (`2` rows)
  - historical company-profit impact versus the immediately preceding VEVO generation: `7d +EUR 7.95` to `EUR 372.93`; `30d -EUR 13.52` to `EUR 255.21`; `90d -EUR 34.52` to `EUR 1,993.66`; full `-EUR 26.10` to `EUR 17,504.90`. The individual full-history changes are `2x Ylang +EUR 28.18`, combo 1+1 `-EUR 49.12`, combo 1+3 `-EUR 22.00`, and wool balls combo `+EUR 16.84`
  - missing-cost exposure after the repair is `11` products / `49` rows / `52` units / `EUR 409.97` revenue, or `0.35%` of full `EUR 118,599.91` revenue; estimated fallback profit is `EUR 143.52` and all four report periods have `qa_failure_count=0`
  - implementation PR `#223` merged as `8fa4c68be2b74b5cc7f3c61eea06ac0f12cb607d`; full suite `168` tests OK, reporting QA smoke, compile/config parsing, `git diff --check`, independent resolver review, and independent historical-impact replay passed
  - ECR build run `29441831388` published exact digest `sha256:e87751ca04c074f8d5410850fb22e755bd3244c8ccc09d18f71b44b3c2ea1dab`; deploy/backfill run `29442051858` used Fargate task `a5aaf0a2d4a246bfab931dfe3a91c13c`, private IP `172.31.7.247`, task definition `vevo-reporting-daily:17`, and dedicated task role `BiznisWebReportingTaskRole-vevo`
  - the task exited `0` after host-side `LOCALHOST_LIVE_DASHBOARD_OK:vevo:periods=7d,30d,90d,full` and `LIVE_ARTIFACT_MARKER_OK`; schedule `vevo-daily-report-email` remains `ENABLED` at `01:00 Europe/Bratislava` and was promoted to `:17` only after the host and artifact gates
  - immutable generation `20260715T190824Z` is stored under `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/vevo/20260715T190824Z/`; `latest/generation.json` points to exactly eight live JSON/HTML artifacts, and independent download verification matched every byte size and SHA-256
  - App Runner update operation `c66288207482400d8122a9fcea163b13` finished `SUCCEEDED`; service `biznisweb-vevo-production-board` is `RUNNING` on the exact new digest, current DNS IPs are `3.126.244.1`, `35.157.121.17`, and `3.74.221.100`, and public `/health` returns HTTP `200`
  - independent authenticated checks returned HTTP `200` for `/production/vevo`, `/api/production/vevo/live?refresh=1`, `/report/vevo?period=full`, `/dashboard/vevo?period=full`, and all four JSON periods; every JSON response exactly matched its immutable manifest hash, while the full report contains the CEO cockpit, `35%` fallback copy, and working `7d/30d/90d/full` links
  - simultaneous `ecommerce.ardanpreston.com` deployment remains isolated: this rollout changed only VEVO ECS/Fargate, the VEVO S3 prefix, shared ECR image metadata, and the VEVO App Runner service; it made no EC2, ecommerce process, port, service, or filesystem mutation
  - in-app visual browser automation remains locally unavailable with `failed to write kernel assets` / OS error `3`; production is unaffected and host, S3, authenticated HTML/API, and workflow UI smoke all passed
  - Next exact step: verify the next scheduled `vevo-daily-report-email` run at `01:00 Europe/Bratislava` stays on task definition `:17`, creates a fresh immutable generation and CloudWatch stream, and sends SES successfully; rerun the visual desktop/mobile check when the in-app browser connection is restored

- VEVO decision-safety remediation is deployed and the full reporting history is regenerated (2026-07-15):
  - configured the requested `35%` margin fallback only when no mapped purchase cost exists; mapped real costs and intentional below-cost sales retain precedence. The zero-revenue gift exception is explicitly ROY-only; VEVO keeps mapped cost even on `EUR 0` lines
  - product-cost QA now serializes the complete missing-cost product list with SKU, rows, units, net revenue, share of total item revenue, and estimated pre-ad profit; the modern report renders the full list instead of only five products
  - weekly and cohort CAC now use complete Facebook + Google spend calendars, including spend on zero-order days and spend-only weeks; weekly CAC, LTV/CAC, payback, and cumulative CAC are `undefined`, never a false `0`, until a valid acquired-customer denominator exists, while cumulative spend remains reconciled; critical QA emails receive a `[CRITICAL QA]` subject and a do-not-use-for-decisions warning
  - VEVO now receives the CEO cockpit with 7D/30D/90D/full profit windows, a 30-day profit waterfall, five confidence-aware actions, a real-creditnote versus order-status-proxy view, and an explicit `EUR 70/day` manual fixed-overhead estimate warning
  - live dashboard JSON/HTML endpoints now select exact `7d`, `30d`, `90d`, and `full` artifacts through an atomic `latest/generation.json` pointer with immutable keys, size and SHA-256 checks; an existing invalid/unreadable manifest fails closed, while the mutable aliases are used only for explicit pre-manifest `NoSuchKey/404` compatibility
  - deploy workflow now creates a dedicated `BiznisWebReportingTaskRole-vevo`, copies only non-artifact baseline permissions, grants only the VEVO S3 prefix, registers a candidate task definition, runs the full Fargate backfill and localhost marker, validates S3, and only then promotes `vevo-daily-report-email`; App Runner update/UI smoke follows the host gate and has an explicit pre-deploy context marker
  - exact pre-change evidence through `2026-07-14`: `13` missing-cost products, `53` item rows, `56` units, `EUR 523.47` net revenue, `0.441%` of official `EUR 118,599.91` revenue, and `EUR 183.21` estimated profit at 35%; verified from production stream `ecs/reporting/0d567827c19b4d8bb97407d321c9bfac` plus a read-only `2026-06-17..2026-07-14` GraphQL delta (`225` orders, `13` fallback rows, `EUR 187.62`)
  - recent concentration is more material than the full-history ratio: `2026-06-15..2026-07-14` missing-cost revenue is `EUR 203.79`, or `3.53%` of `EUR 5,769.95` revenue, with `EUR 71.33` estimated profit at 35%; product-cost QA now exposes and warns on this rolling 30-day share
  - zero-revenue audit: VEVO has no `EUR 0` line without a purchase-cost map. Its two `EUR 0` lines have known costs (`8586024430341` = `EUR 4.29`, `H-4F7230B9` = `EUR 0.31`) and now correctly retain total cost `EUR 4.60` / profit `EUR -4.60`; the 13-SKU paid missing-cost list is therefore complete
  - pre-bundle follow-up captured by this snapshot is resolved by the newer deployment entry above: `H-975E4FC5` now uses the confirmed `2 x EUR 6.14` component cost. The separate review of divergent mapped costs for CZ `Spropitné` and `Pojištění proti rozbití` versus their zero-cost SK variants remains open
  - review found and fixed pre-merge blockers around schedule promotion order, shared VEVO/ROY task-role S3 permissions, non-atomic S3 aliases, spend-only `CAC=0`, legacy/modern false-zero CAC rendering, broken live `7d/30d/90d` switching, and the rolling-window anchor; the ROY zero-revenue gift scope now uses token-boundary knife matching, so `ROY nožnice` and every other non-knife line keep mapped cost
  - local verification after the fixes: full unit suite `157` tests OK; targeted manifest/CAC/ROY-gift/live-period tests `12` OK; `scripts/reporting_qa_smoke.py`, Python compile, workflow YAML parse, outer and nested Bash syntax, all workflow Python heredocs, modern dashboard JavaScript syntax, and `git diff --check` OK; independent workflow and data-code reviews both report no P0/P1 blockers
  - infra hard-gate before implementation: ECS/Fargate instance-id `N/A`, schedule/service `vevo-daily-report-email`, task definition `vevo-reporting-daily:14`, image digest `sha256:c23651ebd051bd88cecd9f529e70b5f61f4a891aa7d31d166835783b6807c30b`; App Runner instance-id `N/A`, service `biznisweb-vevo-production-board`, URL `https://2mhmsmgq3m.eu-central-1.awsapprunner.com`, paths `/health`, `/production/vevo`, `/api/vevo/latest?period=full`, status `RUNNING`
  - PR `#219` merged as `9763651895f6273cc7c00db38addd00aad76bc53`; ECR build run `29435475267` published candidate digest `sha256:1425de51b4be2bb2084085d70aacaa03c93efd425a66da0b2ba4e2e73ae5c216`
  - first deploy run `29435680826` failed safely before ECS task creation because the inline `RunTask` container override was about `10,355` bytes versus the AWS `8,192`-byte limit; after that failed attempt the schedule remained ENABLED on `vevo-reporting-daily:14`, App Runner remained unchanged, and unscheduled candidate task definition `vevo-reporting-daily:15` was harmless
  - the recovery fix moves the complete localhost/backfill/marker gate into tracked `scripts/live_dashboard_refresh_gate.sh`, uses a short absolute-path ECS override, keeps the script in the ECR build path filter, and adds a hard override-size regression gate
  - recovery verification: serialized override is about `446` bytes, full suite `160` tests OK, the exact ECR test list `152` tests OK, recovery tests `3/3` OK, YAML and Bash syntax OK, `git diff --check` OK, and independent review reports no P0/P1 blockers
  - refresh-script recovery PR `#220` merged as `bea1688302356f9d431e6c932dc30915ad2ff233`; ECR build run `29436541436` published digest `sha256:efe5ceb088738b4ff9c7064e87bfdf6c8dd0080b740a420db1f1c7185877898d`
  - successful deploy/backfill run `29436711100` used Fargate task `8f10a9fe05c140a0bb2243d050be630d`, private IP `172.31.28.67`, task definition `vevo-reporting-daily:16`, dedicated task role `BiznisWebReportingTaskRole-vevo`, and a `446`-byte override; the task exited `0` after `LOCALHOST_LIVE_DASHBOARD_OK:vevo:periods=7d,30d,90d,full` and `LIVE_ARTIFACT_MARKER_OK`
  - one BizniWeb `price_elements` lookup for order `2602007112` returned an internal error; the tracked fallback continued, payment metadata enrichment reported `26/27`, and the complete `7,090`-order export finished successfully
  - immutable live generation `20260715T174819Z` is stored under `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/vevo/20260715T174819Z/`; `latest/generation.json` contains exactly eight JSON/HTML artifacts with verified byte sizes and SHA-256 hashes
  - schedule `vevo-daily-report-email` remains ENABLED and was promoted only after host/S3 gates from `vevo-reporting-daily:14` to `vevo-reporting-daily:16`; the task definition and App Runner service use the exact protected digest above
  - App Runner update operation `f74fa84500144e4ea6cfd77d94c063bc` finished `SUCCEEDED`; service `biznisweb-vevo-production-board` is `RUNNING` at `https://2mhmsmgq3m.eu-central-1.awsapprunner.com` with VEVO-only instance role and S3 prefix `daily-reports/vevo`
  - independent authenticated live verification passed `/health`, `/production/vevo`, `/api/production/vevo/live?refresh=1`, and JSON/HTML accounting routes for `7d`, `30d`, `90d`, and `full`; every report contains the CEO cockpit, live period href map, five actions, and the configured `35%` missing-cost rule
  - live company results through `2026-07-14`: `7d` revenue/profit/margin `EUR 1,425.19 / 364.98 / 25.61%`; `30d` `EUR 5,769.95 / 268.73 / 4.66%`; `90d` `EUR 23,370.14 / 2,028.18 / 8.68%`; full history `EUR 118,599.91 / 17,531.00 / 14.78%`
  - the complete live missing-cost exposure is `13` products, `53` rows, `56` units, `EUR 523.47` revenue (`0.44%` of full revenue), and `EUR 183.24` estimated product profit; the rolling latest `30d` concentration is `EUR 203.79` (`3.53%`) and `EUR 71.34` estimated product profit, so it is immaterial historically but noticeable recently
  - at this deployment snapshot the live QA had `0` failures and `0` unknown-source rows; its `H-975E4FC5` follow-up was completed by the newer bundle deployment entry above
  - simultaneous `ecommerce.ardanpreston.com` work is isolated: this deploy changed only the VEVO Fargate schedule/task definition, VEVO S3 prefix, ECR digest, and VEVO App Runner service; it did not mutate any EC2 host, ecommerce process, port, filesystem path, or service
  - in-app browser automation was locally unavailable (`failed to write kernel assets`, OS error `3`); production was unaffected, and the localhost host gate, App Runner health, authenticated live HTML/API checks, and workflow accounting smoke all passed, but a separate visual browser/UI check remains pending
  - This deployment snapshot is superseded by the current VEVO bundle deployment entry above; follow its `Next exact step` for task definition `:17`

- ROY Micro SD 64GB cross-shop identity fix is deployed and fully backfilled (2026-07-15):
  - product identity PR `#216` merged to `main` as `5d090621ed359ae99e968e916a158a41efef5d2c`; ECR build run `29424051875` published digest `sha256:7b9bcfa3418d179008b584f92e63e582d13f6223402b7493f30fd52b3c51f18b`
  - live `/api/operations/roy/live?refresh=1` reproduced `H-69235D5B` and the Czech title `Micro SD CARD 64GB s adaptérem` three times; historical reporting had the same 64GB product split across `23942440833`, `H-1DADF217`, `H-69235D5B`, and `H-791A744A`
  - ROY now canonicalizes the Slovak, Czech, and Hungarian 64GB names to `Micro SD KARTA 64GB s adaptérom` and the stable reporting SKU `MICRO-SD-64GB`
  - the mapping is intentionally name-scoped because historical EAN `23942440833` is reused by 32GB rows; all `42` historical 64GB rows / `53` units / `EUR 667.44` revenue merge, while all `396` 32GB rows remain outside the 64GB identity
  - the canonical SKU has the known `EUR 3.30` purchase cost; `26` hash rows / `31` units move from the `35%` fallback to mapped cost, reducing historical product expense by `EUR 165.16` and increasing product/company profit by the same amount before any later source-data changes
  - canonical reporting keeps source auditability in `raw_product_sku`; the Czech row exports `raw_product_sku=H-69235D5B` while cost, aggregation, inventory, and UI use `product_sku=MICRO-SD-64GB`
  - verification: project JSON and Python compile OK; focused reporting/operations/dashboard/auth/mobile suite `92` tests OK; direct live-inventory regression verifies canonical SKU, `EUR 3.30` unit cost, `EUR 66.00` cost value on `20` units, and 32GB separation; full suite `134` tests OK; reporting QA smoke and `git diff --check` OK
  - runtime hard-gate before code: App Runner instance-id `N/A`, current DNS IPs `3.126.228.15`, `3.120.216.162`, `3.75.104.192`, service `biznisweb-roy-operations-dashboard`, runtime `python live_dashboard_server.py --host 0.0.0.0 --port 8080`, UI path `/production/roy`, API path `/api/operations/roy/live`, status `RUNNING`, health HTTP `200`
  - first deploy run `29424476203` stopped before any ROY runtime change on `ImageAlreadyExistsException`; hardening PR `#217` merged as `2399c7ff6665fa84d26932ab22eb1ef486484bbc` and now preserves exact manifest bytes, verifies tag digests, and accepts a concurrent tag race only after an exact digest post-check; independent review found no blocker, workflow YAML and extracted Bash parse, full suite `134` tests, reporting QA smoke, ECR manifest round-trip, and `git diff --check` pass
  - successful ROY deploy/backfill run `29425508821` used task `2b92a3099287475c91149fc7ac3f97e6`, private IP `172.31.40.168`, task definition `roy-reporting-daily:47`, and exact image `sha256:7b9bcfa3418d179008b584f92e63e582d13f6223402b7493f30fd52b3c51f18b`; the task exited `0` after localhost `LIVE_ARTIFACT_MARKER_OK` reported `294` KPI days, `160` inventory rows, and `19` artifact-time alerts
  - immutable full-history evidence is under `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/20260715T152848Z/`; stable latest was refreshed at `2026-07-15T15:28:50Z`, covers `2025-09-24..2026-07-14`, contains `5,912` item rows / `3,273` orders / `294` days, has `is_partial=false`, `partial_sources=[]`, and `qa_failure_count=0`
  - the backfill merges exactly `42` 64GB rows / `53` units / `EUR 667.44` revenue into `MICRO-SD-64GB`; mapped expense is `EUR 174.90` and product profit `EUR 492.54`, so all-time company profit increased by exactly `EUR 165.16` from `EUR 21,175.89` to `EUR 21,341.05`; daily, weekly, and latest-30-day company profit increased by `EUR 12.42`, `EUR 18.63`, and `EUR 88.46`
  - all `396` historical 32GB rows remain separate (`F_206` or the reused raw EAN) with zero leakage into `MICRO-SD-64GB`; stable payload contains zero occurrences of `H-69235D5B`, `H-1DADF217`, `H-791A744A`, the Czech title, or the Hungarian title, while raw export audit still retains each source identifier
  - App Runner operation `f1940c99e1114e018611af61c7f713e3` finished `SUCCEEDED`; service `biznisweb-roy-operations-dashboard` is `RUNNING` on the same new digest, and post-host workflow smoke passed `/health`, `/production/roy`, authenticated live API, and the picking-list PDF
  - independent live API verification returned HTTP `200`, marker `roy-operations-dashboard`, generated_at `2026-07-15T15:41:15Z`, one visible canonical inventory row, Slovak product name, `39` available units, `EUR 3.30` unit cost, `EUR 128.70` inventory cost, and zero old hash/Czech/Hungarian identity occurrences; the product is `Healthy` with about `69.4` days of cover and low-confidence `Monitor`, not an automatic purchase order
  - VEVO runtime stayed unchanged on `vevo-reporting-daily:14` and digest `sha256:c23651ebd051bd88cecd9f529e70b5f61f4a891aa7d31d166835783b6807c30b`; only its preservation tag was corrected to that already-running digest because both projects share ECR lifecycle protection
  - the in-app browser kernel was locally unavailable after reset (`failed to write kernel assets`, OS error `3`); this did not affect production, and the required UI ordering was still satisfied by the successful host marker followed by live HTML/API/PDF workflow smoke
  - Next exact step: verify the next regular `roy-daily-report-email` run at `01:30 Europe/Bratislava` stays on `roy-reporting-daily:47` and emits `MICRO-SD-64GB` without any visible legacy hash; then optimize the four report windows to reuse one inventory snapshot

- ROY reporting decision-safety remediation is deployed and fully backfilled (2026-07-15):
  - purchase-cost precedence is corrected in the reusable reporting core: a resolved mapped purchase cost now wins over legacy zero-cost, zero-margin, `35%`, and `15%` assumptions
  - the sole mapped-cost exception is an item line genuinely sold for `0 EUR`; it is marked `zero_revenue_gift` and keeps `0 EUR` cost, covering free Lux knife sets / ROY knives without weakening precedence for positively priced rows
  - ROY's `35%` missing-cost margin remains a fallback only when no purchase cost can be resolved, while mapped negative-margin clearance sales remain unchanged
  - inventory output now has a hard purchase-decision gate: exact quantities/dates are hidden and `Order now` / `Prepare PO` flags are disabled until cost coverage, forecast backtest, inbound purchase orders, and negative-stock thresholds all pass; internal estimates remain available only as warning evidence
  - `diagnostika`, `praca`, and `testovanie` service lines are excluded from stock alerts
  - marketing action verdicts require at least `14` active, control, and comparable days plus `high` confidence; otherwise the result is `Experiment required`, never `Scale` or `Cut`
  - tracked paid CAC and cohort payback now use blended Meta + Google spend; customer concentration now exports real CM3 profit shares; customer segmentation uses the realized-revenue marker instead of one corrupted localized status label
  - dashboard fixes: same-item frequencies preserve labels such as `2x`; missing-cost copy reflects the configured `35%` policy; unsafe exact reorder values render as `Blocked`; ROY inventory shows the gate blockers; the `1280px` navigation collapse was removed, mobile navigation is compact and horizontally scrollable, grid children can shrink safely, and wide tables remain contained
  - a ROY-only CEO decision cockpit now leads with company profit, optional prorated profit plan (`not configured` until a real target is supplied), mapped versus estimated fallback product profit, inventory profit at risk, annualized GMROI, inventory cash, and inbound/draft-PO cash context
  - the cockpit explains the latest `30` days versus the prior `30` through revenue, product-cost, ads, fulfillment, fixed-cost, and reconciliation drivers, then emits exactly five confidence-gated actions: reprice, marketing, fill cost, purchase/data repair, and dead-stock clearance
  - browser verification on the stable full-history artifact: at `1280x720` the sidebar remains `240px`, the main cockpit is visible, and the document has no horizontal overflow; a clean initial load at `390x844` uses a compact horizontally scrollable navigation strip, keeps the document within the `375px` content viewport, and renders `2x` rather than `0x` without `€nan`
  - browser review found and fixed one stale data assertion: payback now uses blended Meta + Google CAC, so the parity QA check uses `paid_cac` / `blended_cac` too instead of incorrectly comparing against Facebook-only CAC
  - final local verification: full unit suite `131` tests OK with `1` optional local PDF-text test skipped; focused `test_reporting_calculation_fixes.py` `34` tests OK and `test_dashboard_modern.py` `7` tests OK; `scripts/reporting_qa_smoke.py`, Python compile, ROY settings JSON, browser checks, and `git diff --check` OK
  - code merged through PR `#214` as `c08739d99a223e6faa996083bd84cc8add64d3e6`; ECR build `29416183988` published digest `sha256:02f6e63bda9fec7de6345f0426b8151c66c0e0849e027dcca69fc20afe3f777d`
  - ROY-only runtime now uses `roy-reporting-daily:46`; schedule `roy-daily-report-email` remains enabled at `cron(30 1 * * ? *)` in `Europe/Bratislava`; VEVO remains unchanged on `vevo-reporting-daily:14` and digest `sha256:c23651ebd051bd88cecd9f529e70b5f61f4a891aa7d31d166835783b6807c30b`
  - deploy hard-gate passed in production-smoke run `29416452217`: Fargate task `21a6903af01140d19f0094272b503f0a`, private IP `172.31.46.61`, service/schedule `roy-daily-report-email`, runtime path `/app/daily_report_runner.py --project roy`, `LOCALHOST_MARKER_OK`, `UI_SMOKE_OK:roy:daily-profit-loss`, exit `0`
  - tagged production evidence is under `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/20260715T131616Z/`; it contains all `5,912` item rows and `294` days with `qa_failure_count=0`, `is_partial=false`, and exact daily revenue-minus-cost parity
  - stable full-history backfill task `ddba07af680b4141957f1261490288f2` ran on Fargate IP `172.31.8.47` and task definition `roy-reporting-daily:46`, then stopped at `2026-07-15 16:00 Europe/Bratislava` with exit `0`; host-side evidence includes `LOCALHOST_BACKFILL_MARKER_OK` and `UI_SMOKE_OK:roy:stable-latest-decision-safety`
  - stable artifacts `daily-reports/roy-sk/latest/dashboard_payload_latest.json` and `report_latest.html` were regenerated at `2026-07-15T13:59:38Z`; the payload covers `2025-09-24` through `2026-07-14`, has blank `output_tag`, `qa_failure_count=0`, `is_partial=false`, and `source_overall_status=warning` only because non-critical inventory/data warnings remain
  - corrected company profit is `EUR 21,175.89` versus the old `EUR 25,316.07` (`EUR -4,140.18` full history), while the latest `30` days improved from `EUR 3,397.57` to `EUR 3,965.19` (`EUR +567.62`); this reconciles the expected recent improvement with removal of older paid zero-cost exceptions
  - the stable payload has `358` true zero-revenue gift rows with total gift revenue and product cost both `EUR 0`; the Lux knife set now has `76` gift rows at zero cost and `10` paid rows (`11` units) with mapped cost `EUR 182.38`, leaving `EUR 108.42` paid-row product profit
  - inventory decisions remain deliberately `warning_only` / low confidence because retail cost coverage is `14.76%`, forecast WAPE `65.8%`, median accuracy `34.9%`, forecasts within `20%` only `10.71%`, inbound is not modeled, and `17` products have negative stock; marketing remains `Experiment required` with only `5` control/comparable days
  - Next exact step: reuse one inventory snapshot across the `7d`, `30d`, `90d`, and full-history report builds so a full ROY backfill does not fetch the same inventory pages four times; preserve the current decision gates and runtime markers while optimizing

- ROY missing-purchase-cost fallback is permanently deployed as a `35%` product margin and full history was regenerated on `2026-07-15`:
  - code PR `#210` merged as `e178af8`; `projects/roy/settings.json` sets `missing_cost_margin_pct = 35`, so only products without any resolvable purchase cost use expense `65%` of net item revenue and source `missing_cost_margin_35_fallback`
  - mapped purchase costs keep precedence, including real negative-margin clearance sales; unit coverage preserves a mapped `80 EUR` cost on `50 EUR` net revenue as `-30 EUR` product profit
  - VEVO calculation remains unchanged at the default `0%` missing-cost margin; the final runtime deploy and smoke are ROY-only
  - QA metadata follow-up PR `#211` merged as `923f7e9` and now describes the configured `35%` margin / `65%` expense instead of the obsolete zero-margin wording
  - local verification: reporting calculation suite `28` tests OK, full unit suite `122` tests OK, `scripts/reporting_qa_smoke.py` OK, Python compile OK, workflow YAML parse and extracted Bash syntax OK, `git diff --check` OK
  - initial production image build `29389383467` published digest `sha256:c23651ebd051bd88cecd9f529e70b5f61f4a891aa7d31d166835783b6807c30b`; production smoke `29389478844` passed for ROY on task `c8d4d19092a74de79c0b74fac789ab32`, private IP `172.31.18.68`, task definition `roy-reporting-daily:44`, `LOCALHOST_MARKER_OK`, then `UI_SMOKE_OK:roy:daily-profit-loss`
  - stable full-history backfill hard-gate: instance-id `N/A (AWS ECS/Fargate)`, service `roy-daily-report-email`, task `3bc656a3a8d84bdbbb920178a047bbf9`, private IP `172.31.1.160`, task definition `roy-reporting-daily:44`, marker path `http://127.0.0.1:8000/marker.json`, exit code `0`
  - backfill period `2025-09-24..2026-07-14` published immutable prefix `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/20260715T062500Z/` and refreshed `latest/dashboard_payload_latest.json` plus `latest/report_latest.html` at `2026-07-15T06:25:02Z`
  - backfill validation: `LOCALHOST_BACKFILL_MARKER_OK` was curled directly on the host before `UI_SMOKE_OK:roy:stable-latest-backfill`; all sources are healthy, `is_partial=false`, `partial_sources=[]`, and QA has `0` failures
  - full-history fallback result: `59` rows, net revenue `1272.80 EUR`, expense `827.26 EUR`, product profit `445.51 EUR`, margin `35.00%`, max rounding delta `0.005 EUR`, and `0` legacy `missing_cost_zero_margin_fallback` rows
  - order `2677003496`: main XTAR battery row changed from expense/profit `45.53/0.00 EUR` to `29.59/15.94 EUR`; with `0.16 EUR` zero-cost tip, order profit before ads is `16.10 EUR` on `45.69 EUR` net revenue
  - intentional losses remain real: `266` mapped negative-margin rows remain; ROY powerbank SKU `IS-Q6L` has `8` mapped loss rows and was not replaced by the fallback
  - day `2026-07-14` changed from expense/profit-before-ads `581.69/340.17 EUR` to `508.65/413.21 EUR` on unchanged `921.86 EUR` net revenue
  - final QA image build `29394519821` published digest `sha256:3a944b61c98ab8f60fc8d0118c285fec80a0e6aa047effb8eb3d7aed24ad5115`; ROY schedule now targets `roy-reporting-daily:45` on this exact protected digest, while the VEVO schedule/digest was not changed
  - final ROY-only production smoke run `29394825661` passed on task `477bf62f133a452a939c044bc503aca0`, private IP `172.31.34.205`, task definition `roy-reporting-daily:45`, `LOCALHOST_MARKER_OK`, then `UI_SMOKE_OK:roy:daily-profit-loss`, and `PRODUCTION_SMOKE_OK:roy`; `send_email=false`
  - final tagged full-history artifact prefix `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/20260715T071450Z/` confirms corrected QA policy `configured 35% margin estimate (65% of net item revenue is treated as expense)`, `59` fallback rows, `1272.80 EUR` fallback revenue, `445.51 EUR` fallback profit, `is_partial=false`, and `0` QA failures
  - live App Runner service `biznisweb-roy-operations-dashboard` is `RUNNING`; authenticated `/health`, `/production/roy`, and `/api/operations/roy/live` returned HTTP `200`, HTML marker `roy-operations-dashboard`, and data generated at `2026-07-15T06:26:16Z`
  - ECR protection follow-up PR `#212` merged as `e644597`; the workflow preserves exact manifest bytes and verifies the protection tag digest after `put-image`, preventing a formatting-induced digest mismatch
  - Current status: ROY calculation is deployed, full history and stable latest are regenerated, future ROY schedule is pinned to the final image, and VEVO reporting data/configuration stayed unchanged
  - Next exact step: monitor the next regular `roy-daily-report-email` run for the same `35%` fallback source and SES delivery

- VEVO daily reporting email outage on `2026-07-09` is fixed and ECR digest protection is being hardened:
  - symptom: the regular `vevo-daily-report-email` run for `2026-07-09 01:00 Europe/Bratislava` did not send a VEVO reporting email; ROY sent normally at `2026-07-09 01:30 Europe/Bratislava`
  - root cause: `vevo-daily-report-email` still targeted `vevo-reporting-daily:12` with image digest `sha256:ebd43d8904940e03bdcc1253a749119eb943d80565b387611b2a23d73e6d28a9`; the digest had become untagged after the `2026-07-07` ROY App Runner deploy pushed `latest` to `sha256:c7aa0845f40a773da717c5ddf076de0e7413217a6d8d3ccb9116ca97b866dede`, then ECR lifecycle deleted the untagged VEVO digest
  - evidence: ECR `batch-get-image` for `sha256:ebd43d8904940e03bdcc1253a749119eb943d80565b387611b2a23d73e6d28a9` returned `ImageNotFound`; CloudTrail still showed the scheduled VEVO `RunTask` at `2026-07-08T23:00:13Z` for task `adfca2c7aed64091ab3fe1a342503817`, but `/ecs/vevo-reporting-daily` had no application log stream for that run; ROY task `fa90233638d04cd1931bd03c29824417` logged SES `MessageId=0107019f442e998f-8c6f3367-f63f-4db9-a941-2616b6e0fed5-000000`
  - drift source: CloudTrail showed `Deploy Live Dashboard App Runner` run `28850346056` registered `roy-reporting-daily:43` and updated only `roy-daily-report-email` at `2026-07-07T07:49Z`, bypassing the earlier `production-reporting-smoke` guard that only protected that workflow
  - repair run: `Production Reporting Smoke` run `29000488380` with `project=all`, `send_email=false`, `update_task_image=true` updated VEVO from `vevo-reporting-daily:12` to `vevo-reporting-daily:13` on `sha256:c7aa0845f40a773da717c5ddf076de0e7413217a6d8d3ccb9116ca97b866dede`
  - repair hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.1.133`, service `vevo-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:13`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/b44ca918db124255be1fbb6891a31662`, marker path `http://127.0.0.1:8000/marker.json`
  - repair verification: VEVO dry host smoke in run `29000488380` showed `LOCALHOST_MARKER_OK`, `UI_SMOKE_OK:vevo:production-board`, `UI_SMOKE_OK:vevo:daily-profit-loss`; ROY dry smoke also passed with task `a752e29e34324e98b943bf92e91a365d`
  - email resend: `Production Reporting Smoke` run `29008069794` with `project=vevo`, `send_email=true`, `update_task_image=false` sent the missing VEVO report via SES `MessageId=0107019f4645cbeb-c78b87bb-a6c3-49fa-83fa-bc1768a9b9d6-000000`
  - resend hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.15.232`, service `vevo-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:13`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/2b322c0fec82493b980a806472573746`, marker path `http://127.0.0.1:8000/marker.json`
  - resend verification: CloudWatch showed `data/vevo/report_latest.html`, `data/vevo/dashboard_payload_latest.json`, `LOCALHOST_MARKER_OK`, `daily_profit_rows=432`, `creditnote_count=280`, `credited_gross_eur=4938.74`, `send_email=true`, `PRODUCTION_BOARD_OK`, `UI_SMOKE_OK:vevo:production-board`, and `UI_SMOKE_OK:vevo:daily-profit-loss`
  - immediate ECR protection: digest `sha256:c7aa0845f40a773da717c5ddf076de0e7413217a6d8d3ccb9116ca97b866dede` now has tags `latest`, `vevo-reporting-daily-current`, and `roy-reporting-daily-current`, so it is protected from the current untagged-image lifecycle rule
  - code hardening branch: `codex/protect-reporting-schedule-images` updates `.github/workflows/production-reporting-smoke.yml` and `.github/workflows/deploy-live-dashboard-apprunner.yml` so daily schedule digests are protected with project-specific ECR tags whenever report smoke runs or App Runner deploy touches reporting artifacts
  - local verification for hardening branch:
    - YAML parse for both workflows returned `YAML_OK`
    - extracted workflow bash blocks passed `bash -n` after LF normalization
    - `git diff --check`
  - Current status: VEVO scheduled daily reporting is enabled at `01:00 Europe/Bratislava`, targets `vevo-reporting-daily:13`, and uses the current protected digest `sha256:c7aa0845f40a773da717c5ddf076de0e7413217a6d8d3ccb9116ca97b866dede`
  - Next exact step: merge the hardening PR, then monitor the next regular `vevo-daily-report-email` run on `2026-07-10 01:00 Europe/Bratislava` for a new CloudWatch stream and SES `MessageId`

- ROY picking-list print confirmation fix is merged, deployed, and live state is repaired on `2026-07-07`:
  - code PR: `https://github.com/vzeman/biznisweb/pull/207`, merged as `ff1fa6e Fix ROY picking PDF print confirmation flow`
  - deploy run: `https://github.com/vzeman/biznisweb/actions/runs/28850346056`
  - App Runner hard-gate context: service `biznisweb-roy-operations-dashboard`, service ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, instance-id/IP `N/A` because AWS App Runner is managed, production path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`, health path `/health`
  - deployed image: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:c7aa0845f40a773da717c5ddf076de0e7413217a6d8d3ccb9116ca97b866dede`
  - App Runner operation: `8ab498381b454bd181ecc345dd306f44` completed, workflow smoke ended with `APP_RUNNER_ROY_OPERATIONS_OK` and `APP_RUNNER_DEPLOY_OK`
  - live symptom: ROY live dashboard still showed active orders from `2026-07-06`, but the normal picking-list PDF flow skipped them while newer `2026-07-07` orders could still be printed
  - live root cause evidence: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/operations/state.json` already contained active `2026-07-06` orders in `printed_picking_orders`, including batch `picking-20260707062639`; therefore the normal PDF endpoint filtered those orders out
  - code root cause: `GET /api/operations/roy/picking-lists.pdf` generated the PDF and immediately called `mark_picking_orders_printed()` before the browser/user could prove that the download or physical print succeeded
  - change: picking-list PDF GET is now read-only; it defaults to currently unprinted active orders and can still render all active orders for preview with `preview=1`/`include_printed=1`
  - change: a separate explicit `POST /api/operations/roy/picking-lists/printed` marks only the submitted order numbers as printed
  - change: `/api/operations/roy/live` annotates active orders with `picking_printed`, `picking_printed_at`, and print summary counts, and the ROY dashboard shows the print state plus separate `Vysklad. PDF` and `Označiť vytlačené` controls
  - remote state backup before repair: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/operations/backups/state-20260707T080233Z-before-picking-print-repair.json`
  - remote state repair: removed erroneous active printed flags for `2677003373`, `2678000179`, `2677003374`, `2677003375`, `2677003376`, `2677003377`; adjusted batch `picking-20260707062639` from `10` to `4` remaining historical order numbers
  - live verification after repair:
    - `/health` returned `{"ok": true, "projects": ["roy", "vevo"]}`
    - `/api/operations/roy/live?refresh=1` at `2026-07-07T08:08:51Z` returned marker `roy-operations-dashboard`, `picking_printed_orders=0`, `picking_unprinted_orders=5`, active order nums `2677003373`, `2678000179`, `2677003374`, `2677003376`, `2677003384`
    - default dashboard PDF endpoint `/api/operations/roy/picking-lists.pdf?refresh=1` returned HTTP `200`, `application/pdf`, filename `roy-vyskladnovacie-listy-5-20260707-0810.pdf`, size `120794`, PDF contained active order nums `2677003373`, `2678000179`, `2677003374`, `2677003376`, `2677003384`
    - S3 state ETag stayed unchanged across PDF GET: `"ae7b3f9484125809d44c431d7a00dcc4"` before and after, proving GET is read-only in production
    - `/production/roy` HTML contains marker `roy-operations-dashboard`, link `pickingPdfLink`, button `markPickingPrintedBtn`, and the `Tlač` column
  - local verification:
    - `python -m py_compile live_dashboard_server.py roy_operations_dashboard.py`
    - `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_picking_lists_pdf tests.test_live_dashboard_auth tests.test_live_dashboard_mobile` (`37` tests OK)
  - Next exact step: warehouse can download/print the current `Vysklad. PDF`; only after the physical print/download succeeds, click `Označiť vytlačené` so the explicit POST records the printed batch

- ROY recent zero-margin product SKU override is deployed and stable latest artifacts were regenerated on `2026-07-01`:
  - source audit: ROY stable export `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/20260701T083224Z/export_20250924-20260630.csv` showed `70` product labels / `66` unique SKUs with `missing_cost_zero_margin_fallback` in `2026-04-01..2026-06-30`
  - code/config change: PR `#205` merged as `86e348b`; reporting runtime supports `margin_override_skus`, and `projects/roy/settings.json` sets `35%` product margin for those `66` SKUs
  - runtime effect: matching SKU rows use cost `65%` of net `item_total_without_tax` and source `margin_35_override`; explicit zero-cost and zero-margin exceptions still take precedence
  - local verification before merge:
    - `python -m json.tool projects\roy\settings.json`
    - `python -m py_compile export_orders.py reporting_core\runtime.py tests\test_reporting_calculation_fixes.py`
    - `python -m unittest tests.test_reporting_calculation_fixes` (`25` tests OK)
    - `python -m unittest tests.test_reporting_calculation_fixes tests.test_roy_inventory_model tests.test_dashboard_modern` (`41` tests OK)
    - `python scripts\reporting_qa_smoke.py`
    - `git diff --check`
  - production image: ECR `vevo-reporting:latest` digest `sha256:ebd43d8904940e03bdcc1253a749119eb943d80565b387611b2a23d73e6d28a9` from build run `28509060634`
  - production smoke: GitHub run `28509192170` completed `success`; schedules were updated to `vevo-reporting-daily:12` and `roy-reporting-daily:42`
  - ROY smoke task: `40363c1f7812425eb98dda558e673d01`, private IP `172.31.18.96`, `roy-reporting-daily:42`, exit code `0`, marker `LOCALHOST_MARKER_OK`, UI marker `UI_SMOKE_OK:roy:daily-profit-loss`
  - stable ROY backfill: task `e86d17adb1f545d3b29a754dafe73e13`, private IP `172.31.36.213`, `roy-reporting-daily:42`, exit code `0`, command `daily_report_runner.py --project roy --skip-email --skip-invoices --creditnote-storno-dry-run`
  - backfill marker for `2026-04-01..2026-06-30`:
    - `sku_override_count=66`
    - `target_sku_rows=421`
    - `zero_margin_fallback_rows_after=0`
    - `sku_margin_35_rows=413`
    - `sku_margin_35_revenue_eur_net=4023.20`
    - `sku_margin_35_expense_eur=2615.24`
    - `sku_margin_35_profit_eur=1407.96`
    - `sku_margin_35_margin_pct=34.996`
    - `source_counts={"margin_35_override":413,"zero_cost_override":8}`
  - stable S3 latest refreshed at `2026-07-01T12:01:21Z`:
    - `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
    - `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/report_latest.html`
  - live ROY App Runner check: `biznisweb-roy-operations-dashboard` was `RUNNING`; `/production/roy` contained `roy-operations-dashboard`, `/api/operations/roy/live?refresh=1` returned marker `roy-operations-dashboard` with `generated_at=2026-07-01T12:04:36Z`
  - Current status: deployed, stable ROY latest report/dashboard regenerated with 35% SKU margin on net prices, and public ROY operations dashboard reads the refreshed payload
  - Next exact step: monitor the next scheduled `roy-daily-report-email` run on `roy-reporting-daily:42` and re-audit any new future `missing_cost_zero_margin_fallback` rows separately

- ROY knife-brand VO margin override is deployed and stable latest artifacts were regenerated on `2026-07-01`:
  - code PR: `https://github.com/vzeman/biznisweb/pull/203`, merged to `main` as `3df5f41b2a3909ea43ff8d41a45188dcad0fd9af`
  - change: reporting runtime supports generic `margin_override_brands` and `margin_override_label_patterns` percentage maps, applied after explicit zero-cost/zero-margin exceptions and before SKU cost maps or missing-cost zero-margin fallback
  - change: `projects/roy/settings.json` sets `35%` product margin for these knife brands: Opinel, Morakniv, Walther, Kizlyar, Higonokami, Ganzo, Ruike, Helle, Cold Steel, Civivi, Victorinox, Bestech, Mikov, Boker, Joker, Kanetsune, Muela, Marttiini, Benchmade, Spyderco
  - runtime effect: matching product labels use cost `65%` of net line price and source `margin_35_override`; explicit exceptions still win, so the existing zero-cost `Walther 2x20...` exception remains zero-cost
  - local verification before merge:
    - `python -m json.tool projects\roy\settings.json`
    - `python -m py_compile export_orders.py reporting_core\runtime.py tests\test_reporting_calculation_fixes.py`
    - `python -m unittest tests.test_reporting_calculation_fixes` (`23` tests OK)
    - `python -m unittest tests.test_reporting_calculation_fixes tests.test_roy_inventory_model tests.test_dashboard_modern` (`39` tests OK)
    - `python scripts\reporting_qa_smoke.py`
    - `git diff --check`
  - image/deploy: ECR build run `28498303881` published `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:65f10b1f6646f56a0cbb2c36f7fd72bd1b54fa041d8046529c98809b28be9248`
  - scheduled task definitions after production smoke run `28498402842`: `roy-daily-report-email` -> `roy-reporting-daily:41`; `vevo-daily-report-email` -> `vevo-reporting-daily:11`
  - hard-gate smoke evidence for ROY: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.43.101`, task `d50ca24ca0694ce8874231cadb0c070d`, marker path `http://127.0.0.1:8000/marker.json`, UI path `http://127.0.0.1:8787/dashboard/roy`, `PRODUCTION_SMOKE_OK:roy:d50ca24ca0694ce8874231cadb0c070d:172.31.43.101`, `UI_SMOKE_OK:roy:daily-profit-loss`
  - stable latest backfill: one-off ROY ECS task `914eac51fd7448bf99050344e4512887` on `roy-reporting-daily:41`, private IP `172.31.11.202`, `--skip-email --skip-invoices --creditnote-storno-dry-run`, exit code `0`
  - backfill marker: `LOCALHOST_BACKFILL_MARKER_OK`, `daily_profit_rows=280`, `creditnote_count=115`, `credited_gross_eur=11520.89`, `margin_35_rows=16`, `margin_35_revenue_eur=419.76`, `margin_35_expense_eur=272.87`, `margin_35_profit_eur=146.89`, `margin_35_margin_pct=34.9938`, `margin_override_brands_configured=20`
  - backfill UI/S3 verification: `UI_SMOKE_OK:roy:stable-latest-backfill`; `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json` and `report_latest.html` were last modified at `2026-07-01T08:32:25Z`
  - Current status: production code, scheduled runtime image, ROY stable latest dashboard payload, and ROY stable latest HTML report are updated
  - Next exact step: after the next scheduled run on `2026-07-02 01:30 Europe/Bratislava`, confirm `roy-reporting-daily:41` still emits `margin_35_override` rows and sends the normal daily email

- VEVO daily reporting email outage from `2026-06-27` is fixed on `2026-06-29`:
  - symptom: VEVO daily emails stopped after the last successful scheduled runs on `2026-06-25 01:00 Europe/Bratislava` and `2026-06-26 01:00 Europe/Bratislava`; ROY continued sending daily SES emails on `2026-06-27`, `2026-06-28`, and `2026-06-29`
  - root cause: EventBridge Scheduler still invoked `vevo-daily-report-email`, but its target `vevo-reporting-daily:9` was pinned to ECR digest `sha256:f6b1d59a73dc3db38f9efae07f25ebca92946793b9f0df1b7807ac623b4893c1`; ECR lifecycle policy expires untagged images older than `7` days, and that digest returned `ImageNotFound`
  - evidence: `/ecs/vevo-reporting-daily` had no application log streams for the scheduled tasks started at `2026-06-26T23:00:13Z`, `2026-06-27T23:00:13Z`, and `2026-06-28T23:00:13Z`; CloudTrail still recorded the daily `RunTask` calls for task IDs `3e04d8ec31d94914a0513369225f2dbb`, `7338f080a0b4459cb78121890ac60f98`, and `381f92fcc2444a358d8a53f0a4fd1db0`
  - restore action: manual `Production Reporting Smoke` run `28345745563` updated `vevo-daily-report-email` from `vevo-reporting-daily:9` to `vevo-reporting-daily:10`, using current ECR digest `sha256:3c6ac1f3b30a2746cf4a6f4bb72678a5950034cf686f6b678a5371c57d1f0749`
  - hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.17.178`, service `vevo-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:10`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/6590fc1b98dc4c0bb6853d14a130bc84`, marker path `http://127.0.0.1:8000/marker.json`, UI path `http://127.0.0.1:8787/dashboard/vevo`
  - verification: CloudWatch stream `ecs/reporting/6590fc1b98dc4c0bb6853d14a130bc84` saved `data/vevo/report_latest.html` and `data/vevo/dashboard_payload_latest.json`, sent SES `MessageId=0107019f11645817-e1bb38cc-272c-40ef-905b-b1d9a2c52b34-000000`, showed `LOCALHOST_MARKER_OK`, `daily_profit_rows=422`, `creditnote_count=274`, `credited_gross_eur=4854.34`, `send_email=true`, `UI_SMOKE_OK:vevo:production-board`, and `UI_SMOKE_OK:vevo:daily-profit-loss`
  - recurrence guard: `.github/workflows/production-reporting-smoke.yml` now fails when `update_task_image=true` is dispatched for anything other than `project=all`, so future image refreshes update and smoke both daily schedules instead of leaving one project pinned to an older digest
  - Current status: VEVO scheduled daily reporting is enabled at `01:00 Europe/Bratislava` and now targets `vevo-reporting-daily:10`; the immediate recovery email was sent by SES
  - Next exact step: monitor the next regular `vevo-daily-report-email` scheduled run on `2026-06-30 01:00 Europe/Bratislava` and confirm a new SES message plus CloudWatch stream

- ROY fixed monthly expenses are raised to `6500 EUR/month` in source-of-truth on `2026-06-24`:
  - branch/worktree: `codex/roy-fixed-monthly-6500` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - change: `projects/roy/settings.json` now sets `fixed_monthly_cost` to `6500`
  - change: `scripts/reporting_qa_smoke.py` now asserts ROY runtime loads `fixed_monthly_cost = 6500.0`
  - expected runtime effect after production image refresh: ROY fixed overhead spreads as `6500 / days_in_month`, e.g. June daily fixed allocation becomes `216.67 EUR/day` before CSV rounding
  - code PR `#198` merged to `main` as `dae2a8c248f938753aa9a7fe07f1d301b589c4be`
  - local verification:
    - `python -m json.tool projects\roy\settings.json`
    - `python -m py_compile export_orders.py scripts\reporting_qa_smoke.py reporting_core\runtime.py`
    - `python scripts\reporting_qa_smoke.py`
    - `python -m unittest tests.test_reporting_calculation_fixes tests.test_dashboard_modern` (`26` tests OK)
    - `git diff --check`
  - ECR refresh: run `28096120599` succeeded for `vevo-reporting:latest`, digest `sha256:3c6ac1f3b30a2746cf4a6f4bb72678a5950034cf686f6b678a5371c57d1f0749`
  - ROY production reporting smoke: run `28096233324` succeeded with `project=roy`, marker `roy-fixed-6500-20260624`, `send_email=false`, `update_task_image=true`
  - ROY hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.28.73`, service `roy-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:40`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/45556687af884eb1b81a75e33cf551fe`, marker path `http://127.0.0.1:8000/marker.json`
  - ROY verification: scheduled task `roy-daily-report-email` now targets `roy-reporting-daily:40` with image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:3c6ac1f3b30a2746cf4a6f4bb72678a5950034cf686f6b678a5371c57d1f0749`; host log showed a `216.67` EUR daily fixed allocation, `LOCALHOST_MARKER_OK`, `daily_profit_rows=273`, `creditnote_count=114`, `credited_gross_eur=11462.82`, `send_email=false`, report path `data/roy/report_latest__roy-roy-fixed-6500-20260624.html`, payload path `data/roy/dashboard_payload_latest__roy-roy-fixed-6500-20260624.json`, and UI smoke `UI_SMOKE_OK:roy:daily-profit-loss`
  - ROY full-history stable-latest backfill: one-off ECS task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/a9ba9aeb13334b3d9b1f6e1348411b62` ran on `roy-reporting-daily:40` with private IP `172.31.9.254`, command `daily_report_runner.py --project roy --skip-email --skip-invoices --creditnote-storno-dry-run`, and exit code `0`
  - backfill host verification: `LOCALHOST_BACKFILL_MARKER_OK`, `date_from=2025-09-24`, `date_to=2026-06-23`, `generated_at=2026-06-24T13:15:12Z`, `last_fixed_daily_cost=216.67`, `daily_profit_rows=273`, `creditnote_count=114`, `credited_gross_eur=11462.82`, report path `data/roy/report_latest.html`, payload path `data/roy/dashboard_payload_latest.json`, and UI smoke `UI_SMOKE_OK:roy:stable-latest-backfill`
  - S3 latest verification: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/report_latest.html` and `.../dashboard_payload_latest.json` were updated at `2026-06-24T13:15:14Z`; S3 payload now reports `last_label=2026-06-23`, `last_fixed=216.67`, `daily_rows=273`, `creditnotes=114`
  - Current status: ROY scheduled daily reporting and stable S3 latest artifacts now use `6500 EUR/month` fixed expenses across the full `2025-09-24..2026-06-23` backfilled reporting range; no real email was sent during smoke or backfill
  - Next exact step: monitor the next regular ROY scheduled daily report for the updated fixed overhead; no known code or deploy blocker remains

- VEVO live dashboard App Runner outage is fixed and deployed on `2026-06-23`:
  - symptom verified before code: VEVO App Runner public URL returned HTTP `404` for `/health`, `/production/vevo`, and `/api/production/vevo/live?refresh=1`, while ROY `/health` returned `200`
  - VEVO hard-gate before code: instance-id `N/A (AWS App Runner managed service)`, private IP `N/A (AWS App Runner managed service)`, service `biznisweb-vevo-production-board`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-vevo-production-board/8c8a7a5d694b401baeccf0f1af19ca50`, health path `https://zxtma5mxta.eu-central-1.awsapprunner.com/health`, production path `https://zxtma5mxta.eu-central-1.awsapprunner.com/production/vevo`, live API path `https://zxtma5mxta.eu-central-1.awsapprunner.com/api/production/vevo/live?refresh=1`, image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:58df20cab335f7376331103676737c04acc17d23a1c43a5aa8c2aad719257bb1`, runtime auth user `marek`
  - ROY comparison hard-gate: instance-id `N/A (AWS App Runner managed service)`, private IP `N/A (AWS App Runner managed service)`, service `biznisweb-roy-operations-dashboard`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, health path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`, production path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - root cause: both App Runner services used the shared instance role `BiznisWebLiveDashboardAppRunnerInstanceRole`; the latest ROY deploy rewrote the inline `LiveDashboardRuntimeSecrets` policy for ROY-only runtime resources, so VEVO instances failed startup with `AccessDeniedException` on `arn:aws:secretsmanager:eu-central-1:919341186960:secret:vevo/reporting/runtime-env-ygoPma`
  - change: `.github/workflows/deploy-live-dashboard-apprunner.yml` now uses project-scoped instance roles `BiznisWebLiveDashboardAppRunnerInstanceRole-${PROJECT}` so VEVO and ROY deployments cannot overwrite each other's runtime secret policy
  - follow-up change: the App Runner deploy workflow now waits for the specific App Runner `OperationId` to reach `SUCCEEDED` before running public curl smoke tests; this prevents a rollback from being hidden by curl checks against an older still-running revision
  - change: `projects/vevo/settings.json` now stores the current VEVO live dashboard auth user `marek`, so workflow input defaults cannot silently switch the user-facing Basic Auth username
  - change: the VEVO production board frontend now uses the same sanitized same-origin `fetchApi()` helper as the ROY operations dashboard, preventing Basic Auth credentialed URL fetch failures in browser JS
  - runtime finding after PR `#195`: VEVO App Runner deploy attempts `28002478387` and `28003183626` both rolled back while the workflow could still report a public smoke success against the recovered old revision; shared role hotfix restored direct old-revision API availability (`/health`, `/production/vevo`, and `/api/production/vevo/live?refresh=1` returned `200`, `active_orders=42`, `manufacturing_products=27`, `orders_scanned=300`), but the served HTML still used the old relative `fetch(url)` code
  - local verification:
    - `python -m json.tool projects\vevo\settings.json`
    - `python -m py_compile live_dashboard_server.py`
    - `python -m unittest tests.test_live_dashboard_mobile tests.test_live_dashboard_auth tests.test_production_board` (`8` tests OK)
    - `python -c "from pathlib import Path; import yaml; yaml.safe_load(Path('.github/workflows/deploy-live-dashboard-apprunner.yml').read_text(encoding='utf-8')); print('YAML_OK')"`
    - `git diff --check`
  - code PRs: PR `#195` merged App Runner role/auth/frontend fix as `eb914d9b4bf11e24d3d45cb204c78aad071b4c60`; PR `#196` merged App Runner operation-status hardening as `8f039685db754ffe89eb618ebaf9da0cbf6e5faf`
  - ECR refresh: build run `28002400151` succeeded for `vevo-reporting:latest`, digest `sha256:e68f779029649981787b12c2508b8dbbb78c5e8d0f9224863c87cd06eae3d8af`
  - stale App Runner service handling: old service ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-vevo-production-board/8c8a7a5d694b401baeccf0f1af19ca50` had no custom domains, was stuck on deleted image `sha256:58df20cab335f7376331103676737c04acc17d23a1c43a5aa8c2aad719257bb1`, and was deleted with operation `1e8fd9c8fa9f404b825ea91cfdc9f9f6`
  - final App Runner deploy: workflow run `28003667053` recreated service `biznisweb-vevo-production-board`; App Runner operation `f04d182e2d8a49b2b55bb5ce24d46d21` reached `SUCCEEDED`; workflow smoke returned `APP_RUNNER_PRODUCTION_BOARD_OK:active_orders=42:manufacturing_products=27:units_to_make=79.0:orders_scanned=300` and `APP_RUNNER_DEPLOY_OK:biznisweb-vevo-production-board:https://2mhmsmgq3m.eu-central-1.awsapprunner.com`
  - current VEVO hard-gate: instance-id `N/A (AWS App Runner managed service)`, private IP `N/A (AWS App Runner managed service)`, service `biznisweb-vevo-production-board`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-vevo-production-board/2711a253ae014a8aaf1a37929997496d`, role `arn:aws:iam::919341186960:role/BiznisWebLiveDashboardAppRunnerInstanceRole-vevo`, health path `https://2mhmsmgq3m.eu-central-1.awsapprunner.com/health`, production path `https://2mhmsmgq3m.eu-central-1.awsapprunner.com/production/vevo`, live API path `https://2mhmsmgq3m.eu-central-1.awsapprunner.com/api/production/vevo/live?refresh=1`, image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:e68f779029649981787b12c2508b8dbbb78c5e8d0f9224863c87cd06eae3d8af`
  - direct host verification: `/health` returned `200`; `/production/vevo` returned `200` with `vevo-production-board`, `fetchApi(path)` present, and old `await fetch(url, ...)` absent; `/api/production/vevo/live?refresh=1` returned `200`, `project=vevo`, `active_orders=42`, `manufacturing_products=27`, `units_to_make=79.0`, `orders_scanned=300`, `cache.status=refreshed`
  - UI verification: Playwright Chromium smoke through the credentialed URL passed (`1` test, `15.3s`): title `VEVO Production Board`, marker `1`, metrics `6`, product cards/rows `27`, orders `42`, `hasFetchApi=true`, `hasOldRelativeFetch=false`, `bodyHasFailedFetch=false`, empty error box, no severe console issues; screenshot `C:\Users\Patrik jankech\AppData\Local\Temp\vevo-ui-smoke.png`
  - Current status: VEVO live production board is back online at `https://2mhmsmgq3m.eu-central-1.awsapprunner.com/production/vevo` using Basic Auth user `marek`; ROY App Runner remains separate on `biznisweb-roy-operations-dashboard`
  - Next exact step: use the new VEVO App Runner URL above for the live dashboard and monitor the next regular refresh; no known code or deploy blocker remains

- ROY inventory cost value history is implemented and deployed on `2026-06-18`:
  - branch/worktree: `codex/roy-inventory-cost-value` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - context: ROY already calculated current `inventory_cost_value` in the inventory snapshot and showed it as a dashboard KPI, but the daily reporting payload did not persist a historical series for a trend chart
  - change: ROY product demand analytics now builds `inventory_cost_history_rows` from the current inventory snapshot and merges it with the previous stable `dashboard_payload_latest.json` from local output/S3, deduplicated by snapshot date and capped to `730` points
  - change: the modern dashboard payload now includes `roy_product_demand.inventory_cost_history_rows`
  - change: the ROY inventory section now renders an `Inventory cost value trend` chart with inventory cost value, retail value, and dead-stock cost value in time
  - code PR `#193` merged to `main` as `11157aecffa65e6dc302bcd740ed193d294c2b8f`
  - local verification:
    - `python -m py_compile export_orders.py dashboard_modern.py daily_report_runner.py`
    - `python -m unittest tests.test_roy_inventory_model tests.test_dashboard_modern`
    - `python -m unittest tests.test_roy_inventory_model tests.test_dashboard_modern tests.test_roy_operations_dashboard tests.test_reporting_calculation_fixes tests.test_creditnote_export tests.test_creditnote_storno_guard` (`81` tests OK)
    - `git diff --check`
  - ECR refresh: run `27758206041` succeeded for `vevo-reporting:latest`, digest `sha256:3e97b5034433b09c3f4a4b9b9f5b3a0a3a1ad9c475fffcf799add5c4668032be`
  - production ROY reporting smoke: run `27758328013` succeeded with `project=roy`, marker `roy-inventory-cost-value-20260618`, `send_email=true`, `update_task_image=true`
  - ROY hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.39.116`, service `roy-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:39`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/d72920a7a2aa4fb69799903574a02a13`, marker path `http://127.0.0.1:8000/marker.json`
  - ROY verification: task image updated from digest `sha256:f6b1d59a73dc3db38f9efae07f25ebca92946793b9f0df1b7807ac623b4893c1` to `sha256:3e97b5034433b09c3f4a4b9b9f5b3a0a3a1ad9c475fffcf799add5c4668032be`, `task-image-updated=true`, SES `MessageId=0107019edad1c5b8-543e9286-435e-41e9-8d23-07f3bd0ff385-000000`, `LOCALHOST_MARKER_OK`, `send_email=true`, `daily_profit_rows=267`, `creditnote_count=110`, `credited_gross_eur=11063.52`, report path `data/roy/report_latest.html`, payload path `data/roy/dashboard_payload_latest.json`, UI smoke `UI_SMOKE_OK:roy:daily-profit-loss`
  - Note: direct local S3 artifact read from this Windows PC was not possible because local AWS credentials were not configured; production task generated the stable report/payload and local tests explicitly assert the new `royInventoryCostValueChart` and `inventory_cost_history_rows` payload
  - Current status: ROY scheduled daily email now points to the image containing the inventory cost value history and chart; the series starts from the first available stable payload/current snapshot and grows with each daily run
  - Next exact step: monitor the next regular ROY morning email; no known code/deploy blocker remains for this requested metric

- Creditnote carrier rate from creditnote documents is implemented and deployed on `2026-06-18`:
  - branch/worktree: `codex/creditnote-rate-from-creditnotes` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - context: the daily carrier table showed carriers with `Creditnotes > 0` but `Rate = 0.00%` because the rate used sent-creditnoted order count as the numerator
  - change: monthly creditnote carrier audit now calculates `Dobropis rate %` as `Dobropisy / Odoslane objednavky` while keeping `Dobropisovane objednavky` as a separate sent-order audit column
  - change: daily dashboard creditnote metrics now calculate overall and per-carrier `creditnote_rate_pct`, rate index, outlier gating, and carrier sorting from creditnote document count instead of credited sent order count
  - code PR `#191` merged to `main` as `9fd307c9a1bdeb7abe3cb2a5b8d71eab682f1b08`
  - local verification:
    - `python -m py_compile export_orders.py creditnote_export.py dashboard_modern.py daily_report_runner.py`
    - `python -m unittest tests.test_creditnote_export tests.test_reporting_calculation_fixes tests.test_dashboard_modern` (`40` tests OK)
    - `python -m unittest tests.test_creditnote_export tests.test_creditnote_storno_guard tests.test_reporting_calculation_fixes tests.test_dashboard_modern tests.test_invoice_generation` (`55` tests OK)
    - `git diff --check`
  - ECR refresh: run `27752466811` succeeded for `vevo-reporting:latest`, digest `sha256:f6b1d59a73dc3db38f9efae07f25ebca92946793b9f0df1b7807ac623b4893c1`
  - production daily reporting smoke: run `27752601944` succeeded with `project=all`, marker `creditnote-rate-docs-20260618`, `send_email=true`, `update_task_image=true`
  - VEVO hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.6.196`, service `vevo-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:9`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/15fc7cc2e24741db90ab742703d38086`, marker path `http://127.0.0.1:8000/marker.json`
  - VEVO verification: task image digest `sha256:f6b1d59a73dc3db38f9efae07f25ebca92946793b9f0df1b7807ac623b4893c1`, `task-image-updated=true`, SES `MessageId=0107019eda4e95e7-766fb685-afe6-4bcc-a6c4-6f486537495a-000000`, `LOCALHOST_MARKER_OK`, `has_creditnote_payload=true`, `send_email=true`, `creditnote_count=267`, `credited_gross_eur=4794.93`, report path `data/vevo/report_latest.html`, UI smoke `UI_SMOKE_OK:vevo:production-board` and `UI_SMOKE_OK:vevo:daily-profit-loss`
  - ROY hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.25.38`, service `roy-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:38`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/2af40f698aeb4b499750790405ec0f38`, marker path `http://127.0.0.1:8000/marker.json`
  - ROY verification: task image digest `sha256:f6b1d59a73dc3db38f9efae07f25ebca92946793b9f0df1b7807ac623b4893c1`, `task-image-updated=true`, SES `MessageId=0107019eda8293a3-c6919a8b-246d-478d-8abd-112aadf137b0-000000`, `LOCALHOST_MARKER_OK`, `has_creditnote_payload=true`, `send_email=true`, `creditnote_count=110`, `credited_gross_eur=11063.52`, report path `data/roy/report_latest.html`, UI smoke `UI_SMOKE_OK:roy:daily-profit-loss`
  - monthly creditnote deploy: initial auto run `27752466796` raced ECR and registered old digest `sha256:df5f61b564d155dc0a7f9a658682e1cbf7d20089b066125f0c898a82980ab5ac`; manual rerun `27756747537` succeeded after ECR and updated `monthly-creditnote-export` to task definition `monthly-creditnote-export:9` using digest `sha256:f6b1d59a73dc3db38f9efae07f25ebca92946793b9f0df1b7807ac623b4893c1`
  - monthly verification: `CREDITNOTE_EXPORT_MARKER_OK`, `DEPLOY_MONTHLY_CREDITNOTE_EXPORT_OK`, and carrier rows now show non-zero rates when `Dobropisy > 0`, e.g. VEVO Packeta `8/114 = 7.02%`, VEVO SPS Balikovo `11/260 = 4.23%`, ROY Packeta `5/134 = 3.73%`
  - Current status: scheduled daily emails for both shops and the monthly creditnote schedule now point to the image with `Creditnotes / Sent` carrier rates
  - Next exact step: implement the next requested ROY inventory cost value metric and deploy it through the same PR/ECR/production-smoke path

- Monthly combined ROY+VEVO creditnote export deploy is fixed and verified on `2026-06-18`:
  - code/workflow PRs merged to `main`:
    - PR `#186` (`8685eea41de88516c462c9261695a5ea9656e679`) added GitHub Secret -> SSM credential overrides for monthly ROY/VEVO admin credentials
    - PR `#187` (`f04859d22a2b3031b7c2dc8b647475561b5566be`) isolated monthly runtime from project `.env` files and added runner-side admin credential preflight
    - PR `#188` (`fffa6aaa8470791f154bd7f7676cca67ef9ae323`) removed inherited unprefixed `BIZNISWEB_*` fallback credentials and added Fargate-side admin preflight
    - PR `#189` (`175fc2d0df8d5052e4332519eeee935db3c75cdd`) added project-prefixed API URL/token support so carrier/reporting audit works without unprefixed source credentials
  - GitHub Secrets now configured: `ROY_BIZNISWEB_USERNAME`, `ROY_BIZNISWEB_PASSWORD`, `VEVO_BIZNISWEB_USERNAME`, `VEVO_BIZNISWEB_PASSWORD`; values were populated from local project `.env` files without printing secret values
  - ECR refresh: run `27751472242` succeeded for `vevo-reporting:latest`, digest `sha256:df5f61b564d155dc0a7f9a658682e1cbf7d20089b066125f0c898a82980ab5ac`
  - final monthly deploy/smoke: manual workflow run `27751599462` succeeded on `main` after the ECR refresh; `send_email_now=false`, so SES email send was dry-run only
  - hard-gate context: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.17.194`, service `monthly-creditnote-export`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/monthly-creditnote-export:7`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/ad2e56e713534e7e82161b7810984ce6`, image digest `sha256:df5f61b564d155dc0a7f9a658682e1cbf7d20089b066125f0c898a82980ab5ac`, marker path `http://127.0.0.1:8000/marker.json`
  - deploy verification markers: `GITHUB_SECRET_ADMIN_LOGIN_OK` for ROY and VEVO, `credential_override_count=4`, `TASK_CREDENTIAL_OVERRIDES_OK secret_count=4 skip_project_env=true`, `FARGATE_ADMIN_LOGIN_OK` for ROY and VEVO, `CREDITNOTE_EXPORT_MARKER_OK`, and `DEPLOY_MONTHLY_CREDITNOTE_EXPORT_OK schedule=monthly-creditnote-export task_definition=...:7`
  - final smoke summary: `exported_rows=39`, project counts `ROY=14`, `VEVO=25`, fetch totals `ROY fetched=120/exported=14`, `VEVO fetched=288/exported=25`, carrier rows `15`, non-unknown carrier rows `14`
  - reporting exclusion/carrier audit is healthy: `audit_errors={}`, `checked_orders=39`, `excluded_from_revenue=39`, `included_in_revenue=0`, `order_not_found=0`, `sent_creditnoted_orders=0`
  - current production status: EventBridge schedule `monthly-creditnote-export` now points to task definition `monthly-creditnote-export:7` with the corrected image and credential/runtime handling
  - Next exact step: monitor the next regular monthly run on `2026-07-14 06:00 Europe/Bratislava`; use `send_email_now=true` only when an immediate real email send is intentionally needed

- Monthly creditnote prefixed API runtime fix is in progress on `2026-06-18`:
  - branch/worktree: `codex/monthly-creditnote-prefixed-api-runtime` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - context: deploy run `27750720572` succeeded after PR `#188` and confirmed `FARGATE_ADMIN_LOGIN_OK` for ROY and VEVO, but the generated monthly summary still showed reporting audit errors `BIZNISWEB_API_TOKEN missing for project 'roy'/'vevo'`, causing carrier context to degrade to `Unknown carrier`
  - root cause: the monthly task now correctly avoids unprefixed source credentials, but reusable reporting runtime still read only unprefixed `BIZNISWEB_API_TOKEN` for order-context/carrier audit; multi-project monthly runtime needs prefixed `ROY_`/`VEVO_` API URL/token support
  - change: `reporting_core.config` now resolves project-prefixed env values first (`ROY_BIZNISWEB_API_TOKEN`, `VEVO_BIZNISWEB_API_TOKEN`, etc.) with legacy unprefixed fallback
  - change: `reporting_core.runtime` and `creditnote_export.fetch_project_reporting_order_context()` now use the project-prefixed resolver for API token/runtime context
  - local verification:
    - `python -m py_compile creditnote_export.py reporting_core/config.py reporting_core/runtime.py`
    - `python -m unittest tests.test_creditnote_export`
    - ECS-like local live smoke with `REPORT_SKIP_PROJECT_ENV=true` and only prefixed ROY/VEVO runtime credentials: `exported_rows=39`, `project_counts={"ROY":14,"VEVO":25}`, `audit_errors={}`, `carrier_rows=15`, `non_unknown_carriers=14`
    - `git diff --check`
  - Next exact step: commit/push this code fix, merge through PR, wait for ECR rebuild, then rerun `Deploy Monthly Creditnote Export` so task definition uses the rebuilt image and verify carrier/reporting audit has no API-token errors

- Monthly creditnote export Fargate runtime preflight is in progress on `2026-06-18`:
  - branch/worktree: `codex/monthly-creditnote-fargate-preflight` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - context: PR `#187` merged as `f04859d22a2b3031b7c2dc8b647475561b5566be`; deploy run `27750334925` proved `GITHUB_SECRET_ADMIN_LOGIN_OK` for ROY and VEVO, `credential_override_count=4`, and `TASK_CREDENTIAL_OVERRIDES_OK`, but the Fargate smoke still failed ROY admin login with `401 Unauthorized`
  - hard-gate context from failed run: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.19.15`, service `monthly-creditnote-export`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/monthly-creditnote-export:4`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/f7fc158140e64bd8ab6fea8979a1e497`, image digest `sha256:0bcb914911f4726949032991bff95a390e8bae1cd7984f91e5604e1b55fe7699`, marker path `http://127.0.0.1:8000/marker.json`
  - change: monthly task-definition builder now removes inherited unprefixed `BIZNISWEB_USERNAME`, `BIZNISWEB_PASSWORD`, `BIZNISWEB_API_TOKEN`, and `BIZNISWEB_API_URL` from the source daily task so the monthly export cannot silently fall back to stale source credentials
  - change: host smoke now runs a Fargate-side admin-login preflight before `monthly_creditnote_export_runner.py`, logging only credential presence booleans and `FARGATE_ADMIN_LOGIN_OK` markers, never secret values
  - local verification:
    - YAML parse check for `.github/workflows/deploy-monthly-creditnote-export.yml`
    - embedded Python heredoc syntax check for the workflow
    - `git diff --check`
  - Next exact step: merge this workflow diagnostic/fallback removal, rerun `Deploy Monthly Creditnote Export`, and use the Fargate-side preflight result to distinguish runtime credential injection from a BizniWeb/ECS egress authorization problem

- Monthly creditnote export runtime env isolation fix is in progress on `2026-06-18`:
  - branch/worktree: `codex/monthly-creditnote-runtime-env` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - context: PR `#186` merged as `8685eea41de88516c462c9261695a5ea9656e679` and deploy run `27749819179` proved that GitHub Secrets were present (`credential_override_count=4`) and task definition `monthly-creditnote-export:3` was registered, but the smoke task still failed ROY admin login with `401 Unauthorized`
  - hard-gate context from failed follow-up run: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.3.234`, service `monthly-creditnote-export`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/monthly-creditnote-export:3`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/91f56b39195d4c6dafe608ac072767c3`, image digest `sha256:0bcb914911f4726949032991bff95a390e8bae1cd7984f91e5604e1b55fe7699`, marker path `http://127.0.0.1:8000/marker.json`
  - local finding: clean-process admin login using `projects/roy/.env` and `projects/vevo/.env` succeeds for both projects, so the local credential values are valid; the monthly runtime must avoid image/project `.env` overriding ECS runtime credentials
  - change: monthly deploy workflow now sets `REPORT_SKIP_PROJECT_ENV=true` in the registered ECS task definition and in the host-smoke container override
  - change: monthly deploy workflow now performs a runner-side ROY/VEVO GitHub Secret admin-login preflight before writing SSM overrides, and asserts the registered task definition contains the required `ROY_`/`VEVO_` credential override secrets
  - local verification:
    - YAML parse check for `.github/workflows/deploy-monthly-creditnote-export.yml`
    - embedded Python heredoc syntax check for the workflow
    - stdlib admin-login preflight succeeds locally for ROY and VEVO using the same request shape added to the workflow
    - `_login_admin()` succeeds locally for ROY and VEVO with `REPORT_SKIP_PROJECT_ENV=true` and prefixed runtime credentials
    - `git diff --check`
  - Next exact step: validate/merge this workflow follow-up, then rerun `Deploy Monthly Creditnote Export` and require `GITHUB_SECRET_ADMIN_LOGIN_OK`, `TASK_CREDENTIAL_OVERRIDES_OK`, `CREDITNOTE_EXPORT_MARKER_OK`, and localhost marker before closing the monthly export deploy issue

- Monthly creditnote export credential override fix is in progress on `2026-06-18`:
  - branch/worktree: `codex/fix-monthly-creditnote-credentials` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - context: deploy workflow run `27742671211` failed in the monthly creditnote export host smoke because the Fargate task received stale ROY BizniWeb web credentials and `/admin/login/authenticate/` returned `401 Unauthorized`
  - hard-gate context from the failed run: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.11.12`, service `monthly-creditnote-export`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/monthly-creditnote-export:2`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/95a845aef9f148b1a36bb7c3f7466914`, marker path `http://127.0.0.1:8000/marker.json`
  - change: `.github/workflows/deploy-monthly-creditnote-export.yml` now accepts `ROY_BIZNISWEB_USERNAME`, `ROY_BIZNISWEB_PASSWORD`, `VEVO_BIZNISWEB_USERNAME`, and `VEVO_BIZNISWEB_PASSWORD` from GitHub Secrets, writes provided values into AWS SSM `SecureString` parameters, and wires those SSM parameters into the monthly ECS task definition as credential overrides
  - change: the monthly task execution role gets an inline policy to read only the configured SSM credential override parameters, while the existing copied daily-task credentials remain the fallback when overrides are not present
  - remote secret setup: the four GitHub Secrets above were populated from local project `.env` files without printing secret values
  - local verification:
    - YAML parse check for `.github/workflows/deploy-monthly-creditnote-export.yml`
    - embedded Python heredoc syntax check for the workflow
    - `git diff --check`
  - Next exact step: commit/push this branch, merge through PR, then run/monitor `Deploy Monthly Creditnote Export` and require `CREDITNOTE_EXPORT_MARKER_OK` plus localhost marker before treating the monthly export deploy as fixed

- Creditnote shipped-before-cancel reporting correction is implemented and deployed on `2026-06-18`:
  - code PR `#184` merged to `main` as `8437d7723100b7b9f5abed65a5bb3b462af0f68d`
  - change: creditnoted/canceled orders count as sent only when the current status is `Odoslaná`/`Odoslana` or the creditnote storno guard persisted an audit showing the previous status before cancellation was shipped
  - change: carrier dobropis rate now uses sent orders as denominator and sent creditnoted orders as numerator; total credited EUR and creditnote document counts remain visible
  - change: retained creditnote fulfillment cost is kept only for creditnoted orders that were demonstrably sent before being canceled/refunded
  - change: `creditnote_storno_guard.py` now records previous order status before changing eligible creditnoted revenue orders to `Storno`, and persists the audit locally plus S3 when `REPORT_S3_BUCKET` is configured
  - local verification:
    - `python -m py_compile export_orders.py creditnote_export.py creditnote_storno_guard.py daily_report_runner.py dashboard_modern.py`
    - `python -m unittest tests.test_creditnote_export tests.test_creditnote_storno_guard tests.test_reporting_calculation_fixes tests.test_dashboard_modern tests.test_invoice_generation`
    - `python -m unittest tests.test_creditnote_export tests.test_creditnote_storno_guard tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_reporting_calculation_fixes tests.test_dashboard_modern tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity tests.test_roy_picking_lists_pdf`
    - `python -m json.tool projects\roy\settings.json`; `python -m json.tool projects\vevo\settings.json`; `git diff --check`
  - ECR refresh: run `27742671194` succeeded, image digest `sha256:0bcb914911f4726949032991bff95a390e8bae1cd7984f91e5604e1b55fe7699`
  - production reporting smoke: run `27742785997` succeeded with `project=all`, marker `shipped-creditnote-20260618`, `send_email=true`, `update_task_image=true`
  - VEVO hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.8.124`, service `vevo-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:8`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/a4336972bed74697b965ced4b3d543c7`, task image digest `sha256:0bcb914911f4726949032991bff95a390e8bae1cd7984f91e5604e1b55fe7699`, `task-image-updated=true`
  - VEVO verification: SES `MessageId=0107019ed99af316-56d7082e-1821-40c5-a5c7-19afc911e80e-000000`, localhost marker `LOCALHOST_MARKER_OK`, `has_creditnote_payload=true`, `send_email=true`, `creditnote_count=267`, `credited_gross_eur=4794.93`, report path `data/vevo/report_latest.html`, UI smoke `UI_SMOKE_OK:vevo:production-board` and `UI_SMOKE_OK:vevo:daily-profit-loss`
  - ROY hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.20.25`, service `roy-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:37`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/ea014a9e2172470b83ecfd0fdc2f9cda`, task image digest `sha256:0bcb914911f4726949032991bff95a390e8bae1cd7984f91e5604e1b55fe7699`, `task-image-updated=true`
  - ROY verification: SES `MessageId=0107019ed9c45858-7709c644-0eca-4738-bedb-a4a848542743-000000`, localhost marker `LOCALHOST_MARKER_OK`, `has_creditnote_payload=true`, `send_email=true`, `creditnote_count=110`, `credited_gross_eur=11063.52`, report path `data/roy/report_latest.html`, UI smoke `UI_SMOKE_OK:roy:daily-profit-loss`
  - Current status: scheduled daily emails for both shops now point to the new image and the corrected reporting emails were regenerated and sent on `2026-06-18`
  - Next exact step: monitor the next regular morning email for both e-shops; no known code/deploy blocker remains for shipped-before-cancel creditnote reporting

- Production report email regeneration follow-up is in progress on `2026-06-18`:
  - branch/worktree: `codex/report-smoke-logs` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - PR `#177` and PR `#178` are merged to `main`; ECR rebuild run `27734814979` succeeded for `vevo-reporting:latest` with digest `sha256:10202cb947ab0ab50ec2be9fe6331c8cc48e5204df60ebdaffe271369dd03bbd`
  - first manual `Production Reporting Smoke` dispatch run `27734954008` used `project=all`, `marker=creditnote-email-20260618`, `send_email=true`
  - VEVO hard-gate context from that run: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.40.4`, service `vevo-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:5`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/b992759c693845039e84f6f58542f268`
  - the run failed before `LOCALHOST_MARKER_OK`, SES proof, or UI smoke could be recorded; GitHub log showed the VEVO task running through `840s` and then ended after the CloudWatch log stream line without enough task-stop context
  - change in this branch: production smoke now prints explicit task timeout/stopped context, container exit code/reason, and bounded `--no-paginate` CloudWatch log output so the next run can distinguish report failure from smoke/log collection failure
  - local verification: workflow YAML parse check and `git diff --check`
  - Next exact step: commit/push this workflow hardening, merge it through PR, then re-dispatch production reporting with `send_email=true` and record `LOCALHOST_MARKER_OK`, SES message IDs, and UI smoke for VEVO and ROY

- Production email rerun marker/tag correction is in progress on `2026-06-18`:
  - branch/worktree: `codex/report-email-untagged-rerun` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - rerun `27735689741` for `project=all`, `marker=creditnote-email-20260618-rerun`, `send_email=true` started VEVO task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/69ff9f779be0455aa562e17e4d946a98`
  - VEVO hard-gate context: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.28.112`, service `vevo-daily-report-email`, CloudWatch stream `/ecs/vevo-reporting-daily:ecs/reporting/69ff9f779be0455aa562e17e4d946a98`
  - VEVO report generation completed and SES sent `MessageId=0107019ed8eea57d-883dd5e4-7574-44f9-afbe-927d58b6cb9e-000000`
  - the run still failed before localhost marker/UI smoke because `REPORT_OUTPUT_TAG` was present during `send_email=true`; `daily_report_runner.py` wrote tagged latest files like `report_latest__vevo-creditnote-email-20260618-rerun.html`, while validation correctly expected untagged `report_latest.html`
  - change in this branch: separate `REPORT_MARKER` from `REPORT_OUTPUT_TAG`, unset `REPORT_OUTPUT_TAG` before real email generation, and keep marker metadata available for localhost validation
  - Next exact step: validate/merge this workflow fix, then run ROY `send_email=true`; avoid re-sending VEVO unless an untagged production latest refresh is explicitly needed

- Production daily reporting task image refresh is in progress on `2026-06-18`:
  - branch/worktree: `codex/reporting-task-image-refresh` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - ROY rerun `27736341042` used task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:35`, private IP `172.31.45.82`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/ce2ad8375fe844abb8b162d13489fc72`
  - ROY generated and sent email `MessageId=0107019ed917c4a0-07b1b061-c542-42ef-8c66-dd8dde05111b-000000`, but smoke failed with `AssertionError: creditnote summary missing from dashboard payload`
  - because the code path would write `summary.available=false` even on creditnote API failure, a missing `dashboard.creditnotes.summary` indicates the scheduled reporting task was still running an older image/task definition rather than the new PR `#177` reporting code
  - change in this branch: `Production Reporting Smoke` gains `update_task_image`; when true it resolves ECR `vevo-reporting:latest` to the current digest, registers a new daily reporting task definition if the scheduled container image differs, updates the EventBridge schedule, and then runs the host smoke/email task against that refreshed definition
  - Next exact step: validate/merge this workflow update, dispatch production reporting smoke with `update_task_image=true`, and require `LOCALHOST_MARKER_OK`, SES message IDs, and UI smoke before considering both shops fixed
- Production reporting smoke email dispatch mode is implemented locally on `2026-06-18`:
  - branch/worktree: `codex/report-email-dispatch` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - workflow `.github/workflows/production-reporting-smoke.yml` keeps the default `send_email=false` dry-run behavior
  - manual dispatch with `send_email=true` runs the real untagged `daily_report_runner.py --project <project> --skip-invoices`, sends the SES report email, writes `LOCALHOST_MARKER_OK`, verifies the new `dashboard.creditnotes` payload and visible HTML section, then runs the existing localhost UI/API smoke
  - hard-gate targets: instance-id `N/A (scheduled ECS/Fargate task)`, service names `vevo-daily-report-email` and `roy-daily-report-email`, private IP resolved at ECS task start, marker path `http://127.0.0.1:8000/marker.json`, local UI path `http://127.0.0.1:8787/dashboard/{project}`
  - Next exact step: validate workflow syntax, commit/push, open/merge PR, then dispatch `Production Reporting Smoke` with `project=all` and `send_email=true` after ECR build `27734814979` succeeds

- Daily reporting creditnote metrics visibility fix is implemented locally on `2026-06-18`:
  - branch/worktree: `codex/creditnote-carrier-audit` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - reason: the morning `2026-06-18` VEVO/ROY report emails were generated before PR `#177` was merged/deployed, and the main HTML/email report did not yet expose the new creditnote metrics as a visible reporting section
  - change: `export_orders.py` now builds `advanced_dtc_metrics["creditnotes"]` from the BizniWeb creditnote registry for the report window, converts credited gross/net amounts to EUR, keeps revenue-exclusion audit counts, and exposes retained fulfillment cost on creditnoted/storno orders
  - change: creditnote carrier rate is now `creditnoted orders / sent orders by the same carrier`, where sent orders include current realized orders plus found creditnoted orders so Storno creditnotes do not disappear from the denominator
  - change: `dashboard_modern.py` renders a visible `Creditnotes and carrier return rate` section in the main HTML report and embeds the same data under `dashboard.creditnotes`
  - change: `daily_report_runner.py` appends a `DOBROPISY` block into the plain-text email summary from the dashboard payload
  - local verification:
    - `python -m py_compile export_orders.py creditnote_export.py dashboard_modern.py daily_report_runner.py`
    - `python -m unittest tests.test_creditnote_export tests.test_reporting_calculation_fixes tests.test_dashboard_modern`
    - `python -m unittest tests.test_creditnote_export tests.test_creditnote_storno_guard tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_reporting_calculation_fixes tests.test_dashboard_modern tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity tests.test_roy_picking_lists_pdf`
    - `python -m json.tool projects\roy\settings.json`; `python -m json.tool projects\vevo\settings.json`
    - `git diff --check` (only existing Git CRLF normalization warning for `creditnote_export.py`)
  - Next exact step: commit/push this fix, mark PR `#177` ready, merge to `main`, wait for ECR `latest` rebuild, then run production host smoke and real report/email regeneration for VEVO and ROY

- Credit-note credited-amount, revenue-exclusion, and carrier-rate audit extension is implemented locally on `2026-06-17`:
  - branch/worktree: `codex/creditnote-carrier-audit` in `C:\Users\Patrik jankech\Desktop\biznisweb-creditnote-carrier-audit`
  - PDF/email summary now shows positive total credited amount separately from the existing signed accounting summary
  - exact creditnoted order numbers are checked through BizniWeb `getOrder(order_num)` and the existing realized-revenue decision logic; the PDF/email summary reports how many creditnoted orders still remain in reporting revenue
  - carrier stats are grouped by normalized provider per e-shop (`Packeta`, `SPS Balikovo`, `Slovenska posta`, `DPD`, etc.), not by individual pickup-point address; `Dobropis rate % = dobropisovane objednavky / realized objednavky v reportovanom obdobi`
  - CloudWatch metrics added for `CreditnoteExportGrossAmount` and `CreditnoteExportRevenueIncludedOrders`; local smoke can use `--skip-metrics` / `CREDITNOTE_EXPORT_SKIP_METRICS=1` without changing production defaults
  - local verification:
    - `python -m py_compile creditnote_export.py monthly_creditnote_export_runner.py`
    - `python -m unittest tests.test_creditnote_export`
    - `python -m unittest tests.test_creditnote_export tests.test_invoice_generation tests.test_unpaid_order_cancellation`
    - `python -m json.tool projects\roy\settings.json`
    - `python -m json.tool projects\vevo\settings.json`
    - `git diff --check` (only Git CRLF normalization warning for `creditnote_export.py`)
    - PDF text check confirmed sections `Dobropisovana suma spolu`, `Dobropisy podla prepravcu`, `Kontrola vylucenia z reporting revenue`, and rate definition in the generated PDF
  - live local smoke: `python monthly_creditnote_export_runner.py --reference-date 2026-06-14 --skip-email --skip-metrics --output-tag carrier_audit_smoke3` exited `0`
    - output PDF: `data/combined_exports/dobropisy_actual_roy_vevo_2026-05_created_carrier_audit_smoke3.pdf` (ignored artifact)
    - exported dobropisy: total `39`, ROY `14`, VEVO `25`
    - positive credited gross totals from smoke summary: EUR bucket `1,649.91`, Kc `1,294.00`, RON `94.14`
    - revenue exclusion audit: checked `39`, excluded from realized revenue `34`, still included `5`, original orders not found `0`, audit errors `0`
    - top nonzero carrier rates by provider in smoke: ROY FanBox `1/1 = 100.00%`; VEVO Slovenska posta `3/37 = 8.11%`; VEVO DPD `3/42 = 7.14%`; VEVO Packeta `8/114 = 7.02%`; VEVO SPS Balikovo `11/261 = 4.21%`; ROY Packeta `5/129 = 3.88%`; ROY Kurier na adresu `3/87 = 3.45%`; ROY SPS Balikovo `4/122 = 3.28%`; ROY Slovenska posta `1/31 = 3.23%`
    - BizniWeb logged one transient `price_elements` internal-server warning for VEVO order `2602007112`; the runner completed and summary `audit_errors` stayed empty
  - Next exact step: commit and push `codex/creditnote-carrier-audit`, open/merge PR, then deploy the monthly credit-note task and record the Fargate hard-gate marker/context after workflow success

- Monthly combined ROY+VEVO credit-note export automation was implemented locally on `2026-06-17`:
  - scope: one repo-local mini module for accounting credit notes, not GraphQL invoices
  - source endpoint: BizniWeb admin `/erp/orders/creditnotes/getListJson`
  - default window: previous calendar month based on `Europe/Bratislava`; when the scheduler runs on the 14th, it exports the previous month
  - output: one PDF file in `data/combined_exports`; no XLSX workbook and no source JSON sidecar are generated by the monthly credit-note run
  - email: SES raw email with the PDF attached; default recipient `mil.terem@gmail.com`; sender comes from `CREDITNOTE_EXPORT_EMAIL_FROM` or `REPORT_EMAIL_FROM`
  - schedule metadata: `projects/roy/settings.json` -> `monthly_creditnote_export`, schedule `monthly-creditnote-export`, cron `cron(0 6 14 * ? *)`, timezone `Europe/Bratislava`, task family `monthly-creditnote-export`, projects `roy,vevo`
  - deploy workflow: `.github/workflows/deploy-monthly-creditnote-export.yml` registers/updates the dedicated ECS task family and EventBridge Scheduler job, copies ROY/VEVO BizniWeb credentials into prefixed runtime env/secrets, and verifies a host smoke marker at `http://127.0.0.1:8000/marker.json`
  - local verification:
    - `python -m py_compile creditnote_export.py monthly_creditnote_export_runner.py`
    - `python -m json.tool projects\roy\settings.json`
    - `python -m unittest tests.test_creditnote_export tests.test_invoice_generation tests.test_unpaid_order_cancellation`
    - `python monthly_creditnote_export_runner.py --reference-date 2026-06-14 --skip-email --output-tag local_pdf_smoke2` returned `exported_rows=39`, ROY `14`, VEVO `25`, generated a valid `%PDF-` file, and did not create `.xlsx` or `_source.json` artifacts for that tag
    - parsed `.github/workflows/deploy-monthly-creditnote-export.yml` and `.github/workflows/build-and-push-ecr.yml` with `yaml.safe_load`
    - `git diff --check`
  - local generated artifacts: `data/combined_exports/dobropisy_actual_roy_vevo_2026-05_created_local_pdf_smoke2.pdf` (ignored export output)
  - Next exact step: merge through PR, let `Build and Push ECR` publish an image containing `monthly_creditnote_export_runner.py`, then run `Deploy Monthly Creditnote Export` and record the Fargate hard-gate marker/context after the workflow succeeds

- ROY live dashboard failed-to-fetch mitigation is implemented and deployed on `2026-06-13`:
  - root cause found live: `/api/operations/roy/live` is healthy but can take about 49-51 seconds when cache expires because the ROY operations snapshot scans BiznisWeb orders and stock; dashboard auto-refresh is 90 seconds while cache TTL is 60 seconds, so regular live refreshes can hit the slow path
  - second root cause found by production UI test: when the page is opened via a URL containing Basic Auth credentials, browser JS resolves relative API `fetch()` calls against a credentialed document URL and rejects the request; ROY operations fetch calls now use a sanitized `window.location.origin` API helper
  - change: normal non-forced ROY operations API calls now return the last cached snapshot immediately when cache is stale and start a background refresh instead of blocking the UI request
  - change: cache writes are guarded by a lock and invalidation token so an old background refresh cannot overwrite a cache invalidated by dashboard actions
  - change: the frontend keeps the latest rendered data visible and reports a clearer message if a refresh fails after data was already loaded; all ROY operations API fetches use sanitized same-origin URLs
  - local tests: `python -m py_compile roy_operations_dashboard.py live_dashboard_server.py`; `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_picking_lists_pdf tests.test_live_dashboard_auth tests.test_live_dashboard_mobile`; `python -m py_compile live_dashboard_server.py roy_operations_dashboard.py`; `python -m unittest tests.test_roy_operations_dashboard tests.test_live_dashboard_auth tests.test_live_dashboard_mobile`; `git diff --check`
  - PR/deploy: PR `#173` merged stale-cache behavior into `main` (`8c426fa`), PR `#174` merged sanitized ROY fetch URLs into `main` (`9f118a2`); final ROY App Runner deploy run `27460189231` succeeded with digest `sha256:c41f9463ee724ac1be904130179958afe76eef5f2998b40b487a52e098e241de`
  - App Runner hard-gate: instance-id `N/A (AWS App Runner managed service)`, private IP `N/A (AWS App Runner managed service)`, service `biznisweb-roy-operations-dashboard`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, health path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`, production path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - production smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=7:personal_pickups=0:inventory_alerts=20:inventory_rows=160:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=138008`; `APP_RUNNER_DEPLOY_OK:biznisweb-roy-operations-dashboard:https://qvfzvh82c3.eu-central-1.awsapprunner.com`
  - post-deploy verification: `/health` returned `200`; `/production/roy` returned `200`; `/api/operations/roy/live` returned marker `roy-operations-dashboard`, `cache.status=fresh` in `0.50s`, and later `cache.status=stale_revalidating` in `0.76s` after TTL expiry; browser UI test through credential URL rendered without `Failed to fetch`, without credential URL error, and with no console errors
  - Next exact step: monitor the next scheduled auto-refresh window; no known blocker

- ROY wholesale detection VAT-basis fix is implemented and deployed on `2026-06-10`:
  - root cause: ROY picking-list wholesale detection compared order item prices against current retail final prices on a gross/VAT-including basis; foreign company orders sold without VAT could therefore look like discounted/wholesale orders even when the customer paid the normal net price
  - change: wholesale detection now compares unit prices on a net basis; order line net price is compared against current retail final price converted to net using `operations_dashboard.wholesale_detection.retail_tax_rate=23`
  - behavior: foreign B2B VAT-exempt orders at normal net retail price are not marked wholesale; foreign B2B VAT-exempt orders with a true net discount still are marked wholesale
  - local tests: `python -m json.tool projects\roy\settings.json`; `python -m py_compile roy_operations_dashboard.py live_dashboard_server.py`; `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_reporting_calculation_fixes tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity tests.test_roy_picking_lists_pdf tests.test_live_dashboard_auth tests.test_live_dashboard_mobile`; `git diff --check`
  - PR/deploy: PR `#170` merged into `main` (`b691152`); first ECR build run `27258412717` failed at Amazon ECR login with `connect ETIMEDOUT 3.122.128.199:443`, rerun of the same workflow succeeded with digest `sha256:6ff2f4885a51fa46f731505a18dcfc31622329a767597f45ada7746a7a454969`; ROY App Runner deploy run `27258883863` succeeded
  - Fargate hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.15.182`, service `roy-daily-report-email`, image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:6ff2f4885a51fa46f731505a18dcfc31622329a767597f45ada7746a7a454969`, marker path `http://127.0.0.1:8000/marker.json`, marker `LIVE_ARTIFACT_MARKER_OK`
  - App Runner hard-gate: instance-id `N/A (AWS App Runner managed service)`, private IP `N/A (AWS App Runner managed service)`, service `biznisweb-roy-operations-dashboard`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, health path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`, production path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`, digest `sha256:6ff2f4885a51fa46f731505a18dcfc31622329a767597f45ada7746a7a454969`
  - production smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=29:personal_pickups=0:inventory_alerts=19:inventory_rows=160:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=322138`; `APP_RUNNER_DEPLOY_OK:biznisweb-roy-operations-dashboard:https://qvfzvh82c3.eu-central-1.awsapprunner.com`
  - post-deploy live verification: `/health` returned `200`; `/api/operations/roy/live?refresh=1` returned marker `roy-operations-dashboard`, `fulfillable_orders=29`, and only one current wholesale-flagged fulfillable order, `2677002963`, with `comparison_basis=net` and a true net discount of `25.0%`
  - Next exact step: if a specific foreign VAT-exempt company order still shows VO, inspect that order's item `tax_rate`, line net price, and product `final_price` payload from BiznisWeb

- ROY multilingual COD fallback is implemented and deployed on `2026-06-09`:
  - change: ROY `operations_dashboard` and `realized_revenue` now include wider COD payment title fallbacks for future foreign-language orders, including English, Polish, Romanian, German, French, Italian, Spanish/Portuguese, Balkan, Dutch, and Cyrillic COD terms
  - change: string normalization in `roy_operations_dashboard.py` and `export_orders.py` now folds Latin characters that do not decompose through standard accent removal, including Polish `ł`, so titles such as `Płatność przy odbiorze` match configured fallback terms
  - stable behavior: known BiznisWeb payment IDs `7`, `10`, and `16` remain the primary safe match; multilingual text matching is a fallback for new language variants or new payment IDs with recognizable COD wording
  - local tests: `python -m json.tool projects\roy\settings.json`; `python -m py_compile roy_operations_dashboard.py export_orders.py live_dashboard_server.py`; `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_reporting_calculation_fixes tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity tests.test_roy_picking_lists_pdf tests.test_live_dashboard_auth tests.test_live_dashboard_mobile`; `git diff --check`
  - PR/deploy: PR `#168` merged into `main` (`2252ec1`); ECR build run `27211059373` succeeded with `latest` digest `sha256:5fde6f46b52c54edd53ca685550f45cb57a5775c3f6f15c46b585a9b68fa9e77`; ROY App Runner deploy run `27211206949` succeeded
  - Fargate hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.15.147`, service `roy-daily-report-email`, image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:5fde6f46b52c54edd53ca685550f45cb57a5775c3f6f15c46b585a9b68fa9e77`, marker path `http://127.0.0.1:8000/marker.json`, marker `LIVE_ARTIFACT_MARKER_OK`
  - App Runner hard-gate: instance-id `N/A (AWS App Runner managed service)`, private IP `N/A (AWS App Runner managed service)`, service `biznisweb-roy-operations-dashboard`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, health path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`, production path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`, digest `sha256:5fde6f46b52c54edd53ca685550f45cb57a5775c3f6f15c46b585a9b68fa9e77`
  - production smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=26:personal_pickups=0:inventory_alerts=19:inventory_rows=160:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=292621`; `APP_RUNNER_DEPLOY_OK:biznisweb-roy-operations-dashboard:https://qvfzvh82c3.eu-central-1.awsapprunner.com`
  - post-deploy live verification: `/health` returned `200`; `/api/operations/roy/live?refresh=1` returned marker `roy-operations-dashboard`, `fulfillable_orders=26`, `cod_waiting_orders=9`, and orders `2679000026`/`2679000027` still as `cod_waiting`
  - Next exact step: when a new foreign-language payment method appears, verify whether its payment title matches one of the configured COD fallbacks; if BiznisWeb uses an unrelated title, add its stable payment `reference_id`

- ROY HU COD fulfillment fix is implemented and deployed on `2026-06-09`:
  - root cause: Hungarian COD orders such as `2679000027` and `2679000026` use BiznisWeb payment `Utánvétes fizetés` with `reference_id=16`; ROY operations dashboard only recognized SK/CZ COD IDs `7` and `10`, so those orders stayed `not_ready` and were not included in picking-list PDF generation
  - change: `projects/roy/settings.json` now recognizes `cod_payment_ids=["7","10","16"]` and text fallbacks `utanvetes`/`utanvet` in both `operations_dashboard` and `realized_revenue`
  - local live-data verification: direct BiznisWeb checks for orders `2679000027` and `2679000026` now return `fulfillable=True`, `fulfillment_reason=cod_waiting`
  - local tests: `python -m json.tool projects\roy\settings.json`; `python -m py_compile roy_operations_dashboard.py export_orders.py live_dashboard_server.py`; `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_reporting_calculation_fixes tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity tests.test_roy_picking_lists_pdf tests.test_live_dashboard_auth tests.test_live_dashboard_mobile`; `git diff --check`
  - PR/deploy: PR `#166` merged into `main` (`f82c521`); ECR build run `27209415810` succeeded with `latest` digest `sha256:d7418756823d4b6b3387130aae44e2a05a391e46562244a1c8e75f81beb795a8`; ROY App Runner deploy run `27209579662` succeeded
  - Fargate hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.19.244`, service `roy-daily-report-email`, task definition `roy-reporting-daily:31`, task `efc61698ad01438a8940498d3df6b647`, image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:d7418756823d4b6b3387130aae44e2a05a391e46562244a1c8e75f81beb795a8`, marker path `http://127.0.0.1:8000/marker.json`, marker `LIVE_ARTIFACT_MARKER_OK`
  - App Runner hard-gate: instance-id `N/A (AWS App Runner managed service)`, private IP `N/A (AWS App Runner managed service)`, service `biznisweb-roy-operations-dashboard`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, health path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`, production path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`, digest `sha256:d7418756823d4b6b3387130aae44e2a05a391e46562244a1c8e75f81beb795a8`
  - production smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=25:personal_pickups=0:inventory_alerts=19:inventory_rows=160:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=283825`; `APP_RUNNER_DEPLOY_OK:biznisweb-roy-operations-dashboard:https://qvfzvh82c3.eu-central-1.awsapprunner.com`
  - post-deploy live verification: `/api/operations/roy/live?refresh=1` returned both orders `2679000027` and `2679000026` as `fulfillable=True`, `fulfillment_reason=cod_waiting`; `/api/operations/roy/picking-lists.pdf?refresh=1&preview=1` returned a PDF containing both order numbers without marking them printed
  - UI verification: Browser smoke on `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy` showed `NA VYBAVENIE=25`, `16 online + 9 dobierka`, and both order numbers after live data loaded
  - Next exact step: use the live dashboard print action when ready; the preview verification did not mark these orders as printed

- VEVO/ROY invoice email automation change is implemented locally on `2026-06-03`:
  - branch: `codex/auto-email-created-invoices`; merged PR `#163` into `main` (`63a0f40`)
  - change: `invoice_generation.send_invoice_email` is explicit and enabled for VEVO and ROY; newly created invoices must be emailed through the BizniWeb invoice email action when the setting is enabled
  - implementation: invoice finalization now resolves invoice id from JSON/HTML response or by re-reading `getOrder(order_num) { invoices { id invoice_num } }`, then calls `/erp/orders/invoices/sendEmail/<invoice_id>` with AJAX headers
  - runner behavior: `invoice_runner.py` and the legacy daily-report invoice hook now publish email metrics and fail the run if a created invoice cannot be emailed or if the invoice id cannot be resolved
  - ECR refresh: Build and Push ECR run `26882007249` succeeded for merge commit `63a0f40`; `latest` digest `sha256:9a144775eedf3199d992f881dff2ca186e4787d681ef8493e4bd34c8db5c53eb`
  - smoke workflow addition: PR `#164` merged into `main` (`86b0d0b`) and added `.github/workflows/production-invoice-smoke.yml`, a GitHub Actions workflow that runs `invoice_runner.py --dry-run` on the real VEVO/ROY invoice ECS/Fargate schedule task definitions and verifies `curl http://127.0.0.1:8000/marker.json` with `INVOICE_SMOKE_MARKER_OK`
  - local verification: `python -m py_compile generate_invoices.py invoice_runner.py daily_report_runner.py`; `python -m json.tool projects/vevo/settings.json`; `python -m json.tool projects/roy/settings.json`; `python -m json.tool templates/reporting-client/settings.template.json`; `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`; `git diff --check`
  - workflow verification: `python scripts/security_ci.py`; YAML parse for `.github/workflows/production-invoice-smoke.yml`; `git diff --check`
  - production invoice smoke run `26882388082` succeeded with `project=all`, marker `invoice-email-dry-run-20260603`, and ECR latest digest `sha256:9a144775eedf3199d992f881dff2ca186e4787d681ef8493e4bd34c8db5c53eb`
    - VEVO hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.47.175`, service `vevo-daily-invoice-generation`, task definition `vevo-invoice-daily:2`, task `6d57340ef98a4b10bda67b45ced8ab33`, image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`, marker path `http://127.0.0.1:8000/marker.json`
    - VEVO marker/dry-run: `INVOICE_SMOKE_MARKER_OK`, window `2026-05-28..2026-06-03`, `send_invoice_email=true`, `matched=0`, `created=0`, `failed=0`, `emailed=0`, `email_failed=0`, `missing_invoice_ids=0`, `skipped_zero_total=0`
    - ROY hard-gate: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.19.251`, service `roy-daily-invoice-generation`, task definition `roy-invoice-daily:2`, task `3cc158ad01ce4905a8d1414a08d35ec4`, image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`, marker path `http://127.0.0.1:8000/marker.json`
    - ROY marker/dry-run: `INVOICE_SMOKE_MARKER_OK`, window `2026-05-28..2026-06-03`, `send_invoice_email=true`, `matched=0`, `created=0`, `failed=0`, `emailed=0`, `email_failed=0`, `missing_invoice_ids=0`, `skipped_zero_total=3`
  - production status: deployed via ECR `latest`; future scheduled invoice runs for VEVO and ROY will create invoices and require BizniWeb customer email sending when matching orders exist
  - Next exact step: monitor the next real scheduled VEVO/ROY invoice runs and confirm their summaries keep `failed=0`, `email_failed=0`, and `missing_invoice_ids=0`; if a real created invoice appears, verify BizniWeb shows the invoice email as sent for that order

- ROY personal pickup ready checkbox was implemented and deployed on `2026-06-02`:
  - change: ROY live dashboard personal pickup cards now have a separate `Pripravené k odberu` checkbox before the existing customer pickup/`Odoslaná` action
  - backend: new `POST /api/operations/roy/pickup/<order_num>/ready` action validates the order as a paid ROY personal pickup, then changes BiznisWeb status to `Pripravené k odberu` (`status_id=23`)
  - ship action: the existing `POST /api/operations/roy/pickup/<order_num>/ship` remains, but is now enabled only when the pickup is already in `Pripravené k odberu`; it changes the status to `Odoslaná` (`status_id=4`)
  - verified locally: `python -m json.tool projects/roy/settings.json`; `python -m unittest tests.test_roy_operations_dashboard tests.test_reporting_calculation_fixes tests.test_unpaid_order_cancellation`
  - merged PR `#161` into `main` (`ea4c4fd`); ECR `latest` refreshed by run `26818947940` with digest `sha256:ee3c11c129eb3febf8fc4fc58c990a3ae73e6e3d7bf35d5e6f36c5e1270af709`
  - ROY App Runner deploy run `26819093966` succeeded with `skip_artifact_refresh=true`:
    - Fargate hard-gate: IP `172.31.12.146`, service `roy-daily-report-email`, task def `roy-reporting-daily:30`, task `dc1a5e3042854360a97e49ab08bffc8b`, marker `LIVE_ARTIFACT_MARKER_OK`
    - App Runner hard-gate: service `biznisweb-roy-operations-dashboard`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`, image digest `sha256:ee3c11c129eb3febf8fc4fc58c990a3ae73e6e3d7bf35d5e6f36c5e1270af709`
    - App Runner smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=3:personal_pickups=1:inventory_alerts=19:inventory_rows=160:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=104818`
  - local post-deploy curl with SSM password was not possible from this Windows shell because local AWS credentials were unavailable; GitHub deploy smoke verified the production host with the configured credentials
  - Next exact step: use the live dashboard to mark a real paid personal pickup as `Pripravené k odberu`, then confirm BiznisWeb shows status `Pripravené k odberu` and the dashboard keeps the order available for the final `Odoslaná` pickup action

- ROY App Runner Basic Auth username was restored to the correct user-facing login `roy21` on `2026-06-02`:
  - symptom: browser Basic Auth prompt accepted/stored a wrong username while `/api/operations/roy/live` returned a plain auth challenge/error; the frontend then showed `Unexpected token 'A' ... is not valid JSON`
  - root cause: `Deploy Live Dashboard App Runner` accepted `auth_user` as a manual workflow input, wrote it directly to runtime `LIVE_DASHBOARD_AUTH_USER`, and then smoke-tested with the same input, so a wrong deploy username could pass verification
  - runtime fix: re-ran `Deploy Live Dashboard App Runner` with `auth_user=roy21` and `skip_artifact_refresh=true`, so report/S3 data were not regenerated
  - App Runner hard-gate run `26817149992`: service `biznisweb-roy-operations-dashboard`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`, image digest `sha256:7ebcb7bab80a22725c4ee46221efe38e68eecd24314936eda0cccbce91afc802`
  - marker/smoke verified: Fargate skip-refresh marker `LIVE_ARTIFACT_MARKER_OK`, App Runner host smoke `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=0:personal_pickups=0:inventory_alerts=19:inventory_rows=160:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=80171`, and `APP_RUNNER_DEPLOY_OK:biznisweb-roy-operations-dashboard:https://qvfzvh82c3.eu-central-1.awsapprunner.com`
  - prevention: ROY now has `projects/roy/settings.json` -> `live_dashboard.auth_user=roy21`; the App Runner deploy workflow uses that configured project value and ignores a mismatching manual `auth_user` input
  - prevention verified after merge by App Runner deploy run `26817925919`: workflow was intentionally started with wrong manual `auth_user=roy2`, logged `Using configured live dashboard auth user for roy; workflow input auth_user is ignored.`, used current image digest `sha256:6114f0ee901df9779d058945173a1df35439725bf69f88c45bdd82c7d152556b`, and passed smoke `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=1:personal_pickups=0:inventory_alerts=19:inventory_rows=160:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=88496`
  - latest hard-gate evidence for run `26817925919`: Fargate skip-refresh IP `172.31.39.48`, service `roy-daily-report-email`, task def `roy-reporting-daily:29`, marker `LIVE_ARTIFACT_MARKER_OK`; App Runner service `biznisweb-roy-operations-dashboard`, path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`, `APP_RUNNER_DEPLOY_OK`
  - Next exact step: use `roy21` for ROY live dashboard login; password is unchanged; do not change `live_dashboard.auth_user` unless the browser-facing username is intentionally changed

- ROY live operations dashboard inventory regression was fixed and deployed on `2026-06-02`:
  - root cause: `dashboard_payload_latest.json` is the full/all-time sidecar, and the all-time `roy_product_demand` payload can have `inventory_rows=0` / `alert_rows=0`; the live operations dashboard was using that payload for the stock tables
  - code now embeds a separate `dashboard.roy_operations_inventory` block into the main sidecar from the preferred rolling period report (`monthly`, then `weekly`, then `daily`) when that period has inventory rows
  - ROY operations inventory rendering now prefers `roy_operations_inventory` and falls back to `roy_product_demand`
  - workflow smoke now fails if ROY production operations inventory has no `inventory_rows`
  - verified locally: `python -m unittest tests.test_reporting_calculation_fixes tests.test_unpaid_order_cancellation tests.test_roy_inventory_model tests.test_roy_operations_dashboard tests.test_dashboard_modern`; simulated live inventory payload returned `inventory_rows=160` and `alert_rows=9`
  - merged PR `#156` into `main` (`82d192c`); ECR `latest` refreshed by run `26814207391` with digest `sha256:7ebcb7bab80a22725c4ee46221efe38e68eecd24314936eda0cccbce91afc802`
  - ROY production artifact refresh/App Runner deploy run `26814312615` succeeded:
    - Fargate hard-gate: IP `172.31.27.187`, service `roy-daily-report-email`, task def `roy-reporting-daily:28`, task `10b6c69202294ed7a17bd91b7c4bae19`, S3 latest `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`, marker `LIVE_ARTIFACT_MARKER_OK`
    - S3 artifact smoke: `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=251:inventory_alerts=23.0:inventory_rows=160`
    - App Runner hard-gate: service `biznisweb-roy-operations-dashboard`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`, image digest `sha256:7ebcb7bab80a22725c4ee46221efe38e68eecd24314936eda0cccbce91afc802`
    - App Runner smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=0:personal_pickups=0:inventory_alerts=19:inventory_rows=160:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=80171`
  - Next exact step: monitor the next scheduled ROY report and confirm the live dashboard continues to show non-empty inventory rows after the scheduled refresh

- ROY/VEVO realized revenue filtering was corrected on `2026-06-02` so shipped prepaid orders are not lost after the current BizniWeb status changes from `Platba online - zaplatené` to `Odoslaná`:
  - COD still counts only when payment is COD and status is `Čaká na vybavenie` or `Odoslaná`
  - prepaid/card/bank-transfer orders still count immediately in status `Platba online - zaplatené`
  - prepaid/card/bank-transfer orders now also count when current status is fulfilled (`Odoslaná`), because BizniWeb exposes only current status and older paid orders otherwise disappeared from rolling windows
  - order cache schema was bumped to `3` because cached order files contain already-filtered order lists and must be revalidated after this filter change
  - local no-cache ROY recompute for `2026-05-03..2026-06-01` verified `385` orders and `25 712.62 EUR` net revenue versus the stale production KPI `181` orders / `12 879.48 EUR`
  - merged PR `#154` into `main` (`42bec2a`), ECR `latest` refreshed by run `26804665958` with digest `sha256:c2dd3f89c4fef771d605229d38c71fab9155c21a8f4aa0848a53e1964787f6d0`
  - ROY production artifact refresh/App Runner deploy run `26804769280` succeeded:
    - Fargate hard-gate: IP `172.31.29.39`, service `roy-daily-report-email`, task def `roy-reporting-daily:27`, task `f080a5713d5b47949656ab956c97817c`, S3 latest `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`, marker `LIVE_ARTIFACT_MARKER_OK`
    - S3 artifact smoke: `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=251:inventory_alerts=0.0`
    - App Runner hard-gate: service `biznisweb-roy-operations-dashboard`, ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`, image digest `sha256:c2dd3f89c4fef771d605229d38c71fab9155c21a8f4aa0848a53e1964787f6d0`
    - App Runner smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=3:personal_pickups=1:inventory_alerts=0:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=111243`
- Env governance added for multi-PC workflow
- Pre-commit hook install script exists for Bash and PowerShell
- CI validates env contract and blocks tracked secret env files
- Repo-scoped `PROJECT_STATE.md` exists
- Bootstrap scripts now exist for macOS/Linux and Windows PowerShell
- VEVO report schedule `vevo-daily-report-email` is enabled for `01:00 Europe/Bratislava`
- VEVO production report task definition `vevo-reporting-daily:5` uses full-history runtime range from `2025-05-03` to `yesterday` and sets `REPORT_SKIP_INVOICES=true`
- ROY report schedule `roy-daily-report-email` is enabled for `01:30 Europe/Bratislava`
- ROY production report task definition `roy-reporting-daily:3` uses full-history runtime range from `2025-09-24` to `yesterday` and sets `REPORT_SKIP_INVOICES=true`
- VEVO/ROY daily profit-loss history is deployed in the shared modern reporting dashboard:
  - merged PR `#64` into `main` (`dad3f913e5bbe8789f2a214d19d822929f1e292e`)
  - ECR `latest` refreshed by `Build and Push ECR` run `26210072736` with digest `sha256:57ae5b73c83bcb83c8a58bd7c1395ce69e328e8631d42ca75c009650f7c6a1ce`
  - production-equivalent host smoke run `26211921297` verified both scheduled ECS/Fargate tasks with `curl localhost` marker and live UI smoke
  - scheduled reports from `2026-05-22` should include the daily plus/minus history: VEVO at `01:00 Europe/Bratislava`, ROY at `01:30 Europe/Bratislava`
- Reporting order cache revalidation is implemented for delayed payment/status changes:
  - orders from the last `14` days are always fetched fresh
  - orders `15..60` days old are refreshed when cache age reaches `7` days
  - orders `61..365` days old are refreshed when cache age reaches `30` days
  - older order days are refreshed when cache age reaches `90` days
  - production image digest `sha256:b94f7ee02c01d4cb1782cea89f8f9769533d7299d4f43055d279f66e598c53a4` was verified on VEVO and ROY Fargate host checks
- Invoice generation is separated from reporting in production:
  - VEVO invoice schedule `vevo-daily-invoice-generation` is enabled for `cron(0/15 6-23 * * ? *) Europe/Bratislava`; final same-day sweep `vevo-same-day-invoice-sweep` runs at `cron(58 23 * * ? *)`; both target `vevo-invoice-daily:2`
  - ROY invoice schedule `roy-daily-invoice-generation` is enabled for `cron(5/15 6-23 * * ? *) Europe/Bratislava`; final same-day sweep `roy-same-day-invoice-sweep` runs at `cron(59 23 * * ? *)`; both target `roy-invoice-daily:2`
  - invoice task definitions run `python invoice_runner.py --project <project>` on the production image, so runtime windows are computed in `Europe/Bratislava`
  - first real interval scheduled runs on `2026-05-07` exited with `failed=0`: VEVO task `9abd3ced7d1e4a418150e023ab47307a` created `3` invoices; ROY task `7d97e245ef8343ad826e00e406ca6207` matched `0` and skipped one zero-total order
  - strict same-day coverage is now polled through `23:58/23:59 Europe/Bratislava`; a status change after the final sweep would require a BizniWeb status-change webhook/event to be mathematically guaranteed on the same calendar day
  - `daily_report_runner.py` now defaults to report-only behavior; `invoice_runner.py` is the repo-local standalone invoice runner for production invoice schedules
  - `projects/vevo/settings.json` and `projects/roy/settings.json` keep separate project-owned `report_schedule` and `invoice_generation` schedule metadata
  - zero-total orders are excluded before invoice creation
  - invoice pagination now fetches newest orders first and stops once it passes the configured invoice window
  - invoice debug logging now redacts auth headers instead of printing API tokens
  - `2026-05-04` production catch-up generated all currently eligible missing invoices for `2026-05-01..2026-05-04`: VEVO `16/16`, ROY `15/15`, with post-run API audit showing `eligible_missing_invoice=0` for both projects
  - `2026-05-09` incident fix restricts invoice eligibility to exact configured shipped statuses only: VEVO/ROY `eligible_statuses = ["Odoslaná"]`; `Čaká na vybavenie` is not eligible
  - production invoice schedules are enabled again after the shipped-only fix was deployed and verified on ECR digest `sha256:b4bd1d16d0eb4ae14b7777761da93ce17c029507a0597c5d8cfff158751ecab6`
  - first real scheduled runs after the fix on `2026-05-09` exited with `failed=0`: VEVO task `3b8b8b50649f4688b80622c701e5cb58` matched `0`; ROY task `b8b4dbefa7c44ce78ad1aeb69a64a9ee` matched `0`
- Production schedule drift from the evening cadence was corrected on `2026-04-28`:
  - VEVO `21:00 Europe/Bratislava` -> `01:00 Europe/Bratislava`
  - ROY `21:30 Europe/Bratislava` -> `01:30 Europe/Bratislava`
  - ROY order `2677002371` was confirmed as eligible and remediated by a one-off production Fargate invoice catch-up
- VEVO task role CloudWatch metric policy now allows the active namespace `BizniswebReporting` (and keeps backward-compatible `VevoReporting`)
- Manual ECS production-equivalent run succeeded on `2026-04-03` with:
  - HTML report saved as `data/vevo/report_20250503-20260402.html`
  - SES delivery confirmed in CloudWatch logs
  - no remaining `PutMetricData` warning in the verified log stream
- Manual ROY ECS production-equivalent run succeeded on `2026-04-12` with:
  - HTML report saved as `data/roy/report_20250924-20260411.html`
  - SES delivery confirmed in CloudWatch logs
  - scheduler target updated to `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:2`
- Fixed `html_report_generator.py` period-switcher syntax so `Env Check` / `reporting_qa_smoke.py` pass again on GitHub Actions and on local Python 3.11.
- VEVO runtime now supports explicit `fixed_daily_cost`, and March 2026 verification confirms `CM3` now diverges from `CM2` once fixed overhead is applied (`fixed_daily_cost = 70 EUR`).
- VEVO modern dashboard now surfaces practical marketing decision metrics:
  - total marketing spend
  - spend / revenue
  - CM3 margin
  - CM3 per ad euro
  - CM2 -> CM3 drag
  - paid-day CM3 win rate
  - returning revenue share on paid days
  - best CM3 spend ranges
- VEVO spend-bucket effectiveness table now shows:
  - CM3 margin
  - returning revenue share
  - AOV
- CFO KPI helper no longer double-subtracts fixed costs when building company-margin views from `date_agg`.
- CFO KPI smoke coverage now validates layer mapping and window invariants:
  - `profit` must reconcile to post-ad profit ex fixed (`contribution_profit`)
  - `company_margin_with_fixed` secondary value must reconcile to CM3 / net profit
  - margin percentages must reconcile back to their absolute profit layers

## 6) Integration Notes (External Systems)

### Doklady
- Integration is API-level only
- Doklady remains system-of-record for accounting document state
- Canonical code repository: `Terem21/doklady-saas`
- Do not store Doklady runtime assumptions here beyond API contract references

### OpenClaw
- OpenClaw runs on separate infrastructure
- Any launcher/tunnel helper must live in the OpenClaw repo, not here
- This repo should only keep reporting-side integration notes, not server-specific local launcher paths

## 7) Current Risks / Gaps

- README is still primarily product/user oriented, not full operator documentation
- No formal API contract package yet for cross-project integrations
- No container/bootstrap parity check in CI yet
- Runtime/deploy docs for separate OpenClaw infra still belong in another repo and are not defined there yet
- Partial upstream failures (ads/weather/etc.) now surface explicit source-health metadata in HTML/CFO outputs and JSON sidecars; downstream email/ops policy still needs alert tightening.
- Main production HTML report now uses the modern dashboard shell.
- Standalone CFO HTML output was removed from the artifact contract and daily email flow; CFO KPI logic now lives only inside the main report.
- Daily SES email now attaches only the main HTML report.
- Legacy `__test` and `__test2` artifacts are no longer part of the active workflow.
- Env Check CI baseline now validates partial-data rendering in the active HTML layer (`html_report_generator.py` / `dashboard_modern.py`) instead of the retired daily runner rendering path.
- Production dashboard now keeps `Executive KPI deck` on its own `Daily / Weekly / Monthly` switch while the rest of the report uses a separate global analytics window switcher in the sidebar.
- Executive KPI deck now supports `All-time` alongside Daily / Weekly / Monthly in both VEVO and ROY reports, using the shared CFO KPI payload and the active modern dashboard shell.
- Period bundle generation is enabled for plain production reports, so the sidebar analytics switch now works outside of test-tag exports too.
- Shipping semantics are now normalized to net shipping in runtime config and dashboard labels, but CM taxonomy naming is still mixed between legacy labels and CM1/CM2/CM3 terms in some views.
- Full QA assertions are now computed into `data_quality` sidecars and surfaced in dashboard/email/CloudWatch.
- Lifecycle remains a proxy because BiznisWeb reporting still exposes only current/final status, not full order-status history.
- Segment CAC/payback is still incomplete as a hard metric because payment fees and order-level attribution are not modeled deeply enough for final B2B/B2C CAC claims.
- Vevo growth model blocks are now wired into the active dashboard shell:
  - direct vs assisted profitability
  - CRM funnel KPI layer
  - scent-size refill matrix
  - bundle recommender
  - promo / discount quality
- Lifecycle is now visible as an explicit proxy layer built from final statuses plus tracked excluded payment-failure orders.
- B2B/B2C analytics now expose CM-based unit economics instead of only a raw revenue/profit split.
- Product cost coverage QA is now active in source-health and the modern dashboard:
  - VEVO March 2026 export now passes with `0.00%` fallback revenue share after re-importing the April 2026 Excel costs and restoring title-first / alias-aware expense matching
  - ROY March 2026 export is `warning` because fallback coverage still touches 3.20% of item revenue and 6.26% of pre-ad item profit
- ROY inventory valuation is now live, but cost-value accuracy still depends on explicit product-expense mapping coverage:
  - live verification on `2026-04-15` reached `94.39%` of on-hand units and `87.81%` of retail-value exposure
  - unmapped inventory still exists, so cost-value totals must keep surfacing coverage metadata
- ROY full-history export now performs an extra live product-catalog pagination pass for inventory (`56` pages in the current catalog), so report runtime is materially higher than before the inventory layer
- Biznisweb inventory can expose negative or zero available quantities on active products; the report now flags those rows explicitly instead of crashing, but replenishment response policy is still an open product decision
- VEVO now resolves ambiguous shared-EAN fragrance SKUs by exact item label / compound key before identifier fallback, so Natural vs Premium 500ml/200ml lines no longer collapse onto the same cost.
- ROY now supports project-configured excluded order statuses for realized revenue filtering, so non-revenue final states can be removed without hardcoded edits in `export_orders.py`.
- ROY dashboard now exposes product-demand analytics in the active modern report:
  - growing products
  - declining products
  - product seasonality
  - product sales forecast from historical data
  - top brands by revenue
  - top brands by profit
- ROY dashboard now also exposes live Biznisweb inventory analytics in the active modern report:
  - current on-hand units by product
  - inventory value at mapped cost
  - inventory retail value
  - stock-risk watchlist with projected stockout dates
  - dead-stock candidates
  - inventory cost-coverage diagnostics
  - restock-priority scoring
  - revenue-at-risk rollups
  - inventory turns by brand / family
  - forecast backtest accuracy
- ROY inventory alert workflow is now wired through the active render/output path on the task branch:
  - `dashboard_payload.json` now carries actionable `alert_rows`
  - the modern HTML dashboard now renders a dedicated `Actionable inventory alerts` table
  - the daily email summary builder now reads the dashboard payload and appends a `SKLADOVE ALERTY` section with top reorder actions
- ROY operations dashboard performance/inbound workflow is live in production:
  - merged PR `#97` into `main` (`4f6ddaa2e3fc94c426fcda5e68aca3c1b7c880af`)
  - browser-smoke JS escape fix merged PR `#99` into `main` (`805d3c6bbf84e8eba1ff061550c7ad7edf1fbbe9`) and redeployed
  - App Runner service `biznisweb-roy-operations-dashboard` serves `/production/roy`
  - ROY operations dashboard Basic Auth username is `roy21`; password is held in the production secret, not git
  - live API returns top brands/products, loss-product warnings, inbound-stock state, and `auto_refresh_seconds = 90`
  - rendered production HTML now emits valid `cssEscape` JavaScript and renders inbound/loss-product controls from the live API payload
  - production S3 state write/read was smoke-tested through inbound save + clear on `/api/operations/roy/inbound/__codex_smoke__`
- ROY HC800 product-loss diagnosis/fix is live in production:
  - merged PR `#124` into `main` (`81bfa92`)
  - `Wachman HC800` has import code `16689` and confirmed purchase cost `13.70 EUR` ex VAT
  - historical local dashboard payload showed HC800 with positive gross profit (`5991.95 EUR`, `44.1%`) but negative post-fixed profit (`-3599.06 EUR`), so it looked loss-making where the UI labelled post-fixed profit as generic `Zisk`
  - `projects/roy/product_expenses.json` now maps import code `16689` directly to `13.7`, instead of relying only on legacy title hashes
  - ROY operations top brand/product tables now rank and display `gross_profit`/`gross_margin_pct`; `loss_product_rows` remains gross-loss only
  - production deploy workflow run `26515291098` succeeded on `2026-05-27`
  - hard-gate context: App Runner instance/IP `N/A`, service `biznisweb-roy-operations-dashboard`, service ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, path `/production/roy`, image digest `sha256:9392f103055338f87ed004d75bc3695eab32a1139911c91d94a6387adca91d9e`
  - host refresh verification: ECS/Fargate private IP `172.31.32.42`, service `roy-daily-report-email`, localhost marker `LIVE_ARTIFACT_MARKER_OK`, `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=245:inventory_alerts=22.0`
  - production API/UI smoke verified HC800 `sku=16689` with `gross_profit=5887.36`, `gross_margin_pct=43.3`, `hc800_in_loss_rows=0`, one remaining gross-loss row `Roy powerbanka 10000mAh`, and HTML labels `Hruby zisk` / `Hruba marza`
- ROY operations App Runner auth username was repaired after an incorrect manual deploy input:
  - `2026-05-27` regression source: ROY App Runner was redeployed with `auth_user=roy`, while the agreed ROY login is `roy21`
  - VEVO dashboard/service was not changed
  - redeploy workflow run `26516980643` reset `biznisweb-roy-operations-dashboard` to `auth_user=roy21`
  - production smoke verified `https://qvfzvh82c3.eu-central-1.awsapprunner.com/` returns `200` with `roy21:roy21`, includes `/production/roy`, and returns `401` for the old wrong `roy:roy21`
  - production smoke verified `/production/roy` returns `200` with `roy21:roy21` and contains marker `roy-operations-dashboard`
- ROY unpaid-order cancellation automation is live in production:
  - standalone runner `unpaid_order_cancellation_runner.py` changes stale unpaid bank-transfer/card orders to `Nezaplatená - zrušená objednávka`
  - ROY target status was verified through BizniWeb API as ID `74`
  - EventBridge Scheduler job `roy-unpaid-order-cancellation` is enabled for `cron(10 2 * * ? *) Europe/Bratislava`
  - production task definition `roy-unpaid-order-cancellation:3` uses ECR digest `sha256:be3e39f3184ef479d899fb97682792a998c72d30bc41cc7dcd8f2670629c8ac3`
  - production one-off execute run `26510343214` updated `11` stale unpaid orders and returned `failed_orders=0`
  - post-execute read-only dry-run on `2026-05-27` scanned `1500` ROY orders through `2026-01-29` and found `eligible_orders=0`
- ROY operations picking-list PDF export is live in production:
  - merged PR `#121` into `main` (`de1dcaa2aaea75ef31f24ed266c96e4e4330d497`)
  - deploy-smoke PR `#122` merged into `main` (`678a3a85d3e16980ba5bb4dc2d1ade620ec921c2`)
  - dashboard header now has one-click `Vysklad. PDF` download
  - endpoint `/api/operations/roy/picking-lists.pdf?refresh=1` generates one PDF with one picking list page per fulfillable order
  - PDF uses the same expanded order items as the operations dashboard, so configured bundle components are reflected in picking lists
  - local server smoke on `2026-05-27` returned `application/pdf`, download filename `roy-vyskladnovacie-listy-30-20260527-1429.pdf`, `%PDF-` header, and `167256` bytes
  - production App Runner service `biznisweb-roy-operations-dashboard` serves `/production/roy`
  - production deploy workflow run `26512874128` succeeded on `2026-05-27`
  - hard-gate context: App Runner instance/IP `N/A`, service ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, image digest `sha256:50e1453696f58df747e174d0e0c5e4969f20fe0bdc8a72ebcadea7f289525397`
  - host refresh verification: ECS/Fargate private IP `172.31.13.228`, service `roy-daily-report-email`, task definition `roy-reporting-daily:12`, localhost marker `LIVE_ARTIFACT_MARKER_OK`
  - production UI/API smoke verified `Vysklad. PDF`, `/api/operations/roy/picking-lists.pdf?refresh=1`, `%PDF-`, `application/pdf`, `Content-Disposition`, and `143923` PDF bytes for `32` fulfillable orders

## 8) Next Exact Step
- Monitor BizniWeb transient non-JSON order-list errors in the next scheduled VEVO/ROY reporting runs; if they keep recurring, harden pagination so repeated page failures fail the run explicitly instead of relying on opposite-direction fallback.
- VEVO corrected revenue logic is deployed in the `latest` image and will be used by the next `vevo-daily-report-email` run; run an untagged VEVO report/email task manually only if an immediate corrected VEVO email artifact is required before the next schedule.

## 9) Change Log

### 2026-05-27 (ROY HC800 gross-profit dashboard fix deployed)
- Investigated `Wachman HC800` in ROY operations dashboard:
  - import code `16689`
  - configured purchase cost should be `13.70 EUR` ex VAT
  - local payload had `gross_profit = 5991.95`, `gross_margin_pct = 44.1`, and `profit_with_fixed = -3599.06`
  - root cause of the confusing loss signal was post-fixed profit being displayed under generic `Zisk` in top product/brand widgets, while true product-loss logic is gross-profit based
- Changed implementation:
  - added exact import-code cost mapping `"16689": 13.7`
  - product/brand profit rankings now sort by `gross_profit`
  - ROY operations UI labels top product/brand profit columns as `Hrubý zisk` / `Hrubá marža`
  - live commercial snapshot enriches older product-profit rows with gross fields from revenue rows and sorts by gross profit defensively
- Verified locally:
  - `python -m py_compile export_orders.py dashboard_modern.py live_dashboard_server.py roy_operations_dashboard.py`
  - `python -m unittest tests.test_roy_inventory_model tests.test_roy_operations_dashboard tests.test_reporting_product_identity`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `git diff --check`
- Verified production:
  - PR `#124` merged to `main` (`81bfa92`)
  - ECR build run `26515037022` pushed digest `sha256:9392f103055338f87ed004d75bc3695eab32a1139911c91d94a6387adca91d9e`
  - App Runner deploy run `26515291098` succeeded for `biznisweb-roy-operations-dashboard`
  - App Runner hard-gate: instance/IP `N/A`, service ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, path `/production/roy`, image digest `sha256:9392f103055338f87ed004d75bc3695eab32a1139911c91d94a6387adca91d9e`
  - host refresh marker: private IP `172.31.32.42`, service `roy-daily-report-email`, localhost marker `LIVE_ARTIFACT_MARKER_OK`
  - deploy smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=33:personal_pickups=2:inventory_alerts=22.0:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=147175`
  - live API smoke: `Wachman HC800` (`sku=16689`) has `gross_profit=5887.36`, `gross_margin_pct=43.3`, `profit_with_fixed=-3669.53`, and `hc800_in_loss_rows=0`
  - live HTML smoke returned `200`, marker `roy-operations-dashboard`, gross-profit labels present, and old post-fixed loss label absent

### 2026-05-27 (ROY App Runner auth username repaired)
- Fixed a ROY-only production access regression:
  - cause: manual App Runner deploy input used `auth_user=roy` instead of the agreed `roy21`
  - service: `biznisweb-roy-operations-dashboard`
  - public base URL: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/`
  - ROY dashboard path: `/production/roy`
  - VEVO dashboard/service was not touched
- Verified before redeploy:
  - `/health` without auth returned `200`
  - `/` with `roy21:roy21` returned `401`
  - `/production/roy` with `roy21:roy21` returned `401`
  - `/production/roy` with the unintended `roy:roy21` returned `200`
- Redeployed production:
  - workflow run `26516980643`
  - project input `roy`
  - service input `biznisweb-roy-operations-dashboard`
  - auth user input `roy21`
  - image digest `sha256:9392f103055338f87ed004d75bc3695eab32a1139911c91d94a6387adca91d9e`
- Verified after redeploy:
  - App Runner hard-gate: instance/IP `N/A`, service ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`, health path `/health`, dashboard path `/production/roy`
  - host refresh marker: private IP `172.31.39.71`, service `roy-daily-report-email`, marker `LIVE_ARTIFACT_MARKER_OK`, `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=245:inventory_alerts=22.0`
  - deploy smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=33:personal_pickups=2:inventory_alerts=22.0:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=147175`
  - live public smoke: `/` with `roy21:roy21` returned `200` and contains `/production/roy`
  - live public smoke: `/production/roy` with `roy21:roy21` returned `200` and contains marker `roy-operations-dashboard`
  - live public smoke: `/api/operations/roy/live` with `roy21:roy21` returned `200`
  - live public smoke: `/` and `/production/roy` with old wrong `roy:roy21` now return `401`

### 2026-05-27 (ROY operations picking-list PDF export deployed)
- Added one-click PDF picking-list export to the ROY operations dashboard:
  - new module `roy_picking_lists_pdf.py`
  - new endpoint `/api/operations/roy/picking-lists.pdf?refresh=1`
  - new header button `Vysklad. PDF`
  - dependency `reportlab>=4.2.0`
  - Docker image now installs `fonts-dejavu-core` for Slovak/Czech diacritics in generated PDFs
  - App Runner deploy smoke now downloads and validates the combined PDF endpoint with production Basic Auth secrets
- Verified locally:
  - `python -m py_compile live_dashboard_server.py roy_picking_lists_pdf.py`
  - `python -m unittest tests.test_roy_picking_lists_pdf tests.test_roy_operations_dashboard tests.test_live_dashboard_auth`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `git diff --check`
- Local endpoint smoke:
  - `GET http://127.0.0.1:8790/api/operations/roy/picking-lists.pdf?refresh=1`
  - HTTP `200`, content type `application/pdf`
  - file header `%PDF-`
  - generated `30` picking lists into one PDF
- Production deploy and smoke:
  - PR `#121` merged the feature; PR `#122` merged the deploy smoke guard
  - ECR digest `sha256:50e1453696f58df747e174d0e0c5e4969f20fe0bdc8a72ebcadea7f289525397`
  - deploy workflow run `26512874128`, conclusion `success`
  - App Runner service `biznisweb-roy-operations-dashboard`
  - service URL `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - production PDF smoke returned `143923` bytes and verified `%PDF-`, `application/pdf`, and `Content-Disposition`
  - refresh task marker `LIVE_ARTIFACT_MARKER_OK`; S3 latest payload marker `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=245:inventory_alerts=22.0`

### 2026-05-27 (ROY unpaid-order cancellation automation staged)
- Added standalone stale unpaid order cancellation code:
  - `unpaid_order_cancellation.py`
  - `unpaid_order_cancellation_runner.py`
  - `tests/test_unpaid_order_cancellation.py`
- Added ROY config for bank-transfer/card unpaid orders older than `14` days:
  - target status `Nezaplatená - zrušená objednávka`
  - target status ID `74`
  - payment refs `6`, `17`, `18`, `11`, `20`
  - daily schedule metadata `roy-unpaid-order-cancellation`, `cron(10 2 * * ? *) Europe/Bratislava`
- Added GitHub Actions workflow `.github/workflows/deploy-unpaid-order-cancellation.yml` to register the ECS task definition, create/update the EventBridge Scheduler job, and verify a host dry-run marker at `http://127.0.0.1:8000/marker.json`.
- Follow-up workflow registration note:
  - the deploy workflow also has a `push` trigger scoped to `main` and only the unpaid-cancellation files so GitHub registers it and future automation-code changes refresh the scheduler through the same host-smoke path
  - the workflow has manual `execute_now=true` for an explicit one-off real run after the dry-run marker passes
- Verified locally:
  - `python -m py_compile unpaid_order_cancellation.py unpaid_order_cancellation_runner.py`
  - `python -m unittest tests.test_unpaid_order_cancellation`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `git diff --check`
- ROY local dry-run:
  - reference date `2026-05-27`
  - cutoff `2026-05-13`
  - scanned `1500` orders / `50` pages
  - oldest scanned order date `2026-01-29`
  - eligible orders `11`
- Code/deploy merged:
  - PR `#117`: core runner/config/deploy workflow, merge commit `5e00db0`
  - PR `#118`: workflow registration push trigger, merge commit `f2751c1`
  - PR `#119`: manual `execute_now` mode, merge commit `51a6de7`
  - main ECR build run `26510209844` succeeded
  - deployed image digest `sha256:be3e39f3184ef479d899fb97682792a998c72d30bc41cc7dcd8f2670629c8ac3`
- Production hard-gate / host verification:
  - dry-run deploy run `26509838383`: private IP `172.31.21.96`, service `roy-unpaid-order-cancellation`, task definition `roy-unpaid-order-cancellation:1`, marker `UNPAID_CANCELLATION_MARKER_OK`
  - post-update dry-run deploy run `26510209876` succeeded from `main`
  - execute run `26510343214`: private IP `172.31.17.70`, service `roy-unpaid-order-cancellation`, task definition `roy-unpaid-order-cancellation:3`, marker `UNPAID_CANCELLATION_EXECUTE_MARKER_OK`
  - execute summary: scanned `1500`, eligible `11`, updated `11`, failed `0`, cutoff `2026-05-13`
  - follow-up local read-only dry-run after execute: scanned `1500`, eligible `0`, target status ID `74`

### 2026-05-27 (ROY live dashboard new-order sound alert deployed)
- Code merged:
  - PR `#115`: `Add ROY new order sound alert`, merge commit `4c12249`
- ECR refresh:
  - workflow: `Build and Push ECR`
  - run: `26506485497`
  - image digest: `sha256:48e0e07ae5e30a180cd4461e3be50ffe8ca686adcdfa02b42bce6887d9275b63`
- Final ROY live dashboard deploy/refresh:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26506589260`
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.6.213`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:11`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/2e3a6091bb264dadb0085f25c3b54dc0`
  - image identifier: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:48e0e07ae5e30a180cd4461e3be50ffe8ca686adcdfa02b42bce6887d9275b63`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - latest artifact path: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
- Public App Runner verification:
  - `/health`, `/production/roy`, and `/api/operations/roy/live?refresh=1` returned HTTP `200`
  - `/production/roy` contains `soundToggleBtn`, `playNewOrderSound`, and `notifyAboutNewFulfillableOrders`
  - API marker: `roy-operations-dashboard`
  - auto-refresh: `90` seconds
  - current live summary showed `32` fulfillable orders
- Browser UI verification:
  - `/production/roy` loaded as `ROY Operations Dashboard`
  - visible header showed `Posledná aktualizácia 2026-05-27T11:13:22Z. Auto refresh 90s.`
  - sound toggle changed from `Zvuk vyp.` to `Zvuk zap.` after click, with `aria-pressed=true`
  - order table rendered `24` rows in the overview table
- Next exact step:
  - monitor the next real newly fulfillable ROY order after sound is enabled in the browser and confirm the audible alert fires on the following dashboard refresh

### 2026-05-27 (ROY live dashboard new-order sound alert)
- Branch: `codex/roy-new-order-sound-alert`
- Change:
  - added a ROY dashboard sound toggle for new fulfillable orders
  - generated the alert tone in-browser with Web Audio API, so no extra static asset is required
  - the dashboard stores the sound preference in browser `localStorage`
  - the first live payload only seeds the known fulfillable order numbers; sound is played only when a later refresh contains a new fulfillable `order_num`
  - because browsers require user activation for audio, the toggle arms sound on click and shows a waiting state until the browser allows playback
- Local verification:
  - `python -m py_compile live_dashboard_server.py roy_operations_dashboard.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_production_board`
  - rendered ROY operations dashboard inline script extracted from `build_roy_operations_dashboard_html("roy")` and checked with `node --check`
  - `python scripts\reporting_qa_smoke.py`
  - `git diff --check`
- Next exact step:
  - open/merge PR, wait for ECR build, deploy/refresh ROY App Runner, then verify `/production/roy`

### 2026-05-27 (ROY dashboard order bundle components deployed)
- Code merged:
  - PR `#113`: `Expand ROY dashboard bundle order items`, merge commit `6ff8427`
- ECR refresh:
  - workflow: `Build and Push ECR`
  - run: `26504547879`
  - image digest: `sha256:7cc4b8aa5c891acab2b641dec2c1f3e777e31147c463da6e4b21691a0aee5e34`
- Final ROY live dashboard deploy/refresh:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26504675800`
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.27.253`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:10`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/3353c3a7d6d940ff959eefc50ef36c5e`
  - image identifier: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:7cc4b8aa5c891acab2b641dec2c1f3e777e31147c463da6e4b21691a0aee5e34`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - latest artifact path: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
- Public App Runner verification:
  - `/health`, `/production/roy`, and `/api/operations/roy/live?refresh=1` returned HTTP `200`
  - API marker: `roy-operations-dashboard`
  - auto-refresh: `90` seconds
  - live order payload had `31` fulfillment orders, `1` personal pickup order, and `70` rendered order item rows
  - live order item rows had zero parent hits for `Set MACO STOP VEĽKÝ`, `Set proti medveďom VEĽKÝ`, and `Wachman Rio Solar 4G`
  - live order item rows included component hits such as `Puzdro MACO STOP na sprej 300ml`
  - live performance rows still had zero parent hits for the same bundle names and component hits including `Fotopasca Wachman Rio 4G`
- Browser UI verification:
  - `/production/roy` loaded as `ROY Operations Dashboard`
  - visible header showed `Posledná aktualizácia 2026-05-27T10:35:14Z. Auto refresh 90s.`
  - rendered order sections had `orderParentHits=[]` and component hits for MACO STOP spray, pouch, and bear bell
  - rendered performance/country sections had `performanceParentHits=[]` and component hits for `Fotopasca Wachman Rio 4G` and `Najsilnejší sprej na medvede MACO STOP Extreme 300ml hmla`
- Next exact step:
  - monitor the next scheduled ROY report and the next real bundle order; confirm bundle parent products stay absent while component units remain present

### 2026-05-27 (ROY dashboard order items expand bundle products to components)
- Branch: `codex/roy-dashboard-order-bundle-components`
- Change:
  - ROY operations order rows now reuse `product_component_expansion_rules` to display configured bundle products as pickable component items.
  - `Set MACO STOP VEĽKÝ` / `Set proti medveďom VEĽKÝ` display as MACO STOP 300ml hmla, 300ml pouch, and bear bell components.
  - `Wachman Rio Solar 4G` displays as `Fotopasca Wachman Rio 4G` plus `Univerzálne solárne napájanie BL8000 pre fotopascu`.
  - If a component is also present separately in the same order, order item quantities are merged by import code / EAN / warehouse number / label.
- Local verification:
  - `python -m py_compile roy_operations_dashboard.py live_dashboard_server.py export_orders.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_reporting_product_identity`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_reporting_product_identity`
  - `python scripts\reporting_qa_smoke.py`
  - `git diff --check`
- Next exact step:
  - open/merge PR, wait for ECR build, deploy/refresh ROY App Runner, then verify `/production/roy` order rows and product tables

### 2026-05-27 (ROY bundle products expand to reporting components deployed)
- Code merged:
  - PR `#111`: `Expand ROY bundle products into reporting components`, merge commit `6ad2166`
- ECR refresh:
  - workflow: `Build and Push ECR`
  - run: `26502412537`
  - image digest: `sha256:a3c9dcf6eae41249bfe169c0f060d80430ce5b835d0fd8a2e49b6aa142895eae`
- Final ROY live dashboard deploy/refresh:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26502538905`
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.35.80`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:9`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/0d06237522fc468f943fcd5219273e1e`
  - image identifier: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:a3c9dcf6eae41249bfe169c0f060d80430ce5b835d0fd8a2e49b6aa142895eae`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - latest artifact path: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
- Public App Runner verification:
  - `/health`, `/production/roy`, and `/api/operations/roy/live?refresh=1` returned HTTP `200`
  - API marker: `roy-operations-dashboard`
  - auto-refresh: `90` seconds
  - live product performance and country top-product payloads contain zero parent bundle rows for `Set MACO STOP VEĽKÝ`, `Set proti medveďom VEĽKÝ`, and `Wachman Rio Solar 4G`
  - component rows now appear in top-product and country rows, including `Fotopasca Wachman Rio 4G` and `Najsilnejší sprej na medvede MACO STOP Extreme 300ml hmla`
- Browser UI verification:
  - `/production/roy` loaded as `ROY Operations Dashboard`
  - visible header showed `Posledná aktualizácia 2026-05-27T09:52:23Z. Auto refresh 90s.`
  - rendered performance/country tables had no parent bundle hits and included component hits for `Fotopasca Wachman Rio 4G` and `Najsilnejší sprej na medvede MACO STOP Extreme 300ml hmla`
  - browser screenshot capture timed out in the in-app browser, but DOM/API verification passed
- Next exact step:
  - monitor the next scheduled ROY report and confirm bundle parent products stay absent from product performance rows while component units remain present

### 2026-05-27 (ROY bundle products expand to reporting components)
- Branch: `codex/roy-bundle-component-reporting`
- Change:
  - added ROY reporting product component expansion rules for `Set MACO STOP VEĽKÝ` / `Set proti medveďom VEĽKÝ`
  - added ROY reporting product component expansion rules for `Wachman Rio Solar 4G`
  - the canonical reporting item frame now replaces configured bundle rows with component rows while preserving total item revenue
  - component revenue is allocated by component cost weight, and component product expense is resolved through the normal ROY product-expense map
  - added import-code cost aliases for component SKUs `14832`, `12840`, `F_482`, `F_1472`, and `F_486`
  - added regression tests proving bundle parent SKUs are absent from product aggregations and component quantities/revenue remain correct
- Local cached-export verification:
  - `data/roy/export_20250922-20260526.csv` keeps total reporting revenue unchanged at `198681.13 EUR`
  - canonical reporting rows increase from `4659` to `4752` because bundle lines are expanded to components
  - bundle component parent labels matched: `Set MACO STOP VEĽKÝ`, `Set proti medveďom VEĽKÝ`, and `Wachman Rio Solar 4G`
- Local verification:
  - `python -m unittest tests.test_reporting_product_identity tests.test_roy_inventory_model`
  - `python -m py_compile export_orders.py dashboard_modern.py roy_operations_dashboard.py live_dashboard_server.py`
  - `python -m json.tool projects\roy\settings.json`
  - `python -m json.tool projects\roy\product_expenses.json`
  - `python -m unittest tests.test_reporting_product_identity tests.test_reporting_calculation_fixes tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_dashboard_modern`
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `git diff --check`
- Next exact step:
  - open/merge PR, wait for ECR build, deploy/refresh ROY App Runner live dashboard, then verify `/api/operations/roy/live?refresh=1`

### 2026-05-27 (ROY MACO STOP large-set cost aliases deployed)
- Code merged:
  - PR `#109`: `Fix ROY MACO STOP set cost aliases`, merge commit `75ff552`
- ECR refresh:
  - workflow: `Build and Push ECR`
  - run: `26500162403`
  - image digest: `sha256:088d31ba6f53a23e2ea0272a58ba131c8a2576aa7c52ff02b914a47c630af24f`
- Final ROY live dashboard deploy/refresh:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26500262307`
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.45.188`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:8`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/2fd79569a3c546e4b56c1327fb66228e`
  - image identifier: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:088d31ba6f53a23e2ea0272a58ba131c8a2576aa7c52ff02b914a47c630af24f`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - latest artifact path: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
- Public App Runner verification:
  - service name: `biznisweb-roy-operations-dashboard`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - `/health`, `/production/roy`, and `/api/operations/roy/live?refresh=1` returned HTTP 200
  - API marker: `roy-operations-dashboard`
  - auto-refresh: `90` seconds
  - current live loss table has `1` row, `Roy powerbanka 10000mAh`, and no MACO STOP set row
  - browser UI smoke confirmed the loaded dashboard shows `Produkty v strate` with `HRUBY ZISK/STRATA`, no `Zisk s fixom`, and no MACO STOP set in the loss section
- Next exact step:
  - monitor the next automatic scheduled ROY refresh and confirm the cost alias remains active in generated export source attribution

### 2026-05-27 (ROY MACO STOP large-set cost aliases)
- Branch: `codex/roy-maco-stop-set-cost`
- Change:
  - added ROY cost aliases `H-226DA29F` and `133652` at `26.58 EUR` net purchase cost for the large MACO STOP set
  - added resolver regression coverage so the set resolves at the same cost across hash SKU and import/warehouse-code variants
- Investigation:
  - cached ROY export showed `H-226DA29F` sold 41 units with `missing_cost_zero_margin_fallback`, causing zero gross margin in product reporting
  - at `26.58 EUR` per unit, the same cached rows reconcile to `1089.78 EUR` product cost and `988.47 EUR` gross profit on `2078.25 EUR` net revenue
- Local verification:
  - `python -m unittest tests.test_roy_inventory_model`
  - `python -m py_compile export_orders.py dashboard_modern.py roy_operations_dashboard.py live_dashboard_server.py`
  - `python -m unittest tests.test_reporting_product_identity tests.test_reporting_calculation_fixes tests.test_roy_operations_dashboard`
  - `python scripts\reporting_qa_smoke.py`
  - `git diff --check`
- Next exact step:
  - open/merge PR, wait for ECR build, deploy/refresh ROY App Runner live dashboard, then verify `/api/operations/roy/live?refresh=1`

### 2026-05-27 (Live dashboard refresh task pins current image)
- Branch: `codex/live-dashboard-refresh-image-pin`
- Context:
  - PR `#104` updated ROY loss-product logic and App Runner served the updated HTML, but the live API payload still showed a loss row without `gross_profit`.
  - The deploy workflow refreshed the S3 report artifact using the existing scheduled ECS task definition and only re-registered it when S3 env values changed, so the refresh task could run an older reporting image than App Runner.
- Change:
  - `Deploy Live Dashboard App Runner` now compares the scheduled ECS refresh task image with the current ECR image digest.
  - When the image differs, the workflow registers a new ROY reporting task definition pinned to the current image digest before running the refresh.
  - The refresh hard-gate log now prints the refresh task `image-identifier`.
- Local verification:
  - parsed `.github/workflows/deploy-live-dashboard-apprunner.yml` with `yaml.safe_load`
  - `git diff --check`
- Next exact step:
  - open/merge PR, wait for ECR build, rerun guarded ROY App Runner deploy, then verify loss rows have `gross_profit < 0`.

### 2026-05-27 (ROY loss products use gross profit only)
- Branch: `codex/roy-loss-products-gross-profit`
- Change:
  - ROY `loss_product_rows` are now selected only when product `gross_profit` / `cm1_profit` is negative.
  - Products that are profitable on gross margin but negative only after ads/fixed overhead are no longer included in `Produkty v strate`.
  - The live dashboard loss-products table now shows only `Hrubý zisk/strata` and `Hrubá marža`, not post-fixed profit.
  - Dashboard payload serialization now includes `gross_profit` and `gross_margin_pct` for loss-product rows.
- Local verification:
  - `python -m unittest tests.test_roy_inventory_model tests.test_roy_operations_dashboard tests.test_dashboard_modern`
  - `python -m py_compile export_orders.py dashboard_modern.py roy_operations_dashboard.py live_dashboard_server.py`
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_dashboard_modern tests.test_reporting_product_identity tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_inventory_model tests.test_roy_operations_dashboard`
  - rendered ROY operations dashboard script extracted from `build_roy_operations_dashboard_html("roy")` and checked with `node --check`
  - `git diff --check`
- Next exact step:
  - open/merge PR, wait for ECR build, rerun guarded ROY App Runner deploy, then verify live `/production/roy`.

### 2026-05-27 (ROY country performance renderer fix)
- Branch: `codex/roy-country-json-safe`
- Context:
  - PR `#101` added ROY country performance metrics to the live operations dashboard.
  - The first guarded deploy refresh run `26491693543` failed during production report generation because `dashboard_modern._json_safe()` called `pd.isna()` on nested `top_products` lists in `country_rows`.
- Change:
  - `dashboard_modern._json_safe()` now recursively serializes dictionaries, lists, tuples, sets, and array-like values before scalar missing-value handling.
  - `pd.NaT` and other missing scalar values remain serialized as JSON `null`.
  - Added regression coverage for nested country `top_products` rows.
- Local verification:
  - `python -m unittest tests.test_dashboard_modern tests.test_roy_inventory_model tests.test_roy_operations_dashboard`
  - `python -m py_compile dashboard_modern.py export_orders.py roy_operations_dashboard.py live_dashboard_server.py`
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_dashboard_modern tests.test_reporting_product_identity tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_inventory_model tests.test_roy_operations_dashboard`
  - `git diff --check`
  - rendered ROY operations dashboard script extracted from `build_roy_operations_dashboard_html("roy")` and checked with `node --check`
- Next exact step:
  - completed by the production verification entry below.

### 2026-05-27 (ROY country performance live verified)
- Code merged:
  - PR `#101`: country performance metrics, merge commit `febe9d5203cbb72b2a1fdefedc83f65c082522a7`.
  - PR `#102`: JSON-safe renderer fix, merge commit `44430c2f208aa10a7794b236f311c487d1b74749`.
- ECR build:
  - workflow: `Build and Push ECR`
  - run: `26492066552`
  - head SHA: `44430c2f208aa10a7794b236f311c487d1b74749`
  - image id: `sha256:9ec40973e9ad0ab436d43165caf99a8e50eb8548a0f64c12b761888408e0fdb1`
  - pushed `latest` digest: `sha256:80e903bc22d5e416b6f06d05523cbdeff12ab3198df7056cd58fc51324f931d1`
- Guarded ROY App Runner deploy:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26492135298`
  - conclusion: `success`
  - refresh hard-gate: `instance-id=N/A (scheduled ECS/Fargate task)`, private IP `172.31.2.221`, service `roy-daily-report-email`, task definition `roy-reporting-daily:4`, task ARN `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/b345f871f31547c1849b4df64c37d3f5`, marker path `http://127.0.0.1:8000/marker.json`
  - latest artifact path: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
  - localhost marker gate passed through the workflow (`LIVE_ARTIFACT_MARKER_OK`)
  - App Runner service: `biznisweb-roy-operations-dashboard`
  - App Runner service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production URL: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - API URL: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/api/operations/roy/live?refresh=1`
- Live verification:
  - `/health`, `/production/roy`, and `/api/operations/roy/live?refresh=1` returned HTTP `200`
  - rendered HTML contains `Executive KPI deck`, `Krajiny`, and `countryPerformanceBody`
  - API marker is `roy-operations-dashboard`, project is `roy`, refresh remains `90` seconds
  - `performance.country_rows` contains `8` countries
  - first country row `SK`: revenue `182660.75`, gross profit `86051.18`, spend `19279.69`, net profit `25510.49`
  - first SK top products include `WD0021` Wachman Discovery 4G, `F_1472` Fotopasca Wachman Rio 4G, and import code `12474` Fotopasca Wachman Solar Pro
- Next exact step:
  - monitor the next automatic ROY scheduled report refresh and verify `country_rows` stay populated without manual refresh.

### 2026-05-27 (ROY operations performance and inbound workflow)
- Branch: `codex/roy-dashboard-top-margin-workflow`
- Change:
  - ROY reporting payload now emits top product rows by revenue, top product rows by profit, and loss-product rows.
  - ROY live operations API now exposes top 3 brands by revenue/profit, top 10 products by revenue/profit, and loss-product warnings.
  - loss-product warnings can be acknowledged from the live dashboard and are persisted in ROY operations state.
  - low-stock products can be marked as ordered by entering ordered units and expected arrival date.
  - inbound ordered units are counted in the live dashboard stock-risk calculation, so sufficiently covered products are removed from reorder alerts.
  - inbound markers auto-clear after the next BiznisWeb stock increase for the SKU.
  - App Runner instance policy now grants state persistence access to `operations/*` under the live dashboard S3 artifact prefix.
- Local verification:
  - `python -m py_compile export_orders.py dashboard_modern.py roy_operations_dashboard.py live_dashboard_server.py daily_report_runner.py`
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `git diff --check`
- Production deploy:
  - PR: `#97` merged into `main` at `4f6ddaa2e3fc94c426fcda5e68aca3c1b7c880af`
  - ECR build workflow: `Build and Push ECR` run `26488950760`, conclusion `success`, digest `sha256:4d7f5f83c4bbe2046f170c5adec346fffc9b6eac64ddf09f0ba4eed1cb08d9b8`
  - deploy workflow: `Deploy Live Dashboard App Runner` run `26489012030`, conclusion `success`
  - App Runner service: `biznisweb-roy-operations-dashboard`
  - App Runner service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - public URL: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - API path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/api/operations/roy/live?refresh=1`
  - App Runner instance/IP: `N/A (AWS App Runner managed service)`
  - image path: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
- Production hard-gate / smoke:
  - scheduled ROY refresh task hard-gate from deploy run: `instance-id=N/A (scheduled ECS/Fargate task)`, private IP `172.31.39.8`, service `roy-daily-report-email`, task definition `roy-reporting-daily:4`, marker path `http://127.0.0.1:8000/marker.json`
  - live HTML smoke: `/health = 200`, `/production/roy = 200`, marker `roy-operations-dashboard`, `Executive KPI deck`, `Top znacky`, `Top produkty`, `Produkty v strate`, and inbound controls present
  - live API smoke: marker `roy-operations-dashboard`, brand revenue/profit rows `3/3`, product revenue/profit rows `10/10`, loss-product rows `58`, inbound order count `0`, actionable stock alerts `20`, refresh `90` seconds
  - live state smoke: inbound save returned `ok=true` with `storage=s3`; inbound clear returned `ok=true`, `removed=true`, `storage=s3`
- Follow-up browser-smoke fix:
  - browser smoke found rendered production JS syntax failure in the `CSS.escape` fallback; root cause was Python string escaping producing `replace(/["\]/g, ...)`
  - fix PR `#99` merged into `main` at `805d3c6bbf84e8eba1ff061550c7ad7edf1fbbe9`
  - local verification: `python -m py_compile live_dashboard_server.py roy_operations_dashboard.py`; `python -m unittest tests.test_roy_operations_dashboard`; `python -m unittest tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard`; `git diff --check`
  - ECR build workflow: `Build and Push ECR` run `26490106355`, conclusion `success`, digest `sha256:fd36d882a103b7202af706a302f11464ef61cf132da1ec4fece8a3d119d40162`
  - deploy workflow: `Deploy Live Dashboard App Runner` run `26490167572`, conclusion `success`, head SHA `805d3c6bbf84e8eba1ff061550c7ad7edf1fbbe9`
  - scheduled ROY refresh task hard-gate from deploy run: `instance-id=N/A (scheduled ECS/Fargate task)`, private IP `172.31.23.94`, service `roy-daily-report-email`, marker path `http://127.0.0.1:8000/marker.json`
  - final live smoke: `/health = 200`, `/production/roy = 200`, rendered HTML has valid `replace(/["\\]/g, '\\$&')` and no invalid regex; API marker `roy-operations-dashboard`
  - final render smoke with production HTML + production API: inbound controls `20`, brand rows `3`, product rows `10`, loss rows `58`, loss acknowledgements `58`, auto refresh `90s`, no rendered message error
  - final S3 state smoke: inbound save returned `ok=true` with `storage=s3`; inbound clear returned `ok=true`, `removed=true`, `storage=s3`
- Next exact step:
  - monitor next scheduled ROY refresh and verify inbound auto-clear after the first real restock.

### 2026-05-27 (ROY Facebook spend audit)
- Checked ROY Facebook Ads source path:
  - financial/reporting spend comes from Meta account-level daily insights via `FacebookAdsClient.get_daily_spend()`;
  - the modern dashboard `Daily spend, clicks and impressions` chart uses `fb_detailed_metrics` from Meta account-level insights.
- Live Meta API read-only check for account `act_1374274514249768`:
  - `2026-05-18..2026-05-23`: Facebook spend `0.00 EUR`, no campaign rows.
  - `2026-05-25`: Facebook spend `4.76 EUR`.
  - `2026-05-26`: Facebook spend `14.12 EUR`.
  - campaign/adset/ad source: `SK-Sale-Fotopasce-ACQ` / `SK-Interest` / `SK-Fotopasce-4G-Video-V1`, all reported `ACTIVE`.
- Found a dashboard visualization issue: `fb_daily` payload only included dates returned by Meta, so zero-spend days were omitted from the Facebook delivery chart instead of plotted as `0`.
- Added a code fix on branch `codex/roy-fb-spend-zero-fill` to zero-fill missing dates in `dashboard_modern.py`, with a unit test in `tests/test_dashboard_modern.py`.
- Verified locally:
  - `python -m unittest tests.test_dashboard_modern`
  - `python -m py_compile dashboard_modern.py`
  - `python -m unittest discover -s tests`

### 2026-05-26 (ROY App Runner live dashboard deployed)
- Merged deployment fix PRs into `main`:
  - PR `#81`: S3 latest artifact provisioning / refresh workflow.
  - PR `#82`: `REPORT_S3_BUCKET` / `REPORT_S3_PREFIX` ECS secret-vs-env conflict fix.
  - PR `#83`: deploy-time artifact validation aligned to the actual ROY dashboard payload contract.
- Successful deployment:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26456807209`
  - result: `success`
  - App Runner service name: `biznisweb-roy-operations-dashboard`
  - App Runner service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - public URL: `https://qvfzvh82c3.eu-central-1.awsapprunner.com`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - API path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/api/operations/roy/live?refresh=1`
  - image: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - image digest: `sha256:47b5b883000827514df4173eb412224068c22b7f120f513806214db2c3cfe1a8`
  - latest artifact path: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
- ECS/Fargate hard-gate context from the successful artifact refresh:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.45.147`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:4`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/faab2d506f9e4bbcaef550cda3a68b47`
  - localhost marker path: `http://127.0.0.1:8000/marker.json`
  - S3 latest artifact verification passed inside the deploy workflow.
- Public verification:
  - `/health` returned OK.
  - unauthenticated `/production/roy` returned HTTP `401`.
  - authenticated `/production/roy` returned `roy-operations-dashboard`, `Executive KPI deck`, and `Osobné odbery` markers.
  - authenticated `/api/operations/roy/live?refresh=1` returned:
    - `fulfillable_orders=55`
    - `personal_pickups=1`
    - `auto_refresh_seconds=90`
    - `kpi_months=9`
    - `inventory_alerts=24`
  - browser UI smoke verified clean Basic Auth URL loading, month switch to `2026-04`, and `Sklad` tab rendering with inventory cost and retail values.
  - browser screenshot capture timed out through CDP twice, but DOM/UI and API verification passed.
- Operational conclusion:
  - ROY live operations dashboard is available through App Runner with Basic Auth.
  - daily ROY reporting schedule now targets task definition revision `roy-reporting-daily:4`, which writes latest dashboard artifacts to S3 for the live dashboard.

### 2026-05-26 (ROY App Runner S3 artifact deploy fix)
- Branch: `codex/roy-live-dashboard-s3-fix`
- Context:
  - PR `#80` was merged to `main` as `32ba4e05a86cb909253b42ca1e5f5f9157dd012c`.
  - ECR build run `26453851082` succeeded after the merge.
  - App Runner deploy run `26453973092` created/updated service `biznisweb-roy-operations-dashboard`, but failed smoke because the runtime had no ROY latest KPI artifact: `Executive KPI windows missing`.
  - First retry run `26455029828` failed before ECS refresh because the existing ROY task definition had `REPORT_S3_BUCKET` as a container secret and the workflow also added it as an environment variable.
  - Second retry run `26455259337` reached ECS/Fargate refresh and logged hard-gate context (`private-ip=172.31.35.172`, task definition `roy-reporting-daily:4`, S3 latest artifact path), but the refresh task exited `1` because the workflow marker assertion checked obsolete payload keys (`dashboard.executive_kpis` / `dashboard.inventory_model`) instead of the actual ROY dashboard contract.
  - Hard-gate context from the failed deploy:
    - instance-id: `N/A (AWS App Runner managed service)`
    - private IP: `N/A (AWS App Runner managed service)`
    - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
    - public URL: `https://qvfzvh82c3.eu-central-1.awsapprunner.com`
    - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
    - artifact path was invalid: `s3:///daily-reports/roy-sk/latest/`
- Fix in progress:
  - deploy workflow now resolves the scheduled ROY reporting task definition from EventBridge Scheduler before deployment.
  - if no S3 bucket is configured, ROY deploy defaults to `biznisweb-reporting-artifacts-919341186960-eu-central-1`.
  - workflow ensures the bucket exists with public access blocked and AES256 server-side encryption enabled.
  - workflow grants the ROY reporting task role `s3:GetObject` / `s3:PutObject` access under `daily-reports/roy-sk/*`.
  - workflow registers a new ROY reporting task definition revision with `REPORT_S3_BUCKET` and `REPORT_S3_PREFIX` when needed, then updates the daily schedule target to that revision.
  - task definition mutation removes conflicting `REPORT_S3_BUCKET` / `REPORT_S3_PREFIX` container secrets before writing explicit environment variables.
  - artifact marker validation now checks the same payload keys used by the ROY operations runtime: `dashboard.kpis`, `dashboard.series`, and `dashboard.roy_product_demand`.
  - before App Runner smoke, workflow runs a one-off ECS/Fargate ROY report refresh, verifies a localhost marker in the task logs, verifies S3 `latest/dashboard_payload_latest.json`, and asserts KPI windows/months plus inventory summary.
- Local verification:
  - YAML parse for `.github/workflows/deploy-live-dashboard-apprunner.yml`
  - extracted deploy Bash script passes `bash -n`
  - `python -m unittest`
  - `python scripts\security_ci.py`
  - `git diff --check`
- Next exact step:
  - push the fix branch, open/merge PR, rebuild ECR if needed, rerun the ROY App Runner deploy workflow, then verify the live URL with Basic Auth `roy21`.

### 2026-05-26 (ROY operations dashboard implementation)
- Branch: `codex/roy-live-dashboard`
- Draft PR: `https://github.com/vzeman/biznisweb/pull/80`
- Added a dedicated ROY operations dashboard behind `/production/roy`:
  - Executive KPI deck is rendered from the existing ROY reporting payload.
  - Daily / Weekly / Monthly / All-time KPI windows remain available.
  - calendar month switching is generated from the reporting time series.
  - the page auto-refreshes from live data every `90` seconds.
- Added live ROY fulfillment data:
  - `/api/operations/roy/live` fetches open BiznisWeb orders directly.
  - fulfillable orders include `Platba online - zaplatené` and `Čaká na vybavenie` only when the payment price element is dobierka/dobírka.
  - the public payload excludes customer PII.
- Added personal pickup handling:
  - personal pickup is detected from shipping `Osobný odber na sklade` / shipping id `11`.
  - pickup rows render a checkbox action.
  - `POST /api/operations/roy/pickup/<order_num>/ship` validates that the order is a ROY personal pickup in an allowed status, then changes BiznisWeb status to `Odoslaná` (`status_id=4`).
- Added ROY inventory dashboard surfaces:
  - home alerts use existing reporting inventory alert rows.
  - stock tab exposes inventory cost value without VAT, retail value without VAT, risk rows, projected stockout date, reorder date, suggested reorder units, and lead-time context.
- Updated ROY inventory lead times:
  - Wachman: `30` working days.
  - ROY: `30` working days.
  - MACO STOP: `12` working days.
  - SD / memory storage family: `3` working days.
  - added family-level lead-time fallback for memory storage products.
- App Runner workflows:
  - deploy workflow is now project-aware instead of VEVO-only.
  - ROY can use project-specific Basic Auth secret `ROY_LIVE_DASHBOARD_AUTH_PASSWORD`.
  - App Runner can read latest reporting artifacts from S3 for the KPI/inventory payload.
  - ROY deploy smoke asserts `/production/roy`, `roy-operations-dashboard`, KPI windows/months, inventory summary, and live order payload.
  - ECR build workflow now tracks `roy_operations_dashboard.py` and runs `tests.test_roy_operations_dashboard`.
- Local verification:
  - `python -m py_compile live_dashboard_server.py roy_operations_dashboard.py export_orders.py`
  - `python -m unittest`
  - `python scripts\security_ci.py`
  - `python scripts\reporting_qa_smoke.py`
  - `git diff --check`
  - YAML parse for updated workflows
  - extracted App Runner deploy Bash script passes `bash -n`
  - local `/production/roy` returned the `roy-operations-dashboard` marker, `Executive KPI deck`, and `Osobné odbery`
  - local `/api/operations/roy/live?refresh=1` returned live ROY data with `fulfillable_orders=55`, `personal_pickups=1`, `kpi_months=8`, and inventory alert data
  - in-app browser smoke verified KPI month switching, inventory tab rendering, and no console errors
- Known issues / notes:
  - in-app browser screenshot capture timed out through CDP, but DOM/UI verification passed.
  - PR checks are green: `env-check`, `secret-scan`, `security-baseline`, `observability-baseline`.
  - GitHub Actions secret `ROY_LIVE_DASHBOARD_AUTH_PASSWORD` was set on `2026-05-26`; the requested runtime username/password is `roy21` / secret-backed password value.
  - production deploy is not done yet in this branch.
  - ROY App Runner deployment needs `ROY_LIVE_DASHBOARD_AUTH_PASSWORD` set to the intended password and an S3 bucket/prefix that exposes `dashboard_payload_latest.json` under `latest/`.
- Next exact step:
  - review/merge PR `#80`, build the ECR image, then deploy App Runner service `biznisweb-roy-operations-dashboard` with path `/production/roy` and Basic Auth user `roy21`.

### 2026-05-11
- Implemented smart reporting cache revalidation so delayed payment/status changes are not stuck in older daily cache buckets:
  - `REPORT_ALWAYS_REFRESH_DAYS=14`
  - `REPORT_WEEKLY_REFRESH_DAYS=60`
  - `REPORT_MONTHLY_REFRESH_DAYS=365`
  - `REPORT_OLD_CACHE_TTL_DAYS=90`
- Added regression coverage for always-fresh recent order days, weekly revalidation, monthly revalidation, and older-history revalidation.
- Local verification:
  - `python -m unittest tests.test_reporting_calculation_fixes tests.test_invoice_generation`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `git diff --check`
- Deployment hard-gate context before production rollout:
  - instance-id/IP: `N/A` until ECS/Fargate runtime task is started
  - service/schedule names: `vevo-daily-report-email`, `roy-daily-report-email`
  - task definitions: `vevo-reporting-daily:5`, `roy-reporting-daily:3`
  - image path: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - marker path for host verification: `http://127.0.0.1:8000/marker.json`
- Merged PR `#62` into `main`:
  - merge commit: `bc297a91117dcef85f53d0e56a10fa9ba8d80a1f`
  - guarded build run: `25650565685`
  - result: `success`
  - ECR `latest` digest: `sha256:b94f7ee02c01d4cb1782cea89f8f9769533d7299d4f43055d279f66e598c53a4`
- Ran production-equivalent host checks on the merged image:
  - VEVO task `ca5970a4389243c5a92ece3a39f68758`, private IP `172.31.8.131`, exit `0`, marker `LOCALHOST_MARKER_OK:vevo:cache-revalidation-check`, cache policy `fresh <= 14d; 7d TTL for 15-60d; 30d TTL for 61-365d; 90d TTL after 365d`
  - ROY task `67de23deaa6b450fbbb0f71b73ac9a8b`, private IP `172.31.9.25`, exit `0`, marker `LOCALHOST_MARKER_OK:roy:cache-revalidation-check`, cache policy `fresh <= 14d; 7d TTL for 15-60d; 30d TTL for 61-365d; 90d TTL after 365d`
- Ran post-host UI smoke on the same production image:
  - task `fae91ab1d9a749eda4661803483ab028`, private IP `172.31.29.164`, exit `0`
  - verified `/health`, `/dashboard/vevo?period=full`, and `/dashboard/roy?period=full` via localhost curl/grep
  - marker `UI_SMOKE_OK:live-dashboard-shell`

### 2026-05-09
- Investigated accidental invoice creation for orders that were not shipped yet.
- Confirmed immediate hard-gate runtime context:
  - instance-id/IP: `N/A` because invoice automation runs on scheduled ECS/Fargate tasks
  - service/schedule names: `vevo-daily-invoice-generation`, `vevo-same-day-invoice-sweep`, `roy-daily-invoice-generation`, `roy-same-day-invoice-sweep`
  - task definitions: `vevo-invoice-daily:2`, `roy-invoice-daily:2`
  - image path: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - log paths: `/ecs/vevo-invoice-daily`, `/ecs/roy-invoice-daily`
- Temporarily disabled all four production invoice schedules to stop further invoice creation during the fix.
- Root cause:
  - `_status_matches_invoice_generation(...)` used substring logic: `odoslan` OR (`cak` AND `vybaven`)
  - that made `Čaká na vybavenie` invoice-eligible even though it is not a shipped status
  - tests also incorrectly asserted `Čaká na vybavenie` as eligible
- Corrected the source-of-truth rule:
  - invoice generation now uses exact normalized allow-list matching
  - VEVO and ROY project settings set `invoice_generation.eligible_statuses = ["Odoslaná"]`
  - `Čaká na vybavenie`, `Čaká na úhradu`, expired online payments, and composite statuses like `madfrog stara odoslana` are not eligible
- Local verification before deploy:
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `git diff --check`
  - local live API dry-runs for `2026-05-07..2026-05-09` returned `matched=0` for both VEVO and ROY with the shipped-only rule
- Merged PR `#60` into `main`:
  - merge commit: `96a67ba8ffdab50c2a87f7ccfe3e3292c3c0f640`
  - guarded main build run: `25590961380`
  - result: `success`
  - ECR `latest` digest after main build: `sha256:b4bd1d16d0eb4ae14b7777761da93ce17c029507a0597c5d8cfff158751ecab6`
- Ran production Fargate dry-run host checks on the merged `main` image:
  - VEVO task `6c68d0a148b74b6cab61c9614750ea30`, private IP `172.31.28.102`, marker `LOCALHOST_MARKER_OK:vevo:shipped-only-invoice-check`, `STATUS_ALLOWLIST_OK`, dry-run `matched=0 created=0 failed=0 skipped_zero_total=0`
  - ROY task `372a7b7950b146fe815f76f38b879699`, private IP `172.31.14.217`, marker `LOCALHOST_MARKER_OK:roy:shipped-only-invoice-check`, `STATUS_ALLOWLIST_OK`, dry-run `matched=0 created=0 failed=0 skipped_zero_total=0`
- Re-enabled all four production invoice schedules after verification:
  - `vevo-daily-invoice-generation`: `ENABLED`, `cron(0/15 6-23 * * ? *)`, target `vevo-invoice-daily:2`
  - `vevo-same-day-invoice-sweep`: `ENABLED`, `cron(58 23 * * ? *)`, target `vevo-invoice-daily:2`
  - `roy-daily-invoice-generation`: `ENABLED`, `cron(5/15 6-23 * * ? *)`, target `roy-invoice-daily:2`
  - `roy-same-day-invoice-sweep`: `ENABLED`, `cron(59 23 * * ? *)`, target `roy-invoice-daily:2`
- Verified first real scheduled interval runs after re-enable:
  - VEVO task `3b8b8b50649f4688b80622c701e5cb58`, private IP `172.31.37.70`, image digest `sha256:b4bd1d16d0eb4ae14b7777761da93ce17c029507a0597c5d8cfff158751ecab6`, exit `0`, summary `matched=0 created=0 failed=0 skipped_zero_total=0`
  - ROY task `b8b4dbefa7c44ce78ad1aeb69a64a9ee`, private IP `172.31.40.89`, image digest `sha256:b4bd1d16d0eb4ae14b7777761da93ce17c029507a0597c5d8cfff158751ecab6`, exit `0`, summary `matched=0 created=0 failed=0 skipped_zero_total=1`

### 2026-05-07
- Re-investigated VEVO/ROY invoice generation after another missing-invoice report.
- Confirmed hard-gate runtime context:
  - instance-id/IP: `N/A` because invoice automation runs on scheduled ECS/Fargate tasks, not a fixed EC2 host
  - service/schedule names: `vevo-daily-invoice-generation`, `roy-daily-invoice-generation`
  - task definitions: `vevo-invoice-daily:1`, `roy-invoice-daily:1`
  - image path: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - log paths: `/ecs/vevo-invoice-daily`, `/ecs/roy-invoice-daily`
- Confirmed the root causes:
  - schedulers were not down; CloudWatch showed successful runs on `2026-05-04`, `2026-05-05`, and `2026-05-06`, with created invoices and `failed=0`
  - orders continued becoming invoice-eligible after the previous `20:00/20:30` run times
  - `_status_matches_invoice_generation(...)` did not normalize Slovak diacritics, so `Čaká na vybavenie` did not match the intended `cak` + `vybaven` rule
- Fixed and verified invoice eligibility:
  - added diacritic normalization before invoice status matching in `generate_invoices.py`
  - added regression coverage for `Odoslaná`, `Čaká na vybavenie`, `Čaká na úhradu`, and expired online payments
  - local dry-run after the fix matched VEVO `8` and ROY `9`, including `Čaká na vybavenie`
- Moved production invoice schedules later in the day:
  - VEVO `cron(0 20 * * ? *)` -> `cron(0 23 * * ? *)`
  - ROY `cron(30 20 * * ? *)` -> `cron(30 23 * * ? *)`
  - confirmed both EventBridge Scheduler jobs remain `ENABLED`
- Verified locally before deploy:
  - `python -m py_compile generate_invoices.py invoice_runner.py daily_report_runner.py`
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
- Deployed refreshed production image through guarded GitHub Actions workflow:
  - workflow: `Build and Push ECR`
  - run: `25492945637`
  - branch: `codex/invoice-catchup-state-20260504`
  - result: `success`
  - ECR `latest` digest: `sha256:b44976b106aa1ca5f2e7daf849be3204c51869a1cc6f6fe935425f27fa781831`
- Merged PR `#56` into `main`:
  - merge commit: `1d4001f5bed185616147b21b77ac08c53f9f6b31`
  - guarded main build run: `25493503184`
  - result: `success`
  - ECR `latest` digest after main build: `sha256:2d88f5e9089ad5bd8582d1cf7ffc2569098b6ed793704515b99c74a6980ec065`
- Ran production Fargate dry-run diagnostics on the refreshed image with direct host marker verification:
  - VEVO dry-run task `bf08ecb50f4443d793ae709818b177bd`, private IP `172.31.1.212`, marker `LOCALHOST_MARKER_OK:vevo:invoice-fix-dry-run`, matched `8`, failed `0`
  - ROY dry-run task `5457ec4f1c334af488dcec769d4aa958`, private IP `172.31.4.43`, marker `LOCALHOST_MARKER_OK:roy:invoice-fix-dry-run`, matched `9`, failed `0`
- Ran production Fargate invoice catch-up for `2026-05-04..2026-05-07`:
  - VEVO catch-up task `1b28857e31434fbe80b06484a8d7c149`, private IP `172.31.46.210`, marker `LOCALHOST_MARKER_OK:vevo:invoice-fix-catchup`, created `8`, failed `0`
  - ROY catch-up task `dd1c5cd25f814c6a844e822dc724f303`, private IP `172.31.44.51`, marker `LOCALHOST_MARKER_OK:roy:invoice-fix-catchup`, created `9`, failed `0`
- Verified live BizniWeb API after catch-up:
  - VEVO `eligible_missing_invoice=0` for `2026-05-04..2026-05-07`
  - ROY `eligible_missing_invoice=0` for `2026-05-04..2026-05-07`
  - remaining missing invoices are only noneligible statuses: expired/declined online payments, `Storno`, `Čaká na úhradu`
- Ran final production host checks on the merged `main` image:
  - VEVO final check task `410cf5e1be9749f3902f3f2c7a5243b0`, private IP `172.31.36.180`, marker `LOCALHOST_MARKER_OK:vevo:main-image-final-check`, status matcher `STATUS_MATCH_OK`, dry-run `matched=0`
  - ROY final check task `42fe785d126e4e80b25794215a382c21`, private IP `172.31.34.112`, marker `LOCALHOST_MARKER_OK:roy:main-image-final-check`, status matcher `STATUS_MATCH_OK`, dry-run `matched=0`
- Hardened same-day invoice automation after the stricter requirement that all invoices must be created on the shipment day:
  - updated Git-backed VEVO/ROY schedule metadata to interval polling plus final same-day sweeps
  - registered `vevo-invoice-daily:2` and `roy-invoice-daily:2`, both running `python invoice_runner.py --project <project>`
  - updated EventBridge Scheduler:
    - `vevo-daily-invoice-generation`: `cron(0/15 6-23 * * ? *) Europe/Bratislava`
    - `vevo-same-day-invoice-sweep`: `cron(58 23 * * ? *) Europe/Bratislava`
    - `roy-daily-invoice-generation`: `cron(5/15 6-23 * * ? *) Europe/Bratislava`
    - `roy-same-day-invoice-sweep`: `cron(59 23 * * ? *) Europe/Bratislava`
  - verified schedule targets directly in AWS: VEVO schedules target `vevo-invoice-daily:2`; ROY schedules target `roy-invoice-daily:2`
  - ran production Fargate dry-run host checks with direct localhost markers:
    - VEVO task `d1a37ed83f3f4858854545fe121f44df`, private IP `172.31.44.249`, marker `LOCALHOST_MARKER_OK:vevo:invoice-runner-schedule-check`, `STATUS_MATCH_OK`, exit `0`
    - ROY task `5f72fc0b5a7d48e68b4a96dfe5e9c30d`, private IP `172.31.20.69`, marker `LOCALHOST_MARKER_OK:roy:invoice-runner-schedule-check`, `STATUS_MATCH_OK`, exit `0`
  - verified the first real interval scheduled runs after the change:
    - VEVO task `9abd3ced7d1e4a418150e023ab47307a`, private IP `172.31.11.244`, task definition `vevo-invoice-daily:2`, exit `0`, summary `matched=3 created=3 failed=0 skipped_zero_total=0`
    - ROY task `7d97e245ef8343ad826e00e406ca6207`, private IP `172.31.42.63`, task definition `roy-invoice-daily:2`, exit `0`, summary `matched=0 created=0 failed=0 skipped_zero_total=1`
  - local verification before deploy/update passed:
    - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes`
    - `python scripts\reporting_qa_smoke.py`
    - `python scripts\security_ci.py`
    - `git diff --check`
  - merged PR `#58` into `main`:
    - merge commit: `58eb15d96c20922158e5d640971bd53df81ff805`
    - guarded main build run: `25495027756`
    - result: `success`
    - ECR `latest` digest after main build: `sha256:78d9c0b56ec0cf5b0f1300062bf3b408967ff54c4ad5e3f28d0467e71e705e7a`
  - ran final production host checks on the merged `main` image:
    - VEVO final check task `ef7c31eecae34e1b8f80bac7dcf4d285`, private IP `172.31.16.232`, marker `LOCALHOST_MARKER_OK:vevo:main-image-same-day-schedule-check`, status matcher `STATUS_MATCH_OK`, dry-run `matched=0 created=0 failed=0 skipped_zero_total=0`
    - ROY final check task `af718e7fe9af45d5beae2ff25b518728`, private IP `172.31.20.88`, marker `LOCALHOST_MARKER_OK:roy:main-image-same-day-schedule-check`, status matcher `STATUS_MATCH_OK`, dry-run `matched=0 created=0 failed=0 skipped_zero_total=0`

### 2026-05-04
- Investigated why VEVO and ROY invoices were not generated for recent eligible orders.
- Confirmed hard-gate runtime context:
  - instance-id/IP: `N/A` as invoice automation runs on scheduled ECS/Fargate tasks, not a fixed EC2 host
  - service/schedule names: `vevo-daily-invoice-generation`, `roy-daily-invoice-generation`
  - task definitions: `vevo-invoice-daily:1`, `roy-invoice-daily:1`
  - image path: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - image digest verified on host tasks: `sha256:050350f9f8b9e76bec170935a8c9dbffbb1a9044b42b35f78435249a3c8bbe90`
  - log paths: `/ecs/vevo-invoice-daily`, `/ecs/roy-invoice-daily`
- Confirmed both EventBridge Scheduler jobs are `ENABLED`:
  - VEVO `cron(0 20 * * ? *)` in `Europe/Bratislava`
  - ROY `cron(30 20 * * ? *)` in `Europe/Bratislava`
- Reviewed CloudWatch invoice logs from `2026-05-01..2026-05-03`:
  - scheduled jobs started, logged in to BizniWeb successfully, fetched orders, and exited cleanly
  - each scheduled run ended with `matched=0`, so no invoices were created during those runs
- Ran production dry-run diagnostics with direct host marker verification:
  - VEVO dry-run task `e3083d578c654911819584773a9ed533`, private IP `172.31.1.75`, marker `LOCALHOST_MARKER_OK:vevo:invoice-dry-run`, matched `16`, failed `0`
  - ROY dry-run task `fcaea9db308c42578b46e4d0942bfb0f`, private IP `172.31.46.195`, marker `LOCALHOST_MARKER_OK:roy:invoice-dry-run`, matched `15`, failed `0`
- Ran production Fargate invoice catch-up for `2026-05-01..2026-05-04`:
  - VEVO catch-up task `3436093656e94dfbac9f8c329f2bdf93`, private IP `172.31.40.111`, marker `LOCALHOST_MARKER_OK:vevo:invoice-catchup`, created `16`, failed `0`
  - ROY catch-up task `b312e817c0274cc7becd2f602a70bb1d`, private IP `172.31.38.114`, marker `LOCALHOST_MARKER_OK:roy:invoice-catchup`, created `15`, failed `0`
- Verified live BizniWeb API after catch-up:
  - VEVO `eligible_missing_invoice=0` for `2026-05-01..2026-05-04`
  - ROY `eligible_missing_invoice=0` for `2026-05-01..2026-05-04`
- Operational conclusion:
  - automation was not down; the previous scheduled runs had no eligible `Odoslana`/no-invoice orders at the time they ran
  - the missing invoices became eligible before the next scheduled evening run, so the one-off catch-up closed the backlog early
  - if this repeats after normal fulfillment timing, adjust invoice run timing or add a second daily catch-up pass

### 2026-04-28
- Fixed reporting calculation issues found during VEVO/ROY audit:
  - geo profitability now includes Google Ads allocation from order-level paid spend instead of subtracting only Facebook spend
  - 7D/30D/90D period bundle reports now carry full-history customer first-purchase dates so returning customers are not counted as new just because the visible report window is shorter
  - zero-revenue ROY orders now allocate item-level order overhead, paid spend, and fixed overhead by cost/quantity/equal-share fallback instead of dropping all item-level overhead
  - unknown currencies now fail fast instead of being silently treated as EUR
  - missing product costs now use a conservative zero-margin fallback instead of the previous default `1.00 EUR` cost
  - product-cost QA/dashboard wording now reflects missing-cost fallback semantics
- Added regression coverage in `tests/test_reporting_calculation_fixes.py`.
- Updated the ECR build workflow to run the new calculation regression tests before publishing the image.
- Verified locally with:
  - `python -m py_compile export_orders.py dashboard_modern.py daily_report_runner.py reporting_core\cfo_kpis.py`
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
- Deployed the refreshed runtime image via manual GitHub Actions dispatch:
  - workflow: `Build and Push ECR`
  - run: `25035445695`
  - result: `success`
  - refreshed ECR `latest` digest: `sha256:fb4902aca189511b1f17711fe37751ad9c7a060329add3626a8c02829863adc1`
- Verified production-equivalent report host smoke checks for both active projects:
  - hard-gate context: scheduled ECS/Fargate tasks, instance-id `N/A` because there is no fixed EC2 host, image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`, marker path `http://127.0.0.1:8000/marker.json`
  - VEVO service/schedule `vevo-daily-report-email`, task definition `vevo-reporting-daily:5`, task ARN `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/ce0f3c43d7ed4a3a9a645f9f851a59d7`, private runtime IP `172.31.5.177`, exit code `0`, marker `LOCALHOST_MARKER_OK`, `REPORT_SKIP_INVOICES=true`
  - ROY service/schedule `roy-daily-report-email`, task definition `roy-reporting-daily:3`, task ARN `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/7824cad369b04b6c88936e3fb5f666c6`, private runtime IP `172.31.2.70`, exit code `0`, marker `LOCALHOST_MARKER_OK`, `REPORT_SKIP_INVOICES=true`
  - both host smoke tasks ran `tests.test_reporting_calculation_fixes` inside the deployed image and passed `Ran 5 tests ... OK`
- Verified local UI smoke after host checks:
  - `live_dashboard_server.py` served `/health`, `/dashboard/vevo`, `/dashboard/roy`, `/api/vevo/latest`, and `/api/roy/latest`
  - both project dashboards exposed the `live-dashboard-app` marker and non-empty dashboard payloads
- Merged PR `#52` into `main` and confirmed the guarded ECR build from `main` succeeded:
  - merge commit: `a3b11413658fd40389d151f5fffffb9023380eb8`
  - build run: `25035803745`
  - ECR `latest` digest after the `main` build: `sha256:645868fb78e8419db49165f5d3e76a4a2f95fafb895fa124a5c32748350930f8`
- Manual VEVO report generation/send completed from production Fargate:
  - service/schedule: `vevo-daily-report-email`
  - task definition: `vevo-reporting-daily:5`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/c760e8377e78458bb66cb85640a34708`
  - runtime private IP: `172.31.38.81`
  - image digest in task: `sha256:645868fb78e8419db49165f5d3e76a4a2f95fafb895fa124a5c32748350930f8`
  - exit code: `0`
  - generated report: `data/vevo/report_20250503-20260427.html`
  - SES message id: `0107019dd2a78c7d-f98a66f4-0bde-4e7f-8840-ace08e65375d-000000`
  - invoice generation was skipped by flag
- Initial manual ROY report generation correctly failed on the new unknown-currency guardrail:
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/68a4848b2d6d4212ad7252793174e54b`
  - runtime private IP: `172.31.29.145`
  - exit code: `1`
  - failure: `Unknown currency RON; add an explicit EUR conversion rate to projects/roy/settings.json`
- Added explicit ROY `RON` conversion rate and deployed it:
  - PR `#53` merged into `main` as `ce634f0a6b3dda53cbed42c401df90ce847a4aa5`
  - source rate: ECB euro reference rate for `27 April 2026`, `1 EUR = 5.0954 RON`
  - configured rate: `1 RON = 0.19625545 EUR`
  - build run: `25036265850`
  - ECR `latest` digest after hotfix: `sha256:050350f9f8b9e76bec170935a8c9dbffbb1a9044b42b35f78435249a3c8bbe90`
- Manual ROY report generation/send completed after the hotfix:
  - service/schedule: `roy-daily-report-email`
  - task definition: `roy-reporting-daily:3`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/092c24eec0f441f68285a9ef195e16c0`
  - runtime private IP: `172.31.16.194`
  - image digest in task: `sha256:050350f9f8b9e76bec170935a8c9dbffbb1a9044b42b35f78435249a3c8bbe90`
  - exit code: `0`
  - generated report: `data/roy/report_20250924-20260427.html`
  - SES message id: `0107019dd2b5dbc1-cc224788-c3e2-42ce-a17e-8c175460fdb0-000000`
  - invoice generation was skipped by flag

- Split reporting and invoice generation into separate production schedules:
  - report schedules remain early morning so the prior day is complete:
    - VEVO `vevo-daily-report-email`: `cron(0 1 * * ? *)`, timezone `Europe/Bratislava`, target `vevo-reporting-daily:5`
    - ROY `roy-daily-report-email`: `cron(30 1 * * ? *)`, timezone `Europe/Bratislava`, target `roy-reporting-daily:3`
  - report task definitions set `REPORT_SKIP_INVOICES=true`
  - invoice schedules run the same day in the evening:
    - VEVO `vevo-daily-invoice-generation`: `cron(0 20 * * ? *)`, timezone `Europe/Bratislava`, target `vevo-invoice-daily:1`
    - ROY `roy-daily-invoice-generation`: `cron(30 20 * * ? *)`, timezone `Europe/Bratislava`, target `roy-invoice-daily:1`
  - created invoice log groups:
    - `/ecs/vevo-invoice-daily`
    - `/ecs/roy-invoice-daily`
  - extended scheduler IAM policy to allow task families:
    - `vevo-invoice-daily:*`
    - `roy-invoice-daily:*`
- Added repo-local standalone invoice runner support:
  - new `invoice_runner.py` computes default invoice windows from today's date in `Europe/Bratislava`
  - `daily_report_runner.py` now defaults to report-only behavior (`REPORT_SKIP_INVOICES=true`)
  - shared CloudWatch metric publishing moved to `reporting_core.metrics`
  - ECR build workflow now triggers on `invoice_runner.py`
- Added explicit per-project schedule metadata:
  - `projects/vevo/settings.json` declares `vevo-daily-report-email` and `vevo-daily-invoice-generation`
  - `projects/roy/settings.json` declares `roy-daily-report-email` and `roy-daily-invoice-generation`
  - `templates/reporting-client/settings.template.json` now includes separate report/invoice schedule blocks for future projects
- Verified locally with:
  - `python -m py_compile invoice_runner.py daily_report_runner.py generate_invoices.py reporting_core\metrics.py reporting_core\__init__.py`
  - `python -m unittest tests.test_invoice_generation`
  - `git diff --check`
- Verified production host-level markers:
  - VEVO report check task `eca78a00d2f142b2908088cf0ec4ce47`, private IP `172.31.34.23`, marker `LOCALHOST_MARKER_OK`, `report_skip_invoices=true`
  - ROY report check task `c0e2b80e33ce40ff9b77ceef587c7b3f`, private IP `172.31.28.148`, marker `LOCALHOST_MARKER_OK`, `report_skip_invoices=true`
  - VEVO invoice dry-run task `5783f35fcdb64408bc641e78bea70196`, private IP `172.31.45.190`, marker `LOCALHOST_MARKER_OK`, fetched `87`, matched `0`, failed `0`
  - ROY invoice dry-run task `947a29d9c04740dd8abf548918573256`, private IP `172.31.38.58`, marker `LOCALHOST_MARKER_OK`, fetched `80`, matched `0`, failed `0`, skipped zero-total `3`

- Investigated why ROY order `2677002371` did not get an invoice:
  - live BizniWeb API showed the order was ROY, purchased `2026-04-27 10:17:28`, status `Odoslaná`, non-zero total, and had no invoice
  - local invoice filter verification showed the order matched the configured invoice criteria for window `2026-04-21..2026-04-27`
  - root cause was scheduler drift: AWS EventBridge had been moved to evening runs (`21:00/21:30 Europe/Bratislava`), so at `2026-04-28 06:34 Europe/Bratislava` the ROY `2026-04-27` invoice pass had not run yet
- Corrected production AWS schedules back to early morning:
  - `vevo-daily-report-email`: `cron(0 1 * * ? *)`, timezone `Europe/Bratislava`, target unchanged at `vevo-reporting-daily:4`
  - `roy-daily-report-email`: `cron(30 1 * * ? *)`, timezone `Europe/Bratislava`, target unchanged at `roy-reporting-daily:2`
- Ran a one-off ROY production Fargate invoice catch-up for `2026-04-27`:
  - hard-gate context: scheduled ECS/Fargate, instance-id/IP not fixed until runtime, image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`, marker path `http://127.0.0.1:8000/marker.json`
  - task ARN `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/0b846a03229d496983987ca58a7c3bf8`
  - runtime private IP `172.31.26.154`
  - CloudWatch summary: matched `4`, created `4`, failed `0`, skipped zero-total `0`
  - localhost marker returned `LOCALHOST_MARKER_OK`
  - post-run API verification confirmed order `2677002371` now has invoice `2677002178`

### 2026-04-24
- Added automatic daily invoice-generation wiring for both active reporting clients on task branch `codex/daily-invoice-automation`:
  - `daily_report_runner.py` now supports an invoice step after the daily report flow
  - new runner controls:
    - `--skip-invoices`
    - `--invoice-dry-run`
  - project settings now explicitly enable invoice automation for:
    - `vevo`
    - `roy`
  - client template now carries the shared `invoice_generation` settings block for future projects
- Hardened `generate_invoices.py` for production-safe daily automation:
  - added shared `run_invoice_generation(...)` helper for the daily runner hook
  - changed pagination to `pur_date DESC` with an early stop once the fetch goes older than the requested invoice window
  - kept the invoice scan bounded to the rolling configured lookback instead of walking the whole shop history
  - excluded zero-total orders before invoice creation
  - redacted auth headers from debug logging after live dry-run exposed that the old diagnostics would print the API header
- Extended deployment protection:
  - `.github/workflows/build-and-push-ecr.yml` now rebuilds the image when `generate_invoices.py` changes
  - CI now runs `python -m unittest tests.test_invoice_generation`
- Verified locally with:
  - `python -m py_compile generate_invoices.py daily_report_runner.py tests/test_invoice_generation.py`
  - `python -m unittest tests.test_invoice_generation`
  - live dry-run VEVO invoice scan for `2026-04-18 .. 2026-04-24`
  - live dry-run ROY invoice scan for `2026-04-18 .. 2026-04-24`
- Verification outcome:
  - VEVO live dry-run stopped after `4` pages, filtered `118` recent orders, matched `25` invoice candidates, skipped `0` zero-total orders
  - ROY live dry-run stopped after `3` pages, filtered `83` recent orders, matched `28` invoice candidates, skipped `4` zero-total orders
  - the descending pagination fix removed the previous blocker where the scan could fail before reaching the newest days

### 2026-04-16
- Clarified the top KPI meaning for VEVO/modern reporting:
  - the existing top-level `Profit` KPI is the absolute post-ad profit layer (`contribution_profit` / `post_ad_contribution_profit`), not net profit after fixed overhead
  - renamed the exposed label to `Post-ad profit (€)` / `Post-ad zisk (€)` in the active dashboard payload, modern metric library, legacy CFO top-card renderer, and live dashboard context view
  - added smoke coverage asserting the KPI payload now exposes the explicit post-ad profit label
- Applied the agreed ROY inventory business rules in config:
  - thresholds confirmed for `Critical <= 14d`, `Low <= 30d`, `Watch <= 45d`, `Dead stock >= 90d`
  - alert delivery narrowed to the 30-day bucket, 45-day rows kept as watchlist only
  - primary inventory basis = cost, secondary = retail
  - restock prioritization switched to margin without fixed overhead
  - lead times configured by brand:
    - `maco_stop = 10 wd`
    - `wachman = 20 wd`
    - `roy = 50 wd`
  - alert exclusions configured for service / reklamacie / gifts / spare parts / test / obvious noise
  - initial bundle-to-component rule added for MACO STOP spray sets
- Extended ROY inventory analytics runtime with:
  - actionable `alert_rows`
  - conservative alert-demand blend by forecast confidence
  - reorder deadline / reorder quantity / `Order now` vs `Prepare PO`
  - hero-brand handling for Wachman and MACO STOP
  - bundle-demand shifting onto component SKUs
  - explicit inbound-stock placeholder state (`not_modeled`)
- Wired the new inventory alert outputs into the active report surfaces:
  - `dashboard_modern.py` renders summary mini-cards plus the new actionable alert table
  - `daily_report_runner.py` reads `dashboard_payload.json` and appends `SKLADOVE ALERTY` into the outbound email text
- Removed the ROY sample funnel from the active runtime and report surface:
  - `export_orders.py` no longer computes sample-funnel analytics for the ROY project
  - `dashboard_modern.py` now suppresses the sample-funnel block for ROY while keeping it available for VEVO
- Verified locally on live ROY data with a one-pass export:
  - `python -m py_compile export_orders.py dashboard_modern.py daily_report_runner.py`
  - production-equivalent ROY export for `2025-09-24 -> 2026-04-15` using output tag `inventory_alerts_verify`
  - generated artifacts:
    - `data/roy/report_20250924-20260415__inventory_alerts_verify.html`
    - `data/roy/dashboard_payload_20250924-20260415__inventory_alerts_verify.json`
  - verification outcome:
    - `29` actionable 30d alerts
    - `13` hero-SKU alerts
    - `29` `Order now`, `0` `Prepare PO`
    - `EUR 18,001.42` revenue at risk over 30d
    - `EUR 50,795.03` inventory cost value
    - `12` excluded noise rows
    - HTML contains `Actionable inventory alerts`
    - email summary text contains `SKLADOVE ALERTY`
- Verified the ROY sample-funnel removal locally with:
  - production-equivalent ROY export for `2025-09-24 -> 2026-04-15` using output tag `no_sample_verify`
  - generated artifacts:
    - `data/roy/report_20250924-20260415__no_sample_verify.html`
    - `data/roy/dashboard_payload_20250924-20260415__no_sample_verify.json`
  - verification outcome:
    - `dashboard.sample_funnel.summary = {}`
    - `dashboard.sample_funnel.windows = []`
    - `dashboard.sample_funnel.entry_rows = []`
    - ROY HTML no longer renders the sample-funnel section
    - inventory alert section remains present and populated

### 2026-04-15
- Added Roy inventory snapshot ingestion from Biznisweb GraphQL product data:
  - live product inventory fetch with warehouse rows, quantities, available quantities, and retail pricing
  - cost-value mapping reuses the existing product-expense source of truth instead of retail price
- Extended Roy product-demand analytics with inventory outputs:
  - inventory valuation rows
  - stock-risk rows with projected stockout date / days of cover
  - dead-stock rows
  - inventory summary KPIs in the dashboard payload
- Added Roy dashboard rendering for the new inventory layer:
  - inventory snapshot summary cards
  - forecast table enriched with on-hand qty, days of cover, projected stockout, and stock-risk level
  - inventory valuation table
  - stock-risk watchlist
  - dead-stock table
- Extended Roy inventory analytics with operational inventory metrics:
  - restock-priority score and bucket (`Urgent`, `High`, `Plan`, `Monitor`)
  - 30d / 45d revenue-at-risk and profit-at-risk rollups
  - dead-stock share on total inventory value / units
  - negative-stock unit-gap tracking
  - inventory turns and estimated days-in-inventory by brand and product family
  - forecast holdout backtest accuracy rows and summary KPIs
- Added Roy dashboard rendering for the extra inventory operations layer:
  - revenue-at-risk table
  - restock-priority table
  - inventory turns by family
  - inventory turns by brand
  - forecast backtest accuracy table
- Added explicit `inventory_model` settings to:
  - `projects/roy/settings.json`
  - `templates/reporting-client/settings.template.json`
- Verified locally with:
  - `python -m py_compile export_orders.py dashboard_modern.py`
  - `python export_orders.py --project roy --from-date 2025-09-24 --to-date 2026-04-14 --output-tag inventory_probe`
  - `python export_orders.py --project roy --from-date 2025-09-24 --to-date 2026-04-14 --output-tag inventory_ops`
- Verification outcome on live Roy data:
  - Biznisweb inventory snapshot fetch succeeded (`56` pages, `2370` warehouse rows)
  - full-history Roy dashboard payload now reports:
    - `1669` tracked products
    - `410` products with stock
    - `12347` available units
    - `EUR 50910.11` inventory cost value
    - `EUR 183338.35` inventory retail value
    - `81.28%` unit coverage by mapped costs
    - `78.48%` retail-value coverage by mapped costs
    - `32` critical / negative-stock items
    - `39` items at 30-day stock risk
    - `43` items at 45-day stock risk
    - `17` out-of-stock items with recent demand
    - `10` negative-stock products with `115` units below zero
    - `328` dead-stock candidates worth `EUR 1866.59` at mapped cost
    - dead-stock share now surfaces as `3.67%` of mapped inventory cost and `17.08%` of on-hand units
    - revenue at risk now surfaces as `EUR 18336.52` over the 30-day risk bucket and `EUR 20802.32` over the 45-day bucket
    - current restock watchlist contains `31` urgent and `7` high-priority products
    - forecast backtest currently covers `25` products, but the first live sample is still weak (`74.77%` WAPE, `12.00%` within 20% error)
  - rendered HTML now contains the new sections:
    - `Inventory snapshot`
    - `Inventory valuation`
    - `Stock risk watchlist`
    - `Dead stock candidates`
    - `Revenue at risk`
    - `Restock priority`
    - `Inventory turns by family`
    - `Inventory turns by brand`
    - `Forecast backtest accuracy`
- Split Doklady into its own GitHub repository: `Terem21/doklady-saas`.
- Reporting workflow now treats repositories as product boundaries and branches as short-lived task scopes only.
- Prepared reporting repo branch cleanup by identifying merged/superseded remote branches versus the still-active reporting branches.

### 2026-04-14
- Added regression coverage for CFO KPI deck layer mapping in `scripts/reporting_qa_smoke.py`.
- Smoke QA now asserts for `daily`, `weekly`, `monthly`, and `all_time` that:
  - revenue reconciles to raw series totals
  - KPI `profit` reconciles to `contribution_profit`
  - secondary company-profit value reconciles to `net_profit`
  - post-ad and company margins recompute correctly from absolute values
- Hardened `reporting_core/cfo_kpis.py` date parsing so KPI payload building accepts `date`, `datetime`, and `pd.Timestamp`, not only strict `YYYY-MM-DD` strings.
- Fixed modern dashboard `consistency` payload mapping so ROAS / margin / CAC deltas and OK flags now serialize from the live validation keys instead of stale field names.
- Geo QA now warns explicitly when country profitability excludes non-zero Google Ads spend, because country rows currently model Facebook-attributed spend only.
- Expanded `scripts/reporting_qa_smoke.py` with regressions for:
  - dashboard consistency payload serialization
  - geo profitability warning when Google spend is present but not country-attributed
- Verified locally with:
  - `python -m py_compile dashboard_modern.py export_orders.py scripts/reporting_qa_smoke.py`
  - `python scripts/reporting_qa_smoke.py`

### 2026-04-13
- Added shared runtime support for explicit per-day fixed overhead via `fixed_daily_cost` while preserving monthly fixed-cost support.
- Set VEVO project runtime to `fixed_daily_cost = 70` so CM3 metrics include fixed overhead in the active reporting path.
- Fixed CFO KPI aggregation mismatch:
  - daily rows now use post-ad profit ex fixed (`contribution_profit`) as the pre-CM3 layer,
  - company-margin-with-fixed now subtracts actual fixed overhead from the daily rows instead of reapplying a blind constant over already-net profit.
- Added new marketing decision metrics into the active modern dashboard and payload:
  - marketing spend / revenue
  - CM3 per ad euro
  - CM2 -> CM3 drag
  - paid-day CM3 win rate
  - returning revenue share on paid days
  - best CM3 spend range
  - best CM3 margin range
- Expanded spend-bucket effectiveness rows with CM3 margin, returning revenue share, and AOV.
- Verified locally with:
  - `python -m py_compile export_orders.py dashboard_modern.py reporting_core/runtime.py reporting_core/cfo_kpis.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python scripts/reporting_qa_smoke.py`
- Verification outcome:
  - `aggregate_by_date_20260301-20260331.csv` shows `fixed_daily_cost = 70.0` and `CM2 != CM3`
  - `dashboard_payload_20260301-20260331.json` now contains `dashboard.marketing_decision_summary`
  - rendered HTML contains the new marketing cards and the expanded spend-bucket table

### 2026-03-30
- Added env governance baseline: `.env.required`, pre-commit hook, CI env check.
- Added cross-platform bootstrap scripts for macOS/Linux and Windows PowerShell.
- Narrowed `PROJECT_STATE.md` to this repository only.
- Removed cross-project state ownership from this repo; left only integration notes.

### 2026-03-31
- Completed `P3.1` reusable reporting core foundation:
  - added package `reporting_core/` as the shared source of truth for project config + runtime loading,
  - moved project config helpers behind `reporting_core.config` and kept `project_config.py` as a backward-compatible shim,
  - added `reporting_core.runtime` with `ProjectRuntime` and reusable runtime application/loading helpers,
  - added `reporting_core.contracts` with `ReportingArtifactSet` + canonical output artifact builder,
  - switched `export_orders.py`, `daily_report_runner.py`, and `generate_invoices.py` to import from `reporting_core`,

### 2026-04-10
- Added Roy bundle/accessory model as a first-class advanced DTC metric using project-configured anchor device families and accessory groups.
- Bundle/accessory outputs now include pair-level attach rate and contribution uplift, device family summary, and accessory group quality summary.
- Modern dashboard now renders Roy bundle/accessory charts in the Products/Operations library without changing the current production shell.
- Verified with real Roy March 2026 export: HTML report generated successfully and new bundle/accessory chart IDs are present in the rendered output.
  - updated daily runner to consume the shared artifact contract instead of rebuilding output paths ad hoc,
  - verified syntax with `python -m py_compile export_orders.py daily_report_runner.py generate_invoices.py project_config.py reporting_core\\__init__.py reporting_core\\config.py reporting_core\\runtime.py reporting_core\\contracts.py`,
  - verified ROY smoke export on `2026-03-01..2026-03-02`,
  - verified project-aware daily runner on `2026-03-01..2026-03-02` with `--skip-export --skip-email`.
- Added new Week-of-Month analytics (Week 1-4) into reporting pipeline in export_orders.py.
- Wired Week-of-Month outputs into HTML report generation (html_report_generator.py) with 2 charts and performance table.
- Added aggregation for week-level pattern visibility: orders, revenue, profit, margin, AOV, avg daily revenue/profit, active days/months.

### 2026-04-03
- Verified VEVO production scheduler and runtime wiring end-to-end on AWS:
  - Scheduler `vevo-daily-report-email` remains enabled at `01:00 Europe/Bratislava`
  - ECS cluster `vevo-reporting-cluster`
  - Task definition `vevo-reporting-daily:3`
  - Image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
- Confirmed runtime secret `vevo/reporting/runtime-env` still points to:
  - `REPORT_FROM_DATE=2025-05-03`
  - `REPORT_PROJECT=vevo`
  - `REPORT_EMAIL_TO=mil.terem@gmail.com,vzeman@gmail.com,maker.martuska@gmail.com`
- Fixed VEVO task-role CloudWatch metric permission drift:
  - previous inline IAM policy allowed only namespace `VevoReporting`
  - runtime writes metrics into `BizniswebReporting`
  - updated inline policy `vevo-reporting-put-metrics` to allow both namespaces
- Re-ran a manual ECS production-equivalent task and verified in `/ecs/vevo-reporting-daily` log stream:
  - `HTML report saved: data/vevo/report_20250503-20260402.html`
  - `SES message sent`
  - no `WARN: failed to publish CloudWatch metric ... PutMetricData`
- Verified syntax via python -m py_compile export_orders.py html_report_generator.py.
- Revised Week-of-Month methodology to remove day-count bias:
  - uses only days 1-28 (4x7 equal windows),

### 2026-04-03
- Retired the standalone `test2` dashboard variant after promoting it to production:
  - renamed `dashboard_test2.py` to `dashboard_modern.py`,
  - removed `test2` from the HTML renderer dispatch variants,
  - updated security CI to validate the renamed production dashboard module,
  - cleaned local `__test2` artifacts from the active workspace.
- Fixed false-positive `Env Check` / `security-baseline` CI failure after promoting the modern dashboard renderer:
  - `scripts/security_ci.py` no longer expects the `Partial Data` marker inside `daily_report_runner.py`,
  - CI now validates partial-data rendering in the actual HTML layer (`html_report_generator.py` and `dashboard_modern.py`),
  - retained a runner-level assertion that the main HTML report artifact is still attached by `daily_report_runner.py`.
- Verified locally with:
  - `python scripts/security_ci.py`
  - `python -m py_compile scripts/security_ci.py`
- Added modern timeframe UX split for the new production dashboard:
  - `Executive KPI deck` keeps its own independent `Daily / Weekly / Monthly` toggle,
  - all non-KPI chart sections now use a global sidebar `Analytics window` switch,
  - global period links preserve the currently active section anchor while switching report variant,
  - plain production reports now generate the `_periods/...` bundle needed for the sidebar switcher (previously this existed only for tagged/test outputs).
- Verified locally with:
  - `python -m py_compile dashboard_test2.py export_orders.py html_report_generator.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - uses full months only (drops partial first/last month for this metric),
  - daily normalization uses calendar_days (includes zero-order days).
- Added fairness diagnostics in table: `Calendar Days` and `Active Day Rate`.
- Added new Day-of-Month analytics (1-31) to reporting pipeline:

### 2026-04-10 (cohort-normalized unit economics)
- Added cohort-normalized CAC / LTV / payback views into `export_orders.py` so acquisition cohorts can be compared on mature horizons instead of only via global blended shortcuts.
- New cohort unit economics payload now exports, per acquisition cohort:
  - blended and FB CAC
  - 30/60/90/180-day revenue LTV
  - 30/60/90/180-day contribution LTV
  - 30/60/90/180-day contribution LTV/CAC
  - 30/60/90/180-day CAC recovery %
  - average and median payback days by horizon
- Added mature weighted summary fields for cohort-normalized contribution LTV/CAC and payback recovery into the advanced DTC summary layer.
- Wired the new cohort payload into `dashboard_modern.py` and added three customer analytics charts:
  - `custCohortContributionLtvCacChart`
  - `custCohortPaybackRecoveryChart`
  - `custCohortCacVsContributionChart`
- Added null-safe rendering for immature cohort horizons so missing maturity now renders as gaps instead of fake zeroes.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project vevo --from-date 2025-05-03 --to-date 2026-04-09`

### 2026-04-10 (geo confidence guardrails)
- Added project-level `geo_confidence` settings for VEVO and ROY with separate country/city thresholds.
- Export layer now computes confidence metadata per country/city:

### 2026-04-11
- Restored the missing VEVO April 2026 cost pipeline inside the active reporting line instead of the stale side branch:
  - added repo-local Excel importer `scripts/import_product_expenses_excel.py`,
  - imported the latest VEVO workbook from `D:\product_expense_rebuild_20250503-20260407 (4).xlsx`,
  - added `projects/vevo/product_name_aliases.json`,
  - enabled VEVO `expense_match_mode = title_first` in `projects/vevo/settings.json`,
  - extended `reporting_core.runtime` to load `expense_match_mode` and alias files,
  - extended `export_orders.py` to resolve costs by exact label / compound key before shared EAN fallback and to canonicalize VEVO reporting identities for analytics.
- Verified on fresh March 2026 exports:
  - VEVO `Parfum do prania Vevo Natural No.07 Ylang Absolute (500ml)` now uses `6.14 EUR`,
  - VEVO `Parfum do prania Vevo Premium No.07 Ylang Absolute (500ml)` now uses `13.9 EUR`,
  - VEVO `Parfum do prania Vevo Premium No.09 Pure Garden (500ml)` now uses `14.36 EUR`,
  - VEVO `Parfum do prania Vevo Premium No.08 Cotton Dream (200ml)` resolves via compound key at `6.69 EUR`,
  - VEVO product cost fallback share is now `0.00%` revenue / `0.00%` profit for March 2026.
- Verified locally with:
  - `python -m py_compile export_orders.py reporting_core/runtime.py dashboard_modern.py html_report_generator.py scripts/security_ci.py scripts/import_product_expenses_excel.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - `python scripts/security_ci.py`
  - `python scripts/reporting_qa_smoke.py`
- Transferred the April-side ROY non-revenue order status filtering into the active runtime/config path:
  - added shared `excluded_order_statuses` runtime support in `reporting_core/runtime.py`,
  - replaced hardcoded realized-revenue filters in `export_orders.py` with a shared helper,
  - wired ROY-specific excluded statuses into `projects/roy/settings.json`,
  - exposed the setting in `templates/reporting-client/settings.template.json`,
  - aligned failed-payment-only segmentation with the shared failed-payment status list.
- Verified locally with:
  - `python -m py_compile export_orders.py reporting_core/runtime.py dashboard_modern.py html_report_generator.py`
  - `python export_orders.py --project roy --from-date 2026-04-07 --to-date 2026-04-07`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - `python scripts/reporting_qa_smoke.py`
  - `confidence_status`
  - `confidence_score`
  - `low_sample`
  - `hide_economics`
- Geo profitability output now exposes guarded fields:
  - `contribution_profit_guarded`
  - `contribution_margin_pct_guarded`
  - `fb_cpo_guarded`
- Source health now includes `qa.geo` summary with:
  - ready / observe / ignore counts
  - unknown country rate
  - warning list
- Modern dashboard geography section now renders:
  - Geo confidence guardrails panel
  - confidence badges for country rows
  - guarded geo profitability chart/table values (`N/A` on low-sample markets)

### 2026-04-11 (QA assertions + shipping semantics)
- Verified shared QA assertion layer end-to-end on real March 2026 VEVO and ROY exports.
- Export layer now computes `qa.assertions` with:
  - shell/library parity checks for critical economics metrics,
  - refund binding presence,
  - platform/attributed CPA arithmetic mismatch detection,
  - attributed orders tolerance checks,
  - missing dimension counts (`day_name`, `anchor_item`, `attached_item`, `anchor_orders`, `country`),
  - `null_label_rate_pct`, `qa_failure_count`, `qa_warning_count`.
- Daily runner now includes data-quality summary in email body and publishes CloudWatch QA metrics:
  - `ReportQaWarnings`
  - `ReportQaFailures`
  - `ReportQaCritical`
  - `ReportPartialData`
- Modern dashboard now renders both failure and warning assertion blocks plus richer geo confidence share cards.
- Shipping terminology was normalized from subsidy-style wording to `Net shipping` / `shipping_net_cost` in config, export, and dashboard labels.
- Verified locally with:
  - `python -m py_compile export_orders.py dashboard_modern.py daily_report_runner.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Added CI guardrails so geo QA metadata and the dashboard geo-confidence panel cannot disappear silently.
- Verification target:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`

### 2026-04-11 (product cost coverage QA)
- Added explicit `expense_source` tagging on item rows so item-level costs are classified as:
  - mapped product SKU
  - mapped item label
  - configured overrides
  - default 1.00 EUR fallback
- Added `qa.product_expense_coverage` into `source_health` / `data_quality` sidecars with:
  - fallback row/unit/revenue/profit shares
  - top fallback items by impact
  - expense-source mix summary
- Modern dashboard now renders:
  - `Product cost coverage`
  - `Expense source mix`
  - `Top default-cost items`
- Verified locally with:
  - `python -m py_compile export_orders.py dashboard_modern.py html_report_generator.py scripts\\reporting_qa_smoke.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python scripts\\reporting_qa_smoke.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Current decision on payment fees:
  - keep `excluded_not_modeled` for now
  - current `ORDER_QUERY` still does not ingest any payment-fee / payment-method fee field from BiznisWeb
  - next safe step is to fix product-cost coverage first, then decide whether fees should come from an expanded API payload or a reproducible config layer
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2025-09-24 --to-date 2026-04-09`
- Verification outcome:
  - VEVO full-history report `data\\vevo\\report_20250503-20260409.html` contains all three cohort chart IDs
  - ROY full-history report `data\\roy\\report_20250924-20260409.html` contains all three cohort chart IDs
  - both exports complete successfully with the new cohort-normalized views embedded in the modern dashboard.

### 2026-04-03
- Promoted the modern dashboard shell (`test2`) to the default production HTML renderer.
- Removed standalone CFO HTML from `reporting_core.contracts` and from `daily_report_runner.py`.
- Changed daily SES delivery to send only the main HTML report attachment.
- Cleaned local legacy `__test` artifacts and regenerated the plain VEVO March production report to verify the new default renderer.

### 2026-04-01
- Added side-by-side output variant support for safe UI redesign/testing without overwriting working report artifacts:
  - new optional `output_tag` support in `reporting_core.contracts.build_artifact_set(...)`,
  - tagged artifacts render as `__<tag>` before file extension, e.g. `report_...__ui_test.html`,
  - `export_orders.py` now accepts `--output-tag` and isolates cleanup to the active output variant only,
  - `daily_report_runner.py` now accepts `--output-tag` and generates tagged CFO outputs against the same tagged artifact set,
  - verified syntax with `python -m py_compile reporting_core\\contracts.py reporting_core\\__init__.py export_orders.py daily_report_runner.py`,
  - verified smoke exports for VEVO and ROY on `2026-03-30..2026-03-31` with `--output-tag ui_test`,
  - verified tagged CFO generation for both projects:
    - `data\\vevo\\cfo_graphs_20260330-20260331__ui_test.html`
    - `data\\roy\\cfo_graphs_20260330-20260331__ui_test.html`,
  - generated full-range side-by-side test artifacts without email sending:
    - VEVO:
      - `data\\vevo\\report_20250503-20260331__ui_test.html`
      - `data\\vevo\\cfo_graphs_20250503-20260331__ui_test.html`
      - `data\\vevo\\email_strategy_20250503-20260331__ui_test.html`
    - ROY:
      - `data\\roy\\report_20250924-20260331__ui_test.html`
      - `data\\roy\\cfo_graphs_20250924-20260331__ui_test.html`.
- Updated ROY project baseline `report_from_date` from `2025-08-06` to `2025-09-24` in `projects/roy/settings.json`.
- Re-generated ROY reporting outputs for `2025-09-24..2026-03-31` without email delivery so current artifacts match the new start boundary.
- Fixed report headings to use project-level `reporting_system_name` across generated HTML outputs.
- Main HTML reports now render `Vevo reporting`, `Roy reporting`, and future client names from project config instead of a hardcoded BizniWeb title.
- CFO dashboards now use the same project reporting title in the HTML `<title>` and visible header, with `CFO Executive Dashboard` kept as a subtitle.
- Email strategy HTML now also uses the project reporting title in the document title and main heading.
- Verified regeneration for VEVO (`2025-05-03..2026-03-31`) and ROY (`2025-08-06..2026-03-31`) without email delivery.

- Fixed Daily Orders chart visibility issue in shared HTML generator:
  - date coverage in `aggregate_by_date_*` was already complete, including zero-order days,
  - pure bar rendering made zero-order days look like "missing days" at the start of sparse client timelines,
  - `Daily Orders` now overlays a thin line series on top of bars so zero-order periods remain visually continuous instead of appearing absent.
- Re-generated ROY report for `2025-08-06..2026-03-31` after the chart fix.

### 2026-04-01
- Completed `P4.2` observability baseline for reporting:
  - added `scripts/observability_snapshot.py` for project-level artifact + source-health snapshots,
  - added `.github/workflows/observability-check.yml` to generate/upload a JSON observability artifact in CI,
  - extended `scripts/security_ci.py` to require and syntax-check the observability baseline.
- Completed `P4.4` reporting templates baseline:
  - added `templates/reporting-client/` with `settings.template.json`, `.env.example`, `product_expenses.json`, and onboarding README,
  - added `scripts/scaffold_client.py` to scaffold a new client bundle under `projects/<slug>/`,
  - updated `README_DEV.md` with observability and client-template usage.
  - integrated in export_orders.py (`analyze_day_of_month`) and HTML generation,
  - uses full months only for unbiased phase-of-month comparisons,
  - normalizes by calendar occurrences for each day number (1..31),
  - added 2 charts + normalized performance table in HTML report.
- Hardened geographic reporting for Top Cities:
  - country now prefers `delivery_country` and falls back to `invoice_country`,
  - city now prefers `delivery_city` and falls back to `invoice_city`,
  - empty cities are excluded from ranking.
- Hardened reporting repo hygiene for multi-PC work:
  - `.gitignore` now blocks local `.env.*` runtime files while preserving safe templates,
  - `.gitattributes` enforces LF for Python/Markdown/template files,
  - added safe tracked template `.env.roy.sk.template`,
  - cleared CRLF-only working tree noise before continuing.
- Completed `P1.4` partial-data handling for reporting outputs:
  - added source-health contract per run (`source_health`) with per-source status/mode/detail fields,
  - export now writes `data_quality_<range>.json` sidecar metadata next to report artifacts,
  - main HTML report renders a visible Data Quality banner/table before KPI cards,
  - CFO HTML runner loads the same sidecar and renders the same source-health banner,
  - runner keeps backward compatibility by not requiring the JSON sidecar for legacy artifact existence checks,
  - verified syntax with `python -m py_compile export_orders.py html_report_generator.py daily_report_runner.py google_ads.py weather_client.py facebook_ads.py generate_invoices.py http_client.py`,
  - verified ROY smoke run end-to-end on `2026-03-01..2026-03-03`, including generated `data_quality_*.json`, main HTML report, and CFO HTML banner rendering.
- Completed `P1.3` reporting integration hardening:
  - added shared `http_client.py` with default timeout + retry policy for external integrations,
  - moved Facebook Ads API auth to `Authorization: Bearer` header instead of query params,
  - removed direct `requests.get` usage from Facebook Ads client in favor of shared session helper,
  - added configurable timeouts for BizniWeb GraphQL transport in reporting and invoice flows,
  - moved weather client to shared retry/timeout session,
  - documented HTTP timeout/retry knobs in `.env.example`,
  - verified syntax with `python -m py_compile http_client.py weather_client.py facebook_ads.py export_orders.py generate_invoices.py`.
  - city now prefers `delivery_city` and falls back to `invoice_city`,
  - blank city values are excluded from ranking,
  - ties are sorted by revenue first and order count second.
- Added project-scoped weather configuration for VEVO and ROY in `projects/<project>/settings.json`.
- Added `weather_client.py`:
  - historical daily weather fetch from Open-Meteo archive API,
  - monthly local cache per project/location,
  - weighted location support prepared for future multi-city rollout.

### 2026-04-02
- Completed deep research pass for professional ecommerce dashboard structure using BI/dashboard best-practice sources and ecommerce analytics guides.
- Added fully isolated `test2` main-report renderer in `dashboard_test2.py`:
  - separate from the current production renderer and separate from the existing `__test` shell,
  - focused on executive KPI hierarchy, grouped business-question sections, and explicit source-health presentation,
  - uses the same reporting data and existing CFO KPI payload instead of inventing new business logic.
- Wired `generate_html_report(..., dashboard_variant=...)` so only `--output-tag test2` activates the new renderer.
- Kept existing production and `__test` report outputs untouched.
- Verified syntax with `python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py`.
- Generated isolated VEVO March artifacts for review:
  - `data\\vevo\\report_20260301-20260331__test2.html`
  - `data\\vevo\\email_strategy_20260301-20260331__test2.html`
  - period child reports under `data\\vevo\\_periods\\report_20260301-20260331__test2\\...`
- Added V1 weather impact analytics into `export_orders.py`:
  - merges daily weather with `date_agg`,
  - computes weather buckets (`Good / Neutral / Bad`),
  - computes weekday baseline deltas for revenue, profit, orders, AOV,
  - computes direct and lagged weather correlations,
  - exports project-scoped `weather_impact_<range>.csv`.
- Added Weather Impact section into `html_report_generator.py`:
  - correlation KPI cards,
  - precipitation vs revenue/profit time-series chart,
  - weather bucket uplift vs weekday baseline chart,
  - weather bucket performance table.
- Verified syntax with:
  - `python -m py_compile export_orders.py html_report_generator.py weather_client.py`
- Verified ROY runtime smoke test end-to-end on:
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-07`
  - confirmed Weather Impact section rendered in generated HTML.
- VEVO runtime smoke test remains blocked by expired Facebook token during ads fetch; weather implementation itself is not the blocker.
- Regenerated full-range client outputs without email sending:
  - ROY: `data/roy/report_20250922-20260330.html` and `data/roy/export_20250922-20260330.csv`
  - VEVO: `data/vevo/report_20250503-20260330.html` and `data/vevo/export_20250503-20260330.csv`
- Verified VEVO full-range regeneration again with working Facebook Ads enrichment after providing a valid runtime Meta token for the process.
- Added Advanced DTC metrics pack (1/2/3/4/7/8/9/10/11) into reporting pipeline:
  - new analyzer in export_orders.py: `analyze_advanced_dtc_metrics(df)`,
  - wired to `generate_html_report(..., advanced_dtc_metrics=...)`,
  - added summary KPI cards for first-order/repeat contribution, contribution LTV/CAC, margin stability, SKU Pareto concentration.
- Added new Advanced DTC visual outputs in html_report_generator.py:
  - Contribution by Basket Size chart + table,
  - Payday Window Index chart + table,
  - Cohort Payback Days chart + table,
  - Margin Stability chart,
  - SKU Contribution Pareto chart + table,
  - Attach Rate table for key products.
- Smoke-tested analyzer on synthetic dataset and verified syntax with:
  - `python -m py_compile export_orders.py html_report_generator.py`
- Hardened local repo hygiene for reporting runtime secrets:
  - `.gitignore` now ignores arbitrary local `.env.*` runtime variants while preserving tracked safe templates
  - `.gitattributes` now enforces LF for python/markdown/env-template files to avoid false CRLF-only diffs
  - added safe tracked template `/.env.roy.sk.template` for roy-specific local bootstrap without committing secrets
- Completed `P2.2` reporting client-boundary refactor:
  - added shared `project_config.py` to centralize per-project env loading, project settings, display-name/reporting defaults, API URL resolution, and BizniWeb base URL derivation,
  - removed remaining Vevo-specific runtime defaults from generic reporting flow in `export_orders.py`,
  - kept Vevo legacy product-cost fallback scoped only to Vevo; non-Vevo projects no longer inherit Vevo costs implicitly,
  - gated Vevo-only email strategy HTML behind per-project config (`enable_email_strategy_report`) so ROY and future clients do not generate Vevo-branded strategy output,
  - made `daily_report_runner.py` project-driven for email subject/body text, SES configuration-set fallback, and CloudWatch namespace selection,
  - removed `email_strategy_html` from required daily-runner outputs so non-Vevo projects can run cleanly without Vevo-only artifacts,
  - made `generate_invoices.py` project-aware via `--project`, per-project env bootstrap, and BizniWeb base URL derivation from the selected project API endpoint instead of hardcoded Vevo URLs,
  - extended project settings:
    - `projects/vevo/settings.json` now declares display/reporting defaults and explicitly enables the Vevo email-strategy artifact,
    - `projects/roy/settings.json` now declares project display/reporting defaults and explicitly disables the Vevo-only strategy artifact,
  - verified syntax with `python -m py_compile project_config.py export_orders.py daily_report_runner.py generate_invoices.py`,
  - verified ROY smoke export on `2026-03-01..2026-03-02`,
  - verified VEVO smoke export on `2026-03-01..2026-03-02`,
  - verified project-aware invoice bootstrap on ROY with `python generate_invoices.py --project roy --from-date 2026-03-01 --to-date 2026-03-02 --dry-run --no-web-login`.
- Completed `P2.4` reporting security CI baseline:
  - extended `.github/workflows/env-check.yml` to run on the active reporting branch and added `secret-scan` + `security-baseline` jobs,
  - added `scripts/security_ci.py` with repo-local assertions for shared HTTP hardening (`Authorization` header usage, shared retry session, partial-data/source-health invariants),
  - wired CI to fail fast if reporting core loses the partial-data markers or Meta auth hardening contract,
  - verified the local baseline script with `python scripts/security_ci.py`.

- Fixed VEVO local Meta token/bootstrap drift on 2026-04-01:
  - confirmed AWS runtime secret `vevo/reporting/runtime-env` still contains a valid Facebook Ads token,
  - synced local root `.env` VEVO token with the valid runtime token,
  - rewrote `.env` without UTF-8 BOM after a local PowerShell write introduced BOM and broke the first env key (`BIZNISWEB_API_TOKEN`),
  - hardened all reporting-side `load_dotenv(...)` calls to use `encoding="utf-8-sig"` so BOM-prefixed `.env` files no longer break the first key,
  - verified VEVO smoke export on `2026-03-31..2026-03-31` with successful Facebook Ads enrichment (`Successfully connected to Facebook Ads account: Wachman`, spend fetched, ROAS restored).
- Fixed VEVO Google Ads runtime hygiene on 2026-04-01:
  - normalized AWS Secrets Manager entry `vevo/reporting/runtime-env` from malformed pseudo-JSON into valid JSON,
  - aligned runtime `GOOGLE_ADS_LOGIN_CUSTOMER_ID` to an empty value because VEVO Google Ads API access works directly on customer `7592903323` and fails when the old MCC login header is forced,
  - verified Google Ads API connectivity locally with `test_connection=True` against `Vevo.sk (7592903323)`,
  - verified that March 2026 Google Ads spend is correctly `0.00` because both `Vevo.sk (7592903323)` and `Vevo.sk - old (1025163995)` return zero March campaign rows via GAQL,
  - confirmed the zero Google Ads spend in the VEVO March report is a real account state, not an integration bug.

- UI redesign baseline for main HTML reporting (test track) on 2026-04-01:
  - replaced legacy purple-gradient dashboard skin in `html_report_generator.py` with a modern analytics layout (neutral background, stronger typography hierarchy, denser KPI cards, cleaner tables, larger chart canvases),
  - increased chart readability (`max-height` up to 420px, improved spacing, better responsive behavior),
  - standardized euro symbol rendering by replacing mojibake `â‚¬` occurrences with HTML entity `&#8364;` in report output templates,
  - normalized collapsible toggle glyph to `&#9662;` to avoid encoding drift in generated HTML,
  - validated syntax with `python -m py_compile html_report_generator.py daily_report_runner.py`,
  - regenerated side-by-side test artifacts (no email) with `--output-tag ui_test`:
    - VEVO: `data/vevo/report_20250503-20260331__ui_test.html`
    - ROY: `data/roy/report_20250922-20260331__ui_test.html`.

- SK/EN full-translation + user-friendly pass completed on 2026-04-01:
  - strengthened bilingual rendering in `html_report_generator.py` with explicit `data-en`/`data-sk` coverage for guidance and quick-read sections so language switch is end-to-end usable,
  - added plain-language onboarding block for non-finance users (`metric-cheatsheet`) explaining Revenue, Net Profit, ROAS, and CAC vs Break-even CAC in business-friendly wording,
  - corrected Slovak readability/diacritics in key guidance text (`Ako čítať tento report (jednoducho)`),
  - normalized confusing delta KPI labels to explicit text:
    - `ROAS Check Delta`
    - `Margin Check Delta (pp)`
    - `CAC Check Delta`
    with corresponding SK mappings (`Kontrola ... delta`),
  - updated translation dictionaries and replacement maps to match the new labels and avoid previous symbol-encoding drift.
- Validation executed:
  - `python -m py_compile html_report_generator.py`,
  - full regenerate (no email) with final outputs tag `lang_full3`:
    - VEVO: `data/vevo/report_20250503-20260331__lang_full3.html`
    - ROY: `data/roy/report_20250922-20260331__lang_full3.html`
  - verified generated HTML contains:
    - project title headers (`Vevo reporting` / `Roy reporting`),
    - language switch texts with SK+EN variants,
    - user-friendly KPI cheat-sheet block,
    - updated Delta KPI labels.

- Sidebar/menu test track for main VEVO reporting completed on 2026-04-02:
  - redesigned the main report shell in `html_report_generator.py` to a dashboard-style layout with:
    - sticky left sidebar,
    - metric-group menu (`Overview`, `Revenue & profitability`, `Customers & retention`, `Marketing & ads`, `Geography`, `Products`, `Operations & diagnostics`),
    - section-level group switching without changing any business calculations,
    - warmer executive dashboard styling closer to modern admin dashboards.
  - wrapped the long report into navigable dashboard sections while preserving existing charts/tables and SK/EN language switching,
  - added client-side section filter persistence via `localStorage` (`reportMetricGroup`),
  - generated only one stable VEVO March test artifact:
    - `data/vevo/report_20260301-20260331__test.html`
  - cleaned previous VEVO/ROY tagged test artifacts (`__lang_*`, `__ui_*`, `__ui_test`, etc.) so only:
    - original untagged reports remain,
    - one current VEVO test HTML remains for UI review.

Next exact step:
- Review `data/vevo/report_20260301-20260331__test.html` visually and decide whether the new professional period switcher should stay as the baseline UX for the dashboard test track before deeper chart-visual redesign starts.

### 2026-04-02
- Extended the isolated VEVO March `test2` dashboard so it keeps the `test2` hero/intro shell while pulling in richer analytics previously available only in the fuller report/test track.
- `dashboard_test2.py` now renders additional data groups:
  - customer retention and concentration,
  - refund trend,
  - cohort retention chart + table,
  - calendar patterns (day-of-week, week-of-month, day-of-month),
  - weather uplift,
  - geo profitability table,
  - product margin breakout chart,
  - product trend chart + table.
- `html_report_generator.py` now passes the richer analytics payloads into the isolated `test2` renderer:
  - `day_of_week_analysis`
  - `week_of_month_analysis`
  - `day_of_month_analysis`
  - `weather_analysis`
  - `geo_profitability`
  - `product_trends`
  - `customer_concentration`
  - `cohort_analysis`
- Kept the `test2` top section intact:
  - hero header,
  - side language switcher,
  - period switcher,
  - executive KPI deck.
- Fixed the `test2` sidebar so the project badge uses the project initial dynamically and the navigation now includes the new `Patterns` section with correct ordering.
- Verified with:
  - `python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py`
  - successful VEVO March regenerate:
    - `data/vevo/report_20260301-20260331__test2.html`
  - HTML presence checks for:
    - `Executive KPI deck`
    - `Customer quality and retention`
    - `Calendar patterns and weather`
    - `Geo profitability`
    - `Product trend table`

### 2026-04-02
- Added reusable CFO KPI payload builder in `reporting_core/cfo_kpis.py` so the main report can reuse the same executive KPI logic as the standalone CFO dashboard.
- Wired `export_orders.py` to compute `cfo_kpi_payload` from the existing report data (`date_agg` + exported order rows) without changing the underlying financial calculations.
- Injected a new top-of-report CFO KPI panel into `html_report_generator.py`:
  - placed above the old summary cards,
  - uses the same KPI set as the CFO dashboard,
  - supports `Daily / Weekly / Monthly` switching,
  - respects the existing SK/EN language switch,
  - uses the new dashboard shell styling instead of the legacy card layout.
- Verified syntax with:
  - `python -m py_compile export_orders.py html_report_generator.py reporting_core\\__init__.py reporting_core\\cfo_kpis.py`
- Regenerated only the VEVO March test artifact (no email):
  - `data/vevo/report_20260301-20260331__test.html`
- Verified the generated HTML contains the new executive block and embedded KPI payload (`CFO_TOP_KPI`) with the expected metrics:
  - Revenue
  - Profit
  - Orders
  - AOV
  - CAC
  - ROAS
  - Pre-Ad Contribution Margin
  - Post-Ad Margin
  - Company Margin (incl. fixed)

### 2026-04-02
- Reverted the two latest test-only date-range UI experiments from the VEVO March dashboard prototype:
  - removed the global chart date-range filter,
  - removed the per-section chart date-range filters.
- Restored the test UI baseline to the previous state:
  - sidebar navigation stays,
  - top CFO KPI band stays,
  - no chart-range controls are rendered.
- Regenerated the VEVO March test artifact after the revert:
  - `data/vevo/report_20260301-20260331__test.html`
- Verified the regenerated HTML no longer contains the removed range UI markers (`chart-range-panel`, `chart-range-start`, `chart-range-end`).

### 2026-04-02
- Implemented a server-driven professional period switcher for the VEVO March dashboard test track without touching production outputs.
- `export_orders.py` now builds preset report variants for tagged/test exports and links them as full-report period views instead of doing client-side chart cropping:
  - `7D`
  - `30D`
  - `90D` when the selected range is long enough
  - `FULL`
- Added reusable helpers for:
  - period-range slicing from already fetched orders,
  - preset period-spec generation,
  - relative-link payload generation for parent/child report variants.
- `html_report_generator.py` now renders the same period switcher:
  - globally at the top,
  - inside every major dashboard section (`Overview`, `Business`, `Customers`, `Marketing`, `Geography`, `Customer structure`, `Products`, `Operations`).
- Section links preserve anchors (for example `#section-marketing`) and the dashboard JS reopens the correct sidebar metric group after cross-period navigation.
- Generated only test artifacts under the hidden bundle path for tagged outputs, keeping the visible top-level test report as the main entry point:
  - `data/vevo/report_20260301-20260331__test.html`
  - `data/vevo/_periods/report_20260301-20260331__test/7d/...`
  - `data/vevo/_periods/report_20260301-20260331__test/30d/...`
- Verified:
  - syntax with `python -m py_compile export_orders.py html_report_generator.py`,
  - successful VEVO March test regenerate with variant bundle creation,
  - zero leftover literal `{render_period_switcher(...)}`
  - working period-switcher links for all major sections in both the main report and child period variants.

### 2026-04-02
- Merged the richer analytics payload from the fuller reporting build into the isolated `test2` dashboard track while keeping the `test2` intro/hero shell unchanged.
- `dashboard_test2.py` now renders additional sections from the richer reporting data:
  - customer quality and retention,
  - calendar patterns and weather,
  - geo profitability,
  - product trend breakout/table.
- `html_report_generator.py` passes the richer analytics payload through to `generate_test2_dashboard(...)`.
- Fixed a `test2` serialization bug by replacing the raw `customer_concentration` DataFrame payload with a JSON-safe summary object in the dashboard bootstrap payload.
- Cleaned the visible Slovak labels in `test2` that were previously mojibake/broken:
  - `Kvalita zákazníkov a retencia`
  - `Toto rozširuje pekný ...`
  - `Denná miera refundov odhaľuje operačné problémy, nie len súčet.`
- Regenerated and verified the VEVO March `test2` artifact:
  - `data/vevo/report_20260301-20260331__test2.html`
- Verified:
  - syntax with `python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py`
  - successful VEVO March `test2` regenerate
  - expected sections present in HTML
  - cleaned Slovak strings present in final HTML output

### 2026-04-02
- Expanded the VEVO March `test2` dashboard so it keeps the preferred `test2` shell/hero design but now pulls in the much richer metric surface from the fuller `test` reporting line.
- `html_report_generator.py` now passes the full analytics payload families into `generate_test2_dashboard(...)`, including customer/retention, CLV/CAC, order-size, combinations, advanced DTC, B2B/B2C, order status, ads effectiveness, lifecycle segments, first-item retention, same-item repurchase, time-to-nth-by-first-item, detailed FB metrics, cost-per-order, hourly/day-of-week Meta stats, LTV by date, and consistency checks.
- `dashboard_test2.py` now renders a `Full metric library` layer inside the `test2` design shell with added chart galleries for:
  - customer quality and repeat behavior,
  - calendar and weather patterns,
  - product and operational drilldowns,
  - economics and marketing drilldowns.
- Verified syntax with:
  - `python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py`
- Regenerated only the VEVO March `test2` artifact (no email):
  - `data/vevo/report_20260301-20260331__test2.html`
- Verified:
  - export completed successfully,
  - inline dashboard script parses successfully in Node (`new Function(...)`),
  - new gallery chart ids and render calls are present in the generated `test2` HTML.
- Next exact step:
  - visually review `report_20260301-20260331__test2.html` in the browser and decide which `test2` sections/cards should replace the legacy report layout next.

### 2026-04-03
- Extended VEVO March 	est2 so the design shell stays unchanged but the metric coverage moves much closer to the original 	est report.
- dashboard_test2.py now fills the previously empty standalone library containers:
  - libraryEconomicsStandalone
  - libraryMarketingStandalone
  - libraryCustomersStandalone
- Added standalone charts for missing metric families from the legacy report, including:
  - economics: revenue vs total cost, total costs, product costs, gross margin, packaging, shipping, fixed costs, items sold, avg items per order, scatter revenue vs cost, all-metrics overview, LTV by acquisition date, LTV-based profit
  - marketing: FB spend, Google spend, FB vs Google spend, spend vs clicks, campaign conversion rate, cost per conversion, CTR, CPC, spend share, campaign CPO, campaign ROAS, spend bucket orders
  - customer value: refund amount, CLV, CAC, CLV vs CAC, LTV/CAC ratio, return time, payback trend
- Added scroll-aware sidebar navigation in 	est2:
  - the active menu item now switches based on the visible section instead of staying hardcoded on Overview
  - sidebar links now smooth-scroll to the relevant section and update browser hash
- Verified:
  - python -m py_compile dashboard_test2.py html_report_generator.py export_orders.py
  - VEVO March 	est2 regenerate completed successfully
  - final inline dashboard script parses successfully in Node
  - standalone library containers and new chart ids are present in data/vevo/report_20260301-20260331__test2.html
- Next exact step:
  - visually review 
eport_20260301-20260331__test2.html and decide whether the remaining legacy tables should also be redesigned into 	est2 cards/panels or left outside the dashboard shell.
### 2026-04-04
- Added an `Executive metrics tile deck` to the end of section `10 Full library` in the modern production dashboard, keeping the current dashboard design while surfacing all major top-level KPI metrics in a compact tile grid.
- `dashboard_modern.py` now computes and renders a large summary tile set covering revenue, cost stack, profit, daily averages, orders/items, AOV, CAC/ROAS/MER, revenue per customer, contribution layers, break-even CAC, CAC headroom, payback, refund summary, repeat purchase rate, and related executive checks.
- Added reusable helpers for tile formatting/styling and new tile-grid CSS so the metrics render as readable dashboard cards instead of legacy summary boxes.
- Verified with:
  - `python -m py_compile dashboard_modern.py html_report_generator.py export_orders.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Verified in generated output:
  - `data/vevo/report_20260301-20260331.html`
  - tile deck heading is present in `Full library`
  - tile labels such as `Total revenue (net)`, `Revenue LTV/CAC`, `ROI`, and `Repeat purchase rate` render in the final HTML.
- Next exact step:
  - visually review the new tile deck in the March VEVO report and decide whether any low-signal tiles should be removed or regrouped.
### 2026-04-08
- Fixed the modern dashboard global period switcher architecture so it can work from a single emailed HTML attachment instead of depending only on sibling `_periods/...` files being present on disk.
- `dashboard_modern.py` now tags each global period link with a stable `data-period-key`, persists canonical period hrefs, and injects embedded report variants into non-full period transitions so 7D / 30D / 90D switching can work in a single-file/offline context.
- `html_report_generator.py` now passes `embedded_period_reports` through to the modern dashboard renderer.
- `export_orders.py` now builds a lightweight embedded period bundle (base64 child variants for non-full ranges) for the main/full report so the sidebar global time switcher has local content to swap to.
- `reporting_core/cfo_kpis.py` now includes `secondary_metrics` in KPI windows; the modern dashboard uses that to show nominal company profit beneath `Company margin (incl. fixed)` in the Executive KPI deck.
- Verified with:
  - `python -m py_compile dashboard_modern.py html_report_generator.py export_orders.py reporting_core/cfo_kpis.py`
  - a direct synthetic render smoke test through `generate_html_report(...)` confirming:
    - `data-period-key` is present,
    - embedded period report bootstrap is present,
    - Company margin KPI renders a secondary nominal profit value.
- Note:
  - live VEVO March export fetch timed out during API work, so runtime verification for the full real report should be rechecked in the next session after a successful export run.
- Next exact step:
  - run a full VEVO export successfully and verify that sidebar period switching works end-to-end from the generated emailed HTML artifact, not just from local disk bundle files.
### 2026-04-08 (runtime verification update)
- Re-ran a full real VEVO March export after the single-file period-switch fix:
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Runtime verification now succeeded end-to-end; the generated report is:
  - `data/vevo/report_20260301-20260331.html`
- Verified on the generated HTML artifact:
  - embedded period bundle is present (`INLINE_EMBEDDED_PERIOD_REPORTS`)
  - period switch links render with `data-period-key`
  - Executive KPI deck includes secondary nominal company profit rendering (`kpi-secondary`) for `Company margin (incl. fixed)`
  - sibling period HTML variants exist under `_periods/report_20260301-20260331/...`
- Outcome:
  - the previously incomplete verification from the earlier 2026-04-08 entry is now closed; full export/runtime generation works for the fixed implementation.
- Next exact step:
  - visually verify from the actual emailed HTML attachment context that 7D / 30D switching behaves correctly in the browser the user uses to open the attachment.
### 2026-04-08 (executive KPI trend strip)
- Added recent trend visualization directly into the `Executive KPI deck` so the top CFO cards show not only current KPI values and comparison deltas, but also short operational trend context.
- `reporting_core/cfo_kpis.py` now builds server-side trend payloads for each KPI window:
  - daily: last 14 daily points
  - weekly: last 8 rolling 7-day points
  - monthly: last 8 rolling 30-day points
- `dashboard_modern.py` now renders per-card sparkline strips with a compact trend delta label inside each Executive KPI card.
- The existing `Company margin (incl. fixed)` secondary nominal profit stays in place and now sits alongside the new trend strip.
- Verified with:
  - `python -m py_compile dashboard_modern.py reporting_core/cfo_kpis.py html_report_generator.py export_orders.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Verified in generated output:
  - `data/vevo/report_20260301-20260331.html`
  - KPI trend CSS/JS markers are present (`kpi-trend`, `sparklineSvg`)
  - report regeneration completed successfully end-to-end
- Next exact step:
  - visually review the Executive KPI deck in the latest VEVO report and decide whether the sparklines should be made denser/subtler or whether a separate mini trend row is needed for any specific KPI.
### 2026-04-08 (VEVO runtime build + scheduler verification)
- Verified that merging the Executive KPI trend-strip change into `main` did not automatically rebuild the VEVO runtime image because `build-and-push-ecr.yml` was not watching `dashboard_modern.py` or `reporting_core/**`.
- Manually dispatched the `Build and Push ECR` workflow and confirmed a fresh `latest` image in ECR:
  - repository: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - newest pushed digest tagged `latest`: `sha256:57a95c3fa57ea5d53e081fd48f340800585a2f4901dd118d039a816719fd090b`
- Confirmed production runtime identifiers before deploy:
  - scheduler: `vevo-daily-report-email`
  - cluster: `vevo-reporting-cluster`
  - task definition before fix: `vevo-reporting-daily:3`
  - log group: `/ecs/vevo-reporting-daily`
  - runtime secret: `vevo/reporting/runtime-env`
- Detected runtime drift in ECS task definition `:3`:
  - Google Ads credentials existed in Secrets Manager,
  - but were not mapped into the container secret env list,
  - which caused the container to log `Google Ads credentials not fully configured`.
- Registered new ECS task definition revision `vevo-reporting-daily:4` with all Google Ads secret mappings added:
  - `GOOGLE_ADS_DEVELOPER_TOKEN`
  - `GOOGLE_ADS_CLIENT_ID`
  - `GOOGLE_ADS_CLIENT_SECRET`
  - `GOOGLE_ADS_REFRESH_TOKEN`
  - `GOOGLE_ADS_CUSTOMER_ID`
  - `GOOGLE_ADS_LOGIN_CUSTOMER_ID`
- Updated scheduler `vevo-daily-report-email` to target `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:4`.
- Ran manual verification tasks:
  - revision `:3` task showed the missing-Google warning,
  - revision `:4` task started successfully from the new image digest and no longer emitted the early missing-Google-credentials warning.
- Regenerated a fresh local full-history VEVO report without sending email:
  - `python daily_report_runner.py --project vevo --from-date 2025-05-03 --to-date 2026-04-08 --skip-email`
  - generated artifact: `data/vevo/report_20250503-20260408.html`
- Next exact step:
  - merge the workflow path fix so future dashboard/runtime merges rebuild ECR automatically without requiring a manual dispatch.
### 2026-04-08 (weather archive cutoff fix)
- Fixed Open-Meteo archive integration in `weather_client.py`.
- Root cause:
  - weather cache/fetch logic requested whole calendar months,
  - for in-progress months that meant requests like `2026-04-01 -> 2026-04-30`,
  - Open-Meteo archive API rejects future days, so the request returned `400 Bad Request`.
- Implemented fix:
  - clamp weather fetches to the last historically available day (`UTC today - 1 day`),
  - return empty weather payload if the requested month starts after the archive cutoff,
  - use distinct cache keys for partial months (`_through_YYYYMMDD`) so incomplete current-month caches do not freeze and block later refreshes.
- Verified with:
  - `python -m py_compile weather_client.py`
  - direct WeatherClient fetch for `2026-04-01 -> 2026-04-07` returned 7 rows successfully
  - full real VEVO report run: `python daily_report_runner.py --project vevo --from-date 2025-05-03 --to-date 2026-04-07 --skip-email`
- Verification result:
  - weather warning `400 Client Error` is gone from the report run,
  - report generated successfully: `data/vevo/report_20250503-20260407.html`.
- Next exact step:
  - merge the weather fix branch into `main` so tomorrow's runtime image can include the corrected weather behavior on the next build/deploy cycle.
- Follow-up deployability fix:
  - `.github/workflows/build-and-push-ecr.yml` now also watches `weather_client.py` and `http_client.py`,
  - so future weather/runtime HTTP changes will automatically rebuild the VEVO ECR image after merge to `main`.
### 2026-04-08 (GitHub Actions Node 24 readiness)
- Upgraded GitHub Actions workflow dependencies that were still running on the deprecated Node 20 action runtime.
- Updated:
  - `.github/workflows/build-and-push-ecr.yml`
    - `actions/checkout@v4` -> `actions/checkout@v5`
    - `aws-actions/configure-aws-credentials@v4` -> `aws-actions/configure-aws-credentials@v5.1.1`
  - `.github/workflows/env-check.yml`
    - all `actions/checkout@v4` -> `actions/checkout@v5`
  - `.github/workflows/observability-check.yml`
    - `actions/checkout@v4` -> `actions/checkout@v5`
- Scope intentionally limited to the actions explicitly causing deprecation warnings in recent ECR workflow runs.
- Verified locally by reviewing all workflow YAML references after the upgrade.
- Next exact step:
  - push the branch, open a PR, and verify on GitHub Actions that the deprecation warning is gone from the next workflow run.
### 2026-04-08 (force Node 24 runtime for GitHub Actions)
- Verified that upgrading to the latest pinned workflow actions removed `actions/checkout@v4` but did not fully remove the GitHub deprecation annotation because `aws-actions/configure-aws-credentials@v5.1.1` still runs on the older JavaScript action runtime.
- Added `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: "true"` at workflow level in:
  - `.github/workflows/build-and-push-ecr.yml`
  - `.github/workflows/env-check.yml`
  - `.github/workflows/observability-check.yml`
- This uses GitHub's documented opt-in path so JavaScript actions execute on Node 24 now, instead of waiting for the future runner default switch.
- Next exact step:
  - push the branch, run/observe the next workflow execution, and confirm the deprecation annotation is gone.
### 2026-04-08 (GitHub Actions v6 action pins)
- After validating the GitHub release feeds, upgraded workflow pins further to the current major releases:
  - `actions/checkout@v6.0.2`
  - `aws-actions/configure-aws-credentials@v6.1.0`
- Kept the `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` workflow env override in place as an additional safety measure during the Node 24 transition window.
- Next exact step:
  - push the branch, merge it, and verify on the next `Build and Push ECR` run that the old Node 20 deprecation annotation no longer appears.
### 2026-04-10 (shared reporting P0 audit fixes)
- Audited the external recommendations against the real VEVO/ROY reporting code and prioritized only the issues that were actually reproducible in the current codebase.
- Confirmed and fixed shell-vs-library binding drift in the modern dashboard:
  - economics mini-cards now read the same financial registry values as the full-library tiles for:
    - `pre_ad_contribution_per_order`
    - `break_even_cac`
    - `payback_orders`
    - `contribution_ltv_cac`
- Fixed refund shell binding to use `refunds_analysis.summary` consistently across shell cards, payload and full-library tiles.
- Added shared render-time normalization for dimension hydration, so UI consumers stop rendering empty/placeholder labels when the producer already has equivalent fields:
  - `day_name <- day_of_week`
  - `anchor_item <- key_product`
  - `anchor_orders <- key_orders`
  - `attached_item <- attached_product`
  - `pre_ad_contribution_margin_pct <- pre_ad_margin_pct`
  - `cum_contribution_pct <- cum_contribution_share_pct`
- Hardened null propagation / source coverage semantics:
  - source health `status=ok/manual` is no longer treated as metric availability when coverage is zero,
  - VEVO `google_ads.active_days=0` now renders `Google CPO = N/A` instead of a misleading `€0.00`,
  - ROY keeps numeric Google CPO because its manual Google source has positive active-day coverage.
- Clarified campaign semantics in the Facebook ingestion/rendering path:
  - preserved platform fields separately from attribution estimates,
  - campaign rows now expose `platform_conversions` and `cost_per_platform_conversion`,
  - attribution rows now expose `attributed_orders_est`, `cost_per_attributed_order`, and `attribution_method`,
  - CPO analysis now emits `campaign_attribution_summary` with `coverage_ratio` and `oversubscription_ratio`.
- Verified with real March 2026 regenerations:
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Smoke verification outcome:
  - VEVO shell economics cards show real values instead of zero fallbacks,
  - VEVO shell `Google CPO` now shows `N/A`,
  - ROY shell economics cards show real values and keep numeric Google CPO,
  - no remaining `null` hydration symptoms were found in generated HTML for weekday / attach-rate / geo consumer labels during targeted checks.
- Follow-up hardening completed:
  - added explicit attribution QA metadata into `source_health.qa.attribution`,
  - QA now evaluates campaign spend coverage, oversubscription and platform CPA arithmetic mismatches,
  - `source_health.overall_status` now escalates to `warning` when QA warnings exist even if raw sources loaded cleanly,
  - modern dashboard now surfaces attribution QA twice:
    - as a health card in the source health grid,
    - as a dedicated marketing panel (`Attribution QA guardrails`) with coverage, oversubscription, campaign-row count and CPA mismatch count,
  - fixed mojibake / bad currency rendering inside the modern marketing section (`&euro;`, ASCII-safe SK copy for reconciliation text),
  - added CI assertions so regressions fail if:
    - attribution QA builder is removed from export,
    - campaign attribution summary disappears,
    - dashboard stops rendering the attribution QA panel.
- Verified with:
  - `python -m py_compile dashboard_modern.py export_orders.py facebook_ads.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO `data_quality_20260301-20260331.json` now contains attribution QA metadata with `qa_status=ok`,
  - ROY `data_quality_20260301-20260331.json` now contains attribution QA metadata with `qa_status=warning`,
  - VEVO and ROY modern reports render the new QA panel and reconciliation values with proper euro symbols,
  - ROY now explicitly warns in dashboard that campaign-level Facebook spend coverage is missing while daily spend exists.
- Next exact step:
  - add thresholded attribution warning banners to the hero/executive shell so severe coverage or oversubscription issues are visible before the user reaches the marketing section.
### 2026-04-10 (hero attribution warning banner)
- Added a thresholded attribution warning banner to the modern dashboard hero shell in `dashboard_modern.py`.
- The banner now appears before the Executive KPI deck whenever attribution QA emits warnings, with severity-aware styling:
  - `critical` for missing campaign spend coverage, empty campaign attribution tables, severe coverage drift, severe oversubscription, or platform CPA mismatches,
  - `warning` for softer attribution QA issues.
- The hero banner exposes the key QA diagnostics directly in the shell:
  - coverage ratio
  - oversubscription ratio
  - CPA mismatch count
  - campaign row count
- The existing raw warning list is reused in the hero banner so the same QA evidence is visible both:
  - in the shell,
  - and later in the marketing section.
- Added a CI assertion in `scripts/security_ci.py` so the build fails if the hero-level attribution warning surface is removed from the modern dashboard.
- Verified with:
  - `python -m py_compile dashboard_modern.py export_orders.py facebook_ads.py scripts\\security_ci.py`
  - `python scripts\\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO March 2026 report stays clean with no hero attribution banner when QA is healthy,
  - ROY March 2026 report now shows a critical hero attribution banner before the Executive KPI deck,
  - CI passes with the new shell-level guard in place.
- Next exact step:
  - start the Vevo sample funnel model as the next shared business-modeling expansion after the P0 reporting hardening is now visible in the shell.
### 2026-04-10 (Vevo sample funnel model)
- Added a Vevo sample funnel model in `export_orders.py` to track first-order sample-entry customers into repeat and full-size conversion windows.
- Entry cohort definition is now explicit:
  - first order contains at least one sample item
  - first order does not contain a full-size item
- The model computes and exports:
  - repeat conversion by 7/14/30/60/90 day windows
  - any full-size conversion by 7/14/30/60/90 day windows
  - 200ml conversion by 7/14/30/60/90 day windows
  - 500ml conversion by 7/14/30/60/90 day windows
  - top sample entry-product quality rows ranked by downstream conversion
- Added the sample funnel payload pass-through to:
  - `html_report_generator.py`
  - `dashboard_modern.py`
- Added a dedicated sample funnel block in the main Customers section of the modern dashboard:
  - entry customers
  - repeat 30d
  - full-size 30d
  - full-size 60d
  - median days to full-size
  - top entry product
  - sample funnel window chart
  - entry-product quality table
- Added customer-library drilldowns in the modern dashboard:
  - `custSampleFunnelWindowChart`
  - `custSampleEntryProductChart`
- Hardened the sample funnel implementation so it uses robust revenue-field fallback selection instead of assuming item-level revenue columns exist in every export/sub-period bundle.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO March 2026 export completed successfully
  - sample funnel CSV outputs were generated for the 7d and full March bundles
  - `data\\vevo\\report_20260301-20260331.html` contains:
    - `sampleFunnelChart`
    - `custSampleFunnelWindowChart`
    - `custSampleEntryProductChart`
- Next exact step:
  - start the Roy bundle/accessory model as the next business-model expansion after the Vevo sample funnel model is now live in the modern dashboard.
### 2026-04-10 (Roy bundle and accessory model)
- Added Roy-specific anchor device families and accessory groups to `projects/roy/settings.json` so bundle economics no longer depends on ad-hoc string logic in dashboard code.
- Added a dedicated Roy bundle/accessory model in `export_orders.py` that computes:
  - pair-level attach rate
  - incremental order contribution uplift
  - anchor device family summary
  - accessory group summary
- Exposed the new Roy bundle/accessory payload through the modern dashboard renderer in `dashboard_modern.py`.
- Added new Roy charts and tables:
  - `prodBundleAccessoryAttachChart`
  - `prodBundleAccessoryUpliftChart`
  - `prodBundleAccessoryFamilyChart`
  - `prodBundleAccessoryGroupChart`
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - Roy March 2026 report regenerated successfully
  - new bundle/accessory charts render in `data\\roy\\report_20260301-20260331.html`
- Next exact step:
  - add cohort-normalized CAC / LTV / payback views so global shortcut metrics are complemented by acquisition-cohort recovery curves.
### 2026-04-10 (Cohort-normalized unit economics)
- Added cohort-normalized CAC / LTV / payback views in `export_orders.py` for both VEVO and ROY.
- The cohort model now computes 30/60/90/180-day acquisition-cohort views with:
  - customers
  - revenue LTV
  - contribution LTV
  - contribution LTV / CAC
  - recovery percentage
  - average / median payback days
- Added cohort-normalized charts to the modern dashboard in `dashboard_modern.py` for both projects:
  - `custCohortContributionLtvCacChart`
  - `custCohortPaybackRecoveryChart`
  - `custCohortCacVsContributionChart`
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project vevo --from-date 2025-05-03 --to-date 2026-04-09`
  - `python export_orders.py --project roy --from-date 2025-09-24 --to-date 2026-04-09`
- Verification outcome:
  - full-history VEVO and ROY reports regenerate successfully
  - both reports now contain cohort-normalized unit-economics charts in the customer section / full library
- Next exact step:
  - normalize shipping sign semantics so positive values always mean business cost and negative values mean shipping profit, then update labels and formulas consistently across runtime, export and dashboard layers.
### 2026-04-10 (Shipping net semantics cleanup)
- Replaced ambiguous `shipping_subsidy_per_order` semantics with canonical `shipping_net_per_order` in the runtime/config layer:
  - positive value = business shipping cost
  - negative value = shipping profit / over-recovery
- Added runtime alias handling in `reporting_core/runtime.py` so existing settings can still load, while new project configs and templates now use:
  - `shipping_net_per_order`
- Updated project settings:
  - `projects/vevo/settings.json` now uses `shipping_net_per_order: 0.2`
  - `projects/roy/settings.json` now uses `shipping_net_per_order: -0.2`
  - `templates/reporting-client/settings.template.json` now uses `shipping_net_per_order`
- Updated export math in `export_orders.py` to use canonical `shipping_net_cost` in:
  - daily aggregation
  - total cost
  - pre-ad contribution
  - post-ad contribution
  - geo profitability
  - financial summaries
- Preserved backward-compatible aliases where needed so existing consumers do not break, but all key formulas now read `shipping_net_cost` first.
- Updated downstream readers:
  - `reporting_core/cfo_kpis.py`
  - `daily_report_runner.py`
  - `dashboard_modern.py`
  - `html_report_generator.py`
- Dashboard/UI cleanup:
  - renamed visible labels from `Shipping Subsidy` to `Net shipping`
  - modern dashboard tiles and charts now explain that positive means cost and negative means shipping profit
  - legacy/shared generator fallbacks now read `shipping_net_cost` before old subsidy aliases
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py reporting_core\\runtime.py reporting_core\\cfo_kpis.py daily_report_runner.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO and ROY March 2026 reports regenerate successfully
  - modern HTML outputs now render `Net shipping` instead of the ambiguous subsidy label
  - shipping math stays stable while sign semantics are now explicit and consistent
- Next exact step:
  - add full QA assertions in pipeline for shell/library parity, campaign arithmetic integrity and normalized-dimension completeness (`day_name`, `anchor_item`, `country`).
### 2026-04-10 (CM taxonomy surfaced + full data QA assertions)
- Added explicit CM1 / CM2 / CM3 taxonomy aliases and dashboard cards so the economics section no longer depends only on legacy pre-ad/post-ad naming.
- Added pipeline-level `data_assertions` QA in `export_orders.py` covering:
  - shell vs library parity for key economics metrics
  - campaign CPA arithmetic integrity
  - normalized dimension completeness (`day_name`, `anchor_item`, `attached_item`, `anchor_orders`, `country`)
  - attributed orders tolerance vs total orders
  - refund registry presence and consistency deltas
- Added `margin_stability` QA with 7-day smoothing for fixed-margin alerting, including raw vs smoothed extreme-day counts and min/max smoothed margin bounds.
- Wired both QA builders into `source_health.qa` so they render in the modern dashboard and participate in warning propagation.
- Added modern dashboard sections:
  - `CM1 / CM2 / CM3 taxonomy`
  - `Data assertions`
  - `Smoothed fixed-margin alerts`
- Extended `scripts/security_ci.py` so CI now fails if these new QA builders / dashboard sections disappear.
- Fixed a runtime bug in monthly aggregation where `cm3_margin_pct` incorrectly referenced a non-existent `month_agg['profit_margin_pct']`; it now computes directly from `net_profit / total_revenue`.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py scripts\security_ci.py`
  - `python scripts\security_ci.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO and ROY March 2026 reports regenerate successfully
  - new QA sections render without breaking period bundles
  - consistency checks remain green after CM taxonomy exposure
- Next exact step:
  - add acquisition-source x product-family cube for ROY and VEVO so channel efficiency can be evaluated by product family instead of only globally.
### 2026-04-11 (Acquisition-source x product-family cube)
- Added order-level ad spend hydration into the advanced DTC pipeline so first-order source proxies can be derived consistently from the first purchase day.
- Wired the existing `analyze_acquisition_source_product_family_cube(...)` model into `analyze_advanced_dtc_metrics(...)` and exposed it in the exported advanced metrics payload as:
  - `acquisition_product_family_cube`
- Extended the modern dashboard payload in `dashboard_modern.py` with:
  - `acquisition_family.cube_rows`
  - `acquisition_family.source_rows`
  - `acquisition_family.family_rows`
  - `acquisition_family.summary`
- Added three new marketing library charts to the modern dashboard for both VEVO and ROY:
  - `Source proxy x product family`
  - `90d contribution by source proxy x family`
  - `Source proxy summary`
- The new view is explicitly proxy-based, using paid-day presence (`facebook_paid_day`, `google_paid_day`, `mixed_paid_day`, `organic_unknown_day`) rather than pretending to be exact order-level attribution.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO and ROY March 2026 reports regenerate successfully
  - both rendered HTML reports contain the new acquisition-family charts and chart bindings
  - no regression in existing advanced DTC or marketing sections
- Next exact step:
  - add Vevo cohort refill model so refill timing is measured by first-item cohort and horizon, not only by generic repeat-purchase logic.

### 2026-04-11 (revenue + manual ads regression fix)
- Disabled manual ads totals as a fallback path unless a project explicitly opts into `prefer_manual_ads_totals=true`.
- Removed Roy manual FB/Google totals from project settings so Roy now relies only on live Meta/Google Ads sources.
- Namespaced Facebook and Google Ads caches by ad account/customer IDs to avoid cross-project cache pollution.
- Fixed order-item revenue sourcing to prefer BizniWeb explicit line totals (`items.sum` as net, `items.sum_with_tax` as gross) instead of inferring VAT from unreliable `is_net_price` flags.
- Modern dashboard KPI labels now explicitly mark revenue and AOV as net metrics.
- Verified with no-cache exports:
  - Roy full range `2025-09-24 .. 2026-04-10`
  - Vevo full range `2025-05-03 .. 2026-04-10`
- Roy now connects to the live Google Ads account `Roy.sk` (`5313708530` via MCC `6704852923`) and no longer uses the old fixed spend fallback.
- Vevo now connects to the live Google Ads account `Vevo.sk` (`7592903323`) with no fixed-spend fallback.

### 2026-04-11 (short SES email body regression fix)
- Identified that `daily_report_runner.py` still sent the old long-form executive summary in the SES plain-text body.
- Replaced the body template with a short production mail:
  - attachment notice
  - covered date range
  - concise data quality status
  - one short QA warning note only when needed
- Removed the old `build_report_summary(...)` output from the actual SES send path; the long CFO-style narrative is no longer injected into the mail body.
- Verified locally with:
  - `python -m py_compile daily_report_runner.py`
  - direct function render of `build_email_body(...)`
- Expected runtime effect:
  - the daily scheduled VEVO mail should again send the short, clear body once the updated image is built and pulled by the scheduled ECS task.

### 2026-04-11 (Vevo cohort refill model + reporting QA smoke)
- Added a dedicated Vevo refill cohort model in `export_orders.py` so refill timing is measured by first-order entry bucket and cohort month, not only by generic repeat-purchase logic.
- Export now produces refill artifacts:
  - `refill_cohort_buckets_<range>.csv`
  - `refill_cohort_windows_<range>.csv`
  - `refill_cohort_months_<range>.csv`
- Wired `refill_cohort_analysis` through `html_report_generator.py` into the modern dashboard renderer.
- Extended `dashboard_modern.py` with a refill cohort block in the Customers section and new full-library charts:
  - `Refill cohort timing`
  - `Refill bucket quality`
  - `custRefillWindowChart`
  - `custRefillBucketChart`
  - `custRefillCohortChart`
- Fixed refund QA parity checks so refund summary metrics are asserted against the shared financial registry instead of only checking presence.
- Added `scripts/reporting_qa_smoke.py` and wired it into `env-check.yml` plus `scripts/security_ci.py` so behavior-level reporting QA runs in CI, not only static checks.
- Verified with:
  - `python -m py_compile export_orders.py html_report_generator.py dashboard_modern.py scripts\security_ci.py scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `python scripts\reporting_qa_smoke.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
- Verification outcome:
  - VEVO March 2026 report regenerates successfully
  - refill cohort charts render in the modern dashboard without breaking existing sections
  - reporting QA smoke passes locally and is now enforced by CI
- Next exact step:
  - merge this step to `main`, then implement shared geo confidence scoring and low-sample geo guardrails for both VEVO and ROY.

### 2026-04-11 (B2B/B2C unit economics + lifecycle proxy)
- Fixed `_build_growth_order_item_frames(...)` so order-level fixed-overhead allocation now uses the same project/runtime daily fixed-cost logic as the main report instead of the old `CFO_FIXED_DAILY_COST_EUR` fallback.
- Added `excluded_status_orders` tracking in the fetch/filter pipeline so excluded payment-failure orders can be surfaced analytically without polluting reportable revenue exports.
- Expanded `analyze_b2b_vs_b2c(...)` from a raw split into a segment unit-economics view with:
  - CM1 / CM2 / CM3 profit
  - revenue per customer
  - repeat-customer rate
  - CM2 / CM3 per order
  - new vs returning order counts
- Expanded `analyze_order_status(...)` into two layers:
  - final-status mix
  - explicit lifecycle proxy buckets built from final statuses + tracked excluded payment failures
- Updated the modern dashboard to render the new analytics in the active shell and operations library:
  - lifecycle proxy chart + table
  - B2B/B2C unit economics table
  - B2B/B2C unit-economics library chart
  - lifecycle proxy library chart
  - final-status table now shows reportable CM2 per order
- Verified with:
  - `python -m py_compile export_orders.py dashboard_modern.py html_report_generator.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - `python scripts\\reporting_qa_smoke.py`
- Verification outcome:
  - VEVO and ROY March 2026 exports regenerate successfully on `codex/segment-unit-econ-lifecycle`
  - both `report_20260301-20260331.html` outputs contain:
    - `orderLifecycleProxyChart`
    - `opsLifecycleProxyChart`
    - `opsB2bUnitEconomicsChart`

### 2026-04-11 (VEVO/ROY dashboard fixed vs no-fixed profit views)
- Transferred the April-side fixed / no-fixed profit presentation into the active dashboard line without overwriting the newer CM-based runtime logic.
- Extended export-side analytics to emit explicit with-fixed and without-fixed economics across the affected drilldowns:
  - weekday / week-of-month / day-of-month patterns
  - weather impact
  - geographic profitability
  - product margins
  - customer concentration
  - ads effectiveness
  - basket contribution
  - SKU Pareto
- Updated the modern dashboard to surface both views side-by-side across the active shell and library:
  - main revenue / profit charts now show both profit curves
  - margin charts now separate ex-fixed vs incl-fixed pre/post-ad margins
  - marketing tables and charts now show spend output with both profit variants
  - geo, product, customer, basket and SKU Pareto tables now expose both values explicitly
  - library drilldowns for patterns / products / economics now render both fixed states instead of a single legacy profit alias
- Verified with:
  - `python -m py_compile dashboard_modern.py export_orders.py html_report_generator.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - `python scripts\\reporting_qa_smoke.py`
- Verification outcome:
  - VEVO and ROY March 2026 reports regenerate successfully
  - rendered HTML for both projects now contains explicit `Profit ex fixed` / `Profit incl. fixed` and matching contribution / margin variants across the affected tables and charts

### 2026-04-11 (daily email summaries with current metrics)
- Transferred the April-side daily email summary content into the active runner without losing the newer QA-status intro added later.
- Replaced the old unused long-form executive summary body with the newer sectioned summary structure built from current export data:
  - quick overview
  - what is good
  - what weakened
  - likely cause
  - next-step recommendation
  - data note
- Wired the generated summary back into `build_email_body(...)` so the SES mail now sends:
  - attachment + covered period
  - data-quality / QA status
  - the new metric-driven daily summary
  - closing system note
- Verified with:
  - `python -m py_compile daily_report_runner.py`
  - direct render of `build_report_summary(...)` + `build_email_body(...)` for VEVO March 2026 artifacts
  - direct render of `build_report_summary(...)` + `build_email_body(...)` for ROY March 2026 artifacts
  - `python scripts\\security_ci.py`
- Verification outcome:
  - VEVO and ROY summary text now renders from the actual current artifacts
  - QA warning / partial-data note remains visible at the top of the email

### 2026-04-11 (live dashboard latest-artifact view)
- Transferred the April-side live dashboard latest-artifact view into the active reporting line without overwriting the newer March/April reporting hardening work.
- Extended the shared artifact contract so every export now writes:
  - the range-specific HTML report
  - `report_latest.html`
  - the range-specific dashboard payload JSON extracted from the rendered report
  - `dashboard_payload_latest.json`
- Updated the modern dashboard HTML renderer so the embedded report payload is emitted in a stable JSON script block that can be extracted safely for the live dashboard API.
- Updated the daily runner S3 upload flow so the stable latest artifacts are uploaded alongside the dated outputs for direct live-dashboard consumption.
- Added `live_dashboard_server.py` as a repo-local live viewer that serves:
  - health endpoint
  - project list
  - latest project payload API
  - report iframe route
  - live dashboard route with period switching
- Verified locally with:
  - `python -m py_compile live_dashboard_server.py export_orders.py dashboard_modern.py daily_report_runner.py reporting_core\\contracts.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - localhost smoke checks:
    - `GET /health`
    - `GET /api/vevo/latest?period=full`
    - `GET /api/roy/latest?period=full`
    - `GET /dashboard/vevo?period=full`
    - `GET /dashboard/roy?period=full`
- Verification outcome:
  - live dashboard routes now render against the newest VEVO and ROY artifacts
  - both project dashboards expose the expected `data-marker=\"live-dashboard-app\"`
  - stable latest report and payload artifacts are now produced by the export pipeline instead of depending on ad hoc local files

### 2026-04-11 (ad incrementality analysis)
- Transferred the April-side ad incrementality analysis into the active reporting line without reverting the newer QA, CM and cost-coverage work.
- Replaced the old simplified ads-effectiveness fallback in `export_orders.py` with a richer daily decision model that now:
  - builds a daily ads dataset from aggregated report outputs
  - carries new vs returning split into the ads layer
  - compares ad-active vs baseline days when ad-off days exist
  - falls back to higher-spend vs lower-spend comparisons when the account is always on
  - produces verdict, confidence and incremental spend/revenue/profit/CAC metrics
- Extended the modern dashboard payload and HTML report with:
  - `incrementality_primary`
  - `incrementality_rows`
  - `Ad impact verdict`
  - `Incrementality comparison table`
- Verified locally with:
  - `python -m py_compile export_orders.py dashboard_modern.py live_dashboard_server.py daily_report_runner.py reporting_core\\contracts.py`
  - `python export_orders.py --project vevo --from-date 2026-03-01 --to-date 2026-03-31`
  - `python export_orders.py --project roy --from-date 2026-03-01 --to-date 2026-03-31`
  - `python scripts\\reporting_qa_smoke.py`
  - `python scripts\\security_ci.py`
  - localhost live snapshot check:
    - `GET /health`
    - `GET /api/vevo/latest?period=full`
    - `GET /api/roy/latest?period=full`
- Verification outcome:
  - VEVO March 2026 now exposes two higher-spend-vs-lower-spend incrementality views with primary verdict `Scale`
  - ROY March 2026 now exposes two higher-spend-vs-lower-spend incrementality views with primary verdict `Hold / test more`
  - live API snapshots now carry the incrementality payload used by the read-only live dashboard

### 2026-04-11 (full-history regeneration + refill cohort hardening)
- Regenerated the active full-history VEVO and ROY reports on `codex/segment-unit-econ-lifecycle` using the current production-candidate reporting code:
  - VEVO `2025-05-03 .. 2026-04-10`
  - ROY `2025-09-24 .. 2026-04-10`
- Hardened `analyze_refill_cohorts(...)` in `export_orders.py` so VEVO full-history exports no longer crash when a cohort slice has no second-order match rows.
- The refill cohort merge now backfills the missing second-order columns with null/false defaults before downstream timing and window calculations.
- Fresh full-history artifacts now exist locally for both projects:
  - `data/vevo/report_20250503-20260410.html`
  - `data/roy/report_20250924-20260410.html`
  - matching `report_latest.html`, `dashboard_payload_*.json`, `data_quality_*.json`, and CSV exports for both projects
- Verified locally with:
  - `python -m py_compile export_orders.py`
  - `python export_orders.py --project vevo --from-date 2025-05-03 --to-date 2026-04-10`
  - `python export_orders.py --project roy --from-date 2025-09-24 --to-date 2026-04-10`
- Verification outcome:
  - VEVO full-history export now completes successfully despite empty second-order cohort slices
  - ROY full-history export still completes successfully on the same branch

### 2026-04-11 (Python 3.11 CI syntax compatibility)
- Fixed a GitHub Actions merge blocker in `dashboard_modern.py` that only surfaced on CI Python 3.11.
- Root cause:
  - the large dashboard HTML f-string still contained three inline fallback expressions with escaped quotes,
  - Python 3.12 accepted the file locally, but CI Python 3.11 rejected it with `SyntaxError: f-string expression part cannot include a backslash`.
- Moved the affected table-body builders into precomputed HTML variables:
  - campaign attribution estimate table
  - same-item purchase frequency table
  - cohort payback table
- Verified locally with:
  - `python -m py_compile dashboard_modern.py export_orders.py daily_report_runner.py html_report_generator.py live_dashboard_server.py`
  - `python scripts/reporting_qa_smoke.py`
- Verification outcome:
  - local syntax checks pass again
  - the PR branch is now ready for GitHub CI to re-run on a Python-3.11-compatible dashboard renderer

### 2026-04-11 (production activation on main)
- Merged PR `#31 Finalize April reporting transfers` into `main`.
- Verified the merge landed as commit `e13b329` on `origin/main`.
- Verified the production image refresh completed successfully via GitHub Actions:
  - workflow: `Build and Push ECR`
  - run: `24286215777`
  - result: `success`
- Production hard-gate confirmation for the active daily runner:
  - instance-id: `N/A` (scheduled ECS task, no fixed EC2 host)
  - IP: `N/A` (no fixed host / no localhost endpoint)
  - service name: `vevo-daily-report-email` scheduler targeting ECS family `vevo-reporting-daily:4`
  - image path: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
- Operational note:
  - this repo currently documents only the VEVO AWS daily email runner as production,
  - ROY reporting is now in `main` too, but a separate ROY scheduled AWS runner is not yet documented in this repo.

### 2026-04-11 (VEVO exclude `madfrog stara odoslana`)
- Added VEVO project-level excluded order status:
  - `madfrog stara odoslana`
- Fixed a reporting bug in `fetch_orders(...)`:
  - cached historical day buckets were previously extended straight into the export result without re-applying `_filter_by_status(...)`,
  - this allowed newly excluded statuses to survive inside older cached VEVO history even after the config changed.
- Runtime impact:
  - VEVO now excludes `madfrog stara odoslana` both for freshly fetched days and for older cached days.
- Verified locally with:
  - `python export_orders.py --project vevo --from-date 2025-05-03 --to-date 2025-05-05`
  - `python export_orders.py --project vevo --from-date 2025-05-03 --to-date 2026-04-10`
  - `python scripts/reporting_qa_smoke.py`
  - `python -m py_compile export_orders.py daily_report_runner.py dashboard_modern.py html_report_generator.py`
- Verification outcome:
  - VEVO control window `2025-05-03 .. 2025-05-05` dropped from 5 orders to 2 orders after the exclusion was applied through cache as well
  - `data/vevo/export_20250503-20250505.csv` no longer contains `madfrog stara odoslana`
  - `data/vevo/export_20250503-20260410.csv` no longer contains `madfrog stara odoslana`
  - refreshed full-history VEVO artifacts were regenerated on the current code path

### 2026-04-11 (ROY product demand analytics)
- Added Roy-only product-demand analytics into `export_orders.py` and the active modern dashboard:
  - growing products based on recent 4-week revenue vs previous 4 weeks
  - declining products on the same comparison window
  - product seasonality based on full historical months in the selected report range
  - next-30-day product sales forecast from weekly historical revenue and units
  - top brands by revenue
  - top brands by profit
- Added project-configured Roy brand groups in `projects/roy/settings.json` to avoid noisy pseudo-brand labels.
- Added brand display guardrails so tiny one-order / low-revenue brands do not dominate the profit ranking tables.
- Verified locally with:
  - `python -m py_compile export_orders.py dashboard_modern.py html_report_generator.py`
  - `python export_orders.py --project roy --from-date 2025-09-24 --to-date 2026-04-10`
  - `python scripts/reporting_qa_smoke.py`
- Verification outcome:
  - full-history Roy report regenerates successfully
  - dashboard payload now includes `roy_product_demand.summary`, trend rows, seasonality rows, forecast rows, and brand ranking rows
  - full-history Roy payload currently reports:
    - `23` growing products
    - `15` declining products
    - `26` forecasted products
    - `14` displayed brands after guardrails

### 2026-04-11 (ROY product demand analytics on main)
- Merged PR `#34 Add Roy product demand analytics` into `main`.
- Verified the merge landed as commit `26fbefc` on `origin/main`.
- Verified the production image refresh completed successfully via GitHub Actions:
  - workflow: `Build and Push ECR`
  - run: `24288614393`
  - result: `success`
- Operational note:
  - the new Roy analytics blocks are now part of the production image used by the reporting runtime
  - this repo still does not document a separate AWS scheduled daily runner for ROY, so scheduling remains a separate product decision

### 2026-04-12 (ROY scheduled daily runner verification + alignment)
- Verified that ROY already had an active AWS scheduler and runtime secret:
  - scheduler: `roy-daily-report-email`
  - ECS cluster: `vevo-reporting-cluster`
  - runtime secret: `roy/reporting/runtime-env`
  - log group: `/ecs/roy-reporting-daily`
- Found a production drift:
  - ROY scheduler was still targeting `roy-reporting-daily:1`
  - task definition revision `1` still used `REPORT_FROM_DATE=2025-09-22`
  - current project source-of-truth in `projects/roy/settings.json` uses `2025-09-24`
- Registered new ECS task definition revision `roy-reporting-daily:2` with:
  - `REPORT_FROM_DATE=2025-09-24`
  - unchanged cluster/network/security/image/secret wiring
- Ran a manual production-equivalent ECS task on `roy-reporting-daily:2` and verified in CloudWatch:
  - task exited with `exitCode=0`
  - private runtime IP during task execution: `172.31.15.32`
  - `HTML report saved: data/roy/report_20250924-20260411.html`
  - `Latest HTML report saved: data/roy/report_latest.html`
  - `SES message sent`
- Updated scheduler `roy-daily-report-email` to target:
  - `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:2`
- Operational note:
  - ROY now has a real scheduled AWS daily email runner and it is aligned with the current reporting start date
  - the current runtime secret still sends ROY report emails to `mil.terem@gmail.com`

### 2026-04-14 (production regression gate + host verification hardening)
- Verified the KPI regression fixes were merged to `main` as PR `#39` and the production image refresh completed:
  - merge commit on `main`: `28a7c2692b0baa50755b77384e7f0514ac1476b3`
  - ECR `latest` digest after merge: `sha256:d4188b3febd622e7c308dc08e4aa8a79ca214af4addae86820a2ad689cafb47f`
- Ran a manual VEVO production-equivalent ECS task on the new image and verified host-level artifact rendering with a localhost marker in CloudWatch:
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/2dbe88c1a6d948e78fded177a3fe108f`
  - private runtime IP: `172.31.4.112`
  - marker: `LOCALHOST_MARKER_OK`
  - verified serialized consistency payload fields (`roas_delta`, `margin_delta`, `cac_delta`) were present in the generated dashboard artifact
- Found a production-verification gap during the equivalent ROY host check:
  - the runtime image did not include `curl`, so a strict `curl localhost` host verification wrapper exited `127` before the report marker step
  - this was a verification-tooling gap, not evidence of the KPI regression returning
- Hardened production deployment to make this class of issue much harder to reintroduce:
  - added `curl` into the Docker runtime image so future host checks can use the required localhost verification path directly
  - added `python scripts/reporting_qa_smoke.py` as a hard gate inside `.github/workflows/build-and-push-ecr.yml` before any ECR login/build/push
  - broadened build trigger paths to include `projects/**`, `templates/**`, and `scripts/reporting_qa_smoke.py` so runtime-shaping changes cannot silently miss a production image refresh
- Verification to run after merge of this hardening branch:
  - GitHub Actions `Build and Push ECR` must pass with the new smoke gate
  - rerun manual ROY ECS verification with `curl localhost` marker on the refreshed image
- Next exact step:
  - merge the hardening branch, wait for the guarded ECR build to finish, then rerun the ROY manual host-level verification and confirm the marker payload in CloudWatch

### 2026-04-14 (production regression gate completed)
- Merged PR `#40 Harden production reporting deployment gate` into `main`:
  - merge commit on `main`: `0d8fc675d26cba54dc67b772cb6815d89d6beac6`
  - PR URL: `https://github.com/vzeman/biznisweb/pull/40`
- Verified the guarded production image refresh completed successfully:
  - workflow: `Build and Push ECR`
  - run: `24381166896`
  - result: `success`
  - confirmed smoke gate step `Reporting regression smoke gate` passed before image push
  - refreshed ECR `latest` digest: `sha256:75124d167ae8cdaa24b13f558b7229e8440fa0ad979530eb705432b74ed38ae8`
- Ran a manual ROY production-equivalent ECS task on the refreshed image and verified host-level localhost rendering with `curl`:
  - task definition: `roy-reporting-daily:2`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/03b85008c74249989259296410da2bcc`
  - private runtime IP: `172.31.46.224`
  - image digest in task: `sha256:75124d167ae8cdaa24b13f558b7229e8440fa0ad979530eb705432b74ed38ae8`
  - task exit code: `0`
  - marker: `LOCALHOST_MARKER_OK`
  - generated HTML confirmed the geo QA warning text was present (`geo_warning_present=true`)
- Operational conclusion:
  - the KPI regression fix is now in production
  - production image publication is now gated by the reporting smoke test, so a future broken `main` change should fail before it can overwrite ECR `latest`
- Known follow-up worth auditing separately:
  - the ROY `prodcheck3` marker payload returned `consistency.* = null`; this did not block the deployed KPI fix or localhost HTML verification, but if ROY is expected to show those consistency deltas in UI, it should get a dedicated follow-up audit
- Next exact step:
  - optionally audit why ROY `consistency` fields are null in the prodcheck artifact and decide whether that is expected dataset behavior or another dashboard binding gap

### 2026-04-14 (ROY fixed cost source-of-truth raised to 5500 EUR/month)
- Updated ROY project settings source-of-truth in `projects/roy/settings.json`:
  - `fixed_monthly_cost`: `4900` -> `5500`
- Removed runner-level fixed-cost drift in `daily_report_runner.py`:
  - `_window_aggregate()` no longer subtracts a stale global fallback from `net_profit`
  - runner company-profit-with-fixed now follows the exported `net_profit` rows, which already include fixed overhead
- Added regression guards in `scripts/reporting_qa_smoke.py`:
  - assert ROY runtime loads `fixed_monthly_cost = 5500`
  - assert daily runner does not double-subtract fixed overhead from aggregate rows
- Expected runtime effect after production image refresh:
  - future ROY daily runs will load `5500 EUR/month` from Git-backed project settings
  - April daily fixed allocation becomes `183.33 EUR/day` before CSV rounding
- Next exact step:
  - merge the branch, wait for the guarded ECR build to finish, then verify on a manual ROY ECS task that localhost marker output reports `fixed_monthly_cost = 5500`

### 2026-04-14 (ROY fixed cost 5500 EUR/month live in production)
- Merged PR `#42 Raise ROY fixed cost to 5500 EUR` into `main`:
  - merge commit on `main`: `981f17f444074fb02b5551512ac8026067d3d6f6`
  - PR URL: `https://github.com/vzeman/biznisweb/pull/42`
- Verified the guarded production image refresh completed successfully:
  - workflow: `Build and Push ECR`
  - run: `24381555697`
  - result: `success`
  - confirmed smoke gate step `Reporting regression smoke gate` passed before image push
- Ran a manual ROY production-equivalent ECS task on the refreshed image and verified runtime config directly on host via `curl localhost` marker:
  - schedule/service name: `roy-daily-report-email`
  - task definition: `roy-reporting-daily:2`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/cb2ad219d0ff45bd8aeabd7b365cd4d5`
  - private runtime IP: `172.31.34.177`
  - image digest in task: `sha256:466427162365093665b5727c5e3d8b765bf40937b378b2248caa269579ccd7dd`
  - task exit code: `0`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - marker: `LOCALHOST_MARKER_OK`
  - marker payload:
    - `fixed_monthly_cost = 5500.0`
    - `fixed_daily_cost_override = 0.0`
    - `daily_fixed_cost_for_2026_04_15 = 183.33333333333334`
- Operational conclusion:
  - tomorrow's ROY scheduled run will start using `5500 EUR/month`
  - because the value lives in Git-backed project settings on `main` and the production image build is now gated plus triggered by `projects/**`, it should not silently flip back to `4900` without another explicit code/config change
- Next exact step:
  - no immediate action required unless ROY fixed costs change again in source-of-truth

### 2026-05-21 (VEVO/ROY daily profit-loss history UI)
- Branch: `codex/daily-profit-loss-history`
- PR: `https://github.com/vzeman/biznisweb/pull/64` (merged to `main` as `dad3f913e5bbe8789f2a214d19d822929f1e292e`)
- Added a shared daily profit/loss history block to the modern reporting dashboard:
  - daily final profit after fixed overhead is classified per day as plus/minus/break-even
  - plus days render green and minus days render red in the summary cards, chart, and full daily ledger
  - the block is in the shared renderer, so it applies to both VEVO and ROY reporting
- Added smoke coverage in `scripts/reporting_qa_smoke.py` for both `Vevo reporting` and `Roy reporting`.
- Verified locally with:
  - `python -m py_compile dashboard_modern.py scripts\reporting_qa_smoke.py`
  - `python scripts\reporting_qa_smoke.py`
- GitHub PR checks passed:
  - `env-check`
  - `secret-scan`
  - `security-baseline`
  - `observability-baseline`
- Verification note:
  - smoke passed; local environment still logs the existing Google Ads credentials warning when credentials are not configured
- Next exact step:
  - completed by the production verification entry below

### 2026-05-21 (VEVO/ROY daily profit-loss production verified)
- Merged daily profit-loss reporting UI into `main`:
  - PR: `https://github.com/vzeman/biznisweb/pull/64`
  - merge commit: `dad3f913e5bbe8789f2a214d19d822929f1e292e`
- Guarded production image refresh completed:
  - workflow: `Build and Push ECR`
  - run: `26210072736`
  - result: `success`
  - image: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - digest: `sha256:57ae5b73c83bcb83c8a58bd7c1395ce69e328e8631d42ca75c009650f7c6a1ce`
  - reporting smoke gate and invoice automation regression test passed before image push
- Calculation-safety verification:
  - `python -m py_compile dashboard_modern.py scripts\reporting_qa_smoke.py`
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_reporting_calculation_fixes tests.test_invoice_generation`
  - deterministic dashboard payload matched `origin/main` exactly after removing only the newly added `daily_profit_loss` key
  - conclusion: no existing reporting math/calculation output changed; the added data is a view over the existing daily final profit series
- Production-equivalent host smoke completed from `main`:
  - workflow: `Production Reporting Smoke`
  - run: `26211921297`
  - job: `77124653366`
  - result: `success`
  - workflow commit: `a5673fa1c2fb582456ef53261142400d237b2fc4`
- VEVO hard-gate verification:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.38.243`
  - service name: `vevo-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:5`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/3b96f7cddc0e47f98db7af47f7ac2a8b`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - UI path: `http://127.0.0.1:8787/dashboard/vevo`
  - marker: `LOCALHOST_MARKER_OK`
  - daily profit rows: `383`; plus days: `250`; minus days: `133`
  - UI smoke: `UI_SMOKE_OK:vevo:daily-profit-loss`
- ROY hard-gate verification:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.24.102`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:3`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/b80dc33704044c0eaa4cc1d6b29587d9`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - UI path: `http://127.0.0.1:8787/dashboard/roy`
  - marker: `LOCALHOST_MARKER_OK`
  - daily profit rows: `239`; plus days: `155`; minus days: `84`
  - UI smoke: `UI_SMOKE_OK:roy:daily-profit-loss`
- Operational conclusion:
  - the new daily plus/minus historical section is live in the production image used by both report schedules
  - VEVO scheduled report on `2026-05-22 01:00 Europe/Bratislava` should include it
  - ROY scheduled report on `2026-05-22 01:30 Europe/Bratislava` should include it
- Next exact step:
  - after the `2026-05-22` scheduled emails run, check the scheduled run logs once and confirm they used image digest `sha256:57ae5b73c83bcb83c8a58bd7c1395ce69e328e8631d42ca75c009650f7c6a1ce`

### 2026-05-21 (VEVO production board local implementation)
- Branch: `codex/vevo-production-board`
- PR: `https://github.com/vzeman/biznisweb/pull/70` (merged to `main` as `30c6093ba0666676aa4982edac9f5022b7047df1`)
- Added a VEVO-only production board for active unshipped orders:
  - live page: `/production/vevo`
  - JSON endpoint: `/api/production/vevo/live`
  - included statuses: `Čaká na vybavenie`, `Platba online - zaplatené`
  - manufactured items are limited to product labels matching configured VEVO terms
  - configured exception: `Vevo Ylang Absolute prací gél 1L`
  - future outsourced VEVO products can be added to `production_board.excluded_product_labels`
- Added scan controls because BiznisWeb rejects status filtering for this API token with HTTP 412:
  - client-side status filtering over newest orders by purchase date
  - minimum scan: `10` pages / `300` newest orders
  - max scan: `30` pages
  - stop after repeated pages without active orders
- Updated the ECR build gate so future production-board/live-server edits rebuild the production image:
  - added `live_dashboard_server.py` and `production_board.py` to `.github/workflows/build-and-push-ecr.yml` path triggers
  - added `tests.test_production_board` to the image publication regression test step
- Data privacy note:
  - production board API/UI does not return customer names; it returns order numbers, dates, statuses, sums, product labels, quantities, and ignored-item reasons
- Verified locally:
  - `python -m py_compile production_board.py live_dashboard_server.py`
  - `python -m unittest tests.test_production_board tests.test_invoice_generation tests.test_reporting_calculation_fixes`
  - `git diff --check`
  - local server `http://127.0.0.1:8788/production/vevo`
  - HTML marker: `vevo-production-board`
  - live API summary: `18` active orders, `18` manufacturing products, `36.0` units to make, `8.0` ignored units
  - live scan: `300` orders / `10` pages, oldest scanned order `2026-05-06 21:38:00`, oldest active order `2026-05-12 12:34:27`, `limit_reached=false`
  - verified API response contains `0` customer-name fields
- Not deployed yet:
  - ECR `latest` refreshed after merge by `Build and Push ECR` run `26219915597`
  - refreshed digest: `sha256:db4f16d55b38b317f43ac58760d760d56b1255ad015c42cd3ee65e7177abbd3b`
  - production smoke still needs the new production-board host route check before closing deployment verification
- Next exact step:
  - merge the production smoke workflow enhancement, then run `Production Reporting Smoke` for `vevo` with a production-board marker and verify localhost marker + `/production/vevo`

### 2026-05-21 (VEVO production board smoke workflow enhancement)
- Branch: `codex/production-board-smoke`
- PR: `https://github.com/vzeman/biznisweb/pull/71` (merged to `main` as `ff0e97cb394a1a47b4dfce3238062906a3793480`)
- Added host-level production board checks to `.github/workflows/production-reporting-smoke.yml`:
  - when `production_board.enabled` is true, the ECS/Fargate smoke task curls `http://127.0.0.1:8787/production/<project>`
  - verifies the `vevo-production-board` HTML marker
  - curls `http://127.0.0.1:8787/api/production/<project>/live?refresh=1`
  - verifies active statuses, configured Ylang gel exclusion, structured summary/products/orders, and no customer-name fields
  - writes `PRODUCTION_BOARD_OK` into the localhost marker payload served from `http://127.0.0.1:8000/marker.json`
  - prints `UI_SMOKE_OK:<project>:production-board`
- Verified locally:
  - YAML parse of `.github/workflows/production-reporting-smoke.yml`
  - `git diff --check`
- Next exact step:
  - completed by the production verification entry below

### 2026-05-21 (VEVO production board production verified)
- Merged VEVO production board into `main`:
  - PR: `https://github.com/vzeman/biznisweb/pull/70`
  - merge commit: `30c6093ba0666676aa4982edac9f5022b7047df1`
- Merged production-board host smoke enhancement into `main`:
  - PR: `https://github.com/vzeman/biznisweb/pull/71`
  - merge commit: `ff0e97cb394a1a47b4dfce3238062906a3793480`
- Guarded production image refresh completed:
  - workflow: `Build and Push ECR`
  - run: `26219915597`
  - result: `success`
  - image: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - digest: `sha256:db4f16d55b38b317f43ac58760d760d56b1255ad015c42cd3ee65e7177abbd3b`
  - reporting smoke gate, invoice tests, reporting calculation tests, and production-board tests passed before image push
- VEVO hard-gate verification:
  - workflow: `Production Reporting Smoke`
  - run: `26220199241`
  - job: `77152916053`
  - result: `success`
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.18.178`
  - service name: `vevo-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:5`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/e686ab804e0647dd92183e03e45e9bec`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - dashboard UI path: `http://127.0.0.1:8787/dashboard/vevo`
  - production board UI path: `http://127.0.0.1:8787/production/vevo`
  - CloudWatch log stream: `/ecs/vevo-reporting-daily:ecs/reporting/e686ab804e0647dd92183e03e45e9bec`
- Production-board host assertions executed inside the ECS task:
  - `curl -fsS http://127.0.0.1:8787/production/vevo`
  - verified HTML marker `vevo-production-board`
  - `curl -fsS http://127.0.0.1:8787/api/production/vevo/live?refresh=1`
  - verified active statuses `Čaká na vybavenie` and `Platba online - zaplatené`
  - verified excluded product `Vevo Ylang Absolute prací gél 1L`
  - verified structured `summary`, `products`, and `orders` payloads
  - verified no `customer` fields are returned in order payloads
  - wrote `PRODUCTION_BOARD_OK` into the localhost marker payload
  - emitted `UI_SMOKE_OK:vevo:production-board`
- Operational conclusion:
  - the VEVO production board is included in the current production image used by the VEVO scheduled runtime
  - the tool is available in the live dashboard server at `/production/vevo`
  - future production-board/live-server changes now trigger ECR rebuilds and production smoke can verify the production-board route
- Next exact step:
  - expose/use the hosted live dashboard entrypoint for production users; code/runtime verification is complete

### 2026-05-21 (VEVO production board App Runner exposure in progress)
- Branch: `codex/live-dashboard-apprunner`
- PR: `https://github.com/vzeman/biznisweb/pull/73` (merged to `main` as `8e2f10c93cffb722f2c852ffa2019598bbdcdaa4`)
- Goal:
  - expose the already-deployed VEVO production board as a persistent AWS-hosted HTTPS service for users outside this PC
- Chosen hosting target:
  - AWS App Runner from the existing private ECR image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - service name: `biznisweb-vevo-production-board`
  - health path: `/health`
  - user path: `/production/vevo`
- Rationale:
  - App Runner gives a stable public HTTPS service URL without maintaining a fixed EC2 host or ALB for this small internal tool
  - the existing ECS/Fargate reporting image is reused, so Git/ECR remain the deployment source of truth
- Security change:
  - added optional Basic Auth to `live_dashboard_server.py`
  - `/health` remains unauthenticated for managed health checks
  - all other live dashboard routes require auth when `LIVE_DASHBOARD_AUTH_USER` and `LIVE_DASHBOARD_AUTH_PASSWORD` are configured
  - App Runner deploy stores `LIVE_DASHBOARD_AUTH_PASSWORD` in SSM Parameter Store at `/biznisweb/live-dashboard/basic-auth-password`
- Deployment automation:
  - added `.github/workflows/deploy-live-dashboard-apprunner.yml`
  - workflow creates/updates App Runner ECR access and runtime instance roles
  - workflow reuses the existing `BIZNISWEB_API_TOKEN` secret reference from `vevo-reporting-daily`
  - workflow creates or updates the App Runner service and verifies:
    - public `/health`
    - authenticated `/production/vevo`
    - authenticated `/api/production/vevo/live?refresh=1`
    - `vevo-production-board` marker
    - configured active statuses and Ylang gel exclusion
    - no customer fields in the payload
- Verified locally:
  - `python -m py_compile live_dashboard_server.py`
  - `python -m unittest tests.test_live_dashboard_auth tests.test_production_board`
  - YAML parse for all `.github/workflows/*.yml`
  - `git diff --check`
- Next exact step:
  - completed by the App Runner deployment verification entry below

### 2026-05-21 (VEVO production board App Runner deployed)
- Merged App Runner deployment support into `main`:
  - PR: `https://github.com/vzeman/biznisweb/pull/73`
  - merge commit: `8e2f10c93cffb722f2c852ffa2019598bbdcdaa4`
- Guarded production image refresh completed after merge:
  - workflow: `Build and Push ECR`
  - run: `26222226926`
  - result: `success`
  - image: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - digest: `sha256:88d1873db4ee893d0f9bc7ea65f66b444f95b8cbe4870ccab8f7579165b2d60c`
  - regression test step ran `23` tests including `tests.test_live_dashboard_auth`
- App Runner deployment completed:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26222324018`
  - result: `success`
  - instance-id: `N/A (AWS App Runner managed service)`
  - private IP: `N/A (AWS App Runner managed service)`
  - service name: `biznisweb-vevo-production-board`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-vevo-production-board/8c8a7a5d694b401baeccf0f1af19ca50`
  - public URL: `https://zxtma5mxta.eu-central-1.awsapprunner.com`
  - health path: `https://zxtma5mxta.eu-central-1.awsapprunner.com/health`
  - production board path: `https://zxtma5mxta.eu-central-1.awsapprunner.com/production/vevo`
  - Basic Auth username: `vevo`
  - Basic Auth password source: SSM SecureString `/biznisweb/live-dashboard/basic-auth-password`
- App Runner smoke verification:
  - public `/health` returned successfully
  - authenticated `/production/vevo` returned the `vevo-production-board` HTML marker
  - authenticated `/api/production/vevo/live?refresh=1` returned:
    - `active_orders=23`
    - `manufacturing_products=21`
    - `units_to_make=44.0`
    - `orders_scanned=300`
  - verified configured active statuses and `Vevo Ylang Absolute prací gél 1L` exclusion
  - verified no `customer` fields in the production-board API payload
- Operational conclusion:
  - users outside this PC can now access the VEVO production board through the App Runner HTTPS URL with Basic Auth
  - next recommended exposure step is attaching a memorable domain such as `vyroba.vevo.sk` or `production.vevo.sk` to the App Runner service
- Next exact step:
  - choose the public hostname and DNS owner, then attach it as an App Runner custom domain and distribute the Basic Auth credentials through a secure channel

### 2026-05-21 (VEVO production board App Runner auth rotation in progress)
- Branch: `codex/set-apprunner-auth-credentials`
- PR: `https://github.com/vzeman/biznisweb/pull/75` (merged to `main` as `f95e496c9cd59916012fd13b3ae4f825a0eb3848`)
- Requested credential change:
  - Basic Auth username: `marek`
  - Basic Auth password: managed via GitHub Actions secret `LIVE_DASHBOARD_AUTH_PASSWORD`
- Deployment workflow change:
  - `.github/workflows/deploy-live-dashboard-apprunner.yml` now overwrites SSM SecureString `/biznisweb/live-dashboard/basic-auth-password` from `secrets.LIVE_DASHBOARD_AUTH_PASSWORD` when the secret is configured
  - if the secret is not configured, the workflow keeps the previous behavior and only generates a random password when the SSM parameter does not exist
- Local setup:
  - updated GitHub Actions secret `LIVE_DASHBOARD_AUTH_PASSWORD` for repo `vzeman/biznisweb`
- App Runner redeploy completed:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26224999626`
  - result: `success`
  - service name: `biznisweb-vevo-production-board`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-vevo-production-board/8c8a7a5d694b401baeccf0f1af19ca50`
  - image digest: `sha256:88d1873db4ee893d0f9bc7ea65f66b444f95b8cbe4870ccab8f7579165b2d60c`
  - health path: `https://zxtma5mxta.eu-central-1.awsapprunner.com/health`
  - production board path: `https://zxtma5mxta.eu-central-1.awsapprunner.com/production/vevo`
- Verification:
  - unauthenticated `/production/vevo` returned HTTP `401`
  - authenticated request with `marek` and the configured password returned HTTP `200`
  - deploy smoke returned `APP_RUNNER_PRODUCTION_BOARD_OK:active_orders=24:manufacturing_products=21:units_to_make=45.0:orders_scanned=300`
- Operational conclusion:
  - active Basic Auth credentials are now username `marek` and the password stored in `LIVE_DASHBOARD_AUTH_PASSWORD` / SSM
- Next exact step:
  - share `https://zxtma5mxta.eu-central-1.awsapprunner.com/production/vevo` with username `marek`; optionally attach custom domain later

### 2026-05-21 (VEVO production board mobile layout deployed)
- Branch: `codex/mobile-production-board`
- PR: `https://github.com/vzeman/biznisweb/pull/77` (merged to `main` as `ddaaf353d7f778dda91af1788703dd91c9c97165`)
- Change:
  - production-board calculations and API payload are unchanged
  - `/production/vevo` now renders mobile product cards below `680px` instead of forcing the desktop production table onto phone screens
  - the desktop production table remains available on wider screens
- Local verification:
  - `python -m py_compile live_dashboard_server.py production_board.py`
  - `python -m unittest tests.test_live_dashboard_auth tests.test_production_board tests.test_live_dashboard_mobile`
  - `git diff --check`
  - local server on `127.0.0.1:8788` returned the `vevo-production-board` marker and `productsCards` mobile layout marker
  - local API returned `active_orders=26`, `manufacturing_products=21`, `units_to_make=48.0`, `orders_scanned=300`
- Next exact step:
  - completed by the App Runner digest deployment verification entry below

### 2026-05-21 (App Runner digest deploy fix deployed)
- Branch: `codex/apprunner-digest-deploy`
- PR: `https://github.com/vzeman/biznisweb/pull/78` (merged to `main` as `0fdca381e3040172e15ff3b17c1d569a97821c68`)
- Issue found:
  - App Runner deploy workflow could succeed while keeping the same `latest` image identifier string, so the public service could continue serving the previous runtime image
- Change:
  - deploy workflow now resolves the current ECR `latest` digest and passes the digest-specific ECR image identifier to App Runner
  - deploy smoke now checks the mobile production-board HTML markers `productsCards` and `@media (max-width:680px)`
  - ECR build regression test list now includes `tests.test_live_dashboard_mobile`
- Build/deploy:
  - ECR build run `26228096475` succeeded with digest `sha256:58df20cab335f7376331103676737c04acc17d23a1c43a5aa8c2aad719257bb1`
  - App Runner deploy run `26228215320` succeeded for service `biznisweb-vevo-production-board`
  - instance-id: `N/A (AWS App Runner managed service)`
  - private IP: `N/A (AWS App Runner managed service)`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-vevo-production-board/8c8a7a5d694b401baeccf0f1af19ca50`
  - production board path: `https://zxtma5mxta.eu-central-1.awsapprunner.com/production/vevo`
- Verification:
  - unauthenticated `/production/vevo` returned HTTP `401`
  - authenticated HTML returned `vevo-production-board`, `productsCards`, `table-wrap desktop-products`, and `@media (max-width:680px)` markers
  - authenticated API returned `active_orders=26`, `manufacturing_products=21`, `units_to_make=48.0`, `orders_scanned=300`
- Next exact step:
  - test the public URL on an actual phone; if a branded URL is wanted, attach a custom App Runner domain such as `vyroba.vevo.sk`

### 2026-05-26 (ROY inventory tab 100 visible products)
- Branch: `codex/roy-inventory-100`
- Change:
  - `/production/roy` inventory tab now renders the first `100` stock products in the browser instead of the previous `80`
  - added a dashboard HTML regression assertion for the `visibleInventoryLimit = 100` marker
- Local verification:
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_live_dashboard_auth tests.test_live_dashboard_mobile`
- Next exact step:
  - open PR, merge to `main`, deploy the live dashboard App Runner service, then verify the public ROY URL shows at least 100 inventory rows when enough rows exist

### 2026-05-26 (ROY restock alerts include relevant historical sellers)
- Branch: `codex/roy-restock-relevant-products`
- Change:
  - ROY inventory model now adds historically relevant sold SKUs into restock analysis even when they are missing from the current BiznisWeb inventory snapshot
  - historical restock relevance threshold is configurable and currently requires at least `3` orders, `3` units, and `50` EUR net revenue
  - products without a brand/family lead-time override now use default `5` working days for reorder timing
  - alert/restock/revenue-at-risk rows now require the historical relevance threshold, so one-off or two-off products are filtered out
  - ECR build workflow now runs `tests.test_roy_inventory_model`
- Local verification:
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model`
  - `git diff --check`
- Next exact step:
  - open/merge PR, rebuild ECR, deploy ROY App Runner, verify deploy smoke and public `/production/roy`

### 2026-05-26 (ROY live dashboard inventory row payload limits)
- Branch: `codex/roy-dashboard-row-limits`
- PR: `https://github.com/vzeman/biznisweb/pull/87` (merged to `main` as `6a2175701d42d5ae6ddcb1a25df405c9f597d29a`)
- Change:
  - modern ROY dashboard payload now serializes up to `160` inventory rows, `120` stock alert rows, and `120` restock priority rows
  - live ROY dashboard UI now renders up to `100` stock alert rows and `100` inventory rows when enough rows exist
  - App Runner API adapter now keeps up to `120` stock alert rows from the reporting payload
- Local verification:
  - `python -m py_compile dashboard_modern.py live_dashboard_server.py roy_operations_dashboard.py export_orders.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_live_dashboard_auth tests.test_live_dashboard_mobile`
  - `git diff --check`
- Build/deploy:
  - ECR build run `26461410451` succeeded
  - App Runner deploy run `26461516011` succeeded for service `biznisweb-roy-operations-dashboard`
  - instance-id: `N/A (AWS App Runner managed service)`
  - private IP: `N/A (AWS App Runner managed service)`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
- Public verification:
  - authenticated HTML returned `roy-operations-dashboard`, `Executive KPI deck`, `visibleInventoryAlertLimit = 100`, and `visibleInventoryLimit = 100`
  - authenticated API returned `marker=roy-operations-dashboard`, `fulfillable_orders=58`, `personal_pickups=1`, `auto_refresh_seconds=90`, `kpi_months=9`
  - inventory API returned `inventory_products_with_stock=610`, `inventory_rows=160`, `inventory_alerts=23`, `alert_rows=23`, `history_only_inventory_products=15`, `historical_restock_relevant_products=79`, `default_lead_time_working_days=5`, `restock_rows=26`
  - browser UI smoke verified overview `Skladové upozornenia` renders `23` alert rows and the `Sklad` tab renders `100` inventory rows with meta `610 produktov so skladom`
- Next exact step:
  - review real alert quality in `/production/roy`; adjust `historical_restock_min_revenue` upward if low-value accessories create too much noise

### 2026-05-26 (ROY product identity uses BiznisWeb import code)
- Branch: `codex/roy-import-code-product-identity`
- PR: `https://github.com/vzeman/biznisweb/pull/89` (merged to `main` as `f78bb07824e1f50442f0679a39ffb600ea7b1f8a`)
- Change:
  - ROY reporting now prefers BiznisWeb `import_code` as the canonical `product_sku` before EAN/title-hash fallback
  - same import code now groups translated product names together, e.g. HU/CZ/SK Micro SD variants become one product when the import code matches
  - inventory snapshot grouping now uses the same import-code-first product identity as historical demand
  - product expense lookup keeps compatibility with warehouse number, import code, EAN, current SKU, and legacy title-hash keys so existing ROY cost mappings still resolve
  - added regression tests for import-code identity and legacy cost fallback
- Local verification:
  - `python -m py_compile export_orders.py live_dashboard_server.py roy_operations_dashboard.py dashboard_modern.py`
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model`
  - `git diff --check`
- Build/deploy:
  - ECR build run `26463672888` succeeded
  - App Runner deploy run `26463770481` succeeded for service `biznisweb-roy-operations-dashboard`
  - instance-id: `N/A (AWS App Runner managed service)`
  - private IP: `N/A (AWS App Runner managed service)`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
- Public verification:
  - authenticated API returned `marker=roy-operations-dashboard`, `inventory_rows=160`, `alert_rows=18`, `restock_rows=21`, `inventory_products_with_stock=610`
  - SKU identity check returned first inventory SKUs `WD0021,11001,11005,R99003,21003,R99002,12474,F_1472,21002,14832`
  - confirmed SKU `12474` is present and maps to `Fotopasca Wachman Solar Pro`
  - browser UI smoke verified overview `Skladové upozornenia` renders `18` alert rows and `Sklad` tab renders `100` rows with first visible SKU `WD0021`
- Next exact step:
  - review live restock grouping for Micro SD language variants and tune any remaining aliases only where BiznisWeb import code is missing

### 2026-05-26 (VEVO and ROY reporting product identity uses import code)
- Branch: `codex/reporting-import-code-product-identity`
- Change:
  - enabled `product_identity.prefer_import_code` for VEVO as well as ROY
  - reporting aggregations already run through `add_reporting_product_identity_columns`, so `date_product_agg`, `items_agg`, product margins/trends, ROY demand analytics, and dashboard payloads now use import-code-first identity for both projects
  - added a shared regression test proving VEVO and ROY reporting collapse translated names with the same import code into one `items_agg` product row
  - ECR build workflow now runs `tests.test_reporting_product_identity`
- Local verification:
  - `python -m py_compile export_orders.py dashboard_modern.py live_dashboard_server.py roy_operations_dashboard.py`
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `git diff --check`
- Next exact step:
  - completed by the production verification entry below

### 2026-05-26 (VEVO and ROY reporting import-code identity production verified)
- Branch: `codex/reporting-import-code-deploy-state`
- Code PR merged:
  - PR: `https://github.com/vzeman/biznisweb/pull/91`
  - merge commit: `b86a4b37c2850a9488b9be359958cc4c2dd1884d`
- Guarded production image refresh completed:
  - workflow: `Build and Push ECR`
  - run: `26465206487`
  - ECR image: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest`
  - pushed digest: `sha256:1a6b52ae53ba299b7b8b7e5cc606e44823a78e164869ae21e538bdc6dc19535e`
- Production-equivalent host smoke completed:
  - workflow: `Production Reporting Smoke`
  - run: `26465310874`
  - input: `project=all`, `marker=import-code-product-identity`
- VEVO hard-gate context:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.24.109`
  - service name: `vevo-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:5`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/00b135660f284ed78e4d9be4661e8395`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - UI path: `http://127.0.0.1:8787/dashboard/vevo`
- VEVO verification:
  - `curl localhost` marker returned `LOCALHOST_MARKER_OK` with `daily_profit_rows=388`, `positive_days=250`, `negative_days=138`
  - production board marker returned `PRODUCTION_BOARD_OK` with `active_orders=23`, `manufacturing_products=22`, `units_to_make=46.0`, `customer_fields_returned=0`
  - UI smoke returned `UI_SMOKE_OK:vevo:daily-profit-loss` and `UI_SMOKE_OK:vevo:production-board`
  - final host marker: `PRODUCTION_SMOKE_OK:vevo:00b135660f284ed78e4d9be4661e8395:172.31.24.109`
- ROY hard-gate context:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.18.226`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:4`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/7bae84d5e8364db3b0cd3f00d2c63639`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - UI path: `http://127.0.0.1:8787/dashboard/roy`
- ROY verification:
  - `curl localhost` marker returned `LOCALHOST_MARKER_OK` with `daily_profit_rows=244`, `positive_days=159`, `negative_days=85`
  - UI smoke returned `UI_SMOKE_OK:roy:daily-profit-loss`
  - final host marker: `PRODUCTION_SMOKE_OK:roy:7bae84d5e8364db3b0cd3f00d2c63639:172.31.18.226`
- Next exact step:
  - watch the next scheduled daily reports once and confirm the generated report artifacts still show expected product grouping

### 2026-05-26 (ROY MACO STOP large set component demand)
- Branch: `codex/roy-maco-stop-set-components`
- Change:
  - ROY inventory model now treats each sold `Set MACO STOP VELKY` as component demand for:
    - `Najsilnejsi sprej na medvede MACO STOP Extreme 300ml hmla`
    - `Puzdro MACO STOP na sprej 300ml`
    - `Zvoncek na medvede, plasic medvedov`
  - the configured bundle SKU remains excluded from operational restock/alert demand once the demand is shifted to components
  - component rows inherit bundle order/unit/revenue relevance for the historical restock threshold, so components sold only through the set can still trigger ROY stock alerts
  - added regression coverage proving three set sales produce three component alert/restock rows and no standalone set alert/restock row
- Local verification:
  - `python -m py_compile export_orders.py dashboard_modern.py live_dashboard_server.py roy_operations_dashboard.py`
  - `python -m unittest tests.test_roy_inventory_model tests.test_roy_operations_dashboard`
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `git diff --check`
- Next exact step:
  - open/merge PR, wait for ECR build, deploy/refresh ROY App Runner live dashboard, then verify `/production/roy`

### 2026-05-26 (ROY MACO STOP large set hmla-specific component)
- Branch: `codex/roy-maco-stop-set-hmla-specific`
- Change:
  - tightened the MACO STOP set spray component pattern to `300ml hmla`, so sold sets do not add demand to `MACO STOP Extreme 300ml gel`
  - extended the regression test with a `300ml gel` inventory row and asserted it is not included in set-driven alert/restock rows
- Local verification:
  - `python -m py_compile export_orders.py`
  - `python -m unittest tests.test_roy_inventory_model tests.test_roy_operations_dashboard`
  - `python scripts\reporting_qa_smoke.py`
  - `python -m unittest tests.test_invoice_generation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `git diff --check`
- Next exact step:
  - open/merge PR, wait for ECR build, deploy/refresh ROY App Runner live dashboard, then verify `/production/roy` with the hmla-specific rule

### 2026-05-26 (ROY MACO STOP large set component demand deployed)
- Code merged:
  - PR `#93`: `Map ROY MACO STOP set demand to components`, merge commit `c0c0f13`
  - PR `#94`: `Limit ROY MACO STOP set spray match to hmla`, merge commit `08d79b4`
- Final guarded ECR refresh:
  - workflow: `Build and Push ECR`
  - run: `26469696698`
  - image digest: `sha256:519d464638a76bf0d39a4e04139592682deb881893207753f841e8583e5baf5b`
  - note: PR `#94` did not trigger the ECR path workflow automatically, so the build was manually dispatched from `main`
- Final ROY live dashboard deploy/refresh:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26469803008`
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.28.173`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:4`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/fef8c86b56284f73955f28b00748a107`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - latest artifact path: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
  - localhost marker: `LIVE_ARTIFACT_MARKER_OK`, `kpi_series_days=244`, `inventory_alerts=19`
- App Runner hard-gate:
  - instance-id: `N/A (AWS App Runner managed service)`
  - private IP: `N/A (AWS App Runner managed service)`
  - service name: `biznisweb-roy-operations-dashboard`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - image identifier: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:519d464638a76bf0d39a4e04139592682deb881893207753f841e8583e5baf5b`
  - health path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - public smoke: `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=61:personal_pickups=1:inventory_alerts=19.0:kpi_months=9`
- Public API verification on `/api/operations/roy/live?refresh=1`:
  - `set_alert_or_restock_count=0`
  - `hmla_alert_or_restock_count=2`
  - `gel_alert_or_restock_count=0`
  - `puzdro_alert_or_restock_count=2`
  - `Zvonček na medvede, plašič medveďov` is present in inventory with `available_quantity=368`, so it is healthy and not an urgent alert
- Next exact step:
  - monitor the next scheduled ROY report once and confirm the alert counts stay consistent with the live dashboard

### 2026-05-27 (ROY loss products gross-profit runtime guard)
- Branch: `codex/roy-loss-products-runtime-gross-guard`
- Change:
  - ROY operations dashboard now shows `Produkty v strate` only when the product row has negative gross profit (`gross_profit` or `cm1_profit`)
  - rows that are negative only after fixed costs are ignored by the live operations snapshot, including stale payload rows without gross-profit data
  - ROY App Runner deploy smoke now validates generated payload, S3 latest artifact, live HTML, and live API for gross-profit-only loss products
- Local verification:
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_dashboard_modern`
  - `python -m py_compile roy_operations_dashboard.py live_dashboard_server.py dashboard_modern.py export_orders.py`
  - `python scripts\reporting_qa_smoke.py`
  - workflow YAML parse check for `.github/workflows/deploy-live-dashboard-apprunner.yml`
  - `git diff --check`
- Next exact step:
  - run local tests, open/merge PR, wait for ECR build, deploy ROY App Runner, and verify `/production/roy` live API

### 2026-05-27 (ROY loss products gross fields in payload)
- Branch: `codex/roy-loss-products-payload-gross-fields`
- Change:
  - ROY modern dashboard payload now preserves `gross_profit` and `gross_margin_pct` in `roy_product_demand.loss_product_rows`
  - this fixes the deploy validation failure where the reporting filter selected gross-loss rows but the serialized live payload no longer carried the gross-loss values
- Local verification:
  - `python -m unittest tests.test_dashboard_modern tests.test_roy_inventory_model tests.test_roy_operations_dashboard`
  - `python -m py_compile export_orders.py dashboard_modern.py roy_operations_dashboard.py live_dashboard_server.py`
  - `python scripts\reporting_qa_smoke.py`
  - workflow YAML parse check for `.github/workflows/deploy-live-dashboard-apprunner.yml`
  - `git diff --check`
- Next exact step:
  - run focused tests, open/merge PR, rebuild ECR image, rerun ROY App Runner deploy, and verify live `/production/roy`

### 2026-05-27 (ROY loss products gross-only deployed)
- Code merged:
  - PR `#106`: runtime guard for ROY live `loss_product_rows`, merge commit `8c61efc`
  - PR `#107`: payload carries `gross_profit` / `gross_margin_pct` and ROY product gross loss uses product cost before allocated costs, merge commit `9f6723a`
- ECR refresh:
  - workflow: `Build and Push ECR`
  - run: `26498162375`
  - image digest: `sha256:94e3eaa3686e25bc2a09a32430ce79670290d94c5098a0914ab2420a489b7b02`
- Final ROY live dashboard deploy/refresh:
  - workflow: `Deploy Live Dashboard App Runner`
  - run: `26498273361`
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.10.177`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:7`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/5ade2058c01346408f1fa46c3b551eee`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - latest artifact path: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
- App Runner hard-gate:
  - service name: `biznisweb-roy-operations-dashboard`
  - image identifier: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:94e3eaa3686e25bc2a09a32430ce79670290d94c5098a0914ab2420a489b7b02`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
- Public API verification:
  - `/health`, `/production/roy`, and `/api/operations/roy/live?refresh=1` returned HTTP 200
  - API marker: `roy-operations-dashboard`
  - auto-refresh: `90` seconds
  - live `loss_product_rows` all have negative `gross_profit`
  - current live loss table has `1` row: `Roy powerbanka 10000mAh`, `gross_profit=-51.30`, `gross_margin_pct=-45.1`
  - HTML table header is `Hrubý zisk/strata` and no longer contains `Zisk s fixom`
- Next exact step:
  - monitor the next scheduled ROY report once and confirm the loss-product row count remains gross-profit-only

### 2026-05-27 (ROY picking-list PDF note, barcode, VO signal)
- Branch: `codex/roy-picking-list-barcode-notes`
- Change:
  - ROY operations order query now fetches customer note, internal note, customer contact, invoice/delivery address, item unit prices, product final prices, and tax rate for picking-list PDF output
  - picking-list PDF now renders an order-number Code128 barcode in the top-right, prints the customer note prominently, prints invoice/delivery address blocks, and marks likely wholesale orders with `VEĽKOOBCHOD / VO CENY`
  - wholesale order marking is configured for ROY as: company customer plus at least one order item priced at least 10% below the current product final retail net price
- Local verification:
  - `python -m py_compile roy_operations_dashboard.py roy_picking_lists_pdf.py live_dashboard_server.py`
  - `python -m unittest tests.test_roy_picking_lists_pdf tests.test_roy_operations_dashboard`
  - `python -m json.tool projects\roy\settings.json`
  - direct BiznisWeb GraphQL smoke for ROY operations query: scanned `30`, fulfillable `10`, note/address/wholesale fields present
  - full ROY scan smoke: scanned `330`, pages `11`, fulfillable `27`, current wholesale-marked fulfillable orders `0`
  - generated local sample: `data/roy-picking-list-layout-smoke.pdf`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `git diff --check`
- Next exact step:
  - open/merge PR, rebuild ECR image, deploy ROY App Runner dashboard, then verify `/api/operations/roy/picking-lists.pdf?refresh=1` on production

### 2026-05-27 (ROY picking-list PDF note/barcode deployed)
- Code merged:
  - PR `#127`: `Add ROY picking-list notes and barcode`, merge commit `04c3e6c`
- ECR refresh:
  - workflow: `Build and Push ECR`
  - run: `26522837890`
  - image digest: `sha256:1b2973102ef6cd523aa87ffe3d1bdb4a5b2a18f94e317f70c910f9afc072c9f8`
- ROY live dashboard deploy:
  - first deploy run `26522968512` failed before App Runner update while waiting for the ROY artifact refresh task
  - second deploy run `26524447820` completed successfully
- Fargate hard-gate context from the successful deploy run:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.45.181`
  - service name: `roy-daily-report-email`
  - task definition: `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:14`
  - task ARN: `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/d8d2ae604c5a4c3fbc89788ece4470a5`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - latest artifact path: `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
- App Runner / public verification:
  - service name: `biznisweb-roy-operations-dashboard`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - `/health` returned `{"ok": true}`
  - `/production/roy` returned the `roy-operations-dashboard` marker and the picking-list PDF link
  - `/api/operations/roy/live?refresh=1` returned marker `roy-operations-dashboard`, `auto_refresh_seconds=90`, and order rows now include `customer_note`, `wholesale_pricing`, and `invoice_address`
  - `/api/operations/roy/picking-lists.pdf?refresh=1` returned a valid PDF with `34` pages and `275096` bytes; extracted first page contains order `2677002576`, `Poznámka klienta`, customer note text, and address blocks
- Next exact step:
  - watch one real downloaded picking-list PDF during warehouse use and adjust spacing only if long notes or long addresses crowd the product table

### 2026-05-27 (ROY picking-list large handling flags)
- Branch: `codex/roy-picking-list-big-flags`
- Change:
  - ROY picking-list PDF now prints a large red `OSOBNÝ ODBER - NEBALIŤ` banner when the order is a personal pickup
  - ROY picking-list PDF now prints a large orange `VEĽKOOBCHODNÁ OBJEDNÁVKA` banner when wholesale pricing is detected
  - personal pickup detection in the PDF renderer accepts both the snapshot `personal_pickup` flag and shipping title fallback containing `osobný odber`
- Local verification:
  - `python -m py_compile roy_picking_lists_pdf.py`
  - `python -m unittest tests.test_roy_picking_lists_pdf`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `git diff --check`
- Next exact step:
  - merge PR, rebuild ECR image, deploy ROY App Runner dashboard, and verify live `/api/operations/roy/picking-lists.pdf?refresh=1`

### 2026-05-27 (ROY App Runner deploy skip refresh mode)
- Branch: `codex/deploy-skip-roy-artifact-refresh`
- Change:
  - `deploy-live-dashboard-apprunner.yml` now accepts `skip_artifact_refresh`
  - when enabled, the ROY Fargate pre-deploy task skips the expensive daily report regeneration but still starts a local marker server and verifies it with `curl http://127.0.0.1:8000/marker.json`
  - existing S3 latest artifact validation and App Runner public smoke remain in place
- Reason:
  - the PDF-only deploy for the large handling flags was blocked before App Runner update because the ROY artifact refresh task kept running beyond the practical deploy window
- Next exact step:
  - merge PR, rerun ROY App Runner deploy with `skip_artifact_refresh=true`, then verify live PDF text contains both large banners

### 2026-05-27 (ROY picking-list large handling flags deployed)
- Code merged:
  - PR `#129`: `Add large ROY picking list flags`, merge commit `cf926e1`
  - PR `#130`: `Allow ROY deploy without artifact refresh`, merge commit `2a48306`
- ECR refresh:
  - workflow run: `26525813154`
  - image digest: `sha256:99dd978c6fd8eb7b2a7d0d3ede5b8fc670d667488b335f1f6fd9f628ba743c3e`
- ROY live dashboard deploy:
  - normal deploy run `26525928747` failed before App Runner update because the ROY artifact refresh task ran too long
  - deploy run `26527680459` completed successfully with `skip_artifact_refresh=true`
- Fargate hard-gate context from deploy run `26527680459`:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.16.201`
  - service name: `roy-daily-report-email`
  - image: `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:99dd978c6fd8eb7b2a7d0d3ede5b8fc670d667488b335f1f6fd9f628ba743c3e`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - marker response: `{"marker": "LIVE_ARTIFACT_MARKER_OK", "project": "roy", "mode": "skip_artifact_refresh"}`
  - latest S3 artifact validation: `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=245:inventory_alerts=22.0`
- App Runner / public verification:
  - service name: `biznisweb-roy-operations-dashboard`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - health path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`
  - `/health` returned OK
  - App Runner smoke returned `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=34:personal_pickups=2:inventory_alerts=22.0:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=276911`
  - `/api/operations/roy/picking-lists.pdf?refresh=1` returned a valid PDF with `34` pages and `276911` bytes
  - live PDF page for order `2677002764` contains large `OSOBNÝ ODBER - NEBALIŤ`
  - live PDF pages for orders `2677002708` and `2677002747` contain large `VEĽKOOBCHODNÁ OBJEDNÁVKA`
- Next exact step:
  - watch the next real warehouse download from the ROY dashboard and only adjust spacing if operators report long notes crowding the product rows

### 2026-05-27 (ROY louder new-order sound alert)
- Branch: `codex/roy-louder-new-order-sound`
- Change:
  - ROY operations dashboard new-order alert sound changed from a soft sine beep to a louder two-tone generated Web Audio alarm
  - the sound test played when enabling the toggle is louder, so the operator can confirm browser audio is armed
  - added the `loud-two-tone-v2` marker to the rendered dashboard HTML for deployment verification
- Local verification:
  - `python -m py_compile live_dashboard_server.py roy_operations_dashboard.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_production_board`
  - rendered ROY operations dashboard inline script extracted from `build_roy_operations_dashboard_html("roy")` and checked with `node --check`
  - `python scripts\security_ci.py`
  - `git diff --check`
- Next exact step:
  - open/merge PR, rebuild ECR image, deploy ROY App Runner dashboard with `skip_artifact_refresh=true`, then verify live `/production/roy` contains `loud-two-tone-v2`

### 2026-05-27 (ROY louder new-order sound alert deployed)
- Code merged:
  - PR `#132`: `Make ROY new order sound louder`, merge commit `ae95089`
- ECR refresh:
  - workflow run: `26528371271`
  - image digest: `sha256:137206c12c5c44b97d929064ec1bb60d32a3d52e07d4a3d87ce33d64cc237f2e`
- ROY live dashboard deploy:
  - workflow run: `26528505778`
  - deploy mode: `skip_artifact_refresh=true`
- Fargate hard-gate context from deploy run `26528505778`:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.46.127`
  - service name: `roy-daily-report-email`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - marker response: `{"marker": "LIVE_ARTIFACT_MARKER_OK", "project": "roy", "mode": "skip_artifact_refresh"}`
  - latest S3 artifact validation: `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=245:inventory_alerts=22.0`
- App Runner / public verification:
  - service name: `biznisweb-roy-operations-dashboard`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - health path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`
  - App Runner smoke returned `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=36:personal_pickups=2:inventory_alerts=22.0:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=290171`
  - `/health` returned OK
  - `/production/roy` contains `roy-operations-dashboard`, `soundToggleBtn`, `loud-two-tone-v2`, `playOrderAlertBurst`, `playNewOrderSound(1, 0.75)`, and `function playNewOrderSound(count=1, volume=1)`
  - `/api/operations/roy/live?refresh=1` returned marker `roy-operations-dashboard` and `auto_refresh_seconds=90`
- Next exact step:
  - have the browser tab sound toggle enabled during warehouse use and confirm the new alert volume is sufficient on the next real fulfillable order

### 2026-05-27 (ROY paid-only personal pickup list)
- Branch: `codex/roy-paid-only-personal-pickups`
- Change:
  - ROY operations dashboard personal-pickup list now includes only paid personal pickup orders that are not already shipped
  - COD personal pickups can still appear in the fulfillable order table when they match the COD fulfillment rule, but they no longer appear in the dedicated `Osobné odbery` panel
  - pickup ship checkbox/action is allowed only for paid personal pickup rows, so unpaid/cancelled pickup orders cannot be marked shipped through the dashboard action
  - scan metadata now counts only paid personal pickups for `personal_pickups_seen_during_scan`
- Pre-change live observation:
  - `/api/operations/roy/live?refresh=1` returned `2` personal pickup rows
  - order `2677002554` was present with status `Nezaplatená - zrušená objednávka`, payment `Bankovým prevodom`, shipping `Osobný odber na sklade`
  - order `2677002764` was present with status `Platba online - zaplatené`, payment `Okamžitá platba online`, shipping `Osobný odber na sklade`
- Local verification:
  - `python -m py_compile roy_operations_dashboard.py live_dashboard_server.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_production_board`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - `git diff --check`
- Next exact step:
  - open/merge PR, rebuild ECR image, deploy ROY App Runner dashboard with `skip_artifact_refresh=true`, then verify live personal pickup list excludes `2677002554`

### 2026-05-27 (ROY paid-only personal pickup list deployed)
- Code merged:
  - PR `#134`: `Show only paid ROY personal pickups`, merge commit `c1b532d`
- ECR refresh:
  - workflow run: `26529256498`
  - image digest: `sha256:43aaf4036e8ebd7a7622d921a6dd02ef1e1d4e13dd6464a66f55438454b5cb6b`
- ROY live dashboard deploy:
  - workflow run: `26529371749`
  - deploy mode: `skip_artifact_refresh=true`
- Fargate hard-gate context from deploy run `26529371749`:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.43.81`
  - service name: `roy-daily-report-email`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - marker response: `{"marker": "LIVE_ARTIFACT_MARKER_OK", "project": "roy", "mode": "skip_artifact_refresh"}`
  - latest S3 artifact validation: `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=245:inventory_alerts=22.0`
- App Runner / public verification:
  - service name: `biznisweb-roy-operations-dashboard`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - health path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`
  - App Runner smoke returned `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=36:personal_pickups=1:inventory_alerts=22.0:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=290171`
  - `/health` returned OK
  - `/api/operations/roy/live?refresh=1` returned marker `roy-operations-dashboard`, summary `personal_pickups=1`, and `1` pickup row
  - live pickup list includes `2677002764` with status `Platba online - zaplatené`, `paid_personal_pickup=true`, `pickup_action_allowed=true`
  - live pickup list excludes cancelled unpaid pickup order `2677002554`
- Next exact step:
  - keep the paid-only pickup rule unless warehouse process explicitly needs a separate unpaid pickup queue

### 2026-05-28 (ROY picking lists print-once state)
- Branch: `codex/roy-print-picking-once`
- Change:
  - ROY picking-list PDF endpoint now filters out orders already recorded in operations state under `printed_picking_orders`
  - when the normal dashboard PDF link generates a non-empty PDF, included order numbers are recorded with `printed_at`, `batch_id`, status, purchase date, and sum
  - repeated clicks only include newly fulfillable orders that have not been printed before; previously printed but still unshipped orders are skipped
  - deploy smoke now calls `/api/operations/roy/picking-lists.pdf?refresh=1&preview=1`, so production verification does not mark real orders as printed
  - PDF print state load is fail-closed for configured S3 state, to avoid falling back to an empty local state and reprinting already printed orders
- Local verification:
  - `python -m py_compile roy_operations_dashboard.py live_dashboard_server.py roy_picking_lists_pdf.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_picking_lists_pdf tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_production_board`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `python scripts\reporting_qa_smoke.py`
  - `python scripts\security_ci.py`
  - workflow YAML parse for `.github/workflows/deploy-live-dashboard-apprunner.yml`
  - `git diff --check`
- Next exact step:
  - open/merge PR, rebuild ECR image, deploy ROY App Runner dashboard, then verify preview PDF is side-effect-free and one real PDF download records the printed order batch

### 2026-05-28 (ROY picking lists print-once state deployed)
- Code merged:
  - PR `#136`: `Print ROY picking lists only once`, merge commit `413bd0b`
- ECR refresh:
  - workflow run: `26570433424`
  - image digest: `sha256:2e67e7dcb7a4f8ed0374f63d021d01792900974cacbfc773b613387e4724fe92`
- ROY live dashboard deploy:
  - first deploy run `26570534424` updated App Runner to the new digest but failed during immediate public smoke with a transient HTTP 500 after the update
  - second deploy run `26570904754` completed successfully with `skip_artifact_refresh=true`
- Fargate hard-gate context from successful deploy run `26570904754`:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.18.184`
  - service name: `roy-daily-report-email`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - marker response: `{"marker": "LIVE_ARTIFACT_MARKER_OK", "project": "roy", "mode": "skip_artifact_refresh"}`
  - latest S3 artifact validation: `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=246:inventory_alerts=22.0`
- App Runner / public verification:
  - service name: `biznisweb-roy-operations-dashboard`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - health path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`
  - App Runner smoke returned `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=32:personal_pickups=1:inventory_alerts=22.0:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=262483`
  - `/health` returned OK
  - `/api/operations/roy/live?refresh=1` returned marker `roy-operations-dashboard`
  - preview PDF `/api/operations/roy/picking-lists.pdf?refresh=1&preview=1` returned `262483` bytes
  - `printed_picking_order_count` stayed `0` before and after the preview PDF, confirming deploy smoke and preview mode are side-effect-free
  - normal non-preview PDF download was intentionally not executed during verification because it would mark the current `32` fulfillable orders as printed
- Next exact step:
  - first real warehouse click on `Vysklad. PDF` will mark that batch in `printed_picking_orders`; verify after that click that repeated PDF download returns only newly arrived orders

### 2026-05-28 (ROY live stock overlay for critical inventory alerts)
- Branch: `codex/roy-live-stock-refresh-fix`
- Investigation:
  - direct BizniWeb product search confirmed current stock:
    - `Micro SD KARTA 64GB s adaptérom`: `available_quantity=20`
    - `Držiak na fotopascu 1` / `F_375`: `available_quantity=20`
  - live dashboard inventory still showed both as `Out of stock` because the operations dashboard used the latest reporting artifact inventory snapshot (`inventory_snapshot_date=2026-05-27`) without a live stock overlay
  - Micro SD 64GB also exposed a product identity edge case: historical SKU/EAN `23942440833` currently searches to Micro SD 32GB, so stock matching must prefer the alert product title when SKU/EAN points at a different product title
- Change:
  - ROY operations dashboard now refreshes current BizniWeb stock for severe inventory alerts (`Negative stock`, `Out of stock`, `Critical`) before returning `/api/operations/roy/live`
  - current stock lookup uses batched `getProductList(search=...)` with retry and single-term fallback when BizniWeb returns transient non-JSON errors
  - refreshed availability recalculates risk level, days of cover, suggested reorder units, and removes products from alert/restock/revenue-at-risk rows when stock is no longer critical
  - title-vs-identifier scoring prevents a stale historical EAN/SKU from applying the stock of a different product
- Local verification:
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `python -m unittest tests.test_dashboard_modern tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - local integration against latest ROY dashboard payload + live BizniWeb stock search returned `target_count=23`, `matched_count=23`, `error_count=0`
  - after overlay, alert/restock/revenue-at-risk/stock-risk rows no longer contain `23942440833` / Micro SD 64GB or `F_375` / Držiak na fotopascu 1
- Next exact step:
  - commit/push branch, open PR, merge, rebuild ECR image, deploy ROY App Runner dashboard with `skip_artifact_refresh=true`, then verify `/api/operations/roy/live?refresh=1` no longer reports Micro SD 64GB or `F_375` as out of stock

### 2026-05-28 (ROY live API first-call retry hardening)
- Branch: `codex/roy-live-api-retry`
- Context:
  - PR `#138` was merged and ECR build run `26572098571` pushed digest `sha256:48de2624fe600cdb4166aadd340b4a8ce3bd9449e7b3cd2f0ecafd0d850a90b6`
  - deploy run `26572189877` updated App Runner to that digest and passed host marker checks, but failed public smoke with a transient HTTP `500`
  - direct post-run verification then showed `/health`, `/production/roy`, `/api/operations/roy/live?refresh=1`, and preview picking-list PDF all returned `200`
  - live API confirmed the stock overlay works: Micro SD 64GB / `23942440833` and Držiak / `F_375` were absent from alert/restock/revenue-at-risk/stock-risk rows
  - the live API response had `cache.status=stale_after_error` after a later forced refresh because BizniWeb returned a transient non-JSON GraphQL response during live order fetch
- Change:
  - added per-page retry with backoff to `fetch_open_orders_for_roy_operations` so a one-off BizniWeb GraphQL non-JSON response does not fail the first cold `/api/operations/roy/live` call after App Runner restart
- Local verification:
  - `python -m py_compile roy_operations_dashboard.py live_dashboard_server.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_live_dashboard_auth tests.test_live_dashboard_mobile`
- Next exact step:
  - commit/push retry branch, PR/merge, rebuild ECR image, rerun ROY App Runner deploy with `skip_artifact_refresh=true`, and verify public live API stock alert rows

### 2026-05-28 (ROY live API cold-start generation retry)
- Branch: `codex/roy-live-cold-start-retry`
- Context:
  - PR `#139` was merged and ECR build run `26572693527` pushed digest `sha256:5f3b0f3c4f3915823cd8ff1e77777e106d4bff39b3e3fedc3a9d848491396ad9`
  - deploy run `26572809832` again updated App Runner and passed host marker checks, but the first public `/api/operations/roy/live?refresh=1` call after cold start returned HTTP `500`
  - direct repeated verification showed the first live call failed with a transient BizniWeb non-JSON GraphQL response, while the next calls returned `200`
- Change:
  - `get_cached_roy_operations_snapshot` now retries full snapshot generation up to `3` times when no cache exists yet, which specifically covers App Runner cold-start deploy smoke
- Local verification:
  - `python -m py_compile roy_operations_dashboard.py live_dashboard_server.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_live_dashboard_auth tests.test_live_dashboard_mobile`
- Next exact step:
  - run local py_compile/unit tests, commit/push/PR/merge, rebuild ECR, rerun ROY App Runner deploy, verify public live API stock alerts

### 2026-05-28 (App Runner deploy public smoke curl retry)
- Branch: `codex/deploy-smoke-curl-retry`
- Context:
  - PR `#140` was merged and ECR build run `26573358606` pushed digest `sha256:7260ca4d47cad0953208e556a1914ea91dfd90a0bb698b5848f4b690350eb7e7`
  - deploy run `26573465453` updated App Runner and passed host marker checks, but public smoke still failed on one transient HTTP `500`
  - direct manual verification after the run showed the same endpoints recover on retry: `/api/operations/roy/live?refresh=1` returned `200` and preview picking-list PDF returned `200`
- Change:
  - deploy workflow public smoke now wraps health, dashboard HTML, live API, and preview PDF curls with retry/backoff and raises ROY live/PDF max-time to `240s`
- Local verification:
  - workflow YAML parse for `.github/workflows/deploy-live-dashboard-apprunner.yml`
- Next exact step:
  - parse workflow YAML, commit/push/PR/merge workflow retry, rerun ROY App Runner deploy and verify live alert rows

### 2026-05-28 (ROY live stock overlay deployed)
- Code merged:
  - PR `#138`: `Fix ROY live stock alert refresh`, merge commit `7607420`
  - PR `#139`: `Harden ROY live API order fetch retries`, merge commit `70f87ec`
  - PR `#140`: `Retry ROY live snapshot on cold start`, merge commit `fc25137`
  - PR `#141`: `Retry App Runner public smoke curls`, merge commit `bb43d05`
- ECR refresh:
  - final workflow run: `26573358606`
  - image digest: `sha256:7260ca4d47cad0953208e556a1914ea91dfd90a0bb698b5848f4b690350eb7e7`
- ROY live dashboard deploy:
  - workflow run `26574120208` completed successfully with `skip_artifact_refresh=true`
- Fargate hard-gate context from deploy run `26574120208`:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.31.206`
  - service name: `roy-daily-report-email`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - marker response: `{"marker": "LIVE_ARTIFACT_MARKER_OK", "project": "roy", "mode": "skip_artifact_refresh"}`
  - latest S3 artifact validation: `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=246:inventory_alerts=22.0`
- App Runner / public verification:
  - service name: `biznisweb-roy-operations-dashboard`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - health path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`
  - App Runner smoke returned `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=35:personal_pickups=1:inventory_alerts=22:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=282435`
  - final direct `/api/operations/roy/live?refresh=1` check confirmed stock overlay `target_count=23`, `matched_count=23`, `error_count=0`
  - Micro SD 64GB / `23942440833` and Držiak na fotopascu 1 / `F_375` are absent from `alert_rows`, `restock_priority_rows`, `revenue_at_risk_rows`, and `stock_risk_rows`
  - BizniWeb GraphQL was intermittently returning non-JSON responses during verification; live dashboard now returns the last valid cached payload on those refresh errors
- Next exact step:
  - monitor whether BizniWeb GraphQL instability continues; if yes, consider reducing live stock lookup scope further or adding a short background refresh queue instead of request-time lookups

### 2026-05-30 (ROY picking-list product row formatting)
- Branch: `codex/roy-picking-list-product-format`
- Change:
  - ROY picking-list PDF product rows now show a larger bold quantity value, a new `Cena/ks` column, and EAN as a barcode with small EAN text below it
  - ROY operations order snapshots now carry BizniWeb item unit price into dashboard/PDF data
  - PDF font registration now uses a real bold font when available, so quantity emphasis renders as actual bold text
- Local verification:
  - `python -m py_compile roy_picking_lists_pdf.py roy_operations_dashboard.py live_dashboard_server.py`
  - `python -m unittest tests.test_roy_picking_lists_pdf tests.test_roy_operations_dashboard`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `git diff --check`
- Next exact step:
  - commit/push branch, open PR, merge to `main`, rebuild/deploy ROY App Runner dashboard, then verify live preview picking-list PDF returns a valid PDF

### 2026-05-30 (ROY picking-list PDF row formatting deployed)
- Code merged:
  - PR `#143`: `Format ROY picking list product rows`, merge commit `00608a4`
- ECR refresh:
  - workflow run: `26674047515`
  - image digest: `sha256:fe3650d29ed635d3bd0c556a6d2c19a5771eb163a58c91fc6b0984a42f14c860`
- ROY live dashboard deploy:
  - workflow run: `26674086236`
  - deploy mode: `skip_artifact_refresh=true`
- Fargate hard-gate context from deploy run:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.40.100`
  - service name: `roy-daily-report-email`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - marker response: `{"marker": "LIVE_ARTIFACT_MARKER_OK", "project": "roy", "mode": "skip_artifact_refresh"}`
- App Runner hard-gate / public verification:
  - instance-id: `N/A (AWS App Runner managed service)`
  - private IP: `N/A (AWS App Runner managed service)`
  - service name: `biznisweb-roy-operations-dashboard`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - health path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/health`
  - App Runner smoke returned `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=6:personal_pickups=0:inventory_alerts=20:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=130112`
  - direct `/health` returned `200`
  - direct `/production/roy` returned `200` and contains marker `roy-operations-dashboard`
  - preview picking-list PDF `/api/operations/roy/picking-lists.pdf?refresh=1&preview=1` returned `200`, `130112` bytes, and starts with `%PDF-`
  - extracted preview PDF text contains `Ks`, `Cena/ks`, `Import kód`, and `EAN`
  - live API returned marker `roy-operations-dashboard`; current fulfillable order items had unit prices on `9/9` rows
- Next exact step:
  - visually review one newly downloaded picking-list PDF from the dashboard before using it operationally for all orders

### 2026-05-30 (ROY wholesale trigger diagnosis)
- Branch: `codex/roy-wholesale-discount-trigger`
- Finding:
  - direct BiznisWeb check for order `2677002772` now returns customer `Blackmarket s.r.o.` as `Company`
  - current ROY wholesale logic evaluates order `2677002772` as wholesale: `2/2` priced lines discounted, max discount `49.1%`
  - local PDF generated from the current code for order `2677002772` contains `VEĽKOOBCHOD / VO CENY`, `VEĽKOOBCHODNÁ OBJEDNÁVKA`, and `VO ceny: áno, zľava do 49.1%`
  - root risk: ROY config still required BiznisWeb to classify the customer as `Company`; if the customer was not classified that way at print time, discount-only wholesale orders could miss the flag
- Change:
  - ROY wholesale detection no longer requires `Company` customer classification; any order line discounted at least `10%` vs current retail final price triggers the wholesale flag
  - wholesale detection reason text now distinguishes company and non-company discounted orders
- Correction:
  - this discount-only trigger was too broad in production because normal discounted person orders also matched it; see the follow-up correction below
- Local verification:
  - `python -m json.tool projects\roy\settings.json`
  - `python -m py_compile roy_operations_dashboard.py roy_picking_lists_pdf.py live_dashboard_server.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_picking_lists_pdf`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `git diff --check`
- Next exact step:
  - superseded by the stricter company-signal correction below

### 2026-05-30 (ROY wholesale trigger company-signal correction)
- Branch: `codex/roy-wholesale-company-signal`
- Finding:
  - after deploying the discount-only trigger, the current live API marked `6/6` fulfillable orders as wholesale
  - four of those orders were `Person` customers with ordinary discounts around `18.7%`; they should not be VO just because the product is discounted
  - the intended safe trigger is `Company` customer signal plus at least one priced line discounted by `10%` or more vs current product final retail price
  - order `2677002772` still qualifies under this stricter rule because BiznisWeb currently returns `Blackmarket s.r.o.` as `Company`
- Change:
  - restored ROY `wholesale_detection.require_company_customer=true`
  - kept the `10%` discount threshold and clearer reason text
  - added a regression test proving a discounted `Person` order is not flagged as wholesale when the company signal is required
- Local verification:
  - `python -m json.tool projects\roy\settings.json`
  - `python -m py_compile roy_operations_dashboard.py roy_picking_lists_pdf.py live_dashboard_server.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_picking_lists_pdf`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - direct current BiznisWeb check with local code returned `fulfillable=6`, `wholesale=2`, wholesale order numbers `2677002789` and `2677002792`; the four `Person` customer orders with ordinary `18.7%` discounts were not wholesale
  - `git diff --check`
- Next exact step:
  - commit/push branch, open PR, merge, rebuild ECR, deploy ROY App Runner, then verify current live wholesale count is no longer `6/6`

### 2026-05-30 (ROY wholesale company-signal correction deployed)
- Code merged:
  - PR `#147`: `Restore ROY wholesale company signal`, merge commit `0fde4f3`
- ECR refresh:
  - workflow run: `26674841497`
  - image digest: `sha256:6da688ffb7f96bde0ab412f5bf5f99a13afa19c8309237d64e2a412c7d3b3107`
- ROY live dashboard deploy:
  - workflow run: `26674889679`
  - deploy mode: `skip_artifact_refresh=true`
- Fargate hard-gate context from deploy run:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.34.58`
  - service name: `roy-daily-report-email`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - marker response: `{"marker": "LIVE_ARTIFACT_MARKER_OK", "project": "roy", "mode": "skip_artifact_refresh"}`
- App Runner / public verification:
  - instance-id: `N/A (AWS App Runner managed service)`
  - private IP: `N/A (AWS App Runner managed service)`
  - service name: `biznisweb-roy-operations-dashboard`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - App Runner smoke returned `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=6:personal_pickups=0:inventory_alerts=20:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=130112`
  - direct `/health` returned `200`
  - direct `/production/roy` returned `200` and contains marker `roy-operations-dashboard`
  - direct `/api/operations/roy/live?refresh=1` returned `200`, marker `roy-operations-dashboard`, `orders=6`, `wholesale_orders=2`
  - live wholesale order numbers are `2677002789` and `2677002792`; four `Person` customer orders with ordinary `18.7%` discounts are not wholesale
  - preview picking-list PDF returned `200`, `130112` bytes, starts with `%PDF-`, and the first wholesale page contains `VEĽKOOBCHOD / VO CENY`, `VEĽKOOBCHODNÁ OBJEDNÁVKA`, and `VO ceny: áno, zľava do 49.1%`
- Next exact step:
  - if order `2677002772` was printed from an older already-downloaded PDF, use a newly generated PDF for corrected VO banners; future discounted person orders should no longer be flagged as wholesale

### 2026-05-30 (ROY wholesale gross price and coupon guard)
- Branch: `codex/roy-wholesale-gross-price-trigger`
- Finding:
  - BiznisWeb ROY item `price` / `sum` values behave as net values even when `is_net_price=false`, while `product.final_price` behaves as a gross regular price
  - previous wholesale comparison divided item net prices by VAT again, creating a false `18.7%` discount on full-price person orders
  - `sum_with_tax / quantity` is the reliable gross item unit price for the live order payload
  - order `2677002772` satisfies the intended rule: IČO `51983095`, no discount code, HC800 `25.50` vs `29.99` gross, Discovery `59.99` vs `95.90` gross
- Change:
  - ROY operations query now fetches item `sum_with_tax`
  - wholesale detection now treats company signal as present only when `company_id` / IČO exists
  - wholesale discount comparison now uses gross item unit price vs gross product final price
  - discount-code price elements (`percent_discount`, coupon/voucher/gift/kód signals) prevent the wholesale flag
  - wholesale examples now expose `order_unit_gross` and `retail_unit_gross`
- Local verification:
  - `python -m json.tool projects\roy\settings.json`
  - `python -m py_compile roy_operations_dashboard.py roy_picking_lists_pdf.py live_dashboard_server.py`
  - `python -m unittest tests.test_roy_operations_dashboard tests.test_roy_picking_lists_pdf`
  - direct current BiznisWeb check with local code returned `fulfillable=6`, `wholesale=2`, wholesale order numbers `2677002789` and `2677002792`; full-price person orders had `max_discount=0.0`, and the discount-code order had `discount_code_used=True`
  - direct order `2677002772` check returned `wholesale=True`, `discount_code_used=False`, `max_discount_pct=37.4`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_roy_picking_lists_pdf tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity`
  - `git diff --check`
- Next exact step:
  - commit/push branch, open PR, merge, rebuild ECR, deploy ROY App Runner, then verify live wholesale count and preview PDF banners

### 2026-05-30 (ROY wholesale gross price and coupon guard deployed)
- Code merged:
  - PR `#149`: `Fix ROY wholesale gross price trigger`, merge commit `142fead`
- ECR refresh:
  - workflow run: `26675578725`
  - image digest: `sha256:f46baf4d1132b067ff8fa3c4177556b316e348e868d10b5b001c5e657c855bf9`
- ROY live dashboard deploy:
  - workflow run: `26675624303`
  - deploy mode: `skip_artifact_refresh=true`
- Fargate hard-gate context from deploy run:
  - instance-id: `N/A (scheduled ECS/Fargate task)`
  - private IP: `172.31.38.25`
  - service name: `roy-daily-report-email`
  - marker path: `http://127.0.0.1:8000/marker.json`
  - marker response: `{"marker": "LIVE_ARTIFACT_MARKER_OK", "project": "roy", "mode": "skip_artifact_refresh"}`
- App Runner / public verification:
  - instance-id: `N/A (AWS App Runner managed service)`
  - private IP: `N/A (AWS App Runner managed service)`
  - service name: `biznisweb-roy-operations-dashboard`
  - service ARN: `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
  - production path: `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - App Runner smoke returned `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=6:personal_pickups=0:inventory_alerts=20:kpi_months=9:gross_loss_products=1:picking_pdf_bytes=130112`
  - direct `/health` returned `200`
  - direct `/production/roy` returned `200` and contains marker `roy-operations-dashboard`
  - direct `/api/operations/roy/live?refresh=1` returned `200`, marker `roy-operations-dashboard`, `orders=6`, `wholesale_orders=2`
  - live wholesale order numbers are `2677002789` and `2677002792`
  - full-price person orders now show `max_discount=0.0`; order `2677002795` shows `discount_code_used=True` and is not wholesale
  - preview picking-list PDF returned `200`, `130112` bytes, starts with `%PDF-`, and first page contains `VEĽKOOBCHOD / VO CENY`, `VEĽKOOBCHODNÁ OBJEDNÁVKA`, and `VO ceny: áno, zľava do 37.4%`
- Next exact step:
  - use newly generated picking-list PDFs for future VO checks; the older already-downloaded PDFs do not recalculate banners

### 2026-06-02 (VEVO/ROY realized revenue payment filter)
- Branch: `codex/realized-revenue-payment-filter`
- PR: `#151` (`https://github.com/vzeman/biznisweb/pull/151`)
- Context:
  - ROY report for `2026-06-01` counted transfer order `2677002831` even though it was not paid yet
  - reporting revenue is item-level net revenue (`item_total_without_tax`), not gross order total with VAT/shipping
- Change:
  - realized reporting revenue now includes only:
    - COD payment with status `Čaká na vybavenie` or `Odoslaná`
    - status `Platba online - zaplatené` for paid online-card / bank-transfer orders
  - all other statuses/payments are excluded until the order reaches `Platba online - zaplatené`, at which point the order is counted retroactively on its original purchase date
  - the same `realized_revenue` config is defined for VEVO and ROY
  - order query now fetches `price_elements`, and exported CSV rows include payment/shipping audit fields plus realized-revenue decision fields
  - order cache schema bumped to `2`, so old cached order days without payment metadata are refreshed before reporting
- Local verification:
  - `python -m py_compile export_orders.py`
  - `python -m json.tool projects\vevo\settings.json`
  - `python -m json.tool projects\roy\settings.json`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity` (`62` tests OK)
  - `git diff --check`
- Live read-only verification:
  - ROY `2026-06-01`: current API returned `14` orders; new realized-revenue filter includes `13`
  - order `2677002831` is excluded with reason `cod_status_without_cod_payment` because status is `Čaká na vybavenie` and payment is `Bankovým prevodom` (`reference_id=6`)
  - ROY item-net revenue for `2026-06-01` changes from `1401.46` to `791.83` EUR under the new filter
- Known issues:
  - production is not deployed yet; corrected historical figures require PR merge, image rebuild, and a full-history reporting refresh for VEVO and ROY
- Next exact step:
  - commit/push this branch, open PR, merge to `main`, rebuild the reporting image, run VEVO/ROY full-history reports, then verify on host with localhost marker before UI smoke

### 2026-06-02 (VEVO/ROY realized revenue deployed and ROY latest refreshed)
- Code merged:
  - PR `#151`: `Filter reporting revenue by payment state`, merge commit `3e7356c`
  - PR `#152`: `Harden order payment metadata fallback`, merge commit `ce9896a`
- Final realized revenue logic:
  - COD (`dobierka`) counts only with status `ÄŚakĂˇ na vybavenie` or `OdoslanĂˇ`
  - online card / bank transfer counts only with status `Platba online - zaplatenĂ©`
  - unpaid transfer/card orders are excluded until the paid status appears, then they count retroactively on original `pur_date`
  - CSV export includes audit fields `realized_revenue`, `realized_revenue_reason`, `payment_title`, `payment_reference_id`, `shipping_title`, `shipping_reference_id`
- Fetch hardening:
  - `getOrderList` still fetches `price_elements` by default
  - if BizniWeb returns an internal error on `price_elements`, the same page is retried without `price_elements`
  - COD-status orders on fallback pages try per-order payment enrichment via `getOrder`
  - if payment metadata still fails, the order is not counted and is marked `cod_status_missing_payment_metadata`
- Local verification:
  - `python -m py_compile export_orders.py`
  - `python -m unittest tests.test_reporting_calculation_fixes`
  - `python -m unittest tests.test_invoice_generation tests.test_unpaid_order_cancellation tests.test_reporting_calculation_fixes tests.test_production_board tests.test_live_dashboard_auth tests.test_live_dashboard_mobile tests.test_roy_operations_dashboard tests.test_roy_inventory_model tests.test_reporting_product_identity` (`64` tests OK)
  - `git diff --check`
  - live read-only VEVO probe fetched `600` DESC orders, hit order `2602007112`, continued through fallback, and excluded it as `cod_status_missing_payment_metadata`
- ECR refresh:
  - workflow run `26800501453` succeeded
  - image digest `sha256:709b0f56a664748c5cce28304f93a2b36bbf46d3d159653052dab1ebf7357ef2`
- Production reporting smoke:
  - workflow run `26800590329` succeeded for VEVO and ROY with marker `realized-revenue-fallback-20260602`
  - VEVO Fargate hard-gate:
    - instance-id `N/A (scheduled ECS/Fargate task)`
    - private IP `172.31.8.236`
    - service `vevo-daily-report-email`
    - task definition `vevo-reporting-daily:5`
    - task `5cb7e06ce8c548eebbc24a6f99ec95fc`
    - marker `LOCALHOST_MARKER_OK`
    - UI smoke `UI_SMOKE_OK:vevo:production-board` and `UI_SMOKE_OK:vevo:daily-profit-loss`
    - marker summary `daily_profit_rows=395`, production board `active_orders=55`, `manufacturing_products=37`, `units_to_make=105.0`
    - `price_elements` fallback used twice; metadata enrichment `28/27/1` and `17/17/0`; only failed metadata order was VEVO `2602007112`
  - ROY Fargate smoke:
    - instance-id `N/A (scheduled ECS/Fargate task)`
    - private IP `172.31.20.151`
    - service `roy-daily-report-email`
    - task definition `roy-reporting-daily:25`
    - task `02f1317a52fa403786079e810b4c80fa`
    - marker `LOCALHOST_MARKER_OK`
    - UI smoke `UI_SMOKE_OK:roy:daily-profit-loss`
    - marker summary `daily_profit_rows=251`, `positive_days=166`, `negative_days=85`
    - ROY saw transient BizniWeb non-JSON order-list errors, but DESC pagination still reached the requested historical boundary (`oldest_in_batch=2025-08-01`, needed `2025-09-24`)
- ROY stable latest refresh / App Runner deploy:
  - workflow run `26801879954` succeeded with `skip_artifact_refresh=false`
  - Fargate hard-gate:
    - instance-id `N/A (scheduled ECS/Fargate task)`
    - private IP `172.31.30.127`
    - service `roy-daily-report-email`
    - task definition `roy-reporting-daily:26`
    - task `6c6fd4892d6c466b8061704d8275ec49`
    - marker path `http://127.0.0.1:8000/marker.json`
    - latest S3 artifact `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/latest/dashboard_payload_latest.json`
    - marker `LIVE_ARTIFACT_MARKER_OK`
    - S3 check `ROY_LIVE_ARTIFACTS_OK:kpi_series_days=251:inventory_alerts=13.0`
  - App Runner hard-gate:
    - instance-id `N/A (AWS App Runner managed service)`
    - private IP `N/A (AWS App Runner managed service)`
    - service `biznisweb-roy-operations-dashboard`
    - service ARN `arn:aws:apprunner:eu-central-1:919341186960:service/biznisweb-roy-operations-dashboard/ff762bb1c93148638741c62e7abb45b2`
    - production path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
    - image digest `sha256:709b0f56a664748c5cce28304f93a2b36bbf46d3d159653052dab1ebf7357ef2`
    - smoke `APP_RUNNER_ROY_OPERATIONS_OK:fulfillable_orders=3:personal_pickups=1:inventory_alerts=9:kpi_months=10:gross_loss_products=1:picking_pdf_bytes=109035`
    - deploy `APP_RUNNER_DEPLOY_OK:biznisweb-roy-operations-dashboard:https://qvfzvh82c3.eu-central-1.awsapprunner.com`
- ROY order `2677002831` note:
  - initial live read-only check during PR #151 showed the order as transfer payment + `ÄŚakĂˇ na vybavenie`, so it was correctly excluded and ROY `2026-06-01` item-net revenue was `791.83` EUR under the new filter
  - later live check showed the same order changed to `Platba online - zaplatenĂ©`, so it now correctly counts retroactively on `2026-06-01`; ROY `2026-06-01` item-net revenue is therefore `1401.46` EUR under the requested paid-status rule
- AWS billing/runtime observation:
  - this task was not blocked by an AWS payment/runtime outage: GitHub AWS credentials worked, ECR push succeeded, ECS/Fargate tasks ran, CloudWatch logs were readable, S3 latest artifact refresh succeeded, and App Runner deploy/smoke succeeded
- Known issues:
  - BizniWeb API intermittently returns non-JSON error pages during ROY order-list pagination; current opposite-direction fallback covered the verified runs, but repeated failures should be treated as the next pagination hardening target
  - VEVO latest scheduled email/report will use the corrected image on the next `vevo-daily-report-email` run; immediate VEVO untagged email backfill was not sent during this session
- Next exact step:
  - monitor the next scheduled VEVO/ROY reporting runs for BizniWeb non-JSON pagination errors and, if recurring, implement stricter page-level retry/fail-fast telemetry before changing more reporting logic

### 2026-06-17 (creditnote Storno guard and full-history refresh)
- Branch: `codex/creditnote-carrier-audit`
- Change:
  - added `creditnote_storno_guard.py`, a reusable pre-export guard that scans creditnotes and changes creditnoted orders to `Storno` only when the order still counts in realized reporting revenue
  - enabled the guard for `roy` and `vevo` project settings with target status `Storno`
  - wired the guard into `daily_report_runner.py` before export, with metrics, fail-fast behavior on mutation failures, and automatic `--clear-cache` when any order status is changed
  - added CLI/env controls: `--skip-creditnote-storno-guard`, `--creditnote-storno-dry-run`, `REPORT_SKIP_CREDITNOTE_STORNO_GUARD`, `REPORT_CREDITNOTE_STORNO_DRY_RUN`
  - kept GitHub smoke/artifact-refresh workflows in `--creditnote-storno-dry-run` mode so verification jobs do not mutate BizniWeb
  - added `audits/creditnote_storno_20260617.json` with the live order-change audit
- Live BizniWeb action:
  - target status resolved to `Storno` with status id `17` for both shops
  - ROY dry run found `51` eligible creditnoted revenue orders; live run changed all `51`; failures `0`
  - VEVO dry run found `45` eligible creditnoted revenue orders; live run changed all `45`; failures `0`
  - post-mutation dry run found `eligible_orders=0` for both ROY and VEVO
- Backfill:
  - regenerated ROY local reporting outputs for `2025-09-24..2026-06-16` with `--clear-cache`; export completed with exit code `0` and found `2888` orders
  - regenerated VEVO local reporting outputs for `2025-05-03..2026-06-16` with `--clear-cache`; export completed with exit code `0`
  - refreshed local latest files: `data/roy/report_latest.html`, `data/roy/dashboard_payload_latest.json`, `data/vevo/report_latest.html`, `data/vevo/dashboard_payload_latest.json`
- Verification:
  - `python -m py_compile creditnote_storno_guard.py creditnote_export.py monthly_creditnote_export_runner.py daily_report_runner.py`
  - `python -m unittest tests.test_creditnote_storno_guard tests.test_creditnote_export tests.test_invoice_generation tests.test_unpaid_order_cancellation` (`31` tests OK)
  - `python -m json.tool projects\roy\settings.json`
  - `python -m json.tool projects\vevo\settings.json`
  - YAML parse check for modified GitHub workflows
  - `git diff --check`
- Known issues:
  - code is not deployed yet; daily production automation will use the guard only after merge/build/deploy
  - local regenerated report artifacts were not uploaded to S3 in this step
- Next exact step:
  - commit and push this branch, open/merge PR, rebuild the reporting image, then run a production artifact refresh with infra hard-gate verification before UI checks

### 2026-06-17 (creditnote fulfillment cost correction)
- Branch: `codex/creditnote-carrier-audit`
- Correction:
  - creditnoted/storno orders now remove revenue and sold-product COGS, but keep outbound fulfillment cost because the parcel was actually sent
  - added `creditnote_fulfillment_costs` settings for ROY and VEVO
  - daily/date/month aggregations now expose `creditnote_fulfillment_orders`, `creditnote_packaging_cost`, `creditnote_shipping_net_cost`, and `creditnote_fulfillment_cost`
  - `packaging_cost`, `shipping_net_cost`, `total_cost`, `net_profit`, CM1, CM2, and financial metrics now include those retained fulfillment costs
  - cache schema bumped to `4` and daily cache now stores raw status orders, not only revenue-included orders, so cached future runs can still account for dobropis/storno fulfillment costs
- Corrected impact of the 2026-06-17 status-change intervention:
  - ROY: `51` orders, revenue down `6279.26` EUR, net profit down `3469.10` EUR
  - VEVO: `45` orders, revenue down `628.65` EUR, net profit down `407.40` EUR
  - Total: `96` orders, revenue down `6907.91` EUR, net profit down `3876.50` EUR
- Current regenerated reports now retain all creditnote fulfillment costs:
  - ROY: `105` creditnote fulfillment orders, retained fulfillment cost `26.25` EUR
  - VEVO: `266` creditnote fulfillment orders, retained fulfillment cost `133.00` EUR
- Backfill:
  - regenerated ROY local reporting outputs for `2025-09-24..2026-06-16` with `--clear-cache`; exit code `0`
  - regenerated VEVO local reporting outputs for `2025-05-03..2026-06-16` with `--clear-cache`; exit code `0`
- Verification:
  - `python -m py_compile export_orders.py creditnote_storno_guard.py creditnote_export.py monthly_creditnote_export_runner.py daily_report_runner.py`
  - `python -m unittest tests.test_reporting_calculation_fixes tests.test_creditnote_storno_guard tests.test_creditnote_export tests.test_invoice_generation tests.test_unpaid_order_cancellation` (`49` tests OK)
  - `python -m json.tool projects\roy\settings.json`
  - `python -m json.tool projects\vevo\settings.json`
  - `python -m json.tool data\roy\dashboard_payload_latest.json`
  - `python -m json.tool data\vevo\dashboard_payload_latest.json`
  - `git diff --check`
- Known issues:
  - code is not deployed yet; production daily reporting needs PR merge/build/deploy and production artifact refresh
  - regenerated local artifacts are not uploaded to S3 yet
- Next exact step:
  - commit and push correction to PR `#177`, then merge/deploy and run production artifact refresh with required infra hard-gate verification before UI checks

### 2026-06-18 (daily email output tag reset after creditnote guard)
- Branch: `codex/reset-output-tag-after-storno`
- Context:
  - production reporting smoke run `27737957097` refreshed the VEVO scheduled task definition to the new creditnote reporting image and confirmed the Fargate hard-gate context before export
  - VEVO hard-gate context from that run: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.14.23`, service `vevo-daily-report-email`, task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:6`, task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/694dfb31d74c47f0ab155148ac985417`, image digest `sha256:10202cb947ab0ab50ec2be9fe6331c8cc48e5204df60ebdaffe271369dd03bbd`
  - the run failed before SES send because `creditnote_storno_guard.py` built its exporter with `output_tag=creditnote_storno_guard`; `BiznisWebExporter.__init__` writes that tag to `REPORT_OUTPUT_TAG`, and the daily export subprocess inherited it
  - result: the real daily run produced tagged files like `report_...__creditnote_storno_guard.html`, while `daily_report_runner.py` correctly expected untagged daily files for the email
- Change:
  - `daily_report_runner.py` now restores the daily runner `REPORT_OUTPUT_TAG` immediately after the creditnote storno guard finishes and before spawning the export subprocess
  - added a regression test that simulates the guard leaking `REPORT_OUTPUT_TAG=creditnote_storno_guard` and asserts the subsequent daily export receives the intended empty tag
- Local verification:
  - `python -m py_compile daily_report_runner.py`
  - `python -m unittest tests.test_invoice_generation tests.test_creditnote_storno_guard tests.test_creditnote_export tests.test_reporting_calculation_fixes tests.test_dashboard_modern` (`51` tests OK)
  - `git diff --check`
- Next exact step:
  - commit/push this branch, merge through PR, wait for the ECR rebuild, then dispatch `Production Reporting Smoke` with `project=all`, `send_email=true`, and `update_task_image=true` to regenerate and send the corrected VEVO and ROY reports

### 2026-06-18 (creditnote reporting email regeneration completed)
- Code merged:
  - PR `#182`: `Restore report output tag after creditnote guard`, merge commit `cee74e40ceff584b6109e2d7547fae64c08847c8`
- ECR refresh:
  - workflow run `27738925105` succeeded
  - image digest `sha256:2b358fceeae7dc26bf4196b4cc11048a67416deb12c8c891143b4225b48c1aa5`
- Production reporting rerun:
  - workflow run `27739014933` succeeded with `project=all`, marker `creditnote-email-20260618-final2`, `send_email=true`, `update_task_image=true`
  - VEVO schedule `vevo-daily-report-email` updated from digest `sha256:10202cb947ab0ab50ec2be9fe6331c8cc48e5204df60ebdaffe271369dd03bbd` to `sha256:2b358fceeae7dc26bf4196b4cc11048a67416deb12c8c891143b4225b48c1aa5`
  - ROY schedule `roy-daily-report-email` updated from digest `sha256:c41f9463ee724ac1be904130179958afe76eef5f2998b40b487a52e098e241de` to `sha256:2b358fceeae7dc26bf4196b4cc11048a67416deb12c8c891143b4225b48c1aa5`
- VEVO verification:
  - instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.10.0`, service `vevo-daily-report-email`
  - task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:7`
  - task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/053d8a2e87f640d1ae522419b454f992`
  - task image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:2b358fceeae7dc26bf4196b4cc11048a67416deb12c8c891143b4225b48c1aa5`, `task-image-updated=true`
  - SES `MessageId=0107019ed949eeb9-b4aa020a-ee22-4041-a082-5ad1342cdab2-000000`
  - localhost marker `LOCALHOST_MARKER_OK`, `has_creditnote_payload=true`, `send_email=true`, `creditnote_count=267`, `credited_gross_eur=4794.93`, report path `data/vevo/report_latest.html`
  - UI smoke `UI_SMOKE_OK:vevo:production-board` and `UI_SMOKE_OK:vevo:daily-profit-loss`
- ROY verification:
  - instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.42.198`, service `roy-daily-report-email`
  - task definition `arn:aws:ecs:eu-central-1:919341186960:task-definition/roy-reporting-daily:36`
  - task `arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/b83d7d5a6cf4426881f65ac906134528`
  - task image `919341186960.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@sha256:2b358fceeae7dc26bf4196b4cc11048a67416deb12c8c891143b4225b48c1aa5`, `task-image-updated=true`
  - SES `MessageId=0107019ed971382b-a168b65e-3369-4c9f-b0f3-d344d2ffba2f-000000`
  - localhost marker `LOCALHOST_MARKER_OK`, `has_creditnote_payload=true`, `send_email=true`, `creditnote_count=110`, `credited_gross_eur=11063.52`, report path `data/roy/report_latest.html`
  - UI smoke `UI_SMOKE_OK:roy:daily-profit-loss`
- Current status:
  - corrected reporting emails for both e-shops were regenerated and sent on `2026-06-18`
  - future scheduled daily emails now use the image containing the visible creditnote metrics and the output-tag leak fix
- Next exact step:
  - monitor the next regular morning email for both e-shops; no known code blocker remains for the requested creditnote reporting metrics

### 2026-07-15 (ROY KIRVO purchase cost and full-history production refresh)
- Repo/branch:
  - implementation merged through PR `#226` as commit `7a11d6c0df3ab34f1d4a5920aade615720f1fdb0`
  - production-state handoff branch: `codex/roy-kirvo-deploy-state`
- Change:
  - mapped the net purchase cost `1.90 EUR` for active KIRVO lure SKU `H-9D2E0A2C`
  - mapped the same cost for inactive catalog title alias `H-9400721F`
  - removed the stale `35%` margin override for `H-9D2E0A2C`, so the known purchase cost wins
  - added regression coverage for both exact title hashes and known-cost precedence over a legacy margin override
- Historical KIRVO verification through `2026-07-14`:
  - `4` rows / `4` units, item-net revenue `36.58 EUR`
  - immutable export now reports `expense_per_item=1.90` and `expense_source=mapped_product_sku` on all four rows
  - product cost changed from `23.76 EUR` to `7.60 EUR`
  - pre-ad product profit changed from `12.82 EUR` to `28.98 EUR`, a `+16.16 EUR` correction
- Profit reconciliation against the immediately preceding immutable generation:
  - company profit changed from `21,341.05 EUR` to `20,225.42 EUR` (`-1,115.63 EUR`)
  - KIRVO contributes `+16.16 EUR` of that change
  - the same current image also activates the previously requested zero-revenue rule: only configured ROY knife gifts sold for `0 EUR` retain zero cost; `264` non-knife zero-revenue rows now retain their known real purchase costs, adding `1,131.53 EUR` of product cost
  - Google Ads source data refreshed by `0.26 EUR`; arithmetic reconciliation is `+16.16 - 1,131.53 - 0.26 = -1,115.63 EUR`
  - `76` configured zero-revenue ROY knife-gift rows remain at zero cost as requested
- Local/code verification:
  - full unit suite: `169` tests OK
  - `python scripts/reporting_qa_smoke.py` passed
  - Python compile, both modified JSON parses, and `git diff --check` passed
- Production build and full-history backfill:
  - ECR build workflow `29445620486` succeeded
  - exact image digest `sha256:38506ae26d5b490c4d327185062235225a91d8ec0437bdc139d91874bcd4048a`
  - deploy/backfill workflow `29445783791` succeeded
  - Fargate hard-gate identity: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.20.19`, service `roy-daily-report-email`, task `95871c05f11f4f0b9dd7ba5f7096a935`, candidate task definition `roy-reporting-daily:48`, runtime gate script `/app/scripts/live_dashboard_refresh_gate.sh`, marker path `http://127.0.0.1:8000/marker.json`
  - Fargate task stopped normally with container exit code `0`
  - host checks passed: `LOCALHOST_LIVE_DASHBOARD_OK:roy:periods=7d,30d,90d,full` and `LIVE_ARTIFACT_MARKER_OK` with `kpi_series_days=294`, `inventory_rows=160`, `inventory_alerts=18`
  - immutable S3 generation `20260715T201821Z` contains exactly eight manifest artifacts under `s3://biznisweb-reporting-artifacts-919341186960-eu-central-1/daily-reports/roy-sk/20260715T201821Z/`
  - scheduler `roy-daily-report-email` is `ENABLED`, `cron(30 1 * * ? *)`, `Europe/Bratislava`, and promoted from task definition `:47` to `:48`
  - task definition `:48` uses dedicated role `BiznisWebReportingTaskRole-roy` and the exact immutable digest above
- App Runner and live UI/API verification after the host marker:
  - instance-id/IP `N/A (AWS App Runner managed service)`, service `biznisweb-roy-operations-dashboard`, path `https://qvfzvh82c3.eu-central-1.awsapprunner.com/production/roy`
  - App Runner operation `c826bae59ec4481aa672a2bcffae2644` succeeded and the service is `RUNNING` on the exact new digest
  - public DNS resolved to `3.68.0.57`, `3.74.6.217`, and `3.66.161.94`; `/health` returned HTTP `200` with `ok=true`
  - authenticated UI returned HTTP `200` with the ROY operations and Executive KPI markers
  - authenticated full-period API returned HTTP `200`, `project=roy`, `period=full`, company profit `20,225.42 EUR`, `qa_failure_count=0`, and `is_partial=false`
- Known issues:
  - one transient BizniWeb `price_elements` failure on order `2677001207` was recovered by the existing fallback; the completed export is not partial
  - the production Facebook access token appeared in a local ignored `.env` line in tool output during a read-only audit; it was not committed or added to any project artifact, but it must be treated as compromised and rotated in Meta plus the production secret store
- Next exact step:
  - rotate the production Facebook access token, update the managed production secret without committing it, then verify the Facebook connection and monitor the next scheduled `roy-daily-report-email` run on task definition `:48`

### 2026-07-16 (live dashboard Unicode Basic Auth hardening)
- Verification false alarm and real robustness bug:
  - a read-only PowerShell verification pipeline corrupted the ASCII SSM password into a non-ASCII request value; those malformed requests returned HTTP `502`
  - direct secret loading via boto3 confirmed ROY production stayed healthy: authenticated board/API/PDF routes returned HTTP `200`
  - the malformed request still exposed a real server bug: string-based `hmac.compare_digest` raises `TypeError` for non-ASCII input instead of rejecting invalid authentication with HTTP `401`
- Fix in progress:
  - compare UTF-8 encoded credential bytes in constant time and reject unencodable values safely
  - add regression tests for valid UTF-8 credentials and malformed non-ASCII input against ASCII production credentials
- Verified before deploy:
  - production traceback identifies the failure at `live_dashboard_server.py:66`
  - reporting artifacts, scheduler, App Runner service, and correctly authenticated live routes remain healthy
- Next exact step:
  - pass the focused/full test gates, merge through PR, rebuild the exact image, deploy both App Runner services without refreshing artifacts, then verify malformed auth returns `401` and valid protected live UI/API routes return `200`

### 2026-07-16 (item-level exported cent identity)
- Audit finding:
  - ROY immutable generation `20260715T224903Z` had `23` legacy 35% fallback rows where independently rounded `profit_before_ads` differed from exported revenue minus exported cost by `0.01 EUR`
  - the net item/product-profit overstatement was `0.03 EUR`; company profit was unaffected because company aggregation recomputes profit from summed revenue and costs
  - VEVO immutable generation `20260715T220511Z` had `0` mismatches across `13,094` item rows, but shared code was susceptible to the same half-cent edge case
- Fix:
  - every exported item profit now equals cent-rounded exported revenue minus cent-rounded exported cost
  - non-authoritative ROI intentionally stays on its established raw-value basis, so the correction does not create unrelated ROI churn
  - regression and QA smoke fixtures cover both `missing_cost_margin_35_fallback` and `margin_35_override`
- Local verification:
  - focused cent-identity and authoritative-margin tests passed
  - full suite: `178` tests passed
  - reporting QA smoke, Python compile, and `git diff --check` passed
- Known separate reporting issue:
  - standalone ROY 30d/90d payloads do not resolve all creditnoted orders from the full-history context, omitting `0.50/0.75 EUR` of fulfillment cost; the full payload is correct and this needs a separate fix
- Next exact step:
  - merge through PR, build the exact ECR image, regenerate the affected ROY history, verify zero row identity mismatches, and synchronize the production App Runner services/scheduled task definitions to the final image

### 2026-07-16 (period credit-note context parity)
- Root cause:
  - the full-history exporter retained excluded/Storno orders, but the child exporters used for the standalone `7d`, `30d`, and `90d` reports received only realized orders
  - this caused all period credit-note audits to report those orders as `order_not_found` and caused ROY to omit `0.50 EUR` of 30-day and `0.75 EUR` of 90-day retained fulfillment cost
- Safety action:
  - replacement VEVO rollout run `29458042558` was cancelled before localhost validation, S3 publication, scheduler promotion, or App Runner deployment
  - candidate task `c623dcb99cc443c1a858046546709aac` (`172.31.29.81`, task definition `vevo-reporting-daily:20`) stopped with the explicit reason `Cancelled before publish: period creditnote context P1 fix required`
  - production remained on task definition `:19`, S3 generation `20260715T220511Z`, and healthy digest `sha256:2ac61cc50ae86c9b11052c1a4b2cc9bd2d75c13f2a544c86b8a01ac3bccd7f12`
- Fix merged through PR `#232` on `codex/reporting-period-creditnote-context`:
  - each period child now receives deep-copied, range-filtered `excluded_status_orders` and `excluded_orders`
  - credit-note audits separately receive a shared read-only full-history lookup because a credit note created in the selected period can refer to an older order
  - carrier denominators remain explicitly period-scoped; the historical lookup cannot inflate lifecycle, CRM, fulfillment, or carrier-rate denominators
  - regression coverage verifies the `7d`/`30d`/`90d` slices, an in-period credit note for an older order, credit-note audit resolution, CRM/lifecycle boundaries, carrier denominator isolation, and retained fulfillment cost
- Verified so far:
  - focused period context regression passed
  - full suite: `179` tests passed
  - reporting QA smoke, Python compile, and `git diff --check` passed
  - independent re-review found no remaining P0/P1 issues and marked the change safe to merge
- Final status:
  - deployed in the final VEVO/ROY generations above; all period credit-note audits have `order_not_found=0` and fulfillment parity is exact

### 2026-07-16 (VEVO attributed CPA release gate and rollback)
- Release blocker:
  - immutable VEVO generation `20260716T001511Z` passed all product-accounting, credit-note, cent-identity, and period-parity audits but its 7-day payload had one critical data assertion
  - campaign `SK-Sale-VEVO-SandBox-ABO-ACQ` reported spend `0.28 EUR`, attributed orders `0.2`, and CPA `1.21 EUR`; the displayed arithmetic requires `1.40 EUR`
  - root cause: campaign CPA used the raw attributed-order estimate while the payload exposed the estimate rounded to one decimal place
- Safety action:
  - deploy run `29459459581` was cancelled as soon as the P1 was identified
  - because publication/promotion raced immediately ahead of cancellation, S3 `latest` was restored and byte-validated against healthy generation `20260715T220511Z`
  - scheduler `vevo-daily-report-email` was restored and verified `ENABLED`, `cron(0 1 * * ? *)`, `Europe/Bratislava`, task definition `vevo-reporting-daily:19`
  - App Runner rollback operation `0f1d7cef5e7144e9a22d86c8f82a1c9c` completed `SUCCEEDED`; service `biznisweb-vevo-production-board` is `RUNNING` on healthy digest `sha256:2ac61cc50ae86c9b11052c1a4b2cc9bd2d75c13f2a544c86b8a01ac3bccd7f12` and `/health` returns HTTP `200`
  - immutable generation `20260716T001511Z` remains only as audit evidence and is not the live `latest`
- Fix merged through PR `#233` on `codex/reporting-attributed-cpa-rounding`:
  - campaign attribution now serializes attributed orders to four decimals and uses that exact denominator consistently for CPA, estimated revenue, ROAS, QA, and campaign ranking
  - a production-shaped replay of the original campaign row now reports `0.2305` attributed orders and `1.21 EUR` CPA; the deterministic unit fixture reports `0.232` and `1.21 EUR`, and both displayed calculations reconcile without the one-decimal distortion
  - ultra-small estimates that round below `0.0001` are marked `insufficient_sample`, expose CPA as `null`/`N/A`, and raise an explicit QA warning instead of looking like free acquisition
  - modern and legacy dashboard tables use adaptive precision; charts preserve a missing CPA as `null` instead of converting it to zero
  - boundary regressions cover `0.249/0.251/0.500` ranking, the exact production row, and an ultra-small positive attribution
- Verification:
  - focused reporting/dashboard suite: `76` tests OK
  - full unit suite: `183` tests OK
  - reporting QA smoke, Python compile, both project settings JSON parses, and `git diff --check` passed
- Final status:
  - deployed in VEVO generation `20260716T010224Z`; 7-day QA is `ok`, all four periods have zero failures, and the production UI/API arithmetic reconciles

### 2026-08-20 (VEVO bundle purchase costs from real components)
- Change merged through PR `#276` as `7ee2a29358a0fd3d90374b66fe5212cfdde00566`:
  - bundle/set purchase costs are derived from the current mapped purchase costs of their individual bottles/components instead of stale copied totals
  - configured totals are: Essence Sample Set `3.12 EUR`, Natural Discovery `2.44 EUR`, Premium Discovery `1.98 EUR`, Complete Discovery `3.77 EUR`, Natural Bestsellers 3x200 ml `9.44 EUR`, Natural Complete Fragrance 6x200 ml `19.54 EUR`, and Ylang Absolute + Pure Garden 2x500 ml `12.47 EUR`
  - homogeneous Ylang Absolute 2x/3x500 ml bundles remain inferred from the single 500 ml bottle at `12.28/18.42 EUR`
  - Vevo Ylang Absolute floor cleaner 500 ml is mapped at the supplied real purchase cost `2.35 EUR` excl. VAT; 2x/3x bundles are therefore inferred at `4.70/7.05 EUR`
  - stale direct set totals and the erroneous `1.00 EUR` Bestsellers alias were removed so future bottle-cost corrections propagate automatically to the sets
- Verification:
  - VEVO settings and product-expense JSON parse passed
  - focused bundle-cost regressions: `5` tests passed
  - full suite: `291` tests passed
  - `python -m py_compile export_orders.py` and `git diff --check` passed
- Production build and deployment:
  - immutable build workflow `32369972475` succeeded with image `git-7ee2a29358a0fd3d90374b66fe5212cfdde00566` and digest `sha256:2639fecce2f8bd9cb561838b6fe2d24bdce0b1725c7605233022e761e40a232d`
  - protected deploy workflow `32370203470` succeeded and regenerated all `7d`, `30d`, `90d`, and `full` artifacts
  - Fargate hard-gate identity: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.32.133`, service `vevo-daily-report-email`, task `93b495feac2f4801a90ae4b4f0c7fdea`, candidate/promoted task definition `vevo-reporting-daily:28`, runtime `/app`, and marker `http://127.0.0.1:8000/marker.json`
  - read-only diagnostic workflow `32372552491` confirmed the task is stopped with container exit code `0`, `LOCALHOST_LIVE_DASHBOARD_OK:vevo:periods=7d,30d,90d,full`, and `LIVE_ARTIFACT_MARKER_OK`
  - immutable generation `20260820T130043Z` is live and scheduler `vevo-daily-report-email` is `ENABLED` on task definition `:28`
  - the protected workflow's authenticated App Runner HTML/API gates passed; a separate Chrome visual attempt was blocked locally by `ERR_BLOCKED_BY_CLIENT` for the App Runner origin, so a local-browser visual pass is not claimed
- Known input gap:
  - the real purchase cost of one `Vevo Pure Harmony 500ml` bottle is not present in the current cost map; its 2x/3x bundles remain intentionally unmapped rather than using an invented cost
- Next exact step:
  - provide the real ex-VAT purchase cost of one `Vevo Pure Harmony 500ml` bottle; then map it once so its 2x/3x bundles are inferred automatically

### 2026-08-20 (VEVO bundle gifts and two new full-size sets)
- External catalog change:
  - four full-size VEVO sets now have new exact product labels and include a real Vevo Shot plus one or two wooden 7 ml measuring cups
  - new products `Kompletný prací rituál – Ylang Absolute` and `Dve najobľúbenejšie vône + čistá práčka` were created in BiznisWeb
- Reporting change:
  - added six exact-label component-cost rules for the new catalog state while retaining the previous rules unchanged for historical orders
  - derived ex-VAT COGS: Bestsellers 3x200 + Shot + cup `10.40 EUR`; Complete 6x200 + Shot + two cups `20.81 EUR`; 3x Ylang 500 + Shot + two cups `19.69 EUR`; Ylang 500 + 3 gels + Shot + cup `14.39 EUR`; complete laundry ritual `9.53 EUR`; Ylang + Pure Garden + Shot + two cups `13.74 EUR`
  - regression coverage asserts every new exact label, total cost, and configured-rule source
- Verified locally:
  - full unit suite: `291` tests passed
  - reporting QA smoke, Python compile, both VEVO JSON parses, and `git diff --check` passed
  - no local server, worker, watcher, tunnel, or persistent runtime was started
- Merge, build, and production deployment:
  - implementation PR `#278` merged as `68acf04867e5bf15aaf6232a310c86b66d5a94da`
  - immutable build workflow `32380067814` succeeded with tag `git-68acf04867e5bf15aaf6232a310c86b66d5a94da` and digest `sha256:953df51e3c07bac20e985e22ca535d47c3706ff5bb76900401b50ac51ddcc20c`
  - protected deploy workflow `32380343280` succeeded and regenerated all `7d`, `30d`, `90d`, and `full` artifacts
  - Fargate hard-gate identity: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.32.86`, service `vevo-daily-report-email`, task `82b7fafb9888427a9e07bb72fbae9524`, candidate/promoted task definition `vevo-reporting-daily:29`, runtime `/app`, and marker `http://127.0.0.1:8000/marker.json`
  - direct read-only diagnostic workflow `32383281685` confirmed the task stopped normally with exit code `0`, emitted `LOCALHOST_LIVE_DASHBOARD_OK:vevo:periods=7d,30d,90d,full`, and emitted `LIVE_ARTIFACT_MARKER_OK`
  - immutable generation `20260820T145026Z` is live and scheduler `vevo-daily-report-email` is `ENABLED` on task definition `:29`
  - the protected App Runner HTML/API gates passed on exact image digest `sha256:953df51e3c07bac20e985e22ca535d47c3706ff5bb76900401b50ac51ddcc20c`
- Next exact step:
  - collect 14 days of orders under the new exact labels, then compare units, AOV, contribution profit, orders above the free-shipping threshold, and cannibalization against the immediately preceding matched-weekday window

### 2026-08-20 (VEVO Natural Complete upgraded to two Shots)
- External catalog change:
  - `Vevo Natural Complete 6×200 ml + 2× Vevo Shot a 2 odmerky ZADARMO` now contains two Vevo Shots and two wooden 7 ml measuring cups
  - customer-facing value is `121.30 EUR`, sale price remains `89.90 EUR`, displayed savings are `31.40 EUR / 25.9%`, and the gifts are presented at `19.80 EUR`
- Reporting change:
  - added a new exact-label component-cost rule for the two-Shot catalog state while retaining the one-Shot rule unchanged for historical orders
  - derived ex-VAT COGS is `21.46 EUR`: six 200 ml perfumes, two Vevo Shots, and two wooden measuring cups
- Verified locally:
  - focused exact-label/cost regression passed
  - full unit suite: `291` tests passed
  - VEVO settings JSON parse, reporting QA smoke, Python compile, and `git diff --check` passed
  - no local server, worker, watcher, tunnel, or persistent runtime was started
- Merge, build, and production deployment:
  - implementation PR `#280` merged as `3c59f4da470966b6a8e20649473b9b58e7293f50`
  - immutable build workflow `32388731178` succeeded with tag `git-3c59f4da470966b6a8e20649473b9b58e7293f50` and digest `sha256:04b5039afe84aeebda08b3a46036cb1d1ecbcdc93661757d0b7c77b1ccb47feb`
  - protected deploy workflow `32388991632` succeeded and regenerated all `7d`, `30d`, `90d`, and `full` artifacts
  - Fargate hard-gate identity: instance-id `N/A (scheduled ECS/Fargate task)`, private IP `172.31.27.142`, service `vevo-daily-report-email`, task `668a0e723cdb45bbbe45cb1d7f343b84`, candidate/promoted task definition `vevo-reporting-daily:30`, runtime `/app`, and marker `http://127.0.0.1:8000/marker.json`
  - the refresh emitted `LOCALHOST_LIVE_DASHBOARD_OK:vevo:periods=7d,30d,90d,full` and `LIVE_ARTIFACT_MARKER_OK`; immutable generation `20260820T161914Z` passed the live generation manifest gate
  - scheduler `vevo-daily-report-email` was promoted to task definition `:30`; authenticated App Runner production-board and VEVO accounting gates passed on the exact image digest, followed by `APP_RUNNER_DEPLOY_OK`
- Next exact step:
  - collect 14 days of orders under the new two-Shot exact label, then compare units, AOV, contribution profit, orders above the free-shipping threshold, and cannibalization against the immediately preceding matched-weekday window

# 2026-08-23 — VEVO GrowthBook Production zero-allocation UI preparation

- GrowthBook external state created and reloaded one object at a time:
  - separate Production JavaScript SDK connection `sdk_19g6lmt5wnngy`, version `1.7.0`, project `VEVO SK Web`, environment `production`, API host `https://cdn.growthbook.io`; the task-scoped client key was not recorded
  - Production A/A draft `exp_19g6mmt5wugpk` with tracking key `vevo-sk-aa-001`, assignment attribute `id`, `control`/`variant` at `50/50`, Production source `ds_19g6mmt5stlp6`, exact one goal/six secondary/one guardrail Production metrics, Bayesian statistics, and no activation metric
  - feature `vevo-sk-aa-assignment` live revision `2` remains Production-disabled and staging-enabled; draft revision `3` contains the new Production-only experiment rule but is unpublished
- Safety read-back:
  - Production experiment status is `Draft`; it was not started
  - Production SDK reports `Not connected`; no GTM loader points to it
  - Production live allocation remains `0%`; Preview/staging remains on live revision `2`; CTA stays draft/stopped
  - no GTM publish, Meta Ads mutation, BiznisWeb mutation, price/cart/checkout/order change, or paid upgrade occurred
- Implementation gate opened on `codex/vevo-growthbook-production-ui-gate`:
  - the committed storefront remains compile-time Production-disabled
  - the reproducible builder can produce a temporary Production artifact only with dev mode off and an API Gateway hostname matching the reviewed collector evidence hash
  - SDK key and collector URL remain task-scoped environment inputs and are never committed
- Next exact step:
  - merge the reviewed Production builder, generate the exact temporary artifact from that `main` commit, create four new unpublished Production GTM tags in workspace `16`, read back their IDs, delete the artifact, and record only the tag IDs and SHA-256

## 2026-08-25 — VEVO GrowthBook Starter-to-Pro fail-closed transition

Date: 2026-08-25
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-growthbook-pro-upgrade-gate`

What changed:

- Added the versioned `growthbook_pro_upgrade.json` state machine, offline hash-bound recorder/validator, and `GROWTHBOOK_PRO_UPGRADE_RUNBOOK.md`.
- The paid review can open only after independently verified A/A `PASS` plus the reviewed zero-allocation stop; a fresh action-time confirmation must bind exactly one Pro seat at a `$40 USD` monthly recurring base price.
- The verified transition requires six unique Preview/Production metric IDs for LCP, INP, and CLS p75, exact contract hashes, successful configuration read-back/query tests, and a canonical sanitized observation without payment, identity, customer, or order data.
- CTA baseline, activation, running checkpoint, completion, workspace validation, and security CI now require the verified Pro/p75 lifecycle and exact four guardrails: client-error rate plus LCP/INP/CLS p75.
- CTA remains draft at `0%`; automatic GrowthBook, GTM, Meta Ads, BiznisWeb, collector/reporting, price, product, stock, cart, checkout, payment, and order mutations remain closed.

What is verified:

- Full Python suite: `751` tests passed.
- GrowthBook storefront JavaScript suite: `9` tests passed.
- Focused Pro/CTA/workspace/workflow tests passed, including future verified-workspace observation validation.
- `validate_growthbook_pro_upgrade.py`, `validate_growthbook_workspace.py`, `security_ci.py`, Ruff, Python compilation, JSON/YAML parsing, and `git diff --check` passed.
- No live GrowthBook purchase, subscription, metric, experiment, GTM, Meta Ads, BiznisWeb, AWS, commerce, price, product, stock, cart, checkout, payment, or order mutation occurred.
- No local server, worker, watcher, tunnel, Docker stack, or persistent runtime was started.

Known issues:

- Production A/A is still inside its pre-registered blind measurement window; Pro authorization and metric creation remain intentionally unavailable now.
- The first permitted population checkpoint remains `2026-09-02 03:45 Europe/Bratislava` after the natural daily reconciliation.

Next exact step:

- Verify the first natural Production reconciliation result-blind after `2026-08-26 03:45 Europe/Bratislava`; do not inspect population, arms, outcomes, Meta dimensions, or performance before the pre-registered `2026-09-02 03:45` checkpoint.
- After an independently verified A/A `PASS` and zero-allocation stop read-back, obtain fresh action-time confirmation, open the reviewed one-seat `$40/month` Pro gate through a separate PR, then perform and record the manual Pro/p75 read-back before collecting the CTA baseline.

## 2026-08-25 — VEVO monitoring split between cloud execution and local readback

Date: 2026-08-25
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-monitoring-heartbeat-readback`

What changed:

- Kept `.github/workflows/monitor-vevo-growthbook-production-aa-infra.yml` as the active PC-independent overnight monitor at `04:15 Europe/Bratislava` after the `03:45` reconciliation.
- Moved the local Codex heartbeat `vevo-production-a-a-monitoring` to `09:00 Europe/Bratislava`, when the PC and desktop app are more likely to be available, and preserved it as a coordinator/readback only.
- Updated the heartbeat prompt to require `GROWTHBOOK_PRO_UPGRADE_RUNBOOK.md`, a fresh one-seat `$40/month` action-time confirmation, six verified Preview/Production p75 metrics, and workspace state `production_aa_completed_cta_sample_freeze_pro_quantiles_verified` before the CTA baseline.

What is verified:

- GitHub workflow `Monitor VEVO GrowthBook Production A/A Infrastructure` is active on `main`; its two UTC cron slots retain the pre-credential DST gate so exactly the `04:15 Europe/Bratislava` slot performs the result-blind AWS readback.
- Automation readback is `ACTIVE`, targets this thread, is scheduled daily at `09:00`, and contains the Pro runbook, verified-Pro state, and exact `$40 USD` monthly marker.
- The first permitted population checkpoint remains `2026-09-02 03:45 Europe/Bratislava`; neither automation may read population, arms, outcomes, Meta dimensions, or performance before that gate.
- No GrowthBook, GTM, Meta Ads, BiznisWeb, AWS, commerce, price, product, stock, cart, checkout, payment, or order mutation occurred.

Next exact step:

- Let the GitHub monitor execute after the first natural `2026-08-26 03:45 Europe/Bratislava` reconciliation, then use the `09:00` heartbeat only to verify the successful exact-main run and its single canonical result-blind artifact.

## 2026-08-25 — VEVO Meta Ads → GrowthBook/reporting CTA release gate

Date: 2026-08-25
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-growthbook-meta-reporting-gate`

What changed:

- Added `growthbook_meta_reporting_contract.json` and an offline validator that bind the canonical Meta URL parameters to the exact stable campaign/ad-set/ad/placement fields in the allowlisted collector, anonymous reporting facts, query-tested Production GrowthBook assignment SQL, and CTA activation gate.
- The contract preserves a single canonical `https://www.vevo.sk` destination for the first CTA test. GrowthBook owns the on-site `50/50` split; Meta A/B split, arm-specific destinations, and arm/variation URL parameters are fail-closed.
- Meta dimension slices are explicitly diagnostic only. They cannot declare a winner or replace the pre-registered all-eligible-traffic decision.
- `growthbook_cta_activation.json`, its recorder, runtime release validator, workspace validator, and security CI now hash-bind and verify the Meta/reporting contract before manual CTA start review can open.

What is verified:

- The existing Production clone observation is SHA-bound and proves data source `ds_19g6mmt5stlp6` used the exact query-tested assignment SQL hash that exposes all four stable Meta dimensions.
- Full Python suite: `757` tests passed. GrowthBook storefront JavaScript suite: `9` tests passed. Focused Ruff, Python compilation, JSON parsing, workspace/security validation, and `git diff --check` passed; the dedicated validator reports the complete offline chain valid.
- No A/A population, arm, outcome, Meta dimension, conversion, revenue, CM1, or performance result was read. No Meta Ads, GrowthBook, GTM, BiznisWeb, AWS, traffic, price, product, stock, cart, checkout, payment, or order mutation occurred.
- No local server, worker, watcher, tunnel, Docker stack, or persistent runtime was started.

Known issues:

- The Production A/A remains inside its pre-registered blind window. This release gate prepares the later CTA handoff but does not authorize a live ad edit, traffic reroute, CTA start, or result read.

Next exact step:

- Verify the first natural Production reconciliation result-blind after `2026-08-26 03:45 Europe/Bratislava`; before `2026-09-02 03:45`, continue to avoid population, arm, outcome, Meta dimension, conversion, revenue, CM1, and performance reads.

## 2026-08-25 — VEVO durable GrowthBook hypothesis/decision registry

Date: 2026-08-25
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-growthbook-hypothesis-registry`

What changed:

- Added the PII-free `growthbook_hypothesis_registry.json` as the durable Git audit source of truth for the exact first CTA hypothesis, GrowthBook experiment/feature IDs, allowed change, population, `50/50` variations, primary metric, CM1 guardrail, and diagnostic-only Meta dimensions; GrowthBook remains the analytical UI.
- The final CTA recorder now pre-validates and writes both the closed final-snapshot manifest and the registry decision. It retains the complete aggregate decision, exact sample/effect/interval/guardrail evidence and provenance, verifies the decision artifact hash, and SHA-binds the canonical registry into the final snapshot.
- Workspace validation, the standalone final-snapshot validator, security CI, plan, Pro workspace contract, activation runbook, and recorder tests now fail closed on a missing, premature, identity-bearing, automatically mutating, tampered, or hash-mismatched registry decision.

What is verified:

- Full Python suite: `760` tests passed.
- GrowthBook storefront JavaScript suite: `9` tests passed.
- Dedicated hypothesis/final-snapshot tests, standalone hypothesis/final-snapshot/workspace validators, security CI, focused Ruff, Python compilation, JSON parsing, and `git diff --check` passed.
- No A/A population, arm, outcome, Meta dimension, conversion, revenue, CM1, performance, or experiment result was read. No GrowthBook, GTM, Meta Ads, BiznisWeb, AWS, traffic, price, product, stock, cart, checkout, payment, or order mutation occurred.
- No local server, worker, watcher, tunnel, Docker stack, or persistent runtime was started.

Known issues:

- The registry is intentionally preregistered with `final_decision=null`; the Production A/A remains inside its result-blind pre-registered window and the CTA experiment remains unstarted.

Next exact step:

- Verify the first natural Production reconciliation result-blind after `2026-08-26 03:45 Europe/Bratislava`; before `2026-09-02 03:45`, do not inspect population, arms, outcomes, Meta dimensions, conversions, revenue, CM1, performance, or any result.

## 2026-08-25 — VEVO exact-main result-blind infrastructure preflight

Date: 2026-08-25
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-infra-preflight-c989`

What is verified:

- Read-only workflow run `32864004703` succeeded on exact merged `main` commit `c989488f579999aff5442e35e23994eb8c6e74ec` before the first natural reconciliation was due.
- Its one canonical sanitized JSON has SHA-256 `c8d05bdc39412abfacc010654bef1b4a225f3bb0bb2d23cd2ae7248f53d0bb3` and independently passes `validate_growthbook_aa_infra_health_evidence.py` against the checked-in Production reconciliation deploy evidence.
- The recorded phase is exactly `waiting_for_first_natural_run`: reconciliation schedule `ENABLED`, all three alarms clear, DLQ empty, and no natural task/marker claim made before the due boundary.
- Population, arm, outcome, Meta-dimension, and performance reads are all `false`; AWS resource mutation and every GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, and commerce mutation boundary remain `false`.
- The downloaded local verification copy and its exact temporary directory were removed after validation. No local server, worker, watcher, tunnel, Docker stack, or persistent runtime was started.

Next exact step:

- Let the PC-independent GitHub monitor run at `2026-08-26 04:15 Europe/Bratislava`, after the first natural `03:45` reconciliation, and validate only its exact-main canonical result-blind artifact; do not read any experimental population or result before `2026-09-02 03:45`.

## 2026-08-26 — VEVO first natural Production reconciliation verified result-blind

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-first-natural-evidence`

What changed:

- The first scheduled monitor reached the post-due gate after GitHub delay, but the short-lived ECS stopped-task listing had already expired. PRs `#423`, `#424`, and `#425` made the monitor retention-safe without weakening the no-result boundary.
- The monitor now discovers the exact task from the bounded CloudWatch success marker plus the Scheduler-authenticated CloudTrail `RunTask` event, prefers retained ECS state, and records an explicit schema-v2 retention source instead of inventing a private IP.
- A `null` private IP under `cloudtrail_run_task_retention_recovery` is valid only for read-only reconciliation monitoring. It does not satisfy the separate live IP plus localhost-marker hard gate required before any infrastructure mutation.

What is verified:

- PR `#423` merged as `5bf0549a7412685dcf06bf89ac8ca30bff162fa1`, PR `#424` as `514cbcb5742b8a5ca8c3aa076f6ced80cf12dda8`, and PR `#425` as `3876798c1c581ea0dab8f2dde14c92baab2540f7`; every required PR check passed.
- Exact-main read-only workflow run `32932181925` succeeded on `3876798c1c581ea0dab8f2dde14c92baab2540f7` and produced exactly one canonical sanitized artifact.
- The artifact SHA-256 is `d1166ce95dd6369b882d0d63eedf4b85ad9503de6513665dd222a13ac5be4104`; independent local validation with `validate_growthbook_aa_infra_health_evidence.py` passed, and the temporary download was deleted.
- Its phase is `natural_reconciliation_verified` for `2026-08-26T03:45:00+02:00`: schedule `ENABLED`, Scheduler `RunTask` verified, success marker and generated/published parity verified, all three alarms clear, and retained DLQ empty.
- ECS stopped-task state had expired, so `runtime_state_retained=false`, `identity_source=cloudtrail_run_task_retention_recovery`, and `private_ip=null` are recorded explicitly. No live-IP hard-gate claim is made.
- The retention changes passed `764` Python tests, `9` storefront JavaScript tests, security CI, Ruff, Python compilation, YAML parsing, and `git diff --check`.
- Population, arm, outcome, Meta-dimension, conversion, revenue, CM1, performance, and reporting row-count reads are all `false`; no AWS resource, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, product, stock, cart, checkout, payment, or order mutation occurred.
- No local server, worker, watcher, tunnel, Docker stack, or persistent runtime was started.

Next exact step:

- Continue daily result-blind infrastructure monitoring, but do not inspect population, arms, outcomes, Meta dimensions, conversions, revenue, CM1, performance, or any experiment result before the pre-registered first checkpoint at `2026-09-02 03:45 Europe/Bratislava`.

## 2026-08-26 — VEVO outcome-blind checkpoint made ECS-retention-safe

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-checkpoint-retention`

What changed:

- Pre-checkpoint review found that `check-vevo-growthbook-production-aa-window.yml` still depended on the short-lived ECS `STOPPED` task listing and would likely fail when the 09:00 coordinator ran after the 03:45 reconciliation.
- The read-only checkpoint now selects the exact reconciliation through a bounded CloudWatch success marker plus the Scheduler-authenticated CloudTrail `RunTask` event, then prefers retained ECS state when available.
- Canonical checkpoint evidence schema `2` records `identity_source`, `scheduler_run_task_verified`, and `runtime_state_retained`. If ECS state has expired, the artifact records `cloudtrail_run_task_retention_recovery`, `runtime_state_retained=false`, and `private_ip=null` instead of inventing or treating a historical IP as live.
- The null-IP retention fallback is valid only for this read-only outcome-blind checkpoint. It never satisfies the live private-IP plus localhost-marker hard gate required before infrastructure mutation.
- The offline validator and recorder retain legacy schema-`1` compatibility while enforcing the exact schema-`2` source/IP/retention relationships.

What is verified:

- `769` Python tests passed, including behavior tests for both retained and expired ECS task state; `9` storefront JavaScript tests passed.
- Focused Ruff checks, Python compilation, YAML parsing, workspace/measurement-window/security validators, and `git diff --check` passed.
- The workflow still permits only one cumulative eligible-device aggregate query, never reads arms or outcomes, uploads one canonical sanitized artifact, and deletes raw AWS/log/query files before upload.
- No checkpoint workflow was dispatched early. No population, eligible count, arm, split, SRM, conversion, revenue, CM1, Meta dimension, performance, or other experiment result was read.
- No AWS resource, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, product, stock, cart, checkout, payment, or order mutation occurred. No local runtime process was started.

Next exact step:

- Merge the retention-safe checkpoint workflow after CI, continue daily result-blind monitoring, and dispatch the outcome-blind checkpoint only at or after `2026-09-02 03:45 Europe/Bratislava` from exact `main` with `confirm_checkpoint=true`.

## 2026-08-26 — VEVO CTA outcome-blind checkpoint made ECS-retention-safe

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-checkpoint-retention`

What changed:

- Pre-launch review found the future CTA assignment checkpoint had the same short-lived ECS `STOPPED` task dependency as the earlier A/A checkpoint and could fail hours after its `03:45` reconciliation.
- `check-vevo-growthbook-production-cta-window.yml` now binds the exact reconciliation through a bounded CloudWatch success marker plus Scheduler-authenticated CloudTrail `RunTask`, validates the immutable task definition/image, and prefers retained ECS state when it exists.
- CTA checkpoint evidence schema `2` records `identity_source`, `scheduler_run_task_verified`, and `runtime_state_retained`. Expired ECS state produces the explicit identity-free combination `cloudtrail_run_task_retention_recovery`, `runtime_state_retained=false`, and `private_ip=null`.
- The historical null-IP fallback remains valid only for the read-only outcome-blind CTA checkpoint and never satisfies the live-IP plus localhost-marker gate required before infrastructure mutation.
- The offline validator and recorder remain compatible with legacy schema `1` while enforcing exact v2 source/IP/retention relationships and the `172.31.0.0/16` boundary for retained private IPs.

What is verified:

- `774` Python tests passed, including CTA behavior tests for both retained and expired ECS task state; `9` storefront JavaScript tests passed.
- Focused Ruff checks, Python compilation, YAML parsing, CTA/workspace/security validators, and `git diff --check` passed.
- The CTA workflow remains main-only, explicit-confirmation-only, and fail-closed before AWS until a verified CTA start. It still permits only one cumulative eligible-device aggregate, never reads arms/outcomes, never stops assignment automatically, and uploads one sanitized artifact after raw-file cleanup.
- No workflow was dispatched, no A/A or CTA population/result was read, and no AWS resource, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, product, stock, cart, checkout, payment, or order mutation occurred. No local runtime process was started.

Next exact step:

- Merge the CTA retention-safe preparation after CI; keep the CTA lifecycle closed, continue daily result-blind A/A monitoring, and do not dispatch the A/A checkpoint before `2026-09-02 03:45 Europe/Bratislava`.

## 2026-08-26 — VEVO protected CTA final look made ECS-retention-safe

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-final-retention`

What changed:

- The future one-look CTA final-snapshot workflow no longer enumerates short-lived ECS `STOPPED` history to find its latest post-due Production reconciliation.
- It now selects the latest exact success from a bounded CloudWatch window, binds the task ID to one Scheduler-authenticated CloudTrail `RunTask`, verifies the exact cluster/group/task definition and immutable image, and rejects any failure marker at or after the selected success.
- Retained ECS state still supplies and validates the historical task IP when available. After expiry the pre-query context records `cloudtrail_run_task_retention_recovery`, `runtime-retained=false`, and `private-ip=expired` without claiming a live hard gate.
- The separate newly launched diagnostic Fargate task remains mandatory: only its exact `172.31.0.0/16` private IP plus direct localhost health and `/app` runtime markers can open the single aggregate Athena outcome query.
- Security CI, behavioral tests, the activation runbook, the GrowthBook plan, and the workspace handoff now enforce and document this separation.

What is verified:

- PR `#429` merged as `796e2c1897bf9c071f527a8b4e5e874f33c6a388`; `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` all passed.
- `776` Python tests passed, including retained and expired ECS-state execution of the exact inline runtime-selection block; `9` storefront JavaScript tests passed.
- Focused CTA final builder/recorder/workflow tests, GrowthBook completion/measurement/final/workspace validators, security CI, Ruff, Python compilation, YAML parsing, and `git diff --check` passed.
- The workflow still has exactly one diagnostic `aws ecs run-task`, exactly one aggregate Athena start, and one artifact bundle containing only the canonical identity-free snapshot and offline decision. It has no stopped-task listing, deploy path, automatic winner application, or GrowthBook/GTM/Meta Ads/BiznisWeb/commerce mutation path.
- No workflow was dispatched, no AWS task or query ran, and no A/A/CTA population, arm, SRM, conversion, revenue, CM1, Meta-dimension, performance, or result was read. No production state changed and no local runtime process was started.

Next exact step:

- Continue cloud-based daily result-blind infrastructure monitoring. Do not inspect population, arms, outcomes, Meta dimensions, conversions, revenue, CM1, performance, or any experiment result before the pre-registered first A/A checkpoint at `2026-09-02 03:45 Europe/Bratislava`; dispatch the protected outcome-blind checkpoint only at or after that gate from exact `main` with `confirm_checkpoint=true`.

## 2026-08-26 — VEVO current-main Production A/A infrastructure reverified result-blind

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-exact-main-health`

What changed:

- No production configuration changed. After the retention-safe checkpoint and final-look PRs landed, one new manual result-blind infrastructure monitor was dispatched from their exact current `main` solely to close the commit-provenance gap.
- The repository-owned GitHub schedule remains the PC-independent daily monitor, while the existing Codex heartbeat `vevo-production-a-a-monitoring` was read back as `ACTIVE`, attached to this thread, and scheduled daily at `09:00 Europe/Bratislava` as coordinator/readback only.

What is verified:

- Workflow run `32935473209` succeeded on exact main commit `83eef030bb5db080fe41f01b3806ae7714020708` and produced exactly one artifact named `vevo-growthbook-production-aa-infra-health`.
- The GitHub artifact ZIP SHA-256 is `5ed000ea47ac238c100b24614e2dd4e84fa6c746d1927d6543bb7e6dbea9fa03`; it contains only `vevo-growthbook-production-aa-infra-health.json`, whose SHA-256 is `d85e71f351ee4e442e68d2f8369b966a916d6db200cde12eebb1589a5c2db27d`.
- Independent validation with `validate_growthbook_aa_infra_health_evidence.py` passed against the checked-in Production reconciliation deploy evidence.
- Canonical schema `2` records `natural_reconciliation_verified` for `2026-08-26T03:45:00+02:00`: schedule `ENABLED`, Scheduler `RunTask` verified, exact task definition and immutable image, generated/published parity verified without row counts, all three alarms `OK`, DLQ empty, and source schedule unchanged.
- ECS stopped-task state had expired, so the artifact correctly records `identity_source=cloudtrail_run_task_retention_recovery`, `runtime_state_retained=false`, and `private_ip=null`; it makes no live-IP hard-gate claim.
- Experimental population, arm assignment, outcomes, Meta dimensions, performance values, reporting row counts, raw AWS payloads, CloudWatch messages, credentials, and event/device/customer/order identities are all absent or explicitly `false`. Every AWS-resource, GrowthBook, GTM, Meta Ads, BiznisWeb, collector/reporting, commerce, and workflow/experiment-gate mutation flag is `false`.
- The artifact download directory was independently path-checked and deleted after validation. No local AWS credentials, local server, worker, watcher, tunnel, Docker stack, or persistent runtime was used.

Next exact step:

- Let the cloud monitor continue daily. Before `2026-09-02 03:45 Europe/Bratislava`, inspect only exact-main result-blind infrastructure evidence. At or after that frozen gate, dispatch the protected outcome-blind checkpoint once for its exact due boundary with `confirm_checkpoint=true`; use only its cumulative eligible-device count, never arms or outcomes, and record the canonical artifact through the offline recorder and reviewed PR.

## 2026-08-26 — VEVO A/A checkpoint capture made PC-independent

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-cloud-checkpoints`

What changed:

- The protected A/A checkpoint workflow now has two GitHub UTC schedules for the DST alternatives of `04:30 Europe/Bratislava`, after the frozen `03:45` reconciliation.
- A pre-AWS gate admits only the correct local-time slot. It skips before credentials and before the population query when the run is pre-due, on the wrong DST slot, after resolution, or for a checkpoint already recorded on `main`.
- Scheduled checkpoint identity is derived from the frozen local calendar date rather than current Git history. This preserves exact daily checkpoint artifacts while the desktop PC is off or earlier artifacts are still waiting for offline recording.
- Manual `confirm_checkpoint=true` remains limited to the next missing checkpoint and its original 24-hour due gate. Every credential, AWS, aggregate-query, evidence, cleanup, upload, and summary step is conditioned on `RUN_CHECKPOINT=true`.
- Checkpoint artifacts are retained for 90 days. After multi-day downtime they must be recorded sequentially; the earliest artifact reaching `1,000` resolves the window and later captures are ignored.
- Monitoring, activation, plan, and workspace runbooks now document the cloud capture and unchanged outcome-blind stopping rule.

What is verified:

- PR `#432` merged as `4abda7d4fc593bb4134c07552449283ceb740acc`; `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` all passed.
- GitHub API readback reports workflow ID `341899955`, path `.github/workflows/check-vevo-growthbook-production-aa-window.yml`, and state `active` on exact synchronized `main`.
- The versioned host boundary remains instance `N/A:Fargate`, private IP `172.31.39.76`, service `vevo-growthbook-reconcile-production`, runtime path `/app`; this change does not deploy or mutate that service.
- `782` Python tests and all `9` storefront JavaScript tests passed, including pre-due, wrong-DST, already-recorded, resolved-window, summer-slot, winter-slot, multi-day calendar-index, manual-gate, and post-gate step-condition behavior.
- GrowthBook workspace and A/A measurement-window validators, central security CI, focused Ruff, Python compilation, YAML parsing, and `git diff --check` passed.
- The workflow still contains exactly one aggregate eligible-device query with no arm or outcome columns, one canonical identity-free artifact, no automatic stop/winner path, and no AWS-resource, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, or commerce mutation path.
- No checkpoint was dispatched, no AWS query or deploy ran, no A/A population or result was read, and no browser/UI action or local server, worker, watcher, tunnel, Docker stack, or persistent runtime was started.

Known issues:

- None introduced. GitHub scheduled execution remains subject to GitHub availability; the explicit manual same-window fallback remains available.

Next exact step:

- Before `2026-09-02 03:45 Europe/Bratislava`, continue only result-blind infrastructure monitoring. Let the first admitted scheduled checkpoint capture at `2026-09-02 04:30 Europe/Bratislava`, then independently download/hash its one canonical artifact and record it through a separate reviewed PR; do not read arms, outcomes, Meta dimensions, conversion, revenue, CM1, or performance before the window resolves.

## 2026-08-26 — VEVO Codex heartbeat aligned with cloud checkpoint capture

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-heartbeat-cloud-handoff`

What changed:

- Existing heartbeat automation `vevo-production-a-a-monitoring` was updated in place; no duplicate automation was created.
- Its result-blind pre-checkpoint boundary now permits the repository-owned scheduled workflow to execute only its pre-AWS skip gate before `2026-09-02 03:45 Europe/Bratislava`, while continuing to forbid a manual checkpoint dispatch and every population/result read.
- At a due checkpoint the heartbeat must first inspect and wait for the correct `04:30 Europe/Bratislava` GitHub scheduled run and may not dispatch a duplicate. Manual `confirm_checkpoint=true` is only a fallback when no relevant scheduled artifact exists and the original 24-hour daily gate is still open.
- If the local PC was offline across several checkpoints, the heartbeat must process retained artifacts in ascending checkpoint-index order, resolve at the earliest artifact with at least `1,000` eligible devices, and ignore all later captures after that boundary.

What is verified:

- Automation readback reports ID `vevo-production-a-a-monitoring`, kind `heartbeat`, status `ACTIVE`, the unchanged daily `09:00` schedule, and this goal thread as its target.
- The saved prompt contains the exact no-duplicate scheduled-artifact preference, pre-AWS skip boundary, manual same-window fallback, 90-day sequential catch-up, first-qualifying-checkpoint rule, and all existing GrowthBook Pro/CTA/external-mutation hard gates.
- Official OpenAI Scheduled tasks documentation confirms that local-project scheduled tasks require the computer and app to remain running; the GitHub schedule is therefore the durable checkpoint producer and the Codex heartbeat is only its later coordinator/readback.
- No GitHub workflow, AWS query/deploy, browser/UI action, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, commerce, or experiment gate was mutated. No population, arm, outcome, Meta-dimension, conversion, revenue, CM1, performance, or result was read.

Known issues:

- The local Codex heartbeat cannot run while the PC/app is unavailable; this no longer risks losing the checkpoint because GitHub captures and retains the canonical artifact independently for 90 days.

Next exact step:

- Continue daily result-blind infrastructure monitoring. Before `2026-09-02 03:45 Europe/Bratislava`, accept only infrastructure-health evidence and scheduled checkpoint skip behavior. At the first due boundary, consume the earliest successful exact-main scheduled checkpoint artifact without dispatching a duplicate, then record it through the offline recorder and reviewed PR.

## 2026-08-26 — VEVO future CTA checkpoint capture made PC-independent

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-cloud-checkpoints`

What changed:

- The future protected CTA assignment checkpoint workflow now schedules both UTC alternatives of `04:30 Europe/Bratislava`, after the frozen daily `03:45` reconciliation boundary.
- Its pre-AWS gate admits only the correct DST slot and skips before credentials when the CTA window is closed, before the first due date, on the wrong DST slot, after the day-42 maximum, or for an index already recorded on `main`.
- Scheduled checkpoint identity is derived from the frozen local calendar date instead of Git history, preserving exact daily artifacts while the desktop PC is off or earlier artifacts still await offline recording.
- Manual `confirm_checkpoint=true` remains an in-window fallback for the next missing index. Every credential, AWS, aggregate-query, evidence, cleanup, upload, and summary step is conditioned on `RUN_CHECKPOINT=true`.
- The single canonical identity-free checkpoint artifact is retained for 90 days. CTA activation, assignment stop, winner evaluation, GrowthBook Pro purchase, Meta Ads changes, and all external or commerce mutations remain separate reviewed gates.
- The activation runbook, GrowthBook plan/workspace documentation, central security markers, and behavior tests now describe and enforce the same cloud-capture contract.

What is verified:

- The checked-in CTA window remains `waiting_for_verified_cta_start` with `read_only_checkpoint_allowed=false`; scheduled runs therefore stop before AWS and cannot query population now.
- The versioned host boundary remains instance `N/A:Fargate`, private IP `172.31.39.76`, service `vevo-growthbook-reconcile-production`, runtime path `/app`; this repository-only change does not deploy or mutate that service.
- `788` Python tests and all `9` storefront JavaScript tests passed. Focused CTA checkpoint tests cover closed, pre-due, wrong-DST, resolved, already-recorded, calendar-derived catch-up, winter-slot, post-day-42, manual-index, daily-gate, and post-gate step conditions.
- CTA measurement-window/workspace/Pro/completion validators, central security CI, focused Ruff, Python compilation, YAML parsing, and `git diff --check` passed.
- No workflow was dispatched, no AWS query or deploy ran, no A/A or CTA population, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read. No browser/UI action or local server, worker, watcher, tunnel, Docker stack, or persistent runtime was started.

Known issues:

- None introduced. GitHub scheduled execution remains subject to GitHub availability; the explicit same-window manual fallback remains available after a verified CTA start.

Next exact step:

- Merge this future CTA cloud-capture preparation after CI, while keeping the CTA lifecycle closed. Before `2026-09-02 03:45 Europe/Bratislava`, continue only result-blind A/A infrastructure monitoring; at the first A/A due boundary, consume the earliest successful exact-main scheduled artifact without dispatching a duplicate and record it through the offline recorder and reviewed PR.

## 2026-08-26 — VEVO CTA cloud checkpoint merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-cloud-checkpoints-state`

What changed:

- PR `#435` merged the PC-independent future CTA checkpoint preparation into `main` as `7e6828a641251e8cf091087d0138a2f834ad4b90`.
- This handoff replaces the completed merge instruction with the next operational A/A gate; it introduces no workflow, runtime, experiment, reporting, advertising, or commerce change.

What is verified:

- PR `#435` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- Local `main` and `origin/main` both read back the exact merge commit `7e6828a641251e8cf091087d0138a2f834ad4b90`.
- GitHub API reports workflow ID `342017535`, path `.github/workflows/check-vevo-growthbook-production-cta-window.yml`, and state `active` on synchronized `main`.
- The checked-in CTA manifest remains `waiting_for_verified_cta_start`, with `release_boundaries.read_only_checkpoint_allowed=false`; the cloud schedule therefore exits before AWS and before any population query until a separately verified CTA start.
- No workflow was dispatched, no A/A or CTA population/result was read, and no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, pricing, cart, checkout, payment, or order state changed. No local runtime process was started.

Known issues:

- None introduced. CTA cloud capture is installed but intentionally dormant until all preceding Pro, baseline, sample-freeze, activation, and start-observation gates pass.

Next exact step:

- Continue daily result-blind A/A infrastructure monitoring. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect population, arms, outcomes, Meta dimensions, conversion, revenue, CM1, performance, or results. At the first due boundary, use the earliest successful exact-main scheduled A/A checkpoint artifact without dispatching a duplicate, independently verify and record it through a reviewed PR, and resolve only at the first artifact with at least `1,000` eligible devices.

## 2026-08-26 — VEVO current-main A/A infrastructure readback remains healthy

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-infra-current-main-20260826`

What changed:

- Dispatched one explicit result-blind read-only infrastructure monitor from exact synchronized `main` commit `dc4d02d3387528cb74c3b2a804fbb60806eb70df` after the A/A and future CTA cloud-checkpoint merges.
- GitHub run `32939220338` produced the sole canonical artifact `vevo-growthbook-production-aa-infra-health` (artifact ID `9595930112`), retained until `2026-09-09T06:41:42Z`.
- This entry records external readback only; no code, workflow, runtime, experiment, reporting, advertising, or commerce configuration changed.

What is verified:

- The workflow completed successfully and bound run `32939220338` to exact `main` commit `dc4d02d3387528cb74c3b2a804fbb60806eb70df`.
- The independently downloaded canonical artifact passed `validate_growthbook_aa_infra_health_evidence.py` and has SHA-256 `773b9a7da51e1ff07b52f56a68713aaff046a9d5414738daacfbf0453486336b`.
- AWS account `919341186960`, region `eu-central-1`, collector/reconciliation stacks, enabled `03:45 Europe/Bratislava` schedule, Scheduler-authenticated `RunTask`, immutable task definition `vevo-growthbook-reconcile-production:3`, image digest, service `vevo-growthbook-reconcile-production`, and runtime path `/app` all match the protected evidence.
- All three reconciliation alarms are `OK`, the DLQ is empty, the natural `2026-08-26 03:45 Europe/Bratislava` success marker is verified, generated/published parity is verified, and source schedule `vevo-daily-report-email` remains enabled on `vevo-reporting-daily:33`.
- The artifact explicitly proves no experimental population, arm assignment, outcomes, Meta dimensions, performance values, reporting row counts, identifiers, credentials, raw AWS payloads, or CloudWatch messages were read or retained, and every mutation boundary is `false`.
- The downloaded sanitised file and its dedicated temporary directory were removed after validation. No local server, worker, watcher, tunnel, Docker stack, or persistent runtime was started.

Known issues:

- ECS stopped-task state had expired before this later readback, so schema `2` correctly records `cloudtrail_run_task_retention_recovery`, `runtime_state_retained=false`, and `private_ip=null`. This is valid only for result-blind historical monitoring; any infrastructure mutation still requires a fresh live private IP plus direct localhost marker hard gate.

Next exact step:

- Continue daily result-blind infrastructure monitoring without duplicate manual runs. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect population, arms, outcomes, Meta dimensions, conversion, revenue, CM1, performance, or results. At the first due boundary, prefer the earliest successful exact-main `04:30 Europe/Bratislava` scheduled A/A checkpoint artifact, independently validate and record it through a reviewed PR, and resolve only at the first artifact with at least `1,000` eligible devices.

## 2026-08-26 — VEVO gate-critical handoff artifacts made 90-day retention-safe

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-gate-artifact-retention`

What changed:

- Extended only five one-time, hash-bound inter-gate artifacts from 14 to 90 days: automated A/A evidence, reviewed manual A/A QA evidence, assembled A/A snapshot/decision, aggregate CTA planning baseline, and CTA-only runtime-readiness evidence.
- Updated the machine-readable A/A snapshot and CTA baseline contracts, the offline baseline validator, workflow/security tests, central security CI, and the A/A/CTA runbooks to enforce the same 90-day contract.
- Repeated daily infrastructure-health evidence remains 14 days, existing recorded deployment evidence remains in Git, and one-time encrypted credentials remain one day; no unnecessary sensitive or routine artifact retention was expanded.

What is verified:

- GitHub artifact metadata in this repository proves the effective repository policy supports 90 days: artifact `9596061718` was created at `2026-08-26T06:47:09Z` and expires at `2026-11-24T06:46:37Z`; multiple other current artifacts have the same effective duration. The direct settings endpoint returned `403` for the current token, so no unsupported settings-level claim is made.
- `788` Python tests and all `9` storefront JavaScript tests passed. The focused retention/lifecycle suite passed `55` tests.
- A/A measurement-window, CTA baseline, workspace, Pro, completion, and central security validators passed; Ruff, Python compilation, five-workflow YAML parsing, and `git diff --check` passed.
- Every affected workflow remains main-only and preserves its existing pre-credential lifecycle gates, identity-free canonical output, raw-response cleanup, no-winner boundary, and external/commerce mutation exclusions.
- The versioned Production host boundary remains instance `N/A:Fargate`, private IP `172.31.39.76`, service `vevo-growthbook-reconcile-production`, runtime path `/app`; this repository-only retention change does not deploy or mutate that service and therefore does not claim a new live-host hard gate.
- No GitHub workflow was dispatched for this change, no A/A or CTA population, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read, and no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, cart, checkout, payment, stock, or order state changed. No local runtime process was started.

Known issues:

- None introduced. Actual future artifact creation remains subject to GitHub Actions availability, but the reviewed inputs and effective 90-day repository retention prevent normal extended PC downtime from losing these handoffs.

Next exact step:

- Merge this retention hardening after CI. Continue result-blind A/A infrastructure monitoring without duplicate manual runs; before `2026-09-02 03:45 Europe/Bratislava`, do not inspect population, arms, outcomes, Meta dimensions, conversion, revenue, CM1, performance, or results. At the first due boundary, use the earliest successful exact-main scheduled A/A checkpoint artifact, independently validate and record it through a reviewed PR, and resolve only at the first artifact with at least `1,000` eligible devices.

## 2026-08-26 — VEVO 90-day gate-artifact retention merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-gate-artifact-retention-state`

What changed:

- PR `#438` merged the 90-day one-time gate-artifact retention contract into `main` as `9c43cb655d7b5f819ce51bd13e45e2f44b8587d0`.
- This handoff replaces the completed merge instruction with the next time-gated A/A operation; it changes no workflow behavior beyond the already merged artifact retention values.

What is verified:

- PR `#438` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- Local `main` and `origin/main` both read back exact merge commit `9c43cb655d7b5f819ce51bd13e45e2f44b8587d0`.
- The five gate-critical workflow uploads and both machine-readable output contracts read back `90`; daily health evidence remains `14` and encrypted credentials remain `1`.
- No result workflow was dispatched, no population or experiment result was read, no production/external state changed, and no local runtime process was started.

Known issues:

- None introduced. The effective repository artifact policy is proven by current 90-day `created_at`/`expires_at` metadata even though the current CLI token cannot read the separate Actions-policy settings endpoint.

Next exact step:

- Continue result-blind A/A infrastructure monitoring without duplicate manual runs. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect population, arms, outcomes, Meta dimensions, conversion, revenue, CM1, performance, or results. At the first due boundary, use the earliest successful exact-main scheduled A/A checkpoint artifact, independently validate and record it through a reviewed PR, and resolve only at the first artifact with at least `1,000` eligible devices.

## 2026-08-26 — VEVO Meta handoff aligned with the running A/A lifecycle

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-meta-aa-lifecycle-handoff`

What changed:

- Replaced the stale pre-publication Meta/population next gates in `growthbook_workspace.json` with the current frozen-window A/A evidence boundary.
- The workspace now says explicitly that a complete stable campaign/ad-set/ad/placement exposure must be present in the final frozen A/A evidence or the decision remains `NOT_READY`.
- Added a regression test that binds the checked-in running-A/A state to this gate while preserving the historical zero-coverage baseline, the no-bulk-live-edit rule, and the no-observed-Meta-mutation flag.

What is verified:

- PR `#440` contains only the workspace contract, its offline validator, and the regression test; no runtime or external configuration file changed.
- `789` Python tests and all `9` storefront JavaScript tests passed. GrowthBook workspace, Meta/reporting, A/A measurement-window, Pro, CTA measurement-window/completion, and central security validators passed; Ruff, Python compilation, and `git diff --check` passed.
- No A/A population, arm, outcome, Meta dimension, conversion, revenue, CM1, performance, or result was read. No AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, product, stock, cart, checkout, payment, or order state changed. No local runtime process was started.

Known issues:

- The historical Meta delivery audit remains intentionally recorded as zero complete-contract ads. Only the frozen final A/A evidence may prove whether at least one dimension-complete exposure occurred; the repository change makes no such result claim.

Next exact step:

- Merge PR `#440` after CI. Continue result-blind monitoring until `2026-09-02 03:45 Europe/Bratislava`; then consume the earliest successful exact-main A/A checkpoint artifact, record it through the offline hash-bound recorder, and keep A/A `NOT_READY` unless every frozen acceptance gate—including the complete stable Meta exposure—is proven.

## 2026-08-26 — VEVO Meta running-A/A handoff merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-meta-aa-lifecycle-state`

What changed:

- PR `#440` merged the current running-A/A Meta/population lifecycle handoff into `main` as `a2263b593719faf5862ce1a2d1f202f3e2c54898`.
- This handoff replaces the completed merge instruction with the frozen checkpoint boundary; it introduces no runtime, experiment, advertising, reporting, or commerce mutation.

What is verified:

- PR `#440` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- Local `main` and `origin/main` both read back exact merge commit `a2263b593719faf5862ce1a2d1f202f3e2c54898`.
- The checked-in workspace keeps the historical complete-contract Meta-ad baseline at `0`, forbids bulk live-ad edits, and now points only to the frozen final-evidence gate; no result is inferred or pre-recorded.
- No result workflow was dispatched, no population, arm, outcome, Meta dimension, conversion, revenue, CM1, performance, or result was read, and no production/external state or local runtime process changed.

Known issues:

- None introduced. A/A can pass the Meta acceptance requirement only if the future frozen evidence independently proves at least one complete stable Meta exposure; otherwise it must remain `NOT_READY`.

Next exact step:

- Continue result-blind infrastructure monitoring without duplicate manual runs. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect experiment population or results. At the first due boundary, use the earliest successful exact-main scheduled checkpoint artifact, independently validate and record it through a reviewed PR, and resolve only at the first artifact with at least `1,000` eligible devices.

## 2026-08-26 — VEVO CTA final artifact provenance hardened

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-final-provenance`

What changed:

- Added a third canonical, PII-free final-look file, `vevo-growthbook-cta-final-provenance.json`, to the protected future CTA workflow artifact bundle.
- The workflow-generated file binds the exact repository, workflow path, first run attempt, main commit, artifact name, and SHA-256 of the aggregate snapshot and offline decision.
- The offline recorder now requires an independently hashed canonical provenance file and rejects a swapped run ID, main commit, file set, or snapshot/decision hash before it can close the final-look gate.
- The provenance SHA-256 is stored in both the final-snapshot manifest and durable hypothesis registry and is cross-validated with their existing snapshot, decision, run, commit, verdict, recommendation, and registry-hash bindings.
- Updated the machine-readable output contract, runbook, plan, security CI, workflow tests, builder tests, recorder tests, and registry schema. No result workflow was dispatched.

What is verified:

- `791` Python tests and all `9` storefront JavaScript tests passed. The focused final builder/recorder/workflow/registry suite passed `21` tests, including execution of the exact inline provenance producer and negative cases for a swapped run, commit, and file hash.
- CTA final-snapshot, hypothesis-registry, workspace, CTA completion, CTA measurement-window, A/A measurement-window, Pro-upgrade, and central security validators passed.
- Scoped Ruff, Python compilation, JSON/YAML and inline-Python parsing, canonical-byte checks, and `git diff --check` passed.
- The versioned Production host boundary remains instance `N/A:Fargate`, recorded deployment private IP `172.31.39.76`, service `vevo-growthbook-reconcile-production`, and runtime path `/app`. This repository-only change performs no deploy or infrastructure mutation and therefore does not claim a new live-host readback.
- No A/A population, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read. No AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, cart, checkout, payment, stock, or order state changed. No local server, worker, watcher, tunnel, Docker stack, or persistent process was started.

Known issues:

- None introduced. The provenance applies to the future CTA final-look bundle; it does not open any current A/A or CTA result gate.

Next exact step:

- Merge this provenance hardening after CI. Continue result-blind infrastructure monitoring without duplicate manual runs. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect experiment population or results; at the first due boundary use the earliest successful exact-main scheduled A/A checkpoint artifact and record it through the offline hash-bound workflow.

## 2026-08-26 — VEVO CTA final provenance merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-final-provenance-state`

What changed:

- PR `#442` merged the future CTA final-artifact provenance hardening into `main` as `3cbcd0a5ac5ea885c2f82d10de0c8f8d7ddc38a4`.
- This handoff replaces the completed merge instruction with the frozen A/A checkpoint boundary and introduces no runtime, experiment, reporting, advertising, storefront, or commerce mutation.

What is verified:

- PR `#442` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- Local `main` and `origin/main` both read back exact merge commit `3cbcd0a5ac5ea885c2f82d10de0c8f8d7ddc38a4` before this handoff branch was created.
- The merged final-look bundle contract contains only the canonical aggregate snapshot, offline decision, and PII-free provenance. The offline recorder requires and cross-binds all three hashes with the exact first workflow run and main commit.
- The protected CTA final-look workflow was not dispatched. No A/A population, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read, and no production/external state or local runtime process changed.

Known issues:

- None introduced. The current A/A remains result-blind and the future CTA final-look remains closed.

Next exact step:

- Continue result-blind infrastructure monitoring without duplicate manual runs. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect experiment population or results. At the first due boundary, use the earliest successful exact-main scheduled A/A checkpoint artifact, independently validate and record it through a reviewed PR, and resolve only at the first artifact with at least `1,000` eligible devices.

## 2026-08-26 — VEVO A/A snapshot provenance hardened

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-snapshot-provenance`

What changed:

- Added a third canonical, PII-free A/A assembly file, `vevo-growthbook-aa-provenance.json`, to the future protected snapshot artifact bundle.
- The assembly workflow now admits only the first workflow attempt and binds the exact repository, workflow path, run ID, main commit, artifact name, snapshot hash, decision hash, and both reviewed source-component workflow/run/commit/artifact hashes.
- The offline A/A completion recorder now independently validates the canonical provenance, requires its SHA-256 in the completion manifest and zero-allocation stop readback, and rejects any attempt to rebind an already recorded PASS to another assembly artifact.
- Updated the machine-readable snapshot and completion contracts, A/A runbook, GrowthBook plan/workspace, central security CI, and regression fixtures. No result workflow was dispatched.

What is verified:

- `795` Python tests and all `9` storefront JavaScript tests passed, including execution of the exact inline provenance producer/validator and negative cases for changed file hashes, swapped runs, commits, source-component hashes, stop-readback hashes, and recorded-PASS rebinding.
- A/A measurement-window, A/A completion, Pro-upgrade, workspace, CTA completion, and central security validators passed; scoped Ruff, Python compilation, JSON/YAML parsing, and `git diff --check` passed.
- The protected A/A assembly workflow is active and has no run yet, which is expected because its first admitted scheduled checkpoint is still in the future. Existing manual infrastructure-monitor runs were not duplicated.
- The versioned Production host boundary remains instance `N/A:Fargate`, recorded deployment private IP `172.31.39.76`, service `vevo-growthbook-reconcile-production`, and runtime path `/app`. This repository-only change performs no deploy or infrastructure mutation and therefore does not claim a new live-host readback.
- No A/A population, eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read. No AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, cart, checkout, payment, stock, or order state changed. No local server, worker, watcher, tunnel, Docker stack, or persistent process was started.

Known issues:

- None introduced. The provenance contract applies only to the future A/A snapshot bundle and does not open the currently frozen result gate.

Next exact step:

- Merge this A/A provenance hardening after CI. Continue result-blind infrastructure monitoring without duplicate manual runs. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect experiment population or results; at the first due boundary use the earliest successful exact-main scheduled A/A checkpoint artifact and record it through the offline hash-bound workflow.

## 2026-08-26 — VEVO A/A snapshot provenance merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-snapshot-provenance-state`

What changed:

- PR `#444` merged the protected A/A snapshot provenance contract into `main` as `33e4895395cdf08c922e782c09d5b57f0127f584`.
- This handoff replaces the completed merge instruction with the frozen checkpoint boundary and introduces no runtime, experiment, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#444` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- `origin/main` and this state branch both read back exact merge commit `33e4895395cdf08c922e782c09d5b57f0127f584`.
- The merged A/A artifact bundle contains only the canonical aggregate snapshot, offline decision, and PII-free provenance; the offline recorder requires and cross-binds all three hashes with the exact first assembly run, main commit, and both source artifacts.
- The protected A/A snapshot workflow was not dispatched. No A/A population, eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read, and no production/external state or local runtime process changed.

Known issues:

- None introduced. The A/A result gate remains closed until the scheduled checkpoint boundary.

Next exact step:

- Continue result-blind infrastructure monitoring without duplicate manual runs. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect experiment population or results. At the first due boundary, use the earliest successful exact-main scheduled A/A checkpoint artifact, independently validate its snapshot, decision, provenance, and source bindings, record it through a reviewed PR, and resolve only at the first artifact with at least `1,000` eligible devices.

## 2026-08-26 — VEVO CTA start directly bound to verified Pro evidence

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-pro-provenance`

What changed:

- Added the exact GrowthBook Pro transition manifest and its canonical six-metric observation as first-class dynamic source bindings in `growthbook_cta_activation.json`.
- The offline CTA review recorder now validates the verified Pro plan/metric contract against the current workspace, requires canonical file hashes, persists both hashes, and rejects either file changing before the recorded CTA start.
- The CTA runtime release validator now independently validates the canonical Pro manifest/readback before AWS credentials; the workspace validator also requires the exact observation and hash in every verified-Pro state.
- Updated the CTA activation runbook, rollout plan, workspace handoff, central security CI, and regression tests. Workspace flags alone can no longer impersonate the paid Pro/quantile-metric gate.

What is verified:

- `796` Python tests and all `9` storefront JavaScript tests passed. Focused Pro/CTA/workspace tests passed `46` cases, including rehashed billing-contract drift, swapped Pro source hashes, non-canonical evidence, and runtime-release rejection.
- GrowthBook workspace, Pro-upgrade, CTA completion, and central security validators passed; scoped Ruff, Python compilation, JSON parsing, and `git diff --check` passed.
- The official GrowthBook pricing page read on `2026-08-26` still lists Cloud Pro at `$40` per seat per month; no checkout, subscription, trial, payment method, or account state was opened or changed.
- The versioned Production host boundary remains instance `N/A:Fargate`, recorded deployment private IP `172.31.39.76`, service `vevo-growthbook-reconcile-production`, and runtime path `/app`. This repository-only change performs no deploy or infrastructure mutation and therefore does not claim a new live-host readback.
- No A/A population, eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read. No workflow was dispatched and no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, cart, checkout, payment, stock, or order state changed. No local runtime process was started.

Known issues:

- None introduced. The new bindings remain null and fail-closed until the future A/A PASS/stop and explicitly authorized Pro transition genuinely create the canonical files.

Next exact step:

- Merge this direct Pro-to-CTA provenance hardening after CI. Continue result-blind monitoring without duplicate manual runs. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect experiment population or results; at the first due boundary process the earliest successful exact-main A/A checkpoint through the protected offline chain.

## 2026-08-26 — VEVO direct Pro-to-CTA provenance merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-pro-provenance-state`

What changed:

- PR `#446` merged the direct GrowthBook Pro manifest/readback bindings for the future CTA activation into `main` as `7ce987afb85643c3babc8a7d62044c5fbb55149d`.
- This handoff replaces the completed merge instruction with the frozen A/A checkpoint boundary and introduces no runtime, experiment, billing, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#446` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- `origin/main` and this state branch both read back exact merge commit `7ce987afb85643c3babc8a7d62044c5fbb55149d`.
- The merged waiting CTA manifest contains the null, fail-closed `pro_upgrade` and `pro_upgrade_observation` bindings. The future release/start recorders require both canonical files, validate them against the current workspace, and reject either hash changing after review.
- No result workflow was dispatched, no A/A population or result was read, no paid GrowthBook action occurred, and no production/external state or local runtime process changed.

Known issues:

- None introduced. The Pro evidence files do not yet exist because the paid transition remains correctly gated behind verified A/A PASS, zero-allocation stop, and fresh user confirmation.

Next exact step:

- Continue result-blind infrastructure monitoring without duplicate manual runs. Before `2026-09-02 03:45 Europe/Bratislava`, do not inspect experiment population or results. At the first due boundary, use the earliest successful exact-main scheduled A/A checkpoint artifact, independently validate and record it through a reviewed PR, and resolve only at the earliest artifact that satisfies the frozen eligible-device threshold.

## 2026-08-26 — VEVO CTA safety-only contract prepared

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-safety-contract`

What changed:

- Added the fail-closed `growthbook_cta_safety_monitoring.json` contract for the future CTA test. It freezes the 24-hour cadence, exact aggregate no-identity schema, 200 measured page loads per arm, existing LCP/INP/CLS and client-error thresholds, immediate commerce/runtime-error stop reasons, and zero primary/business/Meta/winner/automatic-mutation access.
- Added the offline `evaluate_growthbook_cta_safety.py` evaluator with only `CONTINUE`, `CONTINUE_NOT_MATURE`, and `STOP_REQUIRED` decisions, plus a checked-in contract/hash validator and regression suite.
- Integrated the waiting safety contract into the workspace and central security validators. Both CTA start-source hashes, collection permission, manual stop permission, and all external mutation boundaries remain null/false.
- Updated the CTA plan, activation runbook, and workspace handoff without claiming that operational collection exists yet.

What is verified:

- `806` Python tests and all `9` storefront JavaScript tests pass. The focused safety evaluator suite passes `10` cases, including strict threshold boundaries, every immediate pre-maturity commerce stop, malformed/non-finite evidence, forbidden outcome/identity fields, canonical CLI output, and contract/hash drift.
- The safety contract, full workspace validator, central security CI, scoped Ruff, Python compilation, JSON parsing, and `git diff --check` pass.
- The versioned Production host boundary remains instance `N/A:Fargate`, recorded deployment private IP `172.31.39.76`, service `vevo-growthbook-reconcile-production`, and runtime path `/app`. This repository-only change performs no deploy or infrastructure mutation and therefore does not claim a new live-host readback.
- No A/A population, eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read. No result workflow was dispatched and no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, cart, checkout, payment, stock, or order state changed. No local runtime process was started.

Known issues:

- The immutable safety contract and evaluator now exist, but the protected checkpoint collector/recorder and the `STOP_REQUIRED` to reviewed manual GrowthBook stop/completion handoff are not implemented yet. Until that separate work is merged and verified, operational early-safety monitoring remains closed and Gate 4 is not launch-ready.

Next exact step:

- Merge this safety-contract change after CI, then implement the hash/run/commit-bound safety checkpoint collection/recording path and connect a verified `STOP_REQUIRED` decision to the existing reviewed manual CTA stop/completion lifecycle. Continue the result-blind A/A boundary: before `2026-09-02 03:45 Europe/Bratislava`, do not inspect experiment population or results or dispatch result workflows.

## 2026-08-26 — VEVO CTA safety-only contract merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-safety-contract-state`

What changed:

- PR `#448` merged the fail-closed CTA safety-only contract and offline evaluator into `main` as `a4e87378275da9e64382fee45ddc93d0e5b1c310`.
- This handoff replaces the completed merge instruction with the next executable safety-lifecycle step and introduces no runtime, experiment, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#448` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- `origin/main` and this state branch both read back exact merge commit `a4e87378275da9e64382fee45ddc93d0e5b1c310` before this handoff edit.
- The merged safety manifest remains closed with null CTA start-source hashes and all collection, stop, outcome, winner, and automatic-mutation boundaries false. The offline evaluator accepts only aggregate safety fields and has no external client.
- No result workflow was dispatched, no A/A population or result was read, and no production/external state or local runtime process changed.

Known issues:

- The operational safety checkpoint collector/recorder and verified `STOP_REQUIRED` to reviewed manual CTA stop/completion handoff are still pending. Gate 4 remains not launch-ready until both are implemented and verified.

Next exact step:

- Implement the hash-bound safety stop lifecycle integration first, then the protected PC-independent checkpoint collection path. Continue the frozen A/A boundary: before `2026-09-02 03:45 Europe/Bratislava`, do not inspect experiment population or results or dispatch result workflows.

## 2026-08-26 — VEVO CTA safety stop lifecycle prepared

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-safety-stop-lifecycle`

What changed:

- Added the offline `record_growthbook_cta_safety_checkpoint.py` lifecycle recorder. It can bind a future verified CTA start while leaving collection disabled, and later accepts only canonical evidence/decision/provenance artifacts whose independent SHA-256 values, successful workflow run, and exact main commit all match. It re-runs the frozen safety evaluator itself.
- A `CONTINUE` or `CONTINUE_NOT_MATURE` checkpoint leaves assignment and the reviewed stop lifecycle unchanged. A verified `STOP_REQUIRED` closes safety collection, records the exact trigger, and opens only the existing manual CTA stop review; it performs no external mutation and cannot call a winner.
- Extended `growthbook_cta_measurement_window.json` with an explicit reviewed stop-trigger contract. The normal first-`N`/day-42 path remains outcome-blind, while a safety stop must preserve all three safety artifact hashes and its observed time.
- Extended `record_growthbook_cta_completion.py` so either reviewed trigger reaches the same exact manual GrowthBook-only stop readback, stopped safety state, zero Production allocation, and frozen 14-day follow-up. Completion now hash-binds the safety manifest and validates all six resulting manifests before writing.
- Updated the existing outcome-blind checkpoint workflow fixture for the expanded closed stop schema, strengthened central security/workspace validation, and documented that protected safety collection remains a separate disabled step.

What is verified:

- The full official Python suite passes `814` tests and the storefront JavaScript suite passes all `9` tests. The focused safety/window/completion/final-snapshot suite passes `38` cases, including forged decisions, altered measurement state, swapped workflow run/main commit, both commerce and performance stops, the unchanged outcome-blind path, and the shared 14-day follow-up.
- CTA safety, measurement-window, completion, workspace, and central security validators pass. Scoped Ruff, Python compilation, VEVO JSON parsing, workflow YAML parsing, and `git diff --check` pass.
- The checked-in manifests remain fail-closed at the pre-CTA waiting state. All safety collection/recording/manual-stop, primary/business-outcome, winner, and automatic external-mutation gates remain false.
- No A/A population, eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read. No result or safety workflow was dispatched; no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, product, cart, checkout, payment, stock, or order state changed. No local runtime process was started.

Known issues:

- The reviewed safety STOP lifecycle is now executable offline, but the protected PC-independent safety checkpoint collection workflow is not implemented. Recording remains deliberately impossible from the checked-in state because all three collection/recording gates are false.

Next exact step:

- Merge this lifecycle change after CI, then implement the protected main-only safety collection workflow with the exact Production Fargate identity/localhost hard gate, aggregate identity-free safety evidence, independent decision/provenance hashes, and no primary/business/Meta/winner or mutation access. Before `2026-09-02 03:45 Europe/Bratislava`, continue to avoid A/A population/results and do not dispatch result workflows.

## 2026-08-26 — VEVO CTA safety stop lifecycle merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-safety-stop-lifecycle-state`

What changed:

- PR `#450` merged the hash/run/commit-bound CTA safety recorder and reviewed manual-stop/follow-up integration into `main` as `f9159bee557b4041703cbc25b06868c89b87e6fa`.
- This handoff records the completed merge and advances the next exact step to the separate protected collection workflow. It introduces no runtime, experiment, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#450` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- `origin/main` and this state branch both read back exact merge commit `f9159bee557b4041703cbc25b06868c89b87e6fa` before this handoff edit.
- The merged checked-in manifests remain fail-closed before CTA start. A future verified `STOP_REQUIRED` can open only the same reviewed manual CTA stop and 14-day follow-up as the outcome-blind window; it cannot stop assignment automatically or call a winner.
- No A/A population or result was read, no result or safety workflow was dispatched, and no production/external state or local runtime process changed.

Known issues:

- The protected PC-independent safety checkpoint collection workflow is still missing. All three collection/recording gates remain false, so operational early-safety monitoring is not launch-ready.

Next exact step:

- Implement the protected main-only safety collection workflow with the exact Production Fargate identity/localhost hard gate, aggregate identity-free performance/client-error and commerce evidence, independently hash-bound decision/provenance, and no primary/business/Meta/winner or mutation access. Keep the A/A result boundary closed until `2026-09-02 03:45 Europe/Bratislava`.

## 2026-08-26 — VEVO CTA protected safety checkpoint collection prepared

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-safety-collection`

What changed:

- Added the main-only hourly `check-vevo-growthbook-production-cta-safety.yml` workflow and deterministic offline `build_growthbook_cta_safety_checkpoint.py` builder. Waiting, closed, pre-due, late, and already-recorded states skip before AWS; a due checkpoint is derived from the verified CTA start and admitted only within the frozen 60-minute window.
- Added the SHA-bound `cta_safety_checkpoint_production.sql`. It emits only two aggregate variation-health rows with eligible-device, measured-page-load, client-error, p75 LCP/INP/CLS, and aggregate assignment-quality fields. It has no primary/business outcome, Meta dimension, raw identity, or winner output.
- The admitted workflow verifies the checked-in Fargate host target (`N/A:Fargate`, recorded deployment IP `172.31.39.76`, service `vevo-growthbook-reconcile-production`, path `/app`), inherited localhost health/marker evidence, current stack/scheduler/task-image invariants, a recent scheduled success marker, clear alarms, and empty DLQ. It probes only the public product and cart URLs with HTTP GET and deletes every raw AWS/Athena/HTML response before uploading the exact canonical evidence/decision/provenance bundle for 90 days.
- Extended the future canonical CTA-start readback with the exact product URL/code, cart URL, CTA text, and normalized EUR price baseline. Initialization now opens only the three protected safety collection/recording gates. Bad query/variation/assignment-quality evidence becomes `STOP_REQUIRED` rather than an unrecordable validation failure; the offline recorder enforces the exact due/lateness window and accepts a later due index even if a prior day was not recorded.
- Strengthened workspace and central security validation, workflow/builder regression coverage, and the CTA plan/runbook/workspace handoff. No workflow was dispatched and the checked-in manifest remains waiting with all gates false.

What is verified:

- The full repository suite passes `832` Python tests and all `9` storefront JavaScript tests. The focused activation/safety evaluator/recorder/builder/workflow suite passes `48` tests. CTA safety contract/hash validation, full workspace validation, central security CI, scoped Ruff, workflow YAML/JSON parsing, Python compilation, and `git diff --check` pass.
- The workflow contains no infrastructure deploy/update/delete, GrowthBook/GTM/Meta Ads/BiznisWeb client, commerce POST, automatic stop, or winner path. Exactly two storefront GET calls are allowed, and only the canonical three-file bundle leaves the runner.
- Before `2026-09-02 03:45 Europe/Bratislava`, no A/A population, eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was read. No result or safety workflow was dispatched; no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, price, product, cart, checkout, payment, stock, or order state changed. No local runtime process was started.

Known issues:

- This change is prepared but not yet merged. The checked-in safety manifest deliberately stays `waiting_for_verified_cta_start`; schedule invocations stop before AWS until the future verified CTA start is recorded and the offline initializer opens the protected gates.

Next exact step:

- Merge this protected collection change after CI. Keep the frozen A/A result boundary closed until `2026-09-02 03:45 Europe/Bratislava`; only then process the protected A/A evidence, and require the verified A/A PASS plus reviewed zero-allocation stop before requesting fresh confirmation for the paid GrowthBook Pro action.

## 2026-08-26 — VEVO CTA protected safety checkpoint collection merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-safety-collection-state`

What changed:

- PR `#452` merged the protected PC-independent CTA safety checkpoint collection path into `main` as `0ae0cbefea16b497d24cec17c1f2a715189a3127`.
- This handoff records the completed merge and replaces the obsolete merge instruction with the next time-gated A/A step. It introduces no runtime, experiment, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#452` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- `origin/main` and this state branch read back exact merge commit `0ae0cbefea16b497d24cec17c1f2a715189a3127` before this handoff edit.
- The merged safety workflow remains fail-closed in the checked-in `waiting_for_verified_cta_start` state. All collection/recording/manual-stop, primary/business-outcome, Meta-dimension, winner, and automatic external-mutation gates remain false, so scheduled runs skip before AWS until a future verified CTA start is initialized.
- No A/A population or result was read, no result or safety workflow was dispatched, and no production/external state or local runtime process changed.

Known issues:

- The A/A result boundary remains time-locked. GrowthBook Pro has not been purchased, and its paid action still requires verified A/A `PASS`, the reviewed zero-allocation stop, and fresh action-time confirmation.

Next exact step:

- Keep the frozen A/A boundary closed until `2026-09-02 03:45 Europe/Bratislava`. At or after that time, process the already protected A/A evidence path exactly once without browser-result peeking; if and only if it independently reproduces `PASS`, complete the reviewed A/A zero-allocation stop, then request fresh confirmation before the paid GrowthBook Pro action.

## 2026-08-26 — VEVO A/A missing-checkpoint recovery hardened

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-checkpoint-backfill-safety`

What changed:

- Static outcome-blind review of the post-boundary A/A snapshot, PASS, reviewed zero-allocation stop, and GrowthBook Pro handoff confirmed that those transitions remain hash/run/commit-bound and fail closed. No A/A population or result was opened.
- Closed a checkpoint-liveness gap: one failed scheduled and same-window run could previously leave the required next checkpoint artifact permanently unavailable because the offline recorder rejects index gaps while the manual workflow also rejected all historical reconstruction.
- Added an explicit schema-`3` checkpoint collection mode. Normal scheduled and manual same-window runs retain the original 24-hour gate. A late run is admitted only by a manual dispatch with both exact confirmations, is marked `manual_historical_backfill`, and reconstructs only `len(checkpoint_history) + 1` at its original preregistered cutoff.
- The offline validator accepts late evidence only when that schema/mode/timing combination is exact; it still rejects gaps, premature backfill, late same-window evidence, unknown modes, unsafe fields, outcome reads, and mutations. Legacy schema `1`/`2` evidence remains valid only inside its original daily gate.
- Updated the runbook and central security assertions. The runbook explicitly forbids creating a backfill when a successful artifact for that index already exists.

What is verified:

- Commit `ad16de14` is pushed to `origin/codex/vevo-aa-checkpoint-backfill-safety`.
- The full repository Python suite passes `835` tests. The focused A/A window/evidence/snapshot/completion/Pro chain passes `81` tests, and all `9` storefront JavaScript tests pass.
- Workspace validation, central security validation, workflow YAML parsing, scoped Ruff lint/format, Python compilation, and `git diff --check` pass.
- No A/A eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was inspected. No workflow was dispatched; no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, product, price, cart, checkout, payment, stock, or order state changed. No local runtime process was started.

Known issues:

- This recovery hardening is pushed but not yet merged. The A/A result boundary remains time-locked, and GrowthBook Pro still requires verified A/A `PASS`, reviewed zero Production allocation, and fresh action-time confirmation.

Next exact step:

- Open and merge the recovery PR after CI. Keep the frozen A/A boundary closed until `2026-09-02 03:45 Europe/Bratislava`; at or after that time, use the earliest successful checkpoint artifact in index order, and use the explicit historical backfill only if that exact next artifact was never created.

## 2026-08-26 — VEVO A/A missing-checkpoint recovery merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-checkpoint-backfill-state`

What changed:

- PR `#454` merged the fail-closed missing-checkpoint recovery into `main` as `cd0f1f7836ece75c2f3b039ac7a46c5ce44f62f9`.
- This handoff records the completed merge and replaces the obsolete merge instruction with the exact time-gated A/A resolution step. It introduces no experiment, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#454` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- `origin/main` and the local worktree both read back exact merge commit `cd0f1f7836ece75c2f3b039ac7a46c5ce44f62f9`.
- Scheduled, same-window, and explicit next-missing historical checkpoint paths are now deterministic and sequential; all remain aggregate-only, outcome-blind, identity-free, winner-free, and externally read-only.
- No A/A population or result was inspected, no workflow was dispatched, and no production/external state or local runtime process changed.

Known issues:

- The A/A result boundary remains time-locked. GrowthBook Pro has not been purchased and still requires verified A/A `PASS`, the reviewed zero-allocation stop, and fresh action-time confirmation.

Next exact step:

- Keep the frozen A/A boundary closed until `2026-09-02 03:45 Europe/Bratislava`. At or after that time, record the earliest successful checkpoint artifact in index order; use schema-`3` historical backfill only if the exact next artifact was never created. Resolve the window without arm/outcome peeking, then process the protected A/A evidence path and continue only on an independently reproduced `PASS`.

## 2026-08-26 — VEVO CTA missing-checkpoint recovery hardened

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-checkpoint-backfill-safety`

What changed:

- Closed the equivalent future CTA assignment-window liveness gap without opening the currently time-locked A/A result boundary. A missed scheduled and same-window CTA artifact can no longer make the strict sequential recorder permanently impossible to advance.
- Added explicit schema-`3` CTA checkpoint collection modes. Scheduled and manual same-window runs retain the original exact daily gate. A late run requires both manual confirmations, is marked `manual_historical_backfill`, and can reconstruct only the exact next missing index at its original preregistered cutoff.
- Preserved schema-`1`/`2` compatibility and the schema-`2` runtime provenance fields. The validator rejects premature backfill, late same-window evidence, unknown or contradictory modes, index gaps, evidence after resolution, outcomes, winner fields, and external mutations.
- Updated the activation runbook and central security markers. Historical backfill is forbidden when a successful artifact for that exact checkpoint already exists.

What is verified:

- Core commit `ec73e406` is pushed to `origin/codex/vevo-cta-checkpoint-backfill-safety`.
- The full repository suite passes `838` Python tests and all `9` storefront JavaScript tests. The broader focused CTA lifecycle suite passes `157` tests.
- CTA measurement-window, final-snapshot, workspace, and central security validators pass. Scoped Ruff on changed Python files, Python compilation, workflow YAML parsing, and `git diff --check` pass.
- No A/A population, eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was inspected. No workflow was dispatched; no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, product, price, cart, checkout, payment, stock, or order state changed. No local runtime process was started.

Known issues:

- This recovery hardening is pushed but not yet merged.
- Static CTA lifecycle audit found a separate circular launch gate: the current activation contract requires verified Production CTA lifecycle evidence before CTA can start, while the required CTA data cannot exist before that start. The recorder is safe but has no protected reproducible producer. This must be corrected on a separate clean branch before CTA launch readiness is claimed.

Next exact step:

- Merge the CTA checkpoint recovery after CI. Then, on a clean branch from updated `main`, replace the circular pre-start lifecycle dependency with a source-explicit, protected reconciliation path and verify the complete activation-to-final-snapshot chain. Keep the A/A boundary closed until `2026-09-02 03:45 Europe/Bratislava` and do not dispatch result workflows before then.

## 2026-08-26 — VEVO CTA missing-checkpoint recovery merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-checkpoint-backfill-state`

What changed:

- PR `#456` merged the fail-closed future CTA missing-checkpoint recovery into `main` as `b5313349256866da2bfa7e76e1ad92180f34c49d`.
- This handoff records the completed merge and advances the source of truth to the independently discovered CTA lifecycle launch-gate repair. It introduces no experiment, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#456` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` before merge.
- `origin/main` and this state branch both read back exact merge commit `b5313349256866da2bfa7e76e1ad92180f34c49d` before this handoff edit.
- Scheduled, same-window, and explicit next-missing historical CTA checkpoint paths are deterministic, sequential, aggregate-only, outcome-blind, identity-free, winner-free, and externally read-only.
- No A/A population or result was inspected, no workflow was dispatched, and no production/external state or local runtime process changed.

Known issues:

- CTA launch readiness remains blocked by a circular lifecycle contract: verified Production CTA lifecycle evidence is required before CTA start even though the required CTA data cannot exist until after start. The current offline recorder is fail-closed, but there is no protected producer that can satisfy this gate reproducibly.

Next exact step:

- On a clean branch from this merge, replace the circular pre-start lifecycle dependency with a source-explicit protected reconciliation path, then verify the complete activation-to-final-snapshot chain. Keep the A/A result boundary closed until `2026-09-02 03:45 Europe/Bratislava` and do not dispatch result workflows before then.

## 2026-08-26 — VEVO CTA lifecycle launch gate repaired and final maturity corrected

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-lifecycle-gate-repair`

What changed:

- Replaced the circular pre-start CTA lifecycle requirement with a source-explicit protected preflight over the completed and stopped Production A/A `vevo-sk-aa-001`. The preflight cannot read future CTA outcomes and requires the exact A/A completion/snapshot hashes, verified `PASS`, reviewed zero Production allocation, a resolved source window, 7-day order attribution, and 14-day per-order lifecycle maturity.
- Added a main-only daily GitHub workflow and offline builder. Before the gate is due it exits successfully before AWS credentials. It admits automatic collection only during the first 24-hour due interval; later recovery requires exact manual confirmation. An admitted run verifies the Production account, Fargate instance identity/private IP, service, `/app` path, task definition/image and inherited localhost markers, then compares the frozen cohort through temporary direct curated S3 facts and one aggregate Athena query.
- Bound the reporting-quality object to the exact retained `facts_generated_at` generation present on the direct frozen cohort, with no object listing or arbitrary latest-object selection. The canonical artifact requires one generation, zero immature orders, at least one mature cancellation/refund/credit-note case, exact lifecycle-count parity, cent-exact CM1 parity, no identities, no CTA outcomes and no external mutation; every raw AWS response and identity-bearing fact is removed before the one artifact upload.
- Strengthened the offline lifecycle recorder, CTA activation recorder and runtime-release validator with canonical observation validation plus exact workflow-run, main-commit, observation SHA-256, source-completion SHA-256 and source-snapshot SHA-256 binding.
- Corrected the CTA final observation boundary from stop plus 14 days to stop plus 21 days everywhere: 7 days in which an attributed order may arrive plus 14 days for that last order to mature. Contracts, hashes, manifests, workflows, evaluators, validators, tests, runbooks and log markers now agree.

What is verified:

- The full repository suite passes `855` Python tests; the focused CTA suite passes `169` tests, the protected lifecycle preflight/recorder/activation group passes `36` tests, and all `9` storefront JavaScript tests pass.
- Central security validation, GrowthBook workspace/completion/final-snapshot/measurement/safety/design/hypothesis/Pro/reporting validators, scoped Ruff lint/format, Python compilation, workflow YAML plus inline-Python parsing, and `git diff --check` pass. The runtime-release command remains intentionally fail-closed because the future protected A/A observation file does not yet exist.
- The current scheduled gate was executed only against checked-in pending contracts and returned `RUN_COLLECTION=false`, reason `aa-pass-stop-window-not-ready`, with `aws=false`.
- No A/A population, eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance or result was inspected. No workflow was dispatched; no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, product, price, cart, checkout, payment, stock or order state changed. No browser or local runtime process was started.

Known issues:

- Core repair commit `714180eeb53da5ee6e17fa2813f68521c2d2b634` is pushed to `origin/codex/vevo-cta-lifecycle-gate-repair`, but is not yet reviewed or merged.
- The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`. GrowthBook Pro remains unpurchased and still requires an independently reproduced A/A `PASS`, reviewed zero Production allocation and fresh action-time confirmation.
- The lifecycle preflight can run only after the eventual resolved A/A window end plus 21 days. Until its canonical artifact is recorded, CTA activation and runtime release remain fail-closed.

Next exact step:

- Commit and push this handoff, open the repair PR and merge only after all required CI checks pass. Keep the A/A result boundary closed until `2026-09-02 03:45 Europe/Bratislava`; at or after that time process only the protected A/A evidence chain. Continue toward GrowthBook Pro and the first non-price CTA A/B test only after an independently verified `PASS`, reviewed zero-allocation stop and fresh purchase confirmation.

## 2026-08-26 — VEVO CTA lifecycle launch-gate repair merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-lifecycle-gate-state`

What changed:

- PR `#458` merged the source-explicit CTA lifecycle preflight, exact evidence/source-hash activation binding and 21-day final-maturity correction into `main` as `d0a0a4c1e40c9e5d02cc95c9741322799a34dc99`.
- This handoff records the completed merge and replaces the obsolete launch-gate repair instruction with the exact time-gated A/A continuation. It introduces no experiment, reporting, advertising, storefront, commerce or infrastructure mutation.

What is verified:

- PR `#458` passed `env-check`, `secret-scan`, `observability-baseline` and `security-baseline` before merge and was `CLEAN`/`MERGEABLE` on exact head `2f41206b3de32963a74e4da31e5cf80bee0abc02`.
- `origin/main` and this state branch both read back exact merge commit `d0a0a4c1e40c9e5d02cc95c9741322799a34dc99` before this handoff edit.
- The circular lifecycle dependency is removed, final CTA maturity is correctly stop plus 21 days, and all CTA activation/runtime gates remain closed until their exact protected evidence exists.
- No A/A population or result was inspected, no workflow was dispatched, and no production/external state or local runtime process changed.

Known issues:

- The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`. GrowthBook Pro remains unpurchased and requires independently reproduced A/A `PASS`, reviewed zero Production allocation and fresh action-time confirmation.
- The lifecycle preflight remains intentionally pending until the resolved A/A source window later reaches its full 21-day order/lifecycle maturity.

Next exact step:

- Keep the frozen A/A boundary closed until `2026-09-02 03:45 Europe/Bratislava`. At or after that time, record the earliest successful outcome-blind checkpoint artifact in index order, resolve the window without arm/outcome peeking, and process only the protected A/A evidence path. Continue only on an independently reproduced `PASS`; after the reviewed zero-allocation stop, request fresh confirmation immediately before purchasing GrowthBook Pro.

## 2026-08-26 — VEVO GrowthBook Pro paid action-time gate hardened

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-pre-aa-readiness-audit`

What changed:

- Audited the static post-A/A chain without opening any protected A/A population or result. GrowthBook remains the onsite randomizer; canonical Meta campaign/ad-set/ad/placement URL IDs remain diagnostic reporting dimensions only, and the existing reporting core plus anonymous curated facts remain the hypothesis-verification path.
- Closed a paid-action authorization gap in the offline GrowthBook Pro transition. The recorder now accepts only a canonical whole-second UTC `Z` confirmation that is no more than 15 minutes old, no more than 60 seconds ahead of the executing clock, and at or after the verified A/A zero-allocation stop.
- An already-open review can now refresh its action-time timestamp only while the exact bound A/A completion and pre-upgrade workspace SHA-256 values remain unchanged. A new read-only `assert-action-time` command rechecks those hashes and freshness immediately before the reviewed paid click.
- Bound the canonical Pro/quantile-metric observation to the refreshed authorization chronology: it must be recorded at or after authorization and no more than four hours later. Added fail-closed tests for stale/future/non-canonical/pre-stop timestamps, source drift, refresh, assertion expiry, and early/late observations.
- Updated the Pro runbook and central security checks. The runbook requires a fresh reviewed confirmation, exact-main assertion, one seat at the official `$40 USD` monthly base price, and stop/escalation instead of backdating when the evidence window expires. GrowthBook Pro remains unpurchased and CTA remains draft at zero allocation.

What is verified:

- The full repository suite passes `859` Python tests and all `9` storefront JavaScript tests. The focused Pro/CTA/workspace suite passes `45` tests.
- GrowthBook Pro/workspace validators, central security validation, scoped Ruff lint, Python compilation, JavaScript syntax, and `git diff --check` pass.
- The official GrowthBook pricing page and metric documentation were rechecked on 2026-08-26: Pro is listed at `$40` per seat per month and Quantile Metrics are a Pro capability matching the preregistered p75 contract.
- No A/A eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was inspected. No workflow was dispatched and no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, product, price, stock, cart, checkout, payment, or order state changed. No browser or local runtime process was started.

Known issues:

- This hardening is not yet merged. The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`.
- A current user acknowledgement is not the future paid action-time confirmation. Purchase remains impossible until an independently reproduced A/A `PASS`, reviewed zero Production allocation, and a new confirmation immediately before the exact `$40 USD` monthly Pro action.

Next exact step:

- Commit and push this hardening, open a PR, and merge only after all required CI checks pass. Then keep the A/A boundary closed until `2026-09-02 03:45 Europe/Bratislava`; at or after that time process only the protected outcome-blind A/A chain. Continue to the reviewed GrowthBook Pro purchase only after verified `PASS`, zero-allocation stop, and a fresh action-time confirmation that passes `assert-action-time` on exact `main`.

## 2026-08-26 — VEVO GrowthBook Pro action-time hardening merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-pro-action-gate-state`

What changed:

- PR `#460` merged the machine-enforced GrowthBook Pro paid action-time gate into `main` as `851ff0f268971c616fd07b226d20f2f896434439`.
- This handoff records the completed merge and replaces the obsolete merge instruction with the exact time-gated A/A continuation. It introduces no experiment, billing, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#460` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` on exact head `61291b8ce9ddac2359c64e9cb8021fb660c4f1b9` before merge.
- `origin/main` and this state branch read back exact merge commit `851ff0f268971c616fd07b226d20f2f896434439`.
- The future paid GrowthBook Pro click now requires an unchanged hash-bound A/A/workspace state and a canonical confirmation no more than 15 minutes old; the canonical post-upgrade observation is bounded to the following four hours. GrowthBook Pro remains unpurchased and CTA remains draft at zero allocation.
- No A/A population or result was inspected, no workflow was dispatched, and no production/external state or local runtime process changed.

Known issues:

- The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`. The current user acknowledgement is not the future action-time purchase confirmation.

Next exact step:

- Keep the frozen A/A boundary closed until `2026-09-02 03:45 Europe/Bratislava`. At or after that time, record the earliest successful outcome-blind checkpoint artifact in index order, resolve the window without arm/outcome peeking, and process only the protected A/A evidence path. Continue only on an independently reproduced `PASS`; after the reviewed zero-allocation stop, request a new confirmation immediately before the exact one-seat `$40 USD` monthly GrowthBook Pro action and require `assert-action-time` to pass on exact `main`.

## 2026-08-26 — VEVO CTA start handoff made source-complete

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-post-aa-operator-readiness`

What changed:

- Audited the static post-boundary operator sequence from A/A resolution through GrowthBook Pro, the completed-A/A 21-day lifecycle preflight, CTA-only runtime, manual CTA start, outcome-blind checkpoints, stop, final 21-day follow-up, and durable reporting/hypothesis decision. No protected A/A result was opened.
- Closed a start-handoff gap: the CTA review bound the canonical lifecycle and runtime artifacts, but `record-start` did not previously re-read those ephemeral evidence files. A reviewed manifest could therefore outlive a deleted or drifted runtime/lifecycle file even though the later workspace validator would eventually reject the recorded state.
- Added the read-only `assert-start-ready` hard gate. On exact synchronized `main` it revalidates the reviewed gate, every versioned source SHA-256, the canonical A/A stop observation, verified Pro manifest/observation, frozen sample, completed-A/A lifecycle manifest/observation and source hashes, CTA-only registry/runtime artifact and workflow provenance, workspace, design, decision, and immutable Meta/reporting contract.
- Made `record-start` call the same complete readiness validator before it can construct either the running activation or workspace state. A missing, non-canonical, rehashed, provenance-drifted, unsafe, or no-longer-reviewed source now fails before any output file is written.
- Updated the CTA runbook to require the assertion plus workspace and Meta/reporting validators immediately before the manual GrowthBook start. Corrected the monitoring sequence so the completed-A/A 21-day lifecycle preflight explicitly precedes the CTA-only runtime and start gates.

What is verified:

- The full repository suite passes `861` Python tests and all `9` storefront JavaScript tests. The focused activation/window/workspace group passes `69` tests.
- GrowthBook Pro, Meta/reporting, workspace, CTA completion/safety, and central security validators pass. Scoped Ruff lint, Python compilation, JavaScript syntax, and `git diff --check` pass.
- The checked-in waiting state executes `assert-start-ready` fail-closed with `CTA manual start review is not open` before attempting to read future evidence. No output or source file changes.
- No A/A eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was inspected. No workflow was dispatched and no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, product, price, stock, cart, checkout, payment, or order state changed. No browser or local runtime process was started.

Known issues:

- This source-complete CTA start gate is not yet merged. The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`.

Next exact step:

- Commit and push this hardening, open a PR, and merge only after all required CI checks pass. Then preserve the frozen A/A boundary until `2026-09-02 03:45 Europe/Bratislava`; at or after that time process only the protected outcome-blind A/A chain and continue through the already versioned gates without skipping the completed-A/A 21-day lifecycle or exact-main CTA start-readiness assertion.

## 2026-08-26 — VEVO CTA start-readiness merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-start-readiness-state`

What changed:

- PR `#462` merged the source-complete CTA start-readiness gate into `main` as `21998faccc266b529bc47737bb11293155e721a9`.
- This handoff records the verified merge and supersedes the preceding pre-merge instruction. It introduces no experiment, billing, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#462` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` on exact head `ccf644bad76d01c9e94c0d8a6d748ff6025e3908` before merge.
- The pull request was mergeable and clean against base `a69db11d343b33e3fc682bf9df4e6555fc778a32`; `origin/main` and this state branch read back exact merge commit `21998faccc266b529bc47737bb11293155e721a9` with that base and head as its parents.
- The future CTA start now requires the completed-A/A lifecycle, CTA-only runtime, A/A stop, Pro, workspace, registry, Meta/reporting, and versioned source evidence to remain canonical and unchanged immediately before start. The checked-in waiting state remains fail-closed.
- No A/A population or result was inspected, no workflow was dispatched, and no production/external state or local runtime process changed.

Known issues:

- The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`. GrowthBook Pro remains unpurchased and the CTA experiment remains draft at zero allocation.

Next exact step:

- Keep the frozen A/A boundary closed until `2026-09-02 03:45 Europe/Bratislava`. At or after that time, record the earliest successful outcome-blind checkpoint artifact in index order, resolve the window without arm/outcome peeking, and process only the protected A/A evidence path. Continue only on an independently reproduced `PASS`; after the reviewed zero-allocation stop, request a fresh action-time confirmation immediately before the exact one-seat `$40 USD` monthly GrowthBook Pro action, then execute the completed-A/A 21-day lifecycle preflight, CTA-only runtime, and exact-main `assert-start-ready` gate before any manual CTA allocation.

## 2026-08-26 — VEVO manual A/A stop handoff made fail-closed

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-resolution-readiness`

What changed:

- Audited the static transition from the future independently reproduced A/A `PASS` to the one reviewed manual GrowthBook stop without opening any protected population or result.
- Added read-only `record_growthbook_aa_completion.py assert-stop-ready`. It revalidates the exact PASS-bound completion, activation and resolved snapshot manifests, requires the manual stop review to remain open, and requires the exact VEVO Production workspace/project to remain on Starter with only A/A live at revision `3`, `100%` traffic, `50/50`, while CTA remains an unstarted staging-only draft at `0%`.
- Strengthened the post-stop recorder to reuse the same exact running-workspace validator before it can construct the zero-allocation state. Updated the activation runbook, long-form plan, tests, and central security contract with the exact synchronized-`main` pre-action sequence and fail-closed marker.

What is verified:

- The full repository suite passes `863` Python tests and all `9` storefront JavaScript tests. The focused completion/workspace group passes `32` tests.
- Completion/workspace validators, central security validation, scoped Ruff lint, Python compilation, and `git diff --check` pass.
- The current waiting state runs the new assertion read-only and fails closed with `manual A/A stop review is not open` and exit code `2`. No file or external state is changed.
- GitHub reports workflow ID `341899955` (`Check VEVO GrowthBook Production A/A Window`) as active on the default `main` branch. As of this audit it has no workflow-run record yet; no workflow was dispatched to manufacture one before the protected boundary. The versioned same-window and exact historical-backfill paths remain available independently of the local PC.
- No A/A eligible count, arm, split, SRM, outcome, conversion, revenue, CM1, Meta dimension, performance, or result was inspected. No result workflow was dispatched and no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, product, price, stock, cart, checkout, payment, or order state changed. No browser or local runtime process remains; one unintended duplicate local unit-test process was identified by exact PID/parent/executable/command, stopped in isolation, and independently verified absent.

Known issues:

- This fail-closed manual-stop gate is not yet merged. The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`.
- The exact A/A window workflow is active but its scheduled-event delivery has not yet been observed. This is not a data-loss boundary because artifacts are retained for 90 days and the recorder supports only the next missing preregistered historical cutoff after its original daily gate closes; nevertheless, do not claim the scheduled path verified until a real metadata-only run record exists.

Next exact step:

- Commit and push this hardening, open a PR, and merge only after all required CI checks pass. Then keep the A/A result boundary closed; inspect only the exact checkpoint workflow's run metadata before the first due boundary, without downloading artifacts or reading results. At or after `2026-09-02 03:45 Europe/Bratislava`, use the earliest successful outcome-blind checkpoint in index order and continue only through the already versioned PASS/stop/Pro/lifecycle/CTA gates.

## 2026-08-26 — VEVO manual A/A stop-readiness merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-stop-readiness-state`

What changed:

- PR `#464` merged the fail-closed manual A/A stop-readiness assertion into `main` as `613ab546cf32cbcc31dbeeb937d22c7c2c34131c`.
- This handoff records the verified merge and supersedes the preceding pre-merge instruction. It introduces no experiment, billing, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#464` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` on exact head `ddcc2e3923177b966d8f930ee8a078efcadd6169` before merge.
- The pull request was mergeable and clean against base `60a16da45b6379435bbc6af741cf0bf634192905`; `origin/main` and this state branch read back exact merge commit `613ab546cf32cbcc31dbeeb937d22c7c2c34131c` with that base and head as its parents.
- The future manual A/A stop now requires exact synchronized `main`, an open PASS-bound stop review, the exact VEVO Production A/A still live at revision `3`, `100%`, `50/50`, and CTA still an unstarted staging-only draft at `0%`. The checked-in waiting state remains fail-closed.
- Existing result-blind manual infra-health run `32939220338` completed successfully on exact workflow `.github/workflows/monitor-vevo-growthbook-production-aa-infra.yml`, attempt `1`, head `dc4d02d3387528cb74c3b2a804fbb60806eb70df`, which is an ancestor of current `main`. Its only artifact `vevo-growthbook-production-aa-infra-health` had GitHub ZIP SHA-256 `b5500f43a230c3c42544d0dfbd71c06bc2aa1df254febfa023e9fb048d8c4167`, contained only the canonical JSON, produced evidence SHA-256 `773b9a7da51e1ff07b52f56a68713aaff046a9d5414738daacfbf0453486336b`, retained exact run/head provenance, and passed `validate_growthbook_aa_infra_health_evidence.py`. The temporary ZIP/extraction directory was deleted and independently confirmed absent. No duplicate monitor was dispatched.
- No A/A population or result was inspected, no result workflow was dispatched, and no production/external state or local runtime process changed.

Known issues:

- The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`. GrowthBook Pro remains unpurchased and the CTA experiment remains draft at zero allocation.
- The exact A/A window workflow remains active but its first scheduled-event run has not yet been observed; keep this as metadata-only monitoring before the frozen due boundary.

Next exact step:

- Keep the frozen A/A boundary closed. Monitor only repository-owned infra-health and exact checkpoint workflow run metadata; do not dispatch the result workflow or open an artifact before the due boundary. At or after `2026-09-02 03:45 Europe/Bratislava`, use the earliest successful outcome-blind checkpoint in index order and continue only through the versioned PASS, exact-main `assert-stop-ready`, verified zero-allocation stop, fresh Pro action-time confirmation, lifecycle, runtime, and CTA gates.

## 2026-08-26 — VEVO manual CTA stop handoff made fail-closed

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-stop-readiness`

What changed:

- Audited the future CTA assignment-stop transition without reading any A/A or CTA population, arm, performance, business outcome, or result.
- Added read-only `record_growthbook_cta_completion.py assert-stop-ready`. It accepts no output or stop-observation argument and revalidates the reviewed outcome-blind or safety stop trigger, hash-bound running activation and canonical start observation, CTA-only collector registry, exact one-seat Pro workspace, stopped A/A at `0%`, CTA as the only active Production experiment at `100%`, and the still-closed one-look final-snapshot gate.
- Made `record-stop` reuse the same full readiness validator before it can accept the independently hashed post-stop readback or construct any stopped-state output. The legacy no-subcommand recorder invocation remains mapped to `record-stop` for compatibility.
- Updated the CTA activation runbook, long-form plan, Pro workspace, tests, and central security contract with the exact clean-`main` pre-action sequence and `VEVO_CTA_STOP_READY` marker.

What is verified:

- The full repository suite passes `866` Python tests and all `9` storefront JavaScript tests. The focused activation/completion/window/final-snapshot/workspace group passes `69` tests, including both outcome-blind and safety-triggered stop paths plus registry, A/A allocation, and final-look drift rejection.
- CTA completion, measurement, safety, final-snapshot, workspace, Pro, and central security validators pass. Scoped Ruff, Python compilation, and `git diff --check` pass.
- The current checked-in waiting state executes `assert-stop-ready` read-only and fails closed with `CTA manual stop review is not open` and exit code `2`, before attempting to read the future start/stop evidence. No output file is written.
- No A/A eligible count, arm, split, SRM, conversion, revenue, CM1, Meta dimension, performance, CTA outcome, or result was inspected. No workflow was dispatched; no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, traffic, product, price, stock, cart, checkout, payment, or order state changed. No browser or local runtime process was started.

Known issues:

- This CTA stop-readiness hardening is not yet merged. The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`.

Next exact step:

- Commit and push this hardening, open a PR, and merge only after all required CI checks pass. Then return to metadata-only monitoring while the A/A boundary remains closed. At or after `2026-09-02 03:45 Europe/Bratislava`, continue only through the versioned outcome-blind A/A checkpoint/PASS/stop/Pro/lifecycle/runtime/CTA chain; immediately before a future CTA stop, require clean exact `main` and a fresh successful `VEVO_CTA_STOP_READY` assertion.

## 2026-08-26 — VEVO manual CTA stop-readiness merge verified

Date: 2026-08-26
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-cta-stop-readiness-state`

What changed:

- PR `#466` merged the fail-closed manual CTA stop-readiness assertion into `main` as `665985a28743d9c294f290d153999ccc662339b8`.
- This handoff records the verified merge and supersedes the preceding pre-merge instruction. It introduces no experiment, billing, reporting, advertising, storefront, commerce, or infrastructure mutation.

What is verified:

- PR `#466` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` on exact head `07650c0b426147003514f5b1f3e2b5069c0bb64e` before merge. GitHub readback identifies base `b20a521c95cab064b29e278496de69a733b4f96e` and exact merge `665985a28743d9c294f290d153999ccc662339b8`.
- Post-merge Env Check run `32981150703`, Observability Baseline run `32981150712`, and Build and Push ECR run `32981150767` all passed on that exact merge commit. The commit tag and `latest` are byte-identical at `sha256:d2a42e511fa385345615449c4c17ca0b96628d2ec298233f98af6df37ac4d787`; the image was built only and was not deployed.
- The future manual CTA stop now requires clean synchronized exact `main`, a reviewed outcome-blind or safety stop trigger, the canonical running CTA-only handoff, one-seat Pro workspace, A/A at zero Production allocation, unchanged registry/GTM state, and a closed final-look gate. The same validation runs again before the offline recorder can construct stopped outputs.
- The present waiting state remains fail-closed. No A/A or CTA population/result was inspected, no result workflow was dispatched, and no production/external state or local runtime process changed.

Known issues:

- The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`. GrowthBook Pro remains unpurchased and the CTA experiment remains draft at zero allocation.

Next exact step:

- Keep the frozen A/A boundary closed. Monitor only repository-owned infra-health and exact checkpoint workflow run metadata; do not dispatch the result workflow or open an artifact before the due boundary. At or after `2026-09-02 03:45 Europe/Bratislava`, use the earliest successful outcome-blind checkpoint in index order and continue only through the versioned PASS, A/A stop, fresh Pro action-time confirmation, lifecycle, runtime, CTA start, CTA stop, follow-up, and one final-look gates.

## 2026-08-27 — VEVO daily infrastructure monitor failed closed before canonical evidence

Date: 2026-08-27
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-infra-marker-diagnostic`

What changed:

- At the daily `09:00 Europe/Bratislava` readback, neither repository-owned VEVO workflow had a new scheduled-event run for 2026-08-27. Both workflows remained active on the default `main` branch.
- With no queued or in-progress infrastructure run, exactly one authorized fallback of `.github/workflows/monitor-vevo-growthbook-production-aa-infra.yml` was dispatched with `confirm_health=true` on exact `main` commit `0701486c6a1c0559b90efe7d684bb3a9165f6091` as run `33048015114`.
- The fallback selected the exact successful natural reconciliation task but failed closed before evidence construction because the expected success-marker/publish-summary log cardinality was not exactly one-to-one. It uploaded no artifact.
- The workflow now reports only the two sanitized log-line cardinalities plus `raw-messages-emitted=false` on that failure path. It still rejects every mismatch and never emits the underlying messages, reporting row counts, identities, or A/A data.

What is verified:

- The failed run reached only the result-blind infrastructure path and stopped at `Verify natural success marker and generated published parity without emitting counts`.
- The failure was classified offline as `marker_summary_cardinality_drift`; the raw failed log was not printed or stored in Git, and its local in-memory SHA-256 was `3a49c96f63077f20a204cd4b8ce25c734ee7712bc1af210fec6e9433638255c7`.
- GitHub reports zero artifacts for the failed fallback. No second fallback, A/A checkpoint dispatch, AWS query outside the managed workflow, GrowthBook action, GTM/Meta/BiznisWeb change, reporting mutation, or commerce mutation occurred.

Known issues:

- The 2026-08-27 daily infrastructure-health gate is not proven. Do not accept this day as healthy until one successful exact-main run produces the single canonical sanitized artifact and it passes independent offline verification.
- Scheduled-event delivery for both repository-owned workflows has not yet been observed for 2026-08-27. The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava` regardless of this operational blocker.

Next exact step:

- Validate this sanitized diagnostic hardening, commit and push it, and merge only after required CI passes. Then inspect only a subsequent exact-main infrastructure monitor run; do not dispatch the A/A checkpoint or read any A/A population/result before the frozen due boundary. If the cardinality gate fails again, use only the sanitized marker-line and summary-line counts to identify the producer/observer mismatch without opening raw logs or outcomes.

## 2026-08-27 — VEVO sanitized infrastructure cardinality diagnostic merged

Date: 2026-08-27
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-infra-marker-diagnostic-state`

What changed:

- PR `#469` merged the fail-closed sanitized marker/publish-summary cardinality diagnostic into `main` as `f9cae982b184fdd8aa9ee1e3ca911762fdf45256`.
- This state handoff supersedes the preceding pre-merge instruction. It does not weaken the one-marker/one-summary gate and introduces no AWS, GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, experiment, traffic, or commerce mutation.

What is verified:

- PR `#469` passed `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` on exact head `b3a78659cf36ffbf517336deb1ac2006142e381c` before merge.
- The focused infrastructure workflow/evidence suite passed `15` tests, central security validation passed, and `git diff --check` passed.
- A future cardinality failure can disclose only the marker-line and publish-summary-line counts plus `raw-messages-emitted=false`; underlying log messages, reporting row counts, identities, and A/A data remain suppressed.
- No second same-day fallback was dispatched after failed run `33048015114`, and no canonical artifact exists for that run.

Known issues:

- The 2026-08-27 daily infrastructure-health gate remains unproven because the only permitted fallback failed closed before evidence construction. Scheduled-event delivery for both repository-owned VEVO workflows was not observed at the daily readback.
- The A/A result boundary remains closed until `2026-09-02 03:45 Europe/Bratislava`. Do not use the infrastructure blocker to dispatch or inspect an A/A checkpoint early.

Next exact step:

- At the next daily post-reconciliation readback, first inspect the scheduled exact-main infrastructure run. Do not dispatch a duplicate while one is queued or in progress. If no relevant run exists after its slot, dispatch exactly one `confirm_health=true` fallback from current exact `main`; accept only a successful run with one canonical sanitized artifact. If the marker/summary gate fails again, use only the new cardinality diagnostic to identify the producer/observer mismatch and continue fail-closed without raw-log or outcome access.

## 2026-09-02 — First Production A/A checkpoint failed closed at aggregate query

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-checkpoint-athena-diagnostic`

What changed:

- At the first frozen due gate, no current-day scheduled infrastructure or checkpoint run existed and no related run was queued or in progress.
- Exactly one infrastructure fallback ran on exact `main` commit `19b8ca75dedc2b3f9861c4c7c2df4b4c44029e89` as run `33601560892`; it succeeded and its sole canonical sanitized artifact passed independent run/head, ZIP digest, one-JSON, JSON hash, and offline-validator checks. The temporary download was deleted and independently confirmed absent.
- Exactly one same-gate checkpoint fallback ran on the same exact `main` as run `33601772806`. The pre-AWS, managed-credential, runtime identity, success-marker, alarm, and DLQ gates passed, but the aggregate-only Athena query failed before evidence construction. No checkpoint artifact was uploaded.
- Added a fail-closed Athena diagnostic that requests only the structured numeric `ErrorCategory`, numeric `ErrorType`, and boolean `Retryable` fields. It explicitly never requests or emits `StateChangeReason`, SQL output, row data, identities, arms, or outcomes.

What is verified:

- The failed checkpoint stopped at `Query only the cumulative eligible-device count`; all evidence and upload steps were skipped and cleanup completed.
- GitHub reports zero artifacts for checkpoint run `33601772806`. The failed log was handled only in memory, was not printed or committed, and had SHA-256 `f8f267d0f38959c74794c66584ddbc0691d4389e15f6f61f48188c099b76434a`.
- The focused checkpoint workflow suite passes `16` tests, `scripts/security_ci.py` passes, and `git diff --check` passes.
- No arm allocation, outcome, conversion, revenue, CM1, Meta dimension, event/device/customer/order identity, GrowthBook action, GTM/Meta/BiznisWeb mutation, commerce mutation, or local AWS credential was read or used.

Known issues:

- The first checkpoint is not recorded because its protected aggregate query failed and produced no canonical artifact. The stopping decision therefore remains unresolved and the A/A experiment must remain unchanged.
- This sanitized diagnostic is not yet merged. A second checkpoint attempt is prohibited until it reaches exact `main`; it remains allowed only inside the original 24-hour checkpoint gate and only while no scheduled artifact or active run exists.

Next exact step:

- Commit and push this diagnostic, open a PR, merge only after required CI passes, synchronize exact clean `main`, and then rerun one same-gate checkpoint fallback if no scheduled artifact or active run exists. Read only the structured sanitized Athena error marker if it fails again; otherwise independently verify and record the sole canonical checkpoint artifact through `record_growthbook_aa_window_checkpoint.py` on a new branch.

## 2026-09-02 — Structured Athena fields were unavailable on the first retry

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-checkpoint-athena-reason-class`

What changed:

- PR `#480` merged the numeric-only Athena diagnostic into `main` as `82eb62d267778808d0c3f510dd7a5c579f0b3abb` after all four required checks passed.
- With no scheduled checkpoint artifact and no active run, one same-gate retry ran on that exact `main` as run `33602443977`. It again failed at the aggregate-only Athena query, before evidence construction, and uploaded no artifact.
- The structured Athena error fields were unavailable. The diagnostic now reads the failure reason only into a runner-local temporary file, maps it to a fixed allowlisted reason class, hashes it, and emits only that class/hash plus any available structured numeric fields. The raw reason is never printed, uploaded, or committed and remains covered by the unconditional cleanup step.

What is verified:

- Run `33602443977` passed the pre-AWS, managed-credential, runtime identity, marker, alarm, and DLQ gates; cleanup passed after the query failure and all evidence/upload/summary steps were skipped.
- GitHub reports zero artifacts. Its failed log was processed only in memory, was not printed or committed, and had SHA-256 `9c5e487a2402f0ce4f55571764aa7a828678faee114b72d4398e5dc3e153954f`.
- No aggregate count, arm, outcome, identity, customer/order data, or external mutation was exposed or performed.

Known issues:

- The first checkpoint remains unresolved and unrecorded. The A/A experiment must remain unchanged.
- This allowlisted reason classifier is not yet merged. Do not rerun the checkpoint until it reaches exact `main`, and then only inside the still-open original daily gate while no scheduled artifact or active run exists.

Next exact step:

- Validate, commit, push, and merge this classifier through required CI. Then synchronize clean exact `main` and make one same-gate checkpoint retry only if the repository-owned scheduled artifact is still absent. Use only the sanitized reason class/hash to repair a safely identifiable query defect; otherwise stop fail-closed without opening raw AWS payloads or experiment outcomes.

## 2026-09-02 — Checkpoint query wrapper failed before the classifier marker

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-checkpoint-athena-status-envelope`

What changed:

- PR `#481` merged the allowlisted Athena reason classifier into `main` as `cf0ffc65e88a11c73ef0b7bbba544d8adb594124` after all required checks passed.
- A delayed scheduled run `33602805439` appeared on the preceding diagnostic commit but failed at the same aggregate query and produced no artifact. With no scheduled artifact and no active run, one current-main same-gate fallback ran as `33603054826` and also failed before the classifier emitted a populated marker.
- Replaced the fragile CLI field projection with runner-local JSON envelopes for Athena query submission and status. Python validates the opaque query-ID shape without emitting the ID, validates the bounded state enum, and classifies a failed status from the same temporary payload. The unconditional cleanup boundary remains unchanged.

What is verified:

- Run `33603054826` stopped at the aggregate-only query after all infrastructure/control gates passed; no evidence or artifact was produced and cleanup passed.
- The failed log contains no populated diagnostic or raw reason. It was inspected only in memory through fixed boolean markers and was not printed or committed.
- The new wrapper never prints the query ID, full Athena status, raw reason, SQL result, identity, arm, or outcome.

Known issues:

- The first checkpoint remains unresolved and the A/A experiment must stay unchanged.
- This robust status envelope is not yet merged. No further checkpoint retry is allowed until it reaches exact `main`, no artifact exists, no run is active, and the original gate remains open.

Next exact step:

- Validate, commit, push, and merge the status envelope through required CI. Then synchronize exact clean `main` and retry the checkpoint once inside the original gate only if no canonical scheduled artifact or active run exists. Continue only with a verified canonical artifact or a fixed sanitized failure class; otherwise remain fail-closed.

## 2026-09-02 — Aggregate query succeeded but the two-row page boundary was ambiguous

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-checkpoint-athena-result-page`

What changed:

- PR `#482` merged the robust Athena submission/status envelope into `main` as `a76ba4ddd940106460e7c88e391e4c335993a49a` after all required checks passed.
- With no canonical checkpoint artifact or active run, one same-gate fallback ran on that exact `main` as run `33603774177`. The query submission marker was emitted and no failed-state classifier marker appeared; the query therefore reached the result readback and failed on the strict aggregate result shape gate.
- Increased Athena `get-query-results` page size from `2` to `3`. The canonical parser still requires exactly the header plus one aggregate row and rejects any third row or `NextToken`; the extra slot only removes the ambiguous token-at-an-exact-two-row-page boundary.

What is verified:

- Run `33603774177` emitted the populated query-submitted marker and then the populated `eligible-device aggregate result shape drift` failure. It emitted no population marker, produced zero artifacts, and cleanup passed.
- The failed log was processed only in memory, never printed or committed, and had SHA-256 `f0e067cc146e273f14bb7ea75f2b25c2bbe879b5d9711b2b77ffb77831b7141d`.
- The change does not broaden the SQL, expose the aggregate value, permit extra rows, read arms/outcomes/identities, or change external state.

Known issues:

- The first checkpoint remains unresolved and unrecorded; the A/A experiment must stay unchanged.
- The page-boundary fix is not yet merged. Do not retry until it reaches exact `main` and no artifact or active run exists inside the original gate.

Next exact step:

- Validate, commit, push, and merge this one-line page-boundary fix through required CI. Then synchronize exact clean `main` and make one final same-gate checkpoint fallback only if no canonical artifact or active run exists. Independently verify and record the artifact if successful; otherwise stop fail-closed on the new sanitized classification.

## 2026-09-02 — First outcome-blind checkpoint recorded; extend one full local day

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-window-checkpoint-1`

What changed:

- PR `#483` merged the strict three-slot Athena result page into `main` as `05bbd2154330321231119d2c5d7dd75ea7975f00` after all required checks passed.
- With no active run or canonical artifact, the final same-gate fallback ran on that exact `main` as run `33604314796`. Every protected gate passed and the run uploaded exactly one canonical artifact `vevo-growthbook-aa-window-checkpoint`.
- Independently verified the exact run/head, GitHub ZIP SHA-256 `ba0e42aa7db7341548025501fd1c7fabf23da3b4e98b933b2bbab3d006c78d8b`, one-file ZIP contract, canonical JSON SHA-256 `14add33881278dcd7ed89c3e5f0a4692c980a72ae2e2100ff087fa4e2757c7b1`, and an offline dry-run of the hash/run/head-bound recorder.
- Recorded checkpoint index `1` only through `record_growthbook_aa_window_checkpoint.py`. The cumulative eligible-device count is `769`, below the preregistered minimum `1,000`, so the frozen stopping rule extends the window by exactly one full Europe/Bratislava local day.
- Stabilized recorder/workflow unit fixtures by explicitly clearing live checkpoint history in test setup; the tests no longer inherit production history as it grows.

What is verified:

- The recorded checkpoint states `extend_one_full_local_day`; arm counts, arm outcomes, outcome metrics, event/device identities, and customer/order data were not read.
- `validate_growthbook_aa_measurement_window.py`, all `27` focused recorder/workflow tests, `scripts/security_ci.py`, and `git diff --check` pass.
- No A/A winner was called, no allocation changed, and no GrowthBook, GTM, Meta Ads, BiznisWeb, reporting, product, price, cart, checkout, payment, or order mutation occurred.

Known issues:

- The window is not resolved because checkpoint `1` has only `769` eligible devices. Protected A/A evidence, manual stop, Pro purchase, and CTA activation remain closed.
- This checkpoint record is not yet merged. The temporary artifact download must be deleted after the branch record is committed and its hashes are preserved in Git state.

Next exact step:

- Commit and push this canonical checkpoint record, open a PR, and merge only after required CI passes. Then delete and independently confirm removal of the local temporary ZIP/JSON/dry-run files. At the next frozen gate on `2026-09-03 03:45 Europe/Bratislava`, consume the earliest successful checkpoint-index `2` artifact; resolve only if its cumulative eligible-device count reaches `1,000`, otherwise extend by exactly one further full local day.

## 2026-09-02 — First outcome-blind checkpoint merge verified

Date: 2026-09-02
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-aa-window-checkpoint-1-state`

What changed:

- PR `#484` merged the canonical checkpoint-index `1` record into `main` as `112760bf434d8cc09f6bf542c6a15ed959b0e82e` after `env-check`, `secret-scan`, `observability-baseline`, and `security-baseline` passed on exact head `843380ef60f9a447f9338b8c154dc28f4a815d7a`.
- The temporary ZIP, extracted checkpoint JSON, and recorder dry-run output were deleted after the hash/run/head-bound record was committed and pushed; their temporary directory was independently confirmed absent.

What is verified:

- Exact clean `main` equals `origin/main`, and `validate_growthbook_aa_measurement_window.py` passes after the merge.
- The first stopping decision remains outcome-blind: `769` cumulative eligible devices, below the fixed `1,000` minimum, therefore exactly one full local-day extension. No arm or outcome data was opened and no external allocation or commerce state changed.

Known issues:

- The A/A window is unresolved. Protected evidence production, the manual A/A stop, GrowthBook Pro billing, and CTA activation remain closed.

Next exact step:

- At or after the next frozen gate on `2026-09-03 03:45 Europe/Bratislava`, first inspect the repository-owned scheduled checkpoint-index `2` run. Do not duplicate an active run. Use a same-gate manual fallback only if no canonical scheduled artifact exists and the original daily gate remains open; independently verify and record the earliest artifact, resolving only at a cumulative count of at least `1,000`.

## 2026-09-04 — Preview sleep requested; read-only identity preflight

Date: 2026-09-04
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-preview-sleep-preflight`

What changed:

- User requested reversible Preview suspension without deletion. Production A/A and all stored data must remain unchanged; the retained load balancer continues to incur its hourly charge.
- Added a main-only, explicitly confirmed, read-only GitHub inspection with one canonical sanitized artifact. It resolves the current Preview Fargate task/IP/service/path/image and fingerprints the Preview stacks plus protected Production stacks and schedules. It reads no event data and performs no AWS mutation.

What is verified:

- The existing Preview service template requires `DesiredCount: 1`; stopping a task alone is not a persistent suspension. Current templates have no supported sleep transition.
- No live identity or infrastructure-mutation gate is claimed before the inspection succeeds. No local AWS credentials are used.

Known issues:

- Preview is not yet suspended. Actual live state must be checked before implementing a lifecycle transition; the existing A/A checkpoint work is unchanged.

Next exact step:

- Validate and merge the read-only preflight through CI, run it once on exact main, independently verify the artifact, then implement only the reviewed no-deletion Preview sleep transition with a fresh runtime/local-host hard gate. Preserve data, ALB, Athena/Glue, reader access, Production resources, and source reporting schedule.

## 2026-09-04 — Preview read-only preflight command-default normalization

- PR #498 merged as `1f43db0f715e9de846380139dfe11325abd6e76f` after all required CI passed. Read-only run `33882218782` stopped at `runtime command drift`, with zero artifacts and no mutation.
- The initial inspector accepted an omitted command but not ECS's equivalent empty/null override. The checked-in Dockerfile supplies the exact collector server CMD. Normalize only these defaults, continue rejecting any other command, and additionally reject any entrypoint override. The artifact explicitly records whether the image default or exact explicit server command is used.
- Verified: command-default and override regression tests, existing read-only tests, security checks, and diff whitespace checks.
- Next exact step: merge this narrow inspector correction through CI, rerun read-only inspection on exact main, and stop on any actual runtime override drift. No sleep/deployment gate is open yet.

## 2026-09-04 — Preview live identity verified; no-deletion suspension prepared

Date: 2026-09-04
Repo: `vzeman/biznisweb`
Branch: `codex/vevo-preview-suspend-runtime`

What changed:

- PR #499 merged as `fa977001c8a4310e1feee175768df8453bc6a763`. Read-only run `33882657735` succeeded; its one-file artifact independently matched GitHub ZIP SHA-256 `8d5315027aa61ae65c1629e0b85821a59a5677ad42b881dbaeb7211484dfb12f` and JSON SHA-256 `74e09975724963b23867621c45ca6f6c3f027df321833c5d0f8790871f1d2021`.
- Live identity: `instance-id=N/A:Fargate`, task `16333910505e474d8be1ad7fb9ebf143`, private IP `172.31.23.149`, service `vevo-growthbook-collector-preview`, path `/app`, task definition `vevo-growthbook-collector-preview:2`, immutable digest `sha256:9478acd98a8caf06374b018c563ee51fa896b9cc92148238579f04aa28a134e1`. The command is the image default, not a runtime override. Preview remains 1/1 with public-IP assignment enabled.
- Added a separate lifecycle manifest and protected suspend-only workflow. It introduces Preview-only suspension parameters while preserving every resource, image, endpoint, data/retention rule, and Production boundary. Ordinary Preview deploys are blocked before AWS while the desired state is suspended.
- The transition requires fresh live identity plus before/after immutable localhost gates, exact template-delta equivalence, two allowlisted non-replacement change sets, service 0/0, Preview schedule disabled, unchanged inventories, and protected-resource fingerprints.

What is verified:

- The preflight read no events or outcomes, made no AWS mutation, and identified independent Production/source-schedule fingerprints. The A/A checkpoint and all experiment/billing gates remain unchanged.
- All 56 focused lifecycle/reconciliation/change-set tests, both CloudFormation template lints, security assertions, and diff checks pass. The controller preserves the exact deployed legacy Preview template and applies only the reviewed sleep fragments; it does not migrate unrelated historical template differences.

Known issues:

- Suspension is prepared, not executed. The retained load balancer and storage/monitoring remain billable. Resume requires a separately requested reviewed inverse lifecycle transition; ordinary deploy is not a resume mechanism.

Next exact step:

- Run focused tests, template lint and security checks, merge this PR after required CI, and dispatch the suspend-only workflow once on exact main within the six-hour preflight validity window. Independently verify the canonical success artifact and commit the resulting suspended-state handoff. Stop on any unreviewed drift or partial execution; never delete resources or touch Production to complete this task.

## 2026-09-04 — Suspension stopped before AWS on validator dependency conflict

- PR #500 merged as `b42dcdeeb3d9b674e1c68e26db99aa4bc162c013` after all four required checks passed. Confirmed run `33884268088` stopped during package installation before configuring AWS credentials; no diagnostic task, change set, resource mutation, or artifact was created.
- The dedicated workflow pinned PyYAML 6.0.2 while its pinned cfn-lint 1.55.1 requires PyYAML >=6.0.3. Corrected only that workflow pin to 6.0.3 and added a regression assertion. Local validation had used the already installed compatible 6.0.3.
- Next exact step: validate dependency resolution and focused tests, merge through required CI, then dispatch one fresh suspension run on exact main while the verified preflight is still within six hours and no active deployment exists. Preview remains active until a verified successful runtime artifact exists.

## 2026-09-04 — Host gates passed; non-executed change-set shape requires read-only diagnosis

- PR #501 merged as `1419dfa3153516740061cfb80daaac0360c7eb51`. Run `33884675728` passed both exact immutable Fargate localhost/marker gates and emitted `PREVIEW_SLEEP_LIVE_IDENTITY_AND_LOCALHOST_GATES_OK`, then stopped at `change set resource allowlist drift` before any change-set execution. No stack-update marker or success artifact exists; both diagnostic tasks completed/stopped through their ownership-checked cleanup.
- Added an optional read-only inspection of only the two exact Preview `preview-sleep-<run-id>` change sets. Its canonical artifact includes resource logical IDs/types, actions, replacements, property-target metadata and execution state, never resource physical IDs, property values, raw AWS payloads, events or outcomes. It independently requires no diagnostic tasks from that failed run to remain running.
- Next exact step: merge and run only this read-only diagnostic for `33884675728`, independently verify its artifact, and explain the exact plan mismatch. Do not execute an unreviewed change set or widen the resource/property allowlist to force suspension through.

## 2026-09-04 — Preserve diagnostics when a later change set was never created

- PR #502 merged as `e84520d075558b1eff9683e33b02f5ea429787c0`. Read-only diagnostic run `33885596593` stopped at its change-set exception adapter; it made no mutation and uploaded no artifact.
- The AWS service model declares the typed `ChangeSetNotFoundException` (error code `ChangeSetNotFound`), a ClientError subclass. Replace exact Python class-name matching with `isinstance`, accepting only this missing-plan code or ValidationError and retaining the first plan's sanitized diagnostic. A regression test proves no raw error message escapes and a missing second plan does not discard the first.
- Next exact step: merge this read-only adapter fix through CI and rerun diagnosis for the original failed suspension run. Do not change the suspension allowlist or execute any plan until its exact shape is independently reviewed.

## 2026-09-04 — Preserve deployed YAML intrinsic spelling; no wider change-set permission

- PR #504 merged into `5c3d4c91d2c648c663c0d84efe923a8925b37a28`. Successful read-only run `33886100678` independently matched ZIP SHA-256 `0716f489739ce828def559271b6f532117bb9d7ff290633bc2a66ee7c31dcb65` and canonical JSON SHA-256 `4bd1b76e7c304fc461bfe6b116e849ed4eb6e21c7e00bd18c6b9fc211afef7e5`.
- The collector plan remains `AVAILABLE`/unexecuted and the second plan does not exist. Both clusters have zero running diagnostic tasks from the failed run. The original Preview service/task/IP/image remain unchanged at 1/1.
- The plan included unwanted role-reference, security-group, VPC-link, policy and task-definition changes, including replacements. The serializer had converted existing YAML `!GetAtt X.Arn` scalar references into JSON array references. Although they resolve equivalently, CloudFormation treats the spelling change as property modification and propagates replacements.
- Replaced whole-YAML serialization with exact node-span edits of only the reviewed runtime/alarm values and insertion of only the new lifecycle parameter/rule/condition. All existing YAML text and intrinsic spellings remain intact; a second parsed-tree comparison still enforces the original exact delta. Original JSON templates retain their original tree representation. The resource/property/replacement allowlists are unchanged.
- Next exact step: validate text preservation against both historical deployed templates, run regression tests and lint, merge after CI, then make one fresh same-boundary suspension attempt. Never execute either the old broad plan or any new plan outside the unchanged four-resource contract.
