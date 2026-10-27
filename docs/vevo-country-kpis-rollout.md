# VEVO country executive metrics rollout

Date: 2026-10-27
Repo: vzeman/biznisweb
Branch: codex/vevo-country-kpi-probe-stop-20261027
Base: 05ce88a21533f1f3bbae74188de0327040d79ee2

## 2026-10-27 continuation: pagination repair ready for release

The owner authorized fixing the known blocker and completing deployment, fresh
report generation and one resend. Branch codex/vevo-meta-pagination-20261027
starts from main9d3aaca. The recorder and independent verifier now separate native
paging metadata (explicit v21/v24 allowlist) from unchanged v21 request paths.
Both preserve exact Graph/account/insights scope, query parameters, directional
cursor chains and secret handling. No provider query or economic formula changes.

179 focused tests pass, including hostile metadata and500+1 row regressions.
Independent review and critical lint pass. A fresh read-only full-range native
check accepted500+101 rows over two pages, including v24 previous/before metadata;
proofSHA ccacdcfd99f3f6712aa76acb72c310a6f5d22ca2d7d874c6c9261d0f94afe260.
This is local admission evidence only, not production-host or publication approval.
Fresh preflight retains55 ENABLED, no running task/IP, /app, FargateN/A,
account919341186960, unchanged ROY83/monthly67/AppRunner4fec and old generation.

Next: PR/CI/exact-source image, fresh release ID, actual probe/LIVE verification,
source/CSV/cards/HTTP/native UI and one conditional SES resend. The narrow repair
does not trigger the monthly accounting workflow. The failed release below is
historical; it will not be retried under the old ID. No production change yet.

## Current outcome: stopped before LIVE, production update and email incomplete

PR651 merged as2820e2679b35f0e27ae52e42f2f688910d236e88 and its exact-source
reporting image built successfully. A real controlled release was attempted:
8f1ad088f28845d2b8462b1776736538 registered candidate56/imagee322 and dispatched
one probe720d30756ba740559c6ad5183657ec9a. Its host and localhost identity gates
passed before provider reads: instanceN/A:Fargate, privateIP172.31.4.66,
servicevevo-daily-report-email,path/app. Restoration finished at04:07:38UTC after the probe stopped with exit1.
No LIVE run, new production country-card generation or requested SES resend
occurred. HTTP and native browser verification of a new generation did not run.
The owner was informed and work stopped under the error-stop instruction. No
code repair, automatic retry, UI attempt or email followed this failure.

Fresh direct-SDK restoration proof
4aabaf92b270184ae5feb79f03c3b6f81f2fb210719c714a311975edb100cae1 verifies
original55/imagec3ccd24c and its ENABLED dynamic schedule exactly match release
preflight, candidate56 is INACTIVE, all13 schedules including12 protected are
unchanged, and ROY lease/outputs remain exact. VEVO generation20261027T003047Z is
byte-identical to baseline. The temporary probe role is removed, own lease is
released under8f, no ROY/VEVO tasks are active and AppRunner4fec is RUNNING with
unchanged configuration. Cleanupfd4dfef56f7df672aff9a1e74700afc03496290e4c8184c48cea646e72bca909
confirms exact controller Python28280/wrapper23720/diagnosis shell29228 absent, zero process kills,
dev services or native UI actions. Private failure archive sealing is complete, as recorded below.
The implementation is in Git but must not be described as deployed reporting.

The original probe failed with probe-meta-source-incomplete-or-rejected. Its
specific inner predicate and original rejected full page were not retained.
Root's fresh local, read-only
two-GET diagnosis specifically rejects probe-meta-source-account-version-path
and reproduces this current concrete blocker: the full-range
country_daily request targets Graphv21.0, while the returned native paging.next
uses HTTPS graph.facebook.com, the same act_1131531703632101 account and insights
endpoint, but names/v24.0/. In this fresh reproduction, the recorder admits
onlyv21 endpoints and rejects the first500-row full-range page before its source
PUT. The provider uses its
cursor instead of following the supplied next URL. Diagnosis SHA256
1cddf74662815c2d640b679b9deaa91c56d7299ec19aa7375b512a97685986c0 is explicitly
a fresh local admission reproduction consistent with the original trace, not
original-probe raw-response or same-host proof. Short7d/30d/90d country_daily
ranges were retained; full history is incomplete. All four original global QA
records have warning status with zero failures. New country/source/CSV/HTTP/native
UI verification did not run or pass. Global QA and offline helper tests cannot
replace the missing complete native source and do not authorize publication.

The monthly dependency is separate: CI40442777562 deployed67/image64c3 and its
actual task543a/IP172.31.3.197/path/app dryrun stopped0 with no actual mail,
retaining proofa0eab98. That successful smoke does not establish VEVO LIVE success.
All country publication/email/HTTP helper tests remain offline preparation; no
actual LIVE, HTTP, native UI or email acceptance is inferred from them. No local
dev server, watcher, worker or tunnel was started.

Private evidence: owner/account919341186960, eu-central-1, bucket
biznisweb-reporting-artifacts-919341186960-eu-central-1. Encrypted archive key:
`data/vevo/reporting/country-kpi-20261027/8f1ad088f28845d2b8462b1776736538/evidence.zip`
SHA256 `9db2df57723ae89349b269bb5afdd5257ba8c406840efe3f6603f21b80e1fc2e`,
3,943,768bytes,213members. Same-prefix `receipt.json` SHA256
`a6f2751a2faff764f61a22af1c4f98ca3691db4995b2accfb173dafcbd54352e`.
Conditional owner-bound AES256 PUTs, full GET/readback and each ZIP member hash
were verified. No new source approval, publication or email acceptance is implied.
The finite uploader shell38144/Python6728 exited0 naturally; fresh checks confirm
both absent. No kill, persistent local service or UI input action occurred.

Next exact step on a future continuation: narrowly repair admission of the
observed pagination-source metadata while retaining the original v21 request,
exact HTTPS Graph origin/account/insights path, query scope, native cursor chain
and credential protections. Do not follow paging.next, accept arbitrary API
versions/hosts, change economic provider queries, rescale amounts, or widen the
existing five-cent reconciliation tolerance. Add actual sanitized pagination
fixtures and malicious-origin/account/query/version regression cases, review
through PR/CI and an exact-source image build, then use a fresh normal release ID
for an isolated probe and LIVE. Require all four actual country_daily ranges,
source/CSV/fixed/CAC/card checks, authenticated HTTP and native UI, then exactly
one conditional email resend to existing recipients. Preserve the failed RID and
baseline; do not blindly re-dispatch it. The preceding approved implementation
and feasibility work are retained below as historical preparation, not the
current production outcome. Private HTTP preparations bound to source2820 and
failed RID8f are historical helpers, not authorization or source evidence for a
future fresh release; rebind and review the new source/identity inputs explicitly.

## Requested outcome

Keep the existing nine executive metrics first. Immediately below, show the same
nine metrics separately for SK, CZ and HU, including daily/weekly/monthly/all-time
selection, comparisons and trends. Publish the new generated report to the live
report route and send it once to its existing recipients. This request authorizes
a normal complete regeneration through 2026-10-26; it is distinct from the prior
unsupported single-day replacement request. Preserve order eligibility and all
approved cost policies.

## Verified starting boundary

Fresh AWS readback: account919341186960/eu-central-1, reporting service
vevo-daily-report-email, clustervevo-reporting-cluster, definition55 ACTIVE,
Fargate instance identity N/A, working directory/app, no active report task or
current private IP. Immutable image:
sha256:c3ccd24ccfc3b229a67a1f5b96fcf3c3d955924cef32b45e9a4a6f1d7fc5b987.
Original schedule ENABLED, cron(0 1 * * ? *), Europe/Bratislava, dynamic yesterday.
New probe and LIVE must independently prove their actual task/IP/service/path,
localhost marker and source before provider execution.

The existing AppRunner biznisweb-vevo-production-board is RUNNING at
2mhmsmgq3m.eu-central-1.awsapprunner.com on immutable image4fec00b3. The supplied
image is the generated modern report, not the separate operations-board cards.
The /report/vevo route serves S3 HTML, so this change does not require an
AppRunner image promotion.

## Implementation boundary

Reuse the CFO window/metric calculation over explicit daily country frames and
the same complete calendar as the overall report. Attach countries and
country_order to the CFO payload; serialize only the bounded country payloads
and reuse one card renderer and one period control for all four grids.
Activation is client-owned configuration: country_executive_kpis_enabled is true
only in VEVO settings and additionally requires measured-country advertising.
The reusable exporter has no hardcoded merchant-name branch for this feature;
ROY and other clients remain opted out.

Current country advertising contains period totals only. Add separate native
daily-country provider reads and a distinct Meta source-capture query kind;
preserve existing daily and period-country queries, original raw evidence,
reconciliation tolerance and overall accounting. Never infer daily country spend
from order shares or rescale native amounts. Retain unknown/unallocated residuals.
Unavailable advertising must produce unavailable dependent metrics, not zero.

Use global customer first-purchase dates for CAC. Allocate the single existing
fixed pool once and pass zero fallback to each country CFO builder. Include
order-level credit adjustments once and attribute actual excluded credited-order
fulfillment costs or retain an explicit residual. Include zero-order calendar
days with their advertising. Preserve overall and ROY results.

## Git and runtime workflow

The shared old checkout has a broken internal refs/codex/turn-diffs checkpoint
that prevents fetch. No shared ref or untracked primary file was changed. A
separate filtered clone from GitHub is the working repository for this feature;
fetch/pull, clean status and the feature branch were verified before edits.

Review/test/commit/push small changes, use a PR for main, then exact-source image
build and the normal vevo_report_image_release controller. Its isolated probe and
LIVE already regenerate full/7d/30d/90d; do not add a redundant same-image rerun.
Changes under reporting_core also trigger monthly-accounting deployment: confirm
that target before merge and wait for its terminal host/dryrun verification before
capturing protected runtime boundaries. Preserve all other schedules and ROY.

After new publication verify country/window arithmetic, full CSV/global financial
invariance, original quality gates, host/localhost, HTTP and browser. Then prepare
the exact generation email, verify the known creditnote denominator wording,
retain existing sender/recipients privately and use one conditional intent plus
one SES attempt. Record MessageId and distinguish acceptance from delivery.

## Implementation and pre-attempt evidence (superseded status)

Before the failed attempt, implementation was committed and pushed in PR651. The original provider
methods are AST-identical to base05ce88; country sources, country CFO calculation
and modern rendering are additive. Only the email creditnote summary display
changes its pre-existing mismatched cohort denominator; financial rates and
sender/recipient behavior are unchanged. Metadata is bounded and sanitized;
unavailable sources show N/A with a short country notice.

Root integration315 tests passed; final frontend/email34 tests passed after the
metadata follow-up. Independent provider/Meta/backend130-test review passed.
Reporting smoke, critical Ruff and workflow YAML checks passed. All six current
PR checks pass, including Windows/Linux regression. New country/source/email
tests are wired into PR and image gates. Do not add these overlapping counts.

Actual read-only provider feasibility through2026-10-26 succeeded for542 days,
both daily and original period sources. Proofd4de80727f1dc46b1fdfca9ff5cac7a8168acacd01aa2b6c89d02f71491e138b.
Native Meta daily-minus-period amounts are SK+0.120205EUR, CZ−0.044675EUR,
HU−0.075525EUR, unknown−0.000002EUR; Google deltas are zero. These distinct native
query granularities are preserved in metadata, never rescaled or used to expand
the existing reconciliation tolerance. This preflight does not approve any later
probe or LIVE source snapshot. Actual release review must require all four
country_daily ranges and usable country decks; legacy recorder compatibility
alone is insufficient.

The pre-existing baseline generation20261027T003047Z was captured with eight
manifest artifacts plus original CSV, global daily aggregate and QA (eleven
objects); full hash/size/encryption/owner checks pass and latest stayed unchanged.
CSV retains customer_first_purchase_date for independent country CAC checking.

Monthly dependency hard-gate is also confirmed: monthly-creditnote-export:66,
image8203526ce14b04ff955aea058ff9bb14f0dff24e011c192427615516d3fc56bb,
FargateN/A/no active IP, actual immutable OCI configuration /app. Proofb3f2c04a5750fb5cc7d35466fddc551f43f305e8bcb6272ec33cd238796d3060.
The first read-only helper wrongly required one ECR image entry; AWS returned two
tags for the same byte-identical digest. Narrow V2 verifies every entry/digest and
the unique OCI manifest/config hash. Original failed helper is preserved; no
runtime or financial change occurred.

At that pre-attempt checkpoint no production mutation or email had occurred.
The controlled probe attempt and rollback above supersede that earlier runtime
status. No local dev server, watcher, worker or tunnel was started.

The pre-attempt plan was to finish the verifier/resend tests, CI/merge/build and
monthly smoke, then run isolated probe/LIVE followed by HTTP/browser and email.
CI/merge/build and monthly smoke subsequently passed; the isolated probe failed.
The current next step is the narrow source-admission repair specified above.

The verifier and resend CLI are now committed at3bf9303. Root final release pack
passed215 tests; the independent verifier/email helper pack passed43 tests.
Windows CI40441785685 rejected the resend test fixture's noncanonical temporary
root; production ROOT is already resolved and the actual path guard correctly
refused that alias. The fixture now resolves its root exactly as production does.
Traversal/reparse checks and all send guards are unchanged. The failed CI log is
retained. Replacement Windows and Linux CI later passed before merge. No runtime
or email action occurred while investigating that test-only mismatch; it is not
the cause of the subsequent probe failure.
