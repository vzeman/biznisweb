# VEVO country executive metrics rollout

Date: 2026-10-27
Repo: vzeman/biznisweb
Branch: codex/vevo-country-kpi-20261027
Base: 05ce88a21533f1f3bbae74188de0327040d79ee2

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

## Current state and next exact step

Implementation is committed and pushed in draft PR651. The original provider
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

No production mutation or email has occurred in this task. No local dev server,
watcher, worker or tunnel has been started; finite tests/readers ended naturally.

Next: complete the independent publication verifier and portable once-only mail
CLI/tests, then final PR checks/review/merge. Wait for exact-source image build
and monthly terminal smoke, freeze the normal release source, run isolated
probe/LIVE with actual source/card verification, then HTTP/browser and one email.
