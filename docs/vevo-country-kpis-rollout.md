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

Implementation in progress. Backend, provider and modern-renderer ownership is
separate; root owns Meta capture admission, Git, runtime, publication verification
and email. No production mutation or email has occurred in this task. No local
dev server, watcher, worker or tunnel has been started.

Next: complete additive country data/rendering tests, inspect the actual native
daily-country source and any residuals, review all changes, then the Git/image/
probe/LIVE/verification/once-only email workflow above.
