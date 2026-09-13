# Standalone creditnote status guard

This is a separate release from invoice generation and unpaid-order cancellation.
The reusable guard only permits whole-order cancellation when finalized creditnotes
fully cover the current order in the same currency and tax basis. Partial credits,
missing evidence and unresolved earlier writes cannot authorize whole-order Storno.
Business writes use the shared project lease and fresh order rechecks. Native
creditnotes must bind both order identities and the final invoice number, and
be explicitly numbered, closed and nonvoided; open or ambiguous lifecycle
records remain review evidence and cannot authorize cancellation.

## Runtime and source boundary

`creditnote_storno_runner.py --project roy|vevo` runs the guard without a report or
email. `--dry-run` has no business, journal or audit writes. The live entry point
requires the configured private state object even if the current candidate set is
empty, and emits an aggregate completion only after a complete inspection.

The separate task families are `roy-creditnote-storno-guard` and
`vevo-creditnote-storno-guard`. Their task roles can access only the respective
project journal and creditnote audit and publish the established metric namespace.
Native creditnote reads also require the administrator login. Task definitions
preserve exactly the four source references for API URL/token and native username/
password, bound to the same project/account/region/runtime secret and version.
The original report sources remain pinned: ROY `roy-reporting-daily:71`, VEVO
`vevo-reporting-daily:33`, with their exact images, commands and reporting times.
The initial migration requires no existing report Scheduler Input and the exact
reviewed default report command/project. It adds only
`REPORT_SKIP_CREDITNOTE_STORNO_GUARD=true` through Scheduler input; wrappers or
unreviewed command/environment overrides are rejected before pausing. Each fresh report task has an empty local order cache; the pinned
images have no bundled cache, mounted cache or S3 order-cache restoration.

## Initial managed migration

1. Finish invoice recovery and verify the current production receipt. Integrate
   the independently reviewed uncollected-obligation closure before release. The
   order-automation deployment containing manual/closure protection must finish
   first; refresh all five invoice/cancellation task-definition and exact image
   pins from that verified runtime. The standalone migration must preserve those
   five captured pins on both success and rollback. Synchronize the clean branch
   and PROJECT_STATE, complete regression and required PR checks, then build the
   exact merged source image. Never select `latest`.
2. A read-only inspection is available from committed source:
   `python scripts/deploy_creditnote_automations.py --commit <source-SHA> --profile codex`.
   It validates both report pins, all protected schedules, private storage and
   absence of active source/A/A capture across GitHub and all account/region ECS
   clusters. It creates no infrastructure or receipt.
3. Dispatch **Deploy Creditnote Automations** once on unchanged main. It shares the
   production automation concurrency group. Freeze main until success or verified
   restoration; a queued job must still pass a fresh current-main check.
4. The migration saves private conditional-write receipts, pauses seven existing
   schedules and waits for all affected writers plus 120 quiet seconds. It runs
   two harmless probes in the original report images and two dry guards in the
   new image. Each requires actual curl localhost and its bound host marker.
5. Two actual live guard completions are required before promotion. The deployer
   rechecks current main and all source/schedule snapshots before each live launch,
   and immediately before the initial pause, then enables the two new guard schedules,
   applies the two report flags and
   restores all five invoice/cancellation schedules exactly.
6. Independently read back the private receipt, all six host outcomes and the exact
   two-report delta. Verify subsequent natural live guard/report runs. A dry-run
   marker is not a live business completion.

Receipts are under `data/roy/order-automation/creditnote-deployments/<commit>/` in
the existing private reporting bucket. They preserve original definitions and
schedules, exact own task identities, launch client tokens and host evidence.
The workflow has a 300-minute bound for six finite 30-minute task limits, initial
drain, API work, probe cleanup and restoration. It never stops natural/live business
tasks as a timeout workaround. Finite read-only probes are stopped only after exact
ownership and command checks, followed by independent STOPPED verification.

## Failure and monitoring

This controller performs the initial migration only. Existing new roles or schedules
require inspection of the previous receipt; rerunning is not a safe recovery plan.
Failed preparation restores original schedules only when their expected identities
still match. After any live attempt, reports stay paused on failure, since replaying
the old inline writer could conflict with an accepted but unconfirmed change.
Concurrent drift is never overwritten. Unused new infrastructure may remain after
a failed preparation; never restore or delete the financial journal as rollback.

ROY guard runs at 23:58 and VEVO at 23:28 Europe/Bratislava, each 92 minutes before
its pinned report. Future completion is monitored, not an enforced dependency in
the unchanged report images. Run-failure, missing-completion, review and Scheduler
DLQ alarms reuse the established operations route. The initial cutover requires
actual success for both projects. Do not manufacture success metrics to clear an
alarm. Recipient delivery is outside this guard; it sends no emails.

No local development service, tunnel or persistent worker is needed. Every critical
entry point is committed Python or a managed GitHub workflow and is reproducible
from the repository and documented AWS runtime.
