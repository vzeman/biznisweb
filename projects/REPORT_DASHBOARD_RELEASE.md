# Report dashboard image promotion

Use the existing cross-platform deployment CLI with `--report-only` for ROY and
VEVO reporting pages. The default picking workflow remains separate. Picking and
operations GET requests can refresh warehouse state, so they are excluded from
this reporting mode, including PDF preview requests. No restock sentinel is used.

Finish both report controllers, their independent financial review gates and
their runtime/HTTP verification before starting this UI stage. The earlier report
verification binds the previous App Runner configuration. Both reporting leases
must now be absent or released, and neither reporting family may have a running
or pending task. Do not clear an active or uncertain lease to pass this gate.

The source must be a clean checkout of the exact merged GitHub `main` SHA with a
successful image build. Freeze that source during both UI releases. Independently
read each current App Runner image and use its immutable digest, not `latest` or
an old documented digest. The helper verifies the account, allowlisted service,
current image, `/app` command configuration and the ECR `git-<sha>` image digest.

For each project, complete probe and promotion sequentially while its host proof
is younger than 30 minutes. Use a new private receipt path for each release:

```text
python scripts/deploy_picking_batch_dashboard.py probe --report-only --project <roy|vevo> --report-to-date <YYYY-MM-DD> --source-sha <merged-40-character-sha> --expected-current-digest <sha256:current-image-digest> --receipt data/<project>-report-ui-release.json --profile codex
python scripts/deploy_picking_batch_dashboard.py promote --report-only --project <roy|vevo> --report-to-date <YYYY-MM-DD> --source-sha <merged-40-character-sha> --expected-current-digest <sha256:current-image-digest> --receipt data/<project>-report-ui-release.json --profile codex
```

`--report-to-date` binds the already published reporting period. It is used only
by the finite candidate and HTTP checks; it is not written to App Runner or a
schedule. The controller snapshots every default-group schedule and both report
leases, and rechecks them before and after image promotion. It never changes
those resources. Receipt mode, project, source, digest, period and original
service configuration must all match during promotion.

The finite Fargate candidate runs `scripts/dashboard_routing_host_gate.py`. It
receives no BiznisWeb API secret, starts a localhost-only server, reads its actual
task ID/private IP/service/path and verifies the localhost marker before reading
reports. Its application guard allows only S3 artifact reads and blocks the
operations/production helpers. The host route allowlist rejects operations and
production APIs and every POST. It is an application boundary, not a change to
the existing task role's IAM permissions.

Candidate and post-promotion HTTP checks use only `/health`, `/report/<project>`,
`/api/<project>/latest` and `/dashboard/<project>` for 7d, 30d, 90d and full periods.
They require the requested project/date/period, the unchanged strict publication
quality predicate and the corrected MER/observational labels. The host also
checks authentication and foreign-project redirects without following them.
HTML, payload and shell hashes must match between candidate and production.
No order, provider, warehouse, maintenance or report-publication write is used.

Promotion requires the candidate's terminal exit 0 and exact task/image/IP proof.
The local server must have closed; the disposable task definition must be
inactive. Only `SourceConfiguration.ImageRepository.ImageIdentifier` changes on
the existing App Runner service. All other service fields and every protected
schedule/lease are checked unchanged. Browser verification of reporting pages
comes only after this host proof and successful public HTTP verification.

Private AES256 receipts are immutable and read back byte-for-byte under
`data/<project>/report-dashboard-deployments/<source-sha>/<started-by>/<revision>-<phase>.json`.
The local receipt is a resumable pointer and must also be archived with the final
evidence. Never publish those receipts or credentials in Git. A normal promotion
ends at `deployed`; retain its App Runner operation ID and host proof.

If RunTask returns a definite failure, cleanup deactivates the owned definition
and stops any returned task only after fresh owner/definition/image checks. If
its outcome is unknown, the controller discovers tasks by its durable
`startedBy`/client token and stops only exact owned matches. It never repeats
RunTask. A task still invisible after bounded discovery leaves
`dispatch-unresolved`; preserve that receipt and investigate before any retry.
After an ambiguous update, timeout, drift or HTTP rejection, inspect the recorded
App Runner operation and actual service rather than blindly rerunning promotion
or rolling back another resource. No global local-process cleanup is used.
