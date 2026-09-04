# Preview runtime suspension (no deletion)

The user authorized suspension only, not teardown. This is separate from the
Production A/A experiment and from GrowthBook Cloud billing. The durable desired
state is `growthbook_preview_lifecycle.json`; historical Preview observations in
`growthbook_workspace.json` remain historical evidence, not permission to wake it.

Verified suspended at `2026-09-04T15:07:52Z`: collector 0/0, Preview schedule
DISABLED. Successful managed run `33887188363` on exact main
`595a39091f990cbe4028c9ea7e83185d08f771fe` is hash-bound in the lifecycle manifest.
Its sole canonical artifact passed independent run/head, ZIP digest, content,
JSON hash, source-manifest and host-marker verification. All four diagnostic
tasks stopped; no resource was deleted/replaced and Production stayed unchanged.
The original suspend transition is now closed and cannot be replayed.

## Exactly what sleeps

- `vevo-growthbook-preview`: `PreviewSuspended=true` sets the collector service
  desired count to zero. It suppresses only the intentionally missing healthy-host
  alarm threshold/missing-data condition.
- `vevo-growthbook-reconciliation-preview`: the same parameter disables the
  Preview schedule and suppresses only its intentionally missing-success alarm.
- No resource is deleted or replaced. The API route, ALB, VPC link, task definition,
  IAM, S3 data, Glue tables, Athena workgroups, policies, logs, and DLQ remain.
  Existing retention policies are unchanged. Retained ALB/storage/monitoring still
  cost money; this does not eliminate the whole Preview monthly bill.
- Production stacks, Production reconciliation, the reporting source schedule,
  GrowthBook objects, GTM, Meta, and BiznisWeb are outside the mutation boundary.

## Execution

1. Merge a successful independently hash-verified current read-only preflight and
   lifecycle request through a reviewed PR. Inspection must be less than six hours
   old, and runtime/stack/protected-resource fingerprints must still match.
2. On clean exact main, inspect runs first; never duplicate an active deployment.
3. Manually dispatch only `suspend-vevo-growthbook-preview.yml` with
   `confirm_suspend=true`. The protected GitHub credential boundary is mandatory.
4. The workflow validates before credentials; proves the exact live Preview
   identity and two immutable localhost diagnostic tasks; rejects a running Preview
   reconciliation; derives only the checked-in sleep fields from the exact
   hash-bound deployed templates (preserving legacy Preview template differences);
   and prepares both change sets before executing either.
5. The only allowed changes are DesiredCount/healthy-host alarm and Preview
   schedule State/missing-success alarm, all Modify without replacement. Added,
   removed, unrelated, or replaced resources fail closed. Production is excluded
   both in the controller and by CloudFormation parameter rules/conditions.
6. After updates, verify service 0/0, schedule DISABLED, identical resource
   inventories, identical protected fingerprints, and repeat both immutable
   localhost probes. All diagnostic tasks are stopped; no local server is used.
7. Independently verify the sole `vevo-preview-suspended` artifact's run/head,
   GitHub ZIP digest, one-file structure, and JSON hash; commit a state readback
   through a PR, then delete only the task's temporary downloaded files.

## Failure and partial execution

Never bypass a failed gate or automatically wake Preview. If the first stack
update succeeds and the second fails, retain the data and report the exact partial
state. Do not rerun the initial transition against already changed fingerprints.
Prepare a new reviewed readback/recovery instead. Non-executed change sets may
remain as harmless audit records; no resource teardown is part of cleanup.

## Waking Preview

Waking it is a separate explicitly requested, reviewed lifecycle transition, not
an automatic consequence of a test or Pro purchase. No rebuild or deletion is
required: the exact inverse is `PreviewSuspended=false` on both existing stacks,
using the same retained task definitions, image digests, endpoint and data.

Before that future transition, collect fresh stable stack/task-definition and
protected-resource readback through managed GitHub credentials, run the exact
one-shot localhost host gates while the service is still at zero, and review
inverse change sets limited to the same four resources/properties. Verify service
1/1, target health, schedule ENABLED, unchanged data-resource identities and
Production, and localhost markers after the change. Only then may a separate PR
set desired state to active and permit ordinary Preview deploys again. The current
suspend-only workflow deliberately does not offer an unreviewed resume switch.

Preview's retained analytical dataset remains available for the planned three
Pro p75 query tests. Future live browser tests require the reviewed wake transition.
