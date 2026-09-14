# VEVO production board image deployment

Use `scripts/deploy_vevo_board_image.py` in an authenticated AWS CloudShell or
an equivalently authenticated checkout. The script targets only account
`919341186960`, Frankfurt, and App Runner service
`biznisweb-vevo-production-board/2711a253ae014a8aaf1a37929997496d`.

Before use, fetch/pull and verify the repository and task branch, the reviewed
source commit and its successful ECR build, the live service ARN, image digest,
command, port, `/app` runtime path, and current DNS. App Runner has no stable
instance ID/private host IP; use N/A and record the managed service identity.
Keep an AWS browser tab marked for handoff while the user supplies login.

The two required phases are separate:

```sh
python scripts/deploy_vevo_board_image.py probe \
  --source-sha MERGED_APPLICATION_COMMIT \
  --expected-current-digest sha256:OBSERVED_OLD_DIGEST \
  --receipt data/vevo-board-deploy.json
python scripts/deploy_vevo_board_image.py promote \
  --source-sha MERGED_APPLICATION_COMMIT \
  --expected-current-digest sha256:OBSERVED_OLD_DIGEST \
  --receipt data/vevo-board-deploy.json
```

The probe uses the existing VEVO report schedule only to read its network and
execution-role/secret/log configuration. It registers a unique temporary task
definition with **no task role**, one API secret, no scheduler target and a
bounded read-only command. It verifies actual ECS identity/private IP, `/app`,
curl localhost health/marker/HTML/live API, exact active status configuration,
nonzero demand, and closes its in-process HTTP server. The task must stop with
exit 0 and its exact image digest; its definition is then deregistered.

Promotion requires a fresh successful receipt, reads the actual stopped task
and logs again, and rejects any intervening service configuration drift. It
changes only the image identifier while retaining the complete source
configuration. It does not modify IAM, business schedules, report artifacts,
order statuses, invoices or payments. It verifies terminal App Runner success,
the unchanged service boundary, and authenticated production health/HTML/API.
Browser UI verification follows these host gates.

If any API mutation has an uncertain response, stop and reconcile the saved
receipt and exact task family/startedBy or App Runner operation before retrying.
Do not delete the receipt to force a second run. An unsuccessful start may
leave a temporary definition requiring explicit cleanup; an ambiguous start
may also have created a task. Inspect exact identities before cleanup. Do not
run multiple probes concurrently for the same source commit.

Record source/digest, host task/IP, marker and counts, inactive definition,
App Runner operation and final UI evidence in `PROJECT_STATE.md`. The receipt
is generated evidence under Git-ignored `data/`; never commit credentials or
customer order details. Production remains running; local test services are
not required. Restore a previous image only after its own fresh host gate and
verification, never by an unverified automatic force update.
