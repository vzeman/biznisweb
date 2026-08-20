# VEVO GrowthBook AWS foundation

This stack defines, but does not activate by default, the PII-free experiment collector and its Athena boundary. The collector is a dedicated ECS/Fargate service behind an internal ALB and an API Gateway HTTP API private integration. It is not part of either reporting App Runner service.

## Safety state

- `growthbook_collector/experiments.json` has an empty `production` registry. A production collector therefore rejects every experiment payload until a separate reviewed activation commit.
- The stack creates no IAM user, access key, GrowthBook credential, DNS record, GTM tag, BiznisWeb script, or Meta change.
- GrowthBook can receive only the managed read policy for the curated fact prefix. It cannot read raw experiment events, BiznisWeb orders, customer exports, invoices, or reporting data outside this dedicated bucket.
- The reporting policy can read raw events and publish curated facts, but it has no delete permission.
- The versioned reconciliation command is dry-run by default. Curated writes require both `--publish` and `GROWTHBOOK_FACT_PUBLISH_ENABLED=true` in the reviewed reporting runtime.
- Raw object keys are immutable: the bucket policy requires `If-None-Match`, and the collector itself sends `IfNoneMatch="*"`.
- API access logs omit IP, user agent, URL/query values, headers, and request bodies. The container disables request logging and never logs event payloads.
- `PublicRouteEnabled` defaults to `false`; the API has no `POST /v1/events` route until the exact running task passes the localhost host gate.

## Proposed retention defaults

- raw validated events: 180 days;
- curated anonymous device facts: 400 days;
- Athena query results: 30 days;
- API/container logs: 30 days.

These are deployment parameters, not an approval record. Confirm them in the change record before the first stack deployment.

## Resources

- one private SSE-S3 bucket with public access blocked and retained on stack deletion;
- one throttled HTTP API with exact-origin CORS and a conditionally activated `POST /v1/events` route;
- one non-root, read-only-root-filesystem ECS/Fargate collector with put-only raw-prefix access;
- one internal ALB and API Gateway VPC link; the task accepts traffic only from the ALB and the ALB only from the VPC link;
- one Glue database with partition-projected raw and curated tables;
- isolated reporting and GrowthBook Athena workgroups with enforced result locations and scan limits;
- attachable least-privilege policies for the existing reporting runtime and the eventual GrowthBook identity;
- retained, payload-free API/container logs plus target-5xx and healthy-host alarms.

## Required deployment hard gate

Do not deploy this template merely because it validates. Before any AWS mutation, record and verify:

1. the exact AWS account `919341186960`, region `eu-central-1`, stack name, VPC, at least two subnet IDs, immutable ECR digest, service name, `/app` runtime path, endpoint path, reporting runtime identity, and Git commit;
2. a CloudFormation change set with `PublicRouteEnabled=false`, no unrelated replacement/deletion, and no Production registry activation;
3. the candidate task ID, `instance-id=N/A (ECS/Fargate)`, task private IP, service name, task definition, container name, `/app` runtime, and exact image digest;
4. a one-shot Fargate task using the exact candidate task definition and image, with its command overridden only to the versioned `host_gate.sh`; that script starts the reviewed server inside the task and runs `curl -fsS http://127.0.0.1:8080/health` plus `curl -fsS http://127.0.0.1:8080/marker.json`, including `VEVO_GROWTHBOOK_COLLECTOR_HOST_OK` and the exact commit version;
5. only after step 4, a second change set enabling the public route with the same image digest, followed by exact CORS/rejection/no-write checks; only then may a storefront Preview/UI test begin;
6. approved retention values and the GrowthBook Athena authentication method shown by the authenticated workspace.

The architecture now has a technical localhost gate. Deployment remains blocked until the exact VPC/subnets and immutable candidate image are resolved under authenticated AWS CI and the two-phase change-set workflow is used. Local AWS credentials are absent, so no resource has been created or updated.

## Validation

From the repository root:

```text
cfn-lint infra/vevo-growthbook/template.yaml
docker build -f growthbook_collector/Dockerfile -t vevo-growthbook-collector:local .
```

Do not run a direct `cloudformation deploy`. Use the protected two-phase Preview workflow so the route cannot exist before localhost verification.
