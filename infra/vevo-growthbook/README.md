# VEVO GrowthBook AWS foundation

This stack defines, but does not activate, the PII-free experiment collector and its Athena boundary.

## Safety state

- `growthbook_collector/experiments.json` has an empty `production` registry. A production collector therefore rejects every experiment payload until a separate reviewed activation commit.
- The stack creates no IAM user, access key, GrowthBook credential, DNS record, GTM tag, BiznisWeb script, or Meta change.
- GrowthBook can receive only the managed read policy for the curated fact prefix. It cannot read raw experiment events, BiznisWeb orders, customer exports, invoices, or reporting data outside this dedicated bucket.
- The reporting policy can read raw events and publish curated facts, but it has no delete permission.
- The versioned reconciliation command is dry-run by default. Curated writes require both `--publish` and `GROWTHBOOK_FACT_PUBLISH_ENABLED=true` in the reviewed reporting runtime.
- Raw object keys are immutable: the bucket policy requires `If-None-Match`, and the collector itself sends `IfNoneMatch="*"`.
- API access logs omit IP, user agent, URL/query values, headers, and request bodies. The Lambda code does not log event payloads.

## Proposed retention defaults

- raw validated events: 180 days;
- curated anonymous device facts: 400 days;
- Athena query results: 30 days;
- API/Lambda logs: 30 days.

These are deployment parameters, not an approval record. Confirm them in the change record before the first stack deployment.

## Resources

- one private SSE-S3 bucket with public access blocked and retained on stack deletion;
- one throttled HTTP API route, `POST /v1/events`, with exact-origin CORS;
- one reserved-concurrency Lambda collector with put-only raw-prefix access;
- one Glue database with partition-projected raw and curated tables;
- isolated reporting and GrowthBook Athena workgroups with enforced result locations and scan limits;
- attachable least-privilege policies for the existing reporting runtime and the eventual GrowthBook identity;
- retained, payload-free API/Lambda logs and a Lambda error alarm.

## Required deployment hard gate

Do not deploy this template merely because it validates. Before any AWS mutation, record and verify:

1. the exact AWS account, region, stack name, collector service/function ARN target, endpoint path, reporting runtime identity, and Git commit;
2. whether the repository's host-verification rule permits this serverless design or requires a host-based collector; Lambda/API Gateway has no instance ID, host IP, service manager, or `curl localhost` surface;
3. approved retention values and the GrowthBook authentication method shown by the actual Pro workspace;
4. a CloudFormation change set with no unrelated replacement/deletion;
5. Preview deployment and marker evidence before any storefront/UI test.

The current hard-gate is intentionally unresolved for serverless deployment. This repository artifact is safe to review and validate, but it is not authorization to create or update AWS resources.

## Validation

From the repository root, after installing AWS SAM CLI in a reproducible development environment:

```text
sam validate --lint --template-file infra/vevo-growthbook/template.yaml
sam build --template-file infra/vevo-growthbook/template.yaml
```

Do not use `sam deploy` until every hard-gate item above is recorded in `PROJECT_STATE.md` and the reviewed change set matches it.
