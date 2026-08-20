# VEVO GrowthBook collector

This package is the isolated, append-only browser-event collector described by `projects/vevo/GROWTHBOOK_DATA_CONTRACT.md`.

Current state: implementation, container host adapter, and local tests only. GrowthBook draft objects exist, but no AWS collector resource, endpoint, GTM tag, cookie, or production traffic allocation exists yet.

## Safety model

- `production` in `experiments.json` is deliberately empty. A separate reviewed commit is required before production can accept an experiment.
- Requests require the exact approved HTTPS origin and analytical-consent state.
- Each event type has an exact field set. Missing, unknown, nested, money, customer, raw URL, click-ID, IP, email, phone, and error-detail data fail closed.
- The server supplies receipt time, partition date, collector version, and risk result.
- S3 keys are partitioned by server receipt date and written with `IfNoneMatch="*"`; only S3 `412 PreconditionFailed` is treated as an idempotent duplicate, while a `409 ConditionalRequestConflict` is retried twice and then fails closed.
- The function has no reason to read, list, overwrite, or delete event objects. Deployment IAM must grant only conditional `PutObject` to the dedicated raw prefix.
- Browser-facing error responses are generic and never echo the payload or identifiers.

## Runtime contract

The host runtime entrypoint is:

```text
python -m growthbook_collector.server
```

The dedicated image is built from `growthbook_collector/Dockerfile`, runs as UID/GID `10001`, and exposes port `8080`. `/health` and `/marker.json` are available only through the task/ALB network; API Gateway publishes only the exact event route after the host gate. The original `handler.lambda_handler` remains a thin compatibility entrypoint for unit-level request behavior, while the Fargate adapter reuses the same validation/persistence functions. Required and optional configuration is documented in `.env.example`; secrets are not used by the browser collector.

The public route accepts only `POST` and API Gateway CORS preflight. API Gateway throttling, the private VPC integration, dedicated encrypted S3 bucket, lifecycle retention, Glue/Athena tables, alarms, and deployment hard gates are handled by the infrastructure layer and are not implied by passing unit tests.

## Verification

From the repository root:

```powershell
python -m py_compile growthbook_collector/handler.py growthbook_collector/server.py
python -m unittest tests.test_growthbook_collector tests.test_growthbook_collector_server -v
python scripts/security_ci.py
```

The focused suite covers strict schema validation, PII rejection, origin/CORS behavior, consent, UUID/time bounds, event/page compatibility, no browser monetary authority, health-event minimization, KMS arguments, conditional idempotency, and fail-closed storage errors.
