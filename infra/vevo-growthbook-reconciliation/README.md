# VEVO GrowthBook Preview recurring reconciliation

This stack creates only the scheduler boundary around the already deployed,
PII-free Preview experiment dataset. It does not create or change a collector,
storefront tag, GrowthBook Production rule, Meta Ads object, BiznisWeb record,
price, product, cart, checkout, order, or payment.

## Runtime contract

- schedule: `vevo-growthbook-reconcile-preview` at `03:30 Europe/Bratislava`;
- target: one exact immutable `vevo-reporting-daily` task-definition revision;
- task command: `/app/scripts/run_scheduled_growthbook_reconciliation.py`;
- processing window: the latest `40` complete UTC raw partitions;
- raw object limit: `50,000`;
- environment: `preview` only;
- publication requires both the runner's fixed `--publish` and
  `GROWTHBOOK_FACT_PUBLISH_ENABLED=true` from the reviewed target override;
- source task secrets are inherited from the exact task definition; no secret is
  stored in this template or the EventBridge target input.

The rolling window covers the seven-day order attribution window and the late
14-day cancellation/refund maturity checkpoint for a 14-day test. Curated facts
use deterministic device/page-load keys, so later daily runs refresh current
authoritative outcomes without deleting prior facts.

## Safety and monitoring

- `ScheduleState` defaults to `DISABLED`;
- the execution role can run only the exact task-definition ARN, pass only its
  exact task/execution roles, and send only to this schedule's retained DLQ;
- the target has two launch retries over one hour and execute-command is off;
- a payload-free explicit failure marker alarm catches normal runtime failures;
- a two-day missing-success alarm catches crashes or missing tasks that cannot
  emit the failure marker;
- a DLQ alarm catches Scheduler-to-ECS delivery failures;
- no policy grants `s3:DeleteObject` or any BiznisWeb mutation.

## Deployment hard gate

Use only the protected workflow. It must record the exact account, region,
source schedule, cluster, task-definition revision, image digest, task role,
execution role, container, CloudWatch log group, VPC subnets/security groups,
service name, and `/app` path before mutation. The workflow then:

1. runs the exact candidate task definition with the localhost health/marker gate;
2. creates/updates this stack with the schedule disabled, installing the DLQ and
   log metric filters before the real runtime marker is emitted;
3. runs the exact scheduled command once and requires exit `0`, the same image
   digest, and the success marker in CloudWatch;
4. enables only the schedule resource through a second validated change set;
5. reads back the exact schedule target, role, network, command, environment,
   retry policy, DLQ, alarms, and enabled state.

Do not run `cloudformation deploy` directly. The workflow must record the exact
Fargate task/private IP and `/app` marker before the schedule stack deploy, and
must not enable the schedule before the one-shot reconciliation gate passes.
