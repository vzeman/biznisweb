#!/usr/bin/env python3
"""Promote only host-verified, immutable order automation tasks.

The five existing schedules are the authority for networking and credentials.
No customer data or AWS response is printed or written to a local artifact.
Rollback snapshots and host identities are saved only in the existing private
reporting bucket. This entry point is used by the main-only managed workflow.
"""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
SERVICES = {
    "roy-invoice-daily": ("roy", "invoice", "roy-daily-invoice-generation"),
    "vevo-invoice-daily": ("vevo", "invoice", "vevo-daily-invoice-generation"),
    "roy-unpaid-order-cancellation": ("roy", "cancellation", "roy-unpaid-order-cancellation"),
}
SCHEDULES = {
    "roy-daily-invoice-generation": "roy-invoice-daily",
    "roy-same-day-invoice-sweep": "roy-invoice-daily",
    "vevo-daily-invoice-generation": "vevo-invoice-daily",
    "vevo-same-day-invoice-sweep": "vevo-invoice-daily",
    "roy-unpaid-order-cancellation": "roy-unpaid-order-cancellation",
}
SCHEDULE_FIELDS = (
    "Name", "GroupName", "Description", "StartDate", "EndDate", "ScheduleExpression",
    "ScheduleExpressionTimezone", "State", "FlexibleTimeWindow", "Target", "KmsKeyArn",
    "ActionAfterCompletion",
)
TASK_FIELDS = (
    "family", "taskRoleArn", "executionRoleArn", "networkMode", "containerDefinitions",
    "volumes", "placementConstraints", "requiresCompatibilities", "cpu", "memory",
    "pidMode", "ipcMode", "proxyConfiguration", "inferenceAccelerators", "ephemeralStorage",
    "runtimePlatform", "enableFaultInjection",
)
SECRET_NAMES = {
    "BIZNISWEB_API_TOKEN", "BIZNISWEB_API_URL", "BIZNISWEB_USERNAME", "BIZNISWEB_PASSWORD",
    "REPORT_S3_BUCKET", "REPORT_S3_PREFIX",
}
MARKER = "ORDER_AUTOMATION_HOST_OK"


def require(condition: bool, code: str) -> None:
    if not condition:
        raise RuntimeError(code)


def schedule_request(schedule: dict) -> dict:
    return {key: copy.deepcopy(schedule[key]) for key in SCHEDULE_FIELDS if key in schedule}


def command_for(family: str) -> list[str]:
    project, kind, _ = SERVICES[family]
    runner = "invoice_runner.py" if kind == "invoice" else "unpaid_order_cancellation_runner.py"
    return ["python", runner, "--project", project]


def established_alarm_route(session, account: str) -> str:
    """Reuse the verified reporting operator route; never subscribe or publish."""
    topic = f"arn:aws:sns:eu-central-1:{account}:vevo-reporting-alerts-mil-final"
    names = ["roy-reporting-run-failed", "vevo-reporting-run-failed"]
    alarms = session.client("cloudwatch").describe_alarms(AlarmNames=names).get("MetricAlarms", [])
    require({alarm.get("AlarmName") for alarm in alarms} == set(names) and len(alarms) == 2,
            "operator-alarm-route-missing")
    require(all(alarm.get("ActionsEnabled") is True and alarm.get("AlarmActions") == [topic] for alarm in alarms),
            "operator-alarm-route-drift")
    sns = session.client("sns")
    require(sns.get_topic_attributes(TopicArn=topic).get("Attributes", {}).get("TopicArn") == topic,
            "operator-topic-identity-drift")
    rows, token = [], None
    for _ in range(20):
        request = {"TopicArn": topic}
        if token:
            request["NextToken"] = token
        page = sns.list_subscriptions_by_topic(**request)
        rows.extend(page.get("Subscriptions", []))
        next_token = page.get("NextToken")
        if not next_token:
            break
        require(next_token != token, "operator-topic-pagination-cycle")
        token = next_token
    else:
        raise RuntimeError("operator-topic-pagination-incomplete")
    require(len(rows) == 1 and rows[0].get("TopicArn") == topic and rows[0].get("Protocol") == "lambda"
            and rows[0].get("Endpoint", "").startswith(f"arn:aws:lambda:eu-central-1:{account}:function:")
            and rows[0].get("SubscriptionArn", "").startswith(topic + ":"), "operator-topic-subscription-drift")
    return topic


def candidate_definition(source: dict, family: str, image: str, report_location: tuple[str, str] | None = None) -> dict:
    require(family in SERVICES and source.get("family") == family, "task-family-drift")
    require(re.fullmatch(r"[0-9]{12}\.dkr\.ecr\.eu-central-1\.amazonaws\.com/vevo-reporting@sha256:[a-f0-9]{64}", image) is not None,
            "image-not-immutable")
    result = {key: copy.deepcopy(source[key]) for key in TASK_FIELDS if key in source}
    containers = result.get("containerDefinitions", [])
    require(len(containers) == 1 and containers[0].get("name") == "reporting", "container-drift")
    container = containers[0]
    require(container.get("command") == command_for(family), "source-command-drift")
    require(source.get("networkMode") == "awsvpc", "source-network-mode-drift")
    project = SERVICES[family][0]
    container.update(image=image, command=command_for(family), workingDirectory="/app")
    environment = {item["name"]: item["value"] for item in container.get("environment", [])}
    environment.update(REPORT_PROJECT=project, REPORT_SKIP_PROJECT_ENV="true",
                       ORDER_AUTOMATION_STATE_PREFIX=f"data/{project}/order-automation")
    # Do not carry one-off date/dry-run overrides into a scheduled live runner.
    for name in ("INVOICE_REFERENCE_DATE", "INVOICE_FROM_DATE", "INVOICE_TO_DATE",
                 "UNPAID_CANCELLATION_REFERENCE_DATE", "REPORT_INVOICE_DRY_RUN",
                 "REPORT_UNPAID_CANCELLATION_DRY_RUN", "ORDER_AUTOMATION_STATE_BUCKET"):
        environment.pop(name, None)
    if report_location:
        environment.update(REPORT_S3_BUCKET=report_location[0], REPORT_S3_PREFIX=report_location[1])
    container["environment"] = [{"name": name, "value": value} for name, value in sorted(environment.items())]
    container["secrets"] = [item for item in container.get("secrets", []) if item["name"] in SECRET_NAMES
                            and not (report_location and item["name"] in {"REPORT_S3_BUCKET", "REPORT_S3_PREFIX"})]
    return result


def desired_schedule(snapshot: dict, task_arn: str, dlq_arn: str, settings: dict) -> dict:
    name = snapshot["Name"]
    require(name in SCHEDULES, "schedule-not-allowed")
    family = SCHEDULES[name]
    require(f":task-definition/{family}:" in task_arn, "schedule-task-family-mismatch")
    kind = SERVICES[family][1]
    raw = settings["invoice_generation" if kind == "invoice" else "unpaid_order_cancellation"]
    sweep = "same-day-invoice-sweep" in name
    expected_name = raw["final_sweep_schedule_name" if sweep else "schedule_name"]
    require(expected_name == name, "settings-schedule-name-drift")
    expression = raw["final_sweep_schedule_expression" if sweep else "schedule_expression"]
    project = SERVICES[family][0]
    expected = ("cron(59 23 * * ? *)" if project == "roy" else "cron(58 23 * * ? *)") if sweep else (
        "cron(10 2 * * ? *)" if kind == "cancellation" else
        "cron(5/15 * * * ? *)" if project == "roy" else "cron(0/15 * * * ? *)"
    )
    require(expression == expected and raw.get("timezone") == "Europe/Bratislava", "settings-schedule-cadence-drift")
    result = schedule_request(snapshot)
    result.update(ScheduleExpression=expression, ScheduleExpressionTimezone="Europe/Bratislava",
                  FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED")
    result["Target"]["EcsParameters"]["TaskDefinitionArn"] = task_arn
    result["Target"]["DeadLetterConfig"] = {"Arn": dlq_arn}
    # Delivery retries concern ECS launch, not application success. A durable
    # application journal handles retries after an accepted launch.
    result["Target"]["RetryPolicy"] = {"MaximumEventAgeInSeconds": 900, "MaximumRetryAttempts": 2}
    result["Target"].pop("Input", None)
    return result


def promote_schedules(scheduler, originals: dict, desired: dict) -> None:
    require(set(originals) == set(SCHEDULES) == set(desired), "incomplete-promotion-set")
    for name, original in originals.items():
        current = scheduler.get_schedule(Name=name)
        require(schedule_request(current) == schedule_request(original), "schedule-changed-before-promotion")
    attempted = []
    try:
        for name, request in desired.items():
            if request == schedule_request(originals[name]):
                continue
            # Include ambiguous API outcomes in rollback, not only acknowledged writes.
            attempted.append(name)
            scheduler.update_schedule(**request)
            current = scheduler.get_schedule(Name=name)
            require(schedule_request(current) == request, "schedule-promotion-readback-mismatch")
    except Exception:
        rollback_failed = False
        for name in reversed(attempted):
            try:
                current = schedule_request(scheduler.get_schedule(Name=name))
                original = schedule_request(originals[name])
                # Never overwrite a concurrent change from another operator.
                require(current in (desired[name], original), "rollback-concurrent-change")
                if current != original:
                    scheduler.update_schedule(**original)
                require(schedule_request(scheduler.get_schedule(Name=name)) == original, "rollback-readback-mismatch")
            except Exception:
                rollback_failed = True
        if rollback_failed:
            raise RuntimeError("schedule-rollback-requires-operator") from None
        raise RuntimeError("schedule-promotion-failed-rollback-verified") from None


class Deployment:
    def __init__(self, session, commit: str):
        self.session, self.commit = session, commit
        self.ecs = session.client("ecs")
        self.scheduler = session.client("scheduler")
        self.logs = session.client("logs")
        self.account = session.client("sts").get_caller_identity()["Account"]
        self.evidence = {"schema": 1, "commit": commit, "hosts": [], "created_at": datetime.now(timezone.utc).isoformat()}
        self.snapshot_key = f"data/roy/order-automation/deployments/{commit}/{uuid.uuid4().hex}.json"

    def source_bucket(self, definition: dict) -> str:
        container = definition["containerDefinitions"][0]
        env = {item["name"]: item["value"] for item in container.get("environment", [])}
        value = env.get("REPORT_S3_BUCKET")
        if not value:
            refs = [item["valueFrom"] for item in container.get("secrets", []) if item["name"] == "REPORT_S3_BUCKET"]
            if not refs:
                return ""
            require(len(refs) == 1, "state-bucket-reference-ambiguous")
            ref = refs[0]
            if ref.startswith(f"arn:aws:ssm:eu-central-1:{self.account}:parameter/"):
                value = self.session.client("ssm").get_parameter(Name=ref, WithDecryption=True)["Parameter"]["Value"]
            else:
                parts = ref.split(":")
                require(len(parts) == 10 and parts[:5] == ["arn", "aws", "secretsmanager", "eu-central-1", self.account]
                        and parts[5] == "secret" and parts[7:] == ["REPORT_S3_BUCKET", "", ""], "state-bucket-reference-type")
                # ECS references this exact JSON key. The secret stays in memory;
                # neither the bundle nor its other fields enter evidence/logs.
                secret = self.session.client("secretsmanager").get_secret_value(SecretId=":".join(parts[:7]))
                value = json.loads(secret["SecretString"])["REPORT_S3_BUCKET"]
        require(isinstance(value, str) and (not value or re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", value) is not None),
                "state-bucket-invalid")
        return value

    def bucket(self, definition: dict) -> str:
        """Bind private state to the current reporting runtime and reviewed config.

        Older invoice task secrets can contain an empty bucket; that is not the
        reporting service's explicit runtime bucket. Never invent a destination.
        """
        from reporting_core.storage import resolve_report_s3_location
        family = definition.get("family")
        require(family in SERVICES, "storage-task-family-drift")
        project = SERVICES[family][0]
        settings = json.loads((ROOT / "projects" / project / "settings.json").read_text(encoding="utf-8"))
        configured = resolve_report_s3_location(project, settings, environ={})
        name = f"{project}-daily-report-email"
        require(settings["report_schedule"]["schedule_name"] == name, "report-storage-schedule-drift")
        schedule = self.scheduler.get_schedule(Name=name)
        require(schedule.get("Name") == name and schedule["Target"]["Arn"] ==
                f"arn:aws:ecs:eu-central-1:{self.account}:cluster/vevo-reporting-cluster", "report-storage-cluster-drift")
        task_arn = schedule["Target"]["EcsParameters"]["TaskDefinitionArn"]
        require(task_arn.startswith(f"arn:aws:ecs:eu-central-1:{self.account}:task-definition/{project}-reporting-daily:"),
                "report-storage-task-drift")
        reporting = self.ecs.describe_task_definition(taskDefinition=task_arn)["taskDefinition"]
        containers = reporting.get("containerDefinitions", [])
        require(reporting.get("family") == f"{project}-reporting-daily" and len(containers) == 1
                and containers[0].get("name") == "reporting", "report-storage-container-drift")
        env = {row["name"]: row["value"] for row in containers[0].get("environment", [])}
        require(not any(row["name"] in {"REPORT_S3_BUCKET", "REPORT_S3_PREFIX"}
                        for row in containers[0].get("secrets", [])), "report-storage-secret-overrides-env")
        actual = (env.get("REPORT_S3_BUCKET"), env.get("REPORT_S3_PREFIX"))
        require(actual == configured and all(actual), "report-storage-config-runtime-mismatch")
        source = self.source_bucket(definition)
        require(not source or source == actual[0], "automation-report-storage-mismatch")
        s3 = self.session.client("s3")
        s3.head_bucket(Bucket=actual[0], ExpectedBucketOwner=self.account)
        require(s3.get_bucket_location(Bucket=actual[0], ExpectedBucketOwner=self.account).get("LocationConstraint")
                == "eu-central-1", "report-storage-region-drift")
        block = s3.get_public_access_block(Bucket=actual[0], ExpectedBucketOwner=self.account)["PublicAccessBlockConfiguration"]
        require(all(block.get(key) is True for key in (
            "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets",
        )), "report-storage-public-access-not-blocked")
        self.evidence.setdefault("storage_bindings", {})[project] = {
            "report_schedule": name, "report_task_definition": task_arn, "bucket": actual[0], "report_prefix": actual[1],
        }
        return actual[0]

    def save_private(self, bucket: str) -> None:
        self.session.client("s3").put_object(
            Bucket=bucket, Key=self.snapshot_key, Body=json.dumps(self.evidence, default=str, sort_keys=True).encode(),
            ContentType="application/json", ServerSideEncryption="AES256", ExpectedBucketOwner=self.account,
        )

    def prepare_state(self, project: str, bucket: str, role_arn: str) -> None:
        from invoice_automation_state import S3AutomationStateStore
        iam = self.session.client("iam")
        role, name = role_arn.rsplit("/", 1)[1], f"OrderAutomationState-{project}"
        previous = None
        try:
            previous = iam.get_role_policy(RoleName=role, PolicyName=name)["PolicyDocument"]
        except Exception as exc:
            require(getattr(exc, "response", {}).get("Error", {}).get("Code") == "NoSuchEntity", "state-policy-read-failed")
        key = f"data/{project}/order-automation/state.json"
        policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
                  "Resource": f"arn:aws:s3:::{bucket}/{key}"}]}
        self.evidence.setdefault("state_policies", []).append({"role": role, "name": name, "previous": previous, "candidate": policy})
        self.save_private(self.evidence_bucket)
        client = self.session.client("s3")
        empty = S3AutomationStateStore(client, bucket, key, project).empty_state()
        try:
            # Bootstrap the exact new key without requiring broad ListBucket.
            # Existing journals, including live leases, are never overwritten.
            client.put_object(Bucket=bucket, Key=key, Body=json.dumps(empty, sort_keys=True).encode(),
                              ContentType="application/json", ServerSideEncryption="AES256", IfNoneMatch="*",
                              ExpectedBucketOwner=self.account)
        except Exception as exc:
            require(getattr(exc, "response", {}).get("Error", {}).get("Code") in {"PreconditionFailed", "412"}, "state-initialization-failed")
        iam.put_role_policy(RoleName=role, PolicyName=name, PolicyDocument=json.dumps(policy))

    def restore_state_policies(self) -> None:
        failed = False
        iam = self.session.client("iam")
        for item in reversed(self.evidence.get("state_policies", [])):
            try:
                try:
                    current = iam.get_role_policy(RoleName=item["role"], PolicyName=item["name"])["PolicyDocument"]
                except Exception as exc:
                    if getattr(exc, "response", {}).get("Error", {}).get("Code") == "NoSuchEntity" and item["previous"] is None:
                        continue
                    raise
                require(current in (item["candidate"], item["previous"]), "state-policy-concurrent-change")
                if item["previous"] is None:
                    iam.delete_role_policy(RoleName=item["role"], PolicyName=item["name"])
                else:
                    iam.put_role_policy(RoleName=item["role"], PolicyName=item["name"], PolicyDocument=json.dumps(item["previous"]))
                try:
                    restored = iam.get_role_policy(RoleName=item["role"], PolicyName=item["name"])["PolicyDocument"]
                    require(restored == item["previous"], "state-policy-rollback-readback")
                except Exception as exc:
                    require(getattr(exc, "response", {}).get("Error", {}).get("Code") == "NoSuchEntity" and item["previous"] is None,
                            "state-policy-rollback-readback")
            except Exception:
                failed = True
        require(not failed, "state-policy-rollback-requires-operator")

    def host_gate(self, family: str, task_arn: str, schedule: dict, digest: str, *, old_image: bool = False) -> dict:
        project, kind, _ = SERVICES[family]
        network = schedule["Target"]["EcsParameters"]["NetworkConfiguration"]["awsvpcConfiguration"]
        network = {key[0].lower() + key[1:]: value for key, value in network.items()}
        if old_image:
            # The pre-upgrade image has no new gate file. Send the exact checked-in
            # stdlib host helper as its candidate-only command, never a shell or
            # an untracked local script. Application runners remain the old image's.
            source = (ROOT / "scripts/order_automation_host_gate.py").read_text(encoding="utf-8")
            filename = "/app/scripts/order_automation_host_gate.py"
            invocation = f"exec(compile({source!r}, {filename!r}, 'exec'), {{'__name__': '__main__', '__file__': {filename!r}}})"
            gate_command = ["python", "-c", invocation,
                            "--project", project, "--kind", kind]
        else:
            gate_command = ["python", "scripts/order_automation_host_gate.py", "--project", project, "--kind", kind]
            if kind == "invoice":
                gate_command.append("--full-backlog")
        overrides = {"containerOverrides": [{"name": "reporting", "command": gate_command}]}
        require(len(json.dumps(overrides).encode()) <= 8192, "candidate-command-too-large")
        response = self.ecs.run_task(
            cluster=schedule["Target"]["Arn"], taskDefinition=task_arn, launchType="FARGATE", count=1,
            networkConfiguration={"awsvpcConfiguration": network}, startedBy="order-automation-host-gate",
            overrides=overrides,
        )
        require(not response.get("failures") and len(response.get("tasks", [])) == 1, "candidate-start-failed")
        own_task = response["tasks"][0]["taskArn"]
        deadline = time.monotonic() + 1800
        task = None
        try:
            while time.monotonic() < deadline:
                reply = self.ecs.describe_tasks(cluster=schedule["Target"]["Arn"], tasks=[own_task])
                require(not reply.get("failures") and len(reply.get("tasks", [])) == 1, "candidate-readback-missing")
                task = reply["tasks"][0]
                require(task["taskDefinitionArn"] == task_arn, "candidate-definition-drift")
                if task["lastStatus"] == "STOPPED":
                    break
                time.sleep(10)
            require(task is not None and task["lastStatus"] == "STOPPED", "candidate-timeout")
        finally:
            # Bound cleanup to the exact task returned by this run_task call.
            check = self.ecs.describe_tasks(cluster=schedule["Target"]["Arn"], tasks=[own_task])
            if check.get("tasks") and check["tasks"][0]["lastStatus"] != "STOPPED":
                observed = check["tasks"][0]
                require(observed["taskDefinitionArn"] == task_arn and observed.get("startedBy") == "order-automation-host-gate",
                        "candidate-cleanup-identity-mismatch")
                self.ecs.stop_task(cluster=schedule["Target"]["Arn"], task=own_task, reason="Bounded order automation host gate cleanup")
                self.ecs.get_waiter("tasks_stopped").wait(cluster=schedule["Target"]["Arn"], tasks=[own_task],
                                                        WaiterConfig={"Delay": 5, "MaxAttempts": 24})
        container = task["containers"][0]
        require(container.get("exitCode") == 0 and container.get("imageDigest") == digest, "candidate-exit-or-image-mismatch")
        ips = [entry["privateIpv4Address"] for entry in container.get("networkInterfaces", []) if entry.get("privateIpv4Address")]
        require(len(ips) == 1 and task.get("launchType") == "FARGATE", "candidate-host-identity-incomplete")
        expected = {"marker": MARKER, "project": project, "kind": kind, "path": "/app", "dry_run": True}
        if kind == "invoice" and not old_image:
            expected["full_backlog"] = True
        markers = []
        token = None
        for _ in range(100):
            request = {"logGroupName": f"/ecs/{family}", "logStreamName": f"ecs/reporting/{own_task.rsplit('/', 1)[1]}", "startFromHead": True}
            if token:
                request["nextToken"] = token
            log = self.logs.get_log_events(**request)
            for event in log.get("events", []):
                message = event.get("message", "")
                if message.startswith(MARKER + " "):
                    markers.append(json.loads(message[len(MARKER) + 1:]))
            next_token = log.get("nextForwardToken")
            if next_token == token:
                break
            require(next_token, "candidate-log-token-missing")
            token = next_token
        else:
            raise RuntimeError("candidate-log-read-incomplete")
        require(markers == [expected], "candidate-localhost-marker-missing")
        print(f"ORDER_AUTOMATION_CANDIDATE_OK:{family}:identity-verified:path=/app:curl-localhost:stopped", flush=True)
        return {"service": family, "task": own_task, "private_ip": ips[0], "instance_id": "N/A:FARGATE",
                "task_definition": task_arn, "image_digest": digest, "path": "/app", "exit_code": 0, "marker": expected}

    def pin_current(self, digest: str) -> None:
        """Freeze current behavior before a shared :latest build can replace it."""
        ecr = self.session.client("ecr")
        image = ecr.describe_images(repositoryName="vevo-reporting", imageIds=[{"imageTag": "latest"}])["imageDetails"]
        require(len(image) == 1 and image[0]["imageDigest"] == digest, "current-image-changed-before-pin")
        image_uri = f"{self.account}.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@{digest}"
        snapshots = {name: self.scheduler.get_schedule(Name=name) for name in SCHEDULES}
        self.validate_snapshots(snapshots)
        definitions = {}
        for family, (_, _, schedule_name) in SERVICES.items():
            definition = self.ecs.describe_task_definition(taskDefinition=snapshots[schedule_name]["Target"]["EcsParameters"]["TaskDefinitionArn"])["taskDefinition"]
            definitions[family] = definition
            require(definition["family"] == family and len(definition["containerDefinitions"]) == 1, "pin-definition-drift")
            container = definition["containerDefinitions"][0]
            require(container.get("name") == "reporting" and container.get("command") == command_for(family), "pin-command-drift")
            if SERVICES[family][1] == "invoice":
                require(container["image"] in (image_uri, f"{self.account}.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting:latest"), "pin-image-drift")
            else:
                require("@sha256:" in container["image"], "cancellation-not-already-pinned")
        self.evidence_bucket = self.bucket(definitions["roy-invoice-daily"])
        self.evidence.update(original_schedules=snapshots, original_task_definitions=definitions,
                             image_digest=digest, phase="before-current-image-pin")
        self.save_private(self.evidence_bucket)
        desired = {name: schedule_request(snapshot) for name, snapshot in snapshots.items()}
        for family, (_, kind, schedule_name) in SERVICES.items():
            if kind != "invoice":
                continue
            source = definitions[family]
            candidate = {key: copy.deepcopy(source[key]) for key in TASK_FIELDS if key in source}
            candidate["containerDefinitions"][0]["image"] = image_uri
            task_arn = self.ecs.register_task_definition(**candidate)["taskDefinition"]["taskDefinitionArn"]
            self.evidence["hosts"].append(self.host_gate(family, task_arn, snapshots[schedule_name], digest, old_image=True))
            for name in SCHEDULES:
                if SCHEDULES[name] == family:
                    desired[name]["Target"]["EcsParameters"]["TaskDefinitionArn"] = task_arn
            self.save_private(self.evidence_bucket)
        # Detect an unrelated image publication during either host check.
        image = ecr.describe_images(repositoryName="vevo-reporting", imageIds=[{"imageTag": "latest"}])["imageDetails"]
        require(image[0]["imageDigest"] == digest, "current-image-changed-during-pin")
        self.evidence.update(desired_schedules=desired, phase="ready-to-pin-current-image")
        self.save_private(self.evidence_bucket)
        promote_schedules(self.scheduler, snapshots, desired)
        self.evidence["phase"] = "current-image-pin-verified"
        self.save_private(self.evidence_bucket)
        print("ORDER_AUTOMATION_CURRENT_IMAGE_PIN_OK:invoice-services=2:schedules=4:behavior-preserved", flush=True)

    def validate_snapshots(self, snapshots: dict) -> None:
        for name, schedule in snapshots.items():
            require(schedule["State"] == "ENABLED" and schedule.get("GroupName", "default") == "default", "source-schedule-state-drift")
            target = schedule["Target"]
            require(target["Arn"] == f"arn:aws:ecs:eu-central-1:{self.account}:cluster/vevo-reporting-cluster", "source-cluster-drift")
            require(f":task-definition/{SCHEDULES[name]}:" in target["EcsParameters"]["TaskDefinitionArn"], "source-family-drift")
            require(target["EcsParameters"].get("TaskCount", 1) == 1 and not target.get("Input"), "source-overrides-drift")

    def unfinished_automation_tasks(self, schedules: dict) -> list[dict]:
        """Include actual PENDING and stopping tasks; never terminate natural jobs.

        ECS desiredStatus=PENDING returns nothing. Tasks actually PENDING are
        returned under desiredStatus=RUNNING; STOPPED can still be stopping.
        """
        cluster = next(iter(schedules.values()))["Target"]["Arn"]
        require(all(row["Target"]["Arn"] == cluster for row in schedules.values()), "drain-cluster-drift")
        families = {SCHEDULES[name] for name in schedules}
        identities = set()
        for family in sorted(families):
            for desired in ("RUNNING", "STOPPED"):
                token, seen = None, set()
                for _ in range(100):
                    request = {"cluster": cluster, "family": family, "desiredStatus": desired, "maxResults": 100}
                    if token:
                        request["nextToken"] = token
                    page = self.ecs.list_tasks(**request)
                    identities.update(page.get("taskArns", []))
                    token = page.get("nextToken")
                    if not token:
                        break
                    require(token not in seen, "drain-task-pagination-cycle")
                    seen.add(token)
                else:
                    raise RuntimeError("drain-task-pagination-incomplete")
        unfinished = []
        values = sorted(identities)
        for start in range(0, len(values), 100):
            requested = values[start:start + 100]
            result = self.ecs.describe_tasks(cluster=cluster, tasks=requested)
            tasks = result.get("tasks", [])
            require(not result.get("failures") and {row.get("taskArn") for row in tasks} == set(requested),
                    "drain-task-readback-incomplete")
            for task in tasks:
                definition = task.get("taskDefinitionArn", "")
                family = definition.rsplit("/", 1)[-1].rsplit(":", 1)[0]
                require(family in families and task.get("clusterArn") == cluster
                        and definition.startswith(f"arn:aws:ecs:eu-central-1:{self.account}:task-definition/"),
                        "drain-task-identity-mismatch")
                require(bool(task.get("lastStatus")), "drain-task-status-missing")
                if task["lastStatus"] != "STOPPED":
                    unfinished.append({key: task.get(key) for key in (
                        "taskArn", "taskDefinitionArn", "lastStatus", "desiredStatus",
                    )})
        return unfinished

    def wait_for_drain(self, paused: dict, *, timeout_seconds: int = 1800, quiet_seconds: int = 120) -> None:
        """Require a quiet interval after pause propagation, bounded to 30 minutes."""
        deadline, quiet_since = time.monotonic() + timeout_seconds, None
        while time.monotonic() < deadline:
            for name, expected in paused.items():
                current = schedule_request(self.scheduler.get_schedule(Name=name))
                require(current == schedule_request(expected) and current["State"] == "DISABLED",
                        "schedule-changed-during-drain")
            active = self.unfinished_automation_tasks(paused)
            now = time.monotonic()
            if active:
                quiet_since = None
            elif quiet_since is None:
                quiet_since = now
            elif now - quiet_since >= quiet_seconds:
                self.evidence["drain"] = {"verified_at": datetime.now(timezone.utc).isoformat(),
                                           "quiet_seconds": quiet_seconds, "unfinished_tasks": 0}
                return
            time.sleep(10)
        raise RuntimeError("old-automation-tasks-drain-timeout")

    def pause_drain_and_promote(self, originals: dict, desired: dict) -> None:
        paused = {name: {**schedule_request(row), "State": "DISABLED"} for name, row in originals.items()}
        # The existing transactional updater handles partial/ambiguous pauses and
        # restores their original states without overwriting concurrent edits.
        promote_schedules(self.scheduler, originals, paused)
        promotion_started = False
        try:
            self.evidence["phase"] = "schedules-paused-draining"
            self.save_private(self.evidence_bucket)
            self.wait_for_drain(paused)
            self.evidence["phase"] = "drain-verified-before-promotion"
            self.save_private(self.evidence_bucket)
            promotion_started = True
            promote_schedules(self.scheduler, paused, desired)
        except Exception:
            try:
                restore_from = paused
                if promotion_started:
                    # A candidate may have started during a partial promotion.
                    # Keep both generations paused and drain it before restoring
                    # old runners, which do not understand the shared lease.
                    current = {name: schedule_request(self.scheduler.get_schedule(Name=name)) for name in originals}
                    require(all(current[name] in (paused[name], desired[name], schedule_request(originals[name]))
                                for name in originals), "drain-rollback-concurrent-change")
                    restore_from = {name: {**row, "State": "DISABLED"} for name, row in current.items()}
                    promote_schedules(self.scheduler, current, restore_from)
                    self.wait_for_drain(restore_from)
                promote_schedules(self.scheduler, restore_from,
                                  {name: schedule_request(row) for name, row in originals.items()})
            except Exception:
                raise RuntimeError("schedule-drain-rollback-requires-operator") from None
            raise RuntimeError("schedule-drain-or-promotion-failed-originals-restored") from None

    def provision_monitoring(self, family: str, schedules: list[dict], bucket: str, task: dict) -> str:
        project, kind, _ = SERVICES[family]
        queue_name = f"{family}-dlq"
        sqs, iam = self.session.client("sqs"), self.session.client("iam")
        queue_url = sqs.create_queue(QueueName=queue_name, Attributes={"MessageRetentionPeriod": "1209600", "SqsManagedSseEnabled": "true"})["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        queue_policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "scheduler.amazonaws.com"},
                        "Action": "sqs:SendMessage", "Resource": queue_arn,
                        "Condition": {"ArnEquals": {"aws:SourceArn": [schedule["Arn"] for schedule in schedules]},
                                      "StringEquals": {"aws:SourceAccount": self.account}}}]}
        sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"Policy": json.dumps(queue_policy)})
        for role_arn in {schedule["Target"]["RoleArn"] for schedule in schedules}:
            policy = {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": "ecs:RunTask", "Resource": f"arn:aws:ecs:eu-central-1:{self.account}:task-definition/{family}:*"},
                {"Effect": "Allow", "Action": "iam:PassRole", "Resource": [task["taskRoleArn"], task["executionRoleArn"]],
                 "Condition": {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}},
                {"Effect": "Allow", "Action": "sqs:SendMessage", "Resource": queue_arn},
            ]}
            iam.put_role_policy(RoleName=role_arn.rsplit("/", 1)[1], PolicyName=f"OrderAutomation-{family}", PolicyDocument=json.dumps(policy))
        cloudwatch = self.session.client("cloudwatch")
        settings = json.loads((ROOT / "projects" / project / "settings.json").read_text(encoding="utf-8"))
        namespace = settings.get("cloudwatch_namespace") or "BizniswebReporting"
        dims = [{"Name": "Project", "Value": project}, {"Name": "RunMode", "Value": "live"}]
        prefix = "InvoiceStandalone" if kind == "invoice" else "UnpaidCancellation"
        specs = [("run-failed", prefix + "RunFailed", 300, 1, "GreaterThanOrEqualToThreshold", "notBreaching"),
                 ("missing-completion", prefix + "RunSucceeded", 1800 if kind == "invoice" else 90000, 1,
                  "LessThanThreshold", "breaching")]
        if kind == "invoice":
            specs.extend([("ambiguous-operations", prefix + "AmbiguousOperations", 900, 1, "GreaterThanOrEqualToThreshold", "notBreaching"),
                          ("review-required", prefix + "ReviewRequired", 900, 1, "GreaterThanOrEqualToThreshold", "notBreaching"),
                          ("pending-backlog", prefix + "BacklogPending", 1800, 1, "GreaterThanOrEqualToThreshold", "notBreaching"),
                          ("stale-full-scan", prefix + "FullScanAgeHours", 1800, 26, "GreaterThanThreshold", "breaching")])
        for suffix, metric, period, threshold, comparison, missing in specs:
            cloudwatch.put_metric_alarm(AlarmName=f"{family}-{suffix}", AlarmDescription="Order automation live execution health; dry runs are excluded.",
                Namespace=namespace, MetricName=metric, Dimensions=dims, Statistic="Maximum" if suffix in ("stale-full-scan", "pending-backlog", "ambiguous-operations") else "Sum",
                Period=period, EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=threshold, ComparisonOperator=comparison,
                TreatMissingData=missing, ActionsEnabled=True, AlarmActions=self.alarm_actions)
        cloudwatch.put_metric_alarm(AlarmName=f"{family}-dlq-not-empty", Namespace="AWS/SQS", MetricName="ApproximateNumberOfMessagesVisible",
            Dimensions=[{"Name": "QueueName", "Value": queue_name}], Statistic="Maximum", Period=300, EvaluationPeriods=1,
            Threshold=1, ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching", ActionsEnabled=True,
            AlarmActions=self.alarm_actions)
        return queue_arn

    def run(self) -> None:
        image = self.session.client("ecr").describe_images(repositoryName="vevo-reporting", imageIds=[{"imageTag": f"git-{self.commit}"}])["imageDetails"]
        require(len(image) == 1 and re.fullmatch(r"sha256:[a-f0-9]{64}", image[0]["imageDigest"]) is not None, "exact-commit-image-missing")
        digest = image[0]["imageDigest"]
        image_uri = f"{self.account}.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@{digest}"
        snapshots = {name: self.scheduler.get_schedule(Name=name) for name in SCHEDULES}
        self.validate_snapshots(snapshots)
        definitions, candidates, buckets = {}, {}, {}
        for family, (project, _, schedule_name) in SERVICES.items():
            source = self.ecs.describe_task_definition(taskDefinition=snapshots[schedule_name]["Target"]["EcsParameters"]["TaskDefinitionArn"])["taskDefinition"]
            buckets[family] = self.bucket(source)
            from reporting_core.storage import resolve_report_s3_location
            settings = json.loads((ROOT / "projects" / project / "settings.json").read_text(encoding="utf-8"))
            _, prefix = resolve_report_s3_location(project, settings, environ={})
            definitions[family] = candidate_definition(source, family, image_uri, (buckets[family], prefix))
        require(buckets["roy-invoice-daily"] == buckets["roy-unpaid-order-cancellation"], "roy-state-bucket-mismatch")
        self.evidence.update(original_schedules=snapshots, candidate_task_definitions=definitions,
                             image_digest=digest, phase="before-candidates")
        evidence_bucket = buckets["roy-invoice-daily"]
        self.evidence_bucket = evidence_bucket
        self.save_private(evidence_bucket)
        # Journal access is a host-check prerequisite. All three hosts must pass
        # before any schedule or monitoring mutation; failed probes restore IAM.
        try:
            seen_state = set()
            for family, (project, _, _) in SERVICES.items():
                identity = (project, buckets[family], definitions[family]["taskRoleArn"])
                if identity not in seen_state:
                    self.prepare_state(*identity)
                    seen_state.add(identity)
            for family, (_, _, schedule_name) in SERVICES.items():
                candidates[family] = self.ecs.register_task_definition(**definitions[family])["taskDefinition"]["taskDefinitionArn"]
                host = self.host_gate(family, candidates[family], snapshots[schedule_name], digest)
                self.evidence["hosts"].append(host)
                self.save_private(evidence_bucket)
        except Exception:
            self.restore_state_policies()
            self.evidence["phase"] = "candidate-failed-state-policy-restored"
            self.save_private(evidence_bucket)
            raise
        require(len(self.evidence["hosts"]) == len(SERVICES), "host-gate-incomplete")
        self.alarm_actions = [established_alarm_route(self.session, self.account)]
        self.evidence["alarm_actions"] = self.alarm_actions
        subprocess.run(["git", "fetch", "origin", "main"], cwd=ROOT, check=True, capture_output=True)
        current_main = subprocess.check_output(["git", "rev-parse", "origin/main"], cwd=ROOT, text=True).strip()
        require(current_main == self.commit, "main-changed-before-promotion")
        dlqs = {}
        for family in SERVICES:
            service_schedules = [snapshot for name, snapshot in snapshots.items() if SCHEDULES[name] == family]
            dlqs[family] = self.provision_monitoring(family, service_schedules, buckets[family], definitions[family])
        desired = {}
        for name, family in SCHEDULES.items():
            settings = json.loads((ROOT / "projects" / SERVICES[family][0] / "settings.json").read_text(encoding="utf-8"))
            desired[name] = desired_schedule(snapshots[name], candidates[family], dlqs[family], settings)
        self.evidence.update(desired_schedules=desired, phase="ready-to-promote")
        self.save_private(evidence_bucket)
        try:
            self.pause_drain_and_promote(snapshots, desired)
        except Exception:
            self.evidence["phase"] = "promotion-failed-review-rollback"
            self.save_private(evidence_bucket)
            raise
        self.evidence["phase"] = "promotion-readback-verified"
        self.save_private(evidence_bucket)
        print("ORDER_AUTOMATION_DEPLOY_OK:services=3:schedules=5:immutable:private-evidence", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", required=True)
    parser.add_argument("--pin-current", action="store_true", help="Pin existing invoice behavior before publishing a new shared image")
    parser.add_argument("--current-image-digest", help="Independently verified current image digest; required for --pin-current")
    parser.add_argument("--profile", help="Local AWS profile, allowed only for --pin-current")
    args = parser.parse_args()
    require(re.fullmatch(r"[a-f0-9]{40}", args.commit) is not None, "commit-invalid")
    import boto3
    if args.pin_current:
        require(re.fullmatch(r"sha256:[a-f0-9]{64}", args.current_image_digest or "") is not None, "pin-current-digest-required")
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        require(head == args.commit, "pin-helper-commit-mismatch")
        branch = subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip()
        require(branch.startswith("codex/"), "pin-helper-review-branch-required")
        tracked_changes = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT, text=True).strip()
        require(not tracked_changes, "pin-helper-uncommitted-changes")
        subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/deploy_order_automations.py",
                        "scripts/order_automation_host_gate.py"], cwd=ROOT, check=True, capture_output=True)
        subprocess.run(["git", "fetch", "origin", branch], cwd=ROOT, check=True, capture_output=True)
        upstream = subprocess.check_output(["git", "rev-parse", "@{upstream}"], cwd=ROOT, text=True).strip()
        require(upstream == head, "pin-helper-not-pushed")
        Deployment(boto3.Session(profile_name=args.profile, region_name="eu-central-1"), args.commit).pin_current(args.current_image_digest)
    else:
        require(not args.profile and not args.current_image_digest, "managed-deploy-local-override-rejected")
        require(os.environ.get("GITHUB_REF") == "refs/heads/main" and os.environ.get("GITHUB_SHA") == args.commit
                and os.environ.get("GITHUB_ACTIONS") == "true", "managed-main-only")
        require(os.environ.get("AWS_REGION") == "eu-central-1", "region-mismatch")
        Deployment(boto3.Session(region_name="eu-central-1"), args.commit).run()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Exception messages can include URLs, credentials or application data.
        code = str(exc) if type(exc) is RuntimeError and re.fullmatch(r"[a-z][a-z0-9-]{1,100}", str(exc)) else "external-operation-failed"
        print(f"ORDER_AUTOMATION_DEPLOY_FAILED:{type(exc).__name__}:{code}", file=sys.stderr)
        raise SystemExit(1) from None
