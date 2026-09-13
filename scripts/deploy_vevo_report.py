#!/usr/bin/env python3
"""Managed VEVO-only report transaction. Never dispatch from a local branch.

Requires separately reviewed current-runtime and readiness receipts. Registers
unscheduled immutable definitions, proves an isolated host, then promotes only
the VEVO report schedule. Uncertain ownership leaves the report paused.
"""
from __future__ import annotations

import argparse
import copy
from contextlib import ExitStack, closing
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
from scripts.deploy_order_automations import TASK_FIELDS, SCHEDULES, schedule_request, require  # noqa: E402
from scripts.reporting_migration_host_gate import ACCOUNT, REGION, BUCKET, PREFIX, SERVICE, canonical, sha, read_private  # noqa: E402

CLUSTER = f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/vevo-reporting-cluster"
GUARDS = {f"{project}-creditnote-storno-guard" for project in ("roy", "vevo")}
PROTECTED = set(SCHEDULES) | GUARDS | {"roy-daily-report-email", "vevo-growthbook-reconcile-production"}
STACKS = {"vevo-growthbook-production", "vevo-growthbook-reconciliation-production"}
WORKFLOW = ".github/workflows/deploy-vevo-report.yml"


def now():
    return datetime.now(timezone.utc)


def gh(path):
    raw = subprocess.check_output(["gh", "api", path], cwd=ROOT, timeout=40)
    return json.loads(raw)


def current_main(commit):
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_SHA") == commit and os.environ.get("GITHUB_WORKFLOW_REF") ==
            "vzeman/biznisweb/" + WORKFLOW + "@refs/heads/main",
            "report-managed-main-required")
    require(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip() == "",
            "report-source-dirty")
    subprocess.run(["git", "fetch", "origin", "main"], cwd=ROOT, check=True, capture_output=True, timeout=40)
    for ref in ("HEAD", "origin/main"):
        require(subprocess.check_output(["git", "rev-parse", ref], cwd=ROOT, text=True).strip() == commit,
                "report-source-main-drift")


def candidate_definition(source, image, release_id=None, *, source_commit=None, gate_sha256=None):
    result = {k: copy.deepcopy(source[k]) for k in TASK_FIELDS if k in source}
    require(result.get("family") == "vevo-reporting-daily" and result.get("networkMode") == "awsvpc",
            "report-definition-family-invalid")
    containers = result.get("containerDefinitions", [])
    require(len(containers) == 1 and containers[0].get("name") == "reporting", "report-container-invalid")
    container = containers[0]
    require(container.get("command") in (None, ["python", "daily_report_runner.py"])
            and not container.get("entryPoint"), "report-production-command-unreviewed")
    env = {row["name"]: row["value"] for row in container.get("environment", [])}
    require(len(env) == len(container.get("environment", [])) and env.get("REPORT_PROJECT") == "vevo",
            "report-environment-unreviewed")
    controls = {"REPORT_SKIP_EMAIL", "REPORT_SKIP_INVOICES", "REPORT_SKIP_CREDITNOTE_STORNO_GUARD"}
    require(not controls.intersection(row["name"] for row in container.get("secrets", [])), "report-control-secret-collision")
    require(re.fullmatch(rf"{ACCOUNT}\.dkr\.ecr\.{REGION}\.amazonaws\.com/vevo-reporting@sha256:[a-f0-9]{{64}}", image),
            "report-image-not-immutable")
    env.update(REPORT_SKIP_INVOICES="true", REPORT_SKIP_CREDITNOTE_STORNO_GUARD="true")
    container.update(image=image, workingDirectory="/app")
    if release_id:
        require(re.fullmatch(r"[a-f0-9]{32}", release_id) and re.fullmatch(r"[a-f0-9]{40}", source_commit or "")
                and re.fullmatch(r"[a-f0-9]{64}", gate_sha256 or ""), "report-probe-source-invalid")
        result["taskRoleArn"] = f"arn:aws:iam::{ACCOUNT}:role/VevoReportProbe-{release_id}"
        env["REPORT_SKIP_EMAIL"] = "true"
        container["command"] = ["python", "scripts/reporting_migration_host_gate.py", "--release-id", release_id,
                                "--source-commit", source_commit, "--image-digest", image.rsplit("@", 1)[1],
                                "--gate-sha256", gate_sha256]
    container["environment"] = [{"name": k, "value": v} for k, v in sorted(env.items())]
    return result


def diagnostic_policy(release_id):
    prefix = PREFIX + release_id + "/"
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [
            f"arn:aws:s3:::{BUCKET}/daily-reports/vevo/*", f"arn:aws:s3:::{BUCKET}/{prefix}*"]},
        {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": [
            f"arn:aws:s3:::{BUCKET}/{prefix}markers/*", f"arn:aws:s3:::{BUCKET}/{prefix}artifacts/*"],
         "Condition": {"StringEquals": {"s3:x-amz-server-side-encryption": "AES256"}}},
    ]}


class Deployment:
    def __init__(self, session, commit, readiness_key, readiness_sha256, *, clock=time.monotonic, sleep=time.sleep):
        from scripts import reporting_runtime_binding as binding
        self.binding = binding
        self.session, self.commit = session, commit
        self.ecs, self.scheduler, self.s3, self.iam = (session.client(name) for name in ("ecs", "scheduler", "s3", "iam"))
        self.clock, self.sleep = clock, sleep
        self.release_id = uuid.uuid4().hex
        self.prefix = PREFIX + self.release_id + "/"
        self.lease = binding.MigrationLease(self.s3, owner=self.release_id)
        self.readiness_key, self.readiness_sha = readiness_key, readiness_sha256
        self.owned_task = self.probe_definition = self.production_definition = None
        self.role_created = False
        self.role_attempted = False
        self.start_uncertain = False
        self.known_schedule = None
        self.original = self.previous = self.protected = None
        self.observed_overlaps = set()
        self.release_attempted = False
        self.last_checkpoint = None

    def event(self, phase, **details):
        value = {"schema_version": 1, "release_id": self.release_id, "source_commit": self.commit,
                 "workflow_run_id": os.environ["GITHUB_RUN_ID"], "phase": phase, "at": now().isoformat(), **details}
        raw = canonical(self.binding.normalized(value))
        key = self.binding.PREFIX + f"transactions/{self.release_id}/{sha(raw)}.json"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ExpectedBucketOwner=ACCOUNT,
                           ServerSideEncryption="AES256", IfNoneMatch="*")
        require(read_private(self.s3, key) == raw, "report-event-readback-failed")

    def schedule(self, name=SERVICE):
        return self.scheduler.get_schedule(Name=name)

    def definition(self, arn):
        return self.ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]

    def role_controls(self, arns):
        controls = {}
        for arn in sorted(arns):
            require(arn.startswith(f"arn:aws:iam::{ACCOUNT}:role/"), "report-protected-role-account")
            name = arn.rsplit("/", 1)[1]
            role = self.iam.get_role(RoleName=name)["Role"]
            require(role["Arn"] == arn, "report-protected-role-identity")
            inline = self.iam.list_role_policies(RoleName=name, MaxItems=1000)
            attached = self.iam.list_attached_role_policies(RoleName=name, MaxItems=1000)
            require(not inline.get("IsTruncated") and not attached.get("IsTruncated"), "report-role-policy-inventory-incomplete")
            value = {k: role[k] for k in ("Arn", "Path", "RoleName", "AssumeRolePolicyDocument", "PermissionsBoundary", "MaxSessionDuration", "Tags") if k in role}
            value["inline"] = {key: self.iam.get_role_policy(RoleName=name, PolicyName=key)["PolicyDocument"]
                               for key in sorted(inline["PolicyNames"])}
            managed = {row["PolicyArn"] for row in attached["AttachedPolicies"]}
            if role.get("PermissionsBoundary"):
                managed.add(role["PermissionsBoundary"]["PermissionsBoundaryArn"])
            value["managed"] = {}
            for policy in sorted(managed):
                version = self.iam.get_policy(PolicyArn=policy)["Policy"]["DefaultVersionId"]
                value["managed"][policy] = {"version": version, "document": self.iam.get_policy_version(
                    PolicyArn=policy, VersionId=version)["PolicyVersion"]["Document"]}
            controls[arn] = self.binding.normalized(value)
        return controls

    def snapshot_protected(self, *, guards_required=True):
        schedules = {}
        for name in sorted(PROTECTED):
            try:
                schedules[name] = self.binding.schedule_snapshot(self.schedule(name))
            except Exception as exc:
                require(not guards_required and name in GUARDS and getattr(exc, "response", {}).get("Error", {}).get("Code")
                        == "ResourceNotFoundException", "report-protected-schedule-unavailable")
        require(all(row.get("State") == "ENABLED" for row in schedules.values()), "report-protected-not-enabled")
        roy = schedules["roy-daily-report-email"]["Target"]["EcsParameters"]["TaskDefinitionArn"]
        require(roy == f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/roy-reporting-daily:71", "report-roy-pin-changed")
        definitions = {}
        for name, schedule in schedules.items():
            arn = schedule["Target"]["EcsParameters"]["TaskDefinitionArn"]
            definition = self.definition(arn)
            if name in GUARDS:
                require(definition.get("family") == name and definition["containerDefinitions"][0].get("command") ==
                        ["python", "creditnote_storno_runner.py", "--project", name.split("-")[0]],
                        "report-standalone-guard-command-invalid")
            definitions[arn] = self.binding.definition_snapshot(definition)
        stacks = {}
        cf = self.session.client("cloudformation")
        for name in sorted(STACKS):
            rows = cf.describe_stacks(StackName=name)["Stacks"]
            require(len(rows) == 1 and rows[0]["StackStatus"] in {"CREATE_COMPLETE", "UPDATE_COMPLETE"},
                    "report-growthbook-stack-not-stable")
            stacks[name] = self.binding.normalized({k: v for k, v in rows[0].items() if k not in {"LastUpdatedTime", "CreationTime"}})
            stacks[name]["template_sha256"] = self.binding.sha256(cf.get_template(StackName=name)["TemplateBody"])
        outputs = {row["OutputKey"]: row["OutputValue"] for row in stacks["vevo-growthbook-production"]["Outputs"]}
        collector = self.ecs.describe_services(cluster=outputs["CollectorClusterArn"], services=[outputs["CollectorServiceArn"]])
        require(not collector.get("failures") and len(collector.get("services", [])) == 1, "report-collector-service-unavailable")
        service = collector["services"][0]
        require(service["taskDefinition"] == outputs["CollectorTaskDefinitionArn"] and service["status"] == "ACTIVE",
                "report-collector-runtime-drift")
        collector_fields = ("serviceArn", "clusterArn", "desiredCount", "taskDefinition", "launchType", "platformVersion",
            "capacityProviderStrategy", "networkConfiguration", "loadBalancers", "serviceRegistries", "deploymentConfiguration",
            "healthCheckGracePeriodSeconds", "schedulingStrategy", "enableExecuteCommand", "propagateTags", "enableECSManagedTags")
        collector_control = self.binding.normalized({k: service[k] for k in collector_fields if k in service})
        definitions[service["taskDefinition"]] = self.binding.definition_snapshot(self.definition(service["taskDefinition"]))
        roles = {td[key] for td in [*definitions.values(), self.original_source]
                 for key in ("taskRoleArn", "executionRoleArn") if td.get(key)}
        roles.update(row["Target"]["RoleArn"] for row in schedules.values())
        roles.add(self.original["Target"]["RoleArn"])
        return {"schedules": schedules, "definitions": definitions, "stacks": stacks,
                "collector": collector_control, "roles": self.role_controls(roles)}

    def bootstrap(self):
        """No schedule/role/provider change: one finite identity-only old-image task."""
        from scripts.bootstrap_reporting_runtime import verify_host
        from scripts.reporting_identity_probe import MARKER as BASELINE_MARKER
        current_main(self.commit)
        require(self.session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "report-account-invalid")
        self.binding.require_private_bucket(self.s3)
        self.lease.acquire()
        try:
            require(self.binding.read_object(self.s3, self.binding.CURRENT_KEY, optional=True) is None,
                    "report-baseline-already-published")
            self.original = self.schedule()
            source = self.definition(self.binding.POLICY["baseline_definition"])
            self.original_source = source
            self.binding.validate_configuration(self.binding.schedule_snapshot(self.original), self.binding.definition_snapshot(source), migrated=False)
            require(source["containerDefinitions"][0]["image"].endswith("@" + self.binding.POLICY["baseline_image_digest"]),
                    "report-baseline-image-invalid")
            protected = self.snapshot_protected(guards_required=False)
            self.exclusion()
            code = (ROOT / "scripts/reporting_identity_probe.py").read_text(encoding="utf-8")
            old = subprocess.check_output(["git", "show", self.binding.POLICY["baseline_source_commit"] + ":daily_report_runner.py"], cwd=ROOT)
            command = self.binding.build_baseline_probe_command(code, sha(old), self.binding.POLICY["baseline_image_digest"])
            overrides = {"containerOverrides": [{"name": "reporting", "command": command}]}
            require(len(json.dumps(overrides).encode()) <= 8192, "report-baseline-command-too-large")
            network = self.original["Target"]["EcsParameters"]["NetworkConfiguration"]["awsvpcConfiguration"]
            network = {k[:1].lower() + k[1:]: v for k, v in network.items()}
            self.probe_definition = source
            current_main(self.commit)
            self.start_uncertain = True
            result = self.ecs.run_task(cluster=CLUSTER, taskDefinition=source["taskDefinitionArn"], launchType="FARGATE", count=1,
                                       startedBy=self.release_id, clientToken=self.release_id, overrides=overrides,
                                       networkConfiguration={"awsvpcConfiguration": network})
            require(not result.get("failures") and len(result.get("tasks", [])) == 1, "report-baseline-start-failed")
            self.owned_task = result["tasks"][0]["taskArn"]
            self.start_uncertain = False
            for _ in range(90):
                self.lease.renew()
                task = self.task()
                if task["lastStatus"] == "STOPPED":
                    break
                self.sleep(5)
            require(task["lastStatus"] == "STOPPED" and task["containers"][0].get("exitCode") == 0, "report-baseline-host-failed")
            log = source["containerDefinitions"][0]["logConfiguration"]["options"]
            request = {"logGroupName": log["awslogs-group"],
                       "logStreamName": log["awslogs-stream-prefix"] + "/reporting/" + self.owned_task.rsplit("/", 1)[1],
                       "startFromHead": True}
            markers, token = [], None
            for _ in range(30):
                page = self.session.client("logs").get_log_events(**request, **({"nextToken": token} if token else {}))
                for event in page.get("events", []):
                    if event["message"].startswith(BASELINE_MARKER + " "):
                        markers.append(json.loads(event["message"].split(" ", 1)[1]))
                next_token = page.get("nextForwardToken")
                if next_token == token:
                    break
                require(next_token, "report-baseline-log-incomplete")
                token = next_token
            else:
                raise RuntimeError("report-baseline-log-cap")
            require(len(markers) == 1, "report-baseline-localhost-marker-missing")
            marker = markers[0]
            require(marker.get("task_arn") == self.owned_task and marker.get("runner_sha256") == sha(old)
                    and marker.get("provider_reads") == 0 and marker.get("path") == "/app" and marker.get("service") == SERVICE,
                    "report-baseline-marker-identity-invalid")
            proof = {"task_arn": self.owned_task, "private_ip": marker["private_ip"], "definition_arn": source["taskDefinitionArn"],
                     "image_digest": marker["image_digest"], "instance_id": "N/A:Fargate", "service": SERVICE, "path": "/app",
                     "localhost_marker_sha256": sha(canonical(marker)), "command": command, "exit_code": 0, "stopped": True}
            record = self.binding.build_baseline_record(self.original, source, release_id=self.release_id,
                workflow_run_id=os.environ["GITHUB_RUN_ID"], protected_sha256=self.binding.sha256(protected),
                candidate_proof=proof, verified_at=now().isoformat())
            verify_host(self.ecs, self.session.client("logs"), record)
            require(protected == self.snapshot_protected(guards_required=False), "report-baseline-protected-drift")
            require(self.binding.schedule_snapshot(self.schedule()) == record["schedule"], "report-baseline-schedule-drift")
            current_main(self.commit)
            self.event("baseline-identity-verified", candidate_proof=proof)
            published = self.binding.publish_verified_binding(self.s3, record, expected_pointer_etag=None, lease=self.lease)
            self.binding.validate_runtime(published, self.schedule(), self.definition(source["taskDefinitionArn"]))
            self.cleanup_task()
            self.release_verified(published)
        except Exception:
            try:
                self.cleanup_task()
            finally:
                self.lease.retain_uncertain()
            raise

    def readiness(self):
        from scripts.reporting_readiness import validate_readiness
        require(re.fullmatch(r"data/vevo/reporting/runtime/readiness/[a-zA-Z0-9_-]+\.json", self.readiness_key or "")
                and re.fullmatch(r"[a-f0-9]{64}", self.readiness_sha or ""), "report-reviewed-readiness-required")
        raw = read_private(self.s3, self.readiness_key, self.binding.LIMIT)
        require(sha(raw) == self.readiness_sha, "report-readiness-hash-invalid")
        receipt = self.binding.decode_json(raw)
        protected = self.snapshot_protected()
        validate_readiness(receipt, s3=self.s3, ecs=self.ecs, ecr=self.session.client("ecr"), protected=protected,
                           vevo_schedule=self.schedule())
        self.protected = protected

    def exclusion(self):
        own = os.environ["GITHUB_RUN_ID"]
        for status in ("queued", "in_progress"):
            for page in range(1, 6):
                rows = gh(f"repos/vzeman/biznisweb/actions/runs?status={status}&per_page=100&page={page}")["workflow_runs"]
                for row in rows:
                    name = Path(row.get("path", "")).name
                    sensitive = (name.startswith(("deploy-", "collect-", "build-vevo-growthbook-"))
                                 or name in {"production-reporting-smoke.yml", "production-invoice-smoke.yml"})
                    require(str(row["id"]) == own or not sensitive, "report-competing-managed-run")
                if len(rows) < 100:
                    break
            else:
                raise RuntimeError("report-managed-exclusion-incomplete")

    def checkpoint(self, *, poll=False):
        if not poll or self.last_checkpoint is None or self.clock() - self.last_checkpoint >= 60:
            self.lease.renew()
            require(self.snapshot_protected() == self.protected, "report-protected-runtime-drift")
            self.exclusion()
            self.last_checkpoint = self.clock()
        if self.known_schedule is not None:
            require(self.binding.schedule_snapshot(self.schedule()) == self.binding.schedule_snapshot(self.known_schedule),
                    "report-transaction-schedule-drift")

    def existing_host(self):
        proof = self.previous["record"]["candidate_proof"]
        result = self.ecs.describe_tasks(cluster=CLUSTER, tasks=[proof["task_arn"]])
        require(not result.get("failures") and len(result.get("tasks", [])) == 1,
                "report-current-host-proof-unavailable")
        task = result["tasks"][0]
        containers = task.get("containers", [])
        require(task["taskArn"] == proof["task_arn"] and task["taskDefinitionArn"] == proof["definition_arn"]
                and task.get("launchType") == "FARGATE" and task.get("clusterArn") == CLUSTER
                and task.get("lastStatus") == "STOPPED" and len(containers) == 1
                and containers[0].get("exitCode") == 0 and containers[0].get("imageDigest") == proof["image_digest"]
                and [row.get("privateIpv4Address") for row in containers[0].get("networkInterfaces", [])] == [proof["private_ip"]],
                "report-current-host-proof-drift")
        self.event("current-host-identity-verified", host=proof)

    def tasks(self, family="vevo-reporting-daily"):
        ids = set()
        for desired in ("RUNNING", "STOPPED"):
            token, seen = None, set()
            for _ in range(100):
                request = {"cluster": CLUSTER, "family": family, "desiredStatus": desired, "maxResults": 100}
                if token:
                    request["nextToken"] = token
                page = self.ecs.list_tasks(**request)
                ids.update(page.get("taskArns", []))
                token = page.get("nextToken")
                if not token:
                    break
                require(token not in seen, "report-task-pagination-cycle")
                seen.add(token)
            else:
                raise RuntimeError("report-task-pagination-incomplete")
        rows = []
        values = sorted(ids)
        for start in range(0, len(values), 100):
            requested = values[start:start + 100]
            result = self.ecs.describe_tasks(cluster=CLUSTER, tasks=requested)
            require(not result.get("failures") and {r["taskArn"] for r in result["tasks"]} == set(requested),
                    "report-task-readback-incomplete")
            rows.extend(result["tasks"])
        return rows

    def provider_overlap(self, *, dispatch=False):
        """Observe current tasks, without reserving future natural schedule slots."""
        active = {row["taskArn"] for family in sorted(set(SCHEDULES.values()) | GUARDS | {"roy-reporting-daily"})
                  for row in self.tasks(family) if row["lastStatus"] != "STOPPED"}
        if dispatch:
            require(not active, "report-observed-provider-task-active")
        new = active - self.observed_overlaps
        if new:
            self.event("natural-provider-task-overlap-observed", task_arns=sorted(new))
            self.observed_overlaps.update(new)

    def live_outputs(self):
        """Metadata only; the ordinary production sink and all its aliases stay fixed."""
        container = self.original_source["containerDefinitions"][0]
        env = {row["name"]: row["value"] for row in container.get("environment", [])}
        prefix = env.get("REPORT_S3_PREFIX", "").strip("/")
        require(env.get("REPORT_S3_BUCKET") == BUCKET and prefix and not prefix.startswith(PREFIX),
                "report-live-input-location-unbound")
        names = {"generation.json", "report_latest.html", "dashboard_payload_latest.json"}
        names.update(f"{kind}_{period}.{extension}" for period in ("7d", "30d", "90d")
                     for kind, extension in (("report", "html"), ("dashboard_payload", "json")))
        result = {}
        for name in sorted(names):
            key = f"{prefix}/latest/{name}"
            try:
                value = self.s3.head_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)
            except Exception as exc:
                require(getattr(exc, "response", {}).get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"},
                        "report-live-output-read-failed")
                result[key] = None
            else:
                result[key] = self.binding.normalized({k: value[k] for k in ("ETag", "ContentLength", "LastModified", "VersionId") if k in value})
        return result

    def exact_image(self):
        builds = gh(f"repos/vzeman/biznisweb/actions/workflows/build-and-push-ecr.yml/runs?head_sha={self.commit}&status=success&per_page=100")["workflow_runs"]
        builds = [r for r in builds if r["head_sha"] == self.commit and r["head_branch"] == "main" and r["conclusion"] == "success"]
        require(bool(builds), "report-exact-build-not-successful")
        images = self.session.client("ecr").describe_images(repositoryName="vevo-reporting", imageIds=[{"imageTag": "git-" + self.commit}])["imageDetails"]
        require(len(images) == 1 and re.fullmatch(r"sha256:[a-f0-9]{64}", images[0].get("imageDigest", "")), "report-exact-image-missing")
        return f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/vevo-reporting@{images[0]['imageDigest']}", str(builds[0]["id"])

    def drain(self):
        deadline, quiet = self.clock() + 3600, None
        while self.clock() < deadline:
            self.checkpoint(poll=True)
            unfinished = [r for r in self.tasks() if r["lastStatus"] != "STOPPED"]
            quiet = None if unfinished else self.clock() if quiet is None else quiet
            if quiet is not None and self.clock() - quiet >= 120:
                return
            self.sleep(10)
        raise RuntimeError("report-quiet-drain-timeout")

    def update(self, desired):
        require(self.known_schedule is not None and self.binding.schedule_snapshot(self.schedule()) ==
                self.binding.schedule_snapshot(self.known_schedule), "report-schedule-write-drift")
        previous = self.known_schedule
        try:
            self.scheduler.update_schedule(**schedule_request(desired))
        finally:
            observed = self.schedule()
            allowed = [self.binding.schedule_snapshot(x) for x in (previous, desired)]
            require(self.binding.schedule_snapshot(observed) in allowed, "report-schedule-write-uncertain")
            self.known_schedule = observed
        require(self.binding.schedule_snapshot(self.known_schedule) == self.binding.schedule_snapshot(desired),
                "report-schedule-write-not-confirmed")

    def create_role(self):
        name = "VevoReportProbe-" + self.release_id
        trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                 "Action": "sts:AssumeRole", "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT},
                 "ArnLike": {"aws:SourceArn": f"arn:aws:ecs:{REGION}:{ACCOUNT}:*"}}}]}
        self.role_trust = trust
        require(self.probe_role() is None, "report-probe-role-already-exists")
        self.role_attempted = True
        try:
            self.iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust),
                                 Tags=[{"Key": "ManagedReportProbe", "Value": self.release_id}])
        finally:
            # Reconcile a committed create with a lost acknowledgement; never
            # interpret eventually consistent not-found as confirmed absence.
            self.resolve_attempted_role()
        require(self.role_created, "report-probe-role-not-created")
        policy = diagnostic_policy(self.release_id)
        self.iam.put_role_policy(RoleName=name, PolicyName="IsolatedProbe", PolicyDocument=json.dumps(policy))
        require(self.iam.get_role_policy(RoleName=name, PolicyName="IsolatedProbe")["PolicyDocument"] == policy,
                "report-probe-policy-readback")
        require(self.iam.list_attached_role_policies(RoleName=name)["AttachedPolicies"] == []
                and self.iam.list_role_policies(RoleName=name)["PolicyNames"] == ["IsolatedProbe"], "report-probe-extra-policy")

    def probe_role(self):
        try:
            return self.iam.get_role(RoleName="VevoReportProbe-" + self.release_id)["Role"]
        except Exception as exc:
            require(getattr(exc, "response", {}).get("Error", {}).get("Code") == "NoSuchEntity", "report-probe-role-read-uncertain")
            return None

    def validate_probe_role(self, role):
        require(role["Arn"] == f"arn:aws:iam::{ACCOUNT}:role/VevoReportProbe-{self.release_id}"
                and {r["Key"]: r["Value"] for r in role.get("Tags", [])} == {"ManagedReportProbe": self.release_id}
                and role.get("AssumeRolePolicyDocument") == self.role_trust,
                "report-probe-role-ownership-invalid")

    def resolve_attempted_role(self):
        for attempt in range(31):
            role = self.probe_role()
            if role is not None:
                self.validate_probe_role(role)
                self.role_created, self.role_attempted = True, False
                return role
            if attempt < 30:
                self.sleep(2)
        # A retained unknown create must never be released as a clean rollback.
        raise RuntimeError("report-probe-role-create-unconfirmed")

    def task(self):
        result = self.ecs.describe_tasks(cluster=CLUSTER, tasks=[self.owned_task])
        require(not result.get("failures") and len(result["tasks"]) == 1, "report-probe-task-missing")
        task = result["tasks"][0]
        require(task["taskArn"] == self.owned_task and task["clusterArn"] == CLUSTER
                and task["taskDefinitionArn"] == self.probe_definition["taskDefinitionArn"]
                and task.get("startedBy") == self.release_id and task.get("launchType") == "FARGATE",
                "report-probe-task-ownership-invalid")
        return task

    def cleanup_task(self):
        if not self.owned_task:
            return
        task = self.task()
        if task["lastStatus"] != "STOPPED":
            self.ecs.stop_task(cluster=CLUSTER, task=self.owned_task, reason="Owned finite VEVO report probe cleanup")
            for _ in range(36):
                if self.task()["lastStatus"] == "STOPPED":
                    break
                self.sleep(5)
            require(self.task()["lastStatus"] == "STOPPED", "report-probe-cleanup-unconfirmed")

    def cleanup_role(self):
        if not self.role_created and not self.role_attempted:
            return
        self.cleanup_task()
        name = "VevoReportProbe-" + self.release_id
        role = self.resolve_attempted_role()
        self.validate_probe_role(role)
        policies = self.iam.list_role_policies(RoleName=name)["PolicyNames"]
        require(self.iam.list_attached_role_policies(RoleName=name)["AttachedPolicies"] == []
                and policies in ([], ["IsolatedProbe"]), "report-probe-role-cleanup-ownership-invalid")
        if policies:
            require(self.iam.get_role_policy(RoleName=name, PolicyName="IsolatedProbe")["PolicyDocument"] == diagnostic_policy(self.release_id),
                    "report-probe-role-policy-drift")
            self.iam.delete_role_policy(RoleName=name, PolicyName="IsolatedProbe")
        self.iam.delete_role(RoleName=name)
        try:
            self.iam.get_role(RoleName=name)
        except Exception as exc:
            require(getattr(exc, "response", {}).get("Error", {}).get("Code") == "NoSuchEntity", "report-probe-role-cleanup-unconfirmed")
        else:
            raise RuntimeError("report-probe-role-still-exists")
        self.role_created = self.role_attempted = False

    def probe(self, image):
        self.checkpoint()
        self.provider_overlap(dispatch=True)
        original_outputs = self.live_outputs()
        ecs = self.original["Target"]["EcsParameters"]
        network = ecs["NetworkConfiguration"]["awsvpcConfiguration"]
        network = {k[:1].lower() + k[1:]: v for k, v in network.items()}
        self.start_uncertain = True
        result = self.ecs.run_task(cluster=CLUSTER, taskDefinition=self.probe_definition["taskDefinitionArn"],
                                   launchType="FARGATE", count=1, startedBy=self.release_id,
                                   clientToken=self.release_id,
                                   networkConfiguration={"awsvpcConfiguration": network})
        require(not result.get("failures") and len(result.get("tasks", [])) == 1, "report-probe-start-failed")
        self.owned_task = result["tasks"][0]["taskArn"]
        self.start_uncertain = False
        ready = None
        deadline = self.clock() + 7200
        next_overlap = self.clock() + 60
        try:
            while self.clock() < deadline:
                self.checkpoint(poll=True)
                if self.clock() >= next_overlap:
                    self.provider_overlap()
                    next_overlap = self.clock() + 60
                task = self.task()
                if ready is None:
                    try:
                        raw = read_private(self.s3, self.prefix + "markers/ready.json")
                    except Exception as exc:
                        require(getattr(exc, "response", {}).get("Error", {}).get("Code") in {"NoSuchKey", "404"},
                                "report-ready-read-failed")
                    else:
                        ready = json.loads(raw)
                        c = task["containers"][0]
                        ips = [row["privateIpv4Address"] for row in c.get("networkInterfaces", [])]
                        require(ready["task_arn"] == self.owned_task and ready["task_definition"] == task["taskDefinitionArn"]
                                and ips == [ready["private_ip"]] and ready["image_digest"] == c.get("imageDigest") == image.rsplit("@", 1)[1]
                                and ready["path"] == "/app" and ready["service"] == SERVICE and ready["release_id"] == self.release_id
                                and ready["source_commit"] == self.commit and ready["gate_sha256"] == self.gate_sha,
                                "report-pre-provider-host-identity-failed")
                        self.event("host-identity-verified-before-provider", identity=ready)
                        signal = canonical({"phase": "host-authorized", "release_id": self.release_id,
                                            "task_arn": self.owned_task, "ready_sha256": sha(raw)})
                        self.s3.put_object(Bucket=BUCKET, Key=self.prefix + "authorize.json", Body=signal,
                                           ExpectedBucketOwner=ACCOUNT, ServerSideEncryption="AES256", IfNoneMatch="*")
                        require(read_private(self.s3, self.prefix + "authorize.json") == signal, "report-signal-readback")
                if task["lastStatus"] == "STOPPED":
                    break
                self.sleep(10)
            require(ready and task["lastStatus"] == "STOPPED" and task["containers"][0].get("exitCode") == 0,
                    "report-probe-failed-or-timeout")
            complete = json.loads(read_private(self.s3, self.prefix + "markers/complete.json"))
            require(all(complete.get(k) == v for k, v in ready.items()) and complete.get("phase") == "report-verified",
                    "report-completion-identity-invalid")
            require(complete.get("localhost_marker_sha256") == sha(canonical(ready))
                    and all(complete.get(k) is False for k in ("provider_writes", "email_sent", "live_outputs_changed"))
                    and all(complete.get(k) is True for k in ("skip_invoices", "skip_inline_guard"))
                    and complete.get("output_manifest_key") == self.prefix + "artifacts/output-manifest.json",
                    "report-completion-safety-invalid")
            manifest = read_private(self.s3, complete["output_manifest_key"])
            require(sha(manifest) == complete["output_manifest_sha256"], "report-output-manifest-mismatch")
            artifacts = json.loads(manifest)["artifacts"]
            expected = {"data_quality.json", "report_latest.html", "dashboard_payload_latest.json"}
            expected.update(f"{kind}_{period}.{extension}" for period in ("7d", "30d", "90d")
                            for kind, extension in (("report", "html"), ("dashboard_payload", "json")))
            require(set(artifacts) == expected, "report-manifest-incomplete")
            for entry in artifacts.values():
                require(entry["key"].startswith(self.prefix + "artifacts/")
                        and type(entry["size"]) is int and 0 < entry["size"] <= 64 * 1024 * 1024
                        and sha(read_private(self.s3, entry["key"], entry["size"] + 1)) == entry["sha256"],
                        "report-output-artifact-mismatch")
            require(self.live_outputs() == original_outputs, "report-live-output-generation-changed")
            self.provider_overlap()
            self.event("isolated-output-readback-verified", live_outputs_sha256=self.binding.sha256(original_outputs),
                       manifest_sha256=complete["output_manifest_sha256"], observed_overlap_count=len(self.observed_overlaps))
            return {"task_arn": self.owned_task, "private_ip": ready["private_ip"], "definition_arn": task["taskDefinitionArn"],
                    "image_digest": ready["image_digest"], "instance_id": "N/A:Fargate", "service": SERVICE, "path": "/app",
                    "localhost_marker_sha256": complete["localhost_marker_sha256"],
                    "command": self.probe_definition["containerDefinitions"][0]["command"], "exit_code": 0, "stopped": True,
                    **{k: complete[k] for k in ("output_manifest_key", "output_manifest_sha256", "provider_writes", "email_sent",
                                               "live_outputs_changed", "skip_invoices", "skip_inline_guard")}}
        finally:
            self.cleanup_task()

    def rollback(self):
        self.cleanup_task()
        require(not self.start_uncertain, "report-run-task-response-uncertain")
        observed = self.schedule()
        require(self.known_schedule is not None and self.binding.schedule_snapshot(observed) ==
                self.binding.schedule_snapshot(self.known_schedule), "report-rollback-concurrent-change")
        self.update({**schedule_request(observed), "State": "DISABLED"})
        self.drain()
        self.update({**schedule_request(self.original), "State": "DISABLED"})
        self.update(self.original)
        current = self.binding.load_current_binding(self.s3, lease=self.lease)
        if current["record_sha256"] != self.previous["record_sha256"]:
            require(current["record"]["release_id"] == self.release_id, "report-rollback-pointer-not-owned")
            proof = {"workflow_run_id": os.environ["GITHUB_RUN_ID"], "observed_at": now().isoformat(),
                     "restored_schedule_sha256": self.binding.sha256(self.binding.schedule_snapshot(self.schedule())),
                     "restored_definition_sha256": self.binding.sha256(self.previous["record"]["task_definition"]),
                     "failed_record_sha256": current["record_sha256"]}
            current = self.binding.restore_previous_binding(self.s3, self.previous, expected_pointer_etag=current["pointer_etag"],
                                                            lease=self.lease, rollback_proof=proof)
        self.binding.validate_runtime(current, self.schedule(), self.definition(self.original["Target"]["EcsParameters"]["TaskDefinitionArn"]))
        self.checkpoint()
        self.event("rollback-readback-verified")

    def pause_known_target(self):
        """Never overwrite foreign drift while retaining an unresolved incident."""
        if self.known_schedule is not None:
            self.update({**schedule_request(self.known_schedule), "State": "DISABLED"})

    def release_verified(self, expected):
        self.release_attempted = True
        self.lease.release()
        observed = self.binding.load_current_binding(self.s3)
        require(observed == {**expected, "migration_etag": self.lease.etag}, "report-terminal-authority-drift")
        self.binding.validate_runtime(observed, self.schedule(), self.definition(
            observed["record"]["task_definition"]["taskDefinitionArn"]))
        self.binding.verify_managed_provenance(observed, require_completed=False)

    def run(self):
        current_main(self.commit)
        require(self.session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "report-account-invalid")
        self.binding.require_private_bucket(self.s3)
        self.lease.acquire()
        touched = False
        try:
            self.previous = self.binding.load_current_binding(self.s3, lease=self.lease)
            self.binding.verify_managed_provenance(self.previous, require_completed=True)
            self.original = self.schedule()
            source = self.definition(self.original["Target"]["EcsParameters"]["TaskDefinitionArn"])
            self.original_source = source
            self.binding.validate_runtime(self.previous, self.original, source)
            self.known_schedule = self.original
            self.readiness()
            self.checkpoint()
            self.existing_host()  # Account/task/IP/service/path before the first mutation.
            image, build_id = self.exact_image()
            self.gate_sha = sha((ROOT / "scripts/reporting_migration_host_gate.py").read_bytes())
            production = candidate_definition(source, image)
            diagnostic = candidate_definition(source, image, self.release_id, source_commit=self.commit, gate_sha256=self.gate_sha)
            self.event("preflight-verified", original=self.original, source=source, protected_sha256=self.binding.sha256(self.protected))
            current_main(self.commit)
            self.checkpoint()
            touched = True  # Set before the possible pause write, including uncertain responses.
            self.update({**schedule_request(self.original), "State": "DISABLED"})
            self.drain()
            self.create_role()
            self.production_definition = self.ecs.register_task_definition(**production)["taskDefinition"]
            self.probe_definition = self.ecs.register_task_definition(**diagnostic)["taskDefinition"]
            for requested, registered in ((production, self.production_definition), (diagnostic, self.probe_definition)):
                require(self.binding.definition_snapshot(registered) == self.binding.definition_snapshot(
                    {**requested, "taskDefinitionArn": registered["taskDefinitionArn"]}), "report-registered-definition-drift")
            proof = self.probe(image)
            self.cleanup_role()
            current_main(self.commit)
            self.checkpoint()
            self.drain()
            require(self.exact_image() == (image, build_id), "report-final-build-or-image-drift")
            current_main(self.commit)
            desired = schedule_request(self.original)
            desired["Target"]["EcsParameters"]["TaskDefinitionArn"] = self.production_definition["taskDefinitionArn"]
            # Validate the complete published contract before enabling its task.
            record = self.binding.build_promotion_record(self.previous, desired, self.production_definition,
                release_id=self.release_id, source_commit=self.commit, image=image, workflow_run_id=os.environ["GITHUB_RUN_ID"],
                build_run_id=build_id, protected_sha256=self.binding.sha256(self.protected), candidate_proof=proof,
                candidate_task_definition=self.probe_definition, verified_at=now().isoformat())
            self.event("candidate-verified", proof=proof, desired=desired)
            self.update({**desired, "State": "DISABLED"})
            self.update(desired)
            self.checkpoint()
            self.binding.validate_runtime({"record": record, "record_sha256": self.binding.sha256(record)},
                                          self.schedule(), self.definition(self.production_definition["taskDefinitionArn"]))
            current = self.binding.publish_verified_binding(self.s3, record, expected_pointer_etag=self.previous["pointer_etag"], lease=self.lease)
            self.binding.validate_runtime(current, self.schedule(), self.definition(self.production_definition["taskDefinitionArn"]))
            self.checkpoint()
            self.event("promotion-readback-verified", record_sha256=current["record_sha256"])
            self.release_verified(current)
        except Exception:
            try:
                require(not self.release_attempted, "report-terminal-release-unconfirmed")
                if touched:
                    self.rollback()
                self.cleanup_role()
                if touched:
                    self.release_verified(self.binding.load_current_binding(self.s3, lease=self.lease))
                else:
                    self.lease.release()
            except Exception:
                try:
                    if touched:
                        self.pause_known_target()
                finally:
                    self.lease.retain_uncertain()
                raise RuntimeError("report-recovery-unconfirmed-retained-lease") from None
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", required=True)
    parser.add_argument("--mode", choices=("bootstrap", "promote"), required=True)
    parser.add_argument("--readiness-key", default="")
    parser.add_argument("--readiness-sha256", default="")
    args = parser.parse_args()
    require(re.fullmatch(r"[a-f0-9]{40}", args.commit), "report-source-invalid")
    import boto3
    from botocore.config import Config
    with ExitStack() as stack:
        original = boto3.Session(region_name=REGION)
        clients = {}
        class Session:
            def client(self, name):
                if name not in clients:
                    clients[name] = stack.enter_context(closing(original.client(name, config=Config(
                        connect_timeout=5, read_timeout=20, retries={"total_max_attempts": 1}))))
                return clients[name]
        deployment = Deployment(Session(), args.commit, args.readiness_key, args.readiness_sha256)
        if args.mode == "bootstrap":
            require(not args.readiness_key and not args.readiness_sha256, "report-baseline-extra-inputs")
            deployment.bootstrap()
        else:
            deployment.run()
    print("VEVO_REPORT_DEPLOY_OK:immutable:private-evidence:protected-runtime-unchanged")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("VEVO_REPORT_DEPLOY_FAILED:" + type(exc).__name__, flush=True)
        raise SystemExit(1) from None
