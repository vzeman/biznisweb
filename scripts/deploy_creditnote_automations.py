#!/usr/bin/env python3
"""Inspect by default; main-only migration of standalone creditnote guards.

Apply pauses and drains all affected writers before API-heavy probes. A complete
live guard run is required before disabling the old inline guard. Future daily
completion is monitored; unchanged report images cannot wait for that result.
Private CAS evidence records partial failures; uncertain live work leaves reports
paused. Infrastructure additions may remain unused after a failed deployment.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import hashlib
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
from scripts.deploy_order_automations import TASK_FIELDS, SCHEDULES as PROTECTED, established_alarm_route, require, schedule_request  # noqa: E402
from scripts.creditnote_automation_host_gate import MARKER as GUARD_MARKER, verify_summary  # noqa: E402
from scripts.reporting_guard_identity_probe import MARKER as REPORT_MARKER, SKIP  # noqa: E402

PROJECTS = ("roy", "vevo")
REPORT_PINS = {
    "roy": (71, "9ff4738f998e3d80e7b76dc543f11bc36413d9d016e72b4a78eb3411433bc541", "fcf6e26341b1e4c1d47f71f6f688b3e09dcbe14b"),
    "vevo": (33, "30a23fcd69eb2d7a41195bffa0bc055d38bc2dd706e9eb07d5126675a21a6add", "e55ccd14b47c660b9b39a5788a1e65a63a98fc1a"),
}
GUARD_SECRET_NAMES = {"BIZNISWEB_API_URL", "BIZNISWEB_API_TOKEN", "BIZNISWEB_USERNAME", "BIZNISWEB_PASSWORD"}


def family(project):
    require(project in PROJECTS, "project-not-allowed")
    return f"{project}-creditnote-storno-guard"


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def comparable_definition(definition):
    """Compare ECS named environment entries without changing stored evidence."""
    result = {key: copy.deepcopy(definition[key]) for key in TASK_FIELDS if key in definition}
    containers = result.get("containerDefinitions", [])
    require(isinstance(containers, list) and all(isinstance(row, dict) for row in containers),
            "definition-containers-invalid")
    for container in containers:
        if "environment" not in container:
            continue
        rows = container["environment"]
        require(isinstance(rows, list), "definition-environment-invalid")
        names = set()
        for row in rows:
            require(isinstance(row, dict) and set(row) == {"name", "value"}
                    and isinstance(row["name"], str) and bool(row["name"])
                    and isinstance(row["value"], str) and row["name"] not in names,
                    "definition-environment-invalid")
            names.add(row["name"])
        container["environment"] = sorted(rows, key=lambda row: row["name"])
    return result


def report_source_hash(commit):
    require(commit in {row[2] for row in REPORT_PINS.values()}, "report-source-commit-not-pinned")
    available = subprocess.run(["git", "cat-file", "-e", f"{commit}:daily_report_runner.py"], cwd=ROOT, capture_output=True)
    if available.returncode:
        subprocess.run(["git", "fetch", "--no-tags", "origin", commit], cwd=ROOT, check=True, capture_output=True)
    source = subprocess.check_output(["git", "show", f"{commit}:daily_report_runner.py"], cwd=ROOT)
    return hashlib.sha256(source).hexdigest()


def require_main(commit):
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()
    require(re.fullmatch(r"[a-f0-9]{40}", commit) is not None, "commit-invalid")
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_SHA") == commit, "apply-requires-managed-main")
    require(git("rev-parse", "HEAD") == commit and not git("status", "--porcelain"), "source-not-clean-exact-main")
    require(git("ls-remote", "origin", "refs/heads/main").split()[0] == commit, "main-moved-during-deployment")


def report_override(snapshot):
    """Preserve every existing Input field; add only this environment override."""
    result = schedule_request(snapshot)
    raw = result["Target"].get("Input")
    payload = json.loads(raw) if raw else {}
    require(isinstance(payload, dict), "report-input-not-object")
    overrides = payload.setdefault("containerOverrides", [])
    require(isinstance(overrides, list) and all(isinstance(row, dict) for row in overrides), "report-input-invalid-containers")
    matches = [row for row in overrides if row.get("name") == "reporting"]
    require(len(matches) <= 1, "report-input-duplicate-container")
    if not matches:
        matches = [{"name": "reporting"}]
        overrides.extend(matches)
    env = matches[0].setdefault("environment", [])
    require(isinstance(env, list) and all(isinstance(row, dict) and isinstance(row.get("name"), str) for row in env),
            "report-input-invalid-environment")
    existing = [row for row in env if row["name"] == SKIP]
    require(len(existing) <= 1, "report-input-duplicate-skip")
    if existing:
        existing[0]["value"] = "true"
    else:
        env.append({"name": SKIP, "value": "true"})
    result["Target"]["Input"] = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return result


def verify_report_launch(snapshot, container, project):
    # Initial migration only: the reviewed schedules have no Input override.
    # Both pinned Dockerfiles use this exact CMD; no wrapper or additional flags
    # may bypass the parser/skip branch verified by the diagnostic host probe.
    require("Input" not in snapshot["Target"], "frozen-report-input-not-reviewed")
    require(container.get("command") in (None, ["python", "daily_report_runner.py"]), "frozen-report-command-drift")
    project_env = [row.get("value") for row in container.get("environment", []) if row.get("name") == "REPORT_PROJECT"]
    require(project_env == [project] and not any(row.get("name") in {"REPORT_PROJECT", SKIP}
            for row in container.get("secrets", [])), "frozen-report-project-binding-drift")


def guard_secret_references(container, project, account):
    """Copy exact API/native-read references from the selected project's source."""
    rows = container.get("secrets", [])
    require(isinstance(rows, list) and all(isinstance(row, dict) for row in rows), "guard-secrets-invalid")
    selected = [row for row in rows if row.get("name") in GUARD_SECRET_NAMES]
    require(len(selected) == len(GUARD_SECRET_NAMES) and {row["name"] for row in selected} == GUARD_SECRET_NAMES,
            "guard-required-secret-missing-or-duplicate")
    bindings = set()
    for row in selected:
        reference = row.get("valueFrom")
        require(isinstance(reference, str) and reference and reference == reference.strip(), "guard-secret-reference-empty-or-invalid")
        parts = reference.split(":")
        require(len(parts) == 10 and parts[:6] == ["arn", "aws", "secretsmanager", "eu-central-1", account, "secret"]
                and re.fullmatch(re.escape(project) + r"/reporting/runtime-env-[A-Za-z0-9]{6}", parts[6]) is not None
                and parts[7] == row["name"] and not (parts[8] and parts[9]), "guard-secret-project-binding-drift")
        bindings.add((parts[6], parts[8], parts[9]))
    require(len(bindings) == 1, "guard-secret-source-binding-split")
    return copy.deepcopy(selected)


def guard_definition(source, project, image, role, bucket, prefix):
    require(re.fullmatch(r"[0-9]{12}\.dkr\.ecr\.eu-central-1\.amazonaws\.com/vevo-reporting@sha256:[a-f0-9]{64}", image) is not None,
            "guard-image-not-immutable")
    require(source.get("family") == f"{project}-invoice-daily" and source.get("networkMode") == "awsvpc", "guard-source-drift")
    result = {key: copy.deepcopy(source[key]) for key in TASK_FIELDS if key in source}
    containers = result.get("containerDefinitions", [])
    require(len(containers) == 1 and containers[0].get("name") == "reporting", "guard-container-drift")
    c = containers[0]
    require(c.get("command") == ["python", "invoice_runner.py", "--project", project], "guard-source-command-drift")
    result.update(family=family(project), taskRoleArn=role)
    c.update(image=image, workingDirectory="/app", command=["python", "creditnote_storno_runner.py", "--project", project])
    require(not c.get("entryPoint"), "guard-source-entrypoint-drift")
    c["environment"] = [{"name": name, "value": value} for name, value in sorted({
        "REPORT_PROJECT": project, "REPORT_SKIP_PROJECT_ENV": "true", "REPORT_S3_BUCKET": bucket,
        "REPORT_S3_PREFIX": prefix, "ORDER_AUTOMATION_STATE_PREFIX": f"data/{project}/order-automation",
    }.items())]
    c["secrets"] = guard_secret_references(c, project, image.split(".", 1)[0])
    c["logConfiguration"] = {"logDriver": "awslogs", "options": {"awslogs-group": f"/ecs/{family(project)}",
        "awslogs-region": "eu-central-1", "awslogs-stream-prefix": "ecs"}}
    return result


def state_policy(account, bucket, project, prefix):
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": [
            f"arn:aws:s3:::{bucket}/data/{project}/order-automation/state.json",
            f"arn:aws:s3:::{bucket}/{prefix}/state/creditnote_status_change_audit.json"]},
        {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": f"arn:aws:s3:::{bucket}", "Condition": {
            "StringEquals": {"s3:prefix": [f"data/{project}/order-automation/state.json",
                                          f"{prefix}/state/creditnote_status_change_audit.json"]}}},
        {"Effect": "Allow", "Action": "cloudwatch:PutMetricData", "Resource": "*",
         "Condition": {"StringEquals": {"cloudwatch:namespace": "BizniswebReporting"}}},
    ]}


class CreditnoteDeployment:
    def __init__(self, session, commit, *, clock=time.monotonic, sleep=time.sleep):
        self.session, self.commit, self.clock, self.sleep = session, commit, clock, sleep
        self.ecs, self.scheduler, self.s3 = (session.client(name) for name in ("ecs", "scheduler", "s3"))
        self.account = session.client("sts").get_caller_identity()["Account"]
        self.cluster = f"arn:aws:ecs:eu-central-1:{self.account}:cluster/vevo-reporting-cluster"
        self.evidence = {"schema": 1, "commit": commit, "created_at": datetime.now(timezone.utc).isoformat(), "hosts": []}
        self.key = f"data/roy/order-automation/creditnote-deployments/{commit}/{uuid.uuid4().hex}.json"
        self.etag = None
        self.expected = {}

    def get_schedule(self, name):
        try:
            return self.scheduler.get_schedule(Name=name)
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") == "ResourceNotFoundException":
                return None
            raise

    def save(self, phase):
        self.evidence["phase"] = phase
        body = json.dumps(self.evidence, sort_keys=True, default=str).encode()
        condition = {"IfMatch": self.etag} if self.etag else {"IfNoneMatch": "*"}
        self.s3.put_object(Bucket=self.bucket, Key=self.key, Body=body, ServerSideEncryption="AES256",
                           ContentType="application/json", ExpectedBucketOwner=self.account, **condition)
        actual = self.s3.get_object(Bucket=self.bucket, Key=self.key, ExpectedBucketOwner=self.account)
        require(actual["Body"].read() == body and actual.get("ServerSideEncryption") == "AES256", "private-evidence-readback-failed")
        self.etag = actual["ETag"]

    def no_capture(self):
        for status in ("queued", "in_progress", "waiting", "requested", "pending"):
            for page in range(1, 11):
                raw = subprocess.check_output(["gh", "api", f"repos/vzeman/biznisweb/actions/runs?status={status}&per_page=100&page={page}"], cwd=ROOT)
                runs = json.loads(raw)["workflow_runs"]
                for run in runs:
                    path = str(run.get("path", "")).lower()
                    if "growthbook" in path and any(x in path for x in ("aa", "quality", "source", "capture")):
                        require(run.get("status") == "completed", "active-aa-capture-workflow")
                if len(runs) < 100:
                    break
            else:
                raise RuntimeError("capture-workflow-inventory-incomplete")
        clusters = self.clusters()
        require(self.cluster in clusters, "reporting-cluster-not-in-inventory")
        task_count = 0
        for cluster in clusters:
            for task in self.tasks(None, cluster=cluster):
                task_count += 1
                name = task["taskDefinitionArn"].rsplit("/", 1)[-1].rsplit(":", 1)[0]
                # Collector-image one-off probes can run in the collector cluster.
                # Only an ordinary ECS service task is exempt from the capture gate.
                overrides = task.get("overrides", {}).get("containerOverrides", [])
                collector_service = (
                    name in {"vevo-growthbook-collector-preview", "vevo-growthbook-collector-production"}
                    and cluster.rsplit("/", 1)[-1] == name
                    and task.get("group") == f"service:{name}"
                    and str(task.get("startedBy", "")).startswith("ecs-svc/")
                    and not any(row.get("command") or row.get("environment") for row in overrides)
                )
                require(not (task["lastStatus"] != "STOPPED" and "growthbook" in name and not collector_service),
                        "active-growthbook-capture-task")
        require(self.clusters() == clusters, "capture-cluster-inventory-changed")
        self.evidence["capture_inventory"] = {"checked_at": datetime.now(timezone.utc).isoformat(),
            "clusters": clusters, "task_count": task_count, "active_nonservice_growthbook_tasks": 0}

    def clusters(self):
        result, token, seen = set(), None, set()
        for _ in range(20):
            request = {"maxResults": 100}
            if token:
                request["nextToken"] = token
            page = self.ecs.list_clusters(**request)
            rows = page.get("clusterArns", [])
            require(isinstance(rows, list) and all(isinstance(row, str) and re.fullmatch(
                rf"arn:aws:ecs:eu-central-1:{self.account}:cluster/[A-Za-z0-9_-]+", row) for row in rows),
                "capture-cluster-identity-drift")
            result.update(rows)
            token = page.get("nextToken")
            if not token:
                return sorted(result)
            require(token not in seen, "capture-cluster-pagination-cycle")
            seen.add(token)
        raise RuntimeError("capture-cluster-pagination-incomplete")

    def tasks(self, families, *, cluster=None):
        cluster = cluster or self.cluster
        require(re.fullmatch(rf"arn:aws:ecs:eu-central-1:{self.account}:cluster/[A-Za-z0-9_-]+", cluster) is not None,
                "task-cluster-identity-drift")
        arns = set()
        for item in families or [None]:
            for status in ("RUNNING", "STOPPED"):
                token, seen = None, set()
                for _ in range(100):
                    request = {"cluster": cluster, "desiredStatus": status, "maxResults": 100}
                    if item:
                        request["family"] = item
                    if token:
                        request["nextToken"] = token
                    page = self.ecs.list_tasks(**request)
                    arns.update(page.get("taskArns", []))
                    token = page.get("nextToken")
                    if not token:
                        break
                    require(token not in seen, "task-pagination-cycle")
                    seen.add(token)
                else:
                    raise RuntimeError("task-pagination-incomplete")
        result = []
        ordered = sorted(arns)
        for offset in range(0, len(ordered), 100):
            wanted = ordered[offset:offset + 100]
            page = self.ecs.describe_tasks(cluster=cluster, tasks=wanted)
            rows = page.get("tasks", [])
            require(not page.get("failures") and {x.get("taskArn") for x in rows} == set(wanted), "task-readback-incomplete")
            require(all(x.get("clusterArn") == cluster and x.get("lastStatus") for x in rows), "task-identity-drift")
            if families:
                require(all(x["taskDefinitionArn"].rsplit("/", 1)[-1].rsplit(":", 1)[0] in families for x in rows), "task-family-drift")
            result.extend(rows)
        return result

    def inspect(self):
        self.no_capture()
        self.originals = {name: self.get_schedule(name) for name in [*PROTECTED, *(f"{p}-daily-report-email" for p in PROJECTS)]}
        require(all(self.originals.values()), "existing-schedule-missing")
        self.old_guards = {family(p): self.get_schedule(family(p)) for p in PROJECTS}
        require(not any(self.old_guards.values()), "guard-migration-already-started-review-private-evidence")
        require(all(row.get("State") == "ENABLED" for row in self.originals.values()), "existing-schedule-not-enabled")
        for name, snapshot in self.originals.items():
            require(snapshot.get("Name") == name and snapshot.get("GroupName", "default") == "default"
                    and snapshot["Target"]["Arn"] == self.cluster, "existing-schedule-identity-drift")
            parameters = snapshot["Target"]["EcsParameters"]
            require(parameters.get("TaskCount", 1) == 1 and parameters.get("LaunchType") == "FARGATE", "existing-schedule-task-mode-drift")
            if name in PROTECTED:
                expected = f"arn:aws:ecs:eu-central-1:{self.account}:task-definition/{PROTECTED[name]}:"
                require(parameters["TaskDefinitionArn"].startswith(expected)
                        and parameters["TaskDefinitionArn"][len(expected):].isdigit(), "protected-task-family-drift")
        self.definitions, self.locations, self.report_sources = {}, {}, {}
        from reporting_core.storage import resolve_report_s3_location
        for project in PROJECTS:
            report = self.originals[f"{project}-daily-report-email"]
            revision, digest, source_commit = REPORT_PINS[project]
            arn = f"arn:aws:ecs:eu-central-1:{self.account}:task-definition/{project}-reporting-daily:{revision}"
            require(report["Target"]["EcsParameters"]["TaskDefinitionArn"] == arn and report["Target"]["Arn"] == self.cluster,
                    "frozen-report-task-pin-drift")
            require(report["ScheduleExpression"] == ("cron(30 1 * * ? *)" if project == "roy" else "cron(0 1 * * ? *)")
                    and report.get("ScheduleExpressionTimezone") == "Europe/Bratislava", "frozen-report-timing-drift")
            td = self.ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
            c = td["containerDefinitions"][0]
            require(len(td["containerDefinitions"]) == 1 and c.get("name") == "reporting" and c["image"].endswith("@sha256:" + digest),
                    "frozen-report-image-drift")
            require(not c.get("entryPoint") and c.get("workingDirectory", "/app") == "/app", "frozen-report-entrypoint-drift")
            verify_report_launch(report, c, project)
            self.definitions[arn] = {k: td[k] for k in TASK_FIELDS if k in td}
            self.report_sources[project] = report_source_hash(source_commit)
            settings = json.loads((ROOT / "projects" / project / "settings.json").read_text(encoding="utf-8"))
            location = resolve_report_s3_location(project, settings, environ={}, required=True)
            env = {row["name"]: row["value"] for row in c.get("environment", [])}
            require((env.get("REPORT_S3_BUCKET"), env.get("REPORT_S3_PREFIX")) == location, "canonical-storage-drift")
            self.locations[project] = location
        require(len({row[0] for row in self.locations.values()}) == 1, "report-storage-bucket-split")
        self.bucket = self.locations["roy"][0]
        self.s3.head_bucket(Bucket=self.bucket, ExpectedBucketOwner=self.account)
        require(self.s3.get_bucket_location(Bucket=self.bucket, ExpectedBucketOwner=self.account).get("LocationConstraint") == "eu-central-1",
                "storage-region-drift")
        block = self.s3.get_public_access_block(Bucket=self.bucket, ExpectedBucketOwner=self.account)["PublicAccessBlockConfiguration"]
        require(all(block.get(k) is True for k in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")),
                "storage-not-private")
        self.evidence.update(original_schedules=self.originals, previous_guards=self.old_guards,
                             report_definition_hashes={k: fingerprint(v) for k, v in self.definitions.items()})
        return {"report_pins": 2, "protected_schedules": len(PROTECTED), "active_capture": False,
                "report_definition_hashes": self.evidence["report_definition_hashes"], "apply": False}

    def check_sources(self):
        for arn, original in self.definitions.items():
            actual = self.ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
            require(comparable_definition(actual) == comparable_definition(original), "frozen-report-definition-drift")

    def update(self, name, request):
        current = self.get_schedule(name)
        expected = self.expected.get(name)
        require((schedule_request(current) if current else None) == expected, "schedule-concurrent-drift")
        self.expected[name] = request  # Include an uncertain API response in rollback.
        self.evidence["expected_schedules"] = copy.deepcopy(self.expected)
        self.save("before-schedule-write")
        if current is None:
            self.scheduler.create_schedule(**request)
        else:
            self.scheduler.update_schedule(**request)
        require(schedule_request(self.get_schedule(name)) == request, "schedule-write-readback-failed")

    def drain(self, families, timeout=1800, quiet=120):
        end, since = self.clock() + timeout, None
        while self.clock() < end:
            for name, expected in self.expected.items():
                if expected is not None:
                    require(schedule_request(self.get_schedule(name)) == expected and expected["State"] == "DISABLED", "paused-schedule-drift")
            active = [x for x in self.tasks(families) if x["lastStatus"] != "STOPPED"]
            now = self.clock()
            if active:
                since = None
            elif since is None:
                since = now
            elif now - since >= quiet:
                return
            self.sleep(10)
        raise RuntimeError("writer-drain-timeout-no-natural-task-stopped")

    def check_schedules(self):
        for name, expected in self.expected.items():
            current = self.get_schedule(name)
            require((schedule_request(current) if current else None) == expected, "schedule-concurrent-drift")
        self.check_sources()

    def pause(self):
        self.expected = {name: schedule_request(row) for name, row in self.originals.items()}
        self.expected.update({name: None for name in self.old_guards})
        self.save("before-writer-pause")
        for name in self.originals:
            paused = copy.deepcopy(self.expected[name])
            paused["State"] = "DISABLED"
            self.update(name, paused)
        self.save("writers-paused")
        self.drain({*PROTECTED.values(), *(f"{p}-reporting-daily" for p in PROJECTS), *(family(p) for p in PROJECTS)})
        self.save("writers-drained")

    def restore(self, *, live_attempted):
        """Never replay business work or overwrite an independently changed schedule."""
        failures = []
        for name in reversed(list(self.expected)):
            try:
                current = self.get_schedule(name)
                current = schedule_request(current) if current else None
                original = schedule_request(self.originals[name]) if name in self.originals else None
                if current not in (self.expected[name], original):
                    failures.append(name)
                    continue
                if original is None:
                    if current is None:
                        continue
                    desired = copy.deepcopy(current)
                    desired["State"] = "DISABLED"
                else:
                    desired = original
                    if live_attempted and name.endswith("-daily-report-email"):
                        desired["State"] = "DISABLED"
                self.expected[name] = current
                self.update(name, desired)
            except Exception:
                failures.append(name)
        self.evidence.update(rollback_failed_schedules=failures, reports_retained_paused=live_attempted)
        self.save("failure-reports-paused" if live_attempted else "failure-original-schedules-restored")
        require(not failures, "rollback-incomplete-review-private-evidence")

    def create_role(self, name, service, policy, *, source_arn=None):
        # A failed migration may retain an unused role. Never adopt, overwrite or
        # delete it implicitly; each managed attempt owns distinct infrastructure.
        attempt = self.key.rsplit("/", 1)[-1].removesuffix(".json")
        require(re.fullmatch(r"[a-f0-9]{32}", attempt) is not None, "role-attempt-invalid")
        name = f"{name}-{attempt[:12]}"
        iam = self.session.client("iam")
        statement = {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole",
                     "Condition": {"StringEquals": {"aws:SourceAccount": self.account}}}
        if source_arn:
            statement["Condition"]["ArnLike"] = {"aws:SourceArn": source_arn}
        trust = {"Version": "2012-10-17", "Statement": [statement]}
        try:
            prior = iam.get_role(RoleName=name)
        except Exception as exc:
            require(getattr(exc, "response", {}).get("Error", {}).get("Code") == "NoSuchEntity", "role-inspection-failed")
            prior = None
        # Initial migration only: never silently adopt or overwrite an existing role.
        require(prior is None, "guard-role-already-exists-review-private-evidence")
        result = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust),
                                 Tags=[{"Key": "ManagedBy", "Value": "biznisweb-creditnote-automation"}])
        arn = result["Role"]["Arn"]
        require(arn == f"arn:aws:iam::{self.account}:role/{name}", "guard-role-identity-drift")
        iam.put_role_policy(RoleName=name, PolicyName="CreditnoteAutomation", PolicyDocument=json.dumps(policy))
        actual = iam.get_role(RoleName=name)["Role"]
        require(actual.get("Arn") == arn and actual.get("AssumeRolePolicyDocument") == trust, "guard-role-readback-failed")
        require(iam.get_role_policy(RoleName=name, PolicyName="CreditnoteAutomation")["PolicyDocument"] == policy,
                "guard-policy-readback-failed")
        self.evidence.setdefault("created_roles", []).append(arn)
        self.save("guard-role-created")
        # GetRole visibility alone does not prove that ECS/Scheduler can yet
        # assume a new role. Allow IAM propagation before any consumer starts,
        # then revalidate without widening trust or retrying an uncertain launch.
        for _ in range(6):
            self.sleep(10)
        actual = iam.get_role(RoleName=name)["Role"]
        require(actual.get("Arn") == arn and actual.get("AssumeRolePolicyDocument") == trust,
                "guard-role-post-propagation-drift")
        require(iam.get_role_policy(RoleName=name, PolicyName="CreditnoteAutomation")["PolicyDocument"] == policy,
                "guard-policy-post-propagation-drift")
        self.save("guard-role-propagation-wait-complete")
        return arn

    def provision(self, project, image):
        service = family(project)
        bucket, prefix = self.locations[project]
        role = self.create_role(f"BiznisWebCreditnoteGuard-{project}", "ecs-tasks.amazonaws.com",
                                state_policy(self.account, bucket, project, prefix),
                                source_arn=f"arn:aws:ecs:eu-central-1:{self.account}:*")
        source_schedule = self.originals[f"{project}-daily-invoice-generation"]
        source = self.ecs.describe_task_definition(taskDefinition=source_schedule["Target"]["EcsParameters"]["TaskDefinitionArn"])["taskDefinition"]
        definition = guard_definition(source, project, image, role, bucket, prefix)
        logs = self.session.client("logs")
        try:
            logs.create_log_group(logGroupName=f"/ecs/{service}")
        except Exception as exc:
            require(getattr(exc, "response", {}).get("Error", {}).get("Code") == "ResourceAlreadyExistsException", "guard-log-provision-failed")
        logs.put_retention_policy(logGroupName=f"/ecs/{service}", retentionInDays=30)
        arn = self.ecs.register_task_definition(**definition)["taskDefinition"]["taskDefinitionArn"]
        actual = self.ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
        require(comparable_definition(actual) == comparable_definition(definition), "guard-definition-readback-failed")
        self.evidence.setdefault("candidate_definitions", {})[project] = {"arn": arn, "definition": definition}
        self.save("guard-candidate-registered")
        return arn, definition

    def read_logs(self, group, stream, prefixes):
        token, seen, output = None, set(), {name: [] for name in prefixes}
        for _ in range(100):
            request = {"logGroupName": group, "logStreamName": stream, "startFromHead": True}
            if token:
                request["nextToken"] = token
            page = self.session.client("logs").get_log_events(**request)
            for event in page.get("events", []):
                text = event.get("message", "")
                for prefix in prefixes:
                    if text.startswith(prefix + " "):
                        output[prefix].append(json.loads(text[len(prefix) + 1:]))
            next_token = page.get("nextForwardToken")
            if next_token == token:
                return output
            require(next_token and next_token not in seen, "guard-log-pagination-invalid")
            seen.add(next_token)
            token = next_token
        raise RuntimeError("guard-log-pagination-incomplete")

    def task_run(self, project, arn, schedule, digest, command, *, kind):
        require(kind in {"report-probe", "guard-probe", "guard-live"}, "task-kind-invalid")
        service = f"{project}-reporting-daily" if kind == "report-probe" else family(project)
        require(f":task-definition/{service}:" in arn, "task-family-invalid")
        definition = self.ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
        c = definition["containerDefinitions"][0]
        require(len(definition["containerDefinitions"]) == 1 and c.get("name") == "reporting"
                and c["image"].endswith("@" + digest) and not c.get("entryPoint"), "task-image-or-command-drift")
        if kind == "report-probe":
            overrides = json.loads(report_override(schedule)["Target"]["Input"])
        else:
            overrides = {"containerOverrides": [{"name": "reporting"}]}
        container = next(row for row in overrides["containerOverrides"] if row["name"] == "reporting")
        container["command"] = command
        require(len(json.dumps(overrides).encode()) <= 8192, "guard-command-too-large")
        network = schedule["Target"]["EcsParameters"]["NetworkConfiguration"]["awsvpcConfiguration"]
        network = {key[0].lower() + key[1:]: value for key, value in network.items()}
        token, owner = uuid.uuid4().hex, "creditnote-automation-migration"
        request = dict(cluster=self.cluster, taskDefinition=arn, launchType="FARGATE", count=1,
                       networkConfiguration={"awsvpcConfiguration": network}, overrides=overrides,
                       startedBy=owner, clientToken=token)
        self.evidence.setdefault("task_attempts", []).append({"kind": kind, "project": project,
            "task_definition": arn, "client_token": token, "started_by": owner})
        self.save("before-" + kind)
        # A lost launch response is not retried here. The saved client token permits
        # later read-only attribution; reports stay paused after any live attempt.
        reply = self.ecs.run_task(**request)
        require(not reply.get("failures") and len(reply.get("tasks", [])) == 1, "guard-task-launch-failed")
        own = reply["tasks"][0]["taskArn"]
        self.evidence["task_attempts"][-1]["task"] = own
        try:
            self.save("started-" + kind)
            end, task = self.clock() + 1800, None
            while self.clock() < end:
                reply = self.ecs.describe_tasks(cluster=self.cluster, tasks=[own])
                require(not reply.get("failures") and len(reply.get("tasks", [])) == 1, "guard-task-readback-missing")
                task = reply["tasks"][0]
                require(task.get("taskArn") == own and task.get("taskDefinitionArn") == arn
                        and task.get("clusterArn") == self.cluster and task.get("startedBy") == owner, "guard-task-identity-drift")
                if task.get("lastStatus") == "STOPPED":
                    break
                self.check_schedules()
                self.sleep(10)
            # Do not stop a live business task: that could interrupt an accepted write.
            require(task and task.get("lastStatus") == "STOPPED", "guard-task-timeout-review-private-evidence")
        finally:
            if kind != "guard-live":
                self.cleanup_probe(own, arn, owner, command)
        containers = [row for row in task.get("containers", []) if row.get("name") == "reporting"]
        require(len(containers) == 1 and containers[0].get("exitCode") == 0
                and containers[0].get("imageDigest") == digest and task.get("launchType") == "FARGATE", "guard-task-failed")
        ips = [row["privateIpv4Address"] for row in containers[0].get("networkInterfaces", []) if row.get("privateIpv4Address")]
        require(len(ips) == 1, "guard-task-ip-missing")
        options = c["logConfiguration"]["options"]
        messages = self.read_logs(options["awslogs-group"], f"{options['awslogs-stream-prefix']}/reporting/{own.rsplit('/', 1)[-1]}",
                                  [REPORT_MARKER, GUARD_MARKER, "CREDITNOTE_STANDALONE_SUMMARY"])
        host = {"task": own, "task_definition": arn, "private_ip": ips[0], "image_digest": digest,
                "service": service, "path": "/app", "instance_id": "N/A:FARGATE", "exit_code": 0, "kind": kind}
        if kind == "report-probe":
            expected = {"marker": REPORT_MARKER, "project": project, "service": service,
                "revision": str(REPORT_PINS[project][0]), "task": own, "private_ip": ips[0], "instance_id": "N/A:FARGATE",
                "path": "/app", "source_sha256": self.report_sources[project], "inline_guard_skipped": True,
                "business_runner_started": False}
            require(messages[REPORT_MARKER] == [expected], "report-localhost-proof-missing")
        else:
            summaries = messages["CREDITNOTE_STANDALONE_SUMMARY"]
            require(len(summaries) == 1, "guard-summary-missing")
            verify_summary(summaries[0], project, dry_run=kind == "guard-probe")
            host["summary"] = summaries[0]
            if kind == "guard-probe":
                require(messages[GUARD_MARKER] == [{"marker": GUARD_MARKER, "project": project,
                    "path": "/app", "dry_run": True}], "guard-localhost-proof-missing")
        self.evidence["hosts"].append(host)
        self.save("completed-" + kind)
        return host

    def cleanup_probe(self, own, arn, owner, command):
        reply = self.ecs.describe_tasks(cluster=self.cluster, tasks=[own])
        require(not reply.get("failures") and len(reply.get("tasks", [])) == 1, "probe-cleanup-task-missing")
        task = reply["tasks"][0]
        require(task.get("taskArn") == own and task.get("taskDefinitionArn") == arn and task.get("clusterArn") == self.cluster
                and task.get("startedBy") == owner, "probe-cleanup-identity-drift")
        if task.get("lastStatus") == "STOPPED":
            return
        rows = task.get("overrides", {}).get("containerOverrides", [])
        require(len(rows) == 1 and rows[0].get("name") == "reporting" and rows[0].get("command") == command,
                "probe-cleanup-command-drift")
        self.ecs.stop_task(cluster=self.cluster, task=own, reason="Bounded owned read-only creditnote migration probe cleanup")
        self.ecs.get_waiter("tasks_stopped").wait(cluster=self.cluster, tasks=[own], WaiterConfig={"Delay": 5, "MaxAttempts": 24})
        observed = self.ecs.describe_tasks(cluster=self.cluster, tasks=[own])
        require(not observed.get("failures") and len(observed.get("tasks", [])) == 1
                and observed["tasks"][0].get("taskArn") == own and observed["tasks"][0].get("lastStatus") == "STOPPED",
                "probe-cleanup-not-stopped")

    def monitor_and_schedule(self, project, arn, definition):
        service = family(project)
        sqs, cloudwatch = self.session.client("sqs"), self.session.client("cloudwatch")
        queue_name = service + "-dlq"
        queue = sqs.create_queue(QueueName=queue_name, Attributes={"MessageRetentionPeriod": "1209600", "SqsManagedSseEnabled": "true"})["QueueUrl"]
        attributes = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn", "MessageRetentionPeriod", "SqsManagedSseEnabled"])["Attributes"]
        require(attributes["MessageRetentionPeriod"] == "1209600" and attributes["SqsManagedSseEnabled"] == "true", "guard-dlq-storage-invalid")
        queue_arn = attributes["QueueArn"]
        require(queue_arn == f"arn:aws:sqs:eu-central-1:{self.account}:{queue_name}", "guard-dlq-identity-invalid")
        schedule_arn = f"arn:aws:scheduler:eu-central-1:{self.account}:schedule/default/{service}"
        policy = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "ecs:RunTask", "Resource": arn,
             "Condition": {"ArnEquals": {"ecs:cluster": self.cluster}}},
            {"Effect": "Allow", "Action": "iam:PassRole", "Resource": [definition["taskRoleArn"], definition["executionRoleArn"]],
             "Condition": {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}},
            {"Effect": "Allow", "Action": "sqs:SendMessage", "Resource": queue_arn}]}
        role = self.create_role(f"BiznisWebCreditnoteSchedule-{project}", "scheduler.amazonaws.com", policy,
                                source_arn=f"arn:aws:scheduler:eu-central-1:{self.account}:schedule-group/default")
        queue_policy = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"AWS": role},
            "Action": "sqs:SendMessage", "Resource": queue_arn}]}
        sqs.set_queue_attributes(QueueUrl=queue, Attributes={"Policy": json.dumps(queue_policy)})
        require(json.loads(sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["Policy"])["Attributes"]["Policy"]) == queue_policy,
                "guard-dlq-policy-readback-failed")
        dims = [{"Name": "Project", "Value": project}, {"Name": "RunMode", "Value": "live"}]
        alarms = []
        for suffix, metric, period, comparison, missing in (
            ("run-failed", "RunFailed", 300, "GreaterThanOrEqualToThreshold", "notBreaching"),
            ("missing-completion", "RunSucceeded", 90000, "LessThanThreshold", "breaching"),
            ("review-required", "ReviewRequiredOrders", 300, "GreaterThanOrEqualToThreshold", "notBreaching"),
        ):
            alarms.append(dict(AlarmName=f"{service}-{suffix}", Namespace="BizniswebReporting",
                MetricName="CreditnoteStandalone" + metric, Dimensions=dims, Statistic="Sum", Period=period,
                EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=1, ComparisonOperator=comparison,
                TreatMissingData=missing, ActionsEnabled=True, AlarmActions=[self.alarm_route]))
        alarms.append(dict(AlarmName=f"{service}-dlq-not-empty", Namespace="AWS/SQS",
            MetricName="ApproximateNumberOfMessagesVisible", Dimensions=[{"Name": "QueueName", "Value": queue_name}],
            Statistic="Maximum", Period=300, EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=1,
            ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching",
            ActionsEnabled=True, AlarmActions=[self.alarm_route]))
        for request in alarms:
            cloudwatch.put_metric_alarm(**request)
        actual = cloudwatch.describe_alarms(AlarmNames=[row["AlarmName"] for row in alarms]).get("MetricAlarms", [])
        require(len(actual) == len(alarms) and all(any(all(observed.get(key) == value for key, value in request.items())
            for observed in actual) for request in alarms), "guard-monitoring-readback-failed")
        result = schedule_request(self.originals[f"{project}-daily-invoice-generation"])
        result.update(Name=service, GroupName="default", State="DISABLED", Description="Standalone creditnote status guard; completion monitored before pinned daily report.",
                      ScheduleExpression="cron(58 23 * * ? *)" if project == "roy" else "cron(28 23 * * ? *)",
                      ScheduleExpressionTimezone="Europe/Bratislava", FlexibleTimeWindow={"Mode": "OFF"},
                      ActionAfterCompletion="NONE")
        for key in ("StartDate", "EndDate", "KmsKeyArn"):
            result.pop(key, None)
        result["Target"]["EcsParameters"]["TaskDefinitionArn"] = arn
        result["Target"]["RoleArn"] = role
        result["Target"]["DeadLetterConfig"] = {"Arn": queue_arn}
        result["Target"]["RetryPolicy"] = {"MaximumEventAgeInSeconds": 900, "MaximumRetryAttempts": 2}
        result["Target"].pop("Input", None)
        self.evidence.setdefault("monitoring", {})[project] = {"queue": queue_arn, "schedule": schedule_arn, "alarms": alarms}
        self.update(service, result)
        self.save("guard-schedule-created-disabled")
        return result

    def promote(self, guard_schedules):
        require({host["service"] for host in self.evidence["hosts"] if host["kind"] == "guard-live"}
                == {family(p) for p in PROJECTS}, "live-guard-completion-required")
        require_main(self.commit)
        self.no_capture()
        self.check_schedules()
        self.save("before-promotion")
        for name, row in guard_schedules.items():
            desired = copy.deepcopy(row)
            desired["State"] = "ENABLED"
            self.update(name, desired)
        for project in PROJECTS:
            name = f"{project}-daily-report-email"
            self.update(name, report_override(self.originals[name]))
        for name in PROTECTED:
            self.update(name, schedule_request(self.originals[name]))
        self.check_schedules()
        require(all(self.expected[name] == schedule_request(self.originals[name]) for name in PROTECTED), "protected-schedule-drift")
        self.save("promotion-readback-verified")

    def apply(self):
        require_main(self.commit)
        self.inspect()
        images = self.session.client("ecr").describe_images(repositoryName="vevo-reporting", imageIds=[{"imageTag": f"git-{self.commit}"}])["imageDetails"]
        require(len(images) == 1 and re.fullmatch(r"sha256:[a-f0-9]{64}", images[0]["imageDigest"]), "guard-exact-image-missing")
        digest = images[0]["imageDigest"]
        image = f"{self.account}.dkr.ecr.eu-central-1.amazonaws.com/vevo-reporting@{digest}"
        self.alarm_route = established_alarm_route(self.session, self.account)
        live_attempted = False
        self.save("preflight-verified")
        # Inventory/source fetches may be slow. Reject a stale release before any
        # schedule write, outside rollback because nothing has been paused yet.
        require_main(self.commit)
        try:
            self.pause()
            source = (ROOT / "scripts/reporting_guard_identity_probe.py").read_text(encoding="utf-8")
            invocation = f"exec(compile({source!r}, '<verified-report-identity-probe>', 'exec'), {{'__name__': '__main__'}})"
            for project in PROJECTS:
                snapshot = self.originals[f"{project}-daily-report-email"]
                revision, old_digest, _ = REPORT_PINS[project]
                command = ["python", "-c", invocation, "--project", project, "--revision", str(revision),
                           "--source-sha256", self.report_sources[project]]
                self.task_run(project, snapshot["Target"]["EcsParameters"]["TaskDefinitionArn"], snapshot,
                              "sha256:" + old_digest, command, kind="report-probe")
            candidates, schedules = {}, {}
            for project in PROJECTS:
                arn, definition = self.provision(project, image)
                candidates[project] = arn
                self.task_run(project, arn, self.originals[f"{project}-daily-invoice-generation"], digest,
                    ["python", "scripts/creditnote_automation_host_gate.py", "--project", project], kind="guard-probe")
                schedules[family(project)] = self.monitor_and_schedule(project, arn, definition)
            for project in PROJECTS:
                self.no_capture()
                # The probes can take hours. Revalidate the exact source and all
                # paused schedules immediately before each possible status write.
                require_main(self.commit)
                self.check_schedules()
                live_attempted = True
                self.task_run(project, candidates[project], self.originals[f"{project}-daily-invoice-generation"], digest,
                    ["python", "creditnote_storno_runner.py", "--project", project], kind="guard-live")
            self.promote(schedules)
        except Exception as exc:
            self.evidence["failure_type"] = type(exc).__name__
            self.restore(live_attempted=live_attempted)
            raise RuntimeError("creditnote-migration-failed-review-private-evidence") from None
        return {"apply": True, "report_pins_preserved": 2, "protected_schedules_preserved": len(PROTECTED),
                "standalone_guards": 2, "live_completions": 2, "reports_enabled": True,
                "future_completion_dependency": "monitored-not-enforced"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    if args.apply:
        require_main(args.commit)
    import boto3
    deployment = CreditnoteDeployment(boto3.Session(profile_name=args.profile, region_name="eu-central-1"), args.commit)
    try:
        result = deployment.apply() if args.apply else deployment.inspect()
    except Exception as exc:
        print(json.dumps({"ok": False, "error_type": type(exc).__name__, "apply": args.apply,
                          "detail": "Review the private deployment evidence; do not retry blindly."}), flush=True)
        return 1
    print(json.dumps({"ok": True, **result}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
