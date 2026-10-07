#!/usr/bin/env python3
"""Current-target VEVO image release, isolated probe, and one report-only rerun.

This deliberately does not publish or claim a legacy managed-runtime binding.
It shares that deployer's exclusion lease, preserves all other schedules, and
records private receipts before each mutation. An uncertain dispatch is never
automatically retried; use ``status`` to inspect its retained receipt first.
"""
from __future__ import annotations

import argparse
import copy
from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import reporting_runtime_binding as binding  # noqa: E402
from scripts.deploy_order_automations import TASK_FIELDS  # noqa: E402
from scripts.deploy_vevo_report import Deployment  # noqa: E402
from scripts.reporting_migration_host_gate import (  # noqa: E402
    ACCOUNT, BUCKET, PREFIX, REGION, SERVICE, canonical, read_private, require, sha, verify_quality,
)
from scripts.vevo_report_image_host import MARKER as LIVE_MARKER  # noqa: E402

CLUSTER = binding.POLICY["cluster"]
RELEASE_PREFIX = "data/vevo/reporting/image-releases/"
EXPECTED_ARTIFACTS = {"report_latest.html", "dashboard_payload_latest.json"} | {
    f"{kind}_{period}.{extension}" for period in ("7d", "30d", "90d")
    for kind, extension in (("report", "html"), ("dashboard_payload", "json"))}


def gh(path):
    return json.loads(subprocess.check_output(["gh", "api", path], cwd=ROOT, timeout=45))


def exact_source(commit):
    require(re.fullmatch(r"[a-f0-9]{40}", commit), "image-release-source-invalid")
    require(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip() == "",
            "image-release-source-dirty")
    subprocess.run(["git", "fetch", "origin", "main"], cwd=ROOT, check=True, capture_output=True, timeout=45)
    for ref in ("HEAD", "origin/main"):
        require(subprocess.check_output(["git", "rev-parse", ref], cwd=ROOT, text=True).strip() == commit,
                "image-release-main-drift")
    require(subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=ROOT, text=True).strip()
            in {"https://github.com/vzeman/biznisweb.git", "https://github.com/vzeman/biznisweb",
                "git@github.com:vzeman/biznisweb.git"}, "image-release-repository-invalid")


def committed_gate_sha(commit):
    """Hash the exact Git blob copied by Linux CI, independent of local EOLs."""
    return sha(subprocess.check_output(["git", "show", commit + ":scripts/reporting_migration_host_gate.py"],
                                      cwd=ROOT, timeout=45))


def image_only_definition(source, image):
    require(source.get("family") == "vevo-reporting-daily" and source.get("networkMode") == "awsvpc",
            "image-release-definition-identity")
    result = {key: copy.deepcopy(source[key]) for key in TASK_FIELDS if key in source}
    containers = result.get("containerDefinitions", [])
    require(len(containers) == 1 and containers[0].get("name") == "reporting", "image-release-container-identity")
    container = containers[0]
    require(container.get("command") in (None, ["python", "daily_report_runner.py"])
            and not container.get("entryPoint") and container.get("workingDirectory", "/app") == "/app",
            "image-release-source-command")
    env = binding.unique_environment(container.get("environment", []))
    require(env.get("REPORT_PROJECT") == "vevo" and env.get("REPORT_S3_BUCKET") == BUCKET
            and env.get("REPORT_S3_PREFIX", "").strip("/") == "daily-reports/vevo",
            "image-release-source-project-or-sink")
    require(env.get("REPORT_SKIP_INVOICES") == "true", "image-release-source-invoice-skip-required")
    require(not {"REPORT_PROJECT", "REPORT_SKIP_INVOICES", "REPORT_SKIP_CREDITNOTE_STORNO_GUARD",
                 "REPORT_SKIP_EMAIL", "REPORT_S3_BUCKET", "REPORT_S3_PREFIX", "REPORT_TO_DATE"}.intersection(
                    row["name"] for row in container.get("secrets", [])), "image-release-control-secret-collision")
    pattern = rf"{ACCOUNT}\.dkr\.ecr\.{REGION}\.amazonaws\.com/vevo-reporting@sha256:[a-f0-9]{{64}}"
    require(re.fullmatch(pattern, image) and re.fullmatch(pattern, container.get("image", "")),
            "image-release-image-not-immutable")
    container["image"] = image
    return result


def task_overrides(release_id, commit, image, gate_sha, to_date, *, probe):
    date.fromisoformat(to_date)
    command = ["python", "scripts/reporting_migration_host_gate.py" if probe else "scripts/vevo_report_image_host.py",
        "--release-id", release_id, "--source-commit", commit, "--image-digest", image.rsplit("@", 1)[1],
        "--gate-sha256", gate_sha]
    if not probe:
        command += ["--to-date", to_date]
    env = {"REPORT_PROJECT": "vevo", "REPORT_SKIP_EMAIL": "true", "REPORT_SKIP_INVOICES": "true",
           "REPORT_SKIP_CREDITNOTE_STORNO_GUARD": "true", "REPORT_TO_DATE": to_date,
           "REPORT_FORCE_NO_CACHE": "true", "REPORT_FORCE_CLEAR_CACHE": "true"}
    result = {"containerOverrides": [{"name": "reporting", "command": command,
               "environment": [{"name": key, "value": value} for key, value in sorted(env.items())]}]}
    if probe:
        result["taskRoleArn"] = f"arn:aws:iam::{ACCOUNT}:role/VevoReportProbe-{release_id}"
    return result


def verify_artifacts(manifest, fetch, *, prefix, to_date, probe):
    entries = manifest.get("artifacts", {})
    expected = EXPECTED_ARTIFACTS | ({"data_quality.json"} if probe else set())
    require(manifest.get("project") == "vevo" and set(entries) == expected, "image-release-manifest-identity")
    for name, entry in entries.items():
        require(isinstance(entry, dict) and entry.get("key") == prefix + name
                and type(entry.get("size")) is int and 0 < entry["size"] <= 64 * 1024 * 1024
                and re.fullmatch(r"[a-f0-9]{64}", entry.get("sha256", "")), "image-release-artifact-identity")
        raw = fetch(entry["key"], entry["size"] + 1)
        require(len(raw) == entry["size"] and sha(raw) == entry["sha256"], "image-release-artifact-hash")
        if name.endswith(".html"):
            require(b"<html" in raw.lower(), "image-release-html-invalid")
        elif name == "data_quality.json":
            verify_quality(json.loads(raw))
        else:
            payload = json.loads(raw)
            require(payload.get("project") == "vevo", "image-release-payload-project")
            verify_quality(payload.get("source_health"))
            # Use the requested payload interval, never the last observed order.
            switcher = payload.get("period_switcher", {})
            current = switcher.get("current_key")
            expected_period = "full" if name == "dashboard_payload_latest.json" else name.removeprefix("dashboard_payload_").removesuffix(".json")
            require(current == expected_period, "image-release-payload-period")
            require(str(payload.get("date_to", ""))[:10] == to_date,
                    "image-release-payload-end-date")


def validate_recovery_receipts(records, release_id, current_schedule, current_definition):
    """Bind an explicitly reviewed stopped release to this exact paused target."""
    by_phase = {}
    for record in records:
        require(record.get("release_id") == release_id and record.get("schema_version") == 1,
                "image-release-recovery-receipt-identity")
        phase = record.get("phase")
        require(isinstance(phase, str) and phase not in by_phase, "image-release-recovery-receipt-duplicate")
        by_phase[phase] = record
    phases = ("preflight-complete", "candidate-registered", "probe-complete", "live-dispatch-requested",
              "live-dispatched", "live-failure-paused-review-required")
    require(all(phase in by_phase for phase in phases), "image-release-recovery-chain-incomplete")
    before, failed = by_phase[phases[0]], by_phase[phases[-1]]
    require(records[0] is before and records[-1] is failed and failed.get("promoted") is True and failed.get("rerun_attempted") is True
            and all(record.get("source_commit") == before.get("source_commit")
                    and record.get("report_to_date") == before.get("report_to_date") for record in records),
            "image-release-recovery-chain-invalid")
    require(re.fullmatch(r"[a-f0-9]{40}", before.get("source_commit", "")), "image-release-recovery-source-invalid")
    date.fromisoformat(before["report_to_date"])
    candidate_arn = by_phase["candidate-registered"]["task_definition"]
    require(re.fullmatch(rf"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/vevo-reporting-daily:[1-9][0-9]*", candidate_arn),
            "image-release-recovery-definition-identity")
    expected = binding.schedule_snapshot(before["schedule"])
    require(expected.get("Name") == SERVICE and expected.get("State") in {"ENABLED", "DISABLED"}
            and before.get("final_schedule_state", expected["State"]) == "ENABLED"
            and (expected["State"] == "ENABLED" or (isinstance(before.get("recovered_from"), dict)
                 and re.fullmatch(r"[a-f0-9]{32}", before["recovered_from"].get("release_id", ""))
                 and re.fullmatch(r"[a-f0-9]{64}", before["recovered_from"].get("receipt_sha256", ""))))
            and expected["Target"]["Arn"] == CLUSTER, "image-release-recovery-original-schedule")
    expected["State"] = "DISABLED"
    expected["Target"]["EcsParameters"]["TaskDefinitionArn"] = candidate_arn
    require(binding.schedule_snapshot(failed["known_schedule"]) == expected
            and binding.schedule_snapshot(current_schedule) == expected, "image-release-recovery-schedule-drift")
    candidate = image_only_definition(before["source_definition"], before["image"])
    candidate["taskDefinitionArn"] = candidate_arn
    require(current_definition.get("status") == "ACTIVE"
            and binding.definition_snapshot(current_definition) == binding.definition_snapshot(candidate),
            "image-release-recovery-definition-drift")
    tasks = failed.get("known_task_arns", {})
    probe = by_phase["probe-complete"]
    require(set(tasks) == {"probe", "live"} and len(set(tasks.values())) == 2
            and tasks["probe"] == probe["terminal_task"]["taskArn"]
            and tasks["live"] == by_phase["live-dispatched"]["task_arn"]
            and by_phase["live-dispatch-requested"]["task_definition"] == candidate_arn,
            "image-release-recovery-task-chain")
    identity, complete = probe["identity"], probe["complete"]
    require(identity.get("marker") == "VEVO_REPORT_PROBE_HOST_OK"
            and identity.get("task_arn") == tasks["probe"] and identity.get("task_definition") == candidate_arn
            and identity.get("release_id") == release_id and identity.get("source_commit") == before["source_commit"]
            and identity.get("image_digest") == before["image"].rsplit("@", 1)[1]
            and identity.get("instance_id") == "N/A:Fargate" and identity.get("service") == SERVICE
            and identity.get("path") == "/app" and re.fullmatch(r"[a-f0-9]{64}", identity.get("gate_sha256", ""))
            and complete.get("phase") == "report-verified"
            and all(complete.get(key) == value for key, value in identity.items())
            and complete.get("localhost_marker_sha256") == sha(canonical(identity))
            and all(complete.get(key) is False for key in ("provider_writes", "email_sent", "live_outputs_changed"))
            and all(complete.get(key) is True for key in ("skip_invoices", "skip_inline_guard")),
            "image-release-recovery-probe-proof")
    expected_live = task_overrides(release_id, before["source_commit"], before["image"],
                                  identity["gate_sha256"], before["report_to_date"], probe=False)
    require(by_phase["live-dispatch-requested"]["overrides"] == expected_live,
            "image-release-recovery-live-command")
    require(failed["live_outputs"] == before["original_live_outputs"], "image-release-recovery-already-published")
    return {"before": before, "failed": failed, "probe": probe, "tasks": tasks,
            "candidate_arn": candidate_arn, "identity": identity}


def validate_stopped_recovery_tasks(recovery, tasks, *, archived_arns=frozenset()):
    """Fresh ECS identities; a stopped live task is never a publication proof."""
    before, identity = recovery["before"], recovery["identity"]
    by_arn = {task["taskArn"]: task for task in tasks}
    require(set(archived_arns).issubset(by_arn), "image-release-recovery-archive-scope")
    require(len(tasks) == 2 and set(by_arn) == set(recovery["tasks"].values()),
            "image-release-recovery-tasks-missing")
    for mode, arn in recovery["tasks"].items():
        task = by_arn[arn]
        containers = task.get("containers", [])
        require((arn in archived_arns or (task.get("clusterArn") == CLUSTER and task.get("launchType") == "FARGATE"))
                and task.get("taskDefinitionArn") == recovery["candidate_arn"]
                and task.get("startedBy") == before["release_id"]
                and task.get("lastStatus") == task.get("desiredStatus") == "STOPPED" and task.get("stoppedAt")
                and len(containers) == 1 and containers[0].get("name") == "reporting"
                and containers[0].get("lastStatus") == "STOPPED" and type(containers[0].get("exitCode")) is int
                and containers[0].get("imageDigest") == identity["image_digest"]
                and containers[0].get("image") == before["image"]
                and binding.stamp(binding.normalized(task["stoppedAt"])) >= binding.stamp(binding.normalized(task["startedAt"])),
                "image-release-recovery-task-not-owned-stopped")
        ips = [row.get("privateIpv4Address") for row in containers[0].get("networkInterfaces", [])]
        require(len(ips) == 1 and re.fullmatch(r"(?:[0-9]{1,3}\.){3}[0-9]{1,3}", ips[0] or ""),
                "image-release-recovery-task-ip")
        if mode == "probe":
            require(containers[0]["exitCode"] == 0 and ips == [identity["private_ip"]],
                    "image-release-recovery-probe-terminal-drift")
        expected = task_overrides(before["release_id"], before["source_commit"], before["image"],
                                  identity["gate_sha256"], before["report_to_date"], probe=mode == "probe")
        actual = copy.deepcopy(task.get("overrides", {}))
        for row in actual.get("containerOverrides", []):
            env = binding.unique_environment(row.get("environment", []))
            row["environment"] = [{"name": key, "value": value} for key, value in sorted(env.items())]
        require(actual.get("containerOverrides") == expected["containerOverrides"]
                and (actual.get("taskRoleArn") == expected["taskRoleArn"] if mode == "probe"
                     else actual.get("taskRoleArn") in (None, before["source_definition"]["taskRoleArn"])),
                "image-release-recovery-task-command-drift")
    return binding.normalized(tasks)


def transfer_recovery_lease(lease, previous_owner, expected_etag):
    """Conditional ownership transfer: no released interval or blind deletion."""
    value, etag = binding.read_object(lease.s3, binding.LOCK_KEY)
    binding.validate_lock(value)
    require(value["owner"] == previous_owner and value["state"] == "uncertain"
            and etag == expected_etag and lease.owner != previous_owner,
            "image-release-recovery-lease-drift")
    lease._write("active", etag)


class ImageRelease(Deployment):
    """Reuse only independently safe IAM/task helpers; no old binding/pin path."""

    def __init__(self, session, commit, to_date, timeout, *, release_id=None, recover_paused_release=None,
                 recovery_receipt_sha256=None, recovery_readback_key=None, recovery_readback_sha256=None):
        super().__init__(session, commit, "", "")
        if release_id is not None:
            self.release_id = release_id
            self.prefix = PREFIX + release_id + "/"
            self.lease = binding.MigrationLease(self.s3, owner=release_id)
        self.to_date, self.timeout = to_date, timeout
        self.receipt_prefix = RELEASE_PREFIX + self.release_id + "/"
        self.sequence = 0
        self.promoted = self.rerun_attempted = self.lease_owned = False
        self.last_checkpoint = 0.0
        self.task_mode = None
        self.task_records = {}
        self.candidate = None
        self.role_trust = None
        self.recover_paused_release = recover_paused_release
        self.recovery_receipt_sha256 = recovery_receipt_sha256
        self.recovery_readback_key, self.recovery_readback_sha256 = recovery_readback_key, recovery_readback_sha256
        self.recovery = None

    def event(self, phase, **details):
        self.sequence += 1
        value = {"schema_version": 1, "release_id": self.release_id, "phase": phase,
                 "source_commit": self.commit, "report_to_date": self.to_date,
                 "at": datetime.now(timezone.utc).isoformat(), "legacy_current_binding_changed": False,
                 "promoted": self.promoted, "rerun_attempted": self.rerun_attempted,
                 "known_task_arns": self.task_records, **details}
        raw = canonical(binding.normalized(value))
        key = self.receipt_prefix + f"{self.sequence:04d}-{phase}.json"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ServerSideEncryption="AES256",
                           ExpectedBucketOwner=ACCOUNT, IfNoneMatch="*")
        require(read_private(self.s3, key, max(len(raw) + 1, 256 * 1024)) == raw, "image-release-receipt-readback")
        print(json.dumps({"release_id": self.release_id, "phase": phase, "receipt_key": key}), flush=True)

    def all_schedules(self):
        result = {}
        for page in self.scheduler.get_paginator("list_schedules").paginate(GroupName="default"):
            for row in page.get("Schedules", []):
                name = row["Name"]
                require(name not in result, "image-release-duplicate-schedule")
                result[name] = binding.schedule_snapshot(self.scheduler.get_schedule(Name=name, GroupName="default"))
        require(SERVICE in result, "image-release-report-schedule-missing")
        return result

    def checkpoint(self, *, poll=False):
        if poll and self.clock() - self.last_checkpoint < 45:
            return
        self.lease.renew()
        current = self.all_schedules()
        require({key: value for key, value in current.items() if key != SERVICE} == self.protected,
                "image-release-other-schedule-drift")
        require(current[SERVICE] == binding.schedule_snapshot(self.known_schedule), "image-release-current-schedule-drift")
        self.last_checkpoint = self.clock()
        print(json.dumps({"release_id": self.release_id, "phase": "runtime-checkpoint",
                          "task_mode": self.task_mode, "owned_task": self.owned_task}), flush=True)

    def no_report_tasks(self):
        require(not [task for task in self.tasks() if task["lastStatus"] != "STOPPED"], "image-release-report-already-running")

    def update_owned_schedule(self, desired, phase):
        self.checkpoint()
        self.event(phase + "-requested", schedule=desired)
        self.update(desired)
        self.event(phase + "-verified", schedule=self.known_schedule)

    def task(self):
        task = super().task()
        expected = task_overrides(self.release_id, self.commit, self.image, self.gate_sha, self.to_date,
                                  probe=self.task_mode == "probe")
        actual = task.get("overrides", {})
        actual_containers = copy.deepcopy(actual.get("containerOverrides"))
        require(isinstance(actual_containers, list) and len(actual_containers) == 1,
                "image-release-task-override-drift")
        for container in actual_containers:
            if "environment" in container:
                env = binding.unique_environment(container["environment"])
                container["environment"] = [{"name": key, "value": value} for key, value in sorted(env.items())]
        require(actual_containers == expected["containerOverrides"], "image-release-task-override-drift")
        if self.task_mode == "probe":
            require(actual.get("taskRoleArn") == expected["taskRoleArn"], "image-release-probe-role-drift")
        else:
            require(actual.get("taskRoleArn") in (None, self.candidate["taskRoleArn"]), "image-release-live-role-drift")
        return task

    def start_task(self, mode):
        self.checkpoint()
        self.no_report_tasks()
        self.task_mode = mode
        self.probe_definition = self.candidate
        overrides = task_overrides(self.release_id, self.commit, self.image, self.gate_sha, self.to_date, probe=mode == "probe")
        network = self.original["Target"]["EcsParameters"]["NetworkConfiguration"]["awsvpcConfiguration"]
        network = {key[:1].lower() + key[1:]: value for key, value in network.items()}
        self.event(mode + "-dispatch-requested", task_definition=self.candidate["taskDefinitionArn"], overrides=overrides,
                   client_token=self.release_id + "-" + mode)
        self.start_uncertain = True
        if mode == "live":
            self.rerun_attempted = True
        result = self.ecs.run_task(cluster=CLUSTER, taskDefinition=self.candidate["taskDefinitionArn"],
            launchType="FARGATE", count=1, startedBy=self.release_id, clientToken=self.release_id + "-" + mode,
            overrides=overrides, networkConfiguration={"awsvpcConfiguration": network})
        require(not result.get("failures") and len(result.get("tasks", [])) == 1, "image-release-dispatch-unconfirmed")
        self.owned_task = result["tasks"][0]["taskArn"]
        self.task_records[mode] = self.owned_task
        self.start_uncertain = False
        self.event(mode + "-dispatched", task_arn=self.owned_task)

    def wait_probe(self):
        deadline, ready, task = self.clock() + self.timeout, None, None
        while self.clock() < deadline:
            self.checkpoint(poll=True)
            task = self.task()
            if ready is None:
                try:
                    raw = read_private(self.s3, self.prefix + "markers/ready.json")
                except Exception as exc:
                    require(binding.error_code(exc) in {"NoSuchKey", "404"}, "image-release-ready-read-failed")
                else:
                    ready = json.loads(raw)
                    require(ready.get("marker") == "VEVO_REPORT_PROBE_HOST_OK", "image-release-ready-marker")
                    self.verify_host(task, ready)
                    self.event("probe-host-verified-before-provider", identity=ready)
                    signal = canonical({"phase": "host-authorized", "release_id": self.release_id,
                                        "task_arn": self.owned_task, "ready_sha256": sha(raw)})
                    self.s3.put_object(Bucket=BUCKET, Key=self.prefix + "authorize.json", Body=signal,
                                       ExpectedBucketOwner=ACCOUNT, ServerSideEncryption="AES256", IfNoneMatch="*")
                    require(read_private(self.s3, self.prefix + "authorize.json") == signal, "image-release-authorize-readback")
            if task["lastStatus"] == "STOPPED":
                break
            self.sleep(10)
        require(ready is not None and task is not None and task["lastStatus"] == "STOPPED"
                and task["containers"][0].get("exitCode") == 0, "image-release-probe-not-successful")
        complete = json.loads(read_private(self.s3, self.prefix + "markers/complete.json"))
        require(all(complete.get(key) == value for key, value in ready.items())
                and complete.get("phase") == "report-verified" and complete.get("localhost_marker_sha256") == sha(canonical(ready))
                and all(complete.get(key) is False for key in ("provider_writes", "email_sent", "live_outputs_changed"))
                and all(complete.get(key) is True for key in ("skip_invoices", "skip_inline_guard")),
                "image-release-probe-completion-invalid")
        key = self.prefix + "artifacts/output-manifest.json"
        raw = read_private(self.s3, key)
        require(complete.get("output_manifest_key") == key and sha(raw) == complete.get("output_manifest_sha256"),
                "image-release-probe-manifest-hash")
        verify_artifacts(json.loads(raw), self.fetch, prefix=self.prefix + "artifacts/", to_date=self.to_date, probe=True)
        require(self.live_outputs() == self.original_outputs, "image-release-probe-changed-live-output")
        self.event("probe-complete", identity=ready, complete=complete, terminal_task=task)

    def verify_host(self, task, marker):
        container = task["containers"][0]
        ips = [row["privateIpv4Address"] for row in container.get("networkInterfaces", [])]
        require(marker.get("task_arn") == self.owned_task and marker.get("task_definition") == self.candidate["taskDefinitionArn"]
                and ips == [marker.get("private_ip")] and marker.get("image_digest") == container.get("imageDigest") == self.image.rsplit("@", 1)[1]
                and marker.get("path") == "/app" and marker.get("service") == SERVICE and marker.get("instance_id") == "N/A:Fargate"
                and marker.get("release_id") == self.release_id and marker.get("source_commit") == self.commit
                and marker.get("gate_sha256") == self.gate_sha, "image-release-host-identity-invalid")

    def fetch(self, key, limit):
        return read_private(self.s3, key, limit)

    def live_marker(self):
        options = self.candidate["containerDefinitions"][0]["logConfiguration"]["options"]
        request = {"logGroupName": options["awslogs-group"], "startFromHead": True,
                   "logStreamName": options["awslogs-stream-prefix"] + "/reporting/" + self.owned_task.rsplit("/", 1)[1]}
        markers, token = [], None
        for _ in range(100):
            page = self.session.client("logs").get_log_events(**request, **({"nextToken": token} if token else {}))
            for row in page.get("events", []):
                message = row.get("message", "")
                if message.startswith(LIVE_MARKER + " "):
                    markers.append(json.loads(message.split(" ", 1)[1]))
            next_token = page.get("nextForwardToken")
            if next_token == token:
                break
            require(next_token is not None, "image-release-live-log-incomplete")
            token = next_token
        else:
            raise RuntimeError("image-release-live-log-cap")
        require(len(markers) == 1, "image-release-live-marker-not-unique")
        return markers[0]

    def wait_live(self):
        deadline, task = self.clock() + self.timeout, None
        while self.clock() < deadline:
            self.checkpoint(poll=True)
            task = self.task()
            if task["lastStatus"] == "STOPPED":
                break
            self.sleep(10)
        require(task is not None and task["lastStatus"] == "STOPPED" and task["containers"][0].get("exitCode") == 0,
                "image-release-live-not-successful")
        marker = self.live_marker()
        self.verify_host(task, marker)
        require(marker.get("report_to_date") == self.to_date and all(marker.get(key) is True
                for key in ("skip_email", "skip_invoices", "skip_inline_guard")), "image-release-live-safety-marker")
        manifest_key = "daily-reports/vevo/latest/generation.json"
        raw = read_private(self.s3, manifest_key)
        manifest = json.loads(raw)
        generation = manifest.get("generation_id", "")
        require(re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", generation) and generation != self.original_generation,
                "image-release-live-generation-not-new")
        verify_artifacts(manifest, self.fetch, prefix="daily-reports/vevo/" + generation + "/", to_date=self.to_date, probe=False)
        require(read_private(self.s3, manifest_key) == raw, "image-release-live-pointer-changed")
        self.event("live-complete", identity=marker, terminal_task=task, generation_manifest=manifest,
                   generation_manifest_sha256=sha(raw), email_sent=False, financial_runners_skipped=True)

    def inspect_paused_recovery(self):
        """Read historical identity proof, then verify every current safety boundary."""
        previous_id = self.recover_paused_release
        require(re.fullmatch(r"[a-f0-9]{32}", previous_id or "") and previous_id != self.release_id
                and re.fullmatch(r"[a-f0-9]{64}", self.recovery_receipt_sha256 or ""),
                "image-release-recovery-arguments")
        prefix = RELEASE_PREFIX + previous_id + "/"
        page = self.s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=65, ExpectedBucketOwner=ACCOUNT)
        keys = sorted(row["Key"] for row in page.get("Contents", []))
        require(not page.get("IsTruncated") and 1 <= len(keys) <= 64
                and all(re.fullmatch(re.escape(prefix) + r"[0-9]{4}-[a-z-]+\.json", key) for key in keys),
                "image-release-recovery-receipts-incomplete")
        raw_records = [read_private(self.s3, key, 1024 * 1024) for key in keys]
        require(sha(raw_records[-1]) == self.recovery_receipt_sha256, "image-release-recovery-receipt-hash")
        records = [json.loads(raw) for raw in raw_records]
        require(all(key == prefix + f"{index:04d}-{record.get('phase')}.json"
                    for index, (key, record) in enumerate(zip(keys, records), 1)),
                "image-release-recovery-receipt-sequence")
        recovery = validate_recovery_receipts(records, previous_id, self.original, self.original_source)
        recovery.update(receipt_key=keys[-1], receipt_sha256=sha(raw_records[-1]),
                        preflight_sha256=sha(raw_records[0]), previous_owner=previous_id)
        require(recovery["identity"]["gate_sha256"] == committed_gate_sha(recovery["before"]["source_commit"]),
                "image-release-recovery-gate-source")
        archived = None
        require(bool(self.recovery_readback_key) == bool(self.recovery_readback_sha256),
                "image-release-recovery-readback-arguments")
        if self.recovery_readback_key:
            key = self.recovery_readback_key
            require(key.startswith("data/vevo/") and ".." not in key and key.endswith(".json")
                    and re.fullmatch(r"[a-f0-9]{64}", self.recovery_readback_sha256 or ""),
                    "image-release-recovery-readback-scope")
            raw = read_private(self.s3, key, 256 * 1024)
            require(sha(raw) == self.recovery_readback_sha256, "image-release-recovery-readback-hash")
            archived = json.loads(raw)
            require(archived.get("release_id") == previous_id and archived.get("source_commit") == recovery["before"]["source_commit"]
                    and archived.get("read_only") is True and archived.get("preflight_sha256") == recovery["preflight_sha256"]
                    and archived.get("failed_receipt_sha256") == recovery["receipt_sha256"]
                    and archived.get("schedule") == binding.schedule_snapshot(self.original)
                    and archived.get("live_outputs") == recovery["before"]["original_live_outputs"]
                    and archived.get("active_reporting_task_arns") == [] and archived.get("probe_role_absent") is True
                    and archived.get("candidate_image") == recovery["before"]["image"]
                    and archived.get("candidate_status") == "ACTIVE"
                    and archived.get("lease", {}).get("state") == "uncertain"
                    and archived["lease"].get("owner") == previous_id
                    and binding.stamp(archived["at"]) >= binding.stamp(recovery["failed"]["at"]),
                    "image-release-recovery-readback-identity")
            require(set(archived.get("owned_tasks", {})) == {"probe", "live"}
                    and all(task.get("taskArn") == recovery["tasks"][mode]
                            and binding.stamp(task["stoppedAt"]) <= binding.stamp(archived["at"])
                            for mode, task in archived["owned_tasks"].items()),
                    "image-release-recovery-readback-tasks")
            validate_stopped_recovery_tasks(recovery, list(archived["owned_tasks"].values()),
                                            archived_arns=frozenset(recovery["tasks"].values()))
        recovery["archived_readback"] = archived
        self.recovery = recovery
        self.verify_paused_boundary()
        return {"release_id": previous_id, "receipt_key": recovery["receipt_key"],
                "receipt_sha256": recovery["receipt_sha256"], "preflight_sha256": recovery["preflight_sha256"],
                "readback_key": self.recovery_readback_key, "readback_sha256": self.recovery_readback_sha256,
                "lease_etag": recovery["lease_etag"], "terminal_tasks": recovery["terminal_tasks"],
                "archived_task_arns": recovery["archived_task_arns"],
                "protected_changes_since_stopped_release": {
                    name: {"before": recovery["before"]["protected_schedules"].get(name), "now": self.protected.get(name)}
                    for name in sorted(set(self.protected) | set(recovery["before"]["protected_schedules"]))
                    if self.protected.get(name) != recovery["before"]["protected_schedules"].get(name)}}

    def verify_paused_boundary(self):
        recovery = self.recovery
        require(binding.schedule_snapshot(self.schedule()) == binding.schedule_snapshot(self.original),
                "image-release-recovery-schedule-drift")
        require(binding.definition_snapshot(self.definition(recovery["candidate_arn"])) ==
                binding.definition_snapshot(self.original_source), "image-release-recovery-definition-drift")
        require(self.live_outputs() == recovery["before"]["original_live_outputs"],
                "image-release-recovery-live-output-drift")
        self.no_report_tasks()
        observed = self.ecs.describe_tasks(cluster=CLUSTER, tasks=list(recovery["tasks"].values()))
        missing = {row.get("arn") for row in observed.get("failures", []) if row.get("reason") == "MISSING"}
        require(len(missing) == len(observed.get("failures", [])) and missing.issubset(recovery["tasks"].values()),
                "image-release-recovery-task-read-failed")
        tasks = observed.get("tasks", [])
        archive = recovery["archived_readback"]
        require(not missing or archive is not None, "image-release-recovery-tasks-expired-require-readback")
        if archive:
            archived_by_arn = {task["taskArn"]: task for task in archive["owned_tasks"].values()}
            for task in tasks:
                previous = archived_by_arn.get(task["taskArn"], {})
                require(all(binding.normalized(task.get(key)) == previous.get(key) for key in
                            ("taskDefinitionArn", "startedBy", "stoppedAt", "lastStatus", "desiredStatus"))
                        and [{key: row.get(key) for key in previous["containers"][0]} for row in task.get("containers", [])]
                            == previous.get("containers"), "image-release-recovery-archived-task-drift")
            tasks += [archived_by_arn[arn] for arn in sorted(missing)]
        recovery["terminal_tasks"] = validate_stopped_recovery_tasks(recovery, tasks, archived_arns=missing)
        recovery["archived_task_arns"] = sorted(missing)
        try:
            self.iam.get_role(RoleName="VevoReportProbe-" + recovery["previous_owner"])
        except Exception as exc:
            require(binding.error_code(exc) == "NoSuchEntity", "image-release-recovery-role-read-failed")
        else:
            raise RuntimeError("image-release-recovery-probe-role-present")
        value, etag = binding.read_object(self.s3, binding.LOCK_KEY)
        binding.validate_lock(value)
        require(value["state"] == "uncertain" and value["owner"] == recovery["previous_owner"]
                and etag == recovery.get("lease_etag", etag), "image-release-recovery-lease-drift")
        recovery["lease_etag"] = etag

    def acquire_release_lease(self):
        if self.recovery is None:
            self.lease.acquire()
        else:
            self.verify_paused_boundary()
            require({key: value for key, value in self.all_schedules().items() if key != SERVICE} == self.protected,
                    "image-release-other-schedule-drift")
            self.event("paused-recovery-handoff-requested", previous_owner=self.recovery["previous_owner"],
                       previous_lease_etag=self.recovery["lease_etag"], recovery_receipt_sha256=self.recovery_receipt_sha256)
            try:
                transfer_recovery_lease(self.lease, self.recovery["previous_owner"], self.recovery["lease_etag"])
            except BaseException:
                self.inspect_failed_lease_handoff()
                raise
        self.lease_owned = True

    def inspect_failed_lease_handoff(self):
        """One recovery read; never replay an uncertain ownership transfer."""
        outcome = "ownership-unconfirmed"
        try:
            value, etag = binding.read_object(self.s3, binding.LOCK_KEY)
            binding.validate_lock(value)
            owned = (value["owner"] == self.lease.owner and value["state"] == "active"
                     and (value == self.lease.pending_value or (self.lease.etag is not None and etag == self.lease.etag)))
            if owned:
                self.lease.etag, self.lease.pending_value = etag, None
                self.event("paused-recovery-handoff-uncertain-requested", confirmed_owner=self.lease.owner,
                           confirmed_lease_etag=etag)
                self.lease._write("uncertain", etag)
                outcome = "owned-uncertain"
            else:
                outcome = "not-owned-no-write"
        except BaseException:
            # An unavailable readback proves neither ownership nor permission to
            # overwrite. Any committed active/uncertain lease still excludes work.
            pass
        try:
            self.event("paused-recovery-handoff-failed-inspection-required", lease_outcome=outcome)
        except BaseException:
            print(json.dumps({"release_id": self.release_id, "phase": "paused-recovery-handoff-failed-inspection-required",
                              "lease_outcome": outcome, "receipt_write_failed": True}), flush=True)

    def restore_paused_recovery_lease(self):
        binding.check_migration(self.s3, lease=self.lease)
        previous = binding.MigrationLease(self.s3, owner=self.recovery["previous_owner"])
        self.event("paused-recovery-lease-return-requested", previous_owner=previous.owner)
        previous._write("uncertain", self.lease.etag)
        self.lease_owned = False

    def run(self):
        exact_source(self.commit)
        require(self.session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "image-release-account-invalid")
        binding.require_private_bucket(self.s3)
        self.image, self.build_id = self.exact_image()
        self.original = self.schedule()
        self.known_schedule = copy.deepcopy(self.original)
        require(self.original["State"] == ("DISABLED" if self.recover_paused_release else "ENABLED")
                and self.original["Target"]["Arn"] == CLUSTER
                and self.original["Target"]["EcsParameters"].get("LaunchType") == "FARGATE", "image-release-schedule-identity")
        self.original_source = self.definition(self.original["Target"]["EcsParameters"]["TaskDefinitionArn"])
        candidate = image_only_definition(self.original_source, self.image)
        require(candidate["containerDefinitions"][0]["image"] != self.original_source["containerDefinitions"][0]["image"],
                "image-release-already-current")
        # Ensure the scheduled path remains report-only, including its existing overrides.
        schedule_input = json.loads(self.original["Target"].get("Input", "{}"))
        overrides = schedule_input.get("containerOverrides", [])
        require(all(row.get("name") == "reporting" and not row.get("command") for row in overrides),
                "image-release-scheduled-command-override")
        effective = binding.unique_environment(self.original_source["containerDefinitions"][0].get("environment", []))
        for row in overrides:
            effective.update(binding.unique_environment(row.get("environment", [])))
        require(effective.get("REPORT_SKIP_INVOICES") == "true"
                and effective.get("REPORT_SKIP_CREDITNOTE_STORNO_GUARD") == "true", "image-release-scheduled-financial-boundary")
        self.protected = {key: value for key, value in self.all_schedules().items() if key != SERVICE}
        self.original_outputs = self.live_outputs()
        self.original_generation = json.loads(read_private(self.s3, "daily-reports/vevo/latest/generation.json"))["generation_id"]
        self.gate_sha = committed_gate_sha(self.commit)
        self.no_report_tasks()
        recovery_proof = self.inspect_paused_recovery() if self.recover_paused_release else None
        self.event("preflight-complete", schedule=self.original, source_definition=self.original_source,
                   protected_schedules=self.protected, build_run_id=self.build_id, image=self.image,
                   original_live_outputs=self.original_outputs, final_schedule_state="ENABLED", recovered_from=recovery_proof)
        self.acquire_release_lease()
        try:
            paused = copy.deepcopy(self.original)
            paused["State"] = "DISABLED"
            self.update_owned_schedule(paused, "report-pause")
            # A task already accepted by Scheduler can become visible after its pause.
            for _ in range(12):
                self.no_report_tasks()
                self.checkpoint(poll=True)
                self.sleep(10)
            self.event("candidate-register-requested", definition=candidate)
            self.candidate = self.ecs.register_task_definition(**candidate)["taskDefinition"]
            registered = {key: self.candidate[key] for key in TASK_FIELDS if key in self.candidate}
            require(binding.definition_snapshot(registered) == binding.definition_snapshot(candidate), "image-release-registration-drift")
            self.production_definition = self.candidate
            self.probe_definition = self.candidate
            self.event("candidate-registered", task_definition=self.candidate["taskDefinitionArn"])
            self.event("probe-role-create-requested", role_arn=f"arn:aws:iam::{ACCOUNT}:role/VevoReportProbe-{self.release_id}")
            self.create_role()
            self.start_task("probe")
            self.wait_probe()
            self.cleanup_role()
            self.owned_task = None
            exact_source(self.commit)
            require(self.exact_image() == (self.image, self.build_id), "image-release-final-source-drift")
            self.checkpoint()
            promoted = copy.deepcopy(self.original)
            promoted["Target"]["EcsParameters"]["TaskDefinitionArn"] = self.candidate["taskDefinitionArn"]
            # Keep disabled until the one-off completes; final enable preserves the original schedule.
            promoted["State"] = "DISABLED"
            self.update_owned_schedule(promoted, "image-promotion")
            self.promoted = True
            self.start_task("live")
            self.wait_live()
            final = copy.deepcopy(promoted)
            final["State"] = "ENABLED"
            self.update_owned_schedule(final, "report-enable")
            self.checkpoint()
            self.lease.release()
            self.lease_owned = False
            self.event("release-complete", final_schedule=self.known_schedule,
                       probe_role_removed=True, all_owned_tasks_stopped=True)
        except BaseException:
            self.recover_failure()
            raise

    def recover_failure(self):
        """Never replay publication or change another actor's schedule/role/task."""
        if not self.lease_owned:
            return
        try:
            self.event("release-failed-inspection-required", start_uncertain=self.start_uncertain)
            if self.start_uncertain:
                self.lease.retain_uncertain()
                return
            self.cleanup_task()
            self.cleanup_role()
            # A failed live run may have published before failure. Preserve the
            # proven image and stop automatic reruns until its output is reviewed.
            if self.rerun_attempted:
                paused = copy.deepcopy(self.known_schedule)
                paused["State"] = "DISABLED"
                self.update(paused)
                self.event("live-failure-paused-review-required", known_schedule=self.known_schedule,
                           live_outputs=self.live_outputs())
                self.lease.retain_uncertain()
                return
            current = binding.schedule_snapshot(self.schedule())
            require(current == binding.schedule_snapshot(self.known_schedule), "image-release-failure-foreign-schedule")
            self.update(self.original)
            if self.candidate is not None:
                require(self.schedule()["Target"]["EcsParameters"]["TaskDefinitionArn"] != self.candidate["taskDefinitionArn"],
                        "image-release-candidate-still-scheduled")
                self.ecs.deregister_task_definition(taskDefinition=self.candidate["taskDefinitionArn"])
                require(self.definition(self.candidate["taskDefinitionArn"])["status"] == "INACTIVE", "image-release-candidate-cleanup")
            if getattr(self, "recovery", None) is not None:
                self.restore_paused_recovery_lease()
            else:
                self.lease.release()
                self.lease_owned = False
            self.event("failed-before-live-restored", schedule=self.original, all_owned_tasks_stopped=True,
                       paused_recovery_owner=(self.recovery["previous_owner"] if getattr(self, "recovery", None) else None))
        except BaseException:
            if self.lease_owned:
                self.lease.retain_uncertain()
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "status"))
    parser.add_argument("--profile")
    parser.add_argument("--commit")
    parser.add_argument("--to-date")
    parser.add_argument("--release-id")
    parser.add_argument("--recover-paused-release")
    parser.add_argument("--recovery-receipt-sha256")
    parser.add_argument("--recovery-readback-key")
    parser.add_argument("--recovery-readback-sha256")
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    args = parser.parse_args()
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    # Finite SDK operations; workflows may poll for a long report, never one unbounded call.
    original_client = session.client
    session.client = lambda name: original_client(name, config=Config(connect_timeout=10, read_timeout=45,
        retries={"total_max_attempts": 2}))
    if args.mode == "status":
        require(args.release_id and re.fullmatch(r"[a-f0-9]{32}", args.release_id), "image-release-id-required")
        s3 = session.client("s3")
        prefix = RELEASE_PREFIX + args.release_id + "/"
        keys = [row["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix,
                ExpectedBucketOwner=ACCOUNT) for row in page.get("Contents", [])]
        require(bool(keys), "image-release-receipts-missing")
        key = sorted(keys)[-1]
        value = json.loads(read_private(s3, key, 1024 * 1024))
        print(json.dumps({name: value[name] for name in ("release_id", "phase", "source_commit", "report_to_date",
                                                       "at", "promoted", "rerun_attempted", "known_task_arns")}))
        return
    require(args.commit and args.to_date and 300 <= args.timeout_seconds <= 14400, "image-release-arguments-invalid")
    require(bool(args.recover_paused_release) == bool(args.recovery_receipt_sha256)
            and (args.recover_paused_release or not (args.recovery_readback_key or args.recovery_readback_sha256)),
            "image-release-recovery-arguments")
    date.fromisoformat(args.to_date)
    release_id = args.release_id or uuid.uuid4().hex
    require(re.fullmatch(r"[a-f0-9]{32}", release_id), "image-release-id-invalid")
    try:
        ImageRelease(session, args.commit, args.to_date, args.timeout_seconds, release_id=release_id,
                     recover_paused_release=args.recover_paused_release, recovery_receipt_sha256=args.recovery_receipt_sha256,
                     recovery_readback_key=args.recovery_readback_key, recovery_readback_sha256=args.recovery_readback_sha256).run()
    except Exception as exc:
        reason = str(exc)
        if not re.fullmatch(r"(?:image-release|report|runtime)-[a-z0-9-]+", reason):
            reason = type(exc).__name__
        print(json.dumps({"release_id": release_id, "phase": "failed", "reason": reason}), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
