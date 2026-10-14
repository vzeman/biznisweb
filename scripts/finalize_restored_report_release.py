#!/usr/bin/env python3
"""Close a proven restored, never-promoted release. No runtime repair is performed.

``inspect`` is GET-only. ``finalize`` archives its fresh inspection and intent,
then makes one conditional uncertain -> released lease write. Unknown outcomes
are deliberately not retried; the immutable intent is the recovery evidence.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import reporting_runtime_binding as binding  # noqa: E402
from scripts.reporting_image_release_lease import ScopedImageLease, read_lease  # noqa: E402
from scripts.reporting_image_release_policy import ACCOUNT, BUCKET, REGION, PROJECTS, project_policy  # noqa: E402
from scripts.reporting_migration_host_gate import canonical, require, sha  # noqa: E402
from scripts.deploy_picking_batch_dashboard import SERVICES  # noqa: E402
from scripts.vevo_report_image_release import (  # noqa: E402
    CLUSTER, ImageRelease, committed_gate_sha, gh, image_only_definition, task_overrides,
)

PHASES = (
    "preflight-complete", "lease-acquire-requested", "report-pause-requested",
    "report-pause-verified", "candidate-register-requested", "candidate-registered",
    "probe-role-create-requested", "probe-dispatch-requested", "probe-dispatched",
    "probe-host-verified-before-provider", "probe-complete", "independent-review-requested",
    "release-failed-inspection-required",
)
LIMIT = 2 * 1024 * 1024
CONTROL_FILE = "scripts/finalize_restored_report_release.py"
APP_FIELDS = ("ServiceArn", "ServiceName", "ServiceId", "ServiceUrl", "SourceConfiguration",
              "InstanceConfiguration", "HealthCheckConfiguration", "NetworkConfiguration",
              "AutoScalingConfigurationSummary", "EncryptionConfiguration", "ObservabilityConfiguration")


def utcnow():
    return datetime.now(timezone.utc)


def digest(value):
    return sha(binding.canonical_bytes(binding.normalized(value)))


def local_path(raw, *, must_exist=False):
    """Check the lexical path and every ancestor before resolving any alias."""
    path = Path(raw)
    require(".." not in path.parts, "finalize-local-path-traversal")
    if not path.is_absolute():
        path = ROOT / path
    require(path.is_relative_to(ROOT / "data"), "finalize-local-path-scope")
    for part in (*reversed(path.parents), path):
        try:
            info = part.lstat()
        except FileNotFoundError:
            require(part != path or not must_exist, "finalize-local-path-missing")
            continue
        require(not stat.S_ISLNK(info.st_mode) and not (getattr(info, "st_file_attributes", 0) & 0x400),
                "finalize-local-path-alias")  # Windows FILE_ATTRIBUTE_REPARSE_POINT
        require(stat.S_ISDIR(info.st_mode) if part != path else stat.S_ISREG(info.st_mode),
                "finalize-local-path-type")
    require(path.resolve() == path, "finalize-local-path-alias")
    return path


def read_inspection(path, expected_sha):
    path = local_path(path, must_exist=True)
    size = path.stat().st_size
    require(0 < size <= LIMIT, "finalize-inspection-file-size")
    with path.open("rb") as handle:
        raw = handle.read(LIMIT + 1)
    local_path(path, must_exist=True)
    require(len(raw) == size and sha(raw) == expected_sha, "finalize-inspection-file-hash")
    return binding.decode_json(raw)


def control_source(commit, release_source):
    """No fetch or checkout: the caller must already be on clean, merged main."""
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT, timeout=45)
    require(not git("status", "--porcelain").strip(), "finalize-control-dirty")
    require(git("rev-parse", "HEAD").decode().strip() == commit
            and gh("repos/vzeman/biznisweb/git/ref/heads/main")["object"]["sha"] == commit,
            "finalize-control-not-current-main")
    require(git("remote", "get-url", "origin").decode().strip() in {
        "https://github.com/vzeman/biznisweb.git", "https://github.com/vzeman/biznisweb",
        "git@github.com:vzeman/biznisweb.git"}, "finalize-control-repository")
    subprocess.run(["git", "merge-base", "--is-ancestor", release_source, commit], cwd=ROOT,
                   check=True, capture_output=True, timeout=45)
    blob = git("show", commit + ":" + CONTROL_FILE)
    require(Path(__file__).read_bytes().replace(b"\r\n", b"\n") == blob,
            "finalize-control-file-not-committed")
    return sha(blob)


def private_object(s3, key, *, optional=False):
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)
    except Exception as exc:
        if optional and binding.error_code(exc) == "NoSuchKey":
            return None
        raise
    with response["Body"] as body:
        size = response.get("ContentLength")
        require(response.get("ServerSideEncryption") == "AES256" and type(size) is int
                and 0 < size <= LIMIT, "finalize-private-object-metadata")
        raw = body.read(LIMIT + 1)
    require(len(raw) == size and response.get("ETag"), "finalize-private-object-incomplete")
    value = binding.decode_json(raw)
    return value, raw, response["ETag"]


def immutable_put(s3, key, value):
    raw = binding.canonical_bytes(binding.normalized(value))
    require(len(raw) <= LIMIT, "finalize-receipt-too-large")
    s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType="application/json",
                  ExpectedBucketOwner=ACCOUNT, ServerSideEncryption="AES256", IfNoneMatch="*")
    observed = private_object(s3, key)
    require(observed[1] == raw, "finalize-receipt-readback")
    return {"key": key, "sha256": sha(raw)}


def validate_history(records, *, policy, release_id, release_source, gate_sha):
    """v0 accepts only the fully proved probe -> review wait -> failure chain."""
    require([row.get("phase") for row in records] == list(PHASES), "finalize-history-phases")
    before = records[0]
    for index, row in enumerate(records):
        require(type(row.get("schema_version")) is int and row["schema_version"] == 1 and row.get("project") == policy.project
                and row.get("release_id") == release_id and row.get("source_commit") == release_source
                and row.get("report_to_date") == before.get("report_to_date")
                and all(row.get(key) is False for key in ("promoted", "rerun_attempted", "legacy_current_binding_changed"))
                and row.get("start_uncertain", False) is False
                and row.get("dispatch_uncertain", False) is False, "finalize-history-identity-or-live")
        require(index == 0 or binding.stamp(row["at"]) >= binding.stamp(records[index - 1]["at"]),
                "finalize-history-time")
    original = binding.schedule_snapshot(before["schedule"])
    require(original.get("Name") == policy.service and original.get("State") == "ENABLED"
            and original["Target"]["Arn"] == CLUSTER and before.get("recovered_from") is None,
            "finalize-original-not-normal-enabled")
    candidate = records[5]["task_definition"]
    require(re.fullmatch(rf"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{policy.family}:[1-9][0-9]*", candidate)
            and candidate != original["Target"]["EcsParameters"]["TaskDefinitionArn"], "finalize-candidate-identity")
    identity, complete = records[10]["identity"], records[10]["complete"]
    task = records[8]["task_arn"]
    require(re.fullmatch(re.escape(CLUSTER.replace(":cluster/", ":task/") + "/") + r"[a-f0-9]{32}", task),
            "finalize-task-namespace")
    expected_identity = {"project": policy.project, "release_id": release_id, "source_commit": release_source,
        "image_digest": before["image"].rsplit("@", 1)[1], "task_arn": task, "task_definition": candidate,
        "instance_id": "N/A:Fargate", "service": policy.service, "path": "/app", "marker": policy.probe_marker,
        "gate_sha256": gate_sha, "private_ip": identity.get("private_ip")}
    ipaddress.IPv4Address(expected_identity["private_ip"])
    require(identity == expected_identity and records[9]["identity"] == identity,
            "finalize-probe-host-identity")
    require(all(row.get("known_task_arns") == ({} if i < 8 else {"probe": task})
                for i, row in enumerate(records)), "finalize-known-tasks")
    overrides = task_overrides(release_id, release_source, before["image"], gate_sha,
                               before["report_to_date"], probe=True, policy=policy)
    require(records[7]["overrides"] == overrides and records[7]["task_definition"] == candidate
            and records[7]["client_token"] == release_id + "-probe"
            and records[6]["role_arn"] == overrides["taskRoleArn"]
            and records[1]["lease_key"] == policy.lease_key, "finalize-dispatch-identity")
    probe_prefix = policy.probe_prefix + release_id + "/"
    require(complete.get("phase") == "report-verified" and all(complete.get(k) == v for k, v in identity.items())
            and complete.get("localhost_marker_sha256") == sha(canonical(identity))
            and all(complete.get(k) is False for k in ("provider_writes", "email_sent", "live_outputs_changed"))
            and all(complete.get(k) is True for k in ("skip_invoices", "skip_inline_guard"))
            and complete.get("output_manifest_key") == probe_prefix + "artifacts/output-manifest.json"
            and re.fullmatch(r"[a-f0-9]{64}", complete.get("output_manifest_sha256", "")), "finalize-probe-complete")
    review = records[11]
    env = binding.unique_environment(before["source_definition"]["containerDefinitions"][0].get("environment", []))
    schedule_overrides = json.loads(original["Target"].get("Input", "{}"))
    for container in schedule_overrides.get("containerOverrides", []):
        require(container.get("name") == "reporting", "finalize-original-override-container")
        env.update(binding.unique_environment(container.get("environment", [])))
    from_date = env.get("REPORT_FROM_DATE")
    require(isinstance(from_date, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", from_date)
            and re.fullmatch(r"[a-f0-9]{64}", before.get("independent_contract_sha256", "")), "finalize-original-review-contract")
    expected_review = {"schema_version": 1, "project": policy.project, "release_id": release_id,
        "source_commit": release_source, "image_digest": identity["image_digest"], "report_from_date": from_date,
        "report_to_date": before["report_to_date"], "probe_manifest_sha256": complete["output_manifest_sha256"],
        "source_contract_sha256": before["independent_contract_sha256"], "approved": True}
    require(review.get("review_key") == probe_prefix + "review/independent-source.json"
            and review.get("bindings") == expected_review and review["bindings"].get("approved") is True
            and type(review["bindings"].get("schema_version")) is int,
            "finalize-review-binding")
    paused = copy.deepcopy(original)
    paused["State"] = "DISABLED"
    require(all(binding.schedule_snapshot(records[i]["schedule"]) == paused for i in (2, 3)),
            "finalize-pause-history")
    expected_definition = image_only_definition(before["source_definition"], before["image"], policy=policy)
    require(binding.definition_snapshot(records[4]["definition"]) == binding.definition_snapshot(expected_definition),
            "finalize-candidate-request")
    return {"before": before, "original": original, "candidate": candidate, "identity": identity,
            "complete": complete, "task": task, "overrides": overrides, "probe_prefix": probe_prefix}


def terminal_probe(response, archived, history):
    tasks, failures = response.get("tasks", []), response.get("failures", [])
    missing = not tasks and len(failures) == 1 and failures[0].get("arn") == history["task"] and failures[0].get("reason") == "MISSING"
    require(missing or (len(tasks) == 1 and not failures), "finalize-probe-current-response")
    task = archived if missing else tasks[0]
    containers = task.get("containers", [])
    require(task.get("taskArn") == history["task"] and task.get("clusterArn") == CLUSTER
            and task.get("launchType") == "FARGATE" and task.get("startedBy") == history["before"]["release_id"]
            and task.get("taskDefinitionArn") == history["candidate"]
            and task.get("lastStatus") == task.get("desiredStatus") == "STOPPED"
            and task.get("stoppedAt") and task.get("startedAt")
            and binding.stamp(binding.normalized(task["stoppedAt"])) >= binding.stamp(binding.normalized(task["startedAt"]))
            and len(containers) == 1 and containers[0].get("name") == "reporting"
            and containers[0].get("lastStatus") == "STOPPED" and type(containers[0].get("exitCode")) is int
            and containers[0]["exitCode"] == 0 and containers[0].get("image") == history["before"]["image"]
            and containers[0].get("imageDigest") == history["identity"]["image_digest"]
            and [row.get("privateIpv4Address") for row in containers[0].get("networkInterfaces", [])]
                == [history["identity"]["private_ip"]], "finalize-probe-not-owned-stopped-zero")
    actual = copy.deepcopy(task.get("overrides", {}))
    for container in actual.get("containerOverrides", []):
        env = binding.unique_environment(container.get("environment", []))
        container["environment"] = [{"name": k, "value": v} for k, v in sorted(env.items())]
    require(actual.get("containerOverrides") == history["overrides"]["containerOverrides"]
            and actual.get("taskRoleArn") == history["overrides"]["taskRoleArn"], "finalize-probe-command-or-role")
    return {"archived_because_missing": missing, "task": binding.normalized(task)}


def lease_matches(found, owner, expected_sha, expected_etag, state="uncertain"):
    value, etag = found
    binding.validate_lock(value)
    require(value["owner"] == owner and value["state"] == state and etag == expected_etag
            and digest(value) == expected_sha, "finalize-lease-drift")


class Inspector:
    def __init__(self, session, args):
        self.args, self.session = args, session
        self.policy = project_policy(args.project)
        self.reader = ImageRelease(session, args.release_source, "2000-01-01", 60,
                                   project=args.project, release_id=args.release_id)
        self.s3 = self.reader.s3

    def capture(self, *, expected_lease=None):
        args, policy, reader = self.args, self.policy, self.reader
        require(self.session.region_name == REGION
                and self.session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "finalize-account-region")
        control_sha = control_source(args.control_source, args.release_source)
        binding.require_private_bucket(self.s3)
        reader.exclusion()
        prefix = policy.release_prefix + args.release_id + "/"
        listing = self.s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, ExpectedBucketOwner=ACCOUNT, MaxKeys=1000)
        expected_keys = [prefix + f"{i:04d}-{phase}.json" for i, phase in enumerate(PHASES, 1)]
        require(not listing.get("IsTruncated") and [row["Key"] for row in listing.get("Contents", [])] == expected_keys,
                "finalize-release-receipt-inventory")
        fetched = [private_object(self.s3, key) for key in expected_keys]
        records = [row[0] for row in fetched]
        require(sha(fetched[-1][1]) == args.failure_sha256, "finalize-failure-hash")
        history = validate_history(records, policy=policy, release_id=args.release_id,
                                   release_source=args.release_source, gate_sha=committed_gate_sha(args.release_source))
        require(private_object(self.s3, history["probe_prefix"] + "review/independent-source.json", optional=True) is None,
                "finalize-approval-exists")
        ready = private_object(self.s3, history["probe_prefix"] + "markers/ready.json")
        authorization = private_object(self.s3, history["probe_prefix"] + "authorize.json")
        require(ready[0] == history["identity"] and sha(ready[1]) == sha(canonical(history["identity"]))
                and authorization[0] == {"phase": "host-authorized", "release_id": args.release_id,
                    "task_arn": history["task"], "ready_sha256": sha(ready[1])}, "finalize-localhost-authorization")
        manifest = private_object(self.s3, history["complete"]["output_manifest_key"])
        require(sha(manifest[1]) == history["complete"]["output_manifest_sha256"], "finalize-probe-manifest-hash")
        schedules = reader.all_schedules()
        require(schedules[policy.service] == history["original"], "finalize-schedule-not-restored")
        require({k: v for k, v in schedules.items() if k != policy.service} == history["before"]["protected_schedules"],
                "finalize-protected-schedule-drift")
        original_arn = history["original"]["Target"]["EcsParameters"]["TaskDefinitionArn"]
        original = reader.definition(original_arn)
        candidate = reader.definition(history["candidate"])
        expected_candidate = image_only_definition(original, history["before"]["image"], policy=policy)
        expected_candidate["taskDefinitionArn"] = history["candidate"]
        require(original.get("taskDefinitionArn") == original_arn and original.get("status") == "ACTIVE"
                and binding.definition_snapshot(original) == binding.definition_snapshot(history["before"]["source_definition"]),
                "finalize-original-definition-drift")
        require(candidate.get("taskDefinitionArn") == history["candidate"] and candidate.get("status") == "INACTIVE"
                and binding.definition_snapshot(candidate) == binding.definition_snapshot(expected_candidate)
                and all(row.get("Target", {}).get("EcsParameters", {}).get("TaskDefinitionArn") != history["candidate"]
                        for row in schedules.values()), "finalize-candidate-not-inactive-unused")
        # ECS desired RUNNING includes pending tasks; PENDING is not a valid desired status.
        reader.no_report_tasks()
        probe = terminal_probe(reader.ecs.describe_tasks(cluster=CLUSTER, tasks=[history["task"]]),
                               records[10]["terminal_task"], history)
        try:
            reader.iam.get_role(RoleName=policy.role_prefix + args.release_id)
        except Exception as exc:
            require(binding.error_code(exc) == "NoSuchEntity", "finalize-probe-role-read-failed")
        else:
            raise RuntimeError("finalize-probe-role-present")
        own = read_lease(self.s3, policy.project)
        if expected_lease is None:
            lease_matches(own, args.release_id, args.lease_sha256, args.lease_etag)
        else:
            lease_matches(own, args.release_id, digest(expected_lease[0]), expected_lease[1], "released")
        peer = PROJECTS["roy" if policy.project == "vevo" else "vevo"]
        peer_lease = read_lease(self.s3, peer.project)
        require(peer_lease[0]["state"] == "released", "finalize-peer-not-released")
        outputs = reader.output_snapshot(policy)
        require(outputs == history["before"]["original_live_outputs"], "finalize-own-outputs-changed")
        definitions = {}
        for row in schedules.values():
            arn = row.get("Target", {}).get("EcsParameters", {}).get("TaskDefinitionArn")
            if arn is None or arn in definitions:
                continue
            require(re.fullmatch(rf"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/[A-Za-z0-9_-]+:[1-9][0-9]*", arn),
                    "finalize-scheduled-definition-namespace")
            definition = reader.definition(arn)
            require(definition.get("taskDefinitionArn") == arn and definition.get("status") == "ACTIVE",
                    "finalize-scheduled-definition-identity")
            definitions[arn] = {"status": "ACTIVE", "definition_sha256": digest(binding.definition_snapshot(definition))}
        apps = {}
        client = self.session.client("apprunner")
        for project, (name, service_id) in SERVICES.items():
            arn = f"arn:aws:apprunner:{REGION}:{ACCOUNT}:service/{name}/{service_id}"
            service = client.describe_service(ServiceArn=arn)["Service"]
            require(service.get("ServiceArn") == arn and service.get("ServiceName") == name
                    and service.get("ServiceId") == service_id and service.get("Status") == "RUNNING",
                    "finalize-apprunner-identity")
            # Hash all configuration fields in memory; never persist auth environment values.
            apps[project] = {"arn": arn, "status": "RUNNING", "configuration_sha256": digest(
                {key: service.get(key) for key in APP_FIELDS})}
        # Peer outputs are today's baseline, not the historical release's baseline.
        return binding.normalized({"schema_version": 1, "account": ACCOUNT, "region": REGION,
            "project": policy.project, "release_id": args.release_id, "release_source": args.release_source,
            "control_source": args.control_source, "control_file_sha256": control_sha,
            "failure_sha256": args.failure_sha256, "receipt_sha256": dict(zip(expected_keys, [sha(x[1]) for x in fetched])),
            "schedules": schedules, "original_definition": binding.definition_snapshot(original),
            "candidate_definition": binding.definition_snapshot(candidate), "candidate_status": "INACTIVE",
            "own_lease": own, "peer_lease": peer_lease, "own_outputs": outputs,
            "peer_outputs": reader.output_snapshot(peer), "probe": probe, "identity": history["identity"],
            "scheduled_definitions": definitions, "apprunner": apps,
            "localhost_ready_sha256": sha(ready[1]), "authorization_sha256": sha(authorization[1]),
            "probe_manifest_sha256": sha(manifest[1]), "active_report_tasks": [], "probe_role_present": False})


def validate_inspection(proof, snapshot, now):
    require(proof.get("schema_version") == 1 and proof.get("phase") == "restored-release-inspected"
            and proof.get("passed") is True and proof.get("snapshot") == snapshot, "finalize-inspection-binding")
    require(0 <= (now - binding.stamp(proof["captured_at"])).total_seconds() < 900,
            "finalize-inspection-expired")


def single_release_cas(lease, old, desired):
    """One PUT only; a lost acknowledgement is accepted only by exact readback."""
    require(lease._read() == old, "finalize-cas-precondition-drift")
    try:
        etag = lease._put(desired, old[1])
        require(isinstance(etag, str) and etag and etag != old[1], "finalize-cas-etag-not-changed")
        return etag
    except Exception as exc:
        try:
            observed, etag = lease._read()
        except Exception:
            raise RuntimeError("finalize-cas-outcome-unknown") from exc
        require(observed == desired and etag != old[1], "finalize-cas-not-confirmed")
        return etag


def finalize(inspector, proof, proof_sha, operation_id, *, now=utcnow):
    require(digest(proof) == proof_sha, "finalize-inspection-sha256")
    snapshot = inspector.capture()
    validate_inspection(proof, snapshot, now())
    old = tuple(snapshot["own_lease"])
    timestamp = now()
    desired = dict(old[0], state="released", updated_at=timestamp.isoformat(),
                   expires_at=(timestamp + timedelta(minutes=20)).isoformat(), generation=uuid.uuid4().hex)
    binding.validate_lock(desired)
    # One intent per failed release, not per invocation UUID: a new UUID cannot
    # bypass an interrupted attempt. A later recovery needs separate review.
    prefix = f"data/{inspector.policy.project}/reporting/release-finalizations/{inspector.args.release_id}/"
    # Existing intent is an explicit no-replay barrier, even when its previous PUT acknowledgement was lost.
    require(private_object(inspector.s3, prefix + "intent.json", optional=True) is None, "finalize-intent-already-exists")
    intent = {"schema_version": 1, "phase": "lease-finalization-intent", "operation_id": operation_id,
              "at": timestamp.isoformat(), "inspection_sha256": proof_sha, "inspection": proof,
              "expected_lease": old, "desired_lease": desired}
    intent_ref = immutable_put(inspector.s3, prefix + "intent.json", intent)
    cas_attempted = False
    try:
        require(inspector.capture() == snapshot, "finalize-boundary-changed-before-cas")
        validate_inspection(proof, snapshot, now())
        policy = inspector.policy
        lease = (binding.MigrationLease(inspector.s3, owner=inspector.args.release_id) if policy.project == "vevo" else
                 ScopedImageLease(inspector.s3, owner=inspector.args.release_id, project=policy.project))
        cas_attempted = True
        etag = single_release_cas(lease, old, desired)
        final = inspector.capture(expected_lease=(desired, etag))
        expected = dict(snapshot, own_lease=[desired, etag])
        require(final == expected, "finalize-boundary-changed-after-cas")
    except Exception as exc:
        failure = {"schema_version": 1, "phase": "finalization-inspection-required", "passed": False,
                   "at": now().isoformat(), "intent": intent_ref, "cas_attempted": cas_attempted,
                   "error_type": type(exc).__name__, "automatic_retry_or_rollback": False}
        try:
            immutable_put(inspector.s3, prefix + "failed.json", failure)
        except Exception:
            # The durable intent still identifies the exact possible write.
            print(json.dumps({"phase": "finalization-failure-receipt-unconfirmed", "intent": intent_ref}), file=sys.stderr)
        raise
    result = {"schema_version": 1, "phase": "restored-release-finalized", "passed": True,
              "at": now().isoformat(), "intent": intent_ref, "snapshot": final,
              "local_process": {"pid": os.getpid(), "parent_pid": os.getppid(), "cwd": str(ROOT),
                                "persistent_children_started": False, "listeners_started": False}}
    return result, immutable_put(inspector.s3, prefix + "complete.json", result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("inspect", "finalize"))
    parser.add_argument("--project", required=True, choices=tuple(PROJECTS))
    for name in ("release-id", "release-source", "control-source", "failure-sha256", "lease-sha256", "lease-etag", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--inspection")
    parser.add_argument("--inspection-sha256")
    parser.add_argument("--operation-id")
    parser.add_argument("--profile")
    args = parser.parse_args()
    for name, size in (("release_id", 32), ("release_source", 40), ("control_source", 40),
                       ("failure_sha256", 64), ("lease_sha256", 64)):
        require(re.fullmatch(rf"[a-f0-9]{{{size}}}", getattr(args, name)), "finalize-argument-format")
    require(bool(args.lease_etag) and len(args.lease_etag) < 200, "finalize-lease-etag-format")
    require((args.mode == "inspect" and not any((args.inspection, args.inspection_sha256, args.operation_id)))
            or (args.mode == "finalize" and args.inspection and re.fullmatch(r"[a-f0-9]{64}", args.inspection_sha256 or "")
                and re.fullmatch(r"[a-f0-9]{32}", args.operation_id or "")), "finalize-mode-arguments")
    output = local_path(args.output)
    require(not output.exists(), "finalize-local-output-collision")
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    # No SDK retries of writes, including an ambiguous CAS or immutable intent.
    session._session.set_default_client_config(Config(connect_timeout=10, read_timeout=30,
                                                     retries={"total_max_attempts": 1}))
    inspector = Inspector(session, args)
    if args.mode == "inspect":
        captured_at = utcnow().isoformat()
        value = {"schema_version": 1, "phase": "restored-release-inspected", "passed": True,
                 "snapshot": inspector.capture(), "captured_at": captured_at}
        remote = None
    else:
        proof = read_inspection(args.inspection, args.inspection_sha256)
        value, remote = finalize(inspector, proof, args.inspection_sha256, args.operation_id)
    output.parent.mkdir(parents=True, exist_ok=True)
    local_path(output)
    raw = binding.canonical_bytes(value)
    with output.open("xb") as handle:
        handle.write(raw)
    print(json.dumps({"phase": value["phase"], "passed": True, "output": str(output), "sha256": sha(raw), "receipt": remote}))


if __name__ == "__main__":
    main()
