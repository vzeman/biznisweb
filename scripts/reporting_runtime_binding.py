"""Current VEVO reporting authority, separate from immutable A/A history.

No provider client exists here. Read gates perform only bounded AWS reads.
Publication/lease methods are called only by a reviewed managed deployment.
"""
from __future__ import annotations

import argparse
import copy
from contextlib import ExitStack, closing
from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
POLICY = json.loads((ROOT / "projects/vevo/reporting_runtime_policy.json").read_text(encoding="utf-8"))
ACCOUNT, REGION, BUCKET = (POLICY[k] for k in ("account", "region", "bucket"))
PREFIX = POLICY["prefix"]
CURRENT_KEY, LOCK_KEY = PREFIX + "current.json", PREFIX + "migration.json"
LIMIT = 512 * 1024
SCHEDULE_FIELDS = frozenset({
    "Name", "GroupName", "Description", "StartDate", "EndDate", "ScheduleExpression",
    "ScheduleExpressionTimezone", "State", "FlexibleTimeWindow", "Target", "KmsKeyArn", "ActionAfterCompletion",
})
TASK_FIELDS = frozenset({
    "family", "taskRoleArn", "executionRoleArn", "networkMode", "containerDefinitions", "volumes",
    "placementConstraints", "requiresCompatibilities", "cpu", "memory", "pidMode", "ipcMode",
    "proxyConfiguration", "inferenceAccelerators", "ephemeralStorage", "runtimePlatform", "enableFaultInjection",
    "taskDefinitionArn",
})
RECORD_KEYS = frozenset({
    "schema_version", "project", "account", "region", "cluster", "service", "path", "phase", "release_id",
    "source_commit", "image_digest", "workflow_run_id", "build_run_id", "protected_sha256", "schedule",
    "task_definition", "predecessor_sha256", "candidate_proof", "verified_at", "historical_evidence_sha256",
    "rollback_proof", "candidate_task_definition",
})


class BindingError(ValueError):
    pass


def require(ok, reason):
    if not ok:
        raise BindingError(reason)


def canonical_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def sha256(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def exact_keys(value, names, reason):
    require(isinstance(value, dict) and set(value) == set(names), reason)


def hex_value(value, length=64):
    return isinstance(value, str) and re.fullmatch(r"[a-f0-9]{%d}" % length, value) is not None


def stamp(value):
    require(isinstance(value, str), "runtime-time-invalid")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise BindingError("runtime-time-invalid") from None
    require(result.tzinfo is not None, "runtime-time-without-zone")
    return result


def now_utc():
    return datetime.now(timezone.utc)


def normalized(value):
    if isinstance(value, datetime):
        require(value.tzinfo is not None, "runtime-aws-date-without-zone")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {k: normalized(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalized(v) for v in value]
    return value


def schedule_snapshot(schedule):
    require(isinstance(schedule, dict), "runtime-schedule-not-object")
    return normalized({k: v for k, v in schedule.items() if k in SCHEDULE_FIELDS})


def definition_snapshot(definition):
    require(isinstance(definition, dict), "runtime-definition-not-object")
    return normalized({k: v for k, v in definition.items() if k in TASK_FIELDS})


def unique_environment(rows):
    require(isinstance(rows, list), "runtime-environment-invalid")
    result = {}
    for row in rows:
        exact_keys(row, {"name", "value"}, "runtime-environment-invalid")
        name, value = row["name"], row["value"]
        require(isinstance(name, str) and name and name not in result and isinstance(value, str), "runtime-environment-duplicate")
        result[name] = value
    return result


def validate_configuration(schedule, definition, *, migrated):
    require(schedule == schedule_snapshot(schedule) and definition == definition_snapshot(definition), "runtime-config-extra-fields")
    require(schedule.get("Name") == POLICY["service"] and schedule.get("GroupName", "default") == "default"
            and schedule.get("State") == "ENABLED" and schedule.get("ScheduleExpression") == "cron(0 1 * * ? *)"
            and schedule.get("ScheduleExpressionTimezone") == "Europe/Bratislava"
            and schedule.get("FlexibleTimeWindow") == {"Mode": "OFF"}, "runtime-schedule-identity")
    target = schedule.get("Target", {})
    ecs = target.get("EcsParameters", {})
    arn = definition.get("taskDefinitionArn", "")
    require(re.fullmatch(rf"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/vevo-reporting-daily:[1-9][0-9]*", arn) is not None
            and ecs.get("TaskDefinitionArn") == arn and target.get("Arn") == POLICY["cluster"]
            and ecs.get("LaunchType") == "FARGATE" and ecs.get("TaskCount", 1) == 1, "runtime-target-identity")
    require(definition.get("family") == POLICY["family"] and definition.get("networkMode") == "awsvpc", "runtime-definition-identity")
    containers = definition.get("containerDefinitions")
    require(isinstance(containers, list) and len(containers) == 1, "runtime-container-count")
    container = containers[0]
    require(container.get("name") == "reporting" and not container.get("entryPoint")
            and container.get("workingDirectory", "/app") == "/app"
            and container.get("command") in (None, ["python", "daily_report_runner.py"]), "runtime-command-identity")
    require(re.fullmatch(rf"{ACCOUNT}\.dkr\.ecr\.{REGION}\.amazonaws\.com/vevo-reporting@sha256:[a-f0-9]{{64}}",
                         container.get("image", "")) is not None, "runtime-image-not-immutable")
    env = unique_environment(container.get("environment", []))
    require(env.get("REPORT_PROJECT") == "vevo", "runtime-project-identity")
    secrets = container.get("secrets", [])
    require(isinstance(secrets, list), "runtime-secrets-invalid")
    names = set()
    for row in secrets:
        exact_keys(row, {"name", "valueFrom"}, "runtime-secret-reference-invalid")
        name, reference = row["name"], row["valueFrom"]
        require(isinstance(name, str) and name and name not in names and name not in env
                and isinstance(reference, str) and reference.startswith(f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:vevo/"),
                "runtime-secret-project-or-duplicate")
        names.add(name)
    require({"BIZNISWEB_API_TOKEN", "BIZNISWEB_API_URL"}.issubset(names)
            and not {"BIZNISWEB_API_TOKEN", "BIZNISWEB_PASSWORD"}.intersection(env), "runtime-credentials-not-references")
    overrides = {}
    if "Input" in target:
        payload = decode_json(target["Input"].encode())
        exact_keys(payload, {"containerOverrides"}, "runtime-input-unreviewed")
        rows = payload["containerOverrides"]
        require(isinstance(rows, list) and len(rows) == 1, "runtime-input-container-count")
        exact_keys(rows[0], {"name", "environment"}, "runtime-input-command-unreviewed")
        require(rows[0]["name"] == "reporting", "runtime-input-container")
        overrides = unique_environment(rows[0]["environment"])
        require(set(overrides).issubset({"REPORT_SKIP_INVOICES", "REPORT_SKIP_CREDITNOTE_STORNO_GUARD"})
                and all(v == "true" for v in overrides.values()), "runtime-input-unreviewed")
    require(not names.intersection({"REPORT_PROJECT", "REPORT_SKIP_EMAIL", "REPORT_SKIP_INVOICES", "REPORT_SKIP_CREDITNOTE_STORNO_GUARD"}), "runtime-secret-overrides-control")
    effective = {**env, **overrides}
    if migrated:
        require(all(effective.get(k) == "true" for k in ("REPORT_SKIP_INVOICES", "REPORT_SKIP_CREDITNOTE_STORNO_GUARD")),
                "runtime-migration-skips-missing")


def validate_record(record):
    exact_keys(record, RECORD_KEYS, "runtime-record-schema")
    require(type(record["schema_version"]) is int and record["schema_version"] == 1, "runtime-record-version")
    for name in ("project", "account", "region", "cluster", "service", "path", "historical_evidence_sha256"):
        require(record[name] == POLICY[name], "runtime-record-policy")
    phase = record["phase"]
    require(phase in {"baseline-readback-verified", "promotion-readback-verified", "rollback-readback-verified"}, "runtime-record-not-terminal")
    require(isinstance(record["release_id"], str) and re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{7,95}", record["release_id"]), "runtime-release-id")
    require(hex_value(record["source_commit"], 40) and hex_value(record["protected_sha256"]), "runtime-source-or-protected-hash")
    require(isinstance(record["workflow_run_id"], str) and re.fullmatch(r"[1-9][0-9]{5,19}", record["workflow_run_id"]), "runtime-run-id")
    stamp(record["verified_at"])
    require(re.fullmatch(r"sha256:[a-f0-9]{64}", record["image_digest"]) is not None, "runtime-digest")
    validate_configuration(record["schedule"], record["task_definition"], migrated=phase == "promotion-readback-verified")
    definition = record["task_definition"]
    candidate = record["candidate_task_definition"]
    require(isinstance(candidate, dict) and candidate == definition_snapshot(candidate), "runtime-candidate-definition")
    require(definition["containerDefinitions"][0]["image"].endswith("@" + record["image_digest"]), "runtime-definition-image")
    if phase == "baseline-readback-verified":
        require(definition["taskDefinitionArn"] == POLICY["baseline_definition"]
                and record["source_commit"] == POLICY["baseline_source_commit"]
                and record["image_digest"] == POLICY["baseline_image_digest"]
                and record["predecessor_sha256"] is None and record["build_run_id"] is None, "runtime-baseline-not-reviewed")
    else:
        require(hex_value(record["predecessor_sha256"]), "runtime-predecessor-missing")
    if phase == "promotion-readback-verified":
        require(isinstance(record["build_run_id"], str) and re.fullmatch(r"[1-9][0-9]{5,19}", record["build_run_id"]), "runtime-build-id")
    proof = record["candidate_proof"]
    common = {"task_arn", "private_ip", "definition_arn", "image_digest", "instance_id", "service", "path", "localhost_marker_sha256", "command", "exit_code", "stopped"}
    report_fields = {"output_manifest_key", "output_manifest_sha256", "provider_writes", "email_sent", "live_outputs_changed", "skip_invoices", "skip_inline_guard"}
    require(isinstance(proof, dict) and set(proof) in (common, common | report_fields), "runtime-host-proof-fields")
    require(proof["instance_id"] == "N/A:Fargate" and proof["service"] == POLICY["service"] and proof["path"] == "/app"
            and proof["definition_arn"] == candidate["taskDefinitionArn"] and proof["image_digest"] == record["image_digest"]
            and type(proof["exit_code"]) is int and proof["exit_code"] == 0 and proof["stopped"] is True
            and hex_value(proof["localhost_marker_sha256"]), "runtime-host-proof-identity")
    require(re.fullmatch(rf"arn:aws:ecs:{REGION}:{ACCOUNT}:task/vevo-reporting-cluster/[a-f0-9]{{32}}", proof["task_arn"]) is not None,
            "runtime-host-task")
    try:
        address = ipaddress.ip_address(proof["private_ip"])
    except ValueError:
        raise BindingError("runtime-host-ip") from None
    require(address in ipaddress.ip_network("172.31.0.0/16"), "runtime-host-ip")
    require(isinstance(proof["command"], list) and proof["command"] and all(isinstance(x, str) and x for x in proof["command"]), "runtime-host-command")
    if phase == "promotion-readback-verified" or report_fields.intersection(proof):
        require(set(proof) == common | report_fields and proof["provider_writes"] is False and proof["email_sent"] is False
                and proof["live_outputs_changed"] is False and proof["skip_invoices"] is True and proof["skip_inline_guard"] is True
                and hex_value(proof["output_manifest_sha256"]) and isinstance(proof["output_manifest_key"], str)
                and proof["output_manifest_key"].startswith(PREFIX + "probes/") and ".." not in proof["output_manifest_key"], "runtime-report-proof")
        expected = copy.deepcopy(definition)
        expected["taskDefinitionArn"] = candidate["taskDefinitionArn"]
        require(re.fullmatch(rf"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/vevo-reporting-daily:[1-9][0-9]*", candidate["taskDefinitionArn"]) is not None
                and candidate["taskDefinitionArn"] != definition["taskDefinitionArn"], "runtime-candidate-arn")
        command = proof["command"]
        require(len(command) == 10 and command[:3] == ["python", "scripts/reporting_migration_host_gate.py", "--release-id"]
                and hex_value(command[3], 32) and command[4:9] == ["--source-commit", record["source_commit"], "--image-digest", record["image_digest"], "--gate-sha256"]
                and hex_value(command[9]), "runtime-candidate-command")
        if phase == "promotion-readback-verified":
            require(command[3] == record["release_id"], "runtime-candidate-release")
        require(proof["output_manifest_key"].startswith(PREFIX + "probes/" + command[3] + "/artifacts/"), "runtime-candidate-output-scope")
        expected["taskRoleArn"] = f"arn:aws:iam::{ACCOUNT}:role/VevoReportProbe-{command[3]}"
        require(expected["taskRoleArn"] != definition.get("taskRoleArn"), "runtime-candidate-role-not-isolated")
        expected_container = expected["containerDefinitions"][0]
        expected_container["command"] = command
        env = unique_environment(expected_container.get("environment", []))
        env["REPORT_SKIP_EMAIL"] = "true"
        actual_container = candidate.get("containerDefinitions", [{}])[0]
        require(unique_environment(actual_container.get("environment", [])) == env, "runtime-candidate-environment")
        expected_container["environment"] = actual_container["environment"]
        require(candidate == expected, "runtime-candidate-unreviewed-difference")
    else:
        require(candidate == definition, "runtime-baseline-candidate-drift")
    if phase == "rollback-readback-verified":
        rollback = record["rollback_proof"]
        exact_keys(rollback, {"workflow_run_id", "observed_at", "restored_schedule_sha256", "restored_definition_sha256", "failed_record_sha256"}, "runtime-rollback-proof")
        require(rollback["workflow_run_id"] == record["workflow_run_id"] and rollback["observed_at"] == record["verified_at"]
                and rollback["restored_schedule_sha256"] == sha256(record["schedule"])
                and rollback["restored_definition_sha256"] == sha256(definition)
                and rollback["failed_record_sha256"] == record["predecessor_sha256"], "runtime-rollback-readback")
    else:
        require(record["rollback_proof"] is None, "runtime-unexpected-rollback")


def validate_runtime(binding, schedule, definition):
    record = binding["record"]
    validate_record(record)
    require(binding["record_sha256"] == sha256(record), "runtime-binding-hash")
    if "status" in definition:
        require(definition["status"] == "ACTIVE", "runtime-definition-inactive")
    require(schedule_snapshot(schedule) == record["schedule"] and definition_snapshot(definition) == record["task_definition"], "runtime-current-config-drift")


def build_promotion_record(previous_binding, schedule, task_definition, *, release_id, source_commit, image,
                           workflow_run_id, build_run_id, protected_sha256, candidate_proof, verified_at, candidate_task_definition):
    validate_record(previous_binding["record"])
    require(previous_binding["record_sha256"] == sha256(previous_binding["record"]), "runtime-predecessor-hash")
    require(previous_binding["record"]["workflow_run_id"] != workflow_run_id, "runtime-promotion-requires-separate-run")
    require(image == task_definition["containerDefinitions"][0]["image"], "runtime-requested-image-drift")
    validate_production_transition(previous_binding["record"], schedule_snapshot(schedule), definition_snapshot(task_definition))
    return _record(schedule, task_definition, release_id=release_id, source_commit=source_commit,
                   image_digest=image.rsplit("@", 1)[-1], workflow_run_id=workflow_run_id, build_run_id=build_run_id,
                   protected_sha256=protected_sha256, candidate_proof=candidate_proof, verified_at=verified_at,
                   phase="promotion-readback-verified", predecessor_sha256=previous_binding["record_sha256"],
                   candidate_task_definition=definition_snapshot(candidate_task_definition))


def validate_production_transition(previous, schedule, definition):
    """A migration cannot silently change inherited credentials or privileges."""
    expected = copy.deepcopy(previous["task_definition"])
    expected["taskDefinitionArn"] = definition["taskDefinitionArn"]
    old_container, new_container = expected["containerDefinitions"][0], definition["containerDefinitions"][0]
    old_container["image"] = new_container["image"]
    if new_container.get("workingDirectory") == "/app":
        old_container["workingDirectory"] = "/app"
    if new_container.get("command") == ["python", "daily_report_runner.py"]:
        old_container["command"] = new_container["command"]
    env = unique_environment(old_container.get("environment", []))
    env.update(REPORT_SKIP_INVOICES="true", REPORT_SKIP_CREDITNOTE_STORNO_GUARD="true")
    require(unique_environment(new_container.get("environment", [])) == env, "runtime-production-environment-drift")
    old_container["environment"] = new_container["environment"]
    require(definition == expected, "runtime-production-unreviewed-difference")
    expected_schedule = copy.deepcopy(previous["schedule"])
    expected_schedule["Target"]["EcsParameters"]["TaskDefinitionArn"] = definition["taskDefinitionArn"]
    require(schedule == expected_schedule, "runtime-production-schedule-drift")


def _record(schedule, definition, **fields):
    result = {"schema_version": 1, **{k: POLICY[k] for k in ("project", "account", "region", "cluster", "service", "path", "historical_evidence_sha256")},
              "schedule": schedule_snapshot(schedule), "task_definition": definition_snapshot(definition),
              "candidate_task_definition": definition_snapshot(definition), "rollback_proof": None, **copy.deepcopy(fields)}
    validate_record(result)
    return result


def build_baseline_record(schedule, task_definition, *, release_id, workflow_run_id, protected_sha256, candidate_proof, verified_at):
    return _record(schedule, task_definition, release_id=release_id, source_commit=POLICY["baseline_source_commit"],
                   image_digest=POLICY["baseline_image_digest"], workflow_run_id=workflow_run_id, build_run_id=None,
                   protected_sha256=protected_sha256, candidate_proof=candidate_proof, verified_at=verified_at,
                   phase="baseline-readback-verified", predecessor_sha256=None)


def decode_json(raw):
    require(isinstance(raw, bytes) and 0 < len(raw) <= LIMIT, "runtime-object-size")
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, "runtime-json-duplicate")
            result[key] = value
        return result
    def constant(_):
        raise BindingError("runtime-json-constant")
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, RecursionError, UnicodeError) as exc:
        raise BindingError("runtime-json-invalid") from exc


def require_private_bucket(s3):
    args = {"Bucket": BUCKET, "ExpectedBucketOwner": ACCOUNT}
    s3.head_bucket(**args)
    require(s3.get_bucket_location(**args).get("LocationConstraint") == REGION, "runtime-bucket-region")
    block = s3.get_public_access_block(**args).get("PublicAccessBlockConfiguration", {})
    require(all(block.get(k) is True for k in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")), "runtime-bucket-public")
    try:
        require(s3.get_bucket_policy_status(**args).get("PolicyStatus", {}).get("IsPublic") is False, "runtime-bucket-policy")
    except Exception as exc:
        if error_code(exc) != "NoSuchBucketPolicy":
            raise
    acl = s3.get_bucket_acl(**args)
    owner, grants = acl.get("Owner", {}).get("ID"), acl.get("Grants")
    require(owner and isinstance(grants, list) and grants and all(row.get("Grantee", {}).get("Type") == "CanonicalUser"
            and row["Grantee"].get("ID") == owner and row.get("Permission") == "FULL_CONTROL" for row in grants), "runtime-bucket-acl")


def error_code(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code")


def read_object(s3, key, *, optional=False):
    require(key.startswith(PREFIX) and ".." not in key, "runtime-key-outside-prefix")
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)
    except Exception as exc:
        if optional and error_code(exc) == "NoSuchKey":
            return None
        raise
    with closing(response["Body"]) as body:
        size = response.get("ContentLength")
        require(response.get("ServerSideEncryption") == "AES256" and type(size) is int and 0 < size <= LIMIT, "runtime-object-metadata")
        raw = body.read(LIMIT + 1)
        require(len(raw) == size, "runtime-object-truncated")
    value = decode_json(raw)
    require(raw == canonical_bytes(value) and isinstance(response.get("ETag"), str) and response["ETag"], "runtime-object-noncanonical")
    return value, response["ETag"]


def write_object(s3, key, value, *, etag=None):
    raw = canonical_bytes(value)
    require(len(raw) <= LIMIT, "runtime-write-size")
    s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType="application/json", ServerSideEncryption="AES256",
                  ExpectedBucketOwner=ACCOUNT, **({"IfMatch": etag} if etag else {"IfNoneMatch": "*"}))
    observed, new_etag = read_object(s3, key)
    require(observed == value, "runtime-write-readback")
    return new_etag


def validate_lock(value):
    exact_keys(value, {"schema_version", "owner", "state", "updated_at", "expires_at", "generation"}, "runtime-lock-schema")
    require(type(value["schema_version"]) is int and value["schema_version"] == 1 and isinstance(value["owner"], str) and value["owner"]
            and value["state"] in {"active", "released", "uncertain"} and hex_value(value["generation"], 32), "runtime-lock-identity")
    require(stamp(value["expires_at"]) > stamp(value["updated_at"]), "runtime-lock-time")


def check_migration(s3, *, lease=None):
    found = read_object(s3, LOCK_KEY, optional=True)
    if found is None:
        require(lease is None, "runtime-lease-disappeared")
        return None
    value, etag = found
    validate_lock(value)
    if lease is not None:
        require(lease.s3 is s3 and value["owner"] == lease.owner and etag == lease.etag and value["state"] == "active"
                and stamp(value["expires_at"]) > lease.now(), "runtime-lease-not-owned")
    else:
        # Expiry is evidence of an interrupted migration, never permission to steal it.
        require(value["state"] == "released", "runtime-migration-active-or-uncertain")
    return etag


class MigrationLease:
    def __init__(self, s3, *, owner, now=None):
        require(isinstance(owner, str) and re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{7,95}", owner), "runtime-lease-owner")
        self.s3, self.owner, self.now, self.etag = s3, owner, now or now_utc, None
        self.pending_value = None

    def _write(self, state, previous):
        now = self.now()
        value = {"schema_version": 1, "owner": self.owner, "state": state, "updated_at": now.isoformat(),
                 "expires_at": (now + timedelta(minutes=20)).isoformat(), "generation": uuid.uuid4().hex}
        self.pending_value = value
        try:
            self.etag = write_object(self.s3, LOCK_KEY, value, etag=previous)
        except Exception:
            # A PUT can commit before its response is lost. Read once; never
            # replay it or assume that a newer foreign generation is ours.
            observed = read_object(self.s3, LOCK_KEY, optional=True)
            if observed is None or observed[0] != value or observed[1] == previous:
                raise
            self.etag = observed[1]
        self.pending_value = None

    def acquire(self):
        require_private_bucket(self.s3)
        prior = check_migration(self.s3)
        self._write("active", prior)
        return self

    def renew(self):
        check_migration(self.s3, lease=self)
        self._write("active", self.etag)

    def release(self):
        check_migration(self.s3, lease=self)
        self._write("released", self.etag)

    def retain_uncertain(self):
        # This may also fail; the existing active/stale lock still blocks readers.
        found = read_object(self.s3, LOCK_KEY)
        if self.pending_value is not None and found[0] == self.pending_value:
            self.etag = found[1]
            self.pending_value = None
        require(found[1] == self.etag and found[0]["owner"] == self.owner, "runtime-lease-lost")
        self._write("uncertain", self.etag)


def load_current_binding(s3, *, lease=None):
    require_private_bucket(s3)
    lock_etag = check_migration(s3, lease=lease)
    pointer, etag = read_object(s3, CURRENT_KEY)
    require(lock_etag is not None, "runtime-established-migration-record-missing")
    exact_keys(pointer, {"schema_version", "record_key", "record_sha256"}, "runtime-pointer-schema")
    require(type(pointer["schema_version"]) is int and pointer["schema_version"] == 1 and hex_value(pointer["record_sha256"])
            and pointer["record_key"] == PREFIX + "releases/" + pointer["record_sha256"] + ".json", "runtime-pointer-identity")
    record, _ = read_object(s3, pointer["record_key"])
    validate_record(record)
    require(record["phase"] in {"baseline-readback-verified", "promotion-readback-verified"}, "runtime-current-record-phase")
    require(sha256(record) == pointer["record_sha256"], "runtime-record-hash")
    require(read_object(s3, CURRENT_KEY) == (pointer, etag) and check_migration(s3, lease=lease) == lock_etag, "runtime-authority-changed")
    return {"record": record, "record_key": pointer["record_key"], "record_sha256": pointer["record_sha256"],
            "pointer_etag": etag, "migration_etag": lock_etag}


def publish_verified_binding(s3, record, *, expected_pointer_etag, lease):
    require_private_bucket(s3)
    check_migration(s3, lease=lease)
    validate_record(record)
    require(record["phase"] in {"baseline-readback-verified", "promotion-readback-verified"}, "runtime-publish-phase")
    require(record["release_id"] == lease.owner, "runtime-publisher-owner")
    require(lease.now() - timedelta(hours=1) <= stamp(record["verified_at"]) <= lease.now(), "runtime-publish-stale-or-future")
    prior = read_object(s3, CURRENT_KEY, optional=True)
    require((prior[1] if prior else None) == expected_pointer_etag, "runtime-pointer-cas-preflight")
    if prior:
        previous = load_current_binding(s3, lease=lease)
        require(record["predecessor_sha256"] == previous["record_sha256"], "runtime-publish-predecessor")
        require(record["workflow_run_id"] != previous["record"]["workflow_run_id"], "runtime-publish-requires-separate-run")
        validate_production_transition(previous["record"], record["schedule"], record["task_definition"])
    else:
        require(record["phase"] == "baseline-readback-verified" and record["predecessor_sha256"] is None, "runtime-bootstrap-required")
    digest = sha256(record)
    key = PREFIX + "releases/" + digest + ".json"
    existing = read_object(s3, key, optional=True)
    if existing:
        require(existing[0] == record, "runtime-immutable-release-drift")
    else:
        write_object(s3, key, record)
    check_migration(s3, lease=lease)
    write_object(s3, CURRENT_KEY, {"schema_version": 1, "record_key": key, "record_sha256": digest}, etag=expected_pointer_etag)
    return load_current_binding(s3, lease=lease)


def restore_previous_binding(s3, previous_binding, *, expected_pointer_etag, lease, rollback_proof):
    current = load_current_binding(s3, lease=lease)
    require(current["pointer_etag"] == expected_pointer_etag and current["record"]["predecessor_sha256"] == previous_binding["record_sha256"], "runtime-rollback-generation")
    validate_record(previous_binding["record"])
    require(previous_binding["record_sha256"] == sha256(previous_binding["record"]), "runtime-rollback-previous-hash")
    record = copy.deepcopy(previous_binding["record"])
    record.update(phase="rollback-readback-verified", release_id=lease.owner + "-rollback", predecessor_sha256=current["record_sha256"],
                  rollback_proof=copy.deepcopy(rollback_proof), workflow_run_id=rollback_proof["workflow_run_id"], verified_at=rollback_proof["observed_at"])
    validate_record(record)
    # The failed run's rollback receipt is audit history, never current authority.
    # Restore the exact previous successful record rather than weakening the
    # independent current-run provenance gate to accept a failed deployment.
    digest = sha256(record)
    key = PREFIX + "rollbacks/" + digest + ".json"
    existing = read_object(s3, key, optional=True)
    if existing:
        require(existing[0] == record, "runtime-rollback-receipt-drift")
    else:
        write_object(s3, key, record)
    check_migration(s3, lease=lease)
    require(load_current_binding(s3, lease=lease) == current, "runtime-rollback-current-changed")
    write_object(s3, CURRENT_KEY, {"schema_version": 1, "record_key": previous_binding["record_key"],
                 "record_sha256": previous_binding["record_sha256"]}, etag=expected_pointer_etag)
    result = load_current_binding(s3, lease=lease)
    require(result["record"] == previous_binding["record"], "runtime-rollback-final-record")
    return result


def verify_current_runtime(s3, scheduler, ecs):
    binding = load_current_binding(s3)
    schedule = scheduler.get_schedule(Name=POLICY["service"])
    definition = ecs.describe_task_definition(taskDefinition=binding["record"]["task_definition"]["taskDefinitionArn"])["taskDefinition"]
    require(definition.get("status") == "ACTIVE", "runtime-definition-not-active")
    validate_runtime(binding, schedule, definition)
    again = load_current_binding(s3)
    require(again == binding, "runtime-binding-changed-during-check")
    return binding


def verify_managed_provenance(binding, *, fetch_run=None, require_completed=True):
    """Use independent GitHub metadata, never a receipt's claimed conclusion."""
    if fetch_run is None:
        def fetch_run(run_id):
            result = subprocess.run(["gh", "api", f"repos/vzeman/biznisweb/actions/runs/{run_id}"],
                                    capture_output=True, timeout=30, check=True)
            return decode_json(result.stdout)
    record = binding["record"]
    run = fetch_run(record["workflow_run_id"])
    own_running = (not require_completed and os.environ.get("GITHUB_ACTIONS") == "true"
                   and os.environ.get("GITHUB_REF") == "refs/heads/main"
                   and os.environ.get("GITHUB_RUN_ID") == record["workflow_run_id"]
                   and os.environ.get("GITHUB_SHA") == run.get("head_sha")
                   and os.environ.get("GITHUB_WORKFLOW_REF") == "vzeman/biznisweb/.github/workflows/deploy-vevo-report.yml@refs/heads/main")
    require(str(run.get("id")) == record["workflow_run_id"] and run.get("head_branch") == "main"
            and run.get("repository", {}).get("full_name") == "vzeman/biznisweb"
            and run.get("path") == ".github/workflows/deploy-vevo-report.yml"
            and run.get("event") == "workflow_dispatch"
            and (own_running and run.get("status") == "in_progress" and run.get("conclusion") is None
                 or run.get("status") == "completed" and run.get("conclusion") == "success"), "runtime-managed-run-unverified")
    if record["phase"] == "promotion-readback-verified":
        require(run.get("head_sha") == record["source_commit"], "runtime-managed-source-drift")
    if record["build_run_id"] is not None:
        build = fetch_run(record["build_run_id"])
        require(str(build.get("id")) == record["build_run_id"] and build.get("head_sha") == record["source_commit"]
                and build.get("head_branch") == "main" and build.get("repository", {}).get("full_name") == "vzeman/biznisweb"
                and build.get("path") == ".github/workflows/build-and-push-ecr.yml"
                and build.get("status") == "completed" and build.get("conclusion") == "success", "runtime-build-unverified")


def build_baseline_probe_command(source_text, runner_sha256, image_digest):
    require(isinstance(source_text, str) and source_text and hex_value(runner_sha256)
            and re.fullmatch(r"sha256:[a-f0-9]{64}", image_digest), "runtime-baseline-probe-input")
    filename = "/app/scripts/reporting_identity_probe.py"
    invocation = f"exec(compile({source_text!r}, {filename!r}, 'exec'), {{'__name__': '__main__', '__file__': {filename!r}}})"
    return ["python", "-c", invocation, "--runner-sha256", runner_sha256, "--image-digest", image_digest]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--compare", type=Path)
    args = parser.parse_args(argv)
    import boto3
    from botocore.config import Config
    with ExitStack() as stack:
        config = Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1})
        clients = {name: stack.enter_context(closing(boto3.client(name, region_name=REGION, config=config)))
                   for name in ("sts", "s3", "scheduler", "ecs", "ecr")}
        require(clients["sts"].get_caller_identity()["Account"] == ACCOUNT, "runtime-account")
        binding = verify_current_runtime(clients["s3"], clients["scheduler"], clients["ecs"])
        verify_managed_provenance(binding)
        record = binding["record"]
        images = clients["ecr"].describe_images(repositoryName="vevo-reporting", imageIds=[{"imageDigest": record["image_digest"]}]).get("imageDetails", [])
        require(len(images) == 1 and images[0].get("imageDigest") == record["image_digest"], "runtime-image-unavailable")
        if record["phase"] == "promotion-readback-verified":
            require("git-" + record["source_commit"] in images[0].get("imageTags", []), "runtime-source-image-tag-drift")
        require(load_current_binding(clients["s3"]) == binding, "runtime-authority-changed-after-provenance")
        if args.compare:
            require(decode_json(args.compare.read_bytes()) == binding, "runtime-workflow-binding-changed")
        args.output.write_bytes(canonical_bytes(binding))
    print("VEVO_CURRENT_REPORT_RUNTIME_OK:read-only:hash-bound")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("VEVO_CURRENT_REPORT_RUNTIME_BLOCKED:review-authority-or-active-migration", file=sys.stderr)
        raise SystemExit(1) from None
