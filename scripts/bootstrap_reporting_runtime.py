"""Publish only the independently reviewed current revision-33 baseline.

Managed workflow entry point; no task launch, schedule update or provider call.
The deployer supplies a hash-bound baseline host proof after its identity probe.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
from datetime import timedelta
import hashlib
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import reporting_runtime_binding as binding  # noqa: E402


def require_managed_source(record):
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, timeout=30).strip()
    commit = os.environ.get("GITHUB_SHA", "")
    binding.require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REF") == "refs/heads/main"
                    and os.environ.get("GITHUB_WORKFLOW_REF") == "vzeman/biznisweb/.github/workflows/deploy-vevo-report.yml@refs/heads/main"
                    and os.environ.get("GITHUB_RUN_ID") == record["workflow_run_id"] and binding.hex_value(commit, 40)
                    and git("rev-parse", "HEAD") == commit and not git("status", "--porcelain")
                    and git("ls-remote", "origin", "refs/heads/main").split() == [commit, "refs/heads/main"], "bootstrap-source-not-managed-main")


def verify_host(ecs, logs, record):
    proof = record["candidate_proof"]
    binding.require(logs is not None, "bootstrap-log-readback-required")
    probe_source = (ROOT / "scripts/reporting_identity_probe.py").read_text(encoding="utf-8")
    expected_command = binding.build_baseline_probe_command(probe_source, binding.POLICY["baseline_runner_sha256"], record["image_digest"])
    binding.require(proof["command"] == expected_command, "bootstrap-probe-source-contract")
    response = ecs.describe_tasks(cluster=binding.POLICY["cluster"], tasks=[proof["task_arn"]])
    tasks = response.get("tasks", [])
    binding.require(not response.get("failures") and len(tasks) == 1, "bootstrap-host-unavailable")
    task = tasks[0]
    containers = task.get("containers", [])
    binding.require(task.get("taskArn") == proof["task_arn"] and task.get("taskDefinitionArn") == proof["definition_arn"]
                    and task.get("clusterArn") == binding.POLICY["cluster"] and task.get("launchType") == "FARGATE"
                    and task.get("lastStatus") == "STOPPED" and len(containers) == 1, "bootstrap-host-identity")
    container = containers[0]
    addresses = {row.get("privateIpv4Address") for row in container.get("networkInterfaces", [])}
    binding.require(container.get("name") == "reporting" and container.get("imageDigest") == proof["image_digest"]
                    and type(container.get("exitCode")) is int and container["exitCode"] == 0 and addresses == {proof["private_ip"]},
                    "bootstrap-container-identity")
    overrides = task.get("overrides", {}).get("containerOverrides", [])
    binding.require(len(overrides) == 1 and overrides[0].get("name") == "reporting"
                    and overrides[0].get("command") == proof["command"], "bootstrap-probe-command")
    markers, token = [], None
    prefix = "VEVO_REPORT_BASELINE_HOST_OK "
    for _ in range(10):
        args = {"logGroupName": "/ecs/vevo-reporting-daily", "logStreamName": "ecs/reporting/" + proof["task_arn"].rsplit("/", 1)[1],
                "startFromHead": True, "limit": 1000}
        if token is not None:
            args["nextToken"] = token
        page = logs.get_log_events(**args)
        binding.require(isinstance(page.get("events"), list), "bootstrap-log-events-invalid")
        for event in page["events"]:
            message = event.get("message")
            binding.require(isinstance(message, str), "bootstrap-log-message-invalid")
            if message.startswith(prefix):
                markers.append(binding.decode_json(message[len(prefix):].encode()))
        next_token = page.get("nextForwardToken")
        binding.require(isinstance(next_token, str) and next_token, "bootstrap-log-page-invalid")
        if next_token == token:
            break
        token = next_token
    else:
        raise binding.BindingError("bootstrap-log-pagination-incomplete")
    expected = {"marker": "VEVO_REPORT_BASELINE_HOST_OK", "task_arn": proof["task_arn"], "private_ip": proof["private_ip"],
                "image_digest": proof["image_digest"], "path": "/app", "service": binding.POLICY["service"],
                "instance_id": "N/A:Fargate", "runner_sha256": binding.POLICY["baseline_runner_sha256"], "provider_reads": 0}
    binding.require(len(markers) == 1 and markers[0] == expected and type(markers[0]["provider_reads"]) is int,
                    "bootstrap-localhost-marker-invalid")
    # The host's canonical semantic marker has no trailing newline.
    marker_sha = hashlib.sha256(binding.canonical_bytes(expected).rstrip(b"\n")).hexdigest()
    binding.require(proof["localhost_marker_sha256"] == marker_sha, "bootstrap-localhost-marker-hash")


def bootstrap(s3, scheduler, ecs, record, *, apply, logs=None):
    binding.validate_record(record)
    binding.require(record["phase"] == "baseline-readback-verified", "bootstrap-baseline-only")
    now = binding.now_utc()
    binding.require(now - timedelta(hours=1) <= binding.stamp(record["verified_at"]) <= now, "bootstrap-stale-proof")
    binding.require_private_bucket(s3)
    binding.check_migration(s3)
    binding.require(binding.read_object(s3, binding.CURRENT_KEY, optional=True) is None, "bootstrap-already-initialized")
    snapshot = {"record": record, "record_sha256": binding.sha256(record)}
    def check():
        schedule = scheduler.get_schedule(Name=binding.POLICY["service"])
        definition = ecs.describe_task_definition(taskDefinition=record["task_definition"]["taskDefinitionArn"])["taskDefinition"]
        binding.validate_runtime(snapshot, schedule, definition)
        verify_host(ecs, logs, record)
    check()
    result = {"applied": False, "record_sha256": snapshot["record_sha256"], "provider_writes": 0, "schedule_writes": 0}
    if not apply:
        return result
    require_managed_source(record)
    lease = binding.MigrationLease(s3, owner=record["release_id"]).acquire()
    try:
        check()
        require_managed_source(record)
        published = binding.publish_verified_binding(s3, record, expected_pointer_etag=None, lease=lease)
        check()
        lease.release()
        binding.require(binding.load_current_binding(s3) == {**published, "migration_etag": lease.etag}, "bootstrap-final-readback")
        return {**result, "applied": True}
    except Exception:
        try:
            lease.retain_uncertain()
        except Exception:
            pass  # A retained active/uncertain lease remains blocked; never steal it.
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    raw = args.record.read_bytes()
    binding.require(binding.hex_value(args.sha256) and hashlib.sha256(raw).hexdigest() == args.sha256, "bootstrap-record-sha256")
    record = binding.decode_json(raw)
    binding.require(raw == binding.canonical_bytes(record), "bootstrap-record-noncanonical")
    binding.validate_record(record)
    if args.apply:
        require_managed_source(record)
    import boto3
    from botocore.config import Config
    with ExitStack() as stack:
        config = Config(connect_timeout=10, read_timeout=30, retries={"total_max_attempts": 1})
        clients = {name: stack.enter_context(closing(boto3.client(name, region_name=binding.REGION, config=config)))
                   for name in ("sts", "s3", "scheduler", "ecs", "logs")}
        binding.require(clients["sts"].get_caller_identity()["Account"] == binding.ACCOUNT, "bootstrap-account")
        result = bootstrap(clients["s3"], clients["scheduler"], clients["ecs"], record, apply=args.apply, logs=clients["logs"])
        print(binding.canonical_bytes(result).decode(), end="")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("VEVO_REPORT_BASELINE_BLOCKED:review-private-proof", file=sys.stderr)
        raise SystemExit(1) from None
