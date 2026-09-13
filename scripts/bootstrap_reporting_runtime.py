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


def verify_host(ecs, record):
    proof = record["candidate_proof"]
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


def bootstrap(s3, scheduler, ecs, record, *, apply):
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
        verify_host(ecs, record)
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
        binding.require(binding.load_current_binding(s3) == published, "bootstrap-final-readback")
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
                   for name in ("sts", "s3", "scheduler", "ecs")}
        binding.require(clients["sts"].get_caller_identity()["Account"] == binding.ACCOUNT, "bootstrap-account")
        result = bootstrap(clients["s3"], clients["scheduler"], clients["ecs"], record, apply=args.apply)
        print(binding.canonical_bytes(result).decode(), end="")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("VEVO_REPORT_BASELINE_BLOCKED:review-private-proof", file=sys.stderr)
        raise SystemExit(1) from None
