"""One reviewed, pre-dispatch incident recovery; read-only unless --apply.

Never a generic expired-lease unlock. The failed run, source, owner and lock
generation are fixed to the September 14 bootstrap incident. No provider access.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.deploy_vevo_report import ACCOUNT, BUCKET, Deployment, canonical, gh, require, sha  # noqa: E402
from scripts import reporting_runtime_binding as binding  # noqa: E402

RUN = "34802278958"
SOURCE = "0e367facbe15baa65cde23a40bdbf5166079922c"
OWNER = "b340ed0fa4fc4396b58096ef404ba324"
GENERATION = "4fb781d4e61643f8b7bc839b23ba94a6"


def inspect(deployment):
    s3 = deployment.s3
    require(deployment.session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "recovery-account")
    binding.require_private_bucket(s3)
    run = gh(f"repos/vzeman/biznisweb/actions/runs/{RUN}")
    require(run["id"] == int(RUN) and run["head_sha"] == SOURCE and run["head_branch"] == "main"
            and run["path"] == ".github/workflows/deploy-vevo-report.yml"
            and run["status"] == "completed" and run["conclusion"] == "failure", "recovery-run")
    start = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
    # ECS terminal metadata is still observable during this bounded recovery.
    require(timedelta(minutes=5) < datetime.now(timezone.utc) - start < timedelta(minutes=55), "recovery-window")
    lock, etag = binding.read_object(s3, binding.LOCK_KEY)
    binding.validate_lock(lock)
    require(lock["state"] == "uncertain" and lock["owner"] == OWNER
            and lock["generation"] == GENERATION, "recovery-lock-drift")
    require(binding.read_object(s3, binding.CURRENT_KEY, optional=True) is None, "recovery-current-present")
    deployment.exclusion()
    tasks = deployment.tasks()
    require(all(t.get("startedBy") != OWNER and t["lastStatus"] == "STOPPED" for t in tasks), "recovery-task-present")
    events = []
    ct = deployment.session.client("cloudtrail")
    for page in ct.get_paginator("lookup_events").paginate(
            LookupAttributes=[{"AttributeKey": "EventName", "AttributeValue": "RunTask"}],
            StartTime=start - timedelta(minutes=1), EndTime=datetime.now(timezone.utc)):
        for row in page["Events"]:
            event = json.loads(row["CloudTrailEvent"])
            request = event.get("requestParameters") or {}
            require(request.get("startedBy") != OWNER and request.get("clientToken") != OWNER, "recovery-launch-attempt")
            events.append(row["EventId"])
    deployment.original = deployment.schedule()
    deployment.original_source = deployment.definition(binding.POLICY["baseline_definition"])
    binding.validate_configuration(binding.schedule_snapshot(deployment.original),
        binding.definition_snapshot(deployment.original_source), migrated=False)
    protected = deployment.snapshot_protected()
    return {"lock": lock, "etag": etag, "run": {k: run[k] for k in (
        "id", "head_sha", "status", "conclusion", "created_at", "updated_at")},
        "schedule": binding.schedule_snapshot(deployment.original), "protected": protected,
        "tasks": binding.normalized(tasks), "cloudtrail_run_task_event_ids": sorted(events)}


def recover(deployment, *, apply=False):
    before = inspect(deployment)
    if not apply:
        return {"verified": True, "applied": False, "owner": OWNER}
    key = binding.PREFIX + "recovery/" + OWNER + "-" + uuid.uuid4().hex
    def audit(suffix, value):
        raw = canonical(binding.normalized(value))
        deployment.s3.put_object(Bucket=BUCKET, Key=key + suffix, Body=raw,
            ExpectedBucketOwner=ACCOUNT, ServerSideEncryption="AES256", IfNoneMatch="*")
        observed, _ = binding.read_object(deployment.s3, key + suffix)
        require(observed == binding.normalized(value), "recovery-audit-readback")
        return sha(raw)
    before_sha = audit("-before.json", before)
    second = inspect(deployment)
    require(all(second[k] == before[k] for k in ("lock", "etag", "schedule", "protected")), "recovery-state-drift")
    now = datetime.now(timezone.utc)
    released = {**before["lock"], "state": "released", "updated_at": now.isoformat(),
                "expires_at": (now + timedelta(minutes=20)).isoformat(), "generation": uuid.uuid4().hex}
    # Preserve incident owner; CAS exactly the independently verified generation.
    new_etag = binding.write_object(deployment.s3, binding.LOCK_KEY, released, etag=before["etag"])
    require(binding.read_object(deployment.s3, binding.LOCK_KEY) == (released, new_etag), "recovery-release-readback")
    require(binding.read_object(deployment.s3, binding.CURRENT_KEY, optional=True) is None, "recovery-current-drift")
    require(binding.schedule_snapshot(deployment.schedule()) == before["schedule"]
            and deployment.snapshot_protected() == before["protected"], "recovery-runtime-drift")
    result = {"verified": True, "applied": True, "owner": OWNER, "before_key": key + "-before.json",
              "before_sha256": before_sha, "released_lock": released, "runtime_writes": 0, "provider_writes": 0}
    audit("-complete.json", result)
    return {**result, "complete_key": key + "-complete.json"}


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.apply:
        def git(*args):
            return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()
        require(git("branch", "--show-current").startswith("codex/") and not git("status", "--porcelain")
                and git("rev-parse", "HEAD") == git("rev-parse", "@{upstream}"), "recovery-clean-pushed-source")
    import boto3
    os.environ["GITHUB_RUN_ID"] = "local-reviewed-recovery"
    deployment = Deployment(boto3.Session(profile_name=args.profile, region_name=binding.REGION), SOURCE, "", "")
    print(json.dumps(recover(deployment, apply=args.apply)))


if __name__ == "__main__":
    main()
