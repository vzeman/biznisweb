"""Retain independent ECS/log readbacks before completed guard tasks expire.

Read-only by default. Optional publication creates one private audit after the
full readiness validator accepts staged evidence and fresh protected runtime.
No task, role, schedule, provider or email mutation is available here.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import hashlib
from io import BytesIO
import json
from pathlib import Path
import re
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import reporting_readiness as readiness  # noqa: E402

require = readiness.require


def read_markers(logs, definition, task_arn):
    container = definition["containerDefinitions"][0]
    options = container["logConfiguration"]["options"]
    stream = options["awslogs-stream-prefix"] + "/" + container["name"] + "/" + task_arn.rsplit("/", 1)[1]
    prefixes = ("REPORTING_GUARD_IDENTITY_OK", "CREDITNOTE_AUTOMATION_HOST_OK", "CREDITNOTE_STANDALONE_SUMMARY")
    output, token, seen = {key: [] for key in prefixes}, None, set()
    for _ in range(100):
        request = {"logGroupName": options["awslogs-group"], "logStreamName": stream, "startFromHead": True}
        if token:
            request["nextToken"] = token
        page = logs.get_log_events(**request)
        for event in page.get("events", []):
            for prefix in prefixes:
                if event["message"].startswith(prefix + " "):
                    output[prefix].append(json.loads(event["message"][len(prefix) + 1:]))
        following = page.get("nextForwardToken")
        if following == token:
            return output
        require(following and following not in seen, "observer-log-pagination-invalid")
        seen.add(following)
        token = following
    raise RuntimeError("observer-log-pagination-incomplete")


def observe(session, *, managed_key, run_id, primary, publish=False):
    readiness.require_source()
    require(re.fullmatch(r"data/roy/order-automation/creditnote-deployments/[a-f0-9]{40}/[a-f0-9]{32}\.json", managed_key),
            "observer-managed-key-invalid")
    s3, ecs, logs = (session.client(name) for name in ("s3", "ecs", "logs"))
    readiness.binding.require_private_bucket(s3)
    response = s3.get_object(Bucket=readiness.BUCKET, Key=managed_key, ExpectedBucketOwner=readiness.ACCOUNT)
    with closing(response["Body"]) as body:
        raw = body.read(2 * 1024 * 1024 + 1)
    require(len(raw) <= 2 * 1024 * 1024 and response.get("ServerSideEncryption") == "AES256", "observer-private-receipt-invalid")
    digest = hashlib.sha256(raw).hexdigest()
    managed = readiness.read_proof(s3, {"key": managed_key, "sha256": digest, "workflow_run_id": str(run_id)},
                                  "standalone_guards", readiness.gh)
    run = readiness.gh(f"repos/vzeman/biznisweb/actions/runs/{run_id}")
    final = {"workflow": {"status": run["status"], "conclusion": run["conclusion"], "headSha": run["head_sha"]},
             "schedules": {}, "task_definitions": {}, "report_task_definitions": {}, "hosts": {}}
    scheduler = session.client("scheduler")
    for name in managed["expected_schedules"]:
        final["schedules"][name] = scheduler.get_schedule(Name=name)
    for host in managed["hosts"]:
        reply = ecs.describe_tasks(cluster=readiness.CLUSTER, tasks=[host["task"]])
        require(not reply.get("failures") and len(reply.get("tasks", [])) == 1, "observer-terminal-task-unavailable")
        task = reply["tasks"][0]
        readiness.verify_task(task, host, host["task_definition"], host["image_digest"])
        definition = ecs.describe_task_definition(taskDefinition=host["task_definition"])["taskDefinition"]
        markers = read_markers(logs, definition, host["task"])
        final["hosts"][host["task"]] = {"managed_host": host, "direct_terminal_task": task,
            "markers": markers, "summaries": markers["CREDITNOTE_STANDALONE_SUMMARY"]}
        if host["kind"] == "report-probe":
            final["report_task_definitions"][host["service"].split("-")[0] + "-daily-report-email"] = definition
        else:
            final["task_definitions"][host["service"]] = definition
    guard_image = managed["candidate_definitions"]["vevo"]["definition"]["containerDefinitions"][0]["image"].rsplit("@", 1)[1]
    audit = {"schema_version": 1, "evidence_type": "independent_creditnote_automation_release",
             "verdict": "promotion_verified", "release_promoted": True, "account": readiness.ACCOUNT,
             "region": readiness.REGION, "bucket": readiness.BUCKET, "commit": managed["commit"],
             "managed_key": managed_key, "managed_sha256": digest, "managed_latest": managed,
             "run_id": int(run_id), "expected_image_digest": guard_image, "final": final}
    audit_raw = json.dumps(audit, sort_keys=True, default=str, separators=(",", ":"), allow_nan=False).encode()
    key = "data/roy/order-automation/audits/" + datetime.now(timezone.utc).strftime("%Y-%m-%d") + "/independent-creditnote-" + uuid.uuid4().hex + ".json"
    reference = {"key": key, "sha256": hashlib.sha256(audit_raw).hexdigest()}

    class StagedS3:
        def get_object(self, **request):
            if request.get("Bucket") == readiness.BUCKET and request.get("Key") == key:
                return {"Body": BytesIO(audit_raw), "ServerSideEncryption": "AES256"}
            return s3.get_object(**request)

        def __getattr__(self, name):
            return getattr(s3, name)

    class StagedSession:
        def client(self, name):
            return StagedS3() if name == "s3" else session.client(name)

    # Only the new, unpublished audit is staged locally. Every managed receipt,
    # source/run/image, marker, primary proof and current runtime read is real.
    readiness.prepare(StagedSession(), primary, reference, publish=False)
    if publish:
        s3.put_object(Bucket=readiness.BUCKET, Key=key, Body=audit_raw, ExpectedBucketOwner=readiness.ACCOUNT,
                      ServerSideEncryption="AES256", ContentType="application/json", IfNoneMatch="*")
        require(readiness.read_document(s3, reference) == json.loads(audit_raw), "observer-audit-readback-failed")
    return {"published": publish, "reference": reference if publish else None, "verified_hosts": len(final["hosts"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--managed-key", required=True)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--primary-key", required=True)
    parser.add_argument("--primary-sha256", required=True)
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    import boto3
    from botocore.config import Config
    with ExitStack() as stack:
        source, clients = boto3.Session(profile_name=args.profile, region_name=readiness.REGION), {}
        class Session:
            def client(self, name):
                if name not in clients:
                    clients[name] = stack.enter_context(closing(source.client(name, config=Config(
                        connect_timeout=5, read_timeout=20, retries={"total_max_attempts": 1}))))
                return clients[name]
        result = observe(Session(), managed_key=args.managed_key, run_id=args.run_id,
                         primary={"key": args.primary_key, "sha256": args.primary_sha256}, publish=args.publish)
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
