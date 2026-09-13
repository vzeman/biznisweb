"""Semantic order/guard release proofs and reproducible private report readiness.

No provider client, task launch or runtime mutation. The only optional write is a
new encrypted readiness artifact, built from successful managed runs and current
AWS readbacks. The report deployer uses the same validator on every input.
"""
from __future__ import annotations

import argparse
import copy
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import reporting_runtime_binding as binding  # noqa: E402
from scripts.deploy_order_automations import SCHEDULES, SERVICES  # noqa: E402

ACCOUNT, REGION, BUCKET = binding.ACCOUNT, binding.REGION, binding.BUCKET
CLUSTER = binding.POLICY["cluster"]
PHASE = "order-and-standalone-guards-independently-verified"
REFERENCES = {"primary_release": ("deployments", ".github/workflows/deploy-order-automations.yml"),
              "standalone_guards": ("creditnote-deployments", ".github/workflows/deploy-creditnote-automations.yml")}
require = binding.require
REPORT_PINS = {"roy": (71, "sha256:9ff4738f998e3d80e7b76dc543f11bc36413d9d016e72b4a78eb3411433bc541"),
               "vevo": (33, binding.POLICY["baseline_image_digest"])}


def gh(path):
    return binding.decode_json(subprocess.check_output(["gh", "api", path], cwd=ROOT, timeout=40))


def read_document(s3, reference):
    require(binding.hex_value(reference["sha256"]), "readiness-reference-hash-format")
    response = s3.get_object(Bucket=BUCKET, Key=reference["key"], ExpectedBucketOwner=ACCOUNT)
    with closing(response["Body"]) as body:
        require(response.get("ServerSideEncryption") == "AES256", "readiness-proof-encryption")
        raw = body.read(2 * 1024 * 1024 + 1)
    require(len(raw) <= 2 * 1024 * 1024 and hashlib.sha256(raw).hexdigest() == reference["sha256"], "readiness-proof-hash")
    # Deployment snapshots can exceed the small runtime-pointer decoder limit.
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, "readiness-proof-duplicate-key")
            result[key] = value
        return result
    def constant(_):
        raise binding.BindingError("readiness-proof-nonfinite")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def read_proof(s3, reference, kind, fetch_run):
    binding.exact_keys(reference, {"key", "sha256", "workflow_run_id"}, "readiness-reference-schema")
    prefix, workflow = REFERENCES[kind]
    match = re.fullmatch(r"data/roy/order-automation/" + prefix + r"/([a-f0-9]{40})/[a-f0-9]{32}\.json", reference["key"])
    require(match and re.fullmatch(r"[1-9][0-9]*", reference["workflow_run_id"]), "readiness-reference-identity")
    proof = read_document(s3, reference)
    require(isinstance(proof, dict) and type(proof.get("schema")) is int and proof["schema"] == 1
            and proof.get("phase") == "promotion-readback-verified" and proof.get("commit") == match[1]
            and not proof.get("rollback") and not proof.get("rollback_failed_schedules")
            and not proof.get("reports_retained_paused"), "readiness-release-not-promoted")
    run = fetch_run(f"repos/vzeman/biznisweb/actions/runs/{reference['workflow_run_id']}")
    require(str(run.get("id")) == reference["workflow_run_id"] and run.get("head_sha") == proof["commit"]
            and run.get("head_branch") == "main" and run.get("repository", {}).get("full_name") == "vzeman/biznisweb"
            and run.get("path") == workflow and run.get("event") == "workflow_dispatch"
            and run.get("status") == "completed" and run.get("conclusion") == "success", "readiness-managed-run-not-successful")
    require(binding.stamp(run["run_started_at"]) <= binding.stamp(proof["created_at"]) <= binding.stamp(run["updated_at"]),
            "readiness-receipt-outside-managed-run")
    return proof


def read_independent(s3, reference, kind, fetch_run):
    binding.exact_keys(reference, {"key", "sha256"}, "readiness-independent-reference-schema")
    require(re.fullmatch(r"data/roy/order-automation/audits/\d{4}-\d{2}-\d{2}/[A-Za-z0-9_-]+\.json", reference["key"]),
            "readiness-independent-reference-scope")
    independent = read_document(s3, reference)
    expected = "independent_order_automation_release" if kind == "primary_release" else "independent_creditnote_automation_release"
    require(type(independent.get("schema_version")) is int and independent["schema_version"] == 1
            and independent.get("evidence_type") == expected and independent.get("verdict") == "promotion_verified"
            and independent.get("release_promoted") is True
            and independent.get("account") == ACCOUNT and independent.get("region") == REGION and independent.get("bucket") == BUCKET,
            "readiness-independent-verdict")
    managed = read_proof(s3, {"key": independent["managed_key"], "sha256": independent["managed_sha256"],
        "workflow_run_id": str(independent["run_id"])}, kind, fetch_run)
    require(independent.get("commit") == managed["commit"] and independent.get("managed_latest") == managed,
            "readiness-independent-managed-crosslink")
    final = independent.get("final", {})
    workflow = final.get("workflow", {})
    require(workflow.get("status") == "completed" and workflow.get("conclusion") == "success"
            and workflow.get("headSha") == managed["commit"], "readiness-independent-workflow")
    schedule_field = "desired_schedules" if kind == "primary_release" else "expected_schedules"
    require({k: binding.schedule_snapshot(v) for k, v in final.get("schedules", {}).items()} ==
            {k: binding.schedule_snapshot(v) for k, v in managed.get(schedule_field, {}).items()}, "readiness-independent-schedules")
    require(isinstance(final.get("hosts"), dict) and set(final["hosts"]) == {h["task"] for h in managed.get("hosts", [])},
            "readiness-independent-host-scope")
    for host in managed.get("hosts", []):
        actual = final["hosts"][host["task"]]
        require(actual.get("managed_host") == host, "readiness-independent-managed-host")
        verify_task(actual.get("direct_terminal_task", {}), host, host["task_definition"], host["image_digest"])
        if kind == "primary_release":
            require(actual.get("markers") == [host.get("marker")]
                    and actual["direct_terminal_task"].get("startedBy") == "order-automation-host-gate", "readiness-independent-host-marker")
        else:
            require(actual["direct_terminal_task"].get("startedBy") == "creditnote-automation-migration", "readiness-independent-guard-owner")
            markers = actual.get("markers", {})
            require(isinstance(markers, dict), "readiness-independent-guard-markers")
            project, host_kind = host["service"].split("-")[0], host["kind"]
            if host_kind == "guard-probe":
                require(markers.get("CREDITNOTE_AUTOMATION_HOST_OK") == [{"marker": "CREDITNOTE_AUTOMATION_HOST_OK",
                    "project": project, "path": "/app", "dry_run": True}], "readiness-independent-guard-localhost")
            if host_kind in {"guard-probe", "guard-live"}:
                require(actual.get("summaries") == [host.get("summary")], "readiness-independent-live-summary")
            if host_kind == "report-probe":
                rows = markers.get("REPORTING_GUARD_IDENTITY_OK", [])
                require(len(rows) == 1 and rows[0].get("marker") == "REPORTING_GUARD_IDENTITY_OK"
                        and rows[0].get("project") == project and rows[0].get("task") == host["task"]
                        and rows[0].get("private_ip") == host["private_ip"] and rows[0].get("path") == "/app"
                        and rows[0].get("inline_guard_skipped") is True and rows[0].get("business_runner_started") is False,
                        "readiness-independent-report-localhost")
    return managed, independent


def verify_host(ecs, host, definition_arn, digest):
    require(host.get("task_definition") == definition_arn and host.get("image_digest") == digest
            and host.get("path") == "/app" and host.get("instance_id") == "N/A:FARGATE"
            and type(host.get("exit_code")) is int and host["exit_code"] == 0,
            "readiness-host-proof-incomplete")
    task_id = host.get("task", "")
    require(re.fullmatch(re.escape(CLUSTER.replace(":cluster/", ":task/")) + r"/[a-f0-9]{32}", task_id), "readiness-host-scope")
    result = ecs.describe_tasks(cluster=CLUSTER, tasks=[task_id])
    require(not result.get("failures") and len(result.get("tasks", [])) == 1, "readiness-host-no-longer-observable")
    verify_task(result["tasks"][0], host, definition_arn, digest)


def verify_task(task, host, definition_arn, digest):
    containers = task.get("containers", [])
    require(task.get("taskArn") == host.get("task") and task.get("taskDefinitionArn") == definition_arn
            and task.get("clusterArn") == CLUSTER and task.get("launchType") == "FARGATE"
            and task.get("lastStatus") == "STOPPED" and len(containers) == 1, "readiness-live-host-drift")
    container = containers[0]
    require(container.get("name") == "reporting" and type(container.get("exitCode")) is int and container["exitCode"] == 0
            and container.get("imageDigest") == digest
            and [item.get("privateIpv4Address") for item in container.get("networkInterfaces", [])] == [host.get("private_ip")],
            "readiness-live-container-drift")


def report_skip_only(original):
    result = binding.schedule_snapshot(original)
    payload = json.loads(result["Target"].get("Input") or "{}")
    require(isinstance(payload, dict), "readiness-original-report-input")
    rows = payload.setdefault("containerOverrides", [])
    require(isinstance(rows, list) and all(isinstance(row, dict) for row in rows), "readiness-report-overrides")
    matches = [row for row in rows if row.get("name") == "reporting"]
    require(len(matches) <= 1, "readiness-report-duplicate-container")
    if not matches:
        matches = [{"name": "reporting"}]
        rows.extend(matches)
    environment = matches[0].setdefault("environment", [])
    require(isinstance(environment, list), "readiness-report-environment")
    found = [row for row in environment if row.get("name") == "REPORT_SKIP_CREDITNOTE_STORNO_GUARD"]
    require(len(found) <= 1, "readiness-report-duplicate-skip")
    if found:
        found[0]["value"] = "true"
    else:
        environment.append({"name": "REPORT_SKIP_CREDITNOTE_STORNO_GUARD", "value": "true"})
    result["Target"]["Input"] = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return result


def verify_definition(protected, arn, expected):
    require(arn.startswith(f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/") and arn in protected["definitions"],
            "readiness-definition-unbound")
    actual = copy.deepcopy(protected["definitions"][arn])
    actual.pop("taskDefinitionArn", None)
    require(actual == binding.normalized(expected), "readiness-definition-drift")
    containers = actual.get("containerDefinitions", [])
    require(len(containers) == 1 and containers[0].get("name") == "reporting", "readiness-container-count")
    image = containers[0].get("image", "")
    require(re.fullmatch(rf"{ACCOUNT}\.dkr\.ecr\.{REGION}\.amazonaws\.com/vevo-reporting@sha256:[a-f0-9]{{64}}", image),
            "readiness-immutable-image-required")
    return image.rsplit("@", 1)[1]


def validate_readiness(receipt, *, s3, ecs, ecr, protected, vevo_schedule, fetch_run=gh, now=None):
    binding.require_private_bucket(s3)
    binding.exact_keys(receipt, {"schema_version", "phase", "account", "region", "verified_at", "protected",
                                 "primary_release", "standalone_guards"}, "readiness-schema")
    age = ((now or datetime.now(timezone.utc)) - binding.stamp(receipt["verified_at"])).total_seconds()
    require(type(receipt["schema_version"]) is int and receipt["schema_version"] == 1 and receipt["phase"] == PHASE
            and receipt["account"] == ACCOUNT and receipt["region"] == REGION and 0 <= age <= 86400
            and receipt["protected"] == protected, "readiness-stale-or-runtime-drift")
    primary, primary_audit = read_independent(s3, receipt["primary_release"], "primary_release", fetch_run)
    require(set(primary.get("desired_schedules", {})) == set(SCHEDULES)
            and set(primary.get("candidate_task_definitions", {})) == set(SERVICES), "readiness-primary-scope")
    for field in ("candidate_drain", "drain"):
        value = primary.get(field, {})
        require(type(value.get("quiet_seconds")) is int and value["quiet_seconds"] >= 120
                and type(value.get("unfinished_tasks")) is int and value["unfinished_tasks"] == 0,
                "readiness-primary-drain-incomplete")
    hosts = primary.get("hosts", [])
    require(isinstance(hosts, list) and len(hosts) == 3 and {row.get("service") for row in hosts} == set(SERVICES),
            "readiness-primary-host-scope")
    for name, family in SCHEDULES.items():
        desired = binding.schedule_snapshot(primary["desired_schedules"][name])
        require(desired.get("Name") == name and desired.get("State") == "ENABLED" and not desired["Target"].get("Input")
                and desired == protected["schedules"][name], "readiness-primary-current-pins")
        arn = desired["Target"]["EcsParameters"]["TaskDefinitionArn"]
        digest = verify_definition(protected, arn, primary["candidate_task_definitions"][family])
        require(digest == primary.get("image_digest"), "readiness-primary-image")
        require(binding.definition_snapshot(primary_audit["final"]["task_definitions"][family]) == protected["definitions"][arn],
                "readiness-primary-independent-definition")
    require(primary_audit.get("expected_image_digest") == primary["image_digest"], "readiness-independent-primary-image")
    for field in ("candidate_drain", "drain"):
        require(primary_audit["final"].get(field) == primary[field], "readiness-independent-drain")
    for host in hosts:
        project, kind, schedule_name = SERVICES[host["service"]]
        marker = {"marker": "ORDER_AUTOMATION_HOST_OK", "project": project, "kind": kind, "path": "/app", "dry_run": True}
        if kind == "invoice":
            marker["full_backlog"] = True
        require(host.get("marker") == marker, "readiness-primary-localhost-marker")
        verify_host(ecs, host, protected["schedules"][schedule_name]["Target"]["EcsParameters"]["TaskDefinitionArn"], primary["image_digest"])
    guards, guard_audit = read_independent(s3, receipt["standalone_guards"], "standalone_guards", fetch_run)
    expected_names = set(SCHEDULES) | {f"{p}-{suffix}" for p in ("roy", "vevo") for suffix in ("daily-report-email", "creditnote-storno-guard")}
    require(set(guards.get("expected_schedules", {})) == expected_names
            and set(guards.get("candidate_definitions", {})) == {"roy", "vevo"}, "readiness-guard-scope")
    current_schedules = {**protected["schedules"], "vevo-daily-report-email": binding.schedule_snapshot(vevo_schedule)}
    for name in expected_names:
        expected = binding.schedule_snapshot(guards["expected_schedules"][name])
        require(expected.get("Name") == name and expected.get("State") == "ENABLED" and expected == current_schedules[name],
                "readiness-guard-current-pins")
    for project, (revision, image) in REPORT_PINS.items():
        name = f"{project}-daily-report-email"
        original = guards["original_schedules"][name]
        require(binding.schedule_snapshot(primary_audit["final"]["report_schedules"][name]) == binding.schedule_snapshot(original),
                "readiness-primary-to-guard-report-origin")
        expected = report_skip_only(original)
        current = copy.deepcopy(current_schedules[name])
        # JSON whitespace is not an effective command/environment difference.
        current["Target"]["Input"] = json.dumps(json.loads(current["Target"].get("Input") or "{}"), sort_keys=True, separators=(",", ":"))
        require(current == expected, "readiness-report-delta-not-only-inline-skip")
        arn = current["Target"]["EcsParameters"]["TaskDefinitionArn"]
        require(arn == f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{project}-reporting-daily:{revision}", "readiness-report-pin")
        definition = guard_audit["final"]["report_task_definitions"][name]
        require(definition["containerDefinitions"][0]["image"].endswith("@" + image)
                and binding.definition_snapshot(primary_audit["final"]["report_task_definitions"][name]) == binding.definition_snapshot(definition)
                and binding.definition_snapshot(ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]) == binding.definition_snapshot(definition),
                "readiness-report-definition-preservation")
    hosts = guards.get("hosts", [])
    required_pairs = {(f"{project}-reporting-daily" if kind == "report-probe" else f"{project}-creditnote-storno-guard", kind)
                      for project in ("roy", "vevo") for kind in ("report-probe", "guard-probe", "guard-live")}
    require(isinstance(hosts, list) and len(hosts) == 6 and {(row.get("service"), row.get("kind")) for row in hosts} == required_pairs,
            "readiness-guard-six-hosts-required")
    guard_digests = set()
    for host in hosts:
        project, kind = host["service"].split("-")[0], host["kind"]
        if kind == "report-probe":
            schedule = current_schedules[f"{project}-daily-report-email"]
            arn = schedule["Target"]["EcsParameters"]["TaskDefinitionArn"]
            definition = ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
            digest = definition["containerDefinitions"][0]["image"].rsplit("@", 1)[-1]
        else:
            candidate = guards["candidate_definitions"][project]
            require(set(candidate) == {"arn", "definition"}, "readiness-guard-candidate-schema")
            arn = current_schedules[f"{project}-creditnote-storno-guard"]["Target"]["EcsParameters"]["TaskDefinitionArn"]
            require(arn == candidate["arn"], "readiness-guard-definition-pin")
            digest = verify_definition(protected, arn, candidate["definition"])
            require(binding.definition_snapshot(guard_audit["final"]["task_definitions"][host["service"]]) == protected["definitions"][arn],
                    "readiness-guard-independent-definition")
            guard_digests.add(digest)
            summary = host.get("summary", {})
            require(summary.get("ok") is True and summary.get("project") == project and summary.get("enabled") is True
                    and summary.get("dry_run") is (kind == "guard-probe") and summary.get("creditnote_scan_complete") is True
                    and summary.get("skipped_locked") is False, "readiness-guard-live-completion")
            require(all(type(summary.get(k)) is int and summary[k] == 0 for k in
                        ("failed_orders", "review_required_orders", "audit_error_orders")), "readiness-guard-needs-review")
            require(type(summary.get("updated_orders")) is int and summary["updated_orders"] >= 0
                    and (kind != "guard-probe" or summary["updated_orders"] == 0), "readiness-guard-dry-write")
        verify_host(ecs, host, arn, digest)
    require(len(guard_digests) == 1, "readiness-guard-image-split")
    require(guard_audit.get("expected_image_digest") == next(iter(guard_digests)), "readiness-independent-guard-image")
    for proof, digest in ((primary, primary["image_digest"]), (guards, next(iter(guard_digests)))):
        images = ecr.describe_images(repositoryName="vevo-reporting", imageIds=[{"imageTag": "git-" + proof["commit"]}])["imageDetails"]
        require(len(images) == 1 and images[0].get("imageDigest") == digest, "readiness-source-image-drift")
    return receipt


def require_source():
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, timeout=40).strip()
    require(not git("status", "--porcelain") and git("rev-parse", "HEAD") == git("rev-parse", "@{upstream}"), "readiness-source-not-clean-pushed")
    branch = git("branch", "--show-current")
    require(branch == "main" or branch.startswith("codex/"), "readiness-source-branch")
    require(git("ls-remote", "origin", "refs/heads/" + branch).split() == [git("rev-parse", "HEAD"), "refs/heads/" + branch],
            "readiness-source-remote-drift")
    require(git("ls-files", "--error-unmatch", "scripts/reporting_readiness.py"), "readiness-source-untracked")


def prepare(session, primary, guards, *, publish=False):
    from scripts.deploy_vevo_report import Deployment
    require_source()
    require(session.client("sts").get_caller_identity()["Account"] == ACCOUNT, "readiness-account")
    deployment = Deployment(session, "0" * 40, "", "")  # Snapshot methods only; no lease/task/update entrypoint.
    binding.require_private_bucket(deployment.s3)
    binding.check_migration(deployment.s3)
    deployment.original = deployment.schedule()
    deployment.original_source = deployment.definition(deployment.original["Target"]["EcsParameters"]["TaskDefinitionArn"])
    protected = deployment.snapshot_protected()
    receipt = {"schema_version": 1, "phase": PHASE, "account": ACCOUNT, "region": REGION,
               "verified_at": datetime.now(timezone.utc).isoformat(), "protected": protected,
               "primary_release": primary, "standalone_guards": guards}
    validate_readiness(receipt, s3=deployment.s3, ecs=deployment.ecs, ecr=session.client("ecr"), protected=protected,
                       vevo_schedule=deployment.original)
    require(protected == deployment.snapshot_protected() and binding.schedule_snapshot(deployment.original) ==
            binding.schedule_snapshot(deployment.schedule()), "readiness-snapshot-changed")
    binding.check_migration(deployment.s3)
    require_source()
    raw = binding.canonical_bytes(receipt)
    key = binding.PREFIX + "readiness/" + uuid.uuid4().hex + ".json"
    digest = hashlib.sha256(raw).hexdigest()
    if publish:
        binding.write_object(deployment.s3, key, receipt)
        actual, _ = binding.read_object(deployment.s3, key)
        require(binding.canonical_bytes(actual) == raw, "readiness-publication-readback")
    return {"published": publish, "key": key if publish else None, "sha256": digest,
            "verified_primary_hosts": 3, "verified_guard_hosts": 6, "provider_writes": 0, "runtime_writes": 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="codex")
    parser.add_argument("--publish", action="store_true")
    for kind in ("primary", "guards"):
        for field in ("key", "sha256"):
            parser.add_argument(f"--{kind}-{field}", required=True)
    args = parser.parse_args()
    import boto3
    from botocore.config import Config
    with ExitStack() as stack:
        source = boto3.Session(profile_name=args.profile, region_name=REGION)
        clients = {}
        class Session:
            def client(self, name):
                if name not in clients:
                    clients[name] = stack.enter_context(closing(source.client(name, config=Config(
                        connect_timeout=5, read_timeout=20, retries={"total_max_attempts": 1}))))
                return clients[name]
        refs = [{"key": getattr(args, kind + "_key"), "sha256": getattr(args, kind + "_sha256")} for kind in ("primary", "guards")]
        print(json.dumps(prepare(Session(), *refs, publish=args.publish), sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("REPORT_READINESS_BLOCKED:" + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1) from None
