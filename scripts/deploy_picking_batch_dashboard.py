"""Probe/promote an exact Git image, preserving every non-image App Runner setting.

Run probe then promote from the clean merged checkout; see projects/PICKING_BATCHES.md
or projects/REPORT_DASHBOARD_RELEASE.md for the isolated --report-only mode.
Private receipts are persisted locally and in the existing encrypted artifacts bucket.
"""
import argparse
import base64
import copy
from datetime import datetime
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid
from urllib.request import Request, build_opener

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.deploy_vevo_board_image import NoRedirect, schedule_boundary, service_boundary

ACCOUNT = "919341186960"
REGION = "eu-central-1"
BUCKET = f"biznisweb-reporting-artifacts-{ACCOUNT}-{REGION}"
REPOSITORY = f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/vevo-reporting"
SERVICES = {
    "roy": ("biznisweb-roy-operations-dashboard", "ff762bb1c93148638741c62e7abb45b2"),
    "vevo": ("biznisweb-vevo-production-board", "2711a253ae014a8aaf1a37929997496d"),
}


def reporting_boundary(clients, cluster):
    """Require finished report releases and retain every scheduler configuration."""
    from scripts.reporting_image_release_lease import read_lease
    schedules = {}
    for page in clients["scheduler"].get_paginator("list_schedules").paginate(GroupName="default"):
        for row in page.get("Schedules", []):
            name = row["Name"]
            assert name not in schedules
            schedules[name] = schedule_boundary(clients["scheduler"].get_schedule(Name=name, GroupName="default"))
    leases = {}
    for project in SERVICES:
        assert project + "-daily-report-email" in schedules
        assert not clients["ecs"].list_tasks(cluster=cluster, family=f"{project}-reporting-daily", desiredStatus="RUNNING")["taskArns"], "Finish reporting tasks before dashboard release"
        lease = read_lease(clients["s3"], project, optional=True)
        assert lease is None or lease[0]["state"] == "released", "Finish reporting controllers before dashboard release"
        leases[project] = lease
    return json.loads(json.dumps({"schedules": schedules, "leases": leases}, default=str))


def candidate_container(project, image, environment, original, *, report_only, report_to_date):
    """The reporting candidate receives no provider secret and no operations command."""
    api_secrets = [] if report_only else [value for value in original["secrets"] if value["name"] == "BIZNISWEB_API_TOKEN"]
    assert report_only or len(api_secrets) == 1
    variables = dict(environment["RuntimeEnvironmentVariables"])
    variables.update({"REPORT_PROJECT": project, "REPORT_SKIP_PROJECT_ENV": "true", "AWS_REGION": REGION,
                      "EXPECTED_APP_RUNNER_SERVICE": SERVICES[project][0]})
    variables.pop("LIVE_DASHBOARD_AUTH_PASSWORD", None)
    if report_only:
        variables["EXPECTED_REPORT_TO_DATE"] = report_to_date
        variables.pop("BIZNISWEB_API_TOKEN", None)
    return {"name": "report-ui-probe" if report_only else "picking-probe", "essential": True,
            "image": image, "workingDirectory": "/app",
            "command": ["python", "scripts/dashboard_routing_host_gate.py" if report_only else "scripts/picking_batch_host_gate.py"],
            "secrets": api_secrets, "environment": [{"name": key, "value": value} for key, value in variables.items()],
            "logConfiguration": copy.deepcopy(original["logConfiguration"])}


def recover_report_dispatch(ecs, receipt, save, *, known_tasks=None, attempts=6, delay=5):
    """Never retry RunTask. Resolve and stop only this recorded candidate owner."""
    receipt["phase"] = "dispatch-cleanup-requested" if known_tasks is not None else "dispatch-uncertain"
    try:
        save(receipt)
    except Exception:
        # Dispatch ownership was already persisted before RunTask. A second
        # evidence failure must not prevent cleanup of that exact known owner.
        receipt["cleanup_receipt_write_failed"] = True
    arns = known_tasks
    if arns is None:
        for attempt in range(attempts):
            result = ecs.list_tasks(cluster=receipt["cluster"], startedBy=receipt["started_by"])
            assert not result.get("nextToken"), "Owned candidate listing incomplete"
            arns = result["taskArns"]
            if arns:
                break
            if attempt + 1 < attempts:
                time.sleep(delay)
        if not arns:
            receipt["phase"] = "dispatch-unresolved"
            save(receipt)
            return False
    tasks = []
    for arn in arns:
        result = ecs.describe_tasks(cluster=receipt["cluster"], tasks=[arn])
        assert not result.get("failures") and len(result["tasks"]) == 1
        task = result["tasks"][0]
        assert task["taskArn"] == arn and task["startedBy"] == receipt["started_by"]
        assert task["taskDefinitionArn"] == receipt["definition"] and len(task["containers"]) == 1
        container = task["containers"][0]
        assert container["image"] == REPOSITORY + "@" + receipt["digest"]
        assert container.get("imageDigest") in (None, receipt["digest"])
        if task["lastStatus"] != "STOPPED":
            ecs.stop_task(cluster=receipt["cluster"], task=arn, reason="Finite report UI candidate dispatch recovery")
            ecs.get_waiter("tasks_stopped").wait(cluster=receipt["cluster"], tasks=[arn], WaiterConfig={"Delay": 5, "MaxAttempts": 24})
            result = ecs.describe_tasks(cluster=receipt["cluster"], tasks=[arn])
            assert not result.get("failures") and len(result["tasks"]) == 1
            task = result["tasks"][0]
        assert task["taskArn"] == arn and task["lastStatus"] == "STOPPED"
        assert task["startedBy"] == receipt["started_by"] and task["taskDefinitionArn"] == receipt["definition"]
        tasks.append(task)
    ecs.deregister_task_definition(taskDefinition=receipt["definition"])
    assert ecs.describe_task_definition(taskDefinition=receipt["definition"])["taskDefinition"]["status"] == "INACTIVE"
    receipt.update({"phase": "dispatch-failed-cleaned", "recovered_tasks": tasks})
    save(receipt)
    return True


def validate_proof(task, proof, receipt):
    assert task["taskArn"] == receipt["task_arn"] and task["taskDefinitionArn"] == receipt["definition"]
    assert task["startedBy"] == receipt["started_by"] and task["lastStatus"] == "STOPPED"
    assert len(task["containers"]) == 1
    assert task["containers"][0].get("exitCode") == 0 and task["containers"][0]["imageDigest"] == receipt["digest"]
    identity = proof["identity"]
    assert identity["task_arn"] == task["taskArn"] and identity["service"] == SERVICES[receipt["project"]][0]
    assert identity["project"] == receipt["project"] and identity["path"] == "/app"
    ips = {x["value"] for a in task.get("attachments", []) for x in a.get("details", []) if x["name"] == "privateIPv4Address"}
    assert ips and set(identity["private_ips"]) == ips
    if receipt.get("mode", "picking") == "reporting":
        from scripts.dashboard_routing_host_gate import PERIODS
        assert proof["marker"] == "dashboard-project-routing-v1" and proof["mode"] == "reporting"
        assert proof["read_only_routes"] is True and proof["business_state_writes"] is False
        assert proof["foreign_redirects"] is True and proof["authentication_required"] is True
        assert identity["command"] == ["scripts/dashboard_routing_host_gate.py"]
        assert [row["period"] for row in proof["reports"]] == list(PERIODS)
        for row in proof["reports"]:
            assert row["project"] == receipt["project"] and row["date_to"] == receipt["report_to_date"]
            assert type(row["html_bytes"]) is int and row["html_bytes"] > 1000
            assert all(re.fullmatch(r"[a-f0-9]{64}", row[key]) for key in ("sha256", "payload_sha256", "shell_sha256"))
        return
    assert receipt.get("mode", "picking") == "picking"
    assert proof["marker"] == "pdf-batch-v1" and proof["batch_races_verified"] is True
    assert proof["synthetic_batch_tests"] >= 5 and proof["preview_pdf_bytes"] > 1000
    assert proof["refresh_policy"] == "independent-orders-v1" and proof["independent_refresh_tests"] >= 9


def main():
    import boto3
    from botocore.config import Config
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("probe", "promote"))
    parser.add_argument("--project", choices=tuple(SERVICES), required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--expected-current-digest", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--profile")
    parser.add_argument("--report-only", action="store_true", help="Verify reporting HTTP surfaces only; never call live operations or picking APIs")
    parser.add_argument("--report-to-date", help="Required reporting window end for --report-only")
    args = parser.parse_args()
    mode = "reporting" if args.report_only else "picking"
    if args.report_only:
        assert args.report_to_date and re.fullmatch(r"\d{4}-\d{2}-\d{2}", args.report_to_date)
        datetime.strptime(args.report_to_date, "%Y-%m-%d")
    else:
        assert args.report_to_date is None
    assert re.fullmatch(r"[0-9a-f]{40}", args.source_sha)
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", args.expected_current_digest)
    assert subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip() == args.source_sha
    assert not subprocess.check_output(["git", "status", "--porcelain"]).strip()
    main_ref = json.loads(subprocess.check_output(["gh", "api", "repos/vzeman/biznisweb/git/ref/heads/main"]))
    assert main_ref["object"]["sha"] == args.source_sha, "Merged source changed; re-review exact deployment"
    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    config = Config(retries={"max_attempts": 0}, connect_timeout=10, read_timeout=30)
    clients = {name: session.client(name, config=config) for name in ("sts", "s3", "apprunner", "ecs", "ecr", "scheduler", "logs", "ssm")}
    assert clients["sts"].get_caller_identity()["Account"] == ACCOUNT
    assert all(clients["s3"].get_public_access_block(Bucket=BUCKET)["PublicAccessBlockConfiguration"].values())
    name, service_id = SERVICES[args.project]
    arn = f"arn:aws:apprunner:{REGION}:{ACCOUNT}:service/{name}/{service_id}"
    app, ecs = clients["apprunner"], clients["ecs"]
    service = app.describe_service(ServiceArn=arn)["Service"]
    previous = REPOSITORY + "@" + args.expected_current_digest
    assert service["ServiceName"] == name and service["ServiceId"] == service_id and service["Status"] == "RUNNING"
    assert service["SourceConfiguration"]["AutoDeploymentsEnabled"] is False
    repository = service["SourceConfiguration"]["ImageRepository"]
    assert repository["ImageIdentifier"] == previous
    environment = repository["ImageConfiguration"]
    assert environment["Port"] == "8080" and environment["StartCommand"] == "python live_dashboard_server.py --host 0.0.0.0 --port 8080"
    assert environment["RuntimeEnvironmentVariables"]["REPORT_PROJECT"] == args.project
    digest = clients["ecr"].describe_images(repositoryName="vevo-reporting", imageIds=[{"imageTag": "git-" + args.source_sha}])["imageDetails"][0]["imageDigest"]
    image = REPOSITORY + "@" + digest
    print(json.dumps({"phase": args.phase, "mode": mode, "service": arn, "instance_id": "N/A managed App Runner",
                      "ip": "N/A managed; candidate private IP verified separately", "path": "/app",
                      "previous": previous, "candidate": image}), flush=True)
    schedule_name = args.project + "-daily-report-email"
    schedule = clients["scheduler"].get_schedule(Name=schedule_name)

    def save(receipt):
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        if args.report_only:
            receipt["receipt_revision"] = receipt.get("receipt_revision", 0) + 1
        body = json.dumps(receipt, indent=2, default=str).encode()
        args.receipt.write_bytes(body)
        key = f"data/{args.project}/picking-batch-deployments/{args.source_sha}/{args.receipt.name}"
        if args.report_only:
            key = f"data/{args.project}/report-dashboard-deployments/{args.source_sha}/{receipt['started_by']}/{receipt['receipt_revision']:04d}-{receipt['phase']}.json"
        clients["s3"].put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json",
                                 ServerSideEncryption="AES256", ExpectedBucketOwner=ACCOUNT,
                                 **({"IfNoneMatch": "*"} if args.report_only else {}))
        if args.report_only:
            saved = clients["s3"].get_object(Bucket=BUCKET, Key=key, ExpectedBucketOwner=ACCOUNT)
            try:
                assert saved.get("ServerSideEncryption") == "AES256" and saved["Body"].read(len(body) + 1) == body
            finally:
                saved["Body"].close()

    def read_task(receipt):
        result = ecs.describe_tasks(cluster=receipt["cluster"], tasks=[receipt["task_arn"]])
        assert not result.get("failures") and len(result["tasks"]) == 1
        return result["tasks"][0]

    def read_proof(receipt):
        messages, token = [], None
        for _ in range(100):
            kwargs = {"logGroupName": receipt["log_group"], "logStreamName": receipt["log_stream"], "startFromHead": True}
            if token:
                kwargs["nextToken"] = token
            result = clients["logs"].get_log_events(**kwargs)
            messages.extend(row["message"] for row in result["events"])
            next_token = result["nextForwardToken"]
            if next_token == token:
                break
            token = next_token
            assert len(messages) < 2000
        else:
            raise RuntimeError("Candidate log pagination incomplete")
        marker = "DASHBOARD_ROUTING_HOST_OK " if args.report_only else "PICKING_BATCH_HOST_OK "
        closed = "DASHBOARD_ROUTING_LOCAL_SERVER_CLOSED" if args.report_only else "PICKING_BATCH_HOST_CLOSED"
        proofs = [json.loads(line.split(" ", 1)[1]) for line in messages if line.startswith(marker)]
        assert len(proofs) == 1 and messages.count(closed) == 1, "Candidate proof incomplete"
        return proofs[0]

    if args.phase == "probe":
        assert not args.receipt.exists(), "Existing receipt must be reconciled before retry"
        target = schedule["Target"]
        kind = "report-ui" if args.report_only else "picking"
        family = f"{args.project}-{kind}-probe-{args.source_sha[:12]}"
        assert not ecs.list_tasks(cluster=target["Arn"], family=family, desiredStatus="RUNNING")["taskArns"], "Candidate already running"
        protected = reporting_boundary(clients, target["Arn"]) if args.report_only else None
        source = ecs.describe_task_definition(taskDefinition=target["EcsParameters"]["TaskDefinitionArn"])["taskDefinition"]
        assert source["family"] == args.project + "-reporting-daily" and len(source["containerDefinitions"]) == 1
        original = source["containerDefinitions"][0]
        container = candidate_container(args.project, image, environment, original,
                                        report_only=args.report_only, report_to_date=args.report_to_date)
        receipt = {"project": args.project, "source_sha": args.source_sha, "digest": digest, "previous": previous,
                   "baseline": service_boundary(service), "report_schedule": schedule_boundary(schedule),
                   "started_by": f"{kind}-{args.project}-{uuid.uuid4().hex[:16]}", "cluster": target["Arn"], "phase": "registering",
                   "mode": mode, "report_to_date": args.report_to_date}
        if args.report_only:
            receipt["protected_reporting"] = protected
        save(receipt)
        definition = ecs.register_task_definition(family=family,
            executionRoleArn=source["executionRoleArn"], taskRoleArn=source["taskRoleArn"], networkMode="awsvpc",
            requiresCompatibilities=["FARGATE"], cpu=service["InstanceConfiguration"]["Cpu"],
            memory=service["InstanceConfiguration"]["Memory"], containerDefinitions=[container])["taskDefinition"]
        receipt["definition"] = definition["taskDefinitionArn"]
        save(receipt)
        raw = target["EcsParameters"]["NetworkConfiguration"]["awsvpcConfiguration"]
        network = {"awsvpcConfiguration": {key[0].lower() + key[1:]: value for key, value in raw.items()}}
        if args.report_only:
            receipt["phase"] = "dispatch-requested"
            save(receipt)
        try:
            result = ecs.run_task(cluster=receipt["cluster"], taskDefinition=receipt["definition"], launchType="FARGATE",
                                  networkConfiguration=network, startedBy=receipt["started_by"], clientToken=receipt["started_by"], count=1)
        except Exception:
            if args.report_only:
                recover_report_dispatch(ecs, receipt, save)
            raise
        if args.report_only and (result.get("failures") or len(result.get("tasks", [])) != 1):
            recover_report_dispatch(ecs, receipt, save, known_tasks=[task["taskArn"] for task in result.get("tasks", [])])
        assert not result.get("failures") and len(result["tasks"]) == 1
        receipt["task_arn"] = result["tasks"][0]["taskArn"]
        options = container["logConfiguration"]["options"]
        receipt.update({"log_group": options["awslogs-group"],
                        "log_stream": options["awslogs-stream-prefix"] + "/" + container["name"] + "/" + receipt["task_arn"].rsplit("/", 1)[1],
                        "phase": "probing"})
        try:
            save(receipt)
            print(("REPORT_UI_CANDIDATE " if args.report_only else "PICKING_CANDIDATE ") + receipt["task_arn"], flush=True)
            for _ in range(180):
                task = read_task(receipt)
                if task["lastStatus"] == "STOPPED":
                    break
                time.sleep(5)
        finally:
            task = read_task(receipt)
            if task["lastStatus"] != "STOPPED":
                assert task["startedBy"] == receipt["started_by"] and task["taskDefinitionArn"] == receipt["definition"]
                ecs.stop_task(cluster=receipt["cluster"], task=receipt["task_arn"], reason="Finite dashboard candidate cleanup")
                ecs.get_waiter("tasks_stopped").wait(cluster=receipt["cluster"], tasks=[receipt["task_arn"]], WaiterConfig={"Delay": 5, "MaxAttempts": 24})
                task = read_task(receipt)
            assert task["lastStatus"] == "STOPPED"
            ecs.deregister_task_definition(taskDefinition=receipt["definition"])
            assert ecs.describe_task_definition(taskDefinition=receipt["definition"])["taskDefinition"]["status"] == "INACTIVE"
            receipt.update({"task": task, "phase": "candidate-stopped"})
            save(receipt)
        proof = read_proof(receipt)
        validate_proof(task, proof, receipt)
        if args.report_only:
            assert reporting_boundary(clients, receipt["cluster"]) == receipt["protected_reporting"], "Protected reporting state changed"
        receipt.update({"proof": proof, "phase": "verified", "verified_at": time.time()})
        save(receipt)
        print(("REPORT_UI_CANDIDATE_VERIFIED " if args.report_only else "PICKING_CANDIDATE_VERIFIED ") + json.dumps(proof), flush=True)
        return

    receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
    assert receipt.get("mode", "picking") == mode and receipt.get("report_to_date") == args.report_to_date, "Candidate mode or reporting window changed"
    assert receipt["phase"] == "verified" and 0 <= time.time() - receipt["verified_at"] < 1800
    assert receipt["source_sha"] == args.source_sha and receipt["digest"] == digest and receipt["previous"] == previous
    assert receipt["project"] == args.project and service_boundary(service) == receipt["baseline"]
    assert schedule_boundary(schedule) == receipt["report_schedule"]
    if args.report_only:
        assert reporting_boundary(clients, receipt["cluster"]) == receipt["protected_reporting"], "Protected reporting state changed"
    proof = read_proof(receipt)
    assert proof == receipt["proof"]
    validate_proof(read_task(receipt), proof, receipt)
    source = copy.deepcopy(service["SourceConfiguration"])
    source["ImageRepository"]["ImageIdentifier"] = image
    receipt["phase"] = "promoting"
    save(receipt)
    receipt["operation_id"] = app.update_service(ServiceArn=arn, SourceConfiguration=source)["OperationId"]
    save(receipt)
    print(("REPORT_UI_PROMOTION " if args.report_only else "PICKING_PROMOTION ") + receipt["operation_id"], flush=True)
    status = "PENDING"
    for _ in range(180):
        matches = [row for row in app.list_operations(ServiceArn=arn)["OperationSummaryList"] if row["Id"] == receipt["operation_id"]]
        assert len(matches) == 1
        status = matches[0]["Status"]
        if status not in {"PENDING", "IN_PROGRESS", "ROLLBACK_IN_PROGRESS"}:
            break
        time.sleep(5)
    assert status == "SUCCEEDED", "Inspect saved operation; do not repeat promotion"
    for _ in range(24):
        deployed = app.describe_service(ServiceArn=arn)["Service"]
        if deployed["Status"] == "RUNNING":
            break
        time.sleep(5)
    assert deployed["Status"] == "RUNNING"
    expected = copy.deepcopy(receipt["baseline"])
    expected["SourceConfiguration"] = source
    assert service_boundary(deployed) == expected, "Non-image service drift"
    password = clients["ssm"].get_parameter(Name=environment["RuntimeEnvironmentSecrets"]["LIVE_DASHBOARD_AUTH_PASSWORD"], WithDecryption=True)["Parameter"]["Value"]
    auth = base64.b64encode((environment["RuntimeEnvironmentVariables"]["LIVE_DASHBOARD_AUTH_USER"] + ":" + password).encode()).decode()
    origin = "https://" + deployed["ServiceUrl"]
    opener = build_opener(NoRedirect())

    def live(path):
        with opener.open(Request(origin + path, headers={"Authorization": "Basic " + auth}), timeout=240) as response:
            assert response.geturl() == origin + path
            return response.read()

    if args.report_only:
        from scripts.dashboard_routing_host_gate import verify_report_http
        results = verify_report_http(args.project, live, expected_to_date=args.report_to_date)
        assert results == receipt["proof"]["reports"], "Reporting artifacts or shell changed since candidate verification"
        receipt["live_reports"] = results
    else:
        assert json.loads(live("/health"))["ok"] is True
        html = live(f"/production/{args.project}")
        assert b'data-picking-policy="pdf-batch-v1"' in html
        assert b'data-refresh-policy="independent-orders-v1"' in html
        data = json.loads(live(f"/api/operations/{args.project}/live?refresh=0"))
        assert data["project"] == args.project
        assert live(f"/api/operations/{args.project}/picking-lists.pdf?preview=1&refresh=0").startswith(b"%PDF-")
        receipt["live_orders"] = len(data["orders"]["orders"])
    assert schedule_boundary(clients["scheduler"].get_schedule(Name=schedule_name)) == receipt["report_schedule"]
    if args.report_only:
        assert reporting_boundary(clients, receipt["cluster"]) == receipt["protected_reporting"], "Protected reporting state changed"
    receipt["phase"] = "deployed"
    save(receipt)
    print(("REPORT_UI_DEPLOYED " if args.report_only else "PICKING_DASHBOARD_DEPLOYED ") + json.dumps({"project": args.project, "image": image, "operation_id": receipt["operation_id"]}), flush=True)


if __name__ == "__main__":
    main()
