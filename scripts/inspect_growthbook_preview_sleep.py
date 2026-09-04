#!/usr/bin/env python3
"""Read-only Preview suspension preflight; never read event data or emit AWS payloads."""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

ACCOUNT = "919341186960"
REGION = "eu-central-1"
STACK = "vevo-growthbook-preview"
RECONCILIATION = "vevo-growthbook-reconciliation-preview"
SERVICE = "vevo-growthbook-collector-preview"
SOURCE_SCHEDULE = "vevo-daily-report-email"


def require(value, message):
    if not value:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str) + "\n"


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def parameters(stack):
    return {item["ParameterKey"]: item["ParameterValue"] for item in stack["Parameters"]}


def outputs(stack):
    return {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}


def stack_read(cf, name):
    rows = cf.describe_stacks(StackName=name)["Stacks"]
    require(len(rows) == 1 and rows[0]["StackName"] == name, "stack identity drift")
    require(rows[0]["StackStatus"] in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}, "stack not stable")
    return rows[0]


def resources_read(cf, name):
    rows = []
    for page in cf.get_paginator("list_stack_resources").paginate(StackName=name):
        rows.extend(page["StackResourceSummaries"])
    return sorted((row["LogicalResourceId"], row["ResourceType"], row["PhysicalResourceId"]) for row in rows)


def stack_fingerprint(cf, stack):
    return digest({"parameters": parameters(stack), "outputs": outputs(stack),
                   "template": cf.get_template(StackName=stack["StackName"], TemplateStage="Original")["TemplateBody"],
                   "resources": resources_read(cf, stack["StackName"])})


def schedule_fingerprint(schedule):
    return digest({key: value for key, value in schedule.items()
                   if key not in {"ResponseMetadata", "CreationDate", "LastModificationDate"}})


def validate_command(container):
    # ECS can serialize an unset image CMD override as an empty list or null.
    # The digest-pinned collector Dockerfile defines this exact default command.
    command = container.get("command")
    require(command in (None, [], ["python", "-m", "growthbook_collector.server"]), "runtime command override drift")
    require(container.get("entryPoint") in (None, []), "runtime entrypoint override drift")
    return "image_default" if command in (None, []) else "explicit_server_command"


def validate_collector(stack, service, definition, tasks):
    p, out = parameters(stack), outputs(stack)
    require(p.get("Environment") == "preview", "not Preview")
    require(out.get("CollectorServiceName") == SERVICE, "service output drift")
    require(service.get("serviceName") == SERVICE and service.get("status") == "ACTIVE", "service identity drift")
    require(service.get("clusterArn") == out["CollectorClusterArn"], "cluster drift")
    require(service.get("taskDefinition") == out["CollectorTaskDefinitionArn"] == definition.get("taskDefinitionArn"), "task definition drift")
    require(service.get("desiredCount") == service.get("runningCount") == 1 and service.get("pendingCount") == 0, "Preview is not exactly one stable running task")
    require(len(service.get("deployments", [])) == 1, "concurrent deployment")
    containers = definition.get("containerDefinitions", [])
    require(len(containers) == 1 and containers[0].get("name") == "collector", "container drift")
    container = containers[0]
    env = {row["name"]: row["value"] for row in container.get("environment", [])}
    require(env.get("GROWTHBOOK_ENVIRONMENT") == "preview", "container environment drift")
    require(env.get("GROWTHBOOK_COLLECTOR_VERSION") == p["CollectorVersion"], "version drift")
    require(env.get("GROWTHBOOK_EVENT_BUCKET") == out["EventBucketName"], "bucket drift")
    require(container.get("image") == p["CollectorImageUri"], "immutable image drift")
    require(re.fullmatch(r"[0-9]+\.dkr\.ecr\.eu-central-1\.amazonaws\.com/vevo-growthbook-collector@sha256:[a-f0-9]{64}", container["image"]), "unpinned image")
    require(container.get("user") == "10001:10001" and container.get("readonlyRootFilesystem") is True, "container safety drift")
    require(container.get("workingDirectory", "/app") == "/app", "runtime path drift")
    command_source = validate_command(container)
    require(len(tasks) == 1, "running task count drift")
    task = tasks[0]
    require(task.get("launchType") == "FARGATE" and task.get("lastStatus") == "RUNNING" and task.get("healthStatus") == "HEALTHY", "task not healthy Fargate")
    require(task.get("clusterArn") == out["CollectorClusterArn"] and task.get("taskDefinitionArn") == out["CollectorTaskDefinitionArn"], "running task identity drift")
    require(len(task.get("containers", [])) == 1 and task["containers"][0].get("imageDigest") == p["CollectorImageUri"].split("@")[-1], "running image drift")
    ips = [d["value"] for a in task.get("attachments", []) for d in a.get("details", []) if d.get("name") == "privateIPv4Address"]
    require(len(ips) == 1 and ipaddress.ip_address(ips[0]) in ipaddress.ip_network("172.31.0.0/16"), "private IP unresolved")
    return {"instance_id": "N/A:Fargate", "task_id": task["taskArn"].rsplit("/", 1)[-1],
            "private_ip": ips[0], "service": SERVICE, "runtime_path": "/app",
            "task_definition": out["CollectorTaskDefinitionArn"].split("/")[-1],
            "image_digest": p["CollectorImageUri"].split("@")[-1], "version": p["CollectorVersion"],
            "desired_count": 1, "running_count": 1, "command_source": command_source,
            "public_ip_assignment": service["networkConfiguration"]["awsvpcConfiguration"]["assignPublicIp"]}


def inspect(session):
    cf, ecs, scheduler = (session.client(name, region_name=REGION) for name in ("cloudformation", "ecs", "scheduler"))
    require(session.client("sts", region_name=REGION).get_caller_identity()["Account"] == ACCOUNT, "AWS account mismatch")
    preview = stack_read(cf, STACK)
    out = outputs(preview)
    services = ecs.describe_services(cluster=out["CollectorClusterArn"], services=[SERVICE])
    require(not services.get("failures") and len(services["services"]) == 1, "service lookup failed")
    definition = ecs.describe_task_definition(taskDefinition=out["CollectorTaskDefinitionArn"])["taskDefinition"]
    arns = ecs.list_tasks(cluster=out["CollectorClusterArn"], serviceName=SERVICE, desiredStatus="RUNNING")["taskArns"]
    require(len(arns) == 1, "Preview already stopped or changed; review before mutation")
    task_response = ecs.describe_tasks(cluster=out["CollectorClusterArn"], tasks=arns)
    require(not task_response.get("failures"), "task lookup failed")
    runtime = validate_collector(preview, services["services"][0], definition, task_response["tasks"])
    reconciliation = stack_read(cf, RECONCILIATION)
    rp = parameters(reconciliation)
    require(rp["Environment"] == "preview", "reconciliation environment drift")
    require(rp["EventBucketName"] == out["EventBucketName"], "reconciliation bucket drift")
    schedule = scheduler.get_schedule(Name="vevo-growthbook-reconcile-preview")
    require(schedule["State"] == rp["ScheduleState"] == "ENABLED", "Preview schedule already changed")
    require(schedule["ScheduleExpression"] == "cron(30 3 * * ? *)" and schedule["ScheduleExpressionTimezone"] == "Europe/Bratislava", "Preview schedule drift")
    protected = {}
    for name in ("vevo-growthbook-production", "vevo-growthbook-reconciliation-production"):
        protected[name] = stack_fingerprint(cf, stack_read(cf, name))
    for name in (SOURCE_SCHEDULE, "vevo-growthbook-reconcile-production"):
        other = scheduler.get_schedule(Name=name)
        require(other["State"] == "ENABLED", "protected schedule not enabled")
        protected[name] = schedule_fingerprint(other)
    return {"schema_version": 1, "evidence_type": "vevo_preview_sleep_preflight",
            "observed_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "workflow_run_id": os.environ["GITHUB_RUN_ID"], "main_commit": os.environ["GITHUB_SHA"],
            "runtime": runtime, "preview_stack_sha256": stack_fingerprint(cf, preview),
            "reconciliation_stack_sha256": stack_fingerprint(cf, reconciliation),
            "preview_schedule_sha256": schedule_fingerprint(schedule), "protected_sha256": protected,
            "data_read": False, "mutation_performed": False, "sleep_deploy_allowed": False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--change-set-run-id", default="")
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REF") == "refs/heads/main" and os.environ.get("GITHUB_REPOSITORY") == "vzeman/biznisweb", "managed exact-main GitHub boundary required")
    import boto3
    session = boto3.Session()
    evidence = inspect(session)
    if args.change_set_run_id:
        require(re.fullmatch(r"[0-9]{8,20}", args.change_set_run_id), "invalid diagnostic run ID")
        evidence["change_set_diagnostic"] = inspect_change_sets(session, args.change_set_run_id)
    args.output.write_text(canonical(evidence), encoding="utf-8")
    print("PREVIEW_SLEEP_READ_ONLY_PREFLIGHT_OK")


def inspect_change_sets(session, run_id):
    cf, ecs = (session.client(name, region_name=REGION) for name in ("cloudformation", "ecs"))
    result = {}
    for name in (STACK, RECONCILIATION):
        stack = stack_read(cf, name)
        cluster = outputs(stack)["CollectorClusterArn"] if name == STACK else parameters(stack)["ClusterArn"]
        running = ecs.list_tasks(cluster=cluster, startedBy="preview-sleep-" + run_id)["taskArns"]
        require(not running, "diagnostic task still running; inspect exact ownership before cleanup")
        try:
            change_set = cf.describe_change_set(StackName=name, ChangeSetName="preview-sleep-" + run_id)
        except Exception as exc:
            require(type(exc).__name__ == "ClientError", "change-set lookup failed")
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            require(code == "ValidationError", "change-set lookup denied or unavailable")
            result[name] = {"lookup": "ValidationError", "diagnostic_tasks_running": 0}
            continue
        rows = []
        for item in change_set.get("Changes", []):
            change = item.get("ResourceChange", {})
            rows.append({key: change.get(key) for key in ("LogicalResourceId", "ResourceType", "Action", "Replacement", "Scope")})
            rows[-1]["Details"] = [{"Target": detail.get("Target", {}), "Evaluation": detail.get("Evaluation"), "ChangeSource": detail.get("ChangeSource")} for detail in change.get("Details", [])]
        result[name] = {"status": change_set.get("Status"), "execution_status": change_set.get("ExecutionStatus"), "changes": rows, "diagnostic_tasks_running": 0}
    return result


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Do not print boto exception bodies, response payloads, or tracebacks.
        print("PREVIEW_SLEEP_PREFLIGHT_FAILED:" + (str(exc) if isinstance(exc, ValueError) else type(exc).__name__))
        raise SystemExit(1) from None
