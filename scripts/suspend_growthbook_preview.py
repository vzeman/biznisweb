#!/usr/bin/env python3
"""One reviewed, no-deletion Preview transition through managed GitHub AWS only.

No data query, deploy/build, IAM mutation, direct ECS scaling, or automatic resume.
CloudFormation must propose only four allowlisted non-replacement modifications.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.inspect_growthbook_preview_sleep import (  # noqa: E402
    ACCOUNT, REGION, STACK, RECONCILIATION, SERVICE, canonical, digest, inspect,
    outputs, parameters, require, resources_read, schedule_fingerprint,
    stack_fingerprint, stack_read,
)
from scripts.resolve_growthbook_host_gate_runtime import resolve_host_gate_runtime  # noqa: E402

MANIFEST = Path("projects/vevo/growthbook_preview_lifecycle.json")
TEMPLATES = {STACK: Path("infra/vevo-growthbook/template.yaml"), RECONCILIATION: Path("infra/vevo-growthbook-reconciliation/template.yaml")}
ALLOWED = {
    STACK: {"CollectorService": ("AWS::ECS::Service", {"DesiredCount"}),
            "CollectorHealthyHostAlarm": ("AWS::CloudWatch::Alarm", {"Threshold", "TreatMissingData"})},
    RECONCILIATION: {"ReconciliationSchedule": ("AWS::Scheduler::Schedule", {"State"}),
                     "ReconciliationMissingSuccessAlarm": ("AWS::CloudWatch::Alarm", {"Threshold", "TreatMissingData"})},
}


class CloudFormationLoader(yaml.SafeLoader):
    pass


def intrinsic(loader, tag, node):
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node)
    else:
        value = loader.construct_mapping(node)
    if tag == "GetAtt" and isinstance(value, str):
        value = value.split(".", 1)
    return {tag if tag in {"Ref", "Condition"} else "Fn::" + tag: value}


CloudFormationLoader.add_multi_constructor("!", intrinsic)


def template_load(body):
    return yaml.load(body, Loader=CloudFormationLoader) if isinstance(body, str) else body


def validate_template_delta(old, new, stack):
    """Normalize only the exact new sleep fields; all other deployed bytes/values stay fixed."""
    normalized = copy.deepcopy(new)
    require(normalized["Parameters"].pop("PreviewSuspended") == {
        "Type": "String", "Default": "false", "AllowedValues": ["false", "true"],
        "Description": "Reversible Preview-only suspension; preserves every resource and all data."}, "sleep parameter drift")
    require(normalized["Conditions"].pop("IsPreviewSuspended") == {
        "Fn::And": [{"Fn::Equals": [{"Ref": "Environment"}, "preview"]}, {"Fn::Equals": [{"Ref": "PreviewSuspended"}, "true"]}]}, "sleep condition drift")
    if "Conditions" not in old and not normalized["Conditions"]:
        del normalized["Conditions"]
    rule = normalized.pop("Rules")
    require(rule == {"PreviewOnlySuspension": {"RuleCondition": {"Fn::Equals": [{"Ref": "PreviewSuspended"}, "true"]},
        "Assertions": [{"Assert": {"Fn::Equals": [{"Ref": "Environment"}, "preview"]}, "AssertDescription": "Production must never be suspended by the Preview lifecycle."}]}}, "Production exclusion rule drift")
    for logical_id, (_, fields) in ALLOWED[stack].items():
        props = normalized["Resources"][logical_id]["Properties"]
        for field in fields:
            original = old["Resources"][logical_id]["Properties"][field]
            asleep = "DISABLED" if field == "State" else "notBreaching" if field == "TreatMissingData" else 0
            require(props[field] == {"Fn::If": ["IsPreviewSuspended", asleep, original]}, "unexpected sleep property transformation")
            props[field] = original
    if stack == RECONCILIATION:
        require(normalized["Outputs"]["ScheduleState"]["Value"] == {"Fn::If": ["IsPreviewSuspended", "DISABLED", {"Ref": "ScheduleState"}]}, "schedule output drift")
        normalized["Outputs"]["ScheduleState"]["Value"] = {"Ref": "ScheduleState"}
    require(normalized == old, "deployed template differs outside the reviewed sleep fields")


def build_template(old, source, stack):
    # Preview predates Production-generalization of the reconciliation template.
    # Preserve that exact deployed template; apply only versioned sleep fragments.
    # The independently frozen stack hash binds the old input. Never redeploy the
    # current shared template wholesale just to change a runtime count/state.
    require("PreviewSuspended" not in old["Parameters"] and "Rules" not in old and "IsPreviewSuspended" not in old.get("Conditions", {}), "sleep support already exists")
    new = copy.deepcopy(old)
    new["Parameters"]["PreviewSuspended"] = copy.deepcopy(source["Parameters"]["PreviewSuspended"])
    new.setdefault("Conditions", {})["IsPreviewSuspended"] = copy.deepcopy(source["Conditions"]["IsPreviewSuspended"])
    new["Rules"] = copy.deepcopy(source["Rules"])
    for logical, (kind, fields) in ALLOWED[stack].items():
        require(new["Resources"][logical]["Type"] == kind, "deployed resource type drift")
        for field in fields:
            original = old["Resources"][logical]["Properties"][field]
            asleep = "DISABLED" if field == "State" else "notBreaching" if field == "TreatMissingData" else 0
            new["Resources"][logical]["Properties"][field] = {"Fn::If": ["IsPreviewSuspended", asleep, original]}
    if stack == RECONCILIATION:
        new["Outputs"]["ScheduleState"]["Value"] = {"Fn::If": ["IsPreviewSuspended", "DISABLED", {"Ref": "ScheduleState"}]}
    validate_template_delta(old, new, stack)
    return new


def validate_changes(payload, stack):
    require(payload.get("Status") == "CREATE_COMPLETE" and payload.get("ExecutionStatus") == "AVAILABLE", "change set not executable")
    rows = [row.get("ResourceChange", {}) for row in payload.get("Changes", [])]
    require(len(rows) == len(ALLOWED[stack]) and {row.get("LogicalResourceId") for row in rows} == set(ALLOWED[stack]), "change set resource allowlist drift")
    for row in rows:
        resource_type, fields = ALLOWED[stack][row["LogicalResourceId"]]
        require(row.get("Action") == "Modify" and row.get("Replacement") == "False" and row.get("ResourceType") == resource_type, "deletion/replacement/addition rejected")
        require(row.get("Scope") == ["Properties"], "change set scope drift")
        require(row.get("Details"), "missing property-level change evidence")
        for detail in row["Details"]:
            target = detail.get("Target", {})
            require(target.get("Attribute") == "Properties" and target.get("Name") in fields, "unrelated property change")


def validate_manifest(state):
    require(state.get("schema_version") == 1 and state.get("desired_state") == "suspended" and state.get("status") == "approved_pending_execution", "sleep transition not open")
    require(state.get("deletion_allowed") is False and state.get("production_mutation_allowed") is False and state.get("ordinary_preview_deploy_allowed") is False, "unsafe lifecycle gate")
    observed = datetime.fromisoformat(state["preflight"]["observed_at_utc"].replace("Z", "+00:00"))
    age = (datetime.now(timezone.utc) - observed).total_seconds()
    require(0 <= age <= 6 * 3600, "preflight older than six hours; fresh reviewed inspection required")


def protected_read(session):
    cf, scheduler = (session.client(name, region_name=REGION) for name in ("cloudformation", "scheduler"))
    result = {}
    for name in ("vevo-growthbook-production", "vevo-growthbook-reconciliation-production"):
        stack = stack_read(cf, name)
        result[name] = stack_fingerprint(cf, stack)
        if name == "vevo-growthbook-production":
            out = outputs(stack)
            ecs = session.client("ecs", region_name=REGION)
            response = ecs.describe_services(cluster=out["CollectorClusterArn"], services=[out["CollectorServiceArn"]])
            require(not response.get("failures") and len(response["services"]) == 1, "Production service lookup failed")
            service = response["services"][0]
            require(service["desiredCount"] == service["runningCount"] == 1 and service["pendingCount"] == 0, "Production service not stable 1/1")
            require(service["taskDefinition"] == out["CollectorTaskDefinitionArn"], "Production task definition drift")
            elb = session.client("elbv2", region_name=REGION)
            for target_group in service["loadBalancers"]:
                targets = elb.describe_target_health(TargetGroupArn=target_group["targetGroupArn"])["TargetHealthDescriptions"]
                require(len(targets) == 1 and targets[0]["TargetHealth"]["State"] == "healthy", "Production target not healthy")
    for name in ("vevo-daily-report-email", "vevo-growthbook-reconcile-production"):
        result[name] = schedule_fingerprint(scheduler.get_schedule(Name=name))
    return result


def host_gate(session, *, cluster, task_definition, network, container_name, command, environment, group, phase):
    """Run only the existing immutable host probe and clean up only this exact task."""
    ecs, logs = (session.client(name, region_name=REGION) for name in ("ecs", "logs"))
    definition = ecs.describe_task_definition(taskDefinition=task_definition)["taskDefinition"]
    containers = definition["containerDefinitions"]
    require(len(containers) == 1 and containers[0]["name"] == container_name, "host container drift")
    image = containers[0]["image"]
    require("@sha256:" in image and image.startswith(f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/"), "host image unpinned")
    digest_value = image.split("@")[-1]
    if container_name == "collector":
        env = {row["name"]: row["value"] for row in containers[0]["environment"]}
        version = env["GROWTHBOOK_COLLECTOR_VERSION"]
        health = f"COLLECTOR_LOCALHOST_HEALTH_OK:preview:{version}"
        marker = f"COLLECTOR_LOCALHOST_MARKER_OK:/app:{version}"
    else:
        version = digest_value
        environment = [{"name": "REPORT_PROJECT", "value": "vevo"}, {"name": "GROWTHBOOK_ENVIRONMENT", "value": "preview"}, {"name": "GROWTHBOOK_RECONCILE_VERSION", "value": version}]
        health = f"GROWTHBOOK_RECONCILE_LOCALHOST_HEALTH_OK:preview:{version}"
        marker = f"GROWTHBOOK_RECONCILE_LOCALHOST_MARKER_OK:/app:{version}"
    started_by = f"preview-sleep-{os.environ['GITHUB_RUN_ID']}"
    task_arn = None
    try:
        response = ecs.run_task(cluster=cluster, taskDefinition=task_definition, launchType="FARGATE", platformVersion="LATEST",
            networkConfiguration=network, group=group, startedBy=started_by, count=1,
            overrides={"containerOverrides": [{"name": container_name, "command": command, "environment": environment}]})
        require(not response.get("failures") and len(response.get("tasks", [])) == 1, "host task launch failed")
        task_arn = response["tasks"][0]["taskArn"]
        ecs.get_waiter("tasks_stopped").wait(cluster=cluster, tasks=[task_arn], WaiterConfig={"Delay": 5, "MaxAttempts": 120})
        response = ecs.describe_tasks(cluster=cluster, tasks=[task_arn])
        task = response["tasks"][0]
        require(task.get("group") == group and task.get("startedBy") == started_by, "host task ownership drift")
        options = containers[0]["logConfiguration"]["options"]
        runtime = resolve_host_gate_runtime(response, {"taskDefinition": definition}, expected_cluster_arn=cluster,
            expected_task_definition_arn=task_definition, expected_container_name=container_name,
            expected_log_group=options["awslogs-group"], expected_log_region=REGION,
            expected_log_prefix=options["awslogs-stream-prefix"], expected_image_digest=digest_value,
            expected_private_cidr="172.31.0.0/16")
        found = set()
        for _ in range(12):
            # Raw log data remains in runner memory; only exact safe marker matches escape.
            page = logs.get_log_events(logGroupName=options["awslogs-group"], logStreamName=runtime["log_stream"], startFromHead=True, limit=1000)
            found = {row["message"].strip() for row in page.get("events", []) if row["message"].strip() in {health, marker}}
            if found == {health, marker}:
                break
            time.sleep(5)
        require(found == {health, marker}, "localhost health/marker missing")
        return {"phase": phase, "instance_id": "N/A:Fargate", "task_id": runtime["task_id"], "private_ip": runtime["private_ip"],
            "service": group, "runtime_path": "/app", "image_digest": digest_value, "localhost_health": True, "localhost_marker": True, "task_stopped": True}
    finally:
        if task_arn:
            state = ecs.describe_tasks(cluster=cluster, tasks=[task_arn])
            require(not state.get("failures") and len(state["tasks"]) == 1, "host cleanup identity unavailable")
            own = state["tasks"][0]
            require(own.get("taskArn") == task_arn and own.get("clusterArn") == cluster and own.get("taskDefinitionArn") == task_definition and own.get("group") == group and own.get("startedBy") == started_by, "host cleanup ownership not proven")
            if own.get("lastStatus") != "STOPPED":
                ecs.stop_task(cluster=cluster, task=task_arn, reason="End this workflow's exact Preview localhost diagnostic")
                ecs.get_waiter("tasks_stopped").wait(cluster=cluster, tasks=[task_arn], WaiterConfig={"Delay": 5, "MaxAttempts": 60})


def suspend(session, state):
    baseline = state["preflight"]
    current = inspect(session)
    for key in ("runtime", "preview_stack_sha256", "reconciliation_stack_sha256", "preview_schedule_sha256", "protected_sha256"):
        require(current[key] == baseline[key], "fresh pre-mutation boundary drift: " + key)
    cf, ecs, scheduler = (session.client(name, region_name=REGION) for name in ("cloudformation", "ecs", "scheduler"))
    stacks = {name: stack_read(cf, name) for name in TEMPLATES}
    inventories = {name: resources_read(cf, name) for name in TEMPLATES}
    out = outputs(stacks[STACK])
    service = ecs.describe_services(cluster=out["CollectorClusterArn"], services=[SERVICE])["services"][0]
    rp = parameters(stacks[RECONCILIATION])
    require(rp["TaskDefinitionArn"].startswith(f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/vevo-growthbook-reconcile-preview:"), "reconciler task family drift")
    schedule_before = scheduler.get_schedule(Name="vevo-growthbook-reconcile-preview")
    require(schedule_before["Target"]["Arn"] == rp["ClusterArn"] and schedule_before["Target"]["EcsParameters"]["TaskDefinitionArn"] == rp["TaskDefinitionArn"], "reconciler scheduled runtime drift")
    reconcile_definition = ecs.describe_task_definition(taskDefinition=rp["TaskDefinitionArn"])["taskDefinition"]
    require(reconcile_definition["taskRoleArn"] == f"arn:aws:iam::{ACCOUNT}:role/BiznisWebReportingTaskRole-vevo", "reconciler role drift")
    # Never race or kill a scheduled Preview reconciliation already in progress.
    for page in ecs.get_paginator("list_tasks").paginate(cluster=rp["ClusterArn"], family="vevo-growthbook-reconcile-preview", desiredStatus="RUNNING"):
        require(not page["taskArns"], "Preview reconciliation in progress; retry after it finishes")
    collector_host = dict(cluster=out["CollectorClusterArn"], task_definition=out["CollectorTaskDefinitionArn"], network=service["networkConfiguration"],
        container_name="collector", command=["/bin/sh", "/app/growthbook_collector/host_gate.sh"], environment=[], group=SERVICE)
    reconciler_host = dict(cluster=rp["ClusterArn"], task_definition=rp["TaskDefinitionArn"], network={"awsvpcConfiguration": {
        "subnets": rp["SubnetIds"].split(","), "securityGroups": rp["SecurityGroupIds"].split(","), "assignPublicIp": rp["AssignPublicIp"]}},
        container_name=rp["ContainerName"], command=["/bin/bash", "/app/scripts/growthbook_preview_reconcile_host_gate.sh"], environment=[], group="vevo-growthbook-reconcile-preview")
    gates = [host_gate(session, **collector_host, phase="before"), host_gate(session, **reconciler_host, phase="before")]
    print("PREVIEW_SLEEP_LIVE_IDENTITY_AND_LOCALHOST_GATES_OK")
    plans = {}
    for name, path in TEMPLATES.items():
        original = template_load(cf.get_template(StackName=name, TemplateStage="Original")["TemplateBody"])
        proposed = build_template(original, template_load(path.read_text(encoding="utf-8")), name)
        values = [{"ParameterKey": row["ParameterKey"], "UsePreviousValue": True} for row in stacks[name]["Parameters"]]
        require("PreviewSuspended" not in parameters(stacks[name]), "sleep parameter already exists; new lifecycle review required")
        values.append({"ParameterKey": "PreviewSuspended", "ParameterValue": "true"})
        change = cf.create_change_set(StackName=name, ChangeSetName="preview-sleep-" + os.environ["GITHUB_RUN_ID"], ChangeSetType="UPDATE",
            TemplateBody=canonical(proposed), Parameters=values, Capabilities=["CAPABILITY_NAMED_IAM"],
            Description="Reviewed Preview sleep only; no deletion or replacement")
        plans[name] = change["Id"]
        cf.get_waiter("change_set_create_complete").wait(ChangeSetName=change["Id"], WaiterConfig={"Delay": 5, "MaxAttempts": 60})
        validate_changes(cf.describe_change_set(ChangeSetName=change["Id"]), name)
    # Recheck all fingerprints immediately before the first execution, after both plans passed.
    require(protected_read(session) == baseline["protected_sha256"], "protected boundary changed before execution")
    for name, key in ((STACK, "preview_stack_sha256"), (RECONCILIATION, "reconciliation_stack_sha256")):
        require(stack_fingerprint(cf, stack_read(cf, name)) == baseline[key], "Preview changed while plans were prepared")
    for name in (RECONCILIATION, STACK):
        validate_changes(cf.describe_change_set(ChangeSetName=plans[name]), name)
        cf.execute_change_set(ChangeSetName=plans[name])
        cf.get_waiter("stack_update_complete").wait(StackName=name, WaiterConfig={"Delay": 10, "MaxAttempts": 120})
        print("PREVIEW_SLEEP_STACK_UPDATED:" + name)
    stopped_service = ecs.describe_services(cluster=out["CollectorClusterArn"], services=[SERVICE])["services"][0]
    require(stopped_service["desiredCount"] == stopped_service["runningCount"] == stopped_service["pendingCount"] == 0, "collector not fully suspended")
    require(stopped_service["taskDefinition"] == out["CollectorTaskDefinitionArn"], "collector image/definition changed")
    require(not ecs.list_tasks(cluster=out["CollectorClusterArn"], serviceName=SERVICE, desiredStatus="RUNNING")["taskArns"], "collector task remains running")
    schedule = scheduler.get_schedule(Name="vevo-growthbook-reconcile-preview")
    require(schedule["State"] == "DISABLED", "Preview schedule not suspended")
    enabled_copy = dict(schedule, State="ENABLED")
    require(schedule_fingerprint(enabled_copy) == baseline["preview_schedule_sha256"], "Preview schedule changed outside State")
    require(all(resources_read(cf, name) == inventories[name] for name in TEMPLATES), "resource deletion/replacement detected")
    gates += [host_gate(session, **collector_host, phase="after"), host_gate(session, **reconciler_host, phase="after")]
    require(protected_read(session) == baseline["protected_sha256"], "protected Production/source resources changed")
    final = {name: stack_fingerprint(cf, stack_read(cf, name)) for name in TEMPLATES}
    return {"schema_version": 1, "evidence_type": "vevo_preview_suspended", "workflow_run_id": os.environ["GITHUB_RUN_ID"],
        "main_commit": os.environ["GITHUB_SHA"], "observed_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "manifest_sha256": hashlib.sha256(MANIFEST.read_bytes()).hexdigest(),
        "collector_desired_count": 0, "collector_running_count": 0, "preview_schedule_state": "DISABLED",
        "resource_inventory_unchanged": True, "data_read": False, "data_deleted": False, "load_balancer_retained": True,
        "protected_sha256": baseline["protected_sha256"], "preview_stack_sha256": final, "host_gates": gates,
        "ordinary_preview_deploy_blocked": True, "automatic_resume_allowed": False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    state = json.loads(MANIFEST.read_text(encoding="utf-8"))
    validate_manifest(state)
    if args.validate:
        print("PREVIEW_SLEEP_LOCAL_GATE_OK:no-deletion:production-protected")
        return
    require(args.output is not None, "output path required")
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REF") == "refs/heads/main" and os.environ.get("GITHUB_REPOSITORY") == "vzeman/biznisweb" and os.environ.get("CONFIRM_SUSPEND") == "true", "managed confirmed main boundary required")
    import boto3
    evidence = suspend(boto3.Session(), state)
    args.output.write_text(canonical(evidence), encoding="utf-8")
    print("PREVIEW_SUSPENDED_VERIFIED:no-deletion:production-unchanged:alb-retained")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        code = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        if type(exc).__name__ == "ClientError":
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            if error_code in {"AccessDenied", "AccessDeniedException", "ValidationError", "ResourceNotFoundException", "ThrottlingException"}:
                code += ":" + error_code
        print("PREVIEW_SUSPEND_FAILED:" + code)
        raise SystemExit(1) from None
