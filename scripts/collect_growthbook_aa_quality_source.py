"""GitHub-hosted, main-only acquisition of the frozen VEVO A/A quality source.

Nothing runs on import. The CLI checks its reviewed GitHub boundary before any
AWS client or token read. Raw events, AWS responses and receipted orders remain
in runner memory. It neither deploys nor publishes ordinary reporting facts.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import ipaddress
import json
import logging
import os
import re
import subprocess
import sys
import time
import zipfile
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reporting_core.experiment_quality_source_io import (
    QualityInputError, RAW_SOURCE_PHASES, RECEIPTED_ORDER_QUERY,
    read_receipted_order_source, read_stable_retained_raw_source,
)
from reporting_core.experiments import (
    ExperimentReceiptWindow, load_experiment_build_config, order_completion_receipts,
)
from scripts.build_growthbook_aa_quality_source import (
    WORKFLOW, build_quality_source, canonical_source_bytes,
)
from scripts.validate_growthbook_aa_measurement_window import validate_measurement_window
from scripts.validate_growthbook_aa_infra_health_evidence import validate_health_evidence
from scripts.record_growthbook_natural_evidence import canonical_evidence_bytes as canonical_health_bytes
from scripts.summarize_growthbook_receipts import summarize_receipts

REPO = "vzeman/biznisweb"
ACCOUNT = "919341186960"
REGION = "eu-central-1"
SERVICE = "vevo-growthbook-collector-production"
RECONCILER = "vevo-growthbook-reconcile-production"
SOURCE_SCHEDULE = "vevo-daily-report-email"
API_URL = "https://vevo.flox.sk/api/graphql"
ARTIFACT = "vevo-growthbook-aa-quality-source"
FILENAME = ARTIFACT + ".json"
HEALTH_WORKFLOW = ".github/workflows/monitor-vevo-growthbook-production-aa-infra.yml"
HEALTH_ARTIFACT = "vevo-growthbook-production-aa-infra-health"
SHA = re.compile(r"^[a-f0-9]{64}$")
COMMIT = re.compile(r"^[a-f0-9]{40}$")
RUN = re.compile(r"^[1-9][0-9]{5,19}$")
RAW_READ_WORKERS = 8


class SourceCollectionError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise SourceCollectionError(message)


CAPTURE_PHASES = (
    "runtime-preflight", "retained-raw-source", *RAW_SOURCE_PHASES, "receipt-parity",
    "reporting-runtime", "managed-token", "receipted-orders",
    "authoritative-facts", "quality-build", "runtime-readback",
)
SAFE_FAILURE_CODES = {
    "retained raw source coverage could not be verified": "raw-coverage-unverified",
    "receipted order coverage could not be verified": "order-coverage-unverified",
    "retained write/receipt parity failed": "receipt-parity-failed",
    "source runtime/control changed during capture": "control-changed",
    "VEVO API URL drift": "api-configuration-drift",
    "unexpected runner API environment": "api-environment-drift",
    "managed token format unsupported": "managed-token-format",
}


class CaptureProgress:
    """Emit only fixed operation names, never inputs, counts or exceptions."""

    def __init__(self, stream=None):
        self.stream = stream
        self.phase = "not-started"

    def __call__(self, phase):
        require(type(phase) is str and phase in CAPTURE_PHASES, "diagnostic phase invalid")
        self.phase = phase
        if self.stream is not None:
            print(f"VEVO_AA_QUALITY_SOURCE_PROGRESS:phase={phase}:raw=false",
                  file=self.stream, flush=True)


def safe_failure_code(error):
    # Never format an exception or inspect SDK payloads/causes. Only exact local
    # exception types and exact constant messages select a fixed output code.
    if type(error) in {SourceCollectionError, QualityInputError}:
        if len(error.args) == 1 and type(error.args[0]) is str:
            known = SAFE_FAILURE_CODES.get(error.args[0])
            if known is not None:
                return known
        return "local-contract-check" if type(error) is SourceCollectionError else "input-read-or-validation"
    return "unclassified-error"


def stamp(value):
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def now_utc():
    return datetime.now(UTC)


def utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    require(isinstance(parsed, datetime) and parsed.utcoffset() == timedelta(0), "UTC identity drift")
    return parsed.astimezone(UTC)


def digest(value):
    # Raw control-plane metadata is hashed in memory, never returned or printed.
    def encode(item):
        if isinstance(item, datetime):
            return item.isoformat()
        raise TypeError("unsupported control metadata type")
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=encode).encode()
    return hashlib.sha256(raw).hexdigest()


def gh_json(path):
    result = subprocess.run(["gh", "api", path], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    require(result.returncode == 0 and len(result.stdout) <= 4 * 1024 * 1024,
            "GitHub provenance read failed")
    return json.loads(result.stdout)


def download_health(run_id, main_commit, expected_json_sha256):
    """Independently bind successful main, sole artifact, GitHub ZIP digest and JSON."""
    require(RUN.fullmatch(run_id) and COMMIT.fullmatch(main_commit)
            and SHA.fullmatch(expected_json_sha256), "health provenance input invalid")
    run = gh_json(f"repos/{REPO}/actions/runs/{run_id}")
    require(str(run.get("id")) == run_id and run.get("head_sha") == main_commit
            and run.get("head_branch") == "main" and run.get("path") == HEALTH_WORKFLOW
            and run.get("status") == "completed" and run.get("conclusion") == "success",
            "health run provenance mismatch")
    listing = gh_json(f"repos/{REPO}/actions/runs/{run_id}/artifacts?per_page=100")
    require(listing.get("total_count") == 1 and len(listing.get("artifacts", [])) == 1,
            "health artifact set mismatch")
    artifact = listing["artifacts"][0]
    require(artifact.get("name") == HEALTH_ARTIFACT and artifact.get("expired") is False,
            "health artifact unavailable")
    result = subprocess.run(["gh", "api", f"repos/{REPO}/actions/artifacts/{artifact['id']}/zip"],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    require(result.returncode == 0 and len(result.stdout) <= 1024 * 1024,
            "health artifact download failed")
    require(artifact.get("digest") == "sha256:" + hashlib.sha256(result.stdout).hexdigest(),
            "GitHub ZIP digest mismatch")
    with zipfile.ZipFile(io.BytesIO(result.stdout)) as archive:
        entries = archive.infolist()
        require(len(entries) == 1 and entries[0].filename == HEALTH_ARTIFACT + ".json"
                and not entries[0].is_dir() and entries[0].file_size <= 1024 * 1024,
                "health ZIP content mismatch")
        raw = archive.read(entries[0])
    evidence = json.loads(raw)
    require(raw == canonical_health_bytes(evidence)
            and hashlib.sha256(raw).hexdigest() == expected_json_sha256, "health JSON mismatch")
    deploy_raw = (ROOT / "projects/vevo/growthbook_production_reconciliation_deploy_evidence.json").read_bytes()
    validate_health_evidence(evidence, json.loads(deploy_raw), deploy_evidence_bytes=deploy_raw)
    require(evidence["provenance"]["workflow_run_id"] == run_id
            and evidence["provenance"]["main_commit"] == main_commit,
            "health evidence provenance mismatch")
    now = now_utc()
    local = now.astimezone(ZoneInfo("Europe/Bratislava"))
    due = local.replace(hour=3, minute=45, second=0, microsecond=0)
    if local < due:
        due -= timedelta(days=1)
    require(evidence["phase"]["status"] == "natural_reconciliation_verified"
            and evidence["phase"]["checked_due_local"] == due.isoformat(timespec="seconds")
            and timedelta(0) <= now - utc(evidence["observed_at_utc"]) <= timedelta(hours=6),
            "fresh successful reconciliation health is required")
    return evidence


def reject_previous_capture(current_run_id):
    # This experiment has one resolved source window. Never collect again after
    # a successful capture merely because its source has not yet been recorded.
    page = 1
    while page <= 100:
        result = gh_json(f"repos/{REPO}/actions/workflows/{Path(WORKFLOW).name}/runs"
                         f"?branch=main&event=workflow_dispatch&per_page=100&page={page}")
        runs = result.get("workflow_runs")
        require(isinstance(runs, list), "source run history unavailable")
        for run in runs:
            if str(run.get("id")) == current_run_id:
                continue
            require(run.get("status") == "completed", "another source run is active")
            require(run.get("conclusion") != "success", "consume the existing successful source capture")
            prior_id = str(run.get("id", ""))
            require(RUN.fullmatch(prior_id), "source history run identity invalid")
            artifacts = gh_json(f"repos/{REPO}/actions/runs/{prior_id}/artifacts?per_page=100")
            require(artifacts.get("total_count") == 0, "a prior source artifact requires recovery, not recapture")
        if len(runs) < 100:
            return
        page += 1
    raise SourceCollectionError("source run history exceeds review bound")


@dataclass(frozen=True)
class SourcePlan:
    window: ExperimentReceiptWindow
    eligible: int
    snapshot_sha256: str
    checkpoint_sha256: str
    foundation_sha256: str
    main_commit: str
    run_id: str
    health_run_id: str
    health_sha256: str


def make_plan(snapshot_raw, workspace, activation, acceptance, reconciliation, environ):
    require(environ.get("GITHUB_ACTIONS") == "true"
            and environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and environ.get("GITHUB_REPOSITORY") == REPO
            and environ.get("GITHUB_REF") == "refs/heads/main"
            and environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and environ.get("GITHUB_RUN_ATTEMPT") == "1"
            and environ.get("GITHUB_WORKFLOW_REF") == f"{REPO}/{WORKFLOW}@refs/heads/main"
            and environ.get("CONFIRM_SOURCE") == "true", "managed main-only source gate is closed")
    run_id, main_commit = environ.get("GITHUB_RUN_ID", ""), environ.get("GITHUB_SHA", "")
    require(RUN.fullmatch(run_id) and COMMIT.fullmatch(main_commit), "source provenance is invalid")
    health_id, health_sha = environ.get("HEALTH_RUN_ID", ""), environ.get("HEALTH_JSON_SHA256", "")
    require(RUN.fullmatch(health_id) and SHA.fullmatch(health_sha) and health_id != run_id,
            "independent health provenance is required")
    snapshot = json.loads(snapshot_raw)
    validate_measurement_window(snapshot, activation, acceptance, reconciliation)
    frozen = snapshot["measurement_window"]
    require(frozen["resolution_status"] == "resolved" and snapshot["snapshot_build_allowed"] is False,
            "source requires the resolved, still closed snapshot")
    auto = snapshot["automated_evidence"]
    require(auto["producer_allowed"] is False and auto["status"] == "not_recorded"
            and auto["quality_report_status"] == "not_recorded"
            and auto["quality_report_key"] is None and auto["quality_report_sha256"] is None,
            "initial source has already been bound")
    require(activation["status"] == "production_aa_running_activation_verified"
            and activation["growthbook"]["allocation_percent"] == 100,
            "A/A running identity drift")
    experiments = {row["tracking_key"]: row for row in workspace["experiments"]}
    require(experiments["vevo-sk-product-cta-color-001"]["status"] == "unstarted_draft"
            and experiments["vevo-sk-product-cta-color-001"]["production_allocation_percent"] == 0,
            "CTA must remain closed")
    foundation = workspace["athena"]["production"]["successful_foundation_deployment"]
    require(foundation["status"] == "passed" and foundation["deployment"]["event_bucket_empty"] is True
            and foundation["deployment"]["public_route_enabled"] is False,
            "verified empty Production foundation is missing")
    observed = utc(foundation["verified_at_utc"])
    start, through = utc(frozen["from_utc"]), utc(frozen["resolved_through_utc"])
    require(observed < start < through <= now_utc(), "source context chronology drift")
    context = observed.replace(hour=0, minute=0, second=0, microsecond=0)
    return SourcePlan(ExperimentReceiptWindow(context, start, through), frozen["resolved_eligible_devices"],
                      hashlib.sha256(snapshot_raw).hexdigest(), frozen["checkpoint_history"][-1]["evidence_sha256"],
                      hashlib.sha256(canonical_source_bytes(foundation)).hexdigest(), main_commit, run_id,
                      health_id, health_sha)


def only(items, message):
    require(isinstance(items, list) and len(items) == 1, message)
    return items[0]


def runtime_preflight(client, plan, activation, reconciliation):
    require(client("sts").get_caller_identity()["Account"] == ACCOUNT, "AWS account drift")
    cfn, ecs, scheduler, s3 = (client(name) for name in ("cloudformation", "ecs", "scheduler", "s3"))
    stack = only(cfn.describe_stacks(StackName="vevo-growthbook-production")["Stacks"], "stack identity drift")
    require(stack["StackStatus"] in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}, "Production stack is unstable")
    outputs = {row["OutputKey"]: row["OutputValue"] for row in stack["Outputs"]}
    require(outputs["CollectorServiceName"] == SERVICE
            and outputs["ExperimentDatabaseName"] == "vevo_growthbook_production"
            and outputs["ReportingWorkGroupName"] == "vevo-growthbook-reporting-production",
            "Production stack output identity drift")
    bucket = outputs["EventBucketName"]
    resource = cfn.describe_stack_resource(StackName="vevo-growthbook-production",
                                           LogicalResourceId="ExperimentDataBucket")["StackResourceDetail"]
    require(resource["PhysicalResourceId"] == bucket and resource["ResourceType"] == "AWS::S3::Bucket",
            "Production bucket identity drift")
    rules = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
    expected_rules = {
        "ExpireRawExperimentEvents": ("experiment-events/raw/", "Expiration", {"Days": 180}),
        "ExpireCuratedExperimentFacts": ("experiment-events/curated/", "Expiration", {"Days": 400}),
        "ExpireAthenaQueryResults": ("athena-results/", "Expiration", {"Days": 30}),
        "AbortIncompleteMultipartUploads": ("", "AbortIncompleteMultipartUpload", {"DaysAfterInitiation": 1}),
    }
    require(len(rules) == 4 and {rule.get("ID") for rule in rules} == set(expected_rules), "retention rule set drift")
    for rule in rules:
        prefix, action, value = expected_rules[rule["ID"]]
        selector = "Prefix" if "Prefix" in rule else "Filter"
        require(set(rule) == {"ID", "Status", selector, action}
                and rule["Status"] == "Enabled" and rule[action] == value
                and rule[selector] == (prefix if selector == "Prefix" else {"Prefix": prefix}),
                "retention policy drift")
    # Retention *policy* coverage, not an unsupported forensic claim that an
    # administrator could never have deleted an object. Receipt parity below
    # independently checks the number of accepted writes over the context.
    require(now_utc() - plan.window.context_from_utc < timedelta(days=180), "source retention elapsed")
    expected_collector = activation["collector"]
    definition_arn = outputs["CollectorTaskDefinitionArn"]
    require(definition_arn.rsplit("/", 1)[-1] == expected_collector["task_definition"], "collector definition drift")
    services = ecs.describe_services(cluster=outputs["CollectorClusterArn"], services=[SERVICE])
    require(not services.get("failures"), "collector service read failed")
    service = only(services["services"], "collector service count drift")
    require(service["status"] == "ACTIVE" and service["desiredCount"] == service["runningCount"] == 1
            and service["pendingCount"] == 0 and service["taskDefinition"] == definition_arn
            and service.get("enableExecuteCommand") is False, "collector service drift")
    tasks = ecs.list_tasks(cluster=outputs["CollectorClusterArn"], serviceName=SERVICE, desiredStatus="RUNNING")
    require(not tasks.get("nextToken"), "collector task list incomplete")
    task_arn = only(tasks["taskArns"], "collector task count drift")
    details = ecs.describe_tasks(cluster=outputs["CollectorClusterArn"], tasks=[task_arn])
    require(not details.get("failures"), "collector task read failed")
    task = only(details["tasks"], "collector task identity drift")
    container = only(task["containers"], "collector container count drift")
    require(task["taskArn"] == task_arn and task["taskDefinitionArn"] == definition_arn
            and task["launchType"] == "FARGATE" and task["lastStatus"] == "RUNNING"
            and container["imageDigest"] == expected_collector["image_digest"], "live collector identity drift")
    addresses = [row["value"] for attachment in task.get("attachments", []) for row in attachment.get("details", [])
                 if row.get("name") == "privateIPv4Address"]
    address = ipaddress.ip_address(only(addresses, "collector private address missing"))
    require(address in ipaddress.ip_network("172.31.0.0/16"), "collector private network drift")
    require(expected_collector["service"] == SERVICE and expected_collector["runtime_path"] == "/app",
            "collector localhost-gated path drift")
    schedule = scheduler.get_schedule(Name=RECONCILER)
    require(schedule["Name"] == RECONCILER and schedule["State"] == "ENABLED" and schedule["ScheduleExpression"] == "cron(45 3 * * ? *)"
            and schedule["ScheduleExpressionTimezone"] == "Europe/Bratislava", "reconciliation schedule drift")
    target = schedule["Target"]["EcsParameters"]["TaskDefinitionArn"]
    require(target == reconciliation["reconciliation"]["task_definition"], "reconciler definition drift")
    definition = ecs.describe_task_definition(taskDefinition=target)["taskDefinition"]
    require(definition["taskDefinitionArn"] == target and definition["networkMode"] == "awsvpc"
            and definition["taskRoleArn"] == f"arn:aws:iam::{ACCOUNT}:role/BiznisWebReportingTaskRole-vevo",
            "managed reconciler role drift")
    reconciler_container = only(definition["containerDefinitions"], "reconciler container drift")
    require(reconciler_container["image"].endswith("@" + reconciliation["reconciliation"]["image_digest"])
            and reconciler_container["command"] == ["/bin/bash", "-lc",
                "cd /app && python scripts/run_scheduled_growthbook_reconciliation.py"], "reconciler image/path drift")
    host_gate = reconciliation["host_gate"]
    require(host_gate["service"] == RECONCILER and host_gate["runtime_path"] == "/app"
            and host_gate["localhost_health_verified"] is True and host_gate["localhost_marker_verified"] is True,
            "reconciler inherited localhost gate missing")
    source_schedule = scheduler.get_schedule(Name=SOURCE_SCHEDULE)
    source_definition_arn = source_schedule["Target"]["EcsParameters"]["TaskDefinitionArn"]
    require(source_schedule["Name"] == SOURCE_SCHEDULE and source_schedule["State"] == "ENABLED"
            and source_schedule["Target"]["Arn"] == schedule["Target"]["Arn"]
            and source_definition_arn == reconciliation["source_runtime"]["task_definition"], "source schedule drift")
    source_definition = ecs.describe_task_definition(taskDefinition=source_definition_arn)["taskDefinition"]
    source_container = only(source_definition["containerDefinitions"], "source container count drift")

    def token_ref(value):
        return only([row for row in value.get("secrets", []) if row.get("name") == "BIZNISWEB_API_TOKEN"],
                    "exact managed token reference missing")["valueFrom"]

    reference = token_ref(reconciler_container)
    require(reference == token_ref(source_container), "managed token inheritance drift")
    # Botocore adds per-request IDs, headers and retry metadata to these two
    # top-level responses. They are not Scheduler configuration and necessarily
    # differ across otherwise identical reads. Preserve every actual field,
    # including nested target/role/input settings; do not mutate the responses.
    schedule_config = {key: value for key, value in schedule.items() if key != "ResponseMetadata"}
    source_config = {key: value for key, value in source_schedule.items() if key != "ResponseMetadata"}
    control = {"source_schedule": source_config, "schedule": schedule_config, "definition": definition,
               "bucket": bucket, "rules": rules, "task": task_arn,
               "private_ip": str(address), "collector_definition": definition_arn}
    return {"bucket": bucket, "log_group": outputs["CollectorContainerLogGroup"],
            "token_reference": reference, "control_sha256": digest(control)}


def read_managed_token(client, reference):
    require(isinstance(reference, str), "managed token reference invalid")
    if reference.startswith(f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter/"):
        response = client("ssm").get_parameter(Name=reference, WithDecryption=True)
        require(response["Parameter"]["ARN"] == reference and response["Parameter"]["Type"] == "SecureString",
                "managed token parameter mismatch")
        token = response["Parameter"]["Value"]
    else:
        parts = reference.split(":")
        require(len(parts) in {7, 10} and ":".join(parts[:6]) == f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret",
                "unsupported managed token reference")
        request = {"SecretId": ":".join(parts[:7])}
        if len(parts) == 10:
            require(parts[7] == "BIZNISWEB_API_TOKEN" and not (parts[8] and parts[9]), "token JSON selector drift")
            if parts[8]:
                request["VersionStage"] = parts[8]
            if parts[9]:
                request["VersionId"] = parts[9]
        response = client("secretsmanager").get_secret_value(**request)
        require(response["ARN"] == request["SecretId"] and "SecretBinary" not in response, "managed secret identity drift")
        token = response["SecretString"]
        if len(parts) == 10:
            token = json.loads(token)["BIZNISWEB_API_TOKEN"]
    require(isinstance(token, str) and re.fullmatch(r"[A-Za-z0-9]{32}", token), "managed token format unsupported")
    return token


def fixed_order_transport(session, token, pace):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate order response field")
            result[key] = value
        return result

    def reject_constant(_value):
        raise SourceCollectionError("nonfinite order response value")

    def execute(query, *, variable_values):
        require(query == RECEIPTED_ORDER_QUERY and set(variable_values) == {"order_num"}
                and re.fullmatch(r"[0-9]{1,20}", variable_values["order_num"]), "order transport scope drift")
        pace()
        with session.post(API_URL, json={"query": query, "variables": variable_values},
                          headers={"BW-API-Key": "Token " + token, "Content-Type": "application/json"},
                          timeout=(10, 45), allow_redirects=False, stream=True) as response:
            require(response.status_code == 200 and response.url == API_URL, "order API response failed")
            require(response.headers.get("Content-Type", "").split(";", 1)[0].strip() == "application/json",
                    "order API response format drift")
            chunks, size = [], 0
            for chunk in response.iter_content(8192):
                size += len(chunk)
                require(size <= 128 * 1024, "order response too large")
                chunks.append(chunk)
            payload = json.loads(b"".join(chunks), object_pairs_hook=unique_object,
                                 parse_constant=reject_constant)
            require(isinstance(payload, dict) and set(payload) == {"data"}, "partial order API response")
            return payload["data"]
    return execute


def receipt_parity(logs, log_group, plan, raw_rows):
    response = logs.describe_log_groups(logGroupNamePrefix=log_group)
    group = only(response["logGroups"], "collector log group identity drift")
    require(group["logGroupName"] == log_group and not response.get("nextToken"), "collector log group drift")
    retention = group.get("retentionInDays")
    require(type(retention) is int and now_utc() - plan.window.context_from_utc < timedelta(days=retention),
            "receipt logs do not cover the source context")
    events, tokens, token = [], set(), None
    while True:
        request = {"logGroupName": log_group, "filterPattern": '"VEVO_GROWTHBOOK_COLLECTOR_RECEIPT"',
                   "startTime": int(plan.window.context_from_utc.timestamp() * 1000),
                   "endTime": int(plan.window.through_utc.timestamp() * 1000), "limit": 10000}
        if token:
            request["nextToken"] = token
        page = logs.filter_log_events(**request)
        events.extend(page["events"])
        require(len(events) <= 100000, "receipt coverage exceeds source bound")
        token = page.get("nextToken")
        if not token:
            break
        require(token not in tokens and len(tokens) < 1000, "receipt pagination drift")
        tokens.add(token)
    summary = summarize_receipts({"events": events}, from_utc=stamp(plan.window.context_from_utc),
                                 through_utc=stamp(plan.window.through_utc))
    in_context = [row for row in raw_rows if utc(row["received_at"]) < plan.window.through_utc]
    require(summary["collector_unique_accepted_event_count"] == len(in_context), "retained write/receipt parity failed")
    return {"context_receipt_summary_sha256": digest(summary), "accepted_write_count_parity_verified": True}


def collect(plan, client, activation, reconciliation, health_binding, *, progress=None):
    progress = progress if progress is not None else CaptureProgress()
    started = now_utc().replace(microsecond=0)
    progress("runtime-preflight")
    before = runtime_preflight(client, plan, activation, reconciliation)
    progress("retained-raw-source")
    # Resolve/cache the explicit-session S3 client on this coordinator thread.
    # Workers share only that existing client's read-only operations, never the
    # Session/factory, mutable client metadata or custom Botocore event hooks.
    raw = read_stable_retained_raw_source(client("s3"), bucket=before["bucket"],
                                         context_from_utc=plan.window.context_from_utc,
                                         through_utc=plan.window.through_utc, progress=progress,
                                         max_read_workers=RAW_READ_WORKERS)
    progress("receipt-parity")
    parity = receipt_parity(client("logs"), before["log_group"], plan, raw.rows)
    receipts = order_completion_receipts(row for row in raw.rows if utc(row["received_at"]) < plan.window.through_utc)
    progress("reporting-runtime")
    # Importing the legacy reporter can load .env, so the CLI verifies the clean
    # runner checkout has none before this point. Its calculator mode constructs
    # no API/ad/weather client and creates no output/cache directory.
    import export_orders as reporting
    from reporting_core.config import load_project_settings
    from reporting_core.runtime import load_project_runtime, apply_project_runtime
    from reporting_core.experiment_orders import build_biznisweb_authoritative_orders
    import requests
    settings = load_project_settings("vevo")
    require(settings["biznisweb_api_url"] == API_URL, "VEVO API URL drift")
    runtime = load_project_runtime("vevo", settings=settings,
        legacy_product_expenses=reporting.LEGACY_VEVO_PRODUCT_EXPENSES,
        default_currency_rates=reporting.CURRENCY_RATES_TO_EUR,
        default_packaging_cost_per_order=reporting.PACKAGING_COST_PER_ORDER,
        default_shipping_subsidy_per_order=reporting.SHIPPING_SUBSIDY_PER_ORDER,
        default_fixed_monthly_cost=reporting.FIXED_MONTHLY_COST, default_fixed_daily_cost=reporting.FIXED_DAILY_COST)
    require(runtime.api_url == API_URL and not runtime.api_token, "unexpected runner API environment")
    apply_project_runtime(runtime, reporting.__dict__)
    exporter = reporting.BizniWebExporter(API_URL, "", project_name="vevo", order_facts_only=True)
    progress("managed-token")
    token = read_managed_token(client, before["token_reference"])
    request_count = 0

    def pace():
        nonlocal request_count
        delay = 0.5
        if request_count and request_count % 100 == 0:
            delay = max(delay, 5.0)
        time.sleep(delay)
        request_count += 1

    progress("receipted-orders")
    with requests.Session() as session:
        session.trust_env = False
        orders = read_receipted_order_source(fixed_order_transport(session, token, pace), completion_receipts=receipts)
    token = None
    progress("authoritative-facts")
    config = load_experiment_build_config(ROOT / "projects/vevo/growthbook_reporting.json")
    config = replace(config, expected_variation_weights={"vevo-sk-aa-001": config.expected_variation_weights["vevo-sk-aa-001"]})
    generated = now_utc().replace(microsecond=0)
    facts = build_biznisweb_authoritative_orders(exporter, orders.orders, completion_receipts=receipts,
        generated_at=generated, maturity_checkpoint_days=config.maturity_checkpoint_days,
        packaging_cost_eur=reporting.PACKAGING_COST_PER_ORDER, shipping_net_cost_eur=reporting.SHIPPING_NET_PER_ORDER)
    progress("quality-build")
    source = build_quality_source(raw.rows, facts, config=config, window=plan.window, generated_at=generated,
        expected_eligible_devices=plan.eligible, snapshot_manifest_sha256=plan.snapshot_sha256,
        checkpoint_evidence_sha256=plan.checkpoint_sha256, workflow_run_id=plan.run_id, main_commit=plan.main_commit)
    progress("runtime-readback")
    after = runtime_preflight(client, plan, activation, reconciliation)
    require(before == after, "source runtime/control changed during capture")
    return {
        "schema_version": 1, "evidence_type": "vevo_growthbook_aa_quality_capture",
        "source": source,
        "acquisition": {
            "started_at_utc": stamp(started), "completed_at_utc": stamp(now_utc()),
            "foundation_evidence_sha256": plan.foundation_sha256,
            "context_floor_source": "verified_empty_production_foundation_utc_day",
            "retention_policy_days": 180,
            "raw_input": raw.sanitized_proof, "order_input": orders.sanitized_proof,
            "receipt_parity": parity, "control_before_sha256": before["control_sha256"],
            "control_after_sha256": after["control_sha256"],
            "managed_token_reference_sha256": hashlib.sha256(before["token_reference"].encode()).hexdigest(),
            "health": health_binding,
            "collector_live_identity_verified": True,
            "reconciler_immutable_localhost_gate_inherited": True,
            "source_schedule_unchanged": True,
        },
        "safety": {"read_only": True, "contains_identities": False, "contains_credentials": False,
                   "contains_raw_aws_payloads": False, "ordinary_publish_allowed": False,
                   "preview_woken": False, "experiment_mutations": False, "winner_calls": False},
    }


def load_inputs():
    from scripts.record_growthbook_aa_evidence_gates import EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED
    require(EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED is True, "exact-window source consumer migration is not reviewed")
    directory = ROOT / "projects/vevo"
    require(not list(ROOT.glob(".env*")) or all(p.name.endswith((".example", ".required", ".template"))
            for p in ROOT.glob(".env*")), "runner environment file boundary drift")
    require(not any(p.is_file() and not p.name.endswith((".example", ".required", ".template"))
                    for p in directory.glob(".env*")), "project environment file boundary drift")
    load = lambda name: json.loads((directory / name).read_bytes())
    activation = load("growthbook_production_aa_activation.json")
    reconciliation = load("growthbook_production_reconciliation_deploy_evidence.json")
    plan = make_plan((directory / "growthbook_aa_snapshot.json").read_bytes(), load("growthbook_workspace.json"),
                     activation, load("growthbook_aa_acceptance.json"), reconciliation, os.environ)
    return plan, activation, reconciliation


def verify_checkout(main_commit):
    for command, expected in ((["git", "rev-parse", "HEAD"], main_commit),
                              (["git", "status", "--porcelain"], "")):
        result = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        require(result.returncode == 0 and result.stdout.decode().strip() == expected,
                "exact clean source checkout is required")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gate-only", action="store_true")
    args = parser.parse_args()
    stage = "local-gate"
    # Preserve only the designated diagnostic stream outside SDK suppression.
    # The callback accepts constant operation names and no source-derived text.
    progress = CaptureProgress(sys.stderr)
    try:
        plan, activation, reconciliation = load_inputs()
        verify_checkout(plan.main_commit)
        require(Path(os.environ.get("GITHUB_WORKSPACE", "")).resolve() == ROOT, "runner checkout path drift")
        stage = "github-provenance"
        reject_previous_capture(plan.run_id)
        health_id, health_sha = plan.health_run_id, plan.health_sha256
        download_health(health_id, plan.main_commit, health_sha)
        if args.gate_only:
            print("VEVO_AA_QUALITY_SOURCE_LOCAL_GATE_OK:aws=false:source-read=false")
            return 0
        stage = "source-capture"
        # Imports/SDKs/legacy financial helpers cannot leak diagnostics. Their
        # data stays in memory; only our validated canonical artifact is written.
        logging.disable(sys.maxsize)
        with open(os.devnull, "w") as sink, redirect_stdout(sink), redirect_stderr(sink):
            import boto3
            session = boto3.Session(region_name=REGION)
            clients = {}

            def client(name):
                if name not in clients:
                    clients[name] = session.client(name)
                return clients[name]

            capture = collect(plan, client, activation, reconciliation,
                              {"workflow_run_id": health_id, "main_commit": plan.main_commit, "sha256": health_sha},
                              progress=progress)
        stage = "artifact-validation"
        # Full capture validator is shared with the future offline recorder.
        from scripts.validate_growthbook_aa_quality_capture import validate_capture
        validate_capture(capture, plan)
        temporary = Path(os.environ["RUNNER_TEMP"]).resolve()
        output_dir = temporary / ("vevo-aa-quality-" + plan.run_id)
        require(not output_dir.exists(), "source output already exists")
        output_dir.mkdir(mode=0o700)
        (output_dir / FILENAME).write_bytes(canonical_source_bytes(capture))
        print("VEVO_AA_QUALITY_SOURCE_CAPTURED:canonical=true:raw=false:mutation=none")
        return 0
    except Exception as error:
        print(f"VEVO_AA_QUALITY_SOURCE_STOPPED:stage={stage}:phase={progress.phase}:"
              f"code={safe_failure_code(error)}:raw=false", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
