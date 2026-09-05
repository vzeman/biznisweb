"""Managed A/A consumer: verify the frozen capture, then audit its exact raw input.

No orders/API/token or rolling curated facts are read. AWS is imported only by
the explicitly gated runner CLI; injected-client helpers are testable offline.
Raw rows and SDK diagnostics must never leave runner memory.
"""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import logging
import os
import subprocess
import sys
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from growthbook_collector.handler import META_ID_RE, META_PLACEMENTS
from reporting_core.experiment_quality_source_io import read_stable_retained_raw_source
from reporting_core.experiments import ExperimentReceiptWindow, build_experiment_facts, load_experiment_build_config
from scripts.build_growthbook_aa_quality_source import canonical_source_bytes, _input_digest
from scripts.growthbook_aa_source_binding import (
    REPO, COMMIT, RUN, read_git_source_inputs, utc, validate_binding, verify_source_bundle,
)
from scripts.validate_growthbook_aa_measurement_window import validate_measurement_window

WORKFLOW = ".github/workflows/collect-vevo-growthbook-production-aa-evidence.yml"
SERVICE = "vevo-growthbook-collector-production"
EXPERIMENT = "vevo-sk-aa-001"


class ConsumerError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise ConsumerError(message)


def validate_consumer_gate(snapshot, activation, acceptance, reconciliation, environ):
    require(environ.get("GITHUB_ACTIONS") == "true"
            and environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and environ.get("GITHUB_REPOSITORY") == REPO
            and environ.get("GITHUB_REF") == "refs/heads/main"
            and environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and environ.get("GITHUB_WORKFLOW_REF") == f"{REPO}/{WORKFLOW}@refs/heads/main"
            and environ.get("GITHUB_RUN_ATTEMPT") == "1"
            and environ.get("CONFIRM_COLLECTION") == "true"
            and COMMIT.fullmatch(environ.get("GITHUB_SHA", ""))
            and RUN.fullmatch(environ.get("GITHUB_RUN_ID", "")), "managed consumer boundary is closed")
    validate_measurement_window(snapshot, activation, acceptance, reconciliation)
    automated = snapshot["automated_evidence"]
    require(snapshot["schema_version"] == 3 and snapshot["snapshot_build_allowed"] is False
            and automated["producer_allowed"] is True and automated["status"] == "not_recorded"
            and automated["window_status"] == "verified_complete_reconciled_production_aa",
            "source-bound automated producer gate is closed")
    binding = automated["quality_source"]
    validate_binding(binding, snapshot["measurement_window"])
    require(activation["status"] == "production_aa_running_activation_verified"
            and activation["growthbook"]["status"] == "running"
            and activation["growthbook"]["allocation_percent"] == 100
            and activation["collector"]["deployment_allowed"] is False,
            "activated A/A identity is not recorded")
    return binding


def github_bytes(path):
    try:
        result = subprocess.run(["gh", "api", path], stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, timeout=90)
        require(result.returncode == 0 and 0 < len(result.stdout) <= 2 * 1024 * 1024,
                "bounded GitHub read failed")
        return result.stdout
    except (OSError, subprocess.TimeoutExpired):
        raise ConsumerError("bounded GitHub read failed") from None


def github_json(path):
    return json.loads(github_bytes(path))


def verify_recorded_source(snapshot, *, read_json=github_json, read_zip=github_bytes,
                           read_inputs=read_git_source_inputs):
    binding = snapshot["automated_evidence"]["quality_source"]
    validate_binding(binding, snapshot["measurement_window"])
    inputs = read_inputs(binding["main_commit"])
    bundle = {}
    for kind, run_id, artifact_id in (
        ("source", binding["run_id"], binding["artifact_id"]),
        ("health", binding["health_run_id"], binding["health_artifact_id"]),
    ):
        bundle[kind + "_run"] = read_json(f"repos/{REPO}/actions/runs/{run_id}")
        bundle[kind + "_artifacts"] = read_json(f"repos/{REPO}/actions/runs/{run_id}/artifacts?per_page=100")
        bundle[kind + "_zip"] = read_zip(f"repos/{REPO}/actions/artifacts/{artifact_id}/zip")
    verified = verify_source_bundle(**bundle, source_inputs=inputs,
        expected_workflow_run_id=binding["run_id"], expected_main_commit=binding["main_commit"],
        expected_evidence_sha256=binding["json_sha256"], expected_health_run_id=binding["health_run_id"],
        expected_health_sha256=binding["health_json_sha256"])
    require(verified.binding == binding, "independent source binding differs from recorded source")
    require(verified.snapshot["measurement_window"] == snapshot["measurement_window"],
            "frozen measurement history changed after source capture")
    return verified.capture


def reject_previous_collection(current_run_id, *, read_json=github_json):
    for page in range(1, 101):
        result = read_json(f"repos/{REPO}/actions/workflows/{Path(WORKFLOW).name}/runs"
                           f"?branch=main&event=workflow_dispatch&per_page=100&page={page}")
        runs = result.get("workflow_runs")
        require(isinstance(runs, list), "automated run history unavailable")
        for run in runs:
            if str(run.get("id")) == current_run_id:
                continue
            require(run.get("status") == "completed", "another automated collection is active")
            require(run.get("conclusion") != "success", "consume existing automated evidence")
            prior_id = str(run.get("id", ""))
            require(RUN.fullmatch(prior_id), "automated history run identity invalid")
            listing = read_json(f"repos/{REPO}/actions/runs/{prior_id}/artifacts?per_page=100")
            require(listing.get("total_count") == 0, "prior automated artifact requires recovery")
        if len(runs) < 100:
            return
    raise ConsumerError("automated run history exceeds the bound")


def frozen_meta_audit(raw_rows, capture, config):
    """Rebuild only assignment/Meta facts; never substitute zero-order outcomes.

    Eligibility in the shared calculator depends on assignment contamination,
    not on the order join. Source quality/outcomes are consumed unchanged.
    """
    rows = list(raw_rows)
    source = capture["source"]
    require(_input_digest(rows) == source["provenance"]["raw_extract_sha256"],
            "retained input differs from the frozen source capture")
    window = ExperimentReceiptWindow(**{key: utc(value) for key, value in source["window"].items()
                                       if key in {"context_from_utc", "from_utc", "through_utc"}})
    aa_config = replace(config, expected_variation_weights={EXPERIMENT: config.expected_variation_weights[EXPERIMENT]})
    facts = build_experiment_facts(rows, (), config=aa_config,
        generated_at=utc(source["provenance"]["generated_at_utc"]), measurement_window=window)
    require(len(facts.quality_reports) == 1, "frozen assignment quality is missing")
    quality = facts.quality_reports[0]
    for key in ("eligible_device_count", "contaminated_device_count", "raw_event_count",
                "unique_event_count", "duplicate_event_count"):
        require(quality[key] == source["quality"][key], "frozen assignment/source parity mismatch")
    eligible = [row for row in facts.device_facts if row["eligible"] == 1 and row["contaminated"] == 0]
    fields = ("meta_campaign_id", "meta_adset_id", "meta_ad_id", "meta_placement")
    invalid = 0
    for row in eligible:
        ids_valid = all(row[key] is None or (isinstance(row[key], str) and META_ID_RE.fullmatch(row[key]))
                        for key in fields[:3])
        placement_valid = row["meta_placement"] is None or row["meta_placement"] in META_PLACEMENTS
        invalid += int(not (ids_valid and placement_valid))
    return {
        "meta_exposure_count": sum(any(row[key] is not None for key in fields) for row in eligible),
        "complete_stable_dimension_exposure_count": sum(all(row[key] is not None for key in fields) for row in eligible),
        "invalid_dimension_row_count": invalid,
    }


def read_frozen_meta(s3, *, bucket, capture, config):
    window = capture["source"]["window"]
    retained = read_stable_retained_raw_source(s3, bucket=bucket,
        context_from_utc=utc(window["context_from_utc"]), through_utc=utc(window["through_utc"]))
    return frozen_meta_audit(retained.rows, capture, config)


def validate_activated_runtime(service, task, definition, activation, *, task_definition_arn):
    recorded = activation["collector"]
    require(recorded["service"] == SERVICE and recorded["runtime_path"] == "/app"
            and recorded["deployment_allowed"] is False and recorded["public_route_enabled"] is True
            and recorded["host_gate_task_id"] and recorded["host_gate_private_ip"]
            and len(recorded["evidence_sha256"]) == 64, "activated collector localhost proof missing")
    require(service.get("status") == "ACTIVE" and service.get("desiredCount") == 1
            and service.get("runningCount") == 1 and service.get("pendingCount") == 0
            and service.get("taskDefinition") == task_definition_arn
            and service.get("enableExecuteCommand") is False, "activated collector service drift")
    require(task.get("taskDefinitionArn") == task_definition_arn and task.get("lastStatus") == "RUNNING"
            and task.get("launchType") == "FARGATE" and task.get("group") == f"service:{SERVICE}"
            and len(task.get("containers", [])) == 1, "activated collector task drift")
    containers = definition.get("containerDefinitions") or []
    require(definition.get("taskDefinitionArn") == task_definition_arn
            and task_definition_arn.rsplit("/", 1)[-1] == recorded["task_definition"]
            and len(containers) == 1 and containers[0].get("name") == "collector"
            and containers[0].get("readonlyRootFilesystem") is True
            and containers[0].get("user") == "10001:10001"
            and str(containers[0].get("image", "")).endswith("@" + recorded["image_digest"]),
            "activated collector definition/hardening drift")
    container = task["containers"][0]
    require(container.get("name") == "collector" and container.get("imageDigest") == recorded["image_digest"],
            "activated collector image drift")
    addresses = [row["value"] for attachment in task.get("attachments", [])
                 for row in attachment.get("details", []) if row.get("name") == "privateIPv4Address"]
    require(len(addresses) == 1 and ipaddress.ip_address(addresses[0]) in ipaddress.ip_network("172.31.0.0/16"),
            "activated collector private network drift")
    return {"instance_id": "N/A:Fargate", "private_ip": addresses[0], "service": SERVICE,
            "path": "/app", "task_id": task["taskArn"].rsplit("/", 1)[-1],
            "image_digest": recorded["image_digest"], "stack_name": "vevo-growthbook-production",
            "database": "vevo_growthbook_production"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("verify-source", "frozen-meta"))
    args = parser.parse_args()
    try:
        from scripts.record_growthbook_aa_evidence_gates import EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED
        require(EXACT_WINDOW_SOURCE_CAPTURE_SUPPORTED is True, "consumer support is not reviewed")
        directory = ROOT / "projects/vevo"
        load = lambda name: json.loads((directory / name).read_bytes())
        snapshot = load("growthbook_aa_snapshot.json")
        binding = validate_consumer_gate(snapshot, load("growthbook_production_aa_activation.json"),
            load("growthbook_aa_acceptance.json"), load("growthbook_production_reconciliation_deploy_evidence.json"), os.environ)
        require(Path(os.environ["GITHUB_WORKSPACE"]).resolve() == ROOT, "consumer checkout path drift")
        for command, expected in ((["git", "rev-parse", "HEAD"], os.environ["GITHUB_SHA"]),
                                  (["git", "status", "--porcelain"], "")):
            result = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=30)
            require(result.returncode == 0 and result.stdout.decode().strip() == expected, "consumer checkout drift")
        temp = Path(os.environ["RUNNER_TEMP"]).resolve() / f"vevo-growthbook-aa-automated-{os.environ['GITHUB_RUN_ID']}"
        require(Path(os.environ["TEMP_EVIDENCE_DIR"]).resolve() == temp and temp.is_dir(), "consumer temporary path drift")
        if args.action == "verify-source":
            reject_previous_collection(os.environ["GITHUB_RUN_ID"])
            capture = verify_recorded_source(snapshot, read_json=github_json, read_zip=github_bytes,
                                             read_inputs=read_git_source_inputs)
            (temp / "verified-source.json").write_bytes(canonical_source_bytes(capture))
            (temp / "reporting-quality.json").write_bytes(canonical_source_bytes(capture["source"]["quality"]))
        else:
            raw = (temp / "verified-source.json").read_bytes()
            require(len(raw) <= 1024 * 1024 and hashlib.sha256(raw).hexdigest() == binding["json_sha256"],
                    "prepared source hash drift")
            capture = json.loads(raw)
            logging.disable(sys.maxsize)
            with open(os.devnull, "w") as sink, redirect_stdout(sink), redirect_stderr(sink):
                import boto3
                session = boto3.Session(region_name="eu-central-1")
                audit = read_frozen_meta(session.client("s3"), bucket=os.environ["PRODUCTION_EVENT_BUCKET"],
                    capture=capture, config=load_experiment_build_config(directory / "growthbook_reporting.json"))
            (temp / "frozen-meta.json").write_bytes(canonical_source_bytes(audit))
        print("VEVO_AA_CONSUMER_OK:stage=" + args.action + ":raw=false:mutations=false")
        return 0
    except Exception:
        print("VEVO_AA_CONSUMER_BLOCKED:stage=" + args.action + ":details=suppressed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
