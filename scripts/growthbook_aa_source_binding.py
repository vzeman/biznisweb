"""Offline, independent provenance for one managed A/A quality capture.

GitHub run and sole-artifact metadata must be downloaded independently from the
authenticated API. Expected run/main/JSON hashes are supplied by the reviewer,
not copied from the source's claims. Source inputs come from the exact local Git
commit, never the later working-tree manifest. No network or AWS client exists.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import stat
import subprocess
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from reporting_core.experiments import ExperimentReceiptWindow
from scripts.build_growthbook_aa_quality_source import WORKFLOW, canonical_source_bytes
from scripts.validate_growthbook_aa_quality_capture import validate_capture_bytes
from scripts.validate_growthbook_aa_infra_health_evidence import validate_health_evidence

ROOT = Path(__file__).resolve().parents[1]
REPO = "vzeman/biznisweb"
ARTIFACT = "vevo-growthbook-aa-quality-source"
HEALTH_WORKFLOW = ".github/workflows/monitor-vevo-growthbook-production-aa-infra.yml"
HEALTH_ARTIFACT = "vevo-growthbook-production-aa-infra-health"
SHA = re.compile(r"^[a-f0-9]{64}$")
COMMIT = re.compile(r"^[a-f0-9]{40}$")
RUN = re.compile(r"^[1-9][0-9]{5,19}$")
INPUT_FILES = (
    "growthbook_aa_snapshot.json", "growthbook_workspace.json",
    "growthbook_production_aa_activation.json", "growthbook_aa_acceptance.json",
    "growthbook_production_reconciliation_deploy_evidence.json",
)
BINDING_KEYS = {
    "schema_version", "workflow", "artifact_name", "file_name", "run_id", "main_commit",
    "artifact_id", "zip_sha256", "json_sha256", "snapshot_sha256", "foundation_sha256",
    "checkpoint_sha256", "health_run_id", "health_artifact_id", "health_zip_sha256",
    "health_json_sha256", "context_from_utc", "from_utc", "through_utc", "captured_at_utc",
}


class SourceBindingError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise SourceBindingError(message)


def utc(value):
    require(isinstance(value, str) and re.fullmatch(r"20[2-9][0-9]-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value),
            "source timestamp format drift")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate_binding(binding, window):
    require(isinstance(binding, dict) and set(binding) == BINDING_KEYS, "source binding field set drift")
    require(type(binding["schema_version"]) is int and binding["schema_version"] == 1
            and binding["workflow"] == WORKFLOW and binding["artifact_name"] == ARTIFACT
            and binding["file_name"] == ARTIFACT + ".json", "source binding identity drift")
    for key in ("run_id", "artifact_id", "health_run_id", "health_artifact_id"):
        require(isinstance(binding[key], str) and RUN.fullmatch(binding[key]), "source binding ID invalid")
    require(binding["run_id"] != binding["health_run_id"], "source and health runs must be independent")
    require(isinstance(binding["main_commit"], str) and COMMIT.fullmatch(binding["main_commit"]), "source main invalid")
    for key in BINDING_KEYS:
        if key.endswith("sha256"):
            require(isinstance(binding[key], str) and SHA.fullmatch(binding[key]), "source binding digest invalid")
    require(window["resolution_status"] == "resolved"
            and binding["from_utc"] == window["from_utc"]
            and binding["through_utc"] == window["resolved_through_utc"]
            and binding["checkpoint_sha256"] == window["checkpoint_history"][-1]["evidence_sha256"],
            "source binding differs from the fixed checkpoint")
    require(utc(binding["context_from_utc"]) < utc(binding["from_utc"])
            < utc(binding["through_utc"]) <= utc(binding["captured_at_utc"]), "source binding chronology drift")


def read_git_source_inputs(commit, *, repository=ROOT):
    """Read only named blobs from an independently selected ancestor commit.

    No fetch, checkout, stash, environment read or working-tree fallback. A
    shallow clone without the source commit fails and must be fetched normally.
    """
    require(isinstance(commit, str) and COMMIT.fullmatch(commit), "source commit invalid")
    repository = Path(repository).resolve()

    def git(*args):
        try:
            result = subprocess.run(["git", "--no-replace-objects", *args], cwd=repository, stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, timeout=30)
        except (OSError, subprocess.TimeoutExpired):
            raise SourceBindingError("exact source Git object is unavailable") from None
        require(result.returncode == 0 and len(result.stdout) <= 4 * 1024 * 1024,
                "exact source Git object is unavailable")
        return result.stdout

    require(Path(git("rev-parse", "--show-toplevel").decode().strip()).resolve() == repository,
            "source Git repository root drift")
    require(git("rev-parse", commit + "^{commit}").decode().strip() == commit, "source Git commit drift")
    git("merge-base", "--is-ancestor", commit, "HEAD")
    return {name: git("show", f"{commit}:projects/vevo/{name}") for name in INPUT_FILES}


def verified_archive(blob, run, listing, *, run_id, main_commit, workflow, artifact_name, json_sha256):
    """Validate independently supplied API metadata and ZIP without extraction."""
    require(RUN.fullmatch(run_id) and COMMIT.fullmatch(main_commit) and SHA.fullmatch(json_sha256),
            "independent source provenance invalid")
    require(str(run.get("id")) == run_id and run.get("head_sha") == main_commit
            and run.get("head_branch") == "main" and run.get("path") == workflow
            and run.get("status") == "completed" and run.get("conclusion") == "success"
            and run.get("repository", {}).get("full_name") == REPO
            and run.get("head_repository", {}).get("full_name") == REPO,
            "successful source main-run provenance mismatch")
    require(run.get("event") in ({"workflow_dispatch"} if workflow == WORKFLOW else {"workflow_dispatch", "schedule"}),
            "source workflow event drift")
    if workflow == WORKFLOW:
        require(type(run.get("run_attempt")) is int and run["run_attempt"] == 1, "source recapture attempt rejected")
    require(listing.get("total_count") == 1 and len(listing.get("artifacts", [])) == 1,
            "source must have exactly one artifact")
    artifact = listing["artifacts"][0]
    require(artifact.get("name") == artifact_name and artifact.get("expired") is False
            and RUN.fullmatch(str(artifact.get("id", ""))), "source artifact identity drift")
    owner = artifact.get("workflow_run") or {}
    require(str(owner.get("id")) == run_id and owner.get("head_sha") == main_commit
            and owner.get("head_branch") == "main", "artifact run ownership drift")
    require(isinstance(blob, bytes) and 0 < len(blob) <= 2 * 1024 * 1024, "source ZIP size invalid")
    zip_sha = hashlib.sha256(blob).hexdigest()
    require(artifact.get("digest") == "sha256:" + zip_sha, "GitHub source ZIP digest mismatch")
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as archive:
            rows = archive.infolist()
            require(len(rows) == 1 and rows[0].filename == artifact_name + ".json"
                    and not rows[0].is_dir() and not stat.S_ISLNK(rows[0].external_attr >> 16)
                    and 0 < rows[0].file_size <= 1024 * 1024, "source ZIP contents invalid")
            raw = archive.read(rows[0])
        payload = json.loads(raw)
        require(raw == canonical_source_bytes(payload) and hashlib.sha256(raw).hexdigest() == json_sha256,
                "source canonical JSON digest mismatch")
    except (zipfile.BadZipFile, UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise SourceBindingError("source archive is invalid") from exc
    return raw, str(artifact["id"]), zip_sha


@dataclass(frozen=True, repr=False)
class VerifiedSource:
    capture: dict
    snapshot: dict
    binding: dict


def verify_source_bundle(*, source_zip, source_run, source_artifacts, health_zip, health_run,
                         health_artifacts, source_inputs, expected_workflow_run_id,
                         expected_main_commit, expected_evidence_sha256,
                         expected_health_run_id, expected_health_sha256):
    """Pure verification; source_inputs must originate at expected_main_commit."""
    require(set(source_inputs) == set(INPUT_FILES), "source checkout input set drift")
    source_raw, artifact_id, zip_sha = verified_archive(source_zip, source_run, source_artifacts,
        run_id=expected_workflow_run_id, main_commit=expected_main_commit, workflow=WORKFLOW,
        artifact_name=ARTIFACT, json_sha256=expected_evidence_sha256)
    health_raw, health_artifact_id, health_zip_sha = verified_archive(health_zip, health_run, health_artifacts,
        run_id=expected_health_run_id, main_commit=expected_main_commit, workflow=HEALTH_WORKFLOW,
        artifact_name=HEALTH_ARTIFACT, json_sha256=expected_health_sha256)
    loaded = {name: json.loads(raw) for name, raw in source_inputs.items()}
    snapshot = loaded["growthbook_aa_snapshot.json"]
    from scripts.validate_growthbook_aa_measurement_window import validate_measurement_window
    validate_measurement_window(snapshot, loaded["growthbook_production_aa_activation.json"],
                                loaded["growthbook_aa_acceptance.json"],
                                loaded["growthbook_production_reconciliation_deploy_evidence.json"])
    window = snapshot["measurement_window"]
    auto = snapshot["automated_evidence"]
    require(snapshot["schema_version"] == 2 and window["resolution_status"] == "resolved"
            and snapshot["snapshot_build_allowed"] is False and auto["producer_allowed"] is False
            and auto["status"] == "not_recorded" and auto["quality_report_status"] == "not_recorded"
            and auto["quality_report_key"] is None and auto["quality_report_sha256"] is None,
            "source commit must precede the source-binding transition")
    foundation = loaded["growthbook_workspace.json"]["athena"]["production"]["successful_foundation_deployment"]
    require(foundation["status"] == "passed" and foundation["deployment"]["event_bucket_empty"] is True
            and foundation["deployment"]["public_route_enabled"] is False, "empty source foundation missing")
    observed = utc(foundation["verified_at_utc"])
    context = observed.replace(hour=0, minute=0, second=0, microsecond=0)
    require(observed < utc(window["from_utc"]), "source foundation chronology drift")
    plan = SimpleNamespace(window=ExperimentReceiptWindow(context, utc(window["from_utc"]), utc(window["resolved_through_utc"])),
        eligible=window["resolved_eligible_devices"], snapshot_sha256=hashlib.sha256(source_inputs["growthbook_aa_snapshot.json"]).hexdigest(),
        checkpoint_sha256=window["checkpoint_history"][-1]["evidence_sha256"],
        foundation_sha256=hashlib.sha256(canonical_source_bytes(foundation)).hexdigest(),
        main_commit=expected_main_commit, run_id=expected_workflow_run_id,
        health_run_id=expected_health_run_id, health_sha256=expected_health_sha256)
    capture = validate_capture_bytes(source_raw, plan, expected_sha256=expected_evidence_sha256)
    health = json.loads(health_raw)
    deploy_raw = source_inputs["growthbook_production_reconciliation_deploy_evidence.json"]
    validate_health_evidence(health, json.loads(deploy_raw), deploy_evidence_bytes=deploy_raw)
    require(health["provenance"]["workflow_run_id"] == expected_health_run_id
            and health["provenance"]["main_commit"] == expected_main_commit,
            "health JSON provenance mismatch")
    started = utc(capture["acquisition"]["started_at_utc"])
    completed = utc(capture["acquisition"]["completed_at_utc"])
    require(utc(health_run["run_started_at"]) <= utc(health["observed_at_utc"])
            <= utc(health_run["updated_at"]) <= started,
            "health must complete independently before source acquisition")
    local = started.astimezone(ZoneInfo("Europe/Bratislava"))
    due = local.replace(hour=3, minute=45, second=0, microsecond=0)
    if local < due:
        due -= timedelta(days=1)
    require(health["phase"]["status"] == "natural_reconciliation_verified"
            and health["phase"]["checked_due_local"] == due.isoformat(timespec="seconds")
            and timedelta(0) <= started - utc(health["observed_at_utc"]) <= timedelta(hours=6),
            "source health was not fresh at acquisition time")
    # API run timestamps are independent of timestamps inside the capture.
    require(utc(source_run["run_started_at"]) <= started <= completed <= utc(source_run["updated_at"]),
            "capture interval is outside its successful run")
    binding = {"schema_version": 1, "workflow": WORKFLOW, "artifact_name": ARTIFACT, "file_name": ARTIFACT + ".json",
        "run_id": plan.run_id, "main_commit": plan.main_commit, "artifact_id": artifact_id,
        "zip_sha256": zip_sha, "json_sha256": expected_evidence_sha256, "snapshot_sha256": plan.snapshot_sha256,
        "foundation_sha256": plan.foundation_sha256, "checkpoint_sha256": plan.checkpoint_sha256,
        "health_run_id": expected_health_run_id, "health_artifact_id": health_artifact_id,
        "health_zip_sha256": health_zip_sha, "health_json_sha256": expected_health_sha256,
        "context_from_utc": context.isoformat().replace("+00:00", "Z"), "from_utc": window["from_utc"],
        "through_utc": window["resolved_through_utc"], "captured_at_utc": capture["acquisition"]["completed_at_utc"]}
    validate_binding(binding, window)
    return VerifiedSource(capture, snapshot, binding)
