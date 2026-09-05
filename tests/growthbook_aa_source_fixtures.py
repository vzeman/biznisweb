"""Synthetic archive/API provenance shared by offline source consumers."""
import copy
import hashlib
import io
import zipfile
from unittest.mock import patch
from datetime import UTC, datetime


def source_bundle():
    # Lazy imports avoid lifecycle test-fixture import cycles.
    from scripts import collect_growthbook_aa_quality_source as source
    from scripts.build_growthbook_aa_quality_source import canonical_source_bytes
    from scripts.record_growthbook_natural_evidence import canonical_evidence_bytes as canonical_health_bytes
    from tests.test_growthbook_managed_quality_source import inputs, capture_fixture, ENV
    from tests.test_growthbook_aa_evidence_gate_recorder import quality_report
    from tests.test_growthbook_aa_infra_health_evidence import health_evidence
    from scripts.growthbook_aa_source_binding import INPUT_FILES
    with patch.object(source, "now_utc", return_value=datetime(2026, 9, 5, 8, tzinfo=UTC)), \
         patch("tests.test_growthbook_pipeline.uid", return_value="12345678-1234-4234-8234-123456789abc"):
        args = inputs()
        _, capture = capture_fixture()
    at = "2026-09-05T08:00:00Z"
    capture["source"]["quality"] = quality_report()
    capture["source"]["quality"]["facts_generated_at"] = at
    capture["source"]["provenance"]["generated_at_utc"] = at
    capture["acquisition"].update(started_at_utc=at, completed_at_utc=at)
    health = health_evidence(post_run=True)
    health["observed_at_utc"] = "2026-09-05T07:58:00Z"
    health["phase"]["checked_due_local"] = "2026-09-05T03:45:00+02:00"
    health["provenance"].update(workflow_run_id=ENV["HEALTH_RUN_ID"], main_commit=ENV["GITHUB_SHA"])
    health_raw = canonical_health_bytes(health)
    health_sha = hashlib.sha256(health_raw).hexdigest()
    capture["acquisition"]["health"]["sha256"] = health_sha
    raw = canonical_source_bytes(capture)

    def archive(payload, name, run_id, artifact_id, workflow):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zipped:
            zipped.writestr(zipfile.ZipInfo(name + ".json", date_time=(2026, 9, 5, 8, 0, 0)), payload)
        blob = buffer.getvalue()
        run = {"id": int(run_id), "head_sha": ENV["GITHUB_SHA"], "head_branch": "main", "path": workflow,
               "status": "completed", "conclusion": "success", "event": "workflow_dispatch", "run_attempt": 1,
               "repository": {"full_name": source.REPO}, "head_repository": {"full_name": source.REPO},
               "run_started_at": "2026-09-05T07:59:00Z", "updated_at": "2026-09-05T08:01:00Z"}
        artifacts = {"total_count": 1, "artifacts": [{"id": artifact_id, "name": name, "expired": False,
            "digest": "sha256:" + hashlib.sha256(blob).hexdigest(),
            "workflow_run": {"id": int(run_id), "head_sha": ENV["GITHUB_SHA"], "head_branch": "main"}}]}
        return blob, run, artifacts

    source_zip, source_run, source_artifacts = archive(raw, source.ARTIFACT, ENV["GITHUB_RUN_ID"], 111111111, source.WORKFLOW)
    health_zip, health_run, health_artifacts = archive(health_raw, source.HEALTH_ARTIFACT, ENV["HEALTH_RUN_ID"], 222222222, source.HEALTH_WORKFLOW)
    health_run.update(run_started_at="2026-09-05T07:57:00Z", updated_at="2026-09-05T07:59:00Z")
    source_inputs = dict(zip(INPUT_FILES, (args[0], *(canonical_source_bytes(value) for value in args[1:5])), strict=True))
    source_inputs[INPUT_FILES[-1]] = (source.ROOT / "projects/vevo" / INPUT_FILES[-1]).read_bytes()
    return dict(source_zip=source_zip, source_run=source_run, source_artifacts=source_artifacts,
        health_zip=health_zip, health_run=health_run, health_artifacts=health_artifacts, source_inputs=copy.deepcopy(source_inputs),
        expected_workflow_run_id=ENV["GITHUB_RUN_ID"], expected_main_commit=ENV["GITHUB_SHA"],
        expected_evidence_sha256=hashlib.sha256(raw).hexdigest(), expected_health_run_id=ENV["HEALTH_RUN_ID"],
        expected_health_sha256=health_sha)
