from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

from scripts import validate_growthbook_workspace as workspace_validator
from scripts.record_growthbook_foundation_evidence import (
    build_foundation_evidence,
    record_foundation_evidence,
)
from scripts.record_growthbook_natural_evidence import (
    canonical_evidence_bytes,
    record_natural_evidence,
)
from scripts.record_growthbook_production_reader_evidence import (
    ALLOWED_CHANGED_PATHS,
    ReaderEvidenceRecordingError,
    build_reader_evidence,
    load_validate_and_record,
    record_reader_evidence,
    validate_reader_evidence,
)
from scripts.verify_growthbook_natural_reconciliation import (
    EXPECTED_IMAGE_DIGEST as NATURAL_IMAGE_DIGEST,
    build_natural_reconciliation_evidence,
)


NATURAL_RUN_ID = "32480000000"
NATURAL_MAIN_COMMIT = "a" * 40
FOUNDATION_RUN_ID = "32480000001"
FOUNDATION_MAIN_COMMIT = "b" * 40
READER_RUN_ID = "32480000002"
READER_MAIN_COMMIT = "c" * 40


def _natural_evidence() -> dict[str, object]:
    return build_natural_reconciliation_evidence(
        {
            "task_id": "d" * 32,
            "private_ip": "172.31.10.20",
            "service": "vevo-growthbook-reconcile-preview",
            "runtime_path": "/app",
            "task_definition": "vevo-growthbook-reconcile-preview:4",
            "image_digest": NATURAL_IMAGE_DIGEST,
            "event_from": "2026-07-14",
            "event_through": "2026-08-22",
            "raw_events": 25,
            "device_facts": 5,
            "performance_facts": 8,
            "quality_reports": 2,
            "generated_published_counts_match": True,
            "dlq_empty": True,
            "alarms_clear": True,
            "source_schedule_enabled": True,
            "cloudtrail_scheduler_run_task_verified": True,
        },
        verified_at=datetime(2026, 8, 23, 2, 0, tzinfo=timezone.utc),
        workflow_run_id=NATURAL_RUN_ID,
        main_commit=NATURAL_MAIN_COMMIT,
    )


class GrowthBookProductionReaderEvidenceRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        workspace = json.loads(workspace_validator.WORKSPACE_PATH.read_text(encoding="utf-8"))
        self.reporting = json.loads(workspace_validator.REPORTING_PATH.read_text(encoding="utf-8"))
        self.registry = json.loads(workspace_validator.REGISTRY_PATH.read_text(encoding="utf-8"))
        natural = _natural_evidence()
        natural_sha = hashlib.sha256(canonical_evidence_bytes(natural)).hexdigest()
        workspace = record_natural_evidence(
            workspace,
            natural,
            evidence_sha256=natural_sha,
            expected_workflow_run_id=NATURAL_RUN_ID,
            expected_main_commit=NATURAL_MAIN_COMMIT,
        )
        foundation = build_foundation_evidence(
            verified_at=datetime(2026, 8, 22, 3, 0, tzinfo=timezone.utc),
            workflow_run_id=FOUNDATION_RUN_ID,
            main_commit=FOUNDATION_MAIN_COMMIT,
            natural_workflow_run_id=NATURAL_RUN_ID,
            natural_main_commit=NATURAL_MAIN_COMMIT,
            natural_evidence_sha256=natural_sha,
            host_task_id="e" * 32,
            host_private_ip="172.31.20.30",
            service_task_id="f" * 32,
            service_private_ip="172.31.20.31",
            task_definition="vevo-growthbook-collector-production:1",
        )
        foundation_sha = hashlib.sha256(canonical_evidence_bytes(foundation)).hexdigest()
        self.workspace = record_foundation_evidence(
            workspace,
            foundation,
            evidence_sha256=foundation_sha,
            expected_workflow_run_id=FOUNDATION_RUN_ID,
            expected_main_commit=FOUNDATION_MAIN_COMMIT,
        )
        self.evidence = build_reader_evidence(
            verified_at=datetime(2026, 8, 22, 4, 0, tzinfo=timezone.utc),
            workflow_run_id=READER_RUN_ID,
            main_commit=READER_MAIN_COMMIT,
            foundation_workflow_run_id=FOUNDATION_RUN_ID,
            foundation_main_commit=FOUNDATION_MAIN_COMMIT,
            foundation_sha256=foundation_sha,
            host_task_id="1" * 32,
            host_private_ip="172.31.21.40",
            task_definition="vevo-growthbook-collector-production:1",
            policy_arn=(
                "arn:aws:iam::919341186960:policy/"
                "vevo-growthbook-readonly-production"
            ),
            database="vevo_growthbook_production",
            workgroup="vevo-growthbook-readonly-production",
            s3_results_url=(
                "s3://vevo-growthbook-production-experimentdatabucket-abc123/"
                "athena-results/growthbook/"
            ),
        )
        self.evidence_sha = hashlib.sha256(
            canonical_evidence_bytes(self.evidence)
        ).hexdigest()

    def record(self, evidence: dict[str, object] | None = None) -> dict[str, object]:
        value = evidence or self.evidence
        return record_reader_evidence(
            self.workspace,
            value,
            evidence_sha256=hashlib.sha256(canonical_evidence_bytes(value)).hexdigest(),
            expected_workflow_run_id=READER_RUN_ID,
            expected_main_commit=READER_MAIN_COMMIT,
        )

    def test_builds_exact_no_secret_reader_evidence(self) -> None:
        validate_reader_evidence(
            self.evidence,
            expected_workflow_run_id=READER_RUN_ID,
            expected_main_commit=READER_MAIN_COMMIT,
            expected_foundation_run_id=FOUNDATION_RUN_ID,
            expected_foundation_main_commit=FOUNDATION_MAIN_COMMIT,
            expected_foundation_sha256=self.evidence[
                "foundation_evidence_provenance"
            ]["artifact_sha256"],
        )
        serialized = json.dumps(self.evidence, sort_keys=True).lower()
        for forbidden in (
            "accesskeyid",
            "secretaccesskey",
            "sessiontoken",
            "customer_email",
            "order_num",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_records_only_reader_and_clone_authorization_paths(self) -> None:
        recorded = self.record()
        from scripts.record_growthbook_natural_evidence import _changed_leaf_paths

        self.assertEqual(ALLOWED_CHANGED_PATHS, _changed_leaf_paths(self.workspace, recorded))
        production = recorded["athena"]["production"]
        self.assertTrue(production["credentials_created"])
        self.assertFalse(production["reader_provisioning_allowed"])
        self.assertTrue(production["growthbook_clone"]["clone_allowed"])
        self.assertEqual("not_started", production["growthbook_clone"]["mutation_status"])
        self.assertEqual(0, recorded["workspace"]["production_allocation_percent"])
        self.assertEqual("not_published", recorded["gtm_preview_workspace"]["publish_status"])

    def test_workspace_validator_accepts_only_hash_bound_reader_state(self) -> None:
        recorded = self.record()
        with mock.patch.object(
            workspace_validator,
            "_load",
            side_effect=[recorded, self.reporting, self.registry],
        ):
            workspace_validator.validate()

        drifted = copy.deepcopy(recorded)
        drifted["athena"]["production"]["reader_evidence_artifact_sha256"] = "0" * 64
        with mock.patch.object(
            workspace_validator,
            "_load",
            side_effect=[drifted, self.reporting, self.registry],
        ):
            with self.assertRaisesRegex(AssertionError, "reader evidence SHA-256"):
                workspace_validator.validate()

    def test_rejects_public_runtime_unsafe_or_wrong_foundation_provenance(self) -> None:
        public = copy.deepcopy(self.evidence)
        public["host_gate"]["private_ip"] = "8.8.8.8"
        with self.assertRaisesRegex(ReaderEvidenceRecordingError, "outside the VEVO VPC"):
            self.record(public)

        unsafe = copy.deepcopy(self.evidence)
        unsafe["safety"]["contains_access_key_id"] = True
        with self.assertRaisesRegex(ReaderEvidenceRecordingError, "safety boundary"):
            self.record(unsafe)

        wrong_foundation = copy.deepcopy(self.evidence)
        wrong_foundation["foundation_evidence_provenance"]["artifact_sha256"] = "0" * 64
        with self.assertRaisesRegex(ReaderEvidenceRecordingError, "foundation SHA-256"):
            self.record(wrong_foundation)

    def test_loader_requires_canonical_bytes_and_independent_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            evidence_path = root / "reader.json"
            workspace_path = root / "workspace.json"
            evidence_path.write_bytes(canonical_evidence_bytes(self.evidence))
            workspace_path.write_text(json.dumps(self.workspace), encoding="utf-8")
            recorded, digest = load_validate_and_record(
                evidence_path=evidence_path,
                workspace_path=workspace_path,
                expected_workflow_run_id=READER_RUN_ID,
                expected_main_commit=READER_MAIN_COMMIT,
            )
            self.assertEqual(self.evidence_sha, digest)
            self.assertTrue(recorded["athena"]["production"]["credentials_created"])

            evidence_path.write_text(json.dumps(self.evidence, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ReaderEvidenceRecordingError, "not canonical"):
                load_validate_and_record(
                    evidence_path=evidence_path,
                    workspace_path=workspace_path,
                    expected_workflow_run_id=READER_RUN_ID,
                    expected_main_commit=READER_MAIN_COMMIT,
                )


if __name__ == "__main__":
    unittest.main()
