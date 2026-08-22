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
from scripts.record_growthbook_natural_evidence import (
    ALLOWED_CHANGED_PATHS,
    EvidenceRecordingError,
    _changed_leaf_paths,
    canonical_evidence_bytes,
    load_validate_and_record,
    record_natural_evidence,
    validate_natural_evidence,
)
from scripts.verify_growthbook_natural_reconciliation import (
    EXPECTED_IMAGE_DIGEST,
    build_natural_reconciliation_evidence,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUN_ID = "32480000000"
MAIN_COMMIT = "a" * 40


def _evidence() -> dict[str, object]:
    result = {
        "task_id": "b" * 32,
        "private_ip": "172.31.10.20",
        "service": "vevo-growthbook-reconcile-preview",
        "runtime_path": "/app",
        "task_definition": "vevo-growthbook-reconcile-preview:4",
        "image_digest": EXPECTED_IMAGE_DIGEST,
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
    }
    return build_natural_reconciliation_evidence(
        result,
        verified_at=datetime(2026, 8, 23, 2, 0, tzinfo=timezone.utc),
        workflow_run_id=RUN_ID,
        main_commit=MAIN_COMMIT,
    )


class GrowthBookNaturalEvidenceRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = json.loads(
            workspace_validator.WORKSPACE_PATH.read_text(encoding="utf-8")
        )
        self.reporting = json.loads(
            workspace_validator.REPORTING_PATH.read_text(encoding="utf-8")
        )
        self.registry = json.loads(
            workspace_validator.REGISTRY_PATH.read_text(encoding="utf-8")
        )
        self.evidence = _evidence()
        self.evidence_sha256 = hashlib.sha256(
            canonical_evidence_bytes(self.evidence)
        ).hexdigest()

    def record(self, workspace: dict[str, object] | None = None) -> dict[str, object]:
        return record_natural_evidence(
            workspace or self.workspace,
            self.evidence,
            evidence_sha256=self.evidence_sha256,
            expected_workflow_run_id=RUN_ID,
            expected_main_commit=MAIN_COMMIT,
        )

    def test_accepts_exact_sanitized_canonical_evidence(self) -> None:
        validate_natural_evidence(
            self.evidence,
            expected_workflow_run_id=RUN_ID,
            expected_main_commit=MAIN_COMMIT,
        )
        self.assertEqual(
            self.evidence_sha256,
            hashlib.sha256(canonical_evidence_bytes(self.evidence)).hexdigest(),
        )

    def test_records_only_exact_reviewed_gate_paths(self) -> None:
        recorded = self.record()
        self.assertEqual(ALLOWED_CHANGED_PATHS, _changed_leaf_paths(self.workspace, recorded))
        recurring = recorded["reconciliation_checkpoint"]["recurring_schedule"]
        production = recorded["athena"]["production"]
        self.assertEqual(
            "verified_via_second_natural_run",
            recurring["first_natural_run_status"],
        )
        self.assertEqual(
            "passed_retention_recovery_run",
            recurring["natural_verifier_status"],
        )
        self.assertEqual(self.evidence_sha256, recurring["natural_evidence_artifact_sha256"])
        self.assertEqual(self.evidence, recurring["natural_verifier_evidence"])
        self.assertTrue(production["foundation_deployment_allowed"])
        self.assertEqual(0, recorded["workspace"]["production_allocation_percent"])
        self.assertFalse(production["credentials_created"])
        self.assertFalse(production["reader_provisioning_allowed"])
        self.assertFalse(production["growthbook_clone"]["clone_allowed"])

    def test_workspace_validator_accepts_only_fully_recorded_verified_state(self) -> None:
        recorded = self.record()
        with mock.patch.object(
            workspace_validator,
            "_load",
            side_effect=[recorded, self.reporting, self.registry],
        ):
            workspace_validator.validate()

        drifted = copy.deepcopy(recorded)
        drifted["reconciliation_checkpoint"]["recurring_schedule"][
            "natural_evidence_artifact_sha256"
        ] = "0" * 64
        with mock.patch.object(
            workspace_validator,
            "_load",
            side_effect=[drifted, self.reporting, self.registry],
        ):
            with self.assertRaisesRegex(AssertionError, "evidence SHA-256"):
                workspace_validator.validate()

    def test_rejects_noncanonical_downloaded_artifact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            evidence_path = temporary / "evidence.json"
            workspace_path = temporary / "workspace.json"
            evidence_path.write_text(json.dumps(self.evidence), encoding="utf-8")
            workspace_path.write_text(json.dumps(self.workspace), encoding="utf-8")
            with self.assertRaisesRegex(EvidenceRecordingError, "not canonical"):
                load_validate_and_record(
                    evidence_path=evidence_path,
                    workspace_path=workspace_path,
                    expected_workflow_run_id=RUN_ID,
                    expected_main_commit=MAIN_COMMIT,
                )

    def test_rejects_extra_pii_field_and_independent_identity_mismatch(self) -> None:
        drifted = copy.deepcopy(self.evidence)
        drifted["customer_email"] = "forbidden@example.test"
        with self.assertRaisesRegex(EvidenceRecordingError, "field set drift"):
            validate_natural_evidence(
                drifted,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )
        with self.assertRaisesRegex(EvidenceRecordingError, "workflow run ID mismatch"):
            validate_natural_evidence(
                self.evidence,
                expected_workflow_run_id="32480000001",
                expected_main_commit=MAIN_COMMIT,
            )

    def test_rejects_boolean_counts_and_non_private_runtime_ip(self) -> None:
        boolean_count = copy.deepcopy(self.evidence)
        boolean_count["reconciliation"]["raw_events"] = True
        with self.assertRaisesRegex(EvidenceRecordingError, "raw_events type drift"):
            validate_natural_evidence(
                boolean_count,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )
        public_ip = copy.deepcopy(self.evidence)
        public_ip["runtime"]["private_ip"] = "8.8.8.8"
        with self.assertRaisesRegex(EvidenceRecordingError, "private IP boundary"):
            validate_natural_evidence(
                public_ip,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )
        unbounded = copy.deepcopy(self.evidence)
        unbounded["reconciliation"]["raw_events"] = 50_001
        with self.assertRaisesRegex(EvidenceRecordingError, "raw-event bound"):
            validate_natural_evidence(
                unbounded,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )

    def test_is_idempotent_for_same_evidence_and_rejects_replacement(self) -> None:
        recorded = self.record()
        self.assertEqual(recorded, self.record(recorded))
        gate_drift = copy.deepcopy(recorded)
        gate_drift["athena"]["production"]["reader_provisioning_allowed"] = True
        with self.assertRaisesRegex(EvidenceRecordingError, "gate state drift"):
            self.record(gate_drift)
        replacement = copy.deepcopy(self.evidence)
        replacement["workflow_run_id"] = "32480000001"
        replacement_sha256 = hashlib.sha256(
            canonical_evidence_bytes(replacement)
        ).hexdigest()
        with self.assertRaisesRegex(EvidenceRecordingError, "different natural evidence"):
            record_natural_evidence(
                recorded,
                replacement,
                evidence_sha256=replacement_sha256,
                expected_workflow_run_id="32480000001",
                expected_main_commit=MAIN_COMMIT,
            )


if __name__ == "__main__":
    unittest.main()
