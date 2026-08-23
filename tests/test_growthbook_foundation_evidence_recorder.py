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
    ALLOWED_CHANGED_PATHS,
    FoundationEvidenceRecordingError,
    build_foundation_evidence,
    load_validate_and_record,
    record_foundation_evidence,
    validate_foundation_evidence,
)
from scripts.record_growthbook_natural_evidence import (
    _changed_leaf_paths,
    canonical_evidence_bytes,
    record_natural_evidence,
)
from scripts.verify_growthbook_natural_reconciliation import (
    EXPECTED_IMAGE_DIGEST as NATURAL_IMAGE_DIGEST,
    build_natural_reconciliation_evidence,
)
from tests.growthbook_test_state import pending_natural_evidence_workspace


NATURAL_RUN_ID = "32480000000"
NATURAL_MAIN_COMMIT = "a" * 40
FOUNDATION_RUN_ID = "32480000001"
FOUNDATION_MAIN_COMMIT = "b" * 40


def _natural_evidence() -> dict[str, object]:
    return build_natural_reconciliation_evidence(
        {
            "task_id": "c" * 32,
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


class GrowthBookFoundationEvidenceRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        workspace = pending_natural_evidence_workspace(
            json.loads(workspace_validator.WORKSPACE_PATH.read_text(encoding="utf-8"))
        )
        self.reporting = json.loads(
            workspace_validator.REPORTING_PATH.read_text(encoding="utf-8")
        )
        self.registry = json.loads(
            workspace_validator.REGISTRY_PATH.read_text(encoding="utf-8")
        )
        self.natural_evidence = _natural_evidence()
        self.natural_sha256 = hashlib.sha256(
            canonical_evidence_bytes(self.natural_evidence)
        ).hexdigest()
        self.workspace = record_natural_evidence(
            workspace,
            self.natural_evidence,
            evidence_sha256=self.natural_sha256,
            expected_workflow_run_id=NATURAL_RUN_ID,
            expected_main_commit=NATURAL_MAIN_COMMIT,
        )
        self.evidence = build_foundation_evidence(
            verified_at=datetime(2026, 8, 22, 3, 0, tzinfo=timezone.utc),
            workflow_run_id=FOUNDATION_RUN_ID,
            main_commit=FOUNDATION_MAIN_COMMIT,
            natural_workflow_run_id=NATURAL_RUN_ID,
            natural_main_commit=NATURAL_MAIN_COMMIT,
            natural_evidence_sha256=self.natural_sha256,
            host_task_id="d" * 32,
            host_private_ip="172.31.20.30",
            service_task_id="e" * 32,
            service_private_ip="172.31.20.31",
            task_definition="vevo-growthbook-collector-production:1",
        )
        self.evidence_sha256 = hashlib.sha256(
            canonical_evidence_bytes(self.evidence)
        ).hexdigest()

    def record(self, workspace: dict[str, object] | None = None) -> dict[str, object]:
        return record_foundation_evidence(
            workspace or self.workspace,
            self.evidence,
            evidence_sha256=self.evidence_sha256,
            expected_workflow_run_id=FOUNDATION_RUN_ID,
            expected_main_commit=FOUNDATION_MAIN_COMMIT,
        )

    def test_accepts_exact_route_disabled_foundation_evidence(self) -> None:
        validate_foundation_evidence(
            self.evidence,
            expected_workflow_run_id=FOUNDATION_RUN_ID,
            expected_main_commit=FOUNDATION_MAIN_COMMIT,
            expected_natural_run_id=NATURAL_RUN_ID,
            expected_natural_main_commit=NATURAL_MAIN_COMMIT,
            expected_natural_sha256=self.natural_sha256,
        )
        self.assertFalse(self.evidence["deployment"]["public_route_enabled"])
        self.assertEqual(0, self.evidence["deployment"]["api_route_count"])
        self.assertTrue(self.evidence["deployment"]["event_bucket_empty"])
        serialized = json.dumps(self.evidence, sort_keys=True)
        for forbidden in (
            "CloudTrailEvent",
            "CloudWatchEvent",
            "AccessKeyId",
            "SecretAccessKey",
            "customer_email",
            "order_num",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_records_only_exact_reader_authorization_paths(self) -> None:
        recorded = self.record()
        self.assertEqual(ALLOWED_CHANGED_PATHS, _changed_leaf_paths(self.workspace, recorded))
        production = recorded["athena"]["production"]
        self.assertEqual(
            "route_disabled_foundation_deployed_verified", production["status"]
        )
        self.assertFalse(production["deployment_allowed"])
        self.assertFalse(production["foundation_deployment_allowed"])
        self.assertTrue(production["reader_provisioning_allowed"])
        self.assertFalse(production["credentials_created"])
        self.assertFalse(production["growthbook_clone"]["clone_allowed"])
        self.assertEqual(0, recorded["workspace"]["production_allocation_percent"])

    def test_workspace_validator_accepts_only_hash_bound_foundation_state(self) -> None:
        recorded = self.record()
        with mock.patch.object(
            workspace_validator,
            "_load",
            side_effect=[recorded, self.reporting, self.registry],
        ):
            workspace_validator.validate()

        drifted = copy.deepcopy(recorded)
        drifted["athena"]["production"]["foundation_evidence_artifact_sha256"] = (
            "0" * 64
        )
        with mock.patch.object(
            workspace_validator,
            "_load",
            side_effect=[drifted, self.reporting, self.registry],
        ):
            with self.assertRaisesRegex(AssertionError, "foundation evidence SHA-256"):
                workspace_validator.validate()

    def test_rejects_noncanonical_artifact_and_extra_pii_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            evidence_path = temporary / "foundation.json"
            workspace_path = temporary / "workspace.json"
            evidence_path.write_text(json.dumps(self.evidence), encoding="utf-8")
            workspace_path.write_text(json.dumps(self.workspace), encoding="utf-8")
            with self.assertRaisesRegex(
                FoundationEvidenceRecordingError, "not canonical"
            ):
                load_validate_and_record(
                    evidence_path=evidence_path,
                    workspace_path=workspace_path,
                    expected_workflow_run_id=FOUNDATION_RUN_ID,
                    expected_main_commit=FOUNDATION_MAIN_COMMIT,
                )

        pii = copy.deepcopy(self.evidence)
        pii["customer_email"] = "forbidden@example.test"
        with self.assertRaisesRegex(
            FoundationEvidenceRecordingError, "field set drift"
        ):
            validate_foundation_evidence(
                pii,
                expected_workflow_run_id=FOUNDATION_RUN_ID,
                expected_main_commit=FOUNDATION_MAIN_COMMIT,
                expected_natural_run_id=NATURAL_RUN_ID,
                expected_natural_main_commit=NATURAL_MAIN_COMMIT,
                expected_natural_sha256=self.natural_sha256,
            )

    def test_rejects_provenance_route_and_runtime_drift(self) -> None:
        provenance = copy.deepcopy(self.evidence)
        provenance["natural_evidence_provenance"]["workflow_run_id"] = "32480000009"
        with self.assertRaisesRegex(
            FoundationEvidenceRecordingError, "provenance mismatch"
        ):
            validate_foundation_evidence(
                provenance,
                expected_workflow_run_id=FOUNDATION_RUN_ID,
                expected_main_commit=FOUNDATION_MAIN_COMMIT,
                expected_natural_run_id=NATURAL_RUN_ID,
                expected_natural_main_commit=NATURAL_MAIN_COMMIT,
                expected_natural_sha256=self.natural_sha256,
            )
        routed = copy.deepcopy(self.evidence)
        routed["deployment"]["public_route_enabled"] = True
        with self.assertRaisesRegex(
            FoundationEvidenceRecordingError, "deployment boundary"
        ):
            validate_foundation_evidence(
                routed,
                expected_workflow_run_id=FOUNDATION_RUN_ID,
                expected_main_commit=FOUNDATION_MAIN_COMMIT,
                expected_natural_run_id=NATURAL_RUN_ID,
                expected_natural_main_commit=NATURAL_MAIN_COMMIT,
                expected_natural_sha256=self.natural_sha256,
            )
        public_ip = copy.deepcopy(self.evidence)
        public_ip["host_gate"]["private_ip"] = "8.8.8.8"
        with self.assertRaisesRegex(
            FoundationEvidenceRecordingError, "private IP boundary"
        ):
            validate_foundation_evidence(
                public_ip,
                expected_workflow_run_id=FOUNDATION_RUN_ID,
                expected_main_commit=FOUNDATION_MAIN_COMMIT,
                expected_natural_run_id=NATURAL_RUN_ID,
                expected_natural_main_commit=NATURAL_MAIN_COMMIT,
                expected_natural_sha256=self.natural_sha256,
            )

    def test_is_idempotent_and_rejects_replacement_or_partial_gate_drift(self) -> None:
        recorded = self.record()
        self.assertEqual(recorded, self.record(recorded))
        partial = copy.deepcopy(recorded)
        partial["athena"]["production"]["growthbook_clone"]["clone_allowed"] = True
        with self.assertRaisesRegex(
            FoundationEvidenceRecordingError, "gate state drift"
        ):
            self.record(partial)
        replacement = copy.deepcopy(self.evidence)
        replacement["workflow_run_id"] = "32480000002"
        replacement_sha256 = hashlib.sha256(
            canonical_evidence_bytes(replacement)
        ).hexdigest()
        with self.assertRaisesRegex(
            FoundationEvidenceRecordingError, "different foundation evidence"
        ):
            record_foundation_evidence(
                recorded,
                replacement,
                evidence_sha256=replacement_sha256,
                expected_workflow_run_id="32480000002",
                expected_main_commit=FOUNDATION_MAIN_COMMIT,
            )


if __name__ == "__main__":
    unittest.main()
