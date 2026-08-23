from __future__ import annotations

import copy
import hashlib
import json
import unittest
from datetime import datetime, timezone
from unittest import mock

from scripts import validate_growthbook_workspace as workspace_validator
from scripts.record_growthbook_foundation_evidence import (
    RECOVERY_ALLOWED_CHANGED_PATHS,
    FoundationEvidenceRecordingError,
    build_foundation_recovery_evidence,
    record_foundation_evidence,
    validate_foundation_evidence,
)
from scripts.record_growthbook_natural_evidence import (
    _changed_leaf_paths,
    canonical_evidence_bytes,
    record_natural_evidence,
)
from scripts.validate_growthbook_changeset import EXPECTED_CREATE_RESOURCES
from scripts.verify_growthbook_foundation_recovery import (
    EXPECTED_CREATION_MAIN_COMMIT,
    EXPECTED_CREATION_RUN_ID,
    EXPECTED_CREATION_WORKFLOW,
    EXPECTED_JOB,
    EXPECTED_STEP_CONCLUSIONS,
    FoundationRecoveryVerificationError,
    validate_creation_run,
    validate_live_stack_resources,
)
from scripts.verify_growthbook_natural_reconciliation import (
    EXPECTED_IMAGE_DIGEST as NATURAL_IMAGE_DIGEST,
    build_natural_reconciliation_evidence,
)
from tests.growthbook_test_state import pending_natural_evidence_workspace


NATURAL_RUN_ID = "32480000000"
NATURAL_MAIN_COMMIT = "a" * 40
RECOVERY_RUN_ID = "32613000000"
RECOVERY_MAIN_COMMIT = "b" * 40


def _creation_run() -> dict[str, object]:
    return {
        "id": EXPECTED_CREATION_RUN_ID,
        "event": "workflow_dispatch",
        "status": "completed",
        "conclusion": "failure",
        "head_branch": "main",
        "head_sha": EXPECTED_CREATION_MAIN_COMMIT,
        "path": EXPECTED_CREATION_WORKFLOW,
        "repository": {"full_name": "vzeman/biznisweb"},
    }


def _creation_jobs() -> dict[str, object]:
    return {
        "jobs": [
            {
                "name": EXPECTED_JOB,
                "status": "completed",
                "conclusion": "failure",
                "steps": [
                    {"name": name, "conclusion": conclusion}
                    for name, conclusion in EXPECTED_STEP_CONCLUSIONS.items()
                ],
            }
        ]
    }


def _stack_resources() -> dict[str, object]:
    return {
        "StackResourceSummaries": [
            {
                "LogicalResourceId": logical_id,
                "ResourceType": resource_type,
                "ResourceStatus": "CREATE_COMPLETE",
            }
            for logical_id, resource_type in EXPECTED_CREATE_RESOURCES.items()
        ]
    }


class GrowthBookFoundationRecoveryTests(unittest.TestCase):
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
        natural = build_natural_reconciliation_evidence(
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
        self.natural_sha = hashlib.sha256(canonical_evidence_bytes(natural)).hexdigest()
        self.workspace = record_natural_evidence(
            workspace,
            natural,
            evidence_sha256=self.natural_sha,
            expected_workflow_run_id=NATURAL_RUN_ID,
            expected_main_commit=NATURAL_MAIN_COMMIT,
        )
        self.evidence = build_foundation_recovery_evidence(
            verified_at=datetime(2026, 8, 23, 3, 0, tzinfo=timezone.utc),
            workflow_run_id=RECOVERY_RUN_ID,
            main_commit=RECOVERY_MAIN_COMMIT,
            natural_workflow_run_id=NATURAL_RUN_ID,
            natural_main_commit=NATURAL_MAIN_COMMIT,
            natural_evidence_sha256=self.natural_sha,
            host_task_id="d" * 32,
            host_private_ip="172.31.20.30",
            service_task_id="e" * 32,
            service_private_ip="172.31.20.31",
            task_definition="vevo-growthbook-collector-production:1",
        )

    def _validate(self, evidence: dict[str, object]) -> None:
        validate_foundation_evidence(
            evidence,
            expected_workflow_run_id=RECOVERY_RUN_ID,
            expected_main_commit=RECOVERY_MAIN_COMMIT,
            expected_natural_run_id=NATURAL_RUN_ID,
            expected_natural_main_commit=NATURAL_MAIN_COMMIT,
            expected_natural_sha256=self.natural_sha,
        )

    def test_accepts_exact_creation_run_and_live_resource_provenance(self) -> None:
        validate_creation_run(_creation_run(), _creation_jobs())
        validate_live_stack_resources(_stack_resources())

    def test_rejects_creation_step_or_live_route_drift(self) -> None:
        jobs = _creation_jobs()
        jobs["jobs"][0]["steps"][0]["conclusion"] = "failure"
        with self.assertRaisesRegex(FoundationRecoveryVerificationError, "step drift"):
            validate_creation_run(_creation_run(), jobs)
        resources = _stack_resources()
        resources["StackResourceSummaries"].append(
            {
                "LogicalResourceId": "CollectorPostRoute",
                "ResourceType": "AWS::ApiGatewayV2::Route",
                "ResourceStatus": "CREATE_COMPLETE",
            }
        )
        with self.assertRaisesRegex(FoundationRecoveryVerificationError, "allowlist"):
            validate_live_stack_resources(resources)

    def test_records_schema_v2_recovery_and_opens_only_reader_gate(self) -> None:
        self._validate(self.evidence)
        sha = hashlib.sha256(canonical_evidence_bytes(self.evidence)).hexdigest()
        recorded = record_foundation_evidence(
            self.workspace,
            self.evidence,
            evidence_sha256=sha,
            expected_workflow_run_id=RECOVERY_RUN_ID,
            expected_main_commit=RECOVERY_MAIN_COMMIT,
        )
        self.assertEqual(
            RECOVERY_ALLOWED_CHANGED_PATHS,
            _changed_leaf_paths(self.workspace, recorded),
        )
        production = recorded["athena"]["production"]
        self.assertEqual(2, production["foundation_evidence_schema_version"])
        self.assertTrue(production["reader_provisioning_allowed"])
        self.assertFalse(production["foundation_deployment_allowed"])
        self.assertFalse(production["growthbook_clone"]["clone_allowed"])
        with mock.patch.object(
            workspace_validator,
            "_load",
            side_effect=[recorded, self.reporting, self.registry],
        ):
            workspace_validator.validate()

    def test_rejects_creation_provenance_and_task_aliasing(self) -> None:
        drifted = copy.deepcopy(self.evidence)
        drifted["creation_provenance"]["workflow_run_id"] = "32612205629"
        with self.assertRaisesRegex(FoundationEvidenceRecordingError, "creation provenance"):
            self._validate(drifted)
        aliased = copy.deepcopy(self.evidence)
        aliased["host_gate"]["task_id"] = aliased["service_runtime"]["task_id"]
        with self.assertRaisesRegex(FoundationEvidenceRecordingError, "tasks must differ"):
            self._validate(aliased)


if __name__ == "__main__":
    unittest.main()
