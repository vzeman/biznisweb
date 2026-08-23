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
    _changed_leaf_paths,
    canonical_evidence_bytes,
    record_natural_evidence,
)
from scripts.record_growthbook_production_clone_evidence import (
    ALLOWED_CHANGED_PATHS,
    FACT_TABLE_KEYS,
    METRIC_KEYS,
    PAID_PRO_METRIC_KEYS,
    CloneEvidenceRecordingError,
    build_clone_observation,
    load_validate_and_record,
    record_clone_evidence,
    validate_clone_observation,
)
from scripts.record_growthbook_production_reader_evidence import (
    build_reader_evidence,
    record_reader_evidence,
)
from scripts.verify_growthbook_natural_reconciliation import (
    EXPECTED_IMAGE_DIGEST as NATURAL_IMAGE_DIGEST,
    build_natural_reconciliation_evidence,
)
from tests.growthbook_test_state import pending_natural_evidence_workspace


NATURAL_RUN_ID = "32490000000"
NATURAL_MAIN_COMMIT = "a" * 40
FOUNDATION_RUN_ID = "32490000001"
FOUNDATION_MAIN_COMMIT = "b" * 40
READER_RUN_ID = "32490000002"
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


def _reader_ready_workspace() -> dict[str, object]:
    workspace = pending_natural_evidence_workspace(
        json.loads(workspace_validator.WORKSPACE_PATH.read_text(encoding="utf-8"))
    )
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
    workspace = record_foundation_evidence(
        workspace,
        foundation,
        evidence_sha256=foundation_sha,
        expected_workflow_run_id=FOUNDATION_RUN_ID,
        expected_main_commit=FOUNDATION_MAIN_COMMIT,
    )
    reader = build_reader_evidence(
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
    reader_sha = hashlib.sha256(canonical_evidence_bytes(reader)).hexdigest()
    return record_reader_evidence(
        workspace,
        reader,
        evidence_sha256=reader_sha,
        expected_workflow_run_id=READER_RUN_ID,
        expected_main_commit=READER_MAIN_COMMIT,
    )


class GrowthBookProductionCloneEvidenceRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = _reader_ready_workspace()
        self.reporting = json.loads(workspace_validator.REPORTING_PATH.read_text(encoding="utf-8"))
        self.registry = json.loads(workspace_validator.REGISTRY_PATH.read_text(encoding="utf-8"))
        self.fact_ids = {
            "vevo_device_outcomes_v1": "ftb_prodDevice123",
            "vevo_performance_vitals_v1": "ftb_prodPerf123",
        }
        self.metric_ids = {
            key: f"fact__Prod{index:02d}Metric"
            for index, key in enumerate(METRIC_KEYS, start=1)
        }
        self.observation = build_clone_observation(
            self.workspace,
            observed_at=datetime(2026, 8, 22, 5, 0, tzinfo=timezone.utc),
            data_source_id="ds_prodFacts123",
            fact_table_ids=self.fact_ids,
            metric_ids=self.metric_ids,
        )
        self.observation_sha = hashlib.sha256(
            canonical_evidence_bytes(self.observation)
        ).hexdigest()

    def record(self, observation: dict[str, object] | None = None) -> dict[str, object]:
        value = self.observation if observation is None else observation
        return record_clone_evidence(
            self.workspace,
            value,
            observation_sha256=hashlib.sha256(
                canonical_evidence_bytes(value)
            ).hexdigest(),
        )

    def test_builds_exact_sanitized_clone_observation(self) -> None:
        validate_clone_observation(self.observation, self.workspace)
        self.assertEqual(0, self.observation["production_data_source"]["assignment_query_result_row_count"])
        self.assertEqual(
            {key: None for key in PAID_PRO_METRIC_KEYS},
            self.observation["paid_pro_quantile_metrics"]["target_metric_ids"],
        )
        serialized = json.dumps(self.observation, sort_keys=True)
        for forbidden in (
            "AKIA",
            "SecretAccessKey",
            "customer_email",
            "order_num",
            "query_result_rows",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_builder_is_hard_disabled_before_reader_evidence(self) -> None:
        pending = json.loads(
            workspace_validator.WORKSPACE_PATH.read_text(encoding="utf-8")
        )
        with self.assertRaisesRegex(CloneEvidenceRecordingError, "reader provenance"):
            build_clone_observation(
                pending,
                observed_at=datetime(2026, 8, 22, 5, 0, tzinfo=timezone.utc),
                data_source_id="ds_prodFacts123",
                fact_table_ids=self.fact_ids,
                metric_ids=self.metric_ids,
            )

    def test_records_only_clone_completion_paths(self) -> None:
        recorded = self.record()
        self.assertEqual(
            ALLOWED_CHANGED_PATHS,
            _changed_leaf_paths(self.workspace, recorded),
        )
        production = recorded["athena"]["production"]
        clone = production["growthbook_clone"]
        self.assertEqual("verified_complete", clone["status"])
        self.assertFalse(clone["clone_allowed"])
        self.assertEqual("created_and_query_verified", clone["mutation_status"])
        self.assertEqual("ds_prodFacts123", clone["target_data_source_id"])
        self.assertEqual(self.fact_ids, clone["target_fact_table_ids"])
        self.assertEqual(self.metric_ids, clone["target_metric_ids"])
        self.assertEqual(0, recorded["workspace"]["production_allocation_percent"])
        self.assertEqual("not_published", recorded["gtm_preview_workspace"]["publish_status"])

    def test_workspace_validator_accepts_only_hash_bound_clone_state(self) -> None:
        recorded = self.record()
        with mock.patch.object(
            workspace_validator,
            "_load",
            side_effect=[recorded, self.reporting, self.registry],
        ):
            workspace_validator.validate()

        drifted = copy.deepcopy(recorded)
        drifted["athena"]["production"]["growthbook_clone"]["observation_sha256"] = "0" * 64
        with mock.patch.object(
            workspace_validator,
            "_load",
            side_effect=[drifted, self.reporting, self.registry],
        ):
            with self.assertRaisesRegex(AssertionError, "clone observation SHA-256"):
                workspace_validator.validate()

    def test_rejects_preview_reuse_paid_upgrade_and_nonempty_queries(self) -> None:
        source_reuse = copy.deepcopy(self.observation)
        source_reuse["production_data_source"]["id"] = "ds_19g6mmt2c4dmn"
        with self.assertRaisesRegex(CloneEvidenceRecordingError, "reuses Preview"):
            self.record(source_reuse)

        paid = copy.deepcopy(self.observation)
        paid["paid_pro_quantile_metrics"]["upgrade_authorized"] = True
        with self.assertRaisesRegex(CloneEvidenceRecordingError, "not authorized"):
            self.record(paid)

        nonempty = copy.deepcopy(self.observation)
        nonempty["production_fact_tables"][FACT_TABLE_KEYS[0]]["query_result_row_count"] = 1
        with self.assertRaisesRegex(CloneEvidenceRecordingError, "empty before traffic"):
            self.record(nonempty)

    def test_rejects_contract_drift_or_unverified_readback(self) -> None:
        metric_drift = copy.deepcopy(self.observation)
        metric_drift["production_metrics"][METRIC_KEYS[0]]["contract_sha256"] = "0" * 64
        with self.assertRaisesRegex(CloneEvidenceRecordingError, "contract hash drift"):
            self.record(metric_drift)

        preview_drift = copy.deepcopy(self.observation)
        preview_drift["source_preview_readback"]["objects_unchanged"] = False
        with self.assertRaisesRegex(CloneEvidenceRecordingError, "unchanged"):
            self.record(preview_drift)

        unsafe = copy.deepcopy(self.observation)
        unsafe["safety"]["gtm_published"] = True
        with self.assertRaisesRegex(CloneEvidenceRecordingError, "safety drift"):
            self.record(unsafe)

        reader_drift = copy.deepcopy(self.observation)
        reader_drift["reader_evidence_provenance"]["artifact_sha256"] = "0" * 64
        with self.assertRaisesRegex(CloneEvidenceRecordingError, "reader evidence provenance"):
            self.record(reader_drift)

    def test_loader_requires_canonical_bytes_hash_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            observation_path = root / "clone.json"
            workspace_path = root / "workspace.json"
            observation_path.write_bytes(canonical_evidence_bytes(self.observation))
            workspace_path.write_text(json.dumps(self.workspace), encoding="utf-8")
            recorded = load_validate_and_record(
                observation_path=observation_path,
                workspace_path=workspace_path,
                expected_observation_sha256=self.observation_sha,
            )
            idempotent = record_clone_evidence(
                recorded,
                self.observation,
                observation_sha256=self.observation_sha,
            )
            self.assertEqual(recorded, idempotent)

            observation_path.write_text(
                json.dumps(self.observation, indent=2), encoding="utf-8"
            )
            with self.assertRaisesRegex(CloneEvidenceRecordingError, "not canonical"):
                load_validate_and_record(
                    observation_path=observation_path,
                    workspace_path=workspace_path,
                    expected_observation_sha256=self.observation_sha,
                )


if __name__ == "__main__":
    unittest.main()
