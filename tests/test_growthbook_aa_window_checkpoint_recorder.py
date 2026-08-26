from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import tempfile
import unittest

from scripts.record_growthbook_aa_window_checkpoint import (
    CheckpointRecordingError,
    load_validate_and_record,
    record_checkpoint,
)
from scripts.validate_growthbook_aa_measurement_window import (
    MeasurementWindowError,
    canonical_evidence_bytes,
    validate_measurement_window,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUN_ID = "32830000000"
MAIN_COMMIT = "a" * 40


def load(name: str) -> dict[str, object]:
    return json.loads((ROOT / "projects" / "vevo" / name).read_text(encoding="utf-8"))


def checkpoint_evidence(
    *, index: int = 1, eligible_devices: int = 900
) -> dict[str, object]:
    candidate_dates = {
        1: (
            "2026-09-01T22:00:00Z",
            "2026-09-01",
            "2026-09-02T03:45:00+02:00",
            "2026-09-02T02:00:00Z",
            7,
        ),
        2: (
            "2026-09-02T22:00:00Z",
            "2026-09-02",
            "2026-09-03T03:45:00+02:00",
            "2026-09-03T02:00:00Z",
            8,
        ),
    }
    through, last_date, due, observed, full_days = candidate_dates[index]
    return {
        "schema_version": 2,
        "evidence_type": "vevo_growthbook_aa_window_checkpoint",
        "status": "passed",
        "experiment_id": "vevo-sk-aa-001",
        "repository": "vzeman/biznisweb",
        "workflow": ".github/workflows/check-vevo-growthbook-production-aa-window.yml",
        "workflow_run_id": RUN_ID,
        "main_commit": MAIN_COMMIT,
        "observed_at_utc": observed,
        "window": {
            "timezone": "Europe/Bratislava",
            "checkpoint_index": index,
            "from_utc": "2026-08-25T22:00:00Z",
            "candidate_through_utc": through,
            "candidate_last_full_local_date": last_date,
            "full_calendar_days": full_days,
            "resolution_due_local": due,
        },
        "runtime": {
            "instance_id": "N/A:Fargate",
            "private_ip": "172.31.10.20",
            "service": "vevo-growthbook-reconcile-production",
            "runtime_path": "/app",
            "task_id": "b" * 32,
            "task_definition": "vevo-growthbook-reconcile-production:3",
            "image_digest": "sha256:" + "c" * 64,
            "identity_source": "ecs_stopped_task",
            "host_gate_evidence_sha256": "21fb2aab84f4839ccff04ca1a479e2ba2de4fef516a86b748a061957459baacb",
            "localhost_health_marker_inherited_from_deploy_evidence": True,
            "localhost_runtime_marker_inherited_from_deploy_evidence": True,
        },
        "control_plane": {
            "schedule_name": "vevo-growthbook-reconcile-production",
            "schedule_due_local": due,
            "schedule_succeeded": True,
            "success_marker_sha256": "d" * 64,
            "publish_summary_sha256": "e" * 64,
            "generated_published_counts_match": True,
            "dlq_empty": True,
            "alarms_clear": True,
            "source_schedule_name": "vevo-daily-report-email",
            "source_schedule_unchanged": True,
            "scheduler_run_task_verified": True,
            "runtime_state_retained": True,
        },
        "population": {
            "metric": "cumulative_eligible_devices_without_arm_outcome_readback",
            "eligible_devices": eligible_devices,
            "database": "vevo_growthbook_production",
            "workgroup": "vevo-growthbook-reporting-production",
            "source_table": "experiment_device_facts",
            "aggregate_query_sha256": "f" * 64,
            "aggregate_result_sha256": "1" * 64,
            "only_aggregate_count_retained": True,
            "arm_counts_read": False,
            "arm_outcomes_read": False,
            "outcome_metrics_read": False,
            "contains_event_or_device_ids": False,
            "contains_customer_or_order_data": False,
        },
        "decision": (
            "resolve" if eligible_devices >= 1000 else "extend_one_full_local_day"
        ),
        "safety": {
            "contains_raw_aws_payloads": False,
            "contains_cloudwatch_messages": False,
            "contains_credentials": False,
            "aws_mutations": False,
            "growthbook_mutations": False,
            "gtm_mutations": False,
            "meta_ads_mutations": False,
            "biznisweb_mutations": False,
            "commerce_mutations": False,
            "winner_calls": False,
            "cta_activation": False,
        },
    }


class GrowthBookAaWindowCheckpointRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshot = load("growthbook_aa_snapshot.json")
        self.activation = load("growthbook_production_aa_activation.json")
        self.acceptance = load("growthbook_aa_acceptance.json")
        self.reconciliation = load(
            "growthbook_production_reconciliation_deploy_evidence.json"
        )

    def record(
        self,
        evidence: dict[str, object],
        snapshot: dict[str, object] | None = None,
    ) -> dict[str, object]:
        digest = hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest()
        return record_checkpoint(
            snapshot or self.snapshot,
            evidence,
            evidence_sha256=digest,
            expected_workflow_run_id=RUN_ID,
            expected_main_commit=MAIN_COMMIT,
            activation=self.activation,
            acceptance=self.acceptance,
            reconciliation=self.reconciliation,
        )

    def test_below_threshold_appends_only_outcome_blind_checkpoint(self) -> None:
        recorded = self.record(checkpoint_evidence())
        window = recorded["measurement_window"]
        self.assertEqual(
            "pending_minimum_window_and_sample", window["resolution_status"]
        )
        self.assertEqual(1, len(window["checkpoint_history"]))
        self.assertIsNone(window["resolved_through_utc"])
        self.assertIsNone(recorded["automated_evidence"]["through_utc"])
        self.assertFalse(recorded["automated_evidence"]["producer_allowed"])
        self.assertFalse(recorded["manual_qa_evidence"]["producer_allowed"])
        self.assertFalse(recorded["snapshot_build_allowed"])
        validate_measurement_window(
            recorded, self.activation, self.acceptance, self.reconciliation
        )

    def test_first_qualifying_checkpoint_resolves_exact_whole_day_window(self) -> None:
        first = self.record(checkpoint_evidence())
        second = self.record(
            checkpoint_evidence(index=2, eligible_devices=1005), snapshot=first
        )
        window = second["measurement_window"]
        self.assertEqual("resolved", window["resolution_status"])
        self.assertEqual("2026-09-02T22:00:00Z", window["resolved_through_utc"])
        self.assertEqual("2026-09-02", window["resolved_last_full_local_date"])
        self.assertEqual(8, window["resolved_full_calendar_days"])
        self.assertEqual(1005, window["resolved_eligible_devices"])
        self.assertEqual(2, len(window["checkpoint_history"]))
        for component in ("automated_evidence", "manual_qa_evidence"):
            self.assertEqual("2026-09-02T22:00:00Z", second[component]["through_utc"])
            self.assertFalse(second[component]["producer_allowed"])
        self.assertFalse(second["snapshot_build_allowed"])

    def test_rejects_wrong_decision_outcome_reads_and_extra_identity(self) -> None:
        wrong_decision = checkpoint_evidence(eligible_devices=999)
        wrong_decision["decision"] = "resolve"
        with self.assertRaisesRegex(CheckpointRecordingError, "decision drift"):
            self.record(wrong_decision)

        outcome_read = checkpoint_evidence()
        outcome_read["population"]["arm_outcomes_read"] = True
        with self.assertRaisesRegex(CheckpointRecordingError, "outcome-blind"):
            self.record(outcome_read)

        identity = checkpoint_evidence()
        identity["population"]["device_id"] = "forbidden"
        with self.assertRaisesRegex(CheckpointRecordingError, "field set drift"):
            self.record(identity)

    def test_accepts_retention_recovery_without_inventing_private_ip(self) -> None:
        evidence = checkpoint_evidence()
        evidence["runtime"]["private_ip"] = None
        evidence["runtime"]["identity_source"] = (
            "cloudtrail_run_task_retention_recovery"
        )
        evidence["control_plane"]["runtime_state_retained"] = False
        recorded = self.record(evidence)
        saved = recorded["measurement_window"]["checkpoint_history"][0]["evidence"]
        self.assertIsNone(saved["runtime"]["private_ip"])
        self.assertFalse(saved["control_plane"]["runtime_state_retained"])

    def test_accepts_explicit_schema_v3_historical_backfill_for_next_missing_index(
        self,
    ) -> None:
        evidence = checkpoint_evidence()
        evidence["schema_version"] = 3
        evidence["collection_mode"] = "manual_historical_backfill"
        evidence["observed_at_utc"] = "2026-09-04T02:30:00Z"
        recorded = self.record(evidence)
        saved = recorded["measurement_window"]["checkpoint_history"][0]["evidence"]
        self.assertEqual("manual_historical_backfill", saved["collection_mode"])
        self.assertEqual(
            "2026-09-01T22:00:00Z", saved["window"]["candidate_through_utc"]
        )

    def test_rejects_schema_v3_backfill_timing_and_mode_contradictions(self) -> None:
        early_backfill = checkpoint_evidence()
        early_backfill["schema_version"] = 3
        early_backfill["collection_mode"] = "manual_historical_backfill"
        with self.assertRaisesRegex(
            CheckpointRecordingError, "before its daily gate closed"
        ):
            self.record(early_backfill)

        late_same_window = checkpoint_evidence()
        late_same_window["schema_version"] = 3
        late_same_window["collection_mode"] = "manual_same_window"
        late_same_window["observed_at_utc"] = "2026-09-04T02:30:00Z"
        with self.assertRaisesRegex(CheckpointRecordingError, "outside its daily gate"):
            self.record(late_same_window)

        unknown_mode = checkpoint_evidence()
        unknown_mode["schema_version"] = 3
        unknown_mode["collection_mode"] = "operator_selected"
        with self.assertRaisesRegex(CheckpointRecordingError, "collection mode drift"):
            self.record(unknown_mode)

    def test_rejects_retention_source_and_private_ip_contradictions(self) -> None:
        missing_retained_ip = checkpoint_evidence()
        missing_retained_ip["runtime"]["private_ip"] = None
        with self.assertRaisesRegex(CheckpointRecordingError, "private IP"):
            self.record(missing_retained_ip)

        invented_expired_ip = checkpoint_evidence()
        invented_expired_ip["runtime"]["identity_source"] = (
            "cloudtrail_run_task_retention_recovery"
        )
        invented_expired_ip["control_plane"]["runtime_state_retained"] = False
        with self.assertRaisesRegex(CheckpointRecordingError, "must be null"):
            self.record(invented_expired_ip)

        unverified_scheduler = checkpoint_evidence()
        unverified_scheduler["control_plane"]["scheduler_run_task_verified"] = False
        with self.assertRaisesRegex(CheckpointRecordingError, "RunTask"):
            self.record(unverified_scheduler)

    def test_accepts_legacy_schema_v1_evidence(self) -> None:
        evidence = checkpoint_evidence()
        evidence["schema_version"] = 1
        evidence["runtime"].pop("identity_source")
        evidence["control_plane"].pop("scheduler_run_task_verified")
        evidence["control_plane"].pop("runtime_state_retained")
        recorded = self.record(evidence)
        self.assertEqual(
            1,
            recorded["measurement_window"]["checkpoint_history"][0]["evidence"][
                "schema_version"
            ],
        )

    def test_rejects_tampered_history_and_non_independent_identity(self) -> None:
        recorded = self.record(checkpoint_evidence())
        tampered = copy.deepcopy(recorded)
        tampered["measurement_window"]["checkpoint_history"][0]["evidence"][
            "decision"
        ] = "resolve"
        with self.assertRaisesRegex(MeasurementWindowError, "SHA-256 mismatch"):
            validate_measurement_window(
                tampered, self.activation, self.acceptance, self.reconciliation
            )

        evidence = checkpoint_evidence()
        digest = hashlib.sha256(canonical_evidence_bytes(evidence)).hexdigest()
        with self.assertRaisesRegex(CheckpointRecordingError, "run ID mismatch"):
            record_checkpoint(
                self.snapshot,
                evidence,
                evidence_sha256=digest,
                expected_workflow_run_id="32830000001",
                expected_main_commit=MAIN_COMMIT,
                activation=self.activation,
                acceptance=self.acceptance,
                reconciliation=self.reconciliation,
            )

    def test_canonical_file_transition_is_atomic_and_idempotent(self) -> None:
        evidence = checkpoint_evidence()
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            evidence_path = temporary / "checkpoint.json"
            snapshot_path = temporary / "snapshot.json"
            output_path = temporary / "recorded.json"
            evidence_path.write_bytes(canonical_evidence_bytes(evidence))
            evidence_sha256 = hashlib.sha256(
                canonical_evidence_bytes(evidence)
            ).hexdigest()
            snapshot_path.write_text(
                json.dumps(self.snapshot, indent=2) + "\n", encoding="utf-8"
            )
            first = load_validate_and_record(
                evidence_path=evidence_path,
                snapshot_path=snapshot_path,
                output_path=output_path,
                expected_evidence_sha256=evidence_sha256,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )
            second = self.record(evidence, snapshot=first)
            self.assertEqual(first, second)
            self.assertEqual(first, json.loads(output_path.read_text(encoding="utf-8")))

            evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
            with self.assertRaisesRegex(CheckpointRecordingError, "not canonical"):
                load_validate_and_record(
                    evidence_path=evidence_path,
                    snapshot_path=snapshot_path,
                    output_path=output_path,
                    expected_evidence_sha256=evidence_sha256,
                    expected_workflow_run_id=RUN_ID,
                    expected_main_commit=MAIN_COMMIT,
                )

    def test_rejects_checkpoint_after_resolution(self) -> None:
        resolved = self.record(checkpoint_evidence(eligible_devices=1000))
        with self.assertRaisesRegex(CheckpointRecordingError, "already resolved"):
            self.record(checkpoint_evidence(index=2, eligible_devices=1100), resolved)


if __name__ == "__main__":
    unittest.main()
