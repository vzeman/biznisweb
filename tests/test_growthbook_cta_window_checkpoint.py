from __future__ import annotations

import copy
import hashlib
import json
import unittest
from datetime import UTC
from pathlib import Path

from scripts import record_growthbook_cta_activation as activation_recorder
from scripts import record_growthbook_cta_window_checkpoint as recorder
from scripts import validate_growthbook_cta_measurement_window as validator
from scripts.freeze_growthbook_cta_sample import calculate_sample_per_arm
from tests import test_growthbook_cta_activation_recorder as activation_fixtures


ROOT = Path(__file__).resolve().parents[1]


def load(relative: str) -> dict:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def pretty_hash(value: dict) -> str:
    raw = (json.dumps(value, indent=2) + "\n").encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class GrowthBookCtaWindowCheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        source = activation_fixtures.GrowthBookCtaActivationRecorderTests(
            methodName="runTest"
        )
        source.setUp()
        opened = source._open()
        self.start_observation = source._start_observation()
        start_hash = hashlib.sha256(
            activation_recorder.canonical_json_bytes(self.start_observation)
        ).hexdigest()
        self.activation, self.running_workspace = activation_recorder.record_start(
            opened,
            source.workspace,
            source.registry,
            self.start_observation,
            observation_sha256=start_hash,
            source_hashes=source.source_hashes,
        )
        self.sample = source.sample
        per_arm, baseline, target = calculate_sample_per_arm(
            exposed_devices=451,
            converted_devices=148,
            relative_mde_percent=25,
            power_percent=80,
            alpha_percent=5,
        )
        self.sample["final"].update(
            {
                "observation_sha256": "9" * 64,
                "aa_snapshot_sha256": source.snapshot_artifact_hash,
                "aa_window_started_at_utc": "2026-08-25T22:00:00Z",
                "aa_window_ended_at_utc": "2026-09-01T22:00:00Z",
                "exposed_devices": 451,
                "converted_devices": 148,
                "baseline_rate_percent": round(100 * baseline, 6),
                "target_rate_percent": round(100 * target, 6),
                "sample_per_arm": per_arm,
                "total_sample": 2 * per_arm,
                "frozen_at_utc": "2026-09-03T22:00:00Z",
            }
        )
        self.sample["next_gate"] = (
            "verify_lifecycle_reconciliation_and_review_activation_before_launch"
        )
        self.activation["source_bindings"]["sample_plan"]["sha256"] = pretty_hash(
            self.sample
        )
        self.contract = load("projects/vevo/growthbook_cta_decision_contract.json")
        self.reconciliation = load(
            "projects/vevo/growthbook_production_reconciliation_deploy_evidence.json"
        )
        self.initial = load("projects/vevo/growthbook_cta_measurement_window.json")
        self.source_hashes = {
            "activation": pretty_hash(self.activation),
            "start_observation": start_hash,
            "sample_plan": pretty_hash(self.sample),
            "decision_contract": hashlib.sha256(
                (
                    ROOT / "projects/vevo/growthbook_cta_decision_contract.json"
                ).read_bytes()
            ).hexdigest(),
            "reconciliation_evidence": hashlib.sha256(
                (
                    ROOT
                    / "projects/vevo/growthbook_production_reconciliation_deploy_evidence.json"
                ).read_bytes()
            ).hexdigest(),
        }
        self.running = recorder.initialize_window(
            self.initial,
            activation=self.activation,
            start_observation=self.start_observation,
            sample_plan=self.sample,
            contract=self.contract,
            reconciliation=self.reconciliation,
            source_hashes=self.source_hashes,
        )
        self.expected = validator.expected_measurement_window(
            self.activation,
            self.start_observation,
            self.sample,
            self.contract,
            self.reconciliation,
        )

    def evidence(self, index: int, eligible: int, decision: str) -> dict:
        candidate, due, last_date, full_days = validator.checkpoint_boundaries(
            self.expected, index
        )
        observed = due.astimezone(UTC).replace(minute=50)
        return {
            "schema_version": 2,
            "evidence_type": "vevo_growthbook_cta_window_checkpoint",
            "status": "passed",
            "experiment_id": "vevo-sk-product-cta-color-001",
            "repository": "vzeman/biznisweb",
            "workflow": ".github/workflows/check-vevo-growthbook-production-cta-window.yml",
            "workflow_run_id": str(50000000000 + index),
            "main_commit": f"{index:040x}"[-40:],
            "observed_at_utc": observed.isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            ),
            "window": {
                "timezone": "Europe/Bratislava",
                "checkpoint_index": index,
                "assignment_started_at_utc": self.expected["assignment_started_at_utc"],
                "candidate_through_utc": candidate.astimezone(UTC)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z"),
                "candidate_last_full_local_date": last_date,
                "full_calendar_days": full_days,
                "resolution_due_local": due.isoformat(timespec="seconds"),
            },
            "runtime": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.21.213",
                "service": "vevo-growthbook-reconcile-production",
                "runtime_path": "/app",
                "task_id": "a" * 32,
                "task_definition": "vevo-growthbook-reconcile-production:2",
                "image_digest": "sha256:" + "b" * 64,
                "identity_source": "ecs_stopped_task",
                "host_gate_evidence_sha256": validator.EXPECTED_RECONCILIATION_SHA256,
                "localhost_health_marker_inherited_from_deploy_evidence": True,
                "localhost_runtime_marker_inherited_from_deploy_evidence": True,
            },
            "control_plane": {
                "schedule_name": "vevo-growthbook-reconcile-production",
                "schedule_due_local": due.isoformat(timespec="seconds"),
                "schedule_succeeded": True,
                "success_marker_sha256": "c" * 64,
                "publish_summary_sha256": "d" * 64,
                "generated_published_counts_match": True,
                "dlq_empty": True,
                "alarms_clear": True,
                "source_schedule_name": "vevo-daily-report-email",
                "source_schedule_unchanged": True,
                "scheduler_run_task_verified": True,
                "runtime_state_retained": True,
            },
            "population": {
                "metric": self.expected["population_metric"],
                "eligible_devices": eligible,
                "target_total_sample": self.expected["target_total_sample"],
                "database": "vevo_growthbook_production",
                "workgroup": "vevo-growthbook-reporting-production",
                "source_table": "experiment_device_facts",
                "aggregate_query_sha256": "e" * 64,
                "aggregate_result_sha256": "f" * 64,
                "only_aggregate_count_retained": True,
                "arm_counts_read": False,
                "arm_outcomes_read": False,
                "outcome_metrics_read": False,
                "contains_event_or_device_ids": False,
                "contains_customer_or_order_data": False,
            },
            "decision": decision,
            "safety": {
                "contains_raw_aws_payloads": False,
                "contains_cloudwatch_messages": False,
                "contains_credentials": False,
                "aws_mutations": False,
                "growthbook_mutations": False,
                "gtm_mutations": False,
                "meta_ads_mutations": False,
                "biznisweb_mutations": False,
                "collector_or_reporting_mutations": False,
                "commerce_mutations": False,
                "winner_calls": False,
                "assignment_stopped": False,
            },
        }

    def record(self, manifest: dict, evidence: dict) -> dict:
        digest = hashlib.sha256(
            validator.canonical_evidence_bytes(evidence)
        ).hexdigest()
        return recorder.record_checkpoint(
            manifest,
            evidence,
            evidence_sha256=digest,
            expected_workflow_run_id=evidence["workflow_run_id"],
            expected_main_commit=evidence["main_commit"],
            activation=self.activation,
            start_observation=self.start_observation,
            sample_plan=self.sample,
            contract=self.contract,
            reconciliation=self.reconciliation,
            source_hashes=self.source_hashes,
        )

    def test_checked_in_manifest_is_fail_closed(self) -> None:
        validator.validate_manifest(
            self.initial,
            load("projects/vevo/growthbook_cta_activation.json"),
            load("projects/vevo/growthbook_cta_sample_plan.json"),
            self.contract,
            self.reconciliation,
        )
        self.assertEqual(validator.WAITING, self.initial["status"])
        self.assertFalse(
            self.initial["release_boundaries"]["read_only_checkpoint_allowed"]
        )
        self.assertFalse(self.initial["assignment_stop"]["manual_review_allowed"])

    def test_waiting_manifest_still_rejects_frozen_contract_drift(self) -> None:
        altered_contract = copy.deepcopy(self.contract)
        altered_contract["decision_timing"]["maximum_full_calendar_days"] = 43
        with self.assertRaises(ValueError):
            validator.validate_manifest(
                self.initial,
                load("projects/vevo/growthbook_cta_activation.json"),
                load("projects/vevo/growthbook_cta_sample_plan.json"),
                altered_contract,
                self.reconciliation,
            )

    def test_initialize_freezes_exact_local_window_and_sample(self) -> None:
        validator.validate_manifest(
            self.running,
            self.activation,
            self.sample,
            self.contract,
            self.reconciliation,
            self.start_observation,
            source_hashes=self.source_hashes,
        )
        window = self.running["measurement_window"]
        self.assertEqual("2026-09-05", window["first_full_local_date"])
        self.assertEqual("2026-09-18T22:00:00Z", window["minimum_through_utc"])
        self.assertEqual("2026-10-16T22:00:00Z", window["maximum_through_utc"])
        self.assertEqual(1084, window["target_total_sample"])
        self.assertTrue(
            self.running["release_boundaries"]["read_only_checkpoint_allowed"]
        )

    def test_below_target_extends_without_opening_stop(self) -> None:
        evidence = self.evidence(1, 1000, "extend_one_full_local_day")
        recorded = self.record(self.running, evidence)
        self.assertEqual(validator.RUNNING, recorded["status"])
        self.assertEqual(1, len(recorded["measurement_window"]["checkpoint_history"]))
        self.assertFalse(recorded["assignment_stop"]["manual_review_allowed"])
        self.assertFalse(recorded["release_boundaries"]["winner_calls_allowed"])

    def test_target_checkpoint_opens_only_manual_stop_review(self) -> None:
        first = self.record(
            self.running, self.evidence(1, 1000, "extend_one_full_local_day")
        )
        second = self.record(
            first,
            self.evidence(2, 1084, "open_manual_stop_review_target_reached"),
        )
        self.assertEqual(validator.RESOLVED, second["status"])
        self.assertTrue(second["assignment_stop"]["manual_review_allowed"])
        self.assertFalse(second["assignment_stop"]["automatic_stop_allowed"])
        self.assertFalse(second["release_boundaries"]["read_only_checkpoint_allowed"])
        self.assertFalse(second["release_boundaries"]["outcome_metrics_read_allowed"])
        self.assertEqual(
            "target_total_sample_reached",
            second["measurement_window"]["resolved_reason"],
        )

    def test_day_42_forces_manual_stop_review_without_target(self) -> None:
        state = copy.deepcopy(self.running)
        for index in range(1, 29):
            state = self.record(
                state, self.evidence(index, 500, "extend_one_full_local_day")
            )
        state = self.record(
            state,
            self.evidence(29, 500, "open_manual_stop_review_maximum_duration_reached"),
        )
        self.assertEqual(validator.RESOLVED, state["status"])
        self.assertEqual(
            "maximum_duration_reached",
            state["measurement_window"]["resolved_reason"],
        )
        self.assertEqual(42, state["measurement_window"]["resolved_full_calendar_days"])

    def test_rejects_arm_or_outcome_read_and_wrong_stop_decision(self) -> None:
        for field in ("arm_counts_read", "arm_outcomes_read", "outcome_metrics_read"):
            altered = self.evidence(1, 1000, "extend_one_full_local_day")
            altered["population"][field] = True
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    recorder.CtaCheckpointRecordingError, "forbidden population read"
                ):
                    self.record(self.running, altered)
        wrong = self.evidence(1, 1000, "open_manual_stop_review_target_reached")
        with self.assertRaisesRegex(
            recorder.CtaCheckpointRecordingError, "decision drift"
        ):
            self.record(self.running, wrong)

    def test_accepts_retention_recovery_without_inventing_private_ip(self) -> None:
        evidence = self.evidence(1, 1000, "extend_one_full_local_day")
        evidence["runtime"]["private_ip"] = None
        evidence["runtime"]["identity_source"] = (
            "cloudtrail_run_task_retention_recovery"
        )
        evidence["control_plane"]["runtime_state_retained"] = False
        recorded = self.record(self.running, evidence)
        saved = recorded["measurement_window"]["checkpoint_history"][0]["evidence"]
        self.assertIsNone(saved["runtime"]["private_ip"])
        self.assertFalse(saved["control_plane"]["runtime_state_retained"])

    def test_accepts_explicit_schema_v3_historical_backfill_for_next_missing_index(
        self,
    ) -> None:
        evidence = self.evidence(1, 1000, "extend_one_full_local_day")
        evidence["schema_version"] = 3
        evidence["collection_mode"] = "manual_historical_backfill"
        evidence["observed_at_utc"] = "2026-09-21T02:30:00Z"
        recorded = self.record(self.running, evidence)
        saved = recorded["measurement_window"]["checkpoint_history"][0]["evidence"]
        self.assertEqual("manual_historical_backfill", saved["collection_mode"])
        self.assertEqual(
            "2026-09-18T22:00:00Z", saved["window"]["candidate_through_utc"]
        )

    def test_rejects_schema_v3_backfill_timing_and_mode_contradictions(self) -> None:
        early_backfill = self.evidence(1, 1000, "extend_one_full_local_day")
        early_backfill["schema_version"] = 3
        early_backfill["collection_mode"] = "manual_historical_backfill"
        with self.assertRaisesRegex(
            recorder.CtaCheckpointRecordingError, "before its daily gate closed"
        ):
            self.record(self.running, early_backfill)

        late_same_window = self.evidence(1, 1000, "extend_one_full_local_day")
        late_same_window["schema_version"] = 3
        late_same_window["collection_mode"] = "manual_same_window"
        late_same_window["observed_at_utc"] = "2026-09-21T02:30:00Z"
        with self.assertRaisesRegex(
            recorder.CtaCheckpointRecordingError, "outside daily gate"
        ):
            self.record(self.running, late_same_window)

        unknown_mode = self.evidence(1, 1000, "extend_one_full_local_day")
        unknown_mode["schema_version"] = 3
        unknown_mode["collection_mode"] = "operator_selected"
        with self.assertRaisesRegex(
            recorder.CtaCheckpointRecordingError, "collection mode drift"
        ):
            self.record(self.running, unknown_mode)

    def test_rejects_retention_source_and_private_ip_contradictions(self) -> None:
        missing_retained_ip = self.evidence(1, 1000, "extend_one_full_local_day")
        missing_retained_ip["runtime"]["private_ip"] = None
        with self.assertRaisesRegex(recorder.CtaCheckpointRecordingError, "IP invalid"):
            self.record(self.running, missing_retained_ip)

        invented_expired_ip = self.evidence(1, 1000, "extend_one_full_local_day")
        invented_expired_ip["runtime"]["identity_source"] = (
            "cloudtrail_run_task_retention_recovery"
        )
        invented_expired_ip["control_plane"]["runtime_state_retained"] = False
        with self.assertRaisesRegex(
            recorder.CtaCheckpointRecordingError, "must be null"
        ):
            self.record(self.running, invented_expired_ip)

        unverified_scheduler = self.evidence(1, 1000, "extend_one_full_local_day")
        unverified_scheduler["control_plane"]["scheduler_run_task_verified"] = False
        with self.assertRaisesRegex(recorder.CtaCheckpointRecordingError, "RunTask"):
            self.record(self.running, unverified_scheduler)

    def test_accepts_legacy_schema_v1_checkpoint(self) -> None:
        evidence = self.evidence(1, 1000, "extend_one_full_local_day")
        evidence["schema_version"] = 1
        evidence["runtime"].pop("identity_source")
        evidence["control_plane"].pop("scheduler_run_task_verified")
        evidence["control_plane"].pop("runtime_state_retained")
        recorded = self.record(self.running, evidence)
        self.assertEqual(
            1,
            recorded["measurement_window"]["checkpoint_history"][0]["evidence"][
                "schema_version"
            ],
        )

    def test_rejects_noncanonical_or_wrong_provenance_hash(self) -> None:
        evidence = self.evidence(1, 1000, "extend_one_full_local_day")
        digest = hashlib.sha256(
            validator.canonical_evidence_bytes(evidence)
        ).hexdigest()
        with self.assertRaisesRegex(
            recorder.CtaCheckpointRecordingError, "workflow run mismatch"
        ):
            recorder.record_checkpoint(
                self.running,
                evidence,
                evidence_sha256=digest,
                expected_workflow_run_id="1",
                expected_main_commit=evidence["main_commit"],
                activation=self.activation,
                start_observation=self.start_observation,
                sample_plan=self.sample,
                contract=self.contract,
                reconciliation=self.reconciliation,
                source_hashes=self.source_hashes,
            )

    def test_rejects_decreasing_cumulative_population(self) -> None:
        first = self.record(
            self.running, self.evidence(1, 1000, "extend_one_full_local_day")
        )
        second = self.evidence(2, 999, "extend_one_full_local_day")
        with self.assertRaisesRegex(
            recorder.CtaCheckpointRecordingError,
            "cumulative eligible-device count decreased",
        ):
            self.record(first, second)


if __name__ == "__main__":
    unittest.main()
