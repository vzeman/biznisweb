from __future__ import annotations

import copy
import hashlib
import json
import unittest

from scripts import evaluate_growthbook_cta as evaluator
from scripts import record_growthbook_cta_lifecycle_reconciliation as recorder


def _observation() -> dict:
    return {
        "schema_version": 2,
        "evidence_type": "vevo_growthbook_cta_prelaunch_lifecycle_reconciliation",
        "target_experiment_id": "vevo-sk-product-cta-color-001",
        "source_experiment_id": "vevo-sk-aa-001",
        "metric_contract_version": "vevo_cm1_v1_2026-08-20",
        "workflow_run_id": "12345678901",
        "main_commit": "d" * 40,
        "observed_at_utc": "2026-09-24T03:45:00Z",
        "source_from_utc": "2026-08-25T22:00:00Z",
        "source_through_utc": "2026-09-02T22:00:00Z",
        "order_window_days": 7,
        "lifecycle_checkpoint_days": 14,
        "minimum_followup_days_after_source_end": 21,
        "source_completion_sha256": "a" * 64,
        "source_aa_snapshot_sha256": "b" * 64,
        "query_template_sha256": "5a3548fa877d206e666c369fd19c4c4da121ccd2059354da55361fae86ecf9d5",
        "reporting_quality_object_key": (
            "experiment-events/curated/quality/experiment_id="
            "vevo-sk-aa-001/facts_generated_at=20260924T034500Z.json"
        ),
        "reporting_quality_object_sha256": "c" * 64,
        "eligible_devices_checked": 75,
        "joined_orders_checked": 75,
        "cm1_absolute_difference_eur": 0,
        "mature_orders_checked": 75,
        "immature_orders_checked": 0,
        "cancelled_orders_checked": 2,
        "refunded_or_creditnoted_orders_checked": 1,
        "direct_curated_cm1_sum_eur": 1192.4,
        "athena_reporting_cm1_sum_eur": 1192.4,
        "lifecycle_counts_match": True,
        "refund_creditnote_value_parity_verified": True,
        "non_realized_value_policy": (
            "zero_value_until_realized_with_explicit_lifecycle_counts"
        ),
        "non_realized_value_policy_verified": True,
        "cta_outcome_data_read": False,
        "contains_event_or_device_identity": False,
        "customer_or_order_identity_in_evidence": False,
        "source_read_only": True,
        "no_external_mutation": True,
    }


class GrowthBookCtaLifecycleRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = json.loads(
            recorder.DEFAULT_MANIFEST_PATH.read_text(encoding="utf-8")
        )
        self.observation = _observation()
        self.body = recorder.canonical_json_bytes(self.observation)
        self.sha256 = hashlib.sha256(self.body).hexdigest()

    def test_records_only_the_hash_bound_identity_free_reconciliation(self) -> None:
        updated, observation = recorder.record(
            observation_bytes=self.body,
            expected_sha256=self.sha256,
            current_manifest=self.manifest,
            verified_at_utc="2026-09-24T04:00:00Z",
            workflow_run_id="12345678901",
            main_commit="d" * 40,
        )

        self.assertEqual(self.observation, observation)
        self.assertTrue(updated["verified"])
        self.assertEqual(self.sha256, updated["observation_sha256"])
        self.assertFalse(updated["activation_allowed"])
        self.assertFalse(updated["customer_or_order_identity_in_evidence"])
        evaluator.validate_lifecycle_manifest(updated, observation)

    def test_rejects_noncanonical_json_even_when_values_are_valid(self) -> None:
        body = json.dumps(self.observation, indent=2).encode("utf-8")

        with self.assertRaisesRegex(recorder.LifecycleRecordingError, "canonical"):
            recorder.record(
                observation_bytes=body,
                expected_sha256=hashlib.sha256(body).hexdigest(),
                current_manifest=self.manifest,
                verified_at_utc="2026-09-24T04:00:00Z",
                workflow_run_id="12345678901",
                main_commit="d" * 40,
            )

    def test_rejects_an_independent_hash_mismatch(self) -> None:
        with self.assertRaisesRegex(recorder.LifecycleRecordingError, "SHA-256"):
            recorder.record(
                observation_bytes=self.body,
                expected_sha256="f" * 64,
                current_manifest=self.manifest,
                verified_at_utc="2026-09-24T04:00:00Z",
                workflow_run_id="12345678901",
                main_commit="d" * 40,
            )

    def test_rejects_workflow_run_id_mismatch(self) -> None:
        with self.assertRaisesRegex(
            recorder.LifecycleRecordingError, "workflow run ID mismatch"
        ):
            recorder.record(
                observation_bytes=self.body,
                expected_sha256=self.sha256,
                current_manifest=self.manifest,
                verified_at_utc="2026-09-24T04:00:00Z",
                workflow_run_id="12345678902",
                main_commit="d" * 40,
            )

    def test_rejects_main_commit_mismatch(self) -> None:
        with self.assertRaisesRegex(
            recorder.LifecycleRecordingError, "main commit mismatch"
        ):
            recorder.record(
                observation_bytes=self.body,
                expected_sha256=self.sha256,
                current_manifest=self.manifest,
                verified_at_utc="2026-09-24T04:00:00Z",
                workflow_run_id="12345678901",
                main_commit="e" * 40,
            )

    def test_rejects_extra_identity_fields(self) -> None:
        altered = copy.deepcopy(self.observation)
        altered["customer_email"] = "forbidden@example.com"
        body = recorder.canonical_json_bytes(altered)

        with self.assertRaisesRegex(recorder.LifecycleRecordingError, "extra"):
            recorder.record(
                observation_bytes=body,
                expected_sha256=hashlib.sha256(body).hexdigest(),
                current_manifest=self.manifest,
                verified_at_utc="2026-09-24T04:00:00Z",
                workflow_run_id="12345678901",
                main_commit="d" * 40,
            )

    def test_rejects_cm1_value_mismatch(self) -> None:
        altered = copy.deepcopy(self.observation)
        altered["athena_reporting_cm1_sum_eur"] = 1191.4
        altered["cm1_absolute_difference_eur"] = 1
        body = recorder.canonical_json_bytes(altered)

        with self.assertRaisesRegex(recorder.LifecycleRecordingError, "parity failed"):
            recorder.record(
                observation_bytes=body,
                expected_sha256=hashlib.sha256(body).hexdigest(),
                current_manifest=self.manifest,
                verified_at_utc="2026-09-24T04:00:00Z",
                workflow_run_id="12345678901",
                main_commit="d" * 40,
            )

    def test_rejects_replacing_an_already_verified_manifest(self) -> None:
        updated, _ = recorder.record(
            observation_bytes=self.body,
            expected_sha256=self.sha256,
            current_manifest=self.manifest,
            verified_at_utc="2026-09-24T04:00:00Z",
            workflow_run_id="12345678901",
            main_commit="d" * 40,
        )

        with self.assertRaisesRegex(
            recorder.LifecycleRecordingError, "already recorded"
        ):
            recorder.record(
                observation_bytes=self.body,
                expected_sha256=self.sha256,
                current_manifest=updated,
                verified_at_utc="2026-09-24T04:00:00Z",
                workflow_run_id="12345678901",
                main_commit="d" * 40,
            )


if __name__ == "__main__":
    unittest.main()
