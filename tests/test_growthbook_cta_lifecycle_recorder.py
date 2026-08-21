from __future__ import annotations

import copy
import hashlib
import json
import unittest

from scripts import evaluate_growthbook_cta as evaluator
from scripts import record_growthbook_cta_lifecycle_reconciliation as recorder


def _observation() -> dict:
    return {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_cta_lifecycle_reconciliation_observation",
        "experiment_id": "vevo-sk-product-cta-color-001",
        "metric_contract_version": "vevo_cm1_v1_2026-08-20",
        "observed_at_utc": "2026-07-29T22:30:00Z",
        "lifecycle_checkpoint_days": 14,
        "reporting_quality_object_key": (
            "experiment-events/curated/quality/experiment_id="
            "vevo-sk-product-cta-color-001/reconciliation.json"
        ),
        "reporting_quality_object_sha256": "c" * 64,
        "experiment_cm1_sum_eur": 1192.4,
        "reporting_cm1_sum_eur": 1192.4,
        "cm1_absolute_difference_eur": 0,
        "mature_orders_checked": 75,
        "cancelled_orders_checked": 2,
        "refunded_orders_checked": 1,
        "creditnote_rows_checked": 1,
        "lifecycle_counts_match": True,
        "refund_creditnote_value_parity_verified": True,
        "non_realized_value_policy": (
            "zero_value_until_realized_with_explicit_lifecycle_counts"
        ),
        "non_realized_value_policy_verified": True,
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
            verified_at_utc="2026-07-29T23:00:00Z",
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
                verified_at_utc="2026-07-29T23:00:00Z",
            )

    def test_rejects_an_independent_hash_mismatch(self) -> None:
        with self.assertRaisesRegex(recorder.LifecycleRecordingError, "SHA-256"):
            recorder.record(
                observation_bytes=self.body,
                expected_sha256="f" * 64,
                current_manifest=self.manifest,
                verified_at_utc="2026-07-29T23:00:00Z",
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
                verified_at_utc="2026-07-29T23:00:00Z",
            )

    def test_rejects_cm1_value_mismatch(self) -> None:
        altered = copy.deepcopy(self.observation)
        altered["reporting_cm1_sum_eur"] = 1191.4
        altered["cm1_absolute_difference_eur"] = 1
        body = recorder.canonical_json_bytes(altered)

        with self.assertRaisesRegex(recorder.LifecycleRecordingError, "parity failed"):
            recorder.record(
                observation_bytes=body,
                expected_sha256=hashlib.sha256(body).hexdigest(),
                current_manifest=self.manifest,
                verified_at_utc="2026-07-29T23:00:00Z",
            )

    def test_rejects_replacing_an_already_verified_manifest(self) -> None:
        updated, _ = recorder.record(
            observation_bytes=self.body,
            expected_sha256=self.sha256,
            current_manifest=self.manifest,
            verified_at_utc="2026-07-29T23:00:00Z",
        )

        with self.assertRaisesRegex(recorder.LifecycleRecordingError, "already recorded"):
            recorder.record(
                observation_bytes=self.body,
                expected_sha256=self.sha256,
                current_manifest=updated,
                verified_at_utc="2026-07-29T23:00:00Z",
            )


if __name__ == "__main__":
    unittest.main()
