from __future__ import annotations

import hashlib
import json
import pathlib
import tempfile
import unittest

from scripts.record_growthbook_natural_evidence import (
    _changed_leaf_paths,
    canonical_evidence_bytes,
)
from scripts.record_growthbook_post_publish_zero_collector_observation import (
    ALLOWED_CHANGED_PATHS,
    EXPECTED_IMAGE_DIGEST,
    PostPublishZeroCollectorEvidenceError,
    expected_pending_activation,
    expected_post_observation_activation,
    load_validate_and_record,
    record_post_publish_zero_collector_observation,
    validate_post_publish_zero_collector_observation,
)


RUN_ID = "32740000001"
MAIN_COMMIT = "a" * 40


def evidence() -> dict[str, object]:
    return {
        "accepted_receipt_count": 0,
        "api_request_count": 0,
        "evidence_type": (
            "vevo_growthbook_post_publish_zero_collector_observation"
        ),
        "from_utc": "2026-08-24T14:34:30Z",
        "main_commit": MAIN_COMMIT,
        "observed_at_utc": "2026-08-24T14:40:00Z",
        "route_key": "POST /v1/events",
        "runtime": {
            "image_digest": EXPECTED_IMAGE_DIGEST,
            "instance_id": "N/A:Fargate",
            "private_ip": "172.31.21.213",
            "runtime_path": "/app",
            "runtime_path_verification": "immutable_image_prior_localhost_marker",
            "service": "vevo-growthbook-collector-production",
            "target_health": "healthy",
            "task_definition": "vevo-growthbook-collector-production:2",
            "task_id": "a3abdbcdd3914c95bb08f03b83eab5fe",
        },
        "safety": {
            "aws_mutations": False,
            "biznisweb_mutations": False,
            "commerce_mutations": False,
            "contains_cloudwatch_messages": False,
            "contains_credentials": False,
            "contains_customer_or_order_data": False,
            "contains_event_or_request_ids": False,
            "growthbook_mutations": False,
            "gtm_mutations": False,
            "meta_ads_mutations": False,
        },
        "schema_version": 1,
        "source": {
            "growthbook_status": "draft_not_started",
            "gtm_live_container_version_id": "15",
            "gtm_publish_status": "published_zero_allocation",
            "production_allocation_percent": 0,
            "workspace_id": "17",
        },
        "through_utc": "2026-08-24T14:38:00Z",
        "workflow_run_id": RUN_ID,
    }


def evidence_sha256(value: dict[str, object]) -> str:
    return hashlib.sha256(canonical_evidence_bytes(value)).hexdigest()


class GrowthBookPostPublishZeroCollectorObservationRecorderTests(unittest.TestCase):
    def record(self, value: dict[str, object] | None = None) -> dict[str, object]:
        payload = evidence() if value is None else value
        return record_post_publish_zero_collector_observation(
            expected_pending_activation(),
            payload,
            evidence_sha256=evidence_sha256(payload),
            expected_workflow_run_id=RUN_ID,
            expected_main_commit=MAIN_COMMIT,
        )

    def test_records_only_reviewed_post_publish_gate_paths(self) -> None:
        pending = expected_pending_activation()
        result = self.record()
        self.assertEqual(ALLOWED_CHANGED_PATHS, _changed_leaf_paths(pending, result))
        self.assertEqual(9, result["schema_version"])
        self.assertEqual(
            "gtm_live_zero_allocation_verified_growthbook_start_review_pending",
            result["status"],
        )
        preflight = result["activation_preflight"]
        post_publish = preflight["post_publish_readback"]
        self.assertTrue(post_publish["zero_collector_request_verified"])
        self.assertTrue(post_publish["growthbook_start_allowed"])
        observation = post_publish["zero_collector_observation"]
        self.assertEqual(0, observation["api_request_count"])
        self.assertEqual(0, observation["accepted_receipt_count"])
        self.assertEqual(RUN_ID, observation["workflow_run_id"])
        self.assertEqual(evidence_sha256(evidence()), observation["artifact_sha256"])
        self.assertTrue(
            preflight["mutation_scope"][
                "start_growthbook_experiment_exp_19g6mmt5wugpk"
            ]
        )
        self.assertTrue(
            preflight["mutation_scope"]["publish_growthbook_feature_revision_3"]
        )
        self.assertFalse(result["traffic"]["activation_allowed"])
        self.assertEqual(0, result["traffic"]["production_allocation_percent"])
        self.assertEqual("review_growthbook_production_aa_start", result["next_gate"])

    def test_post_observation_result_is_exact_and_idempotent(self) -> None:
        result = self.record()
        record = result["activation_preflight"]["post_publish_readback"][
            "zero_collector_observation"
        ]
        self.assertEqual(expected_post_observation_activation(record), result)
        rerun = record_post_publish_zero_collector_observation(
            result,
            evidence(),
            evidence_sha256=evidence_sha256(evidence()),
            expected_workflow_run_id=RUN_ID,
            expected_main_commit=MAIN_COMMIT,
        )
        self.assertEqual(result, rerun)

    def test_rejects_nonzero_counts_or_source_and_safety_drift(self) -> None:
        cases = (
            (("api_request_count",), 1, "exactly zero"),
            (("accepted_receipt_count",), 1, "exactly zero"),
            (("source", "gtm_live_container_version_id"), "14", "source boundary"),
            (("runtime", "private_ip"), "192.0.2.1", "private IP boundary"),
            (("safety", "aws_mutations"), True, "safety boundary"),
        )
        for path, replacement, message in cases:
            with self.subTest(path=path):
                altered = evidence()
                target = altered
                for key in path[:-1]:
                    target = target[key]  # type: ignore[index,assignment]
                target[path[-1]] = replacement  # type: ignore[index]
                with self.assertRaisesRegex(
                    PostPublishZeroCollectorEvidenceError, message
                ):
                    self.record(altered)

    def test_rejects_hash_manifest_and_canonical_byte_drift(self) -> None:
        payload = evidence()
        with self.assertRaisesRegex(
            PostPublishZeroCollectorEvidenceError, "SHA-256 mismatch"
        ):
            record_post_publish_zero_collector_observation(
                expected_pending_activation(),
                payload,
                evidence_sha256="0" * 64,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )

        drifted = expected_pending_activation()
        drifted["traffic"]["activation_allowed"] = True
        with self.assertRaisesRegex(
            PostPublishZeroCollectorEvidenceError, "exact post-publish pending"
        ):
            record_post_publish_zero_collector_observation(
                drifted,
                payload,
                evidence_sha256=evidence_sha256(payload),
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            evidence_path = root / "evidence.json"
            activation_path = root / "activation.json"
            evidence_path.write_text(json.dumps(payload), encoding="utf-8")
            activation_path.write_text(
                json.dumps(expected_pending_activation()), encoding="utf-8"
            )
            with self.assertRaisesRegex(
                PostPublishZeroCollectorEvidenceError, "not canonical"
            ):
                load_validate_and_record(
                    evidence_path=evidence_path,
                    activation_path=activation_path,
                    expected_evidence_sha256=hashlib.sha256(
                        evidence_path.read_bytes()
                    ).hexdigest(),
                    expected_workflow_run_id=RUN_ID,
                    expected_main_commit=MAIN_COMMIT,
                )

    def test_validator_rejects_extra_fields(self) -> None:
        altered = evidence()
        altered["unexpected"] = True
        with self.assertRaisesRegex(
            PostPublishZeroCollectorEvidenceError, "field set drift"
        ):
            validate_post_publish_zero_collector_observation(
                altered,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )


if __name__ == "__main__":
    unittest.main()
