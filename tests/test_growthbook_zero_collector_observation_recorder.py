from __future__ import annotations

import hashlib
import json
import pathlib
import tempfile
import unittest

from scripts import validate_growthbook_production_aa_activation as validator
from scripts.record_growthbook_natural_evidence import (
    _changed_leaf_paths,
    canonical_evidence_bytes,
)
from scripts.record_growthbook_zero_collector_observation import (
    ALLOWED_CHANGED_PATHS,
    EXPECTED_IMAGE_DIGEST,
    ZeroCollectorEvidenceError,
    expected_pending_activation,
    load_validate_and_record,
    record_zero_collector_observation,
    validate_zero_collector_observation,
)


RUN_ID = "32692688625"
MAIN_COMMIT = "bed02cd3176c960d7423d97486bc67d649601241"


def evidence() -> dict[str, object]:
    return {
        "accepted_receipt_count": 0,
        "api_request_count": 0,
        "evidence_type": "vevo_growthbook_zero_collector_observation",
        "from_utc": "2026-08-24T04:30:00Z",
        "main_commit": MAIN_COMMIT,
        "observed_at_utc": "2026-08-24T05:12:49Z",
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
            "gtm_publish_status": "not_published",
            "production_allocation_percent": 0,
            "workspace_id": "17",
        },
        "through_utc": "2026-08-24T04:50:00Z",
        "workflow_run_id": RUN_ID,
    }


def evidence_sha256(value: dict[str, object]) -> str:
    return hashlib.sha256(canonical_evidence_bytes(value)).hexdigest()


class GrowthBookZeroCollectorObservationRecorderTests(unittest.TestCase):
    def record(self, value: dict[str, object] | None = None) -> dict[str, object]:
        payload = evidence() if value is None else value
        return record_zero_collector_observation(
            expected_pending_activation(),
            payload,
            evidence_sha256=evidence_sha256(payload),
            expected_workflow_run_id=RUN_ID,
            expected_main_commit=MAIN_COMMIT,
        )

    def test_records_only_reviewed_zero_traffic_qa_paths(self) -> None:
        pending = expected_pending_activation()
        result = self.record()
        self.assertEqual(ALLOWED_CHANGED_PATHS, _changed_leaf_paths(pending, result))
        self.assertEqual(4, result["schema_version"])
        self.assertEqual(
            "zero_traffic_qa_verified_activation_review_pending", result["status"]
        )
        qa = result["tag_assistant_qa"]
        self.assertEqual("zero_traffic_qa_verified", qa["status"])
        self.assertTrue(qa["zero_collector_request_verified"])
        observation = qa["zero_collector_observation"]
        self.assertEqual("verified_zero_requests_and_receipts", observation["status"])
        self.assertEqual(0, observation["api_request_count"])
        self.assertEqual(0, observation["accepted_receipt_count"])
        self.assertEqual(RUN_ID, observation["workflow_run_id"])
        self.assertEqual(MAIN_COMMIT, observation["main_commit"])
        self.assertEqual(evidence_sha256(evidence()), observation["artifact_sha256"])
        self.assertFalse(result["traffic"]["activation_allowed"])
        self.assertEqual(0, result["traffic"]["production_allocation_percent"])
        self.assertEqual("not_published", result["gtm"]["publish_status"])
        self.assertEqual("draft_not_started", result["growthbook"]["status"])

    def test_post_observation_manifest_is_exact_and_idempotent(self) -> None:
        result = self.record()
        self.assertEqual(validator.EXPECTED_ACTIVATION, result)
        rerun = record_zero_collector_observation(
            result,
            evidence(),
            evidence_sha256=evidence_sha256(evidence()),
            expected_workflow_run_id=RUN_ID,
            expected_main_commit=MAIN_COMMIT,
        )
        self.assertEqual(result, rerun)

    def test_activation_validator_accepts_recorded_closed_state(self) -> None:
        result = self.record()
        workspace = json.loads(validator.WORKSPACE_PATH.read_text(encoding="utf-8"))
        registry = json.loads(validator.REGISTRY_PATH.read_text(encoding="utf-8"))
        validator.validate_activation_handoff(result, workspace, registry)

    def test_rejects_any_nonzero_request_or_receipt_count(self) -> None:
        for field in ("api_request_count", "accepted_receipt_count"):
            with self.subTest(field=field):
                altered = evidence()
                altered[field] = 1
                with self.assertRaisesRegex(ZeroCollectorEvidenceError, "exactly zero"):
                    self.record(altered)

    def test_rejects_runtime_source_safety_and_identity_drift(self) -> None:
        cases = (
            (("runtime", "image_digest"), "sha256:" + "0" * 64, "runtime image"),
            (("runtime", "private_ip"), "192.0.2.10", "private IP boundary"),
            (("source", "gtm_publish_status"), "published", "source boundary"),
            (("safety", "aws_mutations"), True, "safety boundary"),
            (("main_commit",), "0" * 40, "main commit"),
        )
        for path, replacement, message in cases:
            with self.subTest(path=path):
                altered = evidence()
                target = altered
                for key in path[:-1]:
                    target = target[key]  # type: ignore[index,assignment]
                target[path[-1]] = replacement  # type: ignore[index]
                with self.assertRaisesRegex(ZeroCollectorEvidenceError, message):
                    self.record(altered)

    def test_rejects_hash_mismatch_noncanonical_bytes_and_manifest_drift(self) -> None:
        payload = evidence()
        with self.assertRaisesRegex(ZeroCollectorEvidenceError, "SHA-256 mismatch"):
            record_zero_collector_observation(
                expected_pending_activation(),
                payload,
                evidence_sha256="0" * 64,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )

        drifted = expected_pending_activation()
        drifted["traffic"]["activation_allowed"] = True
        with self.assertRaisesRegex(ZeroCollectorEvidenceError, "exact pending"):
            record_zero_collector_observation(
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
            with self.assertRaisesRegex(ZeroCollectorEvidenceError, "not canonical"):
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
        with self.assertRaisesRegex(ZeroCollectorEvidenceError, "field set drift"):
            validate_zero_collector_observation(
                altered,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=MAIN_COMMIT,
            )


if __name__ == "__main__":
    unittest.main()
