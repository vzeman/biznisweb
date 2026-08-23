from __future__ import annotations

import copy
import hashlib
import json
import unittest
from datetime import datetime, timezone
from unittest import mock

from scripts.record_growthbook_natural_evidence import (
    _changed_leaf_paths,
    canonical_evidence_bytes,
)
from scripts.record_growthbook_production_aa_collector_evidence import (
    ALLOWED_CHANGED_PATHS,
    CollectorActivationEvidenceError,
    _accepted_evidence_serializations,
    _legacy_compact_evidence_bytes,
    record_collector_activation_evidence,
    validate_collector_activation_evidence,
)
from scripts import validate_growthbook_production_aa_activation as activation_validator
from scripts import record_growthbook_production_aa_collector_evidence as recorder


RUN_ID = "32499999999"
COMMIT = "a" * 40
IMAGE_DIGEST = "sha256:" + "b" * 64
ENDPOINT_HOST_SHA256 = "c" * 64


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 8, 22, 3, 0, tzinfo=timezone.utc)
        return value if tz is None else value.astimezone(tz)


def evidence() -> dict[str, object]:
    return {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_production_aa_collector_activation",
        "status": "passed",
        "repository": "vzeman/biznisweb",
        "workflow": ".github/workflows/deploy-vevo-growthbook-production-aa-collector.yml",
        "workflow_run_id": RUN_ID,
        "main_commit": COMMIT,
        "verified_at_utc": "2026-08-22T02:00:00Z",
        "aws": {
            "account_id": "919341186960",
            "region": "eu-central-1",
            "stack_name": "vevo-growthbook-production",
            "stack_status": "UPDATE_COMPLETE",
        },
        "deployment": {
            "registry_tracking_keys": ["vevo-sk-aa-001"],
            "image_digest": IMAGE_DIGEST,
            "task_definition": "vevo-growthbook-collector-production:2",
            "public_route_enabled": True,
            "endpoint_host_sha256": ENDPOINT_HOST_SHA256,
            "invalid_probe_raw_snapshot_unchanged": True,
            "growthbook_started": False,
            "gtm_published": False,
            "production_allocation_percent": 0,
            "cta_started": False,
        },
        "host_gate": {
            "instance_id": "N/A:Fargate",
            "private_ip": "172.31.20.10",
            "service": "vevo-growthbook-collector-production",
            "runtime_path": "/app",
            "task_id": "d" * 32,
            "localhost_health_marker_verified": True,
            "localhost_runtime_marker_verified": True,
        },
        "service_runtime": {
            "instance_id": "N/A:Fargate",
            "private_ip": "172.31.20.11",
            "service": "vevo-growthbook-collector-production",
            "runtime_path": "/app",
            "task_id": "e" * 32,
            "target_health": "healthy",
        },
        "safety": {
            "contains_credentials": False,
            "contains_raw_aws_payloads": False,
            "contains_cloudwatch_messages": False,
            "contains_event_or_device_ids": False,
            "contains_customer_or_order_data": False,
            "growthbook_mutations": False,
            "gtm_mutations": False,
            "meta_ads_mutations": False,
            "biznisweb_mutations": False,
            "commerce_mutations": False,
        },
    }


def ready_state() -> tuple[dict, dict, dict]:
    activation = copy.deepcopy(activation_validator.EXPECTED_ACTIVATION)
    activation["status"] = "clone_verified_collector_deploy_ready"
    activation["preconditions"] = {
        "natural_reconciliation_verified": True,
        "route_disabled_foundation_verified": True,
        "production_reader_verified": True,
        "growthbook_clone_verified": True,
    }
    activation["collector"]["deployment_allowed"] = True
    activation["collector"]["registry_entry_present"] = True
    activation["collector"]["public_route_enabled"] = False
    for key in (
        "workflow_run_id",
        "main_commit",
        "image_digest",
        "task_definition",
        "host_gate_task_id",
        "host_gate_private_ip",
        "endpoint_host_sha256",
        "evidence_sha256",
    ):
        activation["collector"][key] = None
    activation["growthbook"]["data_source_id"] = "ds_Production123"
    activation["next_gate"] = "dispatch_production_aa_collector_after_review"

    workspace = json.loads(
        activation_validator.WORKSPACE_PATH.read_text(encoding="utf-8")
    )
    production = workspace["athena"]["production"]
    production["status"] = "route_disabled_foundation_deployed_verified"
    production["reader_provisioning_status"] = (
        "verified_active_encrypted_handoff_ready_for_growthbook"
    )
    production["growthbook_clone"]["status"] = "verified_complete"
    production["growthbook_clone"]["mutation_status"] = (
        "created_and_query_verified"
    )
    production["growthbook_clone"]["target_data_source_id"] = "ds_Production123"

    registry = json.loads(
        activation_validator.REGISTRY_PATH.read_text(encoding="utf-8")
    )
    registry["environments"]["production"] = {
        "vevo-sk-aa-001": copy.deepcopy(
            registry["environments"]["preview"]["vevo-sk-aa-001"]
        )
    }
    return activation, workspace, registry


class GrowthBookProductionAaCollectorEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.datetime_patcher = mock.patch.object(recorder, "datetime", FixedDateTime)
        self.datetime_patcher.start()

    def tearDown(self) -> None:
        self.datetime_patcher.stop()

    def test_valid_sanitized_evidence_records_only_the_reviewed_transition(self) -> None:
        activation, workspace, registry = ready_state()
        observed = evidence()
        digest = hashlib.sha256(canonical_evidence_bytes(observed)).hexdigest()

        result = record_collector_activation_evidence(
            activation,
            workspace,
            registry,
            observed,
            evidence_sha256=digest,
            expected_workflow_run_id=RUN_ID,
            expected_main_commit=COMMIT,
        )

        self.assertEqual(ALLOWED_CHANGED_PATHS, _changed_leaf_paths(activation, result))
        self.assertEqual("collector_verified_ui_preparation_ready", result["status"])
        self.assertFalse(result["collector"]["deployment_allowed"])
        self.assertTrue(result["collector"]["public_route_enabled"])
        self.assertEqual(IMAGE_DIGEST, result["collector"]["image_digest"])
        self.assertEqual(
            ENDPOINT_HOST_SHA256,
            result["collector"]["endpoint_host_sha256"],
        )
        self.assertEqual(0, result["growthbook"]["allocation_percent"])
        self.assertEqual("not_published", result["gtm"]["publish_status"])
        self.assertFalse(result["traffic"]["activation_allowed"])

    def test_tampered_identity_privacy_or_traffic_evidence_is_rejected(self) -> None:
        mutations = (
            ("aws", "account_id", "000000000000"),
            ("deployment", "registry_tracking_keys", ["vevo-sk-product-cta-color-001"]),
            ("deployment", "gtm_published", True),
            ("deployment", "production_allocation_percent", 1),
            ("host_gate", "private_ip", "10.0.0.1"),
            ("safety", "contains_event_or_device_ids", True),
            ("safety", "biznisweb_mutations", True),
        )
        for section, key, value in mutations:
            with self.subTest(section=section, key=key):
                altered = evidence()
                altered[section][key] = value
                with self.assertRaises(CollectorActivationEvidenceError):
                    validate_collector_activation_evidence(
                        altered,
                        expected_workflow_run_id=RUN_ID,
                        expected_main_commit=COMMIT,
                    )

    def test_extra_field_and_reused_service_task_are_rejected(self) -> None:
        altered = evidence()
        altered["credential"] = "forbidden"
        with self.assertRaisesRegex(CollectorActivationEvidenceError, "field set"):
            validate_collector_activation_evidence(
                altered,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=COMMIT,
            )

        altered = evidence()
        altered["service_runtime"]["task_id"] = altered["host_gate"]["task_id"]
        with self.assertRaisesRegex(CollectorActivationEvidenceError, "must be distinct"):
            validate_collector_activation_evidence(
                altered,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=COMMIT,
            )

    def test_pending_manifest_or_wrong_hash_cannot_be_recorded(self) -> None:
        activation, workspace, registry = ready_state()
        observed = evidence()
        digest = hashlib.sha256(canonical_evidence_bytes(observed)).hexdigest()
        activation["collector"]["deployment_allowed"] = False
        with self.assertRaisesRegex(CollectorActivationEvidenceError, "not open"):
            record_collector_activation_evidence(
                activation,
                workspace,
                registry,
                observed,
                evidence_sha256=digest,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=COMMIT,
            )

        activation, workspace, registry = ready_state()
        with self.assertRaisesRegex(CollectorActivationEvidenceError, "SHA-256 mismatch"):
            record_collector_activation_evidence(
                activation,
                workspace,
                registry,
                observed,
                evidence_sha256="f" * 64,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=COMMIT,
            )

    def test_compact_artifact_recovery_is_bound_to_one_exact_run(self) -> None:
        observed = evidence()
        legacy_bytes = _legacy_compact_evidence_bytes(observed)
        legacy_hash = hashlib.sha256(legacy_bytes).hexdigest()
        with (
            mock.patch.object(recorder, "LEGACY_COMPACT_WORKFLOW_RUN_ID", RUN_ID),
            mock.patch.object(recorder, "LEGACY_COMPACT_MAIN_COMMIT", COMMIT),
            mock.patch.object(
                recorder,
                "LEGACY_COMPACT_EVIDENCE_SHA256",
                legacy_hash,
            ),
        ):
            accepted = _accepted_evidence_serializations(
                observed,
                expected_workflow_run_id=RUN_ID,
                expected_main_commit=COMMIT,
            )
            self.assertEqual((canonical_evidence_bytes(observed), legacy_bytes), accepted)
            self.assertEqual(
                (canonical_evidence_bytes(observed),),
                _accepted_evidence_serializations(
                    observed,
                    expected_workflow_run_id="99999999999",
                    expected_main_commit=COMMIT,
                ),
            )

        self.assertEqual("32644408714", recorder.LEGACY_COMPACT_WORKFLOW_RUN_ID)
        self.assertEqual(
            "57b29c3b166eabbbabee4d3b8e69d1b56e2ae8e2",
            recorder.LEGACY_COMPACT_MAIN_COMMIT,
        )
        self.assertEqual(
            "1e156ebdd94f88f7858c0e0b2ddb443fdabe01787ee6f7d673ac80197492ab88",
            recorder.LEGACY_COMPACT_EVIDENCE_SHA256,
        )


if __name__ == "__main__":
    unittest.main()
