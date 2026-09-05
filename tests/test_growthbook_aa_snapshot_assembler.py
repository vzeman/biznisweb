from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import pathlib
import tempfile
import unittest

from scripts.assemble_growthbook_aa_snapshot import (
    SnapshotAssemblyError,
    assemble_snapshot,
    main,
)
from scripts.evaluate_growthbook_aa import _srm, evaluate, load_config


CONFIG = load_config(pathlib.Path("projects/vevo/growthbook_aa_acceptance.json"))
AUTOMATED_RUN_ID = "32490000001"
AUTOMATED_COMMIT = "1" * 40
MANUAL_RUN_ID = "32490000002"
MANUAL_COMMIT = "2" * 40
FROM_UTC = "2026-08-22T22:00:00Z"
THROUGH_UTC = "2026-08-29T22:00:00Z"


def automated_evidence() -> dict[str, object]:
    counts = {"control": 510, "variant": 490}
    return {
        "schema_version": 2,
        "evidence_type": "vevo_growthbook_aa_automated_evidence",
        "experiment_id": "vevo-sk-aa-001",
        "from_utc": FROM_UTC,
        "through_utc": THROUGH_UTC,
        "source_run_id": AUTOMATED_RUN_ID,
        "source_main_commit": AUTOMATED_COMMIT,
        "quality_source_sha256": "d" * 64,
        "production_runtime": {
            "instance_id": "N/A:Fargate",
            "private_ip": "172.31.20.10",
            "service": "vevo-growthbook-collector-production",
            "path": "/app",
            "task_id": "a" * 32,
            "image_digest": f"sha256:{'b' * 64}",
            "stack_name": "vevo-growthbook-production",
            "database": "vevo_growthbook_production",
        },
        "pipeline_counts": {
            "collector_received_event_count": 5010,
            "collector_unique_accepted_event_count": 5000,
            "collector_duplicate_event_count": 10,
            "athena_unique_event_count": 4995,
            "reporting_unique_event_count": 4990,
        },
        "reporting_quality": {
            "raw_event_count": 4995,
            "unique_event_count": 4990,
            "duplicate_event_count": 5,
            "orphan_event_count": 0,
            "eligible_device_count": 1000,
            "contaminated_device_count": 0,
            "srm_p_value": _srm(counts, {"control": 0.5, "variant": 0.5}),
            "unique_transaction_count": 60,
            "exact_joined_transaction_count": 60,
            "unmatched_transaction_count": 0,
            "ambiguous_transaction_count": 0,
            "variation_health": {
                "control": {
                    "eligible_devices": 510,
                    "measured_page_loads": 250,
                    "lcp_p75_ms": 1300,
                    "inp_p75_ms": 100,
                    "cls_p75_milli": 5,
                    "client_error_device_rate_pct": 0.2,
                },
                "variant": {
                    "eligible_devices": 490,
                    "measured_page_loads": 240,
                    "lcp_p75_ms": 1350,
                    "inp_p75_ms": 105,
                    "cls_p75_milli": 6,
                    "client_error_device_rate_pct": 0.3,
                },
            },
        },
        "meta_dimension_audit": {
            "meta_exposure_count": 320,
            "complete_stable_dimension_exposure_count": 120,
            "invalid_dimension_row_count": 0,
            "forbidden_click_identifier_count": 0,
        },
        "privacy_audit": {
            "total_stored_row_count": 5990,
            "audited_row_count": 5990,
            "pii_finding_count": 0,
            "forbidden_field_finding_count": 0,
            "raw_ip_address_stored_count": 0,
            "full_url_stored_count": 0,
            "click_identifier_stored_count": 0,
            "customer_field_stored_count": 0,
        },
        "consent_audit": {
            "pre_consent_request_count": 0,
            "non_analytical_consent_exposure_count": 0,
            "post_withdrawal_event_count": 0,
        },
        "source_read_only": True,
        "contains_raw_aws_payloads": False,
        "contains_cloudwatch_messages": False,
        "contains_event_or_device_ids": False,
        "contains_customer_or_order_data": False,
        "mutation_observed": False,
    }


def manual_evidence() -> dict[str, object]:
    return {
        "schema_version": 1,
        "evidence_type": "vevo_growthbook_aa_manual_qa_evidence",
        "experiment_id": "vevo-sk-aa-001",
        "from_utc": FROM_UTC,
        "through_utc": THROUGH_UTC,
        "source_run_id": MANUAL_RUN_ID,
        "source_main_commit": MANUAL_COMMIT,
        "production_allocation_percent": 100,
        "identical_variations_verified": True,
        "growthbook_srm_warning": False,
        "growthbook_variation_counts": {"control": 509, "variant": 491},
        "commerce_health": {
            "checkout_runtime_error_count": 0,
            "duplicate_ga4_purchase_event_count": 0,
            "duplicate_meta_purchase_event_count": 0,
            "price_cart_checkout_mutation_observed": False,
            "add_to_cart_behavior_regression_observed": False,
            "rollback_test_passed": True,
        },
        "qa_checklist": {
            "desktop_passed": True,
            "mobile_passed": True,
            "consent_accept_passed": True,
            "consent_reject_passed": True,
            "consent_withdrawal_passed": True,
        },
        "tag_assistant_connected": True,
        "production_storefront_observed": True,
        "growthbook_read_only": True,
        "contains_event_or_device_ids": False,
        "contains_customer_or_order_data": False,
        "unplanned_mutation_observed": False,
    }


def assemble(
    automated: dict[str, object] | None = None,
    manual: dict[str, object] | None = None,
) -> dict[str, object]:
    return assemble_snapshot(
        automated or automated_evidence(),
        manual or manual_evidence(),
        expected_automated_run_id=AUTOMATED_RUN_ID,
        expected_automated_commit=AUTOMATED_COMMIT,
        expected_manual_run_id=MANUAL_RUN_ID,
        expected_manual_commit=MANUAL_COMMIT,
        config=CONFIG,
    )


def canonical(payload: dict[str, object]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


class GrowthBookAaSnapshotAssemblerTests(unittest.TestCase):
    def test_assembles_exact_snapshot_and_preserves_pass_evaluation(self) -> None:
        snapshot = assemble()
        self.assertEqual("PASS", evaluate(snapshot, CONFIG)["verdict"])
        self.assertEqual(FROM_UTC, snapshot["full_allocation_started_at_utc"])
        self.assertEqual(5010, snapshot["pipeline_counts"]["collector_received_event_count"])
        serialized = json.dumps(snapshot, sort_keys=True).lower()
        for forbidden in (
            "source_run_id",
            "private_ip",
            "task_id",
            "event_id",
            "device_id",
            "transaction_id",
            "email",
            "fbclid",
        ):
            self.assertNotIn(forbidden, serialized)

    def test_rejects_extended_schema_and_independent_provenance_mismatch(self) -> None:
        extended = automated_evidence()
        extended["unexpected"] = 1
        with self.assertRaisesRegex(SnapshotAssemblyError, "field set drift"):
            assemble(automated=extended)

        wrong_run = automated_evidence()
        wrong_run["source_run_id"] = "32490000999"
        with self.assertRaisesRegex(SnapshotAssemblyError, "run ID mismatch"):
            assemble(automated=wrong_run)

        wrong_type = automated_evidence()
        wrong_type["source_run_id"] = int(AUTOMATED_RUN_ID)
        with self.assertRaisesRegex(SnapshotAssemblyError, "source run ID is invalid"):
            assemble(automated=wrong_type)

    def test_rejects_window_runtime_and_safety_drift(self) -> None:
        wrong_window = manual_evidence()
        wrong_window["through_utc"] = "2026-08-30T22:00:00Z"
        with self.assertRaisesRegex(SnapshotAssemblyError, "component window mismatch"):
            assemble(manual=wrong_window)

        wrong_runtime = automated_evidence()
        wrong_runtime["production_runtime"]["path"] = "/tmp"
        with self.assertRaisesRegex(SnapshotAssemblyError, "runtime path drift"):
            assemble(automated=wrong_runtime)

        public_runtime = automated_evidence()
        public_runtime["production_runtime"]["private_ip"] = "8.8.8.8"
        with self.assertRaisesRegex(SnapshotAssemblyError, "outside the VEVO private VPC"):
            assemble(automated=public_runtime)

        unsafe = automated_evidence()
        unsafe["contains_cloudwatch_messages"] = True
        with self.assertRaisesRegex(SnapshotAssemblyError, "must be false"):
            assemble(automated=unsafe)

        wrong_allocation = manual_evidence()
        wrong_allocation["production_allocation_percent"] = 0
        with self.assertRaisesRegex(SnapshotAssemblyError, "Production allocation drift"):
            assemble(manual=wrong_allocation)

    def test_rejects_component_identities_before_evaluator(self) -> None:
        bad_pipeline = automated_evidence()
        bad_pipeline["pipeline_counts"]["collector_duplicate_event_count"] = 9
        with self.assertRaisesRegex(SnapshotAssemblyError, "receipt identity drift"):
            assemble(automated=bad_pipeline)

        bad_health = automated_evidence()
        del bad_health["reporting_quality"]["variation_health"]["control"]["lcp_p75_ms"]
        with self.assertRaisesRegex(SnapshotAssemblyError, "field set drift"):
            assemble(automated=bad_health)

    def test_cli_requires_canonical_hash_bound_components(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            automated_path = temporary / "automated.json"
            manual_path = temporary / "manual.json"
            output_path = temporary / "snapshot.json"
            automated_raw = canonical(automated_evidence())
            manual_raw = canonical(manual_evidence())
            automated_path.write_bytes(automated_raw)
            manual_path.write_bytes(manual_raw)
            arguments = [
                "--automated",
                str(automated_path),
                "--automated-sha256",
                hashlib.sha256(automated_raw).hexdigest(),
                "--automated-run-id",
                AUTOMATED_RUN_ID,
                "--automated-main-commit",
                AUTOMATED_COMMIT,
                "--manual",
                str(manual_path),
                "--manual-sha256",
                hashlib.sha256(manual_raw).hexdigest(),
                "--manual-run-id",
                MANUAL_RUN_ID,
                "--manual-main-commit",
                MANUAL_COMMIT,
                "--output",
                str(output_path),
            ]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, main(arguments))
            raw = output_path.read_bytes()
            self.assertEqual(raw, canonical(json.loads(raw)))

            noncanonical = copy.deepcopy(manual_evidence())
            manual_path.write_text(json.dumps(noncanonical, indent=2), encoding="utf-8")
            arguments[arguments.index("--manual-sha256") + 1] = hashlib.sha256(
                manual_path.read_bytes()
            ).hexdigest()
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(2, main(arguments))

    def test_cli_rejects_tampered_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            automated_path = temporary / "automated.json"
            manual_path = temporary / "manual.json"
            automated_path.write_bytes(canonical(automated_evidence()))
            manual_path.write_bytes(canonical(manual_evidence()))
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    2,
                    main(
                        [
                            "--automated",
                            str(automated_path),
                            "--automated-sha256",
                            "0" * 64,
                            "--automated-run-id",
                            AUTOMATED_RUN_ID,
                            "--automated-main-commit",
                            AUTOMATED_COMMIT,
                            "--manual",
                            str(manual_path),
                            "--manual-sha256",
                            hashlib.sha256(manual_path.read_bytes()).hexdigest(),
                            "--manual-run-id",
                            MANUAL_RUN_ID,
                            "--manual-main-commit",
                            MANUAL_COMMIT,
                            "--output",
                            str(temporary / "snapshot.json"),
                        ]
                    ),
                )


if __name__ == "__main__":
    unittest.main()
