from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from scripts.evaluate_growthbook_aa import evaluate, load_config
from scripts import validate_growthbook_workspace as workspace_validator
from scripts.record_growthbook_aa_completion import (
    AaCompletionRecordingError,
    canonical_json_bytes,
    load_canonical,
    record_pass,
    record_stop,
    validate_manifest,
)
from scripts.record_growthbook_aa_evidence_gates import (
    open_automated_producer,
    open_manual_producer,
    record_component,
)
from scripts.validate_growthbook_aa_measurement_window import canonical_evidence_bytes
from tests.test_growthbook_aa_evaluator import snapshot as evaluator_snapshot
from tests.test_growthbook_aa_evidence_gate_recorder import (
    AUTOMATED_COMMIT,
    AUTOMATED_RUN_ID,
    MANUAL_COMMIT,
    MANUAL_RUN_ID,
    component_evidence,
    observation as manual_observation,
    quality_key,
    quality_report,
    resolved_snapshot,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]
SNAPSHOT_RUN_ID = "32850000001"
SNAPSHOT_COMMIT = "5" * 40


def load(name: str) -> dict[str, object]:
    return json.loads((ROOT / "projects" / "vevo" / name).read_text(encoding="utf-8"))


def build_snapshot_manifest() -> dict[str, object]:
    manifest = resolved_snapshot()
    quality = quality_report()
    manifest = open_automated_producer(
        manifest,
        quality,
        quality_report_key=quality_key(),
        quality_report_sha256=hashlib.sha256(
            canonical_evidence_bytes(quality)
        ).hexdigest(),
    )
    reviewed = manual_observation()
    manifest = open_manual_producer(
        manifest,
        reviewed,
        observation_sha256=hashlib.sha256(
            canonical_evidence_bytes(reviewed)
        ).hexdigest(),
    )
    automated = component_evidence("automated")
    manifest = record_component(
        manifest,
        automated,
        component_name="automated",
        evidence_sha256=hashlib.sha256(
            canonical_evidence_bytes(automated)
        ).hexdigest(),
        expected_workflow_run_id=AUTOMATED_RUN_ID,
        expected_main_commit=AUTOMATED_COMMIT,
    )
    manual = component_evidence("manual")
    return record_component(
        manifest,
        manual,
        component_name="manual",
        evidence_sha256=hashlib.sha256(canonical_evidence_bytes(manual)).hexdigest(),
        expected_workflow_run_id=MANUAL_RUN_ID,
        expected_main_commit=MANUAL_COMMIT,
    )


def aa_snapshot() -> dict[str, object]:
    value = evaluator_snapshot()
    value["full_allocation_started_at_utc"] = "2026-08-25T22:00:00Z"
    value["evaluated_at_utc"] = "2026-09-01T22:00:00Z"
    return value


def snapshot_provenance(
    snapshot_manifest: dict[str, object],
    *,
    snapshot_sha256: str,
    decision_sha256: str,
    workflow_run_id: str = SNAPSHOT_RUN_ID,
    main_commit: str = SNAPSHOT_COMMIT,
) -> dict[str, object]:
    sources = {}
    for component_name in ("automated_evidence", "manual_qa_evidence"):
        component = snapshot_manifest[component_name]
        sources[component_name] = {
            "workflow": component["workflow"],
            "workflow_run_id": str(component["run_id"]),
            "main_commit": component["main_commit"],
            "artifact_name": component["artifact_name"],
            "artifact_sha256": component["sha256"],
        }
    return {
        "artifact_name": "vevo-growthbook-aa-snapshot",
        "evidence_type": "vevo_growthbook_aa_snapshot_provenance",
        "files": {
            "vevo-growthbook-aa-decision.json": {"sha256": decision_sha256},
            "vevo-growthbook-aa-snapshot.json": {"sha256": snapshot_sha256},
        },
        "main_commit": main_commit,
        "repository": "vzeman/biznisweb",
        "safety": {
            "contains_component_artifacts": False,
            "contains_customer_or_order_data": False,
            "contains_event_or_device_ids": False,
            "contains_raw_aws_payloads": False,
            "external_or_automatic_mutation": False,
            "winner_calls_allowed": False,
        },
        "schema_version": 1,
        "source_components": sources,
        "workflow": ".github/workflows/build-vevo-growthbook-production-aa-snapshot.yml",
        "workflow_run_attempt": 1,
        "workflow_run_id": workflow_run_id,
    }


def stop_observation(
    *, snapshot_sha256: str, decision_sha256: str, provenance_sha256: str
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "observation_type": "vevo_growthbook_production_aa_stop_readback",
        "experiment_id": "vevo-sk-aa-001",
        "observed_at_utc": "2026-09-02T08:15:00Z",
        "aa_pass_snapshot_sha256": snapshot_sha256,
        "aa_pass_decision_sha256": decision_sha256,
        "aa_pass_provenance_sha256": provenance_sha256,
        "growthbook": {
            "build": "5.0.1+8f1db44",
            "project_id": "prj_2CeEJc6J9FwQFix9UhsnKr",
            "environment": "production",
            "aa_experiment_id": "exp_19g6mmt5wugpk",
            "aa_experiment_status": "stopped",
            "aa_production_live_rule_count": 0,
            "aa_production_allocation_percent": 0,
            "aa_feature_live_revision": 4,
            "aa_feature_production_enabled": False,
            "aa_feature_staging_enabled": True,
            "aa_feature_live_rule_count_by_environment": {
                "production": 0,
                "staging": 1,
            },
            "cta_experiment_id": "exp_19g6mmt1qxzrp",
            "cta_experiment_status": "draft_not_started",
            "cta_production_live_rule_count": 0,
            "cta_production_allocation_percent": 0,
        },
        "gtm": {
            "account_id": "6254499282",
            "container_id": "198135331",
            "public_container_id": "GTM-5ZB5LFGB",
            "container_version_id": "15",
            "growthbook_loader_active": True,
            "unprocessed_changes": {"added": 0, "modified": 0, "removed": 0},
        },
        "storefront": {
            "product_path": "/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute",
            "desktop_verified": True,
            "mobile_verified": True,
            "aa_assignment_present": False,
            "cta_class_applied": False,
            "add_to_cart_text_unchanged": True,
            "console_error_count": 0,
            "price_mutated": False,
            "cart_mutated": False,
            "checkout_or_order_mutated": False,
        },
        "mutation_boundaries": {
            "growthbook_manual_mutation_performed": True,
            "growthbook_manual_mutation_scope": (
                "stop_exact_aa_experiment_and_remove_only_its_production_live_rule"
            ),
            "automatic_growthbook_mutation_performed": False,
            "gtm_mutation_performed": False,
            "meta_ads_mutation_performed": False,
            "biznisweb_mutation_performed": False,
            "collector_or_reporting_mutation_performed": False,
            "price_cart_checkout_order_mutation_performed": False,
        },
    }


class GrowthBookAaCompletionRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.completion = load("growthbook_production_aa_completion.json")
        self.activation = load("growthbook_production_aa_activation.json")
        self.pending_snapshot_manifest = load("growthbook_aa_snapshot.json")
        self.snapshot_manifest = build_snapshot_manifest()
        self.snapshot = aa_snapshot()
        self.decision = evaluate(
            self.snapshot,
            load_config(ROOT / "projects" / "vevo" / "growthbook_aa_acceptance.json"),
        )
        self.snapshot_sha = hashlib.sha256(
            canonical_json_bytes(self.snapshot)
        ).hexdigest()
        self.decision_sha = hashlib.sha256(
            canonical_json_bytes(self.decision)
        ).hexdigest()
        self.provenance = self._provenance()
        self.provenance_sha = hashlib.sha256(
            canonical_json_bytes(self.provenance)
        ).hexdigest()

    def _provenance(
        self,
        *,
        decision_sha256: str | None = None,
        workflow_run_id: str = SNAPSHOT_RUN_ID,
        main_commit: str = SNAPSHOT_COMMIT,
    ) -> dict[str, object]:
        return snapshot_provenance(
            self.snapshot_manifest,
            snapshot_sha256=self.snapshot_sha,
            decision_sha256=decision_sha256 or self.decision_sha,
            workflow_run_id=workflow_run_id,
            main_commit=main_commit,
        )

    def record_pass(self) -> dict[str, object]:
        return record_pass(
            self.completion,
            self.activation,
            self.snapshot_manifest,
            self.snapshot,
            self.decision,
            self.provenance,
            workflow_run_id=SNAPSHOT_RUN_ID,
            main_commit=SNAPSHOT_COMMIT,
            snapshot_sha256=self.snapshot_sha,
            decision_sha256=self.decision_sha,
            provenance_sha256=self.provenance_sha,
        )

    def test_checked_in_contract_is_closed_and_valid(self) -> None:
        validate_manifest(
            self.completion,
            self.activation,
            self.pending_snapshot_manifest,
        )
        self.assertFalse(
            self.completion["release_boundaries"]["manual_growthbook_stop_allowed"]
        )
        self.assertFalse(self.completion["release_boundaries"]["cta_activation_allowed"])

    def test_records_only_independently_recomputed_pass(self) -> None:
        recorded = self.record_pass()
        self.assertEqual(
            "aa_pass_recorded_manual_stop_review_allowed", recorded["status"]
        )
        self.assertTrue(
            recorded["release_boundaries"]["manual_growthbook_stop_allowed"]
        )
        self.assertFalse(
            recorded["release_boundaries"]["automatic_growthbook_mutation_allowed"]
        )
        self.assertFalse(recorded["release_boundaries"]["cta_activation_allowed"])
        validate_manifest(recorded, self.activation, self.snapshot_manifest)
        self.assertEqual(
            recorded,
            record_pass(
                recorded,
                self.activation,
                self.snapshot_manifest,
                self.snapshot,
                self.decision,
                self.provenance,
                workflow_run_id=SNAPSHOT_RUN_ID,
                main_commit=SNAPSHOT_COMMIT,
                snapshot_sha256=self.snapshot_sha,
                decision_sha256=self.decision_sha,
                provenance_sha256=self.provenance_sha,
            ),
        )

        tampered = copy.deepcopy(self.decision)
        tampered["gates"][0]["status"] = "fail"
        tampered_sha = hashlib.sha256(canonical_json_bytes(tampered)).hexdigest()
        tampered_provenance = self._provenance(decision_sha256=tampered_sha)
        with self.assertRaisesRegex(
            AaCompletionRecordingError, "independent evaluation"
        ):
            record_pass(
                self.completion,
                self.activation,
                self.snapshot_manifest,
                self.snapshot,
                tampered,
                tampered_provenance,
                workflow_run_id=SNAPSHOT_RUN_ID,
                main_commit=SNAPSHOT_COMMIT,
                snapshot_sha256=self.snapshot_sha,
                decision_sha256=tampered_sha,
                provenance_sha256=hashlib.sha256(
                    canonical_json_bytes(tampered_provenance)
                ).hexdigest(),
            )

    def test_rejects_swapped_provenance_run_commit_file_or_component(self) -> None:
        cases = (
            (
                "workflow run mismatch",
                lambda value: value.update({"workflow_run_id": "32850000002"}),
            ),
            (
                "main commit mismatch",
                lambda value: value.update({"main_commit": "6" * 40}),
            ),
            (
                "hash mismatch: vevo-growthbook-aa-decision.json",
                lambda value: value["files"][
                    "vevo-growthbook-aa-decision.json"
                ].update({"sha256": "f" * 64}),
            ),
            (
                "source mismatch: automated_evidence",
                lambda value: value["source_components"][
                    "automated_evidence"
                ].update({"artifact_sha256": "f" * 64}),
            ),
        )
        for expected, mutate in cases:
            with self.subTest(expected=expected):
                provenance = self._provenance()
                mutate(provenance)
                with self.assertRaisesRegex(AaCompletionRecordingError, expected):
                    record_pass(
                        self.completion,
                        self.activation,
                        self.snapshot_manifest,
                        self.snapshot,
                        self.decision,
                        provenance,
                        workflow_run_id=SNAPSHOT_RUN_ID,
                        main_commit=SNAPSHOT_COMMIT,
                        snapshot_sha256=self.snapshot_sha,
                        decision_sha256=self.decision_sha,
                        provenance_sha256=hashlib.sha256(
                            canonical_json_bytes(provenance)
                        ).hexdigest(),
                    )

    def test_rejects_rebinding_an_already_recorded_pass(self) -> None:
        recorded = self.record_pass()
        replacement_run_id = "32850000002"
        replacement_commit = "6" * 40
        replacement = self._provenance(
            workflow_run_id=replacement_run_id,
            main_commit=replacement_commit,
        )
        with self.assertRaisesRegex(
            AaCompletionRecordingError, "already bound to a different artifact"
        ):
            record_pass(
                recorded,
                self.activation,
                self.snapshot_manifest,
                self.snapshot,
                self.decision,
                replacement,
                workflow_run_id=replacement_run_id,
                main_commit=replacement_commit,
                snapshot_sha256=self.snapshot_sha,
                decision_sha256=self.decision_sha,
                provenance_sha256=hashlib.sha256(
                    canonical_json_bytes(replacement)
                ).hexdigest(),
            )

    def test_rejects_stop_before_pass(self) -> None:
        observed = stop_observation(
            snapshot_sha256=self.snapshot_sha,
            decision_sha256=self.decision_sha,
            provenance_sha256=self.provenance_sha,
        )
        with self.assertRaisesRegex(AaCompletionRecordingError, "before PASS"):
            record_stop(
                self.completion,
                self.activation,
                self.pending_snapshot_manifest,
                load("growthbook_workspace.json"),
                observed,
                observation_sha256=hashlib.sha256(
                    canonical_json_bytes(observed)
                ).hexdigest(),
            )

    def test_records_safe_stop_and_only_prepares_cta_draft(self) -> None:
        passed = self.record_pass()
        observed = stop_observation(
            snapshot_sha256=self.snapshot_sha,
            decision_sha256=self.decision_sha,
            provenance_sha256=self.provenance_sha,
        )
        digest = hashlib.sha256(canonical_json_bytes(observed)).hexdigest()
        recorded, workspace = record_stop(
            passed,
            self.activation,
            self.snapshot_manifest,
            load("growthbook_workspace.json"),
            observed,
            observation_sha256=digest,
        )
        self.assertEqual(
            "production_aa_stopped_verified_cta_activation_blocked",
            recorded["status"],
        )
        self.assertFalse(
            recorded["release_boundaries"]["manual_growthbook_stop_allowed"]
        )
        self.assertFalse(recorded["release_boundaries"]["cta_activation_allowed"])
        self.assertEqual(0, workspace["workspace"]["production_allocation_percent"])
        self.assertFalse(workspace["decision_gates"]["production_activation_allowed"])
        experiments = {row["tracking_key"]: row for row in workspace["experiments"]}
        aa = experiments["vevo-sk-aa-001"]
        cta = experiments["vevo-sk-product-cta-color-001"]
        self.assertEqual("stopped_production_aa_pass_verified", aa["status"])
        self.assertEqual(0, aa["production_allocation_percent"])
        self.assertEqual(["staging"], aa["feature_rule_environments"])
        self.assertEqual("draft", cta["status"])
        self.assertEqual("draft", cta["feature_rule_status"])
        self.assertEqual(0, cta["production_allocation_percent"])
        validate_manifest(
            recorded,
            self.activation,
            self.snapshot_manifest,
            observation=observed,
        )
        self.assertEqual(
            (recorded, workspace),
            record_stop(
                recorded,
                self.activation,
                self.snapshot_manifest,
                workspace,
                observed,
                observation_sha256=digest,
            ),
        )

        wrong_provenance = copy.deepcopy(observed)
        wrong_provenance["aa_pass_provenance_sha256"] = "f" * 64
        with self.assertRaisesRegex(
            AaCompletionRecordingError, "observation provenance binding drift"
        ):
            record_stop(
                passed,
                self.activation,
                self.snapshot_manifest,
                load("growthbook_workspace.json"),
                wrong_provenance,
                observation_sha256=hashlib.sha256(
                    canonical_json_bytes(wrong_provenance)
                ).hexdigest(),
            )

        unsafe = copy.deepcopy(observed)
        unsafe["mutation_boundaries"]["meta_ads_mutation_performed"] = True
        with self.assertRaisesRegex(AaCompletionRecordingError, "unsafe mutation"):
            record_stop(
                passed,
                self.activation,
                self.snapshot_manifest,
                load("growthbook_workspace.json"),
                unsafe,
                observation_sha256=hashlib.sha256(
                    canonical_json_bytes(unsafe)
                ).hexdigest(),
            )

    def test_canonical_artifact_loader_rejects_pretty_printed_input(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = pathlib.Path(temporary_directory) / "snapshot.json"
            path.write_text(json.dumps(self.snapshot, indent=2), encoding="utf-8")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(AaCompletionRecordingError, "canonical JSON"):
                load_canonical(path, digest, "A/A snapshot")

    def test_workspace_validator_accepts_only_the_bound_post_aa_state(self) -> None:
        passed = self.record_pass()
        observed = stop_observation(
            snapshot_sha256=self.snapshot_sha,
            decision_sha256=self.decision_sha,
            provenance_sha256=self.provenance_sha,
        )
        digest = hashlib.sha256(canonical_json_bytes(observed)).hexdigest()
        recorded, workspace = record_stop(
            passed,
            self.activation,
            self.snapshot_manifest,
            load("growthbook_workspace.json"),
            observed,
            observation_sha256=digest,
        )
        reporting = json.loads(
            workspace_validator.REPORTING_PATH.read_text(encoding="utf-8")
        )
        registry = json.loads(
            workspace_validator.REGISTRY_PATH.read_text(encoding="utf-8")
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            completion_path = temporary / "completion.json"
            snapshot_path = temporary / "snapshot-manifest.json"
            observation_path = temporary / "observation.json"
            completion_path.write_text(
                json.dumps(recorded, indent=2) + "\n", encoding="utf-8"
            )
            snapshot_path.write_text(
                json.dumps(self.snapshot_manifest, indent=2) + "\n",
                encoding="utf-8",
            )
            observation_path.write_bytes(canonical_json_bytes(observed))
            with (
                mock.patch.object(
                    workspace_validator, "AA_COMPLETION_PATH", completion_path
                ),
                mock.patch.object(
                    workspace_validator, "AA_SNAPSHOT_PATH", snapshot_path
                ),
                mock.patch.object(
                    workspace_validator,
                    "AA_COMPLETION_OBSERVATION_PATH",
                    observation_path,
                ),
                mock.patch.object(
                    workspace_validator,
                    "_load",
                    side_effect=[workspace, reporting, registry],
                ),
            ):
                workspace_validator.validate()

            unsafe_workspace = copy.deepcopy(workspace)
            unsafe_workspace["experiments"][1][
                "production_allocation_percent"
            ] = 100
            with (
                mock.patch.object(
                    workspace_validator, "AA_COMPLETION_PATH", completion_path
                ),
                mock.patch.object(
                    workspace_validator, "AA_SNAPSHOT_PATH", snapshot_path
                ),
                mock.patch.object(
                    workspace_validator,
                    "AA_COMPLETION_OBSERVATION_PATH",
                    observation_path,
                ),
                mock.patch.object(
                    workspace_validator,
                    "_load",
                    side_effect=[unsafe_workspace, reporting, registry],
                ),
            ):
                with self.assertRaisesRegex(
                    AssertionError, "Production allocation drift"
                ):
                    workspace_validator.validate()


if __name__ == "__main__":
    unittest.main()
