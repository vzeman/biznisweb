from __future__ import annotations

import copy
import hashlib
import json
import unittest
from pathlib import Path

from scripts import record_growthbook_cta_activation as recorder
from scripts import build_growthbook_cta_runtime_readiness as runtime_builder
from scripts import validate_growthbook_cta_runtime_release as release_validator
from scripts.record_growthbook_aa_completion import canonical_json_bytes as aa_canonical_json_bytes
from tests.test_growthbook_aa_completion_recorder import stop_observation


ROOT = Path(__file__).resolve().parents[1]


def load(relative: str) -> dict:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


class GrowthBookCtaActivationRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.activation = load("projects/vevo/growthbook_cta_activation.json")
        self.completion = load("projects/vevo/growthbook_production_aa_completion.json")
        self.snapshot = load("projects/vevo/growthbook_aa_snapshot.json")
        self.sample = load("projects/vevo/growthbook_cta_sample_plan.json")
        self.lifecycle = load("projects/vevo/growthbook_cta_lifecycle_reconciliation.json")
        self.meta_reporting = load(
            "projects/vevo/growthbook_meta_reporting_contract.json"
        )
        self.workspace = load("projects/vevo/growthbook_workspace.json")
        self.registry = load("growthbook_collector/experiments.json")
        self.snapshot_artifact_hash = "a" * 64
        self.registry_hash = "b" * 64
        self.source_hashes = {
            "aa_completion": "1" * 64,
            "aa_snapshot": "2" * 64,
            "sample_plan": "3" * 64,
            "lifecycle_reconciliation": "4" * 64,
            "design_contract": recorder.EXPECTED_STATIC_HASHES["design_contract"],
            "decision_contract": recorder.EXPECTED_STATIC_HASHES["decision_contract"],
            "meta_reporting_contract": recorder.EXPECTED_STATIC_HASHES[
                "meta_reporting_contract"
            ],
            "collector_registry": self.registry_hash,
        }
        self._make_post_aa_state()
        self.runtime = self._runtime_observation()
        self.runtime_hash = hashlib.sha256(
            recorder.canonical_json_bytes(self.runtime)
        ).hexdigest()

    def _make_post_aa_state(self) -> None:
        self.completion["status"] = (
            "production_aa_stopped_verified_cta_activation_blocked"
        )
        self.completion["aa_pass"]["verdict"] = "PASS"
        self.completion["aa_pass"]["snapshot_sha256"] = (
            self.snapshot_artifact_hash
        )
        self.completion["aa_pass"]["provenance_sha256"] = "e" * 64
        self.completion["stop_readback"]["status"] = "verified_zero_allocation"
        self.snapshot["snapshot_build_allowed"] = True
        self.sample["status"] = "sample_frozen_activation_still_blocked"
        self.sample["final"].update(
            {
                "aa_snapshot_sha256": self.snapshot_artifact_hash,
                "total_sample": 1084,
            }
        )
        self.lifecycle.update(
            {
                "status": "verified_production_14d_refund_creditnote_value_reconciliation",
                "verified": True,
                "observation_sha256": "c" * 64,
                "refund_creditnote_value_parity_verified": True,
                "non_realized_value_policy_verified": True,
            }
        )
        self.workspace["state"] = (
            "production_aa_completed_cta_sample_freeze_pro_quantiles_verified"
        )
        self.workspace["workspace"]["plan_type"] = "pro"
        self.workspace["workspace"]["subscription_or_trial_status"] = (
            "pro_active_paid_monthly_one_seat"
        )
        self.workspace["workspace"]["production_allocation_percent"] = 0
        self.workspace["decision_gates"]["production_activation_allowed"] = False
        clone = self.workspace["athena"]["production"]["growthbook_clone"]
        clone["paid_pro_upgrade_authorized"] = True
        metric_map = {row["key"]: row for row in self.workspace["metrics"]}
        for index, key in enumerate(
            [
                "vevo_cls_p75_milli_24h",
                "vevo_inp_p75_24h",
                "vevo_lcp_p75_24h",
            ],
            start=1,
        ):
            preview_id = f"fact__ProPreview{index}"
            production_id = f"fact__ProProduction{index}"
            metric_map[key].update(
                {
                    "growthbook_id": preview_id,
                    "production_growthbook_id": production_id,
                    "status": "growthbook_pro_preview_and_production_created_query_verified",
                    "blocker": None,
                    "blocker_resolved_date": "2026-09-03",
                    "created_verified_date": "2026-09-03",
                    "analysis_query_verified_date": "2026-09-03",
                }
            )
            clone["source_metric_ids"][key] = preview_id
            clone["target_metric_ids"][key] = production_id
        experiments = {
            row["tracking_key"]: row for row in self.workspace["experiments"]
        }
        experiments["vevo-sk-aa-001"].update(
            {
                "status": "stopped_production_aa_pass_verified",
                "feature_rule_status": "staging_only",
                "feature_rule_environments": ["staging"],
                "production_allocation_percent": 0,
            }
        )
        experiments["vevo-sk-product-cta-color-001"].update(
            {
                "status": "draft",
                "feature_rule_status": "draft",
                "feature_rule_environments": ["staging"],
                "production_allocation_percent": 0,
                "pro_guardrail_metrics": recorder.CTA_GUARDRAILS,
                "pro_quantile_metrics_verified_date": "2026-09-03",
            }
        )
        cta = copy.deepcopy(
            self.registry["environments"]["preview"][
                "vevo-sk-product-cta-color-001"
            ]
        )
        self.registry["environments"]["production"] = {
            "vevo-sk-product-cta-color-001": cta
        }

    def _runtime_observation(self) -> dict:
        return {
            "schema_version": 1,
            "evidence_type": "vevo_growthbook_cta_runtime_readiness",
            "experiment_id": "vevo-sk-product-cta-color-001",
            "observed_at_utc": "2026-09-04T06:00:00Z",
            "workflow": {
                "run_id": "40000000001",
                "main_commit": "d" * 40,
                "conclusion": "success",
            },
            "runtime": {
                "instance_id": "N/A:Fargate",
                "private_ip": "172.31.20.40",
                "service": "vevo-growthbook-collector-production",
                "runtime_path": "/app",
                "task_definition": "vevo-growthbook-collector-production:3",
                "image_digest": "sha256:" + "e" * 64,
                "host_gate_task_id": "a" * 32,
                "host_gate_private_ip": "172.31.20.41",
                "localhost_marker_verified": True,
                "target_health": "healthy",
            },
            "control_plane": {
                "registry_sha256": self.registry_hash,
                "production_registry_experiments": [
                    "vevo-sk-product-cta-color-001"
                ],
                "cta_events_before_start": 0,
                "aa_production_allocation_percent": 0,
                "cta_production_allocation_percent": 0,
                "gtm_container_version_id": "15",
                "gtm_unprocessed_changes": 0,
            },
            "safety": {
                "contains_credentials": False,
                "contains_event_or_device_ids": False,
                "contains_customer_or_order_data": False,
                "meta_ads_mutated": False,
                "biznisweb_mutated": False,
                "price_product_cart_checkout_order_mutated": False,
            },
        }

    def _release_stop_observation(self) -> tuple[dict, bytes]:
        decision_hash = "f" * 64
        self.completion["aa_pass"].update(
            {
                "decision_sha256": decision_hash,
                "evaluated_at_utc": "2026-09-01T22:00:00Z",
            }
        )
        observation = stop_observation(
            snapshot_sha256=self.snapshot_artifact_hash,
            decision_sha256=decision_hash,
            provenance_sha256=self.completion["aa_pass"]["provenance_sha256"],
        )
        raw = aa_canonical_json_bytes(observation)
        self.completion["stop_readback"]["observation_sha256"] = hashlib.sha256(
            raw
        ).hexdigest()
        return observation, raw

    def _start_observation(self) -> dict:
        return {
            "schema_version": 1,
            "evidence_type": "vevo_growthbook_cta_activation_readback",
            "experiment_id": "vevo-sk-product-cta-color-001",
            "feature_key": "vevo-sk-product-cta-color",
            "observed_at_utc": "2026-09-04T07:15:00Z",
            "assignment_started_at_utc": "2026-09-04T07:00:00Z",
            "growthbook": {
                "build": "5.0.1+8f1db44",
                "experiment_id": "exp_19g6mmt1qxzrp",
                "experiment_status": "running",
                "environment": "production_only",
                "traffic_percent": 100,
                "variation_weights": {"control": 0.5, "brand_contrast": 0.5},
                "feature_revision": 4,
                "feature_revision_status": "live",
                "active_production_experiments": [
                    "vevo-sk-product-cta-color-001"
                ],
                "aa_status": "stopped_zero_allocation",
                "data_source_id": "ds_19g6mmt5stlp6",
                "goal_metrics": ["vevo_add_to_cart_24h"],
                "secondary_metrics": [
                    "vevo_average_order_value_7d",
                    "vevo_cancelled_order_rate_14d",
                    "vevo_cm1_per_exposed_device_7d",
                    "vevo_revenue_per_exposed_device_7d",
                    "vevo_purchase_conversion_7d",
                    "vevo_refunded_order_rate_14d",
                ],
                "guardrail_metrics": [
                    "vevo_client_error_device_rate_24h",
                    "vevo_lcp_p75_24h",
                    "vevo_inp_p75_24h",
                    "vevo_cls_p75_milli_24h",
                ],
            },
            "gtm": {
                "container_id": "GTM-5ZB5LFGB",
                "container_version_id": "15",
                "unprocessed_changes": 0,
            },
            "tag_assistant": {
                "connected": True,
                "desktop_verified": True,
                "mobile_verified": True,
                "consent_accept_reject_withdrawal_verified": True,
                "control_observed": True,
                "brand_contrast_observed": True,
                "cta_css_matches_design_contract": True,
                "console_error_count": 0,
            },
            "collector": {
                "accepted_receipt_count": 8,
                "target_exposure_count": 4,
                "repeat_exposed_device_count": 1,
                "sticky_consistent_repeat_device_count": 1,
                "sticky_inconsistent_device_count": 0,
                "observed_variations": ["brand_contrast", "control"],
            },
            "commerce": {
                "cta_text_unchanged": True,
                "cta_dimensions_layout_placement_unchanged": True,
                "price_unchanged": True,
                "cart_checkout_order_mutated": False,
            },
            "safety": {
                "contains_credentials": False,
                "contains_event_or_device_ids": False,
                "contains_customer_or_order_data": False,
                "meta_ads_mutated": False,
                "biznisweb_mutated": False,
                "collector_or_reporting_mutated": False,
            },
        }

    def _open(self) -> dict:
        return recorder.open_review(
            self.activation,
            completion=self.completion,
            snapshot=self.snapshot,
            sample=self.sample,
            lifecycle=self.lifecycle,
            workspace=self.workspace,
            registry=self.registry,
            runtime_observation=self.runtime,
            source_hashes=self.source_hashes,
            runtime_observation_sha256=self.runtime_hash,
        )

    def test_checked_in_manifest_is_fail_closed(self) -> None:
        recorder.validate_manifest(self.activation)
        self.assertEqual(recorder.WAITING, self.activation["status"])
        self.assertFalse(
            self.activation["release_boundaries"][
                "manual_growthbook_start_allowed"
            ]
        )
        self.assertTrue(
            all(
                value is False
                for key, value in self.activation["release_boundaries"].items()
                if key != "manual_growthbook_start_allowed"
            )
        )

    def test_runtime_release_gate_accepts_only_post_aa_cta_only_state(self) -> None:
        observation, raw = self._release_stop_observation()
        release_validator.validate_release_state(
            manifest=self.activation,
            completion=self.completion,
            snapshot=self.snapshot,
            sample=self.sample,
            lifecycle=self.lifecycle,
            workspace=self.workspace,
            registry=self.registry,
            stop_observation=observation,
            stop_observation_raw=raw,
            design_sha256=recorder.EXPECTED_STATIC_HASHES["design_contract"],
            decision_sha256=recorder.EXPECTED_STATIC_HASHES["decision_contract"],
            meta_reporting=self.meta_reporting,
            meta_reporting_sha256=recorder.EXPECTED_STATIC_HASHES[
                "meta_reporting_contract"
            ],
            registry_sha256=self.registry_hash,
            storefront_source="var PRODUCTION_ACTIVATION = false;\n",
        )

        unsafe = copy.deepcopy(observation)
        unsafe["gtm"]["unprocessed_changes"]["added"] = 1
        with self.assertRaisesRegex(Exception, "unprocessed changes"):
            release_validator.validate_release_state(
                manifest=self.activation,
                completion=self.completion,
                snapshot=self.snapshot,
                sample=self.sample,
                lifecycle=self.lifecycle,
                workspace=self.workspace,
                registry=self.registry,
                stop_observation=unsafe,
                stop_observation_raw=aa_canonical_json_bytes(unsafe),
                design_sha256=recorder.EXPECTED_STATIC_HASHES["design_contract"],
                decision_sha256=recorder.EXPECTED_STATIC_HASHES["decision_contract"],
                meta_reporting=self.meta_reporting,
                meta_reporting_sha256=recorder.EXPECTED_STATIC_HASHES[
                    "meta_reporting_contract"
                ],
                registry_sha256=self.registry_hash,
                storefront_source="var PRODUCTION_ACTIVATION = false;\n",
            )

    def test_runtime_observation_builder_is_canonical_contract_compatible(self) -> None:
        registry_raw = recorder.canonical_json_bytes(self.registry)
        built = runtime_builder.build_observation(
            manifest=self.activation,
            registry_raw=registry_raw,
            registry=self.registry,
            workflow_run_id="40000000001",
            main_commit="d" * 40,
            private_ip="172.31.20.40",
            host_gate_task_id="a" * 32,
            host_gate_private_ip="172.31.20.41",
            task_definition="vevo-growthbook-collector-production:3",
            image_digest="sha256:" + "e" * 64,
            cta_events_before_start=0,
            observed_at_utc="2026-09-04T06:00:00Z",
        )
        recorder.validate_runtime_observation(built, self.activation)
        self.assertEqual(
            hashlib.sha256(registry_raw).hexdigest(),
            built["control_plane"]["registry_sha256"],
        )
        with self.assertRaisesRegex(
            runtime_builder.CtaRuntimeObservationError,
            "events exist",
        ):
            runtime_builder.build_observation(
                manifest=self.activation,
                registry_raw=registry_raw,
                registry=self.registry,
                workflow_run_id="40000000001",
                main_commit="d" * 40,
                private_ip="172.31.20.40",
                host_gate_task_id="a" * 32,
                host_gate_private_ip="172.31.20.41",
                task_definition="vevo-growthbook-collector-production:3",
                image_digest="sha256:" + "e" * 64,
                cta_events_before_start=1,
                observed_at_utc="2026-09-04T06:00:00Z",
            )

    def test_open_review_binds_all_sources_and_only_manual_start(self) -> None:
        opened = self._open()
        self.assertEqual(recorder.REVIEW_OPEN, opened["status"])
        self.assertEqual(1084, opened["launch_contract"]["target_total_sample"])
        self.assertEqual(
            self.runtime_hash,
            opened["source_bindings"]["runtime_readiness"][
                "observation_sha256"
            ],
        )
        self.assertTrue(
            opened["release_boundaries"]["manual_growthbook_start_allowed"]
        )
        self.assertFalse(
            opened["release_boundaries"]["automatic_growthbook_mutation_allowed"]
        )
        recorder.validate_manifest(opened)

    def test_open_review_rejects_aa_that_is_not_stopped(self) -> None:
        self.completion["status"] = "aa_pass_recorded_manual_stop_review_allowed"
        with self.assertRaisesRegex(
            recorder.CtaActivationRecordingError,
            "verified A/A PASS and stop readback",
        ):
            self._open()

    def test_open_review_rejects_non_cta_or_mismatched_registry(self) -> None:
        self.registry["environments"]["production"] = copy.deepcopy(
            load("growthbook_collector/experiments.json")["environments"][
                "production"
            ]
        )
        with self.assertRaisesRegex(
            recorder.CtaActivationRecordingError, "not CTA-only"
        ):
            self._open()

    def test_open_review_rejects_runtime_without_host_gate(self) -> None:
        self.runtime["runtime"]["localhost_marker_verified"] = False
        self.runtime_hash = hashlib.sha256(
            recorder.canonical_json_bytes(self.runtime)
        ).hexdigest()
        with self.assertRaisesRegex(
            recorder.CtaActivationRecordingError, "localhost marker"
        ):
            self._open()

    def test_open_review_rejects_events_or_gtm_changes_before_start(self) -> None:
        self.runtime["control_plane"]["cta_events_before_start"] = 1
        self.runtime_hash = hashlib.sha256(
            recorder.canonical_json_bytes(self.runtime)
        ).hexdigest()
        with self.assertRaisesRegex(
            recorder.CtaActivationRecordingError, "events exist"
        ):
            self._open()
        self.runtime = self._runtime_observation()
        self.runtime["control_plane"]["gtm_unprocessed_changes"] = 1
        self.runtime_hash = hashlib.sha256(
            recorder.canonical_json_bytes(self.runtime)
        ).hexdigest()
        with self.assertRaisesRegex(
            recorder.CtaActivationRecordingError, "unprocessed changes"
        ):
            self._open()

    def test_record_start_binds_readback_and_running_workspace(self) -> None:
        opened = self._open()
        observation = self._start_observation()
        digest = hashlib.sha256(
            recorder.canonical_json_bytes(observation)
        ).hexdigest()
        recorded, workspace = recorder.record_start(
            opened,
            self.workspace,
            self.registry,
            observation,
            observation_sha256=digest,
            source_hashes=self.source_hashes,
        )
        self.assertEqual(recorder.RUNNING, recorded["status"])
        self.assertFalse(
            recorded["release_boundaries"]["manual_growthbook_start_allowed"]
        )
        self.assertEqual(
            "production_cta_running_activation_verified_pro_quantiles_verified",
            workspace["state"],
        )
        self.assertEqual(100, workspace["workspace"]["production_allocation_percent"])
        cta = {
            row["tracking_key"]: row for row in workspace["experiments"]
        }["vevo-sk-product-cta-color-001"]
        self.assertEqual("running_production_cta_only", cta["status"])
        self.assertEqual(1084, cta["activation_evidence"]["target_total_sample"])
        recorder.validate_manifest(recorded)
        recorder.validate_running_handoff(
            recorded, workspace, self.registry, observation
        )

    def test_record_start_rejects_simultaneous_aa_or_commerce_drift(self) -> None:
        opened = self._open()
        observation = self._start_observation()
        observation["growthbook"]["active_production_experiments"] = [
            "vevo-sk-aa-001",
            "vevo-sk-product-cta-color-001",
        ]
        digest = hashlib.sha256(
            recorder.canonical_json_bytes(observation)
        ).hexdigest()
        with self.assertRaisesRegex(
            recorder.CtaActivationRecordingError,
            "only active Production experiment",
        ):
            recorder.record_start(
                opened,
                self.workspace,
                self.registry,
                observation,
                observation_sha256=digest,
                source_hashes=self.source_hashes,
            )
        observation = self._start_observation()
        observation["commerce"]["price_unchanged"] = False
        digest = hashlib.sha256(
            recorder.canonical_json_bytes(observation)
        ).hexdigest()
        with self.assertRaisesRegex(
            recorder.CtaActivationRecordingError, "commerce readback"
        ):
            recorder.record_start(
                opened,
                self.workspace,
                self.registry,
                observation,
                observation_sha256=digest,
                source_hashes=self.source_hashes,
            )

    def test_record_start_rejects_source_drift_after_review(self) -> None:
        opened = self._open()
        observation = self._start_observation()
        digest = hashlib.sha256(
            recorder.canonical_json_bytes(observation)
        ).hexdigest()
        changed = dict(self.source_hashes)
        changed["sample_plan"] = "f" * 64
        with self.assertRaisesRegex(
            recorder.CtaActivationRecordingError,
            "sample_plan changed after CTA start review",
        ):
            recorder.record_start(
                opened,
                self.workspace,
                self.registry,
                observation,
                observation_sha256=digest,
                source_hashes=changed,
            )

    def test_static_contract_hashes_match_checked_in_files(self) -> None:
        for name, expected in recorder.EXPECTED_STATIC_HASHES.items():
            path = ROOT / recorder.EXPECTED_PATHS[name]
            self.assertEqual(expected, hashlib.sha256(path.read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
