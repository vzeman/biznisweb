from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import unittest
from datetime import datetime, timedelta, timezone

from scripts.record_growthbook_pro_upgrade import (
    ACTION_TIME_FRESHNESS,
    CTA_GUARDRAILS,
    MAX_VERIFIED_UPGRADE_DELAY,
    METRIC_KEYS,
    POST_AA_BLOCKED,
    POST_AA_VERIFIED,
    ProUpgradeError,
    assert_action_time,
    canonical_json_bytes,
    open_review,
    record_upgrade,
    validate_manifest,
    validate_observation,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]
ACTION_TIME = datetime(2026, 9, 3, 10, 0, tzinfo=timezone.utc)


def load(name: str) -> dict:
    return json.loads((ROOT / "projects" / "vevo" / name).read_text(encoding="utf-8"))


class GrowthBookProUpgradeRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = load("growthbook_pro_upgrade.json")
        self.workspace = load("growthbook_workspace.json")
        self.completion = load("growthbook_production_aa_completion.json")

    def completed_sources(self) -> tuple[dict, dict]:
        completion = copy.deepcopy(self.completion)
        completion["status"] = "production_aa_stopped_verified_cta_activation_blocked"
        completion["aa_pass"]["status"] = "verified_pass"
        completion["aa_pass"]["verdict"] = "PASS"
        completion["stop_readback"]["status"] = "verified_zero_allocation"
        completion["stop_readback"]["observed_at_utc"] = "2026-09-03T09:55:00Z"
        workspace = copy.deepcopy(self.workspace)
        workspace["state"] = POST_AA_BLOCKED
        workspace["workspace"]["production_allocation_percent"] = 0
        workspace["decision_gates"]["production_activation_allowed"] = False
        experiments = {row["tracking_key"]: row for row in workspace["experiments"]}
        aa = experiments["vevo-sk-aa-001"]
        aa["status"] = "stopped_production_aa_pass_verified"
        aa["feature_rule_status"] = "staging_only"
        aa["feature_rule_environments"] = ["staging"]
        aa["production_allocation_percent"] = 0
        cta = experiments["vevo-sk-product-cta-color-001"]
        cta["status"] = "draft"
        cta["feature_rule_status"] = "draft"
        cta["production_allocation_percent"] = 0
        return completion, workspace

    def opened(self) -> tuple[dict, dict, dict]:
        completion, workspace = self.completed_sources()
        manifest = open_review(
            self.manifest,
            workspace,
            completion,
            authorized_at_utc="2026-09-03T10:00:00Z",
            confirm_paid_upgrade="true",
            confirmed_seat_count=1,
            confirmed_base_monthly_price=40,
            confirmed_recurring_subscription="true",
            now=ACTION_TIME,
        )
        return manifest, completion, workspace

    def observation(self, manifest: dict) -> dict:
        rows = {}
        for index, key in enumerate(sorted(METRIC_KEYS), start=1):
            rows[key] = {
                "preview_metric_id": f"fact__ProPreview{index}",
                "production_metric_id": f"fact__ProProduction{index}",
                "contract_sha256": manifest["target"]["metric_contract_sha256"][key],
                "preview_configuration_readback_match": True,
                "production_configuration_readback_match": True,
                "preview_query_test_passed": True,
                "production_query_test_passed": True,
            }
        return {
            "schema_version": 1,
            "observation_type": "vevo_growthbook_pro_upgrade_observation",
            "observed_at_utc": "2026-09-03T10:05:00Z",
            "organization": {
                "name": "Vevo",
                "id": "org_19g6mmt1q79o1",
                "project_name": "VEVO SK Web",
                "project_id": "prj_2CeEJc6J9FwQFix9UhsnKr",
            },
            "billing": {
                "plan": "pro",
                "status": "pro_active_paid_monthly_one_seat",
                "seat_count": 1,
                "currency": "USD",
                "base_monthly_price": 40,
                "billing_period": "monthly",
                "recurring_subscription": True,
            },
            "quantile_metrics": rows,
            "cta_draft": {
                "experiment_id": "exp_19g6mmt1qxzrp",
                "status": "draft",
                "production_allocation_percent": 0,
                "guardrail_metrics": CTA_GUARDRAILS,
            },
            "control": {
                "aa_status": "stopped",
                "aa_production_allocation_percent": 0,
                "active_production_experiments": [],
                "gtm_container_version_id": "15",
                "gtm_unprocessed_changes": 0,
            },
            "safety": {
                "contains_credentials": False,
                "contains_payment_method_or_card_data": False,
                "contains_invoice_address_or_tax_id": False,
                "contains_user_email": False,
                "contains_event_or_device_ids": False,
                "contains_customer_or_order_data": False,
                "gtm_mutated": False,
                "meta_ads_mutated": False,
                "biznisweb_mutated": False,
                "collector_or_reporting_mutated": False,
                "price_product_stock_cart_checkout_payment_or_order_mutated": False,
            },
        }

    def test_current_waiting_manifest_is_valid_and_cannot_buy(self) -> None:
        validate_manifest(self.manifest, self.workspace)
        self.assertFalse(self.manifest["release_boundaries"]["manual_paid_upgrade_allowed"])
        self.assertFalse(self.manifest["release_boundaries"]["automatic_paid_upgrade_allowed"])

    def test_review_requires_completed_aa_exact_price_and_recurring_confirmation(self) -> None:
        completion, workspace = self.completed_sources()
        with self.assertRaisesRegex(ProUpgradeError, "confirmed offer"):
            open_review(
                self.manifest,
                workspace,
                completion,
                authorized_at_utc="2026-09-03T10:00:00Z",
                confirm_paid_upgrade="true",
                confirmed_seat_count=1,
                confirmed_base_monthly_price=41,
                confirmed_recurring_subscription="true",
                now=ACTION_TIME,
            )
        with self.assertRaisesRegex(ProUpgradeError, "verified A/A completion"):
            open_review(
                self.manifest,
                self.workspace,
                self.completion,
                authorized_at_utc="2026-09-03T10:00:00Z",
                confirm_paid_upgrade="true",
                confirmed_seat_count=1,
                confirmed_base_monthly_price=40,
                confirmed_recurring_subscription="true",
                now=ACTION_TIME,
            )

    def test_review_requires_fresh_canonical_action_time_after_verified_stop(
        self,
    ) -> None:
        completion, workspace = self.completed_sources()
        cases = (
            (
                "2026-09-03T09:44:59Z",
                ACTION_TIME,
                "action-time confirmation is stale",
            ),
            (
                "2026-09-03T10:01:01Z",
                ACTION_TIME,
                "timestamp is in the future",
            ),
            (
                "2026-09-03T09:54:59Z",
                ACTION_TIME,
                "predates the verified A/A stop",
            ),
            (
                "2026-09-03T10:00:00.000Z",
                ACTION_TIME,
                "canonical whole-second UTC Z",
            ),
        )
        for authorized_at_utc, now, message in cases:
            with self.subTest(authorized_at_utc=authorized_at_utc):
                with self.assertRaisesRegex(ProUpgradeError, message):
                    open_review(
                        self.manifest,
                        workspace,
                        completion,
                        authorized_at_utc=authorized_at_utc,
                        confirm_paid_upgrade="true",
                        confirmed_seat_count=1,
                        confirmed_base_monthly_price=40,
                        confirmed_recurring_subscription="true",
                        now=now,
                    )

    def test_open_review_refreshes_only_unchanged_bound_sources(self) -> None:
        manifest, completion, workspace = self.opened()
        refreshed = open_review(
            manifest,
            workspace,
            completion,
            authorized_at_utc="2026-09-03T10:10:00Z",
            confirm_paid_upgrade="true",
            confirmed_seat_count=1,
            confirmed_base_monthly_price=40,
            confirmed_recurring_subscription="true",
            now=ACTION_TIME + timedelta(minutes=10),
        )
        self.assertEqual(
            "2026-09-03T10:10:00Z",
            refreshed["authorization"]["authorized_at_utc"],
        )
        changed_completion = copy.deepcopy(completion)
        changed_completion["next_gate"] = "unexpected_drift"
        with self.assertRaisesRegex(ProUpgradeError, "completion changed"):
            open_review(
                manifest,
                workspace,
                changed_completion,
                authorized_at_utc="2026-09-03T10:10:00Z",
                confirm_paid_upgrade="true",
                confirmed_seat_count=1,
                confirmed_base_monthly_price=40,
                confirmed_recurring_subscription="true",
                now=ACTION_TIME + timedelta(minutes=10),
            )

    def test_action_time_assertion_expires_and_checks_bound_sources(self) -> None:
        manifest, completion, workspace = self.opened()
        assert_action_time(
            manifest,
            workspace,
            completion,
            now=ACTION_TIME + ACTION_TIME_FRESHNESS,
        )
        with self.assertRaisesRegex(
            ProUpgradeError, "action-time confirmation is stale"
        ):
            assert_action_time(
                manifest,
                workspace,
                completion,
                now=ACTION_TIME + ACTION_TIME_FRESHNESS + timedelta(seconds=1),
            )
        changed_workspace = copy.deepcopy(workspace)
        changed_workspace["workspace"]["production_allocation_percent"] = 1
        with self.assertRaisesRegex(ProUpgradeError, "workspace changed"):
            assert_action_time(
                manifest,
                changed_workspace,
                completion,
                now=ACTION_TIME,
            )

    def test_records_six_unique_metrics_and_keeps_cta_blocked(self) -> None:
        manifest, completion, workspace = self.opened()
        observation = self.observation(manifest)
        digest = hashlib.sha256(canonical_json_bytes(observation)).hexdigest()
        recorded, updated_workspace = record_upgrade(
            manifest,
            workspace,
            completion,
            observation,
            expected_observation_sha256=digest,
        )
        self.assertEqual("pro_active_quantile_metrics_verified_cta_still_blocked", recorded["status"])
        self.assertFalse(recorded["release_boundaries"]["manual_paid_upgrade_allowed"])
        self.assertFalse(recorded["release_boundaries"]["cta_activation_allowed"])
        self.assertEqual(POST_AA_VERIFIED, updated_workspace["state"])
        self.assertEqual("pro", updated_workspace["workspace"]["plan_type"])
        self.assertEqual(0, updated_workspace["workspace"]["production_allocation_percent"])
        experiments = {row["tracking_key"]: row for row in updated_workspace["experiments"]}
        self.assertEqual("draft", experiments["vevo-sk-product-cta-color-001"]["status"])
        self.assertEqual(CTA_GUARDRAILS, experiments["vevo-sk-product-cta-color-001"]["pro_guardrail_metrics"])
        clone = updated_workspace["athena"]["production"]["growthbook_clone"]
        self.assertTrue(clone["paid_pro_upgrade_authorized"])
        self.assertTrue(set(METRIC_KEYS) <= set(clone["source_metric_ids"]))
        self.assertTrue(set(METRIC_KEYS) <= set(clone["target_metric_ids"]))
        validate_manifest(recorded, updated_workspace)
        validate_observation(observation, recorded, updated_workspace)

    def test_rejects_sensitive_observation_and_reused_metric_id(self) -> None:
        manifest, _, workspace = self.opened()
        unsafe = self.observation(manifest)
        unsafe["safety"]["contains_payment_method_or_card_data"] = True
        with self.assertRaisesRegex(ProUpgradeError, "unsafe data"):
            validate_observation(unsafe, manifest, workspace)
        reused = self.observation(manifest)
        first, second = sorted(METRIC_KEYS)[:2]
        reused["quantile_metrics"][second]["preview_metric_id"] = reused["quantile_metrics"][first]["preview_metric_id"]
        with self.assertRaisesRegex(ProUpgradeError, "reused"):
            validate_observation(reused, manifest, workspace)

    def test_observation_must_follow_authorization_within_bounded_window(self) -> None:
        manifest, _, workspace = self.opened()
        before = self.observation(manifest)
        before["observed_at_utc"] = "2026-09-03T09:59:59Z"
        with self.assertRaisesRegex(ProUpgradeError, "authorized upgrade window"):
            validate_observation(before, manifest, workspace)
        late = self.observation(manifest)
        late["observed_at_utc"] = (
            (ACTION_TIME + MAX_VERIFIED_UPGRADE_DELAY + timedelta(seconds=1))
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
        with self.assertRaisesRegex(ProUpgradeError, "authorized upgrade window"):
            validate_observation(late, manifest, workspace)


if __name__ == "__main__":
    unittest.main()
