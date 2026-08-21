from __future__ import annotations

import copy
import hashlib
import json
import pathlib
import tempfile
import unittest

from scripts import freeze_growthbook_cta_sample as freezer


class GrowthBookCtaSampleFreezeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = json.loads(freezer.DEFAULT_PLAN_PATH.read_text(encoding="utf-8"))
        self.workspace = json.loads(freezer.DEFAULT_WORKSPACE_PATH.read_text(encoding="utf-8"))
        self.observation = {
            "schema_version": 1,
            "source_experiment_id": "vevo-sk-aa-001",
            "aa_decision": "PASS",
            "aa_snapshot_sha256": "a" * 64,
            "aa_window_started_at_utc": "2026-08-22T00:00:00Z",
            "aa_window_ended_at_utc": "2026-08-29T00:00:00Z",
            "population_definition": "first_valid_aa_product_page_exposure_proxy",
            "exposed_devices": 451,
            "converted_devices": 148,
            "contains_device_or_event_identity": False,
            "contains_customer_or_order_data": False,
        }

    def observation_hash(self, value: dict | None = None) -> str:
        return hashlib.sha256(freezer.canonical_json_bytes(value or self.observation)).hexdigest()

    def test_checked_in_plan_is_pending_and_reproduces_provisional_target(self) -> None:
        freezer.validate_plan(self.plan)
        per_arm, baseline, target = freezer.calculate_sample_per_arm(
            exposed_devices=451,
            converted_devices=148,
            relative_mde_percent=25,
            power_percent=80,
            alpha_percent=5,
        )
        self.assertEqual(542, per_arm)
        self.assertEqual(1084, 2 * per_arm)
        self.assertAlmostEqual(148 / 451, baseline)
        self.assertAlmostEqual((148 / 451) * 1.25, target)
        self.assertFalse(self.plan["activation_allowed"])

    def test_freeze_updates_only_sample_state_and_keeps_activation_closed(self) -> None:
        plan, workspace = freezer.freeze_sample(
            self.plan,
            self.workspace,
            self.observation,
            observation_sha256=self.observation_hash(),
            frozen_at_utc="2026-08-29T00:00:00Z",
        )
        freezer.validate_plan(plan)
        self.assertEqual("sample_frozen_activation_still_blocked", plan["status"])
        self.assertEqual(542, plan["final"]["sample_per_arm"])
        self.assertEqual(1084, plan["final"]["total_sample"])
        self.assertFalse(plan["activation_allowed"])
        cta = next(
            row
            for row in workspace["experiments"]
            if row["tracking_key"] == "vevo-sk-product-cta-color-001"
        )
        self.assertEqual("draft", cta["status"])
        self.assertEqual(0, cta["production_allocation_percent"])
        self.assertEqual("frozen_from_hash_bound_aa_activation_still_blocked", cta["final_sample_status"])
        self.assertEqual(1084, cta["final_total_sample"])
        self.assertFalse(workspace["decision_gates"]["production_activation_allowed"])
        self.assertFalse(workspace["decision_gates"]["price_tests_allowed"])

    def test_freeze_rejects_wrong_observation_hash(self) -> None:
        with self.assertRaisesRegex(freezer.CtaSampleFreezeError, "SHA-256 mismatch"):
            freezer.freeze_sample(
                self.plan,
                self.workspace,
                self.observation,
                observation_sha256="b" * 64,
                frozen_at_utc="2026-08-29T00:00:00Z",
            )

    def test_freeze_rejects_non_pass_or_identity_bearing_observation(self) -> None:
        for field, value, message in (
            ("aa_decision", "FAIL", "before A/A PASS"),
            ("contains_device_or_event_identity", True, "contains identity"),
            ("contains_customer_or_order_data", True, "contains commerce data"),
        ):
            altered = copy.deepcopy(self.observation)
            altered[field] = value
            with self.subTest(field=field):
                with self.assertRaisesRegex(freezer.CtaSampleFreezeError, message):
                    freezer.freeze_sample(
                        self.plan,
                        self.workspace,
                        altered,
                        observation_sha256=self.observation_hash(altered),
                        frozen_at_utc="2026-08-29T00:00:00Z",
                    )

    def test_freeze_rejects_short_or_small_baseline(self) -> None:
        short = copy.deepcopy(self.observation)
        short["aa_window_ended_at_utc"] = "2026-08-28T23:59:59Z"
        with self.assertRaisesRegex(freezer.CtaSampleFreezeError, "shorter than seven days"):
            freezer.validate_observation(short, self.plan)

        small = copy.deepcopy(self.observation)
        small["exposed_devices"] = 199
        small["converted_devices"] = 50
        with self.assertRaisesRegex(freezer.CtaSampleFreezeError, "integer >= 200"):
            freezer.validate_observation(small, self.plan)

    def test_freeze_rejects_started_cta_or_open_production_gate(self) -> None:
        for mutation, message in (
            (("experiments", 1, "status", "running"), "already running"),
            (("decision_gates", "production_activation_allowed", True), "gate is open"),
        ):
            altered = copy.deepcopy(self.workspace)
            if len(mutation) == 4:
                altered[mutation[0]][mutation[1]][mutation[2]] = mutation[3]
            else:
                altered[mutation[0]][mutation[1]] = mutation[2]
            with self.subTest(message=message):
                with self.assertRaisesRegex(freezer.CtaSampleFreezeError, message):
                    freezer.freeze_sample(
                        self.plan,
                        altered,
                        self.observation,
                        observation_sha256=self.observation_hash(),
                        frozen_at_utc="2026-08-29T00:00:00Z",
                    )

    def test_canonical_loader_and_separate_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = pathlib.Path(temp_dir)
            observation_path = temp / "observation.json"
            plan_output = temp / "plan.json"
            workspace_output = temp / "workspace.json"
            observation_path.write_bytes(freezer.canonical_json_bytes(self.observation))
            loaded = freezer.load_canonical_observation(observation_path)
            plan, workspace = freezer.freeze_sample(
                self.plan,
                self.workspace,
                loaded,
                observation_sha256=self.observation_hash(),
                frozen_at_utc="2026-08-29T00:00:00Z",
            )
            freezer._write_json(plan_output, plan)
            freezer._write_json(workspace_output, workspace)
            self.assertEqual(1084, json.loads(plan_output.read_text(encoding="utf-8"))["final"]["total_sample"])
            self.assertEqual(
                "frozen_from_hash_bound_aa_activation_still_blocked",
                next(
                    row
                    for row in json.loads(workspace_output.read_text(encoding="utf-8"))["experiments"]
                    if row["tracking_key"] == "vevo-sk-product-cta-color-001"
                )["final_sample_status"],
            )

            observation_path.write_text(
                json.dumps(self.observation, indent=2) + "\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(freezer.CtaSampleFreezeError, "not canonical"):
                freezer.load_canonical_observation(observation_path)


if __name__ == "__main__":
    unittest.main()
