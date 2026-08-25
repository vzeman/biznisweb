from __future__ import annotations

import copy
import json
import pathlib
import unittest

from scripts.validate_growthbook_aa_measurement_window import (
    MeasurementWindowError,
    expected_measurement_window,
    validate_measurement_window,
)


ROOT = pathlib.Path(__file__).resolve().parents[1]


def load(name: str) -> dict[str, object]:
    return json.loads((ROOT / "projects" / "vevo" / name).read_text(encoding="utf-8"))


class GrowthBookAaMeasurementWindowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = load("growthbook_aa_snapshot.json")
        self.activation = load("growthbook_production_aa_activation.json")
        self.acceptance = load("growthbook_aa_acceptance.json")
        self.reconciliation = load(
            "growthbook_production_reconciliation_deploy_evidence.json"
        )

    def validate(self, manifest: dict[str, object] | None = None) -> None:
        validate_measurement_window(
            manifest or self.manifest,
            self.activation,
            self.acceptance,
            self.reconciliation,
        )

    def test_checked_in_window_is_derived_from_activation_before_outcomes(self) -> None:
        self.validate()
        window = self.manifest["measurement_window"]
        self.assertEqual("2026-08-26", window["first_full_local_date"])
        self.assertEqual("2026-09-01", window["last_required_full_local_date"])
        self.assertEqual("2026-08-25T22:00:00Z", window["from_utc"])
        self.assertEqual("2026-09-01T22:00:00Z", window["minimum_through_utc"])
        self.assertEqual(
            "2026-09-02T03:45:00+02:00",
            window["earliest_resolution_check_due_local"],
        )
        self.assertEqual(
            "after_minimum_days_resolve_at_first_successful_daily_reconciliation_"
            "with_minimum_eligible_devices",
            window["stopping_rule"],
        )
        self.assertEqual(
            "cumulative_eligible_devices_without_arm_outcome_readback",
            window["stopping_rule_population_metric"],
        )
        self.assertTrue(window["whole_local_day_extensions_only"])
        self.assertTrue(window["outcome_blind_resolution_required"])
        self.assertEqual(
            "pending_minimum_window_and_sample", window["resolution_status"]
        )
        for field in (
            "resolved_last_full_local_date",
            "resolved_through_utc",
            "resolved_full_calendar_days",
            "resolved_eligible_devices",
            "resolved_at_utc",
        ):
            self.assertIsNone(window[field])
        self.assertEqual([], window["checkpoint_history"])
        self.assertFalse(window["post_hoc_window_change_allowed"])
        self.assertEqual(7, window["minimum_full_calendar_days"])
        self.assertEqual(1000, window["minimum_eligible_devices"])

    def test_expected_window_is_recomputed_not_trusted_from_manifest(self) -> None:
        expected = expected_measurement_window(
            self.activation, self.acceptance, self.reconciliation
        )
        self.assertEqual(self.manifest["measurement_window"], expected)

        altered = copy.deepcopy(self.manifest)
        altered["measurement_window"]["from_utc"] = "2026-08-26T22:00:00Z"
        with self.assertRaisesRegex(MeasurementWindowError, "measurement window drift"):
            self.validate(altered)

    def test_later_lifecycle_state_cannot_rewrite_the_frozen_window(self) -> None:
        activation = copy.deepcopy(self.activation)
        activation["status"] = "production_aa_completed"
        activation["traffic"]["production_allocation_percent"] = 0
        activation["traffic"]["active_production_experiments"] = []
        validate_measurement_window(
            self.manifest,
            activation,
            self.acceptance,
            self.reconciliation,
        )

    def test_component_windows_cannot_diverge(self) -> None:
        altered = copy.deepcopy(self.manifest)
        altered["automated_evidence"]["through_utc"] = "2026-09-01T22:00:00Z"
        with self.assertRaisesRegex(MeasurementWindowError, "differs"):
            self.validate(altered)

    def test_window_cannot_be_resolved_before_the_sample_checkpoint(self) -> None:
        altered = copy.deepcopy(self.manifest)
        altered["measurement_window"]["resolved_through_utc"] = (
            "2026-09-01T22:00:00Z"
        )
        with self.assertRaisesRegex(MeasurementWindowError, "lifecycle drift"):
            self.validate(altered)

    def test_activation_or_reconciliation_provenance_drift_is_rejected(self) -> None:
        activation = copy.deepcopy(self.activation)
        activation["activation_readback"]["observed_at_utc"] = "2026-08-26T05:43:54Z"
        with self.assertRaisesRegex(MeasurementWindowError, "measurement window drift"):
            validate_measurement_window(
                self.manifest,
                activation,
                self.acceptance,
                self.reconciliation,
            )

        reconciliation = copy.deepcopy(self.reconciliation)
        reconciliation["schedule"]["enabled"] = False
        with self.assertRaisesRegex(MeasurementWindowError, "schedule drift"):
            validate_measurement_window(
                self.manifest,
                self.activation,
                self.acceptance,
                reconciliation,
            )


if __name__ == "__main__":
    unittest.main()
