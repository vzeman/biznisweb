from __future__ import annotations

import copy
import unittest

from scripts import validate_growthbook_hypothesis_registry as validator
from tests.test_growthbook_cta_final_snapshot_builder import load


class GrowthBookHypothesisRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = load("projects/vevo/growthbook_hypothesis_registry.json")
        self.manifest = load("projects/vevo/growthbook_cta_final_snapshot.json")

    def test_checked_in_registry_is_preregistered_pii_free_and_fail_closed(
        self,
    ) -> None:
        validator.validate_registry(self.registry, self.manifest)
        experiment = self.registry["experiments"][0]
        self.assertEqual(validator.PENDING, experiment["status"])
        self.assertIsNone(experiment["final_decision"])
        self.assertFalse(self.registry["privacy"]["pii_allowed"])

    def test_rejects_identity_data_or_automatic_mutation(self) -> None:
        identity = copy.deepcopy(self.registry)
        identity["experiments"][0]["device_id"] = "forbidden"
        with self.assertRaisesRegex(validator.HypothesisRegistryError, "keys drift"):
            validator.validate_registry(identity)

        identity = copy.deepcopy(self.registry)
        identity["experiments"][0]["population"] = "person@example.com"
        with self.assertRaisesRegex(
            validator.HypothesisRegistryError, "email-like value"
        ):
            validator.validate_registry(identity)

        mutation = copy.deepcopy(self.registry)
        mutation["privacy"]["pii_allowed"] = True
        with self.assertRaisesRegex(
            validator.HypothesisRegistryError, "privacy boundary drift"
        ):
            validator.validate_registry(mutation)

    def test_pending_registry_cannot_bind_a_recorded_manifest(self) -> None:
        recorded = copy.deepcopy(self.manifest)
        recorded["status"] = "final_snapshot_recorded_manual_action_pending"
        with self.assertRaisesRegex(
            validator.HypothesisRegistryError,
            "lacks registry decision",
        ):
            validator.validate_registry(self.registry, recorded)


if __name__ == "__main__":
    unittest.main()
