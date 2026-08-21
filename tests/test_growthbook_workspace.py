from __future__ import annotations

import copy
import json
import pathlib
import unittest
from unittest import mock

from scripts import validate_growthbook_workspace as validator


ROOT = pathlib.Path(__file__).resolve().parents[1]


class GrowthBookWorkspaceContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.workspace = json.loads(validator.WORKSPACE_PATH.read_text(encoding="utf-8"))
        self.reporting = json.loads(validator.REPORTING_PATH.read_text(encoding="utf-8"))
        self.registry = json.loads(validator.REGISTRY_PATH.read_text(encoding="utf-8"))

    def test_checked_in_workspace_contract_is_valid(self) -> None:
        validator.validate()

    def test_assignment_query_is_curated_and_pii_free(self) -> None:
        sql = (ROOT / self.workspace["athena"]["assignment_query"]).read_text(encoding="utf-8")
        validator._validate_sql(
            "assignment",
            sql,
            table="experiment_device_facts",
            experiment_filter=False,
        )
        self.assertNotIn("transaction_id", sql.lower())
        self.assertNotIn("order_num", sql.lower())

    def test_validator_rejects_nonzero_production_allocation(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["workspace"]["production_allocation_percent"] = 1
        with mock.patch.object(validator, "_load", side_effect=[altered, self.reporting, self.registry]):
            with self.assertRaisesRegex(AssertionError, "Production allocation"):
                validator.validate()

    def test_validator_rejects_published_or_production_experiment_rule(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["experiments"][0]["status"] = "running"
        altered["experiments"][0]["feature_rule_status"] = "published"
        altered["experiments"][0]["feature_rule_environments"] = ["staging", "production"]
        with mock.patch.object(validator, "_load", side_effect=[altered, self.reporting, self.registry]):
            with self.assertRaisesRegex(AssertionError, "staging-only draft"):
                validator.validate()

    def test_validator_rejects_committed_or_production_sdk_connection(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["sdk_connection"]["client_key_status"] = "committed"
        altered["sdk_connection"]["production_connection_created"] = True
        with mock.patch.object(validator, "_load", side_effect=[altered, self.reporting, self.registry]):
            with self.assertRaisesRegex(AssertionError, "SDK connection"):
                validator.validate()

    def test_validator_rejects_metric_contract_drift(self) -> None:
        altered = copy.deepcopy(self.workspace)
        metric = next(row for row in altered["metrics"] if row["key"] == "vevo_cm1_per_exposed_device_7d")
        metric["metric_contract_version"] = "drifted"
        with mock.patch.object(validator, "_load", side_effect=[altered, self.reporting, self.registry]):
            with self.assertRaisesRegex(AssertionError, "CM1 contracts differ"):
                validator.validate()

    def test_validator_rejects_created_metric_id_drift(self) -> None:
        altered = copy.deepcopy(self.workspace)
        metric = next(row for row in altered["metrics"] if row["key"] == "vevo_add_to_cart_24h")
        metric["growthbook_id"] = "fact_drifted"
        with mock.patch.object(validator, "_load", side_effect=[altered, self.reporting, self.registry]):
            with self.assertRaisesRegex(AssertionError, "created metric state drift"):
                validator.validate()

    def test_validator_rejects_unapproved_pro_metric_claim(self) -> None:
        altered = copy.deepcopy(self.workspace)
        metric = next(row for row in altered["metrics"] if row["key"] == "vevo_lcp_p75_24h")
        metric["growthbook_id"] = "fact_unapproved"
        metric["status"] = "growthbook_created_ui_verified"
        with mock.patch.object(validator, "_load", side_effect=[altered, self.reporting, self.registry]):
            with self.assertRaisesRegex(AssertionError, "Pro metric blocker state drift"):
                validator.validate()

    def test_validator_rejects_pii_identifier_in_sql(self) -> None:
        with self.assertRaisesRegex(AssertionError, "forbidden identifier email"):
            validator._validate_sql(
                "unsafe",
                """
                SELECT device_id, email, first_exposure_at AS timestamp
                FROM experiment_device_facts
                WHERE metric_contract_version = 'vevo_cm1_v1_2026-08-20'
                  AND eligible = 1
                  AND contaminated = 0
                  AND first_exposure_at BETWEEN '{{startDateISO}}' AND '{{endDateISO}}'
                """,
                table="experiment_device_facts",
                experiment_filter=False,
            )


if __name__ == "__main__":
    unittest.main()
