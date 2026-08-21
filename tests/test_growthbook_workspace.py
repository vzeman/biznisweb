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
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "Production allocation"):
                validator.validate()

    def test_validator_rejects_published_or_production_experiment_rule(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["experiments"][0]["status"] = "running"
        altered["experiments"][0]["feature_rule_status"] = "published"
        altered["experiments"][0]["feature_rule_environments"] = ["staging", "production"]
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "staging-only"):
                validator.validate()

    def test_validator_rejects_aa_analysis_setting_drift(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["experiments"][0]["analysis_settings"]["goal_metrics"] = [
            "vevo_purchase_conversion_7d"
        ]
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "A/A Preview running state drift"):
                validator.validate()

    def test_validator_rejects_started_cta_experiment(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["experiments"][1]["status"] = "running"
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "CTA A/B must remain"):
                validator.validate()

    def test_validator_rejects_committed_or_production_sdk_connection(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["sdk_connection"]["client_key_status"] = "committed"
        altered["sdk_connection"]["production_connection_created"] = True
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "SDK connection"):
                validator.validate()

    def test_validator_rejects_published_or_mutating_gtm_preview_workspace(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["gtm_preview_workspace"]["publish_status"] = "published"
        altered["gtm_preview_workspace"]["workspace_changes"]["modified"] = 1
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "GTM Preview workspace safety state drift"):
                validator.validate()

    def test_validator_rejects_unverified_gtm_bridge_sequence(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["gtm_preview_workspace"]["tags"]["purchase_bridge"][
            "loader_sequence_fail_closed_verified"
        ] = False
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "GTM Preview bridge is not fail closed"):
                validator.validate()

    def test_validator_rejects_lost_tag_assistant_exposure_delivery(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["gtm_preview_workspace"]["tag_assistant_preview"][
            "analytics_only_exposure_delivery_result"
        ] = "failed"
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "GTM Tag Assistant Preview blocker state drift"):
                validator.validate()

    def test_validator_rejects_unverified_reconciliation_checkpoint(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["reconciliation_checkpoint"][
            "raw_curated_reporting_athena_identity_verified"
        ] = False
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "reconciliation checkpoint"):
                validator.validate()

    def test_validator_rejects_growthbook_before_analytics_consent(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["gtm_preview_workspace"]["tag_assistant_preview"][
            "no_analytics_consent_growthbook_or_collector_asset_count"
        ] = 1
        with mock.patch.object(
            validator, "_load", side_effect=[altered, self.reporting, self.registry]
        ):
            with self.assertRaisesRegex(AssertionError, "GTM Tag Assistant Preview blocker state drift"):
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

    def test_validator_rejects_unverified_metric_analysis(self) -> None:
        altered = copy.deepcopy(self.workspace)
        metric = next(row for row in altered["metrics"] if row["key"] == "vevo_add_to_cart_24h")
        metric["analysis_query_synthetic_device_count"] = 0
        with mock.patch.object(validator, "_load", side_effect=[altered, self.reporting, self.registry]):
            with self.assertRaisesRegex(AssertionError, "created metric state drift"):
                validator.validate()

    def test_validator_rejects_numeric_column_type_drift(self) -> None:
        altered = copy.deepcopy(self.workspace)
        altered["fact_tables"][0]["growthbook_numeric_columns_verified"].remove(
            "client_error_observed"
        )
        with mock.patch.object(validator, "_load", side_effect=[altered, self.reporting, self.registry]):
            with self.assertRaisesRegex(AssertionError, "column-type state drift"):
                validator.validate()

    def test_validator_rejects_unapproved_pro_metric_claim(self) -> None:
        altered = copy.deepcopy(self.workspace)
        metric = next(row for row in altered["metrics"] if row["key"] == "vevo_lcp_p75_24h")
        metric["growthbook_id"] = "fact_unapproved"
        metric["status"] = "growthbook_created_query_verified"
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
