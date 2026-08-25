from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from scripts.validate_growthbook_meta_reporting_contract import (
    MetaReportingContractError,
    validate_contract,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads(
    (ROOT / "projects" / "vevo" / "growthbook_meta_reporting_contract.json").read_text(
        encoding="utf-8"
    )
)


class GrowthBookMetaReportingContractTests(unittest.TestCase):
    def test_checked_in_contract_validates_end_to_end_chain(self) -> None:
        validate_contract(CONTRACT)

    def test_meta_cannot_own_the_first_cta_split(self) -> None:
        unsafe = copy.deepcopy(CONTRACT)
        unsafe["traffic_assignment"]["owner"] = "meta"
        with self.assertRaisesRegex(
            MetaReportingContractError, "GrowthBook must own randomization"
        ):
            validate_contract(unsafe)

    def test_arm_specific_destination_is_rejected(self) -> None:
        unsafe = copy.deepcopy(CONTRACT)
        unsafe["traffic_assignment"][
            "arm_specific_destination_or_query_parameter_allowed"
        ] = True
        with self.assertRaisesRegex(MetaReportingContractError, "must not select a CTA arm"):
            validate_contract(unsafe)

    def test_stable_dimension_mapping_is_exact(self) -> None:
        unsafe = copy.deepcopy(CONTRACT)
        unsafe["meta_url_parameter_contract"]["stable_dimension_mappings"][0][
            "reporting_fact_column"
        ] = "campaign_name"
        with self.assertRaisesRegex(MetaReportingContractError, "mapping drift"):
            validate_contract(unsafe)

    def test_meta_dimensions_cannot_replace_primary_decision(self) -> None:
        unsafe = copy.deepcopy(CONTRACT)
        unsafe["analysis_policy"]["dimension_result_may_declare_winner"] = True
        with self.assertRaisesRegex(MetaReportingContractError, "analysis policy drift"):
            validate_contract(unsafe)

    def test_automatic_meta_mutation_is_rejected(self) -> None:
        unsafe = copy.deepcopy(CONTRACT)
        unsafe["release_boundaries"]["automatic_meta_ads_mutation_allowed"] = True
        with self.assertRaisesRegex(MetaReportingContractError, "boundary is open"):
            validate_contract(unsafe)


if __name__ == "__main__":
    unittest.main()
