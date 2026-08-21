from __future__ import annotations

import copy
import json
import unittest

from scripts import validate_growthbook_cta_design as validator


class GrowthBookCtaDesignContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = json.loads(validator.CONTRACT_PATH.read_text(encoding="utf-8"))
        self.source = validator.STOREFRONT_PATH.read_text(encoding="utf-8")

    def test_checked_in_cta_design_is_accessible_and_background_only(self) -> None:
        ratios = validator.validate_contract(self.contract, self.source)

        self.assertAlmostEqual(ratios["start"], 7.9359, places=4)
        self.assertAlmostEqual(ratios["end"], 6.4325, places=4)
        self.assertGreaterEqual(
            min(ratios.values()), self.contract["minimum_wcag_contrast_ratio"]
        )

    def test_rejects_color_drift_and_detects_subthreshold_contrast(self) -> None:
        altered = copy.deepcopy(self.contract)
        altered["text_color"] = "#c9a962"
        altered_source = self.source.replace("#0f172a", "#c9a962")

        with self.assertRaisesRegex(validator.CtaDesignContractError, "contract changed"):
            validator.validate_contract(altered, altered_source)
        self.assertLess(validator.contrast_ratio("#c9a962", "#c9a962"), 4.5)

    def test_rejects_an_unapproved_dimension_property(self) -> None:
        altered_source = self.source.replace(
            '"color:#0f172a!important;" +',
            '"color:#0f172a!important;" +\n      "padding:99px!important;" +',
        )

        with self.assertRaisesRegex(
            validator.CtaDesignContractError, "may change only"
        ):
            validator.validate_contract(self.contract, altered_source)

    def test_rejects_button_label_or_behavior_mutation(self) -> None:
        altered_source = self.source + '\nbutton.textContent = "Kúpiť";\n'

        with self.assertRaisesRegex(
            validator.CtaDesignContractError, "forbidden behavior/content mutation"
        ):
            validator.validate_contract(self.contract, altered_source)


if __name__ == "__main__":
    unittest.main()
