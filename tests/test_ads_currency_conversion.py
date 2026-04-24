import unittest
from unittest.mock import patch

import export_orders
from reporting_core.config import load_project_settings


class AdsCurrencyConversionTests(unittest.TestCase):
    def _make_exporter(self, project_settings=None):
        exporter = export_orders.BizniWebExporter.__new__(export_orders.BizniWebExporter)
        exporter.project_name = "test_project"
        exporter.project_settings = project_settings or {}
        return exporter

    def _make_source_entry(self, key: str, label: str):
        return export_orders.BizniWebExporter._build_source_entry(
            key=key,
            label=label,
            status="ok",
            mode="api",
            message=f"{label} API connected successfully.",
            healthy=True,
            active_days=2,
        )

    def test_project_settings_define_ads_currency_for_vevo_and_roy(self) -> None:
        vevo_settings = load_project_settings("vevo")
        roy_settings = load_project_settings("roy")

        self.assertEqual("EUR", vevo_settings["ads_currency"]["facebook_ads"]["expected_currency"])
        self.assertEqual("EUR", vevo_settings["ads_currency"]["google_ads"]["expected_currency"])
        self.assertEqual("EUR", roy_settings["ads_currency"]["facebook_ads"]["expected_currency"])
        self.assertEqual("EUR", roy_settings["ads_currency"]["google_ads"]["expected_currency"])

    def test_detected_foreign_currency_spend_is_converted_to_eur(self) -> None:
        exporter = self._make_exporter(
            {
                "ads_currency": {
                    "google_ads": {
                        "expected_currency": "PLN",
                    }
                }
            }
        )
        entry = self._make_source_entry("google_ads", "Google Ads")

        with patch.dict(export_orders.CURRENCY_RATES_TO_EUR, {"EUR": 1.0, "PLN": 0.23}, clear=True):
            converted_spend, updated_entry = exporter._apply_ads_currency_handling(
                source_key="google_ads",
                label="Google Ads",
                daily_spend={"2026-04-01": 100.0, "2026-04-02": 50.0},
                detected_currency="PLN",
                source_entry=entry,
            )

        self.assertAlmostEqual(23.0, converted_spend["2026-04-01"])
        self.assertAlmostEqual(11.5, converted_spend["2026-04-02"])
        self.assertEqual("ok", updated_entry["status"])
        self.assertTrue(updated_entry["currency_conversion_applied"])
        self.assertEqual("PLN", updated_entry["resolved_currency"])
        self.assertEqual("EUR", updated_entry["report_currency"])
        self.assertAlmostEqual(150.0, updated_entry["total_original"])
        self.assertAlmostEqual(34.5, updated_entry["total_eur"])

    def test_currency_mismatch_warns_but_uses_detected_currency(self) -> None:
        exporter = self._make_exporter(
            {
                "ads_currency": {
                    "facebook_ads": {
                        "expected_currency": "EUR",
                    }
                }
            }
        )
        entry = self._make_source_entry("facebook_ads", "Facebook Ads")

        with patch.dict(export_orders.CURRENCY_RATES_TO_EUR, {"EUR": 1.0, "PLN": 0.23}, clear=True):
            converted_spend, updated_entry = exporter._apply_ads_currency_handling(
                source_key="facebook_ads",
                label="Facebook Ads",
                daily_spend={"2026-04-01": 100.0},
                detected_currency="PLN",
                source_entry=entry,
            )

        self.assertAlmostEqual(23.0, converted_spend["2026-04-01"])
        self.assertEqual("warning", updated_entry["status"])
        self.assertTrue(
            any("API returned PLN, while project config expects EUR." in warning for warning in updated_entry["warnings"])
        )
        self.assertIn("detected PLN", updated_entry["message"])
        self.assertEqual("PLN", updated_entry["resolved_currency"])
        self.assertAlmostEqual(23.0, updated_entry["total_eur"])

    def test_missing_conversion_rate_ignores_spend_to_prevent_wrong_reporting(self) -> None:
        exporter = self._make_exporter(
            {
                "ads_currency": {
                    "google_ads": {
                        "expected_currency": "RON",
                    }
                }
            }
        )
        entry = self._make_source_entry("google_ads", "Google Ads")

        with patch.dict(export_orders.CURRENCY_RATES_TO_EUR, {"EUR": 1.0}, clear=True):
            converted_spend, updated_entry = exporter._apply_ads_currency_handling(
                source_key="google_ads",
                label="Google Ads",
                daily_spend={"2026-04-01": 100.0},
                detected_currency="RON",
                source_entry=entry,
            )

        self.assertEqual({}, converted_spend)
        self.assertEqual("error", updated_entry["status"])
        self.assertFalse(updated_entry["currency_conversion_applied"])
        self.assertAlmostEqual(100.0, updated_entry["total_original"])
        self.assertAlmostEqual(0.0, updated_entry["total_eur"])
        self.assertIn("no EUR conversion rate", updated_entry["message"])


if __name__ == "__main__":
    unittest.main()
