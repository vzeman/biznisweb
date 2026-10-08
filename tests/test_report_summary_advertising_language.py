import csv
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import daily_report_runner as runner


class SummaryAdvertisingLanguageTests(unittest.TestCase):
    def summary(self, root, recent_revenue, recent_ads):
        path = root / "daily.csv"
        records = []
        with path.open("w", newline="", encoding="utf-8") as stream:
            fields = ["date", "total_revenue", "unique_orders", "product_expense", "packaging_cost",
                      "shipping_net_cost", "fb_ads_spend", "google_ads_spend", "net_profit",
                      "pre_ad_contribution_profit", "pre_ad_contribution_margin_pct", "fixed_daily_cost"]
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for index in range(14):
                day = date(2026, 9, 24) + timedelta(days=index)
                revenue, ads = (recent_revenue, recent_ads) if index >= 7 else (200, 20)
                writer.writerow(dict(zip(fields, [day.isoformat(), revenue, 10, 40, 3, 2, ads, 0,
                                                    revenue - 45 - ads - 70, revenue - 45,
                                                    (revenue - 45) / revenue * 100, 70])))
                records.append({"date": day, "email": f"synthetic-{index}@example.invalid", "first_date": day})
        with patch.object(runner, "_load_order_records", return_value=records):
            return runner.build_report_summary({"aggregate_by_date_csv": path})

    def test_high_shop_ratio_keeps_value_and_profit_but_claims_no_campaign_effect(self):
        with TemporaryDirectory() as folder:
            text = self.summary(Path(folder), 300, 30)
        self.assertIn("shop MER 10.00x", text)
        self.assertIn("cisty zisk po reklamach a fixoch", text)
        self.assertIn("zahrna aj organicke a opakovane nakupy", text)
        self.assertIn("Samotny pomer nedokazuje ucinnost kampani", text)
        self.assertNotIn("Reklama stale funguje", text)
        self.assertNotIn("tym bezpecnejsie sa da skalovat", text)

    def test_declining_ratio_and_rising_cac_require_campaign_evidence(self):
        with TemporaryDirectory() as folder:
            text = self.summary(Path(folder), 100, 40)
        self.assertIn("shop MER 2.50x", text)
        self.assertIn("Shop MER za 7 dni klesol", text)
        self.assertIn("Zmiesany CAC narastol", text)
        self.assertIn("platformovu atribuciu, dozretie objednavok a marzu", text)
        self.assertIn("CO UKAZUJU SUHRNNE DATA", text)
        for unsupported in ["Reklama je slabsia", "Reklama momentalne taha slabsi efekt",
                            "CO TO PRAVDEPODOBNE SPOSOBILO", "kampane s najslabsim ROAS/CAC"]:
            self.assertNotIn(unsupported, text)


if __name__ == "__main__":
    unittest.main()
