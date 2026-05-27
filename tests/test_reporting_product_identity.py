import json
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd

from export_orders import BizniWebExporter


class ReportingIdentityExporter(BizniWebExporter):
    def __init__(self, project_name: str):
        super().__init__(
            api_url="https://example.test/graphql",
            api_token="test-token",
            project_name=project_name,
            output_tag=f"{project_name}-identity-test",
        )
        expense_path = Path(__file__).resolve().parents[1] / "projects" / project_name / "product_expenses.json"
        if expense_path.exists():
            cost_map = json.loads(expense_path.read_text(encoding="utf-8"))
            self.product_expenses_exact = cost_map
            self.product_expenses_normalized = {
                self._normalize_match_text(key): float(value)
                for key, value in cost_map.items()
                if self._normalize_match_text(key)
            }


def reporting_item(
    order_num: str,
    product_sku: str,
    item_label: str,
    item_import_code: str,
    revenue: float,
    quantity: float = 1.0,
) -> dict:
    return {
        "order_num": order_num,
        "customer_email": f"{order_num.lower()}@example.test",
        "purchase_date": "2026-05-01 10:00:00",
        "purchase_date_only": "2026-05-01",
        "product_sku": product_sku,
        "item_label": item_label,
        "item_ean": "",
        "item_import_code": item_import_code,
        "item_quantity": quantity,
        "item_total_without_tax": revenue,
        "total_expense": revenue * 0.4,
        "profit_before_ads": revenue * 0.6,
        "fb_ads_daily_spend": 0.0,
        "google_ads_daily_spend": 0.0,
    }


class ReportingProductIdentityTests(unittest.TestCase):
    def test_vevo_and_roy_reporting_prefer_import_code_identity(self) -> None:
        for project in ("vevo", "roy"):
            with self.subTest(project=project):
                exporter = ReportingIdentityExporter(project)
                self.assertTrue(exporter._prefer_import_code_product_identity())

                item_df = pd.DataFrame(
                    [
                        reporting_item("R-HU", "H-HU", "Micro SD CARD 32GB adapterrel", "12474", 12),
                        reporting_item("R-CZ", "H-CZ", "Micro SD CARD 32GB s adaptérem", "12474", 18),
                    ]
                )

                canonical_df = exporter.add_reporting_product_identity_columns(item_df)
                self.assertEqual(["12474"], canonical_df["product_sku"].drop_duplicates().tolist())

                _, _, items_agg, _, _ = exporter.create_aggregated_reports(
                    canonical_df,
                    datetime(2026, 5, 1),
                    datetime(2026, 5, 1),
                    fb_daily_spend={},
                    google_ads_daily_spend={},
                )

                self.assertEqual(1, len(items_agg))
                self.assertEqual("12474", items_agg.iloc[0]["product_sku"])
                self.assertEqual(2, int(items_agg.iloc[0]["total_quantity"]))
                self.assertEqual(30, float(items_agg.iloc[0]["total_revenue"]))

    def test_roy_reporting_expands_maco_stop_large_set_to_components(self) -> None:
        exporter = ReportingIdentityExporter("roy")
        item_df = pd.DataFrame(
            [
                reporting_item(
                    "R-MACO",
                    "H-226DA29F",
                    "Set MACO STOP VEĽKÝ",
                    "",
                    120.0,
                    quantity=2,
                ),
            ]
        )

        canonical_df = exporter.add_reporting_product_identity_columns(item_df)

        self.assertNotIn("H-226DA29F", set(canonical_df["product_sku"]))
        self.assertNotIn("Set MACO STOP VEĽKÝ", set(canonical_df["item_label"]))
        self.assertEqual({"14832", "12840", "F_482"}, set(canonical_df["product_sku"]))
        self.assertTrue(canonical_df["bundle_component_flag"].all())
        self.assertEqual({"Set MACO STOP VEĽKÝ"}, set(canonical_df["bundle_parent_item_label"]))
        self.assertAlmostEqual(120.0, float(canonical_df["item_total_without_tax"].sum()), places=2)
        self.assertAlmostEqual(52.02, float(canonical_df["total_expense"].sum()), places=2)

        quantity_by_sku = canonical_df.set_index("product_sku")["item_quantity"].to_dict()
        self.assertEqual(2, int(quantity_by_sku["14832"]))
        self.assertEqual(2, int(quantity_by_sku["12840"]))
        self.assertEqual(2, int(quantity_by_sku["F_482"]))

        _, _, items_agg, _, _ = exporter.create_aggregated_reports(
            canonical_df,
            datetime(2026, 5, 1),
            datetime(2026, 5, 1),
            fb_daily_spend={},
            google_ads_daily_spend={},
        )
        self.assertEqual({"14832", "12840", "F_482"}, set(items_agg["product_sku"]))
        self.assertAlmostEqual(120.0, float(items_agg["total_revenue"].sum()), places=2)

    def test_roy_reporting_expands_wachman_rio_solar_to_components(self) -> None:
        exporter = ReportingIdentityExporter("roy")
        item_df = pd.DataFrame(
            [
                reporting_item(
                    "R-RIO",
                    "RIOSOLAR4G",
                    "Wachman Rio Solar 4G",
                    "RioSolar4G",
                    243.82,
                    quantity=1,
                ),
            ]
        )

        canonical_df = exporter.add_reporting_product_identity_columns(item_df)

        self.assertNotIn("RIOSOLAR4G", set(canonical_df["product_sku"]))
        self.assertNotIn("Wachman Rio Solar 4G", set(canonical_df["item_label"]))
        self.assertEqual({"F_1472", "F_486"}, set(canonical_df["product_sku"]))
        self.assertTrue(canonical_df["bundle_component_flag"].all())
        self.assertAlmostEqual(243.82, float(canonical_df["item_total_without_tax"].sum()), places=2)
        self.assertAlmostEqual(153.33, float(canonical_df["total_expense"].sum()), places=2)

        quantity_by_sku = canonical_df.set_index("product_sku")["item_quantity"].to_dict()
        self.assertEqual(1, int(quantity_by_sku["F_1472"]))
        self.assertEqual(1, int(quantity_by_sku["F_486"]))

        _, _, items_agg, _, _ = exporter.create_aggregated_reports(
            canonical_df,
            datetime(2026, 5, 1),
            datetime(2026, 5, 1),
            fb_daily_spend={},
            google_ads_daily_spend={},
        )
        self.assertEqual({"F_1472", "F_486"}, set(items_agg["product_sku"]))
        self.assertAlmostEqual(243.82, float(items_agg["total_revenue"].sum()), places=2)


if __name__ == "__main__":
    unittest.main()
