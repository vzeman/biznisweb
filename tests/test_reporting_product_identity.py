import unittest
from datetime import datetime

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


if __name__ == "__main__":
    unittest.main()
